#!/usr/bin/env python3
"""
Evidence-first Facebook observation extractor (sync Playwright) — MODAL AWARE.

Key fix:
- If a comments modal/dialog exists, expansion is restricted to that dialog
  (prevents clicking everywhere except the modal).

Output:
- fb_extract_out/observations.jsonl
- fb_extract_out/debug/run_<run_id>/thread_<hash>/...
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


DEFAULT_PROFILE_DIR = "/mnt/c/dev/fb_playwright_profile"
DEFAULT_THREADS_FILE = "fb_extract_out/discovered_threads.txt"
DEFAULT_OUT_FILE = "fb_extract_out/observations.jsonl"

# Patterns (tolerant)
RE_PRI_1 = re.compile(r"(view|see)\s+(previous|earlier)\s+comments", re.I)
RE_PRI_2 = re.compile(r"(view|see)\s+(\d+\s+)?more\s+comments", re.I)
RE_PRI_3 = re.compile(r"(view|see)\s+(\d+\s+)?more\s+repl(ies|y)", re.I)
RE_PRI_4 = re.compile(r"^(view|see)\s+repl(ies|y)$", re.I)
RE_SEE_MORE = re.compile(r"^see more$", re.I)


@dataclass
class Hit:
    url: str
    final_url: str
    ts_utc: str
    target: str
    raw_container_text: str
    evidence: dict


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def short_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


def load_threads(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Threads file not found: {path}")
    urls = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        u = line.strip()
        if not u or u.startswith("#"):
            continue
        urls.append(u)
    return urls


def write_jsonl(path: Path, obj: dict) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def screenshot(page, out_path: Path) -> None:
    try:
        page.screenshot(path=str(out_path), full_page=True)
    except Exception:
        pass


def dump_html(page, out_path: Path) -> None:
    try:
        out_path.write_text(page.content(), encoding="utf-8", errors="ignore")
    except Exception:
        pass


def text_or_label(el) -> str:
    try:
        t = (el.inner_text() or "").strip()
    except Exception:
        t = ""
    if t:
        return t
    try:
        a = (el.get_attribute("aria-label") or "").strip()
    except Exception:
        a = ""
    return a


def pick_expansion_root(page):
    """
    If a comments modal exists, restrict expansion to it.
    Score dialogs by affordance density using STRICT dialog-scoped locators.
    Formula: most_rel*10 + leave*10 + write*10 + like + reply
    Tie-break: if scores match, prefer SMALLEST non-zero bbox area (inner modal).
    Fallback to largest bbox if no scores.
    """
    dialogs = page.locator("div[role='dialog']")
    n = dialogs.count()
    if n == 0:
        print("[debug] dialogs=0 chosen_dialog=None score=0 fallback=body")
        return page  # fallback: whole page

    # Track all candidates: (index, score, area)
    candidates = []

    scan = min(n, 6)  # cap; usually 1-2 dialogs
    for i in range(scan):
        try:
            d = dialogs.nth(i)
            
            # Counts (STRICT LOCATORS within d)
            most_rel = d.locator(":text-matches('Most relevant', 'i')").count()
            leave = d.locator("[aria-label='Leave a comment']").count()
            write = d.locator(":text-matches('Write a comment', 'i')").count()
            like = d.locator(":text-matches('Like', 'i')").count()
            reply = d.locator(":text-matches('Reply', 'i')").count()

            score = (most_rel * 10) + (leave * 10) + (write * 10) + like + reply
            
            # Area
            box = d.bounding_box()
            area = (box["width"] * box["height"]) if box else 0

            print(f"[debug] dialog[{i}] area={area} most_rel={most_rel} leave={leave} write={write} like={like} reply={reply} score={score}")
            candidates.append({'idx': i, 'score': score, 'area': area})
        except Exception:
            continue

    if not candidates:
        print("[debug] dialogs_found_but_error chosen=None")
        return page

    # 1. Sort by score DESC
    candidates.sort(key=lambda x: x['score'], reverse=True)
    best_score = candidates[0]['score']

    final_idx = -1
    method = "none"

    if best_score > 0:
        # 2. Filter to only those with best_score
        ties = [c for c in candidates if c['score'] == best_score]
        
        # 3. Tie-break: Smallest non-zero area
        # Filter out zero area if possible, unless all are zero
        non_zero = [c for c in ties if c['area'] > 0]
        if non_zero:
             # Sort by area ASC
             non_zero.sort(key=lambda x: x['area'])
             final_idx = non_zero[0]['idx']
             method = "score_min_area"
             if len(non_zero) > 1 and non_zero[0]['area'] < non_zero[-1]['area']:
                 print(f"[debug] tie on score -> chose smallest area dialog idx={final_idx}")
        else:
             # All zero area, just take first
             final_idx = ties[0]['idx']
             method = "score_zero_area"
    else:
        # Fallback: Largest area
        candidates.sort(key=lambda x: x['area'], reverse=True)
        final_idx = candidates[0]['idx']
        method = "bbox_fallback"

    print(f"[debug] dialogs={n} chosen_dialog={final_idx} best_score={best_score} reason={method}")

    if final_idx >= 0:
        return dialogs.nth(final_idx)
    
    return page


def find_expand_controls(root) -> List[Tuple[int, str, object]]:
    """
    Return list of (priority, label, element-locator).
    IMPORTANT: root is either the modal dialog locator or the page.
    """
    # Use a broad clickable set INSIDE ROOT only.
    loc = root.locator("div[role='button'], a[role='button'], span[role='button'], button, a")
    n = loc.count()
    out: List[Tuple[int, str, object]] = []

    scan = min(n, 600)  # higher cap because we are now scoped to modal
    for i in range(scan):
        try:
            el = loc.nth(i)
            label = text_or_label(el)
            if not label:
                continue
            norm = re.sub(r"\s+", " ", label).strip()

            if RE_PRI_1.search(norm):
                out.append((1, norm, el))
            elif RE_PRI_2.search(norm):
                out.append((2, norm, el))
            elif RE_PRI_3.search(norm):
                out.append((3, norm, el))
            elif RE_PRI_4.search(norm):
                out.append((4, norm, el))
            elif RE_SEE_MORE.search(norm):
                out.append((9, norm, el))
        except Exception:
            continue

    out.sort(key=lambda x: (x[0], x[1]))
    return out



def get_scroll_container(page, root):
    """
    Find the best scrollable container within the dialog scope (or fallback to root).
    """
    # If root is page, we just use window scroll, so return None to signal that
    if hasattr(root, "locator") is False:
        return None

    # JS to find scrollable
    try:
        handle = root.element_handle()
        if not handle: return None

        # Return JSHandle of scrollable element
        return root.evaluate_handle("""(root) => {
            const isScrollable = (el) => {
                const style = window.getComputedStyle(el);
                return (style.overflowY === 'auto' || style.overflowY === 'scroll') && el.scrollHeight > el.clientHeight;
            };
            
            // 1. Check root itself
            if (isScrollable(root)) return root;

            // 2. Check children (BFS or querySelectorAll)
            const all = root.querySelectorAll('*');
            for (const el of all) {
                if (isScrollable(el)) return el;
            }
            return root; // Fallback to root
        }""")
    except Exception:
        return None


def scroll_modal(page, container_handle, amount=1200):
    """
    Scroll the container by amount.
    Returns (scrollTop, scrollHeight, clientHeight) after scroll.
    """
    try:
        if container_handle:
             return container_handle.evaluate("""(el, dy) => { 
                el.scrollBy(0, dy); 
                return [el.scrollTop, el.scrollHeight, el.clientHeight]; 
             }""", amount)
        else:
             # Page scroll
             return page.evaluate("""(dy) => {
                window.scrollBy(0, dy);
                return [window.scrollY, document.body.scrollHeight, window.innerHeight];
             }""", amount)
    except Exception:
        return (0, 0, 0)


def click_control(page, el, delay_s: float) -> bool:
    try:
        try:
            el.scroll_into_view_if_needed(timeout=1500)
        except Exception:
            pass

        try:
            el.click(timeout=2500)
        except Exception:
            el.click(timeout=2500, force=True)

        if delay_s > 0:
            page.wait_for_timeout(int(delay_s * 1000))
        return True
    except Exception:
        return False


def expand_until_stable(page, max_rounds: int, stable_rounds: int, delay_s: float, debug_dir: Path) -> dict:
    """
    Multi-level expansion loop, scoped to modal when present.
    """
    clicks_total = 0
    rounds = 0
    stable = 0
    seen_fp = set()
    last_scroll_top = -1
    nudge_done = False
    root = page

    for _ in range(max_rounds):
        rounds += 1

        root = pick_expansion_root(page)  # modal if present
        
        # --- NEW: Text-Based Expanders ---
        # Prioritize regex text matches using filter() as :text-matches was inconsistent
        expander_pattern = (
            r"(view\s+all\s+\d+\s+repl(?:y|ies)"
            r"|view\s+\d+\s+repl(?:y|ies)"
            r"|see\s+more"
            r"|view\s+more"
            r"|more\s+replies"
            r"|more\s+comments"
            r"|previous\s+items"
            r"|previous\s+replies"
            r"|view\s+previous)"
        )
        re_expander = re.compile(expander_pattern, re.I)
        
        # Use locator().filter() as requested
        text_candidates = root.locator("button, [role='button']").filter(has_text=re_expander)
        
        tc_count = text_candidates.count()
        text_clicks = 0
        # Try to click up to 5 text candidates per round
        for ti in range(min(tc_count, 10)):
             try:
                 el = text_candidates.nth(ti)
                 # Ensure visibility before clicking (click_control handles this too, but prompt asked for strictness)
                 try:
                     el.scroll_into_view_if_needed(timeout=1000)
                 except: pass
                 
                 if click_control(page, el, delay_s):
                     text_clicks += 1
             except: pass
        
        controls = find_expand_controls(root)

        # --- DEBUG: Inventory Dump if Zero Expanders ---
        if rounds == 1 and tc_count == 0 and len(controls) == 0:
             print("[debug] Zero expanders found on round 1. Dumping clickable inventory...")
             try:
                 # Broad clickable search
                 inv = root.locator("button, [role='button'], a, [aria-label]")
                 inv_count = inv.count()
                 print(f"[debug] Inventory scan found {inv_count} items (capped at 80).")
                 for ii in range(min(inv_count, 80)):
                     try:
                         el = inv.nth(ii)
                         # Use evaluate for safety vs inner_text throwing
                         props = el.evaluate("el => ({tag: el.tagName.toLowerCase(), text: el.textContent || '', role: el.getAttribute('role') || '', aria: el.getAttribute('aria-label') || ''})")
                         
                         txt = props['text'].strip().replace("\n", " ")[:120]
                         print(f"   [inv] tag={props['tag']} role={props['role']} aria={props['aria']} txt={txt}")
                     except Exception as e_el:
                         print(f"   [inv] <error reading element {ii}: {e_el}>")
             except Exception as e:
                 print(f"[debug] Inventory dump failed: {e}")

             # --- Evidence-First: Scroll Gating Fix ---
             # MOVED to end of loop to handle ALL rounds, not just round 1
             pass

        fp = "|".join([f"{p}:{lbl}" for (p, lbl, _) in controls][:80])
        if fp in seen_fp:
            stable += 1
        else:
            seen_fp.add(fp)

        clicks_total += text_clicks
        clicked_this_round = text_clicks  # Start with text clicks

        for (pri, label, el) in controls:
            ok = click_control(page, el, delay_s)
            if ok:
                clicked_this_round += 1
                clicks_total += 1
                if clicks_total <= 10:
                    screenshot(page, debug_dir / f"expand_click_{clicks_total:03d}.png")

        print(f"[debug] round={rounds} text_expanders={tc_count} other_expanders={len(controls)} clicks_this_round={clicked_this_round}")

        if clicked_this_round == 0:
            stable += 1
        else:
            stable = 0

        # --- SCROLL LOGIC ---
        # If we clicked nothing, OR we are in a 'text_expanders=0' state (meaning we might be gated),
        # try scrolling to reveal more.
        # Strict condition from prompt: "If clicks_this_round == 0 OR text_expanders == 0 and other_expanders == 0"
        
        should_scroll = (clicked_this_round == 0) or (tc_count == 0 and len(controls) == 0)
        
        if should_scroll:
            try:
                # Resolve container
                c_handle = get_scroll_container(page, root)
                
                # Scroll
                vals = scroll_modal(page, c_handle, 1200)
                st, sh, ch = vals if vals else (0,0,0)
                
                progressed = "no"
                if st != last_scroll_top:
                    progressed = "yes"
                    # If we moved, we are NOT stable, we just uncovered new area.
                    stable = 0 
                    last_scroll_top = st
                
                print(f"[debug] scroll: top={st} height={sh} client={ch} progressed={progressed}")
                
                # --- NUDGE SCROLL ---
                # "if text_expanders==0, other_expanders==0, and scroll progressed=no for 2 consecutive rounds"
                # stable tracks rounds where clicked_this_round==0.
                if progressed == "no" and stable >= 2 and not nudge_done:
                     if tc_count == 0 and len(controls) == 0:
                         print("[debug] Stuck & 0 expanders & no scroll progress. Attempting one-time NUDGE (-400, +1200)...")
                         
                         scroll_modal(page, c_handle, -400)
                         page.wait_for_timeout(1000)
                         
                         vals_nudge = scroll_modal(page, c_handle, 1200)
                         if vals_nudge:
                             last_scroll_top = vals_nudge[0]
                         
                         page.wait_for_timeout(1000)
                         nudge_done = True
                         stable = 0 # Reset stable to allow discovery after nudge
                
                if delay_s > 0:
                    page.wait_for_timeout(int(delay_s * 1000))
            except Exception as e:
                print(f"[debug] Scroll logic failed: {e}")

        if stable >= stable_rounds:
            break

    return {
        "expand_rounds": rounds,
        "expand_clicks_total": clicks_total,
        "expand_stable_rounds_reached": stable,
        "root_scoped_to_dialog": root is not page,
    }


def extract_modal_or_body_text(page) -> str:
    """
    Evidence-first: capture innerText of modal if present, else body.
    """
    try:
        root = pick_expansion_root(page)
        if root is page:
            return (page.locator("body").inner_text() or "").strip()
        return (root.inner_text() or "").strip()
    except Exception:
        return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads-file", default=DEFAULT_THREADS_FILE)
    ap.add_argument("--out-file", default=DEFAULT_OUT_FILE)
    ap.add_argument("--target", required=True)
    ap.add_argument("--profile-dir", default=DEFAULT_PROFILE_DIR)
    ap.add_argument("--headless", type=int, default=0)

    ap.add_argument("--max-expand-rounds", type=int, default=160)
    ap.add_argument("--stable-rounds", type=int, default=4)
    ap.add_argument("--expand-delay", type=float, default=1.0)

    ap.add_argument("--dump-html", type=int, default=0)
    ap.add_argument("--resume", type=int, default=1)
    ap.add_argument("--only-one", type=int, default=0)
    args = ap.parse_args()

    base_dir = Path(__file__).resolve().parent
    out_path = base_dir / args.out_file
    ensure_dir(out_path.parent)

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_root = base_dir / "fb_extract_out" / "debug" / f"run_{run_id}"
    ensure_dir(debug_root)

    threads = load_threads(str(base_dir / args.threads_file))
    if not threads:
        print("[!] No threads found.")
        return 2

    done = set()
    if args.resume and out_path.exists():
        for line in out_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            try:
                obj = json.loads(line)
                if obj.get("url"):
                    done.add(obj["url"])
            except Exception:
                pass

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=args.profile_dir,
            headless=bool(args.headless),
            viewport={"width": 1280, "height": 900},
            args=["--disable-notifications", "--no-sandbox"],
        )
        page = ctx.pages[0] if ctx.pages else ctx.new_page()

        for idx, url in enumerate(threads, start=1):
            if url in done:
                print(f"[{idx}/{len(threads)}] skip (resume): {url}")
                continue

            th = short_hash(url)
            tdir = debug_root / f"thread_{th}"
            ensure_dir(tdir)

            print(f"[{idx}/{len(threads)}] goto: {url}")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(1500)
            except PWTimeoutError:
                print("  [!] timeout on goto")
            except Exception as e:
                print(f"  [!] goto error: {e}")

            screenshot(page, tdir / "start.png")
            if args.dump_html:
                dump_html(page, tdir / "start.html")

            exp = expand_until_stable(
                page,
                max_rounds=args.max_expand_rounds,
                stable_rounds=args.stable_rounds,
                delay_s=args.expand_delay,
                debug_dir=tdir,
            )

            screenshot(page, tdir / "after_expand.png")
            if args.dump_html:
                dump_html(page, tdir / "after_expand.html")

            raw_text = extract_modal_or_body_text(page)

            hit = Hit(
                url=url,
                final_url=page.url,
                ts_utc=now_utc_iso(),
                target=args.target,
                raw_container_text=raw_text,
                evidence={
                    "run_id": run_id,
                    "thread_hash": th,
                    "debug_dir": str(tdir),
                    "expand": exp,
                },
            )
            write_jsonl(out_path, hit.__dict__)
            print(f"  wrote observation | clicks={exp['expand_clicks_total']} rounds={exp['expand_rounds']}")

            if args.only_one:
                break

        ctx.close()

    print(f"\nDone. Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
