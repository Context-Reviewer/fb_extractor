#!/usr/bin/env python3
"""
Crash-safe deterministic discovery for target-authored FB group posts.

Strategy:
- Feed-first scroll group
- Extract candidate post_ids from HTML (regex-based; not anchor-dependent)
- Incrementally VERIFY author by opening candidate posts and matching author links:
    - target UID (if provided) via profile.php?id=...
    - else target slug/url in href paths (query stripped)
- Incrementally WRITE verified frontier rows to JSONL (checkpointing)
- Bound by max_posts / max_scrolls / max_minutes / stop_after_no_new

Output:
- fb_extract_out/frontier_threads.jsonl (one JSON object per line)
- fb_extract_out/discovered_threads.txt (canonical group post URLs, one per line)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import parse_qs, urlparse, urlunparse

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "fb_extract_out"
DEBUG_BASE = OUT_DIR / "discovery_debug"

GROUP_ID_RE = re.compile(r"/groups/(\d+)", re.IGNORECASE)
POST_RE = re.compile(r"/groups/(\d+)/posts/(\d+)", re.IGNORECASE)
PERMALINK_RE = re.compile(
    r"https?://www\.facebook\.com/permalink\.php\?story_fbid=(\d+)&id=(\d+)",
    re.IGNORECASE,
)
STORY_FBID_RE = re.compile(r"story_fbid=(\d+)", re.IGNORECASE)

# author link forms we care about
PROFILE_ID_RE = re.compile(r"profile\.php\?id=(\d+)", re.IGNORECASE)


@dataclass(frozen=True)
class Budget:
    max_posts: int
    max_scrolls: int
    stop_after_no_new: int
    max_minutes: int
    pause_s: float
    verify_every_scrolls: int
    verify_batch: int


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_id() -> str:
    return datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")


def canonicalize_url(url: str) -> str:
    p = urlparse(url)
    clean = p._replace(query="", fragment="")
    u = urlunparse(clean)
    if not u.endswith("/"):
        u += "/"
    return u


def strip_query_fragment(url: str) -> str:
    """Normalize FB hrefs by removing query + fragment (FB appends __cft__, __tn__, etc)."""
    try:
        p = urlparse(url)
        clean = p._replace(query="", fragment="")
        return urlunparse(clean)
    except Exception:
        return url


def parse_group_id(group_url: str) -> str:
    m = GROUP_ID_RE.search(group_url)
    if not m:
        raise ValueError(f"Could not parse group_id from url: {group_url}")
    return m.group(1)


def canonical_group_post_url(group_id: str, post_id: str) -> str:
    return f"https://www.facebook.com/groups/{group_id}/posts/{post_id}/"


def normalize_target_slug(target_profile_url: str) -> str:
    p = urlparse(target_profile_url)
    path = p.path.strip("/")
    if path.lower().startswith("profile.php"):
        return ""
    return path


def page_dump(page, out_html: Path, out_png: Path) -> None:
    out_html.write_text(page.content(), encoding="utf-8", errors="ignore")
    try:
        page.screenshot(path=str(out_png), full_page=True)
    except Exception:
        pass


def safe_goto(page, url: str, *, timeout_ms: int = 60000) -> None:
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)


def looks_logged_out(page) -> bool:
    checks = [
        "input[name='email']",
        "input[name='pass']",
        "form[action*='login']",
        "text=Log in",
        "text=Create new account",
    ]
    for sel in checks:
        try:
            if page.locator(sel).first.count() > 0:
                return True
        except Exception:
            pass
    return False


def looks_fb_error(page) -> bool:
    # very light heuristics; FB varies, but these show up in many hard-fail pages
    checks = [
        "text=This content isn't available right now",
        "text=Something went wrong",
        "text=Sorry, something went wrong",
        "text=Page isn't available",
    ]
    for sel in checks:
        try:
            if page.locator(sel).first.count() > 0:
                return True
        except Exception:
            pass
    return False


def goto_with_retries(page, url: str, dbg_dir: Path, prefix: str, *, tries: int = 3) -> bool:
    for i in range(1, tries + 1):
        try:
            safe_goto(page, url, timeout_ms=60000)
            time.sleep(1.0)
            if looks_fb_error(page):
                page_dump(page, dbg_dir / f"{prefix}_fb_error_try{i}.html", dbg_dir / f"{prefix}_fb_error_try{i}.png")
                continue
            return True
        except PWTimeoutError:
            page_dump(page, dbg_dir / f"{prefix}_timeout_try{i}.html", dbg_dir / f"{prefix}_timeout_try{i}.png")
        except Exception:
            page_dump(page, dbg_dir / f"{prefix}_exc_try{i}.html", dbg_dir / f"{prefix}_exc_try{i}.png")
    return False


def scroll_page(page, *, pause_s: float) -> None:
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(pause_s)


def extract_candidate_post_ids_from_html(html: str, group_id: str) -> List[str]:
    found: List[str] = []

    for m in POST_RE.finditer(html):
        gid, pid = m.group(1), m.group(2)
        if gid == group_id:
            found.append(pid)

    for m in PERMALINK_RE.finditer(html):
        pid, gid = m.group(1), m.group(2)
        if gid == group_id:
            found.append(pid)

    if f"/groups/{group_id}" in html:
        for m in STORY_FBID_RE.finditer(html):
            found.append(m.group(1))

    seen: Set[str] = set()
    uniq: List[str] = []
    for pid in found:
        if pid in seen:
            continue
        seen.add(pid)
        uniq.append(pid)
    return uniq


def _href_to_abs(h: str) -> str:
    if h.startswith("/"):
        return "https://www.facebook.com" + h
    return h


def _extract_profile_id_from_href(h: str) -> Optional[str]:
    """
    Extract profile.php?id=<uid> from href (query may contain tons of junk).
    Accepts relative or absolute.
    """
    try:
        abs_h = _href_to_abs(h)
        p = urlparse(abs_h)
        if p.path.lower().endswith("/profile.php") or p.path.lower() == "/profile.php":
            qs = parse_qs(p.query or "")
            v = qs.get("id", [])
            if v and v[0].isdigit():
                return v[0]
        # fallback regex
        m = PROFILE_ID_RE.search(abs_h)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


def author_matches_target(page, target_profile_url: str, target_slug: str, target_uid: str = "") -> bool:
    """
    Return True if the candidate post page appears to be authored by target.
    Matching order:
      1) if target_uid provided: match profile.php?id=<uid> in ANY author-ish href
      2) match target_profile_url (query stripped) as substring of href normalized
      3) match target slug in href path (query stripped)
    """
    try:
        hrefs: List[str] = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => e.getAttribute('href')).filter(Boolean)"
        )
    except Exception:
        hrefs = []

    t_full = strip_query_fragment(target_profile_url.replace("http://", "https://").rstrip("/")).lower()
    t_slug = target_slug.strip().lower()
    t_uid = (target_uid or "").strip()

    for h in hrefs:
        if not isinstance(h, str):
            continue

        hh_abs = _href_to_abs(h)
        hh_norm = strip_query_fragment(hh_abs).replace("http://", "https://").rstrip("/").lower()

        # 1) UID match (most stable when provided explicitly)
        if t_uid:
            uid = _extract_profile_id_from_href(hh_abs)
            if uid and uid == t_uid:
                return True

        # 2) Full URL match (vanity URL, etc.)
        if t_full and t_full in hh_norm:
            return True

        # 3) Slug in path
        if t_slug:
            try:
                if ("/" + t_slug) in urlparse(hh_norm).path:
                    return True
            except Exception:
                pass

    return False


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def emit_discovered_threads_txt(group_id: str, verified_post_ids: Set[str]) -> None:
    """
    Writes fb_extract_out/discovered_threads.txt deterministically:
    canonical group post URLs sorted numerically by post_id.
    """
    discovered_path = OUT_DIR / "discovered_threads.txt"
    discovered_path.parent.mkdir(parents=True, exist_ok=True)

    # stable ordering
    sorted_pids = sorted(verified_post_ids, key=lambda s: int(s))
    lines = [canonical_group_post_url(group_id, pid) for pid in sorted_pids]
    payload = "\n".join(lines) + ("\n" if lines else "")

    tmp_path = discovered_path.parent / (discovered_path.name + ".tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    tmp_path.replace(discovered_path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Discover target-authored FB group posts (deterministic, crash-safe).")
    ap.add_argument("--group-url", required=True)
    ap.add_argument("--target-profile", required=True)
    ap.add_argument("--target-uid", default="", help="Optional: numeric FB UID for target (most reliable author match).")
    ap.add_argument("--subject-label", default="Target")
    ap.add_argument("--max-posts", type=int, default=200)
    ap.add_argument("--max-scrolls", type=int, default=300)
    ap.add_argument("--stop-after-no-new", type=int, default=8)
    ap.add_argument("--max-minutes", type=int, default=20)
    ap.add_argument("--pause-s", type=float, default=1.2)
    ap.add_argument("--verify-timeout-ms", type=int, default=45000)
    ap.add_argument("--verify-every-scrolls", type=int, default=3, help="how often to run verify batch while scrolling")
    ap.add_argument("--verify-batch", type=int, default=25, help="how many new candidates to verify per batch")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--out", default=str(OUT_DIR / "frontier_threads.jsonl"))
    ap.add_argument("--user-data-dir", required=True)
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    group_url = canonicalize_url(args.group_url)
    target_profile = args.target_profile.replace("http://", "https://").rstrip("/")
    subject_label = args.subject_label

    group_id = parse_group_id(group_url)
    target_slug = normalize_target_slug(target_profile)
    target_uid = (args.target_uid or "").strip()

    budget = Budget(
        max_posts=args.max_posts,
        max_scrolls=args.max_scrolls,
        stop_after_no_new=args.stop_after_no_new,
        max_minutes=args.max_minutes,
        pause_s=args.pause_s,
        verify_every_scrolls=args.verify_every_scrolls,
        verify_batch=args.verify_batch,
    )

    out_path = Path(args.out)
    if args.overwrite and out_path.exists():
        out_path.unlink()

    run = run_id()
    dbg_dir = DEBUG_BASE / run
    dbg_dir.mkdir(parents=True, exist_ok=True)

    stats: Dict[str, Any] = {
        "run_id": run,
        "started_at": now_iso(),
        "group_url": group_url,
        "group_id": group_id,
        "target_profile": target_profile,
        "target_slug": target_slug,
        "target_uid": target_uid or None,
        "scrolls": 0,
        "no_new_streak": 0,
        "candidates_seen": 0,
        "verify_attempts": 0,
        "verified_target_posts": 0,
    }

    start_ts = time.time()

    candidates: List[str] = []
    candidates_set: Set[str] = set()
    verified_post_ids: Set[str] = set()
    verify_cursor = 0

    def checkpoint_stats() -> None:
        (dbg_dir / "stats.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=args.user_data_dir,
            headless=args.headless,
            viewport={"width": 1280, "height": 900},
        )

        page = ctx.new_page()
        verify_page = ctx.new_page()  # create early to avoid ctx.new_page late failures

        ok = goto_with_retries(page, group_url, dbg_dir, "group_start", tries=4)
        if not ok:
            print("[ERR] Could not load group page (login wall or FB error persisted).", file=sys.stderr)
            print(f"[i] Debug: {dbg_dir}", file=sys.stderr)
            checkpoint_stats()
            ctx.close()
            return 3

        if looks_logged_out(page):
            page_dump(page, dbg_dir / "login_wall.html", dbg_dir / "login_wall.png")
            print("[ERR] Logged out / login wall detected. Use a Playwright profile that is logged in.", file=sys.stderr)
            print(f"[i] Debug: {dbg_dir}", file=sys.stderr)
            checkpoint_stats()
            ctx.close()
            return 3

        def run_verify_batch() -> None:
            nonlocal verify_cursor
            if len(verified_post_ids) >= budget.max_posts:
                return

            end = min(len(candidates), verify_cursor + budget.verify_batch)
            while verify_cursor < end and len(verified_post_ids) < budget.max_posts:
                pid = candidates[verify_cursor]
                verify_cursor += 1

                if pid in verified_post_ids:
                    continue

                post_url = canonical_group_post_url(group_id, pid)
                stats["verify_attempts"] += 1

                try:
                    safe_goto(verify_page, post_url, timeout_ms=args.verify_timeout_ms)
                    time.sleep(0.8)
                except PWTimeoutError:
                    continue
                except Exception:
                    continue

                if looks_logged_out(verify_page) or looks_fb_error(verify_page):
                    continue

                if not author_matches_target(verify_page, target_profile, target_slug, target_uid=target_uid):
                    continue

                verified_post_ids.add(pid)
                stats["verified_target_posts"] = len(verified_post_ids)

                row = {
                    "platform": "facebook",
                    "subject_label": subject_label,
                    "source_locator": group_url,
                    "canonical_url": post_url,
                    "post_id": pid,
                    "group_id": group_id,
                    "author_label": subject_label,
                    "author_url": target_profile,
                    "author_uid": target_uid or None,
                    "discovered_at": now_iso(),
                    "evidence": {
                        "method": "feed_scroll_incremental_verify",
                        "scrolls": stats["scrolls"],
                        "verify_attempt": stats["verify_attempts"],
                        "run_id": run,
                    },
                }
                append_jsonl(out_path, row)

                # dump first 3 verified posts for proof
                if len(verified_post_ids) <= 3:
                    page_dump(
                        verify_page,
                        dbg_dir / f"verified_{len(verified_post_ids):03d}.html",
                        dbg_dir / f"verified_{len(verified_post_ids):03d}.png",
                    )

        # Main loop: scroll + extract + verify incrementally
        while True:
            elapsed_min = (time.time() - start_ts) / 60.0
            if elapsed_min >= budget.max_minutes:
                break
            if stats["scrolls"] >= budget.max_scrolls:
                break
            if stats["no_new_streak"] >= budget.stop_after_no_new:
                break
            if len(verified_post_ids) >= budget.max_posts:
                break

            stats["scrolls"] += 1

            try:
                html = page.content()
            except Exception:
                page_dump(page, dbg_dir / "page_content_error.html", dbg_dir / "page_content_error.png")
                checkpoint_stats()
                ctx.close()
                return 4

            pids = extract_candidate_post_ids_from_html(html, group_id)
            new = 0
            for pid in pids:
                if pid in candidates_set:
                    continue
                candidates_set.add(pid)
                candidates.append(pid)
                new += 1

            stats["candidates_seen"] = len(candidates)

            if new == 0:
                stats["no_new_streak"] += 1
            else:
                stats["no_new_streak"] = 0

            # periodic dumps + stats checkpoint
            if stats["scrolls"] in (1, 5, 10, 25, 50) or (stats["scrolls"] % 25 == 0):
                page_dump(
                    page,
                    dbg_dir / f"group_scroll_{stats['scrolls']:03d}.html",
                    dbg_dir / f"group_scroll_{stats['scrolls']:03d}.png",
                )
                checkpoint_stats()

            # verify batch every N scrolls (and also if we just added a lot)
            if (stats["scrolls"] % budget.verify_every_scrolls == 0) or (new >= 20):
                run_verify_batch()
                checkpoint_stats()

            # scroll (crash-safe)
            try:
                scroll_page(page, pause_s=budget.pause_s)
            except Exception:
                # FB sometimes kills the tab; dump what we have and exit cleanly
                page_dump(page, dbg_dir / "scroll_crash.html", dbg_dir / "scroll_crash.png")
                checkpoint_stats()
                ctx.close()
                return 4

        # final verify pass for remaining candidates discovered
        run_verify_batch()
        checkpoint_stats()

        page_dump(page, dbg_dir / "group_after_scroll.html", dbg_dir / "group_after_scroll.png")
        ctx.close()

    # Emit canonical frontier for extractor (deterministic)
    emit_discovered_threads_txt(group_id, verified_post_ids)

    print("Discovery complete.")
    print("Wrote frontier:", out_path)
    print("Verified target posts:", len(verified_post_ids))
    print("Scrolls:", stats["scrolls"], "Candidates:", stats["candidates_seen"], "Verify attempts:", stats["verify_attempts"])
    print("Debug dir:", dbg_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())