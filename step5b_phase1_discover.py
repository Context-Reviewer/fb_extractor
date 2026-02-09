#!/usr/bin/env python3
"""
Phase 1: Discovery (Evidence-first)

Goal:
- On a Facebook *group search* page, collect candidate links that likely point to posts/threads.
- Do NOT attempt to canonicalize or resolve here. Just gather candidates.

Writes:
- fb_extract_out/discovered_threads.txt  (repo-local)

Notes:
- Facebook search sometimes lands on "People" results. We try to click "Posts" or "Posts and comments".
- We ignore /groups/<gid>/user/<uid>/ links (people profiles).
"""

import re
import sys
import time
import argparse
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import sync_playwright


DEFAULT_PROFILE_DIR = "/mnt/c/dev/fb_playwright_profile"
DEFAULT_SCROLL_ROUNDS = 30
SCROLL_DELAY_S = 1.2


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def extract_group_id(url: str) -> str | None:
    m = re.search(r"/groups/([^/]+)/", url)
    return m.group(1) if m else None


def normalize_href(href: str) -> str:
    if href.startswith("/"):
        return "https://www.facebook.com" + href
    return href


def looks_threadish(href: str, aria_label: str) -> bool:
    # Accept common post/thread shapes seen on FB group surfaces
    if "/posts/" in href or "/permalink/" in href:
        return True
    if "story.php" in href or "permalink.php" in href:
        return True
    if "multi_permalinks" in href:
        return True

    # Timestamp-like aria-labels often correspond to permalinks on FB search surfaces
    if aria_label and re.search(r"\b(AM|PM|at|\d{1,2}:\d{2})\b", aria_label):
        return True

    # Relative time variants sometimes appear as text; aria-label might be empty though.
    return False


def try_click_posts_filter(page) -> None:
    # Best-effort: FB UI varies; we try a couple accessible-name matches.
    candidates = [
        ("link", r"^Posts$"),
        ("link", r"^Posts and comments$"),
        ("button", r"^Posts$"),
        ("button", r"^Posts and comments$"),
    ]
    for role, pattern in candidates:
        try:
            loc = page.get_by_role(role, name=re.compile(pattern, re.I))
            if loc.count() > 0:
                loc.first.click(timeout=5000)
                page.wait_for_timeout(1500)
                return
        except Exception:
            continue


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="", help="Facebook group search URL")
    ap.add_argument("--profile-dir", default=DEFAULT_PROFILE_DIR, help="Playwright persistent profile dir")
    ap.add_argument("--headless", type=int, default=0)
    ap.add_argument("--scroll-rounds", type=int, default=DEFAULT_SCROLL_ROUNDS)
    args = ap.parse_args()

    search_url = args.url.strip()
    if not search_url:
        search_url = input("Enter Facebook Group SEARCH URL: ").strip()

    if "facebook.com/groups/" not in search_url or "/search" not in search_url:
        print("[!] This does not look like a group search URL.")
        print("    Expected something like: https://www.facebook.com/groups/<gid>/search?q=...")
        return 2

    gid = extract_group_id(search_url)
    if not gid:
        print("[!] Could not extract group id from URL.")
        return 2

    base_dir = Path(__file__).resolve().parent
    out_dir = base_dir / "fb_extract_out"
    out_file = out_dir / "discovered_threads.txt"
    ensure_dir(out_dir)

    print("=" * 60)
    print("PHASE 1: DISCOVERY (evidence-first)")
    print("=" * 60)
    print(f"Target:  {search_url}")
    print(f"GroupID: {gid}")
    print(f"Profile: {args.profile_dir}")
    print(f"Output:  {out_file}")
    print()

    discovered = set()
    seen_raw = set()

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=args.profile_dir,
            headless=bool(args.headless),
            viewport={"width": 1280, "height": 900},
            args=["--disable-notifications", "--no-sandbox"],
        )
        page = context.pages[0] if context.pages else context.new_page()

        print("[Nav] Going to search page...")
        page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1500)

        input("Verify page loaded / logged in. Press Enter to start discovery...")

        # Try to switch results to Posts
        try_click_posts_filter(page)

        for r in range(args.scroll_rounds):
            print(f"\n--- Scroll {r + 1}/{args.scroll_rounds} ---")

            # Collect a broad set of group-related anchors
            anchors = page.locator(f"a[href*='/groups/{gid}/'], a[href*='story.php'], a[href*='permalink.php']")
            a_count = anchors.count()
            scanned = min(a_count, 350)
            added = 0

            for i in range(scanned):
                try:
                    a = anchors.nth(i)
                    href = a.get_attribute("href") or ""
                    aria = a.get_attribute("aria-label") or ""
                    if not href:
                        continue

                    href = normalize_href(href)

                    # Ignore People results
                    if f"/groups/{gid}/user/" in href:
                        continue

                    # De-dupe by href without fragment (keep query intact)
                    raw_key = href.split("#")[0]
                    if raw_key in seen_raw:
                        continue
                    seen_raw.add(raw_key)

                    if not looks_threadish(raw_key, aria):
                        continue

                    if raw_key not in discovered:
                        discovered.add(raw_key)
                        added += 1
                        print(f"   [Candidate] {raw_key} ({aria})")

                except Exception:
                    continue

            print(f"   Scanned {scanned} anchors | Added {added} | Total {len(discovered)}")

            # Scroll more results
            page.mouse.wheel(0, 1400)
            time.sleep(SCROLL_DELAY_S)

        context.close()

    with out_file.open("w", encoding="utf-8") as f:
        for u in sorted(discovered):
            f.write(u + "\n")

    print("\n" + "=" * 60)
    print("DISCOVERY COMPLETE")
    print("=" * 60)
    print(f"Total candidates: {len(discovered)}")
    print(f"Wrote: {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
