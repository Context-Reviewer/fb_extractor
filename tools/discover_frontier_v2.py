#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Set

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

from discovery.browser import (
    goto_with_retries,
    looks_fb_error,
    looks_logged_out,
    page_dump,
    safe_goto,
)
from discovery.common import (
    Budget,
    DEBUG_BASE,
    append_jsonl,
    build_group_search_url,
    build_group_user_posts_url,
    canonical_group_post_url,
    canonicalize_url,
    now_iso,
    normalize_target_slug,
    parse_group_id,
    run_id,
)
from discovery.io import checkpoint_stats, load_existing_frontier
from discovery.surfaces import surface_group_feed, surface_group_search, surface_profile_group_posts
from discovery.verifier import verify_author


def choose_mode(args, *, have_group: bool) -> str:
    if args.mode != "auto":
        return args.mode

    if have_group and args.target_uid:
        return "profile_group_posts"
    if have_group and args.query:
        return "group_search"
    if not have_group and args.query:
        return "global_post_search"  # not implemented in this first drop
    raise SystemExit("[ERR] auto mode needs --group-url+--query OR --group-url+--target-uid (or --query without group, which is not implemented yet).")


def main() -> int:
    ap = argparse.ArgumentParser(description="Discover target-authored FB posts (v2 surfaces + unified verifier).")
    ap.add_argument("--group-url", default="")
    ap.add_argument("--query", default="")
    ap.add_argument("--target-profile", required=True)
    ap.add_argument("--target-uid", default="")
    ap.add_argument("--subject-label", required=True)

    ap.add_argument("--mode", default="auto", choices=["auto", "group_search", "profile_group_posts", "feed", "global_post_search"])
    ap.add_argument("--max-posts", type=int, default=200)
    ap.add_argument("--max-scrolls", type=int, default=250)
    ap.add_argument("--stop-after-no-new", type=int, default=8)
    ap.add_argument("--max-minutes", type=int, default=20)
    ap.add_argument("--pause-s", type=float, default=1.2)
    ap.add_argument("--verify-timeout-ms", type=int, default=45000)

    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--out", default="fb_extract_out/frontier_threads.jsonl")

    ap.add_argument("--user-data-dir", required=True)
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    target_profile = args.target_profile.replace("http://", "https://").rstrip("/")
    target_uid = (args.target_uid or "").strip()
    target_slug = normalize_target_slug(target_profile)
    subject_label = args.subject_label

    have_group = bool(args.group_url.strip())
    group_url = canonicalize_url(args.group_url) if have_group else ""
    group_id = parse_group_id(group_url) if have_group else ""

    mode = choose_mode(args, have_group=have_group)

    budget = Budget(
        max_posts=args.max_posts,
        max_scrolls=args.max_scrolls,
        stop_after_no_new=args.stop_after_no_new,
        max_minutes=args.max_minutes,
        pause_s=args.pause_s,
    )

    out_path = Path(args.out)
    if args.overwrite and out_path.exists():
        out_path.unlink()

    existing_urls, existing_post_ids = load_existing_frontier(out_path) if args.resume else (set(), set())

    run = run_id()
    dbg_dir = DEBUG_BASE / run
    dbg_dir.mkdir(parents=True, exist_ok=True)

    stats: Dict[str, Any] = {
        "run_id": run,
        "started_at": now_iso(),
        "mode": mode,
        "group_url": group_url or None,
        "group_id": group_id or None,
        "query": args.query or None,
        "target_profile": target_profile,
        "target_slug": target_slug,
        "target_uid": target_uid or None,
        "scrolls": 0,
        "candidates_seen": 0,
        "verify_attempts": 0,
        "verified_target_posts": 0,
        "skipped_existing": 0,
        "skipped_existing_post_id": 0,
        "stopped_reason": None,
    }
    stats_path = dbg_dir / "stats.json"
    checkpoint_stats(stats_path, stats)

    discovered_order = 0
    seen_post_ids: Set[str] = set()
    verified_post_ids: Set[str] = set()

    def stop_if_budget_or_time(start_ts: float) -> Optional[str]:
        if len(verified_post_ids) >= budget.max_posts:
            return "max_posts"
        if (time.time() - start_ts) / 60.0 >= budget.max_minutes:
            return "max_minutes"
        return None

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=args.user_data_dir,
            headless=args.headless,
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()
        verify_page = ctx.new_page()

        if mode == "group_search":
            if not have_group or not args.query:
                print("[ERR] group_search requires --group-url and --query", file=sys.stderr)
                return 2
            entry = build_group_search_url(group_id, args.query)
        elif mode == "profile_group_posts":
            if not have_group or not target_uid:
                print("[ERR] profile_group_posts requires --group-url and --target-uid", file=sys.stderr)
                return 2
            entry = build_group_user_posts_url(group_id, target_uid)
        elif mode == "feed":
            if not have_group:
                print("[ERR] feed requires --group-url", file=sys.stderr)
                return 2
            entry = group_url
        elif mode == "global_post_search":
            print("[ERR] global_post_search not implemented yet in this drop. Use --group-url with --query.", file=sys.stderr)
            return 2
        else:
            print("[ERR] Unknown mode", file=sys.stderr)
            return 2

        ok = goto_with_retries(page, entry, dbg_dir, "entry", tries=4)
        if not ok:
            print("[ERR] Could not load entry surface.", file=sys.stderr)
            print(f"[i] Debug: {dbg_dir}", file=sys.stderr)
            checkpoint_stats(stats_path, stats)
            ctx.close()
            return 3

        if looks_logged_out(page):
            page_dump(page, dbg_dir / "login_wall.html", dbg_dir / "login_wall.png")
            print("[ERR] Logged out / login wall detected. Profile not authenticated.", file=sys.stderr)
            print(f"[i] Debug: {dbg_dir}", file=sys.stderr)
            checkpoint_stats(stats_path, stats)
            ctx.close()
            return 3

        start_ts = time.time()

        if mode == "group_search":
            surf_iter = surface_group_search(page, group_id=group_id, surface_name="group_search", dbg_dir=dbg_dir, budget=budget)
        elif mode == "profile_group_posts":
            surf_iter = surface_profile_group_posts(page, group_id=group_id, surface_name="profile_group_posts", dbg_dir=dbg_dir, budget=budget)
        else:
            surf_iter = surface_group_feed(page, group_id=group_id, surface_name="feed", dbg_dir=dbg_dir, budget=budget)

        for new_pids, scroll_index, end_seen in surf_iter:
            stats["scrolls"] = scroll_index

            reason = stop_if_budget_or_time(start_ts)
            if reason:
                stats["stopped_reason"] = reason
                checkpoint_stats(stats_path, stats)
                break

            if end_seen:
                stats["stopped_reason"] = "end_of_results"

            for pid in new_pids:
                if pid in seen_post_ids:
                    continue
                seen_post_ids.add(pid)
                discovered_order += 1
                stats["candidates_seen"] = len(seen_post_ids)

                post_url = canonical_group_post_url(group_id, pid)
                if pid in existing_post_ids:
                    stats["skipped_existing_post_id"] += 1
                    continue

                if post_url in existing_urls:
                    stats["skipped_existing"] += 1
                    continue

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

                vr = verify_author(
                    verify_page,
                    target_profile_url=target_profile,
                    target_uid=target_uid,
                )
                if not vr.verified:
                    continue

                if pid in verified_post_ids:
                    continue

                verified_post_ids.add(pid)
                stats["verified_target_posts"] = len(verified_post_ids)

                row = {
                    "platform": "facebook",
                    "subject_label": subject_label,
                    "source_locator": entry,
                    "canonical_url": post_url,
                    "post_id": pid,
                    "group_id": group_id,
                    "author_label": subject_label,
                    "author_url": target_profile,
                    "author_uid": target_uid or None,
                    "target_uid": target_uid or None,
                    "target_profile": target_profile,
                    "discovered_at": now_iso(),
                    "evidence": {
                        "method": mode,
                        "surface": mode,
                        "scrolls": scroll_index,
                        "discovered_order": discovered_order,
                        "verify_attempt": stats["verify_attempts"],
                        "verification_method": vr.method,
                        "author_uid_found": vr.author_uid_found,
                        "run_id": run,
                    },
                }
                append_jsonl(out_path, row)
                checkpoint_stats(stats_path, stats)

                if len(verified_post_ids) <= 3:
                    page_dump(
                        verify_page,
                        dbg_dir / f"verified_{len(verified_post_ids):03d}.html",
                        dbg_dir / f"verified_{len(verified_post_ids):03d}.png",
                    )

            checkpoint_stats(stats_path, stats)

            if stats.get("stopped_reason") == "end_of_results":
                checkpoint_stats(stats_path, stats)
                break

        page_dump(page, dbg_dir / "surface_final.html", dbg_dir / "surface_final.png")
        ctx.close()

    print("Discovery v2 complete.")
    print("Mode:", mode)
    print("Frontier:", out_path)
    print("Verified target posts:", len(verified_post_ids))
    print("Scrolls:", stats["scrolls"], "Candidates:", stats["candidates_seen"], "Verify attempts:", stats["verify_attempts"])
    print("Debug dir:", dbg_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
