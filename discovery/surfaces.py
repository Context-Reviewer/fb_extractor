from __future__ import annotations

import re
from typing import Iterator, List, Set, Tuple

from .browser import page_dump, page_has_end_of_results, scroll_page
from .common import POST_RE, stable_dedupe_in_order


def _extract_post_ids_from_anchors(page, group_id: str) -> List[str]:
    try:
        hrefs = page.eval_on_selector_all(
            f"a[href*='/groups/{group_id}/posts/']",
            "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
        )
    except Exception:
        hrefs = []

    pids: List[str] = []
    for h in hrefs:
        if not isinstance(h, str):
            continue
        m = POST_RE.search(h)
        if not m:
            continue
        gid, pid = m.group(1), m.group(2)
        if gid == group_id and pid.isdigit():
            pids.append(pid)

    return stable_dedupe_in_order(pids)


def surface_group_search(page, *, group_id: str, surface_name: str, dbg_dir, budget) -> Iterator[Tuple[List[str], int, bool]]:
    seen: Set[str] = set()
    no_new_streak = 0

    for scroll_index in range(1, budget.max_scrolls + 1):
        pids = _extract_post_ids_from_anchors(page, group_id)
        new: List[str] = []
        for pid in pids:
            if pid in seen:
                continue
            seen.add(pid)
            new.append(pid)

        end_seen = page_has_end_of_results(page)

        if new:
            no_new_streak = 0
        else:
            no_new_streak += 1

        if scroll_index in (1, 3, 5, 10, 25, 50) or (scroll_index % 25 == 0):
            page_dump(
                page,
                dbg_dir / f"{surface_name}_scroll_{scroll_index:03d}.html",
                dbg_dir / f"{surface_name}_scroll_{scroll_index:03d}.png",
            )

        yield (new, scroll_index, end_seen)

        if end_seen:
            break
        if no_new_streak >= budget.stop_after_no_new:
            break

        scroll_page(page, pause_s=budget.pause_s)


def surface_profile_group_posts(page, *, group_id: str, surface_name: str, dbg_dir, budget) -> Iterator[Tuple[List[str], int, bool]]:
    yield from surface_group_search(page, group_id=group_id, surface_name=surface_name, dbg_dir=dbg_dir, budget=budget)


# Optional feed fallback surface (regex over HTML). Used only when you explicitly set --mode feed.
_GROUP_POST_RE = re.compile(r"/groups/(\d+)/posts/(\d+)", re.IGNORECASE)
_PERMALINK_RE = re.compile(
    r"https?://www\.facebook\.com/permalink\.php\?story_fbid=(\d+)&id=(\d+)",
    re.IGNORECASE,
)
_STORY_FBID_RE = re.compile(r"story_fbid=(\d+)", re.IGNORECASE)


def _extract_post_ids_from_html_regex(html: str, group_id: str) -> List[str]:
    found: List[str] = []
    for m in _GROUP_POST_RE.finditer(html):
        gid, pid = m.group(1), m.group(2)
        if gid == group_id:
            found.append(pid)
    for m in _PERMALINK_RE.finditer(html):
        pid, gid = m.group(1), m.group(2)
        if gid == group_id:
            found.append(pid)
    if f"/groups/{group_id}" in html:
        for m in _STORY_FBID_RE.finditer(html):
            found.append(m.group(1))
    return stable_dedupe_in_order(found)


def surface_group_feed(page, *, group_id: str, surface_name: str, dbg_dir, budget) -> Iterator[Tuple[List[str], int, bool]]:
    seen: Set[str] = set()
    no_new_streak = 0

    for scroll_index in range(1, budget.max_scrolls + 1):
        try:
            html = page.content()
        except Exception:
            page_dump(page, dbg_dir / f"{surface_name}_content_error.html", dbg_dir / f"{surface_name}_content_error.png")
            break

        pids = _extract_post_ids_from_html_regex(html, group_id)
        new: List[str] = []
        for pid in pids:
            if pid in seen:
                continue
            seen.add(pid)
            new.append(pid)

        if new:
            no_new_streak = 0
        else:
            no_new_streak += 1

        if scroll_index in (1, 5, 10, 25, 50) or (scroll_index % 25 == 0):
            page_dump(
                page,
                dbg_dir / f"{surface_name}_scroll_{scroll_index:03d}.html",
                dbg_dir / f"{surface_name}_scroll_{scroll_index:03d}.png",
            )

        yield (new, scroll_index, False)

        if no_new_streak >= budget.stop_after_no_new:
            break

        scroll_page(page, pause_s=budget.pause_s)
