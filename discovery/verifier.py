from __future__ import annotations

import re
from typing import List, Optional, Set
from urllib.parse import urlparse

from .common import (
    VerificationResult,
    extract_profile_id_from_href,
    href_to_abs,
    normalize_target_slug,
    strip_query_fragment,
)

# Bounded, deterministic author evidence patterns found in verified pages.
# actorID may appear as:
#   "actorID":100054771426216
#   "actorID":"100054771426216"
_ACTORID_NUM = r'"actorID"\s*:\s*{uid}\b'
_ACTORID_STR = r'"actorID"\s*:\s*"{uid}"\b'


_AUTHOR_SELECTORS = [
    "h2 a[href]",
    "h3 a[href]",
    "strong a[href]",
    "header a[href]",
    "a[role='link'][href]",
]


def _get_first_authorish_href(page) -> Optional[str]:
    for sel in _AUTHOR_SELECTORS:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                h = loc.get_attribute("href")
                if isinstance(h, str) and h.strip():
                    return h.strip()
        except Exception:
            continue
    return None


def _html_actorid_match(page, target_uid: str) -> bool:
    """
    Deterministic: match actorID in the HTML content.
    This is the strongest signal we’ve seen for Comet single-post views.
    """
    if not target_uid:
        return False
    try:
        html = page.content()
    except Exception:
        return False

    uid = re.escape(target_uid)
    pat_num = re.compile(_ACTORID_NUM.format(uid=uid))
    if pat_num.search(html):
        return True
    pat_str = re.compile(_ACTORID_STR.format(uid=uid))
    if pat_str.search(html):
        return True

    return False


def verify_author(page, *, target_profile_url: str, target_uid: str = "") -> VerificationResult:
    """
    Deterministic matching order:
      0) actorID in HTML (if target_uid provided)
      1) Header author href extraction (bounded selectors)
      2) UID match via href scan
      3) Canonical profile URL match
      4) Slug match
    """
    t_uid = (target_uid or "").strip()
    t_full = strip_query_fragment(target_profile_url.replace("http://", "https://").rstrip("/")).lower()
    t_slug = normalize_target_slug(target_profile_url).strip().lower()

    # 0) actorID in HTML
    if t_uid and _html_actorid_match(page, t_uid):
        return VerificationResult(True, "uid", author_uid_found=t_uid)

    # 1) header author href extraction
    h0 = _get_first_authorish_href(page)
    if h0:
        hh_abs = href_to_abs(h0)
        hh_norm = strip_query_fragment(hh_abs).replace("http://", "https://").rstrip("/").lower()

        uid0 = extract_profile_id_from_href(hh_abs)
        if uid0 and t_uid and uid0 == t_uid:
            return VerificationResult(True, "uid", author_uid_found=uid0)

        if t_full and t_full in hh_norm:
            return VerificationResult(True, "profile_url", author_uid_found=uid0)

        if t_slug:
            try:
                if ("/" + t_slug) in urlparse(hh_norm).path:
                    return VerificationResult(True, "slug", author_uid_found=uid0)
            except Exception:
                pass

    # 2+) fallback: scan all hrefs
    try:
        hrefs: List[str] = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => e.getAttribute('href')).filter(Boolean)"
        )
    except Exception:
        hrefs = []

    found_uids: Set[str] = set()

    for h in hrefs:
        if not isinstance(h, str):
            continue

        hh_abs = href_to_abs(h)
        hh_norm = strip_query_fragment(hh_abs).replace("http://", "https://").rstrip("/").lower()

        uid = extract_profile_id_from_href(hh_abs)
        if uid:
            found_uids.add(uid)
            if t_uid and uid == t_uid:
                return VerificationResult(True, "uid", author_uid_found=uid)

        if t_full and t_full in hh_norm:
            return VerificationResult(True, "profile_url", author_uid_found=(next(iter(found_uids)) if found_uids else None))

        if t_slug:
            try:
                path = urlparse(hh_norm).path
                if ("/" + t_slug) in path:
                    return VerificationResult(True, "slug", author_uid_found=(next(iter(found_uids)) if found_uids else None))
            except Exception:
                pass

    return VerificationResult(False, "none", author_uid_found=(next(iter(found_uids)) if found_uids else None))
