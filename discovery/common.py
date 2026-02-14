from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import parse_qs, quote_plus, urlparse, urlunparse

GROUP_ID_RE = re.compile(r"/groups/(\d+)", re.IGNORECASE)
POST_RE = re.compile(r"/groups/(\d+)/posts/(\d+)", re.IGNORECASE)
PROFILE_ID_RE = re.compile(r"profile\.php\?id=(\d+)", re.IGNORECASE)

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "fb_extract_out"
DEBUG_BASE = OUT_DIR / "discovery_debug"


@dataclass(frozen=True)
class Budget:
    max_posts: int
    max_scrolls: int
    stop_after_no_new: int
    max_minutes: int
    pause_s: float


@dataclass(frozen=True)
class VerificationResult:
    verified: bool
    method: str  # uid|profile_url|slug|none
    author_uid_found: Optional[str] = None


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


def href_to_abs(h: str) -> str:
    if h.startswith("/"):
        return "https://www.facebook.com" + h
    return h


def extract_profile_id_from_href(h: str) -> Optional[str]:
    try:
        abs_h = href_to_abs(h)
        p = urlparse(abs_h)
        if p.path.lower().endswith("/profile.php") or p.path.lower() == "/profile.php":
            qs = parse_qs(p.query or "")
            v = qs.get("id", [])
            if v and v[0].isdigit():
                return v[0]
        m = PROFILE_ID_RE.search(abs_h)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


def build_group_search_url(group_id: str, query: str) -> str:
    q = quote_plus(query.strip())
    return f"https://www.facebook.com/groups/{group_id}/search/?q={q}"


def build_group_user_posts_url(group_id: str, uid: str) -> str:
    return f"https://www.facebook.com/groups/{group_id}/user/{uid}/"


def stable_dedupe_in_order(items: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / (path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
