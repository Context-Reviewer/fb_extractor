from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Set, Tuple

from .common import atomic_write_text


def load_existing_frontier(frontier_jsonl: Path) -> Tuple[Set[str], Set[str]]:
    """
    Return (existing_urls, existing_post_ids) from an append-only frontier JSONL.
    """
    if not frontier_jsonl.exists():
        return set(), set()

    urls: Set[str] = set()
    pids: Set[str] = set()

    for line in frontier_jsonl.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue

        u = obj.get("canonical_url")
        if isinstance(u, str) and u:
            urls.add(u)

        pid = obj.get("post_id")
        if isinstance(pid, str) and pid.isdigit():
            pids.add(pid)

    return urls, pids


def checkpoint_stats(stats_path: Path, stats: Dict[str, Any]) -> None:
    atomic_write_text(stats_path, json.dumps(stats, indent=2) + "\n")
