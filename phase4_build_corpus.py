#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportGeneralTypeIssues=false

import argparse
import json
import sys
from pathlib import Path
from bs4 import BeautifulSoup


def die(msg: str):
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(1)


def normalize_text(s: str | None) -> str | None:
    if not s:
        return None
    s = s.replace("\xa0", " ")
    s = " ".join(s.split())
    return s if s else None


def require_any(d: dict, keys: list[str], *, label: str, line_no: int) -> object:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    die(
        f"Missing required field '{label}' (expected one of {keys}) "
        f"in record (line {line_no}). Keys present: {sorted(d.keys())}"
    )


def as_int(x, *, label: str, line_no: int) -> int:
    try:
        return int(x)
    except Exception:
        die(f"Field '{label}' must be int-like on line {line_no}, got: {x!r}")


def write_debug(debug_dir: Path, thread_id: str, block_index: int, html_path: Path, soup: BeautifulSoup):
    debug_dir.mkdir(parents=True, exist_ok=True)
    out = debug_dir / f"{thread_id}_block_{block_index:03d}.txt"

    articles = soup.select('[role="article"]')
    dir_auto = soup.select('div[dir="auto"]')
    statuses = soup.select('[role="status"]')
    loading = soup.select('[aria-label="Loading..."]')
    commentish = soup.select('div[role="article"][aria-label]')

    def snip(text: str, n: int = 220) -> str:
        t = normalize_text(text) or ""
        return (t[:n] + "…") if len(t) > n else t

    lines = []
    lines.append(f"html_path: {html_path}")
    lines.append(f"count role=article: {len(articles)}")
    lines.append(f"count div[dir=auto]: {len(dir_auto)}")
    lines.append(f"count role=status: {len(statuses)}")
    lines.append(f"count aria-label Loading...: {len(loading)}")
    lines.append(f"count role=article[aria-label]: {len(commentish)}")
    lines.append("")

    lines.append("sample role=article[aria-label] values:")
    for i, el in enumerate(commentish[:8], start=1):
        al = el.get("aria-label", "")
        lines.append(f"  [{i}] {snip(al, 160)}")
    lines.append("")

    lines.append("sample div[dir=auto] texts:")
    for i, el in enumerate(dir_auto[:5], start=1):
        lines.append(f"  [{i}] {snip(el.get_text())}")

    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


def choose_best_thread_record_per_id(blocks_jsonl: Path) -> dict[str, dict]:
    """
    Phase3 may contain multiple records for the same thread_id (e.g., repeated runs).
    Deterministic rule:
      - prefer highest block_count
      - if tie, prefer the last occurrence in the file
    """
    threads_by_id: dict[str, tuple[int, int, dict]] = {}
    records_in = 0

    with blocks_jsonl.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            records_in += 1
            try:
                thread = json.loads(line)
            except json.JSONDecodeError as e:
                die(f"Invalid JSON on line {line_no}: {e}")

            tid = require_any(thread, ["thread_id", "thread", "post_thread_id", "threadId"], label="thread_id", line_no=line_no)
            tid = str(tid)

            bc = thread.get("block_count")
            try:
                bc_i = int(bc) if bc is not None else -1
            except Exception:
                bc_i = -1

            prev = threads_by_id.get(tid)
            if prev is None:
                threads_by_id[tid] = (bc_i, line_no, thread)
            else:
                prev_bc, prev_line, _prev_thread = prev
                if (bc_i > prev_bc) or (bc_i == prev_bc and line_no > prev_line):
                    threads_by_id[tid] = (bc_i, line_no, thread)

    # Return only thread dicts (stable ordering handled by caller)
    result = {tid: t for tid, (_bc, _ln, t) in threads_by_id.items()}
    result["_meta_records_in"] = {"records_in": records_in}  # type: ignore
    return result  # type: ignore


def extract_author_from_aria(aria_label: str) -> str | None:
    aria_label = aria_label or ""
    if aria_label.startswith("Comment by "):
        # "Comment by NAME 2 weeks ago"
        rest = aria_label[len("Comment by "):]
        # strip common suffix fragments
        for suf in (" weeks ago", " week ago", " days ago", " day ago", " hours ago", " hour ago", " minutes ago", " minute ago"):
            if suf in rest:
                rest = rest.split(suf, 1)[0]
        return normalize_text(rest.strip())

    if aria_label.startswith("Reply by "):
        # "Reply by NAME to X's comment 2 weeks ago"
        rest = aria_label[len("Reply by "):]
        if " to " in rest:
            rest = rest.split(" to ", 1)[0]
        return normalize_text(rest.strip())

    return None


def extract_text_from_node(node) -> str | None:
    # Tier 1: known good selector (paragraph chunks)
    parts: list[str] = []
    for el in node.select('div[dir="auto"][style*="text-align:start"]'):
        t = normalize_text(el.get_text())
        if t:
            parts.append(t)

    if parts:
        return "\n".join(parts)

    # Tier 2: fallback to largest dir=auto within node (still bounded and deterministic)
    best = None
    best_len = -1
    for el in node.select('div[dir="auto"]'):
        t = normalize_text(el.get_text())
        if t and len(t) > best_len:
            best = t
            best_len = len(t)

    return best


def main():
    parser = argparse.ArgumentParser(
        description="Phase 4: Build comment corpus from Phase 3 block HTML (offline, deterministic, WSL-safe)"
    )
    parser.add_argument("--blocks-jsonl", default="fb_extract_out/phase3_blocks.jsonl")
    parser.add_argument("--blocks-dir", default="fb_extract_out/blocks")
    parser.add_argument("--out", default="fb_extract_out/phase4_corpus.jsonl")
    parser.add_argument("--target-name", default="Sean Roy")
    parser.add_argument("--debug-one", type=int, default=0, help="Write debug for first N processed blocks")
    parser.add_argument("--debug-dir", default="fb_extract_out/phase4_debug")

    args = parser.parse_args()

    blocks_jsonl = Path(args.blocks_jsonl)
    blocks_dir = Path(args.blocks_dir)
    out_path = Path(args.out)
    debug_dir = Path(args.debug_dir)

    if not blocks_jsonl.exists():
        die(f"Missing Phase 3 blocks file: {blocks_jsonl}")
    if not blocks_dir.exists():
        die(f"Missing blocks directory: {blocks_dir}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    target_lower = args.target_name.lower()

    stats = {
        "thread_records_in": 0,
        "threads_deduped": 0,
        "threads_seen": set(),
        "blocks_seen": 0,
        "blocks_missing_html": 0,
        "blocks_processed": 0,
        "blocks_skipped_loading": 0,
        "blocks_skipped_no_articles": 0,
        "blocks_skipped_no_comments": 0,
        "blocks_with_comments": 0,
        "total_comment_nodes": 0,
        "records_written": 0,
        "author_missing": 0,
        "text_missing": 0,
        "target_hits": 0,
    }

    # Deduplicate thread records
    threads_map = choose_best_thread_record_per_id(blocks_jsonl)
    meta = threads_map.pop("_meta_records_in", {"records_in": 0})  # type: ignore
    stats["thread_records_in"] = int(meta.get("records_in", 0))
    stats["threads_deduped"] = len(threads_map)

    debug_written = 0

    with out_path.open("w", encoding="utf-8") as f_out:
        for thread_id in sorted(threads_map.keys()):
            thread = threads_map[thread_id]

            blocks_list = thread.get("blocks")
            if not isinstance(blocks_list, list):
                die(
                    f"Missing or invalid 'blocks' list in thread record (thread_id={thread_id}). "
                    f"Keys present: {sorted(thread.keys())}"
                )

            stats["threads_seen"].add(thread_id)

            thread_url = thread.get("thread_url")
            debug_dir_raw = thread.get("debug_dir")
            after_expand_sha256 = thread.get("after_expand_sha256")
            after_expand_path_raw = thread.get("after_expand_path")
            block_count_declared = thread.get("block_count")

            for block_pos, block in enumerate(blocks_list, start=1):
                stats["blocks_seen"] += 1

                block_index = block.get("block_index", block.get("block_i", block.get("idx", block_pos - 1)))
                block_index = as_int(block_index, label="block_index", line_no=1)

                sha16 = block.get("sha16") or block.get("block_sha16") or block.get("hash16")

                html_path = blocks_dir / thread_id / f"block_{block_index:03d}.html"
                if not html_path.exists():
                    stats["blocks_missing_html"] += 1
                    continue

                html = html_path.read_text(encoding="utf-8", errors="replace")
                soup = BeautifulSoup(html, "html.parser")

                # Skip loading skeletons
                if soup.select_one('[aria-label="Loading..."], [role="status"][aria-label="Loading..."]'):
                    stats["blocks_skipped_loading"] += 1
                    continue

                if not soup.select_one('[role="article"]'):
                    stats["blocks_skipped_no_articles"] += 1
                    continue

                stats["blocks_processed"] += 1

                if args.debug_one and debug_written < args.debug_one:
                    write_debug(debug_dir, thread_id, block_index, html_path, soup)
                    debug_written += 1

                # Extract comment/reply units by aria-label prefixes
                PREFIXES = ("Comment by ", "Reply by ")
                comment_nodes = []
                for n in soup.select('div[role="article"][aria-label]'):
                    al = n.get("aria-label", "")
                    if al.startswith(PREFIXES):
                        comment_nodes.append(n)
                # Deterministic ordering: aria-label + text length

                if not comment_nodes:
                    stats["blocks_skipped_no_comments"] += 1
                    continue

                stats["blocks_with_comments"] += 1
                stats["total_comment_nodes"] += len(comment_nodes)

                comment_index = 0
                for node in comment_nodes:
                    comment_index += 1

                    aria_label = node.get("aria-label", "")
                    is_reply = aria_label.startswith("Reply by ")

                    author = extract_author_from_aria(aria_label)
                    if not author:
                        stats["author_missing"] += 1

                    text = extract_text_from_node(node)
                    if not text:
                        stats["text_missing"] += 1

                    target_hit = (author is not None and author.lower() == target_lower)
                    if target_hit:
                        stats["target_hits"] += 1

                    record = {
                        "corpus_id": f"t:{thread_id}:b:{block_index}:c:{comment_index}",
                        "thread_id": thread_id,
                        "block_index": block_index,
                        "comment_index": comment_index,
                        "author": author,
                        "text": text,
                        "is_reply": is_reply,
                        "target_hit": target_hit,
                        "provenance": {
                            "block_sha16": sha16,
                            "html_relpath": str(html_path),
                            "phase": 4,
                            "thread_url": thread_url,
                            "debug_dir": debug_dir_raw,
                            "after_expand_sha256": after_expand_sha256,
                            "after_expand_path_raw": after_expand_path_raw,
                            "block_count_declared": block_count_declared,
                            "aria_label": aria_label,
                        },
                    }

                    f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    stats["records_written"] += 1

    print("\nPhase 4 Corpus Build Complete")
    print(f"Thread records in: {stats['thread_records_in']}")
    print(f"Threads deduped: {stats['threads_deduped']}")
    print(f"Threads seen: {len(stats['threads_seen'])}")
    print(f"Blocks seen: {stats['blocks_seen']}")
    print(f"Blocks processed: {stats['blocks_processed']}")
    print(f"Blocks missing HTML: {stats['blocks_missing_html']}")
    print(f"Blocks skipped (loading): {stats['blocks_skipped_loading']}")
    print(f"Blocks skipped (no_articles): {stats['blocks_skipped_no_articles']}")
    print(f"Blocks skipped (no_comments): {stats['blocks_skipped_no_comments']}")
    print(f"Blocks with comments: {stats['blocks_with_comments']}")
    print(f"Total comment nodes: {stats['total_comment_nodes']}")
    print(f"Corpus records written: {stats['records_written']}")
    print(f"Author missing: {stats['author_missing']}")
    print(f"Text missing: {stats['text_missing']}")
    print(f"Target hits: {stats['target_hits']}")

    if stats["blocks_processed"] == 0:
        die("No blocks processed — aborting")
    if stats["records_written"] == 0:
        die("No corpus records written — aborting")
    if stats["blocks_missing_html"] > 0:
        die("Missing block HTML detected — aborting")

    print("Phase 4 completed successfully.")


if __name__ == "__main__":
    main()
