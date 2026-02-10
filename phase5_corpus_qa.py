#!/usr/bin/env python3
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportGeneralTypeIssues=false



import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path


def die(msg: str):
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(1)


def normalize_ws(s: str | None) -> str | None:
    if not s:
        return None
    s = s.replace("\xa0", " ")
    s = " ".join(s.split())
    return s if s else None


def read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                die(f"Invalid JSON on line {line_no}: {e}")
            if not isinstance(obj, dict):
                die(f"Expected object on line {line_no}, got: {type(obj)}")
            rows.append(obj)
    return rows


def write_json(path: Path, obj: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main():
    p = argparse.ArgumentParser(description="Phase 5: QA + stats + target-only export (offline, deterministic)")
    p.add_argument("--in", dest="inp", default="fb_extract_out/phase4_corpus.jsonl")
    p.add_argument("--out-dir", default="fb_extract_out")
    p.add_argument("--target-name", default="Sean Roy")
    p.add_argument("--target-required", type=int, default=0, help="If 1, fail if target hits == 0")
    p.add_argument("--max-excerpts", type=int, default=50, help="Max target excerpts in MD report")
    args = p.parse_args()

    in_path = Path(args.inp)
    out_dir = Path(args.out_dir)

    if not in_path.exists():
        die(f"Missing Phase 4 corpus: {in_path}")

    rows = read_jsonl(in_path)
    if not rows:
        die("Phase 4 corpus is empty")

    # Deterministic ordering
    rows.sort(key=lambda r: str(r.get("corpus_id", "")))

    # Basic validation + counters
    required = ["corpus_id", "thread_id", "block_index", "comment_index", "author", "text", "is_reply", "target_hit"]
    missing_required = 0

    authors = Counter()
    threads = Counter()
    reply_counts = Counter()
    target_rows: list[dict] = []

    text_len = []
    for r in rows:
        for k in required:
            if k not in r:
                missing_required += 1
                break

        author = normalize_ws(r.get("author"))
        if author:
            authors[author] += 1

        tid = normalize_ws(r.get("thread_id"))
        if tid:
            threads[tid] += 1

        is_reply = bool(r.get("is_reply"))
        reply_counts["reply" if is_reply else "comment"] += 1

        txt = normalize_ws(r.get("text"))
        if txt:
            text_len.append(len(txt))

        if bool(r.get("target_hit")):
            target_rows.append(r)

    target_hits = len(target_rows)
    total = len(rows)

    # Fail-loud gate
    if args.target_required == 1 and target_hits == 0:
        die("target_required=1 but target_hits == 0")

    # Stats object (stable keys)
    stats = {
        "phase": 5,
        "input": str(in_path),
        "target_name": args.target_name,
        "counts": {
            "records_total": total,
            "records_target_hit": target_hits,
            "records_reply": int(reply_counts["reply"]),
            "records_comment": int(reply_counts["comment"]),
            "threads": len(threads),
            "missing_required_records": missing_required,
        },
        "top_authors": [{"author": a, "count": c} for a, c in authors.most_common(25)],
        "threads_breakdown": [{"thread_id": t, "count": c} for t, c in threads.most_common(50)],
        "text_length": {
            "min": min(text_len) if text_len else None,
            "max": max(text_len) if text_len else None,
            "avg": (sum(text_len) / len(text_len)) if text_len else None,
        },
    }

    # Write outputs
    stats_json = out_dir / "phase5_stats.json"
    stats_md = out_dir / "phase5_stats.md"
    target_jsonl = out_dir / "phase5_target_only.jsonl"
    target_md = out_dir / "phase5_target_only.md"

    write_json(stats_json, stats)

    # Target-only jsonl (deterministic order already)
    with target_jsonl.open("w", encoding="utf-8") as f:
        for r in target_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Human report
    lines = []
    lines.append("# Phase 5 Corpus QA")
    lines.append("")
    lines.append(f"- Input: `{in_path}`")
    lines.append(f"- Target: `{args.target_name}`")
    lines.append("")
    lines.append("## Counts")
    lines.append(f"- Total records: **{total}**")
    lines.append(f"- Target hits: **{target_hits}**")
    lines.append(f"- Replies: **{reply_counts['reply']}**")
    lines.append(f"- Comments: **{reply_counts['comment']}**")
    lines.append(f"- Threads: **{len(threads)}**")
    lines.append(f"- Records missing required fields: **{missing_required}**")
    lines.append("")
    lines.append("## Top authors")
    for a, c in authors.most_common(15):
        lines.append(f"- {a}: {c}")
    lines.append("")
    lines.append("## Text length")
    if text_len:
        lines.append(f"- min: {min(text_len)}")
        lines.append(f"- max: {max(text_len)}")
        lines.append(f"- avg: {sum(text_len)/len(text_len):.2f}")
    else:
        lines.append("- no text lengths available")
    lines.append("")
    write_text(stats_md, "\n".join(lines) + "\n")

    # Target excerpt report
    tlines = []
    tlines.append("# Phase 5 Target-only Excerpts")
    tlines.append("")
    tlines.append(f"- Target: `{args.target_name}`")
    tlines.append(f"- Hits: **{target_hits}**")
    tlines.append("")
    for i, r in enumerate(target_rows[: max(0, args.max_excerpts)], start=1):
        cid = r.get("corpus_id")
        txt = normalize_ws(r.get("text")) or ""
        is_reply = "reply" if bool(r.get("is_reply")) else "comment"
        tlines.append(f"## {i}. {cid} ({is_reply})")
        tlines.append("")
        tlines.append(txt)
        tlines.append("")
    write_text(target_md, "\n".join(tlines) + "\n")

    print("\nPhase 5 QA Complete")
    print(f"Input records: {total}")
    print(f"Target hits: {target_hits}")
    print(f"Wrote: {stats_json}")
    print(f"Wrote: {stats_md}")
    print(f"Wrote: {target_jsonl}")
    print(f"Wrote: {target_md}")


if __name__ == "__main__":
    main()
