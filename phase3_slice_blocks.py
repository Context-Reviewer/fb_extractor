#!/usr/bin/env python3
"""
Phase 3: Slice Blocks (Offline)
Parses after_expand.html from Phase 2 debug artifacts and extracts
semantic blocks (role="article") into individual HTML files.

Deterministic output:
- Stable thread_id
- Stable block ordering (DOM order)
- Verified SHA256 content addressing

Inputs:
  --observations fb_extract_out/observations.jsonl
Outputs:
  --out fb_extract_out/phase3_blocks.jsonl
  --blocks-dir fb_extract_out/blocks
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any

# Try to import BeautifulSoup
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# Try to import lxml for faster parsing if bs4 is present
try:
    import lxml
    HAS_LXML = True
except ImportError:
    HAS_LXML = False


def parse_args():
    parser = argparse.ArgumentParser(description="Slice HTML into blocks (offline phase 3)")
    parser.add_argument("--observations", type=str, default="fb_extract_out/observations.jsonl",
                        help="Input observations JSONL file")
    parser.add_argument("--out", type=str, default="fb_extract_out/phase3_blocks.jsonl",
                        help="Output blocks JSONL index file")
    parser.add_argument("--blocks-dir", type=str, default="fb_extract_out/blocks",
                        help="Directory to save block HTML files")
    return parser.parse_args()


def calculate_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()

def calculate_sha16(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()[:16]


def resolve_debug_path(recorded_path: str, debug_root_local: Path) -> Optional[Path]:
    """
    Resolve the debug directory path by finding the 'run_<timestamp>' segment
    and anchoring it to the local debug root.
    """
    # We expect recorded_path to end with .../debug/run_.../thread_...
    # or just run_.../thread_...
    
    # Normalize separators
    p_str = recorded_path.replace("\\", "/")
    
    # regex for run_YYYY...
    # We look for "run_" followed by digits/chars
    match = re.search(r"(run_[^/]+)/thread_[^/]+", p_str)
    if match:
        # found run_.../thread_...
        # Extract the part from run_... onwards
        start_idx = match.start()
        rel_path = p_str[start_idx:]
        
        local_path = debug_root_local / rel_path
        if local_path.exists():
            return local_path
            
    # Fallback: check if absolute path exists as-is
    p = Path(recorded_path)
    if p.exists():
        return p
        
    return None


def extract_blocks_bs4(html_content: bytes) -> List[Dict[str, Any]]:
    """Extract blocks using BeautifulSoup."""
    parser = "lxml" if HAS_LXML else "html.parser"
    soup = BeautifulSoup(html_content, parser)
    
    blocks = []
    
    # deterministic find_all (doc order)
    articles = soup.find_all(attrs={"role": "article"})
    
    for idx, tag in enumerate(articles):
        # Serialize outer HTML
        # bs4 decode usually returns string, encode to bytes for consistency
        outer_html = str(tag).encode("utf-8")
        
        # Get details
        aria_label = tag.get("aria-label", "").strip()
        text_content = tag.get_text(" ", strip=True)
        
        blocks.append({
            "index": idx,
            "outer_html_bytes": outer_html,
            "aria_label": aria_label,
            "text_content": text_content,
            "text_len": len(text_content)
        })
        
    return blocks


def extract_blocks_regex(html_content: bytes) -> List[Dict[str, Any]]:
    """
    Fallback extraction using stack-based parsing when bs4/lxml are missing.
    Finds <div ... role="article" ...> and extracts the full balanced tag.
    """
    if not HAS_BS4:
        print("[WARN] Using STACK-BASED fallback for block extraction (bs4 is missing)", file=sys.stderr)
    
    html = html_content.decode("utf-8", errors="replace")
    blocks = []
    
    # 1. Find all start indices of <div ... role="article"
    tag_start_re = re.compile(r"<div\s+[^>]*role=[\"']article[\"'][^>]*>", re.I)
    
    block_indices = []
    for m in tag_start_re.finditer(html):
        block_indices.append(m.start())
        
    # 2. For each start, find the matching </div> using a stack
    div_tag_re = re.compile(r"(<div\b[^>]*>)|(</div>)", re.I)
    
    for idx, start_pos in enumerate(block_indices):
        depth = 0
        end_pos = -1
        
        for m in div_tag_re.finditer(html, pos=start_pos):
            is_start = m.group(1) is not None
            if is_start:
                depth += 1
            else:
                depth -= 1
            if depth == 0:
                end_pos = m.end()
                break
        
        if end_pos != -1:
            outer_html = html[start_pos:end_pos]
            content_bytes = outer_html.encode("utf-8")
            
            # naive aria-label extract
            aria_match = re.search(r"aria-label=[\"']([^\"']*)[\"']", outer_html)
            aria_label = aria_match.group(1) if aria_match else ""
            
            # naive text extract (strip tags)
            text_only = re.sub(r"<[^>]+>", " ", outer_html)
            text_content = re.sub(r"\s+", " ", text_only).strip()
            
            blocks.append({
                "index": idx,
                "outer_html_bytes": content_bytes,
                "aria_label": aria_label,
                "text_content": text_content,
                "text_len": len(text_content)
            })
            
    return blocks


def get_debug_dir_raw(rec: Dict[str, Any]) -> Optional[str]:
    """
    Robustly find the debug directory path from various schema locations.
    Can derive from file paths in 'debug' dict.
    """
    ev = rec.get("evidence") or {}
    debug_obj = rec.get("debug") or {}
    ev_debug = (ev.get("debug") if isinstance(ev, dict) else {}) or {}

    # 1. Explicit directory keys
    candidates = [
        ev.get("debug_dir") if isinstance(ev, dict) else None,
        rec.get("debug_dir"),
        rec.get("thread_debug_dir"),
        debug_obj.get("thread_dir") if isinstance(debug_obj, dict) else None,
        debug_obj.get("debug_dir") if isinstance(debug_obj, dict) else None,
        ev_debug.get("thread_dir"),
        ev_debug.get("debug_dir"),
    ]
    
    for path in candidates:
        if isinstance(path, str) and path.strip():
            return path.strip()

    # 2. Derive from file paths in debug object
    if isinstance(debug_obj, dict):
        for key in ["html", "screenshot", "after_expand", "start"]:
            val = debug_obj.get(key)
            if isinstance(val, str) and val.strip():
                if "/" in val or "\\" in val:
                    return str(Path(val).parent)
                    
    return None


def get_thread_id(rec: Dict[str, Any], thread_url: str) -> str:
    """
    Robustly find/derive thread_id.
    """
    ev = rec.get("evidence") or {}
    
    # 1. Explicit ID/Hash
    if isinstance(ev, dict) and ev.get("thread_hash"):
        return ev["thread_hash"]
    if rec.get("thread_id"):
        return rec["thread_id"]
    if rec.get("thread_hash"):
        return rec["thread_hash"]
        
    # 2. Fallback: derive from URL
    if thread_url:
        return calculate_sha16(thread_url.encode("utf-8"))
        
    return "unknown_thread"


def get_thread_url(rec: Dict[str, Any]) -> str:
    return (rec.get("thread_url") or 
            rec.get("url") or 
            rec.get("final_url") or 
            "")


def main():
    args = parse_args()
    
    obs_path = Path(args.observations).resolve()
    if not obs_path.exists():
        print(f"Observations file not found: {obs_path}")
        sys.exit(1)
        
    out_path = Path(args.out)
    blocks_root = Path(args.blocks_dir)
    blocks_root.mkdir(parents=True, exist_ok=True)
    
    # Derived debug root: assume sibling 'debug' folder to observations.jsonl
    debug_root_local = obs_path.parent / "debug"
    
    if not debug_root_local.exists():
        print(f"[WARN] Local debug root not found at expected: {debug_root_local}", file=sys.stderr)
        
    stats = {
        "total_lines": 0,
        "json_parsed": 0,
        "skipped_no_debug_dir": 0,
        "skipped_debug_dir_missing": 0,
        "skipped_after_expand_missing": 0,
        "processed_threads": 0,
        "total_blocks": 0
    }
    
    with obs_path.open("r", encoding="utf-8") as f_in, \
         out_path.open("w", encoding="utf-8") as f_out:
         
        for line in f_in:
            stats["total_lines"] += 1
            line = line.strip()
            if not line: continue
            
            try:
                rec = json.loads(line)
                stats["json_parsed"] += 1
            except json.JSONDecodeError:
                continue
            
            debug_dir_raw = get_debug_dir_raw(rec)
            if not debug_dir_raw:
                stats["skipped_no_debug_dir"] += 1
                continue
                
            debug_dir = resolve_debug_path(debug_dir_raw, debug_root_local)
            if not debug_dir or not debug_dir.exists():
                stats["skipped_debug_dir_missing"] += 1
                continue
                
            html_path = debug_dir / "after_expand.html"
            if not html_path.exists():
                stats["skipped_after_expand_missing"] += 1
                continue
                
            # Read HTML
            try:
                html_bytes = html_path.read_bytes()
            except Exception as e:
                print(f"[ERR] Failed to read {html_path}: {e}", file=sys.stderr)
                continue
                
            html_sha256 = calculate_sha256(html_bytes)
            
            # Extract Blocks
            if HAS_BS4:
                blocks_data = extract_blocks_bs4(html_bytes)
            else:
                blocks_data = extract_blocks_regex(html_bytes)
                if not blocks_data:
                     # This might represent "no blocks found" which is valid, 
                     # or "extraction failed". We can't distinguish easily without BS4.
                     pass
            
            # Process & Save Blocks
            thread_url = get_thread_url(rec)
            thread_id = get_thread_id(rec, thread_url)
            
            # Create thread subdir
            t_dir = blocks_root / thread_id
            t_dir.mkdir(exist_ok=True)
            
            thread_blocks = []
            
            for b in blocks_data:
                idx = b["index"]
                content = b["outer_html_bytes"]
                
                # Deterministic filename
                filename = f"block_{idx+1:03d}.html"
                file_path = t_dir / filename
                
                # Write extraction
                file_path.write_bytes(content)
                
                sha16 = calculate_sha16(content)
                
                thread_blocks.append({
                    "i": idx,
                    "sha16": sha16,
                    "aria_label": b["aria_label"],
                    "text_len": b["text_len"],
                    "html_file": str(file_path.relative_to(Path("."))) if file_path.is_relative_to(Path(".")) else str(file_path)
                })
                
            # Write Index Record
            out_rec = {
                "thread_url": thread_url,
                "thread_id": thread_id,
                "debug_dir": str(debug_dir),
                "after_expand_path": str(html_path),
                "after_expand_sha256": html_sha256,
                "block_count": len(thread_blocks),
                "blocks": thread_blocks
            }
            
            f_out.write(json.dumps(out_rec) + "\n")
            f_out.flush()
            
            stats["processed_threads"] += 1
            stats["total_blocks"] += len(thread_blocks)

    print("--- Phase 3 Slicing Summary ---")
    for k, v in stats.items():
        print(f"{k}: {v}")
    print(f"Index written to: {out_path}")

if __name__ == "__main__":
    main()
