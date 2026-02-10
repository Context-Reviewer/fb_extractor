import hashlib
from pathlib import Path
import sys

# Find latest thread directory by mtime
try:
    debug_dir = Path("fb_extract_out/debug")
    if not debug_dir.exists():
        print(f"Directory not found: {debug_dir.absolute()}")
        sys.exit(1)

    thread_dirs = sorted(
        debug_dir.glob("run_*/thread_*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )

    if not thread_dirs:
        print("No thread dirs found")
        sys.exit(1)

    d = thread_dirs[0]
    print("Latest thread dir:", d)

    for name in ["start.html","after_expand.html","start.png","after_expand.png"]:
        p = d / name
        if not p.exists():
            print(name, "MISSING")
            continue
        h = hashlib.sha256(p.read_bytes()).hexdigest()[:16]
        print(f"{name:15s} hash={h} size={p.stat().st_size}")

except Exception as e:
    print(f"Error: {e}")
