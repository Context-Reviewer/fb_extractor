#!/usr/bin/env bash
set -euo pipefail

THREAD_URL="${1:-https://www.facebook.com/groups/3970539883001618/posts/25560413986920896/}"
PROFILE_DIR="${2:-fb_extract_out/pw_fb_profile}"
TARGET="${3:-Sean Roy}"

source .venv/bin/activate

mkdir -p fb_extract_out
printf "%s\n" "$THREAD_URL" > fb_extract_out/threads_smoke.txt

python extract_observations.py \
  --threads-file fb_extract_out/threads_smoke.txt \
  --out-file fb_extract_out/observations.smoke.jsonl \
  --target "$TARGET" \
  --profile-dir "$PROFILE_DIR" \
  --headless 0 \
  --dump-html 1 \
  --resume 0 \
  --only-one 1

python phase3_slice_blocks.py --observations fb_extract_out/observations.smoke.jsonl
python phase4_build_corpus.py
python phase5_corpus_qa.py

python - <<'PY'
import json
from pathlib import Path
p = Path("fb_extract_out/phase4_corpus.jsonl")
n = sum(1 for l in p.read_text(encoding="utf-8").splitlines() if l.strip()) if p.exists() else 0
print("[OK] phase4_corpus_rows:", n)
PY
SH

chmod +x tools/smoke_one_thread.sh