from pathlib import Path

TARGET_UID = "100054771426216"

p = Path("tools/discover_frontier.py")
lines = p.read_text(encoding="utf-8").splitlines(True)

def find_line(pred):
    for i, ln in enumerate(lines):
        if pred(ln):
            return i
    return -1

# --- Replace author_matches_target(...) function block ---
start = find_line(lambda s: s.startswith("def author_matches_target("))
if start < 0:
    raise SystemExit("ERR: could not find def author_matches_target")

# Find end of function by locating the next top-level "def " after start
end = -1
for i in range(start + 1, len(lines)):
    if lines[i].startswith("def ") and not lines[i].startswith("def author_matches_target"):
        end = i
        break
if end < 0:
    raise SystemExit("ERR: could not find end of author_matches_target block")

replacement = [
    "def author_matches_target(page, target_profile_url: str, target_slug: str, target_uid: str = \"\") -> bool:\n",
    "    try:\n",
    "        hrefs: List[str] = page.eval_on_selector_all(\n",
    "            \"a[href]\",\n",
    "            \"els => els.map(e => e.getAttribute('href')).filter(Boolean)\",\n",
    "        )\n",
    "    except Exception:\n",
    "        hrefs = []\n",
    "\n",
    "    t_full = target_profile_url.replace(\"http://\", \"https://\").rstrip(\"/\").lower()\n",
    "    t_slug = target_slug.strip().lower()\n",
    "    t_uid = (target_uid or \"\").strip()\n",
    "\n",
    "    for h in hrefs:\n",
    "        if not isinstance(h, str):\n",
    "            continue\n",
    "        hh = h\n",
    "        if hh.startswith(\"/\"):\n",
    "            hh = \"https://www.facebook.com\" + hh\n",
    "        hh_norm = hh.replace(\"http://\", \"https://\").rstrip(\"/\").lower()\n",
    "        path = urlparse(hh_norm).path\n",
    "\n",
    "        if t_full and t_full in hh_norm:\n",
    "            return True\n",
    "        if t_slug and (\"/\" + t_slug) in path:\n",
    "            return True\n",
    "        if t_uid:\n",
    "            if f\"/user/{t_uid}\" in path:\n",
    "                return True\n",
    "            if f\"profile.php?id={t_uid}\" in hh_norm:\n",
    "                return True\n",
    "\n",
    "    return False\n",
    "\n",
]

lines[start:end] = replacement

# --- Ensure target_uid exists in main() near target_slug ---
slug_idx = find_line(lambda s: s.strip() == "target_slug = normalize_target_slug(target_profile)")
if slug_idx < 0:
    raise SystemExit("ERR: could not find target_slug assignment")

# If not already present, insert target_uid assignment on next line
has_uid = any("target_uid" in ln and "=" in ln for ln in lines[slug_idx:slug_idx+6])
if not has_uid:
    indent = lines[slug_idx].split("target_slug")[0]
    lines.insert(slug_idx + 1, f"{indent}target_uid = \"{TARGET_UID}\"\n")

# --- Patch call site ---
for i, ln in enumerate(lines):
    if "author_matches_target(" in ln and "verify_page" in ln and "target_profile" in ln and "target_slug" in ln:
        if "target_uid" not in ln:
            lines[i] = ln.replace("target_slug)", "target_slug, target_uid)")
        break

p.write_text("".join(lines), encoding="utf-8")
print("[OK] Patched discover_frontier.py: UID-aware author match + call site wired.")
