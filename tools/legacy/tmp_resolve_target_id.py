import re
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

TARGET = "https://www.facebook.com/sean.roy.9465"

OUTDIR = Path("fb_extract_out/discovery_debug/resolve_target_id")
OUTDIR.mkdir(parents=True, exist_ok=True)

def pick_first(patterns, text):
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(1)
    return None

with sync_playwright() as p:
    ctx = p.chromium.launch_persistent_context(
        user_data_dir="fb_extract_out/playwright_profile",
        headless=False,
    )
    page = ctx.new_page()

    try:
        page.goto(TARGET, wait_until="domcontentloaded", timeout=90000)
        time.sleep(3.0)  # FB keeps network activity; don't use networkidle.
    except Exception as e:
        # Dump whatever we can for diagnosis
        try:
            (OUTDIR / "goto_error.txt").write_text(str(e) + "\n", encoding="utf-8")
        except Exception:
            pass
        try:
            page.screenshot(path=str(OUTDIR / "goto_error.png"), full_page=True)
        except Exception:
            pass
        try:
            (OUTDIR / "goto_error.html").write_text(page.content(), encoding="utf-8", errors="ignore")
        except Exception:
            pass
        raise

    url = page.url
    html = page.content()

    (OUTDIR / "page_url.txt").write_text(url + "\n", encoding="utf-8")
    (OUTDIR / "page.html").write_text(html, encoding="utf-8", errors="ignore")
    try:
        page.screenshot(path=str(OUTDIR / "page.png"), full_page=True)
    except Exception:
        pass

    # Try extracting numeric ID from URL first, then HTML
    patterns_url = [
        r"[?&]id=(\d+)",
        r"/groups/\d+/user/(\d+)",
        r"/people/[^/]+/(\d+)",
    ]

    patterns_html = [
        r'"userID"\s*:\s*"(\d+)"',
        r'"user_id"\s*:\s*"(\d+)"',
        r'"profile_id"\s*:\s*"(\d+)"',
        r"profile\.php\?id=(\d+)",
        r"/groups/\d+/user/(\d+)",
        r"/people/[^/]+/(\d+)",
    ]

    uid = pick_first(patterns_url, url) or pick_first(patterns_html, html)

    print("final_url =", url)
    print("uid =", uid if uid else "NONE")
    print("wrote =", str(OUTDIR))

    ctx.close()
