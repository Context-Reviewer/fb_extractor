#!/usr/bin/env python3
from playwright.sync_api import sync_playwright
from pathlib import Path
import argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user-data-dir", required=True)
    ap.add_argument("--url", default="https://www.facebook.com/")
    args = ap.parse_args()

    Path(args.user_data_dir).mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=args.user_data_dir,
            headless=False,
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()
        page.goto(args.url, wait_until="domcontentloaded")
        print("[i] Browser opened. Log in manually, then return here and press Enter to close.")
        input()
        ctx.close()

if __name__ == "__main__":
    main()