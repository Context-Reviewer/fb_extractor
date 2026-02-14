from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import TimeoutError as PWTimeoutError

LOGIN_CHECKS = [
    "input[name='email']",
    "input[name='pass']",
    "form[action*='login']",
    "text=Log in",
    "text=Create new account",
]

ERROR_CHECKS = [
    "text=This content isn't available right now",
    "text=Something went wrong",
    "text=Sorry, something went wrong",
    "text=Page isn't available",
]


def page_dump(page, out_html: Path, out_png: Path) -> None:
    out_html.write_text(page.content(), encoding="utf-8", errors="ignore")
    try:
        page.screenshot(path=str(out_png), full_page=True)
    except Exception:
        pass


def looks_logged_out(page) -> bool:
    for sel in LOGIN_CHECKS:
        try:
            if page.locator(sel).first.count() > 0:
                return True
        except Exception:
            pass
    return False


def looks_fb_error(page) -> bool:
    for sel in ERROR_CHECKS:
        try:
            if page.locator(sel).first.count() > 0:
                return True
        except Exception:
            pass
    return False


def safe_goto(page, url: str, *, timeout_ms: int = 60000) -> None:
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)


def goto_with_retries(page, url: str, dbg_dir: Path, prefix: str, *, tries: int = 3) -> bool:
    for i in range(1, tries + 1):
        try:
            safe_goto(page, url, timeout_ms=60000)
            time.sleep(1.0)
            if looks_fb_error(page):
                page_dump(page, dbg_dir / f"{prefix}_fb_error_try{i}.html", dbg_dir / f"{prefix}_fb_error_try{i}.png")
                continue
            return True
        except PWTimeoutError:
            page_dump(page, dbg_dir / f"{prefix}_timeout_try{i}.html", dbg_dir / f"{prefix}_timeout_try{i}.png")
        except Exception:
            page_dump(page, dbg_dir / f"{prefix}_exc_try{i}.html", dbg_dir / f"{prefix}_exc_try{i}.png")
    return False


def scroll_page(page, pause_s: float) -> None:
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(pause_s)


def page_has_end_of_results(page) -> bool:
    checks = [
        "text=End of results",
        "text=No results",
        "text=No more results",
    ]
    for sel in checks:
        try:
            if page.locator(sel).first.count() > 0:
                return True
        except Exception:
            pass
    return False
