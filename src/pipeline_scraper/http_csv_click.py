from __future__ import annotations
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from pathlib import Path
import tempfile, os

def _dismiss_onetrust(page) -> None:
    """
    Dismiss OneTrust consent banner or preferences modal if present.
    Tries a sequence of common OneTrust selectors and waits until overlays stop intercepting clicks.
    """
    # Fast exits if already gone/hidden
    try:
        if page.locator("#onetrust-consent-sdk").count() == 0:
            return
    except Exception:
        pass

    # Try common acceptance / dismissal routes (best-effort, short timeouts to keep it snappy)
    selectors_in_order = [
        "#onetrust-accept-btn-handler",                            # Accept All
        "#onetrust-reject-all-handler",                            # Reject All (if configured)
        "button:has-text('Accept all')",
        "button:has-text('I Accept')",
        "button:has-text('Agree')",

        # If a preferences center is shown (dark filter), confirm/close it:
        ".save-preference-btn-handler",                            # Confirm my choices
        ".ot-pc-refuse-all-handler",                               # Reject all in preferences
        "button[aria-label='Close']",                              # Close the preferences modal
        ".ot-close-icon",                                          # Close X icon
    ]
    for sel in selectors_in_order:
        try:
            loc = page.locator(sel)
            if loc.first.is_visible():
                loc.first.click(timeout=1500)
                # Give the UI a moment to settle after each attempt
                page.wait_for_timeout(300)
        except Exception:
            pass

    # Wait until the overlay is gone or at least not intercepting clicks
    for blocker in ["#onetrust-consent-sdk", ".onetrust-pc-dark-filter"]:
        try:
            # Prefer hidden; if still in DOM, hidden is enough
            page.locator(blocker).wait_for(state="hidden", timeout=3000)
        except PWTimeout:
            # Try detached as a fallback; if still present/visible, we'll handle later
            try:
                page.locator(blocker).wait_for(state="detached", timeout=1500)
            except Exception:
                pass

def fetch_csv_by_click(url: str,
                       user_agent: str | None = None,
                       button_text: str = "Download current view as CSV",
                       wait_until: str = "networkidle",
                       navigation_timeout_ms: int = 60_000,
                       click_timeout_ms: int = 60_000) -> str:
    """
    Open the Roche pipeline page, dismiss OneTrust, click the CSV control,
    and return CSV text. Robust to client-side (Blob) downloads.
    """
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=user_agent or None, accept_downloads=True)
        page = ctx.new_page()
        page.set_default_timeout(30_000)

        # Navigate and wait for client rendering to settle
        page.goto(url, wait_until=wait_until, timeout=navigation_timeout_ms)

        # Handle OneTrust consent overlays
        _dismiss_onetrust(page)

        # Wait for the CSV control to render
        # We’ll resolve the clickable ancestor (<a> or <button>) for the span text.
        # 1) Try role-based locators
        loc = page.get_by_role("button", name=button_text)
        if not loc.count():
            loc = page.get_by_role("link", name=button_text)
        # 2) Fallback: nearest clickable ancestor of the span text
        if not loc.count():
            span = page.locator("span.text-link__text", has_text=button_text)
            if span.count():
                # resolve to closest <a> or <button>
                loc = span.first.locator("xpath=ancestor::*[self::a or self::button][1]")

        if not loc.count():
            raise RuntimeError(f"CSV control with text '{button_text}' not found.")

        # Ensure it’s in view (some UIs render it low on the page)
        try:
            loc.first.scroll_into_view_if_needed(timeout=2000)
        except Exception:
            pass

        # If OneTrust popped again for any reason, re-dismiss
        _dismiss_onetrust(page)

        # Click and capture the download (works for Blob and URL downloads)
        with page.expect_download(timeout=click_timeout_ms) as dl_info:
            loc.first.click()

        download = dl_info.value

        # Try to use Playwright's temp path directly (if available)
        tmp_path = download.path()
        if tmp_path is None:
            # Some drivers only materialize on save_as
            fd, tmp_file = tempfile.mkstemp(suffix=".csv")
            os.close(fd)
            download.save_as(tmp_file)
            csv_text = Path(tmp_file).read_text(encoding="utf-8", errors="replace")
            os.remove(tmp_file)
        else:
            csv_text = Path(tmp_path).read_text(encoding="utf-8", errors="replace")

        browser.close()
        return csv_text
