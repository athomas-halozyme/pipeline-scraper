from __future__ import annotations
import time, requests
from typing import Optional, Dict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from urllib.parse import urlparse
from urllib import robotparser

class FetchError(Exception): ...

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(FetchError))
def fetch_html(url: str,
               user_agent: str,
               respect_robots: bool = True,
               timeout: int = 20,
               extra_headers: Optional[Dict[str, str]] = None) -> str:
    url = str(url)
    if respect_robots:
        rp = robotparser.RobotFileParser()
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        try:
            rp.set_url(robots_url); rp.read()
            if not rp.can_fetch(user_agent, url):
                raise FetchError(f"Blocked by robots.txt: {url}")
        except Exception:
            pass

    # Use a *browser-like* UA if the config UA still looks synthetic
    ua = user_agent or ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36")
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
        "Upgrade-Insecure-Requests": "1",
    }
    if extra_headers:
        headers.update(extra_headers)

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code >= 400:
            raise FetchError(f"HTTP {resp.status_code} for {url}")
        time.sleep(1)
        return resp.text
    except requests.RequestException as e:
        raise FetchError(str(e))
    
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(FetchError))   
def fetch_html_with_session(url: str,
                            user_agent: str,
                            warmup_url: Optional[str] = None,
                            respect_robots: bool = True,
                            timeout: int = 20,
                            extra_headers: Optional[Dict[str, str]] = None) -> str:
    url = str(url)
    ua = (user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36")
    base_headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "close",
    }
    if extra_headers:
        base_headers.update(extra_headers)

    s = requests.Session()
    s.headers.update(base_headers)

    if warmup_url:
        try:
            s.get(warmup_url, timeout=timeout)
            time.sleep(0.8)
        except requests.RequestException:
            pass

    try:
        r = s.get(url, timeout=timeout)
        if r.status_code >= 400:
            raise FetchError(f"HTTP {r.status_code} for {url}")
        time.sleep(1)
        return r.text
    except requests.RequestException as e:
        raise FetchError(str(e))
    

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8),
       retry=retry_if_exception_type(FetchError))  
def fetch_html_rendered(url: str, timeout: int = 30, user_agent: Optional[str] = None) -> str:
    from playwright.sync_api import sync_playwright
    url = str(url)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=(user_agent or None))
        page = ctx.new_page()
        page.goto(url, wait_until="networkidle", timeout=timeout*1000)
        html = page.content()
        browser.close()
    return html

