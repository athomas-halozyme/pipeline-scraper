from __future__ import annotations
import requests
from bs4 import BeautifulSoup
from typing import Optional
from ..http import FetchError

SCIENCE_URL = "https://www.takeda.com/science/pipeline/"

def discover_pipeline_pdf(discovery_page: str, user_agent: Optional[str] = None, timeout: int = 30) -> str:
    headers = {
        "User-Agent": user_agent or ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"),
        "Accept": "text/html,*/*;q=0.8",
        "Referer": "https://www.takeda.com/",
        "Connection": "close",
    }
    try:
        r = requests.get(discovery_page, headers=headers, timeout=timeout)
        if r.status_code >= 400:
            raise FetchError(f"HTTP {r.status_code} for {discovery_page}")
    except requests.RequestException as e:
        raise FetchError(str(e))

    soup = BeautifulSoup(r.text, "html.parser")
    # Prefer links to assets-dam.takeda.com that end with .pdf
    best = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower():
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://www.takeda.com" + href
            # Prefer the CDN host if present
            if "assets-dam.takeda.com" in href:
                best = href
                break
            if not best:  # keep a fallback .pdf
                best = href

    if not best:
        raise FetchError("No .pdf link found on Takeda science/pipeline page")
    return best