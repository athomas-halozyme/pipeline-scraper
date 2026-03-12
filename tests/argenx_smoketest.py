# tests/argenx_html_smoketest_v4.py
import re, json, math, time
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from bs4 import BeautifulSoup
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    USE_PLAYWRIGHT = True
except Exception:
    USE_PLAYWRIGHT = False

PIPELINE_URL = "https://argenx.com/pipeline"
COMPANY = "Argenx"
PHASES = ["Preclinical", "Phase 1", "Proof of Concept", "Registrational", "Commercial"]

# NOTE: "hematology-rheumatology" (and underscore variant) → Hematology
TA_MAP = {
    "neurology": "Neurology",
    "nephrology": "Nephrology",
    "hematology-rheumatology": "Hematology",
    "hematology_rheumatology": "Hematology",
    "hematology": "Hematology",
    "rheumatology": "Rheumatology",
    "endocrinology-ophthalmology": "Endocrinology / Ophthalmology",
    "endocrinology-opthalmology": "Endocrinology / Ophthalmology",
    "ophthalmology": "Endocrinology / Ophthalmology",
    "endocrinology": "Endocrinology / Ophthalmology",
    "indication-not-disclosed": "Undisclosed",
    "undisclosed": "Undisclosed",
}

PROG_RX = re.compile(r"\b(Efgartigimod|Empasiprubart|Adimanebart|VYVGART(?: Hytrulo)?|ARGX[-_ ]?\d+|TSP[-_ ]?101)\b", re.I)

ALIASES = {
    "vyvgart": "VYVGART",
    "vyvgart-hytrulo": "VYVGART Hytrulo",
    "vyvgart_hytrulo": "VYVGART Hytrulo",
    "efgartigimod": "Efgartigimod",
}

def normalize_phase(pct):
    if pct is None: return ""
    p = max(0.0, min(100.0, float(pct)))
    idx = min(5, max(1, math.ceil(p/20.0)))
    return PHASES[idx-1]

def norm_prog(tok):
    if not tok: return None
    if re.match(r"argx[-_ ]?\d+", tok, re.I):
        return tok.upper().replace(" ", "-").replace("_","-")
    if tok.lower().startswith("vyvgart"):
        return tok.replace("vyvgart","VYVGART")
    return tok.title()

def program_from_logo(block) -> str|None:
    img = block.select_one(
        ".block-col-first .field--name-field-program-logo img[alt], "
        ".block-col-first .field--name-field-program-logo img[src]"
    )
    if not img: return None
    alt = (img.get("alt") or "").strip()
    src = (img.get("src") or img.get("data-src") or "").strip()

    # Hytrulo detection via filename or alt
    if "hytrulo" in src.lower() or "hytrulo" in alt.lower():
        return "VYVGART Hytrulo"

    if alt:
        if alt.lower() == "vyvgart": return "VYVGART"
        key = alt.lower().replace(" ","-").replace("_","-")
        if key in ALIASES: return ALIASES[key]
        return norm_prog(alt)

    if src:
        base = src.split("?")[0].split("/")[-1]
        stem = base.rsplit(".",1)[0].lower()
        key = stem.replace(" ","-").replace("_","-")
        if key in ALIASES: return ALIASES[key]
        if "vyvgart" in key: return "VYVGART"
        m = re.match(r"(argx[-_ ]?\d+|tsp[-_ ]?101)", stem, re.I)
        if m: return m.group(1).upper().replace(" ","-").replace("_","-")
        return norm_prog(stem)
    return None

def parse_html(html: str):
    soup = BeautifulSoup(html, "lxml")
    blocks = soup.select(".block-content")
    records = []
    for block in blocks:
        # Program (logo or text fallback)
        prog = program_from_logo(block)
        if not prog:
            m = PROG_RX.search(block.get_text(" ", strip=True))
            if m:
                prog = norm_prog(m.group(1))

        # Indications under this program
        items = block.select(".block-col-last .block-table .field--name-field-items > .field__item")
        for it in items:
            t = it.select_one(".item-title span")
            indication = t.get_text(" ", strip=True) if t else None
            if not indication:
                continue

            bar = it.select_one(".item-bar.bar-desktop") or it.select_one(".item-bar.bar-mobile") or it.select_one(".item-bar")
            width = None
            if bar:
                m = re.search(r"width\s*:\s*(\d+(?:\.\d+)?)\s*%", bar.get("style",""))
                if not m and bar.parent:
                    m = re.search(r"width\s*:\s*(\d+(?:\.\d+)?)\s*%", bar.parent.get("style",""))
                if m:
                    width = float(m.group(1))
            phase = normalize_phase(width)

            ta_token, ta = None, ""
            if bar:
                spanbar = bar.select_one("span.bar")
                if spanbar:
                    cls = " ".join(spanbar.get("class", [])).lower()
                    mm = re.search(r"disease-phase--color-([a-z0-9_-]+)", cls)
                    if mm:
                        ta_token = mm.group(1).lower()
                        ta = TA_MAP.get(ta_token, ta_token.title())

            # Exclude undisclosed
            if ta_token == "indication-not-disclosed" or ta == "Undisclosed":
                continue

            records.append({
                "company": COMPANY,
                "drug_name": prog,
                "phase": phase,
                "source_url": PIPELINE_URL,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "indication": indication,
                "therapeutic_area": ta,
                "mechanism": None,
                "raw": None,
            })
    return records

def render_and_capture():
    with sync_playwright() as p:
        br = p.chromium.launch(headless=True)
        ctx = br.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36")
        )
        page = ctx.new_page()
        page.set_default_timeout(30000)

        page.goto(PIPELINE_URL, wait_until="domcontentloaded")

        # Cookie banner (best effort)
        for sel in ["button:has-text('Accept')","button:has-text('I agree')",
                    "button:has-text('Allow all')","button[aria-label*='accept' i]"]:
            try:
                if page.locator(sel).count()>0:
                    page.locator(sel).first.click(timeout=2000)
                    break
            except Exception:
                pass

        # Wait for pipeline container & scroll to hydrate
        try:
            page.wait_for_selector("div.bootstrap-grid >> div.pipeline", state="attached", timeout=15000)
        except PWTimeout:
            pass
        prev = 0
        for _ in range(15):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.5)
            cur = page.evaluate("document.body.scrollHeight")
            if cur == prev: break
            prev = cur

        html = page.content()
        Path("debug").mkdir(exist_ok=True)
        Path("debug/argenx_rendered.html").write_text(html, encoding="utf-8")
        page.close(); ctx.close(); br.close()
        return html

def main():
    if USE_PLAYWRIGHT:
        html = render_and_capture()
    else:
        html = Path("argenx_rendered.html").read_text(encoding="utf-8", errors="ignore")

    rows = parse_html(html)

    # Dedupe by (company, drug_name, phase, indication)
    seen, out = set(), []
    for r in rows:
        key = ((r["company"] or "").lower(),
               (r["drug_name"] or "").lower(),
               (r["phase"] or "").lower(),
               (r["indication"] or "").lower())
        if key not in seen:
            seen.add(key); out.append(r)

    with open("argenx_html_smoketest.jsonl","w",encoding="utf-8") as f:
        for r in out:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    pd.DataFrame(out).to_csv("argenx_html_smoketest_rows.csv", index=False)

    print(f"[OK] Parsed {len(out)} rows → argenx_html_smoketest.jsonl & argenx_html_smoketest_rows.csv")

if __name__ == "__main__":
    main()