
import io, re, tempfile, os, json
import requests
import pandas as pd
import camelot
import pdfplumber

PDF_URL = "https://assets-dam.takeda.com/image/upload/v1769652638/Global/Investor/Financial-Results/FY2025/Q3/qr2025_q3_Pipeline_table_en.pdf"

TA_MAP = {
    "gastrointestinal and inflammation pipeline": "Gastrointestinal & Inflammation",
    "neuroscience pipeline": "Neuroscience",
    "oncology pipeline": "Oncology",
    "other rare diseases pipeline": "Other Rare Diseases",
    "plasma-derived therapies pipeline": "Plasma-Derived Therapies",
    "vaccines pipeline": "Vaccines",
}

PHASE_PATTERNS = [
    (re.compile(r"(?i)P\s*-?III"), "Phase 3"),
    (re.compile(r"(?i)P\s*-?II(?!I)"), "Phase 2"),
    (re.compile(r"(?i)P\s*-?I(?!I)"), "Phase 1"),
    (re.compile(r"(?i)filed|registration|submission|nda|bla"), "Filed"),
    (re.compile(r"(?i)approved|marketed|launched"), "Approved"),
]

DEV_RE = re.compile(r"^(?P<dev>[^\n<]+)\s*(?P<gen><[^>]+>)?")

def detect_ta(page_text: str, last_ta: str|None) -> str|None:
    t = (page_text or "").lower()
    for k, v in TA_MAP.items():
        if k in t:
            return v
    return last_ta

def extract_phase(stage_text: str) -> str:
    s = stage_text or ""
    for pat, lab in PHASE_PATTERNS:
        if pat.search(s):
            return lab
    return ""

def make_drug_name(raw_left: str) -> str:
    if not isinstance(raw_left, str):
        return ""
    m = DEV_RE.match(raw_left.strip())
    if not m: return ""
    dev = (m.group("dev") or "").strip()
    gen = (m.group("gen") or "").strip()  # keep angle brackets
    return (f"{dev} {gen}").strip()

def main():
    print("[SMOKE] Downloading Takeda pipeline PDF…")
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf,*/*;q=0.8",
        "Referer": "https://www.takeda.com/science/pipeline/"
    }
    r = requests.get(PDF_URL, headers=headers, timeout=60)
    r.raise_for_status()
    content = r.content
    print(f"[SMOKE] Bytes: {len(content)}; magic: {content[:5]!r}")
    if not content.lstrip().startswith(b"%PDF-"):
        raise SystemExit("[FAIL] Response is not a PDF.")

    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp.write(content)
    tmp.flush()
    pdf_path = tmp.name
    tmp.close()
    print(f"[SMOKE] Temp PDF: {pdf_path}")

    try:
        rows = []
        with pdfplumber.open(pdf_path) as pdf:
            total = len(pdf.pages)
            print(f"[SMOKE] Total pages: {total}")
            last_ta = None
            start, end = 3, min(total-1, 9)  # pages 4..10
            for page_idx in range(start, end+1):
                page_no = page_idx + 1
                text = pdf.pages[page_idx].extract_text() or ""
                ta = detect_ta(text, last_ta)
                last_ta = ta

                # Camelot lattice first
                print(f"[SMOKE] Page {page_no}: lattice…", end="")
                try:
                    tabs = camelot.read_pdf(
                        pdf_path,
                        pages=str(page_no),
                        flavor="lattice",
                        line_scale=50,
                        strip_text="\n",
                        shift_text=["l","t"],
                    )
                except Exception as e:
                    print(f" error: {e}")
                    tabs = None

                df = None
                if tabs and tabs.n > 0 and tabs[0].df is not None and tabs[0].df.shape[0] > 0:
                    df = tabs[0].df.copy()
                    print(f" OK shape={df.shape}")
                else:
                    print(" none; stream…", end="")
                    try:
                        tabs = camelot.read_pdf(
                            pdf_path,
                            pages=str(page_no),
                            flavor="stream",
                            edge_tol=250,
                            strip_text="\n",
                        )
                    except Exception as e:
                        print(f" error: {e}")
                        tabs = None

                    if tabs and tabs.n > 0 and tabs[0].df is not None and tabs[0].df.shape[0] > 0:
                        df = tabs[0].df.copy()
                        print(f" OK shape={df.shape}")
                    else:
                        print(" none")
                        continue

                if df.shape[1] < 6:
                    print(f"[SMOKE] Page {page_no}: unexpected columns={df.shape[1]} (need >=6)")
                    continue

                cols = list(df.columns)
                df = df.rename(columns={
                    cols[0]: "left", cols[1]: "type", cols[2]: "mod",
                    cols[3]: "ind",  cols[4]: "country", cols[5]: "stage",
                })

                df["left_orig"] = df["left"]
                for c in ("left","type","mod"):
                    df[c] = df[c].replace("", None).ffill()

                starts = df["left_orig"].fillna("").str.strip() != ""
                grp = starts.cumsum()
                df["stage"] = df["stage"].replace("", None)
                df["stage"] = df.groupby(grp)["stage"].transform(lambda s: s.ffill().bfill())
                df = df[df["stage"].astype(str).str.strip() != "Stage"]

                page_rows_before = len(rows)
                for _, r0 in df.iterrows():
                    ind = str(r0["ind"]).strip()
                    if not ind: continue
                    drug = make_drug_name(str(r0["left"]))
                    phase = extract_phase(str(r0["stage"]))
                    rows.append({
                        "drug_name": drug,
                        "phase": phase,
                        "indication": ind,
                        "therapeutic_area": ta
                    })
                print(f"[SMOKE] Page {page_no}: +{len(rows)-page_rows_before} rows")

        print(f"[SMOKE] TOTAL ROWS: {len(rows)}")
        pd.DataFrame(rows).to_csv("takeda_smoketest_rows.csv", index=False)
        with open("takeda_smoketest.jsonl","w",encoding="utf-8") as f:
            for rec in rows:
                f.write(json.dumps({
                    "company": "Takeda",
                    "drug_name": rec["drug_name"],
                    "phase": rec["phase"],
                    "source_url": PDF_URL,
                    "scraped_at": None,
                    "indication": rec["indication"],
                    "therapeutic_area": rec["therapeutic_area"],
                    "mechanism": None,
                    "raw": None
                }, ensure_ascii=False) + "\n")
        print("[SMOKE] Wrote takeda_smoketest_rows.csv and takeda_smoketest.jsonl")

    finally:
        try:
            os.remove(pdf_path)
        except Exception:
            pass

if __name__ == "__main__":
    main()
