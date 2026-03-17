from __future__ import annotations

import os
import re
import tempfile
from typing import List, Optional

import pandas as pd
import pdfplumber

from .base import BaseParser, PipelineRecord
from pipeline_scraper.pdf_extractors import camelot_read_tables_subprocess


# ------------------------------ Therapeutic area detection ------------------------------
TA_MAP = {
    "gastrointestinal and inflammation pipeline": "Gastrointestinal & Inflammation",
    "neuroscience pipeline": "Neuroscience",
    "oncology pipeline": "Oncology",
    "other rare diseases pipeline": "Other Rare Diseases",
    "plasma-derived therapies pipeline": "Plasma-Derived Therapies",
    "vaccines pipeline": "Vaccines",
}

def detect_ta_header(page_text: str) -> Optional[str]:
    t = (page_text or "").lower()
    for key, canon in TA_MAP.items():
        if key in t:
            return canon
    return None


# ------------------------------ Non-TA section header detection ------------------------------
NON_TA_HEADER_KEYWORDS = [
    "select options",
    "other selected assets",
    "contractual rights",
    "commercialize",
    "expected timelines",
    "appendix",
    "glossary",
    "footnotes",
    "definitions",
]

def has_non_ta_section_header(page_text: str) -> bool:
    if not page_text:
        return False
    first_line = next((ln.strip() for ln in (page_text.splitlines() or []) if ln.strip()), "")
    if not first_line:
        return False
    fl = first_line.lower()
    if detect_ta_header(page_text) is not None:
        return False
    if any(kw in fl for kw in NON_TA_HEADER_KEYWORDS):
        return True
    if (":" in first_line) and (len(first_line) >= 25):
        return True
    return False


# ------------------------------ Phase parsing ------------------------------
PHASE_PATTERNS = [
    (re.compile(r"(?i)P\s*-?III"), "Phase 3"),
    (re.compile(r"(?i)P\s*-?II(?!I)"), "Phase 2"),
    (re.compile(r"(?i)P\s*-?I(?!I)"), "Phase 1"),
    (re.compile(r"(?i)filed|registration|submission|nda|bla"), "Filed"),
    (re.compile(r"(?i)approved|marketed|launched"), "Approved"),
]

def extract_phase(s: str) -> str:
    if not s:
        return ""
    for pat, label in PHASE_PATTERNS:
        if pat.search(s):
            return label
    return ""


# ------------------------------ Drug name parsing ------------------------------
DEV_RE = re.compile(r"^(?P<dev>[^\n<]+)\s*(?P<gen><[^>]+>)?")

def extract_drug(raw_left: str) -> str:
    m = DEV_RE.match((raw_left or "").strip())
    if not m:
        return ""
    dev = (m.group("dev") or "").strip()
    gen = (m.group("gen") or "").strip()
    return (f"{dev} {gen}").strip()


# ------------------------------ Camelot via subprocess ------------------------------
def _extract_df_via_subprocess(pdf_path: str, page_no: int) -> Optional[pd.DataFrame]:
    """
    Attempt lattice → stream extraction for a single page via subprocess
    (to avoid Windows atexit cleanup noise). Uses your earlier tuning.
    """
    # Lattice first (your previous tweaks: line_scale, shift_text, strip_text)
    tbls = camelot_read_tables_subprocess(
        pdf_path,
        pages=str(page_no),
        flavor="lattice",
        backend="pdfium",
        use_fallback=False,
        extra_kwargs={
            "line_scale": 50,
            "strip_text": "\n",
            "shift_text": ["l", "t"],
        },
        # tmp_root can be set if you want child TEMP under output dir, e.g. Path(cfg.output_dir)/"camelot_tmp"
        tmp_root=None,
    )
    if tbls:
        data = tbls[0].get("data")
        if data:
            return pd.DataFrame(data)

    # Stream fallback (your edge_tol/strip_text)
    tbls = camelot_read_tables_subprocess(
        pdf_path,
        pages=str(page_no),
        flavor="stream",
        backend="pdfium",
        use_fallback=False,
        extra_kwargs={
            "edge_tol": 250,
            "strip_text": "\n",
        },
        tmp_root=None,
    )
    if tbls:
        data = tbls[0].get("data")
        if data:
            return pd.DataFrame(data)

    return None


def has_6_columns(df) -> bool:
    return df is not None and getattr(df, "shape", (0, 0))[1] >= 6


# ------------------------------ Parser -------------------------------------
class TakedaParser(BaseParser):
    name = "Takeda"

    def parse(self, payload: bytes, source_url: str) -> List[PipelineRecord]:
        if not isinstance(payload, (bytes, bytearray)):
            return []

        # Write PDF bytes to a temp file for the child
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(payload)
            tmp.flush()
            pdf_path = tmp.name

        records: List[PipelineRecord] = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)

                # 1) Find FIRST pipeline page (TA header + ≥6-col table)
                first_page = None
                for idx in range(total_pages):
                    page_text = pdf.pages[idx].extract_text() or ""
                    ta = detect_ta_header(page_text)
                    if ta is None:
                        continue
                    df = _extract_df_via_subprocess(pdf_path, idx + 1)
                    if not has_6_columns(df):
                        continue
                    first_page = idx
                    break

                if first_page is None:
                    return []

                # 2) Find LAST pipeline page (stop at first non-TA header)
                last_page = first_page
                last_ta = detect_ta_header(pdf.pages[first_page].extract_text() or "")
                for idx in range(first_page + 1, total_pages):
                    page_text = pdf.pages[idx].extract_text() or ""
                    if has_non_ta_section_header(page_text):
                        break
                    current_ta = detect_ta_header(page_text)
                    if current_ta is not None:
                        last_ta = current_ta
                    df = _extract_df_via_subprocess(pdf_path, idx + 1)
                    if not has_6_columns(df):
                        break
                    last_page = idx

                # 3) Extract rows across [first_page .. last_page]
                current_page_ta: Optional[str] = None
                for idx in range(first_page, last_page + 1):
                    page_no = idx + 1
                    page_text = pdf.pages[idx].extract_text() or ""
                    page_ta = detect_ta_header(page_text)
                    current_page_ta = page_ta or current_page_ta

                    df = _extract_df_via_subprocess(pdf_path, page_no)
                    if not has_6_columns(df):
                        continue

                    cols = list(df.columns)
                    df = df.rename(columns={
                        cols[0]: "left", cols[1]: "type", cols[2]: "mod",
                        cols[3]: "ind", cols[4]: "country", cols[5]: "stage",
                    })

                    df["left_orig"] = df["left"]
                    for c in ("left", "type", "mod"):
                        df[c] = df[c].replace("", None).ffill()

                    starts = df["left_orig"].fillna("").str.strip() != ""
                    grp = starts.cumsum()

                    df["stage"] = df["stage"].replace("", None)
                    df["stage"] = df.groupby(grp)["stage"].transform(lambda s: s.ffill().bfill())

                    df = df[df["stage"].astype(str).str.strip() != "Stage"]

                    for _, r in df.iterrows():
                        ind = (r["ind"] or "").strip()
                        if not ind:
                            continue
                        drug_name = extract_drug(str(r["left"]))
                        phase = extract_phase(str(r["stage"]))
                        records.append(
                            PipelineRecord(
                                company="Takeda",
                                drug_name=drug_name,
                                phase=phase,
                                indication=ind,
                                therapeutic_area=current_page_ta,
                                source_url=source_url or "",
                                scraped_at=""
                            )
                        )
        finally:
            try:
                os.remove(pdf_path)
            except Exception:
                pass

        return records