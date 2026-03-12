from __future__ import annotations
import os
import re
import tempfile
import shutil
import time
import atexit
from typing import List, Optional

import camelot
import pdfplumber

from .base import BaseParser, PipelineRecord


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
    """
    Return the canonical TA if the page contains a TA header.
    """
    t = (page_text or "").lower()
    for key, canon in TA_MAP.items():
        if key in t:
            return canon
    return None


# ------------------------------ Non-TA section header detection ------------------------------

# Conservative keywords that have shown up on the non-pipeline section that follows the TA tables.
# Extend this list if Takeda introduces new section headers before/after the pipeline table.
NON_TA_HEADER_KEYWORDS = [
    "select options",                # "Select Options: Other Selected Assets ..."
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
    """
    Heuristic: treat the first non-empty line as a 'header candidate'.
    Return True if that line clearly signals a non-TA section header.
    """
    if not page_text:
        return False
    # First non-empty line
    first_line = next((ln.strip() for ln in (page_text.splitlines() or []) if ln.strip()), "")
    if not first_line:
        return False
    fl = first_line.lower()
    if detect_ta_header(page_text) is not None:
        return False  # It's a TA header, not a non-TA section

    # Keyword-based detection (safer than purely syntactic)
    if any(kw in fl for kw in NON_TA_HEADER_KEYWORDS):
        return True

    # Additional mild heuristic: headers often contain a colon and are reasonably long.
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


# ------------------------------ Camelot helper ------------------------------

def extract_df(pdf_path: str, page_no: int):
    """
    Attempt lattice → stream extraction for a single page.
    Return a DataFrame or None.
    """
    try:
        tabs = camelot.read_pdf(
            pdf_path,
            pages=str(page_no),
            flavor="lattice",
            line_scale=50,
            strip_text="\n",
            shift_text=["l", "t"],
        )
    except Exception:
        tabs = None

    if tabs and tabs.n > 0 and tabs[0].df is not None and tabs[0].df.shape[0] > 0:
        return tabs[0].df.copy()

    # fallback: stream
    try:
        tabs = camelot.read_pdf(
            pdf_path,
            pages=str(page_no),
            flavor="stream",
            edge_tol=250,
            strip_text="\n",
        )
    except Exception:
        tabs = None

    if tabs and tabs.n > 0 and tabs[0].df is not None and tabs[0].df.shape[0] > 0:
        return tabs[0].df.copy()

    return None


def has_6_columns(df) -> bool:
    return df is not None and df.shape[1] >= 6


# ------------------------------ Parser -------------------------------------

class TakedaParser(BaseParser):
    name = "Takeda"

    def parse(self, payload: bytes, source_url: str) -> List[PipelineRecord]:
        if not isinstance(payload, (bytes, bytearray)):
            return []

        # ---- Write payload → temp PDF (Camelot requires filesystem path) ----
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(payload)
            tmp.flush()
            pdf_path = tmp.name

        # ---- Create run-scoped temp dir (Windows temp cleanup mitigation) ----
        run_tmp = tempfile.mkdtemp(prefix="camelot_run_")
        prev_tmp_env = os.environ.get("TMP")
        prev_temp_env = os.environ.get("TEMP")
        prev_tmpdir = tempfile.tempdir

        os.environ["TMP"] = run_tmp
        os.environ["TEMP"] = run_tmp
        tempfile.tempdir = run_tmp

        records: List[PipelineRecord] = []
        try:
            with pdfplumber.open(pdf_path) as pdf:

                total_pages = len(pdf.pages)

                # ----------------------------------------------------------
                # 1) Identify FIRST pipeline page:
                #    Page must have a TA header AND a ≥6-column table
                # ----------------------------------------------------------
                first_page = None
                for idx in range(total_pages):
                    page_text = pdf.pages[idx].extract_text() or ""
                    ta = detect_ta_header(page_text)
                    if ta is None:
                        continue
                    df = extract_df(pdf_path, idx + 1)
                    if not has_6_columns(df):
                        continue
                    first_page = idx
                    break

                if first_page is None:
                    return []  # No pipeline content found

                # ----------------------------------------------------------
                # 2) Identify LAST pipeline page:
                #    From first_page+1, STOP at the first page that
                #    HAS A HEADER BUT THAT HEADER IS NOT A TA.
                #    (Exclude that page and everything after.)
                #    Continuation pages with NO header remain included.
                # ----------------------------------------------------------
                last_page = first_page  # inclusive
                last_ta = detect_ta_header(pdf.pages[first_page].extract_text() or "")

                for idx in range(first_page + 1, total_pages):
                    page_text = pdf.pages[idx].extract_text() or ""
                    # If we encounter a non-TA header ⇒ stop BEFORE this page
                    if has_non_ta_section_header(page_text):
                        break

                    # If there is a TA header, update last_ta (new TA section)
                    current_ta = detect_ta_header(page_text)
                    if current_ta is not None:
                        last_ta = current_ta

                    # Keep including continuation pages; require ≥6 columns to be parseable
                    df = extract_df(pdf_path, idx + 1)
                    if not has_6_columns(df):
                        # No table of interest; stop scanning
                        break

                    last_page = idx  # still part of the pipeline span

                # ----------------------------------------------------------
                # 3) Extract rows from pages [first_page .. last_page]
                # ----------------------------------------------------------
                current_page_ta: Optional[str] = None
                for idx in range(first_page, last_page + 1):
                    page_no = idx + 1
                    page_text = pdf.pages[idx].extract_text() or ""
                    page_ta = detect_ta_header(page_text)
                    # Carry TA forward on continuation pages
                    current_page_ta = page_ta or current_page_ta

                    df = extract_df(pdf_path, page_no)
                    if not has_6_columns(df):
                        continue

                    cols = list(df.columns)
                    df = df.rename(columns={
                        cols[0]: "left", cols[1]: "type", cols[2]: "mod",
                        cols[3]: "ind",  cols[4]: "country", cols[5]: "stage",
                    })

                    df["left_orig"] = df["left"]
                    for c in ("left", "type", "mod"):
                        df[c] = df[c].replace("", None).ffill()

                    # group by product blocks
                    starts = df["left_orig"].fillna("").str.strip() != ""
                    grp = starts.cumsum()

                    df["stage"] = df["stage"].replace("", None)
                    df["stage"] = df.groupby(grp)["stage"].transform(lambda s: s.ffill().bfill())

                    # remove header rows
                    df = df[df["stage"].astype(str).str.strip() != "Stage"]

                    for _, r in df.iterrows():
                        ind = (r["ind"] or "").strip()
                        if not ind:
                            continue

                        drug_name = extract_drug(str(r["left"]))
                        phase = extract_phase(str(r["stage"]))
                        rec = PipelineRecord(
                            company="Takeda",
                            drug_name=drug_name,
                            phase=phase,
                            indication=ind,
                            therapeutic_area=current_page_ta,   # TA carried across continuation pages
                            source_url=source_url or "",
                            scraped_at=""
                        )
                        records.append(rec)

        finally:
            # ---- Delay for Windows file lock release ----
            time.sleep(0.1)

            # ---- Remove atexit handlers referencing our temp dir ----
            try:
                handlers = list(getattr(atexit, "_exithandlers", []))
                keep = []
                for fn, targs, kargs in handlers:
                    try:
                        if fn is shutil.rmtree and targs:
                            target = os.path.abspath(str(targs[0]))
                            if target.startswith(os.path.abspath(run_tmp)):
                                continue
                    except Exception:
                        pass
                    keep.append((fn, targs, kargs))
                atexit._exithandlers = keep
            except Exception:
                pass

            # ---- Restore temp env ----
            if prev_tmp_env is not None:
                os.environ["TMP"] = prev_tmp_env
            if prev_temp_env is not None:
                os.environ["TEMP"] = prev_temp_env
            tempfile.tempdir = prev_tmpdir

            # ---- Cleanup our PDF ----
            try:
                os.remove(pdf_path)
            except Exception:
                pass

            # ---- Cleanup run-scoped temp ----
            try:
                shutil.rmtree(run_tmp, ignore_errors=True)
            except Exception:
                pass

        return records