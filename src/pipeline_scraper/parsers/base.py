# src/pipeline_scraper/parsers/base.py

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from ..utils import normalize_phase

@dataclass
class PipelineRecord:
    company: str
    drug_name: str
    phase: str
    source_url: str
    scraped_at: str
    indication: str | None = None
    therapeutic_area: str | None = None
    mechanism: str | None = None
    raw: Dict[str, Any] | None = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _json_sanitize(obj: Any) -> Any:
    """
    Recursively convert objects that the JSON encoder can't handle
    (like Pydantic HttpUrl, Path, datetime, etc.) into serializable
    primitives (str, int, float, bool, None, list, dict).
    """
    # Lazy import to avoid hard dependency
    try:
        from pydantic.networks import AnyHttpUrl, HttpUrl
        http_url_types = (AnyHttpUrl, HttpUrl)
    except Exception:
        http_url_types = tuple()

    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, http_url_types):
        return str(obj)
    if isinstance(obj, (list, tuple)):
        return [_json_sanitize(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _json_sanitize(v) for k, v in obj.items()}

    # Fallback: best-effort string
    return str(obj)


class BaseParser:
    name: str  # company key

    def parse(self, html: str, url: str) -> List[PipelineRecord]:
        raise NotImplementedError

    def _parse_generic_table(self, company: str, html: str, url: str) -> List[PipelineRecord]:
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table')
        results: List[PipelineRecord] = []

        for table in tables:
            # ---- Build header index
            headers: list[str] = []
            thead = table.find('thead')
            if thead:
                headers = [th.get_text(strip=True) for th in thead.find_all('th')]
            else:
                first_row = table.find('tr')
                if first_row:
                    headers = [th.get_text(strip=True) for th in first_row.find_all(['th', 'td'])]

            if not headers:
                continue

            # ---- Map interesting columns
            col_map = {h.lower(): idx for idx, h in enumerate(headers)}
            drug_col = None
            phase_col = None
            indication_col = None

            for key, idx in col_map.items():
                if any(x in key for x in ['drug', 'compound', 'asset', 'product', 'molecule', 'name']):
                    if drug_col is None:
                        drug_col = idx
                if 'phase' in key or 'development' in key:
                    if phase_col is None:
                        phase_col = idx
                if any(x in key for x in ['indication', 'disease', 'condition', 'therapy area', 'therapeutic area']):
                    if indication_col is None:
                        indication_col = idx

            if drug_col is None or phase_col is None:
                # Not a table we can use
                continue

            # ---- Iterate data rows
            body_rows = table.find_all('tr')
            # If there's no <thead>, the first row might be headers—skip if it has <th>
            start_idx = 1 if (body_rows and body_rows[0].find('th')) else 0

            for tr in body_rows[start_idx:]:
                tds = tr.find_all('td')
                if not tds:
                    continue

                # Build the row "cells"
                cells = [td.get_text(strip=True) for td in tds]

                # Guard against short rows
                max_needed = max(drug_col, phase_col, indication_col if indication_col is not None else 0)
                if len(cells) <= max_needed:
                    continue

                # Extract fields
                drug = cells[drug_col].strip()
                phase_raw = cells[phase_col].strip()
                phase = normalize_phase(phase_raw)
                indication = None
                if indication_col is not None and len(cells) > indication_col:
                    ind = cells[indication_col].strip()
                    indication = ind or None

                if drug:
                    results.append(
                        PipelineRecord(
                            company=company,
                            drug_name=drug,
                            phase=phase,
                            indication=indication,
                            source_url=str(url),
                            scraped_at=''
                        )
                    )

        return results