from __future__ import annotations
from typing import List, Optional
import csv, io
from bs4 import BeautifulSoup
from .base import BaseParser, PipelineRecord
from ..utils import normalize_phase

class RocheParser(BaseParser):
    name = 'Roche'

    def _looks_like_csv(self, text: str) -> bool:
        head = text.lstrip()[:256].lower()
        return head.startswith('compound,') or ('phase,' in head and ',' in head)

    def _parse_csv_text(self, text: str, source_url: str) -> List[PipelineRecord]:
        buf = io.StringIO(text)
        reader = csv.DictReader(buf)
        if not reader.fieldnames:
            return []
        # Normalize original headers → a lower() mapping
        fields = [f.strip() for f in reader.fieldnames]
        lower_map = {f.lower(): f for f in fields}
        # Validate it looks like Roche’s CSV (has a name column + phase)
        if not any(k in lower_map for k in ('compound', 'generic name', 'trade name')) or 'phase' not in lower_map:
            return []

        def get(row, key) -> Optional[str]:
            col = lower_map.get(key)
            if not col:
                return None
            val = row.get(col)
            if val is None:
                return None
            s = str(val).strip()
            return s or None

        seen = set()
        out: List[PipelineRecord] = []

        for row in reader:
            drug = get(row, 'trade name') or get(row, 'generic name') or get(row, 'compound') or get(row, 'combination')
            if not drug:
                continue
            phase_raw = get(row, 'phase')
            if not phase_raw:
                continue
            phase = normalize_phase(phase_raw)
            indication = get(row, 'indication')
            therapeutic_area = get(row, 'therapeutic area')

            key = (drug.lower(), phase.lower(), (indication or '').lower())
            if key in seen:
                continue
            seen.add(key)

            out.append(PipelineRecord(
                company=self.name,
                drug_name=drug,
                phase=phase,
                indication=indication,
                therapeutic_area=therapeutic_area,
                source_url=str(source_url),
                scraped_at=''
            ))
        return out

    def parse(self, html: str, url: str) -> List[PipelineRecord]:
        # If the input is actually CSV text (from csv_url or csv_path), parse it
        if self._looks_like_csv(html):
            recs = self._parse_csv_text(html, url)
            if recs:
                return recs

        # Otherwise, fall back to HTML (table) if the site returns HTML
        soup = BeautifulSoup(html, 'lxml')
        recs = self._parse_generic_table(self.name, html, url)
        return recs
