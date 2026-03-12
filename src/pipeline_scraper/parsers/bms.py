from __future__ import annotations
from typing import List, Dict, Any, Optional, Iterable
from bs4 import BeautifulSoup
from html import unescape as html_unescape
import json, re
from pathlib import Path

from .base import BaseParser, PipelineRecord
from ..utils import normalize_phase


class BMSParser(BaseParser):
    name = 'BMS'

    def __init__(self):
        self._debug_dir: Optional[Path] = None

    # (Optional) Called by CLI when --debug is used
    def set_debug_dir(self, debug_dir: Optional[Path]):
        self._debug_dir = debug_dir

    # ----------------------------
    # Embedded JSON helpers
    # ----------------------------
    def _extract_pipeline_json_text(self, html: str) -> Optional[str]:
        """
        Extract raw JSON stored in an element with id='pipeline-data'.
        Works for <script type="application/json" id="pipeline-data">...</script>
        and for generic tags (div) with JSON text content.
        """
        soup = BeautifulSoup(html, 'lxml')
        tag = soup.find(id='pipeline-data')
        if not tag:
            return None

        raw = (tag.string or tag.get_text() or "").strip()
        if not raw:
            # Some sites stash it in a data-* attribute
            for attr in ('data-json', 'data-state', 'data-props', 'data-content'):
                if attr in tag.attrs and tag.attrs[attr]:
                    raw = str(tag.attrs[attr]).strip()
                    break
        if not raw:
            return None

        # Unescape HTML entities and remove simple HTML comment wrappers
        txt = html_unescape(raw)
        txt = re.sub(r'^\s*<!--', '', txt)
        txt = re.sub(r'-->\s*$', '', txt)
        return txt.strip() or None

    def _first_json_snippet(self, text: str) -> Optional[str]:
        """
        If extra text surrounds the JSON, extract the first complete
        top-level JSON object/array using a simple brace/bracket balance.
        """
        starts = [m.start() for m in re.finditer(r'[\{\[]', text)]
        for st in starts:
            stack = []
            for i in range(st, len(text)):
                ch = text[i]
                if ch == '{':
                    stack.append('}')
                elif ch == '[':
                    stack.append(']')
                elif stack and ch == stack[-1]:
                    stack.pop()
                    if not stack:
                        return text[st:i+1]
        return None

    def _load_payload(self, html: str) -> Optional[Dict[str, Any]]:
        raw = self._extract_pipeline_json_text(html)
        if not raw:
            return None

        # Try decoding as-is
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

        # Try extracting the first JSON object/array inside
        snippet = self._first_json_snippet(raw)
        if snippet:
            try:
                return json.loads(snippet)
            except json.JSONDecodeError:
                pass

        # Last resort: replace common HTML entity leftovers
        cleaned = raw.replace('&quot;', '"').replace('&#34;', '"')
        try:
            return json.loads(cleaned)
        except Exception:
            return None

    # ----------------------------
    # Lookup table builders
    # ----------------------------
    def _build_phase_map(self, payload: Dict[str, Any]) -> Dict[str, str]:
        """
        Map phase code -> phase label, e.g., 'bms:phase/phase-3' -> 'Phase 3'
        """
        phase_map: Dict[str, str] = {}
        for row in payload.get('phase', []) or []:
            code = str(row.get('value') or '').strip()
            name = str(row.get('name') or '').strip()
            if code and name:
                phase_map[code] = name
        return phase_map

    def _build_therapeutic_maps(self, payload: Dict[str, Any]) -> tuple[Dict[str, str], Dict[str, str]]:
        """
        Returns:
          category_map: 'bms:therapeutiic-area/oncology' -> 'Oncology'
          subcat_map:   'bms:tumor/1l-non-small-cell-lung-cancer' -> '1L Non-Small Cell Lung Cancer'
        """
        category_map: Dict[str, str] = {}
        subcat_map: Dict[str, str] = {}

        for ta in payload.get('therapeuticarea', []) or []:
            cat_code = str(ta.get('name') or '').strip()
            cat_label = str(ta.get('value') or '').strip()
            if cat_code and cat_label:
                category_map[cat_code] = cat_label
            for entry in (ta.get('list') or []):
                sc_code = str(entry.get('name') or '').strip()
                sc_label = str(entry.get('value') or '').strip()
                if sc_code and sc_label:
                    subcat_map[sc_code] = sc_label

        return category_map, subcat_map

    # ----------------------------
    # Utilities
    # ----------------------------
    def _clean_html_text(self, html_text: str) -> str:
        """
        Turn '<p><b>milvexian</b></p>' or anchor/sup variants into plain text.
        """
        try:
            return BeautifulSoup(html_text, 'lxml').get_text(" ", strip=True)
        except Exception:
            return html_text

    def parse(self, html: str, url: str) -> List[PipelineRecord]:
        payload = self._load_payload(html)

        # Optional: dump parsed payload when --debug is used
        if self._debug_dir is not None:
            try:
                dump_path = self._debug_dir / "BMS_pipeline.json"
                dump_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2) if payload is not None else "null",
                    encoding="utf-8"
                )
            except Exception:
                pass

        if not payload:
            # If JSON isn't available (JS-only and not embedded), we won't get rows.
            return []

        phase_map = self._build_phase_map(payload)               # code -> name (e.g., Registration)
        category_map, subcat_map = self._build_therapeutic_maps(payload)  # code -> label

        records: List[PipelineRecord] = []
        seen = set()

        for item in payload.get('listings', []) or []:
            # --- Drug / compound name
            raw_name = (item.get('compoundname') or '').strip()
            if not raw_name:
                continue
            drug = self._clean_html_text(html_unescape(raw_name))
            if not drug or drug.lower() in {'americas', 'in the pipeline', 'our pipeline at a glance'}:
                continue

            # --- Phase via phaseTag -> phase_map -> normalize
            phase_code = (item.get('phaseTag') or '').strip()
            phase_label = phase_map.get(phase_code, '').strip()
            if not phase_label:
                # fall back to the literal code text if no match
                phase_label = phase_code.split('/')[-1].replace('-', ' ').title()
            phase = normalize_phase(phase_label)  # maps 'Registration' -> 'Filed', etc.

            # --- Indication via subcategory map (preferred), else researcharea
            subcat_code = (item.get('subcategory') or '').strip()
            indication = subcat_map.get(subcat_code)
            if not indication:
                # researcharea often carries a short disease text in <p>...</p>
                research_html = (item.get('researcharea') or '').strip()
                if research_html:
                    indication = self._clean_html_text(html_unescape(research_html)) or None
            if indication:
                # remove stray asterisks used as footnote markers in values
                indication = indication.replace(' *', '').replace('*', '').strip() or None

            # --- Therapy area via category map (nice to have)
            cat_code = (item.get('category') or '').strip()
            therapeutic_area = category_map.get(cat_code)
            if therapeutic_area:
                therapeutic_area = therapeutic_area.replace(' *', '').replace('*', '').strip() or None

            # --- Compose, with indication-aware dedupe
            key = (drug.lower(), phase.lower(), (indication or '').lower())
            if key in seen:
                continue
            seen.add(key)

            records.append(PipelineRecord(
                company=self.name,
                drug_name=drug,
                phase=phase,
                indication=indication,
                therapeutic_area=therapeutic_area,
                source_url=str(url),
                scraped_at=''
            ))

        return records