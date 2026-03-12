from __future__ import annotations
from typing import List, Optional
from bs4 import BeautifulSoup, Tag
from .base import BaseParser, PipelineRecord
from ..utils import normalize_phase

THERAPEUTIC_AREAS = {"oncology", "immunology", "neuroscience", "select other areas"}

class JnJParser(BaseParser):
    name = 'JnJ'

    def _nearest_therapeutic_area(self, node: Tag) -> Optional[str]:
        """
        Walk backward in document order to find the closest preceding H2/H3 heading
        whose text matches a known therapy area. If none found, return None.
        """
        # Prefer ancestors that contain a heading
        parent = node
        for _ in range(6):
            if not isinstance(parent, Tag):
                break
            # Look for a heading within this ancestor
            for h in parent.find_all(['h2', 'h3'], recursive=False):
                t = (h.get_text(strip=True) or '').strip()
                tl = t.lower()
                if tl in THERAPEUTIC_AREAS:
                    return t
            parent = parent.parent

        # Fallback: previous headings in the DOM
        for h in node.find_all_previous(['h2', 'h3']):
            t = (h.get_text(strip=True) or '').strip()
            tl = t.lower()
            if tl in THERAPEUTIC_AREAS:
                return t
        return None

    def parse(self, html: str, url: str) -> List[PipelineRecord]:
        soup = BeautifulSoup(html, 'lxml')
        scope = soup.find('main') or soup

        records: List[PipelineRecord] = []
        seen = set()

        # Each entry is a card like:
        # <li class="pipeline-area_card" data-phase="Registration" ...>
        #   <h3 class="pipeline-area_card-title h4">AKEEGA ...</h3>
        #   <p class="pipeline-area_card-description">Indication</p>
        #   <p class="pipeline-area_card-phase">Registration</p>
        # </li>
        for card in scope.select('li.pipeline-area_card'):
            # Name
            title_el = card.select_one('h3.pipeline-area_card-title')
            drug = title_el.get_text(" ", strip=True) if title_el else None
            if not drug:
                continue

            # Phase: prefer data attribute, else text content
            phase_attr = (card.get('data-phase') or '').strip()
            phase_el = card.select_one('p.pipeline-area_card-phase')
            phase_text = phase_attr or (phase_el.get_text(strip=True) if phase_el else '')
            if not phase_text:
                continue
            phase = normalize_phase(phase_text)

            # Indication (optional)
            ind_el = card.select_one('p.pipeline-area_card-description')
            indication = ind_el.get_text(" ", strip=True) if ind_el else None

            # Therapy area from nearest heading
            therapeutic_area = self._nearest_therapeutic_area(card)

            key = (drug.strip().lower(), phase.strip().lower(), (indication or '').strip().lower())
            if key in seen:
                continue
            seen.add(key)

            records.append(PipelineRecord(
                company=self.name,
                drug_name=drug.strip(),
                phase=phase,
                indication=indication or None,
                therapeutic_area=therapeutic_area,
                source_url=str(url),
                scraped_at=''  # filled by CLI
            ))

        return records