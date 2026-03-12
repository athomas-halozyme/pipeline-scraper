from __future__ import annotations

import re
from dataclasses import asdict
from datetime import datetime, timezone
from typing import List, Optional

from bs4 import BeautifulSoup

try:
    # Your codebase types
    from .base import BaseParser, PipelineRecord
except Exception:
    # Lightweight fallbacks so this module is directly runnable in isolation
    class BaseParser:  # type: ignore
        name: str
        def parse(self, payload: str, source_url: str):
            raise NotImplementedError
    from dataclasses import dataclass
    @dataclass
    class PipelineRecord:  # type: ignore
        company: str
        drug_name: Optional[str]
        phase: str
        indication: str
        therapeutic_area: str
        source_url: str
        scraped_at: str
        mechanism: Optional[str] = None
        raw: Optional[str] = None


class ArgenXParser(BaseParser):
    """
    HTML-first parser for Argenx Pipeline.

    It targets the DOM structure observed under the pipeline container,
    where each program is rendered in a `.block-content` block and each
    indication row appears as `.field--name-field-items > .field__item` with
    a width-encoded bar and a TA token in the bar span class.

    Key behaviors:
      • Program: prefer program logo <img alt> / filename; otherwise read a
        program token from the block text (e.g., Efgartigimod/ARGX-###/VYVGART Hytrulo).
      • Phase: derived from style="width:NN%" of `.item-bar` (desktop first),
        mapped to the unchanged labels: Preclinical, Phase 1, Proof of Concept,
        Registrational, Commercial.
      • Therapeutic Area: derived from `span.bar` class token
        `disease-phase--color-<token>` with normalization rules below.
      • Exclusions: any row whose color class corresponds to “indication-not-disclosed”.

    This mirrors the structure visible on the Argenx Pipeline page and its
    rendered HTML.
    """

    name = "ArgenX"

    PHASES = ["Preclinical", "Phase 1", "Proof of Concept", "Registrational", "Commercial"]

    # TA normalization. Note the product requirement: map the combined
    # hematology/rheumatology token to Hematology only.
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

    # Program tokens when no logo is present
    PROG_RX = re.compile(
        r"\b(Efgartigimod|Empasiprubart|Adimanebart|VYVGART(?: Hytrulo)?|ARGX[-_ ]?\d+|TSP[-_ ]?101)\b",
        re.I,
    )

    # Helpful aliases from filenames/alt text
    ALIASES = {
        "vyvgart": "VYVGART",
        "vyvgart-hytrulo": "VYVGART Hytrulo",
        "vyvgart_hytrulo": "VYVGART Hytrulo",
        "efgartigimod": "Efgartigimod",
    }

    def parse(self, payload: str, source_url: str) -> List[PipelineRecord]:
        if not isinstance(payload, str) or not payload.strip():
            return []

        soup = BeautifulSoup(payload, "lxml")

        # Scope to the pipeline container if present; otherwise search globally.
        root = soup.select_one("div.bootstrap-grid > div.pipeline") or soup

        # Each program is a `.block-content` block.
        blocks = root.select(".block-content") or soup.select(".block-content")
        scraped_at = datetime.now(timezone.utc).isoformat()

        out: List[PipelineRecord] = []
        for block in blocks:
            program = self._program_from_block(block)

            # Each indication row lives under `.field--name-field-items > .field__item`
            items = block.select(
                ".block-col-last .block-table .field--name-field-items > .field__item"
            )
            for it in items:
                indication = self._indication_from_item(it)
                if not indication:
                    continue

                phase = self._phase_from_item(it)
                ta_token, ta = self._ta_from_item(it)

                # Exclude ‘indication-not-disclosed’ rows entirely
                if ta_token == "indication-not-disclosed" or ta == "Undisclosed":
                    continue

                out.append(
                    PipelineRecord(
                        company="Argenx",
                        drug_name=program,
                        phase=phase,
                        indication=indication,
                        therapeutic_area=ta,
                        source_url=source_url or "",
                        scraped_at=scraped_at,
                        mechanism=None,
                        raw=None,
                    )
                )

        # Deduplicate conservatively
        seen = set()
        uniq: List[PipelineRecord] = []
        for r in out:
            key = (
                (r.company or "").lower(),
                (r.drug_name or "").lower(),
                (r.phase or "").lower(),
                (r.indication or "").lower(),
            )
            if key not in seen:
                seen.add(key)
                uniq.append(r)
        return uniq

    # ----------------- helpers -----------------

    def _program_from_block(self, block) -> Optional[str]:
        # Prefer program logo (handles VYVGART vs VYVGART Hytrulo)
        img = block.select_one(
            ".block-col-first .field--name-field-program-logo img[alt], "
            ".block-col-first .field--name-field-program-logo img[src]"
        )
        if img:
            alt = (img.get("alt") or "").strip()
            src = (img.get("src") or img.get("data-src") or "").strip()
            if "hytrulo" in src.lower() or "hytrulo" in alt.lower():
                return "VYVGART Hytrulo"
            if alt:
                if alt.lower() == "vyvgart":
                    return "VYVGART"
                key = alt.lower().replace(" ", "-").replace("_", "-")
                if key in self.ALIASES:
                    return self.ALIASES[key]
                return self._norm_prog(alt)
            if src:
                base = src.split("?")[0].split("/")[-1]
                stem = base.rsplit(".", 1)[0].lower()
                key = stem.replace(" ", "-").replace("_", "-")
                if key in self.ALIASES:
                    return self.ALIASES[key]
                if "vyvgart" in key:
                    return "VYVGART"
                m = re.match(r"(argx[-_ ]?\d+|tsp[-_ ]?101)", stem, re.I)
                if m:
                    return m.group(1).upper().replace(" ", "-").replace("_", "-")
                return self._norm_prog(stem)

        # Otherwise pick a token from block text
        m = self.PROG_RX.search(block.get_text(" ", strip=True))
        if m:
            return self._norm_prog(m.group(1))
        return None

    def _indication_from_item(self, item) -> Optional[str]:
        t = item.select_one(".item-title span")
        if t:
            txt = t.get_text(" ", strip=True)
            if txt:
                return txt
        return None

    def _phase_from_item(self, item) -> str:
        bar = (
            item.select_one(".item-bar.bar-desktop")
            or item.select_one(".item-bar.bar-mobile")
            or item.select_one(".item-bar")
        )
        width = None
        if bar:
            m = re.search(r"width\s*:\s*(\d+(?:\.\d+)?)\s*%", bar.get("style", ""))
            if not m and bar.parent:
                m = re.search(
                    r"width\s*:\s*(\d+(?:\.\d+)?)\s*%", bar.parent.get("style", "")
                )
            if m:
                width = float(m.group(1))
        return self._phase_from_width(width)

    def _ta_from_item(self, item) -> tuple[Optional[str], str]:
        bar = (
            item.select_one(".item-bar.bar-desktop")
            or item.select_one(".item-bar.bar-mobile")
            or item.select_one(".item-bar")
        )
        if not bar:
            return None, ""
        spanbar = bar.select_one("span.bar")
        if not spanbar:
            return None, ""
        cls = " ".join(spanbar.get("class", [])).lower()
        mm = re.search(r"disease-phase--color-([a-z0-9_-]+)", cls)
        if not mm:
            return None, ""
        token = mm.group(1).lower()
        ta = self.TA_MAP.get(token, token.title())
        return token, ta

    # -------- small utilities --------

    def _phase_from_width(self, pct: float | None) -> str:
        if pct is None:
            return ""
        try:
            p = float(pct)
        except Exception:
            return ""
        p = max(0.0, min(100.0, p))
        import math
        idx = min(5, max(1, math.ceil(p / 20.0)))
        return self.PHASES[idx - 1]

    def _norm_prog(self, tok: str | None) -> Optional[str]:
        if not tok:
            return None
        if re.match(r"argx[-_ ]?\d+", tok, re.I):
            return tok.upper().replace(" ", "-").replace("_", "-")
        if tok.lower().startswith("vyvgart"):
            return tok.replace("vyvgart", "VYVGART")
        return tok.title()