
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict

_PHASE_MAP = {
    'discovery': 'Discovery',
    'research': 'Discovery',
    'preclinical': 'Preclinical',
    'phase i': 'Phase 1',
    'phase 1': 'Phase 1',
    'phase 1/2': 'Phase 1/2',
    'phase i/ii': 'Phase 1/2',
    'phase ii': 'Phase 2',
    'phase 2': 'Phase 2',
    'phase 2/3': 'Phase 2/3',
    'phase ii/iii': 'Phase 2/3',
    'phase iii': 'Phase 3',
    'phase 3': 'Phase 3',
    'pivotal': 'Phase 3',
    'registration': 'Filed',
    'filed': 'Filed',
    'submitted': 'Filed',
    'approved': 'Approved',
    'marketed': 'Approved',
    'on market': 'Approved',
    'discontinued': 'Discontinued',
    'paused': 'Paused',
}

_PHASE_PATTERN = re.compile(r"phase\s*(i{1,3}|1(?:/2)?|2(?:/3)?|3)", re.I)


def normalize_phase(text: str) -> str:
    if not text:
        return 'Unknown'
    s = str(text).strip().lower()
    if s in _PHASE_MAP:
        return _PHASE_MAP[s]
    # direct match on known keys
    for k, v in _PHASE_MAP.items():
        if k in s:
            return v
    m = _PHASE_PATTERN.search(s)
    if m:
        token = m.group(0).lower().replace(' ', '')
        token = token.replace('phase', 'phase ')
        token = token.replace('i', 'I')  # normalize roman only visually
        token = token.replace('1/2', '1/2').replace('2/3', '2/3')
        # map again
        return _PHASE_MAP.get(token.lower(), token.title())
    return s.title()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def shape_records_for_output(
    records: List[Dict],
    *,
    drop: tuple[str, ...] = ("mechanism", "raw"),
    front: tuple[str, ...] = ("company", "drug_name", "phase", "indication", "therapeutic_area"),
    tail: tuple[str, ...] = ("source_url", "scraped_at"),
) -> List[Dict]:
    """
    Remove specific columns and reorder fields so that `front` fields come first,
    everything else stays in the middle (preserving relative order), and `tail`
    fields are appended at the end.

    Defaults:
      - drop: ('mechanism', 'raw')
      - front: ('company','drug_name','phase','indication','therapeutic_area')
      - tail: ('source_url','scraped_at')
    """
    if not records:
        return records

    shaped: List[Dict] = []
    for r in records:
        # 1) Drop unwanted keys
        pruned = {k: v for k, v in r.items() if k not in drop}

        # 2) Build ordered dict: front → middle → tail
        ordered: Dict = {}

        # front
        for k in front:
            if k in pruned:
                ordered[k] = pruned[k]

        # middle (everything not in front/tail, keep insertion order)
        for k, v in pruned.items():
            if k not in ordered and k not in tail:
                ordered[k] = v

        # tail
        for k in tail:
            if k in pruned:
                ordered[k] = pruned[k]

        shaped.append(ordered)

    return shaped
