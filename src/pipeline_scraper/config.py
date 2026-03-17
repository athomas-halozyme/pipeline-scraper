
from __future__ import annotations
from pydantic import BaseModel, HttpUrl, Field
from typing import List, Optional, Dict
import yaml
from pathlib import Path

from pydantic import BaseModel, HttpUrl, Field
from typing import List, Optional, Dict


class Partner(BaseModel):
    name: str = Field(..., description="Company name key, e.g., Roche")
    url: HttpUrl
    pdf: bool = False                   # for partners with PDF URL endpoints
    csv_url: Optional[HttpUrl] = None   # for partners with a real CSV endpoint
    csv_path: Optional[str] = None      # for local file testing
    csv_via_click: bool = False         # for partners with a clickable-only CSV link
    headers: Optional[Dict[str, str]] = None
    render_js: bool = False


class AppConfig(BaseModel):
    output_dir: str = 'data'
    user_agent: str = 'HalozymePipelineScraper (athomas@halozyme.com)'
    respect_robots: bool = True
    partners: List[Partner]

    @staticmethod
    def load(path: str | Path) -> 'AppConfig':
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return AppConfig(**data)
