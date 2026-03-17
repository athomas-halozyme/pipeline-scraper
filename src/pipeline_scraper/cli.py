from __future__ import annotations
import argparse
from pathlib import Path
from typing import List
import json
import pandas as pd
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from tqdm import tqdm


from .config import AppConfig
from .http import fetch_html, fetch_html_with_session, fetch_html_rendered, FetchError
from .registry import get_parser
from .utils import utc_now_iso, shape_records_for_output
from .http_csv_click import fetch_csv_by_click
from .discovery.takeda import discover_pipeline_pdf


def write_output(records: List[dict], path: Path, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    # Normalize + reorder columns globally
    records = shape_records_for_output(records)

    if fmt == 'csv':
        df = pd.DataFrame(records)
        cols = list(records[0].keys()) if records else []
        if cols:
            df = df[cols]
        df.to_csv(path, index=False)

    elif fmt in {'jsonl', 'ndjson'}:
        with open(path, 'w', encoding='utf-8') as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + '\n')
    else:
        raise ValueError(f"Unsupported format: {fmt}")



def main():
    parser = argparse.ArgumentParser(description='Pharma pipeline scraper')
    parser.add_argument('--config', required=True, help='Path to YAML config')
    parser.add_argument('--partners', nargs='*', help='Subset of partners to run (by name)')
    parser.add_argument('--out', required=False, help='Output file path; overrides config.output_dir default naming')
    parser.add_argument('--format', default='csv', choices=['csv', 'jsonl'], help='Output format (csv or jsonl)')
    parser.add_argument('--debug', action='store_true', help='Dump fetched HTML/JSON/PDF for troubleshooting')
    args = parser.parse_args()

    cfg = AppConfig.load(args.config)
    selected = set(p.name for p in cfg.partners)
    if args.partners:
        requested = set(args.partners)
        unknown = requested - selected
        if unknown:
            raise SystemExit(f"Unknown partners in --partners: {sorted(unknown)}")
        selected = requested
    
    all_records: List[dict] = []
    now = utc_now_iso()

    debug_dir = Path(cfg.output_dir) / "debug" if args.debug else None
    if debug_dir:
        debug_dir.mkdir(parents=True, exist_ok=True)

    partners_to_run = [p for p in cfg.partners if p.name in selected]
    pbar = tqdm(partners_to_run, desc="Scraping partners", unit="partner")
    

    for p in pbar:
        if p.name not in selected:
            continue

        payload = None
        source_url = None

        try:
            # ------------------------ Takeda (PDF discovery) ------------------------
            if (p.name == "Takeda") and (getattr(p, "pdf_discovery", True)):
                # 1) discover live PDF url from science/pipeline page (or configured discovery_page)
                pdf_url = discover_pipeline_pdf(
                    getattr(p, "discovery_page", None) or str(getattr(p, "url", "https://www.takeda.com/science/pipeline/")),
                    cfg.user_agent
                )
                source_url = pdf_url  # pass through into records/writer

                # 2) download latest & overwrite the single cached PDF
                ua = cfg.user_agent or ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36")
                r = requests.get(
                    pdf_url,
                    headers={
                        "User-Agent": ua,
                        "Accept": "application/pdf,*/*;q=0.8",
                        "Referer": getattr(p, "discovery_page", None) or str(getattr(p, "url", "")) or "https://www.takeda.com/science/pipeline/",
                        "Connection": "close",
                    },
                    timeout=60
                )
                if r.status_code >= 400:
                    raise FetchError(f"HTTP {r.status_code} for {pdf_url}")

                content = r.content
                if not content.lstrip().startswith(b"%PDF-"):
                    raise FetchError("Takeda: expected PDF bytes but got non-PDF")

                # Keep only the latest PDF (overwrite)
                pdf_dir = Path(getattr(p, "pdf_dir", Path(cfg.output_dir) / "pdfs"))
                pdf_dir.mkdir(parents=True, exist_ok=True)
                pdf_path = pdf_dir / (getattr(p, "pdf_filename", "takeda_latest.pdf"))
                pdf_path.write_bytes(content)

                # Optional debug copy
                if debug_dir:
                    (debug_dir / "Takeda_raw.pdf").write_bytes(content)

                # pass BYTES to parser
                payload = content

            # ------------------------ CSV-from-file partners ------------------------
            elif getattr(p, "csv_path", None):
                payload = Path(p.csv_path).read_text(encoding="utf-8")
                source_url = getattr(p, "url", None)

            # ------------------------ CSV-from-url partners -------------------------
            elif getattr(p, "csv_url", None):
                payload = fetch_html(
                    str(p.csv_url),
                    cfg.user_agent,
                    respect_robots=False,
                    extra_headers=(p.headers or None)
                )
                source_url = str(p.csv_url)

            # ------------------------ CSV via click partners ------------------------
            elif getattr(p, "csv_via_click", False):
                payload = fetch_csv_by_click(str(p.url), user_agent=cfg.user_agent)
                source_url = str(p.url)

            # ------------------------ Rendered HTML partners ------------------------
            elif getattr(p, "render_js", False):
                payload = fetch_html_rendered(str(p.url), user_agent=cfg.user_agent)
                source_url = str(p.url)

            # ------------------------ Default HTML partners -------------------------
            else:
                payload = fetch_html(
                    str(p.url),
                    cfg.user_agent,
                    respect_robots=cfg.respect_robots,
                    extra_headers=(p.headers or None)
                )
                source_url = str(p.url)

        except FetchError as e:
            print(f"[WARN] Failed to fetch {p.name}: {e}")
            continue


        
        if p.name == "Takeda" and isinstance(payload, str):
            pdf_url = discover_pipeline_pdf(getattr(p, "discovery_page", None) or str(getattr(p, "url", "")), cfg.user_agent)
            source_url = pdf_url
            ua = cfg.user_agent or ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36")
            r = requests.get(pdf_url, headers={
                "User-Agent": ua,
                "Accept": "application/pdf,*/*;q=0.8",
                "Referer": getattr(p, "discovery_page", None) or str(getattr(p, "url", "")) or "https://www.takeda.com/science/pipeline/",
                "Connection": "close",
            }, timeout=60)
            if r.status_code >= 400:
                raise FetchError(f"HTTP {r.status_code} for {pdf_url}")
            content = r.content
            if not content.lstrip().startswith(b"%PDF-"):
                raise FetchError("Takeda: expected PDF bytes but got non-PDF")
            payload = content  # ensure bytes for the parser


        # Parse
        parser_impl = get_parser(p.name)
        records = parser_impl.parse(payload, source_url or getattr(p, "url", ""))


        # Stamp times + normalize schema for output
        for r in records:
            if not r.scraped_at:
                r.scraped_at = now
            rec = r.to_dict()
            # Ensure required keys exist for uniform JSONL schema
            rec.setdefault("source_url", source_url or getattr(p, "url", ""))
            rec.setdefault("mechanism", None)
            rec.setdefault("raw", None)
            all_records.append(rec)

        print(f"[INFO] {p.name}: extracted {len(records)} records")

    # indication-aware global dedupe (optional but recommended)
    seen = set()
    deduped = []
    for r in all_records:
        key = (
            (r.get('company') or '').strip().lower(),
            (r.get('drug_name') or '').strip().lower(),
            (r.get('phase') or '').strip().lower(),
            (r.get('indication') or '').strip().lower(),
        )
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    all_records = deduped

    # resolve output path
    if args.out:
        out_path = Path(args.out)
    else:
        out_dir = Path(cfg.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        # Convert UTC ISO string → aware UTC datetime
        utc_dt = datetime.fromisoformat(now.replace('Z', '+00:00'))

        # Convert UTC → California time
        local_dt = utc_dt.astimezone(ZoneInfo("America/Los_Angeles"))

        # Build readable timestamp
        stamp = local_dt.strftime('%Y-%m-%d_%H%M_%Z')

        ext = "csv" if args.format == "csv" else "jsonl"
        out_path = out_dir / f"pipeline_{stamp}.{ext}"



    write_output(all_records, out_path, args.format)
    print(f"[OK] Wrote {len(all_records)} records to {out_path}")


if __name__ == '__main__':
    main()