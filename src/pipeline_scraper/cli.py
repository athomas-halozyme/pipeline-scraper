# src/pipeline_scraper/clipy.py
from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import List
import json
import pandas as pd
import requests

from .config import AppConfig
from .http import fetch_html, fetch_html_with_session, fetch_html_rendered, FetchError
from .http_csv_click import fetch_csv_by_click
from .registry import get_parser
from .utils import utc_now_iso, shape_records_for_output


def _setup_logging(verbose: bool, log_level: str | None = None) -> None:
    """
    Configure root logger.
      - INFO by default
      - --verbose upgrades to DEBUG
      - --log-level overrides both (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    level = logging.DEBUG if verbose else logging.INFO
    if log_level:
        level = getattr(logging, log_level.upper(), level)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def write_output(records: List[dict], path: Path, fmt: str) -> None:
    """
    Write CSV or JSONL with a consistent schema order:
      - Drops mechanism/raw
      - Ensures core fields up front
      - Keeps provenance (source_url, scraped_at) at the end
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    shaped = shape_records_for_output(records)

    if fmt == "csv":
        df = pd.DataFrame(shaped)
        cols = list(shaped[0].keys()) if shaped else []
        if cols:
            df = df[cols]
        df.to_csv(path, index=False)
    elif fmt in {"jsonl", "ndjson"}:
        with open(path, "w", encoding="utf-8") as f:
            for r in shaped:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def main():
    parser = argparse.ArgumentParser(description="Pharma pipeline scraper")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    parser.add_argument("--partners", nargs="*", help="Subset of partners to run (by name)")
    parser.add_argument("--out", required=False, help="Output file path; overrides config.output_dir default naming")
    parser.add_argument("--format", default="csv", choices=["csv", "jsonl"], help="Output format (csv or jsonl)")
    parser.add_argument("--debug", action="store_true", help="Dump fetched HTML/JSON/PDF for troubleshooting")
    parser.add_argument("--verbose", "-v", action="store_true", help="More verbose logs (DEBUG level)")
    parser.add_argument("--log-level", default=None, help="Explicit log level (DEBUG, INFO, WARNING, ERROR)")
    args = parser.parse_args()

    _setup_logging(args.verbose, args.log_level)
    log = logging.getLogger("pipeline_scraper")

    cfg = AppConfig.load(args.config)
    selected = set(p.name for p in cfg.partners)

    # Optional subset selection
    if args.partners:
        requested = set(args.partners)
        unknown = requested - selected
        if unknown:
            raise SystemExit(f"Unknown partners in --partners: {sorted(unknown)}")
        selected = requested

    log.info("Running partners: %s", ", ".join(sorted(selected)))
    all_records: List[dict] = []
    now = utc_now_iso()  # ISO UTC for record.scraped_at

    # Debug assets written under output_dir/debug when --debug is set
    debug_dir = Path(cfg.output_dir) / "debug" if args.debug else None
    if debug_dir:
        debug_dir.mkdir(parents=True, exist_ok=True)

    for p in cfg.partners:
        if p.name not in selected:
            continue

        payload = None
        source_url = None

        try:
            t_fetch = time.perf_counter()

            # ------------------------ Takeda (PDF discovery) ------------------------
            if (p.name == "Takeda") and (getattr(p, "pdf_discovery", True)):
                from .discovery.takeda import discover_pipeline_pdf

                pdf_url = discover_pipeline_pdf(
                    getattr(p, "discovery_page", None) or str(getattr(p, "url", "https://www.takeda.com/science/pipeline/")),
                    cfg.user_agent
                )
                source_url = pdf_url
                ua = cfg.user_agent or (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
                )
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

                pdf_dir = Path(getattr(p, "pdf_dir", Path(cfg.output_dir) / "pdfs"))
                pdf_dir.mkdir(parents=True, exist_ok=True)
                pdf_path = pdf_dir / (getattr(p, "pdf_filename", "takeda_latest.pdf"))
                pdf_path.write_bytes(content)
                if debug_dir:
                    (debug_dir / "Takeda_raw.pdf").write_bytes(content)
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

            log.debug("%s: fetch elapsed %.2fs", p.name, time.perf_counter() - t_fetch)

        except FetchError as e:
            log.warning("Failed to fetch %s: %s", p.name, e)
            continue

        # Parse
        parser_impl = get_parser(p.name)
        t_parse = time.perf_counter()
        records = parser_impl.parse(payload, source_url or getattr(p, "url", ""))
        log.info("%s: extracted %d records (parse %.2fs)", p.name, len(records), time.perf_counter() - t_parse)

        # Normalize/collect records
        for r in records:
            if not r.scraped_at:
                r.scraped_at = now
            rec = r.to_dict()
            rec.setdefault("source_url", source_url or getattr(p, "url", ""))
            # (mechanism/raw, if present in some parsers, will be dropped by the shaper)
            all_records.append(rec)

    # Resolve output path / name (California time)
    if args.out:
        out_path = Path(args.out)
    else:
        out_dir = Path(cfg.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        # Build a friendly local-time filename in America/Los_Angeles, minute precision
        utc_dt = datetime.fromisoformat(now.replace("Z", "+00:00"))
        local_dt = utc_dt.astimezone(ZoneInfo("America/Los_Angeles"))
        stamp = local_dt.strftime("%Y-%m-%d_%H%M_%Z")  # e.g., 2026-03-10_1355_PDT
        ext = "csv" if args.format == "csv" else "jsonl"

        if len(selected) == 1:
            only = next(iter(selected)).replace(" ", "")
            out_path = out_dir / f"pipeline_{only}_{stamp}.{ext}"
        else:
            out_path = out_dir / f"pipeline_{stamp}.{ext}"

    # Write
    write_output(all_records, out_path, args.format)
    log.info("Wrote %d records to %s (%s)", len(all_records), out_path, args.format)
    print(f"[OK] Wrote {len(all_records)} records to {out_path}")


if __name__ == "__main__":
    main()
