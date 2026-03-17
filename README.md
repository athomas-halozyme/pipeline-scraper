# Pharma Pipeline Scraper

A lightweight, extensible Python CLI to scrape publicly available **drug/compound names** and **development phases** from partner pipeline pages (e.g., BMS, J&J, Roche, Takeda, argenx).

> **Use responsibly**: Always follow each site's Terms of Use and robots.txt. This tool is for compliant, internal research only.

## Features
- Config-driven list of partners and URLs (YAML)
- Company-specific parsers with a common data schema
- Output to CSV or JSON Lines

## Quickstart

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .

# Dry run pulling all partners from config
python -m pipeline_scraper   --config examples/config.yaml  --format jsonl
```

## Config file (YAML)
```yaml
output_dir: data
user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
respect_robots: true
partners:
  - name: BMS
    url: https://www.bms.com/researchers-and-partners/in-the-pipeline.html
  - name: JnJ
    url: https://www.investor.jnj.com/pipeline/development-pipeline/default.aspx
    render_js: true
    headers:
      User-Agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
      Accept-Language: "en-US,en;q=0.9"
      Referer: "https://www.investor.jnj.com/overview/default.aspx"
      Upgrade-Insecure-Requests: "1"
  - name: Roche
    url: https://www.roche.com/solutions/pipeline
    csv_via_click: true
  - name: Takeda
    # Discovery page containing the “Download the PDF” button
    url: https://www.takeda.com/science/pipeline/
    pdf_discovery: true
    discovery_page: https://www.takeda.com/science/pipeline/
    # keep only the latest pdf; overwrite previous
    pdf_dir: "data/pdfs"
    pdf_filename: "takeda_latest.pdf"
    refresh_pdf: false   # turn true to force a fresh download
  - name: ArgenX
    url: https://argenx.com/pipeline
    render_js: true
```

## Output schema
Each record contains:

- `company` (str)
- `drug_name` (str)
- `phase` (str)
- `indication` (str)
- `therapeutic_area` (str)
- `source_url` (str)
- `scraped_at` (UTC ISO timestamp)

## Scraping methods
- Static HTML: If the table is in the browser and in the page source (`requests + BeautifulSoup`)
- Rendered HTML: If the table is in the browser but is not within the page source (needs rendering: `Playwright`)  
  Config:
  - `render_js: true`
- CSV ingestion: If the partner publishes data files for download (`pandas`)  
  Config:  
  - `csv_url: https://…/file.csv` (if direct URL to CSV)    
  - `csv_via_click: true` (if simulate a download click on the page needed)
- PDF ingestion: If the partner only shares PDFs, use the PDF discovery + extraction route (`pdfplumber`, `camelot`)  
  Config:  
  - `pdf_discovery: true`

## Parser architecture
- `BaseParser` defines the interface and helpers
- One parser per company under `pipeline_scraper/parsers/`
- A generic table parser is provided as a fallback

## Notes
- Parsing rules evolve; keep parsers updated, tested, and company-specific.
