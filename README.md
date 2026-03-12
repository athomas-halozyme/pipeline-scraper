
# Pharma Pipeline Scraper

A lightweight, extensible Python CLI to scrape publicly available **drug/compound names** and **development phases** from partner pipeline pages (e.g., BMS, J&J, Roche, Takeda, argenx).

> ⚠️ **Use responsibly**: Always follow each site's Terms of Use and robots.txt. This tool is for compliant, internal research only.

## Features
- Config-driven list of partners and URLs (YAML)
- Company-specific parsers with a common data schema
- Retry with backoff, polite defaults (UA string, optional robots check)
- Output to CSV or JSON Lines

## Quickstart

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# Dry run using the example config (update URLs first)
python -m pipeline_scraper   --config examples/config.yaml   --out data/pipeline_YYYYMMDD.csv   --format csv   --partners BMS JnJ Roche Takeda ArgenX
```

## Config file (YAML)
```yaml
# examples/config.yaml
output_dir: data
user_agent: "PharmaPipelineScraper/1.0 (+contact@example.com)"
respect_robots: true
partners:
  - name: BMS
    url: https://www.bms.com/researchers-and-partners/in-the-pipeline.html
  - name: JnJ
    url: https://www.investor.jnj.com/pipeline/development-pipeline/default.aspx
  - name: Roche
    url: https://www.roche.com/solutions/pipeline
  - name: Takeda
    url: https://www.takedaoncology.com/science/pipeline/?utm_source=aw_sbr_paidsearch&utm_medium=cpc&utm_campaign=takonpi_corp_sem_crs_aw_sbr_nat_awa_phrs&utm_keyword=takeda-oncology-pipeline&utm_id=takonpi23362&gclsrc=aw.ds&gad_source=1&gad_campaignid=20787203671&gbraid=0AAAAAqTCZsBLrW29wNSt1kvH0gESOn95S&gclid=EAIaIQobChMI98-ZrJuWkwMVRCdECB2LMC-SEAAYASAAEgJ_AvD_BwE
  - name: ArgenX
    url: https://argenx.com/pipeline
```

## Output schema
Each record contains at least:

- `company` (str)
- `drug_name` (str)
- `phase` (normalized; e.g., `Phase 1`, `Phase 2/3`, `Filed`, `Approved`, `Preclinical`, `Discovery`)
- `source_url` (str)
- `scraped_at` (UTC ISO timestamp)

Optional best-effort fields (if easily available): `indication`, `therapy_area`, `mechanism`, `raw` (dict of source columns).

## Parser architecture
- `BaseParser` defines the interface and helpers
- One parser per company under `pipeline_scraper/parsers/`
- A generic table parser is provided as a fallback

## Notes
- Some pages may load dynamically. This project uses `requests + BeautifulSoup`. If JavaScript rendering is required, we can add a Playwright/Selenium fallback later.
- Parsing rules evolve; keep parsers small, tested, and company-specific.

## License
MIT
