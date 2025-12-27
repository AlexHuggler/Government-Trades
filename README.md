# Government-Trades

This repository contains a small utility for scraping trade disclosure tables from a web page and aggregating them by owner (filer, spouse, family) and transaction type (buy/sell). The script now targets [Capitol Trades](https://www.capitoltrades.com/) and automatically collects all listed politicians.

## Setup

Install dependencies with:

```bash
pip install -r requirements.txt
```

## Usage

Run the scraper from the command line to discover Capitol Trades politician IDs, grab multiple paginated trade tables for each member, and aggregate them:

```bash
python scrape_trades.py --list-max-pages 3 --max-pages 3 --raw-csv all_trades_raw.csv --aggregated-csv all_trades_aggregated.csv
```

Key options:

- `--owner-column` and `--transaction-column`: explicitly set column names if automatic detection fails.
- `--raw-csv`: path for saving the raw scraped table (default: `all_trades_raw.csv`).
- `--aggregated-csv`: path for saving the aggregated summary (default: `all_trades_aggregated.csv`).
- `--max-pages`: maximum paginated trade pages to crawl (default: 10).
- `--list-max-pages`: how many politician listing pages to crawl (default: 5).
- `--open-browser`: open your default browser to the first listing page plus a handful of sample trade pages for visual
  validation of what will be scraped (combine with `--preview-count` to control how many samples are opened).
- `--preview-count`: how many sample trade pages to open when using `--open-browser` (default: 1).
- `--skip-ssl-verify`: bypass SSL verification when fetching pages.
- `--base-url`: override the default Capitol Trades base URL if needed.

The script writes both the raw tables (with `politician_id` and, when available, `politician_name`) and an aggregated CSV showing the count of trades for each owner/transaction combination, making it easy to share or further analyze the results.

Notes on discovery reliability:

- The scraper now parses the Capitol Trades Next.js data blob (the `__NEXT_DATA__` script tag) to pull `politicianId` and names directly from the page payload. If that blob is missing, it falls back to anchor links on the listing page and then regex extraction of `politicianId` values in the raw HTML.
- If no politicians are discovered, increase `--list-max-pages`, provide explicit IDs via `--politician-id` when running locally, or verify network access to https://www.capitoltrades.com/.

### Google Colab usage

Open `pelosi_trades_colab.ipynb` in Google Colab for a ready-to-run workflow that installs dependencies, imports the scraper, pulls trades for all listed politicians, and shows both the raw and aggregated views. If you prefer a quick start directly in a Colab cell, run:

```python
%pip install -q pandas requests lxml beautifulsoup4
from scrape_trades import scrape_all_politicians, aggregate_trades, ColumnHints

raw = scrape_all_politicians(list_max_pages=1, max_pages=2)
summary = aggregate_trades(raw, ColumnHints(transaction=None, owner=None))
summary
```
