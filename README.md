# Government-Trades

This repository contains a small utility for scraping trade disclosure tables from a web page and aggregating them by owner (filer, spouse, family) and transaction type (buy/sell). The script is now purpose-built for [Capitol Trades](https://www.capitoltrades.com/) and focuses solely on Representative Nancy Pelosi's disclosures.

## Setup

Install dependencies with:

```bash
pip install -r requirements.txt
```

## Usage

Run the scraper from the command line to grab multiple paginated trade tables for Nancy Pelosi (ID `P000197`) and aggregate them:

```bash
python scrape_trades.py --max-pages 5 --raw-csv pelosi_raw.csv --aggregated-csv pelosi_aggregated.csv
```

Key options:

- `--owner-column` and `--transaction-column`: explicitly set column names if automatic detection fails.
- `--raw-csv`: path for saving the raw scraped table (default: `pelosi_raw_trades.csv`).
- `--aggregated-csv`: path for saving the aggregated summary (default: `pelosi_aggregated_trades.csv`).
- `--max-pages`: maximum paginated trade pages to crawl (default: 10).
- `--skip-ssl-verify`: bypass SSL verification when fetching pages.
- `--base-url`: override the default Capitol Trades base URL if needed.

The script writes both the raw tables and an aggregated CSV showing the count of trades for each owner/transaction combination, making it easy to share or further analyze the results.

### Google Colab usage

Open `pelosi_trades_colab.ipynb` in Google Colab for a ready-to-run workflow that installs dependencies, imports the scraper, pulls Pelosi's trades, and shows both the raw and aggregated views. If you prefer a quick start directly in a Colab cell, run:

```python
%pip install -q pandas requests lxml beautifulsoup4
from scrape_trades import scrape_pelosi_trades, aggregate_trades, ColumnHints

raw = scrape_pelosi_trades(max_pages=3)
summary = aggregate_trades(raw, ColumnHints(transaction=None, owner=None))
summary
```
