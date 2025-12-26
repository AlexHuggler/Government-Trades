# Government-Trades

This repository contains a small utility for scraping trade disclosure tables from a web page and aggregating them by owner (filer, spouse, family) and transaction type (buy/sell). It now includes helpers tailored for the [Capitol Trades](https://www.capitoltrades.com/) site so you can quickly aggregate Nancy Pelosi's trades or every politician the site lists.

## Setup

Install dependencies with:

```bash
pip install -r requirements.txt
```

## Usage

You can still scrape any table URL, but the commands below showcase the Capitol Trades helpers:

```bash
# Scrape one specific disclosure page
python scrape_trades.py --url "https://www.capitoltrades.com/trades?politician=P000197&pageSize=96"

# Crawl multiple paginated trade pages for one politician (Nancy Pelosi) and aggregate
python scrape_trades.py --politician-id P000197 --max-pages 5

# Discover politician IDs from the listing pages and aggregate everyone found
python scrape_trades.py --all-politicians --politician-pages 2
```

Key options:

- `--table-index`: zero-based index of the HTML table to parse (default: first table on the page).
- `--owner-column` and `--transaction-column`: explicitly set column names if automatic detection fails.
- `--raw-csv`: path for saving the raw scraped table when targeting a single URL (default: `raw_trades.csv`).
- `--raw-dir`: directory used to store per-politician CSVs when scraping multiple IDs (default: `raw_trades/`).
- `--aggregated-csv`: path for saving the aggregated summary (default: `aggregated_trades.csv`).
- `--max-pages`: maximum paginated trade pages to crawl per politician (default: 10).
- `--politician-pages`: how many listing pages to scan when `--all-politicians` is enabled (default: 1).
- `--skip-ssl-verify`: bypass SSL verification when fetching pages.

The script writes both the raw tables and an aggregated CSV showing the count of trades for each owner/transaction combination, making it easy to share or further analyze the results. Use `--all-politicians` to quickly gather the same aggregation across everyone listed on the site.
