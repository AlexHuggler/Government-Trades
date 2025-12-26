# Government-Trades

This repository contains a small utility for scraping trade disclosure tables from a web page and aggregating them by owner (filer, spouse, family) and transaction type (buy/sell).

## Setup

Install dependencies with:

```bash
pip install -r requirements.txt
```

## Usage

Run the scraper with the URL that contains the disclosure table:

```bash
python scrape_trades.py --url "https://example.com/page-with-table"
```

Key options:

- `--table-index`: zero-based index of the HTML table to parse (default: first table on the page).
- `--owner-column` and `--transaction-column`: explicitly set column names if automatic detection fails.
- `--raw-csv`: path for saving the raw scraped table (default: `raw_trades.csv`).
- `--aggregated-csv`: path for saving the aggregated summary (default: `aggregated_trades.csv`).
- `--skip-ssl-verify`: bypass SSL verification when fetching the page.

The script writes both the raw table and an aggregated CSV showing the count of trades for each owner/transaction combination, making it easy to share or further analyze the results.
