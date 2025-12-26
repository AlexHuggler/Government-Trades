"""
Scrape and aggregate Nancy Pelosi's trade disclosures from Capitol Trades.

Highlights
----------
* Purpose-built for the Capitol Trades listing of Representative Nancy Pelosi
  (ID ``P000197``).
* Crawls multiple paginated trade pages, stitches them together, and produces a
  shareable aggregation by owner (filer, spouse, family) and buy/sell action.
* Exposes a simple CLI plus importable helpers that work well in Google Colab
  notebooks.

Examples
--------
* Crawl the first 5 pages of Pelosi trades and save CSVs:

    python scrape_trades.py --max-pages 5 --raw-csv pelosi_raw.csv --aggregated-csv pelosi_aggregated.csv

* From a notebook, import and run:

    from scrape_trades import scrape_pelosi_trades, aggregate_trades, ColumnHints
    raw = scrape_pelosi_trades(max_pages=5)
    summary = aggregate_trades(raw, ColumnHints(transaction=None, owner=None))
    display(summary)

The script writes both the raw table (concatenated across pages) and an
aggregated summary to CSV files for easy sharing.
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests


@dataclass
class ColumnHints:
    transaction: Optional[str]
    owner: Optional[str]


class TradeScraperError(Exception):
    """Custom error for scraper failures."""


PELOSI_ID = "P000197"


def fetch_html(url: str, verify_ssl: bool = True) -> str:
    """Download raw HTML with a friendly user agent."""
    headers = {"User-Agent": "Government-Trades-Scraper/1.0"}
    response = requests.get(url, timeout=30, verify=verify_ssl, headers=headers)
    response.raise_for_status()
    return response.text


def fetch_tables(url: str, verify_ssl: bool = True) -> list[pd.DataFrame]:
    """Download tables from the given URL using pandas.read_html."""
    html = fetch_html(url, verify_ssl=verify_ssl)
    try:
        return pd.read_html(html)
    except ValueError as exc:  # No tables found
        raise TradeScraperError(f"No HTML tables found at {url}") from exc


def locate_column(columns: Iterable[str], keywords: Iterable[str], explicit: Optional[str]) -> Optional[str]:
    """Return the column name matching keywords or the explicit override."""
    if explicit:
        return explicit
    lowered = {col.lower(): col for col in columns}
    for key in keywords:
        for lower, original in lowered.items():
            if key in lower:
                return original
    return None


def normalize_transaction(value: str) -> str:
    text = str(value).strip().lower()
    if "buy" in text or "purchase" in text or "acquisition" in text:
        return "Buy"
    if "sell" in text or "sale" in text or "disposition" in text:
        return "Sell"
    return value if value else "Unknown"


def aggregate_trades(table: pd.DataFrame, hints: ColumnHints) -> pd.DataFrame:
    transaction_col = locate_column(
        table.columns,
        ["transaction", "type", "buy", "sell", "acquisition", "disposition"],
        hints.transaction,
    )
    owner_col = locate_column(table.columns, ["owner", "by", "spouse", "family", "filer"], hints.owner)

    missing = []
    if not transaction_col:
        missing.append("transaction/buy-sell column")
    if not owner_col:
        missing.append("owner column")
    if missing:
        raise TradeScraperError(f"Could not locate: {', '.join(missing)}")

    table = table.copy()
    table[transaction_col] = table[transaction_col].apply(normalize_transaction)
    grouped = table.groupby([owner_col, transaction_col]).size().reset_index(name="trade_count")
    grouped = grouped.sort_values(by=[owner_col, transaction_col]).reset_index(drop=True)
    grouped.rename(columns={owner_col: "owner", transaction_col: "transaction"}, inplace=True)
    return grouped


def scrape_pelosi_trades(
    *, base_url: str = "https://www.capitoltrades.com", page_size: int = 96, max_pages: int = 10, verify_ssl: bool = True
) -> pd.DataFrame:
    """Scrape paginated trade tables for Nancy Pelosi and return one DataFrame."""

    frames: list[pd.DataFrame] = []
    for page in range(1, max_pages + 1):
        url = f"{base_url}/trades?politician={PELOSI_ID}&page={page}&pageSize={page_size}"
        try:
            tables = fetch_tables(url, verify_ssl=verify_ssl)
        except TradeScraperError:
            break
        if not tables:
            break
        frames.append(tables[0])
        # Be kind to the remote server.
        time.sleep(0.25)
    if not frames:
        raise TradeScraperError("No trade tables found for Nancy Pelosi; try increasing max_pages or check connectivity.")
    table = pd.concat(frames, ignore_index=True)
    table.insert(0, "politician_id", PELOSI_ID)
    return table


def save_dataframe(df: pd.DataFrame, path: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False, quoting=csv.QUOTE_MINIMAL)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape and aggregate Nancy Pelosi's trade disclosure tables from Capitol Trades."
    )
    parser.add_argument("--owner-column", help="Optional explicit owner column name.")
    parser.add_argument("--transaction-column", help="Optional explicit transaction column name.")
    parser.add_argument("--raw-csv", default="pelosi_raw_trades.csv", help="Path to save the scraped table.")
    parser.add_argument(
        "--aggregated-csv", default="pelosi_aggregated_trades.csv", help="Path to save the aggregated summary table."
    )
    parser.add_argument(
        "--skip-ssl-verify", action="store_true", help="Disable SSL verification when fetching the page."
    )
    parser.add_argument(
        "--base-url",
        default="https://www.capitoltrades.com",
        help="Base URL for Capitol Trades (override for testing or mirrors).",
    )
    parser.add_argument("--page-size", type=int, default=96, help="How many rows to request per page.")
    parser.add_argument("--max-pages", type=int, default=10, help="Maximum paginated pages to crawl.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    hints = ColumnHints(transaction=args.transaction_column, owner=args.owner_column)

    try:
        selected_table = scrape_pelosi_trades(
            base_url=args.base_url,
            page_size=args.page_size,
            max_pages=args.max_pages,
            verify_ssl=not args.skip_ssl_verify,
        )
        aggregated = aggregate_trades(selected_table, hints)
        save_dataframe(aggregated, args.aggregated_csv)
        save_dataframe(selected_table, args.raw_csv)
    except (requests.RequestException, TradeScraperError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Saved raw Pelosi trades to {args.raw_csv}")
    print(f"Saved aggregated Pelosi trades to {args.aggregated_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
