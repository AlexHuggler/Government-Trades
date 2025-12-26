"""
Scrape and aggregate trade disclosures from Capitol Trades pages.

Highlights
----------
* Support scraping a single disclosure table URL.
* Crawl multiple paginated trade pages for one politician using their ID
  (e.g., ``P000197`` for Nancy Pelosi).
* Discover and crawl all politician IDs listed on Capitol Trades and aggregate
  their trades into one shareable CSV.

Examples
--------
* Scrape one page directly:

    python scrape_trades.py --url "https://www.capitoltrades.com/trades?politician=P000197&pageSize=96"

* Crawl all trade pages for Nancy Pelosi (first 5 pages):

    python scrape_trades.py --politician-id P000197 --max-pages 5

* Crawl and aggregate every listed politician (first 2 listing pages):

    python scrape_trades.py --all-politicians --politician-pages 2

The script writes raw trade tables and aggregated summaries to CSV files for
easy sharing.
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
from bs4 import BeautifulSoup


@dataclass
class ColumnHints:
    transaction: Optional[str]
    owner: Optional[str]


class TradeScraperError(Exception):
    """Custom error for scraper failures."""


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


def extract_politician_ids(html: str) -> list[str]:
    """Parse HTML for unique politician IDs embedded in query params."""
    soup = BeautifulSoup(html, "html.parser")
    ids: set[str] = set()
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "politician=" in href:
            for part in href.split("?")[-1].split("&"):
                if part.startswith("politician="):
                    _, value = part.split("=", 1)
                    if value:
                        ids.add(value)
    return sorted(ids)


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


def scrape_trade_pages(base_url: str, politician_id: str, page_size: int, max_pages: int, verify_ssl: bool) -> pd.DataFrame:
    """Scrape all paginated trade tables for a politician and return one DataFrame."""
    frames: list[pd.DataFrame] = []
    for page in range(1, max_pages + 1):
        url = f"{base_url}/trades?politician={politician_id}&page={page}&pageSize={page_size}"
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
        raise TradeScraperError(f"No trade tables found for politician {politician_id}")
    return pd.concat(frames, ignore_index=True)


def scrape_all_politician_ids(base_url: str, pages: int, verify_ssl: bool) -> list[str]:
    """Discover politician IDs across paginated listing pages."""
    discovered: set[str] = set()
    for page in range(1, pages + 1):
        url = f"{base_url}/politicians?page={page}&pageSize=96"
        html = fetch_html(url, verify_ssl=verify_ssl)
        ids = extract_politician_ids(html)
        if not ids:
            break
        discovered.update(ids)
        time.sleep(0.25)
    if not discovered:
        raise TradeScraperError("No politician IDs discovered; check the base URL or page count.")
    return sorted(discovered)


def save_dataframe(df: pd.DataFrame, path: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False, quoting=csv.QUOTE_MINIMAL)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape and aggregate trade disclosure tables from a web page.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--url", help="Page URL containing an HTML table with trade data.")
    source.add_argument("--politician-id", action="append", help="Capitol Trades politician ID to crawl (repeatable).")
    source.add_argument(
        "--all-politicians",
        action="store_true",
        help="Discover all politician IDs from the listing pages and scrape each one.",
    )

    parser.add_argument("--table-index", type=int, default=0, help="Index of the HTML table to parse (default: 0).")
    parser.add_argument("--owner-column", help="Optional explicit owner column name.")
    parser.add_argument("--transaction-column", help="Optional explicit transaction column name.")
    parser.add_argument("--raw-csv", default="raw_trades.csv", help="Path to save the scraped table.")
    parser.add_argument(
        "--aggregated-csv", default="aggregated_trades.csv", help="Path to save the aggregated summary table."
    )
    parser.add_argument(
        "--skip-ssl-verify", action="store_true", help="Disable SSL verification when fetching the page."
    )
    parser.add_argument(
        "--base-url",
        default="https://www.capitoltrades.com",
        help="Base URL for Capitol Trades (override for testing or mirrors).",
    )
    parser.add_argument(
        "--page-size", type=int, default=96, help="How many rows to request per page when crawling politician trades."
    )
    parser.add_argument(
        "--max-pages", type=int, default=10, help="Maximum paginated pages to crawl per politician ID."
    )
    parser.add_argument(
        "--politician-pages",
        type=int,
        default=1,
        help="Number of listing pages to scan when --all-politicians is enabled.",
    )
    parser.add_argument(
        "--raw-dir",
        default="raw_trades",
        help="Directory to store raw CSVs when scraping multiple politicians.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    hints = ColumnHints(transaction=args.transaction_column, owner=args.owner_column)

    try:
        if args.url:
            tables = fetch_tables(args.url, verify_ssl=not args.skip_ssl_verify)
            if args.table_index >= len(tables):
                raise TradeScraperError(
                    f"Table index {args.table_index} is out of range; page contains {len(tables)} table(s)."
                )
            selected_table = tables[args.table_index]
            save_dataframe(selected_table, args.raw_csv)
            aggregated = aggregate_trades(selected_table, hints)
        else:
            politician_ids: list[str]
            if args.all_politicians:
                politician_ids = scrape_all_politician_ids(
                    args.base_url, pages=args.politician_pages, verify_ssl=not args.skip_ssl_verify
                )
            else:
                politician_ids = args.politician_id or []

            raw_frames: list[pd.DataFrame] = []
            for pid in politician_ids:
                table = scrape_trade_pages(
                    args.base_url,
                    politician_id=pid,
                    page_size=args.page_size,
                    max_pages=args.max_pages,
                    verify_ssl=not args.skip_ssl_verify,
                )
                table.insert(0, "politician_id", pid)
                raw_frames.append(table)
                save_dataframe(table, f"{args.raw_dir}/{pid}.csv")

            if not raw_frames:
                raise TradeScraperError("No trade data scraped; check options or network connectivity.")

            selected_table = pd.concat(raw_frames, ignore_index=True)
            aggregated = aggregate_trades(selected_table, hints)

        save_dataframe(aggregated, args.aggregated_csv)
    except (requests.RequestException, TradeScraperError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Saved raw trades to {args.raw_csv if args.url else args.raw_dir}")
    print(f"Saved aggregated trades to {args.aggregated_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
