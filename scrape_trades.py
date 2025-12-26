"""
Scrape trade disclosure tables from a web page and aggregate them.

Usage:
    python scrape_trades.py --url https://example.com/page-with-table

The script downloads the page, extracts the first HTML table (or a user-specified
index), attempts to identify transaction and owner columns, and then groups
counts by owner and transaction type. Results are written to CSV files for
sharing.
"""
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from typing import Iterable, Optional

import pandas as pd
import requests


@dataclass
class ColumnHints:
    transaction: Optional[str]
    owner: Optional[str]


class TradeScraperError(Exception):
    """Custom error for scraper failures."""


def fetch_tables(url: str, verify_ssl: bool = True) -> list[pd.DataFrame]:
    """Download tables from the given URL using pandas.read_html."""
    response = requests.get(url, timeout=30, verify=verify_ssl)
    response.raise_for_status()
    try:
        return pd.read_html(response.text)
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


def save_dataframe(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape and aggregate trade disclosure tables from a web page.")
    parser.add_argument("--url", required=True, help="Page URL containing an HTML table with trade data.")
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    hints = ColumnHints(transaction=args.transaction_column, owner=args.owner_column)

    try:
        tables = fetch_tables(args.url, verify_ssl=not args.skip_ssl_verify)
        if args.table_index >= len(tables):
            raise TradeScraperError(
                f"Table index {args.table_index} is out of range; page contains {len(tables)} table(s)."
            )
        selected_table = tables[args.table_index]
        save_dataframe(selected_table, args.raw_csv)

        aggregated = aggregate_trades(selected_table, hints)
        save_dataframe(aggregated, args.aggregated_csv)
    except (requests.RequestException, TradeScraperError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Saved raw trades to {args.raw_csv}")
    print(f"Saved aggregated trades to {args.aggregated_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
