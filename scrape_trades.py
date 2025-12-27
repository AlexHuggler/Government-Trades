"""
Scrape and aggregate Capitol Trades disclosures for all politicians listed on
Capitol Trades.

Highlights
----------
* Discovers politician IDs from the Capitol Trades "Politicians" listing pages
  and scrapes each member's paginated trade tables.
* Stitches all trades into a single raw dataset plus a shareable aggregation by
  owner (filer, spouse, family) and buy/sell action.
* Exposes a simple CLI plus importable helpers that work well in Google Colab
  notebooks.

Examples
--------
* Crawl the first 3 listing pages, fetch up to 3 trade pages per politician,
  and save CSVs:

    python scrape_trades.py --list-max-pages 3 --max-pages 3 --raw-csv all_raw.csv --aggregated-csv all_aggregated.csv

* From a notebook, import and run:

    from scrape_trades import scrape_all_politicians, aggregate_trades, ColumnHints
    raw = scrape_all_politicians(list_max_pages=2, max_pages=2)
    summary = aggregate_trades(raw, ColumnHints(transaction=None, owner=None))
    display(summary)

The script writes both the raw table (concatenated across politicians and
pages) and an aggregated summary to CSV files for easy sharing.
"""
from __future__ import annotations

import argparse
import csv
import re
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


def discover_politicians(
    *, base_url: str, chamber: Optional[str] = None, page_size: int = 96, max_pages: int = 10, verify_ssl: bool = True
) -> list[tuple[str, Optional[str]]]:
    """Return a list of ``(politician_id, politician_name)`` tuples."""

    ids: list[tuple[str, Optional[str]]] = []
    seen: set[str] = set()
    chamber = chamber.lower() if chamber else None

    def _walk(obj: object) -> None:
        if isinstance(obj, dict):
            # JSON payloads often store both the ID and the name on the same object.
            if "politicianId" in obj:
                pid = str(obj.get("politicianId"))
                if pid and pid not in seen:
                    name = obj.get("fullName") or obj.get("name") or obj.get("displayName")
                    seen.add(pid)
                    ids.append((pid, name if name else None))
            for value in obj.values():
                _walk(value)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    for page in range(1, max_pages + 1):
        url = f"{base_url}/politicians?page={page}&pageSize={page_size}"
        if chamber:
            url += f"&chamber={chamber}"
        html = fetch_html(url, verify_ssl=verify_ssl)
        soup = BeautifulSoup(html, "lxml")

        # Primary path: parse the Next.js data blob to pull politicianId/name pairs.
        blob = soup.find("script", id="__NEXT_DATA__")
        found_this_page = 0
        if blob and blob.string:
            try:
                import json

                data = json.loads(blob.string)
                before = len(ids)
                _walk(data)
                found_this_page = len(ids) - before
            except Exception:
                # Fall back to regex/anchor extraction below
                pass

        # Secondary path: harvest IDs/names from anchor links on the listing page.
        if found_this_page == 0:
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                text = anchor.get_text(strip=True)
                match = re.search(r"/politician/([A-Z0-9]+)", href) or re.search(
                    r"politician=([A-Z0-9]+)", href
                )
                if not match:
                    continue
                pid = match.group(1)
                if pid in seen:
                    continue
                seen.add(pid)
                ids.append((pid, text or None))
                found_this_page += 1

        # Fallback: regex IDs in the raw HTML (works even if the name can't be paired).
        if found_this_page == 0:
            for match in re.finditer(r"politicianId\":\"([A-Z0-9]+)\"", html):
                pid = match.group(1)
                if pid in seen:
                    continue
                seen.add(pid)
                ids.append((pid, None))
                found_this_page += 1

        # If nothing was detected, stop early to avoid hammering empty pages.
        if found_this_page == 0:
            break

        time.sleep(0.2)

    if not ids:
        raise TradeScraperError("No politicians discovered. Try increasing list_max_pages or check connectivity.")
    return ids


def scrape_politician_trades(
    politician_id: str,
    *,
    politician_name: Optional[str] = None,
    base_url: str = "https://www.capitoltrades.com",
    page_size: int = 96,
    max_pages: int = 10,
    verify_ssl: bool = True,
) -> pd.DataFrame:
    """Scrape paginated trade tables for a single politician and return one DataFrame."""

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
        raise TradeScraperError(
            f"No trade tables found for politician {politician_id}; try increasing max_pages or check connectivity."
        )
    table = pd.concat(frames, ignore_index=True)
    table.insert(0, "politician_id", politician_id)
    if politician_name:
        table.insert(1, "politician_name", politician_name)
    return table


def scrape_all_politicians(
    *,
    base_url: str = "https://www.capitoltrades.com",
    page_size: int = 96,
    max_pages: int = 10,
    list_page_size: int = 96,
    list_max_pages: int = 5,
    verify_ssl: bool = True,
    explicit_ids: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Discover politicians and scrape all of their trades."""

    discovered: list[tuple[str, Optional[str]]] = []
    if explicit_ids:
        discovered = [(pid, None) for pid in explicit_ids]
    else:
        discovered.extend(
            discover_politicians(
                base_url=base_url,
                page_size=list_page_size,
                max_pages=list_max_pages,
                verify_ssl=verify_ssl,
            )
        )

    frames: list[pd.DataFrame] = []
    for pid, name in discovered:
        try:
            frames.append(
                scrape_politician_trades(
                    pid,
                    politician_name=name,
                    base_url=base_url,
                    page_size=page_size,
                    max_pages=max_pages,
                    verify_ssl=verify_ssl,
                )
            )
        except TradeScraperError:
            continue
    if not frames:
        raise TradeScraperError(
            "No trade tables collected. Try increasing max_pages/list_max_pages or verify connectivity to Capitol Trades."
        )
    return pd.concat(frames, ignore_index=True)


def save_dataframe(df: pd.DataFrame, path: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False, quoting=csv.QUOTE_MINIMAL)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape and aggregate trade disclosure tables for all politicians from Capitol Trades."
    )
    parser.add_argument("--owner-column", help="Optional explicit owner column name.")
    parser.add_argument("--transaction-column", help="Optional explicit transaction column name.")
    parser.add_argument("--raw-csv", default="all_trades_raw.csv", help="Path to save the scraped table.")
    parser.add_argument(
        "--aggregated-csv", default="all_trades_aggregated.csv", help="Path to save the aggregated summary table."
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
    parser.add_argument(
        "--list-page-size", type=int, default=96, help="How many politicians to request per listing page."
    )
    parser.add_argument("--list-max-pages", type=int, default=5, help="Maximum politician listing pages to crawl.")
    parser.add_argument(
        "--politician-id",
        action="append",
        help="Explicit politician IDs to scrape instead of auto-discovery (can be repeated).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    hints = ColumnHints(transaction=args.transaction_column, owner=args.owner_column)

    try:
        selected_table = scrape_all_politicians(
            base_url=args.base_url,
            page_size=args.page_size,
            max_pages=args.max_pages,
            list_page_size=args.list_page_size,
            list_max_pages=args.list_max_pages,
            explicit_ids=args.politician_id,
            verify_ssl=not args.skip_ssl_verify,
        )
        aggregated = aggregate_trades(selected_table, hints)
        save_dataframe(aggregated, args.aggregated_csv)
        save_dataframe(selected_table, args.raw_csv)
    except (requests.RequestException, TradeScraperError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Saved raw trades to {args.raw_csv}")
    print(f"Saved aggregated trades to {args.aggregated_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
