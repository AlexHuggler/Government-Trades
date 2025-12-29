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


from bs4 import BeautifulSoup

def fetch_html(url: str, verify_ssl: bool = True) -> str:
    # Added a user-agent to mimic a real browser, which helps with large requests
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers, verify=verify_ssl)
    response.raise_for_status()
    return response.text

def get_all_politicians(base_url: str = "https://www.capitoltrades.com", page_size: int = 100) -> dict[str, str]:
    """
    Scrapes the politicians directory by iterating through all pages.
    """
    print(f"Fetching politician directory from {base_url}...")
    politicians = {}
    page = 1
    
    while True:
        # Construct URL with both page and pageSize
        directory_url = f"{base_url}/politicians?page={page}&pageSize={page_size}"
        print(f"  Fetching page {page}...", end="\r")
        
        try:
            html = fetch_html(directory_url)
        except Exception as e:
            print(f"\nError fetching page {page}: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")
        found_on_page = 0
        
        # Iterate through links on the current page
        for link in soup.find_all("a", href=True):
            href = link["href"]
            
            if href.startswith("/politicians/") and len(href.split("/")) == 3:
                id_part = href.split("/")[-1]
                name = link.get_text(strip=True)
                
                # Filter junk and ensure uniqueness
                if name and id_part and id_part not in ["politicians", "trades"]:
                    if name not in politicians:
                        politicians[name] = id_part
                        found_on_page += 1
        
        # BREAK CONDITION: If no new politicians found on this page, stop.
        if found_on_page == 0:
            break
            
        page += 1
        time.sleep(0.5) # Be polite to the server

    print(f"\nSuccess! Found {len(politicians)} total politicians.")
    return politicians

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


def scrape_politician_trades(
    politician_id: str, # Changed from hardcoded constant to argument
    base_url: str = "https://www.capitoltrades.com", 
    page_size: int = 96, 
    max_pages: int = 10, 
    verify_ssl: bool = True ) -> pd.DataFrame:
    """Scrape paginated trade tables for a specific politician ID."""
    
    frames: list[pd.DataFrame] = []
    for page in range(1, max_pages + 1):
        # Insert the dynamic ID here
        url = f"{base_url}/trades?politician={politician_id}&page={page}&pageSize={page_size}"
        try:
            tables = fetch_tables(url, verify_ssl=verify_ssl)
        except TradeScraperError:
            break
        if not tables:
            break
        frames.append(tables[0])
        time.sleep(0.25)
        
    if not frames:
        # Return empty DF instead of raising error so the loop doesn't crash on one empty profile
        return pd.DataFrame() 
        
    table = pd.concat(frames, ignore_index=True)
    table.insert(0, "politician_id", politician_id)
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


    
def clean_trade_data(df):
    df = df.copy()

    # 1. Clean 'politician_name'
    df['clean_name'] = df['politician_name'].str.split(r'(Republican|Democrat|Other|Libertarian)').str[0].str.strip()

    # 2. Clean 'Politician' details
    pol_pattern = r'^(?P<table_name>.+?)(?P<party>Republican|Democrat|Other|Libertarian)(?P<chamber>House|Senate)(?P<state>.*)$'
    pol_parts = df['Politician'].str.extract(pol_pattern)
    df = pd.concat([df, pol_parts], axis=1)

    # 3. Clean 'Traded Issuer'
    issuer_pattern = r'(?P<company_name>.*?)(?P<ticker>[A-Z\.]+:[A-Z]+|N/A)$'
    issuer_parts = df['Traded Issuer'].str.extract(issuer_pattern)
    df['company_name'] = issuer_parts['company_name'].str.strip()
    df['ticker'] = issuer_parts['ticker'].str.strip()

    # 4. Clean Date Columns
    for col in ['Published', 'Traded']:
        df[col] = df[col].astype(str).str.replace(r'(\d{4})$', r' \1', regex=True)
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # --- NEW ADDITION START ---
    # 4.5 Create the MM/YYYY column
    # We duplicate 'Traded' and format it as a string (Month/Year)
    df['Traded_Month_Year'] = df['Traded'].dt.strftime('%m/%Y')
    # --- NEW ADDITION END ---

    # 5. Clean 'Filed After'
    df['filed_days_ago'] = df['Filed After'].astype(str).str.replace('days', '', case=False)
    df['filed_days_ago'] = pd.to_numeric(df['filed_days_ago'], errors='coerce').fillna(0).astype(int)

    # 6. Reorder and Select Final Columns
    final_cols = [
        'clean_name', 'party', 'chamber', 'state',
        'company_name', 'ticker',
        'Owner', 'Type', 'Size', 'Price',
        'Published', 'Traded', 
        'Traded_Month_Year',  # <--- Added here so it appears in final output
        'filed_days_ago', 'politician_id'
    ]
    
    existing_cols = [c for c in final_cols if c in df.columns]
    return df[existing_cols]



def main() -> int:
    args = parse_args()
    
    # 1. Get the map of all politicians
    # Note: You might want to add a flag like --all to trigger this
    all_politicians = get_all_politicians(verify_ssl=not args.skip_ssl_verify)
    
    all_data_frames = []
    
    # 2. Loop through them (Scanning the first 5 for testing)
    # Remove [:5] to scrape everyone
    for name, pol_id in list(all_politicians.items())[:5]: 
        print(f"Scraping trades for {name} ({pol_id})...")
        
        df = scrape_politician_trades(
            politician_id=pol_id,
            base_url=args.base_url,
            page_size=args.page_size,
            max_pages=args.max_pages,
            verify_ssl=not args.skip_ssl_verify
        )
        
        if not df.empty:
            # Add the name column for readability
            df.insert(0, "politician_name", name)
            all_data_frames.append(df)
        else:
            print(f"  No trades found for {name}.")

    # 3. Combine everything into one massive CSV
    if all_data_frames:
        final_df = pd.concat(all_data_frames, ignore_index=True)
        
        # Aggregate
        hints = ColumnHints(transaction=args.transaction_column, owner=args.owner_column)
        aggregated = aggregate_trades(final_df, hints)
        
        save_dataframe(final_df, args.raw_csv)
        save_dataframe(aggregated, args.aggregated_csv)
        print(f"Successfully scraped {len(all_data_frames)} politicians.")
    else:
        print("No data found.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())