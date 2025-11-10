#!/usr/bin/env python3
# build_historical_dataset.py
# Build comprehensive option datasets: Greeks + Price Summary for multiple days

import os
import sys
import csv
import time
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["MASSIVE_API_KEY"].strip()
BASE = "https://api.massive.com/v3"
HDRS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "User-Agent": "massive-historical-builder/1.0"
}

# Configuration
TARGET_UNDERLYINGS = ["QQQ", "NVDA", "TSLA", "COIN", "AMZN", "AMD", "AAPL", "MSFT", "AVGO", "META"]
MAX_DTE = 30
STRIKE_RANGE_PCT = 0.50

def fetch_chain_page(url, headers, params=None):
    """Fetch with retry and rate limiting."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get('Retry-After', 5))
                print(f"      Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                if r.status_code == 404:
                    return None  # No data for this date
                print(f"      Error {r.status_code}")
                return None
            return r.json()
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
    return None

def get_option_chain(underlying, headers, date=None, limit=250):
    """Fetch full chain with pagination. Date in YYYY-MM-DD format for historical data."""
    base_url = f"{BASE}/snapshot/options/{underlying}"
    params = {"limit": limit}
    if date:
        params["date"] = date  # HISTORICAL DATA PARAMETER!
    
    data = fetch_chain_page(base_url, headers, params)
    if not data:
        return []
    
    results = data.get("results", []) or []
    
    next_url = data.get("next_url")
    pages = 1
    while next_url and pages < 50:  # Safety limit
        data = fetch_chain_page(next_url, headers)
        if not data:
            break
        results.extend(data.get("results", []) or [])
        next_url = data.get("next_url")
        pages += 1
        time.sleep(0.1)
    
    return results

def parse_option_ticker(ticker):
    """Parse OSI format."""
    try:
        if not ticker or ':' not in ticker:
            return None
        
        contract = ticker.split(':', 1)[1]
        underlying = ''
        idx = 0
        for char in contract:
            if char.isalpha():
                underlying += char
                idx += 1
            else:
                break
        
        if not underlying:
            return None
        
        rest = contract[idx:]
        if len(rest) < 15:
            return None
        
        date_str = rest[:6]
        year = 2000 + int(date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        exp_date = datetime(year, month, day).date()
        
        call_put = rest[6]
        if call_put not in ('C', 'P'):
            return None
        
        strike_str = rest[7:15]
        strike = int(strike_str) / 1000.0
        
        return (underlying, exp_date, strike, call_put)
    except:
        return None

def extract_full_contract_data(contract_dict, data_date):
    """
    Extract ALL relevant fields from chain snapshot.
    Returns comprehensive dict with Greeks, prices, volume, OI.
    """
    details = contract_dict.get("details", {})
    ticker = details.get("ticker") or contract_dict.get("ticker")
    
    parsed = parse_option_ticker(ticker)
    if not parsed:
        return None
    
    underlying, exp_date, strike, call_put = parsed
    dte = (exp_date - data_date).days
    
    # Greeks
    greeks = contract_dict.get("greeks") or {}
    
    # Quote
    last_quote = contract_dict.get("last_quote") or {}
    
    # Trade
    last_trade = contract_dict.get("last_trade") or {}
    
    # Open Interest
    oi_field = contract_dict.get("open_interest")
    if isinstance(oi_field, dict):
        open_interest = oi_field.get("value")
    else:
        open_interest = oi_field
    
    # Day stats
    day = contract_dict.get("day") or {}
    
    # Underlying asset
    underlying_asset = contract_dict.get("underlying_asset") or {}
    
    return {
        'ticker': ticker,
        'underlying': underlying,
        'expiration': exp_date.isoformat(),
        'dte': dte,
        'strike': strike,
        'type': call_put,
        
        # Greeks (PRIMARY DATA WE NEED)
        'delta': greeks.get('delta'),
        'gamma': greeks.get('gamma'),
        'theta': greeks.get('theta'),
        'vega': greeks.get('vega'),
        'iv': greeks.get('implied_volatility'),
        
        # Open Interest (CRITICAL FOR GAMMA EXPOSURE)
        'open_interest': open_interest,
        
        # Current Prices
        'bid': last_quote.get('bid'),
        'ask': last_quote.get('ask'),
        'mid': (last_quote.get('bid') + last_quote.get('ask')) / 2 if (last_quote.get('bid') and last_quote.get('ask')) else None,
        'bid_size': last_quote.get('bid_size'),
        'ask_size': last_quote.get('ask_size'),
        'last_price': last_trade.get('price'),
        'last_size': last_trade.get('size'),
        
        # Day Volume & Price Range
        'volume': day.get('volume'),
        'open': day.get('open'),
        'high': day.get('high'),
        'low': day.get('low'),
        'close': day.get('close'),
        'vwap': day.get('vwap'),
        
        # Other
        'break_even': contract_dict.get('break_even_price'),
        'underlying_price': underlying_asset.get('price'),
    }

def fetch_day_data(underlying, data_date, max_dte, strike_range_pct):
    """
    Fetch comprehensive option data for one underlying on one day.
    Returns list of contract dicts with all fields.
    """
    print(f"    Fetching {underlying}...", end='', flush=True)
    
    # Use date parameter for historical data
    date_str = data_date.strftime('%Y-%m-%d')
    chain = get_option_chain(underlying, HDRS, date=date_str)
    
    if not chain:
        print(" No data")
        return []
    
    # Get spot price
    spot = None
    for c in chain:
        ua = c.get("underlying_asset") or {}
        if "price" in ua:
            spot = float(ua["price"])
            break
    
    # Process contracts
    filtered = []
    for contract_dict in chain:
        data = extract_full_contract_data(contract_dict, data_date)
        if not data:
            continue
        
        # Filter by DTE
        if data['dte'] < 0 or data['dte'] > max_dte:
            continue
        
        # Filter by strike
        if spot:
            min_strike = spot * (1 - strike_range_pct)
            max_strike = spot * (1 + strike_range_pct)
            if data['strike'] < min_strike or data['strike'] > max_strike:
                continue
        
        filtered.append(data)
    
    print(f" {len(filtered)} contracts")
    return filtered

def save_to_csv(data, out_path):
    """Save contract data to CSV."""
    if not data:
        return
    
    fieldnames = [
        'ticker', 'underlying', 'expiration', 'dte', 'strike', 'type',
        'delta', 'gamma', 'theta', 'vega', 'iv',
        'open_interest',
        'bid', 'ask', 'mid', 'bid_size', 'ask_size',
        'last_price', 'last_size',
        'volume', 'open', 'high', 'low', 'close', 'vwap',
        'break_even', 'underlying_price'
    ]
    
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def build_historical_dataset(start_date, end_date, underlyings):
    """
    Build dataset for date range.
    Creates: data/YYYY-MM-DD/{TICKER}_options.csv for each day/ticker.
    """
    current = start_date
    dates = []
    
    # Generate list of trading days (skip weekends)
    while current <= end_date:
        if current.weekday() < 5:  # Mon-Fri
            dates.append(current)
        current += timedelta(days=1)
    
    print(f"\n{'='*70}")
    print(f"Building Historical Dataset")
    print(f"{'='*70}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Trading days: {len(dates)}")
    print(f"Underlyings: {', '.join(underlyings)}")
    print(f"Output: data/YYYY-MM-DD/{{TICKER}}_options.csv")
    print(f"\nFilters: ≤{MAX_DTE} DTE, ±{int(STRIKE_RANGE_PCT*100)}% strikes")
    print(f"{'='*70}\n")
    
    total_contracts = 0
    total_files = 0
    
    for date_idx, data_date in enumerate(dates, 1):
        print(f"[{date_idx}/{len(dates)}] {data_date.strftime('%Y-%m-%d (%A)')}:")
        
        date_str = data_date.strftime('%Y-%m-%d')
        out_dir = f"data/{date_str}"
        
        for underlying in underlyings:
            try:
                contracts = fetch_day_data(underlying, data_date, MAX_DTE, STRIKE_RANGE_PCT)
                
                if contracts:
                    out_path = os.path.join(out_dir, f"{underlying}_options.csv")
                    save_to_csv(contracts, out_path)
                    total_contracts += len(contracts)
                    total_files += 1
                
                time.sleep(0.3)  # Rate limiting
                
            except Exception as e:
                print(f"    {underlying}: Error - {e}")
        
        print()  # Blank line between days
    
    print(f"\n{'='*70}")
    print(f"✅ Dataset Build Complete!")
    print(f"{'='*70}")
    print(f"Total files created: {total_files}")
    print(f"Total contracts: {total_contracts:,}")
    print(f"Average per file: {total_contracts // total_files if total_files > 0 else 0}")
    print(f"\nData location: data/YYYY-MM-DD/{{TICKER}}_options.csv")
    print(f"\nEach CSV contains:")
    print(f"  • Contract details (ticker, expiration, strike, type)")
    print(f"  • Greeks (delta, gamma, theta, vega, IV)")
    print(f"  • Open Interest")
    print(f"  • Quotes (bid/ask/mid/sizes)")
    print(f"  • Prices (last trade, OHLC, VWAP)")
    print(f"  • Volume")
    print(f"  • Underlying price")

def main():
    # Default: Last 30 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    if len(sys.argv) >= 3:
        try:
            start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
            end_date = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
        except:
            print("Usage: python build_historical_dataset.py [START_DATE] [END_DATE]")
            print("       Dates in YYYY-MM-DD format")
            print("       If not provided, uses last 30 days")
            sys.exit(1)
    
    build_historical_dataset(start_date, end_date, TARGET_UNDERLYINGS)

if __name__ == "__main__":
    main()

