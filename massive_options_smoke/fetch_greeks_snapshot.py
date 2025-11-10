#!/usr/bin/env python3
# fetch_greeks_snapshot.py
# Use REST API to fetch option chain with GREEKS, IV, and OI for backtesting

import os
import sys
import json
import csv
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["MASSIVE_API_KEY"].strip()
BASE = "https://api.massive.com/v3"
HDRS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "User-Agent": "massive-greeks-snapshot/1.0"
}

# Target underlyings
TARGET_UNDERLYINGS = ["QQQ", "NVDA", "TSLA", "COIN", "AMZN", "AMD", "AAPL", "MSFT", "AVGO", "META"]

# Filters
MAX_DTE = 30
STRIKE_RANGE_PCT = 0.50  # ±50% from spot

def fetch_chain_page(url, headers, params=None):
    """Fetch a single page from the API with retry."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            if r.status_code == 429:  # Rate limit
                wait = int(r.headers.get('Retry-After', 5))
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                print(f"    Error {r.status_code}: {r.text[:200]}")
                return None
            return r.json()
        except Exception as e:
            print(f"    Attempt {attempt+1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    return None

def get_option_chain_snapshot(underlying, headers, limit=250):
    """
    Fetch full chain with pagination.
    Returns list of contract dictionaries with Greeks, IV, OI, etc.
    """
    base_url = f"{BASE}/snapshot/options/{underlying}"
    params = {"limit": limit}
    
    data = fetch_chain_page(base_url, headers, params)
    if not data:
        return []
    
    results = data.get("results", []) or []
    
    # Follow pagination
    next_url = data.get("next_url")
    while next_url:
        data = fetch_chain_page(next_url, headers)
        if not data:
            break
        results.extend(data.get("results", []) or [])
        next_url = data.get("next_url")
        time.sleep(0.1)  # Be nice to API
    
    return results

def parse_option_ticker(ticker):
    """Parse OSI format: O:AAPL251107C00230000"""
    try:
        if not ticker or ':' not in ticker:
            return None
        
        contract = ticker.split(':', 1)[1]
        
        # Extract underlying
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
        
        # Parse date (YYMMDD)
        date_str = rest[:6]
        year = 2000 + int(date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        exp_date = datetime(year, month, day).date()
        
        # Call/Put
        call_put = rest[6]
        if call_put not in ('C', 'P'):
            return None
        
        # Strike
        strike_str = rest[7:15]
        strike = int(strike_str) / 1000.0
        
        return (underlying, exp_date, strike, call_put)
    except:
        return None

def extract_contract_data(contract_dict, data_date):
    """
    Extract relevant fields from Massive chain snapshot contract.
    Returns dict with Greeks, IV, OI, prices, etc.
    """
    details = contract_dict.get("details", {})
    
    # Basic contract info
    ticker = details.get("ticker") or contract_dict.get("ticker")
    
    parsed = parse_option_ticker(ticker)
    if not parsed:
        return None
    
    underlying, exp_date, strike, call_put = parsed
    dte = (exp_date - data_date).days
    
    # Greeks
    greeks = contract_dict.get("greeks") or {}
    
    # Quote data
    last_quote = contract_dict.get("last_quote") or {}
    
    # Trade data
    last_trade = contract_dict.get("last_trade") or {}
    
    # Open Interest
    oi_field = contract_dict.get("open_interest")
    if isinstance(oi_field, dict):
        open_interest = oi_field.get("value")
    else:
        open_interest = oi_field
    
    # Day stats
    day = contract_dict.get("day") or {}
    
    # Underlying asset info (if available)
    underlying_asset = contract_dict.get("underlying_asset") or {}
    
    return {
        'ticker': ticker,
        'underlying': underlying,
        'expiration': exp_date.isoformat(),
        'dte': dte,
        'strike': strike,
        'type': call_put,
        
        # Greeks
        'delta': greeks.get('delta'),
        'gamma': greeks.get('gamma'),
        'theta': greeks.get('theta'),
        'vega': greeks.get('vega'),
        'iv': greeks.get('implied_volatility'),
        
        # Open Interest
        'open_interest': open_interest,
        
        # Prices
        'bid': last_quote.get('bid'),
        'ask': last_quote.get('ask'),
        'bid_size': last_quote.get('bid_size'),
        'ask_size': last_quote.get('ask_size'),
        'last_price': last_trade.get('price'),
        
        # Volume
        'volume': day.get('volume'),
        
        # Break even
        'break_even': contract_dict.get('break_even_price'),
        
        # Underlying price (if available)
        'underlying_price': underlying_asset.get('price'),
    }

def fetch_and_save_greeks(underlying, data_date, out_dir, max_dte, strike_range_pct):
    """
    Fetch Greeks snapshot for one underlying and save to CSV.
    """
    print(f"\n[{underlying}]")
    print(f"  Fetching chain snapshot...")
    
    chain = get_option_chain_snapshot(underlying, HDRS, limit=250)
    
    if not chain:
        print(f"  ✗ No data returned")
        return 0
    
    print(f"  Retrieved {len(chain)} contracts")
    
    # Extract underlying price from chain (for strike filtering)
    spot = None
    for c in chain:
        ua = c.get("underlying_asset") or {}
        if "price" in ua:
            spot = float(ua["price"])
            break
    
    if not spot:
        print(f"  Warning: Could not determine spot price, using wide strike range")
    
    print(f"  Spot price: ${spot:.2f}" if spot else "  Spot price: unknown")
    
    # Process and filter contracts
    filtered = []
    stats = {'dte': 0, 'strike': 0, 'no_greeks': 0}
    
    for contract_dict in chain:
        data = extract_contract_data(contract_dict, data_date)
        if not data:
            continue
        
        # Filter by DTE
        if data['dte'] < 0 or data['dte'] > max_dte:
            stats['dte'] += 1
            continue
        
        # Filter by strike (if we have spot)
        if spot:
            min_strike = spot * (1 - strike_range_pct)
            max_strike = spot * (1 + strike_range_pct)
            if data['strike'] < min_strike or data['strike'] > max_strike:
                stats['strike'] += 1
                continue
        
        # Skip if no Greeks (though rare)
        if data['delta'] is None and data['gamma'] is None:
            stats['no_greeks'] += 1
            continue
        
        filtered.append(data)
    
    print(f"  Kept {len(filtered)} contracts (filtered {stats['dte']} by DTE, {stats['strike']} by strike, {stats['no_greeks']} no Greeks)")
    
    if not filtered:
        print(f"  ✗ No contracts after filtering")
        return 0
    
    # Save to CSV
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{underlying}_greeks.csv")
    
    with open(out_path, 'w', newline='') as f:
        fieldnames = [
            'ticker', 'underlying', 'expiration', 'dte', 'strike', 'type',
            'delta', 'gamma', 'theta', 'vega', 'iv',
            'open_interest', 'bid', 'ask', 'bid_size', 'ask_size', 'last_price',
            'volume', 'break_even', 'underlying_price'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered)
    
    print(f"  ✓ Saved to {underlying}_greeks.csv")
    
    return len(filtered)

def main():
    if len(sys.argv) < 2:
        # Default to today if no date provided
        data_date = datetime.now().date()
        print(f"No date provided, using today: {data_date}")
    else:
        try:
            data_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        except ValueError:
            print("Usage: python fetch_greeks_snapshot.py [YYYY-MM-DD]")
            print("\nFetches option chain with Greeks, IV, and OI via REST API")
            print("If no date provided, uses current date")
            sys.exit(1)
    
    out_dir = f"data/{data_date}/greeks"
    
    print(f"\n{'='*60}")
    print(f"Fetching Greeks Snapshot for {data_date}")
    print(f"{'='*60}")
    print(f"\nUnderlyings: {', '.join(TARGET_UNDERLYINGS)}")
    print(f"Max DTE: {MAX_DTE}")
    print(f"Strike range: ±{int(STRIKE_RANGE_PCT*100)}%")
    print(f"Output: {out_dir}/")
    
    total_contracts = 0
    
    for underlying in TARGET_UNDERLYINGS:
        try:
            count = fetch_and_save_greeks(
                underlying, data_date, out_dir, MAX_DTE, STRIKE_RANGE_PCT
            )
            total_contracts += count
            time.sleep(0.5)  # Rate limit courtesy
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    print(f"\n{'='*60}")
    print(f"✅ Done! Total contracts: {total_contracts}")
    print(f"Data saved to: {out_dir}/")
    print(f"\nNext steps:")
    print(f"  1. Analyze gamma exposure by strike")
    print(f"  2. Calculate pin levels from OI + delta")
    print(f"  3. Build reflexivity pressure indicators")

if __name__ == "__main__":
    main()

