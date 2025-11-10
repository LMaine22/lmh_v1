#!/usr/bin/env python3
"""
Live Minute-by-Minute Greeks Collector

Runs continuously during market hours, fetching full options chain every minute.
Builds real-time historical data with Greeks, IV, OI exactly like the CSV format.

Usage:
    python live_minute_collector.py
    
Outputs:
    data/live_collected/YYYY-MM-DD/{TICKER}_options.csv
    (Appends new rows every minute)
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv("massive_options_smoke/.env")

from live_trade_signal import fetch_live_chain

# Configuration
TARGET_UNDERLYINGS = ["QQQ", "NVDA", "TSLA", "COIN", "AMZN", "AMD", "AAPL", "MSFT", "AVGO", "META"]
FETCH_INTERVAL_SEC = 60  # Fetch every 60 seconds
MAX_DTE = 30  # Only keep contracts ≤30 DTE
OUTPUT_BASE = "data/live_collected"

def is_market_hours():
    """Check if currently during market hours (9:30am - 4:00pm ET, Mon-Fri)."""
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo("America/New_York"))
    
    # Check weekday (0=Monday, 6=Sunday)
    if now.weekday() > 4:  # Saturday or Sunday
        return False
    
    # Check time
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= now <= market_close

def collect_and_append(underlying: str, date_str: str, timestamp_utc: str, timestamp_et: str):
    """Fetch chain and append to today's CSV file."""
    
    # Fetch live chain
    chain_df = fetch_live_chain(underlying)
    
    if chain_df.empty:
        return 0
    
    # Calculate DTE and filter
    from zoneinfo import ZoneInfo
    now_date = datetime.now(ZoneInfo("America/New_York")).date()
    chain_df["dte"] = (chain_df["expiration"].dt.date - now_date).dt.days
    chain_df = chain_df[
        (chain_df["dte"] >= 0) & 
        (chain_df["dte"] <= MAX_DTE)
    ].copy()
    
    if chain_df.empty:
        return 0
    
    # Add collection timestamps
    chain_df["collected_at_utc"] = timestamp_utc
    chain_df["collected_at_et"] = timestamp_et
    
    # Reformat to match the historical CSV format
    output_df = pd.DataFrame({
        "ticker": chain_df["option_symbol"],
        "underlying": underlying,
        "expiration": chain_df["expiration"].dt.date,
        "dte": chain_df["dte"],
        "strike": chain_df["strike"],
        "type": chain_df["cp_flag"],
        "delta": chain_df["delta"],
        "gamma": chain_df["gamma"],
        "theta": chain_df["theta"],
        "vega": chain_df["vega"],
        "iv": chain_df["iv"],
        "open_interest": chain_df["open_interest"],
        "bid": chain_df["bid"],
        "ask": chain_df["ask"],
        "mid": chain_df["mid"],
        "bid_size": chain_df["bid_size"],
        "ask_size": chain_df["ask_size"],
        "last_price": chain_df["last_price"],
        "last_size": chain_df.get("last_size"),
        "volume": chain_df["volume"],
        "open": chain_df.get("open"),
        "high": chain_df.get("high"),
        "low": chain_df.get("low"),
        "close": chain_df.get("close"),
        "vwap": chain_df.get("vwap"),
        "break_even": chain_df.get("break_even"),
        "underlying_price": chain_df["underlying_price"],
        "collected_at_utc": timestamp_utc,
        "collected_at_et": timestamp_et
    })
    
    # Save/append to CSV
    out_dir = Path(OUTPUT_BASE) / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{underlying}_options.csv"
    
    # Append mode if file exists
    if out_file.exists():
        output_df.to_csv(out_file, mode='a', header=False, index=False)
    else:
        output_df.to_csv(out_file, mode='w', header=True, index=False)
    
    return len(output_df)

def main():
    print(f"\n{'='*80}")
    print(f"🔴 LIVE MINUTE-BY-MINUTE GREEKS COLLECTOR")
    print(f"{'='*80}")
    print(f"Tickers: {', '.join(TARGET_UNDERLYINGS)}")
    print(f"Interval: {FETCH_INTERVAL_SEC} seconds")
    print(f"Max DTE: {MAX_DTE}")
    print(f"Output: {OUTPUT_BASE}/YYYY-MM-DD/{{TICKER}}_options.csv")
    print(f"{'='*80}")
    print(f"Press Ctrl+C to stop\n")
    
    iteration = 0
    
    try:
        while True:
            # Check market hours
            if not is_market_hours():
                from zoneinfo import ZoneInfo
                now = datetime.now(ZoneInfo("America/New_York"))
                print(f"[{now.strftime('%H:%M:%S')}] Outside market hours - sleeping 5 min...")
                time.sleep(300)  # Sleep 5 minutes
                continue
            
            iteration += 1
            from zoneinfo import ZoneInfo
            now_et = datetime.now(ZoneInfo("America/New_York"))
            now_utc = datetime.now(ZoneInfo("UTC"))
            
            date_str = now_et.strftime("%Y-%m-%d")
            timestamp_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            timestamp_et = now_et.strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"[{now_et.strftime('%H:%M:%S')}] Iteration {iteration} - Collecting...", flush=True)
            
            total_contracts = 0
            for underlying in TARGET_UNDERLYINGS:
                try:
                    count = collect_and_append(underlying, date_str, timestamp_utc, timestamp_et)
                    print(f"  {underlying}: {count} contracts", flush=True)
                    total_contracts += count
                    time.sleep(0.5)  # Small delay between tickers
                except Exception as e:
                    print(f"  {underlying}: ERROR - {e}", flush=True)
            
            print(f"  Total: {total_contracts} contracts collected")
            print(f"  Data: {OUTPUT_BASE}/{date_str}/")
            
            # Calculate next fetch time
            elapsed = (datetime.now(ZoneInfo("UTC")) - now_utc).total_seconds()
            sleep_time = max(1, FETCH_INTERVAL_SEC - elapsed)
            
            print(f"  Next fetch in {sleep_time:.0f}s...\n", flush=True)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print(f"\n\n{'='*80}")
        print(f"Stopped by user after {iteration} iterations")
        print(f"{'='*80}\n")
        return 0

if __name__ == "__main__":
    sys.exit(main())

