#!/usr/bin/env python3
"""
Intraday Greeks Collector - Run every 15-60 minutes during market hours.

This builds your own historical intraday Greeks database by polling Massive REST API.
Over time, you'll have minute/hour-resolution Greeks history for backtesting.

Usage:
    python collect_intraday_greeks.py          # Single snapshot now
    
Setup cron (runs every 30 min during market hours Mon-Fri):
    */30 9-16 * * 1-5 cd /path/to/project && source .venv/bin/activate && python collect_intraday_greeks.py >> logs/greeks_collection.log 2>&1
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from live_trade_signal import fetch_live_chain

load_dotenv("massive_options_smoke/.env")

# Configuration
TARGET_UNDERLYINGS = ["QQQ", "NVDA", "TSLA", "COIN", "AMZN", "AMD", "AAPL", "MSFT", "AVGO", "META"]
MAX_DTE = 30  # Only keep contracts ≤30 DTE
OUTPUT_BASE = "data/intraday_greeks"

def collect_snapshot():
    """Collect one snapshot for all tickers and save with timestamp."""
    
    now_utc = datetime.utcnow()
    now_et = datetime.now()
    timestamp_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_str = now_et.strftime("%Y-%m-%d")
    time_str = now_et.strftime("%H%M%S")
    
    print(f"\n{'='*70}")
    print(f"📸 Collecting Intraday Greeks Snapshot")
    print(f"{'='*70}")
    print(f"UTC: {timestamp_str}")
    print(f"ET:  {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"{'='*70}\n")
    
    # Create output directory
    out_dir = Path(OUTPUT_BASE) / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    
    total_contracts = 0
    
    for underlying in TARGET_UNDERLYINGS:
        try:
            print(f"[{underlying}] Fetching chain...", end='', flush=True)
            
            # Fetch full chain
            chain_df = fetch_live_chain(underlying)
            
            # Filter by DTE
            chain_df["dte"] = (chain_df["expiration"] - now_et.date()).dt.days
            chain_df = chain_df[
                (chain_df["dte"] >= 0) & 
                (chain_df["dte"] <= MAX_DTE)
            ].copy()
            
            if chain_df.empty:
                print(" No contracts ≤30 DTE")
                continue
            
            # Add collection timestamp
            chain_df["collected_at_utc"] = timestamp_str
            chain_df["collected_at_et"] = now_et.strftime("%Y-%m-%d %H:%M:%S")
            
            # Save to parquet (append mode using filename with timestamp)
            out_file = out_dir / f"{underlying}_{time_str}.parquet"
            chain_df.to_parquet(out_file, index=False)
            
            print(f" ✓ {len(chain_df)} contracts → {out_file.name}")
            total_contracts += len(chain_df)
            
        except Exception as e:
            print(f" ✗ Error: {e}")
    
    print(f"\n{'='*70}")
    print(f"✅ Snapshot complete: {total_contracts} total contracts")
    print(f"📁 Saved to: {out_dir}/")
    print(f"{'='*70}\n")
    
    return total_contracts

def main():
    # Check if during market hours (9:30am - 4:00pm ET, Mon-Fri)
    now_et = datetime.now()
    hour = now_et.hour
    minute = now_et.minute
    weekday = now_et.weekday()  # 0=Monday, 4=Friday
    
    is_market_hours = (
        weekday <= 4 and  # Monday-Friday
        ((hour == 9 and minute >= 30) or (10 <= hour < 16) or (hour == 16 and minute == 0))
    )
    
    if not is_market_hours:
        print(f"⏰ Outside market hours (9:30am-4:00pm ET Mon-Fri)")
        print(f"   Current time: {now_et.strftime('%A %H:%M:%S ET')}")
        print(f"   Skipping collection.")
        return 0
    
    try:
        collect_snapshot()
        return 0
    except Exception as e:
        print(f"❌ Collection failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())

