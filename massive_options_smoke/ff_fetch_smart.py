#!/usr/bin/env python3
# ff_fetch_smart.py
# Download MINUTE AGGREGATES (not ticks) and filter by underlying, DTE, and strike range

import os
import sys
import gzip
import csv
from datetime import datetime, timedelta
import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

ENDPOINT = "https://files.massive.com"
BUCKET   = "flatfiles"

# Target underlyings
TARGET_UNDERLYINGS = {
    "QQQ", "NVDA", "TSLA", "COIN", "AMZN", "AMD", "AAPL", "MSFT", "AVGO", "META"
}

# Filters
MAX_DTE = 30  # Only options expiring within 30 days
STRIKE_RANGE_PCT = 0.50  # Keep strikes within ±50% of underlying price (generous initial filter)

def die(msg):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

def s3_client():
    ak = os.getenv("MASSIVE_S3_ACCESS_KEY")
    sk = os.getenv("MASSIVE_S3_SECRET_KEY")
    if not ak or not sk:
        die("Missing S3 credentials in .env")
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        config=Config(signature_version="s3v4"),
    )

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)
    return p

def parse_option_ticker(ticker):
    """
    Parse OSI format: O:AAPL251107C00230000
    Returns: (underlying, expiration_date, strike, call_put) or None
    """
    try:
        if not ticker or ':' not in ticker:
            return None
        
        contract = ticker.split(':', 1)[1]  # Remove "O:"
        
        # Extract underlying (letters at start)
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
        
        # Rest is: YYMMDD + C/P + strike (8 digits, price * 1000)
        rest = contract[idx:]
        if len(rest) < 15:  # Need at least YYMMDD + C/P + 8 digits
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
        
        # Strike (next 8 digits, divide by 1000)
        strike_str = rest[7:15]
        strike = int(strike_str) / 1000.0
        
        return (underlying, exp_date, strike, call_put)
    except:
        return None

def get_underlying_prices(s3, data_date, underlyings):
    """
    Download stocks minute aggs and extract approximate price for each underlying.
    Uses the open price from the first minute bar.
    """
    print("\n[1/2] Getting underlying prices from stocks data...")
    
    year = f"{data_date:%Y}"
    month = f"{data_date:%m}"
    date_fname = f"{data_date:%Y-%m-%d}"
    
    stocks_key = f"us_stocks_sip/minute_aggs_v1/{year}/{month}/{date_fname}.csv.gz"
    
    prices = {}
    
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=stocks_key)
        
        with gzip.GzipFile(fileobj=obj['Body']) as gz:
            reader = csv.DictReader(line.decode('utf-8') for line in gz)
            
            for row in reader:
                ticker = row.get('ticker', '').upper()
                if ticker in underlyings and ticker not in prices:
                    # Use open price as reference
                    price = float(row.get('open', 0))
                    if price > 0:
                        prices[ticker] = price
                        print(f"  {ticker}: ${price:.2f}")
                
                # Stop once we have all
                if len(prices) == len(underlyings):
                    break
        
        if not prices:
            print("  Warning: Could not get prices from stocks data")
            print("  Will use wide strike range as fallback")
    
    except Exception as e:
        print(f"  Warning: {e}")
        print("  Will use wide strike range as fallback")
    
    return prices

def download_and_filter_options(s3, data_date, out_dir, underlyings, max_dte, underlying_prices):
    """
    Download options minute aggregates and filter by underlying, DTE, and strike range.
    """
    print(f"\n[2/2] Downloading and filtering options minute aggregates...")
    print(f"Filters:")
    print(f"  Underlyings: {', '.join(sorted(underlyings))}")
    print(f"  Max DTE: {max_dte}")
    print(f"  Strike range: ±{int(STRIKE_RANGE_PCT*100)}% from underlying price")
    
    year = f"{data_date:%Y}"
    month = f"{data_date:%m}"
    date_fname = f"{data_date:%Y-%m-%d}"
    
    options_key = f"us_options_opra/minute_aggs_v1/{year}/{month}/{date_fname}.csv.gz"
    
    # Get file size
    try:
        response = s3.head_object(Bucket=BUCKET, Key=options_key)
        file_size = response['ContentLength']
        file_size_mb = file_size / (1024 * 1024)
        print(f"\nFile size: {file_size_mb:.1f} MB")
    except Exception as e:
        die(f"Could not find file: {e}")
    
    max_exp_date = data_date + timedelta(days=max_dte)
    
    print(f"\nStreaming and filtering...")
    print("Progress: ", end='', flush=True)
    
    obj = s3.get_object(Bucket=BUCKET, Key=options_key)
    
    out_files = {}
    writers = {}
    
    rows_read = 0
    rows_kept = 0
    last_pct = 0
    
    stats = {
        'filtered_underlying': 0,
        'filtered_dte': 0,
        'filtered_strike': 0,
    }
    
    try:
        with gzip.GzipFile(fileobj=obj['Body']) as gz:
            reader = csv.reader(line.decode('utf-8') for line in gz)
            
            header = next(reader)
            print(f"\nColumns: {header}")
            
            # Find ticker column
            ticker_col = 0  # Usually first column
            
            for row in reader:
                rows_read += 1
                
                # Progress every 1M rows
                if rows_read % 1_000_000 == 0:
                    pct = min(99, int(rows_read / 50_000_000 * 100))  # Estimate ~50M rows for minute aggs
                    if pct > last_pct:
                        print(f"{pct}%...", end='', flush=True)
                        last_pct = pct
                
                if len(row) <= ticker_col:
                    continue
                
                ticker = row[ticker_col]
                parsed = parse_option_ticker(ticker)
                
                if not parsed:
                    continue
                
                underlying, exp_date, strike, call_put = parsed
                
                # Filter 1: Underlying
                if underlying not in underlyings:
                    stats['filtered_underlying'] += 1
                    continue
                
                # Filter 2: DTE
                dte = (exp_date - data_date).days
                if dte < 0 or dte > max_dte:
                    stats['filtered_dte'] += 1
                    continue
                
                # Filter 3: Strike range (if we have underlying price)
                if underlying in underlying_prices:
                    ref_price = underlying_prices[underlying]
                    min_strike = ref_price * (1 - STRIKE_RANGE_PCT)
                    max_strike = ref_price * (1 + STRIKE_RANGE_PCT)
                    
                    if strike < min_strike or strike > max_strike:
                        stats['filtered_strike'] += 1
                        continue
                
                # Keep this row
                if underlying not in out_files:
                    out_path = os.path.join(out_dir, f"{underlying}_options.csv")
                    out_files[underlying] = open(out_path, 'w', newline='')
                    writers[underlying] = csv.writer(out_files[underlying])
                    writers[underlying].writerow(header)
                    print(f"\n  Created: {underlying}_options.csv")
                    print("Progress: ", end='', flush=True)
                
                writers[underlying].writerow(row)
                rows_kept += 1
    
    finally:
        for f in out_files.values():
            f.close()
    
    print(f" 100%")
    print(f"\nProcessed: {rows_read:,} total rows")
    print(f"Kept: {rows_kept:,} rows ({100*rows_kept/rows_read:.2f}%)")
    print(f"\nFiltered out:")
    print(f"  Wrong underlying: {stats['filtered_underlying']:,}")
    print(f"  DTE > {max_dte}: {stats['filtered_dte']:,}")
    print(f"  Strike out of range: {stats['filtered_strike']:,}")
    
    return list(out_files.keys())

def main():
    if len(sys.argv) != 2:
        print("Usage: python ff_fetch_smart.py YYYY-MM-DD")
        print(f"\nDownloads MINUTE AGGREGATES (not tick quotes) with smart filtering:")
        print(f"  • Underlyings: {', '.join(sorted(TARGET_UNDERLYINGS))}")
        print(f"  • Max DTE: {MAX_DTE} days")
        print(f"  • Strike range: ±{int(STRIKE_RANGE_PCT*100)}% from underlying")
        print(f"\nResult: ~100-500 MB instead of 146 GB")
        sys.exit(1)
    
    target_date = sys.argv[1]
    try:
        data_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        die("Date must be YYYY-MM-DD")
    
    s3 = s3_client()
    out_base = ensure_dir(f"data/{target_date}")
    
    # Step 1: Get underlying prices
    underlying_prices = get_underlying_prices(s3, data_date, TARGET_UNDERLYINGS)
    
    # Step 2: Download and filter options
    found = download_and_filter_options(
        s3, data_date, out_base, TARGET_UNDERLYINGS, MAX_DTE, underlying_prices
    )
    
    print(f"\n✅ Done! Filtered data saved to: {out_base}/")
    print(f"\nFound data for: {', '.join(sorted(found))}")
    
    # Show file sizes
    print("\nFile sizes:")
    total_mb = 0
    for underlying in sorted(found):
        path = os.path.join(out_base, f"{underlying}_options.csv")
        size_mb = os.path.getsize(path) / (1024 * 1024)
        total_mb += size_mb
        print(f"  {underlying}_options.csv: {size_mb:.1f} MB")
    
    print(f"\nTotal: {total_mb:.1f} MB (vs 146 GB unfiltered)")

if __name__ == "__main__":
    main()

