#!/usr/bin/env python3
# ff_fetch_filtered.py
# Download OPRA daily quotes and FILTER to specific underlyings only

import os
import sys
import gzip
import csv
from datetime import datetime
import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

ENDPOINT = "https://files.massive.com"
BUCKET   = "flatfiles"

# Target underlyings - adjust as needed
TARGET_UNDERLYINGS = {
    "QQQ", "NVDA", "TSLA", "COIN", "AMZN", "AMD", "AAPL", "MSFT", "AVGO", "META"
}

def die(msg):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

def s3_client():
    ak = os.getenv("MASSIVE_S3_ACCESS_KEY")
    sk = os.getenv("MASSIVE_S3_SECRET_KEY")
    if not ak or not sk:
        die("Missing S3 credentials. Set in .env")
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

def download_and_filter_streaming(s3, key, out_dir, underlyings):
    """
    Stream download, decompress on-the-fly, and filter to target underlyings.
    Never writes the full 12GB file to disk!
    """
    ensure_dir(out_dir)
    
    print(f"\n[STREAMING DOWNLOAD + FILTER]")
    print(f"Source: s3://{BUCKET}/{key}")
    print(f"Filter: {', '.join(sorted(underlyings))}")
    
    # Get file size
    try:
        response = s3.head_object(Bucket=BUCKET, Key=key)
        file_size = response['ContentLength']
        file_size_gb = file_size / (1024 * 1024 * 1024)
        print(f"File size: {file_size_gb:.2f} GB")
    except Exception as e:
        die(f"Could not find file: {e}")
    
    print("\nDownloading and filtering (this will take 5-15 min depending on connection)...")
    print("Progress: ", end='', flush=True)
    
    # Stream from S3
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    
    # Output files - one per underlying
    out_files = {}
    writers = {}
    
    try:
        # Decompress stream
        with gzip.GzipFile(fileobj=obj['Body']) as gz:
            reader = csv.reader(line.decode('utf-8') for line in gz)
            
            # Read header
            header = next(reader)
            print(f"\nColumns: {len(header)}")
            print(f"Header: {header}")
            
            # Find ticker column (contains OSI format like O:AAPL251107C00230000)
            ticker_col = None
            for i, col in enumerate(header):
                if col.lower() in ('ticker', 'symbol', 'contract'):
                    ticker_col = i
                    print(f"Ticker column: '{col}' (index {i})")
                    break
            
            if ticker_col is None:
                die("Could not find 'ticker' or 'symbol' column. Check CSV schema.")
            
            # Process rows
            rows_read = 0
            rows_kept = 0
            last_pct = 0
            
            for row in reader:
                rows_read += 1
                
                # Progress indicator every 10M rows (146GB = billions of rows)
                if rows_read % 10_000_000 == 0:
                    pct = min(99, int(rows_read / 2_000_000_000 * 100))  # Estimate ~2B rows
                    if pct > last_pct:
                        print(f"{pct}%...", end='', flush=True)
                        last_pct = pct
                
                # Parse ticker to extract underlying
                if len(row) <= ticker_col:
                    continue
                
                ticker = row[ticker_col]
                # OSI format: O:AAPL251107C00230000 or similar
                # Extract underlying from after "O:" and before the date
                if not ticker or ':' not in ticker:
                    continue
                
                # Split on ':' and extract underlying (letters before digits)
                try:
                    contract_part = ticker.split(':', 1)[1]  # After "O:"
                    # Extract letters at start (underlying symbol)
                    underlying = ''
                    for char in contract_part:
                        if char.isalpha():
                            underlying += char
                        else:
                            break
                    
                    if not underlying or underlying not in underlyings:
                        continue
                except:
                    continue
                
                # Keep this row - lazy create file/writer
                if underlying not in out_files:
                    out_path = os.path.join(out_dir, f"{underlying}_quotes.csv")
                    out_files[underlying] = open(out_path, 'w', newline='')
                    writers[underlying] = csv.writer(out_files[underlying])
                    writers[underlying].writerow(header)  # Write header
                    print(f"\n  Created: {underlying}_quotes.csv")
                    print("Progress: ", end='', flush=True)
                
                writers[underlying].writerow(row)
                rows_kept += 1
    
    finally:
        # Close all output files
        for f in out_files.values():
            f.close()
    
    print(f" 100%")
    print(f"\nProcessed: {rows_read:,} total rows")
    print(f"Kept: {rows_kept:,} rows for {len(out_files)} underlyings")
    
    return list(out_files.keys())

def main():
    if len(sys.argv) != 2:
        print("Usage: python ff_fetch_filtered.py YYYY-MM-DD")
        print(f"\nWill download OPRA quotes for {len(TARGET_UNDERLYINGS)} underlyings:")
        print(f"  {', '.join(sorted(TARGET_UNDERLYINGS))}")
        print("\nStreams/filters on-the-fly (never stores full 12GB file)")
        sys.exit(1)
    
    target_date = sys.argv[1]
    try:
        dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        die("Date must be YYYY-MM-DD")
    
    year = f"{dt:%Y}"
    month = f"{dt:%m}"
    date_fname = f"{dt:%Y-%m-%d}"
    
    s3 = s3_client()
    out_base = ensure_dir(f"data/{date_fname}")
    
    # Download and filter OPRA quotes
    opra_key = f"us_options_opra/quotes_v1/{year}/{month}/{date_fname}.csv.gz"
    
    try:
        found_underlyings = download_and_filter_streaming(
            s3, opra_key, out_base, TARGET_UNDERLYINGS
        )
        
        print(f"\n✅ Done! Filtered data saved to: {out_base}/")
        print(f"\nFound data for: {', '.join(sorted(found_underlyings))}")
        
        # Show file sizes
        print("\nFile sizes:")
        for underlying in sorted(found_underlyings):
            path = os.path.join(out_base, f"{underlying}_quotes.csv")
            size_mb = os.path.getsize(path) / (1024 * 1024)
            print(f"  {underlying}_quotes.csv: {size_mb:.1f} MB")
    
    except Exception as e:
        die(f"Download failed: {e}")

if __name__ == "__main__":
    main()

