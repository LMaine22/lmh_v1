#!/usr/bin/env python3
# ff_fetch_minute_aggs.py
# Download minute aggregates (much smaller than tick quotes) for options + stocks

import os
import sys
import gzip
import shutil
from datetime import datetime
import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

ENDPOINT = "https://files.massive.com"
BUCKET   = "flatfiles"

def die(msg):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

def s3_client():
    ak = os.getenv("MASSIVE_S3_ACCESS_KEY")
    sk = os.getenv("MASSIVE_S3_SECRET_KEY")
    if not ak or not sk:
        die("Missing S3 credentials. Set MASSIVE_S3_ACCESS_KEY and MASSIVE_S3_SECRET_KEY in .env")
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

def download_with_progress(s3, key, local):
    """Download with progress indicator."""
    ensure_dir(os.path.dirname(local))
    
    # Get file size
    try:
        response = s3.head_object(Bucket=BUCKET, Key=key)
        file_size = response['ContentLength']
        file_size_mb = file_size / (1024 * 1024)
        print(f"  → {os.path.basename(key)} ({file_size_mb:.1f} MB)")
    except Exception as e:
        print(f"  → {os.path.basename(key)}")
        file_size = None
    
    # Progress callback
    class Progress:
        def __init__(self, size):
            self._size = size
            self._seen = 0
            self._last_pct = -1
            
        def __call__(self, bytes_amount):
            self._seen += bytes_amount
            if self._size:
                pct = int((self._seen / self._size) * 100)
                if pct >= self._last_pct + 20:  # Print every 20%
                    print(f"     {pct}%...", end='', flush=True)
                    self._last_pct = pct
    
    progress = Progress(file_size) if file_size else None
    s3.download_file(BUCKET, key, local, Callback=progress)
    print(f" ✓")
    return local

def ungzip(gz_path):
    """Extract gzipped file."""
    out_path = gz_path[:-3]
    print(f"  Extracting...")
    with gzip.open(gz_path, "rb") as src, open(out_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    os.remove(gz_path)  # Remove .gz after extraction
    print(f"  ✓ {os.path.basename(out_path)}")
    return out_path

def main():
    if len(sys.argv) != 2:
        print("Usage: python ff_fetch_minute_aggs.py YYYY-MM-DD")
        print("\nDownloads minute aggregates (compact, suitable for backtesting):")
        print("  - Options minute aggs (all strikes/expiries)")
        print("  - Stocks minute aggs (QQQ underlying)")
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
    out_base = ensure_dir(f"flatfiles_dl/{date_fname}")
    
    print(f"\n=== Downloading minute aggregates for {date_fname} ===\n")
    
    # 1) Options minute aggregates
    print("[1/2] Options minute aggregates (OPRA)...")
    opra_key = f"us_options_opra/minute_aggs_v1/{year}/{month}/{date_fname}.csv.gz"
    try:
        local = download_with_progress(s3, opra_key, os.path.join(out_base, "opra_minute_aggs", f"{date_fname}.csv.gz"))
        ungzip(local)
    except Exception as e:
        print(f"  ✗ Failed: {e}")
    
    # 2) Stocks minute aggregates (for QQQ)
    print("\n[2/2] Stocks minute aggregates (QQQ underlying)...")
    stocks_key = f"us_stocks_sip/minute_aggs_v1/{year}/{month}/{date_fname}.csv.gz"
    try:
        local = download_with_progress(s3, stocks_key, os.path.join(out_base, "stocks_minute_aggs", f"{date_fname}.csv.gz"))
        ungzip(local)
    except Exception as e:
        print(f"  ✗ Failed: {e}")
    
    print(f"\n✅ Done! Files saved to: {out_base}/")
    print("\nNext steps:")
    print("  1. Filter options minute aggs for QQQ contracts")
    print("  2. Build replay engine to test gamma exposure strategies")

if __name__ == "__main__":
    main()

