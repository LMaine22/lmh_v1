#!/usr/bin/env python3
# explore_flat_files.py
# Discover what's actually in the flat files datasets

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

def s3_client():
    ak = os.getenv("MASSIVE_S3_ACCESS_KEY")
    sk = os.getenv("MASSIVE_S3_SECRET_KEY")
    if not ak or not sk:
        print("ERROR: Missing S3 credentials")
        sys.exit(1)
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        config=Config(signature_version="s3v4"),
    )

def peek_file(s3, key, max_rows=5):
    """Download and peek at first few rows of a file."""
    print(f"\n{'='*70}")
    print(f"File: {key}")
    print(f"{'='*70}")
    
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        
        with gzip.GzipFile(fileobj=obj['Body']) as gz:
            reader = csv.reader(line.decode('utf-8') for line in gz)
            
            # Header
            header = next(reader)
            print(f"\nColumns ({len(header)}):")
            for i, col in enumerate(header):
                print(f"  {i:2d}. {col}")
            
            # Sample rows
            print(f"\nFirst {max_rows} rows:")
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                print(f"\nRow {i+1}:")
                for col, val in zip(header, row):
                    if val:  # Only show non-empty
                        print(f"  {col}: {val}")
        
        return header
    
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def main():
    test_date = "2025-11-07"  # A date we know has data
    
    s3 = s3_client()
    
    print(f"\n{'#'*70}")
    print(f"# EXPLORING MASSIVE FLAT FILES FOR {test_date}")
    print(f"{'#'*70}")
    
    datasets_to_check = [
        ("OPTIONS DAY AGGREGATES", f"us_options_opra/day_aggs_v1/2025/11/{test_date}.csv.gz"),
        ("OPTIONS MINUTE AGGREGATES", f"us_options_opra/minute_aggs_v1/2025/11/{test_date}.csv.gz"),
        ("OPTIONS QUOTES", f"us_options_opra/quotes_v1/2025/11/{test_date}.csv.gz"),
        ("OPTIONS TRADES", f"us_options_opra/trades_v1/2025/11/{test_date}.csv.gz"),
    ]
    
    for name, key in datasets_to_check:
        print(f"\n\n{'#'*70}")
        print(f"# {name}")
        print(f"{'#'*70}")
        
        header = peek_file(s3, key, max_rows=2)
        
        if header:
            # Check for Greeks
            greeks_cols = [c for c in header if any(g in c.lower() for g in ['delta', 'gamma', 'theta', 'vega', 'iv', 'implied'])]
            oi_cols = [c for c in header if 'open' in c.lower() and 'interest' in c.lower()]
            
            if greeks_cols or oi_cols:
                print(f"\n✅ FOUND GREEKS/OI COLUMNS:")
                for col in greeks_cols + oi_cols:
                    print(f"   • {col}")
            else:
                print(f"\n❌ No Greeks or OI columns detected")
        
        input("\nPress Enter to continue to next dataset...")

if __name__ == "__main__":
    main()

