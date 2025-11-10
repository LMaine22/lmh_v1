#!/usr/bin/env python3
# ff_list_and_fetch.py
# NO placeholders. Lists available Massive Flat Files keys, then downloads the day you ask for.

import os
import sys
import gzip
import shutil
import re
from datetime import datetime
import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Load .env file
load_dotenv()

ENDPOINT = "https://files.massive.com"
BUCKET   = "flatfiles"
REQ_ENV = ("MASSIVE_S3_ACCESS_KEY", "MASSIVE_S3_SECRET_KEY")

def die(msg):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

def s3_client():
    for k in REQ_ENV:
        if not os.getenv(k):
            die(f"Missing env var {k}. Set your Massive Flat Files S3 credentials in .env file.")
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.getenv("MASSIVE_S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MASSIVE_S3_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
    )

def list_keys_with_prefix(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)
    return p

def download(s3, key, out_dir):
    local = os.path.join(out_dir, os.path.basename(key))
    ensure_dir(out_dir)
    
    # Get file size first
    try:
        response = s3.head_object(Bucket=BUCKET, Key=key)
        file_size = response['ContentLength']
        file_size_mb = file_size / (1024 * 1024)
        print(f"[DL] s3://{BUCKET}/{key} ({file_size_mb:.1f} MB)")
        print(f"     -> {local}")
    except Exception as e:
        print(f"[DL] s3://{BUCKET}/{key}")
        print(f"     -> {local}")
        file_size = None
    
    # Download with progress callback
    class ProgressPercentage:
        def __init__(self, size):
            self._size = size
            self._seen_so_far = 0
            self._last_print = 0
            
        def __call__(self, bytes_amount):
            self._seen_so_far += bytes_amount
            if self._size:
                percentage = (self._seen_so_far / self._size) * 100
                # Print every 10%
                if int(percentage / 10) > self._last_print:
                    print(f"     Progress: {percentage:.0f}% ({self._seen_so_far / (1024*1024):.1f} MB / {self._size / (1024*1024):.1f} MB)")
                    self._last_print = int(percentage / 10)
    
    progress = ProgressPercentage(file_size) if file_size else None
    s3.download_file(BUCKET, key, local, Callback=progress)
    print(f"     ✓ Downloaded")
    return local

def ungzip_if_needed(path):
    if path.endswith(".gz"):
        out = path[:-3]
        print(f"[UNGZIP] {path} -> {out}")
        with gzip.open(path, "rb") as src, open(out, "wb") as dst:
            shutil.copyfileobj(src, dst)
        return out
    return path

def main():
    if len(sys.argv) != 2:
        print("Usage: python ff_list_and_fetch.py YYYY-MM-DD")
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
    
    # 1) Discover available datasets under flatfiles root
    print("=== Listing top-level families under flatfiles/ ===")
    families = set()
    for key in list_keys_with_prefix(s3, ""):
        top = key.split("/", 1)[0]
        if top and not top.endswith(".csv") and not top.endswith(".gz"):
            families.add(top)
    
    for fam in sorted(families):
        print("  -", fam)
    
    # 2) Try to find OPRA datasets
    print("\n=== Discovering OPRA datasets (us_options_opra) ===")
    opra_prefix = "us_options_opra/"
    found = list(list_keys_with_prefix(s3, opra_prefix))
    if not found:
        die("No OPRA flat files visible for your plan yet (us_options_opra not found).")
    
    sample = sorted(set(k.split("/")[1] for k in found if len(k.split("/")) > 1))
    print("OPRA sub-datasets detected:", ", ".join(sorted(sample)))
    
    # Heuristics to find quotes/trades/open_interest datasets
    opra_quotes_root = next((f"us_options_opra/{d}/" for d in sample if "quote" in d.lower()), None)
    opra_trades_root = next((f"us_options_opra/{d}/" for d in sample if "trade" in d.lower()), None)
    opra_oi_root     = next((f"us_options_opra/{d}/" for d in sample if "open" in d.lower() and "interest" in d.lower()), None)
    
    # 3) Optional: Stocks minute aggregates for underlying (QQQ)
    print("\n=== Checking for stocks datasets (QQQ minute aggregates) ===")
    stocks_prefix = "us_stocks_sip/"
    stocks_present = any(k.startswith(stocks_prefix) for k in found)
    
    if not stocks_present:
        # Try listing directly
        stocks_keys = list(list_keys_with_prefix(s3, stocks_prefix))
        stocks_present = len(stocks_keys) > 0
        if stocks_present:
            found.extend(stocks_keys)
    
    if stocks_present:
        # Probe minute aggregates dataset name by scanning children under us_stocks_sip/
        stocks_children = set()
        for key in list_keys_with_prefix(s3, stocks_prefix):
            parts = key.split("/")
            if len(parts) >= 2 and parts[1]:
                stocks_children.add(parts[1])
        
        print("Stocks sub-datasets detected:", ", ".join(sorted(stocks_children)))
        minute_root = next((f"{stocks_prefix}{d}/" for d in stocks_children if "minute" in d.lower()), None)
    else:
        minute_root = None
        print("No stocks flat files detected for your plan (minute aggregates for QQQ may be unavailable).")
    
    out_base = ensure_dir(f"flatfiles_dl/{date_fname}")
    
    # Download OPRA QUOTES for the target day (if present)
    if opra_quotes_root:
        day_key = f"{opra_quotes_root}{year}/{month}/{date_fname}.csv.gz"
        try:
            local = download(s3, day_key, os.path.join(out_base, "opra_quotes"))
            ungzip_if_needed(local)
        except Exception as e:
            print(f"[WARN] Could not fetch OPRA quotes for {date_fname}: {e}")
    else:
        print("[INFO] Could not identify OPRA quotes dataset name automatically.")
    
    # Download OPRA TRADES for the target day (if present)
    if opra_trades_root:
        day_key = f"{opra_trades_root}{year}/{month}/{date_fname}.csv.gz"
        try:
            local = download(s3, day_key, os.path.join(out_base, "opra_trades"))
            ungzip_if_needed(local)
        except Exception as e:
            print(f"[WARN] Could not fetch OPRA trades for {date_fname}: {e}")
    
    # Download OPRA daily Open Interest (if present)
    if opra_oi_root:
        # Many vendors store OI daily without month subdir; try both patterns
        candidates = [
            f"{opra_oi_root}{year}/{month}/{date_fname}.csv.gz",
            f"{opra_oi_root}{year}/{date_fname}.csv.gz",
        ]
        fetched = False
        for key in candidates:
            try:
                local = download(s3, key, os.path.join(out_base, "opra_open_interest"))
                ungzip_if_needed(local)
                fetched = True
                break
            except Exception:
                pass
        
        if not fetched:
            print(f"[WARN] Could not find OPRA OI file for {date_fname} (tried {', '.join(candidates)}).")
    
    # Download Stocks minute aggregates for the day (QQQ)
    if minute_root:
        # Dataset naming varies; we'll search for keys that contain the date and QQQ
        print(f"\n=== Searching {minute_root} for QQQ {date_fname} ===")
        qqq_candidates = []
        for k in list_keys_with_prefix(s3, f"{minute_root}{year}/"):
            if date_fname in k and ("QQQ" in k or "qqq" in k):
                qqq_candidates.append(k)
        
        if qqq_candidates:
            key = sorted(qqq_candidates)[0]
            local = download(s3, key, os.path.join(out_base, "stocks_minute"))
            ungzip_if_needed(local)
        else:
            print("[INFO] No obvious QQQ minute file found for that date (minute dataset naming differs by feed).")
    
    print("\n✅ Done. Files (if any) saved under:", out_base)

if __name__ == "__main__":
    main()

