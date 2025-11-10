"""Flat Files bulk fetcher for fast historical data loading."""
from __future__ import annotations
import os
import gzip
import csv
import pandas as pd
from typing import Set
from tqdm import tqdm
import boto3
from botocore.config import Config

ENDPOINT = "https://files.massive.com"
BUCKET = "flatfiles"

def s3_client():
    ak = os.getenv("MASSIVE_S3_ACCESS_KEY")
    sk = os.getenv("MASSIVE_S3_SECRET_KEY")
    if not ak or not sk:
        raise ValueError("Missing S3 credentials in environment")
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        config=Config(signature_version="s3v4"),
    )

def fetch_opra_quotes_bulk(date_str: str, symbols_set: Set[str]) -> pd.DataFrame:
    """
    Download OPRA quotes flat file and filter to target symbols.
    Much faster than individual REST calls.
    
    Args:
        date_str: YYYY-MM-DD
        symbols_set: Set of option tickers to keep
    
    Returns:
        DataFrame with columns: option_symbol, ts, bid, ask, bid_size, ask_size, etc.
    """
    s3 = s3_client()
    
    # Parse date for S3 path
    year, month, day = date_str.split('-')
    s3_key = f"us_options_opra/quotes_v1/{year}/{month}/{date_str}.csv.gz"
    
    print(f"Downloading flat file: {s3_key}")
    
    # Get file size
    try:
        response = s3.head_object(Bucket=BUCKET, Key=s3_key)
        file_size_gb = response['ContentLength'] / (1024**3)
        print(f"File size: {file_size_gb:.1f} GB (filtering to {len(symbols_set)} contracts)")
    except Exception as e:
        raise RuntimeError(f"Could not find flat file: {e}")
    
    # Stream, decompress, filter
    obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
    
    rows_kept = []
    rows_total = 0
    
    print("Streaming and filtering...")
    with gzip.GzipFile(fileobj=obj['Body']) as gz:
        reader = csv.reader(line.decode('utf-8') for line in gz)
        header = next(reader)
        
        # Find ticker column
        ticker_col = header.index('ticker') if 'ticker' in header else 0
        
        for row in tqdm(reader, desc="Processing quotes", unit=" rows", unit_scale=True):
            rows_total += 1
            
            if len(row) <= ticker_col:
                continue
            
            ticker = row[ticker_col]
            if ticker in symbols_set:
                rows_kept.append(dict(zip(header, row)))
            
            # Progress checkpoint
            if rows_total % 50_000_000 == 0:
                print(f"  Kept {len(rows_kept):,} / {rows_total:,} rows so far...")
    
    if not rows_kept:
        raise RuntimeError(f"No quotes found for target symbols in {s3_key}")
    
    print(f"\nKept {len(rows_kept):,} quotes from {rows_total:,} total rows")
    
    df = pd.DataFrame(rows_kept)
    
    # Normalize columns
    rename = {
        "ticker": "option_symbol",
        "sip_timestamp": "ts",
        "bid_price": "bid",
        "ask_price": "ask",
        "bid_size": "bid_size",
        "ask_size": "ask_size"
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    
    # Convert types
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], unit='ns', utc=True)
    for col in ("bid", "ask", "bid_size", "ask_size"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    return df

def fetch_underlier_quotes_bulk(date_str: str, ticker: str) -> pd.DataFrame:
    """Fetch underlier (stock) quotes from flat files."""
    s3 = s3_client()
    
    year, month, day = date_str.split('-')
    s3_key = f"us_stocks_sip/quotes_v1/{year}/{month}/{date_str}.csv.gz"
    
    print(f"Fetching underlier quotes from: {s3_key}")
    
    obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
    
    rows_kept = []
    with gzip.GzipFile(fileobj=obj['Body']) as gz:
        reader = csv.reader(line.decode('utf-8') for line in gz)
        header = next(reader)
        ticker_col = header.index('ticker') if 'ticker' in header else 0
        
        for row in tqdm(reader, desc=f"Filtering {ticker}", unit=" rows", unit_scale=True):
            if len(row) > ticker_col and row[ticker_col] == ticker:
                rows_kept.append(dict(zip(header, row)))
    
    if not rows_kept:
        raise RuntimeError(f"No quotes found for {ticker}")
    
    df = pd.DataFrame(rows_kept)
    
    # Normalize
    rename = {"sip_timestamp": "ts", "ticker": "ticker"}
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], unit='ns', utc=True)
    
    return df

