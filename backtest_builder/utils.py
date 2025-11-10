from __future__ import annotations
from datetime import datetime, date
from zoneinfo import ZoneInfo
import pandas as pd

NY_TZ = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

def parse_as_of(as_of_str: str) -> date:
    return pd.to_datetime(as_of_str).date()

def parse_cut(cut_str: str, as_of: date) -> datetime:
    # e.g., "16:00:00 America/New_York"
    parts = cut_str.split()
    if len(parts) == 2:
        t_str, tz_name = parts
    else:
        t_str, tz_name = parts[0], "America/New_York"
    hh, mm, ss = [int(x) for x in t_str.split(":")]
    tz = ZoneInfo(tz_name)
    return datetime(as_of.year, as_of.month, as_of.day, hh, mm, ss, tzinfo=tz)

def to_utc(dt):
    return dt.astimezone(UTC)

def dte_days(expiry: date, as_of: date) -> int:
    return (expiry - as_of).days

def year_fraction_ACT_365(start_dt: datetime, end_dt: datetime) -> float:
    delta = end_dt - start_dt
    return delta.total_seconds() / (365.0 * 24 * 3600)

def ensure_parquet_or_csv(df: pd.DataFrame, path: str):
    path = str(path)
    if path.endswith(".parquet"):
        df.to_parquet(path, index=False)
    elif path.endswith(".csv"):
        df.to_csv(path, index=False)
    else:
        df.to_parquet(path + ".parquet", index=False)
