from __future__ import annotations
import json
import pandas as pd
import numpy as np
from datetime import date, datetime
from typing import Optional, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from .massive_client import MassiveClient
from .utils import parse_as_of, parse_cut, to_utc, dte_days, year_fraction_ACT_365, ensure_parquet_or_csv
from .quality import build_session_cut
from .iv import solve_iv

def fetch_contracts_as_of(client: MassiveClient, underlying: str, as_of: str) -> pd.DataFrame:
    rows = []
    for page in client.list_contracts_as_of(underlying, as_of):
        items = page.get("results") or page.get("data") or page
        if isinstance(items, dict):
            for k in ("contracts","results","data","items"):
                if k in items and isinstance(items[k], list):
                    items = items[k]
                    break
        if isinstance(items, list):
            for it in items:
                rows.append(it)
    if not rows:
        raise RuntimeError("No contracts returned; verify endpoint/params and permissions.")
    df = pd.DataFrame(rows)
    
    # Map Massive API fields to our standard names
    rename = {
        "ticker": "option_symbol",
        "underlying_ticker": "underlier",
        "strike_price": "strike",
        "contract_type": "cp_flag",
        "shares_per_contract": "multiplier",
        "expiration_date": "expiration_date"
    }
    
    # Keep all columns, just rename
    df = df.rename(columns=rename)
    
    # Type conversions
    if "expiration_date" in df.columns:
        df["expiration_date"] = pd.to_datetime(df["expiration_date"]).dt.date
    if "strike" in df.columns:
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    if "cp_flag" in df.columns:
        # contract_type is "call" or "put", convert to "C" or "P"
        df["cp_flag"] = df["cp_flag"].astype(str).str.upper().str[0]
    if "multiplier" not in df.columns:
        df["multiplier"] = 100.0
    
    return df

def dte_filter(df_contracts: pd.DataFrame, as_of: date, dte_max: int) -> pd.DataFrame:
    df = df_contracts.copy()
    df["dte"] = df["expiration_date"].apply(lambda ex: dte_days(ex, as_of))
    return df[(df["dte"] >= 0) & (df["dte"] <= dte_max)].reset_index(drop=True)

def _fetch_quotes_one_symbol(client: MassiveClient, opt_symbol: str, as_of_str: str):
    rows = []
    for page in client.list_option_quotes(opt_symbol, as_of_str):
        items = page.get("results") or page.get("data") or page
        if isinstance(items, dict):
            for k in ("quotes","results","data","items"):
                if k in items and isinstance(items[k], list):
                    items = items[k]
                    break
        if isinstance(items, list):
            for it in items:
                rows.append(it)
    return opt_symbol, rows

def fetch_quotes_for_symbols(client: MassiveClient, symbols, as_of_str: str, max_workers=8) -> pd.DataFrame:
    futures, rows = [], []
    print(f"Fetching quotes for {len(symbols)} contracts (parallel, max_workers={max_workers})...")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for sym in symbols:
            futures.append(ex.submit(_fetch_quotes_one_symbol, client, sym, as_of_str))
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Quotes", unit="contract"):
            sym, lst = fut.result()
            for it in lst:
                it["option_symbol"] = sym
                rows.append(it)
    if not rows:
        raise RuntimeError("No quotes returned; verify permissions or use Flat Files.")
    df = pd.DataFrame(rows)
    rename = {"t": "ts", "timestamp":"ts", "bidPrice":"bid", "askPrice":"ask", "lastPrice":"last"}
    for k,v in rename.items():
        if k in df.columns and v not in df.columns:
            df[v] = df[k]
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
    for col in ("bid","ask","last","bid_size","ask_size"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def fetch_underlier_spot(client: MassiveClient, ticker: str, as_of_str: str, cut_dt_utc) -> float:
    """Fetch underlier spot price at the cut time."""
    rows = []
    for page in client.list_underlier_quotes(ticker, as_of_str):
        items = page.get("results") or page.get("data") or []
        if isinstance(items, list):
            rows.extend(items)
    
    if not rows:
        raise RuntimeError(f"No quotes for underlier {ticker} on {as_of_str}")
    
    df = pd.DataFrame(rows)
    if "t" in df.columns:
        df["ts"] = pd.to_datetime(df["t"], utc=True)
    elif "timestamp" in df.columns:
        df["ts"] = pd.to_datetime(df["timestamp"], utc=True)
    
    # Get last quote before cut
    df = df[df["ts"] <= cut_dt_utc].sort_values("ts")
    if df.empty:
        raise RuntimeError(f"No {ticker} quotes before cut time")
    
    last_row = df.iloc[-1]
    # Prefer last trade, fallback to mid
    if "price" in last_row and pd.notna(last_row["price"]):
        return float(last_row["price"])
    if "last" in last_row and pd.notna(last_row["last"]):
        return float(last_row["last"])
    if "bid" in last_row and "ask" in last_row:
        bid, ask = last_row["bid"], last_row["ask"]
        if pd.notna(bid) and pd.notna(ask) and ask >= bid:
            return float(0.5 * (bid + ask))
    
    raise RuntimeError(f"Cannot extract price from {ticker} quotes")

def fetch_open_interest(client: MassiveClient, underlying: str, as_of_str: str, symbols_set: set) -> Dict[str, float]:
    """Fetch open interest for contracts. Returns dict: symbol -> OI."""
    oi_map = {}
    
    # Try snapshot endpoint which may have OI even if not full historical Greeks
    try:
        print(f"Fetching open interest...")
        for page in tqdm(client.get_open_interest_bulk(underlying, as_of_str), desc="OI pages", unit="page"):
            items = page.get("results") or []
            for item in items:
                details = item.get("details", {})
                sym = details.get("ticker") or item.get("ticker")
                if sym and sym in symbols_set:
                    oi_field = item.get("open_interest")
                    if isinstance(oi_field, dict):
                        oi_val = oi_field.get("value")
                    else:
                        oi_val = oi_field
                    if oi_val is not None:
                        oi_map[sym] = float(oi_val)
    except Exception as e:
        print(f"Warning: Could not fetch OI: {e}")
    
    return oi_map

def compute_iv_and_greeks(joined: pd.DataFrame, S0: float, r_by_expiry: Dict, q: float) -> pd.DataFrame:
    from .greeks import bs_greeks
    out = []
    print(f"Solving IV and computing Greeks for {len(joined)} contracts...")
    for _, r in tqdm(joined.iterrows(), total=len(joined), desc="IV/Greeks", unit="contract"):
        K = float(r["strike"])
        expiry = r["expiration_date"]
        cp = str(r["cp_flag"]).upper().startswith("C")
        T = r["T"]
        px = float(r["px_for_iv"])
        r_rate = float(r_by_expiry.get(expiry, 0.0))
        sigma, meta = solve_iv(px, S0, K, r_rate, q, T, cp)
        g = {"delta": np.nan, "gamma": np.nan, "vega": np.nan, "theta": np.nan, "rho": np.nan}
        if not (np.isnan(sigma) or sigma <= 0):
            g = bs_greeks(S0, K, r_rate, q, sigma, T, cp, float(r.get("multiplier",100.0)))
        out.append({**r.to_dict(), "sigma": sigma, "iv_status": meta.get("status"), **g})
    return pd.DataFrame(out)

def build_chain(underlier: str, as_of: str, cut: str, dte_max: int,
                r_flat: Optional[float] = 0.02, q_flat: Optional[float] = 0.0,
                outdir: Optional[str] = None, client: Optional[MassiveClient] = None,
                underlier_px: Optional[float] = None, use_flat_files: bool = True):
    import os
    as_of_d = parse_as_of(as_of)
    cut_dt_local = parse_cut(cut, as_of_d)
    cut_dt_utc = to_utc(cut_dt_local)
    client = client or MassiveClient()
    os.makedirs(outdir or ".", exist_ok=True)

    # 1) Contracts
    df_contracts = fetch_contracts_as_of(client, underlier, as_of)
    ensure_parquet_or_csv(df_contracts, f"{outdir}/contracts_raw.parquet")

    # 2) DTE
    near = dte_filter(df_contracts, as_of_d, dte_max)
    if near.empty:
        raise RuntimeError(f"No 0–{dte_max} DTE contracts for this as_of.")
    ensure_parquet_or_csv(near, f"{outdir}/contracts_dte_0_30.parquet")

    # 3) Quotes (use Flat Files if available, much faster than REST for bulk)
    if use_flat_files:
        try:
            from .flat_files import fetch_opra_quotes_bulk
            print(f"Using Flat Files for quotes (MUCH faster than REST)...")
            quotes = fetch_opra_quotes_bulk(as_of, set(near["option_symbol"].unique()))
        except Exception as e:
            print(f"Flat Files failed ({e}), falling back to REST API...")
            use_flat_files = False
    
    if not use_flat_files:
        quotes = fetch_quotes_for_symbols(client, near["option_symbol"].unique().tolist(), as_of)
    
    if "ts" not in quotes.columns:
        raise RuntimeError("Quotes payload lacks 'ts' column")
    snap = build_session_cut(quotes, cut_dt_utc)
    joined = near.merge(snap[["option_symbol","px_for_iv","mid","bid","ask","spread","ts"]], on="option_symbol", how="left")
    if joined["px_for_iv"].isna().all():
        raise RuntimeError("No usable quotes at cut time.")
    ensure_parquet_or_csv(joined, f"{outdir}/quotes_joined.parquet")

    # 4) Underlier spot
    S0 = underlier_px
    if S0 is None:
        try:
            S0 = fetch_underlier_spot(client, underlier, as_of, cut_dt_utc)
            print(f"Fetched underlier spot: ${S0:.2f}")
        except Exception as e:
            print(f"Warning: Could not fetch underlier spot ({e}), using fallback heuristic")
            mids = joined.dropna(subset=["mid","strike"]).copy()
            if mids.empty:
                raise RuntimeError("Cannot infer S0; provide --underlier-px")
            mids["atm_gap"] = (mids["mid"] / mids["strike"]).abs()
            approx = mids.sort_values("atm_gap").iloc[0]
            S0 = float(approx["strike"])
            print(f"Inferred underlier spot (fallback): ${S0:.2f}")

    expiries = joined["expiration_date"].dropna().unique().tolist()
    r_by_expiry = {ex: float(r_flat) for ex in expiries}
    q = float(q_flat)

    # 6) T
    joined["T"] = joined["expiration_date"].apply(
        lambda ex: year_fraction_ACT_365(cut_dt_utc, datetime(ex.year, ex.month, ex.day, 16, 0, 0, tzinfo=cut_dt_utc.tzinfo))
    )

    # 6.5) Fetch Open Interest
    print("Fetching open interest...")
    oi_map = fetch_open_interest(client, underlier, as_of, set(near["option_symbol"].unique()))
    if oi_map:
        joined["open_interest"] = joined["option_symbol"].map(oi_map)
        print(f"Retrieved OI for {len(oi_map)} contracts")
    else:
        joined["open_interest"] = np.nan
        print("Warning: No OI data available")

    # 7) IV + Greeks
    solved = compute_iv_and_greeks(joined, S0, r_by_expiry, q)
    ensure_parquet_or_csv(solved, f"{outdir}/iv_solved.parquet")

    chain = solved.copy()
    if "open_interest" in chain.columns and chain["open_interest"].notna().any():
        chain["G_expo_1pct"] = chain["gamma"] * chain["open_interest"] * S0 * 0.01
    else:
        chain["G_expo_1pct"] = np.nan
        print("Warning: Gamma exposure not calculated (missing OI)")
    ensure_parquet_or_csv(chain, f"{outdir}/chain_0_30_with_greeks.parquet")

    gamma_agg = chain.groupby(["expiration_date","strike"], dropna=False)["G_expo_1pct"].sum().reset_index()
    ensure_parquet_or_csv(gamma_agg, f"{outdir}/gamma_expo_by_strike.parquet")

    manifest = {
        "underlier": underlier, "as_of": as_of, "cut": cut, "dte_max": dte_max,
        "S0": S0, "r_flat": r_flat, "q_flat": q_flat,
        "records": {
            "contracts_raw": len(df_contracts),
            "contracts_dte_0_30": len(near),
            "quotes_joined": len(joined),
            "chain": len(chain)
        }
    }
    with open(f"{outdir}/manifest.json","w") as f:
        json.dump(manifest, f, indent=2)
    return {
        "contracts_raw": f"{outdir}/contracts_raw.parquet",
        "contracts_dte_0_30": f"{outdir}/contracts_dte_0_30.parquet",
        "quotes_joined": f"{outdir}/quotes_joined.parquet",
        "iv_solved": f"{outdir}/iv_solved.parquet",
        "chain": f"{outdir}/chain_0_30_with_greeks.parquet",
        "gamma_expo": f"{outdir}/gamma_expo_by_strike.parquet",
        "manifest": f"{outdir}/manifest.json"
    }
