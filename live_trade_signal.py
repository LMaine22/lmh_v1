#!/usr/bin/env python3
"""
Live 0-DTE Trade Signal Generator
Fetches current QQQ options data and generates CALL/PUT/NO_TRADE signal.
"""

import os
import sys
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from zero_dte_live_decider import decide_trade, pick_atm_leg

try:
    import databento as db
    DATABENTO_AVAILABLE = True
except ImportError:
    DATABENTO_AVAILABLE = False

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

load_dotenv("massive_options_smoke/.env")

API_KEY = os.environ.get("MASSIVE_API_KEY", "").strip()
if not API_KEY:
    print("ERROR: MASSIVE_API_KEY not found in environment")
    sys.exit(1)

BASE = "https://api.massive.com/v3"
HDRS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "User-Agent": "live-trade-signal/1.0"
}

def fetch_live_chain(underlying: str) -> pd.DataFrame:
    """
    Fetch live options chain with Greeks and OI.
    Returns DataFrame with all necessary columns.
    """
    print(f"📊 Fetching live options chain for {underlying}...")
    
    results = []
    url = f"{BASE}/snapshot/options/{underlying}"
    params = {"limit": 250}
    
    while True:
        r = requests.get(url if "http" in url else f"{BASE}{url}", headers=HDRS, params=params, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"Chain fetch failed: {r.status_code} {r.text[:200]}")
        
        data = r.json()
        page_results = data.get("results", [])
        
        if not page_results:
            break
        
        # Extract fields from each contract
        for contract in page_results:
            details = contract.get("details", {})
            greeks = contract.get("greeks", {})
            last_quote = contract.get("last_quote", {})
            underlying_asset = contract.get("underlying_asset", {})
            
            oi_field = contract.get("open_interest")
            if isinstance(oi_field, dict):
                oi = oi_field.get("value")
            else:
                oi = oi_field
            
            results.append({
                "option_symbol": details.get("ticker") or contract.get("ticker"),
                "strike": details.get("strike_price") or contract.get("strike_price"),
                "expiration": details.get("expiration_date") or contract.get("expiration_date"),
                "cp_flag": (details.get("contract_type") or contract.get("contract_type", "")).upper()[0],
                "gamma": greeks.get("gamma"),
                "delta": greeks.get("delta"),
                "theta": greeks.get("theta"),
                "vega": greeks.get("vega"),
                "iv": greeks.get("implied_volatility"),
                "open_interest": oi,
                "bid": last_quote.get("bid"),
                "ask": last_quote.get("ask"),
                "mid": (last_quote.get("bid") + last_quote.get("ask")) / 2 if (last_quote.get("bid") and last_quote.get("ask")) else None,
                "last_price": contract.get("last_trade", {}).get("price"),
                "volume": contract.get("day", {}).get("volume"),
                "multiplier": 100,
                "underlying_price": underlying_asset.get("price")
            })
        
        # Check for next page
        next_url = data.get("next_url")
        if not next_url:
            break
        
        url = next_url
        params = {}  # next_url already has params
        
        if len(results) > 10000:  # Safety limit
            break
    
    if not results:
        raise RuntimeError(f"No options data returned for {underlying}")
    
    df = pd.DataFrame(results)
    print(f"   ✓ Fetched {len(df)} contracts")
    
    # Filter to 0DTE only
    df["expiration"] = pd.to_datetime(df["expiration"])
    today_ny = pd.Timestamp.now(tz="America/New_York").date()
    df = df[df["expiration"].dt.date == today_ny]
    
    print(f"   ✓ Filtered to {len(df)} 0-DTE contracts for {today_ny}")
    
    # Filter to ATM ±5% strikes only (remove deep ITM/OTM garbage)
    if not df.empty and "underlying_price" in df.columns:
        spot = df["underlying_price"].dropna().iloc[0]
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
        
        # Keep strikes within ±5% of spot
        lower_bound = spot * 0.95
        upper_bound = spot * 1.05
        
        before_filter = len(df)
        df = df[(df["strike"] >= lower_bound) & (df["strike"] <= upper_bound)].copy()
        
        print(f"   ✓ Filtered to {len(df)} liquid strikes (±5% of spot ${spot:.2f})")
        print(f"      Removed {before_filter - len(df)} illiquid deep ITM/OTM strikes")
    
    if df.empty:
        raise RuntimeError("No 0-DTE contracts found for today")
    
    return df

def fetch_minute_bars_databento(ticker: str, lookback_minutes: int = 60) -> pd.DataFrame:
    """
    Fetch REAL minute bars from Databento LIVE API (no delay!).
    Uses EQUS.MINI dataset for real-time equity data.
    
    This connects to the live stream briefly, collects recent bars, then disconnects.
    
    Returns DataFrame with columns: ['ts', 'close', 'volume']
    All timestamps in UTC.
    """
    if not DATABENTO_AVAILABLE:
        raise RuntimeError("databento package not installed. Run: pip install databento")
    
    print(f"📈 Fetching real-time {ticker} bars from Databento Live API...")
    
    # Get your Databento API key from environment
    DATABENTO_KEY = os.environ.get("DATABENTO_API_KEY", "").strip()
    if not DATABENTO_KEY:
        raise RuntimeError("DATABENTO_API_KEY not found in environment. Set it with: export DATABENTO_API_KEY='your_key'")
    
    try:
        import threading
        import queue
        
        # Buffer to collect bars
        bars_buffer = []
        error_holder = [None]
        
        # Create live client
        client = db.Live(key=DATABENTO_KEY)
        
        # Callback to collect bars
        def handle_bar(msg):
            try:
                # Filter for OHLCV messages only (skip metadata like SymbolMapping)
                if not hasattr(msg, 'close') or not hasattr(msg, 'volume'):
                    return  # Skip non-OHLCV messages
                
                # Convert message to dict
                bar = {
                    'ts': pd.Timestamp(msg.ts_event, tz='UTC'),
                    'close': float(msg.close) / 1e9,  # Databento uses fixed-point (nano units)
                    'volume': int(msg.volume)
                }
                bars_buffer.append(bar)
            except Exception as e:
                error_holder[0] = e
        
        # Subscribe to live 1-minute bars
        client.subscribe(
            dataset="DBEQ.BASIC",  # Real-time feed (not delayed like EQUS.MINI)
            schema="ohlcv-1m",
            symbols=[ticker]
        )
        
        client.add_callback(handle_bar)
        
        # Start streaming in background
        client.start()
        
        # Wait briefly to collect some bars (3-5 seconds should get recent bars)
        import time
        print(f"   ⏳ Collecting live bars (waiting 5 seconds)...")
        time.sleep(5)
        
        # Stop streaming
        client.stop()
        
        if error_holder[0]:
            raise error_holder[0]
        
        if not bars_buffer:
            # If no bars collected from live stream, fall back to Historical API for initial load
            print(f"   ⚠️  No bars from live stream yet, using Historical API for backfill...")
            client_hist = db.Historical(DATABENTO_KEY)
            
            end_time = pd.Timestamp.now(tz='UTC')
            start_time = end_time - pd.Timedelta(minutes=lookback_minutes + 10)
            
            data = client_hist.timeseries.get_range(
                dataset='DBEQ.BASIC',  # Real-time feed
                symbols=[ticker],
                schema='ohlcv-1m',
                start=start_time.isoformat(),
                end=end_time.isoformat(),
                stype_in='raw_symbol'
            )
            
            df = data.to_df().reset_index()
            if 'ts_event' in df.columns:
                df = df.rename(columns={'ts_event': 'ts'})
            df['ts'] = pd.to_datetime(df['ts'], utc=True)
            result = df[['ts', 'close', 'volume']].tail(lookback_minutes).copy()
        else:
            # Convert buffer to DataFrame
            df = pd.DataFrame(bars_buffer)
            df = df.sort_values('ts').tail(lookback_minutes)
            result = df[['ts', 'close', 'volume']].copy()
        
        print(f"   ✓ Fetched {len(result)} real-time bars (latest: ${result['close'].iloc[-1]:.2f})")
        print(f"   ✓ Time range: {result['ts'].min()} to {result['ts'].max()}")
        
        return result
        
    except Exception as e:
        raise RuntimeError(f"Databento Live fetch failed: {e}")

def fetch_minute_bars_yfinance(ticker: str, lookback_minutes: int = 60) -> pd.DataFrame:
    """
    Fetch REAL minute bars from Yahoo Finance (free alternative for testing).
    Fetches 2 days to capture yesterday's close for gap analysis.
    
    Returns DataFrame with columns: ['ts', 'close', 'volume']
    All timestamps in UTC.
    """
    if not YFINANCE_AVAILABLE:
        raise RuntimeError("yfinance package not installed. Run: pip install yfinance")
    
    print(f"📈 Fetching real {ticker} bars from Yahoo Finance...")
    
    try:
        stock = yf.Ticker(ticker)
        # Fetch 5 days to get full session data (yfinance limits intraday history)
        # Note: yfinance only keeps last 7 days of 1-minute data
        df = stock.history(period="5d", interval="1m")
        
        if df.empty:
            raise RuntimeError(f"No bars returned from yfinance for {ticker}")
        
        df = df.reset_index()
        df = df.rename(columns={'Datetime': 'ts', 'Close': 'close', 'Volume': 'volume'})
        
        # Ensure timezone aware
        if df['ts'].dt.tz is None:
            df['ts'] = df['ts'].dt.tz_localize('America/New_York')
        df['ts'] = df['ts'].dt.tz_convert('UTC')
        
        # Keep all bars from yesterday + today (for gap calculation)
        # But prioritize today's bars
        result = df[['ts', 'close', 'volume']].copy()
        
        # If we have more than lookback_minutes from today, just take recent
        today_et = pd.Timestamp.now(tz='America/New_York').date()
        result['date'] = result['ts'].dt.tz_convert('America/New_York').dt.date
        today_bars = result[result['date'] == today_et]
        
        if len(today_bars) >= lookback_minutes:
            # Take lookback_minutes from today + add yesterday's last bar for gap calc
            yesterday_bars = result[result['date'] < today_et].tail(1)
            result = pd.concat([yesterday_bars, today_bars.tail(lookback_minutes)])
        
        result = result[['ts', 'close', 'volume']].copy()
        
        print(f"   ✓ Fetched {len(result)} real bars (latest: ${result['close'].iloc[-1]:.2f})")
        print(f"   ✓ Time range: {result['ts'].min()} to {result['ts'].max()}")
        
        return result
        
    except Exception as e:
        raise RuntimeError(f"yfinance fetch failed: {e}")

def fetch_minute_bars_simple(chain_df: pd.DataFrame, lookback_minutes: int = 60) -> pd.DataFrame:
    """
    Create synthetic minute bars from options chain underlying_price.
    Works when stock quotes aren't in your plan (options-only subscription).
    
    This builds a simple time series by tracking the underlying_price field
    in the chain over the recent fetch, with synthetic volume.
    """
    print(f"📈 Creating bars from options chain (options-only plan)...")
    
    # Get current spot from chain
    spot = chain_df["underlying_price"].dropna().iloc[0] if "underlying_price" in chain_df.columns else None
    
    if spot is None:
        raise RuntimeError("Cannot determine underlying price from options chain")
    
    # Create synthetic minute bars (approximation for pressure calc)
    now = pd.Timestamp.now(tz='UTC')
    
    # Generate timestamps for last N minutes
    timestamps = pd.date_range(
        end=now,
        periods=lookback_minutes,
        freq='1T',
        tz='UTC'
    )
    
    # For simplicity: assume spot moved ±0.5% randomly over last hour
    # In reality you'd track actual price changes, but for a quick test this works
    np.random.seed(int(now.timestamp()))
    price_walk = spot + np.random.randn(lookback_minutes) * spot * 0.005
    price_walk[-1] = spot  # Current price is exact
    
    # Synthetic volume (use average from chain if available)
    avg_vol = chain_df["volume"].mean() if "volume" in chain_df.columns else 1000000
    volume = np.random.poisson(avg_vol, lookback_minutes)
    
    df = pd.DataFrame({
        'ts': timestamps,
        'close': price_walk,
        'volume': volume
    })
    
    print(f"   ✓ Generated {len(df)} synthetic bars (spot: ${spot:.2f})")
    print(f"   ⚠️  Note: Using approximation (options-only plan lacks stock data)")
    
    return df

def fetch_minute_bars_smart(ticker: str = "QQQ", lookback_minutes: int = 60, chain_df: pd.DataFrame = None) -> tuple[pd.DataFrame, str]:
    """
    Smart data fetching with automatic fallback:
    1. Try Databento (if API key available)
    2. Fall back to Yahoo Finance (free)
    3. Last resort: synthetic data (requires chain_df)
    
    Returns: (bars_df, data_source_name)
    """
    bars_1m = None
    data_source = "unknown"
    
    # Try Databento first (best quality, institutional data)
    if DATABENTO_AVAILABLE and os.environ.get("DATABENTO_API_KEY"):
        try:
            bars_1m = fetch_minute_bars_databento(ticker, lookback_minutes=lookback_minutes)
            data_source = "Databento"
            return bars_1m, data_source
        except Exception as e:
            print(f"   ⚠️  Databento failed: {e}")
            print(f"   ↳  Falling back to yfinance...")
    
    # Fall back to yfinance (free, good for testing)
    if bars_1m is None and YFINANCE_AVAILABLE:
        try:
            bars_1m = fetch_minute_bars_yfinance(ticker, lookback_minutes=lookback_minutes)
            data_source = "Yahoo Finance"
            return bars_1m, data_source
        except Exception as e:
            print(f"   ⚠️  yfinance failed: {e}")
            print(f"   ↳  Falling back to synthetic data...")
    
    # Last resort: synthetic data (not recommended for live trading)
    if bars_1m is None:
        print(f"   ⚠️  No real data source available")
        print(f"   ↳  Using synthetic data (NOT RECOMMENDED FOR LIVE TRADING)")
        if chain_df is None:
            raise RuntimeError("Synthetic data fallback requires chain_df parameter")
        bars_1m = fetch_minute_bars_simple(chain_df, lookback_minutes=lookback_minutes)
        data_source = "Synthetic (approximation)"
    
    return bars_1m, data_source

def main():
    underlying = "QQQ"
    
    print(f"\n{'='*70}")
    print(f"🎯 LIVE 0-DTE TRADE SIGNAL - {underlying}")
    print(f"{'='*70}")
    # Get current time in ET properly
    from zoneinfo import ZoneInfo
    et_time = datetime.now(ZoneInfo("America/New_York"))
    print(f"Time: {et_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"{'='*70}\n")
    
    try:
        # 1. Fetch live chain with Greeks + OI from Massive
        chain_df = fetch_live_chain(underlying)
        
        # 2. Fetch REAL minute bars (with smart fallback)
        bars_1m = None
        data_source = "unknown"
        
        # Try Databento first (best quality, institutional data)
        if DATABENTO_AVAILABLE and os.environ.get("DATABENTO_API_KEY"):
            try:
                bars_1m = fetch_minute_bars_databento(underlying, lookback_minutes=60)
                data_source = "Databento"
            except Exception as e:
                print(f"   ⚠️  Databento failed: {e}")
                print(f"   ↳  Falling back to yfinance...")
        
        # Fall back to yfinance (free, good for testing)
        if bars_1m is None and YFINANCE_AVAILABLE:
            try:
                bars_1m = fetch_minute_bars_yfinance(underlying, lookback_minutes=60)
                data_source = "Yahoo Finance"
            except Exception as e:
                print(f"   ⚠️  yfinance failed: {e}")
                print(f"   ↳  Falling back to synthetic data...")
        
        # Last resort: synthetic data (not recommended for live trading)
        if bars_1m is None:
            print(f"   ⚠️  No real data source available")
            print(f"   ↳  Using synthetic data (NOT RECOMMENDED FOR LIVE TRADING)")
            bars_1m = fetch_minute_bars_simple(chain_df, lookback_minutes=60)
            data_source = "Synthetic (approximation)"
        
        # 3. Get current spot
        spot = float(bars_1m["close"].iloc[-1])
        print(f"\n💰 Current {underlying} spot: ${spot:.2f}")
        print(f"📡 Data source: {data_source}")
        
        # 4. Make decision
        print(f"\n🧮 Analyzing gamma exposure and hedge pressure...\n")
        decision = decide_trade(chain_df, bars_1m)
        
        # 5. Display results
        print(f"{'='*70}")
        print(f"📢 SIGNAL: {decision.action}")
        print(f"{'='*70}")
        print(f"Confidence: {decision.confidence}")
        print(f"Reason: {decision.reason}")
        print(f"\n📊 Details:")
        print(f"  Max Gamma Strike: ${decision.details.get('max_gamma_strike', 'N/A'):.2f}")
        print(f"  Zero Gamma Strike: ${decision.details.get('zero_gamma_strike', 'N/A'):.2f}" if decision.details.get('zero_gamma_strike') else "  Zero Gamma Strike: N/A")
        print(f"  Distance to Pin: {decision.details.get('distance_to_pin_pct', 0):.2f}%")
        print(f"  Pressure Ratio: {decision.details.get('pressure_ratio', 0):.2f}×")
        print(f"  Pressure Direction: {decision.details.get('direction', 'N/A')}")
        print(f"  Pin Strength: {decision.details.get('pin_strength', 0):.2f}")
        print(f"  Regime: {decision.details.get('regime', 'N/A')}")
        print(f"  Hours to Close: {decision.details.get('hours_to_close', 0):.2f}")
        
        # 6. If signal is CALL or PUT, suggest contract
        if decision.action in ("CALL", "PUT"):
            print(f"\n{'='*70}")
            print(f"💡 SUGGESTED TRADE")
            print(f"{'='*70}")
            
            atm = pick_atm_leg(spot, chain_df, decision.action)
            
            if atm:
                price = atm.get("mid") or atm.get("last") or 0
                qty = atm.get("suggested_qty", 1)
                cost = price * 100 * qty
                
                print(f"  Contract: {atm.get('option_symbol', 'N/A')}")
                print(f"  Strike: ${atm.get('strike', 0):.2f}")
                print(f"  Type: {decision.action}")
                print(f"  Bid: ${atm.get('bid', 0):.2f}")
                print(f"  Ask: ${atm.get('ask', 0):.2f}")
                print(f"  Mid: ${price:.2f}")
                print(f"  Suggested Qty: {qty} contract(s)")
                print(f"  Total Cost: ${cost:.2f}")
                print(f"\n  ⚠️  This is a 0-DTE option - expires TODAY at 4:00pm ET")
                print(f"  ⚠️  Max loss: ${cost:.2f} (100% if expires OTM)")
            else:
                print("  Could not find suitable ATM contract")
        else:
            print(f"\n💤 No trade recommended at this time")
        
        print(f"\n{'='*70}\n")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())

