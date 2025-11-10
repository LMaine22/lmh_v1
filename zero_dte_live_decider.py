
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import date
import pandas as pd
import numpy as np

try:
    from zoneinfo import ZoneInfo
except Exception:
    from backports.zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

@dataclass
class Decision:
    action: str                   # "CALL", "PUT", or "NO_TRADE"
    confidence: str               # "LOW", "MEDIUM", "HIGH", "VERY_HIGH"
    reason: str                   # human-readable explanation
    details: Dict[str, Any]       # raw metrics useful for logging

def _to_date_ny(ts_utc: pd.Timestamp) -> date:
    return ts_utc.tz_convert(NY).date()

def _hours_to_close(ts_utc: pd.Timestamp) -> float:
    """Approximate hours to 4:00pm ET for the given UTC ts."""
    ts_ny = ts_utc.tz_convert(NY)
    close_dt = ts_ny.replace(hour=16, minute=0, second=0, microsecond=0)
    return (close_dt - ts_ny).total_seconds() / 3600.0

def _ensure_cols(df: pd.DataFrame, col_map: Dict[str, str]) -> pd.DataFrame:
    """Rename flexible incoming schema to our canonical names if available."""
    out = df.copy()
    for src, dst in col_map.items():
        if src in out.columns and dst not in out.columns:
            out = out.rename(columns={src: dst})
    return out

def compute_gamma_profile(chain_df: pd.DataFrame, spot: float, today_ny: date) -> Dict[str, Any]:
    """
    Returns:
      {
        'per_strike': pd.DataFrame[strike, net_gamma_signed, abs_gamma, total_oi],
        'max_gamma_strike': float,
        'max_gamma_abs': float,
        'zero_gamma_strike': Optional[float],
        'pin_strength': float
      }
    Notes:
      - We approximate dealer positioning as short to customer OI (dealer sign = -1).
      - BS gamma is positive for both calls & puts; we use sign via dealer_position.
    """
    df = chain_df.copy()

    # Canonicalize column names we'll use
    df = _ensure_cols(df, {
        "option_symbol": "symbol",
        "cp_flag": "cp_flag",
        "type": "cp_flag",
        "strike_price": "strike",
        "last_price": "last",
        "bidPrice": "bid",
        "askPrice": "ask",
        "midPrice": "mid",
        "expiration_date": "expiration",
        "underlier_price": "underlier_price",
    })
    for needed in ["strike", "gamma", "open_interest", "cp_flag", "expiration"]:
        if needed not in df.columns:
            raise ValueError(f"compute_gamma_profile: missing required column '{needed}'")
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["gamma"] = pd.to_numeric(df["gamma"], errors="coerce")
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").fillna(0)
    df["multiplier"] = pd.to_numeric(df.get("multiplier", 100), errors="coerce").fillna(100.0)

    # 0DTE filter (today's expiry in NY time)
    df["expiration"] = pd.to_datetime(df["expiration"]).dt.tz_localize(None).dt.date
    df = df[df["expiration"] == today_ny].copy()
    if df.empty:
        raise ValueError("No 0DTE contracts for today in provided chain. (Check timezone/expiry fields.)")

    # Compute dealer-signed gamma notionals scaled to a 1% underlying move
    # OI represents customer positions; dealers naturally hold opposite side
    # No negation needed - formula assumes OI is customer long, so dealers are implicitly short
    dealer_pos = df["open_interest"].values
    gamma = df["gamma"].values
    mult = df["multiplier"].values
    g_1pct = dealer_pos * gamma * (spot * 0.01) * mult  # signed

    df["g_expo_1pct_signed"] = g_1pct
    df["g_expo_1pct_abs"] = np.abs(g_1pct)

    per_strike = df.groupby("strike").agg(
        net_gamma_signed=("g_expo_1pct_signed", "sum"),
        abs_gamma=("g_expo_1pct_abs", "sum"),
        total_oi=("open_interest", "sum")
    ).reset_index().sort_values("strike")

    # Max gamma (by magnitude)
    idx_max = per_strike["abs_gamma"].idxmax()
    max_gamma_strike = float(per_strike.loc[idx_max, "strike"])
    max_gamma_abs = float(per_strike.loc[idx_max, "abs_gamma"])

    # Zero-gamma: strike where net_gamma_signed crosses zero (linear interpolation)
    zgs = None
    s = per_strike["strike"].values
    g = per_strike["net_gamma_signed"].values
    for i in range(1, len(s)):
        if g[i-1] == 0:
            zgs = float(s[i-1])
            break
        if (g[i-1] < 0 and g[i] > 0) or (g[i-1] > 0 and g[i] < 0):
            w = abs(g[i-1]) / (abs(g[i-1]) + abs(g[i]))
            zgs = float(s[i-1] * (1 - w) + s[i] * w)
            break

    pin_strength = float(max_gamma_abs / (per_strike["abs_gamma"].sum() + 1e-9))

    # Debug output to understand gamma distribution
    print(f"\n🔍 DEBUG: Gamma Profile Analysis")
    print(f"   Spot: ${spot:.2f}")
    print(f"   Max Gamma Strike: ${max_gamma_strike:.2f}")
    print(f"   Zero Gamma Strike: ${zgs:.2f}" if zgs else "   Zero Gamma Strike: None")
    
    # OI Breakdown by Type
    print(f"\n   📊 OI Breakdown by Type:")
    call_oi = df[df['cp_flag'].str.upper().str[0] == 'C']['open_interest'].sum()
    put_oi = df[df['cp_flag'].str.upper().str[0] == 'P']['open_interest'].sum()
    print(f"      Total Call OI: {call_oi:,.0f}")
    print(f"      Total Put OI: {put_oi:,.0f}")
    print(f"      Put/Call Ratio: {put_oi/call_oi:.2f}" if call_oi > 0 else "      Put/Call Ratio: N/A")
    
    # Check if gamma signs make sense
    call_gamma_sum = df[df['cp_flag'].str.upper().str[0] == 'C']['g_expo_1pct_signed'].sum()
    put_gamma_sum = df[df['cp_flag'].str.upper().str[0] == 'P']['g_expo_1pct_signed'].sum()
    print(f"      Net Call Gamma: {call_gamma_sum:+,.0f}")
    print(f"      Net Put Gamma: {put_gamma_sum:+,.0f}")
    
    # Show top 5 strikes by gamma
    print(f"\n   Top 5 Gamma Strikes:")
    top_5 = per_strike.nlargest(5, 'abs_gamma')[['strike', 'net_gamma_signed', 'total_oi']]
    for _, row in top_5.iterrows():
        print(f"      ${row['strike']:.2f}: Gamma={row['net_gamma_signed']:+,.0f}, OI={row['total_oi']:,.0f}")
    
    # Show cumulative gamma at key levels
    below_spot = per_strike[per_strike['strike'] <= spot]['net_gamma_signed'].sum()
    above_spot = per_strike[per_strike['strike'] > spot]['net_gamma_signed'].sum()
    total = below_spot + above_spot
    
    print(f"\n   Cumulative Gamma:")
    print(f"      Below ${spot:.2f}: {below_spot:+,.0f}")
    print(f"      Above ${spot:.2f}: {above_spot:+,.0f}")
    print(f"      Total: {total:+,.0f}")
    
    if zgs:
        dist_to_zgs = ((spot - zgs) / spot) * 100
        print(f"      Zero-Gamma Distance: {dist_to_zgs:+.1f}% from spot")
    
    print()  # Blank line

    return {
        "per_strike": per_strike,
        "max_gamma_strike": max_gamma_strike,
        "max_gamma_abs": max_gamma_abs,
        "zero_gamma_strike": zgs,
        "pin_strength": pin_strength
    }

def compute_session_context(bars_1m: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze the FULL trading session including GAP from yesterday's close.
    
    Returns context about:
    - Yesterday's close (to calculate gap)
    - Where we opened (with gap %)
    - Day's range (high/low)
    - VWAP (key institutional level)
    - Current position in range
    - Trend direction (including gap)
    - Whether we're at extremes
    - Gap fill status
    """
    bars = bars_1m.copy()
    bars = bars.sort_values("ts")
    
    # Market hours: 9:30 AM - 4:00 PM ET
    bars['ts'] = pd.to_datetime(bars['ts'], utc=True)
    bars['ts_et'] = bars['ts'].dt.tz_convert('America/New_York')
    
    # Filter to today's regular trading session (9:30 AM - 4:00 PM)
    today = bars['ts_et'].iloc[-1].date()
    session_start = pd.Timestamp(today).replace(hour=9, minute=30, second=0).tz_localize('America/New_York')
    session_end = pd.Timestamp(today).replace(hour=16, minute=0, second=0).tz_localize('America/New_York')
    
    session_bars = bars[
        (bars['ts_et'] >= session_start) & 
        (bars['ts_et'] < session_end)  # Use < not <= to exclude after-hours
    ].copy()
    
    # DEBUG: Print to verify session bars are correct
    print(f"   🔍 DEBUG: Session bars: {len(session_bars)} minutes")
    if len(session_bars) > 0:
        print(f"      First bar: {session_bars['ts_et'].iloc[0]}")
        print(f"      Last bar: {session_bars['ts_et'].iloc[-1]}")
    
    # Get yesterday's close (last bar before today's session)
    yesterday_bars = bars[bars['ts_et'] < session_start]
    prev_close = float(yesterday_bars['close'].iloc[-1]) if len(yesterday_bars) > 0 else None
    
    if len(session_bars) < 10:
        # Not enough data (market just opened)
        open_price = session_bars['close'].iloc[0] if len(session_bars) > 0 else None
        gap_pct = ((open_price - prev_close) / prev_close * 100) if (prev_close and open_price) else 0
        
        return {
            'open': open_price,
            'prev_close': prev_close,
            'gap_pct': gap_pct,
            'high': None,
            'low': None,
            'vwap': None,
            'current': session_bars['close'].iloc[-1] if len(session_bars) > 0 else None,
            'minutes_into_session': len(session_bars),
            'insufficient_data': True
        }
    
    # Key metrics
    open_price = float(session_bars['close'].iloc[0])  # First bar of session
    high = float(session_bars['close'].max())
    low = float(session_bars['close'].min())
    current = float(session_bars['close'].iloc[-1])
    
    # GAP ANALYSIS (critical for context!)
    gap_pct = ((open_price - prev_close) / prev_close * 100) if prev_close else 0
    gap_size = open_price - prev_close if prev_close else 0
    
    # Gap fill analysis
    if prev_close:
        if gap_pct > 0.2:  # Gap up
            gap_filled = current <= prev_close  # Filled if we traded back below yesterday's close
            gap_fill_pct = min(100, ((open_price - current) / gap_size * 100)) if gap_size > 0 else 0
            gap_type = "GAP_UP"
        elif gap_pct < -0.2:  # Gap down
            gap_filled = current >= prev_close  # Filled if we traded back above yesterday's close
            gap_fill_pct = min(100, ((current - open_price) / abs(gap_size) * 100)) if gap_size != 0 else 0
            gap_type = "GAP_DOWN"
        else:
            gap_filled = False
            gap_fill_pct = 0
            gap_type = "NO_GAP"
    else:
        gap_filled = False
        gap_fill_pct = 0
        gap_type = "UNKNOWN"
    
    # VWAP (volume-weighted average price)
    total_dollar_volume = (session_bars['close'] * session_bars['volume']).sum()
    total_volume = session_bars['volume'].sum()
    vwap = float(total_dollar_volume / total_volume) if total_volume > 0 else open_price
    
    # Range metrics
    range_size = high - low
    range_pct = (range_size / open_price) * 100
    position_in_range = (current - low) / range_size if range_size > 0 else 0.5
    
    # Distance from key levels
    dist_from_open_pct = ((current - open_price) / open_price) * 100
    dist_from_prev_close_pct = ((current - prev_close) / prev_close * 100) if prev_close else dist_from_open_pct
    dist_from_vwap_pct = ((current - vwap) / vwap) * 100
    dist_from_high = high - current
    dist_from_low = current - low
    
    # Trend classification (using TOTAL move from yesterday, not just intraday)
    if dist_from_prev_close_pct > 0.3:
        trend = "BULLISH"
    elif dist_from_prev_close_pct < -0.3:
        trend = "BEARISH"
    else:
        trend = "FLAT"
    
    # Intraday trend (different from total trend)
    if dist_from_open_pct > 0.3:
        intraday_trend = "BULLISH"
    elif dist_from_open_pct < -0.3:
        intraday_trend = "BEARISH"
    else:
        intraday_trend = "FLAT"
    
    # Position classification
    if position_in_range > 0.75:
        position = "NEAR_HIGH"
    elif position_in_range < 0.25:
        position = "NEAR_LOW"
    else:
        position = "MID_RANGE"
    
    # VWAP relationship (institutions care about this)
    if current > vwap * 1.002:  # >0.2% above VWAP
        vwap_position = "ABOVE_VWAP"
    elif current < vwap * 0.998:  # >0.2% below VWAP
        vwap_position = "BELOW_VWAP"
    else:
        vwap_position = "AT_VWAP"
    
    # Time into session
    minutes_into_session = len(session_bars)
    hours_into_session = minutes_into_session / 60.0
    
    # Session phase
    if hours_into_session < 1.0:
        session_phase = "EARLY_SESSION"  # 9:30-10:30
    elif hours_into_session < 3.5:
        session_phase = "MIDDAY"  # 10:30-1:00
    elif hours_into_session < 6.0:
        session_phase = "LATE_SESSION"  # 1:00-3:30
    else:
        session_phase = "CLOSING"  # 3:30-4:00
    
    return {
        'open': open_price,
        'prev_close': prev_close,
        'gap_pct': gap_pct,
        'gap_size': gap_size,
        'gap_type': gap_type,
        'gap_filled': gap_filled,
        'gap_fill_pct': gap_fill_pct,
        'high': high,
        'low': low,
        'current': current,
        'vwap': vwap,
        'range_size': range_size,
        'range_pct': range_pct,
        'position_in_range': position_in_range,
        'dist_from_open_pct': dist_from_open_pct,
        'dist_from_prev_close_pct': dist_from_prev_close_pct,
        'dist_from_vwap_pct': dist_from_vwap_pct,
        'dist_from_high': dist_from_high,
        'dist_from_low': dist_from_low,
        'trend': trend,
        'intraday_trend': intraday_trend,
        'position': position,
        'vwap_position': vwap_position,
        'minutes_into_session': minutes_into_session,
        'hours_into_session': hours_into_session,
        'session_phase': session_phase,
        'insufficient_data': False
    }

def compute_hedge_pressure(chain_df: pd.DataFrame, bars_1m: pd.DataFrame, session_ctx: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Compute hedge pressure relative to SESSION BASELINE (not just last 30 minutes).
    
    Key improvements:
    - Compare current pressure to session average
    - Check if pressure is aligned with day's trend
    - Consider where we are in the session (early/late matters)
    """
    bars = bars_1m.copy()
    bars = _ensure_cols(bars, {"timestamp": "ts", "price": "close"})
    for col in ["ts","close","volume"]:
        if col not in bars.columns:
            raise ValueError(f"compute_hedge_pressure: bars missing column '{col}'")

    bars["ts"] = pd.to_datetime(bars["ts"], utc=True)
    bars = bars.sort_values("ts")
    if len(bars) < 6:
        raise ValueError("Need at least 6 minutes of bars")

    # Get session bars (since 9:30 AM)
    bars['ts_et'] = bars['ts'].dt.tz_convert('America/New_York')
    today = bars['ts_et'].iloc[-1].date()
    session_start = pd.Timestamp(today).replace(hour=9, minute=30).tz_localize('America/New_York')
    session_bars = bars[bars['ts_et'] >= session_start].copy()
    
    if len(session_bars) < 10:
        # Market just opened, use what we have
        session_bars = bars.tail(30)

    close = bars["close"].values
    spot = float(close[-1])
    
    # Recent move (last 5 minutes for momentum)
    lookback = 5
    dS_recent = close[-1] - close[-lookback-1]
    
    # Check consistency over last 5 minutes
    moves_1m = np.diff(close[-lookback-1:])
    up_moves = np.sum(moves_1m > 0)
    down_moves = np.sum(moves_1m < 0)
    consistent = (up_moves >= 3) or (down_moves >= 3)
    
    # SESSION-WIDE METRICS (this is the key addition)
    session_close = session_bars['close'].values
    session_moves = np.diff(session_close)
    session_up_pct = np.sum(session_moves > 0) / len(session_moves) if len(session_moves) > 0 else 0.5
    
    # Dollar volume baseline
    dollar_vol = session_bars["close"] * session_bars["volume"]
    
    # If we have limited data (< 120 minutes), use median instead of mean
    # (mean gets skewed by spikes when we have short windows)
    if len(session_bars) < 120:
        # Limited data - use robust median and wider window
        session_avg_volume = float(dollar_vol.median())
        recent_volume = float(dollar_vol.tail(10).median())  # 10-min median instead of 5-min mean
        print(f"      ⚠️  Limited session data ({len(session_bars)}m) - using median baseline")
    else:
        # Full session - use mean
        session_avg_volume = float(dollar_vol.mean())
        recent_volume = float(dollar_vol.tail(5).mean())
    
    baseline = session_avg_volume

    df = chain_df.copy()
    df = _ensure_cols(df, {"expiration_date": "expiration"})
    if "expiration" in df.columns:
        now_ny = _to_date_ny(bars["ts"].iloc[-1])
        df["expiration"] = pd.to_datetime(df["expiration"]).dt.tz_localize(None).dt.date
        df = df[df["expiration"] == now_ny].copy()

    for needed in ["gamma", "open_interest"]:
        if needed not in df.columns:
            raise ValueError(f"compute_hedge_pressure: missing column '{needed}'")
    df["gamma"] = pd.to_numeric(df["gamma"], errors="coerce").fillna(0.0)
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").fillna(0.0)
    df["multiplier"] = pd.to_numeric(df.get("multiplier", 100), errors="coerce").fillna(100.0)

    # Compute hedge requirement for recent move
    dealer_pos = df["open_interest"].values
    hedge_shares = float(np.nansum(dealer_pos * df["gamma"].values * dS_recent * df["multiplier"].values))
    hedge_dollars = hedge_shares * spot

    # Pressure ratio vs SESSION BASELINE (not just recent 30 min)
    pressure_ratio = abs(hedge_dollars) / (baseline + 1e-9)
    
    # Volume surge detection
    volume_surge = recent_volume / session_avg_volume if session_avg_volume > 0 else 1.0

    # Direction
    direction = "FLAT"
    if hedge_dollars > 0 and consistent:
        direction = "BULLISH"
    elif hedge_dollars < 0 and consistent:
        direction = "BEARISH"
    
    # CONTEXT-AWARE ADJUSTMENTS
    if session_ctx:
        session_trend = session_ctx.get('trend', 'FLAT')
        trend_aligned = (
            (direction == "BULLISH" and session_trend == "BULLISH") or
            (direction == "BEARISH" and session_trend == "BEARISH")
        )
        at_extreme = session_ctx.get('position') in ['NEAR_HIGH', 'NEAR_LOW']
        
        # Adjust pressure based on context
        adjusted_pressure = pressure_ratio
        
        if trend_aligned:
            # Pressure with the trend is more significant
            adjusted_pressure *= 1.3
        else:
            # Counter-trend pressure might be reversal (also significant)
            adjusted_pressure *= 1.2
        
        if at_extreme:
            # Pressure at extremes matters more (breakout or reversal)
            adjusted_pressure *= 1.2
        
        if volume_surge > 1.5:
            # Volume confirmation amplifies pressure
            adjusted_pressure *= 1.1
    else:
        trend_aligned = False
        adjusted_pressure = pressure_ratio

    print(f"\n   💹 PRESSURE ANALYSIS:")
    print(f"      Recent move (5m): {dS_recent:+.2f} ({'consistent' if consistent else 'choppy'})")
    print(f"      Individual moves: {', '.join([f'{m:+.2f}' for m in moves_1m])}")
    print(f"      Hedge pressure: {pressure_ratio:.2f}× (vs session avg)")
    if session_ctx:
        print(f"      Adjusted pressure: {adjusted_pressure:.2f}× (with context)")
        print(f"      Trend alignment: {'YES' if trend_aligned else 'NO'} ({direction} vs {session_ctx.get('trend', 'FLAT')})")
        print(f"      Volume surge: {volume_surge:.2f}×")

    return {
        "ts": pd.to_datetime(bars['ts'].iloc[-1]),
        "spot": spot,
        "dS": dS_recent,
        "hedge_shares": hedge_shares,
        "hedge_dollars": hedge_dollars,
        "baseline_dollars": baseline,
        "pressure_ratio": pressure_ratio,
        "adjusted_pressure": adjusted_pressure,  # NEW
        "direction": direction,
        "consistent": consistent,
        "trend_aligned": trend_aligned if session_ctx else False,  # NEW
        "volume_surge": volume_surge if session_ctx else 1.0,  # NEW
        "session_up_pct": session_up_pct if session_ctx else 0.5  # NEW
    }

def decide_trade(chain_df: pd.DataFrame,
                 bars_1m: pd.DataFrame,
                 now_ts_utc: Optional[pd.Timestamp] = None,
                 min_pressure: float = 2.5,  # Back to 2.5 with session context
                 min_distance_for_momentum_pct: float = 1.0,
                 close_avoid_minutes: int = 15
                 ) -> Decision:
    """
    One-shot decision using FULL SESSION CONTEXT.
    """
    if now_ts_utc is None:
        now_ts_utc = pd.to_datetime(bars_1m["ts"].iloc[-1]).tz_convert("UTC")

    now_ny_date = _to_date_ny(now_ts_utc)
    spot = float(pd.to_numeric(bars_1m.sort_values("ts")["close"].iloc[-1]))

    # COMPUTE SESSION CONTEXT FIRST
    session_ctx = compute_session_context(bars_1m)
    
    print(f"\n   📊 SESSION CONTEXT:")
    if not session_ctx.get('insufficient_data'):
        # Show gap context first (most important!)
        if session_ctx.get('prev_close'):
            print(f"      Previous close: ${session_ctx['prev_close']:.2f}")
            if abs(session_ctx['gap_pct']) > 0.2:
                gap_emoji = "⬆️" if session_ctx['gap_pct'] > 0 else "⬇️"
                print(f"      {gap_emoji} GAP {session_ctx['gap_type']}: {session_ctx['gap_pct']:+.2f}% (${session_ctx['gap_size']:+.2f})")
                if session_ctx['gap_filled']:
                    print(f"         ⚠️  Gap FILLED - returned to prev close!")
                elif session_ctx['gap_fill_pct'] > 50:
                    print(f"         ⚠️  Gap {session_ctx['gap_fill_pct']:.0f}% filled - potential fade")
        
        print(f"      Open: ${session_ctx['open']:.2f} | High: ${session_ctx['high']:.2f} | Low: ${session_ctx['low']:.2f}")
        print(f"      VWAP: ${session_ctx['vwap']:.2f} | Current: ${session_ctx['current']:.2f}")
        print(f"      TOTAL trend (from prev close): {session_ctx['trend']} ({session_ctx['dist_from_prev_close_pct']:+.2f}%)")
        print(f"      Intraday (from open): {session_ctx['intraday_trend']} ({session_ctx['dist_from_open_pct']:+.2f}%)")
        print(f"      Position: {session_ctx['position']} ({session_ctx['position_in_range']*100:.0f}% of range)")
        print(f"      VWAP status: {session_ctx['vwap_position']} ({session_ctx['dist_from_vwap_pct']:+.2f}%)")
        print(f"      Session phase: {session_ctx['session_phase']} ({session_ctx['hours_into_session']:.1f}h)")

    # Compute gamma profile
    gp = compute_gamma_profile(chain_df, spot, now_ny_date)
    max_gamma_strike = gp["max_gamma_strike"]
    zgs = gp["zero_gamma_strike"]
    distance_to_pin_pct = abs(spot - max_gamma_strike) / spot * 100.0

    # Check if gamma wall aligns with session levels
    gamma_near_vwap = False
    gamma_near_high = False
    if not session_ctx.get('insufficient_data'):
        gamma_near_vwap = abs(max_gamma_strike - session_ctx['vwap']) / session_ctx['vwap'] < 0.005
        gamma_near_high = abs(max_gamma_strike - session_ctx['high']) / session_ctx['high'] < 0.005
        
        print(f"      Max gamma wall: ${max_gamma_strike:.2f} (strongest pin level)")
        if gamma_near_vwap:
            print(f"      ⚠️  Gamma wall ${max_gamma_strike:.2f} coincides with VWAP ${session_ctx['vwap']:.2f} - major pivot!")
        if gamma_near_high:
            print(f"      ⚠️  Gamma wall ${max_gamma_strike:.2f} coincides with session high ${session_ctx['high']:.2f} - resistance!")

    # Compute pressure with context
    hp = compute_hedge_pressure(chain_df, bars_1m, session_ctx)
    pressure_ratio = hp.get("adjusted_pressure", hp["pressure_ratio"])  # Use adjusted pressure
    raw_pressure = hp["pressure_ratio"]
    pressure_dir = hp["direction"]
    dS = hp["dS"]
    consistent = hp.get("consistent", False)
    trend_aligned = hp.get("trend_aligned", False)

    hrs_to_close = _hours_to_close(now_ts_utc)
    
    # Early exits
    if hrs_to_close*60 <= close_avoid_minutes:
        return Decision(
            action="NO_TRADE",
            confidence="HIGH",
            reason=f"Final {close_avoid_minutes} minutes: avoid EOD chaos",
            details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "hours_to_close": hrs_to_close}
        )

    if session_ctx.get('insufficient_data'):
        return Decision(
            action="NO_TRADE",
            confidence="LOW",
            reason="Insufficient session data (market just opened)",
            details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "hours_to_close": hrs_to_close}
        )

    regime = "SHORT_GAMMA" if (zgs and spot > zgs) else "LONG_GAMMA"

    # Reject choppy markets
    if pressure_dir == "FLAT":
        return Decision(
            action="NO_TRADE",
            confidence="LOW",
            reason=f"Choppy price action - no consistent direction",
            details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close}
        )
    
    if not consistent:
        return Decision(
            action="NO_TRADE",
            confidence="LOW",
            reason=f"Price moves inconsistent - wait for clearer signal",
            details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close}
        )

    # NEW RULE: PIN DEFENSE (range-bound at gamma wall)
    if distance_to_pin_pct <= 0.3:  # Within 0.3% of pin
        # Check if we've been stuck here (last 10 bars from session_bars)
        bars_for_range = bars_1m.sort_values("ts")
        recent_bars = bars_for_range.tail(10)
        recent_range = recent_bars['close'].max() - recent_bars['close'].min()
        recent_range_pct = (recent_range / spot) * 100
        
        if recent_range_pct < 0.3:  # Last 10 minutes < 0.3% range = PINNED
            # Check if at session extreme (might break out)
            if session_ctx['position'] in ['NEAR_HIGH', 'NEAR_LOW']:
                # At extreme + pinned = potential breakout setup
                # But need STRONG pressure to confirm
                
                if pressure_ratio >= 3.5:
                    # Strong pressure at pin + extreme = breakout imminent
                    return Decision(
                        action="CALL" if pressure_dir == "BULLISH" else "PUT",
                        confidence="HIGH",
                        reason=f"PIN BREAKOUT SETUP: Strong {pressure_dir} pressure {pressure_ratio:.2f}× at ${max_gamma_strike:.2f} pin + session {session_ctx['position'].lower()}",
                        details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close, "pinned": True}
                    )
            
            # Default: pinned without breakout pressure
            position_desc = session_ctx.get('position', 'unknown').lower().replace('_', ' ')
            return Decision(
                action="NO_TRADE",
                confidence="HIGH",  # HIGH confidence NO_TRADE
                reason=f"PIN DEFENSE: Pinned at ${max_gamma_strike:.2f} gamma wall + {position_desc}, range-bound expected (dealers hedging keeps price stable)",
                details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close, "pinned": True}
            )

    # CONTEXT-AWARE DECISION RULES
    
    # RULE 1: Strong pressure with trend alignment
    if pressure_ratio >= 2.5 and trend_aligned:
        if pressure_dir == "BULLISH":
            confidence = "HIGH" if pressure_ratio >= 3.5 else "MEDIUM"
            
            # Boost confidence if multiple factors align
            reason_extra = ""
            if gamma_near_vwap and session_ctx['vwap_position'] == "ABOVE_VWAP":
                confidence = "HIGH"
                reason_extra = " (VWAP support + gamma wall)"
            elif session_ctx['position'] == "MID_RANGE":
                reason_extra = " (mid-range, room to run)"
            
            return Decision(
                action="CALL",
                confidence=confidence,
                reason=f"Dealer BUY pressure {pressure_ratio:.2f}× aligned with {session_ctx['trend']} day{reason_extra}",
                details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close}
            )
        
        elif pressure_dir == "BEARISH":
            confidence = "HIGH" if pressure_ratio >= 3.5 else "MEDIUM"
            
            reason_extra = ""
            if gamma_near_vwap and session_ctx['vwap_position'] == "BELOW_VWAP":
                confidence = "HIGH"
                reason_extra = " (VWAP resistance + gamma wall)"
            elif session_ctx['position'] == "MID_RANGE":
                reason_extra = " (mid-range, room to fall)"
            
            return Decision(
                action="PUT",
                confidence=confidence,
                reason=f"Dealer SELL pressure {pressure_ratio:.2f}× aligned with {session_ctx['trend']} day{reason_extra}",
                details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close}
            )
    
    # RULE 2: Counter-trend pressure at extremes (REVERSAL setup)
    if pressure_ratio >= 3.0 and not trend_aligned and session_ctx['position'] in ['NEAR_HIGH', 'NEAR_LOW']:
        if pressure_dir == "BEARISH" and session_ctx['position'] == "NEAR_HIGH":
            return Decision(
                action="PUT",
                confidence="MEDIUM",
                reason=f"Counter-trend SELL pressure {pressure_ratio:.2f}× at session high - potential reversal",
                details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close}
            )
        elif pressure_dir == "BULLISH" and session_ctx['position'] == "NEAR_LOW":
            return Decision(
                action="CALL",
                confidence="MEDIUM",
                reason=f"Counter-trend BUY pressure {pressure_ratio:.2f}× at session low - potential reversal",
                details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close}
            )
    
    # RULE 3: Pin magnetism WITH context
    if 0.3 <= distance_to_pin_pct <= 2.0 and pressure_ratio >= 2.0:
        pin_dir = "BULLISH" if max_gamma_strike > spot else "BEARISH"
        
        if pin_dir == pressure_dir:
            # Pressure pushing toward pin
            return Decision(
                action="CALL" if pin_dir == "BULLISH" else "PUT",
                confidence="MEDIUM",
                reason=f"Pressure {pressure_ratio:.2f}× pushing toward gamma pin @ ${max_gamma_strike:.2f}",
                details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close}
            )
    
    # Default: No trade - be specific about WHY
    if trend_aligned and pressure_ratio >= 2.0:
        reason = f"Pressure {pressure_ratio:.2f}× aligned with {session_ctx.get('trend', 'UNKNOWN')} trend but below 2.5× threshold"
        confidence = "LOW"
    elif not trend_aligned:
        reason = f"Pressure {pressure_ratio:.2f}× counter to {session_ctx.get('trend', 'UNKNOWN')} trend - waiting for alignment"
        confidence = "LOW"
    elif not consistent:
        reason = f"Price action inconsistent - no clear directional conviction"
        confidence = "LOW"
    else:
        reason = f"Pressure {pressure_ratio:.2f}× too weak (need >2.5×)"
        confidence = "LOW"
    
    return Decision(
        action="NO_TRADE",
        confidence=confidence,
        reason=reason,
        details={**gp, **hp, **session_ctx, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime, "hours_to_close": hrs_to_close}
    )

def pick_atm_leg(spot: float, expiry_chain: pd.DataFrame, kind: str) -> Optional[Dict[str, Any]]:
    """
    Choose a single ATM contract (CALL or PUT) from today's expiry for a $50-$200 position.
    Returns dict with 'symbol','mid','bid','ask','strike','cp_flag','suggested_qty' (contracts)
    """
    df = expiry_chain.copy()
    df = _ensure_cols(df, {
        "last_price": "last",
        "bidPrice": "bid",
        "askPrice": "ask",
        "midPrice": "mid",
        "expiration_date": "expiration",
        "type": "cp_flag",
    })
    df["expiration"] = pd.to_datetime(df["expiration"]).dt.tz_localize(None).dt.date
    today_ny = date.today()
    df = df[df["expiration"] == today_ny]
    df = df[df["cp_flag"].str.upper().str[0] == ( "C" if kind.upper()=="CALL" else "P" )].copy()
    if df.empty:
        return None
    df["mid"] = pd.to_numeric(df.get("mid", np.nan), errors="coerce")
    if df["mid"].isna().all():
        df["bid"] = pd.to_numeric(df.get("bid", np.nan), errors="coerce")
        df["ask"] = pd.to_numeric(df.get("ask", np.nan), errors="coerce")
        df["mid"] = (df["bid"] + df["ask"]) / 2.0
    df["dist"] = (df["strike"] - spot).abs()
    atm = df.sort_values(["dist","mid"]).iloc[0].to_dict()
    price_per_contract = float(atm.get("mid") or atm.get("last") or 0.0)
    if not np.isfinite(price_per_contract) or price_per_contract <= 0:
        qty = 1
    else:
        budget = 150.0
        qty = max(1, int(budget / (price_per_contract * 100.0)))
    atm["suggested_qty"] = qty
    return atm
