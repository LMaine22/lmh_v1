
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
    dealer_pos = -df["open_interest"].values
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

    return {
        "per_strike": per_strike,
        "max_gamma_strike": max_gamma_strike,
        "max_gamma_abs": max_gamma_abs,
        "zero_gamma_strike": zgs,
        "pin_strength": pin_strength
    }

def compute_hedge_pressure(chain_df: pd.DataFrame, bars_1m: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute minute-by-minute hedge pressure and classify the latest minute.

    Inputs:
      chain_df: DataFrame with columns ['gamma','open_interest','multiplier'(opt), ...]
                All rows should be for today's expiry (0DTE) already, or include 'expiration' to filter.
      bars_1m: 1-minute bars with columns ['ts','close','volume'] (ts tz-aware UTC or NY).
               Must include at least last 30-60 minutes.

    Returns (latest snapshot):
      {
        'ts': Timestamp,
        'spot': float,
        'dS': float,
        'hedge_shares': float,
        'hedge_dollars': float,
        'baseline_dollars': float,
        'pressure_ratio': float,
        'direction': 'BULLISH'|'BEARISH'|'FLAT'
      }
    """
    bars = bars_1m.copy()
    bars = _ensure_cols(bars, {"timestamp": "ts", "price": "close"})
    for col in ["ts","close","volume"]:
        if col not in bars.columns:
            raise ValueError(f"compute_hedge_pressure: bars missing column '{col}'")

    bars["ts"] = pd.to_datetime(bars["ts"], utc=True)
    bars = bars.sort_values("ts")
    if len(bars) < 3:
        raise ValueError("Need at least 3 minutes of bars to compute pressure.")

    close = bars["close"].values
    dS = float(close[-1] - close[-2])
    spot = float(close[-1])

    # Dollar volume baseline (rolling 30m median of $ volume per minute)
    dollar_vol = bars["close"] * bars["volume"]
    baseline = float(dollar_vol.rolling(30, min_periods=10).median().iloc[-1])
    if not np.isfinite(baseline) or baseline <= 0:
        baseline = float(np.nanmedian(dollar_vol.values[-60:]))

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

    # shares = (-OI) * gamma * dS * multiplier
    dealer_pos = -df["open_interest"].values
    hedge_shares = float(np.nansum(dealer_pos * df["gamma"].values * dS * df["multiplier"].values))
    hedge_dollars = hedge_shares * spot

    direction = "FLAT"
    if hedge_dollars > 0:
        direction = "BULLISH"
    elif hedge_dollars < 0:
        direction = "BEARISH"

    pressure_ratio = abs(hedge_dollars) / (baseline + 1e-9)

    return {
        "ts": pd.to_datetime(bars['ts'].iloc[-1]),
        "spot": spot,
        "dS": dS,
        "hedge_shares": hedge_shares,
        "hedge_dollars": hedge_dollars,
        "baseline_dollars": baseline,
        "pressure_ratio": pressure_ratio,
        "direction": direction
    }

def decide_trade(chain_df: pd.DataFrame,
                 bars_1m: pd.DataFrame,
                 now_ts_utc: Optional[pd.Timestamp] = None,
                 min_pressure: float = 2.5,
                 min_distance_for_momentum_pct: float = 1.0,
                 close_avoid_minutes: int = 15
                 ) -> Decision:
    """
    One-shot decision using only live data.
    """
    if now_ts_utc is None:
        now_ts_utc = pd.to_datetime(bars_1m["ts"].iloc[-1]).tz_convert("UTC")

    now_ny_date = _to_date_ny(now_ts_utc)
    spot = float(pd.to_numeric(bars_1m.sort_values("ts")["close"].iloc[-1]))

    gp = compute_gamma_profile(chain_df, spot, now_ny_date)
    max_gamma_strike = gp["max_gamma_strike"]
    zgs = gp["zero_gamma_strike"]
    distance_to_pin_pct = abs(spot - max_gamma_strike) / spot * 100.0

    hp = compute_hedge_pressure(chain_df, bars_1m)
    pressure_ratio = hp["pressure_ratio"]
    pressure_dir = hp["direction"]
    dS = hp["dS"]

    hrs_to_close = _hours_to_close(now_ts_utc)
    if hrs_to_close*60 <= close_avoid_minutes:
        return Decision(
            action="NO_TRADE",
            confidence="HIGH",
            reason=f"Final {close_avoid_minutes} minutes: avoid EOD chaos",
            details={**gp, **hp, "distance_to_pin_pct": distance_to_pin_pct, "hours_to_close": hrs_to_close}
        )

    regime = None
    if zgs is not None:
        regime = "SHORT_GAMMA" if spot > zgs else "LONG_GAMMA"

    if distance_to_pin_pct <= 0.5 and pressure_ratio < 1.5:
        return Decision(
            action="NO_TRADE",
            confidence="MEDIUM",
            reason=f"Within {distance_to_pin_pct:.2f}% of max-gamma pin and pressure muted ({pressure_ratio:.2f}×)",
            details={**gp, **hp, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime}
        )

    if pressure_ratio >= max(min_pressure, 2.5):
        if pressure_dir == "BULLISH":
            return Decision(
                action="CALL",
                confidence="HIGH" if pressure_ratio >= 3.0 else "MEDIUM",
                reason=f"Strong dealer BUY pressure {pressure_ratio:.2f}× and dS={dS:+.2f} → CALL bias",
                details={**gp, **hp, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime}
            )
        elif pressure_dir == "BEARISH":
            return Decision(
                action="PUT",
                confidence="HIGH" if pressure_ratio >= 3.0 else "MEDIUM",
                reason=f"Strong dealer SELL pressure {pressure_ratio:.2f}× and dS={dS:+.2f} → PUT bias",
                details={**gp, **hp, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime}
            )

    if (1.8 <= pressure_ratio < max(min_pressure, 2.5)) and (distance_to_pin_pct >= min_distance_for_momentum_pct):
        toward_pin_dir = "BULLISH" if (max_gamma_strike - spot) > 0 else "BEARISH"
        if toward_pin_dir == pressure_dir and pressure_dir != "FLAT":
            return Decision(
                action="CALL" if pressure_dir == "BULLISH" else "PUT",
                confidence="MEDIUM",
                reason=f"Moderate pressure {pressure_ratio:.2f}× aligned toward pin @ {max_gamma_strike:.2f}",
                details={**gp, **hp, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime}
            )

    return Decision(
        action="NO_TRADE",
        confidence="LOW",
        reason=f"No aligned edge: pressure {pressure_ratio:.2f}×, dist_to_pin {distance_to_pin_pct:.2f}%",
        details={**gp, **hp, "distance_to_pin_pct": distance_to_pin_pct, "regime": regime}
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
