import os
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["MASSIVE_API_KEY"].strip()
BASE = "https://api.massive.com/v3"
HDRS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "User-Agent": "massive-options-smoke/1.0"
}

def fetch_chain_page(url, headers, params=None):
    """Fetch a single page from the API."""
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()

def get_option_chain_snapshot(underlying, headers, expiry=None, limit=250):
    """
    Fetch chain with optional expiry filter, pull all pages via next_url.
    """
    base_url = f"{BASE}/snapshot/options/{underlying}"
    params = {"limit": limit}
    if expiry:
        params["expiration_date"] = expiry  # YYYY-MM-DD
    
    data = fetch_chain_page(base_url, headers, params)
    results = data.get("results", []) or []
    
    # follow pagination
    next_url = data.get("next_url")
    while next_url:
        data = fetch_chain_page(next_url, headers)  # next_url already carries query
        results.extend(data.get("results", []) or [])
        next_url = data.get("next_url")
    
    return results

def get_unified_snapshot(ticker: str):
    """
    Unified Snapshot for the underlying (spot, day stats, etc).
    Correct endpoint: /v3/snapshot?ticker.any_of=TICKER
    If your plan doesn't include it, Massive returns 4xx and we surface it clearly.
    """
    url = f"{BASE}/snapshot"
    params = {"ticker.any_of": ticker}
    r = requests.get(url, headers=HDRS, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()

def get_option_contract_snapshot(underlying: str, option_contract: str):
    """
    Option Contract Snapshot — single OSI contract like O:QQQ20251115C00480000
    """
    url = f"{BASE}/snapshot/options/{underlying}/{option_contract}"
    r = requests.get(url, headers=HDRS, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()

def pick_nearest_expiry(chain_results):
    """
    Collect unique expiries and choose earliest >= today (UTC).
    """
    # collect unique expiries
    exps = sorted({row["details"]["expiration_date"] for row in chain_results if "details" in row})
    if not exps:
        raise RuntimeError("No expirations found in chain results.")
    # choose earliest >= today (UTC); otherwise the earliest available
    today = datetime.now(timezone.utc).date().isoformat()
    future = [e for e in exps if e >= today]
    return (future[0] if future else exps[0])

def extract_spot_from_chain(chain_results):
    """
    Find spot from underlying_asset.price in chain results.
    Falls back to median strike if not available.
    """
    # find any row that exposes underlying_asset.price
    for row in chain_results:
        ua = row.get("underlying_asset") or {}
        if "price" in ua and isinstance(ua["price"], (int, float)):
            return float(ua["price"])
    # fallback: use median strike as a crude proxy (last resort for smoke test only)
    strikes = sorted([row["details"]["strike_price"] for row in chain_results if "details" in row])
    if strikes:
        return strikes[len(strikes)//2]
    raise RuntimeError("Could not extract spot from chain.")

def pick_atm_contracts(filtered_for_exp, spot):
    """
    Choose one CALL and one PUT closest to ATM for that expiry.
    """
    calls = [r for r in filtered_for_exp if r.get("details", {}).get("contract_type") == "call"]
    puts  = [r for r in filtered_for_exp if r.get("details", {}).get("contract_type") == "put"]
    
    if not calls or not puts:
        raise RuntimeError(
            f"Selected expiry did not return both calls and puts. "
            f"Found {len(calls)} calls and {len(puts)} puts. "
            f"Try increasing limit or check pagination."
        )
    
    def key_abs_strike(r): 
        return abs(r["details"]["strike_price"] - spot)
    
    call_row = min(calls, key=key_abs_strike)
    put_row  = min(puts,  key=key_abs_strike)
    return call_row, put_row

def summarize_contract_snapshot(snap):
    """Extract key fields from contract snapshot."""
    res = snap.get("results", {})
    # NBBO
    last = res.get("last_quote") or {}
    last_t = res.get("last_trade") or {}
    greeks = res.get("greeks") or {}
    oi     = res.get("open_interest") or res.get("oi") or {}
    day    = res.get("day") or {}
    
    return {
        "contract": res.get("ticker") or res.get("option") or "N/A",
        "break_even": res.get("break_even_price"),
        "iv": greeks.get("implied_volatility"),
        "delta": greeks.get("delta"),
        "gamma": greeks.get("gamma"),
        "vega":  greeks.get("vega"),
        "theta": greeks.get("theta"),
        "open_interest": oi if isinstance(oi, (int, float)) else oi.get("value") if isinstance(oi, dict) else oi,
        "bid": last.get("bid"),
        "ask": last.get("ask"),
        "bid_size": last.get("bid_size"),
        "ask_size": last.get("ask_size"),
        "last_price": last_t.get("price"),
        "volume_day": day.get("volume"),
        "updated_ns": res.get("last_updated")
    }

def main():
    underlying = "QQQ"
    
    print(f"=== Unified snapshot for {underlying} (if your plan includes it) ===")
    try:
        uni = get_unified_snapshot(underlying)
        results_list = uni.get("results", [])
        if results_list:
            rs = results_list[0]  # First result for the ticker
            spot = (rs.get("session") or {}).get("price") or (rs.get("last_trade") or {}).get("price")
            print(json.dumps({"spot": spot, "ticker": rs.get("ticker"), "session": rs.get("session")}, indent=2))
            print(f"[OK] Unified snapshot spot: {spot}")
        else:
            raise RuntimeError("No results in unified snapshot response")
    except Exception as e:
        # If unified isn't in the plan, we still proceed with the chain snapshot.
        print(f"[Info] Unified snapshot unavailable or not in plan: {e}")
        spot = None
    
    print(f"\n=== Option Chain snapshot for {underlying} ===")
    
    # 1) pull a broad page to learn expiries
    print("Fetching initial chain (limit=250, with pagination)...")
    all_rows = get_option_chain_snapshot(underlying, HDRS, expiry=None, limit=250)
    print(f"Retrieved {len(all_rows)} total contracts across all expiries")
    
    if not all_rows:
        raise RuntimeError("Empty chain results. Verify plan & ticker.")
    
    chosen_exp = pick_nearest_expiry(all_rows)
    print(f"Selected nearest expiry: {chosen_exp}")
    
    # 2) pull the *full* selected expiry (paginated)
    print(f"Fetching full chain for expiry {chosen_exp}...")
    rows_this_exp = get_option_chain_snapshot(underlying, HDRS, expiry=chosen_exp, limit=250)
    
    # Count calls and puts for diagnostics
    num_calls = sum(1 for r in rows_this_exp if r.get("details", {}).get("contract_type") == "call")
    num_puts = sum(1 for r in rows_this_exp if r.get("details", {}).get("contract_type") == "put")
    print(f"Contracts @ {chosen_exp}: total={len(rows_this_exp)}, calls={num_calls}, puts={num_puts}")
    
    # 3) spot from chain (DELAYED is fine for a smoke)
    if spot is None:
        spot = extract_spot_from_chain(rows_this_exp)
        print(f"Extracted spot from chain: {spot:.2f}")
    
    # 4) pick ATM
    call_row, put_row = pick_atm_contracts(rows_this_exp, spot)
    
    call_tkr = call_row["details"]["ticker"]
    put_tkr = put_row["details"]["ticker"]
    
    print(f"\n[OK] Expiry: {chosen_exp} | spot≈{spot:.2f}")
    print(f"     ATM call: {call_tkr} @ strike {call_row['details']['strike_price']}")
    print(f"     ATM put:  {put_tkr} @ strike {put_row['details']['strike_price']}")
    
    print(f"\n=== Contract snapshot: {call_tkr} ===")
    call_snap = get_option_contract_snapshot(underlying, call_tkr)
    print(json.dumps(summarize_contract_snapshot(call_snap), indent=2))
    
    print(f"\n=== Contract snapshot: {put_tkr} ===")
    put_snap = get_option_contract_snapshot(underlying, put_tkr)
    print(json.dumps(summarize_contract_snapshot(put_snap), indent=2))
    
    print("\n✅ OK: REST pipe verified end-to-end.\n")

if __name__ == "__main__":
    main()
