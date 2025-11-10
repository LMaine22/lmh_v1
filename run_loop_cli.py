#!/usr/bin/env python3
"""
CLI Loop for Live 0-DTE Trade Signals
Prints decision every N seconds.
"""

import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv("massive_options_smoke/.env")

from live_trade_signal import fetch_live_chain, fetch_minute_bars_simple
from zero_dte_live_decider import decide_trade, pick_atm_leg

REFRESH_SEC = int(os.getenv("REFRESH_SEC", "15"))
TICKER = os.getenv("TICKER", "QQQ")

def print_signal(ticker: str):
    """Fetch data and print signal."""
    try:
        # Fetch live data
        chain_df = fetch_live_chain(ticker)
        bars_1m = fetch_minute_bars_simple(chain_df, lookback_minutes=60)
        spot = float(bars_1m["close"].iloc[-1])
        
        # Make decision
        decision = decide_trade(chain_df, bars_1m)
        
        # Format output
        timestamp = datetime.now().strftime("%H:%M:%S")
        action_emoji = {"CALL": "🟢", "PUT": "🔴", "NO_TRADE": "⚪"}
        conf_emoji = {"VERY_HIGH": "🔥", "HIGH": "⭐", "MEDIUM": "➡️", "LOW": "💤"}
        
        emoji = action_emoji.get(decision.action, "")
        conf = conf_emoji.get(decision.confidence, "")
        pressure = decision.details.get('pressure_ratio', 0)
        pin = decision.details.get('max_gamma_strike', 0)
        dist = decision.details.get('distance_to_pin_pct', 0)
        
        print(f"{timestamp} | {emoji} {decision.action:8s} {conf} {decision.confidence:10s} | "
              f"Spot: ${spot:7.2f} | Pin: ${pin:7.2f} ({dist:+5.2f}%) | "
              f"Pressure: {pressure:5.2f}× {decision.details.get('direction', 'FLAT'):8s}")
        
        # If signal, show contract
        if decision.action in ("CALL", "PUT"):
            atm = pick_atm_leg(spot, chain_df, decision.action)
            if atm:
                contract = atm.get('option_symbol', 'N/A')[:25]
                price = atm.get('mid', 0)
                qty = atm.get('suggested_qty', 1)
                cost = price * 100 * qty
                print(f"         → {contract} @ ${price:.2f} × {qty} = ${cost:.2f} risk")
        
    except Exception as e:
        print(f"{datetime.now().strftime('%H:%M:%S')} | ❌ ERROR: {e}")

def main():
    print(f"\n{'='*100}")
    print(f"🎯 LIVE 0-DTE SIGNAL LOOP - {TICKER}")
    print(f"{'='*100}")
    print(f"Refresh: Every {REFRESH_SEC} seconds")
    print(f"Press Ctrl+C to stop")
    print(f"{'='*100}\n")
    
    try:
        while True:
            print_signal(TICKER)
            time.sleep(REFRESH_SEC)
    except KeyboardInterrupt:
        print(f"\n\n{'='*100}")
        print("Stopped by user")
        print(f"{'='*100}\n")

if __name__ == "__main__":
    main()
