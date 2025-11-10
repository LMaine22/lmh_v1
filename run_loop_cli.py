#!/usr/bin/env python3
"""
CLI Loop for Live 0-DTE Trade Signals with Position Tracking
Monitors entries and exits with trailing stops.
"""

import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

load_dotenv("massive_options_smoke/.env")

from live_trade_signal import fetch_live_chain, fetch_minute_bars_smart
from zero_dte_live_decider import decide_trade, pick_atm_leg

REFRESH_SEC = int(os.getenv("REFRESH_SEC", "15"))
TICKER = os.getenv("TICKER", "QQQ")

# Trade tracking
class TradeTracker:
    """Track position and implement intelligent exit logic"""
    def __init__(self):
        self.in_position = False
        self.entry_time = None
        self.entry_action = None
        self.entry_price = None
        self.entry_pressure = None
        self.entry_direction = None
        self.entry_spot = None
        self.entry_confidence = None
        self.contract_symbol = None
        self.contract_strike = None
        self.quantity = 1
        self.risk = None
        self.peak_pnl_pct = 0
        self.peak_price = 0
    
    def enter(self, decision, spot, atm_data):
        """Enter a new position"""
        self.in_position = True
        self.entry_time = datetime.now(ZoneInfo("America/New_York"))
        self.entry_action = decision.action
        self.entry_price = atm_data.get('mid', 0)
        self.entry_pressure = decision.details.get('adjusted_pressure', 0)
        self.entry_direction = decision.details.get('direction')
        self.entry_spot = spot
        self.entry_confidence = decision.confidence
        self.contract_symbol = atm_data.get('option_symbol', 'N/A')
        self.contract_strike = atm_data.get('strike', 0)
        self.quantity = atm_data.get('suggested_qty', 1)
        self.risk = self.entry_price * 100 * self.quantity
        self.peak_pnl_pct = 0
        self.peak_price = self.entry_price
        
        print(f"\n   🎬 ENTERED {self.entry_action}")
        print(f"      Contract: {self.contract_symbol}")
        print(f"      Strike: ${self.contract_strike:.2f}")
        print(f"      Entry: ${self.entry_price:.2f}")
        print(f"      Spot: ${spot:.2f}")
        print(f"      Pressure: {self.entry_pressure:.2f}×")
        print(f"      Confidence: {self.entry_confidence}")
        print(f"      Risk: ${self.risk:.2f}")
        print(f"      Time: {self.entry_time.strftime('%H:%M:%S %Z')}\n")
    
    def get_current_price(self, chain_df):
        """Get current mid price for our contract"""
        if not self.contract_symbol:
            return None
        contract_row = chain_df[chain_df['option_symbol'] == self.contract_symbol]
        if contract_row.empty:
            return None
        mid = contract_row.iloc[0].get('mid')
        if mid is None or mid <= 0:
            return None
        return float(mid)
    
    def check_exit(self, decision, spot, chain_df):
        """Greeks-based exit logic using delta, gamma, theta, vega, and context"""
        if not self.in_position:
            return False, None, 0, 0
        
        current_price = self.get_current_price(chain_df)
        if current_price is None:
            return False, None, 0, 0
        
        # Get current Greeks for OUR contract
        our_contract = chain_df[chain_df['option_symbol'] == self.contract_symbol]
        if our_contract.empty:
            return False, None, 0, 0
        
        # Handle None values in Greeks
        current_delta = float(our_contract.iloc[0].get('delta') or 0)
        current_gamma = float(our_contract.iloc[0].get('gamma') or 0)
        current_theta = float(our_contract.iloc[0].get('theta') or 0)
        current_vega = float(our_contract.iloc[0].get('vega') or 0)
        current_iv = float(our_contract.iloc[0].get('iv') or 0)
        
        # Market context
        current_pressure = decision.details.get('adjusted_pressure', 0)
        current_direction = decision.details.get('direction')
        hrs_to_close = decision.details.get('hours_to_close', 0)
        session_ctx = decision.details
        vwap = session_ctx.get('vwap', 0)
        dist_from_vwap = abs(spot - vwap) / vwap * 100 if vwap else 0
        max_gamma_strike = decision.details.get('max_gamma_strike', 0)
        dist_to_pin = decision.details.get('distance_to_pin_pct', 0)
        
        # Calculate P&L
        pnl_pct = ((current_price - self.entry_price) / self.entry_price) * 100
        pnl_dollars = (current_price - self.entry_price) * 100 * self.quantity
        time_in_trade = (datetime.now(ZoneInfo("America/New_York")) - self.entry_time).total_seconds() / 60
        
        # Track peak for trailing stop
        if current_price > self.peak_price:
            self.peak_price = current_price
            self.peak_pnl_pct = pnl_pct
        
        # ==== SMART GREEKS-BASED EXIT LOGIC ====
        
        # EXIT #1: SUSTAINED Pressure Reversal (not just 1 candle!)
        # Only trigger if pressure has been OPPOSITE for multiple checks
        if not hasattr(self, 'opposite_pressure_count'):
            self.opposite_pressure_count = 0
        
        if current_direction != self.entry_direction and current_direction != "FLAT":
            self.opposite_pressure_count += 1
            # Only exit if pressure has been opposite for 2+ consecutive checks (30+ seconds)
            if self.opposite_pressure_count >= 2:
                return True, f"🔄 SUSTAINED PRESSURE REVERSAL: {self.entry_direction} → {current_direction} for {self.opposite_pressure_count * 15}+ seconds", pnl_pct, pnl_dollars
            # Otherwise, just a temporary chop - HOLD
        else:
            self.opposite_pressure_count = 0  # Reset
        
        # EXIT #2: VWAP Pullback Detection (DON'T exit on healthy pullbacks!)
        if pnl_pct > 50:  # Only if profitable
            # If we're pulling back TO vwap (not THROUGH it), this is healthy
            if self.entry_action == "CALL":
                if spot > vwap and dist_from_vwap < 0.3:
                    # Pulling back to VWAP but still above = HEALTHY, don't exit
                    pass  # Continue checking other conditions
                elif spot < vwap and current_pressure < 2.0:
                    # Broke BELOW vwap with weak pressure = DANGER
                    return True, f"📉 BROKE VWAP SUPPORT: Spot ${spot:.2f} < VWAP ${vwap:.2f} with weak pressure {current_pressure:.2f}×", pnl_pct, pnl_dollars
            elif self.entry_action == "PUT":
                if spot < vwap and dist_from_vwap < 0.3:
                    # Pulling back to VWAP from below = HEALTHY
                    pass
                elif spot > vwap and current_pressure < 2.0:
                    # Broke ABOVE vwap with weak pressure = DANGER
                    return True, f"📈 BROKE VWAP RESISTANCE: Spot ${spot:.2f} > VWAP ${vwap:.2f} with weak pressure {current_pressure:.2f}×", pnl_pct, pnl_dollars
        
        # EXIT #3: Delta Momentum Check (is option getting MORE or LESS ITM?)
        # For calls: delta increasing = getting more ITM = GOOD
        # For calls: delta decreasing significantly = losing momentum = BAD
        if self.entry_action == "CALL":
            # Delta should be 0.3-0.8 for healthy call
            # If delta drops below 0.25, option losing value fast
            if current_delta < 0.25 and pnl_pct < 50:
                return True, f"📉 DELTA COLLAPSE: Delta {current_delta:.2f} - option going OTM", pnl_pct, pnl_dollars
        elif self.entry_action == "PUT":
            # For puts, look at absolute delta (negative)
            if abs(current_delta) < 0.25 and pnl_pct < 50:
                return True, f"📉 DELTA COLLAPSE: Delta {abs(current_delta):.2f} - option going OTM", pnl_pct, pnl_dollars
        
        # EXIT #4: Theta Burn at Pin (stuck at gamma wall losing time value)
        if dist_to_pin < 0.3 and current_pressure < 1.5:
            # Stuck near pin with low pressure = theta burning with no directional edge
            # Theta from Massive is ALREADY in dollars per day per contract
            theta_per_day = abs(current_theta)
            theta_per_hour = theta_per_day / 24
            # Only alert if losing >5% per hour to theta (very high decay)
            if theta_per_hour > current_price * 0.05:
                return True, f"⏰ THETA BURN AT PIN: Stuck at ${max_gamma_strike:.2f} pin, losing ${theta_per_hour:.3f}/hr (${theta_per_day:.2f}/day)", pnl_pct, pnl_dollars
        
        # EXIT #5: CRITICAL Pressure Reversal (old simple version as backup)
        # Keep this but only if sustained (handled above now)
        
        # TIERED EXIT LOGIC BASED ON P&L
        
        if pnl_pct >= 100:  # Up 100%+
            # Use trailing stop
            if current_pressure < 3.0:
                return True, f"💎 PRESSURE FADING: Up +{pnl_pct:.0f}% but pressure dropped to {current_pressure:.2f}×", pnl_pct, pnl_dollars
            
            # Trailing stop: Exit if dropped 40% from peak (looser to avoid 1-candle noise)
            # ALSO require pressure to have weakened (not just price)
            drawdown_from_peak = self.peak_pnl_pct - pnl_pct
            if drawdown_from_peak > 40 and current_pressure < 3.5:
                return True, f"📉 TRAILING STOP: Dropped {drawdown_from_peak:.0f}% from peak +{self.peak_pnl_pct:.0f}% AND pressure weakened", pnl_pct, pnl_dollars
            
            # Time protection
            if hrs_to_close < 0.5:
                return True, f"⏰ PROTECT GAINS: +{pnl_pct:.0f}% with {hrs_to_close*60:.0f}min left", pnl_pct, pnl_dollars
        
        elif pnl_pct >= 50:  # Up 50-100%
            # Tighter trailing stop
            if current_pressure < 2.5:
                return True, f"📊 PRESSURE WEAKENING: Up +{pnl_pct:.0f}% but pressure {current_pressure:.2f}×", pnl_pct, pnl_dollars
            
            drawdown_from_peak = self.peak_pnl_pct - pnl_pct
            if drawdown_from_peak > 35 and current_pressure < 3.0:
                return True, f"📉 TRAILING STOP: Dropped {drawdown_from_peak:.0f}% from peak +{self.peak_pnl_pct:.0f}% AND pressure weakened", pnl_pct, pnl_dollars
            
            if hrs_to_close < 0.75:
                return True, f"⏰ CLOSING SOON: Lock in +{pnl_pct:.0f}%", pnl_pct, pnl_dollars
        
        elif pnl_pct >= 20:  # Up 20-50%
            # Standard exits
            if current_pressure < self.entry_pressure * 0.5:
                return True, f"📉 PRESSURE EXHAUSTION: {self.entry_pressure:.2f}× → {current_pressure:.2f}×", pnl_pct, pnl_dollars
            
            if hrs_to_close < 1.0:
                return True, f"⏰ CLOSING TIME: Take +{pnl_pct:.0f}%", pnl_pct, pnl_dollars
        
        elif pnl_pct < -20:  # Down 20%+
            # Stop loss
            if pnl_pct <= -30:
                return True, f"🛑 STOP LOSS: {pnl_pct:.1f}%", pnl_pct, pnl_dollars
            
            if current_pressure < 1.5:
                return True, f"💀 DEAD TRADE: {pnl_pct:.1f}% and pressure {current_pressure:.2f}×", pnl_pct, pnl_dollars
        
        # Max hold time: 90 minutes for 0DTE
        if time_in_trade > 90:
            return True, f"⏳ MAX HOLD: {time_in_trade:.0f}min in trade", pnl_pct, pnl_dollars
        
        # Still holding
        return False, None, pnl_pct, pnl_dollars
    
    def exit(self, reason, pnl_pct, pnl_dollars):
        """Exit the position"""
        exit_time = datetime.now(ZoneInfo("America/New_York"))
        duration = (exit_time - self.entry_time).total_seconds() / 60
        
        print(f"\n   🏁 EXITED {self.entry_action}")
        print(f"      Reason: {reason}")
        print(f"      P&L: {pnl_pct:+.1f}% (${pnl_dollars:+.2f})")
        print(f"      Entry: ${self.entry_price:.2f}")
        print(f"      Exit: ${self.entry_price + (pnl_dollars / (100 * self.quantity)):.2f}")
        print(f"      Peak: +{self.peak_pnl_pct:.1f}% (${self.peak_price:.2f})")
        print(f"      Duration: {duration:.1f} minutes")
        print(f"      Time: {exit_time.strftime('%H:%M:%S %Z')}\n")
        
        self.in_position = False

tracker = TradeTracker()

# Track previous signal to prevent flip-flopping
previous_action = None
previous_pressure = 0.0

def print_signal(ticker: str):
    """Fetch data and print signal with position tracking."""
    global previous_action, previous_pressure
    
    try:
        # Fetch live data
        chain_df = fetch_live_chain(ticker)
        bars_1m, data_source = fetch_minute_bars_smart(ticker, lookback_minutes=60, chain_df=chain_df)
        spot = float(bars_1m["close"].iloc[-1])
        
        # Make decision
        decision = decide_trade(chain_df, bars_1m)
        
        # Format timestamp
        et_time = datetime.now(ZoneInfo("America/New_York"))
        timestamp = et_time.strftime("%H:%M:%S ET")
        
        # Get pressure and key metrics
        adjusted_pressure = decision.details.get('adjusted_pressure', 0)
        raw_pressure = decision.details.get('pressure_ratio', 0)
        pin = decision.details.get('max_gamma_strike', 0)
        dist = decision.details.get('distance_to_pin_pct', 0)
        direction = decision.details.get('direction', 'FLAT')
        
        # CHECK IF WE'RE IN A POSITION
        if tracker.in_position:
            # Check for exit (but DON'T auto-close, just ALERT)
            should_exit, exit_reason, pnl_pct, pnl_dollars = tracker.check_exit(decision, spot, chain_df)
            
            if should_exit:
                # ALERT only - don't auto-close
                print(f"{timestamp} | 🚨🚨🚨 EXIT ALERT 🚨🚨🚨")
                print(f"         Reason: {exit_reason}")
                print(f"         P&L: {pnl_pct:+.1f}% (${pnl_dollars:+.2f})")
                print(f"         ⚠️  CONSIDER CLOSING YOUR POSITION! ⚠️")
                # DON'T call tracker.exit() - let user close manually
            else:
                # Show position status with Greeks
                current_price = tracker.get_current_price(chain_df)
                if current_price:
                    # Get our contract's Greeks
                    our_contract = chain_df[chain_df['option_symbol'] == tracker.contract_symbol]
                    if not our_contract.empty:
                        delta = float(our_contract.iloc[0].get('delta') or 0)
                        theta = float(our_contract.iloc[0].get('theta') or 0)
                        # Theta from Massive is ALREADY in dollars per day per contract
                        theta_per_day = abs(theta)
                        theta_per_hour = theta_per_day / 24
                    else:
                        delta = 0
                        theta_per_day = 0
                        theta_per_hour = 0
                    
                    # Check if this is a healthy pullback
                    vwap = decision.details.get('vwap', 0)
                    dist_from_vwap_pct = decision.details.get('dist_from_vwap_pct', 0)
                    
                    hold_reason = ""
                    if tracker.entry_action == "CALL" and spot > vwap and abs(dist_from_vwap_pct) < 0.5:
                        hold_reason = " ✅ VWAP SUPPORT - healthy pullback"
                    elif tracker.entry_action == "PUT" and spot < vwap and abs(dist_from_vwap_pct) < 0.5:
                        hold_reason = " ✅ VWAP RESISTANCE - healthy pullback"
                    
                    # Check opposite pressure count
                    opposite_count = getattr(tracker, 'opposite_pressure_count', 0)
                    if opposite_count > 0:
                        hold_reason = f" ⚠️ Pressure opposite for {opposite_count * 15}s (watching...)"
                    
                    print(f"{timestamp} | 💼 HOLDING {tracker.entry_action:8s} | "
                          f"P&L: {pnl_pct:+6.1f}% (${pnl_dollars:+7.2f}) | "
                          f"Peak: +{tracker.peak_pnl_pct:.0f}% | "
                          f"Spot: ${spot:7.2f} | Pressure: {adjusted_pressure:5.2f}× {direction:8s}{hold_reason}")
                    print(f"         Price: ${current_price:.2f} (Entry: ${tracker.entry_price:.2f}) | "
                          f"Delta: {delta:.3f} | Theta/hr: -${theta_per_hour:.2f} | "
                          f"Time: {(datetime.now(ZoneInfo('America/New_York')) - tracker.entry_time).total_seconds() / 60:.0f}m")
        else:
            # NOT IN POSITION - Show signal with hysteresis
            action_emoji = {"CALL": "🟢", "PUT": "🔴", "NO_TRADE": "⚪"}
            conf_emoji = {"VERY_HIGH": "🔥", "HIGH": "⭐", "MEDIUM": "➡️", "LOW": "💤"}
            
            emoji = action_emoji.get(decision.action, "")
            conf = conf_emoji.get(decision.confidence, "")
            
            # Apply hysteresis (prevent flip-flop)
            if previous_action in ("CALL", "PUT") and decision.action == "NO_TRADE":
                if raw_pressure > 2.0:
                    print(f"{timestamp} | ⏸️  HOLDING {previous_action} signal (pressure {raw_pressure:.2f}× still elevated)")
                    return
            
            # Update state
            previous_action = decision.action
            previous_pressure = raw_pressure
            
            print(f"{timestamp} | {emoji} {decision.action:8s} {conf} {decision.confidence:10s} | "
                  f"Spot: ${spot:7.2f} | Pin: ${pin:7.2f} ({dist:+5.2f}%) | "
                  f"Pressure: {adjusted_pressure:5.2f}× {direction:8s}")
            
            # Show signals but DON'T auto-enter (monitor only mode)
            if decision.action in ("CALL", "PUT"):
                atm = pick_atm_leg(spot, chain_df, decision.action)
                if atm:
                    contract = atm.get('option_symbol', 'N/A')[:25]
                    price = atm.get('mid', 0)
                    qty = atm.get('suggested_qty', 1)
                    cost = price * 100 * qty
                    print(f"         → {contract} @ ${price:.2f} × {qty} = ${cost:.2f} risk")
                    # DO NOT AUTO-ENTER - user manages their own trades
        
    except Exception as e:
        print(f"{timestamp} | ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

def manual_position_entry():
    """Allow manual entry of existing position"""
    print("\n📋 Do you have an existing position? (y/n): ", end='')
    response = input().strip().lower()
    
    if response == 'y':
        print("\n🔧 Enter your position details:")
        
        try:
            action = input("   Type (CALL/PUT): ").strip().upper()
            if action not in ["CALL", "PUT"]:
                print("   Invalid type, skipping position entry")
                return
            
            strike = float(input("   Strike price: $").strip())
            entry_price = float(input("   Entry price per contract: $").strip())
            quantity = int(input("   Quantity (contracts): ").strip() or "1")
            
            # Get current data to set pressure/direction
            print("\n   📊 Fetching current data to find your contract...")
            chain_df = fetch_live_chain(TICKER)
            bars_1m, _ = fetch_minute_bars_smart(TICKER, lookback_minutes=60, chain_df=chain_df)
            decision = decide_trade(chain_df, bars_1m)
            spot = float(bars_1m["close"].iloc[-1])
            
            # Find the contract symbol from strike and type
            matching = chain_df[
                (chain_df['strike'] == strike) & 
                (chain_df['cp_flag'].str.upper().str[0] == action[0])
            ]
            
            if matching.empty:
                print(f"   ❌ Could not find {action} contract at ${strike:.2f} strike")
                return
            
            contract = matching.iloc[0]['option_symbol']
            print(f"   ✓ Found contract: {contract}")
            
            # Initialize tracker manually
            tracker.in_position = True
            tracker.entry_time = datetime.now(ZoneInfo("America/New_York"))
            tracker.entry_action = action
            tracker.entry_price = entry_price
            tracker.entry_pressure = decision.details.get('adjusted_pressure', 0)
            # For manual entry, assume entry was on correct directional pressure
            tracker.entry_direction = "BULLISH" if action == "CALL" else "BEARISH"
            tracker.entry_spot = spot
            tracker.entry_confidence = "MANUAL"
            tracker.contract_symbol = contract
            tracker.contract_strike = strike
            tracker.quantity = quantity
            tracker.risk = entry_price * 100 * quantity
            tracker.peak_pnl_pct = 0
            tracker.peak_price = entry_price
            
            print(f"\n   ✅ Position loaded:")
            print(f"      {action} {contract}")
            print(f"      Strike: ${strike:.2f}")
            print(f"      Entry: ${entry_price:.2f}")
            print(f"      Quantity: {quantity}")
            print(f"      Risk: ${tracker.risk:.2f}")
            print(f"      Current spot: ${spot:.2f}")
            print(f"      Current pressure: {tracker.entry_pressure:.2f}×\n")
            
        except Exception as e:
            print(f"   ❌ Error loading position: {e}")
            print("   Starting without position tracking\n")

def main():
    print(f"\n{'='*100}")
    print(f"🎯 LIVE 0-DTE SIGNAL LOOP WITH POSITION TRACKING - {TICKER}")
    print(f"{'='*100}")
    print(f"Refresh: Every {REFRESH_SEC} seconds")
    print(f"📊 MONITOR MODE: Tracks your position, shows alerts")
    print(f"⚠️  Will alert on: Pressure reversal, significant drawdown, time limits")
    print(f"✋ You manually close - this just monitors!")
    print(f"Press Ctrl+C to stop")
    print(f"{'='*100}\n")
    
    # Check for existing position
    manual_position_entry()
    
    try:
        while True:
            print_signal(TICKER)
            time.sleep(REFRESH_SEC)
    except KeyboardInterrupt:
        print(f"\n\n{'='*100}")
        if tracker.in_position:
            print(f"⚠️  WARNING: Still holding {tracker.entry_action} position!")
            print(f"   Contract: {tracker.contract_symbol}")
            print(f"   Entry: ${tracker.entry_price:.2f} @ {tracker.entry_time.strftime('%H:%M:%S')}")
            print(f"   Manually close position if needed!")
        print("Stopped by user")
        print(f"{'='*100}\n")

if __name__ == "__main__":
    main()
