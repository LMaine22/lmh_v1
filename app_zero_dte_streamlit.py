#!/usr/bin/env python3
"""
Streamlit Dashboard for Live 0-DTE Trade Signals
Auto-refreshes and shows CALL/PUT/NO_TRADE decision with gamma analysis.
"""

import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime
from live_trade_signal import fetch_live_chain, fetch_minute_bars_smart
from zero_dte_live_decider import decide_trade, pick_atm_leg

# Page config
st.set_page_config(
    page_title="0-DTE Live Signal",
    page_icon="🎯",
    layout="wide"
)

# Sidebar settings
st.sidebar.title("⚙️ Settings")
ticker = st.sidebar.text_input("Underlying", value="QQQ")
refresh_sec = st.sidebar.slider("Refresh (seconds)", 5, 60, 15)
lookback_min = st.sidebar.slider("Lookback (minutes)", 30, 120, 60)
min_pressure = st.sidebar.slider("Min Pressure Threshold", 1.5, 5.0, 2.5, 0.5)
close_avoid = st.sidebar.slider("Avoid Final Minutes", 5, 30, 15)

# Main title
st.title(f"🎯 Live 0-DTE Signal: {ticker}")
st.markdown(f"*Updates every {refresh_sec} seconds*")

# Placeholder for main content
placeholder = st.empty()

# Main loop
while True:
    with placeholder.container():
        try:
            # Fetch data
            with st.spinner("📊 Fetching live data..."):
                chain_df = fetch_live_chain(ticker)
                bars_1m, data_source = fetch_minute_bars_smart(ticker, lookback_minutes=lookback_min, chain_df=chain_df)
                spot = float(bars_1m["close"].iloc[-1])
            
            # Make decision
            decision = decide_trade(
                chain_df, 
                bars_1m, 
                min_pressure=min_pressure,
                close_avoid_minutes=close_avoid
            )
            
            # Display results
            st.markdown(f"### Current Price: ${spot:.2f}")
            st.markdown(f"**Last Update:** {datetime.now().strftime('%H:%M:%S')}")
            st.markdown(f"**Data Source:** {data_source}")
            
            # Signal box
            action_color = {
                "CALL": "🟢",
                "PUT": "🔴",
                "NO_TRADE": "⚪"
            }
            
            conf_emoji = {
                "VERY_HIGH": "🔥",
                "HIGH": "⭐",
                "MEDIUM": "➡️",
                "LOW": "💤"
            }
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Signal",
                    f"{action_color.get(decision.action, '')} {decision.action}",
                    ""
                )
            
            with col2:
                st.metric(
                    "Confidence",
                    f"{conf_emoji.get(decision.confidence, '')} {decision.confidence}",
                    ""
                )
            
            with col3:
                pressure = decision.details.get('pressure_ratio', 0)
                st.metric(
                    "Pressure",
                    f"{pressure:.2f}×",
                    f"{decision.details.get('direction', '')}"
                )
            
            st.info(f"**Reason:** {decision.reason}")
            
            # Gamma metrics
            st.markdown("### 📊 Gamma Analysis")
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Max Gamma Strike", f"${decision.details.get('max_gamma_strike', 0):.2f}")
            
            with col2:
                zgs = decision.details.get('zero_gamma_strike')
                st.metric("Zero Gamma Strike", f"${zgs:.2f}" if zgs else "N/A")
            
            with col3:
                st.metric("Distance to Pin", f"{decision.details.get('distance_to_pin_pct', 0):.2f}%")
            
            with col4:
                st.metric("Pin Strength", f"{decision.details.get('pin_strength', 0):.2%}")
            
            with col5:
                hours_left = decision.details.get('hours_to_close', 0)
                st.metric("Hours to Close", f"{hours_left:.2f}h")
            
            # Suggested trade
            if decision.action in ("CALL", "PUT"):
                st.markdown("### 💰 Suggested Trade")
                
                atm = pick_atm_leg(spot, chain_df, decision.action)
                
                if atm:
                    price = atm.get("mid") or atm.get("last") or 0
                    qty = atm.get("suggested_qty", 1)
                    cost = price * 100 * qty
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown(f"""
                        **Contract:** `{atm.get('option_symbol', 'N/A')}`  
                        **Strike:** ${atm.get('strike', 0):.2f}  
                        **Type:** {decision.action}
                        """)
                    
                    with col2:
                        st.markdown(f"""
                        **Bid/Ask:** ${atm.get('bid', 0):.2f} / ${atm.get('ask', 0):.2f}  
                        **Mid:** ${price:.2f}  
                        **Qty:** {qty} contract(s)  
                        **Total Cost:** ${cost:.2f}
                        """)
                    
                    st.warning(f"⚠️ This is a 0-DTE option - expires TODAY at 4:00pm ET. Max loss: ${cost:.2f}")
            
            # Gamma chart
            st.markdown("### 📈 Gamma Exposure by Strike")
            
            if 'per_strike' in decision.details:
                gamma_df = decision.details['per_strike']
                
                # Create chart data
                import matplotlib.pyplot as plt
                
                fig, ax = plt.subplots(figsize=(10, 4))
                ax.bar(gamma_df['strike'], gamma_df['net_gamma_signed'], width=2, alpha=0.7)
                ax.axvline(spot, color='black', linestyle='--', label=f'Spot: ${spot:.2f}')
                ax.axvline(decision.details.get('max_gamma_strike'), color='red', linestyle='--', label='Max Gamma Pin')
                if decision.details.get('zero_gamma_strike'):
                    ax.axvline(decision.details.get('zero_gamma_strike'), color='blue', linestyle='--', label='Zero Gamma')
                ax.set_xlabel('Strike')
                ax.set_ylabel('Net Gamma Exposure (signed)')
                ax.legend()
                ax.grid(alpha=0.3)
                
                st.pyplot(fig)
            
            # Raw data expander
            with st.expander("📋 Raw Data"):
                st.markdown("**Chain Data (0-DTE only)**")
                st.dataframe(chain_df.head(20))
                
                st.markdown("**Minute Bars**")
                st.dataframe(bars_1m.tail(10))
        
        except Exception as e:
            st.error(f"❌ Error: {e}")
            import traceback
            st.code(traceback.format_exc())
    
    # Wait before refresh
    time.sleep(refresh_sec)
    st.rerun()
