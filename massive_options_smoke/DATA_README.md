# Massive Options Data - Comprehensive Guide

**Last Updated:** 2025-11-09  
**Data Provider:** Massive.io (formerly Polygon.io)  
**Coverage:** Historical options data with Greeks, IV, and Open Interest

---

## 📊 What Data We Have

### Historical Options Dataset
**Location:** `data/YYYY-MM-DD/{TICKER}_options.csv`

**Date Range:** October 10 - November 9, 2025 (21 trading days)

**Tickers Covered:**
- QQQ - Invesco QQQ Trust (Nasdaq 100 ETF)
- NVDA - NVIDIA Corporation
- TSLA - Tesla Inc
- COIN - Coinbase Global Inc
- AMZN - Amazon.com Inc
- AMD - Advanced Micro Devices Inc
- AAPL - Apple Inc
- MSFT - Microsoft Corporation
- AVGO - Broadcom Inc
- META - Meta Platforms Inc

**Data Filters Applied:**
- ✅ **DTE (Days to Expiration):** ≤30 days only
- ✅ **Strike Range:** ±50% from underlying price (ATM-focused)
- ✅ **Status:** Active contracts with valid Greeks

---

## 📁 File Structure

```
data/
├── 2025-10-13/
│   ├── QQQ_options.csv       # ~594 contracts
│   ├── NVDA_options.csv      # ~0 contracts (partial coverage)
│   └── ...
├── 2025-10-15/
│   ├── QQQ_options.csv       # ~1,060 contracts
│   ├── NVDA_options.csv      # ~122 contracts
│   ├── TSLA_options.csv      # ~250 contracts
│   ├── COIN_options.csv      # ~196 contracts
│   ├── AMZN_options.csv      # ~132 contracts
│   ├── AMD_options.csv       # ~160 contracts
│   ├── AAPL_options.csv      # ~150 contracts
│   ├── MSFT_options.csv      # ~168 contracts
│   ├── AVGO_options.csv      # ~186 contracts
│   └── META_options.csv      # ~XXX contracts
├── 2025-10-16/
│   └── ...
...
└── 2025-11-09/
    └── ...
```

**Typical Contract Counts per Day:**
- QQQ: 500-1,200 contracts (highest liquidity)
- Large Cap Stocks: 100-300 contracts each
- Total per day: ~2,500-4,000 contracts across all tickers

---

## 📋 CSV Column Reference

Each `{TICKER}_options.csv` file contains the following columns:

### Contract Identification
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `ticker` | string | OSI format option ticker | `O:QQQ251115C00500000` |
| `underlying` | string | Underlying stock/ETF symbol | `QQQ` |
| `expiration` | date | Contract expiration date | `2025-11-15` |
| `dte` | int | Days to expiration | `8` |
| `strike` | float | Strike price | `500.0` |
| `type` | string | Option type | `C` (Call) or `P` (Put) |

### Greeks (CRITICAL FOR GAMMA ANALYSIS)
| Column | Type | Description | Range | Importance |
|--------|------|-------------|-------|------------|
| `delta` | float | Price sensitivity to underlying | -1.0 to 1.0 | ⭐⭐⭐⭐⭐ |
| `gamma` | float | Delta sensitivity (curvature) | 0.0 to ~0.5 | ⭐⭐⭐⭐⭐ |
| `theta` | float | Time decay per day | Usually negative | ⭐⭐⭐ |
| `vega` | float | Volatility sensitivity | 0.0+ | ⭐⭐⭐ |
| `iv` | float | Implied volatility | 0.0 to 5.0+ | ⭐⭐⭐⭐ |

### Open Interest (CRITICAL FOR GAMMA EXPOSURE)
| Column | Type | Description | Importance |
|--------|------|-------------|------------|
| `open_interest` | int | Total outstanding contracts | ⭐⭐⭐⭐⭐ |

**Why OI Matters:** Gamma Exposure = Gamma × Open Interest × Contract Multiplier (100)

### Market Data (Prices & Liquidity)
| Column | Type | Description |
|--------|------|-------------|
| `bid` | float | Best bid price |
| `ask` | float | Best ask price |
| `mid` | float | Mid-market price (bid+ask)/2 |
| `bid_size` | int | Contracts at bid |
| `ask_size` | int | Contracts at ask |
| `last_price` | float | Last traded price |
| `last_size` | int | Last trade size |

### Volume & Price History
| Column | Type | Description |
|--------|------|-------------|
| `volume` | int | Contracts traded today |
| `open` | float | Opening price |
| `high` | float | Intraday high |
| `low` | float | Intraday low |
| `close` | float | Closing price |
| `vwap` | float | Volume-weighted average price |

### Other
| Column | Type | Description |
|--------|------|-------------|
| `break_even` | float | Break-even price at expiration |
| `underlying_price` | float | Current underlying stock/ETF price |

---

## 🔧 Extending the Dataset (What Else Is Available)

### 1. **Historical Intraday Greeks**
**Current Setup:** End-of-day Greeks snapshots (1 per trading day)

**To Add Intraday Greeks Going Forward:**
- Massive provides real-time Greeks via API - can poll every minute
- **Setup:** Run `fetch_greeks_snapshot.py` on schedule (cron every 15-60 min)
- **Example cron:**
  ```bash
  */15 9-16 * * 1-5 cd /path/to/project && python fetch_greeks_snapshot.py >> greeks_log.txt
  ```
- **Result:** Build minute/hour-resolution Greeks history for future analysis

**Note:** Historical minute-by-minute Greeks from past dates require specialized providers (ORATS ~$500/mo, IVolatility)

### 2. **Wider Strike Range**
**Current Filter:** Strikes within ±50% of underlying price

**To Include More Strikes:**
- Edit `STRIKE_RANGE_PCT` in `build_historical_dataset.py`
- Example: `STRIKE_RANGE_PCT = 1.0` includes ±100% (full chain)
- **Trade-off:** Larger files, more illiquid contracts

### 3. **Longer-Dated Expirations**
**Current Filter:** DTE ≤ 30 days

**To Include LEAPS/Longer Expirations:**
- Edit `MAX_DTE` in `build_historical_dataset.py`
- Example: `MAX_DTE = 365` includes options up to 1 year out
- **Use case:** Long-term gamma analysis, LEAPS strategies

### 4. **Extended Historical Range**
**Current Dataset:** Oct 10 - Nov 9, 2025 (1 month)

**To Get More History:**
Massive API provides 2-5 years of historical data (plan dependent)

**Extend back to 2024:**
```bash
python build_historical_dataset.py 2024-01-01 2025-11-09
```

**Extend to full 2 years:**
```bash
python build_historical_dataset.py 2023-11-01 2025-11-09
```

**Estimate:** ~250 trading days/year × 10 tickers × ~150 contracts = ~375,000 contracts/year

### 5. **Live/Real-Time Data Collection**
**Current:** Historical static snapshots

**To Add Live Collection:**
- Script `fetch_greeks_snapshot.py` (no date param) fetches CURRENT market Greeks
- Run during market hours to capture live changes
- Useful for monitoring today's gamma exposure in real-time

**Live gamma dashboard example:**
```bash
# Poll every 5 minutes during trading
watch -n 300 "python fetch_greeks_snapshot.py && python analyze_gamma_exposure.py"
```

### 6. **Corporate Events Context**
**Not Currently Included:** Earnings dates, dividends, splits

**To Add:**
- Cross-reference with external calendar (Yahoo Finance API, Alpha Vantage)
- Flag high-impact dates in your analysis
- **Why it matters:** Gamma/IV spikes around earnings, anomalous pin behavior

---

## 📊 Current Dataset Scope Summary

| Aspect | Current | Can Extend To |
|--------|---------|---------------|
| **Time Range** | Oct 10 - Nov 9, 2025 | 2+ years back |
| **Resolution** | End-of-day | Minute (going forward) |
| **DTE Range** | 0-30 days | Any (up to 2+ years) |
| **Strike Range** | ±50% spot | Full chain (±200%+) |
| **Tickers** | 10 symbols | Any liquid options |
| **Data Points** | ~50K contracts | Millions possible |

---

## 🎯 Data Quality & Completeness

### Dataset Coverage
**Important:** This section describes OUR current downloaded dataset (Oct 10 - Nov 9, 2025), NOT what Massive has available.

**Massive Has Available:** 2-5 years of historical options data with Greeks (plan dependent)

**Our Current Downloaded Dataset:**

| Date Range | QQQ | Other Tickers | Notes |
|------------|-----|---------------|-------|
| Oct 10-12 | Empty | Empty | Non-trading days (Columbus Day weekend) |
| Oct 13-14 | ✅ Full | ⚠️ Partial | Download captured QQQ first, others may have loaded later |
| Oct 15 - Nov 9 | ✅ Full | ✅ Full | Complete coverage for all 10 tickers |

**Note:** The partial data on Oct 13-14 is an artifact of our download process, NOT a Massive API limitation. 

**To Get Complete Data for Any Date:**
```bash
# Re-download specific dates with full coverage
python build_historical_dataset.py 2025-10-13 2025-10-14
```

**Recommendation:** For this initial dataset, start analysis from **Oct 15, 2025** forward. Or re-download Oct 13-14 to fill gaps.

### Data Freshness
- **Source:** Massive.io REST API historical snapshots
- **Delay:** End-of-day data (15-min to 1-day delay depending on plan)
- **Greeks Calculation:** Provided by Massive (not calculated by us)

### Known Limitations
1. **Illiquid Contracts:** Some far OTM options may have `null` Greeks
2. **Corporate Actions:** Splits/dividends may cause ticker/strike adjustments not reflected
3. **Exchange Holidays:** No data on market holidays

---

## 🔧 How to Use This Data

### Quick Start: Load a Day's Data
```python
import pandas as pd

# Load QQQ options for a specific date
df = pd.read_csv('data/2025-10-15/QQQ_options.csv')

print(f"Loaded {len(df)} contracts")
print(f"Expirations: {df['expiration'].unique()}")
print(f"Strike range: ${df['strike'].min():.0f} - ${df['strike'].max():.0f}")
```

### Example 1: Calculate Gamma Exposure by Strike
```python
import pandas as pd
import numpy as np

df = pd.read_csv('data/2025-10-15/QQQ_options.csv')

# Remove contracts with missing data
df = df.dropna(subset=['gamma', 'open_interest', 'delta'])

# Calculate gamma exposure (positive for calls, negative for puts)
df['gamma_exposure'] = np.where(
    df['type'] == 'C',
    df['gamma'] * df['open_interest'] * 100,  # Calls: positive
    -df['gamma'] * df['open_interest'] * 100  # Puts: negative
)

# Aggregate by strike
gex_by_strike = df.groupby('strike')['gamma_exposure'].sum().sort_index()

print("Top Gamma Exposure Strikes:")
print(gex_by_strike.abs().nlargest(10))
```

### Example 2: Find ATM Options
```python
df = pd.read_csv('data/2025-10-15/QQQ_options.csv')

# Get current underlying price
spot = df['underlying_price'].iloc[0]

# Find nearest strike
df['distance_from_atm'] = abs(df['strike'] - spot)
atm_contracts = df.nsmallest(20, 'distance_from_atm')

print(f"Spot: ${spot:.2f}")
print(atm_contracts[['ticker', 'strike', 'type', 'delta', 'gamma', 'open_interest']])
```

### Example 3: Track Pin Levels Over Time
```python
import pandas as pd
from glob import glob

pin_strikes = []

for csv_file in sorted(glob('data/*/QQQ_options.csv')):
    date = csv_file.split('/')[1]  # Extract date from path
    df = pd.read_csv(csv_file)
    
    # Calculate GEX
    df['gex'] = df.apply(
        lambda x: x['gamma'] * x['open_interest'] * 100 * (1 if x['type']=='C' else -1), 
        axis=1
    )
    
    gex_by_strike = df.groupby('strike')['gex'].sum()
    max_gex_strike = gex_by_strike.abs().idxmax()
    
    pin_strikes.append({
        'date': date,
        'pin_strike': max_gex_strike,
        'gex': gex_by_strike[max_gex_strike],
        'spot': df['underlying_price'].iloc[0]
    })

pin_df = pd.DataFrame(pin_strikes)
print(pin_df)
```

---

## 🚀 Next Steps: Analysis Pipeline

### Phase 1: Exploratory Analysis ✅ (Data Ready!)
- [x] Historical data downloaded
- [ ] Load and validate data quality
- [ ] Visualize gamma exposure by strike
- [ ] Identify high-OI strikes (pin candidates)

### Phase 2: Gamma Exposure Calculation (TO BUILD)
**Script Needed:** `analyze_gamma_exposure.py`
- Calculate aggregate GEX by strike
- Weight by open interest
- Identify dealer positioning (long/short gamma)
- Output: Pin levels, charm flows, gamma walls

### Phase 3: Backtesting (TO BUILD)
**Script Needed:** `backtest_pin_strategy.py`
- Test historical pin accuracy
- Measure price magnetism near max GEX strikes
- Calculate P&L from mean reversion trades
- Output: Sharpe ratio, win rate, drawdown

### Phase 4: Live Trading Signals (TO BUILD)
**Script Needed:** `live_signals.py`
- Fetch current Greeks via REST API
- Compare to historical patterns
- Generate entry/exit signals
- Output: Trade recommendations with confidence scores

---

## 📚 Key Concepts

### Gamma Exposure (GEX)
**Formula:** `GEX = Gamma × Open Interest × 100 × ±1`
- **Positive GEX (Calls):** Market makers short calls → must buy underlying when price rises → suppresses volatility
- **Negative GEX (Puts):** Market makers short puts → must sell underlying when price falls → amplifies volatility

### Pin Risk / Max Pain
**Theory:** Options settlement creates "magnetic" effect toward strikes with highest open interest

**Why:** Market makers hedge by buying/selling underlying, pushing price toward max GEX strike

### 0DTE / Short-Dated Options
**Importance:** Gamma explodes near expiration → strongest pin effects on 0-7 DTE

**Our Data:** Filtered to ≤30 DTE to capture this behavior

---

## 🔗 Related Files

| File | Purpose |
|------|---------|
| `smoke_test.py` | Test API connectivity, fetch live Greeks for one ticker |
| `fetch_greeks_snapshot.py` | Fetch current Greeks snapshot (today's data) |
| `build_historical_dataset.py` | Build multi-day historical dataset (what generated this data) |
| `ff_fetch_smart.py` | Fetch minute price aggregates (no Greeks, not recommended) |
| `DATA_README.md` | This file |

---

## 💡 Tips & Best Practices

### For Gamma Analysis:
1. **Focus on Expirations ≤7 DTE** - Strongest gamma effects
2. **Weight by OI** - Gamma without OI is meaningless
3. **Separate Calls/Puts** - They have opposite hedging pressure
4. **Watch for Charm** - Gamma changes throughout the day (need intraday data)

### For Backtesting:
1. **Start with Single Ticker** - QQQ has best data coverage
2. **Use Wide Date Range** - Need 50+ days for statistical significance
3. **Account for Slippage** - Options spreads are wide
4. **Test Multiple DTE** - 0DTE, 1DTE, 3DTE, 7DTE behave differently

### Data Quality Checks:
```python
# Check for missing Greeks
df = pd.read_csv('data/2025-10-15/QQQ_options.csv')
print(f"Missing delta: {df['delta'].isna().sum()}")
print(f"Missing gamma: {df['gamma'].isna().sum()}")
print(f"Missing OI: {df['open_interest'].isna().sum()}")

# Check for stale data
print(f"Underlying price consistency: {df['underlying_price'].nunique()} unique values")
```

---

## 📞 Questions?

**Data Issues?** Check:
1. Date exists and is a trading day
2. Ticker spelling matches exactly
3. API rate limits not exceeded

**Need More Data?** 
```bash
# Extend date range
python build_historical_dataset.py 2024-06-01 2025-11-09

# Add more tickers (edit TARGET_UNDERLYINGS in script)
```

**Ready to Analyze?**
Next up: Build `analyze_gamma_exposure.py` to turn this raw data into actionable pin levels and trading signals!

---

**Generated by:** `build_historical_dataset.py`  
**Data Source:** Massive.io Options Chain Snapshot API  
**Last Build:** 2025-11-09

