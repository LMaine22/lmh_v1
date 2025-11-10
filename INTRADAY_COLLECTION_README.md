# Intraday Greeks Collection System

## 🎯 Purpose

Build your own **intraday Greeks history** by polling Massive REST API every 15-60 minutes during market hours. Over weeks/months, you'll have high-resolution historical Greeks for backtesting intraday strategies.

---

## 📊 What It Collects

**Every 30 minutes (9:30am - 4:00pm ET):**
- Full options chain for 10 tickers (QQQ, NVDA, TSLA, COIN, AMZN, AMD, AAPL, MSFT, AVGO, META)
- Contracts with ≤30 DTE only
- **All fields**: Greeks (delta, gamma, theta, vega, IV), Open Interest, bid/ask, volume, underlying price

**Output Structure:**
```
data/intraday_greeks/
├── 2025-11-10/
│   ├── QQQ_093000.parquet    # 9:30am snapshot
│   ├── NVDA_093000.parquet
│   ├── ...
│   ├── QQQ_100000.parquet    # 10:00am snapshot
│   ├── NVDA_100000.parquet
│   ├── ...
│   └── QQQ_160000.parquet    # 4:00pm snapshot
├── 2025-11-11/
│   └── ...
```

**Data Per Snapshot:**
- ~100-500 contracts per ticker
- Full Greeks + OI + prices
- Timestamp (UTC and ET)
- DTE calculated from collection time

---

## 🚀 Quick Start

### Manual Test (Run Once Now):

```bash
cd "/Users/lutherhart/Library/Mobile Documents/com~apple~CloudDocs/Quant Projects/lmhv_01"
source .venv/bin/activate
export MASSIVE_API_KEY="Vye5Ze9AxWD72_SCI_sVo424agRqD6cC"
python collect_intraday_greeks.py
```

**Expected Output:**
```
📸 Collecting Intraday Greeks Snapshot
UTC: 2025-11-10T14:30:00Z
ET:  2025-11-10 09:30:00 

[QQQ] Fetching chain... ✓ 450 contracts → QQQ_093000.parquet
[NVDA] Fetching chain... ✓ 120 contracts → NVDA_093000.parquet
...
✅ Snapshot complete: 2,500 total contracts
📁 Saved to: data/intraday_greeks/2025-11-10/
```

---

## ⏰ Automated Collection (Cron Setup)

### Option 1: Use Setup Script

```bash
cd "/Users/lutherhart/Library/Mobile Documents/com~apple~CloudDocs/Quant Projects/lmhv_01"
chmod +x setup_cron_collection.sh
./setup_cron_collection.sh
```

Follow the instructions to add to crontab.

### Option 2: Manual Cron Entry

```bash
crontab -e
```

Add this line:
```cron
*/30 9-16 * * 1-5 cd "/Users/lutherhart/Library/Mobile Documents/com~apple~CloudDocs/Quant Projects/lmhv_01" && source .venv/bin/activate && python collect_intraday_greeks.py >> logs/greeks_collection.log 2>&1
```

**Translation:**
- `*/30` = Every 30 minutes
- `9-16` = Between 9am and 4pm (covers 9:30am-4:00pm with script's internal check)
- `1-5` = Monday-Friday
- `>> logs/...` = Append to log file

### Option 3: Use Launchd (macOS native scheduler)

Create file: `~/Library/LaunchAgents/com.lmhv.greeks.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.lmhv.greeks</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>-c</string>
        <string>cd "/Users/lutherhart/Library/Mobile Documents/com~apple~CloudDocs/Quant Projects/lmhv_01" && source .venv/bin/activate && python collect_intraday_greeks.py</string>
    </array>
    <key>StartCalendarInterval</key>
    <array>
        <dict>
            <key>Weekday</key>
            <integer>1-5</integer>
            <key>Hour</key>
            <integer>9</integer>
            <key>Minute</key>
            <integer>30</integer>
        </dict>
        <dict>
            <key>Weekday</key>
            <integer>1-5</integer>
            <key>Hour</key>
            <integer>10</integer>
            <key>Minute</key>
            <integer>0</integer>
        </dict>
        <!-- Add more intervals as needed -->
    </array>
    <key>StandardOutPath</key>
    <string>/Users/lutherhart/Library/Mobile Documents/com~apple~CloudDocs/Quant Projects/lmhv_01/logs/greeks_collection.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/lutherhart/Library/Mobile Documents/com~apple~CloudDocs/Quant Projects/lmhv_01/logs/greeks_collection_error.log</string>
</dict>
</plist>
```

Then:
```bash
launchctl load ~/Library/LaunchAgents/com.lmhv.greeks.plist
```

---

## 📈 What You'll Build Over Time

### After 1 Week:
- ~5 trading days × 13 snapshots/day = 65 snapshots per ticker
- ~32,500 total contract records
- Can analyze intraday gamma shifts, charm effects

### After 1 Month:
- ~21 trading days × 13 snapshots/day = 273 snapshots per ticker
- ~136,500 total contract records
- Can backtest intraday strategies

### After 6 Months:
- ~126 trading days × 13 snapshots/day = 1,638 snapshots per ticker
- ~819,000 total contract records
- Rich dataset for ML/pattern recognition

---

## 🔍 Using the Collected Data

### Load All Snapshots for a Day:

```python
import pandas as pd
from pathlib import Path

date = "2025-11-10"
ticker = "QQQ"

# Load all snapshots for this day
files = sorted(Path(f"data/intraday_greeks/{date}").glob(f"{ticker}_*.parquet"))
snapshots = [pd.read_parquet(f) for f in files]

print(f"Collected {len(snapshots)} snapshots on {date}")

# Combine
df_intraday = pd.concat(snapshots, ignore_index=True)

# Analyze intraday gamma evolution
gamma_by_time = df_intraday.groupby(['collected_at_et', 'strike'])['gamma'].sum()
```

### Track Pin Level Evolution:

```python
import pandas as pd
from glob import glob

# Get all QQQ snapshots across multiple days
all_files = sorted(glob("data/intraday_greeks/*/QQQ_*.parquet"))

pin_evolution = []
for f in all_files:
    df = pd.read_parquet(f)
    
    # Calculate gamma exposure
    df['gex'] = df['gamma'] * df['open_interest'] * df['underlying_price'] * 0.01
    
    # Find max GEX strike
    gex_by_strike = df.groupby('strike')['gex'].sum()
    max_strike = gex_by_strike.abs().idxmax()
    
    pin_evolution.append({
        'timestamp': df['collected_at_et'].iloc[0],
        'pin_strike': max_strike,
        'spot': df['underlying_price'].iloc[0],
        'gex': gex_by_strike[max_strike]
    })

pin_df = pd.DataFrame(pin_evolution)
# Now you can see how pin levels shift throughout the day!
```

---

## ⚙️ Configuration

Edit `collect_intraday_greeks.py` to adjust:

```python
TARGET_UNDERLYINGS = ["QQQ", "SPY", "AAPL"]  # Change tickers
MAX_DTE = 7  # Only collect very short-dated
```

Edit cron frequency:
- `*/15 9-16 * * 1-5` = Every 15 minutes (more granular)
- `*/60 9-16 * * 1-5` = Every hour (lighter load)

---

## 📊 Monitoring

### Check if collection is running:

```bash
# View recent logs
tail -f logs/greeks_collection.log

# Count snapshots collected today
ls data/intraday_greeks/$(date +%Y-%m-%d)/ | wc -l

# Check cron status
crontab -l | grep greeks
```

### Disk Space:

- **Per snapshot**: ~5-20 MB (10 tickers, compressed parquet)
- **Per day**: ~150 MB (13 snapshots)
- **Per month**: ~3 GB
- **Per year**: ~36 GB

---

## 🎯 Benefits vs Historical Data

| Aspect | Historical (Massive) | Intraday Collection (This) |
|--------|---------------------|----------------------------|
| **Availability** | 2-5 years back | Starting now |
| **Resolution** | EOD only | Every 30 min |
| **Greeks** | Must compute from quotes | Live from API |
| **Effort** | Complex pipeline | Simple cron |
| **Coverage** | Past (limited access) | Future (unlimited) |

---

## 🚀 Getting Started TODAY

```bash
# 1. Test manual collection
python collect_intraday_greeks.py

# 2. Set up cron
./setup_cron_collection.sh

# 3. Wait a week, then analyze
python analyze_intraday_gamma.py  # (we'll build this next)
```

After just **1 week** you'll have more intraday Greeks data than most retail traders ever get! 📈

