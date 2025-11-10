# Massive API Options-Only Smoke Test

Zero-placeholder, end-to-end test for Massive (ex-Polygon) **Options REST API** + optional **Flat Files S3** connectivity check.

## What This Does

1. **Authenticate** with your Massive API key
2. **Pull Unified Snapshot** (if your plan includes it) to get the underlying spot price
3. **Pull Option Chain Snapshot** for QQQ — full chain with all expiries/strikes
4. **Select ATM Call & Put** for the nearest expiry
5. **Pull Option Contract Snapshots** for those two contracts — print:
   - Price (bid/ask/last)
   - IV (implied volatility)
   - Greeks (delta, gamma, theta, vega)
   - Open Interest
   - NBBO (bid/ask sizes)
6. **(Optional)** Verify S3 Flat Files connectivity for historical bulk data

## Project Structure

```
massive_options_smoke/
├── README.md              # This file
├── requirements.txt       # Python dependencies
├── .env                   # Your API keys (already configured)
├── smoke_test.py          # Main REST API workflow
├── s3_check.py            # S3 Flat Files connectivity check
└── ff_list_and_fetch.py   # Historical data downloader via S3
```

## Quick Start

### 1. Set up the environment

```bash
cd massive_options_smoke
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Run the smoke test

```bash
python smoke_test.py
```

**Expected output:**
- Unified snapshot data (or note if not in plan)
- Selected expiry and ATM contracts
- Full contract snapshots with IV, Greeks, OI, NBBO

### 3. (Optional) Test S3 Flat Files

First, add your S3 credentials to `.env`:

```bash
MASSIVE_S3_ACCESS_KEY=your_s3_access_key
MASSIVE_S3_SECRET_KEY=your_s3_secret_key
```

Then run:

```bash
python s3_check.py
```

This lists the first 20 objects under `us_options_opra` to verify connectivity.

## Manual cURL Tests

### 1. Unified Snapshot

```bash
export MASSIVE_API_KEY="Vye5Ze9AxWD72_SCI_sVo424agRqD6cC"
curl -s -H "Authorization: Bearer $MASSIVE_API_KEY" \
  "https://api.massive.com/v3/snapshot?ticker.any_of=QQQ" | jq .
```

### 2. Option Chain Snapshot

```bash
curl -s -H "Authorization: Bearer $MASSIVE_API_KEY" \
  "https://api.massive.com/v3/snapshot/options/QQQ" | jq .
```

### 3. Option Contract Snapshot

```bash
CONTRACT="O:QQQ20251115C00480000"  # Replace with actual contract from chain
curl -s -H "Authorization: Bearer $MASSIVE_API_KEY" \
  "https://api.massive.com/v3/snapshot/options/QQQ/$CONTRACT" | jq .
```

## Key Features

✅ **No Placeholders** — All endpoints are real, all parsing is complete  
✅ **Graceful Degradation** — Works without Unified Snapshot if not in plan  
✅ **ATM Selection** — Deterministically picks nearest expiry and ATM strike  
✅ **Full Contract Data** — IV, Greeks, OI, NBBO from live snapshots  
✅ **S3 Verification** — Optional check for historical bulk data access  

## Troubleshooting

### "Unified snapshot unavailable or not in plan"
This is expected if your Massive plan is options-only. The script will derive spot from chain NBBO and continue.

### "Empty chain results"
- Verify ticker symbol (e.g., QQQ)
- Check that options are included in your plan
- Try during market hours for live data

### S3 Connection Errors
- Verify S3 credentials in `.env`
- Check that Flat Files are included in your plan
- Try prefix `us_options_opra` for options or `us_stocks_sip` for stocks

## Historical Data via Flat Files

Once the REST smoke test passes, download historical data for backtesting:

### 1. Set S3 credentials

Get your S3 credentials from the Massive dashboard (Flat Files section) and add to `.env`:

```bash
MASSIVE_S3_ACCESS_KEY=your_s3_access_key_here
MASSIVE_S3_SECRET_KEY=your_s3_secret_key_here
```

Or export as environment variables:

```bash
export MASSIVE_S3_ACCESS_KEY="your_s3_access_key_here"
export MASSIVE_S3_SECRET_KEY="your_s3_secret_key_here"
```

### 2. Download historical data

```bash
python ff_list_and_fetch.py 2025-11-07
```

This will:
- Auto-discover available datasets (OPRA quotes, trades, OI; stocks minute aggregates)
- Download data for the specified date
- Extract gzipped files automatically
- Save to `flatfiles_dl/YYYY-MM-DD/`

**What you get:**
- `opra_quotes/` - Options quotes (all strikes/expiries)
- `opra_trades/` - Options trades
- `opra_open_interest/` - Daily OI snapshot
- `stocks_minute/` - Underlying (QQQ) minute bars (if stocks plan included)

## Next Steps

1. **Extend to more tickers** (SPY, AAPL, etc.)
2. **Build replay engine** for backtesting with historical flat files
3. **Calculate Greeks/gamma exposure** from options chain
4. **Real-time feeds** via WebSocket for live trading signals
5. **Integration** with your quant pipeline

---

**API Key Configured:** `Vye5Ze9AxWD72_SCI_sVo424agRqD6cC`

