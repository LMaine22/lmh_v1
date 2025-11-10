# backtest_builder

Rebuild a **historical 0–30 DTE options chain** for a given date, compute **IV** and **Greeks**, and export artifacts.

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export MASSIVE_API_KEY="...your key..."
python scripts/build_chain_cli.py --underlier QQQ --as-of 2025-05-15 --cut "16:00:00 America/New_York" \
    --dte-max 30 --outdir runs/QQQ/2025-05-15
```
