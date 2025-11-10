#!/usr/bin/env python3
"""
CLI wrapper for building historical option chains with Greeks.

Usage:
    python build_chain.py QQQ 2025-05-15 --dte-max 30 --output backtests/QQQ/2025-05-15
    
This correctly uses:
- /reference/options/contracts?as_of= for historical contract universe
- /quotes/{ticker}?date= for historical quotes
- Computes Greeks from solved IV (not snapshot endpoint)
"""

import argparse
import sys
import os
from pathlib import Path

# Add backtest_builder to path
sys.path.insert(0, str(Path(__file__).parent))

from backtest_builder.chain_builder import build_chain
from backtest_builder.massive_client import MassiveClient

def main():
    parser = argparse.ArgumentParser(
        description="Build historical options chain with Greeks from Massive API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build chain for QQQ on May 15, 2025 (0-30 DTE)
  python build_chain.py QQQ 2025-05-15
  
  # Custom DTE window and output
  python build_chain.py NVDA 2025-05-20 --dte-max 45 --output data/NVDA_20250520
  
  # With custom risk-free rate and dividend yield
  python build_chain.py SPY 2025-06-01 --r 0.045 --q 0.015
  
  # Provide underlier price directly (skips fetching)
  python build_chain.py AAPL 2025-05-15 --underlier-px 225.50

Output Structure:
  {output}/
    ├── contracts_raw.parquet          # All contracts as of date
    ├── contracts_dte_0_30.parquet     # Filtered by DTE
    ├── quotes_joined.parquet          # With session cut quotes
    ├── iv_solved.parquet              # After IV solve
    ├── chain_0_30_with_greeks.parquet # Final chain with Greeks + OI
    ├── gamma_expo_by_strike.parquet   # Gamma exposure aggregated
    └── manifest.json                  # Metadata
        """
    )
    
    parser.add_argument("underlier", help="Underlying ticker (e.g., QQQ, SPY, AAPL)")
    parser.add_argument("as_of", help="Backtest date (YYYY-MM-DD)")
    parser.add_argument("--cut", default="16:00:00 America/New_York",
                        help="Session cut time (default: 16:00:00 America/New_York)")
    parser.add_argument("--dte-max", type=int, default=30,
                        help="Maximum DTE to include (default: 30)")
    parser.add_argument("--r", "--risk-free", type=float, default=0.02, dest="r_flat",
                        help="Flat risk-free rate (default: 0.02)")
    parser.add_argument("--q", "--div-yield", type=float, default=0.0, dest="q_flat",
                        help="Dividend yield (default: 0.0)")
    parser.add_argument("--underlier-px", type=float, default=None,
                        help="Provide underlier spot price (skips fetching)")
    parser.add_argument("--output", "-o", default=None,
                        help="Output directory (default: backtests/{underlier}/{as_of})")
    parser.add_argument("--api-key", default=None,
                        help="Massive API key (default: MASSIVE_API_KEY env var)")
    parser.add_argument("--use-rest", action="store_true",
                        help="Use REST API for quotes (slower but no S3 needed)")
    
    args = parser.parse_args()
    
    # Setup output directory
    if args.output is None:
        args.output = f"backtests/{args.underlier}/{args.as_of}"
    
    Path(args.output).mkdir(parents=True, exist_ok=True)
    
    # Setup client
    client = MassiveClient(api_key=args.api_key) if args.api_key else MassiveClient()
    
    print(f"\n{'='*70}")
    print(f"Building Historical Options Chain")
    print(f"{'='*70}")
    print(f"Underlier:  {args.underlier}")
    print(f"Date:       {args.as_of}")
    print(f"Cut:        {args.cut}")
    print(f"DTE window: 0-{args.dte_max} days")
    print(f"Risk-free:  {args.r_flat:.4f}")
    print(f"Div yield:  {args.q_flat:.4f}")
    print(f"Output:     {args.output}")
    print(f"{'='*70}\n")
    
    try:
        result = build_chain(
            underlier=args.underlier,
            as_of=args.as_of,
            cut=args.cut,
            dte_max=args.dte_max,
            r_flat=args.r_flat,
            q_flat=args.q_flat,
            underlier_px=args.underlier_px,
            outdir=args.output,
            client=client,
            use_flat_files=not args.use_rest  # Default to Flat Files (fast)
        )
        
        print(f"\n{'='*70}")
        print(f"✅ SUCCESS! Chain built successfully")
        print(f"{'='*70}")
        print(f"\nFiles created:")
        for key, path in result.items():
            print(f"  • {key:20s} → {path}")
        
        print(f"\nNext steps:")
        print(f"  1. Inspect: cat {result['manifest']}")
        print(f"  2. Load chain: pd.read_parquet('{result['chain']}')")
        print(f"  3. Analyze gamma: pd.read_parquet('{result['gamma_expo']}')")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())

