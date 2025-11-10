import argparse, os, json
from ..chain_builder import build_chain
from ..massive_client import MassiveClient

def main():
    p = argparse.ArgumentParser(description="Rebuild historical 0–30 DTE chain with IV & Greeks")
    p.add_argument("--underlier", required=True)
    p.add_argument("--as-of", required=True, help="YYYY-MM-DD")
    p.add_argument("--cut", required=False, default="16:00:00 America/New_York")
    p.add_argument("--dte-max", type=int, default=30)
    p.add_argument("--r-flat", type=float, default=0.02)
    p.add_argument("--q-flat", type=float, default=0.00)
    p.add_argument("--outdir", required=True)
    p.add_argument("--underlier-px", type=float, default=None)
    args = p.parse_args()

    client = MassiveClient()
    os.makedirs(args.outdir, exist_ok=True)
    artifacts = build_chain(
        underlier=args.underlier,
        as_of=args.as_of,
        cut=args.cut,
        dte_max=args.dte_max,
        r_flat=args.r_flat,
        q_flat=args.q_flat,
        outdir=args.outdir,
        client=client,
        underlier_px=args.underlier_px
    )
    print(json.dumps(artifacts, indent=2))

if __name__ == "__main__":
    main()
