import numpy as np
import pandas as pd

def valid_mid(bid, ask):
    if pd.isna(bid) or pd.isna(ask):
        return False
    if bid <= 0 or ask <= 0:
        return False
    if ask < bid:
        return False
    mid = 0.5 * (bid + ask)
    if mid <= 0:
        return False
    if (ask - bid) / mid > 0.5:
        return False
    return True

def build_session_cut(quotes_df: pd.DataFrame, cut_ts_utc) -> pd.DataFrame:
    df = quotes_df.sort_values(["option_symbol", "ts"])
    df = df[df["ts"] <= cut_ts_utc]
    ix = df.groupby("option_symbol")["ts"].idxmax()
    snap = df.loc[ix].copy()
    snap["mid"] = np.where(snap.apply(lambda r: valid_mid(r["bid"], r["ask"]), axis=1),
                           (snap["bid"] + snap["ask"]) * 0.5,
                           np.nan)
    snap["px_for_iv"] = snap["mid"].combine_first(snap["last"])
    snap["used_last"] = snap["mid"].isna() & snap["last"].notna()
    snap["spread"] = snap["ask"] - snap["bid"]
    return snap
