from __future__ import annotations
import pandas as pd
from typing import Dict
from datetime import date

def flat_curve(value: float, expiries) -> Dict[date, float]:
    return {d: float(value) for d in expiries}

def load_curve_from_csv(path: str, date_col="as_of", tenor_col="tenor_days", rate_col="rate"):
    df = pd.read_csv(path)
    return df

def assign_by_nearest_tenor(expiries, as_of, curve_df, tenor_col="tenor_days", rate_col="rate"):
    out = {}
    for ex in expiries:
        dte = (ex - as_of).days
        if dte < 0:
            dte = 0
        row = curve_df.iloc[(curve_df[tenor_col] - dte).abs().argsort()].iloc[0]
        out[ex] = float(row[rate_col])
    return out
