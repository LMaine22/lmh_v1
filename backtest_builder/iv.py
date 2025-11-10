from __future__ import annotations
import math
from typing import Tuple

SQRT_2PI = math.sqrt(2.0 * math.pi)

def _phi(x: float) -> float:
    return math.exp(-0.5 * x * x) / SQRT_2PI

def _Phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def bs_price(S, K, r, q, sigma, T, is_call: bool):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        intrinsic = max(0.0, S*math.exp(-q*T) - K*math.exp(-r*T)) if is_call else max(0.0, K*math.exp(-r*T) - S*math.exp(-q*T))
        return intrinsic
    sig_rt = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / sig_rt
    d2 = d1 - sig_rt
    if is_call:
        return S*math.exp(-q*T)*_Phi(d1) - K*math.exp(-r*T)*_Phi(d2)
    else:
        return K*math.exp(-r*T)*_Phi(-d2) - S*math.exp(-q*T)*_Phi(-d1)

def solve_iv(target_price: float, S: float, K: float, r: float, q: float, T: float, is_call: bool,
             tol: float = 1e-6, max_iter: int = 100, lo: float = 1e-4, hi: float = 5.0) -> Tuple[float, dict]:
    intrinsic = max(0.0, S*math.exp(-q*T) - K*math.exp(-r*T)) if is_call else max(0.0, K*math.exp(-r*T) - S*math.exp(-q*T))
    price = max(target_price, intrinsic + 1e-6)
    def f(sig): return bs_price(S, K, r, q, sig, T, is_call) - price
    f_lo, f_hi = f(lo), f(hi)
    if f_lo * f_hi > 0:
        hi_try = hi
        for _ in range(10):
            hi_try *= 2.0
            f_hi = f(hi_try)
            if f_lo * f_hi <= 0:
                hi = hi_try
                break
        else:
            return float("nan"), {"status":"no_bracket"}
    a, b = lo, hi
    fa, fb = f_lo, f_hi
    c, fc = a, fa
    d = e = b - a
    for it in range(max_iter):
        if fb == 0:
            return b, {"status":"ok","iter":it}
        if fa * fb > 0:
            a, fa = c, fc
            d = e = b - a
        if abs(fa) < abs(fb):
            c, fc = b, fb
            b, fb = a, fa
            a, fa = c, fc
        m = 0.5 * (a - b)
        tol1 = 2.0 * 1e-12 * abs(b) + 0.5 * tol
        if abs(m) <= tol1 or fb == 0.0:
            return b, {"status":"ok","iter":it}
        if abs(e) >= tol1 and abs(fc) > abs(fb):
            s = fb / fc
            if a == c:
                p = 2.0 * m * s
                qv = 1.0 - s
            else:
                qv = fc / fa
                r = fb / fa
                p = s * (2.0 * m * qv * (qv - r) - (b - c) * (r - 1.0))
                qv = (qv - 1.0) * (r - 1.0) * (s - 1.0)
            if p > 0:
                qv = -qv
            p = abs(p)
            min1 = 3.0 * m * qv - abs(tol1 * qv)
            min2 = abs(e * qv)
            if 2.0 * p < (min1 if min1 < min2 else min2):
                e = d
                d = p / qv
            else:
                d = m
                e = m
        else:
            d = m
            e = m
        c, fc = a, fa
        a, fa = b, fb
        if abs(d) > tol1:
            b += d
        else:
            b += tol1 if m > 0 else -tol1
        fb = f(b)
    return b, {"status":"max_iter"}
