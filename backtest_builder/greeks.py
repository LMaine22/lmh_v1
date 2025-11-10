from __future__ import annotations
import math

def _phi(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0*math.pi)

def _Phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def bs_greeks(S, K, r, q, sigma, T, is_call: bool, multiplier: float = 100.0):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return {"delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0, "rho": 0.0}
    sig_rt = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / sig_rt
    d2 = d1 - sig_rt
    if is_call:
        delta = math.exp(-q*T) * _Phi(d1)
        theta = (- (S * math.exp(-q*T) * _phi(d1) * sigma) / (2.0 * math.sqrt(T))
                 - r * math.exp(-r*T) * K * _Phi(d2)
                 + q * math.exp(-q*T) * S * _Phi(d1))
        rho = K * T * math.exp(-r*T) * _Phi(d2)
    else:
        delta = - math.exp(-q*T) * _Phi(-d1)
        theta = (- (S * math.exp(-q*T) * _phi(d1) * sigma) / (2.0 * math.sqrt(T))
                 + r * math.exp(-r*T) * K * _Phi(-d2)
                 - q * math.exp(-q*T) * S * _Phi(-d1))
        rho = -K * T * math.exp(-r*T) * _Phi(-d2)
    gamma = (math.exp(-q*T) * _phi(d1)) / (S * sigma * math.sqrt(T))
    vega  = S * math.exp(-q*T) * _phi(d1) * math.sqrt(T)

    return {
        "delta": delta * multiplier,
        "gamma": gamma * multiplier,
        "vega":  vega  * multiplier,
        "theta": theta * multiplier / 365.0,
        "rho":   rho   * multiplier
    }
