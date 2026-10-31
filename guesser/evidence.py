"""guesser.evidence - evidence tiers and multiplicity control on top of the e-process (log BF) of guesser.mixture.

Tiers (thresholds are CONVENTIONS from the brief; E1/E4 report what they cost, nothing here is "tuned"):
    T0 NONE      log BF < ln 3
    T1 PROBE     ln 3 <= log BF < ln(1/alpha2_eff)                         alpha2 = 0.05
    T2 EVIDENCE  log BF >= ln(1/alpha1_eff) after multiplicity correction   alpha1 = 0.01
    T3 EXACT     a validated exact-state expert (set by the caller; see engine)

Multiplicity over S symbols:
    bonferroni : alpha_eff = alpha / S (anytime-valid per symbol by Ville, union bound over symbols)  -> rigorous 'ever' guarantee
    ebh        : e-BH (Wang & Ramdas 2022) on the current e-values: reject the k* largest, k* = max{k : e_(k) >= S/(alpha k)};
                 controls FDR at alpha for e-values at ONE time.  Re-applying it at every scan is not covered by the theorem, so E1
                 measures the session-level 'ever reaches T2' rate for BOTH rules rather than assuming it.
Restarts: run k of a stream is tested at alpha * 6/(pi^2 k^2)  (mixture.Mixture.alpha_run).
"""
from __future__ import annotations

import math
from typing import Dict, List, Sequence

T0, T1, T2, T3 = 0, 1, 2, 3
TIER_NAMES = {T0: "NONE", T1: "PROBE", T2: "EVIDENCE", T3: "EXACT"}
LN3 = math.log(3.0)


def ebh(log_e: Sequence[float], alpha: float) -> List[int]:
    """e-BH: indices rejected.  e_(k) is the k-th largest e-value; reject the k* largest where k* is the largest k with
    e_(k) >= S / (alpha * k).  Works in log space (log_e may be huge)."""
    S = len(log_e)
    if S == 0:
        return []
    order = sorted(range(S), key=lambda i: -log_e[i])
    kstar = 0
    for k, i in enumerate(order, start=1):
        if log_e[i] >= math.log(S / (alpha * k)):
            kstar = k
    return order[:kstar]


def tier_of(log_e: float, alpha1_eff: float, alpha2_eff: float = 0.05, exact: bool = False) -> int:
    if exact:
        return T3
    if log_e >= math.log(1.0 / alpha1_eff):
        return T2
    if log_e >= max(LN3, 0.0) and log_e < math.log(1.0 / alpha2_eff):
        return T1
    if log_e >= math.log(1.0 / alpha2_eff):
        return T1          # between alpha2 and alpha1 thresholds: still weak evidence only
    return T0


def tiers(log_es: Dict[str, float], alpha1: float = 0.01, alpha2: float = 0.05, rule: str = "ebh",
          run_alpha_scale: Dict[str, float] = None, exact: Dict[str, bool] = None) -> Dict[str, int]:
    """Tier of every symbol.  run_alpha_scale[sym] = alpha_run/alpha_total (restart spending), default 1."""
    syms = list(log_es)
    S = max(1, len(syms))
    sc = run_alpha_scale or {}
    ex_ = exact or {}
    out: Dict[str, int] = {}
    if rule == "ebh":
        # e-BH at the per-run alpha of each symbol: use the smallest scale (conservative) for the joint rejection set
        a = alpha1 * min([sc.get(s, 1.0) for s in syms] or [1.0])
        rej = set(syms[i] for i in ebh([log_es[s] for s in syms], a))
        for s in syms:
            if ex_.get(s):
                out[s] = T3
            elif s in rej:
                out[s] = T2
            else:
                out[s] = tier_of(log_es[s], 1.0, alpha2)       # alpha1_eff=1 -> never T2 here
    else:
        for s in syms:
            a1 = alpha1 * sc.get(s, 1.0) / S
            out[s] = tier_of(log_es[s], a1, alpha2, ex_.get(s, False))
    return out
