"""guesser.sim - seeded stream generators (fair + planted structure) and a paper-trading simulator with a Deriv-like payout model.

Every function takes an explicit seed.  Streams return (digits int8 array, quotes float array or None).  Quotes are only
meaningful for the price-increment family; for the others they are None (price experts then stay at the null).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from . import evidence as evd
from . import policy as pol
from .core import GuesserConfig, SymbolGuesser


# ── fair streams ─────────────────────────────────────────────────────────────
def fair_pcg(seed: int, n: int):
    return np.random.default_rng(seed).integers(0, 10, n).astype(np.int8), None


def fair_drbg(seed: int, n: int):
    from rng_eval.zoo import null_drbg
    return null_drbg(seed, n), None


def fair_urandom(seed: int, n: int):
    from rng_eval.zoo import null_urandom
    return null_urandom(seed, n), None


# ── planted structure ────────────────────────────────────────────────────────
def bias(seed: int, n: int, p7: float):
    r = np.random.default_rng(seed)
    p = np.full(10, (1 - p7) / 9)
    p[7] = p7
    return r.choice(10, size=n, p=p).astype(np.int8), None


def markov1(seed: int, n: int, tilt: float):
    """P(D[t+1] = (D[t]+3) mod 10) = 0.1 + tilt, the rest share the remainder equally."""
    r = np.random.default_rng(seed)
    d = np.zeros(n, np.int64)
    d[0] = r.integers(10)
    u = r.random(n)
    alt = r.integers(0, 10, n)
    for t in range(1, n):
        d[t] = (d[t - 1] + 3) % 10 if u[t] < tilt * 10 / 9 + 0.0 else alt[t]
    # u < 10*tilt/9 yields P = 10tilt/9 + (1-10tilt/9)*0.1 = 0.1 + tilt  (exact)
    return d.astype(np.int8), None


def markov2(seed: int, n: int, tilt: float):
    r = np.random.default_rng(seed)
    d = np.zeros(n, np.int64)
    d[:2] = r.integers(0, 10, 2)
    fav = r.integers(0, 10, 100)
    u = r.random(n)
    alt = r.integers(0, 10, n)
    for t in range(2, n):
        d[t] = fav[d[t - 2] * 10 + d[t - 1]] if u[t] < tilt * 10 / 9 else alt[t]
    return d.astype(np.int8), None


def lagcopy(seed: int, n: int, lag: int, p: float):
    """D[t] = D[t-lag] with probability p (else fresh uniform).  Match prob = p + (1-p)/10."""
    r = np.random.default_rng(seed)
    d = r.integers(0, 10, n)
    u = r.random(n)
    for t in range(lag, n):
        if u[t] < p:
            d[t] = d[t - lag]
    return d.astype(np.int8), None


def periodic(seed: int, n: int, period: int, noise: float):
    r = np.random.default_rng(seed)
    pat = r.integers(0, 10, period)
    d = pat[np.arange(n) % period]
    m = r.random(n) < noise
    d = np.where(m, r.integers(0, 10, n), d)
    return d.astype(np.int8), None


def delta2(seed: int, n: int, q: float):
    """D[t] = (D[t-2] + 3) mod 10 with probability q (difference structure at lag 2)."""
    r = np.random.default_rng(seed)
    d = r.integers(0, 10, n)
    u = r.random(n)
    for t in range(2, n):
        if u[t] < q:
            d[t] = (d[t - 2] + 3) % 10
    return d.astype(np.int8), None


def regime(seed: int, n: int, switch: int, inner: Callable):
    """Fair until `switch`, then the structured stream `inner(seed, n)` takes over."""
    base, _ = fair_pcg(seed + 7, n)
    s, _ = inner(seed, n)
    out = base.copy()
    out[switch:] = s[switch:]
    return out, None


def price_inc(seed: int, n: int, rho: float, sd: float = 1.5, dec: int = 2):
    """Price = random walk in units of 10^-dec whose increments are AR(1) (momentum rho); digit = last decimal of the price.
    The digit moves by the increment, so increments predict digit changes - the one case where the digit is a function of
    something richer than itself."""
    r = np.random.default_rng(seed)
    inc = np.zeros(n)
    e = r.standard_normal(n) * sd
    for t in range(1, n):
        inc[t] = rho * inc[t - 1] + e[t]
    v = 100000 + np.cumsum(np.round(inc)).astype(np.int64)
    return (v % 10).astype(np.int8), v / (10.0 ** dec)


def weak(seed: int, n: int, gen: str = "xorshift32", mp: str = "mod10"):
    from rng_eval.zoo import weak_digits
    return weak_digits(gen, mp, seed, n), None


# ── paper trading ────────────────────────────────────────────────────────────
@dataclass
class PaperResult:
    policy: str
    trades: int = 0
    wins: int = 0
    probe_trades: int = 0
    probe_net: float = 0.0
    evidence_trades: int = 0
    pnl: float = 0.0
    start: float = 0.0
    end: float = 0.0
    first_trade_tick: Optional[int] = None
    first_t2_tick: Optional[int] = None
    target_tick: Optional[int] = None
    breakeven_sum: float = 0.0
    ticks: int = 0
    busted: bool = False

    def as_dict(self) -> dict:
        d = dict(self.__dict__)
        d["win_rate"] = self.wins / self.trades if self.trades else None
        d["breakeven"] = self.breakeven_sum / self.trades if self.trades else None
        return d


def paper_trade(digits: np.ndarray, quotes: Optional[np.ndarray], policy: str, margin: float = 0.05, latency: int = 0,
                balance: float = 100.0, min_stake: float = 0.35, target: Optional[float] = None, n_symbols_assumed: int = 20,
                probe_frac: float = 0.05, probe_per_min: int = 12, tick_seconds: float = 1.0, kelly_frac: float = 0.25,
                max_frac: float = 0.05, mode_alpha: float = 0.01, allowed: Tuple[str, ...] = ("OVER", "UNDER"),
                seed: int = 0, warm: int = 0, max_stake: float = 50.0, stop_at_target: bool = False) -> PaperResult:
    """One symbol, one-tick contracts.  A contract bought on the decision at tick t settles on digit t+1+latency (the guesser's
    horizon is 1+latency, as in the engine).  policy: evidence | probe | bold | flat | money_rules.
    Payout ratio r = (1-margin)*10/k.  Deriv deducts the stake at buy time; payout = stake*r on a win."""
    h = 1 + int(latency)
    cfg = GuesserConfig(h=h, margin=margin, allowed=allowed, multiplicity="bonferroni", alpha1=mode_alpha)
    g = SymbolGuesser("SIM", cfg)
    res = PaperResult(policy=policy, start=balance)
    bal = balance
    tgt = target if target is not None else 2.0 * balance
    probe = pol.ProbeBudget(balance, probe_frac, probe_per_min, probe_per_min) if policy == "probe" else None
    pending: List[tuple] = []     # (settle_idx, stake, ratio, mask_row_index, kind, contract)
    cs, mask, ks = g.menu, g.mask, g.ks
    ratio_for = lambda c: pol.model_ratio(len(pol.win_set(c)), margin)
    mm = "evidence" if policy in ("evidence", "flat", "money_rules") else policy
    n = len(digits)
    for i in range(n):
        y = int(digits[i])
        # settle
        keep = []
        for p in pending:
            if p[0] == i:
                win = y in pol.win_set(p[5])
                pay = p[1] * p[2] if win else 0.0
                bal += pay
                res.trades += 1
                res.wins += int(win)
                res.breakeven_sum += 1.0 / p[2]
                res.pnl += pay - p[1]
                if p[4] == "probe":
                    probe.settle(p[1], pay)
                    res.probe_trades += 1
                    res.probe_net += pay - p[1]
                elif p[4] == "evidence":
                    res.evidence_trades += 1
            else:
                keep.append(p)
        pending = keep
        g.on_tick(y, None if quotes is None else float(quotes[i]), float(i))
        res.ticks = i + 1
        if i < warm:
            continue
        if res.target_tick is None and bal >= tgt:
            res.target_tick = i
            if stop_at_target and not pending:
                break
        # tier from the e-process (Bonferroni over the assumed number of symbols; restart spending via alpha_scale)
        a1 = mode_alpha * g.alpha_scale() / max(1, n_symbols_assumed)
        g.tier = evd.tier_of(g.log_e, a1, cfg.alpha2, False)
        if g.tier >= 2 and res.first_t2_tick is None:
            res.first_t2_tick = i
        v = g.view(ratio_for)
        if v is None:
            continue
        slots = 4 - len(pending)
        if policy in ("flat", "money_rules"):
            # baselines: bet the best POSTERIOR-edge contract every tick (no evidence gate), flat min stake or 17.5% of balance
            e = v.p_hat * v.ratio - 1.0
            j = int(np.argmax(e))
            if policy == "flat":
                s = min_stake
            else:
                s = min(max_stake, max(min_stake, pol.cents_down(0.175 * bal)))
            if slots > 0 and bal - s >= 0 and s >= min_stake:
                orders = [pol.Order("SIM", v.menu[j], s, "flat", float(v.p_lb[j]), float(v.ratio[j]), float(e[j]))]
            else:
                orders = []
        else:
            orders, _ = pol.decide([v], mm, bal, min_stake, slots, probe, i * tick_seconds, kelly_frac, max_frac, 0.0,
                                   target=tgt, max_stake=max_stake)
        for o in orders:
            if o.stake > bal + 1e-9:
                if o.kind == "probe":
                    probe.settle(o.stake, o.stake)        # release the reservation, nothing was bought
                continue
            bal -= o.stake
            pending.append((i + h, o.stake, o.ratio, 0, o.kind, o.contract))
            if res.first_trade_tick is None:
                res.first_trade_tick = i
        if bal < min_stake and not pending:
            res.busted = True
            break
    res.end = bal
    return res
