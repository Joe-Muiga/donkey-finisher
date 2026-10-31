"""guesser.policy - contract menu, edge, Kelly, bold play (Dubins-Savage DP), probe budget and the cadence scheduler.
PURE functions / small state classes: no I/O, no clock (callers pass `now`).

Edge.  A digit contract c wins on a set S_c (|S| = k).  With live payout ratio r (payout/stake) and win probability p:
    edge = p*r - 1,   break-even p = 1/r,   fair r = 10/k,   Deriv-like model r = (1-m)*10/k.
On a fair stream p = k/10 so edge = -m < 0 for EVERY contract: no staking rule can fix that (Kelly says 0; bold play only
changes the distribution of outcomes).  Policies below therefore only bet real money when a verified lower bound clears 1/r.
"""
from __future__ import annotations

import functools
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Sequence, Tuple

import numpy as np

EPS = 1e-9
Contract = Tuple[str, Optional[int]]          # ("OVER", 4) | ("UNDER", 5) | ("MATCH", 7) | ("DIFFER", 7) | ("EVEN", None) | ("ODD", None)


# ── menu ─────────────────────────────────────────────────────────────────────
def valid_barrier(ctype: str, b: Optional[int]) -> bool:
    if ctype == "OVER":
        return b is not None and 0 <= b <= 8
    if ctype == "UNDER":
        return b is not None and 1 <= b <= 9
    if ctype in ("MATCH", "DIFFER"):
        return b is not None and 0 <= b <= 9
    return ctype in ("EVEN", "ODD") and b is None


def win_set(c: Contract) -> List[int]:
    t, b = c
    if t == "OVER":
        return [d for d in range(10) if d > b]
    if t == "UNDER":
        return [d for d in range(10) if d < b]
    if t == "MATCH":
        return [b]
    if t == "DIFFER":
        return [d for d in range(10) if d != b]
    if t == "EVEN":
        return [d for d in range(0, 10, 2)]
    if t == "ODD":
        return [d for d in range(1, 10, 2)]
    raise ValueError(c)


def build_menu(allowed: Sequence[str]) -> Tuple[List[Contract], np.ndarray, np.ndarray]:
    """(contracts, mask (nc,10) of winning digits, k (nc,) set sizes).  Barriers follow Deriv: Over 0-8, Under 1-9, Match/Differ 0-9."""
    al = [a.strip().upper() for a in allowed if a.strip()]
    cs: List[Contract] = []
    for t in al:
        if t in ("OVER", "UNDER", "MATCH", "DIFFER"):
            for b in range(10):
                if valid_barrier(t, b):
                    cs.append((t, b))
        elif t in ("EVEN", "ODD"):
            cs.append((t, None))
    mask = np.zeros((len(cs), 10))
    for i, c in enumerate(cs):
        mask[i, win_set(c)] = 1.0
    return cs, mask, mask.sum(axis=1).astype(int)


def model_ratio(k: int, margin: float) -> float:
    return (1.0 - margin) * 10.0 / k


def edge(p: float, r: float) -> float:
    return p * r - 1.0


# ── stake sizing ─────────────────────────────────────────────────────────────
def cents_down(x: float) -> float:
    return math.floor(x * 100 + 1e-7) / 100.0


def kelly_fraction(p: float, r: float) -> float:
    """Full-Kelly fraction for a binary bet with net odds b=r-1: f* = (p r - 1)/(r - 1), 0 when no edge."""
    if r <= 1.0:
        return 0.0
    return max(0.0, (p * r - 1.0) / (r - 1.0))


def kelly_stake(balance: float, p_lb: float, r: float, frac: float, max_frac: float, min_stake: float,
                max_stake: Optional[float] = None) -> float:
    """Fractional Kelly stake on the LOWER-BOUND probability, clipped to [min_stake, max_frac*balance], rounded DOWN to cents.
    0.0 = do not bet (no edge or cannot afford the minimum)."""
    f = kelly_fraction(p_lb, r)
    if f <= 0.0 or balance < min_stake - EPS:
        return 0.0
    s = frac * f * balance
    cap = max_frac * balance
    if max_stake is not None:
        cap = min(cap, max_stake)
    s = min(max(s, min_stake), max(cap, min_stake))
    s = min(s, balance)
    s = cents_down(s)
    return s if s >= min_stake - EPS else 0.0


def bold_stake(balance: float, target: float, min_stake: float, max_stake: Optional[float] = None) -> float:
    """Dubins-Savage bold play: stake min(balance, target - balance), clipped to the stake limits, rounded down to cents."""
    s = min(balance, target - balance)
    if max_stake is not None:
        s = min(s, max_stake)
    s = cents_down(s)
    return s if s >= min_stake - EPS else 0.0


def bold_dp(p: float, r: float, n_units: int, min_units: int = 1, max_units: Optional[int] = None,
            iters: int = 2000) -> Tuple[np.ndarray, np.ndarray]:
    """Maximum probability of reaching `n_units` (the target, in stake units) from balance b = 0..n_units, betting x units
    (min_units <= x <= min(b, max_units)) on a contract that wins with prob p and pays net gain floor(x*(r-1)) units (floor, never round up: rounding up invents edge).
    Value iteration on V(b) (V(n)=1, V(b<min_units)=0).  Returns (V, best_x).  Pure; tested against Monte-Carlo and
    brute force.  best_x[b]=0 means 'cannot bet'."""
    V = np.zeros(n_units + 1)
    V[n_units] = 1.0
    best = np.zeros(n_units + 1, dtype=np.int64)
    mx = max_units if max_units is not None else n_units
    for _ in range(iters):
        Vn = V.copy()
        for b in range(min_units, n_units):
            top = min(b, mx)
            xs = np.arange(min_units, top + 1)
            if len(xs) == 0:
                continue
            up = np.minimum(b + np.floor(xs * (r - 1.0) + 1e-9).astype(np.int64), n_units)   # FLOOR: rounding up would create phantom edge on small bets
            val = p * V[up] + (1.0 - p) * V[b - xs]
            j = int(np.argmax(val))
            Vn[b] = val[j]
            best[b] = xs[j]
        if np.max(np.abs(Vn - V)) < 1e-12:
            V = Vn
            break
        V = Vn
    return V, best


@functools.lru_cache(maxsize=512)
def _bold_cached(p_r: float, r_r: float, nu: int) -> Tuple[np.ndarray, np.ndarray]:
    return bold_dp(p_r, r_r, nu)


def bold_value(p: float, r: float, balance: float, target: float, n_units: int = 40) -> float:
    """P(reach target) under the DP-optimal policy for a contract (p, r), balance on a coarse n_units grid (cached by rounded p, r)."""
    if target <= 0 or balance >= target:
        return 1.0
    V, _ = _bold_cached(round(p, 2), round(r, 3), n_units)
    return float(V[min(n_units, max(0, int(round(balance / target * n_units))))])


# ── probe budget (hard cap) ──────────────────────────────────────────────────
class ProbeBudget:
    """Hard cap on NET probe losses.  The stake of an unsettled probe counts as lost until it settles, so the realised net loss
    can never exceed `cap` (it can only be smaller): allowed(stake) <=> realised_loss + in_flight + stake <= cap."""

    def __init__(self, session_balance: float, frac: float, per_min: int, per_symbol_per_min: int):
        self.cap = max(0.0, float(session_balance) * float(frac))
        self.per_min = int(per_min)
        self.per_sym = int(per_symbol_per_min)
        self.realised_loss = 0.0          # sum(stake - payout) over settled probes (can be negative = net gain)
        self.in_flight = 0.0
        self.times: Deque[float] = deque()
        self.sym_times: Dict[str, Deque[float]] = {}
        self.n = 0

    def reset(self, session_balance: float, frac: float) -> None:
        self.cap = max(0.0, session_balance * frac)
        self.realised_loss = 0.0
        self.in_flight = 0.0

    def _trim(self, now: float) -> None:
        while self.times and now - self.times[0] >= 60.0:
            self.times.popleft()
        for q in self.sym_times.values():
            while q and now - q[0] >= 60.0:
                q.popleft()

    def left(self) -> float:
        return max(0.0, self.cap - max(self.realised_loss, 0.0) - self.in_flight)

    def allowed(self, stake: float, sym: str, now: float) -> bool:
        self._trim(now)
        if len(self.times) >= self.per_min or len(self.sym_times.get(sym, ())) >= self.per_sym:
            return False
        return max(self.realised_loss, 0.0) + self.in_flight + stake <= self.cap + EPS

    def open(self, stake: float, sym: str, now: float) -> None:
        self.in_flight += stake
        self.times.append(now)
        self.sym_times.setdefault(sym, deque()).append(now)
        self.n += 1

    def settle(self, stake: float, payout: float) -> None:
        self.in_flight = max(0.0, self.in_flight - stake)
        self.realised_loss += stake - payout


# ── cadence scheduler ────────────────────────────────────────────────────────
@dataclass
class SymView:
    """What the policy needs to know about one symbol at one scan."""
    sym: str
    tier: int
    menu: List[Contract]
    p_hat: np.ndarray            # honest mixture probability per menu contract
    p_cal: np.ndarray            # calibrated
    p_lb: np.ndarray             # calibrated lower bound
    ratio: np.ndarray            # payout/stake per contract (live / cached / model)


@dataclass
class Order:
    sym: str
    contract: Contract
    stake: float
    kind: str                    # "evidence" | "probe" | "bold"
    p_lb: float
    ratio: float
    edge: float


def decide(views: Sequence[SymView], mode: str, balance: float, min_stake: float, slots: int, probe: Optional[ProbeBudget],
           now: float, kelly_frac: float = 0.25, max_frac: float = 0.05, min_edge: float = 0.0,
           target: Optional[float] = None, max_stake: Optional[float] = None, bold_tier_switch: bool = True,
           min_ratio: float = 0.0) -> Tuple[List[Order], str]:
    """The single scheduler: maps (tier, probe budget left, target distance, open-contract cap) to orders + a one-line reason.
    mode: evidence | probe | bold.  Evidence orders (tier >= 2 and p_lb*r - 1 > min_edge) always come first, best edge first.
    In `probe` mode symbols at T0/T1 may get ONE minimum-stake probe on their best POSTERIOR edge (p_hat*r - 1), inside ProbeBudget.
    In `bold` mode a symbol without T2 evidence gets the bold stake on the contract with the best DP value."""
    orders: List[Order] = []
    why: List[str] = []
    if min_ratio > 0:                       # payout floor: drop every contract paying less than min_ratio x stake (e.g. Over 0 ~ 1.06x)
        import dataclasses
        nv = []
        for v in views:
            keep = [j for j in range(len(v.menu)) if v.ratio[j] >= min_ratio]
            if keep:
                nv.append(dataclasses.replace(v, menu=[v.menu[j] for j in keep], p_hat=v.p_hat[keep], p_cal=v.p_cal[keep], p_lb=v.p_lb[keep], ratio=v.ratio[keep]))
        views = nv
    if slots <= 0 or not views:
        return [], "no free slots"
    ev: List[Order] = []
    for v in views:
        if v.tier >= 2 and len(v.menu):
            e = v.p_lb * v.ratio - 1.0
            j = int(np.argmax(e))
            if e[j] > min_edge:
                s = kelly_stake(balance, float(v.p_lb[j]), float(v.ratio[j]), kelly_frac, max_frac, min_stake, max_stake)
                if s > 0:
                    ev.append(Order(v.sym, v.menu[j], s, "evidence", float(v.p_lb[j]), float(v.ratio[j]), float(e[j])))
    ev.sort(key=lambda o: -o.edge)
    orders.extend(ev[:slots])
    why.append(f"evidence={len(ev)}")
    have = {o.sym for o in orders}
    rest = slots - len(orders)
    if mode == "probe" and probe is not None and rest > 0:
        cand: List[Order] = []
        for v in views:
            if v.sym in have or v.tier >= 2 or not len(v.menu):
                continue
            e = v.p_hat * v.ratio - 1.0
            j = int(np.argmax(e))
            cand.append(Order(v.sym, v.menu[j], min_stake, "probe", float(v.p_lb[j]), float(v.ratio[j]), float(e[j])))
        cand.sort(key=lambda o: -o.edge)
        n_ok = 0
        for o in cand:
            if rest <= 0:
                break
            if probe.allowed(o.stake, o.sym, now):
                orders.append(o)
                probe.open(o.stake, o.sym, now)       # reserve immediately so a burst can never overshoot the cap
                rest -= 1
                n_ok += 1
        why.append(f"probe={n_ok}/{len(cand)} budget_left={probe.left():.2f}")
    elif mode == "bold" and target is not None and rest > 0:
        s = bold_stake(balance, target, min_stake, max_stake)
        n_b = 0
        if s > 0:
            best = None
            for v in views:
                if v.sym in have or not len(v.menu):
                    continue
                vals = [bold_value(float(v.p_hat[j]), float(v.ratio[j]), balance, target) for j in range(len(v.menu))]
                j = int(np.argmax(vals))
                if best is None or vals[j] > best[0]:
                    best = (vals[j], v, j)
            if best is not None and rest > 0:             # bold play stakes the whole distance: one position at a time
                _, v, j = best
                e = v.p_hat * v.ratio - 1.0
                orders.append(Order(v.sym, v.menu[j], s, "bold", float(v.p_lb[j]), float(v.ratio[j]), float(e[j])))
                rest -= 1
                n_b = 1
        why.append(f"bold stake={s:.2f} target={target:.2f} bal={balance:.2f} n={n_b}")
    return orders, " ".join(why) + f" mode={mode} slots={slots}"
