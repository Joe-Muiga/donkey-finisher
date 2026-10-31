"""guesser.stepmodel - step-size match model: StepModel (per symbol), shadow groups, the honest gate.  Pure: no I/O, no clock.

Last digit = (previous digit + price step) mod 10, so the wrapped h-step difference w = (N[t+h]-N[t]) mod 10 is learned per
volatility regime; the digit distribution is that histogram rotated by the current digit.  Prequential: every prediction is
stored before its label exists, scored when the label arrives, and only THEN does the label update the tables.
"""
from __future__ import annotations

import math
from bisect import bisect_right
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

import numpy as np

HS = (1, 2)
WARN = 30
_ROT = [np.array([(y - d) % 10 for y in range(10)]) for d in range(10)]     # q(y) = q_w[(y - D) % 10]


def wilson(k: float, n: float, z: float) -> Tuple[float, float]:
    if n <= 0:
        return 0.0, 1.0
    p = k / n
    z2 = z * z
    d = 1.0 + z2 / n
    c = (p + z2 / (2.0 * n)) / d
    w = z * math.sqrt(max(p * (1.0 - p) / n + z2 / (4.0 * n * n), 0.0)) / d
    return max(0.0, c - w), min(1.0, c + w)


def theory_top1(sigma: float) -> float:
    """Best single-digit hit rate of a rounded, wrapped zero-mean Gaussian step with RMS `sigma` (last-digit units)."""
    if sigma <= 1e-9:
        return 1.0
    f = lambda x: 0.5 * (1.0 + math.erf(x / (sigma * math.sqrt(2.0))))
    K = int(4.0 * sigma / 10.0) + 2
    return sum(f(10 * k + 0.5) - f(10 * k - 0.5) for k in range(-K, K + 1))


class Pred:
    __slots__ = ("h", "regime", "bin", "dstar", "qstar", "q", "ess", "D", "idx")


class Grp:
    """Shadow outcomes of one (h, regime, conf_bin) group."""
    __slots__ = ("n", "hits", "fn", "fh", "rec", "rh", "half", "black")

    def __init__(self, half: int):
        self.n = self.hits = self.fn = self.fh = self.rh = 0
        self.half = half
        self.rec: Deque[int] = deque(maxlen=half)
        self.black = False

    def add(self, hit: int) -> None:
        self.n += 1
        self.hits += hit
        if self.fn < self.half:
            self.fn += 1
            self.fh += hit
        if len(self.rec) == self.half:
            self.rh -= self.rec[0]
        self.rec.append(hit)
        self.rh += hit


class Agg:
    __slots__ = ("n", "hits")

    def __init__(self):
        self.n = self.hits = 0


class StepModel:
    def __init__(self, sym: str, dec: int, cfg):
        self.sym, self.dec, self.cfg = sym, int(dec), cfg
        self.nreg = (len(cfg.vol_edges) + 1) * (3 if cfg.use_drift else 1)
        self.G = {h: np.full(10, 0.5) for h in HS}
        self.gsum = {h: 5.0 for h in HS}
        self.C = {h: np.zeros((self.nreg, 10)) for h in HS}
        self.csum = {h: np.zeros(self.nreg) for h in HS}
        self.groups: Dict[Tuple[int, int, int], Grp] = {}
        self.agg_r: Dict[Tuple[int, int], Agg] = {}
        self.agg_h: Dict[int, Agg] = {h: Agg() for h in HS}
        self.pair_off: set = set()
        self.pend: Deque[tuple] = deque()
        self.events: List[Tuple[int, str]] = []           # (level, message); the caller logs and clears
        self.disabled = False
        self.idx = 0
        self.last_epoch: Optional[float] = None
        self._dt: Optional[float] = None
        self.occ = np.zeros(self.nreg, dtype=np.int64)
        self.sumsq = 0.0
        self.nsq = 0
        self.n_gaps = 0
        self.steps: Deque[Tuple[int, int]] = deque(maxlen=max(10, cfg.cross_n))
        self.cur: Dict[int, Optional[Pred]] = {1: None, 2: None}
        self._reset_hist()
        self.n_gaps = 0

    # ── history ──────────────────────────────────────────────────────────────
    def _reset_hist(self) -> None:
        self.last_N: Optional[int] = None
        self.v = 0.0
        self.m = 0.0
        self._nv = 0
        self.pend.clear()
        self.cur = {1: None, 2: None}

    def gap(self) -> None:
        """Tick gap: step history, volatility and drift restart; the tables survive."""
        self._reset_hist()
        self.n_gaps += 1

    # ── ingest ───────────────────────────────────────────────────────────────
    def on_tick(self, digit: int, quote: Optional[float], epoch: Optional[float]) -> None:
        if self.disabled:
            return
        c = self.cfg
        ep = None if epoch is None else float(epoch)
        if ep is not None:
            if self.last_epoch is not None:
                if ep <= self.last_epoch:
                    return                                   # duplicate / out-of-order
                dt = ep - self.last_epoch
                if self._dt is None:
                    self._dt = dt
                elif dt > c.gap_factor * self._dt:
                    self.gap()
                else:
                    self._dt += 0.05 * (dt - self._dt)
            self.last_epoch = ep
        if quote is None:
            self.gap()                                       # no price: the chain of consecutive steps is broken
            return
        N = int(round(float(quote) * 10 ** self.dec))
        D = N % 10
        if D != int(digit):
            self.disabled = True
            self.events.append((WARN, f"STEP {self.sym}: digit/decimals mismatch (price units give {D}, tick digit {int(digit)}, dec={self.dec}) - step model disabled"))
            return
        self.idx += 1
        i = self.idx
        pend = self.pend
        while pend and pend[0][0] <= i:                      # 1) score the stored predictions, THEN learn their labels
            p = pend.popleft()
            if p[0] == i:
                self._learn(p, D)
        if self.last_N is not None:                          # 2) regime state from ticks up to t
            s = N - self.last_N
            a = abs(s)
            self._nv += 1
            if self._nv <= c.warmup:
                self.v += (a - self.v) / self._nv
            else:
                self.v += c.vol_alpha * (a - self.v)
            self.m += c.drift_alpha * (s - self.m)
            self.sumsq += s * s
            self.nsq += 1
            self.steps.append((int(ep) if ep is not None else i, s))
        self.last_N = N
        if self._nv >= c.warmup:                             # 3) predictions for t+1, t+2 from data up to t
            r = self._regime()
            self.occ[r] += 1
            cur = {}
            for h in HS:
                P = self._predict(h, r, D, i)
                cur[h] = P
                pend.append((i + h, h, r, D, P.dstar, P.qstar, P.bin))
            self.cur = cur
        else:
            self.cur = {1: None, 2: None}

    def _regime(self) -> int:
        c = self.cfg
        b = bisect_right(c.vol_edges, self.v)
        if not c.use_drift:
            return b
        band = c.drift_band * self.v
        return b * 3 + (0 if self.m < -band else (2 if self.m > band else 1))

    def regime_name(self, r: int) -> str:
        return f"{r // 3}{'-=+'[r % 3]}" if self.cfg.use_drift else str(r)

    def _predict(self, h: int, r: int, D: int, i: int) -> Pred:
        c = self.cfg
        g = self.G[h] / self.gsum[h]
        cs = float(self.csum[h][r])
        qw = (self.C[h][r] + c.shrink * g) / (cs + c.shrink)
        qd = qw[_ROT[D]]
        ds = int(qd.argmax())
        P = Pred()
        P.h, P.regime, P.dstar, P.qstar, P.q, P.ess, P.D, P.idx = h, r, ds, float(qd[ds]), qd, cs, D, i
        P.bin = bisect_right(c.conf_edges, P.qstar)
        return P

    def _learn(self, p: tuple, Dt: int) -> None:
        _, h, r, D0, dstar, _q, b = p
        c = self.cfg
        hit = 1 if Dt == dstar else 0
        g = self.groups.get((h, r, b))
        if g is None:
            g = self.groups[(h, r, b)] = Grp(c.half_n)
        g.add(hit)
        pr = self.agg_r.get((h, r))
        if pr is None:
            pr = self.agg_r[(h, r)] = Agg()
        pr.n += 1
        pr.hits += hit
        ph = self.agg_h[h]
        ph.n += 1
        ph.hits += hit
        self._kill(h, r, b, g, ph)
        w = (Dt - D0) % 10                                   # scored above; only now does the label touch the tables
        G = self.G[h]
        G *= c.decay_global
        G[w] += 1.0
        self.gsum[h] = self.gsum[h] * c.decay_global + 1.0
        row = self.C[h][r]
        row *= c.decay_regime
        row[w] += 1.0
        self.csum[h][r] = self.csum[h][r] * c.decay_regime + 1.0

    def _kill(self, h: int, r: int, b: int, g: Grp, ph: Agg) -> None:
        c = self.cfg
        thr = 1.0 / (10.0 * (1.0 - c.assumed_margin))
        if not g.black and g.n >= c.kill_n and g.n % 25 == 0 and wilson(g.hits, g.n, c.z_hi)[1] < thr:
            g.black = True
            self.events.append((WARN, f"STEP BLACKLIST {self.sym} h{h} reg{self.regime_name(r)} bin{b}: n={g.n} hit={g.hits / g.n:.4f}, upper bound below {thr:.4f}"))
        if h not in self.pair_off and ph.n >= 3 * c.kill_n and ph.n % 100 == 0 and wilson(ph.hits, ph.n, c.z_hi)[1] < thr:
            self.pair_off.add(h)
            self.events.append((WARN, f"STEP PAIR OFF {self.sym} h{h}: n={ph.n} hit={ph.hits / ph.n:.4f}, upper bound below {thr:.4f} (shadow only from now)"))

    # ── readouts ─────────────────────────────────────────────────────────────
    def rms(self) -> float:
        return math.sqrt(self.sumsq / self.nsq) if self.nsq else 0.0

    def top_groups(self, h: int, k: int = 3, min_n: int = 30, z: float = 1.64) -> List[tuple]:
        """Best groups of horizon h by Wilson lower bound: (lb, regime, bin, n, hit_rate)."""
        out = [(wilson(g.hits, g.n, z)[0], r, b, g.n, g.hits / g.n) for (hh, r, b), g in self.groups.items() if hh == h and g.n >= min_n]
        out.sort(reverse=True)
        return out[:k]


def gate(m: StepModel, h: int, r: int, b: int, ratio: float, cfg) -> Tuple[bool, str, float]:
    """The honest gate for a real Match at payout `ratio`.  Returns (open, reason, wilson lower bound of the group)."""
    if h in m.pair_off:
        return False, "pair disabled", 0.0
    g = m.groups.get((h, r, b))
    if g is None or g.n == 0:
        return False, "no data", 0.0
    if g.black:
        return False, "blacklisted", 0.0
    if g.n < cfg.min_n:
        return False, f"n {g.n}<{cfg.min_n}", 0.0
    be = 1.0 / ratio
    need = be * (1.0 + cfg.edge_margin)
    lo = wilson(g.hits, g.n, cfg.lb_z)[0]
    if lo <= need:
        return False, f"lb {lo:.3f}<={need:.3f}", lo
    if g.fn < g.half or g.fh / g.fn <= be:
        return False, "first block <= break-even", lo
    if len(g.rec) < g.half or g.rh / len(g.rec) <= be:
        return False, "latest block <= break-even", lo
    par = m.agg_r.get((h, r))
    if par is not None and par.n > 0 and wilson(par.hits, par.n, cfg.z_hi)[1] < be:
        return False, "parent regime below break-even", lo
    return True, "open", lo


def explore_ok(m: StepModel, p: Pred, cfg) -> bool:
    g = m.groups.get((p.h, p.regime, p.bin))
    return (p.h not in m.pair_off and (g is None or (g.n < cfg.min_n and not g.black)) and p.qstar >= cfg.explore_min_q)


# ── cross-symbol diagnostic (report only) ────────────────────────────────────
def _corr(a: np.ndarray, b: np.ndarray, shift: int, min_n: int = 100) -> Optional[Tuple[float, int, float]]:
    """corr(a[e], b[e + shift]) over aligned epochs: (r, n, fisher z)."""
    _, ia, ib = np.intersect1d(a[:, 0], b[:, 0] - shift, return_indices=True)
    n = len(ia)
    if n < min_n:
        return None
    x, y = a[ia, 1].astype(float), b[ib, 1].astype(float)
    sx, sy = x.std(), y.std()
    if sx == 0 or sy == 0:
        return None
    r = float(((x - x.mean()) * (y - y.mean())).mean() / (sx * sy))
    return r, n, math.atanh(max(-0.999999, min(0.999999, r))) * math.sqrt(n - 3)


def cross_pairs(snap: Dict[str, np.ndarray], top: int = 5, lags: Tuple[int, ...] = (-2, -1, 0, 1, 2)) -> Tuple[int, List[dict]]:
    """snap: sym -> int array (n, 2) of (epoch, signed step).  Returns (pairs tested, top pairs by |r| at lag 0 with lag scan)."""
    syms = sorted(snap)
    res = []
    npairs = 0
    for i, sa in enumerate(syms):
        for sb in syms[i + 1:]:
            npairs += 1
            c0 = _corr(snap[sa], snap[sb], 0)
            if c0 is not None:
                res.append((abs(c0[0]), sa, sb, c0))
    res.sort(key=lambda x: -x[0])
    out = []
    for _, sa, sb, c0 in res[:top]:
        a = snap[sa]
        dt = max(1, int(round(float(np.median(np.diff(a[:, 0])))))) if len(a) > 2 else 1
        lg = {k: _corr(a, snap[sb], k * dt) for k in lags}
        out.append({"a": sa, "b": sb, "r": c0[0], "n": c0[1], "z": c0[2],
                    "lags": {k: (None if v is None else (round(v[0], 3), round(v[2], 2))) for k, v in lg.items()}})
    return npairs, out
