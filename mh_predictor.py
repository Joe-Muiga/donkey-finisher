"""
mh_predictor.py - multi-horizon digit model (in-memory only, rebuilt from tick history each boot).

Per symbol and horizon h: predict D[t+h] from D[..t] with three models
  M1 diff   : decayed histogram of (d(j)-d(j-h)) mod 10, P(d) = pmf[(d-d_t) mod 10]
  M2 ctx    : decayed counts on d_t and (d_{t-1},d_t), Dirichlet back-off toward M1
  M3 legacy : k<=3 back-off (as set_predictor), horizon h, no decay
blended by exp(-eta * L_m), L_m = decayed log-loss, floored at 1e-4. Calibration = binned PAV per cover.
The batch walk-forward (numpy, no per-tick loop) and the online update are the same maths (see test_mh.py).
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from typing import Deque, Dict, List, Optional, Sequence, Tuple

import numpy as np

import config
import predictor as P

LN10 = math.log(10.0)
GATE = bool(int(getattr(config, "MH_GATE", 1)))   # False: skip the better-than-uniform checks
FLOOR = 1e-4
A1 = 5.0            # M1 shrinkage toward uniform
S2 = 20.0           # M2 back-off strength
A0, B0 = 50.0, 30.0  # legacy prior / back-off strengths (as set_predictor defaults)
RHO = 1.0 - 1.0 / 300.0   # model-loss decay (~300 scored predictions)
ETA = 1.0
ROLL = 300
WILSON_Z = float(getattr(config, "MH_WILSON_Z", 1.64))   # Wilson z of the per-bin lower bound (env MH_WILSON_Z)
logger = logging.getLogger(__name__)

COVERS: List[P.Candidate] = [("OVER", b) for b in range(0, 9)] + [("UNDER", b) for b in range(1, 10)]
W = np.array([[1.0 if P.wins_digit(c, d) else 0.0 for c in COVERS] for d in range(10)])   # (10, 18)
TRUE_P = W.sum(axis=0) / 10.0


# ── vectorised context counts ────────────────────────────────────────────────
def ctx_counts(ce: np.ndarray, ev_t: np.ndarray, y: np.ndarray, cq: np.ndarray, q_t: np.ndarray, mu: float) -> np.ndarray:
    """For each query q: sum over events e with ce[e]==cq[q] and ev_t[e]<=q_t[q] of mu**(q_t-ev_t) * onehot(y[e])."""
    nq = len(cq)
    if len(ce) == 0:
        return np.zeros((nq, 10))
    N = int(max(ev_t.max(), q_t.max())) + 2
    key = ce.astype(np.int64) * N + ev_t
    order = np.argsort(key, kind="stable")
    key = key[order]
    oh = np.zeros((len(ce), 10))
    oh[np.arange(len(ce)), y[order]] = 1.0 if mu >= 1.0 else mu ** (-ev_t[order].astype(np.float64))
    cum = np.vstack([np.zeros((1, 10)), np.cumsum(oh, axis=0)])
    start = np.searchsorted(key, cq.astype(np.int64) * N, side="left")
    end = np.searchsorted(key, cq.astype(np.int64) * N + q_t, side="right")
    out = cum[end] - cum[start]
    if mu < 1.0:
        out *= (mu ** q_t.astype(np.float64))[:, None]
    return np.maximum(out, 0.0)


def _final_counts(ctx: np.ndarray, y: np.ndarray, ev_t: np.ndarray, n_ctx: int, mu: float, t_end: int) -> np.ndarray:
    w = np.ones(len(ctx)) if mu >= 1.0 else mu ** (t_end - ev_t.astype(np.float64))
    return np.bincount(ctx * 10 + y, weights=w, minlength=n_ctx * 10).reshape(n_ctx, 10)


def _safe_log(p) -> np.ndarray:
    return np.log(np.maximum(p, 1e-9))


# ── calibration (binned PAV, numpy only) ─────────────────────────────────────
class Calibrator:
    """Monotone map raw p -> calibrated p with per-block sample count and Wilson lower bound."""

    def __init__(self, p: np.ndarray, y: np.ndarray, max_bins: int = 20, min_per_bin: int = 100, z: float = WILSON_Z):
        order = np.argsort(p, kind="stable")
        p, y = p[order], y[order]
        nb = max(1, min(max_bins, len(p) // min_per_bin))
        blocks: List[List[float]] = []          # [sum_y, count, hi]
        for idx in np.array_split(np.arange(len(p)), nb):
            if len(idx):
                blocks.append([float(y[idx].sum()), float(len(idx)), float(p[idx[-1]])])
        merged: List[List[float]] = []
        for b in blocks:                        # pool adjacent violators
            merged.append(b)
            while len(merged) > 1 and merged[-2][0] / merged[-2][1] > merged[-1][0] / merged[-1][1]:
                s, c, hi = merged.pop()
                merged[-1][0] += s
                merged[-1][1] += c
                merged[-1][2] = hi
        self.hi = np.array([m[2] for m in merged])
        self.n = np.array([m[1] for m in merged])
        self.p = np.array([m[0] / m[1] for m in merged])
        self.lb = np.array([P.wilson_lower(m[0], m[1], z) for m in merged])

    def __call__(self, p: float) -> Tuple[float, float]:
        k = min(int(np.searchsorted(self.hi, p, side="left")), len(self.hi) - 1)
        return float(self.p[k]), float(self.lb[k])

    # ── held-out check: same bins, NEWER slice that the bins were not fitted on ──
    def set_oos(self, p: np.ndarray, y: np.ndarray, z: float = WILSON_Z) -> None:
        k = np.minimum(np.searchsorted(self.hi, p, side="left"), len(self.hi) - 1)
        self.n_oos = np.bincount(k, minlength=len(self.hi)).astype(np.float64)
        self.hits_oos = np.bincount(k, weights=y, minlength=len(self.hi))
        self.wr_oos = np.where(self.n_oos > 0, self.hits_oos / np.maximum(self.n_oos, 1.0), np.nan)
        self.wr_oos_lb = np.array([P.wilson_lower(h_, n_, z) if n_ > 0 else 0.0 for h_, n_ in zip(self.hits_oos, self.n_oos)])

    def oos_at(self, p: float) -> Optional[Tuple[int, int, float, float]]:
        """(n_oos, hits_oos, wr_oos, wr_oos_lb) of the bin that `p` falls in; None when no held-out slice was stored."""
        if getattr(self, "n_oos", None) is None:
            return None
        k = min(int(np.searchsorted(self.hi, p, side="left")), len(self.hi) - 1)
        return int(self.n_oos[k]), int(self.hits_oos[k]), float(self.wr_oos[k]), float(self.wr_oos_lb[k])


def oos_admissible(cal, p: float, break_even: float, min_n: int, margin: float = 0.0) -> bool:
    """Held-out gate: the bin needs >= min_n held-out samples and a Wilson lower bound above break-even (+margin).
    A calibrator without a held-out slice (tests / fakes) passes."""
    fn = getattr(cal, "oos_at", None)
    if fn is None:
        return True
    r = fn(p)
    if r is None:
        return True
    n, _hits, _wr, lbo = r
    return n >= min_n and lbo > break_even + margin


def oos_admissible_mix(parts, break_even: float, min_n: int, margin: float = 0.0) -> bool:
    """Held-out gate for a lag-mixed cover. parts = [(weight, calibrator, raw_p)], weights sum to 1; each raw_p is looked up
    in ITS OWN horizon's calibrator (the bins were fitted on that horizon's raw P). Every part with a held-out slice needs
    >= min_n held-out samples; the weight-mixed held-out Wilson lower bound must beat break-even (+margin).
    One part -> identical to oos_admissible. Parts without a held-out slice (tests / fakes) are skipped."""
    got = []
    for w, cal, p in parts:
        fn = getattr(cal, "oos_at", None)
        r = fn(p) if fn is not None else None
        if r is None:
            continue
        n, _hits, _wr, lbo = r
        if n < min_n:
            return False
        got.append((w, lbo))
    if not got:
        return True
    tw = sum(w for w, _ in got)
    return sum(w * lbo for w, lbo in got) / tw > break_even + margin


# ── batch walk-forward for one horizon ───────────────────────────────────────
def walk_batch(D: np.ndarray, h: int, lam: float) -> dict:
    """All predictions for t=2..n-1 (target t+h) plus final online state. Pure numpy."""
    n = len(D)
    mu = lam ** 0.25                                  # context rows are sparser: slower decay
    j = np.arange(h + 2, n)                           # event (target) times
    t = np.arange(2, n)                               # query times
    a, b, c3, y = D[j - h], D[j - h - 1], D[j - h - 2], D[j]
    delta = (y - a) % 10
    qa, qb, qc = D[t], D[t - 1], D[t - 2]
    nq = len(t)
    # M1
    cnt1 = ctx_counts(np.zeros(len(j), np.int64), j, delta, np.zeros(nq, np.int64), t, lam)
    pm = (cnt1 + A1 * 0.1) / (cnt1.sum(1, keepdims=True) + A1)
    P1 = pm[np.arange(nq)[:, None], (np.arange(10)[None, :] - qa[:, None]) % 10]
    # M2
    r1 = ctx_counts(a, j, y, qa, t, mu)
    r2 = ctx_counts(b * 10 + a, j, y, qb * 10 + qa, t, mu)
    p2a = (r1 + S2 * P1) / (r1.sum(1, keepdims=True) + S2)
    P2 = (r2 + S2 * p2a) / (r2.sum(1, keepdims=True) + S2)
    # M3 legacy
    cum0 = np.cumsum(np.eye(10)[D], axis=0)[t]
    p3 = (cum0 + A0 * 0.1) / (cum0.sum(1, keepdims=True) + A0)
    for ce, cq in ((a, qa), (b * 10 + a, qb * 10 + qa), (c3 * 100 + b * 10 + a, qc * 100 + qb * 10 + qa)):
        row = ctx_counts(ce, j, y, cq, t, 1.0)
        p3 = (row + B0 * p3) / (row.sum(1, keepdims=True) + B0)
    PM = np.stack([P1, P2, p3])                       # (3, nq, 10)
    # model losses scored at time s = t+h (prediction at t, target D[t+h])
    ok = t + h <= n - 1
    s_idx = (t + h)[ok]
    tgt = D[s_idx]
    lossS = np.zeros((3, n))
    qi = np.nonzero(ok)[0]
    for m in range(3):
        lossS[m, s_idx] = -_safe_log(PM[m, qi, tgt])
    wgt = RHO ** (-np.arange(n, dtype=np.float64))
    Lcum = np.cumsum(lossS * wgt[None, :], axis=1) * (RHO ** np.arange(n, dtype=np.float64))[None, :]   # L_m(t)
    Lq = Lcum[:, t]                                   # (3, nq)
    wt = np.exp(-ETA * (Lq - Lq.min(axis=0, keepdims=True)))
    wt /= wt.sum(axis=0, keepdims=True)
    PB = np.einsum("mq,mqd->qd", wt, PM)
    PB = np.maximum(PB, FLOOR)
    PB /= PB.sum(axis=1, keepdims=True)
    blend_loss = np.full(nq, np.nan)
    blend_loss[qi] = -np.log(PB[qi, tgt])
    # final online state
    tend = n - 1
    st = dict(
        c1=_final_counts(np.zeros(len(j), np.int64), delta, j, 1, lam, tend)[0],
        r1=_final_counts(a, y, j, 10, mu, tend), r2=_final_counts(b * 10 + a, y, j, 100, mu, tend),
        l0=np.bincount(D, minlength=10).astype(np.float64),
        l1=_final_counts(a, y, j, 10, 1.0, tend), l2=_final_counts(b * 10 + a, y, j, 100, 1.0, tend),
        l3=_final_counts(c3 * 100 + b * 10 + a, y, j, 1000, 1.0, tend),
        L=Lcum[:, n - 1].copy())
    return dict(PM=PM, PB=PB, t=t, ok=qi, blend_loss=blend_loss, state=st)


class HState:
    """Online state of one (symbol, horizon); loaded from walk_batch, then updated tick by tick."""

    def __init__(self, h: int, lam: float):
        self.h, self.lam, self.mu = h, lam, lam ** 0.25
        self.c1 = np.zeros(10)
        self.r1, self.r2 = np.zeros((10, 10)), np.zeros((100, 10))
        self.l0 = np.zeros(10)
        self.l1, self.l2, self.l3 = np.zeros((10, 10)), np.zeros((100, 10)), np.zeros((1000, 10))
        self.L = np.zeros(3)
        self.pending: Deque[Tuple[int, np.ndarray, np.ndarray]] = deque()   # (target idx, PM(3,10), PB(10))
        self.cur: Optional[Tuple[np.ndarray, np.ndarray]] = None
        self.roll: Deque[float] = deque(maxlen=ROLL)
        self.cal: Optional[List[Calibrator]] = None       # one per cover
        self.val_ll: Optional[float] = None
        self.gate_gain: Optional[float] = None            # mean (ln10 - log-loss) on the out-of-sample slice
        self.gate_lb: Optional[float] = None              # its lower confidence bound; tradable needs > 0
        self.tradable = False
        self.n_scored = 0

    # shared single-t maths (mirrors walk_batch)
    def _dists(self, a: int, b: int, c: int) -> np.ndarray:
        pm = (self.c1 + A1 * 0.1) / (self.c1.sum() + A1)
        P1 = pm[(np.arange(10) - a) % 10]
        x = self.r1[a]
        p2a = (x + S2 * P1) / (x.sum() + S2)
        x = self.r2[b * 10 + a]
        P2 = (x + S2 * p2a) / (x.sum() + S2)
        p3 = (self.l0 + A0 * 0.1) / (self.l0.sum() + A0)
        for arr, i in ((self.l1, a), (self.l2, b * 10 + a), (self.l3, c * 100 + b * 10 + a)):
            x = arr[i]
            p3 = (x + B0 * p3) / (x.sum() + B0)
        return np.stack([P1, P2, p3])

    def load(self, res: dict, D: np.ndarray, n_total: int) -> None:
        for k, v in res["state"].items():
            setattr(self, k, v)
        PM, PB, t = res["PM"], res["PB"], res["t"]
        for q in np.nonzero(t + self.h > n_total - 1)[0]:             # predictions whose target is still ahead
            self.pending.append((int(t[q]) + self.h, PM[:, q, :].copy(), PB[q].copy()))
        self.cur = (PM[:, -1, :].copy(), PB[-1].copy())
        bl = res["blend_loss"]
        bl = bl[~np.isnan(bl)]
        self.roll.extend(float(x) for x in bl[-ROLL:])
        self.n_scored = len(bl)

    def update(self, buf: Sequence[int], j: int) -> None:
        """New digit buf[-1] at absolute index j (buf holds the last >=64 digits)."""
        h, d = self.h, buf[-1]
        self.l0[d] += 1.0
        self.L *= RHO
        if j >= h + 2:
            a, b, c = buf[-1 - h], buf[-2 - h], buf[-3 - h]
            self.c1 *= self.lam
            self.r1 *= self.mu
            self.r2 *= self.mu
            self.c1[(d - a) % 10] += 1.0
            self.r1[a, d] += 1.0
            self.r2[b * 10 + a, d] += 1.0
            self.l1[a, d] += 1.0
            self.l2[b * 10 + a, d] += 1.0
            self.l3[c * 100 + b * 10 + a, d] += 1.0
        while self.pending and self.pending[0][0] <= j:
            tg, pm, pb = self.pending.popleft()
            if tg == j:
                self.L += -_safe_log(pm[:, d])
                self.roll.append(float(-math.log(max(pb[d], 1e-9))))
                self.n_scored += 1
        if j >= 2:
            pm = self._dists(buf[-1], buf[-2], buf[-3])
            wt = np.exp(-ETA * (self.L - self.L.min()))
            wt /= wt.sum()
            pb = np.maximum(wt @ pm, FLOOR)
            pb /= pb.sum()
            self.pending.append((j + h, pm, pb))
            self.cur = (pm, pb)

    def roll_mean(self) -> Optional[float]:
        return sum(self.roll) / len(self.roll) if len(self.roll) >= 100 else None

    def muted(self) -> bool:
        m = self.roll_mean()
        return GATE and m is not None and m >= LN10


class MHSymbol:
    def __init__(self, symbol: str, hmax_trade: int = 5, hmax_paper: int = 10, decay: float = 0.998):
        self.symbol, self.hmax_trade, self.hmax_paper, self.decay = symbol, hmax_trade, hmax_paper, decay
        self.hs: Dict[int, HState] = {}
        self.buf: Deque[int] = deque(maxlen=64)
        self.n = 0                                     # digits seen; latest index = n-1
        self.last_epoch: Optional[float] = None

    async def fit(self, digits: Sequence[int], last_epoch: Optional[float], deadline: float = 0.0, offload: bool = False) -> dict:
        """Walk-forward over `digits`. Horizon 1 always; others only while time.time() < deadline."""
        D = np.asarray(list(digits), dtype=np.int64)
        n = len(D)
        self.hs.clear()
        self.n, self.last_epoch = n, last_epoch
        self.buf.clear()
        self.buf.extend(int(x) for x in D[-64:])
        rep = {"n": n, "h": {}}
        if n < 600:
            return rep
        for h in range(1, self.hmax_paper + 1):
            if h > 1 and deadline and time.time() > deadline:
                break
            hs = HState(h, self.decay)
            if offload:                                # background refit: numpy off the event loop (objects are not shared yet)
                res = await asyncio.to_thread(walk_batch, D, h, self.decay)
                await asyncio.to_thread(self._load_cal, hs, res, D, n)
            else:
                res = walk_batch(D, h, self.decay)
                self._load_cal(hs, res, D, n)
            self.hs[h] = hs
            rep["h"][h] = (round(hs.val_ll, 4) if hs.val_ll is not None else None, hs.tradable)
            await asyncio.sleep(0)                     # keep websockets alive between horizons
        return rep

    def _load_cal(self, hs: HState, res: dict, D: np.ndarray, n: int) -> None:
        hs.load(res, D, n)
        self._calibrate(hs, res, D)

    def _calibrate(self, hs: HState, res: dict, D: np.ndarray) -> None:
        """Validation slice = last min(3000, 30%) predictions, split in time: the OLDER 60% fits the per-cover
        calibrators, the NEWER 40% is the out-of-sample slice for the significance gate (log-loss improvement vs
        uniform, ln10 - loss, must have a lower CI bound > 0).  Calibration and gate no longer share data."""
        h, t, qi = hs.h, res["t"], res["ok"]
        if len(qi) < 400:
            return
        V = min(3000, int(0.3 * len(qi)))
        sel = qi[-V:]
        PB = res["PB"][sel]
        tgt = D[t[sel] + h]
        loss = -np.log(PB[np.arange(V), tgt])
        k = int(0.6 * V)
        pc, oc = PB[:k] @ W, W[tgt[:k]]
        hs.cal = [Calibrator(pc[:, c], oc[:, c]) for c in range(len(COVERS))]
        pco, oco = PB[k:] @ W, W[tgt[k:]]               # held-out slice (never used to fit the bins)
        for c in range(len(COVERS)):
            hs.cal[c].set_oos(pco[:, c], oco[:, c])
        if h <= self.hmax_trade:
            self._log_oos(hs)
        gain = LN10 - loss[k:]                         # >0 = better than uniform, out-of-sample
        m = float(gain.mean())
        se = float(gain.std(ddof=1) / math.sqrt(len(gain))) if len(gain) > 1 else float("inf")
        z = float(getattr(config, "TEST_GATE_Z", 2.58) if getattr(config, "TEST_MODE", False) else getattr(config, "MH_GATE_Z", 1.96))
        hs.val_ll = LN10 - m
        hs.gate_gain, hs.gate_lb = m, m - z * se
        hs.tradable = h <= self.hmax_trade and (hs.gate_lb > 0 or not GATE)

    def _log_oos(self, hs: HState) -> None:
        """Boot table: in-sample lb of the best admissible bin vs its held-out win rate / lower bound (optimism gap)."""
        r = float(getattr(config, "MH_MIN_PAYOUT_RATIO", 1.9))
        be, me_ = 1.0 / r, float(getattr(config, "MH_MIN_EDGE", 0.0))
        mn, mg = int(getattr(config, "MH_OOS_MIN_N", 30)), float(getattr(config, "MH_OOS_MARGIN", 0.0))
        parts, adm, rem = [], 0, 0
        for c, cand in enumerate(COVERS):
            cal = hs.cal[c]
            ok = [i for i in range(len(cal.hi)) if cal.lb[i] * r - 1.0 > me_]       # admissible in-sample
            if not ok:
                continue
            bad = [i for i in ok if not (cal.n_oos[i] >= mn and cal.wr_oos_lb[i] > be + mg)]
            adm += len(ok)
            rem += len(bad)
            i = ok[-1]                                                               # top admissible bin
            wr = cal.wr_oos[i]
            parts.append(f"{cand[0]}{cand[1]}:lb={cal.lb[i]:.3f} oos={'n/a' if wr != wr else f'{wr:.3f}'}/{cal.wr_oos_lb[i]:.3f} "
                         f"n={int(cal.n_oos[i])} rm={len(bad)}/{len(ok)}")
        logger.info("MH OOS %s h=%d (break-even %.3f): admissible bins %d, removed by OOS check %d | %s",
                    self.symbol, hs.h, be, adm, rem, "; ".join(parts) or "no admissible bin in-sample")

    def on_tick(self, digit: int, epoch: Optional[float] = None) -> bool:
        if epoch is not None:
            if self.last_epoch is not None and epoch <= self.last_epoch:
                return False
            self.last_epoch = epoch
        self.buf.append(int(digit))
        j = self.n
        self.n += 1
        if len(self.buf) >= 3:
            for hs in self.hs.values():
                hs.update(self.buf, j)
        return True

    def ready(self) -> bool:
        return bool(self.hs) and len(self.buf) >= 3

    def calibrated(self, h: int) -> Optional[Tuple[np.ndarray, np.ndarray]]:
        """(p_cal[18], p_lb[18]) for horizon h from the current blended distribution; None if unavailable."""
        hs = self.hs.get(h)
        if hs is None or hs.cal is None or hs.cur is None:
            return None
        pc = hs.cur[1] @ W
        pl = np.empty(len(COVERS))
        for c in range(len(COVERS)):
            pc[c], pl[c] = hs.cal[c](pc[c])
        return pc, pl

    def screen(self, h: int, q: float, min_edge: float, slack: float = 1.03) -> List[Tuple[int, float, float, float]]:
        """Covers that could pass admission: [(cover_idx, p_eff, lb_eff, est_edge_lb)] best first.
        P_eff = (1-q) P_h + q P_(h+1) (falls back to P_h when h+1 is not modelled)."""
        hs = self.hs.get(h)
        if hs is None or not hs.tradable or hs.muted():
            return []
        a = self.calibrated(h)
        if a is None:
            return []
        p, lb = a
        b = self.calibrated(h + 1)
        if b is not None and q > 0:
            p, lb = (1 - q) * p + q * b[0], (1 - q) * lb + q * b[1]
        out = []
        for c, cand in enumerate(COVERS):
            est = P.estimated_payout_ratio(cand) * slack
            e = lb[c] * est - 1.0
            if e > min_edge:
                out.append((c, float(p[c]), float(lb[c]), float(e)))
        out.sort(key=lambda x: -x[3])
        return out
