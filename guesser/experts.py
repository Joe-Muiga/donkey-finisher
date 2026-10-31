"""guesser.experts - the hypothesis classes of the mixture, all incremental and vectorised across experts.

Timeline.  The model keeps the digit history D[0..n-1].  After digit n-1 arrives it must publish, for every expert m, a
predictive distribution q_m over D[n-1+h] (the digit h ticks ahead).  When digit D[s] arrives it is the TARGET of the
prediction made at index s-h; each expert is scored (log q_m(D[s])) and then learns the pair (context at s-h -> D[s]).
Contexts are always built from digits with index <= s-h, so nothing from the future is ever used (tests check this).

Every table expert is "counts + beta * lower-order estimate" (hierarchical back-off / shrinkage, the same family as
rng_core.stat_model).  beta values are CONVENTIONS taken from stat_model (BETA_C=100, BETA_S=300), not re-tuned here;
E4 varies them.

Expert families (names are stable: the harness ablates by prefix)
    U        uniform null, q = 0.1
    F        KT/Dirichlet(1/2) marginal
    Fw200/1000/5000   exponentially-forgetting marginal, decay 1-1/W per tick (cheaper than a sliding window; same idea)
    M1..M3   Markov-k back-off chain over the last k digits (context-tree-weighting is NOT used: back-off KT is simpler,
             O(1) per tick and its regret is measured by E1/E2 rather than assumed)
    L0..L31  single-lag tables P(y | D[t-i])
    DL       lag-h difference  (y - D[t]) mod 10
    P2..P64  per-phase Dirichlet over (index mod p)
    C2..C64  periodic COPY: predict D[t+h-p] with a Beta-learned match probability (prior mean 0.1)
    R        repeat / run-length: P(y = D[t] | run length bucket)
    B        bit structure: parity given last 6 parities, then high bits given parity
    PI_a, PI_b   price-increment experts (need quotes): (y-D[t]) mod 10 given the signed last increment;
                 y given (D[t], sign of increment, decade-carry flag)
"""
from __future__ import annotations

import math
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

LOG10 = math.log(10.0)
BETA_M = 100.0       # back-off strength of Markov chain (convention, from rng_core.stat_model.BETA_C)
BETA_L = 300.0       # shrinkage of the single-lag tables toward F (convention, from stat_model.BETA_S)
BETA_X = 100.0       # shrinkage for the remaining small-context tables (R, PI, P_p phases) toward F
KAPPA_COPY = 2.0     # Beta prior strength of the copy experts, prior mean 0.1 (= null) so they start AT the null
NLAG = 32
PMAX = 64
WINDOWS = (200, 1000, 5000)
PAR_CTX = 6
RUN_B = 4            # run-length buckets 1,2,3,4+
INC_CLIP = 5         # signed increment clip for PI_a  -> 11 buckets

PERIODS = list(range(2, PMAX + 1))
_PH_OFF = np.concatenate([[0], np.cumsum(PERIODS)[:-1]]).astype(np.int64)     # row offset of each period's phase table
_PH_TOT = int(sum(PERIODS))
_PS = np.asarray(PERIODS, dtype=np.int64)
_LAGS = np.arange(NLAG)

NAMES: List[str] = (["U", "F"] + [f"Fw{w}" for w in WINDOWS] + ["M1", "M2", "M3"] + [f"L{i}" for i in range(NLAG)]
                    + ["DL"] + [f"P{p}" for p in PERIODS] + [f"C{p}" for p in PERIODS] + ["R", "B", "PI_a", "PI_b"])
E = len(NAMES)
IDX = {n: i for i, n in enumerate(NAMES)}


def family(name: str) -> str:
    """Family label used for ablations and the 'top expert' report."""
    if name in ("U", "F", "DL", "R", "B"):
        return name
    if name.startswith("Fw"):
        return "Fw"
    if name.startswith("PI"):
        return "PI"
    if name.startswith("M"):
        return "M"
    if name.startswith("L"):
        return "L"
    if name.startswith("P"):
        return "P"
    if name.startswith("C"):
        return "C"
    return name


def default_prior(prior_null: float = 0.90) -> np.ndarray:
    """Prior over experts.  U gets `prior_null`; the rest is split over FAMILIES (equal share per family, mild preference
    for simplicity because the simple families are few and so each member gets more) and within a family equally.
    Documented table (prior_null=0.9, 10 families -> 0.01 each): F, Fw, M, L, DL, P, C, R, B, PI each hold 1% of the
    mass; a single lag table L_i therefore has 0.0003, a single period P_p 0.00016, M1 0.0033, F 0.01."""
    fams: Dict[str, List[int]] = {}
    for i, n in enumerate(NAMES):
        if n != "U":
            fams.setdefault(family(n), []).append(i)
    pr = np.zeros(E)
    pr[0] = prior_null
    share = (1.0 - prior_null) / len(fams)
    for f, ix in fams.items():
        for i in ix:
            pr[i] = share / len(ix)
    return pr


class ExpertBank:
    """All experts of ONE stream at ONE horizon h.  predict() -> (E,10) matrix; update(y) learns the matured pair."""

    def __init__(self, h: int = 1, dec: int = 2, enabled: Optional[Sequence[str]] = None):
        self.h = int(h)
        self.dec = int(dec)
        self.n = 0                                           # digits seen
        self.hist = np.zeros(PMAX + 8 + self.h, dtype=np.int64)    # ring: hist[i % len] = D[i]
        self._hl = len(self.hist)
        self.cF = np.zeros(10)
        self.cW = np.zeros((len(WINDOWS), 10))
        self.decay = np.array([1.0 - 1.0 / w for w in WINDOWS])[:, None]
        self.T1 = np.zeros((10, 10))
        self.T2 = np.zeros((100, 10))
        self.T3 = np.zeros((1000, 10))
        self.TL = np.zeros((NLAG, 10, 10))
        self.cDL = np.zeros(10)
        self.TP = np.zeros((_PH_TOT, 10))
        self.cm = np.zeros(PMAX + 1)                         # copy matches per period
        self.cn = np.zeros(PMAX + 1)                         # copy trials per period
        self.TR = np.zeros((RUN_B, 2))
        self.Tpar = np.zeros((1 << PAR_CTX, 2))
        self.Thi = np.zeros((2, 5))
        self.TPa = np.zeros((2 * INC_CLIP + 1, 10))
        self.TPb = np.zeros((10 * 3 * 2, 10))
        # price state (for PI experts)
        self.v_prev: Optional[int] = None                     # integer price (quote*10^dec) of the newest tick
        self.inc_hist = np.zeros(self._hl, dtype=np.int64)    # signed increment at tick i (0 when unknown)
        self.carry_hist = np.zeros(self._hl, dtype=np.int64)
        self.have_price = np.zeros(self._hl, dtype=bool)
        self.run = 0
        self.run_hist = np.zeros(self._hl, dtype=np.int64)
        self.mask = np.ones(E, dtype=bool)
        if enabled is not None:
            en = set(enabled)
            self.mask = np.array([(n in en) or n == "U" for n in NAMES])
        self._pending: List[tuple] = []                       # predictions waiting for their digit: (target_idx, ctx dict)

    # ── helpers ──────────────────────────────────────────────────────────────
    def _d(self, i: int) -> int:
        return int(self.hist[i % self._hl])

    def _F(self) -> np.ndarray:
        return (self.cF + 0.5) / (self.cF.sum() + 5.0)

    def _contexts(self, t: int) -> dict:
        """Everything needed to predict D[t+h] and, later, to learn from it.  Only digits with index <= t are used."""
        d = self._d
        c = {"t": t, "d0": d(t)}
        c["k1"] = d(t) if t >= 0 else 0
        c["k2"] = d(t) * 10 + d(t - 1) if t >= 1 else -1
        c["k3"] = d(t) * 100 + d(t - 1) * 10 + d(t - 2) if t >= 2 else -1
        lags = np.zeros(NLAG, dtype=np.int64)
        ok = np.zeros(NLAG, dtype=bool)
        m = min(NLAG, t + 1)
        if m > 0:
            idx = (t - _LAGS[:m]) % self._hl
            lags[:m] = self.hist[idx]
            ok[:m] = True
        c["lags"], c["lag_ok"] = lags, ok
        tgt = t + self.h
        c["tgt"] = tgt
        c["ph"] = _PH_OFF + (tgt % _PS)
        # copy references D[tgt-p] = D[t+h-p], known when p >= h and index >= 0
        ref_i = tgt - _PS
        cok = (_PS >= self.h) & (ref_i >= 0)
        refd = np.zeros(len(_PS), dtype=np.int64)
        if cok.any():
            refd[cok] = self.hist[ref_i[cok] % self._hl]
        c["cref"], c["cok"] = refd, cok
        c["rb"] = min(max(int(self.run_hist[t % self._hl]), 1), RUN_B) - 1 if t >= 0 else 0
        pc = 0
        okp = t >= PAR_CTX - 1
        if okp:
            for i in range(PAR_CTX):
                pc |= (d(t - i) & 1) << i
        c["par"] = pc if okp else -1
        pi = t % self._hl
        hp = bool(self.have_price[pi]) and bool(self.have_price[(t - 0) % self._hl])
        c["has_inc"] = hp
        inc = int(self.inc_hist[pi]) if hp else 0
        c["inc"] = inc
        c["pia"] = max(-INC_CLIP, min(INC_CLIP, inc)) + INC_CLIP if hp else -1
        sgn = 0 if inc == 0 else (1 if inc > 0 else 2)
        c["pib"] = (d(t) * 3 + sgn) * 2 + int(self.carry_hist[pi]) if hp else -1
        return c

    # ── prediction ───────────────────────────────────────────────────────────
    def predict(self) -> Optional[Tuple[np.ndarray, dict]]:
        """(E,10) predictive matrix for D[n-1+h] and the context record to hand back to update(); None before 1st digit."""
        if self.n == 0:
            return None
        t = self.n - 1
        c = self._contexts(t)
        PF = self._F()
        Q = np.empty((E, 10))
        Q[0] = 0.1
        Q[1] = PF
        Q[2:5] = (self.cW + 0.5) / (self.cW.sum(axis=1, keepdims=True) + 5.0)
        # Markov back-off chain  F -> M1 -> M2 -> M3
        p = PF
        for k, (T, key) in enumerate(((self.T1, c["k1"]), (self.T2, c["k2"]), (self.T3, c["k3"]))):
            if key >= 0:
                row = T[key]
                p = (row + BETA_M * p) / (row.sum() + BETA_M)
            Q[5 + k] = p
        # single lags
        rows = self.TL[_LAGS, c["lags"]]                      # (32,10)
        QL = (rows + BETA_L * PF) / (rows.sum(axis=1, keepdims=True) + BETA_L)
        QL[~c["lag_ok"]] = PF
        Q[8:8 + NLAG] = QL
        j = 8 + NLAG
        # lag-h difference: P(y) = pm[(y - D[t]) % 10]
        pm = (self.cDL + 0.5) / (self.cDL.sum() + 5.0)
        Q[j] = pm[(np.arange(10) - c["d0"]) % 10]
        j += 1
        # periodic phase tables
        rp = self.TP[c["ph"]]
        Q[j:j + len(PERIODS)] = (rp + BETA_X * PF) / (rp.sum(axis=1, keepdims=True) + BETA_X)
        j += len(PERIODS)
        # periodic copy: q(ref)=a, others (1-a)/9, a = (m + 0.1k)/(n + k)
        a = (self.cm[_PS] + 0.1 * KAPPA_COPY) / (self.cn[_PS] + KAPPA_COPY)
        QC = np.repeat(((1.0 - a) / 9.0)[:, None], 10, axis=1)
        ok = c["cok"]
        QC[np.arange(len(_PS))[ok], c["cref"][ok]] = a[ok]
        QC[~ok] = 0.1
        Q[j:j + len(PERIODS)] = QC
        j += len(PERIODS)
        # repeat / run
        rb = c["rb"]
        ar = (self.TR[rb, 1] + 0.1 * KAPPA_COPY) / (self.TR[rb].sum() + KAPPA_COPY)
        qr = np.full(10, (1.0 - ar) / 9.0)
        qr[c["d0"]] = ar
        Q[j] = qr
        j += 1
        # bits: P(par | last parities) * P(hi | par)
        if c["par"] >= 0:
            pr_ = (self.Tpar[c["par"]] + 0.5) / (self.Tpar[c["par"]].sum() + 1.0)
        else:
            pr_ = np.array([0.5, 0.5])
        hi = (self.Thi + 0.5) / (self.Thi.sum(axis=1, keepdims=True) + 2.5)      # (2,5)
        qb = np.empty(10)
        qb[0::2] = pr_[0] * hi[0]
        qb[1::2] = pr_[1] * hi[1]
        Q[j] = qb
        j += 1
        # price experts
        if c["pia"] >= 0:
            r = self.TPa[c["pia"]]
            dd = (r + BETA_X * ((self.cDL + 0.5) / (self.cDL.sum() + 5.0))) / (r.sum() + BETA_X)
            Q[j] = dd[(np.arange(10) - c["d0"]) % 10]
            r2 = self.TPb[c["pib"]]
            Q[j + 1] = (r2 + BETA_X * PF) / (r2.sum() + BETA_X)
        else:
            Q[j] = 0.1
            Q[j + 1] = 0.1
        Q[~self.mask] = 0.1                                   # disabled experts = the null (they never gain or lose)
        return Q, c

    # ── learning ─────────────────────────────────────────────────────────────
    def push(self, digit: int, quote: Optional[float] = None) -> None:
        """Register digit D[n] in the history (price/run state included).  Call BEFORE learn() for the matured pair."""
        i = self.n
        d = int(digit)
        self.hist[i % self._hl] = d
        self.run = self.run + 1 if (i > 0 and d == self._d(i - 1)) else 1
        self.run_hist[i % self._hl] = self.run
        if quote is not None and not (isinstance(quote, float) and quote != quote):
            v = int(round(float(quote) * (10 ** self.dec)))
            if self.v_prev is not None:
                self.inc_hist[i % self._hl] = v - self.v_prev
                self.carry_hist[i % self._hl] = int((v // 10) != (self.v_prev // 10))
                self.have_price[i % self._hl] = True
            else:
                self.have_price[i % self._hl] = False
            self.v_prev = v
        else:
            self.have_price[i % self._hl] = False
            self.v_prev = None
        self.n += 1

    def learn(self, y: int, c: dict) -> None:
        """Learn the pair (context c at index t -> y = D[t+h]).  Order of count updates is irrelevant to the maths because
        scoring used the PRE-update tables (predict() was computed before y existed)."""
        y = int(y)
        self.cF[y] += 1.0
        self.cW *= self.decay
        self.cW[:, y] += 1.0
        if c["k1"] >= 0:
            self.T1[c["k1"], y] += 1.0
        if c["k2"] >= 0:
            self.T2[c["k2"], y] += 1.0
        if c["k3"] >= 0:
            self.T3[c["k3"], y] += 1.0
        m = c["lag_ok"]
        self.TL[_LAGS[m], c["lags"][m], y] += 1.0
        self.cDL[(y - c["d0"]) % 10] += 1.0
        self.TP[c["ph"], y] += 1.0
        ok = c["cok"]
        ps = _PS[ok]
        self.cn[ps] += 1.0
        self.cm[ps] += (c["cref"][ok] == y)
        rb = c["rb"]
        self.TR[rb, int(y == c["d0"])] += 1.0
        if c["par"] >= 0:
            self.Tpar[c["par"], y & 1] += 1.0
        self.Thi[y & 1, y >> 1] += 1.0
        if c["pia"] >= 0:
            self.TPa[c["pia"], (y - c["d0"]) % 10] += 1.0
            self.TPb[c["pib"], y] += 1.0
