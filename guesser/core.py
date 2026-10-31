"""guesser.core - one symbol's guesser: Mixture + shadow scoring + calibrator feed + menu probabilities.  No I/O.

Shadow scoring: every published prediction is stored before its digit exists; when the target digit arrives we score
    top-1 / top-3 hit of the ranking distribution (chance 10% / 30%), log-loss gain ln(q_mix(y)/0.1) (>= 0 only with real structure),
and feed the set-probability calibrator with ALL menu contracts (so the calibrator sees what the policy may choose from).
"""
from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Deque, List, Optional, Sequence, Tuple

import numpy as np

from . import evidence as evd
from . import policy as pol
from .calibrate import SetCalibrator
from .exact_bridge import ExactExpert
from .mixture import MixConfig, Mixture

logger = logging.getLogger(__name__)


@dataclass
class GuesserConfig:
    h: int = 1
    dec: int = 2
    prior_null: float = 0.90
    share: float = 0.0
    alpha1: float = 0.01
    alpha2: float = 0.05
    lb_z: float = 1.64
    allowed: Tuple[str, ...] = ("OVER", "UNDER")
    margin: float = 0.05
    multiplicity: str = "bonferroni"     # bonferroni (anytime-valid) | ebh
    enabled: Optional[Tuple[str, ...]] = None   # expert names kept (None = all); used by the E4 ablations

    @classmethod
    def from_module(cls, m, dec: int = 2) -> "GuesserConfig":
        g = lambda k, d: getattr(m, k, d)
        al = tuple(x.strip().upper() for x in str(g("GUESSER_ALLOWED_CONTRACTS", "OVER,UNDER")).split(",") if x.strip())
        return cls(h=int(g("GUESSER_HORIZON", 1)), dec=dec, prior_null=float(g("GUESSER_PRIOR_NULL", 0.9)),
                   share=float(g("GUESSER_FIXED_SHARE", 0.0)), alpha1=float(g("GUESSER_ALPHA1", 0.01)),
                   alpha2=float(g("GUESSER_ALPHA2", 0.05)), lb_z=float(g("GUESSER_LB_Z", 1.64)), allowed=al,
                   margin=float(g("GUESSER_ASSUMED_MARGIN", 0.05)),
                   multiplicity=str(g("GUESSER_MULTIPLICITY", "bonferroni")).lower())


def wilson(k: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return 0.0, 1.0
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    w = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, c - w), min(1.0, c + w)


class SymbolGuesser:
    def __init__(self, sym: str, cfg: GuesserConfig, calibrator: Optional[SetCalibrator] = None):
        self.sym = sym
        self.cfg = cfg
        self.cal = calibrator or SetCalibrator(cfg.lb_z)
        self.mix = Mixture(MixConfig(h=cfg.h, dec=cfg.dec, prior_null=cfg.prior_null, share=cfg.share, enabled=cfg.enabled))
        self.menu, self.mask, self.ks = pol.build_menu(cfg.allowed)
        self.tier = evd.T0
        self.last_epoch: Optional[float] = None
        self.disabled = False
        self._pend: Deque[tuple] = deque()
        self.n_sh = self.top1 = self.top3 = 0
        self.ll_sum = 0.0
        self.exact = False
        self.xp = ExactExpert()                              # T3 expert (rng_core.exact), inactive until a fit validates
        self.tail: Deque[int] = deque(maxlen=2000)           # recent digits for the exact solvers
        self.tail_epoch0: Optional[float] = None
        self.tail_n = 0
        # duck-typing for bot_engine (it reads pr.p.horizon / pr.p.entry_delay_ticks / pr.selected)
        self.p = SimpleNamespace(horizon=cfg.h, entry_delay_ticks=cfg.h - 1)
        self.selected = ()

    # ── ingest ───────────────────────────────────────────────────────────────
    def on_tick(self, digit: int, quote: Optional[float] = None, epoch: Optional[float] = None) -> bool:
        if self.disabled:
            return False
        if epoch is not None and self.last_epoch is not None and epoch <= self.last_epoch:
            return False                                     # duplicate / out-of-order tick
        if epoch is not None:
            self.last_epoch = epoch
        try:
            self._ingest(int(digit), quote)
        except Exception:                                    # fail safe: this symbol stops, the engine continues
            self.disabled = True
            logger.error("GUESSER %s disabled after an exception", self.sym, exc_info=True)
            return False
        return True

    def _ingest(self, y: int, quote: Optional[float]) -> None:
        if self.xp.mon is not None:
            self.xp.observe(y)
        if not self.tail:
            self.tail_epoch0 = self.last_epoch
        self.tail.append(y)
        self.tail_n += 1
        i = self.mix.bank.n
        while self._pend and self._pend[0][0] <= i:
            tgt, ph, tr, top3, qm, tier = self._pend.popleft()
            if tgt == i:
                hit = self.mask[:, y]
                self.cal.record_many(tier, self.ks, ph, hit)
                self.n_sh += 1
                self.top1 += int(top3[0] == y)
                self.top3 += int(y in top3)
                self.ll_sum += math.log(qm[y]) + pol_ln10
        self.mix.on_digit(y, quote)
        qm, qr = self.mix.q_mix, self.mix.q_rank
        tier_now = self.tier
        if self.xp.active:                                   # T3: the validated exact model replaces the statistical one
            qx = self.xp.dist(self.cfg.h)
            if qx is not None:
                qm, qr, tier_now = qx, qx, evd.T3
        if qm is not None:
            ph = self.mask @ qm
            top3 = tuple(int(d) for d in np.argsort(-qr, kind="stable")[:3])
            self._pend.append((self.mix.bank.n - 1 + self.cfg.h, ph, None, top3, qm, tier_now))

    def fit(self, digits: Sequence[int], quotes: Optional[Sequence[float]] = None, epochs: Optional[Sequence[float]] = None) -> None:
        for j, y in enumerate(digits):
            self.on_tick(int(y), None if quotes is None else quotes[j], None if epochs is None else epochs[j])

    # ── readouts ─────────────────────────────────────────────────────────────
    @property
    def log_e(self) -> float:
        return self.mix.log_e

    def alpha_scale(self) -> float:
        return self.mix.alpha_run(1.0)

    def view(self, ratio_for, tier: Optional[int] = None) -> Optional[pol.SymView]:
        """ratio_for(contract) -> live/cached/model payout ratio.  None until the first prediction exists."""
        qm = self.mix.q_mix
        if qm is None or self.disabled:
            return None
        t = self.tier if tier is None else tier
        if self.xp.active:
            qx = self.xp.dist(self.cfg.h)
            if qx is not None:
                qm, t = qx, evd.T3
        ph = self.mask @ qm
        pc = np.empty(len(ph))
        lb = np.empty(len(ph))
        for j, k in enumerate(self.ks):
            pc[j], lb[j], _ = self.cal.calibrate(t, int(k), float(ph[j]))
        r = np.array([ratio_for(c) for c in self.menu])
        return pol.SymView(self.sym, t, self.menu, ph, pc, lb, r)

    def describe(self) -> str:
        return f"guesser h={self.cfg.h} ticks={self.mix.n} tier={evd.TIER_NAMES[self.tier]} log_bf={self.mix.log_e:.2f}"

    def stats(self) -> dict:
        n = self.n_sh
        t1 = wilson(self.top1, n)
        t3 = wilson(self.top3, n)
        name, w = self.mix.last_top
        return {"sym": self.sym, "ticks": self.mix.n, "tier": evd.TIER_NAMES[self.tier], "log_bf": round(self.mix.log_e, 3),
                "top_expert": name, "top_w": round(w, 4), "n": n,
                "top1": (round(self.top1 / n, 4) if n else None, [round(x, 4) for x in t1]),
                "top3": (round(self.top3 / n, 4) if n else None, [round(x, 4) for x in t3]),
                "ll_gain": (round(self.ll_sum / n, 6) if n else None), "runs": self.mix.runs}


pol_ln10 = math.log(10.0)
