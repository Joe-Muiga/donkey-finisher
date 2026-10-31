"""guesser.mixture - log-space Bayesian model averaging over guesser.experts, with an explicit uniform null.

    logw_m(t) = ln pi_m + sum_{s<=t} ln q_m(D_s | past)           (BMA; kept relative to its max, so no underflow)
    q_mix(d)  = sum_m w_m q_m(d)                                   (honest predictive, null INCLUDED)
    q_rank(d) = sum_{m != U} w_m q_m(d) / sum_{m != U} w_m          (ranking distribution, null removed: when U dominates,
                q_mix - 0.1 is below float resolution and ties would silently break to the lowest digits - see
                rng_core/stat_model.py, same reasoning)
    log BF(t) = sum_{s<=t} ln( q_mix_s(D_s) / 0.1 )                (e-process)

Because q_mix for target s is fixed before D_s exists and sum_y q_mix(y) = 1, E_H0[q_mix(D_s)/0.1 | past] = 1, so
exp(log BF) is a nonnegative martingale/supermartingale under H0 = iid uniform and Ville's inequality gives
P_H0(sup_t BF_t >= 1/alpha) <= alpha at ANY stopping time.  This holds for every horizon h >= 1 (the prediction for target s is
measurable w.r.t. digits <= s-h, a sub-sigma-field of digits <= s-1) and with the fixed-share layer (weights stay predictable).

Restarts (change points) are not free retries: run k is tested at alpha_k = alpha * 6 / (pi^2 k^2) (sum over k <= alpha), the
cumulative log BF history of earlier runs is kept in `history`, and the number of restarts is capped.
"""
from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, List, Optional, Sequence, Tuple

import numpy as np

from . import experts as ex

LN10 = math.log(10.0)
LN3 = math.log(3.0)


@dataclass
class MixConfig:
    h: int = 1
    dec: int = 2
    prior_null: float = 0.90        # GUESSER_PRIOR_NULL; spec: >= 0.90
    share: float = 0.0              # fixed-share switching rate (E4 decides; default OFF until the harness says otherwise)
    enabled: Optional[Sequence[str]] = None
    restart_drop: float = math.log(20.0)   # minimum evidence collapse (nats below the run's running max) that signals a change point
    restart_frac: float = 0.25             # ...and at least this fraction of the running max (a strongly structured stream has bad stretches)
    max_restarts: int = 8


class Mixture:
    def __init__(self, cfg: Optional[MixConfig] = None):
        self.cfg = cfg or MixConfig()
        self.prior = ex.default_prior(self.cfg.prior_null)
        self.logpi = np.log(self.prior)
        self.runs = 1                                   # restart counter (run k tested at alpha_k)
        self.history: List[float] = []                  # final log BF of closed runs (never discarded: accounting)
        self.events: List[str] = []
        self._reset_run()
        self.n = 0                                      # total digits seen (all runs)
        self.n_scored = 0
        self.last_top: Tuple[str, float] = ("U", 1.0)

    # ── state ────────────────────────────────────────────────────────────────
    def _reset_run(self) -> None:
        self.bank = ex.ExpertBank(self.cfg.h, self.cfg.dec, self.cfg.enabled)
        self.logw = self.logpi.copy()
        self.log_e = 0.0
        self.log_e_max = 0.0
        self.run_scored = 0
        self.n_run = 0
        self._pend: Deque[tuple] = deque()              # (target_idx, Q, ctx, qmix)
        self.q_mix: Optional[np.ndarray] = None
        self.q_rank: Optional[np.ndarray] = None

    def weights(self) -> np.ndarray:
        w = np.exp(self.logw - self.logw.max())
        return w / w.sum()

    def alpha_run(self, alpha_total: float) -> float:
        k = self.runs
        return alpha_total * 6.0 / (math.pi ** 2 * k * k)

    # ── online update ────────────────────────────────────────────────────────
    def on_digit(self, y: int, quote: Optional[float] = None) -> None:
        """Score the prediction whose target is this digit, learn from it, then publish the next prediction."""
        y = int(y)
        b = self.bank
        i = b.n                                         # index of this digit
        while self._pend and self._pend[0][0] <= i:
            tgt, Q, c, qm = self._pend.popleft()
            if tgt == i:
                lq = np.log(Q[:, y])
                self.log_e += math.log(qm[y]) + LN10      # ln(q/0.1)
                if self.log_e > self.log_e_max:
                    self.log_e_max = self.log_e
                self.logw += lq
                if self.cfg.share > 0.0:
                    w = np.exp(self.logw - self.logw.max())
                    w /= w.sum()
                    w = (1.0 - self.cfg.share) * w + self.cfg.share * self.prior
                    self.logw = np.log(w)
                else:
                    self.logw -= self.logw.max()
                b.learn(y, c)
                self.run_scored += 1
                self.n_scored += 1
        b.push(y, quote)
        self.n += 1
        self.n_run += 1
        self._publish()
        self._maybe_restart()

    def _publish(self) -> None:
        r = self.bank.predict()
        if r is None:
            return
        Q, c = r
        w = self.weights()
        qm = w @ Q
        wr = w.copy()
        wr[0] = 0.0
        s = wr.sum()
        self.q_rank = (wr @ Q) / s if s > 1e-300 else qm
        self.q_mix = qm
        self._pend.append((self.bank.n - 1 + self.cfg.h, Q, c, qm))
        j = int(np.argmax(wr))
        self.last_top = (ex.NAMES[j], float(w[j]))

    def _maybe_restart(self) -> None:
        """Change point: structure that earned >= ln 3 of evidence has since lost `restart_drop` nats -> it stopped working.
        Collapse must be >= max(ln 20, 25% of the running max): a clearly structured stream has bad stretches that are not change points.  Start a fresh run (counts and weights reset), spending a fresh alpha share; the old run's log BF stays in history."""
        if self.runs > self.cfg.max_restarts:
            return
        if self.log_e_max >= LN3 and self.log_e_max - self.log_e >= max(self.cfg.restart_drop, self.cfg.restart_frac * self.log_e_max):
            self.history.append(self.log_e)
            self.events.append(f"restart #{self.runs} at n={self.n}: log_e {self.log_e:.2f} fell {self.log_e_max - self.log_e:.2f} from max")
            self.runs += 1
            n_keep = self.n
            h_last = self.bank._d  # noqa: F841 (digits of the old run are intentionally NOT reused)
            self._reset_run()
            self.n = n_keep

    # ── bulk fit (warm start) ────────────────────────────────────────────────
    def fit(self, digits: Sequence[int], quotes: Optional[Sequence[float]] = None) -> None:
        """Warm start = replay the history through the SAME online code (so batch == online by construction; the independent
        check against a brute-force recomputation lives in test_guesser.py).  Call from a worker thread for long histories."""
        for j, y in enumerate(digits):
            self.on_digit(int(y), None if quotes is None else quotes[j])

    # ── gap handling ─────────────────────────────────────────────────────────
    def gap(self) -> None:
        """Ticks were missed: index-based features must not span the gap.  Pending predictions are dropped, the bank's history
        restarts (counts and weights are kept: they are still valid statistics of the generator)."""
        old = self.bank
        nb = ex.ExpertBank(self.cfg.h, self.cfg.dec, self.cfg.enabled)
        for name in ("cF", "cW", "T1", "T2", "T3", "TL", "cDL", "TP", "cm", "cn", "TR", "Tpar", "Thi", "TPa", "TPb"):
            setattr(nb, name, getattr(old, name))
        self.bank = nb
        self._pend.clear()
        self.q_mix = self.q_rank = None

    # ── readouts ─────────────────────────────────────────────────────────────
    def ranked_digits(self) -> Optional[np.ndarray]:
        if self.q_rank is None:
            return None
        return np.argsort(-self.q_rank, kind="stable")

    def cumulative_log_e(self) -> float:
        return float(sum(self.history) + self.log_e)
