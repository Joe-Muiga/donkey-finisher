"""guesser.calibrate - prequential set-probability calibrator.

Why: the mixture's q_mix(S) is a posterior-mean probability.  Once we pick the BEST contract of ~40 on the BEST of ~20 symbols,
the winner's curse makes the picked p_hat optimistic.  So every prediction is stored BEFORE its digit arrives and scored after
(shadow scoring) for ALL menu contracts, and a trade uses only the lower bound of the calibrated probability.

Bins: key = (tier, |S|, bin of d = p_hat - |S|/10).  Edges are conventions (symmetric-ish, finer near 0 where the null lives).
Estimate: Beta posterior with m pseudo-observations centred on the bin's mean p_hat:
        a = m*pbar + hits,  b = m*(1-pbar) + (n-hits)      p_cal = a/(a+b)
        p_lb = p_cal - z*sqrt(p_cal(1-p_cal)/(a+b+1))       (normal approx of the lower Beta quantile, z = GUESSER_LB_Z = 1.64)
With few observations the estimate stays at the model's p_hat (trusting the Bayesian mixture, whose own prior is 0.9 null);
with many it converges to the empirical hit rate - so a mis-calibrated mixture is corrected by data, never by assumption.
m = 30 is a convention; E4 varies nothing here because under the null pbar ~ fair and the correction is zero either way.
"""
from __future__ import annotations

import math
from typing import Dict, Tuple

import numpy as np

EDGES = np.array([-0.02, -0.005, 0.005, 0.01, 0.02, 0.04, 0.08])      # 8 bins
NB = len(EDGES) + 1
PSEUDO = 30.0


class SetCalibrator:
    def __init__(self, z: float = 1.64, pseudo: float = PSEUDO):
        self.z = float(z)
        self.m = float(pseudo)
        self.n: Dict[int, np.ndarray] = {}      # (tier*16+k) -> arrays over bins
        self.hits: Dict[int, np.ndarray] = {}
        self.psum: Dict[int, np.ndarray] = {}

    @staticmethod
    def _key(tier: int, k: int) -> int:
        return tier * 16 + k

    def _arrs(self, key: int):
        if key not in self.n:
            self.n[key] = np.zeros(NB)
            self.hits[key] = np.zeros(NB)
            self.psum[key] = np.zeros(NB)
        return self.n[key], self.hits[key], self.psum[key]

    def record_many(self, tier: int, ks: np.ndarray, p_hat: np.ndarray, hit: np.ndarray) -> None:
        """Vectorised: one entry per menu contract (ks = set sizes)."""
        bins = np.digitize(p_hat - ks / 10.0, EDGES)
        for k in np.unique(ks):
            sel = ks == k
            n, h, ps = self._arrs(self._key(tier, int(k)))
            np.add.at(n, bins[sel], 1.0)
            np.add.at(h, bins[sel], hit[sel].astype(float))
            np.add.at(ps, bins[sel], p_hat[sel])

    def calibrate(self, tier: int, k: int, p_hat: float) -> Tuple[float, float, int]:
        """(p_cal, p_lb, n_in_bin).  Unseen bin -> p_hat itself with the pseudo-count-only lower bound."""
        b = int(np.digitize([p_hat - k / 10.0], EDGES)[0])
        key = self._key(tier, k)
        if key in self.n and self.n[key][b] > 0:
            n, h, ps = self.n[key][b], self.hits[key][b], self.psum[key][b]
            pbar = ps / n
        else:
            n, h, pbar = 0.0, 0.0, p_hat
        a = self.m * pbar + h
        bb = self.m * (1.0 - pbar) + (n - h)
        p = a / (a + bb)
        lb = p - self.z * math.sqrt(p * (1.0 - p) / (a + bb + 1.0))
        return float(p), float(max(lb, 0.0)), int(n)

    def curve(self, tier: int, k: int):
        """Reliability table for reports: [(bin, n, mean predicted, empirical)]."""
        key = self._key(tier, k)
        if key not in self.n:
            return []
        n, h, ps = self.n[key], self.hits[key], self.psum[key]
        return [(i, int(n[i]), float(ps[i] / n[i]), float(h[i] / n[i])) for i in range(NB) if n[i] > 0]
