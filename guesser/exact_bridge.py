"""guesser.exact_bridge - tier T3 (EXACT): wraps rng_core.exact (solvers, ExactMonitor) instead of re-implementing them.

Flow (reusing the V2 validation rule): the solver ladder is run on a segment OFF the event loop (child process via
rng_core.adapter.run_solver_async, or a thread in the harness); a fit must reproduce >= V held-out ticks (done inside the solvers,
V2_VALIDATE_N); the ExactMonitor then follows the live stream: first miss -> suspect (mass cut to 0.90), two misses in 50 ticks -> dropped.
While active the digit mass is capped at `mass` (V2_EXACT_MASS = 0.999), never 1.0 (a CSPRNG that merely LOOKED like a generator for
V ticks must not cost infinite log-loss).
"""
from __future__ import annotations

from typing import Optional, Sequence

import numpy as np


class ExactExpert:
    def __init__(self, mass: float = 0.999, validate_n: int = 24):
        self.mass = float(mass)
        self.V = int(validate_n)
        self.mon = None
        self.name = ""

    @property
    def active(self) -> bool:
        return self.mon is not None and not self.mon.dropped

    def request(self, digits: Sequence[int], epoch0: Optional[float], budget_s: float = 20.0, max_mb: int = 400) -> dict:
        return {"digits": [int(x) for x in digits], "epoch0": epoch0, "budget_s": budget_s, "max_mb": max_mb, "enable_brute": False,
                "try_mt": False, "progress": {}}

    def install(self, fit: dict, seg_digits: Sequence[int], later: Sequence[int] = ()) -> bool:
        """Start monitoring; the ticks that arrived while the solver ran (`later`) must ALL be hit or the fit is discarded."""
        from rng_core.exact.base import ExactMonitor
        mon = ExactMonitor(fit, [int(x) for x in seg_digits])
        for d in later:
            if mon.observe(int(d)) != "hit":
                return False
        self.mon, self.name = mon, fit.get("name", "?")
        return True

    def observe(self, d: int) -> str:
        return "none" if self.mon is None else self.mon.observe(int(d))

    def dist(self, h: int = 1) -> Optional[np.ndarray]:
        if not self.active:
            return None
        pr = self.mon.predict(h)
        if pr is None:
            return None
        mass = 0.90 if self.mon.suspect else self.mass
        p = np.empty(10)
        if pr[0] == "digit":
            p[:] = (1.0 - mass) / 9.0
            p[pr[1]] = mass
        else:
            allowed = list(pr[1])
            p[:] = (1.0 - mass) / max(1, 10 - len(allowed))
            for d in allowed:
                p[d] = mass / len(allowed)
        return p / p.sum()
