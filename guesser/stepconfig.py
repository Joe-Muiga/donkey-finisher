"""guesser.stepconfig - every STEP_* setting (env only; truthy rules identical to config._env_b)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Tuple


def _b(k: str, d) -> bool:
    return os.environ.get(k, str(d)).strip().lower() in ("1", "true", "yes", "on")


def _f(k: str, d) -> float:
    try:
        return float(os.environ.get(k, str(d)).strip())
    except ValueError:
        return float(d)


def _t(k: str, d: Tuple[float, ...]) -> Tuple[float, ...]:
    raw = os.environ.get(k, "").strip()
    try:
        v = tuple(sorted(float(x) for x in raw.replace(";", ",").split(",") if x.strip())) if raw else ()
    except ValueError:
        v = ()
    return v or tuple(d)


@dataclass
class StepConfig:
    mode: bool = False
    trade: bool = True
    vol_alpha: float = 0.02
    vol_edges: Tuple[float, ...] = (1.0, 2.0, 3.0, 4.5, 7.0)
    use_drift: bool = False
    decay_global: float = 0.9998
    decay_regime: float = 0.9995
    shrink: float = 50.0
    conf_edges: Tuple[float, ...] = (0.105, 0.115, 0.13, 0.15, 0.18, 0.23, 0.32)
    min_n: int = 400
    lb_z: float = 3.2
    edge_margin: float = 0.03
    half_n: int = 300
    kill_n: int = 3000
    assumed_margin: float = 0.05
    min_edge: float = 0.0
    kelly_frac: float = 0.10
    max_stake_frac: float = 0.01
    max_stake: float = 25.0
    horizon: str = "auto"
    rtt_frac: float = 0.6
    explore: bool = False
    explore_min_q: float = 0.13
    explore_max_per_min: int = 6
    explore_budget_frac: float = 0.01
    report_seconds: float = 300.0
    # extras (not in the brief's table; safe defaults)
    warmup: int = 50              # steps before a regime exists
    gap_factor: float = 3.0       # epoch gap > factor * typical interval = tick gap
    drift_alpha: float = 0.02     # EWMA rate of the signed step (STEP_USE_DRIFT)
    drift_band: float = 0.15      # flat drift state inside +-band * v
    z_hi: float = 1.64            # z of the upper bounds (parent test, kill rule)
    cross_seconds: float = 300.0  # cross-symbol diagnostic period (0 = off)
    cross_n: int = 2000           # signed steps kept per symbol for it

    @property
    def horizon_fixed(self) -> Optional[int]:
        return int(self.horizon) if self.horizon in ("1", "2") else None

    @classmethod
    def from_env(cls) -> "StepConfig":
        e = os.environ.get
        hz = e("STEP_HORIZON", "auto").strip().lower()
        d = cls()
        return cls(
            mode=_b("STEP_MODE", 0), trade=_b("STEP_TRADE", 1), vol_alpha=_f("STEP_VOL_ALPHA", d.vol_alpha),
            vol_edges=_t("STEP_VOL_EDGES", d.vol_edges), use_drift=_b("STEP_USE_DRIFT", 0),
            decay_global=_f("STEP_DECAY_GLOBAL", d.decay_global), decay_regime=_f("STEP_DECAY_REGIME", d.decay_regime),
            shrink=_f("STEP_SHRINK", d.shrink), conf_edges=_t("STEP_CONF_EDGES", d.conf_edges),
            min_n=int(_f("STEP_MIN_N", d.min_n)), lb_z=_f("STEP_LB_Z", d.lb_z), edge_margin=_f("STEP_EDGE_MARGIN", d.edge_margin),
            half_n=max(1, int(_f("STEP_HALF_N", d.half_n))), kill_n=int(_f("STEP_KILL_N", d.kill_n)),
            assumed_margin=_f("STEP_ASSUMED_MARGIN", d.assumed_margin), min_edge=_f("STEP_MIN_EDGE", d.min_edge),
            kelly_frac=_f("STEP_KELLY_FRAC", d.kelly_frac), max_stake_frac=_f("STEP_MAX_STAKE_FRAC", d.max_stake_frac),
            max_stake=_f("STEP_MAX_STAKE", d.max_stake), horizon=hz if hz in ("auto", "1", "2") else "auto",
            rtt_frac=_f("STEP_RTT_FRAC", d.rtt_frac), explore=_b("STEP_EXPLORE", 0),
            explore_min_q=_f("STEP_EXPLORE_MIN_Q", d.explore_min_q), explore_max_per_min=int(_f("STEP_EXPLORE_MAX_PER_MIN", d.explore_max_per_min)),
            explore_budget_frac=_f("STEP_EXPLORE_BUDGET_FRAC", d.explore_budget_frac),
            report_seconds=_f("STEP_REPORT_SECONDS", d.report_seconds), warmup=max(2, int(_f("STEP_WARMUP", d.warmup))),
            gap_factor=_f("STEP_GAP_FACTOR", d.gap_factor), drift_alpha=_f("STEP_DRIFT_ALPHA", d.drift_alpha),
            drift_band=_f("STEP_DRIFT_BAND", d.drift_band), z_hi=_f("STEP_Z_HI", d.z_hi),
            cross_seconds=_f("STEP_CROSS_SECONDS", d.cross_seconds), cross_n=int(_f("STEP_CROSS_N", d.cross_n)),
        )
