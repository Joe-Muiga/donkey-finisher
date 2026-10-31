"""guesser.step_engine - STEP-SIZE MATCH engine (STEP_MODE=1).  Subclass of GuesserEngine: demo-only guard, money flow,
buy/settle path and warm start are reused; only the step model, the step scan and the step reporting are new."""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np

import config
import predictor as pred
import set_predictor as sp
from bot_engine import _cfg

from . import policy as pol
from .core import GuesserConfig, SymbolGuesser, wilson
from .engine import GuesserEngine
from .stepconfig import StepConfig
from .stepmodel import StepModel, cross_pairs, explore_ok, gate, theory_top1

logger = logging.getLogger(__name__)
pred.CONTRACT_TYPE.setdefault("MATCH", "DIGITMATCH")      # predictor.py has no MATCH entry; _contract_params needs it


@dataclass
class StepOrder(pol.Order):
    h: int = 1
    regime: int = 0
    bin: int = 0
    q: float = 0.0
    p_sh: float = 0.1            # shadow hit rate of the group at order time
    key: tuple = ()              # (symbol, tick epoch) this order was decided on


class StepSymbolGuesser(SymbolGuesser):
    def __init__(self, sym: str, cfg: GuesserConfig, calibrator, scfg: StepConfig):
        super().__init__(sym, cfg, calibrator)
        self.step = StepModel(sym, cfg.dec, scfg)

    def on_tick(self, digit: int, quote: Optional[float] = None, epoch: Optional[float] = None) -> bool:
        ok = super().on_tick(digit, quote, epoch)
        m = self.step
        if ok and not m.disabled:
            try:
                m.on_tick(digit, quote, epoch)
            except Exception:                                # only this symbol's step model stops
                m.disabled = True
                logger.error("STEP %s model disabled after an exception", self.sym, exc_info=True)
            if m.events:
                for lvl, msg in m.events:
                    logger.log(lvl, msg)
                m.events.clear()
        return ok


class StepGuesserEngine(GuesserEngine):
    def __init__(self):
        super().__init__()
        self.scfg = StepConfig.from_env()
        if self.cadence == "step" and not self.scfg.mode:
            logger.critical("GUESSER_CADENCE=step needs STEP_MODE=1: running evidence cadence (never probe)")
            self.cadence = "evidence"
        self.step_probe: Optional[pol.ProbeBudget] = None
        self._rtt: Dict[str, Dict[str, deque]] = {}
        self._h_cache: Dict[str, tuple] = {}
        self._h_used: Dict[str, int] = {}
        self._gates_open: set = set()
        self._step_real: Dict[tuple, list] = {}              # (sym, h, regime, bin) -> [n, wins, pnl]  (realised, separate from shadow)
        self._step_sym: Dict[str, list] = {}                 # sym -> [n, wins, pnl, break-even sum]
        self._step_recent: deque = deque(maxlen=200)         # (win, shadow rate) of the last gated trades
        self._mm_at = 0
        self._step_n = 0

    async def run(self):
        logger.info("STEP MODE: cadence=%s trade=%s horizon=%s explore=%s min_n=%d lb_z=%.2f kelly=%.2f max_stake=%.2f",
                    self.cadence, self.scfg.trade, self.scfg.horizon, self.scfg.explore, self.scfg.min_n, self.scfg.lb_z,
                    self.scfg.kelly_frac, self.scfg.max_stake)
        await super().run()

    def _extra_tasks(self) -> list:
        return super()._extra_tasks() + [self._step_report_loop(), self._step_cross_loop()]

    # ── warm start: same logic as GuesserEngine._fit_symbol, step guesser + gap reset ─────────────────────
    async def _fit_symbol(self, sym: str, initial: bool = False) -> bool:
        g = self.predictors.get(sym)
        if g is not None and not initial and not self._needs_resync():
            return True
        n = int(_cfg("GUESSER_HISTORY_TICKS", 5000)) if g is None else 1000
        ticks = await self.client.get_tick_history(sym, n)
        if len(ticks) < 50 and g is None:
            logger.warning("GUESSER WARM-UP %s: only %d ticks returned", sym, len(ticks))
        if sym not in self._decimals:
            dec_map = _cfg("DIGIT_DECIMALS", {}) or {}
            dec = dec_map.get(sym)
            if dec is None:
                dec = pred.infer_decimals([t["quote"] for t in ticks]) if ticks else 2
            self._decimals[sym] = int(dec)
        dec = self._decimals[sym]
        self._ratios.setdefault(sym, {})
        fresh = g is None
        if g is None:
            cfg = GuesserConfig.from_module(config, dec=dec)
            g = StepSymbolGuesser(sym, cfg, self.cal, self.scfg)
            digits = pred.digits_from_quotes([t["quote"] for t in ticks], dec)
            await asyncio.to_thread(g.fit, [int(d) for d in digits], [t["quote"] for t in ticks], [t["epoch"] for t in ticks])
        else:
            new = [t for t in ticks if g.last_epoch is None or t["epoch"] > g.last_epoch]
            if new and g.last_epoch is not None and ticks and ticks[0]["epoch"] > g.last_epoch:
                g.mix.gap()
                g.step.gap()
                logger.warning("GUESSER %s: tick gap after reconnect, history restarted (counts kept)", sym)
            for t in new:
                g.on_tick(pred.last_digit(t["quote"], dec), t["quote"], t["epoch"])
        last_ep = ticks[-1]["epoch"] if ticks else None
        self._recent_epochs[sym] = [t["epoch"] for t in ticks[-50:]]
        if not self._ratios.get(sym):
            await self._probe_one(sym)
        if last_ep is not None:
            for ep, d in list(self._live.get(sym, ())):
                if ep > last_ep and g.on_tick(d, None, ep):
                    self._recent_epochs[sym].append(ep)
        self.predictors[sym] = g
        logger.info("GUESSER WARM-UP %s: %d ticks fitted, %s", sym, g.mix.n, g.describe())
        if fresh:
            self._warm_log(sym, g, len(ticks))
        return True

    def _warm_log(self, sym: str, g: StepSymbolGuesser, n_ticks: int) -> None:
        m = g.step
        if m.disabled:
            logger.warning("STEP WARM-UP %s: step model disabled", sym)
            return
        sig = m.rms()
        a = m.agg_h[1]
        occ = m.occ.sum()
        occs = " ".join(f"r{m.regime_name(r)}={100.0 * int(c) / occ:.0f}%" for r, c in enumerate(m.occ) if c) if occ else "n/a"
        obs = f"{a.hits / a.n:.3f} (n={a.n})" if a.n else "n/a"
        logger.info("STEP WARM-UP %s: dec=%d ticks=%d sigma(RMS step, last-digit units)=%.2f regimes[%s] theory top-1 %.3f vs observed shadow top-1 h1 %s",
                    sym, m.dec, n_ticks, sig, occs, theory_top1(sig), obs)

    # ── latency tracking (thin timing wrappers) ───────────────────────────────────────────────────────────
    def _rtt_note(self, sym: str, kind: str, dt: float) -> None:
        self._rtt.setdefault(sym, {"prop": deque(maxlen=200), "buy": deque(maxlen=200)})[kind].append(dt)

    async def _propose(self, sym: str, cand, stake: float, duration: Optional[int] = None):
        t0 = time.monotonic()
        r = await super()._propose(sym, cand, stake, duration)
        if r:
            self._rtt_note(sym, "prop", time.monotonic() - t0)
        return r

    def _horizon_for(self, sym: str) -> int:
        """h=1 if p95(proposal)+p95(buy) < STEP_RTT_FRAC * median tick gap, else 2 (STEP_HORIZON=1|2 forces it)."""
        fx = self.scfg.horizon_fixed
        if fx:
            return fx
        now = time.monotonic()
        c = self._h_cache.get(sym)
        if c and now - c[0] < 5.0:
            return c[1]
        d = self._rtt.get(sym)
        h = 2
        if d and len(d["prop"]) >= 5:
            p95p = float(np.percentile(d["prop"], 95))
            p95b = float(np.percentile(d["buy"], 95)) if len(d["buy"]) >= 5 else p95p      # no buy yet: assume a buy costs one proposal
            h = 1 if (p95p + p95b) < self.scfg.rtt_frac * self._tick_interval(sym) else 2
        self._h_cache[sym] = (now, h)
        return h

    def _rtt_text(self, sym: str) -> str:
        d = self._rtt.get(sym)
        if not d or not d["prop"]:
            return "rtt n/a"
        f = lambda q: (f"{np.percentile(q, 50):.2f}/{np.percentile(q, 95):.2f}" if len(q) else "-")
        return f"rtt prop p50/p95 {f(d['prop'])}s buy {f(d['buy'])}s gap {self._tick_interval(sym):.1f}s"

    # ── the scan ──────────────────────────────────────────────────────────────────────────────────────────
    async def _scan_once(self):
        if self.cadence != "step":
            return await super()._scan_once()                # legacy scan; the step model only shadow-scores
        self._g_scans += 1
        sc = self.scfg
        now = time.time()
        bal = float(self.client.balance or 0.0)
        if self.start_balance is None and bal > 0:
            self.start_balance = bal
            self.step_probe = pol.ProbeBudget(bal, sc.explore_budget_frac, sc.explore_max_per_min, max(1, sc.explore_max_per_min // 3))
            logger.info("STEP session start balance %.2f, explore budget %.2f", bal, self.step_probe.cap)
        if not sc.trade:
            self._last_decision = "step shadow only (STEP_TRADE=0)"
            return
        slots = int(_cfg("GUESSER_MAX_OPEN", 6)) - len(self._open) - self._inflight
        if slots <= 0 or self.start_balance is None:
            return
        if self._next_stake() is None:
            self._last_decision = "stopped by money rules"
            return
        min_stake = self.stake
        min_ratio = float(_cfg("GUESSER_MIN_PAYOUT_RATIO", 2.0))
        cands: List[StepOrder] = []
        n_ready = 0
        for sym, g in self.predictors.items():
            m = getattr(g, "step", None)
            if m is None or m.disabled or g.disabled:
                continue
            key = (sym, g.last_epoch if g.last_epoch is not None else g.mix.n)
            if key in self._used or now - self._last_tick_wall.get(sym, 0) > self._max_age(sym):
                continue
            h = self._horizon_for(sym)
            self._h_used[sym] = h
            p = m.cur.get(h)
            if p is None:
                continue
            n_ready += 1
            c = ("MATCH", p.dstar)
            ratio = self._ratio_fn(sym)(c)
            ok, _why, lb = gate(m, h, p.regime, p.bin, ratio, sc)
            grp = m.groups.get((h, p.regime, p.bin))
            p_sh = grp.hits / grp.n if grp is not None and grp.n else 0.1
            gk = (sym, h, p.regime, p.bin)
            if ok:
                if gk not in self._gates_open:
                    self._gates_open.add(gk)
                    logger.warning("STEP GATE OPEN %s h%d reg%s bin%d: n=%d hit=%.3f lb=%.3f vs break-even %.3f", sym, h,
                                   m.regime_name(p.regime), p.bin, grp.n, p_sh, lb, 1.0 / ratio)
                stake = pol.kelly_stake(bal, lb, ratio, sc.kelly_frac, sc.max_stake_frac, min_stake, sc.max_stake)
                if stake > 0:
                    cands.append(StepOrder(sym, c, stake, "step", lb, ratio, lb * ratio - 1.0, h, p.regime, p.bin, p.qstar, p_sh, key))
                continue
            self._gates_open.discard(gk)
            if sc.explore and bal >= min_stake and explore_ok(m, p, sc):
                lb0 = wilson(grp.hits, grp.n, sc.lb_z)[0] if grp is not None and grp.n else 0.0
                cands.append(StepOrder(sym, c, min_stake, "step_explore", lb0, ratio, lb0 * ratio - 1.0, h, p.regime, p.bin, p.qstar, p_sh, key))
        cands.sort(key=lambda o: -o.edge)
        picks: List[StepOrder] = []
        for o in cands:
            if len(picks) >= slots:
                break
            if o.kind == "step_explore":
                if self.step_probe is None or not self.step_probe.allowed(o.stake, o.sym, now):
                    continue
                self.step_probe.open(o.stake, o.sym, now)    # reserve at once: a burst can never overshoot the cap
            picks.append(o)
        msg = f"STEP SCAN {self._g_scans}: ready={n_ready} gated={sum(1 for o in cands if o.kind == 'step')} explore={sum(1 for o in cands if o.kind == 'step_explore')} bal={bal:.2f} -> {len(picks)} order(s)"
        if picks or now - self._last_scan_log > 60:
            logger.info(msg)
            self._last_scan_log = now
        self._last_decision = msg[:160]
        if not picks:
            return
        props = await asyncio.gather(*(self._propose(o.sym, o.contract, o.stake) for o in picks), return_exceptions=True)
        go = []
        for o, prop in zip(picks, props):
            ok = not isinstance(prop, BaseException) and bool(prop) and float(prop.get("payout") or 0) > 0
            ratio = float(prop["payout"]) / o.stake if ok else 0.0
            if ok:
                for d in range(10):                          # a Match pays the same on every digit: refresh the whole cache
                    self._ratios.setdefault(o.sym, {})[("MATCH", d)] = ratio
                if ratio < min_ratio:
                    ok = False
                elif o.kind == "step":
                    m = self.predictors[o.sym].step
                    ok, _why, lb = gate(m, o.h, o.regime, o.bin, ratio, sc)       # honest gate again at the LIVE ratio
                    if ok:
                        o.p_lb = lb
                        ok = lb * ratio - 1.0 > sc.min_edge
            if ok and not self._blocked():
                go.append((o, prop, ratio))
            else:
                self._explore_release(o)
        if not go:
            return
        reserved = []
        for o, prop, ratio in go:
            if self.money.reserve(o.stake):
                reserved.append((o, prop, ratio))
            else:
                self._explore_release(o)
        for o, _, _ in reserved:
            self._used.add(o.key)
        if len(self._used) > 5000:
            self._used.clear()
        self._inflight += len(reserved)
        try:
            res = await asyncio.gather(*(self._buy_order(o, p, r) for o, p, r in reserved), return_exceptions=True)
        finally:
            self._inflight -= len(reserved)
            for o, _, _ in reserved:
                self.money.release(o.stake)
        for (o, _, _), cid in zip(reserved, res):
            if isinstance(cid, BaseException) or not cid:
                if o.kind == "step_explore" and any(w in self._last_buy_err.lower() for w in ("timeout", "timed out")):
                    logger.error("STEP BUY IN DOUBT %s %s: explore reservation %.2f stays HELD", o.sym, o.kind, o.stake)
                else:
                    self._explore_release(o)
            else:
                self._g_orders[o.kind] = self._g_orders.get(o.kind, 0) + 1
                if self._first_trade_wall is None:
                    self._first_trade_wall = time.time()
                    logger.info("STEP first trade %.1f s after boot", self._first_trade_wall - self._boot_wall)

    def _explore_release(self, o) -> None:
        if getattr(o, "kind", "") == "step_explore" and self.step_probe is not None:
            self.step_probe.settle(o.stake, o.stake)         # nothing was bought: give the reservation back

    async def _buy_order(self, o, prop: dict, ratio: float):
        if not isinstance(o, StepOrder):
            return await super()._buy_order(o, prop, ratio)
        g = self.predictors[o.sym]
        d = int(o.contract[1])
        sig = sp.SetSignal(symbol=o.sym, epoch=g.last_epoch, wall=time.time(), ranked=(d,), digits=(d,), probs=(o.q,),
                           set_prob=o.q, contracts=[(o.contract, 1, o.q)])
        t0 = time.monotonic()
        cid = await self._buy_signal(o.edge, sig, (o.contract, 1, o.p_lb), ratio, prop, o.stake)
        if cid:
            self._rtt_note(o.sym, "buy", time.monotonic() - t0)
        if cid and cid in self._open:
            self._open[cid].update({"kind": o.kind, "p_lb": o.p_lb, "q": o.q, "p_sh": o.p_sh, "h": o.h,
                                    "step_key": (o.sym, o.h, o.regime, o.bin)})
            logger.info("STEP ORDER %s %s MATCH%d %s h%d reg%s bin%d stake=%.2f q=%.3f p_sh=%.3f p_lb=%.3f ratio=%.3f", cid, o.sym, d, o.kind,
                        o.h, g.step.regime_name(o.regime), o.bin, o.stake, o.q, o.p_sh, o.p_lb, ratio)
        return cid

    # ── settlement: realised results per group ────────────────────────────────────────────────────────────
    async def _on_contract_result(self, cid: str, msg: dict):
        info = self._open.get(cid)
        poc = msg.get("proposal_open_contract", msg)
        await super()._on_contract_result(cid, msg)
        if info is None or cid in self._open or not info.get("step_key"):
            return
        profit = float(poc.get("profit") or 0.0)
        stake = float(info.get("stake") or 0.0)
        won = profit > 0
        sk = info["step_key"]
        rec = self._step_real.setdefault(sk, [0, 0, 0.0])
        rec[0] += 1
        rec[1] += int(won)
        rec[2] += profit
        sy = self._step_sym.setdefault(sk[0], [0, 0, 0.0, 0.0])
        sy[0] += 1
        sy[1] += int(won)
        sy[2] += profit
        sy[3] += 1.0 / max(float(info.get("ratio") or 1.0), 1.0001)
        self._step_n += 1
        if info.get("kind") == "step_explore" and self.step_probe is not None:
            self.step_probe.settle(stake, stake + profit)
        if info.get("kind") == "step":
            self._step_recent.append((int(won), float(info.get("p_sh") or 0.1)))
            self._horizon_check()

    def _horizon_check(self) -> None:
        r = self._step_recent
        if len(r) < r.maxlen or self._step_n - self._mm_at < 50:
            return
        n = len(r)
        real = sum(w for w, _ in r) / n
        exp = sum(p for _, p in r) / n
        se = math.sqrt(sum(p * (1.0 - p) for _, p in r)) / n
        if real < exp - 3.0 * se:
            self._mm_at = self._step_n
            logger.warning("STEP HORIZON MISMATCH: last %d gated trades hit %.3f vs shadow %.3f (se %.3f) - orders probably land a tick late; try STEP_HORIZON=2", n, real, exp, se)

    # ── reporting ─────────────────────────────────────────────────────────────────────────────────────────
    def _step_lines(self) -> List[str]:
        out: List[str] = []
        tot = {1: [0, 0], 2: [0, 0]}
        sc = self.scfg
        for sym, g in self.predictors.items():
            m = getattr(g, "step", None)
            if m is None:
                continue
            if m.disabled:
                out.append(f"STEP REPORT {sym}: step model DISABLED")
                continue
            hu = self._h_used.get(sym) or self._horizon_for(sym)
            parts = []
            for h in (1, 2):
                a = m.agg_h[h]
                tot[h][0] += a.n
                tot[h][1] += a.hits
                if a.n:
                    lo, hi = wilson(a.hits, a.n, 1.96)
                    parts.append(f"h{h} top1={a.hits / a.n:.4f} CI[{lo:.3f},{hi:.3f}] vs 0.10 n={a.n}")
                else:
                    parts.append(f"h{h} n/a")
            ratio = self._ratio_fn(sym)(("MATCH", 0))
            p = m.cur.get(hu)
            if p is None:
                gs = "warming up"
            else:
                ok, why, _ = gate(m, hu, p.regime, p.bin, ratio, sc)
                gs = f"{'OPEN' if ok else 'closed'} ({why})"
            best = " ".join(f"r{m.regime_name(r)}b{b}:n={n},hit={hr:.3f},lb={lo:.3f}" for lo, r, b, n, hr in m.top_groups(hu, 3)) or "-"
            rs = self._step_sym.get(sym)
            real = (f"trades={rs[0]} hit={rs[1] / rs[0]:.3f} vs be {rs[3] / rs[0]:.3f} pnl={rs[2]:+.2f}" if rs and rs[0] else "trades=0")
            out.append(f"STEP REPORT {sym}: ticks={m.idx} v={m.v:.2f} reg={m.regime_name(p.regime) if p else '-'} gaps={m.n_gaps} | {' | '.join(parts)} | "
                       f"best h{hu} [{best}] vs 1/ratio {1.0 / ratio:.3f} | gate {gs} | {real} | {self._rtt_text(sym)}")
        out.append(self._step_all_line(tot))
        return out

    def _step_all_line(self, tot: Optional[dict] = None) -> str:
        if tot is None:
            tot = {h: [sum(getattr(g, "step").agg_h[h].n for g in self.predictors.values() if hasattr(g, "step")),
                       sum(getattr(g, "step").agg_h[h].hits for g in self.predictors.values() if hasattr(g, "step"))] for h in (1, 2)}
        n = sum(v[0] for v in self._step_sym.values())
        w = sum(v[1] for v in self._step_sym.values())
        be = sum(v[3] for v in self._step_sym.values())
        pnl = sum(v[2] for v in self._step_sym.values())
        sh = " ".join(f"h{h} top1={100.0 * v[1] / v[0]:.2f}% n={v[0]}" for h, v in tot.items() if v[0]) or "no shadow yet"
        return (f"STEP REPORT ALL: shadow {sh} (chance 10%) | gates open {len(self._gates_open)} | step trades {n} "
                f"hit={(100.0 * w / n if n else 0):.1f}% vs break-even {(100.0 * be / n if n else 0):.1f}% | step P&L {pnl:+.2f} | "
                f"orders {self._g_orders} | balance {float(self.client.balance or 0):.2f} (start {self.start_balance})")

    def report_lines(self) -> List[str]:
        return super().report_lines() + [self._step_all_line()]

    async def _step_report_loop(self):
        period = self.scfg.report_seconds
        if period <= 0:
            return
        while True:
            await asyncio.sleep(period)
            try:
                for line in self._step_lines():
                    logger.info(line)
                self._push_dashboard()
            except Exception as exc:
                logger.error("step report error: %s", exc, exc_info=True)

    def _push_dashboard(self) -> None:
        try:
            import keep_alive
            summ = {"gates_open": len(self._gates_open), "trades": sum(v[0] for v in self._step_sym.values()),
                    "wins": sum(v[1] for v in self._step_sym.values()), "pnl": round(sum(v[2] for v in self._step_sym.values()), 2),
                    "symbols": {s: {"ticks": g.step.idx, "v": round(g.step.v, 2), "h": self._h_used.get(s),
                                    "top1_h1": round(g.step.agg_h[1].hits / g.step.agg_h[1].n, 4) if g.step.agg_h[1].n else None,
                                    "top1_h2": round(g.step.agg_h[2].hits / g.step.agg_h[2].n, 4) if g.step.agg_h[2].n else None,
                                    "disabled": g.step.disabled}
                                for s, g in self.predictors.items() if hasattr(g, "step")}}
            keep_alive.update_status(step_summary=summ)      # served by the existing /debug state dump
        except Exception:
            pass

    async def _step_cross_loop(self):
        period = self.scfg.cross_seconds
        if period <= 0:
            return
        while True:
            await asyncio.sleep(period)
            try:
                snap = {s: np.array(list(g.step.steps), dtype=np.int64) for s, g in self.predictors.items()
                        if hasattr(g, "step") and not g.step.disabled and len(g.step.steps) >= 200}
                if len(snap) < 2:
                    continue
                npairs, top = await asyncio.to_thread(cross_pairs, snap, 5)
                logger.info("STEP CROSS: %d pairs tested (null max |z| about %.1f); top by |r| at lag 0 (report only, never trades):",
                            npairs, math.sqrt(2.0 * math.log(max(npairs, 2))))
                for t in top:
                    lg = " ".join(f"{k:+d}:{v[0]:+.3f}(z{v[1]:+.1f})" for k, v in t["lags"].items() if v)
                    logger.info("STEP CROSS %s~%s r=%+.3f n=%d z=%+.2f | lags %s", t["a"], t["b"], t["r"], t["n"], t["z"], lg)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("step cross error: %s", exc, exc_info=True)
