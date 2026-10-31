"""guesser.engine - GUESSER MODE engine (subclass of set_engine.SetEngine; enabled only by GUESSER_MODE=1, demo account only).

Reused unchanged from SetEngine/BotEngine: Deriv client, reconnects/watchdog, trade window / session gates, proposal + buy path
(SetEngine._buy_signal incl. direct-buy fallback), settlement polling, money.reserve/release/note_buy, stats, dashboard.
New: per-symbol SymbolGuesser (guesser.core) replaces SetPredictor; the scan asks guesser.policy.decide() for orders.

Deviation from the brief (documented in docs/guesser/NOTES.md): mh_engine's in-doubt-buy reconciliation is NOT reused (it is wired
into MHEngine's candidate objects).  Here a buy whose outcome is unknown (timeout after the frame went out) keeps its probe-budget
reservation and money hold released only when the engine's own settle loop sees the contract; it is logged loudly.  SetEngine's
buy path already refuses to re-buy the same (symbol, tick), so a doubtful buy can never be duplicated.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Dict, List, Optional

import config
import predictor as pred
import set_predictor as sp
import target_guard
from set_engine import SetEngine, account_allowed
from bot_engine import BotEngine, _cfg

from . import evidence as evd
from . import policy as pol
from .calibrate import SetCalibrator
from .core import GuesserConfig, SymbolGuesser, wilson

logger = logging.getLogger(__name__)


class GuesserEngine(SetEngine):
    def __init__(self):
        super().__init__()
        self.gcfg = GuesserConfig.from_module(config)
        self.cadence = str(_cfg("GUESSER_CADENCE", "probe")).lower()
        self.cal = SetCalibrator(self.gcfg.lb_z)
        self.probe: Optional[pol.ProbeBudget] = None
        self.start_balance: Optional[float] = None
        self._g_scans = 0
        self._g_orders = {"evidence": 0, "probe": 0, "bold": 0}
        self._g_probe_pnl = 0.0
        self._t2_seen: set = set()
        self._first_trade_wall: Optional[float] = None
        self._boot_wall = time.time()
        self._last_scan_log = 0.0
        self._last_buy_err = ""
        self._exact_busy: set = set()

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def run(self):
        ok, why = account_allowed()                     # same demo-only guard as SET/MH mode - never weakened
        if not ok:
            logger.critical("GUESSER MODE REFUSED TO START: %s", why)
            return
        logger.info("GUESSER MODE: %s | cadence=%s contracts=%s h=%d prior_null=%.2f alpha1=%.3f multiplicity=%s min stake %.2f",
                    why, self.cadence, ",".join(self.gcfg.allowed), self.gcfg.h, self.gcfg.prior_null, self.gcfg.alpha1,
                    self.gcfg.multiplicity, self.stake)
        await BotEngine.run(self)

    def _extra_tasks(self) -> list:
        tasks = [self._scanner_loop(), self._guesser_report_loop()]
        if bool(_cfg("GUESSER_EXACT", True)):
            tasks.append(self._exact_loop())
        return tasks

    def _pr_desc(self, pr) -> str:
        return pr.describe()

    # ── warm start (paged history -> batch replay in a worker thread) ────────
    async def _fit_symbol(self, sym: str, initial: bool = False) -> bool:
        g = self.predictors.get(sym)
        if g is not None and not initial and not self._needs_resync():
            return True                                  # online state is already current; periodic refits would only reset evidence
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
        if g is None:
            cfg = GuesserConfig.from_module(config, dec=dec)
            g = SymbolGuesser(sym, cfg, self.cal)
            digits = pred.digits_from_quotes([t["quote"] for t in ticks], dec)
            await asyncio.to_thread(g.fit, [int(d) for d in digits], [t["quote"] for t in ticks], [t["epoch"] for t in ticks])
        else:                                            # resync after a reconnect: feed what we missed; a hole -> gap()
            new = [t for t in ticks if g.last_epoch is None or t["epoch"] > g.last_epoch]
            if new and g.last_epoch is not None and ticks and ticks[0]["epoch"] > g.last_epoch:
                g.mix.gap()
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
        return True

    # ── ticks: pass the QUOTE through (price-increment expert) ───────────────
    def _on_tick(self, sym: str, tick: dict):
        try:
            q, ep = float(tick["quote"]), float(tick.get("epoch", time.time()))
        except Exception:
            return
        self._last_tick_wall[sym] = time.time()
        g = self.predictors.get(sym)
        if g is None:
            return
        digit = pred.last_digit(q, self._decimals[sym])
        if not g.on_tick(digit, q, ep):
            return
        self._live[sym].append((ep, digit))
        self._last_epoch[sym] = ep
        eps = self._recent_epochs.setdefault(sym, [])
        eps.append(ep)
        del eps[:-50]
        self._events[sym].set()

    async def _worker(self, sym: str):
        ev = self._events[sym]
        while True:
            await ev.wait()
            ev.clear()
            self._scan_event.set()                       # the guesser itself already advanced inside _on_tick (< 2 ms)

    async def _on_api_error(self, what: str, exc: Exception):
        self._last_buy_err = f"{what}: {exc}"
        await super()._on_api_error(what, exc)

    # ── tiers across symbols ─────────────────────────────────────────────────
    def _update_tiers(self) -> Dict[str, int]:
        gs = {s: g for s, g in self.predictors.items() if not g.disabled and g.mix.q_mix is not None}
        log_es = {s: g.log_e for s, g in gs.items()}
        sc = {s: g.alpha_scale() for s, g in gs.items()}
        tr = evd.tiers(log_es, self.gcfg.alpha1, self.gcfg.alpha2, self.gcfg.multiplicity, sc, {s: g.xp.active for s, g in gs.items()})
        for s, g in gs.items():
            g.tier = tr[s]
            if tr[s] >= evd.T2 and s not in self._t2_seen:
                self._t2_seen.add(s)
                logger.warning("GUESSER %s reached tier %s (log BF %.2f, top expert %s w=%.3f)%s", s, evd.TIER_NAMES[tr[s]],
                               g.log_e, *g.mix.last_top, " - bold mode switches to Kelly for this symbol" if self.cadence == "bold" else "")
        return tr

    def _ratio_fn(self, sym: str):
        cache = self._ratios.get(sym, {})
        m = self.gcfg.margin

        def f(c):
            r = cache.get(c)
            return float(r) if r else pol.model_ratio(len(pol.win_set(c)), m)
        return f

    def _target(self) -> Optional[float]:
        t = target_guard.target()
        if t is not None:
            return t
        return None if self.start_balance is None else self.start_balance * float(_cfg("GUESSER_TARGET_MULT", 2.0))

    # ── the scan ─────────────────────────────────────────────────────────────
    async def _scan_once(self):
        self._g_scans += 1
        now = time.time()
        bal = float(self.client.balance or 0.0)
        if self.start_balance is None and bal > 0:
            self.start_balance = bal
            self.probe = pol.ProbeBudget(bal, float(_cfg("GUESSER_PROBE_BUDGET_FRAC", 0.05)),
                                         int(_cfg("GUESSER_PROBE_MAX_PER_MIN", 12)), max(1, int(_cfg("GUESSER_PROBE_MAX_PER_MIN", 12)) // 4))
            logger.info("GUESSER session start balance %.2f, probe budget %.2f", bal, self.probe.cap)
        slots = int(_cfg("GUESSER_MAX_OPEN", 6)) - len(self._open) - self._inflight
        if slots <= 0 or self.start_balance is None:
            return
        stake_ok = self._next_stake()
        if stake_ok is None:
            self._last_decision = "stopped by money rules"
            return
        self._update_tiers()
        views = []
        for sym, g in self.predictors.items():
            ep = g.last_epoch
            key = (sym, ep if ep is not None else g.mix.n)
            if g.disabled or key in self._used or now - self._last_tick_wall.get(sym, 0) > self._max_age(sym):
                continue
            v = g.view(self._ratio_fn(sym))
            if v is not None:
                views.append(v)
        if not views:
            return
        min_stake = self.stake
        orders, why = pol.decide(views, self.cadence, bal, min_stake, slots, self.probe, now, float(_cfg("GUESSER_KELLY_FRAC", 0.25)),
                                 float(_cfg("GUESSER_MAX_STAKE_FRAC", 0.05)), float(_cfg("GUESSER_MIN_EDGE", 0.0)),
                                 target=self._target(), max_stake=float(_cfg("GUESSER_MAX_STAKE", 100.0)))
        tiers = ",".join(f"{evd.TIER_NAMES[t][:2]}{sum(1 for v in views if v.tier == t)}" for t in sorted({v.tier for v in views}))
        msg = f"GUESSER SCAN {self._g_scans}: views={len(views)} tiers[{tiers}] bal={bal:.2f} -> {len(orders)} order(s); {why}"
        if orders or now - self._last_scan_log > 60:
            logger.info(msg)
            self._last_scan_log = now
        else:
            logger.debug(msg)
        self._last_decision = msg[:160]
        if not orders:
            return
        # live proposals for all orders at once; evidence orders are re-checked against the REAL ratio
        props = await asyncio.gather(*(self._propose(o.sym, o.contract, o.stake) for o in orders), return_exceptions=True)
        go = []
        for o, prop in zip(orders, props):
            ok = not isinstance(prop, BaseException) and bool(prop) and float(prop.get("payout") or 0) > 0
            ratio = float(prop["payout"]) / o.stake if ok else 0.0
            if ok:
                self._ratios.setdefault(o.sym, {})[o.contract] = ratio
                if o.kind == "evidence" and o.p_lb * ratio - 1.0 <= float(_cfg("GUESSER_MIN_EDGE", 0.0)):
                    ok = False                                  # live payout ate the edge
            if ok and not self._blocked():
                go.append((o, prop, ratio))
            elif o.kind == "probe" and self.probe is not None:
                self.probe.settle(o.stake, o.stake)            # nothing was bought: release the reservation
        if not go:
            return
        reserved = []
        for o, prop, ratio in go:
            if self.money.reserve(o.stake):
                reserved.append((o, prop, ratio))
            elif o.kind == "probe" and self.probe is not None:
                self.probe.settle(o.stake, o.stake)
        for o, _, _ in reserved:
            g = self.predictors[o.sym]
            self._used.add((o.sym, g.last_epoch if g.last_epoch is not None else g.mix.n))
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
                if o.kind == "probe" and self.probe is not None:
                    if any(w in self._last_buy_err.lower() for w in ("timeout", "timed out")):
                        logger.error("GUESSER BUY IN DOUBT %s %s %s: probe reservation %.2f stays HELD (budget conservative)",
                                     o.sym, o.contract, o.kind, o.stake)
                    else:
                        self.probe.settle(o.stake, o.stake)
            else:
                self._g_orders[o.kind] = self._g_orders.get(o.kind, 0) + 1
                if self._first_trade_wall is None:
                    self._first_trade_wall = time.time()
                    logger.info("GUESSER first trade %.1f s after boot", self._first_trade_wall - self._boot_wall)

    async def _buy_order(self, o: pol.Order, prop: dict, ratio: float):
        g = self.predictors[o.sym]
        qm = g.mix.q_mix
        top = tuple(int(d) for d in (g.mix.ranked_digits()[:3] if g.mix.ranked_digits() is not None else range(3)))
        wins = tuple(pol.win_set(o.contract))
        p_hat = float(sum(qm[d] for d in wins)) if qm is not None else len(wins) / 10.0
        sig = sp.SetSignal(symbol=o.sym, epoch=g.last_epoch, wall=time.time(), ranked=top, digits=tuple(sorted(wins)),
                           probs=tuple(float(qm[d]) for d in top) if qm is not None else (0.1,) * 3, set_prob=p_hat,
                           contracts=[(o.contract, len(wins), p_hat)])
        cid = await self._buy_signal(o.edge, sig, (o.contract, len(wins), p_hat), ratio, prop, o.stake)
        if cid and cid in self._open:
            self._open[cid].update({"kind": o.kind, "g_tier": g.tier, "p_lb": o.p_lb})
            logger.info("GUESSER ORDER %s %s %s %s stake=%.2f p_lb=%.3f ratio=%.3f edge=%.3f tier=%s log_bf=%.2f", cid, o.sym,
                        sp.tag(o.contract), o.kind, o.stake, o.p_lb, ratio, o.edge, evd.TIER_NAMES[g.tier], g.log_e)
        return cid

    # ── settlement hook: probe budget accounting ─────────────────────────────
    async def _on_contract_result(self, cid: str, msg: dict):
        info = self._open.get(cid)
        poc = msg.get("proposal_open_contract", msg)
        await super()._on_contract_result(cid, msg)
        if info is not None and cid not in self._open:        # settled by THIS call (popped exactly once)
            profit = float(poc.get("profit") or 0.0)
            stake = float(info.get("stake") or 0.0)
            if info.get("kind") == "probe" and self.probe is not None:
                self.probe.settle(stake, stake + profit)
                self._g_probe_pnl += profit

    # ── T3 exact solvers: child process, never on the loop ───────────────────
    async def _exact_loop(self):
        from rng_core.adapter import run_solver_async
        period = 120.0
        while True:
            await asyncio.sleep(period)
            for sym, g in list(self.predictors.items()):
                if g.disabled or g.xp.active or sym in self._exact_busy or len(g.tail) < 300:
                    continue
                self._exact_busy.add(sym)
                try:
                    seg = list(g.tail)
                    n0 = g.tail_n
                    req = g.xp.request(seg, g.tail_epoch0, budget_s=float(_cfg("V2_SOLVER_BUDGET_SECONDS", 20)),
                                       max_mb=int(_cfg("V2_SOLVER_MAX_MB", 400)))
                    res = await run_solver_async(req)
                    fit = (res or {}).get("fit")
                    if fit:
                        later = list(g.tail)[-(g.tail_n - n0):] if g.tail_n > n0 else []
                        if g.xp.install(fit, seg, later):
                            logger.warning("GUESSER %s T3 EXACT via %s", sym, g.xp.name)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error("exact loop %s: %s", sym, exc)
                finally:
                    self._exact_busy.discard(sym)

    # ── reporting ────────────────────────────────────────────────────────────
    async def _guesser_report_loop(self):
        period = float(_cfg("GUESSER_REPORT_SECONDS", 300))
        if period <= 0:
            return
        while True:
            await asyncio.sleep(period)
            try:
                for line in self.report_lines():
                    logger.info(line)
            except Exception as exc:
                logger.error("guesser report error: %s", exc, exc_info=True)

    def report_lines(self) -> List[str]:
        out = []
        N = T1 = T3_ = 0
        for sym, g in self.predictors.items():
            s = g.stats()
            N += s["n"]; T1 += g.top1; T3_ += g.top3
            out.append(f"GUESSER REPORT {sym}: ticks={s['ticks']} tier={s['tier']} logBF={s['log_bf']} top={s['top_expert']}({s['top_w']}) "
                       f"shadow n={s['n']} top1={s['top1'][0]} CI{s['top1'][1]} vs 0.10 top3={s['top3'][0]} CI{s['top3'][1]} vs 0.30 "
                       f"llgain={s['ll_gain']} runs={s['runs']}{' DISABLED' if g.disabled else ''}")
        T = self._n_trades
        be = f"{100.0 * self._be_sum / T:.1f}%" if T else "-"
        wr = f"{100.0 * self._n_wins / T:.1f}%" if T else "-"
        pb = self.probe
        pr = f"probe budget used {pb.cap - pb.left():.2f}/{pb.cap:.2f} (net realised {pb.realised_loss:+.2f}, probes {pb.n})" if pb else "probe n/a"
        t1 = f"{100.0 * T1 / N:.1f}%" if N else "-"
        t3 = f"{100.0 * T3_ / N:.1f}%" if N else "-"
        out.append(f"GUESSER REPORT ALL: symbols={len(self.predictors)} shadow n={N} top1={t1} (chance 10%) top3={t3} (chance 30%) | trades by kind "
                   f"{self._g_orders} | {pr} | realised win {wr} vs break-even {be} | P&L {self._total_pnl:+.2f} | balance "
                   f"{float(self.client.balance or 0):.2f} (start {self.start_balance}) | scans {self._g_scans}")
        return out
