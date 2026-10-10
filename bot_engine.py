"""
bot_engine.py – lean engine: the walk-forward digit predictor is the ONLY strategy.

Replaces the old multi-strategy engine. There are no trading gates here: no kill
switch, no loss/drawdown/streak limits, no cooldowns, no news filter, no early
exits (digit contracts run to expiry). Only technical necessities remain:
reconnect (deriv_client), auth, rate-limit backoff, error logging, and draining
open contracts before the scheduled redeploy.

Per tick, per symbol:  rank() the tradable contexts -> live proposal (exact
contract, flat stake) -> payout/stake >= MIN_PAYOUT_RATIO and p_lower > 1/ratio
+ EDGE_MARGIN -> buy. Anything else = no trade.
"""
import asyncio
import logging
import statistics
import time
from typing import Dict, List, Optional

import config
import predictor as pred
import predictor_stats
import restart_scheduler
from deriv_client import DerivClient

logger = logging.getLogger(__name__)


def _cfg(name, default):
    return getattr(config, name, default)


class BotEngine:
    def __init__(self):
        self.client = DerivClient()
        self.symbols: List[str] = list(_cfg("PREDICTOR_SYMBOLS", ["R_100"]))
        self.stake = float(_cfg("PREDICTOR_STAKE", 1000.0))
        self.duration = int(_cfg("CONTRACT_DURATION_TICKS", 1))
        self.stats = predictor_stats.get_stats()
        self.predictors: Dict[str, pred.Predictor] = {}
        self._decimals: Dict[str, int] = {}
        self._events: Dict[str, asyncio.Event] = {}
        self._busy: set = set()
        self._open: Dict[str, dict] = {}
        self._ratios: Dict[str, Dict[pred.Candidate, float]] = {}
        self._recent_epochs: Dict[str, List[float]] = {}
        self._last_epoch: Dict[str, float] = {}
        self._halted_until = 0.0
        self._drain_started: Optional[float] = None
        self._lag_obs: List[int] = []
        self._last_decision = "—"
        self.balance = 0.0

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def run(self):
        logger.info("=" * 64)
        logger.info("  Digit predictor bot  –  stake %.2f, symbols %s", self.stake, self.symbols)
        logger.info("=" * 64)
        ws_task = asyncio.create_task(self.client.connect())
        for _ in range(60):
            if self.client.is_connected:
                break
            await asyncio.sleep(1)
        if not self.client.is_connected:
            logger.error("Could not connect/authorise within 60 s")
            ws_task.cancel()
            return
        self.balance = self.client.balance
        self.client.on_balance(lambda b: setattr(self, "balance", b))

        for sym in self.symbols:
            await self._init_symbol(sym)

        tasks = [asyncio.create_task(self._worker(s)) for s in self.symbols]
        tasks += [asyncio.create_task(self._settle_loop()),
                  asyncio.create_task(self._flush_loop()),
                  asyncio.create_task(self._dashboard_loop())]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            ws_task.cancel()

    # ── warm-up ──────────────────────────────────────────────────────────────
    async def _init_symbol(self, sym: str):
        n = int(_cfg("WARMUP_TICKS", 5000))
        ticks = await self.client.get_tick_history(sym, n)
        if len(ticks) < 500:
            logger.warning("WARM-UP %s: only %d ticks returned", sym, len(ticks))
        quotes = [t["quote"] for t in ticks]
        dec_map = _cfg("DIGIT_DECIMALS", {}) or {}
        dec = dec_map.get(sym)
        if dec is None:
            dec = pred.infer_decimals(quotes) if quotes else 2
        self._decimals[sym] = int(dec)
        digits = pred.digits_from_quotes(quotes, self._decimals[sym])
        self._recent_epochs[sym] = [t["epoch"] for t in ticks[-50:]]
        self._ratios[sym] = {}
        pr = pred.Predictor(sym, payout_ratio_fn=lambda c, s=sym: self._ratios.get(s, {}).get(c))
        await self._probe_ratios(sym, pr)
        pr.fit(digits, last_epoch=ticks[-1]["epoch"] if ticks else None)
        pred.register(sym, pr)
        self.predictors[sym] = pr
        self._events[sym] = asyncio.Event()
        await self.client.subscribe_ticks(sym, lambda tick, s=sym: self._on_tick(s, tick))
        logger.info("READY %s: decimals=%d horizon=%d tradable contexts=%d",
                    sym, self._decimals[sym], pr.p.horizon, len(pr.selected))

    async def _probe_ratios(self, sym: str, pr: pred.Predictor):
        """Live payout/stake for candidates that could clear the floor, so
        selection uses real break-evens (never a hardcoded payout)."""
        floor = pr.p.min_payout_ratio
        cands = [c for c in pr.candidates if pred.estimated_payout_ratio(c, pr.p.payout_factor) >= floor * 0.95]
        sem = asyncio.Semaphore(4)

        async def one(c):
            async with sem:
                prop = await self._propose(sym, c, self.stake)
                if prop and prop.get("payout"):
                    self._ratios[sym][c] = float(prop["payout"]) / self.stake
        await asyncio.gather(*(one(c) for c in cands), return_exceptions=True)
        logger.info("PAYOUT PROBE %s: %s", sym,
                    {f"{k[0]}{'' if k[1] is None else k[1]}": round(v, 3) for k, v in self._ratios[sym].items()})

    # ── ticks ────────────────────────────────────────────────────────────────
    def _on_tick(self, sym: str, tick: dict):
        try:
            q, ep = float(tick["quote"]), float(tick.get("epoch", time.time()))
        except Exception:
            return
        pr = self.predictors.get(sym)
        if pr is None:
            return
        if not pr.on_tick(pred.last_digit(q, self._decimals[sym]), ep):
            return
        self._last_epoch[sym] = ep
        eps = self._recent_epochs.setdefault(sym, [])
        eps.append(ep)
        del eps[:-50]
        self._events[sym].set()

    def _blocked(self) -> bool:
        return restart_scheduler.is_redeploy_pending() or time.time() < self._halted_until

    # ── trading ──────────────────────────────────────────────────────────────
    async def _worker(self, sym: str):
        ev = self._events[sym]
        while True:
            await ev.wait()
            ev.clear()
            if self._blocked() or sym in self._busy:
                continue
            self._busy.add(sym)
            try:
                await self._try_trade(sym)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("trade attempt error %s: %s", sym, exc, exc_info=True)
            finally:
                self._busy.discard(sym)

    async def _propose(self, sym: str, cand: pred.Candidate, stake: float) -> Optional[dict]:
        req = {"proposal": 1, "amount": stake, "basis": "stake",
               "contract_type": pred.contract_type(cand), "currency": "USD",
               "duration": self.duration, "duration_unit": "t", "underlying_symbol": sym}
        if cand[1] is not None:
            req["barrier"] = str(cand[1])
        try:
            resp = await self.client._send(req, timeout=10)
            return resp.get("proposal") or None
        except Exception as exc:
            await self._on_api_error("proposal", exc)
            return None

    async def _on_api_error(self, what: str, exc: Exception):
        msg = str(exc)
        logger.warning("Deriv %s rejected: %s", what, msg[:300])
        if "rate" in msg.lower() or "RateLimit" in msg:
            await asyncio.sleep(2.0)                       # rate-limit backoff

    async def _try_trade(self, sym: str):
        pr = self.predictors[sym]
        picks = pr.rank()
        if not picks:
            return
        tick_epoch = self._last_epoch.get(sym)
        for pk in picks[: int(_cfg("PREDICTOR_MAX_PROPOSALS", 3))]:
            if self._blocked():
                return
            prop = await self._propose(sym, pk.cand, self.stake)
            if not prop:
                continue
            payout = float(prop.get("payout") or 0.0)
            ratio = payout / self.stake                     # live ratio, never hardcoded
            self._ratios[sym][pk.cand] = ratio
            dec = pr.accept(pk, ratio)
            tag = f"{pk.cand[0]}{'' if pk.cand[1] is None else pk.cand[1]}"
            if not dec.ok:
                self._last_decision = f"skip {tag}: {dec.reason}"
                logger.info("SKIP %s %s: %s", sym, tag, dec.reason)
                continue
            if self._blocked():
                return
            ask = float(prop.get("ask_price") or self.stake)
            try:
                resp = await self.client._send({"buy": prop["id"], "price": ask}, timeout=10)
            except Exception as exc:
                await self._on_api_error("buy", exc)        # error handling, not a gate
                return
            buy = resp.get("buy") or {}
            cid = str(buy.get("contract_id", ""))
            if not cid:
                logger.warning("BUY returned no contract_id: %s", resp)
                return
            opened = float(buy.get("purchase_time") or time.time())
            self._open[cid] = {"sym": sym, "cand": pk.cand, "stake": self.stake, "ratio": ratio,
                               "opened_at": opened, "decision_epoch": tick_epoch, "t0": time.time(),
                               "ctx": pk.context, "p_lower": pk.p_lower}
            self._last_decision = f"BUY {tag} ctx={pk.context} lb={pk.p_lower:.3f} ratio={ratio:.3f}"
            logger.info("CONTRACT OPENED %s %s %s ctx=%s n=%d p=%.3f lb=%.3f ratio=%.3f be=%.4f edge=%.4f",
                        cid, sym, tag, pk.context, pk.n, pk.p_hat, pk.p_lower, ratio, dec.break_even, dec.edge)
            self.client._contract_symbol_map[cid] = sym
            try:
                await self.client.subscribe_contract(
                    cid, lambda msg, _c=cid: asyncio.create_task(self._on_contract_result(_c, msg)),
                    symbol=sym)
            except Exception as exc:
                logger.warning("subscribe_contract(%s) failed (settle loop will poll): %s", cid, exc)
            return

    # ── settlement ───────────────────────────────────────────────────────────
    async def _on_contract_result(self, cid: str, msg: dict):
        poc = msg.get("proposal_open_contract", msg)
        if not (poc.get("is_sold") or poc.get("is_expired") or poc.get("status") in ("sold", "won", "lost")):
            return
        info = self._open.pop(cid, None)
        if not info:
            return
        try:
            self.client.stop_tracking(cid)
        except Exception:
            pass
        profit = float(poc.get("profit") or 0.0)
        won = profit > 0
        self.stats.record(info["opened_at"], info["stake"], info["ratio"], profit)
        tag = f"{info['cand'][0]}{'' if info['cand'][1] is None else info['cand'][1]}"
        logger.info("CONTRACT SETTLED %s %s %s %s profit=%.2f", cid, info["sym"], tag,
                    "WIN" if won else "LOSS", profit)
        self._note_lag(info, poc)
        try:
            import keep_alive
            keep_alive.record_trade(info["sym"], tag, info["stake"], profit,
                                    balance_after=self.client.balance, won=won, strategy="PREDICTOR")
        except Exception:
            pass

    def _note_lag(self, info: dict, poc: dict):
        """Observed entry delay in ticks vs the configured horizon (log only)."""
        try:
            entry = float(poc.get("entry_tick_time") or 0)
            dec = info.get("decision_epoch")
            eps = self._recent_epochs.get(info["sym"], [])
            if not entry or not dec or len(eps) < 5:
                return
            interval = statistics.median([b - a for a, b in zip(eps, eps[1:]) if b > a] or [1])
            gap = round((entry - dec) / interval)
            self._lag_obs.append(gap)
            # entry tick should be decision tick + 1 + ENTRY_DELAY_TICKS
            expected = 1 + self.predictors[info["sym"]].p.entry_delay_ticks
            if gap != expected:
                logger.warning("LAG CHECK: entry tick was %d ticks after the decision tick, model assumes %d. "
                               "Set PREDICTOR_ENTRY_DELAY_TICKS=%d if this persists.",
                               gap, expected, max(0, gap - 1))
        except Exception:
            pass

    async def _settle_loop(self):
        max_wait = float(_cfg("REDEPLOY_DRAIN_MAX_SECONDS", 60))
        halt_max = float(_cfg("REDEPLOY_HALT_MAX_SECONDS", 600))
        while True:
            await asyncio.sleep(2)
            now = time.time()
            for cid, info in list(self._open.items()):         # poll stragglers
                if now - info["t0"] > 20:
                    poc = await self.client.force_check_contract(cid)
                    if poc:
                        await self._on_contract_result(cid, {"proposal_open_contract": poc})
            if not restart_scheduler.is_redeploy_pending() or now < self._halted_until:
                self._drain_started = None
                continue
            if self._drain_started is None:
                self._drain_started = now
                logger.warning("REDEPLOY DUE: no new trades; draining %d open contract(s)", len(self._open))
            if self._open and now - self._drain_started < max_wait:
                continue
            if self._open:
                logger.warning("Drain timeout: redeploying with %d contract(s) still open", len(self._open))
            await self.stats.flush(force=True)
            logger.warning("REDEPLOY: firing Render deploy")
            restart_scheduler.trigger_redeploy()
            self._halted_until = time.time() + halt_max         # old instance idle until replaced
            self._drain_started = None
            await asyncio.sleep(5)

    async def _flush_loop(self):
        while True:
            await asyncio.sleep(5)
            try:
                await self.stats.flush()
            except Exception as exc:
                logger.error("stats flush error: %s", exc)

    async def _dashboard_loop(self):
        while True:
            await asyncio.sleep(5)
            try:
                import keep_alive
                keep_alive.update_status(
                    balance=round(self.client.balance, 2), session="PREDICTOR",
                    current_symbol=",".join(self.symbols), last_signal=self._last_decision,
                    tradeable_count=sum(len(p.selected) for p in self.predictors.values()),
                    open_contracts_count=len(self._open))
                keep_alive.set_active_trades(len(self._open))
            except Exception:
                pass
