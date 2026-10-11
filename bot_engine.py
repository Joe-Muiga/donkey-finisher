"""
bot_engine.py – lean engine: the walk-forward digit predictor is the ONLY strategy.

Runs continuously (no scheduled redeploy). Self-healing is done in-process: the Deriv
client reconnects forever with backoff, a watchdog re-subscribes / reconnects when a tick
stream goes quiet, the predictor is re-synced after every reconnect and re-fitted
periodically, and the whole engine is restarted by main.py if anything fatal escapes.

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
import collections
import logging
import statistics
import time
from typing import Dict, List, Optional

import config
import predictor as pred
import predictor_stats
import restart_scheduler
import target_guard
from deriv_client import DerivClient

try:                                    # follower system is optional; the collector never depends on it
    import db as _follower_db
except Exception:                       # pragma: no cover
    _follower_db = None
try:
    import follower_manager as _follower_mgr
except Exception:                       # pragma: no cover
    _follower_mgr = None

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
        self._target_done = asyncio.Event()   # set once the TARGET_BALANCE shutdown has finished
        # ── self-healing state ──
        self._last_tick_wall: Dict[str, float] = {}
        self._live: Dict[str, collections.deque] = {}      # recent (epoch, digit) seen live
        self._seen_conn = 0                                 # client.connection_count we are synced to
        self._last_forced_reconnect = 0.0
        # Deriv sometimes answers a buy-by-proposal-id with "Unknown contract proposal".
        # After the first such failure we buy with the contract parameters directly
        # (buy="1" + parameters), which does not depend on a proposal id at all.
        self._direct_buy = False
        # ── performance tracking (log only; nothing here ever blocks a trade) ──
        self._n_trades = 0
        self._n_wins = 0
        self._be_sum = 0.0            # sum of break-even win probabilities (1/ratio) of trades taken
        self._fair_mean = 0.0         # sum of expected profit (in stakes) if every digit were fair
        self._fair_var = 0.0          # sum of variance (in stakes^2) under the same assumption
        self._total_pnl = 0.0

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
        # Target check BEFORE anything is warmed up: if the balance is already at/above
        # TARGET_BALANCE (e.g. service resumed without raising the target) never trade.
        if target_guard.target() is not None:
            try:
                self.balance = await self.client.fetch_balance_live()
            except Exception as exc:
                logger.warning("Initial live balance fetch failed (%s) - using subscribed value", exc)
            logger.info("TARGET GUARD: target=%.2f  live balance=%.2f", target_guard.target(), self.balance)
            if target_guard.should_stop(self.balance):
                if target_guard.mark_reached(self.balance):
                    await self._shutdown_for_target(self.balance)
                ws_task.cancel()
                return
        else:
            logger.info("TARGET GUARD: TARGET_BALANCE not set - no balance target")
        self.client.on_balance(self._on_balance)
        self._seen_conn = self._conn_count()

        for sym in self.symbols:
            await self._init_symbol(sym)

        tasks = [asyncio.create_task(self._worker(s)) for s in self.symbols]
        tasks += [asyncio.create_task(self._settle_loop()),
                  asyncio.create_task(self._flush_loop()),
                  asyncio.create_task(self._dashboard_loop()),
                  asyncio.create_task(self._watchdog_loop()),
                  asyncio.create_task(self._refit_loop()),
                  asyncio.create_task(self._target_monitor_loop()),
                  asyncio.create_task(self._supervise_ws(ws_task))]
        if _follower_mgr is not None:        # Deriv server-time offset for the follower scheduler
            tasks.append(asyncio.create_task(_follower_mgr.collector_time_sync(self.client)))
        stopper = asyncio.create_task(self._target_done.wait())
        main_t = asyncio.gather(*tasks)
        try:
            await asyncio.wait({main_t, stopper}, return_when=asyncio.FIRST_COMPLETED)
            if main_t.done() and not main_t.cancelled():
                main_t.result()                       # re-raise a crash so main.py restarts the engine
        except asyncio.CancelledError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            stopper.cancel()
            ws_task.cancel()

    # ── warm-up ──────────────────────────────────────────────────────────────
    async def _init_symbol(self, sym: str):
        self._events[sym] = asyncio.Event()
        self._live[sym] = collections.deque(maxlen=300)
        self._last_tick_wall[sym] = time.time()
        await self._fit_symbol(sym, initial=True)
        await self.client.subscribe_ticks(sym, lambda tick, s=sym: self._on_tick(s, tick))
        pr = self.predictors[sym]
        logger.info("READY %s: decimals=%d horizon=%d tradable contexts=%d",
                    sym, self._decimals[sym], pr.p.horizon, len(pr.selected))

    async def _fit_symbol(self, sym: str, initial: bool = False) -> bool:
        """Download recent ticks, re-probe payouts and (re)fit the predictor. Used for the
        first warm-up and for every later re-sync/refit. On a refit that cannot get enough
        history the old predictor is kept untouched and False is returned."""
        n = int(_cfg("WARMUP_TICKS", 5000))
        ticks = await self.client.get_tick_history(sym, n)
        if len(ticks) < 500:
            logger.warning("WARM-UP %s: only %d ticks returned", sym, len(ticks))
        if not initial and len(ticks) < 300:
            return False
        quotes = [t["quote"] for t in ticks]
        if sym not in self._decimals:
            dec_map = _cfg("DIGIT_DECIMALS", {}) or {}
            dec = dec_map.get(sym)
            if dec is None:
                dec = pred.infer_decimals(quotes) if quotes else 2
            self._decimals[sym] = int(dec)
        digits = pred.digits_from_quotes(quotes, self._decimals[sym])
        last_ep = ticks[-1]["epoch"] if ticks else None
        self._recent_epochs[sym] = [t["epoch"] for t in ticks[-50:]]
        pr = self.predictors.get(sym)
        if pr is None:
            self._ratios.setdefault(sym, {})
            pr = pred.Predictor(sym, payout_ratio_fn=lambda c, s=sym: self._ratios.get(s, {}).get(c))
        await self._probe_ratios(sym, pr)
        pr.fit(digits, last_epoch=last_ep)
        # ticks that arrived live while we were downloading/probing are not in `ticks`: replay them
        if last_ep is not None:
            for ep, d in list(self._live.get(sym, ())):
                if ep > last_ep and pr.on_tick(d, ep):
                    self._recent_epochs[sym].append(ep)
        pred.register(sym, pr)
        self.predictors[sym] = pr
        return True

    async def _probe_ratios(self, sym: str, pr: pred.Predictor):
        """Live payout/stake for candidates that could clear the floor, so
        selection uses real break-evens (never a hardcoded payout)."""
        floor = pr.p.min_payout_ratio
        ceil_ = pr.p.max_payout_ratio
        cands = [c for c in pr.candidates
                 if pred.estimated_payout_ratio(c, pr.p.payout_factor) >= floor * 0.95
                 and (ceil_ <= 0 or pred.estimated_payout_ratio(c, pr.p.payout_factor) <= ceil_ * 1.05)]
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
        self._last_tick_wall[sym] = time.time()
        pr = self.predictors.get(sym)
        if pr is None:
            return
        digit = pred.last_digit(q, self._decimals[sym])
        if not pr.on_tick(digit, ep):
            return
        self._live[sym].append((ep, digit))
        self._last_epoch[sym] = ep
        eps = self._recent_epochs.setdefault(sym, [])
        eps.append(ep)
        del eps[:-50]
        self._events[sym].set()

    def _conn_count(self) -> int:
        return int(getattr(self.client, "connection_count", 0))

    def _needs_resync(self) -> bool:
        """True after a (re)connect until the predictor has been re-synced: ticks may have
        been missed, so the digit context would be wrong."""
        return self._conn_count() != self._seen_conn

    def _blocked(self) -> bool:
        # Only technical blocks remain: no socket, or predictor not yet re-synced after a
        # reconnect (a few seconds), or the optional scheduled redeploy (off by default).
        return (target_guard.is_reached()
                or restart_scheduler.is_redeploy_pending() or time.time() < self._halted_until
                or not self.client.is_connected or self._needs_resync())

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

    @staticmethod
    def _trade_cand(cand: pred.Candidate) -> pred.Candidate:
        """INVERT: buy the opposite parity of the predictor's pick (Even/Odd only)."""
        if _cfg("PREDICTOR_INVERT", False) and cand[0] in ("EVEN", "ODD"):
            return ("ODD" if cand[0] == "EVEN" else "EVEN", None)
        return cand

    def _contract_params(self, sym: str, cand: pred.Candidate, stake: float) -> dict:
        p = {"amount": stake, "basis": "stake",
             "contract_type": pred.contract_type(cand), "currency": "USD",
             "duration": int(self.duration), "duration_unit": "t", "underlying_symbol": sym}
        if cand[1] is not None:
            p["barrier"] = str(cand[1])
        return p

    async def _buy_direct(self, sym: str, cand: pred.Candidate, price: float) -> dict:
        """Buy without a proposal id: {"buy": "1", "price": max_price, "parameters": {...}}."""
        return await self.client._send(
            {"buy": "1", "price": price, "parameters": self._contract_params(sym, cand, self.stake)},
            timeout=10)

    async def _propose(self, sym: str, cand: pred.Candidate, stake: float) -> Optional[dict]:
        req = {"proposal": 1, **self._contract_params(sym, cand, stake)}
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
            tc = self._trade_cand(pk.cand)                  # side actually bought (flipped if INVERT)
            prop = await self._propose(sym, tc, self.stake)
            if not prop:
                continue
            payout = float(prop.get("payout") or 0.0)
            ratio = payout / self.stake                     # live ratio, never hardcoded
            self._ratios[sym][tc] = ratio
            dec = pr.accept(pk, ratio)                      # same gates: the predictor decides WHEN
            tag = f"{tc[0]}{'' if tc[1] is None else tc[1]}"
            if tc != pk.cand:
                tag += f"(inv {pk.cand[0]})"
            if not dec.ok:
                self._last_decision = f"skip {tag}: {dec.reason}"
                logger.info("SKIP %s %s: %s", sym, tag, dec.reason)
                continue
            if self._blocked():
                return
            ask = float(prop.get("ask_price") or self.stake)
            try:
                if self._direct_buy:
                    resp = await self._buy_direct(sym, tc, ask)
                else:
                    try:
                        resp = await self.client._send({"buy": prop["id"], "price": ask}, timeout=10)
                    except Exception as exc:
                        if "InvalidContractProposal" not in str(exc) and "Unknown contract proposal" not in str(exc):
                            raise
                        logger.warning("Buy by proposal id was refused (%s) - retrying with a direct buy "
                                       "(buy=1 + parameters)", str(exc)[:120])
                        resp = await self._buy_direct(sym, tc, ask)
                        self._direct_buy = True
                        logger.info("Direct buy worked - using it for all further trades")
            except Exception as exc:
                await self._on_api_error("buy", exc)        # error handling, not a gate
                return
            buy = resp.get("buy") or {}
            cid = str(buy.get("contract_id", ""))
            if not cid:
                logger.warning("BUY returned no contract_id: %s", resp)
                return
            opened = float(buy.get("purchase_time") or time.time())
            if _follower_mgr is not None:    # mirror to follower accounts: non-blocking, never raises
                try:
                    _follower_mgr.publish_signal({
                        "signal_id": cid, "symbol": sym, "contract_type": pred.contract_type(tc),
                        "barrier": tc[1], "duration": int(self.duration), "signal_time": time.time()})
                except Exception:
                    pass
            self._open[cid] = {"sym": sym, "cand": tc, "stake": self.stake, "ratio": ratio,
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

    # ── self-healing ─────────────────────────────────────────────────────────
    async def _supervise_ws(self, ws_task: "asyncio.Task"):
        """connect() is meant to run forever. If it ever ends, raise so main.py rebuilds the
        whole engine instead of leaving a zombie that is 'running' but never trades."""
        await ws_task
        if target_guard.is_reached():                  # intentional shutdown, not a crash
            await self._target_done.wait()
            return
        raise RuntimeError("Deriv WebSocket task ended unexpectedly")

    # ── balance target ───────────────────────────────────────────────────────
    def _on_balance(self, bal: float):
        """Runs on EVERY live balance update from Deriv (inside the event loop)."""
        self.balance = bal
        if target_guard.should_stop(bal) and target_guard.mark_reached(bal):
            # flag is already set (no new trade can start); do the slow part as a task
            asyncio.get_running_loop().create_task(self._shutdown_for_target(bal))

    async def _target_monitor_loop(self):
        """Safety net next to the balance stream: poll Deriv's live balance directly."""
        while not target_guard.is_reached():
            await asyncio.sleep(target_guard.check_seconds())
            if target_guard.target() is None or not self.client.is_connected:
                continue
            try:
                await self.client.fetch_balance_live()     # fires _on_balance -> target check
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("target monitor: balance poll failed: %s", exc)

    async def _shutdown_for_target(self, bal: float):
        """Target hit: (flag already set) -> drop the Deriv socket -> flush stats -> suspend Render."""
        try:
            logger.warning("TARGET SHUTDOWN: balance %.2f, %d contract(s) still open on Deriv "
                           "(they settle on Deriv's side; not waiting)", bal, len(self._open))
            try:
                import keep_alive
                keep_alive.update_status(session="TARGET REACHED - STOPPED",
                                         open_contracts_count=len(self._open))
            except Exception:
                pass
            await self.client.shutdown("balance target reached")
            try:
                await asyncio.wait_for(self.stats.flush(force=True), timeout=5)
            except Exception as exc:
                logger.warning("TARGET SHUTDOWN: stats flush skipped: %s", exc)
            await asyncio.to_thread(target_guard.suspend_service)
        except Exception as exc:
            logger.error("TARGET SHUTDOWN error: %s", exc, exc_info=True)
        finally:
            self._target_done.set()

    async def _watchdog_loop(self):
        """Tick streams can die silently while the socket (and its pings) look fine."""
        stale = float(_cfg("STALE_TICK_SECONDS", 30))
        min_gap = float(_cfg("FORCE_RECONNECT_MIN_GAP", 90))
        strikes = {s: 0 for s in self.symbols}
        grace_until = {s: 0.0 for s in self.symbols}
        while True:
            await asyncio.sleep(5)
            now = time.time()
            if not self.client.is_connected:
                for s in self.symbols:                       # downtime is not "stale"
                    self._last_tick_wall[s] = now
                    strikes[s] = 0
                continue
            for sym in self.symbols:
                quiet = now - self._last_tick_wall.get(sym, now)
                if quiet < stale:                            # a real tick arrived: healthy
                    strikes[sym] = 0
                    continue
                if now < grace_until[sym]:                   # give the last fix time to work
                    continue
                try:
                    if strikes[sym] == 0:
                        logger.warning("WATCHDOG %s: no tick for %.0fs - re-subscribing", sym, quiet)
                        strikes[sym] = 1
                        grace_until[sym] = now + stale
                        await self.client.refresh_ticks(sym, lambda tick, s=sym: self._on_tick(s, tick))
                    elif now - self._last_forced_reconnect >= min_gap:
                        logger.warning("WATCHDOG %s: still no tick after re-subscribe - reconnecting", sym)
                        self._last_forced_reconnect = now
                        strikes[sym] = 0
                        grace_until[sym] = now + stale
                        await self.client.force_reconnect(f"no ticks for {sym}")
                except Exception as exc:
                    logger.error("watchdog error %s: %s", sym, exc)

    async def _refit_loop(self):
        """Replaces what the periodic redeploy did implicitly: fresh history, fresh payout
        probe, fresh selection - plus an immediate re-sync after any reconnect."""
        interval = float(_cfg("REFIT_INTERVAL_SECONDS", 300))
        last = time.time()
        while True:
            await asyncio.sleep(5)
            if not self.client.is_connected:
                continue
            resync = self._needs_resync()
            if not resync and not (interval > 0 and time.time() - last >= interval):
                continue
            conn_now = self._conn_count()
            ok_all = True
            for sym in self.symbols:
                try:
                    ok = await self._fit_symbol(sym)
                    ok_all = ok_all and ok
                    if ok:
                        pr = self.predictors[sym]
                        logger.info("%s %s: tradable contexts=%d",
                                    "RESYNC" if resync else "REFIT", sym, len(pr.selected))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    ok_all = False
                    logger.error("refit error %s: %s", sym, exc)
            if ok_all:
                if resync and self._conn_count() == conn_now:
                    self._seen_conn = conn_now
                last = time.time()

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
        if _follower_db is not None:         # batched DB write; never blocks, never raises
            try:
                _follower_db.record_collector_trade(info["opened_at"], info["stake"], info["ratio"], profit)
            except Exception:
                pass
        tag = f"{info['cand'][0]}{'' if info['cand'][1] is None else info['cand'][1]}"
        logger.info("CONTRACT SETTLED %s %s %s %s profit=%.2f", cid, info["sym"], tag,
                    "WIN" if won else "LOSS", profit)
        self._note_lag(info, poc)
        self._after_trade(info, profit, won)
        try:
            import keep_alive
            keep_alive.record_trade(info["sym"], tag, info["stake"], profit,
                                    balance_after=self.client.balance, won=won, strategy="PREDICTOR")
        except Exception:
            pass

    # ── performance log (no limits, no pauses) ───────────────────────────────
    def _after_trade(self, info: dict, profit: float, won: bool):
        self._n_trades += 1
        self._n_wins += 1 if won else 0
        ratio = max(float(info.get("ratio") or 1.0), 1.0001)
        self._be_sum += 1.0 / ratio
        self._total_pnl += profit
        # Edge test: compare actual results with a FAIR game (true digit probabilities, real payout).
        # z near 0 = results are what luck alone produces; z > 3 over many trades = real edge.
        try:
            p_true = pred.true_probability(info["cand"])
            self._fair_mean += p_true * ratio - 1.0
            self._fair_var += p_true * (1.0 - p_true) * ratio * ratio
        except Exception:
            pass
        if self._n_trades % 50 == 0:
            n = self._n_trades
            stakes_won = self._total_pnl / max(self.stake, 1e-9)
            z = (stakes_won - self._fair_mean) / (self._fair_var ** 0.5) if self._fair_var > 0 else 0.0
            logger.info("PERFORMANCE after %d trades: win rate %.1f%% vs break-even %.1f%% | total P&L %+.2f "
                        "| expected for a fair game %+.2f | EDGE z-score %+.2f (needs > 3 to be an edge)",
                        n, 100.0 * self._n_wins / n, 100.0 * self._be_sum / n, self._total_pnl,
                        self._fair_mean * self.stake, z)

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
                    open_contracts_count=len(self._open),
                    target_balance=target_guard.target())
                keep_alive.set_active_trades(len(self._open))
            except Exception:
                pass
