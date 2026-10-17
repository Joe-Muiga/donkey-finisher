"""
bot_engine.py - base engine: Deriv connection, stake rules, settlement, redeploy handling, dashboard.

Strategy-free. The Donkey strategy lives in donkey_engine.py (DonkeyEngine subclasses this class).

Session lifecycle (window_scheduler.py, judged on Deriv's server clock): ONE engine instance serves ONE session:
connect at the connect lead -> authorise, subscribe, warm up, measure the clock (clock_sync.py) -> buy from the open
instant to the close instant -> bounded drain -> hard disconnect.  main.py builds a fresh engine for the next session.

Self-healing: the Deriv client reconnects forever with backoff, a watchdog re-subscribes / reconnects
when a tick stream goes quiet, per-symbol state is re-synced after every reconnect (`_refit_loop` ->
`_resync_symbol`), and main.py restarts the whole engine if anything fatal escapes.

There are no trading gates in this layer: no loss/drawdown/streak limits, no cooldowns, no suspensions.
Only technical necessities remain: reconnect (deriv_client), auth, rate-limit backoff, error logging,
the session gate (send-time, in deriv_client), TARGET_BALANCE and the dashboard kill switch.
"""
import asyncio
import collections
import logging
import statistics
import time
from typing import Dict, List, Optional

import clock_sync
import config
import digit_utils as du
import main_money
import predictor_stats
import target_guard
import window_scheduler as ws
from deriv_client import DerivClient

try:                                    # follower system is optional; the main account never depends on it
    import db as _follower_db
except Exception:                       # pragma: no cover
    _follower_db = None
try:
    import follower_manager as _follower_mgr
except Exception:                       # pragma: no cover
    _follower_mgr = None

logger = logging.getLogger(__name__)


def _fmt_ms(ts: float) -> str:
    """Epoch -> 'HH:MM:SS.mmm' (UTC)."""
    return time.strftime("%H:%M:%S", time.gmtime(ts)) + f".{int((ts % 1) * 1000):03d}"


def _cfg(name, default):
    return getattr(config, name, default)


class BotEngine:
    def __init__(self):
        self.client = DerivClient()
        self.symbols: List[str] = list(_cfg("PREDICTOR_SYMBOLS", ["R_100"]))
        self.stake = float(_cfg("PREDICTOR_STAKE", 0.35))     # MINIMUM stake (env) - also the probe stake
        self.money = main_money.MainMoney()                  # profit-securing money rules (floor / stake sizing)
        self.duration = int(_cfg("CONTRACT_DURATION_TICKS", 1))
        self.stats = predictor_stats.get_stats()
        self._decimals: Dict[str, int] = {}
        self._events: Dict[str, asyncio.Event] = {}
        self._open: Dict[str, dict] = {}
        self._recent_epochs: Dict[str, List[float]] = {}
        self._last_epoch: Dict[str, float] = {}
        self._sess: Optional[ws.Session] = None   # the session this engine serves (connection window)
        self._phase = "connecting"
        self._symbols_ready = False
        self._ready = False                       # symbols warmed up AND Deriv clock measured
        self._ready_at = 0.0                      # server time at which that became true
        self._clock = clock_sync.ClockEstimator(floored=bool(_cfg("DERIV_TIME_IS_FLOORED", True)))
        self._lag_obs: List[int] = []
        self._last_decision = "—"
        self.balance = 0.0
        self._target_done = asyncio.Event()   # set once the TARGET_BALANCE / window-end shutdown has finished
        self._session_over = False            # session closed: no buys, socket dropped, engine returns
        self._feed_followers = True           # DonkeyEngine turns this off (no follower stats / mirroring)
        # ── self-healing state ──
        self._last_tick_wall: Dict[str, float] = {}
        self._seen_conn = 0                                 # client.connection_count we are synced to
        self._last_forced_reconnect = 0.0
        self._worker_started: set = set()      # symbols whose worker an _init_symbols() already started
        self._early_tasks: list = []
        # ── performance tracking (log only; nothing here ever blocks a trade) ──
        self._n_trades = 0
        self._n_wins = 0
        self._be_sum = 0.0            # sum of break-even win probabilities (1/ratio) of trades taken
        self._fair_mean = 0.0         # sum of expected profit (in stakes) if every digit were fair
        self._fair_var = 0.0          # sum of variance (in stakes^2) under the same assumption
        self._total_pnl = 0.0
        self._stakes_won = 0.0        # sum of profit/stake (stakes vary with the money rules)

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def run(self):
        logger.info("=" * 64)
        logger.info("  Digit bot  –  min stake %.2f (PREDICTOR_STAKE), symbols %s", self.stake, self.symbols)
        logger.info("=" * 64)
        ws.reset_clock()                                  # this connection measures the Deriv clock from scratch
        self._sess = ws.connection_session()
        if self._sess is None:
            logger.error("ENGINE START REFUSED: no session connection window is open right now (fail closed)")
            return
        self._set_phase("connecting")
        ws_task = asyncio.create_task(self.client.connect())
        while not self.client.is_connected and not ws_task.done():     # connect() retries until the window ends
            await asyncio.sleep(0.1)
        if not self.client.is_connected:
            logger.error("Could not connect/authorise before the connection window ended")
            ws_task.cancel()
            return
        self._set_phase("warming")
        clock_task = asyncio.create_task(self._clock_sync_loop())     # starts measuring at once, parallel to the warm-up
        session_task = asyncio.create_task(self._session_loop())      # open/close markers, the 2-s readiness check, the close
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
                clock_task.cancel()
                session_task.cancel()
                return
        else:
            logger.info("TARGET GUARD: TARGET_BALANCE not set - no balance target")
        try:
            await self.money.start(self.client)          # saved floor/P, live balance, midnight reset
        except Exception as exc:
            logger.error("MAIN MONEY could not start (%s) - the bot will not trade until it can", exc)
        self.client.on_balance(self._on_balance)
        self._seen_conn = self._conn_count()

        ready = await self._init_symbols()
        self.symbols = ready
        self._symbols_ready = bool(ready)
        if not self.symbols:
            raise RuntimeError("no tradable symbol could be started")
        logger.info("SYMBOLS ACTIVE (%d): %s", len(self.symbols), ",".join(self.symbols))

        tasks = [asyncio.create_task(self._worker(s)) for s in self.symbols if s not in self._worker_started]
        tasks += self._early_tasks
        tasks += [asyncio.create_task(self._settle_loop()),
                  asyncio.create_task(self._flush_loop()),
                  asyncio.create_task(self._dashboard_loop()),
                  asyncio.create_task(self._watchdog_loop()),
                  asyncio.create_task(self._refit_loop()),
                  asyncio.create_task(self._target_monitor_loop()),
                  session_task,
                  clock_task,
                  asyncio.create_task(self._supervise_ws(ws_task))]
        tasks += [asyncio.create_task(c) for c in self._extra_tasks()]
        self._check_ready()
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

    async def _init_symbols(self) -> list:
        """Initialise all symbols one after another (DonkeyEngine overrides this with a concurrent boot)."""
        ready = []
        for sym in list(self.symbols):
            ok = False
            for attempt in (1, 2):                  # one retry: a hiccup must not drop a good symbol
                try:
                    await self._init_symbol(sym)
                    ok = True
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("SYMBOL %s start-up attempt %d failed: %s", sym, attempt, exc)
                    await asyncio.sleep(3)
            if ok:
                ready.append(sym)
            else:
                logger.error("SYMBOL SKIPPED %s: no tick history / no digit contract could be priced "
                             "(not offered on this account?)", sym)
                for d in (self._events, self._decimals, self._last_tick_wall, self._recent_epochs):
                    d.pop(sym, None)
        return ready

    def _extra_tasks(self) -> list:
        """Coroutines a subclass wants run next to the standard loops (none for the original engine)."""
        return []

    # ── per-symbol hooks (DonkeyEngine implements them) ──────────────────────
    async def _init_symbol(self, sym: str):
        raise NotImplementedError

    async def _resync_symbol(self, sym: str) -> None:
        """Rebuild one symbol's state from history after a reconnect."""
        return

    async def _worker(self, sym: str):
        raise NotImplementedError

    def _armed_count(self) -> int:
        return 0

    def _on_settled(self, cid: str, info: dict, poc: dict, profit: float, won: bool) -> None:
        """Hook: called for every settled contract before the performance log (no-op here)."""
        return

    # ── ticks / reconnect bookkeeping ──────────────────────────────────────────
    def _conn_count(self) -> int:
        return int(getattr(self.client, "connection_count", 0))

    def _needs_resync(self) -> bool:
        """True after a (re)connect until per-symbol state has been re-synced: ticks may have
        been missed, so the digit windows would be wrong."""
        return self._conn_count() != self._seen_conn

    def _gate_reason(self) -> Optional[str]:
        """Why a buy may not be decided/sent right now according to the schedule (None = the gate is open).
        Judged on Deriv's server clock; deriv_client._send repeats the check at the moment of sending."""
        ok, why = ws.buy_gate()
        return None if ok else why

    def _next_stake(self) -> Optional[float]:
        """Stake from the money rules for the next buy; None = stopped (floor reached) or not ready."""
        return self.money.next_stake()

    def _blocked(self) -> bool:
        # Only technical blocks remain: the session gate, no socket, or state not yet re-synced after a
        # reconnect (a few seconds).
        return (self._gate_reason() is not None or target_guard.is_reached() or self._session_over
                or not self.client.is_connected or self._needs_resync())

    # ── trading ──────────────────────────────────────────────────────────────
    def _contract_params(self, sym: str, cand: du.Candidate, stake: float, duration: Optional[int] = None) -> dict:
        p = {"amount": stake, "basis": "stake",
             "contract_type": du.contract_type(cand), "currency": "USD",
             "duration": int(self.duration if duration is None else duration), "duration_unit": "t", "underlying_symbol": sym}
        if cand[1] is not None:
            p["barrier"] = str(cand[1])
        return p

    async def _buy_direct(self, sym: str, cand: du.Candidate, price: float, stake: Optional[float] = None,
                          duration: Optional[int] = None, timeout: float = 10) -> dict:
        """Buy without a proposal id: {"buy": "1", "price": max_price, "parameters": {...}}."""
        return await self.client._send(
            {"buy": "1", "price": price, "parameters": self._contract_params(sym, cand, self.stake if stake is None else stake, duration)},
            timeout=timeout)

    async def _propose(self, sym: str, cand: du.Candidate, stake: float, duration: Optional[int] = None) -> Optional[dict]:
        req = {"proposal": 1, **self._contract_params(sym, cand, stake, duration)}
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

    # ── self-healing ─────────────────────────────────────────────────────────
    async def _supervise_ws(self, ws_task: "asyncio.Task"):
        """connect() is meant to run forever. If it ever ends, raise so main.py rebuilds the
        whole engine instead of leaving a zombie that is 'running' but never trades."""
        await ws_task
        if target_guard.is_reached() or self._session_over:   # intentional shutdown, not a crash
            await self._target_done.wait()
            return
        raise RuntimeError("Deriv WebSocket task ended unexpectedly")

    # ── session lifecycle: connect lead -> warm-up -> open -> close -> drain -> hard disconnect ────────────
    def _set_phase(self, phase: str) -> None:
        """connecting / warming / ready (the dashboard turns it into CONNECTING / WARMING UP / SESSION x LIVE)."""
        self._phase = phase
        try:
            import keep_alive
            keep_alive.update_status(engine_phase=phase)
        except Exception:
            pass

    def _check_ready(self) -> None:
        """Ready = symbols warmed up AND the Deriv clock measured (the buy gate also needs the clock)."""
        if self._ready or not self.symbols or self._phase == "connecting":
            return
        if not ws.clock_measured() or not getattr(self, "_symbols_ready", False):
            return
        self._ready = True
        self._ready_at = ws.now()
        self._set_phase("ready")
        sess = self._sess
        if sess is not None:
            d = sess.open - self._ready_at
            if d >= 0:
                logger.info("ENGINE READY %.1fs before Session %s opens (%s)", d, sess.slot, ws.clock_status())
            else:
                logger.critical("ENGINE READY %.1fs AFTER Session %s opened - those seconds of the session were not tradable "
                                "(raise WINDOW_PRECONNECT_SECONDS in config.py if this repeats)", -d, sess.slot)

    async def _sleep_until(self, server_ts: float) -> None:
        """Sleep until Deriv-server time `server_ts` (re-evaluated against the live clock offset, ms-precise)."""
        while True:
            rem = server_ts - ws.now()
            if rem <= 0:
                return
            await asyncio.sleep(rem if rem < 0.004 else (0.002 if rem < 0.05 else min(rem - 0.04, 1.0)))

    async def _session_loop(self):
        """The marker task of the session this engine serves: readiness check 2 s before the open, the OPEN line with the
        clock offset/error, the CLOSE line, then the drain and the hard disconnect."""
        sess = self._sess
        assert sess is not None
        try:
            if ws.now() < sess.open - 2.0:
                await self._sleep_until(sess.open - 2.0)
                self._check_ready()
                if not self._ready:
                    logger.critical("SESSION %s NOT READY 2 s BEFORE OPEN (phase=%s, clock measured=%s, symbols ready=%s): the gate will "
                                    "NOT open early - buying starts only when the engine is ready", sess.slot, self._phase,
                                    ws.clock_measured(), getattr(self, "_symbols_ready", False))
            if ws.now() < sess.open:
                await self._sleep_until(sess.open)
            logger.warning("SESSION %s OPEN: offset %+.2fs, error \u00b1%.2fs | server time %s | ready=%s | gate=%s",
                           sess.slot, ws.clock_offset(), ws.clock_error() if ws.clock_error() is not None else float("nan"),
                           _fmt_ms(ws.now()), self._ready, self._gate_reason() or "OPEN")
            await self._sleep_until(sess.close)
            self._session_over = True                   # _blocked() is now True: no new proposals/buys
            logger.warning("SESSION %s CLOSED: offset %+.2fs, error \u00b1%.2fs | server time %s | open contracts %d",
                           sess.slot, ws.clock_offset(), ws.clock_error() if ws.clock_error() is not None else float("nan"),
                           _fmt_ms(ws.now()), len(self._open))
            await self._end_session(sess)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("session loop error: %s", exc, exc_info=True)
            raise

    async def _clock_sync_loop(self):
        """Measure Deriv-server-minus-local clock to sub-second precision (clock_sync.py): a bisecting burst right after
        connect, a second quiet-network burst shortly before the open, and one probe every CLOCK_REFRESH_SECONDS."""
        est = self._clock
        probe = lambda: clock_sync.probe_client(self.client)          # noqa: E731
        max_probes = int(_cfg("CLOCK_BURST_MAX_PROBES", 10))
        budget = float(_cfg("CLOCK_BURST_BUDGET_SECONDS", 12.0))
        min_gap = float(_cfg("CLOCK_PROBE_MIN_GAP_SECONDS", 0.3))
        refresh = float(_cfg("CLOCK_REFRESH_SECONDS", 30.0))
        pre_open = float(_cfg("CLOCK_PRE_OPEN_BURST_SECONDS", 20.0))
        pre_done = False
        last_probe = 0.0

        def publish(e, why: str) -> None:
            if e is None:
                return
            if abs(e.offset) > float(_cfg("CLOCK_MAX_ABS_OFFSET_SECONDS", 120.0)):
                logger.critical("CLOCK: offset %+.1fs between this machine and Deriv is absurd - ignored, buying stays blocked", e.offset)
                return
            prev = ws.clock_offset() if ws.clock_measured() else None
            ws.set_clock_offset(e.offset, e.n_used, e.error, e.method)
            if prev is None or abs(e.offset - prev) > 0.05 or why != "refresh":
                logger.info("CLOCK %s: %s", why, e.describe())
            if abs(e.offset) > float(_cfg("CLOCK_WARN_OFFSET_SECONDS", 1.5)):
                logger.warning("CLOCK: this machine's clock differs from Deriv by %+.2fs - sessions are judged on Deriv's clock", -e.offset)
            if e.error > float(_cfg("CLOCK_MAX_ERROR_FOR_OK", 0.25)):
                logger.warning("CLOCK: estimate error \u00b1%.2fs is larger than %.2fs (%s)", e.error,
                               float(_cfg("CLOCK_MAX_ERROR_FOR_OK", 0.25)), e.method)
            self._check_ready()

        while not self._session_over:
            try:
                if self.client.is_connected:
                    sess = self._sess
                    till_open = (sess.open - ws.now()) if sess is not None else -1.0
                    if last_probe == 0.0:
                        e = await clock_sync.sync_burst(est, probe, max_probes=max_probes, budget=budget, min_gap=min_gap,
                                                        is_open=lambda: self.client.is_connected and not self._session_over)
                        publish(e, "initial burst")
                        last_probe = time.time()
                    elif not pre_done and 3.0 < till_open <= pre_open:
                        pre_done = True
                        e = await clock_sync.sync_burst(est, probe, max_probes=max_probes,
                                                        budget=max(1.0, min(budget, till_open - 2.0)), min_gap=min_gap,
                                                        is_open=lambda: self.client.is_connected and not self._session_over)
                        publish(e, "pre-open burst")
                        last_probe = time.time()
                    elif time.time() - last_probe >= refresh and not (0.0 < till_open < 3.0):
                        e = await clock_sync.single_probe(est, probe)
                        publish(e, "refresh")
                        last_probe = time.time()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("clock sync failed: %s", exc)
                last_probe = last_probe or time.time()
            await asyncio.sleep(0.5)

    def _pending_work(self) -> int:
        """Things that still need the connection after the close (open contracts; DonkeyEngine adds in-doubt buys)."""
        return len(self._open)

    async def _end_session(self, sess: Optional[ws.Session] = None):
        """Close reached: no new buys; wait for open contracts to settle, but never past the drain deadline
        (close + SESSION_DRAIN_SECONDS), flush stats, then CLOSE THE WEBSOCKET."""
        sess = sess or self._sess
        self._session_over = True
        deadline = sess.drain_end if sess is not None else ws.now() + float(_cfg("SESSION_DRAIN_SECONDS", 10))
        logger.warning("SESSION DRAIN: no new trades; waiting up to %.1fs for %d open contract(s) to settle",
                       max(0.0, deadline - ws.now()), self._pending_work())
        try:
            import keep_alive
            keep_alive.update_status(open_contracts_count=len(self._open))
        except Exception:
            pass
        while self._pending_work() and ws.now() < deadline - 0.25:
            await asyncio.sleep(0.2)
        try:
            await asyncio.wait_for(self.stats.flush(force=True), timeout=2)
        except Exception as exc:
            logger.warning("session end: stats flush skipped: %s", exc)
        left = self._pending_work()
        await self.client.shutdown("session closed")
        logger.warning("SESSION ENDED: disconnected from Deriv at server time %s (%d contract(s)/buy(s) left to settle on Deriv's side)",
                       _fmt_ms(ws.now()), left)
        self._target_done.set()

    # ── balance target ───────────────────────────────────────────────────────
    def _on_balance(self, bal: float):
        """Runs on EVERY live balance update from Deriv (inside the event loop)."""
        self.balance = bal
        self.money.on_balance(bal)
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
        """After any reconnect ticks may have been missed: re-sync every symbol's digit/candle state
        from history. There is NO periodic refit (nothing is fitted any more)."""
        while True:
            await asyncio.sleep(5)
            if not self.client.is_connected or not self._needs_resync():
                continue
            conn_now = self._conn_count()
            ok_all = True
            for sym in list(self.symbols):
                try:
                    await self._resync_symbol(sym)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    ok_all = False
                    logger.error("resync error %s: %s", sym, exc)
            if ok_all and self._conn_count() == conn_now:
                self._seen_conn = conn_now

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
        if _follower_db is not None and self._feed_followers:   # batched DB write; never blocks, never raises
            try:
                _follower_db.record_collector_trade(info["opened_at"], info["stake"], info["ratio"], profit)
            except Exception:
                pass
        tag = f"{info['cand'][0]}{'' if info['cand'][1] is None else info['cand'][1]}"
        logger.info("CONTRACT SETTLED %s %s %s %s profit=%.2f", cid, info["sym"], tag,
                    "WIN" if won else "LOSS", profit)
        try:
            self._on_settled(cid, info, poc, profit, won)
        except Exception as exc:                          # a reporting hook must never break settlement
            logger.error("settlement hook error %s: %s", cid, exc, exc_info=True)
        self._note_lag(info, poc)
        self._after_trade(info, profit, won)
        try:
            import keep_alive
            keep_alive.record_trade(info["sym"], tag, info["stake"], profit,
                                    balance_after=self.client.balance, won=won, strategy="DONKEY")
        except Exception:
            pass

    # ── performance log (no limits, no pauses) ───────────────────────────────
    def _after_trade(self, info: dict, profit: float, won: bool):
        self._n_trades += 1
        self._n_wins += 1 if won else 0
        ratio = max(float(info.get("ratio") or 1.0), 1.0001)
        self._be_sum += 1.0 / ratio
        self._total_pnl += profit
        self._stakes_won += profit / max(float(info.get("stake") or self.stake), 1e-9)
        # Edge test: compare actual results with a FAIR game (true digit probabilities, real payout).
        # z near 0 = results are what luck alone produces; z > 3 over many trades = real edge.
        try:
            p_true = du.true_probability(info["cand"])
            self._fair_mean += p_true * ratio - 1.0
            self._fair_var += p_true * (1.0 - p_true) * ratio * ratio
        except Exception:
            pass
        if self._n_trades % 50 == 0:
            n = self._n_trades
            stakes_won = self._stakes_won
            z = (stakes_won - self._fair_mean) / (self._fair_var ** 0.5) if self._fair_var > 0 else 0.0
            logger.info("PERFORMANCE after %d trades: win rate %.1f%% vs break-even %.1f%% | total P&L %+.2f "
                        "| expected for a fair game %+.2f | EDGE z-score %+.2f (needs > 3 to be an edge)",
                        n, 100.0 * self._n_wins / n, 100.0 * self._be_sum / n, self._total_pnl,
                        self._fair_mean * self.stake, z)

    def _note_lag(self, info: dict, poc: dict):
        """Entry tick vs decision tick (log only). The complement must enter on the tick right AFTER the
        decision tick (gap == 1 tick); that is how we verify we buy on the same tick as the crowd."""
        try:
            entry = float(poc.get("entry_tick_time") or 0)
            dec = info.get("decision_epoch")
            eps = self._recent_epochs.get(info["sym"], [])
            if not entry or not dec or len(eps) < 5:
                return
            interval = statistics.median([b - a for a, b in zip(eps, eps[1:]) if b > a] or [1])
            gap = round((entry - dec) / interval)
            self._lag_obs.append(gap)
            if gap != 1:
                logger.warning("LAG CHECK %s %s: entry tick was %d ticks after the decision tick (expected 1) "
                               "- the entry was NOT on the crowd's tick", info["sym"], info.get("pair") or "-", gap)
        except Exception:
            pass

    async def _settle_loop(self):
        while True:
            await asyncio.sleep(2)
            now = time.time()
            for cid, info in list(self._open.items()):         # poll stragglers
                if now - info["t0"] > 20:
                    poc = await self.client.force_check_contract(cid)
                    if poc:
                        await self._on_contract_result(cid, {"proposal_open_contract": poc})

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
                    balance=round(self.client.balance, 2),
                    current_symbol=",".join(self.symbols), last_signal=self._last_decision,
                    armed_pairs=self._armed_count(),
                    open_contracts_count=len(self._open),
                    target_balance=target_guard.target(), money=self.money.snapshot())
                keep_alive.set_active_trades(len(self._open))
            except Exception:
                pass
