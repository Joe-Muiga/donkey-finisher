"""
bot_engine.py - base engine: Deriv connection, stake rules, settlement, exact session gate, dashboard.

Strategy-free. The Donkey strategy lives in donkey_engine.py (DonkeyEngine subclasses this class).

Self-healing: the Deriv client reconnects forever with backoff, a watchdog re-subscribes / reconnects
when a tick stream goes quiet, per-symbol state is re-synced after every reconnect (`_refit_loop` ->
`_resync_symbol`), and main.py restarts the whole engine if anything fatal escapes.

There are no trading gates in this layer: no loss/drawdown/streak limits, no cooldowns, no suspensions.
Only technical necessities remain: reconnect (deriv_client), auth, rate-limit backoff, error logging,
the two exact trade sessions per hour (window_scheduler, Deriv server clock), draining open contracts after a
session close, TARGET_BALANCE and the dashboard kill switch.
One engine instance serves ONE session: main.py builds a fresh engine for every session and the engine
disconnects from Deriv when its session closes.
"""
import asyncio
import collections
import logging
import statistics
import time
from typing import Dict, List, Optional

import config
import digit_utils as du
import main_money
import predictor_stats
import target_guard
import window_scheduler
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


def _cfg(name, default):
    return getattr(config, name, default)


def _utc(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.gmtime(ts)) + ("%.3f" % (ts % 1.0))[1:]


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
        self._lag_obs: List[int] = []
        self._last_decision = "—"
        self.balance = 0.0
        self._target_done = asyncio.Event()   # set once the TARGET_BALANCE / window-end shutdown has finished
        self._session_over = False            # trade session closed: no buys, socket dropped, engine returns
        self._window_seen = False             # the session this engine serves has actually opened (and been traded)
        self._money_ready = False             # money.start finished
        self._boot_done = False               # symbols warmed up and all loops started
        self._stop_at_close: list = []        # tasks that talk to Deriv: cancelled the instant the session closes
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
        sessions_on = bool(_cfg("TRADE_WINDOW_ENABLED", False))
        ws_task = asyncio.create_task(self.client.connect())
        waited = 0.0
        while not self.client.is_connected:
            if sessions_on:                      # keep retrying (DerivClient backs off) until the session is over
                if not window_scheduler.should_be_connected(window_scheduler.now()):
                    break
            elif waited >= 60:
                break
            await asyncio.sleep(0.5)
            waited += 0.5
        if not self.client.is_connected:
            logger.error("Could not connect/authorise to Deriv (%s)",
                         "the session is over" if sessions_on else "within 60 s")
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
        session_tasks = []                       # clock sync + readiness alarm (sessions only)
        if sessions_on:                          # Deriv server-clock offset: measured while the symbols warm up
            session_tasks = [asyncio.create_task(self._cancellable(self._clock_sync())),
                             asyncio.create_task(self._cancellable(self._readiness_watch()))]
        try:
            await self.money.start(self.client)          # saved floor/P, live balance, midnight reset
            self._money_ready = True
        except Exception as exc:
            logger.error("MAIN MONEY could not start (%s) - the bot will not trade until it can", exc)
        self.client.on_balance(self._on_balance)
        self._seen_conn = self._conn_count()

        ready = await self._init_symbols()
        self.symbols = ready
        if not self.symbols:
            raise RuntimeError("no tradable symbol could be started")
        logger.info("SYMBOLS ACTIVE (%d): %s", len(self.symbols), ",".join(self.symbols))

        t_watchdog = asyncio.create_task(self._cancellable(self._watchdog_loop()))
        t_refit = asyncio.create_task(self._cancellable(self._refit_loop()))
        t_monitor = asyncio.create_task(self._cancellable(self._target_monitor_loop()))
        extra = [asyncio.create_task(self._cancellable(c)) for c in self._extra_tasks()]
        tasks = [asyncio.create_task(self._worker(s)) for s in self.symbols if s not in self._worker_started]
        tasks += self._early_tasks
        tasks += [asyncio.create_task(self._settle_loop()),
                  asyncio.create_task(self._flush_loop()),
                  asyncio.create_task(self._dashboard_loop()),
                  t_watchdog, t_refit, t_monitor,
                  asyncio.create_task(self._window_watch_loop()),
                  asyncio.create_task(self._supervise_ws(ws_task))]
        tasks += extra + session_tasks
        # Everything below talks to Deriv: it dies the instant the session closes (only settlement polling and
        # the in-doubt reconciles keep going through the bounded drain).
        self._stop_at_close = [t_watchdog, t_refit, t_monitor] + extra + session_tasks
        if _follower_mgr is not None:        # Deriv server-time offset for the follower scheduler (main account time)
            tasks.append(asyncio.create_task(_follower_mgr.collector_time_sync(self.client)))
        self._boot_done = True
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

    async def _cancellable(self, coro):
        """Run `coro`; if _end_session cancels it at the session close that is a normal end, not a cancelled task
        (a cancelled child would make the gather in run() finish early and cut the drain short)."""
        try:
            return await coro
        except asyncio.CancelledError:
            if self._session_over:
                return None
            raise

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

    @staticmethod
    def _in_trade_window(ts: Optional[float] = None) -> bool:
        """True when `ts` (epoch seconds on DERIV'S SERVER CLOCK, default window_scheduler.now()) is inside one of the
        two sessions: open <= ts < close, exact to the float second (window_scheduler.in_window)."""
        if not _cfg("TRADE_WINDOW_ENABLED", False):
            return True
        return window_scheduler.in_window(window_scheduler.now() if ts is None else ts)

    @staticmethod
    def _server_now() -> float:
        return window_scheduler.now()

    def _send_block_reason(self) -> Optional[str]:
        """Checked AGAIN at the instant a buy is handed to the socket: a decision taken before the close must not be
        transmitted after it. None = clear."""
        if self._session_over:
            return "session_over"
        if target_guard.is_reached():
            return "target_reached"
        if not self._in_trade_window(window_scheduler.now()):
            return "outside_trade_window"
        return None

    def _next_stake(self) -> Optional[float]:
        """Stake from the money rules for the next buy; None = stopped (floor reached) or not ready."""
        return self.money.next_stake()

    def _blocked(self) -> bool:
        # Only technical blocks remain: outside the session, no socket, or state not yet re-synced after a
        # reconnect (a few seconds).
        return (not self._in_trade_window(window_scheduler.now()) or target_guard.is_reached() or self._session_over
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
        """Buy without a proposal id: {"buy": "1", "price": max_price, "parameters": {...}}.
        The session gate is re-checked here, immediately before the frame is written: nothing is sent after the close."""
        why = self._send_block_reason()
        if why:
            raise RuntimeError(f"BUY_GATE_CLOSED ({why}): buy not sent")
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

    # ── trade session: connected only for the session, exact open / exact close, then drain + disconnect ──
    async def _sleep_until_server(self, target: float):
        """Sleep until the SERVER clock reaches `target`, re-reading the clock each step (an offset refresh mid-wait is
        honoured). Halving steps converge to the instant within a few milliseconds."""
        while True:
            rem = target - window_scheduler.now()
            if rem <= 0:
                return
            await asyncio.sleep(rem / 2.0 if rem > 0.004 else rem)

    def _log_session_open(self, name: str, open_ts: float) -> None:
        info = window_scheduler.clock_info()
        if info["measured"]:
            logger.warning("SESSION %s OPEN at server %s UTC | clock offset %+.3fs, %d sample(s), est. error +-%.3fs",
                           name, _utc(window_scheduler.now()), info["offset"], info["samples"], info["err"] or 0.0)
        else:
            logger.critical("SESSION %s OPEN at LOCAL %s UTC but NO Deriv server-clock sample was obtained: falling back "
                            "to the local clock (offset 0). The gate was not opened early and the session is not skipped.",
                            name, _utc(window_scheduler.now()))
        self._window_seen = True

    async def _window_watch_loop(self):
        """Exact timing for this engine's ONE session: wait for the exact open instant (if it is still ahead), then sleep to
        the exact close instant and end the session within a few milliseconds of it. (The gate itself is evaluated on
        every decision and every send, so it flips at the exact instant whatever this loop's wake-up latency is.)"""
        if not _cfg("TRADE_WINDOW_ENABLED", False):
            return                                          # sessions off: trade all hours, never ends by itself
        ws = window_scheduler
        t = ws.now()
        if ws.session_at(t) is None and ws.seconds_until_open(t) > ws._preconnect() + 5.0:
            # this engine was started for a session that is already over (it connected too late): never sit
            # connected through a gap
            logger.warning("SESSION already over before this engine was ready: disconnecting")
            await self._end_session()
            return
        while True:
            t = ws.now()
            sess = ws.session_at(t)
            if sess is None:
                name, open_ts, _ = ws.next_session(t)
                await self._sleep_until_server(open_ts)
                self._log_session_open(name, open_ts)
                continue
            if not self._window_seen:
                self._log_session_open(sess[0], sess[1])
            await self._sleep_until_server(sess[2])
            if not ws.in_window(ws.now()):
                break
        await self._end_session()

    async def _readiness_watch(self):
        """Warm-up must be complete BEFORE the open instant: 2 s before it, say loudly what is still pending."""
        ws = window_scheduler
        t = ws.now()
        if ws.session_at(t) is not None:
            return                                          # already inside a session: nothing to pre-warn about
        name, open_ts, _ = ws.next_session(t)
        await self._sleep_until_server(open_ts - 2.0)
        pending = self._pending_readiness()
        if pending:
            logger.critical("NOT READY 2 s BEFORE SESSION %s OPENS (%s UTC): pending = %s. The gate will NOT open early; "
                            "trading starts when these clear.", name, _utc(open_ts), ", ".join(pending))
        else:
            logger.info("READY 2 s before session %s opens (%s UTC): connected, warmed up, resynced, clock measured",
                        name, _utc(open_ts))

    def _pending_readiness(self) -> List[str]:
        p = []
        if not self.client.is_connected:
            p.append("connection")
        if not self._money_ready:
            p.append("money.start")
        if not self._boot_done:
            p.append("symbols warm-up")
        elif self._needs_resync():
            p.append("resync after reconnect")
        if _cfg("TRADE_WINDOW_ENABLED", False) and not window_scheduler.clock_measured():
            p.append("server-clock offset")
        return p

    def _drain_busy(self) -> bool:
        """True while something bought before the close is still unsettled (subclasses add in-flight buys)."""
        return bool(self._open)

    def _on_session_end(self) -> None:
        """Hook: called after the drain, before the stats flush and the disconnect (DonkeyEngine logs its report)."""
        return

    async def _end_session(self):
        self._session_over = True                       # _blocked() is now True: no new proposals/buys
        for t in list(self._stop_at_close):             # monitor / refit / watchdog / extras / clock: nothing but
            t.cancel()                                  # settlement and the in-doubt reconciles talks to Deriv now
        drain = float(_cfg("SESSION_DRAIN_SECONDS", 15))
        logger.warning("TRADE SESSION CLOSED at server %s UTC: no new trades; draining %d open contract(s) (max %.0fs)",
                       _utc(window_scheduler.now()), len(self._open), drain)
        t0 = time.monotonic()
        while self._drain_busy() and time.monotonic() - t0 < drain:
            await asyncio.sleep(0.1)                    # ends as soon as nothing is open
        if self._drain_busy():
            logger.warning("drain cap reached after %.0fs: %d contract(s) left to settle on Deriv's side",
                           time.monotonic() - t0, len(self._open))
        try:
            self._on_session_end()
        except Exception as exc:
            logger.warning("session end hook error: %s", exc)
        try:
            await asyncio.wait_for(self.stats.flush(force=True), timeout=5)
        except Exception as exc:
            logger.warning("session end: stats flush skipped: %s", exc)
        try:
            import keep_alive
            keep_alive.update_status(session="IDLE - disconnected until next session",
                                     open_contracts_count=len(self._open))
        except Exception:
            pass
        await self.client.shutdown("trade session closed")      # also stops DerivClient's reconnect loop
        for _ in range(50):                                     # the socket must really be down before we return
            if not self.client.is_connected:
                break
            await asyncio.sleep(0.1)
        logger.warning("SESSION ENDED: disconnected from Deriv (connected=%s, %d contract(s) left to settle on Deriv's side)",
                       self.client.is_connected, len(self._open))
        self._target_done.set()

    # ── server clock: burst of {"time":1} samples, lowest-RTT / interval intersection ──
    # Deriv answers {"time":1} in WHOLE seconds, so one sample is only good to +-0.5 s. Samples spread over several
    # seconds straddle several second-flips; intersecting their intervals (window_scheduler.estimate_offset) recovers
    # the sub-second phase. Then a few probes are timed to land ON the predicted flip to bisect it further.
    # The request counts are named so they can be tuned against Deriv's rate limits (not verified here).
    CLOCK_BURST_SAMPLES = 10          # first burst, spaced CLOCK_BURST_SPACING apart
    CLOCK_BURST_SPACING = 0.3
    CLOCK_REFINE_PROBES = 6           # probes timed onto the predicted second-flip
    CLOCK_REFRESH_SECONDS = 45.0      # a smaller burst every 30-60 s
    CLOCK_REFRESH_SAMPLES = 5
    CLOCK_REFRESH_PROBES = 3
    CLOCK_MAX_ERROR = 1.0             # a result with a worse estimated error is not used

    async def _time_sample(self):
        t0 = time.time()
        resp = await self.client._send({"time": 1}, timeout=3)
        t1 = time.time()
        return (t0, t1, float(resp["time"]))

    async def _measure_clock(self, burst: int, probes: int) -> bool:
        ws = window_scheduler
        samples: list = []
        for _ in range(burst):
            try:
                samples.append(await self._time_sample())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("CLOCK SYNC sample failed: %s", exc)
            await asyncio.sleep(self.CLOCK_BURST_SPACING)
        est = ws.estimate_offset(samples)
        if est is None:
            return False
        if est[1] <= self.CLOCK_MAX_ERROR:
            ws.set_clock_offset(est[0], est[1], len(samples))        # usable at once; refined below
        for _ in range(probes):
            off, err, _used = est
            if err <= 0.02 or not samples:
                break
            rtt = min(b - a for a, b, _ in samples)
            flip_k = int(time.time() + rtt / 2.0 + 0.02 + off - ws.SERVER_TIME_SHIFT) + 1   # next whole server second
            land = flip_k + ws.SERVER_TIME_SHIFT - off                                        # local instant it flips
            await asyncio.sleep(max(0.0, land - rtt / 2.0 - time.time()))
            try:
                samples.append(await self._time_sample())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("CLOCK SYNC probe failed: %s", exc)
                break
            new = ws.estimate_offset(samples)
            if new is not None:
                est = new
        if est[1] > self.CLOCK_MAX_ERROR:
            logger.warning("CLOCK SYNC: estimated error +-%.3fs is too large, not used", est[1])
            return False
        prev = ws.clock_offset() if ws.clock_measured() else None
        if not ws.set_clock_offset(est[0], est[1], len(samples)):
            return False
        if prev is None or abs(est[0] - prev) > max(0.05, est[1]):
            logger.info("CLOCK SYNC: Deriv server - local = %+.3fs (%d sample(s), est. error +-%.3fs)%s", est[0], len(samples),
                        est[1], "" if prev is None else f" [was {prev:+.3f}s]")
        return True

    async def _clock_sync(self):
        """Measure right after the connection is authorised (while the symbols warm up), then refresh every ~45 s
        (never within 4 s of an open/close edge)."""
        ws = window_scheduler
        first = True
        try:
            while True:
                if not self.client.is_connected:
                    await asyncio.sleep(1.0)
                    continue
                if not first:
                    t = ws.now()
                    cur = ws.session_at(t)
                    edge = (cur[2] if cur else ws.next_session(t)[1]) - t
                    if edge < 4.0:
                        await asyncio.sleep(0.5)
                        continue
                ok = await self._measure_clock(self.CLOCK_BURST_SAMPLES if first else self.CLOCK_REFRESH_SAMPLES,
                                               self.CLOCK_REFINE_PROBES if first else self.CLOCK_REFRESH_PROBES)
                if ok:
                    first = False
                await asyncio.sleep(self.CLOCK_REFRESH_SECONDS if ok else 2.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:                            # never let the clock task crash the engine
            logger.error("clock sync stopped: %s", exc, exc_info=True)

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
        """Straggler polling: contracts whose settlement message never arrived are asked for directly."""
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
                    balance=round(self.client.balance, 2), session="DONKEY",
                    current_symbol=",".join(self.symbols), last_signal=self._last_decision,
                    armed_pairs=self._armed_count(),
                    open_contracts_count=len(self._open),
                    target_balance=target_guard.target(), money=self.money.snapshot())
                keep_alive.set_active_trades(len(self._open))
            except Exception:
                pass
