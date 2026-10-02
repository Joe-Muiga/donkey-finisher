"""
bot_engine.py – Central async orchestrator.

v17 — Continuous non-blocking parallel-scan rewrite.

KEY CHANGES vs v16
───────────────────
INIT:
  • Only config.TRADE_SYMBOLS are initialised — no volatility-index /
    ALL_TRADE_SYMBOLS phased startup. Symbols that fail to initialise are
    skipped; the bot never crashes on a single symbol failure.

MAIN LOOP:
  • Scanning and settling are fully decoupled. The main loop scans every
    ready symbol in parallel every cycle via asyncio.gather and does NOT
    wait for open contracts to settle before scanning again.
  • The only execution gate is concurrent-slot availability
    (self.risk.current_concurrent_limit).
  • Signals are ranked by score*0.85 + symbol_win_rate*0.15, deduplicated
    (highest score per symbol kept), and the top N ranked signals are
    executed in parallel via asyncio.gather.

SETTLING:
  • A separate `_settle_loop` task runs independently of the scan loop.
    After every settle wait it resolves orphaned contracts and increments
    `_cycle_count`; once REDEPLOY_EVERY_N_CYCLES is reached it drains all
    open contracts and triggers a Render redeploy.

UNCHANGED:
  • Websocket connection (DerivClient), dashboard serving (_push_dashboard /
    _dashboard_loop), keep_alive integration (record_signal, record_trade,
    record_failure, update_open_contracts, set_active_trades, update_status),
    and the Render redeploy mechanism (restart_scheduler.trigger_redeploy /
    is_redeploy_pending).
"""

import asyncio
import datetime as _dt
import logging
import math
import random
import time
import traceback
from collections import deque
from dataclasses import dataclass, replace
from typing import Deque, Dict, List, Optional, Set, Tuple

import numpy as np

import config
import exit_engine
import restart_scheduler
import fixed_cycle
import kill_switch
import symbols as sym_module
import strategy_stats
import meta_labeling
import pair_suspension
from deriv_client import DerivClient
from candlestick_builder import CandlestickBuilder
from smc_analyzer import SMCAnalyzer, SMCContext
from signal_engine import SignalEngine, SignalResult, compute_enriched_features
from risk_manager import RiskManager
from news_filter import NewsFilter
from trade_journal import TradeJournal
from donkey_guard import get_guard, signal_kind_from_reason
from symbol_manager import SymbolManager
import indicators as ind
from keep_alive import (update_status, set_active_trades,
                        record_trade,
                        record_signal, record_failure,
                        update_open_contracts, update_suspended_symbols, _status)
from symbols import get_symbol_class

logger = logging.getLogger(__name__)

DASHBOARD_PUSH_EVERY  = 5        # push every 5 seconds
INIT_BATCH_SIZE       = getattr(config, "INIT_BATCH_SIZE", 10)

CONTRACT_MAX_AGE_SECS     = getattr(config, "CONTRACT_MAX_AGE_SECS",      900)
CONTRACT_FORCE_CLOSE_SECS = getattr(config, "CONTRACT_FORCE_CLOSE_SECS", 1350)

# ── Reconciliation (Fix C) ───────────────────────────────────────────────
RECONCILE_POLL_INTERVAL_SECS = getattr(config, "RECONCILE_POLL_INTERVAL_SECS", 30)
RECONCILE_MAX_SECS           = getattr(config, "RECONCILE_MAX_SECS",           1800)

# ── Multiplier max-hold (Fix E) ──────────────────────────────────────────
MULTIPLIER_MAX_HOLD_SECS = getattr(config, "MULTIPLIER_MAX_HOLD_MINS", 30) * 60

# ── Redeploy drain (Fix G) ───────────────────────────────────────────────
DRAIN_MAX_SECS = getattr(config, "DRAIN_MAX_SECS", 1800)


# ─── Result container ──────────────────────────────────────────────────────────

@dataclass
class ScanResult:
    symbol:    str
    sig:       SignalResult
    price:     float
    smc_ctx:   SMCContext
    score:     float = 0.0
    rank_key:  float = 0.0   # score * bandit priority weight — sort key only,
                              # never persisted/logged in place of `score`


@dataclass
class PendingEntry:
    """
    A signal that fired but hasn't been bought yet — armed and watched
    tick-by-tick (see bot_engine._check_pending_entry, called from
    _on_tick) until price reaches the native stop-loss, or it expires
    unfilled. See config.py's "DELAYED ENTRY — stop as trigger" block
    (Sep 12 2026 single-trigger redesign, replacing the earlier two-trigger
    designs entirely).

    Single trigger: price reaching the indicator's own native_stop_price.
    That's also where we enter — direction is NEVER flipped, we continue
    the indicator's original call:
      - target = live entry price + STOP_TRIGGERED_TARGET_PCT * (the
        ORIGINAL native_target_price - native_entry_price distance),
        applied toward where the original target was.
      - stop, computed AFTER the target: placed on the opposite side of
        entry from the target, at 1/STOP_TRIGGERED_RISK_REWARD_RATIO of
        the target distance — so the reward:risk ratio is always exactly
        STOP_TRIGGERED_RISK_REWARD_RATIO (2:1 by default), by construction.
    There is no "scrap" outcome. Only the timeout still results in no trade.
    """
    symbol:                str
    sig:                   SignalResult
    direction:              str     # original indicator direction, unchanged at execution
    entry_ref_price:        float   # native_entry_price when armed (used only for the target-distance calc)
    stop_ref_price:         float   # native_stop_price when armed — this IS the trigger
    target_ref_price:       float   # native_target_price when armed (used only for the target-distance calc)
    trigger_price:          float   # == stop_ref_price; kept as its own field for clarity at call sites
    created_at:             float
    strategy:               str


@dataclass
class StopTriggerPendingEntry:
    """
    Handoff (Sep 15 2026) — a NEW, PARALLEL pending-order design, scoped
    only to config.STOP_AS_TRIGGER_SYMBOLS (the nine symbols routed to
    signal_engine.py's evaluate_pullback_trend / evaluate_fast_mean_reversion
    / evaluate_spike_catch_1000 / evaluate_spike_catch_500 /
    evaluate_step_grid). Built alongside — not a modification of —
    PendingEntry/_arm_pending_entry/_check_pending_entry/
    _execute_pending_entry above, which keep governing every other
    symbol's delayed entry exactly as before. See config.py's
    "STOP-AS-TRIGGER ENTRY" section for the full spec and worked example.

    This is genuinely the mechanic PendingEntry's own docstring describes
    (stop-as-trigger, no direction flip) — the live DELAYED_ENTRY_* design
    above evolved into something else (percent-trigger toward target,
    forced flip) without that docstring ever being updated; this class
    implements the stop-as-trigger mechanic for real, for these nine
    symbols only.

    Single trigger: price reaching the evaluator's own native_stop_price.
    That's also where we enter — direction is NEVER flipped:
      - take-profit = the evaluator's original native_target_price, used
        as-is.
      - stop-loss, re-derived AFTER the fill from the live entry-to-target
        distance, so that take_profit_distance / stop_loss_distance is
        STRICTLY greater than config.STOP_AS_TRIGGER_MIN_RR_RATIO.
    Cancel-before-fill: reaching the ORIGINAL native_target_price before
    ever reaching the trigger cancels the pending order outright — no
    trade. A time-based expiry (shared config.DELAYED_ENTRY_TIMEOUT_SECS)
    remains as a fallback safety net.
    """
    symbol:                str
    sig:                    SignalResult
    direction:               str     # original evaluator direction, NEVER flipped at execution
    entry_ref_price:         float   # native_entry_price when armed (informational only)
    stop_ref_price:          float   # native_stop_price when armed — this IS the trigger
    target_ref_price:        float   # native_target_price when armed — used as-is for TP, and as the cancel-before-fill level
    created_at:              float
    strategy:                str


# ─── Thompson sampling bandit ───────────────────────────────────────────────

class _ThompsonBandit:
    """
    Beta(alpha, beta) posterior per (strategy, symbol) pair, used ONLY to
    weight execution priority when multiple signals compete for the same
    cycle's slots — it never touches the composite score itself.

    Deliberately stateless / derived-on-read from strategy_stats rather
    than kept as a running in-memory counter: _settle_loop triggers a
    Render redeploy every config.REDEPLOY_EVERY_N_CYCLES settle ticks,
    which wipes process memory. An in-memory posterior would reset to a
    flat Beta(1,1) prior on every one of those restarts and effectively
    never learn anything. Reading wins/losses straight out of
    strategy_stats' persisted backend means the posterior survives
    redeploys (as long as the underlying SQLite/JSON file sits on a
    disk that itself survives redeploys — see strategy_stats.py's own
    persistence caveat).
    """

    def __init__(self, window: int = strategy_stats.DEFAULT_WINDOW):
        self._window = window

    def sample(self, strategy: str, symbol: str) -> float:
        try:
            rate, _lo, _hi, n = strategy_stats.stats.get_win_rate(
                strategy, symbol, window=self._window)
        except Exception as exc:
            logger.warning(f"bandit.sample({strategy},{symbol}) failed: {exc}")
            return 0.5

        wins   = round(rate * n)
        losses = n - wins
        alpha  = 1.0 + wins    # Beta(1,1) uniform prior with no history
        beta_p = 1.0 + losses

        try:
            return random.betavariate(alpha, beta_p)
        except Exception:
            return 0.5


# ─── Per-strategy expectancy tracker (Implementation Brief v3, task 3) ─────────

class _StrategyExpectancyTracker:
    """
    Per-strategy (MEAN_REVERSION / STEP / JUMP_BUILDUP / TREND_SHIFT /
    BOOM_CRASH / ...) win rate, avg win, avg loss, and profit factor,
    plus a Wilson-score confidence interval on win rate given trade count
    — this is what turns the account-level aggregate the dashboard already
    shows (journal.session_summary()) into something that can actually be
    trusted or distrusted PER STRATEGY, which is the brief's whole point:
    at n=39 trades total, 61.5% isn't yet distinguishable from breakeven.

    Deliberately a separate, self-contained in-memory tracker rather than
    an extension of strategy_stats.py — strategy_stats.py's job (feeding
    the meta-labeling filter and the Thompson bandit) is a different
    consumer of similar data, and this file only has confirmed sight of
    its read API (get_win_rate), not its write/storage internals, so
    bolting new aggregate fields onto it without seeing its source would
    be a guess. Fed from the exact same _apply_settlement() call site that
    already calls strategy_stats.stats.record_trade(), so the two never
    disagree on what a "trade" is. Resets on redeploy along with the rest
    of this process's in-memory state — acceptable here since the ask is
    to surface it, not persist it durably across restarts.
    """

    def __init__(self):
        self._trades: Dict[str, List[Tuple[bool, float]]] = {}

    def record(self, strategy: str, won: bool, pnl: float) -> None:
        self._trades.setdefault(strategy, []).append((won, float(pnl)))

    @staticmethod
    def _wilson_ci(wins: int, n: int, z: float = 1.96) -> Tuple[float, float]:
        """95% Wilson score interval (z=1.96) on a binomial win rate."""
        if n == 0:
            return (0.0, 1.0)
        phat = wins / n
        denom = 1.0 + (z * z) / n
        center = (phat + (z * z) / (2 * n)) / denom
        margin = (z * math.sqrt((phat * (1 - phat) + (z * z) / (4 * n)) / n)) / denom
        return (max(0.0, center - margin), min(1.0, center + margin))

    def summary(self) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for strategy, trades in self._trades.items():
            n = len(trades)
            if n == 0:
                continue
            win_pnls  = [pnl for won, pnl in trades if won]
            loss_pnls = [pnl for won, pnl in trades if not won]
            win_count   = len(win_pnls)
            loss_count  = len(loss_pnls)
            gross_win   = sum(win_pnls)
            gross_loss  = abs(sum(loss_pnls))
            avg_win     = (gross_win / win_count) if win_count else 0.0
            avg_loss    = (gross_loss / loss_count) if loss_count else 0.0
            if gross_loss > 0:
                profit_factor = gross_win / gross_loss
            else:
                profit_factor = None if gross_win == 0 else float("inf")
            win_rate = win_count / n
            ci_lo, ci_hi = self._wilson_ci(win_count, n)
            out[strategy] = {
                "trades":         n,
                "win_rate_pct":   round(win_rate * 100, 1),
                "win_rate_ci_pct": [round(ci_lo * 100, 1), round(ci_hi * 100, 1)],
                "avg_win":        round(avg_win, 4),
                "avg_loss":       round(avg_loss, 4),
                "profit_factor":  (round(profit_factor, 3)
                                    if profit_factor is not None and math.isfinite(profit_factor)
                                    else profit_factor),
                "gross_profit":   round(gross_win, 4),
                "gross_loss":     round(gross_loss, 4),
            }
        return out


# ─── Bot engine ────────────────────────────────────────────────────────────────

class BotEngine:

    def __init__(self):
        self._cycle_count:            int                = 0
        self._running:                bool               = False
        self._session_start_balance:  float              = 0.0
        self._open_contracts:         dict               = {}

        # ── Fixed single-leg trading cycle (Sep 2026, chat-requested;
        #    restructured same day to drop leg 2 and randomize the
        #    cooldown) — see fixed_cycle.py. Set True once leg 1's
        #    rolling-redeploy timer was intercepted into a cooldown
        #    request AND every open contract is confirmed closed;
        #    _main_loop checks this and stops itself so run() can
        #    disconnect from Deriv for the randomized cooldown between
        #    leg 1 runs.
        self._entering_fixed_cooldown: bool = False
        self._contract_open_times:    dict               = {}

        # ── Contracts past CONTRACT_FORCE_CLOSE_SECS that still haven't
        #    settled for real — actively re-polled on RECONCILE_POLL_
        #    INTERVAL_SECS rather than ever being fabricated (Fix C).
        #    {contract_id: {"reconcile_started_at": epoch, "last_poll": epoch}}
        self._reconciling:            dict               = {}

        self._htf:                    Dict[str, CandlestickBuilder] = {}
        self._mtf:                    Dict[str, CandlestickBuilder] = {}
        self._ltf:                    Dict[str, CandlestickBuilder] = {}

        # ── Raw tick buffer — feeds evaluate(ticks=...) for the tick-based
        #    evaluators (digit parity, drift fade, jump buildup, trend
        #    shift), which previously always saw ticks=None because
        #    nothing ever populated or passed a tick buffer.
        self._raw_ticks:              Dict[str, Deque[dict]] = {}

        # ── Delayed-entry / native SL-TP swap (Sep 2026) — signals that
        #    fired but are waiting for price to move DELAYED_ENTRY_TRIGGER_PCT
        #    toward their own native target before actually being bought.
        #    {symbol: PendingEntry}. See config.py for the full mechanism.
        self._pending_entries:        Dict[str, PendingEntry] = {}

        # Handoff (Sep 15 2026) — parallel stop-as-trigger pending-order
        # state, scoped to config.STOP_AS_TRIGGER_SYMBOLS only. Mutually
        # exclusive with self._pending_entries per symbol (the dispatch
        # block below arms a symbol into exactly one of the two dicts,
        # never both) — see _arm_stop_trigger_entry / _check_stop_trigger_entry
        # / _execute_stop_trigger_entry and config.py's "STOP-AS-TRIGGER
        # ENTRY" section.
        self._stop_trigger_pending:   Dict[str, StopTriggerPendingEntry] = {}

        # Closes the race where a symbol sits in neither _pending_entries
        # nor symbol_manager's _active_symbols — from the instant a pending
        # entry triggers (or an immediate signal is selected for execution)
        # until _execute() actually finishes registering it as open. See
        # _check_pending_entry's trigger branch and _execute_pending_entry's
        # finally block for where this gets set/released on the delayed
        # path, and the main cycle loop / _execute_and_release for the
        # immediate path.
        self._executing_symbols:      Set[str] = set()

        self._initialised_symbols:    Set[str]           = set()
        self._initializing:           Set[str]           = set()

        # ── Symbols whose tick subscription failed during _init_data —
        #    candles/data are seeded but no live ticks are arriving, so
        #    these are ready-but-frozen until re-subscribed.
        self._tick_degraded:          Set[str]           = set()

        # ── Consecutive identical buy-placement failures per symbol —
        #    drives the circuit breaker in _execute().
        self._buy_failure_streak:     Dict[str, int]     = {}

        self._confirmed_daily_loss:   float              = 0.0
        self._day_start_balance:      float              = 0.0
        self._confirmed_paused:       bool               = False
        self._current_utc_day:        int                = -1

        # ── Global consecutive-loss circuit breaker (profitability audit,
        #    round 2). RiskManager's own loss_streak is hardwired to 0/1 —
        #    "PLS tracks only win streaks" — and the per-symbol suspension
        #    ladder in symbol_manager.py is scoped to one symbol at a time,
        #    so nothing previously stopped the bot from taking trade after
        #    trade account-wide during a bad run within a single day
        #    (5 of 7 trades lost the session this was added in response
        #    to) short of the much coarser DAILY_LOSS_LIMIT_PCT. This adds
        #    a fast-acting, session-wide pause after N consecutive losses
        #    regardless of symbol/strategy, independent of the % based
        #    daily limit.
        self._global_consecutive_losses: int              = 0
        self._loss_streak_paused_until:  float             = 0.0
        self._donkey_guard = get_guard()  # session stop/take-profit, streak pause, edge log

        # ── Ensemble voting: per-symbol rolling history of (strategy,
        #    direction, timestamp) tuples for every signal produced by
        #    _scan, pruned to config.ENSEMBLE_AGREEMENT_WINDOW_SECS.
        self._ensemble_history:       Dict[str, Deque[Tuple[str, str, float]]] = {}

        # ── Thompson sampling bandit — see _ThompsonBandit docstring
        #    for why this reads through strategy_stats instead of
        #    keeping its own in-memory posterior.
        self._bandit = _ThompsonBandit()

        # ── Per-strategy expectancy tracker (Implementation Brief v3,
        #    task 3) — fed in _apply_settlement(), surfaced in
        #    _push_dashboard(). See _StrategyExpectancyTracker docstring.
        self._strategy_expectancy = _StrategyExpectancyTracker()

        self.client  = DerivClient()
        self.risk    = RiskManager(
            risk_per_trade = config.RISK_PER_TRADE_PCT,
            min_stake      = config.MIN_STAKE,
            max_stake      = config.MAX_STAKE,
            max_concurrent = config.MAX_CONCURRENT_TRADES,
        )
        self.smc     = SMCAnalyzer(ob_expiry_bars=config.OB_EXPIRY_BARS)
        self.signal  = SignalEngine(
            symbols=getattr(config, "TRADE_SYMBOLS", []),
            config=config,
        )
        self.news    = NewsFilter(block_minutes=config.NEWS_BLOCK_MINUTES)
        self.journal = TradeJournal()
        self.symbols = SymbolManager()

    # ── Timeframe routing ──────────────────────────────────────────────────────

    @staticmethod
    def _ltf_gran(symbol: str) -> int:
        return (config.FOREX_LTF_GRANULARITY
                if symbol in sym_module.FOREX
                else config.OTHER_LTF_GRANULARITY)

    @staticmethod
    def _mtf_gran(symbol: str) -> int:
        return getattr(
            config,
            "FOREX_MTF_GRANULARITY" if symbol in sym_module.FOREX else "OTHER_MTF_GRANULARITY",
            getattr(config, "MTF_GRANULARITY", 300),
        )

    # ── Composite score ────────────────────────────────────────────────────────

    def _composite_score(self, sig: SignalResult, symbol: str) -> float:
        signal_score = getattr(sig, "score", 0.0)
        win_rate     = (self.symbols.win_rate(symbol)
                        if hasattr(self.symbols, "win_rate") else 0.5)
        return round(float(signal_score) * 0.85 + float(win_rate) * 0.15, 4)

    # ── Ensemble voting ────────────────────────────────────────────────────────

    def _log_ensemble_signal(self, symbol: str, strategy: str, direction: str):
        """Record every signal _scan produces (whether or not it ends up
        trading) so the agreement window has something to check against."""
        window = getattr(config, "ENSEMBLE_AGREEMENT_WINDOW_SECS", 60)
        now    = time.time()
        dq     = self._ensemble_history.setdefault(symbol, deque())
        dq.append((strategy, direction, now))
        while dq and (now - dq[0][2]) > window:
            dq.popleft()

    def _ensemble_agrees(self, symbol: str, direction: str) -> bool:
        """
        True if >= config.ENSEMBLE_MIN_STRATEGIES_AGREEING distinct
        strategies have signalled `direction` on `symbol` within the
        agreement window (including the current signal, since it was
        logged via _log_ensemble_signal before this is called).

        NOTE: config.py's own STRATEGY ROUTING section states every
        traded symbol is routed to exactly one strategy evaluator
        (MEAN_REVERSION_SYMBOLS / STEP_SYMBOLS / etc are disjoint).
        With that routing, at most one strategy will ever signal a
        given symbol in a given cycle, so this will almost always
        return False once ENSEMBLE_MODE=True — see flags in the
        accompanying explanation before enabling it.
        """
        window    = getattr(config, "ENSEMBLE_AGREEMENT_WINDOW_SECS", 60)
        min_agree = getattr(config, "ENSEMBLE_MIN_STRATEGIES_AGREEING", 2)
        now       = time.time()
        dq        = self._ensemble_history.get(symbol)
        if not dq:
            return False
        strategies = {s for (s, d, ts) in dq
                      if d == direction and (now - ts) <= window}
        return len(strategies) >= min_agree

    # ── Session / day-of-week score weighting ─────────────────────────────────

    def _session_dow_weight(self, symbol: str) -> float:
        """
        Multiplier from config.SESSION_DOW_WEIGHT_TABLE, keyed on the
        symbol's category (via symbols.get_symbol_class), current UTC
        hour, and current UTC day-of-week (Mon=0..Sun=6).

        NOTE: the table shipped in config.py only has entries for Boom/
        Crash/Jump categories, none of which are in RISE_FALL_SYMBOLS /
        TRADE_SYMBOLS right now (BOOM_CRASH=[] and JUMP=[] — disabled,
        see config comments). Until entries are added for the
        categories get_symbol_class() actually returns for your traded
        symbols (volatility indices, stpRNG, drift), every lookup below
        falls through to SESSION_DOW_WEIGHT_DEFAULT=1.0, i.e. a no-op.
        """
        try:
            category = get_symbol_class(symbol)
        except Exception:
            return getattr(config, "SESSION_DOW_WEIGHT_DEFAULT", 1.0)

        table = getattr(config, "SESSION_DOW_WEIGHT_TABLE", {})
        entry = table.get(category)
        if not entry:
            return getattr(config, "SESSION_DOW_WEIGHT_DEFAULT", 1.0)

        now = _dt.datetime.utcnow()

        days = entry.get("days")
        if days is not None and now.weekday() not in days:
            return getattr(config, "SESSION_DOW_WEIGHT_DEFAULT", 1.0)

        hours_utc = entry.get("hours_utc")
        if hours_utc is not None:
            start, end = hours_utc
            if not (start <= now.hour <= end):
                return getattr(config, "SESSION_DOW_WEIGHT_DEFAULT", 1.0)

        return float(entry.get("multiplier", getattr(config, "SESSION_DOW_WEIGHT_DEFAULT", 1.0)))

    # ── Daily loss limit ───────────────────────────────────────────────────────

    def _check_confirmed_loss_limit(self):
        today = _dt.datetime.utcnow().day
        if today != self._current_utc_day:
            self._confirmed_daily_loss = 0.0
            self._day_start_balance    = self.client.balance
            self._current_utc_day      = today
            logger.info(
                f"UTC day reset — day_start_balance=${self._day_start_balance:.4f}")

        if self._day_start_balance > 0:
            loss_ratio = self._confirmed_daily_loss / self._day_start_balance
            if loss_ratio >= config.DAILY_LOSS_LIMIT_PCT:
                if not self._confirmed_paused:
                    logger.warning("DAILY LIMIT HIT — paused until midnight UTC")
                self._confirmed_paused = True
            else:
                self._confirmed_paused = False
        else:
            self._confirmed_paused = False

    @staticmethod
    def _minutes_until_midnight() -> float:
        now      = _dt.datetime.utcnow()
        midnight = (now + _dt.timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0)
        return (midnight - now).total_seconds() / 60.0

    # ── Balance callback ───────────────────────────────────────────────────────

    def _on_balance(self, balance: float):
        self.risk.set_balance(balance)
        # No balance-driven cycle logic anymore — fixed_cycle.py's
        # single-leg-then-cooldown pattern is purely time-based (see
        # _settle_loop's redeploy-pending handling), not balance-,
        # profit-, or drawdown-driven. Nothing to do here for cycle
        # purposes.

    # ── Entry point ────────────────────────────────────────────────────────────

    async def run(self):
        logger.info("=" * 64)
        logger.info("  SIFM Deriv Trading Bot  –  continuous parallel-scan")
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

        self.client.on_balance(self._on_balance)
        self.risk.set_balance(self.client.balance)

        self._day_start_balance     = self.client.balance
        self._session_start_balance = self.client.balance
        self._current_utc_day       = _dt.datetime.utcnow().day

        # ── Fixed single-leg trading cycle (Sep 2026, chat-requested):
        # leg 1 is the only leg now, so there's no leg counter to
        # bootstrap anymore — this just logs the start of a fresh leg 1
        # run. See fixed_cycle.py.
        fixed_cycle.log_trading_start()

        # ── Redeploy-proof open-contract recovery (spec point 10, Aug 2026) ──
        # Render's filesystem is ephemeral across deploys on this plan (no
        # persistent disk attached) and this process's own in-memory
        # _open_contracts dict is wiped on every restart exactly the same
        # way — so after a redeploy (including the force-redeploy every
        # REDEPLOY_INTERVAL_HOURS, spec point 11) both bot_engine's own
        # tracking AND the dashboard's open_contracts start empty even
        # though real contracts may still be open on Deriv's own servers.
        # Re-derive "what's open" from Deriv's own server-side state via
        # get_portfolio() — the only source that doesn't depend on
        # anything surviving locally — and use it to rebuild both this
        # object's tracking and keep_alive's dashboard state before the
        # first scan cycle, so there's zero blackout window right after a
        # redeploy instead of waiting to rediscover open trades piecemeal
        # as settlement pushes trickle in.
        try:
            await self._recover_open_contracts_from_portfolio()
        except Exception as exc:
            logger.warning(f"Open-contract recovery from portfolio failed: {exc}")

        await self._init_all_symbols()

        try:
            self._push_dashboard()
        except Exception:
            pass

        dash_task     = asyncio.create_task(self._dashboard_loop())
        settle_task   = asyncio.create_task(self._settle_loop())
        degraded_task = asyncio.create_task(self._degraded_retry_loop())
        # NOTE: the daily Kenya-midnight redeploy timer (Fix G) is started
        # by main.py's existing restart_scheduler.start_restart_scheduler()
        # call, unchanged — NOT started again here, to avoid two competing
        # scheduler loops. _settle_loop below just reads
        # restart_scheduler.is_redeploy_pending() / calls
        # restart_scheduler.trigger_redeploy().

        try:
            await self._main_loop()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.critical(f"Main loop crashed: {exc}\n{traceback.format_exc()}")
        finally:
            dash_task.cancel()
            settle_task.cancel()
            degraded_task.cancel()
            ws_task.cancel()

    # ── Startup — TRADE_SYMBOLS only ───────────────────────────────────────────

    async def _recover_open_contracts_from_portfolio(self):
        """
        Spec point 10: rebuild both this object's _open_contracts tracking
        and keep_alive's dashboard state directly from Deriv's own
        server-side portfolio ({"portfolio": 1}), rather than trusting
        anything that might have survived the last redeploy locally.
        Called once, at the very start of run(), before the first scan
        cycle.

        Best-effort mapping: proposal/buy responses this bot places itself
        carry richer bookkeeping (strategy, native SL/TP, enriched
        features, etc.) than a portfolio snapshot does, so recovered
        entries are necessarily thinner — enough for the dashboard to show
        the contract live and for _monitor_exit()/_settle_loop() to still
        notice it close, not a full reconstruction of every field
        placement-time registration captures.
        """
        try:
            contracts = await self.client.get_portfolio()
        except Exception as exc:
            logger.warning(f"get_portfolio() failed during recovery: {exc}")
            return

        if not contracts:
            logger.info("PORTFOLIO RECOVERY: no open contracts on Deriv — clean start")
            return

        recovered = 0
        for c in contracts:
            try:
                cid = str(c.get("contract_id", ""))
                if not cid or cid in self._open_contracts:
                    continue
                symbol = c.get("symbol") or c.get("underlying_symbol") or "—"
                contract_type = (c.get("contract_type") or "").upper()
                direction = "LONG" if contract_type in ("MULTUP", "CALL") else (
                    "SHORT" if contract_type in ("MULTDOWN", "PUT") else "—")
                buy_price = float(c.get("buy_price", c.get("purchase_price", 0.0)) or 0.0)
                opened_at = float(c.get("purchase_time", time.time()) or time.time())

                self._open_contracts[cid] = {
                    "symbol":      symbol,
                    "direction":   direction,
                    "stake":       buy_price,
                    "entry_price": None,
                    "opened_at":   opened_at,
                    "rec":         None,
                    "sig":         None,
                    "strategy":    c.get("shortcode", "RECOVERED"),
                    "stop_loss":   None,
                    "take_profit": None,
                    "multiplier":  c.get("multiplier"),
                    "atr_pct":     0.0,
                    "regime":      "NONE",
                    "enriched_features": {},
                    "inverted":    None,
                    "recovered_from_portfolio": True,
                }
                self._contract_open_times[cid] = opened_at

                try:
                    await self.client.subscribe_contract(
                        cid,
                        lambda msg, _cid=cid: asyncio.create_task(
                            self._on_contract_result(_cid, msg)),
                        symbol=symbol,
                    )
                except Exception as exc:
                    logger.warning(f"subscribe_contract({cid}) during recovery failed: {exc}")

                recovered += 1
            except Exception as exc:
                logger.warning(f"Skipping unparsable portfolio contract {c}: {exc}")

        logger.warning(
            f"PORTFOLIO RECOVERY: rebuilt {recovered} open contract(s) from "
            f"Deriv's own server-side state after redeploy/restart"
        )

        # Push immediately — don't wait for the periodic _dashboard_loop
        # tick — so the dashboard has zero blackout window right after a
        # redeploy.
        try:
            self._push_dashboard()
        except Exception:
            pass
        set_active_trades(len(self._open_contracts))

    async def _init_all_symbols(self):
        trade_symbols = list(getattr(config, "TRADE_SYMBOLS",
                             getattr(config, "ALL_TRADE_SYMBOLS",
                             getattr(config, "ALL_SYMBOLS", []))))

        logger.info(f"Initialising {len(trade_symbols)} symbols: {trade_symbols}")

        if not trade_symbols:
            logger.error(
                "NO SYMBOLS IN CONFIG — check TRADE_SYMBOLS / "
                "ALL_TRADE_SYMBOLS in config.py")
            return

        results = await asyncio.gather(
            *[self._init_data(s) for s in trade_symbols],
            return_exceptions=True,
        )
        for s, r in zip(trade_symbols, results):
            if isinstance(r, Exception):
                logger.warning(f"{s}: failed to initialise — skipping ({r})")

        ready = list(self._htf.keys())
        self._initialised_symbols = set(ready)
        self.symbols.update_active(ready)
        logger.info(
            f"Initialised: {len(ready)}/{len(trade_symbols)} "
            f"symbol(s) ready: {ready}")

    # ── Data initialisation — three TFs simultaneously ─────────────────────────

    async def _init_data(
        self,
        symbol:   str,
        htf_bars: int = None,
        mtf_bars: int = None,
        ltf_bars: int = None,
    ):
        logger.info(f"INIT_DATA: starting {symbol}")

        if symbol in self._initializing or symbol in self._htf:
            logger.info(f"INIT_DATA: {symbol} already initializing/initialised — skipping")
            return
        self._initializing.add(symbol)

        if htf_bars is None: htf_bars = config.HTF_BARS
        if mtf_bars is None: mtf_bars = getattr(config, "MTF_BARS", 50)
        if ltf_bars is None: ltf_bars = config.LTF_BARS

        try:
            ltf_gran = self._ltf_gran(symbol)
            mtf_gran = self._mtf_gran(symbol)

            htf_b = CandlestickBuilder(granularity=config.HTF_GRANULARITY,
                                       max_bars=htf_bars + 20)
            mtf_b = CandlestickBuilder(granularity=mtf_gran,
                                       max_bars=mtf_bars + 20)
            ltf_b = CandlestickBuilder(granularity=ltf_gran,
                                       max_bars=ltf_bars + 20)

            try:
                htf_data, mtf_data, ltf_data = await asyncio.wait_for(
                    asyncio.gather(
                        self.client.get_candles(symbol, config.HTF_GRANULARITY, htf_bars),
                        self.client.get_candles(symbol, mtf_gran,               mtf_bars),
                        self.client.get_candles(symbol, ltf_gran,               ltf_bars),
                        return_exceptions=True,
                    ),
                    timeout=15,
                )
            except asyncio.TimeoutError:
                logger.warning(f"{symbol}: get_candles timed out after 15s — skipping")
                return

            if isinstance(htf_data, Exception): htf_data = []
            if isinstance(mtf_data, Exception): mtf_data = []
            if isinstance(ltf_data, Exception): ltf_data = []

            if not htf_data and not ltf_data:
                logger.warning(f"{symbol}: no data — skipping")
                return

            if htf_data: htf_b.seed(htf_data)
            if mtf_data: mtf_b.seed(mtf_data)
            if ltf_data: ltf_b.seed(ltf_data)

            self._htf[symbol] = htf_b
            self._mtf[symbol] = mtf_b
            self._ltf[symbol] = ltf_b
            self._initialised_symbols.add(symbol)

            try:
                await asyncio.wait_for(
                    self.client.subscribe_ticks(
                        symbol,
                        lambda tick, s=symbol: self._on_tick(s, tick)),
                    timeout=10,
                )
                self._tick_degraded.discard(symbol)
            except asyncio.TimeoutError:
                logger.warning(
                    f"{symbol}: subscribe_ticks timed out — "
                    f"DEGRADED (no live tick stream, candles frozen at seed) "
                    f"— will retry every {config.TICK_RESUBSCRIBE_RETRY_SECS}s")
                self._tick_degraded.add(symbol)
            except Exception as tick_exc:
                logger.warning(
                    f"{symbol}: subscribe_ticks failed — "
                    f"DEGRADED (no live tick stream, candles frozen at seed) "
                    f"— will retry every {config.TICK_RESUBSCRIBE_RETRY_SECS}s "
                    f"| {tick_exc}")
                self._tick_degraded.add(symbol)

            logger.info(
                f"{symbol}: ready | htf={htf_b.count} | "
                f"mtf={mtf_b.count} | ltf={ltf_b.count} "
                f"(ltf_gran={ltf_gran}s)")
            logger.info(f"INIT_DATA: {symbol} ready — htf={htf_b.count} ltf={ltf_b.count}")

        except Exception as exc:
            logger.error(f"_init_data({symbol}): skipping — {exc}")
        finally:
            self._initializing.discard(symbol)

    def _on_tick(self, symbol: str, tick: dict):
        import time as _t
        epoch = int(tick.get("epoch", int(_t.time())))
        price = float(tick.get("quote", 0))
        if price == 0:
            logger.debug(f"TICK IGNORED: {symbol} — zero/invalid quote in {tick}")
            return

        buf = self._raw_ticks.setdefault(
            symbol, deque(maxlen=config.TICK_BUFFER_MAXLEN))
        buf.append({"epoch": epoch, "quote": price})

        for store in (self._ltf, self._mtf, self._htf):
            if symbol in store:
                store[symbol].add_tick(epoch, price)

        self._tick_degraded.discard(symbol)
        logger.debug(f"TICK: {symbol} epoch={epoch} price={price}")

        if symbol in self._pending_entries:
            self._check_pending_entry(symbol, price)
        if symbol in self._stop_trigger_pending:
            self._check_stop_trigger_entry(symbol, price)

    # ── Degraded-symbol tick resubscription ─────────────────────────────────────

    async def _degraded_retry_loop(self):
        interval = getattr(config, "TICK_RESUBSCRIBE_RETRY_SECS", 30)
        while True:
            try:
                await asyncio.sleep(interval)
                for symbol in list(self._tick_degraded):
                    logger.info(f"{symbol}: retrying tick subscription (degraded)")
                    try:
                        await asyncio.wait_for(
                            self.client.subscribe_ticks(
                                symbol,
                                lambda tick, s=symbol: self._on_tick(s, tick)),
                            timeout=10,
                        )
                        self._tick_degraded.discard(symbol)
                        logger.info(f"{symbol}: tick subscription recovered")
                    except Exception as exc:
                        logger.warning(
                            f"{symbol}: degraded retry failed — still no "
                            f"live tick stream | {exc}")
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error(f"_degraded_retry_loop: {exc}")

    # ── Dashboard ──────────────────────────────────────────────────────────────

    async def _dashboard_loop(self):
        while True:
            try:
                self._push_dashboard()
            except Exception:
                pass
            try:
                self._check_tracking_divergence()
            except Exception:
                pass
            await asyncio.sleep(DASHBOARD_PUSH_EVERY)

    def _check_tracking_divergence(self) -> None:
        """
        Fix D: bot_engine's _open_contracts and deriv_client's own tracking
        sets (_polling_contracts / _subscribed_contracts) must always
        describe the same set of contracts. Once Fix C removes the
        fabrication path this should never happen — treat any occurrence
        as a bug to investigate, not something to silently absorb.

        Self-heal (Sep 2026): this used to only log the warning and leave
        the gap open forever. Root cause traced to _execute()'s
        `await self.client.subscribe_contract(...)` call being wrapped in
        a bare try/except that just warns on failure — if that one call
        drops (a transient WS hiccup, a race during reconnect, etc.), the
        contract is permanently invisible to deriv_client's tracking for
        the rest of the process's life: no live close push, and it's also
        skipped by deriv_client's own 30s polling fallback (which only
        walks _polling_contracts). It still gets an eventual authoritative
        check via _handle_orphans()'s age-based force_check_contract() /
        sell_contract() calls (those key off _open_contracts directly, not
        the tracking sets, so they're unaffected by this gap) — but until
        then it sits fully blind, and it blocks the redeploy drain loop
        from ever seeing _open_contracts go empty. Re-issuing
        subscribe_contract() here closes that gap as soon as it's
        detected instead of waiting on the age-based path.
        """
        try:
            tracked_by_client = self.client.get_tracked_contract_ids()
        except Exception:
            return

        ours = set(self._open_contracts.keys())
        # Contracts in reconcile_pending are still legitimately open even
        # though the client-side polling entry for them was already
        # consumed by the mechanism that pushed them into reconciliation —
        # don't flag those as a divergence.
        ours_excl_reconciling = ours - set(self._reconciling.keys())

        missing_from_client = ours_excl_reconciling - tracked_by_client
        if missing_from_client:
            logger.warning(
                f"TRACKING DIVERGENCE: {len(missing_from_client)} contract(s) "
                f"open in bot_engine but not tracked by deriv_client: "
                f"{missing_from_client} — re-registering now"
            )
            for cid in missing_from_client:
                asyncio.create_task(self._reregister_tracking(cid))

    async def _reregister_tracking(self, cid: str) -> None:
        """
        Self-heal half of _check_tracking_divergence() above: re-run the
        exact same subscribe_contract() call _execute() makes right after
        a buy, for a contract that's fallen out of deriv_client's tracking.
        subscribe_contract() unconditionally rewrites _polling_contracts[cid]
        and its own _subscribe_ws_contract() no-ops safely if a subscription
        already exists — safe to call redundantly, never double-subscribes.
        """
        info = self._open_contracts.get(cid)
        if info is None:
            return  # closed/settled between the divergence check and now
        symbol = info.get("symbol", "")
        try:
            await self.client.subscribe_contract(
                cid,
                lambda msg, _cid=cid: asyncio.create_task(
                    self._on_contract_result(_cid, msg)),
                symbol=symbol,
            )
            logger.info(f"TRACKING DIVERGENCE HEALED: {cid} re-registered")
        except Exception as exc:
            logger.warning(
                f"TRACKING DIVERGENCE RE-REGISTER FAILED: {cid} — {exc} "
                f"(will retry on the next dashboard cycle)")

    def _push_dashboard(self):
        risk_s  = self.risk.summary()
        summary = self.journal.session_summary()

        # Implementation Brief v3, task 3 — per-strategy win rate / avg
        # win / avg loss / profit factor / CI, next to the aggregate stats
        # already pushed below. Written directly onto the shared `_status`
        # dict (imported by reference from keep_alive) rather than passed
        # as a new kwarg to update_status() below, since this file doesn't
        # have sight of update_status()'s exact parameter list and a typo'd
        # kwarg there would raise before any of the existing fields get
        # updated. `_status` is already read/written this way elsewhere in
        # this function (recent_trades, balance_history), so this follows
        # the same established pattern.
        try:
            _status["strategy_expectancy"] = self._strategy_expectancy.summary()
        except Exception as exc:
            logger.warning(f"strategy_expectancy dashboard push failed: {exc}")

        # Surface the new global consecutive-loss cooldown the same
        # low-risk way (direct _status write) rather than guessing at
        # update_status()'s exact kwarg list.
        try:
            remaining = max(0.0, self._loss_streak_paused_until - time.time())
            _status["paused_for_consecutive_losses"] = remaining > 0
            _status["consecutive_loss_pause_mins_remaining"] = round(remaining / 60, 1)
            _status["global_consecutive_losses"] = self._global_consecutive_losses
        except Exception as exc:
            logger.warning(f"consecutive-loss status push failed: {exc}")

        oc_list = []
        now_ts  = time.time()
        for cid, info in self._open_contracts.items():
            oc_list.append({
                "contract_id": cid,
                "symbol":      info.get("symbol",    "—"),
                "direction":   info.get("direction",  "—"),
                "stake":       info.get("stake",       0.0),
                "opened_at":   info.get("opened_at",  now_ts),
                "strategy":    info.get("strategy",   ""),
                "multiplier":  info.get("multiplier",  None),
            })

        try:
            update_open_contracts(oc_list)
        except Exception:
            pass

        # Task 5/6 fix — dashboard suspension data source.
        #
        # Root cause (confirmed): the old suspended_symbols=[...] block
        # below read directly from self.symbols._suspension_until, which is
        # SymbolManager's WHOLE-SYMBOL suspension state. Per
        # symbol_manager.py's own record_result() comments, self.symbols
        # .suspend() is called from exactly one place now — the buy-failure
        # circuit breaker in _execute() — confirmed by grepping every call
        # site in this codebase (bot_engine.py:~1387 is the only one).
        # Performance-based throttling for the popular-indicator pipeline
        # is scoped to individual (indicator, symbol) pairs via
        # pair_suspension.py instead, and that data was never wired to the
        # dashboard anywhere — pair_suspension.snapshot() existed but was
        # never called. Net effect: "Suspended Symbols" on the dashboard
        # showed only rare API-failure suspensions, while the pair
        # suspensions operators actually care about day to day (and which
        # explain "wins showing while suspended" — the symbol itself was
        # never blocked, only that one underperforming indicator on it)
        # were completely invisible.
        #
        # Fix: two clearly-separated data sources, matching the task's
        # recommendation —
        #   1. pair_suspension.snapshot() -> update_suspended_symbols() ->
        #      _state["suspended_symbols"] (repurposing the previously-
        #      orphaned update_suspended_symbols() as the one function that
        #      owns this key, instead of update_status() also being able to
        #      write it — that dual-writer setup was the second half of the
        #      bug: two functions could both set _state["suspended_symbols"],
        #      only one of which was actually ever called, and it had the
        #      wrong data).
        #   2. Whole-symbol circuit-breaker suspensions kept, but relabeled
        #      to their own field (circuit_breaker_symbols) in the
        #      update_status() call below instead of being conflated with
        #      per-pair suspensions under the same key.
        try:
            pair_susp_list = [
                {"pair": pair, "seconds_remaining": round(secs, 0)}
                for pair, secs in pair_suspension.snapshot().items()
            ]
            update_suspended_symbols(pair_susp_list)
        except Exception as exc:
            logger.warning(f"pair_suspension dashboard push failed: {exc}")

        circuit_breaker_symbols = [
            {
                "symbol":          s,
                "suspended_until": self.symbols._suspension_until.get(s, 0),
            }
            for s in getattr(self.symbols, "_suspension_until", {})
            if self.symbols.is_suspended(s)
        ]

        update_status(
            running               = True,
            balance               = self.client.balance,
            day_start_balance     = self._day_start_balance,
            session_start_balance = self._session_start_balance,
            paused_for_loss_limit = self._confirmed_paused,
            trades_today          = risk_s.get("total_trades", 0),
            wins_today            = risk_s.get("wins", 0),
            losses_today          = risk_s.get("losses", 0),
            session               = (self.symbols.current_session
                                     if hasattr(self.symbols, "current_session")
                                     else "Active"),
            tradeable_count       = len([s for s in self._htf
                                         if not self.symbols.is_suspended(s)]),
            streak                = self.risk.current_streak,
            recent_trades         = _status.get("recent_trades", []),
            best_symbols          = self.symbols.best_symbols(50),
            balance_history       = _status.get("balance_history", []),
            # Task 5 fix: this used to be suspended_symbols=[...] sourced
            # from self.symbols._suspension_until (whole-symbol, circuit-
            # breaker-only suspension) and update_status() writing straight
            # into _state["suspended_symbols"] — the wrong data, written by
            # a second path that raced/overwrote whatever
            # update_suspended_symbols() (above) had just set from the
            # correct pair_suspension.snapshot() source. Now kept as its
            # own clearly-labeled field instead of overloading the
            # suspended_symbols key a second time.
            circuit_breaker_symbols = circuit_breaker_symbols,
            gross_profit          = summary.get("gross_profit",  0),
            gross_loss            = summary.get("gross_loss",    0),
            profit_factor         = summary.get("profit_factor", 0),
            avg_rr                = summary.get("avg_rr",        0),
            best_trade            = _status.get("best_trade",    summary.get("best_trade",  0)),
            worst_trade           = _status.get("worst_trade",   summary.get("worst_trade", 0)),
            open_contracts_count  = len(oc_list),
            active_trades         = len(self._open_contracts),
        )

    # ── Main loop — continuous, non-blocking ───────────────────────────────────

    async def _main_loop(self):
        scan_sleep   = getattr(config, "SCAN_CYCLE_SLEEP", 1)
        cycle_number = 0

        while True:
            cycle_start = time.time()

            # 1. UTC day rollover → daily-loss-limit bookkeeping only.
            #    Per Implementation Brief v2, Requirement 2 / Fix F: the
            #    escalating per-symbol loss-suspension ladder must NOT
            #    reset on this (or any other) calendar boundary — only a
            #    redeploy (real process restart) resets it, which happens
            #    for free since SymbolManager is reconstructed from
            #    scratch on startup. self.symbols.reset_session() is
            #    deliberately no longer called here. The daily-loss-limit
            #    concept below is separate and legitimate — left on its
            #    existing UTC-midnight boundary per the brief's open
            #    question (not moved to Kenya midnight without explicit
            #    sign-off).
            today = _dt.datetime.utcnow().day
            if today != self._current_utc_day:
                self._confirmed_daily_loss = 0.0
                self._day_start_balance    = self.client.balance
                self._current_utc_day      = today
                logger.info(
                    f"UTC day changed → daily-loss-limit reset "
                    f"(symbol suspension ladder NOT reset — redeploy-only) | "
                    f"day_start_balance=${self._day_start_balance:.4f}")

            if restart_scheduler.is_redeploy_pending():
                await asyncio.sleep(scan_sleep)
                continue

            # ── Fixed two-leg trading cycle (Sep 2026, chat-requested) ──
            # _entering_fixed_cooldown is set by _settle_loop once leg
            # 2's ordinary redeploy was intercepted into a cooldown
            # request AND every open contract has been confirmed closed
            # — stop the main loop for good so run()'s finally block
            # cancels ws_task and actually disconnects from Deriv. While
            # a cooldown has merely been *requested* (leg 1 finishing,
            # contracts still draining) just pause new entries, same as
            # the redeploy-pending pause above.
            if self._entering_fixed_cooldown:
                logger.warning(
                    "FIXED-CYCLE: cooldown starting — stopping main "
                    "loop and disconnecting from Deriv")
                return

            if fixed_cycle.is_cooldown_requested():
                await asyncio.sleep(scan_sleep)
                continue

            cycle_number += 1

            # 2. Refresh active list every cycle in case new symbols initialised,
            #    then get queue of ready symbols
            if self._htf:
                self.symbols.update_active(list(self._htf.keys()))

            queue = self.symbols.get_queue(list(self._initialised_symbols))
            logger.info(
                f"CYCLE {cycle_number} | Queue:{len(queue)} | "
                f"Active:{len(self._htf)} initialised")

            # 3. Scan all queued symbols in parallel — never wait for settles
            raw_results = await asyncio.gather(
                *[self._scan(s) for s in queue],
                return_exceptions=True,
            )

            # 4. Collect all non-None signals
            signals = [r for r in raw_results if isinstance(r, ScanResult)]

            # 4b. Log every raw signal into the ensemble agreement tracker —
            #     unconditionally, so the rolling window has data whether
            #     or not ENSEMBLE_MODE is currently on.
            for r in signals:
                self._log_ensemble_signal(
                    r.symbol, getattr(r.sig, "strategy", "unknown"), r.sig.direction)

            # 5. Filter out symbols that can't trade right now
            signals = [
                r for r in signals
                if self.symbols.can_trade_now(r.symbol)
                and r.symbol not in self._executing_symbols
            ]

            # 5b. Ensemble voting gate — require >=N independent strategies
            #     agreeing on direction within the window before a signal
            #     is even eligible to be ranked/executed. No-op when
            #     config.ENSEMBLE_MODE is False (the default).
            if getattr(config, "ENSEMBLE_MODE", False):
                signals = [r for r in signals
                           if self._ensemble_agrees(r.symbol, r.sig.direction)]

            # 6. Rank by score*0.85 + win_rate*0.15
            for r in signals:
                r.score = self._composite_score(r.sig, r.symbol)

            # 6b. Session/day-of-week weighting — multiplier on the final
            #     score itself (not just the ranking order).
            for r in signals:
                r.score = round(r.score * self._session_dow_weight(r.symbol), 4)

            # 7. Remove duplicates — keep highest scored per symbol
            best_per_symbol: Dict[str, ScanResult] = {}
            for r in signals:
                if (r.symbol not in best_per_symbol
                        or r.score > best_per_symbol[r.symbol].score):
                    best_per_symbol[r.symbol] = r

            # 7b. Thompson-sampling bandit — draws a fresh Beta(alpha,beta)
            #     sample per (strategy, symbol) pair each cycle and uses it
            #     to weight *ranking order only* (0.9x-1.1x band, so it
            #     nudges priority among competing signals without
            #     overriding the composite score). r.score itself (already
            #     including session/dow weighting) stays what's logged.
            for r in best_per_symbol.values():
                bandit_sample = self._bandit.sample(
                    getattr(r.sig, "strategy", "unknown"), r.symbol)
                r.rank_key = round(r.score * (0.9 + 0.2 * bandit_sample), 4)

            ranked: List[ScanResult] = sorted(
                best_per_symbol.values(), key=lambda r: r.rank_key, reverse=True)

            # 8. Execute top N where N = current_concurrent_limit, gated by
            #    concurrent-slot availability AND (FIX, profitability
            #    audit) a per-symbol-family cap — correlated synthetic
            #    indices (e.g. R_10/1HZ10V share the same volatility
            #    parameter) were previously treated as fully independent
            #    slots, understating real concurrent drawdown risk.
            concurrent_limit = self.risk.current_concurrent_limit
            open_count       = len(self._open_contracts)
            available_slots  = max(0, concurrent_limit - open_count)

            family_map      = getattr(config, "SYMBOL_FAMILY_MAP", {})
            max_per_family  = getattr(config, "MAX_CONCURRENT_PER_FAMILY", 2)
            family_open_counts: Dict[str, int] = {}
            for c in self._open_contracts.values():
                fam = family_map.get(c.get("symbol"))
                if fam:
                    family_open_counts[fam] = family_open_counts.get(fam, 0) + 1

            in_loss_streak_pause = time.time() < self._loss_streak_paused_until
            if in_loss_streak_pause and available_slots > 0:
                remaining = (self._loss_streak_paused_until - time.time()) / 60
                logger.info(
                    f"PAUSED: global consecutive-loss cooldown — "
                    f"{remaining:.1f}min remaining, no new entries")

            if kill_switch.is_paused():
                if available_slots > 0:
                    logger.info("PAUSED: dashboard kill switch — no new entries")
                in_loss_streak_pause = True

            _guard_ok, _guard_reason = self._donkey_guard.can_enter()
            if not _guard_ok:
                if available_slots > 0:
                    logger.info(f"PAUSED: donkey guard — {_guard_reason}")
                in_loss_streak_pause = True

            top: List[ScanResult] = []
            if available_slots > 0 and not self._confirmed_paused and not in_loss_streak_pause:
                for r in ranked:
                    if len(top) >= available_slots:
                        break
                    fam = family_map.get(r.symbol)
                    if fam:
                        count = family_open_counts.get(fam, 0)
                        if count >= max_per_family:
                            logger.debug(
                                f"SKIP: {r.symbol} — family {fam} at concurrent "
                                f"cap ({count}/{max_per_family})")
                            continue
                        family_open_counts[fam] = count + 1
                    top.append(r)

            # 9. Execute top signals — unless delayed entry (or, for the
            #    handoff's nine symbols, the parallel stop-as-trigger path)
            #    applies, in which case they're armed and watched instead
            #    of bought immediately.
            #    Handoff (Sep 15 2026): config.STOP_AS_TRIGGER_SYMBOLS is
            #    checked FIRST and, when eligible, takes the symbol
            #    exclusively — those nine symbols now ALWAYS go through
            #    _arm_stop_trigger_entry() and never reach
            #    _arm_pending_entry() (the old DELAYED_ENTRY_* flip design)
            #    or the immediate fixed-pct path below, so only the
            #    handoff's own entry/exit design executes for them. Every
            #    other symbol is completely unaffected — delayed_eligible
            #    below is unchanged from before this pass.
            #    Both pending-order paths are armed on their RAW native
            #    levels — scaled+inverted SL/TP (config.SCALED_SL_TP_ENABLED)
            #    is only applied once a DELAYED_ENTRY_* entry actually
            #    triggers, inside _execute_pending_entry(), against those
            #    original levels — not here. Signals that skip both pending
            #    paths (no native levels, DIGIT contracts, etc.) still get
            #    the fixed-pct transform applied immediately below, though
            #    it's a no-op for anything without native levels anyway.
            if top:
                to_execute: List[ScanResult] = []
                for r in top:
                    s = r.sig
                    has_native_levels = (
                        s.direction in ("LONG", "SHORT")
                        and getattr(s, "contract_kind", "RISE_FALL") == "RISE_FALL"
                        and s.native_entry_price is not None
                        and s.native_stop_price is not None
                        and s.native_target_price is not None
                    )
                    stop_trigger_eligible = (
                        has_native_levels
                        and getattr(config, "STOP_AS_TRIGGER_ENABLED", False)
                        and r.symbol in getattr(config, "STOP_AS_TRIGGER_SYMBOLS", [])
                    )
                    delayed_eligible = (
                        not stop_trigger_eligible
                        and has_native_levels
                        and getattr(config, "DELAYED_ENTRY_ENABLED", False)
                    )
                    if stop_trigger_eligible:
                        self._arm_stop_trigger_entry(r.symbol, s)
                    elif delayed_eligible:
                        self._arm_pending_entry(r.symbol, s)
                    else:
                        # Reserve synchronously, same reasoning as the
                        # delayed-entry trigger path: _active_symbols only
                        # gets set once _execute() finishes, so without this
                        # a slow buy call leaves the symbol "available" for
                        # the next cycle to select and execute again.
                        self._executing_symbols.add(r.symbol)
                        to_execute.append(replace(r, sig=self._apply_fixed_pct_native_levels(s)))

                if to_execute:
                    await asyncio.gather(
                        *[self._execute_and_release(r.symbol, r.sig) for r in to_execute],
                        return_exceptions=True,
                    )

            # 10. Cycle log
            streak = self.risk.current_streak
            logger.info(
                f"CYCLE {cycle_number} | "
                f"Queue:{len(queue)} | "
                f"Signals:{len(signals)} | "
                f"Executing:{len(top)} | "
                f"Open:{len(self._open_contracts)} | "
                f"Balance:${self.client.balance:.4f} | "
                f"Streak:{'+' if streak >= 0 else ''}{streak}")

            elapsed   = time.time() - cycle_start
            remainder = max(0.0, scan_sleep - elapsed)
            await asyncio.sleep(remainder if remainder > 0 else 0.01)

    # ── Per-symbol scan ────────────────────────────────────────────────────────

    async def _scan(self, symbol: str) -> Optional[ScanResult]:
        try:
            builder = self._ltf.get(symbol)
            if builder is None:
                return None

            ltf_bars = builder.completed_bars
            ticks    = list(self._raw_ticks.get(symbol, ()))

            sig = self.signal.evaluate(ltf_bars, symbol, ticks=ticks)
            if sig is None or getattr(sig, "direction", "NONE") == "NONE":
                return None

            price = float(ltf_bars[-1].close) if ltf_bars else 0.0

            return ScanResult(
                symbol  = symbol,
                sig     = sig,
                price   = price,
                smc_ctx = SMCContext(),
                score   = getattr(sig, "score", 0.0),
            )

        except Exception as exc:
            logger.error(f"SCAN ERROR {symbol}: {type(exc).__name__}: {exc}", exc_info=True)
            return None

    # ── Delayed entry — two-trigger design (Sep 12 2026) ─────────────────────────
    # See config.py's "DELAYED ENTRY" block for the spec. Monitoring here is
    # always against the RAW, untouched native levels the indicator computed.

    def _arm_pending_entry(self, symbol: str, sig: SignalResult) -> None:
        """Register a firing signal as pending instead of buying it now."""
        if symbol in self._pending_entries:
            return  # already watching a setup for this symbol — don't reset it
        if symbol in self._executing_symbols:
            return  # a trigger for this symbol is already executing — don't
                     # arm a second one on top of it (defense in depth; the
                     # signals-level filter above should already exclude it)

        entry  = float(sig.native_entry_price)
        stop   = float(sig.native_stop_price)
        target = float(sig.native_target_price)
        pct    = getattr(config, "DELAYED_ENTRY_TRIGGER_PCT", 0.25)
        # Direction-agnostic: (target-entry) already carries the right sign
        # for LONG vs SHORT, so this lands PCT% of the way toward the
        # original target whichever direction that is.
        trigger = entry + pct * (target - entry)

        self._pending_entries[symbol] = PendingEntry(
            symbol=symbol, sig=sig, direction=sig.direction,
            entry_ref_price=entry, stop_ref_price=stop, target_ref_price=target,
            trigger_price=trigger,
            created_at=time.time(), strategy=getattr(sig, "strategy", "unknown"),
        )
        flipped = "SHORT" if sig.direction == "LONG" else "LONG"
        ratio   = getattr(config, "STOP_TRIGGERED_RISK_REWARD_RATIO", 2.0)
        logger.info(
            f"PENDING ENTRY ARMED: {symbol} | indicator called {sig.direction} | "
            f"entry_ref={entry:.5f} | TRIGGER @ {trigger:.5f} ({pct*100:.0f}% "
            f"toward native_target={target:.5f}) | on trigger: enter "
            f"{flipped} (inverted — see _execute_pending_entry for why this "
            f"is forced, not optional), take-profit = native_stop="
            f"{stop:.5f}, stop-loss computed after at 1/{ratio:.1f} of the "
            f"target distance for an exact {ratio:.1f}:1 ratio"
        )


    def _check_pending_entry(self, symbol: str, price: float) -> None:
        """Called on every live tick for a symbol with an armed entry."""
        pe = self._pending_entries.get(symbol)
        if pe is None:
            return

        timeout = getattr(config, "DELAYED_ENTRY_TIMEOUT_SECS", 600)
        if time.time() - pe.created_at > timeout:
            logger.info(
                f"PENDING ENTRY EXPIRED: {symbol} | {pe.direction} | never "
                f"reached trigger (native_stop)={pe.trigger_price:.5f} "
                f"within {timeout}s"
            )
            del self._pending_entries[symbol]
            return

        is_long = pe.direction == "LONG"

        # Trigger: price has moved DELAYED_ENTRY_TRIGGER_PCT of the way
        # toward the indicator's own target. For a LONG the trigger sits
        # above native_entry (toward the target), so "reaching" it means
        # price rising to or above it. For a SHORT it's the mirror.
        triggered = (is_long and price >= pe.trigger_price) or \
                    (not is_long and price <= pe.trigger_price)
        if not triggered:
            return

        del self._pending_entries[symbol]
        # Reserve the symbol THE INSTANT it's decided, synchronously —
        # before asyncio.create_task() even schedules the coroutine.
        # Without this, the symbol sits in neither _pending_entries nor
        # symbol_manager's _active_symbols (that only gets set once
        # _execute() actually completes a buy) for the gap between here
        # and whenever the scheduled task actually runs. Any tick or
        # scan cycle landing in that gap sees an "available" symbol and
        # re-arms/re-executes it — this is exactly how multiple
        # simultaneous same-symbol positions were getting opened.
        # _execute_pending_entry()'s finally block releases this.
        self._executing_symbols.add(symbol)
        flipped = "SHORT" if is_long else "LONG"
        logger.info(
            f"PENDING ENTRY TRIGGERED: {symbol} | indicator called "
            f"{pe.direction} | price={price:.5f} crossed trigger="
            f"{pe.trigger_price:.5f} — entering {flipped} (inverted) at "
            f"{price:.5f}, targeting the original native_stop="
            f"{pe.stop_ref_price:.5f}"
        )
        asyncio.create_task(self._execute_pending_entry(symbol, pe, price))

    async def _execute_pending_entry(
            self, symbol: str, pe: PendingEntry, live_price: float) -> None:
        """
        Fires the actual buy once price has reached the native stop-loss
        (the single trigger — see PendingEntry's docstring). Re-checks
        every guard the normal cycle loop would have applied (can_trade_now,
        concurrent slots, family cap) since this can fire between cycles.

        The whole body runs under a finally that releases this symbol's
        _executing_symbols reservation (taken synchronously in
        _check_pending_entry right when the trigger fired) — on every exit
        path, not just the successful one, so a dropped attempt (no slot,
        family cap, can_trade_now False) doesn't leave the symbol
        permanently un-armable.
        """
        try:
            if kill_switch.is_paused():
                logger.info(
                    f"PENDING ENTRY DROPPED: {symbol} — kill switch engaged")
                return

            if not self.symbols.can_trade_now(symbol):
                logger.info(
                    f"PENDING ENTRY DROPPED: {symbol} — can_trade_now() False at trigger time")
                return

            concurrent_limit = self.risk.current_concurrent_limit
            open_count       = len(self._open_contracts)
            if open_count >= concurrent_limit:
                logger.info(
                    f"PENDING ENTRY DROPPED: {symbol} — no concurrent slot free "
                    f"at trigger time ({open_count}/{concurrent_limit})")
                return

            family_map     = getattr(config, "SYMBOL_FAMILY_MAP", {})
            max_per_family = getattr(config, "MAX_CONCURRENT_PER_FAMILY", 2)
            fam = family_map.get(symbol)
            if fam:
                family_count = sum(
                    1 for c in self._open_contracts.values()
                    if family_map.get(c.get("symbol")) == fam
                )
                if family_count >= max_per_family:
                    logger.info(
                        f"PENDING ENTRY DROPPED: {symbol} — family {fam} at "
                        f"concurrent cap ({family_count}/{max_per_family})")
                    return

            # PERCENT-TRIGGER-TO-NATIVE-STOP design (Sep 12 2026): the
            # trigger is DELAYED_ENTRY_TRIGGER_PCT of the way toward the
            # ORIGINAL native_target — price moving that far confirms the
            # indicator's move looked real. What we then trade is the
            # OPPOSITE of the indicator's call, targeting the ORIGINAL
            # native_stop_price directly:
            #   - take-profit = the original native_stop_price, used as-is.
            #   - direction MUST flip to match: with entry now sitting
            #     between the original entry and target, the original
            #     native_stop_price is on the far side, opposite from where
            #     price just came from — that side is only ever a valid
            #     take-profit for the trade running in the OPPOSITE
            #     direction from the indicator's original call. This isn't
            #     a toggle, it's a geometric consequence of this design —
            #     confirmed with the user before implementing.
            #   - stop-loss, computed AFTER the target: opposite side of
            #     entry from the target, at 1/STOP_TRIGGERED_RISK_REWARD_RATIO
            #     of the target distance — so the ratio is always exactly
            #     STOP_TRIGGERED_RISK_REWARD_RATIO (2:1 default), by
            #     construction, regardless of what the original indicator's
            #     own entry-to-stop distance was.
            # entry_price is overridden to the live trigger price since
            # deriv_client.buy_multiplier() measures its SL/TP dollar
            # distance off whatever entry_price it's given.
            ratio = getattr(config, "STOP_TRIGGERED_RISK_REWARD_RATIO", 2.0)

            new_direction = "SHORT" if pe.direction == "LONG" else "LONG"
            new_target = pe.stop_ref_price
            target_distance = new_target - live_price
            new_stop = live_price - (target_distance / ratio)

            swapped_sig = replace(
                pe.sig,
                direction=new_direction,
                native_stop_price=new_stop,
                native_target_price=new_target,
                native_entry_price=live_price,
                execution_inverted=True,
            )
            await self._execute(symbol, swapped_sig)
        finally:
            self._executing_symbols.discard(symbol)

    # ── Stop-as-trigger entry — parallel design (handoff, Sep 15 2026) ───────────
    # See config.py's "STOP-AS-TRIGGER ENTRY" block for the spec and worked
    # example. Scoped exclusively to config.STOP_AS_TRIGGER_SYMBOLS; every
    # other symbol keeps using _arm_pending_entry/_check_pending_entry/
    # _execute_pending_entry above, unchanged. Monitoring here is always
    # against the RAW, untouched native levels the evaluator computed.

    def _arm_stop_trigger_entry(self, symbol: str, sig: SignalResult) -> None:
        """Register one of the six-evaluator signals on the stop-as-trigger path."""
        if symbol in self._stop_trigger_pending:
            return  # already watching a setup for this symbol — don't reset it
        if symbol in self._executing_symbols:
            return  # a trigger for this symbol is already executing — defense in depth

        entry  = float(sig.native_entry_price)
        stop   = float(sig.native_stop_price)
        target = float(sig.native_target_price)

        self._stop_trigger_pending[symbol] = StopTriggerPendingEntry(
            symbol=symbol, sig=sig, direction=sig.direction,
            entry_ref_price=entry, stop_ref_price=stop, target_ref_price=target,
            created_at=time.time(), strategy=getattr(sig, "strategy", "unknown"),
        )
        ratio = getattr(config, "STOP_AS_TRIGGER_MIN_RR_RATIO", 2.0)
        logger.info(
            f"STOP-AS-TRIGGER ARMED: {symbol} | {sig.direction} | "
            f"entry_ref={entry:.5f} | TRIGGER (native_stop)={stop:.5f} | "
            f"cancel-if-hit-first (native_target)={target:.5f} | on trigger: "
            f"enter {sig.direction} (NOT flipped) at live price, take-profit "
            f"stays {target:.5f}, stop-loss re-derived for a strict "
            f">{ratio:.1f}:1 ratio"
        )

    def _check_stop_trigger_entry(self, symbol: str, price: float) -> None:
        """Called on every live tick for a symbol armed on the stop-as-trigger path."""
        pe = self._stop_trigger_pending.get(symbol)
        if pe is None:
            return

        timeout = getattr(config, "DELAYED_ENTRY_TIMEOUT_SECS", 600)
        if time.time() - pe.created_at > timeout:
            logger.info(
                f"STOP-AS-TRIGGER EXPIRED: {symbol} | {pe.direction} | never "
                f"reached trigger (native_stop)={pe.stop_ref_price:.5f} "
                f"within {timeout}s"
            )
            del self._stop_trigger_pending[symbol]
            return

        is_long = pe.direction == "LONG"

        # Cancel-before-fill (spec point 5): the ORIGINAL native_target_price
        # reached before the trigger ever was -> cancelled outright, no
        # trade. Checked before the trigger check below — for a LONG the
        # target sits above entry and the trigger (native_stop) sits
        # below (mirrored for SHORT), so on any single tick at most one of
        # the two conditions can be true; checking cancel first is simply
        # the safer order if that ever weren't the case.
        target_hit_first = (is_long and price >= pe.target_ref_price) or \
                            (not is_long and price <= pe.target_ref_price)
        if target_hit_first:
            logger.info(
                f"STOP-AS-TRIGGER CANCELLED: {symbol} | {pe.direction} | "
                f"price={price:.5f} reached the original native_target="
                f"{pe.target_ref_price:.5f} before ever reaching the "
                f"trigger (native_stop)={pe.stop_ref_price:.5f} — no trade"
            )
            del self._stop_trigger_pending[symbol]
            return

        triggered = (is_long and price <= pe.stop_ref_price) or \
                    (not is_long and price >= pe.stop_ref_price)
        if not triggered:
            return

        del self._stop_trigger_pending[symbol]
        # Reserve the symbol synchronously — same reasoning as
        # _check_pending_entry's trigger branch above (closes the gap
        # between deciding to trade and _execute() actually completing).
        self._executing_symbols.add(symbol)
        logger.info(
            f"STOP-AS-TRIGGER FIRED: {symbol} | {pe.direction} | price="
            f"{price:.5f} reached trigger (native_stop)={pe.stop_ref_price:.5f} "
            f"— entering {pe.direction} (NOT flipped) at {price:.5f}, "
            f"take-profit stays at original native_target="
            f"{pe.target_ref_price:.5f}"
        )
        asyncio.create_task(self._execute_stop_trigger_entry(symbol, pe, price))

    async def _execute_stop_trigger_entry(
            self, symbol: str, pe: StopTriggerPendingEntry, live_price: float) -> None:
        """
        Fires the actual buy once price has reached the evaluator's own
        native_stop_price (the single trigger — see StopTriggerPendingEntry's
        docstring). Re-checks every guard the normal cycle loop would have
        applied (can_trade_now, concurrent slots, family cap) since this can
        fire between cycles — mirrors _execute_pending_entry()'s guard
        re-checks above.

        Direction is never flipped. Take-profit is the original
        native_target_price, unchanged. Stop-loss is re-derived from the
        live entry-to-target distance so that take_profit_distance /
        stop_loss_distance is strictly greater than
        config.STOP_AS_TRIGGER_MIN_RR_RATIO (see config.py's worked
        example for the exact construction).

        Runs under a finally that releases this symbol's
        _executing_symbols reservation on every exit path, not just the
        successful one, so a dropped attempt doesn't leave the symbol
        permanently un-armable.
        """
        try:
            if kill_switch.is_paused():
                logger.info(
                    f"STOP-AS-TRIGGER DROPPED: {symbol} — kill switch engaged")
                return

            if not self.symbols.can_trade_now(symbol):
                logger.info(
                    f"STOP-AS-TRIGGER DROPPED: {symbol} — can_trade_now() False at trigger time")
                return

            concurrent_limit = self.risk.current_concurrent_limit
            open_count       = len(self._open_contracts)
            if open_count >= concurrent_limit:
                logger.info(
                    f"STOP-AS-TRIGGER DROPPED: {symbol} — no concurrent slot free "
                    f"at trigger time ({open_count}/{concurrent_limit})")
                return

            family_map     = getattr(config, "SYMBOL_FAMILY_MAP", {})
            max_per_family = getattr(config, "MAX_CONCURRENT_PER_FAMILY", 2)
            fam = family_map.get(symbol)
            if fam:
                family_count = sum(
                    1 for c in self._open_contracts.values()
                    if family_map.get(c.get("symbol")) == fam
                )
                if family_count >= max_per_family:
                    logger.info(
                        f"STOP-AS-TRIGGER DROPPED: {symbol} — family {fam} at "
                        f"concurrent cap ({family_count}/{max_per_family})")
                    return

            ratio  = getattr(config, "STOP_AS_TRIGGER_MIN_RR_RATIO", 2.0)
            margin = getattr(config, "STOP_AS_TRIGGER_SL_SAFETY_MARGIN", 0.10)
            is_long = pe.direction == "LONG"
            target  = pe.target_ref_price

            target_distance = (target - live_price) if is_long else (live_price - target)
            if target_distance <= 0:
                # Defensive only — _check_stop_trigger_entry's cancel-before-
                # fill check should make this unreachable in practice, but
                # never derive a stop-loss from a non-positive distance.
                logger.warning(
                    f"STOP-AS-TRIGGER DROPPED: {symbol} — non-positive target "
                    f"distance at fill ({target_distance:.5f}), refusing to trade"
                )
                return

            max_sl_distance = target_distance / ratio
            sl_distance     = max_sl_distance * (1.0 - margin)
            new_stop        = (live_price - sl_distance) if is_long else (live_price + sl_distance)

            swapped_sig = replace(
                pe.sig,
                direction=pe.direction,            # unchanged — NEVER flipped
                native_entry_price=live_price,
                native_stop_price=new_stop,
                native_target_price=target,        # unchanged
                execution_inverted=False,
            )
            logger.info(
                f"STOP-AS-TRIGGER EXECUTING: {symbol} | {pe.direction} | "
                f"fill={live_price:.5f} | take-profit={target:.5f} (unchanged) | "
                f"new stop-loss={new_stop:.5f} | ratio="
                f"{(target_distance / sl_distance):.2f}:1 (> {ratio:.1f} required)"
            )
            await self._execute(symbol, swapped_sig)
        finally:
            self._executing_symbols.discard(symbol)

    # ── Fixed-percentage-of-target native levels (immediate entry) ───────────────
    # See config.py's "FIXED-PERCENTAGE-OF-TARGET NATIVE LEVELS" block for the
    # spec. Currently the only active transform on the immediate-execution path —
    # delayed entry and the scaled+inverted transform are both disabled.

    def _apply_fixed_pct_native_levels(self, sig: SignalResult) -> SignalResult:
        """
        Entry is immediate, exactly as the indicator signals —
        native_entry_price is never modified either way. Both stop-loss and
        take-profit are measured off the SAME entry-to-target distance;
        whether direction flips and which side each level lands on depends
        on config.FIXED_ENTRY_INVERT_DIRECTION:

        True (current default): direction flips (LONG<->SHORT) and both
        levels mirror to the sides that match the FLIPPED direction:
            take_profit = entry - FIXED_ENTRY_TP_PCT * (target - entry)
            stop_loss   = entry + FIXED_ENTRY_SL_PCT * (target - entry)

        False: direction is left alone and the same two distances land on
        their ORIGINAL (non-mirrored) sides instead:
            take_profit = entry + FIXED_ENTRY_TP_PCT * (target - entry)
            stop_loss   = entry - FIXED_ENTRY_SL_PCT * (target - entry)

        Either way both formulas are direction-agnostic: (target - entry)
        already carries the right sign for LONG vs SHORT.
        """
        if not getattr(config, "FIXED_ENTRY_LEVELS_ENABLED", False):
            return sig
        if sig.direction not in ("LONG", "SHORT"):
            return sig
        if getattr(sig, "contract_kind", "RISE_FALL") != "RISE_FALL":
            return sig
        if sig.native_entry_price is None or sig.native_target_price is None:
            return sig

        entry    = float(sig.native_entry_price)
        target   = float(sig.native_target_price)
        tp_pct   = getattr(config, "FIXED_ENTRY_TP_PCT", 0.50)
        sl_pct   = getattr(config, "FIXED_ENTRY_SL_PCT", 0.25)
        invert   = getattr(config, "FIXED_ENTRY_INVERT_DIRECTION", True)
        distance = target - entry

        if invert:
            new_direction = "SHORT" if sig.direction == "LONG" else "LONG"
            new_target    = entry - tp_pct * distance
            new_stop      = entry + sl_pct * distance
        else:
            new_direction = sig.direction
            new_target    = entry + tp_pct * distance
            new_stop      = entry - sl_pct * distance

        fixed = replace(
            sig,
            direction=new_direction,
            native_stop_price=new_stop,
            native_target_price=new_target,
            execution_inverted=invert,
        )
        logger.info(
            f"FIXED-PCT NATIVE LEVELS: {sig.direction}"
            f"{' -> ' + new_direction if invert else ''} | entry={entry:.5f} "
            f"(unchanged) | take_profit={new_target:.5f} ({tp_pct*100:.0f}% "
            f"of entry-to-target) | stop_loss={new_stop:.5f} "
            f"({sl_pct*100:.0f}% of entry-to-target) | "
            f"ratio={tp_pct/sl_pct:.2f}:1"
        )
        return fixed

    # ── Scaled native SL/TP (immediate entry) ────────────────────────────────────
    # See config.py's "SCALED NATIVE SL/TP" block for the spec. Distinct from the
    # delayed-entry machinery above: entry is immediate, exactly as the indicator
    # signaled — only the stop-loss and take-profit levels are recomputed.

    def _apply_scaled_native_levels(self, sig: SignalResult) -> SignalResult:
        """
        config.SCALED_SL_TP_INVERT_DIRECTION True (current default): the
        signal's direction is flipped (LONG<->SHORT) and the new stop/target
        are placed on the sides that now match that flipped direction —
        entry itself is never moved, this just mirrors both levels through
        it. With entry=100, native_stop=90, native_target=130 (a LONG) and
        SL_MULT=0.60/TP_MULT=1.33, the executed trade is a SHORT with
        stop=118 (60% of the original entry-to-target distance, now above
        entry) and target=86.7 (133% of the original entry-to-stop
        distance, now below entry):
            new_stop   = entry + SL_MULT * (native_target - entry)
            new_target = entry - TP_MULT * (entry - native_stop)

        False: direction is left alone and the same two distances are
        scaled onto their ORIGINAL sides instead of mirrored:
            new_target = entry + TP_MULT * (entry - native_stop)
            new_stop   = entry - SL_MULT * (native_target - entry)

        Either way both formulas are direction-agnostic — (native_target -
        entry) and (entry - native_stop) already carry the right sign for
        LONG vs SHORT, same trick used in _arm_pending_entry's trigger
        prices — and entry itself (sig.native_entry_price) is never
        modified by this function either way.
        """
        if not getattr(config, "SCALED_SL_TP_ENABLED", False):
            return sig
        if sig.direction not in ("LONG", "SHORT"):
            return sig
        if getattr(sig, "contract_kind", "RISE_FALL") != "RISE_FALL":
            return sig
        if sig.native_entry_price is None or sig.native_stop_price is None \
                or sig.native_target_price is None:
            return sig

        entry   = float(sig.native_entry_price)
        stop    = float(sig.native_stop_price)
        target  = float(sig.native_target_price)
        tp_mult = getattr(config, "SCALED_TP_STOP_MULT", 1.50)
        sl_mult = getattr(config, "SCALED_SL_TARGET_MULT", 0.35)
        invert  = getattr(config, "SCALED_SL_TP_INVERT_DIRECTION", True)

        if invert:
            new_direction = "SHORT" if sig.direction == "LONG" else "LONG"
            new_target    = entry - tp_mult * (entry - stop)
            new_stop      = entry + sl_mult * (target - entry)
        else:
            new_direction = sig.direction
            new_target    = entry + tp_mult * (entry - stop)
            new_stop      = entry - sl_mult * (target - entry)

        scaled = replace(
            sig,
            direction=new_direction,
            native_stop_price=new_stop,
            native_target_price=new_target,
            execution_inverted=invert,
        )
        logger.info(
            f"SCALED NATIVE SL/TP: {sig.direction}"
            f"{' -> ' + new_direction if invert else ''} | entry={entry:.5f} "
            f"(unchanged) | native_stop={stop:.5f}, native_target={target:.5f} "
            f"-> new_stop={new_stop:.5f} ({sl_mult*100:.0f}% of entry-to-target), "
            f"new_target={new_target:.5f} ({tp_mult*100:.0f}% of entry-to-stop)"
        )
        return scaled

    # ── Execution ──────────────────────────────────────────────────────────────

    async def _execute_and_release(self, symbol: str, sig: SignalResult) -> bool:
        """
        Thin wrapper for the immediate (non-delayed-entry) path: releases
        this symbol's _executing_symbols reservation (taken synchronously
        when it was selected into to_execute, above) once the buy attempt
        is fully resolved — mirrors what _execute_pending_entry's finally
        block does for the delayed-entry path.
        """
        try:
            return await self._execute(symbol, sig)
        finally:
            self._executing_symbols.discard(symbol)

    async def _execute(self, symbol: str, sig: SignalResult) -> bool:
        # Dashboard kill switch — clean early exit (deriv_client's buy
        # functions also hard-block, this just avoids counting a paused
        # buy as a failure / tripping the buy-failure circuit breaker).
        if kill_switch.is_paused():
            return False
        # Hard gate — re-verify right before placing the order
        if not self.symbols.can_trade_now(symbol):
            return False

        strategy = getattr(sig, "strategy", "unknown")

        # Implementation Brief v4 §5.1 — leverage-aware meta-labeling
        # features. multiplier/atr_pct/regime are only fully meaningful
        # for config.VOL_MULTIPLIER_SYMBOLS (regime in particular — Boom/
        # Crash don't run VOL_BREAKOUT/VOL_REV_MULT so ml_regime stays
        # "NONE" for them); everything else that isn't a Multiplier
        # symbol at all (Jump, Bear/Bull, etc.) passes the neutral
        # defaults below so the model can distinguish old-regime rows
        # from new ones rather than crashing on missing fields.
        # ml_atr_pct / ml_kalman_noise_pct are now computed for every
        # symbol in config.MULTIPLIER_SYMBOLS (widened from
        # VOL_MULTIPLIER_SYMBOLS only, Aug 2026 — user-directed: Boom/
        # Crash gets the same minimized stop-loss as everything else, see
        # the buy branch below / risk_manager.compute_dynamic_stop_loss_pct()).
        # Also reused below by that buy branch so neither is computed
        # twice.
        ml_multiplier       = 0.0
        ml_atr_pct          = 0.0
        ml_kalman_noise_pct = 0.0
        ml_regime           = "NONE"
        if symbol in getattr(config, "MULTIPLIER_SYMBOLS", []):
            builder = self._ltf.get(symbol)
            bars = builder.completed_bars if builder else []
            if len(bars) >= 15:
                _C = np.array([b.close for b in bars], dtype=float)
                _H = np.array([b.high  for b in bars], dtype=float)
                _L = np.array([b.low   for b in bars], dtype=float)
                _atr_val = ind.atr(_H, _L, _C, config.ATR_PERIOD)
                _last_atr = float(_atr_val[~np.isnan(_atr_val)][-1]) if len(_atr_val) else 0.0
                ml_atr_pct = _last_atr / float(_C[-1]) if _C[-1] else 0.0

                # MINIMIZED STOP-LOSS (user-directed, Aug 2026): the
                # Kalman filter's own residuals against recent price
                # (actual price minus the filter's smoothed level
                # estimate) are the mathematically principled read of
                # how much this symbol is currently just noise, as
                # opposed to a real move — reused here from the same
                # ind.kalman_trend() the Popular Indicator strategy
                # already calls, so it isn't computed twice. Passed to
                # risk_manager.compute_dynamic_stop_loss_pct() below,
                # which uses it to set the tightest defensible stop.
                try:
                    lookback = getattr(config, "STOP_KALMAN_LOOKBACK", 20)
                    _kalman_level, _ = ind.kalman_trend(
                        _C,
                        getattr(config, "POPULAR_KALMAN_PROCESS_VAR", 1e-5),
                        getattr(config, "POPULAR_KALMAN_MEASURE_VAR", 1e-2),
                    )
                    _residuals = (_C - _kalman_level)[-lookback:]
                    _noise_std = float(np.std(_residuals)) if len(_residuals) else 0.0
                    ml_kalman_noise_pct = _noise_std / float(_C[-1]) if _C[-1] else 0.0
                except Exception as exc:
                    logger.debug(f"kalman noise-floor calc failed for {symbol}: {exc}")
                    ml_kalman_noise_pct = 0.0

            ml_multiplier = float(config.MULTIPLIER_MAP.get(symbol, config.DEFAULT_MULTIPLIER))
            if strategy == "VOL_BREAKOUT":
                ml_regime = "TREND"
            elif strategy == "VOL_REV_MULT":
                ml_regime = "RANGE"

        # ENHANCEMENT (win-rate pass, Aug 2026): meta_labeling.py's per-pair
        # EV model (_PairEVModel, Implementation Brief v6 PART 5) needs
        # ENRICHED_FEATURE_KEYS = (rsi, roc, bb_pct_b, atr_expansion_ratio,
        # hour_utc) — nothing in the codebase ever computed or passed
        # these, so that model was permanently starved regardless of trade
        # count. signal_engine.compute_enriched_features() (PART 2/3,
        # newly added) fills that gap here, from whatever candle history
        # is already being tracked for this symbol — works for any
        # candle-based strategy, not just VOL_MULTIPLIER_SYMBOLS. Reused
        # unchanged at settlement (_apply_settlement) so the same values
        # get logged for training as were used for this gate decision.
        enriched_features: Dict[str, float] = {}
        try:
            _builder = self._ltf.get(symbol)
            _bars = _builder.completed_bars if _builder else []
            if _bars:
                enriched_features = compute_enriched_features(_bars)
        except Exception as exc:
            logger.debug(f"compute_enriched_features({symbol}) failed: {exc}")

        # SIGNAL DIRECTION (user-directed, Sep 2026 through Sep 11 2026) —
        # by the time _execute() sees `sig`, any transform has already
        # happened upstream, once, before this function is ever called:
        #   - config.DELAYED_ENTRY_ENABLED (currently False): would have
        #     replaced native_stop_price/native_target_price/native_entry_price
        #     via _execute_pending_entry(), direction untouched.
        #   - config.SCALED_SL_TP_ENABLED (currently True, see
        #     _apply_scaled_native_levels()): rescales native_stop_price/
        #     native_target_price off the original entry-to-stop /
        #     entry-to-target distances, and — when
        #     config.SCALED_SL_TP_INVERT_DIRECTION is also True (currently
        #     is) — flips direction (LONG<->SHORT) and mirrors those two
        #     scaled levels to the sides matching the flipped direction.
        # Either way _execute() itself never inspects or changes
        # sig.direction / native_stop_price / native_target_price — it just
        # sends whatever it was handed. `inverted` below reflects whether
        # THIS particular trade's direction was flipped from what the
        # indicator originally computed, read off
        # sig.execution_inverted (set only by _apply_scaled_native_levels,
        # defaults False for every other path) — this is what
        # _apply_settlement()'s meta-labeling training-label logic and
        # strategy_stats.get_take_invert_stats() key off; see the comments
        # at those call sites for why the field has to keep existing.
        inverted = bool(getattr(sig, "execution_inverted", False))
        if getattr(sig, "contract_kind", "RISE_FALL") != "RISE_FALL" or sig.direction not in ("LONG", "SHORT"):
            logger.debug(
                f"DIRECTION PASSTHROUGH: {symbol} | {strategy} | inversion "
                f"machinery isn't applicable to contract_kind="
                f"{getattr(sig, 'contract_kind', 'RISE_FALL')} anyway "
                f"(DIGIT-style contracts have no LONG/SHORT to flip)")
        elif inverted:
            logger.info(
                f"SIGNAL SCALED + INVERTED: {symbol} | {strategy} | "
                f"executing {sig.direction} (flipped from the indicator's "
                f"original call) | stop={sig.native_stop_price} "
                f"target={sig.native_target_price}"
            )
        else:
            logger.info(
                f"SIGNAL TAKEN AS-IS: {symbol} | {strategy} | {sig.direction} "
                f"(no inversion) | stop={sig.native_stop_price} "
                f"target={sig.native_target_price}"
            )

        # FIX (profitability audit): this call previously passed no
        # arguments, so RiskManager.calculate_stake()'s Kelly overlay
        # (compute_kelly_fraction / _apply_kelly_overlay) always hit its
        # "no pair context supplied" branch and silently no-op'd — the
        # whole edge-based position-sizing system was dead in production.
        # Passing strategy/symbol lets stake actually shrink toward $0 for
        # pairs with no measured edge and scale toward the Kelly-optimal
        # fraction for pairs that do have one.
        stake     = await self.risk.calculate_stake(strategy=strategy, symbol=symbol)

        # Task 1 root-cause fix — RiskManager.calculate_stake() legitimately
        # returns 0.0 as a "don't trade this pair right now" sentinel in
        # three confirmed cases: the Kelly overlay vetoing a pair with no
        # measured edge (fraction<=0), the exposure ceiling already being
        # full (room<=0), or the post-clamp stake landing below MIN_STAKE.
        # All three are real, working risk decisions — not bugs — but
        # nothing downstream used to check for them before building SL/TP
        # off this stake and calling buy_contract()/buy_multiplier(), which
        # is the confirmed root cause of the $0 stake / $0 SL/TP symptom
        # (both dollar formulas have stake as a direct factor). Aborting
        # here, at the earliest point after the value is known, stops a
        # legitimate "skip this trade" decision from ever being turned into
        # a malformed order. deriv_client.py's buy_*() methods also
        # independently validate stake/SL-TP immediately before every API
        # call as a second-layer safety net — this is the root-cause-level
        # fix, not a substitute for that net.
        if not math.isfinite(stake) or stake <= 0:
            logger.warning(
                f"TRADE ABORTED: {symbol} | {strategy} — RiskManager "
                f"returned a non-tradeable stake (${stake!r}); expected "
                f"when the Kelly overlay vetoes this pair, the exposure "
                f"ceiling is full, or the clamped stake fell below "
                f"MIN_STAKE (see risk_manager.calculate_stake() logs above "
                f"for which). Skipping this trade attempt rather than "
                f"sending a $0 order."
            )
            return False
        # NOTE: for DIGIT signals (JUMP_BUILDUP) this is "MATCH"/"DIFFER",
        # not "LONG"/"SHORT" — passed through below to record_signal(),
        # risk.register_open(), and journal.open_trade() purely as a label.
        # Those call sites appear to treat it as opaque metadata rather than
        # branching on the literal value, but this file doesn't have sight
        # of risk_manager.py/trade_journal.py's internals to confirm that
        # with certainty — flagging rather than silently assuming.
        direction = sig.direction

        record_signal(
            symbol    = symbol,
            direction = direction,
            strategy  = getattr(sig, "strategy", "unknown"),
            score     = getattr(sig, "score",    0.0),
        )

        # Implementation Brief v3, finding #3 / task 2: JUMP_BUILDUP fires a
        # digit-contract recommendation (Matches/Differs), not a price
        # direction — build-up confidence has no LONG/SHORT read, jump
        # direction is 50/50 by design. Previously this fell through to the
        # buy_contract() (CALL/PUT) branch below with a hardcoded direction,
        # i.e. blindly betting a coin flip on a sub-1:1 payout every time it
        # fired. sig.contract_kind=="DIGIT" (set by evaluate_jump_buildup())
        # routes it to the real digit-contract path instead — checked before
        # the Multiplier/Rise-Fall split since it's an orthogonal axis (which
        # contract family, not which symbol).
        if getattr(sig, "contract_kind", "RISE_FALL") == "DIGIT":
            digit = getattr(sig, "digit", None)
            match_type = getattr(sig, "match_type", None)
            if digit is None or match_type is None:
                logger.warning(
                    f"PLACEMENT SKIPPED: {symbol} DIGIT signal missing "
                    f"digit/match_type (digit={digit}, match_type={match_type})"
                )
                buy_resp = None
            else:
                buy_resp = await self.client.buy_digit_contract(
                    symbol     = symbol,
                    stake      = stake,
                    digit      = digit,
                    match_type = match_type,
                )
                if buy_resp:
                    self._donkey_guard.note_entry()
        # Boom/Crash, Jump, and Drift Switch symbols don't support CALL/PUT
        # Rise/Fall on this account — route them to buy_multiplier() instead.
        # config.MULTIPLIER_SYMBOLS is the single source of truth for this
        # split (see config.py's "STRATEGY ROUTING" section); everything
        # else keeps using buy_contract() exactly as before.
        # Implementation Brief v4 §4 / Fix H, widened to all of
        # MULTIPLIER_SYMBOLS (user-directed, Aug 2026) — every Multiplier
        # symbol now gets a stop_loss_pct computed live from ATR/Kalman
        # instead of the static STOP_LOSS_MAP default, since that map was
        # calibrated per symbol by hand and the live read is both tighter
        # and self-adjusting to current volatility (see config.py's
        # STOP_KALMAN_SAFETY_MULT section / risk_manager
        # .compute_dynamic_stop_loss_pct()). This now covers Boom/Crash
        # too, so the plain buy_multiplier() fallback branch below is
        # only reached if DYNAMIC_STOP_LOSS_ENABLED itself is off.
        elif (symbol in getattr(config, "MULTIPLIER_SYMBOLS", [])
                and getattr(config, "DYNAMIC_STOP_LOSS_ENABLED", False)):
            # ml_atr_pct / ml_kalman_noise_pct / ml_multiplier were
            # already computed above for the meta-labeling feature dict
            # — reused here so neither is calculated twice per trade.
            # Popular-indicator pipeline (spec points 3/6): when the signal
            # carries native SL/TP PRICE levels (already inverted+swapped
            # above), those take over entirely — a price-distance stop is
            # exactly what point 3 requires instead of any percentage-of-
            # stake method, so dyn_sl_pct/compute_dynamic_stop_loss_pct()
            # is bypassed on this path (Section 1). Fall back to the old
            # dynamic-ATR percentage path only when native prices aren't
            # available (e.g. this signal didn't come from the popular-
            # indicator evaluator, or its price computation failed).
            if sig.native_stop_price is not None and sig.native_target_price is not None:
                buy_resp = await self.client.buy_multiplier(
                    symbol             = symbol,
                    direction          = direction,
                    stake              = stake,
                    multiplier         = int(ml_multiplier) or config.MULTIPLIER_MAP.get(symbol, config.DEFAULT_MULTIPLIER),
                    strategy           = strategy,
                    stop_loss_price    = sig.native_stop_price,
                    take_profit_price  = sig.native_target_price,
                    entry_price        = sig.native_entry_price,
                )
            else:
                dyn_sl_pct = (
                    self.risk.compute_dynamic_stop_loss_pct(ml_atr_pct, ml_multiplier)
                    if ml_atr_pct > 0 else None
                )
                buy_resp = await self.client.buy_multiplier(
                    symbol        = symbol,
                    direction     = direction,
                    stake         = stake,
                    multiplier    = int(ml_multiplier) or config.MULTIPLIER_MAP.get(symbol, config.DEFAULT_MULTIPLIER),
                    stop_loss_pct = dyn_sl_pct,
                    strategy      = strategy,
                )
        elif symbol in getattr(config, "MULTIPLIER_SYMBOLS", set()):
            if sig.native_stop_price is not None and sig.native_target_price is not None:
                buy_resp = await self.client.buy_multiplier(
                    symbol             = symbol,
                    direction          = direction,
                    stake              = stake,
                    strategy           = strategy,
                    stop_loss_price    = sig.native_stop_price,
                    take_profit_price  = sig.native_target_price,
                    entry_price        = sig.native_entry_price,
                )
            else:
                buy_resp = await self.client.buy_multiplier(
                    symbol   = symbol,
                    direction= direction,
                    stake    = stake,
                    strategy = strategy,
                )
        else:
            buy_resp = await self.client.buy_contract(
                symbol      = symbol,
                direction   = direction,
                stake       = stake,
                multiplier  = getattr(sig, "multiplier",  None),
                stop_loss   = getattr(sig, "stop_loss",   None),
                take_profit = getattr(sig, "take_profit", None),
            )

        if buy_resp is None:
            logger.warning(f"PLACEMENT FAILED: {symbol}")
            record_failure(
                symbol    = symbol,
                direction = direction,
                stake     = stake,
                strategy  = getattr(sig, "strategy", "unknown"),
                reason    = "buy_resp=None",
            )

            # ── Circuit breaker — repeated identical buy failures on the
            #    same symbol previously retried every ~3s indefinitely
            #    with the same broken parameters. Suspend after N in a
            #    row so it stops hammering the API on a bug that a
            #    fast retry loop can't fix.
            streak = self._buy_failure_streak.get(symbol, 0) + 1
            self._buy_failure_streak[symbol] = streak
            threshold = config.BUY_FAILURE_CIRCUIT_BREAKER_THRESHOLD
            if streak >= threshold:
                suspend_mins = config.BUY_FAILURE_CIRCUIT_BREAKER_SUSPEND_MINS
                logger.error(
                    f"CIRCUIT BREAKER: {symbol} hit {streak} consecutive "
                    f"buy failures — suspending {suspend_mins}min")
                self.symbols.suspend(symbol, suspend_mins)
                self._buy_failure_streak[symbol] = 0

            try:
                self._push_dashboard()
            except Exception:
                pass
            return False

        self._buy_failure_streak[symbol] = 0

        cid        = str(buy_resp.get("contract_id", ""))
        bal_before = self.client.balance
        buy_price  = float(buy_resp.get("buy_price", stake))

        rec = self.risk.register_open(
            symbol      = symbol,
            direction   = direction,
            stake       = stake,
            entry_price = buy_price,
        )

        try:
            self.journal.open_trade(
                contract_id    = cid,
                symbol         = symbol,
                direction      = direction,
                stake          = stake,
                entry_price    = buy_price,
                balance_before = bal_before,
                asset_class    = get_symbol_class(symbol),
                htf_bias       = "NEUTRAL",
                smc_structure  = "NONE",
                m1             = getattr(sig, "m1_signal", 0),
                m2             = getattr(sig, "m2_signal", 0),
                m3             = getattr(sig, "m3_signal", getattr(sig, "strength", 0)),
                modules        = getattr(sig, "strength", 0),
            )
        except Exception:
            pass

        self._open_contracts[cid] = {
            "symbol":      symbol,
            "direction":   direction,
            "stake":       stake,
            "entry_price": buy_price,
            "opened_at":   time.time(),
            "rec":         rec,
            "sig":         sig,
            "strategy":    getattr(sig, "strategy",    "unknown"),
            "stop_loss":   getattr(sig, "stop_loss",   None),
            "take_profit": getattr(sig, "take_profit", None),
            "multiplier":  ml_multiplier or getattr(sig, "multiplier", None),
            # Implementation Brief v4 §5.1 — carried through to
            # _apply_settlement() so strategy_stats.stats.record_trade()
            # can log them into the `features` JSON column for
            # meta_labeling.py's leverage-aware training set. Neutral
            # defaults (0.0 / "NONE") for anything outside
            # VOL_MULTIPLIER_SYMBOLS — see ml_atr_pct/ml_regime above.
            "atr_pct":     ml_atr_pct,
            "regime":      ml_regime,
            # Same dict used for the meta-label gate decision above — ride
            # through to _apply_settlement() so strategy_stats.record_trade()
            # logs the exact features this trade was evaluated on, not a
            # freshly recomputed (and by settlement time, stale) snapshot.
            "enriched_features": enriched_features,
            # SIGNAL DIRECTION INVERSION: whether _execute() flipped
            # sig.direction before placing this order. _apply_settlement()
            # needs this to record the outcome meta_labeling trains on
            # against the ORIGINAL direction, not the executed one — see
            # the comment there for why.
            "inverted": inverted,
        }
        self._contract_open_times[cid] = time.time()

        # ── Adaptive Exit Engine — tighter-cadence per-contract monitor for
        #    Multiplier contracts only (open-ended risk; Rise/Fall keeps its
        #    existing fixed-expiry handling untouched). Purely additive: does
        #    not change how the contract above was registered.
        #
        # BYPASSED for popular-indicator-pipeline trades (Section 1 of the
        # Aug 2026 request): this engine's contract_update() calls revise
        # SL/TP post-entry, but the spec fixes SL/TP at entry from the
        # picked indicator's own native price levels (already swapped) and
        # says nothing about moving them afterward — letting a trailing
        # exit adjust them post-entry would silently override that. Gate
        # on native_stop_price/native_target_price being present (i.e. this
        # signal actually came from evaluate_popular_indicator()) rather
        # than symbol membership, so any future non-popular-indicator
        # Multiplier strategy still gets the adaptive exit engine exactly
        # as before.
        has_native_levels = (
            getattr(sig, "native_stop_price", None) is not None
            and getattr(sig, "native_target_price", None) is not None
        )
        if (symbol in getattr(config, "MULTIPLIER_SYMBOLS", set())
                and getattr(config, "EXIT_ENGINE_ENABLED", False)
                and symbol in getattr(config, "EXIT_ENGINE_SYMBOLS", set())
                and not has_native_levels):
            asyncio.create_task(self._monitor_exit(cid))

        self.symbols.record_trade_placed(symbol)
        set_active_trades(len(self._open_contracts))

        logger.info(
            f"▶ {direction} {symbol} | ${stake:.2f} | "
            f"x{getattr(sig, 'multiplier', '?')} | "
            f"SL={getattr(sig, 'stop_loss', '?')} "
            f"TP={getattr(sig, 'take_profit', '?')} | "
            f"streak={self.risk.current_streak}")

        try:
            await self.client.subscribe_contract(
                cid,
                lambda msg, _cid=cid: asyncio.create_task(
                    self._on_contract_result(_cid, msg)))
        except Exception as exc:
            logger.warning(f"subscribe_contract({cid}): {exc}")

        try:
            self._push_dashboard()
        except Exception:
            pass

        return True

    # ── Shared settlement interpretation (Fix C.1, Requirement 1) ──────────────
    #
    # ONE implementation of "how do we turn a real Deriv settlement response
    # into a recorded win/loss", used by _on_contract_result() (WS push /
    # immediate poll), the reconciliation path in _handle_orphans(), and the
    # Multiplier max-hold close path. Must NEVER be called with a fabricated
    # or guessed `poc` — every call site is required to have a real
    # is_sold/is_expired response (or a real sell/profit_table response
    # reshaped into the same field names) before calling this.

    async def _apply_settlement(self, cid: str, info: dict, poc: dict,
                                 close_reason: str = "normal") -> None:
        symbol = info["symbol"]
        stake  = info["stake"]
        rec    = info.get("rec")

        sell_price = float(poc.get("sell_price", 0))
        payout     = float(poc.get("payout",     sell_price))
        pnl        = float(poc.get("profit",     0))
        won        = pnl > 0

        try:
            self.journal.close_trade(
                contract_id   = cid,
                exit_price    = sell_price,
                pnl           = pnl,
                payout        = payout,
                balance_after = self.client.balance,
            )
        except Exception:
            pass

        self.symbols.record_contract_closed(symbol)
        self.symbols.record_result(symbol, won=won)
        try:
            self.risk.register_close(rec, exit_price=sell_price, pnl=pnl)
        except Exception:
            pass

        strategy    = info.get("strategy", "unknown")
        entry_score = float(getattr(info.get("sig"), "score", 0.0))

        # Implementation Brief v4 §5.1 — leverage-aware training data.
        # multiplier/atr_pct/regime were stashed onto `info` at open time
        # (see _execute()) and ride into strategy_stats' existing
        # `features` JSON column here — no schema change needed, and
        # meta_labeling._load_all_trades() already knows how to decode
        # them back out. Neutral for anything outside
        # config.VOL_MULTIPLIER_SYMBOLS (multiplier=0.0, atr_pct=0.0,
        # regime="NONE"), same as the meta-labeling call site.
        ml_features = {
            "multiplier": info.get("multiplier") or 0.0,
            "atr_pct":    info.get("atr_pct", 0.0),
            "regime":     info.get("regime", "NONE"),
            # See _execute() — same values the meta-label gate decided on.
            **info.get("enriched_features", {}),
            # SIGNAL DIRECTION INVERSION: _PairEVModel._fit() (meta_labeling.py)
            # reads this back out to flip the training label for inverted
            # trades — see the comment there for why that matters.
            "inverted": bool(info.get("inverted", False)),
        }

        # Feed strategy_stats — this is the source of truth the
        # meta-labeling filter and the Thompson bandit both read from,
        # so it has to be populated for either of those to do anything.
        try:
            strategy_stats.stats.record_trade(
                strategy    = strategy,
                symbol      = symbol,
                entry_score = entry_score,
                won         = won,
                stake       = stake,
                payout      = payout,
                features    = ml_features,
            )
        except Exception as exc:
            logger.warning(f"strategy_stats.record_trade({symbol}) failed: {exc}")

        # Implementation Brief v3, task 3 — same call site as
        # strategy_stats.stats.record_trade() above, so both always agree
        # on trade count.
        try:
            self._strategy_expectancy.record(strategy=strategy, won=won, pnl=pnl)
        except Exception as exc:
            logger.warning(f"strategy_expectancy.record({symbol}) failed: {exc}")

        # Backfill the outcome onto the oldest pending meta-label
        # prediction for this pair, for later validation of the filter.
        try:
            meta_labeling.record_outcome(strategy=strategy, symbol=symbol, won=won)
        except Exception as exc:
            logger.warning(f"meta_labeling.record_outcome({symbol}) failed: {exc}")

        if pnl < 0:
            self._confirmed_daily_loss += abs(pnl)
        self._check_confirmed_loss_limit()

        # Donkey guard: edge log (real payout, breakeven) + session limits.
        if str(strategy).startswith("DONKEY"):
            try:
                _sig = info.get("sig")
                self._donkey_guard.record(
                    symbol      = symbol,
                    signal_kind = signal_kind_from_reason(getattr(_sig, "reason", "")),
                    contract    = str(getattr(_sig, "match_type", "") or ""),
                    barrier     = getattr(_sig, "digit", ""),
                    stake       = stake,
                    payout      = payout,
                    won         = won,
                    pnl         = pnl,
                )
            except Exception as exc:
                logger.warning(f"donkey_guard.record({symbol}) failed: {exc}")

        # ── Global consecutive-loss circuit breaker ─────────────────────
        if won:
            self._global_consecutive_losses = 0
        else:
            self._global_consecutive_losses += 1
            limit = getattr(config, "GLOBAL_CONSECUTIVE_LOSS_LIMIT", 4)
            if self._global_consecutive_losses >= limit:
                pause_mins = getattr(config, "GLOBAL_CONSECUTIVE_LOSS_PAUSE_MINS", 45)
                self._loss_streak_paused_until = time.time() + pause_mins * 60
                logger.warning(
                    f"GLOBAL CONSECUTIVE LOSS LIMIT HIT — {self._global_consecutive_losses} "
                    f"losses in a row across all symbols/strategies — pausing all new "
                    f"entries for {pause_mins}min")

        set_active_trades(len(self._open_contracts))
        update_status(streak=self.risk.current_streak)

        try:
            record_trade(
                symbol        = symbol,
                direction     = info.get("direction", "?"),
                stake         = stake,
                pnl           = pnl,
                balance_after = self.client.balance,
                won           = won,
                strategy      = strategy,
                multiplier    = info.get("multiplier", None),
                close_reason  = close_reason,
            )
        except Exception:
            pass

        try:
            self._push_dashboard()
        except Exception:
            pass

        streak = self.risk.current_streak
        logger.info(
            f"{'✅ WIN' if won else '❌ LOSS'} | "
            f"{symbol} | {strategy} | "
            f"pnl=${pnl:+.4f} | "
            f"balance=${self.client.balance:.4f} | "
            f"streak={streak} | close_reason={close_reason}")

    # ── Contract result callback ───────────────────────────────────────────────

    async def _on_contract_result(self, cid: str, msg: dict):
        poc = msg.get("proposal_open_contract", {})
        if not poc.get("is_sold"):
            return

        info = self._open_contracts.pop(cid, None)
        self._contract_open_times.pop(cid, None)
        self._reconciling.pop(cid, None)
        if not info:
            return

        try:
            self.client.stop_tracking(cid)
        except Exception:
            pass

        await self._apply_settlement(cid, info, poc, close_reason="normal")

    # ── Settle loop — independent of the scan loop ─────────────────────────────

    async def _settle_loop(self):
        settle_wait    = getattr(config, "SETTLE_WAIT_SECS", 15)
        redeploy_every = getattr(config, "REDEPLOY_EVERY_N_CYCLES", 6)
        drain_max_secs = DRAIN_MAX_SECS

        while True:
            try:
                await asyncio.sleep(settle_wait)

                await self._handle_orphans()
                self._check_confirmed_loss_limit()

                self._cycle_count += 1

                # Implementation Brief v2, Fix G: restart_scheduler's daily
                # Kenya-midnight timer is now the single authoritative
                # redeploy trigger (is_redeploy_pending() flips True once
                # per day). REDEPLOY_EVERY_N_CYCLES stays disabled at
                # 999999 by default (see config.py) — kept only so this
                # doesn't silently break if it's ever deliberately lowered.
                redeploy_wanted = (
                    self._cycle_count >= redeploy_every
                    or restart_scheduler.is_redeploy_pending()
                )

                # ── Fixed single-leg cycle: redeploy-due check (Sep 2026,
                # chat-requested; restructured same day to drop leg 2)
                # ──────────────────────────────────────────────────────
                # Checked ONLY here, at the moment an ordinary redeploy is
                # actually about to be allowed through — NOT on every
                # settle tick, so it can't fire ahead of a real redeploy
                # being due. Leg 1 is the only leg now, so there's no
                # leg-counter check left to make: the rolling-redeploy
                # timer firing always means leg 1 just finished, so this
                # always redirects into the cooldown instead
                # (fixed_cycle.request_cooldown()), which the drain block
                # further down picks up on this same tick. The plain
                # "redeploy and keep trading" path below is effectively
                # dead under this trigger now — kept only as a fallback
                # for the (disabled, REDEPLOY_EVERY_N_CYCLES=999999)
                # cycle-count trigger, which never goes through
                # fixed_cycle at all.
                if restart_scheduler.is_redeploy_pending():
                    try:
                        fixed_cycle.on_redeploy_due()
                    except Exception as exc:
                        logger.error(f"FIXED-CYCLE: redeploy check error: {exc}")

                # Ordinary redeploy is suppressed for this tick if the
                # check above just requested the cooldown — the drain
                # block below handles draining+disconnecting from here
                # on, not this plain redeploy path.
                if fixed_cycle.is_cooldown_requested():
                    redeploy_wanted = False

                if redeploy_wanted:
                    n_open = len(self._open_contracts)
                    logger.info(
                        f"REDEPLOY TRIGGERED: draining {n_open} open "
                        f"contract(s) before restart")

                    drain_started = time.time()
                    while self._open_contracts:
                        # Actively try to confirm-close everything
                        # remaining — reuses the exact same reconciliation
                        # (Fix C) and Multiplier max-hold (Fix E) machinery
                        # as normal operation. Never a guess.
                        await self._handle_orphans()
                        if not self._open_contracts:
                            break

                        drain_elapsed = time.time() - drain_started
                        if drain_elapsed >= drain_max_secs:
                            stuck = [
                                f"{cid} (age={int(time.time() - info.get('opened_at', time.time()))}s)"
                                for cid, info in self._open_contracts.items()
                            ]
                            logger.warning(
                                f"DRAIN_MAX_SECS ({drain_max_secs}s) exceeded "
                                f"with {len(self._open_contracts)} contract(s) "
                                f"still not confirmed-closed: {', '.join(stuck)} "
                                f"— DELAYING the redeploy rather than wiping "
                                f"their bookkeeping (Requirement 1). Will keep "
                                f"actively trying to close them and re-check "
                                f"on the next settle tick."
                            )
                            break

                        logger.info(
                            f"Draining — {len(self._open_contracts)} "
                            f"contract(s) open, actively confirming closes")
                        await asyncio.sleep(5)

                    if not self._open_contracts:
                        # No leg to advance anymore (single-leg cycle) —
                        # this branch only still exists for the disabled
                        # cycle-count fallback; under the timer trigger
                        # fixed_cycle.on_redeploy_due() above always
                        # requests a cooldown before redeploy_wanted gets
                        # here, so in practice this path isn't taken by
                        # the normal leg-1-ends flow (see cooldown drain
                        # block below).
                        restart_scheduler.trigger_redeploy()
                        logger.info("Redeploy triggered — standing by")
                        self._cycle_count = 0
                    # else: redeploy stays pending. We deliberately do NOT
                    # clear _open_contracts / _contract_open_times here —
                    # the next settle tick will re-enter this branch and
                    # keep trying to drain for real.

                # ── Fixed single-leg cycle cooldown drain (Sep 2026,
                # chat-requested; restructured same day to drop leg 2 and
                # randomize the cooldown length) ─────────────────────────
                # Same non-destructive drain discipline as the redeploy
                # branch above, triggered by fixed_cycle.on_redeploy_due()
                # — leg 1 is the only leg, so its rolling-redeploy timer
                # firing always requests this cooldown. Once every open
                # contract is confirmed closed, fixed_cycle.enter_
                # cooldown_now() draws a fresh random cooldown length
                # (between config.FIXED_CYCLE_COOLDOWN_MIN_MINUTES and
                # _MAX_MINUTES) and persists that window (so it survives
                # the upcoming redeploy, and any other redeploy landing
                # mid-cooldown), then starts the cooldown supervisor
                # thread; _entering_fixed_cooldown then tells _main_loop
                # to stop itself so run()'s finally block disconnects
                # from Deriv.
                if fixed_cycle.is_cooldown_requested() and not self._entering_fixed_cooldown:
                    n_open = len(self._open_contracts)
                    logger.warning(
                        f"FIXED-CYCLE COOLDOWN REQUESTED: draining {n_open} "
                        f"open contract(s) before disconnecting")

                    drain_started = time.time()
                    while self._open_contracts:
                        await self._handle_orphans()
                        if not self._open_contracts:
                            break

                        drain_elapsed = time.time() - drain_started
                        if drain_elapsed >= drain_max_secs:
                            stuck = [
                                f"{cid} (age={int(time.time() - info.get('opened_at', time.time()))}s)"
                                for cid, info in self._open_contracts.items()
                            ]
                            logger.warning(
                                f"DRAIN_MAX_SECS ({drain_max_secs}s) exceeded "
                                f"with {len(self._open_contracts)} contract(s) "
                                f"still not confirmed-closed: {', '.join(stuck)} "
                                f"— DELAYING the fixed-cycle cooldown rather "
                                f"than wiping their bookkeeping. Will keep "
                                f"actively trying to close them and re-check "
                                f"on the next settle tick."
                            )
                            break

                        logger.info(
                            f"Draining before fixed-cycle cooldown — "
                            f"{len(self._open_contracts)} contract(s) open, "
                            f"actively confirming closes")
                        await asyncio.sleep(5)

                    if not self._open_contracts:
                        # enter_cooldown_now() draws + persists a fresh
                        # random cooldown length itself (see fixed_cycle.
                        # py) and logs it there; just report the
                        # resulting deadline here.
                        cooldown_until = fixed_cycle.enter_cooldown_now()
                        cooldown_min = max(0.0, cooldown_until - time.time()) / 60
                        logger.warning(
                            f"FIXED-CYCLE COOLDOWN ENTERED — ~{cooldown_min:.1f} "
                            f"min, resuming on a fresh leg 1 via redeploy at "
                            f"{_dt.datetime.fromtimestamp(cooldown_until, tz=_dt.timezone.utc).isoformat()}")
                        self._entering_fixed_cooldown = True
                    # else: cooldown stays requested-but-not-entered, same
                    # non-destructive re-try-next-tick pattern as above.

            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error(f"_settle_loop: {exc}")

    # ── Orphan handling ─────────────────────────────────────────────────────────

    async def _handle_orphans(self):
        now = time.time()
        multiplier_symbols = getattr(config, "MULTIPLIER_SYMBOLS", set())

        for cid, info in list(self._open_contracts.items()):
            opened_at = info.get("opened_at", now)
            age       = now - opened_at
            symbol    = info.get("symbol", "UNKNOWN")

            # Multiplier contracts (no fixed expiry) get their own explicit,
            # active-close-only path (Fix E) — never the Rise/Fall
            # age-based logic below.
            if symbol in multiplier_symbols:
                await self._handle_multiplier_orphan(cid, info, age)
                continue

            # Already in the reconcile-pending state from a previous tick —
            # keep polling on its own cadence rather than re-deriving age.
            if cid in self._reconciling:
                await self._reconcile_pending_contract(cid, info)
                continue

            if age >= CONTRACT_FORCE_CLOSE_SECS:
                await self._begin_reconciliation(cid, info)
            elif age >= CONTRACT_MAX_AGE_SECS:
                try:
                    await self.client.force_check_contract(cid)
                except Exception as exc:
                    logger.warning(f"force_check_contract({cid}): {exc}")

    # ── Reconciliation entry point (Fix C) ──────────────────────────────────
    #
    # Replaces the old "just declare a loss" ORPHAN_TIMEOUT branch. A
    # Rise/Fall contract past CONTRACT_FORCE_CLOSE_SECS gets exactly one
    # more authoritative check; if it's genuinely settled, it's recorded
    # for real through the shared _apply_settlement() helper (same logic
    # _on_contract_result() uses). If not, it moves into reconcile_pending
    # and is retried — it is NEVER marked win/loss on a guess.

    async def _begin_reconciliation(self, cid: str, info: dict) -> None:
        symbol = info.get("symbol", "UNKNOWN")
        try:
            poc = await self.client.force_check_contract(cid)
        except Exception as exc:
            logger.warning(f"force_check_contract({cid}) at reconcile-start: {exc}")
            poc = {}

        if poc.get("is_sold") or poc.get("is_expired"):
            self._open_contracts.pop(cid, None)
            self._contract_open_times.pop(cid, None)
            self._reconciling.pop(cid, None)
            try:
                self.client.stop_tracking(cid)
            except Exception:
                pass
            await self._apply_settlement(cid, info, poc, close_reason="normal")
            return

        now = time.time()
        self._reconciling[cid] = {"reconcile_started_at": now, "last_poll": now}
        logger.warning(
            f"RECONCILE PENDING: {cid} ({symbol}) still open after "
            f"{CONTRACT_FORCE_CLOSE_SECS}s — polling every "
            f"{RECONCILE_POLL_INTERVAL_SECS}s until a real result arrives; "
            f"dashboard keeps it visibly open/pending, never a guessed result"
        )
        try:
            self._push_dashboard()
        except Exception:
            pass

    async def _reconcile_pending_contract(self, cid: str, info: dict) -> None:
        state = self._reconciling.get(cid)
        if not state:
            return

        now = time.time()
        if now - state.get("last_poll", 0) < RECONCILE_POLL_INTERVAL_SECS:
            return
        state["last_poll"] = now

        symbol = info.get("symbol", "UNKNOWN")
        try:
            poc = await self.client.force_check_contract(cid)
        except Exception as exc:
            logger.warning(f"force_check_contract({cid}) during reconcile: {exc}")
            poc = {}

        elapsed = now - state["reconcile_started_at"]

        if poc.get("is_sold") or poc.get("is_expired"):
            self._open_contracts.pop(cid, None)
            self._contract_open_times.pop(cid, None)
            self._reconciling.pop(cid, None)
            try:
                self.client.stop_tracking(cid)
            except Exception:
                pass
            await self._apply_settlement(cid, info, poc, close_reason="reconcile_delayed")
            return

        if elapsed >= RECONCILE_MAX_SECS:
            # Last-resort truth source (Fix C.3) — a different real query,
            # never a guess. If it doesn't find anything either, keep
            # retrying in the background; the contract stays visibly
            # open/pending forever if it has to, but never gets a
            # fabricated win/loss.
            try:
                pt = await self.client.profit_table_lookup(cid)
            except Exception as exc:
                logger.error(f"profit_table_lookup({cid}) failed: {exc}")
                pt = {}

            if pt.get("is_sold"):
                self._open_contracts.pop(cid, None)
                self._contract_open_times.pop(cid, None)
                self._reconciling.pop(cid, None)
                try:
                    self.client.stop_tracking(cid)
                except Exception:
                    pass
                await self._apply_settlement(cid, info, pt, close_reason="reconcile_delayed")
                return

            logger.error(
                f"RECONCILE STILL PENDING: {cid} ({symbol}) unresolved "
                f"after {elapsed:.0f}s (ceiling {RECONCILE_MAX_SECS}s) — "
                f"escalating loudly; still NOT recording a guessed result, "
                f"will keep retrying in the background"
            )

    # ── Multiplier contracts — explicit, active closing only (Fix E) ───────

    async def _handle_multiplier_orphan(self, cid: str, info: dict, age: float) -> None:
        if age < MULTIPLIER_MAX_HOLD_SECS:
            return

        symbol = info.get("symbol", "UNKNOWN")
        logger.info(
            f"MULTIPLIER MAX-HOLD: {cid} ({symbol}) held {age:.0f}s >= "
            f"{MULTIPLIER_MAX_HOLD_SECS}s — actively selling to realize the "
            f"real current price (never a guess, per Fix E / Requirement 1)"
        )
        try:
            sell_resp = await self.client.sell_contract(cid, price=0)
        except Exception as exc:
            logger.error(f"sell_contract({cid}) failed during max-hold close: {exc}")
            sell_resp = None

        if not sell_resp:
            # Sell failed — the classic cause (matches the logs you're
            # seeing: "ContractNotFound: This contract was not found among
            # your open positions") is that Deriv already considers this
            # contract closed — e.g. it hit its own limit_order stop-out,
            # or the proposal_open_contract close push never reached us
            # (a subscription dropped/missed across a reconnect) — while
            # our local bookkeeping never got the memo. Retrying a sell on
            # a contract that no longer exists is a guaranteed infinite
            # loop: it fails the same way forever, age keeps climbing, and
            # the position sits "open" in our books indefinitely (this is
            # what produced the 15-16h "stuck" positions). Before giving up
            # for this cycle, run one authoritative check — same
            # force_check_contract → profit_table_lookup fallback Fix C
            # already uses for Rise/Fall — and settle for real if Deriv
            # confirms it's actually closed.
            try:
                poc = await self.client.force_check_contract(cid)
            except Exception as exc:
                logger.warning(f"force_check_contract({cid}) after failed sell: {exc}")
                poc = {}

            if not (poc.get("is_sold") or poc.get("is_expired")):
                try:
                    poc = await self.client.profit_table_lookup(cid)
                except Exception as exc:
                    logger.warning(f"profit_table_lookup({cid}) after failed sell: {exc}")
                    poc = {}

            if poc.get("is_sold") or poc.get("is_expired"):
                logger.info(
                    f"MULTIPLIER ALREADY CLOSED: {cid} ({symbol}) — sell "
                    f"failed because Deriv already considers it closed; "
                    f"settling from the authoritative check instead of "
                    f"retrying a sell forever."
                )
                self._open_contracts.pop(cid, None)
                self._contract_open_times.pop(cid, None)
                self._reconciling.pop(cid, None)
                try:
                    self.client.stop_tracking(cid)
                except Exception:
                    pass
                await self._apply_settlement(cid, info, poc, close_reason="orphan_already_closed")
                return

            # Genuinely still open and the sell call itself failed for some
            # other (presumably transient) reason — this contract must
            # NEVER be removed from _open_contracts without a confirmed
            # close. Leave it open and tracked; retry next
            # _handle_orphans tick.
            logger.warning(
                f"MULTIPLIER SELL FAILED: {cid} ({symbol}) — still open, "
                f"will retry active close next cycle (no bookkeeping wiped)"
            )
            return

        # One authoritative follow-up check to get profit/sell_price in the
        # same shape _apply_settlement() expects.
        try:
            poc = await self.client.force_check_contract(cid)
        except Exception:
            poc = {}

        if not (poc.get("is_sold") or poc.get("is_expired")):
            # Fall back to the sell response itself — sold_for is a real,
            # confirmed value even if the follow-up check hasn't caught up.
            sold_for = float(sell_resp.get("sold_for", 0))
            stake    = float(info.get("stake", 0.0))
            poc = {
                "is_sold":    1,
                "sell_price": sold_for,
                "profit":     sold_for - stake,
                "payout":     sold_for,
            }

        self._open_contracts.pop(cid, None)
        self._contract_open_times.pop(cid, None)
        self._reconciling.pop(cid, None)
        try:
            self.client.stop_tracking(cid)
        except Exception:
            pass
        await self._apply_settlement(cid, info, poc, close_reason="time_based_close")

    # ── Adaptive Exit Engine — per-contract monitoring loop ─────────────────
    #
    # Runs at config.EXIT_POLL_INTERVAL_SECS (default 15s) — tighter than the
    # general _handle_orphans sweep (30s) — because exit timing matters more
    # for open-ended Multiplier risk than the general health-check sweep
    # does. This is purely additive: _handle_multiplier_orphan's 30-minute
    # MULTIPLIER_MAX_HOLD_SECS force-close remains the untouched outer safety
    # bound and still fires if this task ever stops running (e.g. a restart
    # wipes the task but not yet the open-contract record).
    #
    # Never calls _apply_settlement for a natural close (is_sold/is_expired
    # discovered on a poll) — that's owned by _on_contract_result (WS push)
    # or the orphan sweep. Only calls it for a close *this method* actively
    # triggers via sell_contract (CLOSE_NOW), reusing the same shared
    # settlement path everything else uses.

    async def _monitor_exit(self, cid: str) -> None:
        poll_interval = getattr(config, "EXIT_POLL_INTERVAL_SECS", 15)

        while cid in self._open_contracts:
            try:
                await asyncio.sleep(poll_interval)

                # Re-check after the sleep — the contract may have settled
                # via the WS push path or the orphan sweep while we waited.
                info = self._open_contracts.get(cid)
                if info is None:
                    return

                try:
                    poc = await self.client.force_check_contract(cid)
                except Exception as exc:
                    logger.warning(f"_monitor_exit force_check_contract({cid}): {exc}")
                    continue

                if poc.get("is_sold") or poc.get("is_expired"):
                    # Natural close — the existing settlement path handles
                    # recording it. Not our job; just stop monitoring.
                    return

                symbol      = info.get("symbol", "UNKNOWN")
                opened_at   = info.get("opened_at", time.time())
                elapsed     = time.time() - opened_at
                live_profit = float(poc.get("profit", 0.0))

                # BUG FIX (found while implementing the ALT method below,
                # unrelated to any of this session's other changes): these
                # calls never matched exit_engine.record_snapshot()'s /
                # decide_exit()'s actual parameter names (elapsed_secs,
                # stake, current_profit, static_sl_amount, static_tp_amount,
                # multiplier — not symbol/profit/elapsed/poc). Every call
                # has been raising TypeError and getting silently swallowed
                # by the except block below since this was written — the
                # entire rule-based trailing layer and its ML layer have
                # been inert this whole time, independent of the
                # EXIT_ARM_PROFIT_FRACTION / EXIT_TRAIL_LOCK_FRACTION /
                # EXIT_DECAY_CLOSE_FRACTION retuning done earlier this
                # session, which was correct but had nothing to act on.
                # static_sl/tp_amount come from the contract's own live
                # limit_order (ground truth from Deriv, not a
                # recomputation that could drift from what was actually
                # set) — poc.get("limit_order") is documented Deriv API
                # behavior for contracts with a limit_order attached.
                stake = float(info.get("stake", 0.0))
                multiplier = int(info.get("multiplier")
                                  or config.MULTIPLIER_MAP.get(symbol, config.DEFAULT_MULTIPLIER))
                limit_order = poc.get("limit_order") or {}
                static_sl_amount = float(limit_order.get("stop_loss") or 0.0)
                static_tp_amount = float(limit_order.get("take_profit") or 0.0)

                try:
                    exit_engine.record_snapshot(
                        cid,
                        symbol            = symbol,
                        elapsed_secs      = elapsed,
                        stake             = stake,
                        current_profit    = live_profit,
                        static_sl_amount  = static_sl_amount,
                        static_tp_amount  = static_tp_amount,
                        multiplier        = multiplier,
                    )
                    decision = exit_engine.decide_exit(
                        cid,
                        symbol            = symbol,
                        elapsed_secs      = elapsed,
                        stake             = stake,
                        current_profit    = live_profit,
                        static_sl_amount  = static_sl_amount,
                        static_tp_amount  = static_tp_amount,
                        multiplier        = multiplier,
                    )
                except Exception as exc:
                    logger.warning(f"exit_engine decision failed for {cid}: {exc}")
                    continue

                action = getattr(decision, "action", None)

                if action == "TRAIL_UPDATE":
                    try:
                        await self.client.contract_update(
                            cid, stop_loss=decision.new_stop_loss)
                        logger.info(
                            f"ADAPTIVE EXIT TRAIL: {cid} ({symbol}) "
                            f"stop_loss -> {decision.new_stop_loss}")
                    except Exception as exc:
                        logger.warning(f"contract_update({cid}) trail failed: {exc}")
                    continue

                if action == "CLOSE_NOW":
                    logger.info(
                        f"ADAPTIVE EXIT CLOSE: {cid} ({symbol}) elapsed="
                        f"{elapsed:.0f}s profit={live_profit:.4f} — actively "
                        f"selling to realize the real current price")
                    try:
                        sell_resp = await self.client.sell_contract(cid, price=0)
                    except Exception as exc:
                        logger.error(f"sell_contract({cid}) failed during adaptive exit: {exc}")
                        sell_resp = None

                    if not sell_resp:
                        # Same failure mode as _handle_multiplier_orphan's
                        # max-hold close (ContractNotFound because Deriv
                        # already considers this closed — its own
                        # limit_order stop-out fired, or a WS close push
                        # was dropped across a reconnect). Run the same
                        # authoritative check before assuming it's still
                        # open, or this retries a dead sell forever exactly
                        # like the max-hold path did.
                        try:
                            check_poc = await self.client.force_check_contract(cid)
                        except Exception as exc:
                            logger.warning(f"force_check_contract({cid}) after failed adaptive-exit sell: {exc}")
                            check_poc = {}

                        if not (check_poc.get("is_sold") or check_poc.get("is_expired")):
                            try:
                                check_poc = await self.client.profit_table_lookup(cid)
                            except Exception as exc:
                                logger.warning(f"profit_table_lookup({cid}) after failed adaptive-exit sell: {exc}")
                                check_poc = {}

                        if check_poc.get("is_sold") or check_poc.get("is_expired"):
                            logger.info(
                                f"ADAPTIVE EXIT ALREADY CLOSED: {cid} ({symbol}) "
                                f"— sell failed because Deriv already considers "
                                f"it closed; settling from the authoritative "
                                f"check instead of retrying a sell forever."
                            )
                            self._open_contracts.pop(cid, None)
                            self._contract_open_times.pop(cid, None)
                            self._reconciling.pop(cid, None)
                            try:
                                self.client.stop_tracking(cid)
                            except Exception:
                                pass
                            await self._apply_settlement(
                                cid, info, check_poc, close_reason="adaptive_exit_already_closed")
                            try:
                                exit_engine.record_closed(cid, float(check_poc.get("profit", 0.0)))
                            except Exception as exc:
                                logger.warning(f"exit_engine.record_closed({cid}) failed: {exc}")
                            return

                        # No confirmed close — never drop bookkeeping without
                        # one (same invariant _handle_multiplier_orphan
                        # follows). Leave it open/tracked; retry next tick.
                        logger.warning(
                            f"ADAPTIVE EXIT SELL FAILED: {cid} ({symbol}) — "
                            f"still open, will retry next tick")
                        continue

                    # One authoritative follow-up check, same pattern as
                    # _handle_multiplier_orphan's max-hold close.
                    try:
                        close_poc = await self.client.force_check_contract(cid)
                    except Exception:
                        close_poc = {}

                    if not (close_poc.get("is_sold") or close_poc.get("is_expired")):
                        sold_for = float(sell_resp.get("sold_for", 0))
                        stake    = float(info.get("stake", 0.0))
                        close_poc = {
                            "is_sold":    1,
                            "sell_price": sold_for,
                            "profit":     sold_for - stake,
                            "payout":     sold_for,
                        }

                    final_profit = float(close_poc.get("profit", 0.0))

                    self._open_contracts.pop(cid, None)
                    self._contract_open_times.pop(cid, None)
                    self._reconciling.pop(cid, None)
                    try:
                        self.client.stop_tracking(cid)
                    except Exception:
                        pass

                    await self._apply_settlement(
                        cid, info, close_poc, close_reason="adaptive_exit")

                    try:
                        exit_engine.record_closed(cid, final_profit)
                    except Exception as exc:
                        logger.warning(f"exit_engine.record_closed({cid}) failed: {exc}")

                    return

                # action == "HOLD" (or anything unrecognized) — do nothing,
                # keep looping.

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"_monitor_exit({cid}) tick failed: {exc}")
                continue
