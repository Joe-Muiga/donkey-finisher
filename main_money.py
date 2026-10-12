"""
main_money.py - the profit-securing methodology applied to the MAIN account.

Same rules the follower accounts use (money_rules.py, unchanged logic):
  P      = balance at the last reset (midnight Kenya time = 21:00 UTC)
  floor  = max(prev floor, 0.7*P, P + 0.5*(peak - P)); only moves up until the next reset
  stake  = max(PREDICTOR_STAKE, 17.5% of (balance - floor)), rounded down to cents
  buy    = only if balance - stake >= floor; below that the account is "stopped" until the next reset

The minimum stake is the PREDICTOR_STAKE Render environment variable (nothing hard-coded).
State (P / peak / floor / reset day) is persisted - to the database when DATABASE_URL is set,
and always to a local file - so a restart or redeploy in the middle of a day keeps the floor.

Nothing here touches signal / prediction logic: it only answers "what stake, and may I buy?".
"""
import json
import logging
import os
import threading
import time
from dataclasses import replace
from typing import Optional

import money_rules as mr

logger = logging.getLogger("main.money")

STATE_KEY = "main_account_money_state"
STATE_FILE = os.environ.get("MAIN_MONEY_STATE_FILE", "main_money_state.json")


def _lock_threshold() -> float:
    for name in ("MAIN_LOCK_THRESHOLD", "FOLLOWER_LOCK_THRESHOLD"):
        raw = (os.environ.get(name) or "").strip()
        if raw:
            try:
                return float(raw)
            except ValueError:
                pass
    return 5.0


class MainMoney:
    def __init__(self):
        self.min_stake = mr.MIN_STAKE                 # = PREDICTOR_STAKE env var
        self.lock_threshold = _lock_threshold()
        self.state = mr.FState()
        self.balance: Optional[float] = None
        self.pending = 0.0                            # stakes reserved but not yet acknowledged
        self.ready = False
        self._stop_logged = False
        self._milestone_logged = False
        self._dirty = threading.Event()
        self._latest: Optional[dict] = None
        self._lock = threading.Lock()
        self._writer: Optional[threading.Thread] = None

    # ── persistence (never blocks the trading loop) ───────────────────────────
    def _state_dict(self) -> dict:
        s = self.state
        return {"p": s.p, "peak": s.peak, "floor": s.floor,
                "last_reset_date": s.last_reset_date, "milestone_hit": s.milestone_hit}

    def _load(self) -> Optional[dict]:
        db = None
        try:
            import db as _db
            db = _db if _db.enabled() else None
        except Exception:
            db = None
        if db is not None:
            # Redeploys wipe local files, so the database is the real memory. If it cannot be read the
            # bot still trades: it falls back to the local file, else starts from a fresh P/floor.
            for attempt in range(3):
                try:
                    db.ensure_started()
                    raw = db.meta_get(STATE_KEY)
                    return json.loads(raw) if raw else None
                except Exception as exc:
                    logger.warning("MAIN MONEY: database state unreadable (attempt %d/3): %s",
                                   attempt + 1, str(exc)[:100])
                    time.sleep(2.0 * (attempt + 1))
            logger.error("MAIN MONEY: database unreadable - trading continues; using local file / fresh floor")
        else:
            logger.warning("MAIN MONEY: no DATABASE_URL - the floor is kept only in a local file and is LOST on "
                       "every redeploy (the bot redeploys after each trade window)")
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return None

    def _write_all(self, d: dict) -> None:
        raw = json.dumps(d)
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as fh:
                fh.write(raw)
        except Exception:
            pass
        try:
            import db
            if db.enabled():
                db.meta_set(STATE_KEY, raw)
        except Exception as exc:
            logger.warning("MAIN MONEY: database save failed (%s)", str(exc)[:100])

    def _writer_main(self) -> None:
        while True:
            self._dirty.wait()
            time.sleep(0.5)                           # coalesce bursts of balance updates
            self._dirty.clear()
            with self._lock:
                d = self._latest
            if d is not None:
                self._write_all(d)

    def _save_soon(self) -> None:
        with self._lock:
            self._latest = self._state_dict()
        if self._writer is None:
            self._writer = threading.Thread(target=self._writer_main, name="main-money-writer", daemon=True)
            self._writer.start()
        self._dirty.set()

    # ── lifecycle ───────────────────────────────────────────────────────────────
    async def start(self, client) -> None:
        """Load the saved state, read the live balance, apply the midnight reset if one is due."""
        import asyncio
        saved = await asyncio.to_thread(self._load)
        if saved:
            self.state = mr.FState(p=saved.get("p"), peak=saved.get("peak"), floor=saved.get("floor"),
                                   last_reset_date=saved.get("last_reset_date"),
                                   milestone_hit=bool(saved.get("milestone_hit")))
        bal = await client.fetch_balance_live(timeout=8)
        self.on_balance(bal)
        # New session (every trade window): if the last one ended with the floor reached, the bot must not
        # sit idle until midnight. Re-base: P = live balance, new floor = 0.7*P. The old floor may be
        # breached (that is the point: keep trading to grow the account), but trading stays protected.
        if self.state.p is not None and mr.is_stopped(self.state, bal, self.min_stake):
            old_floor = self.state.floor
            self.state = mr.apply_reset(self.state, bal, self.lock_threshold, self.state.last_reset_date)
            logger.warning("MAIN MONEY re-based for this session: floor %s was reached; P=%.2f new floor=%s",
                           _f(old_floor), bal, _f(self.state.floor))
            self._save_soon()
        self.ready = True
        logger.info("MAIN MONEY ready: balance=%.2f P=%s floor=%s tier=%s min_stake(PREDICTOR_STAKE)=%.2f "
                    "lock_threshold=%.2f", bal, _f(self.state.p), _f(self.state.floor), self.state.tier,
                    self.min_stake, self.lock_threshold)

    def on_balance(self, bal: float) -> None:
        """Every live balance update (also called once at start). Applies the Kenya-midnight reset."""
        self.balance = bal
        today = mr.kenya_day_ts(time.time())
        if mr.needs_reset(self.state, today):
            self.state = mr.apply_reset(self.state, bal, self.lock_threshold, today)
            self._stop_logged = False
            self._milestone_logged = False
            logger.info("MAIN MONEY reset (Kenya midnight): P=%.2f tier=%s floor=%s", bal,
                        self.state.tier, _f(self.state.floor))
            self._save_soon()
            return
        new = mr.update_on_balance(self.state, bal, self.lock_threshold)
        if new != self.state:
            if new.floor != self.state.floor:
                logger.info("MAIN MONEY floor %s -> %s (balance %.2f, peak %s)", _f(self.state.floor),
                            _f(new.floor), bal, _f(new.peak))
            self.state = new
            self._save_soon()
        ms = mr.milestone(self.state)
        if ms and not self.state.milestone_hit and bal >= ms:
            self.state = replace(self.state, milestone_hit=True)
            logger.info("MAIN MONEY milestone %.2f reached (balance %.2f) - trading continues", ms, bal)
            self._save_soon()

    # ── stake / buy decisions ──────────────────────────────────────────────────
    def _effective(self) -> Optional[float]:
        if not self.ready or self.balance is None:
            return None
        return mr.effective_balance(self.balance, self.pending)

    def next_stake(self) -> Optional[float]:
        """Stake for the next buy, or None when the account must not trade right now."""
        bal = self._effective()
        if bal is None:
            return None
        stake = mr.stake_for(self.state, bal, self.min_stake)
        if stake is None or not mr.can_buy(self.state, bal, stake, self.min_stake):
            if not self._stop_logged and self.pending == 0:
                self._stop_logged = True
                logger.info("MAIN MONEY stopped: tradable %.2f < min stake %.2f (balance %.2f, floor %s). "
                            "Resumes at the next Kenya-midnight reset.",
                            mr.tradable(self.state, bal), self.min_stake, bal, _f(self.state.floor))
            return None
        self._stop_logged = False
        return stake

    def reserve(self, stake: float) -> bool:
        """Hard buy rule, checked again at the moment of sending: balance - pending - stake >= floor."""
        bal = self._effective()
        if bal is None or not mr.can_buy(self.state, bal, stake, self.min_stake):
            return False
        self.pending += stake
        return True

    def release(self, stake: float) -> None:
        self.pending = max(0.0, self.pending - stake)

    def note_buy(self, buy: dict) -> None:
        """Buy acknowledged: Deriv reports the balance after the stake was deducted."""
        try:
            if buy.get("balance_after") is not None:
                self.on_balance(float(buy["balance_after"]))
        except Exception:
            pass

    def snapshot(self) -> dict:
        s = self.state
        bal = self.balance
        return {"tier": s.tier, "p": s.p, "peak": s.peak, "floor": s.floor,
                "tradable": None if bal is None else round(mr.tradable(s, bal), 2),
                "min_stake": self.min_stake,
                "next_stake": None if bal is None else mr.stake_for(s, bal, self.min_stake),
                "stopped": bool(bal is not None and mr.is_stopped(s, bal, self.min_stake)),
                "milestone": mr.milestone(s), "milestone_hit": s.milestone_hit}


def _f(x: Optional[float]) -> str:
    return "-" if x is None else f"{x:.2f}"
