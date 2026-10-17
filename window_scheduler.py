"""
window_scheduler.py - the trading schedule: two exact-second UTC sessions EVERY hour, EVERY day.

        Session A : opens HH:06:00.000   closes HH:16:01.000
        Session B : opens HH:21:00.000   closes HH:29:01.000          (all numbers live in config.py)

Open is INCLUSIVE, close is EXCLUSIVE:   open <= t < close.   The "+1 s" is config.SESSION_CLOSE_EXTRA_SECONDS.

Everything is judged on DERIV'S SERVER CLOCK: `now()` = local clock + the measured offset (clock_sync.py, set through
`set_clock_offset`).  Every public function takes a raw wall-clock `ts` (default: the current time) and applies the
offset itself, so a caller cannot forget it.

Four moments per session (the connection lifecycle - there is NO "keep open through short gaps"):

        lead_start = open - WINDOW_PRECONNECT_SECONDS   connect, authorise, subscribe, warm up, measure the clock
        open                                            buying allowed
        close                                           buying stops (send-time gate inside DerivClient._send)
        drain_end  = close + SESSION_DRAIN_SECONDS      HARD disconnect, whatever is still open

Outside [lead_start, drain_end) of every session the bot is not connected to Deriv at all.  A bad / unloadable
schedule fails CLOSED: nothing connects, nothing is bought.

Gates (all of them evaluated on the server clock):
    buy_gate()            True only inside open <= t < close, with a measured clock, and not in a session this
                          process booted into when RESUME_TRADING_MID_SESSION_AFTER_BOOT is False.
    connection_allowed()  True only inside lead_start <= t < drain_end.
"""
import logging
import math
import threading
import time
from typing import Callable, Dict, Iterator, List, NamedTuple, Optional, Tuple

import config

logger = logging.getLogger("window")

HOUR = 3600
DAY = 86400

# ─── time source (a fake clock can be injected by the tests) ─────────────────────────────────────────
_wall: Callable[[], float] = time.time


def set_time_source(fn: Optional[Callable[[], float]]) -> None:
    """Tests only: replace the wall clock (None restores time.time)."""
    global _wall
    _wall = fn or time.time


def wall() -> float:
    return _wall()


# ─── Deriv server clock ──────────────────────────────────────────────────────────────────────────────
_clock_lock = threading.Lock()
_offset = 0.0                      # seconds to ADD to the local clock to get Deriv server time
_offset_samples = 0
_offset_error: Optional[float] = None
_offset_method = ""
_offset_set_at = 0.0               # local wall time of the last update


def set_clock_offset(offset: float, samples: int = 1, error: Optional[float] = None, method: str = "") -> None:
    global _offset, _offset_samples, _offset_error, _offset_method, _offset_set_at
    with _clock_lock:
        _offset = float(offset)
        _offset_samples = int(samples)
        _offset_error = None if error is None else float(error)
        _offset_method = method or ""
        _offset_set_at = _wall()


def reset_clock(keep_offset: bool = True) -> None:
    """A new connection re-measures the Deriv clock from scratch: the buy gate stays CLOSED until it has.
    keep_offset=True keeps the previous offset as the best prior for the connect/disconnect times (those are not
    millisecond-critical); keep_offset=False forgets it too (tests)."""
    global _offset, _offset_samples, _offset_error, _offset_method, _offset_set_at
    with _clock_lock:
        if not keep_offset:
            _offset = 0.0
        _offset_samples, _offset_error, _offset_method, _offset_set_at = 0, None, "", 0.0


def clock_offset() -> float:
    return _offset


def clock_samples() -> int:
    return _offset_samples


def clock_error() -> Optional[float]:
    return _offset_error


def clock_measured() -> bool:
    return _offset_samples > 0


def clock_age() -> Optional[float]:
    return (_wall() - _offset_set_at) if _offset_set_at else None


def clock_status() -> str:
    if not clock_measured():
        return "Deriv clock NOT measured"
    err = "" if _offset_error is None else f", error +-{_offset_error:.3f}s"
    return f"Deriv clock offset {_offset:+.3f}s{err} ({_offset_samples} samples{', ' + _offset_method if _offset_method else ''})"


def now() -> float:
    """Best estimate of Deriv server time (epoch seconds, float)."""
    return _wall() + _offset


def server_time(ts: Optional[float] = None) -> float:
    return (_wall() if ts is None else float(ts)) + _offset


# ─── configuration (read at call time; constants only - no environment variable can change the schedule) ───
def preconnect_seconds() -> float:
    return max(0.0, float(config.WINDOW_PRECONNECT_SECONDS))


def drain_seconds() -> float:
    return max(0.0, float(config.SESSION_DRAIN_SECONDS))


def close_extra_seconds() -> float:
    return max(0.0, float(config.SESSION_CLOSE_EXTRA_SECONDS))


# ─── schedule model ─────────────────────────────────────────────────────────────────────────────────────
class Session(NamedTuple):
    slot: str            # "A" / "B"
    hour: int            # epoch hour index (hour start = hour * 3600)
    open: float          # inclusive
    close: float         # exclusive (nominal end + the +1 s)
    lead_start: float    # connect starts here
    drain_end: float     # hard disconnect here

    @property
    def label(self) -> str:
        return f"{self.slot}@{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self.open))}Z"

    @property
    def length(self) -> float:
        return self.close - self.open


class Schedule:
    """Hourly pattern of sessions.  All times are Deriv-server epoch seconds."""

    def __init__(self, slots: Tuple[Tuple[str, float, float], ...], close_extra: float, lead: float, drain: float):
        if not slots:
            raise ValueError("no sessions configured")
        if min(close_extra, lead, drain) < 0:
            raise ValueError("close extra / connect lead / drain must not be negative")
        norm = sorted(((str(n), float(o), float(e)) for n, o, e in slots), key=lambda x: x[1])
        for n, o, e in norm:
            if not (0 <= o < e):
                raise ValueError(f"session {n}: need 0 <= open < end (got {o}, {e})")
            if e + close_extra > HOUR:
                raise ValueError(f"session {n}: close {e + close_extra}s is past the end of the hour")
        names = [n for n, _, _ in norm]
        if len(set(names)) != len(names):
            raise ValueError("duplicate session names")
        self.slots = tuple(norm)
        self.close_extra, self.lead, self.drain = float(close_extra), float(lead), float(drain)
        # every connection window [lead_start, drain_end) must be separate from the next one (each session is its own
        # connect/disconnect cycle), including the wrap-around into the next hour
        for i, (n, o, e) in enumerate(norm):
            nxt = norm[(i + 1) % len(norm)]
            gap_open = nxt[1] + (HOUR if i + 1 >= len(norm) else 0)
            if (e + close_extra + drain) > (gap_open - lead):
                raise ValueError(f"sessions {n} and {nxt[0]}: drain end {e + close_extra + drain:.0f}s is not before the "
                                 f"next connect lead start {gap_open - lead:.0f}s - connection windows would merge")

    # -- construction of Session objects ------------------------------------------------------------------
    def _session(self, hour: int, idx: int) -> Session:
        n, o, e = self.slots[idx]
        base = hour * HOUR
        return Session(n, hour, base + o, base + e + self.close_extra, base + o - self.lead,
                       base + e + self.close_extra + self.drain)

    def sessions_in_hour(self, hour: int) -> List[Session]:
        return [self._session(hour, i) for i in range(len(self.slots))]

    def _around(self, t: float, back: int = 1, fwd: int = 2) -> Iterator[Session]:
        h = int(math.floor(t / HOUR))
        for hh in range(h - back, h + fwd + 1):
            for i in range(len(self.slots)):
                yield self._session(hh, i)

    # -- queries on SERVER time ---------------------------------------------------------------------------
    def session_at(self, t: float) -> Optional[Session]:
        """The session with open <= t < close, else None."""
        for s in self._around(t):
            if s.open <= t < s.close:
                return s
        return None

    def connection_session(self, t: float) -> Optional[Session]:
        """The session whose connection window lead_start <= t < drain_end contains t, else None."""
        for s in self._around(t):
            if s.lead_start <= t < s.drain_end:
                return s
        return None

    def next_session(self, t: float) -> Session:
        """First session that opens strictly after t."""
        best = None
        for s in self._around(t, 1, 2):
            if s.open > t and (best is None or s.open < best.open):
                best = s
        assert best is not None
        return best

    def next_session_after_connect(self, t: float) -> Session:
        """First session whose lead_start is strictly after t (the next connection cycle to start)."""
        best = None
        for s in self._around(t, 1, 2):
            if s.lead_start > t and (best is None or s.lead_start < best.lead_start):
                best = s
        assert best is not None
        return best

    def previous_session(self, t: float) -> Optional[Session]:
        """Latest session whose drain_end <= t."""
        best = None
        for s in self._around(t, 2, 1):
            if s.drain_end <= t and (best is None or s.drain_end > best.drain_end):
                best = s
        return best

    def sessions_between(self, t0: float, t1: float) -> List[Session]:
        """Sessions whose open falls in [t0, t1)."""
        out = [s for s in self._around(t0, 1, int(math.ceil(max(0.0, t1 - t0) / HOUR)) + 1) if t0 <= s.open < t1]
        return sorted(out, key=lambda s: s.open)

    def phase(self, t: float) -> Tuple[str, Optional[Session], float]:
        """(phase, session, seconds to the next phase change).  phase: IDLE / CONNECT / LIVE / DRAIN."""
        for s in self._around(t):
            if s.lead_start <= t < s.open:
                return "CONNECT", s, s.open - t
            if s.open <= t < s.close:
                return "LIVE", s, s.close - t
            if s.close <= t < s.drain_end:
                return "DRAIN", s, s.drain_end - t
        nxt = self.next_session_after_connect(t)
        return "IDLE", nxt, nxt.lead_start - t


# ─── the loaded schedule (fail closed) ──────────────────────────────────────────────────────────────────
_schedule: Optional[Schedule] = None
_load_error: Optional[str] = None
_sched_lock = threading.Lock()


def build_schedule() -> Schedule:
    slots = tuple((n, o, e) for n, o, e in config.SESSION_SLOTS)
    return Schedule(slots, close_extra_seconds(), preconnect_seconds(), drain_seconds())


def schedule() -> Optional[Schedule]:
    """The loaded schedule, or None if the configuration is invalid (then nothing connects and nothing is bought)."""
    global _schedule, _load_error
    if _schedule is None and _load_error is None:
        with _sched_lock:
            if _schedule is None and _load_error is None:
                try:
                    _schedule = build_schedule()
                    logger.warning("SCHEDULE LOADED: %s | connect lead %.0fs | drain %.0fs | close extra %.0fs",
                                   " + ".join(f"Session {n} {int(o // 60):02d}:{int(o % 60):02d}-{int((e + _schedule.close_extra) // 60):02d}:"
                                              f"{int((e + _schedule.close_extra) % 60):02d}" for n, o, e in _schedule.slots),
                                   _schedule.lead, _schedule.drain, _schedule.close_extra)
                except Exception as exc:
                    _load_error = f"{type(exc).__name__}: {exc}"
                    logger.critical("SCHEDULE NOT LOADED (%s) - the bot will NOT connect or trade until config.py is fixed", _load_error)
    return _schedule


def reload_schedule() -> Optional[Schedule]:
    """Tests: rebuild from the (possibly patched) config."""
    global _schedule, _load_error
    with _sched_lock:
        _schedule, _load_error = None, None
    return schedule()


def load_error() -> Optional[str]:
    schedule()
    return _load_error


# ─── boot hold: a process (or engine) that starts INSIDE a session does not trade it ────────────────────
_held_open: Optional[float] = None       # `open` of the session this process must not trade
_hold_lock = threading.Lock()


def note_engine_start(ts: Optional[float] = None) -> Optional[Session]:
    """Call whenever a trading engine is (re)built.  If a session is already open and
    RESUME_TRADING_MID_SESSION_AFTER_BOOT is False, that session is not traded: buying starts at the next open.
    Returns the held session (or None)."""
    global _held_open
    s = schedule()
    held = None
    if s is not None and not config.RESUME_TRADING_MID_SESSION_AFTER_BOOT:
        held = s.session_at(server_time(ts))
    with _hold_lock:
        _held_open = held.open if held is not None else None
    if held is not None:
        logger.warning("BOOT MID-SESSION: started inside session %s (closes %s) - NOT trading it; "
                       "buying resumes at the next open (RESUME_TRADING_MID_SESSION_AFTER_BOOT=False)",
                       held.label, time.strftime("%H:%M:%S", time.gmtime(held.close)))
    return held


def held_session_open() -> Optional[float]:
    return _held_open


def clear_boot_hold() -> None:
    global _held_open
    with _hold_lock:
        _held_open = None


# ─── public API ─────────────────────────────────────────────────────────────────────────────────────────
def in_window(ts: Optional[float] = None) -> bool:
    """Pure schedule check: open <= server time < close.  (The buy gate adds the clock + boot-hold conditions.)"""
    s = schedule()
    return bool(s and s.session_at(server_time(ts)) is not None)


def current_session(ts: Optional[float] = None) -> Optional[Session]:
    s = schedule()
    return s.session_at(server_time(ts)) if s else None


def connection_session(ts: Optional[float] = None) -> Optional[Session]:
    s = schedule()
    return s.connection_session(server_time(ts)) if s else None


def next_session(ts: Optional[float] = None) -> Optional[Session]:
    s = schedule()
    return s.next_session(server_time(ts)) if s else None


def should_be_connected(ts: Optional[float] = None) -> bool:
    """True from the connect lead before an open until the drain deadline after the close - and nowhere else."""
    s = schedule()
    return bool(s and s.connection_session(server_time(ts)) is not None)


def connection_allowed(ts: Optional[float] = None) -> bool:
    """Gate used by every code path that opens a Deriv connection or sends anything to Deriv."""
    return should_be_connected(ts)


def connect_allowed(ts: Optional[float] = None) -> bool:
    """May a NEW Deriv connection (OTP + websocket) be opened now?  Lead start <= t < close: never during the drain,
    where the only job left is letting open contracts settle on the connection that already exists."""
    s = schedule()
    if s is None:
        return False
    t = server_time(ts)
    sess = s.connection_session(t)
    return sess is not None and t < sess.close


def buy_gate(ts: Optional[float] = None) -> Tuple[bool, str]:
    """(allowed, reason).  Evaluated at the moment a buy is about to be written to the socket."""
    s = schedule()
    if s is None:
        return False, f"schedule not loaded ({_load_error})"
    if not clock_measured():
        return False, "Deriv server clock not measured yet"
    t = server_time(ts)
    sess = s.session_at(t)
    if sess is None:
        return False, "outside session"
    if _held_open is not None and sess.open == _held_open:
        return False, "process booted inside this session (RESUME_TRADING_MID_SESSION_AFTER_BOOT=False)"
    return True, ""


def seconds_until_open(ts: Optional[float] = None) -> float:
    """0 inside a session, else seconds until the next open (server time)."""
    s = schedule()
    if s is None:
        return 3600.0
    t = server_time(ts)
    if s.session_at(t) is not None:
        return 0.0
    return s.next_session(t).open - t


def session_end(ts: Optional[float] = None) -> Optional[float]:
    """Server-clock close of the session containing ts, else None."""
    s = schedule()
    if s is None:
        return None
    sess = s.session_at(server_time(ts))
    return sess.close if sess else None


def next_connect_in(ts: Optional[float] = None) -> float:
    """Seconds until the next connect lead starts (0 when a connection window is already running)."""
    s = schedule()
    if s is None:
        return 60.0
    t = server_time(ts)
    if s.connection_session(t) is not None:
        return 0.0
    return max(0.0, s.next_session_after_connect(t).lead_start - t)


def phase(ts: Optional[float] = None) -> Dict[str, object]:
    """Dashboard / logging view of where the schedule is right now."""
    s = schedule()
    if s is None:
        return {"phase": "NO SCHEDULE", "slot": None, "in": 0.0, "session": None}
    ph, sess, rem = s.phase(server_time(ts))
    return {"phase": ph, "slot": sess.slot if sess else None, "in": rem, "session": sess}


def describe_sessions(hour_start: float = 0.0) -> List[str]:
    s = schedule()
    if s is None:
        return []
    out = []
    for x in s.sessions_in_hour(int(hour_start // HOUR)):
        f = lambda v: time.strftime("%M:%S", time.gmtime(v))     # noqa: E731
        out.append(f"Session {x.slot}: connect HH:{f(x.lead_start)}  open HH:{f(x.open)}  close HH:{f(x.close)}  "
                   f"hard disconnect HH:{f(x.drain_end)}")
    return out


def dashboard_view(ts: Optional[float] = None, engine_phase: str = "", redeploy: Optional[dict] = None,
                   engine_alive: bool = True) -> Dict[str, object]:
    """What the dashboard shows: label (IDLE / CONNECTING / WARMING UP / SESSION A LIVE / SESSION B LIVE / DRAINING /
    REDEPLOYING), a countdown line, and the next events (server time).  `engine_phase` is connecting / warming / ready
    (published by the engine); `redeploy` = {"live_by": epoch, "fired_at": epoch} while a fired deploy is replacing us."""
    s = schedule()
    if s is None:
        return {"label": "NO SCHEDULE - NOT TRADING", "detail": str(_load_error or ""), "countdown": None,
                "next_open_in": None, "next_slot": None, "next_connect_in": None}
    t = server_time(ts)
    ph, sess, rem = s.phase(t)
    nxt = s.next_session(t)
    out: Dict[str, object] = {"next_open_in": nxt.open - t, "next_slot": nxt.slot,
                              "next_connect_in": max(0.0, s.next_session_after_connect(t).lead_start - t),
                              "countdown": rem, "phase": ph}
    if ph == "IDLE":
        label = "IDLE"
        detail = f"disconnected - Session {sess.slot} connects in {rem:.0f}s, opens in {nxt.open - t:.0f}s"
        if redeploy and redeploy.get("fired_at") and t < float(redeploy.get("live_by", 0)):
            label = "REDEPLOYING"
            detail = f"new deploy goes live before the connect lead ({float(redeploy['live_by']) - t:.0f}s left); next open in {nxt.open - t:.0f}s"
    elif ph == "CONNECT":
        if not engine_alive or engine_phase in ("", "connecting"):
            label, detail = "CONNECTING", f"Session {sess.slot} opens in {rem:.0f}s"
        else:
            label, detail = "WARMING UP", (f"Session {sess.slot} opens in {rem:.0f}s" +
                                           (" - ready" if engine_phase == "ready" else ""))
    elif ph == "LIVE":
        if engine_phase == "ready":
            label, detail = f"SESSION {sess.slot} LIVE", f"closes in {rem:.0f}s"
        else:
            label, detail = "WARMING UP", f"Session {sess.slot} is open but the engine is not ready yet; closes in {rem:.0f}s"
    else:
        label, detail = "DRAINING", f"Session {sess.slot} closed - hard disconnect in {rem:.0f}s"
    out.update({"label": label, "detail": detail, "slot": sess.slot if sess else None})
    return out


def session_minutes() -> set:
    """Minutes-of-hour covered by the sessions' NOMINAL times (Session A 6..15, Session B 21..28 with the defaults).
    Used only to label statistics (follower verdict page)."""
    out = set()
    for _n, o, e in config.SESSION_SLOTS:
        out.update(range(int(o // 60), int(math.ceil(e / 60.0))))
    return out
