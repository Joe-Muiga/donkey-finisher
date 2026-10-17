"""
window_scheduler.py - the session clock (single source of truth for time).

  * Two trade sessions every hour, every hour of the day (config.TRADE_SESSIONS, seconds of the hour, UTC,
    judged on DERIV'S SERVER CLOCK):   A  HH:06:00.000 -> HH:16:01.000     B  HH:21:00.000 -> HH:29:01.000
    A session is open for  open <= t < close : the open instant is inclusive, the close instant exclusive.
  * now() is the server clock: local time.time() plus a measured offset (set_clock_offset(), fed by the engine's
    clock-sync burst). Every gate decision uses now(), never time.time().
  * The bot is connected to Deriv ONLY inside a session and from _preconnect() seconds before an open (warm-up).
    Each session is its own session: the gap between A and B does NOT keep the socket open.
  * The engine drains already-bought contracts for a short bounded time after a close and disconnects.
  * ONE redeploy per hour (redeploy_clear_cache(), Render with the build cache cleared) is fired in the long
    idle gap after Session B so a fresh instance is up just before Session A:
        fire time = next A open - _preconnect() - config.REDEPLOY_LEAD_SECONDS      (server clock)
    There is no redeploy between HH:06:00 and HH:29:11, none between A and B, none on a timer, none while a
    session is open. A freshly booted instance never redeploys before it has run a session (see main.py).

Needs RENDER_API_KEY (RENDER_SERVICE_ID is injected by Render). A deploy hook URL cannot clear the cache, so it
is not used here. SESSION_WINDOW_ENABLED=0 switches the sessions (and the redeploy) off: connected all hours.
"""
import logging
import math
import os
import time
from typing import List, Optional, Tuple

import requests

import config
import render_api

logger = logging.getLogger("window")

# An offset (Deriv server time minus local time) larger than this is rejected as garbage and the previous value kept.
CLOCK_OFFSET_MAX_SECONDS = 120.0
# Deriv's {"time":1} reports whole seconds. The sample says  S in [st + SHIFT, st + SHIFT + 1)  for the server clock S.
# 0.0 = the reported second is the floor of the server clock. If Deriv were to round to the nearest second this
# would be -0.5. Unverified assumption: named here so it can be changed in one place.
SERVER_TIME_SHIFT = 0.0
# The redeploy is never fired earlier than this long after Session B's close (so never inside HH:06:00-HH:29:11).
REDEPLOY_AFTER_B_MARGIN_SECONDS = 10.0

_offset = 0.0                 # server minus local, seconds
_clock = {"measured": False, "err": None, "samples": 0, "at": 0.0}


def _preconnect() -> float:
    try:
        return max(0.0, float((os.environ.get("WINDOW_PRECONNECT_SECONDS") or "40").strip()))
    except ValueError:
        return 40.0


# ── server clock ─────────────────────────────────────────────────────────────
def now() -> float:
    """Server-clock time: local time plus the measured offset (0.0 until one has been measured)."""
    return time.time() + _offset


def set_clock_offset(offset: float, err: Optional[float] = None, samples: int = 0) -> bool:
    """Store the measured offset (server - local). False (and unchanged) if it is not a number or beyond the limit."""
    global _offset
    try:
        off = float(offset)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(off) or abs(off) > CLOCK_OFFSET_MAX_SECONDS:
        logger.error("CLOCK OFFSET %+.3fs rejected (limit +-%.0fs): keeping %+.3fs", off if math.isfinite(off) else float("nan"),
                     CLOCK_OFFSET_MAX_SECONDS, _offset)
        return False
    _offset = off
    _clock.update(measured=True, err=err, samples=int(samples), at=time.time())
    return True


def clock_offset() -> float:
    return _offset


def clock_measured() -> bool:
    return bool(_clock["measured"])


def clock_info() -> dict:
    return {"offset": _offset, **_clock}


def estimate_offset(samples: List[Tuple[float, float, float]]) -> Optional[Tuple[float, float, int]]:
    """samples: [(local_send, local_recv, server_whole_seconds)]. Returns (offset, error, samples_used) or None.

    The server reports whole seconds, so one sample only says  st+SHIFT <= S < st+SHIFT+1  for the server clock S at the
    moment it answered, which was somewhere in [send, recv]. With mid = (send+recv)/2 and rtt = recv-send that is an
    interval for offset = S - local:  [st+SHIFT-mid-rtt/2, st+SHIFT+1-mid+rtt/2]. Samples taken over several seconds
    straddle several second-flips, so the intersection (best overlap, Marzullo-style) pins the offset to well under
    a second; only the lowest-RTT samples are used (NTP-style)."""
    good = [(a, b, float(st)) for a, b, st in samples if b >= a]
    if not good:
        return None
    rtts = sorted(b - a for a, b, _ in good)
    cut = max(2.0 * rtts[0], rtts[0] + 0.05)
    keep = [s for s in good if (s[1] - s[0]) <= cut]
    if len(keep) < 3:
        keep = sorted(good, key=lambda s: s[1] - s[0])[:3]
    ivs = []
    for a, b, st in keep:
        mid, half = (a + b) / 2.0, (b - a) / 2.0
        ivs.append((st + SERVER_TIME_SHIFT - mid - half, st + SERVER_TIME_SHIFT + 1.0 - mid + half))
    events = sorted([(lo, 0) for lo, _ in ivs] + [(hi, 1) for _, hi in ivs])      # 0 = start sorts before 1 = end at ties
    best, cur, region = 0, 0, None
    for i, (x, kind) in enumerate(events):
        if kind == 0:
            cur += 1
            if cur > best:
                best = cur
                region = (x, events[i + 1][0] if i + 1 < len(events) else x)
        else:
            cur -= 1
    if region is None or best < max(2, len(ivs) // 2 + 1):
        # inconsistent samples: the median centre of the lowest-RTT samples, error = half a second plus the rtt
        centres = sorted(0.5 * (lo + hi) for lo, hi in ivs)
        return centres[len(centres) // 2], 0.5 + cut / 2.0, len(keep)
    lo, hi = region
    return 0.5 * (lo + hi), max(0.0, 0.5 * (hi - lo)), len(keep)


# ── the two sessions ─────────────────────────────────────────────────────────
def _sessions() -> Tuple[Tuple[str, float, float], ...]:
    return tuple((str(n), float(o), float(c)) for n, o, c in config.TRADE_SESSIONS)


def _hour_base(ts: float) -> float:
    return ts - (ts % 3600.0)


def session_at(ts: Optional[float] = None) -> Optional[Tuple[str, float, float]]:
    """(name, open_ts, close_ts) of the session open at `ts` (open <= ts < close), else None."""
    t = now() if ts is None else ts
    base, into = _hour_base(t), t % 3600.0
    for name, o, c in _sessions():
        if o <= into < c:
            return name, base + o, base + c
    return None


def next_session(ts: Optional[float] = None) -> Tuple[str, float, float]:
    """(name, open_ts, close_ts) of the first session whose open instant is strictly after `ts`."""
    t = now() if ts is None else ts
    base = _hour_base(t)
    for h in (0.0, 3600.0):
        for name, o, c in _sessions():
            if base + h + o > t:
                return name, base + h + o, base + h + c
    raise RuntimeError("no session found")      # unreachable: a full hour always contains an open


def session_end(ts: Optional[float] = None) -> Optional[float]:
    """Close instant of the session open at `ts`, else None."""
    s = session_at(ts)
    return s[2] if s else None


def next_session_start(ts: Optional[float] = None) -> float:
    return next_session(ts)[1]


def in_window(ts: Optional[float] = None) -> bool:
    """True while a session is open: open <= t < close, to the float second. SESSION_WINDOW_ENABLED=0: always True."""
    if not config.TRADE_WINDOW_ENABLED:
        return True
    return session_at(ts) is not None


def seconds_until_open(ts: Optional[float] = None) -> float:
    """Seconds until the next session opens (0 while inside a session or with the sessions switched off)."""
    if not config.TRADE_WINDOW_ENABLED:
        return 0.0
    t = now() if ts is None else ts
    if session_at(t) is not None:
        return 0.0
    return next_session(t)[1] - t


def should_be_connected(ts: Optional[float] = None) -> bool:
    """True inside a session or within the pre-connect lead before the next open. False in every gap otherwise."""
    if not config.TRADE_WINDOW_ENABLED:
        return True
    t = now() if ts is None else ts
    if session_at(t) is not None:
        return True
    return next_session(t)[1] - t <= _preconnect()


def connect_at(ts: Optional[float] = None, skip_current: bool = False) -> float:
    """Absolute time at which the bot should next connect: `ts` itself if it should be connected already.
    skip_current=True ignores the session open at `ts` and returns the next session's connect-lead start."""
    t = now() if ts is None else ts
    if not skip_current and should_be_connected(t):
        return t
    base = t
    cur = session_at(t)
    if cur is not None:
        base = cur[2]
    return max(next_session(base)[1] - _preconnect(), base)


def next_connect_in(ts: Optional[float] = None) -> float:
    """Seconds until should_be_connected() turns True (0 when it already is)."""
    t = now() if ts is None else ts
    return max(0.0, connect_at(t) - t)


# ── the one redeploy per hour ────────────────────────────────────────────────
def _next_a_open(t: float) -> float:
    base = _hour_base(t)
    a_open = next(o for n, o, _ in _sessions() if n == "A")
    return base + a_open if base + a_open > t else base + 3600.0 + a_open


def redeploy_allowed(ts: Optional[float] = None) -> bool:
    """False inside HH:A_open .. HH:B_close+margin (no redeploy between/during the sessions), inside any session,
    inside a pre-connect lead, and whenever the sessions are switched off."""
    if not config.TRADE_WINDOW_ENABLED:
        return False
    t = now() if ts is None else ts
    sess = _sessions()
    lo = min(o for _, o, _ in sess)
    hi = max(c for _, _, c in sess) + REDEPLOY_AFTER_B_MARGIN_SECONDS
    if lo <= (t % 3600.0) < hi:
        return False
    return not should_be_connected(t)


def redeploy_fire_ts(ts: Optional[float] = None) -> Optional[float]:
    """Absolute time at which the redeploy for the next Session A is fired: next A open - preconnect lead -
    REDEPLOY_LEAD_SECONDS, never earlier than B's close + margin. None if the sessions are off or the moment has
    passed (we are already in that session's connect lead or inside a session)."""
    if not config.TRADE_WINDOW_ENABLED:
        return None
    t = now() if ts is None else ts
    a_open = _next_a_open(t)
    connect_start = a_open - _preconnect()
    if t >= connect_start:
        return None
    sess = _sessions()
    a_o = next(o for n, o, _ in sess if n == "A")
    b_close = max(c for _, _, c in sess)
    earliest = (a_open - a_o) - 3600.0 + b_close + REDEPLOY_AFTER_B_MARGIN_SECONDS
    fire = max(a_open - _preconnect() - float(getattr(config, "REDEPLOY_LEAD_SECONDS", 300)), earliest)
    return fire if fire < connect_start else None


def redeploy_fire_in(ts: Optional[float] = None) -> Optional[float]:
    """Seconds until the once-per-hour redeploy fire time (0 if it is already due), None if there is none."""
    t = now() if ts is None else ts
    fire = redeploy_fire_ts(t)
    return None if fire is None else max(0.0, fire - t)


def redeploy_clear_cache() -> bool:
    """Blocking. Ask Render for a redeploy with the build cache cleared. True if accepted. Refuses outside the
    allowed idle gap (see redeploy_allowed)."""
    if not redeploy_allowed():
        logger.critical("REDEPLOY REFUSED: not allowed now (sessions off, inside a connect lead, or between Session A's "
                        "open and Session B's close + %.0fs)", REDEPLOY_AFTER_B_MARGIN_SECONDS)
        return False
    if not render_api.api_configured():
        logger.critical("REDEPLOY SKIPPED: RENDER_API_KEY / service id missing - cannot redeploy with a "
                        "cleared build cache. The bot will simply reconnect at the next session.")
        return False
    url = f"{render_api.API_BASE}/services/{render_api.service_id()}/deploys"
    headers = {"Authorization": f"Bearer {render_api.api_key()}", "Accept": "application/json",
               "Content-Type": "application/json"}
    try:
        r = requests.post(url, headers=headers, json={"clearCache": "clear"}, timeout=20)
        if r.status_code in (200, 201, 202):
            logger.warning("REDEPLOY accepted by Render (clearCache=clear): HTTP %s", r.status_code)
            return True
        logger.error("REDEPLOY refused by Render: HTTP %s %s", r.status_code, r.text[:200])
    except Exception as exc:
        logger.error("REDEPLOY request failed: %s", exc)
    return False
