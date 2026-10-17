"""
main.py – entry point (Render web service).

  • Main thread  : Flask (dashboard + /health) – always running.
  • bot-engine   : thread that runs the Donkey strategy in TWO SESSIONS EVERY HOUR (config.TRADE_SESSIONS, UTC,
                   judged on Deriv's server clock):  A HH:06:00.000-HH:16:01.000   B HH:21:00.000-HH:29:01.000.
                   Outside a session (and outside the short pre-connect lead before an open) the bot is NOT
                   connected to Deriv at all; every session gets a fresh engine in the same process.
  • keep-alive   : self-ping of /health.
  • redeploy     : exactly ONE per hour, fired in the idle gap after Session B so a fresh instance is up just before
                   Session A (window_scheduler.redeploy_fire_ts: A open - pre-connect lead - REDEPLOY_LEAD_SECONDS).
                   Never between A and B, never on a timer, never while a session is open, and never by an instance
                   that has not run a session. The old rolling auto-redeploy cycle is gone.
  • SESSION_WINDOW_ENABLED=0 : sessions off - trade all hours, no redeploy.

Env vars: DERIV_API_TOKEN (required), RENDER_API_KEY (the hourly redeploy + stats persistence),
optional DERIV_APP_ID, DERIV_ACCOUNT_MODE ("demo" default / "real" picks which
account the token's OTP is minted for), DASHBOARD_PIN, PREDICTOR_SYMBOLS, ...
(see CHANGES.md).

Follower accounts (optional): DATABASE_URL, ADMIN_PASSWORD, ENCRYPTION_KEY - see render.yaml.
"""
import asyncio
import logging
import os
import sys
import threading
import time

import config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("main")

if not config.DERIV_API_TOKEN:
    logger.critical("DERIV_API_TOKEN is not set! Add it in the Render service environment.")


def _engine_class():
    """The ONLY engine is DonkeyEngine. Demo vs real is decided solely by DERIV_ACCOUNT_MODE (no lock, no refusal)."""
    from donkey_engine import DonkeyEngine
    return DonkeyEngine


def _utc(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.gmtime(ts))


def _countdown(sec: float) -> str:
    m, s = divmod(int(max(0.0, sec)), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {s:02d}s" if h else f"{m}m {s:02d}s"


def _idle_until(ws, keep_alive, target_ts: float, label) -> None:
    """Sleep (disconnected) until the SERVER clock reaches target_ts. Computed sleeps: it wakes ON the target second
    (the last sleep is exactly the remainder), never up to 15 s late. label(rem) -> dashboard status text."""
    while True:
        rem = target_ts - ws.now()
        if rem <= 0:
            return
        try:
            keep_alive.update_status(session=label(rem))
        except Exception:
            pass
        time.sleep(rem if rem <= 1.0 else min(rem - 0.5, 5.0))


def _run_bot():
    """Session loop. Idle (disconnected) -> connect lead -> one engine per session -> idle ... Once per hour, after
    Session B, one redeploy is fired in the idle gap. If an engine ever exits or crashes inside its connect lead it is
    started again; inside an already-open session it is NOT (config.TRADE_ALLOW_MID_SESSION_START=False)."""
    import target_guard
    Engine = _engine_class()
    import window_scheduler as ws
    import keep_alive

    if not config.TRADE_WINDOW_ENABLED:                  # SESSION_WINDOW_ENABLED=0: trade all hours, never redeploy
        while True:
            eng = Engine()
            try:
                asyncio.run(eng.run())
            except Exception as exc:
                logger.error(f"Bot crashed: {exc}", exc_info=True)
            if target_guard.is_reached():
                logger.warning("Balance target reached - bot stays stopped (no restart)")
                return
            logger.warning("Bot engine returned (sessions off); restarting in 5 s")
            time.sleep(5)

    redeploy_due = False        # a Session B has just ended in THIS process (it really ran a session)
    fired_for = set()           # Session A open instants a redeploy was already requested for: never fires twice
    retry = 0
    while True:
        now = ws.now()
        if ws.in_window(now) and not config.TRADE_ALLOW_MID_SESSION_START:
            # started / restarted while a session is already open: sit it out, trade from the next exact open
            cur = ws.session_at(now)
            logger.warning("Session %s is already open (boot or restart inside a session): NOT trading it "
                           "(TRADE_ALLOW_MID_SESSION_START=False); waiting for the next session's open", cur[0])
            nxt = ws.next_session(cur[2])
            _idle_until(ws, keep_alive, ws.connect_at(now, skip_current=True),
                        lambda rem, n=nxt: f"IDLE - disconnected, sitting out the open session; next session {n[0]} "
                                           f"opens {_utc(n[1])} UTC, connecting in {_countdown(rem)}")
            continue
        if not ws.should_be_connected(now):              # outside the sessions: NOT connected to Deriv
            nxt = ws.next_session(now)
            connect_ts = nxt[1] - ws._preconnect()
            if redeploy_due and nxt[0] == "A" and nxt[1] not in fired_for:
                fire_ts = ws.redeploy_fire_ts(now)
                fired_for.add(nxt[1])                    # one shot per Session A, whatever happens
                redeploy_due = False
                if fire_ts is not None:
                    _idle_until(ws, keep_alive, fire_ts,
                                lambda rem, n=nxt, f=fire_ts: f"IDLE - disconnected; hourly redeploy at {_utc(f)} UTC "
                                                              f"(in {_countdown(rem)}), session {n[0]} opens {_utc(n[1])} UTC")
                    keep_alive.update_status(session="IDLE - hourly redeploy requested (build cache cleared)")
                    if ws.redeploy_clear_cache():
                        logger.warning("Hourly redeploy requested for Session A at %s UTC: staying idle (not connecting) "
                                       "until the connect lead; Render replaces this instance", _utc(nxt[1]))
                    else:
                        logger.error("Hourly redeploy refused / not possible: keeping this instance, it reconnects "
                                     "at the next connect lead")
                    continue
                logger.warning("Hourly redeploy moment already passed for Session A at %s UTC: not redeploying", _utc(nxt[1]))
            _idle_until(ws, keep_alive, connect_ts,
                        lambda rem, n=nxt: f"IDLE - disconnected, session {n[0]} opens {_utc(n[1])} UTC, "
                                           f"connecting in {_countdown(rem)}")
            continue
        sess = ws.session_at(now) or ws.next_session(now)
        keep_alive.update_status(session=f"CONNECTING - session {sess[0]} opens {_utc(sess[1])} UTC")
        redeploy_due = False
        eng = Engine()
        try:
            asyncio.run(eng.run())
        except Exception as exc:
            logger.error(f"Bot crashed: {exc}", exc_info=True)
        if target_guard.is_reached():
            logger.warning("Balance target reached - bot stays stopped (no restart)")
            return
        try:                                             # the engine's own shutdown must have finished: nothing stays open
            if eng.client.is_connected:
                logger.error("Deriv connection still open after the engine returned")
        except Exception:
            pass
        if getattr(eng, "_session_over", False):
            retry = 0
            if getattr(eng, "_window_seen", False):
                logger.warning("Session %s finished: disconnected, idling until the next connect lead", sess[0])
                redeploy_due = (sess[0] == "B")          # only an instance that really ran Session B redeploys
            time.sleep(0.5)
            continue
        retry = min(retry + 1, 3)
        logger.warning("Bot engine returned before session %s ended; checking the schedule again in %d s", sess[0], 5 * retry)
        time.sleep(5 * retry)


if __name__ == "__main__":
    from keep_alive import app, start_keep_alive

    from donkey_engine import account_mode_line
    logger.warning(account_mode_line())
    logger.info("Starting Donkey bot … four pairs: crowd Over 2 -> Under 3, Over 3 -> Under 4, "
                "Under 6 -> Over 5, Under 7 -> Over 6; every completed setup is traded, no guards, no suspensions")
    logger.info(f"  Deriv App ID : {config.DERIV_APP_ID}")
    logger.info(f"  API token    : {'SET ✓' if config.DERIV_API_TOKEN else 'MISSING ✗'}")
    logger.info(f"  Symbols      : {config.PREDICTOR_SYMBOLS}  "
                f"min stake (PREDICTOR_STAKE)={config.PREDICTOR_STAKE}")
    logger.info(f"  Account mode : {config.DERIV_ACCOUNT_MODE}")
    logger.info("  Dry run      : %s" % ("ON (DONKEY_DRY_RUN=1) - buys are only logged as 'DONKEY WOULD BUY'" if config.DONKEY_DRY_RUN else "off"))
    import window_scheduler as _ws

    def _hms(sec):
        return "%02d:%02d" % (int(sec) // 60, int(sec) % 60)
    if config.TRADE_WINDOW_ENABLED:
        logger.info("  Trade sessions: " + " | ".join(f"{n} HH:{_hms(o)}-HH:{_hms(c)}" for n, o, c in config.TRADE_SESSIONS)
                    + " every hour (UTC, Deriv server clock; open <= t < close); disconnected from Deriv in between "
                      f"(connect lead {_ws._preconnect():.0f}s before each open)")
        logger.info("  Redeploy     : ONE per hour, fired in the idle gap after Session B, %.0f s before Session A's connect "
                    "lead (%.0f s before its open); never between A and B. Needs RENDER_API_KEY."
                    % (config.REDEPLOY_LEAD_SECONDS, config.REDEPLOY_LEAD_SECONDS + _ws._preconnect()))
        logger.info("  Mid-session boot: %s" % ("trades the open session" if config.TRADE_ALLOW_MID_SESSION_START
                                                 else "sits it out, trades from the next exact open"))
    else:
        logger.info("  Trade sessions: OFF (SESSION_WINDOW_ENABLED=0) - trading all hours, connected all hours")
        logger.info("  Redeploy     : OFF (sessions are off)")
    if not (os.environ.get("PREDICTOR_STAKE") or "").strip():
        logger.warning("PREDICTOR_STAKE is not set on Render - using the default minimum stake %s", config.PREDICTOR_STAKE)
    import target_guard as _tg
    logger.info(f"  Balance target: {_tg.target() if _tg.target() else 'OFF (TARGET_BALANCE not set)'}")
    logger.info(f"  Port         : {config.PORT}")

    start_keep_alive()

    # Follower accounts: own thread + own event loop + Flask Blueprint. Fully optional:
    # any failure here is logged and the main account below starts regardless.
    try:
        import follower_manager
        import follower_web
        follower_web.register(app)
        follower_manager.start()

        def _on_sigterm(signum, frame):
            logger.warning("SIGTERM - flushing follower DB buffers")
            follower_manager.shutdown_blocking(8.0)
            raise SystemExit(0)
        import signal
        signal.signal(signal.SIGTERM, _on_sigterm)
    except Exception as exc:
        logger.error(f"Follower system not started: {exc}", exc_info=True)
    threading.Thread(target=_run_bot, name="bot-engine", daemon=True).start()
    logger.info("Bot engine thread started ✓")
    app.run(host="0.0.0.0", port=config.PORT, use_reloader=False, threaded=True)
