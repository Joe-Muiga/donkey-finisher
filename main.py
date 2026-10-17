"""
main.py – entry point (Render web service).

  • Main thread   : Flask (dashboard + /health) – always running.
  • bot-engine    : one trading engine PER SESSION.  Two sessions every hour, every day, in UTC and on Deriv's server
                    clock (window_scheduler.py / config.py):  Session A HH:06:00.000-HH:16:01.000, Session B
                    HH:21:00.000-HH:29:01.000.  The thread connects at the connect lead before each open, trades,
                    drains and hard-disconnects after the close; between sessions the bot is NOT connected to Deriv.
  • redeploy      : redeploy_planner.py - ONE redeploy per hour, fired from the idle gap after Session B so that the
                    new deploy is live just before the Session A connect lead.  No timer cycle, no other redeploys.
  • keep-alive    : self-ping of /health (keeps the Render instance awake; talks only to this service).
  • followers     : follower_manager.py, on the same two sessions.

Env vars: DERIV_API_TOKEN (required), RENDER_API_KEY (the hourly redeploy, TARGET_BALANCE suspend), optional
DERIV_APP_ID, DERIV_ACCOUNT_MODE ("demo" default / "real" picks which account the token's OTP is minted for),
DASHBOARD_PIN, PREDICTOR_SYMBOLS, ...

Follower accounts (optional): DATABASE_URL, ADMIN_PASSWORD, ENCRYPTION_KEY - see render.yaml.
"""
import asyncio
import logging
import os
import signal
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


def decide_engine_start(phase: str, resume_mid_session: bool, held: bool) -> str:
    """Pure decision used by the loop (and the tests): what to do inside a connection window that has no engine yet.
       'start' | 'skip_mid_session' | 'skip_drain'."""
    if phase == "DRAIN":
        return "skip_drain"                      # the session is already closed: never open a connection for it
    if phase == "LIVE" and (held or not resume_mid_session):
        return "skip_mid_session"                # booted inside the session: wait for the next open
    return "start"


def _run_bot():
    """One engine per session: connect at the connect lead, trade, drain, disconnect; idle (disconnected) otherwise."""
    import target_guard
    import keep_alive
    import redeploy_guard
    import window_scheduler as ws
    Engine = _engine_class()
    handled = None                                   # `open` of the connection window this process is done with
    logged_hold = None
    while True:
        if target_guard.is_reached():
            logger.warning("Balance target reached - bot stays stopped (no restart)")
            return
        err = ws.load_error()
        if err:                                      # fail closed: no valid schedule, no connection, no trading
            time.sleep(30)
            continue
        sess = ws.connection_session()
        if sess is None or sess.open == handled:     # between sessions (or this window is finished): NOT connected
            time.sleep(min(1.0, max(0.05, ws.next_connect_in() if sess is None else 0.5)))
            continue
        if redeploy_guard.hold_connect():            # a fresh deploy is replacing this instance: only ONE may trade
            if logged_hold != sess.open:
                logged_hold = sess.open
                logger.warning("SESSION %s: NOT connecting - %s", sess.slot, redeploy_guard.snapshot().get("hold_reason"))
            time.sleep(1.0)
            continue
        phase = ws.phase()["phase"]
        held = ws.note_engine_start()                # a start inside an open session is held when RESUME_... is False
        action = decide_engine_start(phase, bool(config.RESUME_TRADING_MID_SESSION_AFTER_BOOT), held is not None)
        if action != "start":
            logger.warning("SESSION %s: engine not started (%s) - next connect at the next session's lead", sess.slot, action)
            handled = sess.open
            continue
        eng = Engine()
        started = time.time()
        keep_alive.update_status(engine_alive=True, running=True, engine_phase="connecting")
        try:
            asyncio.run(eng.run())
        except Exception as exc:
            logger.error(f"Bot crashed: {exc}", exc_info=True)
        finally:
            keep_alive.update_status(engine_alive=False, running=False, engine_phase="")
        if target_guard.is_reached():
            logger.warning("Balance target reached - bot stays stopped (no restart)")
            return
        if getattr(eng, "_session_over", False):
            logger.warning("SESSION %s finished after %.0fs - disconnected, idle until the next connect lead", sess.slot,
                           time.time() - started)
            handled = sess.open
            continue
        logger.warning("Bot engine returned before its session ended (after %.0fs); a crash restart inside an open session "
                       "waits for the next open (RESUME_TRADING_MID_SESSION_AFTER_BOOT=False)", time.time() - started)
        time.sleep(5)


def _on_sigterm(signum, frame):
    """Render stops the old instance after a deploy: flush what must be flushed and exit at once."""
    logger.warning("SIGTERM - clean exit")
    try:
        import follower_manager
        follower_manager.shutdown_blocking(8.0)
    except Exception:
        pass
    raise SystemExit(0)


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
    _sched = _ws.schedule()
    if _sched is None:
        logger.critical("  Sessions     : SCHEDULE NOT LOADED (%s) - NO CONNECTION AND NO TRADING", _ws.load_error())
    else:
        for _line in _ws.describe_sessions():
            logger.info("  %s", _line)
        logger.info("  Sessions     : every hour, every day, UTC, judged on Deriv's server clock; disconnected between sessions; "
                    "boot inside a session %s", "TRADES the rest of it" if config.RESUME_TRADING_MID_SESSION_AFTER_BOOT
                    else "waits for the next open")
    if config.IGNORED_LEGACY_ENV:
        logger.warning("IGNORED legacy environment variable(s) still set on Render: %s - they no longer do anything "
                       "(the schedule is fixed in config.py; the periodic redeploy is gone). Delete them in the Render dashboard.",
                       ", ".join(config.IGNORED_LEGACY_ENV))
    if not (os.environ.get("PREDICTOR_STAKE") or "").strip():
        logger.warning("PREDICTOR_STAKE is not set on Render - using the default minimum stake %s", config.PREDICTOR_STAKE)
    if config.PRE_A_REDEPLOY_ENABLED:
        logger.info("  Redeploy     : ONE per hour from the idle gap after Session B, fired (expected deploy duration + margin) "
                    "before the Session A connect lead; default expected duration %ss; no cycle timer",
                    config.REDEPLOY_DEFAULT_DURATION_SECONDS)
    else:
        logger.info("  Redeploy     : OFF (PRE_A_REDEPLOY_ENABLED=0 in config.py)")
    import target_guard as _tg
    logger.info(f"  Balance target: {_tg.target() if _tg.target() else 'OFF (TARGET_BALANCE not set)'}")
    logger.info(f"  Port         : {config.PORT}")

    try:
        import kill_switch
        kill_switch.load_persisted()
    except Exception as exc:
        logger.error("kill switch state not loaded: %s", exc)

    signal.signal(signal.SIGTERM, _on_sigterm)
    start_keep_alive()

    # Follower accounts: own thread + own event loop + Flask Blueprint. Fully optional:
    # any failure here is logged and the main account below starts regardless.
    try:
        import follower_manager
        import follower_web
        follower_web.register(app)
        follower_manager.start()
    except Exception as exc:
        logger.error(f"Follower system not started: {exc}", exc_info=True)
    threading.Thread(target=_run_bot, name="bot-engine", daemon=True).start()
    logger.info("Bot engine thread started ✓")
    try:
        import redeploy_planner
        redeploy_planner.start()                         # Render API only; never touches Deriv
    except Exception as exc:
        logger.error("redeploy planner not started: %s", exc, exc_info=True)
    app.run(host="0.0.0.0", port=config.PORT, use_reloader=False, threaded=True)
