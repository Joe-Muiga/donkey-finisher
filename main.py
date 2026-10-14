"""
main.py – entry point (Render web service).

  • Main thread  : Flask (dashboard + /health) – always running.
  • bot-engine   : asyncio thread running the digit predictor. Starts on EVERY boot
                   and is restarted if it crashes – there is no cycle, cooldown or idle phase.
  • keep-alive   : self-ping of /health.
  • restart sched: OFF by default. Render env var AUTO_REDEPLOY=1 turns it on; the interval is
                   config.AUTO_REDEPLOY_SECONDS (bottom of config.py, default 780 = 13 min). It sets
                   the "redeploy pending" flag every N seconds; bot_engine then stops opening
                   trades, drains open contracts, flushes stats and triggers a Render redeploy.

Env vars: DERIV_API_TOKEN (required), RENDER_API_KEY (redeploy + stats persistence),
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


def _run_bot():
    """Run the engine; if it ever exits or crashes, start it again (never sit idle)."""
    import target_guard
    if config.MH_MODE:
        from mh_engine import MHEngine as Engine
        from set_engine import account_allowed
        allowed, why = account_allowed()
        if not allowed:
            logger.critical("MH MODE REFUSED TO START: %s", why)
            return                                   # demo-only guard, same as set mode
    elif config.SET_MODE:
        from set_engine import SetEngine as Engine, account_allowed
        allowed, why = account_allowed()
        if not allowed:
            logger.critical("SET MODE REFUSED TO START: %s", why)
            return                                   # no restart loop: this needs a config change
    else:
        from bot_engine import BotEngine as Engine
    import window_scheduler as ws
    import keep_alive
    while True:
        now = time.time()
        if not ws.should_be_connected(now):              # outside the window: NOT connected to Deriv
            wait = ws.seconds_until_open(now)
            keep_alive.update_status(session=f"IDLE - disconnected, next window in {int(wait // 60)} min")
            time.sleep(min(15.0, max(1.0, wait - ws._preconnect())))
            continue
        eng = Engine()
        try:
            asyncio.run(eng.run())
        except Exception as exc:
            logger.error(f"Bot crashed: {exc}", exc_info=True)
        if target_guard.is_reached():
            logger.warning("Balance target reached - bot stays stopped (no restart)")
            return
        if getattr(eng, "_session_over", False):
            # the window ran and closed: redeploy with a cleared build cache BEFORE the next connect
            keep_alive.update_status(session="IDLE - redeploying (build cache cleared)")
            if ws.redeploy_clear_cache():
                time.sleep(900)                          # Render replaces this instance; just wait
            else:
                time.sleep(60)                           # no redeploy possible: reconnect at next window anyway
            continue
        logger.warning("Bot engine returned inside the window; restarting in 5 s")
        time.sleep(5)


if __name__ == "__main__":
    from keep_alive import app, start_keep_alive

    logger.info("Starting digit predictor bot … (%s)",
                "MH MODE: multi-horizon digit model, h<=%d, all 18 Over/Under covers, admitted on calibrated edge" % config.MH_HMAX_TRADE
                if config.MH_MODE else "SET MODE: top-%d digit set -> covering Over/Under, all symbols in parallel" % config.SET_SIZE
                if config.SET_MODE else "original single-contract mode")
    logger.info(f"  Deriv App ID : {config.DERIV_APP_ID}")
    logger.info(f"  API token    : {'SET ✓' if config.DERIV_API_TOKEN else 'MISSING ✗'}")
    logger.info(f"  Symbols      : {config.PREDICTOR_SYMBOLS}  "
                f"min stake (PREDICTOR_STAKE)={config.PREDICTOR_STAKE}")
    logger.info(f"  Account mode : {config.DERIV_ACCOUNT_MODE}")
    if config.TEST_MODE:
        import exp_mode
        logger.warning("  TEST MODE ON : frozen config hash %s | demo only | flat stake %s | followers OFF", exp_mode.config_hash(), config.TEST_STAKE)
    logger.info("  Trade window : %s" % (f"minutes {config.TRADE_WINDOW_START_MINUTE}-{config.TRADE_WINDOW_END_MINUTE} of every hour (UTC), strict"
                                         if config.TRADE_WINDOW_ENABLED else "OFF (SESSION_WINDOW_ENABLED=0) - trading all hours, no hourly redeploy"))
    if not (os.environ.get("PREDICTOR_STAKE") or "").strip():
        logger.warning("PREDICTOR_STAKE is not set on Render - using the default minimum stake %s", config.PREDICTOR_STAKE)
    if config.REDEPLOY_INTERVAL_SECONDS > 0:
        logger.info(f"  Redeploy     : AUTO_REDEPLOY ON - every {config.REDEPLOY_INTERVAL_SECONDS:.0f}s "
                    f"({config.REDEPLOY_INTERVAL_SECONDS / 60:.1f} min); hourly trade window forced OFF")
    else:
        logger.info("  Redeploy     : OFF (set AUTO_REDEPLOY=1 on Render to enable)")
    import target_guard as _tg
    logger.info(f"  Balance target: {_tg.target() if _tg.target() else 'OFF (TARGET_BALANCE not set)'}")
    logger.info(f"  Port         : {config.PORT}")

    start_keep_alive()
    if config.REDEPLOY_INTERVAL_SECONDS > 0:
        import restart_scheduler
        restart_scheduler.start_restart_scheduler()      # rolling timer + watchdog (own thread)

    # Follower accounts: own thread + own event loop + Flask Blueprint. Fully optional:
    # any failure here is logged and the main account below starts regardless.
    try:
        if config.TEST_MODE:
            raise RuntimeError("TEST_MODE: follower mirroring is OFF")
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
        logger.error(f"Follower system not started: {exc}", exc_info=not config.TEST_MODE)
    threading.Thread(target=_run_bot, name="bot-engine", daemon=True).start()
    logger.info("Bot engine thread started ✓")
    app.run(host="0.0.0.0", port=config.PORT, use_reloader=False, threaded=True)
