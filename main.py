"""
main.py – entry point (Render web service).

  • Main thread  : Flask (dashboard + /health) – always running.
  • bot-engine   : asyncio thread running the digit predictor. Starts on EVERY boot
                   and is restarted if it crashes – there is no cycle, cooldown or idle phase.
  • keep-alive   : self-ping of /health.
  • restart sched: every REDEPLOY_INTERVAL_SECONDS (default 150) sets the "redeploy
                   pending" flag; bot_engine stops opening trades, drains open
                   contracts, flushes stats and triggers the Render redeploy.

Env vars: DERIV_API_TOKEN (required), RENDER_API_KEY (redeploy + stats persistence),
optional DERIV_APP_ID, DERIV_ACCOUNT_MODE ("demo" default / "real" picks which
account the token's OTP is minted for), DASHBOARD_PIN, PREDICTOR_SYMBOLS, ...
(see CHANGES.md).
"""
import asyncio
import logging
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
    from bot_engine import BotEngine
    while True:
        try:
            asyncio.run(BotEngine().run())
            logger.warning("Bot engine returned; restarting in 5 s")
        except Exception as exc:
            logger.error(f"Bot crashed: {exc}", exc_info=True)
        time.sleep(5)


if __name__ == "__main__":
    from keep_alive import app, start_keep_alive
    from restart_scheduler import start_restart_scheduler

    logger.info("Starting digit predictor bot …")
    logger.info(f"  Deriv App ID : {config.DERIV_APP_ID}")
    logger.info(f"  API token    : {'SET ✓' if config.DERIV_API_TOKEN else 'MISSING ✗'}")
    logger.info(f"  Symbols      : {config.PREDICTOR_SYMBOLS}  stake={config.PREDICTOR_STAKE}")
    logger.info(f"  Redeploy     : every {config.REDEPLOY_INTERVAL_SECONDS:.0f}s")
    logger.info(f"  Port         : {config.PORT}")

    start_keep_alive()
    start_restart_scheduler()
    threading.Thread(target=_run_bot, name="bot-engine", daemon=True).start()
    logger.info("Bot engine thread started ✓")
    app.run(host="0.0.0.0", port=config.PORT, use_reloader=False, threaded=True)
