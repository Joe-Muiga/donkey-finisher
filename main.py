"""
main.py – Entry point for the SIFM Deriv Trading Bot on Render.

Architecture:
  • Thread 1 (main) : Flask web server for Render's health checks —
                       always running, regardless of fixed-cycle phase.
  • Thread 2        : asyncio event loop running the bot engine — only
                       started while fixed_cycle.py's cycle is in its
                       TRADING phase (see below).
  • Thread 3        : self-ping keep-alive (pings /health every 40 s) —
                       always running, so Render sees a healthy service
                       24/7 even while the bot is deliberately not
                       connected to Deriv during the cooldown between
                       cycles.
  • Thread 4        : restart scheduler (rolling redeploy every
                       config.REDEPLOY_INTERVAL_HOURS) — only started in
                       the TRADING phase.
  • Thread 5        : fixed-cycle cooldown supervisor — only started
                       while the cycle is in its COOLDOWN phase, or the
                       instant TRADING transitions into COOLDOWN
                       (started from inside fixed_cycle.py itself in
                       that case, not from here).

Fixed single-leg trading cycle (Sep 2026, chat-requested; restructured
same day to drop leg 2 and randomize the cooldown — see fixed_cycle.py.
Supersedes the old balance-trend Donkey ORIGINAL/RAW switch and the
profit-target cycle, neither of which is imported anywhere anymore;
Donkey Strategy itself now runs ORIGINAL only, RAW is disabled, see
signal_engine._donkey_active_variant()): trading runs forever on a
simple fixed pattern that no balance, profit, drawdown, or time-of-day
condition can alter —
  1. Trade for config.FIXED_CYCLE_LEG_MINUTES ("leg 1") — ONE deploy, no
     auto-redeploy while it's running. config.REDEPLOY_INTERVAL_HOURS is
     already set to this same 5 minutes, so the rolling-redeploy
     mechanism below IS leg 1's timer, and its firing always means leg 1
     just ended.
  2. Drain open contracts, then disconnect from Deriv entirely for a
     cooldown drawn fresh, uniformly at random, between config.
     FIXED_CYCLE_COOLDOWN_MIN_MINUTES and config.
     FIXED_CYCLE_COOLDOWN_MAX_MINUTES (health checks only), then
     redeploy back into a fresh leg 1.
  3. Repeat forever.

main.py's only job for this is deciding, once per boot, whether this
deploy should start the bot at all or just sit in cooldown — everything
else lives in fixed_cycle.py, signal_engine.py and bot_engine.py.

Environment variables (only the first is truly required):
  DERIV_API_TOKEN        – your Deriv API token (trade + read scope)
  RENDER_API_KEY         – Render API key. Lets the bot redeploy itself and
                            remember its cycle + kill-switch state across
                            redeploys. Without it the bot still trades, but
                            the fixed cycle and kill-switch persistence are off.
  (optional) DERIV_APP_ID (default 1089), DERIV_ACCOUNT_MODE (default
            "demo"), DASHBOARD_PIN (default: last 6 chars of the token).
  Render injects RENDER_SERVICE_ID, RENDER_EXTERNAL_URL and PORT itself.
"""

import asyncio
import logging
import threading
import sys
import time
import config
import fixed_cycle

# ─── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = getattr(logging, config.LOG_LEVEL, logging.INFO),
    format  = "%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    stream  = sys.stdout,
)
logger = logging.getLogger("main")

# ─── Sanity check ─────────────────────────────────────────────────────────────
if not config.DERIV_API_TOKEN:
    logger.critical(
        "DERIV_API_TOKEN is not set!\n"
        "Add it as an environment variable in your Render service settings.\n"
        "Get your token at: https://app.deriv.com/account/api-token"
    )
    # Don't exit – let the web server start so Render won't kill the service.

# ─── Bot thread ───────────────────────────────────────────────────────────────
def _run_bot():
    from bot_engine import BotEngine
    bot = BotEngine()
    try:
        asyncio.run(bot.run())
    except Exception as exc:
        logger.error(f"Bot crashed: {exc}", exc_info=True)
    else:
        # run() can now return normally (not just via exception/cancel)
        # when bot_engine decided to enter the fixed-cycle cooldown after
        # leg 1 — see bot_engine.py's _main_loop / _settle_loop.
        # Nothing further to do here: fixed_cycle.enter_cooldown_now()
        # already persisted the cooldown window and started its own
        # supervisor thread before _main_loop stopped itself, so this
        # thread simply ends.
        logger.info(
            "Bot engine thread exited (redeploy-pending or "
            "fixed-cycle-cooldown-entered)")

# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from keep_alive import app, start_keep_alive
    from restart_scheduler import start_restart_scheduler

    logger.info("Starting SIFM Deriv Trading Bot …")
    logger.info(f"  Deriv App ID : {config.DERIV_APP_ID}")
    logger.info(f"  API token    : {'SET ✓' if config.DERIV_API_TOKEN else 'MISSING ✗'}")
    logger.info(f"  Port         : {config.PORT}")
    logger.info(f"  Self-URL     : {config.SELF_URL}")

    # 1. Start keep-alive pinger — ALWAYS, in every phase. This (plus the
    #    Flask app started at the bottom) is what keeps Render's health
    #    checks green during a strategy-switch cooldown even though the
    #    bot is deliberately not connected to Deriv.
    start_keep_alive()

    # 2. Cooldown phase check (Sep 2026) — decides whether this deploy
    #    should start the bot at all or just sit in cooldown, for
    #    fixed_cycle.py's single fixed leg-1-then-randomized-cooldown
    #    pattern (supersedes the old dual strategy-cycle / profit-cycle
    #    check).
    #    This deploy sits out — bot thread and rolling restart scheduler
    #    both stay off — while the cooldown is active; its own supervisor
    #    thread (started below) fires the Render deploy hook once the
    #    window elapses.
    fixed_cooldown_until = fixed_cycle.in_cooldown_at_boot()

    if fixed_cooldown_until is not None:
        remaining_min = max(0.0, fixed_cooldown_until - time.time()) / 60
        logger.warning(
            "\n" + "=" * 64 + "\n"
            f"FIXED-CYCLE COOLDOWN ACTIVE — {remaining_min:.1f} min "
            "remaining.\n"
            "This deploy will NOT connect to Deriv or start the bot "
            "engine — health checks only until the cooldown supervisor "
            "fires the next redeploy into a fresh leg 1.\n" + "=" * 64
        )
        fixed_cycle.start_cooldown_supervisor(fixed_cooldown_until)

        # Neither the bot thread nor the rolling restart scheduler are
        # started in this branch — there's nothing for either to do
        # while intentionally disconnected from Deriv, and starting the
        # restart scheduler here would just spam "redeploy pending"
        # warnings forever since nothing would ever drain/confirm it.

    else:
        # Either genuinely trading already, or a cooldown that has
        # already elapsed by the time this deploy came up (i.e. this
        # deploy IS the resumption the cooldown supervisor triggered) —
        # fixed_cooldown_until being None covers both.
        if fixed_cycle.load_state()["phase"] == "cooldown":
            # The second case above — flip the persisted phase back to
            # "trading" now so it doesn't linger as a stale "cooldown"
            # forever. Just for a clean, non-stale phase record;
            # bot_engine.run() starts a fresh leg 1 regardless via
            # fixed_cycle.log_trading_start().
            fixed_cycle.clear_cooldown_and_resume_trading()

        # 3. Start auto-redeploy scheduler (no-op if RENDER_DEPLOY_HOOK_URL
        #    is unset) — rolling redeploy every REDEPLOY_INTERVAL_HOURS
        #    while the bot is ON/trading.
        start_restart_scheduler()

        # 4. Start bot in background thread
        bot_thread = threading.Thread(target=_run_bot, name="bot-engine", daemon=True)
        bot_thread.start()
        logger.info("Bot engine thread started ✓")

    # 5. Flask in main thread (Render requires a bound web server) —
    #    ALWAYS, in every phase.
    logger.info(f"Starting Flask on 0.0.0.0:{config.PORT}")
    app.run(host="0.0.0.0", port=config.PORT, use_reloader=False, threaded=True)
