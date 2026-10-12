"""
window_scheduler.py - the hourly session clock.

  * The bot is connected to Deriv ONLY from WINDOW_PRECONNECT_SECONDS before minute START of every
    hour until the end of minute END (config.TRADE_WINDOW_*, UTC).  Buying is gated to the exact
    window by BotEngine; the pre-connect only gives the warm-up time to finish by START:00.
  * When the window closes the engine drains open contracts, disconnects, and this module asks
    Render for a redeploy WITH the build cache cleared (POST /v1/services/{id}/deploys
    {"clearCache": "clear"}).  The fresh instance idles until the next window, connects, trades.
  * A freshly booted instance never redeploys by itself: only an instance that actually ran a
    session does, so there is no redeploy loop.

Needs RENDER_API_KEY (RENDER_SERVICE_ID is injected by Render).  A deploy hook URL cannot clear the
cache, so it is not used here.
"""
import logging
import os
import time
from typing import Optional

import requests

import config
import render_api

logger = logging.getLogger("window")


def _preconnect() -> float:
    try:
        return max(0.0, float((os.environ.get("WINDOW_PRECONNECT_SECONDS") or "40").strip()))
    except ValueError:
        return 40.0


def _minute(ts: float) -> float:
    return (ts % 3600) / 60.0


def in_window(ts: float) -> bool:
    if not config.TRADE_WINDOW_ENABLED:              # SESSION_WINDOW_ENABLED=0: always "inside"
        return True
    m = int(ts // 60) % 60
    return config.TRADE_WINDOW_START_MINUTE <= m <= config.TRADE_WINDOW_END_MINUTE


def should_be_connected(ts: float) -> bool:
    """True from PRECONNECT seconds before the window opens until the window closes."""
    if in_window(ts):
        return True
    return seconds_until_open(ts) <= _preconnect()


def seconds_until_open(ts: float) -> float:
    """Seconds until the next window opening (0 while inside the window)."""
    if in_window(ts):
        return 0.0
    start = config.TRADE_WINDOW_START_MINUTE * 60
    into_hour = ts % 3600
    return (start - into_hour) % 3600


def redeploy_clear_cache() -> bool:
    """Blocking. Ask Render for a redeploy with the build cache cleared. True if accepted."""
    if not render_api.api_configured():
        logger.critical("REDEPLOY SKIPPED: RENDER_API_KEY / service id missing - cannot redeploy with a "
                        "cleared build cache. The bot will simply reconnect at the next window.")
        return False
    url = f"{render_api.API_BASE}/services/{render_api.service_id()}/deploys"
    headers = {"Authorization": f"Bearer {render_api.api_key()}", "Accept": "application/json",
               "Content-Type": "application/json"}
    try:
        r = requests.post(url, headers=headers, json={"clearCache": "clear"}, timeout=20)
        if r.status_code in (200, 201, 202):
            logger.warning("REDEPLOY accepted by Render (build cache CLEARED): HTTP %s", r.status_code)
            return True
        logger.error("REDEPLOY refused by Render: HTTP %s %s", r.status_code, r.text[:200])
    except Exception as exc:
        logger.error("REDEPLOY request failed: %s", exc)
    return False
