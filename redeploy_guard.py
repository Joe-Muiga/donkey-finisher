"""
redeploy_guard.py - safety for the 13-minute redeploy cycle.

  * deploy_request(): POST /v1/services/{id}/deploys {"clearCache": "do_not_clear"} (no commitId = LATEST commit).
    A deploy hook cannot clear the cache either; any "clear" query parameter on a hook URL is stripped.
    NOTHING in this module (or restart_scheduler) ever sends clearCache="clear".
  * preflight(): refuse a redeploy (and back off) when the kill switch is on, the test already finished, the engine is not
    ready / too young, this instance does not answer /health, or the last Render deploys failed / are still building.
  * crash-loop memory comes from Render itself (status of the latest deploys), so it survives instances.
"""
import logging
import os
import re
import time
from typing import Optional, Tuple

import requests

import config
import render_api

logger = logging.getLogger("redeploy")

NO_CLEAR = {"clearCache": "do_not_clear"}
FAILED = {"build_failed", "update_failed", "pre_deploy_failed", "canceled"}
IN_PROGRESS = {"created", "queued", "build_in_progress", "update_in_progress", "pre_deploy_in_progress"}

_state = {"fails": 0, "next_ok": 0.0, "last_reason": "", "engine_ready_at": 0.0, "last_deploy_id": "", "last_trigger": 0.0}


def mark_engine_ready() -> None:
    _state["engine_ready_at"] = time.time()


def backoff_seconds(fails: int) -> float:
    base = float(getattr(config, "REDEPLOY_BACKOFF_BASE_SECONDS", 300))
    cap = float(getattr(config, "REDEPLOY_BACKOFF_MAX_SECONDS", 7200))
    return min(cap, base * (2 ** max(0, fails - 1)))


def note_skip(reason: str) -> float:
    """Register a refused redeploy; returns how long to wait before the next attempt."""
    _state["fails"] += 1
    _state["last_reason"] = reason
    wait = backoff_seconds(_state["fails"])
    _state["next_ok"] = time.time() + wait
    logger.error("REDEPLOY SKIPPED (%s) - backing off %.0fs (consecutive refusals: %d)", reason, wait, _state["fails"])
    return wait


def note_ok() -> None:
    _state["fails"] = 0
    _state["last_reason"] = ""


def _headers() -> dict:
    return {"Authorization": f"Bearer {render_api.api_key()}", "Accept": "application/json", "Content-Type": "application/json"}


def recent_deploys(limit: int = 5) -> Optional[list]:
    """Newest-first list of {id, status} from Render; None when the API is unavailable."""
    if not render_api.api_configured():
        return None
    r = requests.get(f"{render_api.API_BASE}/services/{render_api.service_id()}/deploys",
                     headers=_headers(), params={"limit": limit}, timeout=15)
    r.raise_for_status()
    out = []
    for it in r.json():
        d = it.get("deploy", it)
        out.append({"id": d.get("id"), "status": d.get("status"), "createdAt": d.get("createdAt")})
    return out


def local_health(timeout: float = 5.0) -> bool:
    try:
        r = requests.get(f"http://127.0.0.1:{int(getattr(config, 'PORT', 10000))}/health", timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


def preflight() -> Tuple[bool, str]:
    """Blocking (run in a thread).  (True, '') = go; otherwise (False, reason) and the caller backs off."""
    try:
        import kill_switch
        if kill_switch.is_paused():
            return False, "kill switch engaged"
    except Exception:
        pass
    try:
        import exp_stats
        if config.TEST_MODE and exp_stats.EXP.is_done():
            return False, "experiment finished (final verdict printed) - no more cycles"
    except Exception:
        pass
    if not _state["engine_ready_at"]:
        return False, "engine not ready yet (boot/fit not finished)"
    if time.time() - _state["engine_ready_at"] < float(getattr(config, "REDEPLOY_MIN_UPTIME_SECONDS", 120)):
        return False, "instance younger than REDEPLOY_MIN_UPTIME_SECONDS"
    if time.time() < _state["next_ok"]:
        return False, f"backing off for another {_state['next_ok'] - time.time():.0f}s"
    if not local_health(min(10.0, float(getattr(config, "REDEPLOY_HEALTH_TIMEOUT_SECONDS", 180)))):
        return False, "local /health did not answer"
    try:
        deps = recent_deploys(5)
    except Exception as exc:
        return False, f"could not read Render deploy status: {exc}"
    if deps:
        st = deps[0].get("status")
        if st in FAILED:
            fails = 0
            for d in deps:
                if d.get("status") in FAILED:
                    fails += 1
                else:
                    break
            _state["fails"] = max(_state["fails"], fails - 1)
            return False, f"latest deploy {deps[0].get('id')} is {st}"
        if st in IN_PROGRESS:
            return False, f"latest deploy {deps[0].get('id')} still {st} (must be live before another one)"
    return True, ""


def deploy_request() -> Tuple[bool, str]:
    """Blocking. Ask Render for a deploy of the latest commit WITHOUT clearing the build cache."""
    hook = (os.environ.get("RENDER_DEPLOY_HOOK_URL") or getattr(config, "RENDER_DEPLOY_HOOK_URL", "") or "").strip()
    try:
        if render_api.api_configured():
            r = requests.post(f"{render_api.API_BASE}/services/{render_api.service_id()}/deploys",
                              headers=_headers(), json=NO_CLEAR, timeout=20)
            ok = r.status_code in (200, 201, 202)
            if ok:
                try:
                    _state["last_deploy_id"] = (r.json() or {}).get("id", "")
                except Exception:
                    pass
            logger.warning("REDEPLOY via API (clearCache=do_not_clear, latest commit): HTTP %s %s", r.status_code,
                           "" if ok else r.text[:200])
        elif hook:
            hook = re.sub(r"([?&])clear[^&]*&?", r"\1", hook).rstrip("?&")
            r = requests.post(hook, timeout=20)
            ok = r.status_code in (200, 201, 202)
            logger.warning("REDEPLOY via deploy hook (hooks never clear the cache): HTTP %s", r.status_code)
        else:
            return False, "no RENDER_API_KEY/RENDER_SERVICE_ID or RENDER_DEPLOY_HOOK_URL"
        if ok:
            _state["last_trigger"] = time.time()
            return True, _state["last_deploy_id"]
        return False, f"HTTP {r.status_code}"
    except Exception as exc:
        return False, str(exc)
