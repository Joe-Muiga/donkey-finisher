"""
redeploy_guard.py - safety around the ONE hourly pre-Session-A redeploy (planned by redeploy_planner.py).

  * deploy_request(): POST /v1/services/{id}/deploys {"clearCache": "do_not_clear"} (no commitId = LATEST commit).
    A deploy hook cannot clear the cache either; any "clear" query parameter on a hook URL is stripped.
    NOTHING in this repo ever asks Render to clear the build cache.
  * preflight(): refuse a redeploy when the kill switch is on, the balance target was reached, this instance does not
    answer /health, the Render API cannot be read, the last deploys FAILED (exponential back-off measured from the failed
    deploy's own age, so it can never block forever), a deploy is still building, or a deploy already happened in this
    idle gap (Render's own history is the memory, so a freshly booted instance can never fire a second one).
  * hold-connect guard: once a deploy has been fired, THIS (old) instance must not connect at the next connect lead -
    the new instance will, and two instances must never trade the same account.  The hold ends only when the deploy
    is known to have FAILED / been cancelled (then this instance simply carries on) or after REDEPLOY_MAX_DURATION_SECONDS
    with no readable status.  If the deploy goes live, Render stops this process.
  * measured deploy durations (createdAt -> finishedAt of Render's recent deploys) feed the planner's P95.
"""
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

import config
import render_api

logger = logging.getLogger("redeploy")

NO_CLEAR = {"clearCache": "do_not_clear"}
FAILED = {"build_failed", "update_failed", "pre_deploy_failed", "canceled"}
IN_PROGRESS = {"created", "queued", "build_in_progress", "update_in_progress", "pre_deploy_in_progress"}
LIVE_OK = {"live", "deactivated"}

_lock = threading.Lock()
_state = {"fails": 0, "next_ok": 0.0, "last_reason": "", "last_deploy_id": "", "last_trigger": 0.0,
          "fired_at": 0.0, "hold": False, "hold_reason": "", "deploy_status": "", "deploy_live_at": 0.0}


# ─── failure back-off ────────────────────────────────────────────────────────────────────────────────
def backoff_seconds(fails: int) -> float:
    base = float(getattr(config, "REDEPLOY_BACKOFF_BASE_SECONDS", 300))
    cap = float(getattr(config, "REDEPLOY_BACKOFF_MAX_SECONDS", 7200))
    return min(cap, base * (2 ** max(0, fails - 1)))


def note_skip(reason: str) -> None:
    with _lock:
        _state["last_reason"] = reason
    logger.warning("REDEPLOY SKIPPED: %s", reason)


def note_ok() -> None:
    with _lock:
        _state["fails"] = 0
        _state["last_reason"] = ""


# ─── Render API helpers ──────────────────────────────────────────────────────────────────────────────
def _headers() -> dict:
    return {"Authorization": f"Bearer {render_api.api_key()}", "Accept": "application/json", "Content-Type": "application/json"}


def parse_ts(v) -> Optional[float]:
    """Render's ISO-8601 timestamp -> epoch seconds."""
    if not v:
        return None
    try:
        s = str(v).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        s = re.sub(r"(\.\d{6})\d+", r"\1", s)                 # fromisoformat takes at most microseconds
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.timestamp()
    except Exception:
        return None


def _row(item: dict) -> dict:
    d = item.get("deploy", item)
    return {"id": d.get("id"), "status": d.get("status"), "createdAt": parse_ts(d.get("createdAt")),
            "finishedAt": parse_ts(d.get("finishedAt"))}


def recent_deploys(limit: int = 5) -> Optional[List[dict]]:
    """Newest-first [{id, status, createdAt(epoch), finishedAt(epoch|None)}]; None when the API is not configured."""
    if not render_api.api_configured():
        return None
    r = requests.get(f"{render_api.API_BASE}/services/{render_api.service_id()}/deploys",
                     headers=_headers(), params={"limit": int(limit)}, timeout=15)
    r.raise_for_status()
    return [_row(it) for it in r.json()]


def measured_durations(limit: Optional[int] = None) -> List[float]:
    """Seconds from createdAt to finishedAt of recent deploys that went live (what a redeploy really costs)."""
    limit = int(limit or getattr(config, "REDEPLOY_HISTORY_LIMIT", 30))
    out: List[float] = []
    for d in recent_deploys(limit) or []:
        if d["status"] in LIVE_OK and d["createdAt"] and d["finishedAt"] and d["finishedAt"] > d["createdAt"]:
            out.append(d["finishedAt"] - d["createdAt"])
    return out


def deploy_status(deploy_id: str) -> Optional[str]:
    r = requests.get(f"{render_api.API_BASE}/services/{render_api.service_id()}/deploys/{deploy_id}",
                     headers=_headers(), timeout=15)
    r.raise_for_status()
    return _row(r.json())["status"]


def local_health(timeout: float = 5.0) -> bool:
    try:
        r = requests.get(f"http://127.0.0.1:{int(getattr(config, 'PORT', 10000))}/health", timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


# ─── preflight ───────────────────────────────────────────────────────────────────────────────────────
def preflight(gap_start: float, now: Optional[float] = None) -> Tuple[bool, str]:
    """Blocking (run in a thread).  (True, '') = go; otherwise (False, reason).  `gap_start` = epoch of the idle gap's
    first second (Session B's drain end): a deploy created after it means this gap already got its redeploy."""
    now = time.time() if now is None else now
    try:
        import kill_switch
        if kill_switch.is_paused():
            return False, "kill switch engaged"
    except Exception:
        pass
    try:
        import target_guard
        if target_guard.is_reached():
            return False, "balance target reached"
    except Exception:
        pass
    if not render_api.api_configured() and not (os.environ.get("RENDER_DEPLOY_HOOK_URL") or "").strip():
        return False, "no RENDER_API_KEY / RENDER_SERVICE_ID (or deploy hook)"
    if now < _state["next_ok"]:
        return False, f"backing off for another {_state['next_ok'] - now:.0f}s"
    if not local_health(min(10.0, float(getattr(config, "REDEPLOY_HEALTH_TIMEOUT_SECONDS", 180)))):
        return False, "local /health did not answer"
    if not render_api.api_configured():
        return True, ""                                   # deploy-hook only: nothing more can be checked
    try:
        deps = recent_deploys(10)
    except Exception as exc:
        return False, f"could not read Render deploy status: {exc}"
    deps = deps or []
    for d in deps:
        if d["createdAt"] and d["createdAt"] >= gap_start:
            return False, f"a deploy ({d['id']}, {d['status']}) was already created in this gap"
    if deps:
        top = deps[0]
        if top["status"] in IN_PROGRESS:
            return False, f"latest deploy {top['id']} still {top['status']} (must be live before another one)"
        if top["status"] in FAILED:
            fails = 0
            for d in deps:
                if d["status"] in FAILED:
                    fails += 1
                else:
                    break
            age = now - float(top["createdAt"] or now)
            wait = backoff_seconds(fails)
            with _lock:
                _state["fails"] = fails
            if age < wait:                                # measured from the failed deploy, so this ends by itself
                return False, f"latest deploy {top['id']} is {top['status']}; back-off {wait:.0f}s (age {age:.0f}s)"
    return True, ""


# ─── deploy request ──────────────────────────────────────────────────────────────────────────────────
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
                    _state["last_deploy_id"] = (r.json() or {}).get("id", "") or ""
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
            note_fired(_state["last_deploy_id"])
            return True, _state["last_deploy_id"]
        return False, f"HTTP {r.status_code}"
    except Exception as exc:
        return False, str(exc)


# ─── hold-connect guard ──────────────────────────────────────────────────────────────────────────────
def note_fired(deploy_id: str = "") -> None:
    with _lock:
        _state["last_trigger"] = _state["fired_at"] = time.time()
        _state["hold"] = True
        _state["hold_reason"] = f"deploy {deploy_id or '?'} fired - the NEW instance will connect, not this one"
        _state["deploy_status"] = "fired"


def hold_connect() -> bool:
    """True while this (old) instance must NOT open a Deriv connection because a fresh deploy is replacing it."""
    if not _state["hold"]:
        return False
    max_age = float(getattr(config, "REDEPLOY_MAX_DURATION_SECONDS", 1500))
    if time.time() - _state["fired_at"] > max_age + 600 and _state["deploy_status"] not in LIVE_OK:
        release_hold("deploy status unknown for a very long time")
        return False
    return True


def release_hold(reason: str) -> None:
    with _lock:
        was = _state["hold"]
        _state["hold"] = False
        _state["hold_reason"] = ""
    if was:
        logger.warning("REDEPLOY HOLD RELEASED (%s) - this instance may connect again", reason)


def track_deploy(deploy_id: str, poll_seconds: float, deadline: float) -> str:
    """Blocking. Poll the fired deploy until it ends, fails or `deadline` (epoch) passes.  Returns the last status."""
    status = ""
    t_fire = time.time()
    while time.time() < deadline:
        try:
            if deploy_id:
                status = deploy_status(deploy_id) or ""
                with _lock:
                    _state["deploy_status"] = status
                if status in LIVE_OK:
                    _state["deploy_live_at"] = time.time()
                    logger.warning("REDEPLOY live after %.0fs (Render status %s) - this instance will be stopped by Render",
                                   time.time() - t_fire, status)
                    return status
                if status in FAILED:
                    with _lock:
                        _state["fails"] += 1
                    logger.error("REDEPLOY FAILED (Render status %s) - this instance keeps running", status)
                    release_hold(f"deploy {status}")
                    return status
        except Exception as exc:
            logger.warning("REDEPLOY status poll failed: %s", str(exc)[:120])
        time.sleep(max(1.0, poll_seconds))
    return status


def snapshot() -> Dict[str, object]:
    with _lock:
        return dict(_state)
