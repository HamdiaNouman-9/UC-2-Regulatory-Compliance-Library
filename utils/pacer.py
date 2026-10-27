from __future__  import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Dict, Optional,Tuple
from urllib.parse import urlparse

import yaml

logger=logging.getLogger(__name__)

REPO_ROOT=Path(__file__).resolve().parents[1]
CONFIG=REPO_ROOT/"config"/"pace.yml"

_FALLBACK_LEVELS={"normal":1.0,"careful":3.0,"very_careful":5.0}

MAX_THROTTLE_WAIT=120.0
DEFAULT_THROTTLE_WAIT=30.0

_lock=threading.Lock()
_next_slot:Dict[str,float]={}
_config:Optional[dict]=None
_blocked: Dict[str,Tuple[str,str]]={}
_throttled: Dict[str,int]={}
_contacted: set = set()

class SiteBlocked(Exception):
    def __init__(self,host:str,kind: str,reason: str):
        super().__init__(f"{host} refused us ({kind}): {reason}")
        self.host, self.kind, self.reason= host,kind,reason

class SiteThrottled(Exception):                                 
    def __init__(self, host: str, wait: float, reason: str):
        super().__init__(f"{host} asked us to slow down ({reason}); "
                         f"next request in {wait:.0f}s")
        self.host, self.wait, self.reason = host, wait, reason     

# reads the rule book(pace.yml)
def _load() -> dict:
    global _config
    if _config is None:
        try:
            raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8")) or {}
        except Exception as e:
            logger.warning("pacer: could not read %s (%s); using defaults", CONFIG, e)
            raw = {}
        levels = {k: float(v) for k, v in (raw.get("levels") or _FALLBACK_LEVELS).items()}
        _config = {
            "default": raw.get("default", "normal"),
            "levels": levels,
            # cleans the site names, writing them in lowercase
            "hosts": {h.lower().lstrip("."): lvl
                      for h, lvl in (raw.get("hosts") or {}).items()},
            "throttle_only": [h.lower().lstrip(".")                     # NEW
                              for h in (raw.get("throttle_only") or [])],
        }
    return _config
#gets the website name from a link
def _host(url:str)->str:
    value = (url or "").strip().lower()
    if "://" in value:
        host=urlparse(value).hostname or ""
    else:
        host= value.split("/")[0]    
    return host[4:] if host.startswith("www.") else host

def _matches(host: str, hosts) -> bool:                         # NEW
    """host, or any parent domain of it, is in `hosts`."""
    parts = host.split(".")
    return any(".".join(parts[i:]) in hosts for i in range(len(parts) - 1))    

#checks for site level
def level_for(host: str)->str:
    cfg=_load()
    parts= host.split(".")
    for i in range(len(parts)-1):
        lvl=cfg["hosts"].get(".".join(parts[i:]))
        if lvl:
            return lvl
    return cfg["default"]

#turns the level into secondsnown level %r for 
def pause_for(host:str)->float:
    cfg=_load()
    lvl=level_for(host)
    if lvl not in cfg["levels"]:
        logger.warning("pacer: unknown level %r for %s; using default", lvl,host)
        lvl=cfg["default"]
    return cfg["levels"].get(lvl,_FALLBACK_LEVELS["normal"])

# calculate the pause for website
def wait_turn(url:str, min_seconds: float=0.0)-> float:
    host=_host(url)
    if not host:
        return 0.0
    parts= host.split(".")
    for i in range(len(parts)-1):
        entry=_blocked.get(".".join(parts[i:]))
        if entry:
            raise SiteBlocked(host,*entry)
    _contacted.add(host)               
    pause=max(pause_for(host),float(min_seconds or 0))

    with _lock:
        now=time.monotonic()
        start=max(now,_next_slot.get(host,0.0))
        _next_slot[host]=start+pause
    wait=start-now
    if wait>0:
        logger.debug("pacer: %s waits %.1fs (%.0fs pause)",host,wait,pause)
        time.sleep(wait)
    return wait            

def mark_blocked(url: str, reason: str, kind: str = "block") -> None:
    """Put the site on this run's stopped list."""
    host = _host(url)
    if not host:
        return
    with _lock:
        first = host not in _blocked
        if first:
            _blocked[host] = (kind, reason)
    if first:
        logger.warning("pacer: %s refused us (%s: %s) - no more requests to it "
                       "this run", host, kind, reason)


def blocked_sites() -> Dict[str, Tuple[str, str]]:
    """Every site stopped in this run: host -> (kind, reason). For run reports."""
    with _lock:
        return dict(_blocked)


def contacted_sites() -> set:
    with _lock:
        return set(_contacted)

def _retry_after(headers) -> Optional[float]:
    """Seconds from a Retry-After header, or None when there is none."""
    if not headers:
        return None
    value = headers.get("Retry-After") or headers.get("retry-after")
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        return DEFAULT_THROTTLE_WAIT      # an HTTP date: treat as a short wait


def _text_kind(title: str, body: str) -> Tuple[str, str]:
    try:
        from generic_crawler.blockcheck import text_is_blocked, text_is_throttled
    except ImportError:
        return "", ""
    reason = text_is_throttled(title, body)       # first: 1015 is a throttle
    if reason:
        return "throttle", reason
    reason = text_is_blocked(title, body)
    if reason:
        return "block", reason
    return "", ""


def classify(url: str, status=None, headers=None, title: str = "",
             body: str = "") -> Tuple[str, str]:
    """('ok' | 'throttle' | 'block' | 'unknown', reason) for one answer."""
    host = _host(url)
    if status in (403, 404, 429) and _matches(host, _load()["throttle_only"]):
        return "ok", ""                   # its own retries handle these
    kind, reason = _text_kind(title, (body or "")[:3000])
    if kind:
        return kind, reason
    if status == 429:
        return "throttle", "HTTP 429"
    if status and status >= 400 and _retry_after(headers) is not None:
        return "throttle", f"HTTP {status} with Retry-After"
    if status == 403:
        return "unknown", "HTTP 403"
    return "ok", ""


def check_response(url: str, status=None, headers=None, title: str = "",
                   body: str = "") -> None:
    """Call right after every answer. Returns quietly when it is fine.
    Raises SiteThrottled (retry once allowed) or SiteBlocked (stop)."""
    host = _host(url)
    kind, reason = classify(url, status, headers, title, body)
    if kind == "ok":
        with _lock:
            _throttled.pop(host, None)
        return
    if kind == "throttle":
        wait = _retry_after(headers)
        wait = DEFAULT_THROTTLE_WAIT if wait is None else wait
        with _lock:
            count = _throttled.get(host, 0) + 1
            _throttled[host] = count
        if count == 1 and wait <= MAX_THROTTLE_WAIT:
            with _lock:
                _next_slot[host] = max(_next_slot.get(host, 0.0),
                                       time.monotonic() + wait)
            logger.warning("pacer: %s asked us to slow down (%s) - waiting %.0fs "
                           "before one retry", host, reason, wait)
            raise SiteThrottled(host, wait, reason)
        kind = "unknown"
        reason = (f"{reason}; refused again after waiting" if count > 1
                  else f"{reason}; asked to wait {wait:.0f}s")
    mark_blocked(url, reason, kind)
    raise SiteBlocked(host, kind, reason)

def reset_run()->None:
    with _lock:
        _blocked.clear()
        _throttled.clear()
        _contacted.clear()

def reload()->None:
    global _config
    with _lock:
        _config=None

__all__ = ["wait_turn", "pause_for", "level_for", "reload",
           "check_response", "classify", "mark_blocked", "blocked_sites","contacted_sites"
           "SiteBlocked", "SiteThrottled","reset_run"] 
 
