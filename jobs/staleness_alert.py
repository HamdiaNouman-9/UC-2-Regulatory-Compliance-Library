"""Alert when a regulator has produced no update for longer than its interval.

WHAT "AN UPDATE" IS
-------------------
The newest `regulation_versions.created_at` for the regulator. Every new or
modified document writes one (orchestrator._process_versioned_doc), and so does
a withdrawal, so it moves exactly when the library's content for that regulator
moves. It is NOT `run_history`: a crawl that ran fine and found nothing new is a
healthy quiet regulator, and a crawl that never ran is a broken one -- this
check cannot tell those apart on its own, so the alert says how long it has been
and leaves the diagnosis to a person (see `run_history` for whether crawls ran).

Read-only: one SELECT, nothing written and nothing sent. The result is served
by GET /monitoring/staleness in apis/pipeline_api.py; whoever calls it decides
what to do about it. Intervals: config/staleness.yml.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import yaml

_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = _ROOT / "config" / "staleness.yml"


def load_config(path: Path = CONFIG_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _as_utc(value) -> Optional[datetime]:
    """A datetime from the driver, made timezone-aware. created_at is written
    as UTC (datetime.now(timezone.utc)) but DATETIME columns come back naive."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def last_update_by_regulator(repo) -> Dict[str, datetime]:
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT regulator, MAX(created_at) FROM regulation_versions "
                    "WHERE regulator IS NOT NULL AND regulator <> '' "
                    "GROUP BY regulator")
        return {r[0]: _as_utc(r[1]) for r in cur.fetchall() if r[1] is not None}


def find_stale(last_seen: Dict[str, datetime], cfg: dict,
               now: Optional[datetime] = None) -> List[dict]:
    """Regulators whose newest update is older than their allowed interval.

    `regulators:` in the config is the watch list when present -- a regulator
    named there with no versions at all is reported too (never updated is
    the loudest case of stale). Without it, every regulator in the table is
    watched at the default interval.
    """
    now = now or datetime.now(timezone.utc)
    default_days = float(cfg.get("default_days", 30))
    overrides = cfg.get("regulators") or {}
    names = list(overrides) if overrides else list(last_seen)
    stale = []
    for name in names:
        limit = overrides.get(name)
        days = float(limit.get("days", default_days) if isinstance(limit, dict)
                     else limit if limit is not None else default_days)
        seen = last_seen.get(name)
        if seen is None or now - seen > timedelta(days=days):
            stale.append({"regulator": name, "limit_days": days,
                          "last_update": seen,
                          "days_since": None if seen is None
                          else round((now - seen).total_seconds() / 86400, 1)})
    return sorted(stale, key=lambda s: (s["days_since"] is not None,
                                        -(s["days_since"] or 0)))


def check_staleness(repo, now: Optional[datetime] = None) -> dict:
    cfg = load_config()
    now = now or datetime.now(timezone.utc)
    stale = find_stale(last_update_by_regulator(repo), cfg, now)
    for s in stale:
        if s["last_update"] is not None:
            s["last_update"] = s["last_update"].isoformat()
    return {"checked_at": now.isoformat(), "stale_count": len(stale), "stale": stale}
