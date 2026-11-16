"""CHG-009 step 10: the cycle's one re-attempt, 6 hours after a blocked first
attempt.

Nothing in the scheduler re-runs a job on the same day: `scheduler.py`
triggers each monitor job on its weekly or daily schedule through the API,
and the API runs it once. The per-cycle routine (dynamic_crawler/formfill/
access.py) wants one more attempt `REATTEMPT_HOURS` after a cycle's first
blocked attempt, so the API notes the due time here when a job's report
carries `access.cycle_attempt == 1`, and `scheduler_loop` starts the job
when it is due. The note is a JSON file under output/access/ so it
survives an API restart and a person can read what is pending.

Life of a note. `settle(job, result)` runs after every finished job: a
report whose host just blocked its cycle's first attempt writes (or
replaces) the note; a job that was skipped because another monitor job held
the crawl lock keeps the note and clears `started_at`, so the next tick
tries again; any other result removes it, the cycle having been closed or
the site read. `due()` lists notes whose time has passed and that are not
already running; the API marks one `started_at` when it starts the job. A
job that raised has its note removed by `pop`, so a broken job cannot loop.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
PATH = REPO_ROOT / "output" / "access" / "_reattempts.json"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _read(path: Path | None = None) -> dict:
    try:
        return json.loads((path or PATH).read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write(entries: dict, path: Path | None = None) -> None:
    p = path or PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(entries, indent=2), encoding="utf-8")


def _reports(result) -> list:
    """Every orchestrator report inside a job result (one, or a dict of them),
    the same reading as storage.run_store._collect_results."""
    if not isinstance(result, dict):
        return []
    if "access" in result or "changes" in result or "classified" in result:
        return [result]
    return [v for v in result.values() if isinstance(v, dict)
            and ("access" in v or "changes" in v or "classified" in v)]


def settle(job: str, result, path: Path | None = None) -> str | None:
    """Bring the note for `job` in line with how its run ended. Returns
    'noted' (a re-attempt is due later), 'kept' (the run was skipped on the
    crawl lock; the note waits for the next tick), 'removed', or None when
    there was nothing to do."""
    entries = _read(path)
    if isinstance(result, dict) and result.get("skipped"):
        if job in entries:
            entries[job].pop("started_at", None)
            _write(entries, path)
            return "kept"
        return None
    for rep in _reports(result):
        acc = rep.get("access") or {}
        if int(acc.get("cycle_attempt") or 0) == 1 and acc.get("next_attempt_after"):
            entries[job] = {"due": acc["next_attempt_after"], "host": acc.get("host"),
                            "noted_at": _now().isoformat(timespec="seconds")}
            _write(entries, path)
            return "noted"
    if entries.pop(job, None) is not None:
        _write(entries, path)
        return "removed"
    return None


def due(path: Path | None = None, now: datetime | None = None) -> list:
    """Jobs whose re-attempt time has passed and that are not running."""
    now = now or _now()
    out = []
    for job, e in _read(path).items():
        if e.get("started_at"):
            continue
        try:
            when = datetime.fromisoformat(e["due"])
        except Exception:
            out.append(job)                        # unreadable: run it, do not hold it
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        if now >= when:
            out.append(job)
    return out


def mark_started(job: str, path: Path | None = None) -> None:
    entries = _read(path)
    if job in entries:
        entries[job]["started_at"] = _now().isoformat(timespec="seconds")
        _write(entries, path)


def pop(job: str, path: Path | None = None) -> dict | None:
    entries = _read(path)
    e = entries.pop(job, None)
    if e is not None:
        _write(entries, path)
    return e
