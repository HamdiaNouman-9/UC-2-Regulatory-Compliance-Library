"""Review, alerts, scheduling and usage endpoints.

    GET  /runs                              every stored job run (filter: regulator, job, state, alert)
    GET  /runs/{run_id}                     one run: state, timing, alerts, its own report, change counts
    GET  /runs/{run_id}/changes             the new / modified / deleted documents of that run
    POST /runs/{run_id}/changes/{id}/decision   accept or reject ONE change
    POST /runs/{run_id}/decisions           accept or reject several at once
    GET  /alerts                            runs that raised a warning or critical alert
    GET  /scheduler/jobs                    what config/scheduler.yml schedules and when it fires next
    GET  /metrics                           request counts and latency, job history, table sizes

The frontend flow: trigger with POST /trigger/regulators, poll GET /trigger/regulators/status
until the job is `finished`, then read GET /runs?job=<job>&limit=1 and its /changes.
"""
from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime
from threading import Lock
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from storage import run_store

router = APIRouter()
_repo = None


def init(repo) -> None:
    global _repo
    _repo = repo


class DecisionBody(BaseModel):
    decision: str = Field(..., description="accepted | rejected")
    reviewed_by: str = Field(..., description="who is deciding; stored with the decision")
    note: Optional[str] = Field(None, max_length=500)


class BulkDecisionBody(DecisionBody):
    change_ids: List[int] = Field(..., min_length=1, max_length=500)


def _guard(fn, *a, **k):
    try:
        return fn(_repo, *a, **k)
    except run_store.DecisionError as e:
        raise HTTPException(e.status, str(e))


@router.get("/runs", tags=["Runs"])
def list_runs(regulator: Optional[str] = None, job: Optional[str] = None, state: Optional[str] = None,
              alert: Optional[str] = Query(None, description="none | info | warning | critical"),
              limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    return _guard(run_store.list_runs, regulator=regulator, job=job, state=state, alert=alert,
                  limit=limit, offset=offset)


@router.get("/runs/{run_id}", tags=["Runs"])
def get_run(run_id: int):
    run = _guard(run_store.get_run, run_id)
    if not run:
        raise HTTPException(404, f"run {run_id} not found")
    return run


@router.get("/runs/{run_id}/changes", tags=["Runs"])
def run_changes(run_id: int, type: Optional[str] = Query(None, description="new | modified | deleted"),
                decision: Optional[str] = Query(None, description="pending | accepted | rejected"),
                limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    if type and type not in run_store.CHANGE_TYPES:
        raise HTTPException(422, f"type must be one of {run_store.CHANGE_TYPES}")
    if decision and decision not in ("pending",) + run_store.DECISIONS:
        raise HTTPException(422, "decision must be pending, accepted or rejected")
    out = _guard(run_store.list_changes, run_id, change_type=type, decision=decision, limit=limit, offset=offset)
    if out is None:
        raise HTTPException(404, f"run {run_id} not found")
    return out


@router.post("/runs/{run_id}/changes/{change_id}/decision", tags=["Runs"])
def decide_one(run_id: int, change_id: int, body: DecisionBody):
    return _guard(run_store.decide, run_id, change_id, body.decision, body.reviewed_by, body.note)


@router.post("/runs/{run_id}/decisions", tags=["Runs"])
def decide_many(run_id: int, body: BulkDecisionBody):
    """Each change is decided on its own; one that cannot be (already decided differently,
    no previous version to restore) is reported and does not stop the others."""
    done, failed = [], []
    for cid in body.change_ids:
        try:
            done.append(run_store.decide(_repo, run_id, cid, body.decision, body.reviewed_by, body.note))
        except run_store.DecisionError as e:
            failed.append({"change_id": cid, "status": e.status, "error": str(e)})
    return {"run_id": run_id, "decided": len(done), "failed": failed, "results": done}


@router.get("/alerts", tags=["Alerts"])
def alerts(level: Optional[str] = Query(None, description="warning | critical (default: both)"),
           since: Optional[str] = Query(None, description="ISO date/time"),
           limit: int = Query(50, ge=1, le=200)):
    if level and level not in ("warning", "critical"):
        raise HTTPException(422, "level must be warning or critical")
    return _guard(run_store.alerts_feed, level=level, since=since, limit=limit)


# --------------------------------------------------------------------------- #
#  scheduler                                                                   #
# --------------------------------------------------------------------------- #

@router.get("/scheduler/jobs", tags=["Scheduler"])
def scheduler_jobs():
    """The schedule in config/scheduler.yml, with the next firing time worked out from it.

    The scheduler itself is a separate process (`python scheduler/scheduler.py`); this
    endpoint reads its CONFIG and cannot tell whether that process is running.
    """
    from apscheduler.triggers.cron import CronTrigger
    from scheduler import scheduler as sched

    cfg = sched.load_scheduler_config().get("jobs") or {}
    last = {}
    with _repo._get_conn() as conn:
        cur = conn.cursor()
        run_store.ensure_tables(cur)
        cur.execute("SELECT job, MAX(run_id) FROM run_results GROUP BY job")
        ids = {r[0]: r[1] for r in cur.fetchall()}
        for job, rid in ids.items():
            cur.execute("SELECT state, finished_at, alert_level FROM run_results WHERE run_id=?", rid)
            r = cur.fetchone()
            last[job] = {"run_id": rid, "state": r[0], "finished_at": run_store._iso(r[1]), "alert_level": r[2]}
    out = []
    for name, c in cfg.items():
        nxt, err = None, None
        if c.get("enabled") and c.get("trigger") == "cron":
            try:
                trig = CronTrigger(timezone=sched.TIMEZONE, **(c.get("schedule") or {}))
                from datetime import timezone as _tz
                nxt = trig.get_next_fire_time(None, datetime.now(_tz.utc))
                nxt = nxt.isoformat() if nxt else None
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
        mapped = name in sched.JOB_MAPPING
        out.append({"job": name, "enabled": bool(c.get("enabled")), "trigger": c.get("trigger"),
                    "schedule": c.get("schedule"), "next_run": nxt, "mapped_to_a_function": mapped,
                    "last_run": last.get(name), "config_error": err})
    return {"timezone": sched.TIMEZONE, "execution_mode": sched.EXECUTION_MODE,
            "note": "read from config/scheduler.yml; the scheduler runs as its own process",
            "jobs": out}


# --------------------------------------------------------------------------- #
#  usage / metrics                                                             #
# --------------------------------------------------------------------------- #

class RequestMetrics:
    def __init__(self):
        self.since = datetime.utcnow().isoformat()
        self._lock = Lock()
        self._by: Dict[str, dict] = defaultdict(lambda: {"count": 0, "errors_5xx": 0, "errors_4xx": 0,
                                                         "total_ms": 0.0, "max_ms": 0.0})

    def record(self, route: str, status: int, seconds: float) -> None:
        ms = seconds * 1000
        with self._lock:
            m = self._by[route]
            m["count"] += 1
            m["total_ms"] += ms
            m["max_ms"] = max(m["max_ms"], ms)
            if status >= 500:
                m["errors_5xx"] += 1
            elif status >= 400:
                m["errors_4xx"] += 1

    def snapshot(self) -> dict:
        with self._lock:
            routes = {r: {"count": m["count"], "errors_4xx": m["errors_4xx"], "errors_5xx": m["errors_5xx"],
                          "avg_ms": round(m["total_ms"] / m["count"], 1), "max_ms": round(m["max_ms"], 1)}
                      for r, m in self._by.items()}
        return {"since": self.since, "total_requests": sum(v["count"] for v in routes.values()),
                "by_route": dict(sorted(routes.items(), key=lambda kv: -kv[1]["count"]))}


metrics = RequestMetrics()


@router.get("/metrics", tags=["Metrics"])
def get_metrics():
    """In-process request counters (reset when the server restarts), plus what the
    database remembers: job history and table sizes. LLM token usage is on GET /llm/usage."""
    with _repo._get_conn() as conn:
        cur = conn.cursor()
        run_store.ensure_tables(cur)
        cur.execute("SELECT job, COUNT(*), SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END), AVG(seconds), MAX(finished_at) "
                    "FROM run_results GROUP BY job")
        jobs = {r[0]: {"runs": r[1], "failed": r[2] or 0, "avg_seconds": round(r[3], 1) if r[3] else None,
                       "last_finished_at": run_store._iso(r[4])} for r in cur.fetchall()}
        cur.execute("SELECT change_type, COUNT(*) FROM run_changes WHERE decision='pending' GROUP BY change_type")
        pending = {r[0]: r[1] for r in cur.fetchall()}
    return {"generated_at": datetime.utcnow().isoformat(), "api": metrics.snapshot(), "jobs": jobs,
            "pending_review": {t: pending.get(t, 0) for t in run_store.CHANGE_TYPES},
            "database": _repo.counts()}
