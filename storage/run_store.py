"""What each pipeline run found, kept so a person can review it.

Two tables, created on first use (additive, idempotent, like run_history):

    run_results   one row per job run: state, timing, error, the run's own report, alerts
    run_changes   one row per document a run found new / modified / deleted, plus the
                  reviewer's decision

WHAT A DECISION DOES (the review is the only thing that touches `regulations.status`,
which is the human decision column -- see Orchestrator._set_status):

    new       accepted -> status 'active'      rejected -> status 'reject'
    modified  accepted -> status 'active'      rejected -> the previous version is put back
                                               (content, hash, active flag); status untouched
    deleted   accepted -> status 'withdrawn'   rejected -> nothing changes, the decision is recorded

A run itself never writes any of this: new and modified documents are already in the
database when the run ends, deleted ones are still there, and nothing is withdrawn until
somebody accepts it.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DECISIONS = ("accepted", "rejected")
CHANGE_TYPES = ("new", "modified", "deleted")


class DecisionError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def ensure_tables(cursor) -> None:
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'run_results' AND xtype = 'U')
        CREATE TABLE run_results (
            run_id       INT IDENTITY(1,1) PRIMARY KEY,
            job          NVARCHAR(100)  NOT NULL,
            regulators   NVARCHAR(300)  NULL,
            state        NVARCHAR(20)   NOT NULL,
            started_at   DATETIME       NULL,
            finished_at  DATETIME       NULL,
            seconds      FLOAT          NULL,
            error        NVARCHAR(MAX)  NULL,
            summary      NVARCHAR(MAX)  NULL,
            alert_level  NVARCHAR(10)   NULL,
            alerts       NVARCHAR(MAX)  NULL,
            created_at   DATETIME       NOT NULL DEFAULT GETUTCDATE()
        )""")
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name = 'run_changes' AND xtype = 'U')
        CREATE TABLE run_changes (
            change_id           INT IDENTITY(1,1) PRIMARY KEY,
            run_id              INT            NOT NULL,
            change_type         NVARCHAR(12)   NOT NULL,
            regulation_id       INT            NULL,
            version_id          INT            NULL,
            previous_version_id INT            NULL,
            title               NVARCHAR(500)  NULL,
            source_system       NVARCHAR(200)  NULL,
            document_url        NVARCHAR(1000) NULL,
            detail              NVARCHAR(MAX)  NULL,
            decision            NVARCHAR(12)   NOT NULL DEFAULT 'pending',
            reviewed_by         NVARCHAR(100)  NULL,
            reviewed_at         DATETIME       NULL,
            note                NVARCHAR(500)  NULL
        )""")
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_run_changes_run')
        CREATE INDEX ix_run_changes_run ON run_changes (run_id, change_type)""")


# --------------------------------------------------------------------------- #
#  alerts                                                                      #
# --------------------------------------------------------------------------- #

def alerts_for(state: str, error: Optional[str], result: Optional[dict]) -> List[dict]:
    """The reasons a person should look at this run. Pure, so it is testable."""
    out: List[dict] = []
    r = result if isinstance(result, dict) else {}

    def add(level, code, message):
        out.append({"level": level, "code": code, "message": message})

    if state == "failed":
        add("critical", "job_failed", (error or "the job raised")[:300])
    if r.get("skipped"):
        add("warning", "run_skipped", str(r.get("reason") or r.get("skipped"))[:200])
    if r.get("run_trustworthy") is False:
        add("warning", "run_untrusted", "; ".join(r.get("gate_problems") or [])[:300] or "gate failed")
    if r.get("baseline_verdict") not in (None, "PASS"):
        add("warning", "baseline_not_passed", str(r.get("baseline_verdict")))
    feed = r.get("feed") or {}
    if feed.get("status") == "unavailable":
        add("warning", "feed_unavailable", str(feed.get("reason"))[:200])
    ch = r.get("changes") or {}
    empties = [c for c in ch.get("modified") or []
               if (c.get("detail") or {}).get("old_chars", 0) > 0 and (c.get("detail") or {}).get("new_chars", 1) == 0]
    if empties:
        add("warning", "empty_content_versions",
            f"{len(empties)} modified document(s) were stored with NO text where the previous "
            f"version had some; a blank fetch may have been recorded as a change")
    counts = (r.get("withdrawals") or {}).get("counts") or {}
    if counts.get("withdrawal-proposed"):
        add("info", "withdrawals_proposed", f"{counts['withdrawal-proposed']} document(s) proposed for withdrawal")
    pending = sum(len(ch.get(k) or []) for k in CHANGE_TYPES)
    if pending:
        add("info", "changes_pending_review",
            f"{len(ch.get('new') or [])} new, {len(ch.get('modified') or [])} modified, "
            f"{len(ch.get('deleted') or [])} deleted")
    return out


def _level(alerts: List[dict]) -> str:
    order = {"critical": 3, "warning": 2, "info": 1}
    return max((a["level"] for a in alerts), key=lambda x: order[x], default="none")


def _notify(run_id: int, job: str, alerts: List[dict]) -> None:
    """Best-effort webhook for warning and critical alerts (ALERT_WEBHOOK_URL)."""
    url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    worth = [a for a in alerts if a["level"] in ("warning", "critical")]
    if not url or not worth:
        return
    try:
        import requests
        requests.post(url, json={"run_id": run_id, "job": job, "alerts": worth}, timeout=5)
    except Exception as e:                         # an alert must never break the run record
        logger.warning("alert webhook failed: %s", e)


# --------------------------------------------------------------------------- #
#  writing                                                                     #
# --------------------------------------------------------------------------- #

def _dt(v) -> Optional[datetime]:
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v)) if v else None
    except ValueError:
        return None


def _collect_results(result) -> List[dict]:
    """Every orchestrator report inside a job result. A job runs one regulator (the
    report is the result) or several (a dict of reports)."""
    if not isinstance(result, dict):
        return []
    if "changes" in result or "classified" in result:
        return [result]
    return [v for v in result.values() if isinstance(v, dict) and ("changes" in v or "classified" in v)]


def save_run(repo, job: str, regulators: List[str], state: str, started, finished,
             error: Optional[str], result: Optional[dict]) -> int:
    """Store one job run and the changes it found. Returns run_id."""
    reports = _collect_results(result)
    changes = [c for rep in reports for k in CHANGE_TYPES for c in (rep.get("changes") or {}).get(k, [])]
    merged = dict(result) if isinstance(result, dict) else {}
    if not reports:
        alerts = alerts_for(state, error, merged)
    else:
        alerts = alerts_for(state, error, None)
        for rep in reports:
            for a in alerts_for("finished", None, rep):
                if a not in alerts:
                    alerts.append(a)
    summary = json.loads(json.dumps(merged, default=str))
    for rep in [summary] + [v for v in summary.values() if isinstance(v, dict)]:
        rep.pop("changes", None)                       # the rows live in run_changes
    t0, t1 = _dt(started), _dt(finished)
    seconds = (t1 - t0).total_seconds() if t0 and t1 else None
    with repo._get_conn() as conn:
        cur = conn.cursor()
        ensure_tables(cur)
        cur.execute(
            "INSERT INTO run_results (job, regulators, state, started_at, finished_at, seconds, error, "
            "summary, alert_level, alerts) OUTPUT INSERTED.run_id VALUES (?,?,?,?,?,?,?,?,?,?)",
            job, ",".join(regulators or [])[:300], state, t0, t1, seconds, error,
            json.dumps(summary, default=str), _level(alerts), json.dumps(alerts))
        run_id = int(cur.fetchone()[0])
        for c in changes:
            cur.execute(
                "INSERT INTO run_changes (run_id, change_type, regulation_id, version_id, previous_version_id, "
                "title, source_system, document_url, detail) VALUES (?,?,?,?,?,?,?,?,?)",
                run_id, c["type"], c.get("regulation_id"), c.get("version_id"), c.get("previous_version_id"),
                c.get("title"), c.get("source_system"), c.get("document_url"),
                json.dumps(c.get("detail") or {}, default=str))
        conn.commit()
    _notify(run_id, job, alerts)
    return run_id


# --------------------------------------------------------------------------- #
#  reading                                                                     #
# --------------------------------------------------------------------------- #

def _iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


def _run_row(r) -> dict:
    return {"run_id": r[0], "job": r[1], "regulators": [x for x in (r[2] or "").split(",") if x],
            "state": r[3], "started_at": _iso(r[4]), "finished_at": _iso(r[5]),
            "seconds": r[6], "error": r[7], "alert_level": r[9] or "none",
            "alerts": json.loads(r[10] or "[]")}


_RUN_COLS = ("run_id, job, regulators, state, started_at, finished_at, seconds, error, summary, "
             "alert_level, alerts")


def _counts(cur, run_ids: List[int]) -> Dict[int, dict]:
    if not run_ids:
        return {}
    marks = ",".join("?" * len(run_ids))
    cur.execute(f"SELECT run_id, change_type, decision, COUNT(*) FROM run_changes "
                f"WHERE run_id IN ({marks}) GROUP BY run_id, change_type, decision", *run_ids)
    out: Dict[int, dict] = {i: {t: {"total": 0, "pending": 0} for t in CHANGE_TYPES} for i in run_ids}
    for rid, typ, dec, n in cur.fetchall():
        out[rid][typ]["total"] += n
        if dec == "pending":
            out[rid][typ]["pending"] += n
    return out


def list_runs(repo, regulator: Optional[str] = None, job: Optional[str] = None,
              state: Optional[str] = None, alert: Optional[str] = None,
              limit: int = 50, offset: int = 0) -> dict:
    where, args = ["1=1"], []
    if regulator:
        where.append("(',' + regulators + ',') LIKE ?"); args.append(f"%,{regulator},%")
    if job:
        where.append("job = ?"); args.append(job)
    if state:
        where.append("state = ?"); args.append(state)
    if alert:
        where.append("alert_level = ?"); args.append(alert)
    with repo._get_conn() as conn:
        cur = conn.cursor()
        ensure_tables(cur)
        cur.execute(f"SELECT COUNT(*) FROM run_results WHERE {' AND '.join(where)}", *args)
        total = int(cur.fetchone()[0])
        cur.execute(f"SELECT {_RUN_COLS} FROM run_results WHERE {' AND '.join(where)} "
                    f"ORDER BY run_id DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", *args, offset, limit)
        rows = [_run_row(r) for r in cur.fetchall()]
        counts = _counts(cur, [r["run_id"] for r in rows])
    for r in rows:
        r["changes"] = counts.get(r["run_id"])
    return {"total": total, "limit": limit, "offset": offset, "runs": rows}


def get_run(repo, run_id: int) -> Optional[dict]:
    with repo._get_conn() as conn:
        cur = conn.cursor()
        ensure_tables(cur)
        cur.execute(f"SELECT {_RUN_COLS} FROM run_results WHERE run_id = ?", run_id)
        r = cur.fetchone()
        if not r:
            return None
        out = _run_row(r)
        out["summary"] = json.loads(r[8] or "{}")
        out["changes"] = _counts(cur, [run_id])[run_id]
    return out


def list_changes(repo, run_id: int, change_type: Optional[str] = None, decision: Optional[str] = None,
                 limit: int = 100, offset: int = 0) -> Optional[dict]:
    """Rows shaped for the reviewer: `new` rows carry no updated_at (nothing came before
    them); `modified` and `deleted` rows carry the stored row's updated_at."""
    where, args = ["c.run_id = ?"], [run_id]
    if change_type:
        where.append("c.change_type = ?"); args.append(change_type)
    if decision:
        where.append("c.decision = ?"); args.append(decision)
    with repo._get_conn() as conn:
        cur = conn.cursor()
        ensure_tables(cur)
        cur.execute("SELECT 1 FROM run_results WHERE run_id = ?", run_id)
        if not cur.fetchone():
            return None
        w = " AND ".join(where)
        cur.execute(f"SELECT COUNT(*) FROM run_changes c WHERE {w}", *args)
        total = int(cur.fetchone()[0])
        cur.execute(
            f"SELECT c.change_id, c.change_type, c.regulation_id, c.version_id, c.previous_version_id, c.title, "
            f"c.source_system, c.document_url, c.detail, c.decision, c.reviewed_by, c.reviewed_at, c.note, "
            f"g.regulator, g.published_date, CONVERT(varchar(33), g.updated_at, 127), g.status "
            f"FROM run_changes c LEFT JOIN regulations g ON g.id = c.regulation_id WHERE {w} "
            f"ORDER BY c.change_id OFFSET ? ROWS FETCH NEXT ? ROWS ONLY", *args, offset, limit)
        items = []
        for r in cur.fetchall():
            item = {"change_id": r[0], "type": r[1], "regulation_id": r[2], "version_id": r[3],
                    "previous_version_id": r[4], "title": r[5], "regulator": r[13], "source_system": r[6],
                    "document_url": r[7], "published_date": _iso(r[14]),
                    "detail": json.loads(r[8] or "{}"), "decision": r[9], "reviewed_by": r[10],
                    "reviewed_at": _iso(r[11]), "note": r[12], "review_status": r[16] or None}
            if r[1] != "new":
                item["updated_at"] = _iso(r[15])
            items.append(item)
    return {"run_id": run_id, "total": total, "limit": limit, "offset": offset, "changes": items}


# --------------------------------------------------------------------------- #
#  deciding                                                                    #
# --------------------------------------------------------------------------- #

def decide(repo, run_id: int, change_id: int, decision: str, reviewed_by: str,
           note: Optional[str] = None) -> dict:
    if decision not in DECISIONS:
        raise DecisionError(f"decision must be one of {DECISIONS}", 422)
    if not (reviewed_by or "").strip():
        raise DecisionError("reviewed_by is required", 422)
    with repo._get_conn() as conn:
        cur = conn.cursor()
        ensure_tables(cur)
        cur.execute("SELECT change_type, regulation_id, version_id, previous_version_id, decision "
                    "FROM run_changes WHERE change_id = ? AND run_id = ?", change_id, run_id)
        row = cur.fetchone()
        if not row:
            raise DecisionError(f"change {change_id} not found in run {run_id}", 404)
        typ, reg_id, ver_id, prev_id, current = row
        if current != "pending":
            if current == decision:
                return {"change_id": change_id, "decision": decision, "applied": "already recorded"}
            raise DecisionError(f"change {change_id} was already {current}", 409)
        applied = _apply(cur, typ, reg_id, ver_id, prev_id, decision)
        cur.execute("UPDATE run_changes SET decision=?, reviewed_by=?, reviewed_at=GETUTCDATE(), note=? "
                    "WHERE change_id=?", decision, reviewed_by.strip()[:100], (note or "")[:500] or None, change_id)
        conn.commit()
    return {"change_id": change_id, "type": typ, "regulation_id": reg_id, "decision": decision, "applied": applied}


def _apply(cur, typ, reg_id, ver_id, prev_id, decision) -> str:
    if reg_id is None:
        return "no stored row to change"
    if typ == "new":
        cur.execute("UPDATE regulations SET status=? WHERE id=?", "active" if decision == "accepted" else "reject", reg_id)
        return f"regulations.status = {'active' if decision == 'accepted' else 'reject'}"
    if typ == "deleted":
        if decision == "accepted":
            cur.execute("UPDATE regulations SET status='withdrawn' WHERE id=?", reg_id)
            return "regulations.status = withdrawn"
        return "nothing changed"
    # modified
    if decision == "accepted":
        cur.execute("UPDATE regulations SET status='active' WHERE id=?", reg_id)
        return "regulations.status = active"
    if not prev_id:
        raise DecisionError("this change has no previous version to restore", 409)
    cur.execute("SELECT content_html, content_hash FROM regulation_versions WHERE version_id=? AND regulation_id=?",
                prev_id, reg_id)
    old = cur.fetchone()
    if not old:
        raise DecisionError(f"previous version {prev_id} no longer exists", 409)
    cur.execute("UPDATE regulation_versions SET status='inactive' WHERE regulation_id=? AND status='active'", reg_id)
    cur.execute("UPDATE regulation_versions SET status='active' WHERE version_id=?", prev_id)
    cur.execute("UPDATE regulations SET document_html=?, content_hash=? WHERE id=?", old[0], old[1], reg_id)
    return f"version {prev_id} restored as the active version"


def regulation_ids_for_run(repo, run_id: int, types=("new", "modified"),
                           include_rejected: bool = False) -> Optional[List[int]]:
    """Regulation ids a stored run wrote, for triggering analysis on them. None if the
    run does not exist. Changes a reviewer REJECTED are left out unless asked for."""
    types = [t for t in types if t in CHANGE_TYPES]
    with repo._get_conn() as conn:
        cur = conn.cursor()
        ensure_tables(cur)
        cur.execute("SELECT 1 FROM run_results WHERE run_id = ?", run_id)
        if not cur.fetchone():
            return None
        if not types:
            return []
        marks = ",".join("?" * len(types))
        sql = (f"SELECT DISTINCT regulation_id FROM run_changes WHERE run_id = ? AND change_type IN ({marks}) "
               f"AND regulation_id IS NOT NULL")
        if not include_rejected:
            sql += " AND decision <> 'rejected'"
        cur.execute(sql, run_id, *types)
        return sorted(int(r[0]) for r in cur.fetchall())


def alerts_feed(repo, level: Optional[str] = None, since: Optional[str] = None, limit: int = 50) -> dict:
    """Runs that raised at least one alert, newest first."""
    where, args = ["alert_level IN ('warning','critical')"], []
    if level:
        where = ["alert_level = ?"]; args.append(level)
    if since:
        where.append("created_at >= ?"); args.append(_dt(since))
    with repo._get_conn() as conn:
        cur = conn.cursor()
        ensure_tables(cur)
        cur.execute(f"SELECT TOP (?) run_id, job, regulators, state, finished_at, alert_level, alerts "
                    f"FROM run_results WHERE {' AND '.join(where)} ORDER BY run_id DESC", limit, *args)
        items = [{"run_id": r[0], "job": r[1], "regulators": [x for x in (r[2] or "").split(",") if x],
                  "state": r[3], "finished_at": _iso(r[4]), "level": r[5],
                  "alerts": [a for a in json.loads(r[6] or "[]") if a["level"] != "info"]} for r in cur.fetchall()]
    return {"count": len(items), "alerts": items}
