"""End-to-end check of the run-review API against the real database, on SCRATCH data.

    python tests/run_review_e2e.py

Creates three throw-away regulations under the regulator "ZZ TEST REGULATOR" (with
versions), stores one synthetic run that reports them as new / modified / deleted, drives
the API through TestClient (list, changes, decisions, alerts, metrics, scheduler), checks
what each decision did to the rows, and then deletes everything it created.
"""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")
import truststore  # noqa: E402

truststore.inject_into_ssl()

from fastapi.testclient import TestClient  # noqa: E402

import apis.pipeline_api as api  # noqa: E402
from storage import run_store  # noqa: E402

repo = api.repo
client = TestClient(api.app)
FAILS = []


def check(label, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + label + (f"  [{extra}]" if extra and not cond else ""))
    if not cond:
        FAILS.append(label)


def q(sql, *a):
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, *a)
        return cur.fetchall()


def x(sql, *a):
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, *a)
        conn.commit()


def make_reg(title, html):
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO regulations (title, regulator, source_system, category, document_url, document_html, content_hash, status) "
                    "OUTPUT INSERTED.id VALUES (?, 'ZZ TEST REGULATOR', 'ZZ-TEST', 'ZZ', ?, ?, ?, '')",
                    title, f"https://example.invalid/{title}", html, f"hash-{title}")
        rid = int(cur.fetchone()[0])
        conn.commit()
    return rid


def make_version(rid, html, h, status):
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO regulation_versions (regulation_id, regulator, content_html, content_text, content_hash, "
                    "updated_date, change_summary, status) OUTPUT INSERTED.version_id VALUES (?, 'ZZ TEST REGULATOR', ?, ?, ?, "
                    "CAST(GETDATE() AS DATE), 'zz test', ?)", rid, html, html, h, status)
        vid = int(cur.fetchone()[0])
        conn.commit()
    return vid


run_ids, reg_ids = [], []
try:
    r_new = make_reg("ZZ-new", "<p>brand new</p>")
    r_mod = make_reg("ZZ-mod", "<p>version two text</p>")
    r_del = make_reg("ZZ-del", "<p>going away</p>")
    reg_ids = [r_new, r_mod, r_del]
    v_new = make_version(r_new, "<p>brand new</p>", "n1", "active")
    v_old = make_version(r_mod, "<p>version one text</p>", "m1", "inactive")
    v_cur = make_version(r_mod, "", "m2", "active")          # blank new version: should raise the empty-content alert
    make_version(r_del, "<p>going away</p>", "d1", "active")

    synthetic = {"regulator": "ZZ TEST REGULATOR", "crawled": 3, "classified": {"new": 1, "modified": 1, "unchanged": 0, "disappeared": 1},
                 "processed": 2, "run_trustworthy": True, "baseline_verdict": "PASS", "gate_problems": [],
                 "withdrawals": {"counts": {"withdrawal-proposed": 1, "watching": 0, "not-judged": 0}},
                 "changes": {
                     "new": [{"type": "new", "regulation_id": r_new, "version_id": v_new, "previous_version_id": None,
                              "title": "ZZ-new", "source_system": "ZZ-TEST", "document_url": "u1", "detail": {}}],
                     "modified": [{"type": "modified", "regulation_id": r_mod, "version_id": v_cur, "previous_version_id": v_old,
                                   "title": "ZZ-mod", "source_system": "ZZ-TEST", "document_url": "u2",
                                   "detail": {"old_chars": 22, "new_chars": 0}}],
                     "deleted": [{"type": "deleted", "regulation_id": r_del, "version_id": None, "previous_version_id": None,
                                  "title": "ZZ-del", "source_system": "ZZ-TEST", "document_url": "u3",
                                  "detail": {"withdrawal": "withdrawal-proposed"}}]}}
    run_id = run_store.save_run(repo, "zz_test_job", ["ZZTEST"], "finished", "2026-09-21T10:00:00", "2026-09-21T10:05:00", None, synthetic)
    run_ids.append(run_id)
    failed_id = run_store.save_run(repo, "zz_test_job", ["ZZTEST"], "failed", "2026-09-21T11:00:00", "2026-09-21T11:00:03", "RuntimeError: boom", None)
    run_ids.append(failed_id)

    # ---- reading ---------------------------------------------------------
    r = client.get(f"/runs?job=zz_test_job"); j = r.json()
    check("GET /runs filters by job", r.status_code == 200 and j["total"] == 2, r.text[:200])
    r = client.get(f"/runs/{run_id}"); j = r.json()
    check("GET /runs/{id} has counts and alerts", j["changes"]["new"]["total"] == 1 and j["changes"]["deleted"]["pending"] == 1
          and {a["code"] for a in j["alerts"]} >= {"empty_content_versions", "withdrawals_proposed", "changes_pending_review"}, str(j)[:300])
    check("alert level is warning", j["alert_level"] == "warning", j["alert_level"])
    check("GET /runs/{id} carries the run's own report, minus the change rows", j["summary"].get("crawled") == 3 and "changes" not in j["summary"])
    check("unknown run is 200 with an error detail", client.get("/runs/99999999").status_code == 200
          and "not found" in client.get("/runs/99999999").json().get("detail", ""))
    ch = client.get(f"/runs/{run_id}/changes").json()["changes"]
    by = {c["type"]: c for c in ch}
    check("three changes listed", len(ch) == 3)
    check("`new` carries NO updated_at", "updated_at" not in by["new"])
    check("`modified` and `deleted` carry updated_at", "updated_at" in by["modified"] and "updated_at" in by["deleted"])
    check("type filter", [c["type"] for c in client.get(f"/runs/{run_id}/changes?type=deleted").json()["changes"]] == ["deleted"])
    r = client.get(f"/runs/{run_id}/changes?type=bogus")
    check("bad type is 200 with an error detail", r.status_code == 200 and "type must be" in r.json().get("detail", ""))
    al = client.get("/alerts").json()
    check("alerts feed lists the warning run and the critical failed run",
          {a["run_id"] for a in al["alerts"]} >= {run_id, failed_id}, str(al)[:300])
    check("failed run is critical", client.get(f"/runs/{failed_id}").json()["alert_level"] == "critical")

    # ---- deciding --------------------------------------------------------
    cid = {c["type"]: c["change_id"] for c in ch}
    D = lambda cid_, dec, who="tester": client.post(f"/runs/{run_id}/changes/{cid_}/decision", json={"decision": dec, "reviewed_by": who})
    check("decision needs reviewed_by", D(cid["new"], "accepted", "").status_code == 422)
    check("bad decision is 422", D(cid["new"], "maybe").status_code == 422)
    r = D(cid["new"], "accepted"); check("accept NEW", r.status_code == 200, r.text[:200])
    check("  -> regulations.status = active", q("select status from regulations where id=?", r_new)[0][0] == "active")
    check("re-deciding the same way is idempotent", D(cid["new"], "accepted").json().get("applied") == "already recorded")
    check("re-deciding differently is 409", D(cid["new"], "rejected").status_code == 409)
    r = D(cid["modified"], "rejected"); check("reject MODIFIED", r.status_code == 200, r.text[:200])
    active = q("select version_id from regulation_versions where regulation_id=? and status='active'", r_mod)
    check("  -> previous version is active again", [a[0] for a in active] == [v_old], str(active))
    check("  -> regulations row got the old html back", q("select document_html from regulations where id=?", r_mod)[0][0] == "<p>version one text</p>")
    r = D(cid["deleted"], "accepted"); check("accept DELETED", r.status_code == 200, r.text[:200])
    check("  -> regulations.status = withdrawn", q("select status from regulations where id=?", r_del)[0][0] == "withdrawn")
    row = client.get(f"/runs/{run_id}/changes?decision=accepted").json()
    check("reviewer and time are stored", all(c["reviewed_by"] == "tester" and c["reviewed_at"] for c in row["changes"]) and row["total"] == 2)
    check("pending count dropped", client.get(f"/runs/{run_id}").json()["changes"]["new"]["pending"] == 0)

    # bulk: a fresh run so there is something pending
    run2 = run_store.save_run(repo, "zz_test_job", ["ZZTEST"], "finished", "2026-09-21T12:00:00", "2026-09-21T12:01:00", None,
                              {"classified": {}, "changes": {"new": [{"type": "new", "regulation_id": r_new, "version_id": v_new, "previous_version_id": None, "title": "again", "source_system": "ZZ-TEST", "document_url": "u", "detail": {}}], "modified": [], "deleted": []}})
    run_ids.append(run2)
    ids2 = [c["change_id"] for c in client.get(f"/runs/{run2}/changes").json()["changes"]]
    r = client.post(f"/runs/{run2}/decisions", json={"decision": "rejected", "reviewed_by": "tester", "change_ids": ids2 + [999999999]})
    j = r.json()
    check("bulk decides what it can and reports the rest", r.status_code == 200 and j["decided"] == 1 and j["failed"][0]["status"] == 404, r.text[:300])

    # ---- the rest --------------------------------------------------------
    m = client.get("/metrics").json()
    check("metrics: request counters and job history", m["api"]["total_requests"] > 5 and "zz_test_job" in m["jobs"] and "pending_review" in m, str(m)[:200])
    s = client.get("/scheduler/jobs").json()
    enabled = [j for j in s["jobs"] if j["enabled"]]
    check("scheduler: enabled cron jobs have a next_run", enabled and all(j["next_run"] for j in enabled), str([j["job"] for j in enabled if not j["next_run"]]))
    check("scheduler: no config errors", not [j for j in s["jobs"] if j["config_error"]], str([j for j in s["jobs"] if j["config_error"]][:2]))
finally:
    for rid in reg_ids:
        x("delete from regulation_versions where regulation_id=?", rid)
        x("delete from regulations where id=?", rid)
    for rid in run_ids:
        x("delete from run_changes where run_id=?", rid)
        x("delete from run_results where run_id=?", rid)
    left = q("select count(*) from regulations where regulator='ZZ TEST REGULATOR'")[0][0] + q("select count(*) from run_results where job='zz_test_job'")[0][0]
    print("cleanup: scratch rows left =", left)

print(f"\n{len(FAILS)} failing" if FAILS else "\nall checks passed")
sys.exit(1 if FAILS else 0)
