"""The two analysis-trigger endpoints, against the real database on SCRATCH rows.

    python tests/analysis_trigger_e2e.py

The analysis runner is replaced by a fake, so NO LLM call is made and nothing is analysed;
this checks the triggering, skipping, batching and progress logic. Scratch rows use the
regulator "ZZ TEST REGULATOR" and are deleted at the end.
"""
import sys
import time
from datetime import datetime
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

repo, client, FAILS = api.repo, TestClient(api.app), []


def check(label, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + label + (f"  [{extra}]" if extra and not cond else ""))
    if not cond:
        FAILS.append(label)


def x(sql, *a):
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, *a)
        conn.commit()


def make_reg(title):
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO regulations (title, regulator, source_system, category, document_url, document_html, content_hash, status) "
                    "OUTPUT INSERTED.id VALUES (?, 'ZZ TEST REGULATOR', 'ZZ-TEST', 'ZZ', ?, '<p>x</p>', ?, '')",
                    title, f"https://example.invalid/{title}", f"h-{title}")
        rid = int(cur.fetchone()[0])
        conn.commit()
    return rid


calls, already = [], set()


def fake_runner(rid):
    calls.append(rid)
    time.sleep(0.05)
    with api._analysis_lock:
        if rid == FAIL_ID:
            api._analysis_state[rid] = {"state": "failed", "error": "boom", "finished_at": datetime.utcnow().isoformat()}
        else:
            api._analysis_state[rid] = {"state": "done", "requirements_extracted": 3, "activities_extracted": 5,
                                        "finished_at": datetime.utcnow().isoformat()}


def wait(batch_id, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        b = client.get(f"/analysis/batches/{batch_id}").json()
        if b["state"] == "finished":
            return b
        time.sleep(0.2)
    return b


reg_ids, run_ids = [], []
real_runner, real_reqs = api._run_requirement_activity_analysis_for_regulation, repo.get_requirements_for_regulation
try:
    a, b, c, d = (make_reg(f"ZZ-an-{i}") for i in range(4))
    reg_ids = [a, b, c, d]
    FAIL_ID = c
    already.add(b)
    api._run_requirement_activity_analysis_for_regulation = fake_runner
    repo.get_requirements_for_regulation = lambda rid, active_only=True: [{"requirement_id": 1}] if rid in already else []

    # ---- 1. by list of ids ----------------------------------------------
    r = client.post("/analysis/trigger", json={"regulation_ids": [a, b, c, 999999999, a]})
    j = r.json()
    check("POST /analysis/trigger returns 202 with a batch id", r.status_code == 202 and j["batch_id"], r.text[:200])
    check("  duplicates dropped, unknown id reported, analysed one skipped",
          j["requested"] == 4 and j["skipped"]["not_found"] == [999999999] and j["skipped"]["already_analysed"] == [b], str(j["skipped"]))
    check("  two to run", j["total"] == 2)
    done = wait(j["batch_id"])
    check("  batch finishes", done["state"] == "finished" and done["finished_at"], str(done)[:200])
    check("  per-regulation outcomes", done["items"][str(a)]["state"] == "done" and done["items"][str(a)]["requirements_extracted"] == 3
          and done["items"][str(c)]["state"] == "failed" and done["items"][str(c)]["error"] == "boom", str(done["items"]))
    check("  counts", done["counts"] == {"queued": 0, "running": 0, "done": 1, "failed": 1}, str(done["counts"]))
    check("  runner called exactly for the two", sorted(calls) == sorted([a, c]), str(calls))
    check("  shares state with GET /regulation/{id}/analyze", client.get(f"/regulation/{a}/analyze").json()["state"] == "done")

    calls.clear()
    j = client.post("/analysis/trigger", json={"regulation_ids": [b], "force": True}).json()
    wait(j["batch_id"])
    check("force re-analyses an already-analysed regulation", calls == [b], str(calls))

    with api._analysis_lock:
        api._analysis_state[d] = {"state": "running", "started_at": "x"}
    j = client.post("/analysis/trigger", json={"regulation_ids": [d]}).json()
    check("a regulation already being analysed is skipped, not run twice", j["skipped"]["already_running"] == [d] and j["total"] == 0 and j["state"] == "finished")
    with api._analysis_lock:
        api._analysis_state.pop(d, None)

    check("empty list is 422", client.post("/analysis/trigger", json={"regulation_ids": []}).status_code == 422)
    check("unknown batch is 404", client.get("/analysis/batches/nope").status_code == 404)

    # ---- 2. by run ---------------------------------------------------------
    changes = {"new": [{"type": "new", "regulation_id": a, "version_id": None, "previous_version_id": None, "title": "n", "source_system": "s", "document_url": "u", "detail": {}}],
               "modified": [{"type": "modified", "regulation_id": c, "version_id": None, "previous_version_id": None, "title": "m", "source_system": "s", "document_url": "u", "detail": {}},
                            {"type": "modified", "regulation_id": d, "version_id": None, "previous_version_id": None, "title": "m2", "source_system": "s", "document_url": "u", "detail": {}}],
               "deleted": [{"type": "deleted", "regulation_id": b, "version_id": None, "previous_version_id": None, "title": "d", "source_system": "s", "document_url": "u", "detail": {}}]}
    run_id = run_store.save_run(repo, "zz_test_job", ["ZZ"], "finished", "2026-09-21T10:00:00", "2026-09-21T10:01:00", None,
                                {"classified": {}, "changes": changes})
    run_ids.append(run_id)
    ch = client.get(f"/runs/{run_id}/changes?type=modified").json()["changes"]
    rejected = next(c_["change_id"] for c_ in ch if c_["regulation_id"] == d)
    x("UPDATE run_changes SET decision='rejected', reviewed_by='t' WHERE change_id=?", rejected)

    calls.clear()
    r = client.post(f"/analysis/trigger/run/{run_id}")
    j = r.json()
    check("POST /analysis/trigger/run/{id} defaults to new + modified, minus rejected",
          r.status_code == 202 and j["run_id"] == run_id and j["requested"] == 2 and j["source"] == f"run:{run_id}", r.text[:250])
    wait(j["batch_id"])
    check("  analysed the new and the (non-rejected) modified one", sorted(calls) == sorted([a, c]), str(calls))

    calls.clear()
    j = client.post(f"/analysis/trigger/run/{run_id}?types=deleted&force=true").json()
    wait(j["batch_id"])
    check("  types=deleted picks the deleted document", calls == [b], str(calls))
    check("  include_rejected brings the rejected one back",
          client.post(f"/analysis/trigger/run/{run_id}?types=modified&include_rejected=true&force=true").json()["requested"] == 2)
    check("unknown run is 404", client.post("/analysis/trigger/run/99999999").status_code == 404)
    check("bad type is 422", client.post(f"/analysis/trigger/run/{run_id}?types=bogus").status_code == 422)
finally:
    api._run_requirement_activity_analysis_for_regulation = real_runner
    repo.get_requirements_for_regulation = real_reqs
    time.sleep(0.5)
    for rid in reg_ids:
        x("delete from regulations where id=?", rid)
    for rid in run_ids:
        x("delete from run_changes where run_id=?", rid)
        x("delete from run_results where run_id=?", rid)
    from apis.pipeline_api import _analysis_state
    for rid in reg_ids:
        _analysis_state.pop(rid, None)
    print("cleanup: scratch rows left =", repo._get_conn().cursor().execute(
        "select (select count(*) from regulations where regulator='ZZ TEST REGULATOR') + (select count(*) from run_results where job='zz_test_job')").fetchone()[0])

print(f"\n{len(FAILS)} failing" if FAILS else "\nall checks passed")
sys.exit(1 if FAILS else 0)
