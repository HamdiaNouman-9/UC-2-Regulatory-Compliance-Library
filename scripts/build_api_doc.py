"""Build the API reference (.docx) from REAL responses of a running server.

    python scripts/build_api_doc.py [--base http://127.0.0.1:8000] [--out docs/API_REFERENCE.docx]

Every response in the document was captured from the live API when this ran. Endpoints
that would cost LLM tokens, need a file upload, or delete data are listed in the appendix
instead of being called. Mutating endpoints (decisions, add/update regulation) are
exercised on SCRATCH rows (regulator "ZZ TEST REGULATOR") that are deleted afterwards.
Long strings and lists are shortened in the document and marked as such.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from docx import Document  # noqa: E402
from docx.enum.text import WD_BREAK  # noqa: E402
from docx.oxml import OxmlElement  # noqa: E402
from docx.oxml.ns import qn  # noqa: E402
from docx.shared import Pt, RGBColor  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--base", default="http://127.0.0.1:8000")
ap.add_argument("--out", default=str(ROOT / "docs" / "API_REFERENCE_LIVE.docx"))
ap.add_argument("--regulation-id", type=int, default=2644)
args = ap.parse_args()
BASE = args.base.rstrip("/")
REG = args.regulation_id


# --------------------------------------------------------------------------- #
#  calling the API                                                             #
# --------------------------------------------------------------------------- #

def http(method, path, query=None, body=None, timeout=120):
    url = BASE + path
    if query:
        from urllib.parse import urlencode
        url += "?" + urlencode(query, doseq=True)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"} if data else {})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        raw, status = e.read(), e.code
    dt = int((time.time() - t0) * 1000)
    try:
        parsed = json.loads(raw)
    except ValueError:
        parsed = raw.decode("utf-8", "replace")[:500]
    return status, dt, parsed


def shrink(v, depth=0, items=2, keys=5, chars=160):
    if isinstance(v, str):
        return v if len(v) <= chars else v[:chars] + f"… [{len(v) - chars} more characters]"
    if isinstance(v, list):
        out = [shrink(x, depth + 1, items, keys, chars) for x in v[:items]]
        if len(v) > items:
            out.append(f"… {len(v) - items} more item(s), {len(v)} in total")
        return out
    if isinstance(v, dict):
        ks = list(v)
        maplike = sum(isinstance(x, (dict, list)) for x in v.values()) > len(ks) / 2   # a lookup map, not a record
        if depth >= 1 and len(ks) > keys + 3 and maplike:
            out = {k: shrink(v[k], depth + 1, items, keys, chars) for k in ks[:keys]}
            out["…"] = f"{len(ks) - keys} more entries, {len(ks)} in total"
            return out
        return {k: shrink(x, depth + 1, items, keys, chars) for k, x in v.items()}
    return v


ENTRIES = []          # everything that goes into the document
SKIPPED = []          # (endpoint, reason) for the appendix


def rec(group, method, path, summary, *, path_vals=None, query=None, body=None, params_doc=None, body_doc=None,
        new=False, note=None, items=2, keys=5, transform=None, expect=(200, 202), label=None):
    """Call one endpoint for real and keep the request and response."""
    real_path = path
    for k, v in (path_vals or {}).items():
        real_path = real_path.replace("{" + k + "}", str(v))
    status, ms, resp = http(method, real_path, query, body)
    shown = transform(resp) if transform else resp
    e = {"group": group, "method": method, "path": path, "summary": summary, "status": status, "ms": ms,
         "path_vals": path_vals or {}, "query": query or {}, "body": body, "params_doc": params_doc or [],
         "body_doc": body_doc or [], "new": new, "note": note, "label": label,
         "response": shrink(shown, items=items, keys=keys), "ok": status in expect}
    ENTRIES.append(e)
    flag = "ok  " if e["ok"] else "FAIL"
    print(f"{flag} {status} {ms:>6}ms  {method} {real_path}")
    return resp


def P(name, where, typ, desc, example=None, required=False):
    return {"name": name, "in": where, "type": typ, "desc": desc, "example": example, "required": required}


REG_ID = P("regulation_id", "path", "integer", "Id of the regulation.", REG, True)
LANG = P("lang", "query", "string", "Response language: en (default) or ar.", "en")


# --------------------------------------------------------------------------- #
#  database access for scratch data                                            #
# --------------------------------------------------------------------------- #

def open_repo():
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    import truststore
    truststore.inject_into_ssl()
    from storage.mssql_repo import MSSQLRepository
    return MSSQLRepository({"server": os.getenv("MSSQL_SERVER"), "database": os.getenv("MSSQL_DATABASE"),
                            "username": os.getenv("MSSQL_USERNAME"), "password": os.getenv("MSSQL_PASSWORD"),
                            "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}")})


def sql(repo, q, *a, fetch=False):
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute(q, *a)
        out = cur.fetchall() if fetch else None
        conn.commit()
    return out


# =========================================================================== #
#  1. health + trigger + monitoring                                            #
# =========================================================================== #
G_HEALTH, G_TRIG, G_RUNS, G_SCHED, G_LLM, G_ANALYSIS, G_LIB, G_WRITE = (
    "Health", "Trigger and monitoring", "Runs, review and alerts", "Scheduler and metrics", "LLM settings and usage",
    "Requirement and activity analysis", "Library (read)", "Library (write, on scratch rows)")

rec(G_HEALTH, "GET", "/health", "Liveness check.")
rec(G_HEALTH, "GET", "/", "Root; confirms the API is up.")

runs_before = rec(G_RUNS, "GET", "/runs", "Stored job runs, newest first. Each row carries change counts (total and pending review).",
                  query={"limit": 3}, new=True, items=2,
                  params_doc=[P("regulator", "query", "string", "Only runs that included this regulator key, e.g. CBB."),
                              P("job", "query", "string", "Only runs of this job, e.g. monitor_cbb."),
                              P("state", "query", "string", "finished | failed."),
                              P("alert", "query", "string", "none | info | warning | critical."),
                              P("limit", "query", "integer", "Rows per page (1-200, default 50).", 3),
                              P("offset", "query", "integer", "Rows to skip (default 0).")])

rec(G_TRIG, "GET", "/trigger/regulators", "Every regulator the API can trigger, the job behind it, and whether it is active, blocked or unwired. "
    "Snapshot-based regulators (SIMAH, Saudi Exchange) also report snapshot age and whether a live visit is allowed.", items=3, keys=4)

r = rec(G_TRIG, "POST", "/trigger/regulators", "Start the monitoring pipeline for one or more regulators. Returns 202 at once; the job runs in the background. "
        "Accepts keys, full names or acronyms, case-insensitive. Regulators that share a job start it once.",
        body={"regulators": ["CBB"]}, new=False, expect=(202,),
        body_doc=[P("regulators", "body", "array of string", "Regulator keys or names, e.g. [\"CBB\", \"sama\", \"Central Bank of Egypt (CBE)\"].", None, True)])
time.sleep(6)


def only_started(resp):
    if isinstance(resp, dict) and isinstance(resp.get("jobs"), dict):
        resp = dict(resp)
        resp["jobs"] = {k: v for k, v in resp["jobs"].items() if v.get("state") != "never_started"}
        resp["note_for_this_document"] = "jobs that have never started in this server process are omitted here"
    return resp


rec(G_TRIG, "GET", "/trigger/regulators/status", "Current in-memory state of every job reachable from POST /trigger/regulators (running, finished, failed, never_started) "
    "with the finished job's own report. Shared with /trigger/monitor; lost on restart.", transform=only_started, keys=8)

rec(G_TRIG, "POST", "/trigger/monitor/{job}", "Start one monitoring job by name. Returns when it has started, not finished.",
    path_vals={"job": "monitor_cbb"}, expect=(200, 202),
    params_doc=[P("job", "path", "string", "Job name from GET /trigger/monitor, e.g. monitor_cbb.", "monitor_cbb", True)])
time.sleep(6)
rec(G_TRIG, "GET", "/trigger/monitor", "Every monitoring job this API can start, and its state in this process.", keys=6)
rec(G_TRIG, "GET", "/trigger/monitor/{job}", "What this API process last saw of one job, including the report of a finished run.",
    path_vals={"job": "monitor_cbb"}, params_doc=[P("job", "path", "string", "Job name.", "monitor_cbb", True)])
rec(G_TRIG, "GET", "/monitoring/staleness", "Regulators with no update (new, modified or withdrawn document) for longer than their interval in config/staleness.yml.", items=3)

# ---- runs: a real run first ------------------------------------------------
real_run = None
if isinstance(runs_before, dict) and runs_before.get("runs"):
    real_run = runs_before["runs"][0]["run_id"]
if real_run:
    rec(G_RUNS, "GET", "/runs/{run_id}", "One stored run: state, timing, error, alerts, the run's own report and change counts.",
        path_vals={"run_id": real_run}, new=True, params_doc=[P("run_id", "path", "integer", "Run id from GET /runs.", real_run, True)])

# =========================================================================== #
#  2. scratch data: runs with changes, decisions, alerts, writes               #
# =========================================================================== #
repo = open_repo()
from storage import run_store  # noqa: E402

scratch_regs, scratch_runs = [], []


def scratch_reg(title, html):
    r = sql(repo, "INSERT INTO regulations (title, regulator, source_system, category, document_url, document_html, content_hash, status) "
                  "OUTPUT INSERTED.id VALUES (?, 'ZZ TEST REGULATOR', 'ZZ-TEST', 'ZZ', ?, ?, ?, '')",
            title, f"https://example.invalid/{title}", html, f"h-{title}", fetch=True)
    scratch_regs.append(int(r[0][0]))
    return int(r[0][0])


def scratch_ver(rid, html, h, status):
    r = sql(repo, "INSERT INTO regulation_versions (regulation_id, regulator, content_html, content_text, content_hash, updated_date, "
                  "change_summary, status) OUTPUT INSERTED.version_id VALUES (?, 'ZZ TEST REGULATOR', ?, ?, ?, CAST(GETDATE() AS DATE), 'sample', ?)",
            rid, html, html, h, status, fetch=True)
    return int(r[0][0])


try:
    r_new = scratch_reg("Sample new document", "<p>brand new</p>")
    r_mod = scratch_reg("Sample modified document", "<p>version two</p>")
    r_del = scratch_reg("Sample removed document", "<p>going away</p>")
    v_new = scratch_ver(r_new, "<p>brand new</p>", "n1", "active")
    v_old = scratch_ver(r_mod, "<p>version one</p>", "m1", "inactive")
    v_cur = scratch_ver(r_mod, "", "m2", "active")
    scratch_ver(r_del, "<p>going away</p>", "d1", "active")
    ch = lambda t, rid, vid, pv, title, d: {"type": t, "regulation_id": rid, "version_id": vid, "previous_version_id": pv,
                                            "title": title, "source_system": "ZZ-TEST", "document_url": f"https://example.invalid/{title}", "detail": d}
    report = {"regulator": "ZZ TEST REGULATOR", "crawled": 3, "classified": {"new": 1, "modified": 1, "unchanged": 0, "disappeared": 1},
              "processed": 2, "run_trustworthy": True, "baseline_verdict": "PASS", "gate_problems": [],
              "withdrawals": {"counts": {"withdrawal-proposed": 1, "watching": 0, "not-judged": 0}},
              "changes": {"new": [ch("new", r_new, v_new, None, "Sample new document", {})],
                          "modified": [ch("modified", r_mod, v_cur, v_old, "Sample modified document", {"old_chars": 12, "new_chars": 0})],
                          "deleted": [ch("deleted", r_del, None, None, "Sample removed document", {"withdrawal": "withdrawal-proposed"})]}}
    sample_run = run_store.save_run(repo, "zz_sample_job", ["ZZ"], "finished", "2026-09-21T10:00:00", "2026-09-21T10:05:00", None, report)
    failed_run = run_store.save_run(repo, "zz_sample_job", ["ZZ"], "failed", "2026-09-21T11:00:00", "2026-09-21T11:00:03", "RuntimeError: sample failure", None)
    scratch_runs += [sample_run, failed_run]
    SAMPLE_NOTE = ("Sample data: this run was created for the document so every change type is shown. "
                   "Real runs of CBB and CBE contain the same fields.")

    rec(G_RUNS, "GET", "/runs/{run_id}", "One stored run (sample run with all three change types and the alerts they raise).", path_vals={"run_id": sample_run},
        new=True, note=SAMPLE_NOTE, label="sample run", params_doc=[P("run_id", "path", "integer", "Run id.", sample_run, True)])
    changes = rec(G_RUNS, "GET", "/runs/{run_id}/changes", "The new, modified and deleted documents of a run. `new` rows carry no `updated_at`; "
                  "`modified` and `deleted` rows include it. `detail` holds run-specific facts (for modified: old/new text length; for deleted: the withdrawal verdict).",
                  path_vals={"run_id": sample_run}, new=True, note=SAMPLE_NOTE, items=3,
                  params_doc=[P("run_id", "path", "integer", "Run id.", sample_run, True),
                              P("type", "query", "string", "new | modified | deleted."),
                              P("decision", "query", "string", "pending | accepted | rejected."),
                              P("limit", "query", "integer", "Rows per page (1-500, default 100)."),
                              P("offset", "query", "integer", "Rows to skip.")])
    cid = {c["type"]: c["change_id"] for c in changes["changes"]}
    dbody_doc = [P("decision", "body", "string", "accepted | rejected.", None, True),
                 P("reviewed_by", "body", "string", "Who is deciding; stored with the decision.", None, True),
                 P("note", "body", "string", "Optional comment, up to 500 characters.")]
    rec(G_RUNS, "POST", "/runs/{run_id}/changes/{change_id}/decision",
        "Accept or reject ONE change. new: accepted sets regulations.status to active, rejected to reject. modified: accepted sets active, "
        "rejected restores the previous version. deleted: accepted sets withdrawn, rejected changes nothing. Deciding the same way twice is a no-op; the opposite way returns 409.",
        path_vals={"run_id": sample_run, "change_id": cid["new"]}, body={"decision": "accepted", "reviewed_by": "reviewer@example.com", "note": "looks right"},
        new=True, note=SAMPLE_NOTE, body_doc=dbody_doc,
        params_doc=[P("run_id", "path", "integer", "Run id.", sample_run, True), P("change_id", "path", "integer", "Change id from GET /runs/{id}/changes.", cid["new"], True)])
    rec(G_RUNS, "POST", "/runs/{run_id}/changes/{change_id}/decision", "Same call rejecting a MODIFIED change: the previous version becomes the active one again.",
        path_vals={"run_id": sample_run, "change_id": cid["modified"]}, body={"decision": "rejected", "reviewed_by": "reviewer@example.com"},
        new=True, note=SAMPLE_NOTE, body_doc=dbody_doc, label="reject a modified change",
        params_doc=[P("run_id", "path", "integer", "Run id.", sample_run, True), P("change_id", "path", "integer", "Change id.", cid["modified"], True)])
    rec(G_RUNS, "POST", "/runs/{run_id}/decisions", "Decide several changes at once. Each is decided on its own; one that cannot be (for example it was already decided the other way) "
        "is reported in `failed` and does not stop the others.",
        path_vals={"run_id": sample_run}, body={"decision": "accepted", "reviewed_by": "reviewer@example.com", "change_ids": [cid["deleted"], 999999999]},
        new=True, note=SAMPLE_NOTE,
        body_doc=dbody_doc + [P("change_ids", "body", "array of integer", "Changes to decide (1-500).", None, True)],
        params_doc=[P("run_id", "path", "integer", "Run id.", sample_run, True)])
    rec(G_RUNS, "GET", "/runs/{run_id}/changes", "Filtering by decision: what has been accepted so far.", path_vals={"run_id": sample_run},
        query={"decision": "accepted"}, new=True, note=SAMPLE_NOTE, items=3, label="decision=accepted",
        params_doc=[P("run_id", "path", "integer", "Run id.", sample_run, True), P("decision", "query", "string", "pending | accepted | rejected.", "accepted")])
    rec(G_RUNS, "GET", "/alerts", "Runs that raised a warning or critical alert, newest first (info-level items are not listed). "
        "Alerts: job_failed (critical); run_untrusted, baseline_not_passed, feed_unavailable, run_skipped, empty_content_versions (warning). "
        "If ALERT_WEBHOOK_URL is set, warning and critical alerts are also POSTed there.", query={"limit": 5}, new=True, note=SAMPLE_NOTE, items=3,
        params_doc=[P("level", "query", "string", "warning | critical (default both)."), P("since", "query", "string", "ISO date or time."),
                    P("limit", "query", "integer", "Rows (1-200, default 50).", 5)])

    # ---- writes on a scratch regulation --------------------------------------
    added = rec(G_WRITE, "POST", "/regulations/add", "Create a regulation row. `run_analysis` (default false in this call) would also start the LLM analysis.",
                body={"title": "Sample regulation created through the API", "regulator": "ZZ TEST REGULATOR", "source_system": "ZZ-TEST", "category": "ZZ",
                      "document_url": "https://example.invalid/sample", "document_html": "<p>sample text</p>", "run_analysis": False},
                expect=(200, 201), note="Scratch row, deleted after this document was generated.",
                body_doc=[P("title", "body", "string", "Title.", None, True), P("regulator", "body", "string", "Regulator name.", None, True),
                          P("source_system", "body", "string", "Source label."), P("category", "body", "string", "Category label."),
                          P("compliancecategory_id", "body", "integer", "Folder id in the category tree."), P("reference_no", "body", "string", "Reference number."),
                          P("published_date", "body", "string", "Published date."), P("document_url", "body", "string", "Link to the document."),
                          P("document_html", "body", "string", "HTML content."), P("document_text", "body", "string", "Plain text."),
                          P("status", "body", "string", "Review status."), P("run_analysis", "body", "boolean", "Also run analysis after inserting.")],
                params_doc=[LANG])
    new_id = None
    if isinstance(added, dict):
        new_id = added.get("regulation_id") or added.get("id") or (added.get("data") or {}).get("id")
    if new_id:
        scratch_regs.append(int(new_id))
        rec(G_WRITE, "PUT", "/regulations/{regulation_id}", "Update fields of a regulation. Only the fields sent are changed.", path_vals={"regulation_id": new_id},
            body={"title": "Sample regulation (renamed)", "reference_no": "ZZ-001"}, note="Scratch row.",
            params_doc=[P("regulation_id", "path", "integer", "Id of the regulation.", new_id, True), LANG],
            body_doc=[P("title", "body", "string", "New title."), P("reference_no", "body", "string", "New reference number."),
                      P("(other fields)", "body", "", "category, compliancecategory_id, department, published_date, year, document_url, source_page_url, document_html, document_text, status.")])
        rec(G_WRITE, "POST", "/update-status/regulations", "Set the review status of a regulation (for example active, reject).", body={"record_id": new_id, "status": "active"},
            note="Scratch row.", body_doc=[P("record_id", "body", "integer", "Regulation id.", None, True), P("status", "body", "string", "New status.", None, True)])
        vid = scratch_ver(int(new_id), "<p>sample text</p>", "v1", "active")
        rec(G_WRITE, "PATCH", "/regulation/{regulation_id}/versions/{version_id}/status", "Set the status of one content version.",
            path_vals={"regulation_id": new_id, "version_id": vid}, body={"status": "inactive"}, note="Scratch row.",
            params_doc=[P("regulation_id", "path", "integer", "Regulation id.", new_id, True), P("version_id", "path", "integer", "Version id.", vid, True)],
            body_doc=[P("status", "body", "string", "New status, e.g. active or inactive.", None, True)])
finally:
    pass

# =========================================================================== #
#  3. scheduler, metrics, LLM                                                  #
# =========================================================================== #
rec(G_SCHED, "GET", "/scheduler/jobs", "The schedule in config/scheduler.yml with each job's next firing time and last stored run. "
    "The scheduler itself is a separate process (python scheduler/scheduler.py); this endpoint reads its configuration and cannot tell whether that process is running.",
    new=True, items=3)
rec(G_SCHED, "GET", "/metrics", "In-process request counts and latency per route (reset on restart), job history from stored runs, pending reviews, and table sizes. "
    "LLM token usage is on GET /llm/usage.", new=True, keys=4)
rec(G_LLM, "GET", "/llm/usage", "Token usage: `uc` is this application's own analysis calls (group_by model, day, step or regulation); `openrouter` is the provider's view.",
    query={"group_by": "model"}, params_doc=[P("group_by", "query", "string", "model | day | step | regulation.", "model"), P("since", "query", "string", "ISO date."),
                                             P("until", "query", "string", "ISO date, exclusive.")], new=True)
current_model = http("GET", "/llm/settings")[2].get("model") if True else None
rec(G_LLM, "GET", "/llm/settings", "The model used by analyses that start from now.", new=True)
rec(G_LLM, "PUT", "/llm/settings", "Choose the model for analyses that START after this call. Must be an id from GET /llm/models. (Shown here re-setting the current model.)",
    body={"model": current_model}, new=True, body_doc=[P("model", "body", "string", "Model id from /llm/models.", None, True)])
rec(G_LLM, "GET", "/llm/models", "OpenRouter's model catalogue (cached for an hour). Prices are USD per token.", query={"search": "deepseek"}, new=True, items=3,
    params_doc=[P("search", "query", "string", "Filter by id or name.", "deepseek"), P("refresh", "query", "boolean", "Bypass the cache.")])

# =========================================================================== #
#  4. analysis                                                                 #
# =========================================================================== #
b1 = rec(G_ANALYSIS, "POST", "/analysis/trigger", "Start requirement and activity analysis for a list of regulations. Returns 202 with a batch id. Regulations that already have "
         "active requirements are skipped unless `force` is true (shown here: regulation %d is already analysed, so nothing is run and no LLM call is made)." % REG,
         body={"regulation_ids": [REG], "force": False}, new=True, expect=(202,),
         body_doc=[P("regulation_ids", "body", "array of integer", "Regulations to analyse (1-500).", None, True),
                   P("force", "body", "boolean", "Also re-analyse ones that already have requirements (default false).")])
bid = b1.get("batch_id") if isinstance(b1, dict) else None
if bid:
    rec(G_ANALYSIS, "GET", "/analysis/batches/{batch_id}", "Progress of a batch: counts per state and each regulation's outcome. In memory; the last 50 batches are kept.",
        path_vals={"batch_id": bid}, new=True, params_doc=[P("batch_id", "path", "string", "Batch id from a trigger response.", bid, True)])
if real_run:
    rec(G_ANALYSIS, "POST", "/analysis/trigger/run/{run_id}", "Start analysis for the documents a stored run found. Defaults to its new and modified documents, leaving out "
        "changes a reviewer rejected. (This run found no changes, so nothing starts.)", path_vals={"run_id": real_run}, new=True, expect=(202,),
        params_doc=[P("run_id", "path", "integer", "Run id.", real_run, True),
                    P("types", "query", "array of string", "new | modified | deleted (default new and modified)."),
                    P("force", "query", "boolean", "Re-analyse regulations that already have requirements."),
                    P("include_rejected", "query", "boolean", "Include changes a reviewer rejected.")])
rec(G_ANALYSIS, "GET", "/regulation/{regulation_id}/analyze", "What this API process last saw of one regulation's analysis run (POST on the same path starts one). In memory.",
    path_vals={"regulation_id": REG}, params_doc=[REG_ID])

# =========================================================================== #
#  5. library reads                                                            #
# =========================================================================== #
rec(G_LIB, "GET", "/regulations", "Regulations grouped by country. Always pass `limit`: without it every matching regulation is returned in one response (17 to 44 seconds).",
    query={"limit": 2, "regulator": "Saudi Arabian Monetary Authority (SAMA)"}, items=2, keys=5,
    params_doc=[P("regulator", "query", "string", "Regulator name; may be repeated.", "Saudi Arabian Monetary Authority (SAMA)"), P("country", "query", "string", "One country."),
                P("limit", "query", "integer", "Rows to return.", 2), P("offset", "query", "integer", "Rows to skip."), LANG])
rec(G_LIB, "GET", "/regulation/{regulation_id}", "One regulation's details.", path_vals={"regulation_id": REG}, params_doc=[REG_ID, LANG])
rec(G_LIB, "GET", "/regulation/{regulation_id}/versions", "Content version history of a regulation.", path_vals={"regulation_id": REG},
    params_doc=[REG_ID, P("include_details", "query", "boolean", "Include the stored content of each version."), LANG])
rec(G_LIB, "GET", "/regulation/{regulation_id}/versions/active", "The currently active content version.", path_vals={"regulation_id": REG}, params_doc=[REG_ID, LANG])
rec(G_LIB, "GET", "/regulation/{regulation_id}/requirements", "Every requirement for one regulation with its activities nested under it.", path_vals={"regulation_id": REG},
    params_doc=[REG_ID, P("active_only", "query", "boolean", "True (default) returns what is in force now; False also returns superseded spans.", True)], items=2)
rec(G_LIB, "GET", "/regulation/{regulation_id}/activities", "Every activity under every requirement of one regulation, flat; each row keeps requirement_ref_key.",
    path_vals={"regulation_id": REG}, params_doc=[REG_ID, P("active_only", "query", "boolean", "True (default) returns what is in force now.", True)], items=2)
rc = rec(G_LIB, "GET", "/categories/roots", "Top-level folders of the category tree (countries), without children.", params_doc=[LANG], items=2)
root_id = None
if isinstance(rc, dict) and rc.get("data"):
    root_id = rc["data"][0].get("compliancecategory_id")
rec(G_LIB, "GET", "/categories/root", "Top-level folders with their children nested.", params_doc=[LANG], items=1, keys=4)
if root_id:
    rec(G_LIB, "GET", "/categories/children/{parent_id}", "Direct children of one folder.", path_vals={"parent_id": root_id},
        params_doc=[P("parent_id", "path", "integer", "Folder id.", root_id, True), LANG], items=2)
    rec(G_LIB, "GET", "/regulations/by-category/{category_id}", "Regulations stored in one folder.", path_vals={"category_id": root_id},
        params_doc=[P("category_id", "path", "integer", "Folder id.", root_id, True), LANG], items=2)
rec(G_LIB, "GET", "/status/{regulator}", "Legacy pipeline status for SBP, SECP or SAMA only.", path_vals={"regulator": "SAMA"},
    params_doc=[P("regulator", "path", "string", "SBP | SECP | SAMA.", "SAMA", True), LANG])

# =========================================================================== #
#  cleanup of scratch data                                                     #
# =========================================================================== #
for rid in scratch_regs:
    sql(repo, "DELETE FROM regulation_versions WHERE regulation_id=?", rid)
    sql(repo, "DELETE FROM regulations WHERE id=?", rid)
for rid in scratch_runs:
    sql(repo, "DELETE FROM run_changes WHERE run_id=?", rid)
    sql(repo, "DELETE FROM run_results WHERE run_id=?", rid)
left = sql(repo, "SELECT (SELECT COUNT(*) FROM regulations WHERE regulator='ZZ TEST REGULATOR') + (SELECT COUNT(*) FROM run_results WHERE job='zz_sample_job')", fetch=True)[0][0]
print("scratch rows left after cleanup:", left)

SKIPPED += [
    ("POST /regulation/{regulation_id}/analyze", "Starts a real LLM analysis (tokens and minutes). POST /analysis/trigger does the same job for one or many regulations."),
    ("POST /analyze/document, POST /upload-regulation, POST /gap-analysis/single|multi|multi-docs, POST /trigger/batch-analysis", "File upload and LLM cost; not exercised."),
    ("GET /gap-analysis/session/{session_id}", "Needs a stored gap-analysis session."),
    ("GET /requirement-mapping|control-mapping|kpi-mapping/{regulation_id}", "Answer 200 with only {\"detail\": \"No ... found\"} when a regulation has no mappings (checked on %d); a 404 would be the expected status." % REG),
    ("GET /compliance-analysis/{id}, /compliance-analysis-v2/{id}, .../executive-summary, .../requirement/{rid}; GET /regulation/{id}/analysis-versions[/{version_id}]; PATCH .../analysis-versions/{version_id}/status",
     "Older analysis tables (compliance_analysis). That table does not exist in this database, so these return empty results or errors; use the requirement/activity endpoints."),
    ("POST /trigger/staged-analysis|full-analysis|requirement-matching/{id}, POST /trigger/analysis-for-version/{id}/{version_id}, /demo/*, /test/cbb/*", "Older staged-analysis triggers writing compliance_analysis; GET /test/cbb/regulations returns 500 because that table is missing. Replaced by POST /analysis/trigger."),
    ("DELETE /admin/analysis/{id}, DELETE /admin/ar-cache[/{id}], POST /admin/fetch-and-analyze/{id}", "Delete data or run analysis; not exercised."),
    ("POST /trigger/full, POST /trigger/{regulator}", "Run the older full SBP/SECP/SAMA pipelines synchronously; not exercised."),
    ("POST /schedule, POST /update-schedule", "/schedule writes to a table read by an older loop; /update-schedule does nothing and returns success. Real schedules are in config/scheduler.yml (GET /scheduler/jobs)."),
    ("POST /update-status/compliancecategory, POST /update-status/compliance-analysis", "Same request shape as /update-status/regulations; not exercised."),
    ("GET /status/full, GET /categories", "Legacy status list (SBP, SECP, SAMA only) and the full category tree (27 to 36 seconds); not shown."),
]

# =========================================================================== #
#  build the document                                                          #
# =========================================================================== #
doc = Document()
st = doc.styles["Normal"]
st.font.name = "Calibri"
st.font.size = Pt(10)
for s in doc.sections:
    s.left_margin = s.right_margin = Pt(54)
    s.top_margin = s.bottom_margin = Pt(54)

MONO = "Courier New"
BLUE, GREY, GREEN, RED = RGBColor(0x1F, 0x38, 0x64), RGBColor(0x59, 0x59, 0x59), RGBColor(0x2E, 0x7D, 0x32), RGBColor(0xC6, 0x28, 0x28)


def shade(cell, hex_fill):
    tcPr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_fill)
    tcPr.append(shd)


def borders(table, color="BFBFBF"):
    tbl = table._tbl
    pr = tbl.tblPr
    b = OxmlElement("w:tblBorders")
    for edge in ("top", "left", "bottom", "right", "insideH", "insideV"):
        e = OxmlElement(f"w:{edge}")
        e.set(qn("w:val"), "single")
        e.set(qn("w:sz"), "4")
        e.set(qn("w:color"), color)
        b.append(e)
    pr.append(b)


def run(p, text, *, bold=False, italic=False, size=None, color=None, font=None):
    r = p.add_run(text)
    r.bold, r.italic = bold, italic
    if size:
        r.font.size = Pt(size)
    if color:
        r.font.color.rgb = color
    if font:
        r.font.name = font
        r._element.rPr.rFonts.set(qn("w:eastAsia"), font)
    return r


def code_block(text):
    t = doc.add_table(rows=1, cols=1)
    borders(t, "D9D9D9")
    c = t.rows[0].cells[0]
    shade(c, "F5F7FA")
    c.paragraphs[0].text = ""
    lines = text.split("\n")
    p = c.paragraphs[0]
    for i, line in enumerate(lines):
        if i:
            p = c.add_paragraph()
        p.paragraph_format.space_after = Pt(0)
        p.paragraph_format.space_before = Pt(0)
        run(p, line if line else " ", size=7.5, font=MONO)
    doc.add_paragraph().paragraph_format.space_after = Pt(2)


def pj(v):
    return json.dumps(v, indent=2, ensure_ascii=False)


# ---- cover -------------------------------------------------------------------
t = doc.add_paragraph()
run(t, "Regulatory Pipeline API", bold=True, size=24, color=BLUE)
t2 = doc.add_paragraph()
run(t2, "Live response reference", size=14, color=GREY)
doc.add_paragraph()
info = doc.add_table(rows=0, cols=2)
borders(info)
for k, v in [("Generated", datetime.now().strftime("%Y-%m-%d %H:%M")), ("Base URL", BASE),
             ("Authentication", "None. The API allows any origin; put it behind your own authentication before exposing it."),
             ("Sample regulation", f"{REG} (SAMA, Credit Information Law) is used wherever a regulation id is needed."),
             ("Where the responses come from", "Every response below was captured from this running server when the document was generated."),
             ("Shortened output", "Long strings and lists are cut for readability and marked with … (for example \"… 57 more item(s)\"). Field names and structure are exactly as returned."),
             ("NEW", "Endpoints added recently (runs, review, alerts, scheduler, metrics, analysis triggers, LLM usage)."),
             ("Sample data", "Where a call changes data or needs a run with changes, it was run on scratch rows that were deleted afterwards; those entries say so.")]:
    row = info.add_row().cells
    shade(row[0], "DCE6F1")
    run(row[0].paragraphs[0], k, bold=True)
    row[1].paragraphs[0].text = v
doc.add_paragraph()
p = doc.add_paragraph()
run(p, "Typical flow", bold=True, size=12, color=BLUE)
for line in ["1.  POST /trigger/regulators  {\"regulators\": [\"CBB\"]}   ->  202, the job starts in the background.",
             "2.  GET /trigger/regulators/status   ->  poll until the job's state is finished (or failed).",
             "3.  GET /runs?job=<job>&limit=1   ->  the stored run, with change counts and alerts.",
             "4.  GET /runs/{run_id}/changes   ->  the new, modified and deleted documents to review.",
             "5.  POST /runs/{run_id}/decisions   ->  accept or reject them.",
             "6.  POST /analysis/trigger/run/{run_id}   ->  analyse what the run found; poll GET /analysis/batches/{batch_id}.",
             "7.  GET /alerts   ->  anything that needs attention."]:
    q = doc.add_paragraph()
    q.paragraph_format.space_after = Pt(1)
    run(q, line, size=9, font=MONO)

# ---- contents ------------------------------------------------------------------
doc.add_paragraph()
p = doc.add_paragraph()
run(p, "Contents", bold=True, size=12, color=BLUE)
toc = doc.add_table(rows=1, cols=3)
borders(toc)
for i, h in enumerate(["Method", "Path", "Group"]):
    shade(toc.rows[0].cells[i], "DCE6F1")
    run(toc.rows[0].cells[i].paragraphs[0], h, bold=True)
for e in ENTRIES:
    r = toc.add_row().cells
    run(r[0].paragraphs[0], e["method"], bold=True, font=MONO, size=8.5)
    run(r[1].paragraphs[0], e["path"] + (f"  [{e['label']}]" if e["label"] else ""), font=MONO, size=8.5)
    run(r[2].paragraphs[0], e["group"] + ("  NEW" if e["new"] else ""), size=8.5, color=GREEN if e["new"] else None)

# ---- endpoints -------------------------------------------------------------------
last_group = None
for e in ENTRIES:
    if e["group"] != last_group:
        doc.add_paragraph().add_run().add_break(WD_BREAK.PAGE)
        h = doc.add_paragraph()
        run(h, e["group"], bold=True, size=16, color=BLUE)
        last_group = e["group"]
    else:
        doc.add_paragraph()
    head = doc.add_paragraph()
    head.paragraph_format.keep_with_next = True
    run(head, f'{e["method"]} {e["path"]}', bold=True, size=12, color=BLUE, font=MONO)
    if e["new"]:
        run(head, "   NEW", bold=True, size=8, color=GREEN)
    if e["label"]:
        run(head, f'   ({e["label"]})', italic=True, size=9, color=GREY)
    s = doc.add_paragraph()
    run(s, e["summary"], size=9.5, color=GREY)
    if e["note"]:
        n = doc.add_paragraph()
        run(n, e["note"], italic=True, size=8.5, color=RED)

    if e["params_doc"]:
        pt = doc.add_table(rows=1, cols=2)
        borders(pt)
        shade(pt.rows[0].cells[0], "DCE6F1")
        shade(pt.rows[0].cells[1], "DCE6F1")
        run(pt.rows[0].cells[0].paragraphs[0], "Parameters", bold=True)
        run(pt.rows[0].cells[1].paragraphs[0], "Description", bold=True)
        for pr in e["params_doc"]:
            c = pt.add_row().cells
            run(c[0].paragraphs[0], pr["name"], bold=True, font=MONO, size=9)
            if pr["required"]:
                run(c[0].paragraphs[0], " * required", size=7.5, color=RED)
            q = c[0].add_paragraph()
            run(q, f'{pr["type"]}  ({pr["in"]})', italic=True, size=8, color=GREY)
            run(c[1].paragraphs[0], pr["desc"], size=9)
            ex = pr["example"]
            if ex is None and pr["name"] in e["path_vals"]:
                ex = e["path_vals"][pr["name"]]
            if ex is None and pr["name"] in e["query"]:
                ex = e["query"][pr["name"]]
            if ex is not None:
                q = c[1].add_paragraph()
                run(q, "Used in this example: ", size=8, color=GREY)
                run(q, str(ex), size=8.5, font=MONO)
        doc.add_paragraph().paragraph_format.space_after = Pt(0)

    if e["body"] is not None:
        b = doc.add_paragraph()
        run(b, "Request body", bold=True, size=10)
        if e["body_doc"]:
            bt = doc.add_table(rows=1, cols=2)
            borders(bt)
            shade(bt.rows[0].cells[0], "DCE6F1")
            shade(bt.rows[0].cells[1], "DCE6F1")
            run(bt.rows[0].cells[0].paragraphs[0], "Field", bold=True)
            run(bt.rows[0].cells[1].paragraphs[0], "Description", bold=True)
            for pr in e["body_doc"]:
                c = bt.add_row().cells
                run(c[0].paragraphs[0], pr["name"], bold=True, font=MONO, size=9)
                if pr["required"]:
                    run(c[0].paragraphs[0], " * required", size=7.5, color=RED)
                q = c[0].add_paragraph()
                run(q, pr["type"], italic=True, size=8, color=GREY)
                run(c[1].paragraphs[0], pr["desc"], size=9)
            doc.add_paragraph().paragraph_format.space_after = Pt(0)
        code_block(pj(e["body"]))

    rp = doc.add_paragraph()
    run(rp, "Response  ", bold=True, size=10)
    run(rp, str(e["status"]), bold=True, size=10, color=GREEN if e["ok"] else RED)
    run(rp, f'   ({e["ms"]} ms)', size=8.5, color=GREY)
    code_block(pj(e["response"]) if not isinstance(e["response"], str) else e["response"])

# ---- appendix -----------------------------------------------------------------------
doc.add_paragraph().add_run().add_break(WD_BREAK.PAGE)
h = doc.add_paragraph()
run(h, "Appendix: endpoints not shown, and why", bold=True, size=16, color=BLUE)
at = doc.add_table(rows=1, cols=2)
borders(at)
for i, hh in enumerate(["Endpoint(s)", "Reason"]):
    shade(at.rows[0].cells[i], "DCE6F1")
    run(at.rows[0].cells[i].paragraphs[0], hh, bold=True)
for ep, why in SKIPPED:
    c = at.add_row().cells
    run(c[0].paragraphs[0], ep, font=MONO, size=8)
    run(c[1].paragraphs[0], why, size=9)

Path(args.out).parent.mkdir(parents=True, exist_ok=True)
doc.save(args.out)
bad = [e for e in ENTRIES if not e["ok"]]
print(f"\nwrote {args.out}: {len(ENTRIES)} endpoint calls documented, {len(bad)} not OK")
for e in bad:
    print("  NOT OK:", e["method"], e["path"], e["status"])
