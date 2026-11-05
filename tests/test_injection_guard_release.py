"""CHG-008 on the API side: a held row is refused by the re-analysis runner
and released by the reviewer's decision, which starts the analysis it was
held from. Stand-in repositories and cursors throughout; no database.

    .venv/Scripts/python.exe -m pytest tests/test_injection_guard_release.py -v
"""
from __future__ import annotations

import json

import pytest

from storage import run_store


# ---------------------------------------------------------------- stand-ins

class Cursor:
    """Records every SQL statement; answers fetchone from a queue."""
    def __init__(self, answers):
        self.sql = []
        self.answers = list(answers)

    def execute(self, q, *params):
        self.sql.append((" ".join(q.split()), params))

    def fetchone(self):
        return self.answers.pop(0) if self.answers else None


class Conn:
    def __init__(self, cur):
        self.cur = cur
        self.committed = False

    def cursor(self):
        return self.cur

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class Repo:
    def __init__(self, cur):
        self.conn = Conn(cur)

    def _get_conn(self):
        return self.conn


GUARD = {"reasons": ["instruction_phrase"], "hits": [{"source": "page", "pattern": "p", "match": "m", "context": "c"}],
         "hidden": [], "sources_scanned": ["page"], "mode": "enforce", "at": "2026-11-05T10:00:00+00:00"}


def _decide(change_row, status, extra_meta, decision, reviewed_by="abeer"):
    # fetchone order inside decide(): the change row, then (status, extra_meta)
    cur = Cursor([change_row, (status, json.dumps(extra_meta))])
    repo = Repo(cur)
    out = run_store.decide(repo, 5, 11, decision, reviewed_by)
    return out, cur, repo


def _updates(cur, table):
    return [(q, p) for q, p in cur.sql if q.startswith(f"UPDATE {table}")]


# ------------------------------------------------------------ run_store

def test_accepting_a_held_new_row_releases_it_and_records_who():
    out, cur, repo = _decide(("new", 42, 7, None, "pending"), "quarantine",
                             {"content_text": "x", "injection_guard": GUARD}, "accepted")
    assert out["was_quarantined"] is True and out["released"] is True
    assert out["applied"].startswith("regulations.status = active") and "quarantine released" in out["applied"]
    status_sql = [q for q, p in _updates(cur, "regulations") if "status=?" in q]
    assert status_sql and _updates(cur, "regulations")[0][1] == ("active", 42)
    meta_sql = [(q, p) for q, p in _updates(cur, "regulations") if "extra_meta=?" in q]
    assert len(meta_sql) == 1
    meta = json.loads(meta_sql[0][1][0])
    assert meta["content_text"] == "x"                               # the rest of extra_meta kept
    assert meta["injection_guard"]["hits"] == GUARD["hits"]          # the guard's record kept
    assert meta["injection_guard"]["released"]["decision"] == "accepted"
    assert meta["injection_guard"]["released"]["reviewed_by"] == "abeer"
    assert repo.conn.committed


def test_rejecting_a_held_new_row_marks_it_rejected_not_released():
    out, cur, _ = _decide(("new", 42, 7, None, "pending"), "quarantine", {"injection_guard": GUARD}, "rejected")
    assert out["was_quarantined"] is True and out["released"] is False
    assert _updates(cur, "regulations")[0][1] == ("reject", 42)
    meta = json.loads([p for q, p in _updates(cur, "regulations") if "extra_meta=?" in q][0][0])
    assert meta["injection_guard"]["released"]["decision"] == "rejected"


def test_rejecting_a_held_modified_row_restores_the_old_version_and_reactivates():
    cur = Cursor([("modified", 42, 9, 8, "pending"), ("quarantine", json.dumps({"injection_guard": GUARD})),
                  ("<p>old</p>", "oldhash")])          # the previous version's content
    out = run_store.decide(Repo(cur), 5, 11, "rejected", "abeer")
    assert out["was_quarantined"] and not out["released"]
    joined = " | ".join(q for q, _ in cur.sql)
    assert "UPDATE regulation_versions SET status='active' WHERE version_id=?" in joined
    assert "UPDATE regulations SET document_html=?, content_hash=? WHERE id=?" in joined
    assert ("UPDATE regulations SET status='active' WHERE id=?", (42,)) in cur.sql
    assert "previous version restored" in out["applied"]


def test_a_row_that_is_not_held_is_decided_exactly_as_before():
    out, cur, _ = _decide(("new", 42, 7, None, "pending"), "", {}, "accepted")
    assert "was_quarantined" not in out and "released" not in out
    assert out["applied"] == "regulations.status = active"
    assert not [q for q, _ in _updates(cur, "regulations") if "extra_meta=?" in q]


def test_an_already_decided_change_is_untouched():
    cur = Cursor([("new", 42, 7, None, "accepted")])
    out = run_store.decide(Repo(cur), 5, 11, "accepted", "abeer")
    assert out["applied"] == "already recorded"
    assert not _updates(cur, "regulations")


# ------------------------------------------------------ run_results_api

def test_accept_of_a_held_row_starts_the_analysis(monkeypatch):
    from apis import run_results_api as api
    calls = []
    monkeypatch.setattr(api, "_on_release", lambda rid: calls.append(rid) or {"state": "started"})
    monkeypatch.setattr(run_store, "decide", lambda *a, **k: {"regulation_id": 42, "decision": "accepted",
                                                                "was_quarantined": True, "released": True})
    out = api.decide_one(5, 11, api.DecisionBody(decision="accepted", reviewed_by="abeer"))
    assert calls == [42] and out["analysis"] == {"state": "started"}


def test_a_release_whose_analysis_cannot_start_still_stands(monkeypatch):
    from apis import run_results_api as api
    def boom(rid):
        raise RuntimeError("no model key")
    monkeypatch.setattr(api, "_on_release", boom)
    monkeypatch.setattr(run_store, "decide", lambda *a, **k: {"regulation_id": 42, "decision": "accepted",
                                                                "was_quarantined": True, "released": True})
    out = api.decide_one(5, 11, api.DecisionBody(decision="accepted", reviewed_by="abeer"))
    assert out["decision"] == "accepted" and out["analysis"] == "not started: no model key"


def test_an_ordinary_decision_does_not_touch_the_runner(monkeypatch):
    from apis import run_results_api as api
    monkeypatch.setattr(api, "_on_release", lambda rid: pytest.fail("must not be called"))
    monkeypatch.setattr(run_store, "decide", lambda *a, **k: {"regulation_id": 42, "decision": "accepted"})
    out = api.decide_one(5, 11, api.DecisionBody(decision="accepted", reviewed_by="abeer"))
    assert "analysis" not in out


# ------------------------------------------------------------ pipeline_api

@pytest.fixture(scope="module")
def pipeline():
    return pytest.importorskip("apis.pipeline_api")


def test_the_runner_refuses_a_held_row_before_touching_anything(pipeline, monkeypatch):
    class Repo2:
        def get_regulation_by_id(self, rid):
            return {"id": rid, "status": "quarantine", "extra_meta": {}}
        def get_active_regulation_version(self, rid):
            pytest.fail("must stop before the version lookup")
    monkeypatch.setattr(pipeline, "repo", Repo2())
    pipeline._run_requirement_activity_analysis_for_regulation(42)
    st = pipeline._analysis_state[42]
    assert st["state"] == "held" and "quarantined" in st["error"]


def test_the_endpoint_reports_held_without_starting_a_thread(pipeline, monkeypatch):
    class Repo2:
        def get_regulation_by_id(self, rid):
            return {"id": rid, "status": "quarantine"}
    monkeypatch.setattr(pipeline, "repo", Repo2())
    monkeypatch.setattr(pipeline, "Thread", lambda *a, **k: pytest.fail("no thread for a held row"))
    out = pipeline.trigger_requirement_activity_analysis(42)
    assert out["state"] == "held"


def test_the_release_hook_is_the_api_starter(pipeline):
    from apis import run_results_api as api
    assert api._on_release is pipeline._start_requirement_activity_analysis
