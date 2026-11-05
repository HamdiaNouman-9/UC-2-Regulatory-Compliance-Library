"""CHG-008 step 5: what the review page reads. Stand-in cursors; no database.

    .venv/Scripts/python.exe -m pytest tests/test_injection_guard_review_api.py -v
"""
from __future__ import annotations

import json

import pytest

from storage import run_store
from tests.test_injection_guard_release import Cursor, Repo, GUARD


def test_a_held_row_reports_why_and_where_to_decide():
    cur = Cursor([("quarantine", json.dumps({"content_text": "x", "injection_guard": GUARD})),
                  (11, 5, "new", "pending")])
    out = run_store.review_state(Repo(cur), 42)
    assert out["held"] is True and out["review_status"] == "quarantine"
    assert out["guard"]["hits"] == GUARD["hits"] and out["guard"]["reasons"] == ["instruction_phrase"]
    assert out["pending_change"] == {"change_id": 11, "run_id": 5, "type": "new", "decision": "pending"}
    assert "content_text" not in json.dumps(out)              # only the guard's record leaves the row


def test_a_released_row_is_no_longer_held_but_keeps_its_record():
    guard = dict(GUARD, released={"decision": "accepted", "reviewed_by": "abeer", "at": "2026-11-05T11:00:00Z"})
    cur = Cursor([("active", json.dumps({"injection_guard": guard})), (11, 5, "new", "accepted")])
    out = run_store.review_state(Repo(cur), 42)
    assert out["held"] is False and out["review_status"] == "active"
    assert out["guard"]["released"]["reviewed_by"] == "abeer"
    assert out["pending_change"] is None and out["last_change"]["decision"] == "accepted"


def test_an_ordinary_row_has_nothing_to_show():
    cur = Cursor([("", None), None])
    out = run_store.review_state(Repo(cur), 42)
    assert out == {"regulation_id": 42, "review_status": None, "held": False, "guard": None,
                   "pending_change": None, "last_change": None}


def test_a_missing_regulation_is_none_and_404_at_the_api(monkeypatch):
    assert run_store.review_state(Repo(Cursor([None])), 42) is None
    from fastapi import HTTPException
    from apis import run_results_api as api
    monkeypatch.setattr(run_store, "review_state", lambda repo, rid: None)
    with pytest.raises(HTTPException) as e:
        api.regulation_review(42)
    assert e.value.status_code == 404


def test_the_pending_change_is_preferred_over_a_later_decided_one():
    """The ORDER BY puts pending first; the row shape is what the page acts on."""
    cur = Cursor([("quarantine", json.dumps({"injection_guard": GUARD})), (9, 4, "modified", "pending")])
    run_store.review_state(Repo(cur), 42)
    q = [q for q, _ in cur.sql if q.startswith("SELECT TOP 1 change_id")][0]
    assert "CASE WHEN decision='pending' THEN 0 ELSE 1 END, change_id DESC" in q
