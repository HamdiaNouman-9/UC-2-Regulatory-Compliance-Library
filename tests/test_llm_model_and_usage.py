import os
from unittest.mock import MagicMock

os.environ.setdefault("OPENROUTER_API_KEY", "test")

import processor.llm_client as lc
from storage import llm_settings


def test_provider_pin_only_for_default_model():
    other = lc.LLMClient(model="other/model")._build_payload("x", 0.1, 100, False)
    default = lc.LLMClient(model=lc.DEFAULT_MODEL)._build_payload("x", 0.1, 100, False)
    assert "provider" not in other and "provider" in default


def test_get_model_falls_back_when_db_unavailable(monkeypatch):
    repo = MagicMock()
    repo._get_conn.side_effect = RuntimeError("db down")
    monkeypatch.delenv("LLM_MODEL", raising=False)
    assert llm_settings.get_model(repo) == lc.DEFAULT_MODEL
    monkeypatch.setenv("LLM_MODEL", "env/model")
    assert llm_settings.get_model(repo) == "env/model"


def test_openrouter_usage_reports_partial_failure(monkeypatch):
    def fake_get(url, **k):
        if url.endswith("/credits"):
            raise RuntimeError("403")
        r = MagicMock(); r.json.return_value = {"data": {"usage": 1.5}}
        return r
    monkeypatch.setattr(llm_settings.requests, "get", fake_get)
    out = llm_settings.openrouter_usage()
    assert out["key"] == {"usage": 1.5} and "credits" in out["errors"]


class _Cur:
    def __init__(self, fetchall=None, fetchone=None):
        self.executed, self._all, self._one = [], fetchall or [], fetchone

    def execute(self, sql, params=()):
        self.executed.append((sql, params))

    def fetchall(self): return self._all
    def fetchone(self): return self._one


def _repo(cur):
    conn = MagicMock(); conn.cursor.return_value = cur
    conn.__enter__.return_value = conn
    repo = MagicMock(); repo._get_conn.return_value = conn
    return repo


def test_client_reports_usage_and_recorder_writes_a_row(monkeypatch):
    r = MagicMock(status_code=200)
    r.json.return_value = {
        "model": "other/model", "provider": "P",
        "choices": [{"finish_reason": "stop", "message": {"content": "hi"}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 4, "cost": 0.0002,
                  "prompt_tokens_details": {"cached_tokens": 3}}}
    sent = []
    monkeypatch.setattr(lc.requests, "post", lambda *a, **k: (sent.append(k["json"]), r)[1])

    cur = _Cur()
    c = lc.LLMClient(model="other/model")
    c.on_usage = llm_settings.make_usage_recorder(
        _repo(cur), step="requirements", regulation_id=7, version_id=9)
    c.complete("x")

    assert sent[0]["usage"] == {"include": True}
    insert = [p for s, p in cur.executed if s.startswith("INSERT INTO llm_usage")][0]
    assert insert == ("other/model", "P", 10, 4, 3, 0.0002, "requirements", 7, 9)


def test_recorder_swallows_db_failure():
    repo = MagicMock(); repo._get_conn.side_effect = RuntimeError("db down")
    llm_settings.make_usage_recorder(repo)({"model": "m"})   # must not raise


def test_usage_summary_totals_and_bad_group():
    import datetime as dt
    cur = _Cur(fetchall=[("m1", 2, 100, 40, 10, 0.5, None), ("m2", 1, 50, 20, 0, None, None)],
               fetchone=(dt.datetime(2026, 9, 21),))
    out = llm_settings.usage_summary(_repo(cur), group_by="model")
    assert out["total"]["calls"] == 3 and out["total"]["prompt_tokens"] == 150
    assert out["total"]["cost_usd"] == 0.5          # null cost rows are skipped, not zeroed
    assert out["tracking_since"].startswith("2026-09-21")
    try:
        llm_settings.usage_summary(_repo(_Cur()), group_by="bogus")
        assert False
    except ValueError:
        pass
