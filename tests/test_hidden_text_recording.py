"""CHG-006 bookkeeping: the counts reach extra_meta without touching
updated_at, in both repositories and through the orchestrator's recorder.

    .venv/Scripts/python.exe -m pytest tests/test_hidden_text_recording.py -v
"""
from __future__ import annotations

import json
import threading
import types

import pytest


def _mssql_with_recorder():
    from storage.mssql_repo import MSSQLRepository
    calls = []

    class Cur:
        def execute(self, q, params=None):
            calls.append((" ".join(q.split()), list(params or [])))

        def fetchone(self):
            return (1,)

    class Conn:
        def cursor(self):
            return Cur()

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

    repo = MSSQLRepository.__new__(MSSQLRepository)
    repo._get_conn = lambda: Conn()
    repo._lock = threading.Lock()
    return repo, calls


def test_mssql_set_extra_meta_does_not_touch_updated_at():
    repo, calls = _mssql_with_recorder()
    repo.set_extra_meta(7, '{"hidden_text": {"page": {"removed_chars": 5}}}')
    q, params = calls[0]
    assert q == "UPDATE regulations SET extra_meta = ? WHERE id = ?"
    assert params == ['{"hidden_text": {"page": {"removed_chars": 5}}}', 7]
    assert "updated_at" not in q


def test_mssql_update_regulation_still_bumps_updated_at():
    """The contrast: the ordinary update is a modification and says so."""
    repo, calls = _mssql_with_recorder()
    repo.update_regulation(7, title="T")
    assert "updated_at = SYSDATETIMEOFFSET()" in calls[0][0]


def test_excel_repo_set_extra_meta(tmp_path):
    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    from models.models import RegulatoryDocument
    repo = ExcelRepo(tmp_path / "t.xlsx")
    rid = repo._insert_regulation(RegulatoryDocument(
        regulator="R", source_system="S", category="C", title="T",
        document_url="https://a/b", published_date="", reference_no="",
        department="", year=2026, source_page_url="https://a", document_html="<p>x</p>"))
    before = repo.get_regulation_by_id(rid).get("updated_at")
    repo.set_extra_meta(rid, '{"hidden_text": {"page": null, "files": []}}')
    row = repo.get_regulation_by_id(rid)
    assert json.loads(row["extra_meta"])["hidden_text"] == {"page": None, "files": []}
    assert row.get("updated_at") == before


class _Repo:
    def __init__(self):
        self.calls = []

    def set_extra_meta(self, rid, js):
        self.calls.append((rid, json.loads(js)))


def _orch():
    from orchestrator.orchestrator import Orchestrator
    o = Orchestrator.__new__(Orchestrator)
    o.repo = _Repo()
    o._log_step = lambda *a, **k: None
    return o


def test_recorder_writes_page_and_file_parts_once():
    o = _orch()
    doc = types.SimpleNamespace(extra_meta={"monitoring_status": "new"})
    dec = types.SimpleNamespace(hidden_text={"removed_chars": 12, "kept_chars": 900,
                                             "duplicate_chars": 0, "reasons": {"display_none": 12}, "samples": []})
    o._files_hidden = [{"file": "a.pdf", "removed_chars": 30, "duplicate_chars": 0, "reasons": {"tiny": 30}, "samples": []}]
    o._record_hidden_text(5, doc, dec)
    assert len(o.repo.calls) == 1
    rid, meta = o.repo.calls[0]
    assert rid == 5
    assert meta["monitoring_status"] == "new", "existing keys kept"
    assert meta["hidden_text"]["page"]["removed_chars"] == 12
    assert meta["hidden_text"]["files"][0]["file"] == "a.pdf"
    assert doc.extra_meta["hidden_text"] == meta["hidden_text"]
    assert o._files_hidden == [], "file parts consumed, not carried to the next row"


def test_recorder_writes_nothing_when_nothing_was_hidden():
    o = _orch()
    doc = types.SimpleNamespace(extra_meta={})
    o._record_hidden_text(5, doc, types.SimpleNamespace(hidden_text=None))
    o._record_hidden_text(5, doc, None)
    assert o.repo.calls == []
    assert "hidden_text" not in doc.extra_meta


def test_recorder_never_raises():
    o = _orch()
    o.repo.set_extra_meta = lambda *a: (_ for _ in ()).throw(RuntimeError("db down"))
    doc = types.SimpleNamespace(extra_meta={})
    o._record_hidden_text(5, doc, types.SimpleNamespace(hidden_text={"removed_chars": 1}))


def test_pdf_extractor_reports_hidden_text(tmp_path):
    fitz = pytest.importorskip("fitz")
    from processor.Text_Extractor import OCRProcessor
    doc = fitz.open()
    for i in range(3):
        p = doc.new_page(width=595, height=842)
        p.insert_text((72, 72), f"Page {i + 1} heading", fontsize=12)
        y = 100
        for line in range(30):                        # enough text to be a real page, not a scan
            p.insert_text((72, y), "Licensees shall maintain adequate records of all transactions and report them.", fontsize=10)
            y += 14
        if i == 2:
            p.insert_text((72, y + 10), "SECRET white instruction", fontsize=11, color=(1, 1, 1))
    path = tmp_path / "t.pdf"
    doc.save(str(path)); doc.close()
    text, meta = OCRProcessor.extract_text_from_pdf_smart(str(path))
    assert "SECRET" not in text
    assert "Page 3 heading" in text
    assert meta["hidden_text"]["removed_chars"] == len("SECRET white instruction")
    assert meta["hidden_text"]["reasons"] == {"near_background": len("SECRET white instruction")}
