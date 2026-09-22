"""Version tokens: detecting a file that was replaced behind an unchanged link.

WHY THIS FILE LOOKS LIKE THIS

Same reason as the other suites here — importing the orchestrator pulls in the
OCR stack, which is gigabytes and is not needed to check classification. The
heavy modules are stubbed before the import. No network and no database.

What is verified here:

  probe parsing       SharePoint's `{GUID},<version>`, including the `,1pub`
                      form that a digits-anchored pattern drops silently
  probe safety        GET not HEAD, browser UA, ranged, and hrefs containing
                      literal spaces are encoded rather than failing
  honest failure      a probe that could not run is reported as such, never as
                      a document that did not change
  classification      a file swapped at an unchanged URL comes back `modified`
  no reclassification enabling the probe records a baseline token instead of
                      marking the whole library modified

    venv/Scripts/python.exe -m pytest tests/test_fingerprint.py -v
"""
from __future__ import annotations

import json
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class _Any:
    def __getattr__(self, name):
        return _Any()

    def __call__(self, *a, **kw):
        return _Any()

    def __iter__(self):
        return iter(())

    def __bool__(self):
        return False


_PREFIXES = ("fitz", "pdf2image", "pytesseract", "PIL", "cv2", "paddle",
             "paddleocr", "paddlex", "torch", "transformers", "easyocr",
             "docx", "pptx", "camelot", "pdfplumber", "layoutparser",
             "lingua", "langdetect", "openai", "tiktoken", "selenium",
             "bs4", "httpx", "aiohttp", "tenacity")

for _name in _PREFIXES:
    if _name not in sys.modules:
        try:
            __import__(_name)
        except Exception:
            _m = types.ModuleType(_name)
            _m.__getattr__ = lambda attr: _Any()
            _m.__path__ = []
            sys.modules[_name] = _m

from dynamic_crawler import fingerprint                             # noqa: E402
from orchestrator.orchestrator import Orchestrator           # noqa: E402


# --------------------------------------------------------------------------- #
#  doubles                                                                     #
# --------------------------------------------------------------------------- #

class FakeResponse:
    def __init__(self, status_code=206, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


class FakeSession:
    """Answers from a {url: response-or-exception} map and records the calls."""

    def __init__(self, responses=None, default=None):
        self.responses = responses or {}
        self.default = default or FakeResponse(404, {})
        self.calls = []

    def get(self, url, **kw):
        self.calls.append({"url": url, **kw})
        answer = self.responses.get(url, self.default)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Doc:
    def __init__(self, **kw):
        self.document_url = kw.pop("document_url", "")
        self.doc_path = kw.pop("doc_path", [])
        self.content_hash = kw.pop("content_hash", "")
        self.extra_meta = kw.pop("extra_meta", {})
        for k, v in kw.items():
            setattr(self, k, v)


class FakeRepo:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.updates = []

    def find_by_identity(self, url, path):
        want = " > ".join(path) if isinstance(path, (list, tuple)) else str(path)
        for r in self.rows:
            stored = r.get("doc_path")
            stored = " > ".join(stored) if isinstance(stored, (list, tuple)) else str(stored)
            if r.get("document_url") == url and stored == want:
                return dict(r)
        return None

    def find_by_identity_fields(self, fields):
        """The generic lookup. Required since `title` joined the default identity
        on 2026-08-16: three fields no longer fit the two-column shortcut, so
        without this every lookup raises NotImplementedError rather than
        exercising the token logic these tests are about."""
        def norm(v):
            return " > ".join(v) if isinstance(v, (list, tuple)) else str(v or "")
        for r in self.rows:
            if all(norm(r.get(k)) == norm(v) for k, v in fields.items()):
                return dict(r)
        return None

    def find_by_reference(self, ref):
        return None

    def find_regulations_by_source(self, source):
        return []

    def update_regulation(self, regulation_id, **fields):
        self.updates.append((regulation_id, fields))


def orch(repo):
    return Orchestrator(crawler=_Any(), repo=repo, source_name="src:test")


SP_ETAG = '"{A07F4B04-B20F-4D9E-8B81-5E5AB33C4A5F},4"'
SP_ETAG_V5 = '"{A07F4B04-B20F-4D9E-8B81-5E5AB33C4A5F},5"'


# --------------------------------------------------------------------------- #
#  reading the version token off a response                                    #
# --------------------------------------------------------------------------- #

def test_sharepoint_etag_is_recognised():
    url = "https://cma.org.sa/a.pdf"
    s = FakeSession({url: FakeResponse(206, {"ETag": SP_ETAG})})
    token, basis = fingerprint.version_token(url, s)
    assert basis == "etag"
    assert token == "{A07F4B04-B20F-4D9E-8B81-5E5AB33C4A5F},4"


def test_publishing_suffix_is_not_dropped():
    """MOH answers `,1pub`. Anchoring on digits scores it unparseable, and the
    document then reports as never changing."""
    url = "https://www.moh.gov.sa/a.pdf"
    etag = '"{D1C4FD07-BE15-44AC-A824-BA67F635F8EF},1pub"'
    s = FakeSession({url: FakeResponse(206, {"ETag": etag})})
    token, basis = fingerprint.version_token(url, s)
    assert basis == "etag"
    assert token.endswith(",1pub")


def test_opaque_etag_is_still_a_token_but_reported_separately():
    url = "https://example.gov/a.pdf"
    s = FakeSession({url: FakeResponse(206, {"ETag": '"abc123-gzip"'})})
    token, basis = fingerprint.version_token(url, s)
    assert token == "abc123-gzip"
    assert basis == "etag-opaque"


def test_last_modified_used_when_there_is_no_etag():
    url = "https://example.gov/a.pdf"
    s = FakeSession({url: FakeResponse(206, {
        "Last-Modified": "Sun, 02 Aug 2026 13:42:27 GMT",
        "Content-Range": "bytes 0-1/948213"})})
    token, basis = fingerprint.version_token(url, s)
    assert basis == "last-modified+length"
    assert "948213" in token


def test_no_validator_reports_identity_only():
    url = "https://example.gov/a.pdf"
    s = FakeSession({url: FakeResponse(206, {})})
    token, basis = fingerprint.version_token(url, s)
    assert token == ""
    assert basis == fingerprint.BASIS_NONE


def test_transport_error_is_reported_not_swallowed():
    url = "https://example.gov/a.pdf"
    s = FakeSession({url: RuntimeError("connection reset")})
    token, basis = fingerprint.version_token(url, s)
    assert token == ""
    assert basis == fingerprint.BASIS_FAILED


def test_http_error_is_reported_not_swallowed():
    url = "https://example.gov/a.pdf"
    s = FakeSession({url: FakeResponse(403, {"ETag": SP_ETAG})})
    token, basis = fingerprint.version_token(url, s)
    assert basis == fingerprint.BASIS_FAILED


def test_failed_probe_is_distinguishable_from_no_validator():
    """Both give an empty token. They must not give the same reason."""
    assert fingerprint.BASIS_FAILED != fingerprint.BASIS_NONE


# --------------------------------------------------------------------------- #
#  how the request is made                                                     #
# --------------------------------------------------------------------------- #

def test_request_is_a_ranged_get_with_a_browser_agent():
    """HEAD, and a default agent, are answered by a fake 200 on some regulator
    firewalls."""
    url = "https://example.gov/a.pdf"
    s = FakeSession({url: FakeResponse(206, {"ETag": SP_ETAG})})
    fingerprint.version_token(url, s)
    call = s.calls[0]
    assert call["headers"]["Range"] == "bytes=0-1"
    assert "Mozilla/" in call["headers"]["User-Agent"]


def test_literal_spaces_in_the_href_are_encoded():
    """AML's document links contain spaces, which cannot be put on the wire."""
    raw = "https://www.aml.gov.sa/en-us/Rules and Instructions/Bank Accounts.pdf"
    encoded = "https://www.aml.gov.sa/en-us/Rules%20and%20Instructions/Bank%20Accounts.pdf"
    s = FakeSession({encoded: FakeResponse(206, {"ETag": SP_ETAG})})
    token, basis = fingerprint.version_token(raw, s)
    assert basis == "etag"
    assert s.calls[0]["url"] == encoded


def test_non_http_url_is_not_probed():
    s = FakeSession()
    token, basis = fingerprint.version_token("", s)
    assert (token, basis) == ("", fingerprint.BASIS_NONE)
    assert s.calls == []


# --------------------------------------------------------------------------- #
#  batching and reporting                                                      #
# --------------------------------------------------------------------------- #

def test_tokens_for_deduplicates_and_reuses_one_session():
    url = "https://cma.org.sa/a.pdf"
    session = FakeSession({url: FakeResponse(206, {"ETag": SP_ETAG})})
    real = fingerprint.requests
    try:
        fingerprint.requests = types.SimpleNamespace(Session=lambda: session)
        out = fingerprint.tokens_for([url, url, url], workers=1)
    finally:
        fingerprint.requests = real
    assert list(out) == [url]
    assert len(session.calls) == 1


def test_summarise_counts_every_basis():
    tokens = {"a": ("x", "etag"), "b": ("", fingerprint.BASIS_FAILED),
              "c": ("y", "etag"), "d": ("", fingerprint.BASIS_NONE)}
    s = fingerprint.summarise(tokens)
    assert s["probed"] == 4
    assert s["with_token"] == 2
    assert s["by_basis"]["etag"] == 2
    assert s["by_basis"][fingerprint.BASIS_FAILED] == 1


def test_annotate_writes_the_token_onto_the_document():
    url = "https://cma.org.sa/a.pdf"
    session = FakeSession({url: FakeResponse(206, {"ETag": SP_ETAG})})
    doc = Doc(document_url=url, extra_meta={"keep": "me"})
    real = fingerprint.requests
    try:
        fingerprint.requests = types.SimpleNamespace(Session=lambda: session)
        summary = fingerprint.annotate([doc], workers=1)
    finally:
        fingerprint.requests = real
    assert doc.extra_meta["version_token"].endswith(",4")
    assert doc.extra_meta["hash_basis"] == "etag"
    assert doc.extra_meta["keep"] == "me"
    assert summary["with_token"] == 1


# --------------------------------------------------------------------------- #
#  classification — the behaviour this exists for                              #
# --------------------------------------------------------------------------- #

def _stored(url, hash_="H", token=None):
    meta = {"version_token": token} if token is not None else {}
    return {"id": 7, "document_url": url, "doc_path": ["A"],
            "content_hash": hash_, "extra_meta": meta}


def test_file_swapped_at_an_unchanged_url_is_modified():
    """The whole point. Same URL, same link text, so the same content_hash —
    only the server's version token moved."""
    url = "https://cma.org.sa/circular-42.pdf"
    repo = FakeRepo([_stored(url, "H", "{GUID},4")])
    doc = Doc(document_url=url, doc_path=["A"], content_hash="H",
              extra_meta={"version_token": "{GUID},5"})
    buckets = orch(repo).classify_documents([doc])
    assert [d.document_url for d in buckets["modified"]] == [url]
    assert buckets["unchanged"] == []


def test_same_token_stays_unchanged():
    url = "https://cma.org.sa/circular-42.pdf"
    repo = FakeRepo([_stored(url, "H", "{GUID},4")])
    doc = Doc(document_url=url, doc_path=["A"], content_hash="H",
              extra_meta={"version_token": "{GUID},4"})
    buckets = orch(repo).classify_documents([doc])
    assert len(buckets["unchanged"]) == 1
    assert buckets["modified"] == []


def test_a_failed_probe_does_not_invent_a_change():
    """An empty token means "not measured". It must not read as a difference."""
    url = "https://cma.org.sa/circular-42.pdf"
    repo = FakeRepo([_stored(url, "H", "{GUID},4")])
    doc = Doc(document_url=url, doc_path=["A"], content_hash="H",
              extra_meta={"version_token": "", "hash_basis": fingerprint.BASIS_FAILED})
    buckets = orch(repo).classify_documents([doc])
    assert len(buckets["unchanged"]) == 1


def test_first_token_is_recorded_without_reclassifying_the_library():
    """Turning the probe on must not mark every stored document modified."""
    url = "https://cma.org.sa/circular-42.pdf"
    repo = FakeRepo([_stored(url, "H")])           # nothing stored yet
    doc = Doc(document_url=url, doc_path=["A"], content_hash="H",
              extra_meta={"version_token": "{GUID},4", "hash_basis": "etag"})
    o = orch(repo)
    buckets = o.classify_documents([doc])
    assert len(buckets["unchanged"]) == 1
    assert buckets["modified"] == []
    assert len(o._token_backfill) == 1

    assert o._apply_token_backfill() == 1
    regulation_id, fields = repo.updates[0]
    assert regulation_id == 7
    assert json.loads(fields["extra_meta"])["version_token"] == "{GUID},4"


def test_backfill_preserves_metadata_already_stored():
    url = "https://cma.org.sa/circular-42.pdf"
    row = _stored(url, "H")
    row["extra_meta"] = {"regulator_status": "In-Force"}
    repo = FakeRepo([row])
    doc = Doc(document_url=url, doc_path=["A"], content_hash="H",
              extra_meta={"version_token": "{GUID},4", "hash_basis": "etag"})
    o = orch(repo)
    o.classify_documents([doc])
    o._apply_token_backfill()
    stored = json.loads(repo.updates[0][1]["extra_meta"])
    assert stored["regulator_status"] == "In-Force"
    assert stored["version_token"] == "{GUID},4"


def test_a_changed_hash_is_still_modified_without_any_token():
    url = "https://cma.org.sa/circular-42.pdf"
    repo = FakeRepo([_stored(url, "OLD")])
    doc = Doc(document_url=url, doc_path=["A"], content_hash="NEW", extra_meta={})
    buckets = orch(repo).classify_documents([doc])
    assert len(buckets["modified"]) == 1


# --------------------------------------------------------------------------- #
#  the repos agree on the shape of extra_meta                                  #
# --------------------------------------------------------------------------- #

def test_both_repos_return_extra_meta_as_a_dict():
    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    from storage.mssql_repo import MSSQLRepository

    raw = {"extra_meta": json.dumps({"version_token": "{GUID},4"})}
    for repo_cls in (ExcelRepo, MSSQLRepository):
        out = repo_cls._with_extra_meta(dict(raw))
        assert out["extra_meta"] == {"version_token": "{GUID},4"}, repo_cls.__name__

    for repo_cls in (ExcelRepo, MSSQLRepository):
        assert repo_cls._with_extra_meta({"extra_meta": None})["extra_meta"] == {}
        assert repo_cls._with_extra_meta({"extra_meta": "not json"})["extra_meta"] == {}


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
