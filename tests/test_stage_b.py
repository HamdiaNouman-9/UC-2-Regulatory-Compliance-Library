"""The change-detection fixes in Orchestrator, tested without a database.

WHY THIS FILE LOOKS LIKE THIS

Same reason as test_stage_a.py. `orch.py` imports the parent orchestrator, which
pulls in the OCR stack (fitz, pdf2image, paddle, torch). None of that is needed
to check classification logic, and installing it is gigabytes, so the heavy
modules are stubbed before the import.

What is verified here:

  archive id          analysis is archived against the version holding the OLD
                      content, not the one that replaced it
  row refresh         a modify rewrites the row's content, not only its hash
  disappeared         the completeness gate is fed from the repository, so it
                      works on MSSQL and not only on the Excel preview repo
  configurable id     a source config can change what "the same document" means
  early exit          an unchanged inventory actually stops

    venv/Scripts/python.exe -m pytest tests/test_stage_b.py -v
    venv/Scripts/python.exe tests/test_stage_b.py          # no pytest needed
"""
from __future__ import annotations

import sys
import tempfile
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class _Any:
    """Absorbs whatever a stubbed module is asked to do at import time.
    Chainable, so `Builder.from_all().with_x().build()` survives.
    """

    def __getattr__(self, name):
        return _Any()

    def __call__(self, *a, **kw):
        return _Any()

    def __iter__(self):
        return iter(())

    def __bool__(self):
        return False


class _StubFinder:
    """Fabricates any module under PREFIXES that is not really installed.

    The pipeline's OCR dependencies are imported at module scope by the parent
    orchestrator but never called on these paths.
    """

    PREFIXES = ("fitz", "pdf2image", "pytesseract", "PIL", "cv2", "paddle",
                "paddleocr", "paddlex", "torch", "transformers", "easyocr",
                "docx", "pptx", "camelot", "pdfplumber", "layoutparser",
                "lingua", "langdetect", "openai", "tiktoken", "selenium",
                "bs4", "httpx", "aiohttp", "tenacity")

    def find_module(self, name, path=None):
        return self if name.split(".")[0] in self.PREFIXES else None

    def load_module(self, name):
        mod = types.ModuleType(name)
        mod.__getattr__ = lambda attr: _Any()
        mod.__path__ = []
        sys.modules[name] = mod
        return mod


for _name in _StubFinder.PREFIXES:
    if _name not in sys.modules:
        try:
            __import__(_name)
        except Exception:
            _m = types.ModuleType(_name)
            _m.__getattr__ = lambda attr: _Any()
            _m.__path__ = []
            sys.modules[_name] = _m

from orchestrator.orchestrator import Orchestrator          # noqa: E402


# --------------------------------------------------------------------------- #
#  doubles                                                                     #
# --------------------------------------------------------------------------- #

class Doc:
    def __init__(self, **kw):
        self.document_url = kw.pop("document_url", "")
        self.doc_path = kw.pop("doc_path", [])
        self.content_hash = kw.pop("content_hash", "")
        self.extra_meta = kw.pop("extra_meta", {})
        for k, v in kw.items():
            setattr(self, k, v)


class FakeRepo:
    """Records calls. Reads answer from `rows`; writes only get recorded."""

    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []
        self._next_version = 100

    # ---- reads ---- #
    def find_by_identity(self, url, path):
        self.calls.append(("find_by_identity", url, path))
        return next((r for r in self.rows
                     if r.get("document_url") == url
                     and " > ".join(r.get("doc_path") or []) == path), None)

    def find_by_identity_fields(self, fields):
        self.calls.append(("find_by_identity_fields", dict(fields)))
        for r in self.rows:
            for k, v in fields.items():
                stored = r.get(k)
                if isinstance(stored, (list, tuple)):
                    stored = " > ".join(str(x) for x in stored)
                if (str(stored) if stored is not None else "") != v:
                    break
            else:
                return r
        return None

    def find_by_reference(self, ref):
        self.calls.append(("find_by_reference", ref))
        return None

    def find_regulations_by_source(self, source_system):
        self.calls.append(("find_regulations_by_source", source_system))
        return [r for r in self.rows if r.get("source_system") == source_system]

    def get_regulation_by_id(self, rid):
        return next((r for r in self.rows if r.get("id") == rid), {})

    def last_good_run(self, source):
        return getattr(self, "_last_run", None)

    def counts(self):
        return {}

    # ---- writes ---- #
    def __getattr__(self, name):
        def record(*a, **kw):
            self.calls.append((name, a, kw))
            if name == "insert_regulation_version":
                self._next_version += 1
                return self._next_version
            if name == "_insert_regulation":
                return 999
            return None
        return record

    def named(self, name):
        return [c for c in self.calls if c[0] == name]


class FakeCrawler:
    def __init__(self, docs=None, source_system="SRC", hints_path=None):
        self.docs = docs or []
        self.source_system = source_system
        # Only a FORM crawler has one. Its presence is what makes the run's
        # history key per-form; a source config or hand-written wrapper has no
        # hints_path and keeps the bare source name.
        if hints_path is not None:
            self.hints_path = hints_path

    def fetch_documents(self):
        return list(self.docs)


#: The crawl's miss-streak files, kept out of output/ so a test run leaves the
#: real state alone. One directory per build() call, so tests do not share memory.
_STATE = Path(tempfile.mkdtemp(prefix="crawl-state-"))
_BUILT = [0]


def build(repo, crawler=None, **kw):
    """A Orchestrator with the parent's __init__ bypassed."""
    o = Orchestrator.__new__(Orchestrator)
    o.repo = repo
    o.crawler = crawler or FakeCrawler()
    o.source_name = kw.pop("source_name", "src")
    o.identity = Orchestrator._clean_identity(kw.pop("identity", None))
    o.version_key = kw.pop("version_key", None)
    o.analyse = False
    o.limit = None
    _BUILT[0] += 1
    o.change_root = kw.pop("change_root", _STATE / str(_BUILT[0]))
    o.report = {}
    return o


# --------------------------------------------------------------------------- #
#  archive against the OLD version id                                          #
# --------------------------------------------------------------------------- #

def test_archive_uses_the_old_version_not_the_new_one():
    repo = FakeRepo(rows=[{"id": 7, "content_hash": "old",
                           "document_html": "<p>old</p>"}])
    o = build(repo)
    doc = Doc(document_url="u", content_hash="new", title="T",
              extra_meta={"monitoring_status": "modified",
                          "existing_regulation_id": 7})
    o._timed = lambda *a, **k: _null_ctx()
    o.extract_text_content_unified = lambda *a, **k: ("", None)
    o._log_step = lambda *a, **k: None
    o._process_versioned_doc(doc)

    versions = repo.named("insert_regulation_version")
    assert len(versions) == 2, "old snapshot then new snapshot"
    old_id, new_id = 101, 102
    assert versions[0][2]["status"] == "inactive"
    assert versions[1][2]["status"] == "active"

    archived = repo.named("archive_current_analysis")
    assert archived, "analysis must be archived on a modify"
    assert archived[0][1] == (7, old_id), (
        f"archived against {archived[0][1][1]}, expected the OLD version {old_id}")
    assert archived[0][1][1] != new_id


class _null_ctx:
    def __enter__(self):
        return {"status": "", "message": ""}

    def __exit__(self, *a):
        return False


# --------------------------------------------------------------------------- #
#  a modify refreshes the row, not just the hash                               #
# --------------------------------------------------------------------------- #

def test_modify_rewrites_content_not_only_the_hash():
    doc = Doc(document_url="u", content_hash="new", title="New Title",
              document_html="<p>new</p>", published_date="2026-08-01",
              extra_meta={"a": 1})
    fields = Orchestrator._modified_row_fields(doc, "new")
    assert fields["content_hash"] == "new"
    assert fields["document_html"] == "<p>new</p>", (
        "the new hash must not sit next to the old html")
    assert fields["title"] == "New Title"
    assert fields["published_date"] == "2026-08-01"
    assert "extra_meta" in fields


def test_modify_never_blanks_a_field_the_crawl_did_not_return():
    doc = Doc(document_url="u", content_hash="new", title="", document_html=None)
    fields = Orchestrator._modified_row_fields(doc, "new")
    assert "title" not in fields, "an empty title must not erase the stored one"
    assert "document_html" not in fields
    assert fields == {"content_hash": "new"}


# --------------------------------------------------------------------------- #
#  the disappeared bucket                                                      #
# --------------------------------------------------------------------------- #

def test_disappeared_is_filled_from_the_repository():
    stored = [
        {"id": 1, "document_url": "a", "doc_path": ["X"], "content_hash": "h1",
         "source_system": "SRC"},
        {"id": 2, "document_url": "b", "doc_path": ["X"], "content_hash": "h2",
         "source_system": "SRC"},
    ]
    repo = FakeRepo(rows=stored)
    o = build(repo, FakeCrawler(source_system="SRC"))
    seen = Doc(document_url="a", doc_path=["X"], content_hash="h1")

    buckets = o.classify_documents([seen])
    assert [r["id"] for r in buckets["disappeared"]] == [2], (
        "the document the crawl did not return must be reported")
    assert repo.named("find_regulations_by_source"), (
        "must ask the repository, not an Excel-only attribute")


def test_disappeared_is_empty_when_the_run_saw_everything():
    stored = [{"id": 1, "document_url": "a", "doc_path": ["X"],
               "content_hash": "h1", "source_system": "SRC"}]
    o = build(FakeRepo(rows=stored), FakeCrawler(source_system="SRC"))
    buckets = o.classify_documents([Doc(document_url="a", doc_path=["X"],
                                        content_hash="h1")])
    assert buckets["disappeared"] == []
    assert len(buckets["unchanged"]) == 1


# --------------------------------------------------------------------------- #
#  configurable identity                                                       #
# --------------------------------------------------------------------------- #

def test_identity_defaults_to_url_and_path_and_title():
    """`title` joined the default tuple deliberately (2026-08-16).

    Two documents can share a url and a folder and still be different
    instruments — MC publishes a law, its implementing regulation and its
    attachments under one folder, and the three are told apart by title alone.
    The url lookup still takes url and path only; title narrows the result.
    """
    o = build(FakeRepo())
    assert o.identity == ("document_url", "doc_path", "title")
    o.classify_documents([Doc(document_url="u", doc_path=["A", "B"])])
    # Three fields no longer match the two-column fast path in
    # changesignal.find_existing, so the lookup goes through the generic
    # field-based finder rather than find_by_identity(url, path).
    assert o.repo.named("find_by_identity") == []
    fields = o.repo.named("find_by_identity_fields")[0][1]
    assert fields["document_url"] == "u" and fields["doc_path"] == "A > B"


def test_each_form_keeps_its_own_baseline():
    """One regulator, several forms of different sizes, one baseline each.

    ZATCA publishes through five forms. They all reported under the bare
    regulator name, so each run overwrote the previous form's baseline and the
    next form was measured against a count belonging to a different page:
    `count moved 98 -> 4 (95.9%)` — QUARANTINED, on every run, forever.
    """
    guidelines = build(FakeRepo(), FakeCrawler(
        hints_path="/x/hints/zatca.ie_guidelines.yml"), source_name="ZATCA")
    circulars = build(FakeRepo(), FakeCrawler(
        hints_path="/x/hints/zatca.ie_circulars.yml"), source_name="ZATCA")

    assert guidelines._run_key == "ZATCA/zatca.ie_guidelines"
    assert circulars._run_key == "ZATCA/zatca.ie_circulars"
    assert guidelines._run_key != circulars._run_key, (
        "two forms sharing a key is the whole bug — one overwrites the other")


def test_a_crawler_without_a_form_keeps_the_bare_source_name():
    """The re-key must not touch source configs and hand-written wrappers.

    Their stored baselines are keyed on the bare name, and changing it would
    make every one of them run unchecked once for no reason. Only forms pay.
    """
    o = build(FakeRepo(), FakeCrawler(), source_name="Ministry of Health")
    assert o._run_key == "Ministry of Health"
    assert o._history_key("Ministry of Health") == "Ministry of Health"


def test_a_configured_identity_changes_the_lookup():
    stored = [{"id": 5, "reference_no": "R-1", "content_hash": "h",
               "source_system": "SRC"}]
    repo = FakeRepo(rows=stored)
    o = build(repo, FakeCrawler(source_system="SRC"),
              identity=["reference_no"])
    buckets = o.classify_documents([Doc(reference_no="R-1", content_hash="h")])

    assert repo.named("find_by_identity_fields"), "must use the generic lookup"
    assert repo.named("find_by_identity") == [], "must not use the url lookup"
    assert len(buckets["unchanged"]) == 1, "matched on reference_no alone"


def test_identity_accepts_a_bare_string():
    assert Orchestrator._clean_identity("page") == ("page",)
    # The empty/None default follows DEFAULT_IDENTITY, which gained `title`.
    assert Orchestrator._clean_identity([]) == ("document_url", "doc_path", "title")
    assert Orchestrator._clean_identity(None) == ("document_url", "doc_path", "title")


def test_a_repo_that_cannot_honour_the_config_says_so():
    class LegacyRepo:
        """Only the two-column lookup, like a repo written before this."""

        def find_by_identity(self, url, path):
            return None

    o = build(LegacyRepo(), identity=["reference_no"])
    try:
        o.classify_documents([Doc(reference_no="R-1")])
    except NotImplementedError as e:
        assert "reference_no" in str(e)
    else:
        raise AssertionError(
            "a repo without the generic lookup must raise, not classify "
            "every document as new")


# --------------------------------------------------------------------------- #
#  the early exit                                                              #
# --------------------------------------------------------------------------- #

def test_unchanged_inventory_stops_before_processing():
    stored = [{"id": 1, "document_url": "a", "doc_path": ["X"],
               "content_hash": "h1", "source_system": "SRC"}]
    repo = FakeRepo(rows=stored)
    docs = [Doc(document_url="a", doc_path=["X"], content_hash="h1")]
    o = build(repo, FakeCrawler(docs, source_system="SRC"))
    o.check_run_trustworthy = lambda d: (True, [])
    o._inventory_hash = lambda d: "INV"
    repo._last_run = {"inventory_hash": "INV"}
    processed = []
    o._process_docs = lambda todo, name: processed.append(todo)

    report = o.run_for_regulator("REG")
    assert processed == [], "an unchanged inventory must not process anything"
    assert report["processed"] == 0
    assert "skipped" in report
    assert repo.named("record_run") == [], "no run recorded for a no-op"


def test_a_changed_inventory_still_processes():
    repo = FakeRepo(rows=[])
    docs = [Doc(document_url="a", doc_path=["X"], content_hash="h1")]
    o = build(repo, FakeCrawler(docs, source_system="SRC"))
    o.check_run_trustworthy = lambda d: (True, [])
    o._inventory_hash = lambda d: "NEW-INV"
    repo._last_run = {"inventory_hash": "OLD-INV"}
    processed = []
    o._process_docs = lambda todo, name: processed.append(todo)

    report = o.run_for_regulator("REG")
    assert len(processed) == 1 and len(processed[0]) == 1
    assert report["processed"] == 1


def test_pending_work_overrides_an_unchanged_hash():
    """A previous run that died mid-way leaves work behind. The hash is the
    same, but the buckets are not empty, so it must not skip."""
    repo = FakeRepo(rows=[])
    docs = [Doc(document_url="a", doc_path=["X"], content_hash="h1")]
    o = build(repo, FakeCrawler(docs, source_system="SRC"))
    o.check_run_trustworthy = lambda d: (True, [])
    o._inventory_hash = lambda d: "INV"
    repo._last_run = {"inventory_hash": "INV"}
    processed = []
    o._process_docs = lambda todo, name: processed.append(todo)

    report = o.run_for_regulator("REG")
    assert processed, "one new document is pending; the run must not skip"
    assert report["processed"] == 1


# --------------------------------------------------------------------------- #
#  the withdrawal decision on the crawl path                                    #
# --------------------------------------------------------------------------- #

def _absent_run(**kw):
    """A trustworthy run whose stored row is not in the listing."""
    stored = [{"id": 1, "document_url": "gone", "doc_path": [],
               "content_hash": "h1", "source_system": "SRC", "title": "Gone"},
              {"id": 2, "document_url": "here", "doc_path": [],
               "content_hash": "h1", "source_system": "SRC"}]
    repo = FakeRepo(rows=stored)
    # Explicitly None: the fake's __getattr__ fabricates a recorder for any
    # attribute it does not hold, so a `getattr(..., None)` default never fires.
    repo._last_run = None
    docs = [Doc(document_url="here", doc_path=[], content_hash="h1")]
    o = build(repo, FakeCrawler(docs, source_system="SRC"), **kw)
    o.check_run_trustworthy = lambda d: (True, [])
    o._process_docs = lambda todo, name: None
    return o


def test_the_report_carries_a_withdrawals_block():
    report = _absent_run().run_for_regulator("REG")
    assert report["classified"]["disappeared"] == 1
    block = report["withdrawals"]
    assert block["counts"]["withdrawal-proposed"] == 0, \
        "a first run has no history, so it cannot claim two absences"
    assert block["counts"]["not-judged"] == 1
    assert report["disappeared_actioned"] is False


def _seed_streak(root):
    """One prior run, then its streak backdated rather than sleeping 20 hours."""
    _absent_run(change_root=root).run_for_regulator("REG")
    from dynamic_crawler import crawl_absence as ca
    st = ca.store_for("src", root=root)
    # Key format follows the identity tuple, which gained `title` on 2026-08-16.
    st.records["document_url=gone|doc_path=|title=Gone"].update(
        {"signal": ca.SIGNAL, "misses": 1,
         "first_missed": "2020-01-01T00:00:00Z",
         "last_missed": "2020-01-01T00:00:00Z"})
    st.save()


def test_a_second_run_a_day_later_proposes_the_absence():
    root = _STATE / "streak"
    _seed_streak(root)
    block = _absent_run(change_root=root).run_for_regulator("REG")["withdrawals"]
    assert block["counts"]["withdrawal-proposed"] == 1
    assert block["withdrawal-proposed"][0]["url"] == "gone"
    assert block["confirmed"] is False


def test_a_targeted_run_refuses_what_a_full_run_would_propose():
    """A run that walked past pages is not entitled to call anything absent."""
    root = _STATE / "targeted"
    _seed_streak(root)
    o = _absent_run(change_root=root)
    o.crawler.docs.append(Doc(document_url="skipped", doc_path=[],
                              content_hash="", extra_meta={"detail_skipped": True}))
    report = o.run_for_regulator("REG")
    block = report["withdrawals"]
    assert report["targeted_run"]["documents_not_reread"] == 1
    assert block["counts"]["withdrawal-proposed"] == 0
    assert "walked past" in block["watching"][0]["why"]


def test_the_early_exit_still_writes_the_streak_memory():
    """A run that recorded nothing leaves every document unattributed, and an
    unattributed absence can never be judged."""
    root = _STATE / "earlyexit"
    stored = [{"id": 1, "document_url": "a", "doc_path": ["X"],
               "content_hash": "h1", "source_system": "SRC"}]
    repo = FakeRepo(rows=stored)
    docs = [Doc(document_url="a", doc_path=["X"], content_hash="h1")]
    o = build(repo, FakeCrawler(docs, source_system="SRC"), change_root=root)
    o.check_run_trustworthy = lambda d: (True, [])
    o._inventory_hash = lambda d: "INV"
    repo._last_run = {"inventory_hash": "INV"}
    o._process_docs = lambda todo, name: None

    report = o.run_for_regulator("REG")
    assert "skipped" in report
    from dynamic_crawler import crawl_absence as ca
    st = ca.store_for("src", root=root)
    assert st.records["document_url=a|doc_path=X|title="]["signal"] == ca.SIGNAL


def test_the_default_streak_memory_is_the_crawls_own_directory():
    """Not a file a sweep also writes: run_sweep counts an absence for every key
    in the file it opens, and missed() never asks who owns the record."""
    from dynamic_crawler import crawl_absence as ca
    from dynamic_crawler.change_state import DEFAULT_ROOT
    o = _absent_run(change_root=None)
    store = ca.store_for(o.source_name, root=getattr(o, "change_root", None))
    assert store.path.parent == ca.CRAWL_ROOT
    assert store.path.parent != DEFAULT_ROOT


def test_nothing_on_this_path_writes_a_withdrawn_status():
    """`mark_regulation_withdrawn` exists on both repos and is called by nothing."""
    o = _absent_run(change_root=_STATE / "nowrite")
    o.run_for_regulator("REG")
    assert o.repo.named("mark_regulation_withdrawn") == []
    assert o.repo.named("mark_regulation_deleted") == []


# --------------------------------------------------------------------------- #
#  the gate verdict, per source                                                 #
# --------------------------------------------------------------------------- #

class HistoryRepo(FakeRepo):
    """FakeRepo with a working run_history: PASS-only lookup, as both repos are."""

    def __init__(self, rows=None):
        self.history = []
        super().__init__(rows)

    def record_run(self, source, row_count, inventory_hash, verdict, note=""):
        self.calls.append(("record_run",
                           (source, row_count, inventory_hash, verdict, note), {}))
        self.history.append({"source": source, "row_count": row_count,
                             "verdict": verdict, "note": note})

    def last_good_run(self, source):
        runs = [r for r in self.history
                if r["source"] == source and r["verdict"] == "PASS"]
        return runs[-1] if runs else None


def _composite_run(repo, counts, *, blocked=0):
    """One run of a composite: {source label: how many documents it returned}."""
    docs = [Doc(document_url=f"{label}-{i}", doc_path=[], content_hash="h",
                extra_meta={"crawl_source": label})
            for label, n in counts.items() for i in range(n)]
    crawler = FakeCrawler(docs, source_system="SRC")
    crawler.source_names = list(counts)
    if blocked:
        crawler.last_result = {"run": {"blocked_pages": blocked}}
    o = build(repo, crawler)
    o._process_docs = lambda todo, name: None
    return o.run_for_regulator("REG")


def _rows_for(repo, source):
    return [c[1] for c in repo.named("record_run") if c[1][0] == source]


def test_a_broken_sibling_does_not_quarantine_a_healthy_sources_row():
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100, "rulebook": 60})
    report = _composite_run(repo, {"circulars": 100, "rulebook": 20})

    gate = report["gate_by_source"]
    assert gate["rulebook"]["baseline_verdict"] == "QUARANTINED"
    assert gate["circulars"]["baseline_verdict"] == "PASS", (
        "the run's verdict was stamped on a source that was inside tolerance")
    assert report["run_trustworthy"] is False, "the run itself is still distrusted"


def test_a_healthy_sources_baseline_survives_a_broken_sibling():
    """The reason the verdict is per source: last_good_run is PASS-only, so a
    frozen baseline makes a healthy source fail its own count check later."""
    repo = HistoryRepo()
    reports = [_composite_run(repo, {"circulars": circulars, "rulebook": rulebook})
               for circulars, rulebook in ((100, 60), (104, 20), (108, 60),
                                           (112, 60))]
    assert repo.last_good_run("src/circulars")["row_count"] == 112, (
        "the baseline must track the source, not the last run the whole "
        "regulator passed")
    assert [r["gate_by_source"]["circulars"]["baseline_verdict"] for r in reports] == \
        ["PASS"] * 4, "circulars grew 4% a run and never left tolerance"


def test_a_run_wide_problem_quarantines_every_sources_row():
    repo = HistoryRepo()
    report = _composite_run(repo, {"circulars": 100, "rulebook": 60}, blocked=3)
    for label, entry in report["gate_by_source"].items():
        assert entry["baseline_verdict"] == "QUARANTINED", label
        assert "bot-protection" in entry["problems"][0]


def test_a_quarantined_sources_row_records_why():
    """It used to keep only problems prefixed with the source's own name, so a
    row quarantined by a run-wide problem was written with an empty reason."""
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100, "rulebook": 60}, blocked=3)
    assert all("bot-protection" in row[4]
               for row in _rows_for(repo, "src/circulars"))


def test_a_total_count_problem_spares_a_source_that_passed_its_own_check():
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100, "rulebook": 60})
    report = _composite_run(repo, {"circulars": 100, "rulebook": 60,
                                   "sandbox": 20})

    assert any(p.startswith("total:") for p in report["gate_problems"]), \
        "160 -> 180 must move the total out of tolerance"
    for label in ("circulars", "rulebook"):
        assert report["gate_by_source"][label]["baseline_verdict"] == "PASS", (
            f"{label} was checked against its own baseline and passed")


def test_a_total_count_problem_still_blocks_a_source_with_no_baseline():
    """A source that was never checked has only the total as evidence — and a
    short run must not be allowed to set its first baseline."""
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100, "rulebook": 60})
    report = _composite_run(repo, {"circulars": 100, "rulebook": 60,
                                   "sandbox": 20})

    entry = report["gate_by_source"]["sandbox"]
    assert entry["baseline_verdict"] == "QUARANTINED"
    assert entry["problems"][0].startswith("total:")
    assert repo.last_good_run("src/sandbox") is None


def test_the_aggregate_row_is_recorded_for_the_whole_run():
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100, "rulebook": 60})
    _composite_run(repo, {"circulars": 100, "rulebook": 20})
    assert [row[3] for row in _rows_for(repo, "src")] == ["PASS", "QUARANTINED"]


def test_a_single_source_run_still_writes_exactly_one_row():
    repo = HistoryRepo()
    report = _composite_run(repo, {"circulars": 100})
    assert len(repo.named("record_run")) == 1
    assert "gate_by_source" not in report


# --------------------------------------------------------------------------- #
#  which counts may become the baseline                                         #
# --------------------------------------------------------------------------- #

def test_a_count_that_grew_becomes_the_baseline_though_the_run_is_distrusted():
    """Two questions, one column. A run distrusted for a rise is still the best
    record of what the source now holds, and refusing to remember it is what
    made the gate re-detect the same step change for ever."""
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100})
    report = _composite_run(repo, {"circulars": 130})

    assert report["run_trustworthy"] is False, "a 30% jump is still a surprise"
    assert report["baseline_verdict"] == "PASS"
    assert repo.last_good_run("src")["row_count"] == 130


def test_a_step_change_stops_being_reported_on_the_next_run():
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100})
    _composite_run(repo, {"circulars": 130})
    report = _composite_run(repo, {"circulars": 130})
    assert report["gate_problems"] == [], "the gate must not re-detect it for ever"
    assert report["run_trustworthy"] is True


def test_a_count_that_fell_never_becomes_the_baseline_however_often_it_repeats():
    """SDAIA returned 415/363/439 on three runs of identical code. A low prior
    is what opens the withdrawal gate, so a drop waits for a person."""
    repo = HistoryRepo()
    _composite_run(repo, {"laws": 415})
    for _ in range(3):
        report = _composite_run(repo, {"laws": 363})
        assert report["baseline_verdict"] == "QUARANTINED"
    assert repo.last_good_run("src")["row_count"] == 415


def test_a_rise_does_not_excuse_a_problem_of_another_kind():
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100})
    report = _composite_run(repo, {"circulars": 130}, blocked=2)
    assert report["baseline_verdict"] == "QUARANTINED"
    assert repo.last_good_run("src")["row_count"] == 100


def test_a_new_source_takes_a_baseline_only_from_a_run_with_nothing_against_it():
    """The rise argument is about raising a prior, not inventing a first one:
    the run that carries the total problem must not set sandbox's baseline."""
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100, "rulebook": 60})
    first = _composite_run(repo, {"circulars": 100, "rulebook": 60,
                                  "sandbox": 20})
    assert first["gate_by_source"]["sandbox"]["baseline_verdict"] == "QUARANTINED"
    assert repo.last_good_run("src/sandbox") is None

    second = _composite_run(repo, {"circulars": 100, "rulebook": 60,
                                   "sandbox": 20})
    assert second["gate_problems"] == [], "the total settled at 180"
    assert second["gate_by_source"]["sandbox"]["baseline_verdict"] == "PASS"
    assert repo.last_good_run("src/sandbox")["row_count"] == 20


def test_one_source_growing_does_not_hand_a_sibling_a_baseline_it_lost():
    """The rise is rulebook's; circulars fell at the same time and must not be
    carried through on rulebook's problem."""
    repo = HistoryRepo()
    _composite_run(repo, {"circulars": 100, "rulebook": 60})
    report = _composite_run(repo, {"circulars": 80, "rulebook": 130})

    gate = report["gate_by_source"]
    assert gate["rulebook"]["baseline_verdict"] == "PASS"
    assert gate["circulars"]["baseline_verdict"] == "QUARANTINED"
    assert repo.last_good_run("src/circulars")["row_count"] == 100
    assert repo.last_good_run("src/rulebook")["row_count"] == 130


# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  ok    {name}")
            except Exception as e:
                failed += 1
                print(f"  FAIL  {name}: {e}")
    print(f"\n{failed} failed" if failed else "\nall passed")
    sys.exit(1 if failed else 0)
