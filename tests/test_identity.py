"""Per-source identity: what counts as "the same document" belongs to the source.

WHY THIS FILE LOOKS LIKE THIS

Same reason as the other suites here — importing the orchestrator pulls in the
OCR stack, which is gigabytes and is not needed to check classification. The
heavy modules are stubbed before the import. No network and no database.

What is verified here:

  per-source identity   two sources in one run match on different fields
  empty identity        a configured field that is blank on every document
                        aborts the run instead of merging them
  per-source tiebreak   version_key comes from the source, and `null` disables
                        the whole-store reference lookup
  stored inventory      `disappeared` reads every source_system the run covers,
                        not one attribute a composite does not have
  per-source history    a source that returned nothing is visible in the gate
                        instead of hiding inside the regulator's tolerance

    venv/Scripts/python.exe -m pytest tests/test_identity.py -v
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

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

from crawler.generic_crawler_wrapper import (                        # noqa: E402
    CompositeCrawler, _source_options, build_regulator_crawler)
from orchestrator.orchestrator import Orchestrator            # noqa: E402


# --------------------------------------------------------------------------- #
#  doubles                                                                     #
# --------------------------------------------------------------------------- #

class Doc:
    def __init__(self, **kw):
        self.document_url = kw.pop("document_url", "")
        self.doc_path = kw.pop("doc_path", [])
        self.content_hash = kw.pop("content_hash", "")
        self.extra_meta = kw.pop("extra_meta", {})
        self.title = kw.pop("title", "a document")
        self.reference_no = kw.pop("reference_no", None)
        for k, v in kw.items():
            setattr(self, k, v)


class FakeCrawler:
    """One source. Returns what it was given."""

    def __init__(self, docs, source_system="SRC", seed_url="https://x/a"):
        self.docs = docs
        self.source_system = source_system
        self.seed_url = seed_url

    def fetch_documents(self, limit=None):
        return list(self.docs)


class BrokenCrawler:
    source_system = "BROKEN"
    seed_url = "https://x/broken"

    def fetch_documents(self, limit=None):
        raise RuntimeError("site down")


class FakeRepo:
    """Enough repo to classify: both identity lookups, reference, source, runs."""

    def __init__(self, rows=None, runs=None):
        self.rows = rows or []
        self.runs = runs or {}
        self.recorded = []
        self.reference_lookups = []
        self.source_lookups = []

    @staticmethod
    def _path(value):
        return " > ".join(value) if isinstance(value, (list, tuple)) else str(value)

    def find_by_identity(self, url, path):
        for r in self.rows:
            if (r.get("document_url") == url
                    and self._path(r.get("doc_path")) == self._path(path)):
                return dict(r)
        return None

    def find_by_identity_fields(self, fields):
        for r in self.rows:
            if all(self._path(r.get(k)) == v for k, v in fields.items()):
                return dict(r)
        return None

    def find_by_reference(self, ref):
        self.reference_lookups.append(ref)
        return next((dict(r) for r in self.rows if r.get("reference_no") == ref), None)

    def find_regulations_by_source(self, source, regulator=None):
        self.source_lookups.append((source, regulator))
        if not source:
            return []
        return [dict(r) for r in self.rows
                if r.get("source_system") == source
                and (not regulator or r.get("regulator") == regulator)]

    def update_regulation(self, regulation_id, **fields):
        pass

    def record_run(self, source, row_count, inventory_hash, verdict="PASS",
                   problems=""):
        self.recorded.append({"source": source, "row_count": row_count,
                              "inventory_hash": inventory_hash,
                              "verdict": verdict, "problems": problems})

    def last_good_run(self, source):
        return self.runs.get(source)


def orch(repo, crawler=None, **kw):
    return Orchestrator(crawler=crawler or _Any(), repo=repo,
                           source_name="source:TEST", **kw)


def stored(**kw):
    # `title` matches Doc's default deliberately: it joined the identity tuple on
    # 2026-08-16, and a stored row without one does not exist in the real
    # library — every crawler sets a title. Omitting it here made the fixture,
    # not the code, the reason a document failed to match.
    row = {"id": 1, "document_url": "", "doc_path": [], "content_hash": "H",
           "title": "a document",
           "reference_no": None, "source_system": "SRC", "extra_meta": {}}
    row.update(kw)
    return row


# --------------------------------------------------------------------------- #
#  reading the config                                                          #
# --------------------------------------------------------------------------- #

def test_source_identity_wins_over_the_regulator_default():
    opts = _source_options({"name": "grid", "identity": ["reference_no"]},
                           {"identity": ["document_url"]})
    assert opts["identity"] == ["reference_no"]


def test_regulator_identity_is_the_fallback():
    opts = _source_options({"name": "walk"}, {"identity": ["document_url"]})
    assert opts["identity"] == ["document_url"]


def test_no_identity_anywhere_stamps_nothing():
    """Every config today. The document must carry no override at all, so the
    orchestrator's own default applies."""
    assert "identity" not in _source_options({"name": "walk"}, {})


def test_null_version_key_is_kept_and_not_read_as_unset():
    """`version_key: null` disables the tiebreak — a different instruction from
    not mentioning it, which `or` would flatten into the same thing."""
    opts = _source_options({"name": "walk", "version_key": None},
                           {"version_key": "reference_no"})
    assert opts["version_key"] is None
    assert "version_key" in opts


# --------------------------------------------------------------------------- #
#  the composite stamps each source's settings on its own documents            #
# --------------------------------------------------------------------------- #

def test_each_source_stamps_only_its_own_documents():
    a, b = Doc(title="grid row"), Doc(title="walked page")
    c = CompositeCrawler(
        [FakeCrawler([a], "GRID"), FakeCrawler([b], "WALK")],
        [{"name": "grid", "identity": ["reference_no"]}, {"name": "walk"}])
    c.fetch_documents()
    assert a.extra_meta["identity_fields"] == ["reference_no"]
    assert a.extra_meta["crawl_source"] == "grid"
    assert "identity_fields" not in b.extra_meta
    assert b.extra_meta["crawl_source"] == "walk"


def test_source_systems_covers_every_source_including_nested():
    inner = CompositeCrawler([FakeCrawler([], "INNER")])
    c = CompositeCrawler([FakeCrawler([], "OUTER"), inner])
    assert c.source_systems == ["OUTER", "INNER"]


def test_source_names_lists_a_source_that_produced_nothing():
    c = CompositeCrawler([FakeCrawler([], "A"), BrokenCrawler()],
                         [{"name": "works"}, {"name": "broken"}])
    assert c.fetch_documents() == []
    assert c.source_names == ["works", "broken"]


def test_a_failing_source_does_not_take_the_others_down():
    kept = Doc(title="kept")
    c = CompositeCrawler([BrokenCrawler(), FakeCrawler([kept], "A")],
                         [{"name": "broken"}, {"name": "works"}])
    assert [d.title for d in c.fetch_documents()] == ["kept"]


# --------------------------------------------------------------------------- #
#  classification honours the per-document identity                            #
# --------------------------------------------------------------------------- #

def test_two_sources_match_on_different_fields_in_one_run():
    """The case the single per-regulator setting could not express: a circular
    keyed on its reference number and an article keyed on its url, together."""
    repo = FakeRepo([
        stored(id=1, reference_no="C-42", document_url="https://x/old.pdf",
               doc_path=["Circulars"]),
        stored(id=2, document_url="https://x/article-3", doc_path=["Law", "3"]),
    ])
    circular = Doc(title="Circular 42", reference_no="C-42",
                   document_url="https://x/NEW-address.pdf",
                   doc_path=["Circulars"], content_hash="H",
                   extra_meta={"identity_fields": ["reference_no"]})
    article = Doc(title="Article 3", document_url="https://x/article-3",
                  doc_path=["Law", "3"], content_hash="H", extra_meta={})

    buckets = orch(repo).classify_documents([circular, article])

    # The circular matched on its reference number despite the new url, and the
    # article matched on url+path despite having no reference number.
    assert buckets["new"] == []
    assert len(buckets["unchanged"]) == 2


def test_identity_empty_on_every_document_aborts_the_run():
    """The failure this guard exists for: `identity: [reference_no]` against
    articles that have none gives them all the SAME identity."""
    repo = FakeRepo()
    docs = [Doc(title=f"Article {i}", document_url=f"https://x/{i}",
                extra_meta={"identity_fields": ["reference_no"]})
            for i in range(3)]
    with pytest.raises(ValueError) as e:
        orch(repo).classify_documents(docs)
    assert "3 of 3" in str(e.value)
    assert "reference_no" in str(e.value)


def test_one_usable_field_is_enough():
    repo = FakeRepo()
    doc = Doc(title="Article 3", document_url="https://x/3", doc_path=[],
              extra_meta={"identity_fields": ["document_url", "doc_path"]})
    assert len(orch(repo).classify_documents([doc])["new"]) == 1


def test_a_repo_without_the_generic_lookup_says_so():
    class OldRepo(FakeRepo):
        find_by_identity_fields = None

    doc = Doc(title="x", reference_no="C-1",
              extra_meta={"identity_fields": ["reference_no"]})
    with pytest.raises(NotImplementedError):
        orch(OldRepo()).classify_documents([doc])


# --------------------------------------------------------------------------- #
#  the tiebreak is per source too                                              #
# --------------------------------------------------------------------------- #

def test_version_key_comes_from_the_source():
    repo = FakeRepo([stored(id=5, reference_no="C-9",
                            document_url="https://x/old.pdf")])
    doc = Doc(title="Circular 9", reference_no="C-9",
              document_url="https://x/new.pdf", content_hash="H",
              extra_meta={"version_key": "reference_no"})
    buckets = orch(repo, version_key=None).classify_documents([doc])
    assert buckets["new"] == []
    assert repo.reference_lookups == ["C-9"]


def test_null_version_key_switches_the_whole_store_lookup_off():
    """find_by_reference searches every source, so a number that is unique only
    within one source must not be allowed to drive it."""
    repo = FakeRepo([stored(id=5, reference_no="1", document_url="https://x/a")])
    doc = Doc(title="Item 1", reference_no="1", document_url="https://x/b",
              extra_meta={"version_key": None})
    buckets = orch(repo, version_key="reference_no").classify_documents([doc])
    assert len(buckets["new"]) == 1
    assert repo.reference_lookups == []


# --------------------------------------------------------------------------- #
#  what the run compares itself against                                        #
# --------------------------------------------------------------------------- #

def test_stored_inventory_covers_every_source_of_a_composite():
    """The bug: a composite has no `source_system`, so the lookup went out as
    None, both repos answer [] to that, and `disappeared` was always empty."""
    repo = FakeRepo([stored(id=1, source_system="A"), stored(id=2, source_system="B")])
    crawler = CompositeCrawler([FakeCrawler([], "A"), FakeCrawler([], "B")])
    rows = orch(repo, crawler=crawler)._stored_for_source()
    assert sorted(r["id"] for r in rows) == [1, 2]


def test_a_document_stored_by_two_sources_is_listed_once():
    repo = FakeRepo([stored(id=1, source_system="A")])
    crawler = CompositeCrawler([FakeCrawler([], "A"), FakeCrawler([], "A")])
    assert len(orch(repo, crawler=crawler)._stored_for_source()) == 1


def test_a_crawler_with_no_source_system_reports_the_gate_is_inert(caplog):
    """A zero here means "not measured". It must say so."""
    repo = FakeRepo([stored(id=1, source_system="A")])
    with caplog.at_level("WARNING"):
        assert orch(repo)._stored_for_source() == []
    assert "gate is inert" in caplog.text


def test_the_stored_inventory_is_scoped_to_the_regulator_the_documents_carry():
    """`source_system` is not unique: AML and SIMAH both publish under "Rules
    and Regulations". Unscoped, an AML run offers SIMAH's library up as
    disappeared — and a trustworthy run would withdraw it."""
    repo = FakeRepo([
        stored(id=1, source_system="Rules and Regulations", regulator="AML"),
        stored(id=2, source_system="Rules and Regulations", regulator="SIMAH"),
    ])
    crawler = CompositeCrawler([FakeCrawler([], "Rules and Regulations")])
    docs = [Doc(document_url="https://x/1", regulator="AML")]
    rows = orch(repo, crawler=crawler)._stored_for_source(docs)
    assert [r["id"] for r in rows] == [1]
    assert repo.source_lookups == [("Rules and Regulations", "AML")]


def test_documents_that_name_no_regulator_fall_back_to_the_source_alone():
    """Every existing run: the crawlers do not all set it, and an empty scope
    must not turn the gate off."""
    repo = FakeRepo([stored(id=1, source_system="A", regulator="AML"),
                     stored(id=2, source_system="A", regulator="SIMAH")])
    crawler = CompositeCrawler([FakeCrawler([], "A")])
    rows = orch(repo, crawler=crawler)._stored_for_source([Doc(document_url="u")])
    assert sorted(r["id"] for r in rows) == [1, 2]
    assert repo.source_lookups == [("A", None)]


def test_a_run_carrying_two_regulators_is_not_scoped_to_either():
    repo = FakeRepo([stored(id=1, source_system="A", regulator="AML")])
    crawler = CompositeCrawler([FakeCrawler([], "A")])
    docs = [Doc(document_url="1", regulator="AML"),
            Doc(document_url="2", regulator="SIMAH")]
    assert orch(repo, crawler=crawler)._stored_for_source(docs)
    assert repo.source_lookups == [("A", None)]


def test_a_regulator_name_that_matches_nothing_says_why_the_bucket_is_empty(caplog):
    """The safe answer — nothing can be withdrawn from an empty bucket — but it
    must not be a silent one. A display name a word off from the stored value
    would otherwise look exactly like a source that holds nothing."""
    repo = FakeRepo([stored(id=1, source_system="A",
                            regulator="Anti-Money Laundering Permanent Committee")])
    crawler = CompositeCrawler([FakeCrawler([], "A")])
    docs = [Doc(document_url="u", regulator="AML")]
    with caplog.at_level("WARNING"):
        assert orch(repo, crawler=crawler)._stored_for_source(docs) == []
    assert "does not match the stored one" in caplog.text


def test_inventory_hash_distinguishes_identities_on_different_fields():
    o = orch(FakeRepo())
    by_url = Doc(document_url="X", doc_path=[])
    by_ref = Doc(reference_no="X", extra_meta={"identity_fields": ["reference_no"]})
    assert o._inventory_hash([by_url]) != o._inventory_hash([by_ref])


# --------------------------------------------------------------------------- #
#  run history, per source                                                     #
# --------------------------------------------------------------------------- #

def test_a_source_that_returned_nothing_fails_the_gate():
    """94 + 8 documents last run, 94 + 0 this run. Against the regulator total
    that is 7.8% and the tolerance is 5% — but it is 100% of one source, and
    without the per-source row a bigger regulator hides it completely."""
    repo = FakeRepo(runs={
        "source:TEST": {"row_count": 102, "inventory_hash": "x"},
        "source:TEST/works": {"row_count": 94, "inventory_hash": "y"},
        "source:TEST/broken": {"row_count": 8, "inventory_hash": "z"},
    })
    docs = [Doc(document_url=f"https://x/{i}",
                extra_meta={"crawl_source": "works"}) for i in range(94)]
    crawler = CompositeCrawler([FakeCrawler(docs, "A"), BrokenCrawler()],
                               [{"name": "works"}, {"name": "broken"}])
    trustworthy, problems = orch(repo, crawler=crawler).check_run_trustworthy(docs)
    assert not trustworthy
    assert any(p.startswith("broken: count moved 8 -> 0") for p in problems)


def test_a_single_source_run_records_one_row_only():
    """No per-source duplicate when there is only one source: that is every
    formfill run, and its history must not change shape."""
    # Stored already and unchanged, so the run records history without processing
    # anything — this is about the shape of run_history, not about ingestion.
    repo = FakeRepo([stored(id=1, document_url="https://x/1", content_hash="H",
                            source_system="A")])
    docs = [Doc(document_url="https://x/1", content_hash="H")]
    crawler = CompositeCrawler([FakeCrawler(docs, "A")], [{"name": "only"}])
    o = orch(repo, crawler=crawler)
    o.run_for_regulator("TEST")
    assert [r["source"] for r in repo.recorded] == ["source:TEST"]


def test_history_key_stays_within_the_column():
    o = orch(FakeRepo())
    assert len(o._history_key("x" * 400)) == 200


# --------------------------------------------------------------------------- #
#  a regulator that is off on purpose                                          #
# --------------------------------------------------------------------------- #

SOURCES_DIR = Path(__file__).resolve().parents[1] / "config" / "sources"


def _shipped_configs():
    import yaml
    return {f: yaml.safe_load(f.read_text(encoding="utf-8")) or {}
            for f in sorted(SOURCES_DIR.glob("*.yml"))}


def test_a_disabled_regulator_refuses_and_says_why():
    with pytest.raises(ValueError) as e:
        build_regulator_crawler({"regulator": "SIMAH", "sources": [],
                                 "disabled": "blocked until September"})
    assert "blocked until September" in str(e.value)


def test_disabled_wins_over_a_source_list_someone_left_behind():
    """Otherwise re-enabling is one uncommented block away from a live crawl of
    a host we may not touch."""
    with pytest.raises(ValueError) as e:
        build_regulator_crawler({
            "regulator": "SIMAH", "disabled": "blocked",
            "sources": [{"name": "x", "mode": "custom",
                         "crawler_class": "crawler.nope.Nope"}]})
    assert "disabled on purpose" in str(e.value)


def test_an_empty_list_is_refused_and_points_at_the_key():
    """An empty list cannot be told from an unfinished file, so the message has
    to name the way to say 'off on purpose'."""
    with pytest.raises(ValueError) as e:
        build_regulator_crawler({"regulator": "TEST", "sources": []})
    assert "disabled:" in str(e.value)


def test_every_shipped_config_lists_sources_or_says_why_not():
    """The invariant: nothing in config/sources/ parses to nothing in silence."""
    for path, cfg in _shipped_configs().items():
        assert cfg.get("sources") or cfg.get("disabled"), (
            f"{path.name} has neither sources nor a `disabled:` reason")


def test_simah_cannot_reach_a_host_we_are_blocked_from():
    """SIMAH was enabled on 2026-08-13; `disabled:` is gone and a source exists.

    So the guarantee moved. It is no longer "nothing is configured" — it is
    `allow_live: false`, which makes the crawler provably incapable of
    generating traffic whatever its snapshot clock says. simah.com is still
    Cloudflare 1020-blocked and in change_signals.yml skip_hosts until
    2026-09-04; flipping this is a decision about the block, not a code change.
    """
    cfg = _shipped_configs()[SOURCES_DIR / "simah.yml"]
    sources = cfg.get("sources") or []
    assert len(sources) == 1, "enabled, so the source must be present"
    assert sources[0]["init_kwargs"]["allow_live"] is False, (
        "allow_live is the only thing stopping traffic to a blocked host")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
