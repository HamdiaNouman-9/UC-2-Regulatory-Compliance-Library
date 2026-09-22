"""A targeted re-crawl: open the detail pages a sweep shortlisted, and no others.

WHY THIS FILE LOOKS LIKE THIS

Same reason as the other suites here — importing the orchestrator pulls in the
OCR stack, which is gigabytes and is not needed to check classification. The
heavy modules are stubbed before the import. No network, no browser, no
database.

The rules that decide what a targeted run does NOT read are pure functions
(`_split_targets`, `_stamp_hashes`) precisely so they can be tested without a
browser. Both can empty a library if they are wrong.

What is verified here:

  the split         which rows phase 2 opens; a tree is refused, not sliced
  the record hash   an unopened row is never hashed from the LISTING text
  the classifier    an unopened document is `not_reread` — never `modified`,
                    never counted absent, and neither are its attachments
  the report        `targets` is the modified urls only, and an empty file is
                    still written

    venv/Scripts/python.exe -m pytest tests/test_targeted_recrawl.py -v
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

from orchestrator.orchestrator import Orchestrator             # noqa: E402
from dynamic_crawler.formfill.runner import (                         # noqa: E402
    _split_targets, _stamp_hashes, _url_key, content_key)


# --------------------------------------------------------------------------- #
#  doubles                                                                     #
# --------------------------------------------------------------------------- #

class Doc:
    def __init__(self, **kw):
        self.document_url = kw.pop("document_url", "")
        self.source_page_url = kw.pop("source_page_url", "")
        self.doc_path = kw.pop("doc_path", [])
        self.content_hash = kw.pop("content_hash", "")
        self.extra_meta = kw.pop("extra_meta", {})
        self.title = kw.pop("title", "a document")
        self.regulator = kw.pop("regulator", "REG")
        self.reference_no = kw.pop("reference_no", None)
        for k, v in kw.items():
            setattr(self, k, v)


class FakeCrawler:
    source_system = "SRC"
    seed_url = "https://x/a"

    def fetch_documents(self, limit=None):
        return []


class FakeRepo:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.updated = []

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
        return next((dict(r) for r in self.rows if r.get("reference_no") == ref), None)

    def find_regulations_by_source(self, source, regulator=None):
        if not source:
            return []
        return [dict(r) for r in self.rows
                if r.get("source_system") == source
                and (not regulator or r.get("regulator") == regulator)]

    def update_regulation(self, regulation_id, **fields):
        self.updated.append((regulation_id, fields))


def stored(**kw):
    # `title` matches Doc's default: it joined the identity tuple on 2026-08-16,
    # and a stored row without one does not exist in the real library. Without it
    # the fixture — not the code — is why a document fails to match its row, and
    # every unmatched row then lands in `disappeared`.
    row = {"id": 1, "document_url": "", "source_page_url": "", "doc_path": [],
           "content_hash": "H", "reference_no": None, "source_system": "SRC",
           "title": "a document",
           "regulator": "REG", "extra_meta": {}}
    row.update(kw)
    return row


def classify(rows, docs):
    return _classify(rows, docs)[0]


def _classify(rows, docs):
    """(buckets, orchestrator) — the stored-row count lives on the second."""
    o = Orchestrator(crawler=FakeCrawler(), repo=FakeRepo(rows),
                        source_name="source:TEST")
    return o.classify_documents(docs), o


def row(href, **kw):
    r = {"href": href, "title": "t", "row_text": "listing text"}
    r.update(kw)
    return r


# --------------------------------------------------------------------------- #
#  the split — which rows phase 2 opens                                        #
# --------------------------------------------------------------------------- #

def test_no_targeting_opens_every_row():
    """An ordinary crawl must reach exactly the code it reached before."""
    rows = [row("https://x/1"), row("https://x/2")]
    targets, skipped, warns = _split_targets(rows, None, is_tree=False)
    assert targets == rows and skipped == [] and warns == []


def test_targeting_opens_only_the_named_rows():
    rows = [row("https://x/1"), row("https://x/2"), row("https://x/3")]
    targets, skipped, warns = _split_targets(rows, {"https://x/2"}, is_tree=False)
    assert [r["href"] for r in targets] == ["https://x/2"]
    assert [r["href"] for r in skipped] == ["https://x/1", "https://x/3"]
    assert warns == []


def test_every_row_is_either_opened_or_recorded_as_skipped():
    """No row may fall out of a targeted run: one that does is a row the
    completeness gate reports as disappeared."""
    rows = [row(f"https://x/{i}") for i in range(20)]
    targets, skipped, _ = _split_targets(rows, {"https://x/3", "https://x/9"},
                                         is_tree=False)
    assert len(targets) + len(skipped) == len(rows)


def test_a_trailing_slash_still_matches():
    rows = [row("https://x/doc/")]
    targets, _, _ = _split_targets(rows, {_url_key("https://x/doc")}, is_tree=False)
    assert len(targets) == 1


def test_a_fragment_is_part_of_the_identity():
    """SDAIA files four documents at #page=N of one PDF. Dropping the fragment
    would re-crawl all four when the sweep named one."""
    rows = [row("https://x/f.pdf#page=1"), row("https://x/f.pdf#page=2")]
    targets, skipped, _ = _split_targets(rows, {"https://x/f.pdf#page=2"},
                                         is_tree=False)
    assert [r["href"] for r in targets] == ["https://x/f.pdf#page=2"]
    assert len(skipped) == 1


def test_a_tree_is_crawled_whole_and_says_so():
    """On a tree phase 2 IS the discovery walk — targeting it stops the walk
    finding deeper nodes, the same reason --max-details is refused there."""
    rows = [row("https://x/1"), row("https://x/2")]
    targets, skipped, warns = _split_targets(rows, {"https://x/1"}, is_tree=True)
    assert targets == rows and skipped == []
    assert warns and "tree" in warns[0]


def test_an_empty_target_list_opens_nothing_and_does_not_warn():
    """"Nothing changed" is the normal answer, not a mismatch."""
    rows = [row("https://x/1"), row("https://x/2")]
    targets, skipped, warns = _split_targets(rows, set(), is_tree=False)
    assert targets == [] and len(skipped) == 2 and warns == []


def test_urls_that_match_no_row_are_loud():
    rows = [row("https://x/1")]
    targets, _, warns = _split_targets(rows, {"https://other/9"}, is_tree=False)
    assert targets == []
    assert warns and "matched NO row" in warns[0]


# --------------------------------------------------------------------------- #
#  the record hash — the listing is not the document                           #
# --------------------------------------------------------------------------- #

def test_an_unopened_row_is_not_hashed_from_the_listing():
    """The fallback hashes `row_text` when there is no detail text. On an
    unopened row that is the LISTING — a different hash from the one a full
    crawl stored, which reads as an edit on every targeted run."""
    recs = [{"text": "", "row_text": "listing text", "detail_skipped": True}]
    _stamp_hashes(recs)
    assert recs[0]["content_hash"] == ""


def test_an_opened_row_hashes_exactly_as_before():
    """The regression guard: ordinary records must be unaffected."""
    recs = [{"text": "the detail page", "row_text": "listing text"},
            {"text": "", "row_text": "listing text"}]
    _stamp_hashes(recs)
    assert recs[0]["content_hash"] == content_key("the detail page")
    assert recs[1]["content_hash"] == content_key("listing text")


def test_the_listing_hash_is_not_the_detail_hash():
    """Why the test above matters: these two are different values, so an
    unopened row hashed from the listing would classify modified."""
    assert content_key("listing text") != content_key("the detail page")


# --------------------------------------------------------------------------- #
#  the classifier — unopened is not modified, and not absent                   #
# --------------------------------------------------------------------------- #

def test_an_unopened_document_is_never_modified():
    """The whole point. Classified modified, B2's refresh writes the empty page
    over the stored one and archives the live analysis."""
    rows = [stored(id=1, document_url="https://x/1", content_hash="H")]
    doc = Doc(document_url="https://x/1", content_hash="",
              extra_meta={"detail_skipped": True})
    buckets = classify(rows, [doc])
    assert buckets["modified"] == []
    assert [d.document_url for d in buckets["not_reread"]] == ["https://x/1"]


def test_an_unopened_document_is_not_counted_absent():
    rows = [stored(id=1, document_url="https://x/1")]
    doc = Doc(document_url="https://x/1", extra_meta={"detail_skipped": True})
    buckets = classify(rows, [doc])
    assert buckets["disappeared"] == []


def test_an_attachment_of_an_unopened_page_is_not_absent():
    """A targeted run never opens the page, so it never discovers the PDFs
    hanging off it. Those stored rows are absent from the run because nothing
    looked, which is not the same as gone from the site."""
    rows = [stored(id=1, document_url="https://x/page"),
            stored(id=2, document_url="https://x/a.pdf",
                   source_page_url="https://x/page")]
    doc = Doc(document_url="https://x/page", source_page_url="https://x/page",
              extra_meta={"detail_skipped": True})
    buckets, o = _classify(rows, [doc])
    assert buckets["disappeared"] == []
    assert o._not_reread_stored == 1


def test_the_bucket_holds_documents_only():
    """One type per bucket. The `missing`-bucket-of-strings is what made
    `getattr(o, "title", "")` return the built-in METHOD and crash the CLI."""
    rows = [stored(id=1, document_url="https://x/page"),
            stored(id=2, document_url="https://x/a.pdf",
                   source_page_url="https://x/page")]
    doc = Doc(document_url="https://x/page", source_page_url="https://x/page",
              extra_meta={"detail_skipped": True})
    buckets, _ = _classify(rows, [doc])
    assert all(hasattr(d, "document_url") for d in buckets["not_reread"])
    assert not any(isinstance(d, dict) for d in buckets["not_reread"])


def test_a_document_from_a_page_that_was_opened_still_disappears():
    """The exclusion is not blanket: a document the run genuinely did not find,
    on a page it DID open, is still absent."""
    rows = [stored(id=1, document_url="https://x/1"),
            stored(id=2, document_url="https://x/gone")]
    doc = Doc(document_url="https://x/1", content_hash="H")
    buckets = classify(rows, [doc])
    assert [r["id"] for r in buckets["disappeared"]] == [2]


def test_an_ordinary_run_counts_no_stored_rows_held_back():
    rows = [stored(id=1, document_url="https://x/1", content_hash="H"),
            stored(id=2, document_url="https://x/gone")]
    _, o = _classify(rows, [Doc(document_url="https://x/1", content_hash="H")])
    assert o._not_reread_stored == 0


def test_an_ordinary_run_never_fills_not_reread():
    """No document carries the marker unless a targeted run set it, so every
    existing run reaches identical behaviour."""
    rows = [stored(id=1, document_url="https://x/1", content_hash="OLD")]
    doc = Doc(document_url="https://x/1", content_hash="NEW")
    buckets = classify(rows, [doc])
    assert buckets["not_reread"] == []
    assert [d.document_url for d in buckets["modified"]] == ["https://x/1"]


def test_a_targeted_run_still_classifies_the_documents_it_did_open():
    """Targeting narrows what is read, never what is judged from a real read."""
    rows = [stored(id=1, document_url="https://x/1", content_hash="OLD"),
            stored(id=2, document_url="https://x/2", content_hash="H")]
    opened = Doc(document_url="https://x/1", content_hash="NEW")
    walked_past = Doc(document_url="https://x/2",
                      extra_meta={"detail_skipped": True})
    buckets = classify(rows, [opened, walked_past])
    assert [d.document_url for d in buckets["modified"]] == ["https://x/1"]
    assert len(buckets["not_reread"]) == 1
    assert buckets["disappeared"] == []


# --------------------------------------------------------------------------- #
#  the report — what the crawl is handed                                       #
# --------------------------------------------------------------------------- #

def _report(monkeypatch, buckets):
    """Run the CLI's report builder over prepared buckets, with no sweep."""
    from dynamic_crawler.cli import sweep as S
    from dynamic_crawler import changesignal as cs

    full = {cs.NEW: [], cs.MODIFIED: [], cs.UNCHANGED: [], cs.UNKNOWN: [],
            cs.MISSING: []}
    full.update(buckets)

    class Store:
        source = "REG/SRC"
        path = "state.json"

        def keys(self):
            return {"k"}

        def save(self):
            return "state.json"

    monkeypatch.setattr(S.cs, "run_sweep",
                        lambda signal, store: ({"counts": {}}, full))
    monkeypatch.setattr(S.ChangeStateStore, "for_source",
                        classmethod(lambda cls, source, root=None: Store()))
    monkeypatch.setattr(S.withdrawal, "proposals",
                        lambda signal, store, report, buckets: {})
    return S._run(_Any(), "REG/SRC", None, dry_run=True)


def _obs(key, url, title="t"):
    from dynamic_crawler.changesignal import Observation
    return Observation(key=key, url=url, title=title)


def test_targets_are_the_modified_urls_only(monkeypatch):
    """`new` on a detect-only sweep means the first sweep of a document we
    already store — re-crawling on it re-reads the whole source."""
    from dynamic_crawler import changesignal as cs
    report = _report(monkeypatch, {
        cs.MODIFIED: [(_obs("a", "https://x/2"), "moved"),
                      (_obs("b", "https://x/1"), "moved")],
        cs.NEW: [(_obs("c", "https://x/9"), "baseline")]})
    assert report["targets"] == ["https://x/1", "https://x/2"]


def test_a_shortlisted_document_with_no_url_is_reported_not_dropped(monkeypatch):
    """SIMAH's articles have no url of their own. Silently shortening the list
    would say a re-crawl covered the shortlist when it did not."""
    from dynamic_crawler import changesignal as cs
    report = _report(monkeypatch, {
        cs.MODIFIED: [(_obs("Article-4", ""), "moved")]})
    assert report["targets"] == []
    assert report["targets_without_url"] == 1


def test_the_shortlist_carries_the_url(monkeypatch):
    from dynamic_crawler import changesignal as cs
    report = _report(monkeypatch, {
        cs.MODIFIED: [(_obs("a", "https://x/1"), "moved")]})
    assert report["shortlist"]["modified"][0]["url"] == "https://x/1"


def test_a_missing_entry_still_has_no_url_and_does_not_raise(monkeypatch):
    """The `missing` bucket holds bare identity strings, not observations."""
    from dynamic_crawler import changesignal as cs
    report = _report(monkeypatch, {cs.MISSING: [("page=x|id=4", "absent")]})
    assert report["shortlist"]["missing"][0] == {
        "key": "page=x|id=4", "title": "", "url": "", "why": "absent"}


def test_an_empty_targets_file_is_still_written(tmp_path):
    """Empty means "nothing changed"; absent means "the sweep did not run". A
    re-crawl driven by this file has to tell them apart."""
    from dynamic_crawler.cli.sweep import _emit
    out = tmp_path / "targets.txt"
    _emit({"targets": []}, targets_out=str(out))
    assert out.exists() and out.read_text(encoding="utf-8") == ""


def test_the_targets_file_is_one_url_per_line(tmp_path):
    from dynamic_crawler.cli.sweep import _emit
    out = tmp_path / "t.txt"
    _emit({"targets": ["https://x/1", "https://x/2"]}, targets_out=str(out))
    assert out.read_text(encoding="utf-8").splitlines() == ["https://x/1",
                                                            "https://x/2"]


# --------------------------------------------------------------------------- #
#  reading the file back                                                       #
# --------------------------------------------------------------------------- #

def test_no_file_means_an_ordinary_full_crawl():
    from dynamic_crawler.formfill.__main__ import _read_urls
    assert _read_urls(None) is None


def test_an_empty_file_is_a_target_list_of_none_not_an_absent_one(tmp_path):
    """The distinction the whole flag rests on: [] opens no detail page, None
    opens every one."""
    from dynamic_crawler.formfill.__main__ import _read_urls
    p = tmp_path / "t.txt"
    p.write_text("", encoding="utf-8")
    assert _read_urls(str(p)) == []


def test_blank_lines_and_comments_are_ignored(tmp_path):
    from dynamic_crawler.formfill.__main__ import _read_urls
    p = tmp_path / "t.txt"
    p.write_text("# from the sweep\nhttps://x/1\n\n  https://x/2  \n",
                 encoding="utf-8")
    assert _read_urls(str(p)) == ["https://x/1", "https://x/2"]


def test_a_missing_file_stops_the_run(tmp_path):
    """Falling back to a full crawl would turn a typo into a re-read of the
    whole source."""
    from dynamic_crawler.formfill.__main__ import _read_urls
    with pytest.raises(SystemExit):
        _read_urls(str(tmp_path / "nope.txt"))
