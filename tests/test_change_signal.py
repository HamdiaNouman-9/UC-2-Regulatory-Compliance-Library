"""The change sweep: one verdict shape for every regulator, and its memory.

WHY THIS FILE LOOKS LIKE THIS

Same reason as the other suites here — importing the orchestrator pulls in the
OCR stack, which is gigabytes and is not needed to check classification. The
heavy modules are stubbed before the import. No network and no database.

What is verified here:

  identity          the key comes off the document's own extra_meta, so two
                    sources in one sweep key on different fields
  agreement         a stored row and a crawled document produce the SAME key,
                    and the same key the orchestrator already builds
  honest failure    a probe that did not run is `unknown`, never `unchanged`,
                    and it does not erase the token it failed to re-read
  baseline          the first token on a document we already store is a
                    baseline, not a change
  confirm tier      a regulator that re-uploads its library in bulk needs the
                    content hash to agree before anything is `modified`, and
                    the confirm runs for the shortlist only
  absence           only a sweep that can see the whole inventory reports it;
                    a detect-only sweep says "not measured" instead of 0
  the store         round-trips, counts miss streaks, refuses to load a corrupt
                    file as an empty one, and reports a key collision

    venv/Scripts/python.exe -m pytest tests/test_change_signal.py -v
"""
from __future__ import annotations

import hashlib
import json
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

from dynamic_crawler import changesignal as cs                       # noqa: E402
from dynamic_crawler import fingerprint                              # noqa: E402
from dynamic_crawler.change_state import ChangeStateStore, slug      # noqa: E402
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


class FakeSignal(cs.ChangeSignal):
    """A sweep that returns what it was given and counts its confirm calls."""

    name = "fake"

    def __init__(self, observations, confirm_required=False,
                 covers_inventory=False, confirm_hash="", raises=False):
        self.observations = list(observations)
        self.confirm_required = confirm_required
        self.covers_inventory = covers_inventory
        self.confirm_hash = confirm_hash
        self.raises = raises
        self.confirmed = []

    def sweep(self):
        return list(self.observations)

    def confirm(self, obs):
        self.confirmed.append(obs.key)
        if self.raises:
            raise RuntimeError("the document could not be fetched")
        return self.confirm_hash


def store_at(tmp_path, name="state.json") -> ChangeStateStore:
    return ChangeStateStore(tmp_path / name, source="source:TEST")


def seen(url="https://x/1", token="T1", basis="etag", **kw) -> cs.Observation:
    """One observation of a document keyed on its url."""
    doc = Doc(document_url=url,
              extra_meta={"version_token": token, "hash_basis": basis}, **kw)
    return cs.observation_for(doc, default_fields=("document_url",))


def swept(store, observations, **kw) -> tuple:
    return cs.run_sweep(FakeSignal(observations, **kw), store)


def verdicts(buckets) -> dict:
    return {k: [o.key if hasattr(o, "key") else o for o, _r in v]
            for k, v in buckets.items() if v}


# --------------------------------------------------------------------------- #
#  identity — off the document, never off the regulator                        #
# --------------------------------------------------------------------------- #

def test_the_identity_fields_come_from_the_document():
    doc = Doc(reference_no="C-42",
              extra_meta={"identity_fields": ["reference_no"]})
    obs = cs.observation_for(doc, default_fields=("document_url", "doc_path"))
    assert obs.identity_fields == ("reference_no",)
    assert obs.key == "reference_no=C-42"


def test_two_documents_in_one_sweep_key_on_different_fields():
    """The case a single per-regulator setting could not express: a circular
    keyed on its reference number and an article keyed on its url, together."""
    circular = Doc(reference_no="C-42", document_url="https://x/new.pdf",
                   extra_meta={"identity_fields": ["reference_no"]})
    article = Doc(document_url="https://x/article-3", doc_path=["Law", "3"])
    keys = [cs.observation_for(d, ("document_url", "doc_path")).key
            for d in (circular, article)]
    assert keys == ["reference_no=C-42",
                    "document_url=https://x/article-3|doc_path=Law > 3"]


def test_a_stored_row_and_a_crawled_document_key_identically():
    """A sweep over the stored inventory has to be comparable with a sweep of a
    live listing, so both shapes go through one function."""
    fields = {"identity_fields": ["reference_no"]}
    doc = Doc(reference_no="C-9", extra_meta=fields)
    row = {"reference_no": "C-9", "document_url": "https://x/9",
           "extra_meta": json.dumps(fields)}      # the column holds JSON text
    assert cs.observation_for(doc).key == cs.observation_for(row).key


def test_the_field_order_the_source_declared_is_the_key_order():
    doc = Doc(document_url="https://x/1", doc_path=["A"])
    forward = cs.observation_for(doc, ("document_url", "doc_path")).key
    backward = cs.observation_for(doc, ("doc_path", "document_url")).key
    assert forward != backward


def test_the_sweep_and_the_orchestrator_build_the_same_key():
    o = Orchestrator(crawler=_Any(), repo=_Any(), source_name="source:TEST")
    doc = Doc(document_url="https://x/a?b=1", doc_path=["A", "B"])
    assert (cs.identity_key(o._identity_fields_of(doc))
            == cs.observation_for(doc, o.DEFAULT_IDENTITY).key
            == "document_url=https://x/a?b=1|doc_path=A > B|title=a document")


def test_the_inventory_hash_is_the_one_already_stored():
    """Pinned to literal keys: every stored inventory_hash was written by the
    previous implementation, and changing it makes every source miss its early
    exit and insert a run_history row on the next run.

    That cost was PAID ONCE, deliberately, on 2026-08-16: `title` joined the
    default identity, so these keys gained a `|title=` segment and every source
    reconciled once on its next run. Re-pinned to the new literals. Anyone
    changing them again is buying the same one-run reconciliation for every
    source — do it knowingly, not as a side effect of touching the tuple.
    """
    o = Orchestrator(crawler=_Any(), repo=_Any(), source_name="source:TEST")
    docs = [Doc(document_url="https://x/a?b=1", doc_path=["A", "B"]),
            Doc(reference_no="C-1", extra_meta={"identity_fields": ["reference_no"]})]
    keys = sorted(["document_url=https://x/a?b=1|doc_path=A > B|title=a document",
                   "reference_no=C-1"])
    expected = hashlib.md5("\n".join(keys).encode("utf-8")).hexdigest()[:12]
    assert o._inventory_hash(docs) == expected


# --------------------------------------------------------------------------- #
#  the verdict                                                                 #
# --------------------------------------------------------------------------- #

def test_a_key_the_store_has_never_seen_is_new(tmp_path):
    report, buckets = swept(store_at(tmp_path), [seen()])
    assert verdicts(buckets) == {cs.NEW: ["document_url=https://x/1"]}
    assert report["counts"][cs.NEW] == 1


def test_the_same_token_is_unchanged(tmp_path):
    store = store_at(tmp_path)
    swept(store, [seen(token="T1")])
    _report, buckets = swept(store, [seen(token="T1")])
    assert list(verdicts(buckets)) == [cs.UNCHANGED]


def test_a_moved_token_is_modified(tmp_path):
    """A file swapped behind an unchanged link: the url did not move, the
    server's version counter did."""
    store = store_at(tmp_path)
    swept(store, [seen(token="{GUID},4")])
    _report, buckets = swept(store, [seen(token="{GUID},5")])
    assert list(verdicts(buckets)) == [cs.MODIFIED]


def test_a_failed_probe_is_unknown_and_never_unchanged(tmp_path):
    store = store_at(tmp_path)
    swept(store, [seen(token="T1")])
    report, buckets = swept(store, [seen(token="", basis=fingerprint.BASIS_FAILED)])
    assert list(verdicts(buckets)) == [cs.UNKNOWN]
    assert report["counts"][cs.UNCHANGED] == 0
    assert report["by_basis"][fingerprint.BASIS_FAILED] == 1


def test_a_stored_token_and_none_read_is_unknown(tmp_path):
    """No error, no token either — a source that quietly stopped answering must
    not read as a library in which nothing changes."""
    store = store_at(tmp_path)
    swept(store, [seen(token="T1")])
    _report, buckets = swept(store, [seen(token="", basis=fingerprint.BASIS_NONE)])
    assert list(verdicts(buckets)) == [cs.UNKNOWN]


def test_a_failed_probe_does_not_erase_the_stored_token(tmp_path):
    """The stored token is still the last thing the server actually said. Losing
    it would make the next run treat a real change as a first baseline."""
    store = store_at(tmp_path)
    swept(store, [seen(token="{GUID},4")])
    swept(store, [seen(token="", basis=fingerprint.BASIS_FAILED)])
    assert store.get("document_url=https://x/1")["token"] == "{GUID},4"

    _report, buckets = swept(store, [seen(token="{GUID},5")])
    assert list(verdicts(buckets)) == [cs.MODIFIED]


def test_the_first_token_on_a_known_document_is_a_baseline(tmp_path):
    """Enabling a signal must not reclassify a whole library on the first run."""
    store = store_at(tmp_path)
    swept(store, [seen(token="", basis=fingerprint.BASIS_NONE)])
    _report, buckets = swept(store, [seen(token="{GUID},7")])
    assert list(verdicts(buckets)) == [cs.UNCHANGED]
    assert store.get("document_url=https://x/1")["token"] == "{GUID},7"


def test_the_reason_shows_the_part_of_the_token_that_moved(tmp_path):
    """SharePoint puts the version at the end of `{GUID},<version>`. Truncating
    the head printed the same 24 characters twice and explained nothing."""
    store = store_at(tmp_path)
    guid = "{8F332DF8-E485-43BB-B906-85F5D4E7BD59}"
    swept(store, [seen(token=f"{guid},4")])
    _report, buckets = swept(store, [seen(token=f"{guid},5")])
    reason = buckets[cs.MODIFIED][0][1]
    assert reason.endswith("},4 -> {8F332DF...5D4E7BD59},5")


def test_an_opaque_token_still_detects_a_change_and_is_reported_separately(tmp_path):
    store = store_at(tmp_path)
    swept(store, [seen(token="W/abc", basis="etag-opaque")])
    report, buckets = swept(store, [seen(token="W/def", basis="etag-opaque")])
    assert list(verdicts(buckets)) == [cs.MODIFIED]
    assert report["by_basis"] == {"etag-opaque": 1}


# --------------------------------------------------------------------------- #
#  the confirm tier, per source                                                #
# --------------------------------------------------------------------------- #

def test_a_bulk_republish_is_unchanged_when_the_content_did_not_move(tmp_path):
    """One regulator re-uploaded its whole library in a three-second window and
    every version counter moved at once. Without this the sweep shortlists
    everything and triggers a full re-crawl on no change at all."""
    store = store_at(tmp_path)
    swept(store, [seen(token="4")], confirm_required=True, confirm_hash="H1")
    store.records["document_url=https://x/1"]["confirm_hash"] = "H1"
    _report, buckets = swept(store, [seen(token="5")],
                             confirm_required=True, confirm_hash="H1")
    assert list(verdicts(buckets)) == [cs.UNCHANGED]


def test_token_and_content_both_moved_is_modified(tmp_path):
    store = store_at(tmp_path)
    swept(store, [seen(token="4")])
    store.records["document_url=https://x/1"]["confirm_hash"] = "H1"
    _report, buckets = swept(store, [seen(token="5")],
                             confirm_required=True, confirm_hash="H2")
    assert list(verdicts(buckets)) == [cs.MODIFIED]
    assert store.get("document_url=https://x/1")["confirm_hash"] == "H2"


def test_the_confirm_runs_for_the_shortlist_only(tmp_path):
    """Tier two is what a two-tier design exists to avoid paying for."""
    store = store_at(tmp_path)
    swept(store, [seen("https://x/1", "4"), seen("https://x/2", "4")])
    signal = FakeSignal([seen("https://x/1", "5"), seen("https://x/2", "4")],
                        confirm_required=True, confirm_hash="H")
    cs.run_sweep(signal, store)
    assert signal.confirmed == ["document_url=https://x/1"]


def test_a_required_confirm_that_returns_nothing_is_unknown(tmp_path):
    store = store_at(tmp_path)
    swept(store, [seen(token="4")])
    _report, buckets = swept(store, [seen(token="5")],
                             confirm_required=True, confirm_hash="")
    assert list(verdicts(buckets)) == [cs.UNKNOWN]


def test_a_confirm_that_raises_is_unknown_not_a_crash(tmp_path):
    store = store_at(tmp_path)
    swept(store, [seen(token="4")])
    signal = FakeSignal([seen(token="5")], confirm_required=True, raises=True)
    _report, buckets = cs.run_sweep(signal, store)
    assert list(verdicts(buckets)) == [cs.UNKNOWN]


def test_a_failed_confirm_does_not_consume_the_change(tmp_path):
    """The token it moved TO must not be stored by a sweep that never judged it.

    Otherwise the next sweep compares new against new and reads `unchanged` for
    good — a document that really was amended goes quiet after one bad fetch.
    """
    store = store_at(tmp_path)
    swept(store, [seen(token="4")])

    signal = FakeSignal([seen(token="5")], confirm_required=True, raises=True)
    _report, buckets = cs.run_sweep(signal, store)
    assert list(verdicts(buckets)) == [cs.UNKNOWN]
    assert store.get("document_url=https://x/1")["token"] == "4"

    # The next sweep still sees a moved token, so the confirm is retried.
    retry = FakeSignal([seen(token="5")], confirm_required=True, confirm_hash="H")
    _report, buckets = cs.run_sweep(retry, store)
    assert list(verdicts(buckets)) == [cs.MODIFIED]
    assert retry.confirmed == ["document_url=https://x/1"]


def test_a_confirm_that_returns_nothing_does_not_consume_it_either(tmp_path):
    """The same rule for the other way a confirm produces no hash."""
    store = store_at(tmp_path)
    swept(store, [seen(token="4")])
    swept(store, [seen(token="5")], confirm_required=True, confirm_hash="")
    assert store.get("document_url=https://x/1")["token"] == "4"


# --------------------------------------------------------------------------- #
#  absence, and who is allowed to observe it                                   #
# --------------------------------------------------------------------------- #

def test_a_detect_only_sweep_says_absence_was_not_measured(tmp_path):
    """It reads only the urls we already store, so it cannot see a document the
    regulator removed. A `missing: 0` here would be a zero meaning
    "not measured"."""
    store = store_at(tmp_path)
    swept(store, [seen("https://x/1"), seen("https://x/2")])
    report, buckets = swept(store, [seen("https://x/1")])
    assert cs.MISSING not in report["counts"]
    assert "not measured" in report["missing"]
    assert buckets[cs.MISSING] == []


def test_a_sweep_that_covers_the_inventory_reports_missing(tmp_path):
    store = store_at(tmp_path)
    swept(store, [seen("https://x/1"), seen("https://x/2")], covers_inventory=True)
    report, buckets = swept(store, [seen("https://x/1")], covers_inventory=True)
    assert report["counts"][cs.MISSING] == 1
    assert buckets[cs.MISSING][0][0] == "document_url=https://x/2"


def test_the_miss_streak_counts_up_and_resets(tmp_path):
    """Two consecutive absences is the earliest a withdrawal may even be
    considered, and a person decides after that. The store only counts."""
    store = store_at(tmp_path)
    key = "document_url=https://x/2"
    swept(store, [seen("https://x/1"), seen("https://x/2")], covers_inventory=True)
    for expected in (1, 2, 3):
        _report, buckets = swept(store, [seen("https://x/1")], covers_inventory=True)
        assert buckets[cs.MISSING][0][1].startswith(f"absent from {expected} ")
    assert store.get(key)["misses"] == 3

    swept(store, [seen("https://x/1"), seen("https://x/2")], covers_inventory=True)
    assert store.get(key)["misses"] == 0


def test_a_document_seen_but_unreadable_is_not_absent(tmp_path):
    store = store_at(tmp_path)
    swept(store, [seen(token="T1")], covers_inventory=True)
    _report, buckets = swept(store, [seen(token="", basis=fingerprint.BASIS_FAILED)],
                             covers_inventory=True)
    assert buckets[cs.MISSING] == []
    assert store.get("document_url=https://x/1")["misses"] == 0


# --------------------------------------------------------------------------- #
#  the store                                                                   #
# --------------------------------------------------------------------------- #

def test_state_survives_a_round_trip(tmp_path):
    store = store_at(tmp_path)
    swept(store, [seen(token="{GUID},4")])
    path = store.save()

    reloaded = ChangeStateStore(path).load()
    assert reloaded.source == "source:TEST"
    assert reloaded.get("document_url=https://x/1")["token"] == "{GUID},4"
    _report, buckets = swept(reloaded, [seen(token="{GUID},4")])
    assert list(verdicts(buckets)) == [cs.UNCHANGED]


def test_a_corrupt_state_file_refuses_to_load_as_an_empty_one(tmp_path):
    """Loading it as empty would report every document `new` and reset every
    miss streak — silently."""
    path = tmp_path / "state.json"
    path.write_text("{ this is not json", encoding="utf-8")
    with pytest.raises(ValueError) as e:
        ChangeStateStore(path).load()
    assert "not readable change state" in str(e.value)


def test_saving_leaves_no_partial_file_behind(tmp_path):
    store_at(tmp_path).save()
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]


def test_a_missing_state_file_is_an_empty_store_not_an_error(tmp_path):
    store = ChangeStateStore(tmp_path / "never-written.json").load()
    assert store.keys() == set()


def test_a_colliding_key_is_reported_and_the_record_left_alone(tmp_path):
    """Values are joined into the key, so a value carrying a separator could
    address another document's history. Never merge the two silently."""
    store = store_at(tmp_path)
    first = cs.Observation(key="document_url=a|doc_path=b",
                           fields={"document_url": "a", "doc_path": "b"},
                           identity_fields=("document_url", "doc_path"), token="T1")
    store.record(first, cs.NEW)

    clash = cs.Observation(key="document_url=a|doc_path=b",
                           fields={"document_url": "a|doc_path=b", "doc_path": ""},
                           identity_fields=("document_url", "doc_path"), token="T2")
    report, buckets = cs.run_sweep(FakeSignal([clash]), store)
    assert len(report["collisions"]) == 1
    assert not any(buckets[k] for k in buckets)
    assert store.get(first.key)["token"] == "T1"


def test_one_file_per_source(tmp_path):
    assert slug("source:SAMA/circulars") == "source_sama_circulars"
    assert slug("") == "unknown"
    assert len(slug("x" * 300)) == 80


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
