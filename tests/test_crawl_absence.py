"""The withdrawal decision on the CRAWL path, offline.

`crawl_absence` is a pure module on purpose: it decides which absences may be
judged, which is the pair of rules that can empty a library, and it must be
testable without the OCR stack `orch.py` drags in. The orchestrator half is
exercised in test_stage_b.py.

    venv/Scripts/python.exe -m pytest tests/test_crawl_absence.py -q
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dynamic_crawler import crawl_absence as ca      # noqa: E402
from dynamic_crawler import withdrawal as wd         # noqa: E402
from dynamic_crawler.change_state import ChangeStateStore  # noqa: E402

IDENTITY = ("document_url", "doc_path")


class Doc:
    def __init__(self, url, path="", title="", **meta):
        self.document_url = url
        self.doc_path = path
        self.title = title
        self.extra_meta = dict(meta)


def row(url, path="", title="", system="Rules", **kw):
    return {"id": kw.pop("id", 1), "document_url": url, "doc_path": path,
            "title": title, "source_system": system, **kw}


def store(tmp_path, source="AML/Rules"):
    return ChangeStateStore.for_source(source, root=tmp_path)


def key_of(url, path=""):
    return f"document_url={url}|doc_path={path}"


def backdate(st, key, hours):
    """Put a streak's start in the past — the span, not the count, is the rule."""
    when = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    st.records[key]["first_missed"] = when


def judge(st, rows, **kw):
    kw.setdefault("identity", IDENTITY)
    kw.setdefault("labels", ["AML/Rules"])
    kw.setdefault("counts", {"AML/Rules": 8})
    kw.setdefault("priors", {"AML/Rules": 8})
    kw.setdefault("problems", [])
    return ca.judge(st, rows, **kw)


# --------------------------------------------------------------------------- #
#  the store is the crawl's OWN                                                #
# --------------------------------------------------------------------------- #

def test_crawl_root_is_not_the_sweep_root():
    """A sweep counts an absence for every key in the file it opens, and missed()
    never asks who owns the record — so sharing one file lets a daily sweep build
    the crawl's streak and its 20-hour span."""
    from dynamic_crawler.change_state import DEFAULT_ROOT
    assert ca.CRAWL_ROOT != DEFAULT_ROOT
    assert ca.CRAWL_ROOT.parent == DEFAULT_ROOT


def test_store_for_writes_under_the_crawl_root(tmp_path):
    st = ca.store_for("AML/Rules", root=tmp_path / "crawl")
    st.save()
    assert st.path.parent == tmp_path / "crawl"


def test_a_sweep_of_the_same_source_cannot_reach_the_crawls_file(tmp_path):
    sweep = ChangeStateStore.for_source("AML/Rules", root=tmp_path)
    crawl = ca.store_for("AML/Rules", root=tmp_path / "crawl")
    assert sweep.path != crawl.path


# --------------------------------------------------------------------------- #
#  F2 — whose gate problem stops whom                                          #
# --------------------------------------------------------------------------- #

def test_a_problem_naming_one_source_stops_only_that_source():
    v = ca.source_verdicts(["circulars: count moved 28 -> 12 (57.1%), over the 5%"],
                           ["circulars", "rulebook"])
    assert v["circulars"] and not v["rulebook"]


def test_a_run_wide_problem_stops_every_source():
    v = ca.source_verdicts(["3 page(s) came back as a bot-protection challenge"],
                           ["circulars", "rulebook"])
    assert v["circulars"] and v["rulebook"] and v[ca.RUN_WIDE]


def test_the_total_count_problem_is_run_wide():
    """`total:` is not a source label, so it must not be charged to one."""
    v = ca.source_verdicts(["total: count moved 40 -> 12 (70.0%), over the 5%"],
                           ["circulars", "rulebook"])
    assert v["circulars"] and v["rulebook"]


def test_a_healthy_source_is_judged_while_its_sibling_is_blocked(tmp_path):
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a"), Doc("b")], IDENTITY)
    st.records[key_of("a")]["misses"] = 1
    block = judge(st, [row("a", system="Circ"), row("b", system="Rule", id=2)],
                  labels=["circulars", "rulebook"],
                  counts={"circulars": 4, "rulebook": 4},
                  priors={"circulars": 4, "rulebook": 4},
                  problems=["circulars: count moved 28 -> 12 (57.1%), over the 5%"],
                  systems={"Circ": "circulars", "Rule": "rulebook"})
    assert block["by_source"]["circulars"]["may_propose"] is False
    assert block["by_source"]["rulebook"]["may_propose"] is True


def test_a_blocked_source_does_not_advance_its_streak(tmp_path):
    """A blocked run that counted its absences would hand the next good run a
    streak it never earned."""
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    judge(st, [row("a")],
          problems=["1 page(s) came back as a bot-protection challenge"])
    assert st.records[key_of("a")]["misses"] == 0


def test_a_judgeable_source_does_advance_its_streak(tmp_path):
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    judge(st, [row("a")])
    assert st.records[key_of("a")]["misses"] == 1


# --------------------------------------------------------------------------- #
#  the count rule is the withdrawal layer's, not the gate's                    #
# --------------------------------------------------------------------------- #

def test_one_document_lost_from_a_small_source_is_allowed():
    """The gate calls 17 -> 16 a 5.9% swing and quarantines the run. A flat
    percentage makes this layer inert on every source under 20 documents."""
    assert not ca.blocked_for("s", {"s": []}, observed=16, prior=17)


def test_two_documents_lost_from_seventeen_is_not_allowed():
    assert ca.blocked_for("s", {"s": []}, observed=15, prior=17)


def test_a_bulk_loss_is_refused_on_a_large_source():
    assert ca.blocked_for("s", {"s": []}, observed=363, prior=415)


def test_a_source_that_produced_nothing_is_blocked():
    assert ca.blocked_for("s", {"s": []}, observed=0, prior=8)


def test_the_gates_own_tolerance_is_untouched():
    """This layer re-asks the count question; it does not relax the gate. Read
    off the source rather than imported: the orchestrator pulls in the OCR stack."""
    orch = (Path(__file__).resolve().parents[1] / "orchestrator"
            / "orchestrator.py").read_text(encoding="utf-8")
    assert "COUNT_TOLERANCE_PCT = 5.0" in orch
    assert "spread <= COUNT_TOLERANCE_PCT" in orch
    assert wd.COUNT_TOLERANCE_PCT == 5.0


# --------------------------------------------------------------------------- #
#  a targeted run proposes nothing                                             #
# --------------------------------------------------------------------------- #

def test_a_targeted_run_proposes_nothing(tmp_path):
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    st.records[key_of("a")]["misses"] = 4
    backdate(st, key_of("a"), 48)
    block = judge(st, [row("a")], targeted="walked past 61 page(s)")
    assert block["counts"][wd.PROPOSED] == 0
    assert "walked past" in block[wd.WATCHING][0]["why"]


def test_the_same_absence_on_a_full_run_is_proposed(tmp_path):
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    st.records[key_of("a")]["misses"] = 4
    backdate(st, key_of("a"), 48)
    st.records[key_of("a")]["last_missed"] = st.records[key_of("a")]["last_seen"]
    block = judge(st, [row("a")])
    assert block["counts"][wd.PROPOSED] == 1


# --------------------------------------------------------------------------- #
#  F2's other half — a row that cannot be charged to a source                   #
# --------------------------------------------------------------------------- #

def test_source_system_labels_skips_a_system_two_sources_share():
    class C:
        def __init__(self, systems):
            self.source_systems = systems

    class Composite:
        source_names = ["a", "b"]
        crawlers = [C(["Shared"]), C(["Shared", "Own"])]

    assert ca.source_system_labels(Composite()) == {"Own": "b"}


def test_a_single_source_run_charges_every_row_to_it():
    assert ca.owner_of(row("x", system="anything"), ["only"], {}) == "only"


def test_an_unmappable_row_is_not_judged(tmp_path):
    st = store(tmp_path)
    block = judge(st, [row("a", system="Shared")],
                  labels=["circulars", "rulebook"],
                  counts={"circulars": 4, "rulebook": 4},
                  priors={"circulars": 4, "rulebook": 4},
                  systems={})
    assert block["counts"][wd.NOT_JUDGED] == 1
    assert "cannot be told from source_system" in block[wd.NOT_JUDGED][0]["why"]


def test_an_unmappable_row_does_not_advance_a_streak(tmp_path):
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    judge(st, [row("a", system="Shared")], labels=["x", "y"],
          counts={"x": 1, "y": 1}, priors={"x": 1, "y": 1}, systems={})
    assert st.records[key_of("a")]["misses"] == 0


def test_a_row_with_no_identity_value_is_not_judged(tmp_path):
    st = store(tmp_path)
    block = judge(st, [row("", "")])
    assert block["counts"][wd.NOT_JUDGED] == 1
    assert "carries no value" in block[wd.NOT_JUDGED][0]["why"]


# --------------------------------------------------------------------------- #
#  attribution, collisions and the shape of the block                          #
# --------------------------------------------------------------------------- #

def test_a_document_this_crawl_never_recorded_is_not_judged(tmp_path):
    """The first run after this ships has no history, so it may not claim two
    consecutive absences. missed() does not stamp a signal — only being seen does."""
    st = store(tmp_path)
    block = judge(st, [row("a")])
    assert block["counts"][wd.NOT_JUDGED] == 1
    assert block["counts"][wd.PROPOSED] == 0


def test_a_sweeps_record_in_a_shared_file_would_not_be_judged(tmp_path):
    """Defence in depth behind the separate directory: attribution still refuses."""
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    st.records[key_of("a")]["signal"] = "sitemap:mhrsd.regs"
    st.records[key_of("a")]["misses"] = 4
    backdate(st, key_of("a"), 48)
    block = judge(st, [row("a")])
    assert block["counts"][wd.NOT_JUDGED] == 1
    assert "may not judge another's absences" in block[wd.NOT_JUDGED][0]["why"]


def test_two_absent_rows_sharing_one_identity_block_the_source(tmp_path):
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    st.records[key_of("a")]["misses"] = 4
    backdate(st, key_of("a"), 48)
    block = judge(st, [row("a"), row("a", id=2)])
    assert block["counts"][wd.PROPOSED] == 0
    assert "in doubt" in block["by_source"]["AML/Rules"]["blocked_by"][0]


def test_being_seen_clears_a_streak(tmp_path):
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    judge(st, [row("a")])
    assert st.records[key_of("a")]["misses"] == 1
    ca.note_seen(st, [Doc("a")], IDENTITY)
    assert st.records[key_of("a")]["misses"] == 0


def test_positive_evidence_counts_even_on_a_blocked_run(tmp_path):
    """A document present in a blocked run is present."""
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    assert st.records[key_of("a")]["signal"] == ca.SIGNAL


def test_a_document_with_no_identity_is_not_recorded_as_seen(tmp_path):
    st = store(tmp_path)
    assert ca.note_seen(st, [Doc("", "")], IDENTITY) == 0


def test_the_block_names_the_state_file_and_the_rule(tmp_path):
    st = store(tmp_path)
    block = judge(st, [])
    assert str(st.path) == block["state_file"]
    assert "consecutive trustworthy runs" in block["rule"]
    assert block["confirmed"] is False
    assert block["counts"] == {wd.PROPOSED: 0, wd.WATCHING: 0, wd.NOT_JUDGED: 0}


def test_every_entry_carries_one_shape(tmp_path):
    """One bucket, one shape — a bucket of two shapes is what made the sweep CLI
    slice a built-in method."""
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a", title="A")], IDENTITY)
    block = judge(st, [row("a", title="A"), row("", "")])
    for name in (wd.PROPOSED, wd.WATCHING, wd.NOT_JUDGED):
        for entry in block[name]:
            assert set(entry) == {"key", "source", "title", "url", "misses",
                                  "first_seen", "last_seen", "first_missed", "why"}


def test_record_false_leaves_the_streak_alone(tmp_path):
    st = store(tmp_path)
    ca.note_seen(st, [Doc("a")], IDENTITY)
    judge(st, [row("a")], record=False)
    assert st.records[key_of("a")]["misses"] == 0


def test_nothing_here_can_write_a_regulations_row():
    """The decision layer stays on the read-only side of the agreement."""
    source = Path(ca.__file__).read_text(encoding="utf-8")
    for forbidden in ("mark_regulation_withdrawn", "mark_regulation_deleted",
                      "UPDATE", "INSERT", "DELETE"):
        assert forbidden not in source


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
