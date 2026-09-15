"""Offline verification of the span-based Requirement/Activity sync logic
against the exact V1/V2/V3 scenario worked out by hand while designing it:

    V1: requirements 1,2,3,4,5 (all new)
    V2: requirements 1,2,6     (3,4,5 drop, 6 is new)
    V3: requirements 3,4,5,7   (1,2,6 drop, 3/4/5 come BACK, 7 is new)

No real database -- FakeRepo is an in-memory stand-in that mimics the actual
schema's rules (Requirement rows are permanent and content-addressed;
RequirementSpan/ActivitySpan can hold multiple rows per id; at most one OPEN
span per id at a time, enforced the same way UQ_RequirementSpan_OneOpenSpan
enforces it for real). This proves processor/requirement_activity_sync.py's
actual code produces the row states worked out on paper -- it does not prove
anything about the real MSSQL schema/driver.

    venv/Scripts/python.exe -m pytest tests/test_requirement_activity_spans.py -v
    venv/Scripts/python.exe tests/test_requirement_activity_spans.py       # no pytest needed
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processor.requirement_activity_sync import sync_requirements_and_activities


class FakeRepo:
    """Same rules as the real schema, held in memory."""

    def __init__(self):
        self.requirements = {}        # requirement_id -> {regulation_id, ref_key, ...}
        self.requirement_spans = []   # [{requirement_id, introduced_in_version_id, superseded_in_version_id}]
        self.activities = {}          # activity_id -> {requirement_id, ref_key, ...}
        self.activity_spans = []      # same shape, keyed by activity_id
        self._next_req_id = 1
        self._next_act_id = 1

    # -- requirements --------------------------------------------------- #

    def get_all_requirements_for_regulation(self, regulation_id):
        return {r["ref_key"]: rid for rid, r in self.requirements.items()
                if r["regulation_id"] == regulation_id}

    def get_current_requirements(self, regulation_id):
        open_ids = {s["requirement_id"] for s in self.requirement_spans
                    if s["superseded_in_version_id"] is None}
        return {r["ref_key"]: rid for rid, r in self.requirements.items()
                if r["regulation_id"] == regulation_id and rid in open_ids}

    def insert_requirement(self, regulation_id, version_id, ref_key, title,
                           description, source_reference, source_refs,
                           requirement_type_name=""):
        rid = self._next_req_id
        self._next_req_id += 1
        self.requirements[rid] = dict(
            regulation_id=regulation_id, ref_key=ref_key, title=title,
            description=description, source_reference=source_reference)
        self.requirement_spans.append({
            "requirement_id": rid, "introduced_in_version_id": version_id,
            "superseded_in_version_id": None})
        return rid

    def open_requirement_span(self, requirement_id, version_id):
        # UQ_RequirementSpan_OneOpenSpan's job, done in Python: refuse a
        # second open span for the same requirement.
        for s in self.requirement_spans:
            if s["requirement_id"] == requirement_id and s["superseded_in_version_id"] is None:
                raise AssertionError(
                    f"requirement {requirement_id} already has an open span "
                    f"-- would violate UQ_RequirementSpan_OneOpenSpan")
        self.requirement_spans.append({
            "requirement_id": requirement_id, "introduced_in_version_id": version_id,
            "superseded_in_version_id": None})

    def mark_requirement_superseded(self, requirement_id, version_id):
        open_spans = [s for s in self.requirement_spans
                     if s["requirement_id"] == requirement_id
                     and s["superseded_in_version_id"] is None]
        assert len(open_spans) == 1, (
            f"expected exactly one open span for requirement {requirement_id}, "
            f"found {len(open_spans)}")
        open_spans[0]["superseded_in_version_id"] = version_id

    # -- activities -------------------------------------------------------- #

    def get_all_activities_for_requirement(self, requirement_id):
        return {a["ref_key"]: aid for aid, a in self.activities.items()
                if a["requirement_id"] == requirement_id}

    def get_current_activities_for_requirement(self, requirement_id):
        open_ids = {s["activity_id"] for s in self.activity_spans
                    if s["superseded_in_version_id"] is None}
        return {a["ref_key"]: aid for aid, a in self.activities.items()
                if a["requirement_id"] == requirement_id and aid in open_ids}

    def get_activities_for_requirement(self, requirement_id):
        open_ids = {s["activity_id"] for s in self.activity_spans
                    if s["superseded_in_version_id"] is None}
        return [aid for aid, a in self.activities.items()
                if a["requirement_id"] == requirement_id and aid in open_ids]

    def insert_activity(self, requirement_id, version_id, ref_key, title,
                        description, department, frequency, frequency_type,
                        priority, evidence_expected, activity_type_name=""):
        aid = self._next_act_id
        self._next_act_id += 1
        self.activities[aid] = dict(requirement_id=requirement_id, ref_key=ref_key,
                                    title=title, description=description)
        self.activity_spans.append({
            "activity_id": aid, "introduced_in_version_id": version_id,
            "superseded_in_version_id": None})
        return aid

    def open_activity_span(self, activity_id, version_id):
        for s in self.activity_spans:
            if s["activity_id"] == activity_id and s["superseded_in_version_id"] is None:
                raise AssertionError(
                    f"activity {activity_id} already has an open span -- "
                    f"would violate UQ_ActivitySpan_OneOpenSpan")
        self.activity_spans.append({
            "activity_id": activity_id, "introduced_in_version_id": version_id,
            "superseded_in_version_id": None})

    def mark_activity_superseded(self, activity_id, version_id):
        open_spans = [s for s in self.activity_spans
                     if s["activity_id"] == activity_id
                     and s["superseded_in_version_id"] is None]
        assert len(open_spans) == 1, (
            f"expected exactly one open span for activity {activity_id}, "
            f"found {len(open_spans)}")
        open_spans[0]["superseded_in_version_id"] = version_id

    # -- helpers for assertions ------------------------------------------ #

    def open_requirement_ids(self):
        return {s["requirement_id"] for s in self.requirement_spans
                if s["superseded_in_version_id"] is None}

    def spans_for(self, requirement_id):
        return [(s["introduced_in_version_id"], s["superseded_in_version_id"])
                for s in self.requirement_spans if s["requirement_id"] == requirement_id]


REGULATION_ID = 42


def _req(local_id, text, source_reference="Article 1"):
    return {
        "requirement_local_id": local_id,
        "description": text,
        "title": text[:20],
        "source_reference": source_reference,
        "source_refs": [{"source_document": "main_body", "source_reference": source_reference}],
        "requirement_type": "Policy and Procedure",
    }


# Fixed text per logical requirement 1-7, reused verbatim across versions so
# ref_key comes out identical every time it reappears -- exactly the
# "resurrected content" case.
TEXT = {n: f"Requirement number {n} obligation text, unchanged." for n in range(1, 8)}


def test_v1_all_five_are_new():
    repo = FakeRepo()
    reqs = [_req(f"R{n}", TEXT[n]) for n in (1, 2, 3, 4, 5)]
    counts = sync_requirements_and_activities(repo, REGULATION_ID, "V1", reqs, [])

    assert counts["requirements_inserted"] == 5
    assert counts["requirements_reactivated"] == 0
    assert counts["requirements_superseded"] == 0
    assert len(repo.requirements) == 5
    assert len(repo.open_requirement_ids()) == 5
    return repo


def test_v2_two_unchanged_three_superseded_one_new():
    repo = test_v1_all_five_are_new()
    req_ids_by_text = {v["ref_key"]: k for k, v in repo.requirements.items()}

    reqs = [_req("R1", TEXT[1]), _req("R2", TEXT[2]), _req("R6", TEXT[6])]
    counts = sync_requirements_and_activities(repo, REGULATION_ID, "V2", reqs, [])

    assert counts["requirements_unchanged"] == 2      # 1, 2
    assert counts["requirements_inserted"] == 1        # 6
    assert counts["requirements_superseded"] == 3      # 3, 4, 5
    assert counts["requirements_reactivated"] == 0     # nothing back yet

    assert len(repo.requirements) == 6                 # 1-6, permanent catalog
    assert len(repo.open_requirement_ids()) == 3        # 1, 2, 6 currently active
    return repo


def test_v3_three_resurrected_one_new_three_superseded():
    """The scenario the whole span design exists for: 3, 4, 5 come back with
    IDENTICAL text to V1 -- same ref_key as before, now belonging to a row
    that's currently superseded. This must reuse the existing requirement_id
    (no duplicate Requirement row, no ref_key collision) and open a SECOND
    span for it."""
    repo = test_v2_two_unchanged_three_superseded_one_new()

    # Capture requirement_ids for 3,4,5 as of V1 (before V3 touches them).
    ref_key_of = {}
    for rid, r in repo.requirements.items():
        for n in (3, 4, 5):
            if r["description"] == TEXT[n]:
                ref_key_of[n] = rid

    reqs = [_req("R3", TEXT[3]), _req("R4", TEXT[4]), _req("R5", TEXT[5]), _req("R7", TEXT[7])]
    counts = sync_requirements_and_activities(repo, REGULATION_ID, "V3", reqs, [])

    assert counts["requirements_reactivated"] == 3    # 3, 4, 5 -- the whole point
    assert counts["requirements_inserted"] == 1        # 7
    assert counts["requirements_superseded"] == 3      # 1, 2, 6
    assert counts["requirements_unchanged"] == 0

    # Permanent catalog stays at 7 -- resurrection must NOT create new rows.
    assert len(repo.requirements) == 7, (
        f"expected 7 permanent Requirement rows, got {len(repo.requirements)} "
        f"-- resurrection duplicated content instead of reusing the id")

    # requirement_id for 3 is the SAME id used in V1/V2 -- identity survives
    # the gap.
    for n in (3, 4, 5):
        rid = ref_key_of[n]
        spans = repo.spans_for(rid)
        assert len(spans) == 2, (
            f"requirement {n} (id={rid}) should have exactly 2 spans "
            f"(V1->V2, V3->open), got {spans}")
        assert spans[0] == ("V1", "V2")
        assert spans[1] == ("V3", None)

    # Currently active as of V3: 3, 4, 5, 7 -- matches what was actually
    # extracted this run.
    active_texts = {repo.requirements[rid]["description"] for rid in repo.open_requirement_ids()}
    assert active_texts == {TEXT[3], TEXT[4], TEXT[5], TEXT[7]}

    print(f"  requirement_ids reused for 3,4,5 across the gap: {ref_key_of}")
    return repo


def test_no_duplicate_open_spans_ever():
    """Sanity check the FakeRepo's own guard actually fired at some point --
    i.e. this test suite would catch a real bug in the sync logic that tried
    to double-open a span, not just a bug in FakeRepo."""
    repo = test_v3_three_resurrected_one_new_three_superseded()
    seen = {}
    for s in repo.requirement_spans:
        if s["superseded_in_version_id"] is None:
            rid = s["requirement_id"]
            assert rid not in seen, f"requirement {rid} has two open spans at once"
            seen[rid] = True


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"  PASS  {name}")
        except Exception as e:
            failures += 1
            print(f"  FAIL  {name}: {type(e).__name__}: {e}")
    print(f"\n{'FAILED' if failures else 'OK'} -- {failures} failure(s)")
    sys.exit(1 if failures else 0)
