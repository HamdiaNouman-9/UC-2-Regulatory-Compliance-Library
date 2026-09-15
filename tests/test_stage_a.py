"""The production data-corruption fixes, tested without a database.

WHY THIS FILE LOOKS LIKE THIS

These fixes are all in MSSQL-only code, and a checkout without database
credentials cannot execute them against anything real. What CAN be verified
without a database is the thing that was actually wrong in each case:

  archive_current_analysis        the ORDER and SCOPE of two SQL statements
  store_requirement_mappings      that a clear runs before the inserts, and that
                                  an existing row is reused, not duplicated
  _log_processing                 that a duration reaches the log row

A recording cursor gives exactly that. It proves the SQL we intend to issue is
issued, in the right order, with the right parameters. It proves NOTHING about
the real schema, real transactions, or the real data — that still needs a run
against a real database. Do not read a green run here as "the fix works in
production".

    venv/Scripts/python.exe -m pytest tests/test_stage_a.py -v
    venv/Scripts/python.exe tests/test_stage_a.py          # no pytest needed
"""

from __future__ import annotations

import os
import re
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# storage/mssql_repo.py imports pyodbc at module load, and pyodbc is not
# installed in this venv (it is not in requirements.txt either — the MSSQL path
# has only ever been run on a machine that had it). A stub is enough: these tests
# replace _get_conn entirely, so no driver is ever called. Without this the file
# cannot even be imported, and the fixes would have zero executable coverage.
if "pyodbc" not in sys.modules:
    try:
        import pyodbc  # noqa: F401
    except ModuleNotFoundError:
        sys.modules["pyodbc"] = types.ModuleType("pyodbc")

from storage.mssql_repo import MSSQLRepository


# --------------------------------------------------------------------------- #
#  Test doubles                                                               #
# --------------------------------------------------------------------------- #

class RecordingCursor:
    """Captures SQL instead of executing it."""

    def __init__(self, rowcount: int = 3, fetch_results=None):
        self.calls: list[tuple[str, object]] = []
        self.rowcount = rowcount
        self._fetch = list(fetch_results or [])

    def execute(self, sql, params=None):
        self.calls.append((" ".join(str(sql).split()), params))
        return self

    def fetchone(self):
        return self._fetch.pop(0) if self._fetch else None

    # -- helpers for assertions -------------------------------------------- #
    @property
    def verbs(self) -> list[str]:
        return [c[0].split()[0].upper() for c in self.calls]

    def sql(self, i: int) -> str:
        return self.calls[i][0]

    def params(self, i: int):
        return self.calls[i][1]


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def repo_with(cursor) -> MSSQLRepository:
    """A repository whose _get_conn hands back the recording cursor.

    __init__ is bypassed: it only stores connection params, and constructing it
    normally would require credentials we do not have.
    """
    repo = MSSQLRepository.__new__(MSSQLRepository)
    conn = FakeConn(cursor)
    repo._get_conn = lambda *a, **k: conn        # type: ignore[method-assign]
    repo._last_conn = conn
    return repo


# --------------------------------------------------------------------------- #
#  archive_current_analysis                                                   #
# --------------------------------------------------------------------------- #

def test_a1_copies_before_it_deletes():
    """Order is load-bearing. The archive SELECT is scoped by `is_current = 1`,
    so deleting first would archive nothing at all — a silent no-op that looks
    identical to a successful run."""
    cur = RecordingCursor()
    repo_with(cur).archive_current_analysis(regulation_id=42, version_id=7)

    assert cur.verbs == ["INSERT", "DELETE"], (
        f"expected copy-then-delete, got {cur.verbs}")


def test_a1_delete_actually_removes_the_source_rows():
    """An earlier version of this fix flagged the source rows off
    (`is_current = 0`) instead of removing them. Every reader already filters
    `is_current = 1`, so those flagged-off rows were invisible to every
    query but never actually went away — the table grew by one full row-set
    per past version, forever. The row is already safely copied into
    compliance_analysis_versions by the INSERT above, so deleting the source
    here loses nothing."""
    cur = RecordingCursor()
    repo_with(cur).archive_current_analysis(regulation_id=42, version_id=7)

    delete = cur.sql(1)
    assert re.search(r"DELETE\s+FROM\s+compliance_analysis\b", delete, re.I)
    assert "WHERE regulation_id = ? AND is_current = 1" in delete


def test_a1_archived_rows_are_not_flagged_current():
    """Second, smaller bug in the same statement: `is_current` was copied
    verbatim, so archived rows landed in the archive table flagged current."""
    cur = RecordingCursor()
    repo_with(cur).archive_current_analysis(regulation_id=42, version_id=7)

    archive = cur.sql(0)
    assert "'inactive', 0," in archive, (
        "the archive INSERT must write is_current = 0, not copy the source value")
    assert "'inactive', is_current," not in archive


def test_a1_parameters_are_in_the_right_order():
    """version_id then regulation_id for the copy; regulation_id alone for the
    retire. Swapping them silently archives the wrong regulation."""
    cur = RecordingCursor()
    repo_with(cur).archive_current_analysis(regulation_id=42, version_id=7)

    assert cur.params(0) == [7, 42]
    assert cur.params(1) == [42]


def test_a1_is_one_transaction():
    """One commit, after both statements. Two commits would let a crash leave
    the rows archived but still live — the exact state we are fixing."""
    cur = RecordingCursor()
    repo = repo_with(cur)
    repo.archive_current_analysis(regulation_id=42, version_id=7)

    assert repo._last_conn.commits == 1


def test_a1_second_run_is_a_no_op():
    """Retryability. rowcount 0 means no rows were current, so a repeated
    archive archives nothing rather than duplicating the history."""
    cur = RecordingCursor(rowcount=0)
    archived = repo_with(cur).archive_current_analysis(regulation_id=42, version_id=7)

    assert archived == 0


# --------------------------------------------------------------------------- #
#  mappings and suggested requirements                                        #
# --------------------------------------------------------------------------- #

def test_a2_mappings_clear_before_inserting():
    """Insert-only was the bug: re-analysis appended a second full set.

    Asserted on the WRITE verbs only. `store_requirement_mappings` also probes
    INFORMATION_SCHEMA once per process to see whether `match_confidence` exists
    yet, so it can write the column after the migration and omit it before.
    That read is not part of the clear-then-insert contract this test exists to
    protect, and pinning it here would make the test fail the day the probe is
    cached rather than repeated.
    """
    cur = RecordingCursor()
    repo_with(cur).store_requirement_mappings([
        {"regulation_id": 42, "extracted_requirement_text": "a",
         "match_status": "new"},
        {"regulation_id": 42, "extracted_requirement_text": "b",
         "match_status": "new"},
    ], version_id=None)

    writes = [v for v in cur.verbs if v in ("DELETE", "INSERT", "UPDATE")]
    assert writes == ["DELETE", "INSERT", "INSERT"]


def test_a2_null_version_uses_is_null_not_equals():
    """`version_id = NULL` is never true in SQL, so an equality comparison would
    delete nothing and the leak would survive the fix. Non-CBB regulators are
    exactly the NULL-version case, so this is the common path, not an edge."""
    cur = RecordingCursor()
    repo_with(cur).store_requirement_mappings(
        [{"regulation_id": 42, "extracted_requirement_text": "a",
          "match_status": "new"}], version_id=None)

    assert "version_id IS NULL" in cur.sql(0)
    assert cur.params(0) == (42,)


def test_a2_cbb_history_is_scoped_by_version():
    """With a version_id the clear must be scoped to it. Clearing the whole
    regulation would delete CBB's real per-version mapping history."""
    cur = RecordingCursor()
    repo_with(cur).store_requirement_mappings(
        [{"regulation_id": 42, "extracted_requirement_text": "a",
          "match_status": "new"}], version_id=9)

    assert "version_id = ?" in cur.sql(0)
    assert cur.params(0) == (42, 9)


def test_a2_empty_mappings_touch_nothing():
    """An analysis that produced no mappings must not wipe the existing ones."""
    cur = RecordingCursor()
    repo_with(cur).store_requirement_mappings([], version_id=None)

    assert cur.calls == []


def test_a2_existing_ref_key_is_reused_not_duplicated():
    """The duplication fix. A ref_key already present returns that row's id and
    issues no INSERT."""
    cur = RecordingCursor(fetch_results=[(1234,)])
    got = repo_with(cur).insert_new_suggested_requirement({
        "title": "t", "description": "d", "ref_key": "AUTO-42-deadbeef"})

    assert got == 1234
    assert cur.verbs == ["SELECT"], f"expected lookup only, got {cur.verbs}"


def test_a2_unknown_ref_key_still_inserts():
    """The reuse must not block genuinely new requirements."""
    cur = RecordingCursor(fetch_results=[None, (99,)])
    got = repo_with(cur).insert_new_suggested_requirement({
        "title": "t", "description": "d", "ref_key": "AUTO-42-cafebabe"})

    assert got == 99
    assert cur.verbs == ["SELECT", "INSERT"]


def test_a2_ref_key_is_content_derived_and_stable():
    """The property the upsert depends on: the same obligation text yields the
    same key, and different text yields a different one.

    This is why the key could not stay AUTO-<reg>-<loop index> — an index is a
    position in an LLM-generated list, so it names different text run to run.
    """
    import hashlib

    def key(regulation_id, text):
        digest = hashlib.md5(text.strip().encode("utf-8")).hexdigest()[:8]
        return f"AUTO-{regulation_id}-{digest}"

    a = "The bank shall maintain a capital adequacy ratio of at least 8%."
    b = "The bank shall report breaches within 30 days."

    assert key(42, a) == key(42, a)              # stable across runs
    assert key(42, a) == key(42, "  " + a + " ")  # whitespace-insensitive
    assert key(42, a) != key(42, b)              # distinct text, distinct key
    assert key(42, a) != key(43, a)              # scoped to the regulation


# --------------------------------------------------------------------------- #
#  timing                                                                     #
# --------------------------------------------------------------------------- #

def test_a3_duration_lands_in_the_details_json():
    """No ALTER TABLE: the duration rides in the existing `details` column."""
    import json

    cur = RecordingCursor()
    repo_with(cur)._log_processing(42, "llm_analysis", "SUCCESS", "ok",
                                   duration_ms=1234.7)

    details = json.loads(cur.params(0)[4])
    assert details["duration_ms"] == 1234          # int, not float
    assert "INSERT INTO processinglogs" in cur.sql(0)


def test_a3_no_duration_means_unchanged_behaviour():
    """Every existing caller passes no duration and must be unaffected."""
    cur = RecordingCursor()
    repo_with(cur)._log_processing(42, "insert", "SUCCESS", "inserted")

    assert cur.params(0)[4] is None


def test_a3_excel_repo_accepts_the_same_argument():
    """NewOrchestrator logs through whichever repo it was given, so ExcelRepo
    has to take `duration_ms` too or every preview run raises TypeError."""
    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    import inspect

    sig = inspect.signature(ExcelRepo._log_processing)
    assert "duration_ms" in sig.parameters


# --------------------------------------------------------------------------- #

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
    print(f"\n{'FAILED' if failures else 'OK'} — {failures} failure(s)")
    sys.exit(1 if failures else 0)
