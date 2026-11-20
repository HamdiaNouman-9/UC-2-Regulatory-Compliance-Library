"""CHG-032: a library entry that is both a page and a folder.

One compliancecategory node can own a document AND have sub-categories
("Banking Control Law": the law's own text, plus 26 Article nodes beneath
it). The one-letter `type` column (F/R) cannot say "both", so:

  * the API derives `has_children`, `child_count` and `kind`
    (folder / document / both) from the data on every category row;
  * the repository retypes a node to R the moment a regulation is filed
    into it, so `type` means "owns a document" and nothing else;
  * the web page renders from `kind` (checked in a browser, not here).

These tests run with no database: the cursor and connection are fakes that
record the SQL they were given.
"""
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apis.pipeline_api import _attach_regulation_counts, category_kind  # noqa: E402
from scripts.chg032_retype_categories import orphan_ids  # noqa: E402
from storage.mssql_repo import MSSQLRepository  # noqa: E402


# --------------------------------------------------------------------------- #
#  the derived kind                                                             #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("regs, kids, expected", [
    (True, True, "both"),
    (True, False, "document"),
    (False, True, "folder"),
    (False, False, "folder"),
])
def test_kind_is_derived_from_the_two_facts(regs, kids, expected):
    assert category_kind(regs, kids) == expected


# --------------------------------------------------------------------------- #
#  the API helper: one aggregate per fact, looked up by id                      #
# --------------------------------------------------------------------------- #

class FakeCursor:
    """Answers the two GROUP BY queries the helper runs, records every SQL."""

    def __init__(self, reg_counts, child_counts):
        self.reg_counts = reg_counts
        self.child_counts = child_counts
        self.executed = []
        self._rows = []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))
        if "FROM regulations" in sql:
            self._rows = list(self.reg_counts.items())
        elif "FROM compliancecategory" in sql:
            self._rows = list(self.child_counts.items())
        else:  # pragma: no cover - the helper runs nothing else
            raise AssertionError(sql)

    def fetchall(self):
        return [tuple(r) for r in self._rows]


def _cats(*ids):
    return [{"compliancecategory_id": i, "title": f"node {i}", "parentid": None, "type": "F"}
            for i in ids]


def test_every_row_gets_both_facts_and_the_kind():
    cats = _cats(1, 2, 3, 4)
    cur = FakeCursor(reg_counts={1: 1, 2: 12}, child_counts={1: 26, 3: 5})
    _attach_regulation_counts(cur, cats)
    by_id = {c["compliancecategory_id"]: c for c in cats}

    assert by_id[1]["kind"] == "both"
    assert by_id[1]["has_children"] is True and by_id[1]["child_count"] == 26
    assert by_id[1]["has_regulations"] is True and by_id[1]["regulation_count"] == 1

    assert by_id[2]["kind"] == "document"
    assert by_id[2]["child_count"] == 0 and by_id[2]["has_children"] is False

    assert by_id[3]["kind"] == "folder"
    assert by_id[3]["child_count"] == 5 and by_id[3]["regulation_count"] == 0

    assert by_id[4]["kind"] == "folder"
    assert by_id[4]["child_count"] == 0 and by_id[4]["regulation_count"] == 0


def test_the_helper_runs_two_unparameterised_aggregates_whatever_the_list_size():
    # MEASURED 2026-08-28: a per-id IN (...) list blew past the ODBC parameter
    # limit on the full tree. The child count must follow the same rule.
    cats = _cats(*range(1, 9001))
    cur = FakeCursor(reg_counts={}, child_counts={})
    _attach_regulation_counts(cur, cats)
    assert len(cur.executed) == 2
    for sql, params in cur.executed:
        assert params is None
        assert "?" not in sql
    assert "GROUP BY parentid" in cur.executed[1][0]
    assert "WHERE parentid IS NOT NULL" in cur.executed[1][0]


def test_an_empty_list_still_costs_two_queries_and_nothing_else():
    cur = FakeCursor(reg_counts={}, child_counts={})
    _attach_regulation_counts(cur, [])
    assert len(cur.executed) == 2


# --------------------------------------------------------------------------- #
#  the repository: a node is R from the moment it owns a document               #
# --------------------------------------------------------------------------- #

class RecordingCursor:
    def __init__(self):
        self.executed = []
        self.description = []

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), list(params) if params is not None else None))

    def fetchone(self):
        return (4242,)


class RecordingConn:
    def __init__(self):
        self.cursor_obj = RecordingCursor()
        self.committed = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed += 1

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _repo_with(conn):
    repo = MSSQLRepository.__new__(MSSQLRepository)
    repo._get_conn = lambda *a, **k: conn
    return repo


def _retypes(cursor):
    return [(sql, params) for sql, params in cursor.executed
            if sql.startswith("UPDATE compliancecategory SET type = 'R'")]


def test_mark_category_owns_document_is_one_guarded_update():
    cur = RecordingCursor()
    repo = _repo_with(RecordingConn())
    repo.mark_category_owns_document(cur, 77)
    assert len(cur.executed) == 1
    sql, params = cur.executed[0]
    assert sql == ("UPDATE compliancecategory SET type = 'R' "
                   "WHERE compliancecategory_id = ? AND (type IS NULL OR type <> 'R')")
    assert params == [77]


def test_mark_category_owns_document_does_nothing_for_no_node():
    cur = RecordingCursor()
    repo = _repo_with(RecordingConn())
    repo.mark_category_owns_document(cur, None)
    assert cur.executed == []


def _doc(category_id):
    return types.SimpleNamespace(
        regulator="SAMA", source_system="sama_rulebook", category="Laws",
        title="Banking Control Law", document_url="https://example.test/law",
        doc_path=["Laws", "Banking Control Law"], published_date=None,
        reference_no=None, department=None, year=None, source_page_url=None,
        extra_meta={}, document_html="<p>text</p>", type="R", status="",
        content_hash="0" * 32, compliancecategory_id=category_id)


def test_inserting_a_regulation_retypes_its_node_in_the_same_transaction():
    conn = RecordingConn()
    repo = _repo_with(conn)
    rid = repo._insert_regulation(_doc(55))
    assert rid == 4242
    cur = conn.cursor_obj
    kinds = [sql.split()[0] + " " + sql.split()[1] for sql, _ in cur.executed]
    assert kinds == ["INSERT INTO", "UPDATE regulations", "UPDATE compliancecategory"]
    assert _retypes(cur)[0][1] == [55]
    # one commit, after the retype: the node cannot be left F with a row in it
    assert conn.committed == 1


def test_inserting_a_regulation_with_no_node_retypes_nothing():
    conn = RecordingConn()
    repo = _repo_with(conn)
    repo._insert_regulation(_doc(None))
    assert _retypes(conn.cursor_obj) == []


def test_moving_a_regulation_retypes_the_new_node():
    conn = RecordingConn()
    repo = _repo_with(conn)
    repo.update_regulation(9, compliancecategory_id=88, title="moved")
    cur = conn.cursor_obj
    assert len(cur.executed) == 2
    assert cur.executed[0][0].startswith("UPDATE regulations SET")
    assert _retypes(cur)[0][1] == [88]


def test_an_update_that_does_not_move_the_regulation_retypes_nothing():
    conn = RecordingConn()
    repo = _repo_with(conn)
    repo.update_regulation(9, title="renamed", content_hash="1" * 32)
    assert len(conn.cursor_obj.executed) == 1
    assert _retypes(conn.cursor_obj) == []


# --------------------------------------------------------------------------- #
#  the stored-rows script: which nodes it may delete                            #
# --------------------------------------------------------------------------- #

def test_only_childless_r_nodes_without_a_document_are_offered_for_deletion():
    nodes = {
        1: {"id": 1, "title": "dead leaf", "parentid": 9, "type": "R"},
        2: {"id": 2, "title": "folder typed R", "parentid": 9, "type": "R"},
        3: {"id": 3, "title": "a document", "parentid": 9, "type": "R"},
        4: {"id": 4, "title": "empty folder", "parentid": 9, "type": "F"},
        5: {"id": 5, "title": "untyped empty", "parentid": 9, "type": None},
    }
    regs = {3: 1}
    kids = {2: 4}
    childless, with_kids = orphan_ids(nodes, regs, kids)
    assert childless == [1]
    assert with_kids == [2]
