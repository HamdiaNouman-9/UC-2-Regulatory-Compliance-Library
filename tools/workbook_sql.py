"""Turn a checked workbook into plain SQL scripts, for a database that `promote`
cannot reach quickly (prod over VPN) or should not write to row by row.

    python -m tools.workbook_sql delete "Qatar Central Bank (QCB)" --out output/sql/delete_qcb.sql
    python -m tools.workbook_sql insert output/workbooks/qcb.xlsx   --out output/sql/insert_qcb.sql

Then run either file with sqlcmd (large files do not fit an editor):

    sqlcmd -S 10.11.12.76,1437 -d regulatory_monitoring -U devuser -P <pwd> -I -b -f 65001 -i output\\sql\\insert_qcb.sql

WHY: `promote` makes several round trips per row. Over the VPN to prod that was
5+ hours for QCB's 463 rows, and because it inserts EVERY regulation before ANY
version, an interrupted run leaves every inserted row without a version (local
QFCL: 2,183 of them, 2026-09-29). A script runs inside the server in minutes and
inside ONE transaction, so it either lands whole or not at all.

WHAT THE INSERT SCRIPT WRITES is exactly what `promote` writes, because the row
values come from the same code: each workbook row goes through
`MSSQLRepository._insert_regulation` against a recording cursor, and the SQL and
parameters it would have sent are what the script contains.

  * folders    get-or-create by (title, parent), then anywhere in the parent's
               subtree -- promote's `get_folder_id` + `find_folder_in_subtree`.
               A top folder is hung under its country (config/countries.yml).
  * regulation inserted only if no row has the same regulator, title, link,
               doc_path and attachment_links -- so it is safe to re-run.
  * ref_key    <prefix> + the new id, as `compute_regulation_ref_key`.
  * version    the workbook's versions for that regulation, created_at GETDATE().

THE DELETE SCRIPT removes one regulator's regulations and everything that hangs
off them (versions, requirements, activities, their spans, mappings, gap
analysis). Folders are kept: the insert script reuses them. Tables that do not
exist on the target are skipped rather than failing.

Both scripts run in ONE transaction. If any statement fails, SET NOEXEC ON stops
every later batch from executing, so nothing half-done is committed.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import decimal
import json
import os
import sys
from pathlib import Path
from typing import List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

BATCH = 100          # regulations per GO batch


# --------------------------------------------------------------------------- #
#  literals                                                                    #
# --------------------------------------------------------------------------- #

def lit(v) -> str:
    """A T-SQL literal for one parameter value."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float, decimal.Decimal)):
        return str(v)
    if isinstance(v, (_dt.datetime, _dt.date)):
        return "N'" + v.isoformat() + "'"
    s = str(v).replace("'", "''")
    return "N'" + s + "'"


def _guard(body: str) -> str:
    """Stop running if an earlier batch rolled the transaction back."""
    return ("IF @@TRANCOUNT = 0 BEGIN RAISERROR('Transaction is gone: an earlier "
            "statement failed. Nothing more will run; nothing was committed.', "
            "16, 1); SET NOEXEC ON; END;\n" + body)


# --------------------------------------------------------------------------- #
#  delete                                                                      #
# --------------------------------------------------------------------------- #

# (table, WHERE clause over the ids collected first). Order is child -> parent.
_DELETE_STEPS = [
    ("ActivitySpan",
     "activity_id IN (SELECT a.activity_id FROM Activity a JOIN Requirement q "
     "ON q.requirement_id = a.requirement_id WHERE q.regulation_id IN (SELECT id FROM #del_reg)) "
     "OR introduced_in_version_id IN (SELECT version_id FROM #del_ver) "
     "OR superseded_in_version_id IN (SELECT version_id FROM #del_ver)"),
    ("Activity",
     "requirement_id IN (SELECT requirement_id FROM Requirement WHERE regulation_id IN (SELECT id FROM #del_reg))"),
    ("RequirementSpan",
     "requirement_id IN (SELECT requirement_id FROM Requirement WHERE regulation_id IN (SELECT id FROM #del_reg)) "
     "OR introduced_in_version_id IN (SELECT version_id FROM #del_ver) "
     "OR superseded_in_version_id IN (SELECT version_id FROM #del_ver)"),
    ("Requirement", "regulation_id IN (SELECT id FROM #del_reg)"),
    ("sama_requirement_mapping",
     "regulation_id IN (SELECT id FROM #del_reg) OR version_id IN (SELECT version_id FROM #del_ver)"),
    ("gap_analysis", "regulation_id IN (SELECT id FROM #del_reg)"),
    ("regulation_versions", "regulation_id IN (SELECT id FROM #del_reg)"),
    ("regulations", "id IN (SELECT id FROM #del_reg)"),
]


def delete_script(regulator: str) -> str:
    r = lit(regulator)
    out = [f"-- Delete every regulation of {regulator} and what depends on it.",
           "-- Generated by tools/workbook_sql.py. Folders are kept.",
           "SET NOCOUNT ON; SET XACT_ABORT ON;",
           "PRINT 'database: ' + DB_NAME();",
           "IF OBJECT_ID('tempdb..#del_reg') IS NOT NULL DROP TABLE #del_reg;",
           "IF OBJECT_ID('tempdb..#del_ver') IS NOT NULL DROP TABLE #del_ver;",
           f"SELECT id INTO #del_reg FROM regulations WHERE regulator = {r};",
           "SELECT version_id INTO #del_ver FROM regulation_versions "
           "WHERE regulation_id IN (SELECT id FROM #del_reg);",
           # PRINT cannot hold a subquery (Msg 1046), so the counts go through
           # variables first.
           "DECLARE @nr INT, @nv INT;",
           "SELECT @nr = COUNT(*) FROM #del_reg; SELECT @nv = COUNT(*) FROM #del_ver;",
           "PRINT 'regulations to delete: ' + CAST(@nr AS varchar(20));",
           "PRINT 'versions to delete:    ' + CAST(@nv AS varchar(20));",
           "BEGIN TRANSACTION;",
           "GO"]
    for table, where in _DELETE_STEPS:
        # Dynamic SQL, so a table absent on this database is skipped instead of
        # failing the batch at compile time.
        sql = f"DELETE FROM {table} WHERE {where}"
        out.append(_guard(
            f"IF OBJECT_ID(N'{table}', N'U') IS NOT NULL\n"
            f"BEGIN\n"
            f"  EXEC(N'{sql.replace(chr(39), chr(39) * 2)}');\n"
            f"  PRINT '{table}: ' + CAST(@@ROWCOUNT AS varchar(20)) + ' deleted';\n"
            f"END\n"
            f"ELSE PRINT '{table}: table not present, skipped';"))
        out.append("GO")
    out.append(_guard(
        f"IF EXISTS (SELECT 1 FROM regulations WHERE regulator = {r})\n"
        f"BEGIN RAISERROR('Rows remain after delete -- rolling back.', 16, 1); ROLLBACK; SET NOEXEC ON; END\n"
        f"ELSE BEGIN COMMIT; PRINT 'COMMITTED: {regulator.replace(chr(39), chr(39) * 2)} deleted.'; END"))
    out.append("GO")
    out.append("SET NOEXEC OFF;")
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------- #
#  insert                                                                      #
# --------------------------------------------------------------------------- #

class _RecordingCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=()):
        self.log.append((sql, tuple(params)))
        return self

    def fetchone(self):
        return [0]              # the "new id"; replaced by SCOPE_IDENTITY() below


class _RecordingConn:
    def __init__(self, log):
        self.log = log

    def cursor(self):
        return _RecordingCursor(self.log)

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _recorded_insert(repo, doc) -> tuple:
    """(INSERT sql without OUTPUT, params) that `_insert_regulation` would send."""
    log: list = []
    repo._get_conn = lambda *a, **k: _RecordingConn(log)
    repo._insert_regulation(doc)
    sql, params = log[0]
    sql = sql.replace("OUTPUT INSERTED.id", "")
    return sql, params


def insert_script(xlsx: Path) -> str:
    from dynamic_crawler.formfill.promote import _read, _Doc, _int
    from storage.mssql_repo import MSSQLRepository, compute_regulation_ref_key
    from utils.countries import country_for

    data = _read(xlsx)
    regs = data["regulations"]
    if not regs:
        raise SystemExit(f"{xlsx}: no regulations")
    cats = {int(float(c["compliancecategory_id"])): c for c in data["compliancecategory"]
            if str(c.get("compliancecategory_id", "")).strip().replace(".0", "").isdigit()}
    versions_by_reg: dict = {}
    for v in data["regulation_versions"]:
        versions_by_reg.setdefault(_int(v.get("regulation_id")), []).append(v)

    regulator = regs[0].get("regulator") or ""
    out = [f"-- Insert {len(regs)} regulation(s) of {regulator} from {xlsx.name}.",
           "-- Generated by tools/workbook_sql.py -- same values promote writes.",
           "SET NOCOUNT ON; SET XACT_ABORT ON;",
           "PRINT 'database: ' + DB_NAME();",
           "IF OBJECT_ID('tempdb..#fmap') IS NOT NULL DROP TABLE #fmap;",
           "CREATE TABLE #fmap (wb_id INT PRIMARY KEY, db_id INT NOT NULL);",
           "IF OBJECT_ID('tempdb..#stats') IS NOT NULL DROP TABLE #stats;",
           "CREATE TABLE #stats (inserted INT, skipped INT, versions INT);",
           "INSERT INTO #stats VALUES (0, 0, 0);",
           "BEGIN TRANSACTION;",
           "GO"]

    # ---- folders: parent-first, the same get-or-create promote does -------- #
    order: List[int] = []
    seen = set()

    def visit(cid):
        if cid in seen or cid not in cats:
            return
        p = _int(cats[cid].get("parentid"))
        if p is not None:
            visit(p)
        seen.add(cid)
        order.append(cid)

    for cid in sorted(cats):
        visit(cid)

    # QFCL has 9,707 folders: one batch that size is slow for the server to
    # compile, so folders go out 300 per GO batch (#fmap carries across).
    FOLDERS_PER_BATCH = 300
    body = ["DECLARE @p INT, @f INT, @country INT;"]
    nfold = 0
    for cid in order:
        if nfold and nfold % FOLDERS_PER_BATCH == 0:
            out.append(_guard("\n".join(body)))
            out.append("GO")
            body = ["DECLARE @p INT, @f INT, @country INT;"]
        nfold += 1
        c = cats[cid]
        title = str(c.get("title") or "").strip()
        if not title:
            continue
        ctype = lit(str(c.get("type") or "F"))
        parent_wb = _int(c.get("parentid"))
        if parent_wb is not None:
            body.append(f"SELECT @p = db_id FROM #fmap WHERE wb_id = {parent_wb};")
        else:
            ctry = country_for(title)
            if ctry:
                body.append(
                    f"SET @country = NULL; SELECT TOP 1 @country = compliancecategory_id FROM compliancecategory "
                    f"WHERE title = {lit(ctry)} AND parentid IS NULL;\n"
                    f"IF @country IS NULL BEGIN INSERT INTO compliancecategory (title, parentid, type) "
                    f"VALUES ({lit(ctry)}, NULL, N'F'); SET @country = SCOPE_IDENTITY(); END;")
                body.append("SET @p = @country;")
            else:
                body.append("SET @p = NULL;")
        body.append(
            f"SET @f = NULL; SELECT TOP 1 @f = compliancecategory_id FROM compliancecategory "
            f"WHERE title = {lit(title)} AND ((@p IS NULL AND parentid IS NULL) OR parentid = @p);\n"
            # A CTE cannot follow a bare IF in T-SQL, hence BEGIN ... END.
            f"IF @f IS NULL AND @p IS NOT NULL\nBEGIN\n"
            f"  ;WITH tree AS (SELECT compliancecategory_id, title, parentid FROM compliancecategory "
            f"WHERE compliancecategory_id = @p UNION ALL SELECT c.compliancecategory_id, c.title, c.parentid "
            f"FROM compliancecategory c JOIN tree t ON c.parentid = t.compliancecategory_id)\n"
            f"  SELECT TOP 1 @f = compliancecategory_id FROM tree WHERE title = {lit(title)} "
            f"AND compliancecategory_id <> @p;\nEND;\n"
            f"IF @f IS NULL BEGIN INSERT INTO compliancecategory (title, parentid, type) "
            f"VALUES ({lit(title)}, @p, {ctype}); SET @f = SCOPE_IDENTITY(); END;\n"
            f"INSERT INTO #fmap VALUES ({cid}, @f);")
    out.append(_guard("\n".join(body)))
    out.append("GO")

    # ---- regulations + versions, BATCH per GO ----------------------------- #
    repo = MSSQLRepository({"server": "-", "database": "-", "driver": "-"})
    for start in range(0, len(regs), BATCH):
        chunk = regs[start:start + BATCH]
        body = ["DECLARE @r BIGINT, @c INT;"]
        for row in chunk:
            doc = _Doc(row)
            doc.compliancecategory_id = None          # set from #fmap below
            sql, params = _recorded_insert(repo, doc)
            params = list(params)
            # compliancecategory_id is the 13th parameter of _insert_regulation.
            wb_cat = _int(row.get("compliancecategory_id"))
            params[12] = "@c"
            values = ", ".join("@c" if (i == 12) else lit(p) for i, p in enumerate(params))
            placeholder = "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
            if placeholder not in sql:
                raise SystemExit("mssql_repo._insert_regulation changed shape -- "
                                 "update tools/workbook_sql.py to match")
            sql_final = sql.replace(placeholder, f"VALUES ({values},")
            meta = getattr(doc, "extra_meta", None) or {}
            links = (meta.get("attachment_links") or "") if isinstance(meta, dict) else ""
            doc_path_json = params[5]
            prefix = compute_regulation_ref_key(doc.regulator, doc.source_system, 0)[:-1]
            vers = versions_by_reg.get(_int(row.get("id")), [])
            vsql = []
            for v in vers:
                vsql.append(
                    "  INSERT INTO regulation_versions (regulation_id, regulator, content_html, "
                    "content_text, content_hash, updated_date, change_summary, status, created_at) "
                    f"VALUES (@r, {lit(doc.regulator or '')}, {lit(v.get('content_html') or '')}, "
                    f"{lit(v.get('content_text') or '')}, {lit(v.get('content_hash') or '')}, "
                    f"{lit(v.get('updated_date'))}, {lit(v.get('change_summary') or '')}, "
                    f"{lit(v.get('status') or 'active')}, GETDATE());\n"
                    "  UPDATE #stats SET versions = versions + 1;")
            body.append(
                f"SET @c = NULL; SELECT @c = db_id FROM #fmap WHERE wb_id = {lit(wb_cat)};\n"
                f"IF NOT EXISTS (SELECT 1 FROM regulations WHERE regulator = {lit(doc.regulator)} "
                f"AND ISNULL(title, N'') = {lit(doc.title or '')} "
                f"AND ISNULL(document_url, N'') = {lit(doc.document_url or '')} "
                f"AND ISNULL(CAST(doc_path AS NVARCHAR(MAX)), N'') = {lit(doc_path_json or '')} "
                # ISJSON first: one malformed extra_meta anywhere in the shared
                # table would otherwise make JSON_VALUE fail the whole batch.
                f"AND ISNULL(CASE WHEN ISJSON(extra_meta) = 1 THEN "
                f"JSON_VALUE(extra_meta, '$.attachment_links') END, N'') = {lit(links)})\n"
                f"BEGIN\n  {sql_final.strip()};\n"
                f"  SET @r = SCOPE_IDENTITY();\n"
                f"  UPDATE regulations SET ref_key = {lit(prefix)} + CAST(@r AS NVARCHAR(20)) WHERE id = @r;\n"
                f"  UPDATE #stats SET inserted = inserted + 1;\n"
                + ("\n".join(vsql) + "\n" if vsql else "")
                + "END\nELSE UPDATE #stats SET skipped = skipped + 1;")
        out.append(_guard("\n".join(body)))
        out.append(f"PRINT 'rows {start + 1}-{start + len(chunk)} of {len(regs)} done';")
        out.append("GO")

    out.append(_guard(
        "DECLARE @i INT, @s INT, @v INT; SELECT @i = inserted, @s = skipped, @v = versions FROM #stats;\n"
        "COMMIT;\n"
        "PRINT 'COMMITTED. inserted=' + CAST(@i AS varchar(20)) + ' already_present=' + "
        "CAST(@s AS varchar(20)) + ' versions=' + CAST(@v AS varchar(20));"))
    out.append("GO")
    out.append("SET NOEXEC OFF;")
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("delete", help="script deleting one regulator's rows")
    d.add_argument("regulator")
    d.add_argument("--out", required=True)
    i = sub.add_parser("insert", help="script inserting a workbook's rows")
    i.add_argument("workbook")
    i.add_argument("--out", required=True)
    a = ap.parse_args()
    text = delete_script(a.regulator) if a.cmd == "delete" else insert_script(Path(a.workbook))
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size / 1_048_576:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
