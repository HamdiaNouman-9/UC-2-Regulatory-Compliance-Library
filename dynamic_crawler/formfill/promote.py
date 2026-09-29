"""promote.py — put an APPROVED workbook into MSSQL.

The second half of the review workflow. A run writes a workbook and touches no
database; a human reads it; and if it is right, this puts exactly those rows in.

    crawl -> classify -> .xlsx  ->  [ you read it ]  ->  promote -> MSSQL

WHY REPLAY THE WORKBOOK RATHER THAN RE-RUN THE PIPELINE

Re-running against MSSQL would be less code, and wrong. It would re-crawl (the
site may have changed since you looked), re-analyse (the LLM is not
deterministic, and it costs money you have already spent), and could therefore
insert something OTHER than what you approved. Approval has to mean "these rows",
not "whatever this regulator looks like now".

So this reads the sheets and writes them, remapping ids as it goes: the workbook
counts from 1 in its own little world and the database has its own identity
columns, so every id is translated through a map rather than copied.

WHAT IT SKIPS

A document already in the database — matched on the same identity the
orchestrator classifies with, (document_url, doc_path) — is left alone. Promoting
the same workbook twice therefore inserts nothing the second time, which is what
makes this safe to retry after a partial failure.

    venv/Scripts/python.exe -m dynamic_crawler.formfill.promote path/to/run.xlsx
    venv/Scripts/python.exe -m dynamic_crawler.formfill.promote path/to/run.xlsx --dry-run
    venv/Scripts/python.exe -m dynamic_crawler.formfill.promote path/to/run.xlsx --dry-run --with-db

A plain --dry-run opens no connection, which also means it cannot answer "is this
already in the library?" — `skipped_already_present` is then structurally 0. Add
--with-db to let a dry run read (and only read) so that number means something.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from dynamic_crawler.changesignal import find_existing
from typing import Dict, Optional
from utils.countries import country_for

logger = logging.getLogger(__name__)

SHEETS = ("compliancecategory", "regulations", "regulation_versions",
          "compliance_analysis", "requirement_mappings")


def _find_existing(repo, row: dict, url, path):
    """Is this row already in the library?

    THE ORCHESTRATOR'S IMPLEMENTATION, not a second one. This function used to
    reimplement identity here, and every promote bug found on 2026-08-15 was a
    rule `orch` already had and this copy did not — most damagingly, a row with
    an empty document_url skipped the check entirely and was re-inserted on
    every run. An empty document_url is not an edge case: it is the
    multi-attachment convention.

    `changesignal.find_existing` reads a workbook ROW (a dict) exactly as it
    reads a crawled DOCUMENT (an object), so there is one answer to "is this the
    same document?" for both paths.
    """
    return find_existing(repo, row)


def _nan_to_none(v):
    """NaN out, None in — `df.where(df.notna(), None)` above is not enough.

    A column that is empty on every row is read as float64, and float64 CANNOT
    HOLD None: pandas coerces the None straight back to NaN. The value then
    reaches pyodbc as a Python float, which binds it as SQL float, and the
    server rejects it before looking at the column's real type:

        Parameter 10 (""): The supplied value is not a valid instance of data
        type float ... (8023)

    Parameter 10 is `year`, which is nvarchar and would have taken NULL happily.
    All 11 AML rows failed on it, 2026-08-14, and the message names float rather
    than the column, so it reads like a schema mismatch instead of an empty cell.
    """
    if isinstance(v, float) and v != v:      # NaN is the only value != itself
        return None
    # pandas NaT for an all-empty date column, same mechanism.
    if v is not None and type(v).__name__ in ("NaTType", "NAType"):
        return None
    return v


def _read(path: Path) -> Dict[str, list]:
    """Every sheet, with oversized cells restored to full length.

    A cell tops out at 32,767 characters, so ExcelRepo parks anything longer in a
    `.fulltext.json` sidecar and leaves a marker. Reading the cell alone would
    copy the PREVIEW into MSSQL and make the truncation permanent — a 92,995
    character GOSI instrument would arrive as 32,028, its text stopping mid-way
    through Article 26.
    """
    import pandas as pd
    from dynamic_crawler.formfill.excel_repo import resolve_overflow

    sidecar = {}
    sc = path.with_suffix(".fulltext.json")
    if sc.exists():
        try:
            sidecar = json.loads(sc.read_text(encoding="utf-8"))
            logger.info("loaded %d oversized value(s) from %s", len(sidecar), sc.name)
        except Exception as e:
            # Promoting truncated content silently is the failure this whole
            # mechanism exists to prevent, so say so rather than carrying on.
            logger.error("could not read the fulltext sidecar %s: %s — long "
                         "values would be promoted TRUNCATED", sc, e)

    out = {}
    for s in SHEETS:
        try:
            df = pd.read_excel(path, sheet_name=s)
        except Exception:
            out[s] = []
            continue
        rows = [r for r in df.where(df.notna(), None).to_dict("records")
                if any(v not in (None, "") for v in r.values())]
        out[s] = [{k: _nan_to_none(resolve_overflow(v, sidecar))
                   for k, v in r.items()}
                  for r in rows]
    return out


def _as_list(v) -> list:
    """doc_path back to a list, however the workbook flattened it."""
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return list(v)
    s = str(v).strip()
    if s.startswith("["):
        try:
            return list(json.loads(s))
        except Exception:
            return [s]
    for sep in (" > ", " | "):
        if sep in s:
            return s.split(sep)
    return [s] if s else []


class _Doc:
    """A row from the workbook, shaped like the document `_insert_regulation`
    expects. It reads attributes, not dict keys."""

    def __init__(self, row: dict):
        for k, v in row.items():
            setattr(self, str(k), v)
        self.doc_path = _as_list(row.get("doc_path"))
        # EMPTY STATUS IS A DECISION NOBODY HAS MADE YET, and it must survive
        # the trip into MSSQL. `_insert_regulation` reads None as "this code path
        # never set it" and substitutes "active"; it keeps "" as-is. An empty
        # Excel cell arrives as NaN and _nan_to_none turns it into None, which
        # lands it on the wrong side of that test — all 11 AML rows were stored
        # `active` on 2026-08-14, which is the state that decides whether a row
        # moves into the main system database. So it is pinned back to "".
        self.status = "" if row.get("status") in (None, "") else row.get("status")
        self.extra_meta = {}
        raw = row.get("extra_meta")
        if isinstance(raw, str) and raw.strip().startswith("{"):
            try:
                self.extra_meta = json.loads(raw)
            except Exception:
                pass


def promote(xlsx: Path, repo, dry_run: bool = False) -> dict:
    """Insert an approved workbook's rows into `repo`. Returns a report."""
    data = _read(xlsx)
    regs = data["regulations"]
    if not regs:
        return {"error": f"{xlsx.name}: no rows on the `regulations` sheet"}

    folder_map: Dict[int, int] = {}     # workbook category id -> db id
    reg_map: Dict[int, int] = {}        # workbook regulation id -> db id
    inserted, skipped, failed = [], [], []

    # ---- folder tree ---------------------------------------------------- #
    # Walked parent-first so a child never looks for a parent that is not in
    # the map yet. The workbook's own ids are only meaningful inside it.
    cats = {int(c["compliancecategory_id"]): c for c in data["compliancecategory"]
            if str(c.get("compliancecategory_id", "")).strip().isdigit()}

    def resolve_folder(cid) -> Optional[int]:
        if cid is None or not str(cid).strip().replace(".0", "").isdigit():
            return None
        cid = int(float(cid))
        if cid in folder_map:
            return folder_map[cid]
        c = cats.get(cid)
        if c is None:
            return None
        parent_db = resolve_folder(c.get("parentid"))
        title = str(c.get("title") or "").strip()
        if not title:
            return None
        # A node with no parent IN THE WORKBOOK is the regulator: workbooks are
        # exported with the regulator at the root, because that is where
        # doc_path starts. The DATABASE puts the regulator under its country, so
        # resolving this against parent=None would not find the existing node
        # and would INSERT A SECOND ONE at the root — two "Central Bank of
        # Bahrain" folders, one holding the old documents and one the new.
        #
        # doc_path is untouched by any of this (it is an identity field); only
        # where the tree hangs changes. See config/countries.yml.
        if parent_db is None:
            ctry = country_for(title)
            if ctry:
                parent_db = repo.get_folder_id(ctry, None)
                if parent_db is None and not dry_run:
                    parent_db = repo.insert_folder(ctry, None, cat_type="F")
        if dry_run:
            folder_map[cid] = -cid
            return folder_map[cid]
        # Exact (title, parent) only -- no subtree fallback, or a folder that is
        # correct in the workbook collides again against a same-named folder
        # deeper in the DB tree. Same rule as orchestrator._walk_folders; see
        # docs/FOLDER_TREE.md.
        db_id = repo.get_folder_id(title, parent_db)
        if db_id is None:
            db_id = repo.insert_folder(
                title, parent_db, cat_type=str(c.get("type") or "F"))
        folder_map[cid] = db_id
        return db_id

    for cid in sorted(cats):
        resolve_folder(cid)

    # ---- regulations ----------------------------------------------------- #
    for row in regs:
        title = str(row.get("title") or "")[:70]
        url = row.get("document_url")
        path = row.get("doc_path")
        try:
            # Without a repo (a plain --dry-run) there is nothing to ask whether
            # the document already exists, so `skipped_already_present` stays 0
            # and `db_consulted` reports false — it describes what the workbook
            # holds, not what the database would do with it. Pass --with-db to
            # make the number real: every write below is gated on dry_run, so a
            # dry run holding a repo only ever reads.
            existing = _find_existing(repo, row, url, path) if repo else None
            if existing:
                # Already in the library. Promoting the same workbook twice must
                # not duplicate it, and must not silently overwrite it either.
                skipped.append({"title": title, "reason": "already in database",
                                "regulation_id": existing.get("id")})
                if str(row.get("id", "")).strip().replace(".0", "").isdigit():
                    reg_map[int(float(row["id"]))] = existing.get("id")
                continue
            if dry_run:
                inserted.append({"title": title, "regulation_id": None})
                continue
            doc = _Doc(row)
            doc.compliancecategory_id = resolve_folder(row.get("compliancecategory_id"))
            rid = repo._insert_regulation(doc)
            if str(row.get("id", "")).strip().replace(".0", "").isdigit():
                reg_map[int(float(row["id"]))] = rid
            inserted.append({"title": title, "regulation_id": rid})
        except Exception as e:
            logger.error("promote failed for %s: %s", title, e)
            failed.append({"title": title, "error": str(e)[:200]})

    # ---- versions and analysis ------------------------------------------- #
    versions = analyses = mappings = 0
    if not dry_run:
        # The regulator per workbook regulation id, so a version row can name its
        # own source without re-reading the regulations sheet each time.
        reg_regulator = {_int(r.get("id")): (r.get("regulator") or "")
                         for r in regs if _int(r.get("id")) is not None}
        for v in data["regulation_versions"]:
            wb_id = _int(v.get("regulation_id"))
            rid = reg_map.get(wb_id)
            if rid is None:
                continue
            try:
                # A SNAPSHOT THAT IS ALREADY STORED IS NOT A NEW VERSION.
                #
                # Nothing here checked, so promoting the same workbook twice
                # wrote the same snapshot twice: measured 2026-08-14, MHRSD came
                # to 121 versions for 62 regulations, every one of them `active`,
                # which makes "the current version" ambiguous for 59 of them.
                # Promote is supposed to be idempotent, and its own report says
                # so — it reported `skipped_already_present: 62` on the very run
                # that duplicated the versions.
                #
                # content_hash is the snapshot's identity: same hash, same bytes,
                # nothing to record. A genuinely changed document arrives with a
                # different hash and still gets its new row.
                h = (v.get("content_hash") or "").strip()
                prior = repo.get_regulation_versions(rid)
                if h and any((x.get("content_hash") or "") == h for x in prior):
                    continue
                # ONE ACTIVE VERSION PER REGULATION.
                #
                # A new snapshot supersedes the one before it, so the previous
                # rows have to stop claiming to be current. Without this a
                # modified document ends up with two rows both marked `active`
                # and nothing says which is the live one — measured 2026-08-15
                # on the AML versioning test, where the edited document came out
                # with v1 and v8678 both active.
                #
                # `insert_regulation_version` defaults new rows to active, so
                # this runs FIRST and only when there is something to supersede.
                if prior:
                    repo.mark_all_versions_inactive(rid)
                # EVERY argument is passed. This call used to name only three,
                # and `regulator`, `updated_date` and `change_summary` have no
                # defaults — so all 11 AML versions failed with a TypeError that
                # promote caught and logged per row, leaving `regulations`
                # populated and `regulation_versions` empty.
                repo.insert_regulation_version(
                    rid,
                    regulator=reg_regulator.get(wb_id, ""),
                    content_html=v.get("content_html") or "",
                    content_text=v.get("content_text") or "",
                    content_hash=v.get("content_hash") or "",
                    updated_date=v.get("updated_date"),
                    change_summary=v.get("change_summary") or "",
                    # A version's status is the SNAPSHOT's lifecycle (active /
                    # inactive), which the repo manages — not the regulation's
                    # human approve/reject flag, which stays empty.
                    status=v.get("status") or "active")
                versions += 1
            except Exception as e:
                logger.error("version insert failed (reg %s): %s", rid, e)

        rows = _remap(data["compliance_analysis"], "regulation_id", reg_map)
        if rows:
            try:
                repo.store_analysis(rows)
                analyses = len(rows)
            except Exception as e:
                logger.error("analysis insert failed: %s", e)

        rows = _remap(data["requirement_mappings"], "regulation_id", reg_map)
        if rows:
            try:
                repo.store_requirement_mappings(rows)
                mappings = len(rows)
            except Exception as e:
                logger.error("mapping insert failed: %s", e)

    return {
        "workbook": str(xlsx),
        "dry_run": dry_run,
        "db_consulted": repo is not None,
        "folders": len(folder_map),
        "inserted": len(inserted),
        "skipped_already_present": len(skipped),
        "failed": len(failed),
        "regulation_versions": versions,
        "compliance_analysis": analyses,
        "requirement_mappings": mappings,
        "failures": failed[:10],
        "sample_inserted": inserted[:5],
    }


def _int(v) -> Optional[int]:
    try:
        return int(float(v))
    except Exception:
        return None


def _remap(rows: list, key: str, id_map: Dict[int, int]) -> list:
    """Point child rows at the DATABASE's regulation ids, not the workbook's."""
    out = []
    for r in rows:
        rid = id_map.get(_int(r.get(key)))
        if rid is None:
            continue
        r = dict(r)
        r[key] = rid
        for drop in ("analysis_id", "mapping_id", "version_id"):
            r.pop(drop, None)
        out.append(r)
    return out


def _build_repo():
    from dotenv import load_dotenv
    load_dotenv()
    from storage.mssql_repo import MSSQLRepository
    return MSSQLRepository({
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    })


def main() -> int:
    ap = argparse.ArgumentParser(description="Promote an approved workbook to MSSQL")
    ap.add_argument("workbook")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what WOULD be inserted; opens no connection "
                         "and writes nothing")
    ap.add_argument("--with-db", action="store_true",
                    help="during a --dry-run, still open a READ-ONLY connection "
                         "so `skipped_already_present` is a real number rather "
                         "than always 0. Writes are still gated on --dry-run.")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    path = Path(a.workbook)
    if not path.exists():
        raise SystemExit(f"no such workbook: {path}")

    repo = _build_repo() if (a.with_db or not a.dry_run) else None
    if a.with_db:
        # find_by_identity swallows its own exceptions and returns None, so a bad
        # login is indistinguishable from "nothing matched" — the same silent 0
        # this flag exists to remove. A SELECT that is allowed to raise, first.
        repo.get_folder_id("__connectivity_probe__", None)
    report = promote(path, repo, dry_run=a.dry_run)
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    return 1 if report.get("failed") else 0


if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(PROJECT_ROOT))
    sys.exit(main())
