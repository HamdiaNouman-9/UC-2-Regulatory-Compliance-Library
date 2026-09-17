"""
push_sama_to_prod.py
======================
Pushes all 8 crawled SAMA Rulebook sectors (output/sama_rulebook/*.json) to
the production DB.

Special handling for "Laws and Implementing Regulations": the DB already has
19 rows for this category from the old, narrower crawler, and those rows have
real linked analysis (compliance_analysis, compliance_analysis_versions,
sama_requirement_mapping, processinglogs, regulation_versions, gap_analysis,
DEMO_REQUIREMENT_CONTROL_LINK, DEMO_REQUIREMENT_KPI_LINK). Every one of those
19 document_url values has an exact match among the new 853-doc crawl, so:
  1. Insert all 853 new Laws docs (unconditional -- the old 19 rows still
     occupy those same URLs at this point, so a url-exists check would
     wrongly skip exactly the rows we need fresh IDs for).
  2. Match each old row to its new counterpart by document_url.
  3. Re-point every linked table's regulation_id from old id -> new id.
  4. Verify no rows still reference the old ids.
  5. Back up the full old rows + the old->new id mapping to a JSON file
     (output/sama_migration_backup/) so they can be restored if needed.
  6. Delete the 19 old regulation rows.

All other sectors (All Financial Institutions, Banking Sector, Finance
Sector, Payment Systems, Money Exchange Sector, Credit Bureaus, Regulatory
Sandbox) are plain new inserts with a document_url existence check, since
nothing existed for them in the DB before this.

SAMA Circulars is never read or written by this script.

Usage:
    python push_sama_to_prod.py --dry-run     # preview only
    python push_sama_to_prod.py               # asks for 'yes' confirmation
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pyodbc
from dotenv import load_dotenv

load_dotenv(override=True)

from models.models import RegulatoryDocument
from storage.mssql_repo import MSSQLRepository

BACKUP_DIR = Path("output") / "sama_migration_backup"

RETRY_ATTEMPTS = 5
RETRY_BASE_DELAY = 6  # seconds; grows linearly per attempt


def _retry(fn, *args, **kwargs):
    """Run fn, retrying on transient pyodbc connection errors with backoff.
    The DB link has proven intermittently flaky (VPN-dependent) -- most drops
    self-resolve within well under a minute, so this rides those out instead
    of counting every blip as a permanent per-document failure."""
    last_err = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except pyodbc.Error as e:
            last_err = e
            if attempt < RETRY_ATTEMPTS:
                delay = RETRY_BASE_DELAY * attempt
                log.warning(f"  DB op failed (attempt {attempt}/{RETRY_ATTEMPTS}): {e} -- retrying in {delay}s")
                time.sleep(delay)
    raise last_err

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = Path("output") / "sama_rulebook"

LAWS_CATEGORY = "Laws and Implementing Regulations"

OTHER_SECTORS = [
    "All_Financial_Institutions",
    "Banking_Sector",
    "Finance_Sector",
    "Payment_Systems",
    "Money_Exchange_Sector",
    "Credit_Bureaus",
    "Regulatory_Sandbox",
]

LINKED_TABLES = [
    ("compliance_analysis", "regulation_id"),
    ("compliance_analysis_versions", "regulation_id"),
    ("sama_requirement_mapping", "regulation_id"),
    ("processinglogs", "regulation_id"),
    ("regulation_versions", "regulation_id"),
    ("gap_analysis", "regulation_id"),
    ("DEMO_REQUIREMENT_CONTROL_LINK", "REGULATION_ID"),
    ("DEMO_REQUIREMENT_KPI_LINK", "REGULATION_ID"),
]


def _build_repo() -> MSSQLRepository:
    return MSSQLRepository({
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver":   os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    })


def _url_exists(repo: MSSQLRepository, url: str) -> bool:
    query = "SELECT TOP 1 id FROM regulations WHERE document_url = ?"
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, [url])
        return cursor.fetchone() is not None


def _get_or_create_category(repo: MSSQLRepository, doc_path: list) -> Optional[int]:
    parent_id = None
    last_index = len(doc_path) - 1
    for i, title in enumerate(doc_path):
        folder_id = repo.get_folder_id(title, parent_id)
        if folder_id is not None and i == last_index:
            # Leaf segment: don't merge into a slot a different regulation
            # already owns -- create a separate node for this document.
            if repo.regulation_exists_for_category(folder_id):
                folder_id = None
        parent_id = folder_id if folder_id else repo.insert_folder(title, parent_id)
    return parent_id


def _dict_to_doc(d: dict) -> RegulatoryDocument:
    return RegulatoryDocument(
        regulator=d.get("regulator") or "SAMA",
        source_system=d.get("source_system") or "SAMA RULEBOOK",
        category=d.get("category") or "",
        title=d.get("title") or "",
        document_url=d.get("document_url") or "",
        published_date=d.get("published_date"),
        reference_no=d.get("reference_no"),
        year=d.get("year"),
        source_page_url=d.get("source_page_url"),
        file_type=d.get("file_type"),
        extra_meta=d.get("extra_meta") or {},
        document_html=d.get("document_html"),
        doc_path=d.get("doc_path"),
    )


def _insert_docs(repo: MSSQLRepository, docs: List[RegulatoryDocument], check_exists: bool) -> Dict[str, int]:
    """Insert docs, returns {document_url: new_id} for everything inserted (or
    already-existing, if check_exists -- existing rows are looked up too)."""
    url_to_id: Dict[str, int] = {}
    inserted, skipped, errors = 0, 0, 0
    def _process_one(doc):
        if check_exists and _url_exists(repo, doc.document_url):
            return None
        if doc.doc_path:
            doc.compliancecategory_id = _get_or_create_category(repo, doc.doc_path)
        return repo._insert_regulation(doc)

    for i, doc in enumerate(docs, 1):
        try:
            new_id = _retry(_process_one, doc)
            if new_id is None:
                skipped += 1
                continue
            url_to_id[doc.document_url] = new_id
            inserted += 1
            if i % 100 == 0 or i == len(docs):
                log.info(f"  [{i}/{len(docs)}] inserted {inserted}, skipped {skipped}")
        except Exception as e:
            errors += 1
            log.error(f"  [{i}] FAILED after {RETRY_ATTEMPTS} attempts: {doc.title[:60]} -- {e}")
    log.info(f"Done: {inserted} inserted, {skipped} skipped, {errors} errors")
    return url_to_id


def _backup_old_rows(repo: MSSQLRepository, old_ids: List[int]) -> Path:
    """Dump every column of the old regulation rows to a timestamped JSON
    file before any mutation, so they (and the id mapping, appended later)
    can be restored if the migration needs to be reverted."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    id_list = ",".join(str(i) for i in old_ids)
    # pyodbc can't fetch datetimeoffset (created_at/updated_at) columns via a
    # plain SELECT * -- convert them to ISO8601 strings (style 127) at the SQL
    # level instead.
    query = """
        SELECT id, regulator, source_system, category, title, document_url,
               published_date, reference_no, department, year, source_page_url,
               extra_meta,
               CONVERT(VARCHAR(40), created_at, 127) AS created_at,
               CONVERT(VARCHAR(40), updated_at, 127) AS updated_at,
               document_html, doc_path, compliancecategory_id, status, type,
               content_hash, title_hash
        FROM regulations
        WHERE id IN ({})
    """.format(id_list)
    def _run():
        with repo._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(query)
            columns = [c[0] for c in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    rows = _retry(_run)

    for row in rows:
        for k, v in row.items():
            if not isinstance(v, (str, int, float, bool, type(None))):
                row[k] = str(v)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"laws_old_rows_backup_{timestamp}.json"
    backup_path.write_text(
        json.dumps({"old_regulation_rows": rows, "old_to_new_id_map": None}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return backup_path


def _append_id_map_to_backup(backup_path: Path, old_to_new: Dict[int, int]):
    data = json.loads(backup_path.read_text(encoding="utf-8"))
    data["old_to_new_id_map"] = old_to_new
    backup_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def migrate_laws(repo: MSSQLRepository, dry_run: bool):
    print("\n" + "=" * 70)
    print("LAWS AND IMPLEMENTING REGULATIONS -- MIGRATION")
    print("=" * 70)

    def _fetch_old_rows():
        with repo._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, document_url FROM regulations WHERE regulator='SAMA' AND category=?",
                [LAWS_CATEGORY],
            )
            return cur.fetchall()

    old_rows = _retry(_fetch_old_rows)

    if not old_rows:
        print("No old 'Laws and Implementing Regulations' rows found -- already migrated, or nothing to do.")
        return

    old_id_by_url = {url: id_ for id_, url in old_rows}
    old_ids = [id_ for id_, _ in old_rows]
    print(f"Found {len(old_rows)} old rows to migrate.")

    path = OUTPUT_DIR / "Laws_and_Implementing_Regulations.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    docs = [_dict_to_doc(d) for d in raw]
    print(f"Loaded {len(docs)} new documents from {path}")

    matched = sum(1 for d in docs if d.document_url in old_id_by_url)
    missing = [url for url in old_id_by_url if url not in {d.document_url for d in docs}]
    print(f"  {matched} new docs match an old document_url")
    if missing:
        print(f"  WARNING: {len(missing)} old URLs have NO match in the new crawl: {missing}")
        print("  Aborting migration -- resolve this before proceeding.")
        return

    if dry_run:
        print("\n--dry-run: would insert 853 new docs, remap 8 linked tables for "
              f"{len(old_ids)} old->new id pairs, then delete the {len(old_ids)} old rows.")
        return

    backup_path = _backup_old_rows(repo, old_ids)
    print(f"\nBacked up {len(old_ids)} full old rows -> {backup_path}")

    print(f"\nInserting {len(docs)} new documents (unconditional -- old rows still occupy these URLs)...")
    url_to_new_id = _insert_docs(repo, docs, check_exists=False)

    old_to_new: Dict[int, int] = {}
    for url, old_id in old_id_by_url.items():
        new_id = url_to_new_id.get(url)
        if new_id is None:
            print(f"  WARNING: no new id found for old id {old_id} (url={url}) -- skipping remap for this one")
            continue
        old_to_new[old_id] = new_id

    _append_id_map_to_backup(backup_path, old_to_new)
    print(f"Appended old->new id map to backup -> {backup_path}")

    print(f"\nRemapping {len(old_to_new)} old->new id pairs across {len(LINKED_TABLES)} linked tables...")
    for table, col in LINKED_TABLES:
        def _remap_table(table=table, col=col):
            with repo._get_conn() as conn:
                cur = conn.cursor()
                total = 0
                for old_id, new_id in old_to_new.items():
                    cur.execute(f"UPDATE {table} SET {col} = ? WHERE {col} = ?", [new_id, old_id])
                    total += cur.rowcount
                conn.commit()
                return total
        total = _retry(_remap_table)
        print(f"  {table:35s} {total:4d} rows remapped")

    print("\nVerifying no rows still reference old ids...")
    id_list = ",".join(str(i) for i in old_ids)

    def _verify():
        with repo._get_conn() as conn:
            cur = conn.cursor()
            remaining = 0
            for table, col in LINKED_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IN ({id_list})")
                count = cur.fetchone()[0]
                remaining += count
                if count:
                    print(f"  WARNING: {table} still has {count} rows referencing old ids")
            return remaining

    remaining = _retry(_verify)
    if remaining:
        print(f"\n{remaining} rows still reference old ids -- ABORTING delete for safety.")
        return
    print("  Clean -- no remaining references.")

    print(f"\nAbout to permanently DELETE {len(old_ids)} old regulation rows (their analysis has "
          f"already been remapped to the new rows above, and verified clean).")
    answer = input("Type 'yes' to proceed with the delete, anything else to abort: ").strip().lower()
    if answer != "yes":
        print("Aborted -- old rows NOT deleted. New rows and remapped analysis remain as inserted above.")
        return

    print(f"\nDeleting {len(old_ids)} old regulation rows...")

    def _delete():
        with repo._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"DELETE FROM regulations WHERE id IN ({id_list})")
            deleted = cur.rowcount
            conn.commit()
            return deleted

    deleted = _retry(_delete)
    print(f"Deleted {deleted} old rows.")

    def _final_counts():
        with repo._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM regulations WHERE regulator='SAMA' AND category=?",
                [LAWS_CATEGORY],
            )
            final_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM regulations WHERE regulator='SAMA' AND category='SAMA Circulars'")
            circulars_count = cur.fetchone()[0]
            return final_count, circulars_count

    final_count, circulars_count = _retry(_final_counts)
    print(f"\nFinal 'Laws and Implementing Regulations' count: {final_count}")
    print(f"SAMA Circulars count (should be unchanged): {circulars_count}")


def push_other_sectors(repo: MSSQLRepository, dry_run: bool):
    print("\n" + "=" * 70)
    print("OTHER SECTORS -- NEW INSERTS")
    print("=" * 70)

    for sector_file in OTHER_SECTORS:
        path = OUTPUT_DIR / f"{sector_file}.json"
        if not path.exists():
            print(f"  Skipping {sector_file} (file not found)")
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        docs = [_dict_to_doc(d) for d in raw]
        print(f"\n{sector_file}: {len(docs)} documents in JSON")

        if dry_run:
            print(f"  --dry-run: would check {len(docs)} URLs and insert any not already present")
            continue

        _insert_docs(repo, docs, check_exists=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    repo = _build_repo()

    migrate_laws(repo, dry_run=args.dry_run)
    push_other_sectors(repo, dry_run=args.dry_run)

    if args.dry_run:
        print("\n--dry-run complete. No changes made.")
        return

    print("\nAll done.")


if __name__ == "__main__":
    main()
