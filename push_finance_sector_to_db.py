"""
push_finance_sector_to_db.py
=============================
Reads sama_finance_sector_requests.json and inserts into the MSSQL regulations
table using the same folder-hierarchy + insert flow the orchestrator uses for
other SAMA crawlers.

Workflow:
  1. Load JSON
  2. Check which docs already exist in DB (by document_url)
  3. Print a dry-run summary
  4. Ask for confirmation  ->  type 'yes' to proceed
  5. Insert new docs (folder hierarchy first, then regulation row)
  6. Print final counts

Usage:
    python push_finance_sector_to_db.py
    python push_finance_sector_to_db.py --input sama_finance_sector_requests.json
    python push_finance_sector_to_db.py --dry-run   # preview only, no DB writes
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv(override=True)

from models.models import RegulatoryDocument
from storage.mssql_repo import MSSQLRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def _build_repo() -> MSSQLRepository:
    return MSSQLRepository({
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver":   os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    })


def _url_exists(repo: MSSQLRepository, url: str) -> bool:
    """Return True if a regulation with this document_url already exists."""
    query = "SELECT TOP 1 id FROM regulations WHERE document_url = ?"
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [url])
            return cursor.fetchone() is not None
    except Exception as e:
        log.error(f"Existence check failed for {url}: {e}")
        return False


def _get_or_create_category(repo: MSSQLRepository, doc_path: list) -> Optional[int]:
    """Walk doc_path and get-or-create each folder level. Returns leaf compliancecategory_id."""
    parent_id = None
    for title in doc_path:
        folder_id = repo.get_folder_id(title, parent_id)
        parent_id = folder_id if folder_id else repo.insert_folder(title, parent_id)
    return parent_id


def _dict_to_doc(d: dict) -> RegulatoryDocument:
    extra = d.get("extra_meta") or {}
    doc = RegulatoryDocument(
        regulator=d.get("regulator") or "SAMA",
        source_system=d.get("source_system") or "SAMA RULEBOOK",
        category=d.get("category") or "Finance Sector",
        title=d.get("title") or "",
        document_url=d.get("document_url") or "",
        published_date=d.get("published_date"),
        reference_no=d.get("reference_no"),
        year=d.get("year"),
        source_page_url=d.get("source_page_url"),
        file_type=d.get("file_type"),
        extra_meta=extra,
        document_html=d.get("document_html"),
        doc_path=d.get("doc_path"),
    )
    return doc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="sama_finance_sector_requests.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview only — no DB writes")
    args = parser.parse_args()

    # ── Load JSON ──────────────────────────────────────────────────────────
    path = Path(args.input)
    if not path.exists():
        log.error(f"Input file not found: {path}")
        sys.exit(1)

    raw = json.loads(path.read_text(encoding="utf-8"))
    docs = [_dict_to_doc(d) for d in raw]
    log.info(f"Loaded {len(docs)} documents from {path}")

    # ── Connect & check which already exist ───────────────────────────────
    repo = _build_repo()

    new_docs: List[RegulatoryDocument] = []
    already_in_db: List[RegulatoryDocument] = []

    print(f"\nChecking {len(docs)} documents against DB...")
    for i, doc in enumerate(docs, 1):
        if i % 50 == 0:
            print(f"  checked {i}/{len(docs)}...")
        if _url_exists(repo, doc.document_url):
            already_in_db.append(doc)
        else:
            new_docs.append(doc)

    # ── Dry-run summary ───────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"DRY-RUN SUMMARY")
    print("=" * 70)
    print(f"  Total in JSON  : {len(docs)}")
    print(f"  Already in DB  : {len(already_in_db)}  (will be skipped)")
    print(f"  New to insert  : {len(new_docs)}")

    if new_docs:
        from collections import Counter
        cats = Counter(
            (d.doc_path[3] if d.doc_path and len(d.doc_path) > 3 else "(top)")
            for d in new_docs
        )
        print("\n  New docs by category:")
        for cat, n in sorted(cats.items(), key=lambda x: -x[1]):
            print(f"    {n:4d}  {cat}")

    print("=" * 70)

    if args.dry_run or not new_docs:
        if not new_docs:
            print("\nNothing to insert — all documents already in DB.")
        else:
            print("\n--dry-run flag set. No changes made.")
        return

    # ── Confirmation ──────────────────────────────────────────────────────
    print(f"\nThis will INSERT {len(new_docs)} new regulation rows into the DB.")
    answer = input("Type 'yes' to proceed, anything else to cancel: ").strip().lower()
    if answer != "yes":
        print("Cancelled.")
        return

    # ── Insert ────────────────────────────────────────────────────────────
    inserted = 0
    errors = 0

    for i, doc in enumerate(new_docs, 1):
        try:
            # Create folder hierarchy and get compliancecategory_id
            if doc.doc_path:
                doc.compliancecategory_id = _get_or_create_category(repo, doc.doc_path)

            # Insert regulation row
            reg_id = repo._insert_regulation(doc)
            doc.id = reg_id
            inserted += 1
            if i % 25 == 0 or i == len(new_docs):
                log.info(f"  [{i}/{len(new_docs)}] inserted {inserted} so far")
        except Exception as e:
            errors += 1
            log.error(f"  [{i}] FAILED: {doc.title[:60]} — {e}")

    print("\n" + "=" * 70)
    print(f"DONE")
    print(f"  Inserted : {inserted}")
    print(f"  Skipped  : {len(already_in_db)}")
    print(f"  Errors   : {errors}")
    print("=" * 70)


if __name__ == "__main__":
    main()
