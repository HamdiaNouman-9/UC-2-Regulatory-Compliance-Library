"""
push_other_sectors_fast.py
============================
Fast path for pushing the 7 "other" SAMA sectors (everything except Laws and
Implementing Regulations, which push_sama_to_prod.py handles separately) into
the production DB.

push_sama_to_prod.py's _insert_docs() is correct but slow: MSSQLRepository's
get_folder_id/insert_folder/_insert_regulation each open their own brand-new
DB connection, and a single document with a 6-level doc_path needs up to 6
of those round trips. Over a real (occasionally flaky) VPN link that's
7-13 seconds per document.

This script does the same inserts, but:
  - reuses ONE connection per sector instead of reconnecting per call
  - caches resolved folder ids in memory (most documents under a sector
    share most of their doc_path with siblings, so this avoids re-querying
    compliancecategory for folders already resolved this run)
  - bulk-loads all existing document_urls once instead of one SELECT per doc
  - reconnects and retries a document once if the connection drops mid-run

Usage:
    python push_other_sectors_fast.py --dry-run
    python push_other_sectors_fast.py
"""

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

import pyodbc
from dotenv import load_dotenv

load_dotenv(override=True)

from models.models import RegulatoryDocument
from storage.mssql_repo import MSSQLRepository
from push_sama_to_prod import _dict_to_doc, OUTPUT_DIR, OTHER_SECTORS, _build_repo

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def _insert_regulation(conn, document: RegulatoryDocument) -> int:
    """Same SQL as MSSQLRepository._insert_regulation, but using an existing
    connection instead of opening a new one."""
    doc_path_json = json.dumps(document.doc_path) if document.doc_path else None
    extra_meta = document.extra_meta or {}
    extra_meta_json = json.dumps(extra_meta) if extra_meta else None
    department_value = (
        json.dumps(document.department) if isinstance(document.department, list)
        else (str(document.department) if document.department else None)
    )
    year_value = str(document.year) if document.year is not None else None

    sql = """
        INSERT INTO regulations (
            regulator, source_system, category,
            title, document_url, doc_path,
            published_date, reference_no,
            department, year,
            source_page_url, extra_meta,
            compliancecategory_id, document_html,
            type, status
        )
        OUTPUT INSERTED.id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (
        document.regulator, document.source_system, document.category,
        document.title, document.document_url, doc_path_json,
        document.published_date, document.reference_no,
        department_value, year_value,
        document.source_page_url, extra_meta_json,
        getattr(document, "compliancecategory_id", None), document.document_html,
        getattr(document, "type", "R") or "R", getattr(document, "status", "active") or "active",
    ))
    return cur.fetchone()[0]


def _get_or_create_category(conn, doc_path: list, cache: Dict[Tuple[str, Optional[int]], int]) -> Optional[int]:
    parent_id = None
    last_index = len(doc_path) - 1
    for i, title in enumerate(doc_path):
        is_leaf = (i == last_index)
        key = (title, parent_id)
        # The leaf segment is never cached: caching it would let a second
        # document with the same (title, parent) silently reuse the first
        # document's slot, bypassing the "already claimed?" check below.
        if not is_leaf:
            cached = cache.get(key)
            if cached is not None:
                parent_id = cached
                continue
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 compliancecategory_id FROM compliancecategory
            WHERE title = ? AND ((parentid IS NULL AND ? IS NULL) OR parentid = ?)
            """,
            [title, parent_id, parent_id],
        )
        row = cur.fetchone()
        folder_id = int(row[0]) if row else None
        if folder_id is not None and is_leaf:
            cur.execute("SELECT TOP 1 1 FROM regulations WHERE compliancecategory_id = ?", [folder_id])
            if cur.fetchone() is not None:
                folder_id = None  # already claimed by a different document
        if folder_id is None:
            cur.execute(
                "INSERT INTO compliancecategory (title, parentid, type) OUTPUT INSERTED.compliancecategory_id VALUES (?, ?, ?)",
                [title, parent_id, "F"],
            )
            folder_id = int(cur.fetchone()[0])
            conn.commit()
        if not is_leaf:
            cache[key] = folder_id
        parent_id = folder_id
    return parent_id


def push_sector_fast(repo: MSSQLRepository, sector_file: str, existing_urls: set,
                      folder_cache: dict, commit_every: int = 20) -> Tuple[int, int, int]:
    path = OUTPUT_DIR / f"{sector_file}.json"
    if not path.exists():
        print(f"  Skipping {sector_file} (file not found)")
        return 0, 0, 0

    raw = json.loads(path.read_text(encoding="utf-8"))
    docs = [_dict_to_doc(d) for d in raw]
    print(f"\n{sector_file}: {len(docs)} documents in JSON")

    inserted = skipped = errors = 0
    conn = repo._get_conn()
    pending_commits = 0
    try:
        for i, doc in enumerate(docs, 1):
            if doc.document_url in existing_urls:
                skipped += 1
                continue
            for attempt in (1, 2):
                try:
                    if doc.doc_path:
                        doc.compliancecategory_id = _get_or_create_category(conn, doc.doc_path, folder_cache)
                    new_id = _insert_regulation(conn, doc)
                    pending_commits += 1
                    if pending_commits >= commit_every:
                        conn.commit()
                        pending_commits = 0
                    existing_urls.add(doc.document_url)
                    inserted += 1
                    break
                except pyodbc.Error as e:
                    if attempt == 2:
                        errors += 1
                        log.error(f"  [{i}] FAILED after reconnect: {doc.title[:60]} -- {e}")
                        break
                    log.warning(f"  [{i}] connection error, reconnecting and retrying once: {e}")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = repo._get_conn()
                except Exception as e:
                    errors += 1
                    log.error(f"  [{i}] FAILED: {doc.title[:60]} -- {e}")
                    break
            if i % 200 == 0 or i == len(docs):
                conn.commit()
                pending_commits = 0
                log.info(f"  [{i}/{len(docs)}] inserted {inserted}, skipped {skipped}, errors {errors}")
        conn.commit()
    finally:
        conn.close()

    print(f"  Done: {inserted} inserted, {skipped} skipped, {errors} errors")
    return inserted, skipped, errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    repo = _build_repo()

    print("Bulk-loading existing document_urls...")
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT document_url FROM regulations")
        existing_urls = {row[0] for row in cur.fetchall()}
    print(f"  {len(existing_urls)} URLs already in DB")

    if args.dry_run:
        for sector_file in OTHER_SECTORS:
            path = OUTPUT_DIR / f"{sector_file}.json"
            if not path.exists():
                continue
            raw = json.loads(path.read_text(encoding="utf-8"))
            new_count = sum(1 for d in raw if d.get("document_url") not in existing_urls)
            print(f"{sector_file}: {len(raw)} in JSON, {new_count} new")
        return

    folder_cache: Dict[Tuple[str, Optional[int]], int] = {}
    totals = [0, 0, 0]
    for sector_file in OTHER_SECTORS:
        i, s, e = push_sector_fast(repo, sector_file, existing_urls, folder_cache)
        totals[0] += i
        totals[1] += s
        totals[2] += e

    print("\n" + "=" * 70)
    print(f"ALL OTHER SECTORS DONE: {totals[0]} inserted, {totals[1]} skipped, {totals[2]} errors")
    print("=" * 70)


if __name__ == "__main__":
    main()
