"""
backfill_cross_listed_sama_docs.py
=====================================
SAMA's own site cross-lists some documents under more than one sector (the
same document_url shows up in, say, both Banking Sector's and Credit
Bureaus's navigation). The original push deduped by document_url alone, so
each such document only got inserted once -- under whichever sector was
processed first -- and every other sector that also lists it was missing it
under its own category.

Per explicit instruction, this should be an exact mirror of SAMA's site: the
same document_url is allowed multiple rows, one per (document_url, category)
pair it's actually listed under. This script re-walks all 8 sector JSON
files and inserts whatever (document_url, category) combinations are still
missing -- existing rows are untouched, nothing is deleted, this is purely
additive.

Usage:
    python backfill_cross_listed_sama_docs.py --dry-run
    python backfill_cross_listed_sama_docs.py
"""

import argparse
import json
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import pyodbc

from push_sama_to_prod import _dict_to_doc, OUTPUT_DIR, _build_repo
from push_other_sectors_fast import _insert_regulation, _get_or_create_category

RECONNECT_ATTEMPTS = 5
RECONNECT_BASE_DELAY = 6


def _reconnect_with_backoff(repo):
    """Retry obtaining a fresh connection itself, with backoff -- a dead link
    needs more than one naive reconnect attempt to recover, especially right
    after the VPN/network was down for a while."""
    last_err = None
    for attempt in range(1, RECONNECT_ATTEMPTS + 1):
        try:
            return repo._get_conn()
        except pyodbc.Error as e:
            last_err = e
            if attempt < RECONNECT_ATTEMPTS:
                delay = RECONNECT_BASE_DELAY * attempt
                print(f"  reconnect failed (attempt {attempt}/{RECONNECT_ATTEMPTS}): {e} -- retrying in {delay}s")
                time.sleep(delay)
    raise last_err


ALL_SECTOR_FILES = [
    "Laws_and_Implementing_Regulations",
    "All_Financial_Institutions",
    "Banking_Sector",
    "Finance_Sector",
    "Payment_Systems",
    "Money_Exchange_Sector",
    "Credit_Bureaus",
    "Regulatory_Sandbox",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    repo = _build_repo()

    print("Bulk-loading existing (document_url, category) pairs...")
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT document_url, category FROM regulations WHERE regulator='SAMA'")
        existing_pairs = {(row[0], row[1]) for row in cur.fetchall()}
    print(f"  {len(existing_pairs)} pairs already in DB")

    if args.dry_run:
        for fname in ALL_SECTOR_FILES:
            path = OUTPUT_DIR / f"{fname}.json"
            if not path.exists():
                continue
            raw = json.loads(path.read_text(encoding="utf-8"))
            missing = sum(1 for d in raw if (d.get("document_url"), d.get("category")) not in existing_pairs)
            print(f"{fname}: {len(raw)} in JSON, {missing} missing (url,category) pairs to backfill")
        return

    folder_cache: Dict[Tuple[str, Optional[int]], int] = {}
    total_inserted, total_errors = 0, 0

    for fname in ALL_SECTOR_FILES:
        path = OUTPUT_DIR / f"{fname}.json"
        if not path.exists():
            print(f"  Skipping {fname} (file not found)")
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        docs = [_dict_to_doc(d) for d in raw]

        to_insert = [d for d in docs if (d.document_url, d.category) not in existing_pairs]
        print(f"\n{fname}: {len(docs)} in JSON, {len(to_insert)} to backfill")
        if not to_insert:
            continue

        inserted, errors = 0, 0
        conn = _reconnect_with_backoff(repo)
        pending_commits = 0
        for i, doc in enumerate(to_insert, 1):
            for attempt in range(1, RECONNECT_ATTEMPTS + 1):
                try:
                    if doc.doc_path:
                        doc.compliancecategory_id = _get_or_create_category(conn, doc.doc_path, folder_cache)
                    _insert_regulation(conn, doc)
                    pending_commits += 1
                    if pending_commits >= 20:
                        conn.commit()
                        pending_commits = 0
                    existing_pairs.add((doc.document_url, doc.category))
                    inserted += 1
                    break
                except pyodbc.Error as e:
                    if attempt == RECONNECT_ATTEMPTS:
                        errors += 1
                        print(f"  [{i}] FAILED after {RECONNECT_ATTEMPTS} attempts: {doc.title[:60]} -- {e}")
                        break
                    print(f"  [{i}] connection error (attempt {attempt}/{RECONNECT_ATTEMPTS}), reconnecting: {e}")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    pending_commits = 0
                    conn = _reconnect_with_backoff(repo)
                except Exception as e:
                    errors += 1
                    print(f"  [{i}] FAILED: {doc.title[:60]} -- {e}")
                    break
            if i % 200 == 0 or i == len(to_insert):
                try:
                    conn.commit()
                    pending_commits = 0
                except pyodbc.Error:
                    conn = _reconnect_with_backoff(repo)
                    pending_commits = 0
                print(f"  [{i}/{len(to_insert)}] inserted {inserted}, errors {errors}")
        try:
            conn.commit()
        except pyodbc.Error:
            pass
        try:
            conn.close()
        except Exception:
            pass

        print(f"  Done: {inserted} inserted, {errors} errors")
        total_inserted += inserted
        total_errors += errors

    print("\n" + "=" * 70)
    print(f"BACKFILL COMPLETE: {total_inserted} inserted, {total_errors} errors")
    print("=" * 70)


if __name__ == "__main__":
    main()
