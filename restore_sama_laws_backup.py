"""
restore_sama_laws_backup.py
=============================
Reverts the "Laws and Implementing Regulations" migration performed by
push_sama_to_prod.py, using a backup file written by that script
(output/sama_migration_backup/laws_old_rows_backup_<timestamp>.json).

What this does:
  1. Re-inserts the old regulation rows with their ORIGINAL ids (via
     IDENTITY_INSERT, since regulations.id is an identity column).
  2. Reverses the analysis remap: UPDATEs the 8 linked tables' regulation_id
     back from the new id to the original old id.

What this does NOT do:
  - It does not delete the new "Laws and Implementing Regulations" rows that
    push_sama_to_prod.py inserted. Removing those is a separate, deliberate
    decision -- run that cleanup yourself if you actually want a full revert
    rather than just getting the old rows + their analysis linkage back.

Usage:
    python restore_sama_laws_backup.py --backup output/sama_migration_backup/laws_old_rows_backup_20260625_213000.json --dry-run
    python restore_sama_laws_backup.py --backup output/sama_migration_backup/laws_old_rows_backup_20260625_213000.json
"""

import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(override=True)

from storage.mssql_repo import MSSQLRepository

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data = json.loads(Path(args.backup).read_text(encoding="utf-8"))
    rows = data["old_regulation_rows"]
    old_to_new = data.get("old_to_new_id_map")

    print(f"Backup contains {len(rows)} old regulation rows.")
    if not old_to_new:
        print("No old_to_new_id_map present in this backup -- analysis remap cannot be reversed.")
        return

    if args.dry_run:
        print(f"--dry-run: would re-insert {len(rows)} rows with original ids, "
              f"then reverse-remap {len(old_to_new)} id pairs across {len(LINKED_TABLES)} tables.")
        return

    print(f"\nAbout to re-insert {len(rows)} rows (with original ids) and reverse the analysis remap.")
    answer = input("Type 'yes' to proceed: ").strip().lower()
    if answer != "yes":
        print("Aborted.")
        return

    repo = _build_repo()

    columns = list(rows[0].keys())
    placeholders = ", ".join("?" for _ in columns)
    col_list = ", ".join(columns)

    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SET IDENTITY_INSERT regulations ON")
        try:
            for row in rows:
                values = [row[c] for c in columns]
                cur.execute(
                    f"INSERT INTO regulations ({col_list}) VALUES ({placeholders})",
                    values,
                )
        finally:
            cur.execute("SET IDENTITY_INSERT regulations OFF")
        conn.commit()
    print(f"Re-inserted {len(rows)} rows with original ids.")

    print(f"\nReversing remap for {len(old_to_new)} id pairs across {len(LINKED_TABLES)} tables...")
    with repo._get_conn() as conn:
        cur = conn.cursor()
        for table, col in LINKED_TABLES:
            total = 0
            for old_id, new_id in old_to_new.items():
                cur.execute(f"UPDATE {table} SET {col} = ? WHERE {col} = ?", [int(old_id), int(new_id)])
                total += cur.rowcount
            conn.commit()
            print(f"  {table:35s} {total:4d} rows reverted")

    print("\nRestore complete. Note: the new 853-doc rows inserted by the migration were NOT removed.")


if __name__ == "__main__":
    main()
