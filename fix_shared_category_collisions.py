"""
fix_shared_category_collisions.py
====================================
Repairs existing compliancecategory slots that got shared by more than one
regulation (the leaf-collision bug fixed in storage/mssql_repo.py,
orchestrator/orchestrator.py, push_sama_to_prod.py, and
push_other_sectors_fast.py). For each shared slot: keeps one regulation on
the existing node, and gives every other colliding regulation its own new
node (same title, same parent, type='R'), then re-points its
compliancecategory_id.

Scoped to Banking Sector / Finance Sector / Payment Systems only -- the
categories this session's work actually created. Pre-existing collisions in
SAMA Circulars or test data are left untouched (out of scope, not caused by
this work).

Usage:
    python fix_shared_category_collisions.py --dry-run
    python fix_shared_category_collisions.py
"""

import argparse
import os

from dotenv import load_dotenv

load_dotenv(override=True)

from push_sama_to_prod import _build_repo

SCOPE_CATEGORIES = ("Banking Sector", "Finance Sector", "Payment Systems")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    repo = _build_repo()

    with repo._get_conn() as conn:
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in SCOPE_CATEGORIES)
        cur.execute(f"""
            SELECT compliancecategory_id FROM regulations
            WHERE regulator='SAMA' AND category IN ({placeholders})
            AND compliancecategory_id IS NOT NULL
            GROUP BY compliancecategory_id HAVING COUNT(*) > 1
        """, list(SCOPE_CATEGORIES))
        shared_ids = [row[0] for row in cur.fetchall()]

    print(f"Found {len(shared_ids)} shared category slots to repair.\n")

    fixed = 0
    conn = repo._get_conn()
    for cid in shared_ids:
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM regulations WHERE compliancecategory_id=? ORDER BY id", [cid])
        regs = cur.fetchall()

        cur.execute("SELECT title, parentid, type FROM compliancecategory WHERE compliancecategory_id=?", [cid])
        title, parentid, ctype = cur.fetchone()

        print(f"Slot {cid} ('{title}'): {len(regs)} regulations -> keeping reg {regs[0][0]} on this slot")

        for reg_id, reg_title in regs[1:]:
            if args.dry_run:
                print(f"  WOULD create new slot for reg {reg_id} ('{reg_title}')")
                continue
            cur.execute(
                "INSERT INTO compliancecategory (title, parentid, type) OUTPUT INSERTED.compliancecategory_id VALUES (?, ?, ?)",
                [title, parentid, ctype],
            )
            new_cid = cur.fetchone()[0]
            cur.execute("UPDATE regulations SET compliancecategory_id=? WHERE id=?", [new_cid, reg_id])
            print(f"  reg {reg_id} ('{reg_title}') -> new slot {new_cid}")
            fixed += 1

        if not args.dry_run:
            conn.commit()

    conn.close()
    print(f"\nDone: {fixed} regulations moved to their own dedicated slot.")


if __name__ == "__main__":
    main()
