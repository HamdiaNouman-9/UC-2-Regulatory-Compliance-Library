"""
cleanup_rulebook_db.py
======================
Deletes all DB entries for CBB Rulebook source systems.
Uses MSSQLRepository + temp table approach.
"""

import os
import logging
from dotenv import load_dotenv
from storage.mssql_repo import MSSQLRepository

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SOURCE_SYSTEMS = [
    "CBB-Rulebook",
    "CBB-AML-LAW",
    "CBB-CORPGOV"
]

DRY_RUN =False


repo = MSSQLRepository({
    "driver":   os.getenv("MSSQL_DRIVER"),
    "server":   os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "trusted_connection": "yes"
})


def run():
    log.info(f"{'DRY RUN — no changes' if DRY_RUN else '*** LIVE DELETE ***'}")
    log.info(f"Target source_systems: {SOURCE_SYSTEMS}\n")

    ph = ",".join("?" * len(SOURCE_SYSTEMS))

    # IMPORTANT: temp tables require SAME connection
    with repo._get_conn() as conn:
        cur = conn.cursor()

        # Step 1: count
        cur.execute(
            f"""
            SELECT source_system, COUNT(*)
            FROM regulations
            WHERE source_system IN ({ph})
              AND regulator = 'Central Bank of Bahrain'
            GROUP BY source_system
            """,
            SOURCE_SYSTEMS,
        )

        rows = cur.fetchall()
        total_regs = sum(r[1] for r in rows)

        log.info(f"Regulations found: {total_regs:,}")
        for ss, cnt in rows:
            log.info(f"  {ss!r}: {cnt:,}")

        if total_regs == 0:
            log.info("Nothing to delete.")
            return

        # Step 2: temp table
        log.info("\nLoading IDs...")
        cur.execute("CREATE TABLE #del_ids (id BIGINT PRIMARY KEY)")

        cur.execute(
            f"""
            INSERT INTO #del_ids (id)
            SELECT id
            FROM regulations
            WHERE source_system IN ({ph})
              AND regulator = 'Central Bank of Bahrain'
            """,
            SOURCE_SYSTEMS,
        )

        log.info(f"  {cur.rowcount:,} IDs staged")

        # Step 3: counts
        def count_dep(table, col="regulation_id"):
            cur.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {col} IN (SELECT id FROM #del_ids)"
            )
            return cur.fetchone()[0]

        counts = {
            "sama_requirement_mapping":     count_dep("sama_requirement_mapping"),
            "compliance_analysis":          count_dep("compliance_analysis"),
            "compliance_analysis_versions": count_dep("compliance_analysis_versions"),
            "regulation_versions":          count_dep("regulation_versions"),
            "regulations":                  total_regs,
        }

        log.info("\nRows to delete:")
        for table, cnt in counts.items():
            log.info(f"  {table}: {cnt:,}")

        if DRY_RUN:
            cur.execute("DROP TABLE #del_ids")
            log.info("\nDRY RUN complete.")
            return

        # Step 4: delete
        log.info("\nDeleting...")

        def delete_dep(table, col="regulation_id"):
            cur.execute(
                f"DELETE FROM {table} "
                f"WHERE {col} IN (SELECT id FROM #del_ids)"
            )
            log.info(f"  Deleted {cur.rowcount:,} from {table}")

        delete_dep("sama_requirement_mapping")
        delete_dep("compliance_analysis")
        delete_dep("compliance_analysis_versions")
        delete_dep("regulation_versions")

        cur.execute("""
            DELETE FROM regulations
            WHERE id IN (SELECT id FROM #del_ids)
        """)
        log.info(f"  Deleted {cur.rowcount:,} from regulations")

        cur.execute("DROP TABLE #del_ids")
        conn.commit()

        log.info("\nAll deletes committed.")


if __name__ == "__main__":
    run()
