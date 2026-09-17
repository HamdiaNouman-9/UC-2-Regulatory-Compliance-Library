"""
Exports compliance_analysis v2 rows for a regulation as INSERT statements
ready to run on prod DB.
"""

import json, os
from dotenv import load_dotenv
load_dotenv(override=True)
from storage.mssql_repo import MSSQLRepository

REGULATION_ID  = 27880
OUTPUT_FILE    = f"prod_insert_{REGULATION_ID}.sql"

repo = MSSQLRepository({
    "server":   os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "username": os.getenv("MSSQL_USERNAME"),
    "password": os.getenv("MSSQL_PASSWORD"),
    "driver":   os.getenv("MSSQL_DRIVER"),
})

def esc(val):
    """Escape a string value for T-SQL — doubles single quotes, wraps in quotes, or returns NULL."""
    if val is None:
        return "NULL"
    return "N'" + str(val).replace("'", "''") + "'"

with repo._get_conn() as conn:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            regulation_id, version_id,
            requirement_id, requirement_title,
            execution_category, criticality, obligation_type,
            stage1_json, stage2_json, stage3_json, stage4_md,
            analysis_json, schema_version, status, is_current
        FROM compliance_analysis
        WHERE regulation_id = ?
          AND schema_version = 'v2'
          AND is_current = 1
        ORDER BY requirement_id
        """,
        [REGULATION_ID],
    )
    rows = cursor.fetchall()

lines = [
    f"-- Prod INSERT for regulation {REGULATION_ID} ({len(rows)} rows)",
    f"-- Generated from local DB — run on prod after verifying",
    "",
    f"-- Safety check: confirm no v2 rows exist yet on prod",
    f"-- SELECT COUNT(*) FROM compliance_analysis WHERE regulation_id={REGULATION_ID} AND schema_version='v2';",
    "",
]

for row in rows:
    (reg_id, version_id, req_id, req_title,
     exec_cat, criticality, ob_type,
     s1, s2, s3, s4, aj, schema_ver, status, is_current) = row

    lines.append(
        f"INSERT INTO compliance_analysis "
        f"(regulation_id, version_id, requirement_id, requirement_title, "
        f"execution_category, criticality, obligation_type, "
        f"stage1_json, stage2_json, stage3_json, stage4_md, "
        f"analysis_json, schema_version, status, is_current) VALUES ("
        f"{reg_id}, "
        f"{version_id if version_id is not None else 'NULL'}, "
        f"{esc(req_id)}, "
        f"{esc(req_title)}, "
        f"{esc(exec_cat)}, "
        f"{esc(criticality)}, "
        f"{esc(ob_type)}, "
        f"{esc(s1)}, "
        f"{esc(s2)}, "
        f"{esc(s3)}, "
        f"NULL, "
        f"{esc(aj)}, "
        f"{esc(schema_ver)}, "
        f"{esc(status)}, "
        f"{1 if is_current else 0}"
        f");"
    )

lines += [
    "",
    f"-- Verify after insert",
    f"SELECT requirement_id, execution_category, obligation_type, criticality",
    f"FROM compliance_analysis",
    f"WHERE regulation_id = {REGULATION_ID} AND schema_version = 'v2' AND is_current = 1",
    f"ORDER BY requirement_id;",
]

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print(f"Written {len(rows)} INSERT statements to {OUTPUT_FILE}")
print(f"Run that file on prod using SSMS or sqlcmd.")
