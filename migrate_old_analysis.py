"""
Migration: old analysis_json  →  stage1/stage2/stage3/stage4 (schema_version='v2')

Run with DRY_RUN=True first to preview, then set DRY_RUN=False to write.
"""

import json
import os
import re
import sys
from datetime import datetime
from dotenv import load_dotenv
load_dotenv(override=True)

from storage.mssql_repo import MSSQLRepository

# ── Config ────────────────────────────────────────────────────────────────────
DRY_RUN        = False        # set False to actually write to DB
REGULATION_IDS = [27880]


# ── DB ────────────────────────────────────────────────────────────────────────
repo = MSSQLRepository({
    "server":   os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "username": os.getenv("MSSQL_USERNAME"),
    "password": os.getenv("MSSQL_PASSWORD"),
    "driver":   os.getenv("MSSQL_DRIVER"),
})


# ── Inference helpers ─────────────────────────────────────────────────────────

def infer_obligation_type(text: str, department: str = "") -> str:
    t = text.lower()
    d = department.lower()
    if any(w in t for w in ["report", "submit", "furnish", "notify", "disclose", "provide"]):
        return "Reporting"
    if any(w in t for w in ["shall not", "prohibit", "must not", "forbidden", "not permitted"]):
        return "Preventive"
    if any(w in t for w in ["monitor", "review", "examine", "scrutinize", "assess", "inspect"]):
        return "Detective"
    if any(w in t for w in ["policy", "governance", "board", "committee", "approval", "appoint"]):
        return "Governance"
    if any(w in t for w in ["document", "record", "maintain", "keep", "retain"]):
        return "Documentation"
    return "Governance"


def infer_execution_category(text: str, risk_level: str) -> str:
    t = text.lower()
    if any(w in t for w in ["at all times", "continuously", "ongoing", "daily", "always maintain"]):
        return "Ongoing_Control"
    if any(w in t for w in ["annually", "monthly", "quarterly", "periodic", "each year"]):
        return "Periodic_Review"
    if any(w in t for w in ["prior to", "before launching", "before opening", "before entering",
                              "upon appointment", "before distributing", "before use"]):
        return "One_Time_Implementation"
    if any(w in t for w in ["report to", "submit to", "notify", "furnish", "provide to"]):
        if any(w in t for w in ["monthly", "quarterly", "annually", "within"]):
            return "Periodic_Review"
        return "One_Off_Reporting"
    if risk_level == "High":
        return "Ongoing_Control"
    if risk_level == "Low":
        return "One_Time_Implementation"
    return "One_Time_Implementation"


def infer_control_type(obligation_type: str) -> str:
    return "Detective" if obligation_type == "Detective" else "Preventive"


def infer_frequency(execution_category: str, text: str) -> str:
    t = text.lower()
    if "daily"     in t: return "Daily"
    if "weekly"    in t: return "Weekly"
    if "monthly"   in t: return "Monthly"
    if "quarterly" in t: return "Quarterly"
    if "annually"  in t or "annual" in t: return "Annually"
    if execution_category == "Ongoing_Control":    return "Daily"
    if execution_category == "Periodic_Review":    return "Monthly"
    return "Event-Driven"


# ── Core converter ────────────────────────────────────────────────────────────

def convert_regulation(regulation_id: int, old_requirements: list) -> list:
    """
    Convert a flat list of old requirements into new-schema rows.
    One row per requirement, one obligation per row.
    Returns list of dicts ready for repo.store_analysis().
    """
    rows = []
    stage4_lines = [
        f"# Compliance Analysis — Regulation {regulation_id}",
        f"*Migrated from legacy format on {datetime.utcnow().date()}*\n",
        "## Obligation Inventory\n",
        "| ID | Obligation | Type | Criticality | Execution Category |",
        "|----|-----------|------|-------------|-------------------|",
    ]

    for i, req in enumerate(old_requirements, start=1):
        req_id    = f"REQ-{i:03d}"
        ob_id     = f"{req_id}-OB-001"
        text      = req.get("requirement_text", "")
        dept      = req.get("department", "Compliance")
        risk      = req.get("risk_level", "Medium")
        reference = req.get("reference", "")
        controls  = req.get("controls", [])
        kpis      = req.get("kpis", [])

        title            = text[:80].strip() + ("..." if len(text) > 80 else "")
        ob_type          = infer_obligation_type(text, dept)
        exec_cat         = infer_execution_category(text, risk)
        ctrl_type        = infer_control_type(ob_type)
        frequency        = infer_frequency(exec_cat, text)

        # ── stage1_json ──────────────────────────────────────────────────
        stage1 = {
            "requirement_id":    req_id,
            "requirement_title": title,
            "obligations": [{
                "obligation_id":   ob_id,
                "obligation_text": text,
                "source_reference": reference,
            }],
        }

        # ── stage2_json ──────────────────────────────────────────────────
        evidence = []
        t = text.lower()
        if any(w in t for w in ["policy", "procedure"]): evidence.append("Policy")
        if any(w in t for w in ["record", "document", "file"]): evidence.append("Record")
        if any(w in t for w in ["report", "statement"]): evidence.append("Report")
        if any(w in t for w in ["log", "register"]): evidence.append("Log")
        if any(w in t for w in ["approval", "approval by"]): evidence.append("Approval")
        if not evidence: evidence = ["Record"]

        test_method = (
            f"Review {', '.join(controls[:2]) if controls else 'relevant documentation'} "
            f"to verify compliance with {reference or 'this obligation'}."
        )

        stage2 = {
            "requirement_id":    req_id,
            "requirement_title": title,
            "normalized_obligations": [{
                "obligation_id":       ob_id,
                "obligation_text":     text,
                "obligation_type":     ob_type,
                "criticality":         risk,
                "evidence_expected":   evidence,
                "test_method":         test_method,
                "clarity_score":       4,
                "needs_manual_review": False,
                "source_reference":    reference,
                "execution_category":  exec_cat,
            }],
        }

        # ── stage3_json ──────────────────────────────────────────────────
        if controls:
            control_obj = {
                "control_title":           controls[0],
                "control_objective":       (
                    f"Ensure compliance with {reference} by implementing "
                    f"{controls[0].lower()}."
                ),
                "control_description":     (
                    f"{controls[0]}. " +
                    (f"Supporting measures: {'; '.join(controls[1:])}." if len(controls) > 1 else "")
                ).strip(),
                "control_owner":           dept,
                "control_type":            ctrl_type,
                "execution_type":          "Hybrid",
                "frequency":               frequency,
                "control_level":           "Process",
                "evidence_generated":      (
                    f"Compliance documentation, {evidence[0].lower()} records "
                    f"for {reference}."
                ),
                "key_steps":               controls,
                "residual_risk_if_failed": risk,
            }
        else:
            control_obj = None

        stage3 = {
            "requirement_id":    req_id,
            "requirement_title": title,
            "obligations": [{
                "obligation_id":      ob_id,
                "obligation_text":    text,
                "execution_category": exec_cat,
                "control":            control_obj,
            }],
        }

        # ── stage4 table row ─────────────────────────────────────────────
        stage4_lines.append(
            f"| {ob_id} | {text[:60]}{'...' if len(text)>60 else ''} "
            f"| {ob_type} | {risk} | {exec_cat} |"
        )

        # ── original analysis_json (preserve old data) ───────────────────
        old_analysis = {
            "requirement_id":    req_id,
            "requirement_title": title,
            "obligations":       stage2["normalized_obligations"],
            "controls":          [c for c in [control_obj] if c],
        }

        rows.append({
            "regulation_id":      regulation_id,
            "requirement_id":     req_id,
            "requirement_title":  title,
            "execution_category": exec_cat,
            "criticality":        risk,
            "obligation_type":    ob_type,
            "stage1_json":        json.dumps(stage1,       ensure_ascii=False),
            "stage2_json":        json.dumps(stage2,       ensure_ascii=False),
            "stage3_json":        json.dumps(stage3,       ensure_ascii=False),
            "stage4_md":          None,   # set on first row below
            "analysis_json":      json.dumps(old_analysis, ensure_ascii=False),
        })

    # Attach stage4 markdown to the first row only
    if rows:
        stage4_lines += [
            "\n## KPIs Referenced\n",
            *[f"- {k}" for req in old_requirements for k in req.get("kpis", [])],
        ]
        rows[0]["stage4_md"] = "\n".join(stage4_lines)

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}")
    print(f"Migration script  |  DRY_RUN={DRY_RUN}")
    print(f"Regulations: {REGULATION_IDS}")
    print(f"{'='*70}\n")

    for reg_id in REGULATION_IDS:
        print(f"\n-- Regulation {reg_id} ------------------------------------------")

        # Load old row(s) for this regulation
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, analysis_json
                FROM compliance_analysis
                WHERE regulation_id = ?
                  AND (schema_version IS NULL OR schema_version != 'v2')
                ORDER BY id
                """,
                [reg_id],
            )
            old_rows = cursor.fetchall()

        if not old_rows:
            print(f"  No old-schema rows found — skipping.")
            continue

        # Check if new-schema rows already exist
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM compliance_analysis WHERE regulation_id=? AND schema_version='v2' AND is_current=1",
                [reg_id],
            )
            existing_v2 = cursor.fetchone()[0]

        if existing_v2 > 0:
            print(f"  {existing_v2} v2 rows already exist — skipping (use force logic if needed).")
            continue

        all_requirements = []
        for row_id, analysis_json_str in old_rows:
            if not analysis_json_str:
                print(f"  Row {row_id}: empty analysis_json — skipping.")
                continue
            try:
                data = json.loads(analysis_json_str)
                reqs = data.get("requirements", [])
                print(f"  Row {row_id}: {len(reqs)} requirements found in analysis_json")
                all_requirements.extend(reqs)
            except Exception as e:
                print(f"  Row {row_id}: JSON parse error — {e}")
                continue

        if not all_requirements:
            print(f"  No requirements extracted — skipping.")
            continue

        new_rows = convert_regulation(reg_id, all_requirements)
        print(f"\n  Will insert {len(new_rows)} new v2 rows:")

        for r in new_rows:
            s2 = json.loads(r["stage2_json"])
            ob = s2["normalized_obligations"][0]
            print(f"    {r['requirement_id']} | {ob['criticality']:6} | {ob['execution_category']:30} | {ob['obligation_type']:15} | {ob['obligation_text'][:55]}...")

        print(f"\n  stage4_md preview (first 300 chars):")
        print(f"  {new_rows[0]['stage4_md'][:300]}")

        if not DRY_RUN:
            repo.store_analysis(new_rows, version_id=None)
            print(f"\n  [OK] Inserted {len(new_rows)} rows for regulation {reg_id}")
        else:
            print(f"\n  [DRY RUN] — nothing written. Set DRY_RUN=False to insert.")

    print(f"\n{'='*70}")
    print("Done.")


if __name__ == "__main__":
    main()
