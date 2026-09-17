"""
Patch for regulation 27868 (Banking Control Law)
Run AFTER migrate_old_analysis.py (DRY_RUN=False)

Fixes:
  REQ-008  execution_category  One_Time_Implementation → Ongoing_Control
  REQ-018  execution_category  Ongoing_Control         → Periodic_Review
  REQ-022  execution_category  One_Time_Implementation → Periodic_Review
  REQ-004  obligation_type     Governance              → Preventive

Adds:
  REQ-031  Disposal of Debt-Satisfaction Equity Interests  (Art 10 para 2)
  REQ-032  Disposal of Debt-Satisfaction Real Estate       (Art 10 para 5)
  REQ-033  Annual General Meeting + Dual Report to SAMA    (Art 14 para 2)
"""

import json, os
from dotenv import load_dotenv
load_dotenv(override=True)
from storage.mssql_repo import MSSQLRepository

DRY_RUN = False   # set False to actually write

repo = MSSQLRepository({
    "server":   os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "username": os.getenv("MSSQL_USERNAME"),
    "password": os.getenv("MSSQL_PASSWORD"),
    "driver":   os.getenv("MSSQL_DRIVER"),
})

REGULATION_ID = 27868

# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — Fix 4 misclassified rows
# Updates both the column AND the value inside stage2_json / stage3_json
# ─────────────────────────────────────────────────────────────────────────────

FIXES = [
    # (requirement_id, field, old_value, new_value)
    ("REQ-008", "execution_category", "One_Time_Implementation", "Ongoing_Control"),
    ("REQ-019", "execution_category", "One_Time_Implementation", "Periodic_Review"),
    ("REQ-022", "execution_category", "One_Time_Implementation", "Periodic_Review"),
    ("REQ-004", "obligation_type",    "Governance",              "Preventive"),
]

def apply_fixes():
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        for req_id, field, old_val, new_val in FIXES:
            # Fetch current row
            cursor.execute(
                "SELECT id, stage2_json, stage3_json FROM compliance_analysis "
                "WHERE regulation_id=? AND requirement_id=? AND schema_version='v2' AND is_current=1",
                [REGULATION_ID, req_id],
            )
            row = cursor.fetchone()
            if not row:
                print(f"  WARN: {req_id} not found — skipping fix.")
                continue

            row_id, s2_str, s3_str = row
            s2 = json.loads(s2_str) if s2_str else {}
            s3 = json.loads(s3_str) if s3_str else {}

            # Patch stage2_json
            for ob in s2.get("normalized_obligations", []):
                if field in ob:
                    ob[field] = new_val

            # Patch stage3_json
            for ob in s3.get("obligations", []):
                if field in ob:
                    ob[field] = new_val
                if field == "obligation_type" and ob.get("control"):
                    if field == "obligation_type":
                        pass  # control_type is separate — leave as-is

            if DRY_RUN:
                print(f"  [DRY] Would fix {req_id}: {field}  {old_val} → {new_val}")
                ob_preview = (s2.get("normalized_obligations") or [{}])[0]
                print(f"        stage2 {field} after patch: {ob_preview.get(field)}")
            else:
                if field == "execution_category":
                    cursor.execute(
                        "UPDATE compliance_analysis SET execution_category=?, stage2_json=?, stage3_json=?, "
                        "updated_at=GETUTCDATE() WHERE id=?",
                        [new_val, json.dumps(s2, ensure_ascii=False),
                         json.dumps(s3, ensure_ascii=False), row_id],
                    )
                else:  # obligation_type
                    cursor.execute(
                        "UPDATE compliance_analysis SET obligation_type=?, stage2_json=?, "
                        "updated_at=GETUTCDATE() WHERE id=?",
                        [new_val, json.dumps(s2, ensure_ascii=False), row_id],
                    )
                print(f"  Fixed {req_id}: {field}  {old_val} → {new_val}")

        if not DRY_RUN:
            conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — Insert 3 missing requirements (all 4 stages)
# ─────────────────────────────────────────────────────────────────────────────

NEW_ROWS = [

    # ── REQ-031 ───────────────────────────────────────────────────────────────
    {
        "regulation_id":      REGULATION_ID,
        "requirement_id":     "REQ-031",
        "requirement_title":  "Disposal of Debt-Satisfaction Equity Interests",
        "execution_category": "Periodic_Review",
        "criticality":        "Medium",
        "obligation_type":    "Preventive",

        "stage1_json": {
            "requirement_id":    "REQ-031",
            "requirement_title": "Disposal of Debt-Satisfaction Equity Interests",
            "obligations": [{
                "obligation_id":   "REQ-031-OB-001",
                "obligation_text": (
                    "The bank shall liquidate equity interests acquired in satisfaction of debts "
                    "within two years of acquisition, or within any longer period agreed with SAMA."
                ),
                "source_reference": "Article 10, Paragraph 2",
            }],
        },

        "stage2_json": {
            "requirement_id":    "REQ-031",
            "requirement_title": "Disposal of Debt-Satisfaction Equity Interests",
            "normalized_obligations": [{
                "obligation_id":       "REQ-031-OB-001",
                "obligation_text": (
                    "The bank shall liquidate equity interests acquired in satisfaction of debts "
                    "within two years of acquisition, or within any longer period agreed with SAMA."
                ),
                "obligation_type":     "Preventive",
                "criticality":         "Medium",
                "evidence_expected":   ["Record", "Approval", "Log"],
                "test_method": (
                    "Auditor reviews the register of debt-satisfaction equity holdings and confirms "
                    "all interests older than 2 years (without an approved SAMA extension) have been liquidated."
                ),
                "clarity_score":       5,
                "needs_manual_review": False,
                "source_reference":    "Article 10, Paragraph 2",
                "execution_category":  "Periodic_Review",
            }],
        },

        "stage3_json": {
            "requirement_id":    "REQ-031",
            "requirement_title": "Disposal of Debt-Satisfaction Equity Interests",
            "obligations": [{
                "obligation_id":      "REQ-031-OB-001",
                "obligation_text": (
                    "The bank shall liquidate equity interests acquired in satisfaction of debts "
                    "within two years of acquisition, or within any longer period agreed with SAMA."
                ),
                "execution_category": "Periodic_Review",
                "control": {
                    "control_title":       "Debt-Satisfaction Equity Disposal Register",
                    "control_objective": (
                        "Ensure equity interests acquired in debt satisfaction are disposed of "
                        "within the 2-year regulatory deadline or an approved SAMA extension."
                    ),
                    "control_description": (
                        "Maintain a register of all equity interests acquired in debt satisfaction. "
                        "Track acquisition dates, regulatory deadlines, and disposal status. "
                        "Escalate approaching deadlines to senior management and seek SAMA approval for extensions."
                    ),
                    "control_owner":       "Strategic Investments / Legal",
                    "control_type":        "Preventive",
                    "execution_type":      "Manual",
                    "frequency":           "Quarterly",
                    "control_level":       "Process",
                    "evidence_generated": (
                        "Equity disposal register, SAMA extension approvals, disposal confirmation documents."
                    ),
                    "key_steps": [
                        "Maintain register of debt-satisfaction equity interests with acquisition dates and deadlines",
                        "Review register quarterly for items approaching the 2-year limit",
                        "Initiate disposal process at least 6 months before deadline",
                        "Seek SAMA approval for extension where required and document outcome",
                        "Confirm and record completion of disposal",
                    ],
                    "residual_risk_if_failed": "Medium",
                },
            }],
        },

        "stage4_md": None,
    },

    # ── REQ-032 ───────────────────────────────────────────────────────────────
    {
        "regulation_id":      REGULATION_ID,
        "requirement_id":     "REQ-032",
        "requirement_title":  "Disposal of Debt-Satisfaction Real Estate",
        "execution_category": "Periodic_Review",
        "criticality":        "Medium",
        "obligation_type":    "Preventive",

        "stage1_json": {
            "requirement_id":    "REQ-032",
            "requirement_title": "Disposal of Debt-Satisfaction Real Estate",
            "obligations": [{
                "obligation_id":   "REQ-032-OB-001",
                "obligation_text": (
                    "The bank shall liquidate real estate acquired in satisfaction of debts "
                    "(not needed for banking operations or staff housing/recreation) within three years "
                    "of acquisition, or within the period approved by SAMA."
                ),
                "source_reference": "Article 10, Paragraph 5",
            }],
        },

        "stage2_json": {
            "requirement_id":    "REQ-032",
            "requirement_title": "Disposal of Debt-Satisfaction Real Estate",
            "normalized_obligations": [{
                "obligation_id":       "REQ-032-OB-001",
                "obligation_text": (
                    "The bank shall liquidate real estate acquired in satisfaction of debts "
                    "(not needed for banking operations or staff housing/recreation) within three years "
                    "of acquisition, or within the period approved by SAMA."
                ),
                "obligation_type":     "Preventive",
                "criticality":         "Medium",
                "evidence_expected":   ["Record", "Approval", "Log"],
                "test_method": (
                    "Auditor reviews the real estate register and confirms all debt-satisfaction "
                    "properties not used for banking or staff purposes, held beyond 3 years "
                    "without SAMA approval, have been disposed of."
                ),
                "clarity_score":       5,
                "needs_manual_review": False,
                "source_reference":    "Article 10, Paragraph 5",
                "execution_category":  "Periodic_Review",
            }],
        },

        "stage3_json": {
            "requirement_id":    "REQ-032",
            "requirement_title": "Disposal of Debt-Satisfaction Real Estate",
            "obligations": [{
                "obligation_id":      "REQ-032-OB-001",
                "obligation_text": (
                    "The bank shall liquidate real estate acquired in satisfaction of debts "
                    "(not needed for banking operations or staff housing/recreation) within three years "
                    "of acquisition, or within the period approved by SAMA."
                ),
                "execution_category": "Periodic_Review",
                "control": {
                    "control_title":       "Debt-Satisfaction Real Estate Disposal Tracking",
                    "control_objective": (
                        "Ensure real estate acquired in debt satisfaction that is not required "
                        "for banking operations or staff use is disposed of within the 3-year "
                        "regulatory deadline or an approved SAMA extension."
                    ),
                    "control_description": (
                        "Maintain a property register for all real estate acquired in debt satisfaction. "
                        "Classify each property as required for operations/staff or pending disposal. "
                        "Track disposal deadlines and escalate overdue properties to senior management."
                    ),
                    "control_owner":       "Real Estate / Finance",
                    "control_type":        "Preventive",
                    "execution_type":      "Manual",
                    "frequency":           "Quarterly",
                    "control_level":       "Process",
                    "evidence_generated": (
                        "Real estate register, property classification records, SAMA extension approvals, "
                        "disposal confirmation documents."
                    ),
                    "key_steps": [
                        "Maintain register of all debt-satisfaction properties with acquisition dates and purpose classification",
                        "Identify properties classified as non-essential (not for banking/staff use)",
                        "Calculate 3-year disposal deadline for each non-essential property",
                        "Review register quarterly and escalate properties within 6 months of deadline",
                        "Obtain SAMA approval for any required extension and document outcome",
                        "Confirm and record disposal completion",
                    ],
                    "residual_risk_if_failed": "Medium",
                },
            }],
        },

        "stage4_md": None,
    },

    # ── REQ-033 ───────────────────────────────────────────────────────────────
    {
        "regulation_id":      REGULATION_ID,
        "requirement_id":     "REQ-033",
        "requirement_title":  "Annual General Meeting and Dual Report Submission to SAMA",
        "execution_category": "Periodic_Review",
        "criticality":        "Medium",
        "obligation_type":    "Reporting",

        "stage1_json": {
            "requirement_id":    "REQ-033",
            "requirement_title": "Annual General Meeting and Dual Report Submission to SAMA",
            "obligations": [{
                "obligation_id":   "REQ-033-OB-001",
                "obligation_text": (
                    "The bank shall hold the General Meeting within six months of the financial year-end, "
                    "at which both the auditor report and the management annual report shall be read together. "
                    "The bank management shall send copies of both reports to SAMA."
                ),
                "source_reference": "Article 14, Paragraph 2",
            }],
        },

        "stage2_json": {
            "requirement_id":    "REQ-033",
            "requirement_title": "Annual General Meeting and Dual Report Submission to SAMA",
            "normalized_obligations": [{
                "obligation_id":       "REQ-033-OB-001",
                "obligation_text": (
                    "The bank shall hold the General Meeting within six months of the financial year-end, "
                    "at which both the auditor report and the management annual report shall be read together. "
                    "The bank management shall send copies of both reports to SAMA."
                ),
                "obligation_type":     "Reporting",
                "criticality":         "Medium",
                "evidence_expected":   ["Report", "Record", "Approval"],
                "test_method": (
                    "Auditor confirms General Meeting minutes showing both reports were presented "
                    "within 6 months of financial year-end, and verifies SAMA submission receipts for both reports."
                ),
                "clarity_score":       5,
                "needs_manual_review": False,
                "source_reference":    "Article 14, Paragraph 2",
                "execution_category":  "Periodic_Review",
            }],
        },

        "stage3_json": {
            "requirement_id":    "REQ-033",
            "requirement_title": "Annual General Meeting and Dual Report Submission to SAMA",
            "obligations": [{
                "obligation_id":      "REQ-033-OB-001",
                "obligation_text": (
                    "The bank shall hold the General Meeting within six months of the financial year-end, "
                    "at which both the auditor report and the management annual report shall be read together. "
                    "The bank management shall send copies of both reports to SAMA."
                ),
                "execution_category": "Periodic_Review",
                "control": {
                    "control_title":       "Annual General Meeting and SAMA Report Submission Process",
                    "control_objective": (
                        "Ensure the General Meeting is held within 6 months of financial year-end with "
                        "both reports presented, and that copies are submitted to SAMA."
                    ),
                    "control_description": (
                        "Coordinate the scheduling of the Annual General Meeting within the regulatory deadline. "
                        "Ensure both the external auditor report and the management annual report are finalised "
                        "and presented at the meeting. Transmit copies of both reports to SAMA within the same deadline."
                    ),
                    "control_owner":       "Finance / Regulatory Reporting",
                    "control_type":        "Preventive",
                    "execution_type":      "Manual",
                    "frequency":           "Annually",
                    "control_level":       "Process",
                    "evidence_generated": (
                        "AGM notice and minutes, auditor report, management annual report, "
                        "SAMA submission receipts."
                    ),
                    "key_steps": [
                        "Set AGM date within 6 months of financial year-end and communicate to all stakeholders",
                        "Confirm external auditor report is finalised before AGM",
                        "Confirm management annual report is finalised before AGM",
                        "Present both reports at AGM and capture in board minutes",
                        "Submit copies of both reports to SAMA and retain submission receipts",
                    ],
                    "residual_risk_if_failed": "Medium",
                },
            }],
        },

        "stage4_md": None,
    },
]


def insert_new_rows():
    for r in NEW_ROWS:
        # Serialize JSON fields
        r["stage1_json"] = json.dumps(r["stage1_json"], ensure_ascii=False)
        r["stage2_json"] = json.dumps(r["stage2_json"], ensure_ascii=False)
        r["stage3_json"] = json.dumps(r["stage3_json"], ensure_ascii=False)

        # Build analysis_json from stage2 content
        s2 = json.loads(r["stage2_json"])
        s3 = json.loads(r["stage3_json"])
        controls = [ob["control"] for ob in s3.get("obligations", []) if ob.get("control")]
        analysis = {
            "requirement_id":    r["requirement_id"],
            "requirement_title": r["requirement_title"],
            "obligations":       s2.get("normalized_obligations", []),
            "controls":          controls,
        }
        r["analysis_json"] = json.dumps(analysis, ensure_ascii=False)

    if DRY_RUN:
        for r in NEW_ROWS:
            s2 = json.loads(r["stage2_json"])
            ob = s2["normalized_obligations"][0]
            print(f"  [DRY] Would insert {r['requirement_id']}: {r['requirement_title']}")
            print(f"        criticality={ob['criticality']}  exec_cat={ob['execution_category']}  type={ob['obligation_type']}")
            print(f"        obligation_text: {ob['obligation_text'][:90]}...")
            s3 = json.loads(r["stage3_json"])
            ctrl = (s3.get("obligations") or [{}])[0].get("control") or {}
            print(f"        control_title: {ctrl.get('control_title','(none)')}")
            print(f"        control_owner: {ctrl.get('control_owner','(none)')}")
            print(f"        key_steps: {ctrl.get('key_steps', [])}")
            print()
    else:
        repo.store_analysis(NEW_ROWS, version_id=None)
        for r in NEW_ROWS:
            print(f"  Inserted {r['requirement_id']}: {r['requirement_title']}")


# ─────────────────────────────────────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def verify():
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT requirement_id, execution_category, obligation_type, criticality
            FROM compliance_analysis
            WHERE regulation_id = ? AND schema_version = 'v2' AND is_current = 1
            ORDER BY requirement_id
            """,
            [REGULATION_ID],
        )
        rows = cursor.fetchall()

    print(f"\n  {'REQ_ID':<10} {'EXEC_CATEGORY':<30} {'OBL_TYPE':<18} CRITICALITY")
    print(f"  {'-'*80}")
    for req_id, ec, ot, cr in rows:
        print(f"  {req_id:<10} {(ec or ''):<30} {(ot or ''):<18} {cr or ''}")
    print(f"\n  Total rows: {len(rows)}")


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*60)
    print(f"Patch: regulation {REGULATION_ID}")
    print("="*60)

    print("\n[1/2] Applying fixes to 4 misclassified rows...")
    apply_fixes()

    print("\n[2/2] Inserting 3 missing requirements...")
    insert_new_rows()

    print("\n[VERIFY] Final state of compliance_analysis for this regulation:")
    verify()

    print("\nDone. Run GET /compliance-analysis/27868 to confirm.")
