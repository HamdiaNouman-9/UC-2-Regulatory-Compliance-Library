"""
Generates prod_control_links_27880.sql

Links existing COMPLIANCE_REQUIREMENT rows to existing DEMO_CONTROL rows
via DEMO_REQUIREMENT_CONTROL_LINK — 5 rows (deduped by req+ctrl).

Stage3 controls are already stored in compliance_analysis. This patch only
creates the cross-reference between COMPLIANCE_REQUIREMENT and DEMO_CONTROL.
"""

REGULATION_ID = 27880

def esc(s: str) -> str:
    return s.replace("'", "''")

# ── A: DEMO_REQUIREMENT_CONTROL_LINK ─────────────────────────────────────────
# Columns: COMPLIANCEREQUIREMENT_ID, CONTROL_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID
# Deduped on (COMPLIANCEREQUIREMENT_ID, CONTROL_ID); best match_status wins.
#
# How these were derived:
#   Stage3 Ongoing_Control obligations → matched COMPLIANCE_REQUIREMENT (from sama_requirement_mapping)
#   Stage3 control title → semantically matched against DEMO_CONTROL (43 rows, prod snapshot)
#
# Only obligations with a matched_requirement_id contribute links.
# Multiple obligations can map to the same (req, ctrl) pair — only one link is inserted.

CONTROL_LINKS = [
    {
        "compliancerequirement_id": 16,   # "Financial institutions must conduct business risk assessments..."
        "control_id": 18,                  # "Documented risk assessment methodology"
        "match_status": "partially_matched",
        "match_explanation": (
            "Enterprise-Wide AML/CFT Risk Assessment Review (REQ-001-OB-001) partially matches "
            "the Documented risk assessment methodology control: both target AML/CFT risk assessment "
            "documentation and review cycles, but the stage3 control adds annual scheduling, "
            "multi-dimensional coverage, and senior management sign-off requirements not specified "
            "in the existing control."
        ),
    },
    {
        "compliancerequirement_id": 1,    # "AML Customer Due Diligence"
        "control_id": 1,                   # "CDD Verification Checklist"
        "match_status": "fully_matched",
        "match_explanation": (
            "Customer Due Diligence Completion at Onboarding (REQ-002-OB-002) fully matches "
            "the CDD Verification Checklist control: both mandate completion of a CDD checklist "
            "at customer onboarding with sign-off before account activation."
        ),
    },
    {
        "compliancerequirement_id": 2,    # "Enhanced Due Diligence for High Risk Customers"
        "control_id": 2,                   # "PEP Screening Process"
        "match_status": "fully_matched",
        "match_explanation": (
            "PEP Screening and Additional Measures Workflow (REQ-002-OB-005) fully matches "
            "the PEP Screening Process control: both require automated PEP database screening at "
            "onboarding and on an ongoing basis with documented additional measures upon a confirmed "
            "match. Also covers High-Risk Country EDD Application (REQ-005-OB-001) which partially "
            "overlaps with this control's high-risk customer scope."
        ),
    },
    {
        "compliancerequirement_id": 5,    # "Ongoing Customer Monitoring"
        "control_id": 5,                   # "Periodic Customer Review Schedule"
        "match_status": "partially_matched",
        "match_explanation": (
            "Ongoing Transaction Monitoring Against Customer Profile (REQ-007-OB-001) and "
            "Enhanced Monitoring for High-Risk Customer Relationships (REQ-007-OB-003) both "
            "partially match the Periodic Customer Review Schedule control: the existing control "
            "covers periodic profile reviews but the stage3 controls additionally require "
            "continuous transaction-level monitoring and risk-tier-based parameter calibration."
        ),
    },
    {
        "compliancerequirement_id": 76,   # "Financial institutions must implement CFT Guidance Paper"
        "control_id": 31,                  # "Maintain an AML/CFT program compliant with SAMA regulations"
        "match_status": "partially_matched",
        "match_explanation": (
            "AML/CFT Policy Framework Maintenance and Senior Management Approval (REQ-008-OB-001) "
            "partially matches the Maintain AML/CFT program control: both target a comprehensive "
            "AML/CFT compliance framework, but the stage3 control adds explicit senior management "
            "approval, proportionality assessment, and annual review cycle requirements not stated "
            "in the existing control."
        ),
    },
]

# ── B: DEMO_CONTROL IS_SUGGESTED inserts ─────────────────────────────────────
# Columns: TITLE, DESCRIPTION, CONTROL_KEY, IS_SUGGESTED (=1), CREATEDON (=GETDATE())
# 8 new controls for CBB-specific obligations that have no matching existing DEMO_CONTROL.
# All belong to "new" obligations (matched_requirement_id=NULL) so no DEMO_REQUIREMENT_CONTROL_LINK
# can be created — they are stored as suggested controls for future manual linking.

NEW_CONTROLS = [
    {
        "obligation_id": "REQ-003-OB-002",
        "control_key":  f"CBB-AUTO-CTRL-{REGULATION_ID}-REQ003-OB002",
        "title": "Correspondent Banking Shell Bank Prohibition and Periodic Review",
        "description": (
            "Ensures no correspondent banking relationship is established or maintained with a shell bank "
            "or an institution that permits shell bank access. The policy explicitly prohibits such "
            "relationships. All existing correspondent relationships undergo annual due diligence review "
            "including a written certification from each respondent confirming no shell bank access. "
            "Relationships where shell bank access cannot be ruled out are escalated for termination. "
            "Control owner: Correspondent Banking / Compliance."
        ),
    },
    {
        "obligation_id": "REQ-004-OB-001",
        "control_key":  f"CBB-AUTO-CTRL-{REGULATION_ID}-REQ004-OB001",
        "title": "Wire Transfer Originator and Beneficiary Data Completeness",
        "description": (
            "Ensures all outgoing wire transfers carry complete and accurate originator and beneficiary "
            "information throughout the payment chain. The payments system enforces mandatory field "
            "validation before transmission; incomplete transfers are rejected with error codes. A daily "
            "reconciliation report confirms field completeness for all transmitted messages. "
            "Control owner: Operations / Payments. Frequency: Per-Transaction."
        ),
    },
    {
        "obligation_id": "REQ-004-OB-002",
        "control_key":  f"CBB-AUTO-CTRL-{REGULATION_ID}-REQ004-OB002",
        "title": "Wire Transfer Hard Block on Missing Mandatory Data",
        "description": (
            "Prevents final execution of any wire transfer where mandatory originator or beneficiary "
            "fields remain unpopulated after validation. The block cannot be overridden without documented "
            "Compliance approval. Blocked transfers are logged with reason codes and reviewed for potential "
            "suspicious transaction reporting. Control owner: Operations / Compliance. Frequency: Per-Transaction."
        ),
    },
    {
        "obligation_id": "REQ-004-OB-004",
        "control_key":  f"CBB-AUTO-CTRL-{REGULATION_ID}-REQ004-OB004",
        "title": "Wire Transfer Regulatory Compliance Gap Analysis",
        "description": (
            "Maintains a living gap analysis mapping the institution's wire transfer policies and system "
            "controls to every requirement in the AML Implementing Regulation. Reviewed and updated annually "
            "and on each regulatory amendment. Identified gaps are tracked as remediation items with owners "
            "and deadlines. Approved by the MLRO and presented to the AML Committee. "
            "Control owner: Compliance / Operations."
        ),
    },
    {
        "obligation_id": "REQ-005-OB-002",
        "control_key":  f"CBB-AUTO-CTRL-{REGULATION_ID}-REQ005-OB002",
        "title": "AML Permanent Committee Countermeasure Implementation",
        "description": (
            "Ensures all countermeasures prescribed by the AML Permanent Committee for high-risk countries "
            "are implemented within the required timeframe. A designated mechanism tracks official directives; "
            "on receipt, a structured implementation project is initiated with a named owner and deadline. "
            "System-level controls are developed, tested, and deployed before the effective date. "
            "Post-implementation review confirms controls are operating as intended. "
            "Control owner: Compliance / Sanctions Unit."
        ),
    },
    {
        "obligation_id": "REQ-007-OB-002",
        "control_key":  f"CBB-AUTO-CTRL-{REGULATION_ID}-REQ007-OB002",
        "title": "Complex and Unusual Transaction Examination",
        "description": (
            "Ensures every complex, unusually large, or pattern-based suspicious transaction is examined "
            "and documented. Transaction monitoring rules target complex transactions, large-value outliers, "
            "and pattern anomalies. Each triggered alert generates a mandatory examination case. The outcome "
            "is documented regardless of whether an STR is filed; all records are retained for 10 years. "
            "Control owner: Financial Intelligence Unit. Frequency: Per-Transaction + Daily batch."
        ),
    },
    {
        "obligation_id": "REQ-008-OB-003",
        "control_key":  f"CBB-AUTO-CTRL-{REGULATION_ID}-REQ008-OB003",
        "title": "Group-Wide AML Policy Application to Branches and Subsidiaries",
        "description": (
            "Ensures the institution's AML/CFT policies, procedures, and controls are implemented "
            "consistently across all branches and majority-owned subsidiaries. Compliance maintains an "
            "entity register; annual attestation forms are distributed and signed by local compliance "
            "officers. Exceptions are reported to group Compliance for remediation tracking. "
            "Control owner: Compliance / Group Functions. Frequency: Annual + Event-Driven."
        ),
    },
    {
        "obligation_id": "REQ-009-OB-003",
        "control_key":  f"CBB-AUTO-CTRL-{REGULATION_ID}-REQ009-OB003",
        "title": "Tipping-Off Prohibition Enforcement and Training",
        "description": (
            "Ensures all staff are aware of and comply with the prohibition on disclosing STR-related "
            "information. The prohibition is embedded in the employee code of conduct, AML policy, and "
            "STR handling procedure. Mandatory annual AML training includes a dedicated tipping-off module. "
            "The STR process enforces strict information barriers; only authorized personnel have access "
            "to STR-related records. Control owner: Compliance / HR. Frequency: Annual training + Ongoing."
        ),
    },
]


def main():
    lines = ["BEGIN TRANSACTION;", ""]
    lines.append(f"-- Control links + new suggested controls for regulation_id={REGULATION_ID}")
    lines.append(f"-- {len(CONTROL_LINKS)} DEMO_REQUIREMENT_CONTROL_LINK rows (deduped by req+ctrl)")
    lines.append("")

    # ── A: Control links ──────────────────────────────────────────────────────
    lines.append("")
    for i, lnk in enumerate(CONTROL_LINKS, start=1):
        lines.append(
            f"-- {i}. COMPLIANCE_REQUIREMENT id={lnk['compliancerequirement_id']} "
            f"→ DEMO_CONTROL id={lnk['control_id']} [{lnk['match_status']}]"
        )
        lines.append(
            f"INSERT INTO DEMO_REQUIREMENT_CONTROL_LINK "
            f"(COMPLIANCEREQUIREMENT_ID, CONTROL_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID) VALUES "
            f"({lnk['compliancerequirement_id']}, {lnk['control_id']}, "
            f"N'{lnk['match_status']}', N'{esc(lnk['match_explanation'])}', {REGULATION_ID});"
        )
        lines.append("")

    # ── Verification ──────────────────────────────────────────────────────────
    lines.append("-- ── Verification ─────────────────────────────────────────────────────────")
    lines.append("SELECT MATCH_STATUS, COUNT(*) AS cnt")
    lines.append("FROM DEMO_REQUIREMENT_CONTROL_LINK")
    lines.append(f"WHERE REGULATION_ID = {REGULATION_ID}")
    lines.append("GROUP BY MATCH_STATUS;")
    lines.append("")
    lines.append("-- Expected: fully_matched=2, partially_matched=3")
    lines.append("")
    lines.append("COMMIT;")
    lines.append("-- On error: ROLLBACK;")

    sql = "\n".join(lines)
    out = "prod_control_links_27880.sql"
    with open(out, "w", encoding="utf-8") as f:
        f.write(sql)
    print(f"Written to {out} ({len(sql):,} bytes)")
    print(f"  DEMO_REQUIREMENT_CONTROL_LINK rows: {len(CONTROL_LINKS)}")


if __name__ == "__main__":
    main()
