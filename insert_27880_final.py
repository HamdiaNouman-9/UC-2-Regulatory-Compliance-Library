"""
Final insert for regulation 27880 — replaces the 30 single-obligation migrated rows
with 9 properly grouped requirement rows (30 obligations total).
Deletes existing v2 rows first, then inserts the new structured data.
"""

import json, os
from dotenv import load_dotenv
load_dotenv(override=True)
from storage.mssql_repo import MSSQLRepository

REGULATION_ID = 27880
DRY_RUN = False

repo = MSSQLRepository({
    "server":   os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "username": os.getenv("MSSQL_USERNAME"),
    "password": os.getenv("MSSQL_PASSWORD"),
    "driver":   os.getenv("MSSQL_DRIVER"),
})

# ── Final analysis data ────────────────────────────────────────────────────────
REQUIREMENTS = [
    {
        "requirement_id": "REQ-001",
        "requirement_title": "Risk Assessment",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-001-OB-001",
                "obligation_text": "Identify, assess, and document money laundering risks and keep the assessment up to date, taking into account a wide range of risk factors, including those relating to customers, countries or geographic areas, products, services, transactions and delivery channels.",
                "obligation_type": "Detective",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Report", "Record"],
                "test_method": "Auditor reviews the enterprise-wide AML risk assessment document, checks the date of last update, and verifies it covers all required risk factor categories.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 5",
            },
            {
                "obligation_id": "REQ-001-OB-002",
                "obligation_text": "Assess, prior to their use, the risks associated with new products, business practices and technologies.",
                "obligation_type": "Detective",
                "criticality": "High",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Policy", "Approval", "Report"],
                "test_method": "Auditor selects a sample of new products or services launched in the review period and confirms a documented AML risk assessment was completed and approved before go-live.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 5",
            },
            {
                "obligation_id": "REQ-001-OB-003",
                "obligation_text": "Provide risk assessment reports to the supervisory authorities upon request.",
                "obligation_type": "Reporting",
                "criticality": "Medium",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Report", "Record"],
                "test_method": "Auditor confirms a designated repository exists for finalized risk assessment reports and that a procedure for responding to supervisory requests is documented and tested.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 5",
            },
        ],
    },
    {
        "requirement_id": "REQ-002",
        "requirement_title": "Customer Due Diligence",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-002-OB-001",
                "obligation_text": "Shall not keep or open anonymous accounts or accounts in obviously fictitious names, or numbered accounts.",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["System Configuration", "Log", "Procedure"],
                "test_method": "Auditor attempts to open a test account without valid identity documents in a sandboxed environment and confirms the system blocks the action; also reviews a sample of existing accounts for compliance.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 6",
            },
            {
                "obligation_id": "REQ-002-OB-002",
                "obligation_text": "Apply due diligence measures to customers. The Implementing Regulation shall set forth the instances and types of measures.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Procedure", "Record"],
                "test_method": "Auditor reviews the CDD policy and selects a sample of onboarded customers to confirm CDD was completed at onboarding in accordance with the Implementing Regulation.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 7(1)",
            },
            {
                "obligation_id": "REQ-002-OB-003",
                "obligation_text": "Determine the extent of due diligence measures based on the risks related to a customer or business relationship.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record", "Procedure"],
                "test_method": "Auditor reviews the customer risk rating methodology and confirms it maps risk levels to CDD tiers (Simplified / Standard / Enhanced); validates a sample of customer risk ratings against applied CDD level.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 7(2)",
            },
            {
                "obligation_id": "REQ-002-OB-004",
                "obligation_text": "Where a higher risk of money laundering was identified, apply enhanced due diligence measures.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Procedure", "Approval", "Record"],
                "test_method": "Auditor selects a sample of customers rated high-risk and confirms EDD procedures were applied, documented, and escalated for approval in accordance with policy.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 7(2)",
            },
            {
                "obligation_id": "REQ-002-OB-005",
                "obligation_text": "Use appropriate systems to determine whether a customer or beneficial owner is or has become a politically exposed person (PEP) and if so, apply additional measures as prescribed by the Implementing Regulation.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["System Configuration", "Log", "Procedure", "Record"],
                "test_method": "Auditor confirms screening system is configured for PEP databases; reviews a sample of PEP alerts to verify timely review and application of additional measures.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 8",
            },
        ],
    },
    {
        "requirement_id": "REQ-003",
        "requirement_title": "Correspondent Banking",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-003-OB-001",
                "obligation_text": "Before entering into a cross-border correspondent relationship, apply appropriate risk mitigation measures as prescribed by the Implementing Regulation, and satisfy themselves that the respondent institution does not permit their account to be used by a shell bank.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Policy", "Procedure", "Record", "Contract"],
                "test_method": "Auditor selects a sample of new correspondent relationships established in the review period and confirms documented due diligence and a written shell bank assurance from the respondent exist prior to relationship commencement.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 9(1)",
            },
            {
                "obligation_id": "REQ-003-OB-002",
                "obligation_text": "Shall not enter into or continue a correspondent relationship with a shell bank or a respondent institution that permits its account to be used by a shell bank.",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record", "Log"],
                "test_method": "Auditor reviews the correspondent banking policy for an explicit shell bank prohibition and confirms periodic reviews of respondent banks are conducted and documented.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 9(2)",
            },
        ],
    },
    {
        "requirement_id": "REQ-004",
        "requirement_title": "Wire Transfers",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-004-OB-001",
                "obligation_text": "Financial institutions providing wire transfer activities shall obtain information on the originator and beneficiary and ensure that such information is kept with the wire transfer or related message throughout the payment chain.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["System Configuration", "Log", "Procedure"],
                "test_method": "Auditor selects a sample of outgoing wire transfers and confirms all required originator and beneficiary fields are populated and transmitted with the payment message.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 10(1)",
            },
            {
                "obligation_id": "REQ-004-OB-002",
                "obligation_text": "A financial institution that is unable to obtain required originator or beneficiary information shall not permit the execution of the wire transfer.",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["System Configuration", "Log", "Procedure"],
                "test_method": "Auditor confirms a system-level block/reject rule exists for transfers with missing mandatory fields; reviews rejection logs to verify it is operating.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 10(1)",
            },
            {
                "obligation_id": "REQ-004-OB-003",
                "obligation_text": "Record all originator and beneficiary information and keep the records, documents, data, and files in accordance with Article 12.",
                "obligation_type": "Documentation",
                "criticality": "Medium",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Record", "System Configuration"],
                "test_method": "Auditor confirms payment records include full originator and beneficiary data and are stored in compliance with the 10-year retention requirement under Article 12.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 10(2)",
            },
            {
                "obligation_id": "REQ-004-OB-004",
                "obligation_text": "Comply with all measures on wire transfers as set out in the Implementing Regulation.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Procedure"],
                "test_method": "Auditor performs a gap analysis between the wire transfer policy/procedures and the current Implementing Regulation requirements to confirm full alignment.",
                "clarity_score": 3,
                "needs_manual_review": True,
                "source_reference": "Article 10(3)",
            },
        ],
    },
    {
        "requirement_id": "REQ-005",
        "requirement_title": "High-Risk Countries",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-005-OB-001",
                "obligation_text": "Apply enhanced due diligence measures proportionate to the risks involving business relationships and transactions with a person from a country identified as high risk by the FI or the Anti-Money Laundering Permanent Committee.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Procedure", "Record", "Approval"],
                "test_method": "Auditor reviews the country risk list and selects a sample of customers from high-risk countries to confirm EDD was applied and documented.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 11(1)",
            },
            {
                "obligation_id": "REQ-005-OB-002",
                "obligation_text": "Apply the countermeasures prescribed by the Anti-Money Laundering Permanent Committee with respect to high risk countries.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Procedure", "Record"],
                "test_method": "Auditor reviews the mechanism for receiving Committee directives and confirms countermeasures are implemented within the required timeframe after official publication.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 11(2)",
            },
        ],
    },
    {
        "requirement_id": "REQ-006",
        "requirement_title": "Record Keeping",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-006-OB-001",
                "obligation_text": "Keep all records and documents for domestic or international financial transactions as well as commercial and monetary transactions for a period of no less than ten years from the date of concluding the transaction or closure of account.",
                "obligation_type": "Documentation",
                "criticality": "Medium",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Policy", "System Configuration", "Record"],
                "test_method": "Auditor reviews the records retention policy for a 10-year minimum requirement and samples archived transaction records to confirm they are retrievable and within retention period.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 12(1)",
            },
            {
                "obligation_id": "REQ-006-OB-002",
                "obligation_text": "Keep all records obtained through due diligence measures, account files and business correspondences and copies of personal identification documents, including analysis results, for at least ten years after the business relationship has ended or after an occasional transaction.",
                "obligation_type": "Documentation",
                "criticality": "Medium",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Policy", "System Configuration", "Record"],
                "test_method": "Auditor samples closed customer files and confirms CDD records, ID copies, and correspondence are retained and accessible for the required 10-year post-relationship period.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 12(2)",
            },
            {
                "obligation_id": "REQ-006-OB-003",
                "obligation_text": "Records shall be sufficient to permit reconstruction of transactions and shall be maintained in a manner so that they can be readily made available to competent authorities upon request.",
                "obligation_type": "Documentation",
                "criticality": "Medium",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Policy", "Record", "Procedure"],
                "test_method": "Auditor conducts a simulated authority request and measures the time to retrieve complete, legible transaction records; confirms data format and integrity standards are in place.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 12(4)",
            },
            {
                "obligation_id": "REQ-006-OB-004",
                "obligation_text": "Keep records of examined transactions for a period of ten years and make them available to competent authorities upon request.",
                "obligation_type": "Documentation",
                "criticality": "Medium",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Policy", "Record", "System Configuration"],
                "test_method": "Auditor confirms transaction monitoring examination records and investigation notes are archived for a minimum of 10 years and can be retrieved upon a simulated authority request.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 13(4)",
            },
        ],
    },
    {
        "requirement_id": "REQ-007",
        "requirement_title": "Transaction Monitoring",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-007-OB-001",
                "obligation_text": "Monitor and scrutinize transactions, document and data on an ongoing basis to ensure consistency with the knowledge of the customer, the customer's commercial activities and risk profile, and where necessary the customer's source of funds.",
                "obligation_type": "Detective",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["System Configuration", "Log", "Procedure", "Report"],
                "test_method": "Auditor reviews the transaction monitoring system configuration for active scenario-based rules and confirms a sample of generated alerts were reviewed and documented within the defined timeframe.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 13(1)",
            },
            {
                "obligation_id": "REQ-007-OB-002",
                "obligation_text": "Examine any complex and unusual large transaction, and any unusual pattern of transactions that has no clear economic or legal objective.",
                "obligation_type": "Detective",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Procedure", "Log", "Record"],
                "test_method": "Auditor reviews the thresholds and detection scenarios for complex/unusual transactions and confirms a sample of flagged transactions have documented examination outcomes.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 13(2)",
            },
            {
                "obligation_id": "REQ-007-OB-003",
                "obligation_text": "Where the risks of money laundering are higher, perform enhanced due diligence and increase the level and nature of monitoring of the relevant business relationship to determine whether the transaction is unusual or suspicious.",
                "obligation_type": "Detective",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Procedure", "Record", "Log"],
                "test_method": "Auditor selects a sample of high-risk customers and confirms enhanced monitoring parameters (lower thresholds, higher frequency reviews) are applied and documented.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 13(3)",
            },
        ],
    },
    {
        "requirement_id": "REQ-008",
        "requirement_title": "Internal Controls & Governance",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-008-OB-001",
                "obligation_text": "Have in place and effectively implement internal policies, procedures and controls against money laundering aimed at managing and mitigating any identified risks. Policies must be proportionate to nature and size of business and approved by senior management.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Approval", "Procedure"],
                "test_method": "Auditor reviews the AML policy framework document for senior management approval signature and date, and confirms effective implementation through testing of a sample of controls referenced in the policy.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 14(1)(A)",
            },
            {
                "obligation_id": "REQ-008-OB-002",
                "obligation_text": "Review and enhance internal policies, procedures and controls as needed.",
                "obligation_type": "Detective",
                "criticality": "Medium",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Policy", "Record", "Approval"],
                "test_method": "Auditor confirms a scheduled periodic review cycle exists for the AML framework and that trigger-based reviews are conducted upon material regulatory changes; reviews evidence of the last completed review.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 14(1)(A)",
            },
            {
                "obligation_id": "REQ-008-OB-003",
                "obligation_text": "Apply its internal policies, procedures and controls to all of its branches and majority-owned subsidiaries.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record", "Report"],
                "test_method": "Auditor reviews the group AML policy for explicit applicability to branches and majority-owned subsidiaries and confirms implementation attestations have been received from each entity.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 14(1)(B)",
            },
        ],
    },
    {
        "requirement_id": "REQ-009",
        "requirement_title": "STR Reporting & FIU Cooperation",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-009-OB-001",
                "obligation_text": "Promptly and directly report to the General Directorate of Financial Intelligence any transaction suspected or having reasonable grounds to suspect involves proceeds of crime or money laundering, including attempts. Provide a detailed report including all available data and information.",
                "obligation_type": "Reporting",
                "criticality": "High",
                "execution_category": "One_Off_Reporting",
                "evidence_expected": ["Procedure", "Report", "Record"],
                "test_method": "Auditor reviews the STR filing procedure and samples submitted STRs to confirm they include all required data fields and were filed promptly after internal suspicion was raised.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 15(1)",
            },
            {
                "obligation_id": "REQ-009-OB-002",
                "obligation_text": "Promptly and fully respond to requests from the General Directorate of Financial Intelligence for additional information.",
                "obligation_type": "Governance",
                "criticality": "Medium",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Procedure", "Record", "Log"],
                "test_method": "Auditor reviews the procedure for handling FIU information requests and samples responses to confirm they were complete and submitted promptly.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 15(2)",
            },
            {
                "obligation_id": "REQ-009-OB-003",
                "obligation_text": "Prohibit directors, management, and employees from disclosing to a customer or any other person the fact that a report under this Law or related information will be, is being or has been submitted to the Directorate, or that a criminal investigation is being or has been carried out.",
                "obligation_type": "Reporting",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Procedure", "Record"],
                "test_method": "Auditor confirms the tipping-off prohibition is explicitly stated in the employee code of conduct and AML policy, and that annual training records show staff completion of tipping-off awareness training.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 16(1)",
            },
            {
                "obligation_id": "REQ-009-OB-004",
                "obligation_text": "Provide any additional information requested by the General Directorate of Financial Intelligence promptly, including through the supervisory authority when the request does not relate to a submitted report.",
                "obligation_type": "Reporting",
                "criticality": "Medium",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Procedure", "Record"],
                "test_method": "Auditor reviews the procedure for receiving and actioning information requests from the FIU or supervisory authority and confirms response time is tracked and within defined SLA.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 18(1)",
            },
        ],
    },
]

# ── Helpers ───────────────────────────────────────────────────────────────────
EXEC_PRIORITY = ["Ongoing_Control", "One_Time_Implementation",
                 "One_Off_Reporting", "Governance_Approval", "Informational_No_Action"]
CRIT_PRIORITY = ["High", "Medium", "Low"]

def dominant(values, priority):
    present = {v for v in values if v}
    for p in priority:
        if p in present:
            return p
    return priority[-1]

# ── Build rows ─────────────────────────────────────────────────────────────────
def build_rows():
    rows = []
    is_first = True
    for req in REQUIREMENTS:
        req_id    = req["requirement_id"]
        req_title = req["requirement_title"]
        obs       = req["normalized_obligations"]

        exec_cat     = dominant([o["execution_category"] for o in obs], EXEC_PRIORITY)
        criticality  = dominant([o["criticality"] for o in obs], CRIT_PRIORITY)
        ob_types     = ", ".join(dict.fromkeys(o["obligation_type"] for o in obs))

        stage1 = {
            "requirement_id":    req_id,
            "requirement_title": req_title,
            "obligations": [
                {
                    "obligation_id":    o["obligation_id"],
                    "obligation_text":  o["obligation_text"],
                    "source_reference": o["source_reference"],
                }
                for o in obs
            ],
        }

        stage2 = {
            "requirement_id":        req_id,
            "requirement_title":     req_title,
            "normalized_obligations": obs,
        }

        stage3 = {
            "requirement_id":    req_id,
            "requirement_title": req_title,
            "obligations": [
                {
                    "obligation_id":      o["obligation_id"],
                    "obligation_text":    o["obligation_text"],
                    "execution_category": o["execution_category"],
                    "control":            None,
                }
                for o in obs
            ],
        }

        analysis_json = {
            "requirement_id":    req_id,
            "requirement_title": req_title,
            "obligations":       obs,
            "controls":          [],
        }

        rows.append({
            "regulation_id":      REGULATION_ID,
            "requirement_id":     req_id,
            "requirement_title":  req_title,
            "execution_category": exec_cat,
            "criticality":        criticality,
            "obligation_type":    ob_types,
            "stage1_json":        json.dumps(stage1, ensure_ascii=False),
            "stage2_json":        json.dumps(stage2, ensure_ascii=False),
            "stage3_json":        json.dumps(stage3, ensure_ascii=False),
            "stage4_md":          None,
            "analysis_json":      json.dumps(analysis_json, ensure_ascii=False),
        })
        is_first = False
    return rows

# ── Main ──────────────────────────────────────────────────────────────────────
rows = build_rows()

print(f"Regulation {REGULATION_ID} — {len(rows)} requirement groups")
print()
total_obs = 0
for r in rows:
    s2 = json.loads(r["stage2_json"])
    n  = len(s2["normalized_obligations"])
    total_obs += n
    print(f"  {r['requirement_id']} | {r['requirement_title']:<35} | {r['execution_category']:<30} | {r['criticality']:<6} | {n} obligations | {r['obligation_type']}")
print(f"\n  Total obligations: {total_obs}")

if not DRY_RUN:
    # Delete existing v2 rows first
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM compliance_analysis WHERE regulation_id=? AND schema_version='v2'",
            [REGULATION_ID]
        )
        deleted = cursor.rowcount
        conn.commit()
    print(f"\n  Deleted {deleted} existing v2 rows")

    repo.store_analysis(rows, version_id=None)
    print(f"  Inserted {len(rows)} new rows")
else:
    print("\n  [DRY RUN] set DRY_RUN=False to apply")
