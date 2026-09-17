"""
Generates prod_req_mapping_27880.sql
Inserts 31 rows into sama_requirement_mapping for regulation_id=27880.

Matching was done semantically against:
  - COMPLIANCE_REQUIREMENT (110 rows, prod snapshot)

Results: 10 fully_matched | 7 partially_matched | 14 new
"""

REGULATION_ID = 27880

def esc(s: str) -> str:
    return s.replace("'", "''")

MAPPINGS = [
    # ── REQ-001: Risk Assessment ───────────────────────────────────────────────
    {
        "requirement_id": "REQ-001",
        "obligation_id":  "REQ-001-OB-001",
        "obligation_text": "Identify, assess, and document money laundering risks and keep the assessment up to date, taking into account a wide range of risk factors, including those relating to customers, countries or geographic areas, products, services, transactions and delivery channels.",
        "match_status":   "fully_matched",
        "matched_id":     16,
        "explanation":    "Directly covered by existing requirement on AML/CFT/PF business risk assessments (Article 5 risk factors align exactly).",
    },
    {
        "requirement_id": "REQ-001",
        "obligation_id":  "REQ-001-OB-002",
        "obligation_text": "Assess, prior to their use, the risks associated with new products, business practices and technologies.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement specifically covers pre-launch AML risk assessment for new products, business practices, or technologies.",
    },
    {
        "requirement_id": "REQ-001",
        "obligation_id":  "REQ-001-OB-003",
        "obligation_text": "Provide risk assessment reports to the supervisory authorities upon request.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement covers formal provision of AML risk assessment reports to supervisory authorities.",
    },

    # ── REQ-002: Customer Due Diligence ────────────────────────────────────────
    {
        "requirement_id": "REQ-002",
        "obligation_id":  "REQ-002-OB-001",
        "obligation_text": "Shall not keep or open anonymous accounts or accounts in obviously fictitious names, or numbered accounts.",
        "match_status":   "partially_matched",
        "matched_id":     1,
        "explanation":    "CDD requirement (id=1) implicitly prevents anonymous accounts through identity verification, but does not contain an explicit prohibition on anonymous or fictitious-name accounts.",
    },
    {
        "requirement_id": "REQ-002",
        "obligation_id":  "REQ-002-OB-002",
        "obligation_text": "Apply due diligence measures to customers. The Implementing Regulation shall set forth the instances and types of measures.",
        "match_status":   "fully_matched",
        "matched_id":     1,
        "explanation":    "Directly and completely covered by existing AML Customer Due Diligence requirement (id=1).",
    },
    {
        "requirement_id": "REQ-002",
        "obligation_id":  "REQ-002-OB-003",
        "obligation_text": "Determine the extent of due diligence measures based on the risks related to a customer or business relationship.",
        "match_status":   "partially_matched",
        "matched_id":     1,
        "explanation":    "AML CDD requirement (id=1) covers CDD broadly but does not specifically mandate risk-based scoping or tiering of CDD measures.",
    },
    {
        "requirement_id": "REQ-002",
        "obligation_id":  "REQ-002-OB-004",
        "obligation_text": "Where a higher risk of money laundering was identified, apply enhanced due diligence measures.",
        "match_status":   "fully_matched",
        "matched_id":     2,
        "explanation":    "Directly covered by existing Enhanced Due Diligence requirement (id=2) which mandates EDD for high-risk customers.",
    },
    {
        "requirement_id": "REQ-002",
        "obligation_id":  "REQ-002-OB-005",
        "obligation_text": "Use appropriate systems to determine whether a customer or beneficial owner is or has become a politically exposed person (PEP) and if so, apply additional measures as prescribed by the Implementing Regulation.",
        "match_status":   "fully_matched",
        "matched_id":     2,
        "explanation":    "PEP screening and additional measures are explicitly covered by the Enhanced Due Diligence requirement (id=2).",
    },

    # ── REQ-003: Correspondent Banking ────────────────────────────────────────
    {
        "requirement_id": "REQ-003",
        "obligation_id":  "REQ-003-OB-001",
        "obligation_text": "Before entering into a cross-border correspondent relationship, apply appropriate risk mitigation measures as prescribed by the Implementing Regulation, and satisfy themselves that the respondent institution does not permit their account to be used by a shell bank.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement covers pre-entry due diligence for cross-border correspondent banking relationships.",
    },
    {
        "requirement_id": "REQ-003",
        "obligation_id":  "REQ-003-OB-002",
        "obligation_text": "Shall not enter into or continue a correspondent relationship with a shell bank or a respondent institution that permits its account to be used by a shell bank.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement prohibits correspondent relationships with shell banks.",
    },

    # ── REQ-004: Wire Transfers ────────────────────────────────────────────────
    {
        "requirement_id": "REQ-004",
        "obligation_id":  "REQ-004-OB-001",
        "obligation_text": "Financial institutions providing wire transfer activities shall obtain information on the originator and beneficiary and ensure that such information is kept with the wire transfer or related message throughout the payment chain.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement addresses wire transfer originator and beneficiary data requirements or payment chain information integrity.",
    },
    {
        "requirement_id": "REQ-004",
        "obligation_id":  "REQ-004-OB-002",
        "obligation_text": "A financial institution that is unable to obtain required originator or beneficiary information shall not permit the execution of the wire transfer.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement mandates blocking wire transfers when mandatory originator or beneficiary information is missing.",
    },
    {
        "requirement_id": "REQ-004",
        "obligation_id":  "REQ-004-OB-003",
        "obligation_text": "Record all originator and beneficiary information and keep the records, documents, data, and files in accordance with Article 12.",
        "match_status":   "partially_matched",
        "matched_id":     9,
        "explanation":    "Data Retention Policy (id=9) covers record retention broadly but does not specifically address wire transfer originator and beneficiary information recordkeeping.",
    },
    {
        "requirement_id": "REQ-004",
        "obligation_id":  "REQ-004-OB-004",
        "obligation_text": "Comply with all measures on wire transfers as set out in the Implementing Regulation.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement specifically mandates full compliance with wire transfer provisions of the AML Implementing Regulation.",
    },

    # ── REQ-005: High-Risk Countries ──────────────────────────────────────────
    {
        "requirement_id": "REQ-005",
        "obligation_id":  "REQ-005-OB-001",
        "obligation_text": "Apply enhanced due diligence measures proportionate to the risks involving business relationships and transactions with a person from a country identified as high risk by the FI or the Anti-Money Laundering Permanent Committee.",
        "match_status":   "fully_matched",
        "matched_id":     2,
        "explanation":    "Enhanced Due Diligence requirement (id=2) explicitly covers customers from high-risk jurisdictions, directly matching this obligation.",
    },
    {
        "requirement_id": "REQ-005",
        "obligation_id":  "REQ-005-OB-002",
        "obligation_text": "Apply the countermeasures prescribed by the Anti-Money Laundering Permanent Committee with respect to high risk countries.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement covers implementation of AML Permanent Committee countermeasures for specific high-risk countries.",
    },

    # ── REQ-006: Record Keeping ───────────────────────────────────────────────
    {
        "requirement_id": "REQ-006",
        "obligation_id":  "REQ-006-OB-001",
        "obligation_text": "Keep all records and documents for domestic or international financial transactions as well as commercial and monetary transactions for a period of no less than ten years from the date of concluding the transaction or closure of account.",
        "match_status":   "fully_matched",
        "matched_id":     9,
        "explanation":    "Directly and completely covered by existing Data Retention Policy (id=9) requiring 10-year minimum retention for transaction records.",
    },
    {
        "requirement_id": "REQ-006",
        "obligation_id":  "REQ-006-OB-002",
        "obligation_text": "Keep all records obtained through due diligence measures, account files and business correspondences and copies of personal identification documents, including analysis results, for at least ten years after the business relationship has ended or after an occasional transaction.",
        "match_status":   "fully_matched",
        "matched_id":     9,
        "explanation":    "Directly covered by Data Retention Policy (id=9) — CDD records, account files, and ID documents are within its scope, and the 10-year post-relationship retention period matches.",
    },
    {
        "requirement_id": "REQ-006",
        "obligation_id":  "REQ-006-OB-003",
        "obligation_text": "Records shall be sufficient to permit reconstruction of transactions and shall be maintained in a manner so that they can be readily made available to competent authorities upon request.",
        "match_status":   "partially_matched",
        "matched_id":     9,
        "explanation":    "Data Retention Policy (id=9) covers retention duration but does not explicitly require reconstruction-sufficiency of records or formalise the process for making them available to competent authorities.",
    },
    {
        "requirement_id": "REQ-006",
        "obligation_id":  "REQ-006-OB-004",
        "obligation_text": "Keep records of examined transactions for a period of ten years and make them available to competent authorities upon request.",
        "match_status":   "fully_matched",
        "matched_id":     9,
        "explanation":    "Directly covered by Data Retention Policy (id=9) — 10-year minimum retention and authority access both explicitly covered.",
    },

    # ── REQ-007: Transaction Monitoring ──────────────────────────────────────
    {
        "requirement_id": "REQ-007",
        "obligation_id":  "REQ-007-OB-001",
        "obligation_text": "Monitor and scrutinize transactions, document and data on an ongoing basis to ensure consistency with the knowledge of the customer, the customer's commercial activities and risk profile, and where necessary the customer's source of funds.",
        "match_status":   "partially_matched",
        "matched_id":     5,
        "explanation":    "Ongoing Customer Monitoring (id=5) covers periodic customer reviews but does not specifically address transaction-to-profile consistency monitoring on a continuous basis.",
    },
    {
        "requirement_id": "REQ-007",
        "obligation_id":  "REQ-007-OB-002",
        "obligation_text": "Examine any complex and unusual large transaction, and any unusual pattern of transactions that has no clear economic or legal objective.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement specifically mandates examination of complex, unusual, or large transactions or unusual transaction patterns.",
    },
    {
        "requirement_id": "REQ-007",
        "obligation_id":  "REQ-007-OB-003",
        "obligation_text": "Where the risks of money laundering are higher, perform enhanced due diligence and increase the level and nature of monitoring of the relevant business relationship to determine whether the transaction is unusual or suspicious.",
        "match_status":   "partially_matched",
        "matched_id":     5,
        "explanation":    "Ongoing Customer Monitoring (id=5) partially covers this but does not address risk-triggered escalation of monitoring intensity or linkage to EDD.",
    },

    # ── REQ-008: Internal Controls & Governance ───────────────────────────────
    {
        "requirement_id": "REQ-008",
        "obligation_id":  "REQ-008-OB-001",
        "obligation_text": "Have in place and effectively implement internal policies, procedures and controls against money laundering aimed at managing and mitigating any identified risks. Policies must be proportionate to nature and size of business and approved by senior management.",
        "match_status":   "partially_matched",
        "matched_id":     76,
        "explanation":    "CFT Guidance Paper implementation (id=76) partially covers AML internal controls but does not mandate a comprehensive senior-management-approved AML policy framework proportionate to business size.",
    },
    {
        "requirement_id": "REQ-008",
        "obligation_id":  "REQ-008-OB-002",
        "obligation_text": "Review and enhance internal policies, procedures and controls as needed.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement addresses periodic or trigger-based review and enhancement of AML internal policies and controls.",
    },
    {
        "requirement_id": "REQ-008",
        "obligation_id":  "REQ-008-OB-003",
        "obligation_text": "Apply its internal policies, procedures and controls to all of its branches and majority-owned subsidiaries.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement mandates group-wide application of AML policies to all branches and majority-owned subsidiaries.",
    },

    # ── REQ-009: STR Reporting & FIU Cooperation ──────────────────────────────
    {
        "requirement_id": "REQ-009",
        "obligation_id":  "REQ-009-OB-001",
        "obligation_text": "Promptly and directly report to the General Directorate of Financial Intelligence any transaction suspected or having reasonable grounds to suspect involves proceeds of crime or money laundering, including attempts. Provide a detailed report including all available data and information.",
        "match_status":   "fully_matched",
        "matched_id":     3,
        "explanation":    "Directly and completely covered by existing Suspicious Transaction Reporting requirement (id=3) mandating FIU reporting within 3 business days.",
    },
    {
        "requirement_id": "REQ-009",
        "obligation_id":  "REQ-009-OB-002",
        "obligation_text": "Promptly and fully respond to requests from the General Directorate of Financial Intelligence for additional information.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement covers responding to FIU requests for additional information related to submitted or pending reports.",
    },
    {
        "requirement_id": "REQ-009",
        "obligation_id":  "REQ-009-OB-003",
        "obligation_text": "Prohibit directors, management, and employees from disclosing to a customer or any other person the fact that a report under this Law or related information will be, is being or has been submitted to the Directorate, or that a criminal investigation is being or has been carried out.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement establishes a tipping-off prohibition for STR disclosures.",
    },
    {
        "requirement_id": "REQ-009",
        "obligation_id":  "REQ-009-OB-004",
        "obligation_text": "Provide any additional information requested by the General Directorate of Financial Intelligence promptly, including through the supervisory authority when the request does not relate to a submitted report.",
        "match_status":   "new",
        "matched_id":     None,
        "explanation":    "No existing requirement covers FIU information requests that are routed through the supervisory authority or that are unrelated to a filed STR.",
    },

    # ── REQ-030: Record Keeping (Article 13(4) — standalone row) ─────────────
    {
        "requirement_id": "REQ-030",
        "obligation_id":  "REQ-030-OB-001",
        "obligation_text": "Keep records of examined transactions for a period of ten years and make them available to competent authorities upon request.",
        "match_status":   "fully_matched",
        "matched_id":     9,
        "explanation":    "Directly covered by Data Retention Policy (id=9) — 10-year minimum retention for transaction monitoring records and authority access both explicitly covered.",
    },
]


def main():
    lines = ["BEGIN TRANSACTION;", ""]
    lines.append(f"-- sama_requirement_mapping INSERT for regulation_id={REGULATION_ID}")
    lines.append(f"-- 31 obligations | 10 fully_matched | 7 partially_matched | 14 new")
    lines.append(f"-- Generated from semantic analysis against COMPLIANCE_REQUIREMENT (110 rows, prod snapshot)")
    lines.append("")

    fully   = sum(1 for m in MAPPINGS if m["match_status"] == "fully_matched")
    partial = sum(1 for m in MAPPINGS if m["match_status"] == "partially_matched")
    new     = sum(1 for m in MAPPINGS if m["match_status"] == "new")

    for i, m in enumerate(MAPPINGS, start=1):
        mid  = str(m["matched_id"]) if m["matched_id"] is not None else "NULL"
        lines.append(f"-- {i}. {m['obligation_id']} [{m['match_status']}{'→ id=' + str(m['matched_id']) if m['matched_id'] else ''}]")
        lines.append(
            f"INSERT INTO sama_requirement_mapping "
            f"(regulation_id, extracted_requirement_text, "
            f"matched_requirement_id, match_status, match_explanation, version_id) VALUES "
            f"({REGULATION_ID}, N'{esc(m['obligation_text'])}', "
            f"{mid}, N'{m['match_status']}', N'{esc(m['explanation'])}', NULL);"
        )
        lines.append("")

    lines.append("")
    lines.append("-- ── Verification ─────────────────────────────────────────────────────────")
    lines.append("SELECT match_status, COUNT(*) AS cnt")
    lines.append("FROM sama_requirement_mapping")
    lines.append(f"WHERE regulation_id = {REGULATION_ID}")
    lines.append("GROUP BY match_status")
    lines.append("ORDER BY match_status;")
    lines.append("")
    lines.append(f"-- Expected: fully_matched={fully}, partially_matched={partial}, new={new}")
    lines.append("")
    lines.append("COMMIT;")
    lines.append("-- On error: ROLLBACK;")

    sql = "\n".join(lines)
    out = "prod_req_mapping_27880.sql"
    with open(out, "w", encoding="utf-8") as f:
        f.write(sql)
    print(f"Written {len(MAPPINGS)} INSERTs to {out} ({len(sql):,} bytes)")
    print(f"  fully_matched={fully}, partially_matched={partial}, new={new}")


if __name__ == "__main__":
    main()
