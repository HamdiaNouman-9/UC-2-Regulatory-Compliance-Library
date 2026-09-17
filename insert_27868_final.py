"""
Final insert for regulation 27868 — replaces the 33 single-obligation migrated rows
with 9 properly grouped requirement rows (35 obligations total).
Deletes existing v2 rows first, then inserts the new structured data.
"""

import json, os
from dotenv import load_dotenv
load_dotenv(override=True)
from storage.mssql_repo import MSSQLRepository

REGULATION_ID = 27868
DRY_RUN = False

repo = MSSQLRepository({
    "server":   os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "username": os.getenv("MSSQL_USERNAME"),
    "password": os.getenv("MSSQL_PASSWORD"),
    "driver":   os.getenv("MSSQL_DRIVER"),
})

REQUIREMENTS = [
    {
        "requirement_id": "REQ-001",
        "requirement_title": "Licensing & Use of Banking Terminology",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-001-OB-001",
                "obligation_text": "No person, natural or juristic, shall carry on basically any banking business in the Kingdom unless licensed in accordance with the provisions of this Law.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Approval"],
                "test_method": "Auditor verifies the bank holds a valid, current banking license issued by the Minister of Finance and confirms no banking activities are conducted outside the scope of that license.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 2",
            },
            {
                "obligation_id": "REQ-001-OB-002",
                "obligation_text": "Any person not authorized to carry on banking business in the Kingdom shall not use the word 'Bank' or its synonyms in any language on papers, printed matter, commercial address, name, or advertisements.",
                "obligation_type": "Preventive",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record"],
                "test_method": "Auditor reviews all public-facing materials, marketing collateral, and digital presence to confirm the term 'Bank' and synonyms are used only by licensed entities, and that an approval process exists for new naming or marketing.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 5",
            },
            {
                "obligation_id": "REQ-001-OB-003",
                "obligation_text": "A national bank license shall require: the bank to be a Saudi joint-stock company; paid-up capital of not less than SAR 2.5 million payable in cash; founders and board members of good reputation; and memorandum and articles of association approved by the Minister of Finance and National Economy.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "One_Time_Implementation",
                "evidence_expected": ["Approval", "Record", "Contract"],
                "test_method": "Auditor confirms the bank's legal structure is a Saudi joint-stock company, verifies paid-up capital meets the minimum threshold, and reviews board member fit-and-proper declarations and MoF approval of the articles of association.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 3",
            },
            {
                "obligation_id": "REQ-001-OB-004",
                "obligation_text": "Grandfathered banks operating under pre-Law licenses shall comply with SAMA requests for documents and information, and shall comply with Article 3 provisions within any period fixed by SAMA with Council of Ministers approval.",
                "obligation_type": "Governance",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Approval"],
                "test_method": "Auditor confirms a mechanism exists to receive and respond to SAMA information requests promptly, and that any Article 3 compliance deadlines set by SAMA have been met.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 4",
            },
        ],
    },
    {
        "requirement_id": "REQ-002",
        "requirement_title": "Capital Adequacy & Deposit Limits",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-002-OB-001",
                "obligation_text": "The bank's deposit liabilities shall not exceed fifteen times the sum of its reserves and paid-up or invested capital.",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "System Configuration", "Log"],
                "test_method": "Auditor reviews daily or weekly monitoring reports of deposit liabilities versus capital and reserves, and confirms automated alerts are configured to flag breaches of the 15x limit.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 6",
            },
            {
                "obligation_id": "REQ-002-OB-002",
                "obligation_text": "If deposit liabilities exceed fifteen times capital and reserves, the bank must within one month either increase its capital and reserves to the prescribed limit or deposit fifty percent of the excess with SAMA.",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Approval", "Report"],
                "test_method": "Auditor confirms a documented contingency plan exists for capital raising or excess deposit placement with SAMA, and verifies escalation procedures to senior management and the board upon breach.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 6",
            },
            {
                "obligation_id": "REQ-002-OB-003",
                "obligation_text": "The bank shall maintain with SAMA at all times a statutory deposit of not less than 15% of its deposit liabilities (subject to SAMA variation between 10% and 17.5%, or beyond those limits with Ministerial approval).",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Log", "System Configuration"],
                "test_method": "Auditor reviews daily calculations of the required statutory deposit and reconciles with SAMA records to confirm the ratio is maintained at all times within the applicable range.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 7, Paragraph 1",
            },
            {
                "obligation_id": "REQ-002-OB-004",
                "obligation_text": "In addition to the statutory deposit, the bank shall maintain a liquidity reserve of not less than 15% of its deposit liabilities (subject to SAMA increase up to 20%). The reserve must be in cash, gold, or assets convertible to cash within 30 days.",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Log", "System Configuration"],
                "test_method": "Auditor reviews daily liquidity reserve monitoring reports, confirms the composition meets the cash/gold/30-day liquid asset requirement, and verifies the reserve value is at or above the required percentage.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 7, Paragraph 2",
            },
        ],
    },
    {
        "requirement_id": "REQ-003",
        "requirement_title": "Credit & Lending Restrictions",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-003-OB-001",
                "obligation_text": "The bank shall not grant a loan, credit facility, or financial guarantee to any single natural or juristic person for amounts aggregating more than 25% of the bank's reserves and paid-up or invested capital (subject to SAMA increase up to 50%).",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "System Configuration", "Log"],
                "test_method": "Auditor reviews the single-obligor limit monitoring system and selects a sample of large credit approvals to confirm pre-approval exposure checks against the 25% (or approved higher) limit were performed.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 8, Paragraph 1",
            },
            {
                "obligation_id": "REQ-003-OB-002",
                "obligation_text": "The bank shall not grant loans, credit facilities, guarantees, or incur financial liabilities secured by its own shares.",
                "obligation_type": "Preventive",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record", "Procedure"],
                "test_method": "Auditor confirms the credit policy explicitly prohibits the bank's own shares as collateral and tests a sample of secured facilities to verify collateral types do not include the bank's own shares.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 9, Paragraph 1",
            },
            {
                "obligation_id": "REQ-003-OB-003",
                "obligation_text": "The bank shall not grant unsecured loans, credit facilities, guarantees, or incur financial liabilities to: members of its Board or Auditors; establishments where a Board member or Auditor is a partner, manager, or has a direct financial interest; or persons or establishments where a Board member or Auditor is a guarantor.",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record", "Procedure", "Approval"],
                "test_method": "Auditor reviews related-party transaction policy and the register of Board and Auditor interest disclosures; tests a sample of credit facilities to confirm none were granted unsecured to restricted parties.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 9, Paragraph 2",
            },
            {
                "obligation_id": "REQ-003-OB-004",
                "obligation_text": "The bank shall not grant unsecured loans, credit facilities, guarantees, or incur financial liabilities to any of its officials or employees for amounts exceeding four months' salary of the concerned person.",
                "obligation_type": "Preventive",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record", "System Configuration"],
                "test_method": "Auditor reviews the staff loan policy and confirms salary-based limit checks are automated in the loan origination system; samples staff facilities to verify none exceed four months' salary unsecured.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 9, Paragraph 3",
            },
        ],
    },
    {
        "requirement_id": "REQ-004",
        "requirement_title": "Prohibited Activities & Investment Restrictions",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-004-OB-001",
                "obligation_text": "The bank shall not engage in wholesale or retail trade, including import or export, whether for its own account or on commission.",
                "obligation_type": "Preventive",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record"],
                "test_method": "Auditor reviews the policy defining permitted versus prohibited activities and samples transaction records to confirm no wholesale, retail, import, or export trading activity exists.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 10, Paragraph 1",
            },
            {
                "obligation_id": "REQ-004-OB-002",
                "obligation_text": "The bank shall not have a direct interest as shareholder, partner, or owner in any commercial, industrial, agricultural, or other undertaking, except within the limits of Article 10(4) or when acquired in satisfaction of a debt, in which case the interest must be disposed of within 2 years or a longer period agreed with SAMA.",
                "obligation_type": "Preventive",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Approval", "Log"],
                "test_method": "Auditor reviews the inventory of all direct interests in non-financial entities and confirms any debt-satisfaction interests older than 2 years without an approved SAMA extension have been disposed of.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 10, Paragraph 2",
            },
            {
                "obligation_id": "REQ-004-OB-003",
                "obligation_text": "The bank shall not purchase stocks or shares of any bank operating in the Kingdom without SAMA's prior approval.",
                "obligation_type": "Preventive",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Approval", "Record"],
                "test_method": "Auditor reviews the pre-approval process for investments in KSA banks and confirms no such investment was made without documented SAMA approval.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 10, Paragraph 3",
            },
            {
                "obligation_id": "REQ-004-OB-004",
                "obligation_text": "The bank shall not own stocks of any other KSA joint-stock company exceeding 10% of that company's paid-up capital, and the nominal value of such shares shall not exceed 20% of the bank's paid-up capital and reserves.",
                "obligation_type": "Preventive",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "System Configuration", "Log"],
                "test_method": "Auditor reviews the equity holdings register and confirms automated alerts are configured for breaches of both the 10% per-company and 20% aggregate limits.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 10, Paragraph 4",
            },
            {
                "obligation_id": "REQ-004-OB-005",
                "obligation_text": "The bank shall not acquire or lease real estate except as necessary for banking operations, employee housing or recreation, or in satisfaction of debts. Real estate acquired in debt satisfaction and not needed for banking or staff purposes must be liquidated within 3 years or a period approved by SAMA. In special circumstances with SAMA approval, the bank may acquire real estate not exceeding 20% of its paid-up capital and reserves.",
                "obligation_type": "Preventive",
                "criticality": "Low",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Approval", "Log"],
                "test_method": "Auditor reviews the real estate register with justification for each property; confirms debt-satisfaction properties not used for operations or staff are being liquidated within the required period or have a documented SAMA-approved extension.",
                "clarity_score": 3,
                "needs_manual_review": True,
                "source_reference": "Article 10, Paragraph 5",
            },
        ],
    },
    {
        "requirement_id": "REQ-005",
        "requirement_title": "SAMA Prior Approvals",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-005-OB-001",
                "obligation_text": "The bank shall obtain prior written approval from SAMA before: altering composition of paid-up or invested capital; amalgamation or participation with another bank or establishment; acquiring shares in a company outside KSA; ceasing banking business; or opening branches or offices in KSA (or national banks opening outside KSA).",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Approval", "Record"],
                "test_method": "Auditor reviews the checklist of activities requiring SAMA prior written approval and confirms that any such activities in the review period have documented SAMA approval obtained before execution.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 11",
            },
            {
                "obligation_id": "REQ-005-OB-002",
                "obligation_text": "The bank shall obtain prior written approval from SAMA before electing or appointing any director or manager who previously held a similar position in a liquidated bank (unless not responsible for the liquidation) or was removed from a similar post in any banking establishment.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Approval", "Record"],
                "test_method": "Auditor reviews the pre-appointment vetting process for directors and managers, confirms regulatory history checks are conducted, and verifies that any appointments of persons with prior liquidation or removal history have documented SAMA approval.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 12, Paragraph 2(a)(b)",
            },
        ],
    },
    {
        "requirement_id": "REQ-006",
        "requirement_title": "Board Governance & Director Eligibility",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-006-OB-001",
                "obligation_text": "No person shall be a member of the Board of more than one bank.",
                "obligation_type": "Governance",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Log"],
                "test_method": "Auditor reviews annual declarations of directorships from all Board members and confirms background checks for new Board appointments include verification of no concurrent bank board membership.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 12, First sentence",
            },
            {
                "obligation_id": "REQ-006-OB-002",
                "obligation_text": "Any Board member or manager adjudicated bankrupt or convicted of a moral offense shall be considered as having resigned their post. The bank must treat such events as triggering an immediate vacancy.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Policy", "Procedure"],
                "test_method": "Auditor confirms a procedure exists to monitor and act upon disqualifying events (bankruptcy or moral offense conviction) for Board members and managers, and reviews records of any such events in the review period.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 12, Paragraph 3",
            },
            {
                "obligation_id": "REQ-006-OB-003",
                "obligation_text": "The Chairman, Managing Director, Board members, Head Office Manager, and Branch Manager are each responsible within their jurisdiction for any contravention of this Law or its implementing rules.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Policy"],
                "test_method": "Auditor reviews the delegation of authority matrix to confirm clear jurisdictional responsibilities are assigned, and confirms regular compliance reporting to Board and senior management is documented.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 24",
            },
        ],
    },
    {
        "requirement_id": "REQ-007",
        "requirement_title": "Profit Distribution & Statutory Reserve",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-007-OB-001",
                "obligation_text": "Before distributing profits, the bank shall transfer not less than 25% of its net annual profits to the statutory reserve until that reserve equals at least the paid-up capital.",
                "obligation_type": "Governance",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Approval", "Report"],
                "test_method": "Auditor reviews the profit distribution policy, confirms the statutory reserve transfer calculation for the current year, and verifies Board approval of dividends was only granted after the 25% transfer was completed.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 13, Paragraph 1",
            },
            {
                "obligation_id": "REQ-007-OB-002",
                "obligation_text": "The bank shall not pay dividends or remit profits abroad until all capital expenditures including aggregate foundation expenditures and incurred losses have been completely written off.",
                "obligation_type": "Preventive",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Approval", "Report"],
                "test_method": "Auditor confirms the calculation of distributable reserves accounts for full write-off of capital and foundation expenditures and losses, and that audit confirmation of write-offs precedes any dividend payment or profit remittance.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 13, Paragraph 2",
            },
            {
                "obligation_id": "REQ-007-OB-003",
                "obligation_text": "Any action taken to declare or pay dividends in contravention of Article 13 shall be considered null and void.",
                "obligation_type": "Governance",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record"],
                "test_method": "Auditor confirms the dividend declaration process includes a compliance gate requiring sign-off that Article 13 conditions are met before any dividend resolution is approved by the Board.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 13, Paragraph 3",
            },
        ],
    },
    {
        "requirement_id": "REQ-008",
        "requirement_title": "Audit & Financial Reporting",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-008-OB-001",
                "obligation_text": "The bank shall annually appoint two auditors from the approved list registered with the Ministry of Commerce and Industry.",
                "obligation_type": "Governance",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Approval", "Log"],
                "test_method": "Auditor confirms the annual auditor appointment process, verifies both appointed auditors appear on the Ministry of Commerce and Industry approved list, and checks shareholder ratification.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 14, Paragraph 1",
            },
            {
                "obligation_id": "REQ-008-OB-002",
                "obligation_text": "The bank shall hold the General Meeting within six months of the financial year-end, at which both the auditor report and the management annual report shall be read together. The bank management shall send copies of both reports to SAMA.",
                "obligation_type": "Reporting",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Report", "Record", "Approval"],
                "test_method": "Auditor confirms General Meeting minutes showing both reports were presented within 6 months of financial year-end, and verifies SAMA submission receipts for both the auditor and management reports.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 14, Paragraph 2",
            },
            {
                "obligation_id": "REQ-008-OB-003",
                "obligation_text": "The bank shall furnish SAMA by the end of the following month with a consolidated monthly statement of its financial position, true and correct, in the form prescribed by SAMA.",
                "obligation_type": "Reporting",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Report", "Record", "Log"],
                "test_method": "Auditor reviews the automated monthly reporting process and confirms a quality assurance review is performed before submission; samples 3 months of submissions to verify timeliness and SAMA-prescribed format.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 15, First sentence",
            },
            {
                "obligation_id": "REQ-008-OB-004",
                "obligation_text": "The bank shall furnish SAMA within six months of the close of its financial year with a copy of its annual balance sheet and profit and loss account, certified by its auditors, in the form prescribed by SAMA.",
                "obligation_type": "Reporting",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Report", "Record", "Approval"],
                "test_method": "Auditor reviews the annual audit timeline and confirms the certified balance sheet and P&L were submitted to SAMA within six months of financial year-end in the prescribed format.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 15, Second sentence",
            },
        ],
    },
    {
        "requirement_id": "REQ-009",
        "requirement_title": "SAMA Compliance, Supervision & Information",
        "normalized_obligations": [
            {
                "obligation_id": "REQ-009-OB-001",
                "obligation_text": "The bank shall comply with general rules issued by SAMA (with Ministerial approval) regarding: maximum loan limits, prohibition or limitation of loan categories, terms for customer transactions, cash margins for credits or guarantees, minimum loan-to-collateral ratios, and minimum in-Kingdom assets.",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Procedure", "Record"],
                "test_method": "Auditor performs a periodic gap analysis between current SAMA general rules under Article 16 and the bank's policies and procedures, and confirms all applicable rules are implemented.",
                "clarity_score": 3,
                "needs_manual_review": True,
                "source_reference": "Article 16, Paragraph 1",
            },
            {
                "obligation_id": "REQ-009-OB-002",
                "obligation_text": "The bank shall comply with SAMA decisions defining 'deposit liabilities' and determining bank holidays and business hours.",
                "obligation_type": "Governance",
                "criticality": "Medium",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Policy"],
                "test_method": "Auditor confirms a communication channel exists to receive SAMA decisions and that internal systems and calendars are updated promptly to reflect the current definition of deposit liabilities and approved business hours.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 16, Paragraph 2",
            },
            {
                "obligation_id": "REQ-009-OB-003",
                "obligation_text": "The bank shall supply SAMA with any information it requests, within the specified time limit and in the prescribed manner.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Record", "Log"],
                "test_method": "Auditor reviews the tracking system for SAMA ad-hoc information requests and confirms all requests in the review period were responded to within the specified deadline and in the required format.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 17",
            },
            {
                "obligation_id": "REQ-009-OB-004",
                "obligation_text": "During a SAMA inspection, the bank must produce all required books, records, accounts, and documents, and furnish any information requested by SAMA staff or assigned auditors.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "One_Off_Reporting",
                "evidence_expected": ["Record", "Log", "Procedure"],
                "test_method": "Auditor confirms a regulatory inspection readiness program exists with a designated liaison officer, and reviews the most recent SAMA inspection to verify all requested documents were produced promptly.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 18",
            },
            {
                "obligation_id": "REQ-009-OB-005",
                "obligation_text": "Any person who comes into possession of information during duties related to this Law shall not disclose or make use of such information in any manner.",
                "obligation_type": "Preventive",
                "criticality": "High",
                "execution_category": "Ongoing_Control",
                "evidence_expected": ["Policy", "Record", "Procedure"],
                "test_method": "Auditor confirms all staff have signed confidentiality agreements covering information obtained in connection with this Law, and that information security and data leakage prevention controls are in place and tested.",
                "clarity_score": 5,
                "needs_manual_review": False,
                "source_reference": "Article 19",
            },
            {
                "obligation_id": "REQ-009-OB-006",
                "obligation_text": "When persistently violating this Law, the bank must submit to SAMA its reasons for the contravention and proposals to rectify the position within the stated period.",
                "obligation_type": "Governance",
                "criticality": "High",
                "execution_category": "One_Off_Reporting",
                "evidence_expected": ["Report", "Record"],
                "test_method": "Auditor confirms a procedure exists for escalating persistent regulatory violations to senior management and preparing a formal response and remediation plan for SAMA within any deadline set.",
                "clarity_score": 4,
                "needs_manual_review": False,
                "source_reference": "Article 22",
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

def build_rows():
    rows = []
    for req in REQUIREMENTS:
        req_id    = req["requirement_id"]
        req_title = req["requirement_title"]
        obs       = req["normalized_obligations"]

        exec_cat    = dominant([o["execution_category"] for o in obs], EXEC_PRIORITY)
        criticality = dominant([o["criticality"] for o in obs], CRIT_PRIORITY)
        ob_types    = ", ".join(dict.fromkeys(o["obligation_type"] for o in obs))

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
            "requirement_id":         req_id,
            "requirement_title":      req_title,
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
    return rows

# ── Main ──────────────────────────────────────────────────────────────────────
rows = build_rows()

total_obs = sum(len(json.loads(r["stage2_json"])["normalized_obligations"]) for r in rows)
print(f"Regulation {REGULATION_ID} — {len(rows)} requirement groups, {total_obs} total obligations")
print()
for r in rows:
    s2 = json.loads(r["stage2_json"])
    n  = len(s2["normalized_obligations"])
    print(f"  {r['requirement_id']} | {r['requirement_title']:<42} | {r['execution_category']:<22} | {r['criticality']:<6} | {n} obligations | {r['obligation_type']}")

if not DRY_RUN:
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
