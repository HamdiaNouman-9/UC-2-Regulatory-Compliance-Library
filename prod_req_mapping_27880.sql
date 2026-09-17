BEGIN TRANSACTION;

-- sama_requirement_mapping INSERT for regulation_id=27880
-- 31 obligations | 10 fully_matched | 7 partially_matched | 14 new
-- Generated from semantic analysis against COMPLIANCE_REQUIREMENT (110 rows, prod snapshot)

-- 1. REQ-001-OB-001 [fully_matched→ id=16]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Identify, assess, and document money laundering risks and keep the assessment up to date, taking into account a wide range of risk factors, including those relating to customers, countries or geographic areas, products, services, transactions and delivery channels.', 16, N'fully_matched', N'Directly covered by existing requirement on AML/CFT/PF business risk assessments (Article 5 risk factors align exactly).', NULL);

-- 2. REQ-001-OB-002 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Assess, prior to their use, the risks associated with new products, business practices and technologies.', NULL, N'new', N'No existing requirement specifically covers pre-launch AML risk assessment for new products, business practices, or technologies.', NULL);

-- 3. REQ-001-OB-003 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Provide risk assessment reports to the supervisory authorities upon request.', NULL, N'new', N'No existing requirement covers formal provision of AML risk assessment reports to supervisory authorities.', NULL);

-- 4. REQ-002-OB-001 [partially_matched→ id=1]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Shall not keep or open anonymous accounts or accounts in obviously fictitious names, or numbered accounts.', 1, N'partially_matched', N'CDD requirement (id=1) implicitly prevents anonymous accounts through identity verification, but does not contain an explicit prohibition on anonymous or fictitious-name accounts.', NULL);

-- 5. REQ-002-OB-002 [fully_matched→ id=1]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Apply due diligence measures to customers. The Implementing Regulation shall set forth the instances and types of measures.', 1, N'fully_matched', N'Directly and completely covered by existing AML Customer Due Diligence requirement (id=1).', NULL);

-- 6. REQ-002-OB-003 [partially_matched→ id=1]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Determine the extent of due diligence measures based on the risks related to a customer or business relationship.', 1, N'partially_matched', N'AML CDD requirement (id=1) covers CDD broadly but does not specifically mandate risk-based scoping or tiering of CDD measures.', NULL);

-- 7. REQ-002-OB-004 [fully_matched→ id=2]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Where a higher risk of money laundering was identified, apply enhanced due diligence measures.', 2, N'fully_matched', N'Directly covered by existing Enhanced Due Diligence requirement (id=2) which mandates EDD for high-risk customers.', NULL);

-- 8. REQ-002-OB-005 [fully_matched→ id=2]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Use appropriate systems to determine whether a customer or beneficial owner is or has become a politically exposed person (PEP) and if so, apply additional measures as prescribed by the Implementing Regulation.', 2, N'fully_matched', N'PEP screening and additional measures are explicitly covered by the Enhanced Due Diligence requirement (id=2).', NULL);

-- 9. REQ-003-OB-001 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Before entering into a cross-border correspondent relationship, apply appropriate risk mitigation measures as prescribed by the Implementing Regulation, and satisfy themselves that the respondent institution does not permit their account to be used by a shell bank.', NULL, N'new', N'No existing requirement covers pre-entry due diligence for cross-border correspondent banking relationships.', NULL);

-- 10. REQ-003-OB-002 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Shall not enter into or continue a correspondent relationship with a shell bank or a respondent institution that permits its account to be used by a shell bank.', NULL, N'new', N'No existing requirement prohibits correspondent relationships with shell banks.', NULL);

-- 11. REQ-004-OB-001 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Financial institutions providing wire transfer activities shall obtain information on the originator and beneficiary and ensure that such information is kept with the wire transfer or related message throughout the payment chain.', NULL, N'new', N'No existing requirement addresses wire transfer originator and beneficiary data requirements or payment chain information integrity.', NULL);

-- 12. REQ-004-OB-002 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'A financial institution that is unable to obtain required originator or beneficiary information shall not permit the execution of the wire transfer.', NULL, N'new', N'No existing requirement mandates blocking wire transfers when mandatory originator or beneficiary information is missing.', NULL);

-- 13. REQ-004-OB-003 [partially_matched→ id=9]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Record all originator and beneficiary information and keep the records, documents, data, and files in accordance with Article 12.', 9, N'partially_matched', N'Data Retention Policy (id=9) covers record retention broadly but does not specifically address wire transfer originator and beneficiary information recordkeeping.', NULL);

-- 14. REQ-004-OB-004 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Comply with all measures on wire transfers as set out in the Implementing Regulation.', NULL, N'new', N'No existing requirement specifically mandates full compliance with wire transfer provisions of the AML Implementing Regulation.', NULL);

-- 15. REQ-005-OB-001 [fully_matched→ id=2]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Apply enhanced due diligence measures proportionate to the risks involving business relationships and transactions with a person from a country identified as high risk by the FI or the Anti-Money Laundering Permanent Committee.', 2, N'fully_matched', N'Enhanced Due Diligence requirement (id=2) explicitly covers customers from high-risk jurisdictions, directly matching this obligation.', NULL);

-- 16. REQ-005-OB-002 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Apply the countermeasures prescribed by the Anti-Money Laundering Permanent Committee with respect to high risk countries.', NULL, N'new', N'No existing requirement covers implementation of AML Permanent Committee countermeasures for specific high-risk countries.', NULL);

-- 17. REQ-006-OB-001 [fully_matched→ id=9]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Keep all records and documents for domestic or international financial transactions as well as commercial and monetary transactions for a period of no less than ten years from the date of concluding the transaction or closure of account.', 9, N'fully_matched', N'Directly and completely covered by existing Data Retention Policy (id=9) requiring 10-year minimum retention for transaction records.', NULL);

-- 18. REQ-006-OB-002 [fully_matched→ id=9]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Keep all records obtained through due diligence measures, account files and business correspondences and copies of personal identification documents, including analysis results, for at least ten years after the business relationship has ended or after an occasional transaction.', 9, N'fully_matched', N'Directly covered by Data Retention Policy (id=9) — CDD records, account files, and ID documents are within its scope, and the 10-year post-relationship retention period matches.', NULL);

-- 19. REQ-006-OB-003 [partially_matched→ id=9]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Records shall be sufficient to permit reconstruction of transactions and shall be maintained in a manner so that they can be readily made available to competent authorities upon request.', 9, N'partially_matched', N'Data Retention Policy (id=9) covers retention duration but does not explicitly require reconstruction-sufficiency of records or formalise the process for making them available to competent authorities.', NULL);

-- 20. REQ-006-OB-004 [fully_matched→ id=9]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Keep records of examined transactions for a period of ten years and make them available to competent authorities upon request.', 9, N'fully_matched', N'Directly covered by Data Retention Policy (id=9) — 10-year minimum retention and authority access both explicitly covered.', NULL);

-- 21. REQ-007-OB-001 [partially_matched→ id=5]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Monitor and scrutinize transactions, document and data on an ongoing basis to ensure consistency with the knowledge of the customer, the customer''s commercial activities and risk profile, and where necessary the customer''s source of funds.', 5, N'partially_matched', N'Ongoing Customer Monitoring (id=5) covers periodic customer reviews but does not specifically address transaction-to-profile consistency monitoring on a continuous basis.', NULL);

-- 22. REQ-007-OB-002 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Examine any complex and unusual large transaction, and any unusual pattern of transactions that has no clear economic or legal objective.', NULL, N'new', N'No existing requirement specifically mandates examination of complex, unusual, or large transactions or unusual transaction patterns.', NULL);

-- 23. REQ-007-OB-003 [partially_matched→ id=5]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Where the risks of money laundering are higher, perform enhanced due diligence and increase the level and nature of monitoring of the relevant business relationship to determine whether the transaction is unusual or suspicious.', 5, N'partially_matched', N'Ongoing Customer Monitoring (id=5) partially covers this but does not address risk-triggered escalation of monitoring intensity or linkage to EDD.', NULL);

-- 24. REQ-008-OB-001 [partially_matched→ id=76]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Have in place and effectively implement internal policies, procedures and controls against money laundering aimed at managing and mitigating any identified risks. Policies must be proportionate to nature and size of business and approved by senior management.', 76, N'partially_matched', N'CFT Guidance Paper implementation (id=76) partially covers AML internal controls but does not mandate a comprehensive senior-management-approved AML policy framework proportionate to business size.', NULL);

-- 25. REQ-008-OB-002 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Review and enhance internal policies, procedures and controls as needed.', NULL, N'new', N'No existing requirement addresses periodic or trigger-based review and enhancement of AML internal policies and controls.', NULL);

-- 26. REQ-008-OB-003 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Apply its internal policies, procedures and controls to all of its branches and majority-owned subsidiaries.', NULL, N'new', N'No existing requirement mandates group-wide application of AML policies to all branches and majority-owned subsidiaries.', NULL);

-- 27. REQ-009-OB-001 [fully_matched→ id=3]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Promptly and directly report to the General Directorate of Financial Intelligence any transaction suspected or having reasonable grounds to suspect involves proceeds of crime or money laundering, including attempts. Provide a detailed report including all available data and information.', 3, N'fully_matched', N'Directly and completely covered by existing Suspicious Transaction Reporting requirement (id=3) mandating FIU reporting within 3 business days.', NULL);

-- 28. REQ-009-OB-002 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Promptly and fully respond to requests from the General Directorate of Financial Intelligence for additional information.', NULL, N'new', N'No existing requirement covers responding to FIU requests for additional information related to submitted or pending reports.', NULL);

-- 29. REQ-009-OB-003 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Prohibit directors, management, and employees from disclosing to a customer or any other person the fact that a report under this Law or related information will be, is being or has been submitted to the Directorate, or that a criminal investigation is being or has been carried out.', NULL, N'new', N'No existing requirement establishes a tipping-off prohibition for STR disclosures.', NULL);

-- 30. REQ-009-OB-004 [new]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Provide any additional information requested by the General Directorate of Financial Intelligence promptly, including through the supervisory authority when the request does not relate to a submitted report.', NULL, N'new', N'No existing requirement covers FIU information requests that are routed through the supervisory authority or that are unrelated to a filed STR.', NULL);

-- 31. REQ-030-OB-001 [fully_matched→ id=9]
INSERT INTO sama_requirement_mapping (regulation_id, extracted_requirement_text, matched_requirement_id, match_status, match_explanation, version_id) VALUES (27880, N'Keep records of examined transactions for a period of ten years and make them available to competent authorities upon request.', 9, N'fully_matched', N'Directly covered by Data Retention Policy (id=9) — 10-year minimum retention for transaction monitoring records and authority access both explicitly covered.', NULL);


-- ── Verification ─────────────────────────────────────────────────────────
SELECT match_status, COUNT(*) AS cnt
FROM sama_requirement_mapping
WHERE regulation_id = 27880
GROUP BY match_status
ORDER BY match_status;

-- Expected: fully_matched=10, partially_matched=7, new=14

COMMIT;
-- On error: ROLLBACK;