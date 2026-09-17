BEGIN TRANSACTION;

-- Control links + new suggested controls for regulation_id=27880
-- 5 DEMO_REQUIREMENT_CONTROL_LINK rows (deduped by req+ctrl)


-- 1. COMPLIANCE_REQUIREMENT id=16 → DEMO_CONTROL id=18 [partially_matched]
INSERT INTO DEMO_REQUIREMENT_CONTROL_LINK (COMPLIANCEREQUIREMENT_ID, CONTROL_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID) VALUES (16, 18, N'partially_matched', N'Enterprise-Wide AML/CFT Risk Assessment Review (REQ-001-OB-001) partially matches the Documented risk assessment methodology control: both target AML/CFT risk assessment documentation and review cycles, but the stage3 control adds annual scheduling, multi-dimensional coverage, and senior management sign-off requirements not specified in the existing control.', 27880);

-- 2. COMPLIANCE_REQUIREMENT id=1 → DEMO_CONTROL id=1 [fully_matched]
INSERT INTO DEMO_REQUIREMENT_CONTROL_LINK (COMPLIANCEREQUIREMENT_ID, CONTROL_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID) VALUES (1, 1, N'fully_matched', N'Customer Due Diligence Completion at Onboarding (REQ-002-OB-002) fully matches the CDD Verification Checklist control: both mandate completion of a CDD checklist at customer onboarding with sign-off before account activation.', 27880);

-- 3. COMPLIANCE_REQUIREMENT id=2 → DEMO_CONTROL id=2 [fully_matched]
INSERT INTO DEMO_REQUIREMENT_CONTROL_LINK (COMPLIANCEREQUIREMENT_ID, CONTROL_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID) VALUES (2, 2, N'fully_matched', N'PEP Screening and Additional Measures Workflow (REQ-002-OB-005) fully matches the PEP Screening Process control: both require automated PEP database screening at onboarding and on an ongoing basis with documented additional measures upon a confirmed match. Also covers High-Risk Country EDD Application (REQ-005-OB-001) which partially overlaps with this control''s high-risk customer scope.', 27880);

-- 4. COMPLIANCE_REQUIREMENT id=5 → DEMO_CONTROL id=5 [partially_matched]
INSERT INTO DEMO_REQUIREMENT_CONTROL_LINK (COMPLIANCEREQUIREMENT_ID, CONTROL_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID) VALUES (5, 5, N'partially_matched', N'Ongoing Transaction Monitoring Against Customer Profile (REQ-007-OB-001) and Enhanced Monitoring for High-Risk Customer Relationships (REQ-007-OB-003) both partially match the Periodic Customer Review Schedule control: the existing control covers periodic profile reviews but the stage3 controls additionally require continuous transaction-level monitoring and risk-tier-based parameter calibration.', 27880);

-- 5. COMPLIANCE_REQUIREMENT id=76 → DEMO_CONTROL id=31 [partially_matched]
INSERT INTO DEMO_REQUIREMENT_CONTROL_LINK (COMPLIANCEREQUIREMENT_ID, CONTROL_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID) VALUES (76, 31, N'partially_matched', N'AML/CFT Policy Framework Maintenance and Senior Management Approval (REQ-008-OB-001) partially matches the Maintain AML/CFT program control: both target a comprehensive AML/CFT compliance framework, but the stage3 control adds explicit senior management approval, proportionality assessment, and annual review cycle requirements not stated in the existing control.', 27880);

-- ── Verification ─────────────────────────────────────────────────────────
SELECT MATCH_STATUS, COUNT(*) AS cnt
FROM DEMO_REQUIREMENT_CONTROL_LINK
WHERE REGULATION_ID = 27880
GROUP BY MATCH_STATUS;

-- Expected: fully_matched=2, partially_matched=3

COMMIT;
-- On error: ROLLBACK;