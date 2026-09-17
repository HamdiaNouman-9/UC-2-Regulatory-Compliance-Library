"""
Patch for regulation 27880: adds REQ-030 (Article 13(4) — record keeping for examined transactions).
Run this AFTER migrate_old_analysis.py has already inserted the 29 base rows.
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

# ── New requirement data ───────────────────────────────────────────────────────
req_id    = "REQ-030"
ob_id     = "REQ-030-OB-001"
text      = ("Keep records of examined transactions for a period of ten years "
             "and make them available to competent authorities upon request.")
dept      = "IT / Records Management / Financial Intelligence Unit"
risk      = "Medium"
reference = "Article 13(4)"
controls  = [
    "Data retention policy mandating 10-year minimum for transaction monitoring records",
    "Secure archival system for examined transaction logs and investigation notes",
    "Tested retrieval process for providing records to competent authorities",
]

title    = text[:80].strip() + ("..." if len(text) > 80 else "")
ob_type  = "Documentation"          # "keep"/"records" keywords
exec_cat = "One_Time_Implementation" # Medium risk, no periodic/ongoing keywords
freq     = "Event-Driven"

evidence    = ["Record"]
test_method = (f"Review {controls[0].lower()} and {controls[2].lower()} "
               f"to verify compliance with {reference}.")

stage1 = {
    "requirement_id":    req_id,
    "requirement_title": title,
    "obligations": [{
        "obligation_id":    ob_id,
        "obligation_text":  text,
        "source_reference": reference,
    }],
}

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

control_obj = {
    "control_title":       controls[0],
    "control_objective":   (f"Ensure transaction monitoring records are retained for a "
                            f"minimum of ten years and are retrievable upon request."),
    "control_description": (f"{controls[0]}. {controls[1]}. {controls[2]}."),
    "control_owner":       dept,
    "control_type":        "Preventive",
    "execution_type":      "Hybrid",
    "frequency":           freq,
    "control_level":       "Process",
    "evidence_generated":  f"Transaction monitoring records retained for 10+ years per {reference}.",
    "key_steps":           controls,
    "residual_risk_if_failed": risk,
}

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

new_row = {
    "regulation_id":      REGULATION_ID,
    "requirement_id":     req_id,
    "requirement_title":  title,
    "execution_category": exec_cat,
    "criticality":        risk,
    "obligation_type":    ob_type,
    "stage1_json":        json.dumps(stage1, ensure_ascii=False),
    "stage2_json":        json.dumps(stage2, ensure_ascii=False),
    "stage3_json":        json.dumps(stage3, ensure_ascii=False),
    "stage4_md":          None,
    "analysis_json":      json.dumps({
        "requirement_id":    req_id,
        "requirement_title": title,
        "obligations":       stage2["normalized_obligations"],
        "controls":          [control_obj],
    }, ensure_ascii=False),
}

print(f"REG-{REGULATION_ID} | Adding {req_id}")
print(f"  title:    {title}")
print(f"  type:     {ob_type} | exec: {exec_cat} | criticality: {risk}")
print(f"  reference: {reference}")

if not DRY_RUN:
    repo.store_analysis([new_row], version_id=None)
    print(f"  [OK] Inserted {req_id} into local DB")
else:
    print("  [DRY RUN] - set DRY_RUN=False to insert")
