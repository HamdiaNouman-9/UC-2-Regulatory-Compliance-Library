from dotenv import load_dotenv
load_dotenv()
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, UploadFile, File, Form,Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import logging
import os
import json
import tempfile
from typing import Optional, List, Dict, Any
import time
from threading import Thread, Lock
from datetime import time as dtime

from scheduler.scheduler import run_sbp_pipeline, run_secp_pipeline, run_sama_pipeline,run_cbb_monitoring
from storage.mssql_repo import MSSQLRepository
from processor.gap_analyzer import GapAnalyzer
from processor.Text_Extractor import OCRProcessor
from processor.metadata_extractor import extract_metadata_from_text, extract_document_content
import docx as python_docx
from fastapi.responses import JSONResponse, Response
from processor.staged_LLM_Analyzer import StagedLLMAnalyzer
from processor.requirement_matcher import RequirementMatcher
from processor.LlmAnalyzer import LLMAnalyzer
from orchestrator.orchestrator import Orchestrator

from utils.lang_translator import (
    translate_regulation,
    translate_gap_result,
    translate_compliance_requirement,
    translate_texts_batch,
    translate_v2_gap_result,
)
print("=" * 100)
print("PIPELINE_API.PY MODULE LOADED!")
print("Python is executing this file")
print("=" * 100)

logger = logging.getLogger(__name__)
app = FastAPI(title="Regulatory Pipeline API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================================================================== #
#  DB SETUP                                                            #
# ================================================================== #

repo = MSSQLRepository({
    "server":   os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "username": os.getenv("MSSQL_USERNAME"),
    "password": os.getenv("MSSQL_PASSWORD"),
    "driver":   os.getenv("MSSQL_DRIVER"),
})

gap_analyzer = GapAnalyzer()
staged_analyzer = StagedLLMAnalyzer()
requirement_matcher = RequirementMatcher()

from crawler.cbb_monitoring_crawler import monitor_cbb_changes



# Add to REGULATOR_PIPELINES dictionary
REGULATOR_PIPELINES = {
    "SBP":  run_sbp_pipeline,
    "SECP": run_secp_pipeline,
    "SAMA": run_sama_pipeline,
    "CBB":  run_cbb_monitoring,
}

pipeline_lock = Lock()
SUPPORTED_LANGUAGES = {"en", "ar"}


def _validate_lang(lang: str) -> str:
    lang = (lang or "en").lower().strip()
    if lang not in SUPPORTED_LANGUAGES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported language '{lang}'. Supported: en, ar",
        )
    return lang


# ================================================================== #
#  AR CACHE HELPERS                                                    #
# ================================================================== #

def _get_ar_cache(cache_key: str):
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT response_json FROM ar_response_cache WHERE cache_key = ?",
                cache_key,
            )
            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
    except Exception as e:
        logger.warning(f"AR cache read failed for {cache_key}: {e}")
    return None


def _set_ar_cache(cache_key: str, data: dict):
    try:
        serialized = json.dumps(data, ensure_ascii=False, default=str)
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                MERGE ar_response_cache AS target
                USING (SELECT ? AS cache_key) AS src
                ON target.cache_key = src.cache_key
                WHEN MATCHED THEN
                    UPDATE SET response_json = ?, updated_at = GETUTCDATE()
                WHEN NOT MATCHED THEN
                    INSERT (cache_key, response_json) VALUES (?, ?);
                """,
                cache_key, serialized, cache_key, serialized,
            )
            conn.commit()
    except Exception as e:
        logger.warning(f"AR cache write failed for {cache_key}: {e}")


def _invalidate_ar_cache(regulation_id: int):
    keys = [
        f"GET /compliance-analysis-full/{regulation_id}",
        f"GET /requirement-mapping/{regulation_id}",
        f"GET /control-mapping/{regulation_id}",
        f"GET /kpi-mapping/{regulation_id}",
        f"GET /compliance-analysis/{regulation_id}",
        f"GET /regulation/{regulation_id}/analysis-versions",
        f"GET /regulation/{regulation_id}/versions",
    ]
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            for key in keys:
                cursor.execute(
                    "DELETE FROM ar_response_cache WHERE cache_key = ?", key
                )
            conn.commit()
    except Exception as e:
        logger.warning(
            f"AR cache invalidation failed for regulation {regulation_id}: {e}"
        )


# ================================================================== #
#  SERIALISATION HELPERS                                               #
# ================================================================== #

def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def row_to_dict(row, columns):
    return {col: serialize_datetime(value) for col, value in zip(columns, row)}


# ================================================================== #
#  PIPELINE HELPERS                                                    #
# ================================================================== #

def update_heartbeat(regulator: str):
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE pipeline_status
            SET last_heartbeat = GETUTCDATE()
            WHERE regulator = ? AND status = 'RUNNING'
            """,
            regulator,
        )
        conn.commit()


def run_pipeline_async(regulator: str):
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pipeline_status (regulator, status, started_at, last_heartbeat)
            VALUES (?, 'RUNNING', GETUTCDATE(), GETUTCDATE())
            """,
            regulator,
        )
        conn.commit()

    stop_heartbeat = False

    def heartbeat_loop():
        while not stop_heartbeat:
            update_heartbeat(regulator)
            time.sleep(300)

    Thread(target=heartbeat_loop, daemon=True).start()

    try:
        REGULATOR_PIPELINES[regulator]()
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE pipeline_status
                SET status = 'DONE', finished_at = GETUTCDATE()
                WHERE regulator = ? AND status = 'RUNNING'
                """,
                regulator,
            )
            conn.commit()
    except Exception as e:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE pipeline_status
                SET status = 'FAILED', finished_at = GETUTCDATE(), error = ?
                WHERE regulator = ? AND status = 'RUNNING'
                """,
                str(e), regulator,
            )
            conn.commit()


def scheduler_loop():
    logger.info("Scheduler started")
    while True:
        now = datetime.utcnow().time()
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT TOP 1 id, regulator
                FROM pipeline_schedule
                WHERE scheduled_time <= ? AND status = 'PENDING'
                ORDER BY scheduled_time
                """,
                now,
            )
            job = cursor.fetchone()

        if job:
            schedule_id, regulator = job
            if pipeline_lock.acquire(blocking=False):
                try:
                    with repo._get_conn() as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "UPDATE pipeline_schedule SET status='RUNNING' WHERE id=?",
                            schedule_id,
                        )
                        conn.commit()
                    run_pipeline_async(regulator)
                    with repo._get_conn() as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "UPDATE pipeline_schedule SET status='DONE', last_run_at=GETUTCDATE() WHERE id=?",
                            schedule_id,
                        )
                        conn.commit()
                finally:
                    pipeline_lock.release()

        time.sleep(30)


# ================================================================== #
#  PYDANTIC MODELS                                                     #
# ================================================================== #

class ScheduleUpdate(BaseModel):
    regulator: str
    hour: int
    minute: int


class CategoryInfo(BaseModel):
    id: Optional[int]
    title: Optional[str]
    parent_id: Optional[int]
    type: Optional[str]


class RegulationModel(BaseModel):
    id: int
    regulator: str
    source_system: Optional[str]
    category: Optional[str]
    title: str
    document_url: Optional[str]
    document_html: Optional[str]
    published_date: Optional[str]
    reference_no: Optional[str]
    department: Optional[str]
    year: Optional[int]
    source_page_url: Optional[str]
    extra_meta: Optional[Dict[str, Any]]
    created_at: Optional[str]
    updated_at: Optional[str]
    category_info: Optional[CategoryInfo]


class StatusUpdate(BaseModel):
    record_id: int
    status: str


class ComplianceStatusUpdate(BaseModel):
    regulation_id: int
    requirement_id: str
    status: str


class VersionStatusUpdate(BaseModel):
    status: str  # 'active' or 'inactive'


# ================================================================== #
#  GAP ANALYSIS MODELS                                                 #
# ================================================================== #

class GapResult(BaseModel):
    obligation_text: Optional[str] = None
    coverage_status: str
    evidence_text: Optional[str]
    gap_description: Optional[str]
    controls: Optional[str] = None
    kpis: Optional[str] = None
    obligation_id: Optional[str] = None
    requirement_id: Optional[str] = None
    requirement_title: Optional[str] = None
    criticality: Optional[str] = None
    obligation_type: Optional[str] = None
    execution_category: Optional[str] = None


class RegulationGapSummary(BaseModel):
    regulation_id: int
    results: List[GapResult]
    summary: Dict[str, Any]


class GapAnalysisResponse(BaseModel):
    session_id: int
    uploaded_document_name: str
    regulations: List[RegulationGapSummary]


# ================================================================== #
#  FILE HELPERS                                                         #
# ================================================================== #

async def _save_and_extract_file(upload_file: UploadFile) -> str:
    filename = upload_file.filename.lower()
    suffix = os.path.splitext(filename)[-1] or ".pdf"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await upload_file.read()
        tmp.write(content)
        tmp_path = tmp.name
    try:
        if suffix == ".pdf":
            text, _ = OCRProcessor.extract_text_from_pdf_smart(tmp_path)
        elif suffix in (".docx", ".doc"):
            doc = python_docx.Document(tmp_path)
            text = "\n\n".join(
                [p.text for p in doc.paragraphs if p.text.strip()]
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {suffix}",
            )
        return text
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _build_upload_doc_object(
    metadata: dict,
    text: str,
    filename: str,
    compliancecategory_id: Optional[int],
    regulator: Optional[str],
    source_system: Optional[str],
    category: Optional[str],
    document_url: Optional[str] = None,
    document_html: Optional[str] = None,
):
    class _Doc:
        pass

    doc = _Doc()
    doc.title          = metadata.get("title") or filename
    doc.published_date = metadata.get("published_date")
    doc.reference_no   = metadata.get("reference_no")
    doc.year           = metadata.get("year")
    doc.regulator      = (regulator or "UPLOAD").upper()
    doc.source_system  = source_system or "MANUAL-UPLOAD"
    doc.category       = category
    doc.department     = None
    doc.document_url   = document_url or None
    doc.source_page_url = document_url or None
    doc.document_html  = document_html
    doc.doc_path       = None
    doc.status         = metadata.get("status") or "active"
    doc.type           = "uploaded"
    doc.compliancecategory_id = compliancecategory_id
    doc.extra_meta = {
        "org_pdf_text":    text,
        "upload_filename": filename,
    }
    return doc


def _run_upload_requirement_matching(regulation_id: int, rows: list) -> dict:
    """Requirement matching for the upload flow (no version_id — non-CBB uploads)."""
    extracted_requirements = []
    for r in rows:
        s2 = r.get("stage2_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except Exception:
                s2 = {}
        for ob in s2.get("normalized_obligations", []):
            extracted_requirements.append({
                "requirement_text": ob["obligation_text"],
                "department":       "",
                "risk_level":       ob.get("criticality", "Medium"),
                "controls":         [],
                "kpis":             [],
                "_obligation_id":   ob["obligation_id"],
                "_requirement_id":  r.get("requirement_id"),
            })

    if not extracted_requirements:
        return {"skipped": True, "reason": "No obligations to match"}

    existing_requirements  = repo.get_all_compliance_requirements()
    existing_controls      = repo.get_all_demo_controls()
    existing_kpis          = repo.get_all_demo_kpis()
    linked_controls_by_req = repo.get_linked_controls_by_requirement()
    linked_kpis_by_req     = repo.get_linked_kpis_by_requirement()

    matcher = RequirementMatcher()
    match_results = matcher.match_requirements(
        regulation_id=regulation_id,
        extracted_requirements=extracted_requirements,
        existing_requirements=existing_requirements,
        existing_controls=existing_controls,
        existing_kpis=existing_kpis,
        linked_controls_by_req=linked_controls_by_req,
        linked_kpis_by_req=linked_kpis_by_req,
    )

    requirement_mappings   = match_results["requirement_mappings"]
    control_links          = match_results["control_links"]
    kpi_links              = match_results["kpi_links"]
    new_controls_to_insert = match_results["new_controls_to_insert"]
    new_kpis_to_insert     = match_results["new_kpis_to_insert"]

    # version_id=None for all uploaded (non-CBB) documents
    if requirement_mappings:
        repo.store_requirement_mappings(requirement_mappings, version_id=None)

    partially_matched_ids = [
        m["matched_requirement_id"]
        for m in requirement_mappings
        if m["match_status"] == "partially_matched" and m.get("matched_requirement_id")
    ]
    if partially_matched_ids:
        repo.flag_partially_matched_requirements(partially_matched_ids)

    new_req_mappings = [m for m in requirement_mappings if m["match_status"] == "new"]
    for i, mapping in enumerate(new_req_mappings):
        try:
            req_text = mapping["extracted_requirement_text"]
            title = req_text[:100].strip() + ("..." if len(req_text) > 100 else "")
            new_req_id = repo.insert_new_suggested_requirement({
                "title":       title,
                "description": req_text,
                "ref_key":     f"UPLOAD-AUTO-{regulation_id}-{i}",
                "ref_no":      f"REG-{regulation_id}",
            })
            for ctrl in new_controls_to_insert:
                if ctrl.get("_req_id") is None:
                    ctrl["_req_id"] = new_req_id
            for kpi in new_kpis_to_insert:
                if kpi.get("_req_id") is None:
                    kpi["_req_id"] = new_req_id
        except Exception as e:
            logger.error(f"[upload] Failed to insert new suggested requirement: {e}")

    if control_links:
        repo.store_control_links(control_links)
    if kpi_links:
        repo.store_kpi_links(kpi_links)

    for ctrl in new_controls_to_insert:
        try:
            new_ctrl_id = repo.insert_new_suggested_control({
                "title":       ctrl["title"],
                "description": ctrl["description"],
                "control_key": ctrl["control_key"],
            })
            req_id = ctrl.get("_req_id")
            if req_id:
                repo.store_control_links([{
                    "compliancerequirement_id": req_id,
                    "control_id":              new_ctrl_id,
                    "match_status":            "new",
                    "match_explanation":       ctrl.get("_explanation", ""),
                    "regulation_id":           regulation_id,
                }])
        except Exception as e:
            logger.error(f"[upload] Failed to insert new suggested control: {e}")

    for kpi in new_kpis_to_insert:
        try:
            new_kpi_id = repo.insert_new_suggested_kpi({
                "title":       kpi["title"],
                "description": kpi["description"],
                "kisetup_key": kpi["kisetup_key"],
                "formula":     kpi.get("formula", ""),
            })
            req_id = kpi.get("_req_id")
            if req_id:
                repo.store_kpi_links([{
                    "compliancerequirement_id": req_id,
                    "kisetup_id":              new_kpi_id,
                    "match_status":            "new",
                    "match_explanation":       kpi.get("_explanation", ""),
                    "regulation_id":           regulation_id,
                }])
        except Exception as e:
            logger.error(f"[upload] Failed to insert new suggested KPI: {e}")

    return {
        "total":             len(requirement_mappings),
        "fully_matched":     sum(1 for m in requirement_mappings if m["match_status"] == "fully_matched"),
        "partially_matched": sum(1 for m in requirement_mappings if m["match_status"] == "partially_matched"),
        "new":               sum(1 for m in requirement_mappings if m["match_status"] == "new"),
        "control_links":     len(control_links),
        "kpi_links":         len(kpi_links),
        "new_controls":      len(new_controls_to_insert),
        "new_kpis":          len(new_kpis_to_insert),
    }


# ================================================================== #
#  V2 GAP HELPERS                                                      #
# ================================================================== #

_V2_GAP_TRANSLATABLE_FIELDS = [
    "obligation_text", "evidence_text", "gap_description",
    "controls", "kpis", "requirement_title",
]
# ── 5. Trigger endpoints — add lang param for consistency ────────────────────
# These return counts/IDs/status, not document text — no translation needed.
# Just add lang: str = Query("en") to each signature.
 
@app.post("/trigger/full-analysis/{regulation_id}", tags=["V2 Staged Analysis"])
def trigger_full_analysis(
    regulation_id: int,
    force: bool = Query(False),
    lang: str = Query("en"),
):
    # body unchanged — just added lang param
    analysis_result = {}
    try:
        analysis_result = trigger_staged_analysis(regulation_id, force=force)
    except HTTPException as e:
        raise HTTPException(
            e.status_code,
            f"Staged analysis failed: {e.detail if isinstance(e.detail, str) else json.dumps(e.detail)}"
        )
 
    matching_result = {}
    matching_error  = None
    try:
        matching_result = trigger_requirement_matching_v2(regulation_id)
    except HTTPException as e:
        matching_error = e.detail if isinstance(e.detail, str) else json.dumps(e.detail)
        logger.warning(f"[full-analysis] matching failed for {regulation_id}: {matching_error}")
    except Exception as e:
        matching_error = str(e)
        logger.error(f"[full-analysis] matching unexpected error {regulation_id}: {e}")
 
    return {
        "success":       True,
        "regulation_id": regulation_id,
        "force":         force,
        "lang":          lang,
        "analysis": {
            "skipped":                analysis_result.get("skipped", False),
            "requirements_extracted": analysis_result.get("analysis", {}).get("requirements_extracted", 0),
            "version_id":             analysis_result.get("version_id"),
            "content_type":           analysis_result.get("content_type"),
            "text_length":            analysis_result.get("text_length"),
            "by_execution_category":  analysis_result.get("analysis", {}).get("by_execution_category", {}),
            "by_criticality":         analysis_result.get("analysis", {}).get("by_criticality", {}),
        },
        "matching": {
            "success": matching_error is None,
            "error":   matching_error,
            "summary": matching_result.get("summary", {}),
        },
        "next_steps": {
            "view_full_analysis": f"GET  /compliance-analysis/{regulation_id}",
            "view_mapping":       f"GET  /requirement-mapping/{regulation_id}",
            "gap_analysis":       f"POST /gap-analysis/single  (form: regulation_id={regulation_id})",
            "delete_and_rerun":   f"DELETE /admin/analysis/{regulation_id}  →  POST /trigger/full-analysis/{regulation_id}?force=true",
        },
    }

def _run_gap_for_regulation_v2(
    session_id: int, regulation_id: int, uploaded_text: str
) -> RegulationGapSummary:
    # get_compliance_analysis() is the unified read method (replaces get_compliance_analysis_v2)
    rows = repo.get_compliance_analysis(regulation_id)
    if not rows:
        raise HTTPException(
            404,
            f"No analysis found for regulation {regulation_id}. "
            f"Run POST /trigger/staged-analysis/{regulation_id} first.",
        )

    requirements_for_gap = []
    ob_metadata: Dict[str, Dict] = {}

    for row in rows:
        s2 = row.get("stage2_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except Exception:
                s2 = {}
        for ob in s2.get("normalized_obligations", []):
            ob_text = ob["obligation_text"]
            ob_metadata[ob_text] = {
                "obligation_id":      ob["obligation_id"],
                "requirement_id":     row["requirement_id"],
                "requirement_title":  row["requirement_title"],
                "criticality":        ob.get("criticality"),
                "obligation_type":    ob.get("obligation_type"),
                "execution_category": ob.get("execution_category"),
            }
            requirements_for_gap.append({"requirement_text": ob_text})

    if not requirements_for_gap:
        raise HTTPException(
            404,
            f"No obligations found in analysis for regulation {regulation_id}",
        )

    logger.info(
        f"[gap-v2] regulation {regulation_id}, {len(requirements_for_gap)} obligations"
    )
    results = gap_analyzer.analyze_gaps(
        uploaded_text=uploaded_text, requirements=requirements_for_gap
    )
    repo.store_gap_results(session_id, regulation_id, results)

    enriched_results: List[GapResult] = []
    for r in results:
        ob_text = r.get("obligation_text") or r.get("requirement_text", "")
        meta = ob_metadata.get(ob_text, {})
        enriched_results.append(GapResult(
            obligation_text=ob_text,
            coverage_status=r.get("coverage_status", "missing"),
            evidence_text=r.get("evidence_text"),
            gap_description=r.get("gap_description"),
            obligation_id=meta.get("obligation_id"),
            requirement_id=meta.get("requirement_id"),
            requirement_title=meta.get("requirement_title"),
            criticality=meta.get("criticality"),
            obligation_type=meta.get("obligation_type"),
            execution_category=meta.get("execution_category"),
        ))

    exec_values = sorted({r.execution_category for r in enriched_results if r.execution_category})
    summary = {
        "total":   len(enriched_results),
        "covered": sum(1 for r in enriched_results if r.coverage_status == "covered"),
        "partial": sum(1 for r in enriched_results if r.coverage_status == "partial"),
        "missing": sum(1 for r in enriched_results if r.coverage_status == "missing"),
        "by_criticality": {
            "High":   sum(1 for r in enriched_results if r.criticality == "High"),
            "Medium": sum(1 for r in enriched_results if r.criticality == "Medium"),
            "Low":    sum(1 for r in enriched_results if r.criticality == "Low"),
        },
        "by_execution_category": {
            ec: sum(1 for r in enriched_results if r.execution_category == ec)
            for ec in exec_values
        },
    }

    return RegulationGapSummary(
        regulation_id=regulation_id,
        results=enriched_results,
        summary=summary,
    )


def _enrich_results_with_controls_v2(
    results: List[GapResult], regulation_id: int
) -> List[GapResult]:
    rows = repo.get_compliance_analysis(regulation_id)
    if not rows:
        return results

    ob_text_to_control: Dict[str, str] = {}
    for row in rows:
        s2 = row.get("stage2_json") or {}
        s3 = row.get("stage3_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except Exception:
                s2 = {}
        if isinstance(s3, str):
            try:
                s3 = json.loads(s3)
            except Exception:
                s3 = {}
        control_map = {
            ob["obligation_id"]: ob.get("control")
            for ob in s3.get("obligations", [])
            if ob.get("control")
        }
        for ob in s2.get("normalized_obligations", []):
            ctrl = control_map.get(ob["obligation_id"])
            if ctrl:
                ob_text_to_control[ob["obligation_text"]] = ctrl.get("control_title", "")

    for result in results:
        lookup_key = result.obligation_text or getattr(result, "requirement_text", None)
        ctrl_title = ob_text_to_control.get(lookup_key)
        if ctrl_title:
            result.controls = ctrl_title

    return results


def _translate_v2_gap_results(
    results: List[GapResult], lang: str
) -> List[GapResult]:
    ENUM_TRANSLATIONS = {
        "coverage_status": {"covered": "مغطى", "partial": "جزئي", "missing": "مفقود"},
        "criticality":     {"High": "عالي", "Medium": "متوسط", "Low": "منخفض"},
        "obligation_type": {
            "Reporting": "إبلاغ", "Governance": "حوكمة",
            "Preventive": "وقائي", "Detective": "كشف", "Corrective": "تصحيحي",
        },
        "execution_category": {
            "Ongoing_Control": "رقابة مستمرة",
            "One_Time_Implementation": "تنفيذ لمرة واحدة",
            "Periodic_Review": "مراجعة دورية",
            "Governance_Approval": "موافقة الحوكمة",
            "One_Off_Reporting": "إبلاغ لمرة واحدة",
            "Event_Driven": "حسب الحدث",
            "Continuous_Monitoring": "مراقبة مستمرة",
            "Annual_Review": "مراجعة سنوية",
        },
    }
    TEXT_FIELDS = [
        "obligation_text", "evidence_text", "gap_description",
        "controls", "kpis", "requirement_title",
    ]

    results_dicts = [r.dict() for r in results]

    for r in results_dicts:
        for field, translations in ENUM_TRANSLATIONS.items():
            val = r.get(field)
            if val:
                r[field] = translations.get(val, val)

    all_texts, positions = [], []
    for i, r in enumerate(results_dicts):
        for f in TEXT_FIELDS:
            val = r.get(f)
            if val and isinstance(val, str):
                all_texts.append(val)
                positions.append((i, f))

    if all_texts:
        translated = translate_texts_batch(all_texts, lang)
        for (i, f), tr in zip(positions, translated):
            results_dicts[i][f] = tr

    return [GapResult(**r) for r in results_dicts]


def _gap_cache_key(endpoint: str, regulation_id: int, filename: str) -> str:
    return f"POST {endpoint}|reg={regulation_id}|file={filename}"


# ================================================================== #
#  COMPLIANCE ANALYSIS RESPONSE BUILDERS                               #
# ================================================================== #

def build_full_mapping_response(regulation_id: int, lang: str):
    req_mappings = repo.get_requirement_mappings_by_regulation(regulation_id)
    # Unified read — works for all regulators
    v2_rows = repo.get_compliance_analysis(regulation_id)

    ob_id_to_detail: Dict[str, dict] = {}
    ob_id_to_control: Dict[str, dict] = {}
    req_id_to_v2_meta: Dict[str, dict] = {}

    for row in v2_rows:
        s2 = row.get("stage2_json") or {}
        s3 = row.get("stage3_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except Exception:
                s2 = {}
        if isinstance(s3, str):
            try:
                s3 = json.loads(s3)
            except Exception:
                s3 = {}

        req_id_to_v2_meta[row["requirement_id"]] = {
            "requirement_id":     row["requirement_id"],
            "requirement_title":  row["requirement_title"],
            "execution_category": row.get("execution_category"),
            "criticality":        row.get("criticality"),
            "obligation_type":    row.get("obligation_type"),
        }

        s3_control_map = {
            ob["obligation_id"]: ob.get("control")
            for ob in s3.get("obligations", [])
            if ob.get("obligation_id") and ob.get("control")
        }

        for ob in s2.get("normalized_obligations", []):
            ob_id = ob["obligation_id"]
            ob_id_to_detail[ob_id] = ob
            ctrl = s3_control_map.get(ob_id)
            if ctrl:
                ob_id_to_control[ob_id] = ctrl

    matched_req_ids = list({
        m["matched_requirement_id"]
        for m in req_mappings
        if m.get("matched_requirement_id")
    })

    ctrl_links_reg = repo.get_control_links_by_regulation(regulation_id)
    ctrl_links_req = (
        repo.get_control_links_by_requirement_ids(matched_req_ids)
        if matched_req_ids else []
    )

    seen_ctrl = set()
    all_ctrl_links = []
    for c in ctrl_links_reg + ctrl_links_req:
        key = (c["COMPLIANCEREQUIREMENT_ID"], c["CONTROL_ID"])
        if key not in seen_ctrl:
            seen_ctrl.add(key)
            all_ctrl_links.append(c)

    existing_controls_by_req: Dict[int, list] = {}
    for ctrl in all_ctrl_links:
        req_id = ctrl["COMPLIANCEREQUIREMENT_ID"]
        existing_controls_by_req.setdefault(req_id, []).append(ctrl)

    grouped = []
    for mapping in req_mappings:
        matched_req_id = mapping.get("matched_requirement_id")
        obligation_id  = mapping.get("obligation_id")
        v2_req_id      = mapping.get("requirement_id")

        v2_meta    = req_id_to_v2_meta.get(v2_req_id, {})
        ob_detail  = ob_id_to_detail.get(obligation_id, {}) if obligation_id else {}
        stage3_control = ob_id_to_control.get(obligation_id) if obligation_id else None
        db_controls    = existing_controls_by_req.get(matched_req_id, [])

        controls_output = []
        if db_controls:
            for db_ctrl in db_controls:
                ctrl_entry = {
                    "control_id":          db_ctrl["CONTROL_ID"],
                    "control_title":       db_ctrl.get("control_title"),
                    "control_description": db_ctrl.get("control_description"),
                    "control_key":         db_ctrl.get("control_key"),
                    "match_status":        db_ctrl["MATCH_STATUS"],
                    "match_explanation":   db_ctrl.get("MATCH_EXPLANATION"),
                    "is_suggested":        db_ctrl.get("is_suggested") == 1,
                    "source":              "existing_db",
                    "comparison_result":   "existing_matched",
                }
                if stage3_control:
                    db_title = (db_ctrl.get("control_title") or "").lower().strip()
                    s3_title = (stage3_control.get("control_title") or "").lower().strip()
                    if db_title == s3_title or db_title in s3_title or s3_title in db_title:
                        ctrl_entry["comparison_result"] = "matched_with_ai"
                        for k in [
                            "control_objective", "control_owner", "control_type",
                            "execution_type", "frequency", "control_level",
                            "evidence_generated", "key_steps", "residual_risk_if_failed",
                        ]:
                            ctrl_entry[k] = stage3_control.get(k)
                controls_output.append(ctrl_entry)

        if stage3_control:
            matched_titles = {
                (c.get("control_title") or "").lower().strip() for c in db_controls
            }
            s3_title = (stage3_control.get("control_title") or "").lower().strip()
            already_represented = any(
                s3_title == t or s3_title in t or t in s3_title
                for t in matched_titles
            ) if matched_titles else False

            if not already_represented:
                controls_output.append({
                    "control_id":              None,
                    "control_title":           stage3_control.get("control_title"),
                    "control_description":     stage3_control.get("control_description"),
                    "control_objective":       stage3_control.get("control_objective"),
                    "control_owner":           stage3_control.get("control_owner"),
                    "control_type":            stage3_control.get("control_type"),
                    "execution_type":          stage3_control.get("execution_type"),
                    "frequency":               stage3_control.get("frequency"),
                    "control_level":           stage3_control.get("control_level"),
                    "evidence_generated":      stage3_control.get("evidence_generated"),
                    "key_steps":               stage3_control.get("key_steps", []),
                    "residual_risk_if_failed": stage3_control.get("residual_risk_if_failed"),
                    "match_status":            "new",
                    "match_explanation":       "AI-designed control; no existing control matched.",
                    "is_suggested":            True,
                    "source":                  "stage3_llm",
                    "comparison_result":       "ai_designed",
                })

        entry = {
            "obligation_id":              obligation_id,
            "obligation_requirement_id":  v2_req_id,
            "obligation_text":            ob_detail.get("obligation_text") or mapping["extracted_requirement_text"],
            "obligation_type":            ob_detail.get("obligation_type") or v2_meta.get("obligation_type"),
            "criticality":                ob_detail.get("criticality") or v2_meta.get("criticality"),
            "execution_category":         ob_detail.get("execution_category") or v2_meta.get("execution_category"),
            "evidence_expected":          ob_detail.get("evidence_expected", []),
            "test_method":                ob_detail.get("test_method"),
            "clarity_score":              ob_detail.get("clarity_score"),
            "needs_manual_review":        ob_detail.get("needs_manual_review"),
            "source_reference":           ob_detail.get("source_reference"),
            "matched_requirement_id":          matched_req_id,
            "matched_requirement_title":       mapping.get("matched_requirement_title"),
            "matched_requirement_description": mapping.get("matched_requirement_description"),
            "match_status":                    mapping["match_status"],
            "match_explanation":               mapping.get("match_explanation"),
            "requirement_group": {
                "requirement_id":     v2_meta.get("requirement_id"),
                "requirement_title":  v2_meta.get("requirement_title"),
                "execution_category": v2_meta.get("execution_category"),
                "criticality":        v2_meta.get("criticality"),
            },
            "controls": controls_output,
        }
        grouped.append(entry)

    if lang == "ar":
        OB_TEXT_FIELDS   = ["obligation_text", "test_method", "match_explanation",
                             "matched_requirement_title", "matched_requirement_description"]
        CTRL_TEXT_FIELDS = ["control_title", "control_description", "control_objective",
                             "control_owner", "match_explanation", "evidence_generated"]
        all_texts, positions = [], []
        for i, entry in enumerate(grouped):
            for f in OB_TEXT_FIELDS:
                val = entry.get(f)
                if val and isinstance(val, str):
                    all_texts.append(val)
                    positions.append(("ob", i, f, None, None))
            for j, ctrl in enumerate(entry["controls"]):
                for f in CTRL_TEXT_FIELDS:
                    val = ctrl.get(f)
                    if val and isinstance(val, str):
                        all_texts.append(val)
                        positions.append(("ctrl", i, f, j, None))
                for k_idx, step in enumerate(ctrl.get("key_steps") or []):
                    if step and isinstance(step, str):
                        all_texts.append(step)
                        positions.append(("step", i, "key_steps", j, k_idx))
        if all_texts:
            import copy
            grouped = copy.deepcopy(grouped)
            translated = translate_texts_batch(all_texts, lang)
            for (kind, i, f, j, k_idx), tr in zip(positions, translated):
                if kind == "ob":
                    grouped[i][f] = tr
                elif kind == "ctrl":
                    grouped[i]["controls"][j][f] = tr
                elif kind == "step":
                    grouped[i]["controls"][j]["key_steps"][k_idx] = tr

    fully   = sum(1 for r in req_mappings if r["match_status"] == "fully_matched")
    partial = sum(1 for r in req_mappings if r["match_status"] == "partially_matched")
    new     = sum(1 for r in req_mappings if r["match_status"] == "new")
    total_controls     = sum(len(e["controls"]) for e in grouped)
    ai_ctrl_count      = sum(1 for e in grouped for c in e["controls"] if c.get("source") == "stage3_llm")
    matched_ctrl_count = sum(1 for e in grouped for c in e["controls"] if c.get("comparison_result") == "matched_with_ai")

    return {
        "requirements": grouped,
        "summary": {
            "requirements": {
                "total":             len(req_mappings),
                "fully_matched":     fully,
                "partially_matched": partial,
                "new":               new,
            },
            "controls": {
                "total":            total_controls,
                "existing_matched": matched_ctrl_count,
                "ai_designed_new":  ai_ctrl_count,
            },
        },
    }


def build_v2_full_analysis_response(regulation_id: int, lang: str):
    # Unified read from compliance_analysis
    v2_rows = repo.get_compliance_analysis(regulation_id)
    if not v2_rows:
        return {"requirements": [], "summary": {}}

    req_mappings = repo.get_requirement_mappings_by_regulation(regulation_id)
    mapping_lookup = {}
    for m in req_mappings:
        key = m.get("obligation_id") or m.get("extracted_requirement_text")
        if key:
            mapping_lookup[key] = m

    requirements_list = []
    total_obligations = 0
    status_counts = {"fully_matched": 0, "partially_matched": 0, "new": 0}

    for row in v2_rows:
        s2 = row.get("stage2_json") or {}
        s3 = row.get("stage3_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except Exception:
                s2 = {}
        if isinstance(s3, str):
            try:
                s3 = json.loads(s3)
            except Exception:
                s3 = {}

        control_lookup = {
            ob["obligation_id"]: ob.get("control")
            for ob in s3.get("obligations", [])
            if ob.get("obligation_id") and ob.get("control")
        }

        obligations = []
        req_status_counts = {"fully_matched": 0, "partially_matched": 0, "new": 0}

        for ob in s2.get("normalized_obligations", []):
            ob_id   = ob.get("obligation_id")
            ob_text = ob.get("obligation_text")
            mapping = mapping_lookup.get(ob_id) or mapping_lookup.get(ob_text) or {}
            match_status   = mapping.get("match_status", "new")
            matched_req_id = mapping.get("matched_requirement_id")
            match_explanation = mapping.get("match_explanation")

            obligations.append({
                "obligation_id":         ob_id,
                "obligation_text":       ob_text,
                "obligation_type":       ob.get("obligation_type"),
                "criticality":           ob.get("criticality"),
                "execution_category":    ob.get("execution_category"),
                "evidence_expected":     ob.get("evidence_expected"),
                "test_method":           ob.get("test_method"),
                "clarity_score":         ob.get("clarity_score"),
                "needs_manual_review":   ob.get("needs_manual_review"),
                "source_reference":      ob.get("source_reference"),
                "match_status":          match_status,
                "matched_requirement_id": matched_req_id,
                "match_explanation":     match_explanation,
                "control":               control_lookup.get(ob_id),
            })

            if match_status in req_status_counts:
                req_status_counts[match_status] += 1
            total_obligations += 1

        agg_status = "fully_matched"
        if req_status_counts["new"] > 0:
            agg_status = "new"
        elif req_status_counts["partially_matched"] > 0:
            agg_status = "partially_matched"

        status_counts[agg_status] += 1

        requirements_list.append({
            "requirement_id":     row.get("requirement_id"),
            "requirement_title":  row.get("requirement_title"),
            "execution_category": row.get("execution_category"),
            "criticality":        row.get("criticality"),
            "obligation_type":    row.get("obligation_type"),
            # version_id present for CBB rows, None for SAMA/SBP
            "version_id":         row.get("version_id"),
            "match_status":       agg_status,
            "obligations":        obligations,
            "obligations_total":  len(obligations),
            "controls_designed":  sum(1 for ob in obligations if ob.get("control")),
            "status":             row.get("status"),
            "created_at":         serialize_datetime(row.get("created_at")),
        })

    if lang == "ar":
        ENUM_TRANSLATIONS = {
            "match_status": {
                "fully_matched": "مطابق بالكامل",
                "partially_matched": "مطابق جزئيًا",
                "new": "جديد",
            },
            "execution_category": {
                "Ongoing_Control": "رقابة مستمرة",
                "One_Time_Implementation": "تنفيذ لمرة واحدة",
                "Periodic_Review": "مراجعة دورية",
            },
            "criticality": {"High": "عالي", "Medium": "متوسط", "Low": "منخفض"},
            "obligation_type": {
                "Reporting": "إبلاغ", "Governance": "حوكمة",
                "Preventive": "وقائي", "Detective": "كشف", "Corrective": "تصحيحي",
            },
            "control_type":     {"Preventive": "وقائي", "Detective": "كشف", "Corrective": "تصحيحي"},
            "execution_type":   {"Manual": "يدوي", "Automated": "آلي", "Hybrid": "هجين"},
            "frequency": {
                "Daily": "يومي", "Weekly": "أسبوعي", "Monthly": "شهري",
                "Quarterly": "ربع سنوي", "Annually": "سنوي",
                "Event-Driven": "حسب الحدث",
            },
            "control_level":            {"System": "نظام", "Process": "عملية", "Entity": "كيان", "Transaction": "معاملة"},
            "residual_risk_if_failed":  {"High": "عالي", "Medium": "متوسط", "Low": "منخفض"},
        }

        all_texts, positions = [], []
        for r_idx, req in enumerate(requirements_list):
            if req.get("requirement_title"):
                all_texts.append(req["requirement_title"])
                positions.append(("req_title", r_idx, None, "requirement_title", None))
            for field in ["execution_category", "criticality", "obligation_type", "match_status"]:
                val = req.get(field)
                if val and field in ENUM_TRANSLATIONS:
                    req[field] = ENUM_TRANSLATIONS[field].get(val, val)

            for o_idx, ob in enumerate(req["obligations"]):
                if ob.get("obligation_text"):
                    all_texts.append(ob["obligation_text"])
                    positions.append(("ob_text", r_idx, o_idx, "obligation_text", None))
                if ob.get("match_explanation"):
                    all_texts.append(ob["match_explanation"])
                    positions.append(("ob_exp", r_idx, o_idx, "match_explanation", None))
                for field in ["obligation_type", "criticality", "execution_category", "match_status"]:
                    val = ob.get(field)
                    if val and field in ENUM_TRANSLATIONS:
                        ob[field] = ENUM_TRANSLATIONS[field].get(val, val)
                for field in ["test_method", "source_reference"]:
                    val = ob.get(field)
                    if val and isinstance(val, str):
                        all_texts.append(val)
                        positions.append(("ob_field", r_idx, o_idx, field, None))
                for e_idx, evidence in enumerate(ob.get("evidence_expected") or []):
                    if evidence and isinstance(evidence, str):
                        all_texts.append(evidence)
                        positions.append(("evidence", r_idx, o_idx, "evidence_expected", e_idx))

                ctrl = ob.get("control")
                if ctrl:
                    for field in ["control_title", "control_description", "control_objective",
                                  "control_owner", "evidence_generated"]:
                        val = ctrl.get(field)
                        if val and isinstance(val, str):
                            all_texts.append(val)
                            positions.append(("ctrl", r_idx, o_idx, field, None))
                    for field in ["control_type", "execution_type", "frequency",
                                  "control_level", "residual_risk_if_failed"]:
                        val = ctrl.get(field)
                        if val and field in ENUM_TRANSLATIONS:
                            ctrl[field] = ENUM_TRANSLATIONS[field].get(val, val)
                    for k_idx, step in enumerate(ctrl.get("key_steps") or []):
                        if step and isinstance(step, str):
                            all_texts.append(step)
                            positions.append(("step", r_idx, o_idx, "key_steps", k_idx))

        if all_texts:
            translated = translate_texts_batch(all_texts, lang)
            for (kind, r_idx, o_idx, field, sub_idx), tr in zip(positions, translated):
                if kind == "req_title":
                    requirements_list[r_idx][field] = tr
                elif kind == "ob_text":
                    requirements_list[r_idx]["obligations"][o_idx][field] = tr
                elif kind == "ob_exp":
                    requirements_list[r_idx]["obligations"][o_idx][field] = tr
                elif kind == "ob_field":
                    requirements_list[r_idx]["obligations"][o_idx][field] = tr
                elif kind == "evidence":
                    requirements_list[r_idx]["obligations"][o_idx][field][sub_idx] = tr
                elif kind == "ctrl":
                    requirements_list[r_idx]["obligations"][o_idx]["control"][field] = tr
                elif kind == "step":
                    requirements_list[r_idx]["obligations"][o_idx]["control"][field][sub_idx] = tr

    return {
        "requirements": requirements_list,
        "summary": {
            "total_requirements": len(requirements_list),
            "total_obligations":  total_obligations,
            "by_match_status":    status_counts,
        },
    }


# ================================================================== #
#  STARTUP                                                             #
# ================================================================== #

@app.on_event("startup")
def start_scheduler():
    Thread(target=scheduler_loop, daemon=True).start()


# ================================================================== #
#  PIPELINE ENDPOINTS                                                  #
# ================================================================== #

@app.post("/schedule")
def schedule_pipeline(regulator: str, hour: int, minute: int):
    if regulator not in REGULATOR_PIPELINES:
        raise HTTPException(400, "Unknown regulator")
    scheduled_time = dtime(hour, minute)
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            MERGE pipeline_schedule AS target
            USING (SELECT ? AS regulator) AS src
            ON target.regulator = src.regulator
            WHEN MATCHED THEN UPDATE SET scheduled_time=?, status='PENDING'
            WHEN NOT MATCHED THEN INSERT (regulator, scheduled_time) VALUES (?, ?);
            """,
            regulator, scheduled_time, regulator, scheduled_time,
        )
        conn.commit()
    return {"success": True, "regulator": regulator, "scheduled_time": f"{hour:02d}:{minute:02d}"}


@app.post("/update-schedule")
def update_pipeline_schedule(payload: ScheduleUpdate):
    if payload.regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")
    return {
        "status": "success", "regulator": payload.regulator,
        "hour": payload.hour, "minute": payload.minute,
    }


@app.post("/trigger/full")
def trigger_full_pipeline():
    completed, errors = [], []
    for regulator, fn in REGULATOR_PIPELINES.items():
        try:
            fn()
            completed.append(regulator)
        except Exception as e:
            logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)
            errors.append({"regulator": regulator, "error": str(e)})
    return {
        "status":                "done" if not errors else "partial_failure",
        "completed_regulators":  completed,
        "errors":                errors,
        "completed_at":          datetime.utcnow().isoformat(),
    }


@app.post("/trigger/{regulator}")
def trigger_regulator_pipeline(regulator: str):
    # Capture all logs
    logs = []

    logs.append("=" * 80)
    logs.append(f"ENDPOINT CALLED: /trigger/{regulator}")
    logs.append("=" * 80)

    if regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")

    try:
        logs.append(f"Executing pipeline for {regulator}...")
        result = REGULATOR_PIPELINES[regulator]()

        logs.append(f"Result type: {type(result)}")
        logs.append(f"Result: {result}")

        return {
            "status": "done",
            "regulator": regulator,
            "completed_at": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        logs.append(f"ERROR: {e}")
        logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": f"{regulator} pipeline failed: {e}",
                "logs": logs
            }
        )

# ================================================================== #
#  REGULATION ENDPOINTS                                                #
# ================================================================== #

@app.get("/regulations/by-category/{category_id}")
def get_regulations_by_category(
    category_id: int,
    lang: str = Query("en"),
):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = f"GET /regulations/by-category/{category_id}"
            cached = _get_ar_cache(cache_key)
            if cached:
                return cached

        query = """
            SELECT id, regulator, source_system, category, title, document_url, document_html,
                   TRY_CONVERT(DATETIME, published_date, 103) AS published_date,
                   reference_no, department, doc_path, [year], source_page_url, extra_meta,
                   TRY_CAST(created_at AS DATETIME) AS created_at,
                   TRY_CAST(updated_at AS DATETIME) AS updated_at, compliancecategory_id
            FROM regulations WHERE compliancecategory_id = ?
        """
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [category_id])
            rows = cursor.fetchall()
            if not rows:
                return {"success": True, "category_id": category_id, "total": 0, "data": []}
            columns = [col[0] for col in cursor.description]
            data = [row_to_dict(row, columns) for row in rows]

        if lang == "ar":
            data = [translate_regulation(reg, lang) for reg in data]

        response = {"success": True, "lang": lang, "category_id": category_id, "total": len(data), "data": data}
        if lang == "ar":
            _set_ar_cache(cache_key, response)
        return response
    except Exception as e:
        logger.exception("Error fetching regulations by category")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/regulations/{regulator}")
def get_regulations_by_regulator(
    regulator: str,
    category_id: Optional[int] = Query(None),
    year: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    lang: str = Query("en"),
):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = (
                f"GET /regulations/{regulator}"
                f"?category_id={category_id}&year={year}&limit={limit}&offset={offset}"
            )
            cached = _get_ar_cache(cache_key)
            if cached:
                return Response(
                    content=json.dumps(cached, ensure_ascii=False),
                    media_type="application/json",
                )

        query = """
            SELECT r.id, r.regulator, r.source_system, r.category, r.title, r.document_url,
                   r.document_html, TRY_CONVERT(DATETIME, r.published_date, 103) AS published_date,
                   r.reference_no, r.department, r.doc_path, r.[year], r.source_page_url,
                   r.extra_meta, TRY_CAST(r.created_at AS DATETIME) AS created_at,
                   TRY_CAST(r.updated_at AS DATETIME) AS updated_at, r.compliancecategory_id,
                   cc.title AS category_title, cc.parentid AS category_parent_id, cc.type AS category_type
            FROM regulations r
            LEFT JOIN compliancecategory cc ON r.compliancecategory_id = cc.compliancecategory_id
            WHERE r.regulator = ?
        """
        params = [regulator.upper()]
        if category_id is not None:
            query += " AND r.compliancecategory_id = ?"
            params.append(category_id)
        if year is not None:
            query += " AND r.[year] = ?"
            params.append(year)
        query += " ORDER BY TRY_CONVERT(DATETIME, r.published_date, 103) DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([offset, limit])

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows     = cursor.fetchall()
            columns  = [col[0] for col in cursor.description]
            regulations = []
            for row in rows:
                reg_dict = row_to_dict(row, columns)
                if reg_dict.get("document_html"):
                    reg_dict["document_html"] = reg_dict["document_html"].replace('\\"', '"')
                if reg_dict.get("extra_meta"):
                    try:
                        reg_dict["extra_meta"] = json.loads(reg_dict["extra_meta"])
                        reg_dict["extra_meta"].pop("org_pdf_html", None)
                        reg_dict["extra_meta"].pop("org_pdf_text", None)
                    except Exception:
                        pass
                reg_dict["category_info"] = {
                    "id":        reg_dict.pop("compliancecategory_id", None),
                    "title":     reg_dict.pop("category_title", None),
                    "parent_id": reg_dict.pop("category_parent_id", None),
                    "type":      reg_dict.pop("category_type", None),
                }
                regulations.append(reg_dict)

            count_query  = "SELECT COUNT(*) FROM regulations WHERE regulator = ?"
            count_params = [regulator.upper()]
            if category_id is not None:
                count_query += " AND compliancecategory_id = ?"
                count_params.append(category_id)
            if year is not None:
                count_query += " AND [year] = ?"
                count_params.append(year)
            cursor.execute(count_query, count_params)
            total_count = cursor.fetchone()[0]

        if lang == "ar":
            regulations = [translate_regulation(reg, lang) for reg in regulations]

        response_data = {
            "success": True,
            "lang": lang,
            "data": regulations,
            "pagination": {
                "total":        total_count,
                "limit":        limit,
                "offset":       offset,
                "has_more":     (offset + limit) < total_count,
                "current_page": (offset // limit) + 1,
                "total_pages":  (total_count + limit - 1) // limit,
            },
        }

        if lang == "ar":
            _set_ar_cache(cache_key, response_data)

        return Response(
            content=json.dumps(response_data, ensure_ascii=False, default=str),
            media_type="application/json",
        )

    except Exception as e:
        logger.exception(f"Error fetching regulations for {regulator}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/regulation/{regulation_id}")
def get_regulation_detail(
    regulation_id: int,
    lang: str = Query("en"),
):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = f"GET /regulation/{regulation_id}"
            cached = _get_ar_cache(cache_key)
            if cached:
                return Response(
                    content=json.dumps(cached, ensure_ascii=False),
                    media_type="application/json",
                )

        query = """
            SELECT r.id, r.regulator, r.source_system, r.category, r.title, r.document_url,
                   r.document_html, TRY_CONVERT(DATETIME, r.published_date, 103) AS published_date,
                   r.reference_no, r.department, r.doc_path, r.[year], r.source_page_url,
                   r.extra_meta, TRY_CAST(r.created_at AS DATETIME) AS created_at,
                   TRY_CAST(r.updated_at AS DATETIME) AS updated_at, r.status, r.compliancecategory_id,
                   cc.title AS category_title, cc.parentid AS category_parent_id, cc.type AS category_type
            FROM regulations r
            LEFT JOIN compliancecategory cc ON r.compliancecategory_id = cc.compliancecategory_id
            WHERE r.id = ?
        """
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [regulation_id])
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Regulation not found")
            columns  = [col[0] for col in cursor.description]
            reg_dict = row_to_dict(row, columns)
            if reg_dict.get("document_html"):
                reg_dict["document_html"] = reg_dict["document_html"].replace('\\"', '"')
            if reg_dict.get("extra_meta"):
                try:
                    reg_dict["extra_meta"] = json.loads(reg_dict["extra_meta"])
                    reg_dict["extra_meta"].pop("org_pdf_text", None)
                    reg_dict["extra_meta"].pop("org_pdf_html", None)
                except Exception:
                    pass
            reg_dict["category_info"] = {
                "id":        reg_dict.pop("compliancecategory_id", None),
                "title":     reg_dict.pop("category_title", None),
                "parent_id": reg_dict.pop("category_parent_id", None),
                "type":      reg_dict.pop("category_type", None),
            }

        if lang == "ar":
            reg_dict = translate_regulation(reg_dict, lang)

        response_data = {"success": True, "lang": lang, "data": reg_dict}

        if lang == "ar":
            _set_ar_cache(cache_key, response_data)

        return Response(
            content=json.dumps(response_data, ensure_ascii=False, default=str),
            media_type="application/json",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching regulation {regulation_id}")
        raise HTTPException(status_code=500, detail=str(e))


# ================================================================== #
#  COMPLIANCE ANALYSIS ENDPOINTS                                       #
# ================================================================== #

# REPLACE this entire function in pipeline_api.py
# Find: @app.get("/compliance-analysis/{regulation_id}")

@app.get("/compliance-analysis/{regulation_id}")
def get_compliance_analysis_full(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)

    # ── Always return 200 — frontend must never get 404 on this endpoint ──
    # Even if regulation doesn't exist or has no analysis, return structured
    # response so the UI can show the regulation detail without crashing.

    regulation = repo.get_regulation_by_id(regulation_id)

    # Regulation not found — return 200 with empty shell
    if not regulation:
        return JSONResponse(
            status_code=200,
            content={
                "success":        False,
                "lang":           lang,
                "regulation_id":  regulation_id,
                "schema_version": "v2",
                "has_analysis":   False,
                "message":        f"Regulation {regulation_id} not found.",
                "regulation":     None,
                "requirements":   [],
                "summary": {
                    "total_requirements": 0,
                    "total_obligations":  0,
                    "by_match_status": {
                        "fully_matched": 0,
                        "partially_matched": 0,
                        "new": 0,
                    },
                },
            }
        )

    # Regulation exists but no analysis yet
    v2_rows = repo.get_compliance_analysis(regulation_id)
    if not v2_rows:
        return JSONResponse(
            status_code=200,
            content={
                "success":        True,
                "lang":           lang,
                "regulation_id":  regulation_id,
                "schema_version": "v2",
                "has_analysis":   False,
                "message": (
                    f"No compliance analysis found for regulation {regulation_id}. "
                    f"Run POST /trigger/full-analysis/{regulation_id} to generate it."
                ),
                "regulation": {
                    "id":             regulation.get("id"),
                    "title":          regulation.get("title"),
                    "regulator":      regulation.get("regulator"),
                    "source_system":  regulation.get("source_system"),
                    "category":       regulation.get("category"),
                    "published_date": str(regulation.get("published_date") or ""),
                    "reference_no":   regulation.get("reference_no"),
                    "document_url":   regulation.get("document_url"),
                    "source_page_url":regulation.get("source_page_url"),
                    "document_html":  regulation.get("document_html"),
                    "doc_path":       regulation.get("doc_path"),
                    "extra_meta":     regulation.get("extra_meta"),
                    "content_hash":   regulation.get("content_hash"),
                },
                "requirements":   [],
                "summary": {
                    "total_requirements": 0,
                    "total_obligations":  0,
                    "by_match_status": {
                        "fully_matched": 0,
                        "partially_matched": 0,
                        "new": 0,
                    },
                },
            }
        )

    # Has analysis — return full response
    if lang == "ar":
        cache_key = f"GET /compliance-analysis-full/{regulation_id}"
        cached    = _get_ar_cache(cache_key)
        if cached:
            return cached

    mapping_data = build_v2_full_analysis_response(regulation_id, lang)
    result = {
        "success":        True,
        "lang":           lang,
        "regulation_id":  regulation_id,
        "schema_version": "v2",
        "has_analysis":   True,
        "regulation": {
            "id":             regulation.get("id"),
            "title":          regulation.get("title"),
            "regulator":      regulation.get("regulator"),
            "source_system":  regulation.get("source_system"),
            "category":       regulation.get("category"),
            "published_date": str(regulation.get("published_date") or ""),
            "reference_no":   regulation.get("reference_no"),
            "document_url":   regulation.get("document_url"),
            "source_page_url":regulation.get("source_page_url"),
            "document_html":  regulation.get("document_html"),
            "doc_path":       regulation.get("doc_path"),
            "extra_meta":     regulation.get("extra_meta"),
            "content_hash":   regulation.get("content_hash"),
        },
        **mapping_data,
    }

    if lang == "ar":
        _set_ar_cache(cache_key, result)

    return result

# ================================================================== #
#  GAP ANALYSIS ENDPOINTS                                              #
# ================================================================== #

@app.post("/gap-analysis/single", response_model=GapAnalysisResponse, tags=["Gap Analysis"])
async def gap_analysis_single(
    regulation_id: int = Form(...),
    file: UploadFile = File(...),
    lang: str = Form("en"),
):
    lang = _validate_lang(lang)
    if not file.filename.lower().endswith((".pdf", ".docx", ".doc")):
        raise HTTPException(400, "Only PDF and DOCX files are supported")

    if lang == "ar":
        gap_cache_key = _gap_cache_key("/gap-analysis/single", regulation_id, file.filename)
        cached = _get_ar_cache(gap_cache_key)
        if cached:
            return cached

    uploaded_text = await _save_and_extract_file(file)
    if not uploaded_text or len(uploaded_text) < 50:
        raise HTTPException(422, "Could not extract sufficient text from uploaded file")

    session_id = repo.create_gap_session(file.filename, uploaded_text)
    summary    = _run_gap_for_regulation_v2(session_id, regulation_id, uploaded_text)
    summary.results = _enrich_results_with_controls_v2(summary.results, regulation_id)

    if lang == "ar":
        summary.results = _translate_v2_gap_results(summary.results, lang)
        summary = RegulationGapSummary(
            regulation_id=summary.regulation_id,
            results=summary.results,
            summary=summary.summary,
        )

    final_response = GapAnalysisResponse(
        session_id=session_id,
        uploaded_document_name=file.filename,
        regulations=[summary],
    )

    if lang == "ar":
        _set_ar_cache(gap_cache_key, final_response.dict())

    return final_response


@app.post("/gap-analysis/multi", tags=["Gap Analysis"])
async def gap_analysis_multi(
    regulation_ids: str = Form(..., description="Comma-separated regulation IDs"),
    file: UploadFile = File(...),
    lang: str = Form("en"),
):
    lang = _validate_lang(lang)
    if not file.filename.lower().endswith((".pdf", ".docx", ".doc")):
        raise HTTPException(400, "Only PDF and DOCX files are supported")
 
    try:
        reg_ids: List[int] = [int(rid.strip()) for rid in regulation_ids.split(",") if rid.strip()]
    except ValueError:
        raise HTTPException(400, "regulation_ids must be comma-separated integers")
    if not reg_ids:
        raise HTTPException(400, "At least one regulation_id is required")
 
    uploaded_text = await _save_and_extract_file(file)
    if not uploaded_text or len(uploaded_text) < 50:
        raise HTTPException(422, "Could not extract sufficient text from uploaded file")
 
    session_id = repo.create_gap_session(file.filename, uploaded_text)
    regulation_summaries: List[RegulationGapSummary] = []
    errors = []
 
    for reg_id in reg_ids:
        try:
            if lang == "ar":
                gap_cache_key = _gap_cache_key("/gap-analysis/multi", reg_id, file.filename)
                cached = _get_ar_cache(gap_cache_key)
                if cached:
                    regulation_summaries.append(RegulationGapSummary(**cached))
                    continue
 
            summary = _run_gap_for_regulation_v2(session_id, reg_id, uploaded_text)
            summary.results = _enrich_results_with_controls_v2(summary.results, reg_id)
 
            if lang == "ar":
                summary.results = _translate_v2_gap_results(summary.results, lang)
                summary = RegulationGapSummary(
                    regulation_id=summary.regulation_id,
                    results=summary.results,
                    summary=summary.summary,
                )
                _set_ar_cache(gap_cache_key, summary.dict())
 
            regulation_summaries.append(summary)
 
        except HTTPException as e:
            # Always stringify — dict detail causes frontend null.toString() crash
            detail = e.detail
            error_msg = detail if isinstance(detail, str) else json.dumps(detail)
            errors.append({
                "regulation_id": reg_id,
                "error": error_msg,
                "status_code": e.status_code,
            })
            logger.warning(f"[gap-multi] reg_id={reg_id} HTTP {e.status_code}: {error_msg}")
 
        except Exception as e:
            errors.append({"regulation_id": reg_id, "error": str(e), "status_code": 500})
            logger.error(f"[gap-multi] reg_id={reg_id} unexpected: {e}", exc_info=True)
 
    # Return 200 with errors array instead of raising 500
    # Raising 500 causes frontend RxJS to call .toString() on null → crash
    if not regulation_summaries:
        return JSONResponse(
            status_code=200,
            content={
                "session_id":             session_id,
                "uploaded_document_name": file.filename,
                "regulations":            [],
                "errors":                 errors,
                "success":                False,
                "message": (
                    f"Gap analysis failed for all {len(errors)} regulation(s). "
                    "Run POST /trigger/staged-analysis/{id} first to generate analysis."
                ),
            }
        )
 
    return JSONResponse(
        status_code=200,
        content={
            "session_id":             session_id,
            "uploaded_document_name": file.filename,
            "regulations":            [s.dict() for s in regulation_summaries],
            "errors":                 errors,
            "success":                True,
            "partial":                len(errors) > 0,
        }
    )
 


@app.post("/gap-analysis/multi-docs", tags=["Gap Analysis"])
async def gap_analysis_multi_docs(
    regulation_id: int = Form(...),
    files: List[UploadFile] = File(...),
    lang: str = Form("en"),
):
    lang = _validate_lang(lang)
    if not files:
        raise HTTPException(400, "No files uploaded")

    session_results, errors = [], []
    for file in files:
        if not file.filename.lower().endswith((".pdf", ".docx", ".doc")):
            errors.append({"file": file.filename, "error": "Unsupported file type"})
            continue
        try:
            if lang == "ar":
                gap_cache_key = _gap_cache_key("/gap-analysis/multi-docs", regulation_id, file.filename)
                cached = _get_ar_cache(gap_cache_key)
                if cached:
                    session_results.append(cached)
                    continue

            uploaded_text = await _save_and_extract_file(file)
            if len(uploaded_text) < 50:
                raise Exception("Too little text extracted")

            session_id = repo.create_gap_session(file.filename, uploaded_text)
            summary    = _run_gap_for_regulation_v2(session_id, regulation_id, uploaded_text)
            summary.results = _enrich_results_with_controls_v2(summary.results, regulation_id)

            if lang == "ar":
                summary.results = _translate_v2_gap_results(summary.results, lang)
                summary = RegulationGapSummary(
                    regulation_id=summary.regulation_id,
                    results=summary.results,
                    summary=summary.summary,
                )

            result_entry = {
                "file_name":  file.filename,
                "session_id": session_id,
                "summary":    summary.dict(),
            }
            if lang == "ar":
                _set_ar_cache(gap_cache_key, result_entry)
            session_results.append(result_entry)
        except Exception as e:
            errors.append({"file": file.filename, "error": str(e)})

    if not session_results:
        raise HTTPException(500, f"All documents failed: {errors}")

    return {
        "success":            True,
        "lang":               lang,
        "schema_version":     "v2",
        "regulation_id":      regulation_id,
        "documents_analyzed": session_results,
        "errors":             errors,
    }


@app.get("/gap-analysis/session/{session_id}", response_model=GapAnalysisResponse, tags=["Gap Analysis"])
def get_gap_session(session_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)
    if lang == "ar":
        cache_key = f"GET /gap-analysis/session/{session_id}"
        cached    = _get_ar_cache(cache_key)
        if cached:
            return cached

    result = repo.get_gap_results_by_session(session_id)
    if not result:
        raise HTTPException(404, f"No gap analysis session found for ID {session_id}")

    regulations = []
    for reg in result["regulations"]:
        results_list = reg["results"]
        if lang == "ar":
            results_list = [translate_gap_result(r, lang) for r in results_list]
        regulations.append(RegulationGapSummary(
            regulation_id=reg["regulation_id"],
            results=[GapResult(**r) for r in results_list],
            summary=reg["summary"],
        ))

    response = GapAnalysisResponse(
        session_id=result["session_id"],
        uploaded_document_name=result["uploaded_document_name"],
        regulations=regulations,
    )

    if lang == "ar":
        _set_ar_cache(cache_key, response.dict())

    return response


# ================================================================== #
#  CATEGORIES ENDPOINTS                                                #
# ================================================================== #

@app.get("/categories")
def get_categories(lang: str = Query("en")):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = "GET /categories"
            cached    = _get_ar_cache(cache_key)
            if cached:
                return cached

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT compliancecategory_id, title, parentid, type "
                "FROM compliancecategory ORDER BY parentid, title"
            )
            rows    = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            categories = [row_to_dict(row, columns) for row in rows]

        if lang == "ar":
            titles     = [c.get("title") or "" for c in categories]
            translated = translate_texts_batch(titles, lang)
            for cat, tr in zip(categories, translated):
                cat["title"] = tr

        categories_by_id = {cat["compliancecategory_id"]: cat for cat in categories}
        root_categories  = []
        for cat in categories:
            cat["children"] = []
            if cat["parentid"] is None:
                root_categories.append(cat)
            else:
                parent = categories_by_id.get(cat["parentid"])
                if parent:
                    parent["children"].append(cat)

        response = {
            "success": True, "lang": lang,
            "data": {
                "all_categories": categories,
                "hierarchy":      root_categories,
                "total_count":    len(categories),
            },
        }
        if lang == "ar":
            _set_ar_cache(cache_key, response)
        return response
    except Exception as e:
        logger.exception("Error fetching categories")
        raise HTTPException(500, str(e))


@app.get("/categories/roots")
def get_root_categories_only(lang: str = Query("en")):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = "GET /categories/roots"
            cached    = _get_ar_cache(cache_key)
            if cached:
                return cached

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT compliancecategory_id, title, parentid, type "
                "FROM compliancecategory WHERE parentid IS NULL"
            )
            rows    = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            root_categories = [row_to_dict(row, columns) for row in rows]

        if lang == "ar":
            titles     = [c.get("title") or "" for c in root_categories]
            translated = translate_texts_batch(titles, lang)
            for cat, tr in zip(root_categories, translated):
                cat["title"] = tr

        response = {"success": True, "lang": lang, "data": root_categories, "total": len(root_categories)}
        if lang == "ar":
            _set_ar_cache(cache_key, response)
        return response
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/categories/root")
def get_root_categories_with_children(lang: str = Query("en")):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = "GET /categories/root"
            cached    = _get_ar_cache(cache_key)
            if cached:
                return cached

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT compliancecategory_id, title, parentid, type FROM compliancecategory")
            rows    = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            categories = [row_to_dict(row, columns) for row in rows]

        if lang == "ar":
            titles     = [c.get("title") or "" for c in categories]
            translated = translate_texts_batch(titles, lang)
            for cat, tr in zip(categories, translated):
                cat["title"] = tr

        for c in categories:
            c["children"] = []
        root_categories = [c for c in categories if c["parentid"] is None]
        for root in root_categories:
            root["children"] = [
                c for c in categories
                if c["parentid"] == root["compliancecategory_id"]
            ]

        response = {"success": True, "lang": lang, "data": root_categories, "total_root_categories": len(root_categories)}
        if lang == "ar":
            _set_ar_cache(cache_key, response)
        return response
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/categories/children/{parent_id}")
def get_children(parent_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = f"GET /categories/children/{parent_id}"
            cached    = _get_ar_cache(cache_key)
            if cached:
                return cached

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT compliancecategory_id, title, parentid, type "
                "FROM compliancecategory WHERE parentid = ?",
                parent_id,
            )
            rows    = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            children = [row_to_dict(row, columns) for row in rows]

        if lang == "ar":
            titles     = [c.get("title") or "" for c in children]
            translated = translate_texts_batch(titles, lang)
            for cat, tr in zip(children, translated):
                cat["title"] = tr

        response = {"success": True, "lang": lang, "data": children}
        if lang == "ar":
            _set_ar_cache(cache_key, response)
        return response
    except Exception as e:
        raise HTTPException(500, str(e))


# ================================================================== #
#  STATUS ENDPOINTS                                                    #
# ================================================================== #

# ── 4. Status endpoints — add lang param for consistency ─────────────────────
 
@app.get("/status/full")
def get_full_status(lang: str = Query("en")):
    # Status values are technical strings (RUNNING/DONE/FAILED) — not translated
    results = {}
    for regulator in REGULATOR_PIPELINES.keys():
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT TOP 1 status FROM pipeline_status WHERE regulator=? ORDER BY id DESC",
                regulator,
            )
            row = cursor.fetchone()
            results[regulator] = row[0] if row else "NOT_STARTED"
    return {
        "pipeline_status": results,
        "timestamp":       datetime.utcnow().isoformat(),
    }

@app.get("/status/{regulator}")
def get_regulator_status(regulator: str, lang: str = Query("en")):
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TOP 1 status, started_at, finished_at, error "
            "FROM pipeline_status WHERE regulator=? ORDER BY id DESC",
            regulator,
        )
        row = cursor.fetchone()
    if not row:
        return {"regulator": regulator, "status": "NOT_STARTED"}
    return {
        "regulator":   regulator,
        "status":      row[0],
        "started_at":  serialize_datetime(row[1]),
        "finished_at": serialize_datetime(row[2]),
        "error":       row[3],
    }
 

@app.post("/update-status/compliance-analysis")
def update_compliance_analysis_status(payload: ComplianceStatusUpdate):
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE compliance_analysis
                SET status = ?
                WHERE regulation_id = ? AND requirement_id = ?
                """,
                payload.status, payload.regulation_id, payload.requirement_id,
            )
            conn.commit()
            if cursor.rowcount == 0:
                raise HTTPException(
                    404,
                    f"No record found for regulation_id={payload.regulation_id} "
                    f"and requirement_id={payload.requirement_id}",
                )
        return {
            "success":         True,
            "regulation_id":   payload.regulation_id,
            "requirement_id":  payload.requirement_id,
            "status":          payload.status,
            "message":         "Status updated successfully",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error updating compliance_analysis status")
        raise HTTPException(500, str(e))


@app.post("/update-status/compliancecategory")
def update_compliancecategory_status(payload: StatusUpdate):
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE compliancecategory SET status = ? WHERE compliancecategory_id = ?",
                payload.status, payload.record_id,
            )
            conn.commit()
            if cursor.rowcount == 0:
                raise HTTPException(404, f"Record not found: id={payload.record_id}")
        return {"success": True, "table": "compliancecategory", "record_id": payload.record_id, "status": payload.status}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/update-status/regulations")
def update_regulations_status(payload: StatusUpdate):
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE regulations SET status = ? WHERE id = ?",
                payload.status, payload.record_id,
            )
            conn.commit()
            if cursor.rowcount == 0:
                raise HTTPException(404, f"Record not found: id={payload.record_id}")
        return {"success": True, "table": "regulations", "record_id": payload.record_id, "status": payload.status}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


# ================================================================== #
#  STEP 2 — REQUIREMENT MATCHING ENDPOINTS                             #
# ================================================================== #

@app.post("/trigger/requirement-matching/{regulation_id}", tags=["Step 2"])
def trigger_requirement_matching_v2(regulation_id: int):
    """
    Trigger requirement matching for a regulation.
    Reads from the unified compliance_analysis table (works for all regulators).
    Passes version_id through to sama_requirement_mapping for CBB rows.
    """
    rows = repo.get_compliance_analysis(regulation_id)
    if not rows:
        raise HTTPException(
            404,
            f"No analysis for regulation {regulation_id}. "
            f"Run POST /trigger/staged-analysis/{regulation_id} first.",
        )

    extracted_requirements = []
    for row in rows:
        s2 = row.get("stage2_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except Exception:
                s2 = {}
        for ob in s2.get("normalized_obligations", []):
            extracted_requirements.append({
                "requirement_text": ob["obligation_text"],
                "department":       "",
                "risk_level":       ob.get("criticality", "Medium"),
                "controls":         [],
                "kpis":             [],
                "_obligation_id":   ob["obligation_id"],
                "_requirement_id":  row["requirement_id"],
                # version_id is None for SAMA/SBP, populated for CBB
                "_version_id":      row.get("version_id"),
            })

    if not extracted_requirements:
        raise HTTPException(404, f"No obligations found for regulation {regulation_id}")

    req_text_to_v2_meta = {
        r["requirement_text"]: {
            "obligation_id":  r["_obligation_id"],
            "requirement_id": r["_requirement_id"],
            "version_id":     r["_version_id"],
        }
        for r in extracted_requirements
    }

    existing_requirements  = repo.get_all_compliance_requirements()
    existing_controls      = repo.get_all_demo_controls()
    existing_kpis          = repo.get_all_demo_kpis()
    linked_controls_by_req = repo.get_linked_controls_by_requirement()
    linked_kpis_by_req     = repo.get_linked_kpis_by_requirement()

    match_results = requirement_matcher.match_requirements(
        regulation_id=regulation_id,
        extracted_requirements=extracted_requirements,
        existing_requirements=existing_requirements,
        existing_controls=existing_controls,
        existing_kpis=existing_kpis,
        linked_controls_by_req=linked_controls_by_req,
        linked_kpis_by_req=linked_kpis_by_req,
    )

    requirement_mappings   = match_results["requirement_mappings"]
    control_links          = match_results["control_links"]
    kpi_links              = match_results["kpi_links"]
    new_controls_to_insert = match_results["new_controls_to_insert"]
    new_kpis_to_insert     = match_results["new_kpis_to_insert"]

    for mapping in requirement_mappings:
        meta = req_text_to_v2_meta.get(mapping["extracted_requirement_text"], {})
        mapping["obligation_id"]  = meta.get("obligation_id")
        mapping["requirement_id"] = meta.get("requirement_id")
        # Each mapping carries the version_id of the analysis row it came from
        mapping["_version_id"]    = meta.get("version_id")

    if requirement_mappings:
        # Group by version_id and store each batch
        from itertools import groupby
        sorted_mappings = sorted(requirement_mappings, key=lambda m: (m.get("_version_id") or 0))
        for version_id, group in groupby(sorted_mappings, key=lambda m: m.get("_version_id")):
            batch = list(group)
            repo.store_requirement_mappings(batch, version_id=version_id)

    partially_matched_ids = [
        m["matched_requirement_id"]
        for m in requirement_mappings
        if m["match_status"] == "partially_matched" and m.get("matched_requirement_id")
    ]
    if partially_matched_ids:
        repo.flag_partially_matched_requirements(partially_matched_ids)

    new_req_mappings = [m for m in requirement_mappings if m["match_status"] == "new"]
    for i, mapping in enumerate(new_req_mappings):
        try:
            req_text  = mapping["extracted_requirement_text"]
            title     = req_text[:100].strip() + ("..." if len(req_text) > 100 else "")
            new_req_id = repo.insert_new_suggested_requirement({
                "title":       title,
                "description": req_text,
                "ref_key":     f"V2-AUTO-{regulation_id}-{i}",
                "ref_no":      f"REG-{regulation_id}",
            })
            for ctrl in new_controls_to_insert:
                if ctrl.get("_req_id") is None:
                    ctrl["_req_id"] = new_req_id
            for kpi in new_kpis_to_insert:
                if kpi.get("_req_id") is None:
                    kpi["_req_id"] = new_req_id
        except Exception as e:
            logger.error(f"[matching] Failed to insert new suggested requirement: {e}")

    if control_links:
        repo.store_control_links(control_links)
    if kpi_links:
        repo.store_kpi_links(kpi_links)

    for ctrl in new_controls_to_insert:
        try:
            new_ctrl_id = repo.insert_new_suggested_control({
                "title": ctrl["title"], "description": ctrl["description"],
                "control_key": ctrl["control_key"],
            })
            req_id = ctrl.get("_req_id")
            if req_id:
                repo.store_control_links([{
                    "compliancerequirement_id": req_id, "control_id": new_ctrl_id,
                    "match_status": "new", "match_explanation": ctrl.get("_explanation", ""),
                    "regulation_id": regulation_id,
                }])
        except Exception as e:
            logger.error(f"[matching] Failed to insert new suggested control: {e}")

    for kpi in new_kpis_to_insert:
        try:
            new_kpi_id = repo.insert_new_suggested_kpi({
                "title": kpi["title"], "description": kpi["description"],
                "kisetup_key": kpi["kisetup_key"], "formula": kpi.get("formula", ""),
            })
            req_id = kpi.get("_req_id")
            if req_id:
                repo.store_kpi_links([{
                    "compliancerequirement_id": req_id, "kisetup_id": new_kpi_id,
                    "match_status": "new", "match_explanation": kpi.get("_explanation", ""),
                    "regulation_id": regulation_id,
                }])
        except Exception as e:
            logger.error(f"[matching] Failed to insert new suggested KPI: {e}")

    _invalidate_ar_cache(regulation_id)

    return {
        "success":               True,
        "regulation_id":         regulation_id,
        "schema_version":        "v2",
        "obligations_processed": len(extracted_requirements),
        "mappings": [
            {
                "extracted_requirement_text": m["extracted_requirement_text"],
                "obligation_id":              m.get("obligation_id"),
                "requirement_id":             m.get("requirement_id"),
                "match_status":               m["match_status"],
                "matched_requirement_id":     m.get("matched_requirement_id"),
                "match_explanation":          m.get("match_explanation"),
            }
            for m in requirement_mappings
        ],
        "summary": {
            "requirements": {
                "total":             len(requirement_mappings),
                "fully_matched":     sum(1 for m in requirement_mappings if m["match_status"] == "fully_matched"),
                "partially_matched": sum(1 for m in requirement_mappings if m["match_status"] == "partially_matched"),
                "new":               sum(1 for m in requirement_mappings if m["match_status"] == "new"),
            },
            "controls": {"new_links_added": len(control_links), "new_controls_created": len(new_controls_to_insert)},
            "kpis":     {"new_links_added": len(kpi_links),     "new_kpis_created":     len(new_kpis_to_insert)},
        },
    }


@app.get("/requirement-mapping/{regulation_id}", tags=["Step 2"])
def get_requirement_mapping(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)
    if lang == "ar":
        cache_key = f"GET /requirement-mapping/{regulation_id}"
        cached    = _get_ar_cache(cache_key)
        if cached:
            return cached

    results = repo.get_requirement_mappings_by_regulation(regulation_id)
    if not results:
        raise HTTPException(200, f"No requirement mappings found for regulation {regulation_id}.")

    if lang == "ar":
        results = [translate_compliance_requirement(r, lang) for r in results]

    summary = {
        "fully_matched":     sum(1 for r in results if r.get("match_status") == "fully_matched"),
        "partially_matched": sum(1 for r in results if r.get("match_status") == "partially_matched"),
        "new":               sum(1 for r in results if r.get("match_status") == "new"),
        "total":             len(results),
    }
    response = {
        "success": True, "lang": lang,
        "regulation_id": regulation_id,
        "summary": summary, "mappings": results,
    }
    if lang == "ar":
        _set_ar_cache(cache_key, response)
    return response


@app.get("/control-mapping/{regulation_id}", tags=["Step 2"])
def get_control_mapping(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)
    if lang == "ar":
        cache_key = f"GET /control-mapping/{regulation_id}"
        cached    = _get_ar_cache(cache_key)
        if cached:
            return cached

    results = repo.get_control_links_by_regulation(regulation_id)
    if not results:
        raise HTTPException(200, f"No control links found for regulation {regulation_id}.")

    if lang == "ar":
        text_fields = ["control_title", "control_description", "MATCH_EXPLANATION"]
        all_texts, positions = [], []
        for i, r in enumerate(results):
            for f in text_fields:
                if r.get(f):
                    all_texts.append(r[f])
                    positions.append((i, f))
        if all_texts:
            translated = translate_texts_batch(all_texts, lang)
            for (i, f), tr in zip(positions, translated):
                results[i][f] = tr

    summary = {
        "fully_matched":     sum(1 for r in results if r["MATCH_STATUS"] == "fully_matched"),
        "partially_matched": sum(1 for r in results if r["MATCH_STATUS"] == "partially_matched"),
        "new":               sum(1 for r in results if r["MATCH_STATUS"] == "new"),
        "ai_suggested":      sum(1 for r in results if r.get("is_suggested") == 1),
        "total":             len(results),
    }
    response = {
        "success": True, "lang": lang,
        "regulation_id": regulation_id,
        "summary": summary, "control_links": results,
    }
    if lang == "ar":
        _set_ar_cache(cache_key, response)
    return response


@app.get("/kpi-mapping/{regulation_id}", tags=["Step 2"])
def get_kpi_mapping(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)
    if lang == "ar":
        cache_key = f"GET /kpi-mapping/{regulation_id}"
        cached    = _get_ar_cache(cache_key)
        if cached:
            return cached

    results = repo.get_kpi_links_by_regulation(regulation_id)
    if not results:
        raise HTTPException(200, f"No KPI links found for regulation {regulation_id}.")

    if lang == "ar":
        text_fields = ["kpi_title", "kpi_description", "MATCH_EXPLANATION"]
        all_texts, positions = [], []
        for i, r in enumerate(results):
            for f in text_fields:
                if r.get(f):
                    all_texts.append(r[f])
                    positions.append((i, f))
        if all_texts:
            translated = translate_texts_batch(all_texts, lang)
            for (i, f), tr in zip(positions, translated):
                results[i][f] = tr

    summary = {
        "fully_matched":     sum(1 for r in results if r["MATCH_STATUS"] == "fully_matched"),
        "partially_matched": sum(1 for r in results if r["MATCH_STATUS"] == "partially_matched"),
        "new":               sum(1 for r in results if r["MATCH_STATUS"] == "new"),
        "ai_suggested":      sum(1 for r in results if r.get("is_suggested") == 1),
        "total":             len(results),
    }
    response = {
        "success": True, "lang": lang,
        "regulation_id": regulation_id,
        "summary": summary, "kpi_links": results,
    }
    if lang == "ar":
        _set_ar_cache(cache_key, response)
    return response


# ================================================================== #
#  V2 STAGED ANALYSIS ENDPOINTS                                        #
# ================================================================== #

@app.post("/trigger/staged-analysis/{regulation_id}", tags=["V2 Staged Analysis"])
def trigger_staged_analysis(regulation_id: int, force: bool = Query(False)):
    """
    Run 4-stage LLM analysis and store results in compliance_analysis.
    Works for ALL regulators — unified table, no regulator-specific branching here.
    For CBB, fetches content from regulation_versions.
    Use ?force=true to re-run and overwrite existing analysis.
    """
    if not force:
        existing_rows = repo.get_compliance_analysis(regulation_id)
        if existing_rows:
            return {
                "success": True,
                "regulation_id": regulation_id,
                "skipped": True,
                "reason": "Analysis already exists. Use ?force=true to re-run.",
                "existing_count": len(existing_rows),
                "next_step": f"POST /trigger/requirement-matching/{regulation_id}",
            }

    regulation = repo.get_regulation_by_id(regulation_id)
    if not regulation:
        raise HTTPException(404, f"Regulation {regulation_id} not found")

    extra_meta = regulation.get("extra_meta") or {}
    if isinstance(extra_meta, str):
        try:
            extra_meta = json.loads(extra_meta)
        except Exception:
            extra_meta = {}

    text_content = None
    content_type = None

    # ── FOR CBB: Check regulation_versions FIRST ──
    if regulation.get("regulator") == "Central Bank of Bahrain":
        logger.info(f"CBB regulation detected, fetching from regulation_versions...")
        version_data = repo.get_active_regulation_version(regulation_id)

        if version_data:
            content_text = (version_data.get("content_text") or "").strip()
            content_html = (version_data.get("content_html") or "").strip()

            if len(content_text) >= 200:
                text_content = content_text
                content_type = "html"
                logger.info(f"✓ Using content_text from regulation_versions ({len(text_content)} chars)")
            elif len(content_html) >= 200:
                text_content = content_html
                content_type = "html"
                logger.info(f"✓ Using content_html from regulation_versions ({len(text_content)} chars)")

    # ── FOR SAMA/SBP/SECP: Check extra_meta and document_html ──
    if not text_content:
        org_pdf_text = extra_meta.get("org_pdf_text")
        if org_pdf_text and len(org_pdf_text) > 200:
            text_content = org_pdf_text
            content_type = "pdf_text"

    if not text_content:
        doc_html = regulation.get("document_html")
        if doc_html and len(doc_html) > 200:
            text_content = doc_html
            content_type = "html"

    if not text_content:
        raise HTTPException(422, f"No extractable text for regulation {regulation_id}.")

    normalizer = LLMAnalyzer()
    try:
        clean_text = normalizer.normalize_input_text(text_content, content_type=content_type)
    except Exception as e:
        raise HTTPException(422, f"Text normalization failed: {e}")

    if len(clean_text) < 200:
        raise HTTPException(422, f"Text too short ({len(clean_text)} chars).")

    raw_date = regulation.get("published_date")
    published_date = str(raw_date)[:10] if raw_date else ""

    rows = staged_analyzer.analyze(
        text=clean_text,
        regulation_id=regulation_id,
        document_title=regulation.get("title", "Untitled"),
        regulator=regulation.get("regulator") or "",
        reference=regulation.get("reference_no") or "",
        publication_date=published_date,
    )

    if not rows:
        raise HTTPException(422, f"Pipeline extracted 0 requirements for regulation {regulation_id}.")

    # If force=true, clear the existing analysis first
    if force:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM compliance_analysis WHERE regulation_id = ? AND is_current = 1",
                [regulation_id],
            )
            conn.commit()

    # Get version_id from regulation_versions for CBB
    version_id = None
    if regulation.get("regulator") == "Central Bank of Bahrain":
        version_data = repo.get_active_regulation_version(regulation_id)
        if version_data:
            version_id = version_data.get("version_id")

    # Store with version_id (populated for CBB, None for others)
    repo.store_analysis(rows, version_id=version_id)
    _invalidate_ar_cache(regulation_id)

    exec_counts, crit_counts = {}, {}
    for r in rows:
        ec = r.get("execution_category") or "Unknown"
        cr = r.get("criticality") or "Unknown"
        exec_counts[ec] = exec_counts.get(ec, 0) + 1
        crit_counts[cr] = crit_counts.get(cr, 0) + 1

    return {
        "success": True,
        "regulation_id": regulation_id,
        "document_title": regulation.get("title"),
        "text_length": len(clean_text),
        "content_type": content_type,
        "version_id": version_id,  # Add this to show which version was used
        "analysis": {
            "requirements_extracted": len(rows),
            "by_execution_category": exec_counts,
            "by_criticality": crit_counts,
        },
        "next_step": f"POST /trigger/requirement-matching/{regulation_id}",
    }
@app.get("/compliance-analysis-v2/{regulation_id}", tags=["V2 Staged Analysis"])
def get_compliance_analysis_v2(
    regulation_id: int,
    execution_category: Optional[str] = Query(None),
    criticality: Optional[str] = Query(None),
    lang: str = Query("en"),
):
    lang = _validate_lang(lang)
    # Unified read
    rows = repo.get_compliance_analysis(regulation_id)

    if not rows:
        reg = repo.get_regulation_by_id(regulation_id)
        if not reg:
            raise HTTPException(404, f"Regulation {regulation_id} not found")
        return {
            "success": True, "regulation_id": regulation_id, "schema_version": "v2",
            "has_analysis": False,
            "message": f"No analysis. Run POST /trigger/staged-analysis/{regulation_id}.",
            "requirements": [], "summary": {},
        }

    if execution_category:
        rows = [r for r in rows if r.get("execution_category") == execution_category]
    if criticality:
        rows = [r for r in rows if r.get("criticality") == criticality]

    table_rows = []
    for row in rows:
        s2 = row.get("stage2_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except Exception:
                s2 = {}
        table_rows.append({
            "id":                 row["id"],
            "requirement_id":     row.get("requirement_id"),
            "requirement_title":  row.get("requirement_title"),
            "execution_category": row.get("execution_category"),
            "criticality":        row.get("criticality"),
            "obligation_type":    row.get("obligation_type"),
            "obligation_count":   len(s2.get("normalized_obligations", [])),
            # version_id present for CBB, None for others
            "version_id":         row.get("version_id"),
            "status":             row.get("status"),
            "created_at":         serialize_datetime(row.get("created_at")),
        })

    all_rows = repo.get_compliance_analysis(regulation_id)
    exec_counts, crit_counts = {}, {}
    for r in all_rows:
        ec = r.get("execution_category") or "Unknown"
        cr = r.get("criticality") or "Unknown"
        exec_counts[ec] = exec_counts.get(ec, 0) + 1
        crit_counts[cr] = crit_counts.get(cr, 0) + 1

    return {
        "success":        True,
        "lang":           lang,
        "regulation_id":  regulation_id,
        "schema_version": "v2",
        "has_analysis":   True,
        "total":          len(all_rows),
        "filtered_total": len(table_rows),
        "requirements":   table_rows,
        "summary": {
            "by_execution_category": exec_counts,
            "by_criticality":        crit_counts,
        },
    }


@app.get(
    "/compliance-analysis-v2/{regulation_id}/requirement/{requirement_id}",
    tags=["V2 Staged Analysis"],
)
def get_requirement_detail_v2(
    regulation_id: int, requirement_id: str, lang: str = Query("en")
):
    lang = _validate_lang(lang)
    rows = repo.get_compliance_analysis(regulation_id)
    row  = next((r for r in rows if r.get("requirement_id") == requirement_id), None)

    if not row:
        raise HTTPException(
            404,
            f"Requirement '{requirement_id}' not found for regulation {regulation_id}. "
            f"Available: {[r.get('requirement_id') for r in rows]}",
        )

    def _parse(val):
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return {}
        return val or {}

    s2 = _parse(row.get("stage2_json"))
    s3 = _parse(row.get("stage3_json"))

    obligations = s2.get("normalized_obligations", [])
    control_map = {
        ob["obligation_id"]: ob.get("control")
        for ob in s3.get("obligations", [])
        if ob.get("obligation_id")
    }
    enriched_obligations = [
        {**ob, "control": control_map.get(ob.get("obligation_id"))}
        for ob in obligations
    ]

    return {
        "success":            True,
        "lang":               lang,
        "regulation_id":      regulation_id,
        "requirement_id":     row.get("requirement_id"),
        "requirement_title":  row.get("requirement_title"),
        "execution_category": row.get("execution_category"),
        "criticality":        row.get("criticality"),
        "obligation_type":    row.get("obligation_type"),
        "version_id":         row.get("version_id"),
        "obligations":        enriched_obligations,
        "obligations_total":  len(enriched_obligations),
        "controls_designed":  sum(1 for ob in enriched_obligations if ob.get("control")),
        "status":             row.get("status"),
        "created_at":         serialize_datetime(row.get("created_at")),
    }


@app.get(
    "/compliance-analysis-v2/{regulation_id}/executive-summary",
    tags=["V2 Staged Analysis"],
)
def get_executive_summary_v2(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)
    md = repo.get_stage4_executive_summary(regulation_id)
    if not md:
        rows = repo.get_compliance_analysis(regulation_id)
        if not rows:
            raise HTTPException(404, f"No analysis for regulation {regulation_id}.")
        raise HTTPException(404, "Analysis exists but executive summary is empty.")
 
    if lang == "ar":
        cache_key = f"GET /compliance-analysis-v2/{regulation_id}/executive-summary"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached
        translated_list = translate_texts_batch([md], lang)
        md_out = translated_list[0] if translated_list else md
        result = {
            "success":              True,
            "lang":                 lang,
            "regulation_id":        regulation_id,
            "executive_summary_md": md_out,
            "length_chars":         len(md_out),
        }
        _set_ar_cache(cache_key, result)
        return result
 
    return {
        "success":              True,
        "lang":                 lang,
        "regulation_id":        regulation_id,
        "executive_summary_md": md,
        "length_chars":         len(md),
    }
 


# ================================================================== #
#  VERSION HISTORY ENDPOINTS                                           #
# ================================================================== #

@app.get("/regulation/{regulation_id}/versions", tags=["Content Versions"])
def get_regulation_versions(
        regulation_id: int,
        include_details: bool = Query(True, description="Include full regulation details for each version"),
        lang: str = Query("en")
):
    lang = _validate_lang(lang)
    """
    Get content version history for a CBB regulation.

    Returns all versions from regulation_versions table with their content snapshots.

    Use ?include_details=true to get full regulation data (document_html, extra_meta, etc.)
    for each version, similar to the /regulation/{id} endpoint.
    """
    regulation = repo.get_regulation_by_id(regulation_id)
    if not regulation:
        raise HTTPException(404, f"Regulation {regulation_id} not found")

    regulator = regulation.get("regulator")

    if regulator != "Central Bank of Bahrain":
        return {
            "success": True,
            "lang": lang,
            "regulation_id": regulation_id,
            "title": regulation.get("title"),
            "regulator": regulator,
            "total_versions": 0,
            "versions": [],
            "note": "Content versioning is only available for CBB regulations."
        }
    if lang == "ar":
        cache_key = f"GET /regulation/{regulation_id}/versions?include_details={include_details}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    # Get all versions
    versions_query = """
        SELECT 
            version_id,
            regulation_id,
            regulator,
            content_text,
            content_html,
            updated_date,
            created_at,
            change_summary,
            status
        FROM regulation_versions
        WHERE regulation_id = ?
        ORDER BY created_at DESC, version_id DESC
    """

    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(versions_query, [regulation_id])
        cols = [c[0] for c in cursor.description]
        version_rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

    versions = []

    for v in version_rows:
        if include_details:
            # Return full details including content
            version_data = {
                "version_id": v["version_id"],
                "regulation_id": v["regulation_id"],
                "regulator": v["regulator"],
                "updated_date": serialize_datetime(v["updated_date"]) if v["updated_date"] else None,
                "created_at": serialize_datetime(v["created_at"]),
                "change_summary": v["change_summary"],
                "status": v["status"],
                # Full content
                "content_text": v["content_text"],
                "content_html": v["content_html"],
                "content_text_length": len(v["content_text"] or ""),
                "content_html_length": len(v["content_html"] or ""),
                # Add regulation metadata (from main table)
                "regulation_details": {
                    "title": regulation.get("title"),
                    "document_url": regulation.get("document_url"),
                    "source_page_url": regulation.get("source_page_url"),
                    "category": regulation.get("category"),
                    "published_date": serialize_datetime(regulation.get("published_date")),
                    "reference_no": regulation.get("reference_no"),
                    "doc_path": regulation.get("doc_path")
                }
            }
        else:
            # Return summary only (no content)
            version_data = {
                "version_id": v["version_id"],
                "regulation_id": v["regulation_id"],
                "regulator": v["regulator"],
                "updated_date": serialize_datetime(v["updated_date"]) if v["updated_date"] else None,
                "created_at": serialize_datetime(v["created_at"]),
                "change_summary": v["change_summary"],
                "status": v["status"]
            }

        versions.append(version_data)

    if lang == "ar":
        all_texts = []
        positions = []

        for i, v in enumerate(versions):
            # Translate change_summary
            if v.get("change_summary"):
                all_texts.append(v["change_summary"])
                positions.append(("version", i, "change_summary"))

            # Translate regulation details if included
            if include_details and v.get("regulation_details"):
                if v["regulation_details"].get("title"):
                    all_texts.append(v["regulation_details"]["title"])
                    positions.append(("reg_detail", i, "title"))
                if v["regulation_details"].get("category"):
                    all_texts.append(v["regulation_details"]["category"])
                    positions.append(("reg_detail", i, "category"))

        if all_texts:
            translated = translate_texts_batch(all_texts, lang)
            for (kind, i, field), tr in zip(positions, translated):
                if kind == "version":
                    versions[i][field] = tr
                elif kind == "reg_detail":
                    versions[i]["regulation_details"][field] = tr

    response = {
        "success": True,
        "lang": lang,
        "regulation_id": regulation_id,
        "title": regulation.get("title"),
        "regulator": regulator,
        "include_details": include_details,
        "total_versions": len(versions),
        "versions": versions,
        "note": None
    }

    if lang == "ar":
        _set_ar_cache(cache_key, response)

    return response

@app.get("/regulation/{regulation_id}/analysis-versions", tags=["Analysis Versions"])
def get_analysis_versions(
    regulation_id: int,
    include_details: bool = Query(True, description="Include full requirement details (obligations, controls, KPIs)"),
    lang: str = Query("en")
):
    """
    Get version history for a regulation's compliance analysis.

    - current: Active analysis (compliance_analysis table)
    - version_history: Archived analysis (compliance_analysis_versions table)

    Use ?include_details=true to get full requirement details with obligations.
    """
    lang = _validate_lang(lang)

    regulation = repo.get_regulation_by_id(regulation_id)
    if not regulation:
        raise HTTPException(404, f"Regulation {regulation_id} not found")

    regulator = regulation.get("regulator")
    is_cbb = (regulator == "Central Bank of Bahrain")

    if lang == "ar":
        cache_key = f"GET /regulation/{regulation_id}/analysis-versions?include_details={include_details}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    # ── Get current analysis ────────────────────────────────────────────────
    current_rows = repo.get_compliance_analysis(regulation_id)
    current_summary = None

    if current_rows:
        version_id = current_rows[0].get("version_id")

        if include_details:
            current_requirements = []
            for row in current_rows:
                s2 = row.get("stage2_json") or {}
                if isinstance(s2, str):
                    try:
                        s2 = json.loads(s2)
                    except Exception as e:
                        logger.warning(f"Failed to parse stage2_json: {e}")
                        s2 = {}

                if not isinstance(s2, dict):
                    s2 = {}

                obligations = s2.get("normalized_obligations", [])

                current_requirements.append({
                    "requirement_id": row.get("requirement_id"),
                    "requirement_title": row.get("requirement_title"),
                    "execution_category": row.get("execution_category"),
                    "criticality": row.get("criticality"),
                    "obligation_type": row.get("obligation_type"),
                    "obligation_count": len(obligations),
                    "obligations": obligations,
                    "status": row.get("status"),
                    "created_at": serialize_datetime(row.get("created_at"))
                })

            current_summary = {
                "version_id": version_id,
                "status": "active",
                "requirement_count": len(current_rows),
                "requirements": current_requirements,
                "created_at": serialize_datetime(current_rows[0].get("created_at")),
                "label": "Current (active — shown in all analysis endpoints)",
                "note": "version_id is set for CBB regulations, null for SAMA/SBP/SECP."
            }
        else:
            current_summary = {
                "version_id": version_id,
                "status": "active",
                "requirement_count": len(current_rows),
                "created_at": serialize_datetime(current_rows[0].get("created_at")),
                "label": "Current (active — shown in all analysis endpoints)",
                "note": "version_id is set for CBB regulations, null for SAMA/SBP/SECP."
            }

    # ── Get archived versions (CBB only) ───────────────────────────────────
    version_history = []
    total_archived = 0

    if is_cbb:
        archived_query = """
            SELECT 
                cav.version_id,
                cav.regulation_id,
                cav.requirement_id,
                cav.requirement_title,
                cav.execution_category,
                cav.criticality,
                cav.obligation_type,
                cav.stage1_json,
                cav.stage2_json,
                cav.stage3_json,
                cav.status,
                cav.created_at,
                rv.content_hash,
                rv.updated_date,
                rv.change_summary
            FROM compliance_analysis_versions cav
            JOIN regulation_versions rv ON cav.version_id = rv.version_id
            WHERE cav.regulation_id = ?
            ORDER BY cav.version_id DESC, cav.id
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(archived_query, [regulation_id])
            cols = [c[0] for c in cursor.description]
            archived_rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        versions_dict = {}
        for row in archived_rows:
            vid = row["version_id"]
            if vid not in versions_dict:
                versions_dict[vid] = {
                    "version_id": vid,
                    "regulation_id": row["regulation_id"],
                    "status": row["status"],
                    "schema_version": "v2",
                    "archived_at": serialize_datetime(row["created_at"]),
                    "content_hash": row["content_hash"],
                    "updated_date": serialize_datetime(row["updated_date"]) if row["updated_date"] else None,
                    "change_summary": row["change_summary"],
                    "requirement_count": 0,
                    "requirements": [] if include_details else None
                }

            versions_dict[vid]["requirement_count"] += 1

            if include_details:
                s2 = row.get("stage2_json") or {}
                if isinstance(s2, str):
                    try:
                        s2 = json.loads(s2)
                    except Exception as e:
                        logger.warning(f"Failed to parse stage2_json for archived version {vid}: {e}")
                        s2 = {}

                if not isinstance(s2, dict):
                    s2 = {}

                obligations = s2.get("normalized_obligations", [])

                versions_dict[vid]["requirements"].append({
                    "requirement_id": row["requirement_id"],
                    "requirement_title": row["requirement_title"],
                    "execution_category": row["execution_category"],
                    "criticality": row["criticality"],
                    "obligation_type": row["obligation_type"],
                    "obligation_count": len(obligations),
                    "obligations": obligations,
                    "status": row["status"]
                })

        version_history = list(versions_dict.values())
        total_archived = len(version_history)

        if not include_details:
            for v in version_history:
                v.pop("requirements", None)

    # ── Translation (ALL regulators, not just CBB) ─────────────────────────
    if lang == "ar":
        ENUM_TRANSLATIONS = {
            "execution_category": {
                "Ongoing_Control": "رقابة مستمرة",
                "One_Time_Implementation": "تنفيذ لمرة واحدة",
                "Periodic_Review": "مراجعة دورية",
            },
            "criticality": {"High": "عالي", "Medium": "متوسط", "Low": "منخفض"},
            "obligation_type": {
                "Reporting": "إبلاغ", "Governance": "حوكمة",
                "Preventive": "وقائي", "Detective": "كشف", "Corrective": "تصحيحي",
            },
        }

        all_texts = []
        positions = []

        # Translate current requirements
        if current_summary and include_details:
            for r_idx, req in enumerate(current_summary["requirements"]):
                if req.get("requirement_title"):
                    all_texts.append(req["requirement_title"])
                    positions.append(("current_req", r_idx, "requirement_title", None))

                for field in ["execution_category", "criticality", "obligation_type"]:
                    if req.get(field) and field in ENUM_TRANSLATIONS:
                        req[field] = ENUM_TRANSLATIONS[field].get(req[field], req[field])

                for o_idx, ob in enumerate(req.get("obligations", [])):
                    if ob.get("obligation_text"):
                        all_texts.append(ob["obligation_text"])
                        positions.append(("current_ob", r_idx, o_idx, "obligation_text"))

                    for field in ["obligation_type", "criticality", "execution_category"]:
                        if ob.get(field) and field in ENUM_TRANSLATIONS:
                            ob[field] = ENUM_TRANSLATIONS[field].get(ob[field], ob[field])

        # Translate version history requirements
        if include_details:
            for v_idx, version in enumerate(version_history):
                for r_idx, req in enumerate(version.get("requirements", [])):
                    if req.get("requirement_title"):
                        all_texts.append(req["requirement_title"])
                        positions.append(("version_req", v_idx, r_idx, "requirement_title"))

                    for field in ["execution_category", "criticality", "obligation_type"]:
                        if req.get(field) and field in ENUM_TRANSLATIONS:
                            req[field] = ENUM_TRANSLATIONS[field].get(req[field], req[field])

                    for o_idx, ob in enumerate(req.get("obligations", [])):
                        if ob.get("obligation_text"):
                            all_texts.append(ob["obligation_text"])
                            positions.append(("version_ob", v_idx, r_idx, o_idx))

                        for field in ["obligation_type", "criticality", "execution_category"]:
                            if ob.get(field) and field in ENUM_TRANSLATIONS:
                                ob[field] = ENUM_TRANSLATIONS[field].get(ob[field], ob[field])

        # Batch translate all collected texts
        if all_texts:
            translated = translate_texts_batch(all_texts, lang)
            for pos, tr in zip(positions, translated):
                if pos[0] == "current_req":
                    current_summary["requirements"][pos[1]][pos[2]] = tr
                elif pos[0] == "current_ob":
                    current_summary["requirements"][pos[1]]["obligations"][pos[2]][pos[3]] = tr
                elif pos[0] == "version_req":
                    version_history[pos[1]]["requirements"][pos[2]][pos[3]] = tr
                elif pos[0] == "version_ob":
                    version_history[pos[1]]["requirements"][pos[2]]["obligations"][pos[3]]["obligation_text"] = tr

    # ── Build and return response ───────────────────────────────────────────
    response = {
        "success": True,
        "lang": lang,
        "regulation_id": regulation_id,
        "title": regulation.get("title"),
        "regulator": regulator,
        "include_details": include_details,
        "current": current_summary,
        "version_history": version_history,
        "total_archived": total_archived,
        "note": "Version history (archived rows) is only available for CBB regulations." if not is_cbb else None
    }

    if lang == "ar":
        _set_ar_cache(cache_key, response)

    return response

# ── 3. Archived Analysis Version Detail ──────────────────────────────────────
 
@app.get(
    "/regulation/{regulation_id}/analysis-versions/{version_id}",
    tags=["Versions"],
)
def get_analysis_version_detail(
    regulation_id: int, version_id: int, lang: str = Query("en")
):
    lang = _validate_lang(lang)
    rows = repo.get_analysis_version_detail(regulation_id, version_id)
    if not rows:
        raise HTTPException(
            200,
            f"No archived analysis version {version_id} for regulation {regulation_id}. "
            f"Use GET /regulation/{regulation_id}/analysis-versions to see available versions.",
        )
 
    if lang == "ar":
        cache_key = f"GET /regulation/{regulation_id}/analysis-versions/{version_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached
 
        ENUM_TRANSLATIONS = {
            "execution_category": {
                "Ongoing_Control":          "رقابة مستمرة",
                "One_Time_Implementation":  "تنفيذ لمرة واحدة",
                "Periodic_Review":          "مراجعة دورية",
                "Governance_Approval":      "موافقة الحوكمة",
                "One_Off_Reporting":        "إبلاغ لمرة واحدة",
                "Event_Driven":             "حسب الحدث",
                "Continuous_Monitoring":    "مراقبة مستمرة",
                "Annual_Review":            "مراجعة سنوية",
            },
            "criticality":     {"High": "عالي", "Medium": "متوسط", "Low": "منخفض"},
            "obligation_type": {
                "Reporting":  "إبلاغ",  "Governance": "حوكمة",
                "Preventive": "وقائي", "Detective":   "كشف",   "Corrective": "تصحيحي",
            },
        }
 
        all_texts, positions = [], []
        for r_idx, row in enumerate(rows):
            # Translate enum fields in-place
            for field in ["execution_category", "criticality", "obligation_type"]:
                val = row.get(field)
                if val and field in ENUM_TRANSLATIONS:
                    row[field] = ENUM_TRANSLATIONS[field].get(val, val)
 
            # Collect text fields for batch translation
            if row.get("requirement_title"):
                all_texts.append(row["requirement_title"])
                positions.append(("req", r_idx, "requirement_title", None, None))
 
            for o_idx, ob in enumerate(row.get("obligations") or []):
                if ob.get("obligation_text"):
                    all_texts.append(ob["obligation_text"])
                    positions.append(("ob", r_idx, "obligation_text", o_idx, None))
                if ob.get("test_method"):
                    all_texts.append(ob["test_method"])
                    positions.append(("ob", r_idx, "test_method", o_idx, None))
                # Enum fields on obligations
                for field in ["obligation_type", "criticality", "execution_category"]:
                    val = ob.get(field)
                    if val and field in ENUM_TRANSLATIONS:
                        ob[field] = ENUM_TRANSLATIONS[field].get(val, val)
 
        if all_texts:
            translated = translate_texts_batch(all_texts, lang)
            for (kind, r_idx, field, o_idx, _), tr in zip(positions, translated):
                if kind == "req":
                    rows[r_idx][field] = tr
                elif kind == "ob":
                    rows[r_idx]["obligations"][o_idx][field] = tr
 
        result = {
            "success":           True,
            "lang":              lang,
            "regulation_id":     regulation_id,
            "version_id":        version_id,
            "requirement_count": len(rows),
            "status":            "inactive",
            "requirements":      rows,
        }
        _set_ar_cache(cache_key, result)
        return result
 
    return {
        "success":           True,
        "lang":              lang,
        "regulation_id":     regulation_id,
        "version_id":        version_id,
        "requirement_count": len(rows),
        "status":            "inactive",
        "requirements":      rows,
    }
 

@app.patch(
    "/regulation/{regulation_id}/versions/{version_id}/status",
    tags=["Versions"],
)
def update_content_version_status(
    regulation_id: int,
    version_id: int,
    payload: VersionStatusUpdate,
):
    """Update is_current flag for a regulation_versions (content snapshot) row."""
    if payload.status not in ("active", "inactive"):
        raise HTTPException(400, "status must be 'active' or 'inactive'")

    repo.execute_update(
        "UPDATE regulation_versions SET status = ? WHERE regulation_id = ? AND version_id = ?",
        (payload.status, regulation_id, version_id),
    )
    _invalidate_ar_cache(regulation_id)
    return {
        "success":       True,
        "regulation_id": regulation_id,
        "version_id":    version_id,
        "status":        payload.status,
    }


@app.patch(
    "/regulation/{regulation_id}/analysis-versions/{version_id}/status",
    tags=["Versions"],
)
def update_analysis_version_status(
    regulation_id: int,
    version_id: int,
    payload: VersionStatusUpdate,
):
    """
    Update status of an archived analysis version in compliance_analysis_versions.
    Does NOT affect the current active rows in compliance_analysis.
    """
    if payload.status not in ("active", "inactive"):
        raise HTTPException(400, "status must be 'active' or 'inactive'")

    repo.execute_update(
        "UPDATE compliance_analysis_versions SET status = ? WHERE regulation_id = ? AND version_id = ?",
        (payload.status, regulation_id, version_id),
    )
    _invalidate_ar_cache(regulation_id)
    return {
        "success":       True,
        "regulation_id": regulation_id,
        "version_id":    version_id,
        "status":        payload.status,
    }


# ================================================================== #
#  UPLOAD REGULATION ENDPOINT                                          #
# ================================================================== #

@app.post("/upload-regulation", tags=["Upload Regulation"])
async def upload_regulation(
    file: UploadFile = File(...),
    regulator: str = Form(...),
    source_system: Optional[str] = Form(None),
    category: Optional[str] = Form(None),
    compliancecategory_id: Optional[int] = Form(None),
    skip_analysis: bool = Form(False),
    document_url: Optional[str] = Form(None),
):
    """
    Upload a regulation PDF/DOCX and run the full pipeline.

    Stages:
    1. Extract text (OCR/docx)
    2. LLM metadata extraction
    3. Insert into regulations table
    4. 4-stage LLM analysis → stored in compliance_analysis (version_id=None for uploads)
    5. Requirement matching
    """
    filename = file.filename or "upload"
    suffix   = os.path.splitext(filename.lower())[-1]
    if suffix not in (".pdf", ".docx", ".doc"):
        raise HTTPException(400, "Only PDF and DOCX files are supported.")

    tmp_path      = None
    text          = ""
    document_html = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name
        text, document_html = extract_document_content(tmp_path, suffix)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    if not text or len(text) < 100:
        raise HTTPException(
            422,
            f"Could not extract sufficient text (got {len(text or '')} chars, need ≥ 100).",
        )

    metadata = extract_metadata_from_text(text, filename=filename)
    if regulator:
        metadata["regulator"] = regulator.strip().upper()

    doc = _build_upload_doc_object(
        metadata=metadata,
        text=text,
        filename=filename,
        compliancecategory_id=compliancecategory_id,
        regulator=regulator,
        source_system=source_system,
        category=category,
        document_url=document_url,
        document_html=document_html,
    )

    try:
        regulation_id = repo._insert_regulation(doc)
    except Exception as e:
        logger.error(f"[upload] DB insert failed: {e}")
        raise HTTPException(500, f"Failed to save regulation: {e}")

    if skip_analysis:
        return {
            "success":       True,
            "regulation_id": regulation_id,
            "metadata":      metadata,
            "text_length":   len(text),
            "pipeline":      "skipped (skip_analysis=true)",
            "next_step":     f"POST /trigger/staged-analysis/{regulation_id}",
        }

    normalizer = LLMAnalyzer()
    try:
        clean_text = normalizer.normalize_input_text(text, content_type="pdf_text")
    except Exception as e:
        logger.error(f"[upload] Normalization failed: {e}")
        clean_text = text

    analysis_summary = {}
    matching_summary = {}
    rows = []

    try:
        rows = staged_analyzer.analyze(
            text=clean_text,
            regulation_id=regulation_id,
            document_title=doc.title,
            regulator=doc.regulator,
            reference=doc.reference_no or "",
            publication_date=doc.published_date or "",
        )
        if not rows:
            raise ValueError("4-stage analysis returned 0 requirements")

        # Uploaded docs are not CBB, so version_id=None
        repo.store_analysis(rows, version_id=None)

        exec_counts, crit_counts = {}, {}
        for r in rows:
            ec = r.get("execution_category") or "Unknown"
            cr = r.get("criticality") or "Unknown"
            exec_counts[ec] = exec_counts.get(ec, 0) + 1
            crit_counts[cr] = crit_counts.get(cr, 0) + 1

        analysis_summary = {
            "requirements_extracted": len(rows),
            "by_execution_category":  exec_counts,
            "by_criticality":         crit_counts,
        }
    except Exception as e:
        logger.error(f"[upload] LLM analysis failed: {e}")
        analysis_summary = {"error": str(e)}

    if rows:
        try:
            matching_summary = _run_upload_requirement_matching(regulation_id, rows)
        except Exception as e:
            logger.error(f"[upload] Requirement matching failed: {e}")
            matching_summary = {"error": str(e)}

    return {
        "success":       True,
        "regulation_id": regulation_id,
        "extracted_metadata": {
            "title":          doc.title,
            "published_date": doc.published_date,
            "reference_no":   doc.reference_no,
            "year":           doc.year,
            "regulator":      doc.regulator,
            "source_system":  doc.source_system,
            "category":       doc.category,
        },
        "pipeline": {
            "text_extracted_chars": len(text),
            "analysis":             analysis_summary,
            "matching":             matching_summary,
        },
        "next_steps": {
            "view_analysis":   f"GET  /compliance-analysis/{regulation_id}",
            "view_mapping":    f"GET  /requirement-mapping/{regulation_id}",
            "gap_analysis":    f"POST /gap-analysis/single (form: regulation_id={regulation_id})",
            "re_run_analysis": f"POST /trigger/staged-analysis/{regulation_id}?force=true",
        },
    }


# ================================================================== #
#  ADMIN ENDPOINTS                                                     #
# ================================================================== #

@app.delete("/admin/ar-cache/{regulation_id}", tags=["Admin"])
def clear_ar_cache_for_regulation(regulation_id: int):
    _invalidate_ar_cache(regulation_id)
    return {"success": True, "message": f"Arabic cache cleared for regulation {regulation_id}"}


@app.delete("/admin/ar-cache", tags=["Admin"])
def clear_all_ar_cache():
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM ar_response_cache")
            deleted = cursor.rowcount
            conn.commit()
        return {"success": True, "message": f"Cleared {deleted} cached entries"}
    except Exception as e:
        raise HTTPException(500, str(e))


# ================================================================== #
#  HEALTH & ROOT                                                       #
# ================================================================== #

@app.get("/health")
def health_check():
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return {
            "success": True, "status": "healthy",
            "database": "connected",
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {
            "success": False, "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        }


@app.get("/")
def root():
    return {
        "message":             "Regulatory Pipeline API",
        "version":             "2.0.0",
        "supported_languages": ["en", "ar"],
        "schema":              "v2 unified — compliance_analysis is the single source of truth for all regulators",
        "versioning_note": (
            "All regulators write to compliance_analysis (current). "
            "CBB additionally archives to compliance_analysis_versions on content change. "
            "version_id is set on CBB rows; NULL for SAMA/SBP/SECP."
        ),
        "endpoints": {
            "upload":         "POST /upload-regulation",
            "gap_analysis": {
                "single":     "POST /gap-analysis/single",
                "multi":      "POST /gap-analysis/multi",
                "multi_docs": "POST /gap-analysis/multi-docs",
                "session":    "GET  /gap-analysis/session/{session_id}",
            },
            "analysis": {
                "full":         "GET  /compliance-analysis/{regulation_id}",
                "list":         "GET  /compliance-analysis-v2/{regulation_id}",
                "detail":       "GET  /compliance-analysis-v2/{regulation_id}/requirement/{requirement_id}",
                "exec_summary": "GET  /compliance-analysis-v2/{regulation_id}/executive-summary",
                "trigger":      "POST /trigger/staged-analysis/{regulation_id}",
            },
            "matching": {
                "trigger":  "POST /trigger/requirement-matching/{regulation_id}",
                "mappings": "GET  /requirement-mapping/{regulation_id}",
                "controls": "GET  /control-mapping/{regulation_id}",
                "kpis":     "GET  /kpi-mapping/{regulation_id}",
            },
            "versions": {
                "content_history":        "GET   /regulation/{id}/versions",
                "analysis_history":       "GET   /regulation/{id}/analysis-versions",
                "analysis_version_detail":"GET   /regulation/{id}/analysis-versions/{version_id}",
                "update_content_version": "PATCH /regulation/{id}/versions/{version_id}/status",
                "update_analysis_version":"PATCH /regulation/{id}/analysis-versions/{version_id}/status",
            },
            "admin": {
                "clear_reg_cache": "DELETE /admin/ar-cache/{regulation_id}",
                "clear_all_cache": "DELETE /admin/ar-cache",
            },
        },
        "available_regulators": list(REGULATOR_PIPELINES.keys()),
    }


# ================================================================== #
#  CBB TESTING ENDPOINTS                                               #
# ================================================================== #

@app.post("/test/cbb/analyze-top-5", tags=["Testing - CBB"])
def test_cbb_analyze_top_5():
    """
    Test endpoint: Analyze top 5 CBB regulations.

    This will:
    1. Find the 5 most recent CBB regulations
    2. Run staged analysis on each
    3. Run requirement matching on each
    4. Return results
    """
    try:
        # Get top 5 most recent CBB regulations
        query = """
            SELECT TOP 5 
                r.id,
                r.title,
                r.reference_no,
                r.published_date,
                rv.version_id
            FROM regulations r
            LEFT JOIN regulation_versions rv ON r.id = rv.regulation_id AND rv.status = 'active'
            WHERE r.regulator = 'Central Bank of Bahrain'
            ORDER BY r.published_date DESC, r.id DESC
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

        if not rows:
            return {
                "success": False,
                "error": "No CBB regulations found",
                "total_processed": 0,
                "results": []
            }

        results = []

        for row in rows:
            regulation_id = row[0]
            title = row[1]
            reference_no = row[2]
            published_date = row[3]
            version_id = row[4]

            logger.info(f"Processing CBB regulation {regulation_id}: {title}")

            result = {
                "regulation_id": regulation_id,
                "title": title,
                "reference_no": reference_no,
                "published_date": str(published_date) if published_date else None,
                "version_id": version_id,
                "stages": {}
            }

            # Stage 1: Run staged analysis
            try:
                analysis_response = trigger_staged_analysis(regulation_id, force=True)
                result["stages"]["analysis"] = {
                    "success": True,
                    "requirements_extracted": analysis_response.get("analysis", {}).get("requirements_extracted", 0)
                }
            except Exception as e:
                logger.error(f"Analysis failed for {regulation_id}: {e}")
                result["stages"]["analysis"] = {
                    "success": False,
                    "error": str(e)
                }
                results.append(result)
                continue

            # Stage 2: Run requirement matching
            try:
                matching_response = trigger_requirement_matching_v2(regulation_id)
                result["stages"]["matching"] = {
                    "success": True,
                    "summary": matching_response.get("summary", {})
                }
            except Exception as e:
                logger.error(f"Matching failed for {regulation_id}: {e}")
                result["stages"]["matching"] = {
                    "success": False,
                    "error": str(e)
                }

            results.append(result)

        successful = sum(1 for r in results if r["stages"].get("analysis", {}).get("success"))

        return {
            "success": True,
            "total_processed": len(results),
            "successful": successful,
            "failed": len(results) - successful,
            "results": results,
            "next_steps": {
                "view_analysis": "GET /compliance-analysis/{regulation_id}",
                "view_mapping": "GET /requirement-mapping/{regulation_id}",
                "gap_analysis": "POST /gap-analysis/single"
            }
        }

    except Exception as e:
        logger.exception("Error in test_cbb_analyze_top_5")
        raise HTTPException(500, f"Test failed: {e}")


@app.get("/test/cbb/regulations", tags=["Testing - CBB"])
def test_get_cbb_regulations(limit: int = Query(10, ge=1, le=100)):
    """
    Get list of CBB regulations for testing.
    Shows regulation_id, title, version_id, and analysis status.
    """
    try:
        query = f"""
            SELECT TOP {limit}
                r.id,
                r.title,
                r.reference_no,
                r.published_date,
                rv.version_id,
                rv.content_hash,
                CASE WHEN ca.regulation_id IS NOT NULL THEN 1 ELSE 0 END as has_analysis
            FROM regulations r
            LEFT JOIN regulation_versions rv ON r.id = rv.regulation_id AND rv.status = 'active'
            LEFT JOIN (
                SELECT DISTINCT regulation_id 
                FROM compliance_analysis 
                WHERE is_current = 1
            ) ca ON r.id = ca.regulation_id
            WHERE r.regulator = 'Central Bank of Bahrain'
            ORDER BY r.published_date DESC, r.id DESC
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

        regulations = []
        for row in rows:
            regulations.append({
                "regulation_id": row[0],
                "title": row[1],
                "reference_no": row[2],
                "published_date": str(row[3]) if row[3] else None,
                "version_id": row[4],
                "content_hash": row[5],
                "has_analysis": bool(row[6]),
                "analyze_url": f"/trigger/staged-analysis/{row[0]}",
                "view_analysis_url": f"/compliance-analysis/{row[0]}" if row[6] else None
            })

        return {
            "success": True,
            "total": len(regulations),
            "regulations": regulations
        }

    except Exception as e:
        logger.exception("Error fetching CBB regulations")
        raise HTTPException(500, str(e))


# Add to pipeline_api.py

@app.post("/trigger/batch-analysis", tags=["V2 Staged Analysis"])
def trigger_batch_analysis(
        regulation_ids: List[int] = Body(...),
        force: bool = Query(False)
):
    """
    Run analysis for multiple regulations in batch.
    Useful for backfilling unanalyzed CBB regulations.
    """
    results = []

    for reg_id in regulation_ids:
        try:
            # Use the existing endpoint logic
            result = trigger_staged_analysis(reg_id, force=force)
            results.append({
                "regulation_id": reg_id,
                "success": True,
                "result": result
            })
        except Exception as e:
            results.append({
                "regulation_id": reg_id,
                "success": False,
                "error": str(e)
            })

    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful

    return {
        "success": True,
        "total": len(regulation_ids),
        "successful": successful,
        "failed": failed,
        "results": results
    }


@app.post("/trigger/analysis-for-version/{regulation_id}/{version_id}", tags=["V2 Staged Analysis"])
def trigger_analysis_for_specific_version(
        regulation_id: int,
        version_id: int,
        force: bool = Query(False)
):
    """
    Run analysis for a SPECIFIC content version (for backfilling old versions).

    This is useful for:
    - Showcasing version history
    - Backfilling analysis for old versions that were never analyzed
    - Testing the versioning system

    IMPORTANT: This creates analysis with is_current=0 and moves it directly
    to compliance_analysis_versions (archived state).
    """
    regulation = repo.get_regulation_by_id(regulation_id)
    if not regulation:
        raise HTTPException(404, f"Regulation {regulation_id} not found")

    # Check if it's a CBB regulation
    if regulation.get("regulator") != "Central Bank of Bahrain":
        raise HTTPException(400, "This endpoint is only for CBB regulations with versioning")

    # Get the specific version
    version_query = """
        SELECT version_id, regulation_id, content_text, content_html, 
               content_hash, status, change_summary, created_at
        FROM regulation_versions
        WHERE regulation_id = ? AND version_id = ?
    """

    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(version_query, [regulation_id, version_id])
        cols = [c[0] for c in cursor.description]
        row = cursor.fetchone()

        if not row:
            raise HTTPException(404, f"Version {version_id} not found for regulation {regulation_id}")

        version_data = dict(zip(cols, row))

    # Check if this version already has analysis
    existing_check = """
        SELECT COUNT(*) as cnt
        FROM compliance_analysis_versions
        WHERE regulation_id = ? AND version_id = ?
    """

    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(existing_check, [regulation_id, version_id])
        existing_count = cursor.fetchone()[0]

    if existing_count > 0 and not force:
        return {
            "success": True,
            "regulation_id": regulation_id,
            "version_id": version_id,
            "skipped": True,
            "reason": f"Version {version_id} already has {existing_count} archived analysis rows. Use ?force=true to re-run.",
            "existing_count": existing_count
        }

    # Extract content
    content_text = (version_data.get("content_text") or "").strip()
    content_html = (version_data.get("content_html") or "").strip()

    text_content = None
    content_type = None

    if len(content_text) >= 200:
        text_content = content_text
        content_type = "html"
    elif len(content_html) >= 200:
        text_content = content_html
        content_type = "html"

    if not text_content:
        raise HTTPException(422, f"Version {version_id} has no extractable content")

    # Normalize text
    normalizer = LLMAnalyzer()
    try:
        clean_text = normalizer.normalize_input_text(text_content, content_type=content_type)
    except Exception as e:
        raise HTTPException(422, f"Text normalization failed: {e}")

    if len(clean_text) < 200:
        raise HTTPException(422, f"Text too short after normalization ({len(clean_text)} chars)")

    # Run 4-stage analysis
    rows = staged_analyzer.analyze(
        text=clean_text,
        regulation_id=regulation_id,
        document_title=regulation.get("title", "Untitled"),
        regulator=regulation.get("regulator") or "",
        reference=regulation.get("reference_no") or "",
        publication_date=str(version_data.get("created_at") or "")[:10],
    )

    if not rows:
        raise HTTPException(422, f"Pipeline extracted 0 requirements for version {version_id}")

    # If force=true, clear existing archived analysis for this version
    if force and existing_count > 0:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM compliance_analysis_versions WHERE regulation_id = ? AND version_id = ?",
                [regulation_id, version_id]
            )
            conn.commit()

    # Insert into compliance_analysis_versions (archived)
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        for r in rows:
            cursor.execute("""
                INSERT INTO compliance_analysis_versions (
                    regulation_id, version_id, requirement_id, requirement_title,
                    execution_category, criticality, obligation_type,
                    stage1_json, stage2_json, stage3_json,
                    status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'inactive', ?)
            """, [
                regulation_id, version_id,
                r.get("requirement_id"), r.get("requirement_title"),
                r.get("execution_category"), r.get("criticality"), r.get("obligation_type"),
                json.dumps(r.get("stage1_json")) if r.get("stage1_json") else None,
                json.dumps(r.get("stage2_json")) if r.get("stage2_json") else None,
                json.dumps(r.get("stage3_json")) if r.get("stage3_json") else None,
                version_data.get("created_at")
            ])
        conn.commit()
    exec_counts, crit_counts = {}, {}
    for r in rows:
        ec = r.get("execution_category") or "Unknown"
        cr = r.get("criticality") or "Unknown"
        exec_counts[ec] = exec_counts.get(ec, 0) + 1
        crit_counts[cr] = crit_counts.get(cr, 0) + 1

    return {
        "success": True,
        "regulation_id": regulation_id,
        "version_id": version_id,
        "version_status": version_data.get("status"),
        "change_summary": version_data.get("change_summary"),
        "text_length": len(clean_text),
        "content_type": content_type,
        "analysis": {
            "requirements_extracted": len(rows),
            "by_execution_category": exec_counts,
            "by_criticality": crit_counts,
            "stored_as": "archived (compliance_analysis_versions)"
        },
        "next_step": f"GET /regulation/{regulation_id}/analysis-versions"
    }


# ── 2. Active Content Version ─────────────────────────────────────────────────
 
@app.get("/regulation/{regulation_id}/versions/active", tags=["Content Versions"])
def get_active_version(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)
    regulation = repo.get_regulation_by_id(regulation_id)
    if not regulation:
        raise HTTPException(404, f"Regulation {regulation_id} not found")
 
    regulator = regulation.get("regulator")
    if regulator != "Central Bank of Bahrain":
        return {
            "success":        True,
            "lang":           lang,
            "regulation_id":  regulation_id,
            "regulator":      regulator,
            "has_versioning": False,
            "active_version": None,
            "note": "Content versioning is only available for CBB regulations.",
        }
 
    version_data = repo.get_active_regulation_version(regulation_id)
    if not version_data:
        return {
            "success":        True,
            "lang":           lang,
            "regulation_id":  regulation_id,
            "regulator":      regulator,
            "has_versioning": True,
            "active_version": None,
            "note": "No active version found.",
        }
 
    change_summary = version_data.get("change_summary") or ""
    if lang == "ar" and change_summary:
        translated = translate_texts_batch([change_summary], lang)
        change_summary = translated[0] if translated else change_summary
 
    return {
        "success":        True,
        "lang":           lang,
        "regulation_id":  regulation_id,
        "regulator":      regulator,
        "has_versioning": True,
        "active_version": {
            "version_id":          version_data.get("version_id"),
            "content_hash":        version_data.get("content_hash"),
            "updated_date":        serialize_datetime(version_data.get("updated_date")),
            "created_at":          serialize_datetime(version_data.get("created_at")),
            "change_summary":      change_summary,
            "status":              version_data.get("status"),
            "content_text_length": len(version_data.get("content_text") or ""),
            "content_html_length": len(version_data.get("content_html") or ""),
            "content_text":        version_data.get("content_text"),
            "content_html":        version_data.get("content_html"),
        },
    }
 

@app.delete("/admin/analysis/{regulation_id}", tags=["Admin"])
def delete_full_analysis(
    regulation_id: int,
    include_versions: bool = Query(False, description="Also delete archived compliance_analysis_versions"),
):
    deleted = {}
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
 
            cursor.execute(
                "DELETE FROM compliance_analysis WHERE regulation_id = ?",
                [regulation_id]
            )
            deleted["compliance_analysis"] = cursor.rowcount
 
            cursor.execute(
                "DELETE FROM sama_requirement_mapping WHERE regulation_id = ?",
                [regulation_id]
            )
            deleted["requirement_mappings"] = cursor.rowcount
 
            cursor.execute(
                "DELETE FROM DEMO_REQUIREMENT_CONTROL_LINK WHERE REGULATION_ID = ?",
                [regulation_id]
            )
            deleted["control_links"] = cursor.rowcount
 
            cursor.execute(
                "DELETE FROM DEMO_REQUIREMENT_KPI_LINK WHERE REGULATION_ID = ?",
                [regulation_id]
            )
            deleted["kpi_links"] = cursor.rowcount
 
            if include_versions:
                cursor.execute(
                    "DELETE FROM compliance_analysis_versions WHERE regulation_id = ?",
                    [regulation_id]
                )
                deleted["compliance_analysis_versions"] = cursor.rowcount
 
            conn.commit()
 
        _invalidate_ar_cache(regulation_id)
 
        return {
            "success":       True,
            "regulation_id": regulation_id,
            "deleted":       deleted,
            "total_deleted": sum(deleted.values()),
            "note": (
                "Regulation record and regulation_versions snapshots were NOT deleted. "
                "Use include_versions=true to also clear archived analysis history."
            ),
            "next_step": f"POST /trigger/full-analysis/{regulation_id}?force=true",
        }
 
    except Exception as e:
        logger.exception(f"Error deleting analysis for regulation {regulation_id}")
        raise HTTPException(500, str(e))
 

# ── 6. POST /regulations/add — with Arabic response ──────────────────────────
 
class AddRegulationRequest(BaseModel):
    title:                 str
    regulator:             str
    source_system:         Optional[str] = None
    category:              Optional[str] = None
    compliancecategory_id: Optional[int] = None
    reference_no:          Optional[str] = None
    department:            Optional[str] = None
    published_date:        Optional[str] = None
    year:                  Optional[int] = None
    document_url:          Optional[str] = None
    source_page_url:       Optional[str] = None
    document_html:         Optional[str] = None
    document_text:         Optional[str] = None
    status:                Optional[str] = "active"
    run_analysis:          bool          = False

# ── Endpoint — add this to pipeline_api.py ───────────────────────────────────

@app.post("/regulations/add", tags=["Regulations"])
def add_regulation(payload: AddRegulationRequest, lang: str = Query("en")):
    lang = _validate_lang(lang)
 
    class _Doc:
        pass
 
    doc = _Doc()
    doc.title           = payload.title.strip()
    doc.regulator       = payload.regulator.strip().upper()
    doc.source_system   = payload.source_system or "MANUAL"
    doc.category        = payload.category
    doc.reference_no    = payload.reference_no
    doc.department      = payload.department
    doc.published_date  = payload.published_date
    doc.year            = payload.year
    doc.document_url    = payload.document_url
    doc.source_page_url = payload.source_page_url or payload.document_url
    doc.document_html   = payload.document_html
    doc.doc_path        = None
    doc.status          = payload.status or "active"
    doc.type            = "manual"
    doc.compliancecategory_id = payload.compliancecategory_id
    doc.extra_meta = {
        "org_pdf_text": payload.document_text or "",
        "added_via":    "api",
    }
 
    # Duplicate check
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM regulations WHERE title = ? AND regulator = ?",
                [doc.title, doc.regulator],
            )
            existing = cursor.fetchone()
            if existing:
                return {
                    "success":       False,
                    "regulation_id": existing[0],
                    "message":       f"Regulation already exists with id={existing[0]}.",
                    "duplicate":     True,
                }
    except Exception as exc:
        raise HTTPException(500, f"Duplicate check failed: {exc}")
 
    try:
        regulation_id = repo._insert_regulation(doc)
    except Exception as exc:
        logger.error(f"[add-regulation] DB insert failed: {exc}")
        raise HTTPException(500, f"Failed to save regulation: {exc}")
 
    # Translate title for Arabic response
    display_title = doc.title
    if lang == "ar":
        translated = translate_texts_batch([doc.title], lang)
        display_title = translated[0] if translated else doc.title
 
    response = {
        "success":       True,
        "lang":          lang,
        "regulation_id": regulation_id,
        "title":         display_title,
        "regulator":     doc.regulator,
        "status":        doc.status,
        "has_content":   bool(payload.document_text or payload.document_html),
        "next_steps": {
            "view":         f"GET  /regulation/{regulation_id}",
            "run_analysis": f"POST /trigger/full-analysis/{regulation_id}",
            "gap_analysis": f"POST /gap-analysis/single (form: regulation_id={regulation_id})",
        },
    }
 
    if payload.run_analysis:
        if not (payload.document_text or payload.document_html):
            response["analysis"] = {
                "skipped": True,
                "reason":  "run_analysis=true but no document_text or document_html provided.",
            }
        else:
            try:
                analysis_result = trigger_staged_analysis(regulation_id, force=False)
                matching_result = {}
                try:
                    matching_result = trigger_requirement_matching_v2(regulation_id)
                except Exception as me:
                    matching_result = {"error": str(me)}
 
                response["analysis"] = {
                    "success":                True,
                    "requirements_extracted": analysis_result.get("analysis", {}).get("requirements_extracted", 0),
                    "by_execution_category":  analysis_result.get("analysis", {}).get("by_execution_category", {}),
                    "by_criticality":         analysis_result.get("analysis", {}).get("by_criticality", {}),
                }
                response["matching"] = {
                    "success": "error" not in matching_result,
                    "summary": matching_result.get("summary", {}),
                    "error":   matching_result.get("error"),
                }
            except Exception as exc:
                logger.error(f"[add-regulation] Auto-analysis failed: {exc}")
                response["analysis"] = {"success": False, "error": str(exc)}
 
    return response


# ── 7. PUT /regulations/{regulation_id} — with Arabic response ───────────────
 
class UpdateRegulationRequest(BaseModel):
    title:                 Optional[str] = None
    category:              Optional[str] = None
    compliancecategory_id: Optional[int] = None
    reference_no:          Optional[str] = None
    department:            Optional[str] = None
    published_date:        Optional[str] = None
    year:                  Optional[int] = None
    document_url:          Optional[str] = None
    source_page_url:       Optional[str] = None
    document_html:         Optional[str] = None
    document_text:         Optional[str] = None
    status:                Optional[str] = None


@app.put("/regulations/{regulation_id}", tags=["Regulations"])
def update_regulation(
    regulation_id: int,
    payload: UpdateRegulationRequest,
    lang: str = Query("en"),
):
    lang = _validate_lang(lang)
    existing = repo.get_regulation_by_id(regulation_id)
    if not existing:
        raise HTTPException(404, f"Regulation {regulation_id} not found")
 
    updates = {}
    if payload.title           is not None: updates["title"]                 = payload.title
    if payload.category        is not None: updates["category"]              = payload.category
    if payload.compliancecategory_id is not None: updates["compliancecategory_id"] = payload.compliancecategory_id
    if payload.reference_no    is not None: updates["reference_no"]          = payload.reference_no
    if payload.department      is not None: updates["department"]            = payload.department
    if payload.published_date  is not None: updates["published_date"]        = payload.published_date
    if payload.year            is not None: updates["[year]"]                = payload.year
    if payload.document_url    is not None: updates["document_url"]          = payload.document_url
    if payload.source_page_url is not None: updates["source_page_url"]       = payload.source_page_url
    if payload.document_html   is not None: updates["document_html"]         = payload.document_html
    if payload.status          is not None: updates["status"]                = payload.status
 
    if not updates and payload.document_text is None:
        return {"success": True, "regulation_id": regulation_id, "message": "No fields to update"}
 
    set_parts = [f"{col} = ?" for col in updates]
    params    = list(updates.values())
 
    if payload.document_text is not None:
        try:
            extra_meta = existing.get("extra_meta") or {}
            if isinstance(extra_meta, str):
                try:
                    extra_meta = json.loads(extra_meta)
                except Exception:
                    extra_meta = {}
            extra_meta["org_pdf_text"] = payload.document_text
            set_parts.append("extra_meta = ?")
            params.append(json.dumps(extra_meta, ensure_ascii=False))
        except Exception as exc:
            logger.warning(f"[update-regulation] extra_meta update failed: {exc}")
 
    set_parts.append("updated_at = GETUTCDATE()")
    params.append(regulation_id)
 
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE regulations SET {', '.join(set_parts)} WHERE id = ?",
                params,
            )
            conn.commit()
            if cursor.rowcount == 0:
                raise HTTPException(404, f"Regulation {regulation_id} not found during update")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"Update failed: {exc}")
 
    _invalidate_ar_cache(regulation_id)
    content_updated = payload.document_text is not None or payload.document_html is not None
 
    return {
        "success":         True,
        "lang":            lang,
        "regulation_id":   regulation_id,
        "fields_updated":  list(updates.keys()) + (["document_text"] if payload.document_text else []),
        "content_updated": content_updated,
        "next_steps": {
            "view":        f"GET  /regulation/{regulation_id}",
            "re_analysis": f"POST /trigger/full-analysis/{regulation_id}?force=true" if content_updated else None,
        },
    }
 
