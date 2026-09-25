from dotenv import load_dotenv
load_dotenv(override=True)
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, UploadFile, File, Form,Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime
import logging
import uuid
import os
import re
import json
import tempfile
from typing import Optional, List, Dict, Any, Tuple
import time
from threading import Thread, Lock
from datetime import time as dtime

from scheduler.scheduler import run_sbp_pipeline, run_secp_pipeline, run_sama_pipeline
from storage.mssql_repo import MSSQLRepository
from processor.gap_analyzer import GapAnalyzer
from processor.Text_Extractor import OCRProcessor
from processor.metadata_extractor import extract_metadata_from_text, extract_document_content
import docx as python_docx
from fastapi.responses import JSONResponse, Response
from processor.staged_LLM_Analyzer import StagedLLMAnalyzer
from processor import analysis_cache
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
from utils.public_meta import public_extra_meta
from apis import run_results_api
from storage import run_store
logger = logging.getLogger(__name__)
app = FastAPI(title="Regulatory Pipeline API", version="2.0.0")


@app.middleware("http")
async def _count_requests(request, call_next):
    t0 = time.time()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        run_results_api.metrics.record(f"{request.method} {getattr(route, 'path', request.url.path)}", status, time.time() - t0)

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
run_results_api.init(repo)
app.include_router(run_results_api.router)
_diag_logger = logging.getLogger("db_diagnostic")
_diag_logger.info("=" * 60)
_diag_logger.info(f"DB SERVER:   {os.getenv('MSSQL_SERVER')}")
_diag_logger.info(f"DB NAME:     {os.getenv('MSSQL_DATABASE')}")
_diag_logger.info(f"DB USER:     {os.getenv('MSSQL_USERNAME', '(empty = Trusted Connection)')}")
_diag_logger.info(f"DB DRIVER:   {os.getenv('MSSQL_DRIVER')}")
_diag_logger.info(f"DB PASS SET: {'YES' if os.getenv('MSSQL_PASSWORD') else 'NO (empty)'}")
_diag_logger.info("=" * 60)
gap_analyzer = GapAnalyzer()
staged_analyzer = StagedLLMAnalyzer()
requirement_matcher = RequirementMatcher()

# Add to REGULATOR_PIPELINES dictionary
REGULATOR_PIPELINES = {
    "SBP":  run_sbp_pipeline,
    "SECP": run_secp_pipeline,
    "SAMA": run_sama_pipeline,
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


def _attach_regulation_counts(cursor, categories: list):
    """Mark each category dict with whether it has regulations directly
    attached, so a category that is both a folder (has children) and a
    leaf (owns a regulation) surfaces both facts instead of just the tree.

    MEASURED 2026-08-28: this used to filter with
    `WHERE compliancecategory_id IN (?,?,?,...)`, one placeholder per category
    passed in. `compliancecategory` now holds 8,893 rows, and `/categories`
    and `/categories/root` (which pass the full list) blew past what the ODBC
    driver accepts as parameters in one query:
        07002 [Microsoft][ODBC Driver 17 for SQL Server]COUNT field incorrect
        or syntax error (0) (SQLExecDirectW)
    Aggregating over ALL of `regulations` unfiltered and looking counts up by
    id afterwards needs zero parameters and is one query regardless of how
    many categories `categories` holds -- the same code path now works for
    the full-tree callers and the small-list callers (`/categories/roots`,
    `/categories/children/{id}`) alike.
    """
    cursor.execute(
        "SELECT compliancecategory_id, COUNT(*) FROM regulations "
        "WHERE compliancecategory_id IS NOT NULL "
        "GROUP BY compliancecategory_id"
    )
    counts = {row[0]: row[1] for row in cursor.fetchall()}
    for c in categories:
        cnt = counts.get(c["compliancecategory_id"], 0)
        c["has_regulations"] = cnt > 0
        c["regulation_count"] = cnt


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
    ref_key: Optional[str]
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


# ============================================================================
#  MONITORING JOBS OVER HTTP
# ============================================================================
# WHY THESE EXIST
# ---------------
# Every monitor_* job lives in scheduler.py's DIRECT_JOB_MAPPING, but
# EXECUTION_MODE defaults to "API", so the scheduler reads API_JOB_MAPPING —
# where no monitor job existed. Setting `enabled: true` on one therefore logged
#     WARNING: No function mapped for job: monitor_cbe
# and did nothing. No crash, no error surfaced, no monitoring. That was true for
# EVERY regulator, not just the new ones.
#
# WHY THEY RETURN IMMEDIATELY
# ---------------------------
# `scheduler.trigger_via_api` posts with `timeout=10`. A monitoring run takes
# tens of minutes (CBE measured ~40), so a synchronous endpoint hands the
# scheduler a ReadTimeout on every successful run — the shape the existing
# /trigger/CBB/monitoring endpoint has. These start the job on a thread and
# answer 202 straight away, so the scheduler's 10 seconds is plenty and the
# HTTP call reports whether the job STARTED, which is the only thing it can
# honestly know.
#
# CONCURRENCY IS ALREADY HANDLED, and not here: every monitor_* job takes the
# `_run_exclusive` file lock in jobs/monitor_jobs.py, so a second one started
# while another is running returns {"skipped": true} rather than crashing a
# browser. This module does not need its own lock and must not add one — a
# second guard that disagrees with the first is how the two drift.
#
# THE STATE BELOW IS IN MEMORY AND IS LOST ON RESTART, deliberately. It answers
# "what did THIS api process start", nothing more. The durable record of what
# ran is `run_history` in the database, which the completeness gate reads; do
# not build reporting on this dict.

_MONITOR_JOBS = {
    "monitor_cheap_probes", "monitor_sama", "monitor_mc", "monitor_cma",
    "monitor_mlcu", "monitor_cbe", "monitor_cbb", "monitor_bahrain_bourse",
    "monitor_rera", "monitor_sio", "monitor_lloc",
    # Added 2026-09-17 -- all six existed and worked in jobs/monitor_jobs.py
    # and in scheduler.py's own job mappings, but were missing from THIS
    # allowlist, so /trigger/monitor/{job} 404'd "unknown monitoring job" for
    # every one of them even though the function ran fine from a cron slot.
    "monitor_cbj", "monitor_edb", "monitor_mlsd", "monitor_lmra",
    "monitor_justice_canada", "monitor_nbr",
    # monitor_moic / monitor_pdpa existed in jobs/monitor_jobs.py but were
    # wired NOWHERE -- not here, not in scheduler.py, not in
    # config/scheduler.yml. Fixed in all three places 2026-09-17.
    "monitor_moic", "monitor_pdpa",
    # Snapshot-first jobs (2026-09-21): they read a saved page, never the site,
    # unless allow_live is on and the saved page is due. See jobs/monitor_jobs.py.
    "monitor_simah", "monitor_saudi_exchange",
    # NCA, 2026-09-25.
    "monitor_nca",
}

_monitor_state: Dict[str, Dict[str, Any]] = {}
_monitor_lock = Lock()


def _job_callable(name: str):
    """Resolve a pipeline job name to its callable, lazily.

    Monitor jobs live in jobs.monitor_jobs; the three legacy per-regulator
    orchestrator pipelines are already imported at the top of this module
    from scheduler.scheduler. Keeping both kinds in one flat namespace means
    /trigger/monitor/{job} and /trigger/regulators (below) share the exact
    same running-job lock and state -- triggering "SBP" from one and
    "sbp_pipeline" from the other cannot start two overlapping SBP crawls.
    """
    if name == "sbp_pipeline":
        return run_sbp_pipeline
    if name == "secp_pipeline":
        return run_secp_pipeline
    import jobs.monitor_jobs as mj
    return getattr(mj, name)


def _persist_run(name, regulators, state, started, error, result) -> None:
    """Keep the run for the review API. Never allowed to break the job's own record."""
    try:
        run_store.save_run(repo, name, regulators, state, started, datetime.utcnow().isoformat(), error, result)
    except Exception as e:
        logger.error("could not store the run of %s: %s", name, e, exc_info=True)


def _run_monitor_job(name: str) -> None:
    """Run one pipeline job on a worker thread and record how it ended."""
    started = datetime.utcnow().isoformat()
    with _monitor_lock:
        regulators = list((_monitor_state.get(name) or {}).get("regulators") or [])
    try:
        result = _job_callable(name)()
        _persist_run(name, regulators, "finished", started, None, result)
        with _monitor_lock:
            _monitor_state[name] = {
                "state": "finished", "started_at": started, "regulators": regulators,
                "finished_at": datetime.utcnow().isoformat(),
                # The job's own report, unedited. `skipped: true` means the
                # exclusive lock was held — a normal outcome, not a failure.
                "result": result,
            }
    except Exception as e:
        logger.error("monitor job %s failed: %s", name, e, exc_info=True)
        _persist_run(name, regulators, "failed", started, f"{type(e).__name__}: {e}", None)
        with _monitor_lock:
            _monitor_state[name] = {
                "state": "failed", "started_at": started, "regulators": regulators,
                "finished_at": datetime.utcnow().isoformat(),
                "error": f"{type(e).__name__}: {e}",
            }


@app.post("/trigger/monitor/{job}", tags=["Monitoring"], status_code=202)
def trigger_monitor_job(job: str):
    """Start a monitoring job. Returns as soon as it has STARTED, not finished.

    A run takes tens of minutes. Poll `GET /trigger/monitor/{job}` for the
    outcome, or read `run_history` in the database for the durable record.
    """
    if job not in _MONITOR_JOBS:
        raise HTTPException(status_code=404, detail={
            "error": f"unknown monitoring job {job!r}",
            "available": sorted(_MONITOR_JOBS)})

    with _monitor_lock:
        current = _monitor_state.get(job) or {}
        if current.get("state") == "running":
            # Not an error: the caller asked for something already happening.
            return {"job": job, "state": "already_running",
                    "started_at": current.get("started_at")}
        _monitor_state[job] = {"state": "running",
                               "started_at": datetime.utcnow().isoformat()}

    Thread(target=_run_monitor_job, args=(job,), daemon=True,
           name=f"monitor-{job}").start()
    logger.info("monitoring job %s started via API", job)
    return {"job": job, "state": "started",
            "started_at": _monitor_state[job]["started_at"],
            "poll": f"/trigger/monitor/{job}"}


@app.get("/trigger/monitor/{job}", tags=["Monitoring"])
def monitor_job_status(job: str):
    """What this API process last saw of one monitoring job."""
    if job not in _MONITOR_JOBS:
        raise HTTPException(status_code=200, detail={
            "error": f"unknown monitoring job {job!r}",
            "available": sorted(_MONITOR_JOBS)})
    with _monitor_lock:
        state = dict(_monitor_state.get(job) or {"state": "never_started"})
    state["job"] = job
    return state


@app.get("/trigger/monitor", tags=["Monitoring"])
def list_monitor_jobs():
    """Every monitoring job this API can start, and its state in this process."""
    with _monitor_lock:
        seen = {k: dict(v) for k, v in _monitor_state.items()}
    return {"jobs": sorted(_MONITOR_JOBS),
            "state": seen,
            "note": ("state is in memory and resets when the api restarts; "
                     "run_history in the database is the durable record")}


class LLMSettingsBody(BaseModel):
    model: str


@app.get("/llm/models", tags=["LLM"])
def llm_models(search: Optional[str] = None, refresh: bool = False):
    """OpenRouter's model catalogue (cached an hour). `search` filters by id/name.
    Prices are USD per token."""
    from storage import llm_settings
    try:
        models = llm_settings.list_models(refresh=refresh)
    except Exception as e:
        raise HTTPException(502, f"could not reach OpenRouter: {e}")
    if search:
        q = search.lower()
        models = [m for m in models if q in m["id"].lower() or q in (m["name"] or "").lower()]
    return {"count": len(models), "models": models}


@app.get("/llm/settings", tags=["LLM"])
def llm_get_settings():
    from storage import llm_settings
    return {"model": llm_settings.get_model(repo)}


@app.put("/llm/settings", tags=["LLM"])
def llm_put_settings(body: LLMSettingsBody):
    """Choose the model for analyses that START after this call. Must be an id
    from /llm/models."""
    from storage import llm_settings
    from processor.llm_client import DEFAULT_MODEL
    try:
        known = {m["id"] for m in llm_settings.list_models()}
    except Exception as e:
        raise HTTPException(502, f"cannot verify the model, OpenRouter unreachable: {e}")
    if body.model not in known:
        raise HTTPException(400, f"{body.model!r} is not an OpenRouter model id; see GET /llm/models")
    llm_settings.set_model(repo, body.model)
    out = {"model": body.model}
    if body.model != DEFAULT_MODEL:
        out["note"] = ("provider pinning is off for this model, so results are "
                       "less reproducible run to run than with " + DEFAULT_MODEL)
    return out


@app.get("/llm/usage", tags=["LLM"])
def llm_usage(since: Optional[str] = None, until: Optional[str] = None,
              group_by: str = "model", include_openrouter: bool = True):
    """Token usage, two views.

    `uc`: calls made by this app's analysis, from our own records (counts from
    when recording was added; group_by is model, day, step or regulation;
    since/until are ISO dates, until exclusive).
    `openrouter`: everything on the API key, per OpenRouter, all-time. Sections
    OpenRouter refuses are listed under `openrouter.errors`. Pass
    include_openrouter=false to skip it (null in the response) -- it costs two
    outbound calls per request, and the web dashboard only shows `uc`."""
    from storage import llm_settings
    try:
        uc = llm_settings.usage_summary(repo, since, until, group_by)
    except ValueError as e:
        raise HTTPException(200, str(e))
    except Exception as e:
        raise HTTPException(500, f"could not read usage records: {e}")
    return {"uc": uc,
            "openrouter": llm_settings.openrouter_usage() if include_openrouter else None}


@app.get("/monitoring/staleness", tags=["Monitoring"])
def monitoring_staleness():
    """Regulators with no update (new/modified document or withdrawal) for longer
    than their interval in config/staleness.yml. Empty `stale` means all current."""
    from jobs.staleness_alert import check_staleness
    return check_staleness(repo)


# ============================================================================
#  UNIFIED REGULATOR TRIGGER — one call, one or many regulators             #
# ============================================================================
# WHY THIS EXISTS
# ---------------
# /trigger/{regulator} and /trigger/monitor/{job} already exist, but neither
# answers "run this regulator, whichever underlying job that actually is" --
# the caller has to already know that SAMA's real signal is monitor_sama (not
# run_sama_pipeline), that CBB means the orchestrator's TR-feed path, and that
# ZATCA/MOE/SDAIA/AML/MHRSD/KDIPA are not separate jobs at all but one shared
# monitor_cheap_probes sweep. REGULATOR_REGISTRY is that mapping, written down
# once instead of re-derived by every caller.
#
# THIS ALWAYS RUNS THE MONITORING PATH, NEVER A FROM-ZERO RE-INGEST. Every job
# reachable here already diffs against what regulations already holds:
# run_sbp_pipeline / run_secp_pipeline call orchestrator.filter_new_documents
# on every run, CBB is monitor_cbb (config/sources/cbb.yml), and every monitor_* job is a change-detection
# sweep by construction (jobs/monitor_jobs.py's module docstring). There is no
# "wipe and re-crawl everything" mode reachable from this endpoint.
#
# THREE REGULATOR STATUSES, so a caller cannot mistake one kind of "can't run
# this" for another:
#   active   -- has a working job, reachable today.
#   blocked  -- deliberately has NO job. Nothing is in this state today: Saudi
#               Exchange and SIMAH were, until 2026-09-21, and now run as
#               SNAPSHOT-FIRST jobs (see their registry entries). The status stays
#               so a host that gets blocked in future can be refused outright,
#               rather than 404ing as though the name were a typo.
#   unwired  -- has a config/sources/*.yml but no monitoring job was ever
#               written. No regulator is in this state today: MISA was, until
#               2026-09-21, when it was added to CHEAP_PROBE_SOURCES. The status
#               stays so the next unlisted regulator shows up as a distinct
#               answer rather than a 404.
_ACRONYM_RE = re.compile(r'\(([A-Za-z0-9]+)\)\s*$')

REGULATOR_REGISTRY: Dict[str, Dict[str, Any]] = {
    "SBP":  {"job": "sbp_pipeline",  "display": "State Bank of Pakistan (SBP)", "status": "active"},
    "SECP": {"job": "secp_pipeline", "display": "Securities and Exchange Commission of Pakistan (SECP)", "status": "active"},
    # SAMA has two pipelines; this deliberately picks the MONITORING one
    # (SAMA's own revision feed) rather than run_sama_pipeline's full sweep.
    "SAMA": {"job": "monitor_sama",  "display": "Saudi Arabian Monetary Authority (SAMA)", "status": "active"},
    # Of the three CBB implementations found in this audit -- the orchestrator's
    # TR-feed path, the bespoke /trigger/CBB/monitoring crawler, and
    # jobs.monitor_jobs.monitor_cbb's generic config crawl -- this one was
    # confirmed as canonical: it is the actively-documented, fully-versioned
    # path (see orchestrator.py's BaseOrchestrator docstring).
    "CBB":  {"job": "monitor_cbb", "display": "Central Bank of Bahrain", "status": "active"},

    "MC":   {"job": "monitor_mc",   "display": "Ministry of Commerce", "status": "active"},
    "CMA":  {"job": "monitor_cma",  "display": "Capital Market Authority (CMA)", "status": "active"},
    "MOH":  {"job": "monitor_cheap_probes", "display": "Ministry of Health", "status": "active"},
    "MLCU": {"job": "monitor_mlcu", "display": "Egyptian Anti-Money Laundering and Counter-Terrorism Financing Unit (MLCU)", "status": "active"},
    "CBE":  {"job": "monitor_cbe",  "display": "Central Bank of Egypt (CBE)", "status": "active"},
    "BAHRAIN_BOURSE": {"job": "monitor_bahrain_bourse", "display": "Bahrain Bourse (BHB)", "status": "active"},
    "RERA": {"job": "monitor_rera", "display": "Real Estate Regulatory Authority (RERA)", "status": "active"},
    "SIO":  {"job": "monitor_sio",  "display": "Social Insurance Organisation (SIO)", "status": "active"},
    "LLOC": {"job": "monitor_lloc", "display": "Legislation and Legal Opinion Commission (LLOC)", "status": "active"},
    "CBJ":  {"job": "monitor_cbj",  "display": "Central Bank of Jordan (CBJ)", "status": "active"},
    "EDB":  {"job": "monitor_edb",  "display": "Bahrain Economic Development Board (EDB)", "status": "active"},
    "MLSD": {"job": "monitor_mlsd", "display": "Ministry of Labour and Social Development (MLSD)", "status": "active"},
    "LMRA": {"job": "monitor_lmra", "display": "Labour Market Regulatory Authority (LMRA)", "status": "active"},
    "NBR":  {"job": "monitor_nbr",  "display": "National Bureau for Revenue (NBR)", "status": "active"},
    "JUSTICE_CANADA": {"job": "monitor_justice_canada", "display": "Department of Justice Canada (JUS)", "status": "active"},
    "MOIC": {"job": "monitor_moic", "display": "Ministry of Industry and Commerce (MOIC)", "status": "active"},
    "PDPA": {"job": "monitor_pdpa", "display": "Personal Data Protection Authority (PDPA)", "status": "active"},
    "NCA":  {"job": "monitor_nca",  "display": "National Cybersecurity Authority (NCA)", "status": "active"},

    # Bundled into ONE shared job with MOH -- see CHEAP_PROBE_SOURCES in
    # jobs/monitor_jobs.py. Requesting any one (or several) of these six plus
    # MOH starts the same sweep exactly once; the response says so.
    "MOE":   {"job": "monitor_cheap_probes", "display": "Ministry of Education", "status": "active"},
    "SDAIA": {"job": "monitor_cheap_probes", "display": "Saudi Data and AI Authority (SDAIA)", "status": "active"},
    "AML":   {"job": "monitor_cheap_probes", "display": "Anti-Money Laundering Permanent Committee (AML)", "status": "active"},
    "MHRSD": {"job": "monitor_cheap_probes", "display": "Ministry of Human Resource and Social Development (MHRSD)", "status": "active"},
    "ZATCA": {"job": "monitor_cheap_probes", "display": "Zakat, Tax and Customs Authority (ZATCA)", "status": "active"},
    "KDIPA": {"job": "monitor_cheap_probes", "display": "REGULATION GOVERNING COLLECTIVE INVESTMENT SCHEME JUNE 2013", "status": "active"},

    # Joined the shared sweep 2026-09-21 (CHEAP_PROBE_SOURCES). It had been
    # `unwired` only because nobody listed it, not by decision.
    "MISA":  {"job": "monitor_cheap_probes", "display": "Ministry of Investment (MISA)", "status": "active"},

    # SNAPSHOT-FIRST (2026-09-21). Both hosts blocked us after repeated automated
    # visits, so these jobs read a SAVED page and cannot iterate against the site:
    # the saved page's own clock decides when a live visit is allowed (one, no
    # retry, backing off 6h..14d after a block) and `allow_live` in
    # config/sources/<name>.yml ships false. Triggering one of these through this
    # API therefore replays the saved page and makes no request. `snapshot` names
    # the saved page so GET /trigger/regulators can report its state.
    "SIMAH": {"job": "monitor_simah", "display": "Saudi Credit Bureau (SIMAH)",
              "status": "active", "snapshot": "simah.rules", "snapshot_source": "simah"},
    "SAUDI_EXCHANGE": {"job": "monitor_saudi_exchange", "display": "Saudi Exchange",
                       "status": "active", "snapshot": "tadawul.rules",
                       "snapshot_source": "saudi_exchange"},
}


def _resolve_regulator_key(raw: str) -> Optional[str]:
    """Match a caller-supplied regulator string against REGULATOR_REGISTRY --
    its short key ("CBE"), its full display name ("Central Bank of Egypt
    (CBE)"), or the acronym inside that name's parentheses -- all
    case-insensitive. None if nothing matches."""
    v = (raw or "").strip()
    if not v:
        return None
    if v.upper() in REGULATOR_REGISTRY:
        return v.upper()
    vf = v.casefold()
    for key, entry in REGULATOR_REGISTRY.items():
        if entry["display"].casefold() == vf:
            return key
        m = _ACRONYM_RE.search(entry["display"])
        if m and m.group(1).casefold() == vf:
            return key
    return None


class RegulatorTriggerRequest(BaseModel):
    regulators: List[str]


@app.get("/trigger/regulators", tags=["Monitoring"])
def list_regulator_triggers():
    """Every regulator this API knows about: its underlying job (None if it
    has no job) and whether it is active / blocked / unwired. See the module
    comment above for what each status means."""
    from jobs.monitor_jobs import snapshot_report
    out = {}
    for key, v in sorted(REGULATOR_REGISTRY.items()):
        entry = {"display": v["display"], "status": v["status"], "job": v["job"]}
        if v.get("snapshot"):
            # Reads a manifest file; makes no request to the site.
            entry["snapshot"] = snapshot_report(v["snapshot"], v.get("snapshot_source"))
        out[key] = entry
    return {"regulators": out}


@app.get("/trigger/regulators/status", tags=["Monitoring"])
def regulator_trigger_status():
    """Current in-memory state of every job reachable from
    POST /trigger/regulators. Shares its state dict with /trigger/monitor, so
    a job started from either endpoint shows up here."""
    jobs = sorted({v["job"] for v in REGULATOR_REGISTRY.values() if v["job"]})
    with _monitor_lock:
        state = {j: dict(_monitor_state.get(j) or {"state": "never_started"})
                 for j in jobs}
    return {"jobs": state,
            "note": ("state is in memory and resets when the api restarts; "
                     "run_history in the database is the durable record")}


@app.post("/trigger/regulators", tags=["Monitoring"], status_code=202)
def trigger_regulators(request: RegulatorTriggerRequest):
    """Start the MONITORING pipeline for one or more regulators.

    Accepts regulator keys, full display names, or bare acronyms (see
    REGULATOR_REGISTRY / _resolve_regulator_key), case-insensitive, e.g.
    `{"regulators": ["CBB", "sama", "Central Bank of Egypt (CBE)"]}`.

    Several regulators can share one underlying job (MOH + MOE + SDAIA + AML +
    MHRSD + ZATCA + KDIPA all run inside monitor_cheap_probes) -- asking for
    more than one of them starts that job exactly once, not once per name.

    Returns as soon as every resolved job has STARTED, not finished -- these
    are the same long-running jobs behind /trigger/monitor/{job}, some of
    which take hours. Poll GET /trigger/regulators/status, or
    GET /trigger/monitor/{job} for one job by name, for the outcome. The
    durable record is `run_history` in the database.
    """
    if not request.regulators:
        raise HTTPException(400, "Provide at least one regulator")

    resolved: Dict[str, Dict[str, Any]] = {}      # as-requested -> outcome
    jobs_to_start: Dict[str, list] = {}           # job name -> [regulator keys]

    for raw in request.regulators:
        key = _resolve_regulator_key(raw)
        if not key:
            resolved[raw] = {"status": "unknown_regulator",
                             "available": sorted(REGULATOR_REGISTRY)}
            continue
        entry = REGULATOR_REGISTRY[key]
        if entry["status"] == "blocked":
            resolved[raw] = {
                "status": "blocked", "regulator": key,
                "reason": ("this host was permanently/near-permanently blocked "
                           "after automated access from this project; it is "
                           "never retried by this API -- see the module "
                           "comment above and jobs/monitor_jobs.py"),
            }
            continue
        if entry["status"] == "unwired":
            resolved[raw] = {
                "status": "unwired", "regulator": key,
                "reason": "configured (config/sources/*.yml) but no monitoring job has been written yet",
            }
            continue
        job = entry["job"]
        jobs_to_start.setdefault(job, []).append(key)
        resolved[raw] = {"status": "triggered", "regulator": key,
                         "display": entry["display"], "job": job}

    jobs_outcome: Dict[str, Dict[str, Any]] = {}
    for job, keys in jobs_to_start.items():
        with _monitor_lock:
            current = _monitor_state.get(job) or {}
            if current.get("state") == "running":
                outcome = {"state": "already_running",
                          "started_at": current.get("started_at")}
            else:
                _monitor_state[job] = {"state": "running",
                                       "started_at": datetime.utcnow().isoformat(),
                                       "regulators": keys}
                outcome = {"state": "started",
                          "started_at": _monitor_state[job]["started_at"]}
        if outcome["state"] == "started":
            Thread(target=_run_monitor_job, args=(job,), daemon=True,
                  name=f"monitor-{job}").start()
            logger.info("regulator trigger started job %s for %s", job, keys)
        jobs_outcome[job] = {**outcome, "regulators": keys}

    for raw, info in resolved.items():
        if info.get("status") == "triggered":
            info.update(jobs_outcome[info["job"]])

    return {
        "requested": resolved,
        "jobs": jobs_outcome,
        "poll": "/trigger/regulators/status",
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
            SELECT id, ref_key, regulator, source_system, category, title, document_url, document_html,
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


@app.get("/regulations")
def get_regulations(
    regulator: Optional[List[str]] = Query(
        None, description="One or more regulators -- either the exact full name or just its "
                          "acronym, e.g. ?regulator=CBE&regulator=MLCU or "
                          "?regulator=Central Bank of Egypt (CBE), case-insensitive either "
                          "way. Omit to include every regulator."),
    country: Optional[str] = Query(
        None, description="One country -- exact name from config/countries.yml (e.g. "
                          "'Bahrain') or its alpha-3 code (e.g. 'BHR'), case-insensitive. "
                          "Omit to include every country. Combined with regulator as AND: "
                          "country narrows which regulators are eligible, regulator (if also "
                          "given) narrows further within that."),
    # Optional and defaults to None, not 100 -- omit it (or send nothing)
    # and every matching row comes back, no cap at all. Pass a number to
    # actually page. No upper bound on the number itself either -- SQL
    # Server's OFFSET/FETCH NEXT naturally caps out at however many rows
    # match, so there's nothing left for an artificial ceiling to protect.
    limit: Optional[int] = Query(None, ge=1),
    offset: int = Query(0, ge=0),
    lang: str = Query("en"),
):
    """Regulations grouped by country. regulator accepts multiple values;
    country takes exactly one -- see their own descriptions above for how
    they combine. Leave limit unset for everything matching the filter, in
    one response, sorted by published_date desc; pass it to page instead.
    """
    from utils.countries import resolve_country, resolve_regulator, regulators_for_country, country_for

    lang = _validate_lang(lang)
    try:
        # country -> canonical name, rejecting anything that matches neither
        # a name nor a code -- silently ignoring a typo'd country is worse
        # than telling the caller it matched nothing.
        resolved_country = None
        if country:
            resolved_country = resolve_country(country)
            if not resolved_country:
                raise HTTPException(
                    status_code=200,
                    detail=f"Unknown country: {country!r}. Use a name from "
                          f"config/countries.yml or its alpha-3 code.")

        # Two independent filters on the SAME underlying column
        # (regulations.regulator) -- country resolves to the regulators filed
        # under it, `regulator` is the caller's own explicit list (each value
        # resolved from a bare acronym to its full stored name where one
        # matches; passed through as-is otherwise -- see resolve_regulator's
        # own docstring for why an unresolved value is not an error here).
        # When both are given, AND means the intersection; when only one is
        # given, that one alone; when neither, no filter at all
        # (effective_regulators stays None, distinct from "resolved to zero
        # regulators").
        country_regulators = None
        if resolved_country:
            country_regulators = {r.upper() for r in regulators_for_country(resolved_country)}
        explicit_regulators = (
            {(resolve_regulator(r) or r).upper() for r in regulator} if regulator else None
        )

        if country_regulators is not None and explicit_regulators is not None:
            effective_regulators = country_regulators & explicit_regulators
        elif country_regulators is not None:
            effective_regulators = country_regulators
        else:
            effective_regulators = explicit_regulators  # may be None -- no filter

        if lang == "ar":
            cache_key = (
                f"GET /regulations?regulator={sorted(regulator or [])}"
                f"&country={country}&limit={limit}&offset={offset}"
            )
            cached = _get_ar_cache(cache_key)
            if cached:
                return Response(
                    content=json.dumps(cached, ensure_ascii=False),
                    media_type="application/json",
                )

        def _where(alias_prefix: str = "r.") -> Tuple[str, list]:
            clauses, params = [], []
            if effective_regulators is not None:
                if not effective_regulators:
                    # country and regulator were both given and share nothing
                    # -- a real, valid answer ("zero rows"), not an error.
                    clauses.append("1 = 0")
                else:
                    placeholders = ", ".join("?" for _ in effective_regulators)
                    clauses.append(f"UPPER({alias_prefix}regulator) IN ({placeholders})")
                    params.extend(sorted(effective_regulators))
            return (" WHERE " + " AND ".join(clauses)) if clauses else "", params

        where_sql, where_params = _where()

        # OFFSET alone is valid T-SQL and returns everything from `offset`
        # onward with no cap; FETCH NEXT is only appended when a limit was
        # actually given. This is what makes "leave limit unset" mean
        # "everything", not "the server's own idea of a reasonable page size".
        fetch_clause = " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY" if limit is not None else " OFFSET ? ROWS"
        page_params = [offset, limit] if limit is not None else [offset]

        # Sorted by created_at, not published_date -- checked directly:
        # published_date is NULL on 2,692 rows and present-but-unparseable
        # (TRY_CONVERT fails) on another 336, ~a third of the table. SQL
        # Server sorts NULL as the lowest value, so DESC pushed every one of
        # those rows to the very last page, invisible under any real `limit`.
        # created_at has zero NULLs and zero unparseable values -- it's set
        # by the crawler/orchestrator at insert time, not scraped off a
        # source page, so it can't have the same gap.
        query = f"""
            SELECT r.id, r.ref_key, r.regulator, r.source_system, r.category, r.title, r.document_url,
                   r.document_html, TRY_CONVERT(DATETIME, r.published_date, 103) AS published_date,
                   r.reference_no, r.department, r.doc_path, r.[year], r.source_page_url,
                   r.extra_meta, TRY_CAST(r.created_at AS DATETIME) AS created_at,
                   TRY_CAST(r.updated_at AS DATETIME) AS updated_at, r.compliancecategory_id,
                   cc.title AS category_title, cc.parentid AS category_parent_id, cc.type AS category_type
            FROM regulations r
            LEFT JOIN compliancecategory cc ON r.compliancecategory_id = cc.compliancecategory_id
            {where_sql}
            ORDER BY TRY_CAST(r.created_at AS DATETIME) DESC{fetch_clause}
        """
        params = where_params + page_params

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows     = cursor.fetchall()
            columns  = [col[0] for col in cursor.description]
            raw_rows = [row_to_dict(row, columns) for row in rows]

            regulations = []
            for reg_dict in raw_rows:
                if reg_dict.get("document_html"):
                    reg_dict["document_html"] = reg_dict["document_html"].replace('\\"', '"')
                if reg_dict.get("doc_path"):
                    try:
                        reg_dict["doc_path"] = json.loads(reg_dict["doc_path"])
                    except Exception:
                        pass
                if reg_dict.get("extra_meta"):
                    try:
                        # ALLOWLIST, not pop(). Popping two known-bad keys
                        # publishes every key a crawler invents next. See
                        # utils/public_meta.py.
                        reg_dict["extra_meta"] = public_extra_meta(
                            reg_dict["extra_meta"])
                    except Exception:
                        pass
                reg_dict["category_info"] = {
                    "id":        reg_dict.pop("compliancecategory_id", None),
                    "title":     reg_dict.pop("category_title", None),
                    "parent_id": reg_dict.pop("category_parent_id", None),
                    "type":      reg_dict.pop("category_type", None),
                }
                regulations.append(reg_dict)

            count_where_sql, count_where_params = _where()
            cursor.execute(f"SELECT COUNT(*) FROM regulations r{count_where_sql}", count_where_params)
            total_count = cursor.fetchone()[0]

            # Per-country totals across ALL matching rows, not just this page
            # -- one GROUP BY, then bucket each regulator's count under its
            # country in Python, rather than fetching every row just to count.
            group_where_sql, group_where_params = _where(alias_prefix="")
            cursor.execute(
                f"SELECT regulator, COUNT(*) FROM regulations{group_where_sql} GROUP BY regulator",
                group_where_params)
            country_totals: Dict[str, int] = {}
            for reg_name, cnt in cursor.fetchall():
                bucket = country_for(reg_name) or "Unlisted"
                country_totals[bucket] = country_totals.get(bucket, 0) + cnt

        if lang == "ar":
            regulations = [translate_regulation(reg, lang) for reg in regulations]

        # Bucket THIS PAGE's rows by country. A regulator with no entry in
        # config/countries.yml (e.g. SBP/SECP today -- Pakistan isn't listed
        # yet) buckets under "Unlisted" rather than being dropped silently.
        grouped: Dict[str, dict] = {}
        for reg_dict in regulations:
            bucket = country_for(reg_dict.get("regulator")) or "Unlisted"
            grouped.setdefault(bucket, {"total": country_totals.get(bucket, 0), "regulations": []})
            grouped[bucket]["regulations"].append(reg_dict)
        # A country can have a total > 0 (rows exist) but none on THIS page
        # -- still worth listing with an empty page-slice, so pagination
        # doesn't make a country silently vanish from the response shape.
        for bucket, total in country_totals.items():
            grouped.setdefault(bucket, {"total": total, "regulations": []})

        response_data = {
            "success": True,
            "lang": lang,
            "filters": {
                "regulator": regulator, "country": resolved_country,
            },
            "data": grouped,
            "pagination": {
                "total":        total_count,
                "limit":        limit,
                "offset":       offset,
                # limit=None means every matching row (from offset onward)
                # already came back in this one response -- there is no
                # "more" to page to, regardless of how large total is.
                "has_more":     False if limit is None else (offset + limit) < total_count,
                "current_page": 1 if limit is None else (offset // limit) + 1,
                "total_pages":  1 if limit is None else (total_count + limit - 1) // limit if limit else 1,
            },
        }

        if lang == "ar":
            _set_ar_cache(cache_key, response_data)

        return Response(
            content=json.dumps(response_data, ensure_ascii=False, default=str),
            media_type="application/json",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching regulations (regulator={regulator}, country={country})")
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
            SELECT r.id, r.ref_key, r.regulator, r.source_system, r.category, r.title, r.document_url,
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
                raise HTTPException(status_code=200, detail="Regulation not found")
            columns  = [col[0] for col in cursor.description]
            reg_dict = row_to_dict(row, columns)
            if reg_dict.get("document_html"):
                reg_dict["document_html"] = reg_dict["document_html"].replace('\\"', '"')
            if reg_dict.get("doc_path"):
                try:
                    reg_dict["doc_path"] = json.loads(reg_dict["doc_path"])
                except Exception:
                    pass
            if reg_dict.get("extra_meta"):
                try:
                    # ALLOWLIST, not pop() -- see utils/public_meta.py.
                    reg_dict["extra_meta"] = public_extra_meta(
                        reg_dict["extra_meta"])
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
#  REQUIREMENT / ACTIVITY ENDPOINTS                                    #
#  Stage A (RequirementAnalyzer) + Stage B (ActivityAnalyzer), then     #
#  processor.requirement_activity_sync writes only the diff against    #
#  what's already stored. Same code path orch.py runs during a crawl   #
#  (_run_requirement_activity_analysis), triggerable here for one      #
#  regulation on demand instead of waiting for a recrawl.              #
# ================================================================== #

_analysis_state: Dict[int, Dict[str, Any]] = {}
_analysis_lock = Lock()


def _set_analysis_stage(regulation_id: int, stage: str, **extra) -> None:
    """Update the in-progress state without clobbering started_at -- the
    frontend polls this to render a 3-step log (extract text / generate
    requirements / generate activities) instead of a single 'running'."""
    with _analysis_lock:
        current = _analysis_state.get(regulation_id) or {}
        current.update(state="running", stage=stage, **extra)
        _analysis_state[regulation_id] = current


def _run_requirement_activity_analysis_for_regulation(regulation_id: int) -> None:
    try:
        row = repo.get_regulation_by_id(regulation_id)
        if not row:
            raise ValueError(f"no regulations row with id={regulation_id}")
        version = repo.get_active_regulation_version(regulation_id) or {}
        version_id = version.get("version_id")
        if not version_id:
            raise ValueError(f"regulation {regulation_id} has no active "
                             f"regulation_versions row -- nothing to attach spans to")

        meta = row.get("extra_meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}

        # scripts.run_regulation_by_id no longer exists (its .pyc in scripts/__pycache__
        # is the only trace left -- the .py was never committed, so this import broke
        # silently for every caller: POST /regulation/{id}/analyze, /analysis/trigger,
        # /analysis/trigger/run/{run_id}). Same download+OCR/office-extract logic already
        # lives on the orchestrator (_download_and_extract_file dispatches by extension:
        # .pdf through OCR, .docx/.xlsx/.xls through processor/office_text_extractor).
        # crawler=None is fine -- this helper never calls fetch_documents().
        from processor.downloader import Downloader
        _fetch_orch = Orchestrator(crawler=None, repo=repo, downloader=Downloader())
        fetch_attachment_text = _fetch_orch._download_and_extract_file

        _set_analysis_stage(regulation_id, "extracting_text")
        documents = []
        # org_pdf_text is the established slot for pre-OCR'd PDF text
        # elsewhere in this codebase (upload-regulation, analyze/document,
        # orchestrator.py's own "Tier 1a: SAMA pre-OCR'd PDF text") --
        # checked ahead of content_text/document_html for the same reason
        # orchestrator.py does: it is text OCR'd FOR analysis specifically,
        # not a copy of crawled markup, and deliberately excluded from the
        # public API (utils/public_meta.py's NEVER_PUBLISH) since OCR output
        # is often rough and isn't fit to show as "the document".
        main_text = (meta.get("org_pdf_text") or meta.get("content_text")
                    or row.get("document_html") or "").strip()
        if len(main_text) < 200 and row.get("document_url"):
            # Nothing stored at all -- e.g. a PDF-only crawl that was never
            # OCR'd (this is exactly what CBE's Presidential Decree rows
            # hit). Extract it now via the same OCR path already used for
            # attachments below, and persist it so this regulation doesn't
            # need re-extracting on every future analysis run.
            ocr_text = fetch_attachment_text(row["document_url"])
            if ocr_text and len(ocr_text.strip()) >= 200:
                main_text = ocr_text.strip()
                # Both keys, not just org_pdf_text: other code paths in this
                # codebase check content_text (orchestrator.py's Tier 1b)
                # rather than org_pdf_text (Tier 1a), so writing only one
                # would leave this regulation invisible to whichever
                # convention that other path happens to follow.
                updated_meta = dict(meta)
                updated_meta["org_pdf_text"] = main_text
                updated_meta["content_text"] = main_text
                repo.update_regulation(regulation_id,
                                       extra_meta=json.dumps(updated_meta, ensure_ascii=False))
                logger.info(f"backfilled extra_meta.org_pdf_text/content_text for regulation "
                           f"{regulation_id} from document_url OCR ({len(main_text)} chars)")
        if len(main_text) >= 200:
            documents.append({"source_document": "main_body", "text": main_text})
        raw_links = meta.get("attachment_links") or ""
        for url in [u.strip() for u in str(raw_links).split("|") if u.strip()]:
            text = fetch_attachment_text(url)
            if text and len(text.strip()) >= 200:
                name = url.rsplit("/", 1)[-1][:80] or url
                documents.append({"source_document": name, "text": text})
        if not documents:
            raise ValueError("no document in this regulation's bundle produced "
                             ">=200 chars of usable text -- nothing to analyse")

        requirement_types = repo.get_requirement_types()

        from processor.requirement_analyzer import RequirementAnalyzer
        from processor.activity_analyzer import ActivityAnalyzer
        from processor.requirement_activity_sync import sync_requirements_and_activities

        _set_analysis_stage(regulation_id, "generating_requirements",
                            documents=[d["source_document"] for d in documents])
        from storage import llm_settings
        model = llm_settings.get_model(repo)
        req_analyzer = RequirementAnalyzer(model=model)
        req_analyzer.client.on_usage = llm_settings.make_usage_recorder(
            repo, step="requirements",
            regulation_id=regulation_id, version_id=version_id)
        stage_a = req_analyzer.extract_and_classify(
            documents=documents, document_title=row.get("title") or "",
            requirement_types=requirement_types, regulator=row.get("regulator") or "",
            reference=row.get("reference_no") or "",
            publication_date=str(row.get("published_date") or ""))
        requirements = stage_a["requirements"]
        chunk_texts = stage_a["chunk_texts"]

        activities = []
        if requirements:
            _set_analysis_stage(regulation_id, "generating_activities",
                                requirements_extracted=len(requirements))
            act_analyzer = ActivityAnalyzer(model=model)
            act_analyzer.client.on_usage = llm_settings.make_usage_recorder(
                repo, step="activities",
                regulation_id=regulation_id, version_id=version_id)
            activities = act_analyzer.design_activities(
                requirements=requirements, chunk_texts=chunk_texts)

        counts = sync_requirements_and_activities(
            repo, regulation_id, version_id, requirements, activities)

        with _analysis_lock:
            _analysis_state[regulation_id] = {
                "state": "done",
                "stage": "done",
                "finished_at": datetime.utcnow().isoformat(),
                "documents": [d["source_document"] for d in documents],
                "requirements_extracted": len(requirements),
                "activities_extracted": len(activities),
                "counts": counts,
            }
    except Exception as e:
        logger.exception(f"requirement/activity analysis failed for regulation {regulation_id}")
        with _analysis_lock:
            _analysis_state[regulation_id] = {
                "state": "failed",
                "finished_at": datetime.utcnow().isoformat(),
                "error": str(e),
            }


@app.post("/regulation/{regulation_id}/analyze", tags=["Requirements & Activities"],
         status_code=202)
def trigger_requirement_activity_analysis(regulation_id: int):
    """Start Stage A/B requirement+activity extraction for one regulation's
    stored content. Returns as soon as the run has STARTED, not finished --
    an LLM run over a real document takes real time. Poll
    GET /regulation/{regulation_id}/analyze for the outcome."""
    row = repo.get_regulation_by_id(regulation_id)
    if not row:
        raise HTTPException(status_code=404, detail="Regulation not found")

    with _analysis_lock:
        current = _analysis_state.get(regulation_id) or {}
        if current.get("state") == "running":
            return {"regulation_id": regulation_id, "state": "already_running",
                    "started_at": current.get("started_at")}
        _analysis_state[regulation_id] = {"state": "running", "stage": "queued",
                                          "started_at": datetime.utcnow().isoformat()}

    Thread(target=_run_requirement_activity_analysis_for_regulation,
          args=(regulation_id,), daemon=True,
          name=f"reqact-{regulation_id}").start()
    logger.info("requirement/activity analysis started via API for regulation %s",
               regulation_id)
    return {"regulation_id": regulation_id, "state": "started",
            "started_at": _analysis_state[regulation_id]["started_at"],
            "poll": f"/regulation/{regulation_id}/analyze"}


@app.get("/regulation/{regulation_id}/analyze", tags=["Requirements & Activities"])
def requirement_activity_analysis_status(regulation_id: int):
    """What this API process last saw of this regulation's analysis run.
    In-memory only -- resets when the API restarts; the Requirement/Activity
    tables themselves are the durable record of what was actually written."""
    with _analysis_lock:
        state = dict(_analysis_state.get(regulation_id) or {"state": "never_started"})
    state["regulation_id"] = regulation_id
    return state


# ------------------------------------------------------------------ #
#  BATCH TRIGGERS -- the same requirement/activity analyzers            #
# ------------------------------------------------------------------ #
# Both endpoints below feed ONE runner, `_run_requirement_activity_analysis_for_regulation`
# (RequirementAnalyzer then ActivityAnalyzer, synced into the span tables), so a
# regulation is analysed identically whichever way it was triggered, and shares the
# per-regulation state that GET /regulation/{id}/analyze reports.

_analysis_batches: Dict[str, Dict[str, Any]] = {}
_batch_lock = Lock()
ANALYSIS_BATCH_MAX = 500


class AnalysisTriggerRequest(BaseModel):
    regulation_ids: List[int] = Field(..., min_length=1, max_length=ANALYSIS_BATCH_MAX,
                                      description="regulations to analyse")
    force: bool = Field(False, description="also re-analyse regulations that already have active requirements")


def _start_analysis_batch(ids: List[int], force: bool, source: str) -> Dict[str, Any]:
    ids = list(dict.fromkeys(ids))
    skipped = {"not_found": [], "already_running": [], "already_analysed": []}
    todo = []
    for rid in ids:
        if not repo.get_regulation_by_id(rid):
            skipped["not_found"].append(rid)
            continue
        with _analysis_lock:
            if (_analysis_state.get(rid) or {}).get("state") == "running":
                skipped["already_running"].append(rid)
                continue
        if not force and repo.get_requirements_for_regulation(rid, active_only=True):
            skipped["already_analysed"].append(rid)
            continue
        todo.append(rid)

    batch_id = uuid.uuid4().hex[:12]
    now = datetime.utcnow().isoformat()
    batch = {"batch_id": batch_id, "source": source, "force": force, "state": "running" if todo else "finished",
             "started_at": now, "finished_at": None if todo else now, "total": len(todo),
             "requested": len(ids), "skipped": skipped,
             "items": {rid: {"state": "queued"} for rid in todo}}
    with _batch_lock:
        _analysis_batches[batch_id] = batch
        for old in list(_analysis_batches)[:-50]:          # keep the last 50
            _analysis_batches.pop(old, None)
    if todo:
        with _analysis_lock:
            for rid in todo:
                _analysis_state[rid] = {"state": "running", "stage": "queued", "started_at": now}
        Thread(target=_run_analysis_batch, args=(batch_id, todo), daemon=True,
               name=f"reqact-batch-{batch_id}").start()
    return batch


def _run_analysis_batch(batch_id: str, todo: List[int]) -> None:
    from concurrent.futures import ThreadPoolExecutor
    workers = max(1, int(os.getenv("ANALYSIS_BATCH_WORKERS", "2")))

    def one(rid: int) -> None:
        with _batch_lock:
            _analysis_batches[batch_id]["items"][rid] = {"state": "running"}
        try:
            _run_requirement_activity_analysis_for_regulation(rid)
        except Exception as e:                              # the runner records its own failures; this is a backstop
            with _analysis_lock:
                _analysis_state[rid] = {"state": "failed", "error": str(e),
                                        "finished_at": datetime.utcnow().isoformat()}
        with _analysis_lock:
            st = dict(_analysis_state.get(rid) or {})
        item = {"state": st.get("state", "failed")}
        for k in ("error", "requirements_extracted", "activities_extracted"):
            if k in st:
                item[k] = st[k]
        with _batch_lock:
            _analysis_batches[batch_id]["items"][rid] = item

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(one, todo))
    with _batch_lock:
        _analysis_batches[batch_id].update(state="finished", finished_at=datetime.utcnow().isoformat())


def _batch_view(batch: Dict[str, Any]) -> Dict[str, Any]:
    items = batch["items"]
    counts = {s: sum(1 for i in items.values() if i["state"] == s) for s in ("queued", "running", "done", "failed")}
    return {**{k: v for k, v in batch.items() if k != "items"}, "counts": counts,
            "items": {str(k): v for k, v in items.items()}}


@app.post("/analysis/trigger", tags=["Requirements & Activities"], status_code=202)
def trigger_analysis_batch(request: AnalysisTriggerRequest):
    """Start requirement + activity analysis for a LIST of regulations. Returns at once with
    a `batch_id`; poll GET /analysis/batches/{batch_id}. Regulations that already have active
    requirements are skipped unless `force` is true, so re-sending a list costs nothing for the
    ones already done. Regulations already being analysed, or not found, are reported and skipped."""
    batch = _start_analysis_batch(request.regulation_ids, request.force, source="regulation_ids")
    return {**_batch_view(batch), "poll": f"/analysis/batches/{batch['batch_id']}"}


@app.post("/analysis/trigger/run/{run_id}", tags=["Requirements & Activities"], status_code=202)
def trigger_analysis_for_run(run_id: int,
                             types: List[str] = Query(["new", "modified"], description="new | modified | deleted"),
                             force: bool = False, include_rejected: bool = False):
    """Start requirement + activity analysis for the documents a stored run (GET /runs/{id}) found.
    Defaults to its new and modified documents, leaving out changes a reviewer rejected."""
    bad = [t for t in types if t not in run_store.CHANGE_TYPES]
    if bad:
        raise HTTPException(422, f"types must be from {run_store.CHANGE_TYPES}, got {bad}")
    ids = run_store.regulation_ids_for_run(repo, run_id, types=types, include_rejected=include_rejected)
    if ids is None:
        raise HTTPException(404, f"run {run_id} not found")
    if len(ids) > ANALYSIS_BATCH_MAX:
        raise HTTPException(413, f"run {run_id} has {len(ids)} matching documents; the limit is "
                                 f"{ANALYSIS_BATCH_MAX} per batch. Narrow `types` or send ids in slices "
                                 f"to POST /analysis/trigger.")
    batch = _start_analysis_batch(ids, force, source=f"run:{run_id}")
    return {**_batch_view(batch), "run_id": run_id, "poll": f"/analysis/batches/{batch['batch_id']}"}


@app.get("/analysis/batches/{batch_id}", tags=["Requirements & Activities"])
def analysis_batch_status(batch_id: str):
    """Progress of one batch: counts per state and each regulation's outcome. In memory
    (last 50 batches); the requirement/activity tables are the durable record."""
    with _batch_lock:
        batch = _analysis_batches.get(batch_id)
        if not batch:
            raise HTTPException(200, f"batch {batch_id!r} not found (they are kept in memory, last 50)")
        return _batch_view(batch)


@app.get("/regulation/{regulation_id}/requirements", tags=["Requirements & Activities"])
def get_regulation_requirements(regulation_id: int,
                                active_only: bool = Query(
                                    True, description="False also returns "
                                    "superseded requirements, one row per span")):
    """Every requirement for one regulation, each with its activities nested
    under it. active_only=True (default) is what's in force right now."""
    row = repo.get_regulation_by_id(regulation_id)
    if not row:
        raise HTTPException(status_code=200, detail="Regulation not found")
    try:
        requirements = repo.get_requirements_for_regulation(
            regulation_id, active_only=active_only)
        for r in requirements:
            r["activities"] = repo.get_activities_for_requirement_full(
                r["requirement_id"], active_only=active_only)
        return {"success": True, "regulation_id": regulation_id,
                "count": len(requirements), "data": requirements}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching requirements for regulation {regulation_id}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/regulation/{regulation_id}/activities", tags=["Requirements & Activities"])
def get_regulation_activities(regulation_id: int,
                              active_only: bool = Query(
                                  True, description="False also returns "
                                  "superseded activities, one row per span")):
    """Every activity under every requirement of one regulation, flat --
    each row keeps requirement_ref_key so it can still be traced back."""
    row = repo.get_regulation_by_id(regulation_id)
    if not row:
        raise HTTPException(status_code=200, detail="Regulation not found")
    try:
        activities = repo.get_activities_for_regulation(
            regulation_id, active_only=active_only)
        return {"success": True, "regulation_id": regulation_id,
                "count": len(activities), "data": activities}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching activities for regulation {regulation_id}")
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
                    "extra_meta":     public_extra_meta(regulation.get("extra_meta")),
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
            "extra_meta":     public_extra_meta(regulation.get("extra_meta")),
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
    # Raising 500 causes frontend RxJS to call .toString() on null -> crash
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
        raise HTTPException(200, f"No gap analysis session found for ID {session_id}")

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
            _attach_regulation_counts(cursor, categories)

        if lang == "ar":
            titles     = [c.get("title") or "" for c in categories]
            translated = translate_texts_batch(titles, lang)
            for cat, tr in zip(categories, translated):
                cat["title"] = tr

        # Two passes, not one: `ORDER BY parentid, title` sorts each row by
        # ITS OWN parentid, which says nothing about whether a category's
        # PARENT row has already been reached in this same loop. A single
        # pass that set `children=[]` and attached to the parent together
        # threw `KeyError: 'children'` whenever a child's iteration turn came
        # before its parent's — unmasked once `_attach_regulation_counts`
        # stopped 500ing first. `/categories/root` below already does this
        # correctly in two passes; this now matches it.
        categories_by_id = {cat["compliancecategory_id"]: cat for cat in categories}
        for cat in categories:
            cat["children"] = []
        root_categories = []
        for cat in categories:
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
            _attach_regulation_counts(cursor, root_categories)

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
            _attach_regulation_counts(cursor, categories)

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
            _attach_regulation_counts(cursor, children)

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
def trigger_requirement_matching_v2(regulation_id: int, force: bool = False):
    """
    Trigger requirement matching for a regulation.
    Reads from the unified compliance_analysis table (works for all regulators).
    Passes version_id through to sama_requirement_mapping for CBB rows.

    Cached on the obligations plus the internal register: an unchanged input
    returns the stored verdicts without calling the LLM, because re-asking
    produces different answers on the borderline cases. `force=true` re-runs
    anyway -- expect 2-3 verdicts in 39 to move if you do.
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
                # version_id is whatever was active in regulation_versions when
                # the analysis ran; None only if no version row existed yet.
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

    # ── Matching cache ────────────────────────────────────────────────
    # Re-matching the same obligations against the same register disagrees with
    # itself on 2-3 of every 39, because the disagreements are genuine ties
    # rather than sampling noise -- determinism settings measurably do not help
    # (docs/determinism.md). Deciding once is what keeps the stored verdicts
    # stable. The key covers the obligations AND the internal register, so
    # adding a requirement still re-opens the verdicts it could change.
    _reg_row = repo.get_regulation_by_id(regulation_id) or {}
    _extra_meta = _reg_row.get("extra_meta")
    _corpus_hash = analysis_cache.corpus_fingerprint(
        existing_requirements, existing_controls, existing_kpis)
    _stored = repo.get_requirement_mappings_by_regulation(regulation_id) or []
    _should, _match_hash, _why = analysis_cache.decide_matching(
        extra_meta=_extra_meta,
        obligation_texts=[r["requirement_text"] for r in extracted_requirements],
        corpus_hash=_corpus_hash,
        model=getattr(requirement_matcher, "model", ""),
        has_existing_rows=bool(_stored),
        force=force,
    )
    if not _should:
        if not analysis_cache.as_dict(_extra_meta).get(analysis_cache.MATCH_HASH_KEY):
            analysis_cache.record_matching(repo, regulation_id, _extra_meta,
                                           _match_hash,
                                           getattr(requirement_matcher, "model", ""))
        logger.info(f"Requirement matching SKIPPED for {regulation_id}: {_why}")
        return {
            "success": True,
            "skipped": True,
            "reason": _why,
            "regulation_id": regulation_id,
            "existing_mappings": len(_stored),
            "low_confidence": sum(1 for m in _stored
                                  if (m.get("match_confidence") or "high") == "low"),
        }
    logger.info(f"Requirement matching RUNNING for {regulation_id}: {_why}")

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
    # After the writes, never before: a hash recorded against matching that
    # failed to persist would suppress the retry.
    analysis_cache.record_matching(repo, regulation_id, _extra_meta, _match_hash,
                                   getattr(requirement_matcher, "model", ""))

    _low = [m for m in requirement_mappings
            if (m.get("match_confidence") or "high") == "low"]

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
                "match_confidence":           m.get("match_confidence", "high"),
                "match_explanation":          m.get("match_explanation"),
            }
            for m in requirement_mappings
        ],
        # The verdicts a person should look at. Surfaced at the top level rather
        # than buried per-row: a borderline call is only useful if somebody sees
        # it, and nobody scans 39 rows looking for a field.
        "needs_review": [
            {
                "extracted_requirement_text": m["extracted_requirement_text"],
                "match_status":               m["match_status"],
                "matched_requirement_id":     m.get("matched_requirement_id"),
                "match_explanation":          m.get("match_explanation"),
            }
            for m in _low
        ],
        "summary": {
            "requirements": {
                "total":             len(requirement_mappings),
                "fully_matched":     sum(1 for m in requirement_mappings if m["match_status"] == "fully_matched"),
                "partially_matched": sum(1 for m in requirement_mappings if m["match_status"] == "partially_matched"),
                "new":               sum(1 for m in requirement_mappings if m["match_status"] == "new"),
                "low_confidence":    len(_low),
            },
            "controls": {"new_links_added": len(control_links), "new_controls_created": len(new_controls_to_insert)},
            "kpis":     {"new_links_added": len(kpi_links),     "new_kpis_created":     len(new_kpis_to_insert)},
        },
    }

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
            "delete_and_rerun":   f"DELETE /admin/analysis/{regulation_id} -> POST /trigger/full-analysis/{regulation_id}?force=true",
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
    # The content-hash decision needs clean_text, which is computed further
    # down, so the real skip happens after normalization. This early return only
    # covers the case where text extraction itself fails -- previously the
    # endpoint never reached extraction when an analysis already existed, and
    # that must keep working.
    existing_rows = [] if force else repo.get_compliance_analysis(regulation_id)

    def _skip_response(reason: str, rows):
        return {
            "success": True,
            "regulation_id": regulation_id,
            "skipped": True,
            "reason": reason,
            "existing_count": len(rows),
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
                logger.info(f"Using content_text from regulation_versions ({len(text_content)} chars)")
            elif len(content_html) >= 200:
                text_content = content_html
                content_type = "html"
                logger.info(f"Using content_html from regulation_versions ({len(text_content)} chars)")

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

    # ── LAST RESORT: the document IS the file at document_url ──────────
    #
    # Every source above reads text the library already holds. A PDF-only
    # regulator holds none: its `document_url` IS the regulation, `document_html`
    # is empty by design, and nothing ever wrote `content_text`. So the four
    # checks above all miss and the endpoint refused with "No extractable text"
    # — measured 2026-08-16 on the Anti-Money Laundering Law (id=3).
    #
    # That is not a property of one document. It blocks every PDF-only source in
    # the library: AML 11, MOH 83, SDAIA 29, ZATCA Agreements 98, Tadawul 19,
    # MISA's 65 PDFs — 300+ regulations that could not be analysed at all.
    #
    # The orchestrator already downloads and extracts exactly these files
    # (`_download_and_extract_pdf`, with OCR when a PDF has no text layer), so
    # this reuses that path rather than adding a second extractor. It is LAST on
    # purpose: fetching is the expensive option, and any stored text is both
    # cheaper and reproducible.
    if not text_content:
        doc_url = (regulation.get("document_url") or "").strip()
        if doc_url.startswith("http"):
            try:
                from orchestrator.orchestrator import Orchestrator
                from processor.downloader import Downloader
                fetched = Orchestrator(
                    crawler=None, repo=repo, downloader=Downloader()
                )._download_and_extract_pdf(doc_url, regulation_id)
                if fetched and len(fetched) > 200:
                    text_content = fetched
                    content_type = "pdf_text"
                    logger.info("fetched %d chars from document_url for %s",
                                len(fetched), regulation_id)
            except Exception as e:                      # noqa: BLE001 - reported below
                logger.warning("could not fetch %s for regulation %s: %s",
                               doc_url[:90], regulation_id, e)

    if not text_content:
        raise HTTPException(
            422,
            f"No extractable text for regulation {regulation_id}. "
            f"Checked: regulation_versions.content_text/content_html, "
            f"extra_meta.org_pdf_text, document_html, and a download of "
            f"document_url. A regulation with no stored text and no reachable "
            f"file cannot be analysed.")

    normalizer = LLMAnalyzer()
    try:
        clean_text = normalizer.normalize_input_text(text_content, content_type=content_type)
    except Exception as e:
        raise HTTPException(422, f"Text normalization failed: {e}")

    if len(clean_text) < 200:
        raise HTTPException(422, f"Text too short ({len(clean_text)} chars).")

    raw_date = regulation.get("published_date")
    published_date = str(raw_date)[:10] if raw_date else ""

    # ── Content-hash cache ────────────────────────────────────────────
    # Re-analysing identical text produces a DIFFERENT answer (measured: 27/38/44
    # obligations across three runs of one document -- docs/determinism.md), so
    # not re-running is the only way to keep a stored analysis stable. This also
    # makes the skip text-aware: a document that HAS changed now re-analyses
    # automatically, instead of keeping a stale result indefinitely.
    should_run, input_hash, cache_reason = analysis_cache.decide(
        extra_meta=extra_meta,
        clean_text=clean_text,
        model=staged_analyzer.model,
        has_existing_rows=bool(existing_rows),
        force=force,
    )
    if not should_run:
        if not analysis_cache.stored_hash(extra_meta):
            # First sighting of a pre-cache analysis: record the hash now so the
            # next call can make a real comparison.
            analysis_cache.record(repo, regulation_id, extra_meta,
                                  input_hash, staged_analyzer.model)
        logger.info(f"Staged analysis SKIPPED for {regulation_id}: {cache_reason}")
        return _skip_response(cache_reason, existing_rows)

    logger.info(f"Staged analysis RUNNING for {regulation_id}: {cache_reason}")

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

    # Anchor to whatever version_id is active for this regulation right now.
    # Every regulator gets regulation_versions rows since the orchestrator
    # merge (2026-08-16 handoff), not just CBB, so this used to leave
    # version_id NULL for everyone else even though an active version
    # existed — archive_current_analysis(regulation_id, version_id) then had
    # nothing to stamp. Falls back to None only when no version row exists
    # yet (e.g. a regulator that hasn't been through direct-write).
    version_data = repo.get_active_regulation_version(regulation_id)
    version_id = version_data.get("version_id") if version_data else None

    repo.store_analysis(rows, version_id=version_id)
    # Only after a successful store -- a hash recorded against an analysis that
    # failed to persist would suppress the retry.
    analysis_cache.record(repo, regulation_id, extra_meta, input_hash,
                          staged_analyzer.model)
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
            raise HTTPException(200, f"Regulation {regulation_id} not found")
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
            200,
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
            raise HTTPException(200, f"No analysis for regulation {regulation_id}.")
        raise HTTPException(200, "Analysis exists but executive summary is empty.")
 
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
    Get content version history for a regulation.

    Returns all versions from regulation_versions table with their content snapshots.
    Every regulator is versioned this way now -- dynamic_crawler/formfill/orch.py's
    _process_versioned_doc() takes every document through the versioned path
    regardless of regulator (see its own comment: "No `if regulator == CBB`...
    Everything takes the versioned path."). A regulation with no rows here simply
    predates that change, or was never re-crawled since -- not "this regulator
    doesn't support versioning."

    Use ?include_details=true to get full regulation data (document_html, extra_meta, etc.)
    for each version, similar to the /regulation/{id} endpoint.
    """
    regulation = repo.get_regulation_by_id(regulation_id)
    if not regulation:
        raise HTTPException(200, f"Regulation {regulation_id} not found")

    regulator = regulation.get("regulator")

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
                    "ref_key": regulation.get("ref_key"),
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
        "ref_key": regulation.get("ref_key"),
        "title": regulation.get("title"),
        "regulator": regulator,
        "include_details": include_details,
        "total_versions": len(versions),
        "versions": versions,
        "note": None if versions else
            "No versions recorded yet -- this regulation predates content "
            "versioning, or hasn't been re-crawled since.",
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
        raise HTTPException(200, f"Regulation {regulation_id} not found")

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
                "note": "version_id reflects the regulation_versions row active when the analysis ran; null only if no version existed yet."
            }
        else:
            current_summary = {
                "version_id": version_id,
                "status": "active",
                "requirement_count": len(current_rows),
                "created_at": serialize_datetime(current_rows[0].get("created_at")),
                "label": "Current (active — shown in all analysis endpoints)",
                "note": "version_id reflects the regulation_versions row active when the analysis ran; null only if no version existed yet."
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
        "ref_key": regulation.get("ref_key"),
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
    4. 4-stage LLM analysis -> stored in compliance_analysis (version_id=None for uploads)
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
#  STANDALONE ANALYSIS — a document in, the analysis out, nothing     #
#  written anywhere                                                    #
# ================================================================== #
#
# WHY THIS IS SEPARATE FROM /upload-regulation
#
#   /upload-regulation is an INGESTION endpoint: it extracts, then inserts a
#   regulations row, stores the analysis, runs matching against the internal
#   register and creates suggested requirements. Every one of those is a write.
#   Pointing it at a document you only wanted to look at leaves a regulation in
#   the library, an analysis attached to it, and AUTO- requirements in
#   COMPLIANCE_REQUIREMENT that somebody then has to unpick.
#
#   This endpoint runs the SAME extraction and the SAME four-stage analyzer and
#   returns the result. It opens no database connection and writes nothing --
#   not the regulation, not the analysis, not a requirement, not a version row.
#   Nothing here can change the library.
#
#   That makes it the right tool for: checking what the analyzer makes of a
#   document before committing it, comparing two runs of the same PDF, testing a
#   prompt change, and analysing a document that is not a regulation at all.
#
#   It is deliberately NOT cached. The analysis cache keys on a stored
#   regulation's extra_meta, and there is no stored regulation here. Two calls
#   with the same PDF will therefore give slightly different answers -- that is
#   the documented behaviour of long generations (docs/determinism.md), and on
#   this endpoint it is a feature: it is how you measure the variance.

@app.post("/analyze/document", tags=["Standalone Analysis"])
async def analyze_document_standalone(
    file: UploadFile = File(...),
    title: Optional[str] = Form(None),
    regulator: str = Form(""),
    reference: str = Form(""),
    publication_date: str = Form(""),
    include_text: bool = Form(False),
):
    """Extract and analyse a PDF/DOCX. Writes NOTHING to the database.

    Same text extraction (OCR for scanned PDFs) and same four-stage analyzer as
    the ingestion path. The response carries the obligations, controls and
    per-stage JSON; pass `include_text=true` to get the extracted text back too.

    `title`, `regulator`, `reference` and `publication_date` are prompt context
    only -- the analyzer reads them, nothing is stored. Title falls back to the
    filename.
    """
    filename = file.filename or "upload"
    suffix = os.path.splitext(filename.lower())[-1]
    if suffix not in (".pdf", ".docx", ".doc"):
        raise HTTPException(400, "Only PDF and DOCX/DOC files are supported.")

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(await file.read())
            tmp_path = tmp.name
        text, document_html = extract_document_content(tmp_path, suffix)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[analyze/document] extraction failed for {filename}: {e}")
        raise HTTPException(422, f"Text extraction failed: {e}")
    finally:
        # Always, including on the error paths above -- an uploaded document is
        # somebody's regulatory PDF and must not be left in the temp directory.
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception as e:
                logger.warning(f"[analyze/document] could not remove {tmp_path}: {e}")

    if not text or len(text) < 100:
        raise HTTPException(
            422,
            f"Could not extract sufficient text from {filename} "
            f"(got {len(text or '')} chars, need at least 100). A scanned PDF "
            f"with no OCR-able text will land here.")

    normalizer = LLMAnalyzer()
    try:
        clean_text = normalizer.normalize_input_text(text, content_type="pdf_text")
    except Exception as e:
        # Normalization is a cleanup, not a gate. Analysing the raw text is far
        # better than refusing the request.
        logger.warning(f"[analyze/document] normalization failed, using raw text: {e}")
        clean_text = text

    # regulation_id=0: the analyzer only logs it and stamps it into each row, and
    # there is no regulation to name. It is visible in the response as 0 so
    # nobody mistakes an output row for something that was stored.
    rows = staged_analyzer.analyze(
        text=clean_text,
        regulation_id=0,
        document_title=(title or os.path.splitext(filename)[0]).strip(),
        regulator=regulator.strip(),
        reference=reference.strip(),
        publication_date=publication_date.strip(),
    )
    if not rows:
        raise HTTPException(
            422,
            f"The analyzer extracted 0 obligations from {filename}. The text was "
            f"{len(clean_text):,} chars, so it was read -- this usually means the "
            f"document states no obligations (a form, a notice, a cover page).")

    obligations, controls = [], []
    exec_counts, crit_counts = {}, {}
    for r in rows:
        exec_counts[r.get("execution_category") or "Unknown"] = \
            exec_counts.get(r.get("execution_category") or "Unknown", 0) + 1
        crit_counts[r.get("criticality") or "Unknown"] = \
            crit_counts.get(r.get("criticality") or "Unknown", 0) + 1
        try:
            parsed = json.loads(r.get("analysis_json") or "{}")
        except Exception:
            parsed = {}
        for ob in parsed.get("obligations", []) or []:
            obligations.append({
                "requirement_title": r.get("requirement_title", ""),
                "criticality": r.get("criticality"),
                "execution_category": r.get("execution_category"),
                **({k: ob.get(k) for k in
                    ("obligation_id", "obligation_text", "obligation_type",
                     "responsible_party", "frequency", "deadline")
                    if ob.get(k) is not None}),
            })
        controls.extend(parsed.get("controls", []) or [])

    response = {
        "success": True,
        "stored": False,          # stated explicitly: nothing was written
        "document": {
            "filename": filename,
            "file_type": suffix.lstrip("."),
            "title_used": (title or os.path.splitext(filename)[0]).strip(),
            "extracted_chars": len(text),
            "analysed_chars": len(clean_text),
            "has_html": bool(document_html),
        },
        "summary": {
            "requirements": len(rows),
            "obligations": len(obligations),
            "controls": len(controls),
            "by_criticality": crit_counts,
            "by_execution_category": exec_counts,
        },
        "obligations": obligations,
        "controls": controls,
        "stages": [
            {
                "requirement_id": r.get("requirement_id"),
                "requirement_title": r.get("requirement_title"),
                "stage1_json": r.get("stage1_json"),
                "stage2_json": r.get("stage2_json"),
                "stage3_json": r.get("stage3_json"),
                "stage4_md": r.get("stage4_md"),
            }
            for r in rows
        ],
        "note": ("Nothing was written to the database. To ingest this document "
                 "into the library use POST /upload-regulation instead."),
    }
    if include_text:
        response["extracted_text"] = text
    return response


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
            "version_id anchors to whatever regulation_versions row was active when the "
            "analysis ran, for every regulator; NULL only if no version existed yet."
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
                r.ref_key,
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
        raise HTTPException(200, f"Regulation {regulation_id} not found")

    regulator = regulation.get("regulator")

    # Versioning applies to every regulator now -- dynamic_crawler/formfill/
    # orch.py's _process_versioned_doc() takes every document through the
    # versioned path regardless of regulator. A regulation with no active
    # version simply predates that change or hasn't been re-crawled since;
    # it is not specific to any one regulator, so there is nothing to gate
    # on `regulator` here any more.
    version_data = repo.get_active_regulation_version(regulation_id)
    if not version_data:
        return {
            "success":        True,
            "lang":           lang,
            "regulation_id":  regulation_id,
            "ref_key":        regulation.get("ref_key"),
            "regulator":      regulator,
            "has_versioning": False,
            "active_version": None,
            "note": "No active version recorded yet -- this regulation predates "
                   "content versioning, or hasn't been re-crawled since.",
        }

    change_summary = version_data.get("change_summary") or ""
    if lang == "ar" and change_summary:
        translated = translate_texts_batch([change_summary], lang)
        change_summary = translated[0] if translated else change_summary

    return {
        "success":        True,
        "lang":           lang,
        "regulation_id":  regulation_id,
        "ref_key":        regulation.get("ref_key"),
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
 

@app.post("/admin/fetch-and-analyze/{regulation_id}", tags=["Admin"])
def fetch_and_analyze(regulation_id: int):
    import httpx, tempfile

    regulation = repo.get_regulation_by_id(regulation_id)
    if not regulation:
        raise HTTPException(404, f"Regulation {regulation_id} not found")

    extra_meta = regulation.get("extra_meta") or {}
    if isinstance(extra_meta, str):
        try:
            extra_meta = json.loads(extra_meta)
        except Exception:
            extra_meta = {}

    # Get English PDF URL
    doc_url = regulation.get("document_url")
    download_links = extra_meta.get("download_links", [])
    for link in download_links:
        if link.get("language") == "english" and link.get("type") == "pdf":
            doc_url = link.get("url")
            break

    if not doc_url:
        raise HTTPException(422, "No document_url found")

    # Download PDF with browser-like headers to avoid 403
    tmp_path = None
    try:
        logger.info(f"Downloading PDF from {doc_url}")
        resp = httpx.get(
            doc_url,
            timeout=60,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/pdf,application/octet-stream,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Referer": "https://cbben.thomsonreuters.com/",
                "Connection": "keep-alive",
            }
        )
        if resp.status_code != 200:
            raise HTTPException(422, f"PDF download failed: HTTP {resp.status_code}")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(resp.content)
            tmp_path = tmp.name

        text, _ = OCRProcessor.extract_text_from_pdf_smart(tmp_path)
        if not text or len(text) < 200:
            raise HTTPException(422, f"Extracted text too short: {len(text or '')} chars")

        logger.info(f"Extracted {len(text)} chars from PDF")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    # Store text in extra_meta
    extra_meta["org_pdf_text"] = text
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE regulations SET extra_meta = ?, updated_at = GETUTCDATE() WHERE id = ?",
            [json.dumps(extra_meta, ensure_ascii=False), regulation_id]
        )
        conn.commit()

    # Now run full analysis
    analysis_result = trigger_staged_analysis(regulation_id, force=True)
    matching_result = {}
    try:
        matching_result = trigger_requirement_matching_v2(regulation_id)
    except Exception as e:
        matching_result = {"error": str(e)}

    return {
        "success": True,
        "regulation_id": regulation_id,
        "pdf_url": doc_url,
        "text_extracted_chars": len(text),
        "analysis": analysis_result,
        "matching": matching_result,
    }

# ================================================================== #
#  DEMO-ONLY, FULLY STANDALONE ANALYSIS ENDPOINTS                      #
#                                                                       #
#  Zero dependency on MSSQLRepository — it is unsafe to call here      #
#  because several of its methods reference columns that don't exist   #
#  on the current live schema:                                        #
#    - compliance_analysis.is_current, .version_id                     #
#    - sama_requirement_mapping.version_id, .obligation_id,            #
#      .requirement_id                                                 #
#    - regulations.content_hash (used by get_regulation_by_id)         #
#                                                                       #
#  This file opens its own pyodbc connection and writes/reads only     #
#  columns confirmed to exist right now. Reuses staged_analyzer,       #
#  requirement_matcher, LLMAnalyzer since those are pure processing    #
#  classes with no DB calls of their own.                              #
#                                                                       #
#  Paste into pipeline_api.py anywhere after `staged_analyzer`,        #
#  `requirement_matcher` are instantiated.                             #
# ================================================================== #

import pyodbc as _demo_pyodbc


def _demo_get_conn():
    """The SAME connection the rest of the api uses.

    This used to build its own string and interpolate UID/PWD unconditionally:

        UID={os.getenv('MSSQL_USERNAME')};PWD={os.getenv('MSSQL_PASSWORD')};

    With a Windows-authenticated setup — no MSSQL_USERNAME in .env, which is how
    this machine is configured — that produces the literal "UID=None;PWD=None;"
    and the driver answers

        [28000] Login failed for user 'None'. (18456)

    which reads like a missing environment variable rather than the wrong
    AUTHENTICATION MODE. storage/mssql_repo.py::_get_conn already carries that
    fix and the comment explaining it; this was a second copy that never got it.

    Delegating means there is one place that knows how to reach the database,
    so the next auth change cannot fix one caller and miss the other. It also
    inherits the repo's connect retries, which this never had.
    """
    return repo._get_conn()


def _demo_get_regulation(regulation_id: int) -> Optional[dict]:
    """Raw fetch avoiding content_hash/title_hash (not confirmed to exist)."""
    query = """
        SELECT id, regulator, source_system, category, title,
               document_url, published_date, reference_no,
               department, year, source_page_url,
               CAST(extra_meta AS NVARCHAR(MAX)) AS extra_meta,
               document_html
        FROM regulations
        WHERE id = ?
    """
    with _demo_get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, [regulation_id])
        row = cursor.fetchone()
        if not row:
            return None
        cols = [c[0] for c in cursor.description]
        result = dict(zip(cols, row))
        if result.get("extra_meta"):
            try:
                result["extra_meta"] = json.loads(result["extra_meta"])
            except Exception:
                result["extra_meta"] = {}
        return result


@app.post("/demo/trigger/staged-analysis/{regulation_id}", tags=["Demo (standalone)"])
def demo_trigger_staged_analysis(regulation_id: int, force: bool = Query(False)):
    if not force:
        with _demo_get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM compliance_analysis WHERE regulation_id = ?",
                [regulation_id],
            )
            existing_count = cursor.fetchone()[0]
        if existing_count > 0:
            return {
                "success": True,
                "regulation_id": regulation_id,
                "skipped": True,
                "reason": "Analysis already exists. Use ?force=true to re-run.",
                "existing_count": existing_count,
                "next_step": f"POST /demo/trigger/requirement-matching/{regulation_id}",
            }

    regulation = _demo_get_regulation(regulation_id)
    if not regulation:
        raise HTTPException(404, f"Regulation {regulation_id} not found")

    extra_meta = regulation.get("extra_meta") or {}

    text_content = None
    content_type = None

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

    if force:
        with _demo_get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM compliance_analysis WHERE regulation_id = ?", [regulation_id])
            conn.commit()

    insert_sql = """
    INSERT INTO compliance_analysis (
        regulation_id, analysis_json, requirement_id, requirement_title,
        execution_category, criticality, obligation_type,
        stage1_json, stage2_json, stage3_json, stage4_md,
        schema_version, status, created_at, updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE())
"""
    with _demo_get_conn() as conn:
        cursor = conn.cursor()
        for r in rows:
            cursor.execute(
    insert_sql,
    [
        regulation_id,
        json.dumps(r),  # NEW — satisfies analysis_json NOT NULL
        r.get("requirement_id"),
        r.get("requirement_title"),
        r.get("execution_category"),
        r.get("criticality"),
        r.get("obligation_type"),
        json.dumps(r.get("stage1_json")) if r.get("stage1_json") is not None else None,
        json.dumps(r.get("stage2_json")) if r.get("stage2_json") is not None else None,
        json.dumps(r.get("stage3_json")) if r.get("stage3_json") is not None else None,
        r.get("stage4_md"),
        "demo",
        "active",
    ],
)
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
        "document_title": regulation.get("title"),
        "text_length": len(clean_text),
        "content_type": content_type,
        "mode": "demo-standalone",
        "analysis": {
            "requirements_extracted": len(rows),
            "by_execution_category": exec_counts,
            "by_criticality": crit_counts,
        },
        "next_step": f"POST /demo/trigger/requirement-matching/{regulation_id}",
    }


@app.post("/demo/trigger/requirement-matching/{regulation_id}", tags=["Demo (standalone)"])
def demo_trigger_requirement_matching(regulation_id: int):
    with _demo_get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, regulation_id, requirement_id, requirement_title,
                   stage2_json
            FROM compliance_analysis
            WHERE regulation_id = ?
            """,
            [regulation_id],
        )
        cols = [c[0] for c in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

    if not rows:
        raise HTTPException(
            404,
            f"No analysis for regulation {regulation_id}. "
            f"Run POST /demo/trigger/staged-analysis/{regulation_id} first.",
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
            })

    if not extracted_requirements:
        raise HTTPException(404, f"No obligations found for regulation {regulation_id}")

    with _demo_get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COMPLIANCEREQUIREMENT_ID, TITLE, DESCRIPTION FROM COMPLIANCE_REQUIREMENT "
            "WHERE TITLE IS NOT NULL AND DESCRIPTION IS NOT NULL"
        )
        existing_requirements = [
            {"id": r[0], "title": r[1], "description": r[2]} for r in cursor.fetchall()
        ]

        cursor.execute(
            "SELECT CONTROL_ID, TITLE, DESCRIPTION, CONTROL_KEY FROM DEMO_CONTROL WHERE TITLE IS NOT NULL"
        )
        existing_controls = [
            {"id": r[0], "title": r[1], "description": r[2], "control_key": r[3]}
            for r in cursor.fetchall()
        ]

        cursor.execute(
            "SELECT KISETUP_ID, TITLE, DESCRIPTION, KISETUP_KEY FROM DEMO_KPI WHERE TITLE IS NOT NULL"
        )
        existing_kpis = [
            {"id": r[0], "title": r[1], "description": r[2], "kisetup_key": r[3]}
            for r in cursor.fetchall()
        ]

        cursor.execute("SELECT COMPLIANCEREQUIREMENT_ID, CONTROL_ID FROM DEMO_REQUIREMENT_CONTROL_LINK")
        linked_controls_by_req = {}
        for req_id, ctrl_id in cursor.fetchall():
            linked_controls_by_req.setdefault(req_id, []).append(ctrl_id)

        cursor.execute("SELECT COMPLIANCEREQUIREMENT_ID, KISETUP_ID FROM DEMO_REQUIREMENT_KPI_LINK")
        linked_kpis_by_req = {}
        for req_id, kpi_id in cursor.fetchall():
            linked_kpis_by_req.setdefault(req_id, []).append(kpi_id)

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

    # ── Store mappings (only columns that exist on sama_requirement_mapping) ──
    if requirement_mappings:
        insert_sql = """
            INSERT INTO sama_requirement_mapping (
                regulation_id, extracted_requirement_text,
                matched_requirement_id, match_status, match_explanation, created_at
            )
            VALUES (?, ?, ?, ?, ?, GETDATE())
        """
        with _demo_get_conn() as conn:
            cursor = conn.cursor()
            for m in requirement_mappings:
                cursor.execute(
                    insert_sql,
                    [
                        regulation_id,
                        m["extracted_requirement_text"],
                        m.get("matched_requirement_id"),
                        m["match_status"],
                        m.get("match_explanation"),
                    ],
                )
            conn.commit()

    partially_matched_ids = [
        m["matched_requirement_id"]
        for m in requirement_mappings
        if m["match_status"] == "partially_matched" and m.get("matched_requirement_id")
    ]
    if partially_matched_ids:
        with _demo_get_conn() as conn:
            cursor = conn.cursor()
            placeholders = ",".join("?" for _ in partially_matched_ids)
            cursor.execute(
                f"UPDATE COMPLIANCE_REQUIREMENT SET IS_SUGGESTED = 1 "
                f"WHERE COMPLIANCEREQUIREMENT_ID IN ({placeholders})",
                partially_matched_ids,
            )
            conn.commit()

    new_req_mappings = [m for m in requirement_mappings if m["match_status"] == "new"]
    for i, mapping in enumerate(new_req_mappings):
        try:
            req_text = mapping["extracted_requirement_text"]
            title = req_text[:100].strip() + ("..." if len(req_text) > 100 else "")
            with _demo_get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO COMPLIANCE_REQUIREMENT (TITLE, DESCRIPTION, REF_KEY, REF_NO, IS_SUGGESTED, CREATEDON)
                    OUTPUT INSERTED.COMPLIANCEREQUIREMENT_ID
                    VALUES (?, ?, ?, ?, 1, GETDATE())
                    """,
                    [title, req_text, f"DEMO-AUTO-{regulation_id}-{i}", f"REG-{regulation_id}"],
                )
                new_req_id = cursor.fetchone()[0]
                conn.commit()
            for ctrl in new_controls_to_insert:
                if ctrl.get("_req_id") is None:
                    ctrl["_req_id"] = new_req_id
            for kpi in new_kpis_to_insert:
                if kpi.get("_req_id") is None:
                    kpi["_req_id"] = new_req_id
        except Exception as e:
            logger.error(f"[demo-matching] Failed to insert new suggested requirement: {e}")

    if control_links:
        with _demo_get_conn() as conn:
            cursor = conn.cursor()
            for link in control_links:
                cursor.execute(
                    """
                    INSERT INTO DEMO_REQUIREMENT_CONTROL_LINK (
                        COMPLIANCEREQUIREMENT_ID, CONTROL_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        link["compliancerequirement_id"], link["control_id"],
                        link["match_status"], link.get("match_explanation"), link.get("regulation_id"),
                    ],
                )
            conn.commit()

    if kpi_links:
        with _demo_get_conn() as conn:
            cursor = conn.cursor()
            for link in kpi_links:
                cursor.execute(
                    """
                    INSERT INTO DEMO_REQUIREMENT_KPI_LINK (
                        COMPLIANCEREQUIREMENT_ID, KISETUP_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        link["compliancerequirement_id"], link["kisetup_id"],
                        link["match_status"], link.get("match_explanation"), link.get("regulation_id"),
                    ],
                )
            conn.commit()

    for ctrl in new_controls_to_insert:
        try:
            with _demo_get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO DEMO_CONTROL (TITLE, DESCRIPTION, CONTROL_KEY, IS_SUGGESTED, CREATEDON)
                    OUTPUT INSERTED.CONTROL_ID
                    VALUES (?, ?, ?, 1, GETDATE())
                    """,
                    [
                        ctrl["title"][:500], ctrl["description"],
                        ctrl.get("control_key", f"DEMO-AUTO-CTRL-{ctrl['title'][:20]}"),
                    ],
                )
                new_ctrl_id = cursor.fetchone()[0]
                conn.commit()
            req_id = ctrl.get("_req_id")
            if req_id:
                with _demo_get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        INSERT INTO DEMO_REQUIREMENT_CONTROL_LINK (
                            COMPLIANCEREQUIREMENT_ID, CONTROL_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID
                        ) VALUES (?, ?, 'new', ?, ?)
                        """,
                        [req_id, new_ctrl_id, ctrl.get("_explanation", ""), regulation_id],
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"[demo-matching] Failed to insert new suggested control: {e}")

    for kpi in new_kpis_to_insert:
        try:
            with _demo_get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO DEMO_KPI (TITLE, DESCRIPTION, KISETUP_KEY, FORMULA, IS_SUGGESTED, CREATEDON)
                    OUTPUT INSERTED.KISETUP_ID
                    VALUES (?, ?, ?, ?, 1, GETDATE())
                    """,
                    [
                        kpi["title"][:500], kpi["description"],
                        kpi.get("kisetup_key", f"DEMO-AUTO-KPI-{kpi['title'][:20]}"),
                        kpi.get("formula", ""),
                    ],
                )
                new_kpi_id = cursor.fetchone()[0]
                conn.commit()
            req_id = kpi.get("_req_id")
            if req_id:
                with _demo_get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        INSERT INTO DEMO_REQUIREMENT_KPI_LINK (
                            COMPLIANCEREQUIREMENT_ID, KISETUP_ID, MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID
                        ) VALUES (?, ?, 'new', ?, ?)
                        """,
                        [req_id, new_kpi_id, kpi.get("_explanation", ""), regulation_id],
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"[demo-matching] Failed to insert new suggested KPI: {e}")

    return {
        "success":               True,
        "regulation_id":         regulation_id,
        "mode":                  "demo-standalone",
        "obligations_processed": len(extracted_requirements),
        "mappings": [
            {
                "extracted_requirement_text": m["extracted_requirement_text"],
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