from fastapi import FastAPI, BackgroundTasks, HTTPException, Query, UploadFile, File, Form
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

from scheduler.scheduler import run_sbp_pipeline, run_secp_pipeline, run_sama_pipeline
from storage.mssql_repo import MSSQLRepository
from processor.gap_analyzer import GapAnalyzer
from processor.Text_Extractor import OCRProcessor
import docx as python_docx
from fastapi.responses import JSONResponse, Response
from processor.staged_LLM_Analyzer import StagedLLMAnalyzer
from processor.requirement_matcher import RequirementMatcher
from orchestrator.orchestrator import Orchestrator

from utils.lang_translator import (
    translate_regulation,
    translate_gap_result,
    translate_compliance_requirement,
    translate_texts_batch,
    translate_v2_gap_result,
)

logger = logging.getLogger(__name__)
app = FastAPI(title="Regulatory Pipeline API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= DB SETUP =================
repo = MSSQLRepository({
    "server": os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "username": os.getenv("MSSQL_USERNAME"),
    "password": os.getenv("MSSQL_PASSWORD"),
    "driver": os.getenv("MSSQL_DRIVER")
})

gap_analyzer = GapAnalyzer()

REGULATOR_PIPELINES = {
    "SBP": run_sbp_pipeline,
    "SECP": run_secp_pipeline,
    "SAMA": run_sama_pipeline
}

pipeline_lock = Lock()

SUPPORTED_LANGUAGES = {"en", "ar"}

def _validate_lang(lang: str) -> str:
    lang = (lang or "en").lower().strip()
    if lang not in SUPPORTED_LANGUAGES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported language '{lang}'. Supported: en, ar"
        )
    return lang


# ================= AR CACHE HELPERS =================

def _get_ar_cache(cache_key: str):
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT response_json FROM ar_response_cache WHERE cache_key = ?",
                cache_key
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
            cursor.execute("""
                MERGE ar_response_cache AS target
                USING (SELECT ? AS cache_key) AS src
                ON target.cache_key = src.cache_key
                WHEN MATCHED THEN
                    UPDATE SET response_json = ?, updated_at = GETUTCDATE()
                WHEN NOT MATCHED THEN
                    INSERT (cache_key, response_json) VALUES (?, ?);
            """, cache_key, serialized, cache_key, serialized)
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
    ]
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            for key in keys:
                cursor.execute("DELETE FROM ar_response_cache WHERE cache_key = ?", key)
            conn.commit()
    except Exception as e:
        logger.warning(f"AR cache invalidation failed for regulation {regulation_id}: {e}")

# ====================================================
# ================= CONTROL COMPARISON HELPER =================

def _build_stage3_control_entry(control_detail: dict, comparison_result: str = "ai_designed") -> dict:
    return {
        "control_id":              None,
        "control_title":           control_detail.get("control_title"),
        "control_description":     control_detail.get("control_description"),
        "control_objective":       control_detail.get("control_objective"),
        "control_owner":           control_detail.get("control_owner"),
        "control_type":            control_detail.get("control_type"),
        "execution_type":          control_detail.get("execution_type"),
        "frequency":               control_detail.get("frequency"),
        "control_level":           control_detail.get("control_level"),
        "evidence_generated":      control_detail.get("evidence_generated"),
        "key_steps":               control_detail.get("key_steps", []),
        "residual_risk_if_failed": control_detail.get("residual_risk_if_failed"),
        "match_status":            "ai_designed",
        "is_suggested":            True,
        "source":                  "stage3_llm",
        "comparison_result":       comparison_result,
    }

def update_heartbeat(regulator: str):
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE pipeline_status
            SET last_heartbeat = GETUTCDATE()
            WHERE regulator=? AND status='RUNNING'
        """, regulator)
        conn.commit()


# ================= HELPER FUNCTIONS =================
def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def row_to_dict(row, columns):
    result = {}
    for col, value in zip(columns, row):
        result[col] = serialize_datetime(value)
    return result


def run_pipeline_async(regulator: str):
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_status (regulator, status, started_at, last_heartbeat)
            VALUES (?, 'RUNNING', GETUTCDATE(), GETUTCDATE())
        """, regulator)
        conn.commit()

    stop_heartbeat = False

    def heartbeat_loop():
        while not stop_heartbeat:
            update_heartbeat(regulator)
            time.sleep(300)

    heartbeat_thread = Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    try:
        REGULATOR_PIPELINES[regulator]()
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE pipeline_status
                SET status='DONE', finished_at=GETUTCDATE()
                WHERE regulator=? AND status='RUNNING'
            """, regulator)
            conn.commit()
    except Exception as e:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE pipeline_status
                SET status='FAILED', finished_at=GETUTCDATE(), error=?
                WHERE regulator=? AND status='RUNNING'
            """, str(e), regulator)
            conn.commit()
    finally:
        stop_heartbeat = True


def scheduler_loop():
    logger.info("Scheduler started")
    while True:
        now = datetime.utcnow().time()

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 1 id, regulator
                FROM pipeline_schedule
                WHERE scheduled_time <= ? AND status = 'PENDING'
                ORDER BY scheduled_time
            """, now)
            job = cursor.fetchone()

        if job:
            schedule_id, regulator = job
            if pipeline_lock.acquire(blocking=False):
                try:
                    with repo._get_conn() as conn:
                        cursor = conn.cursor()
                        cursor.execute("UPDATE pipeline_schedule SET status='RUNNING' WHERE id=?", schedule_id)
                        conn.commit()

                    run_pipeline_async(regulator)

                    with repo._get_conn() as conn:
                        cursor = conn.cursor()
                        cursor.execute("UPDATE pipeline_schedule SET status='DONE', last_run_at=GETUTCDATE() WHERE id=?", schedule_id)
                        conn.commit()
                finally:
                    pipeline_lock.release()

        time.sleep(30)


def run_regulator_pipeline(regulator: str):
    try:
        logger.info(f"Starting {regulator} pipeline")
        pipeline_func = REGULATOR_PIPELINES.get(regulator)
        if pipeline_func:
            pipeline_func()
        logger.info(f"{regulator} pipeline completed")
    except Exception as e:
        logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)


# ================= PYDANTIC MODELS =================
class ScheduleUpdate(BaseModel):
    regulator: str
    hour: int
    minute: int


class RegulationResponse(BaseModel):
    success: bool
    data: List[Dict[str, Any]]
    pagination: Dict[str, Any]


class ComplianceAnalysisResponse(BaseModel):
    success: bool
    data: Dict[str, Any]


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


class SingleRegulationResponse(BaseModel):
    success: bool
    data: RegulationModel

class StatusUpdate(BaseModel):
    record_id: int
    status: str

class ComplianceStatusUpdate(BaseModel):
    regulation_id: int
    requirement_id: str
    status: str


# ================= GAP ANALYSIS MODELS =================
class GapResult(BaseModel):
    # Primary field — obligation text (the atomic regulatory unit)
    obligation_text: Optional[str] = None
    coverage_status: str
    evidence_text: Optional[str]
    gap_description: Optional[str]
    controls: Optional[str] = None
    kpis: Optional[str] = None
    # V2 enrichment fields — only populated for v2 gap analysis endpoints
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


# ================= FILE HELPERS =================
async def _save_and_extract_file(upload_file: UploadFile) -> str:
    filename = upload_file.filename.lower()
    suffix = os.path.splitext(filename)[-1] or ".pdf"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await upload_file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        if suffix == ".pdf":
            text, metadata = OCRProcessor.extract_text_from_pdf_smart(tmp_path)
            logger.info(f"Extracted {len(text)} chars from uploaded PDF")
        elif suffix in (".docx", ".doc"):
            doc = python_docx.Document(tmp_path)
            text = "\n\n".join([para.text for para in doc.paragraphs if para.text.strip()])
            logger.info(f"Extracted {len(text)} chars from uploaded DOCX")
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {suffix}. Only PDF and DOCX are supported.")
        return text
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# ================= V2 GAP HELPERS =================

# Only these fields contain human-readable text safe to translate.
# Intentionally excludes: obligation_id, requirement_id, criticality,
# obligation_type, execution_category, coverage_status (all are IDs/enums).
_V2_GAP_TRANSLATABLE_FIELDS = [
    "obligation_text",
    "evidence_text",
    "gap_description",
    "controls",
    "kpis",
    "requirement_title",
]


def _run_gap_for_regulation_v2(session_id: int, regulation_id: int, uploaded_text: str) -> RegulationGapSummary:
    """
    V2 gap analysis helper.

    1. Flattens all obligations from staged_analysis (stage2_json) into
       individual gap-check units, each keyed by obligation_text.
    2. Passes only requirement_text to GapAnalyzer (the LLM only needs text).
    3. Post-merges all v2 metadata back onto each result using
       obligation_text as the join key.
    4. Builds an enriched summary with breakdowns by criticality and
       execution_category in addition to the standard covered/partial/missing.
    """
    rows = repo.get_compliance_analysis_v2(regulation_id)
    if not rows:
        raise HTTPException(404, f"No v2 analysis for regulation {regulation_id}. "
                                 f"Run POST /trigger/staged-analysis/{regulation_id} first.")

    requirements_for_gap = []
    # Metadata lookup: obligation_text → all v2 fields for that obligation
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
            meta = {
                "obligation_id":      ob["obligation_id"],
                "requirement_id":     row["requirement_id"],
                "requirement_title":  row["requirement_title"],
                "criticality":        ob.get("criticality"),
                "obligation_type":    ob.get("obligation_type"),
                "execution_category": ob.get("execution_category"),
            }
            requirements_for_gap.append({"requirement_text": ob_text})
            ob_metadata[ob_text] = meta

    if not requirements_for_gap:
        raise HTTPException(404, f"No obligations found in v2 analysis for regulation {regulation_id}")

    logger.info(f"[v2] Running gap analysis: regulation {regulation_id}, {len(requirements_for_gap)} obligations")

    results = gap_analyzer.analyze_gaps(uploaded_text=uploaded_text, requirements=requirements_for_gap)
    repo.store_gap_results(session_id, regulation_id, results)

    # Post-merge: join v2 metadata back onto each result via obligation_text
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

    # Build enriched summary
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
        summary=summary
    )


def _enrich_results_with_controls_v2(results: List[GapResult], regulation_id: int) -> List[GapResult]:
    """
    V2 control enrichment. Pulls control titles from stage3_json.
    Matches on obligation_text (primary key) or requirement_text (alias).
    Does NOT overwrite any v2 metadata already on the result object.
    """
    rows = repo.get_compliance_analysis_v2(regulation_id)
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
        lookup_key = result.obligation_text or result.requirement_text
        ctrl_title = ob_text_to_control.get(lookup_key)
        if ctrl_title:
            result.controls = ctrl_title

    return results


def _translate_v2_gap_results(results: List[GapResult], lang: str) -> List[GapResult]:
    """
    Translate human-readable text fields AND enum/status fields on v2 GapResult objects.
    """
    # Enum/Status Value Translations - EXPANDED
    ENUM_TRANSLATIONS = {
        "coverage_status": {
            "covered": "مغطى",
            "partial": "جزئي",
            "missing": "مفقود"
        },
        "criticality": {
            "High": "عالي",
            "Medium": "متوسط",
            "Low": "منخفض"
        },
        "obligation_type": {
            "Reporting": "إبلاغ",
            "Governance": "حوكمة",
            "Preventive": "وقائي",
            "Detective": "كشف",
            "Corrective": "تصحيحي"
        },
        "execution_category": {
            "Ongoing_Control": "رقابة مستمرة",
            "One_Time_Implementation": "تنفيذ لمرة واحدة",
            "Periodic_Review": "مراجعة دورية",
            "Governance_Approval": "موافقة الحوكمة",
            "One_Off_Reporting": "إبلاغ لمرة واحدة",
            "Event_Driven": "حسب الحدث",
            "Continuous_Monitoring": "مراقبة مستمرة",
            "Annual_Review": "مراجعة سنوية"
        }
    }

    # Text fields to translate via batch API
    TEXT_FIELDS = [
        "obligation_text",
        "evidence_text",
        "gap_description",
        "controls",
        "kpis",
        "requirement_title",
    ]

    results_dicts = [r.dict() for r in results]

    # 1. Translate enum/status fields
    for r in results_dicts:
        for field, translations in ENUM_TRANSLATIONS.items():
            val = r.get(field)
            if val and field in ENUM_TRANSLATIONS:
                r[field] = ENUM_TRANSLATIONS[field].get(val, val)

    # 2. Collect and translate text fields via batch API
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

# ================= STARTUP =================
@app.on_event("startup")
def start_scheduler():
    Thread(target=scheduler_loop, daemon=True).start()


# ================= PIPELINE ENDPOINTS =================

@app.post("/schedule")
def schedule_pipeline(regulator: str, hour: int, minute: int):
    if regulator not in REGULATOR_PIPELINES:
        raise HTTPException(400, "Unknown regulator")
    scheduled_time = dtime(hour, minute)
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            MERGE pipeline_schedule AS target
            USING (SELECT ? AS regulator) AS src
            ON target.regulator = src.regulator
            WHEN MATCHED THEN UPDATE SET scheduled_time=?, status='PENDING'
            WHEN NOT MATCHED THEN INSERT (regulator, scheduled_time) VALUES (?, ?);
        """, regulator, scheduled_time, regulator, scheduled_time)
        conn.commit()
    return {"success": True, "regulator": regulator, "scheduled_time": f"{hour:02d}:{minute:02d}"}


@app.post("/update-schedule")
def update_pipeline_schedule(payload: ScheduleUpdate):
    if payload.regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")
    return {"status": "success", "regulator": payload.regulator, "hour": payload.hour, "minute": payload.minute, "message": f"{payload.regulator} schedule updated successfully"}


@app.post("/trigger/full")
def trigger_full_pipeline():
    completed_regulators = []
    errors = []
    for regulator, pipeline_func in REGULATOR_PIPELINES.items():
        try:
            logger.info(f"Starting {regulator} pipeline")
            pipeline_func()
            completed_regulators.append(regulator)
        except Exception as e:
            logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)
            errors.append({"regulator": regulator, "error": str(e)})
    return {
        "status": "done" if not errors else "partial_failure",
        "completed_regulators": completed_regulators,
        "errors": errors,
        "completed_at": datetime.utcnow().isoformat()
    }


@app.post("/trigger/{regulator}")
def trigger_regulator_pipeline(regulator: str):
    if regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")
    try:
        REGULATOR_PIPELINES[regulator]()
        return {"status": "done", "regulator": regulator, "completed_at": datetime.utcnow().isoformat()}
    except Exception as e:
        logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"{regulator} pipeline failed: {e}")


# ================= REGULATIONS ENDPOINTS =================

@app.get("/regulations/by-category/{category_id}")
def get_regulations_by_category(
    category_id: int,
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
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
                return {"success": True, "category_id": category_id, "total": 0, "data": "No regulations found for this category"}
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


from fastapi.responses import JSONResponse
import json

@app.get("/regulations/{regulator}")
def get_regulations_by_regulator(
        regulator: str,
        category_id: Optional[int] = Query(None),
        year: Optional[int] = Query(None),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = f"GET /regulations/{regulator}?category_id={category_id}&year={year}&limit={limit}&offset={offset}"
            cached = _get_ar_cache(cache_key)
            if cached:
                return Response(
                    content=json.dumps(cached, ensure_ascii=False),
                    media_type="application/json"
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
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            regulations = []
            for row in rows:
                reg_dict = row_to_dict(row, columns)
                if reg_dict.get('document_html'):
                    reg_dict['document_html'] = reg_dict['document_html'].replace('\\"', '"')
                if reg_dict.get('extra_meta'):
                    try:
                        reg_dict['extra_meta'] = json.loads(reg_dict['extra_meta'])
                        reg_dict['extra_meta'].pop('org_pdf_html', None)
                        reg_dict['extra_meta'].pop('org_pdf_text', None)
                    except:
                        pass
                reg_dict['category_info'] = {
                    'id': reg_dict.pop('compliancecategory_id', None),
                    'title': reg_dict.pop('category_title', None),
                    'parent_id': reg_dict.pop('category_parent_id', None),
                    'type': reg_dict.pop('category_type', None)
                }
                regulations.append(reg_dict)

            count_query = "SELECT COUNT(*) FROM regulations WHERE regulator = ?"
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
                "total": total_count, "limit": limit, "offset": offset,
                "has_more": (offset + limit) < total_count,
                "current_page": (offset // limit) + 1,
                "total_pages": (total_count + limit - 1) // limit
            }
        }

        if lang == "ar":
            _set_ar_cache(cache_key, response_data)

        return Response(
            content=json.dumps(response_data, ensure_ascii=False, default=str),
            media_type="application/json"
        )

    except Exception as e:
        logger.exception(f"Error fetching regulations for {regulator}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch regulations: {str(e)}")


@app.get("/regulation/{regulation_id}")
def get_regulation_detail(
    regulation_id: int,
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = f"GET /regulation/{regulation_id}"
            cached = _get_ar_cache(cache_key)
            if cached:
                return Response(
                    content=json.dumps(cached, ensure_ascii=False),
                    media_type="application/json"
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
            columns = [col[0] for col in cursor.description]
            reg_dict = row_to_dict(row, columns)
            if reg_dict.get('document_html'):
                reg_dict['document_html'] = reg_dict['document_html'].replace('\\"', '"')
            if reg_dict.get('extra_meta'):
                try:
                    reg_dict['extra_meta'] = json.loads(reg_dict['extra_meta'])
                    reg_dict['extra_meta'].pop('org_pdf_text', None)
                    reg_dict['extra_meta'].pop('org_pdf_html', None)
                except:
                    pass
            reg_dict['category_info'] = {
                'id': reg_dict.pop('compliancecategory_id', None),
                'title': reg_dict.pop('category_title', None),
                'parent_id': reg_dict.pop('category_parent_id', None),
                'type': reg_dict.pop('category_type', None)
            }

        if lang == "ar":
            reg_dict = translate_regulation(reg_dict, lang)

        response_data = {"success": True, "lang": lang, "data": reg_dict}

        if lang == "ar":
            _set_ar_cache(cache_key, response_data)

        return Response(
            content=json.dumps(response_data, ensure_ascii=False, default=str),
            media_type="application/json"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching regulation {regulation_id}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch regulation: {str(e)}")


# ================= COMPLIANCE ANALYSIS ENDPOINTS =================


def build_full_mapping_response(regulation_id: int, lang: str):
    req_mappings = repo.get_requirement_mappings_by_regulation(regulation_id)
    v2_rows      = repo.get_compliance_analysis_v2(regulation_id)

    # ── Build v2 lookup: obligation_id → full obligation + control from stage2/3 ──
    ob_id_to_detail: Dict[str, dict] = {}   # obligation_id → stage2 obligation dict
    ob_id_to_control: Dict[str, dict] = {}  # obligation_id → stage3 control dict
    req_id_to_v2_meta: Dict[str, dict] = {} # requirement_id (e.g. REQ-001) → row meta

    for row in v2_rows:
        s2 = row.get("stage2_json") or {}
        s3 = row.get("stage3_json") or {}
        if isinstance(s2, str):
            try: s2 = json.loads(s2)
            except: s2 = {}
        if isinstance(s3, str):
            try: s3 = json.loads(s3)
            except: s3 = {}

        req_id_to_v2_meta[row["requirement_id"]] = {
            "requirement_id":     row["requirement_id"],
            "requirement_title":  row["requirement_title"],
            "execution_category": row.get("execution_category"),
            "criticality":        row.get("criticality"),
            "obligation_type":    row.get("obligation_type"),
        }

        # stage3 control map: obligation_id → control dict
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

    # ── Fetch existing DB controls/KPIs linked to matched requirement IDs ──
    matched_req_ids = list({
        m["matched_requirement_id"]
        for m in req_mappings
        if m.get("matched_requirement_id")
    })

    # Controls linked to this regulation (newly stored links)
    ctrl_links_reg = repo.get_control_links_by_regulation(regulation_id)

    # Controls linked to matched requirement IDs (pre-existing links from other regulations)
    ctrl_links_req = repo.get_control_links_by_requirement_ids(matched_req_ids) if matched_req_ids else []

    # Merge both sets, dedup by (req_id, control_id)
    seen_ctrl = set()
    all_ctrl_links = []
    for c in ctrl_links_reg + ctrl_links_req:
        key = (c["COMPLIANCEREQUIREMENT_ID"], c["CONTROL_ID"])
        if key not in seen_ctrl:
            seen_ctrl.add(key)
            all_ctrl_links.append(c)


    # Index by matched_requirement_id
    existing_controls_by_req: Dict[int, list] = {}
    for ctrl in all_ctrl_links:
        req_id = ctrl["COMPLIANCEREQUIREMENT_ID"]
        existing_controls_by_req.setdefault(req_id, []).append(ctrl)


    # ── Build grouped response ──
    grouped = []
    for mapping in req_mappings:
        matched_req_id   = mapping.get("matched_requirement_id")
        obligation_id    = mapping.get("obligation_id")
        v2_req_id        = mapping.get("requirement_id")  # e.g. "REQ-001"

        # Pull v2 meta for this obligation's parent requirement
        v2_meta = req_id_to_v2_meta.get(v2_req_id, {})

        # Stage2 obligation detail
        ob_detail = ob_id_to_detail.get(obligation_id, {}) if obligation_id else {}

        # Stage3 AI-designed control for this obligation
        stage3_control = ob_id_to_control.get(obligation_id) if obligation_id else None
        db_controls = existing_controls_by_req.get(matched_req_id, [])

        # ── Control matching logic ──
        controls_output = []
        if db_controls:
            # Compare each existing DB control against the stage3 AI control
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
                # Attach stage3 detail if titles are similar
                if stage3_control:
                    db_title = (db_ctrl.get("control_title") or "").lower().strip()
                    s3_title = (stage3_control.get("control_title") or "").lower().strip()
                    if db_title == s3_title or db_title in s3_title or s3_title in db_title:
                        ctrl_entry["comparison_result"] = "matched_with_ai"
                        ctrl_entry["control_objective"]       = stage3_control.get("control_objective")
                        ctrl_entry["control_owner"]           = stage3_control.get("control_owner")
                        ctrl_entry["control_type"]            = stage3_control.get("control_type")
                        ctrl_entry["execution_type"]          = stage3_control.get("execution_type")
                        ctrl_entry["frequency"]               = stage3_control.get("frequency")
                        ctrl_entry["control_level"]           = stage3_control.get("control_level")
                        ctrl_entry["evidence_generated"]      = stage3_control.get("evidence_generated")
                        ctrl_entry["key_steps"]               = stage3_control.get("key_steps", [])
                        ctrl_entry["residual_risk_if_failed"] = stage3_control.get("residual_risk_if_failed")
                controls_output.append(ctrl_entry)

        # If stage3 has an AI-designed control but NO existing DB control matched → add as new
        if stage3_control:
            matched_titles = {
                (c.get("control_title") or "").lower().strip()
                for c in db_controls
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
                    "match_explanation":       "AI-designed control from staged analysis; no existing control matched.",
                    "is_suggested":            True,
                    "source":                  "stage3_llm",
                    "comparison_result":       "ai_designed",
                })



        entry = {
            # Obligation-level fields
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
            # Requirement-level matching fields
            "matched_requirement_id":          matched_req_id,
            "matched_requirement_title":       mapping.get("matched_requirement_title"),
            "matched_requirement_description": mapping.get("matched_requirement_description"),
            "match_status":                    mapping["match_status"],
            "match_explanation":               mapping.get("match_explanation"),
            # Parent requirement group info
            "requirement_group": {
                "requirement_id":     v2_meta.get("requirement_id"),
                "requirement_title":  v2_meta.get("requirement_title"),
                "execution_category": v2_meta.get("execution_category"),
                "criticality":        v2_meta.get("criticality"),
            },
            "controls": controls_output,
        }
        grouped.append(entry)

    # ── Arabic translation ──
    if lang == "ar":
        OB_TEXT_FIELDS   = ["obligation_text", "test_method", "match_explanation",
                             "matched_requirement_title", "matched_requirement_description"]
        CTRL_TEXT_FIELDS = ["control_title", "control_description", "control_objective",
                             "control_owner", "match_explanation", "evidence_generated"]

        all_texts: list[str] = []
        positions: list[tuple] = []

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
                # Translate key_steps list
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


    # ── Summary ──
    fully   = sum(1 for r in req_mappings if r["match_status"] == "fully_matched")
    partial = sum(1 for r in req_mappings if r["match_status"] == "partially_matched")
    new     = sum(1 for r in req_mappings if r["match_status"] == "new")

    total_controls    = sum(len(e["controls"]) for e in grouped)
    ai_ctrl_count     = sum(
        1 for e in grouped for c in e["controls"] if c.get("source") == "stage3_llm"
    )
    matched_ctrl_count = sum(
        1 for e in grouped for c in e["controls"] if c.get("comparison_result") == "matched_with_ai"
    )

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
                "total":             total_controls,
                "existing_matched":  matched_ctrl_count,
                "ai_designed_new":   ai_ctrl_count,
            }
        }
    }


# ================= COMPLIANCE ANALYSIS ENDPOINTS =================

# ================= HELPER FUNCTIONS =================
# ... (keep existing helpers like serialize_datetime, row_to_dict, etc.)

def build_v2_full_analysis_response(regulation_id: int, lang: str):
    """
    Builds the V2 Full Analysis Response with Nested Structure:
    Requirements -> Obligations -> Control
    Includes detailed mapping info (match_explanation, matched_requirement_id).
    """
    # 1. Fetch V2 Analysis (Requirements & Obligations from stage2, Controls from stage3)
    v2_rows = repo.get_compliance_analysis_v2(regulation_id)
    if not v2_rows:
        return {"requirements": [], "summary": {}}

    # 2. Fetch Requirement Mappings (For match_status, explanation, matched_id)
    req_mappings = repo.get_requirement_mappings_by_regulation(regulation_id)

    # Create lookup for mapping details by obligation_id or obligation_text
    mapping_lookup = {}
    for m in req_mappings:
        key = m.get("obligation_id") or m.get("extracted_requirement_text")
        if key:
            mapping_lookup[key] = m

    # 3. Construct Nested Structure
    requirements_list = []
    total_obligations = 0
    status_counts = {"fully_matched": 0, "partially_matched": 0, "new": 0}

    for row in v2_rows:
        # Parse JSON fields
        s2 = row.get("stage2_json") or {}
        s3 = row.get("stage3_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except:
                s2 = {}
        if isinstance(s3, str):
            try:
                s3 = json.loads(s3)
            except:
                s3 = {}

        # Build Control Lookup for this Requirement (from stage3)
        control_lookup = {}
        for ob in s3.get("obligations", []):
            ob_id = ob.get("obligation_id")
            if ob_id and ob.get("control"):
                control_lookup[ob_id] = ob.get("control")

        # Process Obligations (from stage2)
        obligations = []
        req_status_counts = {"fully_matched": 0, "partially_matched": 0, "new": 0}

        for ob in s2.get("normalized_obligations", []):
            ob_id = ob.get("obligation_id")
            ob_text = ob.get("obligation_text")

            # Get Match Details from Mapping Table
            mapping = mapping_lookup.get(ob_id) or mapping_lookup.get(ob_text) or {}
            match_status = mapping.get("match_status", "new")
            matched_req_id = mapping.get("matched_requirement_id")
            match_explanation = mapping.get("match_explanation")

            # Get Control
            control_obj = control_lookup.get(ob_id)

            obligation_entry = {
                "obligation_id": ob_id,
                "obligation_text": ob_text,
                "obligation_type": ob.get("obligation_type"),
                "criticality": ob.get("criticality"),
                "execution_category": ob.get("execution_category"),
                "evidence_expected": ob.get("evidence_expected"),
                "test_method": ob.get("test_method"),
                "clarity_score": ob.get("clarity_score"),
                "needs_manual_review": ob.get("needs_manual_review"),
                "source_reference": ob.get("source_reference"),
                "match_status": match_status,
                "matched_requirement_id": matched_req_id,  # Added
                "match_explanation": match_explanation,  # Added
                "control": control_obj
            }
            obligations.append(obligation_entry)

            # Count Status
            if match_status in req_status_counts:
                req_status_counts[match_status] += 1
            total_obligations += 1

        # Determine Requirement Level Status (Aggregated)
        agg_status = "fully_matched"
        agg_status = "fully_matched"
        if req_status_counts["new"] > 0:
            agg_status = "new"
        elif req_status_counts["partially_matched"] > 0:
            agg_status = "partially_matched"

        # Update Global Counts
        if agg_status == "fully_matched":
            status_counts["fully_matched"] += 1
        elif agg_status == "partially_matched":
            status_counts["partially_matched"] += 1
        else:
            status_counts["new"] += 1

        requirements_list.append({
            "requirement_id": row.get("requirement_id"),
            "requirement_title": row.get("requirement_title"),
            "execution_category": row.get("execution_category"),
            "criticality": row.get("criticality"),
            "obligation_type": row.get("obligation_type"),
            "match_status": agg_status,
            "obligations": obligations,
            "obligations_total": len(obligations),
            "controls_designed": sum(1 for ob in obligations if ob.get("control")),
            "status": row.get("status"),
            "created_at": serialize_datetime(row.get("created_at"))
        })

    # 4. Translation Handling (Arabic)
    # 4. Translation Handling (Arabic)
    if lang == "ar":
        # Enum/Status Value Translations
        ENUM_TRANSLATIONS = {
            "match_status": {
                "fully_matched": "مطابق بالكامل",
                "partially_matched": "مطابق جزئيًا",
                "new": "جديد"
            },
            "execution_category": {
                "Ongoing_Control": "رقابة مستمرة",
                "One_Time_Implementation": "تنفيذ لمرة واحدة",
                "Periodic_Review": "مراجعة دورية"
            },
            "criticality": {
                "High": "عالي",
                "Medium": "متوسط",
                "Low": "منخفض"
            },
            "obligation_type": {
                "Reporting": "إبلاغ",
                "Governance": "حوكمة",
                "Preventive": "وقائي",
                "Detective": "كشف",
                "Corrective": "تصحيحي"
            },
            "control_type": {
                "Preventive": "وقائي",
                "Detective": "كشف",
                "Corrective": "تصحيحي"
            },
            "execution_type": {
                "Manual": "يدوي",
                "Automated": "آلي",
                "Hybrid": "هجين"
            },
            "frequency": {
                "Daily": "يومي",
                "Weekly": "أسبوعي",
                "Monthly": "شهري",
                "Quarterly": "ربع سنوي",
                "Annually": "سنوي",
                "Event-Driven": "حسب الحدث",
                "Event-Driven (Pre-season)": "حسب الحدث (قبل الموسم)"
            },
            "control_level": {
                "System": "نظام",
                "Process": "عملية",
                "Entity": "كيان",
                "Transaction": "معاملة"
            },
            "residual_risk_if_failed": {
                "High": "عالي",
                "Medium": "متوسط",
                "Low": "منخفض"
            }
        }

        all_texts = []
        positions = []  # (type, req_idx, ob_idx, field, sub_idx)

        for r_idx, req in enumerate(requirements_list):
            # Requirement Level - Text Fields
            if req.get("requirement_title"):
                all_texts.append(req["requirement_title"])
                positions.append(("req_title", r_idx, None, "requirement_title", None))

            # Requirement Level - Enum Fields
            for field in ["execution_category", "criticality", "obligation_type", "match_status"]:
                val = req.get(field)
                if val and field in ENUM_TRANSLATIONS:
                    req[field] = ENUM_TRANSLATIONS[field].get(val, val)

            # Obligation Level
            for o_idx, ob in enumerate(req["obligations"]):
                # Text Fields
                if ob.get("obligation_text"):
                    all_texts.append(ob["obligation_text"])
                    positions.append(("ob_text", r_idx, o_idx, "obligation_text", None))

                # Match Explanation
                if ob.get("match_explanation"):
                    all_texts.append(ob["match_explanation"])
                    positions.append(("ob_exp", r_idx, o_idx, "match_explanation", None))

                # Obligation Enum Fields
                for field in ["obligation_type", "criticality", "execution_category", "match_status"]:
                    val = ob.get(field)
                    if val and field in ENUM_TRANSLATIONS:
                        ob[field] = ENUM_TRANSLATIONS[field].get(val, val)

                # Obligation Text Fields
                for field in ["test_method", "source_reference"]:
                    val = ob.get(field)
                    if val and isinstance(val, str):
                        all_texts.append(val)
                        positions.append(("ob_field", r_idx, o_idx, field, None))

                # evidence_expected (list of strings)
                evidence_list = ob.get("evidence_expected") or []
                for e_idx, evidence in enumerate(evidence_list):
                    if evidence and isinstance(evidence, str):
                        all_texts.append(evidence)
                        positions.append(("evidence", r_idx, o_idx, "evidence_expected", e_idx))

                # Control Level
                ctrl = ob.get("control")
                if ctrl:
                    # Control Text Fields
                    for field in ["control_title", "control_description", "control_objective",
                                  "control_owner", "evidence_generated"]:
                        val = ctrl.get(field)
                        if val and isinstance(val, str):
                            all_texts.append(val)
                            positions.append(("ctrl", r_idx, o_idx, field, None))

                    # Control Enum Fields
                    for field in ["control_type", "execution_type", "frequency", "control_level",
                                  "residual_risk_if_failed"]:
                        val = ctrl.get(field)
                        if val:
                            if field in ENUM_TRANSLATIONS:
                                ctrl[field] = ENUM_TRANSLATIONS[field].get(val, val)
                            elif field == "residual_risk_if_failed":
                                ctrl[field] = ENUM_TRANSLATIONS["criticality"].get(val, val)

                    # key_steps (list of strings)
                    key_steps = ctrl.get("key_steps") or []
                    for k_idx, step in enumerate(key_steps):
                        if step and isinstance(step, str):
                            all_texts.append(step)
                            positions.append(("step", r_idx, o_idx, "key_steps", k_idx))

        # Batch translate all collected texts
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
            "total_obligations": total_obligations,
            "by_match_status": status_counts
        }
    }


# ================= COMPLIANCE ANALYSIS ENDPOINTS =================

@app.get("/compliance-analysis/{regulation_id}")
def get_compliance_analysis_full(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)

    # Check v2 analysis exists
    v2_rows = repo.get_compliance_analysis_v2(regulation_id)
    if not v2_rows:
        raise HTTPException(404, f"No v2 analysis for regulation {regulation_id}. "
                                 f"Run POST /trigger/staged-analysis/{regulation_id} first.")

    # Check requirement mappings exist (for match details)
    req_mappings = repo.get_requirement_mappings_by_regulation(regulation_id)
    # Note: We allow the call even if mappings are missing (status will default to 'new'),
    # but ideally, matching should be run first.

    if lang == "ar":
        cache_key = f"GET /compliance-analysis-full/{regulation_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    # Use the new builder function
    mapping_data = build_v2_full_analysis_response(regulation_id, lang)

    result = {
        "success": True,
        "lang": lang,
        "regulation_id": regulation_id,
        "schema_version": "v2",
        **mapping_data
    }

    if lang == "ar":
        _set_ar_cache(cache_key, result)

    return result

# ================= GAP CACHE KEY HELPER =================
def _gap_cache_key(endpoint: str, regulation_id: int, filename: str) -> str:
    return f"POST {endpoint}|reg={regulation_id}|file={filename}"


# ============================================================ #
#  GAP ANALYSIS — V2 ONLY                                      #
# ============================================================ #

@app.post("/gap-analysis/single", response_model=GapAnalysisResponse, tags=["Gap Analysis"])
async def gap_analysis_single(
    regulation_id: int = Form(...),
    file: UploadFile = File(...),
    lang: str = Form("en"),
):
    """
    V2 gap analysis — single regulation. Each result includes obligation_text,
    obligation metadata (id, requirement_id, requirement_title, criticality,
    obligation_type, execution_category), and the matched control title.
    Summary includes breakdowns by criticality and execution_category.
    """
    lang = _validate_lang(lang)
    if not file.filename.lower().endswith((".pdf", ".docx", ".doc")):
        raise HTTPException(status_code=400, detail="Only PDF and DOCX files are supported")

    if lang == "ar":
        gap_cache_key = _gap_cache_key("/gap-analysis/single", regulation_id, file.filename)
        cached = _get_ar_cache(gap_cache_key)
        if cached:
            return cached

    uploaded_text = await _save_and_extract_file(file)
    if not uploaded_text or len(uploaded_text) < 50:
        raise HTTPException(status_code=422, detail="Could not extract sufficient text from uploaded file")

    session_id = repo.create_gap_session(file.filename, uploaded_text)
    summary    = _run_gap_for_regulation_v2(session_id, regulation_id, uploaded_text)
    summary.results = _enrich_results_with_controls_v2(summary.results, regulation_id)

    if lang == "ar":
        summary.results = _translate_v2_gap_results(summary.results, lang)
        summary = RegulationGapSummary(
            regulation_id=summary.regulation_id,
            results=summary.results,
            summary=summary.summary
        )

    final_response = GapAnalysisResponse(
        session_id=session_id,
        uploaded_document_name=file.filename,
        regulations=[summary]
    )

    if lang == "ar":
        _set_ar_cache(gap_cache_key, final_response.dict())

    return final_response


@app.post("/gap-analysis/multi", response_model=GapAnalysisResponse, tags=["Gap Analysis"])
async def gap_analysis_multi(
    regulation_ids: str = Form(..., description="Comma-separated regulation IDs"),
    file: UploadFile = File(...),
    lang: str = Form("en"),
):
    """
    V2 gap analysis — one document against multiple regulations.
    All regulations must have v2 staged analysis.
    Each result includes obligation_text and full obligation metadata + controls.
    """
    lang = _validate_lang(lang)
    if not file.filename.lower().endswith((".pdf", ".docx", ".doc")):
        raise HTTPException(status_code=400, detail="Only PDF and DOCX files are supported")
    try:
        reg_ids: List[int] = [int(rid.strip()) for rid in regulation_ids.split(",")]
    except ValueError:
        raise HTTPException(status_code=400, detail="regulation_ids must be comma-separated integers e.g. '1,2,3'")
    if not reg_ids:
        raise HTTPException(status_code=400, detail="At least one regulation_id is required")

    uploaded_text = await _save_and_extract_file(file)
    if not uploaded_text or len(uploaded_text) < 50:
        raise HTTPException(status_code=422, detail="Could not extract sufficient text from uploaded file")

    session_id = repo.create_gap_session(file.filename, uploaded_text)
    regulation_summaries = []
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
                    summary=summary.summary
                )
                _set_ar_cache(gap_cache_key, summary.dict())

            regulation_summaries.append(summary)

        except HTTPException as e:
            logger.error(f"[v2] Gap analysis failed for regulation {reg_id}: {e.detail}")
            errors.append({"regulation_id": reg_id, "error": e.detail})
        except Exception as e:
            logger.error(f"[v2] Unexpected error for regulation {reg_id}: {e}")
            errors.append({"regulation_id": reg_id, "error": str(e)})

    if not regulation_summaries:
        raise HTTPException(status_code=500, detail=f"Gap analysis failed for all regulations. Errors: {errors}")
    if errors:
        logger.warning(f"[v2] Partial failures in multi gap analysis: {errors}")

    return GapAnalysisResponse(
        session_id=session_id,
        uploaded_document_name=file.filename,
        regulations=regulation_summaries
    )


@app.post("/gap-analysis/multi-docs", tags=["Gap Analysis"])
async def gap_analysis_multi_docs(
    regulation_id: int = Form(...),
    files: List[UploadFile] = File(...),
    lang: str = Form("en"),
):
    """
    V2 gap analysis — multiple documents against one regulation.
    Each document is analyzed independently with full obligation-level metadata
    and matched control titles.
    """
    lang = _validate_lang(lang)
    if not files:
        raise HTTPException(400, "No files uploaded")

    session_results = []
    errors = []

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
                    summary=summary.summary
                )

            result_entry = {"file_name": file.filename, "session_id": session_id, "summary": summary.dict()}

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
        "errors":             errors
    }


@app.get("/gap-analysis/session/{session_id}", response_model=GapAnalysisResponse, tags=["Gap Analysis"])
def get_gap_session(
    session_id: int,
    lang: str = Query("en"),
):
    lang = _validate_lang(lang)

    if lang == "ar":
        cache_key = f"GET /gap-analysis/session/{session_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    result = repo.get_gap_results_by_session(session_id)
    if not result:
        raise HTTPException(status_code=200, detail=f"No gap analysis session found for ID {session_id}")

    regulations = []
    for reg in result["regulations"]:
        results_list = reg["results"]
        if lang == "ar":
            results_list = [translate_gap_result(r, lang) for r in results_list]
        regulations.append(RegulationGapSummary(
            regulation_id=reg["regulation_id"],
            results=[GapResult(**r) for r in results_list],
            summary=reg["summary"]
        ))

    response = GapAnalysisResponse(
        session_id=result["session_id"],
        uploaded_document_name=result["uploaded_document_name"],
        regulations=regulations
    )

    if lang == "ar":
        _set_ar_cache(cache_key, response.dict())

    return response


# ================= CATEGORIES ENDPOINTS =================

@app.get("/categories")
def get_categories(lang: str = Query("en")):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = "GET /categories"
            cached = _get_ar_cache(cache_key)
            if cached:
                return cached

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT compliancecategory_id, title, parentid, type FROM compliancecategory ORDER BY parentid, title")
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            categories = [row_to_dict(row, columns) for row in rows]

        if lang == "ar":
            titles = [c.get("title") or "" for c in categories]
            translated = translate_texts_batch(titles, lang)
            for cat, tr in zip(categories, translated):
                cat["title"] = tr

        categories_by_id = {cat['compliancecategory_id']: cat for cat in categories}
        root_categories = []
        for cat in categories:
            cat['children'] = []
            if cat['parentid'] is None:
                root_categories.append(cat)
            else:
                parent = categories_by_id.get(cat['parentid'])
                if parent:
                    parent['children'].append(cat)

        response = {'success': True, 'lang': lang, 'data': {'all_categories': categories, 'hierarchy': root_categories, 'total_count': len(categories)}}

        if lang == "ar":
            _set_ar_cache(cache_key, response)

        return response
    except Exception as e:
        logger.exception("Error fetching categories")
        raise HTTPException(status_code=500, detail=f"Failed to fetch categories: {str(e)}")


@app.get("/categories/roots")
def get_root_categories_only(lang: str = Query("en")):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = "GET /categories/roots"
            cached = _get_ar_cache(cache_key)
            if cached:
                return cached

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT compliancecategory_id, title, parentid, type FROM compliancecategory WHERE parentid IS NULL")
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            root_categories = [row_to_dict(row, columns) for row in rows]

        if lang == "ar":
            titles = [c.get("title") or "" for c in root_categories]
            translated = translate_texts_batch(titles, lang)
            for cat, tr in zip(root_categories, translated):
                cat["title"] = tr

        response = {"success": True, "lang": lang, "data": root_categories, "total": len(root_categories)}

        if lang == "ar":
            _set_ar_cache(cache_key, response)

        return response
    except Exception as e:
        logger.exception("Error fetching root categories")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/categories/root")
def get_root_categories_with_children(lang: str = Query("en")):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = "GET /categories/root"
            cached = _get_ar_cache(cache_key)
            if cached:
                return cached

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT compliancecategory_id, title, parentid, type FROM compliancecategory")
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            categories = [row_to_dict(row, columns) for row in rows]

        if lang == "ar":
            titles = [c.get("title") or "" for c in categories]
            translated = translate_texts_batch(titles, lang)
            for cat, tr in zip(categories, translated):
                cat["title"] = tr

        for c in categories:
            c["children"] = []
        root_categories = [c for c in categories if c["parentid"] is None]
        for root in root_categories:
            root["children"] = [c for c in categories if c["parentid"] == root["compliancecategory_id"]]

        response = {"success": True, "lang": lang, "data": root_categories, "total_root_categories": len(root_categories)}

        if lang == "ar":
            _set_ar_cache(cache_key, response)

        return response
    except Exception as e:
        logger.exception("Error fetching root categories")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/categories/children/{parent_id}")
def get_children(parent_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = f"GET /categories/children/{parent_id}"
            cached = _get_ar_cache(cache_key)
            if cached:
                return cached

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT compliancecategory_id, title, parentid, type FROM compliancecategory WHERE parentid = ?", parent_id)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            children = [row_to_dict(row, columns) for row in rows]

        if lang == "ar":
            titles = [c.get("title") or "" for c in children]
            translated = translate_texts_batch(titles, lang)
            for cat, tr in zip(children, translated):
                cat["title"] = tr

        response = {"success": True, "lang": lang, "data": children}

        if lang == "ar":
            _set_ar_cache(cache_key, response)

        return response
    except Exception as e:
        raise HTTPException(500, str(e))


# ================= STATUS ENDPOINTS =================

@app.get("/status/full")
def get_full_status():
    results = {}
    for regulator in REGULATOR_PIPELINES.keys():
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TOP 1 status FROM pipeline_status WHERE regulator=? ORDER BY id DESC", regulator)
            row = cursor.fetchone()
            results[regulator] = row[0] if row else "NOT_STARTED"
    return {"pipeline_status": results, "timestamp": datetime.utcnow().isoformat()}


@app.get("/status/{regulator}")
def get_regulator_status(regulator: str):
    with repo._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 status, started_at, finished_at, error FROM pipeline_status WHERE regulator=? ORDER BY id DESC", regulator)
        row = cursor.fetchone()
    if not row:
        return {"regulator": regulator, "status": "NOT_STARTED"}
    return {
        "regulator":   regulator,
        "status":      row[0],
        "started_at":  serialize_datetime(row[1]),
        "finished_at": serialize_datetime(row[2]),
        "error":       row[3]
    }


@app.post("/update-status/compliance-analysis")
def update_compliance_analysis_status(payload: ComplianceStatusUpdate):
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE compliance_analysis
                SET status = ?
                WHERE regulation_id = ?
                AND requirement_id = ?
            """,
                payload.status,
                payload.regulation_id,
                payload.requirement_id
            )

            conn.commit()

            if cursor.rowcount == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f"No record found for regulation_id={payload.regulation_id} "
                           f"and requirement_id={payload.requirement_id}"
                )

        return {
            "success": True,
            "regulation_id": payload.regulation_id,
            "requirement_id": payload.requirement_id,
            "status": payload.status,
            "message": "Status updated successfully"
        }

    except Exception as e:
        logger.exception("Error updating compliance_analysis status")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/update-status/compliancecategory")
def update_compliancecategory_status(payload: StatusUpdate):
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE compliancecategory SET status = ? WHERE compliancecategory_id = ?", payload.status, payload.record_id)
            conn.commit()
            if cursor.rowcount == 0:
                raise HTTPException(status_code=200, detail=f"Record not found in compliancecategory with id {payload.record_id}")
        return {"success": True, "table": "compliancecategory", "record_id": payload.record_id, "status": payload.status, "message": "Status updated successfully"}
    except Exception as e:
        logger.exception("Error updating compliancecategory status")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/update-status/regulations")
def update_regulations_status(payload: StatusUpdate):
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE regulations SET status = ? WHERE id = ?", payload.status, payload.record_id)
            conn.commit()
            if cursor.rowcount == 0:
                raise HTTPException(status_code=200, detail=f"Record not found in regulations with id {payload.record_id}")
        return {"success": True, "table": "regulations", "record_id": payload.record_id, "status": payload.status, "message": "Status updated successfully"}
    except Exception as e:
        logger.exception("Error updating regulations status")
        raise HTTPException(status_code=500, detail=str(e))


# ================= STEP 2 — V2 =================

@app.post("/trigger/requirement-matching/{regulation_id}", tags=["Step 2"])
def trigger_requirement_matching_v2(regulation_id: int):
    """
    V2 requirement matching. Flattens obligations from staged_analysis and feeds
    individual obligation texts to the matcher (atomic unit = obligation, not
    requirement group). obligation_id and requirement_id are carried through
    onto each mapping for full traceability.
    """
    rows = repo.get_compliance_analysis_v2(regulation_id)
    if not rows:
        raise HTTPException(404, f"No v2 analysis for regulation {regulation_id}. "
                                 f"Run POST /trigger/staged-analysis/{regulation_id} first.")

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
            })

    if not extracted_requirements:
        raise HTTPException(404, f"No obligations found in v2 analysis for regulation {regulation_id}")

    req_text_to_v2_meta = {
        r["requirement_text"]: {
            "obligation_id":  r["_obligation_id"],
            "requirement_id": r["_requirement_id"],
        }
        for r in extracted_requirements
    }

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
        linked_kpis_by_req=linked_kpis_by_req
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

    if requirement_mappings:
        repo.store_requirement_mappings(requirement_mappings)

    partially_matched_ids = [
        m["matched_requirement_id"] for m in requirement_mappings
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
                "title": title, "description": req_text,
                "ref_key": f"V2-AUTO-{regulation_id}-{i}", "ref_no": f"REG-{regulation_id}"
            })
            for ctrl in new_controls_to_insert:
                if ctrl.get("_req_id") is None:
                    ctrl["_req_id"] = new_req_id
            for kpi in new_kpis_to_insert:
                if kpi.get("_req_id") is None:
                    kpi["_req_id"] = new_req_id
        except Exception as e:
            logger.error(f"[v2] Failed to insert new suggested requirement: {e}")

    if control_links:
        repo.store_control_links(control_links)
    if kpi_links:
        repo.store_kpi_links(kpi_links)

    for ctrl in new_controls_to_insert:
        try:
            new_ctrl_id = repo.insert_new_suggested_control({
                "title": ctrl["title"], "description": ctrl["description"], "control_key": ctrl["control_key"]
            })
            req_id = ctrl.get("_req_id")
            if req_id:
                repo.store_control_links([{
                    "compliancerequirement_id": req_id, "control_id": new_ctrl_id,
                    "match_status": "new", "match_explanation": ctrl.get("_explanation", ""),
                    "regulation_id": regulation_id
                }])
        except Exception as e:
            logger.error(f"[v2] Failed to insert new suggested control: {e}")

    for kpi in new_kpis_to_insert:
        try:
            new_kpi_id = repo.insert_new_suggested_kpi({
                "title": kpi["title"], "description": kpi["description"],
                "kisetup_key": kpi["kisetup_key"], "formula": kpi.get("formula", "")
            })
            req_id = kpi.get("_req_id")
            if req_id:
                repo.store_kpi_links([{
                    "compliancerequirement_id": req_id, "kisetup_id": new_kpi_id,
                    "match_status": "new", "match_explanation": kpi.get("_explanation", ""),
                    "regulation_id": regulation_id
                }])
        except Exception as e:
            logger.error(f"[v2] Failed to insert new suggested KPI: {e}")

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
                "new":               sum(1 for m in requirement_mappings if m["match_status"] == "new")
            },
            "controls": {"new_links_added": len(control_links), "new_controls_created": len(new_controls_to_insert)},
            "kpis":     {"new_links_added": len(kpi_links),     "new_kpis_created":     len(new_kpis_to_insert)}
        }
    }


@app.get("/requirement-mapping/{regulation_id}", tags=["Step 2"])
def get_requirement_mapping(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)

    if lang == "ar":
        cache_key = f"GET /requirement-mapping/{regulation_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    results = repo.get_requirement_mappings_by_regulation(regulation_id)
    if not results:
        raise HTTPException(status_code=200, detail=f"No requirement mappings found for regulation {regulation_id}.")

    if lang == "ar":
        results = [translate_compliance_requirement(r, lang) for r in results]

    summary = {
        "fully_matched":     sum(1 for r in results if r.get("match_status") == "fully_matched"),
        "partially_matched": sum(1 for r in results if r.get("match_status") == "partially_matched"),
        "new":               sum(1 for r in results if r.get("match_status") == "new"),
        "total":             len(results)
    }

    response = {"success": True, "lang": lang, "regulation_id": regulation_id, "summary": summary, "mappings": results}

    if lang == "ar":
        _set_ar_cache(cache_key, response)

    return response


@app.get("/control-mapping/{regulation_id}", tags=["Step 2"])
def get_control_mapping(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)

    if lang == "ar":
        cache_key = f"GET /control-mapping/{regulation_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    results = repo.get_control_links_by_regulation(regulation_id)
    if not results:
        raise HTTPException(status_code=200, detail=f"No control links found for regulation {regulation_id}.")

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
        "total":             len(results)
    }

    response = {"success": True, "lang": lang, "regulation_id": regulation_id, "summary": summary, "control_links": results}

    if lang == "ar":
        _set_ar_cache(cache_key, response)

    return response


@app.get("/kpi-mapping/{regulation_id}", tags=["Step 2"])
def get_kpi_mapping(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)

    if lang == "ar":
        cache_key = f"GET /kpi-mapping/{regulation_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    results = repo.get_kpi_links_by_regulation(regulation_id)
    if not results:
        raise HTTPException(status_code=200, detail=f"No KPI links found for regulation {regulation_id}.")

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
        "total":             len(results)
    }

    response = {"success": True, "lang": lang, "regulation_id": regulation_id, "summary": summary, "kpi_links": results}

    if lang == "ar":
        _set_ar_cache(cache_key, response)

    return response


# ================= ADMIN =================

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


# ================= HEALTH & ROOT =================

@app.get("/health")
def health_check():
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
        return {'success': True, 'status': 'healthy', 'database': 'connected', 'timestamp': datetime.utcnow().isoformat()}
    except Exception as e:
        logger.exception("Health check failed")
        return {'success': False, 'status': 'unhealthy', 'database': 'disconnected', 'error': str(e), 'timestamp': datetime.utcnow().isoformat()}


@app.get("/")
def root():
    return {
        "message":             "Regulatory Pipeline API",
        "version":             "2.0.0",
        "supported_languages": ["en", "ar"],
        "schema":              "v2 (obligation-level, all gap analysis endpoints use v2 pipeline)",
        "endpoints": {
            "gap_analysis": {
                "single":     "POST /gap-analysis/single   — one doc vs one regulation",
                "multi":      "POST /gap-analysis/multi    — one doc vs multiple regulations",
                "multi_docs": "POST /gap-analysis/multi-docs — multiple docs vs one regulation",
                "session":    "GET  /gap-analysis/session/{session_id}"
            },
            "step2": {
                "trigger":  "POST /trigger/requirement-matching/{regulation_id}",
                "mappings": "GET  /requirement-mapping/{regulation_id}",
                "controls": "GET  /control-mapping/{regulation_id}",
                "kpis":     "GET  /kpi-mapping/{regulation_id}"
            },
            "v2_staged_analysis": {
                "trigger":      "POST /trigger/staged-analysis/{regulation_id}",
                "list":         "GET  /compliance-analysis-v2/{regulation_id}",
                "detail":       "GET  /compliance-analysis-v2/{regulation_id}/requirement/{requirement_id}",
                "exec_summary": "GET  /compliance-analysis-v2/{regulation_id}/executive-summary"
            },
            "admin": {
                "clear_reg_cache": "DELETE /admin/ar-cache/{regulation_id}",
                "clear_all_cache": "DELETE /admin/ar-cache"
            }
        },
        "available_regulators": list(REGULATOR_PIPELINES.keys())
    }


# ============================================================ #
#  V2 STAGED ANALYSIS ENDPOINTS                                #
# ============================================================ #

staged_analyzer = StagedLLMAnalyzer()


@app.post("/trigger/staged-analysis/{regulation_id}", tags=["V2 Staged Analysis"])
def trigger_staged_analysis(regulation_id: int, force: bool = Query(False)):
    # ── Guard: skip if already analyzed ─────────────────────────────
    if not force:
        existing_rows = repo.get_compliance_analysis_v2(regulation_id)
        if existing_rows:
            return {
                "success": True,
                "regulation_id": regulation_id,
                "skipped": True,
                "reason": "Staged analysis already exists. Use ?force=true to re-run.",
                "existing_count": len(existing_rows),
                "next_step": f"POST /trigger/requirement-matching/{regulation_id}"
            }

    regulation = repo.get_regulation_by_id(regulation_id)
    if not regulation:
        raise HTTPException(status_code=404, detail=f"Regulation {regulation_id} not found")

    text_content   = None
    content_type   = None
    document_title = regulation.get("title", "Untitled")

    extra_meta = regulation.get("extra_meta") or {}
    if isinstance(extra_meta, str):
        try:
            extra_meta = json.loads(extra_meta)
        except Exception:
            extra_meta = {}

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
        raise HTTPException(status_code=422, detail=f"No extractable text found for regulation {regulation_id}.")

    from processor.LlmAnalyzer import LLMAnalyzer as _LLMAnalyzer
    normalizer = _LLMAnalyzer()
    try:
        clean_text = normalizer.normalize_input_text(text_content, content_type=content_type)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Text normalization failed: {str(e)}")

    if len(clean_text) < 200:
        raise HTTPException(status_code=422, detail=f"Extracted text too short ({len(clean_text)} chars).")

    logger.info(f"[staged-analysis] Starting for regulation {regulation_id}")

    raw_date       = regulation.get("published_date")
    published_date = str(raw_date)[:10] if raw_date else ""

    rows = staged_analyzer.analyze(
        text=clean_text,
        regulation_id=regulation_id,
        document_title=document_title,
        regulator=regulation.get("regulator") or "",
        reference=regulation.get("reference_no") or "",
        publication_date=published_date,
    )

    if not rows:
        raise HTTPException(status_code=422, detail=f"Pipeline extracted 0 requirements for regulation {regulation_id}.")

    repo.store_staged_analysis(rows)
    _invalidate_ar_cache(regulation_id)
    logger.info(f"[staged-analysis] Stored {len(rows)} rows for regulation {regulation_id}")

    exec_counts: dict[str, int] = {}
    crit_counts: dict[str, int] = {}
    for r in rows:
        ec = r.get("execution_category") or "Unknown"
        cr = r.get("criticality") or "Unknown"
        exec_counts[ec] = exec_counts.get(ec, 0) + 1
        crit_counts[cr] = crit_counts.get(cr, 0) + 1

    return {
        "success":        True,
        "regulation_id":  regulation_id,
        "document_title": document_title,
        "text_length":    len(clean_text),
        "content_type":   content_type,
        "analysis": {
            "requirements_extracted": len(rows),
            "by_execution_category":  exec_counts,
            "by_criticality":         crit_counts,
        },
        "next_step": f"POST /trigger/requirement-matching/{regulation_id}"
    }


@app.get("/compliance-analysis-v2/{regulation_id}", tags=["V2 Staged Analysis"])
def get_compliance_analysis_v2(
    regulation_id: int,
    execution_category: Optional[str] = Query(None),
    criticality: Optional[str] = Query(None),
    lang: str = Query("en"),
):
    lang = _validate_lang(lang)
    rows = repo.get_compliance_analysis_v2(regulation_id)

    if not rows:
        reg = repo.get_regulation_by_id(regulation_id)
        if not reg:
            raise HTTPException(status_code=404, detail=f"Regulation {regulation_id} not found")
        return {
            "success": True, "regulation_id": regulation_id, "schema_version": "v2",
            "has_analysis": False,
            "message": f"No v2 analysis. Run POST /trigger/staged-analysis/{regulation_id}.",
            "requirements": [], "summary": {}
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
            "status":             row.get("status"),
            "created_at":         serialize_datetime(row.get("created_at")),
        })

    all_rows = repo.get_compliance_analysis_v2(regulation_id)
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
        }
    }


@app.get("/compliance-analysis-v2/{regulation_id}/requirement/{requirement_id}", tags=["V2 Staged Analysis"])
def get_requirement_detail_v2(regulation_id: int, requirement_id: str, lang: str = Query("en")):
    lang = _validate_lang(lang)

    rows = repo.get_compliance_analysis_v2(regulation_id)
    row  = next((r for r in rows if r.get("requirement_id") == requirement_id), None)

    if not row:
        raise HTTPException(status_code=404,
            detail=f"Requirement '{requirement_id}' not found for regulation {regulation_id}. "
                   f"Available: {[r.get('requirement_id') for r in rows]}")

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
        "obligations":        enriched_obligations,
        "obligations_total":  len(enriched_obligations),
        "controls_designed":  sum(1 for ob in enriched_obligations if ob.get("control")),
        "status":             row.get("status"),
        "created_at":         serialize_datetime(row.get("created_at")),
    }


@app.get("/compliance-analysis-v2/{regulation_id}/executive-summary", tags=["V2 Staged Analysis"])
def get_executive_summary_v2(regulation_id: int):
    md = repo.get_stage4_executive_summary(regulation_id)
    if not md:
        rows = repo.get_compliance_analysis_v2(regulation_id)
        if not rows:
            raise HTTPException(404, f"No v2 analysis for regulation {regulation_id}.")
        raise HTTPException(404, "v2 rows exist but executive summary is empty.")
    return {"success": True, "regulation_id": regulation_id, "executive_summary_md": md, "length_chars": len(md)}