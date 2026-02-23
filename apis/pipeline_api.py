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

from utils.lang_translator import (
    translate_regulation,
    translate_gap_result,
    translate_compliance_requirement,
    translate_texts_batch,
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


# ================= GAP ANALYSIS MODELS =================
class GapResult(BaseModel):
    requirement_text: str
    coverage_status: str
    evidence_text: Optional[str]
    gap_description: Optional[str]
    controls: Optional[str] = None
    kpis: Optional[str] = None


class RegulationGapSummary(BaseModel):
    regulation_id: int
    results: List[GapResult]
    summary: Dict[str, Any]


class GapAnalysisResponse(BaseModel):
    session_id: int
    uploaded_document_name: str
    regulations: List[RegulationGapSummary]


# ================= GAP ANALYSIS HELPERS =================
def _extract_requirements_from_analysis(analysis_data) -> list:
    try:
        data = json.loads(analysis_data) if isinstance(analysis_data, str) else analysis_data
        return data.get("requirements", [])
    except Exception as e:
        logger.error(f"Failed to parse analysis_json: {e}")
        return []


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


def _run_gap_for_regulation(session_id: int, regulation_id: int, uploaded_text: str) -> RegulationGapSummary:
    analysis = repo.get_compliance_analysis(regulation_id)
    if not analysis:
        raise HTTPException(status_code=404, detail=f"No compliance analysis found for regulation {regulation_id}")

    requirements = _extract_requirements_from_analysis(analysis["analysis_data"])
    if not requirements:
        raise HTTPException(status_code=404, detail=f"No requirements found for regulation {regulation_id}")

    logger.info(f"Running gap analysis: regulation {regulation_id}, {len(requirements)} requirements")

    results = gap_analyzer.analyze_gaps(uploaded_text=uploaded_text, requirements=requirements)
    repo.store_gap_results(session_id, regulation_id, results)

    summary = {
        "total":   len(results),
        "covered": sum(1 for r in results if r["coverage_status"] == "covered"),
        "partial": sum(1 for r in results if r["coverage_status"] == "partial"),
        "missing": sum(1 for r in results if r["coverage_status"] == "missing")
    }

    return RegulationGapSummary(
        regulation_id=regulation_id,
        results=[GapResult(**r) for r in results],
        summary=summary
    )


def _enrich_results_with_controls_kpis(
        results: List[GapResult],
        regulation_id: int
) -> List[GapResult]:
    analysis = repo.get_compliance_analysis(regulation_id)
    if not analysis:
        return results

    requirements = _extract_requirements_from_analysis(analysis["analysis_data"])
    req_lookup = {r["requirement_text"]: r for r in requirements}

    for result in results:
        match = req_lookup.get(result.requirement_text)
        if match:
            controls = match.get("controls")
            kpis = match.get("kpis")
            # controls/kpis may be a list (from analysis JSON) or already a string
            if isinstance(controls, list):
                controls = ", ".join(str(c) for c in controls if c)
            if isinstance(kpis, list):
                kpis = ", ".join(str(k) for k in kpis if k)
            result.controls = controls
            result.kpis = kpis

    return results


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
        # include all params in key so different pages/filters cache separately
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

@app.get("/compliance-analysis/{regulation_id}", response_model=ComplianceAnalysisResponse)
def get_compliance_analysis(
    regulation_id: int,
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
    lang = _validate_lang(lang)
    try:
        if lang == "ar":
            cache_key = f"GET /compliance-analysis/{regulation_id}"
            cached = _get_ar_cache(cache_key)
            if cached:
                return cached

        query = """
            SELECT id, regulation_id, analysis_json,
                   CAST(created_at AS DATETIME2) as created_at,
                   CAST(updated_at AS DATETIME2) as updated_at
            FROM compliance_analysis
            WHERE regulation_id = ?
        """
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (regulation_id,))
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=200, detail=f"Compliance analysis not found for regulation ID {regulation_id}")
            columns = [col[0] for col in cursor.description]
            analysis_dict = row_to_dict(row, columns)
            if analysis_dict.get("analysis_json"):
                try:
                    analysis_dict["analysis_json"] = json.loads(analysis_dict["analysis_json"])
                except:
                    pass

        if lang == "ar" and isinstance(analysis_dict.get("analysis_json"), dict):
            reqs = analysis_dict["analysis_json"].get("requirements", [])
            if reqs:
                analysis_dict["analysis_json"]["requirements"] = [
                    translate_compliance_requirement(r, lang) for r in reqs
                ]

        response = {"success": True, "lang": lang, "data": analysis_dict}

        if lang == "ar":
            _set_ar_cache(cache_key, response)

        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching compliance analysis for regulation {regulation_id}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch compliance analysis: {str(e)}")


def build_full_mapping_response(regulation_id: int, lang: str):
    req_mappings = repo.get_requirement_mappings_by_regulation(regulation_id)
    ctrl_links   = repo.get_control_links_by_regulation(regulation_id)
    kpi_links    = repo.get_kpi_links_by_regulation(regulation_id)

    controls_by_req = {}
    for ctrl in ctrl_links:
        req_id = ctrl["COMPLIANCEREQUIREMENT_ID"]
        controls_by_req.setdefault(req_id, []).append({
            "control_id": ctrl["CONTROL_ID"],
            "control_title": ctrl.get("control_title"),
            "control_description": ctrl.get("control_description"),
            "control_key": ctrl.get("control_key"),
            "match_status": ctrl["MATCH_STATUS"],
            "match_explanation": ctrl.get("MATCH_EXPLANATION"),
            "is_suggested": ctrl.get("is_suggested") == 1
        })

    kpis_by_req = {}
    for kpi in kpi_links:
        req_id = kpi["COMPLIANCEREQUIREMENT_ID"]
        kpis_by_req.setdefault(req_id, []).append({
            "kisetup_id": kpi["KISETUP_ID"],
            "kpi_title": kpi.get("kpi_title"),
            "kpi_description": kpi.get("kpi_description"),
            "kisetup_key": kpi.get("kisetup_key"),
            "formula": kpi.get("formula"),
            "match_status": kpi["MATCH_STATUS"],
            "match_explanation": kpi.get("MATCH_EXPLANATION"),
            "is_suggested": kpi.get("is_suggested") == 1
        })

    # ── Build grouped entries (English first) ────────────────────────────
    grouped = []
    for mapping in req_mappings:
        matched_req_id = mapping.get("matched_requirement_id")
        entry = {
            "extracted_requirement_text": mapping["extracted_requirement_text"],
            "match_status": mapping["match_status"],
            "match_explanation": mapping.get("match_explanation"),
            "matched_requirement_id": matched_req_id,
            "matched_requirement_title": mapping.get("matched_requirement_title"),
            "matched_requirement_description": mapping.get("matched_requirement_description"),
            "controls": controls_by_req.get(matched_req_id, []),
            "kpis": kpis_by_req.get(matched_req_id, []),
        }
        grouped.append(entry)

    # ── Translate everything in ONE batch call ───────────────────────────
    if lang == "ar":
        # Collect every translatable string with its location
        REQ_TEXT_FIELDS = [
            "extracted_requirement_text", "match_explanation",
            "matched_requirement_title", "matched_requirement_description",
        ]
        CTRL_TEXT_FIELDS = ["control_title", "control_description", "match_explanation"]
        KPI_TEXT_FIELDS  = ["kpi_title", "kpi_description", "match_explanation"]

        all_texts: list[str] = []
        positions: list[tuple] = []  # (entry_idx, field_or_path, sub_idx?)

        for i, entry in enumerate(grouped):
            # requirement-level fields
            for f in REQ_TEXT_FIELDS:
                val = entry.get(f)
                if val and isinstance(val, str):
                    all_texts.append(val)
                    positions.append(("req", i, f, None))

            # controls
            for j, ctrl in enumerate(entry["controls"]):
                for f in CTRL_TEXT_FIELDS:
                    val = ctrl.get(f)
                    if val and isinstance(val, str):
                        all_texts.append(val)
                        positions.append(("ctrl", i, f, j))

            # kpis
            for j, kpi in enumerate(entry["kpis"]):
                for f in KPI_TEXT_FIELDS:
                    val = kpi.get(f)
                    if val and isinstance(val, str):
                        all_texts.append(val)
                        positions.append(("kpi", i, f, j))

        # Single batch translate — translate_texts_batch already handles chunking
        translated = translate_texts_batch(all_texts, lang)

        # Deep-copy grouped so we don't mutate the source dicts
        import copy
        grouped = copy.deepcopy(grouped)

        # Write translated values back
        for (kind, i, f, j), tr in zip(positions, translated):
            if kind == "req":
                grouped[i][f] = tr
            elif kind == "ctrl":
                grouped[i]["controls"][j][f] = tr
            elif kind == "kpi":
                grouped[i]["kpis"][j][f] = tr

    fully   = sum(1 for r in req_mappings if r["match_status"] == "fully_matched")
    partial = sum(1 for r in req_mappings if r["match_status"] == "partially_matched")
    new     = sum(1 for r in req_mappings if r["match_status"] == "new")

    return {
        "requirements": grouped,
        "summary": {
            "requirements": {
                "total": len(req_mappings),
                "fully_matched": fully,
                "partially_matched": partial,
                "new": new
            },
            "controls": {
                "total": len(ctrl_links),
                "ai_suggested": sum(1 for c in ctrl_links if c.get("is_suggested") == 1)
            },
            "kpis": {
                "total": len(kpi_links),
                "ai_suggested": sum(1 for k in kpi_links if k.get("is_suggested") == 1)
            }
        }
    }

    def _translate_control_list(controls: list, translator) -> list:
        """Translate human-readable fields in a list of control dicts."""
        translated = []
        for ctrl in controls:
            ctrl = dict(ctrl)
            for f in ["control_title", "control_description", "match_explanation"]:
                val = ctrl.get(f)
                if val and isinstance(val, str):
                    try:
                        ctrl[f] = translator.translate(val)
                    except Exception as e:
                        logger.error(f"control field '{f}' translation failed: {e}")
            translated.append(ctrl)
        return translated

    def _translate_kpi_list(kpis: list, translator) -> list:
        """Translate human-readable fields in a list of KPI dicts."""
        translated = []
        for kpi in kpis:
            kpi = dict(kpi)
            for f in ["kpi_title", "kpi_description", "match_explanation"]:
                val = kpi.get(f)
                if val and isinstance(val, str):
                    try:
                        kpi[f] = translator.translate(val)
                    except Exception as e:
                        logger.error(f"kpi field '{f}' translation failed: {e}")
            translated.append(kpi)
        return translated

    # Build a single translator instance to reuse across all entries (avoids
    # constructing a new GoogleTranslator object for every requirement).
    ar_translator = None
    if lang == "ar":
        from deep_translator import GoogleTranslator
        ar_translator = GoogleTranslator(source="en", target="ar")

    grouped = []
    for mapping in req_mappings:
        matched_req_id = mapping.get("matched_requirement_id")
        controls = controls_by_req.get(matched_req_id, [])
        kpis     = kpis_by_req.get(matched_req_id, [])

        entry = {
            "extracted_requirement_text": mapping["extracted_requirement_text"],
            "match_status": mapping["match_status"],
            "match_explanation": mapping.get("match_explanation"),
            "matched_requirement_id": matched_req_id,
            "matched_requirement_title": mapping.get("matched_requirement_title"),
            "matched_requirement_description": mapping.get("matched_requirement_description"),
            "controls": controls,
            "kpis": kpis,
        }

        if lang == "ar":
            # Translate the top-level requirement text fields
            entry = translate_compliance_requirement(entry, lang)

            # translate_compliance_requirement treats controls/kpis as lists of
            # strings (legacy shape) — but here they are lists of dicts, so we
            # translate them separately after the call above.
            entry["controls"] = _translate_control_list(controls, ar_translator)
            entry["kpis"]     = _translate_kpi_list(kpis, ar_translator)

        grouped.append(entry)

    fully   = sum(1 for r in req_mappings if r["match_status"] == "fully_matched")
    partial = sum(1 for r in req_mappings if r["match_status"] == "partially_matched")
    new     = sum(1 for r in req_mappings if r["match_status"] == "new")

    return {
        "requirements": grouped,
        "summary": {
            "requirements": {
                "total": len(req_mappings),
                "fully_matched": fully,
                "partially_matched": partial,
                "new": new
            },
            "controls": {
                "total": len(ctrl_links),
                "ai_suggested": sum(1 for c in ctrl_links if c.get("is_suggested") == 1)
            },
            "kpis": {
                "total": len(kpi_links),
                "ai_suggested": sum(1 for k in kpi_links if k.get("is_suggested") == 1)
            }
        }
    }

@app.get("/compliance-analysis-full/{regulation_id}")
def get_compliance_analysis_full(regulation_id: int, lang: str = Query("en")):
    lang = _validate_lang(lang)

    analysis = repo.get_compliance_analysis(regulation_id)
    if not analysis:
        raise HTTPException(404, "No analysis found")

    if lang == "ar":
        cache_key = f"GET /compliance-analysis-full/{regulation_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    mapping_data = build_full_mapping_response(regulation_id, lang)

    result = {
        "success": True,
        "lang": lang,
        "regulation_id": regulation_id,
        "analysis_meta": {
            "id": analysis["id"],
            "created_at": serialize_datetime(analysis["created_at"]),
            "updated_at": serialize_datetime(analysis["updated_at"])
        },
        **mapping_data
    }

    if lang == "ar":
        _set_ar_cache(cache_key, result)

    return result


# ================= GAP ANALYSIS ENDPOINTS =================

# ================= GAP CACHE KEY HELPER =================
def _gap_cache_key(endpoint: str, regulation_id: int, filename: str) -> str:
    return f"POST {endpoint}|reg={regulation_id}|file={filename}"


# ================= GAP ANALYSIS ENDPOINTS =================

@app.post("/gap-analysis/single", response_model=GapAnalysisResponse, tags=["Gap Analysis"])
async def gap_analysis_single(
    regulation_id: int = Form(..., description="ID of the regulation to check against"),
    file: UploadFile = File(..., description="PDF document to check"),
    lang: str = Form("en", description="Language: 'en' or 'ar'"),
):
    lang = _validate_lang(lang)
    if not file.filename.lower().endswith((".pdf", ".docx", ".doc")):
        raise HTTPException(status_code=400, detail="Only PDF and DOCX files are supported")

    # Check cache before extracting/processing
    if lang == "ar":
        gap_cache_key = _gap_cache_key("/gap-analysis/single", regulation_id, file.filename)
        cached = _get_ar_cache(gap_cache_key)
        if cached:
            logger.info(f"AR cache hit for gap-analysis/single: {gap_cache_key}")
            return cached

    uploaded_text = await _save_and_extract_file(file)
    if not uploaded_text or len(uploaded_text) < 50:
        raise HTTPException(status_code=422, detail="Could not extract sufficient text from uploaded file")

    session_id = repo.create_gap_session(file.filename, uploaded_text)
    regulation_summary = _run_gap_for_regulation(session_id, regulation_id, uploaded_text)
    regulation_summary.results = _enrich_results_with_controls_kpis(regulation_summary.results, regulation_id)

    if lang == "ar":
        GAP_TEXT_FIELDS = ["requirement_text", "evidence_text", "gap_description", "controls", "kpis"]
        all_texts, positions = [], []
        results_dicts = [r.dict() for r in regulation_summary.results]
        for i, r in enumerate(results_dicts):
            for f in GAP_TEXT_FIELDS:
                val = r.get(f)
                if val and isinstance(val, str):
                    all_texts.append(val)
                    positions.append((i, f))
        if all_texts:
            translated = translate_texts_batch(all_texts, lang)
            for (i, f), tr in zip(positions, translated):
                results_dicts[i][f] = tr
        regulation_summary = RegulationGapSummary(
            regulation_id=regulation_summary.regulation_id,
            results=[GapResult(**r) for r in results_dicts],
            summary=regulation_summary.summary
        )

    final_response = GapAnalysisResponse(
        session_id=session_id,
        uploaded_document_name=file.filename,
        regulations=[regulation_summary]
    )

    if lang == "ar":
        _set_ar_cache(gap_cache_key, final_response.dict())

    return final_response


@app.post("/gap-analysis/multi-docs", tags=["Gap Analysis"])
async def gap_analysis_multi_docs(
    regulation_id: int = Form(...),
    files: List[UploadFile] = File(...),
    lang: str = Form("en", description="Language: 'en' or 'ar'"),
):
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
            # Check cache before processing this file
            if lang == "ar":
                gap_cache_key = _gap_cache_key("/gap-analysis/multi-docs", regulation_id, file.filename)
                cached = _get_ar_cache(gap_cache_key)
                if cached:
                    logger.info(f"AR cache hit for gap-analysis/multi-docs: {gap_cache_key}")
                    session_results.append(cached)
                    continue

            uploaded_text = await _save_and_extract_file(file)
            if len(uploaded_text) < 50:
                raise Exception("Too little text extracted")
            session_id = repo.create_gap_session(file.filename, uploaded_text)
            summary = _run_gap_for_regulation(session_id, regulation_id, uploaded_text)
            summary.results = _enrich_results_with_controls_kpis(summary.results, regulation_id)

            if lang == "ar":
                GAP_TEXT_FIELDS = ["requirement_text", "evidence_text", "gap_description", "controls", "kpis"]
                all_texts, positions = [], []
                results_dicts = [r.dict() for r in summary.results]
                for i, r in enumerate(results_dicts):
                    for f in GAP_TEXT_FIELDS:
                        val = r.get(f)
                        if val and isinstance(val, str):
                            all_texts.append(val)
                            positions.append((i, f))
                if all_texts:
                    translated = translate_texts_batch(all_texts, lang)
                    for (i, f), tr in zip(positions, translated):
                        results_dicts[i][f] = tr
                summary = RegulationGapSummary(
                    regulation_id=summary.regulation_id,
                    results=[GapResult(**r) for r in results_dicts],
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

    return {"success": True, "lang": lang, "regulation_id": regulation_id, "documents_analyzed": session_results, "errors": errors}


@app.post("/gap-analysis/multi", response_model=GapAnalysisResponse, tags=["Gap Analysis"])
async def gap_analysis_multi(
    regulation_ids: str = Form(..., description="Comma-separated regulation IDs e.g. 1,2,3"),
    file: UploadFile = File(..., description="PDF document to check"),
    lang: str = Form("en", description="Language: 'en' or 'ar'"),
):
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
            # Check cache before processing this regulation
            if lang == "ar":
                gap_cache_key = _gap_cache_key("/gap-analysis/multi", reg_id, file.filename)
                cached = _get_ar_cache(gap_cache_key)
                if cached:
                    logger.info(f"AR cache hit for gap-analysis/multi: {gap_cache_key}")
                    regulation_summaries.append(RegulationGapSummary(**cached))
                    continue

            summary = _run_gap_for_regulation(session_id, reg_id, uploaded_text)
            summary.results = _enrich_results_with_controls_kpis(summary.results, reg_id)

            if lang == "ar":
                GAP_TEXT_FIELDS = ["requirement_text", "evidence_text", "gap_description", "controls", "kpis"]
                all_texts, positions = [], []
                results_dicts = [r.dict() for r in summary.results]
                for i, r in enumerate(results_dicts):
                    for f in GAP_TEXT_FIELDS:
                        val = r.get(f)
                        if val and isinstance(val, str):
                            all_texts.append(val)
                            positions.append((i, f))
                if all_texts:
                    translated = translate_texts_batch(all_texts, lang)
                    for (i, f), tr in zip(positions, translated):
                        results_dicts[i][f] = tr
                summary = RegulationGapSummary(
                    regulation_id=summary.regulation_id,
                    results=[GapResult(**r) for r in results_dicts],
                    summary=summary.summary
                )
                _set_ar_cache(gap_cache_key, summary.dict())

            regulation_summaries.append(summary)

        except HTTPException as e:
            logger.error(f"Gap analysis failed for regulation {reg_id}: {e.detail}")
            errors.append({"regulation_id": reg_id, "error": e.detail})
        except Exception as e:
            logger.error(f"Unexpected error for regulation {reg_id}: {e}")
            errors.append({"regulation_id": reg_id, "error": str(e)})

    if not regulation_summaries:
        raise HTTPException(status_code=500, detail=f"Gap analysis failed for all regulations. Errors: {errors}")
    if errors:
        logger.warning(f"Partial failures in multi gap analysis: {errors}")

    return GapAnalysisResponse(
        session_id=session_id,
        uploaded_document_name=file.filename,
        regulations=regulation_summaries
    )


@app.get("/gap-analysis/session/{session_id}", response_model=GapAnalysisResponse, tags=["Gap Analysis"])
def get_gap_session(
    session_id: int,
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
    lang = _validate_lang(lang)

    if lang == "ar":
        cache_key = f"GET /gap-analysis/session/{session_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    result = repo.get_gap_results_by_session(session_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"No gap analysis session found for ID {session_id}")

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
def get_categories(
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
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
def get_root_categories_only(
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
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
def get_root_categories_with_children(
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
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
def get_children(
    parent_id: int,
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
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
        "regulator": regulator, "status": row[0],
        "started_at": serialize_datetime(row[1]),
        "finished_at": serialize_datetime(row[2]),
        "error": row[3]
    }


@app.post("/update-status/compliance-analysis")
def update_compliance_analysis_status(payload: StatusUpdate):
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE compliance_analysis SET status = ? WHERE id = ?", payload.status, payload.record_id)
            conn.commit()
            if cursor.rowcount == 0:
                raise HTTPException(status_code=200, detail=f"Record not found in compliance_analysis with id {payload.record_id}")
        return {"success": True, "table": "compliance_analysis", "record_id": payload.record_id, "status": payload.status, "message": "Status updated successfully"}
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


# ================= STEP 2 ENDPOINTS =================

@app.post("/trigger/requirement-matching/{regulation_id}", tags=["Step 2"])
def trigger_requirement_matching(regulation_id: int):
    analysis = repo.get_compliance_analysis(regulation_id)
    if not analysis:
        raise HTTPException(status_code=404, detail=f"No compliance analysis found for regulation {regulation_id}")
    extracted_requirements = analysis["analysis_data"].get("requirements", [])
    if not extracted_requirements:
        raise HTTPException(status_code=404, detail=f"No requirements in analysis for regulation {regulation_id}")

    existing_requirements  = repo.get_all_compliance_requirements()
    existing_controls      = repo.get_all_demo_controls()
    existing_kpis          = repo.get_all_demo_kpis()
    linked_controls_by_req = repo.get_linked_controls_by_requirement()
    linked_kpis_by_req     = repo.get_linked_kpis_by_requirement()

    from processor.requirement_matcher import RequirementMatcher
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
                "ref_key": f"SAMA-AUTO-{regulation_id}-{i}", "ref_no": f"REG-{regulation_id}"
            })
            for ctrl in new_controls_to_insert:
                if ctrl.get("_req_id") is None:
                    ctrl["_req_id"] = new_req_id
            for kpi in new_kpis_to_insert:
                if kpi.get("_req_id") is None:
                    kpi["_req_id"] = new_req_id
        except Exception as e:
            logger.error(f"Failed to insert new suggested requirement: {e}")

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
            logger.error(f"Failed to insert new suggested control: {e}")

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
            logger.error(f"Failed to insert new suggested KPI: {e}")

    # Bust Arabic cache since underlying data changed
    _invalidate_ar_cache(regulation_id)

    return {
        "success": True, "regulation_id": regulation_id,
        "summary": {
            "requirements": {
                "total": len(requirement_mappings),
                "fully_matched":     sum(1 for m in requirement_mappings if m["match_status"] == "fully_matched"),
                "partially_matched": sum(1 for m in requirement_mappings if m["match_status"] == "partially_matched"),
                "new":               sum(1 for m in requirement_mappings if m["match_status"] == "new")
            },
            "controls": {"new_links_added": len(control_links), "new_controls_created": len(new_controls_to_insert)},
            "kpis":     {"new_links_added": len(kpi_links),     "new_kpis_created":     len(new_kpis_to_insert)}
        }
    }


@app.get("/requirement-mapping/{regulation_id}", tags=["Step 2"])
def get_requirement_mapping(
    regulation_id: int,
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
    lang = _validate_lang(lang)

    if lang == "ar":
        cache_key = f"GET /requirement-mapping/{regulation_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    results = repo.get_requirement_mappings_by_regulation(regulation_id)
    if not results:
        raise HTTPException(status_code=200, detail=f"No requirement mappings found for regulation {regulation_id}. Run POST /trigger/requirement-matching/{regulation_id} first.")

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
def get_control_mapping(
    regulation_id: int,
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
    lang = _validate_lang(lang)

    if lang == "ar":
        cache_key = f"GET /control-mapping/{regulation_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    results = repo.get_control_links_by_regulation(regulation_id)
    if not results:
        raise HTTPException(status_code=200, detail=f"No control links found for regulation {regulation_id}. Run POST /trigger/requirement-matching/{regulation_id} first.")

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
def get_kpi_mapping(
    regulation_id: int,
    lang: str = Query("en", description="Language: 'en' or 'ar'"),
):
    lang = _validate_lang(lang)

    if lang == "ar":
        cache_key = f"GET /kpi-mapping/{regulation_id}"
        cached = _get_ar_cache(cache_key)
        if cached:
            return cached

    results = repo.get_kpi_links_by_regulation(regulation_id)
    if not results:
        raise HTTPException(status_code=200, detail=f"No KPI links found for regulation {regulation_id}. Run POST /trigger/requirement-matching/{regulation_id} first.")

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
    """Manually clear Arabic cache for a regulation (e.g. after re-analysis)."""
    _invalidate_ar_cache(regulation_id)
    return {"success": True, "message": f"Arabic cache cleared for regulation {regulation_id}"}


@app.delete("/admin/ar-cache", tags=["Admin"])
def clear_all_ar_cache():
    """Wipe the entire Arabic cache table."""
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
        "message": "Regulatory Pipeline API",
        "version": "2.0.0",
        "supported_languages": ["en", "ar"],
        "language_usage": "Add ?lang=ar to any GET endpoint. For POST form endpoints, include lang=ar as a form field.",
        "endpoints": {
            "pipelines": {
                "trigger_specific": "POST /trigger/{regulator}",
                "trigger_all":      "POST /trigger/full",
                "update_schedule":  "POST /update-schedule"
            },
            "data": {
                "regulations_by_regulator": "GET /regulations/{regulator}?lang=ar",
                "regulation_detail":        "GET /regulation/{id}?lang=ar",
                "compliance_analysis":      "GET /compliance-analysis/{regulation_id}?lang=ar",
                "categories":               "GET /categories?lang=ar"
            },
            "gap_analysis": {
                "single_regulation":    "POST /gap-analysis/single  (form field: lang=ar)",
                "multiple_regulations": "POST /gap-analysis/multi   (form field: lang=ar)",
                "retrieve_session":     "GET /gap-analysis/session/{session_id}?lang=ar"
            },
            "admin": {
                "clear_regulation_cache": "DELETE /admin/ar-cache/{regulation_id}",
                "clear_all_cache":        "DELETE /admin/ar-cache"
            },
            "health": "GET /health"
        },
        "available_regulators": list(REGULATOR_PIPELINES.keys())
    }