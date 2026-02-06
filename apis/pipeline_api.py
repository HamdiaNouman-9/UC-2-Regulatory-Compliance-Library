from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import logging
import os
import json
from typing import Optional, List, Dict, Any

# ---------------- Celery imports commented out ----------------
# from scheduler.celery_app import celery_app, update_schedule, load_schedules_from_db

from scheduler.scheduler import run_sbp_pipeline, run_secp_pipeline, run_sama_pipeline
from storage.mssql_repo import MSSQLRepository

logger = logging.getLogger(__name__)
app = FastAPI(title="Regulatory Pipeline API", version="2.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly in production
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

# Mapping regulator names to functions
REGULATOR_PIPELINES = {
    "SBP": run_sbp_pipeline,
    "SECP": run_secp_pipeline,
    "SAMA": run_sama_pipeline
}


# ---------------- Celery DB/schedule loading commented out ----------------
# Load schedules from DB on startup
# load_schedules_from_db()


# ================= HELPER FUNCTIONS =================
def serialize_datetime(obj):
    """Convert datetime objects to ISO format string"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def row_to_dict(row, columns):
    """Convert database row to dictionary with datetime serialization"""
    result = {}
    for col, value in zip(columns, row):
        result[col] = serialize_datetime(value)
    return result


# ---------- Background pipeline runner ----------
def run_regulator_pipeline(regulator: str):
    try:
        logger.info(f"Starting {regulator} pipeline")
        pipeline_func = REGULATOR_PIPELINES.get(regulator)
        if pipeline_func:
            pipeline_func()
        logger.info(f"{regulator} pipeline completed")
    except Exception as e:
        logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)


# ---------- API Models ----------
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


# ---------- API Endpoints ----------
@app.post("/update-schedule")
def update_pipeline_schedule(payload: ScheduleUpdate):
    if payload.regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")

    # ---------------- Celery schedule update commented out ----------------
    # update_schedule(payload.regulator, payload.hour, payload.minute)

    return {
        "status": "success",
        "regulator": payload.regulator,
        "hour": payload.hour,
        "minute": payload.minute,
        "message": f"{payload.regulator} schedule updated successfully"
    }


@app.post("/trigger/{regulator}")
def trigger_regulator_pipeline(regulator: str):
    if regulator not in REGULATOR_PIPELINES:
        raise HTTPException(status_code=400, detail="Unknown regulator")

    try:
        logger.info(f"Starting {regulator} pipeline (synchronous call)")
        pipeline_func = REGULATOR_PIPELINES[regulator]
        pipeline_func()  # <-- runs immediately without Celery
        logger.info(f"{regulator} pipeline completed")

        return {
            "status": "done",  # indicate the pipeline has completed
            "regulator": regulator,
            "completed_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"{regulator} pipeline failed: {e}")


@app.post("/trigger/full")
def trigger_full_pipeline():
    completed_regulators = []
    errors = []

    for regulator, pipeline_func in REGULATOR_PIPELINES.items():
        try:
            logger.info(f"Starting {regulator} pipeline")
            pipeline_func()
            completed_regulators.append(regulator)
            logger.info(f"{regulator} pipeline completed")
        except Exception as e:
            logger.error(f"Error in {regulator} pipeline: {e}", exc_info=True)
            errors.append({"regulator": regulator, "error": str(e)})

    return {
        "status": "done" if not errors else "partial_failure",
        "completed_regulators": completed_regulators,
        "errors": errors,
        "completed_at": datetime.utcnow().isoformat()
    }


# ================= NEW GET API: REGULATIONS BY REGULATOR =================
@app.get("/regulations/{regulator}", response_model=RegulationResponse)
def get_regulations_by_regulator(
        regulator: str,
        category_id: Optional[int] = Query(None, description="Filter by compliance category ID"),
        year: Optional[int] = Query(None, description="Filter by year"),
        limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
        offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """
    Get all regulations for a specific regulator with optional filters.

    **Parameters:**
    - **regulator**: Regulator name (SBP, SECP, SAMA, etc.) [required]
    - **category_id**: Optional filter by compliance category ID
    - **year**: Optional filter by year
    - **limit**: Number of records to return (default: 100, max: 1000)
    - **offset**: Offset for pagination (default: 0)
    """
    try:
        query = """
            SELECT 
                r.id,
                r.regulator,
                r.source_system,
                r.category,
                r.title,
                r.document_url,
                TRY_CAST(r.published_date AS DATETIME) AS published_date,
                r.reference_no,
                r.department,
                r.[year],
                r.source_page_url,
                r.extra_meta,
                TRY_CAST(r.created_at AS DATETIME) AS created_at,
                TRY_CAST(r.updated_at AS DATETIME) AS updated_at,
                r.compliancecategory_id,
                cc.title AS category_title,
                cc.parentid AS category_parent_id
            FROM regulations r
            LEFT JOIN compliancecategory cc 
                ON r.compliancecategory_id = cc.compliancecategory_id
            WHERE r.regulator = ?
        """

        # Build parameters list
        params = [regulator.upper()]

        if category_id is not None:
            query += " AND r.compliancecategory_id = ?"
            params.append(category_id)

        if year is not None:
            query += " AND r.[year] = ?"
            params.append(year)

        # Pagination must always be included
        query += " ORDER BY r.published_date DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        params.extend([offset, limit])

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            regulations = []
            for row in rows:
                reg_dict = row_to_dict(row, columns)
                # Parse extra_meta JSON if exists
                if reg_dict.get('extra_meta'):
                    try:
                        reg_dict['extra_meta'] = json.loads(reg_dict['extra_meta'])
                    except:
                        pass

                # Add category info
                reg_dict['category_info'] = {
                    'id': reg_dict.pop('compliancecategory_id', None),
                    'title': reg_dict.pop('category_title', None),
                    'parent_id': reg_dict.pop('category_parent_id', None)
                }

                regulations.append(reg_dict)

            # Total count for pagination
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

        return {
            "success": True,
            "data": regulations,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": (offset + limit) < total_count,
                "current_page": (offset // limit) + 1,
                "total_pages": (total_count + limit - 1) // limit
            }
        }

    except Exception as e:
        logger.exception(f"Error fetching regulations for {regulator}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch regulations: {str(e)}")


# ================= GET COMPLIANCE ANALYSIS BY REGULATION ID =================
@app.get("/compliance-analysis/{regulation_id}", response_model=ComplianceAnalysisResponse)
def get_compliance_analysis(regulation_id: int):
    try:
        query = """
            SELECT
                id,
                regulation_id,
                analysis_json,
                TRY_CAST(created_at AS DATETIME) AS created_at,
                TRY_CAST(updated_at AS DATETIME) AS updated_at
            FROM compliance_analysis
            WHERE regulation_id = ?
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, regulation_id)
            row = cursor.fetchone()

            if not row:
                raise HTTPException(
                    status_code=404,
                    detail=f"Compliance analysis not found for regulation ID {regulation_id}"
                )

            columns = [col[0] for col in cursor.description]
            analysis_dict = row_to_dict(row, columns)

            # Parse analysis_json if it is JSON
            if analysis_dict.get("analysis_json"):
                try:
                    analysis_dict["analysis_json"] = json.loads(analysis_dict["analysis_json"])
                except:
                    pass

            return {
                "success": True,
                "data": analysis_dict
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching compliance analysis for regulation {regulation_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch compliance analysis: {str(e)}"
        )


    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching compliance analysis for regulation {regulation_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch compliance analysis: {str(e)}"
        )


# ================= GET ALL COMPLIANCE CATEGORIES =================
@app.get("/categories")
def get_categories():
    """
    Get all compliance categories with hierarchy.

    **Returns:**
    - All categories and hierarchical structure
    """
    try:
        query = """
            SELECT TOP 50
    compliancecategory_id,
    title,
    parentid
FROM compliancecategory
ORDER BY parentid, title
        """

        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            categories = [row_to_dict(row, columns) for row in rows]

            # Organize into hierarchy
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

            return {
                'success': True,
                'data': {
                    'all_categories': categories,
                    'hierarchy': root_categories,
                    'total_count': len(categories)
                }
            }

    except Exception as e:
        logger.exception("Error fetching categories")
        raise HTTPException(status_code=500, detail=f"Failed to fetch categories: {str(e)}")


# ================= HEALTH CHECK =================
@app.get("/health")
def health_check():
    """Health check endpoint to verify API and database connectivity"""
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()

        return {
            'success': True,
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.exception("Health check failed")
        return {
            'success': False,
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }


# ================= ROOT ENDPOINT =================
@app.get("/")
def root():
    """API root endpoint with available endpoints"""
    return {
        "message": "Regulatory Pipeline API",
        "version": "2.0.0",
        "endpoints": {
            "pipelines": {
                "trigger_specific": "POST /trigger/{regulator}",
                "trigger_all": "POST /trigger/full",
                "update_schedule": "POST /update-schedule"
            },
            "data": {
                "regulations_by_regulator": "GET /regulations/{regulator}",
                "compliance_analysis": "GET /compliance-analysis/{regulation_id}",
                "categories": "GET /categories",
                "statistics": "GET /statistics"
            },
            "health": "GET /health"
        },
        "available_regulators": list(REGULATOR_PIPELINES.keys())
    }