import logging
from fastapi import FastAPI
from datetime import datetime

from scheduler.scheduler import run_sbp_pipeline, run_secp_pipeline

app = FastAPI(
    title="Regulatory Pipeline API",
    version="1.0.0"
)

logger = logging.getLogger(__name__)


@app.post("/trigger/full")
def trigger_full_pipeline():
    logger.info("Starting FULL pipeline via API")

    run_sbp_pipeline()
    run_secp_pipeline()

    logger.info("FULL pipeline completed")

    return {
        "status": "done",
        "pipeline": "SBP → SECP",
        "completed_at": datetime.utcnow().isoformat()
    }
