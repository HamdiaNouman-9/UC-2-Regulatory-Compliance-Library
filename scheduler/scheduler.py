import sys
import asyncio

# Force Windows to use SelectorEventLoop (required for Playwright / asyncio subprocesses)
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from twisted.internet import asyncioreactor
asyncioreactor.install(asyncio.new_event_loop())

# Setup Crochet to allow Scrapy to run in sync code
from crochet import setup
setup()

import time
import logging
import os
import yaml
from contextlib import contextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

from orchestrator.orchestrator import Orchestrator
from crawler.sbp_crawler_wrapper import SBPCrawler
from crawler.secp_crawler import SECPCrawler
from processor.downloader import Downloader
from processor.html_fallback_engine import HTMLFallbackEngine
from storage.mssql_repo import MSSQLRepository
import scrapy_runtime
import subprocess
import sys
from pathlib import Path


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

TIMEZONE = os.getenv("TIMEZONE", "Asia/Karachi")

def build_orchestrator(crawler):
    repo = MSSQLRepository({
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER")
    })

    return Orchestrator(
        crawler=crawler,
        repo=repo,
        downloader=Downloader(),
        ocr_engine=HTMLFallbackEngine()
    )

def trigger_full_pipeline_via_api():
    """
    Trigger SBP → SECP pipeline via API
    """
    api_url = os.getenv("PIPELINE_API_URL", "http://localhost:8000/trigger/full")
    logger.info(f"Triggering full pipeline via API: {api_url}")

    response = requests.post(api_url, timeout=10)

    if response.status_code != 200:
        raise RuntimeError(
            f"Pipeline API failed: {response.status_code} {response.text}"
        )


# PIPELINES
def run_sbp_pipeline():
    logger.info("Starting SBP pipeline")
    orchestrator = build_orchestrator(
        #SBPCrawler(api_key=os.getenv("CAPTCHA_API_KEY"))
        SBPCrawler()
    )
    orchestrator.run_for_regulator("SBP")
    logger.info("SBP pipeline completed")


def run_secp_pipeline():
    logger.info("Starting SECP pipeline (isolated process)")
    script_path = os.path.join(os.path.dirname(__file__), "..", "jobs", "secp_job.py")
    script_path = os.path.abspath(script_path)
    project_root = os.path.dirname(os.path.dirname(script_path))
    env = os.environ.copy()
    env["PYTHONPATH"] = project_root
    subprocess.run([sys.executable, script_path], check=True, env=env)

    logger.info("SECP pipeline completed")


# LOAD scheduler.yml
def load_scheduler_config():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "scheduler.yml")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# MAIN SCHEDULER
if __name__ == "__main__":
    scheduler = BackgroundScheduler(timezone=TIMEZONE)

    config = load_scheduler_config()
    jobs = config.get("jobs", {})

    for job_name, job_cfg in jobs.items():
        if not job_cfg.get("enabled", False):
            continue

        trigger = job_cfg.get("trigger")
        schedule = job_cfg.get("schedule", {})

        if job_name == "full_pipeline":
            job_func = trigger_full_pipeline_via_api
            job_id = "full_pipeline_job"
        else:
            continue

        scheduler.add_job(
            job_func,
            trigger=trigger,
            id=job_id,
            max_instances=1,
            replace_existing=True,
            misfire_grace_time=6 * 60 * 60,  # 6 HOURS
            coalesce=False,
            **schedule
        )

        logger.info(f"Loaded job: {job_name.upper()} → {schedule}")

    scheduler.start()
    logger.info("Scheduler started (YAML config)")

    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler stopped")

    import requests



