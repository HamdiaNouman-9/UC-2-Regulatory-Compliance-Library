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
import requests
import subprocess
import sys
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

from orchestrator.orchestrator import Orchestrator
from crawler.sbp_crawler_wrapper import SBPCrawler
from crawler.secp_crawler import SECPCrawler
from processor.downloader import Downloader
from processor.html_fallback_engine import HTMLFallbackEngine
from storage.mssql_repo import MSSQLRepository

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

TIMEZONE = os.getenv("TIMEZONE", "Asia/Karachi")


def build_orchestrator(crawler):
    """Build orchestrator with database connection"""
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
        ocr_engine=HTMLFallbackEngine(),
        analyse=True
    )


# ==============================================================
# OPTION 1: DIRECT EXECUTION (Run pipeline directly in scheduler)
# ==============================================================

def run_sbp_pipeline():
    """Run SBP pipeline directly"""
    logger.info("=" * 60)
    logger.info("Starting SBP pipeline (DIRECT)")
    logger.info("=" * 60)
    try:
        orchestrator = build_orchestrator(SBPCrawler())
        orchestrator.run_for_regulator("SBP")
        logger.info("SBP pipeline completed successfully")
    except Exception as e:
        logger.error(f"SBP pipeline failed: {e}", exc_info=True)


def run_secp_pipeline():
    """Run SECP pipeline in isolated process"""
    logger.info("=" * 60)
    logger.info("Starting SECP pipeline (ISOLATED PROCESS)")
    logger.info("=" * 60)
    try:
        script_path = os.path.join(os.path.dirname(__file__), "..", "jobs", "secp_job.py")
        script_path = os.path.abspath(script_path)
        project_root = os.path.dirname(os.path.dirname(script_path))
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root
        subprocess.run([sys.executable, script_path], check=True, env=env)
        logger.info("SECP pipeline completed successfully")
    except Exception as e:
        logger.error(f"SECP pipeline failed: {e}", exc_info=True)


def run_sama_pipeline():
    """Run SAMA pipeline in isolated process"""
    logger.info("=" * 60)
    logger.info("Starting SAMA pipeline (ISOLATED PROCESS)")
    logger.info("=" * 60)
    try:
        script_path = os.path.join(os.path.dirname(__file__), "..", "jobs", "sama_job.py")
        script_path = os.path.abspath(script_path)
        project_root = os.path.dirname(os.path.dirname(script_path))
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root
        subprocess.run([sys.executable, script_path], check=True, env=env)
        logger.info("SAMA pipeline completed successfully")
    except Exception as e:
        logger.error(f"SAMA pipeline failed: {e}", exc_info=True)

# ==============================================================
# OPTION 2: API-BASED EXECUTION (Trigger via API)
# ==============================================================

def trigger_via_api(regulator: str):
    """
    Trigger a specific regulator pipeline via API.

    Args:
        regulator: Name of regulator (SBP, SECP, SAMA)
    """
    api_base_url = os.getenv("PIPELINE_API_URL", "http://localhost:8000")
    api_url = f"{api_base_url}/trigger/{regulator}"

    logger.info(f"Triggering {regulator} pipeline via API: {api_url}")

    try:
        response = requests.post(api_url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            logger.info(f"{regulator} pipeline triggered successfully")
            logger.info(f"Response: {data}")
        else:
            logger.error(
                f"Failed to trigger {regulator} pipeline. "
                f"Status: {response.status_code}, Response: {response.text}"
            )
            raise RuntimeError(f"API returned status {response.status_code}")

    except requests.exceptions.ConnectionError:
        logger.error(f"Cannot connect to API at {api_url}. Is the API server running?")
        raise
    except Exception as e:
        logger.error(f"Error triggering {regulator} pipeline: {e}", exc_info=True)
        raise


def trigger_sbp_via_api():
    """Trigger SBP via API"""
    trigger_via_api("SBP")


def trigger_secp_via_api():
    """Trigger SECP via API"""
    trigger_via_api("SECP")


def trigger_sama_via_api():
    """Trigger SAMA via API"""
    trigger_via_api("SAMA")


def trigger_full_pipeline_via_api():
    """Trigger full pipeline (all regulators) via API"""
    api_base_url = os.getenv("PIPELINE_API_URL", "http://localhost:8000")
    api_url = f"{api_base_url}/trigger/full"

    logger.info(f"Triggering FULL pipeline via API: {api_url}")

    try:
        response = requests.post(api_url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            logger.info("Full pipeline triggered successfully")
            logger.info(f"Response: {data}")
        else:
            logger.error(
                f"Failed to trigger full pipeline. "
                f"Status: {response.status_code}, Response: {response.text}"
            )
            raise RuntimeError(f"API returned status {response.status_code}")

    except Exception as e:
        logger.error(f"Error triggering full pipeline: {e}", exc_info=True)
        raise

def trigger_monitor_via_api(job: str):
    """Start a monitor_* job through the API and return once it has STARTED.

    NOT once it has finished. `trigger_via_api` above posts with timeout=10 and
    a monitoring run takes tens of minutes (CBE measured ~40), so an endpoint
    that ran the job synchronously would hand this client a ReadTimeout on every
    SUCCESSFUL run. /trigger/monitor/{job} answers 202 as soon as the thread is
    up, so the only thing this call can report — and does report — is whether
    the job started.

    The outcome lives in `run_history` in the database, which is what the
    completeness gate reads. Do not infer success from this returning cleanly.
    """
    api_base_url = os.getenv("PIPELINE_API_URL", "http://localhost:8000")
    api_url = f"{api_base_url}/trigger/monitor/{job}"
    logger.info(f"Starting monitoring job via API: {api_url}")
    try:
        response = requests.post(api_url, timeout=30)
        if response.status_code not in (200, 202):
            logger.error(f"Failed to start {job}. Status: "
                         f"{response.status_code}, Response: {response.text}")
            raise RuntimeError(f"API returned status {response.status_code}")
        data = response.json()
        # `already_running` is the exclusive lock doing its job, not a failure.
        logger.info(f"{job}: {data.get('state')} "
                    f"(started_at={data.get('started_at')})")
    except requests.exceptions.ConnectionError:
        logger.error(f"Cannot connect to API at {api_url}. Is the API running?")
        raise
    except Exception as e:
        logger.error(f"Error starting {job}: {e}", exc_info=True)
        raise



# ==============================================================
# STALENESS CHECK
# ==============================================================

def _log_staleness(result: dict):
    """Stale regulators go to the log at ERROR, one line each, so they stand out
    in scheduler.log. Nothing is sent anywhere yet -- see GET /monitoring/staleness."""
    stale = result.get("stale", [])
    if not stale:
        logger.info("Staleness check: every regulator is within its interval")
        return
    logger.error("Staleness check: %d regulator(s) have had no update within "
                 "their interval", len(stale))
    for r in stale:
        logger.error("  STALE %s: last update %s (%s days ago, allowed %s)",
                     r["regulator"], r["last_update"] or "never",
                     r["days_since"], r["limit_days"])


def run_staleness_check():
    """DIRECT mode: run the check in this process."""
    from jobs.staleness_alert import check_staleness
    repo = MSSQLRepository({
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER")
    })
    _log_staleness(check_staleness(repo))


def trigger_staleness_via_api():
    """API mode: call GET /monitoring/staleness."""
    api_base_url = os.getenv("PIPELINE_API_URL", "http://localhost:8000")
    response = requests.get(f"{api_base_url}/monitoring/staleness", timeout=60)
    if response.status_code != 200:
        raise RuntimeError(f"API returned status {response.status_code}")
    _log_staleness(response.json())


# ==============================================================
# CONFIGURATION LOADER
# ==============================================================

def load_scheduler_config():
    """Load scheduler configuration from YAML"""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "scheduler.yml")

    logger.info(f"Loading scheduler config from: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ==============================================================
# JOB MAPPING
# ==============================================================

# Map job names to functions
# Choose either DIRECT or API execution mode

# The KSA monitoring jobs. Imported lazily inside the mapping build so a broken
# import here cannot stop the existing SBP/SECP jobs from being scheduled.
from jobs.monitor_jobs import (monitor_bahrain_bourse, monitor_cbb,  # noqa: E402
                               monitor_cbe, monitor_cbi, monitor_cbj,
                               monitor_cheap_probes,
                               monitor_cma, monitor_edb, monitor_lloc,
                               monitor_lmra, monitor_mc, monitor_mlcu,
                               monitor_mlsd, monitor_moic, monitor_justice_canada,
                               monitor_nbr, monitor_pdpa, monitor_rera,
                               monitor_sama, monitor_saudi_exchange,
                               monitor_sio, monitor_simah)

DIRECT_JOB_MAPPING = {
    "staleness_check": run_staleness_check,
    "sbp_pipeline": run_sbp_pipeline,
    "secp_pipeline": run_secp_pipeline,
    "sama_pipeline": run_sama_pipeline,

    # ---- KSA monitoring -------------------------------------------------- #
    # Grouped by what each site will answer, not by regulator. See
    # jobs/monitor_jobs.py for the measurement behind each grouping.
    #
    # Saudi Exchange and SIMAH HAVE NO JOB, deliberately: both are blocked, and
    # both blocks were caused by automated access from this address. A scheduled
    # retry is not a way out of them — it is what made them. They are retested
    # BY HAND after the review dates in config/change_signals.yml.
    "monitor_cheap_probes": monitor_cheap_probes,
    "monitor_sama": monitor_sama,
    "monitor_mc": monitor_mc,
    "monitor_cma": monitor_cma,
    "monitor_mlcu": monitor_mlcu,
    "monitor_cbe": monitor_cbe,
    "monitor_bahrain_bourse": monitor_bahrain_bourse,
    # Replaces the retired `cbb_monitoring` above, which wrote straight to
    # MSSQL with no workbook step. See config/scheduler.yml for why it is off.
    "monitor_cbb": monitor_cbb,

    # ---- Bahrain, onboarded by abeeraslam, merged 2026-08-26 ------------- #
    # All three ship DISABLED in config/scheduler.yml: a new regulator goes to a
    # workbook for a person to read first, and this path writes straight to
    # MSSQL, so enabling one before the review makes the first scheduled run the
    # ingest.
    "monitor_rera": monitor_rera,
    "monitor_sio": monitor_sio,
    "monitor_lloc": monitor_lloc,

    # ---- Jordan, onboarded 2026-09-16 ------------------------------------ #
    # Ships DISABLED in config/scheduler.yml: CBJ's workbook has been exported
    # and checked but not yet read or promoted, and this path writes straight to
    # MSSQL. See config/change_signals.yml for why the crawl is its signal.
    "monitor_cbi": monitor_cbi,
    "monitor_cbj": monitor_cbj,
    # ---- Bahrain, onboarded on feature/crawler-dev-fakih, merged 2026-08-28 ---- #
    # Same rule as the three above: all three ship DISABLED in
    # config/scheduler.yml until a person has read their workbook.
    "monitor_edb": monitor_edb,
    "monitor_mlsd": monitor_mlsd,
    "monitor_lmra": monitor_lmra,
    "monitor_nbr": monitor_nbr,
    # Canada's first. Same rule: DISABLED in config/scheduler.yml until a person
    # has read its workbook. Its signal is a cheap probe, so once it is trusted
    # the better home is CHEAP_PROBE_SOURCES and this entry goes away.
    "monitor_justice_canada": monitor_justice_canada,

    # monitor_moic and monitor_pdpa existed in jobs/monitor_jobs.py but were
    # never added here or to config/scheduler.yml -- unreachable by cron OR by
    # API_JOB_MAPPING below, found during the 2026-09-17 regulator-reachability
    # audit. Same rule as every other new regulator: DISABLED until a person
    # has read their workbook.
    "monitor_moic": monitor_moic,
    "monitor_pdpa": monitor_pdpa,

    # SIMAH and Saudi Exchange, added 2026-09-21. They now HAVE jobs because the
    # jobs read a saved page and cannot iterate against the site -- see
    # jobs/monitor_jobs.py. Blocked hosts are still never retried by a machine.
    "monitor_simah": monitor_simah,
    "monitor_saudi_exchange": monitor_saudi_exchange,
}

API_JOB_MAPPING = {
    "staleness_check": trigger_staleness_via_api,
    "sbp_pipeline": trigger_sbp_via_api,
    "secp_pipeline": trigger_secp_via_api,
    "sama_pipeline": trigger_sama_via_api,
    "full_pipeline": trigger_full_pipeline_via_api,

    # ---- monitoring, reachable in API mode as of 2026-08-20 --------------- #
    # Until now every monitor_* job existed ONLY in DIRECT_JOB_MAPPING while
    # EXECUTION_MODE defaulted to "API", so `enabled: true` on any of them
    # logged "No function mapped for job" and monitored nothing — for every
    # regulator, silently. Both mappings now carry the same set, so the
    # execution mode changes HOW a job is reached and never WHETHER it exists.
    #
    # Keep these two dicts in step. A job in one and not the other is invisible
    # in exactly one mode, which is the failure this pair of lines fixes.
    "monitor_cheap_probes": lambda: trigger_monitor_via_api("monitor_cheap_probes"),
    "monitor_sama": lambda: trigger_monitor_via_api("monitor_sama"),
    "monitor_mc": lambda: trigger_monitor_via_api("monitor_mc"),
    "monitor_cma": lambda: trigger_monitor_via_api("monitor_cma"),
    "monitor_mlcu": lambda: trigger_monitor_via_api("monitor_mlcu"),
    "monitor_cbe": lambda: trigger_monitor_via_api("monitor_cbe"),
    "monitor_cbb": lambda: trigger_monitor_via_api("monitor_cbb"),
    "monitor_bahrain_bourse": lambda: trigger_monitor_via_api("monitor_bahrain_bourse"),
    "monitor_rera": lambda: trigger_monitor_via_api("monitor_rera"),
    "monitor_sio": lambda: trigger_monitor_via_api("monitor_sio"),
    "monitor_lloc": lambda: trigger_monitor_via_api("monitor_lloc"),
    "monitor_cbi": lambda: trigger_monitor_via_api("monitor_cbi"),
    "monitor_cbj": lambda: trigger_monitor_via_api("monitor_cbj"),
    "monitor_edb": lambda: trigger_monitor_via_api("monitor_edb"),
    "monitor_mlsd": lambda: trigger_monitor_via_api("monitor_mlsd"),
    "monitor_lmra": lambda: trigger_monitor_via_api("monitor_lmra"),
    "monitor_nbr": lambda: trigger_monitor_via_api("monitor_nbr"),
    "monitor_justice_canada": lambda: trigger_monitor_via_api("monitor_justice_canada"),
    "monitor_moic": lambda: trigger_monitor_via_api("monitor_moic"),
    "monitor_pdpa": lambda: trigger_monitor_via_api("monitor_pdpa"),
    "monitor_simah": lambda: trigger_monitor_via_api("monitor_simah"),
    "monitor_saudi_exchange": lambda: trigger_monitor_via_api("monitor_saudi_exchange"),
}

# Choose which mode to use (set via environment variable or hardcode)
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "API")  # Options: "DIRECT" or "API"

if EXECUTION_MODE == "API":
    JOB_MAPPING = API_JOB_MAPPING
    logger.info("Scheduler configured for API-based execution")
else:
    JOB_MAPPING = DIRECT_JOB_MAPPING
    logger.info("Scheduler configured for direct execution")

# ==============================================================
# MAIN SCHEDULER
# ==============================================================

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("REGULATORY PIPELINE SCHEDULER STARTING")
    logger.info(f"Execution Mode: {EXECUTION_MODE}")
    logger.info(f"Timezone: {TIMEZONE}")
    logger.info("=" * 60)

    # Create scheduler
    scheduler = BackgroundScheduler(timezone=TIMEZONE)

    # Load configuration
    config = load_scheduler_config()
    jobs = config.get("jobs", {})

    # Add jobs to scheduler
    jobs_added = 0
    for job_name, job_cfg in jobs.items():
        if not job_cfg.get("enabled", False):
            logger.info(f"Skipping disabled job: {job_name}")
            continue

        # Get the job function
        job_func = JOB_MAPPING.get(job_name)
        if not job_func:
            logger.warning(f"No function mapped for job: {job_name}")
            continue

        trigger = job_cfg.get("trigger")
        schedule = job_cfg.get("schedule", {})

        # Add job to scheduler
        scheduler.add_job(
            job_func,
            trigger=trigger,
            id=f"{job_name}_job",
            name=job_name.upper(),
            max_instances=1,
            replace_existing=True,
            misfire_grace_time=6 * 60 * 60,  # 6 hours
            coalesce=False,
            **schedule
        )

        jobs_added += 1
        logger.info(f"Loaded job: {job_name.upper()}")
        logger.info(f"  Trigger: {trigger}")
        logger.info(f"  Schedule: {schedule}")

    if jobs_added == 0:
        logger.warning("No jobs were added to the scheduler!")
        sys.exit(1)

    # Start scheduler
    scheduler.start()
    logger.info("=" * 60)
    logger.info(f"Scheduler started with {jobs_added} job(s)")
    logger.info("=" * 60)

    # Print scheduled jobs
    logger.info("\nScheduled Jobs:")
    for job in scheduler.get_jobs():
        next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job.next_run_time else "N/A"
        logger.info(f"  - {job.name}: Next run at {next_run}")

    logger.info("\nScheduler is running. Press Ctrl+C to stop.")

    # Keep running
    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        logger.info("\nShutting down scheduler...")
        scheduler.shutdown()
        logger.info("Scheduler stopped")

