import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from crawler.sama_crawler_wrapper import SAMACombinedCrawler
from orchestrator.orchestrator import Orchestrator
from storage.mssql_repo import MSSQLRepository
from processor.downloader import Downloader


def run_sama_job():
    conn_params = {
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}")
    }

    repo = MSSQLRepository(conn_params)
    crawler = SAMACombinedCrawler(headless=True)
    downloader = Downloader()

    orchestrator = Orchestrator(
        crawler=crawler,
        repo=repo,
        downloader=downloader,
        # ocr_engine and llm_analyzer are optional and handled internally
        analyse=True
    )

    orchestrator.run_for_regulator("SAMA")


if __name__ == "__main__":
    try:
        run_sama_job()
    except Exception:
        import traceback
        traceback.print_exc()
        exit(2)
