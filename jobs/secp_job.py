import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


from crawler.secp_crawler import SECPCrawler
from orchestrator.orchestrator import Orchestrator
from storage.mssql_repo import MSSQLRepository  # CHANGED
from processor.downloader import Downloader
from processor.html_fallback_engine import HTMLFallbackEngine


def run_secp_job():
    conn_params = {
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}")
    }

    repo = MSSQLRepository(conn_params)
    crawler = SECPCrawler(api_key=os.getenv("CAPTCHA_API_KEY"))
    downloader = Downloader()
    ocr_engine = HTMLFallbackEngine()

    orchestrator = Orchestrator(
        crawler=crawler,
        repo=repo,
        downloader=downloader,
        #ocr_engine=ocr_engine
        ocr_engine=None
    )

    orchestrator.run_for_regulator("SECP")


if __name__ == "__main__":
    try:
        run_secp_job()
    except Exception as e:
        import traceback
        traceback.print_exc()
        exit(2)