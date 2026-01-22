import asyncio
import sys

# Force asyncio event loop policy on Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from twisted.internet import asyncioreactor
asyncioreactor.install(asyncio.new_event_loop())

from crochet import setup
setup()

import os
from crawler.sbp_crawler_wrapper import SBPCrawler
from orchestrator.orchestrator import Orchestrator
from storage.mssql_repo import MSSQLRepository  # CHANGED
from processor.downloader import Downloader
from processor.html_fallback_engine import HTMLFallbackEngine


def run_sbp_job():
    conn_params = {
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}")
    }

    repo = MSSQLRepository(conn_params)  # CHANGED
    crawler = SBPCrawler()
    downloader = Downloader()
    ocr_engine = HTMLFallbackEngine()

    orchestrator = Orchestrator(
        crawler=crawler,
        repo=repo,
        downloader=downloader,
        #ocr_engine=ocr_engine
        ocr_engine=None
    )

    orchestrator.run_for_regulator("SBP")

if __name__ == "__main__":
    run_sbp_job()