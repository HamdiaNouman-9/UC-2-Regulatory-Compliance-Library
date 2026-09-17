"""
Standalone SAMA Circulars crawler -- run this file directly.

Not part of the DB/orchestrator pipeline (jobs/sama_job.py). It only crawls
https://rulebook.sama.gov.sa/en/sama-circulars using
crawler.sama_circulars_crawler.SAMARulebookCrawler (Selenium) and writes the
results to output/standalone_crawler/sama_circulars_only/, so they can be
handed off / merged in by hand.

Runs headful (visible browser) by default -- pass --headless to hide it.

By default it also opens a READ-ONLY connection to the regulations DB (same
.env MSSQL_* config as the rest of the pipeline) and compares against what's
already stored for regulator='SAMA', category='SAMA Circulars' -- by issue
date first, title as a tiebreaker for same-day circulars. The circulars table
lists newest first, so as soon as a row's issue date is older than the latest
known date, the rest of the table is assumed already stored and the crawl
stops there -- it does NOT walk every circular on a normal run. If the DB is
unreachable, it warns and falls back to a full crawl.

Run:
    python run_sama_circulars_headless.py
    python run_sama_circulars_headless.py --limit 20        # quick test run
    python run_sama_circulars_headless.py --headless         # no visible browser
    python run_sama_circulars_headless.py --delay 3          # slower, gentler on the site
    python run_sama_circulars_headless.py --no-db-check      # skip DB diff, fetch everything
"""
import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from crawler.sama_circulars_crawler import SAMARulebookCrawler  # noqa: E402

OUTDIR = PROJECT_ROOT / "output" / "standalone_crawler" / "sama_circulars_only"

REGULATOR = "SAMA"
CATEGORY = "SAMA Circulars"


def get_known_circulars() -> list:
    """[{"title", "published_date"}, ...] already stored for SAMA Circulars, read-only.

    Returns [] (and prints a warning) if the DB can't be reached, so the caller
    can fall back to a full crawl instead of failing outright.
    """
    try:
        import os
        import pyodbc
        from dotenv import load_dotenv

        load_dotenv()
        conn_str = (
            f"DRIVER={os.getenv('MSSQL_DRIVER')};SERVER={os.getenv('MSSQL_SERVER')};"
            f"DATABASE={os.getenv('MSSQL_DATABASE')};UID={os.getenv('MSSQL_USERNAME')};"
            f"PWD={os.getenv('MSSQL_PASSWORD')};TrustServerCertificate=yes"
        )
        conn = pyodbc.connect(conn_str, timeout=30, readonly=True)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT title, published_date FROM regulations "
            "WHERE regulator = ? AND category = ? AND published_date IS NOT NULL",
            [REGULATOR, CATEGORY],
        )
        known = [{"title": row[0], "published_date": row[1]} for row in cursor.fetchall()]
        conn.close()
        return known
    except Exception as e:
        print(f"Warning: could not read DB baseline ({e}). Falling back to a full crawl.")
        return []


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=None, help="only crawl the first N circulars (default: all)")
    ap.add_argument("--headless", action="store_true", help="run with no visible browser window (default: visible)")
    ap.add_argument("--delay", type=float, default=1.0, help="seconds to wait between each circular's detail-page request (default: 1.0)")
    ap.add_argument("--no-db-check", action="store_true", help="skip the DB diff and fetch every circular")
    args = ap.parse_args()

    OUTDIR.mkdir(parents=True, exist_ok=True)

    known_documents = None
    if not args.no_db_check:
        known_documents = get_known_circulars()
        if known_documents:
            print(f"DB baseline: {len(known_documents)} SAMA circulars already stored -- will only fetch new/changed ones.")
        else:
            known_documents = None  # unreachable or empty -- full crawl

    start = time.time()
    crawler = SAMARulebookCrawler(headless=args.headless, request_delay=args.delay)
    documents = crawler.fetch_documents(limit=args.limit, known_documents=known_documents)
    elapsed = time.time() - start

    json_path = OUTDIR / "sama_circulars.json"
    crawler.save_to_json(documents, filename=str(json_path))

    try:
        import pandas as pd
        from dataclasses import asdict

        df = pd.DataFrame([asdict(doc) for doc in documents])
        df.to_csv(OUTDIR / "sama_circulars.csv", index=False, encoding="utf-8-sig")

        xlsx_path = OUTDIR / "SAMA_Circulars.xlsx"
        for i in range(3):
            try:
                with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
                    df.to_excel(xw, sheet_name="documents", index=False)
                break
            except PermissionError:
                xlsx_path = OUTDIR / f"SAMA_Circulars_{i + 1}.xlsx"
    except ImportError:
        print("pandas/openpyxl not installed -- skipped CSV/XLSX export, JSON only.")

    mins, secs = divmod(int(elapsed), 60)
    print(f"\nDone: {len(documents)} circulars written to {OUTDIR} (took {mins}m {secs}s)")


if __name__ == "__main__":
    main()
