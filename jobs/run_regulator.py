"""
run_regulator.py — one runner for every regulator.

Replaces the near-identical jobs/sbp_job.py, secp_job.py, sama_job.py: the only
thing that differed between them was which crawler they constructed, and that now
comes from config/sources/<regulator>.yml.

    venv/Scripts/python.exe jobs/run_regulator.py MISA --dry-run
    venv/Scripts/python.exe jobs/run_regulator.py MISA
    venv/Scripts/python.exe jobs/run_regulator.py SAMA --limit 20

--dry-run
    Crawls and prints what WOULD be inserted — document counts, folder paths, a
    sample — and touches neither the database nor the LLM. Use this to check a
    new source before it ever writes to the library. It does not even open a
    database connection.

--to-excel PATH
    Writes the RegulatoryDocument list to Excel, for db_compare.py or review.

Adding a regulator is a YAML file plus a scheduler entry. No python.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import yaml
from dotenv import load_dotenv

from crawler.generic_crawler_wrapper import build_regulator_crawler

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("run_regulator.log", encoding="utf-8"),
              logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

CONFIG_DIR = PROJECT_ROOT / "config" / "sources"


def load_config(regulator: str) -> dict:
    path = CONFIG_DIR / f"{regulator.lower()}.yml"
    if not path.exists():
        known = sorted(p.stem.upper() for p in CONFIG_DIR.glob("*.yml"))
        raise SystemExit(f"No config at {path}. Known regulators: "
                         f"{', '.join(known) or '(none)'}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def summarise(docs, limit=8):
    from collections import Counter
    print(f"\n{'='*78}\n{len(docs)} documents\n{'='*78}")
    if not docs:
        return
    print("by file type :", dict(Counter(d.file_type for d in docs)))
    print("by kind      :", dict(Counter(
        (d.extra_meta or {}).get("record_kind") for d in docs)))
    paths = Counter(" > ".join(d.doc_path or []) for d in docs)
    print(f"\nfolder paths ({len(paths)} distinct):")
    for p, n in paths.most_common(12):
        print(f"   {n:>4}  {p}")
    print(f"\nfirst {limit}:")
    for d in docs[:limit]:
        print(f"   {(d.title or '')[:62]:<62} {d.file_type}")
        print(f"       {d.document_url[:96]}")
    # Anything the pipeline would reject outright is worth seeing now.
    bad = [d for d in docs if not d.title or not d.document_url or not d.doc_path]
    if bad:
        print(f"\n!! {len(bad)} documents are missing a title, url or doc_path")


def to_excel(docs, path):
    import pandas as pd
    rows = [{
        "title": d.title,
        "doc_url": d.document_url,
        "type": d.file_type,
        "section_path": " > ".join(d.doc_path or []),
        "category": d.category,
        "source_page_url": d.source_page_url,
        "content_hash": d.content_hash,
        "record_kind": (d.extra_meta or {}).get("record_kind", ""),
        "text_len": len((d.extra_meta or {}).get("content_text") or ""),
    } for d in docs]
    pd.DataFrame(rows).to_excel(path, index=False)
    print(f"\nWrote {path}")


def build_orchestrator(crawler):
    from orchestrator.orchestrator import Orchestrator
    from storage.mssql_repo import MSSQLRepository
    from processor.downloader import Downloader

    repo = MSSQLRepository({
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    })
    return Orchestrator(crawler=crawler, repo=repo,
                        downloader=Downloader(), ocr_engine=None)


def main():
    ap = argparse.ArgumentParser(description="Run one regulator's pipeline")
    ap.add_argument("regulator", help="e.g. MISA, SAMA (matches config/sources/<name>.yml)")
    ap.add_argument("--dry-run", action="store_true",
                    help="crawl and report only — no database, no LLM")
    ap.add_argument("--to-excel", default="", help="write the documents to an .xlsx")
    ap.add_argument("--limit", type=int, default=0, help="cap documents (testing)")
    args = ap.parse_args()

    cfg = load_config(args.regulator)
    regulator = cfg.get("regulator", args.regulator)
    crawler = build_regulator_crawler(cfg)

    logger.info("=" * 70)
    logger.info("%s — %d source(s)%s", regulator, len(cfg.get("sources") or []),
                "  [DRY RUN]" if args.dry_run else "")
    for s in cfg.get("sources") or []:
        logger.info("   %-10s %s", s.get("mode", "generic"), s.get("name", "?"))
    logger.info("=" * 70)

    if args.dry_run:
        docs = crawler.fetch_documents(limit=args.limit or None)
        summarise(docs)
        if args.to_excel:
            to_excel(docs, args.to_excel)
        print("\nDRY RUN — nothing was written to the database.")
        return 0

    # Not a dry run: the orchestrator calls fetch_documents() itself.
    orch = build_orchestrator(crawler)
    orch.run_for_regulator(regulator)
    logger.info("%s pipeline finished", regulator)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(2)
