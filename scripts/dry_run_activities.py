"""Dry-run Stage A (requirements) + Stage B (activities) on an EXISTING
regulation and print the activities -- writes NOTHING to the database.

Why this exists: POST /regulation/{id}/analyze on an already-analysed
regulation re-hashes every requirement to the same ref_key, counts it as
unchanged and skips its activities (requirement_activity_sync.py), so prompt
changes to activity_analyzer.py never become visible that way. This runs the
same text-gathering + LLM stages as
pipeline_api._run_requirement_activity_analysis_for_regulation, minus the
sync, the extra_meta OCR backfill, and LLM usage recording.

Usage (PowerShell, from the repo root):
    python scripts/dry_run_activities.py 832
Output: printed table + output/dry_run_activities/<regulation_id>.json
"""
import json
import os
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from storage.mssql_repo import MSSQLRepository
from storage import llm_settings
from orchestrator.orchestrator import Orchestrator
from processor.downloader import Downloader
from processor.requirement_analyzer import RequirementAnalyzer
from processor.activity_analyzer import ActivityAnalyzer


def gather_documents(repo, row):
    meta = row.get("extra_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    fetch = Orchestrator(crawler=None, repo=repo, downloader=Downloader())._download_and_extract_file

    documents = []
    main_text = (meta.get("org_pdf_text") or meta.get("content_text")
                 or row.get("document_html") or "").strip()
    if len(main_text) < 200 and row.get("document_url"):
        main_text = (fetch(row["document_url"]) or "").strip()
    if len(main_text) >= 200:
        documents.append({"source_document": "main_body", "text": main_text})
    for url in [u.strip() for u in str(meta.get("attachment_links") or "").split("|") if u.strip()]:
        text = fetch(url)
        if text and len(text.strip()) >= 200:
            documents.append({"source_document": url.rsplit("/", 1)[-1][:80] or url, "text": text})
    return documents


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: python scripts/dry_run_activities.py <regulation_id>")
    regulation_id = int(sys.argv[1])

    repo = MSSQLRepository({
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver":   os.getenv("MSSQL_DRIVER"),
    })
    print(f"DB: {os.getenv('MSSQL_SERVER')} / {os.getenv('MSSQL_DATABASE')}")

    row = repo.get_regulation_by_id(regulation_id)
    if not row:
        sys.exit(f"no regulations row with id={regulation_id}")
    print(f"Regulation {regulation_id}: {row.get('title')}")

    documents = gather_documents(repo, row)
    if not documents:
        sys.exit("no document produced >=200 chars of usable text")
    print(f"Documents: {[d['source_document'] for d in documents]}")

    model = llm_settings.get_model(repo)
    stage_a = RequirementAnalyzer(model=model).extract_and_classify(
        documents=documents, document_title=row.get("title") or "",
        requirement_types=repo.get_requirement_types(), regulator=row.get("regulator") or "",
        reference=row.get("reference_no") or "",
        publication_date=str(row.get("published_date") or ""))
    requirements = stage_a["requirements"]
    print(f"Requirements: {len(requirements)}")

    activities = ActivityAnalyzer(model=model).design_activities(
        requirements=requirements, chunk_texts=stage_a["chunk_texts"]) if requirements else []
    needed = [a for a in activities if a.get("activity_needed")]

    print(f"\nActivities: {len(needed)} (of {len(activities)} requirement outcomes)\n")
    for a in needed:
        print(f"[{a.get('frequency_type') or 'NULL':<12}] {a.get('title')}\n"
              f"               frequency: {a.get('frequency') or '-'}")
    print("\nfrequency_type counts:",
          dict(Counter(a.get("frequency_type") or "NULL" for a in needed)))

    out_dir = ROOT / "output" / "dry_run_activities"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{regulation_id}.json"
    out_file.write_text(json.dumps({"regulation_id": regulation_id, "requirements": requirements,
                                    "activities": activities}, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    print(f"\nSaved: {out_file}")


if __name__ == "__main__":
    main()
