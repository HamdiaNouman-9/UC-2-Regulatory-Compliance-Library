"""A separate API for the new orchestrator, writing to Excel instead of MSSQL.

Deliberately NOT added to `apis/pipeline_api.py`: that one is wired to production
MSSQL and to the existing per-regulator pipelines. This is its own app on its own
port so a run here cannot touch the database.

START IT

    venv/Scripts/python.exe -m uvicorn dynamic_crawler.formfill.api:app --port 8100

TRIGGER IT

    Browser:  http://127.0.0.1:8100/docs          (Swagger — click Execute)
              http://127.0.0.1:8100/forms         (what is available)

    curl:     curl -X POST "http://127.0.0.1:8100/trigger/sama.circulars?limit=5"

WHAT A RUN DOES

    crawl (or reuse the last crawl)  ->  RegulatoryDocument
      -> classify: new / modified / unchanged / disappeared
      -> completeness gate
      -> folder tree + regulations + regulation_versions rows
      -> the text decision (gate, then html vs file, both when they differ)
      -> optional LLM analysis
      -> one .xlsx with a sheet per table

    Nothing is written to MSSQL. Run it twice against the same workbook and the
    second run reports `unchanged` for everything — that is the change detection
    working.

TWO SAFETY DEFAULTS

    reuse_last = true   Uses the crawl output already on disk. Set false to
                        re-crawl (SAMA circulars takes ~45 minutes).
    analyse    = false  No LLM calls, no spend. Set true and each document costs
                        roughly $0.007 and ~4 minutes.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import yaml
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("formfill.api")

REPO_ROOT = Path(__file__).resolve().parents[2]
HINTS_DIR = REPO_ROOT / "dynamic_crawler" / "hints"
SOURCES_DIR = REPO_ROOT / "config" / "sources"
OUT_DIR = REPO_ROOT / "output" / "formfill" / "_orch_runs"

app = FastAPI(
    title="Formfill orchestrator (Excel-backed)",
    description=__doc__,
    version="1.0",
)

# Same permissive policy as apis/pipeline_api.py -- this is a local dev tool
# that writes no further than an Excel file, so an open CORS policy carries
# none of the risk it would on a database-backed API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_runs: Dict[str, dict] = {}
_lock = threading.Lock()


def _forms() -> Dict[str, dict]:
    out = {}
    for f in sorted(HINTS_DIR.glob("*.yml")):
        h = yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        lib = h.get("library") or {}
        meta = h.get("meta") or {}
        out[h.get("name", f.stem)] = {
            "path": str(f.relative_to(REPO_ROOT)),
            "regulator": lib.get("regulator"),
            "source_system": lib.get("source_system"),
            "shape": h.get("shape"),
            "approved": bool(meta.get("approved")),
            "fetch_details": h.get("fetch_details", True),
        }
    return out


def _sources() -> Dict[str, dict]:
    """The GENERIC-crawler regulators, from config/sources/*.yml.

    A regulator here is a LIST of sources, each independently `generic` (zero
    config, the link walker) or `custom` (a hand-written crawler class), and the
    orchestrator treats them identically. That is the hybrid: SAMA keeps its
    tuned circulars crawler while its rulebook sectors ride the generic engine,
    in one file, with no python to change.
    """
    out = {}
    for f in sorted(SOURCES_DIR.glob("*.yml")):
        try:
            cfg = yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        except Exception as e:
            out[f.stem.upper()] = {"path": str(f.relative_to(REPO_ROOT)),
                                   "error": f"unreadable: {e}"}
            continue
        srcs = cfg.get("sources") or []
        entry = {
            "path": str(f.relative_to(REPO_ROOT)),
            "owner": cfg.get("owner"),
            "n_sources": len(srcs),
            "sources": [{"name": s.get("name"), "mode": s.get("mode", "generic"),
                         "seed_url": s.get("seed_url")} for s in srcs],
        }
        # Say why a regulator is off. Without this the listing shows a zero next
        # to the working configs, which reads as one that finds nothing.
        if cfg.get("disabled"):
            entry["disabled"] = str(cfg["disabled"])
        out[cfg.get("regulator", f.stem.upper())] = entry
    return out


@app.get("/", tags=["info"])
def index():
    return {
        "app": "new orchestrator, Excel-backed",
        "docs": "/docs",
        "forms": "/forms",
        "sources": "/sources",
        "trigger_form": "POST /trigger/{form}?limit=5&analyse=false&reuse_last=true",
        "trigger_source": "POST /trigger/source/{regulator}?limit=5&analyse=false",
        "runs": "/runs",
        "database": "NONE — output is an .xlsx per run",
    }


@app.get("/forms", tags=["info"])
def list_forms():
    return _forms()


@app.get("/sources", tags=["info"])
def list_sources():
    return _sources()


@app.get("/runs", tags=["info"])
def list_runs():
    return _runs


@app.get("/runs/{run_id}/excel", tags=["info"])
def download_excel(run_id: str):
    r = _runs.get(run_id)
    if not r or not r.get("excel"):
        raise HTTPException(404, "no such run, or it produced no workbook")
    return FileResponse(r["excel"], filename=Path(r["excel"]).name)


def _seed_repo_from_production_db(repo, regulator: str) -> int:
    """READ-ONLY baseline: copy this regulator's rows as they exist RIGHT NOW
    in the real `regulations` table into the (otherwise empty) ExcelRepo, so
    change detection compares against the actual library instead of an empty
    workbook. Without this, every run of a regulator that's already fully
    onboarded (MISA, etc.) reports its entire inventory as "new" every single
    time -- the workbook has no memory of the database ever having seen them.

    SELECT only -- this never writes to MSSQL. The run itself still only
    ever writes to the local Excel workbook, same guarantee as before.
    """
    from dynamic_crawler.formfill.excel_repo import _flat
    from dynamic_crawler.formfill.promote import _build_repo

    prod = _build_repo()
    with prod._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, ref_key, regulator, source_system, category, title,
                   document_url, doc_path, published_date, reference_no,
                   department, [year], source_page_url, extra_meta,
                   content_hash, compliancecategory_id
            FROM regulations WHERE regulator = ?
        """, (regulator,))
        columns = [c[0] for c in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

    for r in rows:
        repo.t["regulations"].append({k: _flat(v) for k, v in r.items()})
    if rows:
        max_id = max((r.get("id") or 0) for r in rows)
        repo._next["regulations"] = max(repo._next["regulations"], max_id)
    return len(rows)


@app.post("/trigger/{form}", tags=["run"])
def trigger(form: str,
            limit: Optional[int] = 5,
            analyse: bool = False,
            reuse_last: bool = True,
            workbook: Optional[str] = None,
            headed: bool = False,
            check_against_db: bool = True):
    """Run the new orchestrator for one form.

    - **limit** — documents to process. `0` or omit for all. Start small.
    - **analyse** — run the 4-stage LLM analysis. Costs ~$0.007 and ~4 min each.
    - **reuse_last** — use the crawl already on disk instead of re-crawling.
    - **workbook** — append to an existing workbook to see change detection on a
      second run. Omit for a fresh one.
    - **headed** — pop a real, visible browser window on this machine for the
      crawl instead of running invisibly. Only has any effect when
      `reuse_last=false`, since reusing skips the crawl (and the browser)
      entirely.
    - **check_against_db** — default TRUE. Seed the (empty) workbook with this
      regulator's CURRENT rows from the real database (read-only) before
      classifying, so "new" means "not already in the library", not "not in
      this particular local file". Set false to see the raw empty-workbook
      behaviour (everything comes back "new", the historical default).
    """
    forms = _forms()
    if form not in forms:
        raise HTTPException(400, f"unknown form. available: {sorted(forms)}")
    info = forms[form]
    if not info["regulator"] or not info["source_system"]:
        raise HTTPException(400, f"{form} has no `library:` block — cannot name "
                                 "the regulator or source_system")

    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    from orchestrator.orchestrator import Orchestrator
    from dynamic_crawler.formfill.pipeline import FormfillCrawler

    started = datetime.now()
    run_id = f"{form}-{started:%Y%m%d-%H%M%S}"
    out_xlsx = OUT_DIR / (workbook or f"{run_id}.xlsx")

    with _lock:
        crawl_dir = REPO_ROOT / "output" / "formfill" / form
        reuse_from = None
        if reuse_last:
            # newest pages.json under this form's output folder
            cands = sorted(crawl_dir.glob("*/pages.json"),
                           key=lambda p: p.stat().st_mtime, reverse=True)
            if not cands:
                raise HTTPException(400, f"reuse_last=true but no crawl found under "
                                         f"{crawl_dir}. Run the crawler first or pass "
                                         f"reuse_last=false.")
            reuse_from = cands[0]

        repo = ExcelRepo(out_xlsx)
        seeded_from_db = 0
        if workbook and out_xlsx.exists():
            # A workbook from an earlier run already reflects everything this
            # regulator had at that point (itself seeded from the DB the
            # first time round) -- load THAT, not a fresh DB seed, or a
            # document this workbook already marked "modified" would lose
            # that and quietly re-classify off the DB's older content_hash.
            _load_workbook_into(repo, out_xlsx)
        elif check_against_db:
            seeded_from_db = _seed_repo_from_production_db(repo, info["regulator"])
            logger.info("seeded %d existing regulation(s) from production DB for %s",
                       seeded_from_db, info["regulator"])

        crawler = FormfillCrawler(
            str(REPO_ROOT / info["path"]),
            regulator=info["regulator"],
            source_system=info["source_system"],
            require_approved=False,          # a preview run may use a stale form
            out_dir=str(crawl_dir / "api_run") if not reuse_last else None,
            headed=headed,
        )
        if reuse_from:
            crawler._run_crawl = lambda p=reuse_from: json.loads(
                p.read_text(encoding="utf-8"))
            logger.info("reusing crawl %s", reuse_from)

        orch = Orchestrator(
            crawler=crawler, repo=repo, downloader=None,
            source_name=form, analyse=analyse,
            limit=(limit or None),
        )

        try:
            report = orch.run_for_regulator(info["regulator"])
        except Exception as e:
            logger.exception("run failed")
            raise HTTPException(500, f"run failed: {e}")

        excel = repo.save()

    report.update({
        "run_id": run_id,
        "form": form,
        "crawl_reused": str(reuse_from) if reuse_from else None,
        "seeded_from_production_db": seeded_from_db,
        "seconds": round((datetime.now() - started).total_seconds(), 1),
        "excel": str(excel),
        "excel_download": f"/runs/{run_id}/excel",
    })
    _runs[run_id] = report
    return report


@app.post("/approve/{run_id}", tags=["approve"])
def approve(run_id: str, dry_run: bool = True, confirm: bool = False,
            with_db: bool = False):
    """Put an approved run's workbook into MSSQL.

    The second half of the workflow: a run writes a workbook and touches no
    database, you read it, and this puts exactly those rows in.

    - **dry_run** — default TRUE. Reports what would be inserted and opens no
      connection. Look at this before anything else.
    - **confirm** — must ALSO be true to write. Two flags rather than one
      because this is the only call in the app that can reach production, and
      a mistyped url should not be able to.
    - **with_db** — a dry run with no connection cannot ask whether a document
      is already in the library, so `skipped_already_present` is structurally 0
      and `db_consulted` is false. Set this to READ (never write) during a dry
      run and get a real coverage number. Writes stay gated on the two flags
      above.

    Already-present documents are skipped on the identity the orchestrator
    classifies with, so promoting the same workbook twice inserts nothing the
    second time. That makes it safe to retry after a partial failure.
    """
    r = _runs.get(run_id)
    if not r or not r.get("excel"):
        raise HTTPException(404, "no such run, or it produced no workbook")
    xlsx = Path(r["excel"])
    if not xlsx.exists():
        raise HTTPException(404, f"workbook is gone: {xlsx}")

    from dynamic_crawler.formfill.promote import promote, _build_repo

    write = bool(confirm) and not dry_run
    if not write:
        try:
            repo = _build_repo() if with_db else None
            if repo is not None:
                # find_by_identity returns None on connection failure, which is
                # indistinguishable from "not found". Probe with a SELECT that
                # is allowed to raise, so a bad login is a 500 and not a 0.
                repo.get_folder_id("__connectivity_probe__", None)
        except Exception as e:
            raise HTTPException(500, f"could not connect to MSSQL: {e}")
        report = promote(xlsx, repo, dry_run=True)
        report["note"] = ("DRY RUN — nothing was written. Re-send with "
                          "dry_run=false&confirm=true to insert.")
        return report

    logger.warning("PROMOTING %s TO MSSQL", xlsx)
    try:
        repo = _build_repo()
    except Exception as e:
        raise HTTPException(500, f"could not connect to MSSQL: {e}")
    report = promote(xlsx, repo, dry_run=False)
    report["note"] = "written to MSSQL"
    _runs[run_id]["promoted"] = report
    return report


@app.post("/trigger/source/{regulator}", tags=["run"])
def trigger_source(regulator: str,
                   limit: Optional[int] = 5,
                   analyse: bool = False,
                   workbook: Optional[str] = None,
                   check_against_db: bool = True):
    """Run the new orchestrator for one GENERIC-CRAWLER regulator.

    The sibling of `/trigger/{form}`. Same orchestrator, same Excel repo, same
    guarantees — the only difference is which engine produces the documents:
    `config/sources/<regulator>.yml` instead of a formfill hint. That file may
    mix generic and custom sources; `build_regulator_crawler` composes them and
    the orchestrator cannot tell them apart.

    - **limit** — documents to process. `0` or omit for all. Start small.
    - **analyse** — run the 4-stage LLM analysis. Costs money; off by default.
    - **workbook** — append to an existing workbook to see change detection on a
      second run. Omit for a fresh one.
    - **check_against_db** — default TRUE, same as `/trigger/{form}`: seed the
      (empty) workbook with this regulator's current rows from the real
      database (read-only) so "new" means "not already in the library".

    There is no `reuse_last` here. The generic wrapper runs its engine as a
    subprocess and owns its own output directory, so there is no crawl-on-disk
    for this endpoint to point at — every call crawls. Use `limit` to keep it
    short.
    """
    cfg_path = SOURCES_DIR / f"{regulator.lower()}.yml"
    if not cfg_path.exists():
        raise HTTPException(
            400, f"no config at {cfg_path.relative_to(REPO_ROOT)}. "
                 f"available: {sorted(_sources())}")
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    reg_name = cfg.get("regulator", regulator.upper())

    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    from orchestrator.orchestrator import Orchestrator
    from crawler.generic_crawler_wrapper import build_regulator_crawler

    started = datetime.now()
    run_id = f"{reg_name}-{started:%Y%m%d-%H%M%S}"
    out_xlsx = OUT_DIR / (workbook or f"{run_id}.xlsx")

    with _lock:
        try:
            crawler = build_regulator_crawler(cfg)
        except Exception as e:
            raise HTTPException(400, f"{reg_name}: {e}")

        repo = ExcelRepo(out_xlsx)
        seeded_from_db = 0
        if workbook and out_xlsx.exists():
            _load_workbook_into(repo, out_xlsx)
        elif check_against_db:
            seeded_from_db = _seed_repo_from_production_db(repo, reg_name)
            logger.info("seeded %d existing regulation(s) from production DB for %s",
                       seeded_from_db, reg_name)

        orch = Orchestrator(
            crawler=crawler, repo=repo, downloader=None,
            source_name=f"source:{reg_name}", analyse=analyse,
            limit=(limit or None),
            # What counts as "the same document" is a property of the SOURCE, and
            # each source in this file may set its own. These two are the
            # regulator-wide fallback for the sources that do not; omitting
            # identity means (document_url, doc_path).
            identity=cfg.get("identity"),
            version_key=cfg.get("version_key", "reference_no"),
        )
        try:
            report = orch.run_for_regulator(reg_name)
        except Exception as e:
            logger.exception("run failed")
            raise HTTPException(500, f"run failed: {e}")
        excel = repo.save()

    report.update({
        "run_id": run_id,
        "regulator": reg_name,
        "config": str(cfg_path.relative_to(REPO_ROOT)),
        "engines": [s.get("mode", "generic") for s in (cfg.get("sources") or [])],
        "seeded_from_production_db": seeded_from_db,
        "seconds": round((datetime.now() - started).total_seconds(), 1),
        "excel": str(excel),
        "excel_download": f"/runs/{run_id}/excel",
    })
    _runs[run_id] = report
    return report


def _load_workbook_into(repo, path: Path) -> None:
    """Read an existing workbook back so a second run can see the first one.
    Without this, every run starts empty and everything is always 'new'."""
    import pandas as pd
    for sheet in list(repo.t):
        try:
            df = pd.read_excel(path, sheet_name=sheet)
        except Exception:
            continue
        rows = [r for r in df.where(df.notna(), None).to_dict("records")
                if any(v not in (None, "") for v in r.values())]
        repo.t[sheet] = rows
        idcol = {"regulations": "id", "compliancecategory": "compliancecategory_id",
                 "regulation_versions": "version_id",
                 "compliance_analysis": "analysis_id",
                 "requirement_mappings": "mapping_id",
                 "processing_log": "log_id", "run_history": "run_id"}[sheet]
        nums = [int(r[idcol]) for r in rows if str(r.get(idcol, "")).isdigit()]
        repo._next[sheet] = max(nums) if nums else 0
    logger.info("loaded existing workbook: %s", repo.counts())
