"""Crawl one `config/sources/*.yml` regulator WITHOUT going through the API.

WHY THIS EXISTS

`POST /trigger/source/{regulator}` holds a single process-wide `threading.Lock`
for the whole run. That is correct for the API — concurrent crawls would fight
over browsers and over the same workbook — but it means a long regulator blocks
every other regulator behind it.

SAMA is the case that forces the issue. It is the largest source by an order of
magnitude and runs for hours, so triggering it through the API stops all the
short regulators from running at all. This script does exactly what
`trigger_source` does — same `build_regulator_crawler`, same `Orchestrator`,
same `ExcelRepo`, same identity settings — in its own process, so SAMA can run
alongside the others instead of in front of them.

It writes the SAME workbook shape, so `POST /approve/{run_id}` and `promote.py`
treat its output identically. Nothing is written to any database.

    python benchmarks/run_source_standalone.py sama
    python benchmarks/run_source_standalone.py zatca --limit 20
    python benchmarks/run_source_standalone.py sama --out output/.../crawl/SAMA.xlsx

FRESH BY DEFAULT, ON PURPOSE
----------------------------
If the target workbook already exists it is MOVED ASIDE rather than appended to.
Appending is what `--workbook` means on the API and it is right for testing
change detection, but it is wrong for a corrective re-crawl: MC and ZATCA ended
up holding 28+28 and 38+38 rows, the old wrong rows sitting beside the new ones
in the file that gets approved into the database. Pass --append for the change
detection case.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

SOURCES_DIR = REPO_ROOT / "config" / "sources"
CRAWL_DIR = REPO_ROOT / "output" / "formfill" / "_orch_runs" / "crawl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("standalone")


def _read_url_file(path):
    """Urls for --only-urls: one per line, blanks and # comments ignored.

    Returns None when no file was given, which is what FormfillCrawler reads as
    "crawl everything" — distinct from an EMPTY file, which means the sweep ran
    and found nothing changed. The caller checks for that before starting a
    crawl at all; getting it wrong here would turn "nothing to do" into a full
    re-crawl.
    """
    if not path:
        return None
    from pathlib import Path as _P
    lines = [l.strip() for l in _P(path).read_text(encoding="utf-8").splitlines()]
    return [l for l in lines if l and not l.startswith("#")]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("regulator",
                    help="config/sources/<name>.yml, or a form name with --form")
    ap.add_argument("--form", action="store_true",
                    help="treat the argument as a dynamic_crawler/hints/<name>.yml "
                         "form instead of a source config")
    ap.add_argument("--reuse-last", action="store_true",
                    help="replay the crawl already on disk instead of re-crawling "
                         "(forms only) — use when the fix is in EXTRACTION, not "
                         "in the walk")
    ap.add_argument("--label", default=None,
                    help="workbook basename; defaults to the regulator/form name")
    ap.add_argument("--limit", type=int, default=0,
                    help="documents to process; 0 = everything (default)")
    ap.add_argument("--analyse", action="store_true",
                    help="run the 4-stage LLM analysis (costs money; off by default)")
    ap.add_argument("--out", default=None,
                    help="workbook path; defaults to the standard crawl/ folder")
    ap.add_argument("--only-urls", default=None,
                    help="file of urls, one per line — usually written by "
                         "`sweep --targets`. Narrows PHASE 2 to those documents, "
                         "so a monitoring re-crawl opens what changed instead of "
                         "everything. Phase 1 still walks the listing, which is "
                         "what finds documents the sweep cannot see.")
    ap.add_argument("--append", action="store_true",
                    help="append to an existing workbook instead of starting fresh")
    a = ap.parse_args()

    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    from orchestrator.orchestrator import Orchestrator

    cfg = {}
    if a.form:
        cfg_path = REPO_ROOT / "dynamic_crawler" / "hints" / f"{a.regulator}.yml"
        if not cfg_path.exists():
            available = sorted(q.stem for q in (REPO_ROOT / "dynamic_crawler" / "hints").glob("*.yml"))
            logger.error("no form at %s. available: %s", cfg_path, available)
            return 2
        from dynamic_crawler.formfill.schema import load_hints
        lib = (load_hints(str(cfg_path)).get("library") or {})
        reg_name = lib.get("regulator") or a.regulator.upper()
    else:
        cfg_path = SOURCES_DIR / f"{a.regulator.lower()}.yml"
        if not cfg_path.exists():
            available = sorted(q.stem for q in SOURCES_DIR.glob("*.yml"))
            logger.error("no config at %s. available: %s", cfg_path, available)
            return 2
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        reg_name = cfg.get("regulator", a.regulator.upper())

    started = datetime.now()
    run_id = f"{reg_name}-{started:%Y%m%d-%H%M%S}"
    # Named for the CONFIG STEM, not cfg["regulator"], so this agrees with
    # run_all_regulators.py's labels (mc.yml -> MC.xlsx, zatca.yml -> ZATCA.xlsx).
    # Using the regulator's display name produced "Ministry of Commerce.xlsx"
    # beside the harness's "MC.xlsx" — two workbooks for one regulator, one of
    # them stale, in the directory whose whole purpose is "these rows go to the
    # database".
    label = a.label or (a.regulator if a.form else a.regulator.upper())
    out_xlsx = Path(a.out) if a.out else (CRAWL_DIR / f"{label}.xlsx")
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)

    # A corrective re-crawl must REPLACE, not accumulate. The previous file is
    # kept — it is the evidence of what was wrong — just not in the way.
    if out_xlsx.exists() and not a.append:
        # INTO A SUBFOLDER, not alongside. crawl/ means "these rows go to the
        # database", and dead files sitting in it get read as live ones — the
        # directory reached 26 archived workbooks against 11 real ones, and an
        # archived Ministry of Commerce file was mistaken for current output.
        arc = out_xlsx.parent / "_superseded"
        arc.mkdir(exist_ok=True)
        aside = arc / f"{out_xlsx.stem}.superseded-{started:%Y%m%d-%H%M%S}{out_xlsx.suffix}"
        try:
            out_xlsx.rename(aside)
        except PermissionError as e:
            # Usually the file is open in Excel. CONTINUING WOULD APPEND to it,
            # so a corrective re-crawl would end up holding the wrong rows AND
            # the right ones in the file that gets approved. Refuse instead.
            raise SystemExit(
                f"{out_xlsx.name} is locked by another process (usually Excel) "
                f"and cannot be moved aside: {e}. "
                f"Close it and re-run, or pass --append if you genuinely mean to "
                f"compare against it.") from e
        logger.info("previous workbook archived -> _superseded/%s", aside.name)

        # AND PUT IT BACK IF THIS RUN NEVER WRITES ONE.
        #
        # The archive happens at the START and the new workbook is written at the
        # END, so anything that stops the run in between — a timeout, a crash, a
        # blocked host — leaves NO workbook at all, only an orphaned sidecar
        # beside where it used to be. Measured 2026-08-16: CMA's monitoring crawl
        # hit the 5400s timeout and destroyed a 1,979-row workbook that had taken
        # hours to produce. The database was untouched, but the file that feeds
        # `promote` was simply gone.
        #
        # Registered here rather than wrapped in try//finally around the crawl so
        # it also covers SystemExit and a killed process's atexit path.
        import atexit

        def _restore_if_no_output(_p=out_xlsx, _a=aside):
            if _p.exists() or not _a.exists():
                return                      # the run wrote one, or there was none
            try:
                _a.rename(_p)
                logger.error("run produced no workbook — restored the previous "
                             "one from _superseded/%s", _a.name)
            except Exception as e:          # noqa: BLE001 — last-ditch, say so
                logger.error("run produced no workbook and the previous one "
                             "could NOT be restored from %s: %s", _a, e)

        atexit.register(_restore_if_no_output)

    logger.info("=" * 70)
    logger.info("%s  |  config %s  |  limit %s  |  analyse %s",
                reg_name, cfg_path.name, a.limit or "ALL", a.analyse)
    logger.info("workbook -> %s", out_xlsx)
    logger.info("=" * 70)

    if a.form:
        import json as _json
        from dynamic_crawler.formfill.pipeline import FormfillCrawler
        from dynamic_crawler.formfill.schema import load_hints
        lib = (load_hints(str(cfg_path)).get("library") or {})
        run_dir = REPO_ROOT / "output" / "formfill" / a.regulator / "standalone_run"
        crawler = FormfillCrawler(
            str(cfg_path),
            regulator=lib.get("regulator") or reg_name,
            source_system=lib.get("source_system") or a.regulator,
            require_approved=False,
            out_dir=None if a.reuse_last else str(run_dir),
            only_urls=_read_url_file(a.only_urls),
        )
        if a.reuse_last:
            # Replay the crawl already on disk. The right move when the fix is in
            # EXTRACTION rather than in the walk — it skips the browsing but still
            # pays for attachment OCR.
            cands = sorted(
                (REPO_ROOT / "output" / "formfill" / a.regulator).glob("*/pages.json"),
                key=lambda q: q.stat().st_mtime, reverse=True)
            if not cands:
                logger.error("--reuse-last: no crawl on disk for %s", a.regulator)
                return 2
            logger.info("reusing crawl %s", cands[0])
            crawler._run_crawl = lambda q=cands[0]: _json.loads(q.read_text(encoding="utf-8"))
        source_name = a.regulator
    else:
        from crawler.generic_crawler_wrapper import build_regulator_crawler
        crawler = build_regulator_crawler(cfg)
        source_name = f"source:{reg_name}"

    repo = ExcelRepo(out_xlsx)
    orch = Orchestrator(
        crawler=crawler, repo=repo, downloader=None,
        source_name=source_name, analyse=a.analyse,
        limit=(a.limit or None),
        identity=cfg.get("identity"),
        version_key=cfg.get("version_key", "reference_no"),
    )

    try:
        report = orch.run_for_regulator(reg_name)
    except Exception as e:
        # Save whatever was collected before the failure — a partial workbook is
        # far more use than none when diagnosing a long run.
        try:
            repo.save()
            logger.error("run failed; partial workbook saved to %s", out_xlsx)
        except Exception:
            pass
        logger.exception("%s failed: %s", reg_name, e)
        return 1

    repo.save()
    secs = round((datetime.now() - started).total_seconds(), 1)

    counts = report.get("counts") or {}
    logger.info("=" * 70)
    logger.info("%s DONE in %ss  |  new=%s modified=%s unchanged=%s  |  gate=%s",
                reg_name, secs,
                counts.get("new", "?"), counts.get("modified", "?"),
                counts.get("unchanged", "?"),
                (report.get("completeness") or {}).get("verdict", "?"))
    logger.info("workbook: %s", out_xlsx)
    logger.info("run_id:   %s", run_id)
    logger.info("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
