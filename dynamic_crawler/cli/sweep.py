"""CLI: ask each regulator what version it holds of the documents we store.

Read-only. One SELECT against the library, two bytes per document against the
regulator, and the only thing written anywhere is the sweep's own state file under
output/change_state/ — which is what the NEXT sweep compares against. Nothing
here touches a regulations row.

Three signals, one entry point, because they answer the same question and keep
their memory in the same place:

    stored-inventory  re-read the version token of every url we already store.
                      Detect only — it cannot see a document it never stored.
    gosi              one JSON request per seed page, which returns the WHOLE
                      page, so this one can also report a document as absent.
    snapshot-articles one hash per article of a page already saved on disk.
                      No request at all, which is what makes it usable against
                      a host we are blocked from.
    sitemap           one request for a whole source, where the sitemap's
                      per-url lastmod is a real edit history. It refuses to run
                      on a sitemap that only carries its own build time.

Usage:
    python -m dynamic_crawler.cli.sweep --regulator SDAIA \\
        --source "Laws and Regulations" --workbook output/formfill/run.xlsx

    python -m dynamic_crawler.cli.sweep --regulator SDAIA \\
        --source "Laws and Regulations" --with-db --limit 5 --dry-run

    python -m dynamic_crawler.cli.sweep --signal gosi --source SocialInsurance

    python -m dynamic_crawler.cli.sweep --signal snapshot-articles \\
        --regulator SIMAH --source simah.rules

    python -m dynamic_crawler.cli.sweep --signal sitemap --regulator MHRSD \\
        --source mhrsd.regs --run-workbook output/formfill/mhrsd.regs/run/results.xlsx

A first sweep of a source stores a baseline and reports every document as new.
That is not a change report; the second sweep is.

Every report carries a `withdrawals` block: which absent documents meet the
two-consecutive-sweeps rule and which condition stopped the rest. It is a
proposal for a person — nothing here withdraws anything.

It also carries `targets`: the urls of the documents ruled `modified`, which
`formfill run --only-urls` re-crawls and nothing else. `--targets <file>` writes
them for that command to read.
"""

# FIRST, before anything opens a connection. This module runs as its OWN process
# (monitor_jobs._sweep launches `python -m dynamic_crawler.cli.sweep`) and never
# imports orchestrator/orchestrator.py, which is the only other place
# truststore is injected. Without it `requests` verifies against certifi's fixed
# bundle, which cannot complete the chain for hosts that omit their intermediate
# certificate, and every probe to them fails with
#     SSLCertVerificationError: unable to get local issuer certificate
# That is recorded as `probe-failed` -- no version token -- which reads exactly
# like a regulator with nothing to report. Measured 2026-09-15 on AML: the same
# 11 documents gave {"probe-failed": 11} without this and {"etag": 11} with it,
# and it affects every source in CHEAP_PROBE_SOURCES. Certificates are still
# fully verified, against the OS store instead of certifi.
import truststore

truststore.inject_into_ssl()

import argparse
import json
import logging
import sys
from pathlib import Path

from dynamic_crawler import changesignal as cs
from dynamic_crawler.change_state import ChangeStateStore
from dynamic_crawler.gosi_signal import SEEDS, GosiJsonSweep
from dynamic_crawler.inventory_sweep import (StoredInventorySweep,
                                             WorkbookInventory, load_config,
                                             run_workbook_urls, settings_for,
                                             skip_hosts)
from dynamic_crawler.sama_feed_signal import SamaFeedSweep, default_window
from dynamic_crawler.sitemap_signal import DEFAULT_CUT, SitemapLastmodSweep
from dynamic_crawler.snapshot_articles import SnapshotArticleSweep
from dynamic_crawler import withdrawal

logger = logging.getLogger(__name__)


def _run(signal, source: str, state_root=None, dry_run: bool = False) -> dict:
    """Run one signal against its own state file and report. Common to both."""
    store = ChangeStateStore.for_source(source, root=state_root)
    first = not store.keys()
    report, buckets = cs.run_sweep(signal, store)
    report["sweep"] = getattr(signal, "stats", {})
    if first:
        report["note"] = ("first sweep for this source: every document is a "
                          "baseline, not a change")
    if not dry_run:
        report["state_file"] = str(store.save())
    else:
        report["state_written"] = False

    # The shortlist is the product, so print it rather than only counting it.
    def entry(o, why: str) -> dict:
        """The `missing` bucket holds bare identity keys, not observations.

        Not one getattr with a default: `getattr(str, "title")` finds the string
        METHOD, so this raised on the first document any sweep reported absent.
        """
        if isinstance(o, str):
            return {"key": o, "title": "", "url": "", "why": why}
        return {"key": o.key, "title": str(o.title or "")[:70],
                "url": o.url, "why": why}

    verdicts = (cs.MODIFIED, cs.UNKNOWN, cs.NEW, cs.MISSING)
    report["shortlist"] = {
        verdict: [entry(o, why) for o, why in buckets[verdict]]
        for verdict in verdicts if buckets.get(verdict)}

    # What a re-crawl should open, and nothing else. `modified` only: `new` on a
    # detect-only sweep means "first time this was swept", not a new document,
    # and re-crawling on it would re-read the whole source on its first run.
    report["targets"] = sorted({o.url for o, _ in buckets[cs.MODIFIED]
                                if getattr(o, "url", "")})
    without = sum(1 for o, _ in buckets[cs.MODIFIED] if not getattr(o, "url", ""))
    if without:
        # A shortlisted document with no url cannot be handed to a crawl. Said
        # out loud rather than silently dropped from the list.
        report["targets_without_url"] = without
    # Absence counted is not absence acted on. This says which absences meet the
    # rule and which condition stopped the rest; nothing is withdrawn by it.
    report["withdrawals"] = withdrawal.proposals(signal, store, report, buckets)
    return report


def sweep(regulator: str, source_system: str, *, repo=None, workbook=None,
          config_path=None, state_root=None, limit=None, workers=None,
          dry_run: bool = False) -> dict:
    """Sweep one source and return the report."""
    if repo is None and workbook is None:
        raise ValueError("give either a repo or a workbook to read the "
                         "stored inventory from")
    config = load_config(config_path)
    settings = settings_for(config, regulator, source_system)

    signal = StoredInventorySweep(
        repo if repo is not None else WorkbookInventory(workbook),
        regulator, source_system,
        identity=settings.get("identity") or ("document_url", "doc_path"),
        confirm_required=bool(settings.get("confirm")),
        workers=workers or settings.get("workers"),
        timeout=float(settings.get("timeout") or 20),
        limit=limit,
        skip_hosts=skip_hosts(config))

    return _run(signal, f"{regulator}/{source_system}", state_root, dry_run)


def snapshot_sweep(form: str, *, regulator: str = "SIMAH", state_root=None,
                   snapshot_dir=None, allow_stale: bool = False,
                   dry_run: bool = False) -> dict:
    """Hash each article of a page already saved on disk. No request is made."""
    from dynamic_crawler.formfill.snapshot import SnapshotStore

    store = SnapshotStore(form, directory=snapshot_dir)
    signal = SnapshotArticleSweep(form, store=store, page=form,
                                  allow_stale=allow_stale)
    return _run(signal, f"{regulator}/{form}", state_root, dry_run)


def _tracked_urls(regulator: str, source_system: str, *, repo=None,
                  workbook=None, run_workbook=None) -> list:
    """The urls this source stores, from whichever inventory is available."""
    if run_workbook:
        return run_workbook_urls(run_workbook)
    inventory = repo if repo is not None else WorkbookInventory(workbook)
    rows = inventory.find_regulations_by_source(source_system,
                                                regulator=regulator)
    # EVERY url a row is reachable by, not just document_url.
    #
    # The SAMA feed resolves each entry to the document's NODE url, and until
    # 2026-08-24 that was what document_url held. It now holds the PDF (see
    # scripts/sama_pdf_as_document_url.py) and the node url moved to
    # source_page_url -- so matching on document_url alone would have counted
    # all 4,957 rows as `not_in_library`, and monitor_sama answers that by
    # running benchmarks/sama_feed_ingest.py, which would have re-inserted the
    # regulator as new documents.
    #
    # Widening is safe in one direction only, which is the direction we want: it
    # can only turn a false DISCOVERY into a correct match. It can never hide a
    # document the library really lacks, because a genuinely new document
    # matches none of these fields.
    urls = []
    for r in rows:
        for key in ("document_url", "source_page_url"):
            u = str(r.get(key) or "").strip()
            if u:
                urls.append(u)
        meta = r.get("extra_meta")
        if isinstance(meta, str):
            try:
                import json as _json
                meta = _json.loads(meta)
            except Exception:
                meta = None
        if isinstance(meta, dict):
            u = str(meta.get("found_on") or "").strip()
            if u:
                urls.append(u)
    return urls


def sitemap_sweep(source_system: str, *, regulator: str, sitemap_url=None,
                  repo=None, workbook=None, run_workbook=None, config_path=None,
                  state_root=None, dry_run: bool = False) -> dict:
    """Read one sitemap and shortlist the tracked urls whose lastmod moved."""
    config = load_config(config_path)
    settings = settings_for(config, regulator, source_system)
    url = sitemap_url or settings.get("sitemap")
    if not url:
        raise SystemExit(f"no sitemap url for {regulator}/{source_system}: pass "
                         f"--sitemap or add one to config/change_signals.yml")

    signal = SitemapLastmodSweep(
        url, f"{regulator}/{source_system}",
        _tracked_urls(regulator, source_system, repo=repo, workbook=workbook,
                      run_workbook=run_workbook),
        cut_marker=settings.get("cut_marker") or DEFAULT_CUT,
        timeout=float(settings.get("timeout") or 30))
    return _run(signal, f"{regulator}/{source_system}", state_root, dry_run)


def sama_feed_sweep(source_system: str, *, regulator: str, since=None, until=None,
                    repo=None, workbook=None, run_workbook=None,
                    config_path=None, state_root=None, days: int = 30,
                    dry_run: bool = False) -> dict:
    """Read SAMA's own revision feed instead of probing every stored document.

    One request for the whole source, plus one per CHANGED document to turn its
    slug into the node url the library stores. 6,101 requests become 1 + n.

    It cannot see deletions -- a withdrawn document just stops being listed -- so
    `stored-inventory` stays SAMA's way of finding removals. Run this daily and
    that occasionally, not the other way round.
    """
    config = load_config(config_path)
    settings = settings_for(config, regulator, source_system)
    lo, hi = default_window(days)
    signal = SamaFeedSweep(
        f"{regulator}/{source_system}",
        _tracked_urls(regulator, source_system, repo=repo, workbook=workbook,
                      run_workbook=run_workbook),
        since=since or lo, until=until or hi,
        timeout=float(settings.get("timeout") or 45))
    report = _run(signal, f"{regulator}/{source_system}", state_root, dry_run)
    # What the feed itself saw, kept beside the verdicts: an entry that matched
    # nothing in the library is a DISCOVERY, which a stored-inventory probe
    # cannot produce, and it should be visible without re-reading the shortlist.
    report["feed"] = signal.stats
    return report


def gosi_sweep(seed: str, *, regulator: str = "GOSI", config_path=None,
               state_root=None, workers=None, probe_documents: bool = True,
               dry_run: bool = False) -> dict:
    """Sweep one GOSI seed page: one JSON request, no browser, no database."""
    config = load_config(config_path)
    settings = settings_for(config, regulator, seed)

    signal = GosiJsonSweep(seed,
                           timeout=float(settings.get("timeout") or 20),
                           probe_documents=probe_documents,
                           workers=workers or settings.get("workers"),
                           skip_hosts=skip_hosts(config))

    return _run(signal, f"{regulator}/{seed}", state_root, dry_run)


def _repo(a):
    """The read-only connection, or None when the inventory comes off disk."""
    if not a.with_db:
        return None
    # Same connection the rest of the pipeline builds, so the driver and the
    # credentials come from one place.
    from dynamic_crawler.formfill.promote import _build_repo
    repo = _build_repo()
    # A SELECT that is allowed to raise, first: find_regulations_by_source
    # raises on failure but a bad login should not look like an empty library.
    repo.get_folder_id("__connectivity_probe__", None)
    return repo


def _emit(report: dict, json_out=None, targets_out=None) -> int:
    text = json.dumps(report, indent=2, ensure_ascii=False, default=str)
    print(text)
    if json_out:
        Path(json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(json_out).write_text(text, encoding="utf-8")
    if targets_out:
        # One url per line, written even when empty: an empty file is "nothing
        # changed", a missing file is "the sweep did not run", and a re-crawl
        # driven by this must be able to tell them apart.
        Path(targets_out).parent.mkdir(parents=True, exist_ok=True)
        Path(targets_out).write_text("\n".join(report.get("targets") or ()),
                                     encoding="utf-8")
    return 0


def main() -> int:
    # Before argparse prints anything: titles and error pages from these
    # regulators are Arabic, and a cp1252 console kills the run on the first one.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass

    ap = argparse.ArgumentParser(
        description="Ask a regulator which version it holds of what we store")
    ap.add_argument("--signal", default="stored-inventory",
                    choices=("stored-inventory", "gosi", "snapshot-articles",
                             "sitemap", "sama-feed"),
                    help="stored-inventory: probe every url we already store. "
                         "gosi: one JSON request per seed page. "
                         "snapshot-articles: a saved page, no request at all. "
                         "sitemap: one request, per-url lastmod. "
                         "sama-feed: SAMA's own revision page, one request "
                         "instead of 6,101 probes (cannot see deletions)")
    ap.add_argument("--regulator", default="GOSI",
                    help="as stored in the regulations row, e.g. SDAIA. "
                         "Required for --signal stored-inventory")
    ap.add_argument("--source", required=True, dest="source_system",
                    help="the source_system as stored, e.g. 'Laws and "
                         f"Regulations'. For --signal gosi: one of {SEEDS}")
    ap.add_argument("--workbook", help="read the inventory from a formfill "
                                       "workbook instead of the database")
    ap.add_argument("--with-db", action="store_true",
                    help="read the inventory from MSSQL (one SELECT)")
    ap.add_argument("--no-documents", action="store_true",
                    help="gosi: read the page only. It then reports no absence "
                         "at all, because the documents it skipped would be it")
    ap.add_argument("--allow-stale", action="store_true",
                    help="snapshot-articles: sweep a snapshot past its grace "
                         "period anyway. It reports what the page said when it "
                         "was captured, not what it says now")
    ap.add_argument("--snapshot-dir", help="default: output/snapshots")
    ap.add_argument("--run-workbook",
                    help="read the tracked urls from a formfill RUN workbook's "
                         "inventory sheet — the only inventory a form with no "
                         "promoted rows has")
    ap.add_argument("--sitemap", help="sitemap url, when the config has none")
    ap.add_argument("--limit", type=int, help="probe only the first N documents")
    ap.add_argument("--workers", type=int, help="override the configured probes "
                                                "in flight")
    ap.add_argument("--dry-run", action="store_true",
                    help="probe and report, but do not update the state file — "
                         "so the next sweep still compares against today's "
                         "baseline")
    ap.add_argument("--config", help="default: config/change_signals.yml")
    ap.add_argument("--state-root", help="default: output/change_state")
    ap.add_argument("--json", dest="json_out", help="also write the report here")
    ap.add_argument("--feed-days", type=int, default=30,
                    help="sama-feed: how far back to ask, in days (default 30). "
                         "Ignored when --since/--until are given.")
    ap.add_argument("--since", default=None, help="sama-feed: YYYY-MM-DD")
    ap.add_argument("--until", default=None, help="sama-feed: YYYY-MM-DD")
    ap.add_argument("--targets", dest="targets_out",
                    help="write the modified documents' urls here, one per "
                         "line, for `formfill run --only-urls`. Modified only: "
                         "a `new` verdict on a detect-only sweep means the "
                         "first sweep of a document we already store")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    if a.signal == "snapshot-articles":
        report = snapshot_sweep(a.source_system, regulator=a.regulator,
                                state_root=a.state_root,
                                snapshot_dir=a.snapshot_dir,
                                allow_stale=a.allow_stale, dry_run=a.dry_run)
        return _emit(report, a.json_out, a.targets_out)

    if a.signal == "gosi":
        if a.source_system not in SEEDS:
            raise SystemExit(f"--source for --signal gosi is one of {SEEDS}, "
                             f"not {a.source_system!r}")
        report = gosi_sweep(a.source_system, regulator=a.regulator,
                            config_path=a.config, state_root=a.state_root,
                            workers=a.workers,
                            probe_documents=not a.no_documents,
                            dry_run=a.dry_run)
        return _emit(report, a.json_out, a.targets_out)

    if a.signal == "sitemap":
        if not (a.workbook or a.with_db or a.run_workbook):
            raise SystemExit("pass --run-workbook, --workbook or --with-db: the "
                             "guard measures the sitemap against the urls this "
                             "source stores, and cannot run without them")
        report = sitemap_sweep(a.source_system, regulator=a.regulator,
                               sitemap_url=a.sitemap, repo=_repo(a),
                               workbook=a.workbook, run_workbook=a.run_workbook,
                               config_path=a.config, state_root=a.state_root,
                               dry_run=a.dry_run)
        return _emit(report, a.json_out, a.targets_out)

    if a.signal == "sama-feed":
        if not (a.workbook or a.with_db or a.run_workbook):
            raise SystemExit("pass --with-db, --workbook or --run-workbook: the "
                             "feed says what SAMA changed, and the stored "
                             "inventory is what says whether we already hold it")
        report = sama_feed_sweep(a.source_system, regulator=a.regulator,
                                 repo=_repo(a), workbook=a.workbook,
                                 run_workbook=a.run_workbook,
                                 config_path=a.config, state_root=a.state_root,
                                 days=a.feed_days, since=a.since, until=a.until,
                                 dry_run=a.dry_run)
        return _emit(report, a.json_out, a.targets_out)

    if not a.workbook and not a.with_db:
        raise SystemExit("pass --workbook <xlsx> or --with-db: the sweep reads "
                         "the urls it probes out of the stored inventory")
    if a.workbook and not Path(a.workbook).exists():
        raise SystemExit(f"no such workbook: {a.workbook}")

    report = sweep(a.regulator, a.source_system,
                   repo=_repo(a), workbook=a.workbook,
                   config_path=a.config, state_root=a.state_root, limit=a.limit,
                   workers=a.workers, dry_run=a.dry_run)
    # `a.targets_out` IS PASSED HERE TOO. It was omitted on this branch alone —
    # the other three signals above all forward it — so `--targets` was silently
    # ignored on `stored-inventory`, which is the signal every regulator uses.
    #
    # The failure was invisible: the report still listed the modified urls under
    # "targets", the exit code was 0, and nothing warned. Only the FILE was
    # missing, so anything driving a re-crawl from it read "nothing changed" and
    # skipped the crawl. Measured 2026-08-15 on SDAIA: 2 documents modified,
    # 2 targets in the report, 0 written, no crawl.
    return _emit(report, a.json_out, a.targets_out)


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.exit(main())
