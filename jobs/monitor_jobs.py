"""Scheduled monitoring — the jobs the scheduler calls, writing straight to MSSQL.

FIVE JOBS, NOT TWELVE. Regulators are grouped by what their site will actually
answer, because that — not the regulator's importance — is what decides how often
and how expensively it can be checked.

    monitor_cheap_probes   daily    MOE, SDAIA, AML, MHRSD, ZATCA, KDIPA — ask each
                                    stored url for its version token, crawl only
                                    what moved. MOH rides along in the same job
                                    but skips the probe: its crawl already IS a
                                    cheap, self-describing signal (see below).
    monitor_sama           daily    ~3 seconds. SAMA publishes its own
                                    "what changed" page, so one request replaces
                                    6,101 probes — and it also DISCOVERS
                                    documents we do not hold, which a probe
                                    structurally cannot.
    monitor_mc             weekly   ~16 minutes. mc.gov.sa refuses plain HTTP
                                    clients, so no probe can answer and the
                                    CRAWL is the signal.
    monitor_cma            weekly   CMA's token is the current time and a
                                    confirm costs a full page fetch, so the
                                    crawl is the signal here too.
    monitor_mlcu           weekly   Egypt. No ETag, no Last-Modified, no sitemap
                                    lastmod, and its news page carries neither
                                    dates nor document links — every cheaper
                                    signal was measured and ruled out. SHIPPED
                                    DISABLED: the workbook has not been read yet.
    monitor_cbe            weekly   Twelve sources: the circulars API (one
                                    request, and it DISCOVERS) plus eleven
                                    browser crawls of the HTML sections. Weekly
                                    because of the eleven, not the one.
    monitor_bahrain_bourse weekly   Bahrain. One request (the site's own
                                    GetFaq API) returns all seven Legal
                                    Framework sections, ~90 documents. SHIPPED
                                    DISABLED: the workbook has not been read
                                    yet. Bahrain Bourse is an exchange, like
                                    the now-permanently-blocked
                                    saudiexchange.sa — see
                                    docs/HANDOFF_bahrain_bourse.md before
                                    touching this host.

WHERE THE ROWS GO

Straight into MSSQL, by the lead's decision 2026-08-16 — no workbook, no
approval step in the middle.

EXCELREPO IS NOT ON THIS PATH, AND MUST NOT BE PUT BACK ON IT. The class stays
in the repo (it is a working second implementation of the same contract, and
`promote` still replays a workbook when someone deliberately produces one), but
no scheduled job may write through it. Confirmed by the lead 2026-08-16: "data
should drop directly in db no excel needed... keep the repo but dont use it in
actual orch path".

That decision has a cost worth stating once: the workbook was the only place a
person saw rows before they entered the library. What replaces it is `status`
— every row arrives empty and a human sets active/reject — so the review moved
from before the write to after it, and nothing is lost as long as something
actually reads `WHERE status = ''`. `status` is still left EMPTY: the orchestrator's
`_set_status` puts the monitoring state in extra_meta and leaves the column for a
person, so "what arrived overnight and nobody has judged" is exactly

    SELECT * FROM regulations WHERE status = ''

Nothing here writes `active`. A pipeline that approves its own output is not an
approval.

WHY THE BLOCKED SITES HAVE NO JOB AT ALL

Saudi Exchange and SIMAH are deliberately absent, and this is the important part:
they are not merely skipped, they must not be RETRIED BY A MACHINE.

    saudiexchange.sa   Akamai 403 to everything, headless browser included, so
                       it is the IP being judged and not the User-Agent. It was
                       reachable at 18:27 on 2026-08-15 and blocked within two
                       hours, after one crawl plus repeated probes from this
                       address.
    simah.com          Cloudflare 1020-class block. The note in
                       config/change_signals.yml records that it was "triggered
                       by repeated iteration, not volume".

Both blocks were caused by automated access from one address. A scheduled retry
is therefore not a way out of them — it is the thing that made them, and it would
deepen them. `skip_hosts` in config/change_signals.yml already stops a sweep
touching either host; this file additionally gives them no job, so nothing can
schedule its way past that. Each entry carries an `until` date, and that date is
when a PERSON may retest by hand — it is a review date, not an expiry. Nothing
here unblocks itself.

RUNNING THIS

The functions are registered in scheduler/scheduler.py's DIRECT_JOB_MAPPING and
timed by config/scheduler.yml. Nothing starts them from this file.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from filelock import FileLock, Timeout as LockTimeout

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  overlap guard                                                               #
# --------------------------------------------------------------------------- #
#
# APScheduler's `max_instances=1` (set in scheduler.py) only stops ONE job from
# overlapping ITSELF — it does nothing for two different jobs. MHRSD's
# `Page crashed` on 2026-08-16 was memory contention between two separate
# monitor_* jobs' browser crawls running at once: the daily/weekly stagger in
# config/scheduler.yml is a schedule, not a guarantee, and CMA's crawl (up to
# 2h49m measured) can still be running when the next job's trigger fires.
#
# All four monitor_* jobs share this one lock because any of them can end up
# running a browser crawl (`_crawl_into_db` / `FormfillCrawler`), and it is
# concurrent BROWSER work, not concurrent jobs per se, that crashes a page.
#
# Decision: SKIP, not queue. These are recurring sweeps (daily or weekly) —
# the next trigger picks up whatever a skipped run would have found. Queueing
# risks pile-up instead: a slow CMA run queueing behind a stuck cheap_probes
# run, then next week's CMA queueing behind THAT, with no bound on how far
# behind it falls. A skip is visible in the log and cheap to recover from; a
# growing queue of stacked crawls is not.
_LOCK_PATH = REPO_ROOT / "output" / "monitor_jobs.lock"


def _run_exclusive(job_name: str, fn):
    """Run `fn()` only if no other monitor_* job currently holds the crawl
    lock. Returns fn()'s result, or a skip dict if the lock was busy.
    """
    _LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(_LOCK_PATH), timeout=0)
    try:
        with lock:
            return fn()
    except LockTimeout:
        msg = (f"{job_name} SKIPPED: another monitor_* job already holds "
               f"{_LOCK_PATH.name} (concurrent crawls crash a browser page, "
               f"see comment above _run_exclusive). Next scheduled run will "
               f"pick this up.")
        logger.warning(msg)
        return {"skipped": True, "reason": msg}

#: Sources whose site answers a cheap probe honestly. Measured 2026-08-15/16 —
#: every one of these returned 0 false `modified` on a second sweep once its
#: signal was configured (ZATCA and CMA needed `confirm: true`; ZATCA is here,
#: CMA is not, because confirming CMA costs a full page fetch per document).
#: MOH is NOT here — see CRAWL_AS_SIGNAL below, it used to be but the probe step
#: was pure overhead once the site's real API was found.
CHEAP_PROBE_SOURCES = [
    ("Ministry of Education", "Systems, Regulations and Policies"),
    ("Saudi Data and AI Authority (SDAIA)", "Laws and Regulations"),
    ("Anti-Money Laundering Permanent Committee (AML)", "Rules and Regulations"),
    ("Ministry of Human Resource and Social Development (MHRSD)",
     "Regulations and procedural guidelines"),
    ("Zakat, Tax and Customs Authority (ZATCA)", "Rules and Regulations"),
    # KDIPA, added 2026-09-08. ONE url, because config/sources/kdipa.yml
    # declares the single instrument instead of crawling for it -- so this pair
    # costs one request per sweep. Both halves are the same string on purpose;
    # see the entry in config/change_signals.yml for why.
    ("REGULATION GOVERNING COLLECTIVE INVESTMENT SCHEME JUNE 2013",
     "REGULATION GOVERNING COLLECTIVE INVESTMENT SCHEME JUNE 2013"),
]

#: Regulator -> (crawler name, is_form) for the sources whose crawl IS the
#: signal. Kept here rather than derived, so adding one is a deliberate act.
#:
#: MOH joined this 2026-08-17. It used to be a CHEAP_PROBE_SOURCES entry —
#: probe each stored url, crawl only if something moved — but that assumed the
#: only way to read the site was a browser walk of the "Recent"/"Archived"
#: lists (`dynamic_crawler/hints/moh.rules_recent.yml` /
#: `moh.rules_archived.yml`, still in the repo and still correct, just no
#: longer on this path). The real listing page turns out to call a SharePoint
#: REST endpoint that returns all 83 documents, with each one's own
#: last-changed timestamp, in a single ~2 second request
#: (`crawler/moh_crawler.py`). That is already cheaper than a per-url probe
#: loop AND it already tells you what changed, so the probe step was pure
#: overhead — the crawl IS the signal here, same as MC and CMA, just fast
#: enough to run daily instead of weekly.
#:
#: MLCU joined 2026-08-17, and unlike the others it is here because every
#: cheaper option was measured and failed rather than because the host fights
#: us: no ETag, no Last-Modified, no sitemap lastmod, and its news page carries
#: neither dates nor document links. The measurements are the comment on its
#: config/change_signals.yml entry.
#:
#: ITS SCHEDULER SLOT IS `enabled: false` AND MUST STAY THAT WAY until a person
#: has read the workbook. This dict feeds `_crawl_into_db`, which writes
#: STRAIGHT TO MSSQL — turning MLCU on before the workbook is approved would
#: make the first scheduled run be the ingest, with nobody having read anything.
#: CBE joined 2026-08-18, and for the same reason MOH did: its circulars page is
#: a "Load more" pager that a crawl reads 4.5% of (18 of 396, reported `ok`),
#: while the page's own JavaScript calls /api/listing/circulars and returns all
#: 396 in ONE request — with a publication date, the regulator's own category,
#: and a Sitecore GUID per record. No probe can improve on that.
#:
#: CBE differs from MOH in one way that decides its cadence: its config holds
#: TWELVE sources, and eleven of them are browser crawls of the HTML sections.
#: `build_regulator_crawler` has no source filter, so a job gets all twelve or
#: none — which is why CBE is weekly like MC and CMA rather than daily like MOH.
#: If daily circulars are ever wanted, the upgrade is to split cbe.yml in two,
#: not to run eleven browser crawls every night.


#: RERA joined 2026-08-19, and it is here for a DIFFERENT reason from the others.
#: MC and CMA are here because a probe cannot work; RERA's probe works better than
#: almost any source we hold — 117 of 121 stored urls return both an ETag and a
#: Last-Modified, and eight fetched twice 0.4s apart were 8/8 identical.
#:
#: It is here because a probe answers the wrong question. RERA partitions its
#: circulars BY YEAR, one page per year, and a new year is a NEW PAGE
#: (Circulars-issued-in-2026 is a 404 today). A probe re-reads urls we already
#: store, so it can report a silent replacement but can never see a new circular
#: or a new year page. RERA published two circulars in 2025, so the case a probe
#: covers is nearly hypothetical and the case it cannot cover is the whole point.

#: Bahrain Bourse joined 2026-08-20, for the same reason MOH and CBE's
#: circulars did: the Legal Framework accordion is entirely client-rendered
#: (a plain fetch of the page contains none of it) but the page's own JS calls
#: one API, GetFaq, which returns all seven sections — Laws, Rules &
#: Regulations, Resolutions, Guidelines, Circulars, CBB Rules & Regulations,
#: Consultation — in a single ~20 KB request. No probe can improve on that.
#: See crawler/bahrain_bourse_crawler.py for the double-encoding this API
#: requires; doc_path puts the section name in the folder tree, matching the
#: site's own accordion.
#:
#: ITS SCHEDULER SLOT IS `enabled: false` AND MUST STAY THAT WAY until a
#: person has read the workbook, same as MLCU and CBE were shipped.
CRAWL_AS_SIGNAL = {
    "Ministry of Commerce": ("mc", False),
    "Capital Market Authority (CMA)": ("cma", False),
    "Ministry of Health": ("moh", False),
    "Egyptian Anti-Money Laundering and Counter-Terrorism Financing Unit (MLCU)":
        ("mlcu", False),
    "Central Bank of Egypt (CBE)": ("cbe", False),
    "Bahrain Bourse (BHB)": ("bahrain_bourse", False),
    # Migrated off the bespoke cbb_monitoring job 2026-08-20. The Thomson Reuters
    # revision feed is the cheaper signal and should front this — see monitor_cbb.
    "Central Bank of Bahrain": ("cbb", False),

    # ---- Bahrain, added by abeeraslam 2026-08-25 ----------------------- #
    "Real Estate Regulatory Authority (RERA)": ("rera", False),
    "Social Insurance Organisation (SIO)": ("sio", False),
    "Legislation and Legal Opinion Commission (LLOC)": ("lloc", False),
    # PDPA joined 2026-08-31. The cheapest entry on this list by some distance:
    # both its sources are max_pages: 1, so the whole regulator is TWO page
    # loads. Its 60 articles are one page's accordion panels and its 20 executive
    # decisions are one page's links. See config/change_signals.yml for why
    # neither stored-inventory nor snapshot-articles is right despite both being
    # technically available.
    "Personal Data Protection Authority (PDPA)": ("pdpa", False),
    # MOIC joined 2026-09-01, and unusually the probe is the EXPENSIVE option: a
    # stored-inventory sweep is 90 requests against 11 page loads for the whole
    # regulator, and it cannot see a new form category, which is the thing
    # moic.yml's discovering Forms source exists to catch. See
    # config/change_signals.yml for the four alternatives and why each was ruled
    # out.
    "Ministry of Industry and Commerce (MOIC)": ("moic", False),
    # CBJ joined 2026-09-16. Ten sources, all `max_pages: 1`, so the whole
    # regulator is TEN page loads against 376 probes for a stored-inventory
    # sweep -- and that sweep could not see a new circular, which on this
    # regulator is the main event. config/change_signals.yml carries the
    # measurement for all four alternatives, including why stored-inventory is
    # ruled out on evidence rather than on availability: it genuinely works here.
    "Central Bank of Jordan (CBJ)": ("cbj", False),
}


# --------------------------------------------------------------------------- #
#  plumbing                                                                    #
# --------------------------------------------------------------------------- #

def _repo():
    """An MSSQLRepository from .env. Direct writes, no workbook."""
    from dotenv import load_dotenv
    from storage.mssql_repo import MSSQLRepository
    load_dotenv(REPO_ROOT / ".env", override=True)
    return MSSQLRepository({
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    })


def _run(cmd, timeout):
    """A child process, decoded as utf-8.

    `text=True` alone decodes with the LOCALE encoding — cp1252 on this machine —
    and every report here carries Arabic titles. A finished CMA sweep was once
    reported as FAILED for exactly that reason, after 600 seconds of real work.
    """
    t0 = time.time()
    p = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=timeout)
    return p, time.time() - t0


def _sweep(regulator: str, source: str, targets_file: Path, timeout=1800) -> dict:
    """One source's change sweep. Returns the report, or {} if it failed."""
    cmd = [sys.executable, "-B", "-m", "dynamic_crawler.cli.sweep",
           "--signal", _signal_for(regulator, source), "--regulator", regulator,
           "--source", source, "--with-db", "--targets", str(targets_file)]
    try:
        p, dt = _run(cmd, timeout)
        rep = json.loads(p.stdout[p.stdout.index("{"):])
        rep["_seconds"] = round(dt, 1)
        return rep
    except Exception as e:                       # noqa: BLE001 — logged, not raised
        logger.error("sweep failed for %s/%s: %s", regulator, source, str(e)[:200])
        return {}


def _signal_for(regulator: str, source: str) -> str:
    """Which signal this source uses — from config, never hardcoded.

    config/change_signals.yml already describes a source's monitoring (MHRSD's
    sitemap, AML's confirm), so the CHOICE of signal belongs there too.
    """
    import yaml
    cfg = yaml.safe_load(
        (REPO_ROOT / "config" / "change_signals.yml").read_text(encoding="utf-8"))
    for src in (cfg.get("sources") or []):
        if (src.get("regulator") == regulator
                and src.get("source_system") == source):
            return src.get("signal") or "stored-inventory"
    return "stored-inventory"


def build_crawler(name: str, is_form: bool, only_urls=None,
                  only_sources=None):
    """(crawler, regulator) for a form name or a source-config name.

    PUBLIC because `tools/workbook.py` needs the identical crawler in order to
    export to a workbook instead of the database. A second copy of this would
    drift — the workbook and the direct-write path would quietly crawl different
    things, and the workbook you approved would not be what the database got.
    """
    if is_form:
        from dynamic_crawler.formfill.pipeline import FormfillCrawler
        from dynamic_crawler.formfill.schema import load_hints
        path = REPO_ROOT / "dynamic_crawler" / "hints" / f"{name}.yml"
        lib = (load_hints(str(path)).get("library") or {})
        regulator = lib.get("regulator") or name
        return FormfillCrawler(str(path),
                               regulator=regulator,
                               source_system=lib.get("source_system") or name,
                               require_approved=False,
                               only_urls=only_urls), regulator

    import yaml
    from crawler.generic_crawler_wrapper import build_regulator_crawler
    cfg = yaml.safe_load(
        (REPO_ROOT / "config" / "sources" / f"{name}.yml").read_text(
            encoding="utf-8")) or {}
    # only_sources runs SOME of a regulator's sources -- LLOC's nightly job
    # wants its 40-second "Latest Legislation" window, not the 2,838-second
    # classification walk. See build_regulator_crawler for when narrowing is
    # safe: never where the narrowed sources share a source_system with the
    # ones left out.
    return (build_regulator_crawler(cfg, only_sources=only_sources),
            cfg.get("regulator", name.upper()))


def _crawl_into_db(name: str, is_form: bool, only_urls=None, timeout=14400,
                   only_sources=None) -> dict:
    """Crawl a source and write what it finds straight to MSSQL.

    The orchestrator classifies each document new / modified / unchanged against
    the stored rows, versions what changed, and builds the folder tree. `status`
    is left empty by `_set_status` — a person decides that.
    """
    from processor.downloader import Downloader
    from orchestrator.orchestrator import Orchestrator

    crawler, regulator = build_crawler(name, is_form, only_urls,
                                       only_sources=only_sources)
    orch = Orchestrator(crawler=crawler, repo=_repo(), downloader=Downloader(),
                        source_name=regulator)
    t0 = time.time()
    result = orch.run_for_regulator(regulator) or {}
    result["_seconds"] = round(time.time() - t0, 1)
    return result


# --------------------------------------------------------------------------- #
#  the jobs                                                                    #
# --------------------------------------------------------------------------- #

def monitor_cheap_probes() -> dict:
    """DAILY. Probe every source whose site answers honestly, crawl what moved.

    One HTTP request per stored document, about a minute for all six sources.
    A source with nothing changed costs exactly that and no crawl — which is the
    whole point of probing before crawling.
    """
    return _run_exclusive("monitor_cheap_probes", _monitor_cheap_probes_impl)


def _monitor_cheap_probes_impl() -> dict:
    out = {}
    state = REPO_ROOT / "output" / "monitor_targets"
    state.mkdir(parents=True, exist_ok=True)
    for regulator, source in CHEAP_PROBE_SOURCES:
        tf = state / ("".join(c if c.isalnum() else "_" for c in regulator)[:60]
                      + ".txt")
        rep = _sweep(regulator, source, tf)
        counts = rep.get("counts", {})
        targets = [l.strip() for l in
                   (tf.read_text(encoding="utf-8").splitlines()
                    if tf.exists() else []) if l.strip()]
        entry = {"counts": counts, "targets": len(targets),
                 "seconds": rep.get("_seconds")}
        # A crawl only when something actually moved. `new` on a detect-only
        # sweep means "first time this was swept", not a new document, so it
        # must NOT pull a crawl — that would re-read the whole source on its
        # first run.
        if targets:
            forms = _forms_for(regulator)
            if forms:
                # A regulator can have MORE THAN ONE form sharing one
                # (regulator, source_system) pair -- MOH (recent/archived) and
                # ZATCA (5 sub-forms) both do, and the sweep above probes them
                # as one source, so `targets` can mix urls from any of them.
                # Picking just the first form (the old behaviour) silently
                # dropped every target that belonged to a different form: that
                # form's own listing never contains another form's urls, so
                # `only_urls` would find nothing and the change went
                # unrecrawled with no error. Every matching form gets the same
                # target list instead; each one only opens the urls it
                # actually finds in its own listing, so this is safe even when
                # most targets belong to a sibling form.
                entry["crawl"] = {
                    f: _crawl_into_db(f, True, only_urls=targets) for f in forms
                }
            else:
                entry["crawl"] = {"skipped": "no crawler mapped"}
        out[regulator] = entry
        logger.info("%s: %s", regulator, entry)

    # MOH: no probe-then-crawl here. The crawl (crawler/moh_crawler.py, via
    # CRAWL_AS_SIGNAL) reads the site's own SharePoint API directly -- ~2
    # seconds for all 83 documents, cheaper than probing each one individually,
    # and each document carries its own change timestamp so the orchestrator's
    # new/modified/unchanged classification against the DB already does what
    # the probe step exists to do for the other five sources. Direct, every day.
    moh_rep = _crawl_into_db("moh", False)
    out["Ministry of Health"] = moh_rep
    logger.info("Ministry of Health: %s", moh_rep)

    return out


def monitor_sama() -> dict:
    """DAILY. SAMA's own revision page: one request instead of 6,101 probes.

    Also the only signal here that DISCOVERS — an entry matching nothing we hold
    is a document missing from the library, which a stored-inventory probe can
    never report because it only re-reads rows we already have.
    """
    return _run_exclusive("monitor_sama", _monitor_sama_impl)


def _monitor_sama_impl() -> dict:
    state = REPO_ROOT / "output" / "monitor_targets"
    state.mkdir(parents=True, exist_ok=True)
    tf = state / "SAMA.txt"
    rep = _sweep("Saudi Arabian Monetary Authority (SAMA)", "SAMA RULEBOOK", tf)
    out = {"counts": rep.get("counts", {}), "feed": rep.get("feed", {}),
           "seconds": rep.get("_seconds")}
    # Documents the feed named that the library does not hold.
    if (rep.get("feed") or {}).get("not_in_library"):
        p, dt = _run([sys.executable, "-B", "benchmarks/sama_feed_ingest.py"], 3600)
        out["discovery"] = {"rc": p.returncode, "seconds": round(dt, 1)}
    logger.info("SAMA: %s", out)
    return out


def monitor_mc() -> dict:
    """WEEKLY. The crawl is the signal — mc.gov.sa refuses plain HTTP clients.

    Measured 2026-08-15: requests.get is reset on every url, while a headless
    Chromium gets 200 on the same ones. So a probe can only ever answer
    `unknown` here, and re-crawling is the only way to see a change.
    """
    return _run_exclusive("monitor_mc", _monitor_mc_impl)


def _monitor_mc_impl() -> dict:
    res = _crawl_into_db("mc", False, timeout=5400)
    logger.info("Ministry of Commerce: %s", res)
    return res


def monitor_mlcu() -> dict:
    """WEEKLY, AND OFF. The crawl is the signal because nothing cheaper exists.

    Measured 2026-08-17: no ETag, no Last-Modified, no sitemap lastmod, and the
    `3145 اخبار` news page carries no dates and no document links, so it cannot
    say which instrument moved. Full measurements on the change_signals.yml entry.

    LEAVE THE SCHEDULER SLOT DISABLED until a person has read the workbook — this
    path writes straight to MSSQL, and MLCU has never been reviewed.
    """
    return _run_exclusive("monitor_mlcu", _monitor_mlcu_impl)


def _monitor_mlcu_impl() -> dict:
    # Five section pages plus 24 PDFs, serial. Small enough that a probe would
    # have saved almost nothing even if one had been possible.
    res = _crawl_into_db("mlcu", False, timeout=5400)
    logger.info("MLCU: %s", res)
    return res


def monitor_cma() -> dict:
    """WEEKLY. The crawl is the signal, and it must NOT walk the whole history.

    CMA cannot be probed (its Last-Modified is the current time) and cannot be
    confirmed at scale (a confirm is a full page fetch and the host throttles
    after ~60 of 1,979).

    THE ANNOUNCEMENTS TAB IS THE TRAP. It is 3,299 items over 550 pages, and a
    full walk measured 2h49m on 2026-08-16 and still came back with 300 of the
    1,053 announcements we already hold — reported as a clean run. A short crawl
    is worse than none here: the 753 it missed would be ruled `disappeared` and
    become withdrawal proposals.

    Announcements are ordered NEWEST FIRST, so monitoring does not need the
    history at all — only back as far as the newest one already stored. That is
    what `since_days` on the announcements tab is for
    (site_runners/cma_laws.py); it is currently None for the one-off backfill and
    MUST be a small window here. Set CMA_SINCE_DAYS to control it.
    """
    return _run_exclusive("monitor_cma", _monitor_cma_impl)


def _monitor_cma_impl() -> dict:
    days = os.getenv("CMA_SINCE_DAYS", "30")
    os.environ["CMA_SINCE_DAYS"] = days      # read by the CMA runner
    res = _crawl_into_db("cma", False, timeout=14400)
    res["announcements_window_days"] = days
    logger.info("CMA: %s", res)
    return res


def monitor_cbe() -> dict:
    """WEEKLY. Twelve sources: the circulars API, plus eleven section crawls.

    THE CIRCULARS HALF IS THE CHEAP, HONEST SIGNAL and it also DISCOVERS.
    `crawler/cbe_crawler.py` reads /api/listing/circulars in one request and gets
    all 396 with a publication date and a Sitecore GUID each, so the orchestrator's
    new/modified/unchanged classification against the DB already does everything a
    probe step would. It refuses to return a partial inventory rather than let a
    short list read downstream as documents having disappeared.

    THE OTHER ELEVEN ARE BROWSER CRAWLS, which is what makes this weekly. Measured
    2026-08-18 over the nine sections then configured: 100 pages, 152 documents.
    `Regulations Book` is the big one at 143 sitemap urls and is capped at 250.

    WHY THE PACING MATTERS HERE MORE THAN USUAL. cbe.org.eg runs bot protection —
    it already refuses `urllib` outright and answers HEAD with 403. Both hosts in
    `skip_hosts` were blocked by automated access from this address, and SIMAH's
    note records that it was "triggered by repeated iteration, not volume".
    Eleven prefix crawls is real iteration. Weekly, and never in a retry loop.

    A SHORT CRAWL IS THE DANGER, not a slow one — the CMA lesson. A section that
    hits its page cap or times out returns fewer documents than are stored, and
    absent documents are ruled `disappeared`. The orchestrator's completeness gate
    is what stands between that and a withdrawal proposal, and it is keyed per
    source, which is exactly why cbe.yml splits the sections rather than crawling
    /en/laws-regulations as one. Give this job room rather than a tight timeout.
    """
    return _run_exclusive("monitor_cbe", _monitor_cbe_impl)


def _monitor_cbe_impl() -> dict:
    # 10800s = 3 hours. Deliberately generous: CMA's job was killed at 5400s
    # after real work and produced nothing, and a killed run is worse than a slow
    # one because it looks like a source that returned nothing.
    res = _crawl_into_db("cbe", False, timeout=10800)
    logger.info("Central Bank of Egypt: %s", res)
    return res


def monitor_bahrain_bourse() -> dict:
    """WEEKLY, AND OFF. The crawl is the signal, and it is cheap.

    crawler/bahrain_bourse_crawler.py reads the site's own GetFaq API in one
    ~20 KB request and gets all seven Legal Framework sections — Laws, Rules &
    Regulations, Resolutions, Guidelines, Circulars, CBB Rules & Regulations,
    Consultation — so the orchestrator's new/modified/unchanged classification
    against the DB already does everything a probe step would. It refuses to
    return a partial inventory (fewer than MIN_EXPECTED_DOCS, or fewer
    sections than MIN_EXPECTED_SECTIONS) rather than let a short read appear
    downstream as documents having disappeared.

    LEAVE THE SCHEDULER SLOT DISABLED until a person has read the workbook —
    this path writes straight to MSSQL, and Bahrain Bourse has never been
    reviewed. Also: Bahrain Bourse is an exchange, and the last exchange this
    project crawled (saudiexchange.sa) was permanently blocked within two
    hours of ordinary automated access. See docs/HANDOFF_bahrain_bourse.md
    before turning this on or re-measuring anything by hand.
    """
    return _run_exclusive("monitor_bahrain_bourse", _monitor_bahrain_bourse_impl)


def _monitor_bahrain_bourse_impl() -> dict:
    # 5400s = 1.5h, matching MLCU's slot: a similarly small document count (86
    # vs MLCU's 24 PDFs + 5 pages) with OCR on any new/changed PDF.
    res = _crawl_into_db("bahrain_bourse", False, timeout=5400)
    logger.info("Bahrain Bourse: %s", res)
    return res


def monitor_cbb() -> dict:
    """WEEKLY, AND OFF. Seven sources, migrated onto the config flow 2026-08-20.

    THIS REPLACES `cbb_monitoring`, which is retired. That job ran the bespoke
    crawler/cbb_monitoring_crawler.py straight into MSSQL with no workbook step
    and no completeness gate, and it was the only monitoring job in the repo left
    `enabled: true`. MEASURED 2026-08-20 before the migration: 0 CBB rows in the
    database, 0 `CBB-*` source_systems, 0 CBB entries in run_history. A job that
    has been enabled and has never produced a row is not monitoring anything, and
    nothing about its output said so.

    THE REAL SIGNAL IS A NATIVE REVISION FEED, and it should be wired here rather
    than re-crawling seven sections blind:

        https://cbben.thomsonreuters.com/view-revision-updates

    the regulator's own "what changed in this date range" page, already read by
    site_runners/cbb_updates.py. SAMA runs on the SAME Thomson Reuters platform
    with the same endpoint (dynamic_crawler/sama_feed_signal.py) — two regulators,
    one pattern, and the best class of signal in the repo: no probing, no stale
    stamp to be fooled by.

    IT CANNOT SEE DELETIONS. A withdrawn document stops appearing rather than
    saying it went; absence is only visible to a full crawl. So the feed makes the
    crawl RARE, not unnecessary, and `disappeared` still comes from the crawl.

    WHY IT STAYS OFF: two reasons, and the second outlives the first.
      1. Nobody has read a CBB workbook. This path writes straight to MSSQL.
      2. Every monitor job lives in DIRECT_JOB_MAPPING, and the scheduler defaults
         to EXECUTION_MODE=API, which reads API_JOB_MAPPING — where no monitor job
         exists and no endpoint backs one. Enabling this today logs
         "No function mapped for job" and does nothing. Unresolved repo-wide.
    """
    return _run_exclusive("monitor_cbb", _monitor_cbb_impl)


def _monitor_cbb_impl() -> dict:
    # 10800s = 3 hours, matching CBE. Mode 2c walks the whole rulebook sidebar and
    # mode 1 fetches Thomson Reuters resolution pages one at a time; neither is
    # quick, and a killed run reads downstream as a source that returned nothing.
    res = _crawl_into_db("cbb", False, timeout=10800)
    logger.info("Central Bank of Bahrain: %s", res)
    return res


def monitor_rera() -> dict:
    """WEEKLY. Eight small section crawls of rera.gov.bh.

    THE CRAWL IS THE SIGNAL BECAUSE DISCOVERY IS, not because a probe fails. RERA
    answers a probe better than almost anything we hold: 117 of 121 stored urls
    return both an ETag and a Last-Modified, and they are stable (8/8 identical
    when fetched twice 0.4s apart). But circulars are partitioned by year, one
    page per year, and a new year is a NEW PAGE — Circulars-issued-in-2026 is a
    404 today. A probe re-reads what we already store, so it can never see that.

    WHY IT IS CHEAP ANYWAY. RERA is small: 15 pages and 123 documents in the
    measured crawl, all server-rendered, no pager, no JS data source, no WAF. This
    is nothing like the CMA walk that takes 2h49m.

    THE 2026 PAGE IS THE THING TO WATCH. `config/sources/rera.yml` seeds the
    circulars source at the PARENT so prefix scope picks up a new year page the
    first time it exists, with no config change. If you ever hand-check it, try
    BOTH spellings: RERA writes `circulars-issued-in-2020` lower case and
    `Circulars-issued-in-2024` capitalised.

    EXPECT A FEW `unknown` AND DO NOT CHASE THEM. Four stored CloudFront urls are
    dead (403 in a browser too — a library problem, not a sweep one), and the two
    documents hosted on rera.gov.bh itself cannot be fetched by a plain HTTP
    client at all: the host omits its intermediate CA, so requests raises
    CERTIFICATE_VERIFY_FAILED where a browser is fine. The crawl is unaffected —
    Playwright runs with ignore_https_errors.
    """
    return _run_exclusive("monitor_rera", _monitor_rera_impl)


def _monitor_rera_impl() -> dict:
    res = _crawl_into_db("rera", False, timeout=5400)
    logger.info("Real Estate Regulatory Authority: %s", res)
    return res


def monitor_sio() -> dict:
    """WEEKLY. Bahrain's Social Insurance Organisation, both sectors.

    THE CRAWL IS THE SIGNAL BECAUSE NOTHING ELSE ANSWERS. Measured 2026-08-25:
    no ETag, no Last-Modified, not even a Content-Length on any sio.gov.bh page,
    so `stored-inventory` has nothing to read; and /sitemap.xml carries a
    <lastmod> on all 106 urls with ONE distinct value — its own build time — so
    the sitemap adapter refuses it by its own gate.

    WHY IT IS CHEAP ANYWAY. Every law is a Bootstrap modal already in the DOM, so
    a section's ~48 laws come from ONE page load: ten page loads and roughly five
    minutes for all 214 documents. That is cheaper than the probe loop it
    replaces would have been.

    ALL TEN SOURCES MUST RUN TOGETHER. `disappeared` is scoped by
    (regulator, source_system) and sio.yml stores just two — "Private Sectors"
    and "Public Sectors" — so the five sources of a sector share one bucket. A
    run that covered only some of them would have the others' documents absent
    from a run that still claims the sector, and only the completeness gate
    between that and a withdrawal proposal. Hence no `only_sources` here.
    """
    rep = _crawl_into_db("sio", False)
    logger.info("Social Insurance Organisation (SIO): %s", rep)
    return {"Social Insurance Organisation (SIO)": rep}


def monitor_lloc() -> dict:
    """DAILY. Bahrain's LLOC, the `Latest Legislation` window ONLY.

    WHY NARROWED. config/sources/lloc.yml holds four sources and they are not the
    same kind of thing. Latest is 144 records in 15 requests (~40s) and is where
    new Bahraini legislation appears first, with its Official Gazette number.
    `Legislation By Classification` is 1,583 documents over 2,838 SECONDS
    measured — coverage, not a signal. Dragging it into a nightly job would make
    a 40-second question take 47 minutes.

    WHY `only_sources` AND NOT A SECOND CONFIG. build_crawler is public so the
    workbook path and this path build the same crawler; a `lloc.latest.yml` would
    be the second copy its own docstring warns drifts. One config, one source
    list, narrowed at the call.

    A NAME THAT MATCHES NOTHING RAISES rather than monitoring zero sources — so
    renaming the source in the yml breaks this loudly instead of silently.

    THE CLASSIFICATIONS STILL NEED RUNNING, by hand or on a slow cadence:
        python -m tools.workbook export lloc
    They are not watched by anything today, and that is a deliberate gap, not an
    oversight.

    THE HOST THROTTLES WITH 404. lloc.gov.bh answers a burst with a 1,245-byte
    IIS 404 that parses as an empty page; crawler/lloc_crawler.py holds the retry
    budget for it. Do not schedule this alongside another lloc job.
    """
    rep = _crawl_into_db("lloc", False, only_sources=["Latest Legislation"])
    logger.info("LLOC (Latest Legislation): %s", rep)
    return {"Legislation and Legal Opinion Commission (LLOC)": rep}


def monitor_pdpa() -> dict:
    """WEEKLY. Bahrain's PDPA, both sources — TWO page loads for the whole thing.

    THE CRAWL IS THE SIGNAL, and unusually for this list it is also the cheapest
    question available rather than the only one left. Both sources are
    `max_pages: 1`: 60 articles are one page's accordion panels and 20 executive
    decisions are one page's links, so a full re-crawl costs two GETs. Nothing a
    probe loop could ask is cheaper than that, and the crawl already returns 82
    rows with 82 distinct content_hash values.

    WHAT WAS RULED OUT, measured 2026-08-31:

      sitemap           does not exist. /sitemap.xml, /sitemap_index.xml and
                        /en/sitemap.xml all 403 in 111 bytes from AmazonS3, and so
                        does a control path that cannot exist — S3's answer for a
                        missing key. Not a block: the page itself answers 200 in
                        163,302 bytes to a plain request.
      stored-inventory  available but wrong twice over. On "The Law", 60 of 62
                        rows are anchors into ONE page and share one ETag, so it
                        would report all 60 articles modified on any edit to that
                        page. On "Executive Decisions/Orders" the 20 pdfs each
                        have their own honest ETag, but InventorySweep is
                        covers_inventory = False — it re-reads only urls we
                        already store and can never see a NEW decision, which is
                        the main event on that source.
      snapshot-articles right shape, wrong plumbing. The page does parse into 60
                        labelled articles with 60 distinct hashes — but only with
                        Bootstrap 4 class names, which snapshot_sweep() does not
                        thread through, and it reads a snapshot from formfill's
                        store because it exists for SIMAH, a host we cannot crawl
                        at all. We can crawl this one, in one request.

    BOTH SOURCES RUN TOGETHER, and here that is free rather than a constraint:
    pdpa.yml stores one source_system per source, so each has its own
    `disappeared` bucket and neither can strand the other.

    KNOWN GAP: a pdf silently replaced at the same url with the same link text is
    invisible. A discovered document's content_hash is hash("<url>|<title>")
    (generic_crawler/crawler.py:3476); the ETag-reading stamp_declared() path runs
    only for --documents entries. See config/change_signals.yml.
    """
    rep = _crawl_into_db("pdpa", False)
    logger.info("Personal Data Protection Authority (PDPA): %s", rep)
    return {"Personal Data Protection Authority (PDPA)": rep}


def monitor_moic() -> dict:
    """WEEKLY. Bahrain's Ministry of Industry and Commerce, all three sources.

    THE CRAWL IS THE SIGNAL, and here it is also the cheap one. Eleven page loads
    for 95 documents: Commerce and Industry are one filtered listing each
    (`?about[0]=19` / `=20`, a clean 74 + 4 partition of the unfiltered 78), and
    Forms is the index plus the eight sidebar categories it discovers by walking.

    WHY NOT stored-inventory, WHICH GENUINELY WORKS HERE. MOIC's tokens are
    honest, unlike PDPA's: every sampled pdf returns a stable Apache size-mtime
    ETag and 90 of 95 rows are distinct pdfs. It loses twice over — a probe is one
    request per document, so 90 against 11; and InventorySweep is
    covers_inventory = False, so it re-reads only what we already hold and cannot
    see a new regulation, a new form, or a new form CATEGORY.

    RULED OUT, measured 2026-09-01: no sitemap (all three paths 404 with the
    site's 44 KB catch-all, as does a control, and robots.txt names none); no
    revision feed (/en/news is press releases — one document link on the page and
    no mention of a decree, resolution or order); snapshot-articles parses 0 items
    because these rows are files, not article text.

    ALL THREE SOURCES RUN TOGETHER. `disappeared` is scoped by
    (regulator, source_system) and moic.yml stores three, so each has its own
    bucket and none can strand another. No `only_sources` here.

    KNOWN GAP: a pdf silently replaced at the same url with the same link text is
    invisible — a discovered document's content_hash is hash("<url>|<title>").
    A deliberate `--signal stored-inventory` run is the thing that catches it, and
    it is worth doing occasionally. Expect 5 false `modified` on the Forms
    placeholders if you do: they point at tag pages whose gzip ETag moves.
    """
    rep = _crawl_into_db("moic", False)
    logger.info("Ministry of Industry and Commerce (MOIC): %s", rep)
    return {"Ministry of Industry and Commerce (MOIC)": rep}


def monitor_cbj() -> dict:
    """WEEKLY. The Central Bank of Jordan, all ten Legislation sources.

    THE CRAWL IS THE SIGNAL, and like PDPA and MOIC it is also the cheapest
    question available rather than the only one left. Every source in
    config/sources/cbj.yml is `max_pages: 1`, so the whole regulator is TEN page
    loads for 402 rows: CBJ's pagers and its `ddlCategory1` dropdowns are
    DISPLAY ONLY -- the unfiltered markup already carries every document link,
    measured on the 140-document Payment Systems listing across its six pages.

    WHY NOT stored-inventory, WHICH GENUINELY WORKS HERE. CBJ's tokens are
    honest: every sampled pdf returns a real IIS ETag and a distinct, plausible
    Last-Modified (2025-03-25, 2025-12-17, 2026-03-01), not CMA's current-time
    lie. It loses twice over -- a probe is one request per document, so 376
    against ten; and InventorySweep is covers_inventory = False, so it re-reads
    only urls we already hold and can never see a NEW circular. 376 rather than
    402 because 23 multi-attachment rows carry no document_url by design and 3
    are placeholders, so 26 rows cannot be probed by anything.

    RULED OUT, measured 2026-09-16: no sitemap (/sitemap.xml and
    /sitemap_index.xml 404; /EN/sitemap.xml answers 200 at the same 385,447
    bytes as a control path that cannot exist, so it is the catch-all page;
    robots.txt is 404); no revision feed anywhere in the Legislation menu;
    snapshot-articles parses 0 items because these rows are files, not article
    text.

    ALL TEN SOURCES RUN TOGETHER, and here that is a CONSTRAINT rather than a
    convenience. `disappeared` is scoped by (regulator, source_system) and
    cbj.yml gives all ten `source_system: "Legislation"` so the library nests
    them under one parent folder -- so they share ONE bucket and cannot be gated
    apart. cbj.yml states that trade where it is made.

    KNOWN GAP: a pdf silently replaced at the same url with the same link text is
    invisible -- a discovered document's content_hash is hash("<url>|<title>").
    The honest ETags above are what catches that, so a deliberate
    `--signal stored-inventory` run against a slice is worth doing occasionally.
    Expect 3 rows it cannot answer for: the Instructions, Jordanian Constitution
    and AML/CFT placeholders point at pages that publish no file at all.
    """
    rep = _crawl_into_db("cbj", False)
    logger.info("Central Bank of Jordan (CBJ): %s", rep)
    return {"Central Bank of Jordan (CBJ)": rep}



def _forms_for(regulator: str) -> list:
    """Every hints form that crawls this regulator, sorted for determinism.

    A regulator is not always one form: MOH is split recent/archived and ZATCA
    is split five ways (taxes, agreements, and three Information Exchange
    Portal sub-forms), all sharing one (regulator, source_system) pair in
    change_signals.yml because that is what the sweep probes as one source.
    Returning only the first match (the old behaviour) meant a probe could
    detect a change and then hand it to the wrong form to re-crawl, which
    finds nothing in its own listing and drops the change silently.
    """
    from dynamic_crawler.formfill.schema import load_hints
    hints = REPO_ROOT / "dynamic_crawler" / "hints"
    forms = []
    for p in sorted(hints.glob("*.yml")):
        try:
            lib = (load_hints(str(p)).get("library") or {})
        except Exception:
            continue
        if lib.get("regulator") == regulator:
            forms.append(p.stem)
    return forms


__all__ = ["monitor_cheap_probes", "monitor_sama", "monitor_mc", "monitor_cma",
           "monitor_cbe", "monitor_bahrain_bourse"]
