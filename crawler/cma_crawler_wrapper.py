"""CMACrawler — the CMA site runner behind the standard crawler interface.

WHY A WRAPPER RATHER THAN A `mode: generic` SOURCE

`site_runners/cma_laws.py` is not a link walk. CMA publishes nine tabs in six
different page shapes — a chapter-structured law, single pages, card grids,
grouped cards, an iframe, sub-tabbed paginated lists with detail pages, and a
paginated FAQ — and the runner has a handler per shape. Pointing the generic
crawler at the landing page collects the easy ones and silently under-collects
the rest, which is worse than not running it: the completeness gate would take
that undercount as its baseline and never report the gap.

So the shapes stay where they are. This class only adapts the runner's output to
the interface `build_regulator_crawler(mode: custom)` expects, the same way
`crawler/sama_crawler_wrapper.py` does for SAMA.

WHAT IT DOES NOT DO

It does not re-implement, re-scope or "improve" any handler. If a tab is wrong,
it is wrong in `cma_laws.py` and should be fixed there.

Media Center announcements (3,297 over 550 pages) are excluded by default. That
tab is the one shape whose list is NOT held in the DOM, so it pages the site
hundreds of times; it is opt-in via `tabs=[...]` rather than something a routine
crawl pays for.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import contextmanager
from typing import Dict, List, Optional

from playwright.sync_api import sync_playwright

from models.models import RegulatoryDocument
from site_runners import cma_laws
from dynamic_crawler.formfill.runner import stable_url
from crawler.fingerprint import stamp_content_hashes
from utils.event_loop import proactor_event_loop

logger = logging.getLogger(__name__)

#: Laws & Regulations only. Everything else CMA publishes is reachable by
#: naming it in `tabs`, but this is the set that matches the regulator scope.
DEFAULT_TABS = [
    "capital_market_law",
    "sifi",
    "forms",
    "cpe",
    "guides",
    "circulars",
    "public_consultation",
    "implementing_regulations",
    "faqs",
]

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def _scrub_urls(doc):
    """Strip per-request parameters from every url a document carries.

    Applied where the document LEAVES this wrapper, so no code path can bypass
    it. The earlier fix caught the paged branch only, and 4 rows still reached
    the library with `csrt=<token>` on them — CMA's CSRF token, which changes per
    session and would make a multi-attachment row look new on every crawl.
    """
    for attr in ("document_url", "source_page_url"):
        v = getattr(doc, attr, None)
        if isinstance(v, str) and v:
            setattr(doc, attr, stable_url(v))
    meta = getattr(doc, "extra_meta", None)
    if isinstance(meta, dict):
        for k, v in list(meta.items()):
            if isinstance(v, str) and ("http" in v):
                meta[k] = " | ".join(stable_url(x.strip())
                                     for x in v.split("|") if x.strip())
    return doc


# Shared with the SIMAH / Saudi Exchange replay; see utils/event_loop.py for why.
_proactor_event_loop = proactor_event_loop


def _as_doc_list(tab_docs) -> List[dict]:
    """Normalise what `crawl_tab` hands back.

    The shape handlers return `list(documents.values())` — a list of dicts, each
    carrying doc_url / title / section_path / type / found_on. But two early
    error paths return the bare `documents` dict instead, and one returns `[]`.
    Assuming the dict form cost a 272-second CMA run that reported 0 documents
    and PASSED the completeness gate, because an exception per tab was caught
    and logged as "tab failed" rather than raised.
    """
    if not tab_docs:
        return []
    if isinstance(tab_docs, dict):
        return [v for v in tab_docs.values() if isinstance(v, dict)]
    return [d for d in tab_docs if isinstance(d, dict)]


class CMACrawler:
    """Runs the CMA site runner's implemented tabs and returns
    RegulatoryDocument objects."""

    def __init__(
        self,
        headless: bool = True,
        tabs: Optional[List[str]] = None,
        source_system: str = "CMA-RULES",
        regulator: str = "CMA",
        delay_ms: int = 600,
        max_articles: Optional[int] = None,
        max_chapters: Optional[int] = None,
    ):
        self.headless = headless
        self.regulator = regulator
        self.source_system = source_system
        # CMA throttles. The runner's own note: 600-1000ms finishes SOONER than
        # 0, because backing off avoids the throttle it would otherwise trip.
        self.delay_ms = delay_ms
        self.max_articles = max_articles
        self.max_chapters = max_chapters

        requested = tabs or DEFAULT_TABS
        unknown = [t for t in requested if t not in cma_laws.TABS]
        if unknown:
            raise ValueError(
                f"unknown CMA tab(s) {unknown}. Known: {sorted(cma_laws.TABS)}")
        # A tab whose shape has no handler yet would crawl nothing and report
        # success, so drop it loudly instead.
        self.tabs = []
        for t in requested:
            shape = cma_laws.TABS[t].get("shape")
            if shape in cma_laws.IMPLEMENTED:
                self.tabs.append(t)
            else:
                logger.warning("CMA tab %r has shape %r with no handler — skipped",
                               t, shape)

        self.last_result: Dict = {}
        logger.info("Initialized CMACrawler (headless=%s, tabs=%d)",
                    headless, len(self.tabs))

    # ------------------------------------------------------------------ #

    @property
    def source_names(self) -> List[str]:
        """One label per tab, so the completeness gate can size each tab
        against its own history rather than against CMA as a whole. Without
        this a tab dying entirely hides inside the total's tolerance."""
        return [cma_laws.TABS[t]["label"] for t in self.tabs]

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        """Walk every implemented tab and return the documents found.

        `limit` accepts an int (documents overall) or a dict keyed by tab, to
        match the loose convention the other crawlers use.
        """
        overall_cap = limit if isinstance(limit, int) and limit > 0 else None
        per_tab = limit if isinstance(limit, dict) else {}

        docs: List[RegulatoryDocument] = []
        seen = set()
        per_tab_counts: Dict[str, int] = {}
        warnings: List[str] = []
        failed_tabs: List[str] = []
        coverage_gaps: List[dict] = []
        cma_laws.GAPS.clear()

        cma_laws.PACE["detail_ms"] = self.delay_ms
        cma_laws.PACE["page_ms"] = self.delay_ms // 2

        with _proactor_event_loop(), sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=self.headless,
                args=["--disable-dev-shm-usage", "--disable-gpu"])
            ctx = browser.new_context(user_agent=USER_AGENT, locale="en-US",
                                      viewport={"width": 1600, "height": 1000})
            try:
                for tab_key in self.tabs:
                    label = cma_laws.TABS[tab_key]["label"]
                    try:
                        _records, tab_docs = cma_laws.crawl_tab(
                            ctx, tab_key,
                            per_tab.get(tab_key) or self.max_chapters,
                            per_tab.get(tab_key) or self.max_articles)
                    except Exception as e:
                        # One tab failing must not lose the other eight. It is
                        # recorded so the gate sees an incomplete run rather
                        # than a short one.
                        msg = f"CMA tab {label!r} failed: {type(e).__name__}: {e}"
                        logger.error("  %-44s FAILED  %s", label,
                                     f"{type(e).__name__}: {e}"[:70])
                        warnings.append(msg)
                        failed_tabs.append(label)
                        per_tab_counts[label] = 0
                        continue

                    # A COVERAGE GAP IS A FAILED RUN, NOT A LOG LINE.
                    #
                    # The runner reports it when a tab returns fewer rows than the
                    # site's own list says it holds (Prospectuses: 81 of 266, with
                    # every paged tab stopping after one chunk on 2026-09-18) --
                    # but it only PRINTED it, so the run was called trustworthy and
                    # the missing 186 documents were reported as `disappeared`. The
                    # gate reads run.warnings, and matches "coverage gap" there, so
                    # this is what makes the run untrusted: new and modified rows
                    # are still ingested, but absences are not acted on and the
                    # short count is never remembered as a baseline.
                    for g in [g for g in cma_laws.GAPS if g.get("tab") == label]:
                        coverage_gaps.append(g)
                        got, want = g.get("rows"), g.get("source_list_count")
                        detail = (f"read {got} of {want}" if got is not None and want
                                  else g.get("why", "incomplete"))
                        warnings.append(f"coverage gap: {label!r} {detail} "
                                        f"({g.get('why', '')})")
                        logger.error("  %-44s COVERAGE GAP  %s", label, detail)

                    before = len(docs)
                    for d in _as_doc_list(tab_docs):
                        # PER-REQUEST PARAMETERS STRIPPED BEFORE ANYTHING ELSE.
                        #
                        # CMA appends `csrt=<digits>` (a CSRF token) and a
                        # literal `undefined=undefined` from a bug in its own
                        # page. A row whose document_url is empty is identified
                        # by doc_path + attachment_links, so a token that moves
                        # per session makes the row look new on every crawl and
                        # inserts it again. Measured 2026-08-16: 2 of CMA's 69
                        # empty-url rows carry one. Ministry of Commerce had the
                        # same fault with `dt=` and duplicated all 16 of its
                        # attachment rows before it was found.
                        href = stable_url((d.get("doc_url") or "").strip())
                        section_path = d.get("section_path") or ""
                        attachment_links = " | ".join(
                            stable_url(x.strip())
                            for x in str(d.get("attachment_links") or "").split("|")
                            if x.strip())
                        # A multi-attachment row DELIBERATELY carries an empty
                        # document_url — see models.RegulatoryDocument's own
                        # docstring: "document_url IS LEFT EMPTY" when the
                        # files live in extra_meta["attachment_links"] instead.
                        # Requiring a non-empty href here silently dropped
                        # every such row (Forms' one page with 10 attachments,
                        # multi-file Public Consultation topics). Only a row
                        # with NEITHER an href NOR any attachment is the real
                        # "nothing to identify this by" case worth dropping.
                        if not href and not attachment_links:
                            continue
                        # Dedup on (url, section_path), matching the identity
                        # convention this whole system uses -- (document_url,
                        # doc_path). URL alone silently dropped every register
                        # part past the first: Financial Market Institutions'
                        # two parts (Licensed Institutions, Credit Rating
                        # Agencies) share the tab's one landing-page url and
                        # differ only by section_path, so the second was read
                        # as "already seen" and thrown away. When href itself
                        # is empty (multi-attachment rows), fall back to the
                        # attachment set so two different empty-url rows in
                        # the same section still dedup correctly.
                        dedup_key = (href or attachment_links, section_path)
                        if dedup_key in seen:
                            continue
                        seen.add(dedup_key)
                        docs.append(self._to_document(
                            d, href, section_path, label))
                        if overall_cap and len(docs) >= overall_cap:
                            break
                    per_tab_counts[label] = len(docs) - before
                    logger.info("  %-44s %4d document(s)   (running total %d)",
                                label, per_tab_counts[label], len(docs))
                    if overall_cap and len(docs) >= overall_cap:
                        break
            finally:
                try:
                    browser.close()
                except Exception:
                    pass

        # The shape the completeness gate reads.
        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": per_tab_counts,
            "failed_tabs": failed_tabs,
            "coverage_gaps": coverage_gaps,
        }

        # EVERY tab failing is a broken crawler, not a regulator with no
        # documents — but both return an empty list, and the completeness gate
        # cannot tell them apart. It would take the zero as a valid baseline.
        #
        # This is not hypothetical: a wrong assumption about crawl_tab's return
        # type made all nine tabs raise, and the run reported 0 documents and
        # gate=PASS after 272 seconds. cma_laws.py warns about exactly this:
        # "a tab that quietly returns nothing looks exactly like a tab with no
        # documents, and that is the failure mode this whole project keeps
        # tripping over."
        if failed_tabs and len(failed_tabs) == len(self.tabs):
            raise RuntimeError(
                f"every CMA tab failed ({len(failed_tabs)}/{len(self.tabs)}). "
                f"This is a crawler fault, not an empty regulator. "
                f"First error: {warnings[0] if warnings else 'unknown'}")

        # THE SAME FAULT THROUGH A QUIETER DOOR.
        #
        # The guard above counts tabs that RAISED. It does not catch the case
        # where every handler returns empty without raising — and that is the
        # normal way these handlers fail. `load()` retries three times and then
        # returns False, and each handler responds by printing
        # {"event": "error", "message": "tab page did not load"} and returning
        # `records, []`. Nothing propagates.
        #
        # Measured 2026-08-12T00:43: nine tabs, zero exceptions, zero documents,
        # verdict PASS. From the wrapper's side that was indistinguishable from a
        # regulator that publishes nothing, and the completeness gate took the
        # zero as a valid baseline — exactly the outcome the guard above exists
        # to prevent.
        #
        # CMA publishes hundreds of documents across these nine tabs. Zero is
        # never a true answer here, so it is reported as a fault. Reading the
        # runner's own error lines is the way to see WHY (site unreachable,
        # throttled, markup changed).
        if self.tabs and not docs:
            raise RuntimeError(
                f"CMA returned 0 documents across all {len(self.tabs)} tab(s) "
                f"with no exception raised. The handlers return empty rather "
                f"than raising when a page fails to load, so this is a failed "
                f"run, not an empty regulator. Check the runner's "
                f'\'"event": "error"\' lines for the cause.')

        # No tabs at all is the third way to reach a clean zero: every requested
        # tab having an unimplemented shape leaves `self.tabs` empty, the loop
        # never runs, and neither guard above applies.
        if not self.tabs:
            raise RuntimeError(
                "no CMA tab had an implemented handler, so nothing was crawled. "
                "This would otherwise report 0 documents and PASS.")

        logger.info("CMACrawler finished: %d document(s) across %d tab(s)%s",
                    len(docs), len(per_tab_counts),
                    f", {len(failed_tabs)} tab(s) FAILED" if failed_tabs else "")
        # Every document leaves through here, so this is the one place a url
        # can be normalised without a code path bypassing it. The fingerprint is
        # stamped AFTER scrubbing, so it hashes the cleaned url and does not move
        # when a tracking parameter does. All 1,979 stored CMA rows had no
        # fingerprint before this — see crawler/fingerprint.py.
        return stamp_content_hashes(_scrub_urls(d) for d in docs)

    # ------------------------------------------------------------------ #

    def _to_document(self, d: dict, href: str, section_path: str,
                     tab_label: str) -> RegulatoryDocument:
        """One runner document -> one RegulatoryDocument.

        `section_path` is the runner's own trail, already rooted at the tab, so
        it becomes doc_path directly. The orchestrator builds the folder tree
        from that list.
        """
        trail = [p.strip() for p in (section_path or "").split(">") if p.strip()]
        if not trail:
            trail = [tab_label]

        # Some shapes (Announcements, Capital Market Law articles, FAQs) have
        # no downloadable file at all — the page's own text IS the document.
        # extra_meta["content_text"] is the key orchestrator.py's Tier 1b
        # reads before trying to download+extract anything, so this is what
        # lets those rows skip a fetch that would otherwise 404 or re-pull the
        # tab's landing page.
        extra_meta = {
            "crawl_source": tab_label,
            "found_on": d.get("found_on", ""),
            "doc_type": d.get("type", ""),
        }
        content_text = d.get("content_text")
        if content_text:
            extra_meta["content_text"] = content_text
        # single_page tabs (SIFI, Forms, CPE): the page is the one document,
        # and whatever PDFs it links are attachments, not separate documents —
        # see crawl_single_page(). Matches the multi-attachment convention in
        # models.RegulatoryDocument (extra_meta["attachment_links"]).
        attachment_links = d.get("attachment_links")
        if attachment_links:
            extra_meta["attachment_links"] = attachment_links
        if not href and attachment_links:
            # document_url is deliberately empty here (see the docstring in
            # models.RegulatoryDocument), so the default identity
            # (document_url, doc_path) would collapse every multi-attachment
            # row in one folder onto the same ("", doc_path) key. Declare the
            # per-document override the model already expects.
            # `title` included for the same reason it is in the default
            # identity — see changesignal.DEFAULT_IDENTITY (lead, 2026-08-16).
            extra_meta["identity_fields"] = [
                "doc_path", "extra_meta.attachment_links", "title"]

        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=trail[0] if trail else tab_label,
            title=(d.get("title") or "").strip() or href.rsplit("/", 1)[-1],
            document_url=href,
            doc_path=trail,
            published_date=d.get("published_date"),
            reference_no=d.get("reference_no"),
            document_html=d.get("content_html") or None,
            extra_meta=extra_meta,
        )
