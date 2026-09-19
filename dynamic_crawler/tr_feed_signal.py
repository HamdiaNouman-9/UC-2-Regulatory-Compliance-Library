"""The Thomson Reuters rulebook revision feed, as a change signal.

    <base>/view-revision-updates
      ?f_date=on&changed_1[min]=YYYY-MM-DD&changed_1[max]=YYYY-MM-DD&items_per_page=40

Plain GET, no auth, no browser. It filters on Drupal's `changed` timestamp, so
it answers "what did this regulator touch between these dates" directly.

WHY A SECOND MODULE RATHER THAN A PARAMETER ON sama_feed_signal.py
------------------------------------------------------------------
Three regulators in this library run on the same Thomson Reuters platform and
publish this same view:

    rulebook.sama.gov.sa        SAMA   -> dynamic_crawler/sama_feed_signal.py
    cbben.thomsonreuters.com    CBB    -> not wired yet (jobs/monitor_jobs.py:604)
    qfcra-en.thomsonreuters.com QFCL   -> this module

That is an argument for sharing, and eventually one module should serve all
three. It is NOT an argument for editing SAMA's module today: SAMA is a live,
scheduled signal over 6,101 documents, and the deployments differ in ways that
would have to be branched inside it anyway (below). So this is a copy, and
migrating SAMA and CBB onto it is a separate change with its own evidence.

WHAT ACTUALLY DIFFERS FROM THE SAMA DEPLOYMENT
----------------------------------------------
1. THE ENTRY MARKUP. SAMA renders the row counter inside the detail div and the
   date as bare text:

       <div class="book-detail"> 12 . <a href="/en/slug">Title</a> (30 June 2026)

   QFCRA puts the counter in its own field and the date in a <time> element:

       <div class="views-field views-field-counter">…1 -…</div>
       <div class="book-detail"><a href="/rulebook/slug">Title</a>
         (<time datetime="2026-09-02T11:02:18+05:30">02 September 2026</time>)

   MEASURED 2026-09-18: SAMA's ENTRY_RE matches 0 of 240 QFCRA entries. The
   `datetime` attribute is the better token anyway — an ISO stamp rather than a
   localised string that a CMS upgrade could reformat.

2. NO NODE RESOLUTION, WHICH IS THE EXPENSIVE HALF. SAMA's feed links slugs
   while its library stores /en/node/<id>, so it must OPEN every changed page
   just to read the id — the step whose absence made that feed look broken for
   months. QFCRA links /rulebook/<slug>, which is exactly the form
   `source_page_url` already holds. MEASURED 2026-09-18 over 240 live entries:
   178 matched a stored row directly, 0 requests spent resolving. So the sweep
   really is ONE request, not 1 + n, and `canonical_node_url` is deliberately
   absent rather than carried over unused.

3. THE TRAIL IS WORTH KEEPING. `book-trail` gives the folder path the entry sits
   under, already starting with the regulator name:

       Qatar Financial Centre Legislation >>  QFCRA Rules >>  Investment ...

   which is normally the most expensive part of a rulebook crawl to rebuild, and
   it is what lets one feed be routed to four source_systems.

items_per_page IS CAPPED AT 40, AND ASKING FOR MORE RETURNS ZERO
----------------------------------------------------------------
MEASURED 2026-09-18 on qfcra-en, the same trap SAMA documents:

    10  -> 10 entries      50  -> 0
    20  -> 20 entries     100  -> 0
    40  -> 40 entries     200  -> 0

A larger value does not error — the page answers 200 with an empty result, so an
over-large request reads as "nothing changed". PAGE_SIZE is fixed and windows are
walked with `&page=N`.

WHAT IT CANNOT DO
-----------------
Deletions. A withdrawn document simply stops being listed, and no window will
ever mention it. `stored-inventory` remains the way to find removals; run it
occasionally, not daily. The feed makes the crawl RARE, not unnecessary.
"""
from __future__ import annotations

import datetime as _dt
import html as _html
import logging
import re
import time as _t
from typing import List, Optional, Sequence

import requests

from dynamic_crawler.changesignal import ChangeSignal, Observation, identity_key

logger = logging.getLogger(__name__)

__all__ = ["TRFeedSweep", "fetch_entries", "default_window", "section_of",
           "QFCRA_BASE", "PAGE_SIZE", "QFCL_SECTIONS", "UNCRAWLED_SECTIONS"]

QFCRA_BASE = "https://qfcra-en.thomsonreuters.com"

FEED_PATH = "/view-revision-updates"

#: Which `book-trail` section belongs to which source_system in qfcl.yml.
#:
#: THE SPELLING IS THE SITE'S, NOT OURS, and the two differ in both directions:
#: the site says "QFC Regulations" (plural) where the source_system is "QFC
#: Regulation", and it has no QFC Law node at all — the law is a top-level book,
#: so its children are filed under the law's own title. Getting this wrong is not
#: a cosmetic error: `disappeared` is scoped by (regulator, source_system), so an
#: entry attributed to the wrong one proposes a document for withdrawal from a
#: section that never held it.
#:
#: MEASURED 2026-09-18 over 875 feed entries: this map claims 806 of them with
#: ZERO claimed by two source_systems. The 69 it leaves are real sections the
#: library does not crawl — archives, consultation papers, forms, rulemaking
#: instruments, and two "... Guidance" books (see below).
QFCL_SECTIONS = {
    "QFC Law":        {"QFC Law No. (7) of Year 2005"},
    "QFC Regulation": {"QFC Regulations"},
    "QFCA Rules":     {"QFCA Rules"},
    "QFCRA Rules":    {"QFCRA Rules"},
}

#: OUT OF SCOPE, AND WORTH A DECISION RATHER THAN SILENCE. These are top-level
#: books the feed reports and qfcl.yml does not crawl. Archives and consultation
#: papers are clearly out. The two "Guidance" books are less obvious — they hold
#: live QFCRA material (e.g. "GENE Corporate Sustainability Reporting") — and
#: are listed here so a reader sees they were excluded on purpose, not missed.
UNCRAWLED_SECTIONS = {
    "QFCRA Rulebooks Archive", "QFCA Rules Archive", "QFC Regulations Archive",
    "QFC Forms", "QFC Forms Archive", "Consultation Papers",
    "Rulemaking Instruments (By year)",
    "QFCRA Rules Guidance", "QFC Regulations Guidance",
}

#: Fixed, not configurable. See the module docstring: above 40 the page returns
#: an EMPTY result rather than an error, so a "bigger" request silently reads as
#: "nothing changed".
PAGE_SIZE = 40

BASIS_FEED = "thomson reuters revision feed (the regulator's own changed-on date)"

#: One entry AND its trail in a single match, so the two are paired structurally
#: rather than by position. SAMA reads trails positionally and defaults them,
#: which is correct there because some of its rows have none; pairing is
#: available here and is the safer of the two.
_ENTRY_RE = re.compile(
    r'<div class="book-detail">\s*<a href="([^"]+)"[^>]*>(.*?)</a>\s*'
    r'\(\s*<time datetime="([^"]+)"[^>]*>(.*?)</time>\s*\)\s*</div>\s*'
    r'<div class="book-trail">([^<]*)</div>',
    re.S)

#: "Showing results 1 to 10 of 21182" — sanity only; the pager is authoritative.
_TOTAL_RE = re.compile(r"Showing results \d+ to \d+ of ([\d,]+)")

#: A HISTORICAL SNAPSHOT, not the document. The feed links some entries to
#: /node/<id>/revisions/<rev>/view — the page as it stood at that revision —
#: rather than to the canonical /rulebook/<slug>. MEASURED 2026-09-18 over 1,000
#: entries: 125 are this shape and 87 of them sit inside sections we track. A
#: snapshot url matches nothing in the library, so without this every one would
#: be reported as a document we do not hold, and monitor_qfcl would answer a
#: report of 87 phantom discoveries by crawling them in as new rows.
#:
#: MATCHES `/revisions/` ONLY, deliberately. The bare /node/<id> form is the
#: CANONICAL url at SAMA, so excluding that would break this module for the
#: regulator it is meant to grow to cover; a `/revisions/` path is never the
#: current document on any Drupal deployment.
_REVISION_VIEW_RE = re.compile(r"/revisions/\d+", re.I)

#: Transient network faults, retried. Same reasoning as the SAMA module: a DNS
#: blip on this machine must not read as "QFCL monitoring is broken", because the
#: whole point of a feed is that it runs often enough to be trusted.
_RETRIES = 3
_BACKOFF = 2.0


def _get(url: str, timeout: float, params=None) -> str:
    last = None
    for attempt in range(_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=timeout,
                             headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            return r.text
        except (requests.ConnectionError, requests.Timeout) as e:
            # Only connection-level faults. An HTTP error is the regulator
            # answering, and retrying it would ask the same question again.
            last = e
            if attempt < _RETRIES - 1:
                logger.warning("tr feed: %s (attempt %d/%d), retrying in %.0fs",
                               str(e)[:120], attempt + 1, _RETRIES, _BACKOFF)
                _t.sleep(_BACKOFF * (attempt + 1))
    raise last


def section_of(trail: str) -> str:
    """The section a trail names, or "".

    The trail runs REGULATOR >> SECTION >> book >> ..., so the second crumb is
    the section — "QFCRA Rules", "QFC Regulations", "QFC Forms Archive". This is
    what routes one regulator-wide feed to the right source_system, and what
    identifies an entry belonging to a section the library does not crawl.
    """
    crumbs = [c.strip() for c in _html.unescape(trail or "").split(">>")]
    crumbs = [c for c in crumbs if c]
    return crumbs[1] if len(crumbs) > 1 else ""


def fetch_entries(since: str, until: str, *, base_url: str = QFCRA_BASE,
                  timeout: float = 45.0, max_pages: int = 25) -> List[dict]:
    """Every entry the regulator changed in [since, until], walking the pager.

    Stops on the first empty page. `max_pages` is a guard, not a limit anyone
    should hit: 40 x 25 is 1,000 changes in one window, and the measured rate for
    QFCL is 1 in sixty days.
    """
    feed = base_url.rstrip("/") + FEED_PATH
    out, seen = [], set()
    for page in range(max_pages):
        params = {"f_date": "on", "changed_1[min]": since,
                  "changed_1[max]": until, "items_per_page": PAGE_SIZE}
        if page:
            params["page"] = page
        html = _get(feed, timeout, params)
        rows = _ENTRY_RE.findall(html)
        if not rows:
            break
        for href, title, iso, shown, trail in rows:
            href = href.strip()
            if href in seen or _REVISION_VIEW_RE.search(href):
                continue
            seen.add(href)
            out.append({
                "url": href if href.startswith("http")
                       else base_url.rstrip("/") + href,
                "title": " ".join(_html.unescape(title).split()),
                # The ISO attribute is the token; the rendered string is kept
                # only so a person reading a report sees what the page showed.
                "changed_at": iso.strip(),
                "date_shown": " ".join(_html.unescape(shown).split()),
                "book_trail": _html.unescape(trail).strip(),
                "section": section_of(trail),
            })
        if len(rows) < PAGE_SIZE:
            break
    return out


class TRFeedSweep(ChangeSignal):
    """One request per sweep. No per-document requests at all.

    `tracked` is the urls the library already holds — `_tracked_urls` supplies
    both `document_url` and `source_page_url`, which matters here because a QFCL
    provision whose page links a single file carries the FILE in document_url
    and the page in source_page_url, and the feed speaks page urls.

    An entry whose url is in `tracked` is a MODIFIED document; one that is not is
    either a NEW document the feed has discovered — which a stored-inventory
    probe structurally cannot do — or a section this source does not crawl, which
    `section` tells apart without another request.
    """

    name = "sama-feed"        # the signal name in config/change_signals.yml

    def __init__(self, source: str, tracked, *, since: str, until: str,
                 base_url: str = QFCRA_BASE, timeout: float = 45.0,
                 sections: Optional[Sequence[str]] = None):
        self.source = source
        self.tracked = {self._norm(u) for u in (tracked or []) if u}
        self.since, self.until = since, until
        self.base_url = base_url
        self.timeout = timeout
        #: When set, entries whose trail names another section are reported but
        #: not proposed as discoveries for THIS source_system. One feed serves
        #: four of them, and `disappeared` is scoped by (regulator,
        #: source_system) — so an entry must not be attributed to the wrong one.
        self.sections = {s for s in (sections or []) if s}
        self.stats = {}

    @staticmethod
    def _norm(u: str) -> str:
        return str(u or "").split("?")[0].rstrip("/").lower()

    def sweep(self) -> List[Observation]:
        entries = fetch_entries(self.since, self.until,
                                base_url=self.base_url, timeout=self.timeout)
        logger.info("tr feed: %d entr(ies) for %s..%s",
                    len(entries), self.since, self.until)
        obs, matched, unmatched, other = [], 0, 0, 0
        for e in entries:
            url = e["url"]
            known = self._norm(url) in self.tracked
            # A url THIS source already stores is ours whatever the trail says,
            # so the section filter only ever decides where a DISCOVERY belongs.
            # Ordering it this way closes two holes: a top-level node whose trail
            # is just the regulator (the QFC Law root page) carries no section
            # and would otherwise be claimed by all four source_systems, and a
            # stored page filed under a trail we did not anticipate ("QFCRA Rules
            # Guidance") would otherwise be dropped as another section's.
            if not known and self.sections and e["section"] not in self.sections:
                other += 1
                continue
            matched += known
            unmatched += (not known)
            fields = {"document_url": url}
            obs.append(Observation(
                key=identity_key(fields), fields=fields,
                identity_fields=("document_url",),
                # The regulator's own changed-on stamp IS the version token: the
                # feed lists a document only because that stamp moved.
                token=e["changed_at"],
                basis=BASIS_FEED,
                url=url,
                title=e["title"][:120]))
        self.stats = {"entries": len(entries), "already_tracked": matched,
                      "not_in_library": unmatched,
                      "other_sections_skipped": other,
                      "window": f"{self.since}..{self.until}"}
        return obs

    def confirm_required_for(self, obs: Observation) -> bool:
        # The feed lists a document BECAUSE the regulator changed it. There is
        # nothing to second-guess, unlike a counter that can move in bulk.
        return False

    def confirm(self, obs: Observation) -> Optional[str]:
        return None


def default_window(days: int = 30) -> tuple:
    """The last `days` days, as the feed wants them (YYYY-MM-DD)."""
    today = _dt.date.today()
    return (today - _dt.timedelta(days=days)).isoformat(), today.isoformat()
