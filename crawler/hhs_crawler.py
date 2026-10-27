# -*- coding: utf-8 -*-
"""HHS: the "Explore Laws and Regulations" block, and each link in it is a row.

www.hhs.gov/regulations/index.html carries a block headed "Explore Laws and
Regulations" holding ten links. The library wants that block verbatim:

    Department of Health and Human Services (HHS)
      Laws and Regulations                           source_system (page <h1>)
        Explore Laws and Regulations                 folder (the block's <h2>)
          HHS Guidance Portal                        ONE row: the page itself
          HHS Regulations Review                     ONE row
          ...                                        10 rows in all

section_path on every row is where it is LISTED on hhs.gov -- "Home > Laws and
Regulations > Explore Laws and Regulations > <link text>" -- and the landed
page's own breadcrumb is kept as extra_meta.page_breadcrumb.

THE LINKED PAGE IS THE DOCUMENT, NOT A FOLDER OF DOCUMENTS -- the rule OCC took
three builds to arrive at (docs/onboarding_playbook.md section 0). Each link
contributes exactly one row, with that page's text stored. The links ON that
page survive inside the stored body as live absolute urls and become no rows.
That matters most for two of the ten, which are listings rather than pages:

    HHS Guidance Portal   a searchable index of HHS guidance documents
    Regulatory actions    federalregister.gov's HHS agency page, a feed of
                          every document HHS publishes there

Expanding either would be hundreds to thousands of rows; the user chose one row
each on 2026-10-13.

-----------------------------------------------------------------------------
THE LIST OF RECORD IS THE BLOCK ON THE LIVE LANDING PAGE
-----------------------------------------------------------------------------
Not a hardcoded list. The block is read on every run, so a link HHS adds there
becomes a row and a link it removes becomes a `disappeared` candidate -- which
is the change this source should be able to see. A block that cannot be found
is FATAL rather than an empty source: `_links` raises.

The block is isolated as the HIGHEST ANCESTOR OF ITS <h2> THAT CONTAINS NO OTHER
<h2>. Measured 2026-10-13 on the cached page: three wrappers deep that holds
exactly the ten links; one more level up holds three <h2> and sixteen links
("Ways to Get Involved" and "Featured Topics" join it).

-----------------------------------------------------------------------------
TITLES ARE THE BLOCK'S LINK TEXT, NOT THE LANDED PAGE'S <h1>
-----------------------------------------------------------------------------
OCC uses the page's own <h1>. Here the block is the specification -- the user
supplied a screenshot of it -- and for at least one row the two disagree
outright: the link reads "Regulatory actions" and federalregister.gov's <h1>
reads "Health and Human Services Department". The <h1> is recorded in
extra_meta["page_h1"] for every row, so either reading is available.

The external-link marker inside an <a> ("<span class="usa-sr-only">, links to
an external website</span>") is screen-reader text and is removed before the
title is taken.

-----------------------------------------------------------------------------
FOUR HOSTS, ONE BROWSER
-----------------------------------------------------------------------------
    www.hhs.gov          6 links   Akamai: 403 "Access Denied" to plain clients
    federalregister.gov  1         open
    www.healthit.gov     1         open; REDIRECTS to healthit.gov/legislation/
    aspr.hhs.gov         1         open; REDIRECTS to www.aspr.gov/.../prep-act

www.hhs.gov answers `requests` with Akamai's 403 (Server-Timing: ak_p,
errors.edgesuite.net), measured 2026-10-12. A headed persistent Chrome passed
on the first try -- the same recipe dfpi.ca.gov needs for Cloudflare, COPIED
here rather than imported (see copy-over-refactor-across-regulators). Since the
browser is needed for the landing page and six of the ten anyway, all ten go
through it: one client, one fetch policy, one set of failure modes.

REDIRECTS ARE FOLLOWED AND THE LANDED URL IS STORED, so a row is identified by
where the link actually goes. Two of the ten redirect today; FDIC's landing
pages redirected 16 of 22.

HEADLESS IS NOT EXPECTED TO PASS -- not tested here, but the DFPI measurement
that a warm profile does not rescue a headless run is the reason the default is
False. A scheduled run needs a desktop session.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawler.generic_crawler_wrapper import GenericSiteCrawler
from generic_crawler.crawler import absolutize_html, content_key

logger = logging.getLogger(__name__)

_WS = re.compile(r"\s+")


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


#: lxml, NOT "html.parser". html.parser mis-nests real markup -- on occ.gov a
#: self-closing <img/> came out containing the whole article, so removing images
#: stored 194 of 3,416 characters. See crawler/occ_crawler.py PARSER. Named
#: explicitly, with no fallback: a silent downgrade stores a tenth of the page.
PARSER = "lxml"

#: Where the content lives, tried in order, first one with real text wins.
#: MEASURED 2026-10-13 on the cached pages:
#:
#:     www.hhs.gov       main            (landing page; same Drupal theme site-wide)
#:     www.aspr.gov      main#main-content
#:     healthit.gov      NO <main> AT ALL. div.entry-content holds the article
#:                       (7,691 chars); its parent wp-site-blocks adds the
#:                       mega-menu and footer (20,669).
#:
#: federalregister.gov is not measured; `main` is tried first and the body
#: falls back to <body> with a warning rather than storing nothing.
CONTENT_SELECTORS = ("main", "div.entry-content", "[role=main]", "article")

#: Hosts whose content lives somewhere the generic list cannot find, tried
#: BEFORE it. eCFR has no <main> or <article>, so the generic list would fall
#: back to <body> -- search box, menus and all. MEASURED 2026-10-13:
#:     /current/.../part-46                div.part     109,463 chars, all of Part 46
#:     /on/2017-01-03/.../part-46          div.part      76,570 (the pre-2018 rule)
#:     /current/.../section-46.104         div.section   10,711 (one section)
#: div.part first: on a part page div.section would be only its first section.
HOST_CONTENT_SELECTORS = {
    "www.ecfr.gov": ("div.part", "div.section"),
    "ecfr.gov": ("div.part", "div.section"),
}

#: A content region shorter than this is taken to be a wrong match (a skip-link
#: target, an empty <main> shell) and the next selector is tried.
MIN_REGION_TEXT = 200

#: Where the block's link text travels, kept OUT of `section_path` so the two
#: can mean different things -- same split as OCCTopicCrawler.TRAIL_KEY.
LINK_KEY = "hhs_link_text"

#: The folders a row sits in BELOW the source system, as a list. A section the
#: user has not yet asked to expand is ["Explore Laws and Regulations"]; an
#: expanded one adds its own name, and its documents add their title.
TRAIL_KEY = "hhs_trail"

#: A FILE link, by extension. NOT dynamic_crawler.formfill.runner._is_doc: that
#: treats any "/document/" path as a file, and every Guidance Portal detail page
#: lives at /guidance/document/<slug> -- it would call the pages themselves files.
_FILE_RE = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|rtf|txt|zip)(?:$|[?#])", re.I)

#: In-page fetch: the request goes out from inside the open, already-cleared
#: hhs.gov tab, so Akamai sees the same browser, TLS and cookies as a page load
#: -- without rendering. MEASURED 2026-10-13 on 20 Guidance Portal detail pages:
#: 20 x HTTP 200, 0 refusals, 0.95s a page including a 0.5s pause, against
#: ~4s for a full page.goto. That is the difference between ~3 hours and ~12
#: for the portal's ~11,390 items.
_FETCH_JS = """async u => {
  const r = await fetch(u, {credentials: 'include'});
  return [r.status, r.url, await r.text()];
}"""

_CRUMB_SELECTORS = ('nav[aria-label*="readcrumb" i]', 'ol.usa-breadcrumb__list',
                    '.usa-breadcrumb', '.breadcrumb', '[class*="breadcrumb"]')

#: Removed by selector. Screen-reader-only text ("Breadcrumb", ", links to an
#: external website") is not something a reader of the page sees, and it would
#: otherwise sit in the hashed text.
FURNITURE_SELECTORS = (
    ".usa-sr-only",
    ".visually-hidden",
    ".element-invisible",
    "[class*=share]",
    "[class*=social]",
    ".usa-skipnav",
    # SECTION MENUS INSIDE THE CONTENT REGION, matched by shape. MEASURED
    # 2026-10-13: healthit.gov's div.hit-subnav sits INSIDE div.entry-content
    # and opened the stored body with "Policy Policy Legislation Legislation
    # ..."; aspr.gov's div.usa-layout-docs__sidenav sits inside <main> and
    # closed it with the whole "About the PREP Act ... Threat Specific" menu.
    "[class*=subnav]",
    "[class*=sidenav]",
    # federalregister.gov's agency page, MEASURED 2026-10-13. Its stored body
    # opened with the site-wide "Legal Status ... not an official legal
    # edition" disclaimer (div#print-disclaimer), then an <h1> "Agency" title
    # bar, and every listed document carried "New folder / My Clipboard" from
    # its add-to-folder widget, plus a "Subscribe" link. All four are
    # federalregister.gov chrome, the same on every page it serves.
    "#print-disclaimer",
    "div.main-title-bar",
    "div.add-to-folder",
    "#add-to-folder",
    ".subscribe-wrapper",
    # NOT VISIBLE AT ALL. The same page's search-suggestions modal
    # (div#suggestions > div.modal.hidden) put "Enter a search term or FR
    # citation e.g. 88 FR 382 ..." at the top of the stored body once the
    # disclaimer had gone.
    #
    # NOT "[hidden]", AND THAT WAS MEASURED, NOT GUESSED. USWDS accordion
    # panels carry the `hidden` attribute until a reader expands them, so it
    # took PREP from 5,939 stored characters to 1,365 -- every declaration on
    # the page -- and the Guidance Portal from 3,259 to 2,091.
    "#suggestions",
    "div.modal",
    # federalregister.gov DOCUMENT and LISTING pages, MEASURED 2026-10-13 on EO
    # 14094 (2023-07760) and the 1993 Clinton executive-order listing:
    #   div.content-nav-wrapper  the sticky utility rail -- sharing, "Shorter
    #                            Document URL", Other Formats, Public Inspection
    #   div.col-md-3.navigation  the year-by-year presidents navigation, which
    #                            closed the listing's stored body with
    #                            "Kennedy 1963 1962 ... Truman 1953 ..."
    #   span.header-link         "view all Presidential Documents" in the <h1>
    #
    # NOT div.fr-box-reader-aid, AND THAT WAS MEASURED. The navigation sits in
    # one of those boxes, but so does the LISTING ITSELF -- the 57 executive
    # orders of 1993, EO 12866 among them, 15,759 characters. Removing the class
    # stored 814 characters of the page's preamble and none of its orders.
    "div.content-nav-wrapper",
    "div.col-md-3.navigation",
    "span.header-link",
    # The visible half of a USWDS tooltip: "opens in a new tab" after every
    # link that opens one. Its screen-reader twin is .usa-sr-only, above.
    ".usa-tooltip__body",
)

#: Link text that is navigation within the page, not a reference.
_JUMP_TEXT = ("back to top", "return to top", "top of page")

#: Furniture removed by heading, matched on the NORMALISED text on both sides
#: (the OCC lesson: "visit helpwithmybank.gov" in the set never matched the
#: page's "visit helpwithmybank gov"). Not measured on the six hhs.gov pages
#: before the first export -- the audit after it is what decides this list.
FURNITURE_HEADINGS = {
    "popular links", "quick links", "related links", "related resources",
    "page information", "submit feedback", "external link notice",
    "email updates", "follow us", "stay connected", "share this page",
}
_FURNITURE_KEYS = frozenset(_norm(h) for h in FURNITURE_HEADINGS)

#: Prefix beats enumeration: OCC's five spellings of "contact" still missed
#: "Connect with ERSP", because sites name each block after the team it reaches.
FURNITURE_PREFIXES = (
    "contact",
    "connect with",
    "subscribe",
    "sign up for",
    "follow us",
    "stay connected",
)

FURNITURE_PATTERNS = (
    re.compile(r"^get .+ updates$"),
)

#: A <button> whose whole text is one of these is a control and goes. Anything
#: else inside a button is content -- USWDS accordions put the section name
#: there, and stripping <button> emptied every accordion heading on three OCC
#: pages.
CONTROL_LABELS = frozenset((
    "expand all", "collapse all", "show", "hide", "show all", "hide all",
    "open", "close", "menu", "search", "submit", "print", "back to top",
    "filter", "apply", "reset", "clear", "clear all",
))

#: The browser lies that get a headed Chrome past bot management. Injected
#: before any page script runs. Copied from crawler/dfpi_crawler.py.
_STEALTH = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins',
  {get: () => [1,2,3,4,5].map(i => ({name: 'Plugin ' + i}))});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = window.chrome || {runtime: {}};
"""

#: Interstitial titles. Present means the check has not finished, which is a
#: WAIT -- Cloudflare's cleared in 4.5s on DFPI.
_CHALLENGE = ("just a moment", "please wait")
#: Akamai's refusal. Present after the wait means the page is unread.
_DENIED = "access denied"
#: Where federalregister.gov / ecfr.gov send an IP they have flagged as a bot.
_BLOCK_HOSTS = ("unblock.federalregister.gov",)
#: The hosts that block shares; once it has fired, they are not visited again.
_FR_HOSTS = ("www.federalregister.gov", "federalregister.gov",
             "www.ecfr.gov", "ecfr.gov")


def _text(el) -> str:
    """Display text, pipe-free -- " | " is doc_path's separator in the
    workbook, so a pipe in a title reads back as an extra folder (DFPI)."""
    if not el:
        return ""
    return _WS.sub(" ", el.get_text(" ", strip=True)).replace("|", "/").strip()


#: Accessibility text hhs.gov welds onto link labels.
_TITLE_NOISE = re.compile(
    r",?\s*(?:opens in a new tab|links to an external website)\s*", re.I)
#: A file-type/size suffix: "(PDF)", "(PDF, 265 KB)", "[PDF - 1.2 MB]",
#: "(DOCX, 40 KB)". Stripped from titles on the user's instruction of
#: 2026-10-13 ("strip pdf/size off the titles"); file_type already says it.
#: ONLY a type, optionally with a size -- NOT "(PDF Presentation)", which is a
#: name. An earlier "type then anything" form stripped exactly that off the
#: PREP Act page's "Ninth Amendment ... (PDF Presentation)".
_SIZE_SUFFIX = re.compile(
    r"\s*[\(\[]\s*(?:PDF|DOCX?|XLSX?|PPTX?|ZIP|RTF|TXT)"
    r"(?:\s*[-–,:]?\s*[\d.,]+\s*[KMG]i?B)?\s*[\)\]]\s*$",
    re.I)


#: Inline colour declarations. The stored body is shown under the VIEWER's
#: theme, so a colour the source site painted for its own page is wrong there.
#: MEASURED 2026-10-13 on healthit.gov/legislation/: the "ONC's Impact" box
#: carries style="background-color: #f7f8fc" -- near-white -- which in a dark
#: theme drew a white panel with light text on it, all but unreadable.
_INLINE_COLOR = re.compile(
    r"(?:^|;)\s*(?:background(?:-color|-image)?|color)\s*:[^;]*", re.I)


def _strip_inline_colors(root) -> int:
    """Remove background/colour declarations from every inline style. The
    text is untouched, so no content_hash moves."""
    n = 0
    for el in root.find_all(style=True):
        before = el["style"]
        after = _INLINE_COLOR.sub("", before).strip(" ;")
        if after != before.strip(" ;"):
            n += 1
            if after:
                el["style"] = after
            else:
                del el["style"]
    return n


def _strip_link_file_labels(root) -> int:
    """Remove a trailing file-type label from every link's text in a stored
    body: "Summary of the Privacy Rule [PDF, 381 KB]" -> "Summary of the
    Privacy Rule". hhs.gov does not serve the label reliably -- MEASURED on
    two HIPAA pages (that link and "Enforcement Rule [PDF]"): present on
    10-16, absent on 10-19, present on 10-20 -- so it moved the content_hash
    of a page that had not changed (user, 2026-10-22: "make strip fix").
    The label is format, not content; file_type says the same thing. A link
    whose whole text IS the label keeps it, so no link is left empty."""
    n = 0
    for a in root.find_all("a"):
        if getattr(a, "decomposed", False):
            continue
        strings = [s for s in a.find_all(string=True) if s.strip()]
        if not strings:
            continue
        last = strings[-1]
        cut = _SIZE_SUFFIX.sub("", last)
        if cut == last:
            continue
        if not cut.strip() and len(strings) == 1:
            continue
        last.replace_with(cut)
        n += 1
    return n


def _nolang(u: str) -> str:
    """Drop hhs.gov's `language` query parameter. Its own links carry
    ?language=es on ENGLISH files (security101.pdf?language=es) -- a site
    quirk, not a translation -- which made one PDF look like two urls and
    cost the fragment-titled copy its proper name (user, 2026-10-15)."""
    if "language=" not in u:
        return u
    base, _, frag = u.partition("#")
    path, _, q = base.partition("?")
    keep = [p for p in q.split("&") if p and not p.lower().startswith("language=")]
    return path + ("?" + "&".join(keep) if keep else "") + ("#" + frag if frag else "")


#: A link whose whole text names ANOTHER LANGUAGE ("en español", "中文" ...) or
#: that goes to hhs.gov's Spanish site (/es/...) or a *-spanish file. Removed
#: from stored bodies with any sentence that only points to that version
#: (user, 2026-10-15: "other languages ... exclude it").
_LANG_NAMES = (r"(?:en\s+)?espa(?:n|ñ)ol|spanish|chinese|中文|"
               r"vietnamese|tiếng việt|korean|한국어|tagalog|"
               r"russian|русский|arabic|"
               r"العربية|haitian creole|"
               r"krey(?:o|ò)l|fran(?:c|ç)ais|french|portugu(?:e|ê)s|"
               r"portuguese|polski|polish|japanese|日本語|other languages")
_LANG_LINK_TEXT = re.compile(r"^\s*(?:%s)\s*$" % _LANG_NAMES, re.I)
_LANG_HREF = re.compile(r"^(?:https?://[^/]*hhs\.gov)?/es/|[-_]spanish\.\w+$", re.I)
#: "The video presentation in Spanish may be found on HHS's YouTube channel
#: at: <link>" -- a sentence whose only job is to point at the other version.
_LANG_POINTER = re.compile(r"\b(?:in|en)\s+(?:%s)\b" % _LANG_NAMES, re.I)


def _strip_other_languages(root) -> int:
    """Remove other-language links and pointer sentences from a stored body.
    A paragraph that merely MENTIONS a language ("available in English and
    Spanish") and links nothing is content, and stays."""
    n = 0
    for a in root.find_all("a", href=True):
        if getattr(a, "decomposed", False):
            continue
        if _LANG_LINK_TEXT.match(a.get_text(" ", strip=True)) or \
                _LANG_HREF.search(a["href"].strip()):
            parent = a.parent
            a.decompose()
            n += 1
            # The link's now-empty wrapper (span.usa-tooltip) would keep "("
            # and ")" in separate strings; remove it so they can meet.
            while parent is not None and parent.name == "span" and \
                    not parent.get_text(strip=True):
                up = parent.parent
                parent.decompose()
                parent = up
            # "(en español)" and " | en español" leave their punctuation.
            holder = parent.find_parent(["p", "li", "td", "dd"]) or parent
            holder.smooth()
            strings = list(holder.find_all(string=True))
            for i, s in enumerate(strings):
                t = re.sub(r"\(\s*\)", "", s)
                if i == len(strings) - 1:
                    t = re.sub(r"\s*\|\s*$", "", t)
                if t != s:
                    s.replace_with(t)
    for el in root.find_all(["p", "li"]):
        if getattr(el, "decomposed", False):
            continue
        txt = el.get_text(" ", strip=True)
        if el.find("a") is not None and len(txt) <= 300 and _LANG_POINTER.search(txt):
            el.decompose()
            n += 1
    return n


#: A file row's link text that names nothing: "access", "Download 4x5 Pocket
#: Brochure", or a sentence fragment ("paper or electronically").
_WEAK_TITLE = re.compile(r"^(?:download|access|here|click here|view|read|"
                         r"read more|learn more|pdf|link|this|more|see)\b", re.I)


def _weak_title(t: str) -> bool:
    return bool(_WEAK_TITLE.match(t or "")) or (t or "")[:1].islower()


def _clean_title(s: str) -> str:
    s = _TITLE_NOISE.sub(" ", s or "")
    prev = None
    while prev != s:
        prev, s = s, _SIZE_SUFFIX.sub("", s)
    # "( PDF Presentation )" -> "(PDF Presentation)": the space is an artefact
    # of joining a link's text to the words around it, not the site's.
    s = re.sub(r"([(\[])\s+", r"\1", s)
    s = re.sub(r"\s+([)\]])", r"\1", s)
    return _WS.sub(" ", s).strip(" ,;")


def _is_bold_label(el) -> bool:
    """A <p> that is ONLY bold text, with no link: a heading in all but markup.

    hhs.gov writes the Retrospective Review page's date groups this way --
    <p><strong>February 2016 Retrospective Review:</strong></p> followed by a
    <ul> -- not as <h3>. MEASURED 2026-10-13: 10 such groups, 0 <h3>.
    """
    if el.name != "p" or el.find("a"):
        return False
    bold = el.find_all(["strong", "b"])
    if not bold:
        return False
    rest = el.get_text(" ", strip=True)
    for b in bold:
        rest = rest.replace(b.get_text(" ", strip=True), "", 1)
    return not rest.strip(" : ")


class HHSExploreCrawler(GenericSiteCrawler):
    """One row per link in the landing page's "Explore Laws and Regulations"."""

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        #: The block's <h2>, EXACTLY as hhs.gov writes it. A near-miss raises
        #: rather than yielding a source with no rows.
        block_heading: str = "Explore Laws and Regulations",
        #: How many links the block held when this was built. A different count
        #: is logged as a warning, not refused -- HHS adding a link is a change
        #: to record, not an error.
        expected_links: Optional[int] = None,
        category: Optional[str] = None,
        headless: bool = False,
        channel: Optional[str] = "chrome",
        profile_dir: Optional[str] = None,
        challenge_wait: int = 30,
        #: Politeness, and it is not decoration: the Akamai block on
        #: saudiexchange.sa was created by our own repeated probing.
        delay: float = 1.5,
        #: Which of the block's links become a SUBFOLDER with documents of their
        #: own, by link text -> handler. Sections not named here stay one row.
        #: Grown one section at a time, on the user's instruction for each.
        expand: Optional[Dict[str, str]] = None,
        #: Guidance Portal: stop after this many listing pages. None = all of
        #: them (1,139 on 2026-10-13). For trial runs only.
        guidance_max_pages: Optional[int] = None,
        #: Pause between in-page fetches. 0.5s measured clean on 20 pages.
        fetch_delay: float = 0.5,
        #: A walk this long must survive an interruption. Progress is appended
        #: here as it goes, read back on the next start, and DELETED when a run
        #: completes -- so it resumes a broken run and never stands in for a
        #: fresh one.
        progress_dir: Optional[str] = None,
        #: Consecutive refusals before the walk stops. A scheduled retry against
        #: Akamai is what makes a block (ONBOARDING section 3), so it stops and
        #: keeps its progress rather than pressing on.
        max_refusals: int = 5,
        #: Guidance detail pages Akamai refuses mid-walk. Measured 2026-10-19: a
        #: different ~220 per run (403 "Access Denied" on the page, its PDF 200),
        #: so each export lost a different 4% of items without a word. After the
        #: main pass: wait `retry_pause`, then retry each refused page ONCE,
        #: `retry_delay` apart, at most `retry_max` of them -- paced, because a
        #: burst of retries is what provokes Akamai.
        retry_pause: float = 120.0,
        retry_delay: float = 3.0,
        retry_max: int = 500,
        #: MONITORING ONLY. The Guidance Portal's detail pages are ~5,850 of the
        #: run's ~7,000 page loads (about 3.5 h), and all a detail page adds is
        #: which hhs.gov file the item links -- the listing already carries its
        #: title, division, status and issue date, which is what the hash reads.
        #: Every run keeps what it read in `guidance_details.json`. With this
        #: set, a run reuses that record for items still listed and re-reads
        #: only new items plus the OLDEST len(items) * 7 / days of the kept
        #: ones, so a weekly run cycles through every detail page in `days`
        #: days. None (the default, and every export) reads every detail page.
        #: Set by jobs/monitor_jobs.py through HHS_DETAIL_REFRESH_DAYS.
        detail_refresh_days: Optional[float] = None,
        **generic_kwargs,
    ):
        super().__init__(seed_url=seed_url, regulator=regulator,
                         source_system=source_system, category=category,
                         **generic_kwargs)
        self.block_heading = block_heading
        self.expected_links = expected_links
        self.expand = dict(expand or {})
        self.guidance_max_pages = guidance_max_pages
        self.fetch_delay = fetch_delay
        self.progress_dir = progress_dir or os.path.join(
            os.path.expanduser("~"), ".cache", "hhs_progress")
        self.max_refusals = max_refusals
        self.retry_pause = retry_pause
        self.retry_delay = retry_delay
        self.retry_max = retry_max
        if detail_refresh_days is None and os.getenv("HHS_DETAIL_REFRESH_DAYS"):
            detail_refresh_days = float(os.environ["HHS_DETAIL_REFRESH_DAYS"])
        self.detail_refresh_days = detail_refresh_days
        self._skipped: Dict[str, List[dict]] = {}
        self.headless = headless
        self.channel = channel
        self.profile_dir = profile_dir or os.path.join(
            os.path.expanduser("~"), ".cache", "hhs_profile")
        self.challenge_wait = challenge_wait
        self.delay = delay
        self._warnings: List[str] = []
        self._fr_blocked = False

    # ------------------------------------------------------------------ #
    #  the browser the host will talk to                                  #
    # ------------------------------------------------------------------ #
    def _browser(self, p):
        """Headed persistent Chrome with the automation giveaways removed.

        Copied from DFPISectionCrawler._browser; every argument there was
        arrived at by elimination. `headless=False` is the one that decides it.
        """
        ctx = p.chromium.launch_persistent_context(
            self.profile_dir,
            headless=self.headless,
            channel=self.channel,
            args=["--disable-blink-features=AutomationControlled",
                  "--no-first-run", "--no-default-browser-check",
                  "--window-size=1280,900"],
            ignore_default_args=["--enable-automation"],
            locale="en-US",
            timezone_id="America/New_York",
            viewport={"width": 1280, "height": 900},
        )
        ctx.add_init_script(_STEALTH)
        return ctx

    def _get(self, page, url: str) -> Tuple[Optional[str], str]:
        """One page, waited out past any interstitial. (html, FINAL url)."""
        if self._fr_blocked and urlparse(url).netloc.lower() in _FR_HOSTS:
            # Already sent to the CAPTCHA page this run: don't knock again.
            self._warnings.append("skipped (federalregister.gov/ecfr.gov "
                                  "block): %s" % url)
            return None, url
        if self.delay:
            time.sleep(self.delay)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            self._warnings.append("could not open %s: %s" % (url, str(e)[:120]))
            return None, url
        title = ""
        for _ in range(self.challenge_wait):
            page.wait_for_timeout(1000)
            try:
                title = (page.title() or "").lower()
            except Exception:
                continue
            if title and not any(c in title for c in _CHALLENGE) \
                    and _DENIED not in title:
                if urlparse(page.url or "").netloc.lower() in _BLOCK_HOSTS:
                    # federalregister.gov and ecfr.gov answer an IP they
                    # judge automated with a 302 to a CAPTCHA page ("IP Access
                    # Help") -- a normal-looking 200 that the 2026-10-15
                    # export stored as the text of 36 regulations. Never
                    # store it. The user's call (2026-10-16): SKIP those rows
                    # and finish the rest, rather than stop the whole run.
                    # The workbook is then short of every FR/eCFR row --
                    # export it under a new name, never over a good one, or
                    # the next comparison reads them all as "disappeared".
                    self._fr_blocked = True
                    self._warnings.append(
                        "SKIPPED %s -- redirected to %s: federalregister.gov/"
                        "ecfr.gov have blocked this IP as automated. Open that "
                        "page in the crawl's Chrome profile (%s), complete the "
                        "CAPTCHA and 'Request Access'; until then every FR/"
                        "eCFR row is missing from this workbook."
                        % (url, page.url, self.profile_dir))
                    logger.warning("FR/eCFR block: skipping %s and the rest of "
                                   "those hosts this run", url)
                    return None, url
                return page.content(), page.url or url
        # SAID, NOT SWALLOWED. A refused page must never read as a page with
        # nothing on it.
        self._warnings.append("%s: %s" % (
            "Akamai refused" if _DENIED in title else "interstitial did not clear",
            url))
        return None, page.url or url

    # ------------------------------------------------------------------ #
    #  the block                                                          #
    # ------------------------------------------------------------------ #
    def _links(self, html: str, base: str) -> List[Tuple[str, str]]:
        """The block's links, in page order, as (link text, absolute url)."""
        soup = BeautifulSoup(html, PARSER)
        want = _norm(self.block_heading)
        h2 = next((h for h in soup.find_all("h2") if _norm(_text(h)) == want),
                  None)
        if h2 is None:
            raise RuntimeError(
                "no <h2> %r on %s -- the heading must match hhs.gov's wording "
                "exactly; refusing to report an empty source"
                % (self.block_heading, self.seed_url))
        block = h2
        for p in h2.parents:
            if p.name in ("body", "html", "[document]") or len(p.find_all("h2")) > 1:
                break
            block = p
        out: List[Tuple[str, str]] = []
        seen = set()
        for a in block.find_all("a"):
            href = (a.get("href") or "").strip()
            if not href or href.startswith(("#", "mailto:", "tel:")):
                continue
            for sr in a.select(".usa-sr-only, .visually-hidden"):
                sr.decompose()
            name = _clean_title(_text(a))
            url = urljoin(base, href).split("#")[0]
            if not name or urlparse(url).scheme not in ("http", "https") \
                    or url in seen:
                continue
            seen.add(url)
            out.append((name, url))
        if not out:
            raise RuntimeError("the %r block on %s holds no links"
                               % (self.block_heading, self.seed_url))
        if self.expected_links and len(out) != self.expected_links:
            msg = ("the %r block holds %d link(s), expected %d -- HHS has "
                   "changed it" % (self.block_heading, len(out),
                                   self.expected_links))
            logger.warning(msg)
            self._warnings.append(msg)
        return out

    # ------------------------------------------------------------------ #
    #  furniture                                                          #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _furniture_unit(h):
        """The nodes a furniture heading owns -- its USWDS card, or its
        following siblings up to the next heading at or above its rank. Copied
        from OCCTopicCrawler._furniture_unit; rank matters on both tests."""
        card = h.find_parent("li")
        if card is not None and card.find_parent("ul", class_="usa-card-group"):
            return [card]
        card = h.find_parent("div", class_="usa-card__container")
        if card is not None:
            return [card]
        rank = int(h.name[1])
        doomed = [h]
        for sib in h.find_next_siblings():
            if getattr(sib, "name", None) is None:
                doomed.append(sib)
                continue
            if sib.name in ("h1", "h2", "h3", "h4") and int(sib.name[1]) <= rank:
                break
            if any(int(x.name[1]) <= rank
                   for x in sib.find_all(["h1", "h2", "h3", "h4"])):
                break
            doomed.append(sib)
        return doomed

    @staticmethod
    def _unwrap_buttons(main) -> int:
        """Keep a button's label, drop the button. Drop bare controls."""
        gone = 0
        for b in list(main.find_all("button")):
            if getattr(b, "decomposed", False):
                continue
            txt = _text(b)
            if not txt or _norm(txt) in CONTROL_LABELS:
                b.decompose()
                gone += 1
                continue
            b.replace_with(txt)
        return gone

    @staticmethod
    def _strip_selectors(main) -> int:
        """FURNITURE_SELECTORS -- run BEFORE `_unwrap_buttons`.

        MEASURED 2026-10-13: hhs.gov's mobile side-menu toggle is
        <button class="sidenav-toggle">Navigate to:</button>. Unwrapped first,
        it became bare text with no class for [class*=sidenav] to match, and
        four stored bodies opened with "Navigate to:" ahead of their <h1>.
        """
        gone = 0
        for sel in FURNITURE_SELECTORS:
            for el in main.select(sel):
                if not getattr(el, "decomposed", False):
                    el.decompose()
                    gone += 1
        # IN-PAGE JUMP NAVIGATION. "Back to top" links, and a block whose every
        # link is an #anchor into the same page -- the February 2016 Update's
        # "On this page: Published | Anticipated | New". Neither is content,
        # and the jump list would otherwise be read as a folder of links.
        for a in list(main.find_all("a")):
            if not getattr(a, "decomposed", False) and \
                    _norm(_text(a)) in _JUMP_TEXT:
                a.decompose()
                gone += 1
        for el in list(main.find_all(["p", "ul", "ol", "nav", "div"])):
            if getattr(el, "decomposed", False):
                continue
            links = el.find_all("a", href=True)
            if len(links) < 2 or not all(a["href"].startswith("#") for a in links) \
                    or el.find(["h1", "h2", "h3", "h4", "table"]):
                continue
            # SHORT AND MOSTLY LINKS, or it is prose that merely cites its own
            # footnotes -- a Federal Register body links only to #anchors and
            # would otherwise go whole.
            body = len(el.get_text(" ", strip=True))
            linked = sum(len(a.get_text(" ", strip=True)) for a in links)
            if body <= 300 and linked >= 0.5 * body:
                el.decompose()
                gone += 1
        return gone

    @classmethod
    def _strip_furniture(cls, main) -> int:
        """Heading rules -- run AFTER `_unwrap_buttons`, so an accordion heading
        is matched on its real name rather than an empty <button>."""
        gone = 0
        for h in list(main.find_all(["h2", "h3", "h4"])):
            if getattr(h, "decomposed", False):
                continue
            key = _norm(re.sub(r"\s+(Show|Hide)$", "", _text(h)))
            if (key not in _FURNITURE_KEYS
                    and not key.startswith(FURNITURE_PREFIXES)
                    and not any(p.match(key) for p in FURNITURE_PATTERNS)):
                continue
            for d in cls._furniture_unit(h):
                if not getattr(d, "decomposed", False):
                    d.decompose()
            gone += 1
        return gone

    @staticmethod
    def _tidy_empty(main) -> None:
        """Drop containers and headings the removals left holding nothing --
        they render as empty framed boxes. healthit.gov's article also opens
        with an EMPTY <h2>, measured 2026-10-13."""
        for _ in range(6):
            gone = 0
            for el in list(main.find_all(["div", "p", "span", "section", "li",
                                          "ul", "ol", "hr", "h1", "h2", "h3",
                                          "h4", "h5", "h6", "table"])):
                if getattr(el, "decomposed", False):
                    continue
                if el.name not in ("hr",) and el.name[0] != "h" and \
                        el.find(["img", "a", "table", "h1", "h2", "h3", "h4"]):
                    continue
                if (el.get_text(" ", strip=True) or "").strip():
                    continue
                el.decompose()
                gone += 1
            if not gone:
                break

    # ------------------------------------------------------------------ #
    #  the row                                                            #
    # ------------------------------------------------------------------ #
    def _region(self, soup, url: str):
        host = urlparse(url).netloc.lower()
        for sel in HOST_CONTENT_SELECTORS.get(host, ()) + CONTENT_SELECTORS:
            el = soup.select_one(sel)
            if el is not None and len(el.get_text(" ", strip=True)) >= MIN_REGION_TEXT:
                return el, sel
        self._warnings.append("no content region matched on %s; stored <body>"
                              % url)
        return (soup.body or soup), "body"

    @staticmethod
    def _crumb(soup) -> str:
        """A page's own breadcrumb, verbatim. Read on the LANDING page to build
        section_path, and on each linked page for extra_meta.page_breadcrumb."""
        for sel in _CRUMB_SELECTORS:
            el = soup.select_one(sel)
            if not el:
                continue
            for sr in el.select(".usa-sr-only, .visually-hidden"):
                sr.decompose()
            parts, seen = [], set()
            for piece in el.stripped_strings:
                p = _WS.sub(" ", piece).strip(" >/|›")
                if not p or p.lower() in seen or \
                        p.lower() in ("breadcrumb", "page sharing options"):
                    continue
                seen.add(p.lower())
                parts.append(p)
            if parts:
                return " > ".join(parts)
        return ""

    def _page_row(self, url: str, name: str, html: str, listed_at: str,
                  whole: bool = False) -> dict:
        """One linked page, stored with its text. The hash is over visible
        text with the furniture gone (ONBOARDING Step 2, first preference).

        SECTION_PATH IS WHERE THE ROW IS LISTED ON HHS.GOV, not the landed
        page's breadcrumb -- the user's correction of 2026-10-13. The first
        export stored each landed page's own trail, which was EMPTY on two rows
        (the Guidance Portal and federalregister.gov draw none) and on the rest
        was whatever a different site called itself ("HHS > HIPAA Home", ASPR's
        six-level trail). Now every row reads, as OCC's do:

            Home > Laws and Regulations > Explore Laws and Regulations > <link>

        The landed page's own trail is still recorded, as page_breadcrumb.
        """
        soup = BeautifulSoup(html, PARSER)
        crumb = self._crumb(BeautifulSoup(html, PARSER))
        h1 = _text(soup.find("h1"))
        # `whole`: html is already ONE cleaned block (a group_rows section), so
        # there is no region to look for -- and a one-paragraph act is under
        # MIN_REGION_TEXT, which would log a false "no region" warning.
        main, region = ((soup.body or soup), "section") if whole \
            else self._region(soup, url)
        self._strip_selectors(main)
        self._unwrap_buttons(main)
        self._strip_furniture(main)
        for sel in _CRUMB_SELECTORS:
            for el in main.select(sel):
                if not getattr(el, "decomposed", False):
                    el.decompose()
        # No "button" here -- `_unwrap_buttons` already reduced each to its label.
        for el in main.find_all(["img", "figure", "picture", "svg", "iframe",
                                 "script", "style", "noscript", "form", "nav",
                                 "input", "select", "dialog"]):
            if not getattr(el, "decomposed", False):
                el.decompose()
        _strip_other_languages(main)
        _strip_link_file_labels(main)
        self._tidy_empty(main)
        _strip_inline_colors(main)
        frag = absolutize_html(str(main), url)
        text = _WS.sub(" ", BeautifulSoup(frag, PARSER)
                       .get_text(" ", strip=True)).strip()
        return {
            "section_path": "%s > %s" % (listed_at, name),
            "page_breadcrumb": crumb,
            LINK_KEY: name,
            "page_h1": h1,
            "content_region": region,
            "title": name, "url": url,
            "depth": 1, "linked_from_title": name,
            "doc_url": url,
            "parent_page_url": self.seed_url, "status": "ok",
            "n_pdfs": 0,
            # EMPTY: a page citing several files would otherwise be stored
            # under whichever was linked first (see OCCTopicCrawler).
            "pdf_links": "",
            "text_len": len(text),
            "html_file": "", "text": text, "html": frag,
            "content_hash": content_key(text),
        }

    def _listed_at(self, landing_html: str) -> str:
        """"Home > Laws and Regulations > Explore Laws and Regulations".

        The landing page's own breadcrumb plus the block heading. Measured
        2026-10-13 on the cached landing page, its breadcrumb reads "Home > Laws
        and Regulations". If HHS ever drops it, "Home > <its h1>" says the same
        thing rather than leaving section_path to start mid-trail.
        """
        soup = BeautifulSoup(landing_html, PARSER)
        crumb = self._crumb(BeautifulSoup(landing_html, PARSER))
        if not crumb:
            h1 = _text(soup.find("h1"))
            crumb = "Home > %s" % h1 if h1 else "Home"
            self._warnings.append("landing page has no breadcrumb; section_path "
                                  "built from its <h1>")
        return "%s > %s" % (crumb, self.block_heading)

    # ------------------------------------------------------------------ #
    #  section 1: the Guidance Portal                                     #
    # ------------------------------------------------------------------ #
    def _fetch_in_page(self, page, url: str) -> Tuple[Optional[str], str]:
        """GET from inside the open tab -- see _FETCH_JS. (html, final url)."""
        if self.fetch_delay:
            time.sleep(self.fetch_delay)
        try:
            st, final, body = page.evaluate(_FETCH_JS, url)
        except Exception as e:
            self._warnings.append("fetch failed %s: %s" % (url, str(e)[:120]))
            return None, url
        if st != 200 or _DENIED in (body[:2000] or "").lower():
            self._warnings.append("HTTP %s%s: %s" % (
                st, " (Akamai)" if _DENIED in (body[:2000] or "").lower() else "",
                url))
            return None, final or url
        return body, final or url

    @staticmethod
    def _iso_date(s: str) -> str:
        """"10/8/2026" (listing) or "October 8, 2026" (detail) -> 2026-10-08."""
        s = (s or "").strip()
        for fmt in ("%m/%d/%Y", "%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return ""

    def _guidance_listing(self, html: str, base: str) -> Tuple[List[dict], int]:
        """One listing page: its items, and the LAST page index it advertises.

        Columns, measured 2026-10-13: Title | OpDiv/StaffDiv | Guidance Status |
        Issue Date. The title cell's link is the detail page.
        """
        soup = BeautifulSoup(html, PARSER)
        table = soup.select_one("main table")
        items = []
        if table is not None:
            for tr in table.select("tbody tr"):
                tds = tr.find_all("td")
                a = tds[0].find("a", href=True) if tds else None
                if a is None:
                    continue
                cell = lambda i: _text(tds[i]) if len(tds) > i else ""
                items.append({
                    "title": _clean_title(_text(a)),
                    "detail_url": urljoin(base, a["href"]).split("#")[0],
                    "opdiv": cell(1), "status": cell(2),
                    "issue_date": cell(3),
                })
        last = 0
        for a in soup.select("nav.usa-pagination a[href]"):
            m = re.search(r"[?&]page=(\d+)", a["href"])
            if m:
                last = max(last, int(m.group(1)))
        return items, last

    def _guidance_files(self, html: str, base: str) -> Tuple[List[str], str]:
        """The detail page's own document links ON HHS.GOV, and why if none.

        MEASURED 2026-10-13 on 22 detail pages: 20 carry one "Download the
        Guidance Document" link to /guidance/sites/default/files/...pdf. The
        two that do not are the two reasons a guidance item gets NO row:

            "View the Guidance" -> https://www.hrsa.gov/opa     external
            "This document has been rescinded", no link at all  no document

        External links are not recorded (the user's rule, 2026-10-13), and a
        subfolder with nothing in it is never created, so neither becomes a row.
        Both are COUNTED and reported -- see `_skipped`.
        """
        main = BeautifulSoup(html, PARSER).select_one("main")
        if main is None:
            return [], "no <main>"
        files, external = [], []
        for a in main.find_all("a", href=True):
            u = urljoin(base, a["href"]).split("#")[0]
            host = urlparse(u).netloc.lower()
            if _FILE_RE.search(urlparse(u).path):
                if host in ("www.hhs.gov", "hhs.gov"):
                    if u not in files:
                        files.append(u)
                else:
                    external.append(u)
            elif _norm(_text(a)).startswith("view the guidance") and \
                    host not in ("www.hhs.gov", "hhs.gov"):
                external.append(u)
        if files:
            return files, ""
        if external:
            return [], "external: " + external[0]
        if "rescinded" in main.get_text(" ", strip=True).lower():
            return [], "rescinded, no document"
        return [], "no document link"

    def _progress_path(self, key: str) -> str:
        return os.path.join(self.progress_dir, "%s.jsonl" % key)

    def _expand_guidance(self, page, portal_url: str, name: str,
                         listed_at: str, **_) -> List[dict]:
        """Every listing page, then every detail page -> one row per PDF.

            Explore Laws and Regulations
              HHS Guidance Portal                  subfolder (the section)
                HHS Guidance Portal                the portal page itself
                <guidance title>                   subfolder per item
                  <guidance title>                 its PDF, file_type PDF

        The user's instruction of 2026-10-13: "go further just once -- when we
        click on the link there will be a page containing the pdf; the title as
        subfolder and the pdf under it, that's it."
        """
        os.makedirs(self.progress_dir, exist_ok=True)
        prog_file = self._progress_path("guidance")
        done: Dict[str, dict] = {}
        if os.path.exists(prog_file):
            with open(prog_file, encoding="utf-8") as fh:
                for line in fh:
                    try:
                        rec = json.loads(line)
                        done[rec["detail_url"]] = rec
                    except Exception:
                        continue
            logger.info("HHS guidance: resuming, %d item(s) already read", len(done))

        # 1. THE LISTING. Read in full first, so items shifting between pages
        #    while the detail walk runs cannot drop or double anything -- the
        #    detail urls are de-duplicated before a single one is fetched.
        items: List[dict] = []
        seen = set()
        refusals = 0
        pg, last = 0, None
        while True:
            url = portal_url if pg == 0 else "%s?page=%d" % (portal_url.rstrip("/") + "/", pg)
            html, final = self._fetch_in_page(page, url)
            if html is None:
                refusals += 1
                if refusals >= self.max_refusals:
                    raise RuntimeError("HHS guidance: %d consecutive refusals on "
                                       "the listing at page %d -- stopping"
                                       % (refusals, pg))
                pg += 1
                continue
            refusals = 0
            rows, adv = self._guidance_listing(html, final)
            last = adv if last is None else max(last, adv)
            for it in rows:
                if it["detail_url"] not in seen:
                    seen.add(it["detail_url"])
                    items.append(it)
            if pg % 50 == 0:
                logger.info("HHS guidance: listing page %d/%s, %d item(s)",
                            pg + 1, (last or 0) + 1, len(items))
            pg += 1
            if pg > (last or 0) or (self.guidance_max_pages and
                                    pg >= self.guidance_max_pages):
                break
        logger.info("HHS guidance: %d listing page(s), %d distinct item(s)",
                    pg, len(items))

        # 2. THE DETAIL PAGES, resumable. Pages the LAST run could not read
        #    (`refused_file`, kept across runs) go first, while the session is
        #    fresh; the rest keep listing order.
        refused_file = os.path.join(self.progress_dir, "guidance_refused.json")
        refused_prev = set()
        if os.path.exists(refused_file):
            try:
                with open(refused_file, encoding="utf-8") as fh:
                    refused_prev = set(json.load(fh))
            except Exception:
                refused_prev = set()
            logger.info("HHS guidance: %d detail page(s) refused last run go first",
                        len(refused_prev))
        order = sorted(items, key=lambda it: it["detail_url"] not in refused_prev)

        # The kept detail record (see `detail_refresh_days`). Listing fields
        # always come from THIS run's listing; only files / skipped / page_h1
        # are reused, so a status change or reissue still moves the hash.
        cache_file = os.path.join(self.progress_dir, "guidance_details.json")
        cache: Dict[str, dict] = {}
        if os.path.exists(cache_file):
            try:
                with open(cache_file, encoding="utf-8") as fh:
                    cache = json.load(fh)
            except Exception:
                cache = {}
        if self.detail_refresh_days and cache:
            kept = [it for it in items
                    if it["detail_url"] in cache and it["detail_url"] not in done]
            quota = -(-len(items) * 7 // int(self.detail_refresh_days))
            stale = set(sorted((it["detail_url"] for it in kept),
                               key=lambda u: cache[u].get("read_at", ""))[:quota])
            reused = 0
            for it in kept:
                if it["detail_url"] not in stale:
                    done[it["detail_url"]] = dict(cache[it["detail_url"]], **it)
                    reused += 1
            logger.info("HHS guidance: %d item(s) reuse the kept detail record, "
                        "%d kept one(s) re-read as the oldest, %d to read in all",
                        reused, len(stale),
                        sum(1 for it in items if it["detail_url"] not in done))

        def read_detail(it, prog) -> bool:
            html, final = self._fetch_in_page(page, it["detail_url"])
            if html is None:
                return False
            files, why = self._guidance_files(html, final)
            h1 = _text(BeautifulSoup(html, PARSER).select_one("main h1"))
            rec = dict(it, files=files, skipped=why, page_h1=h1,
                       read_at=datetime.now().isoformat(timespec="seconds"))
            done[it["detail_url"]] = rec
            prog.write(json.dumps(rec, ensure_ascii=False) + "\n")
            prog.flush()
            return True

        refusals = 0
        refused: List[dict] = []
        with open(prog_file, "a", encoding="utf-8") as prog:
            for i, it in enumerate(order):
                if it["detail_url"] in done:
                    continue
                if not read_detail(it, prog):
                    refused.append(it)
                    refusals += 1
                    if refusals >= self.max_refusals:
                        raise RuntimeError(
                            "HHS guidance: %d consecutive refusals at item %d of "
                            "%d -- stopping; progress kept in %s"
                            % (refusals, i + 1, len(items), prog_file))
                    continue
                refusals = 0
                if (i + 1) % 250 == 0:
                    logger.info("HHS guidance: %d/%d detail page(s)",
                                i + 1, len(items))

            # 2b. ONE paced retry of the refused pages. A run of consecutive
            #     refusals here ends the retry, not the export: the main pass's
            #     items are good, and whatever is left is reported below.
            if refused:
                batch = refused[:self.retry_max]
                logger.info("HHS guidance: %d detail page(s) refused; retrying %d "
                            "once after %.0fs", len(refused), len(batch),
                            self.retry_pause)
                time.sleep(self.retry_pause)
                refusals = 0
                for it in batch:
                    time.sleep(self.retry_delay)
                    if read_detail(it, prog):
                        refusals = 0
                        continue
                    refusals += 1
                    if refusals >= self.max_refusals:
                        logger.warning("HHS guidance: %d consecutive refusals on "
                                       "retry -- ending the retry", refusals)
                        break

        # 2c. STILL UNREAD: kept for the next run (it retries them first) and
        #     reported. "coverage gap" is the orchestrator's word for a source
        #     that read less than the site lists: the run is not trusted, so
        #     these items cannot be proposed for withdrawal and the short count
        #     does not become the baseline.
        unread = [it for it in items if it["detail_url"] not in done]
        recovered = sum(1 for it in refused if it["detail_url"] in done)
        if unread:
            os.makedirs(self.progress_dir, exist_ok=True)
            with open(refused_file, "w", encoding="utf-8") as fh:
                json.dump([it["detail_url"] for it in unread], fh, indent=0)
            self._warnings.append(
                "coverage gap: HHS Guidance Portal: %d of %d detail page(s) "
                "unread (refused, %d recovered on retry); those items have no "
                "row this run and are retried first next run (%s)"
                % (len(unread), len(items), recovered, refused_file))
        else:
            try:
                os.remove(refused_file)
            except OSError:
                pass
        if refused:
            logger.info("HHS guidance: %d refused, %d recovered on retry, %d unread",
                        len(refused), recovered, len(unread))

        # 3. ROWS. Built from `items` order, not from the progress file, so an
        #    item the listing no longer shows is not resurrected from a resume.
        docs: List[dict] = []
        ids = set()
        skipped: List[dict] = []
        base_trail = [self.block_heading, name]
        for it in items:
            rec = done.get(it["detail_url"])
            if rec is None:
                skipped.append(dict(it, skipped="detail page unread"))
                continue
            if not rec["files"]:
                skipped.append(rec)
                continue
            title = it["title"] or rec.get("page_h1") or it["detail_url"]
            for f in rec["files"]:
                # Two items with the same title AND the same file are one
                # document listed twice; identity would collide, so keep one.
                key = (f, title)
                if key in ids:
                    continue
                ids.add(key)
                date = self._iso_date(it["issue_date"])
                docs.append({
                    "title": title,
                    "doc_url": f,
                    "type": (_FILE_RE.search(urlparse(f).path).group(1).upper()),
                    "found_on": it["detail_url"],
                    "section_path": "%s > %s > %s" % (listed_at, name, title),
                    TRAIL_KEY: base_trail + [title],
                    "published_date": date,
                    "department": it["opdiv"],
                    "guidance_status": it["status"],
                    "issue_date": it["issue_date"],
                    # A file not downloaded is hashed on what the listing says
                    # about it. Status and date are in it, so a draft going
                    # final, a rescission or a reissue moves the hash -- which
                    # `url|title` alone could never show.
                    "content_hash": content_key("%s|%s|%s|%s" % (
                        f, title, it["status"], it["issue_date"])),
                })
        self._skipped["HHS Guidance Portal"] = skipped
        reasons: Dict[str, int] = {}
        for s in skipped:
            k = (s.get("skipped") or "").split(":")[0]
            reasons[k] = reasons.get(k, 0) + 1
        logger.info("HHS guidance: %d item(s), %d file row(s), %d without a row %s",
                    len(items), len(docs), len(skipped), reasons)
        if reasons:
            self._warnings.append("Guidance Portal items with no hhs.gov file: %s"
                                  % reasons)
        # Keep what this run knows about every item still listed -- items gone
        # from the listing drop out. Written by exports too, which is what
        # seeds the first monitored run.
        tmp = cache_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({it["detail_url"]: done[it["detail_url"]] for it in items
                       if it["detail_url"] in done}, fh, ensure_ascii=False)
        os.replace(tmp, cache_file)
        # Complete -- the progress file has done its job.
        try:
            os.remove(prog_file)
        except OSError:
            pass
        return docs

    # ------------------------------------------------------------------ #
    #  section 2 (and any like it): a page whose headings are the tree    #
    # ------------------------------------------------------------------ #
    #: Hosts whose pages are stored as rows. hhs.gov is the regulator's own;
    #: federalregister.gov is the user's one exception to "no external links"
    #: (2026-10-13) -- where HHS points at the regulation there, it is shown.
    PAGE_HOSTS = ("www.hhs.gov", "hhs.gov", "www.federalregister.gov",
                  "federalregister.gov")
    FILE_HOSTS = ("www.hhs.gov", "hhs.gov")

    def _expand_page_tree(self, page, url: str, name: str, listed_at: str,
                          html: str = "", row: Optional[dict] = None,
                          flat: bool = False,
                          group_level: Optional[int] = None,
                          prose_links: bool = False,
                          hosts: Optional[List[str]] = None,
                          buttons: bool = False,
                          include_page: bool = True,
                          para_titles: bool = False,
                          file_hosts_extra: Optional[List[str]] = None,
                          group_rows: bool = False,
                          card_buttons: bool = False,
                          exclude: Optional[List[str]] = None,
                          no_folder: Optional[List[str]] = None,
                          deep: Optional[Dict[str, str]] = None,
                          **_) -> dict:
        """The section's page, read as a tree: headings are folders, listed
        links are the last level.

            <section name>                       e.g. HHS Regulations Review
              <page h1>                          folder
                <page h1>                        the page itself, a row
                <link in a list>                 a row (before any <h2>)
                <h2>                             folder
                  <bold-only paragraph>          folder (hhs.gov's subheading)
                    <link in a list>             a row: PDF, or the page HTML

        REBUILT FROM THE LIVE PAGE ON EVERY RUN -- nothing below is a list of
        names. A new heading or group becomes a folder; a removed link becomes
        a `disappeared` candidate; a renamed heading moves its rows (new rows
        plus withdrawal proposals, because doc_path is part of identity).

        WHAT BECOMES A ROW. Links inside a list item, or a paragraph that is
        nothing but that link. A link inside running prose is a citation, not
        an item, and stays in the stored body -- counted, not dropped.
          hhs.gov file (.pdf, .docx ...)    file row, hashed url|title
          hhs.gov page                      fetched, stored as HTML, text hash
          federalregister.gov page          the same (the user's exception)
          anything else                     NO row; counted in the warnings
        One level only: a linked page's own links stay inside its body.

        FLAT (the `page_links` handler) uses the same rules but makes NO
        folders from the page's headings: the page and its listed links sit
        side by side in the section's folder. The user's choice for "Policy on
        unlawful, unconstitutional, or burdensome regulations", 2026-10-13.

        Per-section options, set in config/sources/hhs.yml:
          group_level  only headings of THIS level become folders, directly
                       under the section (Health IT: each "Key Legislation"
                       <h3> -- HITECH Act, HIPAA ... -- and not the <h2> above
                       them). A heading with no row under it makes no folder.
          prose_links  links inside running text are rows too. Health IT's
                       page has NO list links at all: every reference is a
                       sentence ("ONC is implementing ... HIPAA Privacy Rule").
          hosts        extra hosts counted as the regulator's own, for pages
                       AND files (healthit.gov for Health IT). Kept per section
                       so it cannot create rows anywhere else.
          buttons      ONLY the page's USWDS buttons (a.usa-button) are items,
                       each a FOLDER named by its label holding the page it
                       opens, titled by that page's own heading. Human subjects
                       research protections (45 CFR 46), user, 2026-10-13: "these
                       blue things become subfolders and under them is content".
          include_page False drops the section's own page row ("do not include
                       the main html", same instruction).
          para_titles  a link inside a SHORT paragraph (<= 300 chars) takes
                       the whole paragraph as its title. The PREP Act page
                       writes each item as "<link>Ninth Amendment ...</link>
                       (September 14, 2021)", and its slide deck as "Ninth
                       Amendment ... (<link>PDF Presentation</link>)" -- the
                       link text alone would title that row "PDF Presentation".
          file_hosts_extra
                       hosts whose FILES count, without making their pages
                       rows: govinfo.gov and public-inspection.federalregister.gov
                       serve Federal Register issues as PDFs (PREP Act's 2nd,
                       3rd, 5th, 7th and 12th Amendments).
          group_rows   with group_level: EVERY such heading is a folder holding
                       its own block of the page (the heading and its text, up
                       to the next heading of that level) as an HTML row,
                       addressed as <page>#<anchor>. A heading whose links are
                       all external, or that has none, is still a folder, and
                       an edit to one act's paragraph registers on that act's
                       row. Health IT, user, 2026-10-15: 21st Century Cures
                       Act, MACRA and the Affordable Care Act had no folder.
          card_buttons a USWDS card's "learn more" button (a.usa-button in a
                       .usa-card) is an item too, filed under the card's
                       heading and titled by the page it opens. HIPAA Privacy
                       Rule, user, 2026-10-15: "cover the learn more sections".
          exclude      regexes, case-insensitive, tried on each item's title,
                       every folder above it, and its url path. A match makes
                       NO row and is counted in the warnings. Read on every
                       run, so an item HHS adds later is excluded too ("exclude
                       faqs ... wherever u see it", same instruction).
          no_folder    heading names that make NO folder; their items sit in
                       the folder above ("I would like info on", user,
                       2026-10-15).
          deep         {item name: "sidenav" | "cards"} -- that item's LANDED
                       page is walked one structure further (_deep_landed),
                       keyed by its link text or its card heading. Items not
                       named stay one page.

        ACCORDIONS ARE ALWAYS FOLDERS. A USWDS accordion's label
        (div.usa-accordion__heading) is a heading in all but markup, like a
        bold-only paragraph -- the PREP Act page files every declaration under
        one ("Declaration and Amendments", "Anthrax", ...). Without this the
        whole page would be read as two <h2> folders.

        A FOLDER NAMED AFTER THE PAGE'S <h1> IS SKIPPED WHEN IT REPEATS THE
        SECTION NAME ("Public Readiness and Emergency Preparedness (PREP) Act"
        twice over).
        """
        soup = BeautifulSoup(html, PARSER)
        main, _region = self._region(soup, url)
        self._strip_selectors(main)
        self._unwrap_buttons(main)
        self._strip_furniture(main)
        for sel in _CRUMB_SELECTORS:
            for el in main.select(sel):
                if not getattr(el, "decomposed", False):
                    el.decompose()

        h1 = main.find("h1")
        page_title = _clean_title(_text(h1)) or name
        base = [self.block_heading, name] + \
            ([] if (flat or group_level or buttons or not include_page
                    or _norm(page_title) == _norm(name)) else [page_title])
        page_hosts = tuple(self.PAGE_HOSTS) + tuple(hosts or ())
        file_hosts = tuple(self.FILE_HOSTS) + tuple(hosts or ()) + \
            tuple(file_hosts_extra or ())
        if row is not None and not include_page:
            row["_drop"] = True
        if row is not None:
            # The section's own page becomes the first row INSIDE its folder,
            # under its own <h1> -- the user's choice of 2026-10-13.
            row[TRAIL_KEY] = list(base)
            row["title"] = page_title
            row["section_path"] = "%s > %s" % (
                listed_at, " > ".join(base[1:] + [page_title]))

        heads: Dict[int, str] = {}
        label: Optional[str] = None
        items: List[Tuple[str, str, List[str], bool]] = []
        prose_skipped = 0
        if buttons:
            # `_unwrap_buttons` only touches <button>; these are <a> styled as
            # buttons, so their class survives the clean-up above.
            for a in main.select("a.usa-button[href]"):
                href = a["href"].strip()
                title = _clean_title(_text(a))
                if not title or href.startswith(("#", "mailto:", "tel:")):
                    continue
                # in_prose=True: retitle by the landed page's own heading.
                items.append((title, _nolang(urljoin(url, href).split("#")[0]),
                              base + [title], True))
        for el in ([] if buttons else main.find_all(True)):
            if getattr(el, "decomposed", False) or el is h1:
                continue
            if re.fullmatch(r"h[2-6]", el.name or ""):
                lvl = int(el.name[1])
                heads = {k: v for k, v in heads.items() if k < lvl}
                # "I would like info on ..." -> "I would like info on"
                t = re.sub(r"\s*(\.\.\.|…)$", "",
                           _clean_title(_text(el))).rstrip(":").strip()
                if t and _norm(t) not in {_norm(n) for n in (no_folder or ())}:
                    heads[lvl] = t
                label = None
                continue
            if _is_bold_label(el) or \
                    "usa-accordion__heading" in (el.get("class") or []):
                label = _clean_title(_text(el)).rstrip(":").strip() or None
                continue
            if el.name != "a" or not el.get("href"):
                continue
            href = el["href"].strip()
            if href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            if card_buttons and "usa-button" in (el.get("class") or []) and \
                    el.find_parent(class_="usa-card") is not None:
                # "learn more" names nothing: the card's heading is the
                # folder, and the landed page's <h1> titles the row.
                trail = base + [heads[k] for k in sorted(heads)]
                items.append((trail[-1] if len(trail) > len(base)
                              else _clean_title(_text(el)),
                              _nolang(urljoin(url, href).split("#")[0]), trail, True))
                continue
            holder = el.find_parent(["li", "p"])
            if holder is None:
                continue
            in_prose = holder.name == "p" and \
                _norm(holder.get_text(" ", strip=True)) != _norm(_text(el))
            if in_prose and not prose_links:
                prose_skipped += 1
                continue
            title = _clean_title(_text(el))
            retitle = in_prose
            if in_prose and para_titles:
                ptext = _clean_title(_text(holder))
                # An ITEM ("Ninth Amendment ... (September 14, 2021)"), not a
                # SENTENCE. The PREP page's FAQs link sits in prose ending
                # "...view our COVID-19 PREP Act FAQs." -- a full stop marks a
                # sentence, which then falls back to the landed page's heading.
                if len(ptext) <= 300 and not re.search(r"[.!?]\s*$", ptext):
                    # The paragraph IS the item's name -- keep it, and do not
                    # let the landed page's heading replace it.
                    title, retitle = ptext, False
            if not title:
                continue
            if flat:
                trail = list(base)
            elif group_level:
                trail = base + ([heads[group_level]]
                                if group_level in heads else [])
            else:
                trail = base + [heads[k] for k in sorted(heads)] + \
                    ([label] if label else [])
            items.append((title, _nolang(urljoin(url, href).split("#")[0]), trail,
                          retitle))

        docs: List[dict] = []
        pages: List[dict] = []
        skipped: List[dict] = []
        if group_rows and group_level:
            tag = "h%d" % group_level
            for h in main.find_all(tag):
                t = _clean_title(_text(h)).rstrip(":").strip()
                if not t:
                    continue
                block = [str(h)]
                for sib in h.find_next_siblings():
                    if re.fullmatch(r"h[1-%d]" % group_level, sib.name or ""):
                        break
                    block.append(str(sib))
                anchor = h.get("id") or next(
                    (p.get("id") for p in h.parents if p.get("id")), "") or \
                    re.sub(r"[^a-z0-9]+", "-", t.lower()).strip("-")
                gurl = "%s#%s" % (url.split("#")[0], anchor)
                grow = self._page_row(gurl, t, "<div>%s</div>" % "".join(block),
                                      listed_at, whole=True)
                if not grow["text"]:
                    continue
                grow[TRAIL_KEY] = base + [t]
                grow["section_path"] = "%s > %s" % (
                    listed_at, " > ".join(base[1:] + [t, t]))
                grow["parent_page_url"] = url
                pages.append(grow)
                logger.info("    %-56s %7d chars (block)", t[:56],
                            grow["text_len"])
        excl = [re.compile(p, re.I) for p in (exclude or ())]
        self._deep_seen = set()
        for title, u, trail, in_prose in items:
            if excl and any(rx.search(s) for rx in excl
                            for s in [title, urlparse(u).path] + trail[2:]):
                skipped.append({"title": title, "url": u, "why": "excluded"})
                continue
            host = urlparse(u).netloc.lower()
            sp = "%s > %s" % (listed_at, " > ".join(trail[1:] + [title]))
            if _FILE_RE.search(urlparse(u).path):
                if host not in file_hosts:
                    skipped.append({"title": title, "url": u, "why": "external file"})
                    continue
                docs.append({
                    "title": title, "doc_url": u,
                    "type": _FILE_RE.search(urlparse(u).path).group(1).upper(),
                    "found_on": url, "section_path": sp, TRAIL_KEY: trail,
                    "content_hash": content_key("%s|%s" % (u, title)),
                })
                continue
            if host not in page_hosts:
                skipped.append({"title": title, "url": u, "why": "external page"})
                continue
            phtml, pfinal = self._get(page, u)
            if phtml is None:
                skipped.append({"title": title, "url": u, "why": "unreadable"})
                continue
            anchor = title
            prow = self._page_row(pfinal, title, phtml, listed_at)
            if in_prose:
                # A link inside a sentence is named by the sentence: Health IT's
                # HITAC link reads "select portions of the HITECH Act that
                # relate to ONC's work". The page it lands on names itself, so
                # its own <h1> is the title; the anchor text is kept in
                # LINK_KEY. A FILE has no heading and keeps its link text.
                #
                # THE <h1> OF THE CLEANED BODY, not of the raw page. A Federal
                # Register document's FIRST <h1> is its type ("Presidential
                # Document", "Notice", in div.main-title-bar, stripped as
                # chrome); the document's own title is the next one.
                body = BeautifulSoup(prow["html"], PARSER)
                # Falls back to the first heading of any level: an eCFR single
                # section (§ 46.104) carries no <h1> in its div.section.
                h1 = _clean_title(_text(
                    body.find("h1") or body.find(["h2", "h3", "h4"])))
                if h1:
                    title = h1
                    prow["title"] = h1
                    prow["linked_from_title"] = h1
                    sp = "%s > %s" % (listed_at, " > ".join(trail[1:] + [title]))
            prow[LINK_KEY] = anchor
            prow[TRAIL_KEY] = trail
            prow["section_path"] = sp
            prow["parent_page_url"] = url
            mode = (deep or {}).get(anchor) or (deep or {}).get(trail[-1])
            if mode:
                dp, dd = self._deep_landed(
                    page, mode, pfinal, phtml, prow, trail, listed_at,
                    page_hosts, file_hosts, excl, skipped, section_url=url)
                pages.extend(dp)
                docs.extend(dd)
                continue
            pages.append(prow)
            logger.info("    %-56s %7d chars", title[:56], prow["text_len"])

        self._skipped[name] = skipped
        logger.info("HHS %s: %d page row(s), %d file row(s), %d skipped, "
                    "%d prose link(s) left in the body",
                    name, len(pages), len(docs), len(skipped), prose_skipped)
        if skipped:
            self._warnings.append("%s: %d link(s) without a row: %s" % (
                name, len(skipped), sorted({s["why"] for s in skipped})))
        return {"documents": docs, "pages": pages}

    # ------------------------------------------------------------------ #
    #  one structure further: a landed page's side menu or cards          #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _url_key(u: str) -> str:
        return u.split("#")[0].split("?")[0].rstrip("/").lower()

    def _deep_landed(self, page, mode: str, url: str, html: str, prow: dict,
                     trail: List[str], listed_at: str, page_hosts, file_hosts,
                     excl, skipped: List[dict], section_url: str = ""):
        """Walk a landed page's own structure, the user's instruction for the
        HIPAA Privacy Rule section (2026-10-15): "has levels as we had for many
        sections before ... record any pdfs involved within those pages".

          sidenav  the page's USWDS side menu (ul.usa-sidenav), from the BRANCH
                   holding the page: the page's own menu item when it has
                   children (HIPAA for Individuals), else its parent's (Security
                   Rule NPRM -> "Security"). The branch is a folder holding its
                   own page; a menu item WITH children is a subfolder holding
                   its page and them, as deep as the menu goes.
          cards    the page's USWDS cards: each "learn more" opens one page,
                   filed under the card's heading (Civil Rights).

        Every page so reached is stored, and its hhs.gov FILES are rows too.
        Every menu item is a folder (its menu label) holding its page and that
        page's files; the branch's own page sits in the branch folder. `exclude` applies
        to every item; a link back to the section's own page is skipped.
        """
        pages: List[dict] = []
        docs: List[dict] = []
        seen_files: set = set()
        # Pages stored by THIS SECTION'S walks, by LANDED url: Conscience's
        # menu root (/conscience/index.html) redirects to its first child's
        # page, which would otherwise be stored twice under one identity; and
        # Civil Rights' "Conscience" card would store that page and its ~25
        # PDFs a second time. Reset per section in _expand_page_tree.
        seen_pages = self._deep_seen

        def first(r: Optional[dict]) -> Optional[dict]:
            if r is None or self._url_key(r["url"]) in seen_pages:
                return None
            seen_pages.add(self._url_key(r["url"]))
            return r

        def bad(title: str, u: str, folder: List[str]) -> bool:
            return bool(excl) and any(
                rx.search(s) for rx in excl
                for s in [title, urlparse(u).path] + folder[2:])

        def files_of(row: dict) -> List[Tuple[str, str]]:
            out, seen = [], set()
            for a in BeautifulSoup(row["html"], PARSER).find_all("a", href=True):
                u = _nolang(urljoin(row["url"], a["href"].strip()).split("#")[0])
                # Conscience writes its govinfo.gov citations as
                # "www.govinfo.gov/content/..." with no scheme; the stored body
                # is already absolute, so they read hhs.gov/www.govinfo.gov/...
                # -- a path that does not exist. The file is that host's.
                m = re.match(r"https?://[^/]+/((?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)*"
                             r"\.(?:gov|com|org|net)/.*)", u, re.I)
                if m:
                    u = "https://" + m.group(1)
                if not _FILE_RE.search(urlparse(u).path) or u in seen:
                    continue
                seen.add(u)
                out.append((_clean_title(_text(a)) or
                            os.path.basename(urlparse(u).path), u))
            return out

        def file_row(title: str, u: str, folder: List[str], found_on: str):
            if bad(title, u, folder):
                skipped.append({"title": title, "url": u, "why": "excluded"})
                return
            if urlparse(u).netloc.lower() not in file_hosts:
                skipped.append({"title": title, "url": u, "why": "external file"})
                return
            key = (u, tuple(folder))
            if key in seen_files:
                return
            seen_files.add(key)
            docs.append({
                "title": title, "doc_url": u,
                "type": _FILE_RE.search(urlparse(u).path).group(1).upper(),
                "found_on": found_on, TRAIL_KEY: list(folder),
                "section_path": "%s > %s" % (
                    listed_at, " > ".join(folder[1:] + [title])),
                "content_hash": content_key("%s|%s" % (u, title)),
            })

        def fetch(label: str, u: str) -> Optional[dict]:
            if self._url_key(u) == self._url_key(url):
                return dict(prow)
            h, final = self._get(page, u)
            if h is None:
                skipped.append({"title": label, "url": u, "why": "unreadable"})
                return None
            r = self._page_row(final, label, h, listed_at)
            body = BeautifulSoup(r["html"], PARSER)
            h1 = _clean_title(_text(body.find("h1") or
                                    body.find(["h2", "h3", "h4"])))
            if h1:
                r["title"] = r["linked_from_title"] = h1
            r[LINK_KEY] = label
            r["parent_page_url"] = url
            return r

        def place(r: dict, label: str, folder: List[str], has_kids: bool):
            """EVERY menu item is a subfolder named by its menu label, holding
            its one page and that page's own files (user, 2026-10-15:
            "Security > Security Rule NPRM, Summary of the Security Rule ...
            as subfolders (1 html) and then the respective pdfs within")."""
            own = folder + [label]
            fl = [(t, fu) for t, fu in files_of(r)
                  if urlparse(fu).netloc.lower() in file_hosts
                  and not bad(t, fu, own)]
            r[TRAIL_KEY] = own
            r["section_path"] = "%s > %s" % (
                listed_at, " > ".join(own[1:] + [r["title"]]))
            pages.append(r)
            logger.info("      %-54s %7d chars, %d file(s)",
                        r["title"][:54], r["text_len"], len(fl))
            for t, fu in fl:
                file_row(t, fu, own, r["url"])
            return own

        def norm_href(a) -> str:
            u = urljoin(url, a["href"].strip()).split("#")[0]
            # "Security Guidance" is linked as ...?language=es -- the Spanish
            # view, and other languages are excluded. Drop that one parameter.
            return _nolang(u)

        if mode == "sidenav":
            soup = BeautifulSoup(html, PARSER)
            nav = soup.select_one("ul.usa-sidenav")
            cur = nav.select_one('a[aria-current="page"]') if nav else None
            if cur is None:
                self._warnings.append("%s: no side menu found; stored the page "
                                      "alone" % url)
                pages.append(prow)
                return pages, docs
            li = cur.find_parent("li")
            branch = li if li.find("ul", recursive=False) else \
                (li.find_parent("li") or li)
            ba = branch.find("a")
            bname = _clean_title(_text(ba))
            folder = list(trail) + ([] if _norm(bname) == _norm(trail[-1])
                                    else [bname])
            broot = first(fetch(bname, norm_href(ba)))
            if broot is not None:
                broot[TRAIL_KEY] = folder
                broot["section_path"] = "%s > %s" % (
                    listed_at, " > ".join(folder[1:] + [broot["title"]]))
                pages.append(broot)

            def walk(ul, fold: List[str]):
                for item in ul.find_all("li", recursive=False):
                    a = item.find("a", href=True)
                    if a is None:
                        continue
                    label = _clean_title(_text(a))
                    u = norm_href(a)
                    if bad(label, u, fold):
                        skipped.append({"title": label, "url": u, "why": "excluded"})
                        continue
                    if _FILE_RE.search(urlparse(u).path):
                        file_row(label, u, fold, url)
                        continue
                    if urlparse(u).netloc.lower() not in page_hosts:
                        skipped.append({"title": label, "url": u,
                                        "why": "external page"})
                        continue
                    kids = item.find("ul", recursive=False)
                    r = first(fetch(label, u))
                    own = place(r, label, fold, kids is not None)                         if r is not None else fold + [label]
                    if kids is not None:
                        walk(kids, own)

            sub = branch.find("ul", recursive=False)
            if sub is not None:
                walk(sub, folder)
            if broot is not None:
                # AFTER the menu: a file the menu lists ("Conscience Dear
                # Colleague Letter") keeps the menu's name, not the body's
                # link text (the bare url).
                for t, fu in files_of(broot):
                    file_row(t, fu, folder, broot["url"])
            return pages, docs

        if mode == "cards":
            first(prow)
            pages.append(prow)
            for t, fu in files_of(prow):
                file_row(t, fu, trail, url)
            soup = BeautifulSoup(html, PARSER)
            main, _r = self._region(soup, url)
            for a in main.select(".usa-card a.usa-button[href]"):
                card = a.find_parent(class_="usa-card")
                hd = card.find(re.compile(r"h[2-6]"))
                label = _clean_title(_text(hd)) if hd else _clean_title(_text(a))
                u = norm_href(a)
                fold = list(trail) + [label]
                if bad(label, u, fold):
                    skipped.append({"title": label, "url": u, "why": "excluded"})
                    continue
                if section_url and self._url_key(u) == self._url_key(section_url):
                    skipped.append({"title": label, "url": u,
                                    "why": "the section's own page"})
                    continue
                if _FILE_RE.search(urlparse(u).path):
                    file_row(label, u, fold, url)
                    continue
                if urlparse(u).netloc.lower() not in page_hosts:
                    skipped.append({"title": label, "url": u, "why": "external page"})
                    continue
                r = first(fetch(label, u))
                if r is None:
                    continue
                r[TRAIL_KEY] = fold
                r["section_path"] = "%s > %s" % (
                    listed_at, " > ".join(fold[1:] + [r["title"]]))
                pages.append(r)
                logger.info("      %-54s %7d chars", r["title"][:54], r["text_len"])
                for t, fu in files_of(r):
                    file_row(t, fu, fold, r["url"])
            return pages, docs

        raise ValueError("deep mode %r: expected 'sidenav' or 'cards'" % mode)

    # ------------------------------------------------------------------ #
    #  the walk                                                           #
    # ------------------------------------------------------------------ #
    def _run_crawl(self) -> dict:
        # NOT super()._run_crawl(): the engine runs headless (refused by
        # Akamai) and would follow these pages' links into rows.
        from playwright.sync_api import sync_playwright

        t0 = time.time()
        pages: List[dict] = []
        documents: List[dict] = []
        unread = 0
        handlers = {"guidance_portal": self._expand_guidance,
                    "page_tree": self._expand_page_tree,
                    "page_links": lambda *a, **k: self._expand_page_tree(
                        *a, flat=True, **k)}
        def _spec(v):
            # `expand` values are a handler name, or a mapping with `handler`
            # plus that handler's options (see _expand_page_tree).
            if isinstance(v, dict):
                opts = dict(v)
                return opts.pop("handler", ""), opts
            return v, {}
        for name, v in self.expand.items():
            h = _spec(v)[0]
            if h not in handlers:
                raise ValueError("expand[%r] = %r: no such handler (have %s)"
                                 % (name, h, sorted(handlers)))
        with sync_playwright() as p:
            ctx = self._browser(p)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                html, final = self._get(page, self.seed_url)
                if html is None:
                    raise RuntimeError(
                        "the HHS landing page could not be read (%s) -- "
                        "refusing to report an empty source" % self.seed_url)
                links = self._links(html, final)
                listed_at = self._listed_at(html)
                logger.info("HHS: %d link(s) in %r, listed at %r", len(links),
                            self.block_heading, listed_at)
                for name, url in links:
                    lhtml, lfinal = self._get(page, url)
                    if lhtml is None:
                        unread += 1
                        logger.warning("  %-60s UNREAD", name[:60])
                        continue
                    row = self._page_row(lfinal, name, lhtml, listed_at)
                    # EVERY SECTION IS A SUBFOLDER, and its own page is the
                    # first row inside it -- a single-row section too
                    # (Whistleblower, Complaints and Appeals; user, 2026-10-15),
                    # so all ten links look alike under the block heading.
                    row[TRAIL_KEY] = [self.block_heading, name]
                    if name in self.expand:
                        hname, hopts = _spec(self.expand[name])
                        found = handlers[hname](
                            page, lfinal, name, listed_at, html=lhtml, row=row,
                            **hopts)
                        extra_pages: List[dict] = []
                        if isinstance(found, dict):
                            # A handler that also stores PAGES (page_tree).
                            extra_pages = found.get("pages") or []
                            found = found.get("documents") or []
                        pages.extend(extra_pages)
                        if not found and not extra_pages:
                            # A subfolder holding only its own page is not
                            # what was asked for; say so rather than ship it.
                            self._warnings.append(
                                "%r was to be expanded and produced no "
                                "documents" % name)
                        documents.extend(found)
                    if row.pop("_drop", False):
                        # The section's own page was asked NOT to be a row
                        # (include_page: false). Its children carry the folder.
                        logger.info("  %-60s (page not stored)", name[:60])
                        continue
                    pages.append(row)
                    logger.info("  %-60s %7d chars  %s", name[:60],
                                row["text_len"], row["content_region"])
            finally:
                ctx.close()

        if unread and not pages:
            raise RuntimeError("every HHS link was unreadable -- refusing to "
                               "report an empty source")
        documents = self._retitle_files(documents)
        logger.info("HHS: %d page row(s), %d document row(s), %d unread, %.0fs",
                    len(pages), len(documents), unread, time.time() - t0)
        return {
            "shape": "generic",
            "pages": pages,
            "documents": documents,
            "run": {"seed": self.seed_url, "links": len(pages) + unread,
                    "unread": unread, "blocked_pages": unread},
        }

    def _retitle_files(self, documents: List[dict]) -> List[dict]:
        """File rows whose link text names nothing (user, 2026-10-15):

          A. the same file linked elsewhere in this crawl under a real title
             takes that title -- "access" -> "Right to Access Memo",
             "paper or electronically" -> "Privacy, Security, and Electronic
             Health Records". Urls are compared without ?language= (_nolang).
          B. "Download <thing>" takes its folder as prefix: "Right to Access
             - 4x5 Pocket Brochure", "HIV and HIPAA - 728x90 Web Banners".

        The link text is kept in extra_meta (LINK_KEY). A weak title with no
        better name anywhere stays as HHS wrote it. Rows that become identical
        (same url, folder and title) are stored once."""
        best: Dict[str, Counter] = {}
        # NOT the Guidance Portal's rows: each is titled by its own listing
        # entry, and 378 PDFs are shared by two or more DIFFERENT items.
        def own(d):
            return not d.get("guidance_status")
        for d in documents:
            if not own(d):
                continue
            d["doc_url"] = _nolang(d["doc_url"])
            if not _weak_title(d["title"]):
                best.setdefault(d["doc_url"], Counter())[d["title"]] += 1
        out, seen, n = [], set(), 0
        for d in documents:
            t = d["title"]
            if own(d) and _weak_title(t):
                d.setdefault(LINK_KEY, t)
                if d["doc_url"] in best:
                    t = best[d["doc_url"]].most_common(1)[0][0]
                elif re.match(r"(?i)download\s+", t) and d.get(TRAIL_KEY):
                    t = "%s – %s" % (d[TRAIL_KEY][-1],
                                          re.sub(r"(?i)^download\s+", "", t))
                if t != d["title"]:
                    n += 1
                    d["title"] = t
                    d["section_path"] = re.sub(r"[^>]*$", lambda m: " " + t,
                                               d.get("section_path") or "", count=1)
                    if "content_hash" in d:
                        d["content_hash"] = content_key("%s|%s" % (d["doc_url"], t))
            key = (d["doc_url"], tuple(d.get(TRAIL_KEY) or ()), d["title"])
            if key in seen:
                continue
            seen.add(key)
            out.append(d)
        if n:
            logger.info("HHS: %d weak file title(s) renamed, %d duplicate(s) "
                        "dropped", n, len(documents) - len(out))
        return out

    # ------------------------------------------------------------------ #
    #  rows                                                               #
    # ------------------------------------------------------------------ #
    def _page_is_document(self, r: dict) -> bool:
        """Every record is one of the block's links and all are wanted. The
        inherited length floor could only subtract (four DFPI laws lost their
        folders to it)."""
        return True

    def _apply_trail(self, doc, rec: dict):
        """doc_path is [regulator, source_system, block heading, title].

            HHS > Laws and Regulations > Explore Laws and Regulations > <title>

        The block's <h2> is the one folder, per the library's rule that a
        landing page's headings become its subfolders (user's choice,
        2026-10-13). SET UNCONDITIONALLY from the block heading, never derived
        from `section_path`: the inherited builder would fold the whole listing
        trail ("Home > Laws and Regulations > ...") into folders, which is how
        the HIPAA build of 2026-10-12 landed two rows six deep.
        """
        if doc is None:
            return doc
        # An EXPANDED section's rows carry a longer trail (TRAIL_KEY):
        #     [block, section]          its own page
        #     [block, section, title]   one of its documents
        # A ">" inside a title would split into an extra folder when the trail
        # is joined, so it is swapped for a look-alike in the FOLDER name only.
        trail = rec.get(TRAIL_KEY) or [self.block_heading]
        doc.doc_path = self._doc_path(
            " > ".join(s.replace(">", "›") for s in trail), doc.title)
        if isinstance(getattr(doc, "extra_meta", None), dict):
            doc.extra_meta["section_path"] = rec.get("section_path") or ""
            for k in ("page_breadcrumb", LINK_KEY, "page_h1", "content_region"):
                if rec.get(k):
                    doc.extra_meta[k] = rec[k]
        return doc

    def _doc_from_page_row(self, r: dict, shape: str):
        doc = super()._doc_from_page_row(r, shape)
        if doc is not None and isinstance(getattr(doc, "extra_meta", None), dict):
            doc.extra_meta["content_hash_basis"] = "visible-text-minus-furniture"
        return self._apply_trail(doc, r)

    def _doc_from_document_row(self, d: dict, shape: str):
        """A file row from an expanded section -- today, a Guidance Portal PDF.

        The inherited builder leaves published_date empty ("a link walk cannot
        read issue dates"); this one can, because the portal's listing states
        each item's Issue Date and issuing division.
        """
        doc = self._apply_trail(super()._doc_from_document_row(d, shape), d)
        if doc is None:
            return doc
        doc.published_date = d.get("published_date") or None
        doc.year = (d.get("published_date") or "")[:4] or None
        doc.department = d.get("department") or None
        if isinstance(getattr(doc, "extra_meta", None), dict):
            doc.extra_meta["content_hash_basis"] = "url|title|status|issue_date"
            doc.extra_meta["detail_page"] = d.get("found_on") or ""
            for k in ("guidance_status", "issue_date"):
                if d.get(k):
                    doc.extra_meta[k] = d[k]
        return doc

    def fetch_documents(self, limit: Optional[int] = None):
        docs = super().fetch_documents(limit=limit)
        # A page `_get` could not read is a row this run lacks. Most often the
        # FR/eCFR block: ~36 rows, 0.6% of the source, inside the orchestrator's
        # 5% count tolerance -- so without this they would be ruled
        # `disappeared` and proposed for withdrawal. "coverage gap" is the
        # orchestrator's word for a source that read less than the site holds
        # (check_run_trustworthy): the run is not trusted to withdraw anything.
        # (The guidance walk reports its own unread pages the same way.)
        unread = [w for w in self._warnings if w.startswith((
            "SKIPPED ", "skipped (federalregister", "Akamai refused",
            "interstitial did not clear", "could not open "))]
        if unread:
            self._warnings.append(
                "coverage gap: %d page(s) outside the Guidance Portal unread this "
                "run (%d FR/eCFR block); their rows are missing, not withdrawn"
                % (len(unread), sum(1 for w in unread
                                    if w.startswith(("SKIPPED ", "skipped (")))))
        run = dict((self.last_result or {}).get("run") or {})
        run["warnings"] = list(run.get("warnings") or []) + self._warnings
        self.last_result = {
            "run": run,
            "by_source": {self.source_system: len(docs)},
            "source": self.seed_url,
            # WITHOUT THIS KEY THE GATE NEVER SEES A GAP. Every config-built
            # regulator runs inside a CompositeCrawler, and its last_result
            # forwards a source's "coverage gap" warnings ONLY from a source that
            # declares `coverage_gaps` (generic_crawler_wrapper.py, the CMA fix).
            # Found 2026-10-22: until then the gap warnings above reached
            # run.warnings here and stopped, so a short HHS run was trusted.
            "coverage_gaps": [{"source": self.source_system, "why": w}
                              for w in self._warnings
                              if "coverage gap" in w.lower()],
        }
        logger.info("HHSExploreCrawler[%s] -> %d document(s)",
                    self.source_system, len(docs))
        return docs
