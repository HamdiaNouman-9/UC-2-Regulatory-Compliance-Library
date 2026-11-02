# -*- coding: utf-8 -*-
"""OCC: three topic areas, and the subtopic page under each is the last level.

The Office of the Comptroller of the Currency organises its material under five
topic areas; the library takes three of them, and each is a SOURCE SYSTEM:

    OCC > Supervision and Examination > Bank Secrecy Act (BSA)
    OCC > Laws and Regulations        > OCC Regulations
    OCC > Consumers and Communities   > Consumer Protection

THE SUBTOPIC PAGE IS THE DOCUMENT, NOT A FOLDER OF DOCUMENTS. Each of the 23
subtopics the mega-menu names contributes exactly ONE row: the page itself, with
its text stored. 12 + 4 + 7 = 23 rows for the whole regulator.

ITS LINKS ARE RECORDED, NOT PROMOTED. The Bank Secrecy Act page lists eleven
things -- BSA/AML Examinations, Counter-Terrorist Financing, Suspicious Activity
Reports and the rest. Every one of them survives INSIDE the stored body, as a
live link a reader can follow, because `absolutize_html` rewrites them to
absolute urls before the body is stored. None of them becomes a row. That is the
difference between recording a link and promoting it to a document, and it is
what the manual library shows: one readable page under Bank Secrecy Act, not
eleven stubs around it.

-----------------------------------------------------------------------------
THIS REPLACED TWO EARLIER READINGS, BOTH WRONG, BOTH MEASURED
-----------------------------------------------------------------------------
Recorded because the file would otherwise look like it had always been this
simple, and because each wrong turn cost a full export to see.

  1. SUBTOPIC AS A FOLDER OF BARE LINKS -- 349 rows, of which 326 were a url
     and a title with NOTHING BEHIND THEM. Only the 23 subtopic pages carried
     text (7%), against fdic.xlsx's 984 of 991 (99%). The Bank Secrecy Act
     folder held eleven empty stubs around one readable page.

  2. THE SAME, WITH EVERY LINK FETCHED FOR ITS BODY -- still 349 rows, now 283
     with text (81%). Better, and still not what was asked for: the eleven
     stubs became eleven readable pages, so Bank Secrecy Act showed TWELVE
     documents where the manual library shows one.

The rule is the one the request stated from the start -- "the main topic and its
subtopic htmls/pdfs/docs right under it, this is the last level". The subtopic
items ARE the last level. Reading it as "the subtopic's links are the last
level" is what produced both of the above.

-----------------------------------------------------------------------------
WHY THE SUBTOPICS COME FROM THE SITEMAP AND NOT THE TOPIC INDEX PAGE
-----------------------------------------------------------------------------
Each topic index page carries a "<Topic> Topics" block that looks like the list
to read. It is a TRUNCATED one, ending in "Explore all ... View All". Measured
2026-10-12 on Consumers & Communities:

    the index page lists  6   (no Financial Literacy; "Strategic Partnerships"
                               under a longer name)
    the mega-menu lists   7

/topics/topics-sitemap.html holds the tree the mega-menu is drawn from, and its
level-1 entries match the menu EXACTLY -- 12 for Supervision & Examination, 4
for Laws & Regulations, 7 for Consumers & Communities. The menu is what was
asked for, so the sitemap is the source of record and the index pages are not
read at all.

Levels 2 and 3 of that sitemap (Bank Management > Minority Depository
Institutions, Capital Markets > Asset Management > Custody Services) are not
rows and not folders. They are links on their parent's page, and they stay
there.

-----------------------------------------------------------------------------
NO BROWSER
-----------------------------------------------------------------------------
Unlike dfpi.ca.gov, www.occ.gov answers plain `requests` with 200 on the first
try, so this class walks with a Session rather than driving Playwright. 24
requests for a source -- one sitemap and up to 23 subtopic pages -- and about
30 seconds for the whole regulator.
"""
from __future__ import annotations

import logging
import re
import time
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from crawler.generic_crawler_wrapper import GenericSiteCrawler
from generic_crawler.crawler import absolutize_html, content_key, profile_for

logger = logging.getLogger(__name__)

_WS = re.compile(r"\s+")


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


#: lxml, NOT "html.parser", AND THIS IS NOT A PREFERENCE.
#:
#: Python's html.parser MIS-NESTS occ.gov's markup. MEASURED 2026-10-12 on
#: /topics/consumers-and-communities/project-reach/project-reach.html, whose
#: banner image is ordinary, well-formed, self-closing markup:
#:
#:     <img alt="Project REACh: ..." src="/images/img-project-reach-...png"
#:          style=" width: 100%; height: auto;" title="Project REACh: ..." />
#:
#: html.parser builds a tree in which that <img> CONTAINS 3,037 characters --
#: the entire body of the page. lxml, given the same bytes, gives it none.
#:
#: The symptom was a subtopic that stored 194 characters where the page has
#: 3,416: removing images (which every page here needs, because the viewer
#: blocks off-site images and they render as empty framed boxes) decomposed the
#: whole article along with the banner.
#:
#: IT WOULD NOT HAVE STOPPED AT IMAGES. `_furniture_unit` walks siblings and
#: `_tidy_empty_blocks` tests containers for emptiness; both read the same wrong
#: tree, so the damage was merely most visible here. Switching the parser fixes
#: the tree rather than the one symptom -- patching `decompose` to `unwrap`
#: would have left the rest of the walk reading nonsense.
#:
#: lxml is already a pinned dependency (requirements.txt:62), so this costs
#: nothing to install. It is named explicitly rather than falling back to
#: html.parser on ImportError: a silent downgrade here does not raise, it
#: quietly stores a tenth of the page.
PARSER = "lxml"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0
#: Statuses that are an ANSWER rather than something worth retrying. Only 200 is
#: accepted; the rest are logged and the page is skipped.
_FINAL_STATUSES = (200, 401, 403, 404, 410)

#: The tree the mega-menu is drawn from. Its level-1 entries under each topic
#: <h2> are the subtopics -- see the module docstring for why this is read
#: instead of the topic index page.
SITEMAP_URL = "https://www.occ.gov/topics/topics-sitemap.html"

#: Where the subtopic name travels, kept OUT of `section_path` so the two can
#: mean different things: doc_path is built from this, and `section_path`
#: carries what occ.gov's own breadcrumb says. Same split, and the same reason,
#: as FDICSectionCrawler.HEADING_KEY and DFPISectionCrawler.TRAIL_KEY.
TRAIL_KEY = "occ_subtopic"

#: occ.gov's breadcrumb: "Home > Topics > Supervision & Examination > Bank
#: Secrecy Act (BSA)". Present on every subtopic page.
_CRUMB_SELECTORS = ('nav[aria-label*="readcrumb" i]', 'ol.usa-breadcrumb__list',
                    '.usa-breadcrumb', '.breadcrumb', '[class*="rumb"]')

#: Furniture removed by SELECTOR, because occ.gov gives each of these a class of
#: its own and a class is a surer handle than a heading name.
#:
#:   occgov-section__share   "Share This Page:" plus the social icons. On 273 of
#:                           283 pages measured 2026-10-12 -- effectively every
#:                           page on the site.
#:   usa-summary-box         the right-hand aside holding "Popular Links" (54
#:                           pages) and "Top Tasks" (49). Its headings are <h4>,
#:                           which is why a heading rule scanning h2/h3 never
#:                           saw them.
#:
#: Both are navigation chrome rather than anything the OCC published, and both
#: were reaching the stored text the content_hash is taken over -- so OCC
#: reshuffling its own quick links would have reported half the regulator as
#: changed.
FURNITURE_SELECTORS = (
    "section.occgov-section__share",
    "div.share-icons",
)

#: NOT div.usa-summary-box, AND THAT IS A CORRECTION.
#:
#: The right-hand aside was removed wholesale because it holds "Popular Links"
#: and "Top Tasks". MEASURED across all 23 pages on 2026-10-12, it holds twelve
#: groups in all:
#:
#:     Popular Links                            x9   furniture
#:     Subscribe to Community Affairs Emails    x2   furniture
#:     What's New                               x1   CONTENT
#:
#: and that one is Dispute Resolution's, holding the Bank Appeals Process
#: Brochure and the Ombudsman's Report -- two OCC publications that appear
#: NOWHERE ELSE on that page. Taking the box took them with it.
#:
#: So the box is cleared by the HEADING rules instead, group by group, and
#: `_tidy_empty_blocks` removes the shell once its groups have gone. Every one
#: of the twelve groups carries a heading, so nothing in there is invisible to
#: a rule that reads headings -- which is why this is safe to do by name rather
#: than by selector.

#: Furniture removed by HEADING, for the blocks that carry no class of their own
#: and whose links are ordinary <a> tags no selector separates from real ones.
#:
#: "Related News and Issuances" is the expensive one: it is a rolling
#: three-item feed on 19 of the 23 subtopic pages, so left in, every subtopic
#: would report a change whenever OCC published anything at all.
FURNITURE_HEADINGS = {
    "most requested",
    "news & bulletins", "news and bulletins",
    "related news and issuances",
    "upcoming events",
    "you may be interested in",
    "popular links",
    "top tasks",
    # CONTACT, in every spelling occ.gov uses for it. A phone number and an
    # email address are not a regulation, and they are what moves when a post
    # is reassigned.
    "contact us", "contact the customer assistance group",
    "contact the office of financial technology",
    "contact community affairs", "contact community affairs officers",
    "media contact", "media contacts", "how to reach us",
    "contact the occ", "questions or comments",
    # The Consumer Protection page's customer-service cards. None is a
    # publication; all are "phone us" / "we are not your regulator".
    "visit helpwithmybank.gov",
    "learn who supervises your bank",
    "for customers of credit unions & state banks",
    "assistance for customers of national banks and federal savings associations",
    # Mailing-list promotion, the same block DFPI strips. The two "Get <x>
    # Updates" headings are matched by FURNITURE_PATTERNS instead, so that one
    # mechanism owns them rather than two.
    "subscribe", "email updates", "stay connected", "follow us",
}

#: Furniture matched on the START of the heading rather than the whole of it.
#:
#: NAMING CONTACT BLOCKS INDIVIDUALLY DOES NOT SCALE, and the audit of
#: 2026-10-12 proved it: FURNITURE_HEADINGS listed five spellings of "contact"
#: and still missed two, because occ.gov names each block after the team it
#: reaches --
#:
#:     <h2 class="usa-card__heading">Connect with ERSP</h2>
#:         Email: ERSP   Phone: (202) 649-7938
#:     <h3 class="usa-card__heading">Contact Project REACh</h3>
#:         Connect With Us -> mailto:REACh@occ.treas.gov
#:
#: so the list would need a new entry for every division OCC ever names. A
#: prefix asks the question that actually matters -- does this block exist to
#: put someone in touch with a team -- and it catches the next one unaided.
#:
#: "contact" needs no trailing space: a heading that merely CONTAINS the word
#: ("Media Contact") is caught by FURNITURE_HEADINGS, and no real heading on
#: these 23 pages begins with it.
FURNITURE_PREFIXES = (
    "contact",
    "connect with",
    "subscribe",
    "sign up for",
    "follow us",
    "stay connected",
)

#: Furniture matched on the SHAPE of the heading, where neither an exact name
#: nor a prefix catches it.
#:
#: occ.gov names each mailing-list block after the thing it mails about, so the
#: exact-name approach missed one for the same reason it missed two contact
#: cards. Both instances on these 23 pages:
#:
#:     Get Financial Technology Updates     Financial Technology
#:     Get Strategic Partnership Updates    External Relations and Strategic
#:                                          Partnerships
#:
#: Each sits above an "Enter your email / Subscribe" form. The <form> itself is
#: removed with the other non-content tags; without this the heading survived it
#: and sat in the stored text with nothing beneath it.
FURNITURE_PATTERNS = (
    re.compile(r"^get .+ updates$"),
)

#: Button labels that are a CONTROL and nothing else, so the button may go with
#: its text. Everything else inside a <button> is content -- see
#: `_unwrap_buttons`.
CONTROL_LABELS = frozenset((
    "expand all", "collapse all", "show", "hide", "show all", "hide all",
    "open", "close", "menu", "search", "submit", "print", "back to top",
))

#: The span an occ.gov accordion keeps its section NAME in. Its sibling span
#: holds the chevron and the word "Show"/"Hide", which is the control.
ACCORDION_LABEL = "span.occgov-accordion__button--text"

#: FURNITURE_HEADINGS put through the same normalisation the page text is.
#:
#: THIS IS NOT TIDINESS, IT IS A BUG THAT ALREADY HAPPENED ONCE. `_heading_key`
#: compares on `_norm`, which strips punctuation, so an entry written with an
#: ampersand or a dot could never match what it was written to catch:
#:
#:     "visit helpwithmybank.gov"  -> "visit helpwithmybank gov"
#:     "news & bulletins"          -> "news bulletins"
#:
#: Measured on the first export: the entries WITHOUT punctuation matched and
#: were removed, so the removal looked like it worked -- while Consumer
#: Protection kept all four of its customer-service cards.
_FURNITURE_KEYS = frozenset(_norm(h) for h in FURNITURE_HEADINGS)


def _text(el) -> str:
    """Display text for a title or a heading -- pipe-free.

    THE PIPE IS STRUCTURE, NOT PUNCTUATION. doc_path is a list in memory, but
    `excel_repo` serialises it to the workbook as a " | " join and reads it back
    with .split(" | "), so a title containing that sequence does not survive the
    round trip -- it becomes an extra folder. DFPI produced exactly one such row
    before this was fixed there; the guard travels with the rule.
    """
    if not el:
        return ""
    return _WS.sub(" ", el.get_text(" ", strip=True)).replace("|", "/").strip()


class OCCTopicCrawler(GenericSiteCrawler):
    """One topic area: a row per subtopic page the mega-menu names."""

    #: Checked on the sitemap and all 23 subtopic pages: `main` is present on
    #: every one and holds the content region.
    CONTENT_SELECTOR = "main"
    #: Each topic's block in the sitemap. The same markup renders the A-Z
    #: listing further down the page, which is why the topic <h2> is matched by
    #: name rather than the sections being taken in order.
    SITEMAP_SECTION = "section.occgov-index-items-section"

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        #: The topic's heading in the sitemap, EXACTLY as occ.gov writes it --
        #: "Supervision & Examination", ampersand and all. A near-miss yields a
        #: source with no rows rather than an error, so `_subtopics` raises when
        #: it matches nothing.
        topic: str,
        category: Optional[str] = None,
        sitemap_url: str = SITEMAP_URL,
        request_timeout: int = 45,
        #: Politeness. 24 requests is a small walk; this keeps it unhurried.
        delay: float = 0.3,
        **generic_kwargs,
    ):
        super().__init__(seed_url=seed_url, regulator=regulator,
                         source_system=source_system, category=category,
                         **generic_kwargs)
        self.topic = topic
        self.sitemap_url = sitemap_url
        self.request_timeout = request_timeout
        self.delay = delay
        self._session: Optional[requests.Session] = None
        self._warnings: List[str] = []

    # ------------------------------------------------------------------ #
    #  http                                                               #
    # ------------------------------------------------------------------ #
    def _sess(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers.update({"User-Agent": USER_AGENT,
                              "Accept": "text/html,application/xhtml+xml"})
            self._session = s
        return self._session

    def _fetch(self, url: str) -> Tuple[Optional[str], str]:
        """GET with retries, returning (body, FINAL url after redirects).

        The final url is returned because occ.gov redirects some topic paths,
        and a row stored under the url the sitemap printed would disagree with
        the one a later run reads. Returns None on a final non-200 rather than
        raising: one unreadable subtopic is a review item, not a reason to fail
        the topic -- except for the sitemap, whose caller checks.
        """
        last = None
        for attempt in range(RETRY_ATTEMPTS):
            if attempt or self.delay:
                time.sleep(self.delay if not attempt else RETRY_BACKOFF * attempt)
            try:
                r = self._sess().get(url, timeout=self.request_timeout)
                if r.status_code in _FINAL_STATUSES:
                    if r.status_code != 200:
                        logger.info("  %s -> HTTP %d", url, r.status_code)
                        self._warnings.append("HTTP %d: %s" % (r.status_code, url))
                        return None, (r.url or url)
                    if not r.encoding or r.encoding.lower() == "iso-8859-1":
                        r.encoding = r.apparent_encoding or "utf-8"
                    return r.text, (r.url or url)
                last = RuntimeError("%s returned %d" % (url, r.status_code))
            except requests.RequestException as e:
                last = e
        logger.warning("  could not fetch %s: %s", url, last)
        self._warnings.append("could not fetch %s: %s" % (url, str(last)[:120]))
        return None, url

    # ------------------------------------------------------------------ #
    #  the subtopics                                                      #
    # ------------------------------------------------------------------ #
    def _content(self, html: str):
        soup = BeautifulSoup(html, PARSER)
        return soup.select_one(self.CONTENT_SELECTOR) or soup, soup

    def _subtopics(self, html: str, base: str) -> List[Tuple[str, str]]:
        """The topic's level-1 entries in the sitemap -- i.e. the mega-menu.

        DEPTH IS COUNTED IN <ul> ANCESTORS, AND RELATIVE TO THE SHALLOWEST LINK
        IN THE SECTION, never as an absolute number.

        The absolute depth is a property of the PARSER, not of the document.
        MEASURED 2026-10-12 on the same sitemap bytes, Laws & Regulations:

            html.parser   topic at ul-depth 0, subtopics at 1, children at 2
            lxml          topic at ul-depth 1, subtopics at 2, children at 3

        lxml recovers one unclosed <ul> that html.parser drops, so every depth
        shifts by one. `!= 1` was written against html.parser and silently
        returned the TOPIC'S OWN LINK once the parser changed -- three rows for
        the whole regulator, each the topic page, with no error anywhere.

        So the shallowest link in the section is the topic's own entry, and its
        children are the mega-menu: `base_depth + 1`. If the topic's own link is
        ever absent, the shallowest link IS a subtopic, and the check below says
        so instead of silently taking the generation underneath.
        """
        soup = BeautifulSoup(html, PARSER)
        out: List[Tuple[str, str]] = []
        seen = set()
        for sec in soup.select(self.SITEMAP_SECTION):
            h2 = sec.find("h2")
            if not h2 or _text(h2) != self.topic:
                continue
            links = []
            for a in sec.find_all("a"):
                name = _text(a)
                href = (a.get("href") or "").strip()
                if not name or not href or href.startswith("#"):
                    continue
                url = urljoin(base, href).split("#")[0]
                if urlparse(url).scheme not in ("http", "https"):
                    continue
                links.append((len(a.find_parents("ul")), name, url))
            if not links:
                continue
            base_depth = min(d for d, _n, _u in links)
            top = [n for d, n, _u in links if d == base_depth]
            # The topic's own link is the one entry at the shallowest depth and
            # it is named after the section. Where that holds, the subtopics are
            # one level in; where it does not, this section is already a flat
            # list of subtopics and there is no generation to skip.
            want = base_depth + 1 if (len(top) == 1 and top[0] == self.topic) \
                else base_depth
            for d, name, url in links:
                if d != want or url in seen:
                    continue
                seen.add(url)
                out.append((name, url))
        if not out:
            # A TOPIC NAME THAT MATCHES NOTHING IS FATAL. Config lookups across
            # this repo fall back silently on a near-miss, and the failure here
            # would be a source reporting zero rows with verdict PASS -- the
            # exact way FDIC's Financial Institution Letters disappeared.
            raise RuntimeError(
                "no subtopics found for topic %r in %s -- the heading must "
                "match occ.gov's wording exactly, ampersand included"
                % (self.topic, self.sitemap_url))
        return out

    # ------------------------------------------------------------------ #
    #  furniture                                                          #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _furniture_unit(h):
        """The nodes a furniture heading owns.

        NEITHER OF THE TWO OBVIOUS RULES WORKS ALONE HERE, and both were tried
        against the live pages on 2026-10-12:

          * "the enclosing <section>" -- occ.gov puts many headings inside one
            <section class="occgov-section__content">. Financial Technology has
            15 headings in 3 sections, so decomposing the ancestor of its
            "Related News and Issuances" heading took 13 real ones with it.

          * "the following siblings up to the next heading" -- a sibling can be
            a nested <section> CONTAINING the next real heading. On Consumer
            Protection the "Consumer Protection Topics" block is nested inside
            the section "Learn Who Supervises Your Bank" opens.

        So: a heading inside a USWDS card owns its card and nothing beside it,
        and a sibling walk stops at the first node that IS, or CONTAINS, a
        heading at or above its own rank. Rank matters on both tests -- a
        furniture <h2> owns the <h3> subsections beneath it, and a walk that
        stopped at any heading at all removed the title and left the body.
        """
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
        """Reduce every <button> to its label, and drop the ones that are controls.

        THE SECTION NAME IS INSIDE THE BUTTON. occ.gov builds its accordions the
        USWDS way, so the heading is a <button> and not text:

            <h3 class="usa-accordion__heading">
              <button class="occgov-accordion__button">
                <span class="occgov-accordion__button--text">Bank-Fintech
                  Arrangements </span>
                <span class="...--icon-container"> ... Show </span>
              </button>
            </h3>

        Removing buttons wholesale -- which `_page_row` briefly did, alongside
        <script> and <form> -- therefore emptied the heading and left a bare
        <h3>. MEASURED on Financial Technology, where all five section names
        vanished (Bank-Fintech Arrangements, Digital Assets and Tokenization,
        Artificial Intelligence and Machine Learning, Other Financial Technology
        Topics, Responsible Innovation Framework) while their tables survived,
        so the stored page ran one table into the next with nothing to say which
        was which. Strategic Partnerships and Financial Literacy carry the same
        pattern.

        SO THE TEXT IS KEPT AND THE CONTROL IS DROPPED. Where the accordion's
        own label span exists it is used, which also discards the "Show"/"Hide"
        the icon span carries; otherwise the button's own text is kept unless it
        is a bare control ("Expand All", "Collapse All").

        Runs BEFORE `_strip_furniture` so heading matching sees the real name
        rather than a name with a chevron's text welded onto it.
        """
        gone = 0
        for b in list(main.find_all("button")):
            if getattr(b, "decomposed", False):
                continue
            label = b.select_one(ACCORDION_LABEL)
            txt = _text(label) if label is not None else _text(b)
            if not txt or _norm(txt) in CONTROL_LABELS:
                b.decompose()
                gone += 1
                continue
            b.replace_with(txt)
        return gone

    @staticmethod
    def _heading_key(h) -> str:
        """Heading text, minus the toggle word occ.gov appends to accordions.

        Financial Technology renders "Bank-Fintech Arrangements Show"; the
        "Show" is the control, not the name.
        """
        return _norm(re.sub(r"\s+(Show|Hide)$", "", _text(h)))

    @classmethod
    def _strip_furniture(cls, main) -> int:
        """Remove the chrome, in place. Returns how many blocks went.

        SELECTORS FIRST, then headings. The share row and the Popular Links /
        Top Tasks aside carry classes of their own; everything else has to be
        found by the heading that opens it.

        H4 IS INCLUDED in the heading scan, which h2/h3 alone was not: "Popular
        Links" and "Top Tasks" are <h4> inside the aside, so a scan that stopped
        at h3 never saw them even after they were named here.
        """
        gone = 0
        for sel in FURNITURE_SELECTORS:
            for el in main.select(sel):
                if not getattr(el, "decomposed", False):
                    el.decompose()
                    gone += 1
        for h in list(main.find_all(["h2", "h3", "h4"])):
            if getattr(h, "decomposed", False):
                continue
            key = cls._heading_key(h)
            if (key not in _FURNITURE_KEYS
                    and not key.startswith(FURNITURE_PREFIXES)
                    and not any(p.match(key) for p in FURNITURE_PATTERNS)):
                continue
            for d in cls._furniture_unit(h):
                if not getattr(d, "decomposed", False):
                    d.decompose()
            gone += 1
        return gone

    # ------------------------------------------------------------------ #
    #  the row                                                            #
    # ------------------------------------------------------------------ #
    @classmethod
    def _crumb(cls, soup) -> str:
        """occ.gov's own trail, verbatim, as the SITE draws it.

        "Home > Topics > Supervision & Examination > Bank Secrecy Act (BSA)".
        The library files the row under the subtopic alone, but the site's trail
        is recorded anyway -- it is the only place the workbook says what
        occ.gov calls this page, and it is kept whole even where the tree does
        not use it.
        """
        for sel in _CRUMB_SELECTORS:
            el = soup.select_one(sel)
            if not el:
                continue
            parts, seen = [], set()
            for piece in el.stripped_strings:
                p = re.sub(r"\s+", " ", piece).strip(" >/|›")
                if not p or re.fullmatch(r"[>›/|\s]*", p):
                    continue
                if p.lower() in seen:
                    continue
                seen.add(p.lower())
                parts.append(p)
            if parts:
                return " > ".join(parts)
        return ""

    @staticmethod
    def _page_title(main, soup, fallback: str) -> str:
        """The page's own <h1>, which names it as occ.gov does.

        The sitemap says "Strategic Partnerships"; the page says "External
        Relations and Strategic Partnerships". Same choice DFPI makes for a law
        page, and for the same reason: the page's own words are what the
        document is called.
        """
        for scope in (main, soup):
            el = scope.select_one("h1") if scope is not None else None
            t = _text(el)
            if t:
                return t
        return fallback

    @staticmethod
    def _tidy_empty_blocks(main) -> None:
        """Drop containers the removals left holding nothing.

        Removing the share row or the summary aside leaves its grid wrappers
        behind, and those render as empty framed boxes in the stored body.
        Repeated, because emptying an inner div is what makes its parent empty.
        """
        for _ in range(6):
            gone = 0
            for el in list(main.find_all(["div", "p", "span", "section", "li",
                                          "ul", "hr"])):
                if getattr(el, "decomposed", False):
                    continue
                if el.name != "hr" and el.find(["img", "a", "table",
                                                "h1", "h2", "h3", "h4"]):
                    continue
                if (el.get_text(" ", strip=True) or "").strip():
                    continue
                el.decompose()
                gone += 1
            if not gone:
                break

    def _page_row(self, url: str, name: str, html: str) -> dict:
        """One subtopic page, stored with its text. This is the whole row set.

        THE LINKS ON THIS PAGE SURVIVE INSIDE `html` AND BECOME NO ROWS. That is
        the requirement, not an omission: Bank Secrecy Act lists eleven things
        and the manual library shows ONE document under Bank Secrecy Act, with
        those eleven reachable from inside it. `absolutize_html` rewrites them
        to absolute urls first, so they are live for a reader rather than
        site-relative paths that resolve nowhere -- the same fix FDIC needed
        once its bodies became the only place its links survived.

        THE HASH IS OVER VISIBLE TEXT, which is ONBOARDING Step 2's first
        preference, and it is the reason `_strip_furniture` runs before the text
        is taken rather than after. With the share row and the Popular Links
        aside left in, OCC reshuffling its own quick links would report every
        subtopic in the regulator as modified.
        """
        main, soup = self._content(html)
        crumb = self._crumb(soup)
        title = self._page_title(main, soup, name)
        # BEFORE the furniture pass and before any tag removal: an accordion's
        # section name lives inside a <button>, and both of those steps would
        # otherwise read, or remove, a heading that is empty.
        self._unwrap_buttons(main)
        self._strip_furniture(main)
        # THE BREADCRUMB IS NAVIGATION. It belongs in extra_meta.section_path,
        # not in the text the content_hash is taken over.
        for sel in _CRUMB_SELECTORS:
            for el in main.select(sel):
                el.decompose()
        # And the images go: the artifact viewer blocks off-site images, so a
        # banner renders as an empty framed box. They carry no text, so this
        # changes the hash not at all and the reading of the page a great deal.
        # NO "button" HERE. `_unwrap_buttons` above has already turned every
        # button into either its label or nothing; a button reaching this list
        # takes an accordion's section name with it.
        for el in main.find_all(["img", "figure", "picture", "iframe", "script",
                                 "style", "noscript", "form"]):
            el.decompose()
        self._tidy_empty_blocks(main)
        frag = absolutize_html(str(main), url)
        text = _WS.sub(" ", BeautifulSoup(frag, PARSER)
                       .get_text(" ", strip=True)).strip()
        return {
            "section_path": crumb,
            TRAIL_KEY: name,
            "title": title, "url": url,
            "depth": 1, "linked_from_title": name,
            "doc_url": url,
            "parent_page_url": self.seed_url, "status": "ok",
            "n_pdfs": 0,
            # EMPTY BY THE HOST PROFILE'S RULE. The wrapper takes
            # pdf_links.split(" | ")[0] as org_pdf_link and the orchestrator
            # promotes it into document_url, so a page citing several files
            # would be stored under whichever happened to be linked first.
            "pdf_links": ("" if not profile_for(url).get("page_pdf_link", True)
                          else ""),
            "text_len": len(text),
            "html_file": "", "text": text, "html": frag,
            "content_hash": content_key(text),
        }

    # ------------------------------------------------------------------ #
    #  the walk                                                           #
    # ------------------------------------------------------------------ #
    def _run_crawl(self) -> dict:
        # NOT super()._run_crawl(). The engine would follow these pages' links
        # and make rows of what it found; here the subtopic page is the last
        # level and its links stay inside its body.
        t0 = time.time()
        pages: List[dict] = []

        smap, sbase = self._fetch(self.sitemap_url)
        if smap is None:
            # THE SITEMAP IS FATAL. Without it there are no subtopics, and
            # reporting zero rows quietly is the failure mode this repo has
            # already been bitten by -- a source with row_count 0 and verdict
            # PASS reads as "this regulator publishes nothing".
            raise RuntimeError(
                "the OCC topics sitemap could not be read (%s) -- refusing to "
                "report an empty topic" % self.sitemap_url)
        subs = self._subtopics(smap, sbase)
        logger.info("OCC[%s]: %d subtopic(s)", self.topic, len(subs))

        unread = 0
        for name, url in subs:
            html, final = self._fetch(url)
            if html is None:
                # SAID, NOT SWALLOWED. A subtopic we could not open must not
                # read as a subtopic with nothing in it.
                unread += 1
                logger.warning("  %-44s UNREAD", name[:44])
                continue
            row = self._page_row(final, name, html)
            pages.append(row)
            logger.info("  %-44s %6d chars", name[:44], row["text_len"])

        if unread and not pages:
            raise RuntimeError(
                "every subtopic of %r was unreadable -- refusing to report an "
                "empty topic" % self.topic)
        logger.info("OCC[%s]: %d row(s), %d unread, %.0fs",
                    self.topic, len(pages), unread, time.time() - t0)
        return {
            "shape": "generic",
            "pages": pages,
            "documents": [],
            "run": {"seed": self.seed_url, "subtopics": len(subs),
                    "unread": unread, "blocked_pages": 0},
        }

    # ------------------------------------------------------------------ #
    #  rows                                                               #
    # ------------------------------------------------------------------ #
    def _page_is_document(self, r: dict) -> bool:
        """Every record this walk builds is a subtopic page, and all are wanted.

        The inherited guard asks whether a CRAWLED page is content or merely a
        folder in the site's tree, and answers on length: under `min_page_text`
        (200) it is treated as a container. That is right for a crawl that
        sweeps up whatever it reaches. This walk builds a record for nothing but
        the subtopics the mega-menu names, so there is no container to screen
        out and the length test could only subtract -- which is exactly how four
        DFPI laws lost their folders once their chrome was stripped.

        A SHORT SUBTOPIC PAGE IS NOT AN EMPTY ONE, and it matters more here than
        on DFPI: with the furniture stripped, a thin subtopic can fall under the
        floor, and losing it would remove one of the 23 things this regulator
        IS.
        """
        return True

    def _apply_trail(self, doc, rec: dict):
        """doc_path from the SUBTOPIC; extra_meta.section_path from occ.gov.

        The inherited builders derive both from one `section_path` field, which
        is right when the library's tree IS the site's trail. Here it is not:
        the tree is regulator > source system > subtopic > title, while occ.gov
        says "Home > Topics > Supervision & Examination > Bank Secrecy Act
        (BSA)". Both are worth having and neither should overwrite the other.

        `self._doc_path(trail, title)` rather than a hand-built list, so the
        subtopic still goes through `_clean_trail`, the category rule and
        doc_path_title exactly as any other crumb would.
        """
        if doc is None:
            return doc
        trail = (rec.get(TRAIL_KEY) or "").strip()
        if trail:
            doc.doc_path = self._doc_path(trail, doc.title)
        if isinstance(getattr(doc, "extra_meta", None), dict):
            # ALWAYS, not only where drop_sections is declared. The wrapper
            # makes it conditional to avoid changing other regulators'
            # workbooks; here the tree differs from the site's trail on EVERY
            # row, so the site's trail is always worth recording.
            doc.extra_meta["section_path"] = rec.get("section_path") or ""
            if trail:
                doc.extra_meta[TRAIL_KEY] = trail
        return doc

    def _doc_from_page_row(self, r: dict, shape: str):
        doc = super()._doc_from_page_row(r, shape)
        if doc is not None and isinstance(getattr(doc, "extra_meta", None), dict):
            # Why this row's hash is not the engine's, recorded on the row the
            # way justice_canada_crawler records its own basis, so a reader who
            # notices the pages re-baseline once can see why without reading
            # this file.
            doc.extra_meta["content_hash_basis"] = "visible-text-minus-furniture"
        return self._apply_trail(doc, r)

    def _doc_from_document_row(self, d: dict, shape: str):
        # This walk emits no document rows. The override stays so that a future
        # change which does emit one cannot quietly skip `_apply_trail` and file
        # it at the tree root.
        return self._apply_trail(super()._doc_from_document_row(d, shape), d)

    def fetch_documents(self, limit: Optional[int] = None):
        docs = super().fetch_documents(limit=limit)
        run = dict((self.last_result or {}).get("run") or {})
        run["warnings"] = list(run.get("warnings") or []) + self._warnings
        self.last_result = {
            "run": run,
            "by_source": {self.source_system: len(docs)},
            "source": self.seed_url,
        }
        logger.info("OCCTopicCrawler[%s] -> %d document(s)",
                    self.source_system, len(docs))
        return docs
