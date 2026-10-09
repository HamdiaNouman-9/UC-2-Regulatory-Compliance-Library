# -*- coding: utf-8 -*-
"""FDIC section crawler: the generic engine, re-filed under the page's own headings.

WHY THIS EXISTS RATHER THAN MORE PROFILE KEYS. fdic.gov's section landing pages
group their links under <h2> subject headings -- "Information Technology",
"Policy", "Trust" -- and the library wants those headings as folders with the
linked documents one level inside. Three things make that impossible to express
in generic_crawler/crawler.py, and all three were measured on the 2026-10-05/06
exports rather than reasoned about:

  1. THE HEADING HAS TO REACH DOCUMENTS FOUND ON OTHER PAGES. 161 of the 165
     Supervision and Examinations rows were found on CHILD pages, not on the
     seed, and a document carries the breadcrumb of the page it was found on.
     `group_headings` sees one page at a time and cannot know that
     /bank-examinations/trust-examination-manual was reached from the "Trust"
     heading two pages ago. Only something holding the whole crawl can.

  2. MOST HEADING LINKS LEAVE THE SCOPE. Of the 22 links under the six
     Supervision headings, 14 resolve outside /bank-examinations -- two to other
     hosts entirely (ithandbook.ffiec.gov, federalreserve.gov). All THREE
     "Information Technology" links do. A profile key would have created that
     folder empty; this class can fetch or declare the missing links, so the
     folder holds what the page says it holds.

  3. LINKS REDIRECT, AND THE HEADING WAS KEYED ON THE PRE-REDIRECT URL.
     /regulations/examinations/ratings/ lands at
     /bank-examinations/composite-ratings-definition-list. Matching on anchor
     TEXT instead of url sidesteps redirect resolution altogether -- see
     `_heading_for_page`.

HOW IT WORKS. `_run_crawl` calls the generic engine unchanged, then rewrites its
output before the inherited mapping ever sees it:

    super()._run_crawl()        the engine, untouched
      -> _clean_pages()         furniture out of the stored html
      -> _refile()              section_path := the heading that owns the row
      -> _add_missing_links()   heading links the crawl never reached
    (inherited) _doc_path, _doc_from_*, dedupe, identity -- all unchanged

That seam is the one FormfillCrawler already uses from the other side: it
replaces `_run_crawl` and inherits the mapping. Nothing below `_run_crawl` is
overridden here either, except to stamp `content_hash_basis`.

THE HEADING MAP COMES FROM A DIRECT FETCH, NOT FROM THE CRAWL. Three separate
exports stored the FDIC "About" region in place of the seed page -- rows 393
(/laws-and-regulations/fdic-law-regulations-related-acts), 460 (a Financial
Institution Letter) and 650 (/bank-examinations itself). The live URLs answer
200 with the correct article and no trace of that region, so it is a race inside
the browser run, not a selector fault. The headings are the one input this class
cannot afford to read wrong, so it fetches the seed itself with `requests` --
one request per source, the same way justice_canada_crawler.py reads every page
it trusts. A crawl whose seed capture failed still produces correct folders.

WHAT IT DELIBERATELY DOES NOT DO:

  * It does not touch generic_crawler/crawler.py. The `content_selector` pin on
    www.fdic.gov is still required and still lives there: it runs inside the
    browser, and without it the real content never reaches pages.json at all, so
    no amount of post-processing can recover it.
  * It does not invent a folder for a row it cannot place. An unmatched row goes
    flat under the source system -- see `unmatched`.
  * It does not file by year. Neither does Financial Institution Letters, which
    is the other class in this file -- years were withdrawn there on 2026-10-07
    because reducing the trail to the year cost the site's own breadcrumb.
    Neither of these two has a single year crumb in 269 measured rows.
"""
from __future__ import annotations

import collections
import difflib
import logging
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from crawler.fingerprint import stamp_content_hashes
from crawler.generic_crawler_wrapper import GenericSiteCrawler
from dynamic_crawler.formfill.runner import _ext_type, _is_doc
from generic_crawler.crawler import (absolutize_html, best_doc_title,
                                     content_key, disambiguate_titles,
                                     doc_type_of, is_document_link,
                                     normalize_url, profile_for)

logger = logging.getLogger(__name__)

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0
#: Statuses that are an ANSWER. Anything else is worth retrying; 200 is the only
#: one this class accepts, because a 404 heading page would silently produce a
#: crawl with no folders at all.
_FINAL_STATUSES = (200, 401, 403, 404, 410)

# --------------------------------------------------------------------------- #
#  furniture
# --------------------------------------------------------------------------- #
#: Removed from the STORED html. Each measured against stored document_html --
#: never a static fetch, which is the mistake div.field--name-field-cards cost
#: us: it was measured on the static page, where main#main-content holds no card
#: container, and in the rendered DOM it was most of the seed's real content.
FURNITURE_SELECTORS = (
    # The social row under a headline: Facebook / X / LinkedIn / email / Print.
    # Print is INSIDE this container and needs no selector of its own (checked
    # on all 24 pages that carry it, every one the identical string).
    "div.fdic-share, "
    # The "Contact(s)" fieldset that closes a news item: a mailto to a division,
    # not a statement of the rule. Its SIBLING div.news-related-topics is
    # deliberately left alone -- both are div.news-field-wrapper.usa-fieldset, so
    # the lazy selector takes both, and the topic tags are the only subject
    # classification these pages carry.
    "div.news-contacts, "
    # THE ICONS, and NOT a bare `img`. A bare img selector removes 20 images, of
    # which 10 are the About-region icons and 5 are GENUINE CONTENT on
    # /bank-examinations/privacy-rule-handbook -- "This table reflects the rule's
    # requirements", "A diagram displaying two concentric circles". Those five
    # are the document.
    #
    # div.media--type-image IS NOT THE DISCRIMINATOR, and the first version of
    # this tuple used it and silently took all five handbook diagrams with it.
    # It is Drupal's generic media wrapper and sits around BOTH kinds. What
    # separates them is the paragraph the wrapper sits in:
    #
    #   content  ... < div.media--type-image < div.paragraph--type--text
    #   promo    ... < div.media--type-image < div.paragraph--type--image
    #   promo    ... < div.media--type-image < div.image-opposite-content
    #   icons    ... < div.card-thumbnail    < div.paragraph--type--card
    #
    # So the three promo CONTAINERS are named and the shared wrapper is not.
    "div.card-thumbnail, "
    "div.paragraph--type--image, "
    "div.field--name-field-image-opposite-content"
)

#: "Back to Top" is an anchor to the page's own top, inside a bare <p>. It is
#: matched on the HREF rather than the text so a capitalisation change does not
#: silently stop removing it; the text form below is the belt to this braces,
#: because the same link appears with and without the anchor id on different
#: templates. Measured: 7 of them on composite-ratings-definition-list alone.
_BACKTOTOP_HREF = re.compile(r"^#(top)$", re.I)
_BACKTOTOP_TEXT = re.compile(r"^\s*back\s+to\s+(the\s+)?top\s*$", re.I)

#: The "(PDF Help)" link that follows a "Printable (PDF)" link. Only the ANCHOR
#: is removed, not its paragraph: the paragraph also holds the Printable (PDF)
#: link, which names a real document. The empty "()" left behind is tidied by
#: _tidy_empties.
#:
#: THREE SPELLINGS, and the third was found by auditing a finished export rather
#: than by reading the template:
#:     /acrobat/                              relative
#:     https://www.fdic.gov/acrobat.html      absolute
#:     /help/pdf-help                         the CURRENT one, and it matches
#:                                            neither of the other two
#: Measured 2026-10-06 on the stored html of Consumer Compliance Supervisory
#: Highlights, where a pattern of just "acrobat" left the link standing.
_ACROBAT_HREF = re.compile(r"acrobat|pdf-help", re.I)

_WS = re.compile(r"\s+")

#: Where this class parks the heading it worked out, on the engine's own record
#: dicts. A key of its own rather than `section_path`, because section_path is
#: the SITE's trail and the workbook stores it to answer "what does fdic.gov
#: call this place" -- a different question from "where does the library file
#: it", which is doc_path's job.
HEADING_KEY = "fdic_heading"

#: The trail fdic.gov draws above the page title. Measured: both landing pages
#: say "Home > Resources" and a letter says
#: "Home > News > Financial Institution Letters > 2026".
_CRUMB_SELECTORS = ('nav[aria-label*="readcrumb" i]', '.breadcrumb',
                    'ol[class*="crumb"]', '[class*="rumb"]')


def _breadcrumb_from(html: str) -> str:
    """The site's own trail, for a page this class fetched itself.

    `_add_missing_links` reaches pages the crawl never opened, so the engine
    recorded no breadcrumb for them. Reading it here keeps the rule the same for
    every row: section_path is what the SITE shows, whoever fetched the page.
    """
    if not html:
        return ""
    doc = BeautifulSoup(html, "html.parser")
    for sel in _CRUMB_SELECTORS:
        el = doc.select_one(sel)
        if not el:
            continue
        out, seen = [], set()
        for a in el.find_all(["a", "li", "span"]):
            t = _WS.sub(" ", a.get_text(" ", strip=True)).strip()
            if not t or re.fullmatch(r"[>›/|\s]*", t) or t.lower() in seen:
                continue
            seen.add(t.lower())
            out.append(t)
        if out:
            return " > ".join(out[:8])
    return ""


def _norm(s: str) -> str:
    """Whitespace- and case-normalised, for comparing anchor text to link text.

    Also folds the non-breaking space and the typographic apostrophe, both of
    which fdic.gov emits in headings and link text inconsistently between the
    rendered DOM and the served html -- the difference is invisible and would
    make an anchor fail to match its own heading.
    """
    s = (s or "").replace(" ", " ").replace("’", "'")
    return _WS.sub(" ", s).strip().lower()


def _visible_text(soup: BeautifulSoup) -> str:
    """Approximate the browser's innerText for a cleaned fragment.

    NOT byte-identical to what the engine stores, and that is stated rather than
    hidden: the engine reads innerText from a live DOM, this reads a parsed
    fragment. The consequence is that the FIRST run under this class re-baselines
    every page hash for these two sections -- one `modified` per page, once. It
    is self-consistent from then on, which is what change detection needs.
    """
    for bad in soup(["script", "style", "noscript"]):
        bad.decompose()
    return _WS.sub(" ", soup.get_text(" ", strip=True)).strip()


class FDICSectionCrawler(GenericSiteCrawler):
    """One fdic.gov section landing page, filed under its own <h2> headings."""

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        category: Optional[str] = None,
        #: What to do with a link the crawl never reached. "host" fetches the
        #: ones on fdic.gov and records the off-host ones as reference rows;
        #: "none" leaves them out entirely; "all" fetches off-host links too.
        #:
        #: DEFAULT "host", and that is a judgement worth naming: fetching
        #: fdic.gov pages the section's own landing page points at is the same
        #: host the crawl is already reading, while reaching into
        #: federalreserve.gov is a decision about what this library contains,
        #: not about how it crawls. An off-host link still becomes a row -- with
        #: its title, url and heading -- it just holds no captured html.
        heading_links: str = "host",
        #: A row whose heading cannot be determined.
        #:
        #:   "drop" -- it is not in the library at all. THE DEFAULT, because the
        #:       section is DEFINED as what its landing page lists: the links
        #:       under each heading, be they html, pdf or doc, and nothing else.
        #:       `scope: prefix` reaches far past that -- Applications
        #:       Procedures Manual and its 39 files are never mentioned by
        #:       /bank-examinations -- and those rows were 122 of 184.
        #:   "flat" -- directly under the source system, no folder.
        #:   "keep" -- left on the site's own breadcrumb.
        #:
        #: "drop" IS A CONTENT DECISION, not a tidy-up: it takes the library
        #: from 294 rows to the ~34 the two landing pages name. Changing it back
        #: is one word here and a re-export.
        unmatched: str = "drop",
        request_timeout: int = 30,
        delay: float = 0.0,
        **generic_kwargs,
    ):
        if heading_links not in ("none", "host", "all"):
            raise ValueError("heading_links must be none|host|all")
        if unmatched not in ("drop", "flat", "keep"):
            raise ValueError("unmatched must be drop|flat|keep")
        super().__init__(seed_url=seed_url, regulator=regulator,
                         source_system=source_system, category=category,
                         **generic_kwargs)
        self.heading_links = heading_links
        self.unmatched = unmatched
        self.request_timeout = request_timeout
        self.delay = delay
        self._session: Optional[requests.Session] = None
        #: normalised anchor text -> heading, and absolute url -> heading
        self._by_text: Dict[str, str] = {}
        self._by_url: Dict[str, str] = {}
        #: the url a heading link printed -> the url it actually lands on
        self._final: Dict[str, str] = {}
        #: heading -> [(title, absolute url)], in the order the page lists them
        self._headings: Dict[str, List[Tuple[str, str]]] = {}
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

        THE FINAL URL IS RETURNED BECAUSE IT PREVENTS DUPLICATE ROWS. The seed
        lists legacy paths -- /regulations/examinations/ratings/ -- that 301 to
        /bank-examinations/composite-ratings-definition-list, which the crawl
        already holds under the LATTER. Adding a "missing" link by the url the
        seed printed would store the same page twice under two urls, and the
        inherited dedupe keys on (document_url, doc_path) so it would not catch
        it. `requests` follows the redirect anyway; reading `r.url` makes that
        free rather than costing a HEAD per link.

        Returns None on a final non-200 rather than raising: a heading link we
        cannot open is a review item, not a reason to fail the section.
        ithandbook.ffiec.gov answers 403 to this client. The SEED is the
        exception and its caller checks, because no seed means no headings.
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
                        return None, (r.url or url)
                    if not r.encoding or r.encoding.lower() == "iso-8859-1":
                        r.encoding = r.apparent_encoding or "utf-8"
                    return r.text, (r.url or url)
                last = RuntimeError("%s returned %d" % (url, r.status_code))
            except requests.RequestException as e:
                last = e
        logger.warning("  could not fetch %s: %s", url, last)
        return None, url

    def _get(self, url: str) -> Optional[str]:
        """The body alone, for callers that do not care where it came from."""
        return self._fetch(url)[0]

    # ------------------------------------------------------------------ #
    #  the heading map                                                    #
    # ------------------------------------------------------------------ #
    def _build_heading_map(self) -> None:
        """Read the seed page's <h2>s and the links under each, by direct fetch.

        A link belongs to the last heading seen before it in document order --
        which is what "under the heading" means on this template, and is how a
        reader reads the page.
        """
        html = self._get(self.seed_url)
        if not html:
            raise RuntimeError(
                "FDICSectionCrawler: could not read the seed %s, so no heading "
                "map could be built. Refusing rather than filing every row flat "
                "and calling it a tree." % self.seed_url)
        soup = BeautifulSoup(html, "html.parser")
        main = soup.select_one("main#main-content") or soup
        current = None
        for el in main.find_all(["h2", "a"]):
            if el.name == "h2":
                t = _WS.sub(" ", el.get_text(" ", strip=True)).strip()
                current = t or None
                if current:
                    self._headings.setdefault(current, [])
                continue
            if not current or not el.get("href"):
                continue
            href = el["href"].split("#")[0].strip()
            if not href or href.startswith(("mailto:", "javascript:", "tel:")):
                continue
            text = _WS.sub(" ", el.get_text(" ", strip=True)).strip()
            if not text:
                continue
            url = urljoin(self.seed_url, href)
            # PDF Help is furniture even here: it appears under the last heading
            # on /laws-and-regulations and would otherwise become a document.
            if _ACROBAT_HREF.search(url) or _norm(text) == "pdf help":
                continue
            self._headings[current].append((text, url))
            self._by_text.setdefault(_norm(text), current)
            self._by_url.setdefault(url.rstrip("/"), current)

        self._headings = {h: v for h, v in self._headings.items() if v}
        if not self._headings:
            raise RuntimeError(
                "FDICSectionCrawler: %s parsed but produced no headings. The "
                "template has changed; refusing rather than silently flattening."
                % self.seed_url)
        self._resolve_heading_urls()
        logger.info("FDIC headings on %s: %s", self.seed_url,
                    ", ".join("%s(%d)" % (h, len(v))
                              for h, v in self._headings.items()))

    def _resolve_heading_urls(self) -> None:
        """Follow each heading link's redirects once, and index the FINAL url too.

        WHY THIS IS NOT OPTIONAL. The seed prints legacy paths --
        /regulations/examinations/trust-examination-manual/ -- that land at
        /bank-examinations/trust-examination-manual, and the crawl knows the page
        only by the latter. Without the final url two things break, and the first
        one is invisible:

          * A HUB NEVER BECOMES A PAGE RECORD. The engine crawls
            trust-examination-manual, takes its 19 PDFs, then drops the hub
            itself as a link-wrapper -- the old generic export had FOUR page rows
            out of 26 crawled pages. So those 19 documents name a `found_on` that
            appears nowhere in `pages`, and a heading map keyed on page records
            has nothing to give them. Measured: 19 of the 20 documents on that
            page were filed flat while the page's own row sat under "Trust".
          * `_add_missing_links` then could not tell that the legacy url it was
            about to fetch IS the page the crawl already holds, so it added a
            second copy.

        ONE REQUEST PER LINK, ONCE PER RUN, and a failure is not fatal: the link
        keeps its printed url, which is what every other part of this class
        already copes with.
        """
        for heading, links in self._headings.items():
            for _title, url in links:
                if url in self._final:
                    continue
                final = url
                try:
                    r = self._sess().head(url, timeout=self.request_timeout,
                                          allow_redirects=True)
                    # Some hosts refuse HEAD; a GET is the fallback, and the body
                    # is discarded because only the landing url is wanted here.
                    if r.status_code >= 400:
                        r = self._sess().get(url, timeout=self.request_timeout,
                                             allow_redirects=True, stream=True)
                        r.close()
                    final = r.url or url
                except requests.RequestException as e:
                    logger.debug("  could not resolve %s: %s", url, e)
                self._final[url] = final
                self._by_url.setdefault(final.rstrip("/"), heading)
        moved = sum(1 for u, f in self._final.items()
                    if u.rstrip("/") != f.rstrip("/"))
        logger.info("FDIC heading links resolved: %d, of which %d redirect",
                    len(self._final), moved)

    def _heading_for_url(self, url: str) -> Optional[str]:
        """The heading that owns a url, printed or final. Page record not needed."""
        if not url:
            return None
        u = url.rstrip("/")
        return self._by_url.get(u) or self._by_url.get(
            self._final.get(url, url).rstrip("/"))

    # ------------------------------------------------------------------ #
    #  1. clean the stored html                                           #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _tidy_empties(soup: BeautifulSoup) -> None:
        """Remove the "()" left where a PDF Help anchor used to sit."""
        for p in soup.find_all(["p", "li"]):
            if p.find(["img", "a", "table", "ul", "ol"]):
                continue
            if re.fullmatch(r"[\s() .\-_]*", p.get_text(" ", strip=True) or ""):
                p.decompose()

    def _clean_html(self, html: str) -> str:
        if not html:
            return html
        soup = BeautifulSoup(html, "html.parser")
        for node in soup.select(FURNITURE_SELECTORS):
            node.decompose()
        # Back to Top: by href, then by text, then drop the <p> if that is all
        # it held. Doing it in that order means a template that changes one of
        # the two still loses the link.
        for a in list(soup.find_all("a", href=True)):
            if _BACKTOTOP_HREF.match(a["href"].strip()) or \
                    _BACKTOTOP_TEXT.match(a.get_text(" ", strip=True)):
                parent = a.find_parent(["p", "li", "div"])
                a.decompose()
                if parent is not None and not parent.find(["a", "img", "table"]) \
                        and not (parent.get_text(" ", strip=True) or "").strip():
                    parent.decompose()
        # PDF Help: the anchor only. Its paragraph also carries the real
        # "Printable (PDF)" link, which names a document.
        for a in list(soup.find_all("a", href=True)):
            if _ACROBAT_HREF.search(a["href"]):
                a.decompose()
        self._tidy_empties(soup)
        return str(soup)

    def _clean_pages(self, result: dict) -> None:
        """Clean every page's html, then re-derive the text and the hash from it.

        content_hash for a crawled page is content_key(text) -- set in
        generic_crawler/crawler.py at the single line that writes it. Cleaning
        the html without re-deriving the text would leave the hash standing over
        furniture the stored page no longer shows, so a share-widget change
        would still read as a document revision. Re-deriving here is the same
        move justice_canada_crawler.py makes when it knows a better basis than
        "hash whatever was stored".
        """
        n = chars = 0
        for r in result.get("pages", []) or []:
            html = r.get("html") or ""
            if not html:
                continue
            cleaned = self._clean_html(html)
            if cleaned == html:
                continue
            n += 1
            chars += len(html) - len(cleaned)
            r["html"] = cleaned
            text = _visible_text(BeautifulSoup(cleaned, "html.parser"))
            r["text"] = text
            r["text_len"] = len(text)
            r["content_hash"] = content_key(text)
        logger.info("FDIC furniture: %d page(s) cleaned, %d chars of markup out",
                    n, chars)

    # ------------------------------------------------------------------ #
    #  2. re-file under the heading that owns each row                    #
    # ------------------------------------------------------------------ #
    def _heading_for_page(self, r: dict, pages_by_url: Dict[str, dict],
                          _seen: Optional[set] = None) -> Optional[str]:
        """Walk up parent_page_url until a page the seed links to is reached.

        MATCHED ON ANCHOR TEXT FIRST, url second. The seed's links point at
        legacy paths that redirect -- /regulations/examinations/ratings/ becomes
        /bank-examinations/composite-ratings-definition-list -- so a url match
        fails on exactly the rows that matter, while the anchor text the engine
        recorded in `linked_from_title` is the text the heading sat above.
        """
        _seen = _seen or set()
        url = (r.get("url") or "").rstrip("/")
        if not url or url in _seen:
            return None
        _seen.add(url)
        if url == (self.seed_url or "").rstrip("/"):
            return None                      # the seed is not under a heading
        h = self._heading_for_url(url)
        if h:
            return h
        h = self._by_text.get(_norm(r.get("linked_from_title") or ""))
        if h:
            return h
        parent = (r.get("parent_page_url") or "").rstrip("/")
        if parent and parent in pages_by_url:
            return self._heading_for_page(pages_by_url[parent], pages_by_url, _seen)
        return None

    def _refile(self, result: dict) -> None:
        pages = result.get("pages", []) or []
        documents = result.get("documents", []) or []
        pages_by_url = {(p.get("url") or "").rstrip("/"): p for p in pages}

        page_heading: Dict[str, Optional[str]] = {}
        for p in pages:
            page_heading[(p.get("url") or "").rstrip("/")] = \
                self._heading_for_page(p, pages_by_url)

        # THE SITE'S OWN TRAIL IS NEVER OVERWRITTEN. The heading is recorded in
        # a key of this class's own, and `section_path` is left exactly as the
        # crawl found it -- "Home > Resources" on both landing pages.
        #
        # An earlier version of this method assigned the heading straight into
        # `section_path`, which destroyed that trail before the wrapper could
        # store it: `extra_meta["section_path"]` is the workbook's record of
        # what the SITE shows, and doc_path is the library's tree. They are two
        # different questions and the answer to one must not overwrite the
        # other. See _doc_path's note in generic_crawler_wrapper.py.
        placed = flat = dropped = 0
        keep_pages = []
        for p in pages:
            h = page_heading.get((p.get("url") or "").rstrip("/"))
            if h:
                p[HEADING_KEY] = h
                placed += 1
            elif self.unmatched == "drop":
                dropped += 1
                continue
            elif self.unmatched == "flat":
                p[HEADING_KEY] = ""
                flat += 1
            keep_pages.append(p)

        # A document is filed where the page that linked it is filed. This is
        # the half no profile key can reach: 161 of 165 Supervision rows were
        # found on a child page, and the child page is what the heading owns.
        keep_docs = []
        for d in documents:
            found_on = (d.get("found_on") or "").rstrip("/")
            h = page_heading.get(found_on)
            if not h:
                # THE PAGE RECORD MAY NOT EXIST. The engine crawls a hub, takes
                # its files and then drops the hub as a link-wrapper, so a
                # document's `found_on` routinely names a page that is in no
                # page record at all -- 19 of the 20 documents on
                # trust-examination-manual were filed flat this way while the
                # page's own row sat under "Trust". Ask the url directly.
                h = self._heading_for_url(found_on)
            if not h:
                # Found on the seed itself: match the document's own title or url.
                h = (self._heading_for_url(d.get("doc_url") or "")
                     or self._by_text.get(_norm(d.get("title") or "")))
            if h:
                d[HEADING_KEY] = h
                placed += 1
            elif self.unmatched == "drop":
                dropped += 1
                continue
            elif self.unmatched == "flat":
                d[HEADING_KEY] = ""
                flat += 1
            keep_docs.append(d)

        # REBUILT IN PLACE. `result` is the dict fetch_documents() goes on to
        # read, so the lists have to be replaced rather than rebound -- and the
        # engine's own crawl stats (pages walked, blocked, errors) are left
        # alone, because they describe the CRAWL and are still true. What
        # changes is only which of its findings become library rows.
        pages[:] = keep_pages
        documents[:] = keep_docs
        logger.info("FDIC headings: %d row(s) under a heading, %d %s",
                    placed, (dropped if self.unmatched == "drop" else flat),
                    {"drop": "dropped as not listed on the landing page",
                     "flat": "left flat",
                     "keep": "left on the site trail"}[self.unmatched])
        if not placed:
            self._warnings.append(
                "no row matched any heading on %s -- the folder tree will be "
                "flat" % self.seed_url)

    # ------------------------------------------------------------------ #
    #  3. the heading links the crawl never reached                       #
    # ------------------------------------------------------------------ #
    def _harvest_documents(self, page_url: str, frag: str, heading: str,
                           section_path: str, have: set) -> List[dict]:
        """Every file a FETCHED heading page links, as document rows.

        WHY THIS EXISTS. `_add_missing_links` used to append the page row with
        `n_pdfs: 0` and nothing else, so a heading link the crawl could not
        reach contributed exactly one row and every file on it was lost. That is
        not a judgement about depth -- a page the crawl DOES reach has its files
        collected by the engine, which is where Policy's 58 PDFs come from -- it
        was simply a hole. MEASURED on the 2026-10-06 20:14 export: 8 injected
        pages, 190 file links, 0 rows, including all 41 files of the Risk
        Management Manual of Examination Policies.

        The engine's own helpers do the work -- `is_document_link`,
        `normalize_url`, `doc_type_of`, `best_doc_title` -- so an injected page
        yields the same rows the engine would have produced had it reached the
        page, rather than a second, slightly different idea of what a document
        is. Off-host files are kept for the same reason: the engine keeps them
        (31 govinfo.gov and 17 gpo.gov rows in that export came off crawled
        pages), and an unreachable page must not end up richer or poorer than a
        reachable one.

        Harvested from the CLEANED fragment, not the raw page, so the furniture
        already removed -- the share row, the "PDF Help" link, the contact
        block -- cannot come back in as documents.
        """
        host = urlparse(self.seed_url).netloc.lower()
        out: List[dict] = []
        for a in BeautifulSoup(frag, "html.parser").find_all("a", href=True):
            href = urljoin(page_url, (a.get("href") or "").strip())
            if urlparse(href).scheme not in ("http", "https"):
                continue
            if not is_document_link(href, host):
                continue
            dn = normalize_url(href)
            if dn.rstrip("/") in have:
                continue
            row = a.find_parent(["li", "tr", "p"])
            link = {"text": a.get_text(" ", strip=True),
                    "title_attr": a.get("title") or "",
                    "ctx": row.get_text(" ", strip=True) if row else ""}
            out.append({
                "title": best_doc_title(link, dn),
                "doc_url": dn,
                "type": doc_type_of(href, host),
                "found_on": page_url,
                # The SITE's trail of the page the file was found on -- the same
                # rule every other row follows. The heading travels in
                # HEADING_KEY, never in here.
                "section_path": section_path,
                HEADING_KEY: heading,
            })
            have.add(dn.rstrip("/"))
        return out

    def _rows_from_headings(self, result: dict) -> None:
        """One row per link the landing page lists under a subheading. No more.

        RENAMED FROM `_add_missing_links` 2026-10-08, because it is no longer a
        patch over a crawl -- it IS the section. It used to run after the engine
        and add only the heading links the crawl had failed to reach (all three
        under "Information Technology", which resolve outside scope: prefix and
        two of them to other hosts). Now nothing crawls, so every heading link
        arrives here and `have` starts empty.

        WHAT IT DELIBERATELY DOES NOT DO: open a link to see what IT links.
        "Risk Management Manual of Examination Policies" is one heading link and
        therefore ONE row, not 1 + the 41 PDFs on it. That is the requirement --
        subheadings are the subfolders and the level below them is the last one.
        It is also why `_harvest_documents` is not called from here any more.
        """
        if self.heading_links == "none":
            return
        pages = result.setdefault("pages", [])
        documents = result.setdefault("documents", [])
        have = {(p.get("url") or "").rstrip("/") for p in pages}
        have |= {(d.get("doc_url") or "").rstrip("/") for d in documents}
        # A redirected link is already held under its FINAL url, which is not
        # the url the seed lists. `_refile` has just stamped every row with its
        # heading, so a heading whose links are all accounted for is detectable
        # by counting rows rather than by resolving redirects again.
        filed = {}
        for row in list(pages) + list(documents):
            sp = row.get("section_path") or ""
            if sp:
                filed.setdefault(sp, set()).add(
                    _norm(row.get("linked_from_title") or row.get("title") or ""))

        host = urlparse(self.seed_url).netloc.lower()
        added = 0
        for heading, links in self._headings.items():
            for title, url in links:
                # BOTH the printed url and the one it lands on. The crawl holds
                # a redirected page under its FINAL url only, so checking the
                # printed one alone adds a second copy of every legacy link.
                key = url.rstrip("/")
                final_key = self._final.get(url, url).rstrip("/")
                if key in have or final_key in have \
                        or _norm(title) in filed.get(heading, set()):
                    continue
                off_host = urlparse(url).netloc.lower() != host
                # HEADING_KEY, NOT section_path. These two branches were missed
                # when the heading moved out of `section_path` into a key of its
                # own, and the rows they build are the ones the crawl cannot
                # reach -- so the bug landed on exactly the links that most
                # needed the folder. MEASURED on the 17:09 export: the three
                # off-host heading links (FFIEC handbook, the Federal Reserve's
                # interagency guidelines, FDIC Enforcement Decisions & Orders)
                # arrived carrying a heading nothing read and fell through to
                # the flat case, leaving Information Technology holding 1 of 3.
                #
                # section_path stays EMPTY here rather than being invented: no
                # page was fetched, so there is no trail the site drew, and a
                # guess would be worse than a blank.
                if off_host and self.heading_links != "all":
                    documents.append({
                        "title": title, "doc_url": url,
                        "type": _ext_type(url) if _is_doc(url) else "HTML",
                        "found_on": self.seed_url, "section_path": "",
                        HEADING_KEY: heading,
                    })
                    added += 1
                    continue
                if _is_doc(url):
                    documents.append({
                        "title": title, "doc_url": url, "type": _ext_type(url),
                        "found_on": self.seed_url, "section_path": "",
                        HEADING_KEY: heading,
                    })
                    added += 1
                    continue
                html, final = self._fetch(url)
                # THE REDIRECT CHECK, and it has to happen AFTER the fetch
                # rather than before: the crawl stores this page under its final
                # url, the seed printed the legacy one, and only following the
                # redirect tells us they are the same page. Without this the
                # section gains a second copy of every redirected link.
                if final.rstrip("/") in have:
                    logger.debug("  %s redirects to a row already held (%s)",
                                 url, final)
                    continue
                if html is None:
                    # Still a row: the page says this belongs under the heading,
                    # and a link we could not open is a review item, not a
                    # reason to drop what the regulator published.
                    documents.append({
                        "title": title, "doc_url": final, "type": "HTML",
                        "found_on": self.seed_url, "section_path": "",
                        HEADING_KEY: heading,
                    })
                    self._warnings.append("unreadable heading link: %s" % url)
                    have.add(final.rstrip("/"))
                    added += 1
                    continue
                soup = BeautifulSoup(html, "html.parser")
                main = soup.select_one("main#main-content") or soup
                # ABSOLUTISED, as the engine's `_finish` does to every record it
                # writes (crawler.py:4788). These rows never went through it, so
                # their bodies stored "/sites/default/files/..." and every link
                # in them was dead for a reader -- MEASURED on the 2026-10-08
                # artifact, where the Laws-filed copy of the FIL listing offered
                # 0 usable links against the FIL copy's 30. It matters more now
                # that the files on a page are no longer rows: the body is the
                # only place those links survive.
                frag = absolutize_html(self._clean_html(str(main)), final)
                text = _visible_text(BeautifulSoup(frag, "html.parser"))
                crumb = _breadcrumb_from(html)
                # THE FILES THIS PAGE CARRIES. Without this the page arrives as
                # a lone row and everything it links is lost -- see
                # `_harvest_documents`. The page's own url is registered FIRST,
                # so a page that links to itself through a /documents/ path
                # cannot also become a document row.
                have.add(final.rstrip("/"))
                # THE FILES ON THIS PAGE ARE NOT COLLECTED, and that is the
                # requirement rather than an omission. A heading link is the last
                # level: "Risk Management Manual of Examination Policies" is ONE
                # row, not 1 + the 41 PDFs it lists. Harvesting them is what took
                # Supervision to 163 rows against the 22 its landing page names.
                # The page's body still holds every one of those links, so
                # nothing the FDIC published becomes unreachable -- it simply
                # stops being a row of its own.
                pages.append({
                    # The SITE's trail, read from the page we just fetched --
                    # not the heading. Same rule as every other row.
                    "section_path": crumb,
                    HEADING_KEY: heading,
                    "title": title, "url": final,
                    "depth": 1, "linked_from_title": title,
                    "parent_page_url": self.seed_url, "status": "ok",
                    # ZERO BECAUSE NOTHING IS COLLECTED, which is the honest
                    # value now. It was briefly the real count, when this method
                    # harvested; a stale non-zero here would claim rows that do
                    # not exist.
                    "n_pdfs": 0,
                    # EMPTY, AND IT WOULD HAVE TO BE EVEN IF FILES WERE HARVESTED.
                    # The wrapper takes pdf_links.split(" | ")[0] as org_pdf_link
                    # and the orchestrator PROMOTES that into document_url, so a
                    # page citing several files would be stored under whichever
                    # was linked first -- measured on the 2026-10-07 17:06
                    # export, where 8 injected pages carried a joined link list
                    # as their own url and "Risk Management Manual of Examination
                    # Policies" held all 41 of its PDFs there. `page_pdf_link:
                    # False` on www.fdic.gov exists to stop exactly that.
                    "pdf_links": "",
                    "text_len": len(text),
                    "html_file": "", "text": text, "html": frag,
                    "content_hash": content_key(text),
                })
                added += 1
        if added:
            logger.info("FDIC headings: %d link(s) added that the crawl could "
                        "not reach", added)

    # ------------------------------------------------------------------ #
    #  the seam                                                           #
    # ------------------------------------------------------------------ #
    def _run_crawl(self) -> dict:
        # THE LANDING PAGE'S OWN LINKS, AND NOTHING THEY LEAD TO.
        #
        # This used to run the generic engine and KEEP WHAT IT FOUND -- the
        # heading links, plus every page those opened and every file on those
        # pages -- then re-file the lot under the nearest heading. The result
        # was a section defined by how far a crawl happened to reach:
        # "Information Technology" listed 22 rows where the landing page names
        # 3, "Policy" listed 153 where it names 10, and Supervision came to 163
        # rows against the 22 its own page sets out.
        #
        # The requirement is the landing page: its <h2> subheadings are the
        # subfolders, and under each one sits exactly what that heading links --
        # page, doc or pdf -- with no traversal into those links. So the heading
        # map IS the section, and there is no crawl to re-file.
        #
        # `_clean_pages` and `_refile` are no longer in this path: both existed
        # to repair crawl output, and both are kept below because they are the
        # repair a crawl would still need if one is ever restored here.
        self._build_heading_map()
        result = {"shape": "generic", "pages": [], "documents": [],
                  "run": {"seed": self.seed_url, "blocked_pages": 0}}
        self._rows_from_headings(result)
        return result

    # ------------------------------------------------------------------ #
    #  two trails, kept apart                                             #
    # ------------------------------------------------------------------ #
    def _apply_heading(self, doc, rec: dict):
        """doc_path from the HEADING; extra_meta.section_path from the SITE.

        The inherited builders derive both from one `section_path` field, which
        is fine when the library's tree IS the site's trail. Here it is not:
        the tree is <regulator> > <source system> > <heading> > <title> while
        the site says "Home > Resources". So the trail is rebuilt from the
        heading and the site's own words are written back verbatim.

        `self._doc_path(heading, title)` rather than a hand-built list, so the
        heading still goes through drop_sections, `_clean_trail` and the
        doc_path_title rule exactly as any other crumb would.
        """
        if doc is None:
            return doc
        heading = (rec.get(HEADING_KEY) or "").strip()
        doc.doc_path = self._doc_path(heading, doc.title)
        if isinstance(getattr(doc, "extra_meta", None), dict):
            # ALWAYS, not only where drop_sections is declared. The wrapper
            # makes it conditional to avoid changing other regulators'
            # workbooks; here the library's tree deliberately differs from the
            # site's trail on EVERY row, so the site's trail is always worth
            # recording -- it is the only place the workbook still says what
            # fdic.gov calls this page.
            doc.extra_meta["section_path"] = rec.get("section_path") or ""
            if heading:
                doc.extra_meta["fdic_heading"] = heading
        return doc

    def _doc_from_page_row(self, r: dict, shape: str):
        doc = super()._doc_from_page_row(r, shape)
        if doc is not None and isinstance(getattr(doc, "extra_meta", None), dict):
            # Why this row's hash is not the engine's. Recorded on the row the
            # way justice_canada_crawler records "xml-minus-consolidation-
            # currency", so a reader who notices every page re-baselined once
            # can see why without reading this file.
            doc.extra_meta["content_hash_basis"] = "visible-text-minus-furniture"
        return self._apply_heading(doc, r)

    def _doc_from_document_row(self, d: dict, shape: str):
        return self._apply_heading(super()._doc_from_document_row(d, shape), d)

    def fetch_documents(self, limit: Optional[int] = None):
        docs = super().fetch_documents(limit=limit)
        # STAMP AT THE SINGLE EXIT. GenericSiteCrawler does not -- its rows
        # carry a hash the ENGINE computed, in pages.json. The rows this class
        # injects in `_add_missing_links` never went through the engine, so they
        # arrived with no hash at all: measured on the 17:09 export, the three
        # off-host heading links had an empty content_hash and would therefore
        # have classified `modified` on every run, for ever, writing a version
        # row each time. hash_for falls back to content_key("url|title"), which
        # is stable for a row that has no content to hash.
        #
        # An existing hash is never overwritten, so the 129 rows the engine
        # hashed are untouched -- which is exactly why fingerprint.py says to
        # call this at a crawler's single public exit rather than per branch.
        docs = stamp_content_hashes(docs)
        run = dict((self.last_result or {}).get("run") or {})
        run["warnings"] = list(run.get("warnings") or []) + self._warnings
        self.last_result = {
            "run": run,
            "by_source": {self.source_system: len(docs)},
            "source": self.seed_url,
            "headings": {h: len(v) for h, v in self._headings.items()},
        }
        logger.info("FDICSectionCrawler[%s] -> %d document(s) across %d heading(s)",
                    self.source_system, len(docs), len(self._headings))
        return docs


# ---------------------------------------------------------------------- #
#  Financial Institution Letters                                         #
# ---------------------------------------------------------------------- #
class FDICLettersCrawler(GenericSiteCrawler):
    """The FIL archive, enumerated over plain HTTP, plus two repairs.

    FIL needs no heading map and no re-filing -- its rows sit in one folder --
    so this is NOT an FDICSectionCrawler. It borrows that class's HTTP and
    html-cleaning members explicitly (below) because those are about fdic.gov,
    not about headings, and a second copy of them would be a second thing to
    fix when the site changes.

    IT DOES NOT RUN THE BROWSER ENGINE, AND THAT IS WHY THE SECTION EXISTS AT
    ALL. Every other source here calls `super()._run_crawl()`, which shells the
    generic crawler out to a subprocess with a 3600-second timeout
    (generic_crawler_wrapper.py:679, `timeout: int = 3600`). On 2026-10-07 the
    FIL crawl was killed by that timeout -- the subprocess started at 20:33:45,
    was still fetching at 21:33:39 and was terminated at 21:33:45 exactly one
    hour in, having written no pages.json. CompositeCrawler logs a failing
    source and skips it, so the export shipped 327 rows with no Financial
    Institution Letters at all and `verdict PASS` on every gate.

    THE TIMEOUT WAS THE SYMPTOM. The cause was measured afterwards, on one
    letter, both ways:

        through Playwright   37.8 s   (page.goto itself:  1.7 s)
        plain HTTP GET        0.84 s

    FDIC serves these pages fully rendered -- no JS is needed to see either the
    listing's 25 links or a letter's body. The browser was paying ~36 seconds a
    page for a site that answers in under one, and 953 letters at the browser's
    sustained rate is roughly two days against a one-hour budget. The same 953
    at 0.9 s is about fifteen minutes. Nothing was tuned to achieve that; the
    browser was simply removed.

    SO THE WALK IS: the listing's own ?pg= pages, read in order until one adds
    no letter we do not already hold, and then a GET per letter. The archive
    states its own length -- page 39 carries 3 letters where every earlier page
    carries 25 -- so the walk ENDS rather than being capped, which is what
    `max_pages` could never give us. The old `max_pages: 30` produced a rolling
    window: the 17:06 and 19:26 exports both stored 27 letter pages and shared
    only 21 of them.

    WHY NOT SPLIT THE SECTION BY YEAR, which was the other candidate: because
    year-shaped filing is exactly what was withdrawn on 2026-10-07. Reducing
    the trail to the year cost the site's own breadcrumb (`_sec` produces one
    value and it feeds both doc_path and extra_meta.section_path), and a crawl
    scoped per year would put a year back at the centre of completeness. It is
    also unnecessary: every letter from 1994 to 2026 sits at /<year>/<slug>,
    checked on listing pages 1, 20, 38 and 39, so a year split would have been
    correct and still pointless.

    1. PAGER ROWS. The listing paginates as ?pg=2..4. The engine followed those
       links -- correctly, because that is how the older letters were reached --
       but each pager ALSO became a page record carrying the listing's title,
       so all four collapsed onto one doc_path: measured on the 14:03 export,
       102 rows produced only 99 distinct paths and the folder tree showed ONE
       node that four rows pointed at. `_drop_query_variants` is now inert by
       construction, because `_walk_listing` reads the pagers for their links
       and never records them. It is KEPT, and its synthetic test in
       verify_letters.py with it, because "inert by construction" is a property
       of the walk and not of the site -- a listing that starts linking its
       pagers from inside the body would put them back.

    2. A PAGE THAT CAPTURED SOMEBODY ELSE'S CONTENT. In both the 10-05 and the
       10-07 exports, the letter "Rescission of the Board Statement on the
       Development and Communication of Supervisory Recommendations" stored
       fdic.gov/about -- "an independent agency created by Congress", links to
       /about/leadership/ and /about/careers/ -- and none of its own text.
       Checked rather than assumed: a plain GET of that url returns 200, no
       redirect, no meta refresh, no scripted redirect, and
       main#main-content > article.node--news with the right <h1>, under three
       different user agents. The server served the About node to the browser
       and the letter to a direct fetch, so a direct fetch is the repair.

       WHY THIS MATTERS MORE THAN THE WRONG TEXT: content_hash is computed from
       what was stored, so that row was tracking fdic.gov/about. It would never
       have signalled when the letter itself changed -- a silent monitoring
       failure, not a cosmetic one.

       THE TEST IS THE <h1>, AND THE OBVIOUS TEST DOES NOT WORK. Matching the
       title's words against the body text was tried first and MISSED it: the
       About page says "The Board of Directors of the FDIC manages operations",
       which contains "Board", so the page scored a hit and passed. Comparing
       the stored <h1> with the row's title separates them completely --
       measured across all 30 stored pages, every one of the 24 good letters
       scores 1.00 and the bad one has no <h1> at all.
    """

    #: Borrowed, not re-implemented -- see the class docstring. `_tidy_empties`
    #: is a staticmethod and has to be re-wrapped, or Python binds it as an
    #: instance method and it receives `self` where it expects the soup.
    _sess = FDICSectionCrawler._sess
    _fetch = FDICSectionCrawler._fetch
    _get = FDICSectionCrawler._get
    _tidy_empties = staticmethod(FDICSectionCrawler._tidy_empties)
    _clean_html = FDICSectionCrawler._clean_html
    #: BORROWED RATHER THAN COPIED, for the same reason as the rest of this
    #: block: it is about what a file link looks like on fdic.gov, not about
    #: headings. `_walk_listing` calls it with heading="" -- FIL has no heading
    #: map, and HEADING_KEY is only ever read by FDICSectionCrawler._apply_heading,
    #: which this class does not inherit. The alternative was a second copy of
    #: fifty lines that decide what counts as a document, which is precisely the
    #: thing that must not drift between two sources of one regulator.
    _harvest_documents = FDICSectionCrawler._harvest_documents

    #: How close the stored <h1> must be to the row's title. 0.6 is wide of the
    #: real split rather than tuned to it: the 24 good pages score 1.00 and the
    #: bad one scores 0.00, so anything in between would do and a loose bound
    #: cannot start failing on a letter whose <h1> gains a trailing word.
    H1_MATCH = 0.6

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        category: Optional[str] = None,
        #: False leaves a mismatched capture alone and only warns. The repair
        #: costs one GET per bad page and makes none when nothing is wrong.
        repair_captures: bool = True,
        request_timeout: int = 30,
        delay: float = 0.0,
        **generic_kwargs,
    ):
        super().__init__(seed_url=seed_url, regulator=regulator,
                         source_system=source_system, category=category,
                         **generic_kwargs)
        self.repair_captures = repair_captures
        self.request_timeout = request_timeout
        self.delay = delay
        self._session = None
        self._warnings: List[str] = []
        self._seed_path = urlparse(seed_url).path.rstrip("/")

    # ------------------------------------------------------------------ #
    #  helpers                                                            #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _bare(url: str) -> str:
        """The url with its query and fragment removed."""
        p = urlparse(url or "")
        return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))

    def _is_item(self, url: str) -> bool:
        """Is this a LETTER, or one of the listings that index them?

        By url shape, because the alternative -- "has a year folder" -- stopped
        existing when year filing was turned off. Under the seed path a letter
        is /<year>/<slug> and every listing is shallower: the seed itself, its
        ?pg= pagers, and the /<year> index. Measured on the 14:03 export: 25
        pages at two segments, all of them letters; 5 at one or none, all of
        them listings.
        """
        path = urlparse(url or "").path.rstrip("/")
        if not path.startswith(self._seed_path):
            return False
        return len([s for s in path[len(self._seed_path):].split("/") if s]) >= 2

    @staticmethod
    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

    def _h1_score(self, html: str, title: str) -> float:
        """How well the body's first <h1> matches the row's title; 0.0 if none."""
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html or "", re.S | re.I)
        if not m:
            return 0.0
        got = self._norm(re.sub(r"<[^>]+>", " ", m.group(1)))
        return difflib.SequenceMatcher(None, got, self._norm(title)).ratio()

    # ------------------------------------------------------------------ #
    #  0. the walk                                                        #
    # ------------------------------------------------------------------ #
    #: A listing walk that never stops adding is a bug, not a long archive.
    #: FDIC's is 39 pages; 200 lets it quadruple before THIS rather than the
    #: site ends the walk -- and hitting it is recorded as a warning, so a run
    #: that silently returned a prefix of the archive is not possible.
    MAX_LISTING_PAGES = 200

    def _listing_url(self, n: int) -> str:
        """Page n of the listing. PAGE 1 IS THE BARE SEED, not ?pg=1 -- the
        parameter is read off our own data, not guessed: the 14:03 export filed
        ?pg=2, ?pg=3 and ?pg=4 as rows, and the bare url is page one."""
        return self.seed_url if n <= 1 else "%s?pg=%d" % (self.seed_url, n)

    #: Each letter on the listing sits in its own <article>, and the FDIC's own
    #: reference for it -- FIL-62-2026 -- is a field inside that article rather
    #: than part of the link. Scoping to the article is what lets a number be
    #: attached to the RIGHT letter; a page-wide scan finds 25 numbers and 25
    #: letters with nothing tying them together.
    _ROW_SELECTOR = "article.node--news"
    _RELEASE_SELECTOR = ".field--name-field-release-number .field__item"

    def _letter_links(self, html: str, base: str) -> List[Tuple[str, str, str]]:
        """(url, title, release number) for every LETTER this listing page links.

        THE TITLE IS THE ANCHOR TEXT, DELIBERATELY, and not the letter page's
        own <h1>. Taking the title from the page would make `_h1_score` compare
        the h1 with itself and score 1.00 by construction -- which is exactly
        the check that caught fdic.gov serving the About node in place of the
        Rescission letter. The listing's claim about what a letter is, tested
        against what the letter page actually says, is the whole detector.

        The release number is "" where the markup does not offer one, and the
        caller must cope with that: it is only ever used to separate titles that
        collide, so a missing one costs nothing until two letters share a name.
        """
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select(self._ROW_SELECTOR)
        # THE MARKUP CHANGED, AND THAT IS NOT A REASON TO LOSE THE ARCHIVE.
        # Without the article wrapper there is no way to say which number
        # belongs to which letter, so none is claimed -- but every letter is
        # still found, because `_is_item` is a url shape and needs no markup.
        fell_back = not rows
        if fell_back:
            rows = [soup]
        best: Dict[str, Tuple[str, str]] = {}
        order: List[str] = []
        for row in rows:
            el = None if fell_back else row.select_one(self._RELEASE_SELECTOR)
            release = el.get_text(" ", strip=True) if el else ""
            for a in row.find_all("a", href=True):
                url = urljoin(base, (a.get("href") or "").strip())
                if urlparse(url).scheme not in ("http", "https"):
                    continue
                url = self._bare(url)
                if not self._is_item(url):
                    continue
                text = a.get_text(" ", strip=True)
                if not text:
                    continue
                if url not in best:
                    order.append(url)
                # The longest anchor text is the title. A letter can be linked
                # more than once from its own row, and taking whichever came
                # first would file some letters under a fragment of their name.
                if len(text) > len(best.get(url, ("", ""))[0]):
                    best[url] = (text, release)
        # ONLY WHEN IT COST US SOMETHING. The walk ends by asking for one page
        # past the last, and FDIC answers that with a real page carrying no
        # letters and no articles -- so warning on the fallback alone would put
        # a scary line in every clean run. It matters only if letters were found
        # without their numbers.
        if fell_back and order:
            logger.warning("FDIC letters: no %s on %s -- %d letter(s) found "
                           "without release numbers", self._ROW_SELECTOR, base,
                           len(order))
        return [(u, best[u][0], best[u][1]) for u in order]

    def _page_record(self, url: str, title: Optional[str], have: set,
                     documents: List[dict], depth: int = 1,
                     html: Optional[str] = None,
                     final: Optional[str] = None,
                     release: str = "") -> Optional[dict]:
        """One fetched page, in the shape the inherited mapping expects.

        This is the same record `FDICSectionCrawler._add_missing_links` builds
        for a heading link the crawl cannot reach, and it is built the same way
        on purpose -- breadcrumb from the raw page, furniture out of the stored
        html, files harvested from the CLEANED fragment, hash over the text that
        was actually kept. A letter reached by this walk must not be a different
        kind of row from a heading link reached by that one.
        """
        if html is None:
            html, final = self._fetch(url)
        if html is None:
            self._warnings.append("could not fetch %s" % url)
            return None
        final = final or url
        # REGISTERED BEFORE ITS FILES ARE HARVESTED, so a page that links to
        # itself cannot also become a document row -- same order, same reason as
        # _add_missing_links.
        key = final.rstrip("/")
        if key in have:
            return None
        have.add(key)

        soup = BeautifulSoup(html, "html.parser")
        main = soup.select_one("main#main-content") or soup
        # ABSOLUTISED, because the engine does it and this row has to be the
        # same kind of row. `_finish` runs absolutize_html over every record it
        # writes (crawler.py:4788); a page that skipped the engine would
        # otherwise store "/sites/default/files/..." where a crawled page stores
        # the full url, and every link in the stored body would be dead for a
        # reader. Done BEFORE the files are harvested so the urljoin below has
        # nothing left to resolve.
        frag = absolutize_html(self._clean_html(str(main)), final)
        text = _visible_text(BeautifulSoup(frag, "html.parser"))
        crumb = _breadcrumb_from(html)
        if not title:
            # Only the listing itself arrives without one; it is not an item, so
            # `_repair_captures` never scores it and nothing is weakened here.
            h1 = soup.find("h1")
            title = (h1.get_text(" ", strip=True) if h1 else "").strip() \
                or final.rstrip("/").rsplit("/", 1)[-1]
        # THE LETTER'S OWN ATTACHMENTS ARE NOT COLLECTED. Same rule as Laws and
        # Supervision: the listing's links are the last level, and what a letter
        # itself links is a step further than the section goes. Harvesting them
        # took this section to 1,989 rows -- 954 letters carrying 1,035 files --
        # where the archive is 953 letters. The files stay reachable in the
        # stored body, which is absolutised above precisely so they still work.
        return {
            "section_path": crumb,
            "title": title, "url": final,
            "depth": depth, "linked_from_title": title,
            "parent_page_url": self.seed_url if depth else "",
            "status": "ok",
            # FDIC's own reference for the letter, carried for
            # `_disambiguate_pages` and ignored by everything else.
            "release_no": release,
            "n_pdfs": 0,
            # EMPTY, AND IT WOULD HAVE TO BE EVEN IF FILES WERE HARVESTED. The
            # wrapper takes pdf_links.split(" | ")[0] as org_pdf_link and the
            # orchestrator promotes that into document_url, so a letter citing
            # several files would be stored under whichever was linked first --
            # measured on the 17:06 export, where 9 pages were filed under a
            # file url. `page_pdf_link: False` on www.fdic.gov stops that.
            "pdf_links": "",
            "text_len": len(text),
            "html_file": "", "text": text, "html": frag,
            "content_hash": content_key(text),
        }

    def _walk_listing(self) -> dict:
        """Read the listing's pages for their links, then fetch every letter.

        Returns the same dict shape `generic_crawler` writes to pages.json, so
        the inherited mapping cannot tell the difference -- `fetch_documents`
        reads only `shape`, `pages` and `documents` from it
        (generic_crawler_wrapper.py:979).
        """
        pages: List[dict] = []
        documents: List[dict] = []
        have: set = set()
        best: Dict[str, Tuple[str, str]] = {}
        order: List[str] = []
        seed_html = seed_final = None
        note = ""
        read = 0

        for n in range(1, self.MAX_LISTING_PAGES + 1):
            html, final = self._fetch(self._listing_url(n))
            if html is None:
                # PAGE ONE IS FATAL AND THE REST ARE THE END. Past the last
                # page the site answers 404, which `_fetch` reports the same way
                # as a refusal -- so the distinction that matters is whether we
                # already have an archive. Without page one we have nothing, and
                # returning zero rows quietly is the failure this walk exists to
                # stop.
                if n == 1:
                    raise RuntimeError(
                        "the Financial Institution Letters listing could not be "
                        "read (%s); refusing to report an empty section"
                        % self._listing_url(1))
                break
            read += 1
            if n == 1:
                seed_html, seed_final = html, final
            gained = 0
            for url, text, release in self._letter_links(html, final):
                if url not in best:
                    order.append(url)
                    gained += 1
                if len(text) > len(best.get(url, ("", ""))[0]):
                    best[url] = (text, release)
            # THE WALK ENDS WHEN THE LISTING STOPS ADDING, which is also what
            # happens if the site serves the last page for an out-of-range ?pg=.
            if not gained:
                break
        else:
            note = ("the listing was still producing letters at page %d "
                    "(MAX_LISTING_PAGES) -- the archive may be incomplete"
                    % self.MAX_LISTING_PAGES)
            self._warnings.append(note)

        logger.info("FDIC letters: %d listing page(s) read, %d letter(s) found",
                    read, len(order))

        rec = self._page_record(self.seed_url, None, have, documents, depth=0,
                                html=seed_html, final=seed_final)
        if rec:
            pages.append(rec)
        for i, url in enumerate(order, 1):
            title, release = best[url]
            rec = self._page_record(url, title, have, documents,
                                    release=release)
            if rec:
                pages.append(rec)
            if i % 100 == 0:
                logger.info("  %d/%d letters fetched", i, len(order))

        # THE ENGINE'S WRITE-OUT STEP, which this walk would otherwise skip.
        # Both of these live in `_finish` (crawler.py:4754 and 4768) and both
        # matter here:
        #
        #  * disambiguate_titles -- a title several DIFFERENT files share is not
        #    a title. MEASURED on the first 50 letters of this walk: two
        #    unrelated PDFs both linked as "Final Rule", which file at one
        #    doc_path and are handed ONE folder node, because get_folder_id
        #    matches on title+parent regardless of type.
        #  * the document hash -- a file is not downloaded here, so what
        #    identifies it is its url plus its title. AFTER the rename, never
        #    before: a hash taken over a title the row no longer has makes every
        #    renamed row read as modified on the next run.
        renamed = disambiguate_titles(documents, profile_for(self.seed_url))
        if renamed:
            logger.info("FDIC letters: %d shared document title(s) rewritten",
                        renamed)
        for d in documents:
            d["content_hash"] = content_key("%s|%s" % (d.get("doc_url") or "",
                                                       d.get("title") or ""))

        logger.info("FDIC letters: %d page row(s), %d document row(s)",
                    len(pages), len(documents))
        return {
            "shape": "generic",
            "pages": pages,
            "documents": documents,
            # NO `warnings` KEY HERE. `fetch_documents` appends self._warnings
            # to whatever this holds, and `_repair_captures` adds to that list
            # after this dict is built -- so filling it here would both duplicate
            # the walk's warnings and still miss the repair's.
            "run": {"seed": self.seed_url, "listing_pages": read,
                    "letters": len(order), "blocked_pages": 0},
        }

    # ------------------------------------------------------------------ #
    #  1. the pager rows                                                  #
    # ------------------------------------------------------------------ #
    def _drop_query_variants(self, result: dict) -> None:
        """Drop a page row that is only a query-string view of a row we keep.

        Stated as a RULE rather than as "?pg=", so the pager FDIC adds next
        year under a different parameter is caught by the same line and there
        is no list to keep current. A query view whose bare url is NOT already
        a row is kept -- it is the only record of that content.
        """
        pages = result.setdefault("pages", [])
        bare = {self._bare(p.get("url")) for p in pages
                if not urlparse(p.get("url") or "").query}
        keep, dropped = [], []
        for p in pages:
            url = p.get("url") or ""
            if urlparse(url).query and self._bare(url) in bare:
                dropped.append(url)
                continue
            keep.append(p)
        pages[:] = keep
        for url in dropped:
            logger.info("  dropped pager row %s", url)
        if dropped:
            logger.info("FDIC letters: %d pagination row(s) dropped; the pages "
                        "were still crawled", len(dropped))

    # ------------------------------------------------------------------ #
    #  2. the capture that belongs to another page                        #
    # ------------------------------------------------------------------ #
    def _repair_captures(self, result: dict) -> None:
        """Re-fetch any letter whose stored body is not that letter."""
        checked = repaired = 0
        for p in result.get("pages") or []:
            url = p.get("url") or ""
            if not self._is_item(url):
                continue
            checked += 1
            if self._h1_score(p.get("html") or "", p.get("title") or "") >= self.H1_MATCH:
                continue
            logger.warning("  stored body does not belong to %s -- re-fetching", url)
            html, _final = self._fetch(url)
            if html is None:
                self._warnings.append(
                    "could not re-fetch a page whose capture was wrong: %s" % url)
                continue
            soup = BeautifulSoup(html, "html.parser")
            main = soup.select_one("main#main-content") or soup
            frag = self._clean_html(str(main))
            # ONLY IF THE REPLACEMENT IS ACTUALLY RIGHT. A re-fetch that comes
            # back just as wrong would otherwise overwrite one bad capture with
            # another and hide the problem, so the row is left as it is and the
            # run carries a warning instead.
            if self._h1_score(frag, p.get("title") or "") < self.H1_MATCH:
                self._warnings.append(
                    "re-fetch did not return the expected page either: %s" % url)
                continue
            text = _visible_text(BeautifulSoup(frag, "html.parser"))
            p["html"] = frag
            p["text"] = text
            p["text_len"] = len(text)
            # THE TRAIL COMES FROM THE RE-FETCH TOO. This replaced the body and
            # left `section_path` as the bad capture had it, which is how a
            # repaired row can end up with no trail at all.
            #
            # MEASURED on the 2026-10-08 export: 952 of 954 letters carry the
            # site's trail, and the two that do not -- fil18046 (2018) and the
            # 2013 social-media guidance -- both return
            # "Home > News > Financial Institution Letters > <year>" when
            # fetched by hand. The markup is there; the capture that was wrong
            # about the body was wrong about the breadcrumb as well.
            #
            # ONLY WHEN THE RE-FETCH HAS ONE. An empty crumb here would overwrite
            # a good trail with nothing, which is the failure this is fixing.
            crumb = _breadcrumb_from(html)
            if crumb:
                p["section_path"] = crumb
            # The hash has to move with the content. It was computed from the
            # WRONG page, which is what made this a monitoring failure and not
            # just a display one.
            p["content_hash"] = content_key(text)
            repaired += 1
        logger.info("FDIC letters: %d letter page(s) checked, %d re-fetched",
                    checked, repaired)

    # ------------------------------------------------------------------ #
    #  3. two letters with one name                                       #
    # ------------------------------------------------------------------ #
    def _disambiguate_pages(self, result: dict) -> None:
        """Separate letters the FDIC gave the same name.

        MEASURED ON THE WHOLE ARCHIVE: 35 titles are shared by more than one
        letter, and 91 of the 953 letters -- 9.5% -- sit on one. "Bank Secrecy
        Act" is eight different letters between 2004 and 2024. doc_path ends at
        the title (`doc_path_title: true`) and get_folder_id matches on
        title+parent regardless of type, so all eight are handed ONE folder node
        and seven of them disappear from the tree.

        THIS IS THE RULE THE ENGINE ALREADY APPLIES TO FILES, said again for
        pages. `disambiguate_titles` -- "a title shared by several DIFFERENT
        documents is not a title" -- rewrites only the colliding ones and leaves
        every unique title alone. It runs over `documents` and never sees a page
        row, so this is the same contract rather than a second idea.

        THE REPLACEMENT IS FDIC'S OWN REFERENCE, not the url slug the engine
        falls back to. The slug would name eight letters fil6704, fil11017,
        fil16021 ... which is precisely the "two unreadable titles are worse
        than one shared one" case `disambiguate_titles` guards against with
        _OPAQUE_ID. FIL-67-2004 is what the FDIC itself calls the letter and it
        is already on the listing row we fetched.

        THE YEAR WOULD NOT HAVE FIXED THIS, recorded because a per-year split
        was the alternative: only 15 of the 35 collisions are separated by the
        year at all, and 20 collide WITHIN one year -- four letters titled
        "Assessments Notice of Proposed Rulemaking" were all published in 2010.

        LAST, AFTER `_repair_captures`, and that order is load-bearing. The
        repair scores the stored <h1> against the row's title; appending a
        reference pushes a short title toward the threshold -- "Assessments"
        against "Assessments (FIL-14-2014)" scores 0.65 where H1_MATCH is 0.60.
        Running this last means the detector always sees the listing's own
        words and the stored rows carry the separated ones.
        """
        pages = result.get("pages") or []
        counts = collections.Counter((p.get("title") or "").strip().lower()
                                     for p in pages)
        fixed, bare = 0, []
        for p in pages:
            t = (p.get("title") or "").strip()
            if not t or counts[t.lower()] < 2:
                continue
            rel = (p.get("release_no") or "").strip()
            if not rel:
                # NOT RENAMED TO SOMETHING WORSE. A collision we cannot separate
                # is reported and left readable, which is the same trade
                # disambiguate_titles makes for an opaque slug.
                bare.append(t)
                continue
            p["title"] = "%s (%s)" % (t, rel)
            p["linked_from_title"] = p["title"]
            fixed += 1
        if fixed:
            logger.info("FDIC letters: %d shared letter title(s) separated by "
                        "their FIL number", fixed)
        # SAID OUT LOUD, because a folder node holding two letters is exactly
        # what this method exists to prevent and a silent partial fix would read
        # as a complete one.
        left = {t: n for t, n in collections.Counter(
            (p.get("title") or "").strip().lower() for p in pages).items()
            if n > 1}
        if left:
            self._warnings.append(
                "%d letter title(s) are still shared after disambiguation and "
                "will share a folder node: %s"
                % (len(left), ", ".join(sorted(left)[:5])))
            logger.warning("FDIC letters: %d title(s) still shared%s",
                           len(left),
                           " (%d had no FIL number)" % len(bare) if bare else "")

    # ------------------------------------------------------------------ #
    #  the seam                                                           #
    # ------------------------------------------------------------------ #
    def _run_crawl(self) -> dict:
        # NOT super()._run_crawl(). That shells the browser engine out to a
        # subprocess with a one-hour timeout, which is what lost this section
        # entirely on 2026-10-07 -- see the class docstring. `_walk_listing`
        # returns the same dict the engine would have written.
        result = self._walk_listing()
        self._drop_query_variants(result)
        if self.repair_captures:
            self._repair_captures(result)
        # LAST: it rewrites titles, and _repair_captures tests titles.
        self._disambiguate_pages(result)
        return result

    def fetch_documents(self, limit: Optional[int] = None):
        docs = super().fetch_documents(limit=limit)
        run = dict((self.last_result or {}).get("run") or {})
        run["warnings"] = list(run.get("warnings") or []) + self._warnings
        self.last_result = {
            "run": run,
            "by_source": {self.source_system: len(docs)},
            "source": self.seed_url,
        }
        logger.info("FDICLettersCrawler[%s] -> %d document(s)",
                    self.source_system, len(docs))
        return docs
