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
  * It does not file by year. That is Financial Institution Letters' shape, that
    section stays on the generic engine, and neither of these two has a single
    year crumb in 269 measured rows.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from crawler.generic_crawler_wrapper import GenericSiteCrawler
from dynamic_crawler.formfill.runner import _ext_type, _is_doc
from generic_crawler.crawler import content_key

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
#: _tidy_empties. Both spellings seen on fdic.gov: a relative /acrobat/ and an
#: absolute https://www.fdic.gov/acrobat.html.
_ACROBAT_HREF = re.compile(r"acrobat", re.I)

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
    def _add_missing_links(self, result: dict) -> None:
        """Make a row for every heading link the crawl did not already produce.

        This is why "Information Technology" is a folder with three things in it
        rather than an empty folder: all three of its links resolve outside
        scope: prefix, two of them to other hosts, so the crawl cannot reach any
        of them and no amount of configuration inside the crawl would.
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
                if off_host and self.heading_links != "all":
                    documents.append({
                        "title": title, "doc_url": url,
                        "type": _ext_type(url) if _is_doc(url) else "HTML",
                        "found_on": self.seed_url, "section_path": heading,
                    })
                    added += 1
                    continue
                if _is_doc(url):
                    documents.append({
                        "title": title, "doc_url": url, "type": _ext_type(url),
                        "found_on": self.seed_url, "section_path": heading,
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
                frag = self._clean_html(str(main))
                text = _visible_text(BeautifulSoup(frag, "html.parser"))
                pages.append({
                    # The SITE's trail, read from the page we just fetched --
                    # not the heading. Same rule as every other row.
                    "section_path": _breadcrumb_from(html),
                    HEADING_KEY: heading,
                    "title": title, "url": final,
                    "depth": 1, "linked_from_title": title,
                    "parent_page_url": self.seed_url, "status": "ok",
                    "n_pdfs": 0, "pdf_links": "", "text_len": len(text),
                    "html_file": "", "text": text, "html": frag,
                    "content_hash": content_key(text),
                })
                have.add(final.rstrip("/"))
                added += 1
        if added:
            logger.info("FDIC headings: %d link(s) added that the crawl could "
                        "not reach", added)

    # ------------------------------------------------------------------ #
    #  the seam                                                           #
    # ------------------------------------------------------------------ #
    def _run_crawl(self) -> dict:
        self._build_heading_map()
        result = super()._run_crawl()
        self._clean_pages(result)
        self._refile(result)
        self._add_missing_links(result)
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
