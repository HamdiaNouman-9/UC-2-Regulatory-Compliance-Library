# -*- coding: utf-8 -*-
"""CFPB Interactive Bureau Regulations: one regulation per source, one row per
page of it.

WHAT THE SITE IS. www.consumerfinance.gov/rules-policy/regulations/<part>/ is
the landing page of one regulation (1026 = Regulation Z, 1030 = Regulation DD).
Its sidebar, nav.o-regs3k-navigation, is the regulation's own table of contents:
a div.o-expandable per group, each a list of links. SURVEYED 2026-10-22 from
cached pages:

    1026   Subpart A ... Subpart G (7 groups, 58 sections)
           Appendices (17), Supplement I - Official Interpretations (70)
    1030   Sections (11), Appendices (4), Supplement I (14)

Every link is a page of its own -- /<part>/<n>/ a section, /<part>/<x>/ an
appendix, /<part>/interp-<n>/ the official interpretation of one. 174 pages
for the two parts.

THE SHAPE (doc_path):

    CFPB > Interactive Bureau Regulations > <the regulation> > <TOC group>
         > <page title>

"Interactive Bureau Regulations" is the source_system and the regulation is
the `category` (doc_path_category: true) -- config/sources/cfpb.yml; the
user's tree, 2026-10-22, following the site's breadcrumb. section_path is
that breadcrumb, read off each page verbatim ("Interactive Bureau Regulations
> 12 CFR Part 1026 (Regulation Z)").

The TOC groups are the landing page's headings, so they are the folders (the
library's rule: landing-page headings are the only subfolders). The group's
section range ("§§ 1026.1–1026.4") is NOT part of the folder name: it moves
when a section is added, and a folder that renames itself moves every row
under it.

DISCOVERED, NOT LISTED. Nothing here names a section. The table of contents is
read on every run, so a section, appendix or comment CFPB adds is a new row and
one it removes is a disappeared row -- no config change either way. A new
regulation is one more source block in config/sources/cfpb.yml.

ACCESS. www.consumerfinance.gov is behind Akamai: a plain GET was refused 403
(AkamaiGHost) on 2026-10-22. Chrome is not: headed Chrome read every survey
page first try, and headless Chrome read the 1030 landing page (200, full
table of contents) the same day, so the default is headless -- unlike
hhs.gov and DFPI, which refuse it. `headless: false` is the switch if Akamai
starts refusing. The browser recipe is copied from crawler/hhs_crawler.py
(itself from DFPISectionCrawler), not imported --
copy-over-refactor-across-regulators. Its own profile, ~/.cache/cfpb_profile,
so it never shares a browser with the HHS crawl.

THE STORED BODY of each page is its <h1> plus section.block--flush-top, the
regulation text, with:

  * div.o-regulations-wayfinder removed -- an aria-hidden sticky header
    ("Regulation Z") the page's JS fills in; empty in the html.
  * div.inline-interpretation KEPT AND EXPANDED (user, 2026-10-22: "both
    title + content under its expansion should be there"). Each is the
    Official Interpretation of one paragraph, drawn as a collapsed box (30 on
    1026.2) whose content is already in the html, only hidden. Stored as the
    box's title -- "Official interpretation of 3(a) Business, Commercial ..."
    -- as an <h4>, followed by its content, unhidden; the toggle (button,
    +/- icons) is dropped. So a comment is stored twice, here and on its
    Supplement I page, and an edit to it moves both rows' hashes -- accepted:
    the section page is what a reader sees. `inline_interpretations: false`
    removes the boxes instead.
  * <img> KEPT INLINE (user, 2026-10-22: "we should have it inline").
    Appendix model forms are images -- 290 across seven appendices, 221 on
    1026 Appendix H, hosted on files.consumerfinance.gov and
    images.federalregister.gov. Each is stored as <img src=absolute alt=...>
    and nothing else (loading=lazy and the like dropped), where the site draws
    it. An image is content to the empty-container sweep, so a paragraph
    holding only a form is kept.
  * buttons, scripts and empty containers removed.

FINGERPRINT: visible text of that body (ONBOARDING Step 2, first preference),
followed by the src of every inline image in order. The images are the model
forms, and visible text does not include them, so a form CFPB replaces under a
new file name would otherwise not move the hash. content_text stays the
visible text alone. The effective date of the current version is recorded (published_date and
extra_meta) but NOT hashed: a new version that leaves a page's text alone
should not report that page modified.

EFFECTIVE DATE. /<part>/versions/ lists every version, each a link to
/<part>/<YYYY-MM-DD>/, the current one marked "(current regulation)". That date
is every row's published_date. A version dated after it (an amendment CFPB has
published but not yet in effect) is recorded as extra_meta cfpb_next_version.

Parsed with lxml (docs/onboarding_playbook.md section 4).
"""
from __future__ import annotations

import logging
import os
import re
import time
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawler.fingerprint import stamp_content_hashes
from crawler.generic_crawler_wrapper import GenericSiteCrawler
from generic_crawler.crawler import absolutize_html, content_key

logger = logging.getLogger(__name__)

PARSER = "lxml"
_WS = re.compile(r"\s+")

#: Copied from crawler/hhs_crawler.py.
_STEALTH = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins',
  {get: () => [1,2,3,4,5].map(i => ({name: 'Plugin ' + i}))});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = window.chrome || {runtime: {}};
"""
#: Interstitial titles: present means the check has not finished -- WAIT.
_CHALLENGE = ("just a moment", "please wait")
#: Akamai's refusal. Present after the wait means the page is unread.
_DENIED = "access denied"

#: The regulation's table of contents, and one group in it.
_TOC = "nav.o-regs3k-navigation"
_GROUP = "div.o-expandable"
_GROUP_LABEL = ".o-expandable__label"
#: "Subpart A - General §§1026.1–1026.4" -> "Subpart A - General".
_RANGE = re.compile(r"\s*§.*$")
#: The regulation text on a page.
_BODY = "section.block--flush-top"
#: The site's breadcrumb, one link per crumb.
_CRUMB = "nav.m-breadcrumbs a.m-breadcrumbs__crumb"
#: Removed from the stored body (see the module docstring).
_FURNITURE = ("div.o-regulations-wayfinder",)
_INLINE_INTERP = "div.inline-interpretation"
#: /<part>/<YYYY-MM-DD>/ on the versions page.
_VERSION_HREF = re.compile(r"/(\d{4}-\d{2}-\d{2})/?$")
#: A section page, /<part>/<n>/ -> reference_no "12 CFR <part>.<n>".
_SECTION_HREF = re.compile(r"/regulations/(\d+)/(\d+)/?$")
#: Tags that make a container worth keeping with no text in it. "img": a
#: paragraph holding only a model form has no text and must stay.
_KEEP_EMPTY = ["a", "table", "hr", "br", "img"]


def _text(el) -> str:
    """Display text, pipe-free -- " | " is doc_path's separator in the
    workbook, so a pipe in a title reads back as an extra folder (DFPI)."""
    if el is None:
        return ""
    return _WS.sub(" ", el.get_text(" ", strip=True)).replace("|", "/").strip()


def _visible_text(html: str) -> str:
    soup = BeautifulSoup(html, PARSER)
    for bad in soup(["script", "style", "noscript"]):
        bad.decompose()
    return _WS.sub(" ", soup.get_text(" ", strip=True)).strip()


class CFPBRegulationCrawler(GenericSiteCrawler):
    """One CFPB interactive regulation; seed_url is its landing page."""

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        category: Optional[str] = None,
        #: Keep the Official Interpretation boxes inside section pages, expanded
        #: (title + content). On by the user's call; see the module docstring.
        inline_interpretations: bool = True,
        #: How many pages the table of contents listed when this was built. A
        #: different count is logged as a warning, not refused -- CFPB adding a
        #: section is a change to record, not an error.
        expected_pages: Optional[int] = None,
        #: Headless reads consumerfinance.gov (measured 2026-10-22); False is
        #: the fallback if Akamai starts refusing it.
        headless: bool = True,
        channel: Optional[str] = "chrome",
        profile_dir: Optional[str] = None,
        challenge_wait: int = 30,
        #: Politeness, and not decoration: the Akamai block on saudiexchange.sa
        #: was created by our own repeated probing.
        delay: float = 2.0,
        #: Consecutive refusals before the walk stops. A scheduled retry against
        #: Akamai is what makes a block (ONBOARDING section 3).
        max_refusals: int = 5,
        **generic_kwargs,
    ):
        super().__init__(seed_url=seed_url, regulator=regulator,
                         source_system=source_system, category=category,
                         **generic_kwargs)
        self.inline_interpretations = inline_interpretations
        self.expected_pages = expected_pages
        self.headless = headless
        self.channel = channel
        self.profile_dir = profile_dir or os.path.join(
            os.path.expanduser("~"), ".cache", "cfpb_profile")
        self.challenge_wait = challenge_wait
        self.delay = delay
        self.max_refusals = max_refusals
        self._warnings: List[str] = []
        self._effective = ""
        self._next_version = ""

    # ------------------------------------------------------------------ #
    #  the browser the host will talk to                                  #
    # ------------------------------------------------------------------ #
    def _browser(self, p):
        """Persistent Chrome with the automation giveaways removed, headless
        unless configured otherwise. Copied from HHSExploreCrawler._browser."""
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
        """One page, waited out past any interstitial. (html, FINAL url);
        html is None when the page was refused or would not load."""
        if self.delay:
            time.sleep(self.delay)
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
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
                break
        if _DENIED in title or any(c in title for c in _CHALLENGE):
            self._warnings.append("Akamai refused %s (%r)" % (url, title[:60]))
            return None, url
        status = resp.status if resp is not None else 0
        if status >= 400:
            self._warnings.append("could not open %s: HTTP %d" % (url, status))
            return None, url
        try:
            return page.content(), page.url or url
        except Exception as e:
            self._warnings.append("could not open %s: %s" % (url, str(e)[:120]))
            return None, url

    # ------------------------------------------------------------------ #
    #  parsing -- pure functions of the html, testable on cached pages    #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _toc(html: str, base: str) -> List[Tuple[str, str, str]]:
        """[(group, title, absolute url)] in the order the sidebar lists them."""
        soup = BeautifulSoup(html, PARSER)
        nav = soup.select_one(_TOC)
        if nav is None:
            return []
        out, seen = [], set()
        for grp in nav.select(_GROUP):
            label = _RANGE.sub("", _text(grp.select_one(_GROUP_LABEL))).strip()
            for a in grp.find_all("a", href=True):
                # A link belongs to its innermost group only.
                if a.find_parent("div", class_="o-expandable") is not grp:
                    continue
                url = urljoin(base, a["href"].strip()).split("#")[0]
                if url in seen:
                    continue
                seen.add(url)
                out.append((label, _text(a), url))
        return out

    @staticmethod
    def _crumbs(html: str) -> List[str]:
        """The page's breadcrumb links, in order: ["Interactive Bureau
        Regulations", "12 CFR Part 1026 (Regulation Z)"]. The leading "/" is a
        separator drawn as text, not a crumb."""
        soup = BeautifulSoup(html, PARSER)
        return [t for t in (_text(a) for a in soup.select(_CRUMB)) if t]

    @staticmethod
    def _versions(html: str) -> Tuple[str, str]:
        """(current effective date, first later version date) as ISO strings.
        The later one is '' when CFPB has published no future amendment."""
        soup = BeautifulSoup(html, PARSER)
        main = soup.select_one("#content__main") or soup
        current, dates = "", []
        for li in main.find_all("li"):
            a = li.find("a", href=True)
            m = _VERSION_HREF.search(a["href"]) if a else None
            if not m:
                continue
            dates.append(m.group(1))
            if "current regulation" in li.get_text(" ", strip=True).lower():
                current = m.group(1)
        later = sorted(d for d in dates if current and d > current)
        return current, (later[0] if later else "")

    @staticmethod
    def _hash_basis(frag: str, text: str) -> str:
        """Visible text, then each inline image's src, in page order. A page
        with no images hashes exactly as visible text alone."""
        srcs = [i.get("src") or "" for i in
                BeautifulSoup(frag, PARSER).find_all("img")]
        return text if not srcs else text + "\n" + "\n".join(srcs)

    @staticmethod
    def _expand_interp(soup, box) -> None:
        """One collapsed Official Interpretation box -> its title as an <h4>
        and its content, visible. The box element stays (class
        inline-interpretation, nothing else) so the stored body still marks
        where the comment sits; the toggle button and hiding go."""
        label = _text(box.select_one(".o-expandable__label")) or \
            _text(box.find("button"))
        content = box.select_one(".o-expandable__content")
        kids = list(content.contents) if content is not None else []
        box.clear()
        box.attrs = {"class": ["inline-interpretation"]}
        if label:
            h = soup.new_tag("h4")
            h.string = label
            box.append(h)
        for k in kids:
            box.append(k.extract())

    def _body(self, html: str, url: str) -> Tuple[str, str, str]:
        """(h1 text, cleaned body html, visible text) of one regulation page."""
        soup = BeautifulSoup(html, PARSER)
        h1 = soup.find("h1")
        block = soup.select_one(_BODY)
        if block is None:
            return _text(h1), "", ""
        for sel in _FURNITURE:
            for el in block.select(sel):
                el.decompose()
        for el in block.select(_INLINE_INTERP):
            if not self.inline_interpretations:
                el.decompose()
                continue
            self._expand_interp(soup, el)
        for el in block.find_all(["script", "style", "noscript", "button",
                                  "form", "iframe", "cfpb-icon"]):
            if not getattr(el, "decomposed", False):
                el.decompose()
        # Model forms are images: kept inline, reduced to src (absolute) and
        # alt. An <img> with no src shows nothing and is dropped.
        for img in block.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src:
                img.decompose()
                continue
            alt = (img.get("alt") or "").strip()
            img.attrs = {"src": urljoin(url, src)}
            if alt:
                img["alt"] = alt
        # An EMPTY element that a link on this page points at is an anchor, not
        # clutter: the appendices index their model forms with "#G1"-style
        # links to empty <div id="G1"> markers (1026 App. G/H, 1030 App. B;
        # 134 links, 2026-10-26 audit). Dropping them broke every one. They
        # carry no text or image, so the fingerprint is unchanged.
        targets = {a["href"][1:] for a in block.find_all("a", href=True)
                   if a["href"].startswith("#")}
        for el in reversed(block.find_all(["div", "span", "p", "section"])):
            if el is block or el.find(_KEEP_EMPTY):
                continue
            if el.get("id") in targets:
                continue
            if not el.get_text(strip=True):
                el.decompose()
        head = "<h1>%s</h1>" % _text(h1) if h1 is not None else ""
        inner = "".join(str(c) for c in block.contents).strip()
        frag = absolutize_html(head + inner, url)
        return _text(h1), frag, _visible_text(frag)

    # ------------------------------------------------------------------ #
    def _run_crawl(self) -> dict:
        # NOT super()._run_crawl(): the engine would follow the pages'
        # cross-references into rows, and has no stealth init script.
        from playwright.sync_api import sync_playwright

        t0 = time.time()
        pages: List[dict] = []
        unread = refused_in_row = 0
        with sync_playwright() as p:
            ctx = self._browser(p)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                html, final = self._get(page, self.seed_url)
                if html is None:
                    raise RuntimeError(
                        "the CFPB landing page could not be read (%s) -- "
                        "refusing to report an empty source" % self.seed_url)
                toc = self._toc(html, final)
                if not toc:
                    raise RuntimeError(
                        "no table of contents (%s) on %s -- the page changed "
                        "shape; refusing to report an empty source"
                        % (_TOC, final))
                if self.expected_pages is not None and \
                        len(toc) != self.expected_pages:
                    self._warnings.append(
                        "table of contents lists %d page(s), %d expected -- "
                        "CFPB added or removed some; check the new/disappeared "
                        "rows" % (len(toc), self.expected_pages))
                logger.info("CFPB %s: %d page(s) in %d group(s)",
                            self.source_system, len(toc),
                            len({g for g, _t, _u in toc}))

                vurl = urljoin(final, "versions/")
                vhtml, _vf = self._get(page, vurl)
                if vhtml is not None:
                    self._effective, self._next_version = self._versions(vhtml)
                if not self._effective:
                    self._warnings.append(
                        "no current effective date found on %s" % vurl)

                for i, (group, toc_title, url) in enumerate(toc):
                    phtml, pfinal = self._get(page, url)
                    if phtml is None:
                        unread += 1
                        refused_in_row += 1
                        if refused_in_row >= self.max_refusals:
                            self._warnings.append(
                                "stopped after %d consecutive refusals; %d of "
                                "%d page(s) not attempted"
                                % (refused_in_row, len(toc) - i - 1, len(toc)))
                            unread += len(toc) - i - 1
                            break
                        continue
                    refused_in_row = 0
                    h1, frag, text = self._body(phtml, pfinal)
                    if not text:
                        self._warnings.append("no regulation text (%s) on %s"
                                              % (_BODY, pfinal))
                    # The TOC's wording is the title: it is what the library
                    # lists, and it exists before the page is opened.
                    title = toc_title or h1
                    m = _SECTION_HREF.search(urlparse(url).path)
                    # SECTION_PATH IS THE SITE'S OWN TRAIL, read off the page
                    # (user, 2026-10-22: "capture the correct section path"):
                    #   Interactive Bureau Regulations > 12 CFR Part 1026
                    #   (Regulation Z)
                    # verbatim, never rebuilt from source_system and the TOC
                    # group -- those are the library's tree (doc_path), and the
                    # group is kept beside it as cfpb_toc_group.
                    crumbs = self._crumbs(phtml)
                    if not crumbs:
                        self._warnings.append("no breadcrumb (%s) on %s"
                                              % (_CRUMB, pfinal))
                    pages.append({
                        "section_path": " > ".join(crumbs),
                        "cfpb_group": group,
                        "page_h1": h1,
                        "title": title, "url": url,
                        "depth": 1, "linked_from_title": title,
                        "doc_url": url,
                        "parent_page_url": self.seed_url, "status": "ok",
                        "n_pdfs": 0, "pdf_links": "",
                        "reference_no": ("12 CFR %s.%s" % m.groups()) if m else "",
                        "text_len": len(text),
                        "html_file": "", "text": text, "html": frag,
                        "content_hash": content_key(self._hash_basis(frag, text)),
                    })
                    logger.info("  %-60s %7d chars", title[:60], len(text))
            finally:
                ctx.close()

        if unread:
            # "coverage gap" is the orchestrator's word for a run that read
            # less than the site holds (check_run_trustworthy): such a run may
            # not propose anything for withdrawal.
            self._warnings.append(
                "coverage gap: %d of %d page(s) unread this run; their rows "
                "are missing, not withdrawn" % (unread, len(toc)))
        if unread and not pages:
            raise RuntimeError("every CFPB page was unreadable -- refusing to "
                               "report an empty source")
        logger.info("CFPB %s: %d row(s), %d unread, effective %s, %.0fs",
                    self.source_system, len(pages), unread,
                    self._effective or "?", time.time() - t0)
        return {
            "shape": "generic",
            "pages": pages,
            "documents": [],
            "run": {"seed": self.seed_url, "links": len(toc),
                    "unread": unread, "blocked_pages": unread},
        }

    # ------------------------------------------------------------------ #
    #  rows -> documents                                                  #
    # ------------------------------------------------------------------ #
    def _page_is_document(self, r: dict) -> bool:
        """Every TOC page is wanted, [Reserved] ones included -- the inherited
        length floor could only subtract."""
        return True

    def _doc_from_page_row(self, r: dict, shape: str):
        doc = super()._doc_from_page_row(r, shape)
        if doc is None:
            return doc
        # doc_path SET from the TOC group, never derived from section_path. A
        # ">" in a folder name would split it, so it is swapped for a look-alike.
        doc.doc_path = self._doc_path(
            (r.get("cfpb_group") or "").replace(">", "›"), doc.title)
        doc.published_date = self._effective or None
        doc.year = self._effective[:4] or None
        doc.reference_no = r.get("reference_no") or doc.reference_no
        if isinstance(getattr(doc, "extra_meta", None), dict):
            doc.extra_meta["content_hash_basis"] = \
                "visible-text-minus-furniture+image-srcs"
            doc.extra_meta["section_path"] = r.get("section_path") or ""
            if r.get("cfpb_group"):
                doc.extra_meta["cfpb_toc_group"] = r["cfpb_group"]
            if r.get("page_h1"):
                doc.extra_meta["page_h1"] = r["page_h1"]
            if self._effective:
                doc.extra_meta["cfpb_effective_date"] = self._effective
            if self._next_version:
                doc.extra_meta["cfpb_next_version"] = self._next_version
        return doc

    def fetch_documents(self, limit: Optional[int] = None):
        docs = stamp_content_hashes(super().fetch_documents(limit=limit))
        run = dict((self.last_result or {}).get("run") or {})
        run["warnings"] = list(run.get("warnings") or []) + self._warnings
        self.last_result = {
            "run": run,
            "by_source": {self.source_system: len(docs)},
            "source": self.seed_url,
            # Required for the gate to see a gap at all: CompositeCrawler
            # forwards "coverage gap" warnings only from a source that declares
            # `coverage_gaps` (generic_crawler_wrapper.py, the CMA fix).
            "coverage_gaps": [{"source": self.category or self.source_system,
                               "why": w}
                              for w in self._warnings
                              if "coverage gap" in w.lower()],
        }
        logger.info("CFPBRegulationCrawler[%s] -> %d document(s)",
                    self.source_system, len(docs))
        return docs


# ====================================================================== #
#  CFPB resource pages (TRID) -- a page of sections, linked pages, PDFs   #
# ====================================================================== #
#
# A different shape from a regulation. MEASURED 2026-10-22 on the TILA-RESPA
# Integrated Disclosures (TRID) page,
# /compliance/compliance-resources/mortgage-resources/tila-respa-integrated-
# disclosures/: one landing page whose <h2> headings ("Rules", "Quick
# references", "Guides", "FAQs", "Forms", "Webinars", "Additional materials")
# each hold PDFs or a link to a CFPB page of their own -- the forms page alone
# lists 39 PDFs in five tables. Every page ends "Page last modified <date>".
#
# THE SHAPE (user, 2026-10-22: headings as folders, items one level inside):
#
#   CFPB > <source_system> > <the landing page itself>
#                          > <section heading> > <PDF, or linked page>
#                                              > <linked page's PDFs>
#   ...Forms > <table heading on the forms page> > <form PDF>
#
# Discovered, not listed: sections are the landing page's headings on the
# day, so a section CFPB adds is a new folder. The config names only what to
# DROP (exclude_sections), which sections' links yield to the others
# (defer_sections: "Featured topic" re-links the forms page), which urls are
# never followed (no_follow), and which followed pages file their PDFs under
# their own <h3> tables (file_subfolders).
#
# PDFs ARE READ HERE, not by the pipeline. files.consumerfinance.gov answers
# the orchestrator's download headers with 403 (measured 2026-10-22, the same
# Akamai rule as the pages), so the pipeline could never extract a PDF's text
# and every file row would hash url|title. The crawl's own browser session
# downloads each PDF (context.request: 200, %PDF-), pypdfium2 extracts its text
# (the signed engine: PyMuPDF is blocked by Smart App Control on this machine),
# and the row carries it as content_text, which the pipeline's text gate takes
# before trying a download (textinput.decide). The row's hash is over that
# text; a PDF with no extractable text hashes its bytes (sha256).
#
# TRANSLATIONS: a Spanish form ("... (en Español)") is not a row of its own.
# Its url goes in urdu_url of its English original in the same table -- the
# repo's slot for "the translated/secondary document" (dynamic_crawler/auto/
# shapes.py) -- and is part of that row's hash, so a replaced translation is a
# change. One with no English partner stays a row of its own, with a warning.

_RES_MAIN = "div.u-layout-grid__main"
#: Furniture on every CFPB resource page: the contact / survey prefooter, the
#: email sign-up inset ("Rules related to mortgages ... Sign up"), screen-reader
#: labels ("Category:"), and the in-page breadcrumb if it is inside main.
_RES_FURNITURE = ("footer.o-prefooter", "div.m-inset--email", ".u-visually-hidden",
                  "nav.m-breadcrumbs")
_LAST_MOD = re.compile(r"page last modified", re.I)
#: Link text that names nothing: the title comes from the context instead.
_WEAK_LINK = re.compile(r"^(?:(?:view|download)(?: the)?(?: latest version| pdf)?"
                        r"|issued (?:a |the )?(?:proposed |final )?rule)$", re.I)
_DOWNLOAD_PREFIX = re.compile(r"^download (?:the )?", re.I)
_SPANISH = re.compile(r"\s*\(?\s*en espa(?:ñ|n)ol\s*\)?\s*", re.I)
_FILE_EXT = re.compile(r"\.(?:pdf|docx?|xlsx?)$", re.I)
#: Characters extracted PDF text may carry that are not text and that XML (so
#: an .xlsx) cannot hold. \t \n \r are whitespace and go via _WS instead.
_NOT_TEXT = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f￾￿]")
_HEADS = ["h2", "h3", "h4", "h5"]
#: Where a row's folder trail is carried to the document builder.
TRAIL_KEY = "cfpb_trail"


def _is_file(url: str) -> bool:
    return bool(_FILE_EXT.search(urlparse(url).path))


def _key(s: str) -> str:
    return _WS.sub(" ", s or "").strip().lower()


class CFPBResourcePageCrawler(CFPBRegulationCrawler):
    """A CFPB resource page (TRID) with its sections, linked pages and PDFs.
    seed_url is the landing page. See the block comment above."""

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        category: Optional[str] = None,
        #: Section headings dropped from the tree AND from the stored landing
        #: page, matched case-insensitively ("FAQs", "Additional materials").
        exclude_sections: Optional[List[str]] = None,
        #: Sections whose links are filed there only if no other section links
        #: the same url ("Featured topic" re-links the forms page).
        defer_sections: Optional[List[str]] = None,
        #: Url path prefixes never followed: kept as links in the stored body,
        #: no rows (another source covers them, or another CFPB section).
        no_follow: Optional[List[str]] = None,
        #: Followed pages (url substring) whose PDFs go one folder deeper,
        #: under the <h3> above their table ("forms-samples").
        file_subfolders: Optional[List[str]] = None,
        #: Hosts whose pages are followed, and hosts whose files are read.
        follow_hosts: Optional[List[str]] = None,
        file_hosts: Optional[List[str]] = None,
        #: A file larger than this is a row without text (warned), not a read.
        max_file_mb: float = 60.0,
        #: Seconds one file download may take. files.consumerfinance.gov sent
        #: 46-65 KB/s on 2026-10-26: the 2013 final rule (18 MB) took 402 s.
        #: The old fixed 120 s left it and the 2012 proposed rule (10 MB)
        #: unread, a coverage gap. 900 s covers 18 MB down to ~20 KB/s.
        file_timeout_s: float = 900.0,
        **kwargs,
    ):
        super().__init__(seed_url=seed_url, regulator=regulator,
                         source_system=source_system, category=category, **kwargs)
        self.exclude_sections = {_key(s) for s in (exclude_sections or [])}
        self.defer_sections = {_key(s) for s in (defer_sections or [])}
        self.no_follow = list(no_follow or [])
        self.file_subfolders = list(file_subfolders or [])
        self.follow_hosts = set(follow_hosts or ["www.consumerfinance.gov"])
        self.file_hosts = set(file_hosts or ["files.consumerfinance.gov",
                                             "www.consumerfinance.gov"])
        self.max_file_mb = max_file_mb
        self.file_timeout_s = file_timeout_s

    # ------------------------------------------------------------------ #
    #  cleaning -- pure functions of the html                             #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _cut_section(head) -> None:
        """Remove a heading and everything after it up to the next heading of
        the same or higher rank. A heading wrapped alone (header.m-slug-header)
        is cut with its wrapper."""
        rank = int(head.name[1])
        stop = ["h%d" % i for i in range(1, rank + 1)]
        start = head
        while start.parent is not None and start.parent.name not in ("body", "main") \
                and len([c for c in start.parent.children
                         if getattr(c, "name", None) or str(c).strip()]) == 1:
            start = start.parent
        doomed, sib = [start], start.next_sibling
        while sib is not None:
            # A text node has a .find too (str.find) -- only a Tag is searched.
            if getattr(sib, "name", None) in stop or (
                    getattr(sib, "name", None) and sib.find(stop) is not None):
                break
            doomed.append(sib)
            sib = sib.next_sibling
        for el in doomed:
            el.extract()

    def _clean_main(self, soup, cut: bool):
        """The page's main region with furniture out, and (landing page) the
        excluded sections cut. Returns the element, or None."""
        main = soup.select_one(_RES_MAIN) or soup.find("main")
        if main is None:
            return None
        for sel in _RES_FURNITURE:
            for el in main.select(sel):
                if not getattr(el, "decomposed", False):
                    el.decompose()
        if cut and self.exclude_sections:
            for h in list(main.find_all(_HEADS)):
                if h.parent is not None and _key(_text(h)) in self.exclude_sections:
                    self._cut_section(h)
        return main

    @staticmethod
    def _body_of(main, url: str) -> Tuple[str, str]:
        """(stored html, visible text) of a cleaned main region. Images kept
        inline with an absolute src; controls, scripts and empties out; the
        "Page last modified" line kept as the site shows it."""
        for el in main.find_all(["script", "style", "noscript", "button", "form",
                                 "iframe", "cfpb-icon", "svg", "input"]):
            if not getattr(el, "decomposed", False):
                el.decompose()
        for img in main.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src:
                img.decompose()
                continue
            alt = (img.get("alt") or "").strip()
            img.attrs = {"src": urljoin(url, src)}
            if alt:
                img["alt"] = alt
        for el in reversed(main.find_all(["div", "span", "p", "section", "header",
                                          "ul", "li"])):
            if el is main or el.find(_KEEP_EMPTY):
                continue
            if not el.get_text(strip=True):
                el.decompose()
        inner = "".join(str(c) for c in main.contents).strip()
        frag = absolutize_html(inner, url)
        return frag, _visible_text(frag)

    @staticmethod
    def _last_modified(main) -> str:
        """ISO date of the page's "Page last modified" line, or ''."""
        for s in main.find_all(string=_LAST_MOD):
            p = s.find_parent(["p", "div"])
            t = p.find("time") if p is not None else None
            if t is not None:
                return (t.get("datetime") or "")[:10]
        return ""

    @staticmethod
    def _sections(main, base: str):
        """Every link in a cleaned landing page, with its section and the
        nearest sub-heading: [(section, sub, <a>, absolute url)]. A section is
        an <h2>, or an <h3> that opens its block (the "Featured topic" well);
        <h3>/<h4> under an <h2> are sub-labels."""
        out, section, sub = [], None, None
        for el in main.find_all(_HEADS + ["a"]):
            if el.name == "a":
                href = (el.get("href") or "").strip()
                if not href or href.startswith(("#", "mailto:", "javascript:")):
                    continue
                out.append((section, sub, el, urljoin(base, href).split("#")[0]))
                continue
            label = _text(el)
            if el.name == "h2":
                section, sub = label, None
            elif el.name == "h3":
                blk = el.find_parent("div", class_="block")
                prev = el.find_previous("h2")
                opens = prev is None or blk is None or blk not in prev.parents
                if opens:
                    section, sub = label, None
                else:
                    sub = label
            else:
                sub = label
        return out

    @staticmethod
    def _file_title(a, sub: Optional[str], page_h1: str) -> str:
        """A file row's title from its link and context.
          * in a table row: the first cell -- bold title, then its note
            ("Annotated loan estimate — Rule citations shown")
          * link text that names nothing ("View pdf", "Download the latest
            version"): the sub-heading above it on the landing page, or
            "<page title> — <nearest heading>" on a linked page
          * otherwise the link text, minus a leading "Download (the)"."""
        td = a.find_parent("td")
        if td is not None:
            first = td.find_parent("tr").find("td")
            if first is not None and first is not td:
                b = first.find("b") or first.find("strong")
                head = _text(b) if b is not None else _text(first)
                rest = _text(first)[len(head):].strip() if b is not None else ""
                return head + (" — " + rest if rest else "")
        t = _text(a)
        if not t or _WEAK_LINK.match(t):
            if sub:
                return sub
            h = a.find_previous(_HEADS)
            return "%s — %s" % (page_h1, _text(h)) if h is not None else page_h1
        t = _DOWNLOAD_PREFIX.sub("", t).strip()
        return t[:1].upper() + t[1:]

    # ------------------------------------------------------------------ #
    #  files                                                              #
    # ------------------------------------------------------------------ #
    def _read_file(self, ctx, url: str) -> dict:
        """Download one file through the browser session and extract its
        text. {"text", "sha256", "bytes"} or {"error"}."""
        import hashlib
        if self.delay:
            time.sleep(self.delay)
        try:
            r = ctx.request.get(url, timeout=self.file_timeout_s * 1000)
        except Exception as e:
            return {"error": str(e)[:120]}
        if r.status != 200:
            return {"error": "HTTP %d" % r.status}
        body = r.body()
        if len(body) > self.max_file_mb * 1e6:
            return {"error": "larger than %s MB" % self.max_file_mb}
        out = {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body),
               "text": ""}
        if body[:5] == b"%PDF-":
            try:
                import pypdfium2 as pdfium
                pdf = pdfium.PdfDocument(body)
                parts = []
                for i in range(len(pdf)):
                    tp = pdf[i].get_textpage()
                    parts.append(tp.get_text_range())
                    tp.close()
                pdf.close()
                # pdfium marks a soft hyphen at a line break with U+FFFE, and a
                # PDF can carry other control characters; none is text, and an
                # .xlsx cannot hold them -- the 2026-10-22 TRID export died on
                # the Small Entity Compliance Guide's U+FFFE
                # ("All strings must be XML compatible").
                joined = _NOT_TEXT.sub("", " ".join(parts))
                out["text"] = _WS.sub(" ", joined).strip()
            except Exception as e:
                out["text_error"] = str(e)[:120]
        return out

    # ------------------------------------------------------------------ #
    def _run_crawl(self) -> dict:
        from playwright.sync_api import sync_playwright

        t0 = time.time()
        pages: List[dict] = []
        files: List[dict] = []
        unread = 0
        owner = {}          # url -> the section that files it
        with sync_playwright() as p:
            ctx = self._browser(p)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                html, final = self._get(page, self.seed_url)
                if html is None:
                    raise RuntimeError(
                        "the CFPB page could not be read (%s) -- refusing to "
                        "report an empty source" % self.seed_url)
                soup = BeautifulSoup(html, PARSER)
                crumbs = self._crumbs(html)
                main = self._clean_main(soup, cut=True)
                if main is None:
                    raise RuntimeError("no main region (%s) on %s"
                                       % (_RES_MAIN, final))
                landing_h1 = _text(main.find("h1")) or _text(soup.find("h1"))
                links = self._sections(main, final)
                if not links:
                    self._warnings.append("no links in any section of %s" % final)

                # Who files each url: the first non-deferred section linking
                # it; a deferred section gets only what nobody else links.
                for sec, sub, a, u in links:
                    if sec and _key(sec) not in self.defer_sections:
                        owner.setdefault(u, sec)
                for sec, sub, a, u in links:
                    if sec:
                        owner.setdefault(u, sec)

                seen_files, followed = set(), set()
                for sec, sub, a, u in links:
                    if not sec or owner.get(u) != sec:
                        continue
                    host = urlparse(u).netloc.lower()
                    if any(urlparse(u).path.startswith(n) for n in self.no_follow):
                        continue
                    if _is_file(u):
                        if host in self.file_hosts and u not in seen_files:
                            seen_files.add(u)
                            files.append(self._file(
                                u, self._file_title(a, sub, landing_h1), [sec],
                                final, crumbs, landing_h1))
                        continue
                    if host not in self.follow_hosts or u in followed or \
                            u.rstrip("/") == final.rstrip("/"):
                        continue
                    followed.add(u)
                    phtml, pfinal = self._get(page, u)
                    if phtml is None:
                        unread += 1
                        continue
                    psoup = BeautifulSoup(phtml, PARSER)
                    pcrumbs = self._crumbs(phtml)
                    pmain = self._clean_main(psoup, cut=False)
                    if pmain is None:
                        self._warnings.append("no main region on %s" % pfinal)
                        unread += 1
                        continue
                    ph1 = _text(pmain.find("h1")) or _text(psoup.find("h1")) \
                        or _text(a)
                    deeper = any(s in pfinal for s in self.file_subfolders)
                    for fa in pmain.find_all("a", href=True):
                        fu = urljoin(pfinal, fa["href"].strip()).split("#")[0]
                        if not _is_file(fu) or fu in seen_files or \
                                urlparse(fu).netloc.lower() not in self.file_hosts:
                            continue
                        seen_files.add(fu)
                        trail = [sec]
                        if deeper:
                            h3 = fa.find_previous("h3")
                            if h3 is not None:
                                trail.append(_text(h3))
                        files.append(self._file(fu, self._file_title(fa, None, ph1),
                                                trail, pfinal, pcrumbs, ph1))
                    pages.append(self._page(pfinal, ph1, [sec], pcrumbs, psoup, pmain))
                # The landing page last: its body is built from `main`, and the
                # walk above only reads it.
                pages.insert(0, self._page(final, landing_h1, [], crumbs, soup, main))

                files = self._pair_translations(files)
                for f in files:
                    got = self._read_file(ctx, f["doc_url"])
                    if "error" in got:
                        unread += 1
                        self._warnings.append("could not read file %s: %s"
                                              % (f["doc_url"], got["error"]))
                        f["file_unread"] = True
                        f["file_error"] = got["error"]
                        continue
                    f["text"] = got["text"]
                    f["sha256"] = got["sha256"]
                    f["bytes"] = got["bytes"]
                    basis = got["text"] or ("sha256:" + got["sha256"])
                    if f.get("urdu_url"):
                        basis += "\ntranslation: " + f["urdu_url"]
                    f["content_hash"] = content_key(basis)
                    f["hash_basis"] = ("file-text" if got["text"]
                                       else "file-bytes-sha256")
                    logger.info("  %-60s %7d chars %s", f["title"][:60],
                                len(got["text"]),
                                "(+es)" if f.get("urdu_url") else "")
            finally:
                ctx.close()

        if unread:
            self._warnings.append(
                "coverage gap: %d page(s)/file(s) unread this run; their rows "
                "are missing or unfingerprinted, not withdrawn" % unread)
        logger.info("CFPB %s: %d page row(s), %d file row(s), %d unread, %.0fs",
                    self.source_system, len(pages), len(files), unread,
                    time.time() - t0)
        return {
            "shape": "generic",
            "pages": pages,
            "documents": files,
            "run": {"seed": self.seed_url, "links": len(pages) + len(files),
                    "unread": unread, "blocked_pages": 0},
        }

    def _page(self, url, title, trail, crumbs, soup, main) -> dict:
        lastmod = self._last_modified(main)
        meta = soup.select_one(".o-item-introduction .meta time")
        frag, text = self._body_of(main, url)
        return {
            "section_path": " > ".join(crumbs),
            TRAIL_KEY: trail,
            "page_h1": title,
            "cfpb_last_modified": lastmod,
            "published": ((meta.get("datetime") or "")[:10] if meta is not None
                          else ""),
            "title": title, "url": url,
            "depth": 1, "linked_from_title": title,
            "doc_url": url, "parent_page_url": self.seed_url, "status": "ok",
            "n_pdfs": 0, "pdf_links": "",
            "text_len": len(text), "html_file": "", "text": text, "html": frag,
            "content_hash": content_key(self._hash_basis(frag, text)),
        }

    @staticmethod
    def _file(url, title, trail, found_on, crumbs, found_h1) -> dict:
        return {"doc_url": url, "title": title, TRAIL_KEY: trail,
                "type": urlparse(url).path.rsplit(".", 1)[-1].upper(),
                "found_on": found_on,
                "section_path": " > ".join(list(crumbs) + [found_h1]),
                "content_hash": content_key("%s|%s" % (url, title))}

    def _pair_translations(self, files: List[dict]) -> List[dict]:
        """A "(en Español)" file whose English original sits in the same folder
        becomes that row's urdu_url; one with no partner stays a row."""
        # "(en Español)" sits in the title or in the note after it
        # ("Negative amortization — (en Español)"): the dash it leaves goes too.
        def bare(t):
            return _key(re.sub(r"[\s—–-]+$", "", _SPANISH.sub(" ", t)))
        by = {}
        for f in files:
            if not _SPANISH.search(f["title"]):
                by.setdefault((tuple(f[TRAIL_KEY]), bare(f["title"])), f)
        out = []
        for f in files:
            if _SPANISH.search(f["title"]):
                en = by.get((tuple(f[TRAIL_KEY]), bare(f["title"])))
                if en is not None and not en.get("urdu_url"):
                    en["urdu_url"] = f["doc_url"]
                    continue
                self._warnings.append("translation with no English original, "
                                      "kept as a row: %s" % f["title"])
            out.append(f)
        return out

    # ------------------------------------------------------------------ #
    #  rows -> documents                                                  #
    # ------------------------------------------------------------------ #
    def _doc_from_page_row(self, r: dict, shape: str):
        doc = GenericSiteCrawler._doc_from_page_row(self, r, shape)
        if doc is None:
            return doc
        doc.doc_path = self._doc_path(
            " > ".join(s.replace(">", "›") for s in r.get(TRAIL_KEY) or []),
            doc.title)
        doc.published_date = r.get("published") or None
        doc.year = (r.get("published") or "")[:4] or None
        if isinstance(getattr(doc, "extra_meta", None), dict):
            doc.extra_meta["content_hash_basis"] = \
                "visible-text-minus-furniture+image-srcs"
            doc.extra_meta["section_path"] = r.get("section_path") or ""
            if r.get("page_h1"):
                doc.extra_meta["page_h1"] = r["page_h1"]
            if r.get("cfpb_last_modified"):
                doc.extra_meta["cfpb_last_modified"] = r["cfpb_last_modified"]
        return doc

    def _doc_from_document_row(self, d: dict, shape: str):
        doc = GenericSiteCrawler._doc_from_document_row(self, d, shape)
        if doc is None:
            return doc
        doc.doc_path = self._doc_path(
            " > ".join(s.replace(">", "›") for s in d.get(TRAIL_KEY) or []),
            doc.title)
        if d.get("urdu_url"):
            doc.urdu_url = d["urdu_url"]
        if isinstance(getattr(doc, "extra_meta", None), dict):
            doc.extra_meta["section_path"] = d.get("section_path") or ""
            doc.extra_meta["content_hash_basis"] = d.get("hash_basis") or "url|title"
            # The text the pipeline takes before trying its own download,
            # which this host refuses (textinput.decide prefers content_text).
            if d.get("text"):
                doc.extra_meta["content_text"] = d["text"]
            if d.get("sha256"):
                doc.extra_meta["file_sha256"] = d["sha256"]
                doc.extra_meta["file_bytes"] = d["bytes"]
            if d.get("file_unread"):
                doc.extra_meta["file_unread"] = True
                doc.extra_meta["file_error"] = d.get("file_error", "")
        return doc
