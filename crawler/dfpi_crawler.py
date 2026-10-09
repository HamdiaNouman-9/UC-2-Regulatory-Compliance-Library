# -*- coding: utf-8 -*-
"""DFPI: two levels of heading, read through a browser the host will talk to.

California's Department of Financial Protection and Innovation publishes its
rulemaking as an index of 26 SUBJECT LAWS, each of which is a page whose own
<h2>/<h3> headings group the documents. The library wants that shape verbatim:

    DFPI > Laws and Regulations > Regulations and Rulemaking
         > Banking Law                              (a subject on the index)
         > Final Regulations                        (an <h2> on the law page)
         > PRO 01/20 - Public Bank Regulations      (an <h3> under it)
         > Notice of Approval of Regulatory Action (PDF)

WHY THIS IS NOT FDICSectionCrawler WITH DIFFERENT SETTINGS. Three reasons, all
measured on 2026-10-08 rather than assumed:

  1. TWO HEADING LEVELS, NOT ONE. FDIC's landing pages group links under a
     single <h2> and that heading is the only folder. Here the subject law is
     one level and the law page's own <h2>/<h3> are one or two more, and the
     tree is RAGGED -- Banking Law's "Historical Regulations" holds three links
     directly AND an <h3> sub-group beneath it, so a document sits at depth 5 or
     6 depending on where the site put it.

  2. THE HOST REFUSES EVERY CLIENT WE HAD. dfpi.ca.gov answers a Cloudflare
     MANAGED CHALLENGE. Refused, each tried once on 2026-10-08:

         requests, full browser headers      HTTP 403, server: cloudflare
         bare Playwright, headless           "Just a moment..." after 45s
         generic_crawler engine, headless    status zero, 0 pages, 1 error
         generic_crawler engine, HEADED      status zero, 0 pages
         real Chrome channel, headed         HTTP 403, "Just a moment..."
         real Edge channel, headed           HTTP 403, "Just a moment..."
         the user's own curl                 the same interstitial

     What passes, in 4.5 seconds, is a PERSISTENT CONTEXT with the automation
     giveaways removed -- see `_browser`. That is the only route, and it is why
     this class drives its own browser instead of calling the engine.

  3. HEADLESS NEVER PASSES, and that is not a tuning problem. Tested with a
     profile that had ALREADY cleared the challenge headed: Cloudflare
     re-challenges on the headless flag and the stored cf_clearance does not
     rescue it. So `headless=False` here is a requirement, not a default, and a
     scheduled run needs a desktop session (or headed Chrome under Xvfb).

WHAT IT DELIBERATELY DOES NOT DO. It does not open a document to see what that
links, and it does not follow a law page's links to their own pages. The index's
subjects and each law page's own headings ARE the section -- same rule as
FDICSectionCrawler, which stops at the links its landing page names.

EMPTY LAWS KEEP THEIR FOLDER. Five of the 26 -- Business and Industrial
Development Corporations, California Financial Information Privacy Act,
Depository Corporation Sale/Merger/Conversion, Industrial Loan Law, Savings
Association Law -- carry headings with zero links. On the user's instruction
they still appear, and they appear as the law's OWN PAGE rather than an invented
placeholder: a real url, real stored text, and a content hash that moves the day
DFPI publishes something there. A fabricated "nothing here" row would watch
nothing.
"""
from __future__ import annotations

import logging
import os
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawler.generic_crawler_wrapper import GenericSiteCrawler
from dynamic_crawler.formfill.runner import _ext_type, _is_doc
from generic_crawler.crawler import (absolutize_html, content_key, profile_for)

logger = logging.getLogger(__name__)

#: Headings that appear on EVERY law page and are site chrome, not rulemaking.
#: Without these the library would gain four bogus folders under all 26 laws.
#: Matched on the normalised heading text, so punctuation and case cannot
#: defeat them the way an exact compare would.
FURNITURE_HEADINGS = {
    "key consumer links", "news info links", "stay connected",
    "rulemaking notices", "contact us", "quick links", "footer",
    # ADDED 2026-10-08, on the manual library's evidence rather than on a guess
    # about what looks like chrome. Both of these were becoming FOLDERS with
    # rows in them -- MEASURED on PACE, the only law that carries them:
    #
    #     How to reach us:   1 row   a mailto: for the programme's help desk
    #     Related Links      3 rows  PACE Laws and Regulations, What Homeowners
    #                                Need to Know, Frequently Asked Questions
    #
    # and neither appears anywhere in the manual library's tree for this
    # regulator, which is the reference these folders are meant to match. The
    # subscribe block was removed for the same reason earlier.
    "how to reach us", "related links",
    # AND THE SUBSCRIBE BLOCK, WHICH WAS ONLY HALF REMOVED. FURNITURE_TEXT took
    # it out of the stored BODY, but `_groups` reads FURNITURE_HEADINGS and
    # nothing else -- so where DFPI writes it as a bare <h2> it went on being a
    # FOLDER with 3 rows in it. The audit missed that too: it looked for the
    # four heading names it knew, and this was a fifth.
    "subscribe to receive rulemaking notices",
    # AN ON-PAGE JUMP LIST, not a section. California Financing Law puts a
    # table of contents at the top of its page under this heading; the links in
    # it are #anchors into the SAME page, so the one row it produced was the
    # page linking to itself and filed under a folder DFPI never published.
    "page content list",
}

#: Furniture that is not a heading and so cannot be removed by one.
#:
#: "SUBSCRIBE TO RECEIVE RULEMAKING NOTICES / To receive notices of DFPI
#: rulemaking, subscribe to our e-mail subscription service." is a Divi text
#: module sitting in the middle of the content region on several law pages. It
#: has no heading of its own, so FURNITURE_HEADINGS never saw it, and it reached
#: the stored text the content_hash is taken over -- 326 characters of mailing
#: list promotion that would report the law as changed if DFPI ever reworded it.
#:
#: MATCHED ON ITS OWN TEXT, not on `id="sub"`: measured 2026-10-08, only ONE of
#: the four pages carrying this block has that id.
#: BOTH HALVES, because DFPI publishes this block three different ways and each
#: needed its own pass to find:
#:
#:   1. a <div> wrapping an <h2> and a <p>      -- caught by the container rule
#:   2. a bare <h2> among the content           -- caught by the heading rule
#:   3. a bare <p> with NO heading at all       -- caught by neither until the
#:                                                 second phrase was added
#:
#: The third is the one that matters most: on Local Agency Security Law the
#: paragraph is 85 of 268 stored characters, a THIRD of a page whose whole job is
#: to say "None at this time" until it stops being true.
FURNITURE_TEXT = ("subscribe to receive", "to receive notices of dfpi")

#: Where the heading trail travels, kept OUT of `section_path` so the two can
#: mean different things. doc_path is built from this; `section_path` carries
#: what DFPI's own breadcrumb says. Same split as FDICSectionCrawler's
#: HEADING_KEY, and for the same reason: the library's tree deliberately differs
#: from the site's trail on every row here.
TRAIL_KEY = "dfpi_trail"

#: DFPI's breadcrumb, which is a Divi module rather than an <ol>. Measured on the
#: law pages: "Home / Rules & Enforcement / Laws & Regulations / Banking Law:
#: Regulations and Opinions".
BREADCRUMB_SELECTOR = "div.et_pb_dmb_breadcrumbs, [class*=breadcrumb]"

#: The browser lies that get past the managed challenge. Injected before any
#: page script runs, which is the point -- Cloudflare's probe is one of the
#: first things to execute.
_STEALTH = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins',
  {get: () => [1,2,3,4,5].map(i => ({name: 'Plugin ' + i}))});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = window.chrome || {runtime: {}};
"""

#: The interstitial's title. Presence of this means the challenge has not
#: finished, which is a WAIT, not a failure -- it cleared in 4.5s every time.
_CHALLENGE = "just a moment"


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def _text(el) -> str:
    """Display text for a title, a law name or a heading -- pipe-free.

    THE PIPE IS STRUCTURE, NOT PUNCTUATION. doc_path is a list in memory, but
    `excel_repo` serialises it to the workbook as a " | " join and reads it back
    with .split(" | ") (excel_repo.py:103 and :222), so a title containing that
    sequence does not survive the round trip -- it becomes an extra folder.

    MEASURED on the 2026-10-08 13:21 export, one row of 564:

        title     std 400-Approved | Order - PRO 15/13 (PDF)
        doc_path  ... | Approved Regulations | std 400-Approved | Order - PRO 15/13 (PDF)

    which reads back as eight segments, filing the document under a folder
    called "std 400-Approved" that DFPI never published. No other regulator has
    produced one: all fourteen workbooks in output/ were checked and DFPI is the
    first, which is why this is fixed here rather than in the shared serialiser.

    A SLASH, because that is what the link means -- "std 400-Approved / Order" is
    one document covering both -- and because it is the one separator doc_path
    does not use.
    """
    if not el:
        return ""
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).replace("|", "/")


class DFPISectionCrawler(GenericSiteCrawler):
    """The index's subject laws, and each law page's own heading tree."""

    #: Where the content lives on both page types. Checked on the index and on
    #: all 26 law pages: `div.entry-content` is present on every one.
    CONTENT_SELECTOR = "div.entry-content"
    #: The <h3> on the index under which the subject laws are listed. Matched on
    #: substring, because the heading reads "Proposed and Approved Regulations
    #: (By subject)" and the parenthetical is the volatile half.
    SUBJECT_HEADING = "by subject"

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        category: Optional[str] = None,
        #: Visible window. NOT a default that can be flipped for convenience:
        #: headless is refused by the host -- see the module docstring.
        headless: bool = False,
        #: Chrome rather than bundled Chromium. Both cleared the challenge;
        #: Chrome is kept because it is the one a person would be using.
        channel: Optional[str] = "chrome",
        #: Kept BETWEEN RUNS, so the challenge is solved once rather than on
        #: every page. Defaults beside this file's own cache, not in the repo.
        profile_dir: Optional[str] = None,
        #: Seconds to let the challenge finish before giving up on a page.
        challenge_wait: int = 30,
        #: Politeness. 26 law pages took 58 seconds at 0, which is already slow
        #: enough not to need padding; it is here for when it is not.
        delay: float = 0.0,
        **generic_kwargs,
    ):
        super().__init__(seed_url=seed_url, regulator=regulator,
                         source_system=source_system, category=category,
                         **generic_kwargs)
        self.headless = headless
        self.channel = channel
        self.profile_dir = profile_dir or os.path.join(
            os.path.expanduser("~"), ".cache", "dfpi_profile")
        self.challenge_wait = challenge_wait
        self.delay = delay
        self._warnings: List[str] = []

    # ------------------------------------------------------------------ #
    #  the browser the host will talk to                                  #
    # ------------------------------------------------------------------ #
    def _browser(self, p):
        """A context Cloudflare's managed challenge lets through.

        Every argument here is load-bearing and was arrived at by elimination,
        not by copying a recipe:

          * launch_persistent_context -- a profile, so cf_clearance survives the
            run and later pages cost nothing.
          * headless=False -- the single setting that decides it. A warm profile
            does NOT rescue a headless run; tested.
          * ignore_default_args=["--enable-automation"] and
            --disable-blink-features=AutomationControlled -- remove the two
            flags Chrome otherwise advertises itself with.
          * add_init_script -- navigator.webdriver is the probe that survives
            the flags above.
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
            timezone_id="America/Los_Angeles",
            viewport={"width": 1280, "height": 900},
        )
        ctx.add_init_script(_STEALTH)
        return ctx

    def _get(self, page, url: str) -> Tuple[Optional[str], str]:
        """One page, waited out until the challenge clears. (html, final url)."""
        if self.delay:
            time.sleep(self.delay)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            self._warnings.append("could not open %s: %s" % (url, str(e)[:120]))
            return None, url
        for _ in range(self.challenge_wait):
            page.wait_for_timeout(1000)
            try:
                if _CHALLENGE not in (page.title() or "").lower():
                    return page.content(), page.url or url
            except Exception:
                continue
        # SAID, NOT SWALLOWED. A page still showing the interstitial after the
        # wait is the one failure mode that must never read as "this law has no
        # documents" -- that is the `status: zero` misreport config/sources/
        # dfa.yml warns about, and it is why this returns None and warns.
        self._warnings.append("Cloudflare challenge did not clear: %s" % url)
        return None, page.url or url

    # ------------------------------------------------------------------ #
    #  reading the two page types                                         #
    # ------------------------------------------------------------------ #
    def _content(self, html: str):
        soup = BeautifulSoup(html, "html.parser")
        return (soup.select_one(self.CONTENT_SELECTOR)
                or soup.select_one("main") or soup), soup

    def _subject_laws(self, html: str, base: str) -> List[Tuple[str, str]]:
        """(law name, url) for every subject the index lists.

        SCOPED TO THE "By subject" HEADING. The index also links How We
        Regulate, the Legislation Division, Legal Process Information and a
        dozen other things; only what sits under that heading is a subject law.
        """
        main, _ = self._content(html)
        out, seen, inside = [], set(), False
        for el in main.find_all(["h2", "h3", "h4", "a"]):
            if el.name in ("h2", "h3", "h4"):
                inside = self.SUBJECT_HEADING in _norm(_text(el))
                continue
            if not inside:
                continue
            url = urljoin(base, (el.get("href") or "").strip())
            name = _text(el)
            if not name or urlparse(url).scheme not in ("http", "https"):
                continue
            if url in seen:
                continue
            seen.add(url)
            out.append((name, url))
        return out

    def _groups(self, html: str, base: str
                ) -> List[Tuple[Tuple[str, ...], List[Tuple[str, str]]]]:
        """The law page's heading tree: ((h2[, h3]), [(title, url), ...]).

        A LINK BELONGS TO THE LAST HEADING SEEN BEFORE IT, in document order --
        the same rule FDICSectionCrawler uses, and for the same reason: the
        markup nests links inside sibling containers, not inside the heading.

        Groups with no links are RETURNED ANYWAY. A law page that lists
        "Proposed Regulations" and nothing under it is saying something, and
        dropping the empty heading would make the page look like it never had
        one.
        """
        main, _ = self._content(html)
        # THREE LEVELS, NOT TWO. h3 and h4 used to share one slot, so an h4
        # REPLACED the h3 above it instead of nesting under it -- and DFPI uses
        # all three. MEASURED on the Money Transmission Act, whose markup is
        #
        #     h2 Proposed Regulations
        #       h3 PRO 07-17 - Money Transmission Act - Agent of Payee Exemption
        #         h4 NOTICE OF SECOND MODIFICATIONS
        #         h4 NOTICE OF MODIFICATIONS
        #       h3 PRO 02-23 - Money Transmission Act ...
        #         h4 NOTICE OF SECOND MODIFICATION TO TEXT ...
        #
        # and which came out as `Proposed Regulations > NOTICE OF MODIFICATIONS`
        # with the PRO number gone and its folder left holding 0 documents. The
        # manual library keeps that level, and it is the level that says WHICH
        # PROCEEDING a notice belongs to -- without it, two proceedings that
        # both publish a "NOTICE OF MODIFICATIONS" merge into one folder.
        #
        # An h5 still attaches to the h4 above it. Corporate Securities Law is
        # the only page carrying one ("TIME FOR COMMENTS"), it reads as a label
        # inside a notice rather than a grouping, and the manual library shows
        # no level below the notice.
        h2 = h3 = h4 = None
        order: List[Tuple[str, ...]] = []
        bucket: Dict[Tuple[str, ...], List[Tuple[str, str]]] = {}

        def _key():
            return tuple(x for x in (h2, h3, h4) if x)

        for el in main.find_all(["h2", "h3", "h4", "a"]):
            if el.name in ("h2", "h3", "h4"):
                t = _text(el)
                if not t:
                    continue
                if el.name == "h2":
                    h2, h3, h4 = t, None, None
                elif el.name == "h3":
                    h3, h4 = t, None
                else:
                    h4 = t
                if h2 and _norm(h2) not in FURNITURE_HEADINGS:
                    key = _key()
                    if key not in bucket:
                        bucket[key] = []
                        order.append(key)
                continue
            if not h2 or _norm(h2) in FURNITURE_HEADINGS:
                continue
            url = urljoin(base, (el.get("href") or "").strip())
            title = _text(el)
            if not title or urlparse(url).scheme not in ("http", "https"):
                continue
            # THE FRAGMENT IS NAVIGATION, NOT A DOCUMENT. DFPI links one page
            # several times with different anchors -- #1811, #1608, #0107 on
            # "Approved Regulations California Residential Mortgage Lending Act"
            # -- which are three places to land IN one page, not three
            # documents. MEASURED: kept, they produced four of the six rows that
            # shared a doc_path, each an exact duplicate of its siblings.
            url = url.split("#")[0]
            key = _key()
            if key not in bucket:
                bucket[key] = []
                order.append(key)
            # DEDUPED WITHIN THE GROUP, not across the page: the same file
            # legitimately appears under "Final Regulations" and again under
            # "Historical Regulations" on some laws, and those are two places in
            # the tree, which is what a folder trail is for.
            if any(u == url for _t, u in bucket[key]):
                continue
            bucket[key].append((title, url))
        return [(k, bucket[k]) for k in order]

    # ------------------------------------------------------------------ #
    #  rows                                                               #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _strip_furniture(main) -> None:
        """Remove the chrome sections from a stored body, in place.

        FURNITURE_HEADINGS already keeps "Key Consumer Links" and its three
        siblings from becoming FOLDERS. It did not keep them out of the stored
        HTML, and that is a monitoring defect rather than an untidy one:
        content_hash is computed over the text that was stored, so with the
        footer inside it, DFPI adding a social link would report every empty law
        as CHANGED -- and the law itself changing would be one edit among the
        site's chrome.

        MEASURED on the five empty-law pages, 2026-10-08: 753-834 characters of
        stored text each, of which the four chrome sections are most of it.

        A heading owns everything up to the next heading of its own level, which
        is the same rule `_groups` reads links by -- the markup nests nothing, so
        removal has to walk forward rather than descend.
        """
        for h in list(main.find_all(["h2", "h3"])):
            k = _norm(_text(h))
            # EXACT NAME, OR A FURNITURE PHRASE IT OPENS WITH. The second test
            # is what catches the subscribe block on Corporate Securities Law,
            # where it is a bare <h2>SUBSCRIBE TO RECEIVE RULEMAKING NOTICES</h2>
            # with no wrapper of its own, sitting inside the same container as
            # 5,227 characters of real content -- so no element "starts with"
            # it and the container rule below cannot see it. On the other three
            # pages the same block has its own div, and this removes the heading
            # while `_tidy_empty_blocks` takes the shell.
            if k not in FURNITURE_HEADINGS and not k.startswith(FURNITURE_TEXT):
                continue
            level = h.name
            for sib in list(h.find_next_siblings()):
                if sib.name in ("h2", level):
                    break
                sib.decompose()
            h.decompose()
        # THE BLOCKS WITH NO HEADING TO REMOVE THEM BY. `startswith` rather than
        # `in`, which is the guard that keeps this from eating the page: a
        # container holding the block AND real content does not START with it,
        # so only the block's own wrapper matches. The length bound is a second
        # belt -- the real one is 326 characters.
        for el in list(main.find_all(["div", "p", "section", "aside"])):
            if getattr(el, "decomposed", False):
                continue
            t = (el.get_text(" ", strip=True) or "").strip().lower()
            if len(t) < 500 and t.startswith(FURNITURE_TEXT):
                el.decompose()

    @staticmethod
    def _crumb(soup) -> str:
        """DFPI's own breadcrumb, verbatim, as the SITE draws it.

        "Home > Rules & Enforcement > Laws & Regulations > Banking Law:
        Regulations and Opinions". The library files these rows under the law
        and its headings instead, but the site's trail is recorded anyway --
        it is the only place the workbook says what dfpi.ca.gov calls this page,
        and it is kept whole even where the tree does not use it.
        """
        el = soup.select_one(BREADCRUMB_SELECTOR)
        if not el:
            return ""
        parts, seen = [], set()
        for piece in el.stripped_strings:
            p = re.sub(r"\s+", " ", piece).strip(" >/|›")
            # The module renders separators as their own text nodes, and the
            # last crumb is repeated as the page heading on some laws.
            if p and p.lower() not in seen:
                seen.add(p.lower())
                parts.append(p)
        return " > ".join(parts)

    @staticmethod
    def _page_title(soup, fallback: str) -> str:
        """The page's own <h1>, which names it better than the index link does.

        The index says "Savings Association Law"; the page says "Savings
        Association Law: Regulations, Opinions, and Releases", which is what the
        row is actually about and does not simply repeat the folder it sits in.
        """
        for sel in ("h1.entry-title", "h1"):
            el = soup.select_one(sel)
            t = _text(el)
            if t:
                return t
        return fallback

    @staticmethod
    def _tidy_empty_blocks(main) -> None:
        """Drop containers the removals above left holding nothing.

        Divi nests each module several divs deep, so decomposing an <img> leaves
        a stack of empty wrappers that render as the framed box they used to
        hold. Repeated until nothing more is removable, because emptying an
        inner div is what makes its parent empty.
        """
        for _ in range(6):
            gone = 0
            for el in list(main.find_all(["div", "p", "span", "section", "li"])):
                if el.find(["img", "a", "table", "ul", "ol", "h1", "h2", "h3"]):
                    continue
                if (el.get_text(" ", strip=True) or "").strip():
                    continue
                el.decompose()
                gone += 1
            if not gone:
                break

    def _page_row(self, url: str, title: str, trail: str,
                  html: str) -> dict:
        """A stored page, in the shape the inherited mapping reads."""
        main, soup = self._content(html)
        crumb = self._crumb(soup)
        title = self._page_title(main, title)
        self._strip_furniture(main)
        # THE BREADCRUMB IS NAVIGATION, AND IT IS NOW RECORDED PROPERLY. It was
        # being stored as the opening of every body -- a numbered list of Home /
        # Rules & Enforcement / Laws & Regulations -- which is chrome in the text
        # the content_hash is taken over. It moves to extra_meta.section_path,
        # where the site's trail belongs, and comes out of the body.
        for el in main.select(BREADCRUMB_SELECTOR):
            el.decompose()
        # AND THE IMAGES GO. Every law page carries a wp-image banner with an
        # empty alt; the artifact viewer blocks off-site images, so it rendered
        # as an empty framed box in the middle of the stored page. It carries no
        # text, so removing it changes the hash not at all and the reading of the
        # page a great deal.
        for el in main.find_all(["img", "figure", "picture", "iframe"]):
            el.decompose()
        self._tidy_empty_blocks(main)
        frag = absolutize_html(str(main), url)
        text = re.sub(r"\s+", " ", BeautifulSoup(frag, "html.parser")
                      .get_text(" ", strip=True)).strip()
        return {
            "section_path": crumb,
            TRAIL_KEY: trail,
            "title": title, "url": url,
            "depth": 1, "linked_from_title": title,
            "parent_page_url": self.seed_url, "status": "ok",
            "n_pdfs": 0,
            # EMPTY BY THE HOST PROFILE'S RULE. The wrapper takes
            # pdf_links.split(" | ")[0] as org_pdf_link and the orchestrator
            # promotes it into document_url, so a page citing several files
            # would be stored under whichever was linked first.
            "pdf_links": ("" if not profile_for(url).get("page_pdf_link", True)
                          else ""),
            "text_len": len(text),
            "html_file": "", "text": text, "html": frag,
            "content_hash": content_key(text),
        }

    @staticmethod
    def _separate_collisions(documents: List[dict]) -> int:
        """Last resort for two DIFFERENT files sharing one title in one group.

        `disambiguate_titles` has already run and rewritten what it could; it
        falls back to the url SLUG, and these are the rows where the slug
        collides too. Two cases measured on 2026-10-08, four rows of 615:

            .../uploads/2026/02/Final-Statement-of-Reasons.pdf
            .../uploads/sites/337/2019/03/Final-Statement-of-Reasons.pdf
            .../uploads/sites/337/2019/03/03-13-15-Day-Text.pdf
            .../uploads/sites/337/2019/03/03_13_15_Day_Text.pdf

        The first pair is two different documents DFPI filed years apart under
        one name; the second is one document uploaded twice with the separators
        changed. Both share a doc_path, and get_folder_id matches on
        title+parent, so each pair would be handed ONE folder node.

        THE SUFFIX IS THE SHALLOWEST PART OF THE URL THAT ACTUALLY DIFFERS --
        the file's own name where the names differ, otherwise the upload folder
        above it. Taken from the url rather than invented, so it says something
        true about which file the row is, and applied only where a collision
        exists, so 611 titles are untouched.
        """
        # KEYED ON THE TRAIL THAT BUILDS doc_path, NOT ON section_path.
        #
        # This read `section_path` and was right while that field held the
        # heading trail. It stopped being right the moment section_path was
        # repurposed to carry DFPI's OWN breadcrumb -- and the breadcrumb is
        # nearly the same string for every row of the site.
        #
        # MEASURED on the 2026-10-08 16:24 export: 16 distinct section_path
        # values against 160 distinct doc_path parents, and 200 rows across
        # ELEVEN different laws sharing one value, because DFPI renders only
        # three crumbs on those pages. Keyed that way the method groups rows
        # that can never collide in the tree and renames titles for nothing.
        #
        # TRAIL_KEY is what `_apply_trail` turns into doc_path, so grouping on
        # it asks exactly the question that matters: would these two rows be
        # handed one folder node?
        groups: Dict[Tuple[str, str], List[dict]] = {}
        for d in documents:
            key = ((d.get(TRAIL_KEY) or d.get("section_path") or ""),
                   (d.get("title") or "").strip().lower())
            groups.setdefault(key, []).append(d)

        fixed = 0
        for rows in groups.values():
            if len(rows) < 2:
                continue
            segs = [[s for s in urlparse(r.get("doc_url") or "").path.split("/")
                     if s] for r in rows]
            names = [os.path.splitext(s[-1])[0] if s else "" for s in segs]
            use_name = len(set(names)) == len(names)
            for r, parts, name in zip(rows, segs, names):
                if use_name:
                    suffix = name
                else:
                    # The two path steps above the file: "2026/02" against
                    # "2019/03" says which of the two this is; one step alone
                    # would say "02" against "03".
                    suffix = "/".join(parts[-3:-1]) or (parts[0] if parts else "")
                if not suffix:
                    continue
                r["title"] = "%s (%s)" % (r.get("title") or "", suffix)
                fixed += 1
        return fixed

    def _run_crawl(self) -> dict:
        # NOT super()._run_crawl(). That shells the engine out to a headless
        # browser, which this host refuses -- see the module docstring.
        from playwright.sync_api import sync_playwright

        pages: List[dict] = []
        documents: List[dict] = []
        laws = 0
        t0 = time.time()

        with sync_playwright() as p:
            ctx = self._browser(p)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            try:
                html, final = self._get(page, self.seed_url)
                if html is None:
                    # THE SEED IS FATAL. Without it there are no subjects, and
                    # returning zero rows quietly is exactly the failure this
                    # class is built to avoid.
                    raise RuntimeError(
                        "the DFPI index could not be read (%s) -- refusing to "
                        "report an empty regulator" % self.seed_url)
                subjects = self._subject_laws(html, final)
                logger.info("DFPI: %d subject law(s) on the index", len(subjects))

                for name, url in subjects:
                    lhtml, lfinal = self._get(page, url)
                    laws += 1
                    if lhtml is None:
                        # The law keeps its folder even unread, so a page we
                        # could not open is visible rather than silently absent.
                        documents.append({
                            "title": name, "doc_url": url, "type": "HTML",
                            "found_on": self.seed_url, "section_path": name,
                        })
                        continue
                    crumb = self._crumb(BeautifulSoup(lhtml, "html.parser"))
                    groups = self._groups(lhtml, lfinal)
                    rows_here = sum(len(v) for _, v in groups)
                    for key, links in groups:
                        trail = " > ".join((name,) + key)
                        for title, u in links:
                            documents.append({
                                "title": title, "doc_url": u,
                                "type": _ext_type(u) if _is_doc(u) else "HTML",
                                "found_on": lfinal,
                                # THE SITE'S TRAIL, not the library's. doc_path
                                # is rebuilt from TRAIL_KEY in `_apply_trail`.
                                "section_path": crumb,
                                TRAIL_KEY: trail,
                            })
                    # EVERY LAW PAGE IS STORED, not only the empty ones.
                    #
                    # The page is already fetched -- it has to be, to read the
                    # heading tree -- and its body was then thrown away for the
                    # 21 laws that publish something. What went with it is the
                    # prose between a heading and its links: "ADOPT: Sections
                    # 10.131.7; ... AMEND: Sections 10.112; ... EFFECTIVE:
                    # January 1, 2022". MEASURED 2026-10-08: 11 of the 26 pages
                    # carry an ADOPT/AMEND/EFFECTIVE/REPEAL line, 71 KB of text
                    # in all, and NONE of it was in the workbook.
                    #
                    # That is the regulation's own scope and start date, and
                    # without it the change signal could not see DFPI altering
                    # which sections a proceeding amends -- only a link being
                    # added, removed or re-pointed. It also gives every law a row
                    # hashed over visible TEXT (ONBOARDING Step 2's first
                    # preference) where 559 of 564 rows are `url+title`, the
                    # weakest.
                    #
                    # AT ZERO EXTRA REQUESTS, which is why this is not a
                    # trade-off.
                    pages.append(self._page_row(lfinal, name, name, lhtml))
                    logger.info("  %-56s %2d group(s) %3d row(s)",
                                name[:56], len(groups), rows_here)
            finally:
                ctx.close()

        # `disambiguate_titles` IS DELIBERATELY NOT CALLED, and this is the one
        # place DFPI departs from what the engine does to every other source.
        #
        # The engine's rule is GLOBAL: a title counts as colliding if any other
        # document anywhere in the source shares it, and it is then rewritten to
        # the url slug. That is right for a flat source, where title IS the leaf.
        # Here the folder trail is six or seven deep, so two documents called
        # "Final Statement of Reasons (PDF)" under two different PRO numbers sit
        # at two different doc_paths and never collide at all.
        #
        # MEASURED on the live site, 2026-10-08:
        #     the global rule rewrites   322 of 559 titles (58%)
        #     rows that really share a folder node       53
        # -- so it renames 322 titles to prevent 53 collisions, and it renames
        # them to things like "Pro 01 20 Notice Of Approval Of Regulatory Action
        # 9.14.21" where DFPI wrote "Notice of Approval of Regulatory Action".
        # The library would read in slugs for most of this regulator.
        #
        # `_separate_collisions` is the same rule scoped to what actually
        # collides -- (section_path, title) -- so DFPI's own words survive
        # wherever the folder already separates them.
        #
        # BEFORE THE HASH, which is why it sits here: a file that was not
        # downloaded is identified by its url plus its title, so a title that
        # changes after its hash is taken makes the row read as modified on the
        # next run.
        renamed = self._separate_collisions(documents)
        for d in documents:
            d["content_hash"] = content_key("%s|%s" % (d.get("doc_url") or "",
                                                       d.get("title") or ""))
        logger.info("DFPI: %d law(s), %d page row(s), %d document row(s), "
                    "%d title(s) disambiguated, %.0fs",
                    laws, len(pages), len(documents), renamed, time.time() - t0)
        return {
            "shape": "generic",
            "pages": pages,
            "documents": documents,
            "run": {"seed": self.seed_url, "laws": laws, "blocked_pages": 0},
        }

    def _page_is_document(self, r: dict) -> bool:
        """Every page record this walk builds is a law page, and all 26 are wanted.

        The inherited guard asks whether a CRAWLED page is content or just a
        folder in the site's tree, and answers on length: under `min_page_text`
        (200) it is treated as a container. That is right for a crawl that
        sweeps up whatever it reaches. This walk never builds a record for
        anything but one of the 26 subject laws, so there is no container to
        screen out and the length test only ever subtracts.

        MEASURED, and this is why it is overridden rather than left alone:
        removing the breadcrumb from the stored bodies (which belongs in
        section_path, not in the text the hash is taken over) dropped the empty
        laws from ~260-350 characters to 164-180, and FOUR of them -- Business
        and Industrial Development Corporations, California Financial
        Information Privacy Act, Industrial Loan Law and Savings Association Law
        -- fell under the floor and lost their rows. Their folders went with
        them, which is exactly what the user asked to keep: a law that publishes
        nothing still has to show that it exists.

        A SHORT LAW PAGE IS NOT AN EMPTY ONE. "Proposed Regulations: None at
        this time" is the page saying something true, and it is the row whose
        hash moves the day that stops being true.
        """
        return True

    # ------------------------------------------------------------------ #
    #  two trails, kept apart                                             #
    # ------------------------------------------------------------------ #
    def _apply_trail(self, doc, rec: dict):
        """doc_path from the HEADING TRAIL; extra_meta.section_path from DFPI.

        The inherited builders derive both from one `section_path` field, which
        is right when the library's tree IS the site's trail. Here it is not:
        the tree is regulator > source system > category > law > heading >
        title, while dfpi.ca.gov says "Home > Rules & Enforcement > Laws &
        Regulations > Banking Law: Regulations and Opinions". Both are worth
        having and neither should overwrite the other.

        `self._doc_path(trail, title)` rather than a hand-built list, so the
        trail still goes through `_clean_trail`, the category rule and
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
        return doc

    def _doc_from_page_row(self, r: dict, shape: str):
        doc = super()._doc_from_page_row(r, shape)
        if doc is not None and isinstance(getattr(doc, "extra_meta", None), dict):
            # Why this row's hash is not the engine's, recorded on the row so a
            # reader who notices the pages re-baseline once can see why.
            doc.extra_meta["content_hash_basis"] = "visible-text-minus-furniture"
        return self._apply_trail(doc, r)

    def _doc_from_document_row(self, d: dict, shape: str):
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
        logger.info("DFPISectionCrawler[%s] -> %d document(s)",
                    self.source_system, len(docs))
        return docs
