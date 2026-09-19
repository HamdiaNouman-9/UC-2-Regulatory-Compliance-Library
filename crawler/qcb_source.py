"""
QCBLegislationSource — Qatar Central Bank's Legislation tab, read from the lists
it is actually built out of.

WHY THIS IS NOT ON THE GENERIC ENGINE
-------------------------------------
`generic_crawler/crawler.py` walks listing pages and harvests file links. The
Legislation page has neither. One URL —

    https://www.qcb.gov.qa/en/legislation/Pages/LegislationNew.aspx

— serves an empty shell, and jQuery then fills it from four SharePoint REST
lists. Tabs, the left-hand list of instruments and the file rows are all drawn
client-side and NONE of them changes the URL, so there is no second page for a
crawler to queue and nothing to follow.

MEASURED, 2026-09-17, the capped run in output/qbc_leg:

    n_pages 1      n_documents 1      text_len 0

One PDF. The same three lists answer with 85 English documents (counted below).
The crawl was not misconfigured — it read the DOM at the moment it had one link
in it, which is what a link-walker can see here.

THE FOUR LISTS, AND HOW THE PAGE CHAINS THEM
--------------------------------------------
Lifted from the inline script on the page itself (`LegislationHeaderCategories`,
`LegislationSubCategories`, `LegislationFiles`), all anonymous GETs:

    /_api/web/lists/getbytitle('CategoryHeaderLegislation')/items?$orderby=SortData
        -> THE TABS. Each row carries the names of the two lists below it:
           ListNameSubCat (its subcategories) and DocName (its file library).

    /_api/web/lists/getbytitle('<ListNameSubCat>')/items?$orderby=SortData
        -> THE SUBCATEGORIES, the left-hand column. Keyed by ID, and carrying
           `Level` (the indentation) and `Description` (the instrument's own
           text, drawn in the right-hand pane when you click the folder).

    /_api/web/lists/getByTitle('<DocName>')/items
        ?$filter=(Lang eq 'Both' or Lang eq 'EN')
        &$expand=File,FieldValuesAsText&$orderby=SortData desc,Id desc
        -> THE FILES. `LegislationCacegorieId` (the site's own spelling) is the
           foreign key back to a subcategory.

The fourth, `LegislationCategories`, is the same shape read by ID and is only
used by the page's search box. Nothing here needs it.

WHAT THE LISTS HELD WHEN THIS WAS WRITTEN (2026-09-17, EN/Both)
---------------------------------------------------------------
    Laws                          19 files across 11 subcategories  (+3 skipped)
    Licensing                      2 files across  2
    Supervisory Instructions      14 files across  7  (+2 headings)
    Combating Financial Crimes    45 files across  1
    Enforcement of Sanctions       2 files across  1
                                  --
                                  82

Those are a BASELINE, not a contract. A count that moves is the point of
crawling; a count that collapses to zero is what `fetch_documents` raises on.

SEVEN THINGS THIS SITE DOES THAT COST A READER AN AFTERNOON
-----------------------------------------------------------
1. ZERO-WIDTH SPACES, U+200B, inside stored strings — in titles
   ("Instruction<200b>s for Fi<200b>nancial Technology Companies") and, fatally,
   inside a LIST NAME: 'LF_Sup<200b>ervisoryInstructions'. The title is cleaned
   for display; the list name is NOT, because it is what the API is keyed on
   and a "tidied" one 404s. `<200b>` above stands in for the character:
   a module warning about invisible text must not contain any itself.

2. THREE FILES THE SITE CANNOT REACH, AND THIS SOURCE SKIPS THEM. Laws holds
   rows under LegislationCacegorieId 76, 77 and 78, and its subcategory list has
   no such IDs, so QCB's own sidebar has no entry that would ever display them:

       item 11  category 76  قانون رقم 20 لسنة 2019 ... .pdf
       item 12  category 77  LawNo16_2010.pdf
       item 13  category 78  LawNo9_2011.pdf

   All three are titled "Download full PDF", and all three are plausibly copies
   of laws already carried under a named subcategory.

   An earlier revision filed them under an invented "Uncategorised (category NN)"
   folder to keep them visible. That is worse: it puts documents in the library
   that are not published on the site, under a section name QCB has never used,
   and an invented folder gets read as real in a way an omission never does.

   THEY ARE NOT SILENT. Each is logged with its item ID, its category ID and its
   file, and `last_result["skipped_uncategorised"]` carries the list. If QCB adds
   the missing subcategory rows they appear on the next run with no change here.

3. THE SUBCATEGORY LIST IS TWO DEEP, and only in one tab. Every row carries a
   `Level`; Supervisory Instructions is the only tab that uses Level 1, and its
   two Level-0 rows are HEADINGS holding no files of their own:

       Instructions To Banks                              <- heading, 0 files
         Governance Instructions for Banks                    2
         Instructions To Banks                                1
         Instructions for Insurance Companies                 3
       Instructions to Other Financial Institutions       <- heading, 0 files
         Instructions to Financing Companines                 1
         Instructions to investment companies                 1
         Instructions to Exchange House                       1
         Instructions for Financial Technology Companies      5

   That is the indentation the site draws, and it explains the apparent
   duplicate: "Instructions To Banks" is both the heading (ID 1) and a leaf
   under it (ID 2). `_subcategories` returns a TRAIL per subcategory, so the
   leaf nests under the heading exactly as it does on the page.

   MANUAL-LIBRARY DIVERGENCE: the library's Supervisory Instructions is flat,
   six children, no headings, and omits Financing Companines. The site is what
   is mirrored here. Settle the difference against the first export.

4. SIX .txt STUBS, one of them the whole of "Instructions To Banks". These rows
   attach an 18-byte text file and put the real instrument behind `Link`, which
   on five of the six points at an ARABIC page
   (/ar/legislation/Pages/Instructions-To-Investment-Companies.aspx); the sixth,
   Exchange of tax information, links an English page (/en/Pages/FATCA.aspx).

   The stub file stays the document_url, because it is what the English page
   offers and swapping in the link would quietly change what a row IS. The link
   rides in extra_meta["linked_page"] so the gap is visible rather than guessed
   at. IF THE LIBRARY SHOULD CARRY THE PAGE INSTEAD, that is a decision to take
   against the manual library, not a bug to fix here.

5. BUTTON LABELS STORED AS TITLES. Four Laws rows are called "Download full
   PDF" — the words under the link, not the name of the instrument. Three of
   them are the unreachable files above; the fourth is the whole of the
   "Commercial law" subcategory, and takes that folder's name. `_title_for` has
   the rule and the limit on it: the folder names the document only where the
   folder holds ONE, since Combating Financial Crimes holds 45 and naming one of
   them after the folder would tell it apart from nothing.

6. A FOLDER CAN HOLD THE TEXT. `Description` on a subcategory row is prose the
   site shows when you open that folder. Measured on the English side, exactly
   one is real:

       QCB Law                    1,533 chars -- the Law No. (13) of 2012
                                  Issuance Articles, Article 1 onward
       Combating financial crimes    26 chars -- the folder's own name, dropped
       Commercial Companies Law            -- Arabic only, not taken on an EN run
       Investment Funds Law                -- Arabic only

   Where the folder holds ONE document the prose becomes that document's
   `document_html` and `content_text`: QCB Law is a single PDF plus the law's
   text, and they are the same instrument. Where it holds several there is no
   answer to which document the prose belongs to, so it rides along as
   `extra_meta["section_description"]` and nothing is claimed. Either way the
   text reaches the workbook; `section_description_promoted` says which happened.

7. `Year` IS NOT A PUBLICATION YEAR. The QCB Law of 2012 carries Year 2025; it
   drives the page's year filter. `published_date` is left None rather than
   filled from it, and both `Year` and the CMS `Modified` stamp are kept in
   extra_meta.

THE TREE
--------
    Qatar                                    <- config/countries.yml
      Qatar Central Bank (QCB)               <- regulator, doc_path[0]
        Legislation                          <- source_system
          Supervisory Instructions           <- the tab
            Governance Instructions for Banks   <- the subcategory
              Circular No. 25/2022: Governance instructions in banks

The last segment is the document: `_walk_folders` types it "R" and every segment
above it "F".
"""

from __future__ import annotations

import collections
import json
import logging
import re
import ssl
import time
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Sequence

from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import (GENERIC_LINK_TEXT, _norm_link_text,
                                     clean_doc_title, content_key)
from models.models import RegulatoryDocument

from dynamic_crawler.formfill.runner import _ext_type, _is_doc

logger = logging.getLogger(__name__)

REGULATOR = "Qatar Central Bank (QCB)"
SITE_URL = "https://www.qcb.gov.qa"

#: The tab list every other read starts from.
HEADER_LIST = "CategoryHeaderLegislation"

#: U+200B, sprinkled through this site's stored strings. Stripped from anything
#: a person reads; NEVER from a list name used in a URL.
_ZWSP = "\u200b"

#: SharePoint's default page is 100 items. Combating Financial Crimes alone
#: holds 45 English rows and the Arabic side of the same library is larger, so
#: ask for a page big enough to make `__next` the exception, and follow it when
#: it comes anyway.
PAGE_SIZE = 500

#: A guard, not a cap: a runaway `__next` chain would otherwise loop for ever.
#: At PAGE_SIZE this is 10,000 rows against a library measured at 85.
MAX_PAGES = 20

#: Tries per linked page, not per API call.
#:
#: This host refuses a share of a burst and picks a different share each time --
#: the same behaviour lloc.gov.bh taught the engine ("the host refuses a share of
#: the burst and picks a different share each time, which is exactly why
#: RELOADING WORKS"). Seen here on a bare six-request run: one page failed, the
#: identical run a minute later read all six.
#:
#: It is worth retrying because a MISSED READ IS NOT NEUTRAL. A row whose page
#: could not be read keeps a tier-2 hash while the same row on a successful run
#: carries a tier-1 one, so the next good run reports it `modified` with nothing
#: changed -- the self-moving hash ONBOARDING Step 2 says is worse than no hash.
PAGE_ATTEMPTS = 3


#: A title that is the words on the button rather than the name of the thing.
#: `GENERIC_LINK_TEXT` is matched exactly and cannot carry every phrasing — QCB
#: writes "Download full PDF", which is none of its entries — so this catches the
#: shape: an action verb, then at most a few words. Anchored and length-capped on
#: purpose, so a real title that merely BEGINS with one of these words ("Read
#: Across Guidance for Insurers") is far too long to match.
#:
#: The character class is spelled out rather than written with the usual
#: escapes, and it is the same set. This line was once written with a literal
#: 0x08 byte in place of a word boundary: it compiled, it matched nothing, and
#: it read correctly in every diff. Escapes here are not worth the risk.
_BUTTON_TITLE = re.compile(
    "^(?:download|view|open|click|press|read|see|show)"
    "[ A-Za-z0-9._-]{0,24}$",
    re.IGNORECASE)


#: What a SharePoint page keeps its editorial content in, tightest first.
#: `ms-rtestate-field` is the rich-text field an author actually types into;
#: PlaceHolderMain and .container both wrap it plus a share of the furniture.
#: MEASURED on the six linked pages: the three selectors return 5,011 / 5,024 /
#: 5,007 characters of text on the same page, so the tight one loses nothing.
_CONTENT_SELECTORS = ("div.ms-rtestate-field", '[id*="PlaceHolderMain"]',
                      "div.container")

#: Stripped before the markup is stored. The same list the engine's
#: JS_MAIN_CONTENT works from, minus the selectors that only mean something in a
#: live DOM. This matters more than it looks: the raw rich-text field on
#: /ar/Pages/InstructionsToBanks2024.aspx is 124,687 characters of markup around
#: 26,462 characters of text, nearly all of it inlined script and style, and
#: Excel's cell limit is 32,767.
_PAGE_JUNK = ("script, style, noscript, iframe, object, embed, form, button, "
              "nav, header, footer, .breadcrumb, .bread-crumb, .d-print-none, "
              "[aria-hidden='true'], [hidden]")

#: Attributes kept on stored page markup. Everything else -- `style` and `class`
#: above all -- is dropped.
#:
#: NOT tidiness. MEASURED on /ar/legislation/Pages/Instructions-To-Financing-
#: Companies.aspx, a 104,743-character rich-text field holding 5,011 characters
#: of text:
#:
#:     style   56,828 bytes      class   43,324 bytes      everything else ~4,600
#:
#: and a typical style reads
#: `margin:0px;padding:0px;border:0px;outline:0px;vertical-align:baseline;
#: background:transparent;` on element after element. It is a CSS reset pasted
#: through the editor, it says nothing about the instrument, and the site's own
#: stylesheet supplies it anyway. Keeping it put four of these six rows over
#: Excel's 32,767-character cell and into the sidecar for no gain at all.
#:
#: `dir` and `lang` are KEPT deliberately: five of the six pages are Arabic, and
#: without them the text renders left-to-right wherever it is displayed.
_KEEP_ATTRS = {"href", "src", "colspan", "rowspan", "dir", "lang", "title",
               "target", "alt"}


def _link_title(a, fallback_url: str) -> str:
    """A name for one file link on a QCB page.

    THE ANCHOR IS USUALLY NOT THE NAME. Measured across the six pages:

        Instructions To Banks        anchor IS the name  "الباب الأول: تعليمات ..."
        Exchange House family        anchor = "تحميل"     (Download)
        Combating financial crimes   anchor = ""          (empty)
        Exchange of tax information  anchor = "Download"

    so three of the four shapes carry the name in the ENCLOSING row or list
    item, with the download label sitting inside it. Subtracting the anchor's own
    text from its container handles every one of them, and does it without a
    language list -- `GENERIC_LINK_TEXT` already holds "تحميل" and "Download",
    but a strip-list can only ever cover the labels someone thought of.

    Order: the anchor when it is a real title, then the container minus the
    anchor, then the file's own name. The slug is last because these are
    `ss00.pdf`, `00.pdf`, `001.pdf` -- unique, and meaningless to a reader.
    """
    own = _clean(a.get_text(" ", strip=True))
    if own and _norm_link_text(own) not in GENERIC_LINK_TEXT \
            and not _BUTTON_TITLE.match(own):
        return clean_doc_title(own)[:200]

    # The innermost container that is not simply the whole table: a row or list
    # item holding at most a couple of links is this file's own line.
    node = a
    for _ in range(6):
        node = node.find_parent(["li", "tr", "p", "div"]) if node else None
        if node is None:
            break
        if len(node.find_all("a", href=True)) <= 2:
            ctx = _clean(node.get_text(" ", strip=True))
            if own:
                ctx = _clean(ctx.replace(own, " "))
            ctx = clean_doc_title(ctx)
            if len(ctx) > 3:
                return ctx[:200]

    leaf = urllib.parse.unquote((fallback_url or "").rsplit("/", 1)[-1])
    return _clean(re.sub(r"\.[A-Za-z0-9]{1,5}$", "", leaf)) or "Untitled"


def _file_type_for(url: str) -> str:
    """What the thing at `url` actually is.

    `_is_doc` knows pdf/doc/xls and the extensionless download endpoints, and
    anything it does not recognise used to fall through to "HTML" -- which put
    six .txt placeholders in the workbook labelled as web pages. Asking about the
    extension first keeps the column honest for the two cases this site has that
    `_is_doc` cannot name: a .txt attachment, and an .aspx page.
    """
    bare = (url or "").lower().rsplit("?", 1)[0]
    if _is_doc(url):
        return _ext_type(url)
    if bare.endswith(".txt"):
        return "TXT"
    return "HTML"


def _clean(s: Optional[str]) -> str:
    """Display form of a stored string: no zero-width spaces, no edge padding,
    no doubled internal spaces."""
    return re.sub(r"\s+", " ", (s or "").replace(_ZWSP, "")).strip()


def _desc_to_text(html: Optional[str]) -> str:
    """The reading text of a subcategory Description.

    The field is almost plain text: the only markup QCB puts in it is a line
    break, and it writes that as the malformed `</br>` rather than `<br/>`, so
    both spellings are turned into a newline BEFORE tags are stripped. Strip
    first and the law runs together into one paragraph.

    Paragraph breaks are kept because this is the text of an instrument --
    "Article 1 -" belongs on its own line. Only runs of blank lines are
    collapsed, never the newlines themselves.
    """
    t = re.sub(r"<\s*/?\s*br\s*/?\s*>", "\n", html or "", flags=re.IGNORECASE)
    t = re.sub(r"<[^>]+>", " ", t)
    t = (t.replace("&nbsp;", " ").replace("&amp;", "&")
          .replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
          .replace(_ZWSP, ""))
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r" ?\n ?", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


class QCBLegislationSource:
    """The Legislation tab: five sections, their subcategories and their files.

    Declared in `config/sources/qcb.yml` with `mode: custom`, so `build_source`
    imports and instantiates it with `init_kwargs`.
    """

    def __init__(
        self,
        source_system: str = "Legislation",
        regulator: str = REGULATOR,
        site_url: str = SITE_URL,
        lang: str = "EN",
        # Restrict the run to some tabs BY TITLE, for proving one section before
        # the whole thing. A capped run under-reports by design and must never be
        # promoted as if it were the section.
        only_tabs: Optional[Sequence[str]] = None,
        doc_path_prefix: Optional[Sequence[str]] = None,
        request_delay: float = 0.4,
        timeout: int = 90,
        seed_url: str = f"{SITE_URL}/en/legislation/Pages/LegislationNew.aspx",
        #: Read the page behind each repointed stub row and store its content.
        #: Six extra requests per run, and without them those six rows are a link
        #: and nothing else -- which is what the stub rows were before, one level
        #: further along. Set False to skip the fetches and keep the links.
        fetch_linked_pages: bool = True,
    ):
        if not source_system:
            raise ValueError("QCBLegislationSource needs a source_system — it is "
                             "the key the completeness gate scopes on, and it "
                             "cannot be inferred after the fact")
        self.source_system = source_system
        self.regulator = regulator
        self.site_url = site_url.rstrip("/")
        self.lang = (lang or "EN").upper()
        self.only_tabs = [_clean(t).lower() for t in (only_tabs or [])]
        # Overridable from the YAML so the tree can be tuned against the manual
        # library after the first export WITHOUT a code change.
        self.doc_path_prefix = list(doc_path_prefix or [regulator, source_system])
        self.request_delay = float(request_delay)
        self.timeout = int(timeout)
        self.seed_url = seed_url
        self.fetch_linked_pages = bool(fetch_linked_pages)
        self.last_result: dict = {}

        if self.doc_path_prefix[:1] != [regulator]:
            # tree_path() reads doc_path[0] as the regulator. Anything else and
            # the whole section files under a country that does not exist.
            raise ValueError(
                f"doc_path_prefix must start with the regulator {regulator!r}, "
                f"got {self.doc_path_prefix!r}")

    @property
    def source_systems(self) -> List[str]:
        """What this source writes under. Read by CompositeCrawler and by the
        completeness gate; a list because the contract allows several."""
        return [self.source_system]

    # ------------------------------------------------------------------ #
    #  the API                                                             #
    # ------------------------------------------------------------------ #

    def _get(self, url: str) -> dict:
        # The stored list names carry spaces and U+200B, and the $filter clauses
        # carry spaces and quotes. Encode the lot, but leave OData's own
        # punctuation alone or the query stops parsing.
        safe = ":/?&=$(),'*+"
        req = urllib.request.Request(
            urllib.parse.quote(url, safe=safe),
            headers={
                "User-Agent": "Mozilla/5.0",
                # Without this header SharePoint answers in Atom XML.
                "Accept": "application/json; odata=verbose",
            },
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=self.timeout, context=ctx) as r:
            return json.loads(r.read().decode("utf-8"))["d"]

    def _items(self, url: str) -> List[dict]:
        """Every row of a list query, following SharePoint's `__next` paging."""
        out: List[dict] = []
        seen_pages = 0
        while url and seen_pages < MAX_PAGES:
            d = self._get(url)
            out.extend(d.get("results") or [])
            url = d.get("__next") or ""
            seen_pages += 1
            if url and self.request_delay:
                time.sleep(self.request_delay)
        if url:
            logger.warning("QCB: %s paging stopped at MAX_PAGES=%d with more "
                           "rows waiting — the library grew past what this "
                           "guard expects", self.source_system, MAX_PAGES)
        return out

    def _list_url(self, list_name: str, query: str) -> str:
        # list_name is passed THROUGH, zero-width spaces and all: it is the
        # site's own key, and cleaning it produces a 404.
        return (f"{self.site_url}/_api/web/lists/getbytitle('{list_name}')"
                f"/items?{query}")

    # ------------------------------------------------------------------ #
    #  reading the three levels                                            #
    # ------------------------------------------------------------------ #

    def _tabs(self) -> List[dict]:
        rows = self._items(self._list_url(HEADER_LIST, "$orderby=SortData"))
        tabs = []
        for r in rows:
            title = _clean(r.get(f"Title{self.lang}") or r.get("Title"))
            if self.only_tabs and title.lower() not in self.only_tabs:
                continue
            sub_list, doc_list = r.get("ListNameSubCat"), r.get("DocName")
            if not (title and sub_list and doc_list):
                # A tab that names no lists cannot be read, and skipping it
                # quietly is how a whole section goes missing.
                logger.warning("QCB: tab %r (ID %s) names no lists "
                               "(ListNameSubCat=%r DocName=%r) — skipped",
                               title, r.get("ID"), sub_list, doc_list)
                continue
            tabs.append({"id": r.get("ID"), "title": title,
                         "sub_list": sub_list, "doc_list": doc_list})
        return tabs

    def _subcategories(self, tab: dict) -> Dict[int, dict]:
        """{subcategory ID: {trail, desc_text, desc_html}}.

        A SUBCATEGORY CAN CARRY THE INSTRUMENT'S OWN TEXT. The site draws it in
        the right-hand pane when you click the folder, and it is a `Description`
        field on the row. Measured 2026-09-17, English side: exactly one
        subcategory has real prose -- QCB Law, 1,533 characters of the Law No.
        (13) of 2012 Issuance Articles. Two more (Commercial Companies Law,
        Investment Funds Law) carry Arabic-only descriptions, which an EN run
        does not take.

        A DESCRIPTION THAT IS JUST THE FOLDER'S NAME IS NOT CONTENT. Combating
        Financial Crimes stores "Combating financial crimes" in the field. It is
        dropped by comparing the two, not by a length threshold -- a genuinely
        short description is still a description.

        A trail, not a name, because the subcategory list is TWO DEEP and says so.
        Every row carries `Level`, and Supervisory Instructions is the one tab
        that uses both values — measured 2026-09-17:

            L0  Instructions To Banks                              0 files
            L1    Governance Instructions for Banks                2
            L1    Instructions To Banks                            1
            L1    Instructions for Insurance Companies             3
            L0  Instructions to Other Financial Institutions       0 files
            L1    Instructions to Financing Companines             1
            L1    Instructions to investment companies             1
            L1    Instructions to Exchange House                   1
            L1    Instructions for Financial Technology Companies  5

        which is exactly the indentation the site draws. Laws, Licensing and
        Enforcement of Sanctions are all Level 0; Combating Financial Crimes
        leaves `Level` null, which reads the same as 0.

        A LEVEL-0 ROW WITH NO FILES IS A HEADING, NOT AN EMPTY FOLDER. Both of
        the ones above hold zero documents because their children hold them all —
        so they are not reported as missing content, and they appear in the tree
        only through their children, which is the same thing the site does.

        `$orderby=SortData` is load-bearing: the grouping is positional, a Level 1
        row belonging to the last Level 0 row above it. Out of order, every child
        attaches to the wrong heading. (SortData is a DECIMAL here — 3.5 slots a
        row between 3 and 4 — so it is compared as a float, never as a string.)
        """
        rows = self._items(self._list_url(tab["sub_list"], "$orderby=SortData"))

        def sort_key(r):
            try:
                return float(r.get("SortData") or 0)
            except (TypeError, ValueError):
                return 0.0

        out: Dict[int, dict] = {}
        heading: List[str] = []
        for r in sorted(rows, key=sort_key):
            t = _clean(r.get(f"Title{self.lang}") or r.get("Title"))
            if r.get("ID") is None or not t:
                continue
            try:
                level = int(r.get("Level") or 0)
            except (TypeError, ValueError):
                level = 0
            if level <= 0:
                heading = [t]
                trail = [t]
            else:
                trail = heading + [t]

            desc_html = r.get("Description" if self.lang == "EN"
                              else f"Description{self.lang}") or ""
            desc_text = _desc_to_text(desc_html)
            if _clean(desc_text).casefold() == t.casefold():
                desc_html = desc_text = ""     # the field holds the folder's name

            out[r["ID"]] = {"trail": trail, "desc_html": desc_html,
                            "desc_text": desc_text}
        return out

    def _files(self, tab: dict) -> List[dict]:
        # The page's own filter, verbatim: rows tagged for this language plus
        # rows tagged as belonging to both. Dropping it returns the Arabic
        # library as well, under English folder names.
        query = (f"$filter=(Lang eq 'Both' or Lang eq '{self.lang}')"
                 f"&$expand=File,FieldValuesAsText"
                 f"&$orderby=SortData desc,Id desc&$top={PAGE_SIZE}")
        return self._items(self._list_url(tab["doc_list"], query))

    # ------------------------------------------------------------------ #
    #  mapping                                                             #
    # ------------------------------------------------------------------ #

    def _title_for(self, row: dict, file_ref: str, trail: List[str],
                   siblings: int) -> str:
        """A name for the row: its own title, its folder's, or its file's.

        THE SITE WRITES BUTTON LABELS INTO THE TITLE FIELD. Four Laws rows are
        stored as "Download full PDF" — the words under the link, not the name of
        the instrument. `GENERIC_LINK_TEXT` in the engine is the library's list
        of those, reused here rather than restated, because it already carries
        the invisible-character handling that defeats an exact match (Ministry of
        Commerce stored a document called `click here<200b>`). It does not carry
        "download full pdf", so `_BUTTON_TITLE` widens it for the phrasings a set
        of exact strings cannot keep up with — and only inside this source, so no
        other regulator's titles move.

        WHEN THE TITLE IS A BUTTON, THE FOLDER IS THE NAME — but only where the
        folder holds ONE document. "Commercial law" has a single file called
        "Download full PDF", so the folder names it exactly. Combating Financial
        Crimes has 45, and naming one of them after the folder would make it
        indistinguishable from its 44 siblings; that one falls through to the
        filename, which is what an untitled row has always used.
        """
        t = _clean(row.get(f"Title{self.lang}") or row.get("Title"))
        if (t and _norm_link_text(t) not in GENERIC_LINK_TEXT
                and not _BUTTON_TITLE.match(t)):
            return t
        if siblings == 1 and trail:
            return trail[-1]
        leaf = urllib.parse.unquote((file_ref or "").rsplit("/", 1)[-1])
        return _clean(re.sub(r"\.[A-Za-z0-9]{1,5}$", "", leaf)) or t or "Untitled"

    def _to_regulatory(self, row: dict, tab: str, sub_row: dict,
                       siblings: int = 0) -> Optional[RegulatoryDocument]:
        trail = sub_row["trail"]
        fv = row.get("FieldValuesAsText") or {}
        file_ref = (fv.get("FileRef") or "").strip()
        if not file_ref:
            logger.warning("QCB: %s row ID %s has no FileRef — skipped",
                           " > ".join([tab] + trail), row.get("ID"))
            return None

        # FileRef is server-relative and mostly Arabic, so it needs quoting
        # before it is a URL anyone can fetch. `safe="/"` keeps the path
        # separators and encodes the rest.
        file_url = self.site_url + urllib.parse.quote(file_ref, safe="/()")
        title = self._title_for(row, file_ref, trail, siblings)

        # The page some rows really point at. Server-relative on this site;
        # absolutised so extra_meta holds something clickable.
        link = row.get(f"Link{'' if self.lang == 'EN' else self.lang}")
        if isinstance(link, dict):
            link = link.get("Url")
        link = (link or "").strip()
        if link.startswith("/"):
            link = self.site_url + link

        # THE .txt ATTACHMENTS ARE PLACEHOLDERS. THE LINKED PAGE IS THE DOCUMENT.
        #
        # MEASURED 2026-09-17, all six fetched:
        #
        #   row                                   the .txt            the page
        #   Instructions To Banks                 18 B "Instrution.."  47,092 ch
        #   Instructions to investment companies  18 B "Instrution.."   6,492
        #   Instructions to Exchange House        18 B "Instrution.."   7,112
        #   Instructions to Financing companies   36 B "Instructio.."   6,307
        #   Combating money laundering..          14 B "MondyLoundary"  1,486
        #   Exchange of tax information           24 B (mojibake)       5,005
        #
        # Not one is a document. They are uploads made to satisfy a required-file
        # field, misspelled, and THREE DIFFERENT INSTRUMENTS share the same 18
        # bytes. Storing them made the library hold "Instrution to bank" where
        # the whole of Instructions To Banks is 47,092 characters on the linked
        # page. CRAWLING_OVERVIEW's schema says document_url is "link to the
        # actual file/page" and its second guiding goal is "nothing silently
        # dropped"; the stub fails both.
        #
        # So where a .txt carries a Link, the LINK is the document. The stub is
        # kept in extra_meta rather than discarded, because it is what the
        # English page actually offers and that is worth being able to see.
        #
        # KNOWN AND ACCEPTED: five of the six pages are Arabic (/ar/...). QCB
        # publishes no English version of those instructions; only Exchange of
        # tax information links an English page. An Arabic page is the document;
        # an 18-byte typo is not.
        is_stub = file_ref.lower().endswith(".txt")
        if is_stub and link:
            url, placeholder = link, file_url
        else:
            url, placeholder = file_url, ""

        # THE FOLDER'S TEXT BECOMES THE DOCUMENT'S TEXT, BUT ONLY WHERE THE
        # FOLDER HOLDS ONE DOCUMENT.
        #
        # QCB Law is a subcategory with a single PDF and 1,533 characters of the
        # law's Issuance Articles in its Description -- the folder's prose IS
        # that document, and storing it as `document_html` is what puts the text
        # in front of a reader and in front of the analyser.
        #
        # With two or more files in the folder there is no answer to WHICH of
        # them the prose belongs to, so it is carried as
        # `extra_meta["section_description"]` on each and nothing is claimed. The
        # rule is re-evaluated every run: a folder that gains a second document
        # stops promoting its description, which is the honest outcome rather
        # than a stale claim.
        desc_html = sub_row.get("desc_html") or ""
        desc_text = sub_row.get("desc_text") or ""
        promote = bool(desc_text) and siblings == 1

        # TIER 2: THE PUBLISHER'S OWN CHANGE STAMP.
        #
        # ONBOARDING Step 2 ranks what to hash: the page's visible text first,
        # then "a publisher's own change stamp if one exists (MOH uses
        # SharePoint's `Modified`; it moves when a PDF is replaced, which
        # `url|title` cannot)". Every row in these lists carries `Modified`, and
        # every row here is a LINK to something this source never downloads -- so
        # tier 1 is unavailable and tier 3 would be blind to exactly the change
        # that matters: QCB replacing a PDF behind an unchanged link.
        #
        # Same construction as crawler/moh_crawler.py, but over the FINAL url
        # rather than the stored FileRef, so a row repointed at its linked page
        # also re-hashes if that link ever moves.
        #
        # Set HERE rather than left to `stamp_content_hashes`, which never
        # overwrites an existing hash. That is what lets the tab source keep
        # tier 1 from its own page text while these rows take tier 2.
        modified = (row.get("Modified") or "").strip()
        content_hash = (content_key(f"{url}|{modified}") if modified
                        else content_key(f"{url}|{title}"))

        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=tab,
            title=title,
            document_url=url,
            # There is no per-document page: the Legislation tab is one URL.
            source_page_url=self.seed_url,
            # DERIVED from what the row now points at, not assumed. A repointed
            # stub row is an HTML page and says so; a .txt with no link to
            # repoint to is a text file and says THAT, rather than falling
            # through to "HTML" because `_is_doc` does not know the extension.
            file_type=_file_type_for(url),
            content_hash=content_hash,
            # Deliberately NOT `Year` and NOT `Modified`. See the module header.
            published_date=None,
            doc_path=self.doc_path_prefix + [tab] + trail + [title],
            document_html=desc_html if promote else None,
            extra_meta={
                "record_kind": "document",
                # Always carried, promoted or not, so the folder's text is in the
                # workbook even where this source will not claim it is one
                # document's.
                "section_description": desc_text,
                "section_description_promoted": promote,
                **({"content_text": desc_text} if promote else {}),
                "section_path": " > ".join([tab] + trail),
                "list_item_id": row.get("ID"),
                "list_name": tab,
                "legislation_category_id": row.get("LegislationCacegorieId"),
                "lang": row.get("Lang"),
                # The site's year FILTER value, kept because it is what the page
                # searches on — not a publication date.
                "filter_year": row.get("Year"),
                "cms_modified": row.get("Modified"),
                "title_ar": _clean(row.get("TitleAR")),
                # Non-empty on the .txt stub rows; see the module header.
                "linked_page": link,
                # The placeholder this row USED to point at, carried where a stub
                # was replaced by its page so the swap is visible rather than
                # silent. Empty on every other row.
                "placeholder_file": placeholder,
            },
        )

    # ------------------------------------------------------------------ #
    #  the page behind a repointed row                                     #
    # ------------------------------------------------------------------ #

    def _files_on_page(self, pane, page_doc: RegulatoryDocument,
                       out: List[RegulatoryDocument]) -> int:
        """Every file linked from a captured page, as its own row beside it.

        WHY ROWS AND NOT AN ATTACHMENT LIST. The model supports both, and the
        library has already chosen: of 7,577 stored rows across every regulator,
        51 use the multi-file shape and the largest of those carries 10 files.
        CBE -- the closest analogue, a central bank on the generic engine -- has
        654 rows, none multi-file, and 36 folders holding a page row beside its
        individual PDFs. That is the shape reproduced here.

        The alternative would put 162 files behind one identity, and
        `models.RegulatoryDocument` states the cost: "if a card gains or loses a
        PDF its identity changes, so it reads as one `new` plus one
        `disappeared` rather than `modified`". QCB reissues Instructions to Banks
        annually.

        FLAT, BESIDE THE PAGE, NOT UNDER IT. `page_doc.doc_path[:-1]` is the
        folder the page row sits in, so a file becomes its sibling: the CBE
        shape, and the one that needs nothing inferred. The site marks no volume
        boundaries -- "Volume One" appears zero times on the Instructions to
        Banks page, and `الجزء الأول` zero times -- so the Volume One / Volume
        Two grouping in the manual library is the library's own and cannot be
        read off the page.

        MEASURED, distinct files per page: Banks 162 (from 529 links -- each part
        is linked more than once), Exchange House 70, investment 55, Financing
        53, AML 9, FATCA 1. 350 in all.

        DEDUPED ON (url, folder), AND THE FIRST TITLE WINS.

        One PDF here covers a whole section, and the page lists that section's
        sub-headings underneath it, EACH LINKING THE SAME FILE. Measured on
        Instructions to Banks: 529 links, 350 distinct files across the six
        pages, and one file carried 51 different labels.

            0026-0039.pdf   "أولاً: منظومة كفاية رأس المال"        <- the section
                            "1. نسبة كفاية رأس المال وفقاً ..."     <- its contents
                            "أ- للبنوك التجارية الوطنية التقليدية"
                            ... 11 more

        The file is the document; the rest are its table of contents. Taking the
        FIRST label in document order takes the section heading every time --
        checked on four files whose labels run to 14, 20, 8 and 7 deep -- because
        the page prints the heading before the contents it introduces.

        The others are not thrown away. They go to
        `extra_meta["also_listed_as"]`, which is what a reader searching for
        "نسبة الرافعة المالية" needs to find the file that contains it.
        """
        folder = list(page_doc.doc_path[:-1])
        meta = page_doc.extra_meta or {}
        made = 0

        # Pass 1: url -> every label the page gives it, in document order.
        labels: "collections.OrderedDict[str, List[str]]" = collections.OrderedDict()
        for a in pane.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href or href.startswith(("#", "javascript:", "mailto:")):
                continue
            url = (self.site_url + urllib.parse.quote(urllib.parse.unquote(href),
                                                      safe="/()%:?&=")
                   if href.startswith("/") else href)
            if not _is_doc(url):
                continue
            title = _link_title(a, url)
            seen = labels.setdefault(url, [])
            if title not in seen:
                seen.append(title)

        # Pass 2: one row per file.
        for url, titles in labels.items():
            title = titles[0]
            doc_path = folder + [title]
            key = (url, tuple(folder))
            if key in self._seen_files:
                continue
            self._seen_files.add(key)
            out.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=page_doc.category,
                title=title,
                document_url=url,
                # The page it is published on -- which is the row right beside
                # this one, not the Legislation tab.
                source_page_url=page_doc.document_url,
                file_type=_file_type_for(url),
                published_date=None,
                doc_path=doc_path,
                extra_meta={
                    "record_kind": "document",
                    "section_path": meta.get("section_path", ""),
                    "found_on_page": page_doc.document_url,
                    "page_lang": meta.get("page_lang", ""),
                    # Every OTHER label the page gives this file -- its table of
                    # contents, in the page's own words. Empty for a file listed
                    # once, which is most of them.
                    "also_listed_as": " | ".join(titles[1:]),
                },
                # Tier 3, and honestly so: a file this source never downloads,
                # published on a page that carries no per-file change stamp.
                content_hash=content_key(f"{url}|{title}"),
            ))
            made += 1
        return made

    def _read_linked_pages(self, docs: List[RegulatoryDocument]) -> int:
        """Store the content of the page each repointed stub row now points at.

        WITHOUT THIS THE FIX IS HALF DONE. Repointing moved those six rows off an
        18-byte placeholder and onto the page that holds the instrument, but a
        row that is a bare link is what they already were -- the library still
        would not hold a word of "Instructions To Banks", which is 26,462
        characters of text.

        MEASURED on all six, raw rich-text field -> stored markup (see
        `_KEEP_ATTRS` for where the difference goes):

            Instructions to investment companies  125,832 -> see the run log
            Instructions to Exchange House        146,356
            Instructions to Financing companies   104,743
            Instructions To Banks                 124,687
            Combating money laundering..           17,640
            Exchange of tax information             6,504

        Nearly all of the shrink is `style` and `class` boilerplate, with a
        little inlined script; SharePoint puts all of it INSIDE the content
        field.

        THE HASH MOVES UP A TIER. These rows now hold the page's visible text, so
        ONBOARDING Step 2's first preference applies and `content_hash` is set
        from the text rather than from `url|Modified` -- the list item's stamp
        does not move when the linked page's own content is edited.

        A page that cannot be read leaves the row exactly as it was: a link, no
        text, and a warning. Losing the document would be worse than losing its
        body.
        """
        from bs4 import BeautifulSoup

        filled = 0
        harvested = 0
        out: List[RegulatoryDocument] = []
        self._seen_files: set = set()
        failed: List[dict] = []
        for doc in docs:
            meta = doc.extra_meta or {}
            if not meta.get("placeholder_file"):
                continue
            url = doc.document_url
            raw = ""
            for attempt in range(1, PAGE_ATTEMPTS + 1):
                try:
                    req = urllib.request.Request(
                        url, headers={"User-Agent": "Mozilla/5.0"})
                    ctx = ssl.create_default_context()
                    with urllib.request.urlopen(
                            req, timeout=self.timeout, context=ctx) as r:
                        raw = r.read().decode("utf-8", "replace")
                    break
                except Exception as e:
                    if attempt < PAGE_ATTEMPTS:
                        logger.info("QCB: %s refused (%s), retry %d of %d",
                                    url, type(e).__name__, attempt,
                                    PAGE_ATTEMPTS - 1)
                        time.sleep(attempt)
                    else:
                        logger.error(
                            "QCB: could not read %s for %r after %d attempts "
                            "(%s). The row keeps its link and stays WITHOUT "
                            "text, and its hash falls back a tier -- so the "
                            "next run that does read it will report this "
                            "document as modified when nothing changed.",
                            url, doc.title, PAGE_ATTEMPTS, type(e).__name__)
                        failed.append({"title": doc.title, "url": url,
                                       "error": type(e).__name__})
            if not raw:
                continue

            soup = BeautifulSoup(raw, "html.parser")
            pane = None
            for sel in _CONTENT_SELECTORS:
                found = soup.select(sel)
                if found:
                    pane = max(found,
                               key=lambda e: len(e.get_text(" ", strip=True)))
                    break
            if pane is None:
                logger.warning("QCB: %s has none of %s -- no content stored for "
                               "%r", url, _CONTENT_SELECTORS, doc.title)
                continue

            for junk in pane.select(_PAGE_JUNK):
                junk.decompose()
            for tag in pane.find_all(True):
                for attr in [a for a in tag.attrs if a not in _KEEP_ATTRS]:
                    del tag[attr]
            text = _clean(pane.get_text(" ", strip=True))
            if not text:
                logger.warning("QCB: %s read empty for %r", url, doc.title)
                continue

            doc.document_html = str(pane)
            meta["content_text"] = text
            harvested += self._files_on_page(pane, doc, out)
            # The page's own language, which is the honest label for five of
            # these six: QCB publishes no English version.
            html_tag = soup.find("html")
            if html_tag is not None:
                meta["page_lang"] = (html_tag.get("lang") or "").strip()
                meta["page_dir"] = (html_tag.get("dir") or "").strip()
            doc.extra_meta = meta
            # Tier 1 now that the text is in hand. See the docstring.
            doc.content_hash = content_key(text)
            filled += 1
            if self.request_delay:
                time.sleep(self.request_delay)
        self._linked_failures = failed
        self._harvested = out
        if out:
            logger.info("QCB Legislation — %d file(s) harvested from %d page(s), "
                        "filed beside them", harvested, filled)
        return filled

    # ------------------------------------------------------------------ #
    #  the single public exit                                              #
    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        docs: List[RegulatoryDocument] = []
        per_tab: Dict[str, int] = {}
        orphans: List[dict] = []
        empty_folders: List[str] = []

        for tab in self._tabs():
            subs = self._subcategories(tab)
            rows = self._files(tab)
            used = set()
            before = len(docs)

            # HOW MANY FILES SHARE A SUBCATEGORY, counted before any row is
            # named: `_title_for` falls back to the folder's name only where the
            # folder holds one document, and it cannot know that mid-loop.
            per_cat = collections.Counter(
                r.get("LegislationCacegorieId") for r in rows)

            for row in rows:
                cat_id = row.get("LegislationCacegorieId")
                sub = subs.get(cat_id)
                if sub is None:
                    # A FILE THE SITE ITSELF CANNOT REACH IS NOT IN THE LIBRARY.
                    #
                    # Its LegislationCacegorieId names no row in the subcategory
                    # list, so QCB's own sidebar has no entry that would ever
                    # display it. Three Laws files are in this state (76, 77, 78),
                    # all titled "Download full PDF".
                    #
                    # An earlier version filed them under "Uncategorised
                    # (category NN)" to keep them visible. That put documents in
                    # the library that are not published on the site, under a
                    # folder name QCB has never used — inventing a section is a
                    # worse error than omitting a file, because the omission is
                    # recoverable and the invention gets read as real.
                    #
                    # They are NOT silent: every one is logged with its ID, and
                    # `last_result["skipped_uncategorised"]` carries the list. If
                    # QCB adds the missing subcategory rows they appear by
                    # themselves on the next run, with no change here.
                    orphans.append({
                        "category_id": cat_id,
                        "tab": tab["title"],
                        "item_id": row.get("ID"),
                        "file": (row.get("FieldValuesAsText") or {}).get("FileRef"),
                    })
                    continue
                used.add(cat_id)
                doc = self._to_regulatory(row, tab["title"], sub,
                                          per_cat[cat_id])
                if doc is not None:
                    docs.append(doc)

            per_tab[tab["title"]] = len(docs) - before

            # A HEADING IS NOT AN EMPTY FOLDER. A Level-0 row that other rows are
            # grouped under holds no files by design — "Instructions To Banks"
            # and "Instructions to Other Financial Institutions" are the two —
            # and reporting those as missing content would cry wolf on every run
            # for ever. `_subcategories` gives a heading the trail [itself], and
            # its children the trail [heading, child], so a heading is exactly a
            # 1-long trail that is the PREFIX of some other trail.
            headings = {tuple(v["trail"][:-1]) for v in subs.values()
                        if len(v["trail"]) > 1}
            for cat_id, v in subs.items():
                trail = v["trail"]
                if cat_id in used or tuple(trail) in headings:
                    continue
                # A real subcategory the site shows with nothing under it. This
                # source creates folders from DOCUMENTS, so an empty one simply
                # does not appear in the tree, and this line is the only way
                # anybody learns it was there.
                empty_folders.append(f"{tab['title']} > {' > '.join(trail)}")

            logger.info("QCB Legislation — %-28s %3d file(s) in %d of %d "
                        "subcategories (%d heading(s))", tab["title"],
                        per_tab[tab["title"]], len(used),
                        len(subs) - len(headings), len(headings))

        for o in orphans:
            logger.warning(
                "QCB Legislation — SKIPPED %s item %s: category %s names no "
                "subcategory row, so the site cannot display it either (%s)",
                o["tab"], o["item_id"], o["category_id"],
                urllib.parse.unquote(str(o["file"] or ""))[:90])
        for f in empty_folders:
            logger.warning("QCB Legislation — subcategory with no %s document: %s",
                           self.lang, f)

        # A section that returns nothing is a FINDING, not a result. Every other
        # crawler in the library says this; the one that did not is the reason a
        # scheduled job could produce 0 rows without anyone noticing.
        if not docs:
            raise RuntimeError(
                f"QCB {self.source_system} returned no documents from "
                f"{self.site_url}/_api/web/lists/getbytitle('{HEADER_LIST}'). "
                f"That is a failed read, not an empty section.")

        filled = self._read_linked_pages(docs) if self.fetch_linked_pages else 0
        if filled:
            logger.info("QCB Legislation — read %d linked page(s) into the rows "
                        "that used to point at a placeholder", filled)
        docs.extend(getattr(self, "_harvested", []))

        if isinstance(limit, int) and limit > 0:
            docs = docs[:limit]

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": []},
            "by_source": {self.source_system: len(docs)},
            "linked_pages_read": filled,
            "linked_pages_failed": getattr(self, "_linked_failures", []),
            "files_harvested": len(getattr(self, "_harvested", [])),
            "by_tab": per_tab,
            "skipped_uncategorised": orphans,
            "empty_subcategories": empty_folders,
        }

        # The single exit. Nothing above sets content_hash — these rows are links
        # to files this source never downloads — so every one is stamped here.
        return stamp_content_hashes(docs)




class QCBTabbedPageSource:
    """One QCB page whose tabs are separate documents.

    WHY THIS IS NOT `mode: generic`
    -------------------------------
    /en/Pages/MonetaryPolicyTools.aspx is three instruments at one URL: Monetary
    Policy Goals, Monetary Policy Tools and Exchange Rate Policy, each a Bootstrap
    tab pane. Nothing about it changes the URL, so a link-walker sees one page and
    stores one row holding all three tabs' text -- which is what the first export
    did, 12,078 characters under a single title.

    The engine can pick ONE container per host (`content_selector`), not one per
    source, so three sources pointed at the same URL would each capture the whole
    page. Splitting has to happen where the panes are known, which is here.

    MEASURED 2026-09-17, one request, no browser -- the page is server-rendered
    and every pane is in the DOM at once:

        Monetary Policy Goals   #nav-plans         2,283 html   1,924 text
        Monetary Policy Tools   #nav-investment    1,914 html   1,219 text
        Exchange Rate Policy    #nav-exhangerate  25,961 html   2,169 text

    All three fit an Excel cell, so this also retires the `qcb.fulltext.json`
    sidecar the single-row version needed.

    THE BUTTON NAMES THE PANE. `data-bs-target="#nav-plans"` on the tab button is
    the only thing tying a label to its content -- the pane ids are the site's
    own shorthand ("nav-plans" for Goals, "nav-investment" for Tools, and
    "nav-exhangerate", their spelling) and mean nothing on their own. Reading the
    button is what keeps the titles honest if QCB reorders the tabs.

    THE URL IS `<page>#<pane id>`. A pane has no url of its own, and this is the
    form the engine already uses for the same problem: `keep_modals` rewrites a
    dead `javascript:void(0)` trigger to `#<id>` so the page reads as an index
    over its own children. Identity does not depend on it -- each pane has its
    own doc_path -- but a row should still point at something a person can open.
    """

    def __init__(
        self,
        seed_url: str,
        source_system: str,
        regulator: str = REGULATOR,
        site_url: str = SITE_URL,
        doc_path_prefix: Optional[Sequence[str]] = None,
        #: A pane holding less than this much text is reported and skipped. A tab
        #: that renders empty is a finding, not a document -- but the threshold is
        #: deliberately low, because "Required Reserve" is a real subsection in
        #: 1,219 characters and a generous cut would start eating content.
        min_text: int = 80,
        timeout: int = 90,
    ):
        if not seed_url:
            raise ValueError("QCBTabbedPageSource needs a seed_url")
        if not source_system:
            raise ValueError("QCBTabbedPageSource needs a source_system -- it is "
                             "the key the completeness gate scopes on")
        self.seed_url = seed_url
        self.source_system = source_system
        self.regulator = regulator
        self.site_url = site_url.rstrip("/")
        self.doc_path_prefix = list(doc_path_prefix or [regulator, source_system])
        self.min_text = int(min_text)
        self.timeout = int(timeout)
        self.last_result: dict = {}

        if self.doc_path_prefix[:1] != [regulator]:
            raise ValueError(
                f"doc_path_prefix must start with the regulator {regulator!r}, "
                f"got {self.doc_path_prefix!r}")

    @property
    def source_systems(self) -> List[str]:
        return [self.source_system]

    def _fetch(self) -> str:
        req = urllib.request.Request(
            self.seed_url, headers={"User-Agent": "Mozilla/5.0"})
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=self.timeout, context=ctx) as r:
            return r.read().decode("utf-8", "replace")

    @staticmethod
    def _breadcrumb(soup) -> List[str]:
        """The trail the page shows, as the site shows it.

        Stored raw in `section_path`, "Home" included. This page's template
        serves it in English already -- the /en/Pages/ seeds on the generic
        engine serve Arabic and localise on the client, which is why those are
        read through a browser and this one need not be.
        """
        ol = soup.select_one("ol.breadcrumb, nav[aria-label='breadcrumb'] ol")
        if not ol:
            return []
        return [t for t in (_clean(li.get_text(" ", strip=True))
                            for li in ol.select("li")) if t]

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(self._fetch(), "html.parser")
        crumbs = self._breadcrumb(soup)

        docs: List[RegulatoryDocument] = []
        empty: List[str] = []
        for btn in soup.select('[data-bs-toggle="tab"][data-bs-target^="#"]'):
            pane_id = (btn.get("data-bs-target") or "")[1:]
            label = _clean(btn.get_text(" ", strip=True))
            pane = soup.find(id=pane_id) if pane_id else None
            if not label or pane is None:
                logger.warning("QCB %s: tab %r targets %r, which is not on the "
                               "page -- skipped", self.source_system, label,
                               pane_id)
                continue
            for junk in pane.select("script, style"):
                junk.decompose()
            text = pane.get_text(" ", strip=True).replace(_ZWSP, "").strip()
            if len(text) < self.min_text:
                empty.append(f"{label} (#{pane_id}, {len(text)} chars)")
                continue

            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.source_system,
                title=label,
                document_url=f"{self.seed_url}#{pane_id}",
                source_page_url=self.seed_url,
                file_type="HTML",
                published_date=None,
                doc_path=self.doc_path_prefix + [label],
                document_html=str(pane),
                extra_meta={
                    "record_kind": "page",
                    # The site's own trail plus this tab, stored raw. doc_path is
                    # the library's tree; this is the site's.
                    "section_path": " > ".join(crumbs + [label]),
                    "tab_id": pane_id,
                    "content_text": text,
                },
            ))

        for e in empty:
            logger.warning("QCB %s: tab rendered no content -- %s",
                           self.source_system, e)
        if not docs:
            raise RuntimeError(
                f"QCB {self.source_system}: no tab panes with content at "
                f"{self.seed_url}. That is a failed read, not an empty page.")

        logger.info("QCB %s -- %d tab(s): %s", self.source_system, len(docs),
                    ", ".join(d.title for d in docs))
        if limit and limit > 0:
            docs = docs[:limit]

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": []},
            "by_source": {self.source_system: len(docs)},
            "empty_tabs": empty,
        }
        return stamp_content_hashes(docs)


__all__ = ["QCBLegislationSource", "QCBTabbedPageSource",
           "REGULATOR", "SITE_URL"]
