# -*- coding: utf-8 -*-
"""NCUA page crawler: the generic engine, with ncua.gov's furniture taken out.

WHY THIS EXISTS. The generic engine already captures the right region of
ncua.gov's UDAAP page (one page, 0 files, status ok -- output/ncua/pages.json,
2026-10-19 09:09), so nothing here runs in the browser. What it stores carries
Drupal furniture the engine has no profile key for, and every item below was
counted on that stored html, not on a static fetch:

  * div.lr-label -- 290 of them, one per checklist <td>, each repeating its
    column header ("Item", "Description", "YES", "NO", "N/A"). The site's CSS
    shows them only on narrow screens; in the stored body every cell reads
    "Item 1", "YES ..." Checked against <thead> on all 7 tables: 0 mismatches,
    so the header row already says what they say.
  * .visually-hidden -- 9: the screen-reader notice inside each of 4 external
    links ("This is an external link to a website belonging to another federal
    agency..." plus "(Opens new window)"), and an h2 "Footnotes" heading an
    EMPTY #publication_footnotes block. The real footnotes are inline in the
    body and are kept.
  * .noPrint -- a second copy of the title (div.pseudo-title) and an empty
    div.docket-date.
  * "Last modified on 06/18/26" -- NOT removed: kept visible at the end of the
    body as one <p>, and also recorded as extra_meta["ncua_last_modified"].
  * "Back to top" (#main-content) and its empty target anchor.

The breadcrumb also ends in a false crumb, "(opens new window)" -- the same
screen-reader text, read by the engine's breadcrumb JS. It is dropped from the
recorded section_path.

HOW IT WORKS. Copied from crawler/fdic_crawler.py (FDICSectionCrawler's
`_clean_pages`), not imported -- see copy-over-refactor-across-regulators:

    super()._run_crawl()        the engine, untouched
      -> _clean_pages()         furniture out of the stored html; text and
                                content_hash re-derived from what is left
    (inherited) _doc_path, _doc_from_*, dedupe, identity -- all unchanged

Parsed with lxml, not html.parser (docs/onboarding_playbook.md §4).
"""
from __future__ import annotations

import logging
import re
from typing import Optional

from bs4 import BeautifulSoup

from crawler.fingerprint import stamp_content_hashes
from crawler.generic_crawler_wrapper import GenericSiteCrawler
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

_WS = re.compile(r"\s+")

#: Removed from the STORED html, each measured on output/ncua/pages.json.
FURNITURE_SELECTORS = (
    # Responsive-table labels: one per <td>, a copy of the column header.
    "div.lr-label, "
    # Screen-reader text: the external-link notice and "(Opens new window)"
    # inside <a>, and the hidden h2 over the empty footnotes block.
    ".visually-hidden, "
    # The second copy of the title, and the empty docket-date row.
    ".noPrint, "
    # Drupal's status-message placeholder, always empty here.
    "div[data-drupal-messages-fallback]"
)

_LAST_MODIFIED = re.compile(r"^\s*last modified", re.I)
_BACKTOTOP = re.compile(r"^\s*back to top\s*$", re.I)
#: The false last crumb the breadcrumb JS reads off a screen-reader span.
_SR_CRUMB = re.compile(r"^\(?\s*opens? (in )?(a )?new window\s*\)?$", re.I)
#: Tags that make a container worth keeping even with no text in it.
_KEEP_EMPTY = ["a", "img", "table", "hr", "br", "time", "iframe"]


def _visible_text(soup: BeautifulSoup) -> str:
    """Approximate the browser's innerText for a cleaned fragment.

    Not byte-identical to the engine's innerText, so a page's first hash under
    this class differs from the engine's. It is self-consistent from then on,
    which is what change detection needs.
    """
    for bad in soup(["script", "style", "noscript"]):
        bad.decompose()
    return _WS.sub(" ", soup.get_text(" ", strip=True)).strip()


class NCUAPageCrawler(GenericSiteCrawler):
    """GenericSiteCrawler plus ncua.gov's furniture rules. Same init_kwargs."""

    # ------------------------------------------------------------------ #
    #  furniture                                                          #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _tidy_empties(soup: BeautifulSoup) -> None:
        """Drop containers the removals left with nothing in them.

        Innermost first, so a wrapper whose only child was an emptied block
        goes too. An element holding an anchor is kept: the footnote targets
        (`<a id="footnote5"></a>`) are empty by design.
        """
        for el in reversed(soup.find_all(["div", "span", "p", "section"])):
            if el.find(_KEEP_EMPTY):
                continue
            if not el.get_text(strip=True):
                el.decompose()

    def _clean_html(self, html: str) -> tuple:
        """Return (cleaned html, last-modified ISO date or None)."""
        if not html:
            return html, None
        soup = BeautifulSoup(html, "lxml")
        body = soup.body or soup

        # "Last modified on <time>": KEPT VISIBLE, as the site shows it under
        # the footnotes (the user's call, 2026-10-19), but as one line. Drupal
        # stores it as a label span and a separate div around the <time>, which
        # renders as two lines; it is rebuilt as a single <p>, appended at the
        # end of the body, where the site draws it. The date is also recorded on
        # the row (ncua_last_modified). Found by its label text, not by Drupal's
        # block id, which a theme change moves.
        modified = None
        stamp = None
        for lab in body.find_all(string=_LAST_MODIFIED):
            block = lab.find_parent("div") or lab.parent
            t = block.find("time") if block is not None else None
            if t is None:
                continue
            modified = (t.get("datetime") or "")[:10] or t.get_text(strip=True)
            stamp = soup.new_tag("p", attrs={"class": "last-modified"})
            stamp.append(_WS.sub(" ", str(lab)).strip() + " ")
            new_t = soup.new_tag("time")
            if t.get("datetime"):
                new_t["datetime"] = t["datetime"]
            new_t.string = t.get_text(strip=True)
            stamp.append(new_t)
            block.decompose()
            break

        for node in body.select(FURNITURE_SELECTORS):
            node.decompose()

        # Back to Top, and the empty #main-content anchor it jumps to.
        for a in list(body.find_all("a")):
            href = (a.get("href") or "").strip()
            if _BACKTOTOP.match(a.get_text(" ", strip=True)) or \
                    (a.get("id") == "main-content" and not a.get_text(strip=True)):
                a.decompose()
            elif href == "#main-content":
                a.decompose()

        self._tidy_empties(body)
        if stamp is not None:
            body.append(stamp)
        return "".join(str(c) for c in body.contents).strip(), modified

    def _clean_pages(self, result: dict) -> None:
        """Clean every page's html, then re-derive the text and the hash from it.

        As in fdic_crawler.py: a crawled page's content_hash is content_key(text),
        so cleaning the html without re-deriving the text would leave the hash
        over furniture the stored page no longer shows.
        """
        n = chars = 0
        for r in result.get("pages", []) or []:
            crumbs = [c for c in (r.get("breadcrumb") or [])
                      if not _SR_CRUMB.match((c or "").strip())]
            if crumbs != list(r.get("breadcrumb") or []):
                r["breadcrumb"] = crumbs
                r["section_path"] = " > ".join(crumbs)
            html = r.get("html") or ""
            if not html:
                continue
            cleaned, modified = self._clean_html(html)
            if modified:
                r["ncua_last_modified"] = modified
            if cleaned == html:
                continue
            n += 1
            chars += len(html) - len(cleaned)
            r["html"] = cleaned
            text = _visible_text(BeautifulSoup(cleaned, "lxml"))
            r["text"] = text
            r["text_len"] = len(text)
            r["content_hash"] = content_key(text)
        logger.info("NCUA furniture: %d page(s) cleaned, %d chars of markup out",
                    n, chars)

    # ------------------------------------------------------------------ #
    def _run_crawl(self) -> dict:
        result = super()._run_crawl()
        self._clean_pages(result)
        return result

    def _doc_from_page_row(self, r: dict, shape: str):
        doc = super()._doc_from_page_row(r, shape)
        if doc is not None and isinstance(getattr(doc, "extra_meta", None), dict):
            doc.extra_meta["content_hash_basis"] = "visible-text-minus-furniture"
            # The site's own trail, recorded whether or not drop_sections is
            # set (the wrapper only writes it when it is).
            doc.extra_meta["section_path"] = r.get("section_path") or ""
            if r.get("ncua_last_modified"):
                doc.extra_meta["ncua_last_modified"] = r["ncua_last_modified"]
        return doc

    def fetch_documents(self, limit: Optional[int] = None):
        # Stamp at the single exit (ONBOARDING Step 2). Engine hashes are never
        # overwritten; this only fills a row that arrived without one.
        return stamp_content_hashes(super().fetch_documents(limit=limit))
