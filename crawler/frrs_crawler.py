# -*- coding: utf-8 -*-
"""FRRS page crawler: one federalreserve.gov page, one row.

Two page types, chosen by `body_selector` / `title_selector`:
  * FRRS "Background and Summary" pages (the default, div.dita-data): ~185,000
    of each page's characters are the FRRS-wide sidebar table of contents, so
    the page is read with a plain GET rather than the generic engine, which
    picks its own content region.
  * Compliance Guides to Small Entities (/supervisionreg/reg*cg.htm,
    div#article, title from its h4).

Stored body: the selected element, with comments and empty containers
removed, except elements a link on the page points at (in-page index and
footnote targets). FRRS draws heading levels with CSS on <h1>s, so
h1.titl1 -> <h2> and h1.titl3 -> <h3>; text untouched. A page's "Last Update"
stamp (#lastUpdate, compliance guides) is kept visible at the end of the body
and is the published_date.

doc_path: regulator > source_system > page title. section_path is the site's
breadcrumb; its second crumb is the category unless one is declared. Hash:
visible text.

Helpers copied from crawler/cfpb_crawler.py, not imported.
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Comment

from crawler.fingerprint import stamp_content_hashes
from crawler.generic_crawler_wrapper import GenericSiteCrawler
from generic_crawler.crawler import absolutize_html, content_key

logger = logging.getLogger(__name__)

PARSER = "lxml"
_WS = re.compile(r"\s+")

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")

_BODY = "div.dita-data"
_CRUMB = "ol.breadcrumb li.breadcrumb__item"
_LAST_UPDATE = "#lastUpdate"
_RELEVEL = (("titl1", "h2"), ("titl3", "h3"))
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


class FRRSPageCrawler(GenericSiteCrawler):
    """One FRRS page; seed_url is the page."""

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        category: Optional[str] = None,
        #: Declared: the page cites several CFR parts.
        reference_no: Optional[str] = None,
        body_selector: str = _BODY,
        #: None: the body's first <h1> left after re-levelling.
        title_selector: Optional[str] = None,
        timeout: float = 60.0,
        **generic_kwargs,
    ):
        super().__init__(seed_url=seed_url, regulator=regulator,
                         source_system=source_system, category=category,
                         **generic_kwargs)
        self.reference_no = reference_no
        self.body_selector = body_selector
        self.title_selector = title_selector
        self.timeout = timeout
        self._warnings: List[str] = []

    # ------------------------------------------------------------------ #
    @staticmethod
    def _crumbs(soup) -> List[str]:
        return [c for c in (_text(li) for li in soup.select(_CRUMB)) if c]

    def _body(self, soup, url: str) -> Tuple[str, str, str, str]:
        """(title, cleaned body html, visible text, last-update ISO date)."""
        block = soup.select_one(self.body_selector)
        if block is None:
            return "", "", "", ""
        for c in block.find_all(string=lambda s: isinstance(s, Comment)):
            c.extract()
        for el in block.find_all(["script", "style", "noscript", "button",
                                  "form", "iframe"]):
            el.decompose()
        h1 = None
        for h in block.find_all("h1"):
            cls = h.get("class") or []
            for marker, tag in _RELEVEL:
                if marker in cls:
                    h.name = tag
                    break
            else:
                if h1 is None:
                    h1 = h
        # Empty link targets are anchors, not clutter.
        targets = {a["href"][1:] for a in block.find_all("a", href=True)
                   if a["href"].startswith("#")}
        for el in reversed(block.find_all(["div", "span", "p", "section"])):
            if el is block or el.find(_KEEP_EMPTY):
                continue
            if el.get("id") in targets or el.get("name") in targets:
                continue
            if not el.get_text(strip=True):
                el.decompose()
        title = (_text(soup.select_one(self.title_selector))
                 if self.title_selector else _text(h1))
        # "Last Update: March 01, 2017" sits outside the article; kept visible
        # as one line at its end, so a new date moves the hash.
        updated = ""
        stamp = soup.select_one(_LAST_UPDATE)
        if stamp is not None:
            line = _text(stamp)
            p = soup.new_tag("p", attrs={"class": "last-update"})
            p.string = line
            block.append(p)
            try:
                updated = datetime.strptime(
                    line.split(":", 1)[-1].strip(), "%B %d, %Y"
                ).date().isoformat()
            except ValueError:
                self._warnings.append("unreadable %r on %s" % (line, url))
        inner = "".join(str(c) for c in block.contents).strip()
        frag = absolutize_html(inner, url)
        return title, frag, _visible_text(frag), updated

    # ------------------------------------------------------------------ #
    def _run_crawl(self) -> dict:
        t0 = time.time()
        r = requests.get(self.seed_url, headers={"User-Agent": _UA},
                         timeout=self.timeout)
        if r.status_code != 200:
            raise RuntimeError(
                "FRRS page answered HTTP %d (%s) -- refusing to report an "
                "empty source. Do not retry in a loop." % (r.status_code,
                                                           self.seed_url))
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, PARSER)
        crumbs = self._crumbs(soup)
        if not crumbs:
            self._warnings.append("no breadcrumb (%s) on %s"
                                  % (_CRUMB, self.seed_url))
        title, frag, text, updated = self._body(soup, r.url)
        if not text:
            raise RuntimeError(
                "no article (%s) on %s -- the page changed shape; refusing to "
                "report an empty source" % (self.body_selector, self.seed_url))
        title = title or (crumbs[-1] if crumbs else "")
        logger.info("FRRS %s: %r %d chars, %.1fs", self.source_system,
                    title, len(text), time.time() - t0)
        page = {
            "section_path": " > ".join(crumbs),
            "frrs_area": crumbs[1] if len(crumbs) > 1 else "",
            "frrs_last_modified": r.headers.get("Last-Modified") or "",
            "last_update": updated,
            "title": title, "url": self.seed_url,
            "depth": 0, "linked_from_title": title,
            "doc_url": self.seed_url,
            "parent_page_url": self.seed_url, "status": "ok",
            "n_pdfs": 0, "pdf_links": "",
            "text_len": len(text),
            "html_file": "", "text": text, "html": frag,
            "content_hash": content_key(text),
        }
        return {
            "shape": "generic",
            "pages": [page],
            "documents": [],
            "run": {"seed": self.seed_url, "links": 1,
                    "unread": 0, "blocked_pages": 0},
        }

    # ------------------------------------------------------------------ #
    def _page_is_document(self, r: dict) -> bool:
        return True

    def _doc_from_page_row(self, r: dict, shape: str):
        doc = super()._doc_from_page_row(r, shape)
        if doc is None:
            return doc
        doc.doc_path = self._doc_path("", doc.title)
        if not self.category and r.get("frrs_area"):
            doc.category = r["frrs_area"]
        doc.reference_no = self.reference_no or doc.reference_no
        if r.get("last_update"):
            doc.published_date = r["last_update"]
            doc.year = r["last_update"][:4]
        if isinstance(getattr(doc, "extra_meta", None), dict):
            doc.extra_meta["content_hash_basis"] = "visible-text-minus-furniture"
            doc.extra_meta["section_path"] = r.get("section_path") or ""
            if r.get("frrs_last_modified"):
                doc.extra_meta["frrs_last_modified"] = r["frrs_last_modified"]
        return doc

    def fetch_documents(self, limit: Optional[int] = None):
        docs = stamp_content_hashes(super().fetch_documents(limit=limit))
        run = dict((self.last_result or {}).get("run") or {})
        run["warnings"] = list(run.get("warnings") or []) + self._warnings
        self.last_result = {
            "run": run,
            "by_source": {self.source_system: len(docs)},
            "source": self.seed_url,
        }
        logger.info("FRRSPageCrawler[%s] -> %d document(s)",
                    self.source_system, len(docs))
        return docs
