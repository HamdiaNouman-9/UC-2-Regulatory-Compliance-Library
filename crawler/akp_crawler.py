"""AKPCrawler — one consolidated Alberta Act or Regulation from King's Printer.

The text is the HTML rendering's `div.htmlDocument`: outside it sit the site
navigation, the "Current as of" banner and a "© 1995 - <year>" footer that rolls
every January. A "(Consolidated up to M/YYYY)" line is stripped from text and
hash alike; both dates are kept as metadata. See UC-2-Scratch/ATB/.
"""

from __future__ import annotations

import copy
import logging
import re
import time
import unicodedata
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://kings-printer.alberta.ca/"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

_CURRENT = re.compile(r"Current as of ([A-Z][a-z]+ \d{1,2}, \d{4})")
_CONSOLIDATED = re.compile(r"\(\s*Consolidated up to ([\d/]+)\s*\)")

#: A space goes at these boundaries only; get_text(" ") spaced every inline tag too.
_BLOCK = ("p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "tr", "dt", "dd",
          "div", "section", "table", "ul", "ol", "dl", "br", "hr", "blockquote", "pre")
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[﻿​‌‍⁠]", "", s)       # NFKC keeps zero-widths
    return re.sub(r"\s+", " ", s).strip()


def _block_text(el) -> str:
    """Visible text with breaks at block boundaries only; `el` itself is not modified."""
    el = copy.copy(el)
    for t in el.find_all(["title", "script", "style"]):
        t.decompose()
    for tag in el.find_all(_BLOCK):
        tag.insert_before("\n")
        tag.insert_after("\n")
    return el.get_text("")


class AKPCrawler:
    """One document. `page` is King's Printer's own file name (A45p2.cfm), which
    holds across consolidations; the ISBN in its listing urls does not."""

    def __init__(self, regulator: str, source_system: str, folder: str, page: str,
                 leg_type: str, pdf_file: str, expected_title: str,
                 min_text_chars: int, category: Optional[str] = None,
                 delay: float = 3.0, **_):
        self.regulator = regulator
        self.source_system = source_system
        self.folder = folder
        self.leg_type = leg_type                    # "Acts" | "Regs"
        self.expected_title = expected_title
        self.min_text_chars = int(min_text_chars)
        self.category = category or source_system
        self.delay = float(delay)
        self.html_url = "%s1266.cfm?page=%s&leg_type=%s&display=html" % (BASE, page, leg_type)
        self.pdf_url = "%sserve_pdf.cfm?file=%s&type=%s" % (BASE, pdf_file, leg_type)
        self._s = requests.Session()
        self._s.headers["User-Agent"] = USER_AGENT

    def _get(self, url: str) -> str:
        last = None
        for attempt in range(3):
            try:
                r = self._s.get(url, timeout=90)
                if r.status_code == 200:
                    time.sleep(self.delay)
                    return r.content.decode("utf-8")
                raise RuntimeError("%s returned %d" % (url, r.status_code))
            except requests.RequestException as e:
                last = e
                time.sleep(3.0 * (attempt + 1))
        raise RuntimeError("could not fetch %s: %s" % (url, last))

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        soup = BeautifulSoup(self._get(self.html_url), "html.parser")
        doc_el = soup.select_one("div.htmlDocument")
        if doc_el is None:
            raise RuntimeError("AKP %s: no div.htmlDocument" % self.html_url)
        t = doc_el.find("title")
        title = _norm(t.get_text()) if t else ""
        if title != self.expected_title:
            raise RuntimeError("AKP %s: title %r, expected %r"
                               % (self.html_url, title, self.expected_title))
        banner = _CURRENT.search(_norm(soup.get_text(" ")))
        raw = _norm(_block_text(doc_el))
        consolidated = _CONSOLIDATED.search(raw)
        text = _ILLEGAL_XLSX.sub(" ", _norm(_CONSOLIDATED.sub(" ", raw)))
        if len(text) < self.min_text_chars:
            raise RuntimeError("AKP %s: %d chars, below floor %d"
                               % (self.html_url, len(text), self.min_text_chars))
        meta = {
            "current_as_of": banner.group(1) if banner else "",       # per document; provenance only
            "consolidated_up_to": consolidated.group(1) if consolidated else "",
            "content_text": text,
            "text_chars": len(text),
            "pdf_url": self.pdf_url,
            "content_hash_basis": "htmlDocument-text-minus-consolidation-line",
        }
        doc = RegulatoryDocument(
            regulator=self.regulator, source_system=self.source_system,
            category=self.category, title=title, document_url=self.html_url,
            doc_path=[self.regulator, self.source_system, self.folder, title],
            file_type="HTML", source_page_url=self.html_url,
            published_date=None, reference_no=None, document_html=None,
            extra_meta=meta,
        )
        doc.content_hash = content_key(text)
        docs = stamp_content_hashes([doc])          # single exit
        logger.info("AKP %s: %r, %d chars, current as of %s",
                    self.leg_type, title, len(text), meta["current_as_of"])
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs
