"""ASFICrawler — one alberta.ca page published by the Alberta Superintendent of
Financial Institutions.

The text is the page's `div.paragraph--type--text-block` content: the header news
feed, the "© <year>" footer and Cloudflare's cache-time Last-Modified sit outside
it. Cloudflare's per-request email cipher is decoded to the address. See
UC-2-Scratch/ALBERTA_FIRF/.
"""

from __future__ import annotations

import copy
import logging
import re
import time
import unicodedata
from typing import List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://www.alberta.ca/"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

#: A space goes at these boundaries only; get_text(" ") spaced every inline tag too.
_BLOCK = ("p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "tr", "dt", "dd",
          "div", "section", "table", "ul", "ol", "dl", "br", "hr", "blockquote", "pre")
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[﻿​‌‍⁠]", "", s)       # NFKC keeps zero-widths
    return re.sub(r"\s+", " ", s).strip()


def _cf_decode(code: str) -> str:
    """Cloudflare's `data-cfemail`: first byte is the XOR key for the rest."""
    key = int(code[:2], 16)
    return "".join(chr(int(code[i:i + 2], 16) ^ key) for i in range(2, len(code), 2))


def _block_text(el) -> str:
    """Visible text with breaks at block boundaries only; `el` itself is not modified."""
    el = copy.copy(el)
    for t in el.find_all(["script", "style", "noscript"]):
        t.decompose()
    for span in el.select("[data-cfemail]"):
        span.replace_with(_cf_decode(span["data-cfemail"]))
    for tag in el.find_all(_BLOCK):
        tag.insert_before("\n")
        tag.insert_after("\n")
    return el.get_text("")


class ASFICrawler:
    """One document: the page named by `path`, its text blocks as one row."""

    def __init__(self, regulator: str, source_system: str, folder: str, path: str,
                 expected_title: str, min_text_chars: int,
                 category: Optional[str] = None, delay: float = 3.0, **_):
        self.regulator = regulator
        self.source_system = source_system
        self.folder = folder
        self.expected_title = expected_title
        self.min_text_chars = int(min_text_chars)
        self.category = category or source_system
        self.delay = float(delay)
        self.url = urljoin(BASE, path)
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
        soup = BeautifulSoup(self._get(self.url), "html.parser")
        blocks = soup.select("main div.goa-main-grid div.paragraph--type--text-block")
        if not blocks:
            raise RuntimeError("ASFI %s: no text block" % self.url)
        h1 = soup.find("h1")
        title = _norm(h1.get_text()) if h1 else ""
        if title != self.expected_title:
            raise RuntimeError("ASFI %s: title %r, expected %r"
                               % (self.url, title, self.expected_title))
        text = _ILLEGAL_XLSX.sub(" ", _norm("\n".join(_block_text(b) for b in blocks)))
        if len(text) < self.min_text_chars:
            raise RuntimeError("ASFI %s: %d chars, below floor %d"
                               % (self.url, len(text), self.min_text_chars))
        links = []
        for b in blocks:
            for a in b.find_all("a", href=True):
                href = urljoin(self.url, a["href"])
                if href.startswith("http") and "/cdn-cgi/" not in href and href not in links:
                    links.append(href)
        published = soup.find("meta", attrs={"name": "published"})
        meta = {
            "page_published": published.get("content", "") if published else "",   # provenance only
            "content_text": text,
            "text_chars": len(text),
            # Other publications the page points to, not this page's files.
            "related_links": " | ".join(links),
            "content_hash_basis": "text-block-visible-text",
        }
        doc = RegulatoryDocument(
            regulator=self.regulator, source_system=self.source_system,
            category=self.category, title=title, document_url=self.url,
            doc_path=[self.regulator, self.source_system, self.folder, title],
            file_type="HTML", source_page_url=self.url,
            published_date=None, reference_no=None, document_html=None,
            extra_meta=meta,
        )
        doc.content_hash = content_key(text)
        docs = stamp_content_hashes([doc])          # single exit
        logger.info("ASFI: %r, %d chars, %d related links", title, len(text), len(links))
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs
