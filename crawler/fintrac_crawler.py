"""FintracCrawler — one controller for FINTRAC's tabs; each source entry names its `tab`.

  regulations  one row per regulation FINTRAC lists. They live on laws-lois, the
               same site and template as Justice Canada's Acts, so each is read by
               JusticeCanadaActCrawler: the PDF is the file, FullText.html the
               content, and the XML only the monitoring probe (JusticeXmlSweep).
  guidelines   one row per guidance page on the hub; the hidden glossary and the
               Date Modified block are stripped before hashing.

See UC-2-Scratch/FINTRAC/.
"""

from __future__ import annotations

import copy
import logging
import re
import time
import unicodedata
from typing import List, Optional
from urllib.parse import urljoin, urldefrag, urlparse

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from crawler.justice_canada_crawler import JusticeCanadaActCrawler
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

JUSTICE = "https://laws-lois.justice.gc.ca"
FINTRAC = "https://fintrac-canafe.canada.ca"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

#: Inside every guidance page's <main>: a 57,659-char glossary, identical on 80
#: of 89 pages, and the hand-kept Date Modified. Neither is the guidance.
_GUIDANCE_STRIP = ("div.hidden.definition", "div.pagedetails")

#: A file FINTRAC itself serves. Files on other hosts (the Canada Gazette, Public
#: Safety) are references, recorded in content_links, never this page's files.
_FILE = re.compile(r"\.(pdf|xls[xmb]?|xml|xsd|csv|zip|docx?|pptx?)$", re.I)
_FILES_IDENTITY = ["doc_path", "extra_meta.attachment_links", "title"]

_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _norm(s: str) -> str:
    """NFKC, BOM dropped, whitespace collapsed (U+FEFF is not whitespace to split())."""
    s = unicodedata.normalize("NFKC", s).replace("﻿", "")
    return re.sub(r"\s+", " ", s).strip()


#: HTML gets a space at these boundaries only; get_text(" ") spaced every tag.
_BLOCK = ("p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "tr", "dt", "dd",
          "div", "section", "summary", "details", "caption", "table", "ul", "ol", "dl",
          "br", "hr", "blockquote", "pre", "figure", "figcaption", "header", "footer")


def _block_text(el) -> str:
    """Visible text with breaks at block boundaries only; `el` itself is not modified."""
    el = copy.copy(el)
    for tag in el.find_all(_BLOCK):
        tag.insert_before("\n")
        tag.insert_after("\n")
    return el.get_text("")


class FintracCrawler:
    """One FINTRAC tab. `init_kwargs` beyond the common ones belong to that tab."""

    def __init__(self, regulator: str, source_system: str, tab: str,
                 category: Optional[str] = None, delay: float = 1.0, **params):
        if tab not in self._TABS:
            raise ValueError("FINTRAC: unknown tab %r, expected one of %s"
                             % (tab, sorted(self._TABS)))
        self.regulator = regulator
        self.source_system = source_system
        self.tab = tab
        self.category = category or source_system
        self.delay = float(delay)
        self.params = params
        self._s = requests.Session()
        self._s.headers["User-Agent"] = USER_AGENT

    def _get(self, url: str) -> str:
        last = None
        for attempt in range(3):
            try:
                r = self._s.get(url, timeout=60)
                if r.status_code == 200:
                    time.sleep(self.delay)
                    return r.content.decode("utf-8")
                raise RuntimeError("%s returned %d" % (url, r.status_code))
            except requests.RequestException as e:
                last = e
                time.sleep(2.0 * (attempt + 1))
        raise RuntimeError("could not fetch %s: %s" % (url, last))

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        docs = stamp_content_hashes(self._TABS[self.tab](self))      # single exit
        logger.info("FINTRAC %s: %d documents", self.tab, len(docs))
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs

    # ------------------------------------------------------------------ regulations
    def _regulation(self) -> List[RegulatoryDocument]:
        p = self.params
        code = p["reg_code"]                                   # e.g. SOR-2002-184
        crawler = JusticeCanadaActCrawler(
            regulator=self.regulator, source_system=self.source_system,
            act_code=code, act_folder=p["reg_folder"],
            listing_url="%s/eng/regulations/%s/" % (JUSTICE, code),
            pdf_url="%s/PDF/%s.pdf" % (JUSTICE, code),
            category=self.category, expected_title=p["expected_title"],
            min_text_chars=int(p["min_text_chars"]), delay=self.delay)
        docs = crawler.fetch_documents()
        if crawler.last_result["run"]["warnings"]:
            # A changed title is a different document filed under this folder.
            raise RuntimeError("FINTRAC %s: %s" % (code, crawler.last_result["run"]["warnings"]))
        return docs

    # ------------------------------------------------------------------- guidelines
    def _guidelines(self) -> List[RegulatoryDocument]:
        p = self.params
        hub_url = p["hub_url"]
        wanted = [_norm(s) for s in p["sections"]]
        skip = {urljoin(FINTRAC, u) for u in p.get("skip_urls", [])}

        main = BeautifulSoup(self._get(hub_url), "html.parser").find("main")
        found, links = set(), []                     # (section, url), first section wins
        for h2 in main.find_all("h2"):
            section = _norm(h2.get_text(" "))
            if section not in wanted:
                continue
            found.add(section)
            for el in h2.find_all_next():
                if el.name == "h2":
                    break
                if el.name == "a" and el.get("href"):
                    url = urldefrag(urljoin(hub_url, el["href"]))[0]
                    if url.startswith(FINTRAC) and url not in skip \
                            and url not in {u for _, u in links}:
                        links.append((section, url))
        missing = set(wanted) - found
        if missing:
            raise RuntimeError("FINTRAC hub: sections not found %s" % sorted(missing))
        if len(links) < int(p["min_documents"]):
            raise RuntimeError("FINTRAC hub: %d guidance links, below floor %s"
                               % (len(links), p["min_documents"]))

        docs = []
        for section, url in links:
            m = BeautifulSoup(self._get(url), "html.parser").find("main")
            if m is None:
                raise RuntimeError("FINTRAC %s: no <main>" % url)
            dm = m.select_one("dl#wb-dtmd time") or m.select_one("dl#wb-dtmd dd")
            date_modified = _norm(dm.get_text()) if dm else ""
            for sel in _GUIDANCE_STRIP:
                for el in m.select(sel):
                    el.decompose()
            # Flowcharts and infographics carry content the text does not; record
            # them so the gap is visible. Pipe-joined string, never a list.
            images = list(dict.fromkeys(
                urljoin(url, i["src"]) for i in m.find_all("img") if i.get("src")))
            # The lead's file rule counts only FINTRAC's own files (none today,
            # measured 2026-10-12); every other http link is a reference.
            files, content_links = [], []
            for a in m.find_all("a", href=True):
                u = urldefrag(urljoin(url, a["href"]))[0]
                if not u.startswith("http") or u == url:
                    continue
                if u.startswith(FINTRAC) and _FILE.search(urlparse(u).path):
                    if u not in files:
                        files.append(u)
                elif u not in content_links:
                    content_links.append(u)
            h1 = m.find("h1")
            title = _norm(h1.get_text()) if h1 else ""
            content_text = _ILLEGAL_XLSX.sub(" ", _norm(_block_text(m)))
            if not title or len(content_text) < int(p["min_text_chars"]):
                raise RuntimeError("FINTRAC %s: title %r, %d chars"
                                   % (url, title, len(content_text)))
            meta = {
                "hub_section": section,
                "hub_url": hub_url,
                # An UPDATE date, so never published_date (part of identity).
                "date_modified": date_modified,
                "image_urls": " | ".join(images),
                "image_count": len(images),
                "content_links": " | ".join(content_links),
                "content_text": content_text,
                "text_chars": len(content_text),
                "content_hash_basis": "main-text-minus-glossary-and-date",
            }
            document_url, file_type = url, "HTML"
            if len(files) == 1:
                document_url, file_type = files[0], files[0].rsplit(".", 1)[-1].upper()
            elif files:
                meta["attachment_links"] = " | ".join(files)
                meta["identity_fields"] = _FILES_IDENTITY
                document_url = ""
            doc = RegulatoryDocument(
                regulator=self.regulator, source_system=self.source_system,
                category=self.category, title=title, document_url=document_url,
                doc_path=[self.regulator, self.source_system, title],
                file_type=file_type, source_page_url=url,
                published_date=None, reference_no=None,
                document_html=str(m), extra_meta=meta,
            )
            doc.content_hash = content_key(content_text)
            docs.append(doc)
        return docs

    _TABS = {"regulations": _regulation, "guidelines": _guidelines}
