"""ASFICrawler — the alberta.ca regulatory framework page published by the Alberta
Superintendent of Financial Institutions, and the Acts it lists.

The page's text is its `div.paragraph--type--text-block` content: the header news
feed, the "© <year>" footer and Cloudflare's cache-time Last-Modified sit outside
it. Cloudflare's per-request email cipher is decoded to the address. Each Act and
Regulation is one King's Printer document, crawled by `AKPCrawler`. The page is
the master list: every run checks its "Related Alberta legislation" links against
`acts`. See UC-2-Scratch/ALBERTA_FIRF/.
"""

from __future__ import annotations

import copy
import logging
import re
import time
import unicodedata
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.akp_crawler import AKPCrawler
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://www.alberta.ca/"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

LEGISLATION_HEADING = "Related Alberta legislation"
_EMAIL_HREF = "/cdn-cgi/l/email-protection#"
_MULTI_FILE_IDENTITY = ["doc_path", "extra_meta.attachment_links", "title"]

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


def _decoded(el):
    """A copy of `el` without scripts, its Cloudflare email cipher decoded."""
    el = copy.copy(el)
    for t in el.find_all(["script", "style", "noscript"]):
        t.decompose()
    for span in el.select("[data-cfemail]"):
        span.replace_with(_cf_decode(span["data-cfemail"]))
    return el


def _block_text(el) -> str:
    """Visible text with breaks at block boundaries only; `el` itself is not modified."""
    el = _decoded(el)
    for tag in el.find_all(_BLOCK):
        tag.insert_before("\n")
        tag.insert_after("\n")
    return el.get_text("")


def _block_html(el, base: str) -> str:
    """The block as stored HTML: decoded email, absolute links."""
    el = _decoded(el)
    for a in el.find_all("a", href=True):
        href = a["href"]
        if href.startswith(_EMAIL_HREF):
            a["href"] = "mailto:" + _cf_decode(href[len(_EMAIL_HREF):])
        elif not href.startswith(("#", "tel:", "mailto:")):
            a["href"] = urljoin(base, href)
    for img in el.find_all("img", src=True):
        img["src"] = urljoin(base, img["src"])
    return str(el)


def _listed_acts(blocks) -> Dict[str, tuple]:
    """{open.alberta.ca slug: (sub-heading, link text, href)} under LEGISLATION_HEADING."""
    for b in blocks:
        h2 = next((h for h in b.find_all("h2") if _norm(h.get_text()) == LEGISLATION_HEADING), None)
        if h2 is None:
            continue
        found, sub = {}, ""
        for el in h2.find_all_next():
            if el.name == "h2":
                break
            if el.name == "h3":
                sub = _norm(el.get_text())
            elif el.name == "a" and el.get("href"):
                u = urlparse(el["href"])
                if u.netloc.endswith("open.alberta.ca") and u.path.startswith("/publications/"):
                    slug = u.path.rstrip("/").rsplit("/", 1)[-1]
                    found[slug] = (sub, _norm(el.get_text()), el["href"])
        return found
    return {}


class ASFICrawler:
    """The page named by `path` as one row, then one row per entry of `acts` and
    its `regulations`. Raises before crawling any Act if the page's list and
    `acts` disagree."""

    def __init__(self, regulator: str, source_system: str, folder: str, path: str,
                 expected_title: str, min_text_chars: int, acts: List[dict],
                 category: Optional[str] = None, delay: float = 3.0,
                 check_files: bool = False, **_):
        self.regulator = regulator
        self.source_system = source_system
        self.folder = folder
        self.expected_title = expected_title
        self.min_text_chars = int(min_text_chars)
        self.acts = list(acts)
        self.category = category or source_system
        self.delay = float(delay)
        self.check_files = bool(check_files)      # passed to AKPCrawler: HEAD each PDF
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

    def _check_act_list(self, listed: Dict[str, tuple]) -> None:
        """The page is the master list; `acts` must name exactly what it lists."""
        if not listed:
            raise RuntimeError(
                "ASFI %s: no %r section with open.alberta.ca links was found. The page "
                "layout has changed; open it, find where the Acts are now listed, and "
                "update `_listed_acts` in crawler/asfi_crawler.py." % (self.url, LEGISLATION_HEADING))
        configured = {a["slug"]: a for a in self.acts}
        problems = []
        for slug in sorted(set(listed) - set(configured)):
            sub, text, href = listed[slug]
            problems.append("  on the page, not in the YAML: %s (%r under %r, %s)" % (slug, text, sub, href))
        for slug in sorted(set(configured) - set(listed)):
            problems.append("  in the YAML, not on the page: %s (%s)"
                            % (slug, configured[slug]["expected_title"]))
        for slug in sorted(set(listed) & set(configured)):
            if listed[slug][0] != configured[slug]["folder"]:
                problems.append("  %s moved: the page lists it under %r, the YAML folder is %r"
                                % (slug, listed[slug][0], configured[slug]["folder"]))
        if problems:
            raise RuntimeError(
                "ASFI %s: the Acts under %r no longer match `acts` in "
                "config/sources/asfi.yml. Nothing was crawled, so no row is marked "
                "new or disappeared.\n%s\nTo fix: open the page and confirm the change. "
                "For an added Act, open its open.alberta.ca page and follow its resource "
                "link to King's Printer; add an `acts` entry with slug, folder (the "
                "page's sub-heading), page and pdf_file (King's Printer's file name), "
                "expected_title (the document's <title>), min_text_chars, and any "
                "`regulations` the page says it includes. For a removed Act, delete its "
                "entry. For a moved Act, change its folder. Then export and `check` again."
                % (self.url, LEGISLATION_HEADING, "\n".join(problems)))

    def _page_document(self, soup, blocks) -> RegulatoryDocument:
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
            # Every link on the page; several, so document_url stays empty.
            "attachment_links": " | ".join(links),
            "identity_fields": _MULTI_FILE_IDENTITY,
            "content_hash_basis": "text-block-visible-text",
        }
        html = "\n".join(_block_html(b, self.url) for b in blocks)
        doc = RegulatoryDocument(
            regulator=self.regulator, source_system=self.source_system,
            category=self.category, title=title, document_url="",
            doc_path=[self.regulator, self.source_system, self.folder, title],
            file_type="HTML", source_page_url=self.url,
            published_date=None, reference_no=None,
            document_html=_ILLEGAL_XLSX.sub(" ", html),
            extra_meta=meta,
        )
        doc.content_hash = content_key(text)
        logger.info("ASFI: %r, %d chars, %d links", title, len(text), len(links))
        return doc

    def _legislation(self, act: dict, leg_type: str, folder: str) -> RegulatoryDocument:
        crawler = AKPCrawler(
            regulator=self.regulator, source_system=self.source_system, folder=folder,
            folders=[LEGISLATION_HEADING, folder], page=act["page"], leg_type=leg_type,
            pdf_file=act["pdf_file"], expected_title=act["expected_title"],
            min_text_chars=act["min_text_chars"], category=self.category, delay=self.delay,
            check_files=self.check_files)
        return crawler.fetch_documents()[0]

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        soup = BeautifulSoup(self._get(self.url), "html.parser")
        blocks = soup.select("main div.goa-main-grid div.paragraph--type--text-block")
        if not blocks:
            raise RuntimeError("ASFI %s: no text block" % self.url)
        self._check_act_list(_listed_acts(blocks))
        docs = [self._page_document(soup, blocks)]
        for act in self.acts:
            docs.append(self._legislation(act, "Acts", act["folder"]))
            for reg in act.get("regulations") or []:
                docs.append(self._legislation(reg, "Regs", act["folder"]))
        docs = stamp_content_hashes(docs)          # single exit
        logger.info("ASFI: %d documents (page + %d Acts)", len(docs), len(self.acts))
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs
