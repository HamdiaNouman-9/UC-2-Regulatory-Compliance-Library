"""GovInfoPublicLawCrawler — one U.S. public law, entered from the agency page
that lists it and stored from govinfo.gov (the GPO).

ONE ROW IS ONE PUBLIC LAW. The crawl starts at `page_url` (FinCEN's USA PATRIOT
Act page) and follows its link to the law. The row's file is GPO's PDF; its text
and `document_html` are GPO's plain-text `.htm` rendering of the same law. The
page's own preamble and section summaries are kept in extra_meta, not hashed.
No XML is read.

`.../xml/<PKG>.xml` RETURNS HTTP 200 WITH AN HTML ERROR PAGE, not XML. See
UC-2-Scratch/GOVINFO/.
"""

from __future__ import annotations

import copy
import html as _html
import logging
import re
import time
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://www.govinfo.gov"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

#: The whole law, and the ONLY thing hashed. GPO wraps its plain-text rendering
#: in a single `<pre>`; there is no other markup in the file.
_PRE = re.compile(r"<pre>(.*?)</pre>", re.S | re.I)

#: What the law says about itself, read in place of the MODS record.
_HEADER = re.compile(r"\[(\d+)(?:st|nd|rd|th) Congress Public Law (\d+)\]")
_SHORT_TITLE = re.compile(r"This Act may be cited as the ``(.+?)''")
_APPROVED = re.compile(r"Approved ([A-Z][a-z]+ \d{1,2}, \d{4})\.")
_PACKAGE = re.compile(r"^PLAW-(\d+)publ(\d+)$")

#: The agency page: its main content block, and the body field inside it that
#: holds the preamble. The rest of the block is a per-section accordion.
_PAGE_CONTENT = "#block-fincen-mainpagecontent"
_PAGE_PREAMBLE = "div.block-field-blocknodesite-pagebody"
_PAGE_SECTION_HEADING = ".usa-accordion__heading"

#: A space goes at these boundaries only; get_text(" ") spaced every inline tag too.
_BLOCK = ("p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "tr", "dt", "dd",
          "div", "section", "table", "ul", "ol", "dl", "br", "hr", "blockquote", "pre")

# openpyxl raises on these, inside save(), after the whole crawl has run.
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

#: Retries for a fetch that could not be ANSWERED. A GET changes nothing.
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0

#: Statuses that are an ANSWER rather than a failure to answer.
_FINAL_STATUSES = (200, 401, 403, 404, 410)


def _decode(resp) -> str:
    """Decode a response as UTF-8 unless it declares otherwise.

    NOT `resp.text`: this host serves `Content-Type: text/html` with no charset
    and `requests` then falls back to ISO-8859-1 per RFC 2616, so the section
    symbols and em dashes throughout the Statutes at Large text would decode to
    mojibake and the mojibake is what would be stored and hashed.
    """
    declared = ""
    ctype = resp.headers.get("Content-Type", "")
    if "charset=" in ctype.lower():
        declared = ctype.lower().split("charset=", 1)[1].split(";")[0].strip()
    for enc in (declared, "utf-8", resp.apparent_encoding, "latin-1"):
        if not enc:
            continue
        try:
            return resp.content.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return resp.content.decode("latin-1", errors="replace")


def _norm(s: str) -> str:
    """NFKC-normalise, drop the zero-width marks NFKC keeps, collapse whitespace."""
    s = unicodedata.normalize("NFKC", s or "")
    return " ".join(re.sub(r"[﻿​‌‍⁠]", "", s).split())


def _block_text(el) -> str:
    """Visible text with breaks at block boundaries only; `el` itself is not modified."""
    el = copy.copy(el)
    for t in el.find_all(["script", "style", "noscript"]):
        t.decompose()
    for tag in el.find_all(_BLOCK):
        tag.insert_before("\n")
        tag.insert_after("\n")
    return _norm(el.get_text(""))


class GovInfoPublicLawCrawler:
    """One U.S. public law: entered from `page_url`, its file GPO's PDF, its
    content GPO's plain-text rendering.

    `package_id`, `doc_folder` and `source_system` come from the YAML so a
    change in how GPO titles a law cannot silently reshape `doc_path`.
    """

    def __init__(
        self,
        regulator: str,
        source_system: str,
        package_id: str,
        doc_folder: str,
        page_url: str,
        expected_page_title: str,
        law_link_text: str,
        htm_url: Optional[str] = None,
        pdf_url: Optional[str] = None,
        category: Optional[str] = None,
        expected_title: Optional[str] = None,
        min_text_chars: int = 1000,
        timeout: int = 120,
        delay: float = 1.0,
    ):
        self.regulator = regulator
        self.source_system = source_system
        # "PLAW-107publ56". GPO's own package id, and the key to every url it
        # serves for this law.
        self.package_id = package_id
        # The doc_path tier between source_system and the document. Each law
        # gets its OWN folder: the folder is what scopes `disappeared`, so two
        # laws sharing one would propose each other for withdrawal.
        self.doc_folder = doc_folder
        # The agency page the crawl starts from, its <h1>, and the text of the
        # link on it that leads to the law.
        self.page_url = page_url
        self.expected_page_title = expected_page_title
        self.law_link_text = law_link_text
        self.htm_url = htm_url or "%s/content/pkg/%s/html/%s.htm" % (
            BASE, package_id, package_id)
        self.pdf_url = pdf_url or "%s/content/pkg/%s/pdf/%s.pdf" % (
            BASE, package_id, package_id)
        # The completeness gate's grouping key; not a doc_path crumb.
        self.listing_title = doc_folder
        self.category = category or source_system
        # A recorded observation of the law's own short title, not our chosen
        # title — comparing against our own would warn forever.
        self.expected_title = expected_title
        # A FLOOR, NOT THE COUNT. A short rendering is indistinguishable from
        # GPO serving an error page, so it fails the run rather than proposing
        # the law for withdrawal.
        self.min_text_chars = int(min_text_chars)
        self.timeout = timeout
        self.delay = float(delay)
        self.last_result: Dict = {}
        self._session: Optional[requests.Session] = None

    def _sess(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers.update({"User-Agent": USER_AGENT,
                              "Accept-Language": "en-US,en;q=0.9"})
            self._session = s
        return self._session

    def _get(self, url: str) -> str:
        """GET with retries. A status that is not an ANSWER raises rather than
        returning an empty body — an empty law would be read as a withdrawal.
        A 403 is an answer: FinCEN sits behind Akamai, and a refusal there must
        not be retried, here or on a schedule."""
        last: Optional[Exception] = None
        for attempt in range(RETRY_ATTEMPTS):
            if attempt or self.delay:
                time.sleep(self.delay if not attempt else RETRY_BACKOFF * attempt)
            try:
                r = self._sess().get(url, timeout=self.timeout)
                if r.status_code in _FINAL_STATUSES:
                    if r.status_code != 200:
                        raise RuntimeError("%s returned %d" % (url, r.status_code))
                    return _decode(r)
                last = RuntimeError("%s returned %d" % (url, r.status_code))
            except requests.RequestException as e:
                last = e
        raise RuntimeError("could not fetch %s: %s" % (url, last))

    def _law_link(self, content) -> str:
        """The page's link to the law, followed to where it lands. It must land on
        the configured PDF, or the page now points at a different document."""
        links = [a["href"] for a in content.find_all("a", href=True)
                 if _norm(a.get_text()) == self.law_link_text]
        if not links:
            raise RuntimeError(
                "govinfo %s: %s no longer has a %r link in its content. Open the "
                "page, find where it now links the law, and update `law_link_text` "
                "(and `pdf_url` if the file moved) in config/sources/govinfo.yml."
                % (self.package_id, self.page_url, self.law_link_text))
        href = urljoin(self.page_url, links[0])
        r = self._sess().head(href, allow_redirects=True, timeout=self.timeout)
        landed = r.url
        if landed != self.pdf_url or "pdf" not in r.headers.get("Content-Type", "").lower():
            raise RuntimeError(
                "govinfo %s: the %r link on %s (%s) now lands on %s (HTTP %d, %s), "
                "not the configured %s. Nothing was stored. Open the page and the "
                "link: if FinCEN now points at a different law or version, that is "
                "a scope decision; if only the address moved, update `pdf_url` and "
                "`package_id` in config/sources/govinfo.yml."
                % (self.package_id, self.law_link_text, self.page_url, href, landed,
                   r.status_code, r.headers.get("Content-Type", ""), self.pdf_url))
        return href

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        warnings: List[str] = []

        # ---- the agency page the crawl starts from --------------------------
        soup = BeautifulSoup(self._get(self.page_url), "html.parser")
        h1 = soup.find("h1")
        page_title = _norm(h1.get_text()) if h1 else ""
        if page_title != self.expected_page_title:
            raise RuntimeError(
                "govinfo %s: %s has <h1> %r, expected %r. The page has moved or "
                "changed; refusing to follow its links."
                % (self.package_id, self.page_url, page_title, self.expected_page_title))
        content = soup.select_one(_PAGE_CONTENT)
        preamble = soup.select_one(_PAGE_PREAMBLE)
        if content is None or preamble is None:
            raise RuntimeError(
                "govinfo %s: %s no longer has %s / %s. The page template has "
                "changed; update the selectors in crawler/govinfo_crawler.py."
                % (self.package_id, self.page_url, _PAGE_CONTENT, _PAGE_PREAMBLE))
        # The accordion: one {heading: summary} pair per section of the Act the
        # page summarises. Each heading's button names its body by aria-controls.
        sections = {}
        for h in content.select(_PAGE_SECTION_HEADING):
            button = h.find("button")
            body = content.find(id=button.get("aria-controls")) if button else None
            if body is None:
                raise RuntimeError(
                    "govinfo %s: accordion heading %r on %s has no body. The page "
                    "template has changed; update the accordion reading in "
                    "crawler/govinfo_crawler.py." % (self.package_id, _block_text(h), self.page_url))
            sections[_block_text(h)] = _ILLEGAL_XLSX.sub(" ", _block_text(body))
        if not sections:
            raise RuntimeError(
                "govinfo %s: %s has no section accordion (%s). Open the page; if the "
                "summaries moved, update the selector in crawler/govinfo_crawler.py."
                % (self.package_id, self.page_url, _PAGE_SECTION_HEADING))
        self._law_link(content)                         # the guard: raises unless it lands on pdf_url
        page_links = []
        for a in content.find_all("a", href=True):
            href = urljoin(self.page_url, a["href"])
            if href.startswith("http") and href not in page_links:
                page_links.append(href)

        # ---- the law itself -------------------------------------------------
        page = self._get(self.htm_url)
        pre = _PRE.search(page)
        if not pre:
            # THE 200-STATUS TRAP. govinfo answers a missing rendering with its
            # own Drupal page under HTTP 200, so "it fetched fine" is not
            # evidence that the law came back. Measured on the `.xml` rendering
            # of six unrelated packages 2026-09-22 — all six returned this page.
            raise RuntimeError(
                "govinfo %s: %s has no <pre> block. govinfo serves a missing "
                "rendering as HTTP 200 with its own HTML page, so this is most "
                "likely that page and not the law. Refusing to store it."
                % (self.package_id, self.htm_url))

        content_text = _ILLEGAL_XLSX.sub(
            " ", _norm(_html.unescape(pre.group(1))))
        if len(content_text) < self.min_text_chars:
            raise RuntimeError(
                "govinfo %s: %d characters of text, below the floor of %d. "
                "Refusing a short law — it is indistinguishable from a truncated "
                "or replaced rendering."
                % (self.package_id, len(content_text), self.min_text_chars))

        # IDENTITY CHECK, from the law's own header: "[107th Congress Public
        # Law 56]" must be the configured package, or we fetched another law.
        header, package = _HEADER.search(content_text), _PACKAGE.match(self.package_id)
        if not header or not package or header.groups() != package.groups():
            raise RuntimeError(
                "govinfo %s: %s is headed %r. Refusing to store one law under "
                "another's folder." % (self.package_id, self.htm_url,
                                       header.group(0) if header else None))
        law_number = "Public Law %s-%s" % header.groups()

        short = _SHORT_TITLE.search(content_text)
        if not short:
            raise RuntimeError(
                "govinfo %s: no \"This Act may be cited as\" clause in %s; "
                "refusing to guess a title." % (self.package_id, self.htm_url))
        title = _norm(short.group(1))
        if self.expected_title and self.expected_title != title:
            warnings.append("the recorded title %r is now %r"
                            % (self.expected_title, title))
            logger.warning(warnings[-1])

        # The date Congress enacted it, as the law itself states it. The only
        # date here that describes the LAW; it has not moved since 2001.
        approved = _APPROVED.findall(content_text)
        if len(approved) != 1:
            raise RuntimeError(
                "govinfo %s: expected one \"Approved <date>.\" line in %s, found "
                "%d." % (self.package_id, self.htm_url, len(approved)))
        date_approved = datetime.strptime(approved[0], "%B %d, %Y").date().isoformat()

        meta = {
            "crawl_source": self.listing_title,
            "package_id": self.package_id,
            # The agency page's own introduction to the law: information ABOUT
            # the instrument, not the instrument, so outside document_html and
            # the hash — the rule written down in hints/mhrsd.regs.yml.
            "source_page_overview": _ILLEGAL_XLSX.sub(" ", _block_text(preamble)),
            # The page's section-by-section summaries, label -> value as MHRSD's
            # labelled sidebar is stored; also outside the hash.
            "source_page_sections": sections,
            # Every link in the page's content, as published. Deliberately not
            # `attachment_links`: they are not this law's files.
            "content_links": " | ".join(page_links),
            "content_text": content_text,
            "text_chars": len(content_text),
            "htm_url": self.htm_url,
        }

        doc = RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self.category,
            title=title,
            # Exactly one file, so `document_url` is set and attachment_links
            # stays empty — the single-file branch of the models.py contract.
            document_url=self.pdf_url,
            doc_path=[self.regulator, self.source_system, self.doc_folder, title],
            file_type="PDF",
            source_page_url=self.page_url,
            published_date=date_approved,
            reference_no=law_number,
            document_html=_ILLEGAL_XLSX.sub(" ", pre.group(0)),
            extra_meta=meta,
        )
        # The hash is the law's own text and nothing else; the agency page's
        # preamble and links stay out of it.
        doc.content_hash = content_key(content_text)
        meta["content_hash_basis"] = "gpo-plaintext-rendering"
        docs = stamp_content_hashes([doc])          # single exit

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.listing_title: len(docs)},
            "source": self.page_url,
        }
        logger.info("GovInfoPublicLawCrawler %s: %r, %d chars, approved %s, %d page links",
                    self.package_id, title, len(content_text), date_approved, len(page_links))
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs
