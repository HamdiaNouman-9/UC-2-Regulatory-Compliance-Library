"""LMRA crawlers — Bahrain's labour-market legislation, read with `requests`.

TWO SHAPES, TWO CLASSES, because this regulator publishes in two ways and one
class with a branch inside it would hide that:

  `LMRALegalCategoryCrawler`  one `/en/legal/category/<id>` listing. Each row is
                              an instrument whose FULL TEXT is the HTML of its
                              own `/en/legal/show/<id>` page — the EDB shape.

  `LMRAInstrumentCrawler`     one `/en/page/show/<id>` landing page that IS a
                              single instrument. LMRA Law is split across 47
                              article sub-pages; the Labour Law landing page
                              carries only a summary and the law is its PDF. The
                              `text_from` key in the YAML says which.

Both store the text they extract. The orchestrator writes the version row BEFORE
it extracts anything (`orch.py:869`), so text it extracts itself never reaches a
workbook.
"""

from __future__ import annotations

import hashlib
import logging
import os
import pathlib
import re
import tempfile
import time
import unicodedata
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://www.lmra.gov.bh"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# The one region of the template that holds page content. Everything above and
# below it — the navigation, the footer, the service-outage modal with its own
# dates in it — is chrome that would otherwise enter every fingerprint.
CONTENT = "#page_content"

# Chrome that lives INSIDE `#page_content` and must still go. `.btn-group` and
# `.dropdown-menu` are the "Pages Index" that repeats all 47 article links on
# every article page; without this, 47 documents would share ~500 identical
# characters and an amendment would be diluted by nav.
_INNER_CHROME = ".btn-group, .dropdown-menu, .share-buttons"

# A listing row. The anchor carries the clean title in `title=` and the site's
# posting date in a `<span class="badge">`.
LIST_SELECTOR = "a.list-group-item[href*='/legal/show/']"

# The article sub-pages of a law, in the landing page's own order.
ARTICLE_SELECTOR = ".dropdown-menu a[href*='/page/show/']"
_ARTICLE_LABEL = re.compile(r"^Article\s+\d+$", re.I)

# openpyxl raises on these rather than escaping them, and it raises inside
# save() — after the whole crawl has run. Tab, newline and carriage return are
# legal and are left alone.
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# Below this a detail page is a stub or an error template, not an instrument.
# The shortest real one measured 703 characters.
_MIN_DOC_CHARS = 400

# Below this an assembled law is not a law. LMRA Law measures ~57,000 characters
# across its 47 articles and the Labour Law PDF ~117,000; a nav change that left
# the assembler with nothing would otherwise store an empty instrument and
# report it `modified` rather than failing.
_MIN_INSTRUMENT_CHARS = 2000


def _cache_file(cache_dir: Optional[str], url: str, suffix: str) -> Optional[pathlib.Path]:
    if not cache_dir:
        return None
    d = pathlib.Path(cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    return d / (hashlib.sha256(url.encode()).hexdigest()[:16] + suffix)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _norm(s: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", s or "").split())


def _clean(text: str) -> str:
    """NFKC first, sanitise second, so the text that is hashed is the text that
    reaches the workbook.

    NFKC folds the no-break spaces and Arabic presentation forms this CMS emits,
    so a template toggling `&nbsp;` cannot move a fingerprint on its own.
    """
    text = unicodedata.normalize("NFKC", text or "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return _ILLEGAL_XLSX.sub(" ", text).strip()


#: Retries for a fetch that could not be ANSWERED. A GET changes nothing on the
#: server, so asking again is safe; the backoff is what keeps it polite. Two
#: hosts in this library are already blocked for automated access and neither
#: block was about volume, so this stays small on purpose: 3 attempts, 1s then
#: 2s, a worst case of 3 extra seconds on a document that is failing anyway.
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.0

#: Statuses that are an ANSWER rather than a failure to answer. Retrying these
#: would just repeat a question the server has already settled.
_FINAL_STATUSES = (200, 401, 403, 404, 410)


def _get(url: str, session: requests.Session, timeout: int, delay: float,
         cache_dir: Optional[str], attempts: int = RETRY_ATTEMPTS) -> Tuple[str, int, str]:
    """One page, retrying only what could not be answered.

    Shared by both crawlers so they honour the same dev cache, the same
    politeness delay and the same retry policy. A transient 500 or timeout that
    is allowed through here does not merely lose a page — for an assembled
    document it MOVES THE FINGERPRINT, so it is worth paying a few seconds to
    avoid.
    """
    cp = _cache_file(cache_dir, url, ".html")
    if cp is not None and cp.exists():
        return cp.read_text("utf-8", "replace"), 200, "page-cache"
    wait, html, status, origin = RETRY_BACKOFF, "", 0, "no-attempt"
    for attempt in range(1, max(attempts, 1) + 1):
        try:
            r = session.get(url, timeout=timeout)
            html, status, origin = r.text, r.status_code, "live"
            if status in _FINAL_STATUSES:
                break
            logger.warning("LMRA %s -> HTTP %s (attempt %d/%d)",
                           url[-60:], status, attempt, attempts)
        except requests.RequestException as e:
            html, status, origin = "", 0, type(e).__name__
            logger.warning("LMRA %s -> %s (attempt %d/%d)",
                           url[-60:], type(e).__name__, attempt, attempts)
        if attempt < attempts:
            time.sleep(wait)
            wait *= 2
    if delay:
        time.sleep(delay)
    if cp is not None and status == 200:
        cp.write_text(html, encoding="utf-8")
    return html, status, origin


def _get_bytes(url: str, session: requests.Session, timeout: int, delay: float,
               attempts: int = RETRY_ATTEMPTS):
    """One file, on the same retry policy as `_get`. Returns the response or None."""
    wait = RETRY_BACKOFF
    for attempt in range(1, max(attempts, 1) + 1):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code in _FINAL_STATUSES:
                if delay:
                    time.sleep(delay)
                return r
            logger.warning("LMRA pdf %s -> HTTP %s (attempt %d/%d)",
                           url[-60:], r.status_code, attempt, attempts)
        except requests.RequestException as e:
            logger.warning("LMRA pdf %s -> %s (attempt %d/%d)",
                           url[-60:], type(e).__name__, attempt, attempts)
        if attempt < attempts:
            time.sleep(wait)
            wait *= 2
    return None


def _content(html: str):
    """(element, heading, text, last_update) for one LMRA page.

    The element and the text come from the SAME parse of the SAME block, so they
    can never describe different content. `last_update` is the site's own change
    stamp; it is LEFT IN the text as well as reported separately — it is visible
    text on the page, and cutting parts out of "the page's visible text" is how a
    fingerprint stops being reproducible from what a reader sees.
    """
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    el = soup.select_one(CONTENT)
    if el is None:
        return None, "", "", ""
    for junk in el.select(_INNER_CHROME):
        junk.decompose()

    h = el.select_one("h1, h2")
    heading = _norm(h.get_text(" ", strip=True)) if h else ""

    stamp = ""
    for p in el.select("p.text-muted"):
        t = _norm(p.get_text(" ", strip=True))
        if t.lower().startswith("last update"):
            stamp = t
            break

    # Stored markup is read away from this origin, so a relative src or href
    # would be dead. Attributes only, so this cannot move a fingerprint.
    for e in el.find_all(href=True) + el.find_all(src=True):
        for attr in ("href", "src"):
            v = e.get(attr)
            if v and not v.startswith(("http://", "https://", "mailto:", "tel:",
                                       "#", "data:")):
                e[attr] = urljoin(BASE, v)

    return el, heading, _clean(el.get_text("\n", strip=True)), stamp


def _pdf_link(el) -> str:
    """The one PDF a page offers, absolutised, or "" — never a list.

    Kept OUT of `extra_meta["attachment_links"]`: `tools/workbook.py` counts
    document_url and attachment_links as one set of files and rejects any row
    holding more than one file alongside a document_url. These rows are named by
    their landing page, so the PDF is recorded under its own key instead.
    """
    if el is None:
        return ""
    for a in el.select("a[href]"):
        href = (a.get("href") or "").strip()
        if href.lower().split("?")[0].endswith(".pdf"):
            return urljoin(BASE, href)
    return ""


def _pdf_text(body: bytes, url: str) -> Tuple[str, str, dict]:
    """Extracted text for one PDF, and how much of it the extractor actually read.

    Uses the orchestrator's own extractor so the text stored here is the same
    text it would have produced (`orchestrator.py:273`), not a second opinion.
    """
    tmp = pathlib.Path(tempfile.mkstemp(suffix=".pdf")[1])
    try:
        tmp.write_bytes(body)
        try:
            # Heavy import (cv2, pdfplumber, pytesseract) — kept off module load
            # so importing this crawler stays cheap for anything that only parses.
            from processor.Text_Extractor import OCRProcessor
            text, meta = OCRProcessor.extract_text_from_pdf_smart(pdf_path=str(tmp))
            # NOT `is_ocr_available()`. That is true when ANY traineddata is
            # present, so a deployment with only `eng` reports OCR as working and
            # then OCRs Arabic pages with an English model. Recorded per row
            # rather than assumed — it is a property of the environment the crawl
            # ran in, not of the document.
            ocr_langs = OCRProcessor.ocr_langs()
            ocr_ok = "ara" in ocr_langs.split("+")
        except Exception as e:
            logger.warning("LMRA pdf extraction failed %s: %s", url[:80], e)
            return "", "extract-failed", {}
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass
    meta = meta or {}
    total = int(meta.get("total_pages") or 0)
    good = int(meta.get("good_pages") or 0)
    ocred = int(meta.get("ocr_pages") or 0)

    # A page routed to OCR contributes NOTHING when the engine has no model for
    # its script, but `extract_text_from_pdf_smart` still counts it in
    # `good_pages` and logs it as "OK (0 chars)". Subtract those to get the pages
    # actually read.
    #
    # THE SAME FILE, MEASURED BOTH WAYS on 2026-08-20, is why this guard is not
    # theoretical. The 3.13 MB Arabic annex behind `market-plan (1).pdf` is 9
    # scanned pages, 8 of which need OCR:
    #
    #   ocr_langs 'ara+eng'   15,727 chars, 12,207 Arabic (98.3%) — the resolution
    #   ocr_langs 'eng'       13,299 chars,      0 Arabic — Latin transliteration
    #                         noise, "PAGE 1 o ° 7 ve a 2023 oJo1 6 yprrast!"
    #
    # The two are the same length to within 20%, so NOTHING DOWNSTREAM COULD TELL
    # THEM APART. Stored, the second would BE the resolution: it would
    # fingerprint, promote and reach the analyser as the text of the law. The
    # arithmetic above is what refuses it, and there is nothing to tune — an
    # English PDF with an embedded text layer has ocr_pages == 0 and is
    # unaffected, which is how the Labour Law reads 52 of 52 pages.
    #
    # WHICH BRANCH YOU GET DEPENDS ON THE OCR LANGUAGES AVAILABLE TO THE
    # PROCESS, which is deployment setup — see `.env` — so `ocr_langs` is
    # recorded on every row rather than assumed.
    usable = good - (0 if ocr_ok else ocred)
    info = {"pages": total, "good_pages": good, "ocr_pages": ocred,
            "ocr_available": ocr_ok, "ocr_langs": ocr_langs,
            "pages_read": max(usable, 0)}
    if usable <= 0:
        logger.warning("LMRA pdf %s: %d/%d pages needed OCR and OCR is %s — "
                       "no page was read, discarding the extractor's output",
                       url[-45:], ocred, total,
                       "available" if ocr_ok else "NOT installed for 'ara'")
        return "", "no-pages-read", info
    return _clean(text), "pdf", info


# --------------------------------------------------------------------------- #
#  one /en/legal/category/<id> listing                                        #
# --------------------------------------------------------------------------- #

class LMRALegalCategoryCrawler:
    """One LMRA legislation category. `category_url` and `category_title` come
    from the YAML so a site retitle cannot silently reshape `doc_path`."""

    def __init__(
        self,
        regulator: str,
        source_system: str,
        category_id: str,
        category_url: str,
        category_title: str,
        category: Optional[str] = None,
        timeout: int = 45,
        delay: float = 1.0,
        page_cache_dir: Optional[str] = None,
    ):
        self.regulator = regulator
        self.source_system = source_system
        self.category_id = str(category_id)
        self.category_url = category_url
        self.category_title = category_title
        self.category = category or category_title
        self.timeout = timeout
        # Politeness, not throughput. Two hosts in this library are blocked for
        # automated access from this address, and neither block was about volume.
        self.delay = float(delay)
        # Dev-only read-through cache, also read from LMRA_PAGE_CACHE_DIR. Kept
        # out of the yml: a cache serving production is a stale law reporting
        # `unchanged` after an amendment.
        self.page_cache_dir = page_cache_dir or os.environ.get("LMRA_PAGE_CACHE_DIR") or None
        self.last_result: dict = {}
        self._session: Optional[requests.Session] = None

    @property
    def source_names(self) -> List[str]:
        return [self.category_title]

    def _http(self) -> requests.Session:
        if self._session is None:
            self._session = _session()
        return self._session

    def _fetch(self, url: str) -> Tuple[str, int, str]:
        return _get(url, self._http(), self.timeout, self.delay, self.page_cache_dir)

    def _pdf_of(self, pdf_url: str) -> Tuple[str, str, dict]:
        """The text of a stub page's attachment. Never raises: one unreadable
        attachment is a document without text, not a failed category."""
        r = _get_bytes(pdf_url, self._http(), self.timeout * 4, self.delay)
        if r is None:
            return "", "pdf-unreachable", {}
        if r.status_code != 200 or r.content[:5] != b"%PDF-":
            return "", f"pdf-http-{r.status_code}", {}
        text, origin, info = _pdf_text(r.content, pdf_url)
        # The static files are the ONE place on this host that carries a real
        # change stamp; the dynamic pages carry none. Recorded, not hashed.
        info["last_modified"] = r.headers.get("Last-Modified") or ""
        info["bytes"] = len(r.content)
        return text, origin, info

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []

        html, status, origin = self._fetch(self.category_url)
        if status != 200:
            raise RuntimeError(
                f"LMRA {self.category_title!r}: category page {self.category_url} "
                f"returned HTTP {status}. Nothing may be classified from this.")

        el, heading, _text, _stamp = _content(html)
        if el is None:
            raise RuntimeError(
                f"LMRA {self.category_title!r}: {CONTENT!r} matched nothing at "
                f"{self.category_url} — the template has moved.")
        if heading and _norm(heading) != _norm(self.category_title):
            # `/en/legal/category/<anything>` answers 200 with a template rather
            # than a 404, so a stale id would otherwise file another category's
            # documents — or none — under this folder without failing.
            warnings.append(f"category title moved: yml has {self.category_title!r}, "
                            f"page says {heading!r} — reconcile before trusting doc_path")
            logger.warning(warnings[-1])

        rows, seen = [], set()
        for a in el.select(LIST_SELECTOR):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            url = urljoin(BASE, href)
            if urlparse(url).netloc != urlparse(BASE).netloc or url in seen:
                continue
            seen.add(url)
            badge = a.select_one(".badge")
            listed = _norm(badge.get_text(" ", strip=True)) if badge else ""
            # The `title=` attribute, not the anchor's text: the text has the
            # date badge glued to the front of it.
            label = _norm(a.get("title") or a.get_text(" ", strip=True))
            if badge and listed and label.startswith(listed):
                label = label[len(listed):].strip()
            rows.append((url, label, listed))

        if not rows:
            # An empty category is ruled `disappeared` downstream and becomes a
            # withdrawal proposal against law still in force.
            raise RuntimeError(
                f"LMRA {self.category_title!r}: {LIST_SELECTOR!r} matched no "
                f"instrument at {self.category_url}. That is a broken crawler, "
                f"not an empty category.")

        docs: List[RegulatoryDocument] = []
        empty = 0
        for url, label, listed in rows:
            dhtml, dstatus, dorigin = self._fetch(url)
            dnode, text, page_title, stamp, pdf = None, "", "", "", ""
            pdf_meta: dict = {}
            if dstatus != 200:
                logger.warning("LMRA %s: %s -> HTTP %s", self.category_title, url, dstatus)
                dorigin = f"http-{dstatus}"
            else:
                dnode, page_title, text, stamp = _content(dhtml)
                pdf = _pdf_link(dnode)
                if len(text) < _MIN_DOC_CHARS:
                    # A STUB PAGE, AND THE INSTRUMENT IS ITS ATTACHMENT. Most
                    # detail pages here carry the whole decree as HTML, but one
                    # does not: Resolution 8/2023 (the National Labour Market
                    # Plan) renders 267 characters — its heading, its source line
                    # and a link to a 3.13 MB PDF. Falling through to that file
                    # is the difference between storing the plan and storing a
                    # link to it, and it costs a download only on the pages that
                    # have no text of their own.
                    if pdf:
                        logger.info("LMRA %s: %s is a stub (%d chars) — reading "
                                    "its PDF instead", self.category_title, url,
                                    len(text))
                        text, dorigin, pinfo = self._pdf_of(pdf)
                        pdf_meta = pinfo
                    if len(text) < _MIN_DOC_CHARS:
                        logger.warning("LMRA %s: %s yielded %d chars, below %d — "
                                       "treated as no text", self.category_title,
                                       url, len(text), _MIN_DOC_CHARS)
                        text = ""
                        dorigin = dorigin if pdf else "too-short"

            title = label or page_title or url.rsplit("/", 1)[-1]
            if page_title and label and _norm(page_title) != _norm(label):
                warnings.append(f"title differs: listing {label[:50]!r} vs page "
                                f"{page_title[:50]!r}")
            if not text:
                empty += 1

            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.category,
                title=title,
                document_url=url,
                doc_path=[self.regulator, self.source_system,
                          self.category_title, title],
                file_type="HTML",
                source_page_url=self.category_url,
                # `orch._process_versioned_doc` copies this into
                # regulation_versions.content_html. Nothing hashes it — the
                # fingerprint stays the text — so storing it cannot re-version a
                # stored row.
                document_html=str(dnode) if (text and dnode is not None) else None,
                extra_meta={
                    "crawl_source": self.category_title,
                    "lmra_category_id": self.category_id,
                    "lmra_category_url": self.category_url,
                    # THE SITE'S POSTING DATE, NOT THE INSTRUMENT'S. Resolution
                    # 1/2014 is badged 29-03-2026. It is recorded under a name
                    # that says so and deliberately NOT written to
                    # `published_date`, which readers take to mean enactment.
                    "lmra_listed_date": listed,
                    "last_update": stamp,
                    "pdf_url": pdf,
                    "content_text": text,
                    "text_chars": len(text),
                    "text_origin": dorigin,
                    "source": origin,
                    **{f"pdf_{k}": v for k, v in pdf_meta.items()},
                },
            ))
            if cap and len(docs) >= cap:
                break

        # Single exit. The text is the fingerprint where there is text;
        # `stamp_content_hashes` fills the `document_url|title` floor for the
        # rest, and never overwrites a hash already set here.
        for d in docs:
            text = d.extra_meta["content_text"]
            if text:
                d.content_hash = content_key(text)
            d.extra_meta["content_hash_basis"] = "text" if text else "document_url|title"
        docs = stamp_content_hashes(docs)

        if empty:
            warnings.append(f"{empty} page(s) yielded no text and fell back to the "
                            f"document_url|title fingerprint")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.category_title: len(docs)},
            "source": origin,
        }
        logger.info("LMRALegalCategoryCrawler %s: %d document(s), %s",
                    self.category_title, len(docs), origin)
        return docs


# --------------------------------------------------------------------------- #
#  one law that has a landing page of its own                                 #
# --------------------------------------------------------------------------- #

class LMRAInstrumentCrawler:
    """ONE law, ONE row, named by its landing page.

    An article is not an instrument, so LMRA Law's 47 `Article N` sub-pages are
    assembled into a single document rather than stored as 47. The row is named
    by `/en/page/show/<id>` and not by the PDF, so re-issuing the PDF under a new
    filename cannot read as one `new` plus one `disappeared`.
    """

    def __init__(
        self,
        regulator: str,
        source_system: str,
        page_url: str,
        instrument_title: str,
        folder_title: str,
        text_from: str,
        expected_heading: Optional[str] = None,
        min_articles: int = 1,
        category: Optional[str] = None,
        timeout: int = 60,
        delay: float = 1.0,
        page_cache_dir: Optional[str] = None,
    ):
        if text_from not in ("articles", "pdf"):
            raise ValueError(f"text_from must be 'articles' or 'pdf', not {text_from!r}")
        self.regulator = regulator
        self.source_system = source_system
        self.page_url = page_url
        self.instrument_title = instrument_title
        self.folder_title = folder_title
        self.text_from = text_from
        # WHAT THE PAGE SAYS, NOT WHAT WE CALL IT. The guard below asks "has this
        # page moved?", so it must compare against a recorded observation of the
        # page — LMRA headings its Labour Law page `Labour Law` while the
        # instrument is `LAW NO. 36 OF 2012 ...`. Comparing the heading against
        # our chosen title warned on every run about a difference that is
        # permanent and correct, which is how a guard becomes noise and then gets
        # ignored. Defaults to the title, which is right for LMRA Law.
        self.expected_heading = expected_heading or instrument_title
        # A floor, not a count. Naming the exact 47 here would make a single
        # article being added a crawl failure; zero articles is the failure.
        self.min_articles = int(min_articles)
        self.category = category or folder_title
        self.timeout = timeout
        self.delay = float(delay)
        self.page_cache_dir = page_cache_dir or os.environ.get("LMRA_PAGE_CACHE_DIR") or None
        self.last_result: dict = {}
        self._session: Optional[requests.Session] = None

    @property
    def source_names(self) -> List[str]:
        return [self.folder_title]

    def _http(self) -> requests.Session:
        if self._session is None:
            self._session = _session()
        return self._session

    def _fetch(self, url: str) -> Tuple[str, int, str]:
        return _get(url, self._http(), self.timeout, self.delay, self.page_cache_dir)

    def _from_articles(self, el, warnings: List[str]) -> Tuple[str, dict, str]:
        """The law, assembled from its article sub-pages in the site's own order.

        The links are read from the landing page's "Pages Index", which
        `_content` has already stripped out of the landing page's own text — so
        they are read from a SECOND parse that keeps it.
        """
        links, seen = [], set()
        for a in el.select(ARTICLE_SELECTOR):
            label = _norm(a.get_text(" ", strip=True))
            href = (a.get("href") or "").strip()
            if not href or not _ARTICLE_LABEL.match(label):
                continue
            url = urljoin(BASE, href)
            if url in seen:
                continue
            seen.add(url)
            links.append((url, label))

        if len(links) < self.min_articles:
            raise RuntimeError(
                f"LMRA {self.instrument_title!r}: {ARTICLE_SELECTOR!r} matched "
                f"{len(links)} article sub-page(s) at {self.page_url}, below the "
                f"floor of {self.min_articles}. Storing the law with no text "
                f"would report it `modified` and lose its articles silently.")

        parts, parts_html, failed = [], [], []
        for url, label in links:
            html, status, _o = self._fetch(url)
            if status != 200:
                failed.append(f"{label} (HTTP {status})")
                continue
            node, heading, text, _s = _content(html)
            if not text:
                failed.append(f"{label} (no text)")
                continue
            # The article's own heading is kept: `Article 1` is the site's label
            # and `Definitions` is what the article is about. Losing either makes
            # the assembled law harder to read than the site.
            # THE SAME BLOCK'S MARKUP comes from the SAME parse (`node` above),
            # so `document_html` and `content_text` cannot describe different
            # content. That is the whole reason it is collected here instead of
            # letting the landing page stand in for the law.
            parts_html.append(str(node))
            parts.append(f"{label} — {heading}\n{text}" if heading else f"{label}\n{text}")

        if failed:
            # ALL OR NOTHING, AND LOUDLY.
            #
            # This is the whole reason the retries above exist. The fingerprint is
            # taken over this concatenation, so an article that silently drops out
            # SHORTENS THE TEXT, MOVES THE HASH, and classifies the Act
            # `modified` — two version rows written and the law stored with an
            # article missing. The next clean run flips it back: `modified`
            # again, two more rows. Nothing downstream can tell that apart from a
            # real amendment, because a hash cannot carry an asterisk.
            #
            # There is no third option. 46 of 47 articles is a PLAUSIBLE law,
            # which is the worst kind of wrong.
            #
            # Raising is the safe direction here, verified rather than assumed:
            # `CompositeCrawler.fetch_documents` catches per-source exceptions,
            # logs and carries on, so the cost is this one folder for this one
            # run — and the completeness gate then treats the run as
            # untrustworthy, which means it may not withdraw anything.
            raise RuntimeError(
                f"LMRA {self.instrument_title!r}: {len(failed)} of {len(links)} "
                f"article sub-page(s) unreadable after {RETRY_ATTEMPTS} attempts "
                f"each: {failed[:5]}. Refusing to fingerprint a partial law — a "
                f"short assembly is indistinguishable from an amendment.")

        return ("\n\n".join(parts),
                {"articles": len(links), "articles_read": len(parts)},
                _ILLEGAL_XLSX.sub(" ", "\n".join(parts_html)))

    def _from_pdf(self, pdf_url: str, warnings: List[str]) -> Tuple[str, dict, None]:
        """The law, from the file its landing page links to."""
        if not pdf_url:
            warnings.append("landing page offers no PDF and text_from is 'pdf'")
            return "", {}, None
        r = _get_bytes(pdf_url, self._http(), self.timeout * 3, self.delay)
        if r is None:
            warnings.append(f"pdf {pdf_url[-40:]} unreachable after "
                            f"{RETRY_ATTEMPTS} attempts")
            return "", {}, None
        if r.status_code != 200 or r.content[:5] != b"%PDF-":
            warnings.append(f"pdf {pdf_url[-40:]} -> HTTP {r.status_code}")
            return "", {}, None
        text, _origin, info = _pdf_text(r.content, pdf_url)
        # The static files are the ONE place on this host that carries a real
        # change stamp; the dynamic pages carry none. Recorded, not hashed.
        info["pdf_last_modified"] = r.headers.get("Last-Modified") or ""
        info["pdf_sha256"] = hashlib.sha256(r.content).hexdigest()[:16]
        info["pdf_bytes"] = len(r.content)
        # NO HTML, DELIBERATELY. A PDF has no HTML rendering, so `None` is the
        # honest answer: a blank column is visibly blank, a wrong one is not.
        return text, info, None

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        warnings: List[str] = []

        html, status, origin = self._fetch(self.page_url)
        if status != 200:
            raise RuntimeError(
                f"LMRA {self.instrument_title!r}: landing page {self.page_url} "
                f"returned HTTP {status}. Nothing may be classified from this — "
                f"this folder holds one document, so an empty run proposes the "
                f"whole law for withdrawal.")

        el, heading, page_text, stamp = _content(html)
        if el is None:
            raise RuntimeError(
                f"LMRA {self.instrument_title!r}: {CONTENT!r} matched nothing at "
                f"{self.page_url} — the template has moved.")
        pdf = _pdf_link(el)

        if self.text_from == "articles":
            # A SECOND parse, with the "Pages Index" still in it. `_content`
            # strips that nav so it cannot enter 47 fingerprints; the links
            # themselves are still needed, and re-parsing is cheaper and clearer
            # than teaching `_content` to hand back what it removed.
            nav = BeautifulSoup(html, "html.parser").select_one(CONTENT)
            text, info, body_html = self._from_articles(nav, warnings)
        else:
            text, info, body_html = self._from_pdf(pdf, warnings)

        if len(text) < _MIN_INSTRUMENT_CHARS:
            # NOT a raise. This folder holds exactly one row, and a raise here
            # would empty it — which downstream reads as the law having been
            # withdrawn. The landing page's own text is stored instead, loudly.
            warnings.append(f"assembled text is {len(text)} chars, below "
                            f"{_MIN_INSTRUMENT_CHARS} — stored the landing page's "
                            f"own text instead; the fingerprint is NOT the law")
            logger.error("LMRA %s: %s", self.instrument_title, warnings[-1])
            text = page_text
            # The text now IS the landing page, so its markup is the matching
            # rendering. This is the one case where storing it is correct.
            body_html = _ILLEGAL_XLSX.sub(" ", str(el))
            info["degraded"] = True

        doc = RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self.category,
            title=self.instrument_title,
            document_url=self.page_url,
            doc_path=[self.regulator, self.source_system,
                      self.folder_title, self.instrument_title],
            file_type="HTML",
            source_page_url=self.page_url,
            # The markup of whatever the text was actually taken FROM: the
            # articles, or nothing at all for a PDF. NOT the landing page — that
            # is a heading and a nav list, and `orch._process_versioned_doc`
            # copies this straight into regulation_versions.content_html, where
            # it would claim to be the law.
            document_html=body_html or None,
            extra_meta={
                "crawl_source": self.folder_title,
                "lmra_page_heading": heading,
                "last_update": stamp,
                "pdf_url": pdf,
                "text_from": self.text_from,
                "content_text": text,
                "text_chars": len(text),
                "text_origin": self.text_from,
                "source": origin,
                **{f"{self.text_from}_{k}": v for k, v in info.items()},
            },
        )
        if heading and _norm(heading) != _norm(self.expected_heading):
            warnings.append(f"landing page heading moved: yml expected "
                            f"{self.expected_heading!r}, page says {heading!r} — "
                            f"reconcile before trusting doc_path")
            logger.warning(warnings[-1])

        # Single exit, for a list of one — the same rule as everywhere else, so
        # this cannot drift if a second document is ever added here.
        docs = [doc]
        for d in docs:
            t = d.extra_meta["content_text"]
            if t:
                d.content_hash = content_key(t)
            d.extra_meta["content_hash_basis"] = "text" if t else "document_url|title"
        docs = stamp_content_hashes(docs)

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.folder_title: len(docs)},
            "source": origin,
        }
        logger.info("LMRAInstrumentCrawler %s: %d chars from %s, %s",
                    self.instrument_title, len(text), self.text_from, origin)
        return docs
