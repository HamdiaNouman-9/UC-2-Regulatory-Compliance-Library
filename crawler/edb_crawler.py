"""EDBCrawler — one category of Bahrain's business laws, read with `requests`.

A law here is an HTML page, not a file: the whole instrument is the page text, so
the crawler stores that text and fingerprints it. One instance = one category.
"""

from __future__ import annotations

import hashlib
import logging
import os
import pathlib
import re
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

BASE = "https://bahrainbusinesslaws.com"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

LAW_SELECTOR = 'a[href*="/laws/"]'

# The site's own index of every law, used as a completeness cross-check. It names
# all 64 in one request but carries no categories and no dates, and is
# byte-identical when a law is amended in place — a companion to the crawl, never
# a replacement.
INDEX_URL = f"{BASE}/all-laws/all-laws"

# The law text. A page carries two `div.col-md-12` and the second is empty, so
# the largest by text wins.
BODY_SELECTOR = "main section div.container div.col-md-12"

# October CMS's not-found template. The status code is honest on this host, but
# the body is checked too so a 200-with-error-page cannot pass as a law.
_NOT_FOUND = "the requested page cannot be found"

# openpyxl raises on these instead of escaping them, and it raises inside save()
# — after the whole crawl has run. Tab, newline and carriage return are legal.
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# Below this a "law" is a template or a stub, not an instrument. The smallest
# real law measured 2,435 characters.
_MIN_LAW_CHARS = 800

# A category page holding this many laws is the site's full index, not a
# category. The largest real category measured 13 of the 64.
_FULL_INDEX_MIN = 30


def _cache_file(page_cache_dir: Optional[str], url: str) -> Optional[pathlib.Path]:
    if not page_cache_dir:
        return None
    d = pathlib.Path(page_cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    return d / (hashlib.sha256(url.encode()).hexdigest()[:16] + ".html")


def _get(url: str, session: requests.Session, timeout: int, delay: float,
         page_cache_dir: Optional[str]) -> Tuple[str, int, str]:
    """One page. Shared by the crawler and the inventory check so both honour the
    same dev cache and the same politeness delay."""
    cp = _cache_file(page_cache_dir, url)
    if cp is not None and cp.exists():
        return cp.read_text("utf-8", "replace"), 200, "page-cache"
    r = session.get(url, timeout=timeout)
    if delay:
        time.sleep(delay)
    if cp is not None and r.status_code == 200:
        cp.write_text(r.text, encoding="utf-8")
    return r.text, r.status_code, "live"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _same_title(a: str, b: str) -> bool:
    """Titles compared after NFKC. One law's heading carries a U+00A0 where its
    index label has a space, so a raw comparison warns forever about nothing."""
    norm = lambda s: " ".join(unicodedata.normalize("NFKC", s or "").split())
    return norm(a) == norm(b)


def slug_of(url: str) -> str:
    """The law's own slug — the stable half of a /laws/<slug> url."""
    return (url or "").rstrip("/").rsplit("/", 1)[-1]


def index_slugs(session: Optional[requests.Session] = None, timeout: int = 45,
                delay: float = 1.0,
                page_cache_dir: Optional[str] = None) -> set:
    """Every law slug on /all-laws/all-laws. Raises if the page cannot be read —
    a silently empty index would make the completeness check pass on nothing."""
    page_cache_dir = page_cache_dir or os.environ.get("EDB_PAGE_CACHE_DIR") or None
    html, status, _ = _get(INDEX_URL, session or _session(), timeout, delay,
                           page_cache_dir)
    if status != 200 or _NOT_FOUND in html[:2000].lower():
        raise RuntimeError(f"EDB inventory index {INDEX_URL} returned HTTP {status}")
    soup = BeautifulSoup(html, "html.parser")
    slugs = {slug_of(urljoin(BASE, a.get("href") or ""))
             for a in soup.select(LAW_SELECTOR) if (a.get("href") or "").strip()}
    slugs.discard("")
    if not slugs:
        raise RuntimeError(f"EDB inventory index {INDEX_URL} listed no law")
    return slugs


def inventory_fingerprint(slugs) -> str:
    """Order-immune hash of the law set, so a CMS re-sort is not a change.

    sha256 of the sorted `/laws/<slug>` paths, newline-joined — the recipe behind
    the recorded 2026-08-19 value ccbce110f71e9c3b. Keep it, or a stored baseline
    stops being comparable.
    """
    joined = "\n".join(sorted(f"/laws/{s}" for s in slugs))
    return hashlib.sha256(joined.encode()).hexdigest()[:16]


def verify_inventory_complete(docs, session: Optional[requests.Session] = None,
                              timeout: int = 45, delay: float = 1.0,
                              page_cache_dir: Optional[str] = None) -> dict:
    """The eight category crawls against the site's own index of all 64.

    The completeness gate only catches a LARGE drop, measured per category. This
    catches a ONE-law drop, because it compares against the publisher's own list
    rather than a count of our own. Raises on any mismatch.
    """
    index = index_slugs(session, timeout, delay, page_cache_dir)
    crawled = {slug_of(getattr(d, "document_url", "")) for d in docs}
    crawled.discard("")
    missing = sorted(index - crawled)      # the index has it, no category listed it
    extra = sorted(crawled - index)        # a category listed it, the index does not
    verdict = {"index": len(index), "crawled": len(crawled),
               "missing_from_crawl": missing, "not_in_index": extra,
               "verdict": "OK" if not missing and not extra else "MISMATCH"}
    if missing or extra:
        raise RuntimeError(
            f"EDB inventory mismatch: the index lists {len(index)} law(s), the "
            f"crawl produced {len(crawled)}. Absent from the crawl: "
            f"{missing[:5]}. Absent from the index: {extra[:5]}. A category page "
            f"has silently dropped or gained a law — do not trust this run.")
    return verdict


class EDBCrawler:
    """One EDB law category. `category_url` and `category_title` come from the
    YAML so a site retitle cannot silently reshape `doc_path`."""

    def __init__(
        self,
        regulator: str,
        source_system: str,
        category_slug: str,
        category_url: str,
        category_title: str,
        category: Optional[str] = None,
        timeout: int = 45,
        delay: float = 1.0,
        page_cache_dir: Optional[str] = None,
    ):
        self.regulator = regulator
        self.source_system = source_system
        self.category_slug = category_slug
        self.category_url = category_url
        self.category_title = category_title
        self.category = category or category_title
        self.timeout = timeout
        # Politeness, not throughput. Two hosts in this library are blocked for
        # automated access from this address, and neither block was about volume.
        self.delay = float(delay)
        # Dev-only read-through cache, also read from EDB_PAGE_CACHE_DIR. Kept out
        # of the yml: a cache serving production is a stale law reporting
        # `unchanged` after an amendment.
        self.page_cache_dir = page_cache_dir or os.environ.get("EDB_PAGE_CACHE_DIR") or None
        self.last_result: dict = {}
        self._session: Optional[requests.Session] = None

    def _http(self) -> requests.Session:
        """One keep-alive session for the category page and its laws."""
        if self._session is None:
            self._session = _session()
        return self._session

    @property
    def source_names(self) -> List[str]:
        return [self.category_title]

    # ------------------------------------------------------------------ #

    def _fetch(self, url: str) -> Tuple[str, int, str]:
        """Return (html, status, origin). A cache hit never touches the network."""
        return _get(url, self._http(), self.timeout, self.delay,
                    self.page_cache_dir)

    def _body(self, html: str) -> Tuple[str, str]:
        """The law's text AND the same block's HTML, from one parse.

        Both come from the SAME block, so they can never describe different
        content. The HTML is stored because a law here IS an HTML page — that is
        its source form, not a by-product.
        """
        soup = BeautifulSoup(html, "html.parser")
        for t in soup(["script", "style", "noscript"]):
            t.decompose()
        blocks = soup.select(BODY_SELECTOR)
        if not blocks:
            return "", ""
        best = max(blocks, key=lambda b: len(b.get_text(" ", strip=True)))

        # Stored markup is read away from this origin, so a relative src or href
        # would be dead. Today that is one `<img>`; the 19 `<a>` are all in-page
        # `#` anchors and are deliberately left alone — absolutising those would
        # break them. Attributes only, so this cannot move a fingerprint.
        for el in best.find_all(href=True) + best.find_all(src=True):
            for attr in ("href", "src"):
                v = el.get(attr)
                if v and not v.startswith(("http://", "https://", "mailto:",
                                           "tel:", "#", "data:")):
                    el[attr] = urljoin(BASE, v)

        text = re.sub(r"[ \t]+", " ", best.get_text("\n", strip=True))
        text = re.sub(r"\n{3,}", "\n\n", text)
        # NFKC folds the no-break spaces this CMS emits, so a template toggling
        # `&nbsp;` cannot move a fingerprint on its own. Sanitise second, so the
        # text that is hashed is the text that reaches the workbook.
        text = unicodedata.normalize("NFKC", text)
        # The HTML is NOT NFKC-folded: nothing hashes it, and it should stay what
        # the site served. It is still sanitised, or openpyxl raises inside save().
        return (_ILLEGAL_XLSX.sub(" ", text).strip(),
                _ILLEGAL_XLSX.sub(" ", str(best)))

    def _body_text(self, html: str) -> str:
        """Kept as its own name — the recon and Arabic tools call it directly."""
        return self._body(html)[0]

    def _page_title(self, html: str) -> str:
        """The page's own heading. NOT scoped to `main`: this site puts its only
        `<h2>` outside it, so `main h2` matched nothing on all 72 pages and both
        title guards below were silently inert."""
        soup = BeautifulSoup(html, "html.parser")
        h = soup.select_one("h1, h2")
        return h.get_text(" ", strip=True) if h else ""

    def _law_text(self, url: str) -> Tuple[str, str, str, str]:
        """Return (text, body_html, page_title, origin) for one law page."""
        html, status, origin = self._fetch(url)
        if status != 200 or _NOT_FOUND in html[:2000].lower():
            logger.warning("EDB %s: %s -> HTTP %s / not-found page",
                           self.category_title, url, status)
            return "", "", "", f"http-{status}"
        text, body_html = self._body(html)
        if len(text) < _MIN_LAW_CHARS:
            logger.warning("EDB %s: %s yielded %d chars, below %d — treated as no text",
                           self.category_title, url, len(text), _MIN_LAW_CHARS)
            # The HTML is dropped with the text: storing markup for something
            # ruled unreadable would look like a document that has content.
            return "", "", self._page_title(html), "too-short"
        return text, body_html, self._page_title(html), origin

    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []

        html, status, origin = self._fetch(self.category_url)
        if status != 200 or _NOT_FOUND in html[:2000].lower():
            raise RuntimeError(
                f"EDB {self.category_title!r}: category page {self.category_url} "
                f"returned HTTP {status}. Nothing may be classified from this.")

        soup = BeautifulSoup(html, "html.parser")
        page_title = self._page_title(html)
        if page_title and not _same_title(page_title, self.category_title):
            warnings.append(
                f"category title moved: yml has {self.category_title!r}, page says "
                f"{page_title!r} — reconcile before trusting doc_path")
            logger.warning(warnings[-1])

        pairs, seen = [], set()
        for a in soup.select(LAW_SELECTOR):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            url = urljoin(BASE, href)
            if urlparse(url).netloc != urlparse(BASE).netloc or url in seen:
                continue
            seen.add(url)
            pairs.append((url, " ".join(a.get_text(" ", strip=True).split())))

        if not pairs:
            # An empty category is ruled `disappeared` downstream and becomes a
            # withdrawal proposal against law still in force.
            raise RuntimeError(
                f"EDB {self.category_title!r}: {LAW_SELECTOR!r} matched no law at "
                f"{self.category_url}. That is a broken crawler, not an empty category.")

        # /all-laws/<unknown-slug> answers 200 with the FULL 64-law listing, not
        # a 404 — measured. So a category slug going stale would file all 64 laws
        # under this one category instead of failing. The page's own heading is
        # the tell, and it is checked above; this catches it even if that moves.
        if page_title and not _same_title(page_title, self.category_title) \
                and len(pairs) >= _FULL_INDEX_MIN:
            raise RuntimeError(
                f"EDB {self.category_title!r}: {self.category_url} returned "
                f"{len(pairs)} laws under the heading {page_title!r} — this is the "
                f"site's full index, not a category. The slug is probably stale.")

        docs: List[RegulatoryDocument] = []
        empty = 0
        for url, label in pairs:
            text, body_html, law_title, torigin = self._law_text(url)
            title = label or law_title or url.rsplit("/", 1)[-1].replace("-", " ")
            if law_title and label and not _same_title(law_title, label):
                warnings.append(f"title differs: index {label!r} vs page {law_title!r}")
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
                # The law's source markup. `orch._process_versioned_doc` copies
                # this into regulation_versions.content_html, so both columns are
                # filled from here. Nothing hashes it — the fingerprint stays the
                # text — so adding it cannot re-version a stored row.
                document_html=body_html or None,
                extra_meta={
                    "crawl_source": self.category_title,
                    "edb_category_slug": self.category_slug,
                    "edb_category_url": self.category_url,
                    "content_text": text,
                    "text_chars": len(text),
                    "html_chars": len(body_html),
                    "text_origin": torigin,
                    "source": origin,
                },
            ))
            if cap and len(docs) >= cap:
                break

        # Single exit. The text hash is the fingerprint; `stamp_content_hashes`
        # fills the document_url|title floor only where nothing extracted.
        for d in docs:
            d.content_hash = content_key(d.extra_meta["content_text"])
            d.extra_meta["content_hash_basis"] = (
                "text" if d.content_hash else "document_url|title")
        docs = stamp_content_hashes(docs)

        if empty:
            warnings.append(f"{empty} law page(s) yielded no text and fell back to "
                            f"the document_url|title fingerprint")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.category_title: len(docs)},
            "source": origin,
        }
        logger.info("EDBCrawler %s: %d law(s), %s", self.category_title, len(docs), origin)
        return docs
