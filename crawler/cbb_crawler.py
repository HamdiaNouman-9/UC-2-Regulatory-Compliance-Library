"""
CBB Crawler v2 — MERGED VERSION
=================================
Integrates:
  - NEW sidebar crawler (cbb_rulebook_crawler.py) for Rulebook volumes
  - NEW AML/CorpGov crawler (Aml_crawler_v2.py) for AML Law + Corp Gov
  - All existing modes unchanged (1, 3, 4, 5)

MODE OVERVIEW:
  Mode 1  — CBB Regulations and Resolutions
  Mode 2a — AML Law           ← NOW uses Aml_crawler_v2
  Mode 2b — Corporate Gov     ← NOW uses Aml_crawler_v2
  Mode 2c — Rulebook Volumes  ← NOW uses cbb_rulebook_crawler sidebar
  Mode 3  — Laws & Regulations (cbb.gov.bh accordion)
  Mode 4  — CBB Capital Market Regulations
  Mode 5  — Compliance (cbb.gov.bh #aml/#eofi)
"""

import hashlib
import requests
from bs4 import BeautifulSoup, Tag
import re
import time
import logging
from datetime import datetime
from urllib.parse import urljoin
from typing import List, Optional, Dict, Any

try:
    from crawler.crawler import BaseCrawler
    from models.models import RegulatoryDocument
except ImportError:
    class BaseCrawler:
        pass
    from dataclasses import dataclass, field
    @dataclass
    class RegulatoryDocument:
        regulator: str = ""
        source_system: str = ""
        category: str = ""
        title: str = ""
        document_url: str = ""
        urdu_url: Any = None
        published_date: Any = None
        reference_no: Any = None
        department: Any = None
        year: Any = None
        source_page_url: str = ""
        file_type: Any = None
        document_html: str = ""
        extra_meta: dict = field(default_factory=dict)
        doc_path: list = field(default_factory=list)

# ── New crawler imports ────────────────────────────────────────────────────────
try:
    from cbb_test_crawlers.cbb_rulebook_crawler import (
        crawl_rulebook_sidebar, RulebookDoc, SIDEBAR_SEED,
    )
    from cbb_test_crawlers.Aml_crawler_v2 import (
        crawl_rulebook as crawl_aml_corpgov,
        RulebookDocument as AmlRulebookDocument,
        SOURCES as AML_SOURCES,
    )
except ImportError:
    from cbb_rulebook_crawler import crawl_rulebook_sidebar, RulebookDoc, SIDEBAR_SEED
    from Aml_crawler_v2 import (
        crawl_rulebook as crawl_aml_corpgov,
        RulebookDocument as AmlRulebookDocument,
        SOURCES as AML_SOURCES,
    )

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_URL                       = "https://cbben.thomsonreuters.com"
CBB_RULEBOOK_INDEX             = "https://cbben.thomsonreuters.com/cbb-rulebook"
CAPITAL_MARKET_REGULATIONS_URL = "https://cbben.thomsonreuters.com/cbb-capital-market-regulations"
CAPITAL_MARKET_CATEGORY        = "CBB Capital Market Regulations"
LAWS_REGULATIONS_URL           = "https://www.cbb.gov.bh/laws-regulations/"
COMPLIANCE_URL                 = "https://www.cbb.gov.bh/compliance/"
COMPLIANCE_BASE                = "https://www.cbb.gov.bh"

CATEGORY      = "Compliance"
SOURCE_SYSTEM = "CBB-Compliance"

REQUEST_DELAY = 1.5
MAX_RETRIES   = 3
REGULATOR     = "Central Bank of Bahrain"

STATIC_BASE_URLS = {
    "CBB Regulations and Resolutions": (
        "https://cbben.thomsonreuters.com/rulebook/cbb-regulations-and-resolutions",
        "resolutions",
    ),
}

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
})


# ═══════════════════════════════════════════════════════════════════════════════
#  FACTORY FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_content_hash(extra_meta: Dict) -> str:
    content = (
        extra_meta.get("content_text")
        or extra_meta.get("document_url", "")
        or ""
    )
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def create_cbb_document(**kwargs) -> RegulatoryDocument:
    if "extra_meta" not in kwargs or kwargs["extra_meta"] is None:
        kwargs["extra_meta"] = {}
    doc = RegulatoryDocument(**kwargs)
    doc.extra_meta["content_hash"] = _compute_content_hash(doc.extra_meta)
    return doc


# ─── Shared HTTP helper ────────────────────────────────────────────────────────
def _fetch(url: str) -> Optional[BeautifulSoup]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            log.info(f"✓ Fetched [{resp.status_code}] {url}")
            return BeautifulSoup(resp.content, "lxml")
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    log.error(f"✗ All retries exhausted for {url}")
    return None


def _make_absolute(block: BeautifulSoup, base: str = BASE_URL) -> None:
    for tag in block.find_all(href=True):
        if tag["href"].startswith("/"):
            tag["href"] = urljoin(base, tag["href"])
    for tag in block.find_all(src=True):
        if tag["src"].startswith("/"):
            tag["src"] = urljoin(base, tag["src"])


def _abs(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(COMPLIANCE_BASE, href)


def _is_pdf(url: str) -> bool:
    return url.lower().endswith(".pdf")


# ═══════════════════════════════════════════════════════════════════════════════
#  MODE 2a/2b — AML LAW + CORPORATE GOVERNANCE (new Aml_crawler_v2)
#
#  Converts AmlRulebookDocument → RegulatoryDocument so the rest of the
#  pipeline (orchestrator, DB storage) sees a uniform object type.
# ═══════════════════════════════════════════════════════════════════════════════

SOURCE_KEY_TO_SYSTEM = {
    "aml":     "CBB-AML-LAW",
    "corpgov": "CBB-CORPGOV",
}

def _aml_doc_to_regulatory(doc: AmlRulebookDocument) -> RegulatoryDocument:
    """Convert AmlRulebookDocument → RegulatoryDocument."""
    source_system = SOURCE_KEY_TO_SYSTEM.get(doc.source_key, f"CBB-{doc.source_key.upper()}")
    return create_cbb_document(
        regulator       = REGULATOR,
        source_system   = source_system,
        category        = doc.category,
        title           = doc.title,
        document_url    = doc.url,
        source_page_url = doc.url,
        document_html   = doc.content_html,   # AML uses content_html → maps to document_html
        doc_path        = doc.path,            # AML uses path → maps to doc_path
        extra_meta      = {
            "content_text": doc.content_text,
            "content_hash": doc.content_hash,
            "source_key":   doc.source_key,
            "row_type":     doc.row_type,
            "depth":        doc.depth,
            "is_folder":    doc.row_type == "F",
        },
    )


def _crawl_aml_law() -> List[RegulatoryDocument]:
    """Mode 2a: Bahrain Anti Money Laundering Law 2001."""
    log.info("=== Crawling [aml]: Bahrain Anti Money Laundering Law 2001 ===")
    raw_docs = crawl_aml_corpgov("aml")
    docs = [_aml_doc_to_regulatory(d) for d in raw_docs]
    log.info(f"  AML Law: {len(docs)} documents")
    return docs


def _crawl_corpgov() -> List[RegulatoryDocument]:
    """Mode 2b: Corporate Governance Code."""
    log.info("=== Crawling [corpgov]: Corporate Governance Code ===")
    raw_docs = crawl_aml_corpgov("corpgov")
    docs = [_aml_doc_to_regulatory(d) for d in raw_docs]
    log.info(f"  CorpGov: {len(docs)} documents")
    return docs


# ═══════════════════════════════════════════════════════════════════════════════
#  MODE 2c — RULEBOOK VOLUMES (new sidebar crawler)
#
#  Converts RulebookDoc → RegulatoryDocument.
# ═══════════════════════════════════════════════════════════════════════════════

def _rulebook_doc_to_regulatory(doc: RulebookDoc) -> RegulatoryDocument:
    """Convert RulebookDoc (sidebar crawler) → RegulatoryDocument."""
    return create_cbb_document(
        regulator       = REGULATOR,
        source_system   = "CBB-Rulebook",
        category        = doc.doc_path[1] if len(doc.doc_path) > 1 else "CBB Rulebook",
        title           = doc.title,
        document_url    = doc.url,
        source_page_url = doc.url,
        document_html   = doc.document_html,
        doc_path        = doc.doc_path,
        extra_meta      = {
            "pdf_link":     doc.extra_meta.get("pdf_link"),
            "pdf_links":    doc.extra_meta.get("pdf_links", []),
            "faq_link":     doc.extra_meta.get("faq_link"),
            "content_text": doc.content_text,
            "content_hash": doc.content_hash,
            "is_folder":    doc.is_folder,
            "depth":        doc.depth,
        },
    )


def _crawl_rulebook_volumes(request_delay: float = REQUEST_DELAY) -> List[RegulatoryDocument]:
    """Mode 2c: All 8 rulebook volumes using the sidebar crawler."""
    log.info("=== Crawling [sidebar]: CBB Rulebook Volumes ===")
    raw_docs = crawl_rulebook_sidebar(
        seed_url=SIDEBAR_SEED,
        request_delay=request_delay,
        max_volumes=None,
    )
    docs = [_rulebook_doc_to_regulatory(d) for d in raw_docs]
    log.info(f"  Rulebook volumes: {len(docs)} documents")
    return docs


# ═══════════════════════════════════════════════════════════════════════════════
#  MODE 1 — CBB REGULATIONS AND RESOLUTIONS (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_content(soup: BeautifulSoup) -> tuple:
    content_block = soup.find("div", class_="field--name-body")
    content_html  = ""
    content_text  = ""
    download_links: List[Dict] = []
    english_pdf = None
    arabic_pdf  = None

    if content_block:
        _make_absolute(content_block)
        content_html = str(content_block)
        content_text = content_block.get_text(separator=" ", strip=True)
        for a in content_block.find_all("a", href=True):
            full_url    = a["href"]
            parent_text = a.parent.get_text().lower() if a.parent else ""
            lang        = "arabic" if "arabic" in parent_text else "english"
            download_links.append({"text": a.parent.get_text(strip=True), "url": full_url, "type": "pdf", "language": lang})
            if lang == "english" and not english_pdf:
                english_pdf = full_url
            if lang == "arabic" and not arabic_pdf:
                arabic_pdf = full_url

    return content_html, content_text, download_links, english_pdf, arabic_pdf


def _get_resolution_links(list_url: str) -> List[Dict]:
    soup = _fetch(list_url)
    if not soup:
        return []
    links = []
    seen  = set()
    nav   = soup.find("nav", {"id": "book-block-menu-2200001"})
    root  = nav if nav else soup
    for a in root.find_all("a", href=True):
        text = a.get_text(strip=True)
        if not re.search(r"Resolution\s+No\.|Regulation\s+No\.", text, re.IGNORECASE):
            continue
        full_url = urljoin(BASE_URL, a["href"])
        if full_url in seen:
            continue
        seen.add(full_url)
        links.append({"text": text, "url": full_url})
    return links


def _scrape_resolution(url: str, category: str) -> Optional[RegulatoryDocument]:
    soup = _fetch(url)
    if not soup:
        return None
    title_tag = soup.find("h2", class_="page-title") or soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""
    if not title:
        return None

    resolution_number = ""
    year_int = None
    published_date = None
    m = re.search(
        r"(?:Resolution|Regulation)\s+[Nn]o\.\s*\(?(\d+)\)?\s+(?:of|for)\s+(?:the\s+year\s+)?(\d{4})",
        title,
    )
    if m:
        resolution_number = m.group(1)
        year_int = int(m.group(2))
        try:
            published_date = datetime(year_int, 1, 1).date().isoformat()
        except ValueError:
            pass

    content_html, content_text, download_links, english_pdf, arabic_pdf = _extract_content(soup)
    primary_pdf = english_pdf or arabic_pdf

    prev_resolution = next_resolution = None
    pager = soup.find("div", class_="book-pager")
    if pager:
        for a in pager.find_all("a", href=True):
            rel  = a.get("rel", [])
            href = urljoin(BASE_URL, a["href"])
            text = a.get_text(strip=True)
            if "prev" in rel:
                prev_resolution = {"text": text, "url": href}
            elif "next" in rel:
                next_resolution = {"text": text, "url": href}

    return create_cbb_document(
        regulator=REGULATOR, source_system="CBB-Rulebook", category=category,
        title=title, document_url=primary_pdf or url, urdu_url=None,
        published_date=published_date, reference_no=resolution_number,
        department=None, year=year_int, source_page_url=url,
        file_type="PDF" if primary_pdf else None, document_html=content_html,
        extra_meta={
            "resolution_number": resolution_number, "download_links": download_links,
            "org_pdf_link": english_pdf, "arabic_pdf_link": arabic_pdf,
            "content_text": content_text, "prev_resolution": prev_resolution,
            "next_resolution": next_resolution,
        },
        doc_path=[REGULATOR, category, title],
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  MODE 3 — LAWS & REGULATIONS (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

LAWS_REGULATIONS_BASE = "https://www.cbb.gov.bh"
LAWS_CATEGORY         = "Laws & Regulations"


def _resolve_laws_href(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(LAWS_REGULATIONS_BASE, href)


def _scrape_laws_and_regulations(url: str = LAWS_REGULATIONS_URL) -> List[RegulatoryDocument]:
    soup = _fetch(url)
    if not soup:
        return []

    documents: List[RegulatoryDocument] = []
    accordion_headers = soup.find_all(
        "div",
        id=re.compile(r"^uvc-exp-wrap-\d+$"),
        class_=re.compile(r"ult_exp_section"),
    )

    for header_div in accordion_headers:
        title_el = header_div.find("div", class_="ult_expheader")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            continue

        content_div = header_div.find_next_sibling("div", class_="ult_exp_content")
        if not content_div:
            continue

        all_links = content_div.find_all("a", href=True)
        english_pdf: Optional[str] = None
        arabic_pdf:  Optional[str] = None
        all_download_links: List[Dict] = []

        for a in all_links:
            link_text = a.get_text(strip=True).lower()
            href      = _resolve_laws_href(a["href"])
            if "english" in link_text:
                lang = "english"
                if not english_pdf:
                    english_pdf = href
            elif "arabic" in link_text:
                lang = "arabic"
                if not arabic_pdf:
                    arabic_pdf = href
            else:
                parent_text = (a.parent.get_text() if a.parent else "").lower()
                lang = "arabic" if "arabic" in parent_text else "english"
                if lang == "english" and not english_pdf:
                    english_pdf = href
                elif lang == "arabic" and not arabic_pdf:
                    arabic_pdf = href
            all_download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": lang})

        primary_pdf = english_pdf or arabic_pdf
        _make_absolute(content_div, base=LAWS_REGULATIONS_BASE)
        content_text = content_div.get_text(separator=" ", strip=True)

        doc = create_cbb_document(
            regulator=REGULATOR, source_system="CBB-Laws-Regulations",
            category=LAWS_CATEGORY, title=title,
            document_url=primary_pdf or url, urdu_url=None,
            published_date=None, reference_no=None, department=None, year=None,
            source_page_url=url, file_type="PDF" if primary_pdf else None,
            document_html=str(content_div),
            extra_meta={
                "org_pdf_link": english_pdf, "arabic_pdf_link": arabic_pdf,
                "download_links": all_download_links,
                "content_text": content_text,
                "has_english_pdf": english_pdf is not None,
                "has_arabic_pdf": arabic_pdf is not None,
            },
            doc_path=[REGULATOR, LAWS_CATEGORY, title],
        )
        documents.append(doc)

    return documents


# ═══════════════════════════════════════════════════════════════════════════════
#  MODE 4 — CAPITAL MARKET REGULATIONS (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

_EXTERNAL_PDF_DOMAINS = ("legalaffairs.gov.bh", "www.legalaffairs.gov.bh")


def _is_external_pdf_link(href: str) -> bool:
    return any(domain in href for domain in _EXTERNAL_PDF_DOMAINS)


def _extract_book_child_links(soup: BeautifulSoup) -> List[Dict]:
    children = []
    seen = set()
    for nav_div in soup.find_all("div", id=re.compile(r"book-navigation")):
        for a in nav_div.find_all("a", href=True):
            text     = a.get_text(strip=True)
            full_url = urljoin(BASE_URL, a["href"])
            if full_url not in seen:
                seen.add(full_url)
                children.append({"text": text, "url": full_url})
    return children


def _scrape_book_page_recursive(
    url: str,
    category: str,
    path_so_far: List[str],
    visited: set,
    delay: float,
) -> List[RegulatoryDocument]:
    if url in visited:
        return []
    visited.add(url)

    time.sleep(delay)
    soup = _fetch(url)
    if not soup:
        return []

    title_tag = soup.find("h2", class_="page-title") or soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else (path_so_far[-1] if path_so_far else "")
    if not title:
        return []

    current_path = path_so_far + [title]
    content_html, content_text, download_links, english_pdf, arabic_pdf = _extract_content(soup)
    primary_pdf = english_pdf or arabic_pdf
    child_links = _extract_book_child_links(soup)

    if not content_html and child_links:
        book_nav_div = soup.find("div", id=re.compile(r"book-navigation"))
        if book_nav_div:
            _make_absolute(book_nav_div)
            content_html = str(book_nav_div)
            content_text = book_nav_div.get_text(separator=" ", strip=True)

    prev_page = next_page = None
    pager = soup.find("div", class_="book-pager")
    if pager:
        for a in pager.find_all("a", href=True):
            rel  = a.get("rel", [])
            href = urljoin(BASE_URL, a["href"])
            text = a.get_text(strip=True)
            if "prev" in rel:
                prev_page = {"text": text, "url": href}
            elif "next" in rel:
                next_page = {"text": text, "url": href}

    doc = create_cbb_document(
        regulator=REGULATOR, source_system="CBB-Capital-Market-Regulations",
        category=category, title=title,
        document_url=primary_pdf or url, urdu_url=None,
        published_date=None, reference_no=None, department=None, year=None,
        source_page_url=url, file_type="PDF" if primary_pdf else None,
        document_html=content_html,
        extra_meta={
            "download_links": download_links, "org_pdf_link": english_pdf,
            "arabic_pdf_link": arabic_pdf, "content_text": content_text,
            "book_path": current_path, "depth": len(current_path),
            "has_children": bool(child_links), "prev_page": prev_page, "next_page": next_page,
        },
        doc_path=[REGULATOR, CAPITAL_MARKET_CATEGORY] + current_path,
    )

    results = [doc]
    for child in child_links:
        results.extend(_scrape_book_page_recursive(
            url=child["url"], category=category, path_so_far=current_path,
            visited=visited, delay=delay,
        ))
    return results


def _parse_capital_market_list(url: str) -> tuple:
    soup = _fetch(url)
    if not soup:
        return [], [], ""
    body = soup.find("div", class_="field--name-body")
    if not body:
        return [], [], ""
    _make_absolute(body)
    page_html = str(body)
    internal_links: List[Dict] = []
    external_links: List[Dict] = []
    seen = set()
    market_list = body.find("ul", class_="marketlist")
    search_root = market_list if market_list else body
    for a in search_root.find_all("a", href=True):
        href     = a["href"]
        full_url = urljoin(BASE_URL, href) if href.startswith("/") else href
        link_text = a.get_text(strip=True)
        if not link_text or full_url in seen:
            continue
        seen.add(full_url)
        entry = {"text": link_text, "url": full_url}
        if _is_external_pdf_link(full_url):
            external_links.append(entry)
        else:
            internal_links.append(entry)
    return internal_links, external_links, page_html


def _scrape_external_pdf_doc(link_text: str, link_url: str, source_page_url: str) -> RegulatoryDocument:
    resolution_number = ""
    year_int = None
    published_date = None
    m = re.search(r"Resolution\s+[Nn]o\.?\s*\(?(\d+)\)?\s+of\s+(\d{4})", link_text)
    if m:
        resolution_number = m.group(1)
        year_int = int(m.group(2))
        try:
            published_date = datetime(year_int, 1, 1).date().isoformat()
        except ValueError:
            pass
    return create_cbb_document(
        regulator=REGULATOR, source_system="CBB-Capital-Market-Regulations",
        category=CAPITAL_MARKET_CATEGORY, title=link_text,
        document_url=link_url, urdu_url=None, published_date=published_date,
        reference_no=resolution_number or None, department=None, year=year_int,
        source_page_url=source_page_url, file_type="PDF", document_html="",
        extra_meta={
            "org_pdf_link": None, "arabic_pdf_link": link_url,
            "download_links": [{"text": link_text, "url": link_url, "type": "pdf", "language": "arabic"}],
            "content_text": link_text,
            "is_external_pdf": True, "has_english_pdf": False, "has_arabic_pdf": True,
        },
        doc_path=[REGULATOR, CAPITAL_MARKET_CATEGORY, link_text],
    )


def _scrape_capital_market_regulations(
    url: str = CAPITAL_MARKET_REGULATIONS_URL,
    request_delay: float = REQUEST_DELAY,
) -> List[RegulatoryDocument]:
    log.info(f"=== Crawling [capital_market]: {CAPITAL_MARKET_CATEGORY} ===")
    internal_links, external_links, page_html = _parse_capital_market_list(url)
    documents: List[RegulatoryDocument] = []
    visited = set()
    for i, link in enumerate(internal_links, 1):
        log.info(f"[internal {i}/{len(internal_links)}] {link['text'][:60]}")
        time.sleep(request_delay)
        docs = _scrape_book_page_recursive(
            url=link["url"], category=CAPITAL_MARKET_CATEGORY,
            path_so_far=[], visited=visited, delay=request_delay,
        )
        documents.extend(docs)
    for i, link in enumerate(external_links, 1):
        log.info(f"[external {i}/{len(external_links)}] {link['text'][:60]}")
        documents.append(_scrape_external_pdf_doc(link["text"], link["url"], url))
    log.info(f"Capital Market Regulations total: {len(documents)} documents")
    return documents


# ═══════════════════════════════════════════════════════════════════════════════
#  MODE 5 — COMPLIANCE (unchanged from v2)
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_date(text: str) -> Optional[str]:
    m = re.search(r'(\d{1,2})(?:st|nd|rd|th)?\s+(\w+)\s+(\d{4})', text, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = MONTH_MAP.get(month_name)
        if month:
            try:
                return datetime(year, month, day).date().isoformat()
            except ValueError:
                pass
    m2 = re.search(r'\b(\d{4})\b', text)
    if m2:
        return m2.group(1)
    return None


def _make_doc(section, accordion_title, title, document_url, document_html="",
              extra_meta=None, published_date=None, reference_no=None, file_type=None):
    return create_cbb_document(
        regulator=REGULATOR, source_system=SOURCE_SYSTEM, category=CATEGORY,
        title=title, document_url=document_url, urdu_url=None,
        published_date=published_date, reference_no=reference_no, department=None,
        year=(int(published_date[:4]) if published_date and len(published_date) >= 4 and published_date[:4].isdigit() else None),
        source_page_url=COMPLIANCE_URL, file_type=file_type,
        document_html=document_html, extra_meta=extra_meta or {},
        doc_path=[REGULATOR, CATEGORY, section, accordion_title],
    )


def _parse_text_accordion(content_div, section, accordion_title):
    html = str(content_div)
    text = content_div.get_text(separator=" ", strip=True)
    doc = _make_doc(section=section, accordion_title=accordion_title, title=accordion_title,
                    document_url=COMPLIANCE_URL, document_html=html,
                    extra_meta={"content_text": text, "is_text_only": True})
    doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title]
    return [doc]


def _parse_publications_accordion(content_div, section, accordion_title):
    docs = []
    for a in content_div.find_all("a", href=True):
        href = _abs(a["href"])
        link_text = a.get_text(strip=True)
        parent_text = a.parent.get_text(strip=True) if a.parent else link_text
        doc = _make_doc(section=section, accordion_title=accordion_title,
                        title=link_text or parent_text, document_url=href,
                        document_html=str(a.parent) if a.parent else "",
                        extra_meta={"content_text": parent_text,
                                    "download_links": [{"text": link_text, "url": href, "type": "pdf", "language": "english"}]},
                        file_type="PDF" if _is_pdf(href) else None)
        doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, link_text]
        docs.append(doc)
    return docs


def _parse_legislation_accordion(content_div, section, accordion_title):
    docs = []
    for p in content_div.find_all("p"):
        p_text = p.get_text(separator=" ", strip=True)
        if p_text.startswith("The Kingdom of Bahrain") or not p_text:
            continue
        decree_match = re.match(r'^(\d+)\.\s*(.+?)(?:\s+English|\s+Arabic|$)', p_text, re.DOTALL)
        if not decree_match:
            continue
        entry_num = decree_match.group(1)
        links = p.find_all("a", href=True)
        if not links:
            continue
        full_p_text = p.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in full_p_text.split("\n") if l.strip()]
        title_parts = []
        for line in lines:
            if line in ("English", "Arabic", "English / Arabic", "/"):
                break
            title_parts.append(line)
        title = " ".join(title_parts).strip() or f"Decree Entry {entry_num}"
        english_url = arabic_url = None
        download_links = []
        for a in links:
            href = _abs(a["href"])
            a_text = a.get_text(strip=True).lower()
            lang = "arabic" if a_text == "arabic" else "english"
            if lang == "english" and not english_url:
                english_url = href
            elif lang == "arabic" and not arabic_url:
                arabic_url = href
            download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": lang})
        primary_url = english_url or arabic_url or COMPLIANCE_URL
        doc = _make_doc(section=section, accordion_title=accordion_title, title=title,
                        document_url=primary_url, document_html=str(p),
                        extra_meta={"content_text": full_p_text, "english_url": english_url,
                                    "arabic_url": arabic_url, "download_links": download_links, "entry_number": entry_num},
                        file_type="PDF" if primary_url != COMPLIANCE_URL else None)
        doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, title]
        docs.append(doc)
    return docs


def _parse_guidance_papers_accordion(content_div, section, accordion_title):
    docs = []
    for p in content_div.find_all("p"):
        p_text = p.get_text(separator="\n", strip=True)
        if not p_text:
            continue
        num_match = re.match(r'^(\d+)\.', p_text)
        if not num_match:
            continue
        entry_num = num_match.group(1)
        links = p.find_all("a", href=True)
        if not links:
            continue
        lines = [l.strip() for l in p_text.split("\n") if l.strip()]
        title_parts = []
        for line in lines:
            if line.lower() in ("letter", "guidance paper", "letter / guidance paper", "/"):
                break
            title_parts.append(line)
        title = " ".join(title_parts).strip() or f"Guidance Paper Entry {entry_num}"
        letter_url = guidance_paper_url = None
        download_links = []
        for a in links:
            href = _abs(a["href"])
            a_text = a.get_text(strip=True).lower()
            if "letter" in a_text:
                if not letter_url:
                    letter_url = href
                download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": "english", "role": "letter"})
            elif "guidance" in a_text:
                if not guidance_paper_url:
                    guidance_paper_url = href
                download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": "english", "role": "guidance_paper"})
            else:
                download_links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf", "language": "english"})
        primary_url = guidance_paper_url or letter_url or COMPLIANCE_URL
        doc = _make_doc(section=section, accordion_title=accordion_title, title=title,
                        document_url=primary_url, document_html=str(p),
                        extra_meta={"content_text": p_text, "letter_url": letter_url,
                                    "guidance_paper_url": guidance_paper_url, "download_links": download_links, "entry_number": entry_num},
                        file_type="PDF" if primary_url != COMPLIANCE_URL else None)
        doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, title]
        docs.append(doc)
    return docs


def _parse_directives_section(content_div, section, accordion_title, sub_section_title):
    docs = []
    for p in content_div.find_all("p"):
        p_text = p.get_text(separator="\n", strip=True)
        if not p_text:
            continue
        num_match = re.match(r'^(\d+)\.\s*', p_text)
        if not num_match:
            continue
        entry_num = num_match.group(1)
        links = p.find_all("a", href=True)
        if not links:
            continue
        a = links[0]
        href = _abs(a["href"])
        link_text = a.get_text(strip=True)
        ref_match = re.match(r'^([A-Z][A-Z0-9/_\-\.]+)', link_text)
        reference_no = ref_match.group(1) if ref_match else None
        lines = [l.strip() for l in p_text.split("\n") if l.strip()]
        date_str = None
        for line in lines:
            if re.search(r'\d{4}', line) and any(mn in line.lower() for mn in MONTH_MAP):
                date_str = line
                break
        published_date = _parse_date(date_str) if date_str else None
        download_links = [{"text": ea.get_text(strip=True), "url": _abs(ea["href"]),
                           "type": "pdf" if _is_pdf(_abs(ea["href"])) else "link", "language": "english"}
                          for ea in links]
        doc = _make_doc(section=section, accordion_title=accordion_title, title=link_text,
                        document_url=href, document_html=str(p),
                        extra_meta={"content_text": p_text, "download_links": download_links,
                                    "entry_number": entry_num, "sub_section": sub_section_title, "date_text": date_str},
                        published_date=published_date, reference_no=reference_no,
                        file_type="PDF" if _is_pdf(href) else None)
        doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, sub_section_title, link_text]
        docs.append(doc)
    return docs


def _parse_links_accordion(content_div, section, accordion_title):
    html = str(content_div)
    text = content_div.get_text(separator=" ", strip=True)
    external_links = [{"text": a.get_text(strip=True), "url": a["href"]}
                      for a in content_div.find_all("a", href=True)]
    doc = _make_doc(section=section, accordion_title=accordion_title, title=accordion_title,
                    document_url=COMPLIANCE_URL, document_html=html,
                    extra_meta={"content_text": text, "external_links": external_links, "is_links_collection": True})
    doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title]
    return [doc]


def _parse_mutual_evaluation_accordion(content_div, section, accordion_title):
    html = str(content_div)
    text = content_div.get_text(separator=" ", strip=True)
    links = []
    primary_url = COMPLIANCE_URL
    file_type = None
    for a in content_div.find_all("a", href=True):
        href = a["href"]
        links.append({"text": a.get_text(strip=True), "url": href, "type": "pdf" if _is_pdf(href) else "link"})
        if _is_pdf(href) and primary_url == COMPLIANCE_URL:
            primary_url = href
            file_type = "PDF"
    doc = _make_doc(section=section, accordion_title=accordion_title, title=accordion_title,
                    document_url=primary_url, document_html=html,
                    extra_meta={"content_text": text, "download_links": links}, file_type=file_type)
    doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title]
    return [doc]


def _parse_crs_fatca_beps_accordion(content_div, section, accordion_title, directive_name):
    docs = []
    wrapper_divs = content_div.find_all("div", class_="wpb_wrapper")
    intro_html = intro_text = ""
    found_header = False
    for wpb_div in wrapper_divs:
        h2 = wpb_div.find("h2")
        if h2 and "directives" in h2.get_text(strip=True).lower():
            found_header = True
            if intro_text.strip():
                intro_doc = _make_doc(section=section, accordion_title=accordion_title,
                                      title=f"{accordion_title} – Overview",
                                      document_url=COMPLIANCE_URL, document_html=intro_html,
                                      extra_meta={"content_text": intro_text.strip(), "is_overview": True})
                intro_doc.doc_path = [REGULATOR, CATEGORY, section, accordion_title, f"{accordion_title} – Overview"]
                docs.append(intro_doc)
            continue
        if found_header:
            docs.extend(_parse_directives_section(wpb_div, section, accordion_title, directive_name))
            found_header = False
        else:
            inner_text = wpb_div.get_text(separator=" ", strip=True)
            if inner_text:
                intro_html += str(wpb_div)
                intro_text += " " + inner_text
    return docs


def _route_accordion(accordion_title: str, content_div: Tag, section: str):
    t = accordion_title.lower()
    if "compliance directorate publications" in t:
        return _parse_publications_accordion(content_div, section, accordion_title)
    elif "legislation" in t:
        return _parse_legislation_accordion(content_div, section, accordion_title)
    elif "guidance papers" in t:
        return _parse_guidance_papers_accordion(content_div, section, accordion_title)
    elif "common reporting standard" in t or "crs" in t:
        return _parse_crs_fatca_beps_accordion(content_div, section, accordion_title, "CRS Directives")
    elif "foreign account tax compliance" in t or "fatca" in t:
        return _parse_crs_fatca_beps_accordion(content_div, section, accordion_title, "FATCA Directives")
    elif "base erosion" in t or "beps" in t:
        return _parse_crs_fatca_beps_accordion(content_div, section, accordion_title, "BEPS Directives")
    elif "links" in t or "associations" in t:
        return _parse_links_accordion(content_div, section, accordion_title)
    elif "mutual evaluation" in t:
        return _parse_mutual_evaluation_accordion(content_div, section, accordion_title)
    else:
        return _parse_text_accordion(content_div, section, accordion_title)


def _parse_section(section_div: Tag, section_name: str):
    docs = []
    intro_h2 = section_div.find("h2")
    if intro_h2:
        intro_parts = []
        for sib in intro_h2.find_next_siblings():
            if sib.get("class") and "ult_exp_section_layer" in " ".join(sib.get("class", [])):
                break
            if hasattr(sib, "get_text"):
                t = sib.get_text(strip=True)
                if t:
                    intro_parts.append(str(sib))
        if intro_parts:
            intro_html = "\n".join(intro_parts)
            intro_text = BeautifulSoup(intro_html, "lxml").get_text(separator=" ", strip=True)
            if intro_text:
                intro_doc = _make_doc(section=section_name, accordion_title="Overview",
                                      title=f"{section_name} Overview",
                                      document_url=COMPLIANCE_URL, document_html=intro_html,
                                      extra_meta={"content_text": intro_text, "is_section_intro": True})
                intro_doc.doc_path = [REGULATOR, CATEGORY, section_name, "Overview"]
                docs.append(intro_doc)

    for layer in section_div.find_all("div", class_="ult_exp_section_layer"):
        header_div = layer.find("div", class_="ult_expheader")
        if not header_div:
            header_div = layer.find("div", id=re.compile(r"uvc-exp-wrap"))
            accordion_title = header_div.get("data-title", "").strip() if header_div else ""
        else:
            accordion_title = header_div.get_text(strip=True)
        if not accordion_title:
            continue
        content_div = layer.find("div", class_="ult_exp_content")
        if not content_div:
            empty_doc = _make_doc(section=section_name, accordion_title=accordion_title,
                                  title=accordion_title, document_url=COMPLIANCE_URL,
                                  extra_meta={"is_folder": True, "empty": True})
            empty_doc.doc_path = [REGULATOR, CATEGORY, section_name, accordion_title]
            docs.append(empty_doc)
            continue
        docs.extend(_route_accordion(accordion_title, content_div, section_name))
    return docs


def scrape_compliance(url: str = COMPLIANCE_URL) -> List[RegulatoryDocument]:
    log.info(f"=== Crawling Compliance page: {url} ===")
    soup = _fetch(url)
    if not soup:
        return []
    all_docs = []
    for section_id, section_name in [("aml", "AML"), ("eofi", "EOFI")]:
        section_div = soup.find("div", id=section_id)
        if section_div:
            section_docs = _parse_section(section_div, section_name)
            all_docs.extend(section_docs)
            log.info(f"  {section_name}: {len(section_docs)} documents")
        else:
            log.error(f"Could not find #{section_id} section!")
    log.info(f"=== Compliance crawl complete: {len(all_docs)} total documents ===")
    return all_docs


# ═══════════════════════════════════════════════════════════════════════════════
#  CBBCrawler CLASS — unified entry point
# ═══════════════════════════════════════════════════════════════════════════════

class CBBCrawler(BaseCrawler):

    def __init__(self, request_delay: float = REQUEST_DELAY):
        self.request_delay = request_delay

    def _crawl_resolutions(self, list_url: str, category: str) -> List[RegulatoryDocument]:
        documents = []
        links = _get_resolution_links(list_url)
        for i, link in enumerate(links, 1):
            log.info(f"[{i}/{len(links)}] {link['text'][:80]}")
            time.sleep(self.request_delay)
            doc = _scrape_resolution(link["url"], category)
            if doc:
                documents.append(doc)
        return documents

    def _crawl_laws_and_regulations(self) -> List[RegulatoryDocument]:
        log.info("=== Crawling [accordion]: Laws & Regulations ===")
        return _scrape_laws_and_regulations()

    def _crawl_capital_market_regulations(self) -> List[RegulatoryDocument]:
        return _scrape_capital_market_regulations(
            url=CAPITAL_MARKET_REGULATIONS_URL,
            request_delay=self.request_delay,
        )

    def _crawl_compliance(self) -> List[RegulatoryDocument]:
        return scrape_compliance(url=COMPLIANCE_URL)

    def get_documents(self) -> List[RegulatoryDocument]:
        all_docs: List[RegulatoryDocument] = []

        # Mode 1 — CBB Regulations and Resolutions
        for category, (url, mode) in STATIC_BASE_URLS.items():
            log.info(f"=== Crawling [{mode}]: {category} ===")
            if mode == "resolutions":
                docs = self._crawl_resolutions(url, category)
                all_docs.extend(docs)

        # Mode 2a — AML Law (NEW: Aml_crawler_v2)
        all_docs.extend(_crawl_aml_law())

        # Mode 2b — Corporate Governance (NEW: Aml_crawler_v2)
        all_docs.extend(_crawl_corpgov())

        # Mode 2c — Rulebook Volumes (NEW: sidebar crawler)
        all_docs.extend(_crawl_rulebook_volumes(self.request_delay))

        # Mode 3 — Laws & Regulations
        all_docs.extend(self._crawl_laws_and_regulations())

        # Mode 4 — Capital Market Regulations
        all_docs.extend(self._crawl_capital_market_regulations())

        # Mode 5 — Compliance
        all_docs.extend(self._crawl_compliance())

        return all_docs

    def fetch_documents(self, timeout=None) -> List[RegulatoryDocument]:
        return self.get_documents()