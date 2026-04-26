"""
CBB Crawler v2 — MERGED VERSION
=================================
Integrates new sub-crawlers into the main CBB crawler class.

MODE OVERVIEW:
  Mode 1  — CBB Regulations and Resolutions (Thomson Reuters)
  Mode 2a — AML Law                    <- uses Aml_crawler_v2
  Mode 2b — Corporate Governance       <- uses Aml_crawler_v2
  Mode 2c — Rulebook Volumes           <- uses cbb_rulebook_crawler sidebar
  Mode 3  — Laws & Regulations (cbb.gov.bh accordion)
  Mode 4  — CBB Capital Market Regulations
  Mode 5  — Compliance (cbb.gov.bh #aml/#eofi)

Usage:
    from cbb_crawler_v2 import CBBCrawlerV2
    crawler = CBBCrawlerV2()
    docs = crawler.fetch_documents()          # all modes
    docs = crawler.fetch_documents(mode="1")  # specific mode
"""

import hashlib
import logging
import re
import time
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

try:
    from crawler.crawler import BaseCrawler
    from models.models import RegulatoryDocument
except ImportError:
    class BaseCrawler:
        pass
    from dataclasses import dataclass, field as dc_field

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
        extra_meta: dict = dc_field(default_factory=dict)
        doc_path: list = dc_field(default_factory=list)
        compliancecategory_id: Any = None
        content_hash: str = ""

# ── New crawler imports ────────────────────────────────────────────────────────
try:
    from cbb_test_crawlers.cbb_rulebook_crawler import (
        crawl_rulebook_sidebar, RulebookDoc, SIDEBAR_SEED,
    )
    from cbb_test_crawlers.Aml_crawler_v2 import (
        crawl_rulebook as _crawl_aml_corpgov,
        RulebookDocument as AmlDoc,
        SOURCES as AML_SOURCES,
    )
except ImportError:
    from cbb_rulebook_crawler import crawl_rulebook_sidebar, RulebookDoc, SIDEBAR_SEED
    from Aml_crawler_v2 import (
        crawl_rulebook as _crawl_aml_corpgov,
        RulebookDocument as AmlDoc,
        SOURCES as AML_SOURCES,
    )

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_URL       = "https://cbben.thomsonreuters.com"
CBB_GOV_BASE   = "https://www.cbb.gov.bh"
REGULATOR      = "Central Bank of Bahrain"
REQUEST_DELAY  = 1.2
MAX_RETRIES    = 3

CBB_RULEBOOK_INDEX = "https://cbben.thomsonreuters.com/rulebook/common-volume"
LAWS_REGULATIONS_URL = "https://www.cbb.gov.bh/laws-regulations/"
COMPLIANCE_URL       = "https://www.cbb.gov.bh/compliance/"
CAPITAL_MARKET_URL   = "https://cbben.thomsonreuters.com/rulebook/capital-market-regulations"
RESOLUTIONS_URL      = "https://cbben.thomsonreuters.com/rulebook/cbb-regulations-and-resolutions"

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


# ─── HTTP helpers ─────────────────────────────────────────────────────────────
def _fetch(url: str) -> Optional[BeautifulSoup]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)
    log.error(f"Failed to fetch after {MAX_RETRIES} attempts: {url}")
    return None


def _make_absolute(tag: Tag):
    for a in tag.find_all(href=True):
        if a["href"].startswith("/"):
            a["href"] = BASE_URL + a["href"]


# ─── Mode 1: CBB Regulations and Resolutions ─────────────────────────────────
def _extract_resolution_content(soup: BeautifulSoup) -> tuple:
    body = soup.find("div", class_="field--name-body")
    if not body:
        return "", "", [], None, None
    _make_absolute(body)
    html = str(body)
    text = body.get_text(separator=" ", strip=True)
    links = []
    english_pdf = None
    arabic_pdf  = None
    for a in body.find_all("a", href=True):
        url  = a["href"]
        parent_text = a.parent.get_text().lower() if a.parent else ""
        lang = "arabic" if "arabic" in parent_text else "english"
        links.append({"text": a.get_text(strip=True), "url": url, "type": "pdf", "language": lang})
        if lang == "english" and not english_pdf:
            english_pdf = url
        if lang == "arabic" and not arabic_pdf:
            arabic_pdf = url
    return html, text, links, english_pdf, arabic_pdf


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
        title, re.IGNORECASE
    )
    if m:
        resolution_number = m.group(1)
        year_int = int(m.group(2))
        published_date = date(year_int, 1, 1)

    html, text, links, eng_pdf, ar_pdf = _extract_resolution_content(soup)
    content_hash = hashlib.md5(text.encode("utf-8")).hexdigest() if text else ""

    doc = RegulatoryDocument(
        regulator       = REGULATOR,
        source_system   = "CBB-Compliance",
        category        = category,
        title           = title,
        document_url    = eng_pdf or url,
        source_page_url = url,
        document_html   = html,
        published_date  = published_date,
        reference_no    = resolution_number,
        year            = year_int,
        doc_path        = [REGULATOR, category, title],
        extra_meta      = {
            "arabic_pdf":     ar_pdf,
            "download_links": links,
            "content_text":   text,
            "content_hash":   content_hash,
        },
        content_hash    = content_hash,
    )
    return doc


def _crawl_resolutions(list_url: str, category: str) -> List[RegulatoryDocument]:
    log.info(f"Mode 1 — Fetching resolution links from {list_url}")
    resolution_links = _get_resolution_links(list_url)
    log.info(f"Mode 1 — Found {len(resolution_links)} resolutions")
    docs = []
    for i, link in enumerate(resolution_links, 1):
        doc = _scrape_resolution(link["url"], category)
        if doc:
            docs.append(doc)
        if i % 20 == 0:
            log.info(f"Mode 1 — {i}/{len(resolution_links)} scraped")
        time.sleep(REQUEST_DELAY)
    return docs


# ─── Mode 2a/2b: AML Law & Corporate Governance (Aml_crawler_v2) ─────────────
def _aml_doc_to_regulatory(doc: AmlDoc) -> RegulatoryDocument:
    """Convert AmlDoc to RegulatoryDocument for pipeline compatibility."""
    source_map = {
        "aml":     "CBB-AML-LAW",
        "corpgov": "CBB-CORPGOV",
    }
    return RegulatoryDocument(
        regulator       = REGULATOR,
        source_system   = source_map.get(doc.source_key, f"CBB-{doc.source_key.upper()}"),
        category        = doc.category,
        title           = doc.title,
        document_url    = doc.url,
        source_page_url = doc.url,
        document_html   = doc.content_html,   # content_html -> document_html
        doc_path        = doc.path,            # path -> doc_path
        extra_meta      = {
            "content_text": doc.content_text,
            "content_hash": doc.content_hash,
            "source_key":   doc.source_key,
            "row_type":     doc.row_type,
        },
        content_hash    = doc.content_hash,
    )


def _crawl_aml() -> List[RegulatoryDocument]:
    log.info("Mode 2a — AML Law (Aml_crawler_v2)")
    raw = _crawl_aml_corpgov("aml")
    return [_aml_doc_to_regulatory(d) for d in raw if d.row_type == "R"]


def _crawl_corpgov() -> List[RegulatoryDocument]:
    log.info("Mode 2b — Corporate Governance (Aml_crawler_v2)")
    raw = _crawl_aml_corpgov("corpgov")
    return [_aml_doc_to_regulatory(d) for d in raw if d.row_type == "R"]


# ─── Mode 2c: Rulebook Volumes (cbb_rulebook_crawler sidebar) ────────────────
def _rulebook_doc_to_regulatory(doc: RulebookDoc) -> RegulatoryDocument:
    return RegulatoryDocument(
        regulator       = REGULATOR,
        source_system   = "CBB-Rulebook",
        category        = doc.doc_path[1] if len(doc.doc_path) > 1 else "CBB Rulebook",
        title           = doc.title,
        document_url    = doc.url,
        source_page_url = doc.url,
        document_html   = doc.document_html,
        doc_path        = doc.doc_path,
        extra_meta      = {
            "pdf_link":    doc.extra_meta.get("pdf_link"),
            "pdf_links":   doc.extra_meta.get("pdf_links", []),
            "faq_link":    doc.extra_meta.get("faq_link"),
            "content_text": doc.content_text,
            "content_hash": doc.content_hash,
            "is_folder":   doc.is_folder,
        },
        content_hash    = doc.content_hash,
    )


def _crawl_rulebook() -> List[RegulatoryDocument]:
    log.info("Mode 2c — CBB Rulebook Volumes (sidebar crawler)")
    raw_docs = crawl_rulebook_sidebar(
        seed_url      = SIDEBAR_SEED,
        request_delay = REQUEST_DELAY,
        max_volumes   = None,
    )
    # Return ALL docs (including folders) so caller can handle folder insertion
    return [_rulebook_doc_to_regulatory(d) for d in raw_docs]


# ─── Mode 3: Laws & Regulations (cbb.gov.bh accordion) ──────────────────────
def _scrape_laws_and_regulations() -> List[RegulatoryDocument]:
    log.info(f"Mode 3 — Laws & Regulations: {LAWS_REGULATIONS_URL}")
    soup = _fetch(LAWS_REGULATIONS_URL)
    if not soup:
        return []

    docs = []
    category = "Laws and Regulations"

    accordion_items = soup.find_all("div", class_=re.compile(r"accordion-item|accordion__item"))
    for item in accordion_items:
        heading = item.find(re.compile(r"h[23456]"))
        section_title = heading.get_text(strip=True) if heading else "Unknown Section"

        for a in item.find_all("a", href=True):
            href = a["href"]
            if not href.endswith(".pdf") and "/files/" not in href.lower():
                continue
            full_url = urljoin(CBB_GOV_BASE, href)
            title    = a.get_text(strip=True) or section_title
            html     = str(a.parent) if a.parent else f'<a href="{full_url}">{title}</a>'
            text     = title
            hash_val = hashlib.md5(text.encode()).hexdigest()

            docs.append(RegulatoryDocument(
                regulator       = REGULATOR,
                source_system   = "CBB-Laws-Regulations",
                category        = category,
                title           = title,
                document_url    = full_url,
                source_page_url = LAWS_REGULATIONS_URL,
                document_html   = html,
                doc_path        = [REGULATOR, category, section_title, title],
                extra_meta      = {
                    "section":      section_title,
                    "content_text": text,
                    "content_hash": hash_val,
                },
                content_hash    = hash_val,
            ))
    log.info(f"Mode 3 — Found {len(docs)} documents")
    return docs


# ─── Mode 4: Capital Market Regulations ──────────────────────────────────────
def _scrape_capital_market_regulations() -> List[RegulatoryDocument]:
    log.info(f"Mode 4 — Capital Market Regulations: {CAPITAL_MARKET_URL}")
    soup = _fetch(CAPITAL_MARKET_URL)
    if not soup:
        return []

    docs = []
    category = "CBB Capital Market Regulations"

    nav = soup.find("nav", id=re.compile(r"book-block-menu-"))
    root = nav if nav else soup
    seen = set()

    for a in root.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(BASE_URL, href)
        if full_url in seen or not href.startswith("/rulebook/"):
            continue
        seen.add(full_url)
        title = a.get_text(strip=True)
        if not title:
            continue

        time.sleep(REQUEST_DELAY)
        page_soup = _fetch(full_url)
        if not page_soup:
            continue

        body = page_soup.find("div", class_="field--name-body")
        html = str(body) if body else ""
        text = body.get_text(separator=" ", strip=True) if body else title
        hash_val = hashlib.md5(text.encode()).hexdigest()

        docs.append(RegulatoryDocument(
            regulator       = REGULATOR,
            source_system   = "CBB-Capital-Market-Regulations",
            category        = category,
            title           = title,
            document_url    = full_url,
            source_page_url = full_url,
            document_html   = html,
            doc_path        = [REGULATOR, category, title],
            extra_meta      = {
                "content_text": text,
                "content_hash": hash_val,
            },
            content_hash    = hash_val,
        ))

    log.info(f"Mode 4 — Found {len(docs)} documents")
    return docs


# ─── Mode 5: Compliance (cbb.gov.bh #aml/#eofi) ──────────────────────────────
def _scrape_compliance_section(section_id: str, category: str) -> List[RegulatoryDocument]:
    url  = f"{COMPLIANCE_URL}#{section_id}"
    soup = _fetch(COMPLIANCE_URL)
    if not soup:
        return []

    section = soup.find("div", id=section_id) or soup.find("section", id=section_id)
    if not section:
        log.warning(f"Mode 5 — Section #{section_id} not found")
        return []

    docs = []
    seen = set()

    for a in section.find_all("a", href=True):
        href = a["href"]
        if href.startswith("#") or href in seen:
            continue
        seen.add(href)
        full_url = urljoin(CBB_GOV_BASE, href)
        title    = a.get_text(strip=True)
        if not title:
            continue
        parent_html = str(a.parent) if a.parent else f'<a href="{full_url}">{title}</a>'
        text     = title
        hash_val = hashlib.md5(text.encode()).hexdigest()

        docs.append(RegulatoryDocument(
            regulator       = REGULATOR,
            source_system   = "CBB-Compliance",
            category        = category,
            title           = title,
            document_url    = full_url,
            source_page_url = url,
            document_html   = parent_html,
            doc_path        = [REGULATOR, category, title],
            extra_meta      = {
                "section_id":   section_id,
                "content_text": text,
                "content_hash": hash_val,
            },
            content_hash    = hash_val,
        ))
    return docs


def _scrape_compliance() -> List[RegulatoryDocument]:
    log.info(f"Mode 5 — Compliance sections from {COMPLIANCE_URL}")
    docs = []
    docs += _scrape_compliance_section("aml",  "AML Compliance")
    docs += _scrape_compliance_section("eofi", "EOFI Compliance")
    log.info(f"Mode 5 — Found {len(docs)} documents")
    return docs


# ─── Main Crawler Class ───────────────────────────────────────────────────────
class CBBCrawlerV2(BaseCrawler):
    """
    Merged CBB Crawler v2.
    Runs all modes and returns a combined list of RegulatoryDocument objects.
    """

    REGULATOR = REGULATOR

    def fetch_documents(self, mode: Optional[str] = None) -> List[RegulatoryDocument]:
        """
        Fetch documents from all CBB sources (or a specific mode).

        Args:
            mode: "1", "2a", "2b", "2c", "3", "4", "5", or None (all modes).

        Returns:
            List of RegulatoryDocument objects.
        """
        all_docs: List[RegulatoryDocument] = []

        run_all = mode is None

        if run_all or mode == "1":
            log.info("=== Mode 1: CBB Regulations & Resolutions ===")
            docs = _crawl_resolutions(RESOLUTIONS_URL, "CBB Regulations and Resolutions")
            log.info(f"Mode 1: {len(docs)} documents")
            all_docs.extend(docs)

        if run_all or mode == "2a":
            log.info("=== Mode 2a: AML Law ===")
            docs = _crawl_aml()
            log.info(f"Mode 2a: {len(docs)} documents")
            all_docs.extend(docs)

        if run_all or mode == "2b":
            log.info("=== Mode 2b: Corporate Governance ===")
            docs = _crawl_corpgov()
            log.info(f"Mode 2b: {len(docs)} documents")
            all_docs.extend(docs)

        if run_all or mode == "2c":
            log.info("=== Mode 2c: Rulebook Volumes ===")
            docs = _crawl_rulebook()
            log.info(f"Mode 2c: {len(docs)} documents")
            all_docs.extend(docs)

        if run_all or mode == "3":
            log.info("=== Mode 3: Laws & Regulations ===")
            docs = _scrape_laws_and_regulations()
            log.info(f"Mode 3: {len(docs)} documents")
            all_docs.extend(docs)

        if run_all or mode == "4":
            log.info("=== Mode 4: Capital Market Regulations ===")
            docs = _scrape_capital_market_regulations()
            log.info(f"Mode 4: {len(docs)} documents")
            all_docs.extend(docs)

        if run_all or mode == "5":
            log.info("=== Mode 5: Compliance ===")
            docs = _scrape_compliance()
            log.info(f"Mode 5: {len(docs)} documents")
            all_docs.extend(docs)

        log.info(f"=== CBB Crawler v2 complete: {len(all_docs)} total documents ===")
        return all_docs


# ─── Standalone entry point ───────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    crawler = CBBCrawlerV2()
    docs = crawler.fetch_documents()
    print(f"\nTotal documents: {len(docs)}")
    by_source = {}
    for d in docs:
        by_source.setdefault(d.source_system, 0)
        by_source[d.source_system] += 1
    for src, cnt in sorted(by_source.items()):
        print(f"  {src}: {cnt}")