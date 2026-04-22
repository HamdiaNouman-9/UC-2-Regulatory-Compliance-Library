"""
CBB Monitoring Crawler - FIXED VERSION
=======================================
Fixes:
1. doc_path now reads sidebar active trail (same as full crawler) → exact match
2. Compliance items now include content_html
3. TR pages store document_html and pdf_link correctly
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import logging
from datetime import datetime, date, timedelta
from urllib.parse import urljoin
from typing import List, Optional, Dict
import hashlib
import os

from models.models import RegulatoryDocument

log = logging.getLogger(__name__)

BASE_URL             = "https://cbben.thomsonreuters.com"
CHANGES_URL          = "https://cbben.thomsonreuters.com/view-revision-updates"
LAWS_REGULATIONS_URL = "https://www.cbb.gov.bh/laws-regulations/"
COMPLIANCE_URL       = "https://www.cbb.gov.bh/compliance/"
CBB_GOV_BASE         = "https://www.cbb.gov.bh"

REQUEST_DELAY = 1.0
MAX_RETRIES   = 3
REGULATOR     = "Central Bank of Bahrain"


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s

SESSION = _make_session()


def _fetch(url: str, params: dict = None) -> Optional[BeautifulSoup]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            resp.raise_for_status()
            log.info(f"✓ [{resp.status_code}] {url}")
            return BeautifulSoup(resp.content, "lxml")
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    log.error(f"✗ All retries exhausted: {url}")
    return None


def _make_absolute(block: BeautifulSoup, base: str = BASE_URL) -> None:
    for tag in block.find_all(href=True):
        if tag["href"].startswith("/"):
            tag["href"] = urljoin(base, tag["href"])
    for tag in block.find_all(src=True):
        if tag["src"].startswith("/"):
            tag["src"] = urljoin(base, tag["src"])


# ═══════════════════════════════════════════════════════════════════════════════
#  PART A: THOMSON REUTERS "VIEW UPDATES" MONITORING
# ═══════════════════════════════════════════════════════════════════════════════

def _get_thomson_reuters_changes(from_date: date, to_date: date) -> List[Dict]:
    changed_pages = []
    seen = set()

    params = {
        "f_days":         "on",
        "changed":        "between",
        "min":            from_date.strftime("%d/%m/%Y"),
        "max":            to_date.strftime("%d/%m/%Y"),
        "items_per_page": "40",
        "sort_by":        "revision_timestamp_1",
    }

    page_url = CHANGES_URL
    page_num = 0

    while page_url:
        soup = _fetch(page_url, params=params if page_num == 0 else None)
        if not soup:
            break

        results_area = soup.find("div", class_="view-content")
        if not results_area:
            break

        for row in results_area.find_all("div", class_="views-row"):
            detail_div = row.find("div", class_="book-detail")
            if not detail_div:
                continue
            a = detail_div.find("a", href=True)
            if not a:
                continue

            full_url = urljoin(BASE_URL, a.get("href", ""))
            if full_url in seen:
                continue
            seen.add(full_url)

            title = a.get_text(strip=True)
            change_date = None
            dm = re.search(r"\((\d{1,2}\s+\w+\s+\d{4})\)", detail_div.get_text())
            if dm:
                for fmt in ("%d %B %Y", "%d %b %Y"):
                    try:
                        change_date = datetime.strptime(dm.group(1), fmt).date().isoformat()
                        break
                    except ValueError:
                        pass

            trail_div = row.find("div", class_="book-trail")
            changed_pages.append({
                "title": title,
                "url": full_url,
                "change_date": change_date,
                "breadcrumb": trail_div.get_text(strip=True) if trail_div else "",
            })

        pager = soup.find("nav", class_="pager")
        if pager:
            next_link = pager.find("a", string=re.compile(r"next|›", re.IGNORECASE))
            if next_link and next_link.get("href"):
                page_url = urljoin(BASE_URL, next_link["href"])
                params = None
                page_num += 1
                time.sleep(REQUEST_DELAY)
                continue
        break

    log.info(f"Total TR changed pages found: {len(changed_pages)}")
    return changed_pages


# ═══════════════════════════════════════════════════════════════════════════════
#  DOC PATH EXTRACTION — reads sidebar active trail (same as full crawler)
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_doc_path_from_page(soup: BeautifulSoup, category: str = "") -> List[str]:
    """
    Build full doc_path by reading the sidebar active trail.

    The active trail is the set of <li class="menu-item--active-trail"> elements
    in the book-block-menu nav. Each one has a direct <a> with the node title.
    Walking them in document order gives the exact same path the sidebar crawler
    builds — e.g.:
      ["CBB Rulebook",
       "Central Bank of Bahrain Volume 5—Specialised Licensees",
       "Ad-hoc Communications",
       "Guidelines on Loan Deferral and Liquidity Support Program_14 April 2026"]

    Falls back to Location breadcrumb, then [REGULATOR, category].
    """
    # Primary: sidebar active trail
    for nav in soup.find_all("nav", id=re.compile(r"^book-block-menu-")):
        active_lis = nav.find_all("li", class_=re.compile(r"menu-item--active-trail"))
        if not active_lis:
            continue
        trail = []
        for li in active_lis:
            a = li.find("a", href=True, recursive=False)
            if a:
                text = a.get_text(strip=True)
                if text:
                    trail.append(text)
        if trail:
            return ["CBB Rulebook"] + trail

    # Fallback: Location breadcrumb (<nav class="breadcrumb">)
    crumb_nav = soup.find("nav", class_="breadcrumb")
    if crumb_nav:
        items = [
            a.get_text(strip=True)
            for a in crumb_nav.find_all("a")
            if a.get_text(strip=True)
        ]
        if items:
            return [REGULATOR, "CBB Rulebook"] + items

    return [REGULATOR, category]


# ═══════════════════════════════════════════════════════════════════════════════
#  PART B: CBB.GOV.BH HASH COMPARISON
# ═══════════════════════════════════════════════════════════════════════════════

def _get_laws_and_regulations_hashes() -> List[Dict]:
    soup = _fetch(LAWS_REGULATIONS_URL)
    if not soup:
        return []

    items = []
    for header_div in soup.find_all(
        "div",
        id=re.compile(r"^uvc-exp-wrap-\d+$"),
        class_=re.compile(r"ult_exp_section"),
    ):
        title_el = header_div.find("div", class_="ult_expheader")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            continue

        content_div = header_div.find_next_sibling("div", class_="ult_exp_content")
        if not content_div:
            continue

        content_text = content_div.get_text(separator=" ", strip=True)
        content_html = str(content_div)
        content_hash = hashlib.md5(content_text.encode("utf-8")).hexdigest()

        items.append({
            "title":        title,
            "url":          LAWS_REGULATIONS_URL,
            "content_hash": content_hash,
            "content_text": content_text,
            "content_html": content_html,       # ← fixed: was missing
            "source":       "laws_regulations",
        })

    log.info(f"Laws & Regulations: {len(items)} sections hashed")
    return items


def _get_compliance_hashes() -> List[Dict]:
    soup = _fetch(COMPLIANCE_URL)
    if not soup:
        return []

    items = []

    for section_id, section_name in [("aml", "AML"), ("eofi", "EOFI")]:
        section_div = soup.find("div", id=section_id)
        if not section_div:
            log.warning(f"Could not find #{section_id} on compliance page")
            continue

        layers = section_div.find_all("div", class_="ult_exp_section_layer")
        log.info(f"  Found {len(layers)} accordion layers in {section_name} section")

        for layer in layers:
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
                continue

            content_text = content_div.get_text(separator=" ", strip=True)
            content_html = str(content_div)                  # ← fixed: was missing
            content_hash = hashlib.md5(content_text.encode("utf-8")).hexdigest()

            items.append({
                "title":        accordion_title,
                "url":          COMPLIANCE_URL,
                "content_hash": content_hash,
                "content_text": content_text,
                "content_html": content_html,                # ← fixed: was missing
                "source":       "compliance",
                "section":      section_name,
                "doc_path":     [REGULATOR, "Compliance", section_name, accordion_title],
            })

    log.info(f"Compliance: {len(items)} accordion sections hashed")
    return items


# ═══════════════════════════════════════════════════════════════════════════════
#  CONTENT EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_content(soup: BeautifulSoup) -> Dict:
    content_block = soup.find("div", class_="field--name-body")
    document_html = None
    content_text  = ""
    download_links = []
    english_pdf = None
    arabic_pdf  = None

    if content_block:
        _make_absolute(content_block)
        document_html = str(content_block)
        content_text  = content_block.get_text(separator=" ", strip=True)

        for a in content_block.find_all("a", href=True):
            full_url    = a["href"]
            parent_text = (a.parent.get_text() if a.parent else "").lower()
            lang = "arabic" if "arabic" in parent_text else "english"
            download_links.append({
                "text":     a.parent.get_text(strip=True) if a.parent else "",
                "url":      full_url,
                "language": lang,
                "type":     "pdf" if full_url.lower().endswith(".pdf") else "link",
            })
            if lang == "english" and not english_pdf:
                english_pdf = full_url
            if lang == "arabic" and not arabic_pdf:
                arabic_pdf = full_url

    return {
        "document_html": document_html,
        "content_text":  content_text,
        "download_links": download_links,
        "english_pdf":   english_pdf,
        "arabic_pdf":    arabic_pdf,
    }


def _detect_book_category(soup: BeautifulSoup) -> str:
    for nav in soup.find_all("nav", id=re.compile(r"^book-block-menu-")):
        if nav.find(class_=re.compile(r"menu-item--active-trail")):
            first_a = nav.find("a", href=True)
            if first_a:
                return first_a.get_text(strip=True)
    return "CBB Rulebook"


# ═══════════════════════════════════════════════════════════════════════════════
#  SCRAPING CHANGED PAGES
# ═══════════════════════════════════════════════════════════════════════════════

def _scrape_changed_tr_page(
    url: str,
    category: str,
    monitoring_status: str,
    existing_regulation_id: Optional[int] = None,
) -> Optional[RegulatoryDocument]:
    time.sleep(REQUEST_DELAY)
    soup = _fetch(url)
    if not soup:
        return None

    title_tag = soup.find("h2", class_="page-title") or soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else url.split("/")[-1]

    # ── FIXED: use sidebar active trail for full doc_path ─────────────────────
    doc_path = _extract_doc_path_from_page(soup, category)
    # Ensure current page title is the last element
    if not doc_path or doc_path[-1] != title:
        doc_path = doc_path + [title]

    content      = _extract_content(soup)
    primary_pdf  = content["english_pdf"] or content["arabic_pdf"]
    content_hash = hashlib.md5((content["content_text"] or "").encode()).hexdigest()

    # Parse updated date from page text
    updated_date = None
    m = re.search(r"Updated\s+Date:\s*(\d{1,2}\s+\w+\s+\d{4})", soup.get_text(), re.IGNORECASE)
    if m:
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                updated_date = datetime.strptime(m.group(1).strip(), fmt).date().isoformat()
                break
            except ValueError:
                pass

    # ── FIXED: pdf_links as proper list, pdf_link as primary ─────────────────
    pdf_links = [
        {"name": l["text"] or "PDF", "url": l["url"], "language": l["language"]}
        for l in content["download_links"]
        if l["url"].lower().endswith(".pdf") or "pdf" in l["text"].lower()
    ]

    return RegulatoryDocument(
        regulator       = REGULATOR,
        source_system   = "CBB-Rulebook",
        category        = category,
        title           = title,
        document_url    = primary_pdf or url,
        urdu_url        = None,
        published_date  = updated_date,
        reference_no    = None,
        department      = None,
        year            = None,
        source_page_url = url,
        file_type       = "PDF" if primary_pdf else None,
        document_html   = content["document_html"],   # ← stored in regulations.document_html
        extra_meta      = {
            "pdf_link":              primary_pdf,           # ← primary PDF
            "pdf_links":             pdf_links,             # ← all PDFs
            "org_pdf_link":          content["english_pdf"],
            "arabic_pdf_link":       content["arabic_pdf"],
            "download_links":        content["download_links"],
            "content_text":          content["content_text"],
            "content_hash":          content_hash,
            "monitoring_status":     monitoring_status,
            "existing_regulation_id": existing_regulation_id,
        },
        doc_path = doc_path,
    )


def _create_cbb_gov_bh_doc(
    item: Dict,
    monitoring_status: str,
    existing_regulation_id: Optional[int] = None,
) -> RegulatoryDocument:
    title    = item["title"]
    source   = item["source"]
    category = "Laws & Regulations" if source == "laws_regulations" else "Compliance"
    doc_path = item.get("doc_path", [REGULATOR, category, title])

    return RegulatoryDocument(
        regulator       = REGULATOR,
        source_system   = f"CBB-{source.replace('_', '-').title()}",
        category        = category,
        title           = title,
        document_url    = item["url"],
        urdu_url        = None,
        published_date  = date.today().isoformat(),
        reference_no    = None,
        department      = None,
        year            = None,
        source_page_url = item["url"],
        file_type       = None,
        document_html   = item.get("content_html", ""),   # ← fixed: now populated
        extra_meta      = {
            "pdf_link":          None,
            "pdf_links":         [],
            "content_text":      item["content_text"],
            "content_hash":      item["content_hash"],
            "monitoring_status": monitoring_status,
            "existing_regulation_id": existing_regulation_id,
            "source_section":    source,
            "section":           item.get("section", ""),
        },
        doc_path = doc_path,
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN MONITORING CRAWLER
# ═══════════════════════════════════════════════════════════════════════════════

class CBBMonitoringCrawler:
    def __init__(self, repo, request_delay: float = REQUEST_DELAY):
        self.repo          = repo
        self.request_delay = request_delay

    def _get_last_crawl_date(self) -> date:
        try:
            last_date = self.repo.get_last_cbb_crawl_date()
            if last_date:
                if isinstance(last_date, datetime):
                    return (last_date - timedelta(days=1)).date()
                return last_date - timedelta(days=1)
        except Exception as e:
            log.warning(f"Could not get last crawl date: {e}")
        return date.today() - timedelta(days=30)

    def fetch_documents(self, timeout=None) -> List[RegulatoryDocument]:
        from_date = self._get_last_crawl_date()
        to_date   = date.today()
        log.info(f"=== CBB MONITORING: {from_date} → {to_date} ===")

        all_docs: List[RegulatoryDocument] = []

        # ── Part A: Thomson Reuters ───────────────────────────────────────────
        log.info("=== Monitoring Thomson Reuters content ===")
        tr_changes = _get_thomson_reuters_changes(from_date, to_date)

        for item in tr_changes:
            url   = item["url"]
            title = item["title"]

            existing_id = self.repo.get_regulation_id_by_source_url(url)
            if existing_id:
                stored_hash        = self.repo.get_cbb_content_hash(existing_id)
                monitoring_status  = "modified"
            else:
                stored_hash        = None
                monitoring_status  = "new"

            time.sleep(self.request_delay)
            soup = _fetch(url)
            if not soup:
                continue

            category = _detect_book_category(soup)
            doc      = _scrape_changed_tr_page(
                url=url, category=category,
                monitoring_status=monitoring_status,
                existing_regulation_id=existing_id,
            )
            if not doc:
                continue

            # Skip if content unchanged
            if monitoring_status == "modified" and stored_hash:
                new_hash = doc.extra_meta.get("content_hash")
                if new_hash == stored_hash:
                    log.info(f"  Unchanged (hash match): {title[:60]}")
                    continue

            all_docs.append(doc)

        log.info(f"TR content: {len(all_docs)} changes detected")

        # ── Part B: CBB.gov.bh hash comparison ────────────────────────────────
        log.info("=== Monitoring CBB.gov.bh content ===")

        for get_fn, path_fn in [
            (_get_laws_and_regulations_hashes, lambda t: [REGULATOR, "Laws & Regulations", t]),
            (_get_compliance_hashes,           None),
        ]:
            for item in get_fn():
                title    = item["title"]
                new_hash = item["content_hash"]
                doc_path = item.get("doc_path") or (path_fn(title) if path_fn else [REGULATOR, title])

                existing_id = self.repo.get_regulation_id_by_doc_path(doc_path)
                if existing_id:
                    stored_hash = self.repo.get_cbb_content_hash(existing_id)
                    if stored_hash == new_hash:
                        continue
                    monitoring_status = "modified"
                    log.info(f"  Modified: {title[:60]}")
                else:
                    monitoring_status = "new"
                    log.info(f"  New: {title[:60]}")

                doc = _create_cbb_gov_bh_doc(
                    item,
                    monitoring_status=monitoring_status,
                    existing_regulation_id=existing_id,
                )
                all_docs.append(doc)

        log.info(f"=== TOTAL CHANGES: {len(all_docs)} documents ===")
        return all_docs


# ═══════════════════════════════════════════════════════════════════════════════
#  PIPELINE ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def monitor_cbb_changes():
    log.info("=" * 80)
    log.info("CBB MONITORING CRAWLER STARTED")
    log.info("=" * 80)

    from storage.mssql_repo import MSSQLRepository
    from orchestrator.orchestrator import Orchestrator
    from processor.downloader import Downloader
    from processor.html_fallback_engine import HTMLFallbackEngine

    repo = MSSQLRepository({
        "server":   os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver":   os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    })

    monitoring_crawler = CBBMonitoringCrawler(repo)
    try:
        changed_docs = monitoring_crawler.fetch_documents()
        if not changed_docs:
            log.info("✓ No changes detected.")
            return {"status": "success", "changes_detected": 0,
                    "new_processed": 0, "modified_processed": 0,
                    "message": "No CBB content changes detected"}

        new_docs      = [d for d in changed_docs if d.extra_meta.get("monitoring_status") == "new"]
        modified_docs = [d for d in changed_docs if d.extra_meta.get("monitoring_status") == "modified"]
        log.info(f"Found {len(changed_docs)} changes: {len(new_docs)} new, {len(modified_docs)} modified")

        orchestrator = Orchestrator(
            crawler=monitoring_crawler, repo=repo,
            downloader=Downloader(), ocr_engine=HTMLFallbackEngine()
        )

        processed_new = []
        processed_modified = []
        errors = []

        for doc in new_docs:
            try:
                orchestrator._process_cbb_doc(doc)
                processed_new.append({"title": doc.title, "url": doc.source_page_url})
            except Exception as e:
                log.error(f"Failed NEW {doc.title[:60]}: {e}", exc_info=True)
                errors.append({"title": doc.title, "error": str(e), "type": "new"})

        for doc in modified_docs:
            try:
                orchestrator._process_cbb_doc(doc)
                processed_modified.append({
                    "title": doc.title, "url": doc.source_page_url,
                    "regulation_id": doc.extra_meta.get("existing_regulation_id"),
                })
            except Exception as e:
                log.error(f"Failed MODIFIED {doc.title[:60]}: {e}", exc_info=True)
                errors.append({"title": doc.title, "error": str(e), "type": "modified"})

        log.info(f"New: {len(processed_new)}  Modified: {len(processed_modified)}  Errors: {len(errors)}")
        return {
            "status": "success" if not errors else "partial_failure",
            "changes_detected":    len(changed_docs),
            "new_processed":       len(processed_new),
            "modified_processed":  len(processed_modified),
            "total_errors":        len(errors),
            "new_documents":       processed_new,
            "modified_documents":  processed_modified,
            "errors":              errors,
        }

    except Exception as e:
        log.error(f"CBB monitoring failed: {e}", exc_info=True)
        return {"status": "failed", "error": str(e),
                "changes_detected": 0, "new_processed": 0, "modified_processed": 0}


__all__ = ["CBBMonitoringCrawler", "monitor_cbb_changes"]