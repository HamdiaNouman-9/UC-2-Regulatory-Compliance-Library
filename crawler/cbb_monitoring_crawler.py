"""
CBB Monitoring Crawler
========================
Checks for CBB regulation changes since the last crawl: queries Thomson
Reuters for what changed in the date range, then re-scrapes only the
affected pages (sidebar doc_path, compliance content, TR document_html/pdf_link).
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
EMPTY_HASH    = "d41d8cd98f00b204e9800998ecf8427e"  # MD5 of empty string = folder page

_SECTION_CODE_RE  = re.compile(r'^([A-Z]{1,5}-\d+[A-Z]?(?:\.\d+[A-Z]?)*)', re.IGNORECASE)
_DELETION_KEYWORDS = re.compile(r'\b(deleted|moved\s+to\s+section|removed|transferred)\b', re.IGNORECASE)


def _extract_section_code(title: str) -> Optional[str]:
    """Extract leading section code from a title, e.g. 'LR-1A.1 General' -> 'LR-1A.1'."""
    m = _SECTION_CODE_RE.match((title or "").strip())
    return m.group(1).upper() if m else None


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
            log.info(f"[{resp.status_code}] {url}")
            return BeautifulSoup(resp.content, "lxml")
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    log.error(f"All retries exhausted: {url}")
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

    base_params = {
        "f_date":              "on",
        "min":                 from_date.strftime("%Y-%m-%d"),
        "max":                 to_date.strftime("%Y-%m-%d"),
        "items_per_page":      "40",
        "sort_by":             "revision_timestamp_1",
    }

    page_num = 0

    while True:
        params = dict(base_params)
        if page_num > 0:
            params["page"] = str(page_num)

        soup = _fetch(CHANGES_URL, params=params)
        if not soup:
            break

        results_area = soup.find("div", class_="view-content")
        if not results_area:
            break

        rows = results_area.find_all("div", class_="views-row")
        if not rows:
            break

        def _row_date(r):
            """Extract date from <time datetime="..."> attribute — avoids whitespace regex issues."""
            dd = r.find("div", class_="book-detail")
            if not dd:
                return None
            t = dd.find("time", attrs={"datetime": True})
            if not t:
                return None
            try:
                return datetime.fromisoformat(t["datetime"]).date()
            except (ValueError, KeyError):
                return None

        # Check first entry (newest on page) — if it's already before from_date,
        # the entire page and all following pages are out of range
        first_date = _row_date(rows[0])
        if first_date is not None and first_date < from_date:
            log.info(f"  First entry on page {page_num + 1} dated {first_date} — before {from_date}, stopping.")
            break

        for row in rows:
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
            item_date = _row_date(row)
            change_date = item_date.isoformat() if item_date else None

            # Skip entries outside our date window
            if item_date is not None and (item_date < from_date or item_date > to_date):
                continue

            trail_div = row.find("div", class_="book-trail")
            changed_pages.append({
                "title": title,
                "url": full_url,
                "change_date": change_date,
                "breadcrumb": trail_div.get_text(strip=True) if trail_div else "",
            })

        if len(rows) < 40:
            break
        page_num += 1
        log.info(f"  Fetching page {page_num + 1} of TR changes...")
        time.sleep(REQUEST_DELAY)

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
            return [REGULATOR] + trail

    # Fallback: Location breadcrumb (<nav class="breadcrumb">)
    crumb_nav = soup.find("nav", class_="breadcrumb")
    if crumb_nav:
        items = [
            a.get_text(strip=True)
            for a in crumb_nav.find_all("a")
            if a.get_text(strip=True)
        ]
        if items:
            return [REGULATOR] + items

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
            "content_html": content_html,
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
            content_html = str(content_div)
            content_hash = hashlib.md5(content_text.encode("utf-8")).hexdigest()

            items.append({
                "title":        accordion_title,
                "url":          COMPLIANCE_URL,
                "content_hash": content_hash,
                "content_text": content_text,
                "content_html": content_html,
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


def _extract_toc_links(soup: BeautifulSoup) -> List[str]:
    """
    Get the direct children of the current page from the sidebar navigation.
    Finds the deepest active-trail <li> and returns the links in its nested <ul>.
    """
    seen, links = set(), []
    for nav in soup.find_all("nav", id=re.compile(r"^book-block-menu-")):
        active_items = nav.find_all("li", class_=re.compile(r"menu-item--active-trail"))
        if not active_items:
            continue
        last_active = active_items[-1]
        child_ul = last_active.find("ul", recursive=False)
        if not child_ul:
            continue
        for li in child_ul.find_all("li", recursive=False):
            a = li.find("a", href=True)
            if a:
                full = urljoin(BASE_URL, a["href"])
                if full not in seen:
                    seen.add(full)
                    links.append(full)
    return links


# ═══════════════════════════════════════════════════════════════════════════════
#  SCRAPING CHANGED PAGES
# ═══════════════════════════════════════════════════════════════════════════════

def _scrape_changed_tr_page(
    url: str,
    category: str,
    monitoring_status: str,
    existing_regulation_id: Optional[int] = None,
    soup: Optional[BeautifulSoup] = None,
) -> Optional[RegulatoryDocument]:
    if soup is None:
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

    source_system = "CBB-Rulebook" if "/rulebook/" in url else f"CBB-{category.replace(' ', '-')}"
    return RegulatoryDocument(
        regulator       = REGULATOR,
        source_system   = source_system,
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
        document_html   = content["document_html"],
        extra_meta      = {
            "pdf_link":              primary_pdf,
            "pdf_links":             pdf_links,
            "org_pdf_link":          content["english_pdf"],
            "arabic_pdf_link":       content["arabic_pdf"],
            "download_links":        content["download_links"],
            "content_text":          content["content_text"],
            "content_hash":          content_hash,
            "monitoring_status":     monitoring_status,
            "existing_regulation_id": existing_regulation_id,
            "depth":                 len(doc_path) - 1,
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
        document_html   = item.get("content_html", ""),
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
        self.repo           = repo
        self.request_delay  = request_delay
        self._smart_matcher = None   # initialised lazily in fetch_documents()

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

    def _handle_deletion_notice(self, url: str, title: str, visited: set) -> None:
        """
        Called when a page has empty content AND title contains deletion/moved keywords
        with no TOC children. Finds all matching old records and marks them deleted.

        Priority:
          1. URL-based lookup → marks the reg + its compliancecategory children
          2. Section-code prefix fallback → marks all title-matched records (section + sub-sections)
        """
        ids_to_delete: List[int] = []

        # 1. URL-based lookup
        existing_id = self.repo.get_regulation_id_by_source_url(url)
        if existing_id:
            ids_to_delete.append(existing_id)
            for child_url in self.repo.get_child_source_urls(url):
                child_id = self.repo.get_regulation_id_by_source_url(child_url)
                if child_id and child_id not in ids_to_delete:
                    ids_to_delete.append(child_id)
            log.info(f"    URL match: will delete {len(ids_to_delete)} record(s)")
        else:
            # 2. Section-code fallback (handles URL changes)
            section_code = _extract_section_code(title)
            if not section_code:
                log.warning(f"    Cannot identify section from title: {title[:60]}")
                return
            ids_to_delete = self.repo.find_regulation_ids_by_section_code_prefix(section_code)
            if not ids_to_delete:
                log.warning(f"    No records found for [{section_code}] — may already be deleted or not yet crawled")
                return
            log.info(f"    Section-code match [{section_code}]: will delete {len(ids_to_delete)} record(s)")

        for reg_id in ids_to_delete:
            log.info(f"    Marking deleted: reg_id={reg_id}")
            self.repo.mark_regulation_deleted(reg_id)

    def _handle_folder_change(
        self,
        url: str,
        soup: BeautifulSoup,
        category: str,
        all_docs: List,
        visited: set,
    ) -> None:
        """
        Called when TR reports a folder page (empty content) as changed.

        Algorithm per child found in the sidebar TOC:
          - Not in DB             → NEW: scrape & add; if also a folder, recurse
          - In DB, empty hash     → existing FOLDER: recurse to check subtree
          - In DB, has content    → existing LEAF: TR reports these separately, skip
          (Deleted children — in DB but missing from TOC — are flagged as a warning;
           full deletion handling is a future enhancement.)
        """
        if url in visited:
            return
        visited.add(url)

        toc_urls = _extract_toc_links(soup)
        if not toc_urls:
            log.info(f"    Folder has no TOC children in sidebar: {url}")
            return

        log.info(f"    Folder has {len(toc_urls)} direct children — checking each")

        for child_url in toc_urls:
            if child_url in visited:
                continue

            existing_id  = self.repo.get_regulation_id_by_source_url(child_url)
            stored_hash  = self.repo.get_cbb_content_hash(existing_id) if existing_id else None

            if not existing_id:
                # ── NEW child ────────────────────────────────────────────────
                log.info(f"    NEW child: {child_url}")
                time.sleep(self.request_delay)
                child_soup = _fetch(child_url)
                if not child_soup:
                    continue
                child_category = _detect_book_category(child_soup)

                child_content  = _extract_content(child_soup)
                child_hash     = hashlib.md5((child_content["content_text"] or "").encode()).hexdigest()
                child_toc      = _extract_toc_links(child_soup)

                if child_hash == EMPTY_HASH:
                    child_title_tag = child_soup.find("h2", class_="page-title") or child_soup.find("h1")
                    child_title     = child_title_tag.get_text(strip=True) if child_title_tag else child_url.split("/")[-1]
                    is_child_deletion = bool(_DELETION_KEYWORDS.search(child_title)) and not child_toc
                    if is_child_deletion:
                        log.info(f"    NEW child is a DELETION NOTICE: {child_title[:60]}")
                        self._handle_deletion_notice(child_url, child_title, visited)
                        visited.add(child_url)
                        continue
                    else:
                        # New folder child — recurse into its sub-children
                        self._handle_folder_change(child_url, child_soup, child_category, all_docs, visited)
                else:
                    # New leaf child with content — add to processing queue
                    doc = _scrape_changed_tr_page(
                        url=child_url,
                        category=child_category,
                        monitoring_status="new",
                        existing_regulation_id=None,
                        soup=child_soup,
                    )
                    if doc:
                        all_docs.append(doc)

            elif stored_hash == EMPTY_HASH:
                # ── Existing FOLDER child → recurse ──────────────────────────
                log.info(f"    Recursing into existing folder: {child_url}")
                time.sleep(self.request_delay)
                child_soup = _fetch(child_url)
                if child_soup:
                    child_category = _detect_book_category(child_soup)
                    self._handle_folder_change(child_url, child_soup, child_category, all_docs, visited)

            # else: existing leaf → TR reports content changes separately, skip here

        # Deleted children: in DB but no longer in TOC
        db_urls = set(self.repo.get_child_source_urls(url))
        deleted = db_urls - set(toc_urls)
        for deleted_url in deleted:
            existing_id = self.repo.get_regulation_id_by_source_url(deleted_url)
            if existing_id:
                log.info(f"    DELETED child detected: {deleted_url} (reg_id={existing_id})")
                self.repo.mark_regulation_deleted(existing_id)

    def fetch_documents(self, timeout=None, from_date=None, to_date=None) -> List[RegulatoryDocument]:
        if from_date is None:
            from_date = self._get_last_crawl_date()
        if to_date is None:
            to_date = date.today()
        if isinstance(from_date, str):
            from_date = date.fromisoformat(from_date)
        if isinstance(to_date, str):
            to_date = date.fromisoformat(to_date)
        log.info(f"=== CBB MONITORING: {from_date} to {to_date} ===")

        # Initialise smart matcher once per run (loads all regs + cats from DB)
        if self._smart_matcher is None:
            from crawler.smart_matcher import SmartMatcher
            self._smart_matcher = SmartMatcher(self.repo)

        all_docs: List[RegulatoryDocument] = []

        # ── Part A: Thomson Reuters ───────────────────────────────────────────
        log.info("=== Monitoring Thomson Reuters content ===")
        tr_changes = _get_thomson_reuters_changes(from_date, to_date)
        visited: set = set()  # track URLs already handled (avoids re-processing in folder recursion)

        for item in tr_changes:
            url   = item["url"]
            title = item["title"]

            if url in visited:
                continue

            time.sleep(self.request_delay)
            soup = _fetch(url)
            if not soup:
                continue

            category = _detect_book_category(soup)

            # ── Detect folder page or deletion notice (both have empty body) ─
            content    = _extract_content(soup)
            page_hash  = hashlib.md5((content["content_text"] or "").encode()).hexdigest()

            if page_hash == EMPTY_HASH:
                toc_children     = _extract_toc_links(soup)
                is_deletion_notice = bool(_DELETION_KEYWORDS.search(title)) and not toc_children
                if is_deletion_notice:
                    log.info(f"  DELETION NOTICE: {title[:60]} — marking old records deleted")
                    self._handle_deletion_notice(url, title, visited)
                else:
                    log.info(f"  Folder page: {title[:60]} — checking children")
                    self._handle_folder_change(url, soup, category, all_docs, visited)
                visited.add(url)
                continue

            # ── Normal leaf page — smart identity matching ───────────────
            url_existing_id = self.repo.get_regulation_id_by_source_url(url)

            match = self._smart_matcher.match(
                title=title,
                url=url,
                doc_path=_extract_doc_path_from_page(soup, category),
                content_text=content["content_text"] or "",
                soup=soup,
                url_existing_id=url_existing_id,
            )

            monitoring_status = match["monitoring_status"]
            existing_id       = match["existing_id"]

            if monitoring_status == "unchanged":
                log.info(f"  UNCHANGED [{match['method']}]: {title[:60]}")
                visited.add(url)
                continue

            log.info(
                f"  {monitoring_status.upper()} [{match['method']} "
                f"conf={match['confidence']:.2f}]: {title[:60]}"
            )

            doc = _scrape_changed_tr_page(
                url=url, category=category,
                monitoring_status=monitoring_status,
                existing_regulation_id=existing_id,
                soup=soup,
            )
            if not doc:
                continue

            visited.add(url)
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


# The standalone entry point `monitor_cbb_changes` was retired 2026-09-21: CBB runs
# through config/sources/cbb.yml (monitor_cbb) like every other regulator. The
# helpers above are still imported by crawler/smart_matcher.py and the tests.
