"""
Aml_crawler_v2.py
==================
CBB AML Law + Corporate Governance Crawler.

Uses the /entiresection/{node_id} endpoint to fetch the full tree in one
HTTP request, then parses it via the sibling-<ul> structure.

SOURCES handled:
  "aml"     -> AML Anti-Money Laundering & Combating Financial Crime
  "corpgov" -> Corporate Governance Code

Each section returns RulebookDocument objects with:
  title        : article/section title (e.g. "AML-A.1.1")
  url          : canonical page URL
  path         : list ["AML Law", "AML-A", "AML-A.1", "AML-A.1.1"]
  content_html : full HTML fragment (stored as document_html in DB)
  content_text : plain text
  content_hash : MD5(content_text)
  row_type     : "F" (folder) or "R" (leaf)
  depth        : int
  source_key   : "aml" or "corpgov"
  category     : human-readable category name
"""

import hashlib
import re
import time
import logging
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_URL      = "https://cbben.thomsonreuters.com"
REGULATOR     = "Central Bank of Bahrain"
REQUEST_DELAY = 1.2
MAX_RETRIES   = 3

SOURCES: Dict[str, Dict] = {
    "aml": {
        "index_url": (
            "https://cbben.thomsonreuters.com/rulebook/bahrain-anti-money-laundering-law-2001"
        ),
        "category":  "Bahrain Anti Money Laundering Law 2001",
        "root_path": "AML Law",
    },
    "corpgov": {
        "index_url": (
            "https://cbben.thomsonreuters.com/rulebook/corporate-governance-code-kingdom-bahrain"
        ),
        "category":  "The Corporate Governance Code of the Kingdom of Bahrain",
        "root_path": "Corporate Governance",
    },
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


# ─── Data model ───────────────────────────────────────────────────────────────
@dataclass
class RulebookDocument:
    title: str
    url: str
    path: List[str]          # equivalent to doc_path in rulebook crawler
    content_html: str        # stored as document_html in DB
    content_text: str
    content_hash: str
    row_type: str            # "F" = folder, "R" = leaf
    depth: int
    source_key: str
    category: str
    extra_meta: Dict[str, Any] = field(default_factory=dict)


# ─── HTTP ─────────────────────────────────────────────────────────────────────
def _fetch(url: str) -> Optional[BeautifulSoup]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            log.debug(f"[{resp.status_code}] {url}")
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)
    log.error(f"Failed after {MAX_RETRIES} attempts: {url}")
    return None


# ─── Entiresection fetching ───────────────────────────────────────────────────
def _get_node_id(index_soup: BeautifulSoup) -> Optional[str]:
    """Extract the node_id from an 'Entire section' link on the index page."""
    for a in index_soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"/entiresection/(\d+)", href)
        if m:
            return m.group(1)
    return None


def _fetch_entiresection(node_id: str) -> Optional[BeautifulSoup]:
    url = f"{BASE_URL}/entiresection/{node_id}"
    log.info(f"Fetching entiresection: {url}")
    return _fetch(url)


# ─── Content parsing ──────────────────────────────────────────────────────────
BLOCK_TAGS = {"p", "table", "div", "ul", "ol", "h1", "h2", "h3", "h4", "blockquote"}


def _make_absolute(html_str: str) -> str:
    """Make relative hrefs absolute."""
    return re.sub(
        r'href="(/[^"]*)"',
        lambda m: f'href="{BASE_URL}{m.group(1)}"',
        html_str,
    )


def _extract_li_content(li_tag: Tag) -> tuple:
    """
    Extract (content_html, content_text) from a leaf <li> tag.
    Removes: h2 headings, empty red paragraphs, wrapper <p> with block children.
    """
    html_parts = []

    for child in li_tag.children:
        if not hasattr(child, "name") or not child.name:
            continue
        if child.name == "h2":
            continue
        if child.name == "p" and "color:red" in child.get("style", ""):
            continue
        if child.name == "p" and not child.get("style") and not child.get_text(strip=True):
            continue

        # Unwrap wrapper <p> that contains block-level elements
        if child.name == "p":
            block_children = [
                c for c in child.children
                if hasattr(c, "name") and c.name and c.name.lower() in BLOCK_TAGS
            ]
            if block_children:
                for inner in child.children:
                    if not hasattr(inner, "name") or not inner.name:
                        txt = str(inner).strip()
                        if txt:
                            html_parts.append(f"<p>{txt}</p>")
                    else:
                        html_parts.append(str(inner))
                continue

        html_parts.append(str(child))

    combined = "\n".join(html_parts)
    combined = _make_absolute(combined)
    text = BeautifulSoup(combined, "html.parser").get_text(separator=" ", strip=True)
    return combined, text


# ─── Sibling-<ul> tree parser ─────────────────────────────────────────────────
def _parse_viewall_tree(
    container: Tag,
    path: List[str],
    source_key: str,
    category: str,
    results: List[RulebookDocument],
    depth: int = 0,
):
    """
    Recursively parse the sibling-<ul> structure inside <div id="viewall">.

    In CBB's HTML, sub-sections appear as sibling <ul>s after the parent <li>,
    NOT as nested children of the <li> itself.
    Pattern: <li>Title</li><ul>children</ul><ul>more-children</ul>...
    """
    if not container:
        return

    items = list(container.children)
    i = 0
    while i < len(items):
        item = items[i]

        # Skip non-tags and whitespace
        if not hasattr(item, "name") or not item.name:
            i += 1
            continue

        if item.name == "li":
            h2 = item.find("h2")
            title = h2.get_text(strip=True) if h2 else item.get_text(strip=True)
            title = title.strip()

            # Find the URL for this node
            a_tag = item.find("a", href=True)
            url = urljoin(BASE_URL, a_tag["href"]) if a_tag else ""

            current_path = path + [title]

            # Collect all sibling <ul>s immediately following this <li>
            child_uls = []
            j = i + 1
            while j < len(items):
                sibling = items[j]
                if hasattr(sibling, "name") and sibling.name == "ul":
                    child_uls.append(sibling)
                    j += 1
                elif hasattr(sibling, "name") and sibling.name == "li":
                    break
                else:
                    j += 1

            has_children = bool(child_uls)

            if has_children:
                # Folder node
                results.append(RulebookDocument(
                    title        = title,
                    url          = url,
                    path         = current_path,
                    content_html = "",
                    content_text = "",
                    content_hash = "",
                    row_type     = "F",
                    depth        = depth,
                    source_key   = source_key,
                    category     = category,
                ))
                for child_ul in child_uls:
                    _parse_viewall_tree(
                        container  = child_ul,
                        path       = current_path,
                        source_key = source_key,
                        category   = category,
                        results    = results,
                        depth      = depth + 1,
                    )
                # Skip past the child <ul>s we already processed
                i = j
            else:
                # Leaf node — extract content from <li> body
                content_html, content_text = _extract_li_content(item)
                content_hash = hashlib.md5(content_text.encode("utf-8")).hexdigest() if content_text else ""

                results.append(RulebookDocument(
                    title        = title,
                    url          = url,
                    path         = current_path,
                    content_html = content_html,
                    content_text = content_text,
                    content_hash = content_hash,
                    row_type     = "R",
                    depth        = depth,
                    source_key   = source_key,
                    category     = category,
                ))
                i += 1
        else:
            i += 1


# ─── Public API ───────────────────────────────────────────────────────────────
def crawl_rulebook(target: str = "aml") -> List[RulebookDocument]:
    """
    Crawl a CBB rulebook section using the entiresection endpoint.

    Args:
        target: "aml" or "corpgov"

    Returns:
        List of RulebookDocument objects (folders + leaves).
    """
    if target not in SOURCES:
        raise ValueError(f"Unknown target: {target!r}. Choose from {list(SOURCES.keys())}")

    cfg        = SOURCES[target]
    index_url  = cfg["index_url"]
    category   = cfg["category"]
    root_path  = cfg["root_path"]

    log.info(f"=== CBB AML/CorpGov Crawler: {target} ===")
    log.info(f"Index URL: {index_url}")

    # Step 1: fetch index page to get node_id
    index_soup = _fetch(index_url)
    if not index_soup:
        log.error("Failed to fetch index page")
        return []
    time.sleep(REQUEST_DELAY)

    node_id = _get_node_id(index_soup)
    if not node_id:
        log.warning("No entiresection link found — falling back to index page parsing")
        # Fall back: parse the index page's main content
        viewall = index_soup.find("div", class_="view-content") or index_soup.find("div", id="book-navigation-1")
        results: List[RulebookDocument] = []
        if viewall:
            for ul in viewall.find_all("ul", recursive=False):
                _parse_viewall_tree(ul, [root_path], target, category, results)
        return results

    # Step 2: fetch the full tree via entiresection
    section_soup = _fetch_entiresection(node_id)
    if not section_soup:
        log.error("Failed to fetch entiresection")
        return []
    time.sleep(REQUEST_DELAY)

    # Step 3: parse <div id="viewall">
    viewall = section_soup.find("div", id="viewall")
    if not viewall:
        # Try alternative containers
        viewall = section_soup.find("div", class_="view-content") or section_soup.find("ul")
        if not viewall:
            log.error("Could not find viewall container")
            return []

    results: List[RulebookDocument] = []
    top_ul = viewall.find("ul", recursive=False) or viewall
    _parse_viewall_tree(top_ul, [root_path], target, category, results)

    folders = sum(1 for d in results if d.row_type == "F")
    leaves  = sum(1 for d in results if d.row_type == "R")
    log.info(f"Done [{target}]: {len(results)} total ({folders} folders, {leaves} leaves)")
    return results