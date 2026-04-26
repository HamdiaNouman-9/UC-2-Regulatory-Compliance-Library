"""
CBB Rulebook Sidebar Crawler
============================
Follows the sidebar navigation tree exactly as rendered on the page.

Core insight
------------
The sidebar only expands the ACTIVE node's branch. So when we land on
/rulebook/common-volume we see Common Volume's direct children, but
NOT Part A's sub-children (because Part A is not the active page).

Solution: every node that appears childless in the sidebar gets its
own page fetched to check if it has children. This is done lazily
during recursion — we only fetch when we need to go deeper.

Stops BEFORE "Bahrain Bourse (BHB) Material" (exclusive).

Output per document (RulebookDoc)
----------------------------------
  title          : clean title
  url            : canonical page URL
  doc_path       : ["CBB Rulebook", <volume>, <section>, ...]
  document_html  : full HTML (page body field--name-body)
  content_text   : plain text
  content_hash   : MD5(content_text)
  is_folder      : bool
  depth          : int
  extra_meta     : {
      pdf_link    : str | None   (primary PDF link)
      pdf_links   : [{name, url}] (ALL PDF links)
      faq_link    : str | None
      other_links : [{name, url}]
      is_folder   : bool
  }
"""

import hashlib
import re
import time
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

log = logging.getLogger(__name__)

BASE_URL           = "https://cbben.thomsonreuters.com"
SIDEBAR_SEED       = f"{BASE_URL}/rulebook/common-volume"
REQUEST_DELAY      = 1.2
MAX_RETRIES        = 3
STOP_BEFORE_VOLUME = "Bahrain Bourse (BHB) Material"   # exclusive

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

_SEED_HTML_OVERRIDE: Optional[str] = None

def set_seed_html(html: str) -> None:
    global _SEED_HTML_OVERRIDE
    _SEED_HTML_OVERRIDE = html


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class RulebookDoc:
    title: str
    url: str
    doc_path: List[str]
    document_html: str
    content_text: str
    content_hash: str
    is_folder: bool
    depth: int
    extra_meta: Dict[str, Any] = field(default_factory=dict)


def _hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _fetch(url: str) -> Optional[BeautifulSoup]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            log.debug(f"  OK [{resp.status_code}] {url}")
            return BeautifulSoup(resp.content, "lxml")
        except requests.RequestException as e:
            log.warning(f"  Attempt {attempt}/{MAX_RETRIES} {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    log.error(f"  All retries exhausted: {url}")
    return None


def _abs(href: str) -> str:
    if href.startswith(("http://", "https://")):
        return href
    return urljoin(BASE_URL, href)


# ── Title cleaning ────────────────────────────────────────────────────────────

_CBB_CODE_RE = re.compile(
    r"^("
    r"[A-Z][A-Z0-9]*-[A-Z0-9]+(?:\.[A-Z0-9]+)*"
    r"|(?:Article|Principle|Section|Chapter|Rule|Paragraph)\s+\d+(?:\.\d+)*"
    r")",
    re.IGNORECASE,
)

def _clean_title(raw: str) -> str:
    title = raw.strip()
    m = _CBB_CODE_RE.match(title)
    if not m:
        return title
    code        = m.group(1)
    after_first = title[m.end():]
    sep_m       = re.match(r"^\s*[:\-\u2014]?\s*", after_first)
    candidate   = after_first[sep_m.end():]
    dup_pat     = re.compile(r"^(" + re.escape(code) + r")([:\s]|$)", re.IGNORECASE)
    if dup_pat.match(candidate):
        return candidate.strip()
    return title


# ── Link extraction ───────────────────────────────────────────────────────────

def _extract_links(soup_fragment) -> Dict[str, Any]:
    pdf_links = []
    faq_link  = None
    other     = []
    for a in soup_fragment.find_all("a", href=True):
        href  = _abs(a["href"])
        text  = a.get_text(strip=True)
        entry = {"name": text or href, "url": href}
        if href.lower().endswith(".pdf") or "pdf version" in text.lower():
            pdf_links.append({"name": text or "PDF", "url": href})
        elif "faq" in href.lower() or "faq" in text.lower():
            if not faq_link:
                faq_link = href
        other.append(entry)
    return {
        "pdf_link":    pdf_links[0]["url"] if pdf_links else None,
        "pdf_links":   pdf_links,
        "faq_link":    faq_link,
        "other_links": other,
    }


# ── Sidebar node ──────────────────────────────────────────────────────────────

@dataclass
class _Node:
    text: str
    url: str
    nav_id: str = ""
    children: List["_Node"] = field(default_factory=list)
    _is_folder_hint: bool = False
    _force_folder: bool = False


def _li_is_folder(li: Tag) -> bool:
    """
    A sidebar <li> with menu-item--collapsed or menu-item--expanded
    has an arrow icon = it IS or CAN BE a folder.
    A plain menu-item (dot icon) is a leaf.
    """
    classes = li.get("class", [])
    return (
        "menu-item--collapsed" in classes
        or "menu-item--expanded" in classes
    )


def _parse_ul(ul: Tag) -> List[_Node]:
    """
    Parse a sidebar <ul> into _Node list.
    Folder detection uses CSS classes (arrow vs dot).
    Already-expanded children are parsed immediately.
    Collapsed children have empty list — expanded lazily.
    """
    nodes = []
    for li in ul.find_all("li", recursive=False):
        a = li.find('a', href=True)
        if not a:
            continue
        text      = a.get_text(strip=True)
        url       = _abs(a["href"])
        is_folder = _li_is_folder(li)
        child_ul  = li.find("ul", recursive=False)
        children  = _parse_ul(child_ul) if (child_ul and is_folder) else []
        node      = _Node(text=text, url=url, children=children)
        node._is_folder_hint = is_folder
        nodes.append(node)
    return nodes


# ── Expand a node by fetching its page ───────────────────────────────────────

def _expand_node(url: str, nav_id: str = "") -> tuple:
    """
    Fetch the page at `url` and return (children, soup).

    Finds the <li> in the sidebar whose href matches `url`,
    then reads its direct <ul> as children using _parse_ul
    (which uses CSS classes for folder detection).
    """
    time.sleep(REQUEST_DELAY)
    soup = _fetch(url)
    if not soup:
        return [], None

    target = url.rstrip("/")

    # Search preferred nav first, then all book-block-menu navs
    navs = []
    if nav_id:
        n = soup.find("nav", id=nav_id)
        if n:
            navs.append(n)
    for n in soup.find_all("nav", id=re.compile(r"^book-block-menu-")):
        if n not in navs:
            navs.append(n)

    for nav in navs:
        for li in nav.find_all("li"):
            a = li.find('a', href=True)
            if not a:
                continue
            if _abs(a["href"]).rstrip("/") == target:
                child_ul = li.find("ul", recursive=False)
                children = _parse_ul(child_ul) if child_ul else []
                return children, soup

    return [], soup


# ── Collect top-level volumes ─────────────────────────────────────────────────

def _collect_volumes(seed_url: str) -> List[_Node]:
    """
    Parse the seed page to get all rulebook volume nodes.
    Stops BEFORE STOP_BEFORE_VOLUME.
    """
    if _SEED_HTML_OVERRIDE:
        log.info("  Using injected seed HTML")
        soup = BeautifulSoup(_SEED_HTML_OVERRIDE, "lxml")
    else:
        soup = _fetch(seed_url)
    if not soup:
        return []

    volumes: List[_Node] = []
    for nav in soup.find_all("nav", id=re.compile(r"^book-block-menu-")):
        top_ul = nav.find("ul")
        if not top_ul:
            continue
        top_li = top_ul.find("li", recursive=False)
        if not top_li:
            continue
        a = top_li.find('a', href=True)  # remove recursive=False
        if not a:
         continue
        vol_text = a.get_text(strip=True)

        if STOP_BEFORE_VOLUME.lower() in vol_text.lower():
            log.info(f"  Stopping before: {vol_text!r}")
            break

        vol_url  = _abs(a["href"])
        nav_id   = nav.get("id", "")
        child_ul = top_li.find("ul", recursive=False)
        children = _parse_ul(child_ul) if child_ul else []

        node = _Node(
            text=vol_text, url=vol_url,
            nav_id=nav_id, children=children,
        )
        node._is_folder_hint = True
        node._force_folder   = True
        log.info(f"  Volume: {vol_text!r}  ({len(children)} children in seed)")
        volumes.append(node)

    log.info(f"Found {len(volumes)} top-level volumes in sidebar")
    return volumes


# ── Content helpers ───────────────────────────────────────────────────────────

def _fetch_page_body(soup: BeautifulSoup) -> str:
    body = soup.find("div", class_="field--name-body")
    if body:
        for a in body.find_all("a", href=True):
            a["href"] = _abs(a["href"])
        return str(body)
    return ""


# ── Core recursive processor ──────────────────────────────────────────────────

def _process(
    node: _Node,
    path: List[str],
    depth: int,
    visited: set,
    results: List[RulebookDoc],
    request_delay: float,
) -> None:
    if node.url in visited:
        return
    visited.add(node.url)

    title    = _clean_title(node.text)
    cur_path = path + [title]
    indent   = "  " * depth

    children       = node.children
    page_soup      = None
    is_folder_hint = getattr(node, "_is_folder_hint", bool(children))

    # KEY LOGIC: if marked as folder but no children yet (collapsed in sidebar)
    # → fetch its own page to discover children
    if is_folder_hint and not children:
        children, page_soup = _expand_node(node.url, node.nav_id)
        for c in children:
            if not c.nav_id:
                c.nav_id = node.nav_id
        if children:
            log.debug(f"{indent}  -> expanded {len(children)} children")

    is_folder = bool(children) or getattr(node, "_force_folder", False)
    log.debug(f"{indent}{'[F]' if is_folder else '[L]'} {title}")

    html_content = ""
    content_text = ""
    link_meta    = {
        "pdf_link": None, "pdf_links": [],
        "faq_link": None, "other_links": [],
    }

    if is_folder:
        html_content = f"<div class='folder'><h2>{title}</h2></div>"
        content_text = title
    else:
        # Leaf: fetch its own page and extract field--name-body
        if page_soup is None:
            time.sleep(request_delay)
            page_soup = _fetch(node.url)
        if page_soup:
            page_body_html = _fetch_page_body(page_soup)
            if page_body_html:
                body_soup    = BeautifulSoup(page_body_html, "lxml")
                link_meta    = _extract_links(body_soup)
                html_content = page_body_html
                content_text = body_soup.get_text(separator=" ", strip=True)

    results.append(RulebookDoc(
        title         = title,
        url           = node.url,
        doc_path      = cur_path,
        document_html = html_content,
        content_text  = content_text,
        content_hash  = _hash(content_text or title),
        is_folder     = is_folder,
        depth         = depth,
        extra_meta    = {**link_meta, "is_folder": is_folder},
    ))

    # Recurse into children
    for child in children:
        if not child.nav_id and node.nav_id:
            child.nav_id = node.nav_id
        _process(child, cur_path, depth + 1, visited, results, request_delay)


# ── Public API ────────────────────────────────────────────────────────────────

def crawl_rulebook_sidebar(
    seed_url: str = SIDEBAR_SEED,
    request_delay: float = REQUEST_DELAY,
    max_volumes: Optional[int] = None,
) -> List[RulebookDoc]:
    """
    Crawl all CBB Rulebook volumes from the sidebar tree.

    Each folder node is expanded lazily by fetching its own page.
    Stops before Bahrain Bourse (BHB) Material.

    Args:
        seed_url      : Any rulebook page (sidebar is global).
        request_delay : Seconds between HTTP requests.
        max_volumes   : Limit volumes crawled (None = all).
    """
    log.info("=== CBB Rulebook Sidebar Crawler ===")
    log.info(f"Seed: {seed_url}")
    volumes = _collect_volumes(seed_url)

    if max_volumes is not None:
        volumes = volumes[:max_volumes]
        log.info(f"Limited to {max_volumes} volume(s)")

    all_docs: List[RulebookDoc] = []
    visited:  set               = set()

    for i, vol in enumerate(volumes, 1):
        log.info(f"[{i}/{len(volumes)}] === {vol.text} ===")
        _process(
            node=vol, path=["CBB Rulebook"], depth=0,
            visited=visited, results=all_docs,
            request_delay=request_delay,
        )
        folders = sum(1 for d in all_docs if d.is_folder)
        leaves  = sum(1 for d in all_docs if not d.is_folder)
        log.info(f"  Subtotal: {len(all_docs)} docs ({folders} folders, {leaves} leaves)\n")

    folders = sum(1 for d in all_docs if d.is_folder)
    leaves  = sum(1 for d in all_docs if not d.is_folder)
    log.info(f"=== Done: {len(all_docs)} total ({folders} folders, {leaves} leaves) ===")
    return all_docs