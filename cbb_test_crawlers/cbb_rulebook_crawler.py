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

Output per document
-------------------
  title          : clean title (duplicate section code removed)
  url            : canonical page URL
  doc_path       : ["CBB Rulebook", <volume>, <section>, ...]
  document_html  : full HTML (entiresection preferred, else page body)
  content_text   : plain text
  content_hash   : MD5(content_text)
  is_folder      : bool
  depth          : int
  extra_meta     : {
      pdf_link    : str | None
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
_CACHE = None   # set by enable_cache()

def set_seed_html(html: str) -> None:
    """Inject saved HTML for the seed page (for testing without live access)."""
    global _SEED_HTML_OVERRIDE
    _SEED_HTML_OVERRIDE = html

def enable_cache(cache_dir: str = "rulebook_cache") -> "RulebookCache":
    """
    Enable disk caching.  Call before crawl_rulebook_sidebar().
    Returns the cache object so callers can inspect stats.
    """
    global _CACHE
    from cbb_rulebook_cache import RulebookCache
    _CACHE = RulebookCache(cache_dir)
    log.info(f"Cache enabled: {cache_dir}  "
             f"({_CACHE.doc_count()} docs already done, "
             f"{_CACHE.stats()['cached_pages']} pages cached)")
    return _CACHE


# ── Data model ─────────────────────────────────────────────────────────────────

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


# ── HTTP ───────────────────────────────────────────────────────────────────────

def _fetch(url: str) -> Optional[BeautifulSoup]:
    # Check cache first
    if _CACHE is not None and _CACHE.has_page(url):
        html = _CACHE.get_page(url)
        if html:
            log.debug(f"  CACHE HIT {url}")
            return BeautifulSoup(html, "lxml")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            log.info(f"  OK [{resp.status_code}] {url}")
            html = resp.text
            if _CACHE is not None:
                _CACHE.save_page(url, html)
            return BeautifulSoup(html.encode(), "lxml")
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


# ── Title cleaning ─────────────────────────────────────────────────────────────

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


# ── Link extraction ────────────────────────────────────────────────────────────

def _extract_links(soup_fragment) -> Dict[str, Any]:
    pdf_links = []   # ALL pdf links found, in order
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
        "pdf_link":  pdf_links[0]["url"] if pdf_links else None,   # primary (first)
        "pdf_links": pdf_links,                                      # ALL pdfs
        "faq_link":  faq_link,
        "other_links": other,
    }


# ── Sidebar parsing ────────────────────────────────────────────────────────────

@dataclass
class _Node:
    text: str
    url: str
    nav_id: str = ""
    children: List["_Node"] = field(default_factory=list)


def _li_is_folder(li: Tag) -> bool:
    """
    A sidebar <li> is a folder (has children / arrow icon) when it carries
    menu-item--collapsed or menu-item--expanded.
    A plain menu-item with neither class is a leaf (dot icon).
    """
    classes = li.get("class", [])
    return "menu-item--collapsed" in classes or "menu-item--expanded" in classes


def _parse_ul(ul: Tag) -> List[_Node]:
    """
    Recursively parse a sidebar <ul> into _Node tree.

    Folder detection uses CSS classes (arrow vs dot), NOT presence of a
    child <ul> — because collapsed folders have no <ul> in the HTML yet.
    If a <ul> IS present (expanded active trail), we parse it immediately.
    """
    nodes = []
    for li in ul.find_all("li", recursive=False):
        a = li.find("a", href=True, recursive=False)
        if not a:
            continue
        text     = a.get_text(strip=True)
        url      = _abs(a["href"])
        is_folder = _li_is_folder(li)
        child_ul  = li.find("ul", recursive=False)
        # Parse already-expanded children if present; otherwise mark as folder
        # with empty children list (will be expanded lazily when processed)
        children  = _parse_ul(child_ul) if (child_ul and is_folder) else []
        node      = _Node(text=text, url=url, children=children)
        node._is_folder_hint = is_folder   # carry the hint through
        nodes.append(node)
    return nodes


def _expand_node(url: str, nav_id: str = "") -> tuple:
    """
    Fetch a page and return (children, soup) for the node at `url`.

    Walks ALL <li> elements in every book-block-menu nav to find the one
    whose href matches `url`, then reads its direct <ul> as children.
    Returning soup avoids a second HTTP request when the caller also needs
    the page content (leaf nodes).
    """
    time.sleep(REQUEST_DELAY)
    soup = _fetch(url)
    if not soup:
        return [], None

    target = url.rstrip("/")

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
            a = li.find("a", href=True, recursive=False)
            if not a:
                continue
            if _abs(a["href"]).rstrip("/") == target:
                child_ul = li.find("ul", recursive=False)
                children = _parse_ul(child_ul) if child_ul else []
                return children, soup

    return [], soup


# ── Collect top-level volumes from seed ────────────────────────────────────────

def _collect_volumes(seed_url: str) -> List[_Node]:
    """
    Parse the seed page sidebar to get all rulebook volumes.
    Stops BEFORE STOP_BEFORE_VOLUME (exclusive).
    Children are only populated for the volume that is active on the seed page.
    All other volumes will have children expanded lazily when processed.
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
        a = top_li.find("a", href=True, recursive=False)
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

        node = _Node(text=vol_text, url=vol_url, nav_id=nav_id, children=children)
        node._is_folder_hint = True   # top-level volumes are always folders
        node._force_folder    = True   # never demote a volume to leaf even if 0 children in mock
        log.info(f"  Volume: {vol_text!r}  ({len(children)} children in seed sidebar)")
        volumes.append(node)

    return volumes


# ── Entiresection content fetcher ──────────────────────────────────────────────

def _node_id_from_page(soup: BeautifulSoup) -> Optional[str]:
    for a in soup.find_all("a", href=True):
        m = re.match(r"^/entiresection/(\d+)$", a["href"])
        if m:
            return m.group(1)
    return None


def _fetch_entiresection(node_id: str) -> Optional[str]:
    url  = f"{BASE_URL}/entiresection/{node_id}"
    soup = _fetch(url)
    if not soup:
        return None
    viewall = soup.find("div", id="viewall")
    return str(viewall) if viewall else None


def _fetch_page_body(soup: BeautifulSoup) -> str:
    body = soup.find("div", class_="field--name-body")
    if body:
        for a in body.find_all("a", href=True):
            a["href"] = _abs(a["href"])
        return str(body)
    return ""


# ── Core recursive processor ───────────────────────────────────────────────────

def _doc_to_dict(doc: RulebookDoc) -> dict:
    """Serialize a RulebookDoc to a JSON-safe dict for cache storage."""
    return {
        "title":         doc.title,
        "url":           doc.url,
        "doc_path":      doc.doc_path,
        "document_html": doc.document_html,
        "content_text":  doc.content_text,
        "content_hash":  doc.content_hash,
        "is_folder":     doc.is_folder,
        "depth":         doc.depth,
        "extra_meta":    doc.extra_meta,
    }

def _doc_from_dict(d: dict) -> RulebookDoc:
    """Deserialize a RulebookDoc from cache."""
    return RulebookDoc(
        title         = d["title"],
        url           = d["url"],
        doc_path      = d["doc_path"],
        document_html = d.get("document_html", ""),
        content_text  = d.get("content_text", ""),
        content_hash  = d.get("content_hash", ""),
        is_folder     = d.get("is_folder", False),
        depth         = d.get("depth", 0),
        extra_meta    = d.get("extra_meta", {}),
    )


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

    # Skip if already completed in a previous run
    if _CACHE is not None and _CACHE.is_done(node.url):
        log.debug(f"  {'  '*depth}SKIP (cached) {node.text[:60]}")
        # Restore doc from cache
        cached = _CACHE.get_cached_doc(node.url)
        if cached:
            results.append(_doc_from_dict(cached))
        # Children are known from the _Node tree (passed in from sidebar) — no fetch needed
        children = node.children
        for child in children:
            if not child.nav_id and node.nav_id:
                child.nav_id = node.nav_id
            _process(child, path + [_clean_title(node.text)], depth + 1,
                     visited, results, request_delay)
        return

    title    = _clean_title(node.text)
    cur_path = path + [title]
    indent   = "  " * depth

    # ── Determine folder/leaf using CSS hint, then expand if needed ──────────
    # _is_folder_hint is set by _parse_ul from menu-item--collapsed/expanded.
    # If it's a folder but children list is empty (collapsed in sidebar),
    # fetch the node's own page to get its expanded children.
    # If it's a leaf, skip the expansion fetch entirely.
    children       = node.children
    page_soup      = None
    is_folder_hint = getattr(node, "_is_folder_hint", bool(children))

    if is_folder_hint and not children:
        # Potentially a folder (menu-item--collapsed/expanded) — fetch its page
        # to get its sidebar children. If 0 children come back it is a leaf
        # (the arrow icon is used for items that could have children but don't
        # at this level; confirmed leaf when the page itself has no sub-items).
        children, page_soup = _expand_node(node.url, node.nav_id)
        for c in children:
            if not c.nav_id:
                c.nav_id = node.nav_id
        if children:
            log.info(f"{indent}  -> expanded {len(children)} children from page")

    is_folder = bool(children) or getattr(node, "_force_folder", False)
    log.info(f"{indent}{'[F]' if is_folder else '[L]'} {title}")

    # ── Build document ────────────────────────────────────────────────────────
    html_content = ""
    content_text = ""
    link_meta    = {"pdf_link": None, "faq_link": None, "other_links": []}

    if is_folder:
        # Folder: lightweight placeholder — page already fetched above if needed
        html_content = f"<div class='folder'><h2>{title}</h2></div>"
        content_text = title
    else:
        # Leaf: each leaf is its OWN page on the live site (e.g. /rulebook/esg-a11).
        # We store ONLY that page's field--name-body — NOT the entiresection blob
        # which merges all siblings into one giant document.
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

    doc = RulebookDoc(
        title         = title,
        url           = node.url,
        doc_path      = cur_path,
        document_html = html_content,
        content_text  = content_text,
        content_hash  = _hash(content_text or title),
        is_folder     = is_folder,
        depth         = depth,
        extra_meta    = {**link_meta, "is_folder": is_folder},
    )
    results.append(doc)
    if _CACHE is not None:
        _CACHE.save_doc(_doc_to_dict(doc))

    # ── Recurse into children ─────────────────────────────────────────────────
    for child in children:
        # Propagate nav_id from parent volume so child expansion finds right nav
        if not child.nav_id and node.nav_id:
            child.nav_id = node.nav_id
        _process(child, cur_path, depth + 1, visited, results, request_delay)


# ── Public API ─────────────────────────────────────────────────────────────────

def crawl_rulebook_sidebar(
    seed_url: str = SIDEBAR_SEED,
    request_delay: float = REQUEST_DELAY,
    max_volumes: Optional[int] = None,
) -> List[RulebookDoc]:
    """
    Crawl CBB rulebook volumes in sidebar order, stopping before
    Bahrain Bourse (BHB) Material.

    Parameters
    ----------
    seed_url      : Starting URL — any rulebook page works (sidebar is global).
    request_delay : Seconds between HTTP requests.
    max_volumes   : Limit number of volumes crawled (None = all).

    Each node is expanded on demand: when the sidebar shows a node as
    menu-item--collapsed (arrow, potentially has children) we fetch that
    node's own page which renders the sidebar with that branch expanded.
    """
    log.info(f"=== CBB Rulebook Sidebar Crawler ===")
    log.info(f"Seed: {seed_url}")
    volumes = _collect_volumes(seed_url)

    if max_volumes:
        volumes = volumes[:max_volumes]

    log.info(f"Crawling {len(volumes)} volume(s):\n")

    all_docs: List[RulebookDoc] = []
    visited: set                = set()

    for i, vol in enumerate(volumes, 1):
        log.info(f"[{i}/{len(volumes)}] === Volume: {vol.text} ===")
        _process(
            node=vol, path=["CBB Rulebook"], depth=0,
            visited=visited, results=all_docs, request_delay=request_delay,
        )
        log.info(f"  Subtotal after this volume: {len(all_docs)} docs\n")

    log.info(f"=== Done: {len(all_docs)} total documents ===")
    return all_docs