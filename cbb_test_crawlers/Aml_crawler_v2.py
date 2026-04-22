"""
CBB Rulebook Crawler — Multi-source
====================================
Supports ANY CBB Thomson Reuters rulebook URL using a single crawler.

Pre-configured sources (pass target= to crawl_rulebook()):
  "aml"      → Bahrain Anti Money Laundering Law 2001
  "corpgov"  → Corporate Governance Code of the Kingdom of Bahrain
  <url>      → Any other CBB rulebook URL (auto-detects category from page title)

Key features:
  • <span class="me"> unwrap merges adjacent text nodes (no "combatin g" artefacts)
  • Wrapper-<p> unwrap preserves nested tables/divs (html.parser strategy)
  • Full inline styles — no external stylesheet needed
  • All links made absolute
  • <glossary> → <em>
  • Title deduplication handles: AML-X, Article N, Principle N, Section N, Chapter N …
  • No HTML truncation — full content stored
"""

import hashlib
import re
import time
import logging
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag, NavigableString

log = logging.getLogger(__name__)

BASE_URL    = "https://cbben.thomsonreuters.com"
MAX_RETRIES = 3
REQUEST_DELAY = 1.2

# ─── Pre-configured sources ────────────────────────────────────────────────────
SOURCES: Dict[str, Dict] = {
    "aml": {
        "url":      f"{BASE_URL}/rulebook/bahrain-anti-money-laundering-law-2001",
        "category": "Bahrain Anti Money Laundering Law 2001",
        "system":   "CBB-AML-LAW",
    },
    "corpgov": {
        "url":      f"{BASE_URL}/rulebook/corporate-governance-code-kingdom-bahrain",
        "category": "The Corporate Governance Code of the Kingdom of Bahrain",
        "system":   "CBB-CORPGOV",
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
    "Referer": BASE_URL + "/",
})


# ─── Data model ────────────────────────────────────────────────────────────────
@dataclass
class RulebookDocument:
    title: str
    raw_title: str
    url: str
    depth: int
    path: List[str]
    content_html: str       # raw fragment for DB storage
    content_text: str       # plain text for search / hashing
    content_hash: str       # MD5(content_text)
    row_type: str           # 'F' = folder/section, 'R' = regulation/leaf
    is_leaf: bool
    doc_path_str: str
    source_key: str         # which source this came from, e.g. "aml", "corpgov"
    category: str           # human-readable category label


def _hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ─── Title cleaning ────────────────────────────────────────────────────────────
# Matches ANY CBB section code at position 0 of a title string.
#
# Covers:
#   PREFIX-PART(.PART)*  → AML-A, AML-A.1, AML-2A, HC-10.5, OFS-1.2 …
#   Article N            → Article 1, Article 38
#   Principle N          → Principle 1, Principle 12  (corp gov)
#   Section N            → Section 1
#   Chapter N            → Chapter 3
#   Rule N               → Rule 5
#   Paragraph N          → Paragraph 2
#
# Any new CBB code that matches these patterns is auto-handled.
_CBB_CODE_RE = re.compile(
    r"^("
    r"[A-Z][A-Z0-9]*-[A-Z0-9]+(?:\.[A-Z0-9]+)*"               # PREFIX-PART(.PART)*
    r"|"
    r"(?:Article|Principle|Section|Chapter|Rule|Paragraph)\s+\d+(?:\.\d+)*"  # Keyword N or N.N
    r")",
    re.IGNORECASE,
)


def _clean_title(raw: str) -> str:
    """
    Remove duplicated section codes from CBB <h2> titles.

    The CBB entiresection page repeats the code at the start of every heading:
        'AML-A.1 AML-A.1 Purpose'           →  'AML-A.1 Purpose'
        'AML-2A AML-2A: Money Transfers'    →  'AML-2A: Money Transfers'
        'Article 8 Article 8 Provisions'    →  'Article 8 Provisions'
        'Principle 1 Principle 1 Board …'   →  'Principle 1 Board …'
        'HC-10 HC-10 Corporate Governance'  →  'HC-10 Corporate Governance'

    Titles without a duplicate code (leaf paragraphs, plain section names)
    are returned unchanged.
    """
    title = raw.strip()

    m = _CBB_CODE_RE.match(title)
    if not m:
        return title

    code         = m.group(1)
    after_first  = title[m.end():]

    # Allow optional whitespace + optional separator  ( : - — ) between copies
    sep_m     = re.match(r"^\s*[:\-\u2014]?\s*", after_first)
    candidate = after_first[sep_m.end():]

    # Check whether the same code appears again immediately
    dup_pat = re.compile(
        r"^(" + re.escape(code) + r")([:\s]|$)", re.IGNORECASE
    )
    if dup_pat.match(candidate):
        return candidate.strip()

    return title


# ─── HTTP helper ───────────────────────────────────────────────────────────────
def _fetch(url: str) -> Optional[BeautifulSoup]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            log.info(f"  ✓ [{resp.status_code}] {url}")
            return BeautifulSoup(resp.content, "lxml")
        except requests.RequestException as e:
            log.warning(f"  ✗ Attempt {attempt}/{MAX_RETRIES} for {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
    log.error(f"  ✗ All retries exhausted: {url}")
    return None


def _abs_url(href: str) -> str:
    if href.startswith(("http://", "https://")):
        return href
    return urljoin(BASE_URL, href)


# ─── Node ID extraction ────────────────────────────────────────────────────────
def _extract_node_id(soup: BeautifulSoup) -> Optional[str]:
    """Find the /entiresection/<id> link anywhere on the page."""
    for a in soup.find_all("a", href=True):
        m = re.match(r"^/entiresection/(\d+)$", a["href"])
        if m:
            return m.group(1)
    return None


# ─── Section link discovery ────────────────────────────────────────────────────
def _get_section_links(soup: BeautifulSoup, base_url: str) -> List[Dict]:
    """
    Collect child section links from the sidebar navigation.
    Used as fallback when a single node_id / entiresection is not found.
    """
    links: List[Dict] = []
    seen: set = set()

    def _add(text: str, href: str) -> None:
        url = _abs_url(href)
        if url not in seen and BASE_URL in url and url != base_url:
            seen.add(url)
            links.append({"text": text.strip(), "url": url})

    nav_div = soup.find("div", id="book-navigation-1")
    if nav_div:
        for a in nav_div.find_all("a", href=True):
            _add(a.get_text(strip=True), a["href"])

    if not links:
        for nav in soup.find_all("nav", id=re.compile(r"book-block-menu")):
            for a in nav.find_all("a", href=True):
                if _abs_url(a["href"]).rstrip("/") == base_url.rstrip("/"):
                    parent_li = a.find_parent("li")
                    if parent_li:
                        child_ul = parent_li.find("ul")
                        if child_ul:
                            for ca in child_ul.find_all("a", href=True):
                                _add(ca.get_text(strip=True), ca["href"])
                    break

    return links


# ─── Span unwrap — merge adjacent text nodes ───────────────────────────────────
def _unwrap_me_spans(root) -> None:
    """
    Unwrap <span class="me"> without creating spurious spaces mid-word.

    CBB source uses these spans to wrap single characters:
        combatin<span class="me">g</span>  →  should become "combating"
    A naive unwrap() leaves "combatin g" because BS4 keeps them as separate
    NavigableString siblings.  We merge runs of consecutive text nodes after
    unwrapping.
    """
    for span in root.find_all("span", class_="me"):
        span.unwrap()
    _merge_text_nodes(root)


def _merge_text_nodes(tag: Tag) -> None:
    """Recursively merge consecutive NavigableString siblings."""
    if not hasattr(tag, "contents"):
        return
    i = 0
    while i < len(tag.contents):
        child = tag.contents[i]
        if isinstance(child, NavigableString):
            j = i + 1
            parts = [str(child)]
            while j < len(tag.contents) and isinstance(tag.contents[j], NavigableString):
                parts.append(str(tag.contents[j]))
                j += 1
            if j > i + 1:
                child.replace_with(NavigableString("".join(parts)))
                for _ in range(j - i - 2):
                    tag.contents[i + 1].extract()
        else:
            _merge_text_nodes(child)
        i += 1


# ─── Inline style application ──────────────────────────────────────────────────
def _apply_inline_styles(html_fragment: str) -> str:
    """
    Convert CBB CSS class-based styles to inline styles so the fragment
    renders correctly in any viewer (browser, email, Word) without an
    external stylesheet.

    Also:
      • Unwraps <span class="me"> cleanly
      • Converts <glossary type="i"> → <em>
      • Makes all relative href/src absolute
    """
    soup = BeautifulSoup(html_fragment, "html.parser")

    # <span class="me"> — merge text nodes to avoid split words
    _unwrap_me_spans(soup)

    # ── Tables ────────────────────────────────────────────────────────────────
    for table in soup.find_all("table", class_="df_table"):
        table["style"] = (
            "border-collapse:collapse;"
            "width:100%;"
            "margin:12px 0;"
            "font-family:Arial,sans-serif;"
            "font-size:12px;"
        )
        table["cellspacing"] = "0"
        table["cellpadding"] = "0"
        for tr in table.find_all("tr"):
            for td in tr.find_all("td"):
                base = (
                    "border:1px solid #c0c0c0;"
                    "padding:6px 10px;"
                    "vertical-align:top;"
                    "font-family:Arial,sans-serif;"
                    "font-size:12px;"
                )
                if "df_table_main" in td.get("class", []):
                    td["style"] = (
                        base
                        + "background-color:#d3d3d3;"
                        + "font-weight:bold;"
                        + "text-align:center;"
                    )
                else:
                    align   = td.get("align", "")
                    extra   = f"text-align:{align};" if align else ""
                    existing = td.get("style", "")
                    td["style"] = base + extra + existing

    # ── Amendment history boxes ───────────────────────────────────────────────
    for div in soup.find_all("div", class_="FSAmnd"):
        div["style"] = (
            "background-color:#dce9f7;"
            "border:1px solid #a8c4e0;"
            "color:#1a4a7a;"
            "font-size:11px;"
            "padding:5px 10px;"
            "margin-top:10px;"
            "display:inline-block;"
            "font-family:Arial,sans-serif;"
            "border-radius:3px;"
            "line-height:1.6;"
        )

    # ── Sub-item lists (a), (b) … ─────────────────────────────────────────────
    for div in soup.find_all("div", class_="list"):
        div["style"] = (
            "margin-left:24px;"
            "margin-bottom:4px;"
            "font-family:Arial,sans-serif;"
            "font-size:13px;"
            "line-height:1.6;"
        )

    # ── <glossary> → <em> ─────────────────────────────────────────────────────
    for g in soup.find_all("glossary"):
        g.name = "em"
        for attr in list(g.attrs.keys()):
            del g.attrs[attr]

    # ── Paragraphs ────────────────────────────────────────────────────────────
    for p in soup.find_all("p"):
        if not p.get("style"):
            p["style"] = (
                "font-family:Arial,sans-serif;"
                "font-size:13px;"
                "line-height:1.6;"
                "margin:6px 0;"
            )

    # ── Bold ──────────────────────────────────────────────────────────────────
    for tag in soup.find_all(["strong", "b"]):
        if not tag.get("style"):
            tag["style"] = "font-family:Arial,sans-serif;"

    # ── Links — make absolute ─────────────────────────────────────────────────
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            a["href"] = BASE_URL + href
        elif not href.startswith("http"):
            a["href"] = BASE_URL + "/" + href.lstrip("/")
        if not a.get("style"):
            a["style"] = "color:#0563c1;"

    # ── Images ────────────────────────────────────────────────────────────────
    for img in soup.find_all("img", src=True):
        if img["src"].startswith("/"):
            img["src"] = BASE_URL + img["src"]

    return str(soup)


def wrap_html_for_display(fragment: str, title: str = "") -> str:
    """
    Wrap a raw HTML fragment in a fully self-contained HTML document.
    Uses only inline styles — no <style> block — so it renders identically
    in browsers, html.onlineviewer.net, and email clients.
    """
    styled = _apply_inline_styles(fragment)
    body_style = (
        "font-family:Arial,sans-serif;"
        "font-size:13px;"
        "color:#333;"
        "max-width:960px;"
        "padding:20px;"
        "line-height:1.6;"
    )
    return (
        f'<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        f'<meta charset="utf-8">\n'
        f'<title>{title}</title>\n'
        f'</head>\n'
        f'<body style="{body_style}">\n'
        f'{styled}\n'
        f'</body>\n</html>'
    )


# ─── Content extraction from <li> ─────────────────────────────────────────────
_BLOCK_TAGS = {"p", "table", "div", "ul", "ol", "h1", "h2", "h3", "h4", "blockquote", "pre"}


def _extract_li_content(li_tag: Tag) -> Tuple[str, str]:
    """
    Extract the content HTML and plain text from a <li> in the entiresection page.

    Uses html.parser (not lxml) so nested <p><table>…</table></p> structures
    are preserved rather than being silently closed by a strict parser.

    Wrapper <p> nodes — outer <p> tags that contain block-level children — are
    detected and unwrapped so their inner children are emitted directly.
    """
    li_soup = BeautifulSoup(str(li_tag), "html.parser").find("li") or li_tag
    html_parts: List[str] = []

    for child in li_soup.children:
        if not hasattr(child, "name") or not child.name:
            continue

        # Skip the section heading — stored separately as doc.title
        if child.name == "h2":
            continue

        # Skip the red italic CBB boilerplate warning paragraph
        if child.name == "p" and child.get("style") and "color:red" in child["style"]:
            continue

        # Skip empty unstyled <p> (lxml artefact from nested <p><p>…</p></p>)
        if child.name == "p" and not child.get("style") and not child.get_text(strip=True):
            continue

        # Detect a wrapper <p> that still contains block-level children
        if child.name == "p":
            block_children = [
                c for c in child.children
                if hasattr(c, "name") and c.name and c.name.lower() in _BLOCK_TAGS
            ]
            if block_children:
                # Unwrap: emit each inner child directly
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

    # Make relative links absolute
    combined = re.sub(
        r'href="(/[^"]*)"',
        lambda m: f'href="{BASE_URL}{m.group(1)}"',
        combined,
    )
    combined = re.sub(
        r'src="(/[^"]*)"',
        lambda m: f'src="{BASE_URL}{m.group(1)}"',
        combined,
    )

    text = BeautifulSoup(combined, "html.parser").get_text(separator=" ", strip=True)
    return combined, text


# ─── Tree parser ───────────────────────────────────────────────────────────────
def _parse_ul(
    ul: Tag,
    path: List[str],
    depth: int,
    section_url: str,
    source_key: str,
    category: str,
    results: List[RulebookDocument],
) -> None:
    children = [
        c for c in ul.children
        if hasattr(c, "name") and c.name in ("li", "ul")
    ]
    i = 0
    while i < len(children):
        el = children[i]

        if el.name == "li":
            h2 = el.find("h2")
            raw_title = h2.get_text(strip=True) if h2 else ""
            if not raw_title:
                i += 1
                continue

            clean_t      = _clean_title(raw_title)
            content_html, content_text = _extract_li_content(el)

            # Collect consecutive <ul> siblings = child sections of this node
            child_uls: List[Tag] = []
            j = i + 1
            while j < len(children) and children[j].name == "ul":
                child_uls.append(children[j])
                j += 1

            has_children = bool(child_uls)
            current_path = path + [clean_t]

            results.append(RulebookDocument(
                title        = clean_t,
                raw_title    = raw_title,
                url          = section_url,
                depth        = depth,
                path         = current_path,
                content_html = content_html,
                content_text = content_text,
                content_hash = _hash(content_text or clean_t),
                row_type     = "F" if has_children else "R",
                is_leaf      = not has_children,
                doc_path_str = " > ".join(current_path),
                source_key   = source_key,
                category     = category,
            ))

            for child_ul in child_uls:
                _parse_ul(child_ul, current_path, depth + 1,
                          section_url, source_key, category, results)
            i = j

        elif el.name == "ul":
            _parse_ul(el, path, depth, section_url, source_key, category, results)
            i += 1
        else:
            i += 1


# ─── Entiresection fetcher ─────────────────────────────────────────────────────
def _crawl_entiresection(
    node_id: str,
    path_prefix: List[str],
    source_key: str,
    category: str,
) -> List[RulebookDocument]:
    url  = f"{BASE_URL}/entiresection/{node_id}"
    soup = _fetch(url)
    if not soup:
        return []

    viewall = soup.find("div", id="viewall")
    if not viewall:
        log.warning(f"  No #viewall at {url}")
        return []

    top_ul = viewall.find("ul")
    if not top_ul:
        log.warning(f"  No <ul> in #viewall at {url}")
        return []

    docs: List[RulebookDocument] = []
    _parse_ul(top_ul, path_prefix, 0, url, source_key, category, docs)
    log.info(f"  → {len(docs)} documents from entiresection/{node_id}")
    return docs


# ─── Per-section fallback ──────────────────────────────────────────────────────
def _crawl_section_page(
    section: Dict,
    source_key: str,
    category: str,
) -> List[RulebookDocument]:
    time.sleep(REQUEST_DELAY)
    soup = _fetch(section["url"])
    if not soup:
        return []
    node_id = _extract_node_id(soup)
    if not node_id:
        log.warning(f"  No node_id for {section['url']}")
        return []
    time.sleep(REQUEST_DELAY)
    return _crawl_entiresection(
        node_id,
        path_prefix=[category, section["text"]],
        source_key=source_key,
        category=category,
    )


# ─── Public entry point ────────────────────────────────────────────────────────
def crawl_rulebook(target: str = "aml") -> List[RulebookDocument]:
    """
    Crawl a CBB Thomson Reuters rulebook and return all documents.

    Parameters
    ----------
    target : str
        Either a pre-configured key ("aml", "corpgov") or a full
        CBB rulebook URL (e.g. "https://cbben.thomsonreuters.com/rulebook/…").

    Returns
    -------
    List[RulebookDocument]
        Deduplicated list of all sections and leaf paragraphs found.

    Examples
    --------
    >>> docs = crawl_rulebook("aml")
    >>> docs = crawl_rulebook("corpgov")
    >>> docs = crawl_rulebook("https://cbben.thomsonreuters.com/rulebook/bahrain-financial-trust-law-2006")
    """
    # Resolve target to url / category / source_key
    if target in SOURCES:
        cfg        = SOURCES[target]
        start_url  = cfg["url"]
        category   = cfg["category"]
        source_key = target
    elif target.startswith("http"):
        start_url  = target
        source_key = "custom"
        category   = ""          # will be filled from page title below
    else:
        raise ValueError(
            f"Unknown target {target!r}. "
            f"Use one of {list(SOURCES)} or pass a full URL."
        )

    log.info(f"=== CBB Crawler starting: {start_url} ===")

    index_soup = _fetch(start_url)
    if not index_soup:
        log.error("Failed to fetch index page")
        return []

    # Auto-detect category from page <title> for ad-hoc URLs
    if not category:
        title_tag = index_soup.find("title")
        if title_tag:
            raw = title_tag.get_text(strip=True)
            # "Entire Section | Rulebook" or "Page Title | Rulebook"
            category = raw.split("|")[0].strip() or raw
        else:
            category = start_url.rstrip("/").split("/")[-1].replace("-", " ").title()
        log.info(f"  Auto-detected category: {category!r}")

    node_id       = _extract_node_id(index_soup)
    section_links = _get_section_links(index_soup, start_url)
    log.info(f"  node_id={node_id}, section_links={len(section_links)}")

    all_docs: List[RulebookDocument] = []

    if node_id:
        time.sleep(REQUEST_DELAY)
        all_docs.extend(
            _crawl_entiresection(node_id, path_prefix=[], source_key=source_key, category=category)
        )

    if not all_docs and section_links:
        log.info("  Falling back to per-section crawl")
        for idx, section in enumerate(section_links, 1):
            log.info(f"  [{idx}/{len(section_links)}] {section['text'][:60]}")
            all_docs.extend(
                _crawl_section_page(section, source_key=source_key, category=category)
            )

    # Deduplicate by (path, hash)
    seen: set   = set()
    unique: List[RulebookDocument] = []
    for doc in all_docs:
        key = (doc.doc_path_str, doc.content_hash)
        if key not in seen:
            seen.add(key)
            unique.append(doc)

    log.info(f"=== Done: {len(unique)} unique documents ===")
    return unique


# ─── Convenience aliases ───────────────────────────────────────────────────────
def crawl_aml() -> List[RulebookDocument]:
    """Crawl Bahrain Anti Money Laundering Law 2001."""
    return crawl_rulebook("aml")


def crawl_corpgov() -> List[RulebookDocument]:
    """Crawl Corporate Governance Code of the Kingdom of Bahrain."""
    return crawl_rulebook("corpgov")