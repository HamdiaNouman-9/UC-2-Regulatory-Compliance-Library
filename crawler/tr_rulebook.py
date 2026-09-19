"""Thomson Reuters rulebook sidebar walker — one site's constants are arguments.

WHAT THIS IS
------------
Thomson Reuters hosts several GCC regulators' rulebooks on the same Drupal
*book* platform, with the same markup on every one of them:

    https://cbben.thomsonreuters.com/rulebook/<slug>      Central Bank of Bahrain
    https://rulebook.sama.gov.sa/en/<slug>                SAMA
    https://qfcra-en.thomsonreuters.com/rulebook/<slug>   Qatar Financial Centre

The structure lives in a collapsible sidebar (`nav[id^=book-block-menu-]`) and
the instrument IS the page — down to `Article 1 - Citation`. There are no
listing pages and no PDF-per-instrument, so the generic engine
(`generic_crawler/crawler.py`), which walks links and harvests file URLs, has
nothing to work with here.

WHY THIS IS A COPY AND NOT A REFACTOR
-------------------------------------
This is `cbb_test_crawlers/cbb_rulebook_crawler.py` with its six hardcoded
constants lifted into parameters. That file is NOT imported here and is NOT
changed by this one, DELIBERATELY: CBB has 1,464 documents and a working
checkpoint on disk, and the ask for Qatar was explicitly that nothing outside
Qatar's own files moves.

The cost is two copies of one recursion to keep in sync. If a third regulator
lands on this platform, that trade stops being worth it — at that point point
CBB at this module and delete its copy, verifying the change by replaying
`cbb_test_crawlers/rulebook_checkpoint.json` to the same document count.

THE CORE INSIGHT (unchanged from the CBB original)
--------------------------------------------------
The sidebar only expands the ACTIVE node's branch. Landing on a book root shows
its direct children but NOT its grandchildren, because those parents are not the
active page. So every node that LOOKS childless but is marked as a folder gets
its own page fetched to find out. That is lazy, and it is why a full walk is
thousands of sequential requests.

FOLDER VS LEAF IS A CSS CLASS, NOT A GUESS
------------------------------------------
`li.menu-item--collapsed` / `li.menu-item--expanded` carries an arrow icon: the
node is, or can be, a folder. A plain `li.menu-item` has a dot: it is a leaf.
Inferring instead from "did the page have body text" gets short leaves wrong in
one direction and empty folders wrong in the other.

WHICH BOOKS ONE SEED MEANS — `root_mode`
----------------------------------------
CBB publishes every volume in one sidebar, so its crawler treats EVERY nav on
the seed page as a book to walk (`root_mode="all"`).

Qatar turned out to be neither shape. MEASURED 2026-09-17, one request to
/rulebook/qfc-law-no-7-year-2005: there is exactly ONE nav, rooted at "Qatar
Financial Centre Legislation", carrying ~26 children — the four sections in
scope are four of them, alongside archives, forms, guidance and consultation
papers that are not. So a Qatar source is a BRANCH, not a book.

Hence three modes:

    node  walk the sidebar node that IS the seed, at whatever depth. Qatar.
    seed  walk the book whose ROOT is the seed.
    all   walk every nav on the page as its own book. CBB's shape.

`seed` is still the default, because silently walking the wrong thing is the
expensive failure and every mode here fails loudly instead: `seed` raises when
several books exist and none is the seed, `node` raises when the seed appears
nowhere in the tree.

The failure `node` exists to prevent is concrete: on `seed`, a per-branch URL
matched no book root, the page held exactly one book, and the walk fell back to
that single root — so one source began crawling the entire framework, and all
four would have stored the same documents under four different names with each
completeness gate scoped onto another's rows.

OUTPUT
------
`RulebookDoc` per node, folders included. Callers that store documents should
filter `is_folder` out — a folder is a position in the tree and `doc_path`
already records it, so storing it as a regulation gives a reviewer an entry with
no instrument behind it. (That bug was live in CBB for months because it
filtered on a field the dataclass does not have.)
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

log = logging.getLogger(__name__)

#: Seconds between requests. NOT a throughput knob — every folder node costs its
#: own page load, so a full walk is hours and this is what keeps it polite.
DEFAULT_REQUEST_DELAY = 1.2
DEFAULT_MAX_RETRIES = 3


@dataclass(frozen=True)
class Selectors:
    """The platform's markup, in one place so a site that drifts is one edit.

    Defaults are what `cbb_test_crawlers/cbb_rulebook_crawler.py` reads today,
    measured against cbben.thomsonreuters.com. A site that renders the same
    Drupal book module with a different theme changes these, not the recursion.
    """

    nav_id_prefix: str = "book-block-menu-"
    folder_classes: Sequence[str] = ("menu-item--collapsed", "menu-item--expanded")
    body_class: str = "field--name-body"
    #: The main-content wrapper. `body_class` is NOT unique on a page — see
    #: `_page_body_html` — and this is what separates the article from the
    #: footer boilerplate that wears the same class.
    content_region_class: str = "region-content"
    #: Amendment notes. On this platform they are a one-cell `<table class=
    #: "footnote">` at the end of the body — "Amended by Law No. (2) of 2009
    #: (as from 24th May 2009)." They already land in the text; this pulls them
    #: out as their own field so a reader can see WHICH instrument amended a
    #: provision without reading to the bottom of it.
    footnote_class: str = "footnote"
    #: Extensions that make a link a published file rather than navigation.
    file_exts: Sequence[str] = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".rtf")


DEFAULT_SELECTORS = Selectors()


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


def _new_session() -> requests.Session:
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


# ── Checkpointing ────────────────────────────────────────────────────────────
# MEASURED on CBB 2026-08-24 and 2026-08-25: the uncapped walk died mid-volume
# BOTH times, 8+ hours in, with no traceback — the process was killed, not the
# crawl. Per-book checkpointing means a kill like that costs the book in
# progress and nothing already walked.


def _load_checkpoint(path: Path) -> Dict[str, List[dict]]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("Could not read checkpoint %s, starting clean: %s", path, e)
        return {}


def _save_checkpoint(path: Path, data: Dict[str, List[dict]]) -> None:
    # Write-then-replace, so a kill mid-write cannot leave a half-written,
    # unparseable checkpoint — the one failure worse than having none at all.
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f)
    tmp.replace(path)


# ── HTTP ─────────────────────────────────────────────────────────────────────


def _fetch(session: requests.Session, url: str,
           max_retries: int = DEFAULT_MAX_RETRIES) -> Optional[BeautifulSoup]:
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            log.debug("  OK [%s] %s", resp.status_code, url)
            return BeautifulSoup(resp.content, "lxml")
        except requests.RequestException as e:
            log.warning("  Attempt %d/%d %s: %s", attempt, max_retries, url, e)
            if attempt < max_retries:
                time.sleep(2 ** attempt)
    log.error("  All retries exhausted: %s", url)
    return None


def _abs(base_url: str, href: str) -> str:
    if href.startswith(("http://", "https://")):
        return href
    return urljoin(base_url, href)


def _norm(url: str) -> str:
    return url.rstrip("/").lower()


# ── Title cleaning ───────────────────────────────────────────────────────────

_CODE_RE = re.compile(
    r"^("
    r"[A-Z][A-Z0-9]*-[A-Z0-9]+(?:\.[A-Z0-9]+)*"
    r"|(?:Article|Principle|Section|Chapter|Rule|Paragraph)\s+\d+(?:\.\d+)*"
    r")",
    re.IGNORECASE,
)


def _clean_title(raw: str) -> str:
    """Normalise whitespace, then strip a leading reference code ONLY when the
    code repeats right after it.

    "CM-1.1 CM-1.1 Scope" -> "CM-1.1 Scope". A title that merely starts with a
    code ("Article 1 - Citation") comes back untouched, which is what the manual
    library shows, so the code half is a no-op on most rows by design.

    THE WHITESPACE HALF IS NOT A NO-OP. `a.get_text(strip=True)` strips the ends
    and leaves the middle alone, and this site separates a clause number from its
    heading with a TAB in ~250 of QFCRA Rules' nav labels:

        "BANK 3.1A.10\\tTransfer of instruments between books"

    That tab reaches `title` and, because a node's title is the last element of
    its own `doc_path`, it reaches the tree as well — 573 rows carried one. A tab
    inside a folder name is never what the manual library shows, and `doc_path`
    is an IDENTITY field: fixing it after a promote reads as every affected row
    disappearing and an equal number appearing. Hence collapse here, before the
    first promote, not in a later cleanup.
    """
    title = " ".join(raw.split())
    m = _CODE_RE.match(title)
    if not m:
        return title
    code = m.group(1)
    after_first = title[m.end():]
    sep_m = re.match(r"^\s*[:\-—]?\s*", after_first)
    candidate = after_first[sep_m.end():]
    dup_pat = re.compile(r"^(" + re.escape(code) + r")([:\s]|$)", re.IGNORECASE)
    if dup_pat.match(candidate):
        return candidate.strip()
    return title


# ── Link extraction ──────────────────────────────────────────────────────────


def _extract_links(base_url: str, soup_fragment) -> Dict[str, Any]:
    pdf_links: List[Dict[str, str]] = []
    faq_link: Optional[str] = None
    other: List[Dict[str, str]] = []
    for a in soup_fragment.find_all("a", href=True):
        href = _abs(base_url, a["href"])
        text = a.get_text(strip=True)
        entry = {"name": text or href, "url": href}
        if href.lower().endswith(".pdf") or "pdf version" in text.lower():
            pdf_links.append({"name": text or "PDF", "url": href})
        elif "faq" in href.lower() or "faq" in text.lower():
            if not faq_link:
                faq_link = href
        other.append(entry)
    return {
        "pdf_link": pdf_links[0]["url"] if pdf_links else None,
        "pdf_links": pdf_links,
        "faq_link": faq_link,
        "other_links": other,
    }


def _extract_endnotes(fragment, sel: Selectors) -> List[str]:
    """The amendment notes on a provision, as their own list.

    MEASURED 2026-09-17 across all 26 nodes of QFC Law No. (7): twelve carry
    exactly one `table.footnote`, each a single cell, and all twelve sit INSIDE
    the captured body — so nothing was being lost, it was just inseparable from
    the provision's text. The other fourteen have no note at all, which is the
    site's own answer, not a miss.
    """
    notes: List[str] = []
    for table in fragment.find_all("table", class_=sel.footnote_class):
        for cell in table.find_all(["td", "th"]):
            text = cell.get_text(" ", strip=True)
            if text and text not in notes:
                notes.append(text)
    return notes


def _file_groups(fragment, base_url: str, sel: Selectors) -> List[Dict[str, Any]]:
    """Published files, grouped by the paragraph that introduces them.

    A rulebook FOLDER page is not empty on this platform — it is where the
    instrument's own files are published, one paragraph per instrument:

        "Click here to view the PDF version of the Law No.(2) of 2009.
         Click here to view the Word version ...  ... in Arabic."

    Three links, one law. Flattening them into a single list would lose which
    file belongs to which instrument, and that grouping is exactly what turns a
    folder page into the several rows the manual library shows. Paragraphs with
    no file link (pure navigation) are dropped.
    """
    groups: List[Dict[str, Any]] = []
    exts = tuple(sel.file_exts)
    for para in fragment.find_all(["p", "li"]):
        files = []
        for a in para.find_all("a", href=True):
            url = _abs(base_url, a["href"])
            if url.lower().split("?")[0].endswith(exts) and url not in files:
                files.append(url)
        if not files:
            continue
        text = " ".join(para.get_text(" ", strip=True).split())
        groups.append({"text": text, "files": files})
    return groups


# ── Sidebar nodes ────────────────────────────────────────────────────────────


@dataclass
class _Node:
    text: str
    url: str
    nav_id: str = ""
    children: List["_Node"] = field(default_factory=list)
    _is_folder_hint: bool = False
    _force_folder: bool = False


def _li_is_folder(li: Tag, sel: Selectors) -> bool:
    classes = li.get("class", []) or []
    return any(c in classes for c in sel.folder_classes)


def _parse_ul(ul: Tag, base_url: str, sel: Selectors) -> List[_Node]:
    """One sidebar <ul> into nodes.

    Already-expanded children are read immediately; a collapsed folder comes
    back with an empty child list and is expanded later by fetching its page.
    """
    nodes: List[_Node] = []
    for li in ul.find_all("li", recursive=False):
        a = li.find("a", href=True)
        if not a:
            continue
        is_folder = _li_is_folder(li, sel)
        child_ul = li.find("ul", recursive=False)
        children = _parse_ul(child_ul, base_url, sel) if (child_ul and is_folder) else []
        node = _Node(
            text=a.get_text(strip=True),
            url=_abs(base_url, a["href"]),
            children=children,
        )
        node._is_folder_hint = is_folder
        nodes.append(node)
    return nodes


def _navs_in(soup: BeautifulSoup, sel: Selectors, prefer_nav_id: str = "") -> List[Tag]:
    navs: List[Tag] = []
    if prefer_nav_id:
        n = soup.find("nav", id=prefer_nav_id)
        if n:
            navs.append(n)
    for n in soup.find_all("nav", id=re.compile("^" + re.escape(sel.nav_id_prefix))):
        if n not in navs:
            navs.append(n)
    return navs


def _expand_node(session, url: str, base_url: str, sel: Selectors,
                 nav_id: str, request_delay: float, max_retries: int) -> tuple:
    """Fetch `url` and read the children of ITS OWN <li> in the sidebar.

    Returns (children, soup) — the soup is handed back so a leaf that turned out
    to have no children does not pay for a second fetch of the same page.
    """
    time.sleep(request_delay)
    soup = _fetch(session, url, max_retries)
    if not soup:
        return [], None

    target = _norm(url)
    for nav in _navs_in(soup, sel, nav_id):
        for li in nav.find_all("li"):
            a = li.find("a", href=True)
            if not a:
                continue
            if _norm(_abs(base_url, a["href"])) == target:
                child_ul = li.find("ul", recursive=False)
                children = _parse_ul(child_ul, base_url, sel) if child_ul else []
                return children, soup
    return [], soup


# ── Which book(s) a seed means ───────────────────────────────────────────────


def _node_at(soup: BeautifulSoup, seed_url: str, base_url: str,
             sel: Selectors) -> _Node:
    """The sidebar node whose own href IS the seed, at whatever depth it sits.

    For `root_mode="node"`. Unlike the two book-root modes this searches EVERY
    <li> in the sidebar, not just the top one, so a seed can name a branch
    partway down the tree and the walk covers that branch and nothing else.

    `_force_folder` is deliberately NOT set: a book root is always a folder, but
    a node partway down may legitimately be a leaf, and forcing it would make
    the walk emit a folder that callers drop — the section would come back
    empty and read as a failed crawl.
    """
    target = _norm(seed_url)
    for nav in _navs_in(soup, sel):
        for li in nav.find_all("li"):
            a = li.find("a", href=True)
            if not a:
                continue
            if _norm(_abs(base_url, a["href"])) != target:
                continue
            child_ul = li.find("ul", recursive=False)
            node = _Node(
                text=a.get_text(strip=True),
                url=_abs(base_url, a["href"]),
                nav_id=nav.get("id", "") or "",
                children=_parse_ul(child_ul, base_url, sel) if child_ul else [],
            )
            node._is_folder_hint = _li_is_folder(li, sel) or bool(node.children)
            log.info("  Node: %r (%d children in seed, folder=%s)",
                     node.text, len(node.children), node._is_folder_hint)
            return node

    raise RuntimeError(
        f"root_mode='node': {seed_url} does not appear anywhere in this "
        f"sidebar. The seed must be a page the tree itself links, not a "
        f"redirect or an alias — otherwise the branch being crawled is a guess.")


def _collect_roots(session, seed_url: str, base_url: str, sel: Selectors,
                   root_mode: str, stop_before: Optional[str],
                   max_retries: int) -> List[_Node]:
    """The node(s) to walk from this seed.

    root_mode="node": the sidebar node that IS the seed, wherever it sits in the
    tree. Use this when the site publishes ONE book and the sources are branches
    inside it — MEASURED to be QFC's shape on 2026-09-17: a single
    `nav#book-block-menu-1` rooted at "Qatar Financial Centre Legislation" with
    ~26 children, of which the four in scope are four of them. Without this mode
    every Qatar source would walk that whole root.

    root_mode="seed": only the book whose root IS the seed.

    root_mode="all": every nav on the page is a book (CBB's shape, where one
    sidebar lists thirty volumes).
    """
    soup = _fetch(session, seed_url, max_retries)
    if not soup:
        raise RuntimeError(
            f"Could not fetch seed {seed_url}. That is a failed read, not an "
            f"empty book — nothing downstream can tell the difference, so it "
            f"stops here.")

    if root_mode == "node":
        return [_node_at(soup, seed_url, base_url, sel)]

    roots: List[_Node] = []
    for nav in _navs_in(soup, sel):
        top_ul = nav.find("ul")
        if not top_ul:
            continue
        top_li = top_ul.find("li", recursive=False)
        if not top_li:
            continue
        a = top_li.find("a", href=True)
        if not a:
            continue
        text = a.get_text(strip=True)

        if stop_before and stop_before.lower() in text.lower():
            log.info("  Stopping before: %r", text)
            break

        child_ul = top_li.find("ul", recursive=False)
        node = _Node(
            text=text,
            url=_abs(base_url, a["href"]),
            nav_id=nav.get("id", "") or "",
            children=_parse_ul(child_ul, base_url, sel) if child_ul else [],
        )
        node._is_folder_hint = True
        node._force_folder = True
        roots.append(node)

    if not roots:
        raise RuntimeError(
            f"No sidebar found at {seed_url} — looked for "
            f"nav[id^={sel.nav_id_prefix!r}]. Either the seed is wrong or this "
            f"site's markup differs from the platform default; fix Selectors "
            f"rather than loosening the search.")

    if root_mode == "all":
        for r in roots:
            log.info("  Book: %r (%d children in seed)", r.text, len(r.children))
        return roots

    if root_mode != "seed":
        raise ValueError(f"root_mode must be 'node', 'seed' or 'all', "
                         f"got {root_mode!r}")

    target = _norm(seed_url)
    exact = [r for r in roots if _norm(r.url) == target]
    if exact:
        log.info("  Book: %r (%d children in seed)", exact[0].text,
                 len(exact[0].children))
        return exact[:1]

    if len(roots) == 1:
        # One sidebar, one book. The seed is a page INSIDE it (or redirected),
        # which is fine and unambiguous.
        log.info("  Book: %r — seed did not match the root url, but this page "
                 "carries exactly one book", roots[0].text)
        return roots

    # Several books and none of them is the seed. Guessing here would store the
    # wrong documents under this source's name, and every gate downstream would
    # agree with the mistake.
    raise RuntimeError(
        f"Seed {seed_url} matches none of the {len(roots)} books in this "
        f"sidebar: {[r.text for r in roots]!r}. Point the seed at a book root, "
        f"or pass root_mode='all' if this source really is meant to walk all "
        f"of them.")


# ── Content ──────────────────────────────────────────────────────────────────


def _page_body_html(soup: BeautifulSoup, base_url: str, sel: Selectors) -> str:
    """The article's own body, out of the several divs that share its class.

    `field--name-body` IS NOT UNIQUE. MEASURED 2026-09-17 on
    /rulebook/article-1-definitions — four divs carry it:

        [0]    0 chars, a <script> that restyles modal links
        [1] 1755 chars, the actual Article
        [2]   12 chars, "Legal Notice"          (footer)
        [3]   31 chars, "QFC Regulatory Authority (c) 2020"  (footer)

    A plain `.find()` returns [0], which is how 26 of 26 documents came back
    with an identical 382-character script and no text at all. The workbook
    still passed `check` — shape was perfect, sense was absent. That is the
    failure ONBOARDING means by "only a person can see that a title is a cookie
    banner".

    So: keep only the divs inside the main-content region, which drops the two
    footers, and take the one with the most text, which drops the script. Sites
    that render a single body div (CBB) are unaffected — one candidate in,
    same one out.
    """
    cands = soup.find_all("div", class_=sel.body_class)
    if not cands:
        return ""

    if sel.content_region_class:
        inside = [d for d in cands
                  if d.find_parent("div", class_=sel.content_region_class)]
        if inside:
            cands = inside

    body = max(cands, key=lambda d: len(d.get_text(" ", strip=True)))
    if not body.get_text(strip=True):
        # Every candidate was empty. Returning the markup anyway would store a
        # script tag as an instrument and hash it as content.
        return ""

    for a in body.find_all("a", href=True):
        a["href"] = _abs(base_url, a["href"])
    return str(body)


# ── The walk ─────────────────────────────────────────────────────────────────


def _process(node: _Node, path: List[str], depth: int, visited: set,
             results: List[RulebookDoc], *, session, base_url: str,
             sel: Selectors, request_delay: float, max_retries: int,
             descend: bool = True) -> None:
    """One node, and by default everything under it.

    `descend=False` emits this node ALONE — its page is still read, so a folder
    keeps its published files, but no child is expanded or walked. That is how a
    section's own root row is written before its children are checkpointed
    separately; see `_units`.
    """
    if node.url in visited:
        return
    visited.add(node.url)

    title = _clean_title(node.text)
    cur_path = path + [title]
    indent = "  " * depth

    children = node.children
    page_soup = None
    is_folder_hint = getattr(node, "_is_folder_hint", bool(children))

    # Marked a folder but showing no children = collapsed in this render. Its
    # own page is the only place its children exist.
    if is_folder_hint and not children and descend:
        children, page_soup = _expand_node(
            session, node.url, base_url, sel, node.nav_id,
            request_delay, max_retries)
        for c in children:
            if not c.nav_id:
                c.nav_id = node.nav_id

    is_folder = (bool(children) or is_folder_hint
                 or getattr(node, "_force_folder", False))
    log.debug("%s%s %s", indent, "[F]" if is_folder else "[L]", title)

    html_content = ""
    content_text = ""
    link_meta = {"pdf_link": None, "pdf_links": [], "faq_link": None,
                 "other_links": []}
    endnotes: List[str] = []
    groups: List[Dict[str, Any]] = []

    # BOTH BRANCHES READ THE PAGE NOW. A folder used to get nothing but its own
    # title, on the reasoning that a folder is a position in the tree. That is
    # true of the tree and false of the page: MEASURED 2026-09-17, the folder
    # page for QFC Law No. (7) publishes the law itself — the consolidated text,
    # the 2009 and 2024 amending laws, each in PDF, Word and Arabic. Skipping it
    # dropped five instruments the manual library lists and left the section
    # looking like articles with no law above them.
    if page_soup is None:
        time.sleep(request_delay)
        page_soup = _fetch(session, node.url, max_retries)
    if page_soup:
        body_html = _page_body_html(page_soup, base_url, sel)
        if body_html:
            body_soup = BeautifulSoup(body_html, "lxml")
            link_meta = _extract_links(base_url, body_soup)
            endnotes = _extract_endnotes(body_soup, sel)
            groups = _file_groups(body_soup, base_url, sel)
            html_content = body_html
            content_text = body_soup.get_text(separator=" ", strip=True)

    if is_folder and not content_text:
        # Nothing published on it — fall back to the name, so the node still
        # hashes to something stable rather than to the empty string.
        content_text = title

    results.append(RulebookDoc(
        title=title,
        url=node.url,
        doc_path=cur_path,
        document_html=html_content,
        content_text=content_text,
        # Hash the TEXT, never the HTML: this platform is a CMS and its markup
        # churns on every deploy, which would report every page modified on
        # every run. Falls back to the title only for a page that yielded none.
        content_hash=_hash(content_text or title),
        is_folder=is_folder,
        depth=depth,
        extra_meta={**link_meta, "is_folder": is_folder,
                    "endnotes": endnotes, "file_groups": groups},
    ))

    if not descend:
        return

    for child in children:
        if not child.nav_id and node.nav_id:
            child.nav_id = node.nav_id
        _process(child, cur_path, depth + 1, visited, results,
                 session=session, base_url=base_url, sel=sel,
                 request_delay=request_delay, max_retries=max_retries)


def crawl_rulebook(
    seed_url: str,
    base_url: str,
    root_path: Sequence[str],
    *,
    request_delay: float = DEFAULT_REQUEST_DELAY,
    max_retries: int = DEFAULT_MAX_RETRIES,
    root_mode: str = "seed",
    stop_before: Optional[str] = None,
    max_books: Optional[int] = None,
    resume: bool = True,
    checkpoint_path: Optional[Path] = None,
    selectors: Selectors = DEFAULT_SELECTORS,
    session: Optional[requests.Session] = None,
) -> List[RulebookDoc]:
    """Walk a Thomson Reuters rulebook sidebar into a flat list of nodes.

    Args:
        seed_url: the book root (see `root_mode`).
        base_url: scheme+host, for resolving the sidebar's relative hrefs.
        root_path: what every `doc_path` starts with, e.g.
            ["Qatar Financial Centre Legislation", "QFC", "QFC Law"]. The seed
            node's OWN title is appended by the walk, so this is the part ABOVE
            it — the folders the manual library groups by and the site does not.
        root_mode: "node" | "seed" | "all". See the module docstring; picking
            the wrong one here is how a source ends up crawling its neighbours.
        max_books: walk only the first N books. A cap is a PROOF, not an
            inventory — it under-reports by design and must never be promoted as
            if it were the whole rulebook.
        resume: skip books already finished in `checkpoint_path`.

    RESUME, AND WHAT IT DOES NOT COVER. Only COMPLETED books are cached, and the
    sidebar tree itself never is. A run killed between books loses nothing
    already walked. It does NOT resume WITHIN a book: `visited` dedupes shared
    nodes inside one run only, and persisting it across runs would let an
    already-seen FOLDER return early and skip re-discovering children that were
    never actually reached — silently dropping documents rather than re-walking
    some pages. A book killed partway restarts from its own beginning. Redundant
    work, never silent loss.
    """
    session = session or _new_session()
    root_path = list(root_path)

    checkpoint: Dict[str, List[dict]] = {}
    if resume and checkpoint_path:
        checkpoint = _load_checkpoint(checkpoint_path)
        if checkpoint:
            log.info("Resuming: %d book(s) already in %s",
                     len(checkpoint), checkpoint_path.name)

    log.info("=== TR rulebook walk ===")
    log.info("Seed: %s  (root_mode=%s)", seed_url, root_mode)
    roots = _collect_roots(session, seed_url, base_url, selectors,
                           root_mode, stop_before, max_retries)

    if max_books is not None:
        roots = roots[:max_books]
        log.info("Limited to %d book(s) — this is a proof, not an inventory",
                 max_books)

    # THE CHECKPOINT UNIT IS ONE LEVEL BELOW EACH ROOT, NOT THE ROOT.
    #
    # With `root_mode="node"` there is exactly ONE root — the section — so
    # checkpointing per root means the file is written once, at the very end.
    # MEASURED 2026-09-17: QFC Regulations ran for a long stretch with nothing
    # on disk and nothing on screen, and a kill would have cost every minute of
    # it. That is the CBB failure (8+ hours lost, twice) reproduced by a
    # different route.
    #
    # Splitting at the root's children makes each of QFC Regulations' 21
    # regulations its own unit: saved when it finishes, skipped on a re-run, and
    # announced on its own line so progress is visible. The root itself is one
    # more unit, walked with descend=False so its page (and the files published
    # on it) is read without pulling the whole branch in with it.
    units: List[tuple] = []
    for root in roots:
        if root.children:
            units.append((f"{root.text} :: (root)", root, list(root_path), False))
            child_path = list(root_path) + [_clean_title(root.text)]
            for child in root.children:
                if not child.nav_id:
                    child.nav_id = root.nav_id
                units.append((f"{root.text} :: {child.text}", child,
                              child_path, True))
        else:
            units.append((root.text, root, list(root_path), True))

    all_docs: List[RulebookDoc] = []
    visited: set = set()
    log.info("%d unit(s) to walk", len(units))

    for i, (key, node, path, descend) in enumerate(units, 1):
        if key in checkpoint:
            cached = [RulebookDoc(**d) for d in checkpoint[key]]
            all_docs.extend(cached)
            log.info("[%d/%d] %s — resumed, %d doc(s)",
                     i, len(units), key, len(cached))
            continue

        log.info("[%d/%d] %s", i, len(units), key)
        unit_docs: List[RulebookDoc] = []
        # `depth` stays the depth in the TREE, not in the unit — splitting the
        # walk into units must not change what a row reports about itself.
        _process(node, path, len(path) - len(root_path), visited, unit_docs,
                 session=session, base_url=base_url, sel=selectors,
                 request_delay=request_delay, max_retries=max_retries,
                 descend=descend)
        all_docs.extend(unit_docs)
        log.info("      %d doc(s) here, %d so far", len(unit_docs), len(all_docs))

        if checkpoint_path:
            checkpoint[key] = [asdict(d) for d in unit_docs]
            _save_checkpoint(checkpoint_path, checkpoint)

    folders = sum(1 for d in all_docs if d.is_folder)
    leaves = len(all_docs) - folders
    log.info("=== Done: %d node(s) (%d folders, %d leaves) ===",
             len(all_docs), folders, leaves)
    return all_docs


__all__ = ["RulebookDoc", "Selectors", "crawl_rulebook",
           "DEFAULT_REQUEST_DELAY", "DEFAULT_SELECTORS"]
