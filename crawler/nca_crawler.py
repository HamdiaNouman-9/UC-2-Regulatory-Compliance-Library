"""National Cybersecurity Authority (nca.gov.sa) — the "Cyber Regulations and
Operations" tabs. Two page layouts, two classes, one set of helpers.

NO BROWSER for either. The site is Next.js with server rendering, and the card
listings are fed by a public JSON API that the page's own JavaScript calls.

-------------------------------------------------------------------------------
NCACrawler — a rich-text page (Laws and Regulations)
-------------------------------------------------------------------------------
ONE CMS rich-text block, `div.html-content`, with every instrument written as a
flat run of siblings and no container per instrument:

    <p><strong>Statute of The National Cybersecurity Authority:</strong></p>
    <p>The National Cybersecurity Authority (NCA) was established under ...</p>
    <p>&nbsp;</p>
    <div ...><strong><span class="custom-dga-button"><a href="https://cdn.nca.gov.sa/...pdf">
        View the Statute of The National Cybersecurity Authority</a></span></strong></div>

Neither a formfill `row_selector` nor the generic crawler can separate these —
the generic crawler titled both PDFs by their button ("View the Statute ...")
and could only attach the whole page as HTML. This splits the block on its bold
headings: title = heading, document_html = the text under it, document_url =
the button's file. Measured 2026-09-24: 2 headings, 2 PDFs.

-------------------------------------------------------------------------------
NCACardListCrawler — a card listing (Regulatory Documents)
-------------------------------------------------------------------------------
Cards (title, date, type tag, "Read More") paginated 8 at a time by JavaScript
— the page links carry no href. The page's script fetches them from

    POST https://backend.nca.gov.sa/api/public/cms/content/slugs?size=8&page=N
         {"slugs": ["controls-list", "guidelines-list", ...]}

and that response ALREADY HOLDS each detail page: `values.content.value` is the
HTML the "Read More" page renders, buttons and all. Checked 2026-09-24 against
the rendered detail pages of osmacc and scyberedu: same title, same links,
card/detail date = `publishDate`, the detail page's "Last Update" = `modifiedAt`.
So one request (size=50) replaces 3 listing pages + 17 detail pages.

`values.attachments` (a FILE_LIST on 3 items) is NOT rendered on the detail page
and is ignored: the library stores what a reader of the site can see.

Per card:
    title           values.title
    published_date  publishDate (the card's and the detail page's date)
    document_html   the content minus its file buttons and spacer paragraphs
    document_url    the one file linked, or extra_meta.attachment_links when
                    several (the house rule for multi-file instruments)
    doc_path        regulator > source_system > page > type tag > title

Two cards are not one document and are configured per slug in the YAML:
    html_only     keep the whole content as document_html, links included, and
                  store no files (Cybersecurity Toolkits: 84-row template table)
    split_tables  each table row becomes its own entry under the card, which
                  keeps its description (Implementation Guides: 6 guides)
Both decided by the business, 2026-09-24.
"""

from __future__ import annotations

import logging
import re
import time
import unicodedata
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from dynamic_crawler.formfill.runner import _ext_type
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# The CMS body. The page also carries an EMPTY `div.html-content mb-10` above it
# (the page summary slot), which yields no headings and is skipped naturally.
BLOCK_SELECTOR = "div.html-content"

CARD_API = "https://backend.nca.gov.sa/api/public/cms/content/slugs"

_FILE_EXT = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|zip)(\?|#|$)", re.I)

RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.0

MULTI_FILE_IDENTITY = ["doc_path", "extra_meta.attachment_links", "title"]


# --------------------------------------------------------------------------- #
#  shared helpers                                                              #
# --------------------------------------------------------------------------- #

def _norm(s: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", s or "").replace("\xa0", " ").split())


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en"})
    return s


def _request(session: requests.Session, method: str, url: str, timeout: int,
             **kw) -> requests.Response:
    """One request, retried only when it could not be answered. Small on
    purpose: saudiexchange.sa went from automated access to a permanent block
    in two hours, and this site is the same government estate."""
    wait = RETRY_BACKOFF
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = session.request(method, url, timeout=timeout, **kw)
            if r.status_code == 200:
                return r
            logger.warning("NCA %s %s -> HTTP %s (attempt %d/%d)", method, url,
                           r.status_code, attempt, RETRY_ATTEMPTS)
        except requests.RequestException as e:
            logger.warning("NCA %s %s -> %s (attempt %d/%d)", method, url,
                           type(e).__name__, attempt, RETRY_ATTEMPTS)
        if attempt < RETRY_ATTEMPTS:
            time.sleep(wait)
            wait *= 2
    raise RuntimeError(
        f"NCA {url} could not be fetched. Nothing may be classified from this — "
        f"an empty inventory is ruled `disappeared` downstream.")


def _heading_text(el: Tag) -> str:
    """The heading text when `el` is a heading, else "".

    A heading is an <h2>-<h5>, or a <p> whose ENTIRE visible text is bold
    (<strong>/<b>) — "Statute of The National Cybersecurity Authority:". A <p>
    with a bold phrase inside ordinary prose is body text, not a heading. The
    button wrapper is also bold, so anything holding a link is ruled out first.
    """
    if el.name in ("h2", "h3", "h4", "h5"):
        return _norm(el.get_text(" ", strip=True))
    if el.name != "p" or el.find("a"):
        return ""
    text = _norm(el.get_text(" ", strip=True))
    if not text:
        return ""
    bold = _norm(" ".join(b.get_text(" ", strip=True) for b in el.find_all(["strong", "b"])))
    return text if bold == text else ""


def _is_spacer(el) -> bool:
    if isinstance(el, NavigableString):
        return not _norm(str(el))
    return el.name in ("p", "div", "br") and not _norm(el.get_text()) and not el.find(["a", "img", "table"])


def _is_file_anchor(a: Tag, href: str) -> bool:
    return a.find_parent(class_="custom-dga-button") is not None or bool(_FILE_EXT.search(href))


def _file_links(el: Tag, base: str) -> List[str]:
    """Document links in `el`: the site's buttons (`.custom-dga-button a`) plus
    any other link to a file. Page links in prose are not documents."""
    out = []
    for a in el.find_all("a", href=True):
        href = urljoin(base, a["href"].strip())
        if _is_file_anchor(a, href) and href not in out:
            out.append(href)
    return out


def _is_link_block(el: Tag, base: str) -> bool:
    """A block that is nothing but file links — a "View" button, or a paragraph
    whose whole text is a link to a PDF (ECC's "Essential Cybersecurity Controls
    (ECC 2-2024)"). Navigation, not the instrument's text: the file itself is
    stored as the document."""
    anchors = [a for a in el.find_all("a", href=True)
               if _is_file_anchor(a, urljoin(base, a["href"].strip()))]
    if not anchors:
        return False
    rest = _norm(el.get_text(" ", strip=True))
    for a in anchors:
        rest = rest.replace(_norm(a.get_text(" ", strip=True)), "", 1)
    return not rest.strip()


def _description(html: str, base: str, drop_tables: bool = False):
    """(body_html, body_text, links) of one CMS content block: the prose with
    link-only blocks and spacers removed, and every file it links."""
    soup = BeautifulSoup(html or "", "html.parser")
    links = _file_links(soup, base)
    kept = []
    for child in soup.children:
        if isinstance(child, NavigableString):
            if _norm(str(child)):
                kept.append(str(child))
            continue
        if _is_spacer(child) or _is_link_block(child, base):
            continue
        if drop_tables and child.name == "table":
            continue
        kept.append(str(child))
    body_html = "\n".join(kept)
    body_text = _norm(BeautifulSoup(body_html, "html.parser").get_text(" ", strip=True))
    return body_html, body_text, links


def _files(links: List[str], fallback_url: str, extra: dict) -> tuple:
    """(document_url, file_type) for a set of links, filling `extra` for the
    multi-file case: one instrument, files in extra_meta.attachment_links,
    document_url left EMPTY (see RegulatoryDocument)."""
    if len(links) > 1:
        extra["attachment_links"] = " | ".join(links)
        extra["identity_fields"] = MULTI_FILE_IDENTITY
        types = {_ext_type(l) for l in links}
        return "", types.pop() if len(types) == 1 else "MIXED"
    if links:
        return links[0], _ext_type(links[0])
    return fallback_url, "HTML"


#: Pause between file-size probes. Politeness, not throughput: this host
#: dropped every connection from us for some hours on 2026-09-24 after a day of
#: heavy crawling.
FILE_PROBE_DELAY = 0.5


def _stored_files(d: RegulatoryDocument) -> List[str]:
    """The files a row STORES: its attachment_links, or its document_url when
    that is a file. An html_only card's `content_links` are not stored files and
    are not probed — 161 of them, and the text already carries them."""
    links = d.extra_meta.get("attachment_links")
    if links:
        return [l.strip() for l in links.split("|") if l.strip()]
    if d.document_url and (d.file_type or "HTML") != "HTML":
        return [d.document_url]
    return []


def _file_size(session: requests.Session, url: str, timeout: int) -> int:
    """The file's size in bytes from a ONE-BYTE range request.

    WHY SIZE, and not ETag or Last-Modified. All three are honest here —
    measured 2026-09-25 on ECC 2-2024 (ETag "688b6434-12c9f3", Last-Modified
    31 Jul 2025, its publication) and on osmacc-en.pdf, whose fixed
    nca.gov.sa url 308-redirects to cdn.nca.gov.sa/ar/... with the same
    headers. But the ETag is nginx's mtime-size pair, and mtime can differ
    between CDN nodes holding the same file; a token that flaps reads as
    `modified` every week. Size cannot differ for the same bytes, and a
    replaced PDF of the identical byte length is not a case worth a flapping
    signal.

    Raises when no size can be read: a guessed size would move the fingerprint
    and version a document nobody changed. A failed crawl classifies nothing.
    """
    wait = RETRY_BACKOFF
    last = ""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            r = session.get(url, headers={"Range": "bytes=0-0"}, timeout=timeout,
                            stream=True, allow_redirects=True)
            r.close()
            if r.status_code == 206:
                total = (r.headers.get("Content-Range") or "").rpartition("/")[2]
                if total.isdigit():
                    return int(total)
            if r.status_code == 200 and (r.headers.get("Content-Length") or "").isdigit():
                return int(r.headers["Content-Length"])
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = type(e).__name__
        if attempt < RETRY_ATTEMPTS:
            time.sleep(wait)
            wait *= 2
    raise RuntimeError(f"NCA file size unreadable for {url} ({last}) — refusing to "
                       f"fingerprint without it")


def _stamp(docs: List[RegulatoryDocument], timeout: int = 60) -> List[RegulatoryDocument]:
    """Single exit. The fingerprint is title + text + file links + FILE SIZES,
    so an edited description, a re-uploaded file (a new cdn uuid) AND a file
    replaced at the same url all read `modified`.

    The sizes close the gap the links alone left: several files live at FIXED
    urls (nca.gov.sa/osmacc-en.pdf, ncs_en.pdf, otcc_en.pdf ...), where a
    replacement changes no link. 35 one-byte requests per crawl (2026-09-25).

    `content_links` is an html_only card's links, which store no files but must
    still move the hash. Empty parts vanish under content_key's whitespace
    normalisation, so adding one never changes an existing hash."""
    session = _session()
    sizes: Dict[str, int] = {}
    for d in docs:
        for url in _stored_files(d):
            if url not in sizes:
                sizes[url] = _file_size(session, url, timeout)
                time.sleep(FILE_PROBE_DELAY)
    for d in docs:
        files = _stored_files(d)
        file_sizes = " | ".join(f"{u}={sizes[u]}" for u in files)
        if file_sizes:
            d.extra_meta["file_sizes"] = file_sizes
        basis = " ".join([d.title, d.extra_meta.get("content_text", ""),
                          d.document_url or "", d.extra_meta.get("attachment_links", ""),
                          d.extra_meta.get("content_links", ""), file_sizes])
        d.content_hash = content_key(basis)
        d.extra_meta["content_hash_basis"] = "title+text+links+file_sizes"
    return stamp_content_hashes(docs)


# --------------------------------------------------------------------------- #
#  Laws and Regulations — a rich-text page split on bold headings              #
# --------------------------------------------------------------------------- #

def split_blocks(html: str, page_url: str) -> List[dict]:
    """One dict per bold heading: title, body_html, body_text, links."""
    soup = BeautifulSoup(html, "html.parser")
    entries: List[dict] = []
    for block in soup.select(BLOCK_SELECTOR):
        cur = None
        for child in block.children:
            if isinstance(child, NavigableString):
                continue
            head = _heading_text(child)
            if head:
                cur = {"title": head.rstrip(" :："), "body": [], "links": []}
                entries.append(cur)
                continue
            if cur is None:
                continue          # intro text before the first heading
            cur["links"] += [l for l in _file_links(child, page_url) if l not in cur["links"]]
            # A button or bare file link is navigation, not the instrument's
            # text; a file linked from inside prose keeps its paragraph.
            if not _is_spacer(child) and not _is_link_block(child, page_url):
                cur["body"].append(child)
    for e in entries:
        e["body_html"] = "\n".join(str(x) for x in e.pop("body"))
        e["body_text"] = _norm(BeautifulSoup(e["body_html"], "html.parser").get_text(" ", strip=True))
    return entries


class NCACrawler:
    """One NCA rich-text page. `page_title` is the menu tab's name and becomes
    the folder under `source_system`, so it comes from the YAML rather than the
    page, where a retitle would silently reshape doc_path."""

    def __init__(self, regulator: str, source_system: str, page_url: str,
                 page_title: str, category: Optional[str] = None,
                 timeout: int = 60, delay: float = 1.0):
        self.regulator = regulator
        self.source_system = source_system
        self.page_url = page_url
        self.page_title = page_title
        self.category = category or page_title
        self.timeout = timeout
        self.delay = float(delay)
        self.last_result: dict = {}

    @property
    def source_names(self) -> List[str]:
        return [self.page_title]

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []
        html = _request(_session(), "GET", self.page_url, self.timeout).text
        entries = split_blocks(html, self.page_url)
        if not entries:
            raise RuntimeError(
                f"NCA: no bold headings found in {BLOCK_SELECTOR!r} at "
                f"{self.page_url}. That is a broken crawler, not an empty page — "
                f"every stored row would be proposed for withdrawal.")

        docs: List[RegulatoryDocument] = []
        for e in entries:
            title = e["title"]
            extra = {
                "crawl_source": self.page_title,
                "content_text": e["body_text"],
                "text_chars": len(e["body_text"]),
            }
            document_url, file_type = _files(e["links"], self.page_url, extra)
            if not e["links"]:
                warnings.append(f"{title!r} has no file link; stored against the page url")
            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.category,
                title=title,
                document_url=document_url,
                doc_path=[self.regulator, self.source_system, self.page_title, title],
                file_type=file_type,
                source_page_url=self.page_url,
                document_html=e["body_html"] or None,
                extra_meta=extra,
            ))
            if cap and len(docs) >= cap:
                break

        docs = _stamp(docs)
        titles = [d.title for d in docs]
        if len(titles) != len(set(titles)):
            warnings.append("two headings on the page share a title")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.page_title: len(docs)},
            "source": "live",
        }
        logger.info("NCACrawler %s: %d document(s)", self.page_title, len(docs))
        return docs


# --------------------------------------------------------------------------- #
#  Regulatory Documents — a card listing read from the CMS API                 #
# --------------------------------------------------------------------------- #

def _table_rows(html: str, base: str) -> List[dict]:
    """Rows of the content's table(s) that carry a file: title = the row's
    heading (or first cell), links = its files. The empty spacer row and a
    header row have no file and are skipped."""
    soup = BeautifulSoup(html or "", "html.parser")
    rows = []
    for tr in soup.select("table tr"):
        links = _file_links(tr, base)
        if not links:
            continue
        head = tr.find(["h1", "h2", "h3", "h4", "h5"]) or tr.find(["td", "th"])
        title = _norm(head.get_text(" ", strip=True)) if head else ""
        rows.append({"title": title, "links": links})
    return rows


class NCACardListCrawler:
    """One NCA card listing, read from the CMS API that feeds it.

    `schema_slugs` maps the API's `schemaSlug` to the tag the card shows
    ("controls-list" -> "Policies and controls"); its keys are also the request
    body, so the YAML is the one place the listing's scope is defined.
    """

    def __init__(self, regulator: str, source_system: str, page_url: str,
                 page_title: str, schema_slugs: Dict[str, str],
                 site_root: str = "https://nca.gov.sa/en",
                 html_only: Optional[List[str]] = None,
                 split_tables: Optional[List[str]] = None,
                 category: Optional[str] = None, page_size: int = 50,
                 timeout: int = 60, delay: float = 1.0):
        self.regulator = regulator
        self.source_system = source_system
        self.page_url = page_url
        self.page_title = page_title
        self.schema_slugs = dict(schema_slugs)
        self.site_root = site_root.rstrip("/")
        self.html_only = set(html_only or [])
        self.split_tables = set(split_tables or [])
        self.category = category or page_title
        self.page_size = int(page_size)
        self.timeout = timeout
        self.delay = float(delay)
        self.last_result: dict = {}

    @property
    def source_names(self) -> List[str]:
        return [self.page_title]

    def _items(self) -> List[dict]:
        s = _session()
        host = urlparse(self.site_root)
        s.headers.update({"Accept": "application/json",
                          "Origin": f"{host.scheme}://{host.netloc}",
                          "Referer": self.page_url})
        items, page, total = [], 0, None
        while True:
            r = _request(s, "POST", CARD_API, self.timeout,
                         params={"size": self.page_size, "page": page},
                         json={"slugs": list(self.schema_slugs)})
            data = r.json()
            total = data.get("totalElements")
            items += data.get("content") or []
            if data.get("last", True) or not data.get("content"):
                break
            page += 1
            time.sleep(self.delay)
        # The API states its own total. Fewer items than it claims is a broken
        # read, and would propose the missing ones for withdrawal.
        if total is not None and len(items) != total:
            raise RuntimeError(f"NCA {self.page_title}: API reported {total} items, "
                               f"read {len(items)}")
        return items

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []
        items = self._items()
        if not items:
            raise RuntimeError(
                f"NCA: the card API returned nothing for {self.page_title} "
                f"({list(self.schema_slugs)}). That is a broken crawler, not an "
                f"empty listing — every stored row would be proposed for withdrawal.")

        docs: List[RegulatoryDocument] = []
        for it in items:
            v = it.get("values") or {}
            slug = it.get("contentSlug") or ""
            schema = it.get("schemaSlug") or ""
            title = _norm((v.get("title") or {}).get("value"))
            lang = (v.get("title") or {}).get("language")
            if lang and lang != "en":
                warnings.append(f"{slug}: API answered in {lang!r}, not English")
            type_label = self.schema_slugs.get(schema)
            if type_label is None:
                type_label = schema
                warnings.append(f"{slug}: schemaSlug {schema!r} has no tag label in the YAML")
            by_slug = (it.get("detailsPage") or {}).get("bySlug") or f"/{slug}"
            detail_url = f"{self.site_root}{by_slug.rstrip('/')}/"
            html = (v.get("content") or {}).get("value") or ""
            published = (it.get("publishDate") or "")[:10] or None
            folder = [self.regulator, self.source_system, self.page_title, type_label]

            def meta(text: str, **more) -> dict:
                return {
                    "crawl_source": self.page_title,
                    "nca_type": type_label,
                    "nca_schema_slug": schema,
                    "nca_content_slug": slug,
                    "nca_content_id": it.get("id"),
                    "nca_modified_at": it.get("modifiedAt"),
                    "content_text": text,
                    "text_chars": len(text),
                    **more,
                }

            if slug in self.html_only:
                # The whole content, table and links included, IS the document.
                text = _norm(BeautifulSoup(html, "html.parser").get_text(" ", strip=True))
                links = _file_links(BeautifulSoup(html, "html.parser"), detail_url)
                extra = meta(text, linked_files=len(links))
                # The links still feed the fingerprint, so a replaced template
                # reads as `modified` even though no file is stored.
                extra["content_links"] = " | ".join(links)
                body_html, document_url, file_type = html, detail_url, "HTML"
            else:
                body_html, text, links = _description(
                    html, detail_url, drop_tables=slug in self.split_tables)
                if slug in self.split_tables:
                    table_links = {l for r in _table_rows(html, detail_url) for l in r["links"]}
                    links = [l for l in links if l not in table_links]
                extra = meta(text)
                document_url, file_type = _files(links, detail_url, extra)

            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.category,
                title=title,
                document_url=document_url,
                published_date=published,
                doc_path=folder + [title],
                file_type=file_type,
                source_page_url=detail_url,
                document_html=body_html or None,
                extra_meta=extra,
            ))

            if slug in self.split_tables:
                rows = _table_rows(html, detail_url)
                if not rows:
                    warnings.append(f"{slug}: configured split_tables but no table rows with files")
                for row in rows:
                    rextra = meta("", parent_title=title)
                    rurl, rtype = _files(row["links"], detail_url, rextra)
                    docs.append(RegulatoryDocument(
                        regulator=self.regulator,
                        source_system=self.source_system,
                        category=self.category,
                        title=row["title"],
                        document_url=rurl,
                        published_date=published,
                        doc_path=folder + [title, row["title"]],
                        file_type=rtype,
                        source_page_url=detail_url,
                        extra_meta=rextra,
                    ))
            if cap and len(docs) >= cap:
                break

        docs = _stamp(docs)

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.page_title: len(docs)},
            "source": "live",
        }
        logger.info("NCACardListCrawler %s: %d document(s) from %d card(s)",
                    self.page_title, len(docs), len(items))
        return docs



# --------------------------------------------------------------------------- #
#  Registration and Licensing — one page, its files attached to it             #
# --------------------------------------------------------------------------- #

#: The content child of <main>. NCA uses two layouts: the Enablement and Cyber
#: Threats pages wrap it in `div.py-10`, Registration and Licensing in a plain
#: `div.full-container` (followed by a second one holding "Last Update at", and
#: a `.py-6` feedback widget). The FIRST match in document order is the content
#: on all three, measured 2026-09-25 — the breadcrumb header (`div.bg-primary-5`)
#: comes before it and matches neither.
PAGE_CONTENT_SELECTOR = "main > div.py-10, main > div.full-container:not(.py-6)"


def _page_content(html: str) -> Optional[Tag]:
    """The page's content block, cleaned: no scripts, no images and no image
    wrappers (Next.js pictures render as empty gaps once stored; the table
    checkmarks are cdn.digital.site.sa icons), no "Last Update" line."""
    soup = BeautifulSoup(html, "html.parser")
    body = soup.select_one(PAGE_CONTENT_SELECTOR)
    if body is None:
        return None
    for t in body(["script", "style", "noscript", "svg"]):
        t.decompose()
    for img in body.find_all("img"):
        wrapper = img.parent
        img.decompose()
        # A div that held only the picture is now an empty box: drop it too.
        if wrapper is not None and wrapper.name == "div" and not _norm(wrapper.get_text()) \
                and not wrapper.find(True):
            wrapper.decompose()
    for p in body.find_all("p", string=re.compile(r"^\s*Last Update at")):
        p.decompose()
    return body


class NCAPageCrawler:
    """One NCA information page stored as ONE row: the cleaned page as
    document_html, and every file it links attached to that row — two or more
    in extra_meta.attachment_links with document_url empty (the house rule for
    multi-file instruments), one in document_url.

    Registration and Licensing links two PDFs under "To View" — the MSOC
    licensing framework (also a Regulatory Documents card) and the Investor's
    Guide. Business decision 2026-09-25: both are the page's attachments, not
    rows of their own. Links to other PAGES (the Haseen portal, the registered
    service-provider directory) are not followed: this reads one page.

    Why not the generic crawler: it stores each file as its own row, and its
    `merge_files_at_same_path` only fuses rows sharing a title — the page and
    its PDFs never do.
    """

    def __init__(self, regulator: str, source_system: str, page_url: str,
                 page_title: str, category: Optional[str] = None,
                 timeout: int = 60, delay: float = 1.0):
        self.regulator = regulator
        self.source_system = source_system
        self.page_url = page_url
        self.page_title = page_title
        self.category = category or page_title
        self.timeout = timeout
        self.delay = float(delay)
        self.last_result: dict = {}

    @property
    def source_names(self) -> List[str]:
        return [self.page_title]

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        html = _request(_session(), "GET", self.page_url, self.timeout).text
        body = _page_content(html)
        if body is None:
            raise RuntimeError(
                f"NCA: no content block ({PAGE_CONTENT_SELECTOR!r}) at "
                f"{self.page_url}. That is a broken crawler, not an empty page — "
                f"the stored row would be proposed for withdrawal.")
        text = _norm(body.get_text(" ", strip=True))
        links = _file_links(body, self.page_url)
        extra = {
            "crawl_source": self.page_title,
            "content_text": text,
            "text_chars": len(text),
        }
        document_url, file_type = _files(links, self.page_url, extra)
        docs = _stamp([RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self.category,
            title=self.page_title,
            document_url=document_url,
            doc_path=[self.regulator, self.source_system, self.page_title, self.page_title],
            file_type=file_type,
            source_page_url=self.page_url,
            document_html=body.decode_contents().strip() or None,
            extra_meta=extra,
        )])
        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": []},
            "by_source": {self.page_title: len(docs)},
            "source": "live",
        }
        logger.info("NCAPageCrawler %s: 1 page, %d file(s) attached",
                    self.page_title, len(links))
        return docs
