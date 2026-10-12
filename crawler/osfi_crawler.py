"""OSFICrawler — one controller for OSFI's tabs, and one code path.

Every tab is the same Drupal node type: `article.full` per item. Per-tab
differences are YAML settings:
  listing  "table" (a paginated listing table) | "subtree" (links under root_url's path)
  group    "book" (chapters under their book root) | "sections" (Sections under the
           return that links them) | "breadcrumb" (the page's own breadcrumb)
  files    "linked" (a guideline's own file(s): document_url / attachment_links per
           the lead's file rule) | "attachments" (return templates)
  follow   also store the pages a book root's contents lists and every Related
           documents tab link to, one hop, in `linked_folder`
See UC-2-Scratch/OSFI/.
"""

from __future__ import annotations

import copy
import logging
import re
import time
import unicodedata
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

HOST = "www.osfi-bsif.gc.ca"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

#: A book page embeds every chapter of its book (~1.7M chars) in these; hashing
#: them would re-version all of a book's pages when one chapter changes.
_STRIP = (".is-book.guidance.toc", ".view-book", "div.hidden")
#: xlsm included: FCT_Life_2026_EN.xlsm was dropped when this list lacked it.
_FILE = re.compile(r"\.(pdf|xls[xmb]?|xml|xsd|csv|zip|docx?|docm|pptx?|txt|json)$", re.I)
_SECTION = re.compile(r"^(.*?)\s+[–—-]\s+Section\s+[IVX]+\b")

#: Dated filenames (…-2025-10-31-en.xlsx) change when OSFI replaces a template, so
#: the row is identified by its page and the files go into the hash instead.
_PAGE_IDENTITY = ["source_page_url", "doc_path", "title"]

#: The lead's default identity for a multi-file row (models.py).
_FILES_IDENTITY = ["doc_path", "extra_meta.attachment_links", "title"]

#: The guideline code or return number, under whichever label the page uses.
_REF_LABELS = ("No", "Number", "Return number", "Return numbers")

#: Where a followed link may lead. Data and Forms and Supervision pages are rows
#: of the other two sources, so they are never followed from here.
_FOLLOW_PATHS = ("/en/guidance/guidance-library/", "/en/news/", "/en/consultations/")
_RELATED = ".field--name-field-related-documents"
_BOOK_NAV = ".view-book.view-display-id-pagination a[href]"

_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


#: A space goes at these boundaries only. get_text(" ") spaced EVERY tag, so
#: "<strong>2%</strong>, this" was stored as "2% , this" and phrase search missed.
_BLOCK = ("p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "tr", "dt", "dd",
          "div", "section", "summary", "details", "caption", "table", "ul", "ol", "dl",
          "br", "hr", "blockquote", "pre", "figure", "figcaption", "header", "footer")


def _block_text(el) -> str:
    """Visible text with breaks at block boundaries only; `el` itself is not modified."""
    el = copy.copy(el)
    for tag in el.find_all(_BLOCK):
        tag.insert_before("\n")
        tag.insert_after("\n")
    return el.get_text("")


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[\ufeff\u200b\u200c\u200d\u2060]", "", s)       # NFKC keeps zero-widths
    return re.sub(r"\s+", " ", s).strip()


def _key(u: str) -> str:
    p = urlparse(u)
    return p.netloc + p.path.rstrip("/")


class OSFICrawler:
    """One OSFI tab: listing pages, then each item's `article.full`."""

    def __init__(self, regulator: str, source_system: str, folder: str,
                 group: str, files: str, min_documents: int, min_text_chars: int,
                 listing: str = "table", listing_url: str = "", root_url: str = "",
                 max_pages: int = 20, category: Optional[str] = None,
                 follow: bool = False, linked_folder: str = "Linked documents",
                 published_label: str = "", check_files: bool = False,
                 delay: float = 1.5, **_):
        if listing not in ("table", "subtree"):
            raise ValueError("OSFI: unknown listing %r" % listing)
        if group not in ("book", "sections", "breadcrumb"):
            raise ValueError("OSFI: unknown group %r" % group)
        if files not in ("linked", "attachments"):
            raise ValueError("OSFI: unknown files %r" % files)
        self.regulator = regulator
        self.source_system = source_system
        self.folder = folder
        self.listing = listing
        self.listing_url = listing_url          # table: contains {page}, 0-based
        self.root_url = root_url                # subtree: the tab's landing page
        self.group = group
        self.files = files
        self.min_documents = int(min_documents)
        self.min_text_chars = int(min_text_chars)
        self.max_pages = int(max_pages)
        self.category = category or source_system
        self.follow = bool(follow)
        self.linked_folder = linked_folder
        # The field holding the PUBLICATION date ("Date" on guidelines). Never an
        # update date: published_date is part of a document's identity.
        self.published_label = published_label
        # HEAD every file once a run, so a dead one is never named the document.
        self.check_files = bool(check_files)
        self.delay = float(delay)
        self.dead_links: List[tuple] = []
        self._alive_cache: Dict[str, bool] = {}
        self._s = requests.Session()
        self._s.headers["User-Agent"] = USER_AGENT

    def _get(self, url: str, missing_ok: bool = False) -> Optional[requests.Response]:
        last = None
        for attempt in range(3):
            try:
                r = self._s.get(url, timeout=60)
                if r.status_code == 200:
                    time.sleep(self.delay)
                    return r
                if r.status_code == 404 and missing_ok:
                    time.sleep(self.delay)
                    return None
                raise RuntimeError("%s returned %d" % (url, r.status_code))
            except requests.RequestException as e:
                last = e
                time.sleep(2.0 * (attempt + 1))
        raise RuntimeError("could not fetch %s: %s" % (url, last))

    def _listing(self) -> List[Dict[str, str]]:
        items, seen = [], set()
        for page in range(self.max_pages):
            r = self._get(self.listing_url.format(page=page))
            rows = BeautifulSoup(r.text, "html.parser").select("main table tbody tr")
            if not rows:
                return items
            for tr in rows:
                a = tr.find("a", href=True)
                url = urljoin(r.url, a["href"]) if a else ""
                if url and url not in seen:
                    seen.add(url)
                    items.append({"url": url, "listing_title": _norm(a.get_text())})
        raise RuntimeError("OSFI %s: listing still returning rows after %d pages"
                           % (self.source_system, self.max_pages))

    def _walk(self) -> List[dict]:
        """Every page reachable from root_url by links inside article.full that stay
        under its path. Other OSFI pages are linked too (TLAC, MCT) and must not be
        followed: they are rows of other sources."""
        prefix = urlparse(self.root_url).path.rstrip("/")
        under = lambda u: urlparse(u).netloc == HOST and (
            urlparse(u).path.rstrip("/") == prefix or urlparse(u).path.startswith(prefix + "/"))
        queue, asked, pages = [self.root_url], set(), {}
        while queue:
            u = queue.pop(0)
            if u in asked:
                continue
            asked.add(u)
            if len(asked) > self.max_pages:
                raise RuntimeError("OSFI %s: over %d pages under %s"
                                   % (self.source_system, self.max_pages, prefix))
            p = self._page({"url": u, "listing_title": ""})
            if not under(p["url"]) or p["url"] in pages:   # redirected out, or a duplicate
                continue
            pages[p["url"]] = p
            queue.extend(sorted(l for l in p["links"] if under(l) and l not in asked))
        return list(pages.values())

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        if self.listing == "table":
            items = self._listing()
            if len(items) < self.min_documents:
                raise RuntimeError("OSFI %s: %d listed, below floor %d"
                                   % (self.source_system, len(items), self.min_documents))
            pages = [self._page(it) for it in items]
        else:
            pages = self._walk()
            if len(pages) < self.min_documents:
                raise RuntimeError("OSFI %s: %d pages found, below floor %d"
                                   % (self.source_system, len(pages), self.min_documents))

        if self.follow:
            pages = pages + self._follow(pages)

        # Grouping needs every page first. The folder is always the parent page's
        # own title: OSFI's titles disagree ("Property and casualty (P&C) insurance
        # return (2026)" vs its Sections' "Property and casualty insurance return").
        by_url = {p["url"]: p for p in pages}
        if self.group == "book":
            for p in pages:
                if p["root_url"]:
                    if p["root_url"] not in by_url:
                        if p.get("found_on"):
                            continue        # a linked chapter of an unfollowed book: flat
                        raise RuntimeError("OSFI: book root %s not listed" % p["root_url"])
                    p["group"] = by_url[p["root_url"]]["title"]
        elif self.group == "breadcrumb":
            # Ancestors are the crumbs between the tab folder and the page itself. A
            # page with children sits inside its own folder, as a book root does.
            for p in pages:
                bc = p["crumbs"]
                if not bc or bc[-1] != p["title"] or self.folder not in bc:
                    raise RuntimeError("OSFI: %s breadcrumb %r does not end at its h1 "
                                       "under %r" % (p["url"], bc, self.folder))
                p["anc"] = bc[bc.index(self.folder) + 1:-1]
            for p in pages:
                own = p["anc"] + [p["title"]]
                is_parent = p["url"] != self.root_url and any(
                    q["anc"][:len(own)] == own for q in pages if q is not p)
                p["group"] = own if is_parent else p["anc"]
        else:
            # A return's Sections share one title prefix; its parent is the one page
            # that links EVERY Section of the set (others cross-link just one).
            sets = {}
            for p in pages:
                m = _SECTION.match(p["title"])
                if m:
                    sets.setdefault(m.group(1), []).append(p)
            for prefix, secs in sets.items():
                need = {x["url"] for x in secs}
                parents = [p for p in pages
                           if not _SECTION.match(p["title"]) and need <= p["links"]]
                if len(parents) != 1:
                    raise RuntimeError("OSFI: %d Sections of %r have %d parent pages: %s" % (
                        len(secs), prefix, len(parents), sorted(x["title"] for x in parents)))
                for x in secs + parents:
                    x["group"] = parents[0]["title"]

        docs = [self._doc(p) for p in pages]
        if len({d.title for d in docs}) != len(docs):
            dup = sorted({d.title for d in docs if [x.title for x in docs].count(d.title) > 1})
            raise RuntimeError("OSFI %s: duplicate titles %s" % (self.source_system, dup))
        docs = stamp_content_hashes(docs)                          # single exit
        logger.info("OSFI %s: %d documents (%d linked), %d dead links",
                    self.source_system, len(docs), sum(1 for p in pages if p.get("found_on")),
                    len(self.dead_links))
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs

    def _follow(self, pages: List[dict]) -> List[dict]:
        """One hop: the pages a book root's contents lists and any page's Related
        documents tab link to, that are not rows already. A followed book root
        brings its own chapters. Each keeps `found_on`: the first page, in listing
        order, that linked it (one url — cli/sweep.py reads it as one)."""
        known = {_key(p["url"]) for p in pages} | {_key(p["node_url"]) for p in pages}
        followable = lambda u: (urlparse(u).netloc == HOST and not _FILE.search(urlparse(u).path)
                                and urlparse(u).path.startswith(_FOLLOW_PATHS))
        found_on, queue = {}, []
        for p in pages:
            links = list(p["related_urls"])
            if p["root_url"] and _key(p["root_url"]) == _key(p["url"]):
                links += p["body_links"]                       # a book root's contents lists
            for u in links:
                if followable(u) and _key(u) not in known and _key(u) not in found_on:
                    found_on[_key(u)] = p["url"]
                    queue.append(u)
        out = []
        while queue:
            u = queue.pop(0)
            q = self._page({"url": u, "listing_title": ""}, missing_ok=True)
            if q is None:
                # OSFI links a page it no longer serves. Recorded, never retried here.
                self.dead_links.append((u, found_on[_key(u)]))
                logger.warning("OSFI: %s (linked from %s) is 404", u, found_on[_key(u)])
                continue
            if _key(q["url"]) in known:
                continue                                       # redirected onto a row
            known.add(_key(q["url"]))
            q["found_on"] = found_on[_key(u)]
            out.append(q)
            if q["book_pages"] and _key(q["book_pages"][0]) == _key(q["url"]):
                for c in q["book_pages"][1:]:
                    if _key(c) not in known and _key(c) not in found_on:
                        found_on[_key(c)] = q["url"]
                        queue.append(c)
        return out

    def _alive(self, url: str) -> bool:
        """Does OSFI still serve this file? One HEAD a file a run, cached."""
        if url not in self._alive_cache:
            try:
                r = self._s.head(url, allow_redirects=True, timeout=60)
                if r.status_code in (403, 405):                # HEAD refused: ask with GET
                    r = self._s.get(url, stream=True, timeout=60)
                    r.close()
                ok = r.status_code == 200
            except requests.RequestException:
                ok = True        # unanswered is not dead; a network blip must not demote a file
            self._alive_cache[url] = ok
            time.sleep(self.delay)
        return self._alive_cache[url]

    def _page(self, item: Dict[str, str], missing_ok: bool = False) -> Optional[dict]:
        r = self._get(item["url"], missing_ok=missing_ok)
        if r is None:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        h1 = soup.select_one("main h1")
        title = _norm(h1.get_text()) if h1 else ""
        art = soup.select_one("main .region-content article.full")
        if not title or art is None:
            raise RuntimeError("OSFI %s: title %r, article %s" % (r.url, title, art is not None))

        crumbs = [_norm(li.get_text()) for li in soup.select("#wb-bc li")]
        # The book's own nav: its first real link (not "previous") is the root, and
        # on a root page the list is every page of the book in order.
        book_pages = []
        for a in soup.select(_BOOK_NAV):
            u = urljoin(r.url, a["href"]).split("#")[0]
            if u not in book_pages:
                book_pages.append(u)
        root_url = book_pages[0] if book_pages else ""

        for sel in _STRIP:
            for el in art.select(sel):
                el.decompose()
        # The "Related documents" tab, as {title: url} — read before the body's
        # links so the two can be told apart.
        related, related_urls = {}, []
        rel = art.select_one(_RELATED)
        rel_anchors = set(id(a) for a in rel.find_all("a", href=True)) if rel else set()
        for a in (rel.find_all("a", href=True) if rel else []):
            u = urljoin(r.url, a["href"]).split("#")[0]
            label, n = _norm(a.get_text()) or u, 2
            while label in related and related[label] != u:
                label = "%s (%d)" % (_norm(a.get_text()) or u, n); n += 1
            related[label] = u
            if u not in related_urls:
                related_urls.append(u)
        own, links, body_links, content_links = [], set(), [], []
        for a in art.find_all("a", href=True):
            u = urljoin(r.url, a["href"]).split("#")[0]
            on_host = urlparse(u).netloc == HOST
            if on_host and _FILE.search(urlparse(u).path):
                if u not in own:
                    own.append(u)
                continue
            # The Related documents tab is its own key; these are the article's OTHER links.
            if id(a) not in rel_anchors and u.startswith("http") and u != r.url and u not in content_links:
                content_links.append(u)
            if not on_host:
                continue
            links.add(u)
            if id(a) not in rel_anchors and u not in body_links:
                body_links.append(u)
        text = _ILLEGAL_XLSX.sub(" ", _norm(_block_text(art)))
        if len(text) < self.min_text_chars:
            raise RuntimeError("OSFI %s: %d chars, below floor %d"
                               % (r.url, len(text), self.min_text_chars))
        fields = {}
        # A label's values are its sibling items; "Number" nests inside a paragraph.
        for lab in art.select(".field--label"):
            vals = [_norm(v.get_text(" ")) for v in
                    lab.parent.find_all(class_="field--item", recursive=False)]
            if vals:
                fields[_norm(lab.get_text())] = " | ".join(vals)
        return {"url": r.url, "node_url": item["url"], "listing_title": item["listing_title"],
                "title": title, "group": "", "root_url": root_url, "links": links,
                "crumbs": crumbs, "book_pages": book_pages, "related": related,
                "related_urls": related_urls, "body_links": body_links,
                "content_links": content_links,
                "text": text, "files": own,
                "fields": fields, "html": str(art)}

    def _doc(self, p: dict) -> RegulatoryDocument:
        path = [self.regulator, self.source_system,
                self.linked_folder if p.get("found_on") else self.folder]
        g = p["group"]
        path.extend(g if isinstance(g, list) else ([g] if g else []))
        path.append(p["title"])
        meta = {
            "node_url": p["node_url"],
            "fields": p["fields"],
            "content_text": p["text"],
            "text_chars": len(p["text"]),
        }
        if p.get("found_on"):
            meta["found_on"] = p["found_on"]                   # the lead's provenance key
        if p["related"]:
            meta["related_documents"] = p["related"]           # the tab, {title: url}
        # Other publications the page links to: not this document's files.
        meta["content_links"] = " | ".join(p["content_links"])
        published = None
        if self.published_label and p["fields"].get(self.published_label):
            try:
                published = datetime.strptime(p["fields"][self.published_label],
                                              "%B %d, %Y").date().isoformat()
            except ValueError:
                published = None                               # kept, raw, in fields
        basis = p["text"]
        document_url = p["url"]
        file_type = "HTML"
        if self.files == "attachments":
            meta["identity_fields"] = _PAGE_IDENTITY
            if len(p["files"]) == 1:
                document_url = p["files"][0]               # single-file contract
            elif p["files"]:
                meta["attachment_links"] = " | ".join(p["files"])
                document_url = ""                          # multi-file contract
            if p["files"]:
                basis += " | files: " + " | ".join(sorted(p["files"]))
            meta["content_hash_basis"] = "article-text-plus-file-urls"
        else:
            # The lead's file rule (utils/file_links.py): one file -> document_url,
            # several -> attachment_links with document_url empty, none -> the page.
            # A file OSFI no longer serves is never named the document.
            files = p["files"]
            if self.check_files:
                dead = [f for f in files if not self._alive(f)]
                files = [f for f in files if f not in dead]
                if dead:
                    meta["dead_files"] = " | ".join(dead)
            exts = {urlparse(f).path.rsplit(".", 1)[-1].upper() for f in files}
            if len(files) == 1:
                document_url, file_type = files[0], exts.pop()
            elif files:
                meta["attachment_links"] = " | ".join(files)
                meta["identity_fields"] = _FILES_IDENTITY
                document_url = ""
                file_type = exts.pop() if len(exts) == 1 else "HTML"
            meta["content_hash_basis"] = "article-text-minus-book-toc"
        doc = RegulatoryDocument(
            regulator=self.regulator, source_system=self.source_system,
            category=self.category, title=p["title"], document_url=document_url,
            doc_path=path, file_type=file_type, source_page_url=p["url"],
            published_date=published, reference_no=next((p["fields"][k] for k in _REF_LABELS if p["fields"].get(k)), None),
            document_html=p["html"], extra_meta=meta,
        )
        doc.content_hash = content_key(basis)
        return doc
