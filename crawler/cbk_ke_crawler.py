# -*- coding: utf-8 -*-
"""Central Bank of Kenya (CBK): Legislation and Guidelines, and Circulars.

Two listings, one class, chosen by `listing`:

  * "legislation" -- /policy-procedures/legislation-and-guidelines/. The page
    itself is one HTML row. Its second text module is a run of <strong>
    headings, each followed by its file links; a heading's files are the links
    after it up to the next heading (there is no wrapper per heading). Each
    heading is a folder. A link to an HTML page (Draft CBK Prudential
    Guidelines news post, Green Finance page) is that page's HTML row, read
    from its text modules only -- the Green page's sidebar carries a daily
    exchange-rate widget. That page's files are rows beside it; on the Draft
    CBK post each "Draft ..." document gets its own subfolder. Archives
    (.zip, .rar) are one row each. The first module (intro and "Regulations and
    Guidelines Pursuant to the Banking Act") has no files and is no folder.
  * "circulars" -- .../circulars/. One wpDataTables table (Year, Title); the
    pager is client-side, so the plain GET holds every row. Folder per Year.

doc_path: regulator > Policy Procedures > Legislation and Guidelines >
(heading | Circulars > year) > title.

Hashes: a page row hashes its visible text. A file row hashes
url|title|Last-Modified|Content-Length from a HEAD on the file (the CBK file
server sends both); a failed HEAD falls back to url|title and says so in
extra_meta["hash_basis"]. Archives and .xlsm are stored as one row each with
no text. `skip_extensions` drops file types, each logged as a run warning;
none is skipped today.

All requests go through utils.pacer: a block or a repeated throttle stops the
run instead of reporting a partial listing.

Helpers copied from crawler/frrs_crawler.py, not imported.
"""
from __future__ import annotations

import logging
import re
import time
from typing import List, Optional, Tuple
from urllib.parse import unquote, urljoin, urlsplit

import requests
from bs4 import BeautifulSoup, Comment, Tag

from crawler.fingerprint import stamp_content_hashes
from dynamic_crawler.formfill.runner import _ext_type, _is_doc
from generic_crawler.crawler import absolutize_html, content_key
from models.models import RegulatoryDocument
from utils.pacer import SiteBlocked, SiteThrottled, check_response, wait_turn

logger = logging.getLogger(__name__)

PARSER = "lxml"
_WS = re.compile(r"\s+")

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")

REGULATOR = "Central Bank of Kenya (CBK)"
BASE = "https://www.centralbank.go.ke"
LEGISLATION_URL = f"{BASE}/policy-procedures/legislation-and-guidelines/"
CIRCULARS_URL = f"{BASE}/policy-procedures/legislation-and-guidelines/circulars/"
TRAIL = ["Policy Procedures", "Legislation and Guidelines"]

#: Legislation page: module 0 is the intro, module 1 the headings and files.
_LEG_MODULES = ".et_pb_specialty_column .et_pb_text"
#: A linked post/page: its text modules, never the sidebar widget area.
_LINKED_MODULES = ".entry-content .et_pb_text"
_CIRC_TABLE = "table.wpDataTable"
_FILE_CTYPES = ("pdf", "officedocument", "msword", "ms-excel", "octet-stream",
                "zip", "rar")
#: An archive is one row, stored as-is (user, 2026-10-30): the files inside it
#: have no url of their own. Two on the Draft CBK post (27 and 3 PDFs), a .zip
#: and a .rar on the Green Finance page.
_ARCHIVES = {".zip", ".rar"}
#: Not in the shared document regex but stored here all the same, one row each,
#: typed by extension (user, 2026-10-30): the archives, and .xlsm, a macro
#: workbook ("FX Code Self-Assessment Template"). No text is extracted from
#: either; the row's hash is the HEAD stamp like every other file.
_KEPT_AS_IS = _ARCHIVES | {".xlsm"}
#: A linked page's "Read the Draft Risk Management Guidelines here" names the
#: file; the bare "here" does not.
_HERE = re.compile(r"^(?:read|download)\s+(?:the\s+)?|\s*here\W*$", re.I)
#: The Green Finance link text is the page's <title>: "... Framework | CBK".
_SITE_SUFFIX = re.compile(r"\s*/\s*CBK$")
#: Sucuri's code for "Access to this page was restricted by the administrator":
#: a per-url rule, not a block on us. Measured 2026-10-30 on
#: uploads/2020/06/Procedures-for-authorization-of-Payment-Service-Providers.pdf
#: (403 to curl and requests alike; its sibling in 2020/06 answers 200).
_RESTRICTED = "CUST02"
#: More restricted files than this is not the site's housekeeping.
MAX_RESTRICTED = 5
_SUCURI_REASON = re.compile(r"Block reason:\s*</?[^>]*>?\s*([^<]{3,200})", re.I)
_REF = re.compile(r"^(.*?Circular\s+(?:No\.?|Number)?\s*\d+\s+of\s+\d{4})", re.I)

#: A listing that yields fewer rows than this is a broken page, not a quiet
#: regulator. Measured 2026-10-30: 81 files, 150 circulars.
MIN_ROWS = {"legislation": 40, "circulars": 100}


def _text(el) -> str:
    """Display text, pipe-free -- " | " is doc_path's separator in the
    workbook, so a pipe in a title reads back as an extra folder (DFPI)."""
    if el is None:
        return ""
    return _WS.sub(" ", el.get_text(" ", strip=True)).replace("|", "/").strip()


def _visible_text(html: str) -> str:
    soup = BeautifulSoup(html, PARSER)
    for bad in soup(["script", "style", "noscript"]):
        bad.decompose()
    return _WS.sub(" ", soup.get_text(" ", strip=True)).strip()


def _name_from_url(url: str) -> str:
    """CREDIT_REFERENCE_BUREAU_REGULATIONS_2013.pdf -> a title, for the one
    link on the page with no text."""
    stem = unquote(urlsplit(url).path.rsplit("/", 1)[-1]).rsplit(".", 1)[0]
    return _WS.sub(" ", re.sub(r"[_\-]+", " ", stem)).strip()


def _ext(url: str) -> str:
    path = urlsplit(url).path.lower()
    return "." + path.rsplit(".", 1)[-1] if "." in path.rsplit("/", 1)[-1] else ""


def _is_file(url: str) -> bool:
    return _is_doc(url) or _ext(url) in _KEPT_AS_IS


def _is_heading(el: Tag) -> bool:
    return (el.name in ("strong", "b") and el.find_parent("a") is None
            and el.find("a") is None and bool(_text(el)))


class CBKKenyaCrawler:
    """One CBK listing; see the module docstring."""

    def __init__(
        self,
        listing: str,
        regulator: str = REGULATOR,
        source_system: str = "Legislation and Guidelines",
        seed_url: Optional[str] = None,
        #: Extensions never stored, each logged as a run warning.
        skip_extensions: Optional[List[str]] = None,
        #: One HEAD per file for Last-Modified / Content-Length.
        fetch_stamps: bool = True,
        timeout: float = 60.0,
    ):
        if listing not in MIN_ROWS:
            raise ValueError("listing must be one of %s" % sorted(MIN_ROWS))
        self.listing = listing
        self.regulator = regulator
        self.source_system = source_system
        self.seed_url = seed_url or (LEGISLATION_URL if listing == "legislation"
                                     else CIRCULARS_URL)
        self.skip_extensions = {e.lower() for e in (skip_extensions or [])}
        self.fetch_stamps = fetch_stamps
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = _UA
        self.last_result: dict = {}
        self._warnings: List[str] = []
        self._sent = 0
        self._restricted: List[str] = []

    # ------------------------------------------------------------------ #
    def _request(self, method: str, url: str) -> requests.Response:
        """One paced request; one retry if the site asks us to slow down.
        A refusal names the request: the pacer's message names only the host."""
        for attempt in (1, 2):
            wait_turn(url)
            self._sent += 1
            r = self.session.request(method, url, timeout=self.timeout,
                                     allow_redirects=True)
            if method == "HEAD" and r.status_code == 403 \
                    and r.headers.get("X-Sucuri-Block") == _RESTRICTED:
                return r            # one file the site refuses everyone
            body = r.text[:3000] if method == "GET" else ""
            try:
                check_response(url, r.status_code, r.headers, "", body)
                return r
            except SiteThrottled:
                if attempt == 2:
                    raise
            except SiteBlocked as e:
                # www.centralbank.go.ke sits behind Sucuri, whose block page
                # states its reason.
                why = _SUCURI_REASON.search(body)
                raise SiteBlocked(e.host, e.kind, "%s on request #%d, %s %s%s" % (
                    e.reason, self._sent, method, url,
                    "; Sucuri: " + _WS.sub(" ", why.group(1)).strip() if why else "",
                )) from e
        return r

    def _get_soup(self, url: str) -> Tuple[BeautifulSoup, str]:
        r = self._request("GET", url)
        if r.status_code != 200:
            raise RuntimeError(
                "CBK page answered HTTP %d (%s) -- refusing to report an "
                "empty source. Do not retry in a loop." % (r.status_code, url))
        r.encoding = "utf-8"
        return BeautifulSoup(r.text, PARSER), r.url

    @staticmethod
    def _clean(blocks: List[Tag], url: str) -> Tuple[str, str]:
        """(body html, visible text) of the given modules."""
        parts = []
        for block in blocks:
            for c in block.find_all(string=lambda s: isinstance(s, Comment)):
                c.extract()
            for el in block.find_all(["script", "style", "noscript", "button",
                                      "form", "iframe"]):
                el.decompose()
            parts.append("".join(str(c) for c in block.contents).strip())
        frag = absolutize_html("\n".join(p for p in parts if p), url)
        return frag, _visible_text(frag)

    def _stamp(self, url: str) -> Tuple[str, str]:
        """(hash key, basis) for a file, from its HEAD."""
        if self.fetch_stamps:
            try:
                r = self._request("HEAD", url)
                ctype = (r.headers.get("Content-Type") or "").lower()
                lastmod = (r.headers.get("Last-Modified") or "").strip()
                length = (r.headers.get("Content-Length") or "").strip()
                if r.status_code == 200 and any(c in ctype for c in _FILE_CTYPES) \
                        and (lastmod or length):
                    return "%s|%s" % (lastmod, length), "url|title|last-modified|length"
                if r.status_code == 403:
                    self._restricted.append(url)
                    if len(self._restricted) > MAX_RESTRICTED:
                        raise RuntimeError(
                            "CBK: %d files restricted by the site (%s), last %s "
                            "-- not housekeeping; stopping" % (
                                len(self._restricted), _RESTRICTED, url))
                    self._warnings.append("restricted by the site (Sucuri %s), "
                                          "listed but not downloadable: %s"
                                          % (_RESTRICTED, url))
                    return "", "url|title (WEAK: site restricts the file)"
                self._warnings.append("HEAD %s: HTTP %d, %r -- weak hash"
                                      % (url, r.status_code, ctype))
            except requests.RequestException as e:
                self._warnings.append("HEAD %s failed (%s) -- weak hash"
                                      % (url, type(e).__name__))
        return "", "url|title (WEAK)"

    def _file_doc(self, url: str, title: str, folder: List[str], category: str,
                  found_on: str, **meta) -> RegulatoryDocument:
        key, basis = self._stamp(url)
        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=category,
            title=title,
            document_url=url,
            reference_no=meta.pop("reference_no", None),
            year=meta.pop("year", None),
            source_page_url=found_on,
            file_type=(_ext(url)[1:].upper() if _ext(url) in _KEPT_AS_IS
                       else _ext_type(url)),
            doc_path=[self.regulator] + TRAIL + folder + [title],
            content_hash=content_key("%s|%s|%s" % (url, title, key)),
            extra_meta={"section_path": " > ".join(TRAIL + folder),
                        "record_kind": "document",
                        "hash_basis": basis,
                        **({"file_stamp": key} if key else {}),
                        **meta},
        )

    def _page_doc(self, url: str, title: str, frag: str, text: str,
                  folder: List[str], category: str) -> RegulatoryDocument:
        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=category,
            title=title,
            document_url=url,
            source_page_url=url,
            file_type="HTML",
            doc_path=[self.regulator] + TRAIL + folder + [title],
            document_html=frag,
            content_hash=content_key(text),
            extra_meta={"section_path": " > ".join(TRAIL + folder),
                        "content_text": text,
                        "record_kind": "page",
                        "content_hash_basis": "visible-text"},
        )

    def _skipped(self, url: str) -> bool:
        if _ext(url) in self.skip_extensions:
            self._warnings.append("skipped (%s not stored): %s" % (_ext(url), url))
            return True
        return False

    # ------------------------------------------------------------------ #
    def _legislation(self) -> List[RegulatoryDocument]:
        soup, url = self._get_soup(self.seed_url)
        modules = soup.select(_LEG_MODULES)
        if len(modules) < 2:
            raise RuntimeError(
                "expected 2 text modules (%s) on %s, found %d -- the page "
                "changed shape" % (_LEG_MODULES, url, len(modules)))
        title = _text(soup.select_one("h1")) or "Legislation and Guidelines"
        listing = modules[1]

        # Walk module 1 in document order before _clean touches it.
        heading, items = "", []
        for el in listing.descendants:
            if not isinstance(el, Tag):
                continue
            if _is_heading(el):
                heading = _text(el)
            elif el.name == "a" and el.get("href") \
                    and not el["href"].startswith("#"):
                items.append((heading, urljoin(url, el["href"].strip()),
                              _SITE_SUFFIX.sub("", _text(el))))

        frag, text = self._clean(modules, url)
        docs = [self._page_doc(url, title, frag, text, [], "Legislation and Guidelines")]

        for heading, href, name in items:
            if not heading:
                self._warnings.append("link before any heading: %s" % href)
            folder = [heading] if heading else []
            category = heading or "Legislation and Guidelines"
            if self._skipped(href):
                continue
            if _is_file(href):
                docs.append(self._file_doc(href, name or _name_from_url(href),
                                           folder, category, url))
                continue
            page, purl = self._get_soup(href)
            ptitle = name or _text(page.select_one("h1")) or href
            blocks = page.select(_LINKED_MODULES)
            files = self._linked_files(blocks, purl)
            pfrag, ptext = self._clean(blocks, purl)
            if not ptext:
                raise RuntimeError("no text (%s) on %s -- the page changed shape"
                                   % (_LINKED_MODULES, href))
            docs.append(self._page_doc(href, ptitle, pfrag, ptext, folder, category))
            for fname, furl in files:
                if self._skipped(furl):
                    continue
                # The post's own documents ("Draft ...") each get a subfolder;
                # its notice and comments template sit beside the page row.
                sub = [fname] if fname.lower().startswith("draft ") else []
                docs.append(self._file_doc(furl, fname, folder + sub, category, href))
        return docs

    @staticmethod
    def _linked_files(blocks: List[Tag], url: str) -> List[Tuple[str, str]]:
        """(name, url) of each file linked from a linked page's text. A bare
        "here" is named by its sentence."""
        out, seen = [], set()
        for block in blocks:
            for a in block.find_all("a", href=True):
                furl = urljoin(url, a["href"].strip())
                if not _is_file(furl) or furl in seen:
                    continue
                seen.add(furl)
                name = _text(a)
                if re.fullmatch(r"(?:click\s+)?here\W*", name, re.I):
                    host = a.find_parent(["p", "li"]) or a.parent
                    name = _HERE.sub("", _text(host)).strip()
                out.append((name or _name_from_url(furl), furl))
        return out

    def _circulars(self) -> List[RegulatoryDocument]:
        soup, url = self._get_soup(self.seed_url)
        table = soup.select_one(_CIRC_TABLE)
        if table is None:
            raise RuntimeError("no %s on %s -- the page changed shape"
                               % (_CIRC_TABLE, url))
        docs = []
        for tr in table.find_all("tr"):
            cells = tr.find_all("td")
            if len(cells) < 2:
                continue
            year, a = _text(cells[0]), cells[1].find("a", href=True)
            if a is None:
                self._warnings.append("circular row with no link: %r" % _text(cells[1]))
                continue
            href = urljoin(url, a["href"].strip())
            if self._skipped(href):
                continue
            name = _text(cells[1]) or _name_from_url(href)
            m = _REF.match(name)
            docs.append(self._file_doc(
                href, name, ["Circulars", year or "Undated"], "Circulars", url,
                reference_no=m.group(1).strip() if m else None,
                year=year if year.isdigit() else None))
        # Site order kept on purpose (user, 2026-10-30), though the table runs
        # 2026, 2024, 2025 ... 2011, 2009, 2010.
        return docs

    # ------------------------------------------------------------------ #
    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        t0 = time.time()
        self._warnings, self._restricted = [], []
        docs = (self._legislation() if self.listing == "legislation"
                else self._circulars())
        rows = sum(1 for d in docs if d.extra_meta.get("record_kind") == "document")
        if rows < MIN_ROWS[self.listing]:
            raise RuntimeError(
                "CBK %s: %d file rows, expected at least %d -- refusing to "
                "report a partial listing" % (self.listing, rows,
                                              MIN_ROWS[self.listing]))
        weak = sum(1 for d in docs if "WEAK" in d.extra_meta.get("hash_basis", ""))
        self.last_result = {
            "run": {"seed": self.seed_url, "links": len(docs), "unread": 0,
                    "blocked_pages": 0, "weak_hashes": weak,
                    "restricted_files": list(self._restricted),
                    "warnings": list(self._warnings)},
            "by_source": {self.source_system: len(docs)},
            "source": self.seed_url,
        }
        logger.info("CBKKenyaCrawler[%s] -> %d document(s), %d weak hash(es), "
                    "%.0fs", self.listing, len(docs), weak, time.time() - t0)
        docs = stamp_content_hashes(docs)
        return docs[:limit] if limit else docs
