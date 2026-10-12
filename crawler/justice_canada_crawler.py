"""JusticeCanadaActCrawler — one consolidated federal Act from laws-lois.justice.gc.ca.

ONE ROW IS ONE ACT. The landing page is a table of contents whose `page-N.html`
links are pagination, not documents. The row's file is the Act's PDF; its text
and `document_html` come from FullText.html, because the PDF is bilingual in two
columns and prints the consolidation date on every page.

The site's "Act current to <date>" banner is a SITE-WIDE render-time value and
must never reach the fingerprint; it sits in `div.legisHeader`, which is dropped.

`JusticeXmlSweep` monitors the Acts through their XML, which the workbook never
stores. See UC-2-Scratch/JUSTICE_CANADA/.
"""

from __future__ import annotations

import copy
import html as _html
import logging
import re
import time
import unicodedata
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Comment

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from dynamic_crawler import fingerprint as probe
from dynamic_crawler.changesignal import observation_for
from dynamic_crawler.inventory_sweep import StoredInventorySweep
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://laws-lois.justice.gc.ca"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

# The page's own title and its `Full Document` links. Both hrefs are read back
# rather than assumed so a change in how Justice names its files is a failure
# instead of a silent switch to the wrong Act.
_H1 = re.compile(r"<h1[^>]*class='HeadTitle'[^>]*>(.*?)</h1>", re.S | re.I)
_PDF_HREF = re.compile(r"href='(/PDF/[^']+\.pdf)'", re.I)
_HTML_HREF = re.compile(r"href='(FullText\.html)'", re.I)
_ASSENTED = re.compile(r"id='assentedDate'>(.*?)</p>", re.S | re.I)

# The two dates the header sentence carries. `current to` is the trap.
_CURRENT_TO = re.compile(r"current to\s*(\d{4}-\d{2}-\d{2})", re.I)
_LAST_AMENDED = re.compile(r"on\s*(\d{4}-\d{2}-\d{2})", re.I)

# The XML root, read only by JusticeXmlSweep's confirm step.
_ROOT = re.compile(r"<Statute\b[^>]*>", re.I)

#: THE THREE VOLATILE XML FIELDS, and all three say the same thing: when this
#: consolidation was cut, not what it says. Removed before the monitoring
#: confirm hash, so a currency-only re-cut reads as a republish.
_VOLATILE_ATTRS = ("lims:current-date", "lims:pit-date")
_CONSOLIDATION_STAGES = re.compile(
    r'<Stages\b[^>]*stage="consolidation"[^>]*>.*?</Stages>', re.S | re.I)

_TAG = re.compile(r"<[^>]+>")

#: A space goes at these boundaries only; get_text(" ") spaced every inline tag too.
_BLOCK = ("p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "tr", "dt", "dd",
          "div", "section", "header", "table", "ul", "ol", "dl", "br", "hr",
          "blockquote", "pre")
#: Spans the site's CSS shows on their own line, written with no space between
#: them: "PART I" + "Administrative Officials", "SCHEDULE" + "(Section 2)".
_BLOCK_SPANS = ("[class^=HLabel], [class^=HTitleText], .scheduleLabel, "
                ".OriginatingRef, .scheduleTitleText")

#: Kept in document_html, left out of the text and hash: screen-reader labels
#: ("Marginal note:", "Footnote") and the per-section "Previous Version" box,
#: whose dated href is site navigation, not law.
_NOT_TEXT = (".wb-invisible", "div.PITLink")

# openpyxl raises on these, inside save(), after the whole crawl has run.
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

#: Retries for a fetch that could not be ANSWERED. A GET changes nothing.
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0

#: Statuses that are an ANSWER rather than a failure to answer.
_FINAL_STATUSES = (200, 401, 404, 410)


def _decode(resp) -> str:
    """Decode a response as UTF-8 unless it declares otherwise.

    NOT `resp.text`. This host serves `Content-Type: text/xml` and `text/html`
    with NO charset, and `requests` then falls back to ISO-8859-1 per RFC 2616 —
    so UTF-8 bytes decode to mojibake (U+2002 arrives as "\\u00e2\\u0080\\u0082")
    and the mojibake is what gets stored, hashed and analysed. Measured
    2026-09-09.
    """
    declared = ""
    ctype = resp.headers.get("Content-Type", "")
    if "charset=" in ctype.lower():
        declared = ctype.lower().split("charset=", 1)[1].split(";")[0].strip()
    for enc in (declared, "utf-8", resp.apparent_encoding, "latin-1"):
        if not enc:
            continue
        try:
            return resp.content.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    return resp.content.decode("latin-1", errors="replace")


def _norm(s: str) -> str:
    """NFKC-normalise, drop the zero-width marks NFKC keeps, collapse whitespace.

    `U+FEFF` is not whitespace to `str.split()`, so a BOM would otherwise lead
    the stored text with an invisible character and carry it into the hash.
    """
    s = unicodedata.normalize("NFKC", s or "")
    return " ".join(re.sub(r"[﻿​‌‍⁠]", "", s).split())


def _text_of(markup: str) -> str:
    """Visible text of a small markup fragment, entities resolved."""
    if not markup:
        return ""
    return _norm(_html.unescape(_TAG.sub(" ", markup)))


def _inline_text_of(markup: str) -> str:
    """Visible text of a run of INLINE markup, closing up rather than spacing out.

    The Act titles wrap their chapter citation in `<abbr>`, so `_text_of` renders
    "(<abbr>R.S.C.</abbr>, 1985" as "( R.S.C. , 1985". The title is a `doc_path`
    crumb and therefore an identity field, so the difference is not cosmetic.
    """
    if not markup:
        return ""
    return _norm(_html.unescape(_TAG.sub("", markup)))


def _strip_volatile(xml: str) -> str:
    """The Act's XML with the three consolidation-currency fields removed."""
    root_match = _ROOT.search(xml)
    if root_match:
        root = root_match.group(0)
        cleaned = root
        for attr in _VOLATILE_ATTRS:
            cleaned = re.sub(r'\s*\b%s="[^"]*"' % re.escape(attr), "", cleaned)
        xml = xml[:root_match.start()] + cleaned + xml[root_match.end():]
    return _CONSOLIDATION_STAGES.sub("", xml)


def xml_url_for(act_code: str) -> str:
    return "%s/eng/XML/%s.xml" % (BASE, act_code)


def _act_element(page: str, base: str):
    """FullText.html's `div.container` minus `div.legisHeader`: the Act, its
    Related Provisions and Amendments Not in Force, with absolute links."""
    soup = BeautifulSoup(page, "html.parser")
    body = soup.select_one("div.docContents")
    if body is None or body.parent is None:
        return None
    el = body.parent
    for t in el.select("div.legisHeader"):
        t.decompose()
    for t in el.find_all(["script", "style", "noscript"]):
        t.decompose()
    for c in el.find_all(string=lambda s: isinstance(s, Comment)):
        c.extract()                     # the server farm's rotating ELAPSE/CURATE marker
    for a in el.find_all("a", href=True):
        if not a["href"].startswith(("#", "mailto:", "tel:")):
            a["href"] = urljoin(base, a["href"])
    return el


def _block_text(el) -> str:
    """Text with breaks at block boundaries only, minus `_NOT_TEXT`; `el` is not modified."""
    el = copy.copy(el)
    # A footnote marker is a <sup> in a link whose hidden label reads "Footnote "
    # (in the text) or "Return to footnote " (opening the footnote); hrefs vary
    # (#fn_…, #footnoteb_e). With the label gone it would weld to its neighbour
    # ("Actc", "aS.C. 2021"), so it keeps a space on that side (2026-10-12).
    for a in el.find_all("a"):
        label = a.find(class_="wb-invisible")
        said = label.get_text().strip().lower() if label else ""
        if said.startswith("return to footnote"):
            a.insert_after(" ")
        elif said.startswith("footnote"):
            a.insert_before(" ")
    for sel in _NOT_TEXT:
        for t in el.select(sel):
            t.decompose()
    # A list the page styles inline stays inline: SOR/2002-184 writes a historical
    # note as "[<ul style="display:inline"><li>SOR/2019-240, ss. 26, 27</li></ul>]".
    inline = set()
    for t in el.select('[style*="display:inline"]'):
        inline.add(id(t))
        inline.update(id(d) for d in t.find_all(True))
    for tag in el.find_all(_BLOCK) + el.select(_BLOCK_SPANS):
        if id(tag) in inline:
            continue
        tag.insert_before("\n")
        tag.insert_after("\n")
    return el.get_text("")


class JusticeCanadaActCrawler:
    """One consolidated Act: the PDF as its file, FullText.html as its content.

    `act_code`, `act_folder` and `source_system` come from the YAML so a site
    retitle cannot silently reshape `doc_path`.
    """

    def __init__(
        self,
        regulator: str,
        source_system: str,
        act_code: str,
        act_folder: str,
        listing_url: Optional[str] = None,
        pdf_url: Optional[str] = None,
        category: Optional[str] = None,
        expected_title: Optional[str] = None,
        min_text_chars: int = 1000,
        timeout: int = 120,
        delay: float = 1.0,
    ):
        self.regulator = regulator
        self.source_system = source_system
        # "B-3". Justice's own chapter code, and the key to every url it serves.
        self.act_code = act_code
        # The doc_path tier between source_system and the document. Each Act gets
        # its OWN folder: the folder is what scopes `disappeared`, so two Acts
        # sharing one would propose each other for withdrawal.
        self.act_folder = act_folder
        self.listing_url = listing_url or "%s/eng/acts/%s/" % (BASE, act_code)
        self.pdf_url = pdf_url or "%s/PDF/%s.pdf" % (BASE, act_code)
        # The completeness gate's grouping key; not a doc_path crumb.
        self.listing_title = act_folder
        self.category = category or source_system
        # A recorded observation of the page, not our chosen title — comparing
        # against our own wording would warn forever about a correct difference.
        self.expected_title = expected_title
        # A FLOOR, NOT THE COUNT. A short Act is indistinguishable from Justice
        # repealing it, so it fails the run rather than proposing withdrawal.
        self.min_text_chars = int(min_text_chars)
        self.timeout = timeout
        self.delay = float(delay)
        self.last_result: Dict = {}
        self._session: Optional[requests.Session] = None

    def _sess(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers.update({"User-Agent": USER_AGENT,
                              "Accept-Language": "en-CA,en;q=0.9"})
            self._session = s
        return self._session

    def _get(self, url: str) -> str:
        """GET with retries. A status that is not an ANSWER raises rather than
        returning an empty body — an empty Act would be read as a withdrawal."""
        last: Optional[Exception] = None
        for attempt in range(RETRY_ATTEMPTS):
            if attempt or self.delay:
                time.sleep(self.delay if not attempt else RETRY_BACKOFF * attempt)
            try:
                r = self._sess().get(url, timeout=self.timeout)
                if r.status_code in _FINAL_STATUSES:
                    if r.status_code != 200:
                        raise RuntimeError("%s returned %d" % (url, r.status_code))
                    return _decode(r)
                last = RuntimeError("%s returned %d" % (url, r.status_code))
            except requests.RequestException as e:
                last = e
        raise RuntimeError("could not fetch %s: %s" % (url, last))

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        warnings: List[str] = []

        page = self._get(self.listing_url)

        h1 = _H1.search(page)
        title = _inline_text_of(h1.group(1)) if h1 else ""
        if not title:
            raise RuntimeError(
                "Justice Canada %s: no <h1 class='HeadTitle'> at %s. The page "
                "template has changed; refusing to guess a title."
                % (self.act_code, self.listing_url))
        if self.expected_title and self.expected_title != title:
            warnings.append("the recorded title %r is now %r"
                            % (self.expected_title, title))
            logger.warning(warnings[-1])

        # READ THE PDF LINK BACK OFF THE PAGE rather than trusting the pattern.
        # A mismatch means Justice moved the file and we would otherwise store a
        # stale copy, or somebody else's Act, without noticing.
        advertised = _PDF_HREF.search(page)
        if not advertised:
            raise RuntimeError(
                "Justice Canada %s: %s no longer advertises a PDF Full Document."
                % (self.act_code, self.listing_url))
        if BASE + advertised.group(1) != self.pdf_url:
            raise RuntimeError(
                "Justice Canada %s: page advertises %s but this source is "
                "configured for %s. Refusing to store one Act under another's "
                "folder." % (self.act_code, BASE + advertised.group(1), self.pdf_url))
        if not _HTML_HREF.search(page):
            raise RuntimeError(
                "Justice Canada %s: %s no longer advertises FullText.html."
                % (self.act_code, self.listing_url))
        fulltext_url = urljoin(self.listing_url, "FullText.html")

        # The header sentence carries both dates. Both are provenance ONLY;
        # `current to` is site-wide and render-time.
        banner = _ASSENTED.search(page)
        banner_text = _text_of(banner.group(1)) if banner else ""
        cur = _CURRENT_TO.search(banner_text)
        amd = _LAST_AMENDED.search(banner_text)

        act = _act_element(self._get(fulltext_url), fulltext_url)
        if act is None:
            raise RuntimeError(
                "Justice Canada %s: %s has no div.docContents — the template has "
                "changed." % (self.act_code, fulltext_url))
        document_html = _ILLEGAL_XLSX.sub(" ", str(act))
        content_text = _ILLEGAL_XLSX.sub(" ", _norm(_block_text(act)))
        if len(content_text) < self.min_text_chars:
            raise RuntimeError(
                "Justice Canada %s: %d characters of text, below the floor of "
                "%d. Refusing a short Act — it is indistinguishable from a "
                "repeal." % (self.act_code, len(content_text), self.min_text_chars))
        links = []
        for a in act.find_all("a", href=True):
            if a["href"].startswith("http") and a["href"] not in links:
                links.append(a["href"])

        meta = {
            "crawl_source": self.listing_title,
            "act_code": self.act_code,
            # Recorded, never published_date: it moves with every amendment and
            # published_date is part of a document's identity.
            "last_amended_date": amd.group(1) if amd else "",
            # PROVENANCE ONLY — site-wide, render-time, excluded from the hash.
            "consolidation_current_to": cur.group(1) if cur else "",
            "banner": banner_text,
            "content_text": content_text,
            "text_chars": len(content_text),
            # Every link in the Act's text. Deliberately not `attachment_links`:
            # these are cross-references, not this Act's files.
            "content_links": " | ".join(links),
            "fulltext_html_url": fulltext_url,
            "previous_versions_url": "%s/eng/acts/%s/PITIndex.html" % (BASE, self.act_code),
            "source": self.listing_url,
        }

        doc = RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self.category,
            title=title,
            # Exactly one file, so `document_url` is set and attachment_links
            # stays empty — the single-file branch of the models.py contract.
            document_url=self.pdf_url,
            doc_path=[self.regulator, self.source_system, self.act_folder, title],
            file_type="PDF",
            source_page_url=self.listing_url,
            published_date=None,
            reference_no=self.act_code,
            document_html=document_html,
            extra_meta=meta,
        )
        doc.content_hash = content_key(content_text)
        meta["content_hash_basis"] = "fulltext-html-text"
        docs = stamp_content_hashes([doc])          # single exit

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.listing_title: len(docs)},
            "source": self.listing_url,
        }
        logger.info("JusticeCanadaActCrawler %s: %r, %d chars, %d links, current to %s, "
                    "last amended %s", self.act_code, title, len(content_text), len(links),
                    meta["consolidation_current_to"], meta["last_amended_date"])
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs


class JusticeXmlSweep(StoredInventorySweep):
    """The stored-inventory probe, pointed at each Act's XML.

    The workbook never stores the XML: its url is built from `reference_no`, the
    act code, and its tokens live only in the change-state file. The confirm hash
    drops the three consolidation-currency fields, so a re-cut that only moves
    the dates reads as a republish.
    """

    def sweep(self):
        rows = self.repo.find_regulations_by_source(self.source_system,
                                                    regulator=self.regulator)
        if self.limit:
            rows = rows[:self.limit]
        urls = [xml_url_for(str(r.get("reference_no") or "").strip())
                if str(r.get("reference_no") or "").strip() else "" for r in rows]
        tokens = probe.tokens_for([u for u in urls if u], workers=self.workers,
                                  timeout=self.timeout)
        self._xml = {}
        observations = []
        for row, url in zip(rows, urls):
            token, basis = tokens.get(url, ("", probe.BASIS_NONE))
            obs = observation_for(row, self.identity, token=token, basis=basis)
            self._xml[obs.key] = url
            observations.append(obs)
        self.stats = {"stored_rows": len(rows),
                      "no_probeable_url": sum(1 for u in urls if not u),
                      "skipped_hosts": {},
                      **probe.summarise(tokens)}
        return observations

    def confirm(self, obs) -> Optional[str]:
        url = getattr(self, "_xml", {}).get(obs.key, "")
        if not self.confirm_required or not url:
            return None
        r = requests.get(url, timeout=self.timeout * 3, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        return content_key(_norm(_strip_volatile(_decode(r))))
