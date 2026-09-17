"""JusticeCanadaActCrawler — one consolidated federal Act from laws-lois.justice.gc.ca.

ONE ROW IS ONE ACT. The landing page is a table of contents whose `page-N.html`
links are pagination, not documents; the Act itself is the XML the page offers
under `Full Document`.

The site's prominent "Act current to <date>" banner is a SITE-WIDE render-time
value and must never reach the fingerprint. See UC-2-Scratch/JUSTICE_CANADA/.
"""

from __future__ import annotations

import html as _html
import logging
import re
import time
import unicodedata
from typing import Dict, List, Optional, Tuple

import requests

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://laws-lois.justice.gc.ca"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

# The page's own title and its `Full Document` links. The XML href is read back
# rather than assumed so a change in how Justice names its files is a warning
# instead of a silent switch to the wrong Act.
_H1 = re.compile(r"<h1[^>]*class='HeadTitle'[^>]*>(.*?)</h1>", re.S | re.I)
_XML_HREF = re.compile(r"href='(/eng/XML/[^']+\.xml)'", re.I)
_PDF_HREF = re.compile(r"href='(/PDF/[^']+\.pdf)'", re.I)
_ASSENTED = re.compile(r"id='assentedDate'>(.*?)</p>", re.S | re.I)

# The two dates the header sentence carries. `current to` is the trap.
_CURRENT_TO = re.compile(r"current to\s*(\d{4}-\d{2}-\d{2})", re.I)
_LAST_AMENDED = re.compile(r"on\s*(\d{4}-\d{2}-\d{2})", re.I)

# The XML root's own metadata. `fid` is Justice's stable identifier for the Act
# and does not rotate the way an upload hash does.
_ROOT = re.compile(r"<Statute\b[^>]*>", re.I)
_ATTR = lambda n: re.compile(r'\b%s="([^"]*)"' % re.escape(n))

#: THE THREE VOLATILE FIELDS, and all three say the same thing: when this
#: consolidation was cut, not what it says. Each moves on Justice's roughly
#: monthly re-cut while the Act's text stands still, so each would write a
#: version row per Act per month. Measured 2026-09-09 — see the findings doc.
#:
#: `current-date` and `pit-date` occur EXACTLY ONCE each, on the root element.
#: The consolidation `<Stages>` block occurs once and is the one that hides in
#: VISIBLE TEXT, so stripping attributes alone does not reach it.
_VOLATILE_ATTRS = ("lims:current-date", "lims:pit-date")
_CONSOLIDATION_STAGES = re.compile(
    r'<Stages\b[^>]*stage="consolidation"[^>]*>.*?</Stages>', re.S | re.I)

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")

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
    2026-09-09; XML with no encoding declaration is UTF-8 by specification.
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

    `U+FEFF` is not whitespace to `str.split()`, so a file carrying a BOM — F-3.3
    has one, the only Act of five measured that does — would lead its stored text
    with an invisible character and carry it into the fingerprint.
    """
    return " ".join(
        unicodedata.normalize("NFKC", s or "").replace("\ufeff", "").split())


def _text_of(markup: str) -> str:
    """Visible text of XML/HTML, entities resolved and whitespace collapsed.

    Tags become a SPACE so adjacent block elements do not run together.
    """
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
    """The Act's XML with the three consolidation-currency fields removed.

    This is the fingerprint basis: everything else is kept, including attributes,
    so a provision merely coming into force still moves the hash.
    """
    root_match = _ROOT.search(xml)
    if root_match:
        root = root_match.group(0)
        cleaned = root
        for attr in _VOLATILE_ATTRS:
            cleaned = re.sub(r'\s*\b%s="[^"]*"' % re.escape(attr), "", cleaned)
        xml = xml[:root_match.start()] + cleaned + xml[root_match.end():]
    return _CONSOLIDATION_STAGES.sub("", xml)


class JusticeCanadaActCrawler:
    """One consolidated Act, read from its XML rendering.

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
        xml_url: Optional[str] = None,
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
        self.xml_url = xml_url or "%s/eng/XML/%s.xml" % (BASE, act_code)
        # The completeness gate's grouping key; not a doc_path crumb.
        self.listing_title = act_folder
        self.category = category or source_system
        # A recorded observation of the page, not our chosen title — comparing
        # against our own wording would warn forever about a correct difference.
        self.expected_title = expected_title
        # A FLOOR, NOT THE COUNT. A short XML is indistinguishable from Justice
        # repealing the Act, so it fails the run rather than proposing withdrawal.
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

        # READ THE XML LINK BACK OFF THE PAGE rather than trusting the pattern.
        # A mismatch means Justice moved the file and we would otherwise fetch a
        # stale copy, or somebody else's Act, without noticing.
        advertised = _XML_HREF.search(page)
        if not advertised:
            warnings.append("the page no longer advertises an XML Full Document; "
                            "falling back to the configured url")
            logger.warning(warnings[-1])
        elif BASE + advertised.group(1) != self.xml_url:
            raise RuntimeError(
                "Justice Canada %s: page advertises %s but this source is "
                "configured for %s. Refusing to store one Act under another's "
                "folder." % (self.act_code, BASE + advertised.group(1), self.xml_url))

        pdf = _PDF_HREF.search(page)
        pdf_url = BASE + pdf.group(1) if pdf else ""

        # The header sentence carries both dates. `current to` is recorded as
        # provenance ONLY — it is site-wide and render-time, and is kept out of
        # the fingerprint by `_strip_volatile`.
        banner = _ASSENTED.search(page)
        banner_text = _text_of(banner.group(1)) if banner else ""
        cur = _CURRENT_TO.search(banner_text)
        amd = _LAST_AMENDED.search(banner_text)

        xml = self._get(self.xml_url)
        if not _ROOT.search(xml):
            raise RuntimeError(
                "Justice Canada %s: %s has no <Statute> root — that is not an "
                "Act rendering." % (self.act_code, self.xml_url))

        root = _ROOT.search(xml).group(0)

        def attr(name: str) -> str:
            m = _ATTR(name).search(root)
            return m.group(1) if m else ""

        # The text stored and analysed. The consolidation stage block is dropped
        # here too: it is site plumbing, not law, and leaving it in would make the
        # stored text churn monthly even though the hash would not.
        content_text = _ILLEGAL_XLSX.sub(" ", _text_of(_strip_volatile(xml)))
        if len(content_text) < self.min_text_chars:
            raise RuntimeError(
                "Justice Canada %s: %d characters of text, below the floor of "
                "%d. Refusing a short Act — it is indistinguishable from a "
                "repeal." % (self.act_code, len(content_text), self.min_text_chars))

        meta = {
            "crawl_source": self.listing_title,
            "act_code": self.act_code,
            # Justice's own stable identifier for this Act.
            "lims_fid": attr("lims:fid"),
            "in_force": attr("in-force"),
            "has_previous_version": attr("hasPreviousVersion"),
            # THE PUBLISHER'S CHANGE STAMPS, recorded but NOT used as the
            # fingerprint: measured 2026-09-09, `lastAmendedDate` stands still
            # through coming-into-force changes that do alter the text.
            "last_amended_date": attr("lims:lastAmendedDate") or (amd.group(1) if amd else ""),
            "inforce_start_date": attr("lims:inforce-start-date"),
            # PROVENANCE ONLY — site-wide, render-time, excluded from the hash.
            "consolidation_current_to": attr("lims:current-date") or (cur.group(1) if cur else ""),
            "consolidation_pit_date": attr("lims:pit-date"),
            "banner": banner_text,
            "content_text": content_text,
            "text_chars": len(content_text),
            "xml_url": self.xml_url,
            "pdf_url": pdf_url,
            "fulltext_html_url": "%s/eng/acts/%s/FullText.html" % (BASE, self.act_code),
            # Where a reviewer should click. `document_url` is the XML, which is
            # correct for the library and unreadable for a person.
            "landing_page": self.listing_url,
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
            document_url=self.xml_url,
            doc_path=[self.regulator, self.source_system, self.act_folder, title],
            file_type="XML",
            source_page_url=self.listing_url,
            published_date=meta["last_amended_date"] or None,
            reference_no=self.act_code,
            # NOT the landing page markup: it carries a rotating server comment
            # that changes on every single request. Measured 2026-09-09.
            document_html=None,
            extra_meta=meta,
        )
        docs = [doc]

        # SINGLE EXIT. The hash is the XML minus the three consolidation-currency
        # fields; `stamp_content_hashes` never overwrites it and is left in place
        # as the floor for any future branch that forgets.
        for d in docs:
            basis = _strip_volatile(xml)
            d.content_hash = content_key(_norm(basis))
            d.extra_meta["content_hash_basis"] = "xml-minus-consolidation-currency"
        docs = stamp_content_hashes(docs)

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.listing_title: len(docs)},
            "source": self.listing_url,
        }
        logger.info("JusticeCanadaActCrawler %s: %r, %d chars, current to %s, "
                    "last amended %s", self.act_code, title, len(content_text),
                    meta["consolidation_current_to"], meta["last_amended_date"])
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs
