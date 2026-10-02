"""GovInfoPublicLawCrawler — one U.S. public law from govinfo.gov (the GPO).

ONE ROW IS ONE PUBLIC LAW. GPO publishes each enrolled law as a package
(`PLAW-107publ56`) offering a PDF, a plain-text `.htm` rendering and a MODS
metadata record. The `.htm` is taken as the document; the MODS is read for the
publisher's own metadata and for an identity check.

`.../xml/<PKG>.xml` RETURNS HTTP 200 WITH AN HTML ERROR PAGE, not XML. See
UC-2-Scratch/GOVINFO/.
"""

from __future__ import annotations

import html as _html
import logging
import re
import time
import unicodedata
from typing import Dict, List, Optional

import requests

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://www.govinfo.gov"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

#: The whole law, and the ONLY thing hashed. GPO wraps its plain-text rendering
#: in a single `<pre>`; there is no other markup in the file.
_PRE = re.compile(r"<pre>(.*?)</pre>", re.S | re.I)

#: MODS carries several `<titleInfo>` blocks, and the ones after the first
#: `<relatedItem>` belong to the BILLS this law came from (H.R. 3162, H.R. 5548
#: for PL 107-56) rather than to the law. Everything is read from the cut.
_RELATED = re.compile(r"<relatedItem\b", re.I)
_TITLEINFO = re.compile(r"<titleInfo\b([^>]*)>(.*?)</titleInfo>", re.S | re.I)
_TITLE = re.compile(r"<title>(.*?)</title>", re.S | re.I)
_PUBLIC_LAW_ONLY = re.compile(r"^Public Law \d+-\d+$", re.I)

_TAG = re.compile(r"<[^>]+>")

# openpyxl raises on these, inside save(), after the whole crawl has run.
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

#: Retries for a fetch that could not be ANSWERED. A GET changes nothing.
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0

#: Statuses that are an ANSWER rather than a failure to answer.
_FINAL_STATUSES = (200, 401, 403, 404, 410)


def _decode(resp) -> str:
    """Decode a response as UTF-8 unless it declares otherwise.

    NOT `resp.text`: this host serves `Content-Type: text/html` with no charset
    and `requests` then falls back to ISO-8859-1 per RFC 2616, so the section
    symbols and em dashes throughout the Statutes at Large text would decode to
    mojibake and the mojibake is what would be stored and hashed.
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
    """NFKC-normalise, drop the zero-width marks NFKC keeps, collapse whitespace."""
    return " ".join(
        unicodedata.normalize("NFKC", s or "").replace("﻿", "").split())


def _mods_root(mods: str) -> str:
    """The MODS record down to its first `<relatedItem>`.

    Past that point the titles and dates describe the bills this law was made
    from, not the law, and reading one of those as the title would file the
    USA PATRIOT Act under "United States House Bill 3162".
    """
    cut = _RELATED.search(mods)
    return mods[:cut.start()] if cut else mods


def _mods_value(mods_root: str, tag: str) -> str:
    """The first occurrence of a simple MODS element, entities resolved."""
    m = re.search(r"<(?:mods:)?%s\b[^>]*>(.*?)</(?:mods:)?%s>"
                  % (re.escape(tag), re.escape(tag)), mods_root, re.S | re.I)
    return _norm(_html.unescape(_TAG.sub(" ", m.group(1)))) if m else ""


def _mods_titles(mods_root: str):
    """`(official_title, [alternative titles])` from the law's own MODS record.

    GPO files the official long title under a bare `<titleInfo>` and the popular
    short title plus the "Public Law N-NN" form under `type="alternative"`.
    """
    official, alternatives = "", []
    for attrs, block in _TITLEINFO.findall(mods_root):
        t = _TITLE.search(block)
        if not t:
            continue
        value = _norm(_html.unescape(_TAG.sub(" ", t.group(1))))
        if not value:
            continue
        if "alternative" in attrs.lower():
            alternatives.append(value)
        elif not official:
            official = value
    return official, alternatives


class GovInfoPublicLawCrawler:
    """One U.S. public law, read from GPO's plain-text rendering of it.

    `package_id`, `doc_folder` and `source_system` come from the YAML so a
    change in how GPO titles a law cannot silently reshape `doc_path`.
    """

    def __init__(
        self,
        regulator: str,
        source_system: str,
        package_id: str,
        doc_folder: str,
        htm_url: Optional[str] = None,
        pdf_url: Optional[str] = None,
        mods_url: Optional[str] = None,
        details_url: Optional[str] = None,
        category: Optional[str] = None,
        expected_title: Optional[str] = None,
        min_text_chars: int = 1000,
        timeout: int = 120,
        delay: float = 1.0,
    ):
        self.regulator = regulator
        self.source_system = source_system
        # "PLAW-107publ56". GPO's own package id, and the key to every url it
        # serves for this law.
        self.package_id = package_id
        # The doc_path tier between source_system and the document. Each law
        # gets its OWN folder: the folder is what scopes `disappeared`, so two
        # laws sharing one would propose each other for withdrawal.
        self.doc_folder = doc_folder
        self.htm_url = htm_url or "%s/content/pkg/%s/html/%s.htm" % (
            BASE, package_id, package_id)
        self.pdf_url = pdf_url or "%s/content/pkg/%s/pdf/%s.pdf" % (
            BASE, package_id, package_id)
        self.mods_url = mods_url or "%s/metadata/pkg/%s/mods.xml" % (
            BASE, package_id)
        self.details_url = details_url or "%s/app/details/%s" % (BASE, package_id)
        # The completeness gate's grouping key; not a doc_path crumb.
        self.listing_title = doc_folder
        self.category = category or source_system
        # A recorded observation of GPO's own wording, not our chosen title —
        # comparing against our own would warn forever about a correct difference.
        self.expected_title = expected_title
        # A FLOOR, NOT THE COUNT. A short rendering is indistinguishable from
        # GPO serving an error page, so it fails the run rather than proposing
        # the law for withdrawal.
        self.min_text_chars = int(min_text_chars)
        self.timeout = timeout
        self.delay = float(delay)
        self.last_result: Dict = {}
        self._session: Optional[requests.Session] = None

    def _sess(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers.update({"User-Agent": USER_AGENT,
                              "Accept-Language": "en-US,en;q=0.9"})
            self._session = s
        return self._session

    def _get(self, url: str) -> str:
        """GET with retries. A status that is not an ANSWER raises rather than
        returning an empty body — an empty law would be read as a withdrawal."""
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

        # ---- the law itself -------------------------------------------------
        page = self._get(self.htm_url)
        pre = _PRE.search(page)
        if not pre:
            # THE 200-STATUS TRAP. govinfo answers a missing rendering with its
            # own Drupal page under HTTP 200, so "it fetched fine" is not
            # evidence that the law came back. Measured on the `.xml` rendering
            # of six unrelated packages 2026-09-22 — all six returned this page.
            raise RuntimeError(
                "govinfo %s: %s has no <pre> block. govinfo serves a missing "
                "rendering as HTTP 200 with its own HTML page, so this is most "
                "likely that page and not the law. Refusing to store it."
                % (self.package_id, self.htm_url))

        content_text = _ILLEGAL_XLSX.sub(
            " ", _norm(_html.unescape(pre.group(1))))
        if len(content_text) < self.min_text_chars:
            raise RuntimeError(
                "govinfo %s: %d characters of text, below the floor of %d. "
                "Refusing a short law — it is indistinguishable from a truncated "
                "or replaced rendering."
                % (self.package_id, len(content_text), self.min_text_chars))

        # ---- the publisher's metadata record --------------------------------
        mods = self._get(self.mods_url)
        if "<mods" not in mods[:2000].lower():
            raise RuntimeError(
                "govinfo %s: %s is not a MODS record — same 200-status trap as "
                "the .htm. Refusing to store a law with no metadata."
                % (self.package_id, self.mods_url))
        root = _mods_root(mods)

        # IDENTITY CHECK, not decoration. `accessId` is GPO's own name for the
        # package; if it disagrees with the configured one we fetched a
        # different law and would file it under this folder.
        access_id = _mods_value(root, "accessId")
        if access_id and access_id != self.package_id:
            raise RuntimeError(
                "govinfo %s: %s describes package %r. Refusing to store one law "
                "under another's folder."
                % (self.package_id, self.mods_url, access_id))

        official_title, alternatives = _mods_titles(root)
        # The popular short title ("...(USA PATRIOT ACT) Act of 2001") is what a
        # reviewer recognises; "Public Law 107-56" is also filed as an
        # alternative and is a reference, not a name.
        short_title = next(
            (a for a in alternatives if not _PUBLIC_LAW_ONLY.match(a)), "")
        title = short_title or official_title
        if not title:
            raise RuntimeError(
                "govinfo %s: no <titleInfo> title in %s. The metadata template "
                "has changed; refusing to guess a title."
                % (self.package_id, self.mods_url))
        if self.expected_title and self.expected_title != title:
            warnings.append("the recorded title %r is now %r"
                            % (self.expected_title, title))
            logger.warning(warnings[-1])

        law_number = next(
            (a for a in alternatives if _PUBLIC_LAW_ONLY.match(a)), "")

        meta = {
            "crawl_source": self.listing_title,
            "package_id": self.package_id,
            "access_id": access_id,
            "official_title": official_title,
            "alternative_titles": " | ".join(alternatives),
            "law_number": law_number,
            "congress": _mods_value(root, "congress"),
            "doc_class": _mods_value(root, "docClass"),
            "branch": _mods_value(root, "branch"),
            "publisher": _mods_value(root, "publisher"),
            # The date Congress enacted it. This is the only date here that
            # describes the LAW; it has not moved since 2001 and will not.
            "date_issued": _mods_value(root, "dateIssued"),
            # PROVENANCE ONLY — GPO'S BULK RE-INGEST STAMP, excluded from the
            # hash. Measured 2026-09-22: every public law of 2001 carries a
            # `recordChangeDate` inside one 90-minute window on 2026-01-04, and
            # the HTTP `Last-Modified` and the sitemap `lastmod` say the same
            # thing because they are one event. Hashing any of them would
            # re-version the whole corpus on GPO's next re-ingest.
            "record_change_date": _mods_value(root, "recordChangeDate"),
            "record_creation_date": _mods_value(root, "recordCreationDate"),
            "content_text": content_text,
            "text_chars": len(content_text),
            "htm_url": self.htm_url,
            "pdf_url": self.pdf_url,
            "mods_url": self.mods_url,
            # Where a reviewer should click. `document_url` is the plain-text
            # rendering, which is correct for the library and plain for a person.
            "landing_page": self.details_url,
            "source": self.details_url,
        }

        doc = RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self.category,
            title=title,
            # Exactly one file, so `document_url` is set and attachment_links
            # stays empty — the single-file branch of the models.py contract.
            document_url=self.htm_url,
            doc_path=[self.regulator, self.source_system, self.doc_folder, title],
            file_type="HTML",
            source_page_url=self.details_url,
            published_date=meta["date_issued"] or None,
            reference_no=law_number or self.package_id,
            # The file is one `<pre>` of plain text; there is no markup worth
            # storing and storing it would only duplicate `content_text`.
            document_html=None,
            extra_meta=meta,
        )
        docs = [doc]

        # SINGLE EXIT. The hash is the law's own text and nothing else — no
        # date from the MODS record reaches it. `stamp_content_hashes` never
        # overwrites it and is left in place as the floor for any future branch
        # that forgets.
        for d in docs:
            d.content_hash = content_key(content_text)
            d.extra_meta["content_hash_basis"] = "gpo-plaintext-rendering"
        docs = stamp_content_hashes(docs)

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.listing_title: len(docs)},
            "source": self.details_url,
        }
        logger.info("GovInfoPublicLawCrawler %s: %r, %d chars, issued %s, "
                    "record changed %s", self.package_id, title,
                    len(content_text), meta["date_issued"],
                    meta["record_change_date"])
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs
