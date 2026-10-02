"""FintracCrawler — one controller for FINTRAC's tabs; each source entry names its `tab`.

  regulations  one row per regulation FINTRAC lists; text from laws-lois XML,
               consolidation-cut dates stripped before hashing.
  guidelines   one row per guidance page on the hub; the hidden glossary and the
               Date Modified block are stripped before hashing.

See UC-2-Scratch/FINTRAC/.
"""

from __future__ import annotations

import copy
import html as _html
import logging
import re
import time
import unicodedata
from typing import List, Optional
from urllib.parse import urljoin, urldefrag

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

JUSTICE = "https://laws-lois.justice.gc.ca"
FINTRAC = "https://fintrac-canafe.canada.ca"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

_H1 = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S | re.I)
_XML_HREF = re.compile(r"href='(/eng/XML/[^']+\.xml)'", re.I)
_PDF_HREF = re.compile(r"href='(/PDF/[^']+\.pdf)'", re.I)
_ROOT = re.compile(r"<Regulation\b[^>]*>", re.I)
_INSTRUMENT = re.compile(r"<InstrumentNumber>([^<]+)</InstrumentNumber>")
_ATTR = lambda n: re.compile(r'\b%s="([^"]*)"' % re.escape(n))

#: When the consolidation was cut, not what it says. Root attributes plus the
#: <ConsolidationDate> element, which sits in visible text. Measured 2026-09-29.
_VOLATILE_ATTRS = ("lims:current-date", "lims:pit-date")
_CONSOLIDATION_DATE = re.compile(r"<ConsolidationDate\b.*?</ConsolidationDate>", re.S)

#: Inside every guidance page's <main>: a 57,659-char glossary, identical on 80
#: of 89 pages, and the hand-kept Date Modified. Neither is the guidance.
_GUIDANCE_STRIP = ("div.hidden.definition", "div.pagedetails")

_TAG = re.compile(r"<[^>]+>")
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _norm(s: str) -> str:
    """NFKC, BOM dropped, whitespace collapsed (U+FEFF is not whitespace to split())."""
    s = unicodedata.normalize("NFKC", s).replace("﻿", "")
    return re.sub(r"\s+", " ", s).strip()


#: Elements measured inside <Text> across the six regulations; they sit mid-sentence,
#: so they vanish rather than become a space ("Regulations ." otherwise). FootnoteRef
#: keeps its space so "Act 1" stays searchable. An unknown tag still gets a space.
_INLINE = (r"(?:DefinedTermEn|DefinedTermFr|XRefExternal|XRefInternal|"
           r"DefinitionRef|Repealed|Language|Emphasis)")
_INLINE_XML = re.compile(r"</?%s\b[^>]*>" % _INLINE)
#: A letter touching the OUTSIDE of one keeps a space (Justice's I-3.3 XML has one).
_INLINE_TOUCHING = re.compile(r"(?<=\w)<%s\b[^>]*>|</%s>(?=\w)" % (_INLINE, _INLINE))

#: HTML gets a space at these boundaries only; get_text(" ") spaced every tag.
_BLOCK = ("p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "tr", "dt", "dd",
          "div", "section", "summary", "details", "caption", "table", "ul", "ol", "dl",
          "br", "hr", "blockquote", "pre", "figure", "figcaption", "header", "footer")


def _text(markup: str) -> str:
    markup = _INLINE_XML.sub("", _INLINE_TOUCHING.sub(" ", markup))
    return _norm(_html.unescape(_TAG.sub(" ", markup)))


def _block_text(el) -> str:
    """Visible text with breaks at block boundaries only; `el` itself is not modified."""
    el = copy.copy(el)
    for tag in el.find_all(_BLOCK):
        tag.insert_before("\n")
        tag.insert_after("\n")
    return el.get_text("")


def _inline_text(markup: str) -> str:
    """For titles: the h1 wraps "SOR" in <abbr>, so tags must vanish, not become spaces."""
    return _norm(_html.unescape(_TAG.sub("", markup)))


def _strip_volatile(xml: str) -> str:
    m = _ROOT.search(xml)
    root = m.group(0)
    for a in _VOLATILE_ATTRS:
        root = re.sub(r'\s*\b%s="[^"]*"' % re.escape(a), "", root)
    xml = xml[:m.start()] + root + xml[m.end():]
    return _CONSOLIDATION_DATE.sub("", xml)


class FintracCrawler:
    """One FINTRAC tab. `init_kwargs` beyond the common ones belong to that tab."""

    def __init__(self, regulator: str, source_system: str, tab: str,
                 category: Optional[str] = None, delay: float = 1.0, **params):
        if tab not in self._TABS:
            raise ValueError("FINTRAC: unknown tab %r, expected one of %s"
                             % (tab, sorted(self._TABS)))
        self.regulator = regulator
        self.source_system = source_system
        self.tab = tab
        self.category = category or source_system
        self.delay = float(delay)
        self.params = params
        self._s = requests.Session()
        self._s.headers["User-Agent"] = USER_AGENT

    def _get(self, url: str) -> str:
        last = None
        for attempt in range(3):
            try:
                r = self._s.get(url, timeout=60)
                if r.status_code == 200:
                    time.sleep(self.delay)
                    return r.content.decode("utf-8")
                raise RuntimeError("%s returned %d" % (url, r.status_code))
            except requests.RequestException as e:
                last = e
                time.sleep(2.0 * (attempt + 1))
        raise RuntimeError("could not fetch %s: %s" % (url, last))

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        docs = stamp_content_hashes(self._TABS[self.tab](self))      # single exit
        logger.info("FINTRAC %s: %d documents", self.tab, len(docs))
        return docs[:limit] if isinstance(limit, int) and limit > 0 else docs

    # ------------------------------------------------------------------ regulations
    def _regulation(self) -> List[RegulatoryDocument]:
        p = self.params
        code = p["reg_code"]                                   # e.g. SOR-2002-184
        page_url = "%s/eng/regulations/%s/" % (JUSTICE, code)
        xml_url = "%s/eng/XML/%s.xml" % (JUSTICE, code)

        page = self._get(page_url)
        h1 = _H1.search(page)
        title = _inline_text(h1.group(1)) if h1 else ""
        if title != p["expected_title"]:
            raise RuntimeError("FINTRAC %s: page title %r, expected %r"
                               % (code, title, p["expected_title"]))
        href = _XML_HREF.search(page)
        if not href or JUSTICE + href.group(1) != xml_url:
            raise RuntimeError("FINTRAC %s: page does not offer %s" % (code, xml_url))
        pdf = _PDF_HREF.search(page)

        xml = self._get(xml_url)
        if not _ROOT.search(xml):
            raise RuntimeError("FINTRAC %s: no <Regulation> root" % code)
        inst = _INSTRUMENT.search(xml)
        if not inst or inst.group(1).strip().replace("/", "-") != code:
            raise RuntimeError("FINTRAC %s: XML is instrument %r"
                               % (code, inst and inst.group(1)))

        root = _ROOT.search(xml).group(0)
        attr = lambda n: (_ATTR(n).search(root) or [None, ""])[1]
        basis = _strip_volatile(xml)
        content_text = _ILLEGAL_XLSX.sub(" ", _text(basis))
        if len(content_text) < int(p["min_text_chars"]):
            raise RuntimeError("FINTRAC %s: %d chars, below floor %s"
                               % (code, len(content_text), p["min_text_chars"]))

        meta = {
            "reg_code": code,
            "lims_fid": attr("lims:fid"),
            "last_amended_date": attr("lims:lastAmendedDate"),
            "inforce_start_date": attr("lims:inforce-start-date"),
            "consolidation_current_to": attr("lims:current-date"),   # provenance only
            "content_text": content_text,
            "text_chars": len(content_text),
            "xml_url": xml_url,
            "pdf_url": JUSTICE + pdf.group(1) if pdf else "",
            "landing_page": page_url,
            "previous_versions_url": page_url + "PITIndex.html",
            "content_hash_basis": "xml-minus-consolidation-dates",
        }
        doc = RegulatoryDocument(
            regulator=self.regulator, source_system=self.source_system,
            category=self.category, title=title, document_url=xml_url,
            doc_path=[self.regulator, self.source_system, p["reg_folder"], title],
            file_type="XML", source_page_url=page_url,
            published_date=meta["last_amended_date"] or None,
            reference_no=inst.group(1).strip(), document_html=None, extra_meta=meta,
        )
        doc.content_hash = content_key(_norm(basis))
        return [doc]

    # ------------------------------------------------------------------- guidelines
    def _guidelines(self) -> List[RegulatoryDocument]:
        p = self.params
        hub_url = p["hub_url"]
        wanted = [_norm(s) for s in p["sections"]]
        skip = {urljoin(FINTRAC, u) for u in p.get("skip_urls", [])}

        main = BeautifulSoup(self._get(hub_url), "html.parser").find("main")
        found, links = set(), []                     # (section, url), first section wins
        for h2 in main.find_all("h2"):
            section = _norm(h2.get_text(" "))
            if section not in wanted:
                continue
            found.add(section)
            for el in h2.find_all_next():
                if el.name == "h2":
                    break
                if el.name == "a" and el.get("href"):
                    url = urldefrag(urljoin(hub_url, el["href"]))[0]
                    if url.startswith(FINTRAC) and url not in skip \
                            and url not in {u for _, u in links}:
                        links.append((section, url))
        missing = set(wanted) - found
        if missing:
            raise RuntimeError("FINTRAC hub: sections not found %s" % sorted(missing))
        if len(links) < int(p["min_documents"]):
            raise RuntimeError("FINTRAC hub: %d guidance links, below floor %s"
                               % (len(links), p["min_documents"]))

        docs = []
        for section, url in links:
            m = BeautifulSoup(self._get(url), "html.parser").find("main")
            if m is None:
                raise RuntimeError("FINTRAC %s: no <main>" % url)
            dm = m.select_one("dl#wb-dtmd time") or m.select_one("dl#wb-dtmd dd")
            date_modified = _norm(dm.get_text()) if dm else ""
            for sel in _GUIDANCE_STRIP:
                for el in m.select(sel):
                    el.decompose()
            # Flowcharts and infographics carry content the text does not; record
            # them so the gap is visible. Pipe-joined string, never a list.
            images = list(dict.fromkeys(
                urljoin(url, i["src"]) for i in m.find_all("img") if i.get("src")))
            h1 = m.find("h1")
            title = _norm(h1.get_text()) if h1 else ""
            content_text = _ILLEGAL_XLSX.sub(" ", _norm(_block_text(m)))
            if not title or len(content_text) < int(p["min_text_chars"]):
                raise RuntimeError("FINTRAC %s: title %r, %d chars"
                                   % (url, title, len(content_text)))
            doc = RegulatoryDocument(
                regulator=self.regulator, source_system=self.source_system,
                category=self.category, title=title, document_url=url,
                doc_path=[self.regulator, self.source_system, title],
                file_type="HTML", source_page_url=url,
                published_date=date_modified or None, reference_no=None,
                document_html=str(m),
                extra_meta={
                    "hub_section": section,
                    "hub_url": hub_url,
                    "date_modified": date_modified,        # provenance only, lags edits
                    "image_urls": "|".join(images),
                    "image_count": len(images),
                    "content_text": content_text,
                    "text_chars": len(content_text),
                    "content_hash_basis": "main-text-minus-glossary-and-date",
                },
            )
            doc.content_hash = content_key(content_text)
            docs.append(doc)
        return docs

    _TABS = {"regulations": _regulation, "guidelines": _guidelines}
