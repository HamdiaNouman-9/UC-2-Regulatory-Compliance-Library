"""BahrainBourseLegalFrameworkCrawler — Bahrain Bourse's Legal Framework page,
from the site's own listing API.

WHY THIS IS NOT A BROWSER CRAWL
--------------------------------
The page (https://bahrainbourse.com/EN/RULES%20AND%20REGULATIONS/LEGALFRAMEWORK)
renders an accordion with seven sections — Laws, Rules & Regulations,
Resolutions, Guidelines, Circulars, CBB Rules & Regulations, Consultation — and
NONE of it is in the server HTML. MEASURED 2026-08-20: a plain `curl` fetch of
the page contains zero occurrences of any section name; a Playwright render
(`python -m dynamic_crawler.formfill inspect`) shows 0 raw links vs 409
rendered, confirming the whole thing is built client-side.

The site is on-prem SharePoint 2019 (`_spPageContextInfo`, `isSPO: false`), but
the accordion is NOT a SharePoint list view web part — it is a single custom
call:

    GetFAQ('/en/Rules%20and%20Regulations/LegalFramework/Lists/FAQ',
           'faqlist1', 'No Record Found')

which hits this site's own middleware API, not raw SharePoint REST:

    GET https://webapi.bahrainbourse.com/api/data/GetFaq
        ?listUrl=<double-encoded list path>&websiteID=bhb
    Authorization: Bearer <APIKEY, embedded in the page's HTML>

    -> {"status": 1, "data": [{"title": "Laws", "abstractText": "<ul>...</ul>"},
                               {"title": "Circulars", "abstractText": "<table>...</table>"},
                               ...]}   7 items, ~20 KB, one request, no browser

THE API KEY IS NOT STATIC — FETCH IT EVERY RUN
-------------------------------------------------
MEASURED 2026-08-20: a key captured at 10:14 got HTTP 401 at 10:44, thirty
minutes later; the SAME page fetched fresh at that moment carried a DIFFERENT
`var APIKey = '...'` value, and that new value worked immediately. The key is
short-lived (session- or time-scoped), not a fixed public constant. This
crawler therefore fetches `LISTING_PAGE` first on every run and pulls the
current key out of the HTML with `_APIKEY_RE` — never hardcode a captured
value here, it will work today and 401 tomorrow.

Each "FAQ item" IS one accordion section: `title` is the section name and
`abstractText` is the raw HTML for that section's document list (a `<ul>` of
links for most sections, a `<table>` for Circulars, which also carries a
circular reference number and an issue date per row). This crawler parses that
HTML rather than re-rendering it.

THE DOUBLE-ENCODING TRAP
-------------------------
The page's inline script passes the list path ALREADY percent-encoded
(`...LegalFramework%2FLists%2FFAQ` has `%20` in it for the spaces), and
jQuery's `$.ajax` then encodes that string again for the querystring (the
literal `%` becomes `%25`). Sending the list path only single-encoded gets back
`{"status": 1, "data": []}` — HTTP 200, valid JSON, wrong (empty) answer.
MEASURED 2026-08-20. `LIST_URL_PARAM` below is the exact double-encoded
querystring value that reproduces what the browser actually sends; do not
"simplify" it back to a single `urlencode` call.

CLOSED ITEMS HAVE NO LINK
---------------------------
A handful of list items are plain text with no `<a>` — "DVP Consultation Paper
(Closed)", "BHB Issuers Violations (Closed)", etc. There is no document behind
them (the consultation closed with nothing further published, or the outcome
is filed elsewhere in the same list). They are skipped, not treated as a
parse failure.

doc_path PUTS THE SECTION NAME IN THE FOLDER TREE
----------------------------------------------------
`doc_path = [regulator, source_system, section_title, title]`, so the library
shows Laws / Rules & Regulations / Resolutions / Guidelines / Circulars /
CBB Rules & Regulations / Consultation as their own folders under Legal
Framework, matching the site's own accordion. `config/sources/cbe.yml` keeps
its API category OUT of doc_path for the opposite reason — a circular's
category there is genuinely fluid — but this library's own request is for the
accordion's structure, so the trade is accepted here: if an item ever migrates
between sections (several Circulars rows are literally titled "Consultation
Paper: ...", suggesting a later move to Guidelines once finalised), the
`disappeared`/`new` pair it produces is treated as a real, visible re-filing
event rather than something to hide. The section name is ALSO carried in
`category` and `extra_meta.bhb_section`, so nothing is lost either way.

ZERO-WIDTH SPACES
-------------------
This CMS's rich-text fields are full of `​` (zero-width space) — visible
in the raw HTML as runs of `​​​​​​​​` before nearly every link. Titles and dates
are cleaned of it before use; an uncleaned title would silently mismatch on
re-crawl (same visible text, different code points) and read as `modified`
forever.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from dynamic_crawler.formfill.runner import _ext_type, _is_doc
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://bahrainbourse.com"
LISTING_PAGE = f"{BASE}/EN/RULES%20AND%20REGULATIONS/LEGALFRAMEWORK"
API = "https://webapi.bahrainbourse.com/api/data/GetFaq"

LIST_URL_PARAM = "%2Fen%2FRules%2520and%2520Regulations%2FLegalFramework%2FLists%2FFAQ"

# Embedded in every page of this site (var APIKey = '...' in the page HTML) —
# not a secret held back from anonymous visitors, this is how the site's own
# public pages call their own API. SHORT-LIVED — see "THE API KEY IS NOT
# STATIC" above. Extracted fresh from LISTING_PAGE on every run; never
# hardcode a captured value.
_APIKEY_RE = re.compile(r"var\s+APIKey\s*=\s*'([^']+)'")

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

#: Refuse to believe an implausibly empty answer. MEASURED 2026-08-20: 7
#: sections, ~90 documents total.
MIN_EXPECTED_SECTIONS = 5
MIN_EXPECTED_DOCS = 20

_ZERO_WIDTH_RE = re.compile(r"[​‌‍﻿]")
_WS_RE = re.compile(r"\s+")
_ORDINAL_GLUE_RE = re.compile(r"(\d(?:st|nd|rd|th))([A-Za-z])", re.IGNORECASE)
_ORDINAL_STRIP_RE = re.compile(r"(\d+)(st|nd|rd|th)\b", re.IGNORECASE)


def _clean_text(s: Optional[str]) -> str:
    return _WS_RE.sub(" ", _ZERO_WIDTH_RE.sub("", s or "")).strip()


def _parse_issue_date(text: str) -> Optional[str]:
    """'31st December 2018' / '2ndSeptember 2020' -> '2018-12-31'. Best effort;
    returns None (never raises) on anything unparseable."""
    t = _clean_text(text)
    if not t:
        return None
    t = _ORDINAL_GLUE_RE.sub(r"\1 \2", t)
    t = _ORDINAL_STRIP_RE.sub(r"\1", t)
    try:
        from dateutil import parser as dtparser
        return dtparser.parse(t, dayfirst=False).date().isoformat()
    except Exception:
        return None


def _parse_generic_section(html: str) -> List[Dict[str, str]]:
    """Sections that render as <ul><li>...<a href>title</a>...</li></ul>."""
    soup = BeautifulSoup(html or "", "html.parser")
    items = []
    for li in soup.find_all("li"):
        a = li.find("a", href=True)
        if not a:
            continue  # a "(Closed)" placeholder with no document
        href = (a["href"] or "").strip()
        title = _clean_text(a.get_text())
        if href and title:
            items.append({"title": title, "href": href})
    return items


def _parse_circulars_section(html: str) -> List[Dict[str, str]]:
    """Circulars renders as a 3-column <table>: ref, date of issue, subject."""
    soup = BeautifulSoup(html or "", "html.parser")
    items = []
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        a = tds[2].find("a", href=True)
        if not a:
            continue  # the header row, or a circular with no linked document
        href = (a["href"] or "").strip()
        title = _clean_text(a.get_text())
        if not (href and title):
            continue
        items.append({
            "title": title,
            "href": href,
            "reference_no": _clean_text(tds[0].get_text()),
            "date_text": _clean_text(tds[1].get_text()),
        })
    return items


class BahrainBourseLegalFrameworkCrawler:
    """Bahrain Bourse (BHB) — Legal Framework: Laws, Rules & Regulations,
    Resolutions, Guidelines, Circulars, CBB Rules & Regulations, Consultation."""

    def __init__(
        self,
        regulator: str = "Bahrain Bourse (BHB)",
        source_system: str = "Legal Framework",
        timeout: int = 30,
    ):
        # "Full Name (ACRONYM)" is the library's naming rule. Config lookups
        # match on this string and fall back to defaults SILENTLY when it does
        # not match, so a near-miss is not an error, it is a wrong answer.
        self.regulator = regulator
        self.source_system = source_system
        self.timeout = timeout
        self.last_result: dict = {}

    # ------------------------------------------------------------------ #
    #  the API                                                            #
    # ------------------------------------------------------------------ #

    def _fetch_api_key(self, session: requests.Session) -> str:
        """The listing page first — for cookies, an honest Referer, and the
        CURRENT `APIKey` (see "THE API KEY IS NOT STATIC" in the module
        docstring). One request either way, so this costs nothing extra."""
        resp = session.get(LISTING_PAGE, timeout=self.timeout)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Bahrain Bourse Legal Framework page returned "
                f"{resp.status_code}; cannot read the current API key.")
        m = _APIKEY_RE.search(resp.text)
        if not m:
            raise RuntimeError(
                "Bahrain Bourse Legal Framework page no longer embeds "
                "`var APIKey = '...'` — the page changed; the GetFaq call "
                "cannot be authorised without it.")
        return m.group(1)

    def _fetch_sections(self, session: requests.Session) -> List[dict]:
        api_key = self._fetch_api_key(session)
        url = f"{API}?listUrl={LIST_URL_PARAM}&websiteID=bhb"
        resp = session.get(
            url,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Authorization": f"Bearer {api_key}",
                "Referer": LISTING_PAGE,
            },
            timeout=self.timeout,
        )
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if resp.status_code != 200 or "json" not in ctype:
            snippet = resp.text[:160].replace("\n", " ")
            raise RuntimeError(
                f"Bahrain Bourse Legal Framework API did not return JSON "
                f"(status {resp.status_code}, content-type {ctype!r}). "
                f"Body: {snippet}")
        payload = resp.json() or {}
        if payload.get("status") != 1 or "data" not in payload:
            raise RuntimeError(
                f"Bahrain Bourse Legal Framework API returned an unexpected "
                f"shape (keys: {sorted(payload)[:8]}). The endpoint changed; "
                f"do not treat this as an empty page.")
        sections = payload["data"] or []
        if len(sections) < MIN_EXPECTED_SECTIONS:
            raise RuntimeError(
                f"Bahrain Bourse Legal Framework API returned only "
                f"{len(sections)} section(s) (expected >= "
                f"{MIN_EXPECTED_SECTIONS}). Refusing a partial read.")
        return sections

    # ------------------------------------------------------------------ #
    #  the contract: docs = crawler.fetch_documents()                     #
    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []

        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

        sections = self._fetch_sections(session)
        logger.info("Bahrain Bourse Legal Framework API returned %d section(s): %s",
                    len(sections), [s.get("title") for s in sections])

        docs: List[RegulatoryDocument] = []
        seen = set()
        skipped_no_link = 0
        by_section: Dict[str, int] = {}

        for section in sections:
            section_title = _clean_text(section.get("title"))
            html = section.get("abstractText") or ""
            if not section_title:
                continue

            if section_title.lower() == "circulars":
                items = _parse_circulars_section(html)
            else:
                items = _parse_generic_section(html)

            raw_li_count = html.count("<li") + html.count("<tr")
            skipped_no_link += max(0, raw_li_count - len(items))

            for it in items:
                url = urljoin(BASE, it["href"])
                title = it["title"]
                key = (section_title, url, title)
                if key in seen:
                    continue
                seen.add(key)

                ref = it.get("reference_no") or None
                date_text = it.get("date_text") or ""
                published = _parse_issue_date(date_text) if date_text else None
                if date_text and published is None:
                    warnings.append(
                        f"could not parse issue date {date_text!r} for "
                        f"{title!r}")

                docs.append(RegulatoryDocument(
                    regulator=self.regulator,
                    source_system=self.source_system,
                    category=section_title,
                    title=title,
                    document_url=url,
                    # Section name IN the path — see "doc_path PUTS THE
                    # SECTION NAME IN THE FOLDER TREE" in the module docstring.
                    doc_path=[self.regulator, self.source_system, section_title, title],
                    file_type=_ext_type(url) if _is_doc(url) else "HTML",
                    published_date=published,
                    reference_no=ref,
                    source_page_url=LISTING_PAGE,
                    content_hash=content_key(
                        f"{section_title}|{ref or ''}|{date_text}|{url}|{title}"),
                    extra_meta={
                        "crawl_source": self.source_system,
                        "bhb_section": section_title,
                        "bhb_date_text": date_text or None,
                    },
                ))
                by_section[section_title] = by_section.get(section_title, 0) + 1
                if cap and len(docs) >= cap:
                    break
            if cap and len(docs) >= cap:
                break

        if skipped_no_link:
            warnings.append(
                f"{skipped_no_link} list item(s)/row(s) had no linked document "
                f"(closed consultations, header rows) — skipped")

        if len(docs) < MIN_EXPECTED_DOCS:
            raise RuntimeError(
                f"Bahrain Bourse Legal Framework crawl produced only "
                f"{len(docs)} document(s) (expected >= {MIN_EXPECTED_DOCS}). "
                f"Refusing a partial inventory.")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.source_system: len(docs)},
            "by_section": by_section,
        }
        logger.info("BahrainBourseLegalFrameworkCrawler finished: %d document(s) "
                    "across %d section(s)", len(docs), len(by_section))

        # The single exit. Every hash above is already set and stamp_ never
        # overwrites one; this is the backstop for a branch added later that
        # forgets to hash.
        return stamp_content_hashes(docs)
