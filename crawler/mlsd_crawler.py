"""MLSDCrawler — Bahrain's social-development legislation list, read with `requests`.

One page, one flat list, thirty links. Almost every link is an Arabic PDF behind
`download.php?main=1&id=<32 hex>`, so the crawler downloads each file and stores
its extracted text: the orchestrator writes the version row BEFORE it extracts
anything (`orch.py:869`), so text it extracts itself never reaches a workbook.

ENGLISH TITLES, ARABIC READ ALONGSIDE. The ministry renders this listing in
Arabic by default and in English under `?lang=en`. English titles the row — the
business decision, 2026-08-20 — and the Arabic reading is kept beside it as
`extra_meta["title_ar"]`.

THE ARABIC READ IS NOT DECORATION, it is the disambiguator. The English
translation is LOSSY: it drops "amending some provisions of", so two distinct
instruments collapse onto one English title and become indistinguishable in the
workbook by name alone. In Arabic they are distinct, and correctly so —

    قرار رقم (11) لسنة 2023 ...                       the resolution
    قرار بتعديل بعض أحكام القرار رقم (11) لسنة 2023 ...  the one AMENDING it

Measured 2026-08-20: 30 rows, 30 distinct urls, 29 distinct English titles, 30
distinct Arabic ones. Identity still separates the pair on document_url, so
nothing collides; what `title_ar` buys is a person being able to tell them apart.

ORDER IS LOAD-BEARING. `?lang=en` sets the language in the PHP SESSION, not the
url, so a session that has once asked for English keeps answering in English even
for the bare url. Arabic first, English second, is the only order that gets both
from one session. The two readings are paired on `document_url`, NEVER on
position — position is the site's ordering, and ordering is not identity.
"""

from __future__ import annotations

import hashlib
import logging
import os
import pathlib
import re
import tempfile
import time
import unicodedata
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from models.models import RegulatoryDocument
from crawler.fingerprint import stamp_content_hashes
from generic_crawler.crawler import content_key

logger = logging.getLogger(__name__)

BASE = "https://www.social.gov.bh"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# The list itself. `#reading_area` is the ReadSpeaker region the ministry marks
# up as the readable page, and every legislation row is one `li.mer-list` inside
# it — 30 of them, measured 2026-08-20. Scoped to that region so the site's own
# navigation, which is also `<li><a>`, cannot enter the crawl.
LIST_SELECTOR = "#reading_area li.mer-list"

# Ordinary Arabic. Presentation forms are absent here because NFKC has already
# folded them by the time this runs — see `_pdf_text`.
_ARABIC = re.compile(r"[؀-ۿ]")

# Any letter in any script, for the Arabic ratio recorded on every row. `\w`
# would count digits and underscores, which a page number would then dilute.
_LETTER = re.compile(r"[^\W\d_]", re.UNICODE)

# GLYPHS THE PDF COULD NOT SPELL. U+FFFD is what an extractor emits when a font
# gives it a glyph id it cannot turn into a character; the C1 block is the other
# residue of the same fault. Counted per row because it is invisible otherwise —
# the text reads as ordinary Arabic with the occasional wrong letter.
_UNDECODABLE = re.compile(r"[�\x7f-\x9f]")

# openpyxl raises on these rather than escaping them, and it raises inside
# save() — after the whole crawl has run. Tab, newline and carriage return are
# legal and are left alone.
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# Below this a "page" is a cookie notice or an error template, not an
# instrument. Only applied to the HTML-bodied entries; a PDF is judged on
# whether any Arabic survived extraction instead.
_MIN_PAGE_CHARS = 400


#: Retries for a fetch that could not be ANSWERED. A GET changes nothing on the
#: server, so asking again is safe; the backoff is what keeps it polite. Two
#: hosts in this library are already blocked for automated access and neither
#: block was about volume, so this stays small on purpose: 3 attempts, 1s then
#: 2s, a worst case of 3 extra seconds on a document that is failing anyway.
#:
#: Without this, one transient error stores a row with no text and the
#: `document_url|title` fallback fingerprint; the next run succeeds and the row
#: reads as `modified`, costing two version rows for a document nobody touched.
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.0

#: Statuses that are an ANSWER rather than a failure to answer. Retrying these
#: would just repeat a question the server has already settled. The dead
#: `www.mlsd.gov.bh` host raises instead of answering, and NXDOMAIN is not
#: transient — but it costs 2 extra seconds a week, which is not worth a special
#: case that would then need its own test.
_FINAL_STATUSES = (200, 401, 403, 404, 410)


def _fetch_retrying(session: requests.Session, url: str, timeout: int,
                    delay: float, attempts: int = RETRY_ATTEMPTS):
    """One request, retrying only what could not be answered. Returns the
    response, or None when every attempt failed to get one."""
    wait = RETRY_BACKOFF
    for attempt in range(1, max(attempts, 1) + 1):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code in _FINAL_STATUSES:
                if delay:
                    time.sleep(delay)
                return r
            logger.warning("MLSD %s -> HTTP %s (attempt %d/%d)",
                           url[-60:], r.status_code, attempt, attempts)
        except requests.RequestException as e:
            logger.warning("MLSD %s -> %s (attempt %d/%d)",
                           url[-60:], type(e).__name__, attempt, attempts)
        if attempt < attempts:
            time.sleep(wait)
            wait *= 2
    return None


def _cache_file(cache_dir: Optional[str], url: str, suffix: str) -> Optional[pathlib.Path]:
    if not cache_dir:
        return None
    d = pathlib.Path(cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    return d / (hashlib.sha256(url.encode()).hexdigest()[:16] + suffix)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _norm(s: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", s or "").split())


def _clean(text: str) -> str:
    """NFKC first, sanitise second, so the text that is hashed is the text that
    reaches the workbook."""
    text = unicodedata.normalize("NFKC", text or "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return _ILLEGAL_XLSX.sub(" ", text).strip()


class MLSDCrawler:
    """The ministry's legislation list. `listing_url` and `listing_title` come
    from the YAML so a site retitle cannot silently reshape `doc_path`."""

    def __init__(
        self,
        regulator: str,
        source_system: str,
        listing_url: str,
        listing_title: str,
        category: Optional[str] = None,
        timeout: int = 60,
        delay: float = 1.0,
        page_cache_dir: Optional[str] = None,
        pdf_cache_dir: Optional[str] = None,
    ):
        self.regulator = regulator
        self.source_system = source_system
        self.listing_url = listing_url
        self.listing_title = listing_title
        self.category = category or listing_title
        self.timeout = timeout
        # Politeness, not throughput. Two hosts in this library are blocked for
        # automated access from this address, and neither block was about volume.
        self.delay = float(delay)
        # Dev-only read-through caches, also read from the environment. Kept out
        # of the yml: a cache serving production is a stale law reporting
        # `unchanged` after an amendment.
        self.page_cache_dir = page_cache_dir or os.environ.get("MLSD_PAGE_CACHE_DIR") or None
        self.pdf_cache_dir = pdf_cache_dir or os.environ.get("MLSD_PDF_CACHE_DIR") or None
        self.last_result: dict = {}
        self._session: Optional[requests.Session] = None

    @property
    def source_names(self) -> List[str]:
        return [self.listing_title]

    def _http(self) -> requests.Session:
        if self._session is None:
            self._session = _session()
        return self._session

    # ------------------------------------------------------------------ #
    #  the listing                                                       #
    # ------------------------------------------------------------------ #

    def _listing(self, url: str) -> Tuple[str, str]:
        cp = _cache_file(self.page_cache_dir, url, ".html")
        if cp is not None and cp.exists():
            return cp.read_text("utf-8", "replace"), "page-cache"
        r = _fetch_retrying(self._http(), url, self.timeout, self.delay)
        if r is None or r.status_code != 200:
            raise RuntimeError(
                f"MLSD listing {url} returned "
                f"{'no response after %d attempts' % RETRY_ATTEMPTS if r is None else 'HTTP %s' % r.status_code}"
                f". Nothing may be classified from this — an empty listing is "
                f"ruled `disappeared` downstream and proposes law in force for "
                f"withdrawal.")
        if cp is not None:
            cp.write_text(r.text, encoding="utf-8")
        return r.text, "live"

    def _rows(self, html: str) -> Tuple[List[Tuple[str, str]], int]:
        """(document_url, title) for every row of the list, and how many rows
        repeated a url. The count is returned rather than logged here so the
        CALLER can put it in `last_result` warnings, where a reviewer sees it."""
        soup = BeautifulSoup(html, "html.parser")
        out, seen, repeats = [], set(), 0
        for li in soup.select(LIST_SELECTOR):
            a = li.select_one("a[href]")
            if a is None:
                continue
            href = (a.get("href") or "").strip()
            if not href:
                continue
            url = urljoin(self.listing_url, href)
            if url in seen:
                # A repeated url is one row in the library, not two — but the
                # ministry listing a document twice is worth hearing about, so it
                # is COUNTED rather than silently dropped.
                repeats += 1
                continue
            seen.add(url)
            # The row's own text, not the anchor's: the ministry puts the title
            # inside the anchor today, but a row that ever grows a date or a
            # badge beside it should still title itself from the whole row.
            out.append((url, _norm(li.get_text(" ", strip=True))))
        return out, repeats

    # ------------------------------------------------------------------ #
    #  one document's text                                               #
    # ------------------------------------------------------------------ #

    def _body(self, url: str) -> Tuple[bytes, str, str]:
        """(bytes, content_type, origin) for one document, or (b"", "", why)."""
        cp = _cache_file(self.pdf_cache_dir, url, ".bin")
        if cp is not None and cp.exists():
            ct = "application/pdf" if cp.read_bytes()[:5] == b"%PDF-" else "text/html"
            return cp.read_bytes(), ct, "pdf-cache"
        # `www.mlsd.gov.bh`, the ministry's previous domain, no longer resolves
        # at all — one row still points there. A dead host is a finding about the
        # SITE, so it is recorded and the crawl continues; raising here would
        # lose the other 29 rows over it.
        r = _fetch_retrying(self._http(), url, self.timeout, self.delay)
        if r is None:
            return b"", "", "unreachable"
        if r.status_code != 200:
            logger.warning("MLSD %s -> HTTP %s", url[:90], r.status_code)
            return b"", "", f"http-{r.status_code}"
        body = r.content
        ctype = (r.headers.get("Content-Type") or "").lower()
        if cp is not None:
            cp.write_bytes(body)
        return body, ctype, "live"

    def _pdf_text(self, body: bytes, url: str) -> Tuple[str, str, dict]:
        """Extracted text for one PDF, and how much of it the extractor read.

        Uses the orchestrator's own extractor so the text stored here is the same
        text it would have produced (`orchestrator.py:273`), not a second opinion.
        """
        tmp = pathlib.Path(tempfile.mkstemp(suffix=".pdf")[1])
        try:
            tmp.write_bytes(body)
            try:
                # Heavy import (cv2, pdfplumber, pytesseract) — kept off module
                # load so importing this crawler stays cheap for anything that
                # only parses.
                from processor.Text_Extractor import OCRProcessor
                text, meta = OCRProcessor.extract_text_from_pdf_smart(pdf_path=str(tmp))
                # NOT `is_ocr_available()`. That is true when ANY traineddata is
                # present, so a deployment with only `eng` reports OCR as working
                # and then OCRs Arabic pages with an English model. MLSD
                # publishes only in Arabic, so the question that matters is
                # whether `ara` is installed.
                #
                # RECORDED PER ROW RATHER THAN ASSUMED, because the answer is a
                # property of the environment the crawl was launched in, not of
                # the document. Tesseract's language search path is deployment
                # setup — see `.env` — and the same PDF read with and without
                # `ara` returns text of the same ORDER OF LENGTH either way, so
                # the row itself is the only place the difference is visible.
                ocr_langs = OCRProcessor.ocr_langs()
                ocr_ok = "ara" in ocr_langs.split("+")
            except Exception as e:
                logger.warning("MLSD text extraction failed %s: %s", url[:80], e)
                return "", "extract-failed", {}
        finally:
            try:
                tmp.unlink()
            except OSError:
                pass

        meta = meta or {}
        total = int(meta.get("total_pages") or 0)
        good = int(meta.get("good_pages") or 0)
        ocred = int(meta.get("ocr_pages") or 0)
        # A page routed to OCR contributes NOTHING when the engine is absent, but
        # `extract_text_from_pdf_smart` still counts it in `good_pages` and logs
        # it as "OK (0 chars)". Subtract those to get the pages actually read.
        usable = good - (0 if ocr_ok else ocred)
        info = {"pages": total, "ocr_pages": ocred, "ocr_available": ocr_ok,
                "ocr_langs": ocr_langs, "pages_read": max(usable, 0)}

        # NFKC first: five of these PDFs extract as Arabic PRESENTATION FORMS
        # (U+FB50-FEFF) rather than ordinary Arabic (U+0600-06FF) — the letters
        # are right and in logical order, but they are a different string, so
        # they match neither their own title nor an Arabic search. NFKC maps them
        # back. One document carried 3,926 such characters and now carries none.
        text = _clean(text)

        # DISCARD ON "NO ARABIC", not on the page arithmetic above. MLSD
        # publishes only in Arabic, so text with no Arabic character in it is not
        # the document, whatever the page counts say.
        #
        # WITH `ara` PRESENT NOTHING REACHES THIS LINE — all 28 reachable PDFs
        # extract 91-102% Arabic. It is the guard for the other case, and that
        # case is real and silent: with `ocr_langs` at 'eng', nine of those same
        # 28 return between 808 and 15,310 characters of Latin transliteration
        # noise at exactly ZERO Arabic. Nothing about the length says which
        # happened, so length cannot be the test. Zero is the whole threshold and
        # there is nothing to tune.
        if text and not _ARABIC.search(text):
            logger.warning("MLSD %s: discarding %d chars with no Arabic in them "
                           "— %d/%d pages needed OCR and OCR is %s",
                           url[-40:], len(text), ocred, total,
                           "available" if ocr_ok else "NOT installed for 'ara'")
            return "", "unreadable-text-layer", info

        if usable <= 0:
            return "", "no-pages-read", info

        # HOW MUCH OF THIS TEXT IS ACTUALLY ARABIC, recorded on the row rather
        # than left for a reader to notice. It is the cheapest way to tell a read
        # document from a mis-OCR'd one, and the two populations do not overlap.
        #
        # Measured across the 28 reachable PDFs on 2026-08-20, the same files
        # both ways:
        #
        #   ocr_langs 'ara+eng'   ALL 28 land between 91.2% and 102.4%. Eleven
        #                         have an embedded text layer and need no OCR;
        #                         the other seventeen OCR between 1 and 25 pages
        #                         and still land in that band.
        #   ocr_langs 'eng'       NINE fall to 0.0% and four more to 9.5%, 17.7%,
        #                         30.2% and 39.3% — real Arabic from the pages
        #                         that had a text layer, concatenated with Latin
        #                         noise from the pages that did not.
        #
        # (Ratios slightly over 100% are not a bug: `_LETTER` excludes the Arabic
        # combining marks that `_ARABIC` counts.)
        #
        # NO RATIO THRESHOLD IS APPLIED, deliberately. With `ara` present nothing
        # comes near a cut; without it the zero rule above already catches the
        # nine, and a cut placed to catch the other four would be a number chosen
        # from thirty samples that mis-files the first genuinely bilingual
        # instrument to arrive. The number is stored instead, beside `ocr_langs`,
        # so a low row and the reason for it are both visible in the workbook.
        letters = len(_LETTER.findall(text))
        info["arabic_ratio"] = round(len(_ARABIC.findall(text)) / letters, 3) if letters else 0.0

        # HOW MANY CHARACTERS THIS PDF COULD NOT SPELL, recorded rather than
        # repaired. Measured 2026-08-20: NINE of the 28 reachable PDFs carry
        # between 6 and 218 of these, 0.45%-0.91% of their characters. The cause
        # is upstream and not ours to fix — the fonts are subset-embedded Type0
        # with `Identity-H` encoding, where a /ToUnicode CMap is the only route
        # back to characters, and these subsets ship an incomplete one.
        #
        # IT IS NOT REPAIRED HERE, AND OCR IS NOT THE ANSWER. Measured on the
        # affected pages: OCR with `ara` removes every one of these (0 left) but
        # mangles the instrument's own NUMBER — "39 of 2024" comes back as
        # "(v4) of ٠١"" — while the native text keeps the numbers exactly right
        # and loses only the odd letter inside a word. For legal text that trade
        # runs the wrong way. pdfplumber and PyMuPDF's ligature flags are worse
        # again, at 44-81 per page against native's 17-18.
        #
        # So the number is stored, and a reviewer working the `status = ''` queue
        # can see which documents to read against the original PDF.
        info["undecodable_chars"] = len(_UNDECODABLE.findall(text))
        return text, "pdf", info

    def _html_text(self, body: bytes, url: str) -> Tuple[str, str, str]:
        """(text, html, origin) for a row that answers with a web page.

        A RECOGNISED CONTAINER OR NOTHING. There is no fall-back to `<body>`,
        deliberately — that is the line between storing a document and storing a
        website.

        One row on this list points off-site, at a Prime Minister's Office
        article. `#reading_area` is the ministry's own content region and does
        not exist there, so a `body` fall-back captured that site's chrome
        instead: the stored "text" of a resolution opened
        "ابحث هنا... Search... Skip to Content... الأخبار" and the page carries a
        literal `#cookie-banner`. `check` passed it, because `check` tests shape
        and this was the wrong text in the right shape.

        Two things go wrong if that is stored. It fingerprints the OTHER site's
        navigation, so a PMO redesign reports a Bahraini resolution as amended;
        and it presents a menu to the reviewer and the analyser as the words of
        an instrument. Storing nothing is recoverable — the row is still there,
        with its url, for a person to open. Storing chrome is not, because
        nothing downstream can tell it from text.

        The recognised containers are named rather than guessed, and no external
        site's CSS classes are hardcoded here: this crawler knows social.gov.bh's
        own region and the two standard semantic elements, and anything else is
        somebody else's markup that would drift silently the day they redesign.
        """
        soup = BeautifulSoup(body.decode("utf-8", "replace"), "html.parser")
        for t in soup(["script", "style", "noscript", "header", "footer", "nav"]):
            t.decompose()
        main = (soup.select_one("#reading_area")
                or soup.select_one("[itemprop=articleBody]")
                or soup.select_one("article")
                or soup.select_one("main"))
        if main is None:
            logger.warning("MLSD %s: no recognised content container "
                           "(#reading_area, [itemprop=articleBody], article, "
                           "main) — storing no text rather than this page's "
                           "chrome", url[:90])
            return "", "", "no-content-container"
        text = _clean(main.get_text("\n", strip=True))
        if len(text) < _MIN_PAGE_CHARS:
            logger.warning("MLSD %s yielded %d chars, below %d — treated as no text",
                           url[:80], len(text), _MIN_PAGE_CHARS)
            # The markup is dropped with the text: storing HTML for something
            # ruled unreadable would look like a document that has content.
            return "", "", "too-short"
        return text, _ILLEGAL_XLSX.sub(" ", str(main)), "html"

    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []

        # ARABIC FIRST, ON A SESSION THAT HAS NOT SEEN `?lang=en`. The language
        # is held in the PHP session rather than the url, so a session that has
        # once asked for English keeps answering in English even for the bare
        # url. This order is the only one that gets both from one session — see
        # the module docstring.
        ar_html, origin = self._listing(self.listing_url)
        ar_rows, ar_repeats = self._rows(ar_html)
        en_html, _ = self._listing(self.listing_url + "?lang=en")
        en_rows, en_repeats = self._rows(en_html)

        if ar_repeats or en_repeats:
            warnings.append(
                f"{max(ar_repeats, en_repeats)} row(s) repeat a document_url and "
                f"were collapsed to one — the ministry lists a document twice")
            logger.warning(warnings[-1])

        if not en_rows:
            raise RuntimeError(
                f"MLSD: {LIST_SELECTOR!r} matched no legislation at "
                f"{self.listing_url}. That is a broken crawler, not an empty "
                f"list — every stored row would be proposed for withdrawal.")

        ar_by_url: Dict[str, str] = dict(ar_rows)
        # PAIRED ON URL, NEVER ON POSITION. The two renderings are the same rows
        # in the same order today; pairing them by index would keep working right
        # up until the day it silently gave one law another law's Arabic title.
        missing_ar = [u for u, _t in en_rows if u not in ar_by_url]
        if missing_ar:
            warnings.append(f"{len(missing_ar)} row(s) present in the English "
                            f"listing and absent from the Arabic one")
            logger.warning(warnings[-1])
        if len(ar_rows) != len(en_rows):
            warnings.append(f"listing length differs by language: "
                            f"{len(ar_rows)} ar vs {len(en_rows)} en")
            logger.warning(warnings[-1])

        # The English translation collapses distinct instruments onto one title
        # (see the module docstring). Identity separates them on document_url, so
        # this is not an error — but it is the reason `title_ar` is stored, and a
        # reviewer should be told the count rather than left to notice it.
        en_titles = [t for _u, t in en_rows]
        collapsed = len(en_titles) - len(set(en_titles))
        if collapsed:
            warnings.append(
                f"{collapsed} English title(s) are shared by more than one row — "
                f"the ministry's translation collapses distinct instruments. "
                f"Compare extra_meta['title_ar'] to tell them apart")
            logger.warning(warnings[-1])

        docs: List[RegulatoryDocument] = []
        no_text = 0
        for url, row_title in en_rows:
            body, ctype, borigin = self._body(url)
            text, html, torigin, info = "", None, borigin, {}
            if body[:5] == b"%PDF-":
                text, torigin, info = self._pdf_text(body, url)
                file_type = "PDF"
            elif body and "html" in ctype:
                text, html, torigin = self._html_text(body, url)
                file_type = "HTML"
            else:
                # No bytes at all (a dead host), or bytes that are neither. The
                # row is still law on the ministry's list, so it is stored with
                # the `document_url|title` fingerprint and no text.
                file_type = "PDF" if "download.php" in url else "HTML"

            if not text:
                no_text += 1

            title = row_title or ar_by_url.get(url) or url.rsplit("/", 1)[-1]
            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.category,
                title=title,
                document_url=url,
                doc_path=[self.regulator, self.source_system,
                          self.listing_title, title],
                file_type=file_type,
                source_page_url=self.listing_url,
                # `orch._process_versioned_doc` copies this into
                # regulation_versions.content_html. Only the one HTML-bodied row
                # has any. Nothing hashes it — the fingerprint stays the text.
                document_html=html or None,
                extra_meta={
                    "crawl_source": self.listing_title,
                    "mlsd_listing_url": self.listing_url,
                    # The authoritative name, and the only thing that tells two
                    # rows apart when the translation collapses their titles.
                    "title_ar": ar_by_url.get(url, ""),
                    "content_text": text,
                    "text_chars": len(text),
                    "text_origin": torigin,
                    "source": origin,
                    **{f"pdf_{k}": v for k, v in info.items()},
                },
            ))
            if cap and len(docs) >= cap:
                break

        # Single exit. The text is the fingerprint where there is text;
        # `stamp_content_hashes` fills the `document_url|title` floor for the
        # rest, and never overwrites a hash already set here.
        for d in docs:
            text = d.extra_meta["content_text"]
            if text:
                d.content_hash = content_key(text)
            d.extra_meta["content_hash_basis"] = "text" if text else "document_url|title"
        docs = stamp_content_hashes(docs)

        if no_text:
            warnings.append(f"{no_text} row(s) yielded no text and fell back to "
                            f"the document_url|title fingerprint")

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.listing_title: len(docs)},
            "source": origin,
        }
        logger.info("MLSDCrawler %s: %d document(s), %d without text, %s",
                    self.listing_title, len(docs), no_text, origin)
        return docs
