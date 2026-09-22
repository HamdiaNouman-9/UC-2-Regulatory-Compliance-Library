"""NBRLawsCrawler — Bahrain's VAT legislation, read with `requests`.

ONE ROW IS ONE SECTION OF THE PAGE, not one PDF. `/laws_regulations/vat` is three
`<h2>` sections — a heading, explanatory prose, and the file(s) that section
publishes. A section with several PDFs is one instrument carrying attachments,
which is the shape `models.RegulatoryDocument` documents for SDAIA.

Two NBR-specific hazards drive the rest: the host throttles bursts with 403, and
two of its Arabic-only PDFs carry a text layer that decodes to Latin mojibake and
passes every existing quality gate. See UC-2-Scratch/NBR/NBR_HANDOFF.md.
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

BASE = "https://www.nbr.gov.bh"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

# The instruments live in `.blog-main`. `.blog-sidebar` beside it holds the
# section nav and the page footer holds the Client Service Charter — both are
# `/media/` links too, so an unscoped link walk files site furniture as law.
MAIN_SELECTOR = ".blog-main"
POST_SELECTOR = ".blog-post"

# What splits one section from the next.
_HEADING_TAGS = ("h1", "h2", "h3")

# The two ways NBR links a document. `/media/<slug>` is the stable permalink and
# is what every VAT row uses today; the raw S3 form is how the Excise page links
# three of its six, so it is recognised here rather than discovered later.
_MEDIA_HREF = re.compile(r"/media/[^/?#]+$")
_PDF_HREF = re.compile(r"\.pdf($|[?#])", re.I)

# The S3 object behind a `/media/` page, and the publisher's own name for it.
_S3_PDF = re.compile(r'https?://[^\s"\'<>]+?\.pdf')
_VIEWER_FILENAME = re.compile(r'fileName:\s*"([^"]+)"')

# Ordinary Arabic. Presentation forms are absent by the time this runs — NFKC
# has folded them; two of these PDFs carry 31,691 and 66,571 of them raw.
_ARABIC = re.compile(r"[؀-ۿ]")

# A letter in any script, for the Arabic ratio recorded on every file.
_LETTER = re.compile(r"[^\W\d_]", re.UNICODE)

# Plain A-Z, and the accented ranges a broken Arabic /ToUnicode map emits.
# These two counts are what separate a genuine English page from mojibake.
_ASCII_LETTER = re.compile(r"[A-Za-z]")
_ACCENTED_LATIN = re.compile(r"[À-ÿĀ-ɏʰ-˿Ͱ-Ͽ]")

# U+FFFD and the C1 block: glyphs the PDF could not spell. Counted, not repaired.
_UNDECODABLE = re.compile(r"[�\x7f-\x9f]")

# openpyxl raises on these, inside save(), after the whole crawl has run.
_ILLEGAL_XLSX = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

# The extractor's own per-page marker. Splitting on it is how this crawler gets
# per-page text without re-implementing the page walk.
_PAGE_MARKER = re.compile(r"^PAGE (\d+)$", re.M)

#: How NBR spells the Arabic twin of an English-only file. Candidates only — a
#: name is accepted solely when it turns out to be a url the Arabic page really
#: carries, so a change of convention loses the pairing and warns rather than
#: inventing one.
_ARABIC_TWIN_SUFFIXES = (("_en", "_ar"), ("_en", "_AR"), ("", "_AR"), ("", "_ar"))

#: A page with NO Arabic at all whose Latin letters are mostly accented did not
#: decode. Measured over all 237 pages of the seven VAT PDFs on 2026-08-31:
#: 59 genuine English pages score 0.000-0.002 and the 5 mojibake pages score
#: 0.411-0.457. The populations are two orders of magnitude apart, so this is a
#: separator rather than a tuned threshold. Arabic-bearing pages never reach it.
_MOJIBAKE_ACCENT_RATIO = 0.05

#: Retries for a fetch that could not be ANSWERED. A GET changes nothing, so
#: asking again is safe; the backoff is what keeps it polite.
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.0

#: 403 here is a BURST THROTTLE, not a block: six unpaced requests earn one and a
#: single request 45s later succeeds. It is retried, but on its own much longer
#: backoff — and it is never treated as an answer, so a run that cannot get past
#: it fails loudly instead of reporting an empty listing.
THROTTLE_BACKOFF = 15.0

#: Statuses that are an ANSWER rather than a failure to answer. 403 is
#: deliberately absent — see above.
_FINAL_STATUSES = (200, 401, 404, 410)

#: The identity a multi-file row declares. A row holding three PDFs has no single
#: `document_url`, so every such row in one folder would otherwise share
#: ("", doc_path). Single-file rows keep the default identity — `orch` honours
#: the choice per document, so the two coexist in one run.
_MULTI_FILE_IDENTITY = ["doc_path", "extra_meta.attachment_links", "title"]


def _norm(s: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", s or "").split())


def _clean(text: str) -> str:
    """NFKC first, sanitise second, so the text hashed is the text stored."""
    text = unicodedata.normalize("NFKC", text or "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return _ILLEGAL_XLSX.sub(" ", text).strip()


def _looks_undecoded(page_text: str) -> bool:
    """True when a page's text layer produced Latin noise instead of Arabic.

    Only pages with no Arabic at all are candidates; among those, real English
    is ~0% accented Latin and mojibake is ~44%.
    """
    if not page_text or _ARABIC.search(page_text):
        return False
    accented = len(_ACCENTED_LATIN.findall(page_text))
    if not accented:
        return False
    ascii_letters = len(_ASCII_LETTER.findall(page_text))
    return accented / max(accented + ascii_letters, 1) > _MOJIBAKE_ACCENT_RATIO


def _cache_file(cache_dir: Optional[str], url: str, suffix: str) -> Optional[pathlib.Path]:
    if not cache_dir:
        return None
    d = pathlib.Path(cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    return d / (hashlib.sha256(url.encode()).hexdigest()[:16] + suffix)


class NBRLawsCrawler:
    """One NBR laws-and-regulations page, read as its `<h2>` sections.

    `listing_url`, `folder_title` and `source_system` come from the YAML so a
    site retitle cannot silently reshape `doc_path`.
    """

    def __init__(
        self,
        regulator: str,
        source_system: str,
        listing_url: str,
        folder_title: str,
        listing_title: Optional[str] = None,
        category: Optional[str] = None,
        expected_heading: Optional[str] = None,
        min_sections: int = 1,
        timeout: int = 90,
        delay: float = 3.0,
        page_cache_dir: Optional[str] = None,
        pdf_cache_dir: Optional[str] = None,
    ):
        self.regulator = regulator
        self.source_system = source_system
        self.listing_url = listing_url
        # The doc_path tier between source_system and the section — "VAT".
        self.folder_title = folder_title
        # The completeness gate's grouping key; not a doc_path crumb.
        self.listing_title = listing_title or folder_title
        self.category = category or folder_title
        # A recorded observation of the page, not our chosen title — a guard that
        # compares against the title warns forever about a permanent difference.
        self.expected_heading = expected_heading
        # A FLOOR, NOT THE COUNT. Three sections were measured; naming 3 would
        # turn NBR adding a fourth into a crawl failure. Zero is the failure.
        self.min_sections = int(min_sections)
        self.timeout = timeout
        # Politeness, and this host measurably needs it — see THROTTLE_BACKOFF.
        self.delay = float(delay)
        # Dev-only read-through caches. Kept out of the yml: a cache serving
        # production is a stale law reporting `unchanged` after an amendment.
        self.page_cache_dir = page_cache_dir or os.environ.get("NBR_PAGE_CACHE_DIR") or None
        self.pdf_cache_dir = pdf_cache_dir or os.environ.get("NBR_PDF_CACHE_DIR") or None
        self.last_result: dict = {}
        self._session: Optional[requests.Session] = None
        # The site's own default. Tracked because it is part of the cache key.
        self._lang = "ar"

    @property
    def source_names(self) -> List[str]:
        return [self.listing_title]

    # ------------------------------------------------------------------ #
    #  transport                                                         #
    # ------------------------------------------------------------------ #

    def _http(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers.update({
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            })
            self._session = s
        return self._session

    def _get(self, url: str, attempts: int = RETRY_ATTEMPTS):
        """One request, retrying only what could not be answered. None when
        every attempt failed."""
        wait = RETRY_BACKOFF
        for attempt in range(1, max(attempts, 1) + 1):
            try:
                r = self._http().get(url, timeout=self.timeout)
                if r.status_code in _FINAL_STATUSES:
                    if self.delay:
                        time.sleep(self.delay)
                    return r
                if r.status_code == 403:
                    logger.warning("NBR %s -> 403 (throttled, attempt %d/%d)",
                                   url[-60:], attempt, attempts)
                    if attempt < attempts:
                        time.sleep(THROTTLE_BACKOFF * attempt)
                    continue
                logger.warning("NBR %s -> HTTP %s (attempt %d/%d)",
                               url[-60:], r.status_code, attempt, attempts)
            except requests.RequestException as e:
                logger.warning("NBR %s -> %s (attempt %d/%d)",
                               url[-60:], type(e).__name__, attempt, attempts)
            if attempt < attempts:
                time.sleep(wait)
                wait *= 2
        return None

    def _language(self, lang: str) -> None:
        """Set the session language. NBR holds it in the SESSION, not the url,
        so this must precede the reads it applies to."""
        self._get(BASE + "/language/" + lang)
        self._lang = lang

    def _page(self, url: str) -> Tuple[str, str]:
        # KEYED ON LANGUAGE TOO. The two renderings share one url — the language
        # is in the session — so a url-only cache key would serve the English
        # listing as the Arabic read and silently collapse the two.
        cp = _cache_file(self.page_cache_dir, self._lang + "|" + url, ".html")
        if cp is not None and cp.exists():
            return cp.read_text("utf-8", "replace"), "page-cache"
        r = self._get(url)
        if r is None or r.status_code != 200:
            why = ("no response after %d attempts" % RETRY_ATTEMPTS if r is None
                   else "HTTP %s" % r.status_code)
            raise RuntimeError(
                "NBR page " + url + " returned " + why + ". Nothing may be "
                "classified from this — an empty listing is ruled `disappeared` "
                "downstream and proposes law in force for withdrawal.")
        if cp is not None:
            cp.write_text(r.text, encoding="utf-8")
        return r.text, "live"

    # ------------------------------------------------------------------ #
    #  the page, as sections                                             #
    # ------------------------------------------------------------------ #

    def _sections(self, html: str) -> List[dict]:
        """The page's `<h2>` sections: heading, own markup, prose, file links.

        A SECTION IS THE ROW. NBR publishes an instrument as a heading plus the
        prose describing it plus the file(s) it links; splitting on the links
        instead produces one row per PDF and loses which instrument they belong
        to.
        """
        soup = BeautifulSoup(html, "html.parser")
        main = soup.select_one(MAIN_SELECTOR)
        if main is None:
            raise RuntimeError(
                "NBR " + self.listing_url + ": no " + repr(MAIN_SELECTOR) +
                " on the page. That is a broken crawler, not an empty list — "
                "falling back to the whole body would file the sidebar and the "
                "Client Service Charter as VAT law.")
        post = main.select_one(POST_SELECTOR) or main
        out: List[dict] = []
        cur: Optional[dict] = None
        for el in post.children:
            if getattr(el, "name", None) is None:
                continue
            if el.name in _HEADING_TAGS:
                heading = _norm(el.get_text(" ", strip=True))
                # NBR emits empty `<h2> </h2>` as spacing between sections;
                # those must not start a new one.
                if heading:
                    cur = {"heading": heading, "html": [], "prose": [], "links": []}
                    out.append(cur)
                continue
            if cur is None:
                continue
            cur["html"].append(str(el))
            txt = _norm(el.get_text(" ", strip=True))
            if txt:
                cur["prose"].append(txt)
            for a in el.find_all("a", href=True):
                url = urljoin(self.listing_url, (a.get("href") or "").strip())
                if _MEDIA_HREF.search(url) or _PDF_HREF.search(url):
                    label = _norm(a.get_text(" ", strip=True))
                    if url not in [u for _t, u in cur["links"]]:
                        cur["links"].append((label, url))
        for s in out:
            s["html"] = "".join(s["html"])
            s["prose"] = "\n".join(s["prose"])
        return out

    @staticmethod
    def _slugs(section: dict) -> set:
        return {u.rstrip("/").rsplit("/", 1)[-1] for _t, u in section["links"]}

    @staticmethod
    def _twin_slugs(slugs) -> set:
        """A slug set widened to the Arabic spellings NBR also uses, so an
        English section can be matched to its Arabic counterpart on a FILE they
        share rather than on their position in the page."""
        out = set(slugs)
        for s in slugs:
            for en_suffix, ar_suffix in _ARABIC_TWIN_SUFFIXES:
                if en_suffix and not s.endswith(en_suffix):
                    continue
                out.add((s[:-len(en_suffix)] if en_suffix else s) + ar_suffix)
        return out

    # ------------------------------------------------------------------ #
    #  one file                                                          #
    # ------------------------------------------------------------------ #

    def _resolve(self, url: str) -> Tuple[str, Dict]:
        """The S3 PDF behind a `/media/` permalink, and what the page says about
        it. A url that is already a PDF resolves to itself."""
        if _PDF_HREF.search(url):
            return url, {"media_page": ""}
        html, origin = self._page(url)
        soup = BeautifulSoup(html, "html.parser")
        main = soup.select_one(MAIN_SELECTOR)
        # Scoped to `.blog-main`: the page furniture carries S3 links too, and
        # the first one found off a whole-page regex is not reliably the document.
        pdfs = list(dict.fromkeys(_S3_PDF.findall(str(main)))) if main else []
        fname = _VIEWER_FILENAME.search(html)
        # NBR's own name for the file, from the viewer config. This is the ONLY
        # name the `/media/` page carries: its `<h2 class="blog-post-title">` is
        # rendered EMPTY on every one of them (measured 2026-08-31).
        meta = {"media_page": url, "media_page_origin": origin,
                "file_name": fname.group(1) if fname else "",
                "stated_size": _norm(main.get_text(" ", strip=True)) if main else ""}
        if not pdfs:
            return "", meta
        if len(pdfs) > 1:
            meta["other_pdfs_on_page"] = " | ".join(pdfs[1:])
        return pdfs[0], meta

    def _file(self, url: str) -> Tuple[bytes, Dict, str]:
        """(bytes, S3 validators, origin). The validators are recorded per file,
        not used as the fingerprint — see the handoff."""
        cp = _cache_file(self.pdf_cache_dir, url, ".bin")
        if cp is not None and cp.exists():
            return cp.read_bytes(), {}, "pdf-cache"
        r = self._get(url)
        if r is None:
            return b"", {}, "unreachable"
        if r.status_code != 200:
            logger.warning("NBR file %s -> HTTP %s", url[-60:], r.status_code)
            return b"", {}, "http-%s" % r.status_code
        val = {"s3_etag": (r.headers.get("ETag") or "").strip('"'),
               "s3_last_modified": r.headers.get("Last-Modified") or "",
               "s3_content_length": r.headers.get("Content-Length") or ""}
        if cp is not None:
            cp.write_bytes(r.content)
        return r.content, val, "live"

    def _pdf_text(self, body: bytes, url: str) -> Tuple[str, str, dict]:
        """Extracted text for one PDF, with undecoded pages re-read by OCR."""
        tmp = pathlib.Path(tempfile.mkstemp(suffix=".pdf")[1])
        try:
            tmp.write_bytes(body)
            try:
                # Heavy import (cv2, pdfplumber, pytesseract) — kept off module
                # load so importing this crawler stays cheap for anything that
                # only parses.
                from processor.Text_Extractor import OCRProcessor
                # The orchestrator's own extractor, so the text stored here is
                # the text it would have produced, not a second opinion.
                text, meta = OCRProcessor.extract_text_from_pdf_smart(pdf_path=str(tmp))
                langs = OCRProcessor.ocr_langs()
                ocr_ok = "ara" in (langs or "").split("+")
                text, repaired, unrepaired = self._repair_pages(
                    text, str(tmp), OCRProcessor, ocr_ok)
            except Exception as e:
                logger.warning("NBR text extraction failed %s: %s", url[-60:], e)
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
        # A page routed to OCR contributes nothing when the engine has no model
        # for its script, but the extractor still counts it in `good_pages`.
        usable = good - (0 if ocr_ok else ocred)
        info = {"pages": total, "ocr_pages": ocred, "ocr_available": ocr_ok,
                "ocr_langs": langs, "pages_read": max(usable, 0),
                "reocr_pages": repaired, "undecoded_pages_dropped": unrepaired}

        text = _clean(text)
        if usable <= 0 or not text:
            return "", "no-pages-read", info

        letters = len(_LETTER.findall(text))
        info["arabic_ratio"] = round(len(_ARABIC.findall(text)) / letters, 3) if letters else 0.0
        info["undecodable_chars"] = len(_UNDECODABLE.findall(text))
        return text, "pdf", info

    def _repair_pages(self, text: str, path: str, ocr, ocr_ok: bool):
        """Re-OCR pages whose text layer produced Latin noise; drop them when
        OCR cannot read Arabic. Returns (text, repaired, dropped)."""
        marks = list(_PAGE_MARKER.finditer(text or ""))
        if not marks:
            return text, 0, 0
        out, repaired, dropped = [], 0, 0
        for i, m in enumerate(marks):
            end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
            num = int(m.group(1))
            page = text[m.end():end]
            if not _looks_undecoded(page):
                out.append(text[m.start():end])
                continue
            if not ocr_ok:
                # Storing nothing is recoverable — the row keeps its url for a
                # person to open. Storing mojibake is not: nothing downstream
                # can tell it from the instrument.
                logger.warning("NBR page %d: text layer did not decode and OCR "
                               "has no 'ara' — dropping the page rather than "
                               "storing Latin noise as the law", num)
                dropped += 1
                continue
            fixed = ocr._ocr_single_page(path, num)
            if not fixed or not _ARABIC.search(fixed):
                dropped += 1
                continue
            out.append("PAGE %d\n%s" % (num, fixed))
            repaired += 1
        return "\n\n".join(out), repaired, dropped

    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit=None) -> List[RegulatoryDocument]:
        cap = limit if isinstance(limit, int) and limit > 0 else None
        warnings: List[str] = []

        # ARABIC FIRST, on a session that has not yet asked for English: the
        # language lives in the session, so this order is the only one that gets
        # both renderings from one session. The Arabic read is for the section's
        # own name only — the FILES stored are the English rendering's.
        self._language("ar")
        ar_html, _ = self._page(self.listing_url)
        ar_sections = self._sections(ar_html)

        self._language("en")
        en_html, origin = self._page(self.listing_url)
        en_sections = self._sections(en_html)

        # A SECTION WITH NO FILES IS NOT A DOCUMENT. The page opens with a
        # "VAT Law & Regulations" heading and 142 characters introducing the
        # other three; storing it would put a page intro in the library.
        intro = [s["heading"] for s in en_sections if not s["links"]]
        sections = [s for s in en_sections if s["links"]]

        if not sections:
            raise RuntimeError(
                "NBR: no section under " + repr(MAIN_SELECTOR) + " at " +
                self.listing_url + " carries a document link. That is a broken "
                "crawler, not an empty page — every stored row would be "
                "proposed for withdrawal.")
        if len(sections) < self.min_sections:
            raise RuntimeError(
                "NBR %s: %d section(s) with documents, below the floor of %d. "
                "Refusing a short page — it is indistinguishable from NBR "
                "withdrawing law." % (self.folder_title, len(sections), self.min_sections))

        if self.expected_heading and self.expected_heading not in intro + [s["heading"] for s in en_sections]:
            warnings.append("the recorded page heading %r is no longer on the "
                            "page — it may have moved" % self.expected_heading)
            logger.warning(warnings[-1])

        # PAIRED ON A FILE THEY SHARE, NEVER ON POSITION. The two renderings hold
        # the same sections in the same order today; pairing by index would keep
        # working right up until the day it gave one instrument another's name.
        ar_by_slug = {}
        for s in ar_sections:
            for sl in self._slugs(s):
                ar_by_slug[sl] = s
        unpaired = 0

        docs: List[RegulatoryDocument] = []
        for sec in sections:
            twin = self._twin_slugs(self._slugs(sec))
            ar = next((ar_by_slug[sl] for sl in twin if sl in ar_by_slug), None)
            if ar is None:
                unpaired += 1

            files, parts, no_text = [], [], 0
            for label, link in sec["links"]:
                pdf_url, fmeta = self._resolve(link)
                text, info, val, torigin = "", {}, {}, "no-pdf-link"
                if pdf_url:
                    body, val, borigin = self._file(pdf_url)
                    if body[:5] == b"%PDF-":
                        text, torigin, info = self._pdf_text(body, pdf_url)
                    else:
                        torigin = borigin if not body else "not-a-pdf"
                if not text:
                    no_text += 1
                # THE FILE URL IS THE PDF, not the `/media/` viewer page. The
                # permalink is kept beside it because it is the stable, humane
                # address and the only thing that survives an S3 re-upload.
                files.append(dict({"title": label, "file_url": pdf_url,
                                   "text_chars": len(text), "text_origin": torigin},
                                  **fmeta, **val,
                                  **{"pdf_" + k: v for k, v in info.items()}))
                if text:
                    parts.append("=== %s (%s) ===\n%s"
                                 % (label, fmeta.get("file_name") or
                                    pdf_url.rsplit("/", 1)[-1], text))

            # The section's own prose first, then each attached instrument's
            # text under a heading naming it. The prose is what the section says
            # about the files; without the files' text the law itself would never
            # reach the library, because the orchestrator writes the version row
            # BEFORE it extracts anything of its own (orch.py:869).
            content_text = _clean("\n\n".join([sec["prose"]] + parts))

            urls = [f["file_url"] for f in files if f["file_url"]]
            multi = len(urls) > 1
            meta = {
                "crawl_source": self.listing_title,
                "nbr_listing_url": self.listing_url,
                "section_heading": sec["heading"],
                "title_ar": (ar or {}).get("heading", ""),
                "content_text": content_text,
                "text_chars": len(content_text),
                "prose_chars": len(sec["prose"]),
                "source": origin,
                # THE CONVENTIONAL KEYS, and `file_titles` is not decoration.
                # `orchestrator._extract_text` TIER 0 splits it on "|" to label
                # each file's text when it extracts a multi-file row itself; with
                # it absent the labels fall back to the file name, which here is
                # a 40-character S3 upload hash and names nothing. `record_kind`
                # and `n_files` follow formfill's `combined_attachments` rows.
                "record_kind": "combined_attachments",
                "n_files": len(files),
                "file_titles": " | ".join(f["title"] for f in files),
                "file_count": len(files),
                "files_without_text": no_text,
                # Every file's own url, validators and extraction numbers, in the
                # order NBR lists them.
                "files": files,
                "media_pages": [f.get("media_page", "") for f in files],
            }
            if multi:
                # A multi-file row leaves document_url EMPTY and lists everything
                # here, JOINED WITH " | " — a STRING, not a list.
                #
                # A list looks safe because `excel_repo._flat` joins lists on
                # " | " anyway, but that runs at the REPO and
                # `utils.file_links.normalise_files` runs first, at the
                # orchestrator. Its `split_links` does `str(value)`, so a list
                # arrives as "['https://a', 'https://b']", splits on the comma,
                # and neither part then starts with "http" — every file is
                # dropped and the row reaches `check` with no files at all.
                meta["attachment_links"] = " | ".join(urls)
                meta["identity_fields"] = _MULTI_FILE_IDENTITY

            docs.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.category,
                title=sec["heading"],
                # Exactly one file -> document_url; more than one -> empty, with
                # the set in attachment_links. `check` errors on a multi-file row
                # that carries both.
                document_url="" if multi else (urls[0] if urls else ""),
                doc_path=[self.regulator, self.source_system,
                          self.folder_title, sec["heading"]],
                file_type="PDF",
                source_page_url=self.listing_url,
                # The section's own markup — the prose NBR publishes around its
                # files. This is the row's HTML, and it is genuinely this row's:
                # not the landing page, not the viewer shell.
                document_html=_ILLEGAL_XLSX.sub(" ", sec["html"]) or None,
                extra_meta=meta,
            ))
            if cap and len(docs) >= cap:
                break

        # Single exit. The text is the fingerprint where there is text;
        # `stamp_content_hashes` fills the `document_url|title` floor for the
        # rest and never overwrites a hash already set here.
        for d in docs:
            text = d.extra_meta["content_text"]
            if text:
                d.content_hash = content_key(text)
            d.extra_meta["content_hash_basis"] = "text" if text else "document_url|title"
        docs = stamp_content_hashes(docs)

        if intro:
            warnings.append("%d heading(s) carry no document link and were not "
                            "stored: %s" % (len(intro), ", ".join(intro)))
        if unpaired:
            warnings.append("%d section(s) could not be paired with the Arabic "
                            "rendering on a shared file, so title_ar is empty"
                            % unpaired)
            logger.warning(warnings[-1])
        empty = sum(int(d.extra_meta.get("files_without_text") or 0) for d in docs)
        if empty:
            warnings.append("%d attached file(s) yielded no text" % empty)
        reocr = sum(int(f.get("pdf_reocr_pages") or 0)
                    for d in docs for f in d.extra_meta["files"])
        dropped = sum(int(f.get("pdf_undecoded_pages_dropped") or 0)
                      for d in docs for f in d.extra_meta["files"])
        if reocr:
            warnings.append("%d page(s) had a text layer that did not decode and "
                            "were re-read by OCR" % reocr)
        if dropped:
            warnings.append("%d page(s) did not decode and could NOT be repaired "
                            "— their text is absent, not wrong" % dropped)
            logger.warning(warnings[-1])

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": warnings},
            "by_source": {self.listing_title: len(docs)},
            "source": origin,
        }
        logger.info("NBRLawsCrawler %s: %d section(s), %d file(s), "
                    "%d page(s) re-OCR'd, %s", self.folder_title, len(docs),
                    sum(int(d.extra_meta["file_count"]) for d in docs), reocr, origin)
        return docs
