"""
GenericSiteCrawler — plugs the generic crawler into the existing pipeline.

The orchestrator asks one thing of a crawler (orchestrator.py, run_for_regulator):

    docs = self.crawler.fetch_documents()      # -> List[RegulatoryDocument]

The hand-written crawlers (SAMACombinedCrawler, SBPCrawler, SECPCrawler) already
answer that. The generic crawler did not: it wrote Excel/JSON files. This class is
the translator, so a generic crawl becomes indistinguishable from a hand-written
one as far as the pipeline is concerned.

    crawler = GenericSiteCrawler(
        seed_url      = "https://misa.gov.sa/activities/laws/",
        regulator     = "MISA",
        source_system = "MISA-LAWS",
        category      = "Laws and Regulations",
    )
    Orchestrator(crawler=crawler, repo=..., ...).run_for_regulator("MISA")

WHY IT RUNS IN A SUBPROCESS BY DEFAULT
--------------------------------------
The engine uses Playwright's SYNC api, which refuses to start inside a running
asyncio loop. The pipeline already installs a twisted/asyncio reactor for Scrapy
(scheduler.py, jobs/sbp_job.py call crochet.setup()), so an in-process crawl would
fail there. scheduler.py already runs SECP and SAMA as subprocesses for the same
reason. Pass in_process=True only from a plain script or a test.

WHAT MAPS TO WHAT
-----------------
    crawl row                     RegulatoryDocument
    ----------------------------  ---------------------------------------
    documents[].doc_url           document_url
    documents[].title             title
    documents[].type              file_type      (PDF / DOCX / EXTERNAL ...)
    documents[].found_on          source_page_url
    documents[].section_path      doc_path  (split on " > ", regulator first)
    documents[].content_hash      content_hash
    pages[].text                  extra_meta["content_text"]
    pages[].html                  document_html

`published_date` is left None: a link walk cannot reliably read issue dates. The
orchestrator handles that — filter_new_documents falls back to deduping on
(document_url, category) when published_date is missing.
"""

import json
import logging
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

from models.models import RegulatoryDocument

from dynamic_crawler.formfill.runner import _ext_type, _is_doc

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
ENGINE = REPO_ROOT / "generic_crawler" / "crawler.py"

# Matches MIN_TEXT_LEN in orchestrator.py. A page with at least this much prose
# is a document no matter what else we know about it.
MIN_PAGE_TEXT = 200

# A LEAF page (no children in the site tree) is a real document even when it is
# short: SAMA's "Article 3" is 184 characters of actual law. Only judge such a
# page by length once we know it has no children — otherwise a folder like
# "Chapter 3: Monetary Policy" (10 characters, one child) looks the same.
MIN_LEAF_TEXT = 50

# A page whose visible text is its own title plus a date stamp and nothing else
# is a wrapper around the file it links, not a document. See
# GenericSiteCrawler._is_link_wrapper for the CBE case this was measured on.
#
# MEASURED over all 92 CBE HTML pages, 2026-08-20, residue after removing the
# page's own title and its date stamp:
#
#       0   CBE Risk Appetite Statement   <- the wrapper, the only one under 40
#      47   Laws
#      94   Governance
#      98   Payment Acceptance Channels
#     103   Regulations Book
#
# So the real gap is 0 -> 47, not the comfortable one a first look at a single
# section suggested. 20 sits in the middle of that gap and still catches the
# target with room to spare; 40 would have left a 7-character margin against a
# real page, which is not a margin at all.
#
# Do NOT raise this to catch "nearly empty" pages. The rule keys on residue
# precisely so it cannot become a length rule — MIN_LEAF_TEXT exists because
# SAMA's "Article 3" is 184 characters of actual law, and a generous threshold
# here would start eating documents like it.
WRAPPER_RESIDUE_CHARS = 20



# ---- reading the listing row -------------------------------------------------
# A listing row carries what the detail page usually does not repeat:
#     "... BPRD Circular Letter No. 15 of 2026  July 06 2026 | BPRD | Circular Letters"
# published_date and reference_no are the two fields the pipeline actually uses,
# so parse those generically and keep the raw row for anything else.

_MONTH = (r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
          r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?")
_DATE_PATTERNS = [
    re.compile(rf"\b({_MONTH})\s+(\d{{1,2}}),?\s+(\d{{4}})\b", re.I),   # July 06 2026
    re.compile(rf"\b(\d{{1,2}})\s+({_MONTH}),?\s+(\d{{4}})\b", re.I),   # 06 July 2026
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),                          # 2026-07-06
    re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b"),                      # 06/07/2026
]
# The date the CMS prints under the heading on every page ("13 Aug 2026",
# "23 Mar 2023", "2026-08-13", "13/08/2026"). Removed before judging residue, so
# a wrapper is not saved from the rule by the template's own furniture.
_PAGE_DATE_STAMP_RE = re.compile(
    rf"\b(?:\d{{1,2}}\s+(?:{_MONTH})\s+\d{{4}}"
    rf"|(?:{_MONTH})\s+\d{{1,2}},?\s+\d{{4}}"
    rf"|\d{{4}}-\d{{2}}-\d{{2}}"
    rf"|\d{{1,2}}/\d{{1,2}}/\d{{4}})\b",
    re.I,
)

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun",
     "jul", "aug", "sep", "oct", "nov", "dec"], start=1)}

# "BPRD Circular Letter No. 15 of 2026", "DMMD Circular No.03 of 2026"
_REF_RE = re.compile(
    r"\b([A-Z][A-Za-z&.\-]*(?:\s+[A-Za-z&.\-]+){0,4}\s+No\.?\s*\d+"
    r"(?:\s+of\s+\d{4})?)", re.I)


def _parse_row_date(row_text: str) -> Optional[str]:
    """First recognisable date in the row, as ISO. None when unsure — a wrong
    date is worse than no date, since the pipeline dedupes on it."""
    for pat in _DATE_PATTERNS:
        m = pat.search(row_text or "")
        if not m:
            continue
        try:
            a, b, c = m.group(1), m.group(2), m.group(3)
            if a.lower()[:3] in _MONTHS:                       # July 06 2026
                return f"{int(c):04d}-{_MONTHS[a.lower()[:3]]:02d}-{int(b):02d}"
            if b.lower()[:3] in _MONTHS:                       # 06 July 2026
                return f"{int(c):04d}-{_MONTHS[b.lower()[:3]]:02d}-{int(a):02d}"
            if len(a) == 4:                                    # 2026-07-06
                return f"{int(a):04d}-{int(b):02d}-{int(c):02d}"
            return f"{int(c):04d}-{int(b):02d}-{int(a):02d}"    # 06/07/2026 (d/m/y)
        except Exception:
            continue
    return None


def _parse_row_ref(row_text: str, title: str = "") -> Optional[str]:
    """The regulator's own reference number, if the row states one."""
    body = (row_text or "")
    if title and body.startswith(title):
        body = body[len(title):]        # the title itself is not the reference
    m = _REF_RE.search(body)
    return m.group(1).strip()[:120] if m else None


def _flatten_extracted(extracted: dict) -> dict:
    """content.extract results, ready to merge into extra_meta.

    A selector-only entry gives markup and is kept as-is. A `pairs` entry gives
    {label: value} — MHRSD's sidebar yields "Resolution Number": "137440",
    "Release Date": "23-Shawwal-1446-21-April-2025" — and those are flattened to
    snake_case keys so each is addressable on its own.
    """
    out: dict = {}
    for key, val in (extracted or {}).items():
        if isinstance(val, dict):
            for label, text in val.items():
                slug = re.sub(r"[^a-z0-9]+", "_",
                              str(label).strip().lower()).strip("_")
                if slug and text:
                    out[slug] = text
        elif val:
            out[key] = val
    return out


def _split_section_path(section_path: str) -> List[str]:
    return [p.strip() for p in (section_path or "").split(">") if p.strip()]


def _dedupe_keep_order(parts: List[str]) -> List[str]:
    out = []
    for p in parts:
        if not out or out[-1].strip().lower() != p.strip().lower():
            out.append(p)
    return out


#: Crumbs that name the CMS, not a subject area. SharePoint calls its site
#: collection "Internet", and it shows up in the breadcrumb of every page.
_NON_SUBJECT_CRUMBS = {"internet", "home", "main", "index", "default",
                       "site", "sitepages", "pages", "root", "portal"}


def _crumb_key(s: str) -> str:
    """Comparison form for a folder crumb.

    "Regulations & Laws" and "Regulations and Laws" are the same folder written
    two ways — the site's breadcrumb uses one, the source YAML the other — so a
    plain lowercase comparison keeps both and the trail says it twice.
    """
    s = (s or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _clean_trail(parts: List[str]) -> List[str]:
    """Drop CMS crumbs and any crumb already said earlier in the trail.

    `_dedupe_keep_order` only removes ADJACENT repeats. Ministry of Commerce
    produced:

        Ministry of Commerce | Regulations and Laws | Internet |
        Ministry of Commerce | Regulations & Laws

    — the regulator and the source system repeated at positions 3 and 4, not
    adjacent to their originals because SharePoint's "Internet" sat between
    them. All 115 rows shared that one trail.
    """
    out: List[str] = []
    seen = set()
    for p in parts:
        k = _crumb_key(p)
        if not k or k in _NON_SUBJECT_CRUMBS or k in seen:
            continue
        seen.add(k)
        out.append(p.strip())
    return out


class GenericSiteCrawler:
    """One seed URL -> a list of RegulatoryDocument."""

    def __init__(
        self,
        seed_url: str,
        regulator: str,
        source_system: str,
        category: Optional[str] = None,
        scope: str = "auto",
        max_pages: int = 150,
        max_depth: int = 8,
        out_dir: Optional[str] = None,
        include_pages: str = "auto",      # "auto" | "always" | "never"
        # URL path prefixes this source must NOT walk or collect files from,
        # because another source already owns them. CBE's laws-regulations is
        # the case: it can only be crawled from its parent (the
        # regulations-book page links to nothing), and the parent's prefix scope
        # then swallows /regulations/circulars -- 396 documents the API source
        # already holds with real titles and dates. See
        # generic_crawler/crawler.py::path_excluded.
        exclude_paths: Optional[List[str]] = None,
        # Breadcrumb steps this source must NOT turn into a folder, matched
        # case-insensitively against whole crumbs. For a crumb that names the
        # CMS or repeats the source_system: QCB's every page sits under
        # "Home page", and each of its three generic section pages then repeats
        # its own heading as the crumb below a source_system already called that.
        #
        # PER SOURCE, NOT GLOBAL, and absent by default: a crumb that is chrome
        # on one site is a real folder on another, and _NON_SUBJECT_CRUMBS is
        # shared by every regulator — adding to it changes doc_path, doc_path is
        # an identity field, and the next crawl would read the moved rows as
        # `new` and the stored ones as `disappeared`. Nothing sets this today
        # except config/sources/qcb.yml, so every other regulator's trail is
        # byte-identical to before.
        drop_sections: Optional[List[str]] = None,
        # END THE FOLDER TRAIL WITH THE DOCUMENT'S OWN TITLE.
        #
        # `_walk_folders` types the LAST doc_path segment "R" (a regulation) and
        # every segment above it "F" (a folder). Without this the trail ends at
        # the source system, so the SOURCE SYSTEM becomes the regulation node and
        # the document's real title appears nowhere in the tree.
        #
        # MEASURED on the first QCB export, output/workbooks/qcb.xlsx: "Qatar
        # Central Bank" had 27 children -- one folder (Legislation, whose custom
        # source builds its own path) and 26 nodes typed R, named "Information
        # Security" x6, "Fintech and Innovation" x11 and "ESG and Sustainability
        # Strategy for the Financial Sector" x9, carrying 29 documents between
        # them. `check` passed it: identity is (document_url, doc_path, title)
        # and all 29 are distinct on the url.
        #
        # OFF BY DEFAULT, and it must stay that way for the regulators already
        # stored. doc_path is an identity field -- turning this on for a source
        # that has been promoted re-files every one of its rows, which the next
        # crawl reads as all-new plus all-disappeared, and `disappeared` feeds
        # the withdrawal gate. It is free to set only where the database holds no
        # rows for that source yet.
        doc_path_title: bool = False,
        wait_ms: Optional[int] = None,    # per-site JS settle time
        in_process: bool = False,
        timeout: int = 3600,
    ):
        self.seed_url = seed_url
        self.regulator = regulator
        self.source_system = source_system
        self.category = category
        self.scope = scope
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.out_dir = out_dir
        self.include_pages = include_pages
        self.exclude_paths = list(exclude_paths or [])
        # NORMALISED WITH `_crumb_key`, THE SAME FORM `_clean_trail` COMPARES BY.
        # A plain `.strip().lower()` matches the literal string and misses what
        # a CMS actually serves: QCB's Digital Currency breadcrumb is
        # "Market&#160;Development and Innovation&#160;Sector" -- non-breaking
        # spaces where a person sees spaces -- so an entry typed with ordinary
        # spaces would never fire, and the wrong folder would appear with nothing
        # in the config looking wrong. `_crumb_key` also folds "&" and "and",
        # which is the same mercy for a folder spelled two ways.
        self.drop_sections = {_crumb_key(x) for x in (drop_sections or [])
                              if _crumb_key(x)}
        self.doc_path_title = bool(doc_path_title)
        # How long to let JavaScript settle before reading the page. The engine
        # has always accepted --wait-ms; nothing passed it, so every site got the
        # default. ZATCA's landing page renders its links client-side and read at
        # the default returns text_len=22 and queues NOTHING — the crawl visits
        # the seed, finds no links worth following and stops at one page.
        self.wait_ms = wait_ms
        self.in_process = in_process
        self.timeout = timeout
        self.last_result = {}             # raw crawl output, for diagnostics

    # ------------------------------------------------------------------ #
    #  running the engine                                                  #
    # ------------------------------------------------------------------ #

    def _run_crawl(self) -> dict:
        out = Path(self.out_dir) if self.out_dir else Path(
            tempfile.mkdtemp(prefix="generic_crawl_"))

        if self.in_process:
            sys.path.insert(0, str(REPO_ROOT))
            from generic_crawler.crawler import crawl
            kw = {"wait_ms": self.wait_ms} if self.wait_ms else {}
            crawl(self.seed_url, str(out), max_pages=self.max_pages,
                  max_depth=self.max_depth, scope=self.scope, **kw)
        else:
            cmd = [sys.executable, str(ENGINE),
                   "--url", self.seed_url, "--out", str(out),
                   "--scope", self.scope,
                   "--max-pages", str(self.max_pages),
                   "--max-depth", str(self.max_depth)]
            for ex in (self.exclude_paths or []):
                cmd += ["--exclude", str(ex)]
            if self.wait_ms:
                cmd += ["--wait-ms", str(self.wait_ms)]
            proc = subprocess.run(cmd, capture_output=True, text=True,
                                  encoding="utf-8", errors="replace",
                                  timeout=self.timeout)
            if proc.returncode != 0:
                tail = (proc.stderr or proc.stdout or "").strip().splitlines()
                raise RuntimeError(
                    f"generic crawler failed for {self.seed_url}: "
                    f"{tail[-1] if tail else 'no output'}")

        pages_json = out / "pages.json"
        if not pages_json.exists():
            raise RuntimeError(f"generic crawler produced no pages.json in {out}")
        return json.loads(pages_json.read_text(encoding="utf-8"))

    # ------------------------------------------------------------------ #
    #  mapping                                                             #
    # ------------------------------------------------------------------ #

    def _doc_path(self, section_path: str, title: str = "") -> List[str]:
        """Folder trail for the library.

        ALWAYS starts with the regulator. _get_or_create_compliance_category()
        builds the folder tree from this list, so two regulators that both use a
        top-level folder called "Circulars" would otherwise merge into one node
        and tangle their documents together.
        """
        # `drop_sections` filters HERE and only here. `extra_meta["section_path"]`
        # is stored raw a few lines below, so the workbook still reports the trail
        # the site actually shows even where the folder tree does not use it.
        crumbs = [p for p in _split_section_path(section_path)
                  if _crumb_key(p) not in self.drop_sections]
        parts = [self.regulator, self.source_system] + crumbs
        trail = _clean_trail([p for p in parts if p])
        # Appended AFTER _clean_trail, and never de-duplicated against it: a
        # law whose title repeats its folder name is still a document that has
        # to be its own leaf.
        if self.doc_path_title and title:
            trail.append(str(title).strip())
        return trail

    def _category_for(self, section_path: str) -> str:
        if self.category:
            return self.category
        parts = _split_section_path(section_path)
        return parts[-1] if parts else self.source_system

    def _doc_from_document_row(self, d: dict, shape: str) -> Optional[RegulatoryDocument]:
        url = (d.get("doc_url") or "").strip()
        if not url:
            return None
        section_path = d.get("section_path") or ""
        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self._category_for(section_path),
            title=(d.get("title") or "").strip() or url.rsplit("/", 1)[-1],
            document_url=url,
            published_date=None,          # a link walk cannot read issue dates
            source_page_url=d.get("found_on") or self.seed_url,
            file_type=d.get("type") or None,
            doc_path=self._doc_path(
                section_path,
                (d.get("title") or "").strip() or url.rsplit("/", 1)[-1]),
            content_hash=d.get("content_hash"),
            extra_meta={
                # `crawler`, `shape` and `seed_url` removed 2026-08-12: none had
                # a reader, and this is the DOCUMENT-row path, which the earlier
                # slimming of the page-row path missed — so every file row still
                # carried them.
                "section_path": section_path,
                "record_kind": "document",
            },
        )

    def _doc_from_page_row(self, r: dict, shape: str) -> Optional[RegulatoryDocument]:
        """A content page IS the document on tree sites (SAMA rulebook, CBB):
        there is no attached PDF, the regulation is the page text."""
        url = (r.get("url") or "").strip()
        text = r.get("text") or ""
        if not url:
            return None
        section_path = r.get("section_path") or ""
        title = ((r.get("title") or "").strip()
                 or (r.get("linked_from_title") or "").strip()
                 or url.rsplit("/", 1)[-1])
        # The page's own "Original PDF". The orchestrator looks for exactly this
        # key (extract_text_content_unified, tier 3) and will download + OCR it
        # when the page text is too short to analyse — which is the case for
        # every short article on a rulebook site.
        pdf = (r.get("pdf_links") or "").split(" | ")[0].strip()
        extra_pdf = {"org_pdf_link": pdf} if pdf.startswith("http") else {}
        # A targeted run walked past this row without opening it. Carried so the
        # classifier can tell "not re-read" from "read and now empty"; absent on
        # every ordinary run rather than stamped False on every document.
        extra_skip = {"detail_skipped": True} if r.get("detail_skipped") else {}
        # The listing row is the only place the reference number and issue date
        # appear on most list sites — the detail page rarely repeats them.
        row_text = r.get("row_text") or ""
        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self._category_for(section_path),
            title=title,
            document_url=url,
            published_date=_parse_row_date(row_text),
            reference_no=_parse_row_ref(row_text, title),
            source_page_url=url,
            # DERIVED, not assumed. A "page row" is a row from a listing, and a
            # listing row can link straight at a FILE — SIMAH's Implementing
            # Regulations row points at implementing-regulations.pdf, and was
            # stored as file_type HTML with no text, which reads as a broken page
            # rather than a PDF nobody has fetched yet.
            file_type=_ext_type(url) if _is_doc(url) else "HTML",
            doc_path=self._doc_path(section_path, title),
            document_html=r.get("html") or None,
            content_hash=r.get("content_hash"),
            # extra_meta is stored on every regulation row and read by people, so
            # it holds what a READER needs — not a record of how the crawl ran.
            # Removed as noise: crawler, shape, seed_url, depth, parent_page_url
            # (duplicates the source_page_url column) and row_text (used to parse
            # the date and reference above, then of no further use). None had a
            # single reader.
            #
            # `section_path` CAME BACK, 2026-09-17, BUT ONLY WHERE IT SAYS
            # SOMETHING doc_path DOES NOT.
            #
            # It was dropped here because it duplicated doc_path, and that held
            # while doc_path was built from the breadcrumb and nothing else.
            # `drop_sections` ended it: a source that names crumbs to drop is a
            # source whose tree DELIBERATELY differs from the site's trail, and
            # the trail was the half being thrown away. MEASURED on QCB: a
            # section whose only row is its page — Digital Currency is one —
            # recorded no breadcrumb at all.
            #
            # CONDITIONAL, so a source that declares nothing gets exactly the
            # extra_meta it got before. Restoring it unconditionally would have
            # added the key to 102 CBE page rows and 2 MISA ones on their next
            # export: harmless (identity is (document_url, doc_path, title) and
            # the hash reads document_html, so neither moves) but a diff in
            # someone else's workbook that nobody asked for.
            extra_meta={
                # The trail as the site draws it, crumbs this source drops
                # included. doc_path is the library's tree; this is the site's.
                **({"section_path": section_path} if self.drop_sections else {}),
                # KEEP. Tier 1b of the orchestrator's extraction reads this to
                # avoid re-fetching the page (orchestrator.py:183), and the
                # versioning path reads it to snapshot previous content
                # (formfill/orch.py:685). Dropping it would silently make every
                # analysis re-download its page.
                "content_text": text,
                # KEEP. jobs/run_regulator.py summarises a run by this.
                "record_kind": "page",
                # Blocks the form named in content.extract. They were LIFTED OUT
                # of document_html — so the stored HTML is the instrument and not
                # the site's furniture around it — and kept here under the form's
                # own key, so nothing captured is thrown away.
                # Blocks the form named in content.extract, LIFTED OUT of
                # document_html and kept here. A block read as label/value pairs
                # is flattened — `Resolution Number` becomes
                # extra_meta["resolution_number"], not a nested object nobody can
                # query — because that is the point of parsing it rather than
                # storing its markup.
                **_flatten_extracted(r.get("extracted") or {}),
                **extra_pdf,
                **extra_skip,
            },
        )

    def _want_pages(self, pages: List[dict]) -> bool:
        if self.include_pages == "always":
            return True
        if self.include_pages == "never":
            return False
        # "auto": include pages only when they carry real prose. On a table site
        # the single synthetic page row has no text and must not become a document.
        return any((p.get("text_len") or 0) >= MIN_PAGE_TEXT for p in pages)

    @staticmethod
    def _is_link_wrapper(r: dict) -> bool:
        """A page that is only a heading and a download link — not a document.

        MEASURED 2026-08-20 on CBE. `/en/governance/risk-management-information-
        security/cbe-risk-appetite-statement` is a title, a date stamp and one
        link, and CBE labelled the link with the SAME words as the heading. So
        the crawl recorded TWO documents:

            title "CBE Risk Appetite Statement"  url .../cbe-risk-appetite-statement       (the page)
            title "CBE Risk Appetite Statement"  url .../...-statement-english.pdf         (the file)

        Same title, same doc_path, so `find_existing` merged them onto one row —
        which has ONE content_hash slot and two documents checking against it.
        Whichever hash is stored, the other document mismatches, so the row
        reported `modified` on a run where nothing had changed and wrote a
        version byte-identical to its predecessor. Left alone that is one false
        change and one junk version row per run, for ever, in the report a person
        reads to find REAL changes.

        THE RULE IS RE-EVALUATED EVERY RUN, deliberately, and is not a blocklist.
        The day CBE puts real prose on that page it stops matching here and is
        recorded as a document again. A hardcoded url exclusion would make the
        page invisible for ever, which is a worse failure than the one it fixes.

        NOTHING STOPS BEING CRAWLED. `pages` (everything walked) and `documents`
        (what is recorded) are separate lists; this only decides whether a page
        graduates into the second. The page is still opened every run, so a NEW
        file appearing on it is still found and still recorded — which is the
        only way the file below was ever discovered.

        Scoped tightly on purpose:
          * `n_pdfs >= 1` — a page linking no file cannot be a wrapper around one
          * keys on RESIDUE, never on length. `MIN_LEAF_TEXT` exists because
            SAMA's "Article 3" is 184 characters of real law; a length rule would
            throw that away.
        """
        if not (r.get("n_pdfs") or 0):
            return False
        text = " ".join((r.get("text") or "").split())
        if not text:
            return False              # the length rules below already judge this
        residue = text
        title = " ".join((r.get("title") or "").split())
        if title:
            # `replace`, not a single strip: the CMS prints the heading once and
            # the link label once, and here they are the same string.
            residue = residue.replace(title, " ")
        residue = _PAGE_DATE_STAMP_RE.sub(" ", residue)
        residue = " ".join(residue.split())
        return len(residue) < WRAPPER_RESIDUE_CHARS

    @staticmethod
    def _page_is_document(r: dict) -> bool:
        """Is this page a document, or just a folder in the site's tree?

        Length alone cannot tell them apart — see MIN_LEAF_TEXT. When the walker
        reported how many children a page has, trust that: a leaf is content, a
        page with children is a folder whose content lives underneath it.
        """
        if GenericSiteCrawler._is_link_wrapper(r):
            return False
        text_len = r.get("text_len") or 0
        if text_len >= MIN_PAGE_TEXT:
            return True
        n_children = r.get("n_children")
        if n_children is not None:
            return n_children == 0 and text_len >= MIN_LEAF_TEXT
        return False

    # ------------------------------------------------------------------ #
    #  the pipeline entry point                                            #
    # ------------------------------------------------------------------ #

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        result = self._run_crawl()
        self.last_result = result
        shape = result.get("shape", "generic")
        pages = result.get("pages", []) or []
        documents = result.get("documents", []) or []

        out: List[RegulatoryDocument] = []
        # Dedupe on (url, folder path), NOT url alone. Regulators cross-list one
        # document under several sections and each listing is its own place in the
        # library — the crawler now keys documents the same way, and the DB agrees
        # (document_exists_by_url is category-scoped). Deduping on url here would
        # silently collapse those back into one.
        seen = set()

        def _add(doc):
            if not doc:
                return
            key = (doc.document_url, " > ".join(doc.doc_path or []))
            if key not in seen:
                seen.add(key)
                out.append(doc)

        for d in documents:
            _add(self._doc_from_document_row(d, shape))

        if self._want_pages(pages):
            for r in pages:
                if not self._page_is_document(r):
                    continue                      # folder/index page, not a document
                _add(self._doc_from_page_row(r, shape))

        logger.info(
            "GenericSiteCrawler[%s/%s] shape=%s -> %d documents "
            "(%d file links, %d content pages) from %s",
            self.regulator, self.source_system, shape, len(out),
            len(documents), len(out) - len(documents), self.seed_url,
        )
        return out[:limit] if limit else out


class CompositeCrawler:
    """Runs several sources for one regulator and joins the results.

    Same shape as SAMACombinedCrawler, which already concatenates three different
    crawlers into one list — generalised so the sources can be a mix of generic
    crawlers and hand-written ones. Any object with fetch_documents() works.

    A failing source is logged and skipped; it must not take the others down.

    It also stamps each document with the settings of the source that produced
    it. This loop is the only place that knows which source that was: the
    documents leave here as one flat list, and a custom source can write under
    several source_systems (SAMACombinedCrawler is three crawlers).
    """

    def __init__(self, crawlers: List[object], options: Optional[List[dict]] = None):
        self.crawlers = crawlers
        # One dict per crawler — name, identity, version_key. A missing key means
        # the regulator's default, so empty dicts are the old behaviour exactly.
        self.options = list(options or [{} for _ in crawlers])

    def _label(self, i: int) -> str:
        return (self.options[i].get("name")
                or getattr(self.crawlers[i], "seed_url", None)
                or type(self.crawlers[i]).__name__)

    @property
    def source_names(self) -> List[str]:
        """The sources this was built with, whether or not they produced anything.
        A source that returned nothing is the case worth being able to see."""
        return [self._label(i) for i in range(len(self.crawlers))]

    @property
    def source_systems(self) -> List[str]:
        """Every source_system these sources write under. The completeness gate
        needs all of them: one composite can hold several."""
        out = []
        for c in self.crawlers:
            for s in (getattr(c, "source_systems", None)
                      or [getattr(c, "source_system", None)]):
                if s and s not in out:
                    out.append(s)
        return out

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        docs: List[RegulatoryDocument] = []
        for i, c in enumerate(self.crawlers):
            label, opts = self._label(i), self.options[i]
            try:
                got = c.fetch_documents() or []
                for d in got:
                    meta = dict(getattr(d, "extra_meta", None) or {})
                    meta["crawl_source"] = label
                    if "identity" in opts:
                        meta["identity_fields"] = opts["identity"]
                    if "version_key" in opts:
                        meta["version_key"] = opts["version_key"]
                    d.extra_meta = meta
                docs.extend(got)
                logger.info("  source ok: %s -> %d documents", label, len(got))
            except Exception as e:
                logger.error("  source FAILED: %s -> %s", label, e, exc_info=True)
        return docs[:limit] if limit else docs


def _doc_path_title_for(cfg: dict) -> bool:
    """`doc_path_title`, unless this branch can only honour HALF of what the
    source asked for.

    THE PAIR IS SPLIT ACROSS BRANCHES. `doc_path_title` (end the trail at the
    document) and `doc_path_sections` (whether the breadcrumb goes into the trail
    at all) were written together. Only the first landed here; nothing on this
    branch reads `doc_path_sections`, and three configs merged from
    abeeraslam-compliance set it -- sio.yml sets BOTH, on all ten of its sources.

    Honouring `doc_path_title` alone there would give SIO

        SIO | <system> | <crumb> ... | <law title>

    when its config asked for `SIO | <system> | <law title>` -- a THIRD shape,
    neither what is stored today nor what the file requests. doc_path is an
    identity field, so that is not a cosmetic difference: it re-files every row,
    and the next crawl reads all of them as `new` plus all the stored ones as
    `disappeared`, which is what feeds the withdrawal gate.

    So a source asking for both gets NEITHER, keeping its output byte-identical
    to today, and says so once per build. Delete this function when
    `doc_path_sections` lands -- it exists only while the pair is half here.
    """
    if not cfg.get("doc_path_title"):
        return False
    if "doc_path_sections" in cfg:
        logger.warning(
            "source %r asks for doc_path_title AND doc_path_sections; this "
            "branch implements only the first, and applying it alone would "
            "move every doc_path this source has stored. Both ignored -- port "
            "doc_path_sections before turning this on.", cfg.get("name"))
        return False
    return True


def build_source(cfg: dict):
    """Turn ONE source entry from a regulator's YAML into a crawler object.

        mode: generic  -> GenericSiteCrawler (shared code, no per-site python)
        mode: custom   -> import and instantiate the named class

    Both come back with the same fetch_documents(), which is the whole point:
    whether a source is generic or hand-written stops mattering above this line.
    """
    mode = (cfg.get("mode") or "generic").lower()

    if mode == "custom":
        path = cfg.get("crawler_class")
        if not path or "." not in path:
            raise ValueError(f"source '{cfg.get('name')}': mode=custom needs "
                             f"crawler_class like 'crawler.x_wrapper.XCrawler'")
        module_name, _, cls_name = path.rpartition(".")
        import importlib
        cls = getattr(importlib.import_module(module_name), cls_name)
        return cls(**(cfg.get("init_kwargs") or {}))

    if mode != "generic":
        raise ValueError(f"source '{cfg.get('name')}': unknown mode '{mode}'")

    missing = [k for k in ("seed_url", "regulator", "source_system")
               if not cfg.get(k)]
    if missing:
        raise ValueError(f"source '{cfg.get('name')}': missing {missing}")

    return GenericSiteCrawler(
        seed_url=cfg["seed_url"],
        regulator=cfg["regulator"],
        source_system=cfg["source_system"],
        category=cfg.get("category"),
        exclude_paths=cfg.get("exclude") or [],
        drop_sections=cfg.get("drop_sections"),
        doc_path_title=_doc_path_title_for(cfg),
        scope=cfg.get("scope", "auto"),
        max_pages=int(cfg.get("max_pages", 150)),
        max_depth=int(cfg.get("max_depth", 8)),
        out_dir=cfg.get("out_dir"),
        include_pages=cfg.get("include_pages", "auto"),
        wait_ms=(int(cfg["wait_ms"]) if cfg.get("wait_ms") else None),
    )


def build_regulator_crawler(config: dict, only_sources=None):
    """Build the crawler for a whole regulator from its loaded YAML.

    A regulator is a LIST of sources, each independently generic or custom, so
    one regulator can mix both — e.g. SAMA's rulebook sectors on the generic
    engine while its circulars keep a tuned runner.
    """
    regulator = config.get("regulator")
    if not regulator:
        raise ValueError("config has no 'regulator'")
    # A regulator that is off on purpose says so. An empty list cannot be told
    # from an unfinished file, and it reads as working wherever configs are
    # listed rather than run.
    off = config.get("disabled")
    if off:
        raise ValueError(f"{regulator}: disabled on purpose. {off}")
    sources = config.get("sources") or []
    if not sources:
        raise ValueError(f"{regulator}: config lists no sources. If that is "
                         f"deliberate, say so in `disabled:`")

    # RUN ONLY SOME OF A REGULATOR'S SOURCES, by `name`.
    #
    # LLOC is why: config/sources/lloc.yml holds four sources of very different
    # cost -- "Latest Legislation" is 144 records in ~40 seconds, while
    # "Legislation By Classification" is 1,583 documents over 2,838 seconds. A
    # nightly job wants the first and not the third.
    #
    # DANGEROUS WHERE SOURCES SHARE A source_system. `disappeared` is scoped on
    # (regulator, source_system), so running a subset of sources that share one
    # leaves the others' stored documents absent from a run that still claims
    # that bucket -- and only the completeness gate stands between that and a
    # withdrawal proposal. SIO is exactly that shape (ten sources, two
    # source_systems) and deliberately does NOT narrow. Use this only where the
    # narrowed sources own their source_system outright.
    if only_sources:
        want = {str(x).strip().lower() for x in only_sources}
        kept = [s for s in sources if str(s.get("name", "")).strip().lower() in want]
        missing = want - {str(s.get("name", "")).strip().lower() for s in sources}
        if missing:
            raise ValueError(
                f"{regulator}: only_sources names {sorted(missing)}, which this "
                f"config does not define. Known: "
                f"{sorted(str(s.get('name')) for s in sources)}")
        sources = kept

    # A REGULATOR-LEVEL `exclude:` APPLIES TO EVERY SOURCE, and a source may add
    # its own. Both lists are used; neither replaces the other.
    #
    # MEASURED 2026-08-26 on CBE: the exclude was first written per-source, on
    # `Laws and Regulations`, because that is where the duplication was reported.
    # The re-export then leaked 11 circular pdfs through `Payment Systems and
    # Services` instead -- 9 of them files the Circulars API source already held,
    # and 3 pairs pointing at ONE file titled after whichever page linked it
    # ("Payment Cards", "Mobile Wallets", "Introduction").
    #
    # A subtree owned by another source is a fact about the REGULATOR, not about
    # one of its sources: any section page may link into it. Scoping the rule to
    # the source where the problem was first noticed fixes the example and leaves
    # the class.
    regulator_exclude = list(config.get("exclude") or [])

    built, options = [], []
    for src in sources:
        merged = dict(src)
        merged.setdefault("regulator", regulator)
        if regulator_exclude:
            merged["exclude"] = regulator_exclude + list(merged.get("exclude") or [])
        built.append(build_source(merged))
        options.append(_source_options(src, config))
    return CompositeCrawler(built, options)


def _source_options(src: dict, config: dict) -> dict:
    """Per-source settings for the orchestrator, falling back to the regulator's.

    What counts as "the same document" belongs to the SOURCE: one regulator can
    hold a grid with a reference number and a link walk without one, and a single
    identity for the whole file cannot serve both. `version_key` is looked up with
    `in` rather than `or` because `version_key: null` disables the new-url
    tiebreak, which is a different instruction from not mentioning it.
    """
    opts = {"name": src.get("name")}
    identity = src.get("identity") or config.get("identity")
    if identity:
        opts["identity"] = identity
    for level in (src, config):
        if "version_key" in level:
            opts["version_key"] = level["version_key"]
            break
    return opts


__all__ = ["GenericSiteCrawler", "CompositeCrawler",
           "build_source", "build_regulator_crawler"]
