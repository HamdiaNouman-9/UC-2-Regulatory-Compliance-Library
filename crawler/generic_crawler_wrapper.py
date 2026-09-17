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

import hashlib
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


#: A trailing language label the SITE added to the folder trail, not part of the
#: instrument's place in the library.
#:
#: MEASURED on cbb.gov.bh mode 1: "Resolution No. (34) for the year 2010 ..." is
#: published twice, and the only difference between the two trails is a final
#: crumb of "(Arabic)" or "(English)". Left in place the two copies land in two
#: folders and can never merge; the pair is one instrument in two languages, the
#: same as every other pair in that source.
_LANG_SUFFIX = re.compile(
    r"\s*[\(\[]\s*(arabic|english|عربي|عربى|ar|en)\s*[\)\]]\s*$", re.I)


def _strip_lang(text: str) -> str:
    """Drop a trailing "(Arabic)" / "(English)" label.

    MEASURED on cbb.gov.bh mode 1: the label trails BOTH the title and the last
    doc_path crumb --

        Resolution No. (34) ... Unrestricted Investment Accounts (Arabic)
        Resolution No. (34) ... Unrestricted Investment Accounts (English)

    -- so it is a suffix, not a crumb of its own, and stripping it is what lets
    the two copies group together. Never returns empty: a crumb that is ONLY the
    label keeps its original text rather than vanishing from the trail.
    """
    t = str(text or "")
    out = _LANG_SUFFIX.sub("", t).strip()
    return out or t.strip()


def _trail_for_merge(doc_path):
    """The folder trail with the language label off its last crumb."""
    trail = [str(x) for x in (doc_path or [])]
    if trail:
        trail[-1] = _strip_lang(trail[-1])
    return tuple(trail)


def merge_files_at_same_path(docs: List[RegulatoryDocument]) -> List[RegulatoryDocument]:
    """Rows sharing (doc_path, title) become one C19 multi-attachment row.

    THE FILE ORDER IS SORTED, NOT THE SITE'S. C19 exists because naming a row by
    whichever file the site listed first makes identity depend on the site's
    ordering; identity here is the SET of files, so the set is written in a
    stable order and a re-ordered page cannot move it.

    A group of one is left exactly as it was -- a single-file source keeps
    `document_url`, which is the whole point of the rule being per-group.

    A trailing "(Arabic)" / "(English)" crumb is ignored when grouping and
    dropped from the surviving row's trail: see `_LANG_CRUMB`.

    MODULE-LEVEL, not a method, because `build_source` returns early for
    `mode: custom` and so a custom source can reach no wrapper key at all.
    CompositeCrawler calls this for those; GenericSiteCrawler still calls it
    itself, so a source built standalone by `build_source`
    (benchmarks/run_source_standalone.py) behaves exactly as before.
    """
    groups: dict = {}
    for d in docs:
        groups.setdefault(
            (_trail_for_merge(d.doc_path), _strip_lang(d.title)), []).append(d)

    merged: List[RegulatoryDocument] = []
    for (_path, _title), members in groups.items():
        if len(members) == 1:
            merged.append(members[0])
            continue
        base = members[0]
        # EVERY FILE OF EVERY MEMBER, not just their document_urls. A member may
        # ALREADY be a multi-file row -- 6 of CBB mode 1's 71 rows arrive from
        # the crawler with attachment_links and no document_url -- and an earlier
        # version of this replaced that field, silently dropping those files.
        urls = set()
        for m in members:
            if m.document_url:
                urls.add(m.document_url)
            prev = (m.extra_meta or {}).get("attachment_links")
            if isinstance(prev, str):
                urls.update(x.strip() for x in prev.split("|") if x.strip())
            elif isinstance(prev, (list, tuple)):
                urls.update(str(x).strip() for x in prev if str(x).strip())
        urls = sorted(urls)
        links = " | ".join(urls)
        meta = dict(base.extra_meta or {})
        meta["attachment_links"] = links
        # Declared, not defaulted: document_url is a third of the default
        # identity and is about to be empty, which would collapse every row in
        # one folder onto the same key.
        meta["identity_fields"] = ["doc_path", "extra_meta.attachment_links",
                                   "title"]
        meta["record_kind"] = "multi-attachment"
        # What was fused, so a reader can tell a merged row from a single-file
        # one without inferring it from the empty url.
        meta["merged_from"] = len(members)
        base.extra_meta = meta
        base.document_url = ""
        # The surviving row must not claim to be the Arabic one.
        base.title = _title
        # The trail without the language crumb, so the surviving row does not
        # claim to be the Arabic one.
        base.doc_path = list(_path)
        # The row's hash has to describe the row, and the row is now a SET of
        # files. Inheriting one member's hash would leave it unchanged when a
        # different member was replaced.
        base.content_hash = hashlib.md5(
            ("|".join(_path) + "|" + links).encode("utf-8")).hexdigest()
        types = {m.file_type for m in members if m.file_type}
        base.file_type = types.pop() if len(types) == 1 else None
        logger.info("  merged %d files into one row: %s",
                    len(urls), " > ".join(_path))
        merged.append(base)
    if len(merged) != len(docs):
        logger.info("  merge_files_at_same_path: %d rows -> %d",
                    len(docs), len(merged))
    return merged


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
        wait_ms: Optional[int] = None,    # per-site JS settle time
        in_process: bool = False,
        timeout: int = 3600,
        strategy: str = "auto",
        exclude_documents: Optional[List[str]] = None,
        doc_path_sections: bool = True,
        min_page_text: Optional[int] = None,
        doc_path_title: bool = False,
        doc_path_category: bool = False,
        placeholder_when_empty: Optional[str] = None,
        drop_sections: Optional[List[str]] = None,
        merge_files_at_same_path: bool = False,
        uncategorised_parents: Optional[dict] = None,
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
        # How long to let JavaScript settle before reading the page. The engine
        # has always accepted --wait-ms; nothing passed it, so every site got the
        # default. ZATCA's landing page renders its links client-side and read at
        # the default returns text_len=22 and queues NOTHING — the crawl visits
        # the seed, finds no links worth following and stops at one page.
        self.wait_ms = wait_ms
        self.in_process = in_process
        self.timeout = timeout
        # WHICH WALKER READS THIS LAYOUT. The engine has always accepted
        # --strategy; nothing passed it, so every source ran with shape
        # auto-detection and no config could override the answer. Same class of
        # gap as wait_ms above, and it fails the same way: silently.
        #
        # rera.gov.bh/en/regulations/resolutions detects as `list`, so
        # crawl_list() runs and returns 4 of its 33 documents while reporting a
        # clean run. Seeding the section's PARENT instead gets `generic` and all
        # 33 -- which is why the count changed when rera.yml split the sections.
        # sio.gov.bh is worse: every one of its content pages detects as `list`
        # and returns 0 pages with status `zero`.
        #
        # Default "auto" is exactly today's behaviour, and no existing config
        # sets the key, so MISA, SECP, SBP, ZATCA, MC and CMA are unaffected.
        self.strategy = strategy
        # SITE-WIDE FURNITURE THE CHROME RULE CANNOT SEE. JS_LINKS marks a link
        # `chrome` only when it sits inside <header>/<footer>, deliberately, so a
        # real in-page sidebar is not discarded. A site that links the same
        # corporate PDF from the BODY of every page therefore slips through.
        #
        # MEASURED on rera.gov.bh 2026-08-20: Vision2030Englishlowresolution.pdf
        # and the Cloud First Policy pdf appear on all eight legislation pages, so
        # the workbook held them 8 times each -- 16 of its 150 rows for two
        # documents, neither of which is a real-estate regulation. `check` passes
        # them because they differ by doc_path, which is exactly the "shape, not
        # sense" gap ONBOARDING warns about.
        #
        # Substring match against the document url, per source. Empty by default,
        # so no existing config changes.
        self.exclude_documents = [str(x) for x in (exclude_documents or []) if str(x).strip()]
        # WHETHER THE CRAWLER'S BREADCRUMB BECOMES FOLDERS BELOW source_system.
        #
        # It usually should. But a site whose breadcrumb names the PARENT of the
        # section inverts the tree: rera.gov.bh detail pages carry
        # "Home > Regulations", so every document landed at
        #   RERA | RERA Law And Decrees | Regulations | <title>
        # when the site's own hierarchy is Regulations -> Laws and Decrees. The
        # library then shows a folder called Regulations INSIDE each section,
        # which is the "a folder is upside down" case ONBOARDING says only a
        # person can spot. Default True keeps every existing source unchanged.
        self.doc_path_sections = bool(doc_path_sections)
        # HOW MUCH TEXT MAKES A PAGE A DOCUMENT, per source.
        #
        # MIN_PAGE_TEXT is 200 and that is right for a site whose thin pages are
        # navigation. It is wrong for one whose real records are deliberately
        # short. MEASURED 2026-08-25 on sio.gov.bh, whose laws are Bootstrap
        # modals captured as <page>#<id>:
        #
        #   private-sectors   94 modal records ->  69 pass 200 chars
        #   public-sectors   108 modal records ->  69 pass 200 chars
        #
        # 64 of 202 dropped, because 56 and 66 of them read only "This content
        # will be published soon". The ENGINE keeps those on purpose — the title
        # is a real law, SIO listing one it has not published yet is a fact worth
        # holding, and the day it publishes the text changes and the row reports
        # `modified`. This layer then threw them away for being short, and said
        # nothing. At 50 every modal survives (the shortest measured 62 chars).
        #
        # The leaf fallback below cannot rescue them: SIO reports `n_children` on
        # 0 of 202 pages, so it never fires.
        self.min_page_text = (int(min_page_text) if min_page_text is not None
                              else MIN_PAGE_TEXT)
        # WHETHER THE TITLE IS THE LAST CRUMB OF doc_path.
        #
        # It should be — ONBOARDING says doc_path is `[regulator, source_system,
        # ...folders, title]` and "the last crumb is the document itself", and
        # `_walk_folders` types that last segment as the regulation rather than a
        # folder. Hand-written crawlers (MOH, CBE, LLOC) all append it. This
        # class never has, so every document in a folder claimed the FOLDER as
        # its leaf, and the leaf rule then gave each one its own same-named
        # sibling: measured in a CBE workbook as 658 folder rows for 502 distinct
        # paths, e.g. `Banking Laws` created ten times.
        #
        # Default False because doc_path is part of the DEFAULT IDENTITY
        # (document_url, doc_path, title): turning it on for an existing source
        # re-identifies every stored row, which reads as every document new and
        # every old one disappeared. Safe to enable on a source that has never
        # been promoted.
        self.doc_path_title = bool(doc_path_title)
        # THE CATEGORY AS A FOLDER, not just a column.
        #
        # `category` has always been a FIELD on RegulatoryDocument and nothing
        # more: _doc_path() builds [regulator, source_system] + sections, and the
        # orchestrator builds the folder tree from doc_path alone. So a source
        # configured with `category: "SMEs"` put that label in a column no tree
        # ever reads, and its documents landed beside every other source's.
        #
        # MEASURED on moic.gov.bh 2026-08-31: the eight `?tag=` sources exist
        # only to attach the site's eight sidebar labels, and every one of their
        # documents came back at doc_path [MOIC, Forms, <title>] — the same trail
        # the unfiltered Forms source produces, which is why that config carried
        # ten duplicate rows and no sidebar level at all.
        #
        # Opt-in, because turning it on for every source would insert a level
        # into every regulator that sets `category` — and `_category_for` falls
        # back to the last section crumb or the source_system when none is
        # configured, so most sources would gain a folder that repeats their
        # parent.
        self.doc_path_category = bool(doc_path_category)
        # SECTION CRUMBS THIS SOURCE MUST NOT TURN INTO A FOLDER.
        #
        # `_clean_trail` already drops a crumb that repeats one earlier in the
        # trail, which is why a page whose <h1> matches its source_system never
        # showed that <h1> as a level. Renaming a source breaks that by accident.
        #
        # MEASURED on moic.gov.bh/en/regulations?about[0]=19: the breadcrumb is
        # EMPTY, and section_path still reads "Regulations > Commerce Law" --
        # the first crumb is the page's own <h1>, which `group_headings` reads as
        # the outermost heading. While source_system was "Regulations" the dedupe
        # hid it; naming the source after the site's Commerce / Industry filter
        # exposed it and the tree grew a "Regulations" level under Commerce.
        #
        # Matched case-insensitively on the whole crumb, so it cannot silently
        # eat a longer name that merely contains the word.
        # THE PARENT OF AN ORPHAN SUBCATEGORY, DECLARED BECAUSE THE SITE OMITS IT.
        #
        # `{subcategory: category}`. The engine marks a document it could discover
        # no category for — see `link_title_is_section` in crawler.py: a page's
        # category comes from the anchor that led to it, and the seed has no such
        # anchor. This supplies the missing category for those rows only.
        #
        # MEASURED on moic.gov.bh/en/forms: one form sits under an <H2> "Consumer
        # Protection" that no `?tag=` returns — swept tag=316..330, the 7 ids
        # outside the rail return nothing and no tag in the range returns that
        # form. The <H2> is the same markup as "Companies Control" and "Precious
        # Metals Assay Centre", which ARE subcategories, so it is a subcategory
        # whose parent went missing, not a ninth category.
        #
        # The library's manual tree files it under "Consumer Services" — which is
        # in the rail, and which the site currently reports as empty. So the site
        # is wrong and the declared mapping is the correction.
        #
        # WHY DECLARING IT IS SAFE HERE. It applies ONLY to rows the engine could
        # not place, so the day MOIC tags that form the discovered category wins
        # and this goes inert rather than conflicting. And it lives in the
        # regulator's yml, so no subcategory name is baked into shared code.
        #
        # An unmapped orphan is left where it lands, which is visible rather than
        # silent: it appears at category level and looks like a new category,
        # which is the prompt to either map it or find out why the site dropped it.
        self.uncategorised_parents = {
            str(k).strip().lower(): str(v).strip()
            for k, v in (uncategorised_parents or {}).items()
            if str(k).strip() and str(v).strip()}
        self.drop_sections = {str(x).strip().lower()
                              for x in (drop_sections or []) if str(x).strip()}
        # SEVERAL FILES AT ONE PLACE IN THE TREE ARE ONE DOCUMENT.
        #
        # The sanctioned multi-file shape, from docs/final_changes.md C19:
        #     document_url                 ""     <- deliberately EMPTY
        #     extra_meta.attachment_links  "<pdf> | <pdf>"
        #     extra_meta.identity_fields   ["doc_path",
        #                                   "extra_meta.attachment_links", "title"]
        # `document_url` is half the default identity, and leaving it empty
        # COLLAPSES that identity -- every row in one folder would key on
        # ("", doc_path) -- so a merged row must declare its own. That is done
        # here rather than left to config, because a row that leaves the url
        # empty without declaring identity is broken, not configurable.
        #
        # MEASURED on pdp.gov.bh/en/executive-decisions.html: ten decisions, each
        # an Arabic original plus an English translation. No two urls in a folder
        # are identical, and after `heading_is_title` both files of a decision
        # share a title AND a doc_path -- so the folder IS the group and nothing
        # has to guess at languages.
        #
        # Opt-in, because on any other source two files at one path are two
        # documents that happen to collide, not one document in two formats.
        # lloc.gov.bh is the worked counter-example: docs/changes_2026-08-25.md
        # CHANGE 29 deliberately keeps the pdf canonical there instead.
        self.merge_files_at_same_path = bool(merge_files_at_same_path)
        # A FOLDER FOR A CATEGORY THAT PUBLISHES NOTHING.
        #
        # Folders exist only as a side effect of a document's doc_path
        # (_get_or_create_compliance_category in orchestrator.py), so a source
        # that returns no documents contributes no folder and vanishes from the
        # tree. For a filtered listing that is wrong in a specific way: it cannot
        # be told apart from a category the site never had.
        #
        # MEASURED on moic.gov.bh 2026-08-31: six of the eight form categories
        # publish NOTHING — tag=324 (SMEs) and tag=319 (eCommerce) return zero
        # characters of captured content and zero links. The single "document"
        # each one appeared to have was the site-wide Vision 2030 pdf, which
        # `exclude_document_urls` now drops. So without this the tree would show
        # two form categories where the ministry publishes eight.
        #
        # The value is the placeholder's TITLE, and setting it is what turns this
        # on. Same precedent as SIO's "will be published soon" panels, which are
        # kept for the same reason.
        self.placeholder_when_empty = (str(placeholder_when_empty).strip()
                                       if placeholder_when_empty else None)
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
                  max_depth=self.max_depth, scope=self.scope,
                  strategy=self.strategy, exclude_paths=self.exclude_paths, **kw)
        else:
            cmd = [sys.executable, str(ENGINE),
                   "--url", self.seed_url, "--out", str(out),
                   "--scope", self.scope,
                   "--max-pages", str(self.max_pages),
                   "--max-depth", str(self.max_depth),
                   "--strategy", self.strategy]
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

    def _merge_same_path(self, docs: List[RegulatoryDocument]) -> List[RegulatoryDocument]:
        """Delegates to the module function, which CompositeCrawler shares."""
        return merge_files_at_same_path(docs)

    def _doc_path(self, section_path: str, title: str = "",
                  source_system: Optional[str] = None) -> List[str]:
        """Folder trail for the library.

        ALWAYS starts with the regulator. _get_or_create_compliance_category()
        builds the folder tree from this list, so two regulators that both use a
        top-level folder called "Circulars" would otherwise merge into one node
        and tangle their documents together.
        """
        parts = [self.regulator, source_system or self.source_system]
        # The category sits between the source system and the page's own
        # sections, which is where the site puts it: a sidebar filter is a level
        # ABOVE the headings on the filtered page. `_clean_trail` still drops it
        # if it repeats the source system.
        if self.doc_path_category and self.category:
            parts.append(self.category)
        if self.doc_path_sections:
            parts += [p for p in _split_section_path(section_path)
                      if p.strip().lower() not in self.drop_sections]
        trail = _clean_trail([p for p in parts if p])
        # Appended AFTER _clean_trail, and never de-duplicated against it: a law
        # whose title repeats its folder name ("Ministerial Orders") is still a
        # document that has to be its own leaf.
        if self.doc_path_title and title:
            trail.append(str(title).strip())
        return trail


    def _category_for(self, section_path: str) -> str:
        if self.category:
            return self.category
        parts = _split_section_path(section_path)
        return parts[-1] if parts else self.source_system

    def _excluded(self, url: str) -> bool:
        """True if this url is configured-out furniture. Logged, never silent:
        a rule that removes documents has to be auditable."""
        for pat in self.exclude_documents:
            if pat in url:
                logger.info("  excluded by exclude_documents (%r): %s", pat, url)
                return True
        return False

    def _doc_from_document_row(self, d: dict, shape: str) -> Optional[RegulatoryDocument]:
        url = (d.get("doc_url") or "").strip()
        if url and self._excluded(url):
            return None
        if not url:
            return None
        section_path = d.get("section_path") or ""
        # Hoisted out of the constructor call: `doc_path` needs the same title,
        # and computing it twice is how the two drift.
        title = (d.get("title") or "").strip() or url.rsplit("/", 1)[-1]
        # A row the site filed under nothing gets its declared parent, so it
        # sits at subcategory depth like every other subcategory instead of
        # level with the real categories. See `uncategorised_parents`.
        sec = section_path
        if d.get("uncategorised") and self.uncategorised_parents:
            crumbs = _split_section_path(sec)
            for i, c in enumerate(crumbs):
                parent = self.uncategorised_parents.get(c.strip().lower())
                if parent:
                    logger.info("  uncategorised %r -> declared parent %r",
                                c.strip(), parent)
                    crumbs.insert(i, parent)
                    break
            sec = " > ".join(crumbs)
        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self._category_for(section_path),
            title=title,
            document_url=url,
            published_date=None,          # a link walk cannot read issue dates
            source_page_url=d.get("found_on") or self.seed_url,
            file_type=d.get("type") or None,
            doc_path=self._doc_path(sec, title),
            content_hash=d.get("content_hash"),
            extra_meta={
                # `crawler`, `shape` and `seed_url` removed 2026-08-12: none had
                # a reader, and this is the DOCUMENT-row path, which the earlier
                # slimming of the page-row path missed — so every file row still
                # carried them.
                "section_path": sec,
                # The engine's per-page placeholder, carried through so a reader
                # (and the prune below) can tell it from a real document.
                "record_kind": ("placeholder" if d.get("placeholder")
                                else "document"),
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
            # (duplicates the source_page_url column), section_path (duplicates
            # doc_path) and row_text (used to parse the date and reference above,
            # then of no further use). None had a single reader.
            extra_meta={
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
        return any((p.get("text_len") or 0) >= self.min_page_text for p in pages)

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

    def _page_is_document(self, r: dict) -> bool:
        """Is this page a document, or just a folder in the site's tree?

        Length alone cannot tell them apart — see MIN_LEAF_TEXT. When the walker
        reported how many children a page has, trust that: a leaf is content, a
        page with children is a folder whose content lives underneath it.
        """
        if GenericSiteCrawler._is_link_wrapper(r):
            return False
        text_len = r.get("text_len") or 0
        if text_len >= self.min_page_text:
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

        if self.merge_files_at_same_path:
            out = self._merge_same_path(out)

        # NOTHING PUBLISHED HERE, AND THAT IS WORTH RECORDING. See
        # `placeholder_when_empty` in __init__ for why an empty source would
        # otherwise disappear from the tree entirely.
        if not out and self.placeholder_when_empty:
            trail = [self.regulator, self.source_system]
            if self.category:
                trail.append(self.category)
            trail = _clean_trail([p for p in trail if p])
            # THE PLACEHOLDER IS THE LEAF, NOT THE CATEGORY. `_walk_folders`
            # types the LAST doc_path segment "R" -- the frontend's marker for
            # "this node is a document, not a folder". Without the title
            # appended here the trail ended at the CATEGORY, so the category
            # itself was typed R and the section stopped being a folder.
            #
            # MEASURED on the 2026-09-16 CBJ export: seven of ten listings under
            # "Legislation" came back as folders and three -- Instructions,
            # Jordanian Constitution, AntiMoney Laundering -- as documents
            # sitting beside them, each with no children, and each labelled with
            # the CATEGORY name while the row it stood for carried the longer
            # placeholder title. Appending the title gives the same shape every
            # other source gets from `doc_path_title`: the category stays a
            # folder and the notice is a row inside it.
            trail = trail + [self.placeholder_when_empty]
            logger.info("  no documents: adding the placeholder row at %s",
                        " > ".join(trail))
            out.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self._category_for(""),
                title=self.placeholder_when_empty,
                # The category's own page. Honest, and distinct per category, so
                # the (document_url, doc_path, title) identity separates the
                # placeholders instead of collapsing them into one row.
                document_url=self.seed_url,
                published_date=None,
                source_page_url=self.seed_url,
                file_type=None,
                doc_path=trail,
                # STABLE, and it has to be. `check` refuses a row with no
                # content_hash: it classifies `modified` on every run and writes
                # a version row each time, so six standing-still folders would
                # have grown a version history. Nothing here has text or bytes to
                # hash, so hash what identifies the row -- the same fallback the
                # engine uses for a document it has not downloaded
                # (generic_crawler/crawler.py, `<url>|<title>`).
                content_hash=hashlib.md5(
                    re.sub(r"\s+", " ",
                           "%s|%s" % (self.seed_url, self.placeholder_when_empty))
                    .strip().lower().encode("utf-8")).hexdigest(),
                extra_meta={"section_path": "",
                            # So a reader can tell this from a real document
                            # without matching on the title text.
                            "record_kind": "placeholder"},
            ))

        # A PLACEHOLDER THAT TURNED OUT TO BE WRONG. `empty_page_placeholder`
        # fires per page, before anything knows about `uncategorised_parents`, so
        # a category the site reports as empty can still end up holding a real
        # document once the declared parent is applied — and then the folder both
        # holds a form and says it publishes none. Keep a placeholder only while
        # nothing real lives at or below its folder.
        marks = [d for d in out
                 if (getattr(d, "extra_meta", None) or {}).get("record_kind")
                 == "placeholder"]
        if marks:
            real = [tuple(d.doc_path or []) for d in out if d not in marks]
            drop = []
            for m in marks:
                folder = tuple((m.doc_path or [])[:-1]) if self.doc_path_title \
                    else tuple(m.doc_path or [])
                if folder and any(r[:len(folder)] == folder for r in real):
                    drop.append(m)
            if drop:
                for m in drop:
                    logger.info("  placeholder dropped, folder is not empty: %s",
                                " > ".join(m.doc_path or []))
                out = [d for d in out if d not in drop]

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
                # THE MERGE, FOR SOURCES THAT CANNOT CONFIGURE IT THEMSELVES.
                #
                # `build_source` returns at `mode: custom` before any wrapper key
                # is read, so a custom source -- all seven of CBB's -- could never
                # set `merge_files_at_same_path`. Applied here it is configured in
                # one place for both kinds of source.
                #
                # `not getattr(...)` is what stops it running TWICE: a
                # GenericSiteCrawler with the flag on has already merged in its
                # own fetch_documents, and a second pass would see rows whose
                # document_url is already empty and fuse them into one row with
                # no files at all.
                #
                # AFTER the identity stamping above, so a merged row keeps the
                # identity the merge declared for it. A source that sets both
                # `identity` and this flag would otherwise lose the declaration
                # that stops every row in a folder sharing one key.
                if (opts.get("merge_files_at_same_path")
                        and not getattr(c, "merge_files_at_same_path", False)):
                    got = merge_files_at_same_path(got)
                docs.extend(got)
                logger.info("  source ok: %s -> %d documents", label, len(got))
            except Exception as e:
                logger.error("  source FAILED: %s -> %s", label, e, exc_info=True)
        return docs[:limit] if limit else docs


class DeclaredDocumentsSource:
    """A source whose documents are NAMED IN THE CONFIG, not discovered.

    Some regulators publish one instrument and no index worth walking: a single
    PDF, linked from a page that is otherwise a brochure. Pointing the generic
    crawler at it means launching a browser to walk a site in order to find a
    file we can already name, and every crawl is then one more chance for the
    walk to pick up a cookie banner or miss the file behind a redirect.

    The engine already has the answer, on the CLI:

        --documents "<section> :: <title> :: <url>"

    `generic_crawler.crawler.collect_declared` parses those, fingerprints each
    one and returns rows in the SAME shape the document rows of a real crawl
    have. There was no yml equivalent -- config/sources/cbe.yml says so where it
    has to hand-wave a PDF it cannot reach -- so this is that equivalent.

    WHAT IT BUYS OVER A HAND-WRITTEN WRAPPER. `stamp_declared` asks the server
    for an ETag or Last-Modified and fingerprints on it, falling back to
    `url|title` only when neither is offered -- and it records WHICH in
    `hash_basis`, so a weak fingerprint is visible rather than assumed. That is
    ONBOARDING's second-preference fingerprint (a publisher's own change stamp)
    for free, and it is what lets a one-document source still notice a revision.

    IT IS DELIBERATELY NOT BUILT ON GenericSiteCrawler. There is no crawl here,
    so there is nothing to inherit but a `seed_url` this source does not have.
    """

    def __init__(
        self,
        regulator: str,
        source_system: str,
        documents: List[str],
        category: Optional[str] = None,
        name: Optional[str] = None,
    ):
        if not documents:
            raise ValueError(
                f"source '{name or source_system}': mode=declared needs a "
                f"`documents:` list. A declared source with nothing declared "
                f"would report zero documents and read as a working crawl.")
        self.regulator = regulator
        self.source_system = source_system
        self.category = category or source_system
        self.documents = [str(d) for d in documents]
        self.name = name or source_system
        self.last_result: dict = {}

    @property
    def source_systems(self) -> List[str]:
        return [self.source_system]

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        # Imported here, not at module scope: `crawler.fingerprint` pulls in
        # `generic_crawler.crawler` at import time, and merely READING a config
        # should not pay for the crawl engine. `_run_crawl` defers its import
        # for the same reason.
        sys.path.insert(0, str(REPO_ROOT))
        from crawler.fingerprint import stamp_content_hashes
        from generic_crawler.crawler import collect_declared

        # SECTION DEFAULTS TO EMPTY, not to the category. `collect_declared`
        # stamps its default onto every 2-part entry, and that default would
        # then become a folder crumb sitting directly under the source_system
        # crumb that already says the same thing -- "KDIPA-Laws-and-Regulations
        # | Laws and Regulations". Empty means a 2-part entry gets
        # [regulator, source_system, title] and a 3-part entry gets exactly the
        # folders its author asked for.
        rows = collect_declared(self.documents, documents_section="") or []
        if not rows:
            # Every entry was dropped as a duplicate or unparseable. The config
            # named documents and none survived, which is a config error, not an
            # empty regulator.
            raise RuntimeError(
                f"{self.name}: declared {len(self.documents)} document(s) and "
                f"none parsed. Each entry must end in an http(s) url.")

        out: List[RegulatoryDocument] = []
        weak = []
        for r in rows:
            title = (r.get("title") or "").strip()
            # doc_path is [regulator, source_system, ...folders, title]. The
            # spec's own section path supplies the folders when it names any,
            # and `_clean_trail` drops a folder that merely repeats a crumb
            # already in the trail.
            #
            # THE TITLE IS APPENDED AFTER THE DEDUPE, NOT INSIDE IT. A declared
            # source is often ONE instrument, and such a source is frequently
            # named after that instrument -- so the regulator crumb and the
            # title can be the same string. Deduped together the title vanished,
            # doc_path ended at the source_system, and the last crumb was a
            # folder: the document had no position of its own. The folders are
            # deduped; the leaf never is, because the leaf IS the document.
            trail = _clean_trail(
                [self.regulator, self.source_system]
                + _split_section_path(r.get("section_path") or "")) + [title]
            basis = r.get("hash_basis") or ""
            if "WEAK" in basis.upper():
                weak.append(f"{title} [{basis}]")
            out.append(RegulatoryDocument(
                regulator=self.regulator,
                source_system=self.source_system,
                category=self.category,
                title=title,
                document_url=r.get("doc_url") or "",
                source_page_url=r.get("found_on") or r.get("doc_url") or "",
                file_type=r.get("type") or None,
                doc_path=trail,
                content_hash=r.get("content_hash") or "",
                extra_meta={"hash_basis": basis, "declared": True},
            ))

        if weak:
            # Not fatal -- a document with a weak stamp still belongs in the
            # library. But `url|title` cannot change, so this row will report
            # `unchanged` forever, including after the regulator replaces the
            # file. Say so once, loudly, rather than let it look monitored.
            logger.warning(
                "%s: %d document(s) have no server change-stamp, so their "
                "fingerprint is url|title and a REVISION WILL NOT BE DETECTED: "
                "%s", self.name, len(weak), "; ".join(weak))

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": weak},
            "by_source": {self.source_system: len(out)},
        }
        logger.info("DeclaredDocumentsSource[%s] -> %d document(s)",
                    self.source_system, len(out))
        return stamp_content_hashes(out)


def build_source(cfg: dict):
    """Turn ONE source entry from a regulator's YAML into a crawler object.

        mode: generic  -> GenericSiteCrawler (shared code, no per-site python)
        mode: declared -> DeclaredDocumentsSource (documents named in the yml)
        mode: custom   -> import and instantiate the named class

    All come back with the same fetch_documents(), which is the whole point:
    whether a source is generic, declared or hand-written stops mattering above
    this line.
    """
    mode = (cfg.get("mode") or "generic").lower()

    if mode == "declared":
        missing = [k for k in ("regulator", "source_system", "documents")
                   if not cfg.get(k)]
        if missing:
            raise ValueError(
                f"source '{cfg.get('name')}': mode=declared missing {missing}")
        return DeclaredDocumentsSource(
            regulator=cfg["regulator"],
            source_system=cfg["source_system"],
            documents=cfg["documents"],
            category=cfg.get("category"),
            name=cfg.get("name"),
        )

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
        scope=cfg.get("scope", "auto"),
        max_pages=int(cfg.get("max_pages", 150)),
        max_depth=int(cfg.get("max_depth", 8)),
        out_dir=cfg.get("out_dir"),
        include_pages=cfg.get("include_pages", "auto"),
        strategy=cfg.get("strategy", "auto"),
        exclude_documents=cfg.get("exclude_documents"),
        doc_path_sections=bool(cfg.get("doc_path_sections", True)),
        min_page_text=cfg.get("min_page_text"),
        doc_path_title=bool(cfg.get("doc_path_title", False)),
        doc_path_category=bool(cfg.get("doc_path_category", False)),
        placeholder_when_empty=cfg.get("placeholder_when_empty"),
        drop_sections=cfg.get("drop_sections"),
        uncategorised_parents=cfg.get("uncategorised_parents"),
        merge_files_at_same_path=bool(cfg.get("merge_files_at_same_path", False)),
        wait_ms=(int(cfg["wait_ms"]) if cfg.get("wait_ms") else None),
    )


def build_regulator_crawler(config: dict, only_sources=None):
    """Build the crawler for a whole regulator from its loaded YAML.

    A regulator is a LIST of sources, each independently generic or custom, so
    one regulator can mix both — e.g. SAMA's rulebook sectors on the generic
    engine while its circulars keep a tuned runner.

    `only_sources` NARROWS THE BUILD BY SOURCE `name`, and exists because
    monitoring and coverage are not the same question. LLOC is the case: its
    Latest window is 144 records in 15 requests and is where new legislation
    appears first, while its classification section is 1,583 documents over 47
    minutes. One is a nightly signal, the other a periodic coverage refresh, and
    they are the same regulator in the same file.

    The alternative was a second config holding only the monitored source, and
    `build_crawler`'s docstring is the argument against it: a second copy drifts,
    and then the workbook you approved is not what the monitor watches.

    A name that matches nothing RAISES. Silently monitoring zero sources is the
    failure this whole file keeps guarding against.
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
        wanted = {str(x).strip().lower() for x in only_sources}
        kept = [s for s in sources
                if str(s.get("name") or "").strip().lower() in wanted]
        missing = wanted - {str(s.get("name") or "").strip().lower()
                            for s in sources}
        if missing:
            raise ValueError(
                f"{regulator}: only_sources named {sorted(missing)}, which this "
                f"config does not define. It has "
                f"{[s.get('name') for s in sources]}. Refusing to run a narrowed "
                f"crawl that silently covers less than asked.")
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
        # A site-wide exclusion belongs to the REGULATOR, not to one section, so
        # the top-level list is the default for every source that names none.
        if config.get("exclude_documents") is not None:
            merged.setdefault("exclude_documents", config["exclude_documents"])
        if config.get("doc_path_sections") is not None:
            merged.setdefault("doc_path_sections", config["doc_path_sections"])
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
    # WHO MERGES LANGUAGE PAIRS. Read by CompositeCrawler rather than by the
    # crawler, because `build_source` returns at `mode: custom` before any
    # wrapper key is read -- so for a custom source this is the ONLY route the
    # setting has. A GenericSiteCrawler that already merged is skipped there, so
    # naming it on a generic source is harmless rather than a double merge.
    for level in (src, config):
        if "merge_files_at_same_path" in level:
            opts["merge_files_at_same_path"] = bool(level["merge_files_at_same_path"])
            break
    return opts


__all__ = ["GenericSiteCrawler", "CompositeCrawler",
           "build_source", "build_regulator_crawler"]
