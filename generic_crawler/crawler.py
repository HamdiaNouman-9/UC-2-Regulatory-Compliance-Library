"""
================================================================================
 Standalone Playwright crawler — the present "dynamic" crawler for regulator sites
================================================================================

NEW HERE?  Read CRAWLING_OVERVIEW.md and README.md in this folder first. They
explain what this UC is for and why this generic crawler is our current approach.
This file is a TEST TOOL and is fully SEPARATE from the live pipeline — it only
writes into output/standalone_crawler/.

--------------------------------------------------------------------------------
WHAT IT DOES (in plain words)
--------------------------------------------------------------------------------
You give it ONE start URL and ONE "scope" setting. It opens the page in a real
headless browser and behaves like a diligent person clicking around:

  1. Loads the page and lets JavaScript finish rendering.
  2. Expands menus/trees, reads content inside <frame>s, and scrolls to trigger
     lazy-loaded lists — so nothing stays hidden.
  3. Walks every level of the site (breadth-first: folder within folder ...),
     following links, until it runs out or hits the page/depth caps.
  4. On each page it records:
       - the BREADCRUMB trail   -> the "folder path" (how we mirror site structure)
       - the page CONTENT       -> rendered HTML + plain text
       - every DOCUMENT link    -> PDFs, DOCX, and "Download" buttons, with titles
  5. Keeps the crawl inside the section you asked for using the chosen SCOPE
     (breadcrumb / prefix / host) plus a same-host rule and hard caps.

--------------------------------------------------------------------------------
SCOPE (the only per-site choice — a setting, not code)
--------------------------------------------------------------------------------
  breadcrumb : stay inside the section by matching the breadcrumb trail exactly.
               Best for menu/tree sites with breadcrumbs (SAMA rulebook, CMA).
  prefix     : only follow URLs under the start URL's path (e.g. /circulars/*).
               Best for list-of-documents sites (SBP circulars, SECP acts).
  host       : anything on the same domain (broadest; always use a page cap).

--------------------------------------------------------------------------------
OUTPUTS (all under --out)
--------------------------------------------------------------------------------
  pages.json          full records incl. full html + text
  pages.xlsx          two sheets: "pages" (structure) and "documents" (the files)
  html/<slug>.html    the full readable HTML of each page, one file each
  (schema of these columns is documented in README.md)

Progress is printed to stdout as one JSON object per line so the Streamlit UI
(app.py) — or any wrapper — can stream it live.

--------------------------------------------------------------------------------
HOW TO READ THIS FILE
--------------------------------------------------------------------------------
It is laid out in the order a crawl actually happens:
  A. Small helpers        - URL cleaning, is-this-a-document?, title picking
  B. Browser-side JS       - snippets that run *inside* the page to read it
  C. Page actions          - extract_all(), collect_paginated_links(), expand_tree()
  D. crawl()               - the main loop that ties it all together
  E. Excel writer + CLI

Run directly:
  venv/Scripts/python.exe generic_crawler/crawler.py \
      --url https://rulebook.sama.gov.sa/en/regulatory-sandbox \
      --out output/standalone_crawler/sama_sandbox --scope breadcrumb --max-pages 150
"""

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote, urljoin
from html import escape as _html_escape

from playwright.sync_api import sync_playwright

# Only used by --documents (SECTION F). urllib is NOT enough there: cbe.org.eg's
# WAF rejects its header signature with a 269-byte "Request Rejected" page served
# as HTTP 200, while requests with the same User-Agent gets the real file. Guarded
# so a crawl that declares no documents does not need it installed.
try:
    import requests
except ImportError:                                   # pragma: no cover
    requests = None

# Shape-aware strategies (additive): detect tree / table layouts and dispatch.
try:
    from strategies import detect_shape, crawl_tree, crawl_table, crawl_list
    from blockcheck import blocked_reason
except ImportError:  # when imported as a package
    from .strategies import detect_shape, crawl_tree, crawl_table, crawl_list
    from .blockcheck import blocked_reason

# Windows consoles default to cp1252; force UTF-8 so Arabic/curly chars don't crash logging.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


#: ONE User-Agent for the whole file: the browser context in crawl(), the
#: preflight, and the --documents stamp. It was already this exact string in
#: crawl(); naming it stops the plain-HTTP helpers from drifting away from the
#: browser and being judged differently by a WAF.
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def slugify(text: str) -> str:
    """Filesystem-safe slug (no external dep)."""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", (text or "").lower()).strip("-")
    return s[:80] or "page"

# ============================================================================
# SECTION A — small helpers (URL cleaning, "is this a document?", title picking)
# ============================================================================

# ---- extensions we treat as DOCUMENTS (record, never navigate into) ----
DOC_EXTS = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".zip", ".ppt", ".pptx"}
# ---- extensions we simply ignore ----
SKIP_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".css", ".js",
             ".woff", ".woff2", ".ttf", ".mp4", ".mp3", ".webp"}
TRACKING_PREFIXES = ("utm_",)
TRACKING_KEYS = {"fbclid", "gclid", "gclsrc", "dclid", "msclkid", "_ga", "refresh"}

# Aggregator / print pages: "Entire Section", "Custom Print", "Print/Save as PDF".
# These duplicate content already captured page-by-page AND link to everything,
# which is what makes the crawl go in circles. Skip them by URL and by title.
import hashlib
AGGREGATOR_URL_PAT = re.compile(r"/(entiresection|customprint|custom-print|printpdf|print)(/|$|\?|-)", re.I)
AGGREGATOR_TITLE_PAT = re.compile(r"^\s*(entire section|custom print|print\s*/\s*save)", re.I)


def is_aggregator(url: str, title: str = "") -> bool:
    return bool(AGGREGATOR_URL_PAT.search(url)) or bool(AGGREGATOR_TITLE_PAT.search(title or ""))


# Common non-content chrome pages — never worth crawling.
DENY_PATH_PAT = re.compile(
    r"/(search|login|sign-?in|register|contact|sitemap|rss|feed|revision-updates|"
    r"terms-and-conditions|privacy|cookie)s?(/|$|\?)", re.I)


def first_seg(path: str) -> str:
    segs = [s for s in path.split("/") if s]
    return segs[0] if segs else ""


def path_excluded(path: str, excludes) -> bool:
    """Is this url path inside a subtree the source asked to skip?

    WHY A SOURCE WOULD ASK. CBE's laws-regulations section can only be crawled
    from its PARENT: the /regulations/regulations-book page links to nothing at
    all (measured 2026-08-25 -- 157 links, 3 of them the Arabic copy of itself),
    so seeding it directly records 1 document where the sitemap lists 143.
    Seeding the parent reaches them, but `prefix` scope then also swallows
    /regulations/circulars -- and those 396 circulars already arrive through
    CBE's own API with real titles, dates and categories. 21 of 55 documents on
    that crawl were duplicate circular PDFs.

    So the choice was: lose 142 documents, or duplicate 21 badly. This is the
    third answer -- crawl from the parent and skip the one subtree that is
    already owned by a better source.

    Matched on the PATH PREFIX, and it stops BOTH the page walk and document
    collection. Excluding a page but still harvesting its files would defeat the
    point, since it is the files that duplicate -- and documents are collected
    regardless of scope.
    """
    if not excludes:
        return False
    p = (path or "").rstrip("/").lower()
    for ex in excludes:
        ex = (ex or "").rstrip("/").lower()
        if ex and (p == ex or p.startswith(ex + "/")):
            return True
    return False


def scope_prefix(path: str) -> str:
    """The path that `scope: prefix` means by "under the seed".

    Naively this was `path.rstrip("/")`, which is right only when the seed URL is
    a DIRECTORY. Point it at a page and the page's own filename lands in the
    prefix, so nothing on the site can ever start with it:

        seed    /en/RulesRegulations/Pages/rules.aspx
        prefix  /en/RulesRegulations/Pages/rules.aspx      <- a leaf
        test    /en/RulesRegulations/Agreements  -> False  <- the real content

    Every sibling is rejected, including the documents the seed exists to reach.
    Measured on ZATCA and Ministry of Commerce: 38 and 47 rows of site chrome
    (Contact Us, Careers, News, Brand Identity) and zero regulations.

    TWO SEGMENTS COME OFF, IN ORDER:

    1. A trailing FILENAME — a last segment containing a dot. `/a/b/rules.aspx`
       is the page, `/a/b` is the section it lives in.

    2. A trailing `/pages` — SharePoint keeps every page of a section in a
       `Pages/` folder, so `/en/RulesRegulations/Pages` names a storage folder,
       not a subject area. Sibling sections live at `/en/RulesRegulations/Taxes`
       and `/en/RulesRegulations/Agreements`, which are under the SECTION but
       not under `Pages`. Both KSA sites we crawl this way are SharePoint.

        /en/RulesRegulations/Pages/rules.aspx   -> /en/RulesRegulations
        /en/Regulations/pages/default.aspx      -> /en/Regulations
        /activities/laws                        -> /activities/laws  (unchanged)

    A directory seed is returned untouched, so hosts that already worked are
    unaffected. Never returns "" — an empty prefix matches every path on the
    host, silently turning `prefix` into `host`; the last real segment is kept
    instead.
    """
    p = (path or "").rstrip("/")
    segs = [s for s in p.split("/") if s]
    if segs and "." in segs[-1]:            # a file, not a directory
        segs.pop()
    if len(segs) > 1 and segs[-1].lower() == "pages":   # SharePoint page store
        segs.pop()
    return "/" + "/".join(segs) if segs else ""


def content_key(text: str) -> str:
    """Hash of whitespace-normalized text, to detect same-content duplicate URLs."""
    norm = re.sub(r"\s+", " ", (text or "")).strip().lower()
    return hashlib.md5(norm.encode("utf-8")).hexdigest() if norm else ""


def emit(event: dict):
    """Print one JSON line to stdout (flushed) for live streaming."""
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def normalize_url(url: str) -> str:
    """Drop fragments + tracking params, lowercase host, strip trailing slash."""
    try:
        p = urlparse(url)
    except Exception:
        return url
    if p.scheme not in ("http", "https"):
        return url
    query = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)
             if not (k.lower() in TRACKING_KEYS or k.lower().startswith(TRACKING_PREFIXES))]
    path = re.sub(r"/{2,}", "/", p.path)      # collapse //circulars -> /circulars (SBP bug)
    path = path.rstrip("/") or "/"
    return urlunparse((p.scheme, p.netloc.lower(), path, "", urlencode(query), ""))


def ext_of(url: str) -> str:
    path = urlparse(url).path.lower()
    dot = path.rfind(".")
    return path[dot:] if dot != -1 else ""


# ---- captured HTML: absolute urls, and a file a browser can actually open ----
#
# JS_MAIN_CONTENT returns `clone.innerHTML`, and innerHTML serializes attributes
# AS AUTHORED: a relative src/href stays relative. The result is written to
# html/<slug>.html as a bare fragment with no origin, so a browser opening it
# from disk resolves "/en/laws-regulations" against file:///D:/ — every image
# 404s and every link points at a path that does not exist. Measured on
# cbe.org.eg: the <img> tags are captured correctly and render as broken icons
# with their alt text intact, which is what says the tags are fine and only the
# URL is wrong.
#
# JS_LINKS never had this problem: it reads `a.href`, the IDL property, which
# the DOM has already resolved. That is why documents.xlsx has been right all
# along and only the saved HTML is broken.
#
# regression_check.py already fixes exactly this for its frozen pages (see its
# freeze(): "without it every link becomes file:/// and the document-link counts
# collapse to zero"). Same bug, same remedy, applied to the crawler's own output.
#
# Attributes are rewritten with a regex rather than an HTML parser on purpose:
# this folder is self-contained (README) and we are editing attribute VALUES, not
# restructuring markup, so a parser's re-serialization is risk without a benefit.
# <script> and <style> are stripped by JS_MAIN_CONTENT before we get here, so the
# usual "regex matched inside a script" failure cannot arise.
_URL_ATTRS = ("data-lazy-src", "data-original", "data-src", "data-bg",
              "srcset", "poster", "href", "src")
_ATTR_RE = re.compile(
    r"\b(?P<attr>" + "|".join(_URL_ATTRS) + r")\s*=\s*"
    r"(?P<q>[\"'])(?P<val>[^\"']*)(?P=q)", re.I)
# Not URLs to resolve: in-page anchors, and schemes with no path to join.
_SKIP_URL = re.compile(r"^\s*(#|data:|javascript:|mailto:|tel:|blob:|about:)", re.I)


def _abs_one(base: str, val: str) -> str:
    v = (val or "").strip()
    if not v or _SKIP_URL.match(v):
        return val
    try:
        return urljoin(base, v)
    except Exception:
        return val


def _abs_srcset(base: str, val: str) -> str:
    """srcset is a comma-separated list of "<url> <descriptor>" pairs."""
    out = []
    for part in (val or "").split(","):
        part = part.strip()
        if not part:
            continue
        bits = part.split(None, 1)
        bits[0] = _abs_one(base, bits[0])
        out.append(" ".join(bits))
    return ", ".join(out)


def absolutize_html(html: str, base_url: str) -> str:
    """Resolve every relative url in `html` against `base_url`. Idempotent:
    urljoin on an already-absolute url returns it unchanged."""
    if not html or not base_url:
        return html or ""

    def _sub(m):
        attr, q, val = m.group("attr"), m.group("q"), m.group("val")
        new = (_abs_srcset(base_url, val) if attr.lower() == "srcset"
               else _abs_one(base_url, val))
        return f"{attr}={q}{new}{q}"

    return _ATTR_RE.sub(_sub, html)


def html_document(fragment: str, page_url: str, title: str = "") -> str:
    """Wrap a captured fragment as a standalone file.

    Three things the fragment does not carry on its own:
      * <meta charset> - the file is written UTF-8 with nothing declaring it, so
        Arabic renders as mojibake when opened from disk
      * <base href>    - belt and braces over absolutize_html(): a url built by
        JS after capture, or an attribute the rewrite did not know about, still
        resolves against the site instead of file:///
      * a <title>      - so a folder of these is readable in browser tabs

    CSS is deliberately NOT restored. JS_MAIN_CONTENT strips <style> and <link>
    as page chrome, so these files are the document's text and images, unstyled.
    Images load from the live site, which means viewing one needs a connection.
    """
    safe = re.sub(r"\s+", " ", (title or page_url or "")).strip()[:200]
    base = _html_escape(page_url or "", quote=True)
    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<base href="{base}">
<title>{_html_escape(safe)}</title>
</head>
<body>
{fragment or ""}
</body>
</html>
"""


def write_page_html(out, html_file: str, fragment: str, page_url: str,
                    title: str = "") -> None:
    """The ONE place a captured page becomes a file on disk."""
    body = html_document(fragment, page_url, title)
    path = Path(out) / html_file
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
        return html_file
    except OSError as e:
        # Almost always a path over Windows' 260-character limit, which surfaces
        # as a misleading FileNotFoundError. A crawl that finished must not be
        # lost to a filename, so fall back to a short digest and say so.
        short = f"html/_long/{hashlib.md5(html_file.encode('utf-8')).hexdigest()[:16]}.html"
        try:
            p2 = Path(out) / short
            p2.parent.mkdir(parents=True, exist_ok=True)
            p2.write_text(body, encoding="utf-8")
            emit({"event": "long_path", "wanted": html_file, "written": short,
                  "error": f"{type(e).__name__}: {str(e)[:80]}"})
            return short
        except OSError as e2:
            emit({"event": "error", "message":
                  f"could not write {html_file}: {type(e2).__name__}"})
            return ""


# ---- external portals that HOST law text themselves ----
# Some regulators don't attach a PDF: they link out to a national legal portal
# where the law lives as a web page with no file extension (MISA links 24 of its
# laws to laws.boe.gov.sa/BoeLaws/Laws/LawDetails/<guid>). Nothing in the URL says
# "document", so the link would otherwise be crawled as an ordinary page — or,
# being off-host, dropped entirely.
#
# This is a hand-maintained list and it WILL need a new entry per regulator.
# Check it when onboarding a site: probe_signals.py prints the hosts it saw.
EXTERNAL_LAW_PORTALS = {
    "boe.gov.sa", "laws.boe.gov.sa",    # Bureau of Experts (Council of Ministers)
    "mc.gov.sa",                        # Ministry of Commerce
    "moj.gov.sa", "laws.moj.gov.sa",    # Ministry of Justice
    "pr.gov.sa",                        # Premium Residency
    "zatca.gov.sa",                     # Zakat, Tax and Customs Authority
}


def is_external_law_portal(url: str, seed_host: str = "") -> bool:
    """True if the URL's host is a known legal portal that serves law text itself,
    rather than something we can recognise from the path or extension.

    IT MUST NOT FIRE FOR LINKS THAT STAY ON THE SITE WE ARE CRAWLING.

    EXTERNAL_LAW_PORTALS answers "some OTHER regulator's site links OUT to
    zatca.gov.sa as a cross-reference — that link is a law, not a page to crawl".
    The word doing the work is EXTERNAL. When the seed IS zatca.gov.sa, every
    ordinary navigation link on the site matches the host and is misread as a
    terminal document.

    Measured on ZATCA 2026-08-12: the seed page alone produced n_pages=1 and
    n_documents=38, and those 38 were Contact Us, Careers, News, Magazine and
    Brand Identity. Documents are collected regardless of scope, so no scope
    setting can filter them out — the crawl also never went deeper, because
    every link it could have followed had been marked terminal.

    The standalone crawler (generic_crawler/crawler_MISA_MC_ZATCA.py) already
    carried this seed_host guard and the warning above; this shared engine had
    the copy without it. MC has the same exposure — mc.gov.sa is also listed.
    """
    host = urlparse(url).netloc.lower()
    if seed_host and (host == seed_host or seed_host.endswith("." + host)
                      or host.endswith("." + seed_host)):
        return False
    return any(host == d or host.endswith("." + d) for d in EXTERNAL_LAW_PORTALS)


def is_document_link(url: str, seed_host: str = "") -> bool:
    """True if a link points to a downloadable document — not just plain .pdf/.docx,
    but also download-manager links (WordPress Download Manager `wpdmdl=`, /document/,
    /download/ endpoints) that serve a file without a file extension in the URL,
    AND known external law portals that host the law as a page (see above).
    Very common on gov sites (e.g. SECP acts: /document/<slug>/?wpdmdl=<id>)."""
    if ext_of(url) in DOC_EXTS:
        return True
    p = urlparse(url)
    q = p.query.lower()
    if "wpdmdl=" in q or "download" in q:
        return True
    segs = [s for s in p.path.lower().split("/") if s]
    if any(s in ("document", "documents", "download", "downloads") for s in segs):
        return True
    if is_external_law_portal(url, seed_host):
        return True
    return False


def doc_type_of(url: str, seed_host: str = "") -> str:
    """The file_type stored for a document link.

    `seed_host` matters here for the same reason it does in is_document_link:
    without it, EXTERNAL_LAW_PORTALS matches the site we are CURRENTLY crawling
    and every one of its own pages is stamped EXTERNAL. Measured on Ministry of
    Commerce 2026-08-12 — mc.gov.sa is in that set, so mc.gov.sa's own pages came
    back as EXTERNAL. The guard was threaded through is_document_link and missed
    here.
    """
    e = ext_of(url).lstrip(".").upper()
    if e:
        return e
    if is_external_law_portal(url, seed_host=seed_host):
        return "EXTERNAL"
    return "DOC"


GENERIC_LINK_TEXT = {"", "download", "pdf", "download pdf", "view", "view details",
                     "click here", "read more", "open", "details", "more",
                     # Action words that had been missing. "press here" reached
                     # the library as a document TITLE via the formfill side; the
                     # same words arrive here through anchor text.
                     "press here", "press", "click", "tap here", "here",
                     "download here", "download file", "download document",
                     "see more", "show more", "view more", "read", "detail",
                     "link", "attachment", "file", "document",
                     "اضغط هنا", "تحميل", "المزيد"}


def _norm_link_text(s: str) -> str:
    """Lowercased, with the invisible characters gov sites embed removed.

    GENERIC_LINK_TEXT is matched EXACTLY, so a single zero-width space defeats
    it. Ministry of Commerce stored a document titled `click here​` for
    exactly that reason — visually "click here", not equal to it.
    """
    s = (s or "").strip().lower()
    s = s.replace("​", "").replace("‌", "").replace("‎", "")
    s = s.replace("‏", "").replace("﻿", "").replace(" ", " ")
    s = re.sub(r"[\s.:،…]+", " ", s).strip()
    return s


PAGE_EXTS = {".aspx", ".asp", ".html", ".htm", ".php", ".jsp"}


def title_from_slug(url: str) -> str:
    """Human-readable name from the last URL segment.

    Handles the three things real regulator URLs throw at us:
      * percent-encoding      %20 / Arabic filenames  -> decoded
      * a file extension      ...Policy.pdf           -> dropped
      * squashed camelCase    BeneficiaryVoiceQ1      -> Beneficiary Voice Q1

    Only title-cases when the slug carries NO case information of its own, so
    "SAMA_Circular" is not mangled into "Sama Circular".
    """
    segs = [s for s in urlparse(url).path.split("/") if s]
    if not segs:
        return ""
    try:
        slug = unquote(segs[-1])
    except Exception:
        slug = segs[-1]

    m = re.search(r"\.[A-Za-z0-9]{2,5}$", slug)
    if m and m.group(0).lower() in (DOC_EXTS | PAGE_EXTS):
        slug = slug[:m.start()]

    had_case = not slug.islower() and not slug.isupper()
    slug = re.sub(r"[-_+]+", " ", slug)
    if had_case:
        # aBC -> a BC, and letter/digit boundaries: Q1 2025 stays, Voice2025 splits
        slug = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", slug)
        slug = re.sub(r"(?<=[A-Za-z])(?=\d{4}\b)", " ", slug)
    slug = re.sub(r"\s+", " ", slug).strip()
    if not had_case:
        slug = slug.title()
    return slug[:180]


#: A uuid the way `title_from_slug` renders one: hex groups, separators turned
#: to spaces, title-cased, sometimes with a "(1)" copy marker. Never a name.
_OPAQUE_ID = re.compile(
    r"^[0-9A-Fa-f]{8}[ \-][0-9A-Fa-f]{4}[ \-][0-9A-Fa-f]{4}"
    r"[ \-][0-9A-Fa-f]{4}[ \-][0-9A-Fa-f]{12}\b")


def disambiguate_titles(documents: list, prof: dict = None) -> int:
    """A title shared by several DIFFERENT documents is not a title.

    SDAIA links whole groups of quarterly reports as "2025" and a set of distinct
    policies as "Policies" — 41 of its 415 documents collide this way. The URL
    slug names them properly, so re-derive only the colliding ones and leave every
    unique title untouched. Returns how many were rewritten.

    WHAT THE REPLACEMENT IS. The slug, by default. Under `heading_is_title` it is
    the document's own LAST SECTION CRUMB, and that crumb is then removed from
    `section_path` — a name cannot be both the folder and the title of what sits
    inside it. See the profile key for the measurement; the short version is that
    pdp.gov.bh states each executive decision's real name in that crumb and
    nowhere else, while its two links say only "Download in English" and its
    filenames say "Trans Order Auditor Tasks".

    Unique titles are untouched either way, so this only ever changes a title that
    was going to be rewritten anyway.
    """
    from collections import Counter
    heading_first = bool((prof or {}).get("heading_is_title"))
    # A PLACEHOLDER'S SHARED TITLE IS DELIBERATE. `empty_page_placeholder` gives
    # every empty page the same sentence on purpose, and the url slug is no kind
    # of replacement: all six of MOIC's empty form categories were rewritten to
    # "Forms", the slug of /en/forms?tag=NNN. Excluded from the COUNT as well as
    # the rewrite, so six identical placeholders cannot drag a real title into
    # collision either.
    real = [d for d in documents if not d.get("placeholder")]
    counts = Counter((d.get("title") or "").strip().lower() for d in real)
    fixed = 0
    for d in real:
        t = (d.get("title") or "").strip()
        if not t or counts[t.lower()] < 2:
            continue
        alt, crumbs, from_heading = "", [], False
        if heading_first:
            crumbs = [c.strip() for c in (d.get("section_path") or "").split(">")
                      if c.strip()]
            if crumbs:
                alt, from_heading = crumbs[-1], True
        if not alt:
            alt, from_heading = title_from_slug(d.get("doc_url") or ""), False
        # A REPLACEMENT HAS TO BE BETTER THAN WHAT IT REPLACES.
        #
        # The slug is only a good name when the site names its files. Where it
        # serves them from an opaque id, this rewrite turns a real title into a
        # worse one. MEASURED 2026-09-10 on cbj.gov.jo/EN/List/Laws: two rows
        # both read "Central Bank of Jordan Law No. 23 of 1971" -- the same law
        # as .pdf and .docx -- and were rewritten to
        #
        #     C3F6F427 8350 4226 9C9B 40A6Ffe17933
        #     A6D37E65 4Ed7 41D0 8799 60962F082F77 (1)
        #
        # A shared title is a problem; two unreadable ones are a worse problem,
        # and the rows were never ambiguous to the DATABASE anyway -- identity
        # carries document_url. So keep the shared title and let a person see
        # the duplicate, which is what `merge_files_at_same_path` is for.
        if alt and _OPAQUE_ID.match(alt.strip()):
            continue
        if alt and alt.strip().lower() != t.lower() and len(alt) > 3:
            d["title"] = alt
            if from_heading:
                # The crumb IS the title now. Left in place it would produce a
                # folder and a document of the same name, one inside the other.
                d["section_path"] = " > ".join(crumbs[:-1])
            fixed += 1
    return fixed


def _norm_heading(s: str) -> str:
    """Case/punctuation-insensitive form, so '&' and 'and' compare equal."""
    s = (s or "").lower().replace("&", " and ")
    s = "".join(c if (c.isalnum() or c.isspace()) else " " for c in s)
    return " ".join(s.split())


def collapse_heading_path(path) -> list:
    """Trim a raw heading trail to the headings that actually NAME a section.

    Two conventions, no thresholds:
      1. a heading ending in ':' introduces what follows, it doesn't name it
         ("Learn More about the Policies and Regulations:")
      2. a heading that restates its parent adds nothing ("Laws > Laws")
    """
    kept = []
    for raw in path or []:
        raw = (raw or "").strip()
        if not raw or raw.endswith((":", "：")):
            continue
        n = _norm_heading(raw)
        if not n:
            continue
        if kept:
            prev = _norm_heading(kept[-1])
            if prev and (prev in n or n in prev):
                continue
        kept.append(raw)
    return kept


def _with_link_title(breadcrumb, link_title, prof) -> list:
    """The breadcrumb, plus the anchor text that led here, when the host asks.

    A filtered listing is a page whose subject is named nowhere on it except in
    the link that reached it: /en/forms?tag=326 shows the SUBcategories of
    "Business Services & Bahrain Investors Center Services" and never that name.
    `linked_from_title` is recorded for every queued url, so the label is
    discovered rather than configured -- which is the point, because a hardcoded
    list of the categories a site has today cannot notice the one it adds
    tomorrow.

    Appended to the breadcrumb so it lands AFTER the page's own trail and BEFORE
    its headings. Skipped for the seed (nothing linked to it) and skipped when
    the trail already says it.
    """
    crumbs = list(breadcrumb or [])
    if not (prof or {}).get("link_title_is_section"):
        return crumbs
    lt = clean_doc_title(link_title)
    if not lt or len(lt) > 120:
        return crumbs
    if _norm_heading(lt) in [_norm_heading(c) for c in crumbs]:
        return crumbs
    return crumbs + [lt]


def doc_section_path(breadcrumb: list, group: str = "", nav_path: str = "",
                     heading_path=None) -> str:
    """The folder trail for a document: the page's breadcrumb, then optionally the
    on-page heading the link sits under (aml.gov.sa: "... > Rules and Regulations >
    Laws and Regulations"). The heading is dropped when it just repeats the last
    crumb or the page title, so we never emit "X > X".

    `group` is only passed when --group-headings is set, because a page's headings
    are a real section grouping on SOME sites only. Measured on the others: SECP
    headings are dates, CBB's is "FOLLOW US", and CMA's are the document titles
    themselves — appending those adds a junk or duplicate level. Opt in per site.

    `nav_path` is the structured trail from JS_NAV_PATH (tab panel + category
    heading). It is MORE specific than `group` — two named levels instead of one
    nearest heading — so when a page provides it, it wins and `group` is not also
    appended. It needs no opt-in flag: it is empty unless the markup matches.
    """
    # Drop a crumb that repeats one already in the trail. SDAIA's breadcrumb is
    # "SDAIA > Saudi Data and Artificial Intelligence Authority > SDAIA > About
    # SDAIA" — the same folder name twice. Only the display trail is affected;
    # breadcrumb SCOPE matching reads the raw list, not this.
    trail, _seen = [], set()
    for c in (breadcrumb or []):
        c = re.sub(r"\s+", " ", c or "").strip()
        if not c or c.lower() in _seen:
            continue
        _seen.add(c.lower())
        trail.append(c)

    def _append(parts):
        for p in parts:
            p = re.sub(r"\s+", " ", p).strip()
            if p and p.lower() not in [c.lower() for c in trail]:
                trail.append(p)
        return " > ".join(trail)

    # Precedence, most specific first:
    #   1. nav_path      — tab panel + category, two named levels (MISA)
    #   2. heading_path  — the page's own heading nesting (SDAIA)
    #   3. group         — single nearest heading, the original behaviour
    nav_parts = [p for p in (nav_path or "").split(">") if p.strip()]
    if nav_parts:
        return _append(nav_parts)

    hp = collapse_heading_path(heading_path or [])
    if hp:
        return _append(hp)

    return _append([group or ""])


# A trailing file size is never part of a title. RERA renders every document link
# as the title, a run of tabs, then the size, so 73 of its 134 documents were
# stored as "Royal Decree No. (69) for 2017 ...<tabs>  380.13Kb".
#
# Whitespace is collapsed for the same reason: best_doc_title returns the anchor's
# raw textContent, which — unlike `ctx` — JS_LINKS never normalises, so the
# padding travelled all the way into the library.
#
# NOTE FOR ANY REGULATOR ALREADY STORED: `title` is part of DEFAULT_IDENTITY, so a
# row whose stored title carried padding or a size reads as one `new` plus one
# `disappeared` the first time it is re-crawled. That is a one-time correction of
# a wrong title, not churn to be avoided — but run baseline.py before and after so
# the size of it is known rather than discovered.
#: A trailing size, and/or a trailing page count. Applied repeatedly, so the
#: combined form sio.gov.bh uses — "2.7 MB, 58 Pages" — comes off in two passes:
#: the page count first, then the size. RERA's "380.13Kb" needs only one.
#: A PAGE COUNT IS ONLY STRIPPED WITH THE WORD "page(s)" PRESENT, so a title
#: ending in a bare year or number ("Report 2024", "Decision No. 12") is safe.
#: A LANGUAGE MARKER MAY FOLLOW THE SIZE, and until 2026-08-27 that blocked the
#: whole match, because the size had to be the last thing in the string.
#: Measured on moic.gov.bh/en/regulations: 73 of 79 titles read
#: "The Law of Commerce No. 7 of 1987 5.17 MB EN", so every one kept its size.
#: The list is closed on purpose — a bare two-letter word is not evidence of a
#: language, and "Circular 12 MB EN" is the only shape worth trusting.
_LANG_TAIL = r"(?:\s*[\-–—|,;(\[]*\s*(?:EN|AR|ENG|ARA|English|Arabic|عربي|عربى)\s*[)\]]*)?"

_SIZE_TAIL = re.compile(
    r"[\s\-–—|,;(\[]*"
    r"(?:\d+(?:[.,]\d+)?\s*(?:[KMGT]i?B|bytes?)"
    r"|\d+\s*pages?)"
    r"\s*[)\]]*"
    + _LANG_TAIL +
    r"\s*$",
    re.I)


#: A trailing FORMAT LABEL, and the separator in front of it: "| DOC", "- PDF".
#:
#: A listing that shows a format column puts it in the row text, so the context
#: a title is built from ends "<name> | DOC". MEASURED on the 2026-09-15 CBJ
#: export, 1 row of 313: "Guidelines for Bank Licensing | DOC".
#:
#: THE PIPE IS WHY THIS IS NOT COSMETIC. `doc_path` is a LIST in the library and
#: is flattened with " | " into the workbook, so a pipe inside a title makes the
#: flattened trail ambiguous: `promote` splits it back and gets one crumb too
#: many. That row would land as a FOLDER "Guidelines for Bank Licensing"
#: holding a document called "DOC". `check` passes it -- the shape is valid --
#: which is exactly the class of fault ONBOARDING says only a person can see.
#:
#: The separator is required, so a title that merely ENDS in a format word
#: ("Guide to PDF") is untouched; only "<name> | PDF" is.
#: The separator is a REAL one -- a pipe or a dash, not merely a space. An
#: earlier version wrote `[\s|\-–—]+` and so matched whitespace alone, which
#: turned the legitimate title "Guide to PDF" into "Guide to".
_FORMAT_TAIL = re.compile(
    r"\s*[|\-–—]\s*(?:doc|docx|pdf|xls|xlsx|ppt|pptx|rtf|zip|txt|csv)\s*$", re.I)


def clean_doc_title(s) -> str:
    """One space between words, no trailing file size, no trailing format label."""
    s = re.sub(r"\s+", " ", (s or "")).strip()
    prev = None
    while prev != s:              # "(1.2 MB)" can leave a bracket behind
        prev = s
        s = _SIZE_TAIL.sub("", s).strip()
        # After the size, because "<name> | DOC 1.2 MB" hides the label behind
        # it -- and in the same loop, because removing one can expose the other.
        s = _FORMAT_TAIL.sub("", s).strip()
    return s.strip(" -|–—")


#: A `title="..."` made only of digits and separators. See `best_doc_title`.
_BARE_NUMBER = re.compile(r"^[\d\s.,\-/_]+$")


def _ctx_title(ctx) -> str:
    """The title a row/card context reduces to, or "" if it reduces to nothing.

    ONE definition, deliberately: `best_doc_title` uses it to PICK a context and
    `_merge_links` uses it to CHOOSE BETWEEN two sightings of the same href. Two
    copies would let the merge keep a context the title step then rejects, which
    is exactly the state that produced guid-named documents on cbj.gov.jo.
    """
    t = clean_doc_title(ctx)
    t = re.sub(r"\b(download|pdf|view|click here|read more)\b", "", t,
               flags=re.I).strip(" -|")
    # A CARD LAYOUT REPEATS THE TABLE'S COLUMN HEADINGS inside every card, so
    # the context reads "File Name <the actual name> Type Size 686KB" rather
    # than the name alone. MEASURED on cbj.gov.jo's mobile card list, which is
    # where 14 of its 24 documents are found. These are column labels on any
    # site that uses them; none of them is ever part of a document's name.
    t = re.sub(r"^\s*file\s*name\s*[:\-]?\s*", "", t, flags=re.I)
    t = re.sub(r"\s*\btype\b\s*\bsize\b.*$", "", t, flags=re.I)
    t = t.strip(" -|&,;:")
    return clean_doc_title(t)      # removing "download" can expose a size


def best_doc_title(link: dict, url: str) -> str:
    """Pick a human title for a document link, best source first:
      1. the anchor text            — unless it's a generic 'Download' button
      2. the anchor's title="..."   — usually the full name when the visible text
                                      is a bare year ("2025") or "Policies"
      3. the row/card context       — holds the real title + date on table rows
      4. the URL slug               — last resort
    """
    # Cleaned FIRST, so the length tests judge the real title rather than the
    # padding around it, and returned from ONE exit so a fifth candidate added
    # later cannot skip the cleaning.
    t = clean_doc_title(link.get("text"))
    picked = ""
    if _norm_link_text(t) not in GENERIC_LINK_TEXT and len(t) > 3:
        picked = t
    if not picked:
        ta = clean_doc_title(link.get("title_attr"))
        # A BARE NUMBER IS A CMS NODE ID, NOT A NAME.
        #
        # MEASURED 2026-09-09 on cbj.gov.jo: every one of the 34 document links
        # on the MoUs listing carries title="2373" -- the same id on all of
        # them. Four characters clears the length test, so tier 2 picked it and
        # tier 3 never ran, even though the row context right below held
        # "MoU between CBJ and Bank Al-Maghrib 2017/11/13 2282KB".
        #
        # `disambiguate_titles` then did its job on the wreckage: 34 documents
        # sharing one title is not a title, so it re-derived each from the url
        # slug -- and this site's slugs are guids. The workbook came out with 24
        # rows named "E2Ad73E3 0655 4B5C Aa4A Fa97B1919341".
        #
        # Rejecting it here costs nothing: the ladder simply continues to the
        # row context, then the slug, both of which are strictly better answers
        # than an id. Digits and separators only -- anything with a letter in it
        # is a name someone chose.
        if len(ta) > 3 and not _BARE_NUMBER.match(ta):
            picked = ta
    if not picked:
        ctx = _ctx_title(link.get("ctx"))
        if len(ctx) > 3:
            picked = ctx
    return (picked or clean_doc_title(title_from_slug(url)) or t)[:200]


# ============================================================================
# SECTION A2 — per-site profiles  (TEMPORARY: this moves to config/regulators/*.yml)
# ============================================================================
# Fixes that a site NEEDS but that would change what we already produce for the
# other regulators are opted into per host, so a default run stays byte-identical
# to before. Each key below was added because it was MEASURED to be wrong-by-
# default on some site and right on another — see the notes.
#
# Verified with a side-by-side old-vs-new run on the SAMA rulebook, SAMA
# circulars, CBB, SECP acts, SBP circulars and CMA seeds: with the profile off,
# breadcrumb and content output are unchanged on all six.
SITE_PROFILES = {
    "nca.gov.sa": {
        # NCA (Next.js), MEASURED 2026-09-24 on /en/enablement/ and
        # /en/cyber-operations/ — the two tabs crawled generically; the other
        # NCA tabs are custom (crawler/nca_crawler.py) and never read this.
        #
        # <main> has exactly three children on both pages:
        #   div.bg-primary-5         breadcrumb, <h1>, "Share the page" buttons
        #   div.py-10                the content
        #   div.full-container.py-6  "Was this page useful? ... from 1 Feedbacks"
        # The default <main> capture kept all three, so the stored HTML opened
        # with a bare "Home" list and closed on the feedback count. Naming the
        # middle child excludes the other two by selection.
        "content_selector": "main > div.py-10",
        # INSIDE div.py-10:
        #   * each picture is a Next.js <img> alone in its own <div>, pointing at
        #     /_next/image/... — it renders as an empty gap once stored (2 per
        #     page). The wrapper goes with it, or the gap stays.
        #   * the last block is "Last Update at: 27/08/2025 - 11:33 am Saudi
        #     time", a <p class="py-6"> alone in a div.full-container: page
        #     furniture, same as the feedback widget below it.
        "drop_selectors": "div:has(> img:only-child), img, "
                          "div.full-container:has(> p.py-6:only-child)",
    },
    "www.aml.gov.sa": {
        # The current crumb is a <span class="breadcrumbCurrent">, not an <a>, and
        # the Arabic home link ships in a display:none span. Anchors-only reading
        # therefore drops "Rules and Regulations" and keeps a hidden "AML", which
        # made the breadcrumb-scope anchor match every page on the site.
        "breadcrumb_current": True,
        # SharePoint wraps the whole page in <form id="aspnetForm">, so treating
        # <form> as junk deleted the entire document (0-byte HTML).
        "unwrap_forms": True,
        # No main/#content on the page; without these it falls through to <body>.
        "sharepoint_main": True,
        # The <h3>s ("Laws and Regulations" / "Rules and Instructions") are a real
        # section grouping here. On SECP the headings are dates, on CBB it is
        # "FOLLOW US", on CMA they are the document titles — junk levels there.
        "group_headings": True,
    },
    "www.qcb.gov.qa": {
        # QCB IS SHAREPOINT, AND IT FAILED IN BOTH OF SHAREPOINT'S USUAL WAYS.
        #
        # MEASURED 2026-09-17, one plain GET of
        # /en/FinancialStability/Pages/GreenFinance.aspx (48,431 bytes) plus the
        # two capped crawls already in output/qbc_esg and output/qbc_leg:
        #
        #   id="aspnetForm"            present   -> whole page dropped as a <form>
        #   [id*="PlaceHolderMain"]    present   -> the content wrapper to pick
        #   crawled text_len                   0
        #   crawled html file            235 bytes  <- <body></body>, nothing else
        #
        # The blank capture is NOT the .aspx being slow. The same run harvested
        # 9 PDF links off that page, and JS_LINKS reads the live document — so
        # the page had rendered. Only the CAPTURE was empty, which is the
        # aml.gov.sa failure exactly: <form> is on the junk list, SharePoint
        # wraps the entire document in one, and removing it removes the page.
        "unwrap_forms": True,
        "sharepoint_main": True,
        # THE CURRENT CRUMB IS AN <li>, NOT AN <a>:
        #   <li class="breadcrumb-item"><a ...>Home page</a></li>
        #   <li class="lblCurrentbreadcrumb breadcrumb-item active"
        #       aria-current="page">Sustainability &amp; Financial Inclusion</li>
        # Anchors-only reading therefore returned ["Home page"] on every page of
        # the site — one breadcrumb for the whole host, which is what the first
        # test run reported.
        "breadcrumb_current": True,
        # THE FAQ ACCORDION ON /Digital-Currency.aspx, AND NOTHING ELSE.
        #
        # That page is one paragraph saying what a CBDC is, followed by 21
        # <details>/<summary> question-and-answer blocks. The Q&A is the site
        # explaining itself, not an instrument, and left in it is 95% of the
        # stored text — 7,531 characters against 376 of actual statement.
        #
        # MEASURED across every QCB page in scope, 2026-09-17:
        #   Digital-Currency        21 <details>
        #   MonetaryPolicyTools      0
        #   LegislationNew           0
        #   Information-Security     0
        #   FinancialTechnology      0
        #   GreenFinance             0
        # so naming the ELEMENT costs nothing anywhere else on this host, and
        # needs no per-page setting the engine does not have.
        #
        # `drop_selectors` only edits the CAPTURED HTML. JS_LINKS reads the live
        # document, so a file linked from inside an FAQ answer is still found and
        # still recorded — this hides the prose, never a document.
        #
        # The 376 characters that remain clear MIN_PAGE_TEXT (200) with room to
        # spare, so the page still registers as a document. If QCB ever trims
        # that paragraph the page stops being stored at all, which will look like
        # a disappearance and is worth knowing before it happens.
        "drop_selectors": "details",
    },
    "www.rera.gov.bh": {
        # RERA'S CURRENT CRUMB IS NOT A LINK.  MEASURED 2026-08-21 on
        # /en/regulations/circulars/circulars-issued-in-2020:
        #
        #   <ol class="breadcrumb">
        #     <li class="breadcrumb-item"><a href="/en">Home</a></li>
        #     <li class="breadcrumb-item"><a href="/en/regulations">Regulations</a></li>
        #     <li class="breadcrumb-item active">Circulars issued in 2020</li>
        #   </ol>
        #
        # Anchors-only reading therefore returned ['Home', 'Regulations'] for ALL
        # 15 pages -- every child page filed under its PARENT, with nothing naming
        # the page itself. That is what made section_path look truncated in
        # documents.xlsx.
        #
        # Safe here, unlike the two risks the flag carries elsewhere:
        #   * it can select a DIFFERENT breadcrumb container (measured on CMA).
        #     RERA has exactly ONE .breadcrumb, and it is the same one either way.
        #   * it moves the scope anchor to the current page, which matters only
        #     for `scope: breadcrumb`. All eight RERA sources pin `scope: prefix`,
        #     and auto-detection never reaches the breadcrumb branch because
        #     RERA's links cluster under the seed path and prefix wins first.
        #
        # NOTE RERA'S BREADCRUMB IS ONLY EVER THREE DEEP. It jumps straight from
        # Regulations to the current page, so the 2020 circulars page reads
        # "Home > Regulations > Circulars issued in 2020" and NOT
        # "... > Circulars > Circulars issued in 2020". The missing middle is the
        # site's, not ours -- the section is named by source_system in
        # config/sources/rera.yml, one source per section.
        "breadcrumb_current": True,
        # No main/article/#content on the page, so extraction fell back to <body>
        # and kept the whole shell: 43 chrome divs (mobile-menu, main-footer,
        # top-side-menu, d-print-none blocks holding 27,558 characters).
        # MEASURED 2026-08-20: #page-content holds 77% of the body text at depth 4.
        "content_selector": "#page-content",
        # Two blocks sit INSIDE #page-content and are not the document:
        #
        #   .sider-bar   RERA's right-hand column. One container holding the QR
        #                image (a base64 svg), its "Scan to view from mobile"
        #                caption, an <hr>, and the <h4>Other links</h4> list of
        #                sibling sections. Removing it takes all three at once.
        #   .file-size   the "380.13Kb" label under each document link. A file's
        #                byte count is never part of a regulation, and it is what
        #                was polluting titles through the row context.
        #
        # Both are RERA's own class names, so they are scoped to this host rather
        # than added to the shared junk list.
        "drop_selectors": ".sider-bar, .file-size",
    },
    "www.lloc.gov.bh": {
        # `document.querySelector` returns the first match in DOCUMENT order, not
        # selector order, and this site has NINE matches for the generic list
        # `main, [role="main"], article, #content, .content, #main`:
        #
        #   DIV.content      depth 5      59 chars   <- what was picked: the
        #                                               phone / email / dark-mode bar
        #   DIV.content      depth 5     118
        #   DIV.content      depth 6     152
        #   DIV.bodycontent  depth 3    1714 chars   <- role="main", the real one
        #
        # MEASURED 2026-08-21: a prefix crawl of /Legislation/Latest captured 58
        # characters of text and reported `status: ok`. Naming the site's own
        # wrapper is the same fix RERA and SIO needed, for the same reason.
        "content_selector": "div.bodycontent",
        # FURNITURE INSIDE THE CONTENT WRAPPER, so `content_selector` cannot
        # exclude it by selection. Measured 2026-08-25 on the captured HTML of
        # /en/page/Legislations within the reform project frame (24,939 bytes):
        #
        #   div.bodycontent
        #     div.mainImage                    0 chars   the banner photo
        #     div.page
        #       div.breadcrumbs               70 chars   "(current)/Legislation/..."
        #       div.pagecontent
        #         div.article             20,586 chars   <- THE DOCUMENT
        #       div.pagemenu                 225 chars   6 links: "Search in
        #                                                Legislations", "Legislations
        #                                                related to women", ...
        #
        # `.pagemenu` is the section nav repeated on every page of the site, and
        # `.breadcrumbs` duplicates in the body what the `breadcrumb` column
        # already holds. Both rendered as blue link lists above and below the
        # text in the saved HTML.
        #
        # Dropping the breadcrumb from the CLONE does not cost the breadcrumb
        # column: JS_BREADCRUMB queries the LIVE document, as does JS_LINKS, so
        # neither the trail nor link discovery is affected.
        #
        # NOT dropped: div.mainImage. It carries no text, so it costs nothing in
        # the extracted content, and it is the only thing distinguishing one of
        # these pages from another when a person opens the saved file. Add it here
        # if the saved HTML should be text-only.
        "drop_selectors": ".breadcrumbs, .pagemenu",
    },
    "www.moic.gov.bh": {
        # /en/forms publishes the same list twice: unfiltered, and once per
        # sidebar category. The category exists ONLY in the rail link, and the
        # rail is 8 ordinary anchors, so the walk finds them — and finds a ninth
        # the day it appears. See both keys' docs in DEFAULT_PROFILE.
        "link_title_is_section": True,
        "prefer_deepest_section": True,
        # Six of the eight form categories publish nothing. Without a row each,
        # the tree would show two categories where the ministry lists eight —
        # and would look identical to a site that never had the other six.
        "empty_page_placeholder": "No documents published under this category",
        # MOIC (Bahrain), MEASURED 2026-08-27 on /en/regulations and /en/forms.
        #
        # THERE IS NO BREADCRUMB ON THIS SITE. JS_BREADCRUMB returns [] on both
        # pages, so `section_path` was EMPTY on all 79 regulation documents and
        # all 13 forms — every document filed at the root with no structure at
        # all.
        #
        # The structure it does have is the page's own headings: the listing is
        # an accordion whose <h2> names the topic. With `group_headings` the
        # links' heading_path reaches doc_section_path, and the 79 documents
        # sort into 18 real folders —
        #   Regulations > Commercial Companies      14
        #   Regulations > Consumer Protection       11
        #   Regulations > Commercial Register        7
        #   Regulations > Trademarks                 7
        #   Regulations > Corporate Governance       6
        #   Regulations > Patents                    6   ... and 12 more
        # — and the 13 forms into 5 (Companies Control, Testing and Metrology,
        # Precious Metals Assay Centre, Consumer Protection, Industrial Areas).
        #
        # This is what `group_headings` is for, and the flag's own note explains
        # why it is opt-in: on SECP the headings are dates, on CBB "FOLLOW US",
        # on CMA the document titles. Here they are the taxonomy.
        #
        # NOT NEEDED, and checked rather than assumed:
        #   * no API. /jsonapi, /en/jsonapi, /api, /en/api and
        #     /jsonapi/node/regulation all 404; it is Drupal with JSON:API off,
        #     and there is no /sitemap.xml either.
        #   * no filter urls. ?about[0]=19 (Commerce, 75 files) and =20
        #     (Industry, 5 files) union to exactly the 79 the plain page already
        #     lists, and the 8 form ?tag= values union to 12 of the 13 forms —
        #     so crawling the filters would add nothing and LOSE one form.
        #   * `strategy: generic` IS needed, but that is a crawl argument rather
        #     than a profile key, so it lives in config/sources/moic.yml. Without
        #     it the seed detects as `list` and the run reports `status: ok` with
        #     78 pages, 78 errors and ZERO documents.
        "group_headings": True,
        # THE SIZE AND LANGUAGE UNDER EVERY LINK. Each document anchor is
        #     <a class="DocumentItem">TITLE<div class="Itemlang"><span>5.17 MB</span> EN</div></a>
        # so the captured page reads "... Law of Commerce No. 7 of 1987 / 5.17 MB
        # EN" on every one of the 79 rows. The document TITLES are already clean —
        # `_SIZE_TAIL` strips a trailing size and, since 2026-08-27, a language
        # marker after it — but the html keeps the site's own line, which is
        # neither the title nor useful once the file is stored.
        # `.docImg` is the red PDF glyph repeated before every link —
        #     <a class="DocumentItem"><div class="docImg"><img
        #        src="/themes/custom/indestry/images/pdfDownload.JPG"></div>TITLE</a>
        # 78 of them on the regulations page, identical, and each one an <img>
        # the saved file cannot load anyway (relative to a theme directory).
        "drop_selectors": ".Itemlang, .docImg",
        # Bahrain's national economic vision, linked from the body of both
        # listings and filed under whichever heading it sat below. Not a MOIC
        # regulation. Matched on the directory so a re-upload is still caught.
        "exclude_document_urls": "/2030-vision/",
    },
    "www.pdp.gov.bh": {
        # PDPA (Bahrain), MEASURED 2026-08-27 on the three pages crawled into
        # output/pdpa_regulations, output/pdpa_forms and output/pdpa_exec_orders.
        #
        # 1. THE CRUMBS ARE BARE <li>s. section_path read "Home" on all three
        #    pages, so every document filed under the site root rather than its
        #    own section. The trail is
        #      <ul><li><a>Home</a></li><li class="sep">…</li><li>Forms</li></ul>
        #    and the current crumb carries no anchor, no [aria-current] and no
        #    "current"/"active" class, so neither anchors-only nor
        #    `breadcrumb_current` can see it. VERIFIED: with breadcrumb_li the
        #    crumbs read Home > Forms, Home > Executive Decisions/Orders and
        #    Home > The Law.
        #
        # 2. THE ARTICLE HEADINGS ARE BUTTONS. regulations.html is an accordion:
        #    61 .collapse panels hold 98,087 characters of article text and are
        #    captured fine, but the 62 <button>s that title them — "Article (1)
        #    Definitions" and so on, 2,848 characters — were deleted by the junk
        #    list. The law came out as one run of text with no headings.
        #
        # 3. THE BREADCRUMB IS INSIDE <main>. `content_selector` cannot exclude
        #    it (measured: main contains .breadcrumb-area on all three pages), so
        #    it is named here. Only the <ul> is dropped, NOT the whole area: the
        #    area also holds <h2 class="page-title">, which is the page's own
        #    heading and worth keeping in the captured html.
        # 4. A SCROLL ANIMATION HID MOST OF THE LAW. The pages are built with
        #    WOW.js, which sets `visibility: hidden` inline until an element
        #    scrolls into view. 58 of 68 `.wow` elements on regulations.html were
        #    still hidden at capture — the preamble and every "Section One / Two
        #    / Three" heading — so the saved page opened as a few visible
        #    paragraphs and then blank space. The text itself was always present
        #    (a detached clone's innerText is textContent), so this is a
        #    rendering fix, not a recovery.
        # 5. THE DECISION NAME IS A <div>, NOT A HEADING. Executive
        #    Decisions/Orders names each decision in
        #    `.executive-decisions-pdf > .block:first-child`, with its Arabic and
        #    English pdf in the SECOND .block. JS_LINKS reads h1-h4 only, so all
        #    20 documents came back under the one page heading and the twenty
        #    decisions collapsed into a single folder.
        #
        #    `:first-child` matters: both children carry class `block`, and the
        #    second one holds the download links. Naming `.block` alone would
        #    make a link's nearest heading its OWN container, and every decision
        #    would be titled "تحميل باللغة العربية Download in English".
        # 6. THE LAW'S ARTICLES ARE ACCORDION PANELS, and they are records.
        #    /en/regulations.html renders 60 articles as
        #      <button data-target="#faqOne">Article (1) Definitions</button>
        #      <div class="collapse" id="faqOne"> ...the article text... </div>
        #    which is the shape `keep_modals` already reads — measured: 61
        #    panels, 60 kept, bodies of 1,292-4,017 characters. Without it the
        #    page contributes TWO documents (the pdf and its Gazette copy) and
        #    the law itself is one undifferentiated blob of text.
        #
        #    Safe host-wide: executive-decisions.html and forms.html each carry
        #    exactly ONE panel, the nav dropdown `navbarSupportedContent`, whose
        #    trigger has no text — so it is dropped by the collector's title
        #    check and neither page gains a record.
        "keep_modals": True,
        # The law names each division in two elements — <h5>Section Three</h5>
        # followed by <p>Transfer of Personal Data outside the Kingdom</p> — and
        # all eleven divisions are written that way. Without this the tree holds
        # three folders called "Section One", one per Part, and nothing on the
        # face of them says which law they belong to.
        # Each executive decision is a PAIR of download links -- one Arabic, one
        # English -- under a heading carrying its only real name. Without this the
        # ten decisions are folders and the twenty rows are named after files.
        "heading_is_title": True,
        # The law's own voice is its preamble; its Parts, Sections and 60
        # Articles are all stored as children. Without this the page record
        # repeated the entire law — 119,277 characters against the 3,618 the
        # preamble actually is.
        "page_text_stops_at_children": True,
        "heading_subtitle": True,
        "heading_selector": ".executive-decisions-pdf > .block:first-child",
        # Needed for the above to reach section_path at all: without it
        # doc_section_path is passed neither `group` nor `heading_path`.
        "group_headings": True,
        "breadcrumb_li": True,
        "keep_buttons": True,
        "unhide_animated": True,
        # The WHOLE breadcrumb block, not just its list: the section is a banner
        # carrying a background image and an <h2> that repeats the page title the
        # content states again immediately below ("The Law The Law ..."). Nothing
        # is lost — JS_DOC_TITLE reads the live document, so `title` still says
        # Forms / Executive Decisions/Orders / The Law.
        "drop_selectors": ".breadcrumb-area",
    },
    "www.sio.gov.bh": {
        "keep_modals": True,
        # NO content_selector, deliberately. #govbh-main looks like the content
        # root and is on a LAW page, but on an INDEX page it holds only 1,734
        # characters: the section cards live in a sibling block
        # (SECTION > .container > .row > .services-facilities-main >
        # .documents-single), so selecting #govbh-main silently dropped every
        # READ MORE card and with it the route to Ministerial Edicts and Orders.
        #
        # Naming the chrome instead keeps both shapes of page whole. Measured on
        # the default (body) capture, SIO's chrome was only ~13% of the text, so
        # there is little to gain from selection here and a section to lose.
        "drop_selectors": (
            # site furniture, all top-level siblings of the content
            ".header-menu-banner, footer.footer-sec, .govbh-user-rating, "
            # the "Search / exact.match" box that opens every law list
            ".search-main, "
            # "2.7 MB, 58 Pages" — a sibling of .link-name inside each trigger
            # anchor, which is also why it leaked into titles via a.innerText
            ".pdf-size, "
            # UserWay accessibility widget, injected at body level: uw-sl,
            # uw-s10-*-ruler-guide, userway_buttons_wrapper, uai, ulsti. It only
            # became visible once content_selector was removed, because #govbh-main
            # had been excluding it by accident.
            '[class*="userway"], [class*="uw-s"], [class*="uw-sl"], .uai, .ulsti'
        ),
    },
    "sdaia.gov.sa": {
        # SharePoint, same as aml.gov.sa: the whole page is inside <form
        # id="aspnetForm"> and there is no <main>/#content.
        "unwrap_forms": True,
        "sharepoint_main": True,
        # Measured 2026-08-01 on RegulationsAndPolicies.aspx: all 36 documents
        # sit under real section headings ("Personal Data Protection Law and The
        # implementing Regulation", "Data classification Policy and Regulations",
        # "Freedom of Information Policy and Regulations"). Without this every
        # document lands in one flat folder — the single nearest heading is the
        # same "[Laws and Regulations]" label for all 36.
        "group_headings": True,
    },
    "www.cbe.org.eg": {
        # CBE HAS NO CONTENT WRAPPER AT ALL. Measured 2026-08-24 on /en/governance:
        # no <main>, no role="main", no <article>, no #content, no .content. So
        # JS_MAIN_CONTENT falls through to <body> and captures the whole page --
        # the same situation as sio.gov.bh and rera.gov.bh.
        #
        # And there is nothing to name instead: the content sections are SIBLINGS
        # of the furniture inside div.site-container.main, in this order --
        #   <header>  accessibilityToolbar  breadcrumbs  <content...>  pagenote
        #   <footer>
        # -- so `content_selector` cannot select the content without also picking
        # a wrapper that holds the chrome. Naming the chrome is the only option
        # here, exactly as SIO's note argues.
        #
        # Each selector below, and what it removes (chars per page, measured on
        # /en/governance, /en/laws-regulations/laws/banking-laws and
        # /en/aml-cft/related-regulations):
        #
        #   .cbe-accessibility-toolbar  12   "A A contrast" -- the font-size and
        #       contrast strip. It is a <section> OUTSIDE </header>, which is why
        #       the junk list's `header` entry never touched it, and it is the
        #       reason every CBE page's captured text used to OPEN with the three
        #       words "A", "A", "contrast" before any of the document.
        #
        #   .most-searched              51   "Most Searched: Inflation Targets,
        #       Financial Stability" -- sits inside the hero block. Both its links
        #       point at /en/search-results, so it is also junk LINKS on every
        #       page, and being outside <header> they are not marked chrome.
        #
        # DO NOT reach for [data-comp-name="pagenoteLayout"] instead of .pagenote.
        # CBE renders the page's REAL body under that same comp name: 851 chars of
        # law titles on banking-laws, 461 on aml-cft/related-regulations. Only the
        # trailing note carries `pagenote`, and dropping by comp name would delete
        # the document.
        #
        # NOT dropped, deliberately: the `breadcrumbs` section. It also holds the
        # page's <h1>, which JS_DOC_TITLE reads, so removing it costs the title.
        # The search box itself needs nothing -- data-comp-name search-bar-toggle
        # and search-btn-toggle are both INSIDE <header> and the junk list already
        # removes them (measured: 0 of 4 pages have either outside the header).
        #   .relatedlinks-section    57-116  "In this section: Internal Audit
        #       Charter, ..." -- the in-page anchor nav. Measured on the four
        #       captures that have one: 2-4 links, never more than 116 chars, and
        #       never any prose.
        #
        #       NOT `.overview-column-layout`, which is TWO LEVELS UP and holds
        #       the document. That block is
        #           .overview-column-layout > .two-column-layout >
        #               .right-section-overview  -> .relatedlinks-section (nav)
        #               .left-section-overview   -> section.rich-texts (the TEXT)
        #       so dropping the outer class deletes the page body: measured 809 of
        #       1,558 chars on governance/internal-audit -- 56% of the page -- plus
        #       960 on aml-cft/egyptian-fiu, which is where the law is quoted.
        #
        #   .Image-Container         1,583 bytes of html, ZERO text -- the hero
        #       banner: a <picture> with three srcset breakpoints, a base64 LQIP
        #       placeholder, and the .bg/.blur overlay divs. Presentation only.
        #       It does not reduce network traffic -- the browser has already
        #       fetched the banner by capture time, so `asset_failures` is
        #       unaffected.
        #
        #   .ui-helper-hidden-accessible  jQuery UI's aria-live region, injected
        #       at body level and always empty. 109 bytes of nothing.
        #
        #   .breadcrumbdropdown      the mobile breadcrumb's collapsed <ul>. Present
        #       on 62 of 62 captures, 43 bytes each, and EMPTY on every one of them
        #       -- 0 characters of text in total.
        #
        #       It survived the junk list because that list names `[hidden]`, the
        #       HTML ATTRIBUTE, and this is Tailwind's `class="hidden"` -- a
        #       display:none utility the attribute selector does not match.
        #
        #       Dropping bare `.hidden` would catch all 64 such elements on this
        #       site (3,138 bytes, 28 chars of text) and was NOT done: Tailwind
        #       pairs `hidden` with `md:block` to mean "hidden on mobile, VISIBLE on
        #       desktop", so the class is not a reliable statement that content is
        #       furniture. Measured today CBE has zero such elements carrying text,
        #       but that is a fact about this month's markup, not about the class.
        #
        # MEASURED on the deepest governance page (project-risk): the capture was
        # 13,729 bytes for 2,229 characters of actual document -- 18%. The toolbar
        # alone was 3,942 bytes, 29% of the file, for twelve characters of text.
        #
        #   section[data-comp-name="breadcrumbs"] .breadcrumbs   1,214 bytes / 84
        #       chars -- the visible trail, which the `breadcrumb` column already
        #       holds properly. Its first <li> is EMPTY (it existed only to hold
        #       the .breadcrumbdropdown above), so the saved file rendered a stray
        #       bullet over the trail in a browser. SCOPED to the hero on purpose:
        #       a bare `.breadcrumbs` is a common enough class to catch something
        #       else the day CBE reskins.
        #
        # WHAT IS LEFT IN, AND WHY:
        #   .newsupdateddata -- the page's own date, and it MOVES. Corrected
        #       2026-08-24: three sampled pages all read "23 Mar 2023" and it was
        #       first written up here as a static string carrying nothing. It is
        #       not. governance/compliance reads 09 Feb 2026 while
        #       .../market-risk reads 23 Mar 2023. This is the publisher's own
        #       last-updated stamp, and keeping it inside the captured TEXT is what
        #       lets a page whose only change is that stamp still classify as
        #       `modified`. The `.pagenote` section at the foot of the page repeats
        #       the same date, so dropping that one costs nothing -- dropping BOTH
        #       would make a date-only update invisible.
        #
        #   the hero <section> itself, which is what holds .newsupdateddata and the
        #       <h1>. Removing it would also close the ~500px white gap the saved
        #       HTML renders now, because JS stamps a computed
        #       style="height: 501.203px" on the section before capture and
        #       dropping .Image-Container leaves that height behind with nothing in
        #       it. The gap is cosmetic in a browser; the change stamp is not.
        #
        #   content images: /-/media/.../rich-text/others/compliance.png sits in a
        #       rich-texts section and is part of the document, not furniture.
        #
        # NONE OF THIS AFFECTS DISCOVERY. JS_LINKS and JS_BREADCRUMB query the
        # LIVE document; drop_selectors applies to JS_MAIN_CONTENT's clone. So a
        # selector here can never lose a document or a folder level -- only
        # captured text. It CAN delete real text, which is what the
        # .overview-column-layout note above is about.
        "drop_selectors": (
            ".cbe-accessibility-toolbar, "
            ".most-searched, "
            ".relatedlinks-section, "
            ".Image-Container, "
            ".ui-helper-hidden-accessible, "
            # COMPOUND, not descendant. `.hidden breadcrumbdropdown` -- the form
            # this had on 2026-08-24 -- reads as "an element of TYPE
            # breadcrumbdropdown inside something with class hidden". Both classes
            # are on the SAME <ul class="hidden breadcrumbdropdown">, and there is
            # no such element type, so it matched 0 of 62 captures. It is also
            # valid CSS, so the try/catch around drop_selectors never fired: the
            # removal simply never happened, silently. Verified: the descendant
            # form matches 0, `.hidden.breadcrumbdropdown` matches 1.
            ".hidden.breadcrumbdropdown, "
            # RESTORED 2026-08-28. Every CBE page ends with a grey line reading
            # "This page was last updated 13 Aug 2026". It is template furniture,
            # and leaving it in cost more than a stray sentence:
            #
            # GenericSiteCrawler._is_link_wrapper drops a page whose text is only
            # its own title and a date -- that is how the Risk Appetite Statement
            # page, which holds nothing but a heading and a download link, is kept
            # out of the library while its PDF is kept in. The rule measures the
            # RESIDUE after removing the title and the date stamp, against
            # WRAPPER_RESIDUE_CHARS = 20.
            #
            # With .pagenote stripped the residue is 0. Without it the words
            # "This page was last updated" survive -- 26 characters, six over the
            # threshold -- so the page was recorded as a document, the file rule
            # then pointed it at the same PDF, and `check` rejected the workbook
            # for two rows sharing one identity (2026-08-28, cbe_clean.xlsx).
            #
            # The fix is to remove the furniture, NOT to raise the threshold:
            # that number is deliberately not a "short pages are junk" rule --
            # SAMA's "Article 3" is 184 characters of real law.
            ".pagenote, "
            'section[data-comp-name="breadcrumbs"] .breadcrumbs'
        ),
    },
}

DEFAULT_PROFILE = {
    "breadcrumb_current": False,
    #: Read the breadcrumb container's <li>s as the crumbs, link or not. For a
    #: site whose current crumb is a bare <li> with no class and no aria marker.
    "breadcrumb_li": False,
    #: Keep <button> in the captured content, for a site whose section headings
    #: ARE buttons (an accordion). Off everywhere else, where a button is a
    #: control and its label is not part of the document.
    "keep_buttons": False,
    #: Clear the inline `visibility: hidden` a scroll-animation library leaves on
    #: elements that never came into view. Neutralises the style, never deletes
    #: the element: the text was already captured, it is the saved page that
    #: renders blank.
    "unhide_animated": False,
    "unwrap_forms": False,
    "sharepoint_main": False,
    "group_headings": False,
    "keep_modals": False,
    #: A CSS selector for the site's real content wrapper, prepended to
    #: JS_MAIN_CONTENT's list. Empty means "use the defaults", which is what every
    #: host without an entry does.
    "content_selector": "",
    #: When several documents share one title, name them after their SECTION
    #: instead of their URL slug -- and drop that crumb from `section_path`, since
    #: it has become the title.
    #:
    #: `disambiguate_titles` exists because a title shared by different documents
    #: is not a title. Its replacement is the URL slug, which is right for SDAIA
    #: (whole groups of quarterly reports linked as "2025", named properly by
    #: their filenames) and second-best where the page already states the real
    #: name one level up.
    #:
    #: MEASURED on pdp.gov.bh/en/executive-decisions.html: each of the ten
    #: executive decisions is a PAIR of links whose text is the Arabic
    #: "download in Arabic" and "Download in English". Neither is in
    #: GENERIC_LINK_TEXT, so both survive best_doc_title; then ten documents share
    #: each of the two titles, every one is rewritten from the slug, and the result
    #: reads
    #:     Regarding Data Protection Guardians / Trans Order Auditor Tasks
    #: where the decision's name is the only real title on the page. With this on:
    #:     Regarding Data Protection Guardians  <- the title, and the only folder
    #:
    #: OPT-IN, because it would change every site that currently gets a slug out
    #: of a collision. On SDAIA the colliding links sit under a SHARED heading, so
    #: naming them after it would put the collision straight back -- the slug is
    #: the better answer there and stays the default.
    #:
    #: It only ever applies to a title `disambiguate_titles` was going to rewrite
    #: anyway, so a document with a title of its own is never touched.
    "heading_is_title": False,
    #: The ANCHOR TEXT THAT LED HERE becomes the outer folder for this page's
    #: documents.
    #:
    #: A filtered listing — /en/forms?tag=326 — is a page whose subject is named
    #: nowhere on it except in the rail link that reached it. Its own headings are
    #: the SUBcategories, and its breadcrumb is the unfiltered parent, so a crawl
    #: files its documents exactly where the unfiltered page files them and the
    #: filter's whole contribution is lost.
    #:
    #: This reads `linked_from_title`, which the walker already records for every
    #: queued url, so the label is DISCOVERED. That is the point: hardcoding the
    #: eight known tags as eight sources cannot notice a ninth.
    #:
    #: MEASURED on moic.gov.bh/en/forms: 8 rail anchors, and with this on the
    #: documents of ?tag=326 read
    #:     Business Services & Bahrain Investors Center Services > Companies Control
    #: instead of "Companies Control" alone. Skipped for the seed, which nothing
    #: linked to, and skipped when the label already opens the trail.
    "link_title_is_section": False,
    #: A PAGE THAT YIELDED NO DOCUMENT still contributes one row, so the folder
    #: it sits in exists. The value is that row's title; setting it turns this on.
    #:
    #: Folders are built from the trails of documents, so a category that
    #: publishes nothing has no folder and cannot be told apart from a category
    #: the site never had. That matters most on exactly the sites where the
    #: categories are DISCOVERED: `link_title_is_section` finds them by walking,
    #: and an empty one would be found and then silently dropped.
    #:
    #: PER PAGE, not per source. The wrapper has `placeholder_when_empty` for a
    #: source that yields nothing, and it cannot help here: one source now walks
    #: every category, so the source is not empty even when six of its pages are.
    #:
    #: MEASURED on moic.gov.bh/en/forms: the walk visits the index plus 8
    #: categories; tag=326 gives 10 forms, tag=325 gives 1, and the other SIX give
    #: zero — tag=324 and tag=319 return zero characters of content and zero
    #: links. So six placeholders, and none for the index, which has 12.
    #:
    #: The row points at the page itself, which is honest and distinct per
    #: category, so identity separates the placeholders instead of collapsing
    #: them. Only for pages that are IN SCOPE and were really read: a page that
    #: errored or was blocked is not an empty page and gets nothing.
    "empty_page_placeholder": "",
    #: One file found under SEVERAL trails keeps only the DEEPEST.
    #:
    #: Normally a document in two sections is cross-listed on purpose, and both
    #: placements are kept. On a site that publishes the same list unfiltered AND
    #: per category, the two placements are not two sections — they are one
    #: document seen twice, once with its category and once without.
    #:
    #: MEASURED on moic.gov.bh/en/forms: 11 of the 12 forms appear on both the
    #: unfiltered page and exactly one tag page. Without this the tree carries
    #: them twice; with it the categorised trail wins and the 1 orphan that no tag
    #: returns still arrives from the unfiltered page. Ties keep the first seen.
    "prefer_deepest_section": False,
    #: Under `keep_modals`, END the page's own text where its CHILD RECORDS
    #: begin — at the heading block that opens the accordion.
    #:
    #: A page of panels is an index over its children plus whatever it says in
    #: its own voice. On pdp.gov.bh/en/regulations.html that own voice is the
    #: law's preamble: the royal citation list, First through Fourth Article, and
    #: the enactment date. Everything after it is Part > Section > Article, all of
    #: which is already stored as 60 child records under 11 folders.
    #:
    #: MEASURED on that page: the captured text went 119,277 -> 3,618 characters
    #: and what remains ends at "12/07/2018 A.D." — the preamble, nothing else.
    #: MEASURED on sio.gov.bh/en/ministerial-orders, the other `keep_modals`
    #: host: 17,441 -> 17,441, unchanged even with this on, because its 47
    #: triggers have no heading block above them to cut at.
    #:
    #: THE BOUNDARY IS THE CONTIGUOUS HEADING BLOCK, not the outermost heading.
    #: Three earlier attempts cut at the outermost heading above the first panel
    #: and every one of them returned an EMPTY page, because on this host the
    #: outermost heading above the panels is the page title and the preamble
    #: hangs below it:
    #:     h2 The Law                        <- page title
    #:     h3 The Law
    #:     h4 The Shura Council ...          <- the preamble is headings too
    #:     h5 First Article .. Fourth Article, 28 /10/1439 A.H. 12/07/2018 A.D.
    #:     h4 Personal Data Protection Law   <- the children start HERE
    #:     h5 Section One
    #:        button Article (1) Definitions
    #: So the walk goes backwards from the first trigger and steps only from a
    #: heading to a heading OUTSIDE it (h5 then h4), stopping the instant
    #: anything else intervenes — which is what separates "Section One" and
    #: "Personal Data Protection Law" from the h5s of the preamble above them.
    #: h1 and h2 are never a boundary: those are page titles.
    "page_text_stops_at_children": False,
    #: Append the short paragraph that FOLLOWS a heading to the heading itself,
    #: joined with " - ".
    #:
    #: Some documents split a division's number from its subject:
    #:     <h5>Section Three</h5><p>Transfer of Personal Data outside the Kingdom</p>
    #: MEASURED on pdp.gov.bh/en/regulations.html, which does this for all eleven
    #: divisions of the Personal Data Protection Law. Without it the folders read
    #: "Section One", "Section two", "Section Three" three times over — accurate
    #: and unusable, because a law has a Section One in every Part.
    #:
    #: Only the IMMEDIATELY following sibling, only when it is short, and only
    #: when it differs from the heading, so a heading followed by a body
    #: paragraph is left alone.
    "heading_subtitle": False,
    #: A selector for elements that ACT as headings on this host but are not
    #: h1-h6. Added to the two heading lists JS_LINKS builds, so `group` and
    #: `heading_path` — and through them `section_path` and the folder trail —
    #: can see them.
    #:
    #: MEASURED on pdp.gov.bh/en/executive-decisions.html: each decision is
    #:     <div class="executive-decisions-pdf">
    #:       <div class="block">Regarding Data Protection Guardians</div>
    #:       <div class="block"> <a>عربية</a> <a>Download in English</a> </div>
    #: so the name of the decision — the folder every one of its documents
    #: belongs in — is a bare DIV. Without this every document on the page came
    #: back under the single page heading and the twenty decisions collapsed
    #: into one.
    #:
    #: Ranked BELOW every real heading (see rankOf), so a page that has both
    #: keeps its h1..h6 nesting and gains this as the innermost level.
    #:
    #: Off by default. A div is not a heading in general, and naming one as a
    #: heading on a site that did not ask for it would re-file its documents.
    "heading_selector": "",
    #: Substrings of a DOCUMENT URL that mean "this is site furniture, not a
    #: document of this section", comma-separated.
    #:
    #: config/sources/*.yml already has an `exclude_documents` list and the
    #: wrapper applies it — but only on the wrapper's path. A direct
    #: `crawler.py --url ...` run knows nothing about it, so every pages.xlsx
    #: still carried the excluded file: measured on moic.gov.bh, where
    #: /sites/default/files/2030-vision/vision2030-en.pdf appeared in all 14
    #: exploratory crawls while the workbook was correctly clean.
    #:
    #: Site-wide furniture belongs to the HOST rather than to one source config,
    #: which is why it is here as well. Excluded documents are recorded in the
    #: run's `chrome_dropped` audit, never silently discarded — the same
    #: treatment header/footer links get.
    "exclude_document_urls": "",
    #: Extra selectors to delete from the captured content, comma-separated.
    #:
    #: For furniture that sits INSIDE the content wrapper, so naming the wrapper
    #: cannot exclude it. One list rather than a boolean per widget: the next site
    #: with a sidebar needs an entry here, not another flag.
    #:
    #: It only edits the CAPTURED HTML. JS_LINKS reads the live document
    #: separately, so nothing here can hide a link from the crawl or drop a
    #: document from the documents sheet.
    "drop_selectors": "",
}


def _excluded_doc_url(url: str, prof: dict) -> bool:
    """Is this document url site furniture, per the host profile?

    Substring match, like the wrapper's `exclude_documents`, so a directory
    ("/2030-vision/") survives a re-upload under a new filename.
    """
    pats = (prof or {}).get("exclude_document_urls") or ""
    if isinstance(pats, str):
        pats = [p.strip() for p in pats.split(",")]
    for p in pats:
        if p and p in url:
            return True
    return False


def profile_for(url: str) -> dict:
    """Per-host switches for the site-specific fixes; defaults = pre-existing
    behaviour for every host not listed."""
    prof = dict(DEFAULT_PROFILE)
    prof.update(SITE_PROFILES.get(urlparse(url).netloc.lower(), {}))
    return prof


# ============================================================================
# SECTION B — browser-side JavaScript snippets
# These strings are handed to the browser and run INSIDE the page (via
# page.evaluate). That is how we read what a real user sees after JS renders.
# Snippets that a profile can change take an `opts` argument (the profile dict).
# ============================================================================
# ---------------- browser-side extraction (runs as JS in the page) --------------

# Pull the breadcrumb trail as a list of visible link/label texts.
JS_BREADCRUMB = r"""
(opts) => {
  opts = opts || {};
  const isSep = t => !t || /^[>›—–\-|/·]+$/.test(t);   // separator-only or empty
  const sels = ['.breadcrumb','.bread-crumb','nav[aria-label*="readcrumb" i]',
                'ol[class*="crumb"]','[class*="rumb"]'];
  for (const s of sels) {
    for (const el of document.querySelectorAll(s)) {
      // skip the hidden PDF-clone breadcrumb (its separators are the only tagged text)
      if (el.closest('[aria-hidden="true"], #pdfDownloadLayout') ||
          /pdf/i.test(el.className || '')) continue;
      // Only crumbs a real user can SEE. Bilingual sites (aml.gov.sa) ship the
      // other language's home link in a display:none span — including it would
      // poison the section anchor and make breadcrumb scope match every page.
      // Rendered-box test, not a style test: display:none on an ANCESTOR leaves the
      // crumb's own computed display as "inline", so only asking about the node
      // itself would keep the hidden crumb. No box => the user cannot see it.
      const visible = n => !opts.breadcrumb_current ||
                           ((n.getClientRects().length > 0 ||
                             n.offsetWidth > 0 || n.offsetHeight > 0) &&
                            getComputedStyle(n).visibility !== 'hidden');
      // Breadcrumbs are links, BUT the current (last) crumb is often NOT a link —
      // it's a <span class="breadcrumbCurrent">/[aria-current]. Reading those too
      // is opt-in: it can pick a DIFFERENT breadcrumb container than anchors-only
      // did (measured on CMA: "Capital Market Authority" -> "Home") and it moves
      // the scope anchor from the parent section to the current page.
      const sel = opts.breadcrumb_current
        ? 'a, [class*="current" i], [aria-current], [class*="active" i]'
        : 'a';
      // `breadcrumb_li`: THE CRUMBS ARE THE <li>s, LINK OR NOT.
      //
      // pdp.gov.bh writes the trail as
      //     <ul><li><a href="index.html">Home</a></li>
      //         <li class="sep"><span class="fal fa-angle-double-left"></span></li>
      //         <li>Forms</li></ul>
      // so the current crumb is a BARE <li> — no anchor, no [aria-current], no
      // "current"/"active" class. Anchors-only reads ['Home'], and
      // `breadcrumb_current` reads ['Home'] too, because the selectors it adds
      // match nothing here. The <li> fallback below would have caught it, but it
      // only fires when NOTHING matched, and 'Home' is something.
      //
      // Measured on /en/forms.html, /en/executive-decisions.html and
      // /en/regulations.html: section_path was "Home" on all three, so every
      // document filed under the site root instead of its own section.
      //
      // Separator <li>s cost nothing: their text is an icon span, so `isSep`
      // drops them on emptiness.
      let parts;
      if (opts.breadcrumb_li) {
        parts = Array.from(el.querySelectorAll('li')).filter(visible)
          .map(n => n.textContent.trim()).filter(t => !isSep(t) && t.length < 200);
      } else {
        let nodes = Array.from(el.querySelectorAll(sel));
        // drop a node that merely CONTAINS another crumb node (avoid double counting)
        nodes = nodes.filter(n => !nodes.some(o => o !== n && n.contains(o)));
        parts = nodes.filter(visible)
          .map(n => n.textContent.trim()).filter(t => !isSep(t) && t.length < 200);
        if (!parts.length)
          parts = Array.from(el.querySelectorAll('li')).filter(visible)
            .map(n => n.textContent.trim()).filter(t => !isSep(t) && t.length < 200);
      }
      const out = [];
      for (const p of parts) if (out[out.length-1] !== p) out.push(p);
      if (out.length) return out;
    }
  }
  return [];
}
"""

# Main content: clone the best content container, then strip everything that isn't
# the actual document text. Critically this removes:
#   - HIDDEN PDF-CLONE blocks (SBP circulars render the body twice: once visible,
#     once inside a hidden #pdfDownloadLayout/#pdfContentClone used to make their PDF)
#     -> without stripping these the saved HTML is DUPLICATED.
#   - buttons like "Download PDF", the accessibility widget, banners, breadcrumb,
#     back-to-top — page chrome we don't want in the captured content.
JS_MAIN_CONTENT = r"""
(opts) => {
  opts = opts || {};
  // Extra containers are opt-in: querySelector returns the first match in DOCUMENT
  // order, not selector order, so widening this list can change which element is
  // picked on a site that already matched one.
  const sels = 'main, [role="main"], article, #content, .content, #main' +
               (opts.sharepoint_main ? ',[id*="PlaceHolderMain"], #contentBox, .main-content' : '');
  // A per-host content wrapper wins over the generic list. querySelector returns
  // the first match in DOCUMENT order, not selector order, so the site's own
  // container has to be asked for SEPARATELY or a shallower generic match would
  // beat it. Sites whose markup has no main/article/#content fall back to <body>
  // and capture the entire page — measured on sio.gov.bh (143 chrome divs
  // surviving) and rera.gov.bh (43). Naming the wrapper excludes chrome by
  // SELECTION, so a widget added later is outside it automatically and there is
  // no junk list to keep extending.
  let pick = null;
  if (opts.content_selector) {
    try { pick = document.querySelector(opts.content_selector); } catch (e) { pick = null; }
  }
  if (!pick) pick = document.querySelector(sels);
  const src = pick || document.body || document.documentElement;
  if (!src) return { html: '', text: '' };   // frameset top docs have no <body>
  const clone = src.cloneNode(true);
  // ASP.NET / SharePoint wrap the ENTIRE page in <form id="aspnetForm">. Removing
  // <form> outright then deletes the whole page (we saw 0-byte HTML on aml.gov.sa).
  // So when opted in: UNWRAP content-bearing forms (keep children, drop the
  // wrapper) and only REMOVE small ones, the real search/subscribe widgets.
  if (opts.unwrap_forms) {
    clone.querySelectorAll('form').forEach(f => {
      const meaty = (f.innerText || '').trim().length > 400 ||
                    f.querySelector('h1,h2,h3,h4,table,ul,ol,article');
      if (meaty) { while (f.firstChild) f.parentNode.insertBefore(f.firstChild, f); }
      f.remove();
    });
  }
  // `keep_modals` does NOT inline the panels. Each one becomes its own record
  // (see JS_MODALS below and the collector in crawl()), because a page holding 47
  // laws is one document where the library wants 47. Two things happen here:
  //
  //   - the trigger's dead href is repaired: javascript:void(0) becomes
  //     "#<id>", which absolutize_html turns into <page>#<id> — the same url the
  //     per-law record is stored under, so the page reads as an index over its
  //     own children.
  //   - the PANEL IS REMOVED, so the page does not also carry the text of every
  //     child. This comment used to claim the junk sweep already did that. It
  //     only did so by accident: a Bootstrap MODAL carries aria-hidden="true"
  //     and the sweep drops it, but a Bootstrap ACCORDION panel is a plain
  //     .collapse and survived. MEASURED on pdp.gov.bh/en/regulations.html:
  //     80,523 of the page's characters were its own 61 panels, stored a second
  //     time as 61 child records. On sio.gov.bh, whose panels are modals, this
  //     changes the captured text by 0 characters.
  if (opts.keep_modals) {
    clone.querySelectorAll('a[data-bs-target], [data-target]').forEach(a => {
      const t = a.getAttribute('data-bs-target') || a.getAttribute('data-target') || '';
      if (t.charAt(0) === '#') a.setAttribute('href', t);
      if (t.length > 1) {
        let p = null;
        try { p = clone.querySelector('#' + CSS.escape(t.slice(1))); } catch (e) {}
        if (p) p.remove();
      }
    });
  }
  // `keep_buttons`: THE HEADINGS ARE BUTTONS ON SOME SITES.
  //
  // 'button' is in this list because a button is normally a control — "Download
  // PDF", "back to top". On an accordion it is the HEADING. Measured on
  // pdp.gov.bh/en/regulations.html: 62 buttons carrying 2,848 characters, every
  // one of them an article title ("Article (1) Definitions", "Article (2) Scope
  // of Application", ...), while the 61 .collapse panels holding the 98,087
  // characters of article text survive. So the law was captured as one
  // undifferentiated run of text with every heading deleted.
  //
  // Opt-in per host: leaving buttons in by default would put "Download PDF" and
  // "Submit" back into the text of every other site.
  const junk = [
    'script','style','noscript','nav','aside','header','footer','iframe',
    ...(opts.keep_buttons ? [] : ['button']),
    ...(opts.unwrap_forms ? [] : ['form']),
    '[aria-hidden="true"]','[hidden]','.no-print',
    // The SITE'S OWN "this is not content" marker. Bootstrap's d-print-none means
    // "hide when printing", which is what a page says about its own furniture.
    // rera.gov.bh puts its Back / share / print / font-size toolbar in a
    // <ul class="d-print-none"> INSIDE #page-content, so naming the content
    // container cannot exclude it — only this can.
    '.d-print-none','[class*="print-none"]',
    '#pdfDownloadLayout','[id*="pdfDownload"]','[id*="Clone"]','[id*="clone"]',
    '#accessibility-modal','[id*="accessibility"]','.back-to-top',
    '.overlay-mega-menu','.bg-overlay','.pages-banner','.bread-crumb','.breadcrumb',
    '[style*="display:none"]','[style*="display: none"]'
  ];
  clone.querySelectorAll(junk.join(',')).forEach(n => n.remove());
  // `page_text_stops_at_children`: the page's own text ends where its children
  // begin. AFTER the junk sweep on purpose — a navbar toggler is a
  // [data-bs-target] too, and running this before <nav> was swept made the
  // navbar the first trigger and emptied the page.
  if (opts.keep_modals && opts.page_text_stops_at_children) {
    const trig = clone.querySelector('[data-bs-target], [data-target]');
    let bound = null;
    if (trig) {
      const flat = Array.from(clone.querySelectorAll(
          'h1,h2,h3,h4,h5,h6,[data-bs-target],[data-target]'));
      // Backwards from the accordion, heading to OUTER heading only. h1/h2 are
      // page titles and can never be the boundary. See the profile key's docs
      // for the measured heading order this reads.
      let rank = 7;
      for (let j = flat.indexOf(trig) - 1; j >= 0; j--) {
        const m = /^H([3-6])$/.exec(flat[j].tagName);
        if (!m) break;
        const r = +m[1];
        if (r >= rank) break;
        rank = r; bound = flat[j];
      }
    }
    if (bound) {
      try {
        const rg = clone.ownerDocument.createRange();
        rg.setStartBefore(bound);
        rg.setEnd(clone, clone.childNodes.length);
        rg.deleteContents();
      } catch (e) { /* no boundary we can act on: leave the page whole */ }
    }
  }
  // `unhide_animated`: A SCROLL ANIMATION LEFT THE PAGE INVISIBLE.
  //
  // WOW.js and its relatives set `visibility: hidden` inline on every element
  // carrying their trigger class, and only clear it when the element scrolls
  // into view. A crawler never scrolls, so whatever sat below the fold stays
  // hidden. MEASURED on pdp.gov.bh/en/regulations.html: 58 of 68 `.wow`
  // elements, which is most of the law — its preamble, and every "Section One /
  // Two / Three" heading between the articles.
  //
  // THE TEXT WAS NEVER LOST, which is why this is not a junk-list problem and
  // why deleting those elements would be exactly wrong: a detached clone's
  // innerText is textContent, so `text` already holds all 119,359 characters and
  // content_hash was always right. It is the SAVED HTML that renders as a
  // sequence of blank gaps, because the inline style travels with it.
  //
  // So the style is neutralised rather than the element removed, and only for a
  // host that asks: an inline `visibility: hidden` elsewhere may be deliberate.
  if (opts.unhide_animated) {
    clone.querySelectorAll('[style*="visibility"]').forEach(n => {
      if (/hidden/i.test(n.style.visibility || '')) n.style.visibility = 'visible';
    });
    clone.querySelectorAll('[style*="opacity"]').forEach(n => {
      if (parseFloat(n.style.opacity) === 0) n.style.opacity = '';
    });
  }
  // Per-host furniture that lives INSIDE the content wrapper. Wrapped in a
  // try/catch so one bad selector in a profile cannot empty a whole crawl — a
  // typo should cost the removal, not the page.
  if (opts.drop_selectors) {
    try {
      clone.querySelectorAll(opts.drop_selectors).forEach(n => n.remove());
    } catch (e) { /* invalid selector: keep the content, drop nothing */ }
  }
  return { html: clone.innerHTML, text: (clone.innerText || '').trim() };
}
"""

# ONE RECORD PER MODAL PANEL — for `keep_modals` hosts.
#
# sio.gov.bh publishes each law as a Bootstrap modal already in the DOM: the
# visible page is a list of titles, and clicking one opens the law's full text.
# Captured as a page, that is ONE document holding up to 49 laws. The library
# wants one row per law, which is what this returns.
#
# MEASURED 2026-08-20 across SIO's eight legislation pages: 202 panels, 202
# UNIQUE ids, a title on every one from both the trigger and the panel's own
# heading (and the two agree).
#
# THE ID IS FOR IDENTITY, NEVER FOR A NAME. Every SIO id begins
# "amiri-decree-law-no-1976-27-concerning-amendments-to-articles-38-and-139-of-
# social-insurance-law-" and differs only in a trailing number, whatever the law
# actually is — a template artefact. So the url is <page>#<id>, which is unique
# and is the site's own fragment, and the TITLE comes from the trigger text.
#
# 122 of the 202 read only "This content will be published soon". They are kept
# deliberately: the title is real, SIO listing a law it has not published yet is
# a fact worth holding, and the day it publishes one the text changes and change
# detection reports `modified`.
JS_MODALS = r"""
(opts) => {
  opts = opts || {};
  const out = [], seen = new Set();
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  // A file's size and page count are never part of a regulation. Inside a modal
  // sio.gov.bh renders them as a CLASS-LESS <div> after <h4>Download Document</h4>
  //     <h4>Download Document</h4><div>2.7 MB, 58 Pages</div>
  // so no selector can name it. Matched on its text instead, and only when the
  // element's WHOLE text is the measurement, so a sentence that happens to
  // mention a size is untouched.
  const SIZE_ONLY = /^\s*\d+(?:[.,]\d+)?\s*(?:[KMGT]i?B|bytes?)?\s*[,;]?\s*(?:\d+\s*pages?)?\s*$/i;
  const isSizeOnly = t => {
    const s = (t || '').trim();
    if (!s || s.length > 30) return false;
    if (!/\d/.test(s)) return false;
    if (!/(?:[KMGT]i?B|bytes?|pages?)/i.test(s)) return false;
    return SIZE_ONLY.test(s);
  };
  const scrub = el => {
    // Same drop_selectors the page capture uses — a modal is content too.
    if (opts.drop_selectors) {
      try { el.querySelectorAll(opts.drop_selectors).forEach(n => n.remove()); }
      catch (e) { /* invalid selector: drop nothing */ }
    }
    el.querySelectorAll('*').forEach(n => {
      if (n.children.length === 0 && isSizeOnly(n.textContent)) n.remove();
    });
    return el;
  };
  // THE HEADINGS A PANEL SITS UNDER, in document order.
  //
  // A page of panels is not flat. PDPA's Personal Data Protection Law is
  // Part > Section > Article, and the Part and Section are plain h4/h5 elements
  // BEFORE the accordion — nothing links them to a panel but position. Without
  // this every one of the 60 articles inherited the page's own section_path and
  // landed in a single folder.
  //
  // textContent, never innerText: these headings are exactly the ones a scroll
  // animation leaves at `visibility: hidden` (see `unhide_animated`), and
  // innerText returns "" for a hidden element. Reading textContent is why this
  // works on a page the crawler never scrolled.
  //
  // Walks BACKWARDS from the trigger, keeping the nearest heading at each rank
  // and stopping at the first h1/h2 — so a Section from the PREVIOUS Part can
  // never leak into this one.
  const _flat = Array.from(document.querySelectorAll(
      'h1,h2,h3,h4,h5,h6,[data-bs-target],[data-target]'));
  // A heading's own text, plus the short paragraph that names it when the host
  // says its divisions are written that way (see `heading_subtitle`).
  const headingLabel = el => {
    const t = clean(el.textContent);
    if (!opts.heading_subtitle || !t) return t;
    const n = el.nextElementSibling;
    if (!n || !/^(P|SPAN|DIV|H6)$/.test(n.tagName)) return t;
    const sub = clean(n.textContent);
    if (!sub || sub === t || sub.length > 90) return t;
    return t + ' - ' + sub;
  };
  const headingTrailFor = el => {
    const at = _flat.indexOf(el);
    if (at < 0) return [];
    const best = {};
    for (let j = at - 1; j >= 0; j--) {
      const e = _flat[j];
      const m = /^H([1-6])$/.exec(e.tagName);
      if (!m) continue;
      const rank = +m[1];
      const t = headingLabel(e);
      if (!t || t.length > 160) continue;
      if (best[rank] === undefined) best[rank] = t;
      if (rank <= 2) break;
    }
    return Object.keys(best).sort().map(k => best[k]);
  };

  for (const a of document.querySelectorAll('[data-bs-target], [data-target]')) {
    const raw = a.getAttribute('data-bs-target') || a.getAttribute('data-target') || '';
    if (raw.charAt(0) !== '#' || raw.length < 2) continue;
    const id = raw.slice(1);
    if (seen.has(id)) continue;
    const m = document.getElementById(id);
    if (!m) continue;
    seen.add(id);
    // The BODY, not the whole dialog. Measured on sio.gov.bh: all 47 panels
    // carry a .modal-header whose only content is the close button, and none
    // has a .modal-footer, so storing the dialog stored that wrapper on every
    // law.
    // CLONED before scrubbing, so the live page is never mutated — the crawl
    // reads this same DOM again for links and for the page capture.
    const body = scrub((m.querySelector('.modal-body') || m).cloneNode(true));
    // innerText on a hidden element is unreliable, so read textContent and
    // normalise it ourselves. Computed ONCE, from the body.
    const text = (body.textContent || '').replace(/[ \t]+/g, ' ')
                                         .replace(/\n{3,}/g, '\n\n').trim();
    const head = m.querySelector('h1,h2,h3,h4,.modal-title');
    out.push({
      id: id,
      title: clean(a.innerText || a.textContent) || clean(head && head.textContent),
      text: text,
      html: (body.innerHTML || ''),
      heading_path: headingTrailFor(a),
      placeholder: /will be published soon/i.test(text)
    });
  }
  return out;
}
"""

# The real document title. Page <title>/banner is often generic ("Circulars"),
# so prefer a subject-like heading in the body, skipping banner/nav/breadcrumb.
JS_DOC_TITLE = r"""
() => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  const banned = el => el.closest('.pages-banner,.banner,header,nav,.bread-crumb,' +
                                  '.breadcrumb,footer,[aria-hidden="true"],#pdfDownloadLayout');
  // 1) The real title is the main heading (green <h2>): first h1/h2 that is NOT the
  //    site banner and not the generic word "Circulars".  (Do NOT trust
  //    .circular-subject here — SBP tags BOTH the recipient line and the subject
  //    with that class, so it's ambiguous.)
  for (const h of document.querySelectorAll('h1, h2')) {
    if (banned(h)) continue;
    const t = clean(h.innerText);
    if (t && t.length > 4 && !/^circulars?$/i.test(t)) return t.slice(0, 250);
  }
  // 2) fallbacks
  for (const sel of ['.pdf-title', 'h1.entry-title', '.page-title h1']) {
    const e = document.querySelector(sel);
    if (e && !banned(e)) { const t = clean(e.innerText); if (t.length > 4) return t.slice(0, 250); }
  }
  return '';
}
"""

# All anchors: absolute href + visible text + whether it's a pagination/nav link.
# `nav=true` links (page numbers, next/prev, pager containers) are followed at LOW
# priority so real content/detail pages get crawled before index pages.
JS_LINKS = r"""
(opts) => {
opts = opts || {};
// A host may name extra elements that act as headings (see `heading_selector`).
// Guarded: one bad selector in a profile must cost the extra headings, not the
// whole link harvest, and querySelectorAll throws on invalid syntax.
let extraSel = "";
if (opts.heading_selector) {
  try { document.querySelector(opts.heading_selector); extraSel = ", " + opts.heading_selector; }
  catch (e) { extraSel = ""; }
}
const isExtra = el => {
  if (!opts.heading_selector) return false;
  try { return el.matches(opts.heading_selector); } catch (e) { return false; }
};
// Headings, in document order, resolved ONCE per call (the DOM changes between
// calls — expand_tree reveals more — so this must not be cached on window).
const heads = Array.from(document.querySelectorAll('h1,h2,h3,h4,legend,caption' + extraSel))
  .map(h => ({ el: h, text: (h.innerText || '').replace(/\s+/g, ' ').trim() }))
  .filter(h => h.text && h.text.length < 120);

// Ranked headings, for the NESTED trail (heading_path) rather than the single
// nearest heading (group). h1..h6 nest; legend/caption/summary act as headings
// but sort below any real one. Headings inside nav/aside/header/footer label a
// widget, not the content, so they are excluded here (they are NOT excluded from
// `group`, whose behaviour must stay exactly as it was).
const rankOf = el => {
  const t = el.tagName;
  if (/^H[1-6]$/.test(t)) return parseInt(t[1], 10);
  if (el.getAttribute('role') === 'heading') {
    const n = parseInt(el.getAttribute('aria-level') || '0', 10);
    if (n >= 1 && n <= 6) return n;
  }
  if (t === 'LEGEND' || t === 'CAPTION' || t === 'SUMMARY') return 7;
  // A profile-named heading sits below every real one, so a page keeps its
  // h1..h6 nesting and gains this as the innermost level.
  if (isExtra(el)) return 6;
  return 0;
};
const inWidget = el => !!el.closest(
  'nav, aside, header, footer, [role="navigation"], [role="complementary"], '
  + '[role="banner"], [role="contentinfo"]');
const rankedHeads = Array.from(document.querySelectorAll(
    'h1,h2,h3,h4,h5,h6,legend,caption,summary,[role="heading"]' + extraSel))
  .map(h => ({ el: h, rank: rankOf(h),
               text: (h.innerText || h.textContent || '').replace(/\s+/g, ' ').trim() }))
  .filter(h => h.rank && h.text && h.text.length < 300 && !inWidget(h.el));
// Links are not always <a href>. Government SharePoint routinely ships a
// <button onclick="window.location.href='...'"> that navigates exactly like a
// link — CMA builds its entire article index that way, so every article was
// invisible to a walk that only reads anchors. Note the destination must come
// from the onclick: CMA's matching data-bs-target id is lowercase and 404s.
// Collected as pseudo-anchors so the rest of this function is unchanged.
const _cands = [];
for (const a of document.querySelectorAll('a[href]')) _cands.push({el: a, href: a.href});
for (const b of document.querySelectorAll('[onclick]')) {
  if (b.tagName === 'A' && b.hasAttribute('href')) continue;    // already have it
  const oc = b.getAttribute('onclick') || '';
  const m = oc.match(/location\.href\s*=\s*['"]([^'"]+)['"]/i);
  if (!m) continue;
  let abs = '';
  try { abs = new URL(m[1], location.href).href; } catch (e) { continue; }
  _cands.push({el: b, href: abs});
}
return _cands.map(({el: a, href: _href}) => {
  const t = (a.textContent || '').trim();
  let nav = false, el = a;
  for (let i = 0; i < 4 && el; i++) {
    const c = ((el.className && el.className.toString ? el.className.toString() : '') + ' ' +
               (el.id || '')).toLowerCase();
    if (/paginat|pager|page-numbers|page-nav/.test(c)) { nav = true; break; }
    el = el.parentElement;
  }
  const rel = (a.getAttribute('rel') || '').toLowerCase();
  if (rel === 'next' || rel === 'prev') nav = true;
  if (/^(\d+|«|»|<|>|‹|›|\.\.\.|next|previous|prev|first|last)$/i.test(t)) nav = true;
  // Context = text of the nearest row/card/item — this holds the real title + date
  // when the link itself is just a generic "Download" button (e.g. SECP tables).
  let ctx = '', row = a;
  for (let i = 0; i < 5 && row; i++) {
    const tag = row.tagName || '';
    const cn = (row.className && row.className.toString ? row.className.toString() : '');
    if (/^(TR|LI|ARTICLE)$/.test(tag) || /card|item|box|publication|row|result/i.test(cn)) {
      ctx = (row.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 250);
      break;
    }
    row = row.parentElement;
  }
  // SECOND PASS — only when the first found nothing a title could be made of.
  //
  // The class test above stops at the NEAREST row/card ancestor, which on a
  // card layout can be the wrapper around the button rather than the card.
  // MEASURED on cbj.gov.jo, whose mobile card list supplies 14 of its 24
  // documents:
  //
  //   div.col-8                      ''
  //   div.row py-2 mx-0              'View & Download'   <- pass 1 stops here
  //   div.col-12 border py-3 ...     'File Name MoU between CBJ and Banking
  //                                   Regulation and Supervision Agency of
  //                                   Turkey. 2011/09/06 Type Size 1898KB'
  //   div.row                        all 24 documents, 2663 chars
  //
  // The naming element carries no row/card class, so no widening of the class
  // test reaches it -- and the ancestor above it DOES match while holding the
  // whole list, which on other sites would hand a listing's text to a single
  // document as its title.
  //
  // So this pass ignores class entirely and stops on two honest signals: text
  // that still says something once button words are removed, and a container
  // that has not yet grown into the list. Guarded by `_meat(ctx) <= 3` so a
  // page where pass 1 already worked is untouched -- every regulator that has
  // a usable ctx today keeps exactly the one it has.
  const _meat = s => (s || '')
      .replace(/\b(download|pdf|view|click here|read more)\b/gi, '')
      .replace(/[^A-Za-z0-9]+/g, ' ').trim();
  if (_meat(ctx).length <= 3) {
    let up = a;
    for (let i = 0; i < 6 && up; i++) {
      // More than a couple of links means we have climbed out of the item and
      // into the list holding it; that text names the section, not this file.
      if (up.querySelectorAll && up.querySelectorAll('a[href]').length > 2) break;
      const t = (up.innerText || '').replace(/\s+/g, ' ').trim();
      if (_meat(t).length > 3 && t.length <= 250) { ctx = t; break; }
      up = up.parentElement;
    }
  }
  // Group heading = nearest heading BEFORE this link in document order. Listing
  // pages split their documents under on-page headings that never appear in the
  // breadcrumb (aml.gov.sa: "Laws and Regulations" / "Rules and Instructions"),
  // so without this the sub-section of the trail is lost.
  let group = '';
  for (const h of heads) {
    // compareDocumentPosition: 4 = a comes AFTER h -> h precedes the link, keep it.
    // Once a heading no longer precedes the link, every later one won't either.
    if (h.el.compareDocumentPosition(a) & 4) group = h.text; else break;
  }
  // heading_path = the NESTED trail. Walk the ranked headings that precede this
  // link, popping any of equal or shallower rank, exactly as the page nests them.
  // On SDAIA `group` returns the same "[Laws and Regulations]" for all 36
  // documents, while this returns the real section each one sits under.
  const stack = [];
  for (const h of rankedHeads) {
    if (!(h.el.compareDocumentPosition(a) & 4)) break;
    while (stack.length && stack[stack.length - 1].rank >= h.rank) stack.pop();
    stack.push(h);
  }
  const heading_path = stack.map(s => s.text);
  // Chrome = this link sits in the site header/footer, i.e. furniture repeated on
  // every page (Privacy Policy, the mega-menu, social icons) rather than this
  // page's content. Used to keep furniture OUT OF THE DOCUMENTS LIST only — a
  // chrome link is still followed as a page, because on some sites the mega-menu
  // is the only route to real content (SECP's masthead holds 86% of its links).
  // <nav>/<aside> are deliberately NOT treated as chrome: unlike header/footer
  // they are often a genuine in-page category sidebar.
  const chrome = !!a.closest('header, footer, [role="banner"], [role="contentinfo"]');
  return { href: _href, text: t.slice(0, 300), nav: nav, ctx: ctx, group: group,
           heading_path: chrome ? [] : heading_path,
           chrome: chrome,
           // title="..." often holds the full document name when the visible
           // link text is a bare year ("2025") or a generic word ("Policies").
           title_attr: (a.getAttribute('title') || '').trim().slice(0, 300) };
});
}
"""

# Structured section trail, for pages that group their links in tab panels or
# accordions rather than in a breadcrumb. Two levels: the panel's own heading
# ("Sectoral Legislations") and the group heading nearest the link ("Real estate
# sector"). Where a breadcrumb only says "Home > Activities", this is the real
# structure of the page.
#
# Measured coverage: MISA 89 links, SECP/SBP/SAMA/SDAIA 0 — it stays quiet on
# markup it does not recognise, which is why it needs no per-site flag.
JS_NAV_PATH = r"""
() => {
  const HEADING_SEL = 'h2,h3,h4,h5,h6';
  const clean = el => el ? (el.textContent || '').replace(/\s+/g, ' ').trim() : '';
  const SECTION_SELECTORS = ['.regulationContent', '[id$="Show"]',
                             '[class*="tab-panel" i]', '[class*="tabpanel" i]'];
  const closestSection = el => {
    for (const sel of SECTION_SELECTORS) {
      try { const f = el.closest(sel); if (f) return f; } catch (e) {}
    }
    return null;
  };
  const categoryLabel = (a, section) => {
    // the card/banner this link sits in usually carries the category heading
    const panel = a.closest('.showLawItems, [class*="banner" i], [class*="panel" i]');
    if (panel) { const t = clean(panel.querySelector(HEADING_SEL)); if (t) return t; }
    // mobile variant: the category is the <li> preceding the wrapper
    const mob = a.closest('[class*="MobItems" i], [class*="mob-items" i]');
    if (mob) {
      let sib = mob.previousElementSibling;
      while (sib) {
        if (sib.tagName === 'LI') { const t = clean(sib); if (t) return t; }
        sib = sib.previousElementSibling;
      }
    }
    // otherwise: the last heading inside the section that PRECEDES this link
    if (section) {
      const hs = Array.from(section.querySelectorAll(HEADING_SEL));
      const before = hs.filter(h =>
        !!(h.compareDocumentPosition(a) & Node.DOCUMENT_POSITION_FOLLOWING));
      if (before.length) return clean(before[before.length - 1]);
    }
    return '';
  };
  return Array.from(document.querySelectorAll('a[href]')).map(a => {
    const section = closestSection(a);
    const parts = [clean(section ? section.querySelector(HEADING_SEL) : null),
                   categoryLabel(a, section)].filter(Boolean);
    return { href: a.href, text: clean(a), nav_path: parts.join(' > ') };
  });
}
"""


# A "status" chip if the page shows one (e.g. In-Force).
JS_STATUS = r"""
() => {
  const el = Array.from(document.querySelectorAll('*'))
    .find(n => /status\s*:/i.test(n.textContent || '') && n.children.length < 4);
  if (!el) return '';
  const m = (el.textContent || '').match(/status\s*:\s*([A-Za-z\- ]{2,40})/i);
  return m ? m[1].trim() : '';
}
"""


# ============================================================================
# SECTION C — page actions (run against a loaded page, may click / read frames)
# ============================================================================


def _merge_links(store: dict, links) -> dict:
    """Merge links into an href-keyed store. First sighting wins, EXCEPT:

      * a link seen outside the header/footer even once is never chrome, and
      * a sighting that carries a heading trail beats one that lost it.

    Keying on href alone is deliberate. The same URL usually appears in both the
    content and the footer of a page; whichever copy happened to be seen first
    would otherwise decide whether a real document survives the chrome filter.
    Clicking (pagination, accordions) can also re-render a link without its
    surrounding headings, so an earlier, richer sighting must not be overwritten.
    """
    for l in links or []:
        h = l.get("href")
        if not h:
            continue
        if h not in store:
            store[h] = l
            continue
        if not l.get("chrome"):
            store[h]["chrome"] = False
        if not store[h].get("heading_path") and l.get("heading_path"):
            store[h]["heading_path"] = l["heading_path"]
        if not store[h].get("group") and l.get("group"):
            store[h]["group"] = l["group"]
        # A RICHER CONTEXT WINS, the same rule as the heading trail above.
        #
        # MEASURED 2026-09-09 on cbj.gov.jo: 34 document link sightings, 24
        # unique hrefs, 10 of them seen TWICE -- once from the row that names
        # the instrument, once from a "View & Download" button beside it:
        #
        #     ctx: 'MOU between CBJ and TRC 2022/04/30 686KB'
        #     ctx: 'View & Download'
        #
        # First-sighting-wins gave 14 of 24 the button. Its context reduces to
        # nothing under `_ctx_title`, so `best_doc_title` fell past it to the
        # url slug, and this site's slugs are guids.
        #
        # Compared through `_ctx_title` rather than raw length, so the winner is
        # judged by what a title would actually be made of: "View & Download" is
        # 15 characters and worth none of them.
        if _ctx_title(l.get("ctx")) and (
                len(_ctx_title(l.get("ctx")))
                > len(_ctx_title(store[h].get("ctx")))):
            store[h]["ctx"] = l.get("ctx")
    return store


def extract_all(page, opts=None):
    """Frame-aware extraction — the key to handling ALL site types.

    Old sites (e.g. SBP) use <frameset>: the top document has no <body>, and the
    real links/content live inside child <frame>s. Modern sites (e.g. SAMA) are a
    single document. This walks every frame, merges the links, keeps the richest
    content frame, and takes a breadcrumb/status from whichever frame has one — so
    the same code works for both without site-specific config.
    `opts` is the per-host profile (see SITE_PROFILES); None = default behaviour.
    Returns (breadcrumb, content{html,text}, status, links)."""
    opts = opts or DEFAULT_PROFILE
    breadcrumb, status = [], ""
    best = {"html": "", "text": ""}
    link_map = {}
    for fr in page.frames:                 # page.frames includes the main frame
        try:
            _merge_links(link_map, fr.evaluate(JS_LINKS, opts))
        except Exception:
            pass
        try:
            c = fr.evaluate(JS_MAIN_CONTENT, opts)
            if c and len(c.get("text", "")) > len(best["text"]):
                best = c
        except Exception:
            pass
        if not breadcrumb:
            try:
                b = fr.evaluate(JS_BREADCRUMB, opts)
                if b:
                    breadcrumb = b
            except Exception:
                pass
        if not status:
            try:
                s = fr.evaluate(JS_STATUS)
                if s:
                    status = s
            except Exception:
                pass
    return breadcrumb, best, status, list(link_map.values())


def collect_paginated_links(page, max_clicks=60):
    """Handle IN-PAGE (JavaScript) pagination — e.g. DataTables 'Show N entries' +
    page 1/2 buttons (SECP), where clicking a page re-draws the table with no URL
    change. Strategy: (1) set the page-size select to its largest option so more rows
    show at once, then (2) click Next repeatedly, harvesting links after each draw.
    Returns the union of all anchors seen across the in-page pages."""
    seen = {}

    def harvest():
        for fr in page.frames:
            try:
                for l in fr.evaluate(JS_LINKS):
                    h = l.get("href")
                    if h and h not in seen:
                        seen[h] = l
            except Exception:
                pass

    # (1) maximise a DataTables-style length menu ("Show N entries").
    try:
        for sel in page.query_selector_all("select"):
            vals = sel.evaluate("s => Array.from(s.options).map(o => o.value)")
            nums = [v for v in vals if str(v).lstrip("-").isdigit()]
            if not nums:
                continue
            # prefer "All" (-1) if present, else the biggest number
            best = "-1" if "-1" in nums else max(nums, key=lambda x: int(x))
            if best == "-1" or int(best) >= 50:
                sel.select_option(best)
                page.wait_for_timeout(1200)
                break
    except Exception:
        pass

    harvest()

    # (2) click "Next" until it's gone/disabled or nothing new appears.
    NEXT_SELECTORS = [".paginate_button.next", "a.paginate_button.next", "li.next a",
                      "a.next", "[rel='next']", ".pagination .next a", "button.next"]
    for _ in range(max_clicks):
        before = len(seen)
        el = None
        for s in NEXT_SELECTORS:
            cand = page.query_selector(s)
            if cand:
                cls = (cand.get_attribute("class") or "").lower()
                aria = (cand.get_attribute("aria-disabled") or "").lower()
                if "disabled" in cls or aria == "true":
                    continue
                el = cand
                break
        if el is None:
            break
        try:
            el.scroll_into_view_if_needed(timeout=800)
            el.click(timeout=1000)
            page.wait_for_timeout(800)
        except Exception:
            break
        harvest()
        if len(seen) == before:      # no new links -> we're done
            break
    return list(seen.values())


# ---------------------------------------------------------------------------
# Click-and-verify link revealing.
#
# The old approach guessed which element was the "Next" button from a hardcoded
# selector list, and expand_tree() clicked anything that looked like a toggle
# WITHOUT checking whether the click helped — or even whether it navigated away,
# which silently left the rest of the page's extraction running on the wrong
# document.
#
# This does not try to know which element is the right one. It clicks a plausible
# candidate and checks whether the page actually gained links; every click is
# budgeted, verified, and undone if it navigated. That makes it safe to run on a
# site nobody has looked at.
# ---------------------------------------------------------------------------

# Candidates are found in the page and stamped with a temporary id so Python can
# click them. Nothing here decides what "works" — the click loop does.
JS_CLICKABLES = r"""
() => {
  document.querySelectorAll('[data-crawl-id]').forEach(
    n => n.removeAttribute('data-crawl-id'));

  // NOTE: each regex MUST stay on one line. JavaScript has no /x flag, so a
  // literal broken across lines is a SyntaxError — and page.evaluate() would then
  // throw on every call, silently disabling this whole tier.
  const NEVER = /log\s*out|sign\s*out|logout|signout|delete|remove|unsubscribe|submit|register|sign\s*up|login|sign\s*in|print|download|share/i;
  const ADVANCE = /^\s*(next|more|load\s*more|show\s*(all|more)|view\s*(all|more)|see\s*more|older|newer|\d{1,4}|»|›|>>|>)\s*$/i;
  const ADVANCE_CLASS = /paginat|pager|page-numbers|page-nav|load-?more|show-?more/i;
  const EXPAND_CLASS  = /toggle|expand|collaps|accordion|tree|caret|chevron|has-children|dropdown/i;

  const out = [];
  let id = 0;
  for (const el of document.querySelectorAll(
      'a, button, [role="button"], [aria-expanded], summary, '
      + '[class*="toggle"], [class*="expand"]')) {
    const r = el.getBoundingClientRect();
    if (r.width < 2 || r.height < 2) continue;              // not on screen
    const cs = getComputedStyle(el);
    if (cs.visibility === 'hidden' || cs.display === 'none' || cs.opacity === '0') continue;

    const text = (el.innerText || el.textContent || '').replace(/\s+/g,' ').trim().slice(0,80);
    const aria = (el.getAttribute('aria-label') || '').trim().slice(0, 80);
    const cls  = ((el.className && el.className.toString) ? el.className.toString() : '')
                 + ' ' + (el.id || '');
    const label = (text + ' ' + aria).trim();
    if (NEVER.test(label) || NEVER.test(cls)) continue;

    // Only click things with NO real destination. A real href is already in the
    // crawl queue; clicking it just navigates away and wastes budget. We want
    // buttons, "#" links and javascript: links — controls that change the page.
    const href = el.getAttribute('href');
    if (el.tagName === 'A' && href && !/^\s*(#|javascript:)/i.test(href)) continue;
    if (/disabled/i.test(cls) || el.getAttribute('aria-disabled') === 'true' || el.disabled)
      continue;

    let kind = '';
    if (el.getAttribute('aria-expanded') === 'false' || EXPAND_CLASS.test(cls)) kind = 'expand';
    if (ADVANCE.test(label) || ADVANCE_CLASS.test(cls)) kind = 'advance';
    if (!kind) continue;

    el.setAttribute('data-crawl-id', String(id));
    out.push({ cid: id, kind: kind, label: label.slice(0,60), cls: cls.slice(0,80) });
    id++;
  }
  return out;
}
"""

REVEAL_CLICK_BUDGET = 25   # hard cap on clicks per page — stops runaway loops
REVEAL_SETTLE_MS    = 600  # how long to let the page redraw after a click
REVEAL_MAX_BARREN   = 1    # stop after the first click that gains nothing

#: Hard cap on how many pages a NUMBERED pager is walked (tier 0b below). LLOC's
#: "Latest Legislation" is 15; the cap allows a section to grow fourfold before
#: this, rather than the site, is what stops the walk. A run that hits it says so.
PAGER_MAX_PAGES = 60
#: Give up on a numbered pager after this many consecutive pages that add
#: nothing. Two, not one: a pager whose first step is a no-op is broken, but one
#: genuinely duplicate page in the middle of a long list is not a reason to stop.
PAGER_MAX_BARREN = 2
#: How long to wait for one pager step's content to actually replace the old.
PAGER_STEP_TIMEOUT_MS = 8000
#: Settle AFTER the link set is seen to change, before reading it. The change
#: fires on the first mutation, which may be part-way through the render.
PAGER_SETTLE_MS = 700
#: Attempts per pager PAGE. A step whose XHR was refused leaves the previous
#: page's rows on screen, which is indistinguishable from an empty page until it
#: is asked for again.
PAGER_STEP_ATTEMPTS = 3

#: How many times to load a page whose own scripts 404'd. Was effectively 2 (the
#: goto retry); a third attempt costs one page load only on a host that is
#: actually refusing assets, and on lloc.gov.bh each load is an independent roll.
ASSET_RELOAD_ATTEMPTS = 3
#: Pause before re-loading a page whose scripts were refused. Long enough for a
#: burst limiter to forget, short enough not to dominate a crawl.
ASSET_RELOAD_WAIT_MS = 2500

#: A page that answered 4xx/5xx is still recorded IF it carries at least this
#: much text, because a handful of CMSs serve real content under a wrong status
#: and silently dropping those would shrink crawls that work today. Below it, the
#: page is an error page and is skipped. lloc.gov.bh's throttled 404 body is
#: ~300 characters of IIS boilerplate.
HTTP_ERROR_MIN_TEXT = 600

# The old hardcoded guesses, kept as a cheap fast path before the discovery tier.
# ---------------------------------------------------------------------------
# NUMBERED PAGERS  ("1 2 3 ... 15", or a <select> of page numbers)
# ---------------------------------------------------------------------------
# The Next-button tiers below advance one step at a time and STOP at the first
# step that gains nothing (REVEAL_MAX_BARREN = 1). That is right for an infinite
# "Load more", and wrong for a pager that states its own length: if step 1 fails
# for any reason, the other fourteen pages are never even attempted.
#
# MEASURED on www.lloc.gov.bh/Legislation/Latest, 144 records over 15 pages:
# the reveal event read `"clicks": 1, "gained": 0` and the crawl recorded ten
# documents with `status: ok`.
#
# So a pager that ENUMERATES its pages is walked explicitly. Two forms, both
# site-agnostic:
#
#   select  a <select> whose option values are exactly the consecutive integers
#           1..N. That is a page-NUMBER menu. It is deliberately distinguished
#           from a page-SIZE menu ("Show 10 / 25 / 50 / 100 entries", which tier
#           0 maximises): 10,25,50,100 is not 1..N, so SECP's DataTables length
#           menu can never be mistaken for one.
#   links   sibling clickable controls whose visible text is 1..N — the ordinary
#           "1 2 3 4 5" pager, whether the site builds it from <a>, <span> or
#           <li>. The current page is usually not a link, which is why the set is
#           allowed to be missing exactly one member.
#
# THE CHANGE EVENT IS DISPATCHED NATIVELY, not through the site's framework, so a
# pager wired with an inline onchange="..." works without jQuery being involved
# in the dispatch. It cannot help if the site's OWN handler is missing — see the
# script-reload guard in crawl(), which is what makes that survivable.
JS_NUMBERED_PAGER = r"""
() => {
  const ints = xs => xs.every(x => /^[0-9]+$/.test(x));
  // A page-number menu: option values are exactly 1..N, in order, N >= 2.
  const selects = Array.from(document.querySelectorAll('select'));
  for (let i = 0; i < selects.length; i++) {
    const vals = Array.from(selects[i].options).map(o => (o.value || '').trim());
    if (vals.length < 2 || !ints(vals)) continue;
    const nums = vals.map(Number);
    let seq = true;
    for (let k = 0; k < nums.length; k++) if (nums[k] !== k + 1) seq = false;
    if (!seq) continue;
    return { kind: 'select', key: selects[i].id ? '#' + selects[i].id : String(i),
             byId: !!selects[i].id, pages: nums.length };
  }
  // A numbered link pager: clickable siblings labelled 1..N, at most one absent
  // (the current page is commonly rendered as plain text).
  const cand = Array.from(document.querySelectorAll('a, span, li, button'))
    .filter(n => n.children.length === 0 && /^[0-9]+$/.test((n.textContent || '').trim()));
  const byParent = new Map();
  cand.forEach(n => {
    const par = n.closest('ul, ol, nav, div');
    if (!par) return;
    if (!byParent.has(par)) byParent.set(par, []);
    byParent.get(par).push(n);
  });
  let best = null;
  byParent.forEach((nodes, par) => {
    const nums = Array.from(new Set(nodes.map(n => Number((n.textContent || '').trim()))))
                      .sort((a, b) => a - b);
    if (nums.length < 2 || nums[0] !== 1) return;
    const max = nums[nums.length - 1];
    if (max - nums.length > 1) return;      // at most one member missing
    if (!best || max > best.pages) best = { kind: 'links', key: '', pages: max };
  });
  return best;
}
"""

# Drive one step of a pager found by JS_NUMBERED_PAGER. Returns true if the page
# was asked to move; the CALLER decides whether anything was gained, because a
# request that was made and refused must not read as "the pager ended".
# What the page's link set looks like right now. Used to WAIT for a pager step to
# land, because `wait_for_load_state("networkidle")` is useless here: at the
# moment the step is driven the page IS idle — the XHR has not started — so the
# wait returns instantly and the harvest reads the OLD page. Measured on
# lloc.gov.bh: the select was driven successfully (`moved: True`) and the link set
# was still page 1 after 6 seconds of network-idle waiting.
JS_LINK_SIG = r"""
() => {
  const a = Array.from(document.querySelectorAll('a[href]'));
  return a.length + '|' + a.map(n => n.getAttribute('href')).join(',').slice(0, 4000);
}
"""

JS_PAGER_GOTO = r"""
(a) => {
  if (a.kind === 'select') {
    const el = a.byId ? document.querySelector(a.key)
                      : document.querySelectorAll('select')[Number(a.key)];
    if (!el) return false;
    const want = String(a.n);
    if (!Array.from(el.options).some(o => (o.value || '').trim() === want)) return false;
    el.value = want;
    // Native events, so an inline onchange="fetchResult(...)" fires without the
    // site's own framework being needed for the dispatch itself.
    el.dispatchEvent(new Event('input',  { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    return true;
  }
  const want = String(a.n);
  // RE-VERIFY THE PAGER CONTEXT AT CLICK TIME. Matching "the first leaf whose
  // text is 2" anywhere in the document would happily click a year, a table
  // cell or a footnote marker. The control must sit in a container that also
  // holds a sibling leaf reading "1" — which is what made it look like a pager
  // during detection.
  const leaves = Array.from(document.querySelectorAll('a, span, li, button'))
    .filter(n => n.children.length === 0);
  const hit = leaves.find(n => {
    if ((n.textContent || '').trim() !== want) return false;
    const par = n.closest('ul, ol, nav, div');
    if (!par) return false;
    return leaves.some(o => o !== n && par.contains(o) &&
                            (o.textContent || '').trim() === '1');
  });
  if (!hit) return false;
  hit.click();
  return true;
}
"""


KNOWN_ADVANCE_SELECTORS = [
    ".paginate_button.next", "a.paginate_button.next", "li.next a", "a.next",
    "[rel='next']", ".pagination .next a", "button.next",
]


def _harvest(page, opts=None):
    """Every link the crawler can currently see, keyed by href.
    Goes through extract_all() so we see exactly what the crawl will see."""
    try:
        _bc, _c, _s, links = extract_all(page, opts)
    except Exception:
        return {}
    return _merge_links({}, links)


def _unstamp(page):
    """Remove the data-crawl-id markers so they never reach the saved HTML."""
    try:
        page.evaluate("() => document.querySelectorAll('[data-crawl-id]')"
                      ".forEach(n => n.removeAttribute('data-crawl-id'))")
    except Exception:
        pass


def _click_and_keep(page, el, seen, opts=None):
    """Click ONE element; keep the result only if the page gained links.

    Always leaves the browser on the page we started from — the guard expand_tree
    lacks. Returns True if new links appeared.
    """
    before_url = page.url
    before_n = len(seen)
    try:
        el.scroll_into_view_if_needed(timeout=800)
        el.click(timeout=1200)
        page.wait_for_timeout(REVEAL_SETTLE_MS)
    except Exception:
        return False                       # not clickable / detached — no harm

    # A click that NAVIGATED is not a reveal. Undo it, or everything after this
    # runs against the wrong document.
    if page.url != before_url:
        try:
            page.go_back(wait_until="domcontentloaded", timeout=15000)
            page.wait_for_timeout(300)
        except Exception:
            pass
        return False

    _merge_links(seen, _harvest(page, opts).values())
    return len(seen) > before_n


def watch_asset_failures(page, host):
    """Record SCRIPT responses from `host` that failed, and return the live set.

    WHY A CRAWLER HAS TO CARE ABOUT SCRIPT 404s
    -------------------------------------------
    A page whose JavaScript did not arrive still renders. It renders its
    server-side first screen, looks completely healthy, and does none of the
    things its JavaScript would have done — so a crawl reads the first ten rows
    of a fifteen-page listing and reports success.

    MEASURED on www.lloc.gov.bh, three separate Playwright loads minutes apart,
    same User-Agent, no change on our side:

        load A   4 of 8 scripts 404 (jquery, wow, Chart1, main1)
                 -> "jQuery is not defined" x3, window.fetchResult undefined
        load B   2 of 2 scripts 404 after blocking the other six
                 -> same
        load C   6 of 8 scripts 200, jquery STILL 404
                 -> window.fetchResult IS a function, but it calls $.ajax

    and a plain `requests` GET of that same main1.js returned 200 / 35,113 chars.
    Nothing is broken on the site; the host refuses a share of the burst and picks
    a different share each time. Which is exactly why RELOADING WORKS — the next
    load is a fresh roll.

    Only SCRIPTS are watched, and only on the crawl's own host. A missing image or
    a third-party analytics 404 changes nothing about what we extract, and
    treating those as failures would reload half the web.

    `host` IS PASSED IN RATHER THAN READ FROM page.url. The first version compared
    against `page.url` and caught nothing: responses arrive while Playwright still
    reports the page as `about:blank`, so every comparison failed and the run
    reported `asset_failures: 0` on a page whose jQuery had not loaded. Measured
    directly — every response logged with page.url alongside it read
    `(404, 'document', 'about:blank', 'Latest')`.
    """
    want = (host or "").lower()
    failed = set()

    def _on_response(resp):
        try:
            if resp.status < 400:
                return
            if resp.request.resource_type != "script":
                return
            if urlparse(resp.url).netloc.lower() != want:
                return
            failed.add(resp.url)
        except Exception:
            pass                      # a listener must never break the crawl

    page.on("response", _on_response)
    return failed


def walk_numbered_pager(page, seen, opts=None):
    """Walk a pager that STATES ITS OWN LENGTH, merging each page's links.

    Returns (gained, pages_walked, note). `note` is non-empty when the walk ended
    for a reason worth reporting rather than because it finished — an unreachable
    page, a dead pager, or the cap.

    WHY THIS EXISTS AS A SEPARATE TIER: the Next-button tiers stop at the first
    barren step, which throws away a stated page count. Here the count is known,
    so a failure to advance is a FINDING and is reported, not the end of the list.
    """
    try:
        info = page.evaluate(JS_NUMBERED_PAGER)
    except Exception as e:
        return 0, 0, f"pager scan failed: {type(e).__name__}"
    if not info:
        return 0, 0, ""

    declared = int(info.get("pages") or 0)
    last = min(declared, PAGER_MAX_PAGES)
    gained = walked = barren = 0
    note = ""

    for n in range(2, last + 1):
        before_url = page.url
        before = len(seen)
        # RE-DRIVE, don't just re-read. MEASURED on lloc.gov.bh across four runs
        # of the same 15-page listing: 146, 122, 136 documents. The host refuses a
        # share of requests, so an individual pager step's XHR can fail while the
        # steps around it succeed — and the page then still shows the PREVIOUS
        # page's rows, which reads as "this page added nothing". Asking for the
        # same page number again is the only thing that recovers it.
        moved = False
        for step_try in range(1, PAGER_STEP_ATTEMPTS + 1):
            try:
                sig = page.evaluate(JS_LINK_SIG)
                moved = page.evaluate(JS_PAGER_GOTO, {**info, "n": n})
            except Exception as e:
                note = f"page {n} could not be driven: {type(e).__name__}"
                moved = False
                break
            if not moved:
                break
            try:
                page.wait_for_function(
                    "(old) => { const a = Array.from(document.querySelectorAll('a[href]'));"
                    " return a.length + '|' + a.map(n => n.getAttribute('href'))"
                    ".join(',').slice(0, 4000) !== old; }",
                    arg=sig, timeout=PAGER_STEP_TIMEOUT_MS)
            except Exception:
                pass
            page.wait_for_timeout(PAGER_SETTLE_MS)
            if page.url != before_url:
                break                      # navigated — handled below
            _merge_links(seen, _harvest(page, opts).values())
            if len(seen) > before:
                break                      # this page gave us something
            if step_try < PAGER_STEP_ATTEMPTS:
                page.wait_for_timeout(PAGER_SETTLE_MS * 3)
        if note:
            break
        if not moved:
            note = f"page {n} of {declared} was not reachable in the pager"
            break
        # A PAGER STEP THAT NAVIGATED IS NOT A REVEAL. If the control was a real
        # <a href> the pages have real urls, so the BFS reaches them by itself —
        # and continuing to drive a pager on a document we have moved away from
        # would corrupt everything after this. Harvest what this page offered,
        # go back, and stop. MEASURED: cma_regs (3 pages) and secp_acts (2) both
        # expose numbered link pagers, so this path is not hypothetical.
        if page.url != before_url:
            _before = len(seen)
            _merge_links(seen, _harvest(page, opts).values())
            gained += len(seen) - _before
            walked += 1
            try:
                page.go_back(wait_until="domcontentloaded", timeout=15000)
                page.wait_for_timeout(300)
            except Exception:
                pass
            note = (f"pager page {n} is a real link, not an in-page control — "
                    f"left to the BFS")
            break

        step = len(seen) - before
        gained += step
        walked += 1
        barren = 0 if step else barren + 1
        if barren >= PAGER_MAX_BARREN:
            # The pager is present and states N pages, but driving it adds
            # nothing. On lloc.gov.bh that means the site's own handler never
            # loaded — a silent 10-of-144 unless it is said out loud.
            note = (f"pager declares {declared} pages but {barren} consecutive "
                    f"page(s) added no links — the site's pager is not working")
            break

    if not note and declared > last:
        note = (f"pager declares {declared} pages, walked {last} "
                f"(PAGER_MAX_PAGES)")
    return gained, walked, note


def reveal_all_links(page, budget=REVEAL_CLICK_BUDGET, opts=None):
    """Click things that might reveal more links; return the union of all states.

    Three tiers, cheapest first:
      0. maximise a "Show N entries" page-size menu
      1. the known Next selectors  (fast path)
      2. candidates discovered by JS_CLICKABLES  (site-agnostic fallback)

    Supersedes collect_paginated_links() + expand_tree(), which remain defined
    for probe_scope() and manual debugging.
    """
    seen = _harvest(page, opts)
    baseline = len(seen)
    clicks = 0

    # ---- tier 0: maximise a DataTables-style "Show N entries" menu ----
    try:
        for sel in page.query_selector_all("select"):
            vals = sel.evaluate("s => Array.from(s.options).map(o => o.value)")
            nums = [v for v in vals if str(v).lstrip("-").isdigit()]
            if not nums:
                continue
            best = "-1" if "-1" in nums else max(nums, key=lambda x: int(x))
            if best == "-1" or int(best) >= 50:
                sel.select_option(best)
                page.wait_for_timeout(1200)
                break
    except Exception:
        pass
    _merge_links(seen, _harvest(page, opts).values())
    g0 = len(seen) - baseline
    _mark = len(seen)

    # ---- tier 0b: a pager that states its own length ----
    # Before the Next tiers, because it is cheaper and more complete: it knows how
    # many pages there are, so it neither stops at the first barren step nor
    # depends on a Next control existing.
    gp, pager_pages, pager_note = walk_numbered_pager(page, seen, opts)
    _mark = len(seen)

    # ---- tier 1: the known selectors ----
    barren = 0
    for sel in KNOWN_ADVANCE_SELECTORS:
        while clicks < budget and barren < REVEAL_MAX_BARREN:
            try:
                el = page.query_selector(sel)
            except Exception:
                el = None
            if el is None:
                break
            cls = (el.get_attribute("class") or "").lower()
            if "disabled" in cls or (el.get_attribute("aria-disabled") or "") == "true":
                break
            gained = _click_and_keep(page, el, seen, opts)
            clicks += 1
            barren = 0 if gained else barren + 1
            if not gained:
                break
    g1 = len(seen) - _mark
    c1 = clicks
    _mark = len(seen)

    # ---- tier 2: discovered candidates ----
    tried = set()
    barren = 0
    n_cands = 0
    js_error = ""
    while clicks < budget and barren < REVEAL_MAX_BARREN:
        try:
            cands = page.evaluate(JS_CLICKABLES)
        except Exception as e:
            # A broken JS_CLICKABLES looks EXACTLY like "this page has no
            # candidates". Report it rather than letting the tier vanish silently.
            js_error = f"{type(e).__name__}: {str(e)[:120]}"
            break
        n_cands = max(n_cands, len(cands))
        # Expanders first: they ADD to the page. Pagination REPLACES content, so
        # expanding before advancing captures both states rather than one.
        cands.sort(key=lambda c: {"expand": 0, "advance": 1}.get(c.get("kind"), 2))

        pick = None
        for c in cands:
            # Identity is label+class, NOT cid — cid is reassigned on every scan.
            key = (c.get("kind"), c.get("label"), c.get("cls"))
            if key not in tried:
                pick = c
                tried.add(key)
                break
        if pick is None:
            break

        try:
            el = page.query_selector(f"[data-crawl-id='{pick['cid']}']")
        except Exception:
            el = None
        if el is None:
            continue                       # vanished between scan and click
        gained = _click_and_keep(page, el, seen, opts)
        clicks += 1
        barren = 0 if gained else barren + 1

    _unstamp(page)
    ev = {"event": "reveal", "links": len(seen), "gained": len(seen) - baseline,
          "t0_select": g0, "t0b_pager": gp, "t1_known": g1,
          "t2_discovered": len(seen) - _mark,
          "clicks": clicks, "t1_clicks": c1, "t2_clicks": clicks - c1,
          "pager_pages": pager_pages, "cands": n_cands}
    if js_error:                 # tier 2 never ran — a code bug, not a site
        ev["js_error"] = js_error
    # A pager that was found and did not work is the single most dangerous thing
    # on this path: the page still renders its first screen and the crawl looks
    # healthy. Never let that be silent.
    if pager_note:
        ev["pager_note"] = pager_note
    emit(ev)
    return list(seen.values())


# ============================================================================
# AUTO-SCOPE — work the boundary out from the landing page
#
# Scope used to be a human decision typed in before the crawl. Here the crawler
# reads the seed page and derives it, so the answer is re-derived on every run
# rather than remembered from the last one.
#
# SCOPE answers "how far may I wander" (breadcrumb / prefix / host).
# SHAPE (strategies.py) answers "how do I read this layout" (tree / table / BFS).
# They are INDEPENDENT: SECP is table+prefix, SAMA is tree+breadcrumb, SDAIA is
# generic+breadcrumb. Both are decided from the same single page load below.
# ============================================================================

# Tuning knobs — CALIBRATED 2026-07-28 against 6 regulator sites.
# Measured with generic_crawler/calibrate_scope.py. Re-run it before changing these.
#
#   site           docs  under  cands  ratio  share  crumbs   correct scope
#   SECP acts        23      0    164     0%    12%       2   prefix   (rule 3)
#   SBP circulars     0     33    168    20%     0%       2   prefix   (rule 2, count)
#   SAMA sandbox      0      0     43     0%     0%       2   breadcrumb
#   SAMA CB law       1      0     43     0%     2%       3   breadcrumb
#   MISA laws        68      0     27     0%    72%       5   prefix   (rule 3)
#   SDAIA regs       36      0    117     0%    24%       9   breadcrumb (file guard)
#
# THESE FIVE VALUES ARE GLOBAL. Tuning one to fix a single site can silently flip
# another site's scope — re-run the harness on ALL sites before changing any.
PREFIX_MIN_RATIO   = 0.30  # UNVALIDATED: no site fires this rule. Kept as a net for
                           # "few links, mostly children" (high ratio, low count).
PREFIX_MIN_COUNT   = 10    # gap 0 -> 33 (SBP). Well clear on both sides.
LISTING_DOC_MIN    = 5     # gap 1 (SAMA) -> 23 (SECP). Floor against tiny pages.
LISTING_DOC_SHARE  = 0.07  # gap 2% (SAMA) -> 12% (SECP). NARROW — weakest knob.
                           # Relies on the seed_is_file guard to exclude SDAIA (24%).
BREADCRUMB_MIN_LEN = 2     # DO NOT RAISE: SAMA sandbox has exactly 2 crumbs.


def detect_scope(breadcrumb, links, seed_url, seed_host):
    """Choose 'breadcrumb' | 'prefix' | 'host' from evidence on the seed page.

    Pure function over the three things extract_all() already returns. The reason
    string is logged, so a wrong guess is diagnosable at a glance.
    """
    seed_path = urlparse(seed_url).path.rstrip("/")

    # Real folder depth. A leading /en/ or /ar/ is a language marker, not a level.
    segs = [s for s in seed_path.split("/") if s]
    if segs and re.fullmatch(r"[a-z]{2,3}", segs[0]):
        segs = segs[1:]

    # A seed ending .aspx/.html/.php is a PAGE, not a folder — nothing can sit
    # "under" it, so prefix scope would strand the crawl on that one page.
    seed_is_file = bool(re.search(r"\.(aspx?|html?|php|jsp)$", seed_path, re.I))

    candidates = under = docs = 0
    for l in links:
        href = l.get("href") or ""
        p = urlparse(href)
        if p.scheme not in ("http", "https") or p.netloc.lower() != seed_host:
            continue                                    # off-site / mailto:
        # seed_host matters here too: without it every same-host link on a
        # portal host counts as a document, and this function's docs-vs-pages
        # ratio is exactly what chooses the scope.
        if is_document_link(href, seed_host):
            docs += 1                                   # a file, not a page
            continue
        if ext_of(href) in SKIP_EXTS:
            continue                                    # images, css, fonts
        path = re.sub(r"/{2,}", "/", p.path).rstrip("/")
        if path == seed_path:
            continue                                    # link back to the seed
        candidates += 1
        if seed_path and path.startswith(seed_path + "/"):
            under += 1

    ratio = (under / candidates) if candidates else 0.0
    doc_share = (docs / (docs + candidates)) if (docs + candidates) else 0.0
    crumbs = [c for c in breadcrumb if c and c.strip()]
    ev = (f"{docs} docs, {under}/{candidates} links under '{seed_path or '/'}' "
          f"({ratio:.0%}), {len(crumbs)} breadcrumb steps")

    if not segs:
        return "host", f"seed is the site root, nothing narrower to stay inside — {ev}"
    if not seed_is_file and (ratio >= PREFIX_MIN_RATIO or under >= PREFIX_MIN_COUNT):
        return "prefix", f"links cluster under the seed path — {ev}"
    if not seed_is_file and docs >= LISTING_DOC_MIN and doc_share >= LISTING_DOC_SHARE:
        return "prefix", f"seed page is mostly document links (a listing page) — {ev}"
    if len(crumbs) >= BREADCRUMB_MIN_LEN:
        return "breadcrumb", f"flat URLs but a real breadcrumb trail — {ev}"
    return "host", f"no clear signal, falling back to same-domain — {ev}"


def probe_scope(page, seed_url, seed_host, opts=None):
    """Read an ALREADY-LOADED seed page and let detect_scope() judge it.

    Snapshot BEFORE expanding: expand_tree() clicks things, and on some sites a
    click navigates away or tears the DOM down, leaving nothing to read. crawl()
    guards against this by snapshotting first and merging both passes; the probe
    must do the same or a healthy page can measure as empty.
    """
    try:
        bc1, _c1, _s1, ln1 = extract_all(page, opts)
        try:
            expand_tree(page)          # a collapsed menu hides links worth counting
            bc2, _c2, _s2, ln2 = extract_all(page, opts)
        except Exception:
            bc2, ln2 = [], []
        breadcrumb = bc1 or bc2        # first non-empty trail wins
        links = list(_merge_links({}, ln1 + ln2).values())

        # A page with NO links is a failed measurement, not evidence. SBP
        # intermittently serves an empty page; reading that as "no clear signal"
        # returns 'host', which would send the crawl across the whole domain —
        # the most damaging possible answer to get from a blank page.
        if not links:
            fallback = "prefix" if urlparse(seed_url).path.strip("/") else "host"
            return fallback, (f"seed page yielded no links (site flaky or not "
                              f"rendered) — defaulting to {fallback}, not guessing")

        return detect_scope(breadcrumb, links, seed_url, seed_host)
    except Exception as e:
        # Prefer the safer default: a wrong 'prefix' collects too little (visible,
        # fixable); a wrong 'host' can wander an entire domain.
        fallback = "prefix" if urlparse(seed_url).path.strip("/") else "host"
        return fallback, f"probe failed ({str(e)[:120]}) — defaulting to {fallback}"


def expand_tree(page, max_rounds=40):
    """Best-effort: repeatedly click collapsed expanders so child links appear.
    Generic (aria-expanded + common toggle classes). No-ops harmlessly if the
    site doesn't use them — BFS still reaches everything by navigating in."""
    total = 0
    for _ in range(max_rounds):
        toggles = page.query_selector_all(
            "[aria-expanded='false'], .collapsed > .toggle, li.has-children > .expander, "
            ".tree-toggle, .accordion-toggle"
        )
        clicked = 0
        for t in toggles:
            try:
                if t.is_visible():
                    t.click(timeout=800)
                    clicked += 1
            except Exception:
                pass
        total += clicked
        if clicked == 0:
            break
        page.wait_for_timeout(250)
    return total


# ============================================================================
# SECTION D — crawl(): the main loop that ties everything together
#   queue of pages -> load -> render/scroll/expand -> extract -> scope check ->
#   record documents + page -> enqueue child links -> repeat until caps hit.
# ============================================================================


def crawl(seed_url, out_dir, max_pages=150, max_depth=8, scope="auto",
          headless=True, wait_ms=700, nav_timeout=60000, strategy="auto",
          group_headings=False, list_details=True, max_details=None,
          outer_prefix=None, section_name=None, max_widened=60,
          exclude_paths=None):
    """`outer_prefix` / `section_name` / `max_widened` drive --follow-section-links.

    They are None by default, which leaves prefix scope byte-identical to what it
    has always been. Only the --subpaths driver passes them, and only when the
    flag is set — so no existing caller, including the pipeline's
    GenericSiteCrawler, can reach the widened branch. See its comment at the
    enqueue gate below.
    """
    out = Path(out_dir)
    (out / "html").mkdir(parents=True, exist_ok=True)

    seed_norm = normalize_url(seed_url)
    seed_host = urlparse(seed_norm).netloc.lower()
    # Per-host fixes. Hosts with no profile behave exactly as they did before.
    # (scope_prefix is defined at module level — see its docstring for why the
    #  seed's own filename must not end up in the prefix.)
    prof = profile_for(seed_norm)
    group_headings = group_headings or prof["group_headings"]
    seed_prefix = scope_prefix(urlparse(seed_norm).path)  # "under the seed path" for prefix scope
    # If the seed sits under a 2-3 letter language segment (/en/...), lock the crawl
    # to that language so we don't load every page's /ar/ mirror just to reject it.
    _seg0 = first_seg(urlparse(seed_norm).path)
    lang_lock = _seg0 if re.fullmatch(r"[a-z]{2,3}", _seg0 or "") else None

    visited = set()
    # Urls queued by the WIDENED branch: they sit outside the section's own path
    # and were followed on suspicion, so the site's breadcrumb — not the url —
    # decides whether they belong. Empty unless --follow-section-links is on.
    widened_urls = set()
    section_needle = re.sub(r"[-_]+", " ", (section_name or "").strip()).strip().lower()
    content_hashes = {}     # content_key -> url that first recorded it
    records = []
    # keep_modals hosts only: one record per modal panel, collected separately so
    # they do NOT consume --max-pages. 202 panels behind 10 pages would otherwise
    # stop the walk at the cap with most of the site unvisited.
    modal_records = []
    documents = {}          # normalized doc url -> record
    chrome_documents = {}   # same, for links found only in the site header/footer
    link_titles = {}        # normalized url -> the anchor text that linked to it
    link_parents = {}       # normalized url -> the page it was linked from
    section_anchor = None   # set from the seed's breadcrumb (last item)

    # What the walk learned about itself. These were all emitted as events and
    # then forgotten — baseline.py re-parsed stdout to get them back — so nothing
    # downstream of the process could tell a clean run from a truncated one.
    note = {"blocked_pages": 0, "errors": 0, "retries": 0, "asset_failures": 0,
            "cap_hit": False, "seed_loaded": True, "stopped": "", "resume": {}}

    emit({"event": "start", "seed": seed_norm, "scope": scope,
          "max_pages": max_pages, "max_depth": max_depth})

    with sync_playwright() as pw:
        # --disable-dev-shm-usage: Chromium's default /dev/shm is small, and a
        # long crawl (SBP: 4,160 detail pages) exhausts it and takes the whole
        # browser down mid-run. Observed: dead after ~44 detail pages.
        browser = pw.chromium.launch(
            headless=headless,
            args=["--disable-dev-shm-usage", "--disable-gpu",
                  "--renderer-process-limit=2"])
        ctx = browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
        )
        page = ctx.new_page()
        # Live set of same-host scripts that failed to load. Read by the page
        # loop below, which reloads rather than crawling a half-built page.
        asset_failures = watch_asset_failures(page, seed_host)

        # ---- shape-aware dispatch (additive) ---------------------------------
        # Load the seed once, detect its layout, and hand off to a specialised
        # strategy. Falls through to the existing BFS when shape == "generic".
        # ONE seed load answers BOTH questions: how far may I go (scope) and how
        # do I read this layout (shape). They are independent, and loading the
        # page twice to ask them separately would only risk them disagreeing.
        seed_loaded = False
        if strategy != "generic" or scope == "auto":
            # RETRY. Every other page in this crawler gets two attempts; the seed
            # used to get one — and the seed decides BOTH the scope and the shape
            # for the entire run. A single transient DNS blip (these sites give
            # them regularly) silently downgraded the whole crawl to
            # generic + default scope. An empty page counts as a failure too.
            for attempt in range(1, 4):
                try:
                    page.goto(seed_norm, wait_until="domcontentloaded", timeout=nav_timeout)
                    page.wait_for_timeout(wait_ms + 2500)
                    try: page.mouse.wheel(0, 2500); page.wait_for_timeout(600)
                    except Exception: pass
                    if page.evaluate("()=>document.querySelectorAll('a[href]').length") > 15:
                        seed_loaded = True
                        break
                    note["retries"] += 1
                    emit({"event": "retry", "url": seed_norm, "attempt": attempt,
                          "message": "seed rendered with no links"})
                except Exception as e:
                    note["retries"] += 1
                    emit({"event": "retry", "url": seed_norm, "attempt": attempt,
                          "message": str(e)[:160]})
                page.wait_for_timeout(2000)
            # A challenge page can satisfy the link check above, and a blocked
            # SEED means every decision below it — scope, shape, the whole walk —
            # was made against the WAF's page rather than the site's.
            reason = blocked_reason(page)
            if reason:
                note["blocked_pages"] += 1
                note["stopped"] = f"seed blocked by bot protection ({reason})"
                emit({"event": "blocked", "url": seed_norm, "reason": reason})
            if not seed_loaded:
                note["seed_loaded"] = False
                note["errors"] += 1
                # Not just a failed page: the seed decides scope AND shape for the
                # whole run, so everything below it was decided on defaults
                # (MERGE_LOG §13, robustness fix 1).
                note["stopped"] = note["stopped"] or (
                    "seed did not load after 3 attempts — scope and shape fell "
                    "back to defaults, so this walk may be of the wrong pages")
                emit({"event": "error", "url": seed_norm,
                      "message": "seed did not load after 3 attempts — scope and "
                                 "shape fall back to defaults"})

        # SHAPE BEFORE SCOPE — order matters and is not arbitrary.
        # probe_scope() calls expand_tree(), which CLICKS things, and on some
        # sites (SBP) a click tears the listing out of the DOM. Detecting shape
        # afterwards then sees a wrecked page: SBP's 26 `h4.mb-2` rows vanish and
        # it is misread as 'generic'. detect_shape() only reads (its child probe
        # opens a separate page), so it is safe to run first.
        shape = "generic"
        if strategy != "generic":
            shape = (strategy if strategy in ("tree", "table", "list")
                     else detect_shape(page, ctx))
            emit({"event": "shape", "requested": strategy, "detected": shape})

        if scope == "auto":
            if seed_loaded:
                scope, why = probe_scope(page, seed_norm, seed_host, prof)
            else:
                scope = "prefix" if seed_prefix.strip("/") else "host"
                why = f"seed did not load — defaulting to {scope}"
            emit({"event": "scope", "scope": scope, "reason": why})

        if strategy != "generic":
            if shape in ("tree", "table", "list"):
                if shape == "tree":
                    recs, docs, walked = crawl_tree(ctx, seed_norm, out, max_pages=max_pages,
                                                    max_depth=max_depth,
                                                    wait_ms=max(wait_ms, 2000))
                elif shape == "list":
                    # For this shape max_pages bounds LISTING pages, not
                    # documents: SBP's 139 listing pages hold 4,160 entries, so
                    # the default 150 covers it. list_details=False stops before
                    # the expensive detail pass (see crawl_list).
                    recs, docs, walked = crawl_list(ctx, seed_norm, out,
                                                    max_pages=max_pages,
                                                    wait_ms=wait_ms,
                                                    fetch_details=list_details,
                                                    max_details=max_details)
                else:
                    recs, docs, walked = crawl_table(ctx, seed_norm, out,
                                                     max_pages=max_pages * 50,
                                                     wait_ms=wait_ms)
                # The walker reports what it hit; the seed checks above already
                # put their own findings in `note`. Add, never overwrite.
                _merge_note(note, walked)
                # Write FIRST, close second. A dead browser can hang close(),
                # and losing a completed walk to a failed teardown is the worst
                # possible trade.
                result = _finish(out, seed_norm, recs, docs, [], shape=shape, note=note)
                _safe_close(browser)
                return result
            # shape == "generic": continue with the existing link-walk below.

        queue = deque([(seed_norm, 0)])      # content / detail pages (high priority)
        page_queue = deque()                 # pagination / index pages (low priority)
        visited.add(seed_norm)

        while (queue or page_queue) and len(records) < max_pages:
            # Always drain real content pages before following pagination, so a page
            # cap is spent on actual documents, not on index pages full of links.
            url, depth = queue.popleft() if queue else page_queue.popleft()
            nav_ok = False
            last_err = ""
            # ATTEMPTS COUNT TWO DIFFERENT FAILURES. A goto that raises is the
            # obvious one. The other is a load that SUCCEEDS while the page's own
            # scripts 404 — see watch_asset_failures: the page renders its
            # server-side first screen, looks healthy, and its pager does
            # nothing. Reloading is the fix because the host refuses a different
            # share of the burst each time.
            http_status = None
            for attempt in range(1, ASSET_RELOAD_ATTEMPTS + 1):
                asset_failures.clear()
                try:
                    # Load the DOM fast, then give late XHR a SHORT settle window.
                    # (Using wait_until="networkidle" in goto hangs the full timeout on
                    #  chatty sites like SBP whose analytics beacons never go idle.)
                    resp = page.goto(url, wait_until="domcontentloaded",
                                     timeout=nav_timeout)
                    http_status = resp.status if resp is not None else None
                    try:
                        page.wait_for_load_state("networkidle", timeout=2500)
                    except Exception:
                        pass
                    nav_ok = True
                except Exception as e:
                    last_err = str(e)[:200]
                    note["retries"] += 1
                    emit({"event": "retry", "url": url, "attempt": attempt,
                          "message": last_err})
                    page.wait_for_timeout(1500)
                    continue
                # A 4xx/5xx DOCUMENT is retried for the same reason a refused
                # script is: on a throttling host the next load is a fresh roll.
                # Measured on lloc.gov.bh — /Legislation/Latest itself came back
                # `(404, 'document')` with no scripts requested at all, and the
                # same url served the full 48 KB page a minute later.
                if (http_status or 0) >= 400 and attempt < ASSET_RELOAD_ATTEMPTS:
                    note["retries"] += 1
                    emit({"event": "retry", "url": url, "attempt": attempt,
                          "message": f"HTTP {http_status} on the page itself; "
                                     f"reloading"})
                    page.wait_for_timeout(ASSET_RELOAD_WAIT_MS)
                    continue
                if not asset_failures or attempt == ASSET_RELOAD_ATTEMPTS:
                    if asset_failures:
                        # Kept, but SAID. A crawl standing on a page whose scripts
                        # never arrived must not look like a clean one.
                        note["asset_failures"] = (note.get("asset_failures", 0)
                                                  + len(asset_failures))
                        emit({"event": "assets_missing", "url": url,
                              "attempt": attempt,
                              "scripts": sorted(asset_failures)[:6],
                              "count": len(asset_failures)})
                    break
                note["retries"] += 1
                emit({"event": "retry", "url": url, "attempt": attempt,
                      "message": f"{len(asset_failures)} same-host script(s) "
                                 f"failed to load; reloading",
                      "scripts": sorted(asset_failures)[:6]})
                page.wait_for_timeout(ASSET_RELOAD_WAIT_MS)
            if not nav_ok:
                note["errors"] += 1
                emit({"event": "error", "url": url, "depth": depth, "message": last_err})
                continue
            # AN ERROR PAGE IS NOT A PAGE. Before this, a throttled 404 was
            # recorded as an ordinary thin page: `status: ok`, one page, twelve
            # documents. The text test is the safety valve — a few CMSs serve real
            # content under a 404 status, and those are kept and flagged rather
            # than dropped, because dropping them would silently shrink a crawl
            # that used to work.
            if (http_status or 0) >= 400:
                note["errors"] += 1
                try:
                    body_len = len((page.inner_text("body") or "").strip())
                except Exception:
                    body_len = 0
                emit({"event": "http_error", "url": url, "depth": depth,
                      "status": http_status, "text_len": body_len,
                      "recorded": body_len >= HTTP_ERROR_MIN_TEXT})
                if body_len < HTTP_ERROR_MIN_TEXT:
                    continue
            # Checked on every page, not just the seed: a WAF usually lets the
            # first few through and starts serving challenges once it has decided
            # we are a bot — which is exactly how SIMAH was tripped.
            reason = blocked_reason(page)
            if reason:
                note["blocked_pages"] += 1
                if note["blocked_pages"] <= 3:
                    emit({"event": "blocked", "url": url, "reason": reason})
                continue          # never record a challenge page as a page
            try:
                # Many JS sites (e.g. SBP) render their list only on scroll (lazy load).
                # Scroll down a few times to trigger it, then return to top.
                try:
                    for _ in range(3):
                        page.mouse.wheel(0, 6000)
                        page.wait_for_timeout(300)
                    page.evaluate("window.scrollTo(0, 0)")
                except Exception:
                    pass
                # Wait for real content/links to appear (SPA routes inject body via JS).
                try:
                    page.wait_for_function(
                        "() => document.querySelectorAll('a[href]').length > 15 "
                        "|| (document.body && document.body.innerText.trim().length > 500)",
                        timeout=10000)
                except Exception:
                    pass
            except Exception as e:
                note["errors"] += 1
                emit({"event": "error", "url": url, "depth": depth, "message": str(e)[:200]})
                continue

            # Snapshot NOW, before redirect-prone delays. Some SPAs (SBP) render the
            # content then client-side-redirect away seconds later; capturing here keeps it.
            # Prefer the real document heading over the generic <title>/banner text.
            try:
                title = (page.evaluate(JS_DOC_TITLE) or "").strip()
            except Exception:
                title = ""
            if not title:
                title = (page.title() or "").strip()
            try:
                bc1, c1, st1, ln1 = extract_all(page, prof)
            except Exception:
                bc1, c1, st1, ln1 = [], {"html": "", "text": ""}, "", []
            # Then let the tree expand and snapshot again (tree sites reveal more links).
            try:
                page.wait_for_timeout(wait_ms)
                expand_tree(page)
                bc2, c2, st2, ln2 = extract_all(page, prof)
            except Exception:
                bc2, c2, st2, ln2 = [], {"html": "", "text": ""}, "", []
            # Merge: richer content wins; links are the union; first non-empty crumb/status.
            content = c1 if len(c1.get("text", "")) >= len(c2.get("text", "")) else c2
            breadcrumb = bc1 or bc2
            status = st1 or st2
            # Reveal links hidden behind in-page controls: "Show N entries"
            # menus, Next buttons, accordions, "Load more". Click-and-verify —
            # a click is kept only if the page actually gained links, and undone
            # if it navigated. Supersedes collect_paginated_links().
            try:
                ln3 = reveal_all_links(page, opts=prof)
            except Exception:
                ln3 = []
            # Merge the three passes (pre-expand, post-expand, paginated) with the
            # same rule extract_all uses: never let a later, poorer sighting of a
            # URL overwrite a richer earlier one.
            links = list(_merge_links({}, ln1 + ln2 + ln3).values())
            if not title:
                title = (page.title() or "").strip() or (breadcrumb[-1] if breadcrumb else "")

            # Structured section trail from tab panels / accordion containers.
            # Fires only where the markup provides it (measured: 89 links on MISA,
            # 0 on SECP/SBP/SAMA/SDAIA), so it is always on — it cannot affect a
            # site whose markup it does not match.
            nav_path_map = {}
            try:
                for item in (page.evaluate(JS_NAV_PATH) or []):
                    if item.get("nav_path"):
                        key = (normalize_url(item.get("href") or ""),
                               (item.get("text") or "").strip())
                        nav_path_map.setdefault(key, item["nav_path"])
            except Exception:
                pass

            if section_anchor is None:
                # anchor = last meaningful breadcrumb item, else the page title
                section_anchor = (breadcrumb[-1] if breadcrumb else title).strip().lower()
                emit({"event": "anchor", "section_anchor": section_anchor})

            # --- scope decision ---
            in_scope = True
            crumb_l = [re.sub(r"\s+", " ", c).strip().lower() for c in breadcrumb]
            if scope == "breadcrumb":
                # EXACT match: the section anchor must be one of the breadcrumb steps.
                # (Substring matching wrongly pulled in parent sections, e.g. "Capital
                #  Market" leaking into "Capital Market Law".)
                anchor = re.sub(r"\s+", " ", section_anchor or "").strip()
                in_scope = bool(anchor) and anchor in crumb_l
                # the seed itself always counts
                if url == seed_norm:
                    in_scope = True
            elif scope == "prefix":
                in_scope = urlparse(url).path.startswith(seed_prefix)
            elif scope == "host":
                in_scope = True  # same-host already enforced at enqueue time

            # --- a WIDENED page is judged by the site's own breadcrumb, FIRST ---
            #
            # This runs BEFORE the document collection below, unlike the ordinary
            # out-of-scope check. An ordinary out-of-scope page was linked from
            # inside the section and its files are still the section's; a widened
            # page was fetched on nothing but suspicion, so if the site says it is
            # not in this section then nothing on it belongs here — not its text,
            # not its files.
            #
            # Measured on sio.gov.bh, which is why the breadcrumb is trusted:
            #   /en/law-no-24-of-1976   ['Home', 'Private Sectors']   content
            #   /en/amendment-decrees   ['Home', 'Private Sectors']   content
            #   /en/about-sio           ['Home']                      nav
            #   /en/faqs                ['Home']                      nav
            #   /en/privacy-policy      ['Privacy Policy']             nav
            # The site knows the page sits under Private Sectors even though its
            # url does not say so. That is the whole basis of this rule.
            if url in widened_urls:
                if not section_needle or section_needle not in crumb_l:
                    emit({"event": "skip", "url": url, "depth": depth,
                          "reason": "widened-not-in-section",
                          "section": section_needle, "breadcrumb": breadcrumb})
                    continue
                # The breadcrumb vouched for it, so it IS in the section — which
                # the prefix rule above could never say, since being outside the
                # prefix is what made it widened in the first place.
                in_scope = True
                emit({"event": "widened", "url": url, "depth": depth,
                      "section": section_needle, "breadcrumb": breadcrumb})

            # --- collect document links on this page regardless of scope ---
            # Includes plain .pdf/.docx AND download-manager links (SECP "Download"
            # buttons → /document/<slug>/?wpdmdl=<id>) that carry the title as link text.
            page_docs = []
            for l in links:
                href = l["href"]
                if urlparse(href).scheme in ("http", "https") and is_document_link(href, seed_host):
                    dn = normalize_url(href)
                    if path_excluded(urlparse(dn).path, exclude_paths):
                        continue          # its files belong to that source too
                    rec_doc = {
                        "title": best_doc_title(l, dn),   # real title/date, not "Download"
                        "doc_url": dn,
                        "type": doc_type_of(href, seed_host),
                        "found_on": url,
                        "section_path": doc_section_path(
                            # `link_title_is_section`: the rail link that reached
                            # this page names what the page is a filtered view OF,
                            # and nothing on the page itself says it. Appended to
                            # the BREADCRUMB, not prepended to the finished trail:
                            # the category refines the page's own position, so it
                            # belongs after "Forms" and before the page's own
                            # headings. See the profile key's docs.
                            _with_link_title(breadcrumb, link_titles.get(url, ""),
                                             prof),
                            l.get("group") if group_headings else "",
                            nav_path_map.get(
                                (dn, (l.get("text") or "").strip()), ""),
                            l.get("heading_path") if group_headings else None),
                    }
                    # Site furniture (header/footer) is not this section's content:
                    # a "Privacy Policy" PDF or a site-wide guidebook banner would
                    # otherwise be filed under whatever page happened to show it.
                    # Dropped, but KEPT IN AN AUDIT LIST so nothing vanishes silently
                    # and the rule can be checked against a real crawl.
                    if l.get("chrome"):
                        chrome_documents.setdefault(dn, rec_doc)
                        continue
                    # Host-level furniture, by url. Same audit list as chrome, so
                    # `exclude_document_urls` can be checked against a real crawl
                    # rather than trusted.
                    if _excluded_doc_url(dn, prof):
                        rec_doc["reason"] = "profile exclude_document_urls"
                        chrome_documents.setdefault(dn, rec_doc)
                        continue
                    page_docs.append(dn)
                    # Keyed by (url, section_path), not url alone: regulators
                    # deliberately cross-list one document under several sections,
                    # and each listing is a separate place in the library. The DB
                    # agrees — document_exists_by_url(url, category) is
                    # category-scoped precisely for SAMA's cross-listed documents.
                    # NO CATEGORY COULD BE DISCOVERED FOR THIS ONE. Under
                    # `link_title_is_section` a page's category comes from the
                    # anchor that led to it; the seed has no such anchor, so a
                    # document that survives only from the seed is one the site
                    # filed under no category at all. Marked here, routed by the
                    # wrapper's `uncategorised_source_system`.
                    if prof.get("link_title_is_section") and not clean_doc_title(
                            link_titles.get(url, "")):
                        rec_doc["uncategorised"] = True
                    key = (dn, rec_doc["section_path"])
                    if key not in documents:
                        documents[key] = rec_doc

            # NOTHING PUBLISHED ON THIS PAGE, AND THAT IS WORTH A ROW. See
            # `empty_page_placeholder` in DEFAULT_PROFILE for why this is per
            # page rather than per source.
            ph_title = (prof.get("empty_page_placeholder") or "").strip()
            if ph_title and not page_docs and in_scope:
                ph_sec = doc_section_path(
                    _with_link_title(breadcrumb, link_titles.get(url, ""), prof),
                    "", "", None)
                ph_key = (url, ph_sec)
                if ph_key not in documents:
                    documents[ph_key] = {
                        "title": ph_title,
                        # The page itself. There is no file to point at, and the
                        # page is what the reader would open to check.
                        "doc_url": url,
                        "type": "PAGE",
                        "found_on": url,
                        "section_path": ph_sec,
                        "placeholder": True,
                    }
                    emit({"event": "empty_page", "url": url,
                          "section_path": ph_sec,
                          "why": "in scope, read, and carrying no document"})

            if not in_scope:
                emit({"event": "skip", "url": url, "depth": depth,
                      "reason": "out-of-scope", "breadcrumb": breadcrumb})
                continue

            # --- skip aggregator / print pages (duplicate content + cause circling) ---
            if is_aggregator(url, title):
                emit({"event": "skip", "url": url, "depth": depth, "reason": "aggregator"})
                continue

            # --- skip same-content duplicates (Drupal -0/-1 twins, aliases) ---
            ckey = content_key(content["text"])
            if ckey and ckey in content_hashes:
                emit({"event": "skip", "url": url, "depth": depth,
                      "reason": "duplicate-content", "same_as": content_hashes[ckey]})
                continue
            if ckey:
                content_hashes[ckey] = url

            # --- record this page ---
            slug = slugify(urlparse(url).path or title) or f"page-{len(records)}"
            html_file = f"html/{slug}.html"
            write_page_html(out, html_file, content["html"], url, title)

            rec = {
                "section_path": " > ".join(breadcrumb),
                "title": title,
                "url": url,
                "depth": depth,
                # How we got here. The page's own <title> is often generic
                # ("Details", "Circulars"); the anchor that led to it usually
                # carries the real document name.
                "linked_from_title": link_titles.get(url, ""),
                "parent_page_url": link_parents.get(url, ""),
                "status": status,
                "n_pdfs": len(page_docs),
                "pdf_links": " | ".join(page_docs),
                "text_len": len(content["text"]),
                "html_file": html_file,
                "text": content["text"],
                "html": content["html"],
                "breadcrumb": breadcrumb,
            }
            records.append(rec)

            # --- one record per modal panel (keep_modals hosts) --------------
            # The page above is now just the index; the law texts are these.
            if prof.get("keep_modals"):
                try:
                    panels = page.evaluate(JS_MODALS, prof) or []
                except Exception as e:
                    panels = []
                    note["errors"] += 1
                    emit({"event": "error", "url": url,
                          "message": f"modal extraction failed: {str(e)[:120]}"})
                for m in panels:
                    mid = (m.get("id") or "").strip()
                    mtitle = clean_doc_title(m.get("title"))
                    if not mid or not mtitle:
                        continue
                    murl = f"{url}#{mid}"
                    if murl in visited:
                        continue
                    visited.add(murl)
                    # The panel's own Part/Section trail, when the host says its
                    # headings are a real grouping. Without `group_headings` this
                    # is exactly the old behaviour — every panel inherits the
                    # page's path — which is what sio.gov.bh wants: its modals
                    # are a flat list of laws under one sector.
                    msec = rec["section_path"]
                    if group_headings:
                        trail = [t for t in (m.get("heading_path") or []) if t]
                        if trail:
                            msec = doc_section_path(breadcrumb, "", "",
                                                    trail)
                    modal_records.append({
                        "section_path": msec,
                        "title": mtitle,
                        "url": murl,
                        "depth": depth + 1,
                        "linked_from_title": title,
                        "parent_page_url": url,
                        "status": status,
                        "n_pdfs": 0,
                        "pdf_links": "",
                        "text_len": len(m.get("text") or ""),
                        "html_file": "",
                        "text": m.get("text") or "",
                        "html": m.get("html") or "",
                        "breadcrumb": breadcrumb,
                    })
                if panels:
                    emit({"event": "modals", "url": url, "found": len(panels),
                          "kept": len(modal_records),
                          "placeholders": sum(1 for m in panels if m.get("placeholder"))})
            emit({"event": "visit", "url": url, "depth": depth, "title": title,
                  "section_path": rec["section_path"], "n_pdfs": len(page_docs),
                  "text_len": rec["text_len"], "recorded": len(records),
                  "queued": len(queue), "page_queued": len(page_queue)})

            # --- enqueue children ---
            for l in links:
                href = l["href"]
                nh = normalize_url(href)
                if nh in visited:
                    continue
                pu = urlparse(nh)
                if pu.scheme not in ("http", "https"):
                    continue
                if pu.netloc.lower() != seed_host:      # same-host rule
                    continue
                e = ext_of(nh)
                if e in SKIP_EXTS or is_document_link(nh, seed_host):  # assets ignored; docs recorded above
                    continue
                if is_aggregator(nh, l.get("text", "")):  # never crawl entire-section/print pages
                    continue
                if lang_lock and first_seg(pu.path) != lang_lock:  # skip other-language mirrors
                    continue
                if DENY_PATH_PAT.search(pu.path) or pu.path in ("/", f"/{lang_lock}"):  # chrome pages
                    continue
                if path_excluded(pu.path, exclude_paths):
                    continue          # a subtree another source owns
                if scope == "prefix" and not pu.path.startswith(seed_prefix):
                    # WIDENED BRANCH (--follow-section-links). A section's own
                    # content does not always live under the section's path.
                    # sio.gov.bh publishes flat: /en/private-sectors links its
                    # four instruments at /en/law-no-24-of-1976,
                    # /en/amendment-decrees and so on — siblings, not children.
                    # Prefix scope rejected all four here, silently (this used to
                    # be a bare `continue` with no event), so the crawl recorded
                    # the listing page and reported `ok`: 8,171 characters where
                    # the section actually holds 96,941.
                    #
                    # THREE CONDITIONS, and all three matter:
                    #   outer_prefix     the wider boundary is the --seed, not the
                    #                    section, so this can only ever wander
                    #                    inside the site the user already named
                    #   url == seed_norm found ON the section page. One hop only:
                    #                    a widened page's own off-prefix links are
                    #                    NOT followed, or this decays into `host`
                    #   not chrome       the header/footer flag JS_LINKS already
                    #                    computes. A pre-filter, not the decision:
                    #                    on SIO it takes 39 candidates down to 7,
                    #                    and every candidate costs a page load
                    #                    because a rejected page is not recorded
                    #                    and therefore does not count toward
                    #                    max_pages. `max_widened` is the backstop.
                    #
                    # The DECISION is the breadcrumb test above, not any of these.
                    # These only choose what is worth asking about.
                    if not (outer_prefix
                            and url == seed_norm
                            and not l.get("chrome")
                            and pu.path.startswith(outer_prefix)
                            and len(widened_urls) < max_widened):
                        continue
                    widened_urls.add(nh)
                # Remember HOW we reached this page: the anchor text that linked to
                # it, and the page it was linked from. A detail page's own <title>
                # is often generic ("Details"), while the link that led to it
                # carries the real document name — so this is frequently the best
                # title we will ever have for it.
                link_titles.setdefault(nh, (l.get("text") or "").strip()[:300])
                link_parents.setdefault(nh, url)
                if l.get("nav"):
                    # Pagination / "Next" links: LOW priority (real rows crawled first)
                    # and they do NOT consume the depth budget, so a long pagination
                    # chain (e.g. SBP: 133 pages via repeated "Next") is never cut off
                    # by max_depth. Only max_pages bounds it.
                    visited.add(nh)
                    page_queue.append((nh, depth))
                elif depth < max_depth:
                    # Real content / detail page (a row): counts toward depth.
                    visited.add(nh)
                    queue.append((nh, depth + 1))

        # The cap stops the walk with work still queued. That is the difference
        # between "this is the site" and "this is 150 pages of the site", and
        # what is left in the queues is where a resumed run would start.
        left = len(queue) + len(page_queue)
        if len(records) >= max_pages and left:
            note["cap_hit"] = True
            note["stopped"] = (f"page cap: {len(records)} of max_pages={max_pages}, "
                               f"{left} URLs still queued")
            note["resume"] = {"pages_walked": len(records), "queued": left,
                              "next_urls": [u for u, _ in list(queue)[:5]]
                                           or [u for u, _ in list(page_queue)[:5]]}
            emit({"event": "cap", "pages": len(records), "queued": left})

        _safe_close(browser)

    # ---- write outputs ----
    # A document seen in the header/footer on one page but in real content on
    # another IS content — the content sighting wins. `documents` is keyed by
    # (url, section_path), so compare on the url part only.
    kept_urls = {k[0] for k in documents}
    dropped_chrome = [d for u, d in chrome_documents.items() if u not in kept_urls]

    # Panels come after the pages so the index reads before its children.
    # `prefer_deepest_section`: one file, one placement — the most specific one.
    # See the profile key's docs for why this is opt-in and not the default.
    # `documents` is keyed by (url, section_path) — the key already carries both
    # halves, so the deepest placement is decided without touching the values.
    if prof.get("prefer_deepest_section") and documents:
        depth_of = lambda sp: len([x for x in (sp or "").split(" > ") if x.strip()])
        best = {}                       # url -> the key with the deepest trail
        for key in documents:
            u, sp = key[0], key[1]
            if u not in best or depth_of(sp) > depth_of(best[u][1]):
                best[u] = key
        keep = set(best.values())
        dropped = len(documents) - len(keep)
        if dropped:
            emit({"event": "deepest_section", "kept": len(keep),
                  "dropped": dropped,
                  "why": "same file also found under a shallower trail"})
            # Rebuilt by comprehension over items(), so discovery order survives.
            documents = {k: v for k, v in documents.items() if k in keep}

    return _finish(out, seed_norm, records + modal_records,
                   list(documents.values()),
                   dropped_chrome, shape="generic", note=note)


def _safe_close(browser):
    """Close the browser without letting it take the run down.

    A crashed Chromium does not just raise on close() — it can BLOCK, waiting for
    a process that is never going to answer. That hung the whole crawler after a
    successful walk, so results were computed and then never written. Give it a
    few seconds on a side thread and move on.
    """
    import threading
    done = threading.Event()

    def _close():
        try:
            browser.close()
        except Exception:
            pass
        finally:
            done.set()

    threading.Thread(target=_close, daemon=True).start()
    done.wait(10)


# THE OUTCOME, IN ONE WORD. `done` used to mean only "the walk reached _finish",
# and every consumer read that as success — main() never even set an exit code. A
# WAF page reaches _finish too, and its 1,054 characters clear the 200-char bar
# that makes a page a document.
#
#   ok          pages recorded, nothing blocked, nothing cut short
#   blocked     a page was a bot-protection wall; no count means anything
#   zero        no pages recorded — a failed extraction, not an empty site
#   incomplete  cap hit / seed never loaded / browser died / pages failed.
#               REPORTED, NOT FATAL (MERGE_LOG §13: keep the rows, log
#               INCOMPLETE). `stopped` and `resume` are what
#               resume-from-where-it-died will read; today that is thrown away.
#
# NO-DOCS stays in baseline_report.py: only it has the cross-site context to tell
# SAMA's real 3 files from a broken extraction, and it already flags it.
FATAL_STATUSES = ("blocked", "zero")


def _merge_note(note: dict, walked: dict) -> dict:
    """Fold a walker's findings into the run's note. Counters add; the first
    `stopped` wins, because the earliest thing that cut the walk short is the one
    a reader needs to act on."""
    for k in ("blocked_pages", "errors", "retries"):
        note[k] = note.get(k, 0) + (walked or {}).get(k, 0)
    note["stopped"] = note.get("stopped") or (walked or {}).get("stopped", "")
    note["resume"] = note.get("resume") or (walked or {}).get("resume") or {}
    return note


def run_status(counts: dict) -> str:
    """Classify a finished run. Pure function of the counters, so it is testable
    without a browser and cannot drift from what `done` reports.

    `errors` is counted but does not decide the status: a page that 404s is skipped
    by design, and if 1 bad page in 150 said INCOMPLETE the word would stop meaning
    anything. Only the walk being CUT SHORT flips it — cap, dead browser, or a seed
    that never arrived (which decides scope and shape for everything after it).

    ONE BAD PAGE IN 150 IS NOISE. ALL 150 IS A FAILED RUN, and that case had no
    rule until 2026-08-27. The shape-detection trap produces it exactly:
    `crawl_list` treats each row's PDF as a page to navigate into, so every
    "page" is an error and no document is ever recorded — and the run reported
    `ok`. Measured four times before this was added:

        cbe.org.eg  /en/laws-regulations/regulations/circulars
                    list, 10 pages, 10 errors, 0 documents, `ok`
        sio.gov.bh  six sections, each  list, 0 pages, `zero`
        moic.gov.bh /en/regulations
                    list, 78 pages, 78 errors, 0 documents, `ok`
        moic.gov.bh /en/regulations?about[0]=19
                    list, 74 pages, 74 errors, 0 documents, `ok`

    Two of those cost a full export apiece before anyone noticed. The rule is
    deliberately narrow — every page errored AND nothing was collected — so a
    run that got documents, or that had one bad page among good ones, is
    untouched. `zero` rather than a new word: the vocabulary is already handled
    everywhere (`_looks_ok` accepts only ok/incomplete) and a walk whose every
    page failed did find nothing, whatever the page counter says.
    """
    if counts.get("blocked_pages"):
        return "blocked"
    if not counts.get("pages"):
        return "zero"
    if (counts.get("errors", 0) >= counts.get("pages", 0)
            and not counts.get("documents")):
        return "zero"
    if (counts.get("cap_hit") or counts.get("stopped")
            or not counts.get("seed_loaded", True)):
        return "incomplete"
    return "ok"


def _finish(out, seed_norm, records, documents, chrome_dropped, shape, note=None):
    """Shared tail for every walker: normalise, hash, write, return.

    Every path through crawl() ends here, so tree/table/generic produce the same
    schema, files, return value — and now the same outcome vocabulary. `note`
    carries what the walk learned; absent, the run is judged on its counts alone.
    Returns (records, documents).
    """
    # Rewrite only titles that several different documents share (SDAIA's "2025").
    # The profile comes from the seed rather than a new argument, so every caller
    # of _finish is unchanged.
    renamed = disambiguate_titles(documents, profile_for(seed_norm))

    # content_hash: what change detection compares between runs. It is the hash of
    # the page's TEXT, not its HTML — HTML churns on every deploy (build ids,
    # cache-busting query strings) and would report every page as modified.
    for r in records:
        r.setdefault("html", "")
        r.setdefault("breadcrumb", [])
        r.setdefault("linked_from_title", "")
        r.setdefault("parent_page_url", "")
        r["content_hash"] = content_key(r.get("text") or "")
    for d in documents:
        # A document is a file we have not downloaded here, so hash what identifies
        # it: its URL plus its title. Real content hashing happens when the
        # pipeline fetches the file.
        d["content_hash"] = content_key(
            f"{d.get('doc_url','')}|{d.get('title','')}")

    # Absolute urls in the captured html, and one openable file per page.
    #
    # Done HERE for the reason content_hash is stamped here: three walkers
    # serialize innerHTML (crawler.py JS_MAIN_CONTENT, strategies.py x2) and a
    # fourth will be written by someone who never read this comment. One exit
    # every walker passes through cannot be forgotten.
    #
    # It also gives the tree/table/list walkers the html files they never wrote
    # (they set html_file=""), so the column means the same thing whichever
    # walker ran. Rewriting the generic walker's files is not wasted work: the
    # in-walk copy exists so a crashed run still leaves something readable, and
    # this pass is what makes a FINISHED run correct.
    #
    # content_hash is unaffected - it hashes text, never html - so nothing
    # re-classifies as `modified` because of this.
    used = {}
    for i, r in enumerate(records):
        page_url = r.get("url") or seed_norm
        r["html"] = absolutize_html(r.get("html") or "", page_url)
        if not r["html"]:
            continue
        name = r.get("html_file") or ""
        if not name:
            _pu = urlparse(page_url)
            parent = r.get("parent_page_url") or ""
            page_part = slugify(urlparse(parent).path) if (_pu.fragment and parent)                 else slugify(_pu.path or "")
            if _pu.fragment and parent:
                # A CHILD RECORD (a modal panel). FLAT, in the same html/ folder as
                # everything else — but named from its own TITLE, not its fragment
                # id. sio.gov.bh gives every panel an id beginning
                # "amiri-decree-law-no-1976-27-concerning-amendments-to-articles-
                # 38-and-139-of-social-insurance-law-", whatever the law is, so a
                # fragment-derived name made all 202 identical after slugify's
                # 80-character cut and the collision counter turned them into
                # ...-with-resp-1 .. -19. Titles are distinct and searchable.
                stem = slugify(f"{page_part}-{clean_doc_title(r.get('title')) or _pu.fragment}")
            else:
                stem = page_part or slugify(r.get("title") or "")
            base = "html/" + (stem or f"page-{i}")
            n = used.get(base, 0)
            used[base] = n + 1
            name = f"{base}.html" if not n else f"{base}-{n}.html"
            r["html_file"] = name
        written = write_page_html(out, name, r["html"], page_url,
                                 r.get("title") or "")
        r["html_file"] = written or name

    n = dict(note or {})
    counts = {"pages": len(records), "documents": len(documents),
              "blocked_pages": n.get("blocked_pages", 0),
              "errors": n.get("errors", 0), "retries": n.get("retries", 0),
              # Pages we crawled anyway, standing on incomplete JavaScript. Zero
              # on every site measured except lloc.gov.bh; non-zero means "read
              # the assets_missing events before trusting this run".
              "asset_failures": n.get("asset_failures", 0),
              "cap_hit": bool(n.get("cap_hit")),
              "seed_loaded": n.get("seed_loaded", True),
              "stopped": n.get("stopped", "")}
    status = run_status(counts)

    # `status` travels in pages.json as well as the event: the file outlives the
    # stdout stream, and whoever reads it later must be able to tell a crawl from
    # a challenge page. Additive — every existing key is untouched. `pages` and
    # `documents` stay the LISTS they have always been, so the counters go in
    # under their own names.
    outcome = {f"n_{k}" if k in ("pages", "documents") else k: v
               for k, v in counts.items()}
    (out / "pages.json").write_text(
        json.dumps({"seed": seed_norm, "shape": shape, "status": status,
                    **outcome, "resume": n.get("resume") or {},
                    "pages": records,
                    "documents": documents, "chrome_dropped": chrome_dropped},
                   ensure_ascii=False, indent=2),
        encoding="utf-8")
    _write_excel(out / "pages.xlsx", records, documents, chrome_dropped)

    emit({"event": "done", "status": status, "shape": shape, **counts,
          "resume": n.get("resume") or {},
          "chrome_dropped": len(chrome_dropped),
          "titles_disambiguated": renamed,
          "out_dir": str(out), "xlsx": str(out / "pages.xlsx")})
    return records, documents


# ============================================================================
# SECTION E — write the Excel workbook + command-line entry point
# ============================================================================


def _write_excel(path, records, documents, chrome_dropped=None):
    import pandas as pd
    CELL_MAX = 32000
    page_rows = [{
        "section_path": r["section_path"],
        "title": r["title"],
        "url": r["url"],
        "depth": r["depth"],
        "linked_from_title": r.get("linked_from_title", ""),
        "parent_page_url": r.get("parent_page_url", ""),
        "status": r["status"],
        "n_pdfs": r["n_pdfs"],
        "pdf_links": r["pdf_links"][:CELL_MAX],
        "text_len": r["text_len"],
        "content_hash": r.get("content_hash", ""),
        "html_file": r["html_file"],
        "text_preview": (r["text"] or "")[:CELL_MAX],
    } for r in records]

    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        pd.DataFrame(page_rows).to_excel(xw, sheet_name="pages", index=False)
        if documents:
            pd.DataFrame(documents).to_excel(xw, sheet_name="documents", index=False)
        # Audit sheet: what the header/footer rule removed. If a real document
        # ever shows up here, the rule is wrong — check this before trusting a run.
        if chrome_dropped:
            pd.DataFrame(chrome_dropped).to_excel(
                xw, sheet_name="chrome_dropped", index=False)


# ============================================================================
# SECTION F — several SECTIONS of one site in one command (--subpaths), and
#             documents the crawl cannot reach (--documents)
#
# Everything here sits ABOVE crawl(). It calls crawl() once per section and
# never changes its signature, because `crawl` is imported by the live pipeline
# (crawler/generic_crawler_wrapper.py:253, used by MC, MISA, SAMA and ZATCA) and
# by crawler/fingerprint.py. main() is imported by nothing, so the driver is safe
# to live here.
#
# WITHOUT --subpaths NOTHING IN THIS SECTION RUNS. `--url` behaves exactly as it
# always has, which matters: the wrapper and baseline.py both invoke this file as
# a subprocess with `--url`.
# ============================================================================

#: Worst wins. The same words run_status already uses, ranked by how much they
#: should stop you: a blocked host invalidates everything, an empty section is a
#: failed extraction, a short walk is still usable data. NOT a second definition
#: of "did this run work" — it orders run_status's answers, it does not replace
#: them.
STATUS_RANK = {"ok": 0, "incomplete": 1, "not-run": 1, "zero": 2, "blocked": 3}


def read_subpaths(subpaths: str, subpaths_file=None) -> list:
    """The section list, from --subpaths and/or --subpaths-file. Order is kept,
    duplicates dropped (a repeated section would crawl twice into one dir)."""
    raw = list((subpaths or "").split(","))
    if subpaths_file:
        path = Path(subpaths_file)
        if not path.exists():
            raise SystemExit(f"no such file: {path}")
        for line in path.read_text(encoding="utf-8").splitlines():
            raw.append(line.split("#", 1)[0])
    out, seen = [], set()
    for item in raw:
        item = item.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def section_url(seed: str, sub: str) -> str:
    """A full URL is taken as given; anything else is joined onto the seed."""
    if sub.lower().startswith(("http://", "https://")):
        return normalize_url(sub)
    base = seed if seed.endswith("/") else seed + "/"
    return normalize_url(urljoin(base, sub.lstrip("/")))


def section_dir_name(url: str, seed: str, taken: set) -> str:
    """A readable folder name per section, from the part of the path the section
    adds to the seed. Collisions get a numeric suffix rather than overwriting:
    two sections sharing one directory is the merged-baseline bug by accident."""
    seed_path = urlparse(seed).path.rstrip("/")
    path = urlparse(url).path
    tail = path[len(seed_path):] if path.startswith(seed_path) else path
    name = slugify(tail) or slugify(path) or "section"
    candidate, n = name, 1
    while candidate in taken:
        n += 1
        candidate = f"{name}-{n}"
    taken.add(candidate)
    return candidate


def preflight(url: str, timeout: int = 20) -> tuple:
    """Is this URL there at all? Returns (ok, detail).

    Deliberately shallow. It catches the hard 404 a typo produces; it cannot tell
    you a single-page app answered 200 and rendered nothing — that is what the
    crawl's own `zero` status is for. HEAD first, because some servers answer it
    without building the page; GET after, since plenty of government servers
    reject HEAD outright.

    REQUESTS, NOT URLLIB. Measured on cbe.org.eg with an IDENTICAL User-Agent:
    urllib gets a 269-byte "Request Rejected" page served as HTTP 200, requests
    gets the real page. The WAF is judging the header signature, not the client
    being a browser — so a urllib preflight there reported `ok 200` for every
    path including a typo, which is worse than no check. urllib is kept as the
    fallback for an environment without requests.

    MEASURED 2026-08-18 once it used requests: cbe.org.eg returns a real HTTP 404
    for /en/no-such-section-xyz and 200 for every genuine section. So the check
    works there after all — the "this host answers 200 for anything" reading was
    an artefact of urllib being refused, not the site's behaviour.

    KNOWN LIMIT that remains: a site CAN answer any path with 200 and render a
    soft-404, and a browser navigation does not fail on a 404 page either — so a
    section can still crawl to one recorded page. The `thin` note is what catches
    that; this only has to catch the hard 404.
    """
    if requests is not None:
        for method in ("head", "get"):
            try:
                r = getattr(requests, method)(
                    url, headers={"User-Agent": USER_AGENT}, timeout=timeout,
                    allow_redirects=True,
                    **({"stream": True} if method == "get" else {}))
                if method == "get":
                    r.close()
                if r.status_code in (403, 405, 501) and method == "head":
                    continue      # the server dislikes HEAD, not the URL
                if r.status_code == 403:
                    # Often the WAF, not a missing page. Let the crawl decide;
                    # blocked_reason reads the actual rendered page.
                    return True, "403 (may be bot protection - the crawl will judge)"
                if r.status_code < 400:
                    return True, f"{r.status_code} {method.upper()}"
                return False, f"HTTP {r.status_code}"
            except Exception as e:
                if method == "head":
                    continue
                return False, f"{type(e).__name__}: {str(e)[:80]}"
        return False, "unreachable"

    for method in ("HEAD", "GET"):
        req = urllib.request.Request(url, method=method,
                                     headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return True, f"{resp.status} {method}"
        except urllib.error.HTTPError as e:
            if e.code in (403, 405, 501) and method == "HEAD":
                continue
            if e.code == 403:
                return True, "403 (may be bot protection - the crawl will judge)"
            return False, f"HTTP {e.code}"
        except Exception as e:
            if method == "HEAD":
                continue
            return False, f"{type(e).__name__}: {str(e)[:80]}"
    return False, "unreachable"


def read_outcome(out_dir: Path) -> dict:
    """One section's own verdict, read back from pages.json rather than taken
    from crawl()'s return value — the same thing a later reader would see, the
    same reason _report_outcome reads the file."""
    try:
        data = json.loads((out_dir / "pages.json").read_text(encoding="utf-8"))
    except Exception as e:
        return {"status": "zero", "n_pages": 0, "n_documents": 0,
                "stopped": f"could not read pages.json: {e}"}
    return {
        "status": data.get("status", "ok"),
        "n_pages": data.get("n_pages", 0),
        "n_documents": data.get("n_documents", 0),
        "blocked_pages": data.get("blocked_pages", 0),
        "errors": data.get("errors", 0),
        "retries": data.get("retries", 0),
        "stopped": data.get("stopped", ""),
    }


def thin_note(row: dict) -> str:
    """A section that answered but produced almost nothing.

    Preflight cannot catch every typo. A browser navigation does NOT fail on a
    404 page — it renders it — so a bad sub-path still records one page, and
    run_status correctly calls that `ok`: one page IS a successful crawl of one
    page. Measured on cbe.org.eg 2026-08-18:
    `/en/definitely-not-a-real-section` renders a page titled "404" and yields
    1 page / 0 documents, beside `/en/laws-regulations` at 17 pages / 55
    documents. (Its HTTP status IS 404, so the requests-based preflight above
    now stops that one earlier — but a site that soft-404s with a 200 would slip
    through, and this is the net under it.)

    So this is a NOTE, not a status. A sixth status word would put a second
    definition of a working run next to run_status. The number is the evidence; a
    person reads the row and decides. (A REAL thin page exists too:
    /en/sustainability/principles-and-regulatory-framework is a genuine leaf with
    1 page and no attachments — which is exactly why this cannot be an error.)
    """
    if row.get("status") not in ("ok", "incomplete"):
        return ""
    if row.get("n_documents", 0) == 0 and row.get("n_pages", 0) <= 1:
        return ("thin - check this sub-path exists; some sites answer 200 for "
                "any path")
    return ""


# ---------------------------------------------------------------------------
# DECLARED DOCUMENTS (--documents)
#
# Some documents are reachable only from the site's navigation. CBE links its
# Procurement PDF from the "About CBE" menu, so it appears on EVERY page, and the
# header/footer rule correctly files it under `chrome_dropped` rather than
# letting 17 pages contribute 17 copies of it.
#
# The fix is not to weaken that rule. Un-dropping a nav link records it once per
# page, each row carrying the section_path of the page it was found on — measured
# on CBE as "Home > Laws and Regulations", the wrong folder — and documents are
# keyed on (url, section_path), so the same file under three crawled sections
# becomes three documents that propose each other as withdrawn.
#
# So the nav document is DECLARED: named once, with the folder it really belongs
# to, outside the crawl entirely.
#
# HOW IT IS FINGERPRINTED. A declared document has no page text to hash, and
# `url|title` — the usual fallback for a file we have not downloaded — cannot
# move when the publisher replaces the PDF behind an unchanged link. That would
# add a document change detection can never notice changing. So the server is
# asked, in the order crawler/fingerprint.py prefers:
#
#   1. ETag           the publisher's own change stamp
#   2. Last-Modified  the same, weaker
#   3. url|title      the honest fallback, recorded AS a fallback
#
# Measured on the CBE Procurement PDF: ETag ffc4891297f348f3be3d044356700fdb,
# Last-Modified Thu, 16 Jun 2022. Real values, not a clock.
#
# `hash_basis` travels with the row because a stamp that quietly degraded to
# url|title looks exactly like one that did not.
# ---------------------------------------------------------------------------


def parse_document_spec(spec: str, default_section: str) -> dict:
    """One --documents entry. The url is always last, so the form reads
    left-to-right from least to most specific:

        <url>
        <title> :: <url>
        <section path> :: <title> :: <url>
    """
    parts = [p.strip() for p in spec.split("::")]
    parts = [p for p in parts if p]
    if not parts:
        return {}
    url = normalize_url(parts[-1])
    if not url.lower().startswith(("http://", "https://")):
        raise SystemExit(f"--documents entry does not end in a url: {spec!r}")
    title = parts[-2] if len(parts) >= 2 else (title_from_slug(url) or url)
    section = parts[0] if len(parts) >= 3 else default_section
    return {"title": title, "doc_url": url, "section_path": section}


def stamp_declared(url: str, title: str, timeout: int = 25) -> tuple:
    """(content_hash, basis) for a declared document. Never raises: a stamp we
    could not read must not take a run down, it must be visible as a weaker one.

    HEAD then GET, because cbe.org.eg refuses HEAD with 403 and answers GET with
    200. `stream=True` on the GET so the headers arrive without pulling the body.
    """
    etag = lastmod = None
    if requests is not None:
        for method in ("head", "get"):
            try:
                r = getattr(requests, method)(
                    url, headers={"User-Agent": USER_AGENT}, timeout=timeout,
                    allow_redirects=True,
                    **({"stream": True} if method == "get" else {}))
                if r.status_code < 400:
                    etag = (r.headers.get("ETag") or "").strip('"') or None
                    lastmod = (r.headers.get("Last-Modified") or "").strip() or None
                if method == "get":
                    r.close()
                if etag or lastmod:
                    break
            except Exception:
                continue
    if etag:
        return content_key(f"{url}|etag:{etag}"), "etag"
    if lastmod:
        return content_key(f"{url}|last-modified:{lastmod}"), "last-modified"
    return content_key(f"{url}|{title}"), "url|title (WEAK - no server stamp)"


def collect_declared(documents, documents_file=None,
                     documents_section="Documents") -> list:
    """Every --documents / --documents-file entry, stamped and ready to write."""
    specs = list(documents or [])
    if documents_file:
        path = Path(documents_file)
        if not path.exists():
            raise SystemExit(f"no such file: {path}")
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                specs.append(line)
    if not specs:
        return []
    if requests is None:
        print("  note: `requests` is not installed, so declared documents fall "
              "back to a url|title hash", file=sys.stderr)

    out, seen = [], set()
    print(f"Declared documents ({len(specs)})")
    for spec in specs:
        doc = parse_document_spec(spec, documents_section)
        if not doc or doc["doc_url"] in seen:
            continue
        seen.add(doc["doc_url"])
        doc["content_hash"], doc["hash_basis"] = stamp_declared(
            doc["doc_url"], doc["title"])
        doc["type"] = doc_type_of(doc["doc_url"])
        doc["found_on"] = ""          # declared, not found on a crawled page
        doc["subsite"] = "(declared)"
        out.append(doc)
        print(f"  {doc['title']}  [{doc['hash_basis']}]")
        print(f"    {doc['section_path']}  <-  {doc['doc_url']}")
    print()
    return out


# ---------------------------------------------------------------------------
# the roll-up
# ---------------------------------------------------------------------------


def merged_documents(rows: list, root: Path) -> list:
    """Every section's documents, each tagged with the section it came from."""
    docs = []
    for row in rows:
        if row.get("status") == "not-run":
            continue
        try:
            raw = (root / row["dir"] / "pages.json").read_text(encoding="utf-8")
        except Exception:
            continue
        for d in json.loads(raw).get("documents") or []:
            docs.append({"subsite": row["subsite"], **d})
    return docs


def report_duplicates(docs: list) -> list:
    """The same file under two sections. NOT deduplicated on purpose.

    Two causes, and they need opposite responses. Overlapping prefixes
    double-count and make sections propose each other's documents as withdrawn.
    A genuine cross-listing is two real places in the library — the DB agrees,
    document_exists_by_url(url, category) is category-scoped for exactly that.
    Collapsing the rows silently would hide the first and destroy the second, so
    both are reported and a person reads the pair.
    """
    by_url = {}
    for d in docs:
        key = normalize_url(d.get("doc_url") or "")
        if key:
            by_url.setdefault(key, []).append(d.get("subsite", ""))
    return [{"doc_url": url, "subsites": ", ".join(sorted(set(subs)))}
            for url, subs in by_url.items() if len(set(subs)) > 1]


def write_summary(root: Path, rows: list, docs: list, dupes: list) -> None:
    import pandas as pd
    with pd.ExcelWriter(root / "summary.xlsx", engine="openpyxl") as xw:
        pd.DataFrame(rows).to_excel(xw, sheet_name="subsites", index=False)
        if docs:
            pd.DataFrame(docs).to_excel(xw, sheet_name="documents", index=False)
        if dupes:
            pd.DataFrame(dupes).to_excel(xw, sheet_name="duplicates", index=False)
    # The pages sheet is NOT merged here. It carries full page text, each section
    # already has its own pages.xlsx, and _write_excel owns that column contract.
    # A second writer of the same sheet is how two writers drift apart.


def crawl_sections(args) -> int:
    """--subpaths: crawl each section as its own run, sequentially, then roll up.

    FIVE RULES, and the reason each one exists:

    1. ONE OUTPUT DIRECTORY PER SECTION, never merged. Each keeps its own status
       and its own baseline. Merging them is the ZATCA bug: five forms shared one
       baseline, overwrote each other, and every run was quarantined.
    2. PREFLIGHT THE WHOLE LIST FIRST and refuse to crawl if a path is
       unreachable, so a typo costs a second rather than an hour.
    3. SEQUENTIAL, never parallel. Playwright's sync API drives one browser, and
       pacing is most of what keeps a crawl off a WAF.
    4. `blocked` ABORTS THE WHOLE RUN. Continuing to hit other paths on a host
       whose bot wall just answered is what turns a soft block into a permanent
       one — saudiexchange.sa and simah.com were both blocked that way. `zero`
       and `incomplete` do NOT abort: they are that section's problem.
    5. THE ROLL-UP ORDERS run_status's ANSWERS, it does not invent new ones.
    """
    subs = read_subpaths(args.subpaths, args.subpaths_file)
    if not subs:
        raise SystemExit("no sections given - pass --subpaths and/or --subpaths-file")

    seed = normalize_url(args.seed or args.url)
    root = Path(args.out)
    root.mkdir(parents=True, exist_ok=True)

    # Parsed and stamped BEFORE any crawling: a malformed --documents entry
    # should cost a second, not an hour of crawl followed by a SystemExit.
    declared = collect_declared(args.documents, args.documents_file,
                                args.documents_section)

    taken, plan = set(), []
    for sub in subs:
        url = section_url(seed, sub)
        plan.append({"subsite": sub, "url": url,
                     "dir": section_dir_name(url, seed, taken)})

    # ---- preflight ----------------------------------------------------------
    if not args.no_preflight:
        print(f"Preflight {len(plan)} section(s)")
        bad = []
        for item in plan:
            ok, detail = preflight(item["url"])
            item["preflight"] = detail
            print(f"  {'ok  ' if ok else 'FAIL'}  {item['url']}  ({detail})")
            if not ok:
                bad.append(item)
        if bad and not args.skip_unreachable:
            print(f"\n{len(bad)} section(s) did not answer. Nothing was crawled.",
                  file=sys.stderr)
            print("  Fix the list, or pass --skip-unreachable to crawl the rest.",
                  file=sys.stderr)
            return 2
        for item in bad:
            item["status"] = "not-run"
        print()

    # ---- crawl, one section at a time --------------------------------------
    rows, aborted = [], False
    for i, item in enumerate(plan, 1):
        if item.get("status") == "not-run":
            rows.append({**item, "n_pages": 0, "n_documents": 0,
                         "stopped": "unreachable at preflight"})
            continue
        if aborted:
            rows.append({**item, "status": "not-run", "n_pages": 0,
                         "n_documents": 0,
                         "stopped": "skipped: an earlier section was blocked"})
            continue

        out_dir = root / item["dir"]
        print(f"[{i}/{len(plan)}] {item['url']}  ->  {out_dir}")
        # --follow-section-links only: the section's own name, from its sub-path,
        # is what the breadcrumb must contain. The last segment, so a nested
        # sub-path like sustainability/principles-and-regulatory-framework is
        # matched on the part the site would actually name.
        widen = {}
        if args.follow_section_links:
            widen = {"outer_prefix": scope_prefix(urlparse(seed).path) or "/",
                     "section_name": item["subsite"].rstrip("/").split("/")[-1],
                     "max_widened": args.max_widened}
        try:
            crawl(item["url"], out_dir,
                  max_pages=args.max_pages, max_depth=args.max_depth,
                  scope=args.scope, headless=not args.headful,
                  wait_ms=args.wait_ms, strategy=args.strategy,
                  group_headings=args.group_headings,
                  list_details=not args.no_details,
                  max_details=args.max_details or None, **widen)
        except Exception as e:
            print(f"  crawl raised {type(e).__name__}: {e}", file=sys.stderr)

        outcome = read_outcome(out_dir)
        rows.append({**item, **outcome})
        print(f"  {outcome['status']}: {outcome['n_pages']} pages, "
              f"{outcome['n_documents']} documents")
        if outcome["status"] == "blocked":
            aborted = True
            print("  BLOCKED - stopping. Re-running the rest now is what turns a "
                  "soft block into a permanent one.", file=sys.stderr)

    # ---- roll up ------------------------------------------------------------
    for r in rows:
        r["note"] = r.get("note") or thin_note(r)
    docs = merged_documents(rows, root) + declared
    dupes = report_duplicates(docs)
    overall = "ok"
    for r in rows:
        if STATUS_RANK.get(r.get("status"), 0) > STATUS_RANK.get(overall, 0):
            overall = r["status"]
    if overall == "not-run":
        overall = "incomplete"

    (root / "summary.json").write_text(
        json.dumps({"seed": seed, "scope": args.scope, "status": overall,
                    "n_subsites": len(rows), "n_documents": len(docs),
                    "n_declared_documents": len(declared),
                    "n_duplicate_documents": len(dupes), "subsites": rows},
                   ensure_ascii=False, indent=2), encoding="utf-8")
    write_summary(root, rows, docs, dupes)

    print("\n" + "=" * 70)
    print(f"{overall.upper()}  -  {len(rows)} section(s), {len(docs)} documents")
    for r in rows:
        print(f"  {r.get('status', '?'):<11} {r['subsite']:<28} "
              f"{r.get('n_pages', 0):>5} pages  {r.get('n_documents', 0):>5} docs"
              + (f"   {r['stopped']}" if r.get("stopped") else "")
              + (f"   [{r['note']}]" if r.get("note") else ""))
    if declared:
        weak = [d for d in declared if d["hash_basis"].startswith("url|title")]
        print(f"  {len(declared)} declared document(s), not crawled"
              + (f" - {len(weak)} with NO server stamp, which cannot report a "
                 f"replacement behind the same url" if weak else ""))
    if dupes:
        print(f"\n  {len(dupes)} document(s) appear under more than one section - "
              f"see the duplicates sheet.\n  Overlapping prefixes double-count and "
              f"propose each other's documents as withdrawn; a genuine "
              f"cross-listing is two real places. Read the pair.")
    print(f"  {root / 'summary.json'}")
    return 1 if overall in FATAL_STATUSES else 0


def main():
    ap = argparse.ArgumentParser(description="Standalone Playwright sidebar crawler (test tool)")
    # --url is the original single-site flag and MUST keep working: the live
    # wrapper (generic_crawler_wrapper.py:258) and baseline.py both invoke this
    # file as a subprocess with it. --seed is the same thing under the name the
    # multi-section form reads better with.
    ap.add_argument("--url", help="Seed URL (single-site crawl)")
    ap.add_argument("--seed", help="alias for --url; with --subpaths, the site root")
    ap.add_argument("--subpaths", default="",
                    help="comma-separated sections under --seed, each crawled as "
                         "its own run into its own directory, e.g. "
                         "governance,laws-regulations,aml-cft. Nested paths are "
                         "fine (sustainability/principles-and-regulatory-framework)")
    ap.add_argument("--subpaths-file",
                    help="one section per line (# comments allowed)")
    ap.add_argument("--documents", action="append", default=[],
                    help="record a document WITHOUT crawling it, for a file the "
                         "site links only from its navigation (the header/footer "
                         "rule files those under chrome_dropped, correctly). "
                         "Repeatable. Form: '<url>', '<title> :: <url>', or "
                         "'<section path> :: <title> :: <url>'")
    ap.add_argument("--documents-file",
                    help="one --documents entry per line (# comments allowed)")
    ap.add_argument("--documents-section", default="Documents",
                    help="folder trail for --documents entries that name none")
    ap.add_argument("--skip-unreachable", action="store_true",
                    help="--subpaths: crawl the sections that answered instead of "
                         "refusing the whole list (default: refuse, so a typo is "
                         "loud)")
    ap.add_argument("--no-preflight", action="store_true",
                    help="--subpaths: do not check the sections before crawling")
    ap.add_argument("--follow-section-links", action="store_true",
                    help="--subpaths + --scope prefix: also follow links found ON "
                         "a section page that fall OUTSIDE its path but inside the "
                         "--seed, and keep them only if the site's own breadcrumb "
                         "names the section. For sites that publish flat, where a "
                         "section's content lives at sibling urls (sio.gov.bh). "
                         "One hop, header/footer links skipped, capped by "
                         "--max-widened. Off by default: it changes what a prefix "
                         "crawl walks")
    ap.add_argument("--max-widened", type=int, default=60,
                    help="--follow-section-links: most off-path candidates one "
                         "section may fetch. A rejected page is not recorded, so "
                         "it does not count toward --max-pages; this is the cap "
                         "that does (default 60)")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--exclude", action="append", default=[],
                    help="url path prefix to skip, repeatable. Skips both the "
                         "page walk and any document under it -- for a subtree "
                         "another source already owns, e.g. CBE circulars.")
    ap.add_argument("--max-pages", type=int, default=150)
    ap.add_argument("--max-depth", type=int, default=8)
    ap.add_argument("--scope", choices=["auto", "breadcrumb", "prefix", "host"],
                default="auto",
                help="auto = work it out from the landing page; or force one by hand")
    ap.add_argument("--strategy", choices=["auto", "tree", "table", "list", "generic"],
                    default="auto",
                    help="auto = detect layout (tree/table) and dispatch; else force one")
    ap.add_argument("--group-headings", action="store_true",
                    help="Append the on-page heading a document sits under to its "
                         "section_path (aml.gov.sa 'Laws and Regulations'). Off by "
                         "default: on other sites those headings are dates or titles.")
    ap.add_argument("--headful", action="store_true", help="Show the browser window")
    ap.add_argument("--wait-ms", type=int, default=700, help="Settle wait after each page")
    ap.add_argument("--no-details", action="store_true",
                    help="list shape: walk the listing only, skip detail pages "
                         "(cheap full inventory — use for change detection)")
    ap.add_argument("--max-details", type=int, default=0,
                    help="list shape: cap how many detail pages are opened")
    args = ap.parse_args()

    if not (args.url or args.seed):
        ap.error("one of --url or --seed is required")

    # MULTI-SECTION: several sections of one site, each its own run, then a
    # roll-up. Its own exit code, because 9 sections have 9 statuses and
    # _report_outcome reads exactly one pages.json.
    if args.subpaths or args.subpaths_file:
        return crawl_sections(args)

    # SINGLE SITE: unchanged from before, byte for byte. A --documents entry is
    # still honoured so one file can be declared alongside a single-section crawl;
    # with no --documents this is exactly the old path.
    declared = collect_declared(args.documents, args.documents_file,
                                args.documents_section)

    crawl(args.seed or args.url, args.out,
          max_pages=args.max_pages, max_depth=args.max_depth,
          list_details=not args.no_details,
          max_details=args.max_details or None,
          scope=args.scope, headless=not args.headful, wait_ms=args.wait_ms,
          strategy=args.strategy, group_headings=args.group_headings,
          exclude_paths=args.exclude)

    if declared:
        # Appended to the run's own documents list rather than kept in a second
        # file, so `pages.json` stays the one answer for what this crawl found.
        out = Path(args.out)
        data = json.loads((out / "pages.json").read_text(encoding="utf-8"))
        data["documents"] = (data.get("documents") or []) + declared
        data["n_documents"] = len(data["documents"])
        data["n_declared_documents"] = len(declared)
        (out / "pages.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        _write_excel(out / "pages.xlsx", data.get("pages") or [],
                     data["documents"], data.get("chrome_dropped") or [])
        print(f"  {len(declared)} declared document(s) added, not crawled")

    # The status is authoritative, and `pages.json` is where it survives the
    # process. A blocked or empty crawl exits non-zero so a caller that reads
    # nothing else still cannot mistake it for a crawl; INCOMPLETE exits 0 by
    # design — the rows are worth keeping, they just are not the whole site.
    return _report_outcome(Path(args.out))


def _report_outcome(out: Path) -> int:
    """Print the outcome and turn it into an exit code. Reads pages.json rather
    than a return value so it reports the same thing a later reader would see."""
    try:
        data = json.loads((out / "pages.json").read_text(encoding="utf-8"))
    except Exception as e:
        print(f"\nNO-RESULT  -  could not read {out / 'pages.json'}: {e}",
              file=sys.stderr)
        return 1

    status = data.get("status", "ok")
    line = (f"{data.get('n_pages', 0)} pages | {data.get('n_documents', 0)} documents"
            f" | {data.get('blocked_pages', 0)} blocked"
            f" | {data.get('errors', 0)} errors | {data.get('retries', 0)} retries")
    if status == "ok":
        print(f"\nOK  -  {line}\n  {out}")
        return 0

    w = sys.stderr
    print(f"\n{'=' * 70}\n{status.upper()}  -  {data.get('seed', '')}\n{'=' * 70}", file=w)
    print(f"  {line}", file=w)
    if data.get("stopped"):
        print(f"  stopped: {data['stopped']}", file=w)
    if data.get("resume"):
        print(f"  resume:  {json.dumps(data['resume'])}", file=w)
    print(f"  {_NEXT_STEP.get(status, '')}\n  read: {out / 'pages.json'}", file=w)
    return 1 if status in FATAL_STATUSES else 0


# Kept next to the exit code so the two cannot say different things. ASCII only:
# this text lands in scheduler logs and pipes, where a Windows console encoding
# turns a stray em-dash into a UnicodeEncodeError.
_NEXT_STEP = {
    "blocked": ("A bot-protection wall answered instead of the site. Nothing here can\n"
                "  be used. Slow the pacing, or give this source a hand-written\n"
                "  crawler. Do NOT re-run in a loop - that is what trips a WAF."),
    "zero": ("No pages were recorded, which is a failed extraction, not an empty\n"
             "  site. Check the scope and the shape first - calibrate_shape.py and\n"
             "  calibrate_scope.py both exit non-zero on a wrong answer."),
    "incomplete": ("Part of the site was not walked, so these counts are a floor, not a\n"
                   "  total. Read `stopped` above before comparing this run with any\n"
                   "  other, and never treat it as coverage."),
}


if __name__ == "__main__":
    sys.exit(main())
