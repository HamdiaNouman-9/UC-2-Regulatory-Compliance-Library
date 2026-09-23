"""CBI — Central Bank of the Islamic Republic of Iran, Laws & Regulations.

Three listing pages under one section, read as one source. Config lives in
config/sources/cbi.yml; this file is the `mode: custom` crawler it names.

---------------------------------------------------------------------------
1. WHY THIS IS NOT `mode: generic`
---------------------------------------------------------------------------
EVERY LISTING ROW LINKS TO AN .aspx THAT SERVES A PDF. Measured 2026-09-23:

    GET https://www.cbi.ir/page/2234.aspx -> 200, application/pdf, 780.8 KB

and the engine decides what a link IS from its extension, its query string and
a `document`/`download` path segment. Checked against this branch:

    is_document_link("https://www.cbi.ir/page/2234.aspx") -> False
    doc_type_of(...)                                       -> "ASPX"

So on the generic engine the eleven instruments are not documents at all: the
crawl records the listing page and harvests nothing. That is what the capped run
showed, and it is the whole reason this file exists.

A HOST RULE IN `is_document_link` WOULD HAVE BEEN WORSE, and that function's own
docstring says why. ZATCA, 2026-08-12: marking host-matching links terminal gave
`n_pages=1, n_documents=38`, and the 38 were Contact Us, Careers, News, Magazine
and Brand Identity -- "the crawl also never went deeper, because every link it
could have followed had been marked terminal". CBI reproduces that exactly. Its
sidebar is /page/<digits>.aspx too -- Executive Board 1389, Treasury of National
Jewels 1475, Library 1450 -- so any rule broad enough to catch the laws catches
the bank's org chart, in shared code, for every regulator that ever crawls a
host with the same url shape.

Here the rows are taken from the listing's content container and the org chart
is never in scope. `file_type` is stamped PDF from the ROW, not inferred from
the url, which is the other half of what the generic engine could not do.

---------------------------------------------------------------------------
2. THE BROWSER HAS TO BE A REAL ONE, AND IT HAS TO ASK TWICE
---------------------------------------------------------------------------
www.cbi.ir sits behind F5/Shape. It refuses HTTP 200 WITH AN HTML BODY, which
is the CBE trap (config/sources/cbe.yml: a 269-byte "Request Rejected" served as
200) and the LLOC trap (a throttle arriving as an empty-parsing 404) in one
host: an empty listing and a refused one are indistinguishable to anything that
trusts the status line. So `_judge` reads the BODY.

MEASURED 2026-09-23, seven client shapes, in the order they were tried:

    curl, browser headers                 200, 40 KB  -> TSPD js challenge
    requests.Session, browser headers     200, 5.5 KB -> the same challenge
    Playwright bundled chromium, headless 200, 125 B  -> "Request Rejected"
    Playwright bundled chromium, HEADFUL  200, 125 B  -> "Request Rejected"
    generic_crawler.crawler, depth 0      status ZERO -> "Request Rejected"
    ---- and then, after the site was confirmed to load by hand ----
    real Chrome, persistent profile       200         -> the challenge, no block
    the same, RELOADED once               200, 58 KB  -> THE PAGE

AN EARLIER VERSION OF THIS FILE CONCLUDED "it is the address being judged and
not the client", citing headful being refused exactly as headless was. THAT WAS
WRONG, and it was wrong in the expensive direction: it is the CLIENT. The site
loads by hand on this very network, which is what disproved it. What the first
four clients have in common is not an address, it is that every one of them is
detectable as automation -- Playwright's BUNDLED chromium build, a blank
profile, and `navigator.webdriver`. Three changes fix it, and `_render` makes
all three:

    channel="chrome"                            the INSTALLED browser
    launch_persistent_context(user_data_dir)    a profile that keeps cookies
    --disable-blink-features=AutomationControlled

AND THEN IT MUST ASK TWICE. The first request for a url returns the challenge:
F5 runs its javascript, sets TSPD_101, and does NOT swap the document. The
SECOND request carries the cookie and returns the page. That is why `_render`
reloads rather than giving up -- measured, attempt 1 challenge / attempt 2 page.

DO NOT WAIT ON `networkidle`. It never settles on this host: a page that had
already rendered timed out at 90 seconds. `_render` waits for the page's own
footer text instead, which is the thing we came for and is absent from the
challenge page.

`generic_crawler.blockcheck.text_is_blocked` is called first and kept -- one
definition of "is this a bot wall", shared with both engines. It does not carry
F5's wording today:

    text_is_blocked("Request Rejected", "The requested URL was rejected...")
    -> ""

so `_F5` below adds it FOR THIS SOURCE ONLY. Adding it to `BLOCK_RE` would be
the better fix and belongs in a change of its own: that regex is on every
regulator's path, and a false positive there fails runs that are fine.

---------------------------------------------------------------------------
3. THE LISTING, AS THE SITE ACTUALLY WRITES IT
---------------------------------------------------------------------------
CAPTURED 2026-09-23 through the client in §2. This is the markup, not a guess:

    <ul class="simplelist">
      <li><a href="/page/2234.aspx">The Monetary and Banking Act</a>
          <span dir="ltr">781 KB -</span>
          <img alt="PDF icon" src="/Images/icon_pdf.gif"/></li>

Three things follow, and all three are why this parser is small:

  * THE TITLE IS THE ANCHOR TEXT, already clean. No size, no format label -- so
    the shared `clean_doc_title` has nothing to do here and is applied anyway,
    because it costs nothing and the day CBI changes the template it is the
    rule every other regulator already uses.
  * THE SIZE IS A SIBLING <span>, not part of the link. An earlier version read
    it off the anchor and got "" for every row, which silently downgraded every
    fingerprint to url|title. It is read from the ROW.
  * THE SITE DECLARES THE FILE TYPE, in the icon's alt text. `file_type` comes
    from there rather than from the url (which says ASPX) or from a hardcoded
    "PDF" -- so a row CBI publishes as a DOC arrives as a DOC.

MEASURED ROW COUNTS, from that same capture:

    /simplelist/1457.aspx  Laws          5 rows
    /simplelist/1458.aspx  Regulations   6 rows
    /simplelist/1459.aspx  Circulars     0 rows   <- the <ul> EXISTS and is empty

Circulars having a real, empty `.simplelist` is what makes "empty" the site's
answer rather than a failed read. `min_rows` carries the floor per listing; a
listing under its floor raises a COVERAGE GAP, which the composite forwards to
the completeness gate (generic_crawler_wrapper.py::CompositeCrawler.last_result).
Circulars' floor is 0, and becomes a real number the day CBI publishes there.

NOTHING HAS TO BE ADDED FOR CIRCULARS TO APPEAR. The folder tree is walked from
each document's `doc_path` (orchestrator.py::_walk_folders), so a folder exists
only where a document put one -- which is why a Circulars folder is absent from
the workbook today rather than sitting there empty. The listing is still read on
every run, so the first document CBI publishes there arrives with
`doc_path[... , "Circulars", title]` and the folder is created on that run. No
code change, no config change; raise `min_rows["Circulars"]` afterwards so a
later disappearance is reported.

---------------------------------------------------------------------------
4. ONE CLASS, SEVERAL SECTIONS -- AND WHY THE TRAIL IS CLEANED
---------------------------------------------------------------------------
This class is instantiated once per SECTION, not once per regulator, because
`disappeared` is scoped on (regulator, source_system) and each section is its
own source_system. config/sources/cbi.yml holds two today:

    Laws and Regulations   3 listings (Laws / Regulations / Circulars)
    Prudential Regulations 1 listing

A section with ONE listing names its category after itself, so the naive trail
says the same thing twice:

    CBI | Prudential Regulations | Prudential Regulations | <title>

`_clean_trail` -- the shared rule, imported not copied -- drops a crumb already
said earlier, and the title is appended AFTER the dedupe because the leaf is the
document. A multi-listing section is untouched: Laws and Regulations still
reads CBI | Laws and Regulations | Laws | <title>.

THE SECTIONS RUN SEQUENTIALLY, which matters more than it looks: they share one
browser profile, and Chrome takes an exclusive lock on a user_data_dir. The
composite's fetch loop is a plain `for` over its sources
(generic_crawler_wrapper.py) and `_pages` closes its context before returning,
so the lock is always free by the time the next section opens. Two sections
fetched in parallel on one profile would fail; give them separate
`user_data_dir` values if that ever changes.
"""

from __future__ import annotations

import logging
import re
import time
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from crawler.fingerprint import stamp_content_hashes
# The ONE definition of "drop a folder crumb already said earlier in the trail".
# Imported rather than copied: it is what every other regulator's doc_path goes
# through, and §4 below depends on its exact behaviour.
from crawler.generic_crawler_wrapper import _clean_trail
from generic_crawler.blockcheck import text_is_blocked
from generic_crawler.crawler import USER_AGENT, clean_doc_title, content_key
from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)

REGULATOR = "Central Bank of Iran (CBI)"
SITE_URL = "https://www.cbi.ir"

#: Where the browser profile lives when the YAML names none. Under output/,
#: which .gitignore already covers -- a profile is a cache, not source.
_DEFAULT_PROFILE = str(
    Path(__file__).resolve().parents[1] / "output" / ".browser" / "cbi")

#: The section the three listings hang off. Stored on every row as the site's
#: own name for the folder; `doc_path` is the library's tree, not this.
SECTION_URL = f"{SITE_URL}/section/1454.aspx"
SECTION_NAME = "Laws & Regulations"

#: category -> the site's listing id. The ORDER IS THE SITE'S, and it is the
#: order rows land in the workbook. Overridable from the YAML.
LISTINGS: Dict[str, int] = {"Laws": 1457, "Regulations": 1458, "Circulars": 1459}

#: The floor measured 2026-09-23. Under it is a coverage gap, not a result.
MIN_ROWS: Dict[str, int] = {"Laws": 5, "Regulations": 6, "Circulars": 0}

#: A listing row. The id MUST be numeric: that is what separates an instrument
#: from the sidebar's hand-named pages (/page/Contact_en.aspx), and it is the
#: cheap half of the §1 problem.
_ROW_HREF = re.compile(r"/page/(\d+)\.aspx\b", re.I)

#: F5's wording, which `blockcheck.BLOCK_RE` does not carry -- §2.
_F5 = re.compile(r"(request rejected|the requested url was rejected"
                 r"|window\[[\"']bobcmn[\"']\])", re.I)

#: `.simplelist` is the real one, CONFIRMED against the captured page (§3) --
#: it is also what the url path is named after. The rest are fallbacks kept for
#: the day the template changes; the first that contains a row link wins, and
#: which one fired is recorded in `last_result["container"]`.
CONTENT_SELECTORS: Sequence[str] = (
    "ul.simplelist", ".simplelist", ".SimpleList",
    "#ctl00_ContentPlaceHolder1_SimpleList", "#content .list", "#ContentArea",
)

#: The page's own footer counter. `_render` waits for this rather than for
#: `networkidle`, which never settles here (§2), and it is absent from the
#: challenge page -- so it doubles as "the real document arrived".
CONTENT_MARKER = "Visits"

#: The icon that names a row's file type: <img alt="PDF icon">. The site is the
#: authority on this; the url is not (it says ASPX).
_ICON_ALT = re.compile(r"\b(PDF|DOCX?|XLSX?|PPTX?|RTF|ZIP|TXT|CSV)\b", re.I)

#: Ancestors a row may never sit inside -- the sidebar carries /page/<digits>
#: links of its own and the structural fallback has no other way to know.
_CHROME_TAGS = ("nav", "header", "footer", "aside")
_CHROME_IDENT = ("menu", "nav", "sidebar", "side-bar", "breadcrumb", "footer",
                 "header", "topbar")

#: A trailing file size, and any format label after it.
#:
#: THE SIZE IS READ OUT, NOT JUST REMOVED, because it is the only field on the
#: row that MOVES when CBI replaces a PDF behind an unchanged url -- the
#: weakness crawler/fingerprint.py names in its third preference.
#:
#: `clean_doc_title` (the shared RERA rule) already strips most of these, and it
#: stays the primary: it is what every other regulator's titles go through. It
#: does NOT strip one shape, measured against it here rather than assumed:
#:
#:     clean_doc_title("The Banking and Monetary Act (1339) 117 KB (PDF)")
#:     -> "The Banking and Monetary Act (1339) 117 KB (PDF)"
#:
#: because its `_FORMAT_TAIL` requires a REAL separator ("| PDF", "- PDF") and a
#: bare space is not one -- deliberately, so the legitimate title "Guide to PDF"
#: survives -- and the unmatched "(PDF)" then blocks `_SIZE_TAIL` behind it.
#: Stripped locally instead of widening the shared regex, which is on every
#: regulator's path and whose narrowness is a decision, not an oversight.
#:
#: A title ending in a bare number is safe: a unit word is required, so
#: "Act (1339)" does not match.
_SIZE = re.compile(
    r"""[\s ]*[\(\[\-–—|,;]*\s*
        (?P<size>\d[\d.,]*\s*(?:bytes?|[KMGT]i?B))
        \s*[\)\]]*
        (?:\s*[\(\[\-–—|]*\s*(?:PDF|DOCX?|XLSX?|PPTX?|RTF|ZIP|TXT|CSV)\s*[\)\]]*)?
        \s*$""",
    re.I | re.X)


#: A size ANYWHERE in the row's trailing text -- for `<span dir="ltr">781 KB -</span>`,
#: whose trailing " -" is why the anchored `_SIZE` above cannot be reused here.
_SIZE_ANY = re.compile(r"(\d[\d.,]*\s*(?:bytes?|[KMGT]i?B))", re.I)


def _split_row_text(raw: str) -> Tuple[str, str]:
    """('The Monetary and Banking Act', '781 KB') from one row's link text.

    The local strip runs FIRST so the shape `clean_doc_title` cannot see is gone
    before it looks, then the shared rule runs and owns everything else.
    """
    raw = _clean(raw)
    size = ""
    m = _SIZE.search(raw)
    if m:
        size = _clean(m.group("size"))
        raw = raw[:m.start()].strip()
    return clean_doc_title(raw), size

#: Content types that mean "the server sent the document, not a challenge page".
#: A stamp read off anything else is a stamp for the WAF -- §4 of _stamp.
_FILE_CTYPES = ("application/pdf", "application/msword", "application/octet-stream",
                "application/vnd.openxmlformats", "application/vnd.ms-")

_WS = re.compile(r"\s+")


def _clean(s: Optional[str]) -> str:
    return _WS.sub(" ", (s or "").replace("​", "")).strip()


class CBIBlocked(RuntimeError):
    """The host answered, and what it answered was not the page.

    Its own class because the caller must not read it as an empty section -- §2:
    both refusals arrive as HTTP 200 with an HTML body.
    """


class CBIListingSource:
    """The three Laws & Regulations listings, as one source.

    ONE SOURCE, THREE CATEGORIES -- not three sources sharing a `source_system`.
    `disappeared` is scoped on (regulator, source_system), so three entries under
    one source_system could only ever be run together anyway, and `only_sources`
    would be a live hazard on them (generic_crawler_wrapper.py::
    build_regulator_crawler). Reading all three here removes the question: this
    source produces the whole section or it raises.
    """

    def __init__(
        self,
        source_system: str = "Laws and Regulations",
        regulator: str = REGULATOR,
        site_url: str = SITE_URL,
        listings: Optional[Dict[str, int]] = None,
        min_rows: Optional[Dict[str, int]] = None,
        doc_path_prefix: Optional[Sequence[str]] = None,
        #: Pinned to the real container in the YAML (§3). Empty means "try
        #: CONTENT_SELECTORS, then fall back structurally".
        content_selector: str = "",
        #: THE INSTALLED browser, not Playwright's bundled build -- §2. Set to ""
        #: to use the bundled one, which this host refuses.
        browser_channel: str = "chrome",
        #: Where the TSPD cookie lives between the first request and the second,
        #: and between runs. Under output/, which is gitignored.
        user_data_dir: str = "",
        #: Headful. Kept configurable because it is the kind of thing a CI box
        #: has to change, but note that headless was refused here even with the
        #: real Chrome channel on the first attempts (§2).
        headless: bool = False,
        #: Attempt 1 gets the challenge BY DESIGN; the reload gets the page.
        #: Three leaves one spare.
        max_attempts: int = 3,
        #: Ask each document's url for an ETag / Last-Modified, one request per
        #: row. ONBOARDING's second-preference fingerprint; see `_stamp` for the
        #: guard that stops a WAF's own ETag being mistaken for a document's.
        fetch_stamps: bool = True,
        #: Seconds between page loads. Three listing loads plus one head per row,
        #: so this is politeness, not throttle protection -- but read §2 before
        #: lowering it.
        request_delay: float = 3.0,
        timeout: int = 90,
    ):
        if not source_system:
            raise ValueError("CBIListingSource needs a source_system -- it is "
                             "the key the completeness gate scopes on")
        self.regulator = regulator
        self.source_system = source_system
        self.site_url = site_url.rstrip("/")
        self.listings = dict(listings or LISTINGS)
        self.min_rows = dict(min_rows if min_rows is not None else MIN_ROWS)
        self.doc_path_prefix = list(doc_path_prefix or [regulator, source_system])
        self.content_selector = content_selector or ""
        self.browser_channel = browser_channel or ""
        self.user_data_dir = str(user_data_dir or _DEFAULT_PROFILE)
        self.headless = bool(headless)
        self.max_attempts = max(1, int(max_attempts))
        self.fetch_stamps = bool(fetch_stamps)
        self.request_delay = float(request_delay)
        self.timeout = int(timeout)
        self.last_result: dict = {}

        if self.doc_path_prefix[:1] != [regulator]:
            raise ValueError(
                f"doc_path_prefix must start with the regulator {regulator!r}, "
                f"got {self.doc_path_prefix!r}")

    @property
    def source_systems(self) -> List[str]:
        return [self.source_system]

    # ------------------------------------------------------------------ #
    #  reading a listing                                                   #
    # ------------------------------------------------------------------ #

    def _listing_url(self, list_id: int) -> str:
        return f"{self.site_url}/simplelist/{list_id}.aspx"

    def _browser(self, pw):
        """The one client this host answers -- §2 lists the six it does not.

        `launch_persistent_context` rather than `launch` + `new_context` because
        the profile is half the point: F5 sets TSPD_101 on the first request and
        the cookie has to survive to the second. The other half is
        `channel="chrome"`, which runs the INSTALLED browser rather than
        Playwright's bundled build.
        """
        Path(self.user_data_dir).mkdir(parents=True, exist_ok=True)
        return pw.chromium.launch_persistent_context(
            self.user_data_dir,
            channel=self.browser_channel or None,
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled",
                  "--disable-dev-shm-usage"],
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1366, "height": 900},
        )

    def _render_on(self, page, url: str) -> str:
        """One listing, asking as many times as §2 says it takes.

        ATTEMPT 1 GETTING THE CHALLENGE IS NORMAL: F5 runs its script, sets the
        cookie and leaves the document in place. The reload is what collects the
        page. Measured -- attempt 1 challenge, attempt 2 page.

        Waits for `CONTENT_MARKER`, never `networkidle`: the latter timed out at
        90 seconds on a page that had already rendered.
        """
        html, last_err = "", None
        for attempt in range(1, self.max_attempts + 1):
            try:
                if attempt == 1:
                    page.goto(url, wait_until="domcontentloaded",
                              timeout=self.timeout * 1000)
                else:
                    page.reload(wait_until="domcontentloaded",
                                timeout=self.timeout * 1000)
                page.wait_for_function(
                    "m => document.body && document.body.innerText.includes(m)",
                    arg=CONTENT_MARKER, timeout=25000)
                html = page.content()
                last_err = None
                logger.debug("CBI %s: content on attempt %d", url, attempt)
                break
            except Exception as e:
                # EVERY navigation failure retries, not just the timeout.
                # `ERR_INTERNET_DISCONNECTED` on a reload once escaped this loop
                # as a raw Playwright error, past both the retry and `_judge`.
                # A dropped network and a challenge page are different problems
                # and BOTH deserve the second attempt this host needs anyway.
                last_err = e
                try:
                    html = page.content()
                except Exception:
                    html = ""
                logger.info("CBI %s: attempt %d did not reach content "
                            "(%d bytes, %s) -- retrying", url, attempt,
                            len(html), type(e).__name__)
                if attempt < self.max_attempts and self.request_delay:
                    time.sleep(self.request_delay)

        if last_err is not None and not html:
            # Nothing to judge. A transport failure is NOT a block, and calling
            # it one would send the reader to the wrong page of notes.
            raise RuntimeError(
                f"CBI {url}: {self.max_attempts} attempts, no page. "
                f"Last error: {type(last_err).__name__}: {last_err}") from last_err

        self._judge(url, html, _clean(page.title()),
                    page.evaluate(
                        "() => document.body ? document.body.innerText : ''"))
        return html

    def _render(self, url: str) -> str:
        """`_render_on` with a browser of its own, for a single listing.

        `fetch_documents` opens ONE browser for all three instead, so the
        profile's cookie is paid for once rather than three times.
        """
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            ctx = self._browser(pw)
            try:
                page = ctx.pages[0] if ctx.pages else ctx.new_page()
                return self._render_on(page, url)
            finally:
                ctx.close()

    @staticmethod
    def _judge(url: str, html: str, title: str = "", text: str = "") -> None:
        """Raise unless this really is a page from the site.

        THE SHARED CHECK RUNS FIRST so Cloudflare/Akamai wording keeps ONE
        definition, then F5's is added locally -- §2. Neither is detectable from
        the status line, which is 200 for both.
        """
        shared = text_is_blocked(title, text or html)
        if shared:
            raise CBIBlocked(f"{url}: bot-protection wall ({shared}).")
        m = _F5.search(f"{title}\n{text}\n{html[:4000]}")
        if m:
            raise CBIBlocked(
                f"{url}: F5 refusal ({m.group(0)[:40]!r}) served as HTTP 200. "
                f"This is NOT an empty section -- see crawler/cbi_crawler.py §2.")
        if len(html or "") < 2000:
            raise CBIBlocked(
                f"{url}: {len(html or '')} bytes of body. The listing template "
                f"alone is tens of KB -- this is a refusal, not a page.")

    # ------------------------------------------------------------------ #
    #  finding the rows                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _in_chrome(el) -> bool:
        for node in getattr(el, "parents", []):
            if (getattr(node, "name", "") or "").lower() in _CHROME_TAGS:
                return True
            if not hasattr(node, "get"):
                continue
            blob = (str(node.get("id") or "") + " "
                    + " ".join(node.get("class") or [])).lower()
            if any(w in blob for w in _CHROME_IDENT):
                return True
        return False

    def _container(self, soup) -> Tuple[object, str]:
        """The element holding the listing, and how it was found.

        `how` is recorded because §3's selectors are candidates: the first export
        is what turns one of them into a fact.
        """
        if self.content_selector:
            el = soup.select_one(self.content_selector)
            if el is None:
                raise CBIBlocked(
                    f"content_selector {self.content_selector!r} matched nothing. "
                    f"It is PINNED in the YAML, so this is the site changing "
                    f"shape -- not something to paper over with the fallback.")
            return el, f"configured:{self.content_selector}"

        # TWO PASSES, and the second one matters more than it looks.
        #
        # Pass 1 wants a container that HAS rows. Pass 2 accepts one that merely
        # EXISTS, because "the listing is there and it is empty" is CBI's actual
        # answer for Circulars -- it serves `<ul class="simplelist">` with no
        # <li> -- and that is the only thing that distinguishes an empty section
        # from a failed read once `_judge` has passed.
        #
        # NOTE `is not None`, never truthiness: an empty bs4 Tag has __len__ 0
        # and is FALSY. Written as `if el:` this skipped the empty Circulars
        # container, fell through to the structural guess, and reported
        # "fallback:whole-document" for a page whose container was right there.
        for sel in CONTENT_SELECTORS:
            el = soup.select_one(sel)
            if el is not None and _ROW_HREF.search(str(el)):
                return el, f"candidate:{sel}"
        for sel in CONTENT_SELECTORS:
            el = soup.select_one(sel)
            if el is not None:
                return el, f"candidate:{sel} (present, no rows)"

        # Structural fallback: the ancestor holding the most row links, ignoring
        # anything inside the site chrome (§1).
        best, best_n = None, 0
        for a in soup.find_all("a", href=_ROW_HREF):
            if self._in_chrome(a):
                continue
            node = a.parent
            for _ in range(4):
                if node is None:
                    break
                n = sum(1 for x in node.find_all("a", href=_ROW_HREF)
                        if not self._in_chrome(x))
                if n > best_n:
                    best, best_n = node, n
                node = node.parent
        if best is None:
            return soup, "fallback:whole-document (0 rows)"
        return best, f"fallback:structural ({best_n} rows)"

    def _rows(self, html: str, listing_url: str) -> Tuple[List[dict], str]:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        container, how = self._container(soup)

        rows, seen = [], set()
        for a in container.find_all("a", href=_ROW_HREF):
            if self._in_chrome(a):
                continue
            url = urllib.parse.urljoin(listing_url, a.get("href") or "")
            if url in seen:
                continue

            # THE ROW, not the anchor. The size and the type icon are SIBLINGS
            # of the link (§3), so everything but the title is read from the
            # <li> around it. Falling back to the anchor itself keeps this
            # working if the template ever flattens.
            row_el = a.find_parent("li") or a.parent or a

            title, size = _split_row_text(_clean(a.get_text(" ", strip=True)))
            if not size:
                # `<span dir="ltr">781 KB -</span>`. Read from the row's own
                # text minus the title, so a template that moves the span
                # somewhere else in the <li> still finds it.
                tail = _clean(row_el.get_text(" ", strip=True))
                if title and tail.startswith(title):
                    tail = tail[len(title):]
                m = _SIZE_ANY.search(tail)
                if m:
                    size = _clean(m.group(1))

            if not title:
                # A row with no title text is a finding: the title is what the
                # library files it under, and identity includes it.
                logger.warning("CBI: row %s has no title text (row=%r)", url,
                               _clean(row_el.get_text(" ", strip=True))[:80])
                continue

            seen.add(url)
            rows.append({"url": url, "title": title, "size": size,
                         "file_type": self._file_type(row_el)})
        return rows, how

    @staticmethod
    def _file_type(row_el) -> str:
        """The row's own declaration of its type: `<img alt="PDF icon">`.

        The SITE is the authority here and the url is not -- it says ASPX (§1),
        and a hardcoded "PDF" would quietly mislabel the first DOC that CBI
        publishes. Falls back to PDF because that is what all eleven rows are
        today, and a missing icon is not a reason to store no type at all.
        """
        for img in getattr(row_el, "find_all", lambda *a, **k: [])("img"):
            m = _ICON_ALT.search(f"{img.get('alt') or ''} {img.get('src') or ''}")
            if m:
                return m.group(1).upper()
        return "PDF"

    # ------------------------------------------------------------------ #
    #  the fingerprint                                                     #
    # ------------------------------------------------------------------ #

    def _stamp(self, url: str) -> Tuple[str, str]:
        """(token, basis) from the server's own change stamp, or ("", "").

        Shaped after generic_crawler.crawler.stamp_declared -- HEAD then GET,
        because a server that refuses HEAD with 403 answers GET with 200
        (cbe.org.eg), and `stream=True` so the headers arrive without the body.

        COPIED RATHER THAN CALLED, for one reason that matters here: this host
        answers a refused request with HTTP 200 and a full set of headers. An
        ETag read off the TSPD challenge is a fingerprint for the challenge, and
        those rotate per request -- which is the "hash that changes on its own"
        that crawler/fingerprint.py calls worse than no hash at all. So a stamp
        is trusted ONLY when the response looks like the file: a document
        content-type, and no refusal marker in what little body we see.
        """
        try:
            import requests
        except Exception:
            return "", ""
        for method in ("head", "get"):
            try:
                r = getattr(requests, method)(
                    url, headers={"User-Agent": USER_AGENT}, timeout=self.timeout,
                    allow_redirects=True,
                    **({"stream": True} if method == "get" else {}))
                ctype = (r.headers.get("Content-Type") or "").lower()
                etag = (r.headers.get("ETag") or "").strip('"') or None
                lastmod = (r.headers.get("Last-Modified") or "").strip() or None
                body = ""
                if method == "get":
                    try:
                        body = next(r.iter_content(2048), b"").decode(
                            "utf-8", "replace")
                    except Exception:
                        body = ""
                    r.close()
                if r.status_code >= 400:
                    continue
                if not any(c in ctype for c in _FILE_CTYPES):
                    logger.warning(
                        "CBI: %s answered %s with content-type %r -- not the "
                        "document, so its stamp is ignored", url, method, ctype)
                    continue
                if body and _F5.search(body):
                    logger.warning("CBI: %s returned a refusal body; stamp "
                                   "ignored", url)
                    continue
                if etag:
                    return f"etag:{etag}", "etag"
                if lastmod:
                    return f"last-modified:{lastmod}", "last-modified"
            except Exception:
                continue
        return "", ""

    # ------------------------------------------------------------------ #
    #  the pipeline entry point                                            #
    # ------------------------------------------------------------------ #

    def _to_regulatory(self, row: dict, category: str, listing_url: str,
                       stamp: Tuple[str, str]) -> RegulatoryDocument:
        token, basis = stamp
        doc = RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=category,
            title=row["title"],
            document_url=row["url"],
            source_page_url=listing_url,
            # FROM THE ROW'S OWN ICON, not from the url. /page/<id>.aspx serves
            # a PDF and doc_type_of would stamp "ASPX" (§1) -- and a hardcoded
            # "PDF" would mislabel the first DOC CBI publishes. This is the
            # field `mode: declared` cannot get right, and half the reason this
            # source is worth its own file.
            file_type=row.get("file_type") or "PDF",
            # CBI publishes no date on these listings. An invented one is worse
            # than none.
            published_date=None,
            # THE FOLDERS ARE CLEANED, THE TITLE IS APPENDED AFTER -- §4.
            #
            # A section whose listing carries the section's own name would
            # otherwise get that name twice: Prudential Regulations is ONE
            # listing, so its source_system and its category are the same
            # string, and the naive trail reads
            #   CBI | Prudential Regulations | Prudential Regulations | <title>
            # `_clean_trail` drops the repeat and the trail comes out flat, which
            # is what the reference library draws.
            #
            # The title is appended OUTSIDE the dedupe, exactly as
            # DeclaredDocumentsSource does it: the leaf IS the document, so a
            # document that happens to share its folder's name must still keep
            # its own node rather than vanishing into the folder above it.
            doc_path=_clean_trail(self.doc_path_prefix + [category]) + [row["title"]],
            extra_meta={
                "record_kind": "file",
                # The site's own trail, stored raw. doc_path is the library's.
                "section_path": f"Home > {SECTION_NAME} > {category}",
                "section_url": SECTION_URL,
                "listing_url": listing_url,
                "cbi_page_id": _ROW_HREF.search(row["url"]).group(1),
                "file_size_text": row["size"],
                "hash_basis": basis or (
                    "url|title|size" if row["size"] else "url|title (WEAK)"),
            },
        )
        # SET HERE so stamp_content_hashes leaves it alone (it never overwrites).
        #
        # Preference order is crawler/fingerprint.py's: the server's own change
        # stamp when we could read one, otherwise url|title plus the row's file
        # SIZE -- the only field on the listing that moves when CBI replaces a
        # PDF behind an unchanged link. Both are stable between runs, which is
        # the whole requirement.
        key = token or (f"{row['title']}|{row['size']}" if row["size"]
                        else row["title"])
        doc.content_hash = content_key(f"{row['url']}|{key}")
        return doc

    def _pages(self):
        """[(category, list_id, html)] for every listing, from ONE browser.

        THE SINGLE NETWORK SEAM. Everything above this line is parsing and
        everything below is documents, so this is the one method a test
        replaces -- and replacing it is what keeps the suite from launching a
        browser twenty times against a live regulator. If you add another way to
        fetch a listing, route it through here.
        """
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            ctx = self._browser(pw)
            try:
                page = ctx.pages[0] if ctx.pages else ctx.new_page()
                out = []
                for i, (category, list_id) in enumerate(self.listings.items()):
                    if i and self.request_delay:
                        time.sleep(self.request_delay)
                    out.append((category, list_id,
                                self._render_on(page, self._listing_url(list_id))))
            finally:
                ctx.close()
        return out

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        docs: List[RegulatoryDocument] = []
        by_category: Dict[str, int] = {}
        containers: Dict[str, str] = {}
        gaps: List[str] = []
        weak: List[str] = []
        empty: List[str] = []

        # ONE BROWSER FOR ALL THREE LISTINGS. The challenge is paid for on the
        # first url and the cookie carries to the rest -- measured, listings two
        # and three came back on their FIRST attempt once the first had run.
        # Opening a context per listing pays it three times.
        #
        # `_pages` is what the tests replace: they hand back fixtures and never
        # start a browser.
        for i, (category, list_id, html) in enumerate(self._pages()):
            url = self._listing_url(list_id)
            rows, how = self._rows(html, url)
            containers[category] = how
            by_category[category] = len(rows)

            floor = int(self.min_rows.get(category, 0))
            if len(rows) < floor:
                # A SHORT READ IS NOT A RESULT. Forwarded to the completeness
                # gate through the composite, which only passes warnings from a
                # source declaring `coverage_gaps`.
                gaps.append(f"{category}: {len(rows)} of {floor} expected")
            if not rows:
                empty.append(f"{category} ({url})")
                logger.warning("CBI %s: 0 rows via %s", category, how)
                continue

            for row in rows:
                stamp = self._stamp(row["url"]) if self.fetch_stamps else ("", "")
                if self.fetch_stamps and self.request_delay:
                    time.sleep(self.request_delay)
                doc = self._to_regulatory(row, category, url, stamp)
                if "WEAK" in doc.extra_meta["hash_basis"]:
                    weak.append(f"{doc.title} [{doc.extra_meta['hash_basis']}]")
                docs.append(doc)
            logger.info("CBI %s -- %d row(s) via %s", category, len(rows), how)

        if not docs:
            raise CBIBlocked(
                "CBI: every listing came back empty. The site publishes rows "
                "under Laws and Regulations, so this is a failed read -- see "
                "crawler/cbi_crawler.py §2 for the two shapes a refusal takes.")

        for g in gaps:
            logger.warning("CBI coverage gap -- %s", g)
        if weak:
            # Not fatal: a weak row still belongs in the library. But url|title
            # cannot change, so it reports `unchanged` forever -- including after
            # CBI replaces the file. Said once, loudly, rather than left to look
            # monitored. Same wording as DeclaredDocumentsSource, deliberately.
            logger.warning(
                "CBI: %d document(s) have no server change-stamp, so their "
                "fingerprint cannot see a replaced file: %s",
                len(weak), "; ".join(weak))

        self.last_result = {
            "run": {"blocked_pages": 0,
                    "warnings": [f"coverage gap -- {g}" for g in gaps] + weak},
            "by_source": {self.source_system: len(docs)},
            "by_category": by_category,
            "coverage_gaps": gaps,
            "empty_listings": empty,
            # Which selector fired, per listing. §3: the line that turns a
            # candidate into a fact on the first export that reaches the site.
            "container": containers,
        }
        if limit and limit > 0:
            docs = docs[:limit]
        # The single exit. Every hash above is already set and stamp_ never
        # overwrites one; this is the backstop for a branch added later.
        return stamp_content_hashes(docs)


__all__ = ["CBIListingSource", "CBIBlocked", "REGULATOR", "SITE_URL",
           "SECTION_URL", "SECTION_NAME", "LISTINGS", "MIN_ROWS"]
