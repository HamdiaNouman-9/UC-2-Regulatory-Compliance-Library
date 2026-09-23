"""CBI's refusals arrive as HTTP 200, and one of them parses clean.

Why these are tests and not a live check: every run against www.cbi.ir costs a
request to a regulator behind F5/Shape, and a refusal there is HTTP 200 with an
HTML body -- so the interesting behaviour is precisely the behaviour a passing
run does NOT exercise. The bodies below are the real ones the host served on
2026-09-23, and tests/fixtures/cbi_*.html are the real pages captured the same
day, so the failure modes stay distinguishable from an empty section when
nobody is watching.

    venv/Scripts/python.exe -m pytest tests/test_cbi_listing.py -q
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from crawler.cbi_crawler import CBIBlocked, CBIListingSource  # noqa: E402

# What the F5 ASM served both a headless AND a headful Chromium, and what
# generic_crawler.crawler recorded as its one page. 121 characters of body text,
# valid HTML, HTTP 200 -- it parses into a listing with no rows, which is the
# whole danger.
REQUEST_REJECTED = (
    "<html><head><title>Request Rejected</title></head><body>"
    "The requested URL was rejected. Please consult with your administrator."
    "Your support ID is: &lt;7880755047531793436&gt;"
    "<a href='javascript:history.back();'>[Go Back]</a></body></html>"
)

# What curl and requests got instead: the TSPD javascript challenge, 40 KB on
# the first request. Its marker, padded past the length guard so the test proves
# the MARKER is what fires and not the size -- a 40 KB refusal sails through any
# length check, which is why there is a marker check at all.
TSPD_CHALLENGE = (
    '<html><head><meta http-equiv="Pragma" content="no-cache"/>'
    '<script type="text/javascript">(function(){'
    'window["bobcmn"] = "1011101010101020000000620000000520000000";'
    + "/*%s*/" % ("0123456789abcdef" * 200)
    + '})();</script></head><body></body></html>'
)

# Cloudflare's, to prove the SHARED detector is still the first thing consulted
# rather than replaced by the local F5 patterns.
CLOUDFLARE_BLOCK = (
    "<html><head><title>Attention Required! | Cloudflare</title></head>"
    "<body><div id='cf-error-details'><h1>Sorry, you have been blocked</h1>"
    "</div>%s</body></html>" % ("<p>padding</p>" * 200)
)

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name):
    """The REAL page, captured from www.cbi.ir on 2026-09-23 through the client
    crawler/cbi_crawler.py §2 describes. An earlier version of this file used
    markup I had invented, and it was wrong about the two things that matter:
    the size is not in the anchor text, and the row declares its own file type
    in an icon's alt. Fixtures are captures now, not guesses."""
    return (FIXTURES / f"cbi_{name}.html").read_text(encoding="utf-8")


LAWS_PAGE = _fixture("laws")
REGULATIONS_PAGE = _fixture("regulations")
CIRCULARS_EMPTY = _fixture("circulars")

PAGES = {"1457.aspx": LAWS_PAGE, "1458.aspx": REGULATIONS_PAGE,
         "1459.aspx": CIRCULARS_EMPTY}


def _source(pages=None, **kw):
    """A source that reads fixtures instead of the site.

    `_pages` IS THE SEAM, and it is stubbed rather than `_render`, because
    `_pages` is where the browser is opened. An earlier version of this file
    stubbed `_render`; a later refactor moved the browser into `_pages`, the
    stub stopped intercepting, and the suite launched a real Chrome per test
    against a live regulator until it was killed. Stub the seam the module
    names, not the method that happened to be underneath it.

    `fetch_stamps=False` by default: a stamp is one request per row, and these
    tests must not touch a network at all.
    """
    pages = PAGES if pages is None else pages
    kw.setdefault("fetch_stamps", False)
    src = CBIListingSource(request_delay=0, **kw)

    def _fake_pages():
        out = []
        for category, list_id in src.listings.items():
            url = src._listing_url(list_id)
            html = pages[f"{list_id}.aspx"]
            src._judge(url, html, "", html)   # the same judge a live fetch runs
            out.append((category, list_id, html))
        return out

    src._pages = _fake_pages
    return src


# --------------------------------------------------------------------------- #
#  the refusals                                                                 #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("body,says", [
    (REQUEST_REJECTED, "F5 refusal"),
    (TSPD_CHALLENGE, "F5 refusal"),
    (CLOUDFLARE_BLOCK, "bot-protection wall"),
])
def test_a_refusal_raises_rather_than_reading_as_empty(body, says):
    src = CBIListingSource(request_delay=0)
    with pytest.raises(CBIBlocked) as e:
        src._judge("https://www.cbi.ir/simplelist/1457.aspx", body, "", body)
    assert says in str(e.value)


def test_the_shared_blockcheck_is_consulted_first():
    """Cloudflare wording must still be diagnosed by the ONE shared definition
    (generic_crawler/blockcheck.py), not by this module's local F5 patterns."""
    src = CBIListingSource(request_delay=0)
    with pytest.raises(CBIBlocked) as e:
        src._judge("https://www.cbi.ir/x", CLOUDFLARE_BLOCK, "", CLOUDFLARE_BLOCK)
    assert "F5" not in str(e.value)


def test_the_block_page_would_otherwise_parse_as_an_empty_listing():
    """The point of the guard, stated as a test: the block page is valid HTML
    and yields zero rows, which is indistinguishable from an empty section."""
    src = CBIListingSource(request_delay=0)
    rows, _ = src._rows(REQUEST_REJECTED, "https://www.cbi.ir/simplelist/1457.aspx")
    assert rows == []


def test_a_truncated_body_is_a_refusal_not_a_page():
    src = CBIListingSource(request_delay=0)
    with pytest.raises(CBIBlocked):
        src._judge("https://www.cbi.ir/simplelist/1457.aspx", "<html></html>")


# --------------------------------------------------------------------------- #
#  reading the rows                                                             #
# --------------------------------------------------------------------------- #

def test_sidebar_page_links_are_not_legislation():
    """Executive Board, Treasury of National Jewels and Library are
    /page/<digits>.aspx too. This is the ZATCA failure the module header cites:
    a rule broad enough to catch the laws files the org chart as law."""
    src = CBIListingSource(request_delay=0)
    rows, how = src._rows(LAWS_PAGE, "https://www.cbi.ir/simplelist/1457.aspx")
    titles = [r["title"] for r in rows]
    assert len(rows) == 5, how
    for chrome in ("Executive Board", "The Treasury of National Jewels", "Library"):
        assert chrome not in titles


def test_contact_en_is_excluded_by_the_numeric_id_rule():
    """/page/Contact_en.aspx must not match: the id being numeric is what
    separates an instrument from a hand-named sidebar page."""
    from crawler.cbi_crawler import _ROW_HREF
    assert not _ROW_HREF.search("/page/Contact_en.aspx")
    assert _ROW_HREF.search("/page/2234.aspx").group(1) == "2234"


@pytest.mark.parametrize("raw,title,size", [
    ("The Monetary and Banking Act - 781 KB", "The Monetary and Banking Act", "781 KB"),
    ("Anti-Money Laundering Law - 19 KB", "Anti-Money Laundering Law", "19 KB"),
    ("The Law for Usury (Interest) Free Banking (105 KB)",
     "The Law for Usury (Interest) Free Banking", "105 KB"),
    ("The Banking and Monetary Act (1339) 117 KB (PDF)",
     "The Banking and Monetary Act (1339)", "117 KB"),
])
def test_the_size_leaves_the_title_and_survives_for_the_fingerprint(raw, title, size):
    """The size is stripped from the title and read out for the fingerprint --
    it is the only field on the row that moves when CBI replaces a PDF behind an
    unchanged url."""
    from crawler.cbi_crawler import _split_row_text
    assert _split_row_text(raw) == (title, size)


def test_the_one_shape_the_shared_rule_does_not_strip():
    """MEASURED against `clean_doc_title`, not assumed. Its `_FORMAT_TAIL` needs
    a real separator ("| PDF", "- PDF") and a bare space is not one -- which is
    deliberate, so "Guide to PDF" survives -- and the unmatched "(PDF)" then
    blocks `_SIZE_TAIL` behind it. That is why the local strip runs first
    instead of the shared regex being widened."""
    from crawler.cbi_crawler import _split_row_text
    from generic_crawler.crawler import clean_doc_title
    raw = "The Banking and Monetary Act (1339) 117 KB (PDF)"
    assert clean_doc_title(raw) == raw                      # shared rule: no-op
    assert _split_row_text(raw) == ("The Banking and Monetary Act (1339)", "117 KB")


def test_a_title_that_ends_in_a_number_is_not_mistaken_for_a_size():
    """A unit word is required, so a year or a decree number survives."""
    from crawler.cbi_crawler import _split_row_text
    for t in ("The Banking and Monetary Act (1339)", "Decision No. 12", "Report 2024"):
        assert _split_row_text(t) == (t, "")


def test_a_row_becomes_a_document_the_library_can_place():
    docs = _source().fetch_documents()
    laws = [d for d in docs if d.category == "Laws"]
    act = next(d for d in laws if d.title == "The Monetary and Banking Act")

    assert act.regulator == "Central Bank of Iran (CBI)"
    assert act.source_system == "Laws and Regulations"
    assert act.doc_path == ["Central Bank of Iran (CBI)", "Laws and Regulations",
                            "Laws", "The Monetary and Banking Act"]
    assert act.document_url == "https://www.cbi.ir/page/2234.aspx"
    assert act.source_page_url == "https://www.cbi.ir/simplelist/1457.aspx"
    # /page/2234.aspx SERVES A PDF. From the row, never the extension -- this is
    # the field `mode: declared` gets wrong and the reason for this whole file.
    assert act.file_type == "PDF"
    assert act.published_date is None
    assert act.extra_meta["cbi_page_id"] == "2234"
    assert act.extra_meta["file_size_text"] == "781 KB"
    assert act.content_hash


def test_every_document_carries_a_fingerprint_and_it_is_stable():
    first = _source().fetch_documents()
    second = _source().fetch_documents()
    assert all(d.content_hash for d in first)
    assert [d.content_hash for d in first] == [d.content_hash for d in second], \
        "a hash that moves between runs reports every document modified, forever"


def test_a_replaced_pdf_moves_the_fingerprint():
    src = CBIListingSource(request_delay=0, fetch_stamps=False)
    row = {"url": "https://www.cbi.ir/page/2234.aspx",
           "title": "The Monetary and Banking Act", "size": "781 KB"}
    before = src._to_regulatory(row, "Laws", "u", ("", ""))
    after = src._to_regulatory({**row, "size": "802 KB"}, "Laws", "u", ("", ""))
    assert before.content_hash != after.content_hash


def test_a_server_stamp_beats_url_title_and_is_recorded():
    src = CBIListingSource(request_delay=0, fetch_stamps=False)
    row = {"url": "https://www.cbi.ir/page/2234.aspx",
           "title": "The Monetary and Banking Act", "size": "781 KB"}
    weak = src._to_regulatory(row, "Laws", "u", ("", ""))
    strong = src._to_regulatory(row, "Laws", "u", ("etag:abc123", "etag"))
    assert weak.content_hash != strong.content_hash
    assert strong.extra_meta["hash_basis"] == "etag"
    assert "WEAK" not in weak.extra_meta["hash_basis"]   # it has a size


def test_identities_are_distinct_across_all_three_categories():
    """`check` refuses a workbook whose rows share an identity -- they overwrite
    each other on insert and the library gains fewer rows than the workbook
    shows."""
    docs = _source().fetch_documents()
    ids = [(d.document_url, tuple(d.doc_path), d.title) for d in docs]
    assert len(set(ids)) == len(ids)


# --------------------------------------------------------------------------- #
#  empty listings and short reads                                               #
# --------------------------------------------------------------------------- #

def test_empty_circulars_is_reported_not_raised():
    """CBI publishes nothing under Circulars today. The listing is still read,
    and the emptiness has to be visible in the run."""
    src = _source()
    docs = src.fetch_documents()
    assert src.last_result["by_category"] == {"Laws": 5, "Regulations": 6,
                                              "Circulars": 0}
    # THE CONTAINER IS PRESENT AND EMPTY -- that is CBI's answer, and it is the
    # only thing separating an empty section from a failed read once `_judge`
    # has passed. An empty bs4 Tag is falsy, so this was once reported as
    # "fallback:whole-document" for a page whose <ul> was right there.
    assert "no rows" in src.last_result["container"]["Circulars"]
    assert "simplelist" in src.last_result["container"]["Circulars"]
    assert any("Circulars" in e for e in src.last_result["empty_listings"])
    assert not any(d.category == "Circulars" for d in docs)
    # Which selector fired, per listing -- the line that turns the module's
    # candidate selectors into a fact on the first export that reaches the site.
    assert set(src.last_result["container"]) == {"Laws", "Regulations", "Circulars"}


def test_a_short_read_is_a_coverage_gap_the_composite_can_see():
    """Under the measured floor is not a result. The composite forwards
    `coverage_gaps` to the completeness gate, and only from a source that
    declares them.

    The floor is raised above what the site currently serves rather than the
    fixture being trimmed: the fixtures are captures and stay untouched.
    """
    src = _source(min_rows={"Laws": 5, "Regulations": 9, "Circulars": 0})
    src.fetch_documents()
    gaps = src.last_result["coverage_gaps"]
    assert any("Regulations: 6 of 9" in g for g in gaps), gaps
    assert any("coverage gap" in w.lower()
               for w in src.last_result["run"]["warnings"])


def test_no_gap_when_every_listing_meets_its_floor():
    """The floors that ship in config/sources/cbi.yml, against the real pages."""
    src = _source(min_rows={"Laws": 5, "Regulations": 6, "Circulars": 0})
    src.fetch_documents()
    assert src.last_result["coverage_gaps"] == []


def test_every_listing_empty_is_a_failed_read():
    """Three empty listings is not a section CBI emptied overnight. It is §2."""
    with pytest.raises(CBIBlocked):
        _source({"1457.aspx": CIRCULARS_EMPTY, "1458.aspx": CIRCULARS_EMPTY,
                 "1459.aspx": CIRCULARS_EMPTY}).fetch_documents()


def test_a_pinned_content_selector_that_misses_raises():
    """A pinned selector matching nothing is the site changing shape, and must
    not fall through to the structural guess."""
    src = _source(content_selector="#never-going-to-match")
    with pytest.raises(CBIBlocked) as e:
        src.fetch_documents()
    assert "PINNED" in str(e.value)


# --------------------------------------------------------------------------- #
#  the stamp guard                                                              #
# --------------------------------------------------------------------------- #

def test_a_stamp_is_refused_when_the_response_is_not_the_file():
    """THE REASON `_stamp` IS COPIED FROM stamp_declared RATHER THAN CALLED.

    This host answers a refused request with HTTP 200 and a full set of headers,
    so an ETag can be read off the CHALLENGE. Those rotate per request, which is
    the "hash that changes on its own" crawler/fingerprint.py calls worse than
    no hash at all.
    """
    import crawler.cbi_crawler as m

    class _Resp:
        status_code = 200
        headers = {"Content-Type": "text/html; charset=utf-8",
                   "ETag": "rotates-every-request"}
        def iter_content(self, n):
            yield TSPD_CHALLENGE.encode()
        def close(self):
            pass

    class _Requests:
        @staticmethod
        def head(*a, **k):
            return _Resp()
        @staticmethod
        def get(*a, **k):
            return _Resp()

    real = __import__("requests")
    sys.modules["requests"] = _Requests
    try:
        src = CBIListingSource(request_delay=0)
        assert src._stamp("https://www.cbi.ir/page/2234.aspx") == ("", "")
    finally:
        sys.modules["requests"] = real


def test_a_stamp_is_taken_when_the_response_really_is_the_pdf():
    class _Resp:
        status_code = 200
        headers = {"Content-Type": "application/pdf", "ETag": '"abc123"'}
        def iter_content(self, n):
            yield b"%PDF-1.5"
        def close(self):
            pass

    class _Requests:
        @staticmethod
        def head(*a, **k):
            return _Resp()
        @staticmethod
        def get(*a, **k):
            return _Resp()

    real = __import__("requests")
    sys.modules["requests"] = _Requests
    try:
        src = CBIListingSource(request_delay=0)
        assert src._stamp("https://www.cbi.ir/page/2234.aspx") == \
            ("etag:abc123", "etag")
    finally:
        sys.modules["requests"] = real


# --------------------------------------------------------------------------- #
#  what the real markup taught, and the invented fixture did not               #
# --------------------------------------------------------------------------- #

def test_the_size_comes_from_the_row_not_the_anchor():
    """<a>title</a><span dir="ltr">781 KB -</span>.

    The size is a SIBLING of the link. Read off the anchor it is "" for every
    row, which silently downgrades every fingerprint to url|title -- the exact
    thing `hash_basis` exists to make visible. This is the test that would have
    caught it; the invented fixture put the size inside the anchor and so could
    not.
    """
    src = CBIListingSource(request_delay=0)
    rows, _ = src._rows(LAWS_PAGE, "https://www.cbi.ir/simplelist/1457.aspx")
    by_title = {r["title"]: r for r in rows}
    assert by_title["The Monetary and Banking Act"]["size"] == "781 KB"
    assert by_title["Anti-Money Laundering Law"]["size"] == "19 KB"
    assert all(r["size"] for r in rows), \
        "a row with no size falls back to a url|title fingerprint"


def test_the_file_type_comes_from_the_row_s_own_icon():
    """<img alt="PDF icon">. The SITE declares the type; the url says ASPX and a
    hardcoded "PDF" would mislabel the first DOC CBI publishes."""
    src = CBIListingSource(request_delay=0)
    rows, _ = src._rows(LAWS_PAGE, "https://www.cbi.ir/simplelist/1457.aspx")
    assert {r["file_type"] for r in rows} == {"PDF"}


def test_a_doc_row_would_arrive_as_a_doc():
    """The icon is read, not assumed -- so a non-PDF row is not mislabelled."""
    from bs4 import BeautifulSoup
    src = CBIListingSource(request_delay=0)
    li = BeautifulSoup(
        '<li><a href="/page/999.aspx">Something</a>'
        '<span dir="ltr">12 KB -</span>'
        '<img alt="DOC icon" src="/Images/icon_doc.gif"/></li>',
        "html.parser").find("li")
    assert src._file_type(li) == "DOC"


def test_the_real_eleven_are_all_there_with_the_ids_the_config_records():
    """config/sources/cbi.yml §2 lists every page id. This pins the config to
    the capture: if CBI renumbers, one of the two is wrong and this says so."""
    docs = _source().fetch_documents()
    got = {(d.category, d.extra_meta["cbi_page_id"]) for d in docs}
    assert len(docs) == 11
    for pid in ("2234", "5320", "2235", "5298", "2560"):
        assert ("Laws", pid) in got, pid
    for pid in ("2483", "17538", "17545", "17546", "17547", "17548"):
        assert ("Regulations", pid) in got, pid


# --------------------------------------------------------------------------- #
#  a second section: Prudential Regulations                                    #
# --------------------------------------------------------------------------- #

PRUDENTIAL_PAGE = _fixture("prudential")


def _prudential(**kw):
    """The second source_system, wired as config/sources/cbi.yml wires it."""
    kw.setdefault("fetch_stamps", False)
    src = CBIListingSource(
        request_delay=0,
        source_system="Prudential Regulations",
        doc_path_prefix=["Central Bank of Iran (CBI)", "Prudential Regulations"],
        listings={"Prudential Regulations": 1463},
        min_rows={"Prudential Regulations": 15},
        content_selector="ul.simplelist",
        **kw)

    def _fake_pages():
        url = src._listing_url(1463)
        src._judge(url, PRUDENTIAL_PAGE, "", PRUDENTIAL_PAGE)
        return [("Prudential Regulations", 1463, PRUDENTIAL_PAGE)]

    src._pages = _fake_pages
    return src


def test_a_single_listing_section_does_not_say_its_name_twice():
    """THE POINT OF `_clean_trail` HERE.

    Prudential Regulations is one listing, so its category repeats its
    source_system. Without the dedupe the trail would read

        CBI | Prudential Regulations | Prudential Regulations | <title>

    and the library would draw a folder inside a folder of the same name.
    """
    docs = _prudential().fetch_documents()
    assert len(docs) == 15
    d = next(x for x in docs if x.title == "Electronic Banking System By-law")
    assert d.doc_path == ["Central Bank of Iran (CBI)", "Prudential Regulations",
                          "Electronic Banking System By-law"]
    assert all(len(x.doc_path) == 3 for x in docs)


def test_the_multi_listing_section_still_keeps_its_category_folder():
    """The dedupe must not flatten Laws and Regulations, whose categories are
    real folders that differ from the source_system."""
    docs = _source().fetch_documents()
    assert all(len(d.doc_path) == 4 for d in docs)
    assert docs[0].doc_path[1:3] == ["Laws and Regulations", "Laws"]


def test_the_title_survives_even_if_it_repeats_a_folder():
    """The leaf IS the document, so it is appended AFTER the dedupe -- the same
    rule DeclaredDocumentsSource states. A document named after its own section
    must still get a node of its own."""
    src = CBIListingSource(request_delay=0, source_system="Prudential Regulations",
                           doc_path_prefix=["Central Bank of Iran (CBI)",
                                            "Prudential Regulations"])
    d = src._to_regulatory(
        {"url": "https://www.cbi.ir/page/1.aspx", "title": "Prudential Regulations",
         "size": "1 KB", "file_type": "PDF"},
        "Prudential Regulations", "u", ("", ""))
    assert d.doc_path == ["Central Bank of Iran (CBI)", "Prudential Regulations",
                          "Prudential Regulations"]


def test_the_two_sections_share_no_identity():
    """Near-duplicate titles across the two sections (15114/2483, 15115/17545)
    are different instruments. Identity must separate them."""
    docs = _source().fetch_documents() + _prudential().fetch_documents()
    ids = [(d.document_url, tuple(d.doc_path), d.title) for d in docs]
    assert len(set(ids)) == len(ids) == 26


def test_the_site_trail_is_stored_raw_even_though_the_tree_differs():
    """CBI files this under Bank Supervision; the library files it under the
    regulator. The workbook reports what the SITE shows in `section_path`."""
    d = _prudential().fetch_documents()[0]
    assert d.extra_meta["section_path"].startswith("Home > ")
    assert d.source_system == "Prudential Regulations"
