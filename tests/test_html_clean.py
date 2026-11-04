"""CHG-001: the stored-HTML allowlist and the three places it is applied.

No network, no database, no OCR stack: `storage.mssql_repo` is driven through
a fake connection that records the SQL parameters, and `ExcelRepo` through an
in-memory table that is never saved.

What is verified here:

  removal       the five bypasses the old regex pass let through (unquoted
                onerror, svg onload, entity-encoded javascript:, iframe, form)
                plus data: urls, target=, comments and script/style bodies
  retention     links, images, tables, lists, headings, inline style/class/id
                -- the content a reviewer must still see
  text kept     a forbidden CONTAINER loses its tag, not its words
  hash safety   on every fixture page the visible text is unchanged, so
                cleaning can never manufacture a `modified`
  idempotence   cleaning clean markup is a no-op
  writers       insert, update and version row, in both repos, store the
                cleaned markup whatever the caller passed

    .venv/Scripts/python.exe -m pytest tests/test_html_clean.py -v
"""
from __future__ import annotations

import glob
import threading
from pathlib import Path

import pytest

from utils.html_clean import clean_html, dropped_in
from scripts.html_clean_scan import visible

FIXTURES = sorted(glob.glob(str(Path(__file__).parent / "fixtures" / "*.html")))

DIRTY = '<p>x</p><script>bad()</script><img src=x onerror=alert(1)>'
CLEAN = '<p>x</p><img src="x">'


# --------------------------------------------------------------------------- #
#  removal                                                                     #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("fragment, gone", [
    ('<img src=x onerror=alert(1)>',                    "onerror"),
    ('<svg onload=alert(1)><circle/></svg>',            "svg"),
    ('<a href="&#106;avascript:alert(1)">x</a>',        "href"),
    ('<a href="javascript:alert(1)">x</a>',             "href"),
    ('<a href="data:text/html,hi">x</a>',               "href"),
    ('<a href="data:image/png;base64,AAAA">x</a>',      "href"),
    ('<img src="data:text/html,hi">',                   "src"),
    ('<img src="data:image/svg+xml;base64,PHN2Zz4=">',  "src"),
    ('<img src=" &#100;ata:text/html,hi">',             "src"),
    ('<a href="https://x" target="_blank">x</a>',       "target"),
    ('<iframe src="https://x"></iframe>',               "iframe"),
    ('<form action="/p"><input name=q></form>',         "form"),
    ('<form action="/p"><input name=q></form>',         "input"),
    ('<button onclick="go()">Go</button>',              "button"),
    ('<object data="x.swf"></object>',                  "object"),
    ('<embed src="x.swf">',                             "embed"),
    ('<meta http-equiv="refresh" content="0;url=x">',   "meta"),
    ('<link rel="stylesheet" href="x.css">',            "link"),
    ('<base href="https://evil/">',                     "base"),
    ('<div onmouseover="x()">t</div>',                  "onmouseover"),
    ('<p>a<!-- hidden <script>x</script> -->b</p>',     "<!--"),
    ('<script>bad()</script><p>t</p>',                  "bad()"),
    ('<style>p{display:none}</style><p>t</p>',          "display:none"),
    ('<noscript>fallback</noscript><p>t</p>',           "fallback"),
])
def test_removed(fragment, gone):
    assert gone not in clean_html(fragment)


# --------------------------------------------------------------------------- #
#  retention                                                                   #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("fragment, kept", [
    ('<a href="https://a.b/c.pdf">pdf</a>',             'href="https://a.b/c.pdf"'),
    ('<a href="/sites/f.pdf">rel</a>',                  'href="/sites/f.pdf"'),
    ('<a href="mailto:x@y.z">m</a>',                    'href="mailto:x@y.z"'),
    ('<a href="#s2">anchor</a>',                        'href="#s2"'),
    ('<img src="https://a/b.png" alt="fig" width="3">', 'alt="fig"'),
    ('<img src="data:image/png;base64,iVBORw0KGgo=">',  'src="data:image/png;base64,iVBORw0KGgo="'),
    ('<img src="data:image/jpeg;base64,/9j/4AAQ">',     'src="data:image/jpeg;base64,/9j/4AAQ"'),
    ('<table border="1"><tr><td colspan="2">c</td></tr></table>', 'colspan="2"'),
    ('<ul><li>i</li></ul><ol start="3"><li>j</li></ol>', 'start="3"'),
    ('<h2 id="s1">H</h2>',                              'id="s1"'),
    ('<p style="color:red" class="note">t</p>',         'style="color:red"'),
    ('<p style="color:red" class="note">t</p>',         'class="note"'),
    ('<span dir="rtl" lang="ar">t</span>',              'dir="rtl"'),
    ('<strong>b</strong><em>i</em><sup>2</sup>',        '<sup>2</sup>'),
    ('<blockquote cite="https://a">q</blockquote>',     'cite="https://a"'),
])
def test_kept(fragment, kept):
    assert kept in clean_html(fragment)


def test_forbidden_container_keeps_its_words():
    out = clean_html('<form action="/p"><input name=q><button>Go</button>'
                     'This notice applies to all licensees.</form>')
    assert "This notice applies to all licensees." in out
    assert "<form" not in out and "<input" not in out


def test_links_get_noopener():
    assert 'rel="noopener noreferrer"' in clean_html('<a href="https://x">x</a>')


# --------------------------------------------------------------------------- #
#  contract                                                                    #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("value", [None, ""])
def test_empty_passes_through_unchanged(value):
    assert clean_html(value) is value


@pytest.mark.parametrize("path", FIXTURES, ids=[Path(p).name for p in FIXTURES])
def test_fixture_visible_text_unchanged(path):
    raw = Path(path).read_text(encoding="utf-8", errors="replace")
    out = clean_html(raw)
    assert visible(out) == visible(raw)
    assert clean_html(out) == out, "not idempotent"


def test_idempotent_on_dirty_input():
    once = clean_html(DIRTY)
    assert once == CLEAN
    assert clean_html(once) == once


# --------------------------------------------------------------------------- #
#  the scan's counter                                                          #
# --------------------------------------------------------------------------- #

def test_dropped_in_counts_what_goes():
    lost = dropped_in(DIRTY)
    assert lost["script"] == 1
    assert lost["img@onerror"] == 1
    assert "img@x" not in lost, "a bare attribute VALUE is not an attribute"
    assert "a@rel" not in dropped_in('<a href="https://x">x</a>')


def test_dropped_in_is_empty_for_clean_markup():
    assert not dropped_in(CLEAN)
    assert not dropped_in(None)


# --------------------------------------------------------------------------- #
#  the writers                                                                 #
# --------------------------------------------------------------------------- #

def _doc(html):
    from models.models import RegulatoryDocument
    return RegulatoryDocument(regulator="Central Bank of Bahrain", source_system="S",
                              category="C", title="T", document_url="https://a/b",
                              published_date="", reference_no="", department="",
                              year=2026, source_page_url="https://a",
                              document_html=html)


def _mssql_with_recorder():
    from storage.mssql_repo import MSSQLRepository
    calls = []

    class Cur:
        def execute(self, q, params=None):
            calls.append((" ".join(q.split()), list(params or [])))

        def fetchone(self):
            return (1,)

    class Conn:
        def cursor(self):
            return Cur()

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

    repo = MSSQLRepository.__new__(MSSQLRepository)
    repo._get_conn = lambda: Conn()
    repo._lock = threading.Lock()
    return repo, calls


def test_mssql_insert_stores_clean_html():
    repo, calls = _mssql_with_recorder()
    repo._insert_regulation(_doc(DIRTY))
    q, params = next(c for c in calls if "INSERT INTO regulations " in c[0])
    assert CLEAN in params and DIRTY not in params


def test_mssql_update_stores_clean_html():
    repo, calls = _mssql_with_recorder()
    repo.update_regulation(5, document_html=DIRTY, title="T")
    q, params = calls[0]
    assert q.startswith("UPDATE regulations SET document_html = ?")
    assert params[0] == CLEAN


def test_mssql_version_stores_clean_html():
    repo, calls = _mssql_with_recorder()
    repo.insert_regulation_version(5, regulator="R", content_html=DIRTY,
                                   content_text="x", content_hash="h")
    q, params = calls[0]
    assert "INSERT INTO regulation_versions" in q
    assert params[2] == CLEAN


def test_excel_repo_stores_clean_html(tmp_path):
    from dynamic_crawler.formfill.excel_repo import ExcelRepo
    repo = ExcelRepo(tmp_path / "t.xlsx")
    rid = repo._insert_regulation(_doc(DIRTY))
    assert repo.get_regulation_by_id(rid)["document_html"] == CLEAN
    repo.update_regulation(rid, document_html=DIRTY)
    assert repo.get_regulation_by_id(rid)["document_html"] == CLEAN
    repo.insert_regulation_version(rid, content_html=DIRTY)
    assert repo.t["regulation_versions"][-1]["content_html"] == CLEAN
