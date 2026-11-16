"""CHG-009 step 4: the refusal policy, end to end, against a site on localhost.

No regulator is visited. A stub HTTP server plays the site: a listing page of
four rows, four document pages, and a scripted misbehaviour on one of them --
a persistent 403, a single 429 with Retry-After, a Cloudflare-style wall. The
real runner drives a real headless browser at it, so what is tested is the
behaviour the design doc promises, not a model of it:

  * the first refusal earns one long pause and ONE retry of that URL;
  * a second refusal ends the run, the host's block memory advances, and every row
    not opened is recorded as detail_skipped rather than as emptied;
  * a refusal that clears on the retry is recovered and the run completes;
  * a host inside its backoff window is refused before a single request;
  * a clean run resets the block memory;
  * pages.json carries a `run` block the orchestrator's gate reads.

    .venv/Scripts/python.exe -m pytest tests/test_formfill_refusal_server.py -q
"""

import json
import sys
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytest.importorskip("playwright.sync_api")

# `tests/test_change_signal.py` and `tests/test_snapshot_articles.py` install
# module-shaped stand-ins for paddle, torch, fitz and friends in sys.modules at
# import time, and a Playwright launch in the same process then dies inside
# torch's patched `inspect.getfile` ("<module 'paddle' ...> is a built-in
# module"). Every browser-backed formfill test on `main` errors in the full
# suite for this reason (29 errors measured 2026-11-10); this file says so and
# steps aside rather than adding thirteen more. Run it on its own.
# A stand-in is a real ModuleType whose every attribute, `__file__` included,
# resolves to a placeholder object; a genuine module's `__file__` is a str or
# absent.
_STUBBED = sorted(n for n, m in sys.modules.items()
                  if n.split(".")[0] in ("paddle", "torch", "fitz", "paddleocr")
                  and not isinstance(getattr(m, "__file__", None), (str, type(None))))
if _STUBBED:
    pytest.skip(f"browser tests cannot launch beside stubbed modules {_STUBBED[:3]}; "
                "run this file on its own", allow_module_level=True)

from dynamic_crawler.formfill import access, runner  # noqa: E402
from utils import pacer  # noqa: E402

PROSE = ("Article 1. The following words and phrases, wherever mentioned in this Law, "
         "shall have the meanings assigned thereto unless the context requires otherwise. "
         "Article 2. The Authority shall supervise the application of this Law. ") * 4

WALL = """<html><head><title>Attention Required! | Cloudflare</title></head><body>
<div id="cf-error-details"><h1>Sorry, you have been blocked</h1>
<p>You are unable to access this site. This website is using a security service to
protect itself from online attacks.</p></div></body></html>"""


def _listing(n: int) -> str:
    rows = "".join(f'<div class="row"><a href="/doc{i}">Document {i}</a></div>' for i in range(1, n + 1))
    return f"<html><head><title>Laws</title></head><body><h1>Laws</h1>{rows}<p>{PROSE}</p></body></html>"


def _doc(i: int) -> str:
    return (f"<html><head><title>Document {i}</title></head><body><main><h1>Document {i}</h1>"
            f"<p>{PROSE}</p></main></body></html>")


class _Site:
    """Scripted answers per path. `script[path]` is a list of (status, headers,
    body); each request pops the next entry and the last one repeats."""

    def __init__(self, n_docs=4):
        self.script = {"/": [(200, {}, _listing(n_docs))]}
        for i in range(1, n_docs + 1):
            self.script[f"/doc{i}"] = [(200, {}, _doc(i))]
        self.hits = []
        self.requests = []        # (path, headers) per request
        self.cleared = 0          # times the scripted challenge cleared
        self.lock = threading.Lock()

    def answer(self, path, headers=None):
        if path == "/favicon.ico":           # the full browser asks; not a page
            return 404, {}, ""
        if path == "/clear":                 # the scripted challenge page reports in
            with self.lock:
                self.cleared += 1
                self.script["/"] = [(200, {}, _listing(4))]
            return 204, {}, ""
        with self.lock:
            self.hits.append(path)
            self.requests.append((path, dict(headers or {})))
            steps = self.script.get(path)
            if not steps:
                return 404, {}, "<html><body>not found</body></html>"
            step = steps.pop(0) if len(steps) > 1 else steps[0]
            return step


def _handler(site: _Site):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            status, headers, body = site.answer(self.path.split("?")[0],
                                                {k.lower(): v for k, v in self.headers.items()})
            data = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            for k, v in headers.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *a):          # keep pytest output clean
            pass
    return H


@pytest.fixture
def site():
    s = _Site()
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _handler(s))
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    s.base = f"http://127.0.0.1:{srv.server_address[1]}"
    yield s
    srv.shutdown()
    srv.server_close()


@pytest.fixture(autouse=True)
def fast(monkeypatch, tmp_path):
    """Milliseconds where production has seconds, and the block memory in tmp."""
    monkeypatch.setattr(access, "RETRY_PAUSE_MS", (40, 80))
    monkeypatch.setattr(access, "CHALLENGE_WAIT_MS", 2_000)
    monkeypatch.setattr(access, "ACCESS_DIR", tmp_path / "access")
    monkeypatch.setattr(access, "PROFILE_DIR", tmp_path / "profiles")
    monkeypatch.setattr(access, "pace_for", lambda url, hints=None, config=None: access.Pace(
        host=access.host_of(url), level="test", multiplier=1.0, base_ms=(5, 15)))
    pacer.reload()
    pacer.reset_run()
    yield
    pacer.reset_run()


def _hints(base):
    return {"version": 1, "name": "stub", "seed_url": base + "/", "shape": "list",
            "scope": "prefix", "row_selector": "div.row",
            "pagination": {"mode": "none", "max_pages": 1}, "fields": {}}


def _run(site, tmp_path):
    out = tmp_path / "out"
    summary = runner.run(_hints(site.base), out, wait_ms=50, write_excel=False)
    pages = json.loads((out / "pages.json").read_text(encoding="utf-8"))
    return summary, pages


def _manifest(site):
    return access.AccessState(access.host_of(site.base)).manifest()


# --------------------------------------------------------------------------- #

def test_a_clean_run_reads_everything_and_resets_the_block_memory(site, tmp_path):
    summary, pages = _run(site, tmp_path)
    assert summary["rows"] == 4 and summary["blocked_pages"] == 0
    # One navigation per page. Until CHG-009 every detail page was fetched
    # twice (a first load whose result the second one overwrote).
    assert site.hits == ["/", "/doc1", "/doc2", "/doc3", "/doc4"]
    assert summary["stopped"] == "" and summary["refused"] is False
    assert sum(1 for r in pages["pages"] if r.get("text_len", 0) > 200) == 4
    assert pages["run"] == {"blocked_pages": 0, "warnings": summary["warnings"],
                            "refused": False, "refused_reason": "", "stopped": "",
                            "source": "live", "access": summary["access"]}
    assert summary["access"]["blocked_cycles"] == 0 and summary["access"]["cycle_attempt"] == 0
    m = _manifest(site)
    assert m["last_attempt_result"] == "ok" and m["consecutive_blocks"] == 0


def test_a_persistent_403_gets_one_retry_then_stops_the_run(site, tmp_path):
    site.script["/doc2"] = [(403, {}, "")]
    summary, pages = _run(site, tmp_path)

    # One navigation, one retry, nothing after.
    assert site.hits.count("/doc2") == 2
    assert "/doc3" not in site.hits and "/doc4" not in site.hits

    assert summary["stopped"].startswith(f"blocked at {site.base}/doc2")
    assert summary["blocked_pages"] == 1
    assert summary["access"]["refusals"] == 2 and summary["access"]["recovered"] == 0
    assert summary["access"]["consecutive_blocks"] == 1
    assert summary["access"]["next_attempt_after"]

    # doc1 was read; doc2..doc4 are recorded as not opened, never as emptied.
    by_title = {r["title"]: r for r in pages["pages"]}
    assert by_title["Document 1"]["text_len"] > 200
    for t in ("Document 2", "Document 3", "Document 4"):
        assert by_title[t].get("detail_skipped") is True
        assert by_title[t].get("content_hash", "") == ""
    assert summary["detail_skipped"] == 3

    assert pages["run"]["blocked_pages"] == 1 and pages["run"]["stopped"]
    assert any("REFUSED at" in w for w in summary["warnings"])
    assert any("STOPPED BY BOT PROTECTION" in w for w in summary["warnings"])
    assert _manifest(site)["consecutive_blocks"] == 1


def test_an_akamai_style_403_with_a_body_is_a_refusal_too(site, tmp_path):
    """The bare 403 above makes Chromium throw; Akamai's carries a page and
    renders. Both must reach the same verdict."""
    site.script["/doc2"] = [(403, {}, "<html><head><title>Access Denied</title></head>"
                                       "<body><h1>Access Denied</h1><p>You don't have "
                                       "permission to access this resource. Reference "
                                       "#18.4f2c1</p></body></html>")]
    summary, _ = _run(site, tmp_path)
    assert site.hits.count("/doc2") == 2 and "/doc3" not in site.hits
    assert summary["stopped"].startswith(f"blocked at {site.base}/doc2")
    assert summary["access"]["refusals"] == 2


def test_a_429_that_clears_on_the_retry_is_recovered(site, tmp_path):
    site.script["/doc3"] = [(429, {"Retry-After": "1"}, ""), (200, {}, _doc(3))]
    summary, pages = _run(site, tmp_path)
    assert site.hits.count("/doc3") == 2
    assert summary["stopped"] == "" and summary["rows"] == 4
    assert summary["access"]["refusals"] == 1 and summary["access"]["recovered"] == 1
    assert summary["blocked_pages"] == 0
    assert sum(1 for r in pages["pages"] if r.get("text_len", 0) > 200) == 4
    assert _manifest(site)["consecutive_blocks"] == 0


def test_a_second_refusal_after_a_recovery_stops_without_another_retry(site, tmp_path):
    site.script["/doc2"] = [(429, {"Retry-After": "1"}, ""), (200, {}, _doc(2))]
    site.script["/doc4"] = [(403, {}, "")]
    summary, _ = _run(site, tmp_path)
    assert site.hits.count("/doc2") == 2
    assert site.hits.count("/doc4") == 1, "the one retry was spent on doc2"
    assert summary["stopped"].startswith(f"blocked at {site.base}/doc4")
    assert summary["access"]["refusals"] == 2 and summary["access"]["recovered"] == 1


def test_a_wall_on_the_seed_stops_before_any_row(site, tmp_path):
    site.script["/"] = [(200, {}, WALL)]
    summary, pages = _run(site, tmp_path)
    assert site.hits == ["/", "/"], "one load, one retry, nothing else"
    assert summary["rows"] == 0 and summary["blocked_pages"] == 1
    assert summary["stopped"].startswith(f"blocked at {site.base}/")
    assert pages["pages"] == [] and pages["run"]["stopped"]
    assert _manifest(site)["consecutive_blocks"] == 1


def test_inside_the_backoff_window_nothing_is_requested(site, tmp_path):
    st = access.AccessState(access.host_of(site.base))
    st.record_block("http 403")
    summary, pages = _run(site, tmp_path)
    assert site.hits == []
    assert summary["refused"] is True and "next attempt after" in summary["refused_reason"]
    assert summary["rows"] == 0 and summary["stopped"] == ""
    assert pages["pages"] == [] and pages["run"]["refused"] is True
    assert (tmp_path / "out" / "run.json").exists()
    assert _manifest(site)["consecutive_blocks"] == 1, "a refused run does not count as a block"


def _expire(site):
    st = access.AccessState(access.host_of(site.base))
    m = st.manifest()
    m["next_attempt_after"] = access._iso(access._now() - timedelta(minutes=1))
    st.path.write_text(json.dumps(m), encoding="utf-8")


def test_the_cycles_re_attempt_is_one_load_and_closes_the_cycle(site, tmp_path):
    """Attempt 2 of a cycle (6 h after the first blocked attempt): no in-run
    retry, the first refusal ends the run, the cycle is closed as blocked and
    no window is opened, so the next scheduled run goes ahead."""
    st = access.AccessState(access.host_of(site.base))
    st.record_block("http 403")
    _expire(site)
    site.script["/"] = [(403, {}, "")]
    summary, pages = _run(site, tmp_path)
    assert site.hits == ["/"], "one load, no retry, nothing else"
    assert summary["stopped"].startswith(f"blocked at {site.base}/")
    assert summary["access"]["refusals"] == 1 and summary["access"]["recovered"] == 0
    assert any("on the cycle's re-attempt" in w and "Cycle closed as blocked (1 in a row)" in w
               for w in summary["warnings"])
    m = _manifest(site)
    assert m["blocked_cycles"] == 1 and m["cycle_attempt"] == 0 and m["next_attempt_after"] is None
    assert pages["run"]["access"]["blocked_cycles"] == 1
    assert st.may_attempt()[0]


def test_two_blocked_cycles_are_carried_on_the_run_block(site, tmp_path):
    """Cycle 1 blocked on both attempts, cycle 2 the same: the run block the
    orchestrator and the run store read says blocked_cycles 2, which is the
    alert's trigger. A skipped run inside the window carries the count too."""
    site.script["/"] = [(403, {}, "")]
    _run(site, tmp_path / "c1a")                        # cycle 1, attempt 1 (+ in-run retry)
    _expire(site)
    _run(site, tmp_path / "c1b")                        # cycle 1, attempt 2
    assert _manifest(site)["blocked_cycles"] == 1
    summary, pages = _run(site, tmp_path / "c2a")       # cycle 2, attempt 1
    assert summary["access"]["blocked_cycles"] == 1 and summary["access"]["cycle_attempt"] == 1
    skipped, spages = _run(site, tmp_path / "c2skip")   # inside the window: refused before start
    assert skipped["refused"] is True and spages["run"]["access"]["blocked_cycles"] == 1
    _expire(site)
    summary, pages = _run(site, tmp_path / "c2b")       # cycle 2, attempt 2
    assert summary["access"]["blocked_cycles"] == 2
    assert pages["run"]["access"]["blocked_cycles"] == 2
    assert pages["run"]["access"]["first_blocked_at"]
    assert pages["run"]["access"]["cycle_attempt"] == 0, "the closing attempt, where the alert fires"
    assert site.hits.count("/") == 6, "2 + 1 + 2 + 0 (skipped) + 1 loads over two cycles"


def test_a_listing_cut_short_on_a_later_page_records_each_row_once(site, tmp_path):
    """Page 1 harvests four rows, page 2 is refused: the run stops in phase 1,
    no detail page is opened, and every row is recorded once as not opened.
    Before the review fix of step 10 the rows were recorded twice, once as a
    record with a hash and once as detail_skipped."""
    site.script["/p2"] = [(403, {}, "")]
    hints = dict(_hints(site.base), pagination={"mode": "url_page", "pattern": site.base + "/p{page}",
                                                "max_offset": 2, "step": 2, "max_pages": 5})
    out = tmp_path / "out"
    summary = runner.run(hints, out, wait_ms=50, write_excel=False)
    pages = json.loads((out / "pages.json").read_text(encoding="utf-8"))
    assert site.hits == ["/", "/p2", "/p2"], "page 1, page 2, its one retry, no detail page"
    assert summary["rows"] == 4 and summary["stopped"].startswith(f"blocked at {site.base}/p2")
    assert summary["records"] == 4 and summary["detail_skipped"] == 4
    assert sorted(r["title"] for r in pages["pages"]) == [f"Document {i}" for i in range(1, 5)]
    assert all(r.get("detail_skipped") is True and r.get("content_hash", "") == "" for r in pages["pages"])


def test_an_expired_window_lets_the_run_go_and_a_clean_run_resets(site, tmp_path):
    st = access.AccessState(access.host_of(site.base))
    m = st.record_block("http 403")
    m["next_attempt_after"] = access._iso(access._now() - timedelta(minutes=1))
    st.path.write_text(json.dumps(m), encoding="utf-8")
    summary, _ = _run(site, tmp_path)
    assert summary["refused"] is False and summary["rows"] == 4
    assert _manifest(site)["consecutive_blocks"] == 0


def test_a_snapshot_replay_makes_no_request_at_all(site, tmp_path):
    """The replay exists to keep a blocked host offline. Before CHG-009 the
    duplicate detail load went live for every row of a replayed form."""
    snap = tmp_path / "s.html"
    snap.write_text(_listing(2), encoding="utf-8")
    hints = _hints(site.base)
    hints["include_page"] = True            # the seed itself is a row, as on SIMAH
    out = tmp_path / "out"
    summary = runner.run(hints, out, wait_ms=50, write_excel=False, snapshot=snap)
    assert summary["source"] == "snapshot" and summary["refused"] is False
    assert site.hits == [], f"a replay touched the network: {site.hits}"
    assert summary["access"] is None and summary["pace"] is None
    assert not (tmp_path / "profiles").exists(), "a replay needs no profile"
    assert summary["browser"]["profile"] == ""


# --------------------------------------------------------------------------- #
# CHG-009 step 5: what the site sees
# --------------------------------------------------------------------------- #

def test_the_site_sees_a_visitor_not_a_headless_browser(site, tmp_path):
    summary, _ = _run(site, tmp_path)
    ua_seen = {h.get("user-agent", "") for _, h in site.requests}
    assert len(ua_seen) == 1
    ua = ua_seen.pop()
    assert "Chrome/" in ua and "HeadlessChrome" not in ua
    assert ua == summary["browser"]["user_agent"]
    langs = {h.get("accept-language", "") for _, h in site.requests}
    assert langs == {"en-US"}, "the form's locale, as the browser sends it"
    b = summary["browser"]
    assert b["viewport"] == {"width": 1280, "height": 720}, "the width the forms were verified at"
    assert b["channel"] in ("chrome", "chromium", "default") and b["headed_retry"] is False
    assert b["scripted"] is False and b["script_retry"] is False, \
        "a host that never challenges never gets the visitor script"
    assert Path(b["profile"]) == tmp_path / "profiles" / "127.0.0.1"
    assert (tmp_path / "profiles" / "127.0.0.1").is_dir()


def test_the_forms_locale_sets_the_language_header(site, tmp_path):
    hints = _hints(site.base)
    hints["locale"] = "ar-SA"
    runner.run(hints, tmp_path / "out", wait_ms=50, write_excel=False)
    langs = {h.get("accept-language", "") for _, h in site.requests}
    assert langs == {"ar-SA"}


def test_cookies_survive_between_runs_on_the_same_host(site, tmp_path):
    """The point of the profile: a clearance a site grants once is kept. A
    clearance cookie carries an expiry (cf_clearance lasts 30 min to a year);
    a SESSION cookie is dropped between launches by Chromium itself, which is
    correct and not what this tests."""
    site.script["/"] = [(200, {"Set-Cookie": "cleared=yes; Path=/; Max-Age=3600"}, _listing(4))]
    runner.run(_hints(site.base), tmp_path / "out1", wait_ms=50, write_excel=False)
    first = [h.get("cookie", "") for p, h in site.requests if p == "/"]
    site.requests.clear()
    runner.run(_hints(site.base), tmp_path / "out2", wait_ms=50, write_excel=False)
    second = [h.get("cookie", "") for p, h in site.requests if p == "/"]
    assert first == [""], "nothing to send on the first visit"
    assert second == ["cleared=yes"], "the second run presents the cookie the site set"


def test_a_seed_refused_headless_is_retried_once_headed_when_a_display_exists(
        site, tmp_path, monkeypatch):
    """Policy, not pixels: the relaunch is observed and kept headless so the
    test opens no window. The launch path it exercises is the same one."""
    site.script["/"] = [(403, {}, ""), (200, {}, _listing(4))]
    monkeypatch.setattr(access, "display_available", lambda: True)
    asked = []
    original = runner._Browser.relaunch

    def observed(self, headless, scripted=None):
        asked.append(headless)
        return original(self, True, scripted)
    monkeypatch.setattr(runner._Browser, "relaunch", observed)
    summary, _ = _run(site, tmp_path)
    assert asked == [False], "exactly one relaunch, asked to be visible"
    assert site.hits.count("/") == 2 and summary["rows"] == 4
    assert summary["access"]["refusals"] == 1 and summary["access"]["recovered"] == 1
    assert summary["browser"]["headed_retry"] is True and summary["browser"]["relaunches"] == 1


# --------------------------------------------------------------------------- #
# CHG-009 step 7: a challenge is waited out, and a host that cleared only in a
# visible browser starts with one next time
# --------------------------------------------------------------------------- #

def _challenge(reload_after_ms=None):
    """Cloudflare's shape: a 403, `cf-mitigated: challenge`, the interstitial
    title. With `reload_after_ms` the page does what a cleared challenge does
    and loads itself again; without it, it sits there."""
    script = (f"<script>setTimeout(()=>location.reload(), {reload_after_ms})</script>"
              if reload_after_ms is not None else "")
    return (403, {"cf-mitigated": "challenge"},
            "<html><head><title>Just a moment...</title></head><body>"
            f"<p>Checking your browser before accessing the site.</p>{script}</body></html>")


def test_a_challenge_that_clears_is_waited_out_not_refused(site, tmp_path):
    site.script["/"] = [_challenge(reload_after_ms=300), (200, {}, _listing(4))]
    summary, _ = _run(site, tmp_path)
    assert site.hits.count("/") == 2, "the interstitial and the page it became"
    assert summary["rows"] == 4
    assert summary["access"]["refusals"] == 0 and not summary["stopped"]
    assert not any("REFUSED" in w for w in summary["warnings"])
    assert _manifest(site)["last_attempt_result"] == "ok"


def test_a_challenge_that_never_clears_is_a_refusal_after_the_wait(site, tmp_path, monkeypatch):
    monkeypatch.setattr(access, "display_available", lambda: False)
    site.script["/"] = [_challenge()]
    summary, _ = _run(site, tmp_path)
    assert site.hits.count("/") == 2, "once, the pause, the one retry"
    assert summary["stopped"].startswith(f"blocked at {site.base}/")
    assert "just a moment" in summary["stopped"].lower()
    assert _manifest(site)["consecutive_blocks"] == 1
    assert _manifest(site).get("needs_headed") is None


def test_a_host_that_cleared_only_headed_starts_headed_next_time(site, tmp_path, monkeypatch):
    """Run one: refused headless, cleared on the visible retry -> the manifest
    says so. Run two: starts visible, no refusal, no relaunch. Both relaunch
    and launch are observed and kept headless so no window opens."""
    monkeypatch.setattr(access, "display_available", lambda: True)
    asked = {"launch": [], "relaunch": []}
    launch, relaunch = runner._Browser.launch, runner._Browser.relaunch

    def observed_launch(self, headless):
        asked["launch"].append(headless)
        return launch(self, True)

    def observed_relaunch(self, headless, scripted=None):
        asked["relaunch"].append(headless)
        return relaunch(self, True, scripted)
    monkeypatch.setattr(runner._Browser, "launch", observed_launch)
    monkeypatch.setattr(runner._Browser, "relaunch", observed_relaunch)

    # a bare 403, not a challenge: an uncleared challenge earns the script
    # retry first (step 8); the visible retry is for every other refusal
    site.script["/"] = [(403, {}, ""), (200, {}, _listing(4))]
    one = runner.run(_hints(site.base), tmp_path / "out1", wait_ms=50, write_excel=False)
    assert asked["launch"][0] is True and asked["relaunch"] == [False]
    assert one["rows"] == 4 and one["access"]["recovered"] == 1
    assert one["access"]["needs_headed"] is True and _manifest(site)["needs_headed"] is True

    asked["launch"].clear(); asked["relaunch"].clear(); site.hits.clear()
    site.script["/"] = [_challenge(reload_after_ms=300), (200, {}, _listing(4))]
    two = runner.run(_hints(site.base), tmp_path / "out2", wait_ms=50, write_excel=False)
    assert asked["launch"][0] is False, "started visible on the manifest's word"
    assert asked["relaunch"] == [] and two["browser"]["relaunches"] == 0
    assert two["rows"] == 4 and two["access"]["refusals"] == 0
    assert _manifest(site)["needs_headed"] is True

    monkeypatch.setattr(access, "display_available", lambda: False)
    asked["launch"].clear()
    site.script["/"] = [(200, {}, _listing(4))]
    three = runner.run(_hints(site.base), tmp_path / "out3", wait_ms=50, write_excel=False)
    assert asked["launch"][0] is True, "no display: headless, whatever the manifest says"
    assert three["rows"] == 4 and _manifest(site)["needs_headed"] is False, \
        "a clean headless run clears the flag"


# --------------------------------------------------------------------------- #
# CHG-009 step 8: the visitor script as the retry after an uncleared challenge
# --------------------------------------------------------------------------- #

def _scripted_challenge():
    """A challenge whose script clears only for a browser that hides the
    webdriver flag the way the visitor script does: a real Chrome, and
    Playwright's without the script, both say `false`, not `undefined`."""
    return (403, {"cf-mitigated": "challenge"},
            "<html><head><title>Just a moment...</title></head><body>"
            "<p>Checking your browser.</p><script>"
            "if (navigator.webdriver === undefined) {"
            "  fetch('/clear').then(() => location.reload()); }"
            "</script></body></html>")


def _observe_relaunch(monkeypatch):
    """Relaunches recorded as (headless, scripted) and kept headless so the
    tests open no window; the script setting is honoured."""
    asked = []
    original = runner._Browser.relaunch

    def observed(self, headless, scripted=None):
        asked.append((headless, scripted))
        return original(self, True, scripted)
    monkeypatch.setattr(runner._Browser, "relaunch", observed)
    return asked


def test_an_uncleared_challenge_earns_the_retry_with_the_script(site, tmp_path, monkeypatch):
    monkeypatch.setattr(access, "display_available", lambda: True)
    asked = _observe_relaunch(monkeypatch)
    site.script["/"] = [_scripted_challenge()]
    summary, _ = _run(site, tmp_path)
    assert asked == [(True, True)], "one relaunch, same mode, with the script"
    assert site.cleared == 1 and summary["rows"] == 4
    assert summary["access"]["refusals"] == 1 and summary["access"]["recovered"] == 1
    b = summary["browser"]
    assert b["script_retry"] is True and b["headed_retry"] is False
    assert summary["access"]["needs_script"] is True
    assert _manifest(site)["script_needed_at"] and _manifest(site)["history"][-1]["script_needed"] is True


def test_a_host_that_needed_the_script_starts_with_it_for_thirty_days(site, tmp_path, monkeypatch):
    monkeypatch.setattr(access, "display_available", lambda: False)
    asked = _observe_relaunch(monkeypatch)
    access.AccessState(access.host_of(site.base)).record_ok(script_needed=True)
    site.script["/"] = [_scripted_challenge()]
    summary, _ = _run(site, tmp_path)
    assert asked == [] and summary["browser"]["started_scripted"] is True
    assert site.cleared == 1 and summary["rows"] == 4
    assert summary["access"]["refusals"] == 0
    assert _manifest(site)["history"][-1]["script_needed"] is False, "started with it, not needed by a retry"


def test_after_the_memory_expires_the_host_starts_clean_again(site, tmp_path, monkeypatch):
    monkeypatch.setattr(access, "display_available", lambda: False)
    asked = _observe_relaunch(monkeypatch)
    state = access.AccessState(access.host_of(site.base))
    state.record_ok(script_needed=True)
    m = state.manifest()
    m["script_needed_at"] = access._iso(access._now() - timedelta(days=access.SCRIPT_MEMORY_DAYS + 1))
    state._write(m)
    summary, _ = _run(site, tmp_path)
    assert summary["browser"]["started_scripted"] is False and asked == []
    assert summary["rows"] == 4 and site.cleared == 0


def test_a_wall_does_not_earn_the_script_only_the_visible_retry(site, tmp_path, monkeypatch):
    """Scoped: Akamai's wall is not a challenge. With a display the retry is
    visible, without one it is on the same page; never with the script."""
    asked = _observe_relaunch(monkeypatch)
    wall = (403, {}, "<html><head><title>Access Denied</title></head><body>"
                     "<h1>Access Denied</h1><p>You don't have permission.</p></body></html>")
    monkeypatch.setattr(access, "display_available", lambda: True)
    site.script["/"] = [wall]
    summary, _ = _run(site, tmp_path)
    assert asked == [(False, None)] and summary["browser"]["script_retry"] is False
    assert summary["stopped"].startswith(f"blocked at {site.base}/")
    assert "script_needed_at" not in _manifest(site)


def test_a_challenge_that_clears_on_its_own_never_gets_the_script(site, tmp_path, monkeypatch):
    asked = _observe_relaunch(monkeypatch)
    site.script["/"] = [_challenge(reload_after_ms=300), (200, {}, _listing(4))]
    summary, _ = _run(site, tmp_path)
    assert asked == [] and summary["browser"]["scripted"] is False
    assert summary["rows"] == 4 and "script_needed_at" not in _manifest(site)


def test_without_a_display_the_seed_retry_stays_headless(site, tmp_path, monkeypatch):
    site.script["/"] = [(403, {}, ""), (200, {}, _listing(4))]
    monkeypatch.setattr(access, "display_available", lambda: False)
    asked = []
    monkeypatch.setattr(runner._Browser, "relaunch",
                        lambda self, headless, scripted=None: asked.append(headless))
    summary, _ = _run(site, tmp_path)
    assert asked == [] and summary["rows"] == 4
    assert summary["browser"]["headed_retry"] is False


def test_the_orchestrator_gate_sees_a_stopped_and_a_refused_run():
    """The gate reads `last_result["run"]`; both outcomes must be problems."""
    from orchestrator.orchestrator import Orchestrator

    class _Crawler:
        def __init__(self, run):
            self.last_result = {"run": run}

    class _Repo:
        def last_good_run(self, key):
            return None

    def gate(run):
        o = Orchestrator.__new__(Orchestrator)
        o.crawler, o.repo, o.source_name = _Crawler(run), _Repo(), "stub"
        trusted, problems = o.check_run_trustworthy([])[:2]
        return problems

    assert any("stopped early" in p for p in gate(
        {"blocked_pages": 1, "warnings": [], "stopped": "blocked at x: http 403"}))
    assert any("refused before start" in p for p in gate(
        {"blocked_pages": 0, "warnings": [], "refused": True, "refused_reason": "backing off"}))
    assert not any("stopped" in p or "refused" in p for p in gate(
        {"blocked_pages": 0, "warnings": []}))
