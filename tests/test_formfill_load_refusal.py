"""CHG-009 step 2: a refusal leaves the retry loop.

Before this, `_load` saw only text. A 403 with an empty body looked like a slow
render and went round the retry loop three times on the listing and twice on
every detail page -- the pattern the scheduler notes call "what makes a block".
These tests pin the new behaviour on a scripted page object, with no browser
and no network: a refusal returns after ONE navigation carrying the reason; a
timeout or an empty render is still retried as before.

    .venv/Scripts/python.exe -m pytest tests/test_formfill_load_refusal.py -q
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dynamic_crawler.formfill import runner  # noqa: E402
from utils import pacer  # noqa: E402

URL = "https://misa.gov.sa/activities/laws/"
WALL = ("Attention Required! | Cloudflare",
        "Sorry, you have been blocked. You are unable to access simah.com. "
        "This website is using a security service to protect itself from online attacks.")
REAL = ("Rules and Regulations",
        "Article-1 The following words and phrases, wherever mentioned in this Law, shall have "
        "the meanings assigned thereto unless the context requires otherwise. " * 5)


@pytest.fixture(autouse=True)
def _fresh_pacer():
    pacer.reload()
    pacer.reset_run()
    yield
    pacer.reset_run()


class _Resp:
    def __init__(self, status, headers=None):
        self.status = status
        self.headers = headers or {}


class _Mouse:
    def wheel(self, dx, dy):
        pass


class _Page:
    """One scripted answer per navigation. Each entry is either a `_Resp` with
    the (title, body, anchors) the page then shows, or an exception to raise."""

    def __init__(self, script):
        self.script = list(script)
        self.gotos = []
        self.mouse = _Mouse()
        self.url = "about:blank"
        self.title, self.body, self.anchors = "", "", 0

    def goto(self, url, **kw):
        self.gotos.append(url)
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        resp, (self.title, self.body, self.anchors) = step
        self.url = url
        return resp

    def wait_for_timeout(self, ms):
        pass

    def set_content(self, html, **kw):
        self.title, self.body, self.anchors = "snapshot", "x" * 1000, 50

    def evaluate(self, js, arg=None):
        if "document.title" in js:
            return {"t": self.title, "x": self.body[:3000], "a": self.anchors,
                    "n": len(self.body)}
        return len(self.body)


# --------------------------------------------------------------------------- #
# refusals: one navigation, then out
# --------------------------------------------------------------------------- #

def test_a_bare_403_returns_after_one_navigation():
    page = _Page([(_Resp(403), ("", "", 0))] * 3)
    res = runner._load(page, URL, wait_ms=0, tries=3)
    assert not res
    assert res.refusal and res.refusal.kind == "unknown" and res.refusal.status == 403
    assert res.attempts == 1
    assert len(page.gotos) == 1, "a refusal must not be retried"


def test_a_429_is_a_throttle_with_its_retry_after():
    page = _Page([(_Resp(429, {"retry-after": "90"}), ("", "", 0))] * 3)
    res = runner._load(page, URL, wait_ms=0, tries=3)
    assert not res and res.refusal.throttled
    assert res.refusal.retry_after == "90"
    assert len(page.gotos) == 1


def test_a_wall_page_on_a_200_is_a_block_and_not_retried():
    page = _Page([(_Resp(200), (*WALL, 3))] * 3)
    res = runner._load(page, URL, wait_ms=0, tries=3)
    assert not res and res.refusal.kind == "block"
    assert "blocked" in res.refusal.reason.lower() or "attention" in res.refusal.reason.lower()
    assert len(page.gotos) == 1


def test_a_refusal_on_a_later_attempt_still_stops_there():
    page = _Page([TimeoutError("nav"), (_Resp(403), ("", "", 0)), (_Resp(200), (*REAL, 30))])
    res = runner._load(page, URL, wait_ms=0, tries=3)
    assert not res and res.refusal.status == 403
    assert res.attempts == 2 and len(page.gotos) == 2


# --------------------------------------------------------------------------- #
# not refusals: the old behaviour, kept
# --------------------------------------------------------------------------- #

def test_a_real_page_loads_with_its_status():
    page = _Page([(_Resp(200), (*REAL, 30))])
    res = runner._load(page, URL, wait_ms=0)
    assert res and res.ok and res.status == 200 and res.attempts == 1
    assert not res.refusal


def test_a_timeout_is_retried_and_may_succeed():
    page = _Page([TimeoutError("nav"), (_Resp(200), (*REAL, 30))])
    res = runner._load(page, URL, wait_ms=0, tries=3)
    assert res and res.attempts == 2 and len(page.gotos) == 2


def test_an_empty_render_is_retried_to_the_end_without_a_refusal():
    page = _Page([(_Resp(200), ("", "", 0))] * 3)
    res = runner._load(page, URL, wait_ms=0, tries=3)
    assert not res and not res.refusal
    assert res.attempts == 3 and len(page.gotos) == 3


def test_a_404_and_a_500_are_failed_loads_not_refusals():
    for status in (404, 500):
        page = _Page([(_Resp(status), ("Not found", "", 0))] * 2)
        res = runner._load(page, URL, wait_ms=0, tries=2)
        assert not res and not res.refusal and len(page.gotos) == 2


def test_a_snapshot_touches_no_network():
    page = _Page([])
    res = runner._load(page, URL, wait_ms=0, snap="<html><body>saved</body></html>")
    assert res and res.status == 200 and page.gotos == []


def test_a_response_of_none_is_handled():
    # Playwright returns None for a same-document navigation.
    page = _Page([(None, (*REAL, 30))])
    res = runner._load(page, URL, wait_ms=0)
    assert res and res.status == 0


# --------------------------------------------------------------------------- #
# throttle_only hosts: the pacer's rule, honoured here too
# --------------------------------------------------------------------------- #

def test_a_throttle_only_host_keeps_its_own_retries():
    # config/pace.yml lists nbr.gov.bh under throttle_only: a 429 there is not a
    # refusal (utils.pacer), so the runner retries it like any failed load.
    page = _Page([(_Resp(429), ("", "", 0))] * 2 + [(_Resp(200), (*REAL, 30))])
    res = runner._load(page, "https://www.nbr.gov.bh/laws", wait_ms=0, tries=3)
    assert res and res.attempts == 3 and len(page.gotos) == 3


# --------------------------------------------------------------------------- #
# _blocked still answers for a page already in hand
# --------------------------------------------------------------------------- #

def test_blocked_reads_the_page_text():
    page = _Page([])
    page.title, page.body = WALL
    assert runner._blocked(page)
    page.title, page.body = REAL
    assert runner._blocked(page) == ""


def test_blocked_is_quiet_when_the_probe_throws():
    class Dead:
        url = URL

        def evaluate(self, js, arg=None):
            raise RuntimeError("Target closed")
    assert runner._blocked(Dead()) == ""


# --------------------------------------------------------------------------- #
# CHG-009 step 3: a jittered pause before every navigation, varied scrolling,
# and none of it on a snapshot replay
# --------------------------------------------------------------------------- #

from dynamic_crawler.formfill import access  # noqa: E402


class _RecMouse:
    def __init__(self):
        self.wheels, self.moves = [], []

    def wheel(self, dx, dy):
        self.wheels.append(dy)

    def move(self, x, y, steps=1):
        self.moves.append((x, y))


class _TimedPage(_Page):
    """Records every wait and what happened between them."""

    def __init__(self, script):
        super().__init__(script)
        self.mouse = _RecMouse()
        self.events = []          # ("wait", ms) | ("goto", url) | ("set_content",)

    def wait_for_timeout(self, ms):
        self.events.append(("wait", ms))

    def goto(self, url, **kw):
        self.events.append(("goto", url))
        return super().goto(url, **kw)

    def set_content(self, html, **kw):
        self.events.append(("set_content",))
        super().set_content(html, **kw)


def _pace(lo=100, hi=200, mult=1.0):
    return access.Pace(host="misa.gov.sa", level="test", multiplier=mult, base_ms=(lo, hi))


def _wait_before(events, marker):
    """The wait immediately before the first event of kind `marker`."""
    for i, e in enumerate(events):
        if e[0] == marker:
            prev = events[i - 1] if i else None
            return prev[1] if prev and prev[0] == "wait" else None
    return None


def test_a_pause_in_range_is_taken_before_the_first_navigation():
    page = _TimedPage([(_Resp(200), (*REAL, 30))])
    runner._load(page, URL, wait_ms=0, pace=_pace(100, 200))
    assert 100 <= _wait_before(page.events, "goto") <= 200


def test_the_pause_scales_with_the_level():
    page = _TimedPage([(_Resp(200), (*REAL, 30))])
    runner._load(page, URL, wait_ms=0, pace=_pace(100, 200, mult=5))
    assert 500 <= _wait_before(page.events, "goto") <= 1000


def test_every_retry_is_paced_too():
    page = _TimedPage([TimeoutError("nav"), TimeoutError("nav"), (_Resp(200), (*REAL, 30))])
    runner._load(page, URL, wait_ms=0, tries=3, pace=_pace(100, 200))
    gotos = [i for i, e in enumerate(page.events) if e[0] == "goto"]
    assert len(gotos) == 3
    for i in gotos:
        assert page.events[i - 1][0] == "wait" and 100 <= page.events[i - 1][1] <= 200


def test_scrolling_varies_with_a_pace_and_moves_the_mouse():
    distances = set()
    for _ in range(8):
        page = _TimedPage([(_Resp(200), (*REAL, 30))])
        runner._load(page, URL, wait_ms=0, pace=_pace())
        assert 1 <= len(page.mouse.wheels) <= 3
        assert all(1500 <= d <= 7000 for d in page.mouse.wheels)
        assert len(page.mouse.moves) == 1
        distances.update(page.mouse.wheels)
    assert len(distances) > 3, "the scroll must not be the same on every page"


def test_without_a_pace_the_old_two_fixed_strokes_remain():
    page = _TimedPage([(_Resp(200), (*REAL, 30))])
    runner._load(page, URL, wait_ms=0)
    assert page.mouse.wheels == [6000, 6000] and page.mouse.moves == []
    assert _wait_before(page.events, "goto") is None


def test_a_snapshot_replay_pays_no_pause_even_with_a_pace():
    page = _TimedPage([])
    res = runner._load(page, URL, wait_ms=5000, snap="<html><body>saved</body></html>",
                       pace=_pace(100_000, 200_000))
    assert res and page.gotos == []
    assert _wait_before(page.events, "set_content") is None
    assert all(ms <= 300 for kind, *rest in page.events for ms in rest if kind == "wait")


def test_pause_helper_is_a_no_op_without_a_pace():
    page = _TimedPage([])
    assert runner._pause(page, None) == 0 and page.events == []
    ms = runner._pause(page, _pace(50, 60))
    assert 50 <= ms <= 60 and page.events == [("wait", ms)]


def test_pace_for_the_run_comes_from_the_hints_host_and_is_reported(tmp_path, monkeypatch):
    """run() decides one pace per run from the seed's host and says so in the
    summary. Checked without a browser by stopping run() right after it has
    decided: sync_playwright is replaced with a stub that raises a marker."""
    seen = {}

    class _Stop(Exception):
        pass

    def fake_pace_for(url, hints=None, config=None):
        seen["url"], seen["hints"] = url, hints
        return _pace()

    def fake_pw():
        raise _Stop()

    monkeypatch.setattr(runner.access, "pace_for", fake_pace_for)
    monkeypatch.setattr(runner, "sync_playwright", fake_pw)
    hints = {"name": "x", "seed_url": "https://www.cma.gov.sa/en/x",
             "row_selector": "li", "pace": "careful"}
    with pytest.raises(_Stop):
        runner.run(hints, tmp_path / "out", write_excel=False)
    assert seen["url"] == hints["seed_url"] and seen["hints"] is hints


def test_a_snapshot_run_decides_no_pace(tmp_path, monkeypatch):
    called = []
    monkeypatch.setattr(runner.access, "pace_for", lambda *a, **k: called.append(a))

    class _Stop(Exception):
        pass

    def fake_pw():
        raise _Stop()
    monkeypatch.setattr(runner, "sync_playwright", fake_pw)
    snap = tmp_path / "s.html"
    snap.write_text("<html><body>saved</body></html>", encoding="utf-8")
    hints = {"name": "x", "seed_url": "https://simah.com/rules", "row_selector": "li"}
    with pytest.raises(_Stop):
        runner.run(hints, tmp_path / "out", snapshot=snap, write_excel=False)
    assert called == [], "a replay touches no network, so nothing is paced"


# --------------------------------------------------------------------------- #
# CHG-009 step 5: the guard's single retry can move to another page
# --------------------------------------------------------------------------- #

def _guard():
    return runner._Guard(pace=None, state=None, warnings=[])


def test_the_retry_runs_on_the_page_the_hook_returns(monkeypatch):
    monkeypatch.setattr(runner.access, "RETRY_PAUSE_MS", (1, 2))
    first = _TimedPage([(_Resp(403), ("", "", 0))])
    second = _TimedPage([(_Resp(200), (*REAL, 30))])
    calls = []

    def hook(refusal):
        calls.append(refusal.status)
        return second
    res = _guard().load(first, URL, wait_ms=0, tries=3, retry_page=hook)
    assert res and calls == [403], "the hook is handed the refusal"
    assert first.gotos == [URL] and second.gotos == [URL]


def test_a_hook_returning_none_retries_on_the_same_page(monkeypatch):
    monkeypatch.setattr(runner.access, "RETRY_PAUSE_MS", (1, 2))
    page = _TimedPage([(_Resp(403), ("", "", 0)), (_Resp(200), (*REAL, 30))])
    res = _guard().load(page, URL, wait_ms=0, tries=3, retry_page=lambda ref: None)
    assert res and page.gotos == [URL, URL]


def test_the_hook_is_only_called_for_the_single_retry(monkeypatch):
    monkeypatch.setattr(runner.access, "RETRY_PAUSE_MS", (1, 2))
    g = _guard()
    calls = []
    ok_page = _TimedPage([(_Resp(200), (*REAL, 30))])
    g.load(ok_page, URL, wait_ms=0, retry_page=lambda ref: calls.append(1))
    assert calls == [], "no refusal, no retry, no hook"
    refused = _TimedPage([(_Resp(403), ("", "", 0)), (_Resp(403), ("", "", 0))])
    with pytest.raises(runner.RunStopped):
        g.load(refused, URL, wait_ms=0, retry_page=lambda ref: calls.append(1) or None)
    assert calls == [1]


# --------------------------------------------------------------------------- #
# CHG-009 step 10: the cycle's re-attempt has no in-run retry
# --------------------------------------------------------------------------- #

def test_the_re_attempt_stops_on_the_first_refusal_and_calls_no_hook(monkeypatch):
    monkeypatch.setattr(runner.access, "RETRY_PAUSE_MS", (1, 2))
    g = runner._Guard(pace=None, state=None, warnings=[], in_run_retry=False)
    calls = []
    page = _TimedPage([(_Resp(403), ("", "", 0)), (_Resp(200), (*REAL, 30))])
    with pytest.raises(runner.RunStopped):
        g.load(page, URL, wait_ms=0, retry_page=lambda ref: calls.append(1) or None)
    assert page.gotos == [URL], "one load; the 200 that would have followed is never asked for"
    assert calls == [] and g.refusals == 1 and g.recovered == 0
    assert any("on the cycle's re-attempt" in w for w in g.warnings)
    assert not any("after the one retry" in w for w in g.warnings)
