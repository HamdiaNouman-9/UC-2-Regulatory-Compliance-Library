"""The CBB feed gate, offline. A feed that fails must never read as 'nothing changed'.

    venv/Scripts/python.exe -m pytest tests/test_cbb_feed_signal.py -q
"""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dynamic_crawler import cbb_feed_signal as f  # noqa: E402

TODAY = dt.date(2026, 9, 21)


def state(days_ago):
    return {"last_full_crawl": (TODAY - dt.timedelta(days=days_ago)).isoformat()}


def test_no_recorded_crawl_means_crawl():
    v = f.decide({}, TODAY, fetch=lambda *a, **k: [])
    assert v["crawl"] and "no full crawl" in v["reason"]


def test_revisions_since_last_crawl_means_crawl():
    v = f.decide(state(3), TODAY, fetch=lambda *a, **k: [{"title": "LR-1", "changed": "2026-09-20"}])
    assert v["crawl"] and v["changes"] == 1 and v["sample"]


def test_quiet_feed_and_recent_crawl_skips():
    v = f.decide(state(3), TODAY, fetch=lambda *a, **k: [])
    assert not v["crawl"] and v["changes"] == 0


def test_an_old_crawl_runs_even_when_the_feed_is_quiet():
    called = []
    v = f.decide(state(31), TODAY, fetch=lambda *a, **k: called.append(1) or [])
    assert v["crawl"] and "deletions" in v["reason"] and not called


def test_unreadable_feed_is_reported_unavailable_and_nothing_is_crawled():
    def boom(*a, **k):
        raise f.FeedUnavailable("HTTP 403")
    v = f.decide(state(3), TODAY, fetch=boom)
    assert not v["crawl"] and v["status"] == "unavailable" and "403" in v["reason"]
    assert v["changes"] is None          # not 0: 'no changes' was never established


def test_window_opens_before_the_last_crawl():
    seen = {}
    f.decide(state(5), TODAY, fetch=lambda since, until, **k: seen.update(s=since, u=until) or [])
    assert seen["s"] == TODAY - dt.timedelta(days=5 + f.OVERLAP_DAYS) and seen["u"] == TODAY


def test_bad_state_date_is_treated_as_none():
    assert f.decide({"last_full_crawl": "junk"}, TODAY, fetch=lambda *a, **k: [])["crawl"]


def test_a_200_that_is_not_the_feed_is_rejected():
    from bs4 import BeautifulSoup
    assert not f._looks_like_the_feed(BeautifulSoup("<html><h1>Access denied</h1></html>", "html.parser"))
    assert f._looks_like_the_feed(BeautifulSoup('<div class="view-content"></div>', "html.parser"))


def test_state_round_trip(tmp_path):
    p = tmp_path / "s.json"
    f.record_full_crawl(TODAY, p)
    assert f.load_state(p)["last_full_crawl"] == "2026-09-21"
    assert f.load_state(tmp_path / "missing.json") == {}


def test_volume_selection_matches_the_live_sidebar_titles():
    from types import SimpleNamespace as N
    from cbb_test_crawlers.cbb_rulebook_crawler import _select_volume
    live = ["Common Volume", "Central Bank of Bahrain Volume 1—Conventional Banks",
            "Central Bank of Bahrain Volume 2—Islamic Banks",
            "Central Bank of Bahrain Volume 7—Collective Investment Undertakings",
            "Central Bank of Bahrain Volume 10—Hypothetical"]
    vs = [N(text=x) for x in live]
    assert _select_volume(vs, "Volume 1")[0].text.endswith("Conventional Banks")
    assert _select_volume(vs, "volume 7")[0].text.endswith("Undertakings")
    assert _select_volume(vs, "Common Volume")[0].text == "Common Volume"
    import pytest
    with pytest.raises(ValueError):
        _select_volume(vs, "Volume 3")
