"""CHG-009 step 1: the access module, with no browser.

Pins the three answers access.py gives the runner -- how long to pause,
whether a response is a refusal, whether a host may be visited -- so they stay
right when the runner is wired to them in later steps.

    .venv/Scripts/python.exe -m pytest tests/test_formfill_access.py -q
"""

import json
import random
import sys
from datetime import timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dynamic_crawler.formfill import access  # noqa: E402
from utils import pacer  # noqa: E402


# --------------------------------------------------------------------------- #
# config/pace.yml
# --------------------------------------------------------------------------- #

def _pace_file(tmp_path, text: str) -> Path:
    p = tmp_path / "pace.yml"
    p.write_text(text, encoding="utf-8")
    return p


def test_shipped_pace_file_loads_and_names_the_known_hosts():
    cfg = access.PaceConfig.load()
    assert cfg.base_ms == (1500, 3500)
    assert cfg.level_for("simah.com") == "very_careful"
    assert cfg.level_for("www.cma.gov.sa") == "careful"        # parent-domain match
    assert cfg.level_for("rulebook.sama.gov.sa") == "normal"   # not listed
    assert cfg.is_throttle_only("nbr.gov.bh")
    assert not cfg.is_throttle_only("simah.com")


def test_missing_file_gives_defaults(tmp_path):
    cfg = access.PaceConfig.load(tmp_path / "absent.yml")
    assert cfg.base_ms == access.DEFAULT_BASE_MS
    assert cfg.level_for("anything.example") == "normal"


def test_most_specific_host_wins(tmp_path):
    cfg = access.PaceConfig.load(_pace_file(tmp_path, """
base_ms: [100, 200]
hosts:
  gov.sa: careful
  cma.gov.sa: very_careful
"""))
    assert cfg.level_for("www.cma.gov.sa") == "very_careful"
    assert cfg.level_for("misa.gov.sa") == "careful"
    assert cfg.level_for("gov.sa") == "careful"


@pytest.mark.parametrize("bad", [
    "base_ms: [5000, 100]",                 # low > high
    "base_ms: [-1, 100]",                   # negative
    "base_ms: fast",                        # not numbers
    "default: relaxed",                     # unknown default level
    "hosts:\n  x.example: sprint",          # unknown host level
    "levels:\n  careful: 0",                # multiplier must be positive
    "- just\n- a\n- list",                  # not a mapping
])
def test_a_bad_pace_file_fails_at_load(tmp_path, bad):
    with pytest.raises(ValueError):
        access.PaceConfig.load(_pace_file(tmp_path, bad))


def test_scalar_base_ms_is_a_fixed_pause(tmp_path):
    cfg = access.PaceConfig.load(_pace_file(tmp_path, "base_ms: 700"))
    assert cfg.base_ms == (700, 700)


# --------------------------------------------------------------------------- #
# pace_for / draw
# --------------------------------------------------------------------------- #

def _cfg(**kw):
    return access.PaceConfig(**kw)


def test_draw_stays_in_range_and_scales_with_the_level():
    cfg = _cfg(base_ms=(1000, 2000), hosts={"cma.gov.sa": "careful"})
    normal = access.pace_for("https://misa.gov.sa/laws", config=cfg)
    careful = access.pace_for("https://www.cma.gov.sa/en/x", config=cfg)
    rng = random.Random(7)
    for _ in range(200):
        assert 1000 <= normal.draw(rng) <= 2000
        assert 3000 <= careful.draw(rng) <= 6000


def test_draw_varies():
    pace = access.pace_for("https://misa.gov.sa/", config=_cfg(base_ms=(1000, 3000)))
    rng = random.Random(1)
    draws = {pace.draw(rng) for _ in range(30)}
    assert len(draws) > 20, "pauses must not repeat: a fixed interval is what a rate rule looks for"


def test_hints_pace_overrides_the_host_table():
    cfg = _cfg(hosts={"cma.gov.sa": "careful"})
    p = access.pace_for("https://cma.gov.sa/", hints={"pace": "very_careful"}, config=cfg)
    assert p.level == "very_careful" and p.multiplier == 5
    with pytest.raises(ValueError):
        access.pace_for("https://cma.gov.sa/", hints={"pace": "sprint"}, config=cfg)


def test_throttle_only_travels_with_the_pace():
    cfg = _cfg(throttle_only=frozenset({"nbr.gov.bh"}))
    assert access.pace_for("https://www.nbr.gov.bh/laws", config=cfg).throttle_only
    assert not access.pace_for("https://cbb.gov.bh/", config=cfg).throttle_only


def test_host_of():
    assert access.host_of("https://WWW.CMA.gov.sa/en/x?y=1") == "www.cma.gov.sa"
    assert access.host_of("not a url") == ""
    assert access.host_of("") == ""


# --------------------------------------------------------------------------- #
# retry_after_ms
# --------------------------------------------------------------------------- #

def test_retry_after_seconds_and_cap():
    assert access.retry_after_ms("30", 5000) == 30_000
    assert access.retry_after_ms("99999", 5000) == 3_600_000   # an hour at most
    assert access.retry_after_ms(None, 5000) == 5000
    assert access.retry_after_ms("soon", 5000) == 5000
    assert access.retry_after_ms("0", 5000) == 5000


def test_retry_after_http_date():
    from email.utils import format_datetime
    when = access._now() + timedelta(seconds=90)
    ms = access.retry_after_ms(format_datetime(when, usegmt=True), 5000)
    assert 85_000 <= ms <= 91_000
    past = format_datetime(access._now() - timedelta(seconds=90), usegmt=True)
    assert access.retry_after_ms(past, 5000) == 5000


# --------------------------------------------------------------------------- #
# classify -- by utils.pacer.classify, so the browser runner and the requests
# crawlers (CBI, CBK Kenya) agree on what a refusal is
# --------------------------------------------------------------------------- #

CLOUDFLARE_BLOCK = ("Attention Required! | Cloudflare",
                    "Sorry, you have been blocked\nYou are unable to access simah.com")
REAL_PAGE = ("Rules and Regulations",
             "Article-1 The following words and phrases, wherever mentioned in this Law, "
             "shall have the meanings assigned thereto unless the context requires otherwise.")
URL = "https://misa.gov.sa/activities/laws/"


@pytest.fixture(autouse=True)
def _fresh_pacer():
    pacer.reload()
    pacer.reset_run()
    yield
    pacer.reset_run()


def test_a_bare_403_is_a_refusal_even_with_an_empty_body():
    r = access.classify(URL, 403, {}, "", "")
    assert r and r.kind == "unknown" and r.status == 403 and not r.throttled


def test_429_is_a_throttle_and_carries_retry_after():
    r = access.classify(URL, 429, {"Retry-After": "120"}, "", "")
    assert r and r.throttled and r.retry_after == "120"


def test_a_5xx_with_retry_after_is_a_throttle_without_it_a_failed_load():
    assert access.classify(URL, 503, {"Retry-After": "30"}, "", "").throttled
    assert not access.classify(URL, 503, {}, "", "")


def test_wall_text_on_a_200_is_a_block():
    r = access.classify(URL, 200, {}, *CLOUDFLARE_BLOCK)
    assert r and r.kind == "block" and not r.throttled
    assert "blocked" in r.reason.lower() or "attention required" in r.reason.lower()


def test_a_real_page_is_not_a_refusal():
    r = access.classify(URL, 200, {}, *REAL_PAGE)
    assert not r and r.reason == "" and r.kind == ""


def test_other_statuses_are_not_refusals():
    # A 404 or a 500 is a failed load, which the runner may retry as before.
    assert not access.classify(URL, 404, {}, "Not found", "")
    assert not access.classify(URL, 500, {}, "", "")
    assert not access.classify(URL, None, None, "", "")


def test_throttle_only_hosts_keep_their_own_retries():
    # pacer: on a throttle_only host a 403/404/429 is 'ok' -- its own retries
    # handle these -- so the runner must not count it as a refusal either.
    assert not access.classify("https://www.nbr.gov.bh/x", 429, {}, "", "")
    assert not access.classify("https://www.nbr.gov.bh/x", 403, {}, "", "")
    # ...but a wall page there is still a wall.
    assert access.classify("https://www.nbr.gov.bh/x", 200, {}, *CLOUDFLARE_BLOCK)


def test_wall_text_only_read_from_the_first_3000_chars():
    body = "x" * 3000 + " you have been blocked"
    assert not access.classify(URL, 200, {}, "", body)


# --------------------------------------------------------------------------- #
# AccessState
# --------------------------------------------------------------------------- #

def test_fresh_host_may_attempt(tmp_path):
    st = access.AccessState("misa.gov.sa", tmp_path)
    assert st.may_attempt() == (True, "")
    assert not st.path.exists(), "nothing is written until something happens"


def _expire(st, hours_ago=0.0):
    """Move the host's window into the past by rewriting the manifest."""
    m = st.manifest()
    m["next_attempt_after"] = access._iso(access._now() - timedelta(minutes=1, hours=hours_ago))
    st.path.write_text(json.dumps(m), encoding="utf-8")
    return m


def test_first_block_of_a_cycle_opens_the_six_hour_window(tmp_path):
    st = access.AccessState("simah.com", tmp_path)
    before = access._now()
    m = st.record_block("http 403")
    assert m["consecutive_blocks"] == 1 and m["cycle_attempt"] == 1
    assert m["blocked_cycles"] == 0, "one blocked attempt is not a blocked cycle"
    assert m["first_blocked_at"]
    after = access._parse(m["next_attempt_after"])
    h = access.REATTEMPT_HOURS
    assert timedelta(hours=h - 0.01) < after - before < timedelta(hours=h + 0.01)
    assert m["history"][-1]["attempt"] == 1 and m["history"][-1]["reattempt_hours"] == h
    assert not st.may_attempt()[0]


def test_inside_the_window_is_refused_with_a_reason(tmp_path):
    st = access.AccessState("simah.com", tmp_path)
    st.record_block("you have been blocked")
    ok, why = st.may_attempt()
    assert not ok
    assert "simah.com" in why and "1 time" in why and "next attempt after" in why


def test_after_the_window_the_next_run_is_the_cycles_second_attempt(tmp_path):
    st = access.AccessState("simah.com", tmp_path)
    st.record_block("http 403")
    _expire(st)
    assert st.may_attempt()[0]
    assert st.attempt_number() == 2


def test_second_block_closes_the_cycle_and_opens_no_window(tmp_path):
    st = access.AccessState("simah.com", tmp_path)
    st.record_block("http 403")
    _expire(st)
    m = st.record_block("http 403")
    assert m["consecutive_blocks"] == 2 and m["cycle_attempt"] == 0
    assert m["blocked_cycles"] == 1
    assert m["next_attempt_after"] is None, "nothing until the next scheduled cycle"
    assert m["history"][-1]["attempt"] == 2 and m["history"][-1]["reattempt_hours"] is None
    assert st.may_attempt()[0] and st.attempt_number() == 1


def test_two_blocked_cycles_escalate_and_the_routine_goes_on(tmp_path):
    st = access.AccessState("simah.com", tmp_path)
    first = None
    for cycle in (1, 2, 3):
        st.record_block("http 403")
        _expire(st)
        m = st.record_block("http 403")
        first = first or m["first_blocked_at"]
        assert m["blocked_cycles"] == cycle
        assert m["first_blocked_at"] == first, "the streak keeps its start date"
        assert st.may_attempt()[0], "no cycle count ever refuses the next scheduled run"
    assert m["blocked_cycles"] == 3, "the count the run store's rule reads (critical at 2)"
    assert m["consecutive_blocks"] == 6


def test_a_stale_open_cycle_is_closed_as_blocked_by_the_next_block(tmp_path):
    """The 6-hour re-attempt never came (the API was down); the next scheduled
    run's block closes the old cycle and starts a new one, so the count is
    not one short."""
    st = access.AccessState("simah.com", tmp_path)
    st.record_block("http 403")
    _expire(st, hours_ago=access.CYCLE_STALE_HOURS + 1)
    assert st.attempt_number() == 1, "a stale cycle makes the next run a first attempt again"
    m = st.record_block("http 403")
    assert m["blocked_cycles"] == 1 and m["cycle_attempt"] == 1
    assert m["next_attempt_after"], "the new cycle opened its own window"


def test_ok_resets_the_block_memory(tmp_path):
    st = access.AccessState("simah.com", tmp_path)
    st.record_block("http 403")
    _expire(st)
    st.record_block("http 403")
    st.record_block("http 403")
    m = st.record_ok()
    assert m["consecutive_blocks"] == 0 and m["cycle_attempt"] == 0 and m["blocked_cycles"] == 0
    assert m["first_blocked_at"] is None and m["next_attempt_after"] is None
    assert m["last_attempt_result"] == "ok" and m["last_ok_at"]
    assert st.may_attempt()[0] and st.attempt_number() == 1
    # The next block starts a fresh cycle.
    assert st.record_block("http 403")["history"][-1]["attempt"] == 1


def test_history_is_capped_at_twenty(tmp_path):
    st = access.AccessState("x.example", tmp_path)
    for _ in range(30):
        st.record_ok()
    assert len(st.manifest()["history"]) == 20


def test_manifest_file_is_named_after_the_host(tmp_path):
    st = access.AccessState("WWW.CMA.gov.sa", tmp_path)
    st.record_ok()
    assert (tmp_path / "www.cma.gov.sa.json").exists()


def test_corrupt_manifest_reads_as_empty(tmp_path):
    st = access.AccessState("x.example", tmp_path)
    st.path.write_text("{not json", encoding="utf-8")
    assert st.manifest() == {}
    assert st.may_attempt()[0]


# --------------------------------------------------------------------------- #
# vary
# --------------------------------------------------------------------------- #

class _Mouse:
    def __init__(self):
        self.wheels, self.moves = [], []

    def wheel(self, dx, dy):
        self.wheels.append((dx, dy))

    def move(self, x, y, steps=1):
        self.moves.append((x, y, steps))


class _Page:
    def __init__(self):
        self.mouse = _Mouse()
        self.waits = []

    def wait_for_timeout(self, ms):
        self.waits.append(ms)


def test_vary_scrolls_a_different_distance_each_time():
    seen = set()
    for seed in range(10):
        p = _Page()
        done = access.vary(p, random.Random(seed))
        assert 1 <= len(done["strokes"]) <= 3
        assert all(1500 <= dy <= 7000 for dy in done["strokes"])
        assert [dy for _, dy in p.mouse.wheels] == done["strokes"]
        assert len(p.mouse.moves) == 1 and done["mouse"] == p.mouse.moves[0][:2]
        seen.add(tuple(done["strokes"]))
    assert len(seen) > 1


def test_vary_survives_a_dead_page():
    class Dead:
        class mouse:
            @staticmethod
            def wheel(dx, dy):
                raise RuntimeError("Target closed")
        def wait_for_timeout(self, ms):
            pass
    assert access.vary(Dead(), random.Random(0))["mouse"] is None


# --------------------------------------------------------------------------- #
# CHG-009 step 5: browser identity helpers
# --------------------------------------------------------------------------- #

def test_visitor_user_agent_drops_only_the_headless_word():
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) HeadlessChrome/141.0.7390.37 Safari/537.36")
    assert access.visitor_user_agent(ua) == ua.replace("HeadlessChrome", "Chrome")
    assert "141.0.7390.37" in access.visitor_user_agent(ua), "the real version stays"
    assert access.visitor_user_agent(ua.replace("HeadlessChrome", "Chrome")) == ua.replace("HeadlessChrome", "Chrome")
    assert access.visitor_user_agent("") == ""


def test_profile_dir_is_per_host_under_the_profiles_root(monkeypatch, tmp_path):
    monkeypatch.setattr(access, "PROFILE_DIR", tmp_path)
    assert access.profile_dir("WWW.CMA.gov.sa") == tmp_path / "www.cma.gov.sa"
    assert access.profile_dir("") == tmp_path / "unknown"


def test_a_challenge_is_known_by_header_or_title_and_a_wall_is_not():
    assert access.is_challenge(403, {"cf-mitigated": "challenge"}, "")
    assert access.is_challenge(403, {"CF-Mitigated": "Challenge"}, "")
    assert access.is_challenge(403, {}, "Just a moment...")
    assert access.is_challenge(200, None, "just a moment")
    assert not access.is_challenge(403, {"cf-mitigated": "block"}, "")
    assert not access.is_challenge(403, {}, "Access Denied")
    assert not access.is_challenge(403, {}, "Sorry, you have been blocked")
    assert not access.is_challenge(200, {}, "Laws and Regulations")


def test_a_clean_run_records_whether_it_needed_a_visible_browser(tmp_path):
    s = access.AccessState("challenge.example", tmp_path)
    assert s.needs_headed() is False
    s.record_ok(headed=True)
    assert s.needs_headed() is True
    assert s.manifest()["history"][-1]["headed"] is True
    s.record_ok()
    assert s.needs_headed() is False, "a clean headless run clears it"
    s.record_block("wall")
    assert s.needs_headed() is False, "a block does not set it"


def test_the_visitor_script_is_remembered_for_thirty_days(tmp_path, monkeypatch):
    s = access.AccessState("challenge.example", tmp_path)
    assert s.needs_script() is False
    s.record_ok(script_needed=False)
    assert s.needs_script() is False, "a clean run without it leaves no stamp"
    s.record_ok(script_needed=True)
    assert s.needs_script() is True
    stamp = s.manifest()["script_needed_at"]
    s.record_ok(script_needed=False)
    assert s.needs_script() is True and s.manifest()["script_needed_at"] == stamp, \
        "a run that merely started with the script does not renew the stamp"
    later = access._now() + timedelta(days=access.SCRIPT_MEMORY_DAYS, minutes=1)
    monkeypatch.setattr(access, "_now", lambda: later)
    assert s.needs_script() is False, "the memory expires"
    s.record_block("wall")
    assert s.manifest()["script_needed_at"] == stamp, "a block does not touch the stamp"


def test_the_visitor_script_says_what_the_doc_says_it_says():
    js = access.VISITOR_SCRIPT
    assert "'webdriver'" in js and "undefined" in js
    assert "chrome.runtime" not in js and "{runtime: {}}" in js
    assert "'plugins'" in js and "'languages'" in js
    assert js.count("\n") <= 8, "four lines copied from the DFPI crawler, nothing added"


def test_a_refusal_carries_whether_it_was_an_uncleared_challenge():
    r = access.Refusal("Just a moment", "block", 403)
    assert r.challenge is False
    r.challenge = True
    assert r and r.challenge


def test_display_available_follows_the_platform(monkeypatch):
    import sys as _sys
    monkeypatch.setattr(_sys, "platform", "win32")
    assert access.display_available()
    monkeypatch.setattr(_sys, "platform", "linux")
    monkeypatch.delenv("DISPLAY", raising=False)
    monkeypatch.delenv("WAYLAND_DISPLAY", raising=False)
    assert not access.display_available()
    monkeypatch.setenv("DISPLAY", ":0")
    assert access.display_available()
