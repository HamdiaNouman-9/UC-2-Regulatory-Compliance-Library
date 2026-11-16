"""CHG-009 step 10: the alert after two blocked cycles, its delivery channels,
and the note that schedules a cycle's 6-hour re-attempt. No browser, no
database, no network: the SMTP and webhook clients are replaced.

    .venv/Scripts/python.exe -m pytest tests/test_chg009_escalation.py -q
"""
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scheduler import reattempts  # noqa: E402
from storage import alert_channels, run_store  # noqa: E402

ACCESS = {"host": "dfpi.ca.gov", "refusals": 2, "recovered": 0, "consecutive_blocks": 4,
          "cycle_attempt": 0, "blocked_cycles": 2, "first_blocked_at": "2026-11-05T09:00:00+00:00",
          "next_attempt_after": None}


def _report(**access):
    acc = dict(ACCESS, **access)
    return {"regulator": "DFPI", "source": "dfpi", "crawled": 0, "classified": {}, "changes": {},
            "run_trustworthy": False, "gate_problems": ["crawl stopped early: blocked at x"],
            "access": acc}


# --------------------------------------------------------------------------- #
# the rule
# --------------------------------------------------------------------------- #

def _codes(alerts):
    return [(a["level"], a["code"]) for a in alerts]


def test_one_blocked_cycle_is_the_warning_it_always_was():
    alerts = run_store.alerts_for("finished", None, _report(blocked_cycles=1))
    assert ("warning", "run_untrusted") in _codes(alerts)
    assert not any(c == "site_blocked_repeatedly" for _, c in _codes(alerts))


def test_the_attempt_that_closes_the_second_blocked_cycle_is_critical():
    alerts = run_store.alerts_for("finished", None, _report(blocked_cycles=2, cycle_attempt=0))
    assert ("critical", "site_blocked_repeatedly") in _codes(alerts)
    assert run_store._level(alerts) == "critical"
    msg = next(a["message"] for a in alerts if a["code"] == "site_blocked_repeatedly")
    assert "dfpi.ca.gov has refused the crawler in 2 consecutive scheduled cycles since 2026-11-05" in msg
    assert "next scheduled cycle" in msg and "A person decides" in msg


def test_the_alert_comes_once_per_cycle_with_the_count():
    """The next cycle's first attempt and a run skipped inside its window
    carry the count too; they do not alert again. The attempt that closes
    the next cycle does, with the count moved on."""
    first = _report(blocked_cycles=2, cycle_attempt=1, next_attempt_after="2026-11-26T15:00:00+00:00")
    assert not any(c == "site_blocked_repeatedly" for _, c in _codes(run_store.alerts_for("finished", None, first)))
    skipped = dict(first, gate_problems=["crawl refused before start: host is backing off"])
    assert not any(c == "site_blocked_repeatedly" for _, c in _codes(run_store.alerts_for("finished", None, skipped)))
    closing = run_store.alerts_for("finished", None, _report(blocked_cycles=3, cycle_attempt=0))
    msg = next(a["message"] for a in closing if a["code"] == "site_blocked_repeatedly")
    assert "3 consecutive scheduled cycles" in msg


def test_no_access_block_no_alert():
    rep = _report(); rep.pop("access")
    assert not any(c == "site_blocked_repeatedly" for _, c in _codes(run_store.alerts_for("finished", None, rep)))
    rep["access"] = None
    assert not any(c == "site_blocked_repeatedly" for _, c in _codes(run_store.alerts_for("finished", None, rep)))


# --------------------------------------------------------------------------- #
# delivery
# --------------------------------------------------------------------------- #

CRIT = [{"level": "critical", "code": "site_blocked_repeatedly", "message": run_store.blocked_message(ACCESS)}]
WARN = [{"level": "warning", "code": "run_untrusted", "message": "gate failed"}]
INFO = [{"level": "info", "code": "changes_pending_review", "message": "1 new"}]


class _Smtp:
    """Stands in for smtplib.SMTP / SMTP_SSL and records what was sent."""
    sent = []
    logins = []
    tls = 0

    def __init__(self, host, port, timeout=None):
        self.host, self.port = host, port

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def starttls(self):
        _Smtp.tls += 1

    def login(self, user, password):
        _Smtp.logins.append((user, password))

    def send_message(self, msg):
        _Smtp.sent.append(msg)


@pytest.fixture
def smtp(monkeypatch):
    _Smtp.sent, _Smtp.logins, _Smtp.tls = [], [], 0
    monkeypatch.setattr(alert_channels.smtplib, "SMTP", _Smtp)
    monkeypatch.setattr(alert_channels.smtplib, "SMTP_SSL", _Smtp)
    return _Smtp


def _email_env(monkeypatch, **extra):
    env = {"ALERT_CHANNEL": "email", "SMTP_HOST": "mail.example", "SMTP_PORT": "587",
           "SMTP_USER": "bot", "SMTP_PASSWORD": "pw", "ALERT_EMAIL_FROM": "uc2@example",
           "ALERT_EMAIL_TO": "ops@example, lead@example"}
    env.update(extra)
    for k in ("ALERT_CHANNEL", "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD",
              "ALERT_EMAIL_FROM", "ALERT_EMAIL_TO", "ALERT_EMAIL_LEVEL", "ALERT_WEBHOOK_URL"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)


def test_default_channel_is_none_and_sends_nothing(monkeypatch, smtp):
    monkeypatch.delenv("ALERT_CHANNEL", raising=False)
    assert alert_channels.send(1, "monitor_dfpi", CRIT) == "none"
    assert smtp.sent == []


def test_email_sends_the_critical_alert_with_the_one_sentence(monkeypatch, smtp):
    _email_env(monkeypatch)
    assert alert_channels.send(7, "monitor_dfpi", WARN + CRIT) == "email"
    assert len(smtp.sent) == 1 and smtp.tls == 1 and smtp.logins == [("bot", "pw")]
    msg = smtp.sent[0]
    assert msg["Subject"] == "UC-02 monitor_dfpi: CRITICAL site_blocked_repeatedly"
    assert msg["From"] == "uc2@example" and msg["To"] == "ops@example, lead@example"
    text = msg.get_content()
    assert "run 7" in text and "dfpi.ca.gov has refused the crawler in 2 consecutive scheduled cycles" in text
    assert "gate failed" not in text, "warnings are not mailed unless ALERT_EMAIL_LEVEL says so"


def test_email_level_warning_mails_warnings_too(monkeypatch, smtp):
    _email_env(monkeypatch, ALERT_EMAIL_LEVEL="warning")
    assert alert_channels.send(8, "monitor_sama", WARN) == "email"
    assert "gate failed" in smtp.sent[0].get_content()


@pytest.mark.parametrize("value", ["Critical", " CRITICAL ", "bogus"])
def test_email_level_is_normalised_and_an_unknown_value_mails_criticals_only(monkeypatch, smtp, value, caplog):
    _email_env(monkeypatch, ALERT_EMAIL_LEVEL=value)
    with caplog.at_level(logging.WARNING):
        assert alert_channels.send(8, "monitor_sama", WARN + CRIT) == "email"
    text = smtp.sent[0].get_content()
    assert "gate failed" not in text and "dfpi.ca.gov" in text
    assert ("not one of" in caplog.text) == (value == "bogus")


def test_a_bad_smtp_port_is_logged_and_stored_only(monkeypatch, smtp, caplog):
    _email_env(monkeypatch, SMTP_PORT="abc")
    with caplog.at_level(logging.WARNING):
        assert alert_channels.send(8, "monitor_sama", CRIT) == "none"
    assert smtp.sent == [] and "SMTP_PORT='abc' is not a number" in caplog.text


def test_email_sends_nothing_for_warnings_or_info_by_default(monkeypatch, smtp):
    _email_env(monkeypatch)
    assert alert_channels.send(9, "monitor_sama", WARN + INFO) == "none"
    assert smtp.sent == []


def test_email_without_settings_is_logged_and_stored_only(monkeypatch, smtp, caplog):
    _email_env(monkeypatch, SMTP_HOST="")
    with caplog.at_level(logging.WARNING):
        assert alert_channels.send(10, "monitor_dfpi", CRIT) == "none"
    assert smtp.sent == [] and "ALERT_EMAIL_TO is unset" in caplog.text


def test_email_failure_never_raises(monkeypatch, caplog):
    _email_env(monkeypatch)

    class Boom:
        def __init__(self, *a, **k):
            raise ConnectionRefusedError("no smtp here")
    monkeypatch.setattr(alert_channels.smtplib, "SMTP", Boom)
    with caplog.at_level(logging.WARNING):
        assert alert_channels.send(11, "monitor_dfpi", CRIT) == "none"
    assert "alert email failed" in caplog.text


def test_ssl_port_uses_smtp_ssl_without_starttls(monkeypatch, smtp):
    _email_env(monkeypatch, SMTP_PORT="465", SMTP_USER="", SMTP_PASSWORD="")
    assert alert_channels.send(12, "monitor_dfpi", CRIT) == "email"
    assert smtp.tls == 0 and smtp.logins == []


def test_webhook_keeps_its_shape_and_sends_warnings(monkeypatch):
    posted = []
    monkeypatch.setenv("ALERT_CHANNEL", "webhook")
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example/x")

    class R:
        @staticmethod
        def post(url, json=None, timeout=None):
            posted.append((url, json, timeout))
    monkeypatch.setitem(sys.modules, "requests", R)
    assert alert_channels.send(13, "monitor_sama", WARN + CRIT + INFO) == "webhook"
    url, body, timeout = posted[0]
    assert url == "https://hooks.example/x" and timeout == 5
    assert body == {"run_id": 13, "job": "monitor_sama", "alerts": WARN + CRIT}


def test_webhook_without_url_sends_nothing(monkeypatch):
    monkeypatch.setenv("ALERT_CHANNEL", "webhook")
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert alert_channels.send(14, "monitor_sama", CRIT) == "none"


@pytest.mark.parametrize("name", ["teams", "slack"])
def test_placeholders_log_and_keep_the_alert_stored(monkeypatch, smtp, name, caplog):
    monkeypatch.setenv("ALERT_CHANNEL", name)
    with caplog.at_level(logging.WARNING):
        assert alert_channels.send(15, "monitor_dfpi", CRIT) == "none"
    assert f"ALERT_CHANNEL={name} is a placeholder" in caplog.text and smtp.sent == []


def test_unknown_channel_falls_back_to_none(monkeypatch, caplog):
    monkeypatch.setenv("ALERT_CHANNEL", "pigeon")
    with caplog.at_level(logging.WARNING):
        assert alert_channels.send(16, "monitor_dfpi", CRIT) == "none"
    assert "not one of" in caplog.text


def test_notify_delegates_and_never_raises(monkeypatch, caplog):
    def boom(*a, **k):
        raise RuntimeError("channel down")
    monkeypatch.setattr(alert_channels, "send", boom)
    with caplog.at_level(logging.WARNING):
        run_store._notify(17, "monitor_dfpi", CRIT)
    assert "alert delivery failed" in caplog.text


# --------------------------------------------------------------------------- #
# the 6-hour re-attempt note
# --------------------------------------------------------------------------- #

def _due_in(hours):
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat(timespec="seconds")


def test_a_blocked_first_attempt_is_noted_with_its_due_time(tmp_path):
    p = tmp_path / "r.json"
    rep = _report(blocked_cycles=0, cycle_attempt=1, next_attempt_after=_due_in(6))
    assert reattempts.settle("monitor_dfpi", rep, p) == "noted"
    e = reattempts._read(p)["monitor_dfpi"]
    assert e["due"] == rep["access"]["next_attempt_after"] and e["host"] == "dfpi.ca.gov" and e["noted_at"]
    assert reattempts.due(p) == [], "not due yet"
    assert reattempts.due(p, now=datetime.now(timezone.utc) + timedelta(hours=6, minutes=1)) == ["monitor_dfpi"]


def test_a_closed_cycle_or_a_clean_run_removes_the_note(tmp_path):
    p = tmp_path / "r.json"
    assert reattempts.settle("monitor_dfpi", _report(blocked_cycles=1, cycle_attempt=0), p) is None, "nothing to remove"
    reattempts.settle("monitor_dfpi", _report(cycle_attempt=1, next_attempt_after=_due_in(6)), p)
    assert reattempts.settle("monitor_dfpi", _report(blocked_cycles=1, cycle_attempt=0), p) == "removed"
    reattempts.settle("monitor_dfpi", _report(cycle_attempt=1, next_attempt_after=_due_in(6)), p)
    assert reattempts.settle("monitor_dfpi", _report(blocked_cycles=0, cycle_attempt=0, next_attempt_after=None), p) == "removed"
    assert reattempts.settle("monitor_dfpi", None, p) is None
    assert reattempts._read(p) == {}


def test_a_run_skipped_on_the_crawl_lock_keeps_the_note_for_the_next_tick(tmp_path):
    p = tmp_path / "r.json"
    reattempts.settle("monitor_dfpi", _report(cycle_attempt=1, next_attempt_after=_due_in(-1)), p)
    assert reattempts.due(p) == ["monitor_dfpi"]
    reattempts.mark_started("monitor_dfpi", p)
    assert reattempts.due(p) == [], "a running job is not started twice"
    assert reattempts.settle("monitor_dfpi", {"skipped": True, "reason": "lock"}, p) == "kept"
    assert reattempts.due(p) == ["monitor_dfpi"], "the skip cleared started_at; the next tick tries again"
    assert reattempts.settle("monitor_other", {"skipped": True, "reason": "lock"}, p) is None


def test_a_job_of_several_reports_is_noted_from_the_blocked_one(tmp_path):
    p = tmp_path / "r.json"
    result = {"SAMA": _report(blocked_cycles=0, cycle_attempt=0, next_attempt_after=None),
              "CMA": _report(host="cma.org.sa", blocked_cycles=0, cycle_attempt=1, next_attempt_after=_due_in(6))}
    assert reattempts.settle("monitor_sama", result, p) == "noted"
    assert reattempts._read(p)["monitor_sama"]["host"] == "cma.org.sa"


def test_pop_removes_the_note_and_a_second_note_replaces_the_first(tmp_path):
    p = tmp_path / "r.json"
    reattempts.settle("monitor_dfpi", _report(cycle_attempt=1, next_attempt_after=_due_in(6)), p)
    later = _due_in(7)
    reattempts.settle("monitor_dfpi", _report(cycle_attempt=1, next_attempt_after=later), p)
    assert reattempts._read(p)["monitor_dfpi"]["due"] == later
    assert reattempts.pop("monitor_dfpi", p)["due"] == later
    assert reattempts._read(p) == {} and reattempts.pop("monitor_dfpi", p) is None


def test_an_unreadable_due_time_is_run_rather_than_held(tmp_path):
    p = tmp_path / "r.json"
    reattempts._write({"monitor_x": {"due": "not a date"}}, p)
    assert reattempts.due(p) == ["monitor_x"]


def test_a_naive_due_time_is_read_as_utc(tmp_path):
    p = tmp_path / "r.json"
    naive = (datetime.now(timezone.utc) + timedelta(minutes=5)).replace(tzinfo=None).isoformat(timespec="seconds")
    reattempts._write({"monitor_x": {"due": naive}}, p)
    assert reattempts.due(p) == [], "five minutes ahead in UTC is not due"
    assert reattempts.due(p, now=datetime.now(timezone.utc) + timedelta(minutes=6)) == ["monitor_x"]
