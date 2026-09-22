from datetime import datetime, timedelta, timezone

from jobs.staleness_alert import find_stale

NOW = datetime(2026, 9, 21, tzinfo=timezone.utc)


def test_default_interval_flags_only_the_quiet_regulator():
    seen = {"A": NOW - timedelta(days=40), "B": NOW - timedelta(days=5)}
    stale = find_stale(seen, {"default_days": 30}, NOW)
    assert [s["regulator"] for s in stale] == ["A"]
    assert stale[0]["days_since"] == 40.0


def test_per_regulator_override_and_never_updated():
    seen = {"A": NOW - timedelta(days=10)}
    cfg = {"default_days": 30, "regulators": {"A": 7, "B": {"days": 3}}}
    stale = find_stale(seen, cfg, NOW)
    assert {s["regulator"] for s in stale} == {"A", "B"}
    assert next(s for s in stale if s["regulator"] == "B")["last_update"] is None
