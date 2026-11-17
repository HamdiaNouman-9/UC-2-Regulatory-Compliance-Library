"""CHG-009: the alert when a regulator has been read N scheduled runs in a row
with nothing new or modified. No database: the run store is driven through
a fake cursor that answers the one SELECT the streak needs.

    .venv/Scripts/python.exe -m pytest tests/test_chg009_quiet_runs.py -q
"""
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage import run_store  # noqa: E402


def _report(regulator="CBB", new=0, modified=0, crawled=120, trusted=True, **extra):
    rep = {"regulator": regulator, "source": regulator.lower(), "crawled": crawled,
           "classified": {"new": new, "modified": modified, "unchanged": crawled - new - modified,
                          "disappeared": 0, "not_reread": 0},
           "changes": {"new": [], "modified": [], "deleted": []},
           "run_trustworthy": trusted, "gate_problems": [] if trusted else ["crawl stopped early"]}
    rep.update(extra)
    return rep


def _stored(*reports):
    """A stored summary: one report, or a dict of them for a multi-regulator job."""
    if len(reports) == 1:
        return json.dumps(reports[0])
    return json.dumps({r["regulator"]: r for r in reports})


# --------------------------------------------------------------------------- #
# what counts as a quiet reading
# --------------------------------------------------------------------------- #

def test_a_trusted_read_with_nothing_new_or_modified_is_quiet():
    assert run_store.quiet_reading(_report()) is True


@pytest.mark.parametrize("kw", [{"new": 1}, {"modified": 2}, {"new": 3, "modified": 1}])
def test_a_read_that_found_something_is_not_quiet(kw):
    assert run_store.quiet_reading(_report(**kw)) is False


def test_disappeared_documents_alone_do_not_count_as_an_update():
    rep = _report()
    rep["classified"]["disappeared"] = 2
    assert run_store.quiet_reading(rep) is True


@pytest.mark.parametrize("kw", [{"trusted": False}, {"crawled": 0}, {"crawled": 0, "trusted": False}])
def test_a_run_that_could_not_read_the_site_is_not_a_reading(kw):
    assert run_store.quiet_reading(_report(**kw)) is None


# --------------------------------------------------------------------------- #
# the configured limit
# --------------------------------------------------------------------------- #

def test_the_limit_defaults_to_four():
    assert run_store.quiet_limit("CBB", {}) == 4
    assert run_store.quiet_limit("CBB", {"default_days": 30}) == 4


def test_the_limit_can_be_one_number_or_a_block_with_exceptions():
    assert run_store.quiet_limit("CBB", {"quiet_runs": 2}) == 2
    cfg = {"quiet_runs": {"default": 4, "regulators": {"SECP": 2}}}
    assert run_store.quiet_limit("CBB", cfg) == 4
    assert run_store.quiet_limit("SECP", cfg) == 2


def test_zero_turns_the_alert_off_and_junk_falls_back_to_the_default():
    assert run_store.quiet_limit("CBB", {"quiet_runs": 0}) == 0
    assert run_store.quiet_limit("CBB", {"quiet_runs": {"default": 0}}) == 0
    assert run_store.quiet_limit("CBB", {"quiet_runs": "four"}) == 4
    assert run_store.quiet_limit("CBB", {"quiet_runs": None}) == 4
    assert run_store.quiet_limit("CBB", {"quiet_runs": -3}) == 0


# --------------------------------------------------------------------------- #
# the streak
# --------------------------------------------------------------------------- #

def test_the_first_quiet_run_starts_at_one_and_a_change_resets():
    assert run_store.quiet_runs([], "CBB", _report()) == 1
    assert run_store.quiet_runs([], "CBB", _report(new=1)) == 0


def test_the_newest_stored_report_carries_the_count_forward():
    previous = [_stored(_report(quiet_runs=3)), _stored(_report(quiet_runs=2))]
    assert run_store.quiet_runs(previous, "CBB", _report()) == 4
    assert run_store.quiet_runs(previous, "CBB", _report(modified=1)) == 0


def test_a_run_that_could_not_read_the_site_carries_the_count_unchanged():
    previous = [_stored(_report(quiet_runs=3))]
    assert run_store.quiet_runs(previous, "CBB", _report(trusted=False)) == 3
    assert run_store.quiet_runs(previous, "CBB", _report(crawled=0)) == 3


def test_rows_without_the_regulator_or_without_a_report_are_skipped():
    """A failed run stores no report; a multi-regulator job's summary holds
    several. The count comes from the newest row that has this regulator."""
    previous = ["{}", _stored(_report("SECP", quiet_runs=9)),
                _stored(_report("SECP", quiet_runs=8), _report("CBB", quiet_runs=2)), "not json"]
    assert run_store.quiet_runs(previous, "CBB", _report()) == 3
    assert run_store.quiet_runs(previous, "SECP", _report("SECP")) == 10


def test_reports_stored_before_this_change_start_the_count_at_zero():
    previous = [_stored(_report())]                 # no quiet_runs key on it
    assert run_store.quiet_runs(previous, "CBB", _report()) == 1


# --------------------------------------------------------------------------- #
# the alert rule
# --------------------------------------------------------------------------- #

def _codes(alerts):
    return [(a["level"], a["code"]) for a in alerts]


def test_the_alert_comes_at_the_limit_and_on_every_quiet_run_after_it():
    for n in (1, 2, 3):
        assert ("critical", "no_updates_for_runs") not in _codes(
            run_store.alerts_for("finished", None, _report(quiet_runs=n, quiet_limit=4)))
    for n in (4, 5, 9):
        alerts = run_store.alerts_for("finished", None, _report(quiet_runs=n, quiet_limit=4))
        assert ("critical", "no_updates_for_runs") in _codes(alerts)
        assert run_store._level(alerts) == "critical"


def test_the_message_names_the_regulator_the_count_and_the_limit():
    alerts = run_store.alerts_for("finished", None, _report(quiet_runs=4, quiet_limit=4))
    msg = next(a["message"] for a in alerts if a["code"] == "no_updates_for_runs")
    assert msg.startswith("CBB has returned no new or modified document in 4 consecutive runs (limit 4).")
    assert "config/staleness.yml" in msg


def test_a_limit_of_zero_or_an_unstamped_report_never_alerts():
    assert ("critical", "no_updates_for_runs") not in _codes(
        run_store.alerts_for("finished", None, _report(quiet_runs=40, quiet_limit=0)))
    assert ("critical", "no_updates_for_runs") not in _codes(
        run_store.alerts_for("finished", None, _report()))


# --------------------------------------------------------------------------- #
# save_run end to end, through a fake cursor
# --------------------------------------------------------------------------- #

class _Cursor:
    def __init__(self, previous):
        self.previous = previous
        self.executed = []
        self.inserted = []

    def execute(self, sql, *args):
        self.executed.append((sql, args))
        if sql.startswith("INSERT INTO run_results"):
            self.inserted.append(args)

    def fetchone(self):
        return (7,)                                 # the new run_id

    def fetchall(self):
        return [(s,) for s in self.previous] if "summary FROM run_results" in self.executed[-1][0] else []


class _Conn:
    def __init__(self, cur):
        self.cur = cur

    def cursor(self):
        return self.cur

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Repo:
    def __init__(self, previous):
        self.cur = _Cursor(previous)

    def _get_conn(self):
        return _Conn(self.cur)


def _saved(repo):
    row = repo.cur.inserted[-1]
    return json.loads(row[7]), row[8], json.loads(row[9])      # summary, alert_level, alerts


def _lookbacks(repo):
    return [(sql, args) for sql, args in repo.cur.executed if "summary FROM run_results" in sql]


@pytest.fixture
def quiet_env(monkeypatch):
    """No delivery, and a known config: limit 4, SECP 2."""
    import jobs.staleness_alert as sa
    monkeypatch.setenv("ALERT_CHANNEL", "none")
    monkeypatch.setattr(sa, "load_config",
                        lambda path=None: {"quiet_runs": {"default": 4, "regulators": {"SECP": 2}}})


def test_save_run_stamps_the_count_and_stores_the_alert(quiet_env):
    repo = _Repo([_stored(_report(quiet_runs=3))])
    rep = _report()
    run_id = run_store.save_run(repo, "monitor_cbb", ["CBB"], "finished",
                                "2026-11-17T09:00:00", "2026-11-17T09:10:00", None, rep)
    assert run_id == 7
    summary, level, alerts = _saved(repo)
    assert summary["quiet_runs"] == 4 and summary["quiet_limit"] == 4
    assert rep["quiet_runs"] == 4                   # the caller's report carries it too
    assert level == "critical"
    assert ("critical", "no_updates_for_runs") in _codes(alerts)
    assert [args for _, args in _lookbacks(repo)] == [(run_store.QUIET_LOOKBACK_ROWS, "monitor_cbb")]


def test_one_quiet_run_short_of_the_limit_is_stored_without_an_alert(quiet_env):
    repo = _Repo([_stored(_report(quiet_runs=2))])
    run_store.save_run(repo, "monitor_cbb", ["CBB"], "finished",
                       "2026-11-17T09:00:00", "2026-11-17T09:10:00", None, _report())
    summary, level, alerts = _saved(repo)
    assert summary["quiet_runs"] == 3 and level == "none" and alerts == []


def test_save_run_counts_each_regulator_of_a_job_at_its_own_limit(quiet_env):
    repo = _Repo([_stored(_report("CBB", quiet_runs=1), _report("SECP", quiet_runs=1))])
    result = {"CBB": _report("CBB"), "SECP": _report("SECP")}
    run_store.save_run(repo, "monitor_gulf", ["CBB", "SECP"], "finished",
                       "2026-11-17T09:00:00", "2026-11-17T09:10:00", None, result)
    summary, level, alerts = _saved(repo)
    assert (summary["CBB"]["quiet_runs"], summary["CBB"]["quiet_limit"]) == (2, 4)
    assert (summary["SECP"]["quiet_runs"], summary["SECP"]["quiet_limit"]) == (2, 2)
    assert level == "critical"
    assert [a["message"][:4] for a in alerts if a["code"] == "no_updates_for_runs"] == ["SECP"]


def test_a_failed_run_stores_nothing_about_quietness(quiet_env):
    repo = _Repo([_stored(_report(quiet_runs=3))])
    run_store.save_run(repo, "monitor_cbb", ["CBB"], "failed",
                       "2026-11-17T09:00:00", "2026-11-17T09:10:00", "RuntimeError: x", None)
    summary, level, alerts = _saved(repo)
    assert summary == {} and level == "critical"
    assert _codes(alerts) == [("critical", "job_failed")]
    assert _lookbacks(repo) == []


def test_a_failed_lookback_keeps_the_run_record_and_counts_from_zero(quiet_env, caplog):
    repo = _Repo([_stored(_report(quiet_runs=3))])

    def boom(sql, *args):
        if "summary FROM run_results" in sql:
            raise RuntimeError("timeout")
        _Cursor.execute(repo.cur, sql, *args)
    repo.cur.execute = boom
    with caplog.at_level("WARNING", logger="storage.run_store"):
        run_id = run_store.save_run(repo, "monitor_cbb", ["CBB"], "finished",
                                    "2026-11-17T09:00:00", "2026-11-17T09:10:00", None, _report())
    assert run_id == 7
    summary, level, alerts = _saved(repo)
    assert (summary["quiet_runs"], level, alerts) == (1, "none", [])
    assert "quiet_runs lookback for monitor_cbb failed" in caplog.text


def test_a_missing_config_file_means_the_default_limit(monkeypatch, tmp_path, caplog):
    import jobs.staleness_alert as sa
    monkeypatch.setenv("ALERT_CHANNEL", "none")
    monkeypatch.setattr(sa.load_config, "__defaults__", (tmp_path / "absent.yml",))
    monkeypatch.setattr(run_store, "QUIET_RUNS_DEFAULT", 9)
    repo = _Repo([])
    with caplog.at_level("WARNING", logger="storage.run_store"):
        run_store.save_run(repo, "monitor_cbb", ["CBB"], "finished",
                           "2026-11-17T09:00:00", "2026-11-17T09:10:00", None, _report())
    summary, _, _ = _saved(repo)
    assert (summary["quiet_runs"], summary["quiet_limit"]) == (1, 9)
    assert "config/staleness.yml not read" in caplog.text
