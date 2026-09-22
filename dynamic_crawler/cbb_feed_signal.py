"""CBB's own "what changed" page, used to decide WHETHER the multi-hour crawl runs.

    https://cbben.thomsonreuters.com/view-revision-updates
      ?f_date=on&changed_1[min]=YYYY-MM-DD&changed_1[max]=YYYY-MM-DD&items_per_page=40

THE FILTER IS `changed_1[min]` / `changed_1[max]`, NOT `min` / `max`. MEASURED 2026-09-21: the
plain `min`/`max` inputs are on the page but IGNORED -- a window entirely in the future
returned the same 40 newest entries as today's. crawler/cbb_monitoring_crawler.py sends
them and compensates by filtering rows client-side. This module uses the working names
(the same as SAMA's) and still checks every returned date against the window, so a
filter that stops working is an error and not a silent "everything changed".

The rulebook is hosted on Thomson Reuters, the same platform as SAMA's (see
sama_feed_signal.py). One paged GET answers "which sections were revised between
these dates". A full CBB crawl is thousands of sequential requests and many hours,
so running it daily to learn that nothing changed is the expensive way round.

WHAT THIS DECIDES, AND WHAT IT DOES NOT

    feed shows revisions since the last full crawl   -> crawl
    feed shows none, last full crawl is recent       -> skip the crawl
    last full crawl is older than FULL_CRAWL_EVERY_DAYS -> crawl anyway
    feed could not be read                           -> UNAVAILABLE: no crawl, and said so
    no full crawl ever recorded                      -> crawl

It is a GATE, not a shortlist: CBB's rulebook sections carry no url of their own
(cbb.yml, change_signals.yml note 2), so a changed section cannot be re-fetched by
address; the source has to be re-walked. What the gate saves is every run in which
nothing changed.

IT CANNOT SEE DELETIONS. A withdrawn section simply stops appearing. That is what
FULL_CRAWL_EVERY_DAYS is for: absence is only ever found by a full crawl.

A FEED THAT FAILS MUST NOT READ AS "NOTHING CHANGED" -- it is reported as UNAVAILABLE. The site answers a client
without browser-like headers with a bare `403 Forbidden`; measured 2026-09-21. The
old helper (`crawler.cbb_monitoring_crawler._get_thomson_reuters_changes`) stops
and returns an empty list on any bad response, which is indistinguishable from a
quiet fortnight. This module raises `FeedUnavailable` instead.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

FEED_URL = "https://cbben.thomsonreuters.com/view-revision-updates"

#: Fixed. Drupal caps a page at 40 and a larger value can answer 200 with nothing
#: (SAMA measured 50 -> 0 entries), which would read as "no changes".
PAGE_SIZE = 40
MAX_PAGES = 100
REQUEST_DELAY = 1.0
RETRIES = 3

#: A full crawl at least this often, whatever the feed says. Deletions are only
#: visible to a crawl, and the crawl's own withdrawal rule needs repeated absence.
FULL_CRAWL_EVERY_DAYS = 30

#: The window is opened this many days before the last full crawl so a revision
#: dated the day of that crawl (time zones, a crawl that ran mid-day) is not lost.
OVERLAP_DAYS = 1

STATE_PATH = Path(__file__).resolve().parents[1] / "output" / "change_state" / "cbb_feed.json"

_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


class FeedUnavailable(RuntimeError):
    """The feed could not be read. NOT the same as 'nothing changed'."""


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


def _get(session: requests.Session, params: dict) -> BeautifulSoup:
    last: Optional[Exception] = None
    for attempt in range(RETRIES):
        if attempt:
            time.sleep(2.0 * attempt)
        try:
            r = session.get(FEED_URL, params=params, timeout=30)
            if r.status_code != 200:
                raise FeedUnavailable(f"HTTP {r.status_code} from the revision feed")
            soup = BeautifulSoup(r.text, "html.parser")
            if not _looks_like_the_feed(soup):
                raise FeedUnavailable("the page is not the revision feed "
                                      f"({len(r.text)} bytes, no results area or filter form)")
            return soup
        except (requests.RequestException, FeedUnavailable) as e:
            last = e
            logger.warning("CBB feed attempt %d/%d failed: %s", attempt + 1, RETRIES, e)
    raise FeedUnavailable(str(last))


def _looks_like_the_feed(soup: BeautifulSoup) -> bool:
    """A 200 that is not the feed (a block page, a maintenance page) must not pass
    as an empty window. The results area, or the date filter that is always on the
    page, has to be present."""
    return bool(soup.find("div", class_="view-content")
                or soup.find("input", attrs={"name": "min"})
                or soup.find("div", class_="view-empty")
                or soup.select_one("div.view-revision-updates"))


def _entry_date(row) -> Optional[_dt.date]:
    d = row.find("div", class_="book-detail")
    t = d.find("time", attrs={"datetime": True}) if d else None
    if not t:
        return None
    try:
        return _dt.datetime.fromisoformat(t["datetime"]).date()
    except ValueError:
        return None


def fetch_changes(since: _dt.date, until: Optional[_dt.date] = None,
                  session: Optional[requests.Session] = None,
                  max_pages: Optional[int] = None) -> List[Dict]:
    """Sections revised in [since, until] (both days included). Raises FeedUnavailable on
    any failure. `max_pages` stops early and returns what was read: enough to answer
    "did anything change", not to count it.

    The site treats changed_1[max] as END-EXCLUSIVE (measured 2026-09-21: 14..14 is empty,
    14..15 holds the 14th), so the request sends until + 1 day."""
    until = until or _dt.date.today()
    session = session or _session()
    out: List[Dict] = []
    seen = set()
    for page in range(MAX_PAGES):
        params = {"f_date": "on", "changed_1[min]": since.isoformat(),
                  "changed_1[max]": (until + _dt.timedelta(days=1)).isoformat(),
                  "items_per_page": str(PAGE_SIZE)}
        if page:
            params["page"] = str(page)
        soup = _get(session, params)
        rows = soup.select("div.view-content div.views-row")
        for row in rows:
            d = row.find("div", class_="book-detail")
            a = d.find("a", href=True) if d else None
            if not a:
                continue
            href = a["href"]
            if href in seen:
                continue
            seen.add(href)
            when = _entry_date(row)
            if when is not None and not (since <= when <= until):
                raise FeedUnavailable(
                    f"entry dated {when} is outside the requested window {since}..{until}; "
                    f"the date filter is not being applied")
            trail = row.find("div", class_="book-trail")
            out.append({"title": a.get_text(strip=True), "href": href,
                        "changed": when.isoformat() if when else None,
                        "trail": trail.get_text(strip=True) if trail else ""})
        if len(rows) < PAGE_SIZE:
            return out
        if max_pages and page + 1 >= max_pages:
            return out
        time.sleep(REQUEST_DELAY)
    raise FeedUnavailable(f"still more results after {MAX_PAGES} pages; refusing to treat "
                          f"a truncated read as complete")


# --------------------------------------------------------------------------- #
#  state + decision                                                            #
# --------------------------------------------------------------------------- #

def load_state(path: Path = STATE_PATH) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_state(state: dict, path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def record_full_crawl(when: Optional[_dt.date] = None, path: Path = STATE_PATH) -> None:
    """Call ONLY after a full crawl finished and was trustworthy."""
    st = load_state(path)
    st["last_full_crawl"] = (when or _dt.date.today()).isoformat()
    save_state(st, path)


def decide(state: dict, today: Optional[_dt.date] = None, *,
           fetch=fetch_changes, every_days: int = FULL_CRAWL_EVERY_DAYS) -> dict:
    """{'crawl': bool, 'status': 'crawl'|'skip'|'unavailable', 'reason': str, 'changes': ...}"""
    today = today or _dt.date.today()
    raw = state.get("last_full_crawl")
    try:
        last = _dt.date.fromisoformat(raw) if raw else None
    except ValueError:
        last = None
    if last is None:
        return {"crawl": True, "status": "crawl", "reason": "no full crawl recorded yet", "changes": None}
    age = (today - last).days
    if age >= every_days:
        return {"crawl": True, "status": "crawl", "changes": None, "age_days": age,
                "reason": f"last full crawl {age} days ago (>= {every_days}); "
                          f"deletions are only visible to a crawl"}
    try:
        changes = fetch(last - _dt.timedelta(days=OVERLAP_DAYS), today, max_pages=1)
    except FeedUnavailable as e:
        return {"crawl": False, "status": "unavailable", "changes": None, "age_days": age,
                "reason": f"feed unavailable ({e}); nothing was crawled and nothing is "
                          f"assumed about what changed"}
    if changes:
        return {"crawl": True, "status": "crawl", "changes": len(changes), "age_days": age,
                "reason": f"{len(changes)}{'+' if len(changes) >= PAGE_SIZE else ''} section(s) revised since {last}",
                "sample": [f"{c['title']} ({c['changed']})" for c in changes[:5]]}
    return {"crawl": False, "status": "skip", "changes": 0, "age_days": age,
            "reason": f"feed shows no revisions since {last}; full crawl not due for "
                      f"{every_days - age} more day(s)"}
