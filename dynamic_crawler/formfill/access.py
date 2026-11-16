"""CHG-009: how the generic crawler approaches a site, and when it stops.

Three questions, answered here and nowhere else:

  * How long to wait before the next navigation -- `pace_for(url, hints)`,
    read from config/pace.yml, drawn fresh each time so no two pauses match.
  * Whether a response was a refusal -- `classify(...)`: HTTP 403 / 429 / 503,
    a Retry-After header, or the bot-protection wall text that runner._blocked
    has matched since 2026-08-02.
  * Whether a host may be visited at all -- `AccessState(host)`: the per-cycle
    routine (one attempt, one more 2 minutes later inside it, one more 6 hours
    later, then nothing until the next scheduled cycle) persisted under
    output/access/<host>.json so the wait is inspectable, and the count of
    consecutive blocked cycles that earns a person the alert after two.

Why this exists: four hosts banned this project's address after a few hours of
ordinary automated access (simah.com, saudiexchange.sa, hhs.gov, dfpi.ca.gov),
and in every case the runner had retried refusals at once, paced nothing, and
carried on after the first block. See docs/chg_009_bot_protection_backoff.md.

What already existed, and is reused rather than rewritten:

  * `utils/pacer.py` (HamdiaNouman-9, 2026-10-27) reads config/pace.yml for the
    requests-based crawlers (CBI, CBK Kenya): `wait_turn` paces by host level,
    `classify` names an answer ok / throttle / block / unknown, and a refused
    host is remembered for the rest of the process. `classify` is called from
    here so the Playwright runner and the requests crawlers agree on what a
    refusal is, including the `throttle_only` hosts whose 403/404/429 are
    their own retries' business.
  * `generic_crawler/blockcheck.py` holds the one list of wall phrases.

What this adds for a browser run: a jittered pause (pacer's pause is the level
in whole seconds, the same every time), a per-form override from the hints
file, a Retry-After that understands HTTP dates, and a block memory that
survives the process, since pacer's memory of a refusal ends with the run.

Nothing here solves a CAPTCHA or spoofs a fingerprint. A host that still
refuses after this is asked for an allowlist entry.
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import yaml

from utils import pacer

REPO_ROOT = Path(__file__).resolve().parents[2]
PACE_PATH = REPO_ROOT / "config" / "pace.yml"
ACCESS_DIR = REPO_ROOT / "output" / "access"
# One browser profile per host, so the cookies a site sets -- a Cloudflare
# clearance among them -- survive from one run to the next. Not committed.
PROFILE_DIR = REPO_ROOT / "output" / "profiles"

# Playwright's default, stated rather than assumed. The first plan was 1440x900
# as "a visitor's laptop"; the first live run (MHRSD, 2026-11-10) then found no
# "next" control on a pager that two recorded runs had walked, and every form's
# selectors were verified at 1280x720 -- a responsive pager is exactly what a
# width change breaks. The forms' width stays; a per-form `viewport:` can
# change it where someone has verified the form at another size.
VIEWPORT = {"width": 1280, "height": 720}
DEFAULT_LOCALE = "en-US"

# The pause taken before a navigation when config/pace.yml says nothing: a
# range, not a number, because a visitor never waits the same time twice.
DEFAULT_BASE_MS = (1500, 3500)
DEFAULT_LEVELS = {"normal": 1, "careful": 3, "very_careful": 5}

# The pause before the ONE in-run retry a first refusal earns: the user's
# "two minutes" (2026-11-16), long enough for a burst rule to expire, short
# enough that a scheduled job does not hold the global lock for long. A
# range so the tests can narrow it; both ends are the same in production.
RETRY_PAUSE_MS = (120_000, 120_000)

# The per-cycle routine (the user's word, 2026-11-16, replacing the 6h..14d
# ladder of steps 1 to 8): after the first blocked attempt of a scheduled
# cycle the host is left alone for REATTEMPT_HOURS, then tried once more
# without the in-run retry; a second block closes the cycle as blocked and
# nothing touches the host until the next scheduled cycle. A cycle whose
# re-attempt never came within CYCLE_STALE_HOURS (the API was down) is closed
# as blocked by the next attempt, so the count stays honest. After two
# consecutive blocked cycles the run store raises the critical alert
# (storage/run_store.py, ESCALATE_AFTER_CYCLES) and a person decides; the
# routine itself does not stop.
REATTEMPT_HOURS = 6
CYCLE_STALE_HOURS = 24

# How long a browser-side challenge (Cloudflare's "Just a moment" page, served
# as a 403 with `cf-mitigated: challenge`) is given to finish before the page
# is read. The DFPI crawler, which lives behind one, waits 30 s and records
# that it clears in about 4.5 s in a visible browser; headless it never
# clears (crawler/dfpi_crawler.py, measured 2026-10-08), and the wait then
# costs the one page its 30 s before the refusal is called.
CHALLENGE_WAIT_MS = 30_000

# The visitor script (step 8, the user's "adopt scoped", 2026-11-12). Four
# lines copied from crawler/dfpi_crawler.py (`_STEALTH`), the one setting that
# clears Cloudflare's managed challenge on dfpi.ca.gov, measured eleven times
# that day: nothing else did, visible or headless. It is applied ONLY to a
# retry after a challenge that did not clear, and to later runs on a host
# that needed it within SCRIPT_MEMORY_DAYS; a host that never challenges never
# runs it. What it tells the site that is not true of this browser: that
# `navigator.webdriver` is undefined (a real Chrome says `false`), and that a
# `chrome.runtime` object exists on an ordinary page (a real Chrome has none).
# The plugin list and the languages it sets match what a visible Chrome on the
# same machine reports. Measured again whenever Playwright is upgraded.
VISITOR_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins',
  {get: () => [1,2,3,4,5].map(i => ({name: 'Plugin ' + i}))});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = window.chrome || {runtime: {}};
"""
SCRIPT_MEMORY_DAYS = 30


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def _parse(ts: str | None) -> datetime | None:
    try:
        return datetime.fromisoformat(ts) if ts else None
    except ValueError:
        return None


def host_of(url: str) -> str:
    """`https://www.cma.gov.sa/en/x` -> `www.cma.gov.sa`; '' when unparsable."""
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


# --------------------------------------------------------------------------- #
# Pacing
# --------------------------------------------------------------------------- #

@dataclass
class PaceConfig:
    """config/pace.yml, validated once. A bad file fails here, at load, not in
    the middle of a crawl."""
    base_ms: tuple[int, int] = DEFAULT_BASE_MS
    default: str = "normal"
    levels: dict = field(default_factory=lambda: dict(DEFAULT_LEVELS))
    hosts: dict = field(default_factory=dict)
    throttle_only: frozenset = frozenset()

    @classmethod
    def load(cls, path: str | Path | None = None) -> "PaceConfig":
        p = Path(path or PACE_PATH)
        if not p.exists():
            return cls()
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"{p}: expected a mapping at the top level")

        base = raw.get("base_ms", DEFAULT_BASE_MS)
        if isinstance(base, (int, float)):
            base = (int(base), int(base))
        if (not isinstance(base, (list, tuple)) or len(base) != 2
                or not all(isinstance(x, (int, float)) and x >= 0 for x in base)
                or base[0] > base[1]):
            raise ValueError(f"{p}: base_ms must be [low_ms, high_ms] with low <= high")

        levels = dict(DEFAULT_LEVELS)
        for name, mult in (raw.get("levels") or {}).items():
            if not isinstance(mult, (int, float)) or mult <= 0:
                raise ValueError(f"{p}: level {name!r} must be a positive number")
            levels[str(name)] = mult

        default = str(raw.get("default", "normal")).strip()
        if default not in levels:
            raise ValueError(f"{p}: default level {default!r} is not in levels")

        hosts = {}
        for host, level in (raw.get("hosts") or {}).items():
            level = str(level).strip()
            if level not in levels:
                raise ValueError(f"{p}: host {host!r} names unknown level {level!r}")
            hosts[str(host).strip().lower()] = level

        throttle = frozenset(str(h).strip().lower()
                             for h in (raw.get("throttle_only") or []) if str(h).strip())

        return cls(base_ms=(int(base[0]), int(base[1])), default=default,
                   levels=levels, hosts=hosts, throttle_only=throttle)

    def _match(self, table, host: str):
        """The host itself, then each parent domain: `www.cma.gov.sa` matches a
        `cma.gov.sa` entry. The most specific match wins."""
        parts = host.split(".")
        for i in range(len(parts) - 1):
            candidate = ".".join(parts[i:])
            if candidate in table:
                return candidate
        return None

    def level_for(self, host: str) -> str:
        key = self._match(self.hosts, host)
        return self.hosts[key] if key else self.default

    def is_throttle_only(self, host: str) -> bool:
        return self._match(self.throttle_only, host) is not None


@dataclass
class Pace:
    """What the runner needs to know before touching one host."""
    host: str
    level: str
    multiplier: float
    base_ms: tuple[int, int]
    throttle_only: bool = False

    def draw(self, rng: random.Random | None = None) -> int:
        """Milliseconds to wait before the next navigation. A fresh draw every
        call: a fixed interval is the first thing a rate rule looks for."""
        lo, hi = self.base_ms
        r = (rng or random).uniform(lo, hi)
        return int(r * self.multiplier)


def pace_for(url: str, hints: dict | None = None,
             config: PaceConfig | None = None) -> Pace:
    """The pace for `url`. `pace:` in the hints file names a level and overrides
    the host table for that one form; the host table overrides the default."""
    cfg = config or PaceConfig.load()
    host = host_of(url)
    level = cfg.level_for(host)
    override = (hints or {}).get("pace")
    if override:
        override = str(override).strip()
        if override not in cfg.levels:
            raise ValueError(f"hints pace {override!r} is not a level in {PACE_PATH.name}")
        level = override
    return Pace(host=host, level=level, multiplier=float(cfg.levels[level]),
                base_ms=cfg.base_ms, throttle_only=cfg.is_throttle_only(host))


def retry_after_ms(header: str | None, default_ms: int) -> int:
    """`Retry-After` as milliseconds: seconds, or an HTTP date. Anything else,
    or anything over an hour, falls back to `default_ms` -- a site may ask for
    an hour's silence and get it, but not a week's inside one run."""
    if not header:
        return default_ms
    h = header.strip()
    secs: float | None = None
    if h.isdigit():
        secs = float(h)
    else:
        try:
            from email.utils import parsedate_to_datetime
            secs = (parsedate_to_datetime(h) - _now()).total_seconds()
        except Exception:
            secs = None
    if secs is None or secs <= 0:
        return default_ms
    return int(min(secs, 3600) * 1000)


# --------------------------------------------------------------------------- #
# Refusals
# --------------------------------------------------------------------------- #

@dataclass
class Refusal:
    """Why a response is not a page. `reason` is '' for an ordinary page.

    `kind` is pacer's word: `throttle` (the site asked for a wait), `block`
    (a wall page) or `unknown` (a bare 403). A `Refusal` is false for a page.
    """
    reason: str = ""
    kind: str = ""
    status: int = 0
    retry_after: str | None = None
    #: the answer was a challenge interstitial that did not clear in its time:
    #: the one refusal that earns the retry with the visitor script
    challenge: bool = False

    def __bool__(self) -> bool:
        return bool(self.reason)

    @property
    def throttled(self) -> bool:
        return self.kind == "throttle"


def classify(url: str, status: int | None, headers, title: str, body: str) -> Refusal:
    """Name a refusal from what came back, by `utils.pacer.classify`, so one
    definition serves the browser runner and the requests crawlers. Status
    counts as well as text: a 403 with an empty body is a refusal, and the old
    text-only check called it 'a page that failed to load' and retried it."""
    kind, reason = pacer.classify(url, status, headers, title or "", (body or "")[:3000])
    retry_after = None
    if headers:
        try:
            retry_after = headers.get("Retry-After") or headers.get("retry-after")
        except Exception:
            retry_after = None
    if kind == "ok":
        return Refusal("", "", int(status or 0), retry_after)
    return Refusal(reason, kind, int(status or 0), retry_after)


# --------------------------------------------------------------------------- #
# Per-host state
# --------------------------------------------------------------------------- #

class AccessState:
    """The clock for one host, persisted so a scheduled run can read it.

    Manifest fields the routine reads and writes:
      consecutive_blocks  blocked attempts in a row, any cycle
      cycle_attempt       blocked attempts in the current cycle (0, 1); 2 closes it
      blocked_cycles      consecutive scheduled cycles that ended blocked
      first_blocked_at    when the streak began, for the alert text
      next_attempt_after  set for REATTEMPT_HOURS after the first block of a
                          cycle, None otherwise
    A clean run resets all of them. The file is the record -- a person can
    see why a job did nothing and how many cycles a host has refused.
    """

    def __init__(self, host: str, directory: str | Path | None = None):
        self.host = (host or "").lower() or "unknown"
        self.dir = Path(directory or ACCESS_DIR)
        self.path = self.dir / f"{self.host}.json"

    def manifest(self) -> dict:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _write(self, m: dict) -> dict:
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(m, indent=2), encoding="utf-8")
        return m

    def may_attempt(self) -> tuple[bool, str]:
        """(allowed, why). Refused only inside the 6-hour window after the
        first blocked attempt of a cycle; a host with any number of blocked
        cycles behind it is tried again on its next scheduled cycle."""
        m = self.manifest()
        after = _parse(m.get("next_attempt_after"))
        if after and _now() < after:
            n = int(m.get("consecutive_blocks", 0))
            return False, (f"{self.host}: blocked {n} time(s) in a row "
                           f"({m.get('last_block_reason', '?')!r}); "
                           f"next attempt after {m['next_attempt_after']}")
        return True, ""

    def attempt_number(self) -> int:
        """Which attempt of the cycle a run starting now would be: 1 (the
        scheduled attempt, which carries the in-run retry) or 2 (the
        re-attempt REATTEMPT_HOURS later, one load and no retry). A cycle
        left open longer than CYCLE_STALE_HOURS past its window counts as a
        fresh cycle; `record_block` closes the old one then."""
        m = self.manifest()
        if int(m.get("cycle_attempt", 0)) != 1:
            return 1
        return 1 if self._cycle_stale(m) else 2

    @staticmethod
    def _cycle_stale(m: dict) -> bool:
        after = _parse(m.get("next_attempt_after"))
        return bool(after) and _now() >= after + timedelta(hours=CYCLE_STALE_HOURS)

    def record_block(self, reason: str) -> dict:
        """A blocked attempt. The first of a cycle opens the 6-hour window;
        the second closes the cycle as blocked and clears the window, so the
        next scheduled run goes ahead and starts a new cycle. Returns the
        manifest, whose `blocked_cycles` the run block and the alert read."""
        m = self.manifest()
        now = _now()
        cycles = int(m.get("blocked_cycles", 0))
        attempt = int(m.get("cycle_attempt", 0)) + 1
        if attempt == 2 and self._cycle_stale(m):
            # The re-attempt never came; the old cycle is closed as blocked
            # and this block is the first attempt of a new one.
            cycles += 1
            attempt = 1
        if attempt == 1:
            after = _iso(now + timedelta(hours=REATTEMPT_HOURS))
            cycle_attempt = 1
        else:
            after = None
            cycles += 1
            cycle_attempt = 0
        m.update({
            "host": self.host,
            "last_attempt_at": _iso(now),
            "last_attempt_result": "blocked",
            "last_block_reason": reason,
            "consecutive_blocks": int(m.get("consecutive_blocks", 0)) + 1,
            "cycle_attempt": cycle_attempt,
            "blocked_cycles": cycles,
            "first_blocked_at": m.get("first_blocked_at") or _iso(now),
            "next_attempt_after": after,
            "history": (m.get("history") or [])[-19:] + [
                {"at": _iso(now), "result": "blocked", "reason": reason,
                 "attempt": attempt, "blocked_cycles": cycles,
                 "reattempt_hours": REATTEMPT_HOURS if after else None}],
        })
        return self._write(m)

    def record_ok(self, headed: bool = False, script_needed: bool = False) -> dict:
        """A run that read its pages. Resets the block memory and clears the
        window: the site is letting this address through again. `headed` records
        whether that took a visible browser, so the next run can start with
        one (`needs_headed`) instead of paying the refusal, the pause and
        the relaunch again; a clean headless run clears it. `script_needed`
        records that the visitor script was what recovered the run (the
        retry after an uncleared challenge), stamped at `script_needed_at`:
        the host then starts with the script for SCRIPT_MEMORY_DAYS. A run
        that merely started with it does not renew the stamp, so a host
        that stops challenging falls back to a clean start by itself."""
        m = self.manifest()
        now = _now()
        m.update({
            "host": self.host,
            "last_attempt_at": _iso(now),
            "last_attempt_result": "ok",
            "last_ok_at": _iso(now),
            "consecutive_blocks": 0,
            "cycle_attempt": 0,
            "blocked_cycles": 0,
            "first_blocked_at": None,
            "next_attempt_after": None,
            "needs_headed": bool(headed),
            "history": (m.get("history") or [])[-19:] + [
                {"at": _iso(now), "result": "ok", "headed": bool(headed),
                 "script_needed": bool(script_needed)}],
        })
        if script_needed:
            m["script_needed_at"] = _iso(now)
        return self._write(m)

    def needs_headed(self) -> bool:
        """Did the last clean run on this host need a visible browser?"""
        return bool(self.manifest().get("needs_headed"))

    def needs_script(self) -> bool:
        """Was the visitor script needed on this host within SCRIPT_MEMORY_DAYS?"""
        at = _parse(self.manifest().get("script_needed_at"))
        return bool(at) and _now() < at + timedelta(days=SCRIPT_MEMORY_DAYS)


# --------------------------------------------------------------------------- #
# Browser identity
# --------------------------------------------------------------------------- #

def profile_dir(host: str) -> Path:
    return Path(PROFILE_DIR) / ((host or "unknown").lower())


def visitor_user_agent(ua: str) -> str:
    """The browser's own string with the one word that gives it away removed.
    Playwright's Chromium, in either headless build, reports
    `HeadlessChrome/141.0...`; the version is otherwise exactly right, which is
    why the string is read from the browser rather than pinned (the old pin
    said Chrome 120 against a Chromium 141, a mismatch a WAF can score)."""
    return (ua or "").replace("HeadlessChrome", "Chrome")


def is_challenge(status, headers, title: str) -> bool:
    """Is this answer a challenge interstitial -- a page that may become the
    real one once the browser has run its script -- rather than a wall? Two
    signals: Cloudflare's `cf-mitigated: challenge` response header, and the
    interstitial's title. A wall ("you have been blocked", "Access Denied")
    is not one and is classified at once."""
    try:
        if headers and str(headers.get("cf-mitigated") or headers.get("CF-Mitigated")
                           or "").strip().lower() == "challenge":
            return True
    except Exception:
        pass
    return "just a moment" in (title or "").lower()


def display_available() -> bool:
    """Can a headed browser open a window here? Windows and macOS always have
    a desktop session for an interactive user; on Linux only with a display."""
    import os
    import sys as _sys
    if _sys.platform in ("win32", "darwin"):
        return True
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


# --------------------------------------------------------------------------- #
# The human-shaped part
# --------------------------------------------------------------------------- #

def vary(page, rng: random.Random | None = None) -> dict:
    """Scroll and move like a reader, differently every time. Replaces the two
    fixed wheel strokes of 6000 in runner._load, which were the same on every
    page of every run.

    Returns what it did, so a test can assert on it without a browser.
    """
    r = rng or random
    strokes = r.randint(1, 3)
    done = {"strokes": [], "mouse": None}
    try:
        for _ in range(strokes):
            dy = r.randint(1500, 7000)
            page.mouse.wheel(0, dy)
            done["strokes"].append(dy)
            page.wait_for_timeout(r.randint(150, 450))
        x, y = r.randint(80, 1200), r.randint(80, 700)
        page.mouse.move(x, y, steps=r.randint(4, 12))
        done["mouse"] = (x, y)
    except Exception:
        pass           # a closed page or a frame swap: the content is what matters
    return done
