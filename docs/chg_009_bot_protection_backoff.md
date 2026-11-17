# CHG-009: the generic crawler behaves like a visitor and stops when a site refuses it (started 2026-11-10)

Branch `feature/chg-009-bot-protection-backoff`, from `main`. Independent of
CHG-001 to CHG-008: it touches the Playwright runner and a new access module
only, and none of the earlier branches touch either.

## What was wrong

Every Gulf form (20 hint files under `dynamic_crawler/hints/`) is crawled by
`dynamic_crawler/formfill/runner.py`. It detects a bot-protection wall by
its text and counts it, and the orchestrator marks the run as a problem.
That is where the protection ends. The runner

- never reads the HTTP status of a navigation, so a bare 403 or 429 with no
  wall text is "a page that failed to load" and is retried at once, three
  times on the listing and twice on every detail page. The scheduler notes
  for HHS and CFPB record the lesson: "a retry loop against Akamai is what
  makes a block";
- paces nothing between detail pages, scrolls the same two wheel strokes
  on every page, and waits the same fixed time;
- opens a fresh, cookieless browser context on every run, so a clearance
  cookie earned once is thrown away;
- sends a user agent pinned to Chrome 120 while the bundled Chromium is
  141, always `en-US`, at Playwright's default 1280x720 viewport, in the
  headless shell build. Each is a fingerprint signal on its own;
- keeps going after a block, at the same pace, through every remaining
  page, which is how a temporary challenge becomes a firewall rule;
- knows nothing of `config/pace.yml`, the per-host pacing file, or of
  `utils/pacer.py`, which reads it for the requests-based CBI and CBK Kenya
  crawlers (added 2026-10-27). The Playwright runner, which every Gulf form
  goes through, has never been connected to either.

Four hosts have blocked this project, each from one address after a few
hours of ordinary automated access: simah.com (Cloudflare 1020, triggered
by iteration), saudiexchange.sa (Akamai, clean at 18:27 and refused within
two hours on 2026-08-15), hhs.gov and dfpi.ca.gov (refuse headless). The
local database holds no stored wall page and `run_history` has no
bot-protection problem on record, because every blocked run was discarded.
The evidence is in the documentation, not the data.

The pieces that fix this already exist in the repository, each in a place
the runner does not use: `utils/pacer.py` reads the host
levels in `config/pace.yml`, paces one request per host, classifies an
answer as ok, throttle, block or unknown and remembers a refused host for
the rest of the process, for the requests-based crawlers;
`generic_crawler/blockcheck.py` holds the one list of wall phrases; the US
custom crawlers carry a persistent profile, a status check on every
navigation and a stop after consecutive refusals
(`crawler/cfpb_crawler.py`); and the snapshot store carries a backoff
ladder of 6h, 24h, 72h, 7d, 14d written to a manifest
(`dynamic_crawler/formfill/snapshot.py`). This change connects the runner
to them.

## What it does not do

No CAPTCHA solving, no fingerprint-spoofing libraries, no proxies or
address rotation. The network side is out of scope here. One exception,
decided by the user on 2026-11-12 after the measurements in step 8: the
four-line visitor script the DFPI crawler already runs, applied only as
the retry after a challenge that did not clear and to later runs on a
host that needed it within 30 days. What it alters is stated in step 8
and in the register row; a host that never challenges never runs it. The aim is a
browser that behaves like one careful visitor and stops when told no, so a
regulator that still refuses is asked for an allowlist entry (SIMAH's own
block page asks for exactly that). Behaviour-based bot scoring is what this
helps against; an address-reputation ban, the SIMAH case, is not fixed by
any local change, and the backoff only keeps the project from making it
worse.

## Design

One module, `dynamic_crawler/formfill/access.py`, one config file already
present, one state directory.

- `pace_for(url, hints)` reads `config/pace.yml` and returns the pause to
  take before a navigation: a base range (new key `base_ms`, default
  1500 to 3500) times the host's level (`normal` 1, `careful` 3,
  `very_careful` 5, matched on the host or any parent domain), with a
  random draw each time so no two pauses are equal. A `pace:` key in the
  hints file overrides the host level for one form. `throttle_only` hosts
  pace but never back off: their refusals are rate limits, not bans, so a
  429 waits and resumes rather than ending the run.
- `classify(url, status, headers, title, body)` names a refusal by
  calling `utils.pacer.classify`, so the browser runner and the requests
  crawlers agree on what one is: a wall page is a `block`, a 429 or a 5xx
  with `Retry-After` is a `throttle`, a bare 403 is `unknown`, and on a
  `throttle_only` host a 403, 404 or 429 is the site's own retries'
  business and not a refusal. The wall phrases stay in
  `generic_crawler/blockcheck.py`, one list. `_blocked()` keeps its name
  and becomes a caller of this.
- `AccessState(host)` is the snapshot store's manifest shape applied to a
  live source: `output/access/<host>.json` with `consecutive_blocks`,
  `cycle_attempt`, `blocked_cycles`, `first_blocked_at`, `last_block`,
  `last_ok`, `next_attempt_after`. `may_attempt()`, `attempt_number()`,
  `record_block(reason)`, `record_ok()`, `escalate()`. Steps 1 to 8 ran
  the snapshot store's ladder (6h, 24h, 72h, 7d, 14d); step 10 replaced
  it with the per-cycle routine in rule 9, on the user's word
  (2026-11-16). The snapshot store keeps its own ladder for the
  snapshot-first jobs; nothing here imports it any more.
- `vary(page)` performs the human-shaped part: one to three wheel strokes of
  a random distance, a mouse move to a random point, and a settle wait
  with jitter. Replaces the fixed two strokes of 6000 in `_load`.

What the runner does with it:

1. **Before the run.** `may_attempt()` for the seed host. Inside a backoff
   window the live run is refused with a summary carrying
   `refused: true`, the reason and `next_attempt_after`. A snapshot replay
   is never refused: it touches no network.
2. **On every navigation.** Pause by `pace_for`, then `goto`, then read the
   response status. A refusal (status or wall text) is **not retried**.
   The existing retry loop stays for network errors and timeouts only.
3. **On the first refusal in a run.** One pause of 2 minutes, then that
   URL once more. If it is a page, carry on. If it is refused again, the
   run **ends**: `record_block`, every remaining row goes to
   `detail_skipped` (the orchestrator already treats a partial run as not
   a full crawl), `blocked_pages` counts what was seen, and the summary says
   `stopped: "blocked"`. Rows already read are kept, as today, and the wall
   is never stored as content, as today. On the cycle's re-attempt (rule
   9) there is no in-run retry: the first refusal ends the run.
4. **On a clean run.** `record_ok()` resets the block memory: attempts,
   cycles, the streak's start date and the window.
5. **Browser identity.** `launch_persistent_context` on
   `output/profiles/<host>/` so cookies survive between runs; the
   `chromium` channel, which is the full browser in new headless mode
   rather than the headless shell; no user agent override, only the word
   `HeadlessChrome` replaced by `Chrome` in the browser's own string, so
   the version always matches the binary; viewport 1440x900 as the
   snapshot capture already uses; `Accept-Language` from `locale:` in the
   hints file, default `en-US,en;q=0.9,ar;q=0.8`.
6. **Headed retry.** If the seed is refused headless and a display exists
   (Windows, or `DISPLAY` set), one more attempt headed, logged as a
   `note` event like `requires_headed` is today. Without a display the run
   ends under rule 3. `requires_headed` stays and keeps its meaning.
7. **A challenge is waited out, and remembered.** A 403 that carries
   `cf-mitigated: challenge` or the "Just a moment" title is a page that
   may become the real one once the browser has run the site's script,
   not a wall. It gets `CHALLENGE_WAIT_MS` (30 s, the DFPI crawler's
   figure) for the title to change or a later navigation to land, and
   the status of what it became is what is classified. A challenge that
   does not clear is a refusal under rule 3, by its title, as before. A
   clean run records in the manifest whether it took a visible browser
   (`needs_headed`); the next run on that host starts visible when a
   display exists, instead of paying the refusal, the pause and the
   relaunch again, and a clean headless run clears the flag.
8. **The visitor script, scoped.** A challenge that did not clear in its
   time earns the one retry with `access.VISITOR_SCRIPT` on the same
   profile, in the same mode; any other refusal keeps rule 6. A run the
   script recovered stamps `script_needed_at` on the host's manifest, and
   the host starts with the script for `SCRIPT_MEMORY_DAYS` (30). A run
   that merely started with it does not renew the stamp, so a host that
   stops challenging falls back to a clean start by itself. The browser
   channel order is the installed Chrome, then Playwright's Chromium, then
   the default build.
9. **The per-cycle routine** (the user's word, 2026-11-16, replacing the
   ladder). A scheduled cycle gives a site at most three loads of a
   refused page: the scheduled attempt, its one retry 2 minutes later
   (rule 3), and one re-attempt `REATTEMPT_HOURS` (6) later with no
   retry. The first blocked attempt opens the 6-hour window
   (`next_attempt_after`); the second closes the cycle as blocked
   (`blocked_cycles` + 1, no window), and nothing touches the site until
   the next scheduled cycle. There is no growing backoff: the only waits
   are 2 minutes, 6 hours and the schedule itself. The re-attempt is
   started by the API: `_run_monitor_job` notes the due time in
   `output/access/_reattempts.json` (`scheduler/reattempts.py`) when a
   job's report carries `access.cycle_attempt == 1`, and `scheduler_loop`
   starts the job when it is due; the note survives a restart, waits
   while the job is already running, and is kept when the run was skipped
   on the crawl lock so the next tick tries again. The re-attempt re-runs
   the whole job, so a job of several regulators reads its clean ones
   again six hours later for the one that blocked. A cycle
   whose re-attempt never came within `CYCLE_STALE_HOURS` (24) is closed
   as blocked by the next block, so the count stays honest. The routine
   never stops by itself: after escalation (rule 10) the site stays on
   its schedule and the alert repeats with the count, until a run
   succeeds or a person unschedules the site.
10. **The alert after two blocked cycles.** The run block carries the
   host's block memory (`access`: `blocked_cycles`, `first_blocked_at`,
   `next_attempt_after`, `cycle_attempt`), the orchestrator copies it
   into its report, and `storage/run_store.py` adds a critical
   `site_blocked_repeatedly` alert when `blocked_cycles` is
   `ESCALATE_AFTER_CYCLES` (2) or more, once per blocked cycle, on the
   attempt that closed it; one blocked cycle stays the `run_untrusted`
   warning it was. The alert is on the run row and in
   `GET /alerts` with nothing configured, so a cycle refused inside the
   6-hour window shows in the feed by its warning and the critical comes
   once the cycle is closed. Delivery is
   `storage/alert_channels.py`, chosen by `ALERT_CHANNEL`: `none`
   (default), `webhook` (the shape that existed), `email` (chosen by the
   organisation 2026-11-16: SMTP with STARTTLS or SSL, criticals only
   unless `ALERT_EMAIL_LEVEL=warning`), `teams` and `slack` as logged
   placeholders. The one sentence a person reads is built once
   (`blocked_message`) and is the same on every channel; it names the
   host, the count, the streak's start and what the crawler does next,
   and ends with the four choices: wait, request an allowlist,
   unschedule, upload by hand.
11. **The alert after N quiet runs** (the user's ask, 2026-11-17: a
   regulator site that has returned no new update in more than four
   iterations, the number configurable, 2 or 4). The run store counts,
   per regulator, the consecutive runs of its job (scheduled or started
   by hand through `POST /trigger/monitor/{job}`) that read the site and
   classified nothing as new or modified, and stamps the count on the
   run's report as `quiet_runs` next to the limit that applied
   (`quiet_limit`). A quiet reading is a report with `crawled > 0`,
   `run_trustworthy` not false, and zero `new` and `modified` in
   `classified`; a reading with either resets the count to 0; a run
   that could not read the site (blocked, stopped early, short count,
   nothing crawled, or the job failed) neither counts nor resets, so a
   blocked month does not pass for a quiet one and does not hide one
   either. Disappeared documents are not an update. The count continues
   from the newest stored report of the regulator in the same job's
   rows (`QUIET_LOOKBACK_ROWS`, 50, read back; a report stored before
   this change starts at 0). The limit comes from
   `config/staleness.yml`: `quiet_runs` as one number, or a block with
   `default` (4) and `regulators` exceptions by the name the orchestrator
   reports; 0 turns the rule off for that regulator, and a missing or
   unreadable file means the default. From the limit onward every quiet
   run carries a critical `no_updates_for_runs` alert (the same path as
   rule 10: on the run row, in `GET /alerts`, delivered by
   `ALERT_CHANNEL`, so the email channel mails it with nothing else
   set) whose sentence names the regulator, the count and the limit and
   tells the person what to do: check whether the site changed shape or
   the regulator is quiet, and raise its limit when the silence is
   expected. Weekly jobs make the count weeks; a daily job counts days,
   so its limit is set in runs. The older reading of "no update", days
   since the last stored version (`jobs/staleness_alert.py`,
   `GET /monitoring/staleness`, logged by the scheduler), is unchanged;
   the two answer different questions, as the config file's header says.

Downstream: `orchestrator/orchestrator.py` already turns `blocked_pages`
into a problem and refuses a short run as a baseline, and `verify.py`
already fails a run with blocked pages. Step 4 found that a form run's
`pages.json` never carried the `run` block the gate reads, so the runner
now writes it, and the gate also treats `refused` and `stopped` as
problems (three lines). The new summary keys (`refused`, `stopped`,
`access`, `pace`) are additions.

## Steps

1. `access.py`, `base_ms` in `config/pace.yml`, unit tests
   (`tests/test_formfill_access.py`): pace levels and the parent-domain
   match, jitter bounds, `classify` on status, header and text, the
   manifest ladder, `throttle_only`. Pure, no browser. Done 2026-11-10:
   36 tests, all passing; the block-detection, snapshot, generic-crawler
   outcome and CBI listing suites (110) still pass. Reworked the same day
   once `utils/pacer.py` was found: `classify` now calls the pacer instead
   of carrying its own status table and wall regex. `config/pace.yml` gained `base_ms: [1500, 3500]` and a
   header saying who reads it; the eight host levels are unchanged.
2. `_load` returns the status and classifies; refusals leave the retry
   loop. Tests with a fake page object, in the style of
   `tests/test_formfill_block_detection.py`. Done 2026-11-10: `_load`
   returns a `LoadResult` (truthy when the page arrived, `refusal` set
   when the site said no, `attempts` and `status` for the record); a
   refusal returns after one navigation, a timeout or empty render is
   retried as before. The four callers (seed, listing pages, both
   detail-page loops) count a refusal in `blocked_pages` and warn, so a
   bare 403 is no longer "failed to load". `_blocked()` now reaches the
   pacer's verdict through `access.classify`; the runner's own copy of
   the wall regex is gone. `tests/test_formfill_load_refusal.py`, 13
   tests, no browser. The six formfill and generic-crawler suites give
   7 failed / 108 passed on this branch and on a throwaway worktree of
   `main` at 968f3cf alike: the 7 are `test_formfill_detail_extraction`
   calling `JS_DETAIL` without its arguments, older than this branch.
3. Pacing and `vary` in phases 1 and 2. A snapshot replay pays no pause:
   measured by running the SIMAH snapshot before and after and comparing
   wall time and output. Done 2026-11-10: `run()` decides one pace per
   run from the seed's host (`None` on a replay), emits a `pace` event and
   records it in the summary; `_load` takes a jittered pause before every
   `goto`, retries included, and calls `access.vary` instead of the two
   fixed wheel strokes; a pager click, in both the click walker and the
   tree pager, is paused the same way. A row served from a snapshot
   inside a live run is not paused either. Nine more tests in
   `tests/test_formfill_load_refusal.py` (22 in all). No SIMAH snapshot
   exists on this laptop, so the offline comparison used the one-page
   fixture from `tests/test_formfill_snapshot.py`, replayed three times
   through a real browser on this branch and on the `main` worktree:
   same two records, same fingerprint, 7.1 to 7.9 s here against 6.8 to
   11.6 s on main, no pace recorded. The six regression suites stay at
   7 failed / 108 passed on both.
4. `AccessState` wired: refuse inside a window, first-refusal pause and
   single retry, stop on the second, `record_ok`. Tests drive the phase-2
   loop with a fake page that refuses on a chosen row. Done 2026-11-10,
   with the stub-server test brought forward from step 5 as agreed: a
   `_Guard` in the runner owns the policy (one pause of 60 to 120 s, or
   the site's Retry-After, then one retry; any refusal after that raises
   `RunStopped`, advances the host's ladder and ends the phase); `run()`
   refuses before the first request inside a backoff window and writes
   the same three files with `refused` set; rows not opened after a stop
   are recorded `detail_skipped`; a clean run calls `record_ok`.
   `tests/test_formfill_refusal_server.py`: a localhost site played by
   `http.server`, a real headless browser, 9 tests (clean run, persistent
   403, 429 that clears, second refusal after a recovery, wall on the
   seed, inside the window, expired window, offline replay, the
   orchestrator gate). Two findings on the way, both fixed here:
   - The formfill `pages.json` carried no `run` block, and
     `pipeline.FormfillCrawler` hands that file to the orchestrator as
     `last_result`, whose gate reads `run.blocked_pages` and
     `run.warnings`. A blocked form run has therefore always reached the
     gate looking like a clean run with fewer rows. The file now carries
     `run` (blocked_pages, warnings, refused, stopped, source), and the
     gate in `orchestrator/orchestrator.py` treats `refused` and
     `stopped` as problems, so such a run can neither act on absences nor
     become a baseline.
   - Every detail page was fetched twice. A block at the top of the
     phase-2 loop loaded, expanded, wall-checked and evaluated each row's
     page, and the `else` branch below did all four again and overwrote
     the result; the in-page branch never read it either. Since 67b1e9b5
     (2026-08-05). On a snapshot replay that first load had no snapshot
     to serve and went to the live site once per row: the SIMAH replay,
     built to keep a blocked host offline, was contacting simah.com. The
     block is removed; the stub site now sees exactly one request per
     page, a replay sees none, and the offline replay from step 3 fell
     from 7.1 to 7.9 s to 1.8 to 1.9 s with the same records and
     fingerprint, the difference being two failed live navigations.
   Regression: the six suites stay at 7 failed / 108 passed; the unit
   suites (load-refusal, access, crawl-absence) 89 passed.
5. Browser identity and the headed retry. Verified first by replaying a
   snapshot (identical output, no network), then by one live run of one
   form the user names, with the pace level set to `careful`, and the
   summary compared with that form's last recorded run. Done 2026-11-10,
   the offline part: a `_Browser` wrapper in the runner opens a
   persistent context on `output/profiles/<host>/` through the
   `chromium` channel (the full browser in new headless mode; the
   default build is the fallback when the channel is missing), at
   1440x900, with the form's `locale:`, and with the browser's own user
   agent read back with `HeadlessChrome` corrected to `Chrome` (both
   installed builds report `HeadlessChrome/141.0...`; the string is read
   on a first launch, cached for the process, and the context opened
   again with it). A replay uses a plain context and no profile. The
   guard's single retry takes an optional page to retry on; the seed
   passes `_Browser.headed_retry_page`, which relaunches visibly on the
   same profile when the first load was refused headless and a display
   exists, and does nothing otherwise. Six more stub-server tests (15 in
   all): the site sees `Chrome/141...` and never `HeadlessChrome`, the
   locale sets Accept-Language, a cookie with an expiry set on run one is
   presented on run two, a seed refused headless is relaunched once
   visibly when a display exists and not otherwise, a replay creates no
   profile. The six regression suites stay at 7 failed / 108 passed; the
   unit suites 95 passed; the replay stays at 1.8 s. Three things
   measured on the way, each recorded in a comment or a test:
   - Playwright's `locale` wins over an `extra_http_headers`
     Accept-Language, so the header is whatever the locale makes it
     (`en-US`), not the longer list first planned; the helper for that
     list was removed rather than left unused.
   - The full Chromium build does not render a 4xx/5xx with an EMPTY
     body: `goto` raises instead of returning a response, so a bare 403
     read as a failed load and was retried. `_load` now records the
     main-frame response through a listener and classifies that status
     when the navigation throws. A bodied 403 (Akamai's shape) renders
     and is classified as before; both are tested.
   - Chromium drops session cookies between launches by design; a
     clearance cookie carries an expiry, so the cookie test sets one.
   The live run against a form the user names is still owed and belongs
   to step 6.
7. Challenge wait and `needs_headed` (added 2026-11-12, after the
   measurement below showed the headed retry could never report a
   cleared Cloudflare challenge as anything but refused). Done offline
   first, measured on the way:
   - What a page's own script sees of the runner's browser was measured
     on `about:blank` in four launches (headless and visible, with and
     without Chrome's `--enable-automation` default flag dropped as the
     DFPI crawler does): `navigator.webdriver` is `false` in all four on
     this build (Chromium 141 with `AutomationControlled` disabled),
     plugins 5, `window.chrome` present, the WebGL renderer the real
     Intel adapter. The DFPI crawler's two extra settings change nothing
     a page can see here and were not copied. What a server sees was
     measured on a localhost stub: with the user agent corrected the
     client-hint headers (`sec-ch-ua: "Chromium";v="141"`, platform
     Windows, not mobile) stay consistent with it, and a visible and a
     headless launch send the same header set. One headless tell
     remains and is left alone: `outerWidth/outerHeight` equal the
     viewport headless and exceed it visible.
   - Reading `_load` against a Cloudflare interstitial showed two faults
     for the headed path: the "Just a moment" title is classified as a
     block the moment the page settles, before the challenge can clear,
     and even when it clears the first response's 403 is the status
     that classification sees. `access.is_challenge` names the shape,
     `_await_challenge` in the runner polls the title and the navigation
     listener for up to `CHALLENGE_WAIT_MS` and hands back the status of
     what the page became; `AccessState.record_ok(headed=)` and
     `needs_headed()` carry the memory; the runner starts visible on the
     manifest's word when a display exists.
   - Tests: two unit tests (`is_challenge` by header and by title, a wall
     is not one; the flag set, kept and cleared); three stub-server tests
     with a 403 `cf-mitigated: challenge` page that reloads itself after
     300 ms, or never does: a clearing challenge is waited out with zero
     refusals, an unclearing one is refused after the wait and the one
     retry, and a host cleared only on the visible retry starts visible
     on the next run, with no relaunch, and drops the flag after a clean
     headless run without a display. Stub suite 18 passed; unit suites 97
     passed; six regression suites 7 failed / 109 passed, the same seven
     `test_formfill_detail_extraction` failures as `main`.
   - Measured live on dfpi.ca.gov 2026-11-12 (user's word), three
     requests through the runner's own `_Browser`, `_load` and
     `headed_retry_page` on a fresh profile, the real 30 s wait:

     | arm | mode | result |
     |---|---|---|
     | 1 | headless | 403, "Just a moment...", uncleared after 30 s, `__cf_bm` set |
     | 2 | the visible retry, same profile | 403, "Just a moment...", uncleared after 30 s |
     | 3 | headless again, same profile | 403, "Just a moment...", uncleared after 30 s |

     The challenge wait and the events (`challenge`, `challenge_uncleared`)
     behaved as built; the visible retry as built does NOT clear this
     challenge. The DFPI crawler's own notes agree: a plain visible Chrome
     and a plain visible Edge were refused there too (2026-10-08), and what
     cleared in 4.5 s was its persistent context with Chrome's
     `--enable-automation` default dropped and its init script (webdriver
     undefined, five named plugins, languages, `chrome.runtime`), in LA
     time, at 1280x900, with no user-agent override. Which of those is
     load-bearing was never isolated. Isolated 2026-11-12 (user's word,
     "ok four"): four visible one-page loads, each on a fresh profile,
     all through the `chromium` channel, the runner's `_load` and its
     30 s wait:

     | arm | settings | challenge | what followed |
     |---|---|---|---|
     | 1 | the runner's visible launch alone | uncleared after 30 s | -- |
     | 2 | + `--enable-automation` dropped, no UA override | uncleared after 30 s | -- |
     | 3 | + the DFPI init script | **cleared in ~10 s**, `cf_clearance` set | 403 "403 Forbidden", 19 characters of text |
     | 4 | the DFPI recipe exactly (LA time, 1280x900, its args) | **cleared in ~15 s**, `cf_clearance` set | 403 "403 Forbidden", 19 characters of text |

     Two findings. The init script is the part that clears Cloudflare's
     challenge: a visible window does nothing on its own and neither
     does dropping the automation flag. And clearing it was not the end:
     the page the browser was sent to after the clearance was a bare
     "403 Forbidden", not Cloudflare's interstitial and not Cloudflare's
     block page, with a new `cf_clearance` cookie in hand. The runner
     reported that correctly (`challenge_cleared`, then a refusal "HTTP
     403" from the page that followed). What differs from the DFPI
     crawler, whose profile was last used 2026-11-03: it runs the
     installed Chrome (`channel="chrome"`), not Playwright's Chromium.
     Whether that second 403 is the channel, the address after seven
     challenges in one afternoon, or the site having changed since, was
     the next measurement (user's word, "ok two"), both visible, the
     DFPI recipe, the final response's headers captured:

     | arm | browser, profile | result |
     |---|---|---|
     | A | installed Chrome 157, fresh profile | challenge cleared, then the real page: 200, "Laws & Regulations - DFPI", 266 anchors, 16.8 s |
     | B | Chromium 141 on arm 4's cleared profile, an hour later | the real page at once: 200, 267 anchors, 7.6 s, no challenge |

     So the bare "403 Forbidden" after arms 3 and 4 was neither the
     address nor the site: the same Chromium profile was served an hour
     later with the clearance it had earned, and the installed Chrome was
     served fresh on its first visit. What is left open, one try each, is
     whether a Chromium visitor's FIRST request after clearing bounces at
     the origin (WP Engine) where Chrome's does not, or whether that was a
     moment's refusal. The runner's own retry would have covered either:
     a refusal on the page after a cleared challenge is the first refusal
     in the run, which earns the pause and the one retry.

     Conclusion of the live work: the one setting that gets a visible
     browser past Cloudflare's managed challenge is the DFPI crawler's
     init script, and it is the one setting this change has so far
     declined on principle. Two of its four lines state things that are
     true of a visible Chrome on this laptop already (five plugins,
     `languages` from the locale); two do not (`navigator.webdriver`
     made undefined where a real Chrome says `false`; a `chrome.runtime`
     object where a real Chrome has none on an ordinary page). Whether
     the generic runner adopts it is the user's decision, recorded below
     when made.
   - More measures (user's word, "we should try more measures"), four
     one-page loads on fresh profiles, 2026-11-12:

     | arm | what | challenge | what followed |
     |---|---|---|---|
     | A | the runner's `_Browser` + the DFPI init script, visible | cleared in ~13 s | bare "403 Forbidden" |
     | B | the same, **headless** | **cleared in ~16 s** | bare "403 Forbidden" |
     | C | patchright 1.64, its headless shell 156 | uncleared after 30 s | -- |
     | D | patchright 1.64, installed Chrome 157, visible | uncleared after 30 s | -- |

     Three findings. The init script clears the challenge HEADLESS as
     well, in the full Chromium build the runner now uses; the DFPI
     crawler's "headless never passes" was measured on 2026-10-08 with
     the headless shell, and does not hold for this build. A maintained
     stealth build (patchright) clears nothing here, headless or visible,
     so it earns no place. And the bare "403 Forbidden" after a cleared
     challenge is now 4 of 4 for Chromium 141 on its first request after
     clearing, against 1 of 1 served for the installed Chrome 157 fresh
     and 1 of 1 served for a Chromium profile an hour after clearing. It
     is at the origin (`x-powered-by: WP Engine`), not at Cloudflare, and
     it is exactly the shape the guard was built for: a first refusal,
     one long pause, one retry. The next measurement runs that mechanism
     itself: the runner's guard, headless, with the script, the real 60
     to 120 s pause, on DFPI; and the same through the installed Chrome
     to say whether the build matters at the origin. On the user's word.
     Environment notes: patchright's `install chromium` removed the
     project's Chromium 141 build from the shared Playwright folder (put
     back with `playwright install chromium`, 91 MB), and its own full
     Chromium 156 cannot load its DLL on this laptop (Smart App Control,
     the PyMuPDF story again), so arm D went through the installed Chrome
     as patchright itself recommends.
   - The guard itself (user's word, "ok guard"), 2026-11-12: `_Guard.load`
     on the seed, headless, the DFPI init script on the live context, the
     real pause, the headed retry hook, fresh profiles, access state in
     temp:

     | build | course of the run | result |
     |---|---|---|
     | Playwright's Chromium 141 | challenge cleared in ~16 s; bare 403 at the origin; refusal, paused 112 s; visible retry on the same profile: no challenge (the clearance held) and the bare 403 again; stopped, ladder at 1 | refused, 129 s |
     | installed Chrome 157, headless | challenge cleared in ~13 s; the real page | **200, "Laws & Regulations - DFPI", 267 anchors, 12.7 s, zero refusals** |

     So the build is what the origin refuses, not the clearance and not
     the headless state: Playwright 1.56's Chromium is a year behind the
     current Chrome (141 against 157), and a current browser announcing a
     year-old version is a tell a WAF can act on without any challenge.
     The earlier line in this document that the installed Chrome earns no
     place was measured without the script and is withdrawn below. With
     the script and the installed Chrome, the generic runner reads DFPI
     headless end to end through its own mechanism, no display, no
     retry. What the scheduler host then needs is a current Chrome
     (`playwright install chrome` fetches it), with Playwright's Chromium
     as the fallback where there is none.
8. The visitor script as a scoped retry tactic, the 30-day memory, the
   channel order (the user's "adopt scoped", 2026-11-12). Done offline:
   `VISITOR_SCRIPT` and `SCRIPT_MEMORY_DAYS` in `access.py`, four lines
   copied from `crawler/dfpi_crawler.py` with what they alter written
   above them; `Refusal.challenge` set by `_load` when the interstitial
   did not clear; `_Browser.retry_page` replaces `headed_retry_page` and
   takes the refusal: script first for an uncleared challenge, else the
   visible relaunch as before; `_Browser.CHANNELS = ("chrome",
   "chromium")` tried in order with the default build last;
   `record_ok(script_needed=)` and `needs_script()`; the run starts
   scripted on the manifest's word. Tests: three unit (the stamp set,
   kept, expired, untouched by a block; the script says what this
   document says; the refusal flag) and five on the stub site, whose
   challenge page clears only for a browser that reports `webdriver` as
   undefined, the one thing the script changes that a real Chrome does
   not: an uncleared challenge earns the scripted retry and the stamp; a
   stamped host starts scripted and is served without a refusal; an
   expired stamp starts clean; an Akamai-style wall earns the visible
   retry and never the script; a challenge that clears on its own never
   sees it. One older test changed premise: a host that "cleared only
   visibly" now does so from a bare 403, since an uncleared challenge is
   the script's case. Stub suite 23 passed; unit suites 100 passed; the
   six regression suites 7 failed / 109 passed, the same seven. Live
   confirmation (user's word, "ok dfpi run"), 2026-11-12, `runner.run`
   on a throwaway hint for the DFPI regulations page, headless, fresh
   profile and access state in temp, three detail pages at most: the
   installed Chrome 157 was chosen; the challenge did not clear in 30 s;
   refusal, 88 s pause; the scripted relaunch on the same profile; the
   challenge cleared to a 200; recovered; 30 rows matched on the
   listing, 3 detail pages read, 7 documents found; 164 s in all; zero
   blocked pages, run not stopped, ladder at 0, `script_needed_at`
   stamped, `needs_headed` false; the `run` block in `pages.json`
   carries the one REFUSED warning. The generic runner reads a
   Cloudflare-challenged host headless, end to end, through its own
   mechanism.
9. Full suite against a throwaway worktree of `main`; this document's
   measurement and decisions sections; the register row. Suite done
   2026-11-10, both trees with `--continue-on-collection-errors` and
   `tests/test_snapshot_articles.py` deselected (it does not run beside
   the others on this laptop):

   | | failed | passed | errors | skipped |
   |---|---|---|---|---|
   | `main` at 968f3cf | 19 | 531 | 32 | 0 |
   | this branch, step 6 | 19 | 595 | 32 | 1 |
   | this branch, after step 8 (2026-11-12) | 19 | 600 | 32 | 1 |

   The same 19 failures and 32 errors by name on both; nothing fails on
   the branch that does not fail on `main`. The 64 extra passes are the
   new unit suites. The one skip is `tests/test_formfill_refusal_server.py`
   as a whole: `tests/test_change_signal.py` installs module-shaped
   stand-ins for paddle, torch and fitz at import time, and a Playwright
   launch in the same process then dies inside torch's patched
   `inspect.getfile`. Every browser-backed formfill test on `main` (29 of
   its 32 errors, and the one snapshot failure) breaks in the full suite
   for exactly this reason; the new file detects the stand-ins and skips
   with that message instead of adding thirteen more errors. Run on its
   own it passes, 15 tests in about two minutes. The three collection
   errors are `test_rulebook`, `test_staged_analyzer_offline` and
   `test_versioning_live`, which do not import on this laptop on either
   tree. The live run against the MHRSD form is recorded below.

   **Live run 1, MHRSD, 2026-11-10 12:52, headless, pace `careful`, phase 2
   capped at five pages, output to the scratchpad, nothing stored.** What
   the site saw: `Chrome/141.0.0.0` with no headless marker, the `chromium`
   channel, `en-US`, a fresh profile created at
   `output/profiles/www.hrsd.gov.sa/`, pauses drawn from 4.5 to 10.5 s.
   Two results. First, the run completed its crawl and then crashed before
   writing a file: the click pager assigns a dict to a local named `state`,
   which shadowed the access state of the same name, so `record_ok` was
   called on a dict. Renamed to `access_state`; the stub-server tests that
   reach `record_ok` pass again. Second, the pager reported
   `page 2: no-control`: the "next" selector matched nothing on page 1,
   where the recorded runs of 2026-09-18 and 09-21 walked four pages for
   63 and 64 rows. The one thing this branch had changed that a pager can
   see is the viewport, moved to 1440x900 from the 1280x720 every form was
   verified at. Reverted to 1280x720, stated explicitly, with the reason
   in `access.py`. Whether the viewport was the cause, or the site has
   changed since September, is what live run 2 is for; it is owed and
   needs the user's word, since it is a second visit.

   **Live run 2, MHRSD, 2026-11-10 13:1x, same settings, viewport
   1280x720, user's word given.** Completed in 272 s. The site saw the
   same identity as run 1 and the profile from run 1 was reused
   (`relaunches: 0`). Listing: 4 pages walked, 18 + 18 + 18 + 6 = 60
   rows, fill rates 100 % on title, document_url and published_date;
   phase 2 read its 5 capped pages; `blocked_pages` 0, no refusal, the
   access file for the host written with `last_attempt_result: ok`.
   Against the 62 MHRSD rows in the local copy (2026-09-22), by title:
   4 gone from the site (the domestic-labour circular, the Child
   Protection executive regulations, the labour-education guide, the
   LRI guidebook), 2 new (project-management localisation guide,
   recruitment-sector violations and penalties), 1 retitled (the
   children's hospitality centres controls lost "1440 H."). 62 - 4 + 2 =
   60: the walk was complete and the difference is the site's, which is
   what the orchestrator's count gate would hold for a person to confirm.
   `page 2: no-control` appeared again, so the viewport was NOT its
   cause: for `pagination.mode: click` the runner walks the pager inline
   (`while page_no < max_click_pages`, which did the four pages) and then
   ALSO calls `_click_through`, which starts on the last page, finds no
   "next" and warns "only page 1 was walked". Identical on `main`; it is
   a misleading warning, not a lost page, and belongs to a form-engine
   fix outside this change. The viewport decision stands on its own
   ground: the forms were verified at 1280x720 and nothing measured
   argues for moving them.
10. The per-cycle routine and the alert (the user's words, 2026-11-16:
   "on day 1 we try, then after 2 minutes, then six hours later; a week
   later the same; if the 2nd week blocks again we escalate to a human
   who decides"; channel: email). Done offline. `access.py`:
   `RETRY_PAUSE_MS` 120 s, `REATTEMPT_HOURS` 6, `CYCLE_STALE_HOURS` 24,
   `ESCALATE_AFTER_CYCLES` 2, `attempt_number()`, `escalate()`,
   `record_block` as cycle bookkeeping, `record_ok` resetting all of it;
   `BACKOFF_HOURS` no longer imported. `runner.py`: `_Guard(in_run_retry)`
   False on the cycle's re-attempt, `_cycle_fields` on the access summary,
   the refused-before-start block and the `run` block, the `start` event
   says which attempt the run is. `orchestrator.py`: `access` copied into
   both report shapes. `storage/run_store.py`: the rule and
   `blocked_message`; `_notify` delegates. New: `storage/alert_channels.py`
   and `scheduler/reattempts.py`; `apis/pipeline_api.py` notes the
   re-attempt after a job, starts due ones from `scheduler_loop`, and
   `/trigger/monitor/{job}` shares `_start_monitor_job` with it. Tests:
   `tests/test_formfill_access.py` (the ladder tests became seven cycle
   tests: the window, attempt 2, the closed cycle, three cycles and
   escalation, the stale cycle, the reset), `tests/test_chg009_escalation.py`
   (new, 28: the rule at 1, 2 and 3 cycles, once per cycle on the
   closing attempt and not on the next first attempt or a skipped run,
   the message; email through a
   recorded SMTP stand-in incl. levels, their normalisation, SSL port,
   unset settings, a bad port and a refused connection; webhook shape
   unchanged; placeholders and an unknown channel; `_notify` never
   raising; the re-attempt note: noted, removed by a closed cycle or a
   clean run, kept through a run skipped on the crawl lock and not
   started twice while running, a job of several reports, replace and
   pop, unreadable and naive due times), `tests/test_formfill_load_refusal.py`
   (the re-attempt guard stops on the first refusal and calls no hook),
   `tests/test_formfill_refusal_server.py` (the run block gains `access`;
   three new cases: the re-attempt is one load and closes the cycle; two
   blocked cycles reach `blocked_cycles` 2 on the run block through
   five runs, a skipped one included, in six loads; a listing cut short
   on its second page records each harvested row once). Unit suites 101
   passed; stub-site suite 26 passed in 4 min 5 s; the six neighbouring
   suites 3 failed / 117 passed, the same
   three as on `main` (`test_a_single_page_form_is_allowed`,
   `test_every_shipped_config_lists_sources_or_says_why_not`,
   `test_archive_uses_the_old_version_not_the_new_one`). The stub-site
   suite and the full suite are recorded in step 11.

   Settings on the scheduler host, all optional (`storage/alert_channels.py`
   docstring is the reference): `ALERT_CHANNEL=email`, `SMTP_HOST`,
   `SMTP_PORT` (587 STARTTLS, 465 SSL), `SMTP_USER`, `SMTP_PASSWORD`,
   `ALERT_EMAIL_FROM`, `ALERT_EMAIL_TO` (comma-separated),
   `ALERT_EMAIL_LEVEL` (`critical` default). The SMTP password and any
   webhook URL live in `.env`, never in code.

   An independent review of the branch (2026-11-16) found, and this step
   fixed: a listing stopped on a later page with rows in hand recorded
   every row twice (once as a record, once as not opened) since step 4;
   a due re-attempt was dropped when the job was already running or was
   skipped on the crawl lock; the alert fired two or three times a cycle
   (the closing attempt, the next first attempt, a skipped run) where the
   intent is once; `ALERT_EMAIL_LEVEL` was case-sensitive and an unknown
   value mailed warnings; a non-numeric `SMTP_PORT` raised out of
   `send`; the `access` block had two shapes. Two helpers only tests
   used (`escalate`, `pending`) were removed, and three assertions that
   could not fail were made to.

   What a week costs a site that blocks: at most three loads of a refused
   page per cycle, where steps 1 to 8 made two per run with windows that
   grew to 14 days and retried indefinitely; escalation after two cycles
   where there was none.
11. Full suite on this branch after step 10, against the `main` numbers
   of step 9 (`main` is still 968f3cf; origin fetched 2026-11-16):

   | | failed | passed | errors | skipped |
   |---|---|---|---|---|
   | `main` at 968f3cf (step 9) | 19 | 531 | 32 | 0 |
   | this branch, after step 8 (2026-11-12) | 19 | 600 | 32 | 1 |
   | this branch, after step 10 (2026-11-16) | 19 | 628 | 32 | 1 |
   | this branch, after the review fixes (2026-11-16) | 19 | 632 | 32 | 1 |

   The same 19 failures and 32 errors by name as after step 8 (the only
   differences in the name lists are formatting artefacts: the asyncio
   log lines and the spelling of the three collection errors). The
   extra passes are `tests/test_chg009_escalation.py` and the new guard
   test. The one skip is the stub-site file skipping itself inside the
   full suite, as step 9 explains; run on its own after the review fixes
   it passed 26 in 4 min 5 s. Log:
   `full_suite_009_step10.log` in the session scratchpad.
12. The quiet-runs alert (rule 11), 2026-11-17. `storage/run_store.py`:
   `quiet_reading`, `quiet_limit`, `quiet_runs`, `quiet_message`, the
   stamping step inside `save_run` (one `SELECT TOP 50 summary` of the
   job's rows, before the alerts are read) and the rule in `alerts_for`;
   `config/staleness.yml`: the `quiet_runs` block and a header that
   tells the two readings apart; README section 5. Tests:
   `tests/test_chg009_quiet_runs.py` (25: what counts as a reading, the
   limit's three shapes and its off switch, the streak over stored rows
   with failed runs and multi-regulator jobs in between, the rule at and
   past the limit, and `save_run` through a fake cursor: the count and
   limit stored on the summary, each regulator of a job at its own
   limit, a failed run reading nothing back, a missing config file, a
   lookback that fails keeping the run record and counting from 0).
   Full suite: step 13.
13. Full suite on this branch after step 12 (2026-11-17), against the
   `main` numbers of step 9:

   | | failed | passed | errors | skipped |
   |---|---|---|---|---|
   | `main` at 968f3cf (step 9) | 19 | 531 | 32 | 0 |
   | this branch, after the review fixes (2026-11-16) | 19 | 632 | 32 | 1 |
   | this branch, after step 12 (2026-11-17) | 19 | 672 | 32 | 2 |

   The same 19 failures and 32 errors by name as in step 11 (compared
   line by line). Of the extra passes, 24 are
   `tests/test_chg009_quiet_runs.py` (25 after the final check added
   one); the rest are tests the step-11 log
   did not see because it was written before the last review edits of
   the pushed commit (no test file changed between 1f11258 and this
   step except the new one). The second skip is
   `tests/test_snapshot_articles.py`'s one test that needs a saved real
   page on disk. Log: `full_suite_009_step12.log` in the session
   scratchpad, 7 min 2 s.

## Measurement before wiring (2026-11-10)

Local copy of `uc2-db` (2026-09-22) and the on-disk run summaries.

- `regulations`: 47,237 rows. No row's HTML or text contains a wall
  phrase. The one hit for "ddos protection" is SAMA's Cyber Security
  Framework. The 8,827 CBB rows under 400 characters of HTML are "click
  here to download" stubs; the 504 SAMA ones are "available only in
  Arabic" notices.
- `run_history`: 113 runs, 19 with problems, none mentioning bot
  protection. Every problem is a count movement.
- 20 `pages.json` summaries on disk: `blocked_pages` is 0 or absent in
  all.
- `config/pace.yml`: 8 hosts with a level, 2 `throttle_only`. Read by
  `utils/pacer.py` for CBI and CBK Kenya; not by the Playwright runner.
  (First written here as "0 readers": a search that stopped at fifteen
  hits missed the pacer. Corrected 2026-11-10 before step 2.)

So there is no stored baseline of what a block looks like in this
pipeline's own data, and no false-positive risk to measure against: the
wall patterns are already in production in `_blocked` and have not
matched a genuine page in 47,237 rows.

## What the change can and cannot get past (measured 2026-11-10, user's word given)

One page per host through three clients, then the same 40 HHS pages twice.

| Host and wall | plain request | old identity (headless shell, Chrome 120 pinned) | new identity (CHG-009) |
|---|---|---|---|
| ola.org, no protection | 200 | not tried | 200 |
| consumerfinance.gov, Akamai, lenient that day | 200 | 200 | 200 |
| hhs.gov, Akamai | 403 Access Denied | 403 Access Denied | 200, the real page |
| dfpi.ca.gov, Cloudflare managed challenge | 403 "Just a moment" | 403 "Just a moment" | 403 "Just a moment"; **200, the real page, with step 8's script and the installed Chrome (2026-11-12)** |

Sustained, 40 distinct hhs.gov pages taken offline from the saved
regulations page, headless, one attempt per page, the guard's stop rule
on:

| arm | pace | scrolling | profile | read | refusals | elapsed |
|---|---|---|---|---|---|---|
| B, the full behaviour | careful, 4.5 to 10.5 s | varied | persistent | 40 of 40 | 0 | 449 s |
| A, identity only | none | fixed | fresh context | 40 of 40 | 0 | 154 s |

What this says, no more and no less:

- **Akamai's door check is passed by the identity.** HHS refused the old
  crawler headless and refused a plain client; it served the new identity
  on the first request and for 40 pages after. The repository's notes
  that HHS "cannot run on a headless box" describe the old identity.
- **At 40 pages the pacing made no measurable difference on HHS.** Arm A,
  at roughly four seconds a page with no pauses and no profile, was not
  refused either. The bans on record came after about two hours of
  traffic (saudiexchange.sa) or after repeated iteration (simah.com), and
  a 40-page run is neither. The pacing's value is in not reaching that
  point, which this test was too short to show and which a test long
  enough to show it would risk the address to prove. The stop-and-backoff
  rule's value is proven separately, on the stub site.
- **A Cloudflare managed challenge is passed only by the visitor
  script.** DFPI served the same "Just a moment" page to every client
  without it, the new identity included, visible or headless, with
  `cf-mitigated: challenge`; with the script it cleared in 10 to 16 s
  every time, headless included (step 7 and step 8 measurements, eleven
  loads on 2026-11-12). The origin behind it then served the installed
  Chrome 157 and refused Playwright's Chromium 141, so the channel order
  changed with it.
- **The installed Google Chrome, on its own, does not change that**
  (one request, headless, 2026-11-10: the same "Just a moment" page
  after 12 s). The earlier conclusion drawn from it, that there is no
  reason to prefer the installed Chrome, is withdrawn: with the script
  the challenge clears for both builds and the origin then serves only
  the current one. A scheduler host wants a current Chrome
  (`playwright install chrome`); Playwright's Chromium stays the
  fallback.
- **A site that already refuses this address by rule** (simah.com,
  saudiexchange.sa) was not tested and would not be helped. The route
  there is still an allowlist request.

## Decisions

- Stop the run on the second refusal, not the fifth as the CFPB crawler
  does. CFPB's five are consecutive page reads on a host that was already
  letting the profile through; here the first refusal is the signal and
  the retry after a long pause is the test of whether it was transient.
- The pause before the single retry is 2 minutes (the user's figure,
  2026-11-16; steps 1 to 8 drew 60 to 120 s), long enough for a burst
  rule to expire and short enough that a scheduled job does not hold the
  global lock for long.
- No growing backoff. The ladder of steps 1 to 8 (6h, 24h, 72h, 7d, 14d)
  retried a blocked site indefinitely at a widening gap and told no one;
  the user's routine (2026-11-16) bounds a cycle at three loads and puts
  a person in the loop after two blocked cycles. The crawler keeps its
  weekly routine after escalation rather than pausing the site, so the
  alert repeats with the count until a run succeeds or a person
  unschedules it; pausing was offered and not chosen.
- The alert is stored first and delivered second, so the feed shows it
  with nothing configured. Email is the chosen channel; it mails
  criticals only by default because the escalation is critical and a
  weekly digest of warnings was not asked for. Teams and Slack are
  logged placeholders so choosing one later is a formatter, not a
  redesign.
- The 6-hour re-attempt is noted by the API that ran the job, in a file
  under `output/access/`, and started by the API's own scheduler loop:
  `scheduler/scheduler.py` only triggers jobs over HTTP and never sees a
  result, and a one-off APScheduler job would not survive a restart.
- No per-page delay on a snapshot replay. A replay is offline; pacing it
  would make a zero-traffic job slow for nothing.
- `throttle_only` means what `utils/pacer.py` already makes it mean: a
  403, 404 or 429 from such a host is not a refusal, its own retries
  handle it. One definition, in the pacer; the runner does not add a
  second.
- The headless shell is replaced by the `chromium` channel rather than
  the installed Chrome, so the scheduler host needs nothing installed
  beyond what Playwright already fetches.
- The user agent is derived from the browser rather than pinned, so it
  stops going stale the moment Playwright is upgraded.
- The viewport stays at Playwright's 1280x720, stated rather than
  assumed. The forms were verified at that width; the first live run
  suggested a wider window changes a pager. Fingerprint value of a
  different size was never measured and is not worth a verified form.
- The headed retry is a relaunch of the same profile, not a second
  browser beside the first: the clearance a visible window earns is then
  on the profile the next headless run opens.
- A challenge is given 30 s before it is called a refusal, headless as
  well as visible, although headless it is not expected to clear: the
  wait is the measurement, two pages at most per run under the stop
  rule, and it keeps one definition of "refused" for both modes.
- `needs_headed` is a memory, not a rule: a host that opened visibly
  starts visibly, and nothing re-probes headless on purpose. A clean
  headless run (no display on the scheduler host) clears it, so the
  flag follows what the host last allowed rather than a schedule.
- The DFPI crawler's `--enable-automation` removal was measured and not
  copied: it changes nothing a page can see on this build, and it
  cleared nothing on dfpi.ca.gov. Its init script was measured, found to
  be the one thing that clears the challenge, and adopted scoped (rule
  8) on the user's decision. The crawler's own finding that the headless
  state is what the challenge scores was made on the headless shell and
  does not hold for the full build: the script clears it headless.
- The script is a retry tactic and a 30-day memory, not a default. The
  register row must be able to say, per host, whether the crawler ever
  altered what the site saw; a host that never challenges never did.
- A maintained stealth build (patchright 1.64) was measured and cleared
  nothing, headless or visible; it earns no dependency.
- `output/profiles/` holds cookies and is never committed; it sits under
  `output/`, which the ignore file already covers (checked in step 5).
- The quiet-runs count is kept on the run's own report, not computed
  from scratch each time: one SELECT of the job's recent rows instead
  of a walk whose depth would have to grow with the limit, and the count is visible in
  `GET /runs/{id}` with the limit it was judged against. Runs that
  could not read the site carry the count rather than reset it: a reset
  would let a blocked site look healthy, and counting them would make
  a blocked site look quiet when the blocked alert already covers it.
- The alert is critical and repeats on every quiet run from the limit
  onward, like the blocked alert, so a mailbox set to criticals only
  receives it and a regulator that publishes rarely is tuned by its own
  limit in the config rather than by a quieter alert level.

## Register row text

**Change (what / where).** `dynamic_crawler/formfill/access.py`, new:
per-host pacing from `config/pace.yml` with jitter, refusal
classification through the existing `utils/pacer.py`, and a per-host
block memory persisted under `output/access/` that bounds a scheduled
cycle at three loads of a refused page (the attempt, one retry 2 minutes
later, one re-attempt 6 hours later) and counts consecutive blocked
cycles. `storage/run_store.py`: a critical `site_blocked_repeatedly`
alert when a site has refused two scheduled cycles in a row, repeated
each blocked cycle, and a critical `no_updates_for_runs` alert when a
regulator has been read a configurable number of scheduled runs in a
row (`config/staleness.yml`, default 4, per regulator) with nothing new
or modified; `storage/alert_channels.py`, new: delivery by
`ALERT_CHANNEL`, email chosen (SMTP), webhook kept, Teams and Slack as
placeholders. `scheduler/reattempts.py`, new, and `apis/pipeline_api.py`:
the 6-hour re-attempt noted after a blocked first attempt and started
when due. `dynamic_crawler/formfill/runner.py`: status-aware loads with
no retry on a refusal, a jittered pause and varied scrolling before every
navigation, one 2-minute pause and one retry on the first refusal then
the run ends, one
navigation per detail page where there were two, a persistent profile
per host, the browser's own user agent, source locale, the full-browser
headless channel, one headed retry when the seed is refused headless and
a display exists, a challenge interstitial waited out (30 s) and its
cleared status classified instead of the 403 it arrived with, a per-host
`needs_headed` memory so a host that only opened visibly starts visibly
next time, the installed Chrome preferred over Playwright's Chromium,
and a `run` block in `pages.json` so the orchestrator's gate sees a form
run's blocked, stopped or refused state (`orchestrator/orchestrator.py`,
three lines). One browser alteration, scoped: after a challenge page
that does not clear, the one retry runs the DFPI crawler's four-line
init script (`navigator.webdriver` reported as undefined, a
`chrome.runtime` object present, five named plugins, `en-US` languages),
and a host it recovered keeps it for 30 days; every other host sees an
unaltered browser. No CAPTCHA solving, no stealth libraries, no address
rotation.

**Justification for Change.** Problem: the generic crawler retries a
refused page at once, paces nothing, discards its cookies every run and
continues through every remaining page after a block. Four regulator
hosts have banned the project's address after a few hours of exactly this
behaviour, and a ban costs the library a source until the regulator
allowlists it. The per-host pacing file and its reader exist for the requests-based
crawlers but the Playwright runner is not connected to them.
Proposed Change: make the crawler behave like one careful visitor and stop
when refused. Refusals are recognised by status as well as text and are
never retried in a loop; pauses are drawn per host from `config/pace.yml`;
a profile keeps the clearance a site has already granted; a first refusal
earns one 2-minute pause and one retry, a second ends the run, one
re-attempt follows 6 hours later, and a site that refuses the whole
cycle is left alone until its next scheduled cycle. Two blocked cycles
in a row raise a critical alert, stored with the run and emailed, and a
person decides: wait, request an allowlist, unschedule, or upload by
hand. A regulator read for a configurable number of runs in a row with
nothing new or modified raises a second critical alert on the same
path, so a crawler that still runs but no longer sees the site's
updates is noticed within the chosen number of weeks. The orchestrator's existing handling of `blocked_pages` and short
runs is unchanged, so a stopped run is reported and never becomes a
baseline.
