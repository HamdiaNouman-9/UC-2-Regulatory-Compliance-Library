# CHG-002: crawled HTML is cleaned and sandboxed at display (started 2026-11-04)

Branch `feature/chg-002-html-display-sandbox`, based on
`feature/chg-001-html-ingestion-cleaner` because it reuses that branch's
allowlist. Second of the two changes that answer the reviewer's finding that
crawled regulator pages reach a reviewer's browser as the regulator's site
served them. CHG-001 fixed the **stored record** for rows written from now
on; this change fixes the **display**, which also covers the rows stored
before CHG-001 (42,444 in the September database copy).

This file is kept up to date as the steps land. Steps not yet done are
marked.

## What was wrong

`web/app.js` `sanitizeHtml` (line 120) ran five regular expressions over the
stored HTML, after cutting it at 200,000 characters, and put the result
straight into the page's own DOM with `innerHTML` (`renderRegulationTab`,
line 522). An unquoted `onerror`, an `<svg onload>`, an entity-encoded
`javascript:` href, an `<iframe>` and a `<form>` all passed, and whatever
passed ran with the reviewer's session. `index.html` loaded no sanitising
library and set no content security policy.

## Design

Two layers, so that a bypass of one gains nothing:

1. **DOMPurify decides what is shown.** Configured from the same allowlist as
   the cleaner, read from `web/html_allowlist.json`. Links get
   `target="_blank"` and `rel="noopener noreferrer"`.
2. **A sandboxed iframe decides what can run.** The cleaned HTML goes into an
   iframe with `srcdoc`, `sandbox="allow-popups allow-popups-to-escape-sandbox"`
   (no scripts, no same-origin, no forms) and a content security policy
   inside the document that allows only images and inline styles.

The allowlist has one source, `utils/html_clean.py`. A test fails if the
JSON the browser reads drifts from it.

## Steps

| # | Step | State |
|---|---|---|
| 1 | Branch from `main`, then fast-forwarded onto CHG-001 so `utils/html_clean.py` is present | done |
| 2 | Vendor DOMPurify 3.4.17 into `web/vendor/` with licence, README (source, integrity, SHA-256) and the script tag in `index.html` before `app.js` | done |
| 3 | One allowlist for both sides: `python -m utils.html_clean` writes `web/html_allowlist.json`; `tests/test_html_display.py` checks the file matches the module and the vendored file matches its README | done |
| 4 | Replace `sanitizeHtml` with DOMPurify configured from the JSON, plus the link hook | done |
| 5 | Render in a sandboxed iframe with CSP; description-box styles move inside; frame fixed at the box's old 360 px with its own scrollbar | done |
| 6 | Truncate after cleaning, at a tag boundary, re-parsed so nothing is left open | done |
| 7 | Node test running DOMPurify over the same fixtures as the Python tests (`tests/js`, 53 cases) | done |
| 8 | Review UI against the local database copy, headless Chromium: MC 101 (onclick headings), SAMA 1064 (inline image), CBK Kenya 47853, crafted dirty row 48087 | done |
| 9 | Handoff: [chg_002_handoff_2026-11-04.md](chg_002_handoff_2026-11-04.md) | done |

## What the review UI showed (2026-11-04, local copy, API on :8000)

Headless Chromium opened `web/index.html`, called `openRegulation(id)` for
each row, waited for the frame, and inspected it. The crafted row 48087 was
inserted into the local copy with raw markup, bypassing the CHG-001 cleaner
on purpose: an unquoted `onerror`, `<svg onload>`, a plain and an
entity-encoded `javascript:` link, an iframe, a form with a button, a script
that rewrites the parent page's title and background, a `display:none`
style, an `onclick` table cell, an `onmouseover` paragraph, and one inline
PNG.

| Row | Frame | Dialogs | Parent page touched | Scripts / `on*` / `javascript:` inside | Links, all `_blank` + noopener | Images loaded | Words |
|---|---|---|---|---|---|---|---|
| MC 101 | yes | 0 | no | 0 / 0 / 0 | 5 of 5 | 0 of 0 | 3,559 |
| SAMA 1064 | yes | 0 | no | 0 / 0 / 0 | 17 of 17 | 1 of 1 (inline PNG) | 9,021 |
| CBK 47853 | yes | 0 | no | 0 / 0 / 0 | 75 of 75 | 0 of 0 | 894 |
| crafted 48087 | yes | 0 | no | 0 / 0 / 0 | 1 of 1 | 1 of 2 (the inline PNG; `src=x` is a broken image) | 22 |

The crafted row's words are all there ("form words stay", "table cell",
"last paragraph"); the javascript link is plain text; the iframe, form,
button and svg are gone; the script never ran (title and background
unchanged). The sandbox attribute on every frame is
`allow-popups allow-popups-to-escape-sandbox`. The three real rows look as
they did before the change, with the same 360 px box.

Row 48087 stays in the local copy only. It does not exist anywhere else.

## What changed (so far)

| Where | What |
|---|---|
| `web/vendor/purify.min.js`, `DOMPURIFY-LICENSE`, `README.md` | DOMPurify 3.4.17 (released 2026-10-12), npm tarball integrity verified, SHA-256 `cd37898f…2c31` recorded |
| `web/index.html` | loads `vendor/purify.min.js` before `app.js`; app.js cache key v3 to v4 |
| `utils/html_clean.py` | `allowlist_as_dict()`, `write_allowlist_json()`, `python -m utils.html_clean` |
| `web/html_allowlist.json` | generated; tags, attributes, url schemes, data-image prefixes, link rel |
| `web/html_allowlist.js` | generated beside the JSON; `window.HTML_ALLOWLIST = {...}`, loaded by a script tag so it works from disk as well as served |
| `tests/test_html_display.py` | allowlist JSON and JS match the module; vendored file matches README; script order in index.html |
| `web/app.js` | `sanitizeHtml` and its five regexes removed. New block: `purifier()` (DOMPurify instance configured from the allowlist, per-tag attribute hook, `data:image` on `img` only, link hook), `cleanStoredHtml()` (clean, then truncate at a tag boundary and re-parse), `descriptionFrame()` (sandboxed iframe with CSP), `mountDescription()` (frame, or escaped text if the sanitiser is missing, never raw html). `renderRegulationTab` no longer puts stored html through its template string. |
| `web/style.css` | `.desc-box.has-frame`, `.desc-frame` (360 px, the box's old max-height), fallback note and text styles |
| `tests/js/html_display.test.js`, `tests/js/package.json` | Node test runner with jsdom: the Python removal and retention cases, link hook, idempotence, fixtures keep their words, truncation, sandbox attributes and CSP, fallback without a sanitiser. `cd tests/js && npm install && npm test` |
| `.gitignore` | `node_modules/` |

## Findings while testing

- **`FORCE_BODY` is required.** DOMPurify parses with the browser's HTML
  parser, which starts in head mode: a stored fragment that begins with
  `<noscript>` or `<style>` had its text fall out into the body as words.
  nh3 parses a body fragment. `FORCE_BODY: true` makes DOMPurify do the same,
  and the noscript case then matches the Python test.
- **The frame cannot size itself.** Auto-height needs a script inside the
  frame, and the sandbox forbids scripts. The frame is fixed at 360 px with
  its own scrollbar, which is what the old `.desc-box` already did with
  `max-height: 360px; overflow-y: auto`. No visible change for the reviewer.
- **Relative links resolve against the UI's origin**, inside the frame as
  they did in the page. That is unchanged by this branch;
  `scripts/absolutise_document_html.py` is the fix for it and is separate.

## Decisions

- **Vendored, not CDN.** The review UI must work without internet, the
  version must not move under us, and the checksum is what was reviewed.
- **Based on CHG-001's branch.** The display reads the cleaner's allowlist.
  When CHG-001 merges to `main`, this branch's own commits are the diff.
- **DOMPurify stays even though CHG-001 cleans at ingestion.** Old rows were
  never cleaned, and a display that trusts storage is one bug away from the
  original finding.
