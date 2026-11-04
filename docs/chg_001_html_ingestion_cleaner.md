# CHG-001: crawled HTML is cleaned before it is stored (2026-11-03)

Branch `feature/chg-001-html-ingestion-cleaner`. One of two changes that answer
the reviewer's finding that crawled regulator pages reach a reviewer's browser
exactly as the regulator's site served them. This change fixes the **stored
record**; CHG-002 (DOMPurify plus a sandboxed iframe in `web/app.js`) fixes the
**display**, and covers rows stored before this change.

## What was wrong

`regulations.document_html` held the page's own markup byte for byte. The only
check was five regular expressions in `web/app.js` `sanitizeHtml`, applied
after truncation, and each of these got past it: an unquoted `onerror`, an
`<svg onload>`, an entity-encoded `javascript:` href, an `<iframe>`, a `<form>`.

## What changed

| Where | What |
|---|---|
| `utils/html_clean.py` | The allowlist and `clean_html()`. nh3 (Rust, html5ever) so the tree it sees is the one the browser will build. |
| `storage/mssql_repo.py` | `clean_html` on all three writers of HTML: `_insert_regulation`, `update_regulation` when `document_html` is among the fields, `insert_regulation_version` (`content_html`). |
| `dynamic_crawler/formfill/excel_repo.py` | The same three calls, so a workbook holds what the database would. The workbook is what gets promoted. |
| `scripts/html_clean_scan.py` | Count-only scan of stored rows or workbooks: what would be dropped, per regulator, and whether any visible text would be lost. No apply mode, on purpose. |
| `tests/test_html_clean.py` | 54 cases: removal, retention, text kept inside a forbidden container, idempotence, visible text unchanged on every fixture, all six writers. |
| `requirements.txt` | `nh3==0.3.7`. |

The cleaner lives in the repo layer, not in the crawlers (about thirty of
them) and not in the API, because every writer of `document_html` goes through
`storage/mssql_repo.py`, including the upload endpoint's client-supplied
`document_html`. A caller cannot forget.

**Kept:** headings, paragraphs, lists, tables, links, images, emphasis, inline
`style`/`class`/`id`. Links keep http, https, mailto, tel and relative urls.
**Removed with content:** script, style, noscript, comments. These are exactly
what the fingerprint already ignores. **Removed, words kept:** iframe, object,
embed, form, input, button, select, svg, meta, link, base, every `on*`
handler, `target`, `javascript:` and `data:` urls, except `data:image/*`
raster images on `img`.

## Why the content hash is safe

The hash is computed from the **live** page at the crawler's exit, before
storage, and compared with the stored hash on the next run. Cleaning the stored
copy does not enter that comparison, so it cannot produce a false `modified`.

Measured on `tests/fixtures/*.html` and across 53 exported workbooks (60,637
rows carrying HTML: CBB including volumes 1-7, QFC, SAMA, CFPB, CBE, MHRSD,
MC, QCB, CBK Kenya, MISA and the US regulators): **zero rows lose visible
text.** The stored copy differs only in ways a reader cannot see: a bare `&`
is written back as `&amp;`, U+00A0 as `&nbsp;`, comments are gone, and a
removed tag leaves no space where the fingerprint's regex would put one.
That matters only to a script that re-hashes **stored** HTML
(`scripts/backfill_mc_hashes.py`). If that is ever needed, the fix is to decode
entities in `crawler/fingerprint.py` `text_of_html`, not in the cleaner.

## What the scan found (workbooks, 2026-11-03)

| Dropped | Count | Verdict |
|---|---|---|
| `<glossary type="i">` (CBB, QFC) | 98,074 | Thomson Reuters rulebook tag around a defined term; the word stays, the tag goes |
| `<ref book= rule=>` (QFC) | 1,905 | same family, a cross-reference; the words stay |
| `target` on links | 16,236 | by design; the display hook sets `_blank` |
| `bordercolor` on tables (QFC) | 12,500 | presentation only; **added to the allowlist** so the tables look the same |
| `data-*`, `aria-*`, `role`, `tabindex`, `itemprop` | ~7,000 | Drupal, Wagtail and SharePoint bookkeeping |
| `onclick` on MC accordion headings | 44 | handler removed; the heading and its text stay, it just no longer folds |
| `<o:p>`, `<sc>`, Word-export attributes (SAMA) | ~60 | Word artefacts; 30 of 6,103 SAMA rows touched at all |
| option, select, input, form, button, svg icons, iframe, meta, style bodies, Drupal placeholders | ~500 | the forbidden list working |

One allowlist change from the workbooks: `bordercolor`. Every other drop is
bookkeeping, a handler, or a custom tag whose text survives. 416 workbook
cells held the overflow marker and were skipped; the database run covers them.

## What the scan found (database, 2026-11-04)

Run against `uc2-db_2026-09-22.bak` restored into SQL Server 2025 Express on
the development laptop (the shared dev server at 10.11.12.76 is not reachable
from outside the office network). 42,444 of 46,938 regulation rows carry
HTML; 23,365 would change; the scan takes about a minute.

| Finding | Count | Verdict |
|---|---|---|
| `<img src="data:image/png;base64,…">` (SAMA 63 rows, MC 1) | 64 | real figures pasted into the rulebook; **allowlist changed**: `data:image/*` raster types are now kept on `img@src` only, and nowhere else |
| `javascript:` hrefs (MC article modals 103, CMA empty anchors 43) | 146 | by design |
| SAMA links with U+00A0 inside the url | 2 | broken on the live site too; dropped |
| CBB link to an `.xls` form | 1 | inside an HTML comment the regulator commented out; never visible |
| CBE 9915 "Monitoring Framework": labels of a JavaScript-drawn SVG chart (`foreignObject`) | 1 row, 335 chars | the only row whose visible text changes; the same terms are in the paragraph above the chart. Accepted. |
| `<glossary>` (CBB) | 47,443 | as in the workbooks |
| `a@target`, `data-*`, `aria-*`, `role`, `tabindex`, Drupal placeholders, `<o:p>` | ~17,000 | as in the workbooks |

ZATCA, SIO, CMA, CBE, LMRA, PDPA, RERA and SIMAH rows, which the workbook
scan never saw, show nothing new. The full dropped ranking is in the scan
output; re-run with `python -m scripts.html_clean_scan --samples 5 --text`
against any copy of the database.

The database copy was 22 September 2026. Rows added since then live in the
workbooks that were scanned separately.

## End-to-end: two CBK Kenya crawls into the local copy (2026-11-04)

Pass one inserted all 234 documents through `_insert_regulation`; pass two
classified 234 unchanged, 0 modified, and no stored hash or `updated_at`
moved. The three HTML rows match `clean_html` of the raw October workbook
markup, with identical visible text and content hash. Details in
[chg_001_handoff_2026-11-04.md](chg_001_handoff_2026-11-04.md).

## Not done on this branch

- Old rows are not rewritten. CHG-002's sandbox covers them, and a rewrite can
  be run later from the scan script if wanted.
- Images are still fetched from regulator servers when a page is displayed.
  Separate decision.
- `tests/test_formfill_snapshot.py` fails one test when run in the same
  process after `tests/test_fingerprint.py`, on `main` as well as here. Run it
  on its own.
