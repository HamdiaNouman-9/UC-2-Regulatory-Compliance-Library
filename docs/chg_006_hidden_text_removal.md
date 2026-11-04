# CHG-006: hidden text is removed from the model's copy (started 2026-11-04)

Branch `feature/chg-006-hidden-text-removal`, from `main`. First of the
prompt-injection changes in the register; CHG-007, CHG-008 and CHG-009 follow.

This file is kept up to date as the steps land. Steps not yet done are
marked.

## What was wrong

The text handed to the requirement and activity analysers was everything the
page or file contained, visible or not. `decide()` in
`dynamic_crawler/formfill/textinput.py` took the crawler's `content_text` or
the raw `document_html`; the live-page fallback flattened the page with
BeautifulSoup dropping only script, style, noscript, header, footer and nav;
PDFs went through PyMuPDF `page.get_text("text")`, which returns every glyph
whatever its colour, size or position. Hidden text is the one way to plant an
instruction a human reviewer will never notice, and the model read it.

## Design

One module, `processor/hidden_text.py`, produces the reader's view:

- `visible_html_text(html)` removes elements a reader cannot see, by the
  register's list and inline styles only: `hidden`, `aria-hidden`,
  `display:none`, `visibility:hidden`, `opacity:0`, zero font size, zero
  box, far off-screen position or text-indent, collapsing clip. Stylesheets
  are not evaluated, so a section hidden by a class a reader can click open
  is kept, as is `<details>`. Script, style, noscript and template go as
  code and are not counted.
- `visible_pdf_page_text(page)` walks PyMuPDF spans and drops those under
  four points, outside the page box, or within a small colour distance of
  the background drawn under them (white, or a filled rectangle; under an
  image the background is unknown and the span is kept). Render mode is
  deliberately not a criterion: an OCR layer over a scanned page is
  invisible text and is the only text that page has.

Both return the text, the characters removed, a tally of reasons and a few
samples. Nothing stored changes: the html, the file and the content hash
stay exact. The counts go to `extra_meta` so a person can see them, and so
CHG-008's quarantine can act on "more than ten percent hidden".

## Steps

| # | Step | State |
|---|---|---|
| 1 | Branch from `main` | done |
| 2 | `processor/hidden_text.py` and `tests/test_hidden_text.py` | done |
| 3 | Measure before wiring: `scripts/hidden_text_scan.py`, read-only, over the local database copy per regulator and 22 PDFs on disk | done |
| 4 | Wire the HTML side: `decide()` prunes `document_html` and, only when something was hidden, uses the pruned markup instead of `content_text`; the live-page fetch prunes the same way | done |
| 5 | Wire the PDF side inside `extract_text_from_pdf_smart`, scanned and OCR paths untouched; counts in the extractor's metadata | done |
| 6 | Record the counts in `extra_meta["hidden_text"]` through a new `set_extra_meta` that leaves `updated_at` alone | done |
| 7 | The wired gate over every stored page (95 hidden, 2,995 clean, see below); one Ministry of Commerce orchestrator pass into a workbook stand-in with analysers off: 57 crawled, 57 new, 57 rows carry `extra_meta["hidden_text"]` | done |
| 8 | Handoff: [chg_006_handoff_2026-11-04.md](chg_006_handoff_2026-11-04.md) | done |

## Orchestrator pass (Ministry of Commerce, 2026-11-04)

Same crawler and orchestrator as the scheduler's job, `analyse=False`, into
an empty workbook (ExcelRepo) rather than the database copy, because MC's
rows already exist there and would classify `unchanged`, which never
reaches the text decision. 57 documents crawled across 20 laws in 14
minutes, all `new`; all 57 came back with `extra_meta["hidden_text"]`
filled by `_record_hidden_text` through `set_extra_meta`, with the
`hidden_text` step logged for each. Nine of those rows have an
`extra_meta` larger than an Excel cell (their `content_text` is the whole
law), so in the workbook the cell holds the overflow marker and the full
value, counts included, sits in the workbook's sidecar file; in the
database the column has no such limit. Every hit is MC's own furniture: the
"Number of parts / chapters / articles" bar hidden with `display:none` (90
characters on the law pages) and the attachment labels on the small
attachment pages (about 215 characters, most of those pages). The live
pages hide that bar on every law page now; the September database copy had
it on 26 rows only, so the regulator's template has changed since. No file
reported hidden text. `updated_at` on the rows was not bumped by the
recording.

## What the measurement found (2026-11-04, before wiring)

**Stored pages, local database copy, 42,448 rows, 34 seconds.**

| | |
|---|---|
| pages with any hidden text | 95 |
| characters removed / kept | 8,495 / 41,031,947 |
| pages over the 10 % quarantine ratio | 15 |
| rows carrying a crawler `content_text` | 36,312 |

Every hit is site furniture, not content and not an instruction: Ministry
of Commerce rows hide attachment labels and `null` placeholders with
`display:none` (26 rows); CMA hides SharePoint's "Page Content" label and
one row's "Follow" menu (66 rows, 4,525 characters on row 8555); SIO hides a
"Read More" button with `visibility:hidden`. No regulator hides anything a
reader would want. **For CHG-008:** the 15 rows over ten percent are tiny
landing pages of 50 to 400 characters where a hidden label is most of the
page. A ratio alone would quarantine genuine rows; the trigger needs a
minimum absolute size as well (several hundred characters removed).

**`content_text` versus `document_html`.** `decide()` prefers the crawler's
`content_text`. For 36,249 of the 36,312 rows that have both, it contains at
least 80 % of the visible page text, so it is the same content flattened.
The hidden snippets appear inside `content_text` in only 4 rows. Wiring
plan: when `document_html` is present and the visible-text pass removes
something, the visible text replaces `content_text` as the page input;
otherwise today's choice stands, so 42,353 rows see no change of input and
no re-analysis.

**PDFs on disk, 22 files, 1.26 million characters.** Nothing hidden in any
of them, with one false alarm that changed the design: a CBK Kuwait
framework PDF carries a ghost copy of every page drawn one page-width to
the left, 2,552 spans, all exact copies of text visible on other pages.
PyMuPDF's default extraction already dropped them silently. Counted as
hidden they were 28 % of the document. They are now recognised as
duplicates through a document-wide pool of visible text and reported
separately from hidden text.

## The wired gate against the stored rows (2026-11-04)

`decide()` run over every stored page, with each row's own `content_text`
and URL, no fetching: all 95 pages with hidden text come back with
`hidden_text` filled and the hidden snippet absent from the model input
(the one apparent exception, MC row 131, hides the word "null", which also
appears legitimately elsewhere on the page). None of the 95 is skipped by
the gate. A random 2,995 clean pages give an input, type and skip decision
identical to the old gate's, byte for byte, so their analysis input hash
does not move. 99 seconds for the lot.

## What changed (so far)

| Where | What |
|---|---|
| `processor/hidden_text.py` | the two functions and the `VisibleText` result (`text`, `removed_chars`, `reasons`, `samples`, `ratio()`, `as_meta()`) |
| `dynamic_crawler/formfill/textinput.py` | `decide()` now prepares the page: `strip_hidden_html` on `document_html`; if anything was hidden the pruned **markup** replaces `content_text` as the page input (the analyser's own normaliser still renders its tables), otherwise the choice is exactly what it was. The gate and the html-versus-file choice moved unchanged into `_decide()`. `Decision.hidden_text` carries the counts; `str(dec)` says "N hidden chars removed" |
| `orchestrator/orchestrator.py` | `_safe_page_text` prunes the live page the same way and reports through `Decision.hidden_text`; `_download_and_extract_pdf` keeps each file's counts; `_record_hidden_text` writes `extra_meta["hidden_text"] = {"page", "files"}` after the text decision, only when something was hidden, and never raises |
| `processor/Text_Extractor.py` | `extract_text_from_pdf_smart` reads each page through `visible_pdf_page_text` with the document-wide duplicate pool, before the bad-page and broken-text checks; `metadata["hidden_text"]` added (zeros when clean). Scanned and OCR paths untouched. The crawlers that call it (CBI, LMRA, MLCU, MLSD, NBR) see one extra metadata key and nothing else |
| `storage/mssql_repo.py`, `dynamic_crawler/formfill/excel_repo.py` | `set_extra_meta(id, json)`: extra_meta only, `updated_at` untouched, because `update_regulation` bumps it and a note about a change must not read as a second change |
| `tests/test_hidden_text_gate.py` | hidden text gone from the model input and recorded; wins over `content_text`; a clean page gives the identical input and type as before; the gate still skips an all-hidden page; recorded when a file duplicates the page |
| `tests/test_hidden_text_recording.py` | `set_extra_meta` in both repositories leaves `updated_at` alone while `update_regulation` still bumps it; the recorder writes page and file parts once, writes nothing when clean, never raises; the PDF extractor reports and removes white text on a real three-page file |
| `scripts/hidden_text_scan.py` | count-only scan of stored pages (per regulator, reasons with samples, rows over the quarantine ratio, `content_text` presence) and of PDF files; no apply mode |
| `tests/test_hidden_text.py` | every hiding technique on the list removes and names its reason; ghost copies within a page and across pages are duplicates, not hidden; collapsed sections, details, faint and small-but-readable text stay; PDF white-on-white, near-background on a box, tiny and off-page go; white on a dark box and text over an image stay; layout equals plain extraction when nothing is hidden |

## Decisions

- **Inline styles only for pages.** Evaluating stylesheets would need a
  browser; the register's list is about inline hiding, and a class-hidden
  section is one a reader can open.
- **Off-page PDF text is counted, not silently dropped.** PyMuPDF clips
  extraction to the page box (or crop box) by default, which would have
  hidden the hiding. Extraction runs with an infinite clip so the text is
  seen, counted and then removed. Clearing the mediabox flag alone was not
  enough; the explicit clip is what works.
- **Background under a span, not the page's white.** White text on a dark
  heading bar is normal in regulator PDFs. The fill drawn under the span's
  centre is the background; an image there means unknown, keep.
- **Render mode is not a criterion.** OCR layers are invisible text by
  design.
- **A dropped span that duplicates visible text is not hidden content.**
  Found by the first PDF measurement: a CBK Kuwait framework PDF carries a
  ghost copy of every page one page-width to the left, 99,018 characters
  that PyMuPDF's default extraction already excluded. Counting it as hidden
  would have put a genuine document over the quarantine threshold. Such
  spans are dropped but reported as `duplicate_chars`, outside the ratio.
