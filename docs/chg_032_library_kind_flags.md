# CHG-032: a library entry that is both a page and a folder (2026-11-20)

Branch `feature/chg-032-library-kind-flags`, from `main` at 968f3cf.
Use case UC-02. The user's words: "add an additional flag has_children
to solve the problem of an entry being both a page and having children".

## What was wrong

One row of `compliancecategory` can own a document and have sub-categories
at the same time. SAMA's "Banking Control Law" (node 1275) is stored as a
regulation with the law's full text, and it is also the parent of 26
Article nodes, each stored as its own regulation. The table's `type`
column holds one letter, F (folder) or R (regulation), set once when the
node is created: by position in the document's path
(`Orchestrator._walk_folders`: last segment R, every earlier segment F) or
copied from the workbook (`dynamic_crawler/formfill/promote.py`, default
F). Nothing ever revised it, so the letter said how a node was born, not
what it held, and it could not say "both".

Measured 2026-11-20 on the local copy of the database (2026-09-22 data
plus the change branches' test rows):

| | |
|---|---|
| category nodes | 60,373 (F 13,374; R 46,999) |
| nodes that own a document AND have children | 968 |
| of which typed R (the document arrived first) | 948 |
| of which typed F (the folder existed first) | 20: CMA 3, CBE 17 |
| nodes typed R that own no document | 228, all childless (Bahrain 126, KSA 93, Diagnostic 9) |
| nodes owning more than one document | 155 (CMA "Prospectuses" holds 12 in one node) |

The web page (`web/`) could not trust the letter, so for every R node it
fetched the node's children to choose between a folder icon and a document
icon, one request per card, and past 60 cards it stopped asking and guessed
from the letter. When a node had children, the page listed only the
children and dropped the node's own document on purpose: the full text of
"Banking Control Law" was not reachable by browsing, only its articles
were. 968 stored texts were hidden that way.

## What it does not do

- No third `type` value such as B. It would have to be maintained on every
  child insert under an R node and on every document filed into an F node,
  and would go stale the first time a path forgot. Whether a node has
  children is derived from `parentid` when read.
- No change to the identity rule, the folder-tree rule, or what a document
  is. No crawler is touched.
- No deletion. The 228 R nodes without a document are reported, not
  written (see "The stored rows").
- No change to the UC-15 panels of the page (Requirements and Activities
  tab, Model and Usage).

## Design

1. **The API sends the two facts and the answer.**
   `apis.pipeline_api._attach_regulation_counts` already marked every
   category row with `has_regulations` and `regulation_count` from one
   unfiltered `GROUP BY` over `regulations`. It now runs a second aggregate
   of the same shape over `compliancecategory` grouped by `parentid` and
   marks `has_children` and `child_count`. Zero parameters, two queries
   however long the list, for the same reason the first one is written
   that way (measured 2026-08-28: a per-id `IN` list broke the ODBC
   parameter limit on the full tree). Each row also carries `kind`,
   derived by `category_kind`:

       both      owns at least one regulation and has at least one child
       document  owns at least one regulation, no children
       folder    everything else (including an empty node)

   Every endpoint that returns categories gets the fields for free:
   `/categories`, `/categories/roots`, `/categories/root`,
   `/categories/children/{id}`, `/categories/subtree/{id}`.
   `/regulation/{id}` adds `category_info.child_count` and
   `category_info.kind` (one correlated count on the single row), so the
   detail page knows whether the document has articles beneath it.

2. **`type` means "owns a document", and the repository keeps it true.**
   `MSSQLRepository.mark_category_owns_document` runs one guarded update
   (`SET type = 'R' WHERE compliancecategory_id = ? AND type <> 'R'`) on
   the cursor of the transaction that files a regulation into a node:
   at the end of `_insert_regulation`, and in `update_regulation` whenever
   `compliancecategory_id` is among the fields. A no-op on a node that is
   already R. This covers the orchestrator, promote and any other writer in
   one place, so the "folder existed first" drift cannot recur. The
   orchestrator walk is unchanged in logic; its docstring now says where
   the retype happens.

3. **The page renders by `kind`** (`web/app.js`, the Library view).
   - The icon is chosen from `node.kind`: a document icon for `document`,
     a folder icon otherwise, and for `both` a folder icon with a small
     document badge. No lookahead fetch per card and no 60-card cap:
     rendering the root listing makes one request where it used to make
     one per R node.
   - A `both` node opens as a folder whose first row or card is its own
     document (title from `regulations.title`, fetched with the existing
     `/regulations/by-category/{id}`), then the sub-categories. A node
     holding several documents lists each of them. Nothing is hidden and
     nothing is duplicated.
   - A `document` node has no caret in the tree and opens directly, as
     before, now without a children fetch.
   - The detail view of a document whose node has children shows
     "Contains N sub-sections" with a link that browses into the node:
     the way back down from the whole law to its articles.
   - Opening a `both` node's own document from the grid pushes the node
     again on the breadcrumb stack, so Back returns to that listing and
     not to the level above it.
   - The "hide dead-end branches" lookahead is gone. A folder with no
     children and no document is dropped from a listing on the counts
     alone. A folder whose children are all such dead ends still shows
     and says "(empty)" when opened; the fix for those is in the category
     data (the 228 orphans below), not in the page.
   - Against an API older than this change the page falls back to the
     letter (R = document, else folder), which is what it did before.

4. **The stored rows** (`scripts/chg032_retype_categories.py`, dry run by
   default, `--apply` to write). Lists the F nodes that own a document and
   retypes them to R, writing every original value to
   `output/backup_chg032_<stamp>.json` first and verifying by re-reading.
   Lists the R nodes that own no document to
   `output/chg032_orphans_<stamp>.json` with their child count and branch
   root. All 228 are childless, so each is a dead leaf left behind by a
   failed insert or a document that moved folders. The user chose deletion
   on 2026-11-20: `--delete-orphans --apply` backs the childless ones up
   to `output/backup_chg032_orphans_<stamp>.json`, deletes them with the
   three selection conditions re-checked in the `DELETE` itself (typed R,
   no regulation, no child), and verifies none remain. Checked before
   deleting: the only foreign keys to `compliancecategory` are its own
   `parentid` and `regulations.compliancecategory_id`, and no regulation
   points at any of the 228. An orphan WITH children is a folder in all
   but letter and is never deleted by the script. A later crawl of the
   same site may create such a leaf again for a document whose insert
   fails; that is the orchestrator's existing behaviour, not a regression.

## What was checked

- `tests/test_chg032_kind_flags.py` (13 tests, no database): the four
  kind answers; every row gets both facts and the kind; the helper runs
  exactly two unparameterised aggregates whatever the list size; the
  retype is one guarded update, inside the insert's transaction, after the
  `ref_key` update and before the commit; a move retypes the new node; an
  update that does not move the regulation retypes nothing.
- Live against the local copy through the API (2026-11-20): node 1275
  "Banking Control Law" returns `kind: both`, `regulation_count: 1`,
  `child_count: 26`; node 4878 "Prospectuses" (typed F) returns
  `kind: both`, `regulation_count: 12`, `child_count: 3`;
  `/regulation/1213` returns `category_info.child_count: 26`,
  `kind: both`; `/categories` returns 60,373 rows, every one with a kind
  (document 45,823; folder 13,582; both 968).
- Headless Chromium against the local API (scratch script `ui_check.py`):
  the grid for node 1275 shows 27 cards, the own document first and 26
  Article folders after it; opening the own document shows "Contains 26
  sub-sections"; "Browse them" returns to the listing with the own
  document first; Back from the own document returns to node 1275, not
  to its parent; the tree expands node 1275 to 27 rows with the own
  document first and the badge on the node; rendering the root listing
  makes zero `/categories/children` lookups; no page errors.
- Data fix applied on the local copy on 2026-11-20: 20 nodes retyped
  F to R, verified; then the 228 childless orphan R nodes deleted,
  verified none remain; both backed up in `output/`. The report after
  both runs: 60,145 nodes (F 13,354; R 46,791), 968 of kind both,
  0 orphans. Nothing on the shared database was touched.
- Full offline suite on the branch, 2026-11-20: 560 passed, 19 failed,
  1 skipped, 32 errors (`--continue-on-collection-errors`; three files
  never collect on this laptop). Main at 968f3cf, run the same day in a
  throwaway worktree: 547 passed, 19 failed, 32 errors, the same names (test_js_snippets_parse,
  test_stage_a, test_stage_b, test_formfill_snapshot, test_identity's
  mhrsd.yml config check, the formfill browser tests); the 13 extra
  passes are this change's tests. No failure is in a file this change
  touches.

## Overlap with CHG-026

The CHG-026 branch (unmerged) carries an earlier, smaller version of two
pieces of this change: `child_count` in the counts helper and the
own-document row in the page. This change supersedes both. Whichever
branch merges second will conflict in exactly those two regions of
`apis/pipeline_api.py` and `web/app.js`; the resolution is to keep this
branch's version, which is a superset.

## Not in this change, for the user to decide

- Whether `/regulations/by-category/{id}` should stop returning
  `document_html` for every row. The page uses it only to list titles;
  for a node holding many documents the response is heavy. Pre-existing.
- The dead-end chains the old lookahead hid (a folder whose only children
  are empty folders). They are category data, from crawl-time navigation
  items that were never populated.

## Handoff

Files: `apis/pipeline_api.py` (`category_kind`,
`_attach_regulation_counts`, `/regulation/{id}`), `storage/mssql_repo.py`
(`mark_category_owns_document`, `_insert_regulation`, `update_regulation`),
`orchestrator/orchestrator.py` (docstring only), `web/app.js`,
`web/style.css`, `scripts/chg032_retype_categories.py`,
`tests/test_chg032_kind_flags.py`, this document.

Run: `python -m pytest tests/test_chg032_kind_flags.py -q`; start the API
(`python -m uvicorn apis.pipeline_api:app --port 8000`) and open
`web/index.html`; `python -m scripts.chg032_retype_categories` for the
report, `--apply` to retype, `--delete-orphans --apply` to also delete
the childless orphan nodes. On the shared database: dry run first, then
both writes on the user's word.

Not committed; the user commits.
