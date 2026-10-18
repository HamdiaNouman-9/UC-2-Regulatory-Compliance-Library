# Ministry of Commerce titles: from "Laws" to the law's name, everywhere

2026-10-18. Changed together, in one step:

| What | File |
|---|---|
| Crawler emits the new title and `doc_path` | `crawler/mc_crawler_wrapper.py` (`MC_TITLE`) |
| Stored rows and tree leaves | `output/mc_title_fix/mc_title_fix.sql` (run by hand) |
| Local monitoring state | `scripts/mc_title_fix_state.py --apply` (run after the SQL, on every machine that runs MC monitoring) |

## Problem

All 73 Ministry of Commerce documents were named just `Laws` (27), `Regulation` (19) or
`Attachments` (27), in three places: `regulations.title`, the last element of
`regulations.doc_path`, and the regulation's own `R` leaf in `compliancecategory`.
This followed a 2026-08-14 decision: the law is the folder, so its name would not be repeated. Outside the
tree (detail, requirements screens, lists, search), 27 documents all called "Laws"
cannot be told apart.

## Change

| Part | Before (title, doc_path[3], R leaf) | After (all three) |
|---|---|---|
| law | `Laws` | `Commercial Books Law` |
| regulation | `Regulation` | `Commercial Books Law - Regulation` |
| attachment | `Attachments` | `Commercial Books Law - Attachments` |

The tree now reads `Commercial Books Law > Commercial Books Law - Regulation`. This
repeats the law's name, which was accepted as the price.

The law name comes from `extra_meta.law`, which all 73 rows have. The 73 new titles are distinct.
**Not changed:** the law folders (type `F`), `doc_path[0..2]`, versions,
requirements and activities.

## Why all three, and the crawler, in one step

- **`title`** feeds `_hashless_unchanged`, where a mismatch reads as `modified` (a new version and an LLM
  re-analysis). It is also part of the multi-file Attachments' `identity_fields` (26 of 27),
  where a mismatch reads as a new document plus a disappeared one, which feeds the withdrawal gate.
- **`doc_path`** is in every identity tuple (`find_by_identity` matches
  `document_url` and `doc_path`).
- **The `R` leaf** is found by the next crawl's folder walk using `doc_path`'s last
  element under the law folder. If they disagree, the walk creates a new leaf,
  re-points the regulation, and orphans the old one.

If any one of these drifts from the others, the next crawl misreads every Ministry of Commerce row.

## Measured before writing the SQL

- 73/73 rows consistent: `title` = `doc_path[3]` = leaf title = the part label, the leaf
  is type `R`, the leaf's parent is the law folder, `doc_path[2]` = `extra_meta.law`.
- 0 shared leaves and 0 orphan leaves under the Ministry of Commerce law folders.
- All law names are plain ASCII with no `"` or `\`, and every stored `doc_path` is
  exactly `json.dumps(list)`. So the SQL rebuilds the last element by text, producing
  the same bytes the crawler writes. This was verified for 73/73 rows with a read-only
  query against `MC_TITLE`.

## Safety in the SQL

- One transaction. It aborts unless every row is exactly in the measured shape.
- It updates leaves, then regulations.
- It rolls back unless title, `doc_path[3]` and the leaf agree for every row,
  `doc_path` is valid JSON with 4 elements, no generic title remains, and no two Ministry of Commerce
  titles collide.
- It can be rerun safely: already-renamed rows are not selected.
- One statement (DBeaver-safe). Compiled against prod with `SET NOEXEC ON`.

## Local state

`output/change_state/ministry_of_commerce_regulations_and_laws.json` keys its 42 records
by `document_url + doc_path`. `scripts/mc_title_fix_state.py` rewrites the keys and fields
(it keeps a `.bak` and can be rerun safely). No Ministry of Commerce workbook exists in `output/workbooks/`,
so there is nothing stale to promote.
