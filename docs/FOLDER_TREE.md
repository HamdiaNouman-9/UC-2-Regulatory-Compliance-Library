# Folder tree: a folder is reused only when its whole path matches

Changed 2026-09-29. Affects every regulator; in practice only CBB's output changes.

## The rule

A document's `doc_path` is walked segment by segment into `compliancecategory`.
At each step a folder is **reused only if one with the same title already exists
under the same parent**, meaning the whole path down to it is identical.
Otherwise a new folder is created.

So two folders with the same name in different places are two folders. The
regulator put them there, and we copy that structure.

The leaf rule is unchanged: a document's own node (type `R`) is never shared
with a different document.

## What was wrong

Both the tree walk (`orchestrator._walk_folders`) and `promote` had a fallback.
When `(title, parent)` found nothing, `find_folder_in_subtree(title, parent)`
reused any folder with that title **at any depth** below the parent.

The CBB rulebook has two real folders called "Quarterly Updates" in each volume:

```
Volume 1—Conventional Banks > Quarterly Updates
    -> January 2024, October 2023, ...   (the update LETTERS)

Volume 1—Conventional Banks > Part A > Introduction > UG Users' Guide
    > UG-3 Rulebook Maintenance and Access > UG-3.1 Rulebook Maintenance
    > Quarterly Updates
    -> UG-3.1.1 .. UG-3.1.5              (rules describing the process)
```

Part A is walked first. When the letters arrived, the volume-level "Quarterly
Updates" was not found as a direct child, so the fallback found the deep one and
filed every letter under the Users' Guide. Measured 2026-09-16: 63 letters in
Volume 1, and Volumes 2–6 were the same. `doc_path` was always correct; only the
tree was wrong.

## What changed

| File | Change |
|---|---|
| `orchestrator/orchestrator.py` `_walk_folders` | subtree fallback removed |
| `dynamic_crawler/formfill/promote.py` `resolve_folder` | subtree fallback removed. Without this, a correct workbook would collide again against a same-named deep folder already in the DB |
| `scripts/reparent_cbb_quarterly_updates.py` | docstring: only needed for workbooks exported before 2026-09-29 |
| `crawler/cbb_crawler.py` | stale comment updated |

`find_folder_in_subtree` itself is left in `storage/mssql_repo.py` and
`excel_repo.py`. Nothing calls it any more.

## Why this is safe for other regulators

A depth cap was tried and reverted on 2026-09-16 over cross-regulator risk, so
this time it was measured first. Every workbook in `output/workbooks/` (15 of
them: CBB non-rulebook, CBE ×5, QCB, QFCL with 8,034 rows, NCA ×2, MLCU,
Bahrain Bourse, Civil Defense ×2, ZATCA) was replayed from an empty tree with
and without the fallback. The result was **identical trees, with 0 rows filed
differently**. The fallback fired only for the CBB rulebook collision.

A synthetic replay of the two Quarterly Updates paths above reproduces the bug
with the fallback, and files both correctly without it.

The fallback's original stated reason was a page whose trail skips an
intermediate level (for example, a missing "CBB Rulebook"). Under the new rule,
such a page gets its own folder chain that matches its `doc_path`. That is
consistent: the tree follows `doc_path`.

## Fixing volumes already in the database

Rows promoted before this change still point at the UG-3.1 node, and `promote`
skips documents that already exist, so it will not move them.
`scripts/fix_cbb_volume_tree.py` repairs one volume at a time, in place:

```
python -m scripts.fix_cbb_volume_tree --volume 1 --workbook <path>\cbb_vol1.xlsx   # dry run
python -m scripts.fix_cbb_volume_tree --volume 1 --apply
python -m scripts.fix_cbb_volume_tree --volume 5 --only-section "Quarterly Updates" --apply
```

- **It moves nodes, not documents.** For each row it compares the tree chain
  with `[country] + doc_path`. Where they differ, it creates any missing folder
  using the exact-path rule and sets the leaf node's `parentid`. No regulation,
  version or analysis row is touched, and all ids stay the same.
- **Why not delete and re-promote the corrected workbooks?** That gives about
  8,600 rows per volume new ids and loses their version history and analysis, just
  to move about 63 letters. The corrected workbooks differ from what was promoted
  only in the tree.
- **It is safe.** Dry run by default. `--workbook` cross-checks that the DB holds
  the same documents as the file. Each run writes a backup of every move to
  `output/backup_cbb_tree_*.json`. Writes run in one transaction and are verified
  against `doc_path` before commit, and rolled back if the check fails.
- **Volume 5 needs `--only-section "Quarterly Updates"`.** Its 320 "Specific
  Modules" rows also disagree with `doc_path`, and that has not been checked
  against the site.
- **Tested offline 2026-09-29** by putting the corrected Vol 1 and Vol 5
  workbooks back into the old shape. Vol 1: 63 letters moved, 1 folder created,
  8,582 rows untouched. Vol 5 with the flag: 45 letters moved. Re-running the
  fix moves 0.

## What this does NOT fix
- **Old workbooks.** A `cbb_vol*.xlsx` exported before 2026-09-29 still has the
  wrong tree. Re-export it, or run `scripts/reparent_cbb_quarterly_updates.py`.
