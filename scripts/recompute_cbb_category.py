"""Give CBB's `category` column the section it belongs to, from `doc_path`.

    python -m scripts.recompute_cbb_category                    # dry run
    python -m scripts.recompute_cbb_category --apply
    python -m scripts.recompute_cbb_category --dir output/workbooks/cbb

THE PROBLEM
-----------
`crawler/cbb_crawler.py` `_rulebook_doc_to_regulatory` sets

    category = doc.doc_path[1] if len(doc.doc_path) > 1 else "CBB Rulebook"

and `doc_path[1]` on the rulebook is ALWAYS the literal "CBB Rulebook". So every
row in a volume carries one category value. MEASURED 2026-09-16 over the nine
workbooks in output/workbooks/cbb:

    cbb_vol1.xlsx   8,645 rows   1 distinct category
    cbb_vol2.xlsx   8,164 rows   1
    cbb_vol6.xlsx   5,637 rows   1
    ... every volume the same

CRAWLING_OVERVIEW.md §2 defines the column as "the section it belongs to (from
the site's structure)". One value for 8,645 rows is not that, and it has a
visible cost: the rulebook's "Quarterly Updates" section -- 63 quarters per
volume, each an update letter PDF -- is findable ONLY by substring-matching
`doc_path`. Its `title` is a bare month ("January 2024"), its `category` is
"CBB Rulebook", its `source_system` is "CBB-Rulebook-Vol-1". A reader filtering
the column the schema tells them to filter finds nothing, which is how a
complete section came to look missing.

The same flatness reaches cbb.xlsx from different lines (the hardcoded
`category = "..."` assignments around cbb_crawler.py:347 and :487): 1,275 rows,
7 distinct values, while the module level below them holds 26.

THE RULE
--------
The section is the deepest folder that is NOT the document, at a fixed depth per
shape -- not `doc_path[-2]`, which is the immediate parent and far too granular
to filter on (measured 717-2,013 distinct values per volume):

    rulebook (doc_path[1] == "CBB Rulebook")   ->  doc_path[3]   the module
    everything else                            ->  doc_path[2]   the module

and where that index would land ON the leaf (a path too short to have a section),
the current `doc_path[1]` is kept. MEASURED, this run:

    cbb.xlsx        1,112 of 1,275 rows change    87 keep the fallback   26 distinct
    cbb_vol0.xlsx      95 of    97                 2                      5
    cbb_vol1.xlsx   8,645 of 8,645                 0                      6
    cbb_vol2.xlsx   8,164 of 8,164                 0                      6
    cbb_vol3.xlsx   3,177 of 3,177                 0                      5
    cbb_vol4.xlsx   3,005 of 3,005                 0                      5
    cbb_vol5.xlsx   4,722 of 4,722                 0                      6
    cbb_vol6.xlsx   5,637 of 5,637                 0                      5
    cbb_vol7.xlsx     170 of   170                 0                      4

WHY THIS IS A RECOMPUTE AND NOT A HAND-EDIT
-------------------------------------------
docs/ONBOARDING.md forbids editing a workbook to make an error go away, because
"the next crawl reproduces it, and the library ends up disagreeing with the
site". That rule is about masking a crawler bug, and it does not describe this
change: the workbooks already AGREE with the site. Verified 2026-09-16 against
cbben.thomsonreuters.com -- every volume's Quarterly Updates count matches the
site exactly (63/63, 52/52, 45/45, 33/33, 14/14), including the site's own
internal gaps. Nothing is missing. `category` is a DENORMALISED COPY of a level
of `doc_path`, and `doc_path` -- the identity field, the thing the folder tree is
built from -- is correct. This recomputes the copy from the original, in the same
file, changing no other column.

    !! THE CRAWLER LINE MUST BE FIXED IN THE SAME CHANGE. !!

Without it the next `tools.workbook export` writes the flat value again and the
workbook silently reverts -- which IS the situation ONBOARDING warns about. This
script refuses to look like the whole fix: it prints that reminder every run.

WHY IT IS SAFE TO CHANGE IN PLACE
---------------------------------
`category` is the one column that carries no structural weight. Checked in the
code, not assumed:

  * IDENTITY is (document_url, doc_path, title) -- tools/workbook.py:71. `category`
    is not in it, so no row changes identity and none can collide.
  * CBB's fingerprint is MD5(content_text) -- cbb_test_crawlers/cbb_rulebook_crawler.py.
    `category` is not hashed, so nothing reclassifies `modified` or gains a
    version row.
  * `promote` builds the database folder tree from the `compliancecategory` SHEET
    and `doc_path`, never from this column -- dynamic_crawler/formfill/promote.py.
    The tree is untouched, and so is every `compliancecategory_id`.
  * No classification or change-detection path reads it.

No sidecar is involved: the longest text in any CBB workbook is 11,618 characters,
under Excel's 32,767 cell cap, so no `.fulltext.json` exists to keep in step.

HOW IT WRITES
-------------
Never over the original. Each workbook is rewritten to `<name>.recat.xlsx`, that
copy is REOPENED and verified -- every sheet's row count unchanged, and
document_url/doc_path/title identical on every row -- and only then swapped in,
with the original kept as `<name>.pre-recat.xlsx`. If verification fails the
original is left exactly as it was and the run stops.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from collections import Counter
from pathlib import Path

import openpyxl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_DIR = REPO_ROOT / "output" / "workbooks" / "cbb"

#: The level of `doc_path` that names the section, per shape. Keyed on
#: doc_path[1], which is the source's own top folder.
RULEBOOK_TOP = "CBB Rulebook"
SECTION_INDEX_RULEBOOK = 3
SECTION_INDEX_DEFAULT = 2

#: Compared before and after the rewrite. These three ARE the row's identity
#: (tools/workbook.py IDENTITY), so if any one of them moved, the rewrite did
#: something this script has no business doing.
GUARDED = ("document_url", "doc_path", "title")


def _flat(v) -> str:
    return " ".join(str("" if v is None else v).split())


def _parts(doc_path) -> list:
    return [p.strip() for p in _flat(doc_path).split("|") if p.strip()]


def section_for(doc_path) -> tuple:
    """(section, used_fallback) for one doc_path.

    The leaf is the document itself, so an index at or past it means this path
    has no section level and the source's own top folder is the best answer.
    """
    p = _parts(doc_path)
    if not p:
        return "", True
    i = SECTION_INDEX_RULEBOOK if (len(p) > 1 and p[1] == RULEBOOK_TOP) \
        else SECTION_INDEX_DEFAULT
    if i >= len(p) - 1:
        return (p[1] if len(p) > 1 else p[0]), True
    return p[i], False


def _header(ws) -> list:
    return [_flat(c.value) for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _sheet_shape(path: Path) -> dict:
    """Row count per sheet, for the after-the-fact comparison."""
    wb = openpyxl.load_workbook(path, read_only=True)
    try:
        return {n: sum(1 for r in wb[n].iter_rows(values_only=True)
                       if any(v is not None for v in r))
                for n in wb.sheetnames}
    finally:
        wb.close()


def _guarded_rows(path: Path) -> list:
    wb = openpyxl.load_workbook(path, read_only=True)
    try:
        ws = wb["regulations"]
        it = ws.iter_rows(values_only=True)
        hdr = [_flat(h) for h in next(it)]
        idx = [hdr.index(g) for g in GUARDED if g in hdr]
        return [tuple(_flat(r[i]) for i in idx) for r in it
                if any(v is not None for v in r)]
    finally:
        wb.close()


def plan(path: Path) -> dict:
    """What would change in this workbook. Opens it read-only."""
    wb = openpyxl.load_workbook(path, read_only=True)
    try:
        ws = wb["regulations"]
        it = ws.iter_rows(values_only=True)
        hdr = [_flat(h) for h in next(it)]
        if "category" not in hdr or "doc_path" not in hdr:
            return {"skip": "no category/doc_path column"}
        ci, di = hdr.index("category"), hdr.index("doc_path")
        rows = changed = fallback = 0
        before, after = Counter(), Counter()
        for r in it:
            if not any(v is not None for v in r):
                continue
            rows += 1
            old = _flat(r[ci])
            new, fb = section_for(r[di])
            fallback += fb
            before[old] += 1
            after[new] += 1
            if new != old:
                changed += 1
        return {"rows": rows, "changed": changed, "fallback": fallback,
                "before": before, "after": after}
    finally:
        wb.close()


def rewrite(path: Path) -> dict:
    """Write the corrected workbook beside the original, verify, then swap."""
    tmp = path.with_suffix(".recat.xlsx")
    keep = path.with_suffix(".pre-recat.xlsx")

    shape_before = _sheet_shape(path)
    guarded_before = _guarded_rows(path)

    wb = openpyxl.load_workbook(path)          # NOT read_only: we are writing
    try:
        ws = wb["regulations"]
        hdr = _header(ws)
        ci, di = hdr.index("category") + 1, hdr.index("doc_path") + 1
        changed = 0
        for row in range(2, ws.max_row + 1):
            dp = ws.cell(row=row, column=di).value
            if dp is None:
                continue
            new, _ = section_for(dp)
            cell = ws.cell(row=row, column=ci)
            if _flat(cell.value) != new:
                cell.value = new
                changed += 1
        wb.save(tmp)
    finally:
        wb.close()

    # ---- verify the copy before it replaces anything -----------------------
    shape_after = _sheet_shape(tmp)
    if shape_after != shape_before:
        tmp.unlink(missing_ok=True)
        raise SystemExit(
            f"{path.name}: REFUSED -- sheet row counts changed in the rewrite.\n"
            f"  before {shape_before}\n  after  {shape_after}\n"
            f"The original is untouched.")

    guarded_after = _guarded_rows(tmp)
    if guarded_after != guarded_before:
        bad = next((i for i, (a, b) in enumerate(zip(guarded_before,
                                                     guarded_after), 1)
                    if a != b), None)
        tmp.unlink(missing_ok=True)
        raise SystemExit(
            f"{path.name}: REFUSED -- an identity column moved "
            f"(first at row {bad}). The original is untouched.")

    shutil.copy2(path, keep)
    tmp.replace(path)
    return {"changed": changed, "backup": keep.name}


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write the change; without it this is a dry run")
    ap.add_argument("--dir", default=str(DEFAULT_DIR),
                    help=f"directory of workbooks (default {DEFAULT_DIR})")
    ap.add_argument("--glob", default="*.xlsx",
                    help="which workbooks in that directory (default *.xlsx)")
    a = ap.parse_args()

    d = Path(a.dir)
    if not d.is_dir():
        raise SystemExit(f"no such directory: {d}")
    books = sorted(p for p in d.glob(a.glob)
                   if not p.name.endswith((".recat.xlsx", ".pre-recat.xlsx",
                                           ".partial.xlsx")))
    if not books:
        raise SystemExit(f"no workbooks matched {a.glob} in {d}")

    total_rows = total_changed = 0
    for p in books:
        r = plan(p)
        if r.get("skip"):
            print(f"{p.name}: skipped -- {r['skip']}")
            continue
        total_rows += r["rows"]
        total_changed += r["changed"]
        print(f"\n{p.name}")
        print(f"  rows {r['rows']}  changed {r['changed']}  "
              f"fallback {r['fallback']}")
        print(f"  category: {len(r['before'])} distinct -> "
              f"{len(r['after'])} distinct")
        for k, v in r["after"].most_common(8):
            print(f"      {v:6d}  {k[:64]}")
        if len(r["after"]) > 8:
            print(f"      ... {len(r['after']) - 8} more")
        if a.apply:
            w = rewrite(p)
            print(f"  WROTE {w['changed']} cells; original kept as {w['backup']}")

    print(f"\n{len(books)} workbook(s), {total_rows} rows, "
          f"{total_changed} would change" if not a.apply else
          f"\n{len(books)} workbook(s), {total_rows} rows, "
          f"{total_changed} changed")

    if not a.apply:
        print("\nDry run. Re-run with --apply to write.")
    else:
        print("\nNext: python -m tools.workbook check <each workbook> "
              "-- it must still say OK.")

    print("\nREMINDER: this fixes the FILES, not the crawler. Until "
          "crawler/cbb_crawler.py:275\n"
          "  category = doc.doc_path[1] ...\n"
          "uses the section index instead, the next `tools.workbook export` "
          "writes the\nflat value again and these workbooks revert.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
