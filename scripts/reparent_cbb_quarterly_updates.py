"""Put CBB's Quarterly Updates letters where the site puts them: under the volume.

    python -m scripts.reparent_cbb_quarterly_updates                 # dry run
    python -m scripts.reparent_cbb_quarterly_updates --apply

THE PROBLEM
-----------
Every CBB volume publishes a Quarterly Update Letter, and the rulebook lists them
under the volume itself. The site's own breadcrumb, fetched 2026-09-16:

    Central Bank of Bahrain Volume 1—Conventional Banks > Quarterly Updates
        > January 2024

`doc_path` in the workbook agrees with that, exactly:

    Central Bank of Bahrain | CBB Rulebook
        | Central Bank of Bahrain Volume 1—Conventional Banks
        | Quarterly Updates | January 2024

But the FOLDER TREE does not. Following the row's `compliancecategory_id` up
through the `compliancecategory` sheet gives eleven levels, not five:

    Bahrain > Central Bank of Bahrain > CBB Rulebook > Volume 1 > Part A
        > Introduction > UG Users' Guide > UG-3 Rulebook Maintenance and Access
        > UG-3.1 Rulebook Maintenance > Quarterly Updates > January 2024

A TITLE COLLISION, not a stray node. The site really does have two things called
"Quarterly Updates" in one volume:

    UG Users' Guide > UG-3.1 Rulebook Maintenance > Quarterly Updates
        -> UG-3.1.1 .. UG-3.1.5, five rules DESCRIBING the update process
    Volume 1 > Quarterly Updates
        -> January 2024, October 2023, ... the 63 update LETTERS

`repo.find_folder_in_subtree(title, parent)` matches a title anywhere in a
subtree, so the section under the volume was never created and its letters were
hung off the Users' Guide subsection instead. Volume 5's tree has 1,017 titles
appearing more than once, which is how a collision like this finds a victim.

That is why a complete, correct set of update letters could not be found: they
sit five levels inside the Users' Guide. It is also a structure error in the
sense CRAWLING_OVERVIEW.md §1 means -- the library disagrees with the site about
where a section lives -- and `tools.workbook check` cannot see it, because it
verifies that `compliancecategory_id` RESOLVES, never that it resolves to the
place `doc_path` names.

MEASURED 2026-09-16 across output/workbooks/cbb:

    cbb_vol0.xlsx    1 letter   parent is the volume        ALREADY CORRECT
    cbb_vol1.xlsx   63 letters  parent is UG-3.1's node     wrong
    cbb_vol2.xlsx   63          UG-3.1's node               wrong
    cbb_vol3.xlsx   63          UG-3.1's node               wrong
    cbb_vol4.xlsx   52          UG-3.1's node               wrong
    cbb_vol5.xlsx   45          UG-3.1's node               wrong
    cbb_vol6.xlsx   33          UG-3.1's node               wrong
    cbb_vol7.xlsx   14          parent is the volume        ALREADY CORRECT

Volumes 0 and 7 prove the shape is reachable; six volumes lost the race with the
Users' Guide. (Volume 0's section carries the site's own typo, "Quartely
Updates". That is the site's spelling and is left alone.)

THE REPAIR
----------
`doc_path` is the authority: it is an identity field, it matches the site, and
the leaf node's title already equals `doc_path[-1]` on all 319 rows. So per
affected volume:

    1. add ONE folder node -- title from doc_path[3], parent = the volume node
    2. re-parent that volume's letter nodes onto it

and nothing else. In particular:

    * the `regulations` sheet is not touched AT ALL -- this script asserts it is
      byte-identical afterwards. No doc_path, no category, no identity, no
      `compliancecategory_id`: the letters keep their own nodes, those nodes
      simply move to the right parent.
    * the UG-3.1 node STAYS, with UG-3.1.1 .. UG-3.1.5 still under it. It is a
      real part of the Users' Guide and is not what went wrong.

WHAT IS DELIBERATELY NOT FIXED
------------------------------
`doc_path` disagrees with the tree on other rows too -- 320 in Volume 5 under
"Specific Modules (By Type of Licensee)" and 68 in cbb.xlsx under "CBB
Disclosure Standards". Those are a DIFFERENT shape: the tree is deeper than
`doc_path` along the same spine (it carries extra "Part A" / "Introduction"
levels), rather than hanging off a wrong branch. Which of the two is right there
has NOT been checked against the site, so this script leaves them alone rather
than guessing. It reports them so they are not forgotten.

    !! THE CRAWLER IS STILL UNFIXED. !!

This moves nodes in the FILES. The collision lives in the tree walk
(`find_folder_in_subtree` matching a title across a subtree), so the next
`tools.workbook export` reproduces it. Re-run this script after any re-export
until that is fixed.

HOW IT WRITES
-------------
Never over the original: rewritten to `<name>.requarter.xlsx`, that copy
reopened and verified -- the `regulations` sheet identical row for row, the
`compliancecategory` sheet grown by exactly the number of nodes added, and EVERY
affected row's tree chain now equal to its `doc_path` -- then swapped in, with
the original kept as `<name>.pre-requarter.xlsx`. If any check fails the original
is untouched and the run stops.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import openpyxl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_DIR = REPO_ROOT / "output" / "workbooks" / "cbb"

#: doc_path[3] on a letter row, lowercased. Volume 0 carries the site's typo.
SECTION_TITLES = {"quarterly updates", "quartely updates"}

CAT_SHEET = "compliancecategory"
REG_SHEET = "regulations"


def _flat(v) -> str:
    return " ".join(str("" if v is None else v).split())


def _parts(v) -> list:
    return [p.strip() for p in _flat(v).split("|") if p.strip()]


def _read(path: Path) -> dict:
    """Nodes, rows, and the header layouts, read-only."""
    wb = openpyxl.load_workbook(path, read_only=True)
    try:
        it = wb[CAT_SHEET].iter_rows(values_only=True)
        chdr = [_flat(h) for h in next(it)]
        nodes = {}
        for r in it:
            if not any(v is not None for v in r):
                continue
            d = dict(zip(chdr, r))
            nodes[_flat(d["compliancecategory_id"])] = {
                "title": _flat(d["title"]),
                "parent": _flat(d["parentid"]),
                "type": _flat(d.get("type")),
            }
        it = wb[REG_SHEET].iter_rows(values_only=True)
        rhdr = [_flat(h) for h in next(it)]
        rows = [dict(zip(rhdr, r)) for r in it
                if any(v is not None for v in r)]
        return {"nodes": nodes, "rows": rows, "chdr": chdr, "rhdr": rhdr}
    finally:
        wb.close()


def _chain(nodes: dict, nid: str) -> list:
    out, seen = [], set()
    while nid in nodes and nid not in seen:
        seen.add(nid)
        out.append(nodes[nid]["title"])
        nid = nodes[nid]["parent"]
    return list(reversed(out))


def _aligned(nodes: dict, row: dict) -> bool:
    """Does this row's tree chain say what its doc_path says?

    The tree carries the country above the regulator; doc_path starts at the
    regulator, so the first crumb is dropped when it is not in doc_path.
    """
    dp = _parts(row.get("doc_path"))
    tr = _chain(nodes, _flat(row.get("compliancecategory_id")))
    if tr and dp and tr[0] not in dp:
        tr = tr[1:]
    return tr == dp


def plan(path: Path) -> dict:
    """What this workbook needs. Reads only."""
    data = _read(path)
    nodes, rows = data["nodes"], data["rows"]

    letters = []          # (row, doc_path parts)
    for r in rows:
        dp = _parts(r.get("doc_path"))
        if len(dp) > 4 and dp[3].lower() in SECTION_TITLES:
            letters.append((r, dp))

    # every other row whose tree disagrees -- reported, never touched
    other_mismatch = sum(
        1 for r in rows
        if not _aligned(nodes, r)
        and not (len(_parts(r.get("doc_path"))) > 4
                 and _parts(r.get("doc_path"))[3].lower() in SECTION_TITLES))

    if not letters:
        return {"letters": 0, "moves": [], "new_node": None,
                "other_mismatch": other_mismatch}

    section_title = letters[0][1][3]
    volume_title = letters[0][1][2]

    # The volume node: titled doc_path[2]. Matched by title, then confirmed by
    # having the source folder (doc_path[1]) as its parent.
    vol_ids = [i for i, n in nodes.items()
               if n["title"] == volume_title
               and nodes.get(n["parent"], {}).get("title") == letters[0][1][1]]
    if len(vol_ids) != 1:
        return {"skip": f"expected exactly one {volume_title!r} node under "
                        f"{letters[0][1][1]!r}, found {len(vol_ids)}"}
    vol_id = vol_ids[0]

    # Already a section node in the right place?
    existing = [i for i, n in nodes.items()
                if n["title"] == section_title and n["parent"] == vol_id]

    moves = []
    for r, dp in letters:
        cid = _flat(r.get("compliancecategory_id"))
        node = nodes.get(cid)
        if node is None:
            return {"skip": f"row {r.get('title')!r} points at missing node {cid}"}
        # The leaf must already BE the letter; this script never renames.
        if node["title"] != dp[4]:
            return {"skip": f"node {cid} is {node['title']!r} but doc_path says "
                            f"{dp[4]!r} -- not a pure re-parent"}
        if existing and node["parent"] == existing[0]:
            continue                      # already right
        moves.append(cid)

    return {"letters": len(letters), "moves": moves,
            "new_node": None if existing else (section_title, vol_id),
            "existing": existing[0] if existing else None,
            "section": section_title, "volume": volume_title,
            "other_mismatch": other_mismatch}


def rewrite(path: Path, p: dict) -> dict:
    tmp = path.with_suffix(".requarter.xlsx")
    keep = path.with_suffix(".pre-requarter.xlsx")
    before = _read(path)

    wb = openpyxl.load_workbook(path)
    try:
        ws = wb[CAT_SHEET]
        hdr = [_flat(c.value) for c in next(ws.iter_rows(min_row=1, max_row=1))]
        c_id = hdr.index("compliancecategory_id") + 1
        c_ti = hdr.index("title") + 1
        c_pa = hdr.index("parentid") + 1
        c_ty = hdr.index("type") + 1 if "type" in hdr else None

        target = p.get("existing")
        added = 0
        if p["new_node"]:
            title, vol_id = p["new_node"]
            new_id = str(max(int(i) for i in before["nodes"]
                             if i.isdigit()) + 1)
            row = ws.max_row + 1
            ws.cell(row=row, column=c_id).value = new_id
            ws.cell(row=row, column=c_ti).value = title
            ws.cell(row=row, column=c_pa).value = vol_id
            if c_ty:
                ws.cell(row=row, column=c_ty).value = "F"
            target = new_id
            added = 1

        wanted = set(p["moves"])
        moved = 0
        for r in range(2, ws.max_row + 1):
            if _flat(ws.cell(row=r, column=c_id).value) in wanted:
                ws.cell(row=r, column=c_pa).value = target
                moved += 1
        wb.save(tmp)
    finally:
        wb.close()

    # ---- verify the copy ---------------------------------------------------
    after = _read(tmp)

    if after["rows"] != before["rows"]:
        tmp.unlink(missing_ok=True)
        raise SystemExit(f"{path.name}: REFUSED -- the regulations sheet changed. "
                         f"Original untouched.")
    if len(after["nodes"]) != len(before["nodes"]) + added:
        tmp.unlink(missing_ok=True)
        raise SystemExit(
            f"{path.name}: REFUSED -- node count {len(before['nodes'])} -> "
            f"{len(after['nodes'])}, expected +{added}. Original untouched.")

    bad = [r for r in after["rows"]
           if len(_parts(r.get("doc_path"))) > 4
           and _parts(r.get("doc_path"))[3].lower() in SECTION_TITLES
           and not _aligned(after["nodes"], r)]
    if bad:
        tmp.unlink(missing_ok=True)
        raise SystemExit(
            f"{path.name}: REFUSED -- {len(bad)} letter row(s) still disagree "
            f"with doc_path after the move (e.g. {bad[0].get('title')!r}). "
            f"Original untouched.")

    shutil.copy2(path, keep)
    tmp.replace(path)
    return {"moved": moved, "added": added, "backup": keep.name}


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write the change; without it this is a dry run")
    ap.add_argument("--dir", default=str(DEFAULT_DIR))
    a = ap.parse_args()

    d = Path(a.dir)
    if not d.is_dir():
        raise SystemExit(f"no such directory: {d}")
    books = sorted(p for p in d.glob("*.xlsx")
                   if not p.name.endswith((".recat.xlsx", ".pre-recat.xlsx",
                                           ".requarter.xlsx",
                                           ".pre-requarter.xlsx",
                                           ".partial.xlsx")))
    total_moved = other = 0
    for path in books:
        p = plan(path)
        if p.get("skip"):
            print(f"{path.name}: SKIPPED -- {p['skip']}")
            continue
        other += p["other_mismatch"]
        if not p["letters"]:
            print(f"{path.name}: no Quarterly Updates rows")
            continue
        if not p["moves"] and not p["new_node"]:
            print(f"{path.name}: {p['letters']} letters, already correct")
            continue
        print(f"\n{path.name}")
        print(f"  {p['letters']} letters under {p['section']!r}")
        if p["new_node"]:
            print(f"  add node {p['new_node'][0]!r} under "
                  f"{p['volume'][:48]!r}")
        print(f"  re-parent {len(p['moves'])} letter node(s)")
        total_moved += len(p["moves"])
        if a.apply:
            w = rewrite(path, p)
            print(f"  WROTE +{w['added']} node, {w['moved']} re-parented; "
                  f"original kept as {w['backup']}")

    print(f"\n{total_moved} letter node(s) "
          f"{'re-parented' if a.apply else 'would move'}")
    if other:
        print(f"\nNOT TOUCHED: {other} other row(s) whose doc_path still "
              f"disagrees with the tree\n(Volume 5 'Specific Modules', "
              f"cbb.xlsx 'CBB Disclosure Standards'). Different shape --\n"
              f"the tree is deeper along the same spine, and which is right has "
              f"not been\nchecked against the site.")
    if not a.apply:
        print("\nDry run. Re-run with --apply to write.")
    else:
        print("\nNext: python -m tools.workbook check <each workbook>.")
    print("\nREMINDER: the collision is in the tree walk "
          "(find_folder_in_subtree matches a\ntitle across a subtree), so the "
          "next export reproduces it. Re-run this after\nany re-export until "
          "the crawler is fixed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
