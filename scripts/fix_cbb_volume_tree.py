"""Fix ONE CBB rulebook volume's folder tree in the database, in place.

    python -m scripts.fix_cbb_volume_tree --volume 1                                 # dry run
    python -m scripts.fix_cbb_volume_tree --volume 1 --workbook path/cbb_vol1.xlsx   # + cross-check
    python -m scripts.fix_cbb_volume_tree --volume 1 --apply

Run from PowerShell, not Git Bash (see the note on slash args in the memory).

WHY THIS EXISTS
---------------
Volumes 1-6 were promoted while the folder walk still had the subtree fallback
(docs/FOLDER_TREE.md), so each volume's Quarterly Updates letters sit under
"... > UG-3.1 Rulebook Maintenance > Quarterly Updates" instead of
"Volume N > Quarterly Updates". `doc_path` on every row was always right; only the
tree is wrong.

WHY IN PLACE, NOT DELETE-AND-REPROMOTE
--------------------------------------
Deleting a volume and promoting the corrected workbook would give every
regulation a new id, throwing away its regulation_versions history, any analysis
tied to the id, and every link to it -- over 8,645 rows for Volume 1 alone, to
move about 63. The corrected workbooks differ from what was promoted ONLY in the
tree (`reparent_cbb_quarterly_updates.py` asserts the regulations sheet is
byte-identical), so moving nodes gets exactly the same end state.

WHAT IT DOES, per volume
------------------------
1. Reads that volume's regulations (by source_system) and the folder tree.
2. For each row, compares the chain above its leaf node with
   [country] + doc_path. Rows that already match are left alone.
3. For a row that does not match, walks doc_path with the EXACT-PATH rule
   (reuse a folder only on the same title under the same parent; create it
   otherwise) to find where the leaf belongs, and plans to re-parent the leaf.
4. With --apply: inserts the missing folders, updates the leaves' parentid, then
   re-reads every moved row and checks its chain now equals doc_path -- all in
   ONE transaction, rolled back if any check fails.

It never touches `regulations`, `regulation_versions` or anything analytical.
The only writes are INSERTs into compliancecategory and UPDATEs of
compliancecategory.parentid on leaf nodes. Before applying, every move is saved
to output/backup_cbb_tree_<volume>_<timestamp>.json (leaf id, old parent, new
parent), which is enough to undo it.

--only-section limits the fix to rows under one section (doc_path[3], e.g.
"Quarterly Updates"). USE IT FOR VOLUME 5: its 320 "Specific Modules (By Type of
Licensee)" rows also disagree with doc_path, and which of the two is right there
has not been checked against the site.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.countries import country_for  # noqa: E402

REGULATOR = "Central Bank of Bahrain"
SECTION_INDEX = 3      # [regulator, "CBB Rulebook", volume, SECTION, ...]


def source_system_for(volume: str) -> str:
    v = str(volume).strip().lower()
    if v in ("0", "common", "common volume"):
        return "CBB-Rulebook-Common"
    return f"CBB-Rulebook-Vol-{int(v)}"


def parse_path(v) -> list:
    """doc_path as stored: JSON in the DB, ' | ' text in a workbook."""
    if isinstance(v, (list, tuple)):
        return [str(s).strip() for s in v]
    s = str(v or "").strip()
    if s.startswith("["):
        return [str(x).strip() for x in json.loads(s)]
    return [x.strip() for x in s.split(" | ")] if s else []


def _int(v):
    return None if v in (None, "") else int(float(v))


class Tree:
    """compliancecategory in memory, with planned inserts kept separate."""

    def __init__(self, rows):
        self.node = {}                       # id -> [title, parent]
        self.kids = defaultdict(list)        # (parent, title) -> [ids]
        for cid, title, parent in rows:
            cid, parent = _int(cid), _int(parent)
            self.node[cid] = [title, parent]
            self.kids[(parent, title)].append(cid)
        for k in self.kids:
            self.kids[k].sort()              # lowest id wins, deterministically
        self.planned = {}                    # negative id -> (title, parent)

    def chain(self, cid) -> list:
        out, seen = [], set()
        while cid is not None and cid in self.node and cid not in seen:
            seen.add(cid)
            out.append(self.node[cid][0])
            cid = self.node[cid][1]
        return out[::-1]

    def resolve(self, titles: list) -> int:
        """Exact-path walk; plans (does not write) any missing folder."""
        parent = None
        for t in titles:
            hit = self.kids.get((parent, t))
            if hit:
                parent = hit[0]
                continue
            pid = -(len(self.planned) + 1)
            self.planned[pid] = (t, parent)
            self.node[pid] = [t, parent]
            self.kids[(parent, t)] = [pid]
            parent = pid
        return parent

    def path_of(self, cid) -> str:
        return " > ".join(self.chain(cid))


def plan(regs, tree: Tree, country: str, only_section=None) -> dict:
    """Pure: no I/O. `regs` is [(id, title, doc_path, compliancecategory_id)]."""
    ok, moves, skipped = 0, [], []
    diverge = Counter()
    leaf_target = {}
    for rid, title, raw_path, leaf in regs:
        path = parse_path(raw_path)
        leaf = _int(leaf)
        want = ([country] if country else []) + path
        if not path or leaf is None or leaf not in tree.node:
            skipped.append((rid, "no doc_path or leaf node"))
            continue
        have = tree.chain(leaf)
        if have == want:
            ok += 1
            continue
        if only_section and (len(path) <= SECTION_INDEX
                             or path[SECTION_INDEX] not in only_section):
            skipped.append((rid, "outside --only-section"))
            continue
        if tree.node[leaf][0] != path[-1]:
            skipped.append((rid, f"leaf title {tree.node[leaf][0]!r} != "
                                 f"doc_path[-1] {path[-1]!r}"))
            continue
        k = next((i for i, (a, b) in enumerate(zip(want, have)) if a != b),
                 min(len(want), len(have)))
        diverge[" > ".join(want[max(0, k - 1):k + 1])] += 1
        target = tree.resolve(want[:-1])
        if leaf in leaf_target and leaf_target[leaf] != target:
            skipped.append((rid, f"leaf {leaf} shared by rows wanting "
                                 f"different parents"))
            continue
        leaf_target[leaf] = target
        moves.append({"regulation_id": rid, "leaf_id": leaf,
                      "old_parent": tree.node[leaf][1], "new_parent": target,
                      "doc_path": " > ".join(path)})
    # one move per leaf node
    uniq = {m["leaf_id"]: m for m in moves}
    return {"ok": ok, "moves": list(uniq.values()), "rows_moved": len(moves),
            "skipped": skipped, "diverge": diverge}


def cross_check(workbook: Path, source_system: str, db_regs) -> dict:
    """Is the DB holding the SAME documents as the workbook? (doc_path, title)."""
    import openpyxl
    wb = openpyxl.load_workbook(workbook, read_only=True)
    rows = wb["regulations"].iter_rows(values_only=True)
    h = next(rows)
    wb_keys = set()
    for r in rows:
        d = dict(zip(h, r))
        if d.get("source_system") != source_system:
            continue
        wb_keys.add((tuple(parse_path(d.get("doc_path"))), str(d.get("title") or "")))
    db_keys = {(tuple(parse_path(p)), str(t or "")) for _, t, p, _ in db_regs}
    return {"workbook_rows": len(wb_keys), "db_rows": len(db_keys),
            "only_in_db": sorted(db_keys - wb_keys)[:10],
            "n_only_in_db": len(db_keys - wb_keys),
            "only_in_workbook": sorted(wb_keys - db_keys)[:10],
            "n_only_in_workbook": len(wb_keys - db_keys)}


def _connect():
    from dynamic_crawler.formfill.promote import _build_repo
    conn = _build_repo()._connect()
    conn.autocommit = False
    return conn


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--volume", required=True, help="1-7, or 'common'")
    ap.add_argument("--workbook", help="corrected cbb_volN.xlsx, to cross-check")
    ap.add_argument("--only-section", action="append",
                    help="limit to rows whose doc_path[3] is this (repeatable)")
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    a = ap.parse_args()

    source_system = source_system_for(a.volume)
    country = country_for(REGULATOR)
    conn = _connect()
    cur = conn.cursor()

    cur.execute("SELECT id, title, doc_path, compliancecategory_id "
                "FROM regulations WHERE source_system = ?", source_system)
    regs = [tuple(r) for r in cur.fetchall()]
    print(f"{source_system}: {len(regs)} regulation row(s) in the database")
    if not regs:
        print("nothing to do")
        return 0

    if a.workbook:
        cc = cross_check(Path(a.workbook), source_system, regs)
        print(f"cross-check vs {a.workbook}: workbook {cc['workbook_rows']}, "
              f"db {cc['db_rows']}, only in db {cc['n_only_in_db']}, "
              f"only in workbook {cc['n_only_in_workbook']}")
        for k in ("only_in_db", "only_in_workbook"):
            for p, t in cc[k][:5]:
                print(f"    {k}: {' > '.join(p)[-120:]}")

    cur.execute("SELECT compliancecategory_id, title, parentid FROM compliancecategory")
    tree = Tree(cur.fetchall())
    p = plan(regs, tree, country, set(a.only_section) if a.only_section else None)

    print(f"already correct: {p['ok']}   to move: {p['rows_moved']} row(s), "
          f"{len(p['moves'])} leaf node(s)   skipped: {len(p['skipped'])}")
    for seg, n in p["diverge"].most_common(10):
        print(f"    {n:5d}  diverge at: {seg[-110:]}")
    new = sorted(tree.planned, reverse=True)
    print(f"new folders: {len(new)}")
    for pid in new[:15]:
        print(f"    new folder: {tree.path_of(pid)[-140:]}")
    for m in p["moves"][:5]:
        print(f"    move leaf {m['leaf_id']}: {tree.path_of(m['old_parent'])[-70:]}"
              f"  ->  {tree.path_of(m['new_parent'])[-70:]}")
    reasons = Counter(r for _, r in p["skipped"])
    for r, n in reasons.most_common(5):
        print(f"    skipped {n}: {r}")

    if not a.apply:
        print("\nDRY RUN -- nothing written. Re-run with --apply.")
        return 0
    if not p["moves"]:
        print("nothing to apply")
        return 0

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    backup = REPO_ROOT / "output" / f"backup_cbb_tree_{source_system}_{stamp}.json"
    backup.write_text(json.dumps({"source_system": source_system, "moves": p["moves"]},
                                 indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"backup: {backup}")

    try:
        real = {}
        # parents before children: planned ids were assigned in walk order
        for pid in sorted(tree.planned, reverse=True):
            t, parent = tree.planned[pid]
            parent = real.get(parent, parent)
            cur.execute("INSERT INTO compliancecategory (title, parentid, type) "
                        "OUTPUT INSERTED.compliancecategory_id VALUES (?, ?, 'F')",
                        t, parent)
            real[pid] = int(cur.fetchone()[0])
        for m in p["moves"]:
            cur.execute("UPDATE compliancecategory SET parentid = ? "
                        "WHERE compliancecategory_id = ?",
                        real.get(m["new_parent"], m["new_parent"]), m["leaf_id"])

        # VERIFY inside the transaction: every moved row's chain == doc_path
        cur.execute("SELECT compliancecategory_id, title, parentid FROM compliancecategory")
        after = Tree(cur.fetchall())
        moved_ids = {m["regulation_id"] for m in p["moves"]}
        bad = [rid for rid, _, raw, leaf in regs if rid in moved_ids
               and after.chain(_int(leaf)) != [country] + parse_path(raw)]
        if bad:
            raise RuntimeError(f"{len(bad)} moved row(s) still disagree with "
                               f"doc_path, e.g. {bad[:5]}")
        conn.commit()
    except Exception:
        conn.rollback()
        print("ROLLED BACK -- nothing was changed")
        raise
    print(f"committed: {len(real)} folder(s) created, {len(p['moves'])} leaf "
          f"node(s) re-parented, all verified against doc_path")
    return 0


if __name__ == "__main__":
    sys.exit(main())
