"""CHG-032: make `compliancecategory.type` mean "owns a document" on stored rows.

    python -m scripts.chg032_retype_categories                            # dry run: report only
    python -m scripts.chg032_retype_categories --apply                    # retype, with a backup
    python -m scripts.chg032_retype_categories --delete-orphans           # dry run of the delete
    python -m scripts.chg032_retype_categories --delete-orphans --apply   # delete, with a backup

WHAT IS WRONG TODAY
-------------------
`type` holds one letter, F (folder) or R (regulation), chosen ONCE when the
node is created: by position in the document's path (orchestrator) or copied
from the workbook (promote). Nothing ever revised it, so it says how the node
was born, not what it holds:

    F node that later received a document   stays F   (the folder existed first)
    R node whose document never arrived     stays R   (failed insert, or the
                                                       document moved folders)

Since CHG-032 the repository retypes a node to R the moment a regulation is
filed into it, so new drift of the first kind cannot happen. This script
brings the stored rows in line, once.

WHAT IT DOES
------------
1. F nodes that own at least one regulation: retyped to R. Every original
   value is written to output/backup_chg032_<stamp>.json first, and the
   update is verified by re-reading the rows.
2. R nodes that own no regulation: reported to
   output/chg032_orphans_<stamp>.json and to the console, with their child
   count and the root of their branch. With --delete-orphans, the CHILDLESS
   ones are deleted (a dead leaf: a path segment created for a document
   whose row never arrived, or that later moved folders; measured
   2026-11-20 on the local copy, all 228 were childless and nothing
   referenced them: the only foreign keys to compliancecategory are its own
   parentid and regulations.compliancecategory_id). Backed up first to
   output/backup_chg032_orphans_<stamp>.json and verified by re-reading.
   One WITH children is a folder in all but letter and is never deleted
   here; it is listed for the user. The user chose deletion on 2026-11-20.

Whether a node is a FOLDER is never stored: the API derives it from
`parentid` at read time (`apis.pipeline_api._attach_regulation_counts`).
See docs/chg_032_library_kind_flags.md.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env", override=True)

from jobs.monitor_jobs import _repo  # noqa: E402

RETYPE_SQL = ("UPDATE compliancecategory SET type = 'R' "
              "WHERE compliancecategory_id = ? AND (type IS NULL OR type <> 'R')")


def _counts(cur):
    """Regulations per node and children per node: two unfiltered aggregates,
    the same two the API runs."""
    cur.execute("SELECT compliancecategory_id, COUNT(*) FROM regulations "
                "WHERE compliancecategory_id IS NOT NULL GROUP BY compliancecategory_id")
    regs = {r[0]: r[1] for r in cur.fetchall()}
    cur.execute("SELECT parentid, COUNT(*) FROM compliancecategory "
                "WHERE parentid IS NOT NULL GROUP BY parentid")
    kids = {r[0]: r[1] for r in cur.fetchall()}
    return regs, kids


def _nodes(cur):
    cur.execute("SELECT compliancecategory_id, title, parentid, type FROM compliancecategory")
    return {r[0]: {"id": r[0], "title": r[1], "parentid": r[2], "type": r[3]}
            for r in cur.fetchall()}


def _root_of(nodes, nid):
    seen = set()
    while nodes[nid]["parentid"] is not None and nid not in seen:
        seen.add(nid)
        nid = nodes[nid]["parentid"]
        if nid not in nodes:
            return "(parent missing)"
    return nodes[nid]["title"]


def orphan_ids(nodes, regs, kids):
    """The R nodes that own no regulation, split into (childless, with_children).
    Only the first list is ever deleted."""
    orphans = sorted(i for i, n in nodes.items() if n["type"] == "R" and not regs.get(i))
    return ([i for i in orphans if not kids.get(i)],
            [i for i in orphans if kids.get(i)])


def _regulators_of(cur, ids):
    if not ids:
        return {}
    out = {}
    # small lists only (the F-with-document set), so an IN list is safe here
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        cur.execute("SELECT compliancecategory_id, MIN(regulator), COUNT(*) FROM regulations "
                    f"WHERE compliancecategory_id IN ({','.join('?' * len(chunk))}) "
                    "GROUP BY compliancecategory_id", chunk)
        for cid, reg, n in cur.fetchall():
            out[cid] = (reg, n)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="write: retype the F-with-document nodes (and, with --delete-orphans, delete)")
    ap.add_argument("--delete-orphans", action="store_true",
                    help="delete the childless R nodes that own no regulation (with --apply; a dry run otherwise)")
    a = ap.parse_args()
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    out_dir = REPO_ROOT / "output"
    out_dir.mkdir(exist_ok=True)

    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT @@SERVERNAME, DB_NAME()")
        server, db = cur.fetchone()
        print(f"database: {db} on {server}")

        nodes = _nodes(cur)
        regs, kids = _counts(cur)

        by_type = {}
        for n in nodes.values():
            by_type[n["type"]] = by_type.get(n["type"], 0) + 1
        both = sum(1 for i in nodes if regs.get(i) and kids.get(i))
        print(f"nodes: {len(nodes)} by type {by_type}; "
              f"own a document AND have children (kind=both): {both}; "
              f"own more than one document: {sum(1 for v in regs.values() if v > 1)}")

        to_retype = sorted(i for i, n in nodes.items() if n["type"] != "R" and regs.get(i))
        childless, with_kids = orphan_ids(nodes, regs, kids)
        orphans = sorted(childless + with_kids)
        who = _regulators_of(cur, to_retype)

        print(f"\n1. nodes typed {sorted(set(nodes[i]['type'] for i in to_retype)) or '-'} "
              f"that own a document -> R: {len(to_retype)}")
        for i in to_retype:
            n = nodes[i]
            reg, cnt = who.get(i, ("?", regs.get(i)))
            print(f"   {i:>6}  {n['type']!s:>4}  docs={cnt:<3} kids={kids.get(i, 0):<3} "
                  f"[{reg}] {n['title'][:60]!r}")

        print(f"\n2. R nodes with no document: {len(orphans)} "
              f"= {len(childless)} childless ({'to delete' if a.delete_orphans else 'reported'}) "
              f"+ {len(with_kids)} with children (never deleted here)")
        roots = {}
        for i in orphans:
            r = _root_of(nodes, i)
            roots[r] = roots.get(r, 0) + 1
        for r, c in sorted(roots.items(), key=lambda x: -x[1]):
            print(f"   {c:>5}  under {r!r}")
        report = [{"id": i, "title": nodes[i]["title"], "parentid": nodes[i]["parentid"],
                   "child_count": kids.get(i, 0), "root": _root_of(nodes, i)} for i in orphans]
        orphan_file = out_dir / f"chg032_orphans_{stamp}.json"
        orphan_file.write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")
        print(f"   written -> {orphan_file}")

        if not a.apply:
            print("\nDRY RUN -- nothing written to the database. Re-run with --apply "
                  "to retype the nodes in 1" + (" and delete the childless nodes in 2." if a.delete_orphans else "."))
            return 0

        rc = 0
        if a.delete_orphans:
            rc = _delete_orphans(cur, conn, nodes, childless, out_dir, stamp)
        if not to_retype:
            print("\nnothing to retype.")
            return rc

        backup = out_dir / f"backup_chg032_{stamp}.json"
        backup.write_text(json.dumps(
            [{"id": i, "title": nodes[i]["title"], "parentid": nodes[i]["parentid"],
              "type": nodes[i]["type"]} for i in to_retype],
            indent=1, ensure_ascii=False), encoding="utf-8")
        print(f"\nbacked up -> {backup}")

        changed = 0
        for i in to_retype:
            cur.execute(RETYPE_SQL, [i])
            changed += cur.rowcount if cur.rowcount is not None else 0
        conn.commit()

        cur.execute("SELECT compliancecategory_id, type FROM compliancecategory "
                    f"WHERE compliancecategory_id IN ({','.join('?' * len(to_retype))})", to_retype)
        wrong = [cid for cid, t in cur.fetchall() if t != "R"]
        print(f"retyped {changed} of {len(to_retype)}; verified: "
              f"{'all R' if not wrong else f'{len(wrong)} NOT R: {wrong[:20]}'}")
        print(f"{backup} holds every original value.")
        return rc or (0 if not wrong else 1)


def _delete_orphans(cur, conn, nodes, childless, out_dir, stamp) -> int:
    if not childless:
        print("\nno childless orphan nodes to delete.")
        return 0
    backup = out_dir / f"backup_chg032_orphans_{stamp}.json"
    backup.write_text(json.dumps(
        [{"id": i, "title": nodes[i]["title"], "parentid": nodes[i]["parentid"],
          "type": nodes[i]["type"]} for i in childless],
        indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"\nbacked up -> {backup}")
    deleted = 0
    for i in range(0, len(childless), 500):
        chunk = childless[i:i + 500]
        # the same three conditions as the selection, re-checked at delete time
        cur.execute(
            "DELETE c FROM compliancecategory c "
            f"WHERE c.compliancecategory_id IN ({','.join('?' * len(chunk))}) AND c.type = 'R' "
            "AND NOT EXISTS (SELECT 1 FROM regulations r WHERE r.compliancecategory_id = c.compliancecategory_id) "
            "AND NOT EXISTS (SELECT 1 FROM compliancecategory k WHERE k.parentid = c.compliancecategory_id)",
            chunk)
        deleted += cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    left = 0
    for i in range(0, len(childless), 500):
        chunk = childless[i:i + 500]
        cur.execute("SELECT COUNT(*) FROM compliancecategory "
                    f"WHERE compliancecategory_id IN ({','.join('?' * len(chunk))})", chunk)
        left += cur.fetchone()[0]
    print(f"deleted {deleted} of {len(childless)}; verified: "
          f"{'none remain' if left == 0 else f'{left} STILL PRESENT'}")
    print(f"{backup} holds every deleted row.")
    return 0 if left == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
