"""CHG-026: give each law stored once per article its file once, for one regulator.

    python -m scripts.chg026_split_laws --regulator "Saudi Arabian Monetary Authority (SAMA)"
    python -m scripts.chg026_split_laws --regulator "Central Bank of Egypt (CBE)" --apply

WHAT IS WRONG
-------------
A regulator that publishes a law as one PDF plus a page per article shows the
PDF on every article page, so the crawl stored one row per article each with
the whole-law PDF in `document_url` (MEASURED 2026-11-17 on the 2026-09-22
copy: 3,285 SAMA article rows under 595 law rows; a trace of the same in CBE,
CMA and CBB), and the law's own row with no file at all. The pipeline fetched
and extracted a law's PDF once per article.

THE RULE, the same one utils.file_links.claim_shared_files applies at the
orchestrator to every crawl from now on, keyed the same way on doc_path: a
row whose path is above other rows' paths is a hub; hubs are taken deepest
first; a hub claims the PDF that two or more rows beneath it carry. The hub row takes the PDF as `document_url`; the carriers
take their own page (`source_page_url`, the article's node url, what the
crawler emits for them) as `document_url`. A row that carries the same file
elsewhere in the tree (a circular that is also split into articles under a
law) is left as it is: one document in two places is allowed; one law stored
once per article is not.

    law row       document_url     page  ->  pdf          found_on <- page
    article row   document_url     pdf   ->  its page     source_page_url <- found_on
                  extra_meta.found_on       removed

THIS CHANGES IDENTITY (`document_url`, `doc_path`), as the 2026-08-24 shuffle
(scripts/sama_pdf_as_document_url.py) did, so the rows and the orchestrator
rule move together: this script and the rule are one change. A sector's
landing row from the older SAMA crawl never claims (the current crawler
stores none); a hub that already carries a different file claims nothing,
and is counted. Every original value is written to output/ before anything is
updated, and the update is verified by re-reading the rows.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

#: regulator, source system, sector, sector landing page: the older SAMA crawl's root rows
SECTOR_DEPTH = 4


from utils.file_links import _is_file as is_file  # noqa: E402  the orchestrator's own file test


def is_pdf(url) -> bool:
    """Kept under its old name for the callers; any file the orchestrator
    treats as one (pdf, office documents, download endpoints), not pdf only:
    a hub carrying a spreadsheet already carries a file."""
    return bool(is_file(str(url or "")))


def is_sector_hub(row: dict) -> bool:
    """A sector's own landing row from the older combined SAMA crawl: doc_path
    ends with the sector name twice. The current crawler stores no such row
    (it starts at the sector page's topic boxes), so nothing it emits could
    ever carry what such a row claimed; it is left out of the hubs."""
    path = row.get("doc_path") or []
    return len(path) <= SECTOR_DEPTH and len(path) >= 2 and path[-1] == path[-2]


def plan(rows: List[dict]) -> dict:
    """What to change. Pure, so it is testable without a database.

    The same rule as the orchestrator (utils.file_links.claim_shared_files),
    keyed the same way, on `doc_path`: a row whose path is a strict prefix
    of other rows' paths is a hub; hubs are taken deepest first; a hub claims
    the PDF that two or more rows below it still carry, and those rows take
    their own page. Two rows at one path are ambiguous and claim nothing; a
    sector's landing row from the older crawl never claims (the crawler
    stores none); a PDF carried by one row under a hub stays with that row.
    Rows elsewhere in the tree that carry the same file (a circular that is
    also split into articles under a law) are not that hub's business and
    are left as they are. A carrier's page must be its own under the hub:
    two rows with one page are two files listed on that page, not two
    articles, and keep their files. The category tree is not consulted: it and
    doc_path can disagree (the leaf rule files a same-named document on a
    sibling node), and the orchestrator sees only paths.

    `rows`: dicts with id, title, document_url, source_page_url, extra_meta
    (dict), doc_path (list). Returns {"owners": {row_id: pdf}, "carriers":
    [(row_id, document_url, source_page_url, extra_meta)], "skipped":
    Counter of hubs by reason, "left": Counter of carrier rows by reason,
    "groups": [...]}.
    """
    by_id = {r["id"]: r for r in rows}
    at_path: Dict[tuple, List[int]] = defaultdict(list)
    below: Dict[tuple, List[int]] = defaultdict(list)
    for r in rows:
        path = tuple(r.get("doc_path") or [])
        if not path:
            continue
        at_path[path].append(r["id"])
        for k in range(1, len(path)):
            below[path[:k]].append(r["id"])
    depth_of = {r["id"]: len(r.get("doc_path") or []) for r in rows}

    original = {r["id"]: r["document_url"] for r in rows}
    current = dict(original)                       # the file each row carries now
    owners: Dict[int, str] = {}
    carriers: list = []
    skipped: Counter = Counter()                   # hubs that claimed nothing, by reason
    left: Counter = Counter()                      # carrier rows they would have moved
    claimed_by: Dict[int, int] = {}                # carrier row -> the hub that took its file
    unmovable: set = set()                         # carriers with no page of their own
    hubs = sorted((path for path in at_path if below.get(path)
                   and not all(is_sector_hub(by_id[i]) for i in at_path[path])),
                  key=len, reverse=True)
    for path in hubs:
        # a carrier's page must be its own under the hub (see claim_shared_files);
        # a hub below that already claimed is a carrier whatever its listing page
        pages = Counter(str(by_id[i]["source_page_url"] or "").strip() for i in below[path])
        own_page = {i for i in below[path]
                    if pages[str(by_id[i]["source_page_url"] or "").strip()] == 1 or i in owners}
        carried = Counter(current[i] for i in below[path] if i in own_page and is_pdf(current[i]))
        pdf, count = next(iter(carried.most_common(1)), (None, 0))
        if count < 2:
            continue
        ids = [i for i in below[path] if i in own_page and current[i] == pdf]
        if len(at_path[path]) > 1:
            skipped["two rows at one path"] += 1
            left["two rows at one path"] += len(ids)
            continue
        owner = at_path[path][0]
        orow = by_id[owner]
        if is_pdf(orow["document_url"]) and orow["document_url"] != pdf:
            skipped["hub already carries another file"] += 1
            left["hub already carries another file"] += len(ids)
            continue
        moved = []
        for i in ids:
            if i in owners:                        # a hub below claimed first; the law above takes over
                del owners[i]
                current[i] = original[i]
                for c, h in claimed_by.items():
                    if h == i:
                        claimed_by[c] = owner
                moved.append(i)
                continue
            r = by_id[i]
            page = str(r["source_page_url"] or "").strip()
            if not page.lower().startswith("http") or is_pdf(page):
                unmovable.add(i)                   # stays a carrier; the hub above sees it again
                continue
            meta = dict(r["extra_meta"] or {})
            listing = str(meta.pop("found_on", "") or "").strip()
            carriers.append((i, page, listing or page, meta))
            current[i] = page
            claimed_by[i] = owner
            moved.append(i)
        if not moved:
            skipped["no carrier could move"] += 1
            left["no carrier could move"] += len(ids)
            continue
        owners[owner] = pdf
        current[owner] = pdf
    if unmovable:
        left["carrier has no page url"] = len(unmovable)
    groups = []
    for owner, pdf in owners.items():
        mine = [c for c, h in claimed_by.items() if h == owner]
        groups.append({"owner": owner, "title": by_id[owner]["title"], "pdf": pdf, "carriers": len(mine),
                       "depth": max(depth_of[c] - depth_of[owner] for c in mine)})
    groups.sort(key=lambda g: -g["carriers"])
    return {"owners": owners, "carriers": carriers, "skipped": skipped, "left": left, "groups": groups}


def owner_update(row: dict, pdf: str) -> tuple:
    """(document_url, extra_meta) for a law row taking its PDF."""
    meta = dict(row["extra_meta"] or {})
    old = str(row["document_url"] or "").strip()
    if old and old != pdf:
        meta.setdefault("found_on", old)
    return pdf, meta


# --------------------------------------------------------------------------- #

def _load(cur, regulator: str):
    cur.execute("""SELECT id, title, document_url, source_page_url,
                          CAST(extra_meta AS nvarchar(max)), doc_path
                   FROM regulations WHERE regulator = ? ORDER BY id""", [regulator])
    rows = []
    for rid, title, du, spu, em, dp in cur.fetchall():
        try:
            meta = json.loads(em or "{}")
        except Exception:
            meta = {}
        try:
            path = json.loads(dp or "[]")
        except Exception:
            path = []
        rows.append({"id": int(rid),
                     "title": title or "", "document_url": du or "", "source_page_url": spu or "",
                     "extra_meta": meta if isinstance(meta, dict) else {},
                     "doc_path": path if isinstance(path, list) else []})
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--regulator", required=True,
                    help="the regulator's name exactly as regulations.regulator holds it")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    regulator = a.regulator

    from jobs.monitor_jobs import _repo
    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        rows = _load(cur, regulator)
        if not rows:
            print(f"no rows for regulator {regulator!r}")
            return 1
        p = plan(rows)
        by_id = {r["id"]: r for r in rows}

        print(f"{regulator}")
        print(f"rows                              : {len(rows):,}")
        print(f"law rows taking their pdf         : {len(p['owners']):,}")
        print(f"article rows taking their page    : {len(p['carriers']):,}")
        for k in sorted(set(p["skipped"]) | set(p["left"])):
            print(f"left alone, {k:<34}: {p['skipped'][k]} hub(s), {p['left'][k]} carrier row(s)")
        print("hubs claiming, by depth of their deepest carrier:",
              dict(sorted(Counter(g["depth"] for g in p["groups"]).items())))
        for g in p["groups"][:5]:
            print(f"  e.g. {g['title'][:50]!r}: {g['carriers']} carrier(s), depth {g['depth']}, "
                  f"{g['pdf'].rsplit('/', 1)[-1]}")
        if p["carriers"]:
            i, page, spu, meta = p["carriers"][0]
            old = by_id[i]
            print(f"\nexample article id {i}: {old['title'][:50]!r}")
            print(f"  document_url    {old['document_url'][:70]}")
            print(f"               -> {page[:70]}")
            print(f"  source_page_url {old['source_page_url'][:70]}")
            print(f"               -> {spu[:70]}")

        if not a.apply:
            print("\nDRY RUN -- nothing written. Re-run with --apply.")
            return 0
        if not p["owners"]:
            print("\nnothing to apply")
            return 0

        touched = set(p["owners"]) | {c[0] for c in p["carriers"]}
        stamp = datetime.now().strftime("%Y-%m-%d_%H%M")
        slug = "".join(ch if ch.isalnum() else "_" for ch in regulator)[:40].strip("_")
        backup = REPO_ROOT / "output" / f"backup_chg026_{slug}_{stamp}.json"
        backup.parent.mkdir(parents=True, exist_ok=True)
        backup.write_text(json.dumps(
            [{k: by_id[i][k] for k in ("id", "document_url", "source_page_url", "extra_meta")}
             for i in sorted(touched)], ensure_ascii=False), encoding="utf-8")
        print(f"\nbacked up -> {backup}")

        for i, page, spu, meta in p["carriers"]:
            cur.execute("UPDATE regulations SET document_url = ?, source_page_url = ?, extra_meta = ? "
                        "WHERE id = ?", [page, spu, json.dumps(meta, ensure_ascii=False), i])
        for i, pdf in p["owners"].items():
            du, meta = owner_update(by_id[i], pdf)
            cur.execute("UPDATE regulations SET document_url = ?, extra_meta = ? WHERE id = ?",
                        [du, json.dumps(meta, ensure_ascii=False), i])
        conn.commit()
        print(f"updated {len(p['carriers']):,} article row(s) and {len(p['owners']):,} law row(s)")

        # ---- verify: re-read and re-plan; the rule must have nothing left to do -- #
        rows2 = _load(cur, regulator)
        again = plan(rows2)
        by2 = {r["id"]: r for r in rows2}
        bad_owner = [i for i, pdf in p["owners"].items() if by2[i]["document_url"] != pdf]
        bad_carrier = [c[0] for c in p["carriers"] if is_pdf(by2[c[0]]["document_url"])]
        print()
        if again["owners"] or bad_owner or bad_carrier:
            print(f"VERIFICATION FAILED: {len(again['owners'])} group(s) still to move, "
                  f"{len(bad_owner)} owner(s) and {len(bad_carrier)} carrier(s) not as planned")
            print(f"{backup} holds every original value.")
            return 1
        print("VERIFIED: every planned law row carries its pdf, no planned article row does, "
              "and the rule finds nothing more to move.")
        print("\nNEXT: on the scheduler host the stored-inventory change state for this regulator "
              "keys on identity, which moved; the first sweep after this reconciles, the second is clean.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
