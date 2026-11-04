"""Count what the CHG-001 allowlist would remove from stored `document_html`.

    python -m scripts.html_clean_scan                         # database, count only
    python -m scripts.html_clean_scan --workbook out/x.xlsx   # one or more workbooks
    python -m scripts.html_clean_scan --samples 5 --text      # show examples

THIS SCRIPT NEVER WRITES. It exists to tune `utils/html_clean.py` before the
cleaner is trusted on the modified path: every tag or attribute that would be
dropped is tallied per regulator, so a genuine content tag that a regulator
CMS emits (and the list does not know) shows up as a ranked line rather than
as a silently plainer page.

Three answers, per regulator and in total:

    dropped       `tag` / `tag@attr` and how many times it would go
    text changed  rows whose VISIBLE text differs after cleaning, once
                  entities are decoded -- the one number that should be zero,
                  because the cleaner removes only script/style/noscript
                  bodies, which the fingerprint already ignores
    overflow      workbook cells that hold the overflow marker instead of the
                  html (full value lives in the sidecar); counted, not scanned

Old rows are NOT rewritten by this change: the display sandbox (CHG-002)
covers them. `--apply` is deliberately absent.
"""
from __future__ import annotations

import argparse
import collections
import html as htmlmod
import re
import sys
from pathlib import Path
from typing import Iterable, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from crawler.fingerprint import text_of_html                      # noqa: E402
from dynamic_crawler.formfill.excel_repo import OVERFLOW_PREFIX   # noqa: E402
from utils.html_clean import clean_html, dropped_in               # noqa: E402

Row = Tuple[str, str, str]   # (row id, regulator, html)


def rows_from_db() -> Iterable[Row]:
    from jobs.monitor_jobs import _repo
    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, regulator, document_html FROM regulations "
                    "WHERE document_html IS NOT NULL AND LEN(document_html) > 0")
        for rid, reg, h in cur.fetchall():
            yield str(rid), str(reg or ""), str(h)


def rows_from_workbook(path: Path) -> Iterable[Row]:
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True)
    if "regulations" not in wb.sheetnames:
        print(f"  {path}: no 'regulations' sheet, skipped", file=sys.stderr)
        return
    ws = wb["regulations"]
    it = ws.iter_rows(values_only=True)
    header = [str(h or "") for h in next(it)]
    try:
        i_id, i_reg, i_html = (header.index("id"), header.index("regulator"),
                               header.index("document_html"))
    except ValueError:
        print(f"  {path}: regulations sheet lacks id/regulator/document_html",
              file=sys.stderr)
        return
    for r in it:
        h = r[i_html]
        if h:
            yield f"{path.name}:{r[i_id]}", str(r[i_reg] or ""), str(h)


_COMMENT = re.compile(r"<!--.*?-->", re.S)


def visible(h: str) -> str:
    """Visible characters, for an equality test that only fires on LOST TEXT.

    Three things are deliberately normalised away because a reader cannot see
    them and the cleaner changes all three:
      comments     the fingerprint's tag regex stops at the first `>` inside a
                   comment, so comment debris ("-->") leaks into ITS text;
                   nh3 removes the whole comment
      entities     nh3 writes U+00A0 back as `&nbsp;` and `&` as `&amp;`
      spacing      the fingerprint replaces every tag with a space, nh3 removes
                   an unknown tag with none, so `<glossary>x</glossary>,`
                   reads `x ,` before and `x,` after
    """
    text = htmlmod.unescape(text_of_html(_COMMENT.sub(" ", h)))
    return "".join(text.split())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workbook", nargs="+", type=Path,
                    help="scan these workbooks instead of the database")
    ap.add_argument("--samples", type=int, default=3,
                    help="rows to show per dropped tag (default 3)")
    ap.add_argument("--text", action="store_true",
                    help="print the visible-text diff for rows whose text changes")
    ap.add_argument("--top", type=int, default=25, help="lines per table")
    a = ap.parse_args()

    if a.workbook:
        rows = (r for p in a.workbook for r in rows_from_workbook(p))
    else:
        rows = rows_from_db()

    n_rows = n_dirty = n_overflow = 0
    dropped_total: collections.Counter = collections.Counter()
    dropped_by_reg = collections.defaultdict(collections.Counter)
    rows_by_reg: collections.Counter = collections.Counter()
    dirty_by_reg: collections.Counter = collections.Counter()
    samples = collections.defaultdict(list)
    text_changed = []

    for rid, reg, h in rows:
        n_rows += 1
        rows_by_reg[reg] += 1
        if OVERFLOW_PREFIX in h:
            n_overflow += 1
            continue
        lost = dropped_in(h)
        if lost:
            n_dirty += 1
            dirty_by_reg[reg] += 1
            dropped_total.update(lost)
            dropped_by_reg[reg].update(lost)
            for key in lost:
                if len(samples[key]) < a.samples:
                    samples[key].append(rid)
        before, after = visible(h), visible(clean_html(h) or "")
        if before != after:
            text_changed.append((rid, reg, before, after))

    print(f"rows carrying html            : {n_rows:,}")
    print(f"rows the cleaner would change : {n_dirty:,}")
    print(f"rows whose VISIBLE TEXT changes: {len(text_changed):,}   <- should be 0")
    if n_overflow:
        print(f"workbook overflow cells skipped: {n_overflow:,}")

    print("\nper regulator            rows   changed")
    for reg, n in rows_by_reg.most_common():
        print(f"  {reg[:36]:36} {n:>6} {dirty_by_reg[reg]:>8}")

    print(f"\ndropped, all regulators (top {a.top})   count   sample rows")
    for key, n in dropped_total.most_common(a.top):
        print(f"  {key:32} {n:>8}   {', '.join(samples[key])}")

    for reg, c in sorted(dropped_by_reg.items(), key=lambda kv: -sum(kv[1].values())):
        print(f"\n  {reg}")
        for key, n in c.most_common(a.top):
            print(f"      {key:32} {n:>8}")

    if text_changed:
        print(f"\nrows whose visible text changes ({len(text_changed)}):")
        for rid, reg, before, after in text_changed[:a.top]:
            print(f"  {rid}  {reg}  {len(before)} -> {len(after)} chars")
            if a.text:
                bw, aw = before.split(), after.split()
                for i, (x, y) in enumerate(zip(bw, aw)):
                    if x != y:
                        print(f"      first diff at word {i}: {x!r} -> {y!r}")
                        break
                else:
                    print(f"      word count {len(bw)} -> {len(aw)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
