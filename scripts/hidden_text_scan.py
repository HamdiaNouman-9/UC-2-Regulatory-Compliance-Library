"""Count what CHG-006 would remove from the model's copy. Never writes.

    python -m scripts.hidden_text_scan                      # stored pages (database)
    python -m scripts.hidden_text_scan --pdf a.pdf b.pdf    # files on disk
    python -m scripts.hidden_text_scan --pdf-dir folder     # every .pdf under a folder
    python -m scripts.hidden_text_scan --top 30 --samples 5

This exists so the hidden-text rules are tuned BEFORE they are wired in: every
reason that fires is ranked per regulator with samples, so a rule that
catches genuine content (a white heading on a dark bar, a collapsed section)
shows up as a line to read rather than as a silently shorter document.

Reported for pages:
    rows, rows with anything removed, characters removed, rows over the
    quarantine threshold (CHG-008: more than ten percent), per regulator;
    reasons ranked with sample rows and snippets; the rows with the highest
    share removed; and how many rows carry a crawler `content_text`, since
    decide() prefers it over document_html and a hidden instruction may
    already be inside it.
Reported for PDFs:
    per file: pages, characters kept and removed, reasons, snippets.
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
import time
from pathlib import Path
from typing import Iterable, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from processor.hidden_text import (pdf_visible_pool, visible_html_text,    # noqa: E402
                                   visible_pdf_page_text)

QUARANTINE_RATIO = 0.10


def rows_from_db() -> Iterable[Tuple[str, str, str, str]]:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
    from jobs.monitor_jobs import _repo
    with _repo()._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, regulator, document_html, extra_meta FROM regulations "
                    "WHERE document_html IS NOT NULL AND LEN(document_html) > 0")
        for rid, reg, h, meta in cur.fetchall():
            yield str(rid), str(reg or ""), str(h), str(meta or "")


def scan_pages(a) -> None:
    t0 = time.time()
    n = n_hit = chars_removed = chars_kept = n_quar = 0
    with_content_text = 0
    by_reg = collections.defaultdict(lambda: [0, 0, 0, 0])      # rows, hit, removed, quarantine
    reasons = collections.Counter()
    reason_rows = collections.defaultdict(list)                 # reason -> [(rid, reg, snippet)]
    worst = []                                                  # (ratio, rid, reg, removed, total, samples)
    for rid, reg, html, meta in rows_from_db():
        n += 1
        if '"content_text": "' in meta and '"content_text": ""' not in meta:
            with_content_text += 1
        v = visible_html_text(html)
        chars_kept += len(v.text)
        br = by_reg[reg]; br[0] += 1
        if v.removed_chars:
            n_hit += 1; chars_removed += v.removed_chars; br[1] += 1; br[2] += v.removed_chars
            for k, c in v.reasons.items():
                reasons[k] += c
            for k, snip in v.samples:
                if len(reason_rows[k]) < a.samples:
                    reason_rows[k].append((rid, reg, snip))
            r = v.ratio()
            if r > QUARANTINE_RATIO:
                n_quar += 1; br[3] += 1
            worst.append((r, rid, reg, v.removed_chars, v.total_chars, v.samples[:2]))
    worst.sort(reverse=True)
    print(f"stored pages scanned            : {n:,}   ({time.time()-t0:.0f}s)")
    print(f"pages with hidden text          : {n_hit:,}")
    print(f"characters removed / kept       : {chars_removed:,} / {chars_kept:,}")
    print(f"pages over {QUARANTINE_RATIO:.0%} removed (quarantine) : {n_quar:,}")
    print(f"rows with a crawler content_text: {with_content_text:,}   <- decide() prefers it over document_html")
    print("\nper regulator                          rows    hit   removed  >10%")
    for reg, (rows, hit, rem, q) in sorted(by_reg.items(), key=lambda x: -x[1][1]):
        if hit:
            print(f"  {reg[:38]:38} {rows:>6} {hit:>6} {rem:>9,} {q:>5}")
    print(f"\nreasons (chars)                 count   sample rows / snippets")
    for k, c in reasons.most_common():
        print(f"  {k:28} {c:>10,}")
        for rid, reg, snip in reason_rows[k]:
            print(f"      {rid:>6}  {reg[:30]:30}  {snip[:90]!r}")
    print(f"\nrows with the largest share removed (top {a.top}):")
    for r, rid, reg, rem, tot, samples in worst[:a.top]:
        print(f"  {rid:>6}  {reg[:30]:30}  {r:5.0%}  {rem:>7,} of {tot:>8,}   {samples[0][0] if samples else ''}: {samples[0][1][:70]!r}" if samples else
              f"  {rid:>6}  {reg[:30]:30}  {r:5.0%}  {rem:>7,} of {tot:>8,}")


def scan_pdfs(paths, a) -> None:
    import fitz
    t0 = time.time()
    tot_removed = tot_kept = 0
    reasons = collections.Counter()
    print(f"{'file':60} pages   kept  removed  reasons")
    for p in paths:
        try:
            doc = fitz.open(str(p))
        except Exception as e:
            print(f"  {p.name[:58]:58}  cannot open: {e}")
            continue
        kept = removed = dup = 0
        rs = collections.Counter(); samples = []
        pool = pdf_visible_pool(doc)
        for page in doc:
            v = visible_pdf_page_text(page, known_visible=pool)
            kept += len(v.text); removed += v.removed_chars; dup += v.duplicate_chars
            rs.update(v.reasons)
            samples.extend(v.samples)
        tot_removed += removed; tot_kept += kept; reasons.update(rs)
        print(f"  {p.name[:58]:58} {len(doc):>5} {kept:>7,} {removed:>8,}  {dict(rs) if rs else ''}"
              + (f"  duplicates dropped: {dup:,}" if dup else ""))
        for k, snip in samples[:a.samples]:
            print(f"        {k:16} {snip[:90]!r}")
        doc.close()
    print(f"\ntotal kept {tot_kept:,}  removed {tot_removed:,}  reasons {dict(reasons)}   ({time.time()-t0:.0f}s)")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pdf", nargs="+", type=Path, help="PDF files to scan instead of the database")
    ap.add_argument("--pdf-dir", type=Path, help="scan every .pdf under this folder")
    ap.add_argument("--samples", type=int, default=3, help="sample rows per reason")
    ap.add_argument("--top", type=int, default=20, help="rows with the largest share removed")
    a = ap.parse_args(argv)
    if a.pdf or a.pdf_dir:
        paths = list(a.pdf or [])
        if a.pdf_dir:
            paths += sorted(a.pdf_dir.rglob("*.pdf"))
        scan_pdfs(paths, a)
    else:
        scan_pages(a)
    return 0


if __name__ == "__main__":
    sys.exit(main())
