"""CHG-008 observe pass: what the injection guard would hold, over everything
stored. Never writes.

    python -m scripts.injection_guard_scan                  # every row of the database
    python -m scripts.injection_guard_scan --samples 5 --top 20
    python -m scripts.injection_guard_scan --pdf-dir DIR    # plain text of PDF files

The pattern list in config/injection_patterns.yml is only safe once it has
been run over every stored page: a phrase that occurs in genuine regulation
text shows up here as a line to read, with the matched words in context,
and is removed or tightened before enforcement. Reported: rows scanned,
rows with any hit, hits per pattern with samples, hits per regulator, and
the rows the hidden-text rule would hold.
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from processor.injection_guard import config, inspect   # noqa: E402


def rows_from_db():
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
    from jobs.monitor_jobs import _repo
    with _repo()._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, regulator, extra_meta FROM regulations")
        for rid, reg, meta in cur.fetchall():
            try:
                m = json.loads(meta) if meta else {}
            except Exception:
                m = {}
            page = str(m.get("content_text") or "")
            files = []
            if m.get("org_pdf_text"):
                files.append(("org_pdf_text", str(m["org_pdf_text"])))
            yield str(rid), str(reg or ""), page, files, m.get("hidden_text")


def pdf_items(paths):
    import fitz
    for p in paths:
        try:
            doc = fitz.open(p)
            text = "\n".join(page.get_text() for page in doc)
        except Exception as e:
            print(f"  skipped {p.name}: {e}")
            continue
        yield p.name, p.parent.name, "", [(p.name, text)], None


def scan(items, a) -> None:
    cfg = config()
    t0 = time.time()
    n = n_hit = n_hidden = 0
    chars = 0
    per_pattern = collections.Counter()
    rows_per_pattern = collections.defaultdict(set)
    per_reg = collections.Counter()
    samples = collections.defaultdict(list)
    hidden_rows = []
    for rid, reg, page, files, hidden in items:
        n += 1
        chars += len(page) + sum(len(t) for _, t in files)
        v = inspect(page, files, hidden, cfg=cfg)
        if v.hits:
            n_hit += 1
            per_reg[reg] += 1
            for h in v.hits:
                per_pattern[h.pattern] += 1
                rows_per_pattern[h.pattern].add(rid)
                if len(samples[h.pattern]) < a.samples:
                    samples[h.pattern].append((rid, reg, h.source, h.match, h.context))
        if v.hidden:
            n_hidden += 1
            hidden_rows.append((rid, reg, v.hidden))
    print(f"\n{n:,} rows scanned in {time.time() - t0:.0f}s, {chars:,} characters; "
          f"mode in config: {cfg.mode}, {len(cfg.phrases)} phrases, markers {'on' if cfg.marker_shapes else 'off'}, "
          f"hidden > {cfg.hidden_ratio:.0%} over {cfg.hidden_min_chars:,} chars")
    print(f"rows with a phrase or marker hit: {n_hit:,}   rows the hidden-text rule would hold: {n_hidden:,}")
    if per_pattern:
        print("\nHits per pattern (rows):")
        for p, c in per_pattern.most_common():
            print(f"  {c:>6,} in {len(rows_per_pattern[p]):>5,} rows  {p}")
        print("\nRows with hits per regulator:")
        for reg, c in per_reg.most_common(a.top):
            print(f"  {c:>6,}  {reg}")
        print("\nSamples per pattern:")
        for p, lst in samples.items():
            print(f"  -- {p}")
            for rid, reg, src, match, ctx in lst:
                print(f"     id {rid} [{reg}] in {src}: matched {match!r}\n        {ctx}")
    else:
        print("\nno phrase or marker hit anywhere")
    if hidden_rows:
        print(f"\nHidden-text rule, {len(hidden_rows)} rows:")
        for rid, reg, hv in hidden_rows[:a.top]:
            print(f"  id {rid} [{reg}]: " + "; ".join(f"{x['source']} {x['ratio']:.0%} of {x['total_chars']:,}" for x in hv))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pdf-dir", type=Path)
    ap.add_argument("--samples", type=int, default=3)
    ap.add_argument("--top", type=int, default=15)
    a = ap.parse_args(argv)
    if a.pdf_dir:
        paths = sorted(a.pdf_dir.rglob("*.pdf"))
        print(f"{len(paths)} PDF files under {a.pdf_dir}")
        scan(pdf_items(paths), a)
    else:
        scan(rows_from_db(), a)
    return 0


if __name__ == "__main__":
    sys.exit(main())
