"""Count what CHG-007's quoting would remove from stored text. Never writes.

    python -m scripts.quoted_source_scan                    # database: content_text, document_html, org_pdf_text
    python -m scripts.quoted_source_scan --pdf-dir folder   # plain text of every .pdf under a folder
    python -m scripts.quoted_source_scan --samples 5

The stripper in processor/quoted_source.py removes two shapes from a source
before it is quoted: a lookalike of our own marker ([[[BEGIN ...]]] /
[[[END ...]]]) and any tag name our prompts use (<document>, </task> ...).
Both should be absent from genuine regulation text. This scan says whether
that holds over everything stored, per field and per regulator, with the
matched text shown, so a false removal is a line to read rather than a
silently altered input.
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from processor.quoted_source import _MARKER_LOOKALIKE, _TAG_LOOKALIKE   # noqa: E402

FIELDS = ("content_text", "document_html", "org_pdf_text")
_TRIPLE = re.compile(r"\[\[\[|\]\]\]")          # the bracket run alone, wider than the marker shape


def rows_from_db():
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
    from jobs.monitor_jobs import _repo
    with _repo()._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, regulator, document_html, extra_meta FROM regulations")
        for rid, reg, html, meta in cur.fetchall():
            try:
                m = json.loads(meta) if meta else {}
            except Exception:
                m = {}
            yield (str(rid), str(reg or ""), {
                "content_text": str(m.get("content_text") or ""),
                "document_html": str(html or ""),
                "org_pdf_text": str(m.get("org_pdf_text") or ""),
            })


def _context(text: str, m: "re.Match", width: int = 60) -> str:
    s, e = max(0, m.start() - width), min(len(text), m.end() + width)
    return " ".join(text[s:e].split())


def scan_texts(items, a) -> None:
    """items: iterable of (rid, regulator, {field: text})."""
    t0 = time.time()
    n = 0
    chars = collections.Counter()
    nonempty = collections.Counter()
    hits = collections.Counter()                 # (field, kind) -> matches
    rows_hit = collections.defaultdict(set)      # (field, kind) -> rids
    by_reg = collections.Counter()               # regulator -> matches
    samples = collections.defaultdict(list)      # (field, kind) -> [(rid, reg, match, context)]
    tag_names = collections.Counter()
    for rid, reg, fields in items:
        n += 1
        for f, text in fields.items():
            if not text:
                continue
            nonempty[f] += 1
            chars[f] += len(text)
            for kind, rx in (("marker", _MARKER_LOOKALIKE), ("tag", _TAG_LOOKALIKE), ("triple_bracket", _TRIPLE)):
                for m in rx.finditer(text):
                    hits[(f, kind)] += 1
                    rows_hit[(f, kind)].add(rid)
                    by_reg[reg] += 1
                    if kind == "tag":
                        tag_names[m.group(0).lower()] += 1
                    if len(samples[(f, kind)]) < a.samples:
                        samples[(f, kind)].append((rid, reg, m.group(0), _context(text, m)))
    print(f"\n{n:,} rows scanned in {time.time() - t0:.0f}s")
    for f in FIELDS:
        print(f"  {f:14s} {nonempty[f]:>7,} non-empty, {chars[f]:>13,} chars")
    print("\nMatches (what the stripper would remove, plus any triple bracket at all):")
    any_hit = False
    for f in FIELDS:
        for kind in ("marker", "tag", "triple_bracket"):
            c = hits[(f, kind)]
            if c:
                any_hit = True
            print(f"  {f:14s} {kind:15s} {c:>6,} matches in {len(rows_hit[(f, kind)]):>6,} rows")
    if not any_hit:
        print("  none")
        return
    if tag_names:
        print("\nTag spellings matched:")
        for t, c in tag_names.most_common():
            print(f"  {c:>6,}  {t}")
    print("\nBy regulator:")
    for reg, c in by_reg.most_common(a.top):
        print(f"  {c:>6,}  {reg}")
    print("\nSamples:")
    for key, lst in samples.items():
        print(f"  -- {key[0]} / {key[1]}")
        for rid, reg, match, ctx in lst:
            print(f"     id {rid} [{reg}] matched {match!r}\n        ...{ctx}...")


def pdf_items(paths):
    import fitz
    for p in paths:
        try:
            doc = fitz.open(p)
            text = "\n".join(page.get_text() for page in doc)
        except Exception as e:
            print(f"  skipped {p.name}: {e}")
            continue
        yield (p.name, p.parent.name, {"content_text": "", "document_html": "", "org_pdf_text": text})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pdf-dir", type=Path)
    ap.add_argument("--samples", type=int, default=3)
    ap.add_argument("--top", type=int, default=15)
    a = ap.parse_args(argv)
    if a.pdf_dir:
        paths = sorted(a.pdf_dir.rglob("*.pdf"))
        print(f"{len(paths)} PDF files under {a.pdf_dir}")
        scan_texts(pdf_items(paths), a)
    else:
        scan_texts(rows_from_db(), a)
    return 0


if __name__ == "__main__":
    sys.exit(main())
