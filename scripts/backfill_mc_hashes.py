"""Give the stored Ministry of Commerce rows a content_hash, without hiding edits.

    python -m scripts.backfill_mc_hashes            # dry run
    python -m scripts.backfill_mc_hashes --apply

WHY THIS EXISTS

All 48 MC rows stored on 2026-08-14 have a NULL content_hash (the column was not
written by `_insert_regulation` until 2026-08-16). With no stored hash, the
orchestrator cannot compare content and falls back to `_hashless_unchanged`,
which looks at title, doc_path and the file set ONLY. So a law whose text was
edited but whose title and files did not move reads `unchanged` for ever.

MEASURED 2026-09-21 against the 2026-09-18 crawl: "Law of Commercial
Agencies / Regulation" (row 117) has the same 39 articles but a 27-article block
renumbered and ~900 characters shorter. The run classified it `unchanged`.

WHY IT IS NOT JUST "STORE TODAY'S HASH"

That is the obvious backfill and it is the wrong one. Stamping the crawl's hash
on a row whose content has ALREADY changed records the edit as the baseline and
hides it permanently -- `_hashless_unchanged` says so itself: "a false
`unchanged` hides a real edit for good". So each row is checked first:

    content matches the crawl   -> store the CRAWL'S hash. Next run: unchanged.
    content differs             -> store the hash of what we HOLD. Next run: the
                                   crawl's hash differs, the row reads
                                   `modified`, and a version row is written.
    no page in the crawl        -> leave NULL. Nothing to verify it against, and
                                   inventing a baseline is the failure above.

"Matches" ignores whitespace and the "⌄" chevron the site added to its section
headings after 2026-08-14; that glyph is markup, not content, and counting it
would make every row look edited.

The crawl compared against is the one already on disk
(output/mc_three_part/pages.json), read through MCLawsCrawler(reuse=True) so the
hash is computed by the crawler's own code and cannot drift from it. Nothing is
fetched.

Only rows whose content_hash IS NULL are touched, so re-running is a no-op.
`updated_at` is left alone: this records a fingerprint, not a change to the
document.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

REGULATOR = "Ministry of Commerce"
CHEVRON = "⌄"


def _norm(text) -> str:
    return re.sub(r"\s+", " ", (text or "").replace(CHEVRON, "")).strip()


def _lawid(*urls):
    for u in urls:
        m = re.search(r"lawId=([0-9a-f-]{36})", u or "", re.I)
        if m:
            return m.group(1).lower()
    return None


def _files(row_or_doc_meta: dict, document_url: str) -> list:
    """The file urls of one row, in stored order."""
    links = [x.strip() for x in str((row_or_doc_meta or {}).get("attachment_links") or "")
             .split("|") if x.strip()]
    if not links and document_url:
        links = [document_url.strip()]
    return links


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    a = ap.parse_args()

    from crawler.fingerprint import text_of_html
    from crawler.mc_crawler_wrapper import MCLawsCrawler
    from dynamic_crawler.formfill.runner import content_key
    from jobs.monitor_jobs import _repo

    fresh = {}
    for d in MCLawsCrawler(reuse=True).fetch_documents():
        meta = d.extra_meta or {}
        key = (_lawid(d.source_page_url, d.document_url), meta.get("mc_part"))
        if key[0] and key[1]:
            fresh[key] = d
    print(f"crawl on disk: {len(fresh)} law parts")

    repo = _repo()
    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT id, document_url, source_page_url, doc_path,
                              document_html, extra_meta
                       FROM regulations
                       WHERE regulator = ? AND content_hash IS NULL
                       ORDER BY id""", REGULATOR)
        rows = cur.fetchall()
    print(f"stored {REGULATOR} rows with a NULL content_hash: {len(rows)}")

    plan, skipped = [], []
    for rid, doc_url, page_url, doc_path, html, meta_json in rows:
        meta = json.loads(meta_json) if meta_json else {}
        part = meta.get("mc_part")
        law = json.loads(doc_path)[2] if doc_path else "?"
        label = f"{rid:>6} {law[:44]:44} / {part}"
        d = fresh.get((_lawid(page_url, doc_url), part))
        if d is None:
            skipped.append((label, "law is not on the crawl -- left NULL"))
            continue
        dmeta = d.extra_meta or {}

        if part == "attachment":
            stored = _files(meta, doc_url or "")
            new = _files(dmeta, d.document_url or "")
            same = bool(stored) and set(stored) == set(new)
            mine = content_key(" | ".join(stored))
        else:
            same = _norm(meta.get("content_text")) == _norm(dmeta.get("content_text"))
            if not (html or "").strip():
                skipped.append((label, "no stored html to hash -- left NULL"))
                continue
            mine = content_key(text_of_html(html))

        if same:
            plan.append((rid, label, d.content_hash, "VERIFIED"))
        else:
            plan.append((rid, label, mine, "DIFFERS"))

    verified = [p for p in plan if p[3] == "VERIFIED"]
    differs = [p for p in plan if p[3] == "DIFFERS"]
    print(f"\n  will store the crawl's hash (content verified identical): {len(verified)}")
    print(f"  will store the hash of the STORED content (edit pending):  {len(differs)}")
    for _, label, h, _v in differs:
        print(f"      DIFFERS  {label}")
    print(f"  left NULL:                                                 {len(skipped)}")
    for label, why in skipped[:30]:
        print(f"      skip     {label}  ({why})")

    if not a.apply:
        print("\ndry run -- nothing written. Re-run with --apply.")
        return 0

    n_reg = n_ver = 0
    with repo._get_conn() as conn:
        cur = conn.cursor()
        for rid, _label, h, _v in plan:
            if not h:
                continue
            cur.execute("UPDATE regulations SET content_hash = ? "
                        "WHERE id = ? AND content_hash IS NULL", h, rid)
            n_reg += cur.rowcount
            cur.execute("UPDATE regulation_versions SET content_hash = ? "
                        "WHERE regulation_id = ? AND status = 'active' "
                        "AND content_hash IS NULL", h, rid)
            n_ver += cur.rowcount
        conn.commit()
    print(f"\nwritten: {n_reg} regulations row(s), {n_ver} active version row(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
