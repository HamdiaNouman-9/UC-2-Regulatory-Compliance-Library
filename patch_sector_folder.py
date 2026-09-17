"""
patch_sector_folder.py
========================
Re-crawls a single folder within an already-crawled sector (using the fixed
sama_rulebook_crawler logic: cycle-safe dedup, notification-box links excluded
from listing recursion, notification-box content captured as its own hub doc)
and patches the sector's existing JSON in place -- removes the old entries
under that folder and replaces them with the freshly-crawled ones.

Usage:
    python patch_sector_folder.py \
        --input output/sama_rulebook/Finance_Sector.json \
        --sector "Finance Sector" \
        --sector-url "https://rulebook.sama.gov.sa/en/book-category/1365" \
        --folder-title "Anti Money Laundering and Combating the Financing of Terrorism" \
        --folder-url "https://rulebook.sama.gov.sa/en/anti-money-laundering-and-combating-financing-terrorism-3"
"""

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from crawler.sama_rulebook_crawler import SAMAFullRulebookCrawler, _Node


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--sector", required=True)
    parser.add_argument("--sector-url", required=True)
    parser.add_argument("--path-prefix", default="", help="Slash-separated titles between the sector and the folder, e.g. 'Rules and Instructions'")
    parser.add_argument("--folder-title", required=True)
    parser.add_argument("--folder-url", required=True)
    args = parser.parse_args()

    prefix_titles = [p for p in args.path_prefix.split("/") if p]
    full_prefix = ["SAMA", "SAMA RULEBOOK", args.sector] + prefix_titles + [args.folder_title]

    crawler = SAMAFullRulebookCrawler()
    crawler._sector_name = args.sector
    crawler._sector_url = args.sector_url
    crawler._book_nav_id = crawler._book_nav_id_for(args.sector_url)

    node = _Node(title=args.folder_title, url=args.folder_url, is_folder_hint=True)
    results = []
    crawler._process(node, prefix_titles, len(prefix_titles), set(), results)

    print(f"\nRe-crawled {len(results)} document(s) under '{args.folder_title}':")
    for d in results:
        print("  -", " > ".join(d.doc_path))

    path = Path(args.input)
    existing = json.loads(path.read_text(encoding="utf-8"))
    before = len(existing)
    kept = [d for d in existing if (d.get("doc_path") or [])[:len(full_prefix)] != full_prefix]
    removed = before - len(kept)

    new_data = [asdict(d) for d in results]
    kept.extend(new_data)

    path.write_text(json.dumps(kept, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nRemoved {removed} old entries, added {len(new_data)} new entries.")
    print(f"Sector total: {before} -> {len(kept)}")
    print(f"Saved -> {path}")


if __name__ == "__main__":
    main()
