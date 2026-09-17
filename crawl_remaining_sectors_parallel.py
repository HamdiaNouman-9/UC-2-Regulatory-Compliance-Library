"""
crawl_remaining_sectors_parallel.py
====================================
Runs the remaining SAMA Rulebook sectors concurrently, one thread per sector.
Each thread builds its own SAMAFullRulebookCrawler instance (own requests.Session),
so there's no shared mutable crawl state between sectors — only stdout logging
is interleaved.

Usage:
    python crawl_remaining_sectors_parallel.py
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from crawler.sama_rulebook_crawler import SAMAFullRulebookCrawler, _Node

SECTORS = [
    ("Banking Sector",        "https://rulebook.sama.gov.sa/en/book-category/1363"),
    ("Finance Sector",        "https://rulebook.sama.gov.sa/en/book-category/1365"),
    ("Money Exchange Sector", "https://rulebook.sama.gov.sa/en/book-category/1366"),
    ("Credit Bureaus",        "https://rulebook.sama.gov.sa/en/book-category/5902"),
    ("Regulatory Sandbox",    "https://rulebook.sama.gov.sa/en/book-category/1368"),
]


def run_sector(name: str, url: str):
    crawler = SAMAFullRulebookCrawler(use_selenium=False)
    node = _Node(title=name, url=url, is_folder_hint=True)
    docs = crawler.crawl_sector(node)
    crawler._save_sector(name, docs)
    return name, len(docs)


def main():
    results = {}
    failures = {}
    with ThreadPoolExecutor(max_workers=len(SECTORS)) as pool:
        futures = {pool.submit(run_sector, name, url): name for name, url in SECTORS}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                sector, count = fut.result()
                results[sector] = count
                print(f"\n*** DONE: {sector} -> {count} docs ***\n")
            except Exception as e:
                failures[name] = str(e)
                print(f"\n*** FAILED: {name} -> {e} ***\n")

    print("\n" + "=" * 70)
    print("PARALLEL CRAWL COMPLETE")
    print("=" * 70)
    for sector, count in results.items():
        print(f"  {sector:30s} {count:4d} docs")
    if failures:
        print("\nFailures:")
        for sector, err in failures.items():
            print(f"  {sector:30s} {err}")
    total = sum(results.values())
    print(f"\n  TOTAL: {total} documents")


if __name__ == "__main__":
    main()
