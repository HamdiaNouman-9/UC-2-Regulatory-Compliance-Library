"""
crawl_all_sectors_parallel.py
===============================
Re-crawls all 8 SAMA Rulebook sectors with the latest crawler fixes
(cycle-safe dedup, notification-box exclusion, folder/listing hub-content
capture), in parallel but with safeguards against the connection-drop /
rate-limiting issues seen when running too many threads against the SAMA
site at once:
  - capped concurrency (MAX_WORKERS) instead of one thread per sector
  - per-sector retry if a sector's crawl dies partway through

Usage:
    python crawl_all_sectors_parallel.py
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from crawler.sama_rulebook_crawler import SAMAFullRulebookCrawler, _Node

SECTORS = [
    ("Laws and Implementing Regulations", "https://rulebook.sama.gov.sa/en/book-category/1361"),
    ("All Financial Institutions",        "https://rulebook.sama.gov.sa/en/book-category/1362"),
    ("Banking Sector",                    "https://rulebook.sama.gov.sa/en/book-category/1363"),
    ("Finance Sector",                    "https://rulebook.sama.gov.sa/en/book-category/1365"),
    ("Payment Systems",                   "https://rulebook.sama.gov.sa/en/book-category/1367"),
    ("Money Exchange Sector",             "https://rulebook.sama.gov.sa/en/book-category/1366"),
    ("Credit Bureaus",                    "https://rulebook.sama.gov.sa/en/book-category/5902"),
    ("Regulatory Sandbox",                "https://rulebook.sama.gov.sa/en/book-category/1368"),
]

MAX_WORKERS = 4
MAX_RETRIES_PER_SECTOR = 2


def run_sector(name: str, url: str):
    last_err = None
    for attempt in range(1, MAX_RETRIES_PER_SECTOR + 1):
        try:
            crawler = SAMAFullRulebookCrawler(use_selenium=False)
            node = _Node(title=name, url=url, is_folder_hint=True)
            docs = crawler.crawl_sector(node)
            if not docs:
                # crawl_sector() fails soft (returns []) on a dead landing-page
                # fetch instead of raising -- treat that as a failure here so
                # it retries, and so we never overwrite existing good data
                # with an empty result.
                raise RuntimeError(f"crawl_sector returned 0 documents for {name}")
            crawler._save_sector(name, docs)
            return name, len(docs), None
        except Exception as e:
            last_err = e
            print(f"\n*** {name}: attempt {attempt}/{MAX_RETRIES_PER_SECTOR} failed ({e}) -- retrying ***\n")
            time.sleep(5)
    return name, 0, str(last_err)


def main():
    only = sys.argv[1:]
    sectors = [(n, u) for n, u in SECTORS if not only or n in only]

    results = {}
    failures = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(run_sector, name, url): name for name, url in sectors}
        for fut in as_completed(futures):
            name, count, err = fut.result()
            if err:
                failures[name] = err
                print(f"\n*** FAILED: {name} -> {err} ***\n")
            else:
                results[name] = count
                print(f"\n*** DONE: {name} -> {count} docs ***\n")

    print("\n" + "=" * 70)
    print("PARALLEL CRAWL COMPLETE")
    print("=" * 70)
    for sector, count in results.items():
        print(f"  {sector:40s} {count:4d} docs")
    if failures:
        print("\nFailures (after retries):")
        for sector, err in failures.items():
            print(f"  {sector:40s} {err}")
    print(f"\n  TOTAL: {sum(results.values())} documents")


if __name__ == "__main__":
    main()
