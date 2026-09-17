"""
detect_shape.py -- test the page-shape detector on any regulator URL.

It fetches the page, tells you which SHAPE the agent would pick (flat_table /
sidebar_tree), and shows the evidence it used. No crawling, no LLM, no files
written -- just "what would the agent decide about this page".

Usage:
    python detect_shape.py https://rulebook.sama.gov.sa/en/sama-circulars
    python detect_shape.py https://rulebook.sama.gov.sa/en/saudi-central-bank-law
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import requests
from dynamic_crawler.auto import shapes

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

EXPLAIN = {
    "flat_table": ("This page is ONE list/table of documents. The agent will grab every "
                   "row of the table (and follow pagination), one document per row. "
                   "Completeness is checked against the number of rows in the table."),
    "sidebar_tree": ("This page is a NESTED tree of folders/categories. The agent will "
                     "recurse into every folder down to the individual documents. "
                     "Completeness is checked by whether it recursed deep enough."),
}


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    url = sys.argv[1]
    print(f"Fetching {url} ...")
    try:
        html = requests.get(url, timeout=30, headers={"User-Agent": _UA}).text
    except Exception as e:
        print(f"Could not fetch the page: {e}")
        sys.exit(1)

    shape = shapes.classify(html)

    print("\n" + "=" * 66)
    print(f"  DETECTED SHAPE :  {shape.name}")
    print(f"  EVIDENCE       :  {shape.evidence}")
    print(f"  'limit' means  :  {shape.limit_meaning}")
    print("=" * 66)
    print("\n  " + EXPLAIN.get(shape.name, ""))
    if shape.name == "flat_table":
        print(f"\n  -> A correct crawl should return about {shape.evidence.get('total_count')} documents.")
    print("\n  (This is just detection. To actually crawl, use the Crawl tab in the app.)\n")


if __name__ == "__main__":
    main()
