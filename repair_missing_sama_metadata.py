"""
repair_missing_sama_metadata.py
=================================
355 SAMA documents have substantial document_html but completely empty
reference_no / published_date / extra_meta -- the structured-leaf extractor
looks for an info-table on the page, and when it's not found at crawl time,
silently falls back to plain-content capture with no metadata extraction at
all. Confirmed for at least one case ("Finance Companies Control Law") that
the live page DOES have the info-table right now, meaning this was very
likely a transient fetch glitch during the original crawl.

This re-fetches each affected page fresh and re-runs structured-leaf
extraction. If the info-table is found this time, updates reference_no,
published_date, year, extra_meta, and document_html in place. If still not
found, the row is left untouched -- it's genuinely plain content, not a bug.

Usage:
    python repair_missing_sama_metadata.py --dry-run
    python repair_missing_sama_metadata.py
"""

import argparse
import json
import time

import pyodbc
from dotenv import load_dotenv

load_dotenv(override=True)

from push_sama_to_prod import _build_repo
from crawler.sama_rulebook_crawler import SAMAFullRulebookCrawler, REQUEST_DELAY


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    repo = _build_repo()
    crawler = SAMAFullRulebookCrawler(use_selenium=False)

    with repo._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, title, document_url, category FROM regulations
            WHERE regulator='SAMA' AND reference_no IS NULL AND published_date IS NULL
            AND extra_meta IS NULL AND LEN(document_html) > 2000
        """)
        rows = cur.fetchall()

    print(f"Found {len(rows)} suspect rows to recheck.\n")

    fixed, still_missing, errors = 0, 0, 0
    conn = repo._get_conn()
    pending = 0
    for i, (reg_id, title, url, category) in enumerate(rows, 1):
        try:
            soup = crawler._fetch(url)
            if not soup:
                errors += 1
                print(f"  [{i}] fetch failed: {title[:50]}")
                continue

            real_title = crawler._extract_page_title(soup, title)
            structured = crawler._extract_structured_leaf(soup, real_title)
            if not structured:
                still_missing += 1
                continue

            year = crawler._extract_year(structured["date_gregorian"])
            extra_meta = {}
            if structured["org_pdf_link"]:
                extra_meta["org_pdf_link"] = structured["org_pdf_link"]
            if structured["status"]:
                extra_meta["status"] = structured["status"]
            if structured["date_hijri"]:
                extra_meta["issue_date_hijri"] = structured["date_hijri"]

            if args.dry_run:
                fixed += 1
                print(f"  [{i}] WOULD FIX: {title[:50]} ({category}) -> "
                      f"ref={structured['reference_no']}, date={structured['date_gregorian']}")
                continue

            cur = conn.cursor()
            cur.execute(
                """
                UPDATE regulations SET reference_no=?, published_date=?, year=?,
                       extra_meta=?, document_html=?
                WHERE id=?
                """,
                [
                    structured["reference_no"], structured["date_gregorian"], year,
                    json.dumps(extra_meta) if extra_meta else None,
                    structured["document_html"], reg_id,
                ],
            )
            pending += 1
            if pending >= 20:
                conn.commit()
                pending = 0
            fixed += 1
        except pyodbc.Error as e:
            errors += 1
            print(f"  [{i}] DB error: {e}")
            try:
                conn.close()
            except Exception:
                pass
            conn = repo._get_conn()
            pending = 0
        except Exception as e:
            errors += 1
            print(f"  [{i}] error: {title[:50]} -- {e}")

        if i % 50 == 0 or i == len(rows):
            print(f"  [{i}/{len(rows)}] fixed {fixed}, still missing {still_missing}, errors {errors}")
        time.sleep(REQUEST_DELAY)

    if not args.dry_run:
        try:
            conn.commit()
        except pyodbc.Error:
            pass
        conn.close()

    print(f"\nDone: {fixed} fixed, {still_missing} confirmed genuinely plain-content, {errors} errors")


if __name__ == "__main__":
    main()
