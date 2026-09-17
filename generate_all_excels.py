"""
generate_all_excels.py
=======================
Converts every already-crawled output/sama_rulebook/<Sector>.json into its own
Excel report via sama_finance_sector_to_excel.build_excel(), skipping the
combined sama_rulebook_all.json and any empty/stale 0-doc files.

Usage:
    python generate_all_excels.py
"""

import json
from pathlib import Path

from sama_finance_sector_to_excel import build_excel, EXCEL_DIR

INPUT_DIR = Path("output") / "sama_rulebook"
SKIP_FILES = {"sama_rulebook_all.json"}


def main():
    EXCEL_DIR.mkdir(parents=True, exist_ok=True)
    json_files = sorted(INPUT_DIR.glob("*.json"))

    for path in json_files:
        if path.name in SKIP_FILES:
            continue

        documents = json.loads(path.read_text(encoding="utf-8"))
        if not documents:
            print(f"Skipping {path.name} (0 docs)")
            continue

        sector_label = path.stem.replace("_", " ")
        xlsx_path = EXCEL_DIR / f"SAMA_{path.stem}.xlsx"
        build_excel(documents, xlsx_path, sector_label=sector_label)
        print(f"{sector_label:45s} {len(documents):4d} docs  ->  {xlsx_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
