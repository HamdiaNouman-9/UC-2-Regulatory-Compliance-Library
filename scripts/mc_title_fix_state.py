"""Carry the Ministry of Commerce title change into the local monitoring state.

    python scripts/mc_title_fix_state.py            # dry run, writes nothing
    python scripts/mc_title_fix_state.py --apply    # rewrites, keeps a .bak

Run AFTER output/mc_title_fix/mc_title_fix.sql, on every machine that runs MC
monitoring (its output/change_state/ is local to that machine).

WHY: output/change_state/ministry_of_commerce_regulations_and_laws.json keys its
records by identity -- `document_url=...|doc_path=[...]` -- and doc_path's last
element changed from "Laws" / "Regulation" / "Attachments" to the part's full
title. Left alone, the next stored-inventory sweep matches none of its records:
every document reads as newly seen and every old key starts an absence streak,
and absence streaks feed the `disappeared` -> withdrawal gate.

The new last element is derived exactly as crawler/mc_crawler_wrapper.py
MC_TITLE does, from the law (doc_path[2]) and the old part label. Idempotent:
records already carrying the new form are left as they are.
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from crawler.mc_crawler_wrapper import MC_TITLE  # noqa: E402

STATE = ROOT / "output" / "change_state" / "ministry_of_commerce_regulations_and_laws.json"
LABEL_TO_PART = {"Laws": "law", "Regulation": "regulation", "Attachments": "attachment"}


def _new_doc_path(doc_path_json: str):
    """New json.dumps(list) for an old doc_path, or None if already migrated."""
    p = json.loads(doc_path_json)
    if len(p) != 4 or p[3] not in LABEL_TO_PART:
        return None
    p[3] = MC_TITLE[LABEL_TO_PART[p[3]]].format(law=p[2])
    return json.dumps(p)


def main(apply: bool) -> int:
    if not STATE.exists():
        print(f"no state file at {STATE} - nothing to do")
        return 0
    data = json.loads(STATE.read_text(encoding="utf-8"))
    records = data.get("records", {})
    out, changed = {}, 0
    for key, rec in records.items():
        fields = dict(rec.get("fields") or {})
        new_dp = _new_doc_path(fields["doc_path"]) if "doc_path" in fields else None
        if new_dp is None:
            out[key] = rec
            continue
        old_dp = fields["doc_path"]
        fields["doc_path"] = new_dp
        if "title" in fields:
            fields["title"] = json.loads(new_dp)[3]
        new_key = key.replace(f"doc_path={old_dp}", f"doc_path={new_dp}")
        if new_key == key:
            raise RuntimeError(f"doc_path not found in key, refusing to guess: {key[:120]}")
        out[new_key] = {**rec, "fields": fields}
        changed += 1
    print(f"{len(records)} records, {changed} to migrate, {len(records) - changed} unchanged")
    if not apply:
        print("dry run - pass --apply to write")
        return 0
    if changed:
        shutil.copy2(STATE, STATE.with_suffix(".json.bak"))
        data["records"] = out
        STATE.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"written; backup at {STATE.with_suffix('.json.bak')}")
    return 0


if __name__ == "__main__":
    sys.exit(main("--apply" in sys.argv))
