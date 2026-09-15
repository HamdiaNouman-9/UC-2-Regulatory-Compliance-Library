"""ExcelRepo — the same schema as MSSQL, written to a workbook instead.

A drop-in stand-in for `storage/mssql_repo.py::MSSQLRepository`. The orchestrator
never learns it is not talking to SQL Server: same method names, same return
shapes, same auto-increment ids. Rows accumulate in memory and are written to one
.xlsx at the end, one sheet per table.

WHY
    Running the new orchestrator against production MSSQL means ~700 inserts, a
    folder tree and analysis rows you then have to unpick. This gives the whole
    flow — folder tree, versioning, change detection, analysis — with output you
    can open in Excel and throw away.

SHEETS (mirroring the real tables)
    regulations             one row per document
    compliancecategory      the folder tree (id, title, parentid)
    regulation_versions     content snapshots — now for every regulator
    compliance_analysis     the LLM output
    requirement_mappings    matched obligations
    processing_log          one row per step, per document
    run_history             row_count + inventory_hash per run (the monitoring gate)

WHAT IT DOES NOT DO
    The requirement-matching corpus reads (`get_all_compliance_requirements`,
    `get_all_demo_controls`, `get_all_demo_kpis`) return EMPTY. There is no
    internal register in a spreadsheet, so matching will correctly find nothing
    to match against. Anything that depends on the real corpus has to be checked
    against the real database — this repo cannot tell you about it.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _as_dict(obj) -> dict:
    """Every attribute, not just the declared dataclass fields.

    `asdict()` returns ONLY declared fields, and `RegulatoryDocument` does not
    declare `status` — the real `_insert_regulation` picks it up with
    `getattr(document, "status", "active")`. So a plain asdict() silently dropped
    the monitoring state that the pipeline had just set, and the column came out
    empty while looking like nothing was wrong.
    """
    if isinstance(obj, dict):
        return dict(obj)
    out = {}
    if is_dataclass(obj):
        out.update(asdict(obj))
    out.update({k: v for k, v in vars(obj).items()
                if not k.startswith("_") and k not in out})
    return out


#: Excel's hard limit is 32,767 characters per cell. Anything longer is cut, and
#: the cut is what a reader sees AND what `promote.py` would copy into the
#: database — so a long document's HTML silently lost its tail on the way in.
_CELL_LIMIT = 32000

#: Every value that overflowed a cell, keyed by the marker written in its place.
#: A module-level store because `_flat` is called from seven places without a
#: repo instance; `ExcelRepo.save()` writes it out beside the workbook.
_OVERFLOW: Dict[str, str] = {}

#: Marker written into the cell. `promote.py` and `_load_workbook_into` look for
#: this prefix and rehydrate from the sidecar.
OVERFLOW_PREFIX = "@@ff-overflow:"


def _stash_overflow(v: str) -> str:
    """Park an oversized value in the sidecar and return its marker + a preview.

    The cell stays READABLE — the preview is the first 32k, as before — but it is
    no longer the only copy. GOSI made the cost visible: a 92,995-character
    instrument was stored as 32,028 characters, so the HTML stopped at Article 26
    and nothing said the rest had been dropped except a suffix in the middle of
    the text.
    """
    import hashlib
    key = hashlib.md5(v.encode("utf-8")).hexdigest()[:16]
    _OVERFLOW[key] = v
    return (v[:_CELL_LIMIT]
            + f"  …[truncated for Excel, {len(v):,} chars — full value in "
              f"the .fulltext.json sidecar] {OVERFLOW_PREFIX}{key}")


def _flat(v: Any) -> Any:
    """Excel cannot hold a list or a dict, and it truncates at 32,767 chars.

    Oversized strings are preserved in a sidecar rather than thrown away — see
    `_stash_overflow`. Lists become " | " joins, which is how doc_path and
    extra_meta's attachment_links read in a workbook.
    """
    if isinstance(v, (list, tuple)):
        return " | ".join(str(x) for x in v)
    if isinstance(v, dict):
        s = json.dumps(v, ensure_ascii=False)
        return _stash_overflow(s) if len(s) > _CELL_LIMIT else s
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, str) and len(v) > _CELL_LIMIT:
        return _stash_overflow(v)
    return v


def resolve_overflow(value: Any, sidecar: Dict[str, str] | None) -> Any:
    """The full value behind a cell, or the cell unchanged.

    Anything reading a workbook back — `promote.py` on its way to MSSQL, or the
    API's `_load_workbook_into` — must call this, or it copies the preview into
    the database and the truncation becomes permanent.
    """
    if not isinstance(value, str) or OVERFLOW_PREFIX not in value:
        return value
    key = value.rsplit(OVERFLOW_PREFIX, 1)[-1].strip()
    if sidecar and key in sidecar:
        return sidecar[key]
    return value


class ExcelRepo:
    def __init__(self, out_path: str | Path):
        self.out_path = Path(out_path)
        self.t: Dict[str, List[dict]] = {
            "regulations": [], "compliancecategory": [], "regulation_versions": [],
            "compliance_analysis": [], "requirement_mappings": [],
            "processing_log": [], "run_history": [],
        }
        self._next = {k: 0 for k in self.t}
        # The orchestrator processes documents on a thread pool
        # (DOC_MAX_WORKERS, default 4). `self._next[t] += 1` is a read-modify-
        # write and is NOT atomic, so without this two documents could be handed
        # the same primary key — silently, with the workbook still looking fine.
        self._lock = threading.RLock()

    def _id(self, table: str) -> int:
        with self._lock:
            self._next[table] += 1
            return self._next[table]

    # ---------------- regulations ----------------------------------------- #

    def _insert_regulation(self, doc) -> int:
        d = _as_dict(doc)
        rid = self._id("regulations")
        d["id"] = rid
        d["inserted_at"] = datetime.now().isoformat(timespec="seconds")
        self.t["regulations"].append({k: _flat(v) for k, v in d.items()})
        return rid

    def update_regulation(self, regulation_id: int, **fields) -> None:
        updated_at = fields.pop("updated_at", None)
        for r in self.t["regulations"]:
            if r.get("id") == regulation_id:
                r.update({k: _flat(v) for k, v in fields.items()})
                r["updated_at"] = _flat(updated_at) if updated_at is not None \
                    else datetime.now().isoformat(timespec="seconds")
                return

    def get_regulation_by_id(self, regulation_id: int) -> Optional[dict]:
        return next((dict(r) for r in self.t["regulations"]
                     if r.get("id") == regulation_id), None)

    # Identity lookups. An empty workbook means every document is new on the
    # first run — which is what makes a second run against the same workbook the
    # interesting test of change detection.

    def document_exists(self, title, published_date, doc_path) -> bool:
        path = " > ".join(doc_path or []) if isinstance(doc_path, list) else (doc_path or "")
        return any(r.get("title") == title
                   and str(r.get("published_date") or "") == str(published_date or "")
                   and str(r.get("doc_path") or "") == path
                   for r in self.t["regulations"])

    def document_exists_by_url(self, document_url: str, category: str = None) -> bool:
        return any(r.get("document_url") == document_url
                   and (category is None or r.get("category") == category)
                   for r in self.t["regulations"])

    def document_exists_by_source_url(self, source_page_url: str) -> bool:
        return any(r.get("source_page_url") == source_page_url
                   for r in self.t["regulations"])

    @staticmethod
    def _norm_path(v) -> str:
        """doc_path in ONE canonical form, whatever it was stored as.

        The same field has three representations in this codebase and a plain
        string compare matches none of them:

            MSSQL       json.dumps(list)   '["MISA", "MISA-LAWS"]'
            ExcelRepo   " | ".join(list)   'MISA | MISA-LAWS'
            classify    " > ".join(list)   'MISA > MISA-LAWS'

        So find_by_identity never matched, every document came back `new` on
        every run, and a second run against the same workbook inserted all of
        them again — 3 documents became 6 rows. Change detection, the whole point
        of the monitoring path, could not work.
        """
        if v is None:
            return ""
        if isinstance(v, (list, tuple)):
            parts = list(v)
        else:
            s = str(v).strip()
            if s.startswith("["):
                try:
                    parts = json.loads(s)
                except Exception:
                    parts = [s]
            elif " > " in s:
                parts = s.split(" > ")
            elif " | " in s:
                parts = s.split(" | ")
            else:
                parts = [s] if s else []
        return " > ".join(str(p).strip() for p in parts if str(p).strip())

    @staticmethod
    def _with_extra_meta(r: Optional[dict]) -> Optional[dict]:
        """extra_meta as a dict, however the workbook happened to store it.
        Mirrors the MSSQL method so both repos hand back the same shape."""
        if r is None:
            return None
        raw = r.get("extra_meta")
        if isinstance(raw, dict):
            return r
        r["extra_meta"] = {}
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                r["extra_meta"] = parsed if isinstance(parsed, dict) else {}
            except Exception:
                pass
        return r

    def find_by_identity(self, document_url: str, doc_path: str) -> Optional[dict]:
        """The identity lookup `classify_documents` uses: (document_url, doc_path)."""
        want = self._norm_path(doc_path)
        return self._with_extra_meta(
            next((dict(r) for r in self.t["regulations"]
                  if r.get("document_url") == document_url
                  and self._norm_path(r.get("doc_path")) == want), None))

    def find_by_identity_fields(self, fields: dict) -> Optional[dict]:
        """Identity lookup on whichever columns the source config names.
        Mirrors the MSSQL method."""
        fields = {k: v for k, v in (fields or {}).items() if v not in (None, "")}
        if not fields:
            return None
        from dynamic_crawler.changesignal import resolve_field
        for r in self.t["regulations"]:
            row = self._with_extra_meta(dict(r))
            for k, v in fields.items():
                # A dotted name reads inside extra_meta — used by the
                # multi-attachment identity (doc_path + the files it carries),
                # which has no single document_url to compare.
                stored = resolve_field(row, k) if "." in k else row.get(k)
                if k == "doc_path":
                    stored, v = self._norm_path(stored), self._norm_path(v)
                if (str(stored) if stored is not None else "") != str(v):
                    break
            else:
                return row
        return None

    def find_regulations_by_source(self, source_system: str,
                                   regulator: Optional[str] = None) -> list:
        """Everything stored for this source. Mirrors the MSSQL method — same
        regulator scoping, and extra_meta comes back parsed there too."""
        if not source_system:
            return []
        return [self._with_extra_meta(dict(r)) for r in self.t["regulations"]
                if r.get("source_system") == source_system
                and (not regulator or r.get("regulator") == regulator)
                and (r.get("status") or "") != "withdrawn"]

    def mark_regulation_withdrawn(self, regulation_id: int, reason: str) -> None:
        """A regulator has withdrawn this document. Nothing calls this yet.

        Mirrors the MSSQL method — status, a marker version, no delete. Added on
        both sides at once because three bugs so far have been a repo method that
        existed on one.
        """
        if not self.get_regulation_by_id(regulation_id):
            raise ValueError(f"no regulation {regulation_id} to withdraw")
        self.mark_all_versions_inactive(regulation_id)
        self.insert_regulation_version(regulation_id, status="withdrawn",
                                       change_summary=str(reason or "")[:400])
        self.update_regulation(regulation_id, status="withdrawn")

    def find_by_reference(self, reference_no: str) -> Optional[dict]:
        """The tiebreak: same reference number at a new URL is a new VERSION of an
        existing document, not a new document."""
        if not reference_no:
            return None
        return next((dict(r) for r in self.t["regulations"]
                     if r.get("reference_no") == reference_no), None)

    # ---------------- the folder tree -------------------------------------- #

    def get_folder_id(self, title: str, parent_id: Optional[int]) -> Optional[int]:
        return next((f["compliancecategory_id"] for f in self.t["compliancecategory"]
                     if f["title"] == title and f["parentid"] == parent_id), None)

    def insert_folder(self, title: str, parent_id, cat_type: str = "F") -> int:
        fid = self._id("compliancecategory")
        self.t["compliancecategory"].append(
            {"compliancecategory_id": fid, "title": title,
             "parentid": parent_id, "type": cat_type})
        return fid

    def find_folder_in_subtree(self, title: str, ancestor_id: int) -> Optional[int]:
        kids = {ancestor_id}
        changed = True
        while changed:
            changed = False
            for f in self.t["compliancecategory"]:
                if f["parentid"] in kids and f["compliancecategory_id"] not in kids:
                    kids.add(f["compliancecategory_id"]); changed = True
        return next((f["compliancecategory_id"] for f in self.t["compliancecategory"]
                     if f["title"] == title and f["compliancecategory_id"] in kids
                     and f["compliancecategory_id"] != ancestor_id), None)

    def regulation_exists_for_category(self, compliancecategory_id: int) -> bool:
        return any(r.get("compliancecategory_id") == compliancecategory_id
                   for r in self.t["regulations"])

    # ---------------- versions -------------------------------------------- #

    def insert_regulation_version(self, regulation_id: int, content_text: str = "",
                                  content_html: str = "", content_hash: str = "",
                                  updated_date=None, change_summary: str = "",
                                  status: str = "active", created_at=None, **kw) -> int:
        vid = self._id("regulation_versions")
        self.t["regulation_versions"].append({
            "version_id": vid, "regulation_id": regulation_id,
            "content_hash": content_hash, "status": status,
            "updated_date": _flat(updated_date or date.today()),
            "change_summary": change_summary,
            "content_text": _flat(content_text), "content_html": _flat(content_html),
            "created_at": _flat(created_at) if created_at is not None
                else datetime.now().isoformat(timespec="seconds"),
        })
        return vid

    # kept for callers that still use the CBB-era name
    insert_cbb_version = insert_regulation_version

    def get_regulation_versions(self, regulation_id: int) -> list:
        return [dict(v) for v in self.t["regulation_versions"]
                if v["regulation_id"] == regulation_id]

    def get_active_regulation_version(self, regulation_id: int) -> Optional[dict]:
        return next((dict(v) for v in self.t["regulation_versions"]
                     if v["regulation_id"] == regulation_id and v["status"] == "active"), None)

    def mark_all_versions_inactive(self, regulation_id: int) -> int:
        n = 0
        for v in self.t["regulation_versions"]:
            if v["regulation_id"] == regulation_id and v["status"] == "active":
                v["status"] = "inactive"; n += 1
        return n

    def get_content_hash(self, regulation_id: int) -> Optional[str]:
        r = self.get_regulation_by_id(regulation_id)
        return (r or {}).get("content_hash")

    # the CBB-named pair the orchestrator still calls
    get_cbb_content_hash = get_content_hash

    def update_cbb_content_hash(self, regulation_id: int, content_hash: str):
        self.update_regulation(regulation_id, content_hash=content_hash)

    def get_last_cbb_crawl_date(self):
        return None

    # ---------------- analysis -------------------------------------------- #

    def store_analysis(self, rows: list, version_id: Optional[int] = None):
        for row in rows or []:
            d = _as_dict(row)
            d["analysis_id"] = self._id("compliance_analysis")
            d["version_id"] = version_id
            self.t["compliance_analysis"].append({k: _flat(v) for k, v in d.items()})

    def archive_current_analysis(self, regulation_id: int, version_id: int) -> int:
        n = 0
        for a in self.t["compliance_analysis"]:
            if a.get("regulation_id") == regulation_id and a.get("status") != "inactive":
                a["status"] = "inactive"; a["archived_version_id"] = version_id; n += 1
        return n

    def store_requirement_mappings(self, mappings: list, version_id: Optional[int] = None):
        for m in mappings or []:
            d = _as_dict(m)
            d["mapping_id"] = self._id("requirement_mappings")
            d["version_id"] = version_id
            self.t["requirement_mappings"].append({k: _flat(v) for k, v in d.items()})

    def store_control_links(self, *a, **k):  return None
    def store_kpi_links(self, *a, **k):      return None
    def insert_new_suggested_control(self, *a, **k):     return None
    def insert_new_suggested_kpi(self, *a, **k):         return None
    def insert_new_suggested_requirement(self, *a, **k): return None
    def flag_partially_matched_requirements(self, *a, **k): return None
    def get_linked_controls_by_requirement(self, *a, **k): return []
    def get_linked_kpis_by_requirement(self, *a, **k):    return []

    # The internal register does not exist in a spreadsheet. Empty, not fabricated
    # — matching will correctly report nothing to match against.
    def get_all_compliance_requirements(self, *a, **k): return []
    def get_all_demo_controls(self, *a, **k):           return []
    def get_all_demo_kpis(self, *a, **k):               return []

    # ---------------- logging + run history ------------------------------- #

    def _log_processing(self, regulation_id, step, status, message, doc_url=None,
                        duration_ms=None, **kw):
        # duration_ms is a real column here rather than JSON: a preview workbook
        # is read by eye, and the point of the timing is to be sortable in Excel.
        self.t["processing_log"].append({
            "log_id": self._id("processing_log"), "regulation_id": regulation_id,
            "step": step, "status": status, "message": _flat(message),
            "document_url": doc_url,
            "duration_ms": int(duration_ms) if duration_ms is not None else None,
            "logged_at": datetime.now().isoformat(timespec="seconds"),
        })

    def record_run(self, source: str, row_count: int, inventory_hash: str,
                   verdict: str, note: str = ""):
        """The monitoring gate's memory: what a run found, so the next run can
        tell a real withdrawal from a broken crawl."""
        self.t["run_history"].append({
            "run_id": self._id("run_history"), "source": source,
            "row_count": row_count, "inventory_hash": inventory_hash,
            "verdict": verdict, "note": note,
            "run_at": datetime.now().isoformat(timespec="seconds"),
        })

    def last_good_run(self, source: str) -> Optional[dict]:
        runs = [r for r in self.t["run_history"]
                if r["source"] == source and r["verdict"] in ("PASS", "WARN")]
        return runs[-1] if runs else None

    # ---------------- raw connection: deliberately refused ----------------- #

    def _get_conn(self):
        raise NotImplementedError(
            "ExcelRepo has no SQL connection. The CBB path in orchestrator.py "
            "issues raw UPDATE statements through _get_conn(); NewOrchestrator "
            "replaces that with mark_all_versions_inactive() so versioning works "
            "for every regulator without hand-written SQL.")

    # ---------------- write it out ---------------------------------------- #

    def sidecar_path(self) -> Path:
        """Where the untruncated values live, beside the workbook."""
        return self.out_path.with_suffix(".fulltext.json")

    def save(self) -> Path:
        import pandas as pd
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(self.out_path, engine="openpyxl") as xl:
            for name, rows in self.t.items():
                df = pd.DataFrame(rows or [{"(empty)": ""}])
                df.to_excel(xl, sheet_name=name[:31], index=False)
        # Anything too long for a cell, in full. Written only when something
        # actually overflowed, so most runs produce no sidecar at all.
        if _OVERFLOW:
            sc = self.sidecar_path()
            sc.write_text(json.dumps(_OVERFLOW, ensure_ascii=False), encoding="utf-8")
            logger.info("ExcelRepo wrote %s (%d oversized value(s) preserved)",
                        sc, len(_OVERFLOW))
        logger.info("ExcelRepo wrote %s", self.out_path)
        return self.out_path

    def counts(self) -> dict:
        return {k: len(v) for k, v in self.t.items()}


__all__ = ["ExcelRepo"]
