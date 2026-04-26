from typing import Dict, List, Optional
import pyodbc
import json
from storage.repository import DocumentRepository
from models.models import RegulatoryDocument
import logging

logger = logging.getLogger(__name__)


class MSSQLRepository(DocumentRepository):
    """
    MSSQL implementation for regulatory documents.

    VERSIONING STRATEGY (unified):
    ─────────────────────────────────────────────────────────────────────
    compliance_analysis          → CURRENT active rows for ALL regulators
                                   (CBB, SAMA, SBP, SECP). version_id is
                                   populated for CBB, NULL for others.

    compliance_analysis_versions → ARCHIVED/historical rows for CBB only.
                                   Rows move here (status='inactive') when
                                   a CBB document's content changes and a
                                   new version is created.

    regulation_versions          → Content snapshots for CBB (HTML + text
                                   + hash per version). regulator column
                                   already exists on this table.
    ─────────────────────────────────────────────────────────────────────
    """

    def __init__(self, conn_params: dict):
        self.conn_params = conn_params

    # ================================================================== #
    #  CONNECTION                                                          #
    # ================================================================== #

    def _get_conn(self):
        conn_str = (
            f"DRIVER={self.conn_params.get('driver', '{ODBC Driver 17 for SQL Server}')};"
            f"SERVER={self.conn_params['server']};"
            f"DATABASE={self.conn_params['database']};"
            f"UID={self.conn_params['username']};"
            f"PWD={self.conn_params['password']}"
        )
        return pyodbc.connect(conn_str)

    # ================================================================== #
    #  FOLDER MANAGEMENT                                                   #
    # ================================================================== #

    def get_folder_id(self, title: str, parent_id: Optional[int]) -> Optional[int]:
        """
        Look up a folder by title + parent_id in compliancecategory.
        Returns the folder's ID, or None if not found.
        """
        query = """
            SELECT TOP 1 compliancecategory_id
            FROM compliancecategory
            WHERE title = ?
              AND (
                (parentid IS NULL AND ? IS NULL)
                OR parentid = ?
              )
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [title, parent_id, parent_id])
            row = cursor.fetchone()
            return int(row[0]) if row else None

    def insert_folder(self, title: str, parent_id, cat_type: str = "F") -> int:
     query = """
        INSERT INTO compliancecategory (title, parentid, type)
        OUTPUT INSERTED.compliancecategory_id
        VALUES (?, ?, ?)
    """
     with self._get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(query, [title, parent_id, cat_type])
        row = cursor.fetchone()
        conn.commit()
        return int(row[0])
    # ================================================================== #
    #  REGULATION INSERT / UPDATE                                          #
    # ================================================================== #

    def _insert_regulation(self, document: RegulatoryDocument) -> int:
        doc_path_list = getattr(document, "doc_path", None)
        doc_path_json = json.dumps(doc_path_list) if doc_path_list else None

        extra_meta = getattr(document, "extra_meta", {}) or {}
        if getattr(document, "urdu_url", None):
            extra_meta["urdu_url"] = document.urdu_url
        extra_meta_json = json.dumps(extra_meta) if extra_meta else None

        document_html = getattr(document, "document_html", None)

        # type defaults to "R" for regulations; callers can override via document.type
        doc_type   = getattr(document, "type",   "R") or "R"
        doc_status = getattr(document, "status", "active") or "active"

        sql = """
            INSERT INTO regulations (
                regulator, source_system, category,
                title, document_url, doc_path,
                published_date, reference_no,
                department, year,
                source_page_url, extra_meta,
                compliancecategory_id, document_html,
                type, status
            )
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                department_value = (
                    json.dumps(document.department)
                    if isinstance(document.department, list)
                    else (str(document.department) if document.department else None)
                )
                year_value = str(document.year) if document.year is not None else None
                cursor.execute(sql, (
                    document.regulator,
                    document.source_system,
                    document.category,
                    document.title,
                    document.document_url,
                    doc_path_json,
                    document.published_date,
                    document.reference_no,
                    department_value,
                    year_value,
                    document.source_page_url,
                    extra_meta_json,
                    getattr(document, "compliancecategory_id", None),
                    document_html,
                    doc_type,
                    doc_status,
                ))
                reg_id = cursor.fetchone()[0]
                conn.commit()

            document.id = reg_id
            logger.info(f"Inserted regulation ID: {reg_id} (type={doc_type})")
            return reg_id
        except Exception as e:
            logger.error(f"Failed to insert regulation: {e}")
            raise

    def update_regulation(self, regulation_id: int, **kwargs):
        if not kwargs:
            return
        set_clause = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values()) + [regulation_id]
        query = f"""
            UPDATE regulations
            SET {set_clause}, updated_at = SYSDATETIMEOFFSET()
            WHERE id = ?
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, tuple(values))
                conn.commit()
            logger.info(f"Updated regulation {regulation_id}: {list(kwargs.keys())}")
        except Exception as e:
            logger.error(f"Failed to update regulation {regulation_id}: {e}")
            raise

    def save_metadata(self, document: RegulatoryDocument) -> None:
        try:
            regulation_id = document.id
            if not regulation_id:
                logger.warning("Cannot save metadata: document has no ID")
                return

            extra_meta = getattr(document, "extra_meta", {}) or {}
            extra_meta_json = json.dumps(extra_meta) if extra_meta else None

            query = """
                UPDATE regulations
                SET extra_meta = ?, updated_at = SYSDATETIMEOFFSET()
                WHERE id = ?
            """

            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (extra_meta_json, regulation_id))
                conn.commit()

            logger.info(f"Saved metadata for regulation {regulation_id}")

        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
            raise

    # ================================================================== #
    #  DOCUMENT EXISTENCE CHECKS                                           #
    # ================================================================== #

    def document_exists(self, title: str, published_date: str, doc_path: list) -> bool:
        doc_path_json = json.dumps(doc_path) if doc_path else None
        query = """
            SELECT 1
            FROM regulations
            WHERE title = ?
              AND (
                    (published_date IS NULL AND ? IS NULL)
                    OR published_date = ?
                  )
              AND (
                    (doc_path IS NULL AND ? IS NULL)
                    OR doc_path = ?
                  )
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    query,
                    (title, published_date, published_date, doc_path_json, doc_path_json)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Document existence check failed: {e}")
            return False

    def document_exists_by_source_url(self, source_page_url: str) -> bool:
        return self.get_regulation_id_by_source_url(source_page_url) is not None

    def get_regulation_id_by_source_url(self, source_page_url: str) -> Optional[int]:
        query = """
            SELECT id
            FROM regulations
            WHERE source_page_url = ?
              AND regulator = 'Central Bank of Bahrain'
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (source_page_url,))
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Failed to check source_page_url existence: {e}")
            return None


    # ================================================================== #
    #  REGULATION RETRIEVAL                                                #
    # ================================================================== #

    def get_regulation_id_by_doc_path(self, doc_path: list) -> Optional[int]:
     if not doc_path:
        return None
     doc_path_json = json.dumps(doc_path, ensure_ascii=False)
     query = """
        SELECT TOP 1 id
        FROM regulations
        WHERE doc_path = ?
    """
     try:
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [doc_path_json])
            row = cursor.fetchone()
            return int(row[0]) if row else None
     except Exception as e:
        logger.error(f"Failed to check doc_path existence: {e}")
        return None

    def get_regulation_by_id(self, regulation_id: int) -> Optional[dict]:
        query = """
            SELECT
                id, regulator, source_system, category, title,
                document_url, doc_path, published_date, reference_no,
                department, year, source_page_url,
                CAST(extra_meta AS NVARCHAR(MAX)) as extra_meta,
                compliancecategory_id,
                CAST(created_at AS DATETIME2) as created_at,
                CAST(updated_at AS DATETIME2) as updated_at,
                document_html, content_hash, type, status
            FROM regulations
            WHERE id = ?
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (regulation_id,))
                columns = [column[0] for column in cursor.description]
                row = cursor.fetchone()
                if row:
                    result = dict(zip(columns, row))
                    if result.get('doc_path'):
                        result['doc_path'] = json.loads(result['doc_path'])
                    if result.get('department'):
                        try:
                            result['department'] = json.loads(result['department'])
                        except Exception:
                            pass
                    if result.get('extra_meta'):
                        result['extra_meta'] = json.loads(result['extra_meta'])
                    return result
                return None
        except Exception as e:
            logger.error(f"Failed to get regulation by ID: {e}")
            return None

    # ================================================================== #
    #  CBB CONTENT VERSIONING  (regulation_versions table)                 #
    # ================================================================== #

    def get_last_cbb_crawl_date(self):
        query = """
            SELECT MAX(created_at)
            FROM regulations
            WHERE regulator = 'Central Bank of Bahrain'
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                row = cursor.fetchone()
                return row[0] if row and row[0] else None
        except Exception as e:
            logger.error(f"Failed to get last CBB crawl date: {e}")
            return None

    def get_cbb_content_hash(self, regulation_id: int) -> Optional[str]:
        query = "SELECT content_hash FROM regulations WHERE id = ?"
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (regulation_id,))
                row = cursor.fetchone()
                return row[0] if row and row[0] else None
        except Exception as e:
            logger.warning(f"Could not get content hash for {regulation_id}: {e}")
            return None

    def update_cbb_content_hash(self, regulation_id: int, content_hash: str):
        query = """
            UPDATE regulations
            SET content_hash = ?, updated_at = SYSDATETIMEOFFSET()
            WHERE id = ?
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (content_hash, regulation_id))
                conn.commit()
                logger.info(f"Updated content hash for regulation {regulation_id}")
        except Exception as e:
            logger.error(f"Failed to update content hash: {e}")
            raise

    def insert_regulation_version(
        self,
        regulation_id: int,
        regulator: str,
        content_html: str,
        content_text: str,
        content_hash: str,
        updated_date,
        change_summary: str,
        status: str = "active",
    ) -> int:
        """
        Insert a new version snapshot into regulation_versions.
        Returns the new version_id.
        """
        query = """
            INSERT INTO regulation_versions
                (regulation_id, regulator, content_html, content_text,
                 content_hash, updated_date, change_summary, status, created_at)
            OUTPUT INSERTED.version_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [
                regulation_id,
                regulator,
                content_html,
                content_text,
                content_hash,
                updated_date,
                change_summary,
                status,
            ])
            row = cursor.fetchone()
            conn.commit()
            return int(row[0])
        
    def insert_cbb_version(
        self,
        regulation_id: int,
        content_html: str,
        content_text: str,
        content_hash: str,
        updated_date,
        change_summary: str,
    ) -> int:
        return self.insert_regulation_version(
            regulation_id=regulation_id,
            regulator='Central Bank of Bahrain',
            content_html=content_html,
            content_text=content_text,
            content_hash=content_hash,
            updated_date=updated_date,
            change_summary=change_summary,
        )

    def get_regulation_versions(self, regulation_id: int) -> list:
        query = """
            SELECT
                version_id, regulation_id, regulator,
                content_hash, updated_date,
                CAST(created_at AS DATETIME2) as created_at,
                change_summary, status
            FROM regulation_versions
            WHERE regulation_id = ?
            ORDER BY version_id DESC
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (regulation_id,))
                cols = [c[0] for c in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get regulation versions for {regulation_id}: {e}")
            return []

    def get_active_regulation_version(self, regulation_id: int) -> Optional[dict]:
        """
        Get the current active version for a regulation.
        Returns a dict with version fields, or None.
        """
        query = """
            SELECT TOP 1
                version_id, content_html, content_text, content_hash,
                status, change_summary, created_at
            FROM regulation_versions
            WHERE regulation_id = ?
              AND status = 'active'
            ORDER BY created_at DESC, version_id DESC
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [regulation_id])
            row = cursor.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cursor.description]
            return dict(zip(cols, row))
        
    def mark_all_versions_inactive(self, regulation_id: int) -> int:
        """
        Mark ALL active versions for a regulation as inactive.
        Call this BEFORE inserting a new active version.
        Returns the count of rows updated.
        """
        query = """
            UPDATE regulation_versions
            SET status = 'inactive'
            WHERE regulation_id = ?
              AND status = 'active'
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [regulation_id])
            rows_updated = cursor.rowcount
            conn.commit()
            return rows_updated    

    # ================================================================== #
    #  COMPLIANCE ANALYSIS — UNIFIED PRIMARY STORE                         #
    # ================================================================== #

    def store_analysis(
        self,
        rows: List[dict],
        version_id: Optional[int] = None,
    ) -> None:
        query = """
            INSERT INTO compliance_analysis (
                regulation_id, version_id,
                requirement_id, requirement_title,
                execution_category, criticality, obligation_type,
                stage1_json, stage2_json, stage3_json, stage4_md,
                analysis_json, schema_version,
                status, is_current
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'v2', 'active', 1)
        """

        def _s(v):
            if v is None:
                return None
            return v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)

        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                for row in rows:
                    cursor.execute(query, (
                        row["regulation_id"],
                        version_id,
                        row.get("requirement_id"),
                        row.get("requirement_title"),
                        row.get("execution_category"),
                        row.get("criticality"),
                        row.get("obligation_type"),
                        _s(row.get("stage1_json")),
                        _s(row.get("stage2_json")),
                        _s(row.get("stage3_json")),
                        row.get("stage4_md"),
                        _s(row.get("analysis_json")),
                    ))
                conn.commit()
                logger.info(
                    f"Stored {len(rows)} analysis rows in compliance_analysis "
                    f"(version_id={version_id})"
                )
        except Exception as e:
            logger.error(f"Failed to store analysis: {e}")
            raise

    def store_staged_analysis(self, rows: List[dict]) -> None:
        self.store_analysis(rows, version_id=None)

    def archive_current_analysis(self, regulation_id: int, version_id: int) -> int:
        """
        Archive current compliance_analysis rows for a regulation+version
        into compliance_analysis_versions.
        Returns count archived.
        """
        query = """
            INSERT INTO compliance_analysis_versions
                (regulation_id, version_id, requirement_text, obligation_type,
                 control_category, risk_level, compliance_status, created_at)
            SELECT
                regulation_id, ?, requirement_text, obligation_type,
                control_category, risk_level, compliance_status, GETDATE()
            FROM compliance_analysis
            WHERE regulation_id = ?
              AND is_current = 1
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [version_id, regulation_id])
            archived = cursor.rowcount
            conn.commit()
            return archived

    # ================================================================== #
    #  COMPLIANCE ANALYSIS — READ                                          #
    # ================================================================== #

    def get_compliance_analysis(self, regulation_id: int) -> List[dict]:
        query = """
            SELECT
                id, regulation_id, version_id,
                requirement_id, requirement_title,
                execution_category, criticality, obligation_type,
                analysis_json, stage1_json, stage2_json, stage3_json, stage4_md,
                schema_version, status, is_current,
                CAST(created_at AS DATETIME2) as created_at
            FROM compliance_analysis
            WHERE regulation_id = ?
              AND schema_version = 'v2'
              AND is_current = 1
            ORDER BY requirement_id
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, [regulation_id])
                return self._parse_analysis_rows(cursor)
        except Exception as e:
            logger.error(f"Failed to get compliance analysis: {e}")
            return []

    def get_compliance_analysis_v2(self, regulation_id: int) -> List[dict]:
        return self.get_compliance_analysis(regulation_id)

    def get_analysis_version_history(self, regulation_id: int) -> list:
        query = """
            SELECT
                cav.version_id,
                cav.regulation_id,
                cav.status,
                cav.schema_version,
                MIN(cav.created_at)    AS archived_at,
                rv.content_hash,
                rv.updated_date,
                rv.change_summary,
                COUNT(cav.id)          AS requirement_count
            FROM compliance_analysis_versions cav
            LEFT JOIN regulation_versions rv
                   ON cav.version_id = rv.version_id
            WHERE cav.regulation_id = ?
            GROUP BY
                cav.version_id, cav.regulation_id, cav.status,
                cav.schema_version,
                rv.content_hash, rv.updated_date, rv.change_summary
            ORDER BY cav.version_id DESC
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, [regulation_id])
                cols = [c[0] for c in cursor.description]
                rows = []
                for row in cursor.fetchall():
                    d = dict(zip(cols, row))
                    for f in ["archived_at", "updated_date"]:
                        if d.get(f):
                            d[f] = str(d[f])
                    rows.append(d)
                return rows
        except Exception as e:
            logger.error(f"Failed to get analysis version history for {regulation_id}: {e}")
            return []

    def get_analysis_versions(self, regulation_id: int) -> list:
        return self.get_analysis_version_history(regulation_id)

    def get_analysis_version_detail(self, regulation_id: int, version_id: int) -> list:
        query = """
            SELECT
                id, regulation_id, version_id,
                requirement_id, requirement_title,
                execution_category, criticality, obligation_type,
                stage2_json, stage3_json, stage4_md,
                schema_version, status, created_at
            FROM compliance_analysis_versions
            WHERE regulation_id = ? AND version_id = ?
            ORDER BY requirement_id
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, [regulation_id, version_id])
                cols = [c[0] for c in cursor.description]
                rows = []
                for row in cursor.fetchall():
                    d = dict(zip(cols, row))
                    for f in ["stage2_json", "stage3_json"]:
                        if d.get(f) and isinstance(d[f], str):
                            try:
                                d[f] = json.loads(d[f])
                            except Exception:
                                pass
                    if d.get("created_at"):
                        d["created_at"] = str(d["created_at"])
                    rows.append(d)
                return rows
        except Exception as e:
            logger.error(f"Failed to get version detail {version_id}: {e}")
            return []

    def get_stage4_executive_summary(self, regulation_id: int) -> Optional[str]:
        query = """
            SELECT TOP 1 stage4_md
            FROM compliance_analysis
            WHERE regulation_id = ?
              AND schema_version = 'v2'
              AND is_current = 1
              AND stage4_md IS NOT NULL
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (regulation_id,))
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Failed to get stage4 summary: {e}")
            return None

    def _parse_analysis_rows(self, cursor) -> List[dict]:
        cols = [c[0] for c in cursor.description]
        rows = []
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            for field in ("analysis_json", "stage1_json", "stage2_json", "stage3_json"):
                if d.get(field) and isinstance(d[field], str):
                    try:
                        d[field] = json.loads(d[field])
                    except Exception:
                        pass
            rows.append(d)
        return rows

    # ================================================================== #
    #  REQUIREMENT MATCHING — FETCH EXISTING                               #
    # ================================================================== #

    def get_all_compliance_requirements(self) -> list:
        query = """
            SELECT
                COMPLIANCEREQUIREMENT_ID as id,
                TITLE                    as title,
                DESCRIPTION              as description
            FROM COMPLIANCE_REQUIREMENT
            WHERE TITLE IS NOT NULL AND DESCRIPTION IS NOT NULL
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                results = [{"id": r[0], "title": r[1], "description": r[2]} for r in rows]
                logger.info(f"Fetched {len(results)} existing compliance requirements")
                return results
        except Exception as e:
            logger.error(f"Failed to fetch compliance requirements: {e}")
            return []

    def get_all_demo_controls(self) -> list:
        query = """
            SELECT CONTROL_ID, TITLE, DESCRIPTION, CONTROL_KEY
            FROM DEMO_CONTROL
            WHERE TITLE IS NOT NULL
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                return [
                    {"id": r[0], "title": r[1], "description": r[2], "control_key": r[3]}
                    for r in cursor.fetchall()
                ]
        except Exception as e:
            logger.error(f"Failed to fetch demo controls: {e}")
            return []

    def get_all_demo_kpis(self) -> list:
        query = """
            SELECT KISETUP_ID, TITLE, DESCRIPTION, KISETUP_KEY
            FROM DEMO_KPI
            WHERE TITLE IS NOT NULL
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                return [
                    {"id": r[0], "title": r[1], "description": r[2], "kisetup_key": r[3]}
                    for r in cursor.fetchall()
                ]
        except Exception as e:
            logger.error(f"Failed to fetch demo KPIs: {e}")
            return []

    def get_linked_controls_by_requirement(self) -> dict:
        query = "SELECT COMPLIANCEREQUIREMENT_ID, CONTROL_ID FROM DEMO_REQUIREMENT_CONTROL_LINK"
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = {}
                for req_id, ctrl_id in cursor.fetchall():
                    result.setdefault(req_id, []).append(ctrl_id)
                return result
        except Exception as e:
            logger.error(f"Failed to fetch linked controls: {e}")
            return {}

    def get_linked_kpis_by_requirement(self) -> dict:
        query = "SELECT COMPLIANCEREQUIREMENT_ID, KISETUP_ID FROM DEMO_REQUIREMENT_KPI_LINK"
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                result = {}
                for req_id, kpi_id in cursor.fetchall():
                    result.setdefault(req_id, []).append(kpi_id)
                return result
        except Exception as e:
            logger.error(f"Failed to fetch linked KPIs: {e}")
            return {}

    # ================================================================== #
    #  REQUIREMENT MATCHING — STORE                                        #
    # ================================================================== #

    def store_requirement_mappings(self, mappings: list, version_id: Optional[int] = None):
        query = """
            INSERT INTO sama_requirement_mapping (
                regulation_id, extracted_requirement_text,
                matched_requirement_id, match_status, match_explanation,
                version_id
            ) VALUES (?, ?, ?, ?, ?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                for m in mappings:
                    cursor.execute(query, (
                        m["regulation_id"],
                        m["extracted_requirement_text"],
                        m.get("matched_requirement_id"),
                        m["match_status"],
                        m.get("match_explanation"),
                        version_id,
                    ))
                conn.commit()
                logger.info(
                    f"Stored {len(mappings)} requirement mappings (version_id={version_id})"
                )
        except Exception as e:
            logger.error(f"Failed to store requirement mappings: {e}")
            raise

    def flag_partially_matched_requirements(self, matched_requirement_ids: list):
        if not matched_requirement_ids:
            return
        placeholders = ",".join(["?" for _ in matched_requirement_ids])
        query = f"""
            UPDATE COMPLIANCE_REQUIREMENT
            SET IS_SUGGESTED = 1
            WHERE COMPLIANCEREQUIREMENT_ID IN ({placeholders})
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, matched_requirement_ids)
                conn.commit()
                logger.info(f"Flagged {len(matched_requirement_ids)} requirements with IS_SUGGESTED=1")
        except Exception as e:
            logger.error(f"Failed to flag partially matched requirements: {e}")
            raise

    def insert_new_suggested_requirement(self, requirement: dict) -> int:
        query = """
            INSERT INTO COMPLIANCE_REQUIREMENT (TITLE, DESCRIPTION, REF_KEY, REF_NO, IS_SUGGESTED, CREATEDON)
            OUTPUT INSERTED.COMPLIANCEREQUIREMENT_ID
            VALUES (?, ?, ?, ?, 1, GETDATE())
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (
                    requirement.get("title", "")[:500],
                    requirement.get("description", ""),
                    requirement.get("ref_key", "SAMA-AUTO"),
                    requirement.get("ref_no", "")
                ))
                new_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Inserted new suggested requirement ID: {new_id}")
                return new_id
        except Exception as e:
            logger.error(f"Failed to insert new suggested requirement: {e}")
            raise

    def store_control_links(self, control_links: list):
        query = """
            INSERT INTO DEMO_REQUIREMENT_CONTROL_LINK (
                COMPLIANCEREQUIREMENT_ID, CONTROL_ID,
                MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID
            ) VALUES (?, ?, ?, ?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                for link in control_links:
                    cursor.execute(query, (
                        link["compliancerequirement_id"],
                        link["control_id"],
                        link["match_status"],
                        link.get("match_explanation"),
                        link.get("regulation_id")
                    ))
                conn.commit()
                logger.info(f"Stored {len(control_links)} control links")
        except Exception as e:
            logger.error(f"Failed to store control links: {e}")
            raise

    def store_kpi_links(self, kpi_links: list):
        query = """
            INSERT INTO DEMO_REQUIREMENT_KPI_LINK (
                COMPLIANCEREQUIREMENT_ID, KISETUP_ID,
                MATCH_STATUS, MATCH_EXPLANATION, REGULATION_ID
            ) VALUES (?, ?, ?, ?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                for link in kpi_links:
                    cursor.execute(query, (
                        link["compliancerequirement_id"],
                        link["kisetup_id"],
                        link["match_status"],
                        link.get("match_explanation"),
                        link.get("regulation_id")
                    ))
                conn.commit()
                logger.info(f"Stored {len(kpi_links)} KPI links")
        except Exception as e:
            logger.error(f"Failed to store KPI links: {e}")
            raise

    # ================================================================== #
    #  REQUIREMENT MATCHING — READ MAPPINGS                                #
    # ================================================================== #

    def get_requirement_mappings_by_regulation(self, regulation_id: int) -> list:
        query = """
            SELECT
                srm.regulation_id,
                srm.extracted_requirement_text,
                srm.matched_requirement_id,
                srm.match_status,
                srm.match_explanation,
                srm.version_id,
                srm.obligation_id,
                srm.requirement_id,
                cr.TITLE as matched_requirement_title,
                cr.DESCRIPTION as matched_requirement_description
            FROM sama_requirement_mapping srm
            LEFT JOIN COMPLIANCE_REQUIREMENT cr 
                ON srm.matched_requirement_id = cr.COMPLIANCEREQUIREMENT_ID
            WHERE srm.regulation_id = ?
            ORDER BY srm.id
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, [regulation_id])
                cols = [c[0] for c in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get requirement mappings: {e}")
            return []

    def get_control_links_by_regulation(self, regulation_id: int) -> list:
        query = """
            SELECT
                drcl.COMPLIANCEREQUIREMENT_ID,
                drcl.CONTROL_ID,
                drcl.MATCH_STATUS,
                drcl.MATCH_EXPLANATION,
                drcl.REGULATION_ID,
                dc.TITLE as control_title,
                dc.DESCRIPTION as control_description,
                dc.CONTROL_KEY as control_key,
                dc.IS_SUGGESTED as is_suggested
            FROM DEMO_REQUIREMENT_CONTROL_LINK drcl
            JOIN DEMO_CONTROL dc ON drcl.CONTROL_ID = dc.CONTROL_ID
            WHERE drcl.REGULATION_ID = ?
            ORDER BY drcl.COMPLIANCEREQUIREMENT_ID
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, [regulation_id])
                cols = [c[0] for c in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get control links: {e}")
            return []

    def get_kpi_links_by_regulation(self, regulation_id: int) -> list:
        query = """
            SELECT
                drkl.COMPLIANCEREQUIREMENT_ID,
                drkl.KISETUP_ID,
                drkl.MATCH_STATUS,
                drkl.MATCH_EXPLANATION,
                drkl.REGULATION_ID,
                dk.TITLE as kpi_title,
                dk.DESCRIPTION as kpi_description,
                dk.KISETUP_KEY as kisetup_key,
                dk.IS_SUGGESTED as is_suggested
            FROM DEMO_REQUIREMENT_KPI_LINK drkl
            JOIN DEMO_KPI dk ON drkl.KISETUP_ID = dk.KISETUP_ID
            WHERE drkl.REGULATION_ID = ?
            ORDER BY drkl.COMPLIANCEREQUIREMENT_ID
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, [regulation_id])
                cols = [c[0] for c in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get KPI links: {e}")
            return []

    def get_control_links_by_requirement_ids(self, requirement_ids: list) -> list:
        if not requirement_ids:
            return []
        placeholders = ",".join(["?" for _ in requirement_ids])
        query = f"""
            SELECT
                drcl.COMPLIANCEREQUIREMENT_ID,
                drcl.CONTROL_ID,
                drcl.MATCH_STATUS,
                drcl.MATCH_EXPLANATION,
                drcl.REGULATION_ID,
                dc.TITLE as control_title,
                dc.DESCRIPTION as control_description,
                dc.CONTROL_KEY as control_key,
                dc.IS_SUGGESTED as is_suggested
            FROM DEMO_REQUIREMENT_CONTROL_LINK drcl
            JOIN DEMO_CONTROL dc ON drcl.CONTROL_ID = dc.CONTROL_ID
            WHERE drcl.COMPLIANCEREQUIREMENT_ID IN ({placeholders})
            ORDER BY drcl.COMPLIANCEREQUIREMENT_ID
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, requirement_ids)
                cols = [c[0] for c in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get control links by requirement IDs: {e}")
            return []

    def insert_new_suggested_control(self, control: dict) -> int:
        query = """
            INSERT INTO DEMO_CONTROL (TITLE, DESCRIPTION, CONTROL_KEY, IS_SUGGESTED, CREATEDON)
            OUTPUT INSERTED.CONTROL_ID
            VALUES (?, ?, ?, 1, GETDATE())
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (
                    control.get("title", "")[:500],
                    control.get("description", ""),
                    control.get("control_key", f"SAMA-AUTO-CTRL-{control.get('title', '')[:20]}")
                ))
                new_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Inserted new suggested control ID: {new_id}")
                return new_id
        except Exception as e:
            logger.error(f"Failed to insert new suggested control: {e}")
            raise

    def insert_new_suggested_kpi(self, kpi: dict) -> int:
        query = """
            INSERT INTO DEMO_KPI (TITLE, DESCRIPTION, KISETUP_KEY, FORMULA, IS_SUGGESTED, CREATEDON)
            OUTPUT INSERTED.KISETUP_ID
            VALUES (?, ?, ?, ?, 1, GETDATE())
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (
                    kpi.get("title", "")[:500],
                    kpi.get("description", ""),
                    kpi.get("kisetup_key", f"SAMA-AUTO-KPI-{kpi.get('title', '')[:20]}"),
                    kpi.get("formula", "")
                ))
                new_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Inserted new suggested KPI ID: {new_id}")
                return new_id
        except Exception as e:
            logger.error(f"Failed to insert new suggested KPI: {e}")
            raise

    # ================================================================== #
    #  LOGGING                                                             #
    # ================================================================== #

    def _log_processing(self, regulation_id, step, status, message, details=None, document_url=None):
        details_json = json.dumps(details) if details else None
        query = """
            INSERT INTO processinglogs (regulation_id, step, status, message, details)
            VALUES (?, ?, ?, ?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (regulation_id, step, status, message, details_json))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to write processing log: {e}")

    # ================================================================== #
    #  UTILITY                                                             #
    # ================================================================== #

    def execute_query(self, query: str, params: tuple = ()) -> list:
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"execute_query failed: {e}")
            raise

    def execute_update(self, query: str, params: tuple = ()) -> int:
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                rowcount = cursor.rowcount
                conn.commit()
                return rowcount
        except Exception as e:
            logger.error(f"execute_update failed: {e}")
            raise