from typing import Dict

import pyodbc
import json
from storage.repository import DocumentRepository
from models.models import RegulatoryDocument
import logging

logger = logging.getLogger(__name__)


class MSSQLRepository(DocumentRepository):
    """
    MSSQL implementation for regulatory documents.
    Uses pyodbc for SQL Server connectivity.
    """

    def __init__(self, conn_params: dict):
        """
        conn_params should contain:
        {
            'server': 'localhost',
            'database': 'regulations_db',
            'username': 'your_user',
            'password': 'your_password',
            'driver': '{ODBC Driver 17 for SQL Server}'  # or appropriate driver
        }
        """
        self.conn_params = conn_params

    # -------------------- CONNECTION --------------------
    def _get_conn(self):
        conn_str = (
            f"DRIVER={self.conn_params.get('driver', '{ODBC Driver 17 for SQL Server}')};"
            f"SERVER={self.conn_params['server']};"
            f"DATABASE={self.conn_params['database']};"
            f"UID={self.conn_params['username']};"
            f"PWD={self.conn_params['password']}"
        )
        return pyodbc.connect(conn_str)

    # -------------------- FOLDER MANAGEMENT --------------------
    def get_folder_id(self, title: str, parent_id: int = None) -> int:
        """
        Get the compliancecategory_id for a given title and parent.
        Returns None if not found.
        """
        query = """
            SELECT compliancecategory_id 
            FROM compliancecategory 
            WHERE title = ? AND (parentid = ? OR (parentid IS NULL AND ? IS NULL))
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (title, parent_id, parent_id))
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Failed to get folder ID: {e}")
            return None

    def insert_folder(self, title: str, parent_id: int = None) -> int:
        """
        Insert a new folder in compliancecategory table.
        Returns the new compliancecategory_id.
        """
        query = """
            INSERT INTO compliancecategory (title, parentid)
            OUTPUT INSERTED.compliancecategory_id
            VALUES (?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (title, parent_id))
                folder_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Created folder: {title} (ID: {folder_id}, Parent: {parent_id})")
                return folder_id
        except Exception as e:
            logger.error(f"Failed to insert folder: {e}")
            raise

    # -------------------- IDENTITY --------------------
    def _get_regulation_id(self, document: RegulatoryDocument):
        """
        Get regulation ID based on reference_no or document_url.
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()

                # First try with reference_no if available
                if document.reference_no:
                    cursor.execute(
                        """
                        SELECT id FROM regulations
                        WHERE regulator=? AND category=? AND reference_no=?
                        """,
                        (document.regulator, document.category, document.reference_no)
                    )
                    row = cursor.fetchone()
                    if row:
                        return row[0]

                # Fall back to document_url
                cursor.execute(
                    """
                    SELECT id FROM regulations
                    WHERE regulator=? AND category=? AND document_url=?
                    """,
                    (document.regulator, document.category, document.document_url)
                )
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Failed to get regulation ID: {e}")
            return None

    # -------------------- INSERT --------------------
    def _insert_regulation(self, document: RegulatoryDocument) -> int:
        """
        Insert a regulation. doc_path is stored as NVARCHAR(MAX) (JSON array string).
        urdu_url is stored inside extra_meta JSON.
        document_html is optional - inserted if present, otherwise NULL.
        """
        doc_path_list = getattr(document, "doc_path", None)

        # Convert doc_path list to JSON string for MSSQL
        doc_path_json = json.dumps(doc_path_list) if doc_path_list else None

        # Merge urdu_url into extra_meta
        extra_meta = getattr(document, "extra_meta", {}) or {}
        if getattr(document, "urdu_url", None):
            extra_meta["urdu_url"] = document.urdu_url

        # Convert extra_meta to JSON string
        extra_meta_json = json.dumps(extra_meta) if extra_meta else None

        # Get document_html (optional - only SAMA has it at insert time)
        document_html = getattr(document, "document_html", None)

        sql = """
            INSERT INTO regulations (
                regulator, source_system, category,
                title, document_url, doc_path,
                published_date, reference_no,
                department, year,
                source_page_url, extra_meta,
                compliancecategory_id, document_html
            )
            OUTPUT INSERTED.id
            VALUES (?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                if isinstance(document.department, list):
                    department_value = json.dumps(document.department)
                else:
                    department_value = str(document.department) if document.department else None
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
                    document_html  # Will be None for SBP/SECP, has value for SAMA
                ))
                reg_id = cursor.fetchone()[0]
                conn.commit()

            document.id = reg_id
            logger.info(f"Inserted regulation ID: {reg_id}")
            return reg_id
        except Exception as e:
            logger.error(f"Failed to insert regulation: {e}")
            raise

    # -------------------- DOCUMENT EXISTENCE --------------------
    def document_exists(self, title: str, published_date: str, doc_path: list) -> bool:
        """
        Check if document exists based on title, published_date, and doc_path.
        """
        # Convert doc_path list to JSON string for comparison
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

    # -------------------- HTML STORAGE --------------------
    def save_document_html(self, regulation_id: int, html_content: str):
        """
        Save HTML content to the regulations table.
        """
        query = """
            UPDATE regulations 
            SET document_html = ?, updated_at = SYSDATETIMEOFFSET()
            WHERE id = ?
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (html_content, regulation_id))
                conn.commit()
                logger.info(f"HTML content saved for regulation ID: {regulation_id}")
        except Exception as e:
            logger.error(f"Failed to save HTML content: {e}")
            raise

    # -------------------- LOGGING --------------------
    def _log_processing(self, regulation_id, step, status, message, details=None):
        """
        Log processing steps to processinglogs table.
        """
        # Convert details dict to JSON string if provided
        details_json = json.dumps(details) if details else None

        query = """
            INSERT INTO processinglogs (
                regulation_id, step, status, message, details
            )
            VALUES (?, ?, ?, ?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    query,
                    (regulation_id, step, status, message, details_json)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to write processing log: {e}")

    # -------------------- UTILITY METHODS --------------------
    def get_regulation_by_id(self, regulation_id: int) -> dict:
        """
        Retrieve a regulation by ID.
        Returns a dictionary with all fields.
        """
        query = """
            SELECT 
                id, regulator, source_system, category, title, 
                document_url, doc_path, published_date, reference_no,
                department, year, source_page_url,
                CAST(extra_meta AS NVARCHAR(MAX)) as extra_meta,
                compliancecategory_id,
                CAST(created_at AS DATETIME2) as created_at,
                CAST(updated_at AS DATETIME2) as updated_at,
                document_html
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
                    # Parse JSON fields
                    if result.get('doc_path'):
                        result['doc_path'] = json.loads(result['doc_path'])
                    if result.get('department'):
                        try:
                            result['department'] = json.loads(result['department'])
                        except:
                            pass  # Keep as string if not JSON
                    if result.get('extra_meta'):
                        result['extra_meta'] = json.loads(result['extra_meta'])
                    return result
                return None
        except Exception as e:
            logger.error(f"Failed to get regulation by ID: {e}")
            return None

    def update_regulation(self, regulation_id: int, **kwargs):
        """
        Update specific fields of a regulation.
        """
        if not kwargs:
            return

        # Handle JSON fields
        if 'doc_path' in kwargs and isinstance(kwargs['doc_path'], list):
            kwargs['doc_path'] = json.dumps(kwargs['doc_path'])
        if 'extra_meta' in kwargs and isinstance(kwargs['extra_meta'], dict):
            kwargs['extra_meta'] = json.dumps(kwargs['extra_meta'])

        set_clause = ", ".join([f"{key} = ?" for key in kwargs.keys()])
        query = f"""
            UPDATE regulations 
            SET {set_clause}, updated_at = SYSDATETIMEOFFSET()
            WHERE id = ?
        """

        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (*kwargs.values(), regulation_id))
                conn.commit()
                logger.info(f"Updated regulation ID {regulation_id}: {list(kwargs.keys())}")
        except Exception as e:
            logger.error(f"Failed to update regulation: {e}")
            raise

    def save_metadata(
            self,
            document: RegulatoryDocument,
            ocr_text: str,
            classification: str
    ):
        """
        Save OCR text and classification metadata for the latest regulation version.
        MSSQL equivalent of Postgres implementation.
        """
        reg_id = document.id or self._get_regulation_id(document)
        if not reg_id:
            return

        latest = self._get_latest_version(reg_id)
        if not latest:
            return

        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE regulation_versions
                    SET ocr_text = ?, updated_at = SYSDATETIMEOFFSET()
                    WHERE id = ?
                    """,
                    (ocr_text, latest["id"])
                )
                conn.commit()

            self._log_processing(
                regulation_id=reg_id,
                version_id=latest["id"],
                step="classification",
                status="DONE",
                message=classification
            )

        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
            raise

    def store_compliance_analysis(self, regulation_id: int, analysis_data: dict):
        """
        Store LLM compliance analysis results in the compliance_analysis table.

        Args:
            regulation_id: The ID of the regulation
            analysis_data: Dictionary containing the LLM analysis results
        """
        analysis_json = json.dumps(analysis_data, ensure_ascii=False)

        # Check if analysis already exists for this regulation
        check_query = "SELECT id FROM compliance_analysis WHERE regulation_id = ?"

        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(check_query, (regulation_id,))
                existing = cursor.fetchone()

                if existing:
                    # Update existing analysis
                    update_query = """
                        UPDATE compliance_analysis
                        SET analysis_json = ?,
                            updated_at = SYSDATETIMEOFFSET()
                        WHERE regulation_id = ?
                    """
                    cursor.execute(update_query, (analysis_json, regulation_id))
                    logger.info(f"Updated compliance analysis for regulation {regulation_id}")
                else:
                    # Insert new analysis
                    insert_query = """
                        INSERT INTO compliance_analysis (regulation_id, analysis_json)
                        VALUES (?, ?)
                    """
                    cursor.execute(insert_query, (regulation_id, analysis_json))
                    logger.info(f"Inserted compliance analysis for regulation {regulation_id}")

                conn.commit()

        except Exception as e:
            logger.error(f"Failed to store compliance analysis for regulation {regulation_id}: {e}")
            raise

    # -------------------- RETRIEVE SINGLE ANALYSIS --------------------
    def get_compliance_analysis(self, regulation_id: int) -> dict:
        """
        Retrieve compliance analysis for a specific regulation.

        Args:
            regulation_id: The ID of the regulation

        Returns:
            Dictionary containing the analysis data, or None if not found
        """
        query = """
                SELECT id, regulation_id, analysis_json,
                       CAST(created_at AS DATETIME2) as created_at,
                       CAST(updated_at AS DATETIME2) as updated_at
                FROM compliance_analysis
                WHERE regulation_id = ?
            """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (regulation_id,))
                row = cursor.fetchone()
                if row:
                    result = {
                        "id": row[0],
                        "regulation_id": row[1],
                        "analysis_data": json.loads(row[2]),
                        "created_at": row[3],
                        "updated_at": row[4],
                    }
                    return result
                return None
        except Exception as e:
            logger.error(f"Failed to retrieve compliance analysis for regulation {regulation_id}: {e}")
            return None

    # -------------------- RETRIEVE ALL ANALYSES --------------------
    def get_all_compliance_analyses(self, limit: int = None, offset: int = 0) -> list:
        """
        Retrieve all compliance analyses with optional pagination.

        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip

        Returns:
            List of dictionaries containing analysis data
        """
        query = """
            SELECT id, regulation_id, analysis_json,
                   CAST(created_at AS DATETIME2) as created_at,
                   CAST(updated_at AS DATETIME2) as updated_at
            FROM compliance_analysis
            ORDER BY created_at DESC
        """

        if limit:
            query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()

                results = []
                for row in rows:
                    results.append({
                        "id": row[0],
                        "regulation_id": row[1],
                        "analysis_data": json.loads(row[2]),
                        "created_at": row[3],
                        "updated_at": row[4],
                    })

                return results
        except Exception as e:
            logger.error(f"Failed to retrieve compliance analyses: {e}")
            return []

        # -------------------- GAP ANALYSIS --------------------

    def create_gap_session(self, document_name: str, document_text: str) -> int:
            """
            Create a new gap analysis session for an uploaded document.
            Returns the new session ID.
            """
            query = """
                INSERT INTO gap_analysis_session (uploaded_document_name, uploaded_document_text)
                OUTPUT INSERTED.id
                VALUES (?, ?)
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, (document_name, document_text))
                    session_id = cursor.fetchone()[0]
                    conn.commit()
                    logger.info(f"Created gap analysis session ID: {session_id}")
                    return session_id
            except Exception as e:
                logger.error(f"Failed to create gap analysis session: {e}")
                raise

    def store_gap_results(self, session_id: int, regulation_id: int, results: list):
            """
            Store gap analysis results for a regulation within a session.

            Args:
                session_id:    ID of the gap_analysis_session row
                regulation_id: ID of the regulation being checked against
                results:       List of dicts with keys:
                               requirement_text, coverage_status, evidence_text, gap_description
            """
            query = """
                INSERT INTO gap_analysis (
                    session_id, regulation_id,
                    requirement_text, coverage_status,
                    evidence_text, gap_description
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    for result in results:
                        cursor.execute(query, (
                            session_id,
                            regulation_id,
                            result.get("requirement_text"),
                            result.get("coverage_status"),
                            result.get("evidence_text"),
                            result.get("gap_description")
                        ))
                    conn.commit()
                    logger.info(
                        f"Stored {len(results)} gap results for "
                        f"session {session_id}, regulation {regulation_id}"
                    )
            except Exception as e:
                logger.error(f"Failed to store gap results: {e}")
                raise

    def get_gap_results_by_session(self, session_id: int) -> dict:
            """
            Retrieve all gap analysis results for a session, grouped by regulation.

            Returns:
                {
                    "session_id": int,
                    "uploaded_document_name": str,
                    "created_at": str,
                    "regulations": [
                        {
                            "regulation_id": int,
                            "results": [
                                {
                                    "requirement_text": str,
                                    "coverage_status": str,
                                    "evidence_text": str or None,
                                    "gap_description": str or None
                                }
                            ],
                            "summary": {
                                "total": int,
                                "covered": int,
                                "partial": int,
                                "missing": int
                            }
                        }
                    ]
                }
            """
            session_query = """
                SELECT id, uploaded_document_name, created_at
                FROM gap_analysis_session
                WHERE id = ?
            """
            results_query = """
                SELECT regulation_id, requirement_text, coverage_status,
                       evidence_text, gap_description
                FROM gap_analysis
                WHERE session_id = ?
                ORDER BY regulation_id
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()

                    # Fetch session info
                    cursor.execute(session_query, (session_id,))
                    session_row = cursor.fetchone()
                    if not session_row:
                        logger.warning(f"No session found for ID: {session_id}")
                        return None

                    # Fetch all results
                    cursor.execute(results_query, (session_id,))
                    rows = cursor.fetchall()

                # Group by regulation_id
                grouped: Dict[int, list] = {}
                for row in rows:
                    reg_id = row[0]
                    if reg_id not in grouped:
                        grouped[reg_id] = []
                    grouped[reg_id].append({
                        "requirement_text": row[1],
                        "coverage_status": row[2],
                        "evidence_text": row[3],
                        "gap_description": row[4]
                    })

                # Build response
                regulations = []
                for reg_id, results in grouped.items():
                    summary = {
                        "total": len(results),
                        "covered": sum(1 for r in results if r["coverage_status"] == "covered"),
                        "partial": sum(1 for r in results if r["coverage_status"] == "partial"),
                        "missing": sum(1 for r in results if r["coverage_status"] == "missing")
                    }
                    regulations.append({
                        "regulation_id": reg_id,
                        "results": results,
                        "summary": summary
                    })

                return {
                    "session_id": session_row[0],
                    "uploaded_document_name": session_row[1],
                    "created_at": str(session_row[2]),
                    "regulations": regulations
                }

            except Exception as e:
                logger.error(f"Failed to retrieve gap results for session {session_id}: {e}")
                return None

            # -------------------- STEP 2: REQUIREMENT MATCHING --------------------

    def get_all_compliance_requirements(self) -> list:
        """
        Fetch all existing internal requirements from COMPLIANCE_REQUIREMENT table.
        Returns list of dicts with id, title, description.
        """
        query = """
                SELECT
                    COMPLIANCEREQUIREMENT_ID as id,
                    TITLE                    as title,
                    DESCRIPTION              as description
                FROM COMPLIANCE_REQUIREMENT
                WHERE TITLE IS NOT NULL
                  AND DESCRIPTION IS NOT NULL
            """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                results = []
                for row in rows:
                    results.append({
                        "id": row[0],
                        "title": row[1],
                        "description": row[2]
                    })
                logger.info(f"Fetched {len(results)} existing compliance requirements")
                return results
        except Exception as e:
            logger.error(f"Failed to fetch compliance requirements: {e}")
            return []

    def store_requirement_mappings(self, mappings: list):
        """
        Store requirement matching results in sama_requirement_mapping table.

        Args:
            mappings: List of dicts with keys:
                      regulation_id, extracted_requirement_text,
                      matched_requirement_id, match_status, match_explanation
        """
        query = """
                INSERT INTO sama_requirement_mapping (
                    regulation_id,
                    extracted_requirement_text,
                    matched_requirement_id,
                    match_status,
                    match_explanation
                )
                VALUES (?, ?, ?, ?, ?)
            """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                for mapping in mappings:
                    cursor.execute(query, (
                        mapping["regulation_id"],
                        mapping["extracted_requirement_text"],
                        mapping.get("matched_requirement_id"),  # NULL if new
                        mapping["match_status"],
                        mapping.get("match_explanation")
                    ))
                conn.commit()
                logger.info(f"Stored {len(mappings)} requirement mappings")
        except Exception as e:
            logger.error(f"Failed to store requirement mappings: {e}")
            raise

    def flag_partially_matched_requirements(self, matched_requirement_ids: list):
        """
        Set IS_SUGGESTED = 1 on existing requirements that were partially matched,
        so compliance team knows they need to be reviewed and updated.

        Args:
            matched_requirement_ids: List of COMPLIANCEREQUIREMENT_IDs to flag
        """
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
                logger.info(
                    f"Flagged {len(matched_requirement_ids)} partially matched "
                    f"requirements with IS_SUGGESTED = 1"
                )
        except Exception as e:
            logger.error(f"Failed to flag partially matched requirements: {e}")
            raise

    def insert_new_suggested_requirement(self, requirement: dict) -> int:
        """
        Insert a brand new requirement into COMPLIANCE_REQUIREMENT as IS_SUGGESTED = 1.
        Used when matching verdict is 'new' — creates a suggested requirement for
        human review.

        Args:
            requirement: dict with keys:
                         title, description, ref_key (optional), ref_no (optional)

        Returns:
            New COMPLIANCEREQUIREMENT_ID
        """
        query = """
                INSERT INTO COMPLIANCE_REQUIREMENT (
                    TITLE,
                    DESCRIPTION,
                    REF_KEY,
                    REF_NO,
                    IS_SUGGESTED,
                    CREATEDON
                )
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

    def get_requirement_mappings_by_regulation(self, regulation_id: int) -> list:
        """
        Retrieve all matching results for a given regulation.
        Useful for API endpoint to show step 2 results.
        """
        query = """
                SELECT
                    m.id,
                    m.regulation_id,
                    m.extracted_requirement_text,
                    m.matched_requirement_id,
                    m.match_status,
                    m.match_explanation,
                    CAST(m.created_at AS DATETIME2) as created_at,
                    cr.TITLE       as matched_requirement_title,
                    cr.DESCRIPTION as matched_requirement_description
                FROM sama_requirement_mapping m
                LEFT JOIN COMPLIANCE_REQUIREMENT cr
                       ON m.matched_requirement_id = cr.COMPLIANCEREQUIREMENT_ID
                WHERE m.regulation_id = ?
                ORDER BY m.id
            """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (regulation_id,))
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description]

                results = []
                for row in rows:
                    results.append(dict(zip(columns, row)))

                return results
        except Exception as e:
            logger.error(f"Failed to get requirement mappings for regulation {regulation_id}: {e}")
            return []

        # ================================================================== #
        #  FETCH EXISTING DATA                                                 #
        # ================================================================== #

        def get_all_compliance_requirements(self) -> list:
            """Fetch all existing requirements for matching."""
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
                    return [{"id": r[0], "title": r[1], "description": r[2]} for r in rows]
            except Exception as e:
                logger.error(f"Failed to fetch compliance requirements: {e}")
                return []

    def get_all_demo_controls(self) -> list:
            """Fetch all existing controls for matching."""
            query = """
                SELECT
                    CONTROL_ID  as id,
                    TITLE       as title,
                    DESCRIPTION as description,
                    CONTROL_KEY as control_key
                FROM DEMO_CONTROL
                WHERE TITLE IS NOT NULL
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    return [{"id": r[0], "title": r[1], "description": r[2], "control_key": r[3]} for r in rows]
            except Exception as e:
                logger.error(f"Failed to fetch demo controls: {e}")
                return []

    def get_all_demo_kpis(self) -> list:
            """Fetch all existing KPIs for matching."""
            query = """
                SELECT
                    KISETUP_ID  as id,
                    TITLE       as title,
                    DESCRIPTION as description,
                    KISETUP_KEY as kisetup_key
                FROM DEMO_KPI
                WHERE TITLE IS NOT NULL
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    return [{"id": r[0], "title": r[1], "description": r[2], "kisetup_key": r[3]} for r in rows]
            except Exception as e:
                logger.error(f"Failed to fetch demo KPIs: {e}")
                return []

    def get_linked_controls_by_requirement(self) -> dict:
            """
            Returns dict: { compliancerequirement_id → [control_id, ...] }
            Used by matcher to know what's already linked so it doesn't duplicate.
            """
            query = """
                SELECT COMPLIANCEREQUIREMENT_ID, CONTROL_ID
                FROM DEMO_REQUIREMENT_CONTROL_LINK
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    result = {}
                    for req_id, ctrl_id in rows:
                        result.setdefault(req_id, []).append(ctrl_id)
                    return result
            except Exception as e:
                logger.error(f"Failed to fetch linked controls: {e}")
                return {}

    def get_linked_kpis_by_requirement(self) -> dict:
            """
            Returns dict: { compliancerequirement_id → [kisetup_id, ...] }
            Used by matcher to know what's already linked so it doesn't duplicate.
            """
            query = """
                SELECT COMPLIANCEREQUIREMENT_ID, KISETUP_ID
                FROM DEMO_REQUIREMENT_KPI_LINK
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    result = {}
                    for req_id, kpi_id in rows:
                        result.setdefault(req_id, []).append(kpi_id)
                    return result
            except Exception as e:
                logger.error(f"Failed to fetch linked KPIs: {e}")
                return {}

        # ================================================================== #
        #  STORE REQUIREMENT MAPPINGS                                          #
        # ================================================================== #

    def store_requirement_mappings(self, mappings: list):
            """Store requirement matching results in sama_requirement_mapping."""
            query = """
                INSERT INTO sama_requirement_mapping (
                    regulation_id, extracted_requirement_text,
                    matched_requirement_id, match_status, match_explanation
                ) VALUES (?, ?, ?, ?, ?)
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
                            m.get("match_explanation")
                        ))
                    conn.commit()
                    logger.info(f"Stored {len(mappings)} requirement mappings")
            except Exception as e:
                logger.error(f"Failed to store requirement mappings: {e}")
                raise

    def flag_partially_matched_requirements(self, matched_requirement_ids: list):
            """Set IS_SUGGESTED = 1 on partially matched existing requirements."""
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
            """Insert brand new AI-suggested requirement into COMPLIANCE_REQUIREMENT."""
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

        # ================================================================== #
        #  STORE CONTROL LINKS                                                 #
        # ================================================================== #

    def store_control_links(self, control_links: list):
            """
            Insert new rows into DEMO_REQUIREMENT_CONTROL_LINK.
            Used when an existing control is matched to a requirement but not yet linked.
            """
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

        # ================================================================== #
        #  STORE KPI LINKS                                                     #
        # ================================================================== #

    def store_kpi_links(self, kpi_links: list):
            """
            Insert new rows into DEMO_REQUIREMENT_KPI_LINK.
            Used when an existing KPI is matched to a requirement but not yet linked.
            """
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
        #  INSERT NEW SUGGESTED CONTROLS AND KPIs                             #
        # ================================================================== #

    def insert_new_suggested_control(self, control: dict) -> int:
            """
            Insert a brand new AI-suggested control into DEMO_CONTROL with IS_SUGGESTED=1.
            Returns new CONTROL_ID.
            """
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
            """
            Insert a brand new AI-suggested KPI into DEMO_KPI with IS_SUGGESTED=1.
            Returns new KISETUP_ID.
            """
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
        #  FETCH MAPPING RESULTS (for API response)                           #
        # ================================================================== #

    def get_requirement_mappings_by_regulation(self, regulation_id: int) -> list:
            """Fetch requirement matching results with matched requirement details."""
            query = """
                SELECT
                    m.id,
                    m.regulation_id,
                    m.extracted_requirement_text,
                    m.matched_requirement_id,
                    m.match_status,
                    m.match_explanation,
                    CAST(m.created_at AS DATETIME2) as created_at,
                    cr.TITLE       as matched_requirement_title,
                    cr.DESCRIPTION as matched_requirement_description
                FROM sama_requirement_mapping m
                LEFT JOIN COMPLIANCE_REQUIREMENT cr
                       ON m.matched_requirement_id = cr.COMPLIANCEREQUIREMENT_ID
                WHERE m.regulation_id = ?
                ORDER BY m.id
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, (regulation_id,))
                    rows = cursor.fetchall()
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in rows]
            except Exception as e:
                logger.error(f"Failed to get requirement mappings for regulation {regulation_id}: {e}")
                return []

    def get_control_links_by_regulation(self, regulation_id: int) -> list:
            """Fetch control links for a regulation with control details."""
            query = """
                SELECT
                    l.ID,
                    l.COMPLIANCEREQUIREMENT_ID,
                    l.CONTROL_ID,
                    l.MATCH_STATUS,
                    l.MATCH_EXPLANATION,
                    CAST(l.LINKED_ON AS DATETIME2) as LINKED_ON,
                    dc.TITLE        as control_title,
                    dc.DESCRIPTION  as control_description,
                    dc.CONTROL_KEY  as control_key,
                    dc.IS_SUGGESTED as is_suggested
                FROM DEMO_REQUIREMENT_CONTROL_LINK l
                LEFT JOIN DEMO_CONTROL dc ON l.CONTROL_ID = dc.CONTROL_ID
                WHERE l.REGULATION_ID = ?
                ORDER BY l.COMPLIANCEREQUIREMENT_ID, l.ID
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, (regulation_id,))
                    rows = cursor.fetchall()
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in rows]
            except Exception as e:
                logger.error(f"Failed to get control links for regulation {regulation_id}: {e}")
                return []

    def get_kpi_links_by_regulation(self, regulation_id: int) -> list:
            """Fetch KPI links for a regulation with KPI details."""
            query = """
                SELECT
                    l.LINKAGE_ID,
                    l.COMPLIANCEREQUIREMENT_ID,
                    l.KISETUP_ID,
                    l.MATCH_STATUS,
                    l.MATCH_EXPLANATION,
                    CAST(l.LINKED_ON AS DATETIME2) as LINKED_ON,
                    dk.TITLE        as kpi_title,
                    dk.DESCRIPTION  as kpi_description,
                    dk.KISETUP_KEY  as kisetup_key,
                    dk.FORMULA      as formula,
                    dk.IS_SUGGESTED as is_suggested
                FROM DEMO_REQUIREMENT_KPI_LINK l
                LEFT JOIN DEMO_KPI dk ON l.KISETUP_ID = dk.KISETUP_ID
                WHERE l.REGULATION_ID = ?
                ORDER BY l.COMPLIANCEREQUIREMENT_ID, l.LINKAGE_ID
            """
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, (regulation_id,))
                    rows = cursor.fetchall()
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in rows]
            except Exception as e:
                logger.error(f"Failed to get KPI links for regulation {regulation_id}: {e}")
                return []