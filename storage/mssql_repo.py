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
    def _log_processing(self, regulation_id, step, status, message, details=None, **kwargs):
        """
        Log processing steps to processinglogs table.
        Accepts extra kwargs like document_url, version_id etc. and ignores them.
        """
        # Convert details dict to JSON string if provided
        details_json = json.dumps(details) if details else None

        # Ignore extra kwargs like document_url, version_id
        # They might be passed by callers but aren't stored in this table

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
            SELECT id, regulation_id, analysis_json, created_at, updated_at
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
            SELECT id, regulation_id, analysis_json, created_at, updated_at
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