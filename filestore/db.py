import logging
import time
from typing import List, Optional

import pyodbc

logger = logging.getLogger(__name__)


class FileStoreDB:
    def __init__(self, conn_params: dict):
        self.conn_params = conn_params
        self._conn = None

    # ------------------------------------------------------------ connection

    def _connect(self, retries: int = 3, delay: float = 5.0):
        user = self.conn_params.get("username")
        pwd = self.conn_params.get("password")
        auth = f"UID={user};PWD={pwd};" if user else "Trusted_Connection=yes;"
        conn_str = (
            f"DRIVER={self.conn_params['driver']};"
            f"SERVER={self.conn_params['server']};"
            f"DATABASE={self.conn_params['database']};"
            f"{auth}TrustServerCertificate=yes;"
        )
        for attempt in range(retries):
            try:
                return pyodbc.connect(conn_str, timeout=30, autocommit=True)
            except pyodbc.Error as e:
                if attempt == retries - 1:
                    raise
                logger.warning(f"filestore DB connect {attempt + 1}/{retries} failed, retrying in {delay}s: {e}")
                time.sleep(delay)

    def _cursor(self):
        if self._conn is None:
            self._conn = self._connect()
        return self._conn.cursor()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------ urls

    def get_url(self, url_hash: str):
        return self._cursor().execute(
            "SELECT sha256, etag, last_modified FROM dbo.fs_url_seen WHERE url_hash = ?",
            url_hash).fetchone()

    def upsert_url(self, url_hash, url, sha256, etag, last_modified) -> None:
        cur = self._cursor()
        # In an UPDATE, SQL Server evaluates every right-hand side against the OLD
        # row, so the CASE compares the previous sha256 with the new one.
        cur.execute("""
            UPDATE dbo.fs_url_seen
               SET last_changed_at = CASE WHEN sha256 = ? THEN last_changed_at ELSE SYSUTCDATETIME() END,
                   sha256 = ?, etag = ?, last_modified = ?, last_checked_at = SYSUTCDATETIME()
             WHERE url_hash = ?""",
            sha256, sha256, etag, last_modified, url_hash)
        if cur.rowcount == 0:
            try:
                cur.execute("""
                    INSERT INTO dbo.fs_url_seen (url_hash, url, sha256, etag, last_modified)
                    VALUES (?, ?, ?, ?, ?)""",
                    url_hash, url, sha256, etag, last_modified)
            except pyodbc.IntegrityError:
                pass   # another worker inserted it first

    def touch_url(self, url_hash: str) -> None:
        self._cursor().execute(
            "UPDATE dbo.fs_url_seen SET last_checked_at = SYSUTCDATETIME() WHERE url_hash = ?",
            url_hash)

    # ------------------------------------------------------------ files

    def get_file(self, sha256: str):
        return self._cursor().execute(
            "SELECT sha256, size_bytes, content_type, extension, storage_path "
            "FROM dbo.fs_stored_file WHERE sha256 = ?", sha256).fetchone()

    def insert_file(self, sha256, size_bytes, content_type, extension, storage_path, first_url) -> bool:
        """True if this call stored it; False if it was already there."""
        try:
            self._cursor().execute("""
                INSERT INTO dbo.fs_stored_file
                    (sha256, size_bytes, content_type, extension, storage_path, first_url)
                VALUES (?, ?, ?, ?, ?, ?)""",
                sha256, size_bytes, content_type, extension, storage_path, first_url)
            return True
        except pyodbc.IntegrityError:
            return False

    # ------------------------------------------------------------ document links

    def link_document(self, document_key, url_hash, url, sha256, role) -> None:
        cur = self._cursor()
        cur.execute("""
            UPDATE dbo.fs_document_file
               SET sha256 = ?, url = ?, role = ?, updated_at = SYSUTCDATETIME()
             WHERE document_key = ? AND url_hash = ?""",
            sha256, url, role, document_key, url_hash)
        if cur.rowcount == 0:
            try:
                cur.execute("""
                    INSERT INTO dbo.fs_document_file (document_key, url_hash, url, sha256, role)
                    VALUES (?, ?, ?, ?, ?)""",
                    document_key, url_hash, url, sha256, role)
            except pyodbc.IntegrityError:
                pass

    def get_document_files(self, document_key: str) -> List:
        return self._cursor().execute("""
            SELECT df.url, df.sha256, df.role,
                   t.method, t.full_text, t.text_hash, t.page_count, t.low_quality
              FROM dbo.fs_document_file df
              LEFT JOIN dbo.fs_file_text t ON t.sha256 = df.sha256
             WHERE df.document_key = ?
             ORDER BY df.id""", document_key).fetchall()

    # ------------------------------------------------------------ text

    def get_text(self, sha256: str):
        return self._cursor().execute("""
            SELECT sha256, method, extractor_version, page_count, low_quality, text_hash, full_text
              FROM dbo.fs_file_text WHERE sha256 = ?""", sha256).fetchone()

    def save_text(self, sha256, method, extractor_version, page_count, ocr_pages,
                  low_quality, text_hash, full_text) -> None:
        cur = self._cursor()
        cur.execute("""
            UPDATE dbo.fs_file_text
               SET method = ?, extractor_version = ?, page_count = ?, ocr_pages = ?,
                   low_quality = ?, char_count = ?, text_hash = ?, full_text = ?,
                   created_at = SYSUTCDATETIME()
             WHERE sha256 = ?""",
            method, extractor_version, page_count, ocr_pages,
            low_quality, len(full_text), text_hash, full_text, sha256)
        if cur.rowcount == 0:
            try:
                cur.execute("""
                    INSERT INTO dbo.fs_file_text
                        (sha256, method, extractor_version, page_count, ocr_pages,
                         low_quality, char_count, text_hash, full_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    sha256, method, extractor_version, page_count, ocr_pages,
                    low_quality, len(full_text), text_hash, full_text)
            except pyodbc.IntegrityError:
                pass
