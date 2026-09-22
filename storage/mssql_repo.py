from typing import Dict, List, Optional, Tuple
import pyodbc
import json
import re
import time
from storage.repository import DocumentRepository
from models.models import RegulatoryDocument
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------- #
#  regulations.ref_key -- REG-{country}-{regulator}-{source}-{id}         #
#  e.g. REG-SAU-SAMA-RUL-104. {country} is ISO 3166-1 alpha-3, derived    #
#  from the regulator via utils.countries.country_code_for -- not a      #
#  separate input, so every caller already passing regulator/source/id   #
#  gets it for free.                                                     #
#                                                                          #
#  Module-level, not methods: needed by both _insert_regulation below     #
#  (new rows) and scripts/backfill_regulation_ref_keys.py (existing rows) #
#  -- one implementation, so the two can never compute it two different   #
#  ways.                                                                  #
# ---------------------------------------------------------------------- #

_ACRONYM_STOPWORDS = {"of", "the", "and", "for", "in", "on"}


def regulator_acronym(regulator: str) -> str:
    """SAMA from "Saudi Arabian Monetary Authority (SAMA)". Most regulator
    names in this table already end in a parenthesized short code -- use it
    verbatim when present. The few that don't ("Ministry of Education",
    "Saudi Exchange") fall back to an initialism of the significant words,
    stopwords dropped: "Ministry of Education" -> "ME"."""
    if not regulator:
        return "UNK"
    m = re.search(r'\(([A-Za-z0-9]+)\)\s*$', regulator.strip())
    if m:
        return m.group(1).upper()
    words = [w for w in re.findall(r"[A-Za-z]+", regulator) if w.lower() not in _ACRONYM_STOPWORDS]
    initials = "".join(w[0] for w in words).upper()
    return initials or "UNK"


def source_acronym(source_system: str, reg_acronym: str) -> str:
    """First 3 letters of source_system, with the regulator's own name
    stripped off the front first if it's there (checked against real data:
    SAMA's source_system is "SAMA RULEBOOK", CMA's is "CMA-RULES" -- space
    and hyphen both occur, hence the character class covering both). Most
    regulators (CBE in particular) have a source_system that never repeats
    the regulator name at all -- e.g. "Circulars", "Financial Stability" --
    so for those this is just "first 3 letters of the whole thing". Two
    different source_systems CAN reduce to the same 3 letters (CBE's
    "Financial Stability" and "Financial Technology" both give "FIN") --
    harmless, since the ref_key's own trailing {id} already guarantees
    uniqueness regardless; this segment is for readability, not identity."""
    s = (source_system or "").strip()
    if not s:
        return "GEN"
    stripped = re.sub(rf'^{re.escape(reg_acronym)}[\s\-_]*', '', s, flags=re.IGNORECASE) or s
    letters = re.sub(r'[^A-Za-z]', '', stripped)
    return (letters[:3] or "GEN").upper()


def compute_regulation_ref_key(regulator: str, source_system: str, regulation_id) -> str:
    from utils.countries import country_code_for
    country = country_code_for(regulator)
    reg = regulator_acronym(regulator)
    src = source_acronym(source_system, reg)
    return f"REG-{country}-{reg}-{src}-{regulation_id}"


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

    def _get_conn(self, retries: int = 4, delay: float = 8.0):
        # WINDOWS AUTH WHEN NO USERNAME IS SET.
        #
        # This used to interpolate UID/PWD unconditionally. With a local
        # SQLEXPRESS-style setup — where .env carries no MSSQL_USERNAME because
        # the connection is a Windows one — that produced the literal string
        # "UID=None;PWD=None;" and the driver rejected the login. The failure
        # names the user "None", which reads like a missing env var rather than
        # the wrong authentication mode.
        user = self.conn_params.get("username")
        pwd = self.conn_params.get("password")
        auth = (f"UID={user};PWD={pwd};" if user
                else "Trusted_Connection=yes;")
        conn_str = (
    f"DRIVER={self.conn_params['driver']};"
    f"SERVER={self.conn_params['server']};"
    f"DATABASE={self.conn_params['database']};"
    f"{auth}"
    f"TrustServerCertificate=yes;"
    f"ConnectRetryCount=3;ConnectRetryInterval=5;"
)
        last_exc = None
        for attempt in range(retries):
            try:
                return pyodbc.connect(conn_str, timeout=30)
            except Exception as e:
                last_exc = e
                if attempt < retries - 1:
                    logger.warning(f"DB connect attempt {attempt+1}/{retries} failed, retrying in {delay}s: {e}")
                    time.sleep(delay)
        raise last_exc

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

    def find_folder_in_subtree(self, title: str, ancestor_id: int) -> Optional[int]:
        """
        Search for a category with the given title anywhere in the subtree
        rooted at ancestor_id (inclusive). Returns the category_id if found,
        None otherwise.

        Used by _get_or_create_compliance_category when a doc_path segment is
        not found as a direct child of the current parent — e.g. a deletion
        notice page whose sidebar trail omits an intermediate folder level
        (like 'CBB Rulebook') that already exists in the tree.
        """
        query = """
            WITH tree AS (
                SELECT compliancecategory_id, title, parentid
                FROM compliancecategory
                WHERE compliancecategory_id = ?
                UNION ALL
                SELECT c.compliancecategory_id, c.title, c.parentid
                FROM compliancecategory c
                JOIN tree t ON c.parentid = t.compliancecategory_id
            )
            SELECT TOP 1 compliancecategory_id
            FROM tree
            WHERE title = ?
              AND compliancecategory_id != ?
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [ancestor_id, title, ancestor_id])
            row = cursor.fetchone()
            return int(row[0]) if row else None

    def regulation_exists_for_category(self, compliancecategory_id: int) -> bool:
        """True if a regulation already points to this exact category node.
        Used when resolving the leaf (final) segment of a doc_path: if a
        node with the same (title, parent_id) already has a different
        regulation attached, it must not be reused for another one -- doing
        so silently merges two distinct documents into a single tree slot,
        making one of them unreachable in the tree."""
        query = "SELECT TOP 1 1 FROM regulations WHERE compliancecategory_id = ?"
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, [compliancecategory_id])
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"regulation_exists_for_category check failed: {e}")
            return False
    def get_child_source_urls(self, parent_source_url: str) -> List[str]:
        """
        Return source_page_url of all CBB regulations whose compliancecategory
        node's parent is the same category as the regulation at parent_source_url.
        Used by the monitoring crawler to detect deleted children.
        """
        query = """
            SELECT r2.source_page_url
            FROM regulations r1
            JOIN compliancecategory c1
                ON r1.compliancecategory_id = c1.compliancecategory_id
            JOIN compliancecategory c2
                ON c2.parentid = c1.compliancecategory_id
            JOIN regulations r2
                ON r2.compliancecategory_id = c2.compliancecategory_id
            WHERE r1.source_page_url = ?
              AND r1.regulator = 'Central Bank of Bahrain'
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, [parent_source_url])
                return [row[0] for row in cursor.fetchall() if row[0]]
        except Exception as e:
            logger.error(f"get_child_source_urls failed: {e}")
            return []

    def mark_regulation_deleted(self, regulation_id: int) -> None:
        """
        Handle a CBB page that has been removed from the TOC:
          1. Mark all currently active versions as inactive (preserve history)
          2. Insert a new version with status='deleted' to record the deletion event
        """
        from datetime import date as _date
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()

                # Step 1: mark existing active versions as inactive
                cursor.execute(
                    """
                    UPDATE regulation_versions
                    SET status = 'inactive'
                    WHERE regulation_id = ? AND status = 'active'
                    """,
                    [regulation_id]
                )

                # Step 2: insert a 'deleted' marker version
                cursor.execute(
                    """
                    INSERT INTO regulation_versions
                        (regulation_id, regulator, content_html, content_text,
                         content_hash, updated_date, change_summary, status)
                    OUTPUT INSERTED.version_id
                    VALUES (?, 'Central Bank of Bahrain', NULL, NULL,
                            NULL, ?, 'Page removed from CBB TOC', 'deleted')
                    """,
                    [regulation_id, _date.today().isoformat()]
                )
                row = cursor.fetchone()
                version_id = int(row[0]) if row else None

                # Step 3: update regulations.status so API list queries can filter it
                cursor.execute(
                    "UPDATE regulations SET status = 'deleted' WHERE id = ?",
                    [regulation_id]
                )
                conn.commit()
                logger.info(
                    f"Marked regulation {regulation_id} as deleted "
                    f"(new version_id={version_id})"
                )
        except Exception as e:
            logger.error(f"mark_regulation_deleted failed for reg {regulation_id}: {e}")

    def mark_regulation_withdrawn(self, regulation_id: int, reason: str) -> None:
        """A regulator has withdrawn this document. Nothing calls this yet.

        `status = 'withdrawn'` and a marker version, never a DELETE: both repos'
        `find_regulations_by_source` already exclude the status, so the row leaves
        the completeness gate and every change sweep while staying readable.
        Raises rather than logging, because a half-applied withdrawal is worse
        than a failed one.
        """
        from datetime import date as _date
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT regulator FROM regulations WHERE id = ?",
                           [regulation_id])
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"no regulation {regulation_id} to withdraw")
            cursor.execute(
                """
                UPDATE regulation_versions
                SET status = 'inactive'
                WHERE regulation_id = ? AND status = 'active'
                """,
                [regulation_id])
            cursor.execute(
                """
                INSERT INTO regulation_versions
                    (regulation_id, regulator, content_html, content_text,
                     content_hash, updated_date, change_summary, status)
                OUTPUT INSERTED.version_id
                VALUES (?, ?, NULL, NULL, NULL, ?, ?, 'withdrawn')
                """,
                [regulation_id, row[0], _date.today().isoformat(),
                 str(reason or "")[:400]])
            version = cursor.fetchone()
            cursor.execute(
                "UPDATE regulations SET status = 'withdrawn', "
                "updated_at = SYSDATETIMEOFFSET() WHERE id = ?",
                [regulation_id])
            conn.commit()
        logger.warning("regulation %s withdrawn (version %s): %s", regulation_id,
                       int(version[0]) if version else None, reason)

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
        # `status` is the HUMAN review decision (active / reject) and it governs
        # whether a row is promoted into the main system, so an EMPTY status must
        # survive as empty — it means "nobody has reviewed this yet".
        #
        # `or "active"` used to coerce "" straight back to "active", which marked
        # every freshly crawled row as though a person had already approved it.
        # A crawler that sets no status at all still defaults to "active", which
        # is the pre-existing behaviour for every non-formfill path.
        doc_status = getattr(document, "status", None)
        doc_status = "active" if doc_status is None else doc_status

        # `content_hash` IS in this list, and has to be.
        #
        # It was missing until 2026-08-16, so a brand-new row was stored with a
        # NULL hash however carefully its crawler had computed one — the value
        # went onto the regulation_VERSION row and nowhere else. The next run
        # then read NULL from `regulations`, could not match it against anything,
        # and classified the document `modified`: two more version rows, a full
        # re-extraction and re-OCR, and only then was the hash written (by
        # `update_regulation` on the modified path). Every document cost one
        # phantom revision on its second sighting, and every ONBOARDING of a new
        # regulator paid it once per document.
        #
        # ExcelRepo never had the bug — it copies the whole document — so the
        # workbook showed a fingerprint the database did not have, and the two
        # repos disagreed about the same crawl.
        sql = """
            INSERT INTO regulations (
                regulator, source_system, category,
                title, document_url, doc_path,
                published_date, reference_no,
                department, year,
                source_page_url, extra_meta,
                compliancecategory_id, document_html,
                type, status, content_hash
            )
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    getattr(document, "content_hash", None) or None,
                ))
                reg_id = cursor.fetchone()[0]
                # ref_key needs the row's own id, which only exists after the
                # INSERT above -- same transaction, so a crash between the two
                # statements can never leave a regulation row with no ref_key.
                ref_key = compute_regulation_ref_key(
                    document.regulator, document.source_system, reg_id)
                cursor.execute(
                    "UPDATE regulations SET ref_key = ? WHERE id = ?", (ref_key, reg_id))
                conn.commit()

            document.id = reg_id
            logger.info(f"Inserted regulation ID: {reg_id} (type={doc_type}, ref_key={ref_key})")
            return reg_id
        except Exception as e:
            logger.error(f"Failed to insert regulation: {e}")
            raise

    def update_regulation(self, regulation_id: int, **kwargs):
        """
        updated_at defaults to SYSDATETIMEOFFSET() (unchanged behaviour).
        Pass it explicitly in kwargs to pin it to a specific instant instead --
        e.g. to make it match the created_at of a regulation_versions row
        written in the same modification, rather than the moment this
        particular statement happens to execute.
        """
        if not kwargs:
            return
        updated_at = kwargs.pop("updated_at", None)
        set_parts = [f"{k} = ?" for k in kwargs.keys()]
        values = list(kwargs.values())
        if updated_at is not None:
            set_parts.append("updated_at = ?")
            values.append(updated_at)
        else:
            set_parts.append("updated_at = SYSDATETIMEOFFSET()")
        values.append(regulation_id)
        query = f"""
            UPDATE regulations
            SET {", ".join(set_parts)}
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

    def document_exists_by_url(self, document_url: str, category: Optional[str] = None) -> bool:
        """Existence check by document_url -- used as a fallback for documents
        that have no published_date, so they don't get silently re-inserted
        on every crawl run. Scoped by category when provided, since some
        documents (e.g. SAMA) are intentionally cross-listed under more than
        one category for the same document_url -- a bare url check would
        wrongly treat the second category's copy as a duplicate."""
        if category is not None:
            query = "SELECT TOP 1 id FROM regulations WHERE document_url = ? AND category = ?"
            params = (document_url, category)
        else:
            query = "SELECT TOP 1 id FROM regulations WHERE document_url = ?"
            params = (document_url,)
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"document_url existence check failed: {e}")
            return False

    # ================================================================== #
    #  THE NEW ORCHESTRATOR'S CONTRACT                                     #
    #                                                                      #
    #  NewOrchestrator (dynamic_crawler/formfill/orch.py) needs five        #
    #  methods that ExcelRepo already implements. Two of them —            #
    #  find_by_identity and find_by_reference — are called WITHOUT a        #
    #  hasattr guard, so until now a run against this repo crashed on the   #
    #  first document. That is why the pipeline had only ever run against   #
    #  Excel.                                                              #
    # ================================================================== #

    @staticmethod
    def _norm_doc_path(v) -> str:
        """doc_path in one canonical form, whatever it was stored as.

        This column holds `json.dumps(list)` here, ExcelRepo writes
        `" | ".join(list)`, and classify_documents compares `" > ".join(list)`.
        A plain string compare matches none of the three, which is exactly the
        bug that made every document look `new` on every run.
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
    def _with_extra_meta(r: dict) -> dict:
        """extra_meta is JSON text in the column and a dict everywhere else.

        Both identity lookups omitted it, so the archive step read the old row's
        content_text as "" and every archived version was stored empty.
        """
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

    def find_by_identity(self, document_url: str, doc_path) -> Optional[dict]:
        """The identity classify_documents uses: (document_url, doc_path).

        Filtered on document_url in SQL, then matched on doc_path in python —
        the column is JSON text and the caller passes an arrow-joined string, so
        the comparison cannot be done in the WHERE clause without depending on
        both sides having been written by the same code.

        `source_page_url` is selected for the SAMA feed. Since 2026-08-24
        SAMA's document_url holds the PDF and the node url moved here, and
        the feed matches on the node url -- without this column every feed
        entry reads as a document the library does not hold, and
        monitor_sama answers that by running a discovery ingest.
        """
        if not document_url:
            return None
        want = self._norm_doc_path(doc_path)
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id, title, document_url, doc_path, content_hash, "
                    "compliancecategory_id, category, status, "
                    "CAST(extra_meta AS NVARCHAR(MAX)) as extra_meta "
                    "FROM regulations WHERE document_url = ?", (document_url,))
                cols = [c[0] for c in cursor.description]
                for row in cursor.fetchall():
                    r = dict(zip(cols, row))
                    if self._norm_doc_path(r.get("doc_path")) == want:
                        return self._with_extra_meta(r)
            return None
        except Exception as e:
            logger.error(f"find_by_identity failed: {e}")
            return None

    #: Columns a source YAML may key its identity on. Whitelisted because the
    #: names reach a WHERE clause; values are always parameterised.
    IDENTITY_COLUMNS = frozenset({
        "document_url", "doc_path", "reference_no", "title", "category",
        "source_page_url", "source_system", "regulator", "published_date",
    })

    def find_by_identity_fields(self, fields: dict) -> Optional[dict]:
        """Identity lookup on whichever columns the source config names.

        `doc_path` is JSON text written by our own code, so it is compared in
        python after the SQL narrows on everything else — same reason
        `find_by_identity` does.
        """
        fields = {k: v for k, v in (fields or {}).items() if v not in (None, "")}
        if not fields:
            return None
        # `extra_meta.<key>` is compared in PYTHON, exactly like doc_path, so it
        # never reaches the WHERE clause and does not need to be a column. The
        # multi-attachment identity uses it: a card with seven PDFs has no single
        # document_url, so its identity is doc_path plus the files it carries.
        meta_fields = {k: v for k, v in fields.items() if k.startswith("extra_meta.")}
        bad = set(fields) - self.IDENTITY_COLUMNS - set(meta_fields)
        if bad:
            raise ValueError(
                f"identity column(s) not allowed: {sorted(bad)}. "
                f"Allowed: {sorted(self.IDENTITY_COLUMNS)}, "
                f"or extra_meta.<key> (compared in python)")

        sql_fields = {k: v for k, v in fields.items()
                      if k != "doc_path" and k not in meta_fields}
        where = " AND ".join(f"{k} = ?" for k in sql_fields) or "1 = 1"
        want = self._norm_doc_path(fields["doc_path"]) if "doc_path" in fields else None
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id, title, document_url, doc_path, content_hash, "
                    "reference_no, compliancecategory_id, category, status, "
                    "CAST(extra_meta AS NVARCHAR(MAX)) as extra_meta "
                    f"FROM regulations WHERE {where}", list(sql_fields.values()))
                cols = [c[0] for c in cursor.description]
                from dynamic_crawler.changesignal import resolve_field
                for row in cursor.fetchall():
                    r = dict(zip(cols, row))
                    if want is not None and self._norm_doc_path(r.get("doc_path")) != want:
                        continue
                    # The extra_meta fields still have to match, or a
                    # doc_path-only match would return a SIBLING card — every
                    # multi-attachment row in a folder shares its doc_path, and
                    # the attachment set is the only thing separating them.
                    r = self._with_extra_meta(r)
                    if all(str(resolve_field(r, k) or "") == str(v)
                           for k, v in meta_fields.items()):
                        return r
            return None
        except ValueError:
            raise
        except Exception as e:
            logger.error(f"find_by_identity_fields failed: {e}")
            return None

    def find_regulations_by_source(self, source_system: str,
                                   regulator: Optional[str] = None) -> List[dict]:
        """Every live regulation this source stored, for the completeness gate.

        Two regulators publish under "Rules and Regulations" and two more under
        "Laws and Regulations", so `source_system` alone can return another
        regulator's documents — which then read as disappeared. Scope it whenever
        the regulator is known. extra_meta carries the source's identity fields
        and the last version token; a change sweep cannot read either without it.
        """
        if not source_system:
            return []
        where = "WHERE source_system = ? "
        params = [source_system]
        if regulator:
            where += "AND regulator = ? "
            params.append(regulator)
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id, title, document_url, source_page_url, doc_path, content_hash, "
                    "reference_no, regulator, source_system, category, status, "
                    "CAST(extra_meta AS NVARCHAR(MAX)) as extra_meta "
                    "FROM regulations "
                    + where +
                    "  AND (status IS NULL OR status <> 'withdrawn')",
                    params)
                cols = [c[0] for c in cursor.description]
                return [self._with_extra_meta(dict(zip(cols, row)))
                        for row in cursor.fetchall()]
        except Exception as e:
            # Raises rather than returning []: an empty list here reads as
            # "nothing disappeared" and would silently disarm the gate.
            logger.error(f"find_regulations_by_source failed: {e}")
            raise

    def find_by_reference(self, reference_no: str) -> Optional[dict]:
        """The tiebreak: the same reference number at a NEW url is the same
        document republished, not a new document plus a disappearance."""
        if not reference_no:
            return None
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT TOP 1 id, title, document_url, doc_path, content_hash, "
                    "compliancecategory_id, category, status "
                    "FROM regulations WHERE reference_no = ? ORDER BY id ASC",
                    (reference_no,))
                row = cursor.fetchone()
                if not row:
                    return None
                return dict(zip([c[0] for c in cursor.description], row))
        except Exception as e:
            logger.error(f"find_by_reference failed: {e}")
            return None

    def _ensure_run_history(self, cursor) -> None:
        """Create run_history on first use.

        Additive and idempotent — it holds one row per crawl run and nothing
        else reads it. The completeness gate compares this run's document count
        against the last good one, so without the table the gate has no
        baseline and can only catch the failures visible within a single run.
        """
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM sysobjects
                           WHERE name = 'run_history' AND xtype = 'U')
            CREATE TABLE run_history (
                run_id         INT IDENTITY(1,1) PRIMARY KEY,
                source         NVARCHAR(200) NOT NULL,
                row_count      INT           NOT NULL,
                inventory_hash NVARCHAR(64)  NULL,
                verdict        NVARCHAR(32)  NULL,
                problems       NVARCHAR(500) NULL,
                run_at         DATETIME      NOT NULL DEFAULT GETUTCDATE()
            )""")

    def record_run(self, source: str, row_count: int, inventory_hash: str,
                   verdict: str = "PASS", problems: str = "") -> None:
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                self._ensure_run_history(cursor)
                cursor.execute(
                    "INSERT INTO run_history (source, row_count, inventory_hash, "
                    "verdict, problems) VALUES (?, ?, ?, ?, ?)",
                    (source, int(row_count), inventory_hash or "",
                     verdict, (problems or "")[:500]))
                conn.commit()
        except Exception as e:
            logger.error(f"record_run failed: {e}")

    def last_good_run(self, source: str) -> Optional[dict]:
        """The most recent run this source is allowed to be compared against.

        PASS only. Comparing against a quarantined run would let one short crawl
        set the baseline for the next, and the gate would drift down with it.
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                self._ensure_run_history(cursor)
                cursor.execute(
                    "SELECT TOP 1 run_id, source, row_count, inventory_hash, "
                    "verdict, run_at FROM run_history "
                    "WHERE source = ? AND verdict = 'PASS' ORDER BY run_id DESC",
                    (source,))
                row = cursor.fetchone()
                if not row:
                    return None
                return dict(zip([c[0] for c in cursor.description], row))
        except Exception as e:
            logger.error(f"last_good_run failed: {e}")
            return None

    def counts(self) -> dict:
        """Row counts per table, for the run report."""
        out = {}
        for t in ("regulations", "compliancecategory", "regulation_versions",
                  "compliance_analysis", "requirement_mappings"):
            try:
                with self._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {t}")
                    out[t] = int(cursor.fetchone()[0])
            except Exception:
                out[t] = None
        return out

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

    def find_regulation_id_by_section_code(self, section_code: str) -> Optional[int]:
        """
        Find the oldest CBB regulation whose title starts with the exact section code
        (not a sub-section). Used as a URL-change fallback for leaf pages.

        Matches: 'LR-1A.1', 'LR-1A.1 General Matters', 'LR-1A.1 [Deleted...]'
        Excludes: 'LR-1A.1.1 Sub-section' (child)
        """
        import re as _re
        sc_pattern = _re.compile(
            r'^(' + _re.escape(section_code) + r')(\s|\[|$)',
            _re.IGNORECASE,
        )
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, title FROM regulations
                    WHERE regulator = 'Central Bank of Bahrain'
                      AND title LIKE ?
                    ORDER BY id ASC
                """, [section_code + '%'])
                for row in cursor.fetchall():
                    reg_id, title = row[0], (row[1] or "")
                    if sc_pattern.match(title.strip()):
                        return int(reg_id)
            return None
        except Exception as e:
            logger.error(f"find_regulation_id_by_section_code failed: {e}")
            return None

    def find_regulation_ids_by_section_code_prefix(self, section_code: str) -> List[int]:
        """
        Find all CBB regulations whose title begins with section_code (the section
        itself AND all sub-sections). Used to mark an entire section tree as deleted.

        Matches: 'LR-1A.1', 'LR-1A.1 Title', 'LR-1A.1.1 Sub', 'LR-1A.1.35 Sub'
        Excludes: 'LR-1A.10' (different section sharing the same prefix characters)
        """
        import re as _re
        sc_pattern = _re.compile(
            r'^' + _re.escape(section_code) + r'(\s|\[|\.|$)',
            _re.IGNORECASE,
        )
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, title FROM regulations
                    WHERE regulator = 'Central Bank of Bahrain'
                      AND title LIKE ?
                      AND status != 'deleted'
                    ORDER BY id ASC
                """, [section_code + '%'])
                return [
                    int(row[0])
                    for row in cursor.fetchall()
                    if sc_pattern.match((row[1] or "").strip())
                ]
        except Exception as e:
            logger.error(f"find_regulation_ids_by_section_code_prefix failed: {e}")
            return []
    def get_regulation_id_by_document_url(self, document_url: str,
                                          regulator: Optional[str] = None) -> Optional[int]:
        """The id behind a document_url, so a caller can compare content hashes.

        `document_exists_by_url` answers a similar question with a bool, which is
        all `filter_new_documents` needs. Change detection needs the id itself:
        without it there is no stored hash to compare against, and an amended
        document is indistinguishable from an unchanged one.

        Scoped by regulator when given. `get_regulation_id_by_source_url` above
        hardcodes CBB in its SQL; this one takes the regulator as an argument so
        the next caller does not have to add a third near-copy.

        Added for SIMAH change detection (crawler/simah_wrapper.py). Nothing else
        calls it, and no existing behaviour changes.
        """
        if not document_url:
            return None
        if regulator:
            query = ("SELECT TOP 1 id FROM regulations "
                     "WHERE document_url = ? AND regulator = ?")
            params = (document_url, regulator)
        else:
            query = "SELECT TOP 1 id FROM regulations WHERE document_url = ?"
            params = (document_url,)
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                row = cursor.fetchone()
                return int(row[0]) if row else None
        except Exception as e:
            logger.error(f"Failed to look up regulation by document_url: {e}")
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
                id, ref_key, regulator, source_system, category, title,
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
            SELECT CONVERT(varchar(30), MAX(created_at), 120)
            FROM regulations
            WHERE regulator = 'Central Bank of Bahrain'
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                row = cursor.fetchone()
                if row and row[0]:
                    from datetime import datetime
                    return datetime.fromisoformat(row[0]).date()
                return None
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
        regulator: str = "",
        content_html: str = "",
        content_text: str = "",
        content_hash: str = "",
        updated_date=None,
        change_summary: str = "",
        status: str = "active",
        created_at=None,
        **kw,
    ) -> int:
        """
        DEFAULTS MATCH ExcelRepo's, DELIBERATELY.

        These two repos are meant to be interchangeable — that is the whole
        reason a run can write a workbook and `promote` can replay it into the
        database. They were not: every argument here was REQUIRED while
        ExcelRepo's had defaults and a **kw that silently swallowed extras.

        So the orchestrator's own versioning worked perfectly against a workbook
        and failed against MSSQL with

            insert_regulation_version() missing 1 required positional argument:
            'regulator'

        on every document. Measured 2026-08-16 on AML: 11 of 11 failed, and the
        run still reported PASS because each failure was caught per-document.
        Nobody had noticed because until that day every row reached the database
        through `promote`, never through the orchestrator writing directly.

        `**kw` is here for the same reason ExcelRepo has it: a caller written
        against one repo must not crash against the other.

        `created_at` is normally left to GETDATE() (unchanged default
        behaviour). Pass it explicitly when this version's timestamp must
        match another write in the same logical operation exactly -- e.g.
        regulations.updated_at on a modification -- rather than letting SQL
        Server stamp it independently at whatever instant this statement
        happens to execute.
        """
        """
        Insert a new version snapshot into regulation_versions.
        Returns the new version_id.
        """
        created_at_sql = "?" if created_at is not None else "GETDATE()"
        query = f"""
            INSERT INTO regulation_versions
                (regulation_id, regulator, content_html, content_text,
                 content_hash, updated_date, change_summary, status, created_at)
            OUTPUT INSERTED.version_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, {created_at_sql})
        """
        params = [
            regulation_id,
            regulator,
            content_html,
            content_text,
            content_hash,
            updated_date,
            change_summary,
            status,
        ]
        if created_at is not None:
            params.append(created_at)
        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
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

    # The copy half of the archive. `is_current` is written as 0 rather than
    # copied from the source row: these rows ARE the archive, so landing them
    # flagged current was wrong regardless of the delete step below.
    _ARCHIVE_ANALYSIS_SQL = """
        INSERT INTO compliance_analysis_versions
            (regulation_id, version_id,
             requirement_id, requirement_title,
             execution_category, criticality, obligation_type,
             stage1_json, stage2_json, stage3_json, stage4_md,
             analysis_json, schema_version, status, is_current,
             created_at)
        SELECT
            regulation_id, ?,
            requirement_id, requirement_title,
            execution_category, criticality, obligation_type,
            stage1_json, stage2_json, stage3_json, stage4_md,
            analysis_json, schema_version, 'inactive', 0,
            GETDATE()
        FROM compliance_analysis
        WHERE regulation_id = ?
          AND is_current = 1
    """

    # The delete half. MUST run after the copy: the SELECT above is scoped by
    # `is_current = 1`, so deleting first would archive nothing. Once this
    # runs, the row's only surviving copy is the one just written into
    # compliance_analysis_versions above -- which is the point: that table is
    # already the durable archive, so keeping a second, flagged-off copy in
    # compliance_analysis bought no extra recoverability, just an
    # ever-growing table (every reader filters `is_current = 1` already, so
    # the flagged-off copies were invisible dead weight, not a safety net
    # anyone could actually query).
    _DELETE_ANALYSIS_SQL = """
        DELETE FROM compliance_analysis
        WHERE regulation_id = ?
          AND is_current = 1
    """

    def _archive_analysis_stmts(self, cursor, regulation_id: int,
                                version_id: int) -> Tuple[int, int]:
        """Issue both archive statements on an open cursor. Returns
        (archived, deleted).

        Split out from the public method so the ORDER and SCOPE of the two
        statements can be tested without a database — see
        tests/test_stage_a.py. Those are the two things that were wrong.
        """
        cursor.execute(self._ARCHIVE_ANALYSIS_SQL, [version_id, regulation_id])
        archived = cursor.rowcount
        cursor.execute(self._DELETE_ANALYSIS_SQL, [regulation_id])
        deleted = cursor.rowcount
        return archived, deleted

    def archive_current_analysis(self, regulation_id: int, version_id: int) -> int:
        """Move the current compliance_analysis rows into
        compliance_analysis_versions. Returns the count archived.

        THE BUG THIS FIXES. This method used to run the INSERT ... SELECT alone.
        Copying without retiring left the old rows with `is_current = 1`, and
        every reader of this table filters on exactly that flag
        (`get_compliance_analysis`, and apis/pipeline_api.py). So after an
        update a regulation returned BOTH its old and its new requirement set as
        live, and the next update archived both again — the archive table growing
        quadratically. `ExcelRepo.archive_current_analysis` retires correctly,
        which is why every preview run looked clean while production doubled.

        DELETE, not `is_current = 0`. An earlier version of this fix flagged
        the old rows off instead of removing them, reasoning that it kept them
        "recoverable if an archive turns out to have been wrong." That
        reasoning didn't hold up: the row is already durably copied into
        compliance_analysis_versions by the INSERT ... SELECT above, before
        this DELETE ever runs, so nothing is lost by removing it here too.
        Flagging off instead of deleting just left compliance_analysis
        accumulating one full row-set per past version forever, all of it
        invisible to every reader (they all filter `is_current = 1`) — a
        silently growing table with no one able to query the rows it grew by.

        Both statements share ONE transaction, so a crash between them leaves
        neither applied — the archive and the delete cannot diverge.

        Safe to re-run: a second call finds no `is_current = 1` rows, archives 0
        and deletes 0. That is what makes it retryable after a partial failure.
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            archived, deleted = self._archive_analysis_stmts(
                cursor, regulation_id, version_id)
            conn.commit()

        if archived != deleted:
            # Not raised: the transaction already committed and the rows are
            # consistent with each other. This means rowcount reporting differed
            # from what we expect, which is worth investigating but not worth
            # failing an ingestion run over.
            logger.error(
                f"archive_current_analysis count mismatch for regulation "
                f"{regulation_id}: archived {archived}, deleted {deleted}")
        else:
            logger.info(
                f"Archived {archived} analysis row(s) for regulation "
                f"{regulation_id} into compliance_analysis_versions "
                f"(version_id={version_id})")
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

    # Clearing is scoped to (regulation_id, version_id) rather than to the
    # regulation alone. For CBB each content version keeps its own mapping set,
    # so clearing everything for the regulation would destroy real history. For
    # every other regulator version_id is NULL, and those are exactly the rows
    # that used to accumulate one full set per re-analysis.
    _CLEAR_MAPPINGS_NULL_VERSION_SQL = (
        "DELETE FROM sama_requirement_mapping "
        "WHERE regulation_id = ? AND version_id IS NULL")
    _CLEAR_MAPPINGS_SQL = (
        "DELETE FROM sama_requirement_mapping "
        "WHERE regulation_id = ? AND version_id = ?")

    def store_requirement_mappings(self, mappings: list, version_id: Optional[int] = None):
        """Replace this regulation+version's mappings instead of appending.

        THE BUG THIS FIXES. This was a plain INSERT loop with no cleanup, so
        re-analysing a regulation appended a second full set of mappings. For CBB
        `version_id` at least told the sets apart; for every other regulator it is
        NULL, so old and new mappings were indistinguishable.

        Clear and insert share one transaction: a crash cannot leave the
        regulation with no mappings at all.
        """
        if not mappings:
            return

        # `match_confidence` is written only when the column exists, so this
        # works both before and after the migration that adds it. The matcher
        # flags a verdict `low` when two internal requirements were similarly
        # plausible -- those are genuine ties, not model noise, and a person
        # should see them rather than have one silently chosen. Detected once
        # per call rather than per row.
        has_conf = self._mapping_has_confidence()
        # `version_id` is probed for the SAME reason as match_confidence: not
        # every database has it. The production instance
        # (regulatory_monitoring) carries the 7-column original --
        # id, regulation_id, extracted_requirement_text, matched_requirement_id,
        # match_status, match_explanation, created_at -- and both inserts above
        # named version_id unconditionally, so matching died there with
        #     [42S22] Invalid column name 'version_id'
        # AFTER the analysis had already been done and stored. The expensive
        # half succeeded and the cheap half threw.
        #
        # Degrading is right here rather than demanding a migration first: a
        # database without the column simply has no per-version mappings, which
        # is what it had before the column existed.
        has_version = self._mapping_has_version_id()
        cols = ["regulation_id", "extracted_requirement_text",
                "matched_requirement_id", "match_status", "match_explanation"]
        if has_version:
            cols.append("version_id")
        if has_conf:
            cols.append("match_confidence")
        insert_sql = (
            "INSERT INTO sama_requirement_mapping (" + ", ".join(cols) + ") "
            "VALUES (" + ", ".join("?" for _ in cols) + ")"
        )
        reg_ids = sorted({m["regulation_id"] for m in mappings})
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                replaced = 0
                for rid in reg_ids:
                    if not has_version:
                        # No version column: every row for this regulation is
                        # the one set there can be, so clear them all. Scoping
                        # on a column that does not exist is what threw.
                        cursor.execute(
                            "DELETE FROM sama_requirement_mapping "
                            "WHERE regulation_id = ?", (rid,))
                    elif version_id is None:
                        cursor.execute(self._CLEAR_MAPPINGS_NULL_VERSION_SQL, (rid,))
                    else:
                        cursor.execute(self._CLEAR_MAPPINGS_SQL, (rid, version_id))
                    replaced += max(cursor.rowcount, 0)
                for m in mappings:
                    values = [
                        m["regulation_id"],
                        m["extracted_requirement_text"],
                        m.get("matched_requirement_id"),
                        m["match_status"],
                        m.get("match_explanation"),
                    ]
                    if has_version:
                        values.append(version_id)
                    if has_conf:
                        values.append(m.get("match_confidence") or "high")
                    cursor.execute(insert_sql, tuple(values))
                conn.commit()
                logger.info(
                    f"Stored {len(mappings)} requirement mappings "
                    f"(version_id={version_id}, replaced {replaced} prior row(s))"
                )
        except Exception as e:
            logger.error(f"Failed to store requirement mappings: {e}")
            raise

    _HAS_VERSION_ID: Optional[bool] = None

    def _mapping_has_version_id(self) -> bool:
        """Whether sama_requirement_mapping carries `version_id`.

        Same shape and same caching as `_mapping_has_confidence` below, and the
        same safe direction on error: False falls back to the insert that works
        everywhere. Restart the API after adding the column -- the answer is
        cached for the life of the process.
        """
        if MSSQLRepository._HAS_VERSION_ID is None:
            try:
                with self._get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_NAME = 'sama_requirement_mapping' "
                        "AND COLUMN_NAME = 'version_id'")
                    MSSQLRepository._HAS_VERSION_ID = cur.fetchone() is not None
            except Exception as e:
                logger.warning("could not check for version_id: %s", e)
                MSSQLRepository._HAS_VERSION_ID = False
        return bool(MSSQLRepository._HAS_VERSION_ID)

    _HAS_MATCH_CONFIDENCE: Optional[bool] = None

    def _mapping_has_confidence(self) -> bool:
        """Whether sama_requirement_mapping carries `match_confidence` yet.

        Cached on the class after the first check: this is asked once per
        store_requirement_mappings call, the API builds several repo instances,
        and the schema does not change while the process runs. Returning False on
        any error is the safe direction -- it falls back to the six-column
        insert, which always works.

        OPERATIONAL NOTE: because the answer is cached for the life of the
        process, applying the migration that adds the column needs an API
        RESTART before confidence starts being written. Rows inserted in between
        simply have no confidence recorded; nothing fails.
        """
        if MSSQLRepository._HAS_MATCH_CONFIDENCE is None:
            try:
                with self._get_conn() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_NAME = 'sama_requirement_mapping' "
                        "AND COLUMN_NAME = 'match_confidence'")
                    MSSQLRepository._HAS_MATCH_CONFIDENCE = bool(cur.fetchone()[0])
            except Exception as e:
                logger.warning(f"Could not check for match_confidence column: {e}")
                MSSQLRepository._HAS_MATCH_CONFIDENCE = False
        return MSSQLRepository._HAS_MATCH_CONFIDENCE

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

    def find_requirement_by_ref_key(self, ref_key: str) -> Optional[int]:
        """COMPLIANCEREQUIREMENT_ID for a ref_key, or None.

        Lowest id wins, so repeated calls resolve to the same row even if
        duplicates already exist from before this check was added.
        """
        if not ref_key:
            return None
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT TOP 1 COMPLIANCEREQUIREMENT_ID FROM COMPLIANCE_REQUIREMENT "
                    "WHERE REF_KEY = ? ORDER BY COMPLIANCEREQUIREMENT_ID ASC",
                    (ref_key,))
                row = cursor.fetchone()
                return int(row[0]) if row else None
        except Exception as e:
            logger.error(f"find_requirement_by_ref_key failed: {e}")
            return None

    def insert_new_suggested_requirement(self, requirement: dict) -> int:
        """Insert a suggested requirement, or return the id already holding this
        ref_key.

        THE BUG THIS FIXES. Nothing checked for an existing row, so every
        re-analysis of a regulation inserted a fresh set of AUTO-… requirements
        into COMPLIANCE_REQUIREMENT.

        This is only correct because the caller's ref_key is derived from the
        requirement TEXT (see Orchestrator._run_requirement_matching). It used to
        be AUTO-<regulation_id>-<loop index>, and an index is not an identity: the
        LLM can emit a different number of requirements, in a different order, on
        the next run — so AUTO-42-0 could name different regulatory text each
        time, and returning the existing row for it would be wrong. With a
        content-derived key, a matching key means the same text, and reusing the
        row is the right answer rather than a collision to work around.
        """
        ref_key = requirement.get("ref_key", "SAMA-AUTO")
        existing = self.find_requirement_by_ref_key(ref_key)
        if existing is not None:
            logger.info(
                f"Suggested requirement {ref_key} already exists as {existing}; "
                f"reusing rather than inserting a duplicate")
            return existing

        query = """
            INSERT INTO COMPLIANCE_REQUIREMENT (TITLE, DESCRIPTION, REF_KEY, REF_NO, IS_SUGGESTED, CREATEDON)
            OUTPUT INSERTED.COMPLIANCEREQUIREMENT_ID
            VALUES (?, ?, ?, ?, 1, GETDATE())
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (
                    # `or ""` throughout: the model emits JSON null for a field
                    # it cannot fill, and dict.get only substitutes its default
                    # for an ABSENT key, not a present-but-null one.
                    (requirement.get("title") or "")[:500],
                    requirement.get("description") or "",
                    ref_key,
                    requirement.get("ref_no") or ""
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
                    (control.get("title") or "")[:500],
                    control.get("description") or "",
                    control.get("control_key")
                        or f"SAMA-AUTO-CTRL-{(control.get('title') or '')[:20]}"
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
                    (kpi.get("title") or "")[:500],
                    kpi.get("description") or "",
                    kpi.get("kisetup_key")
                        or f"SAMA-AUTO-KPI-{(kpi.get('title') or '')[:20]}",
                    kpi.get("formula") or ""
                ))
                new_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Inserted new suggested KPI ID: {new_id}")
                return new_id
        except Exception as e:
            logger.error(f"Failed to insert new suggested KPI: {e}")
            raise

    # ================================================================== #
    #  GAP ANALYSIS                                                        #
    # ================================================================== #
    #
    # `gap_analysis` and `gap_analysis_session` already existed in the schema
    # with no repository methods pointed at them — apis/pipeline_api.py called
    # `repo.create_gap_session`, `repo.store_gap_results` and
    # `repo.get_gap_results_by_session`, none of which existed, so every
    # /gap-analysis/* endpoint 500'd on the first line that touched the repo.
    #
    # `gap_analysis` only carries (regulation_id, requirement_text,
    # coverage_status, evidence_text, gap_description, session_id) — no
    # obligation_id/requirement_id/criticality/etc. That matches what
    # `_run_gap_for_regulation_v2` in pipeline_api.py actually hands to
    # `store_gap_results`: the RAW `gap_analyzer.analyze_gaps()` output, before
    # it enriches each result with obligation/requirement metadata pulled live
    # from `compliance_analysis` for that single response. So a session replayed
    # later via GET /gap-analysis/session/{id} legitimately comes back without
    # that enrichment — it was never persisted, by the original design, not by
    # an omission here.

    def create_gap_session(self, uploaded_document_name: str,
                            uploaded_document_text: str) -> int:
        """Start a gap-analysis session for one uploaded document, return its id."""
        query = """
            INSERT INTO gap_analysis_session
                (uploaded_document_name, uploaded_document_text, created_at)
            OUTPUT INSERTED.id
            VALUES (?, ?, SYSDATETIMEOFFSET())
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (uploaded_document_name, uploaded_document_text))
                session_id = cursor.fetchone()[0]
                conn.commit()
                logger.info(f"Created gap session {session_id} for "
                            f"{uploaded_document_name!r}")
                return session_id
        except Exception as e:
            logger.error(f"Failed to create gap session: {e}")
            raise

    def store_gap_results(self, session_id: int, regulation_id: int,
                           results: list) -> None:
        """Persist one regulation's gap results under an existing session.

        `results` is `gap_analyzer.analyze_gaps()`'s raw output — each item
        carries both `obligation_text` and `requirement_text` (the analyzer's
        own backward-compat alias, same value); either name is accepted here
        since callers have used both historically.
        """
        if not results:
            return
        query = """
            INSERT INTO gap_analysis
                (regulation_id, requirement_text, coverage_status,
                 evidence_text, gap_description, session_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, SYSDATETIMEOFFSET())
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                for r in results:
                    cursor.execute(query, (
                        regulation_id,
                        r.get("requirement_text") or r.get("obligation_text") or "",
                        r.get("coverage_status", "missing"),
                        r.get("evidence_text"),
                        r.get("gap_description"),
                        session_id,
                    ))
                conn.commit()
                logger.info(f"Stored {len(results)} gap result(s) for session "
                            f"{session_id}, regulation {regulation_id}")
        except Exception as e:
            logger.error(f"Failed to store gap results: {e}")
            raise

    def get_gap_results_by_session(self, session_id: int) -> Optional[dict]:
        """Everything recorded under one gap-analysis session, grouped by
        regulation. `None` if the session id doesn't exist — the caller turns
        that into a 404 rather than an empty-but-real session."""
        query_session = """
            SELECT id, uploaded_document_name,
                   CAST(created_at AS DATETIME2) AS created_at
            FROM gap_analysis_session WHERE id = ?
        """
        query_results = """
            SELECT regulation_id, requirement_text, coverage_status,
                   evidence_text, gap_description
            FROM gap_analysis
            WHERE session_id = ?
            ORDER BY regulation_id, id
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query_session, (session_id,))
                session_row = cursor.fetchone()
                if not session_row:
                    return None

                cursor.execute(query_results, (session_id,))
                by_regulation: Dict[int, list] = {}
                for reg_id, req_text, status, evidence, gap_desc in cursor.fetchall():
                    by_regulation.setdefault(reg_id, []).append({
                        "obligation_text": req_text,
                        "coverage_status": status,
                        "evidence_text":   evidence,
                        "gap_description": gap_desc,
                    })

                regulations = []
                for reg_id, results_list in by_regulation.items():
                    counts = {"total": len(results_list), "covered": 0,
                              "partial": 0, "missing": 0}
                    for r in results_list:
                        status = r["coverage_status"]
                        if status in counts:
                            counts[status] += 1
                    regulations.append({
                        "regulation_id": reg_id,
                        "results": results_list,
                        "summary": counts,
                    })

                return {
                    "session_id": session_row[0],
                    "uploaded_document_name": session_row[1],
                    "regulations": regulations,
                }
        except Exception as e:
            logger.error(f"Failed to get gap results for session {session_id}: {e}")
            return None

    # ================================================================== #
    #  LOGGING                                                             #
    # ================================================================== #

    def _log_processing(self, regulation_id, step, status, message, details=None,
                        document_url=None, duration_ms=None):
        """`duration_ms` rides inside the existing `details` JSON column.

        Deliberately not a new column: this is diagnostic data, and an ALTER
        TABLE against production to hold a timing field is not a trade worth
        making. Query it with JSON_VALUE(details, '$.duration_ms').

        Nothing in the pipeline recorded how long a step took, so "the pipeline
        is slow" could only be guessed at. Collecting it now means the deferred
        optimisation work starts with history instead of from zero.
        """
        if duration_ms is not None:
            details = dict(details or {})
            details["duration_ms"] = int(duration_ms)
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
    #  REQUIREMENT / ACTIVITY LOOKUPS                                      #
    # ================================================================== #

    def get_requirement_types(self) -> List[str]:
        """Every RequirementType name, in display order.

        The analyzer prompt must never hardcode this list -- it is a lookup
        table specifically so the taxonomy can change without a code or
        prompt edit. Callers pass the returned list into the analyzer; the
        analyzer itself stays DB-free, same reasoning as every other LLM
        client in this codebase.
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM RequirementType ORDER BY requirement_type_id")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to fetch requirement types: {e}")
            raise

    def get_activity_types(self) -> List[str]:
        """Every ActivityType name, in display order. Same reasoning as
        get_requirement_types() -- passed into the analyzer, never hardcoded."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM ActivityType ORDER BY activity_type_id")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to fetch activity types: {e}")
            raise

    def get_requirement_type_id(self, name: str) -> Optional[int]:
        """Analyzer output only ever carries the TYPE NAME (it stays DB-free,
        same reasoning as get_requirement_types itself) -- this is the one
        place that turns it back into the FK id insert_requirement needs."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT requirement_type_id FROM RequirementType WHERE name = ?", (name,))
                row = cursor.fetchone()
                return int(row[0]) if row else None
        except Exception as e:
            logger.error(f"Failed to look up requirement_type_id for {name!r}: {e}")
            return None

    def get_activity_type_id(self, name: str) -> Optional[int]:
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT activity_type_id FROM ActivityType WHERE name = ?", (name,))
                row = cursor.fetchone()
                return int(row[0]) if row else None
        except Exception as e:
            logger.error(f"Failed to look up activity_type_id for {name!r}: {e}")
            return None

    def get_departments(self) -> List[str]:
        """Every Department name, in display order. Same reasoning as
        get_activity_types() -- activity_analyzer.py must pick a department
        from this list rather than inventing free text. See
        migrations/2026-09-08_add_department_lookup.sql."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM Department ORDER BY department_id")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to fetch departments: {e}")
            raise

    # ================================================================== #
    #  REQUIREMENT / ACTIVITY VERSIONING                                   #
    #  See migrations/2026-09-08_requirement_activity_spans.sql and        #
    #  processor/requirement_activity_sync.py for the diff logic that      #
    #  calls these.                                                        #
    #                                                                       #
    #  Requirement/Activity are permanent, content-addressed catalogs --   #
    #  one row per distinct ref_key, ever, never duplicated, never         #
    #  deleted. Version lifecycle lives entirely in RequirementSpan/       #
    #  ActivitySpan instead: a requirement/activity can have MORE than one #
    #  span (one row per contiguous stretch of being active), which is     #
    #  what lets content that was superseded reappear later without        #
    #  hitting ref_key's UNIQUE constraint or misrepresenting the gap in   #
    #  between as "always active" -- see the migration file's docstring    #
    #  for the full reasoning.                                             #
    # ================================================================== #

    def get_all_requirements_for_regulation(self, regulation_id: int) -> Dict[str, int]:
        """{ref_key: requirement_id} for EVERY requirement this regulation has
        ever had, active or not -- the "has this exact content ever existed
        before" check. Distinguishes a resurrected requirement (reuse the
        id, open a new span) from genuinely new content (insert a new row).
        Deliberately unfiltered by span status, unlike get_current_requirements
        below."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT ref_key, requirement_id FROM Requirement WHERE regulation_id = ?",
                    (regulation_id,))
                return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to fetch all requirements for regulation "
                        f"{regulation_id}: {e}")
            raise

    def get_current_requirements(self, regulation_id: int) -> Dict[str, int]:
        """{ref_key: requirement_id} for every requirement with an OPEN span
        under this regulation -- what the sync diff compares a fresh
        extraction's ref_keys against to decide "unchanged" vs "needs a
        write"."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT r.ref_key, r.requirement_id "
                    "FROM Requirement r "
                    "JOIN RequirementSpan s ON s.requirement_id = r.requirement_id "
                    "WHERE r.regulation_id = ? AND s.superseded_in_version_id IS NULL",
                    (regulation_id,))
                return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to fetch current requirements for regulation "
                        f"{regulation_id}: {e}")
            raise

    def insert_requirement(self, regulation_id: int, version_id: int, ref_key: str,
                           title: str, description: str, source_reference: str,
                           source_refs: list, requirement_type_name: str = "",
                           actor: str = "", nature: str = "", condition_text: str = "",
                           cross_references: Optional[list] = None,
                           disposition: str = "", disposition_reason: str = "") -> int:
        """Brand-new content: a Requirement row (permanent) plus its first
        RequirementSpan (opened at version_id), in one transaction -- a
        requirement must never exist without at least one span.

        actor/nature/condition_text/cross_references/disposition/
        disposition_reason are content, same category as title/description --
        see migrations/2026-09-09_add_requirement_disposition_fields.sql.
        All optional (default "") so existing callers that predate the
        OBL/COND/REG/DEF/INFO disposition work keep working unchanged."""
        requirement_type_id = (self.get_requirement_type_id(requirement_type_name)
                               if requirement_type_name else None)
        source_refs_json = json.dumps(source_refs, ensure_ascii=False) if source_refs else None
        cross_refs_json = (json.dumps(cross_references, ensure_ascii=False)
                           if cross_references else None)
        req_query = """
            INSERT INTO Requirement
                (regulation_id, ref_key, title, description, source_reference,
                 source_refs, requirement_type_id, actor, nature, condition_text,
                 cross_references, disposition, disposition_reason)
            OUTPUT INSERTED.requirement_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        span_query = """
            INSERT INTO RequirementSpan (requirement_id, introduced_in_version_id)
            VALUES (?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(req_query, (
                    regulation_id, ref_key, title, description, source_reference,
                    source_refs_json, requirement_type_id,
                    actor or None, nature or None, condition_text or None,
                    cross_refs_json, disposition or None, disposition_reason or None,
                ))
                requirement_id = int(cursor.fetchone()[0])
                cursor.execute(span_query, (requirement_id, version_id))
                conn.commit()
                return requirement_id
        except Exception as e:
            logger.error(f"Failed to insert requirement {ref_key!r}: {e}")
            raise

    def open_requirement_span(self, requirement_id: int, version_id: int) -> None:
        """Resurrected content: the Requirement row already exists (its
        ref_key matched something in get_all_requirements_for_regulation), so
        only a new span is needed -- no content write, no risk of colliding
        with the ref_key UNIQUE constraint on the row that's still there from
        its earlier, now-closed span."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO RequirementSpan (requirement_id, introduced_in_version_id) "
                    "VALUES (?, ?)",
                    (requirement_id, version_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to open span for requirement {requirement_id}: {e}")
            raise

    def mark_requirement_superseded(self, requirement_id: int, version_id: int) -> None:
        """Closes the ONE open span for this requirement --
        UQ_RequirementSpan_OneOpenSpan guarantees there is at most one row
        this can ever match."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE RequirementSpan SET superseded_in_version_id = ? "
                    "WHERE requirement_id = ? AND superseded_in_version_id IS NULL",
                    (version_id, requirement_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to mark requirement {requirement_id} superseded: {e}")
            raise

    def get_all_activities_for_requirement(self, requirement_id: int) -> Dict[str, int]:
        """{ref_key: activity_id} for EVERY activity this requirement has ever
        had, active or not -- the Activity-level equivalent of
        get_all_requirements_for_regulation, used the same way (resurrection
        vs brand-new)."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT ref_key, activity_id FROM Activity WHERE requirement_id = ?",
                    (requirement_id,))
                return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to fetch all activities for requirement "
                        f"{requirement_id}: {e}")
            raise

    def get_current_activities_for_requirement(self, requirement_id: int) -> Dict[str, int]:
        """{ref_key: activity_id} for open-span activities under this
        requirement -- the Activity-level equivalent of
        get_current_requirements, used by the sync diff to decide "unchanged"
        vs "needs a write" for one requirement's activities."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT a.ref_key, a.activity_id "
                    "FROM Activity a "
                    "JOIN ActivitySpan s ON s.activity_id = a.activity_id "
                    "WHERE a.requirement_id = ? AND s.superseded_in_version_id IS NULL",
                    (requirement_id,))
                return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"Failed to fetch current activities for requirement "
                        f"{requirement_id}: {e}")
            raise

    def get_activities_for_requirement(self, requirement_id: int) -> List[int]:
        """OPEN activity_ids under one requirement -- for cascading supersede
        when the requirement itself becomes superseded."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT a.activity_id "
                    "FROM Activity a "
                    "JOIN ActivitySpan s ON s.activity_id = a.activity_id "
                    "WHERE a.requirement_id = ? AND s.superseded_in_version_id IS NULL",
                    (requirement_id,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to fetch activities for requirement {requirement_id}: {e}")
            raise

    def insert_activity(self, requirement_id: int, version_id: int, ref_key: str,
                        title: str, description: str, suggested_department: str,
                        frequency: str, frequency_type: Optional[str], priority: str,
                        evidence_expected: list, suggested_activity_type: str = "") -> int:
        """Same split as insert_requirement: an Activity row (permanent) plus
        its first ActivitySpan, in one transaction.

        suggested_department / suggested_activity_type are plain free text --
        no FK, no lookup-table matching. See migrations/
        2026-09-14_drop_activity_type_fk.sql and processor/activity_analyzer.py's
        module docstring for why: the model suggests both itself, and neither
        is sent to it as a controlled list any more."""
        evidence_text = "; ".join(evidence_expected) if evidence_expected else None
        act_query = """
            INSERT INTO Activity
                (requirement_id, ref_key, title, description, suggested_department, frequency,
                 frequency_type, priority, evidence_expected, suggested_activity_type)
            OUTPUT INSERTED.activity_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        span_query = """
            INSERT INTO ActivitySpan (activity_id, introduced_in_version_id)
            VALUES (?, ?)
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(act_query, (
                    requirement_id, ref_key, title, description, suggested_department, frequency,
                    frequency_type, priority, evidence_text, suggested_activity_type,
                ))
                activity_id = int(cursor.fetchone()[0])
                cursor.execute(span_query, (activity_id, version_id))
                conn.commit()
                return activity_id
        except Exception as e:
            logger.error(f"Failed to insert activity {ref_key!r}: {e}")
            raise

    def open_activity_span(self, activity_id: int, version_id: int) -> None:
        """Resurrected activity -- same reasoning as open_requirement_span."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO ActivitySpan (activity_id, introduced_in_version_id) "
                    "VALUES (?, ?)",
                    (activity_id, version_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to open span for activity {activity_id}: {e}")
            raise

    def mark_activity_superseded(self, activity_id: int, version_id: int) -> None:
        """Closes the ONE open span for this activity --
        UQ_ActivitySpan_OneOpenSpan guarantees there is at most one row this
        can ever match."""
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE ActivitySpan SET superseded_in_version_id = ? "
                    "WHERE activity_id = ? AND superseded_in_version_id IS NULL",
                    (version_id, activity_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to mark activity {activity_id} superseded: {e}")
            raise

    def get_requirements_for_regulation(self, regulation_id: int,
                                        active_only: bool = True) -> List[dict]:
        """Full Requirement rows for one regulation, each carrying its span
        dates -- what GET /regulation/{id}/requirements returns."""
        query = """
            SELECT r.requirement_id, r.regulation_id, r.ref_key, r.title, r.description,
                   r.source_reference, r.source_refs, rt.name AS requirement_type,
                   r.actor, r.nature, r.condition_text, r.cross_references,
                   r.disposition, r.disposition_reason, r.status,
                   r.created_on, r.updated_on,
                   s.introduced_in_version_id, s.superseded_in_version_id
            FROM Requirement r
            JOIN RequirementSpan s ON s.requirement_id = r.requirement_id
            LEFT JOIN RequirementType rt ON rt.requirement_type_id = r.requirement_type_id
            WHERE r.regulation_id = ?
        """
        if active_only:
            query += " AND s.superseded_in_version_id IS NULL"
        query += " ORDER BY r.requirement_id"
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (regulation_id,))
                columns = [c[0] for c in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                for r in rows:
                    for key in ("source_refs", "cross_references"):
                        if r.get(key):
                            try:
                                r[key] = json.loads(r[key])
                            except Exception:
                                pass
                return rows
        except Exception as e:
            logger.error(f"Failed to fetch requirements for regulation {regulation_id}: {e}")
            raise

    def get_activities_for_requirement_full(self, requirement_id: int,
                                            active_only: bool = True) -> List[dict]:
        """Full Activity rows for one requirement -- the per-requirement
        drill-down, and what GET /regulation/{id}/requirements nests under
        each requirement it returns."""
        query = """
            SELECT a.activity_id, a.requirement_id, a.ref_key, a.title, a.description,
                   a.suggested_department, a.suggested_activity_type, a.frequency,
                   a.frequency_type, a.priority, a.evidence_expected, a.status,
                   a.created_on, a.updated_on,
                   s.introduced_in_version_id, s.superseded_in_version_id
            FROM Activity a
            JOIN ActivitySpan s ON s.activity_id = a.activity_id
            WHERE a.requirement_id = ?
        """
        if active_only:
            query += " AND s.superseded_in_version_id IS NULL"
        query += " ORDER BY a.activity_id"
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (requirement_id,))
                columns = [c[0] for c in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                for a in rows:
                    a["evidence_expected"] = (a["evidence_expected"].split("; ")
                                              if a.get("evidence_expected") else [])
                return rows
        except Exception as e:
            logger.error(f"Failed to fetch activities for requirement {requirement_id}: {e}")
            raise

    def get_activities_for_regulation(self, regulation_id: int,
                                      active_only: bool = True) -> List[dict]:
        """Every activity under every requirement of one regulation, flat --
        what GET /regulation/{id}/activities returns. Each row keeps its
        requirement's ref_key so the flat list can still be traced back to
        the requirement it belongs to."""
        query = """
            SELECT a.activity_id, a.requirement_id, r.ref_key AS requirement_ref_key,
                   a.ref_key, a.title, a.description, a.suggested_department,
                   a.suggested_activity_type, a.frequency, a.frequency_type, a.priority,
                   a.evidence_expected, a.status, a.created_on, a.updated_on,
                   s.introduced_in_version_id, s.superseded_in_version_id
            FROM Activity a
            JOIN ActivitySpan s ON s.activity_id = a.activity_id
            JOIN Requirement r ON r.requirement_id = a.requirement_id
            WHERE r.regulation_id = ?
        """
        if active_only:
            query += " AND s.superseded_in_version_id IS NULL"
        query += " ORDER BY a.requirement_id, a.activity_id"
        try:
            with self._get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (regulation_id,))
                columns = [c[0] for c in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
                for a in rows:
                    a["evidence_expected"] = (a["evidence_expected"].split("; ")
                                              if a.get("evidence_expected") else [])
                return rows
        except Exception as e:
            logger.error(f"Failed to fetch activities for regulation {regulation_id}: {e}")
            raise

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