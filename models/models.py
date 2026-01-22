from dataclasses import dataclass, field
from typing import Optional, Dict


@dataclass
class RegulatoryDocument:
    """
    Standard regulatory document model for SECP/SBP.
    """

    # ---- Identity ----
    regulator: str
    source_system: str
    category: str

    # ---- Title / URLs ----
    title: str
    document_url: str
    urdu_url: Optional[str] = None

    # ---- Metadata ----
    published_date: Optional[str] = None
    reference_no: Optional[str] = None
    fingerprint: Optional[str] = None

    # ---- Folder / compliance category ----
    compliancecategory_id: Optional[int] = None  # links to COMPLIANCECATEGORY table
    doc_path: Optional[list] = None             # hierarchy for folder creation

    # ---- SBP Context / optional ----
    department: Optional[str] = None
    year: Optional[str] = None

    # ---- Source Page ----
    source_page_url: Optional[str] = None

    file_type: Optional[str] = None
    extra_meta: Dict = field(default_factory=dict)

    # ---- HTML content ----
    document_html: Optional[str] = None

    # ---- DB assigned ID ----
    id: Optional[int] = None
