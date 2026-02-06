import os
import logging
import gc
from processor.downloader import Downloader
from storage.mssql_repo import MSSQLRepository
from processor.html_fallback_engine import HTMLFallbackEngine
from typing import List
from utils.pdfco_utils import pdfco_pdf_to_html
from processor.LlmAnalyzer import LLMAnalyzer
from processor.Text_Extractor import OCRProcessor
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("orchestrator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, crawler, repo: MSSQLRepository, downloader: Downloader,
                 ocr_engine: HTMLFallbackEngine=None,llm_analyzer: LLMAnalyzer = None):
        self.crawler = crawler
        self.repo = repo
        self.downloader = downloader
        self.ocr_engine = ocr_engine
        self.llm_analyzer = LLMAnalyzer()


    def create_robust_session(self):
        """Create session with retry logic for network issues"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def log(self, regulation_id, step, status, message, doc_url=None):
        try:
            self.repo._log_processing(
                regulation_id=regulation_id,
                step=step,
                status=status,
                message=message,
                document_url=doc_url
            )
        except Exception as e:
            logger.error(f"Failed to write processing log: {e}")

    def run_for_regulator(self, regulator_name: str):
        logger.warning(f"=== RUNNING REGULATOR: {regulator_name} ===")

        docs = self.crawler.fetch_documents()
        logger.warning(f"Scraped {len(docs)} documents from crawler")

        new_docs, existing_docs = self.filter_new_documents(docs)
        logger.warning(f"{len(new_docs)} new documents to process, {len(existing_docs)} already exist in DB")

        if not new_docs:
            logger.warning("No new documents to process. Exiting...")
            return

        for idx, doc in enumerate(new_docs, start=1):
            logger.info(f"Processing document {idx}/{len(new_docs)}: {doc.title} ({doc.published_date})")
            self._process_single_doc(idx, doc, regulator_name)
            gc.collect()

        logger.warning(f"Finished processing all {len(new_docs)} documents.")

    def filter_new_documents(self, all_documents: List):
        new_docs, existing_docs = [], []

        for doc in all_documents:
            logger.info(
                f"Checking document: {doc.title}, "
                f"published_date={doc.published_date}, "
                f"regulator={getattr(doc, 'regulator', None)}"
            )

            if doc.published_date:
                exists = self.check_exists_in_db(
                    doc.title,
                    doc.published_date,
                    getattr(doc, "doc_path", None)
                )

                if exists:
                    existing_docs.append(doc)
                else:
                    new_docs.append(doc)
                continue

            if getattr(doc, "category", "").lower() == "regulatory returns":
                exists = self.check_exists_in_db(
                    doc.title,
                    None,
                    getattr(doc, "doc_path", None)
                )

                if exists:
                    existing_docs.append(doc)
                else:
                    new_docs.append(doc)
                continue

            if getattr(doc, "source_system", "").upper() == "DPC-CIRCULAR":
                logger.info(f"DPC document without published_date → allowed: {doc.title}")

                exists = self.check_exists_in_db(
                    doc.title,
                    None,
                    getattr(doc, "doc_path", None)
                )

                if exists:
                    existing_docs.append(doc)
                else:
                    new_docs.append(doc)
                continue

            logger.warning(
                f"Skipping {doc.title} "
                f"(missing published_date, regulator={doc.regulator})"
            )

        return new_docs, existing_docs

    def _get_or_create_compliance_category(self, hierarchy: list) -> int:
        logger.info(f"Creating/fetching compliance category for path: {' / '.join(hierarchy)}")
        parent_id = None
        for title in hierarchy:
            folder_id = self.repo.get_folder_id(title, parent_id)
            if folder_id:
                parent_id = folder_id
            else:
                parent_id = self.repo.insert_folder(title, parent_id)
        logger.info(f"Final compliance category ID: {parent_id}")
        return parent_id

    def check_exists_in_db(self, title: str, published_date: str, doc_path: list) -> bool:
        try:
            exists = self.repo.document_exists(title, published_date, doc_path)
            logger.info(f"Check exists in DB: {title} → {exists}")
            return exists
        except Exception as e:
            logger.error(f"Failed to check document existence: {e}")
            return False

    def _process_single_doc(self, idx, doc, regulator_name):
        logger.info(f"[{idx}] Starting processing: {doc.title}")

        # Build doc path
        try:
            if hasattr(doc, "doc_path") and isinstance(doc.doc_path, list):
                compliancecategory_id = self._get_or_create_compliance_category(doc.doc_path)
                doc.compliancecategory_id = compliancecategory_id
            else:
                doc.compliancecategory_id = None
        except Exception as e:
            logger.error(f"Failed to assign compliance category: {e}")
            doc.compliancecategory_id = None

        # REGULATORY RETURNS: INSERT WITHOUT DOWNLOAD
        if getattr(doc, "category", "").lower() == "regulatory returns":
            try:
                regulation_id = self.repo._insert_regulation(doc)
                doc.id = regulation_id
                self.log(
                    regulation_id,
                    "insert",
                    "SUCCESS",
                    "Regulatory Return inserted (no document)"
                )
                logger.info(
                    f"Regulatory Return inserted without document → ID {regulation_id}"
                )
                return
            except Exception as e:
                logger.error(f"Failed to insert Regulatory Return: {e}")
                self.log(None, "insert", "ERROR", str(e),doc_url=getattr(doc, "document_url", None))
                return
            # SAMA: INSERT DIRECTLY (NO DOWNLOAD/CONVERSION NEEDED)
        # SAMA: INSERT DIRECTLY (NO DOWNLOAD/CONVERSION NEEDED)
        if regulator_name.upper() == "SAMA":
            try:
                org_pdf_link = doc.extra_meta.get("org_pdf_link")
                text_content = None
                content_type = None

                # CASE 1: org_pdf_link missing BUT document_html exists
                if not org_pdf_link and getattr(doc, "document_html", None):
                    text_content = doc.document_html
                    content_type = "html"  # ✅ Mark as HTML
                    logger.info(
                        f"Using existing document_html for SAMA doc → {doc.title} "
                        f"({len(text_content)} chars)"
                    )

                # CASE 2: org_pdf_link exists - SMART EXTRACTION
                elif org_pdf_link:
                    try:
                        # Download PDF
                        original_url = doc.document_url
                        doc.document_url = org_pdf_link
                        doc.file_type = "PDF"

                        file_path, _ = self.downloader.download(doc)
                        logger.info(f"Downloaded SAMA PDF → {file_path}")

                        # Restore original URL
                        doc.document_url = original_url

                        # SMART EXTRACTION
                        self.log(None, "smart_extraction", "STARTED", "Starting smart PDF extraction")

                        text_content, metadata = OCRProcessor.extract_text_from_pdf_smart(
                            pdf_path=file_path
                        )
                        content_type = "pdf_text"  # ✅ Mark as pre-cleaned PDF text

                        # Store results
                        doc.extra_meta["org_pdf_text"] = text_content
                        doc.extra_meta["extraction_metadata"] = metadata

                        self.log(
                            None,
                            "smart_extraction",
                            "SUCCESS",
                            f"Extracted {metadata['good_pages']}/{metadata['total_pages']} pages "
                            f"({metadata['ocr_pages']} used OCR)"
                        )

                        logger.info(
                            f"Smart extraction complete:\n"
                            f"   Total pages: {metadata['total_pages']}\n"
                            f"   Good pages: {metadata['good_pages']}\n"
                            f"   Bad pages filtered: {metadata['bad_pages']}\n"
                            f"   OCR pages: {metadata['ocr_pages']}\n"
                            f"   Final text: {len(text_content)} chars"
                        )

                        # Cleanup PDF file
                        if os.path.exists(file_path):
                            os.remove(file_path)

                    except Exception as e:
                        logger.error(f"Smart extraction failed: {e}")
                        self.log(None, "smart_extraction", "ERROR", str(e))
                        text_content = None
                        content_type = None

                else:
                    logger.warning("No org_pdf_link or document_html for SAMA document")

                # Validate extracted content
                if not text_content or len(text_content) < 100:  # Lowered threshold for HTML
                    logger.error(
                        f"Insufficient text extracted ({len(text_content) if text_content else 0} chars)"
                    )
                    self.log(
                        None,
                        "validation",
                        "ERROR",
                        "Insufficient text content after extraction"
                    )
                    return

                # Insert regulation into database
                regulation_id = self.repo._insert_regulation(doc)
                doc.id = regulation_id

                self.log(
                    regulation_id,
                    "insert",
                    "SUCCESS",
                    "SAMA document inserted"
                )
                logger.info(f"SAMA document inserted → ID {regulation_id}")

                # LLM Analysis with content type
                try:
                    self.log(
                        regulation_id,
                        "llm_analysis",
                        "STARTED",
                        f"Starting LLM analysis ({content_type})"
                    )

                    # ✅ Pass content with explicit type
                    analysis_result = self.llm_analyzer.analyze_regulation(
                        content=text_content,
                        regulation_id=regulation_id,
                        document_title=doc.title,
                        content_type=content_type  # ✅ NEW PARAMETER
                    )

                    self.repo.store_compliance_analysis(
                        regulation_id=regulation_id,
                        analysis_data=analysis_result
                    )

                    self.log(
                        regulation_id,
                        "llm_analysis",
                        "SUCCESS",
                        f"Analysis complete: {len(analysis_result.get('requirements', []))} requirements"
                    )

                    logger.info(
                        f"LLM analysis completed for regulation {regulation_id}"
                    )

                except Exception as e:
                    logger.error(
                        f"LLM analysis failed for regulation {regulation_id}: {e}"
                    )
                    self.log(
                        regulation_id,
                        "llm_analysis",
                        "ERROR",
                        str(e)
                    )

                return

            except Exception as e:
                logger.error(f"Failed to process SAMA document: {e}")
                self.log(None, "insert", "ERROR", str(e))
                return

        # NORMAL FLOW (ALL OTHER REGULATORS)
        try:
            # Download PDF
            file_path, _ = self.downloader.download(doc)
            logger.info(f"Downloaded file → {file_path}")
        except Exception as e:
            self.log(None, "download", "ERROR", str(e))
            logger.error(f"Download failed for {doc.title}: {e}")
            return

        try:
            # PDF.co HTML → store for UI
            try:
                html_content = pdfco_pdf_to_html(file_path)
                doc.document_html = html_content
                logger.info(f"PDF.co HTML generated for UI → {len(html_content)} chars")
            except Exception as e:
                logger.error(f"PDF.co HTML conversion failed: {e}")
                doc.document_html = None

            # Extract text for LLM
            try:
                text_content, metadata = OCRProcessor.extract_text_from_pdf_smart(file_path)
                logger.info(f"Extracted {len(text_content)} chars for LLM analysis")
            except Exception as e:
                logger.error(f"Text extraction failed: {e}")
                text_content = None

            # Insert document into DB
            regulation_id = self.repo._insert_regulation(doc)
            doc.id = regulation_id
            self.log(regulation_id, "insert", "SUCCESS", "Document inserted")
            logger.info(f"Document inserted → ID {regulation_id}")

            #  LLM analysis (only if text exists)
            if text_content and len(text_content) > 500:
                try:
                    self.log(regulation_id, "llm_analysis", "STARTED", "Starting LLM analysis")
                    analysis_result = self.llm_analyzer.analyze_regulation(
                        content=text_content,
                        regulation_id=regulation_id,
                        document_title=doc.title
                    )
                    self.repo.store_compliance_analysis(
                        regulation_id=regulation_id,
                        analysis_data=analysis_result
                    )
                    self.log(
                        regulation_id,
                        "llm_analysis",
                        "SUCCESS",
                        f"Analysis complete: {len(analysis_result.get('requirements', []))} requirements"
                    )
                    logger.info(f"LLM analysis completed for regulation {regulation_id}")
                except Exception as e:
                    logger.error(f"LLM analysis failed: {e}")
                    self.log(regulation_id, "llm_analysis", "ERROR", str(e))
            else:
                logger.warning("Insufficient text for LLM analysis")
                self.log(regulation_id, "llm_analysis", "ERROR", "Insufficient text for LLM")

        except Exception as e:
            logger.error(f"Failed to process document {doc.title}: {e}")

        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
            gc.collect()
