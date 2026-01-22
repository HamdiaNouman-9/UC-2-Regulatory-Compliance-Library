import os
import logging
import gc
from processor.downloader import Downloader
from storage.mssql_repo import MSSQLRepository
from processor.html_fallback_engine import HTMLFallbackEngine
from typing import List

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
                 ocr_engine: HTMLFallbackEngine):
        self.crawler = crawler
        self.repo = repo
        self.downloader = downloader
        self.ocr_engine = ocr_engine

    def log(self, regulation_id, step, status, message):
        try:
            self.repo._log_processing(
                regulation_id=regulation_id,
                step=step,
                status=status,
                message=message
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
            logger.info(f"Checking document: {doc.title}, published_date={doc.published_date}")

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

            logger.warning(f"Skipping {doc.title} because published_date is missing")

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
                self.log(None, "insert", "ERROR", str(e))
                return

        # NORMAL FLOW (ALL OTHER CATEGORIES)

        try:
            file_path, _ = self.downloader.download(doc)
            logger.info(f"Downloaded file → {file_path}")
        except Exception as e:
            self.log(None, "download", "ERROR", str(e))
            logger.error(f"Download failed for {doc.title}: {e}")
            return

        try:
            regulation_id = self.repo._insert_regulation(doc)
            doc.id = regulation_id
            self.log(regulation_id, "insert", "SUCCESS", "Document inserted")
            logger.info(f"Document inserted → ID {regulation_id}")
        except Exception as e:
            logger.error(f"Failed to insert document: {e}")
            self.log(None, "insert", "ERROR", str(e))
            return

        del file_path
        gc.collect()
        logger.info(f"Finished processing document: {doc.title}")