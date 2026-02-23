import os
import logging
import gc
from processor.downloader import Downloader
from storage.mssql_repo import MSSQLRepository
from processor.html_fallback_engine import HTMLFallbackEngine
from typing import List
from utils.pdfco_utils import pdfco_pdf_to_html
from processor.LlmAnalyzer import LLMAnalyzer
from processor.requirement_matcher import RequirementMatcher
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
                 ocr_engine: HTMLFallbackEngine = None, llm_analyzer: LLMAnalyzer = None):
        self.crawler = crawler
        self.repo = repo
        self.downloader = downloader
        self.ocr_engine = ocr_engine
        self.llm_analyzer = LLMAnalyzer()
        self.requirement_matcher = RequirementMatcher()

    def create_robust_session(self):
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
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

    # ================================================================== #
    #  STEP 2: FULL REQUIREMENT + CONTROL + KPI MATCHING                  #
    # ================================================================== #

    def _run_requirement_matching(self, regulation_id: int, analysis_result: dict):
        """
        Cross-reference newly extracted requirements, controls, and KPIs
        against existing internal records.

        Non-blocking — failures are logged but don't break the main flow.
        """
        try:
            self.log(regulation_id, "requirement_matching", "STARTED",
                     "Starting requirement + control + KPI matching")

            # 1 - Get extracted requirements from LLM result
            extracted_requirements = analysis_result.get("requirements", [])
            if not extracted_requirements:
                logger.warning(f"No requirements to match for regulation {regulation_id}")
                self.log(regulation_id, "requirement_matching", "SKIPPED",
                         "No extracted requirements to match")
                return

            # 2 - Fetch all existing internal data
            existing_requirements = self.repo.get_all_compliance_requirements()
            existing_controls     = self.repo.get_all_demo_controls()
            existing_kpis         = self.repo.get_all_demo_kpis()

            # 3 - Fetch existing links so matcher knows what's already connected
            linked_controls_by_req = self.repo.get_linked_controls_by_requirement()
            linked_kpis_by_req     = self.repo.get_linked_kpis_by_requirement()

            # 4 - Run full matching
            match_results = self.requirement_matcher.match_requirements(
                regulation_id=regulation_id,
                extracted_requirements=extracted_requirements,
                existing_controls=existing_controls,
                existing_kpis=existing_kpis,
                existing_requirements=existing_requirements,
                linked_controls_by_req=linked_controls_by_req,
                linked_kpis_by_req=linked_kpis_by_req
            )

            requirement_mappings   = match_results["requirement_mappings"]
            control_links          = match_results["control_links"]
            kpi_links              = match_results["kpi_links"]
            new_controls_to_insert = match_results["new_controls_to_insert"]
            new_kpis_to_insert     = match_results["new_kpis_to_insert"]

            # 5 - Store requirement mappings
            if requirement_mappings:
                self.repo.store_requirement_mappings(requirement_mappings)

            # 6 - Flag partially matched requirements with IS_SUGGESTED = 1
            partially_matched_ids = [
                m["matched_requirement_id"]
                for m in requirement_mappings
                if m["match_status"] == "partially_matched"
                and m["matched_requirement_id"] is not None
            ]
            if partially_matched_ids:
                self.repo.flag_partially_matched_requirements(partially_matched_ids)

            # 7 - Insert new suggested requirements and link their controls/KPIs
            new_req_mappings = [m for m in requirement_mappings if m["match_status"] == "new"]
            for i, mapping in enumerate(new_req_mappings):
                try:
                    req_text = mapping["extracted_requirement_text"]
                    title    = req_text[:100].strip() + ("..." if len(req_text) > 100 else "")
                    new_req_id = self.repo.insert_new_suggested_requirement({
                        "title":       title,
                        "description": req_text,
                        "ref_key":     f"SAMA-AUTO-{regulation_id}-{i}",
                        "ref_no":      f"REG-{regulation_id}"
                    })
                    logger.info(f"Inserted new suggested requirement ID {new_req_id}")

                    # Update _req_id on orphaned new controls/KPIs that belong to this req
                    for ctrl in new_controls_to_insert:
                        if ctrl.get("_req_id") is None:
                            ctrl["_req_id"] = new_req_id
                    for kpi in new_kpis_to_insert:
                        if kpi.get("_req_id") is None:
                            kpi["_req_id"] = new_req_id

                except Exception as e:
                    logger.error(f"Failed to insert new suggested requirement: {e}")

            # 8 - Store new control links (existing controls newly linked to existing reqs)
            if control_links:
                self.repo.store_control_links(control_links)
                logger.info(f"Stored {len(control_links)} new control links")

            # 9 - Store new KPI links
            if kpi_links:
                self.repo.store_kpi_links(kpi_links)
                logger.info(f"Stored {len(kpi_links)} new KPI links")

            # 10 - Insert new suggested controls and link them
            for ctrl in new_controls_to_insert:
                try:
                    new_ctrl_id = self.repo.insert_new_suggested_control({
                        "title":       ctrl["title"],
                        "description": ctrl["description"],
                        "control_key": ctrl["control_key"]
                    })
                    req_id = ctrl.get("_req_id")
                    if req_id:
                        self.repo.store_control_links([{
                            "compliancerequirement_id": req_id,
                            "control_id":               new_ctrl_id,
                            "match_status":             "new",
                            "match_explanation":        ctrl.get("_explanation", ""),
                            "regulation_id":            regulation_id
                        }])
                    logger.info(f"Inserted new suggested control ID {new_ctrl_id}")
                except Exception as e:
                    logger.error(f"Failed to insert new suggested control: {e}")

            # 11 - Insert new suggested KPIs and link them
            for kpi in new_kpis_to_insert:
                try:
                    new_kpi_id = self.repo.insert_new_suggested_kpi({
                        "title":       kpi["title"],
                        "description": kpi["description"],
                        "kisetup_key": kpi["kisetup_key"],
                        "formula":     kpi.get("formula", "")
                    })
                    req_id = kpi.get("_req_id")
                    if req_id:
                        self.repo.store_kpi_links([{
                            "compliancerequirement_id": req_id,
                            "kisetup_id":               new_kpi_id,
                            "match_status":             "new",
                            "match_explanation":        kpi.get("_explanation", ""),
                            "regulation_id":            regulation_id
                        }])
                    logger.info(f"Inserted new suggested KPI ID {new_kpi_id}")
                except Exception as e:
                    logger.error(f"Failed to insert new suggested KPI: {e}")

            # Summary
            fully   = sum(1 for m in requirement_mappings if m["match_status"] == "fully_matched")
            partial = sum(1 for m in requirement_mappings if m["match_status"] == "partially_matched")
            new_r   = sum(1 for m in requirement_mappings if m["match_status"] == "new")

            self.log(
                regulation_id, "requirement_matching", "SUCCESS",
                f"Reqs: {fully} fully / {partial} partial / {new_r} new | "
                f"Ctrl links: {len(control_links)} | KPI links: {len(kpi_links)} | "
                f"New controls: {len(new_controls_to_insert)} | New KPIs: {len(new_kpis_to_insert)}"
            )

        except Exception as e:
            logger.error(f"Requirement matching failed for regulation {regulation_id}: {e}")
            self.log(regulation_id, "requirement_matching", "ERROR", str(e))
            # Non-blocking

    # ================================================================== #
    #  MAIN FLOW                                                           #
    # ================================================================== #

    def run_for_regulator(self, regulator_name: str):
        logger.warning(f"=== RUNNING REGULATOR: {regulator_name} ===")
        docs = self.crawler.fetch_documents()
        logger.warning(f"Scraped {len(docs)} documents from crawler")

        new_docs, existing_docs = self.filter_new_documents(docs)
        logger.warning(f"{len(new_docs)} new / {len(existing_docs)} existing")

        if not new_docs:
            logger.warning("No new documents to process. Exiting...")
            return

        for idx, doc in enumerate(new_docs, start=1):
            logger.info(f"Processing {idx}/{len(new_docs)}: {doc.title}")
            self._process_single_doc(idx, doc, regulator_name)
            gc.collect()

        logger.warning(f"Finished processing all {len(new_docs)} documents.")

    def filter_new_documents(self, all_documents: List):
        new_docs, existing_docs = [], []
        for doc in all_documents:
            if doc.published_date:
                exists = self.check_exists_in_db(doc.title, doc.published_date, getattr(doc, "doc_path", None))
                (existing_docs if exists else new_docs).append(doc)
                continue
            if getattr(doc, "category", "").lower() == "regulatory returns":
                exists = self.check_exists_in_db(doc.title, None, getattr(doc, "doc_path", None))
                (existing_docs if exists else new_docs).append(doc)
                continue
            if getattr(doc, "source_system", "").upper() == "DPC-CIRCULAR":
                exists = self.check_exists_in_db(doc.title, None, getattr(doc, "doc_path", None))
                (existing_docs if exists else new_docs).append(doc)
                continue
            logger.warning(f"Skipping {doc.title} (missing published_date)")
        return new_docs, existing_docs

    def _get_or_create_compliance_category(self, hierarchy: list) -> int:
        parent_id = None
        for title in hierarchy:
            folder_id = self.repo.get_folder_id(title, parent_id)
            parent_id = folder_id if folder_id else self.repo.insert_folder(title, parent_id)
        return parent_id

    def check_exists_in_db(self, title, published_date, doc_path) -> bool:
        try:
            return self.repo.document_exists(title, published_date, doc_path)
        except Exception as e:
            logger.error(f"Failed to check document existence: {e}")
            return False

    def _process_single_doc(self, idx, doc, regulator_name):
        logger.info(f"[{idx}] Starting: {doc.title}")

        try:
            if hasattr(doc, "doc_path") and isinstance(doc.doc_path, list):
                doc.compliancecategory_id = self._get_or_create_compliance_category(doc.doc_path)
            else:
                doc.compliancecategory_id = None
        except Exception as e:
            logger.error(f"Failed to assign compliance category: {e}")
            doc.compliancecategory_id = None

        if getattr(doc, "category", "").lower() == "regulatory returns":
            try:
                regulation_id = self.repo._insert_regulation(doc)
                doc.id = regulation_id
                self.log(regulation_id, "insert", "SUCCESS", "Regulatory Return inserted (no document)")
                return
            except Exception as e:
                logger.error(f"Failed to insert Regulatory Return: {e}")
                self.log(None, "insert", "ERROR", str(e), doc_url=getattr(doc, "document_url", None))
                return

        # ------------------------------------------------------------------ #
        #  SAMA FLOW                                                           #
        # ------------------------------------------------------------------ #
        if regulator_name.upper() == "SAMA":
            try:
                org_pdf_link = doc.extra_meta.get("org_pdf_link")
                text_content = None
                content_type = None

                if not org_pdf_link and getattr(doc, "document_html", None):
                    text_content = doc.document_html
                    content_type = "html"

                elif org_pdf_link:
                    try:
                        original_url = doc.document_url
                        doc.document_url = org_pdf_link
                        doc.file_type = "PDF"
                        file_path, _ = self.downloader.download(doc)
                        doc.document_url = original_url

                        text_content, metadata = OCRProcessor.extract_text_from_pdf_smart(pdf_path=file_path)
                        content_type = "pdf_text"

                        doc.extra_meta["org_pdf_text"] = text_content
                        doc.extra_meta["extraction_metadata"] = metadata

                        if os.path.exists(file_path):
                            os.remove(file_path)
                    except Exception as e:
                        logger.error(f"Smart extraction failed: {e}")
                        text_content = None
                        content_type = None

                if not text_content or len(text_content) < 100:
                    logger.error("Insufficient text extracted")
                    self.log(None, "validation", "ERROR", "Insufficient text content")
                    return

                regulation_id = self.repo._insert_regulation(doc)
                doc.id = regulation_id
                self.log(regulation_id, "insert", "SUCCESS", "SAMA document inserted")

                try:
                    self.log(regulation_id, "llm_analysis", "STARTED", f"Starting LLM analysis ({content_type})")

                    analysis_result = self.llm_analyzer.analyze_regulation(
                        content=text_content,
                        regulation_id=regulation_id,
                        document_title=doc.title,
                        content_type=content_type
                    )

                    self.repo.store_compliance_analysis(
                        regulation_id=regulation_id,
                        analysis_data=analysis_result
                    )

                    self.log(regulation_id, "llm_analysis", "SUCCESS",
                             f"Analysis complete: {len(analysis_result.get('requirements', []))} requirements")

                    # ✅ STEP 2: Full matching
                    self._run_requirement_matching(regulation_id, analysis_result)

                except Exception as e:
                    logger.error(f"LLM analysis failed for regulation {regulation_id}: {e}")
                    self.log(regulation_id, "llm_analysis", "ERROR", str(e))

                return

            except Exception as e:
                logger.error(f"Failed to process SAMA document: {e}")
                self.log(None, "insert", "ERROR", str(e))
                return

        # ------------------------------------------------------------------ #
        #  NORMAL FLOW                                                         #
        # ------------------------------------------------------------------ #
        try:
            file_path, _ = self.downloader.download(doc)
        except Exception as e:
            self.log(None, "download", "ERROR", str(e))
            return

        try:
            try:
                html_content = pdfco_pdf_to_html(file_path)
                doc.document_html = html_content
            except Exception as e:
                logger.error(f"PDF.co HTML conversion failed: {e}")
                doc.document_html = None

            try:
                text_content, metadata = OCRProcessor.extract_text_from_pdf_smart(file_path)
            except Exception as e:
                logger.error(f"Text extraction failed: {e}")
                text_content = None

            regulation_id = self.repo._insert_regulation(doc)
            doc.id = regulation_id
            self.log(regulation_id, "insert", "SUCCESS", "Document inserted")

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
                    self.log(regulation_id, "llm_analysis", "SUCCESS",
                             f"Analysis complete: {len(analysis_result.get('requirements', []))} requirements")

                    # ✅ STEP 2: Full matching
                    self._run_requirement_matching(regulation_id, analysis_result)

                except Exception as e:
                    logger.error(f"LLM analysis failed: {e}")
                    self.log(regulation_id, "llm_analysis", "ERROR", str(e))
            else:
                logger.warning("Insufficient text for LLM analysis")
                self.log(regulation_id, "llm_analysis", "ERROR", "Insufficient text")

        except Exception as e:
            logger.error(f"Failed to process document {doc.title}: {e}")
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
            gc.collect()