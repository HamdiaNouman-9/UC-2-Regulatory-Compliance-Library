import os
import hashlib
import logging
import gc
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from processor.downloader import Downloader
from storage.mssql_repo import MSSQLRepository
from processor.html_fallback_engine import HTMLFallbackEngine
from typing import List, Optional, Tuple
from processor.LlmAnalyzer import LLMAnalyzer
from processor.requirement_matcher import RequirementMatcher
from processor.Text_Extractor import OCRProcessor

# certifi's static CA bundle cannot always complete a real chain -- mc.gov.sa
# sends a leaf cert but not the intermediate, which certifi-based verification
# rejects outright (SSLCertVerificationError: unable to get local issuer
# certificate) even though the site is genuinely fine. Windows' own trust
# store does AIA chain-building and resolves it. truststore makes every
# ssl.SSLContext in this process (so every `requests` call too) use the OS
# store instead of certifi -- this is its normal, once-at-startup usage
# pattern, not a verification bypass: certificates are still fully validated,
# just against a store that can actually complete this chain. Verified
# directly against a live mc.gov.sa download URL, 2026-09-07: SSLError without
# this, clean 4.2MB PDF with it.
import truststore
truststore.inject_into_ssl()

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from processor.staged_LLM_Analyzer import StagedLLMAnalyzer
import json
from datetime import date
from utils.countries import tree_path as country_tree_path
from utils.file_links import normalise_all as normalise_files

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("orchestrator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# A bare User-Agent is not enough for every regulator's WAF. SDAIA's rejects
# it outright -- HTTP 200, but a text/html "Request Rejected... consult with
# your administrator" body instead of the PDF -- which _download_and_extract_pdf
# then handed straight to fitz.open() as if it were a real (if tiny/scanned)
# PDF, so it silently reported "1 page, 0 chars extracted" rather than the
# real problem. Adding Accept/Accept-Language (no page-specific Referer
# needed -- checked directly against a live SDAIA URL, 2026-09-07) is enough
# to pass. Shared here so every download path uses the same headers rather
# than each accumulating its own partial fix.
_FILE_DOWNLOAD_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "application/pdf,application/xhtml+xml,text/html;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _get_with_retries(url, *, attempts=3, backoff=1.5, **kwargs):
    """A download failure IS worth retrying, unlike an OCR failure -- network
    conditions genuinely vary between attempts (a WAF hiccup, a momentarily
    slow server), where re-running the same OCR on the same bytes would just
    get the same result every time. Short backoff (1.5s, 3s), not the LLM
    client's longer one -- these are cheap, fast requests, not paid API calls
    worth pacing carefully."""
    last_exc = None
    for attempt in range(attempts):
        if attempt:
            time.sleep(backoff * attempt)
        try:
            resp = requests.get(url, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            logger.warning(f"    fetch attempt {attempt + 1}/{attempts} failed for "
                           f"{url[:80]}: {e}")
    raise last_exc

MIN_TEXT_LEN = 200


class BaseOrchestrator:
    """
    Central pipeline controller.

    VERSIONING STRATEGY (unified — all regulators use compliance_analysis):
    ────────────────────────────────────────────────────────────────────────
    All analysis goes into compliance_analysis (is_current=1, schema_version='v2').

    For CBB specifically:
      - version_id is set on every compliance_analysis row (links to
        regulation_versions.version_id).
      - When a CBB document is modified, old compliance_analysis rows are
        moved to compliance_analysis_versions (status='inactive') and
        deleted from compliance_analysis BEFORE new analysis is written.
      - regulation_versions holds the content snapshots (HTML/text/hash).

    For SAMA / SBP / SECP:
      - version_id is NULL on compliance_analysis rows.
      - No archiving, no regulation_versions rows.
    ────────────────────────────────────────────────────────────────────────
    Ref Key:
    Regulation:  REG-{regulator}-{source}-{id}
    """

    def __init__(self, crawler, repo: MSSQLRepository, downloader: Downloader,
                 ocr_engine: HTMLFallbackEngine = None, llm_analyzer: LLMAnalyzer = None):
        self.crawler = crawler
        self.repo = repo
        self.downloader = downloader
        self.ocr_engine = ocr_engine
        # self.llm_analyzer = LLMAnalyzer()
        # self.staged_analyzer = StagedLLMAnalyzer()
        # self.requirement_matcher = RequirementMatcher()
        self._llm_analyzer = llm_analyzer
        self._staged_analyzer = None
        self._requirement_matcher = None

    @property
    def llm_analyzer(self) -> LLMAnalyzer:
        if self._llm_analyzer is None:
            self._llm_analyzer = LLMAnalyzer()
        return self._llm_analyzer

    @property
    def staged_analyzer(self) -> StagedLLMAnalyzer:
        if self._staged_analyzer is None:
            self._staged_analyzer = StagedLLMAnalyzer()
        return self._staged_analyzer

    @property
    def requirement_matcher(self) -> RequirementMatcher:
        if self._requirement_matcher is None:
            self._requirement_matcher = RequirementMatcher()
        return self._requirement_matcher

    # ================================================================== #
    #  HELPERS                                                             #
    # ================================================================== #

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
    #  UNIFIED CONTENT EXTRACTION — 3-TIER STRATEGY                        #
    # ================================================================== #

    def extract_text_content_unified(
            self,
            doc,
            regulation_id: Optional[int] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Unified 3-tier content extraction.
        Returns (text_content, content_type) or (None, None).

        FOR CBB: If regulation_id is provided, fetch from regulation_versions first.
        """
        extra_meta = getattr(doc, "extra_meta", {}) or {}

        # ── TIER 0: an instrument published as SEVERAL files ──────────────
        #
        # SDAIA's "Personal Data Protection Law and The implementing Regulation"
        # attaches three PDFs — the law, its implementing regulation and the
        # transfer regulation. All three ARE the instrument, so all three have to
        # reach the analyzer. Reading only document_url would analyse one and
        # silently drop the regulatory text of the other two.
        #
        # Each file is delimited by a header naming it. The analyzer records a
        # `source_reference` per obligation, so with markers present an obligation
        # can still say WHICH file it came from — without them a combined row
        # loses attribution between the law and its implementing regulation.
        #
        # Only for genuinely multi-file rows: one url falls through to the tiers
        # below, unchanged.
        #
        # Read from extra_meta["attachment_links"], which is where the files live.
        # This used to read a `document_urls` list field on the document; that
        # field was removed 2026-08-12 in favour of extra_meta, so a multi-file
        # instrument now survives into the database with no schema change at all.
        # `document_url` is EMPTY on these rows, so without this tier they would
        # reach the analyzer with no text whatsoever.
        urls = [u.strip() for u in
                str(extra_meta.get("attachment_links") or "").split("|") if u.strip()]
        if len(urls) > 1:
            logger.info(f"  TIER 0: instrument carries {len(urls)} files")
            titles = [t.strip() for t in
                      str(extra_meta.get("file_titles") or "").split("|")]
            parts, got = [], 0
            for i, u in enumerate(urls):
                label = (titles[i] if i < len(titles) and titles[i]
                         else u.rsplit("/", 1)[-1])
                piece = self._download_and_extract_pdf(u, regulation_id)
                if not piece or len(piece) < MIN_TEXT_LEN:
                    # Recorded, not fatal. A row that loses one of three files is
                    # still worth analysing, but the gap must be visible in the
                    # text rather than inferred from a short document.
                    logger.warning(f"    file {i+1}/{len(urls)} yielded no text: {u[:70]}")
                    parts.append(f"=== FILE {i+1}/{len(urls)}: {label} | {u} ===\n"
                                 f"[no text could be extracted from this file]")
                    continue
                got += 1
                parts.append(f"=== FILE {i+1}/{len(urls)}: {label} | {u} ===\n{piece}")

            if got:
                combined = "\n\n".join(parts)
                logger.info(f"  TIER 0: {got}/{len(urls)} files -> "
                            f"{len(combined):,} chars combined")
                return combined, "pdf_text"
            logger.warning("  TIER 0: no file yielded text; falling through")

        # ── CBB VERSIONED CONTENT: Fetch from regulation_versions ──
        if regulation_id:
            try:
                # Check if this is a CBB regulation
                reg_data = self.repo.get_regulation_by_id(regulation_id)
                if reg_data and reg_data.get("regulator") == "Central Bank of Bahrain":
                    logger.info(f"  CBB regulation detected, fetching from regulation_versions...")

                    # Get the ACTIVE version
                    version_data = self.repo.get_active_regulation_version(regulation_id)

                    if version_data:
                        content_text = (version_data.get("content_text") or "").strip()
                        content_html = (version_data.get("content_html") or "").strip()

                        if len(content_text) >= MIN_TEXT_LEN:
                            logger.info(f"  CBB VERSION: content_text ({len(content_text):,} chars)")
                            return content_text, "html"

                        if len(content_html) >= MIN_TEXT_LEN:
                            logger.info(f"  CBB VERSION: content_html ({len(content_html):,} chars)")
                            return content_html, "html"

                        logger.warning(f"  CBB version exists but text too short")
            except Exception as e:
                logger.warning(f"  Failed to fetch CBB version content: {e}")

        # Tier 1a: SAMA pre-OCR'd PDF text
        org_pdf_text = (extra_meta.get("org_pdf_text") or "").strip()
        if len(org_pdf_text) >= MIN_TEXT_LEN:
            logger.info(f"  TIER 1a: org_pdf_text ({len(org_pdf_text):,} chars)")
            return org_pdf_text, "pdf_text"

        # Tier 1b: CBB / pre-extracted HTML content_text (from extra_meta)
        content_text = (extra_meta.get("content_text") or "").strip()
        if len(content_text) >= MIN_TEXT_LEN:
            logger.info(f"  TIER 1b: content_text ({len(content_text):,} chars)")
            return content_text, "html"

        # Tier 2: Stored document HTML
        document_html = (getattr(doc, "document_html", None) or "").strip()
        if len(document_html) >= MIN_TEXT_LEN:
            logger.info(f"  TIER 2: document_html ({len(document_html):,} chars)")
            return document_html, "html"
        # Tier 3: Download & OCR
        logger.info("  TIER 3: no pre-extracted text, trying downloads...")

        org_pdf_link = extra_meta.get("org_pdf_link")
        if org_pdf_link:
            text = self._download_and_extract_pdf(org_pdf_link, regulation_id)
            if text and len(text) >= MIN_TEXT_LEN:
                return text, "pdf_text"

        document_url = getattr(doc, "document_url", None) or ""
        if document_url.lower().endswith(".pdf"):
            text = self._download_and_extract_pdf(document_url, regulation_id)
            if text and len(text) >= MIN_TEXT_LEN:
                return text, "pdf_text"

        arabic_pdf_link = extra_meta.get("arabic_pdf_link")
        if arabic_pdf_link:
            text = self._download_and_extract_pdf(arabic_pdf_link, regulation_id)
            if text and len(text) >= MIN_TEXT_LEN:
                return text, "pdf_text"

        urdu_url = extra_meta.get("urdu_url")
        if urdu_url:
            text = self._download_and_extract_pdf(urdu_url, regulation_id)
            if text and len(text) >= MIN_TEXT_LEN:
                return text, "pdf_text"

        if document_url and not document_url.lower().endswith(".pdf"):
            logger.info("  Tier 3e: fetching HTML from document_url...")
            try:
                resp = requests.get(
                    document_url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=30
                )
                resp.raise_for_status()
                html = resp.text
                if html and len(html) >= MIN_TEXT_LEN:
                    logger.info(f"  Tier 3e: HTML ({len(html):,} chars)")
                    return html, "html"
            except Exception as e:
                logger.warning(f"  Tier 3e HTML fetch failed: {e}")

        logger.warning("  All extraction tiers exhausted")
        return None, None

    def _download_and_extract_pdf(
        self,
        pdf_url: str,
        regulation_id: Optional[int] = None
    ) -> Optional[str]:
        import tempfile
        tmp_path = None
        try:
            logger.info(f"    PDF: {pdf_url[:80]}")
            resp = _get_with_retries(
                pdf_url,
                headers=_FILE_DOWNLOAD_HEADERS,
                timeout=60,
                stream=True
            )

            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                tmp_path = tmp.name

            with open(tmp_path, "rb") as f:
                head = f.read(5)
            if head != b"%PDF-":
                # Not a real PDF -- almost always a WAF/error page served with
                # a 200 status at a .pdf url. Handing this to fitz used to
                # "succeed" with a garbage 1-page/near-empty result that read
                # as a bad scan rather than the real problem: the download
                # never got the actual file.
                logger.warning(f"    Not a real PDF (starts with {head!r}) -- "
                               f"likely blocked/redirected, not extracting")
                if regulation_id:
                    self.log(regulation_id, "pdf_extraction", "ERROR",
                             f"response was not a PDF (starts with {head!r})")
                return None

            text_content, metadata = OCRProcessor.extract_text_from_pdf_smart(pdf_path=tmp_path)

            if metadata.get("low_quality"):
                # Non-empty, so `if text_content:` alone would call this a
                # success -- but so few pages survived (see
                # OCRProcessor._flag_low_quality) that trusting the fragment
                # would be worse than treating it as a failed extraction.
                logger.warning(
                    f"    Extraction quality too low to trust "
                    f"({metadata.get('good_pages')}/{metadata.get('total_pages')} pages) "
                    f"-- treating as failed, not returning the fragment")
                if regulation_id:
                    self.log(regulation_id, "pdf_extraction", "ERROR",
                             f"low quality: {metadata.get('good_pages')}/"
                             f"{metadata.get('total_pages')} pages usable")
                return None

            if text_content:
                logger.info(
                    f"    Extracted {len(text_content):,} chars "
                    f"(method={metadata.get('method', '?')})"
                )
                if regulation_id:
                    self.log(regulation_id, "pdf_extraction", "SUCCESS",
                             f"{len(text_content):,} chars, {metadata.get('method', '?')}")
                return text_content
            else:
                logger.warning("    Empty text from PDF")
                if regulation_id:
                    self.log(regulation_id, "pdf_extraction", "ERROR", "empty text from PDF")
                return None

        except Exception as e:
            logger.warning(f"    PDF download/extract failed: {e}")
            if regulation_id:
                self.log(regulation_id, "pdf_extraction", "ERROR", str(e))
            return None
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    _OFFICE_EXTS = (".docx", ".xlsx", ".xls")

    def _download_and_extract_file(
        self,
        url: str,
        regulation_id: Optional[int] = None
    ) -> Optional[str]:
        """Same download-to-temp-file shape as _download_and_extract_pdf, but
        dispatches by extension so a .docx/.xlsx/.xls attachment (regulator
        bundles routinely mix these with PDFs -- a comment-submission form
        alongside a draft law, an annex spreadsheet alongside a circular) gets
        real text instead of decide()'s fetch_file_text callback returning
        None for anything that isn't a PDF. .pdf still goes through the
        existing OCR-aware path above -- this only adds the formats that
        never had one."""
        ext = url.lower().rsplit("?", 1)[0].rsplit("#", 1)[0]
        ext = "." + ext.rsplit(".", 1)[-1] if "." in ext.rsplit("/", 1)[-1] else ""
        if ext == ".pdf" or ext not in self._OFFICE_EXTS:
            return self._download_and_extract_pdf(url, regulation_id)

        from processor.office_text_extractor import extract_office_text
        import tempfile
        tmp_path = None
        try:
            logger.info(f"    FILE ({ext}): {url[:80]}")
            resp = _get_with_retries(
                url,
                headers=_FILE_DOWNLOAD_HEADERS,
                timeout=60,
                stream=True
            )

            with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                tmp_path = tmp.name

            text_content = extract_office_text(tmp_path, suffix=ext)
            if text_content:
                logger.info(f"    Extracted {len(text_content):,} chars ({ext})")
                if regulation_id:
                    self.log(regulation_id, "office_extraction", "SUCCESS",
                             f"{len(text_content):,} chars ({ext})")
                return text_content
            logger.warning(f"    Empty/unsupported text from {ext} file")
            return None
        except Exception as e:
            logger.warning(f"    {ext} download/extract failed: {e}")
            if regulation_id:
                self.log(regulation_id, "office_extraction", "ERROR", str(e))
            return None
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    # ================================================================== #
    #  REQUIREMENT MATCHING                                                #
    # ================================================================== #

    def _run_requirement_matching(
        self,
        regulation_id: int,
        analysis_result: dict,
        version_id: Optional[int] = None
    ):
        """
        Cross-reference extracted requirements against existing internal records.
        version_id is passed for CBB (links mappings to a content version),
        None for SAMA/SBP/SECP.
        """
        try:
            self.log(regulation_id, "requirement_matching", "STARTED",
                     f"Starting matching (version_id={version_id})")

            extracted_requirements = analysis_result.get("requirements", [])
            if not extracted_requirements:
                logger.warning(f"No requirements to match for regulation {regulation_id}")
                self.log(regulation_id, "requirement_matching", "SKIPPED",
                         "No extracted requirements")
                return

            existing_requirements  = self.repo.get_all_compliance_requirements()
            existing_controls      = self.repo.get_all_demo_controls()
            existing_kpis          = self.repo.get_all_demo_kpis()
            linked_controls_by_req = self.repo.get_linked_controls_by_requirement()
            linked_kpis_by_req     = self.repo.get_linked_kpis_by_requirement()

            # DECIDE ONCE, THEN KEEP THE ANSWER.
            #
            # Re-matching the same obligations against the same register gives a
            # different answer on 2-3 of every 39 -- genuine ties between similar
            # requirements, which temperature 0 and a fixed seed do not fix
            # because the model is not being random, it is being asked an
            # ambiguous question (docs/determinism.md). The only way the stored
            # verdicts stay stable is not to re-ask.
            #
            # This path had no cache at all, so every re-processed document was
            # re-matched from scratch. The hash covers the obligations AND the
            # internal register, so adding a requirement still re-opens the
            # verdicts it could change.
            from processor import analysis_cache
            reg_row = self.repo.get_regulation_by_id(regulation_id) or {}
            extra_meta = reg_row.get("extra_meta")
            corpus_hash = analysis_cache.corpus_fingerprint(
                existing_requirements, existing_controls, existing_kpis)
            obligation_texts = [r.get("requirement_text") or r.get("obligation_text") or ""
                                for r in extracted_requirements]
            existing_mappings = []
            try:
                existing_mappings = self.repo.get_requirement_mappings_by_regulation(
                    regulation_id) or []
            except Exception:
                pass          # not on every repo; treat as "nothing stored yet"
            should_match, match_hash, why = analysis_cache.decide_matching(
                extra_meta=extra_meta,
                obligation_texts=obligation_texts,
                corpus_hash=corpus_hash,
                model=getattr(self.requirement_matcher, "model", ""),
                has_existing_rows=bool(existing_mappings),
                force=bool(getattr(self, "_force_matching", False)),
            )
            if not should_match:
                logger.info(f"Requirement matching SKIPPED for {regulation_id}: {why}")
                self.log(regulation_id, "requirement_matching", "SKIPPED", why)
                if not analysis_cache.as_dict(extra_meta).get(
                        analysis_cache.MATCH_HASH_KEY):
                    analysis_cache.record_matching(
                        self.repo, regulation_id, extra_meta, match_hash,
                        getattr(self.requirement_matcher, "model", ""))
                return
            logger.info(f"Requirement matching RUNNING for {regulation_id}: {why}")

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

            if requirement_mappings:
                self.repo.store_requirement_mappings(requirement_mappings, version_id=version_id)

            partially_matched_ids = [
                m["matched_requirement_id"]
                for m in requirement_mappings
                if m["match_status"] == "partially_matched"
                and m["matched_requirement_id"] is not None
            ]
            if partially_matched_ids:
                self.repo.flag_partially_matched_requirements(partially_matched_ids)

            new_req_mappings = [m for m in requirement_mappings if m["match_status"] == "new"]
            for mapping in new_req_mappings:
                try:
                    req_text = mapping["extracted_requirement_text"]
                    title    = req_text[:100].strip() + ("..." if len(req_text) > 100 else "")
                    # The key is derived from the TEXT, not from the loop index.
                    #
                    # It used to be AUTO-<regulation_id>-<i>, and `i` is a position
                    # in a list the LLM produced. Re-analyse the same regulation and
                    # the model may emit a different number of new requirements in a
                    # different order, so AUTO-42-0 could name different regulatory
                    # text on every run. That made the row un-upsertable: the only
                    # options were to duplicate forever, or to overwrite one
                    # requirement with another's text.
                    #
                    # Hashing the text gives a stable identity — same obligation,
                    # same key, every run — which is what lets
                    # insert_new_suggested_requirement reuse the existing row.
                    digest = hashlib.md5(req_text.strip().encode("utf-8")).hexdigest()[:8]
                    new_req_id = self.repo.insert_new_suggested_requirement({
                        "title":       title,
                        "description": req_text,
                        "ref_key":     f"AUTO-{regulation_id}-{digest}",
                        "ref_no":      f"REG-{regulation_id}"
                    })
                    for ctrl in new_controls_to_insert:
                        if ctrl.get("_req_id") is None:
                            ctrl["_req_id"] = new_req_id
                    for kpi in new_kpis_to_insert:
                        if kpi.get("_req_id") is None:
                            kpi["_req_id"] = new_req_id
                except Exception as e:
                    logger.error(f"Failed to insert new suggested requirement: {e}")

            if control_links:
                self.repo.store_control_links(control_links)
            if kpi_links:
                self.repo.store_kpi_links(kpi_links)

            for ctrl in new_controls_to_insert:
                try:
                    new_ctrl_id = self.repo.insert_new_suggested_control({
                        "title": ctrl["title"], "description": ctrl["description"],
                        "control_key": ctrl["control_key"]
                    })
                    req_id = ctrl.get("_req_id")
                    if req_id:
                        self.repo.store_control_links([{
                            "compliancerequirement_id": req_id,
                            "control_id": new_ctrl_id,
                            "match_status": "new",
                            "match_explanation": ctrl.get("_explanation", ""),
                            "regulation_id": regulation_id
                        }])
                except Exception as e:
                    logger.error(f"Failed to insert new suggested control: {e}")

            for kpi in new_kpis_to_insert:
                try:
                    new_kpi_id = self.repo.insert_new_suggested_kpi({
                        "title": kpi["title"], "description": kpi["description"],
                        "kisetup_key": kpi["kisetup_key"], "formula": kpi.get("formula", "")
                    })
                    req_id = kpi.get("_req_id")
                    if req_id:
                        self.repo.store_kpi_links([{
                            "compliancerequirement_id": req_id,
                            "kisetup_id": new_kpi_id,
                            "match_status": "new",
                            "match_explanation": kpi.get("_explanation", ""),
                            "regulation_id": regulation_id
                        }])
                except Exception as e:
                    logger.error(f"Failed to insert new suggested KPI: {e}")

            fully   = sum(1 for m in requirement_mappings if m["match_status"] == "fully_matched")
            partial = sum(1 for m in requirement_mappings if m["match_status"] == "partially_matched")
            new_r   = sum(1 for m in requirement_mappings if m["match_status"] == "new")
            low_c   = sum(1 for m in requirement_mappings
                          if m.get("match_confidence") == "low")

            # Recorded only after the mappings are stored. A hash written against
            # matching that failed to persist would suppress the retry -- the same
            # rule the analysis cache follows.
            analysis_cache.record_matching(
                self.repo, regulation_id, extra_meta, match_hash,
                getattr(self.requirement_matcher, "model", ""))

            self.log(
                regulation_id, "requirement_matching", "SUCCESS",
                f"Reqs: {fully} fully / {partial} partial / {new_r} new | "
                f"{low_c} low-confidence | "
                f"Ctrl links: {len(control_links)} | KPI links: {len(kpi_links)} | "
                f"New controls: {len(new_controls_to_insert)} | New KPIs: {len(new_kpis_to_insert)}"
            )

        except Exception as e:
            logger.error(f"Requirement matching failed for regulation {regulation_id}: {e}")
            self.log(regulation_id, "requirement_matching", "ERROR", str(e))

    # ================================================================== #
    #  UNIFIED LLM ANALYSIS — ALL REGULATORS                               #
    #                                                                       #
    #  Single analysis method used by CBB, SAMA, SBP, SECP.               #
    #  The only difference is whether version_id is passed in.             #
    # ================================================================== #

    def _run_llm_analysis(
        self,
        regulation_id: int,
        doc,
        text_content: str,
        content_type: str,
        version_id: Optional[int] = None,
    ) -> bool:
        """
        Run the 4-stage LLM pipeline and store results in compliance_analysis.

        Works for ALL regulators:
          - version_id=None  -> SAMA / SBP / SECP (no content versioning)
          - version_id=<int> -> CBB (links analysis row to a regulation_versions snapshot)

        Returns True on success, False on failure.
        """
        try:
            self.log(regulation_id, "llm_analysis", "STARTED",
                     f"4-stage LLM (version_id={version_id}, "
                     f"content_type={content_type}, text_len={len(text_content):,})")

            clean_text = self.llm_analyzer.normalize_input_text(
                text_content, content_type=content_type
            )

            if len(clean_text) < MIN_TEXT_LEN:
                raise ValueError(
                    f"Text too short after normalization ({len(clean_text)} chars)"
                )

            rows = self.staged_analyzer.analyze(
                text=clean_text,
                regulation_id=regulation_id,
                document_title=getattr(doc, "title", "Untitled"),
            )

            if not rows:
                raise ValueError("4-stage analysis returned no requirements")

            # All regulators write to the same table.
            # version_id=None for SAMA/SBP, version_id=<int> for CBB.
            self.repo.store_analysis(rows, version_id=version_id)

            self.log(regulation_id, "llm_analysis", "SUCCESS",
                     f"4-stage analysis: {len(rows)} rows stored "
                     f"(version_id={version_id})")

            # Build requirement list for matching
            extracted_for_matcher = []
            for r in rows:
                s2 = r.get("stage2_json") or {}
                if isinstance(s2, str):
                    try:
                        s2 = json.loads(s2)
                    except Exception:
                        s2 = {}
                for ob in s2.get("normalized_obligations", []):
                    extracted_for_matcher.append({
                        "requirement_text": ob["obligation_text"],
                        "department": "",
                        "risk_level": ob.get("criticality", "Medium"),
                        "controls": [],
                        "kpis": [],
                        "_obligation_id": ob["obligation_id"],
                        "_requirement_id": r.get("requirement_id"),
                    })

            combined_for_matcher = {"requirements": extracted_for_matcher}
            # version_id flows through to sama_requirement_mapping
            self._run_requirement_matching(
                regulation_id, combined_for_matcher, version_id=version_id
            )
            return True

        except Exception as e:
            logger.error(f"LLM analysis failed for regulation {regulation_id}: {e}")
            self.log(regulation_id, "llm_analysis", "ERROR", str(e))
            return False

    # Keep the old names as aliases so any code that imported them still works
    def _run_llm_analysis_unified(self, regulation_id, doc, text_content,
                                   content_type) -> bool:
        return self._run_llm_analysis(
            regulation_id, doc, text_content, content_type, version_id=None
        )

    def _run_llm_analysis_versioned(self, regulation_id, doc, text_content,
                                     content_type, version_id) -> bool:
        return self._run_llm_analysis(
            regulation_id, doc, text_content, content_type, version_id=version_id
        )

    # ================================================================== #
    #  DOCUMENT FILTERING                                                   #
    # ================================================================== #

    def run_for_regulator(self, regulator_name: str):
        logger.warning(f"=== RUNNING REGULATOR: {regulator_name} ===")
        docs = self.crawler.fetch_documents()
        # THE FILE RULE, applied where EVERY document passes: one file ->
        # document_url, several -> extra_meta.attachment_links, never both.
        # Done here rather than per crawler because each crawler had
        # invented its own spelling (org_pdf_link, arabic_pdf, pdf_link)
        # and a frontend had to know all of them. See utils/file_links.py.
        docs = normalise_files(docs)
        logger.warning(f"Scraped {len(docs)} documents from crawler")

        new_docs, existing_docs = self.filter_new_documents(docs)
        logger.warning(f"{len(new_docs)} new / {len(existing_docs)} existing")

        if not new_docs:
            logger.warning("No new documents to process. Exiting...")
            return

        self._process_docs(new_docs, regulator_name)

        logger.warning(f"Finished processing all {len(new_docs)} documents.")

    def _process_docs(self, docs: List, regulator_name: str):
        """Process documents concurrently.

        Each document is an independent chain of network-bound calls (download,
        OCR, four LLM stages), so they overlap cleanly. Total in-flight LLM
        requests are additionally bounded by LLM_MAX_CONCURRENCY inside
        StagedLLMAnalyzer, so raising DOC_MAX_WORKERS cannot stampede OpenRouter.

        Set DOC_MAX_WORKERS=1 to restore the previous serial behaviour.
        """
        total = len(docs)
        workers = max(1, int(os.getenv("DOC_MAX_WORKERS", "4")))

        if workers == 1 or total == 1:
            for idx, doc in enumerate(docs, start=1):
                logger.info(f"  [{idx}/{total}] {str(doc.title)[:60]}")
                self._process_single_doc(idx, doc, regulator_name)
                gc.collect()
            return

        logger.warning(f"  Processing {total} documents with {workers} workers")
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self._process_single_doc, idx, doc, regulator_name): doc
                for idx, doc in enumerate(docs, start=1)
            }
            for fut in as_completed(futures):
                doc = futures[fut]
                done += 1
                try:
                    fut.result()
                    logger.info(f"  [{done}/{total}] done: {str(doc.title)[:60]}")
                except Exception as e:
                    # One bad document must not abort the batch — but the message
                    # alone is not diagnosable. `'NoneType' object is not
                    # subscriptable` told us nothing about WHICH call failed and
                    # cost a whole debugging pass on 2026-08-16; the traceback
                    # goes to debug so it is there when needed and silent when not.
                    logger.error(f"  [{done}/{total}] FAILED: {str(doc.title)[:60]} — {e}")
                    logger.debug("document that failed: %s", str(getattr(doc, "document_url", ""))[:120],
                                 exc_info=True)
        gc.collect()

    def run_for_cbb(self, mode: str = "auto", from_date=None, to_date=None, skip_analysis: bool = False):
        from crawler.cbb_crawler import CBBCrawlerV2
        from crawler.cbb_monitoring_crawler import CBBMonitoringCrawler

        logger.warning(f"=== CBB PIPELINE: mode={mode} skip_analysis={skip_analysis} ===")
        self._skip_analysis = skip_analysis

        if mode == "auto":
            last_date = self.repo.get_last_cbb_crawl_date()
            mode = "monitoring" if last_date else "full"
            logger.warning(f"  Auto-detected mode: {mode} (last crawl: {last_date})")

        crawler = CBBCrawlerV2(request_delay=1.5) if mode == "full" \
            else CBBMonitoringCrawler(repo=self.repo, request_delay=1.0)

        fetch_kwargs = {}
        if mode == "monitoring" and from_date is not None:
            fetch_kwargs["from_date"] = from_date
        if mode == "monitoring" and to_date is not None:
            fetch_kwargs["to_date"] = to_date

        docs = crawler.fetch_documents(**fetch_kwargs)
        logger.warning(f"  Fetched {len(docs)} documents")

        new_docs, existing_docs = self.filter_new_documents(docs)
        modified_docs = [
            d for d in existing_docs
            if d.extra_meta.get("monitoring_status") == "modified"
        ]
        docs_to_process = new_docs + modified_docs

        logger.warning(
            f"  {len(new_docs)} new / {len(modified_docs)} modified / "
            f"{len(existing_docs) - len(modified_docs)} unchanged"
        )

        if not docs_to_process:
            logger.warning("  No documents to process.")
            return

        self._process_docs(docs_to_process, "CBB")

        logger.warning(f"  CBB pipeline complete. Processed {len(docs_to_process)} documents.")

    # ================================================================== #
    #  SIMAH — the only non-CBB path that versions a modified document     #
    # ================================================================== #

    def run_for_simah(self):
        """New documents plus amended ones.

        Separate from `run_for_regulator`, which discards existing docs and so
        can never report a modification. Scoped to SIMAH on purpose — no shared
        method is modified, and no other regulator calls this.
        """
        logger.warning("=== RUNNING REGULATOR: SIMAH ===")
        docs = self.crawler.fetch_documents()
        # THE FILE RULE, applied where EVERY document passes: one file ->
        # document_url, several -> extra_meta.attachment_links, never both.
        # Done here rather than per crawler because each crawler had
        # invented its own spelling (org_pdf_link, arabic_pdf, pdf_link)
        # and a frontend had to know all of them. See utils/file_links.py.
        docs = normalise_files(docs)
        logger.warning(f"Fetched {len(docs)} documents from crawler")

        buckets = {"new": [], "modified": [], "unchanged": []}
        for d in docs:
            status = (getattr(d, "extra_meta", {}) or {}).get(
                "monitoring_status", "new")
            buckets.get(status, buckets["new"]).append(d)

        logger.warning(
            f"  {len(buckets['new'])} new / {len(buckets['modified'])} modified"
            f" / {len(buckets['unchanged'])} unchanged")

        # Say what we cannot see; silence would read as coverage.
        blind = [d for d in docs
                 if not (getattr(d, "extra_meta", {}) or {}).get(
                     "hash_covers_content", True)]
        if blind:
            logger.warning(
                f"  {len(blind)} document(s) hashed on identity only — an edit "
                f"at the same URL CANNOT be detected: "
                + ", ".join((d.title or "?")[:40] for d in blind))

        docs_to_process = buckets["new"] + buckets["modified"]
        if not docs_to_process:
            logger.warning("  Nothing to process.")
            return

        for idx, doc in enumerate(docs_to_process, start=1):
            logger.info(f"  [{idx}/{len(docs_to_process)}] {(doc.title or '')[:60]}")
            self._process_simah_doc(idx, doc)
            gc.collect()

        logger.warning(f"  SIMAH pipeline complete. "
                       f"Processed {len(docs_to_process)} documents.")

    def _process_simah_doc(self, idx, doc):
        """Insert a new document, or version an amended one.

        Mirrors `_process_cbb_doc`, which hardcodes CBB's regulator string and
        log step — rewriting that would change a path three live regulators use.
        """
        meta = getattr(doc, "extra_meta", {}) or {}
        status = meta.get("monitoring_status", "new")
        existing_reg_id = meta.get("existing_regulation_id")
        content_hash = meta.get("content_hash", "") or getattr(doc, "content_hash", "")
        document_html = getattr(doc, "document_html", None)
        content_text = meta.get("content_text", "")

        try:
            if hasattr(doc, "doc_path") and isinstance(doc.doc_path, list):
                # tree_path prepends the COUNTRY. doc.doc_path itself is left
                # alone: it is an identity field, and rewriting it would
                # reclassify every stored document. See config/countries.yml.
                doc.compliancecategory_id = self._get_or_create_compliance_category(
                    country_tree_path(doc.doc_path,
                                      getattr(doc, "regulator", "")))
            else:
                doc.compliancecategory_id = None
        except Exception as e:
            logger.error(f"Failed to assign compliance category: {e}")
            doc.compliancecategory_id = None

        # ── MODIFIED ────────────────────────────────────────────────────── #
        if status == "modified" and existing_reg_id:
            try:
                logger.warning(f"  Versioning MODIFIED SIMAH document "
                               f"(reg_id={existing_reg_id})")

                existing = self.repo.get_regulation_by_id(existing_reg_id)
                if existing:
                    old_html = existing.get("document_html") or ""
                    old_meta = existing.get("extra_meta") or {}
                    old_text = old_meta.get("content_text") or ""
                    old_hash = existing.get("content_hash") or ""
                else:
                    logger.warning(f"Could not fetch existing regulation "
                                   f"{existing_reg_id}")
                    old_html = old_text = old_hash = ""

                # Deactivate before inserting, or two versions read as current.
                with self.repo._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        UPDATE regulation_versions
                        SET status = 'inactive'
                        WHERE regulation_id = ?
                        AND status = 'active'
                        """,
                        (existing_reg_id,)
                    )
                    rows_updated = cursor.rowcount
                    conn.commit()
                    logger.info(f"  Marked {rows_updated} version(s) inactive")

                old_version_id = self.repo.insert_regulation_version(
                    regulation_id=existing_reg_id,
                    regulator=doc.regulator,
                    content_html=old_html,
                    content_text=old_text,
                    content_hash=old_hash,
                    updated_date=date.today(),
                    change_summary=(
                        f"Previous version archived on {date.today().isoformat()}"),
                    status='inactive',
                )
                logger.info(f"  Archived old content as version {old_version_id}")

                archived = self.repo.archive_current_analysis(
                    existing_reg_id, old_version_id)
                logger.info(f"  Archived {archived} analysis rows")

                current_version_id = self.repo.insert_regulation_version(
                    regulation_id=existing_reg_id,
                    regulator=doc.regulator,
                    content_html=document_html,
                    content_text=content_text,
                    content_hash=content_hash,
                    updated_date=doc.published_date,
                    change_summary=(
                        f"Content changed at source, detected "
                        f"{date.today().isoformat()} "
                        f"(snapshot captured {meta.get('captured_at', '?')})"),
                )
                logger.info(f"  Created new version {current_version_id}")

                self.repo.update_cbb_content_hash(existing_reg_id, content_hash)
                self.repo.update_regulation(
                    existing_reg_id,
                    document_html=document_html,
                    published_date=doc.published_date,
                )
                self.log(existing_reg_id, "simah_version", "SUCCESS",
                         f"Versions: {old_version_id} (archived) -> "
                         f"{current_version_id} (active)")

                self._extract_and_analyze_versioned(
                    doc, existing_reg_id, current_version_id)
            except Exception as e:
                logger.error(f"Failed to version SIMAH doc {doc.title}: {e}")
                self.log(existing_reg_id, "simah_version", "ERROR", str(e))
            return

        # ── NEW ─────────────────────────────────────────────────────────── #
        try:
            regulation_id = self.repo._insert_regulation(doc)
            doc.id = regulation_id
            # Store the hash now, or the next run backfills and misses the first
            # real amendment.
            self.repo.update_cbb_content_hash(regulation_id, content_hash)
            self.log(regulation_id, "insert", "SUCCESS",
                     "SIMAH document inserted")
        except Exception as e:
            logger.error(f"Failed to insert SIMAH document: {e}")
            self.log(None, "insert", "ERROR", str(e),
                     doc_url=getattr(doc, "document_url", None))
            return

        self._extract_and_analyze(doc, regulation_id)

    def filter_new_documents(self, all_documents: List):
        new_docs, existing_docs = [], []

        for doc in all_documents:
            if getattr(doc, "regulator", "") == "Central Bank of Bahrain":
                source_url = getattr(doc, "source_page_url", None)
                if not source_url:
                    logger.warning(f"CBB doc has no source_page_url, skipping: {doc.title}")
                    continue
                exists = self.repo.document_exists_by_source_url(source_url)
                (existing_docs if exists else new_docs).append(doc)
                continue

            if doc.published_date:
                exists = self.check_exists_in_db(
                    doc.title, doc.published_date, getattr(doc, "doc_path", None)
                )
                (existing_docs if exists else new_docs).append(doc)
                continue

            if getattr(doc, "category", "").lower() == "regulatory returns":
                exists = self.check_exists_in_db(
                    doc.title, None, getattr(doc, "doc_path", None)
                )
                (existing_docs if exists else new_docs).append(doc)
                continue

            if getattr(doc, "source_system", "").upper() == "DPC-CIRCULAR":
                exists = self.check_exists_in_db(
                    doc.title, None, getattr(doc, "doc_path", None)
                )
                (existing_docs if exists else new_docs).append(doc)
                continue

            # Fallback for any regulator/source whose documents may lack a
            # published_date (e.g. SAMA Rulebook hub/listing-page documents)
            # -- dedupe by (document_url, category) instead of dropping the
            # document. Category-scoped because some documents (e.g. SAMA) are
            # intentionally cross-listed under more than one category for the
            # same document_url -- a bare url check would wrongly skip the
            # second category's copy as a "duplicate".
            document_url = getattr(doc, "document_url", None)
            if document_url:
                exists = self.repo.document_exists_by_url(document_url, getattr(doc, "category", None))
                (existing_docs if exists else new_docs).append(doc)
                continue

            logger.warning(f"Skipping {doc.title} (missing published_date)")

        return new_docs, existing_docs

    def _get_or_create_compliance_category(self, hierarchy: list) -> int:
        parent_id = None
        last_index = len(hierarchy) - 1
        for i, title in enumerate(hierarchy):
            folder_id = self.repo.get_folder_id(title, parent_id)

            if folder_id is None and parent_id is not None:
                # Not found as a direct child. The doc_path may be missing
                # intermediate levels (e.g. a deletion notice page whose
                # sidebar trail skips 'CBB Rulebook'). Search the subtree
                # rooted at the current parent — if found, jump to that node
                # so subsequent segments resolve against the correct parent.
                folder_id = self.repo.find_folder_in_subtree(title, parent_id)

            if folder_id is not None and i == last_index:
                # Leaf segment: if a different regulation already owns this
                # exact (title, parent) slot, don't merge into it -- create a
                # separate node so this document gets its own tree position.
                if self.repo.regulation_exists_for_category(folder_id):
                    folder_id = None

            parent_id = folder_id if folder_id else self.repo.insert_folder(title, parent_id)
        return parent_id

    def check_exists_in_db(self, title, published_date, doc_path) -> bool:
        try:
            return self.repo.document_exists(title, published_date, doc_path)
        except Exception as e:
            logger.error(f"Failed to check document existence: {e}")
            return False

    # ================================================================== #
    #  SINGLE DOC PROCESSING — MAIN ENTRY                                  #
    # ================================================================== #

    def _process_single_doc(self, idx, doc, regulator_name):
        logger.info(f"[{idx}] Starting: {doc.title}")
        regulator_upper = regulator_name.upper()

        try:
            if hasattr(doc, "doc_path") and isinstance(doc.doc_path, list):
                doc.compliancecategory_id = self._get_or_create_compliance_category(
                    country_tree_path(doc.doc_path,
                                      getattr(doc, "regulator", "")))
            else:
                doc.compliancecategory_id = None
        except Exception as e:
            logger.error(f"Failed to assign compliance category: {e}")
            doc.compliancecategory_id = None

        # Regulatory Returns: insert only, no LLM
        if getattr(doc, "category", "").lower() == "regulatory returns":
            try:
                import hashlib
                doc.title_hash = (
                    hashlib.md5((doc.title or "").encode("utf-8")).hexdigest()
                    if doc.title else None
                )
                regulation_id = self.repo._insert_regulation(doc)
                doc.id = regulation_id
                self.log(regulation_id, "insert", "SUCCESS",
                         "Regulatory Return inserted (no document)")
            except Exception as e:
                logger.error(f"Failed to insert Regulatory Return: {e}")
                self.log(None, "insert", "ERROR", str(e),
                         doc_url=getattr(doc, "document_url", None))
            return

        # CBB: versioned path
        if regulator_upper == "CBB":
            self._process_cbb_doc(doc)
            return

        # SAMA / SBP / SECP: simple insert -> extract -> analyze
        try:
            regulation_id = self.repo._insert_regulation(doc)
            doc.id = regulation_id
            self.log(regulation_id, "insert", "SUCCESS",
                     f"{regulator_name} document inserted")
        except Exception as e:
            logger.error(f"Failed to insert {regulator_name} document: {e}")
            self.log(None, "insert", "ERROR", str(e))
            return

        self._extract_and_analyze(doc, regulation_id)

    # ================================================================== #
    #  CBB-SPECIFIC: VERSIONED INSERT / UPDATE                             #
    # ================================================================== #

    def _process_cbb_doc(self, doc):
        """
        CBB versioning logic:

        NEW document:
          1. Insert regulation record
          2. Create regulation_versions snapshot (version_id=N)
          3. Store content hash on regulations row
          4. Run analysis -> store in compliance_analysis with version_id=N

        MODIFIED document:
          1. Fetch old content from regulations
          2. Archive old content -> regulation_versions (version_id=A)
          3. Archive old analysis -> compliance_analysis_versions (status=inactive)
             AND delete from compliance_analysis
          4. Create new regulation_versions snapshot (version_id=B)
          5. Update regulations row with new content + hash
          6. Run analysis -> store in compliance_analysis with version_id=B
        """
        extra_meta        = getattr(doc, "extra_meta", {}) or {}
        monitoring_status = extra_meta.get("monitoring_status", "new")
        existing_reg_id   = extra_meta.get("existing_regulation_id")
        content_hash      = extra_meta.get("content_hash", "")
        content_text      = extra_meta.get("content_text", "")
        document_html     = getattr(doc, "document_html", None)
        regulation_id     = None
        current_version_id = None

        # ── MODIFIED ─────────────────────────────────────────────────────
        if monitoring_status == "modified" and existing_reg_id:
            try:
                logger.info(
                    f"  Processing MODIFIED CBB document (reg_id={existing_reg_id})"
                )

                # Step 1: fetch old content
                existing = self.repo.get_regulation_by_id(existing_reg_id)
                if existing:
                    old_html = existing.get("document_html") or ""
                    old_meta = existing.get("extra_meta") or {}
                    old_text = old_meta.get("content_text") or ""
                    old_hash = existing.get("content_hash") or ""
                else:
                    logger.warning(
                        f"Could not fetch existing regulation {existing_reg_id}"
                    )
                    old_html = old_text = old_hash = ""

                # Step 2: CRITICAL - Mark ALL existing active versions as inactive
                with self.repo._get_conn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        UPDATE regulation_versions 
                        SET status = 'inactive' 
                        WHERE regulation_id = ? 
                        AND status = 'active'
                        """,
                        (existing_reg_id,)
                    )
                    rows_updated = cursor.rowcount
                    conn.commit()
                    logger.info(f"  Marked {rows_updated} existing version(s) as inactive")

                # Step 3: archive old CONTENT -> regulation_versions (as inactive)
                old_version_id = self.repo.insert_regulation_version(
                    regulation_id=existing_reg_id,
                    regulator="Central Bank of Bahrain",
                    content_html=old_html,
                    content_text=old_text,
                    content_hash=old_hash,
                    updated_date=date.today(),
                    change_summary=(
                        f"Previous version archived on {date.today().isoformat()}"
                    ),
                    status='inactive',
                )
                logger.info(f"  Archived old content as version {old_version_id}")

                # Step 4: archive old ANALYSIS -> compliance_analysis_versions
                #         AND clear compliance_analysis
                archived = self.repo.archive_current_analysis(
                    existing_reg_id, old_version_id
                )
                logger.info(
                    f"  Archived {archived} analysis rows "
                    f"(version={old_version_id}, status=inactive)"
                )

                # Step 5: create NEW content version (active by default)
                current_version_id = self.repo.insert_regulation_version(
                    regulation_id=existing_reg_id,
                    regulator="Central Bank of Bahrain",
                    content_html=document_html,
                    content_text=content_text,
                    content_hash=content_hash,
                    updated_date=doc.published_date,
                    change_summary=f"Updated content on {date.today().isoformat()}",
                )
                logger.info(f"  Created new version {current_version_id}")

                # Step 6: update regulations row
                self.repo.update_cbb_content_hash(existing_reg_id, content_hash)
                self.repo.update_regulation(
                    existing_reg_id,
                    document_html=document_html,
                    published_date=doc.published_date,
                )

                self.log(
                    existing_reg_id, "cbb_version", "SUCCESS",
                    f"Versions: {old_version_id} (archived) -> "
                    f"{current_version_id} (active)"
                )
                regulation_id = existing_reg_id

            except Exception as e:
                logger.error(f"Failed to version CBB doc {doc.title}: {e}")
                self.log(existing_reg_id, "cbb_version", "ERROR", str(e))
                return

        # ── NEW ──────────────────────────────────────────────────────────
        else:
            try:
                logger.info("  Processing NEW CBB document")

                regulation_id = self.repo._insert_regulation(doc)
                doc.id = regulation_id

                self.repo.update_cbb_content_hash(regulation_id, content_hash)

                current_version_id = self.repo.insert_regulation_version(
                    regulation_id=regulation_id,
                    regulator="Central Bank of Bahrain",
                    content_html=document_html,
                    content_text=content_text,
                    content_hash=content_hash,
                    updated_date=doc.published_date,
                    change_summary="Initial crawl",
                )
                logger.info(
                    f"  Created initial version {current_version_id} "
                    f"for regulation {regulation_id}"
                )
                self.log(
                    regulation_id, "insert", "SUCCESS",
                    f"CBB page inserted with initial version {current_version_id}"
                )

            except Exception as e:
                logger.error(f"Failed to insert CBB doc {doc.title}: {e}")
                self.log(None, "insert", "ERROR", str(e))
                return

        # Skip LLM for shallow/folder pages
        depth = extra_meta.get("depth", 0)
        if depth < 2:
            logger.info(
                f"  Skipping LLM for CBB page (depth={depth}) — folder/index page"
            )
            self.log(regulation_id, "llm_analysis", "SKIPPED",
                     f"depth={depth}, folder/index page")
            return

        # Honour global skip flag
        if getattr(self, "_skip_analysis", False):
            logger.info(f"  Skipping analysis (--skip-analysis) for reg {regulation_id}")
            return

        # Run analysis with version_id
        logger.info(
            f"  Running analysis for regulation {regulation_id}, "
            f"version {current_version_id}"
        )
        self._extract_and_analyze(doc, regulation_id, version_id=current_version_id)

    # ================================================================== #
    #  EXTRACT + ANALYZE — UNIFIED FOR ALL REGULATORS                      #
    # ================================================================== #

    def _extract_and_analyze(
        self,
        doc,
        regulation_id: int,
        version_id: Optional[int] = None,
    ):
        """
        Run content extraction then 4-stage LLM analysis.

        Works for ALL regulators. version_id is:
          - None        -> SAMA / SBP / SECP (no content versioning)
          - <int>       -> CBB (links compliance_analysis row to regulation_versions)
        """
        logger.info(
            f"  Extraction (regulation_id={regulation_id}, version_id={version_id})"
        )

        text_content, content_type = self.extract_text_content_unified(
            doc, regulation_id=regulation_id
        )

        if not text_content or len(text_content) < MIN_TEXT_LEN:
            msg = f"Insufficient text: {len(text_content or '')} chars"
            logger.error(f"  {msg}")
            self.log(regulation_id, "validation", "ERROR", msg)
            return

        success = self._run_llm_analysis(
            regulation_id=regulation_id,
            doc=doc,
            text_content=text_content,
            content_type=content_type,
            version_id=version_id,
        )

        if success:
            logger.info(
                f"  Analysis complete "
                f"(regulation_id={regulation_id}, version_id={version_id})"
            )
        else:
            logger.warning(
                f"  Analysis failed "
                f"(regulation_id={regulation_id}, version_id={version_id})"
            )

    # Keep old method names as aliases — they delegate to the unified method
    def _extract_and_analyze_versioned(self, doc, regulation_id: int, version_id: int):
        self._extract_and_analyze(doc, regulation_id, version_id=version_id)

# --------------------------------------------------------------------------- #
#  ONE ORCHESTRATOR                                                            #
# --------------------------------------------------------------------------- #
#
# `Orchestrator` is no longer this class. It resolves to the MERGED class —
# `dynamic_crawler.formfill.orch.NewOrchestrator`, which is BaseOrchestrator plus
# the identity, classification, versioning and folder-tree logic.
#
# WHY THIS AND NOT A FLATTENED FILE
#
# The two were never rivals: NewOrchestrator subclasses this one and overrides
# exactly five methods, with a single `super()` call between them. The
# inheritance IS the merge. What was actually wrong is that callers could still
# reach the BASE, and four of them did — jobs/run_regulator.py, jobs/sama_job.py,
# jobs/sbp_job.py and crawler/cbb_monitoring_crawler.py all constructed it
# directly, so those runs had NO change classification, NO version rows and NO
# compliancecategory tree. They looked like they were working.
#
# Rebinding the name fixes that for every caller at once, without moving 2,130
# lines of working code between files and hoping nothing shifted.
#
# The import is LAZY (PEP 562 module __getattr__) because orch.py imports this
# module — doing it at the top would be a cycle. Anything that genuinely wants
# the pre-merge behaviour asks for `BaseOrchestrator` by name and thereby says so.
def __getattr__(name):
    if name == "Orchestrator":
        from dynamic_crawler.formfill.orch import NewOrchestrator
        return NewOrchestrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
