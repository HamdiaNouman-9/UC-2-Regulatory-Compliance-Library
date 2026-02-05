import os
import json
import logging
import re
import requests
from typing import Dict, Any, List
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from processor.Text_Extractor import OCRProcessor
import pytesseract
from utils.lang_detector import detect_language

load_dotenv()
logger = logging.getLogger(__name__)
class LLMAnalyzer:
    """
    Analyzes KSA banking regulations using LLM with OCR fallback for scanned documents.
    """

    def __init__(
            self,
            model: str = "deepseek/deepseek-v3.2",
            max_chunk_size: int = 12000,
            min_text_length: int = 300
    ):
        self.model = model
        self.max_chunk_size = max_chunk_size
        self.min_text_length = min_text_length
        self.openrouter_api_key = os.getenv("OPENROUTER_API_KEY")

        if not self.openrouter_api_key:
            raise ValueError("Missing OPENROUTER_API_KEY environment variable")

        # Verify OCR capability at startup
        if not OCRProcessor.is_ocr_available():
            logger.warning(
                "Arabic OCR not available. Scanned documents will fail.\n"
                "Install Tesseract with Arabic support:\n"
                "  Linux: sudo apt-get install tesseract-ocr tesseract-ocr-ara\n"
                "  Windows: Install from https://github.com/UB-Mannheim/tesseract/wiki\n"
                "           Then download ara.traineddata from https://github.com/tesseract-ocr/tessdata"
            )

    def normalize_input_text(self, content: str) -> str:
        """
        Normalize text for LLM consumption.
        Handles both HTML content and pre-cleaned PDF text from smart extraction.
        """

        # Check if this is already cleaned PDF text from smart extraction
        # (Smart extraction adds "PAGE X" markers and separators)
        if "PAGE" in content[:300] and "=" * 60 in content[:300]:
            logger.info("Detected pre-cleaned PDF text from smart extraction")
            # Just light cleanup, no need for HTML parsing or OCR
            text = re.sub(r'\s+', ' ', content)
            text = re.sub(r'\n{3,}', '\n\n', text)
            return text.strip()

        # Original HTML processing
        soup = BeautifulSoup(content, 'html.parser')

        for tag in soup(['script', 'style', 'noscript', 'header', 'footer', 'svg']):
            tag.decompose()

        text = soup.get_text(separator='\n\n').strip()
        text = re.sub(r'\s+', ' ', text)

        if len(text) > self.min_text_length:
            logger.info(f"Native text extraction: {len(text)} chars")
            return self._post_clean_text(text)

        logger.warning(
            f"Native text too short ({len(text)} chars). "
            f"Triggering OCR fallback..."
        )

        text = OCRProcessor.extract_text_from_pdf_smart(content)

        if len(text) < self.min_text_length:
            raise ValueError(
                f"Extracted text too short after OCR ({len(text)} chars)"
            )

        return self._post_clean_text(text)

    def _post_clean_text(self, text: str) -> str:
        """Final cleanup pass for LLM consumption"""
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]{2,}', ' ', text)
        return text.strip()


    def extract_json_from_llm_response(self, text: str) -> dict:
        text = text.strip()

        # Strategy 1: Direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Strategy 2: Markdown code block
        code_block_match = re.search(
            r'```(?:json)?\s*({[\s\S]*?})\s*```',
            text,
            re.IGNORECASE
        )
        if code_block_match:
            try:
                return json.loads(code_block_match.group(1).strip())
            except json.JSONDecodeError:
                pass

        # Strategy 3: First valid JSON object
        brace_start = text.find('{')
        brace_end = text.rfind('}')
        if brace_start != -1 and brace_end > brace_start:
            candidate = text[brace_start:brace_end + 1]
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass

        logger.warning(f"JSON extraction failed. First 300 chars:\n{text[:300]}...")
        return {"requirements": []}

    # PROMPT ENGINEERING

    def _build_prompt(
            self,
            text: str,
            document_title: str,
            chunk_info: str = None
    ) -> str:
        chunk_context = f"\n{chunk_info}\n" if chunk_info else ""

        return f"""
You are the Compliance Head of a KSA Bank.

REGULATION DOCUMENT: {document_title}{chunk_context}

REGULATION TEXT:
{text}

================================================================================

Your task is to extract, analyze, and summarize requirements, internal actions, and compliance tasks arising from the regulation above.

IMPORTANT:
- If the regulation does NOT impose explicit legal obligations on banks, identify necessary INTERNAL compliance actions (e.g., awareness, documentation update, policy alignment, vendor communication).
- Do NOT invent penalties, timelines, or enforcement actions unless explicitly stated.
- All extracted items must be defensible during audit or regulatory review.
- Focus ONLY on actionable requirements - skip general background information.
- Each requirement must be distinct - do not repeat similar items.

Please follow the structure below:

1. List of Requirements / Actionable Tasks:
- requirement_text must closely mirror the wording and intent of the regulation.
- Describe WHAT is required, not HOW the bank must implement it internally.
- Do not assume systems, policies, committees, timelines, or documents unless explicitly stated in the regulation.
- Use precise verbs from the regulation (e.g., shall maintain, shall notify, shall provide, shall obtain, shall retain).
- Identify each regulatory requirement OR internal compliance task triggered by the regulation.
- Suggest the relevant department responsible for each item.

2. Risk Assessment:
- Assess the risk level (High / Medium / Low) based on impact and likelihood of misalignment or misinterpretation.

3. Repercussions:
- List potential repercussions such as regulatory observations, supervisory comments, audit findings, or operational confusion.
- Do NOT state monetary penalties unless explicitly mentioned.

4. Controls and KPIs:
- Identify controls required to ensure continued compliance or regulatory alignment.
- Suggest measurable KPIs ONLY where logically applicable.
- KPIs must be specific, calculable, and measurable (e.g., "Percentage of transactions screened within 24h" NOT "Improve screening")

5. Output Format:
- Present the output in a structured, Excel-ready format with the following columns:
  - requirement_text (the actual requirement/task)
  - department (responsible department)
  - risk_level (High/Medium/Low)
  - repercussions (potential consequences - array of strings)
  - controls (required controls - array of strings)
  - kpis (measurable indicators - array of strings)
  - reference (specific article/clause/paragraph from regulation)

6. Reference Field:
   - Must clearly point to the paragraph, clause, or section from which the task is derived
   - If no numbering exists, describe the paragraph location (e.g., "Final paragraph of Section 3.2")

Additional rules:
- Output in JSON format ONLY with structure: {{"requirements": [...]}}

- Each requirement object MUST have these exact fields:
  requirement_text, department, risk_level, repercussions, controls, kpis, reference

- repercussions, controls, and kpis MUST be arrays of strings (not single strings)

- Do NOT include general background information as a requirement.
- Do NOT repeat the same task.
- Keep language concise and professional.
- Output in English only.
"""

    def analyze_regulation(
            self,
            content: str,
            regulation_id: int,
            document_title: str = "Untitled Regulation"
    ) -> Dict[str, Any]:

        try:
            # Extract meaningful text
            text = self.normalize_input_text(content)

            # 🔍 LANGUAGE DETECTION (NEW)
            lang = detect_language(text)
            logger.info(
                f"Detected language for regulation {regulation_id}: {lang}"
            )

            if lang not in ("en", "ar"):
                logger.warning(
                    f"Unsupported or unclear language ({lang}) for regulation {regulation_id}"
                )

            # Optional: store language in DB / metadata
            # self.repo.update_document_language(regulation_id, lang)

            if len(text) < self.min_text_length:
                raise ValueError(
                    f"Insufficient text for analysis ({len(text)} chars)."
                )

            logger.info(
                f"Analyzing regulation {regulation_id} "
                f"[lang={lang}] with {len(text)} chars"
            )

            prompt = self._build_prompt(text, document_title)
            response_text = self._call_llm(prompt)

            analysis_result = self.extract_json_from_llm_response(response_text)
            unique_requirements = self._deduplicate_requirements(
                analysis_result.get("requirements", [])
            )

            return {"requirements": unique_requirements}

        except Exception as e:
            logger.exception(f"Analysis failed for regulation {regulation_id}")
            raise

    def analyze_regulation_chunked(
            self,
            chunks: List[Dict],
            regulation_id: int,
            document_title: str = "Untitled Regulation"
    ) -> Dict[str, Any]:
        """
        Analyze regulation in chunks with OCR fallback per chunk
        """
        all_requirements = []

        for chunk in chunks:
            try:
                chunk_info = f"[Chunk {chunk['chunk_num']} of {chunk['total_chunks']}]"
                logger.info(f"Analyzing {chunk_info} for regulation {regulation_id}...")

                # Critical: Apply OCR fallback to each chunk
                clean_text = self.normalize_input_text(chunk["text"])

                prompt = self._build_prompt(clean_text, document_title, chunk_info)
                response = self._call_llm(prompt)
                parsed = self.extract_json_from_llm_response(response)

                # Annotate with source chunk
                for req in parsed.get("requirements", []):
                    req["source_chunk"] = chunk["chunk_num"]
                    all_requirements.append(req)

                logger.info(
                    f"{chunk_info}: Found {len(parsed.get('requirements', []))} requirements"
                )

            except Exception as e:
                logger.error(f"Chunk {chunk.get('chunk_num', '?')} failed: {e}")
                continue

        unique_requirements = self._deduplicate_requirements(all_requirements)
        logger.info(
            f"Chunked analysis complete: {len(all_requirements)} raw → "
            f"{len(unique_requirements)} unique requirements"
        )
        return {"requirements": unique_requirements}


    def _deduplicate_requirements(self, requirements: List[Dict]) -> List[Dict]:
        seen = set()
        unique = []

        for req in requirements:
            text = self._normalize_text(req.get("requirement_text", ""))
            if text and text not in seen:
                seen.add(text)
                unique.append(req)

        return unique

    def _normalize_text(self, text: str) -> str:
        if not text:
            return ""
        text = text.lower()
        text = re.sub(r'\s+', ' ', text)
        return text.strip(' .,:;!?')


    def _call_llm(self, prompt: str) -> str:
        """
        Call OpenRouter API with corrected URL (no trailing spaces)
        """
        url = "https://openrouter.ai/api/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:3000",
            "X-Title": "Saudi Banking Compliance Copilot"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a senior banking compliance officer with deep expertise in "
                        "Saudi Arabian regulations (SAMA, CMA). You extract precise requirements "
                        "from regulatory texts without hallucination."
                    )
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,  # Lower for precision
            "max_tokens": 4000
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=120)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            return content.strip() if content else '{"requirements": []}'
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter API error: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"API response: {e.response.text[:500]}")
            raise

    # UTILITIES

    def get_text_stats(self, content: str) -> Dict:
        """
        Get statistics about extracted text (after OCR if needed)
        """
        try:
            text = self.normalize_input_text(content)
            return {
                "characters": len(text),
                "estimated_tokens": len(text) // 4,
                "needs_chunking": len(text) > self.max_chunk_size,
                "sufficient_for_analysis": len(text) >= self.min_text_length,
                "ocr_used": len(text) > 500 and "ara" in text.lower()[:200]  # Heuristic
            }
        except Exception as e:
            return {
                "error": str(e),
                "characters": 0,
                "sufficient_for_analysis": False
            }
