import os
import json
import logging
import re
import requests
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class GapAnalyzer:
    """
    Compares an uploaded internal policy document against extracted SAMA requirements
    and returns a coverage verdict per requirement.
    """

    def __init__(
        self,
        model: str = "deepseek/deepseek-v3.2",
        max_chunk_size: int = 12000
    ):
        self.model = model
        self.max_chunk_size = max_chunk_size
        self.openrouter_api_key = os.getenv("OPENROUTER_API_KEY")

        if not self.openrouter_api_key:
            raise ValueError("Missing OPENROUTER_API_KEY environment variable")

    # ------------------------------------------------------------------ #
    #  PUBLIC ENTRY POINT                                                  #
    # ------------------------------------------------------------------ #

    def analyze_gaps(
        self,
        uploaded_text: str,
        requirements: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Compare uploaded document text against a list of requirements.

        Args:
            uploaded_text:  Full text extracted from the user's uploaded PDF.
            requirements:   List of requirement dicts from compliance_analysis.analysis_json.
                            Each dict must have at least 'requirement_text'.

        Returns:
            List of result dicts with keys:
                requirement_text, coverage_status, evidence_text, gap_description
        """
        if not requirements:
            logger.warning("No requirements provided for gap analysis")
            return []

        if not uploaded_text or len(uploaded_text) < 50:
            logger.error("Uploaded document text is too short for gap analysis")
            raise ValueError("Uploaded document text is insufficient for analysis")

        # If uploaded doc is large, chunk it — but keep requirements intact
        if len(uploaded_text) > self.max_chunk_size:
            logger.info(
                f"Uploaded doc is large ({len(uploaded_text)} chars). "
                f"Processing in chunks."
            )
            return self._analyze_in_chunks(uploaded_text, requirements)

        return self._analyze_single(uploaded_text, requirements)

    # ------------------------------------------------------------------ #
    #  SINGLE PASS (doc fits in one prompt)                                #
    # ------------------------------------------------------------------ #

    def _analyze_single(
        self,
        uploaded_text: str,
        requirements: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:

        prompt = self._build_gap_prompt(uploaded_text, requirements)
        response_text = self._call_llm(prompt)
        results = self._parse_gap_response(response_text, requirements)
        return results

    # ------------------------------------------------------------------ #
    #  CHUNKED PASS (doc too large for single prompt)                      #
    # ------------------------------------------------------------------ #

    def _analyze_in_chunks(
        self,
        uploaded_text: str,
        requirements: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Split uploaded doc into chunks, run gap analysis on each chunk,
        then merge: if a requirement is 'covered' in ANY chunk, mark covered.
        Partial beats missing. Covered beats partial.
        """
        # Split uploaded text into chunks
        chunks = self._split_text(uploaded_text)
        logger.info(f"Split uploaded doc into {len(chunks)} chunks")

        # Track best verdict per requirement index
        # verdict priority: covered > partial > missing
        priority = {"covered": 2, "partial": 1, "missing": 0}
        best_results: Dict[int, Dict] = {}  # req_index → best result so far

        for chunk_num, chunk_text in enumerate(chunks, start=1):
            logger.info(f"Analyzing chunk {chunk_num}/{len(chunks)}")
            try:
                chunk_results = self._analyze_single(chunk_text, requirements)

                for idx, result in enumerate(chunk_results):
                    current_status = result.get("coverage_status", "missing")
                    current_priority = priority.get(current_status, 0)

                    if idx not in best_results:
                        best_results[idx] = result
                    else:
                        existing_priority = priority.get(
                            best_results[idx].get("coverage_status", "missing"), 0
                        )
                        if current_priority > existing_priority:
                            best_results[idx] = result

            except Exception as e:
                logger.error(f"Chunk {chunk_num} failed: {e}")
                continue

        # Fill in any missing indexes with default missing verdict
        final_results = []
        for idx, req in enumerate(requirements):
            if idx in best_results:
                final_results.append(best_results[idx])
            else:
                final_results.append({
                    "requirement_text": req.get("requirement_text", ""),
                    "coverage_status": "missing",
                    "evidence_text": None,
                    "gap_description": "Requirement not found in uploaded document."
                })

        return final_results

    # ------------------------------------------------------------------ #
    #  PROMPT BUILDER                                                      #
    # ------------------------------------------------------------------ #

    def _build_gap_prompt(
        self,
        uploaded_text: str,
        requirements: List[Dict[str, Any]]
    ) -> str:

        # Format requirements as numbered list for the LLM
        requirements_block = ""
        for idx, req in enumerate(requirements, start=1):
            req_text = req.get("requirement_text", "")
            requirements_block += f"{idx}. {req_text}\n"

        return f"""
You are a senior banking compliance auditor specializing in Saudi Arabian regulations (SAMA).

You have been given:
1. An INTERNAL POLICY DOCUMENT uploaded by the bank.
2. A list of REGULATORY REQUIREMENTS extracted from a SAMA regulation.

Your task is to assess how well the internal policy document covers each regulatory requirement.

================================================================================
INTERNAL POLICY DOCUMENT:
{uploaded_text}
================================================================================

REGULATORY REQUIREMENTS TO CHECK:
{requirements_block}
================================================================================

For EACH requirement, return one of three verdicts:
- "covered"  → The document clearly and fully addresses this requirement.
- "partial"  → The document touches on this requirement but does not fully address it.
- "missing"  → The document does not address this requirement at all.

For each verdict also provide:
- evidence_text: The exact excerpt from the uploaded document that supports your verdict.
                 If verdict is "missing", set this to null.
- gap_description: A concise explanation of what is lacking or missing.
                   If verdict is "covered", set this to null.

IMPORTANT RULES:
- Base your verdict ONLY on the uploaded document text provided. Do not assume anything.
- evidence_text must be a direct quote from the uploaded document, not paraphrased.
- Be strict: "covered" means the document explicitly addresses the requirement.
- Output ONLY raw JSON, no markdown, no code blocks.

Output format:
{{
  "results": [
    {{
      "requirement_text": "<exact requirement text>",
      "coverage_status": "covered | partial | missing",
      "evidence_text": "<direct quote from uploaded doc or null>",
      "gap_description": "<what is lacking or null>"
    }}
  ]
}}
"""

    # ------------------------------------------------------------------ #
    #  RESPONSE PARSER                                                     #
    # ------------------------------------------------------------------ #

    def _parse_gap_response(
        self,
        response_text: str,
        requirements: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Parse LLM response into list of gap result dicts.
        Falls back to marking all as missing if parsing fails.
        """
        try:
            # Strip markdown fences if present
            cleaned = re.sub(r'^```(?:json)?\s*', '', response_text.strip(), flags=re.IGNORECASE)
            cleaned = re.sub(r'\s*```$', '', cleaned)

            parsed = json.loads(cleaned)
            results = parsed.get("results", [])

            if not results:
                raise ValueError("Empty results array in LLM response")

            # Validate each result has required fields
            validated = []
            for item in results:
                validated.append({
                    "requirement_text": item.get("requirement_text", ""),
                    "coverage_status": item.get("coverage_status", "missing"),
                    "evidence_text": item.get("evidence_text") or None,
                    "gap_description": item.get("gap_description") or None
                })

            logger.info(f"Parsed {len(validated)} gap results from LLM response")
            return validated

        except Exception as e:
            logger.error(f"Failed to parse gap analysis response: {e}")
            logger.error(f"Raw response preview: {response_text[:300]}")

            # Fallback: mark all requirements as missing
            return [
                {
                    "requirement_text": req.get("requirement_text", ""),
                    "coverage_status": "missing",
                    "evidence_text": None,
                    "gap_description": "Analysis failed — could not parse LLM response."
                }
                for req in requirements
            ]

    # ------------------------------------------------------------------ #
    #  UTILITIES                                                           #
    # ------------------------------------------------------------------ #

    def _split_text(self, text: str) -> List[str]:
        """Split text into chunks by paragraph, respecting max_chunk_size."""
        paragraphs = text.split('\n\n')
        chunks, current_chunk, current_length = [], [], 0

        for para in paragraphs:
            if current_length + len(para) > self.max_chunk_size and current_chunk:
                chunks.append("\n\n".join(current_chunk))
                current_chunk = []
                current_length = 0
            current_chunk.append(para)
            current_length += len(para)

        if current_chunk:
            chunks.append("\n\n".join(current_chunk))

        return chunks

    def _call_llm(self, prompt: str) -> str:
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
                        "You are a senior banking compliance auditor with deep expertise in "
                        "Saudi Arabian regulations (SAMA). You assess policy documents against "
                        "regulatory requirements with precision and without hallucination."
                    )
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,
            "max_tokens": 8000
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=120)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            return content.strip() if content else '{"results": []}'
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter API error: {e}")
            raise