import os
import json
import logging
import re
import requests
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class GapAnalyzer:
    """
    Compares an uploaded document against extracted regulatory obligations
    and returns a coverage verdict per obligation.
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
        Compare uploaded document text against a list of obligations.

        Args:
            uploaded_text:  Full text extracted from the user's uploaded document.
            requirements:   List of obligation dicts. Each dict must have at least
                            'requirement_text' (used as the obligation text for analysis).

        Returns:
            List of result dicts with keys:
                obligation_text, requirement_text (alias), coverage_status,
                evidence_text, gap_description
        """
        if not requirements:
            logger.warning("No obligations provided for gap analysis")
            return []

        if not uploaded_text or len(uploaded_text) < 50:
            logger.error("Uploaded document text is too short for gap analysis")
            raise ValueError("Uploaded document text is insufficient for analysis")

        # If uploaded doc is large, chunk it — but keep obligations intact
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
        then merge: if an obligation is 'covered' in ANY chunk, mark covered.
        Partial beats missing. Covered beats partial.
        """
        chunks = self._split_text(uploaded_text)
        logger.info(f"Split uploaded doc into {len(chunks)} chunks")

        priority = {"covered": 2, "partial": 1, "missing": 0}
        best_results: Dict[int, Dict] = {}

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

        final_results = []
        for idx, req in enumerate(requirements):
            if idx in best_results:
                final_results.append(best_results[idx])
            else:
                ob_text = req.get("requirement_text", "")
                final_results.append({
                    "obligation_text":   ob_text,
                    "requirement_text":  ob_text,   # backward-compat alias
                    "coverage_status":   "missing",
                    "evidence_text":     None,
                    "gap_description":   "Obligation not found in uploaded document."
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

        obligations_block = ""
        for idx, req in enumerate(requirements, start=1):
            ob_text = req.get("requirement_text", "")
            obligations_block += f"{idx}. {ob_text}\n"

        return f"""
You are a senior compliance auditor specializing in regulatory analysis.

You have been given:
1. An UPLOADED DOCUMENT (this may be any type of document — a policy, procedure,
   framework, guidelines, manual, contract, report, or any other document).
2. A list of REGULATORY OBLIGATIONS extracted from a regulation.

Your task is to assess how well the uploaded document covers each regulatory obligation.

================================================================================
UPLOADED DOCUMENT:
{uploaded_text}
================================================================================

REGULATORY OBLIGATIONS TO CHECK:
{obligations_block}
================================================================================

For EACH obligation, return one of three verdicts:
- "covered"  → The document clearly and fully addresses this obligation.
- "partial"  → The document touches on this obligation but does not fully address it.
- "missing"  → The document does not address this obligation at all.

For each verdict also provide:
- evidence_text: The exact excerpt from the uploaded document that supports your verdict.
                 If verdict is "missing", set this to null.
- gap_description: A concise explanation of what is lacking or missing.
                   If verdict is "covered", set this to null.

IMPORTANT RULES:
- Base your verdict ONLY on the uploaded document text provided. Do not assume anything.
- evidence_text must be a direct quote from the uploaded document, not paraphrased.
- Be strict: "covered" means the document explicitly addresses the obligation.
- Do NOT assume the document is an internal policy — it could be any type of document.
- Output ONLY raw JSON, no markdown, no code blocks.

Output format:
{{
  "results": [
    {{
      "obligation_text": "<exact obligation text>",
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
        Returns both obligation_text and requirement_text (alias) for compatibility.
        """
        try:
            cleaned = re.sub(r'^```(?:json)?\s*', '', response_text.strip(), flags=re.IGNORECASE)
            cleaned = re.sub(r'\s*```$', '', cleaned)

            parsed = json.loads(cleaned)
            results = parsed.get("results", [])

            if not results:
                raise ValueError("Empty results array in LLM response")

            validated = []
            for item in results:
                ob_text = item.get("obligation_text") or item.get("requirement_text", "")
                validated.append({
                    "obligation_text":  ob_text,
                    "requirement_text": ob_text,   # backward-compat alias for GapResult model
                    "coverage_status":  item.get("coverage_status", "missing"),
                    "evidence_text":    item.get("evidence_text") or None,
                    "gap_description":  item.get("gap_description") or None
                })

            logger.info(f"Parsed {len(validated)} gap results from LLM response")
            return validated

        except Exception as e:
            logger.error(f"Failed to parse gap analysis response: {e}")
            logger.error(f"Raw response preview: {response_text[:300]}")

            return [
                {
                    "obligation_text":  req.get("requirement_text", ""),
                    "requirement_text": req.get("requirement_text", ""),
                    "coverage_status":  "missing",
                    "evidence_text":    None,
                    "gap_description":  "Analysis failed — could not parse LLM response."
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
            "X-Title": "Regulatory Compliance Copilot"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a senior compliance auditor with deep expertise in "
                        "regulatory analysis. You assess documents against regulatory "
                        "obligations with precision and without hallucination."
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