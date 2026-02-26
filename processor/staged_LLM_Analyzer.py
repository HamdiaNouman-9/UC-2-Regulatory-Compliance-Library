import os
import json
import logging
import re
import requests
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class StagedLLMAnalyzer:
    """
    4-stage regulatory analysis pipeline.
    Produces one structured row per extracted requirement.
    """

    def __init__(self, model: str = "deepseek/deepseek-v3.2"):
        self.model = model
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("Missing OPENROUTER_API_KEY")

    # ------------------------------------------------------------------ #
    #  PUBLIC ENTRY POINT                                                  #
    # ------------------------------------------------------------------ #

    def analyze(
            self,
            text: str,
            regulation_id: int,
            document_title: str,
            regulator: str = "",
            reference: str = "",
            publication_date: str = "",
    ) -> List[Dict]:
        # Stage 1: Extract
        s1_raw = self._call_llm(
            self._prompt_stage1(text, document_title, regulator, reference, publication_date),
            temperature=0.1
        )
        s1_data = self._parse_json(s1_raw)
        if not s1_data.get("requirements"):
            logger.warning(f"Stage 1 returned no requirements for regulation {regulation_id}")
            return []

        # Stage 2+2.5: Normalize + classify
        s2_raw  = self._call_llm(self._prompt_stage2(json.dumps(s1_data, ensure_ascii=False)), temperature=0.1)
        s2_data = self._parse_json(s2_raw)

        # Stage 3: Control design — only for Ongoing_Control obligations
        ongoing_reqs = [
            r for r in s2_data.get("requirements", [])
            if any(
                ob.get("execution_category") == "Ongoing_Control"
                for ob in r.get("normalized_obligations", [])
            )
        ]
        if ongoing_reqs:
            s3_raw  = self._call_llm(
                self._prompt_stage3(json.dumps({"requirements": ongoing_reqs}, ensure_ascii=False)),
                temperature=0.25
            )
            s3_data = self._parse_json(s3_raw)
        else:
            s3_data = {"requirements": []}

        # Stage 4: Executive markdown
        s4_md = self._call_llm(
            self._prompt_stage4(s1_data, s2_data, s3_data, document_title),
            temperature=0.3,
            max_tokens=4000
        )

        return self._assemble_rows(s1_data, s2_data, s3_data, s4_md, regulation_id)

    # ------------------------------------------------------------------ #
    #  PROMPTS                                                             #
    # ------------------------------------------------------------------ #

    def _prompt_stage1(
            self,
            text: str,
            document_title: str,
            regulator: str = "",
            reference: str = "",
            publication_date: str = "",
    ) -> str:
        return f"""You are a senior regulatory compliance analyst.
Extract structured requirements and atomic obligations from the regulatory circular below.

━━━ EXTRACTION RULES ━━━
- Extract ONLY binding obligations that use words like: must, shall, required to, obligated to.
- Ignore explanatory text, preambles, definitions, and non-binding guidance.
- If a sentence contains more than one distinct action, split it into separate atomic obligations.
- Do not split obligations that share a single subject and are logically inseparable into one action.
- Preserve the exact regulatory meaning — do not paraphrase or interpret beyond what is written.
- Do not invent any content.
- Each obligation must be independently testable by an auditor.

━━━ GROUPING RULES ━━━
- Group obligations into logical requirement clusters by topic (e.g. cash management, staffing, pricing display, security, reporting).
- Each requirement group should contain between two and six obligations — never more than six.
- If the regulation has no explicit section numbers, infer groupings from semantic topic, not paragraph order.
- Aim for between four and eight requirement groups total regardless of document length.

━━━ DEDUPLICATION RULES ━━━
- Before returning, check every obligation against all others in the same requirement group.
- If two obligations share the same core action and subject, keep only one — discard the duplicate entirely.
- Do not extract the same sentence or list item twice even if it appears more than once in the source text.

━━━ LANGUAGE RULES ━━━
- The regulation text may be in Arabic or any other language.
- All output fields (obligation_text, requirement_title, source_reference) must be written in English.
- Translate accurately while preserving the exact regulatory meaning — do not summarize or interpret.

Return STRICT JSON only — no markdown, no explanation, no code fences:
{{
  "regulator": "{regulator}",
  "reference": "{reference}",
  "publication_date": "{publication_date}",
  "requirements": [
    {{
      "requirement_id": "REQ-001",
      "requirement_title": "",
      "obligations": [
        {{
          "obligation_id": "REQ-001-OB-001",
          "obligation_text": "",
          "source_reference": ""
        }}
      ]
    }}
  ]
}}

Document Title: {document_title}
Regulator: {regulator}
Reference Number: {reference}
Publication Date: {publication_date}

Regulation Text:
{text}
"""

    def _prompt_stage2(self, stage1_json: str) -> str:
        return f"""You are refining previously extracted regulatory obligations.

Tasks:
1. Remove exact duplicates.
2. Split any obligation that still contains more than one action.
3. Standardize obligation_type to exactly one of:
   Preventive | Detective | Governance | Reporting | Documentation
4. Assign criticality — exactly one of: High | Medium | Low
5. Assign evidence_expected — array from:
   Policy | Procedure | System Configuration | Log | Approval | Report | Contract | Record | Other
6. Assign test_method — one sentence: how an auditor verifies this.
7. Assign clarity_score (integer 1–5).
8. Set needs_manual_review: true only if obligation cannot be made atomic.
9. Assign execution_category — exactly one of:
   Ongoing_Control | One_Time_Implementation | One_Off_Reporting | Governance_Approval | Informational_No_Action

Rules:
- Do NOT invent obligations.
- Do NOT change regulatory meaning.
- Keep obligation_text close to original.
- All output must be in English.

Return STRICT JSON only:
{{
  "requirements": [
    {{
      "requirement_id": "",
      "requirement_title": "",
      "normalized_obligations": [
        {{
          "obligation_id": "",
          "obligation_text": "",
          "obligation_type": "",
          "criticality": "",
          "evidence_expected": [],
          "test_method": "",
          "clarity_score": 5,
          "needs_manual_review": false,
          "source_reference": "",
          "execution_category": ""
        }}
      ]
    }}
  ]
}}

Input:
<<<START>>>
{stage1_json}
<<<END>>>
"""

    def _prompt_stage3(self, stage2_ongoing_json: str) -> str:
        return f"""You are a senior banking internal controls architect.

For each obligation where execution_category = "Ongoing_Control", design one internal control.

Each control must include:
- control_title
- control_objective (one sentence)
- control_description (2-3 sentences)
- control_owner (realistic bank department)
- control_type: Preventive | Detective
- execution_type: Manual | Automated | Hybrid
- frequency (e.g. Daily | Weekly | Per-Transaction | Event-Driven)
- control_level: Policy | Process | System
- evidence_generated (what artifact proves this ran)
- key_steps (array of 3-5 steps)
- residual_risk_if_failed: Low | Medium | High

Rules:
- One control per Ongoing_Control obligation.
- For non-Ongoing obligations: include the obligation but set control: null.
- Controls must be auditable and specific.
- All output must be in English.

Return STRICT JSON only:
{{
  "requirements": [
    {{
      "requirement_id": "",
      "requirement_title": "",
      "obligations": [
        {{
          "obligation_id": "",
          "obligation_text": "",
          "execution_category": "",
          "control": {{ ... }} or null
        }}
      ]
    }}
  ]
}}

Input:
<<<START>>>
{stage2_ongoing_json}
<<<END>>>
"""

    def _prompt_stage4(self, s1: dict, s2: dict, s3: dict, document_title: str) -> str:
        return f"""You are compiling a formal enterprise regulatory impact document.

Using ONLY the structured data below, generate a professional markdown report with these sections:

## 1. Executive Summary
## 2. Requirement Overview
## 3. Obligation Inventory
(markdown table: Obligation ID | Text | Type | Criticality | Execution Category)
## 4. Execution Classification Summary
(markdown table: Category | Count | Primary Owner)
## 5. Control Engineering Summary
(markdown table: Control Title | Owner | Type | Execution | Frequency | Residual Risk)
## 6. Architectural & Operational Implications

Rules:
- Use only information from the inputs.
- Do not invent regulatory content.
- Professional, concise, enterprise tone.
- All output must be in English.
- Return formatted markdown only.

Document: {document_title}

Stage 1:
{json.dumps(s1, ensure_ascii=False)[:3000]}

Stage 2:
{json.dumps(s2, ensure_ascii=False)[:3500]}

Stage 3:
{json.dumps(s3, ensure_ascii=False)[:3000]}
"""

    # ------------------------------------------------------------------ #
    #  ASSEMBLY — one dict per requirement = one DB row                   #
    # ------------------------------------------------------------------ #

    def _assemble_rows(
        self,
        s1: dict, s2: dict, s3: dict,
        s4_md: str,
        regulation_id: int
    ) -> List[Dict]:

        s2_by_id = {r["requirement_id"]: r for r in s2.get("requirements", [])}
        s3_by_id = {r["requirement_id"]: r for r in s3.get("requirements", [])}

        rows = []
        is_first = True

        for req in s1.get("requirements", []):
            req_id  = req["requirement_id"]
            s2_req  = s2_by_id.get(req_id, {})
            s3_req  = s3_by_id.get(req_id, {})

            obligations = s2_req.get("normalized_obligations", req.get("obligations", []))

            criticality        = self._dominant(
                [ob.get("criticality") for ob in obligations],
                ["High", "Medium", "Low"]
            )
            execution_category = self._dominant(
                [ob.get("execution_category") for ob in obligations],
                ["Ongoing_Control", "One_Time_Implementation",
                 "One_Off_Reporting", "Governance_Approval", "Informational_No_Action"]
            )
            obligation_types = ", ".join({
                ob.get("obligation_type") for ob in obligations if ob.get("obligation_type")
            })

            analysis_json = {
                "requirement_id":    req_id,
                "requirement_title": req.get("requirement_title", ""),
                "obligations":       obligations,
                "controls":          [
                    ob.get("control") for ob in s3_req.get("obligations", [])
                    if ob.get("control")
                ],
            }

            rows.append({
                "regulation_id":      regulation_id,
                "requirement_id":     req_id,
                "requirement_title":  req.get("requirement_title", ""),
                "execution_category": execution_category,
                "criticality":        criticality,
                "obligation_type":    obligation_types,
                "analysis_json":      json.dumps(analysis_json, ensure_ascii=False),
                "stage1_json":        json.dumps(req,    ensure_ascii=False),
                "stage2_json":        json.dumps(s2_req, ensure_ascii=False),
                "stage3_json":        json.dumps(s3_req, ensure_ascii=False),
                "stage4_md":          s4_md if is_first else None,
                "schema_version":     "v2",
            })
            is_first = False

        return rows

    # ------------------------------------------------------------------ #
    #  HELPERS                                                             #
    # ------------------------------------------------------------------ #

    def _dominant(self, values: list, priority: list) -> str:
        present = {v for v in values if v}
        for p in priority:
            if p in present:
                return p
        return priority[-1] if priority else ""

    def _parse_json(self, text: str) -> dict:
        text = text.strip()
        text = re.sub(r'^```(?:json)?\s*\n?', '', text, flags=re.IGNORECASE | re.MULTILINE)
        text = re.sub(r'\n?```\s*$', '', text, flags=re.MULTILINE)
        text = re.sub(r',(\s*[}\]])', r'\1', text)
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse failed: {e} | preview: {text[:200]}")
            return {"requirements": []}

    def _call_llm(self, prompt: str, temperature: float = 0.1, max_tokens: int = 8000) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:3000",
            "X-Title": "Saudi Banking Compliance Copilot",
        }
        payload = {
            "model": self.model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a senior banking compliance officer specialising in SAMA and CMA regulations. "
                        "Return only valid JSON unless explicitly told otherwise. Never hallucinate. "
                        "Always respond in English regardless of the input language."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }
        try:
            resp = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers, json=payload, timeout=180
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            return content.strip() if content else "{}"
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter API error: {e}")
            raise