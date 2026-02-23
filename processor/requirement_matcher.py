import os
import json
import logging
import re
import requests
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class RequirementMatcher:
    """
    Cross-references newly extracted SAMA requirements against the bank's
    existing internal COMPLIANCE_REQUIREMENT, DEMO_CONTROL, and DEMO_KPI tables.

    Flow per extracted requirement:
    1. Match requirement against existing COMPLIANCE_REQUIREMENT
    2. If fully_matched or partially_matched:
       - Fetch already-linked controls/KPIs for that matched requirement
       - Compare extracted controls against ALL existing controls
         → Already linked to this req  → skip
         → Matches existing but not linked yet → add new link
         → No match anywhere → insert as new suggested control + link
       - Same logic for KPIs
    3. If new requirement:
       - All extracted controls and KPIs go in as new suggested items
    """

    def __init__(self, model: str = "deepseek/deepseek-v3.2"):
        self.model = model
        self.openrouter_api_key = os.getenv("OPENROUTER_API_KEY")
        if not self.openrouter_api_key:
            raise ValueError("Missing OPENROUTER_API_KEY environment variable")

    # ================================================================== #
    #  PUBLIC ENTRY POINT                                                  #
    # ================================================================== #

    def match_requirements(
        self,
        regulation_id: int,
        extracted_requirements: List[Dict[str, Any]],
        existing_requirements: List[Dict[str, Any]],
        existing_controls: List[Dict[str, Any]],
        existing_kpis: List[Dict[str, Any]],
        linked_controls_by_req: Dict[int, List[int]],  # req_id → [control_ids already linked]
        linked_kpis_by_req: Dict[int, List[int]]       # req_id → [kpi_ids already linked]
    ) -> Dict[str, Any]:
        """
        Full matching: requirements → controls → KPIs.

        Returns dict with:
            requirement_mappings:   list → insert into sama_requirement_mapping
            control_links:          list → insert into DEMO_REQUIREMENT_CONTROL_LINK
            kpi_links:              list → insert into DEMO_REQUIREMENT_KPI_LINK
            new_controls_to_insert: list → insert into DEMO_CONTROL as IS_SUGGESTED=1
            new_kpis_to_insert:     list → insert into DEMO_KPI as IS_SUGGESTED=1
        """
        if not extracted_requirements:
            logger.warning(f"No extracted requirements to match for regulation {regulation_id}")
            return self._empty_result()

        requirement_mappings = []
        control_links        = []
        kpi_links            = []
        new_controls         = []
        new_kpis             = []

        for idx, extracted_req in enumerate(extracted_requirements, start=1):
            req_text     = extracted_req.get("requirement_text", "")
            req_controls = extracted_req.get("controls", [])  # strings from LLM
            req_kpis     = extracted_req.get("kpis", [])      # strings from LLM

            logger.info(f"[{idx}/{len(extracted_requirements)}] Requirement: {req_text[:80]}...")

            # ----------------------------------------------------------
            # STEP 1: Match requirement
            # ----------------------------------------------------------
            req_match = self._match_single_requirement(
                regulation_id=regulation_id,
                extracted_req_text=req_text,
                existing_requirements=existing_requirements
            )
            requirement_mappings.append(req_match)

            matched_req_id = req_match.get("matched_requirement_id")
            match_status   = req_match.get("match_status")

            # ----------------------------------------------------------
            # STEP 2: Match controls
            # ----------------------------------------------------------
            if req_controls:
                already_linked_ctrl_ids = linked_controls_by_req.get(matched_req_id, []) if matched_req_id else []

                for ctrl_idx, ctrl_text in enumerate(req_controls):
                    if not ctrl_text or not ctrl_text.strip():
                        continue

                    if existing_controls:
                        ctrl_result      = self._match_single_item(ctrl_text, existing_controls, "control")
                        ctrl_status      = ctrl_result["match_status"]
                        matched_ctrl_id  = ctrl_result.get("matched_id")
                        ctrl_explanation = ctrl_result.get("explanation", "")
                    else:
                        ctrl_status      = "new"
                        matched_ctrl_id  = None
                        ctrl_explanation = "No existing controls in DB."

                    if ctrl_status == "new" or matched_ctrl_id is None:
                        # No match anywhere — create new suggested control
                        new_controls.append({
                            "title":       ctrl_text[:200].strip(),
                            "description": ctrl_text,
                            "control_key": f"SAMA-AUTO-CTRL-{regulation_id}-{idx}-{ctrl_idx}",
                            "_req_id":     matched_req_id,
                            "_explanation": "No existing control matches this extracted control."
                        })
                        logger.info(f"  Control NEW: {ctrl_text[:60]}")

                    elif matched_ctrl_id in already_linked_ctrl_ids:
                        # Already linked to this requirement — nothing to do
                        logger.info(f"  Control ALREADY LINKED (id={matched_ctrl_id}): {ctrl_text[:60]}")

                    else:
                        # Matches existing control but not yet linked to this req
                        if matched_req_id:
                            control_links.append({
                                "compliancerequirement_id": matched_req_id,
                                "control_id":               matched_ctrl_id,
                                "match_status":             ctrl_status,
                                "match_explanation":        ctrl_explanation,
                                "regulation_id":            regulation_id
                            })
                            logger.info(f"  Control LINK (id={matched_ctrl_id}, {ctrl_status}): {ctrl_text[:60]}")

            # ----------------------------------------------------------
            # STEP 3: Match KPIs
            # ----------------------------------------------------------
            if req_kpis:
                already_linked_kpi_ids = linked_kpis_by_req.get(matched_req_id, []) if matched_req_id else []

                for kpi_idx, kpi_text in enumerate(req_kpis):
                    if not kpi_text or not kpi_text.strip():
                        continue

                    if existing_kpis:
                        kpi_result      = self._match_single_item(kpi_text, existing_kpis, "kpi")
                        kpi_status      = kpi_result["match_status"]
                        matched_kpi_id  = kpi_result.get("matched_id")
                        kpi_explanation = kpi_result.get("explanation", "")
                    else:
                        kpi_status      = "new"
                        matched_kpi_id  = None
                        kpi_explanation = "No existing KPIs in DB."

                    if kpi_status == "new" or matched_kpi_id is None:
                        new_kpis.append({
                            "title":       kpi_text[:200].strip(),
                            "description": kpi_text,
                            "kisetup_key": f"SAMA-AUTO-KPI-{regulation_id}-{idx}-{kpi_idx}",
                            "formula":     kpi_text,
                            "_req_id":     matched_req_id,
                            "_explanation": "No existing KPI matches this extracted KPI."
                        })
                        logger.info(f"  KPI NEW: {kpi_text[:60]}")

                    elif matched_kpi_id in already_linked_kpi_ids:
                        logger.info(f"  KPI ALREADY LINKED (id={matched_kpi_id}): {kpi_text[:60]}")

                    else:
                        if matched_req_id:
                            kpi_links.append({
                                "compliancerequirement_id": matched_req_id,
                                "kisetup_id":               matched_kpi_id,
                                "match_status":             kpi_status,
                                "match_explanation":        kpi_explanation,
                                "regulation_id":            regulation_id
                            })
                            logger.info(f"  KPI LINK (id={matched_kpi_id}, {kpi_status}): {kpi_text[:60]}")

        # Summary
        fully   = sum(1 for r in requirement_mappings if r["match_status"] == "fully_matched")
        partial = sum(1 for r in requirement_mappings if r["match_status"] == "partially_matched")
        new_req = sum(1 for r in requirement_mappings if r["match_status"] == "new")
        logger.info(
            f"Matching complete for regulation {regulation_id}: "
            f"Reqs: {fully} fully / {partial} partial / {new_req} new | "
            f"New ctrl links: {len(control_links)} | New KPI links: {len(kpi_links)} | "
            f"New controls: {len(new_controls)} | New KPIs: {len(new_kpis)}"
        )

        return {
            "requirement_mappings":   requirement_mappings,
            "control_links":          control_links,
            "kpi_links":              kpi_links,
            "new_controls_to_insert": new_controls,
            "new_kpis_to_insert":     new_kpis
        }

    # ================================================================== #
    #  REQUIREMENT MATCHING                                                #
    # ================================================================== #

    def _match_single_requirement(
        self,
        regulation_id: int,
        extracted_req_text: str,
        existing_requirements: List[Dict[str, Any]]
    ) -> Dict[str, Any]:

        existing_block = "\n".join(
            f"ID: {r['id']}\nTitle: {r['title']}\nDescription: {r['description']}\n---"
            for r in existing_requirements
        )

        prompt = f"""
You are a senior banking compliance officer.

NEW REQUIREMENT EXTRACTED FROM SAMA REGULATION:
{extracted_req_text}

================================================================================
EXISTING INTERNAL REQUIREMENTS:
{existing_block}
================================================================================

Does any existing requirement already cover this new one?

Verdicts:
- "fully_matched"     → Existing requirement clearly and completely covers the new one.
- "partially_matched" → Existing requirement partially covers the new one.
- "new"               → Nothing covers this at all.

Output ONLY raw JSON:
{{
  "match_status": "fully_matched | partially_matched | new",
  "matched_id": <integer or null>,
  "explanation": "<brief explanation>"
}}
"""
        response = self._call_llm(prompt)
        result   = self._parse_response(response)

        return {
            "regulation_id":              regulation_id,
            "extracted_requirement_text": extracted_req_text,
            "matched_requirement_id":     result.get("matched_id"),
            "match_status":               result.get("match_status", "new"),
            "match_explanation":          result.get("explanation", "")
        }

    # ================================================================== #
    #  CONTROL / KPI MATCHING                                             #
    # ================================================================== #

    def _match_single_item(
        self,
        extracted_text: str,
        existing_items: List[Dict[str, Any]],
        item_type: str  # "control" or "kpi"
    ) -> Dict[str, Any]:

        label = "CONTROL" if item_type == "control" else "KPI"

        existing_block = "\n".join(
            f"ID: {item['id']}\nTitle: {item['title']}\nDescription: {item['description']}\n---"
            for item in existing_items
        )

        prompt = f"""
You are a senior banking compliance officer.

NEW {label} EXTRACTED FROM A SAMA REGULATION:
{extracted_text}

================================================================================
EXISTING INTERNAL {label}S:
{existing_block}
================================================================================

Does any existing {label.lower()} already cover this new one?

Verdicts:
- "fully_matched"     → Existing {label.lower()} clearly and completely covers this.
- "partially_matched" → Existing {label.lower()} partially covers this.
- "new"               → Nothing covers this at all.

Output ONLY raw JSON:
{{
  "match_status": "fully_matched | partially_matched | new",
  "matched_id": <integer or null>,
  "explanation": "<brief explanation>"
}}
"""
        response = self._call_llm(prompt)
        return self._parse_response(response)

    # ================================================================== #
    #  SHARED PARSER                                                       #
    # ================================================================== #

    def _parse_response(self, response_text: str) -> Dict[str, Any]:
        default = {"match_status": "new", "matched_id": None, "explanation": "Parsing failed."}
        try:
            cleaned = re.sub(r'^```(?:json)?\s*', '', response_text.strip(), flags=re.IGNORECASE)
            cleaned = re.sub(r'\s*```$', '', cleaned)
            parsed  = json.loads(cleaned)

            match_status = parsed.get("match_status", "new")
            matched_id   = parsed.get("matched_id")
            explanation  = parsed.get("explanation", "")

            if match_status not in ("fully_matched", "partially_matched", "new"):
                return default

            if match_status == "new":
                matched_id = None

            return {
                "match_status": match_status,
                "matched_id":   matched_id,
                "explanation":  explanation
            }
        except Exception as e:
            logger.error(f"Failed to parse LLM response: {e} | Raw: {response_text[:200]}")
            return default

    # ================================================================== #
    #  LLM CALL                                                            #
    # ================================================================== #

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
                        "You are a senior banking compliance officer with deep expertise in "
                        "Saudi Arabian regulations (SAMA). You compare regulatory requirements, "
                        "controls, and KPIs precisely and without hallucination."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,
            "max_tokens":  300
        }
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            return content.strip() if content else '{"match_status": "new", "matched_id": null, "explanation": "No response"}'
        except requests.exceptions.RequestException as e:
            logger.error(f"OpenRouter API error: {e}")
            raise

    def _empty_result(self) -> Dict[str, Any]:
        return {
            "requirement_mappings":   [],
            "control_links":          [],
            "kpi_links":              [],
            "new_controls_to_insert": [],
            "new_kpis_to_insert":     []
        }