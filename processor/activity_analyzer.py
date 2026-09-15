"""
Activity design -- Stage B of the new Requirement/Activity flow. Consumes
requirement_analyzer.py's output directly (requirements + chunk_texts).
Separate from processor/staged_LLM_Analyzer.py, same as requirement_analyzer.py.

One call per source chunk, not per requirement. Every requirement already
carries the chunk_id of the rule text it came from (requirement_analyzer.py
stamps this during extraction); this module groups by that id so a chunk's
rule text is sent to the LLM once per batch of requirements, not once per
requirement -- see the token-cost discussion this was built to answer.

activity_type and department are NOT controlled lists here -- reversed
2026-09-14, per business direction: the model suggests its own
suggested_activity_type / suggested_department freely, and neither list is
sent in the prompt at all. Activity.activity_type_id (a real FK into
ActivityType) and the old free-text-but-prompt-constrained Activity.department
were dropped in migrations/2026-09-14_drop_activity_type_fk.sql, replaced by
plain NVARCHAR suggested_activity_type / suggested_department columns with no
FK and no matching attempted against ActivityType/Department. Contrast with
requirement_analyzer.py's requirement_type, which is UNCHANGED -- still a
controlled list, still an FK -- this reversal is scoped to Activity's two
fields only, not the pipeline generally.

Ref Key:
Activity:    ACT-{requirement_id}-{sha256(suggested_activity_type + norm_text)[:10]}
"""

import json
import logging
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

from processor.llm_client import LLMClient, TruncatedResponseError, StructuralLLMError

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a senior compliance operations analyst. You decide whether a "
    "regulatory requirement needs a concrete activity to satisfy it, and design "
    "that activity when it does. You never invent requirements."
)


class ActivityAnalyzer:
    """Designs activities against already-extracted, already-classified
    requirements, grouped by the source chunk each one came from."""

    def __init__(self, model: str = "deepseek/deepseek-v3.2", max_workers: int = 4,
                 deterministic: Optional[bool] = None):
        self.max_workers = max_workers
        self.client = LLMClient(model=model, system_prompt=_SYSTEM_PROMPT,
                                 deterministic=deterministic)

    # ------------------------------------------------------------------ #
    #  PUBLIC ENTRY POINT                                                  #
    # ------------------------------------------------------------------ #

    def design_activities(
        self,
        requirements: List[Dict],
        chunk_texts: Dict[int, str],
        language: str = "English",
    ) -> List[Dict]:
        """Returns one entry per requirement: either
        {"requirement_local_id", "activity_needed": False, "why_not": "..."} or
        {"requirement_local_id", "activity_needed": True, "suggested_activity_type",
         "title", "description", "suggested_department", "frequency",
         "frequency_type", "priority", "evidence_expected", "needs_manual_review"}.

        Filtering to just the ones that get inserted into Activity is the
        caller's job -- both outcomes are real information a reviewer should
        be able to see, same reasoning as classification's needs_manual_review.

        No activity_types / department_list parameters -- neither is a
        controlled list any more (see module docstring). The model suggests
        both fields itself; nothing here validates them against a lookup
        table.
        """
        if not requirements:
            return []

        groups: Dict[int, List[dict]] = defaultdict(list)
        for r in requirements:
            groups[r.get("chunk_id")].append(r)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            results = list(pool.map(
                lambda item: self._run_group(item[0], chunk_texts.get(item[0], ""), item[1], language),
                groups.items()))

        merged = [row for group_rows in results for row in group_rows]
        needed = sum(1 for r in merged if r.get("activity_needed"))
        suspect = sum(1 for r in merged if r.get("non_bank_actor_suspected"))
        logger.info(f"Activity design: {len(requirements)} requirement(s) -> "
                   f"{needed} activity(ies), {len(merged) - needed} not needed")
        if suspect:
            logger.warning(f"{suspect} activity(ies) named a likely non-bank actor as "
                           f"suggested_department (Public Prosecution, Customs, a court, etc.) "
                           f"despite the prompt rule -- flagged needs_manual_review as a "
                           f"safety net, not silently trusted")
        return merged

    # Case-insensitive substrings that name a government/judicial body rather than
    # an internal function of the regulated entity. A safety net, not the primary
    # fix -- the prompt rule above is what should stop these from being generated
    # at all; this catches what gets through anyway. Deliberately over-inclusive
    # (a false positive here just means an extra manual look, not a lost row).
    # Deliberately NOT using bare "investigat" or "regulator" -- those are
    # substrings of entirely legitimate internal bank department names
    # ("Fraud Investigation Unit", "Regulatory Compliance", "AML
    # Investigations"), which would make this backstop noisier than the bug
    # it's catching. Multi-word phrases specifically naming an external body
    # are far less likely to collide with a real internal team name.
    #
    # Still meaningful now that suggested_department is free text with no
    # controlled list to constrain it -- if anything, this check now carries
    # MORE of the load than it did before 2026-09-14, since there is no list
    # membership left to structurally rule these out at all.
    _NON_BANK_ACTOR_MARKERS = (
        "public prosecution", "prosecutor", "prosecution", "customs", "court",
        "judicial", "judiciary", "investigative authority", "investigating authority",
        "state security", "ministry", "council of ministers", "government committee",
        "police", "law enforcement", "the regulator",
    )

    @classmethod
    def _is_non_bank_actor(cls, department: str) -> bool:
        d = (department or "").strip().casefold()
        return bool(d) and any(marker in d for marker in cls._NON_BANK_ACTOR_MARKERS)

    # ------------------------------------------------------------------ #
    #  PER-CHUNK GROUP, SELF-HEALING ON TRUNCATION -- NOT PRE-BATCHED      #
    # ------------------------------------------------------------------ #

    def _run_group(self, chunk_id, chunk_text: str, group: List[dict], language: str) -> List[dict]:
        """One call for the WHOLE chunk group, not fixed-size batches.

        THE BUG THIS FIXES. Pre-slicing into batches of 8 could split
        structurally-identical requirements from the same chunk into
        different, independent calls -- observed directly: three "SAMA may
        vary/increase X%" clauses (R0004, R0005, R0008) correctly got
        activity_needed=false in one batch, while a fourth, textually
        identical clause (R0010) landed in the NEXT batch with no visibility
        into how its siblings were judged, and fabricated a "bank submits a
        request" activity the source text never states. Keeping the whole
        group in one call is what let requirement_type classification and
        citation grouping self-correct earlier -- same fix, same reason.
        Truncation retry below still adaptively splits when a group is
        genuinely too large for one response; that split is reactive, not a
        pre-emptive guess at a safe size."""
        return self._activity_shard(chunk_text, group, language)

    def _activity_shard(self, chunk_text: str, batch: List[dict],
                        language: str, depth: int = 0) -> List[dict]:
        by_id = {r["requirement_local_id"]: r for r in batch}
        compact = [{"i": r["requirement_local_id"], "t": r["description"]} for r in batch]

        try:
            raw = self.client.complete(
                self._prompt_activity(
                    chunk_text,
                    json.dumps(compact, ensure_ascii=False, separators=(",", ":")),
                    language),
                temperature=0.1, max_tokens=6000, expect_json=True, label="activity-design",
            )
            deltas = self._parse_json(raw).get("a") or []
        except TruncatedResponseError:
            if len(batch) > 1 and depth < 3:
                mid = len(batch) // 2
                logger.warning(f"Activity design truncated for a {len(batch)}-item batch; "
                               f"splitting and retrying")
                first = self._activity_shard(chunk_text, batch[:mid], language, depth + 1)
                second = self._activity_shard(chunk_text, batch[mid:], language, depth + 1)
                return first + second
            # THE BUG THIS FIXES. This used to `return []` directly here and
            # in the except-Exception branch below -- bypassing _rehydrate
            # entirely, which is the ONLY place the "missing from response"
            # fallback lives. _rehydrate's backfill loop only runs over
            # whatever it's handed as `deltas`; an empty return before ever
            # calling it means no row -- not even a flagged one -- exists for
            # any id in this batch. Measured on a real run: the last 6 of 50
            # chunk-groups in a large document hit total call failures, and
            # 88 requirements silently had NO activity decision at all -- not
            # true, not false, absent -- with nothing in the output
            # indicating they were ever supposed to have one. Routing through
            # _rehydrate([], by_id, ...) reuses its existing per-id backfill
            # unchanged: an empty deltas list means every id in `by_id` is
            # "missing", which is exactly what happened.
            logger.error(f"Activity design truncated and cannot be split further; "
                        f"{len(batch)} requirement(s) will have no activity decision")
            return self._rehydrate([], by_id)
        except StructuralLLMError:
            # Same reasoning as requirement_analyzer.py's identical re-raise:
            # every retry already failed the same way, every remaining group
            # would too -- propagate rather than flagging this one group's
            # activities as merely "no decision".
            raise
        except Exception as e:
            logger.error(f"Activity design failed for batch: {e}")
            return self._rehydrate([], by_id)

        return self._rehydrate(deltas, by_id)

    def _rehydrate(self, deltas: List[dict], by_id: Dict[str, dict]) -> List[dict]:
        rows = []
        seen = set()

        for d in deltas:
            if not isinstance(d, dict):
                continue
            rid = d.get("i")
            if rid not in by_id:
                logger.warning(f"Activity design returned unknown requirement id {rid}; skipped")
                continue
            seen.add(rid)

            if not d.get("n"):
                rows.append({
                    "requirement_local_id": rid,
                    "activity_needed": False,
                    "why_not": d.get("w", ""),
                })
                continue

            # n=true with an empty/missing "acts" list is itself a malformed
            # response -- a requirement judged "needs an activity" with none
            # attached is not different from silence, and must not be read as
            # a considered zero-activity decision.
            acts = d.get("acts") or []
            if not acts:
                logger.warning(f"Requirement {rid}: n=true but no activities in 'acts'")
                rows.append({
                    "requirement_local_id": rid,
                    "activity_needed": False,
                    "why_not": "model said an activity was needed but returned none",
                    "needs_manual_review": True,
                })
                continue

            for act in acts:
                if not isinstance(act, dict):
                    continue
                dept = act.get("dept", "")
                non_bank_actor = self._is_non_bank_actor(dept)

                rows.append({
                    "requirement_local_id": rid,
                    "activity_needed": True,
                    "suggested_activity_type": act.get("c", ""),
                    "title": act.get("ti", ""),
                    "description": act.get("d", ""),
                    "suggested_department": dept,
                    "frequency": act.get("f", ""),
                    "frequency_type": act.get("ft") if act.get("ft") in ("Ongoing", "One-Time") else None,
                    "priority": act.get("p", ""),
                    "evidence_expected": act.get("e") or [],
                    "needs_manual_review": non_bank_actor,
                    "non_bank_actor_suspected": non_bank_actor,
                })

        # A requirement the model silently dropped from its response is not the
        # same as one it explicitly judged "no activity needed" -- flag it
        # rather than let it read as a considered decision nobody made.
        for rid in by_id:
            if rid not in seen:
                logger.warning(f"Requirement {rid} missing from activity design response")
                rows.append({
                    "requirement_local_id": rid,
                    "activity_needed": False,
                    "why_not": "no response from model",
                    "needs_manual_review": True,
                })

        return rows

    # ------------------------------------------------------------------ #
    #  PROMPT                                                              #
    # ------------------------------------------------------------------ #

    def _prompt_activity(self, chunk_text: str, requirements_json: str, language: str) -> str:
        return f"""<document>
{chunk_text}
</document>

<requirements>
{requirements_json}
</requirements>

<task>
For every requirement above, decide whether it needs a concrete activity -- a task or
control someone actually performs to satisfy it. If yes, design that activity.
Use the <document> above only as grounding for specifics (e.g. what a report is called,
who it goes to, on what basis) -- do not extract new requirements from it.
</task>

<decision_rules>
- FIRST, identify who performs the action -- the sentence's actual actor. An activity is
  needed ONLY when that actor is the regulated entity itself (the bank / financial
  institution / company the compliance program is for -- whichever the document
  addresses as "it", "the FI", "the bank", "the company"). If the actor is anyone else --
  the regulator, a court, Public Prosecution, an investigator, Customs, State Security, a
  government committee, a ministry -- there is NO activity for the regulated entity, no
  matter how the sentence is phrased.
- This applies EQUALLY to "shall/must" (mandatory) and "may" (discretionary) wording --
  verb mood does not change who the actor is. "The Public Prosecution SHALL issue a
  warrant" is exactly as not-the-regulated-entity's-task as "the regulator MAY increase
  this limit." A whole article can be mostly about criminal procedure, extradition,
  customs enforcement, or court process -- described in mandatory "shall" language
  throughout -- without a single line of it being the regulated entity's own action.
- If you are about to name a government body, court, or authority as an activity's
  "suggested_department" -- Public Prosecution, Customs, a ministry, a court, an
  investigative authority -- that IS the signal you should have marked
  activity_needed: false instead. suggested_department must always be an internal
  function of the regulated entity itself (Compliance, Legal, Treasury, etc.), never an
  external body's name.
- Watch specifically for a supervising or monitoring body's OWN actions described
  alongside the regulated entity's, inside the same article (e.g. "the agency shall refer
  / shall notify the Public Prosecution") -- those belong to that body, not to the
  regulated entity, even when most of the surrounding article is about the regulated
  entity's genuine obligations.
- Some requirements are purely informational, structural, or a definition/principle
  statement with no discrete task attached -- these need NO activity. Say so plainly.
- A requirement needs an activity only if there is something the regulated entity itself
  must actually DO to satisfy it (submit a report, run a check, obtain an approval, retain
  a record, respond to a request, etc.). Do not invent a task for an actor the text
  doesn't actually assign one to.
- A requirement CAN need more than one activity, when it genuinely demands distinct
  actions -- e.g. a clause requiring both an ongoing control AND a periodic report is
  two activities, not one. Do not split a single action into multiple activities just
  to fill the list; only split when the actions are genuinely separate.
</decision_rules>

<design_rules>
For each activity needed, in "acts":
- suggested_activity_type: your own short label for the kind of activity this is (e.g.
  "Control Testing", "Reporting Submission", "Policy Update") -- your best judgement, not
  a fixed list. Be consistent with how you'd label a genuinely similar activity elsewhere.
- title: short activity name.
- description: 2-3 sentences, what actually happens.
- suggested_department: the realistic responsible internal department, your best
  judgement -- never an external body (see decision_rules above).
- frequency: e.g. Daily | Weekly | Monthly | Per-Transaction | Event-Driven | One-Time.
- frequency_type: exactly "Ongoing" or "One-Time".
- priority: High | Medium | Low.
- evidence_expected: a SPECIFIC description of the artifact that proves this happened --
  drawn from the requirement's own wording and the document above (name, recipient,
  format, threshold, timeframe if stated). NOT a generic category like "Report" or "Log" --
  name the actual thing.
- Do not echo the requirement text back.

Do NOT invent requirements or change their meaning. Do NOT set any date fields --
scheduling is decided by a person, not by you.
</design_rules>

<language_rules>
The document is in {language}. ALL text you write must be in {language}. Do NOT translate.
</language_rules>

<output_format>
Return ONLY minified JSON on a single line: no line breaks, no indentation, no markdown,
no code fences, no explanation.
Fields: i = requirement id, n = activity_needed (bool), w = why_not (only if n is false),
acts = array of activities (only if n is true, one or more), each with:
c = suggested_activity_type, ti = title, d = description, dept = suggested_department,
f = frequency, ft = frequency_type, p = priority, e = evidence_expected
Schema:
{{"a":[{{"i":"","n":false,"w":""}},{{"i":"","n":true,"acts":[{{"c":"","ti":"","d":"","dept":"","f":"","ft":"","p":"","e":[""]}}]}}]}}
</output_format>"""

    # ------------------------------------------------------------------ #
    #  JSON PARSING                                                        #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_json(text: str) -> dict:
        cleaned = re.sub(r'^```(?:json)?\s*', '', (text or "").strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r'\s*```$', '', cleaned)
        try:
            return json.loads(cleaned)
        except Exception as e:
            logger.error(f"Failed to parse JSON: {e} | Raw: {cleaned[:200]}")
            return {}
