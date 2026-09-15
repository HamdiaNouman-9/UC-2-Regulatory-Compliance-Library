"""
Requirement/Activity versioning -- diffs a fresh Stage A/B extraction against
what's already stored for a regulation, and writes only what actually
changed. Separate from requirement_analyzer.py/activity_analyzer.py on
purpose: those stay DB-free (same reasoning as every other analyzer here),
this is the one place that owns the write decision.

THE LOGIC, in one line: ref_key is content-derived (see
requirement_analyzer.py's module docstring), so unchanged content re-hashes
to the same ref_key on every re-analysis. A row that already exists with that
ref_key needs no write at all; only genuinely new, vanished, or RESURRECTED
content produces one.

Requirement/Activity rows are permanent, content-addressed catalogs -- one
row per distinct ref_key, ever, never duplicated, never deleted. Version
lifecycle lives entirely in RequirementSpan/ActivitySpan
(migrations/2026-09-08_requirement_activity_spans.sql): a requirement/
activity can have MORE THAN ONE span, one per contiguous stretch of being
active, which is what lets content that was superseded reappear later
without hitting ref_key's UNIQUE constraint or misrepresenting the gap in
between as "always active" -- a single introduced/superseded pair on the
content row itself (the earlier design) could not represent that gap at all.

Three outcomes per ref_key, checked in this order:
    ref_key has an OPEN span already      -> unchanged, untouched
    ref_key exists but has no open span   -> RESURRECTED: reuse the same
                                              requirement_id/activity_id,
                                              open a new span, no content write
    ref_key has never been seen before    -> genuinely NEW: insert content
                                              row + its first span

    a previously-open ref_key missing from this run -> close its span
                                              (mark_*_superseded)

Activities are diffed the SAME way, but only ever for a requirement this run
actually touched (inserted or reactivated) -- an UNCHANGED requirement's
activities are left completely alone, on purpose. Re-running Stage B against
requirements the content diff says are identical is a separate, deliberate
operation (e.g. after a prompt improvement), not something a
content-triggered recrawl should do on its own.
"""

import hashlib
import logging
import re
from typing import Dict, List

logger = logging.getLogger(__name__)


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().casefold())


def compute_requirement_ref_key(regulation_id: int, source_reference: str, text: str) -> str:
    """REQ-{regulation_id}-{sha256(source_reference + "|" + normalized_text)[:10]}
    -- matches the scheme in requirement_analyzer.py's module docstring."""
    digest = hashlib.sha256(
        f"{(source_reference or '').strip()}|{_norm(text)}".encode("utf-8")).hexdigest()[:10]
    return f"REQ-{regulation_id}-{digest}"


def compute_activity_ref_key(requirement_id: int, suggested_activity_type: str, text: str) -> str:
    """ACT-{requirement_id}-{sha256(suggested_activity_type + "|" + normalized_text)[:10]}"""
    digest = hashlib.sha256(
        f"{(suggested_activity_type or '').strip()}|{_norm(text)}".encode("utf-8")).hexdigest()[:10]
    return f"ACT-{requirement_id}-{digest}"


def _sync_activities_for_requirement(
    repo,
    requirement_id: int,
    version_id: int,
    this_runs_activities: List[dict],
    counts: Dict[str, int],
) -> None:
    """The same unchanged/resurrected/new logic as the top-level requirement
    diff, one level down. Works unchanged whether requirement_id is brand
    new (both lookups below come back empty, so every wanted activity is
    "new") or reactivated (it may carry real history from an earlier span)
    -- no separate code path needed for either case, same as the caller
    doesn't need one either."""
    all_known = repo.get_all_activities_for_requirement(requirement_id)
    currently_active = repo.get_current_activities_for_requirement(requirement_id)
    seen_ref_keys = set()

    for act in this_runs_activities:
        if not act.get("activity_needed"):
            continue
        ref_key = compute_activity_ref_key(
            requirement_id, act.get("suggested_activity_type", ""), act.get("description", ""))
        seen_ref_keys.add(ref_key)

        if ref_key in currently_active:
            continue  # unchanged, untouched

        if ref_key in all_known:
            repo.open_activity_span(all_known[ref_key], version_id)
            counts["activities_reactivated"] += 1
            continue

        repo.insert_activity(
            requirement_id=requirement_id, version_id=version_id, ref_key=ref_key,
            title=act.get("title", ""), description=act.get("description", ""),
            suggested_department=act.get("suggested_department", ""), frequency=act.get("frequency", ""),
            frequency_type=act.get("frequency_type"), priority=act.get("priority", ""),
            evidence_expected=act.get("evidence_expected") or [],
            suggested_activity_type=act.get("suggested_activity_type", ""),
        )
        counts["activities_inserted"] += 1

    for ref_key, activity_id in currently_active.items():
        if ref_key not in seen_ref_keys:
            repo.mark_activity_superseded(activity_id, version_id)
            counts["activities_superseded"] += 1


def sync_requirements_and_activities(
    repo,
    regulation_id: int,
    version_id: int,
    requirements: List[dict],
    activities: List[dict],
) -> Dict[str, int]:
    """Writes the diff between `requirements`/`activities` (this run's Stage
    A/B output, keyed by requirement_local_id -- see requirement_analyzer.py/
    activity_analyzer.py) and what's currently active for `regulation_id`.

    Returns counts: {"requirements_inserted", "requirements_reactivated",
    "requirements_superseded", "requirements_unchanged",
    "activities_inserted", "activities_reactivated", "activities_superseded"}.
    """
    counts = {
        "requirements_inserted": 0, "requirements_reactivated": 0,
        "requirements_superseded": 0, "requirements_unchanged": 0,
        "activities_inserted": 0, "activities_reactivated": 0,
        "activities_superseded": 0,
    }

    # ALL content this regulation has ever had (open span or not) -- the
    # "has this exact text existed before" check that tells a resurrection
    # apart from genuinely new content.
    all_known = repo.get_all_requirements_for_regulation(regulation_id)
    # Only what's currently active -- what a fresh extraction's ref_keys are
    # actually diffed against.
    current = repo.get_current_requirements(regulation_id)

    activities_by_req = {}
    for a in activities:
        activities_by_req.setdefault(a.get("requirement_local_id"), []).append(a)

    seen_ref_keys = set()

    for r in requirements:
        ref_key = compute_requirement_ref_key(
            regulation_id, r.get("source_reference", ""), r.get("description", ""))
        seen_ref_keys.add(ref_key)

        if ref_key in current:
            counts["requirements_unchanged"] += 1
            continue

        if ref_key in all_known:
            requirement_id = all_known[ref_key]
            repo.open_requirement_span(requirement_id, version_id)
            counts["requirements_reactivated"] += 1
        else:
            requirement_id = repo.insert_requirement(
                regulation_id=regulation_id, version_id=version_id, ref_key=ref_key,
                title=r.get("title", ""), description=r.get("description", ""),
                source_reference=r.get("source_reference", ""),
                source_refs=r.get("source_refs") or [],
                requirement_type_name=r.get("requirement_type", ""),
                actor=r.get("actor", ""), nature=r.get("nature", ""),
                condition_text=r.get("condition", ""),
                cross_references=r.get("cross_references") or [],
                disposition=r.get("disposition", ""),
                disposition_reason=r.get("disposition_reason", ""),
            )
            counts["requirements_inserted"] += 1

        _sync_activities_for_requirement(
            repo, requirement_id, version_id,
            activities_by_req.get(r.get("requirement_local_id"), []), counts)

    # Whatever was active before this run but never re-appeared -- superseded
    # (span closed), not deleted. Its activities cascade the same way.
    for old_ref_key, requirement_id in current.items():
        if old_ref_key in seen_ref_keys:
            continue
        repo.mark_requirement_superseded(requirement_id, version_id)
        counts["requirements_superseded"] += 1
        for activity_id in repo.get_activities_for_requirement(requirement_id):
            repo.mark_activity_superseded(activity_id, version_id)
            counts["activities_superseded"] += 1

    logger.info(f"Requirement/Activity sync for regulation {regulation_id}, version "
               f"{version_id}: {counts}")
    return counts
