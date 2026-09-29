"""One shape for "did this document change?", for every regulator.

Every signal measured across the ten regulators is the same two steps with a
different field in the middle: read something the server maintains cheaply, then
shortlist what moved. This module is the part that does not vary — an
observation, a verdict, and what a run remembers. The adapters that produce
observations are separate.

It sits outside the formfill engine because for most regulators the sweep is an
HTTP client and a parse: no browser, no form, no shape.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import List, Optional

from dynamic_crawler import fingerprint

logger = logging.getLogger(__name__)

NEW = "new"
MODIFIED = "modified"
UNCHANGED = "unchanged"
UNKNOWN = "unknown"          # seen, but the signal could not be read
MISSING = "missing"          # in the store, not in a sweep that covers everything


# --------------------------------------------------------------------------- #
#  identity — read off the document, never off the regulator                   #
# --------------------------------------------------------------------------- #

def clean_fields(fields) -> tuple:
    """One string, a list or a tuple, all to a tuple of field names."""
    if isinstance(fields, str):
        fields = [fields]
    return tuple(str(f).strip() for f in (fields or ()) if str(f).strip())


def _meta_of(obj) -> dict:
    """extra_meta, whether it arrives as a dict or as the JSON text a column
    holds."""
    meta = (obj.get("extra_meta") if isinstance(obj, dict)
            else getattr(obj, "extra_meta", None))
    if isinstance(meta, str) and meta.strip():
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    return meta if isinstance(meta, dict) else {}


def declared_fields(obj, default=()) -> tuple:
    """The identity fields for THIS document or row.

    A regulator's config is a LIST of sources, so the choice belongs to the
    source that produced the document — a grid keyed on a circular number and a
    link walk with no number cannot share one. The crawler stamps the source's
    own choice on each document; the regulator-wide value is only the fallback.
    """
    return clean_fields(_meta_of(obj).get("identity_fields")) or clean_fields(default)


def resolve_field(obj, name):
    """One identity field's value, supporting `extra_meta.<key>`.

    A dotted name reads a key INSIDE extra_meta. That exists for the
    multi-attachment case: a card holding seven PDFs has no single
    `document_url` to be identified by — the identity is its folder plus the set
    of files it carries, and the files live in
    `extra_meta["attachment_links"]`.

    Only extra_meta is reachable this way, and only one level deep. Identity has
    to be comparable by both repos, and extra_meta is the one column that both
    already parse back into a dict.
    """
    if "." in name:
        head, key = name.split(".", 1)
        container = (obj.get(head) if isinstance(obj, dict)
                     else getattr(obj, head, None))
        if isinstance(container, str) and container.strip():
            try:
                container = json.loads(container)
            except Exception:
                container = {}
        return (container or {}).get(key) if isinstance(container, dict) else None
    return obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)


def fields_of(obj, fields) -> dict:
    """The identity of one document or stored row, as {field: value}."""
    out = {}
    for name in fields:
        value = resolve_field(obj, name)
        if isinstance(value, (list, tuple)):
            value = " > ".join(str(v) for v in value)
        out[name] = str(value).strip() if value is not None else ""
    return out



#: The identity every source uses unless it declares otherwise.
#
# TITLE IS PART OF IT, by the lead's decision 2026-08-16.
#
# Measured before the change, over 8,713 stored rows:
#   (document_url, doc_path) was ALREADY unique  -> 0 true duplicates
#   doc_path ends with the title on 96.1% of rows -> title is largely implied
#   33.4% of rows share a title with another row -> title alone identifies nothing
#
# So it adds no discriminating power, and it adds a failure mode: identity
# fields are ANDed, so an edited title makes a document a false `new` AND a
# false `disappeared`, and `disappeared` feeds the withdrawal gate. Renaming
# every Ministry of Commerce title on 2026-08-14 would, under this identity,
# have orphaned all 48 MC rows.
#
# Recorded here so the trade is visible: it is a deliberate choice to treat a
# changed title as a different document, not an oversight. If titles are ever
# edited in bulk again, expect phantom new/disappeared pairs and check the
# withdrawal gate before acting on them.
DEFAULT_IDENTITY = ("document_url", "doc_path", "title")


def identity_for(obj, default=DEFAULT_IDENTITY) -> tuple:
    """The identity FIELDS for one document or stored row.

    A source declares its own in `extra_meta["identity_fields"]` — the
    multi-attachment case does, because a row holding seven PDFs has no single
    document_url to be identified by. Everything else uses the default.
    """
    declared = resolve_field(obj, "extra_meta.identity_fields")
    if declared:
        return clean_fields(declared) or tuple(default)
    return tuple(default)



def files_of(obj) -> set:
    """Every file this document carries, from WHICHEVER column holds them.

    `document_url` and `attachment_links` are two spellings of one thing — the
    document's files — and which one is used depends only on how many there are:

        exactly one file   -> document_url,      attachment_links empty
        more than one      -> attachment_links,  document_url empty

    Read together, so an empty column simply contributes nothing and the other
    one carries the answer.
    """
    out = set()
    one = str(resolve_field(obj, "document_url") or "").strip()
    if one:
        out.add(one)
    many = str(resolve_field(obj, "extra_meta.attachment_links") or "").strip()
    for part in many.split("|"):
        part = part.strip()
        if part:
            out.add(part)
    return out


def _same_document(obj, row) -> bool:
    """Do these two carry any file in common?

    OVERLAP, not equality — because a document that gains or loses an attachment
    CROSSES BETWEEN THE TWO SPELLINGS above and would never match itself:

        stored, 1 file    document_url = fileA        attachment_links = -
        crawled, 2 files  document_url = -            attachment_links = fileA | fileB

    Exact comparison fails there and the crawl inserts a SECOND row for the same
    instrument — a phantom duplicate whose cause looks nothing like its symptom.
    On overlap it matches on fileA, which is the truth: the same document now has
    an extra annex, so it is a new VERSION and not a new document.

    Only ever applied within one `doc_path`, whose last crumb already separates
    `Laws` / `Regulation` / `Attachments`, so two genuinely different documents
    cannot be merged by a shared file.
    """
    mine, theirs = files_of(obj), files_of(row)
    return bool(mine and theirs and (mine & theirs))



def _by_files(repo, finder, obj, doc_path: str):
    """The stored row with the same PATH and TITLE that shares a FILE with this
    document -- the same instrument after it gained or lost an attachment.

    The identity rule, decided 2026-09-29:

        path + title + files all the same          -> same document
        path + title same, files OVERLAP           -> same document, new version
            (one PDF added, or one removed, at least one old PDF still there)
        path + title same, files ALL different     -> a NEW document
        path or title different                    -> a NEW document

    EVERY row with this path and title is checked, not just the first: QCB lists
    several different circulars under one title in one folder, and comparing
    only the first would miss the one this document actually continues.
    """
    if not doc_path or not callable(finder):
        return None
    fields = {"doc_path": doc_path}
    title = str(resolve_field(obj, "title") or "").strip()
    if title:
        fields["title"] = title
    try:
        return finder(fields, accept=lambda row: _same_document(obj, row))
    except TypeError:
        # A repo whose finder predates `accept` (the test doubles): it can only
        # return the first row, so check that one the old way.
        candidate = finder(fields)
        return candidate if candidate and _same_document(obj, candidate) else None
    except (ValueError, NotImplementedError):
        return None


def find_existing(repo, obj, default=DEFAULT_IDENTITY):
    """The stored row matching this document's configured identity, or None.

    THE ONE IMPLEMENTATION. It used to exist twice — once in
    `NewOrchestrator._find_existing` and once in `promote._find_existing` — and
    the copies did not agree. Every promote bug found on 2026-08-15 was a rule
    the orchestrator already had and promote did not:

      * a row with an EMPTY document_url skipped the check entirely and was
        re-inserted on every run (MHRSD duplicated 3, MC would have duplicated 16)
      * versions stacked because nothing compared content_hash
      * no version superseded its predecessor

    Two implementations of "is this the same document?" will always drift, and
    the drift is invisible until the library has duplicates in it. So both
    callers come here.

    The default identity keeps using `find_by_identity`, which is the tested path
    every existing source runs on. Anything else needs the generic lookup, and a
    repo that cannot offer one cannot honour the config — say so rather than
    silently classifying everything as new.
    """
    fields = fields_of(obj, identity_for(obj, default))
    finder = getattr(repo, "find_by_identity_fields", None)
    # `find_by_identity` is the two-column shortcut and cannot express a third
    # field, so it is only usable when the identity is exactly those two.
    if tuple(fields) == ("document_url", "doc_path"):
        hit = repo.find_by_identity(fields.get("document_url"),
                                    fields.get("doc_path"))
        # THE SAME FILE-OVERLAP FALLBACK AS BELOW, and it has to be here too.
        #
        # A document that drops from several files to ONE moves from the
        # attachment_links spelling back to document_url, so it arrives on THIS
        # branch while its stored row is on the other. Without this it is a
        # phantom duplicate in the direction nobody tests, because the exact
        # lookup on this path succeeds for every document that has not changed.
        _p = fields.get("doc_path", "")
        return hit or _by_files(repo, finder, obj, _p)
    if not callable(finder):
        # Say what is actually wrong. This used to read "it only supports
        # {default}", which since `title` joined the default prints the SAME
        # list on both sides — "cannot look up on [a,b,c]; it only supports
        # [a,b,c]". The repo's capability was never the default identity; it is
        # whether it implements the generic finder at all.
        raise NotImplementedError(
            f"{type(repo).__name__} has no find_by_identity_fields, so it can "
            f"only match on the two-column shortcut (document_url, doc_path). "
            f"This identity is {list(fields)}.")
    try:
        hit = finder(fields)
    except ValueError:
        # The source named a column the repo will not match on. Falling back to
        # doc_path alone is still far better than inserting a duplicate.
        return finder({"doc_path": fields.get("doc_path", "")})
    # NOTHING MATCHED EXACTLY — so look for the same path + title sharing a file
    # (an attachment added or removed). That is the ONLY fallback.
    #
    # NO TITLE-ONLY FALLBACK, removed 2026-09-29. "Same folder + same title =
    # same document at a new url" (added for MHRSD, 2026-08-16) merged QCB's
    # DIFFERENT circulars that share a title: promote skipped 14 of them, and
    # monitoring would have versioned each over the first. A document whose
    # files are ALL new is now a new document; its predecessor reads as
    # disappeared, which is never acted on without two trusted runs and a person.
    _p = fields.get("doc_path", "")
    return hit or _by_files(repo, finder, obj, _p)

def identity_key(fields: dict) -> str:
    """`field=value|field=value`, in the order the source declared the fields.

    `field=` and not the values alone: one run can carry two sources whose
    identities are different fields entirely.
    """
    return "|".join(f"{k}={v}" for k, v in fields.items())


# --------------------------------------------------------------------------- #
#  what a sweep produces                                                       #
# --------------------------------------------------------------------------- #

@dataclass
class Observation:
    """One document as this sweep saw it."""
    key: str
    fields: dict = field(default_factory=dict)
    identity_fields: tuple = ()
    token: str = ""
    basis: str = fingerprint.BASIS_NONE
    url: str = ""
    confirm_hash: str = ""
    title: str = ""


def observation_for(obj, default_fields=(), token=None, basis=None) -> Observation:
    """An Observation for a crawled document or a stored regulation row.

    Both shapes go through the same function so a document keys identically
    whichever side it is read from — that is what makes a sweep over the stored
    inventory comparable with a sweep over a live listing.
    """
    names = declared_fields(obj, default_fields)
    values = fields_of(obj, names)
    meta = _meta_of(obj)
    get = obj.get if isinstance(obj, dict) else lambda k, d=None: getattr(obj, k, d)
    return Observation(
        key=identity_key(values), fields=values, identity_fields=names,
        token=str(meta.get("version_token") or "") if token is None else str(token),
        basis=(str(meta.get("hash_basis") or fingerprint.BASIS_NONE)
               if basis is None else basis),
        url=str(get("document_url", "") or ""),
        title=str(get("title", "") or ""))


# --------------------------------------------------------------------------- #
#  the verdict                                                                 #
# --------------------------------------------------------------------------- #

def _short(token: str, keep: int = 28) -> str:
    """Enough of a token to see what moved.

    The version sits at the END of `{GUID},<version>`, so a plain head
    truncation prints two identical strings and hides the only part that changed.
    """
    return token if len(token) <= keep else f"{token[:8]}...{token[-12:]}"


def verdict_for(obs: Observation, stored: Optional[dict],
                confirm_required: bool = False) -> tuple:
    """(verdict, reason). One observation against what the last sweep stored.

    `unknown` exists so that a probe which did not run is never reported as a
    document which did not change — those two are indistinguishable in the
    output otherwise, and the second is the one people act on.
    """
    if stored is None:
        return NEW, "not in the change store"

    old = str(stored.get("token") or "")
    new = str(obs.token or "")

    if obs.basis == fingerprint.BASIS_FAILED:
        return UNKNOWN, "the probe did not run"
    if old and not new:
        return UNKNOWN, f"a token was stored and this run read none ({obs.basis})"
    if not old:
        # First token for a document already known: a baseline, not a change.
        # Enabling a signal must not reclassify a whole library.
        return UNCHANGED, "no stored token to compare against — baseline recorded"
    if old == new:
        return UNCHANGED, "token unchanged"

    if not confirm_required:
        return MODIFIED, f"token moved {_short(old)} -> {_short(new)}"

    # This source re-uploads its library in bulk, so a moved counter is not by
    # itself evidence that anything was edited.
    if not obs.confirm_hash:
        return UNKNOWN, "token moved and no confirming content hash was read"
    old_hash = str(stored.get("confirm_hash") or "")
    if not old_hash:
        return MODIFIED, "token moved; no stored content hash to compare against"
    if obs.confirm_hash == old_hash:
        return UNCHANGED, "token moved, content did not — a bulk republish"
    return MODIFIED, "token and content both moved"


# --------------------------------------------------------------------------- #
#  the interface, and the run                                                  #
# --------------------------------------------------------------------------- #

class ChangeSignal:
    """Tier one is `sweep`, tier two is `confirm`.

    `covers_inventory` is the discover/detect split. A sweep of a live listing
    sees everything the regulator publishes, so it may report a document as
    absent. A sweep over our own stored urls only ever looks at what it already
    knew — absence is not something it can observe, and saying "0 missing" there
    would be a zero that means "not measured".

    `confirm_required` is per source, not a property of the platform: one
    regulator's counters carry real per-document history, another's move all at
    once on a bulk re-upload.
    """

    name = "change-signal"
    confirm_required = False
    covers_inventory = False

    def sweep(self) -> List[Observation]:
        raise NotImplementedError

    def confirm_required_for(self, obs: Observation) -> bool:
        """Per observation, because one source can carry two kinds of token.

        GOSI's page date is shared by every instrument on the page, so only a
        content hash separates an amendment from a republish; the version counter
        on the documents that page links to is per file and is proof on its own.
        """
        return self.confirm_required

    def confirm(self, obs: Observation) -> Optional[str]:
        """A content hash for one shortlisted observation, or None when the
        token is proof on its own."""
        return None


def _confirm(signal: ChangeSignal, obs: Observation) -> str:
    try:
        return str(signal.confirm(obs) or "")
    except Exception as e:                  # an unreadable confirm is `unknown`
        logger.warning("confirm failed for %s: %s", obs.key[:80], e)
        return ""


def run_sweep(signal: ChangeSignal, store, record: bool = True) -> tuple:
    """Sweep, classify against the store, record, and report.

    Nothing here touches the library. The shortlist IS the product: what a
    caller does with `modified` — re-crawl, re-analyse — stays outside, and that
    is what keeps a sweep runnable with no database.
    """
    observations = list(signal.sweep() or [])
    buckets = {NEW: [], MODIFIED: [], UNCHANGED: [], UNKNOWN: [], MISSING: []}
    by_basis, collisions, seen, confirmed = {}, [], set(), 0
    prior = store.last_observed(signal.name) if hasattr(store, "last_observed") else None

    for obs in observations:
        by_basis[obs.basis] = by_basis.get(obs.basis, 0) + 1
        clash = store.collision(obs.key, obs.fields)
        if clash:
            collisions.append(clash)      # never merge two documents' history
            # Seen, and left alone. NOT dropped: `missing` is everything this
            # sweep did not see, so dropping it would offer a document the sweep
            # just read as a candidate for withdrawal.
            seen.add(obs.key)
            continue

        seen.add(obs.key)
        stored = store.get(obs.key)
        verdict, reason = verdict_for(obs, stored)
        if verdict == MODIFIED and signal.confirm_required_for(obs):
            obs.confirm_hash = _confirm(signal, obs)      # the shortlist only
            confirmed += 1
            verdict, reason = verdict_for(obs, stored, confirm_required=True)
            if verdict == UNKNOWN:
                # The confirm did not run, so this document was never judged.
                # Storing the token it moved TO would consume the change: the
                # next sweep compares new against new and reads `unchanged` for
                # good. The mirror of the store's rule that a failed probe must
                # not erase a token — this one must not advance one.
                obs.token = str((stored or {}).get("token") or "")

        buckets[verdict].append((obs, reason))
        if record:
            store.record(obs, verdict, signal.name)

    absent = sorted(store.keys() - seen) if signal.covers_inventory else []
    for key in absent:
        streak = (store.missed(key) if record
                  else int((store.get(key) or {}).get("misses") or 0) + 1)
        buckets[MISSING].append((key, f"absent from {streak} consecutive sweep(s)"))

    report = {
        "signal": signal.name,
        "source": store.source,
        "observed": len(observations),
        "counts": {k: len(v) for k, v in buckets.items()},
        "by_basis": by_basis,
        "without_token": sum(1 for o in observations if not o.token),
        "confirm_required": signal.confirm_required,
        "confirmed": confirmed,
        "collisions": collisions,
        # What the previous sweep saw, read BEFORE this one is noted: the
        # withdrawal gate refuses a sweep whose count collapsed.
        "observed_last": prior,
        "state_file": str(store.path),
    }
    if record and hasattr(store, "note_run"):
        store.note_run(signal.name, len(observations))
    if not signal.covers_inventory:
        report["counts"].pop(MISSING)
        report["missing"] = ("not measured — this sweep reads only documents "
                             "already stored, so it cannot see an absence")
    return report, buckets


__all__ = ["NEW", "MODIFIED", "UNCHANGED", "UNKNOWN", "MISSING",
           "Observation", "ChangeSignal", "observation_for", "verdict_for",
           "run_sweep", "clean_fields", "declared_fields", "fields_of",
           "identity_key"]
