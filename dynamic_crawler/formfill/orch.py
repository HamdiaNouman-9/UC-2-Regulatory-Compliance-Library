"""NewOrchestrator — the orchestrator changes we agreed, as a subclass.

`orchestrator.py` and `storage/mssql_repo.py` still have uncommitted changes from
another session, so nothing here edits them. Everything is an override, which
also makes the diff reviewable: what follows IS the list of changes.

WHAT IS DIFFERENT FROM THE PARENT

1. ONE DOOR. `run_for_regulator` handles every regulator including CBB. There is
   no `run_for_cbb`, and no `if regulator_upper == "CBB"` fork.

2. classify_documents() REPLACES filter_new_documents(). Four outcomes instead of
   two — new / modified / unchanged / disappeared — decided by ONE configured
   identity key instead of three hardcoded per-regulator branches. It also sets
   the two keys the crawler cannot know, because they need a database lookup:
       extra_meta["monitoring_status"]      = "new" | "modified"
       extra_meta["existing_regulation_id"]
   Doing it here keeps crawlers DB-free, which they must be: formfill runs as a
   subprocess.

3. VERSIONING FOR EVERY REGULATOR. `_process_versioned_doc` is the parent's CBB
   path with the CBB check removed and its raw `UPDATE regulation_versions` SQL
   replaced by `repo.mark_all_versions_inactive()`.

4. THE COMPLETENESS GATE. A run may not mark anything disappeared unless the run
   itself is trustworthy: no bot-protection pages, no early stop, not capped, and
   the count within tolerance of the last good run. SDAIA returned 415/363/439 on
   three runs of identical code — a run that "loses" 52 documents is not a run
   where 52 were withdrawn.

5. THE TEXT DECISION. `extract_text_content_unified` is replaced by
   formfill/textinput.py: a gate (is there anything to analyse at all?) and then
   the HTML-vs-file choice — same content, send the HTML; different, SEND BOTH.

6. NO STRING BRANCHES. "regulatory returns" is not special-cased by name; a
   document is analysed when there is text to analyse and skipped when there is
   not, which the parent already does for short text.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from orchestrator.orchestrator import BaseOrchestrator, MIN_TEXT_LEN

from dynamic_crawler import crawl_absence
from dynamic_crawler.changesignal import (clean_fields, fields_of, files_of,
                                          find_existing as _shared_find_existing,
                                          identity_for as _shared_identity_for,
                                          identity_key)
from dynamic_crawler.formfill.textinput import decide_for_document
from utils.countries import tree_path as country_tree_path
from utils.file_links import normalise_all as normalise_files

logger = logging.getLogger(__name__)

# Percent spread against the last good run before a run is distrusted.
COUNT_TOLERANCE_PCT = 5.0


def _text(v) -> str:
    """A stripped string from anything a workbook cell can hold.

    `(v or "").strip()` is the obvious way to write this and it is WRONG for
    values that came back from Excel. pandas represents an empty cell as
    `float("nan")`, and **NaN is truthy** — so `or ""` never fires and `.strip()`
    is called on a float:

        run failed: 'float' object has no attribute 'strip'

    That one line blocked every GOSI and MOH re-run. It only bites on the SECOND
    pass, when a workbook written earlier is read back, which is exactly the
    change-detection path — the crawl works, the comparison against stored rows
    dies.
    """
    if v is None:
        return ""
    if isinstance(v, float):        # NaN, and any stray numeric cell
        return "" if v != v else str(v).strip()
    return str(v).strip()


class NewOrchestrator(BaseOrchestrator):
    def __init__(self, crawler, repo, downloader=None, *,
                 source_name: str = "unknown",
                 # None, not a literal tuple: a literal here is a SECOND copy of
                 # the default that _clean_identity can never fall through to, so
                 # it silently outranks DEFAULT_IDENTITY. That is exactly what
                 # happened when `title` was added on 2026-08-16 — the constant
                 # gained the field, this line did not, and every orchestrator
                 # built without an explicit identity kept keying on two fields
                 # while the sweep and promote keyed on three. The two then built
                 # different absence keys, so streaks recorded by one side were
                 # invisible to the other and no withdrawal could ever accumulate.
                 identity: Optional[tuple] = None,
                 version_key: Optional[str] = "reference_no",
                 analyse: bool = False,
                 limit: Optional[int] = None,
                 change_root=None,
                 **kw):
        # The analyzers are constructed by the parent's __init__ and are only
        # touched when analyse=True, so an analysis-free run costs nothing.
        super().__init__(crawler=crawler, repo=repo, downloader=downloader, **kw)
        self.source_name = source_name
        self.identity = self._clean_identity(identity)
        self.version_key = version_key or None
        self.analyse = analyse
        self.limit = limit
        # Where the per-document miss streaks live. Its own directory, not one a
        # change sweep also writes — see crawl_absence.CRAWL_ROOT.
        self.change_root = change_root
        self.report: Dict = {}
        # The folder walk is a get-then-insert across several repo calls, so a
        # lock inside the repo cannot make it safe. Two documents sharing a
        # parent folder would each find it missing and each create it, giving
        # one folder two ids and splitting the tree.
        self._folder_lock = threading.RLock()
        # (regulation_id, stored_meta, doc) for rows that are unchanged but have
        # no version token stored yet.
        self._token_backfill: List = []

    # ------------------------------------------------------------------ #
    #  IDENTITY + CLASSIFICATION                                          #
    # ------------------------------------------------------------------ #

    # Kept in step with changesignal.DEFAULT_IDENTITY, which is where the
    # reasoning lives. Title added 2026-08-16 by the lead's decision.
    DEFAULT_IDENTITY = ("document_url", "doc_path", "title")

    @staticmethod
    def _clean_identity(identity) -> tuple:
        """One string, a list or a tuple, all to a tuple of field names.

        A source YAML writes `identity: [reference_no]` or `identity: page`, so
        the value arrives in whatever shape yaml produced. The change sweep has
        to key a document exactly as this does or the two cannot be compared, so
        the shaping lives in the shared module and not here.
        """
        return clean_fields(identity) or NewOrchestrator.DEFAULT_IDENTITY

    def _identity_for(self, doc) -> tuple:
        """The identity fields for THIS document.

        One run can mix sources, so the fields come from the source that produced
        the document when it declared any, and from the run default otherwise.

        Delegated to `changesignal.identity_for` so `promote` resolves identity
        identically — the two used to have separate copies and they drifted.
        """
        return _shared_identity_for(doc, self.identity)

    def _identity_fields_of(self, doc) -> dict:
        """The configured identity of one document, as {field: value}."""
        return fields_of(doc, self._identity_for(doc))

    def _check_identities(self, docs: List) -> None:
        """Refuse a run in which a configured identity is empty on any document.

        Every field blank means every such document carries the SAME identity, so
        they match each other and the second overwrites the first. Fatal rather
        than skip-and-continue: a skipped document is also a document this run did
        not see, which would put it in `disappeared` and withdraw it because its
        key went missing.
        """
        bad = [d for d in docs if not any(self._identity_fields_of(d).values())]
        if bad:
            raise ValueError(
                f"{len(bad)} of {len(docs)} documents have an empty identity "
                f"{list(self._identity_for(bad[0]))} — e.g. "
                f"{[str(getattr(d, 'title', '?'))[:60] for d in bad[:3]]}")

    def _version_key_for(self, doc) -> Optional[str]:
        """The field the new-url tiebreak compares, per source.

        Read with `in` because a source may set it to null to switch the tiebreak
        off — `find_by_reference` searches the whole store, across sources, so a
        reference number that is only unique within one source must not drive it.
        """
        meta = getattr(doc, "extra_meta", None) or {}
        if "version_key" in meta:
            return meta["version_key"] or None
        return self.version_key

    def _identity_of(self, doc) -> tuple:
        """The identity as an ordered tuple — what logs and dedupe keys want."""
        return tuple(self._identity_fields_of(doc).values())

    def _find_existing(self, doc) -> Optional[dict]:
        """The stored row matching this document's configured identity.

        The default identity keeps using `find_by_identity`, which is the tested
        path every existing source runs on. Anything else needs the generic
        lookup, and a repo that does not offer one cannot honour the config —
        say so rather than silently classifying everything as new.
        """
        return _shared_find_existing(self.repo, doc, self.identity)

    def _hashless_unchanged(self, existing: dict, doc) -> bool:
        """Whether nothing OBSERVABLE moved, for when content_hash cannot be
        compared (missing on the stored row, the crawl, or both).

        `if old_hash and new_hash and old_hash == new_hash` — the rule this
        replaces one half of — treats an absent hash as "cannot match", which is
        `modified`. That is exactly the bug in crawler/fingerprint.py's
        docstring: every un-hashed source got reclassified `modified` on EVERY
        run, ten MOH documents reaching five versions of identical content
        before anyone noticed. Stamping hashes at the crawler's exit fixed new
        crawls; this covers the sources that still cannot produce one (SECP,
        Saudi Exchange — see fingerprint_fix_2026-08-16.md §3a) and the one-time
        backfill run every already-stored, not-yet-hashed row goes through.

        Compares title, doc_path and the file set (`files_of`, which already
        knows the document_url / extra_meta.attachment_links split — a document
        that gained or dropped a PDF has changed even if its title and primary
        url did not move). `reference_no` joins the check only when the stored
        row actually carries the column — `find_by_identity`'s two-column
        shortcut never selects it, so its ABSENCE there means "not fetched", not
        "blank", and must not read as a mismatch.

        Deliberately biased toward `modified`, not `unchanged`: with no hash to
        confirm content, this is the only signal left, and a false `unchanged`
        hides a real edit for good — nothing ever re-checks a document once it
        settles there. A false `modified` only costs one redundant version row.
        """
        if _text(existing.get("title")) != _text(getattr(doc, "title", "")):
            return False
        if self.repo._norm_doc_path(existing.get("doc_path")) != \
                self.repo._norm_doc_path(getattr(doc, "doc_path", None)):
            return False
        if files_of(existing) != files_of(doc):
            return False
        if "reference_no" in existing:
            if _text(existing.get("reference_no")) != _text(getattr(doc, "reference_no", "")):
                return False
        return True

    @staticmethod
    def _set_status(doc, monitoring_status: str) -> None:
        """Put the monitoring state in `status`, where the schema expects it.

        `regulations.status` is a real column — `_insert_regulation` reads
        `getattr(document, "status", "active")` — so the monitoring state belongs
        there rather than buried in extra_meta.

        THREE things wanted that one column and they are not the same thing:

          our lifecycle          active / inactive, used by the archive logic
          the REGULATOR's status "In-Force" / "Superseded", straight off SAMA's
                                 own table column
          the monitoring state   new / modified / unchanged

        The regulator's claim about its own document is not our record's state, so
        it moves to extra_meta["regulator_status"] and `status` becomes ours
        alone. Anything already in `status` from a form field is preserved there
        rather than being silently overwritten.

        A FOURTH MEANING, AND THE ONE THAT WINS
        ---------------------------------------
        `status` is the HUMAN REVIEW decision — active or reject — and it governs
        whether a crawled row is promoted into the main system. Nothing automated
        may write it, or the pipeline would be approving its own output.

        So the monitoring state goes to extra_meta["monitoring_status"] ONLY, and
        `status` is left EMPTY for a person to fill. Writing "new" here made a
        machine-generated value sit in the column a reviewer's decision belongs
        in, with no way to tell the two apart afterwards.
        """
        meta = doc.extra_meta = dict(getattr(doc, "extra_meta", None) or {})
        site_status = _text(getattr(doc, "status", ""))
        if site_status and site_status.lower() not in (
                "new", "modified", "unchanged", "active", "inactive", "withdrawn"):
            meta.setdefault("regulator_status", site_status)
        meta["monitoring_status"] = monitoring_status
        # Empty, not the monitoring state: a human sets active/reject here.
        doc.status = ""

    def classify_documents(self, docs: List) -> Dict[str, List]:
        """new / modified / unchanged / disappeared / not_reread.

        `modified` is decided on content_hash: same identity, different hash. When
        the hash matches we do nothing at all — that is the cheap common case and
        the reason a nightly run is minutes rather than hours.

        `not_reread` is only ever filled by a targeted run: the documents it
        chose not to open, neither compared nor counted as absent. Stored rows
        held back from `disappeared` for the same reason are counted in
        `_not_reread_stored` rather than mixed into a bucket of documents.
        """
        buckets = {"new": [], "modified": [], "unchanged": [], "disappeared": [],
                   "not_reread": []}
        seen_ids = set()
        # Pages a targeted run walked past without opening. Their stored
        # attachments are not produced by such a run either, so both halves are
        # kept out of `disappeared` below.
        not_reread_pages = set()
        self._not_reread_stored = 0
        self._token_backfill = []
        self._check_identities(docs)

        for doc in docs:
            existing = self._find_existing(doc)

            # A row this run did not open has no content to compare, and its
            # hash would be of the LISTING. Comparing it reads as an edit and
            # B2's refresh then writes the empty page over the stored one.
            if (getattr(doc, "extra_meta", None) or {}).get("detail_skipped"):
                for u in (getattr(doc, "document_url", ""),
                          getattr(doc, "source_page_url", "")):
                    if u:
                        not_reread_pages.add(_text(u).rstrip("/"))
                if existing is not None:
                    seen_ids.add(existing.get("id"))
                buckets["not_reread"].append(doc)
                continue

            # Tiebreak: a regulator that republishes at a NEW url would otherwise
            # look like one new document plus one disappearance. Same reference
            # number means it is the same document at a new address.
            version_key = self._version_key_for(doc)
            if existing is None and version_key:
                ref = getattr(doc, version_key, None)
                if ref:
                    existing = self.repo.find_by_reference(ref)

            if existing is None:
                doc.extra_meta = dict(getattr(doc, "extra_meta", None) or {})
                self._set_status(doc, "new")
                buckets["new"].append(doc)
                continue

            seen_ids.add(existing.get("id"))
            old_hash = _text(existing.get("content_hash"))
            new_hash = _text(getattr(doc, "content_hash", ""))
            doc.extra_meta = dict(getattr(doc, "extra_meta", None) or {})
            doc.extra_meta["existing_regulation_id"] = existing.get("id")

            # A hash built from the URL and link text cannot move when the file
            # behind an unchanged link is replaced. The server's version token
            # can, so either one moving means modified.
            old_meta = existing.get("extra_meta")
            old_token = str((old_meta or {}).get("version_token") or "") \
                if isinstance(old_meta, dict) else ""
            new_token = str(doc.extra_meta.get("version_token") or "")

            if old_hash and new_hash:
                hash_says_unchanged = old_hash == new_hash
            else:
                # Neither side can prove content moved by hash alone — one or
                # both are un-fingerprinted. Fall back to what IS observable
                # rather than defaulting to `modified`, which is the bug
                # crawler/fingerprint.py's docstring describes.
                hash_says_unchanged = self._hashless_unchanged(existing, doc)

            if hash_says_unchanged:
                if old_token and new_token and old_token != new_token:
                    self._set_status(doc, "modified")
                    buckets["modified"].append(doc)
                    continue
                # First sight of a token for a document already stored: record it
                # without reprocessing, so enabling the probe costs one metadata
                # write per document instead of a full reclassification.
                if new_token and not old_token:
                    self._token_backfill.append((existing.get("id"),
                                                 dict(old_meta or {}), doc))
                self._set_status(doc, "unchanged")
                buckets["unchanged"].append(doc)
            else:
                self._set_status(doc, "modified")
                buckets["modified"].append(doc)

        # Anything in the store for this source that this run did not see —
        # except what it deliberately did not look at. A document hanging off a
        # page a targeted run skipped is absent from the run because nothing
        # opened that page, which is not the same as gone from the site.
        for r in self._stored_for_source(docs):
            if r.get("id") in seen_ids:
                continue
            if not_reread_pages and any(
                    _text(r.get(k)).rstrip("/") in not_reread_pages
                    for k in ("source_page_url", "document_url")):
                self._not_reread_stored += 1
                continue
            buckets["disappeared"].append(r)

        return buckets

    def _apply_token_backfill(self) -> int:
        """Store first-seen version tokens on rows nothing else will write.

        An unchanged document is not otherwise touched, so without this the token
        would be re-read and discarded on every run and never become a baseline
        to compare against.
        """
        written = 0
        for regulation_id, stored_meta, doc in self._token_backfill:
            meta = dict(stored_meta or {})
            new_meta = getattr(doc, "extra_meta", None) or {}
            meta["version_token"] = new_meta.get("version_token", "")
            meta["hash_basis"] = new_meta.get("hash_basis", "")
            try:
                self.repo.update_regulation(
                    regulation_id,
                    extra_meta=json.dumps(meta, ensure_ascii=False, default=str))
                written += 1
            except Exception as e:
                logger.warning("could not store version token for %s: %s",
                               regulation_id, e)
        return written

    @staticmethod
    def _regulator_of(docs: Optional[List]) -> Optional[str]:
        """The one regulator this run's documents belong to, if it is one.

        Taken from the documents rather than from the run's name because these
        are the strings that get written to the row — a display name that differs
        by a word would scope the lookup to nothing.
        """
        names = {_text(getattr(d, "regulator", ""))
                 for d in (docs or [])} - {""}
        return names.pop() if len(names) == 1 else None

    def _stored_for_source(self, docs: Optional[List] = None) -> List[dict]:
        """What the library already holds for the sources this run covers.

        This used to read `self.repo.t`, an ExcelRepo-only table behind a
        hasattr. On MSSQL the guard was False, so `disappeared` was always empty
        and the completeness gate had nothing to gate.

        It then read `crawler.source_system`, which a composite of several sources
        does not have — so the lookup went out as None, both repos answered [] for
        a falsy source, and `disappeared` was silently empty again for every
        regulator built from a source config. Ask for every source it covers.

        Scoped by regulator when the documents agree on one, because
        `source_system` is not unique across regulators: two publish under "Rules
        and Regulations" and two under "Laws and Regulations". Unscoped, this
        bucket can hold a sibling regulator's library and offer it up as
        disappeared.
        """
        sources = [s for s in (getattr(self.crawler, "source_systems", None)
                               or [getattr(self.crawler, "source_system", None)])
                   if s]
        if not sources:
            logger.warning("%s exposes no source_system — `disappeared` will be "
                           "empty and the completeness gate is inert",
                           type(self.crawler).__name__)
            return []

        regulator = self._regulator_of(docs)
        finder = getattr(self.repo, "find_regulations_by_source", None)
        if not callable(finder):
            if hasattr(self.repo, "t"):
                return [r for r in self.repo.t["regulations"]
                        if r.get("source_system") in sources
                        and (not regulator or r.get("regulator") == regulator)]
            logger.warning("%s cannot list stored regulations — `disappeared` "
                           "will be empty and the completeness gate is inert",
                           type(self.repo).__name__)
            return []

        def rows_for(scope: Optional[str]) -> List[dict]:
            found, seen = [], set()
            for source in sources:
                for r in (finder(source, regulator=scope) if scope else finder(source)):
                    if r.get("id") not in seen:
                        seen.add(r.get("id"))
                        found.append(r)
            return found

        rows = self._within_crawled_folders(rows_for(regulator), docs)
        if regulator and not rows:
            # An empty bucket is the safe answer — nothing can be withdrawn from
            # it — but it must not be a silent one. Say whether the source really
            # holds nothing or the regulator string does not match the column.
            unscoped = rows_for(None)
            if unscoped:
                logger.warning(
                    "%d stored row(s) under %s, none of them under regulator %r — "
                    "`disappeared` is empty because the run's regulator name does "
                    "not match the stored one",
                    len(unscoped), sources, regulator)
        return rows

    @staticmethod
    def _parent_folder(doc_path) -> tuple:
        """The folder a document sits in: its doc_path minus its own leaf."""
        if isinstance(doc_path, str):
            try:
                doc_path = json.loads(doc_path)
            except Exception:
                return ()
        return tuple(str(x) for x in (doc_path or [])[:-1])

    def _within_crawled_folders(self, rows: List[dict], docs) -> List[dict]:
        """Keep only stored rows sitting in a folder THIS RUN actually walked.

        `source_system` is not fine-grained enough to scope `disappeared`. This
        is the third time that has bitten (see _stored_for_source's docstring for
        the first two). ZATCA is the third: its five forms all publish under
        regulator ZATCA + source_system "Rules and Regulations", so each form
        compared itself against the whole 151-document corpus and declared its
        four siblings withdrawn:

            taxes          34 found + 117 "disappeared"
            agreements     98 found +  53 "disappeared"
            ie_circulars   11 found + 140 "disappeared"      151 = all of ZATCA

        The folder a document sits in IS the form, and the run knows its own
        folders without anything having to be stored: ZATCA's forms land under
        "Zakat, Tax and Customs Regulations", "Tax and Customs Agreements" and
        the three "Information Exchange Portal/..." folders respectively.

        NOT a fixed doc_path depth. The three Information Exchange forms separate
        at crumb 4, but for the other two forms crumb 4 is already the document's
        own title — so any fixed depth is wrong for one group or the other.
        Parent-of-leaf is right for both because it is relative to each document.

        Fails SAFE in every direction: a run that returns nothing, or is limited,
        or skips a folder, narrows this set and therefore proposes FEWER
        withdrawals. The failure mode this replaces — proposing a sibling form's
        entire library as withdrawn — is the one that loses documents.
        """
        folders = {self._parent_folder(getattr(d, "doc_path", None))
                   for d in (docs or [])}
        folders.discard(())
        if not folders:
            # Nothing to scope by: either the run found nothing, or this crawler
            # does not build doc_paths. Leave the caller's rows alone rather than
            # silently emptying the bucket — the completeness gate handles the
            # empty-run case, and this must not become a second, quieter place
            # where `disappeared` disappears.
            return rows
        kept = [r for r in rows if self._parent_folder(r.get("doc_path")) in folders]
        if len(kept) != len(rows):
            logger.info("disappeared scope: %d of %d stored row(s) are in the "
                        "%d folder(s) this run walked", len(kept), len(rows),
                        len(folders))
        return kept

    # ------------------------------------------------------------------ #
    #  THE FOLDER TREE — folders are "F", the document's own node is "R"   #
    # ------------------------------------------------------------------ #

    def _get_or_create_compliance_category(self, hierarchy: list,
                                           for_regulation_id=None) -> int:
        """Same walk as the parent, but it types the nodes.

        `compliancecategory.type` is what the frontend uses to tell a folder from
        a regulation. The parent calls `insert_folder(title, parent_id)` and never
        passes the third argument, so every node took the default "F" — including
        the leaf, which since the doc_path change IS the document. Every document
        therefore rendered as an empty folder.

        The convention already exists in the repo (tests/push_vol7_draft.py):

            cat_type = "R" if (is_last and is_leaf) else "F"

        Intermediate nodes are folders; the last segment is the regulation.
        """
        with self._folder_lock:
            return self._walk_folders(hierarchy, for_regulation_id)

    def _walk_folders(self, hierarchy: list, for_regulation_id=None) -> int:
        parent_id = None
        last_index = len(hierarchy) - 1

        for i, title in enumerate(hierarchy):
            folder_id = self.repo.get_folder_id(title, parent_id)

            if folder_id is None and parent_id is not None:
                folder_id = self.repo.find_folder_in_subtree(title, parent_id)

            # Leaf rule, unchanged from the parent: never hand one document's node
            # to another. A same-named sibling is created instead.
            #
            # UNLESS THE OCCUPANT IS THIS DOCUMENT. The rule cannot otherwise
            # tell "someone else's leaf" from "the leaf of the very document I am
            # re-processing", so on a re-run EVERY stored document tripped it and
            # took a fresh folder: measured 2026-08-16, one AML run added 11
            # duplicate leaves, and it would have added 11 more every run for
            # ever. Invisible until now because the workbook path resolves
            # folders in `promote`, not here — this walk only runs when the
            # orchestrator writes to the database directly.
            if folder_id is not None and i == last_index:
                occupied = self.repo.regulation_exists_for_category(folder_id)
                if occupied and not self._leaf_belongs_to(folder_id, for_regulation_id):
                    folder_id = None

            if folder_id is None:
                folder_id = self.repo.insert_folder(
                    title, parent_id, cat_type=("R" if i == last_index else "F"))
            parent_id = folder_id

        return parent_id

    def _leaf_belongs_to(self, folder_id: int, regulation_id) -> bool:
        """Is the regulation sitting in this leaf the one we are re-processing?

        Answered from the stored row rather than by trusting the caller: a
        document that has moved folders must still get a new leaf.
        """
        if not regulation_id:
            return False
        row = self.repo.get_regulation_by_id(regulation_id) or {}
        try:
            return int(row.get("compliancecategory_id") or 0) == int(folder_id)
        except (TypeError, ValueError):
            return False

    # ------------------------------------------------------------------ #
    #  THE COMPLETENESS GATE                                             #
    # ------------------------------------------------------------------ #

    def _inventory_hash(self, docs: List) -> str:
        # `field=value`, not the values alone: one run can carry two sources whose
        # identities are different fields entirely.
        keys = sorted(identity_key(self._identity_fields_of(d)) for d in docs)
        return hashlib.md5("\n".join(keys).encode("utf-8")).hexdigest()[:12]

    def _docs_by_source(self, docs: List) -> Dict[str, List]:
        """Documents grouped by the source that produced them.

        Every source the crawler was built with gets a key even when it produced
        nothing — a source that returned zero documents is the case this exists to
        make visible, and it is invisible in a group-by over the documents.
        """
        groups: Dict[str, List] = {
            name: [] for name in (getattr(self.crawler, "source_names", None) or [])}
        for d in docs:
            label = ((getattr(d, "extra_meta", None) or {}).get("crawl_source")
                     or self.source_name)
            groups.setdefault(label, []).append(d)
        return groups

    @property
    def _run_key(self) -> str:
        """The run_history key for THIS run — per FORM, not per regulator.

        One regulator can publish through several forms of very different sizes.
        ZATCA has five, and they all reported under the bare regulator name, so
        each run overwrote the previous form's baseline and the next form was
        measured against a number belonging to a different page:

            agreements     98 documents  -> writes baseline 98
            ie_guidelines   4 documents  -> "count moved 98 -> 4 (95.9%)"
                                            QUARANTINED, every single run

        Nothing was wrong. The counts were never comparable. Worse, the gate
        never settled: whichever form ran last set the baseline the next one
        failed against, so the check was permanent noise rather than a signal —
        and a check people learn to ignore is worse than no check.

        Only FORM runs are re-keyed. A crawler with no `hints_path` (a source
        config, a hand-written wrapper) keeps the bare source name, so its stored
        baseline still matches and it pays no reconciliation.
        """
        form = Path(str(getattr(self.crawler, "hints_path", "") or "")).stem
        return f"{self.source_name}/{form}"[:200] if form else self.source_name

    def _history_key(self, label: str) -> str:
        """run_history is per source. `run_history.source` is NVARCHAR(200) and
        record_run logs its own failures, so an overflow would cost the gate its
        baseline quietly — truncate here instead."""
        base = self._run_key
        key = base if label == self.source_name else f"{base}/{label}"
        return key[:200]

    def _last_good(self, key: str) -> Optional[dict]:
        return (self.repo.last_good_run(key)
                if hasattr(self.repo, "last_good_run") else None)

    def _count_problem(self, label: str, prev: int, now: int) -> Optional[str]:
        spread = abs(now - prev) / max(prev, 1) * 100
        if spread <= COUNT_TOLERANCE_PCT:
            return None
        return (f"{label}: count moved {prev} -> {now} ({spread:.1f}%), over the "
                f"{COUNT_TOLERANCE_PCT}% tolerance")

    def _note_count(self, label: str, prev: int, now: int,
                    problems: List[str]) -> None:
        """Record a count problem and which way it moved. `_baseline_verdict`
        needs the direction — a count that rose is still a safe baseline."""
        problem = self._count_problem(label, prev, now)
        if not problem:
            return
        problems.append(problem)
        self._count_problems.append(problem)
        if now > prev:
            self._grew.append(problem)

    def _baseline_verdict(self, problems: List[str], label: str = "") -> str:
        """Whether this count may be the baseline the next run compares against.

        Not the same question as whether the run may act on absences, and
        sharing one answer deadlocked the gate: a run distrusted for a count is
        also a run that refuses to remember what it saw, so the same problem is
        re-detected for ever. A prior that is too high only makes the withdrawal
        gate stricter; a prior that is too low is what opens it. So a count that
        rose is remembered and a count that fell waits for a person.
        """
        grew = getattr(self, "_grew", [])
        if label in getattr(self, "_unchecked", []) and problems:
            # The rise argument is about RAISING a prior, never about inventing
            # a first one: this source's count has not been checked against
            # anything, so a run carrying any problem must not set its baseline.
            return "QUARANTINED"
        return "PASS" if all(p in grew for p in problems) else "QUARANTINED"

    def check_run_trustworthy(self, docs: List) -> tuple:
        """(trustworthy, [reasons]). Only a trustworthy run may act on
        'disappeared'; an untrustworthy one still ingests new and modified.

        The count problems are also kept on their own: the withdrawal decision
        allows one document where this flat 5% allows none, and it needs to tell
        the two kinds of problem apart without parsing the message.
        """
        problems = []
        self._count_problems: List[str] = []
        # The count problems that were a RISE, and the sources this run had no
        # baseline for, so their count was never checked.
        self._grew: List[str] = []
        self._unchecked: List[str] = []
        crawl = getattr(self.crawler, "last_result", None) or {}
        run = crawl.get("run") or {}

        blocked = run.get("blocked_pages", 0)
        if blocked:
            problems.append(f"{blocked} page(s) came back as a bot-protection challenge")
        for w in run.get("warnings", []) or []:
            if "capped" in w.lower() or "stopped at page" in w.lower():
                problems.append(w[:120])

        # `_run_key`, not `source_name`: the "total" check is what produced
        # "count moved 98 -> 4" by measuring one form against another's baseline.
        last = self._last_good(self._run_key)
        if last and last.get("row_count"):
            self._note_count("total", last["row_count"], len(docs), problems)

        # Per source as well as in total. A composite logs a failed source and
        # carries on, so a small source dying entirely hides inside a 5% tolerance
        # measured against the regulator's whole inventory.
        groups = self._docs_by_source(docs)
        if len(groups) > 1:
            for label, group in groups.items():
                prev = (self._last_good(self._history_key(label))
                        or {}).get("row_count")
                if not prev:
                    self._unchecked.append(label)
                    continue
                self._note_count(label, prev, len(group), problems)
        return (not problems), problems

    def _source_gate(self, groups: Dict[str, List],
                     problems: List[str]) -> Dict[str, List[str]]:
        """Which gate problems stop which source, for the per-source history rows.

        The same attribution the withdrawal decision uses, with one addition: a
        `total` count problem stops only the sources that had no baseline of
        their own to be checked against. A source checked individually and found
        within tolerance is already answered; a source with no history was never
        checked, and letting a short run set its first baseline is what
        `last_good_run` exists to prevent.
        """
        verdicts = crawl_absence.source_verdicts(problems, list(groups))
        totals = [p for p in getattr(self, "_count_problems", [])
                  if p.startswith("total:")]
        unchecked = getattr(self, "_unchecked", [])
        return {label: [p for p in (verdicts.get(label) or [])
                        if p not in totals or label in unchecked]
                for label in groups}

    def _withdrawals(self, buckets: Dict[str, List], groups: Dict[str, List],
                     problems: List[str]) -> dict:
        """The withdrawal decision for the documents this run did not see.

        Call this BEFORE `record_run`, or the count baseline is this run's own row
        and the check can never fire. The count problems are dropped from the
        reasons because this layer re-asks that question with its own allowance.
        """
        store = crawl_absence.store_for(self.source_name,
                                        root=getattr(self, "change_root", None))
        crawl_absence.note_seen(store, buckets["new"] + buckets["modified"]
                                + buckets["unchanged"] + buckets["not_reread"],
                                self.identity)
        skipped = len(buckets["not_reread"]) + self._not_reread_stored
        block = crawl_absence.judge(
            store, buckets["disappeared"],
            identity=self.identity,
            labels=list(groups),
            counts={label: len(group) for label, group in groups.items()},
            priors={label: (self._last_good(self._history_key(label)) or {})
                    .get("row_count") for label in groups},
            problems=[p for p in problems
                      if p not in getattr(self, "_count_problems", [])],
            systems=crawl_absence.source_system_labels(self.crawler),
            # A run that walked past pages without opening them is not entitled
            # to call anything absent, the same rule as a sweep's --no-documents.
            targeted=(f"this run walked past {skipped} page(s) or row(s) without "
                      f"opening them; only a full crawl may propose"
                      if skipped else ""))
        store.save()
        return block

    # ------------------------------------------------------------------ #
    #  THE TEXT DECISION                                                  #
    # ------------------------------------------------------------------ #

    def extract_text_content_unified(self, doc, regulation_id: Optional[int] = None):
        """The gate, then HTML-vs-file. Replaces first-tier-wins."""
        dec = decide_for_document(
            doc,
            fetch_file_text=self._safe_pdf_text,
            fetch_page_text=self._safe_page_text,
            min_text_len=MIN_TEXT_LEN,
        )
        self._last_decision = dec
        logger.info("  %s", dec)
        if dec.skip:
            return None, None
        return dec.text, dec.content_type

    def _safe_pdf_text(self, url: str) -> Optional[str]:
        """Despite the name (kept to avoid touching the one call site above),
        this now dispatches by extension via _download_and_extract_file --
        .pdf still gets the OCR-aware path, .docx/.xlsx/.xls get
        processor.office_text_extractor, anything else returns None exactly
        as before."""
        try:
            return self._download_and_extract_file(url)
        except Exception as e:                     # never lose a document to a fetch
            logger.warning("  file fetch failed for %s: %s", url[:70], e)
            return None

    def _safe_page_text(self, url: str) -> Optional[str]:
        try:
            import requests
            from bs4 import BeautifulSoup
            from orchestrator.orchestrator import _FILE_DOWNLOAD_HEADERS
            r = requests.get(url, timeout=45, headers=_FILE_DOWNLOAD_HEADERS)
            soup = BeautifulSoup(r.text, "html.parser")
            for t in soup(["script", "style", "noscript", "header", "footer", "nav"]):
                t.decompose()
            return soup.get_text(" ", strip=True)
        except Exception as e:
            logger.warning("  page fetch failed for %s: %s", url[:70], e)
            return None

    # ------------------------------------------------------------------ #
    #  ONE PROCESSING PATH FOR EVERY REGULATOR                            #
    # ------------------------------------------------------------------ #

    def _process_single_doc(self, idx, doc, regulator_name):
        try:
            if isinstance(getattr(doc, "doc_path", None), list):
                # The stored row for THIS document, so the leaf rule can tell
                # "my own folder" from "someone else's".
                existing = self._find_existing(doc) or {}
                # COUNTRY goes in the tree only — never into doc_path, which
                # is an identity field. See config/countries.yml.
                doc.compliancecategory_id = self._get_or_create_compliance_category(
                    country_tree_path(doc.doc_path,
                                      getattr(doc, "regulator", "")),
                    for_regulation_id=existing.get("id"))
            else:
                doc.compliancecategory_id = None
        except Exception as e:
            logger.error("folder tree failed: %s", e)
            doc.compliancecategory_id = None

        # No `if regulator == CBB` and no `if category == "regulatory returns"`.
        # Everything takes the versioned path; whether it gets analysed is decided
        # by whether there is text, not by its name.
        self._process_versioned_doc(doc)

    def _process_versioned_doc(self, doc):
        meta = getattr(doc, "extra_meta", None) or {}
        status = meta.get("monitoring_status", "new")
        existing_id = meta.get("existing_regulation_id")
        new_hash = getattr(doc, "content_hash", "") or ""

        if status == "modified" and existing_id:
            # RETIRE the current version, do not COPY it.
            #
            # This used to insert an "archived" row holding the old content and
            # then insert the new one — two rows per change. But the old content
            # already HAS a row: the one that is active right now. Copying it
            # produced a duplicate every single time a document changed, which is
            # where the 554 identical pairs cleaned up on 2026-08-16 came from.
            # Deduping them was treating the symptom; this is the cause.
            #
            # `mark_all_versions_inactive` was already being called on the line
            # below, so the retire half was always there. Only the redundant copy
            # is removed.
            old = self.repo.get_regulation_by_id(existing_id) or {}
            # Read BEFORE retiring — afterwards nothing is active to find, so
            # there would be no way to tell "already had a version row" from
            # "never versioned at all" for the fallback branch just below.
            prev = self.repo.get_active_regulation_version(existing_id) or {}
            old_version_id = prev.get("version_id")
            self.repo.mark_all_versions_inactive(existing_id)
            if old_version_id is None:
                # No active row to retire: a regulation stored before versioning
                # existed, so its old content has never been snapshotted. Write
                # the archive row in that case only — otherwise the previous
                # content is lost rather than merely uncopied.
                old_version_id = self.repo.insert_regulation_version(
                    regulation_id=existing_id,
                    regulator=getattr(doc, "regulator", "") or "",
                    content_text=(old.get("extra_meta") or {}).get("content_text", "")
                                 if isinstance(old.get("extra_meta"), dict) else "",
                    content_html=old.get("document_html") or "",
                    content_hash=old.get("content_hash") or "",
                    updated_date=date.today(), status="inactive",
                    change_summary=f"archived {date.today().isoformat()}")
            # One shared instant for both writes below, so regulations.updated_at
            # reads exactly the moment this new version was created rather than
            # whatever moment its own separate UPDATE statement happens to run.
            modified_at = datetime.now(timezone.utc)
            version_id = self.repo.insert_regulation_version(
                regulation_id=existing_id,
                regulator=getattr(doc, "regulator", "") or "",
                content_text=meta.get("content_text", ""),
                content_html=getattr(doc, "document_html", "") or "",
                content_hash=new_hash, updated_date=date.today(), status="active",
                change_summary="content changed", created_at=modified_at)
            self.repo.update_regulation(existing_id, updated_at=modified_at,
                                        **self._modified_row_fields(doc, new_hash))
            regulation_id = existing_id
            # `or ""` is not belt-and-braces. dict.get returns its default only
            # when the key is ABSENT; a row fetched from SQL always has the key,
            # carrying None for a NULL column. Every regulation crawled before
            # content_hash was populated is exactly that case, so this slice
            # raised `'NoneType' object is not subscriptable` for all 370
            # documents of the 2026-08-16 batch — after the writes, so the data
            # was correct and only the logging died.
            self._log_step(regulation_id, "version", "SUCCESS",
                           f"new version {version_id} "
                           f"(was {(old.get('content_hash') or '')[:8]})")
        else:
            regulation_id = self.repo._insert_regulation(doc)
            doc.id = regulation_id
            version_id = self.repo.insert_regulation_version(
                regulation_id=regulation_id,
                regulator=getattr(doc, "regulator", "") or "",
                content_text=meta.get("content_text", ""),
                content_html=getattr(doc, "document_html", "") or "",
                content_hash=new_hash, updated_date=date.today(), status="active",
                change_summary="first version")
            self._log_step(regulation_id, "insert", "SUCCESS", "inserted")

        # Timed: this is the step that downloads and OCRs, so it is one of the two
        # places a slow run actually spends its time.
        with self._timed(regulation_id, "text_decision") as t:
            text, content_type = self.extract_text_content_unified(doc, regulation_id)
            dec = getattr(self, "_last_decision", None)
            t["status"] = "SKIPPED" if not text else "SUCCESS"
            t["message"] = str(dec)

        if not text:
            return

        # An attachment_links bundle's html/content_text is deliberately just
        # a thin wrapper (see utils/file_links.py's "combined" mode and
        # dynamic_crawler/formfill/pipeline.py's docstring: "the files ARE
        # it") -- never meant to stand in for the real content on its own. If
        # every attachment this row has was attempted (after retries, inside
        # _download_and_extract_file/_pdf) and none produced usable text, `text`
        # above is non-None only because decide() fell back to the html
        # wrapper -- analysing THAT would produce a plausible-looking but
        # misleading result, not a smaller-but-honest one. Refuse instead:
        # skip just the analysis step, not the insert/version work already
        # done above, so the row stays tracked for a later recrawl to retry.
        if (meta.get("attachment_links") and dec is not None
                and dec.attempted_files > 0 and dec.succeeded_files == 0):
            self._log_step(
                regulation_id, "requirement_activity_analysis", "FAILED",
                f"{dec.attempted_files} attachment(s) failed to produce usable text "
                f"after retries; refusing to analyse the html-only fallback for an "
                f"attachment-bundle document")
            return

        if not self.analyse:
            self._log_step(regulation_id, "requirement_activity_analysis", "SKIPPED",
                           f"analyse=False; would have sent {len(text):,} chars "
                           f"as {content_type}")
            return

        # REPLACES the old 4-stage staged_LLM_Analyzer / compliance_analysis
        # flow (_run_llm_analysis), which this method used to run first and
        # then run this alongside, additively, while the new tables were
        # still being proven out. That intermediate step is over: the
        # Requirement/Activity pipeline is now the only analysis path.
        # BaseOrchestrator still defines _run_llm_analysis/StagedLLMAnalyzer
        # for any caller that wants it directly, but the main per-document
        # flow no longer calls it, and compliance_analysis rows /
        # requirement_matching stop being written from here.
        self._run_requirement_activity_analysis(
            regulation_id=regulation_id, version_id=version_id, doc=doc, dec=dec)

    def _run_requirement_activity_analysis(self, regulation_id, version_id, doc, dec):
        """New Requirement/Activity pipeline: per-document bundle (html +
        every attachment, built from the SAME Decision already computed above
        -- no re-fetching), Stage A/B, then diff-and-write against what's
        already stored (see processor/requirement_activity_sync.py)."""
        from processor.requirement_analyzer import (
            RequirementAnalyzer, ChunkingError)
        from processor.activity_analyzer import ActivityAnalyzer
        from processor.llm_client import StructuralLLMError
        from processor.requirement_activity_sync import sync_requirements_and_activities

        documents = []
        if dec is not None and dec.html_text and len(dec.html_text.strip()) >= MIN_TEXT_LEN:
            documents.append({"source_document": "main_body", "text": dec.html_text})
        for name, file_text in (dec.file_parts if dec is not None else []):
            documents.append({"source_document": name, "text": file_text})
        if not documents:
            self._log_step(regulation_id, "requirement_activity_analysis", "SKIPPED",
                           "no document produced usable text")
            return

        try:
            requirement_types = self.repo.get_requirement_types()
        except Exception as e:
            self._log_step(regulation_id, "requirement_activity_analysis", "ERROR",
                           f"could not fetch requirement type lookup: {e}")
            return

        with self._timed(regulation_id, "requirement_activity_analysis") as t:
            try:
                stage_a = RequirementAnalyzer().extract_and_classify(
                    documents=documents, document_title=getattr(doc, "title", "") or "",
                    requirement_types=requirement_types,
                    regulator=getattr(doc, "regulator", "") or "",
                    reference=getattr(doc, "reference_no", "") or "",
                    publication_date=str(getattr(doc, "published_date", "") or ""),
                )
                requirements = stage_a["requirements"]
                activities = []
                if requirements:
                    # No activity_types / department_list -- neither is a
                    # controlled list for Activity any more (2026-09-14). See
                    # processor/activity_analyzer.py's module docstring.
                    activities = ActivityAnalyzer().design_activities(
                        requirements=requirements, chunk_texts=stage_a["chunk_texts"])
                counts = sync_requirements_and_activities(
                    self.repo, regulation_id, version_id, requirements, activities)
                t["message"] = str(counts)
            except (StructuralLLMError, ChunkingError) as e:
                # Deliberately NOT caught by a bare except below -- these mean
                # the whole run should stop being trusted (bad API key/
                # unreachable endpoint, or a chunker bug), not that this one
                # document had a content quirk. See processor/llm_client.py
                # and requirement_analyzer.py for why these two specifically
                # propagate instead of being swallowed per-chunk.
                t["status"] = "FAILED"
                t["message"] = f"{type(e).__name__}: {e}"
                logger.error(f"  Requirement/Activity analysis stopped for regulation "
                            f"{regulation_id}: {e}")
            except Exception as e:
                t["status"] = "FAILED"
                t["message"] = str(e)
                logger.error(f"  Requirement/Activity analysis failed for regulation "
                            f"{regulation_id}: {e}")

    @staticmethod
    def _modified_row_fields(doc, new_hash: str) -> dict:
        """What a modify refreshes on the `regulations` row.

        Updating content_hash alone left the new hash next to the old html, so
        the hash no longer described its own row. Empty values are dropped — a
        crawl that returns no title must not blank the stored one.
        """
        # An EMPTY hash must never overwrite a stored one — the same rule the
        # loop below applies to every other column, which content_hash was
        # silently exempt from. A crawler that omits content_hash (MOHCrawler
        # did) makes every document `modified`, and this line then wrote the
        # empty value back, so the next run could not match either. That is what
        # turned one missing field into a permanent re-versioning loop: 83 MOH
        # documents re-versioned every run, two rows each.
        fields = {}
        if new_hash:
            fields["content_hash"] = new_hash
        for column in ("title", "document_html", "published_date",
                       "reference_no", "category"):
            value = getattr(doc, column, None)
            if value not in (None, "", []):
                fields[column] = value
        meta = getattr(doc, "extra_meta", None)
        if isinstance(meta, dict) and meta:
            fields["extra_meta"] = json.dumps(meta, ensure_ascii=False, default=str)
        return fields

    def _log_step(self, regulation_id, step, status, message, duration_ms=None):
        try:
            self.repo._log_processing(regulation_id, step, status, message,
                                      duration_ms=duration_ms)
        except Exception:
            pass

    @contextmanager
    def _timed(self, regulation_id, step):
        """Time a step and log it whether it succeeds or raises.

        Nothing in the pipeline recorded step durations, so where a run spends
        its time was unanswerable with data. The expensive steps are the text
        decision (download + OCR) and the analysis (~4 minutes a document), and
        those are the two this wraps.

        The caller's own message is set via the yielded dict, so a step can still
        say WHAT it did as well as how long it took.
        """
        box = {"status": "SUCCESS", "message": ""}
        t0 = time.perf_counter()
        try:
            yield box
        except Exception as e:
            self._log_step(regulation_id, step, "FAILED", str(e)[:400],
                           duration_ms=(time.perf_counter() - t0) * 1000)
            raise
        else:
            self._log_step(regulation_id, step, box["status"], box["message"],
                           duration_ms=(time.perf_counter() - t0) * 1000)

    # ------------------------------------------------------------------ #
    #  THE RUN                                                            #
    # ------------------------------------------------------------------ #

    def run_for_regulator(self, regulator_name: str) -> Dict:
        docs = self.crawler.fetch_documents()
        # THE FILE RULE, applied where EVERY document passes: one file ->
        # document_url, several -> extra_meta.attachment_links, never both.
        # Done here rather than per crawler because each crawler had
        # invented its own spelling (org_pdf_link, arabic_pdf, pdf_link)
        # and a frontend had to know all of them. See utils/file_links.py.
        docs = normalise_files(docs)
        logger.warning("crawler returned %d documents", len(docs))

        trustworthy, problems = self.check_run_trustworthy(docs)
        inv = self._inventory_hash(docs)
        buckets = self.classify_documents(docs)
        tokens_stored = self._apply_token_backfill()

        # `_run_key` again: read the early-exit baseline from the same place the
        # run writes it, or a form matches a sibling's inventory hash (it never
        # will) and re-does the whole crawl every time.
        last = self._last_good(self._run_key)
        if (last and last.get("inventory_hash") == inv
                and not any(buckets[k] for k in ("new", "modified", "disappeared"))):
            # This logged "nothing to do" and then did everything anyway.
            # Exits only when the buckets agree with the hash — an unchanged
            # inventory with pending work means a previous run died mid-way.
            logger.warning("inventory hash unchanged (%s) — nothing to do", inv)
            self.report = {
                "regulator": regulator_name,
                "source": self.source_name,
                "crawled": len(docs),
                "classified": {k: len(v) for k, v in buckets.items()},
                "processed": 0,
                "limit": self.limit,
                "analyse": self.analyse,
                "skipped": "inventory hash unchanged since last good run",
                "inventory_hash": inv,
                "run_trustworthy": trustworthy,
                "gate_problems": problems,
                "disappeared_actioned": False,
                # Nothing is absent on this path, but the streak memory is still
                # written: a run that recorded nothing leaves every document
                # unattributed, and an unattributed absence can never be judged.
                "withdrawals": self._withdrawals(
                    buckets, self._docs_by_source(docs), problems),
                "version_tokens_stored": tokens_stored,
                "tables": self.repo.counts() if hasattr(self.repo, "counts") else {},
            }
            return self.report

        todo = buckets["new"] + buckets["modified"]
        if self.limit:
            todo = todo[:self.limit]

        # The parent's thread pool, not a loop of our own.
        #
        # `_process_docs` already does what this needs: DOC_MAX_WORKERS documents
        # in flight (default 4), LLM calls separately capped by
        # LLM_MAX_CONCURRENCY inside StagedLLMAnalyzer so more workers cannot
        # stampede OpenRouter, and one failed document never aborting the batch.
        # Looping serially here quietly gave all of that up — with analyse=true
        # at roughly four minutes a document, a 40-document run took 2.7 hours
        # instead of 40 minutes.
        #
        # Set DOC_MAX_WORKERS=1 to get the serial behaviour back.
        if todo:
            self._process_docs(todo, regulator_name)

        verdict = self._baseline_verdict(problems)
        groups = self._docs_by_source(docs)
        withdrawals = self._withdrawals(buckets, groups, problems)
        gate = self._source_gate(groups, problems)
        if hasattr(self.repo, "record_run"):
            self.repo.record_run(self._run_key, len(docs), inv, verdict,
                                 "; ".join(problems)[:400])
            # One row per source too, each with the verdict its OWN problems
            # earn. Stamping the run's verdict here froze a healthy source's
            # baseline for as long as a sibling was broken, and `last_good_run`
            # reads PASS only — so it then failed its own count check against a
            # baseline several runs old.
            if len(groups) > 1:
                for label, group in groups.items():
                    own = gate[label]
                    self.repo.record_run(self._history_key(label), len(group),
                                         self._inventory_hash(group),
                                         self._baseline_verdict(own, label),
                                         "; ".join(own)[:400])

        self.report = {
            "regulator": regulator_name,
            "source": self.source_name,
            "crawled": len(docs),
            "classified": {k: len(v) for k, v in buckets.items()},
            "processed": len(todo),
            "limit": self.limit,
            "analyse": self.analyse,
            "inventory_hash": inv,
            "run_trustworthy": trustworthy,
            # Whether this count becomes the baseline, which is a different
            # question: a run distrusted only because the inventory GREW is
            # still the best record of what the source now holds.
            "baseline_verdict": verdict,
            "gate_problems": problems,
            # Still False, and it is not the same claim as `withdrawals`: that
            # block is a proposal for a person, and no code here writes a status.
            "disappeared_actioned": False,
            "withdrawals": withdrawals,
            "version_tokens_stored": tokens_stored,
            "tables": self.repo.counts() if hasattr(self.repo, "counts") else {},
        }
        if len(groups) > 1:
            self.report["by_source"] = {k: len(v) for k, v in groups.items()}
            self.report["gate_by_source"] = {
                label: {"baseline_verdict": self._baseline_verdict(own, label),
                        "problems": own}
                for label, own in gate.items()}
        if buckets["not_reread"] or self._not_reread_stored:
            # A targeted run. Said out loud, because its `unchanged` count is
            # not the same claim a full crawl's is: most of this source was
            # never looked at.
            self.report["targeted_run"] = {
                "documents_not_reread": len(buckets["not_reread"]),
                "stored_rows_not_reread": self._not_reread_stored,
            }
        if buckets["disappeared"]:
            self.report["note"] = (
                f"{len(buckets['disappeared'])} document(s) were not seen this run. "
                + f"See `withdrawals`: {withdrawals['counts']}. Nothing is "
                  f"withdrawn by this run — the block is a proposal, and the "
                  f"status write needs a senior developer's approval.")
        return self.report


__all__ = ["NewOrchestrator"]
