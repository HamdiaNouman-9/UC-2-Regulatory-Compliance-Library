"""CBBSource — one CBB crawl mode, wearing the contract `config/sources/*.yml` expects.

WHY THIS FILE EXISTS
--------------------
`CBBCrawlerV2` predates the source-config flow and does not fit it in three ways:

  1. `fetch_documents(mode=...)` takes a MODE, not a `limit`. The composite calls
     `fetch_documents()` with no arguments, so an unwrapped CBBCrawlerV2 would run
     all seven modes as ONE source — one baseline, one gate, one change signal for
     the whole regulator. ONBOARDING's rule is the opposite: several small sources
     beat one large one, because each keeps its own baseline and one section
     breaking stays visible instead of being absorbed into a bigger number.

  2. It exposes NO `source_system` / `source_systems` attribute. Without one,
     `formfill/orch.py::_stored_for_source` logs "exposes no source_system --
     `disappeared` will be empty and the completeness gate is inert" and carries
     on. The export would work and be blind, which is the worst combination.

  3. It hashes with an inline `hashlib.md5(text)` rather than the library's single
     definition. `crawler/fingerprint.py` exists so there is exactly one answer to
     "what is a content_hash"; CBB was a fourth.

This adapter fixes all three WITHOUT touching `cbb_crawler.py`, so the existing
`cbb_monitoring` job keeps behaving exactly as it does today while the new path is
proven alongside it.

MODE 1 AND MODE 5 BOTH WROTE "CBB-Compliance"
---------------------------------------------
MEASURED by reading the code, 2026-08-20: mode 1 is Thomson Reuters *Regulations
and Resolutions*, but `_scrape_resolution` (cbb_crawler.py:196) stamps it
`source_system = "CBB-Compliance"` -- the same value mode 5 uses. It reads as a
copy-paste.

That is fatal to the new flow, which scopes `disappeared` and the completeness
gate on (regulator, source_system): split into two sources under one name, each
one's gate sees the other's stored rows and reports them missing. `change_signals
.yml` records MHRSD hitting exactly this.

Normally renaming a `source_system` is expensive -- it is an identity key, so
stored rows move. MEASURED 2026-08-20: **CBB has 0 rows in this database**, 0
`CBB-*` source_systems and 0 entries in run_history. So the rename costs nothing
today and cannot be done free again. `SOURCE_SYSTEM_OVERRIDE` below applies it.

If this repo is ever pointed at a database that DOES hold CBB rows under
"CBB-Compliance" from mode 1, drop the override rather than migrating the rows --
a wrong-but-consistent name is safer than a rename that splits a document's
history in two.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from crawler.fingerprint import stamp_content_hashes
from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)

#: Regulator name. "Full Name (ACRONYM)" is the library's rule, but CBB's existing
#: crawler already writes the bare form and `cbb_monitoring_crawler.py` agrees, so
#: this matches what is already there rather than inventing a third spelling.
REGULATOR = "Central Bank of Bahrain"

#: Mode 1 is Regulations and Resolutions, not Compliance. See the module docstring.
#: Keyed by mode so the rewrite is visible and reversible in one place.
SOURCE_SYSTEM_OVERRIDE: Dict[str, str] = {
    "1": "CBB-Regulations-and-Resolutions",
}


class CBBSource:
    """One mode of `CBBCrawlerV2`, as a source the config flow can build.

    Declared in `config/sources/cbb.yml` with `mode: custom`, so
    `build_source` imports and instantiates it per source with `init_kwargs`.
    """

    def __init__(
        self,
        mode: str,
        source_system: str,
        regulator: str = REGULATOR,
        max_volumes: Optional[int] = None,
        resume: bool = True,
    ):
        if not mode:
            raise ValueError("CBBSource needs a mode ('1', '2a', ... '5')")
        self.mode = str(mode)
        # Declared in the YAML rather than discovered, so the completeness gate
        # has an answer BEFORE the crawl runs -- a source that returns nothing
        # still has to be able to say what it would have returned under.
        self.source_system = source_system
        self.regulator = regulator
        # Mode 2c ONLY. The rulebook sidebar is thousands of sequential requests
        # at 1.2s each; uncapped it ran 80 minutes without finishing on
        # 2026-08-20 and the export had to be killed with nothing to show. Set it
        # in config/sources/cbb.yml to prove the flow, then remove it for the
        # real run. Ignored by every other mode.
        self.max_volumes = max_volumes
        # Mode 2c ONLY. MEASURED 2026-08-24 and 2026-08-25: the uncapped walk
        # died mid-"Volume 1—Conventional Banks" both times, 8+ hours in, with
        # no traceback -- something killed the process, not the crawl. With
        # resume=True (default) a re-run of this same source skips every
        # volume `cbb_test_crawlers/cbb_rulebook_crawler.py` already
        # checkpointed to disk instead of starting over from Common Volume.
        # Set False in config/sources/cbb.yml for a deliberate clean re-crawl.
        self.resume = resume
        self.last_result: dict = {}

    @property
    def source_systems(self) -> List[str]:
        """What this source writes under. Read by CompositeCrawler and by the
        completeness gate; a list because the contract allows several."""
        return [self.source_system]

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        # Imported here, not at module scope: cbb_crawler.py pulls in the
        # sub-crawlers under cbb_test_crawlers/ at import time, and a config
        # listing CBB sources should not pay that cost just to be read.
        from crawler.cbb_crawler import CBBCrawlerV2

        docs = CBBCrawlerV2().fetch_documents(
            mode=self.mode, max_volumes=self.max_volumes,
            resume=self.resume) or []

        # A mode that returns nothing is a FINDING, not a result. Every other
        # crawler in the library says this; CBB never did, which is one reason a
        # scheduled job could produce 0 rows without anyone noticing.
        if not docs:
            raise RuntimeError(
                f"CBB mode {self.mode} ({self.source_system}) returned no "
                f"documents. That is a failed read, not an empty section.")

        override = SOURCE_SYSTEM_OVERRIDE.get(self.mode)
        for d in docs:
            if override:
                d.source_system = override
            # The mode is what was actually run; keep it so a row can be traced
            # back to the code that produced it without guessing from the name.
            meta = dict(getattr(d, "extra_meta", None) or {})
            meta.setdefault("cbb_mode", self.mode)
            d.extra_meta = meta

        wrong = sorted({d.source_system for d in docs} - {self.source_system})
        if wrong:
            # Loud, because a source writing under a name the config did not
            # declare is invisible to the gate scoped on that name.
            raise RuntimeError(
                f"CBB mode {self.mode} declared source_system "
                f"{self.source_system!r} but produced {wrong!r}. Fix the config "
                f"or SOURCE_SYSTEM_OVERRIDE -- a mismatch here makes the "
                f"completeness gate scope onto rows that do not exist.")

        cap = limit if isinstance(limit, int) and limit > 0 else None
        if cap:
            docs = docs[:cap]

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": []},
            "by_source": {self.source_system: len(docs)},
        }
        logger.info("CBBSource[mode %s / %s] -> %d document(s)",
                    self.mode, self.source_system, len(docs))

        # The single exit. Every doc CBB builds already sets an md5 content_hash
        # and stamp_ never overwrites one, so this changes nothing for them --
        # it is the backstop for the branches that DO leave it empty (measured:
        # `_scrape_resolution` sets "" whenever the page yielded no text).
        return stamp_content_hashes(docs)


__all__ = ["CBBSource", "REGULATOR", "SOURCE_SYSTEM_OVERRIDE"]
