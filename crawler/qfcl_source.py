"""QFCLSource — one Qatar Financial Centre rulebook section, as a config source.

WHAT THIS IS
------------
The adapter between `crawler/tr_rulebook.py` (a sidebar walker that knows
nothing about this library) and the contract `config/sources/qfcl.yml` expects.
It is `crawler/cbb_source.py` wearing Qatar's names, and it exists for the same
three reasons that file lists:

  1. **One source per section, not one per regulator.** Four sections are
     declared separately in the YAML so each keeps its own baseline, its own
     completeness gate and its own change signal. A section that silently
     returns nothing is then VISIBLE, instead of being absorbed into a bigger
     number. ONBOARDING's rule, and the reason CBB is seven sources.

  2. **`source_system` is declared, not discovered.** Without a
     `source_systems` attribute `formfill/orch.py::_stored_for_source` logs
     "exposes no source_system -- `disappeared` will be empty and the
     completeness gate is inert" and carries on. The export would work and be
     blind, which is the worst of the two.

  3. **One definition of `content_hash`.** Everything leaves through
     `crawler.fingerprint.stamp_content_hashes`.

NAMES ARE SETTLED HERE, NOW
---------------------------
`source_system` is an identity key: stored rows move when it changes, and a
rename splits a document's version history in two. It is free to choose only
while the database holds zero rows under it, which is true today and will not be
true again after the first promote. The four names below are therefore final:

    QFC Law  ·  QFC Regulation  ·  QFCA Rules  ·  QFCRA Rules

`REGULATOR` must be spelled identically here, in `config/sources/qfcl.yml` and
in `config/countries.yml`. `country_for()` matches the exact string, and a
mismatch is the cheap-looking failure that is expensive to notice: every
document is still correct, so nothing complains, and the whole tree just sits at
the ROOT instead of under Qatar. `workbook check` warns on it; it does not
error.

DOC_PATH STARTS AT THE REGULATOR
--------------------------------
`utils.countries.tree_path` reads `doc_path[0]` as the regulator and prepends
the country, so:

    doc_path   = [REGULATOR, <section>, <book>, <part>, ..., <title>]
    tree       = Qatar > Qatar Financial Centre Legislation > QFC > ...

which is the manual library's shape node for node. (CBB puts "CBB Rulebook" in
that slot rather than its regulator name, which is why its tree does not get a
country. Not corrected here — that is CBB's identity key and this change was
scoped to touch nothing outside Qatar.)

LEAVES, PLUS THE FOLDERS THAT ARE THEMSELVES RULES
--------------------------------------------------
The walker emits folders too. MOST are dropped: a folder is a position in the
tree and `doc_path` already records it, so storing one as a regulation gives a
reviewer an entry to open with no instrument behind it. CBB shipped that bug for
months because it filtered on a field the dataclass does not have — the filter
below is on `is_folder`, which it does.

BUT "a folder holds no text of its own" IS FALSE ON THIS PLATFORM, and dropping
all of them lost real law. MEASURED 2026-09-18 across all four checkpoints: 784
folder pages carry body text, and for 486 of them that text is a numbered
operative rule that appears in NO other row. The clearest case:

    INMA 2.1.6 Firm must notify Authority of actual or potential breach
        (folder, 1,601 chars: "(1) An INMA firm must notify the Regulatory
         Authority if the firm becomes aware ...")
      └─ INMA 2.1.6 Guidance          (leaf, 157 chars — the only row we stored)

We were storing the guidance note and not the rule it explains. The site diff
did not catch it because the folder DOES exist in the stored tree — as a
`doc_path` crumb. Its text was simply nowhere.

WHAT SEPARATES THE TWO. Every page on this platform ends with its amendment
provenance ("Amended by QFCRA RM/2013-1 (as from 1st January 2015).", "Derived
from ...", "Editorial changes ..."). On a structural container that is the ONLY
text, and a row reading just that is precisely the empty entry the paragraph
above objects to. So `_own_rule_text` strips sentences that BEGIN with a
provenance verb and keeps the folder only if something substantive remains.
Measured: 486 promoted, 298 left as folders. The threshold is not knife-edge —
20 chars gives 488 and 60 gives 483 — because the two populations barely
overlap.

A FOLDER THAT ALREADY PUBLISHES FILES IS EXCLUDED. Its "text" is the sentences
introducing those files ("Click here to view the PDF version of Law as
amended."), which `_instruments_from_folders` has already turned into rows and
which its own comment says must not be hashed, since the site rewords them. One
folder hits this today: QFC Law No. (7) of Year 2005.

THE SHAPE IS NOT NEW. A document sitting at a path that is also a folder of
other documents is how SAMA — the same Thomson Reuters platform — already
stores 911 of its rows.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional, Sequence

from crawler.fingerprint import stamp_content_hashes
from crawler.tr_rulebook import Selectors, crawl_rulebook
from generic_crawler.crawler import content_key
from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)

#: EXACT, and repeated in config/sources/qfcl.yml and config/countries.yml.
#: Taken from the scope sheet's "Regulator / Framework" column.
REGULATOR = "Qatar Financial Centre Legislation"

BASE_URL = "https://qfcra-en.thomsonreuters.com"

#: Checkpoints live per source, NOT in one shared file. Four sections crawled as
#: four independent runs would otherwise key on book title into the same dict
#: and read each other's progress as their own.
CHECKPOINT_DIR = Path(__file__).resolve().parent / "_checkpoints"

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")

#: A sentence that is the site's own amendment trail rather than the law.
#: ANCHORED AT THE START of a sentence on purpose. An unanchored `\bamended\b`
#: also matches "the Regulations as amended by the Council", which is rule text,
#: and stripping that would classify a real rule as provenance-only and drop it
#: — the exact failure this module is being changed to fix. The verbs are the
#: ones the site actually uses, counted over all 784 folder pages that carry
#: text: Amended 253, Derived 218, Inserted 122, Deleted 67, Restructured 16,
#: Added 1, plus "Editorial changes" as its own opener (76).
_PROVENANCE = re.compile(
    r"^(?:amended|inserted|deleted|substituted|renumbered|restructured|added"
    r"|transferred|derived\s+from|editorial\s+changes)\b", re.I)

#: Sentence split on a full stop followed by whitespace. Good enough because the
#: only judgement being made is "is there anything here besides the trail".
_SENTENCE = re.compile(r"(?<=\.)\s+")

#: Below this many characters of non-provenance text a folder stays a folder.
#: See the module docstring: the populations barely overlap, so the exact value
#: moves the count by single digits.
_MIN_OWN_TEXT = 30


class QFCLSource:
    """One QFC rulebook section.

    Declared in `config/sources/qfcl.yml` with `mode: custom`, so `build_source`
    imports and instantiates it per source with `init_kwargs`.
    """

    def __init__(
        self,
        seed_url: str,
        source_system: str,
        section: str,
        regulator: str = REGULATOR,
        base_url: str = BASE_URL,
        doc_path_prefix: Optional[Sequence[str]] = None,
        request_delay: float = 1.2,
        root_mode: str = "seed",
        max_books: Optional[int] = None,
        resume: bool = True,
        checkpoint_path: Optional[str] = None,
        selectors: Optional[dict] = None,
    ):
        if not seed_url:
            raise ValueError("QFCLSource needs a seed_url")
        if not source_system:
            raise ValueError("QFCLSource needs a source_system — it is the key "
                             "the completeness gate scopes on, and it cannot be "
                             "inferred after the fact")
        if not section:
            raise ValueError("QFCLSource needs a section (the folder under the "
                             "regulator, e.g. 'QFC Regulation')")

        self.seed_url = seed_url
        self.source_system = source_system
        self.section = section
        self.regulator = regulator
        self.base_url = base_url
        # Overridable from the YAML so the tree can be tuned to match the manual
        # library after the first export WITHOUT a code change. The default is
        # the two levels we know: the regulator, then the section. The book's
        # own sidebar title becomes the level below, supplied by the site.
        self.doc_path_prefix = list(doc_path_prefix or [regulator, section])
        self.request_delay = float(request_delay)
        self.root_mode = root_mode
        # A cap is a PROOF, not an inventory: it under-reports by design and must
        # never be promoted as if it were the whole section. Remove it for the
        # real run and give it an evening.
        self.max_books = max_books
        self.resume = resume
        self._checkpoint_path = (
            Path(checkpoint_path) if checkpoint_path
            else CHECKPOINT_DIR / f"qfcl_{_SAFE.sub('-', source_system)}.json")
        self._selectors = Selectors(**selectors) if selectors else Selectors()
        self.last_result: dict = {}

        if self.doc_path_prefix[:1] != [regulator]:
            # tree_path() reads doc_path[0] as the regulator. Anything else and
            # the whole section files under a country that does not exist.
            raise ValueError(
                f"doc_path_prefix must start with the regulator "
                f"{regulator!r}, got {self.doc_path_prefix!r}")

    @property
    def source_systems(self) -> List[str]:
        """What this source writes under. Read by CompositeCrawler and by the
        completeness gate; a list because the contract allows several."""
        return [self.source_system]

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        nodes = crawl_rulebook(
            seed_url=self.seed_url,
            base_url=self.base_url,
            root_path=self.doc_path_prefix,
            request_delay=self.request_delay,
            root_mode=self.root_mode,
            max_books=self.max_books,
            resume=self.resume,
            checkpoint_path=self._checkpoint_path,
            selectors=self._selectors,
        ) or []

        leaves = [self._to_regulatory(n) for n in nodes if not n.is_folder]
        rule_folders = [self._to_regulatory(n) for n in nodes
                        if n.is_folder and self._is_own_rule(n)]
        instruments = self._instruments_from_folders(nodes)
        logger.info("QFCLSource[%s] — %d provision(s) + %d folder(s) that are "
                    "themselves rules + %d published instrument(s) from %d "
                    "node(s)", self.source_system, len(leaves),
                    len(rule_folders), len(instruments), len(nodes))
        leaves = leaves + rule_folders + instruments

        # A section that returns nothing is a FINDING, not a result. Every other
        # crawler in the library says this; the one that did not is the reason a
        # scheduled job could produce 0 rows without anyone noticing.
        if not leaves:
            raise RuntimeError(
                f"QFCL section {self.section!r} ({self.source_system}) returned "
                f"no documents from {self.seed_url}. That is a failed read, not "
                f"an empty section.")

        docs = leaves

        cap = limit if isinstance(limit, int) and limit > 0 else None
        if cap:
            docs = docs[:cap]

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": []},
            "by_source": {self.source_system: len(docs)},
        }

        # The single exit. The walker already sets an md5 over the page text and
        # stamp_ never overwrites one, so this changes nothing for a page that
        # had text — it is the backstop for one that did not.
        return self._drop_republished(stamp_content_hashes(docs))

    # ── Republished siblings ────────────────────────────────────────────────
    @staticmethod
    def _drop_republished(docs: List[RegulatoryDocument]
                          ) -> List[RegulatoryDocument]:
        """Collapse siblings the SITE publishes more than once, and only those.

        MEASURED on the live sidebar: IBANK 5.2.1 "What Part 5.2 does" has three
        children, all titled "IBANK 5.2.1 Guidance", at
        /ibank-521-guidance, -guidance-0 and -guidance-1. Drupal's `-0`/`-1`
        suffixes are what a slug collision looks like, and all three pages carry
        the same text. The crawl reproduced the site faithfully; the site is what
        repeats.

        That lands as three rows on ONE `doc_path`, because a node's title is the
        last element of its own path. `doc_path` is an identity field, so the
        three compete to be the same stored row and a later run has no stable
        answer for which one wins.

        THE TEST IS DELIBERATELY NARROW: same `doc_path` AND same
        `content_hash`. Two provisions that merely share a title still differ in
        hash, so they are BOTH kept and the collision stays visible — that case
        is a judgement about the rulebook and must not be silently swallowed
        here. Only an exact republication is dropped, which by definition loses
        no text. The surviving row keeps the first URL in walk order, and the
        ones it stood in for are recorded on it rather than thrown away.
        """
        seen: dict = {}
        out: List[RegulatoryDocument] = []
        for d in docs:
            key = (tuple(d.doc_path or ()), d.content_hash)
            first = seen.get(key)
            if first is None:
                seen[key] = d
                out.append(d)
                continue
            dupes = first.extra_meta.setdefault("republished_at", [])
            if d.document_url:
                dupes.append(d.document_url)
            logger.info(
                "QFCLSource[%s] — dropped an exact republication of %r "
                "(%s duplicates %s)", d.source_system, d.title,
                d.document_url, first.document_url)
        if len(out) != len(docs):
            logger.warning("QFCLSource — %d republished sibling(s) collapsed; "
                           "%d row(s) remain", len(docs) - len(out), len(out))
        return out

    # ── Folders that are themselves rules ───────────────────────────────────

    @staticmethod
    def _own_rule_text(content_text: str) -> str:
        """What a page says beyond its own amendment trail.

        Returns the text with provenance sentences removed. The caller decides
        whether what is left is enough; this only separates the two kinds of
        sentence, so a reader can check the rule by eye on any page:

            "Amended by QFCRA RM/2015-3 (as from 1st January 2016). Amended by
             QFCRA RM/2023-5 (as from 1st July 2024)."     -> ""
            "(1) An asset is HQLA if it falls within any of rules 9.3.10 to
             9.3.12 ... Derived from QFCRA RM/2014-2."     -> the rule, no trail
        """
        parts = (p.strip() for p in _SENTENCE.split(content_text or "")
                 if p and p.strip())
        return " ".join(p for p in parts if not _PROVENANCE.match(p)).strip()

    @classmethod
    def _is_own_rule(cls, node) -> bool:
        """True when this FOLDER publishes a rule of its own, not just a heading.

        Three things disqualify a folder, and the order matters:

        1. It publishes FILES. `_instruments_from_folders` has already turned
           those into rows, and this page's text is the sentences introducing
           them — which that method's own comment says must not be hashed,
           because the site rewords them. Emitting both would store the same
           instrument twice, once under a hash that moves on a copy edit.
        2. Its text is only the title. `tr_rulebook._process` substitutes the
           title when a folder page yields no body, so this is the "no body"
           case wearing the title's clothes.
        3. What is left after the amendment trail is too short to be a rule.
        """
        if any(g.get("files")
               for g in (node.extra_meta or {}).get("file_groups") or []):
            return False
        text = (node.content_text or "").strip()
        if not text or text == (node.title or "").strip():
            return False
        return len(cls._own_rule_text(text)) >= _MIN_OWN_TEXT

    # ---- published files on a folder page -----------------------------------
    #
    # "Law No. (2) of 2009", "Law No.(14) of 2009", "Law No. (16) of 2024" —
    # the sentence that introduces each file group names the instrument.
    _LAW_RE = re.compile(
        r"Law\s+No\.?\s*\(?\s*(\d+)\s*\)?\s*of\s*(?:Year\s*)?(\d{4})",
        re.IGNORECASE)

    def _instrument_title(self, text: str, folder_title: str,
                          n_files: int) -> str:
        """Name the instrument a paragraph of file links publishes.

        WHY THE SUFFIX IS CONDITIONAL. "as made" / "as amended" describe a
        VERSION, and in these paragraphs they usually qualify one sentence, not
        the group — the Arabic line reads "...the Law No.(2) of 2009 as made in
        Arabic" while the PDF and Word lines above it carry no qualifier. Taking
        the phrase from anywhere in the paragraph labelled all three 2009/2024
        laws "(as made)", which is wrong about two files out of three.

        So the suffix applies only where it really does describe the whole
        group: a paragraph that names no law (the parent instrument's own
        consolidated text), or one that publishes a single file.
        """
        m = self._LAW_RE.search(text)
        low = text.lower()
        suffix = ""
        if not m or n_files == 1:
            if "as made" in low:
                suffix = " (as made)"
            elif "as amended" in low:
                suffix = " (as amended)"
        if m:
            return f"Law No. ({m.group(1)}) of {m.group(2)}{suffix}"
        # No number in the sentence: it is the parent instrument itself, which
        # the sidebar has already named.
        return f"{folder_title}{suffix or ' (published files)'}"

    def _instruments_from_folders(self, nodes) -> List[RegulatoryDocument]:
        """One row per instrument published on a FOLDER page.

        WHY FOLDERS AND NOT LEAVES. A leaf is a provision; whatever file it
        links is a rendering of the text already stored in that row. A folder
        page is the opposite: it holds no provision of its own and exists to
        publish the instrument — for QFC Law No. (7) that is the consolidated
        text plus the 2009 and 2024 amending laws, three formats each.

        WHERE THEY SIT. As SIBLINGS of the folder, not inside it, because that
        is the shape the manual library shows:

            QFC Law
              QFC Law No. (7) of Year 2005     (folder: 19 Articles, 6 Schedules)
              Law No. (2) of 2009
              Law No. (14) of 2009

        ONE ROW PER INSTRUMENT, NOT PER FILE. PDF, Word and Arabic are three
        renderings of one law, so they are one entry — the library's rule for
        multi-format instruments, spelled out in `models.RegulatoryDocument` and
        implemented for forms as `attachment_is_document: "combined"`
        (dynamic_crawler/formfill/pipeline.py:187). This emits the SAME shape so
        a reader and the analyse path do not have to learn a second one:

            document_url                 ""  (only when there are SEVERAL files)
            extra_meta.attachment_links  "<pdf> | <doc> | <pdf ar>"
            extra_meta.n_files           3
            extra_meta.identity_fields   [doc_path, attachment_links, title]
            content_hash                 over every file, so a change to ANY of
                                         them moves the row

        A single-file group keeps the ordinary (document_url, doc_path)
        identity: there is exactly one url that names it, and emptying the
        column a reader looks in buys nothing. That is the MHRSD correction —
        54 of 57 rows were being emptied for no reason.
        """
        out: List[RegulatoryDocument] = []
        seen: set = set()
        for node in nodes:
            if not node.is_folder:
                continue
            groups = (node.extra_meta or {}).get("file_groups") or []
            if not groups:
                continue
            # The folder's own doc_path ends with itself; its parent is where a
            # sibling belongs.
            parent = list(node.doc_path[:-1]) or list(node.doc_path)
            for g in groups:
                files = [f for f in (g.get("files") or []) if f]
                if not files:
                    continue
                title = self._instrument_title(g.get("text") or "", node.title,
                                               len(files))
                key = (tuple(parent), title)
                if key in seen:
                    continue
                seen.add(key)
                joined = " | ".join(files)
                multi = len(files) > 1
                names = " | ".join(
                    f.rsplit("/", 1)[-1] for f in files)
                out.append(RegulatoryDocument(
                    regulator=self.regulator,
                    source_system=self.source_system,
                    category=self.section,
                    title=title,
                    document_url="" if multi else files[0],
                    source_page_url=node.url,
                    doc_path=parent + [title],
                    file_type=files[0].rsplit(".", 1)[-1].lower()[:8],
                    # The files ARE the document here; there is no page text to
                    # hash, and hashing the introducing sentence would move
                    # whenever the site reworded it.
                    # Every file, so a change to ANY of them changes the row.
                    # Same separator as pipeline.py so the two paths compute the
                    # same hash for the same set of files.
                    content_hash=content_key("|".join(files)),
                    extra_meta={
                        "attachment_links": joined,
                        # `n_files` and `file_titles` are the names the combined
                        # path already uses; anything reading one source's
                        # multi-file rows can read this one's unchanged.
                        "n_files": len(files),
                        "file_titles": names,
                        "record_kind": "combined_attachments",
                        "published_on": node.url,
                        "source_sentence": g.get("text") or "",
                        "section": self.section,
                        **({"identity_fields": [
                            "doc_path", "extra_meta.attachment_links", "title"]}
                           if multi else {}),
                    },
                ))
        return out

    def _to_regulatory(self, node) -> RegulatoryDocument:
        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self.section,
            title=node.title,
            # The page IS the instrument on this platform, so it is both the
            # document and the page it was read from. Any PDF the page links is
            # a rendering of the same text and stays in extra_meta.
            document_url=node.url,
            source_page_url=node.url,
            document_html=node.document_html,
            doc_path=list(node.doc_path),
            file_type="html",
            extra_meta={
                "pdf_link": node.extra_meta.get("pdf_link"),
                "pdf_links": node.extra_meta.get("pdf_links", []),
                "faq_link": node.extra_meta.get("faq_link"),
                "content_text": node.content_text,
                "content_hash": node.content_hash,
                "depth": node.depth,
                "section": self.section,
                "record_kind": "provision",
                # A provision that is ALSO a position in the tree: other rows
                # hang off its doc_path. Kept as `provision` because that is
                # what it is to a reader and to the analyse path; the flag is
                # here so a reviewer can tell why a document has children, and
                # so this population can be counted without re-deriving it.
                # SAMA stores 911 rows of this shape on the same platform.
                "has_children": bool(node.is_folder),
                # The amendment notes as their own field. They remain inside
                # content_text too — this is the site's own trail of which
                # instrument changed this provision and when, and it should be
                # readable without scrolling to the bottom of the text.
                "endnotes": " | ".join(node.extra_meta.get("endnotes") or []),
                "endnote_count": len(node.extra_meta.get("endnotes") or []),
            },
            content_hash=node.content_hash,
        )


__all__ = ["QFCLSource", "REGULATOR", "BASE_URL"]
