"""
Requirement extraction + classification -- Stage A of the new Requirement/
Activity flow. 

Two calls, not one -- extraction and classification stay separate on purpose.
staged_LLM_Analyzer.py's own docstring records that a merged extract+classify
prompt was tried and gave worse results; nothing here revisits that.

requirement_types is a parameter, never a literal in a prompt string. It is a
lookup table (RequirementType) specifically so the taxonomy can change without
touching this file -- see MSSQLRepository.get_requirement_types(). This module
stays DB-free, same reasoning as LLMClient and every other analyzer here: the
caller fetches the list and passes it in.

Ref Key :
Requirement: REQ-{regulation_id}-{sha256(source_reference + norm_text)[:10]}

"""

import hashlib
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

from processor.llm_client import LLMClient, TruncatedResponseError, StructuralLLMError
from utils.lang_detector import detect_language

logger = logging.getLogger(__name__)

LANGUAGE_NAMES = {"ar": "Arabic", "en": "English", "fr": "French", "de": "German", "es": "Spanish"}

# ---------------------------------------------------------------------- #
#  OPTIONAL SEMANTIC SIMILARITY -- candidate-pair prefilter for the        #
#  cross-chunk duplicate check (_flag_cross_chunk_duplicates below).       #
# ---------------------------------------------------------------------- #
# Same optional-dependency pattern as crawler/smart_matcher.py: prefer real
# semantic embeddings when sentence-transformers is installed, but this
# module must keep working without it -- same reasoning as every other
# analyzer here staying usable without extras. UNLIKE smart_matcher.py,
# there is no "cruder but usable" text-similarity fallback here: difflib-
# style character overlap is exactly what _flag_cross_chunk_duplicates's own
# docstring documents as failing on this pipeline's real duplicate case
# ("exert every effort to obtain..." vs "make reasonable efforts to
# collect..." -- near-zero character overlap, same action). So when
# sentence-transformers is unavailable, the fallback is NOT a weaker filter
# -- it is no filter at all: every pair becomes a candidate, identical
# coverage to how this check behaved before candidate pairs existed, just
# judged through the same true/false schema as when vectors are available.
try:
    from sentence_transformers import SentenceTransformer
    import numpy as np

    _dup_st_model = None  # loaded lazily -- most runs never reach this

    def _dup_embed(texts: List[str]):
        global _dup_st_model
        if _dup_st_model is None:
            _dup_st_model = SentenceTransformer("all-MiniLM-L6-v2")
        return _dup_st_model.encode(texts, normalize_embeddings=True)

    _DUP_USE_VECTORS = True
except ImportError:
    _DUP_USE_VECTORS = False

# This is a RECALL filter, not a precision filter -- the AI still judges
# every candidate pair, so erring toward too many candidates costs a bit of
# prompt size, while erring toward too few silently loses real duplicates.
#
# 0.55 (this constant's first value) was checked only against hand-built
# example sentences and turned out much too low on REAL regulatory text --
# measured directly against 5 real documents re-run through this pipeline:
# banking-control alone (113 statements) produced 470 candidate pairs at
# 0.55, and the dup-check call truncated on every single one of the 5
# documents as a result. Real regulations reuse the same sentence template
# for genuinely different rules far more than the hand-built examples did --
# e.g. "Any person who contravenes the provisions of Article 2... shall be
# liable to..." and "...Article 19... shall be liable to..." score 0.87
# despite being two unrelated penalty clauses -- so a threshold tuned on a
# few invented sentences badly underestimated how similar unrelated real
# sentences can look.
#
# 0.68 was chosen by measuring candidate counts on those same 5 real
# documents: every one lands at 48-85 candidate pairs (~2,100-3,800
# estimated output tokens) at 0.68, comfortably inside the raised
# max_tokens below, vs. 104-470 pairs at 0.55-0.60 that caused the
# truncation. Still a recall-biased number, not a precision one -- some
# same-template/different-rule pairs still clear it and get correctly
# judged "not a duplicate" by the AI, which costs a little prompt size but
# not correctness. Re-measure if a much larger document starts truncating
# again; this is a starting point tuned on 5 documents, not a universal
# constant.
_DUP_CANDIDATE_THRESHOLD = 0.68

_SYSTEM_PROMPT = (
    "You are a senior regulatory compliance analyst. You extract and classify "
    "binding regulatory requirements with precision. You never invent content "
    "that is not in the source text, and you never use a classification value "
    "outside the list you are given."
)

_CHUNK_MAX_CHARS = 3500
_CHUNK_MAX_SECTIONS = 5
_CLASSIFY_BATCH_SIZE = 12

# Disposition: the answer to "does this statement warrant an obligation on
# the bank." OBL and COND are what activity design should ever see; REG/DEF/
# INFO are retained on the row (never dropped) so a reviewer can see the
# statement was read and why it isn't downstream work. No entity-profile /
# applicability step yet -- that needs a per-client profile that doesn't
# exist in this system yet; disposition alone doesn't need it, since it's
# decided purely from the actor/nature prompt 1 already recorded.
_DISPOSITIONS = ("OBL", "COND", "REG", "DEF", "INFO")

# The DB's CK_Requirement_Nature check constraint accepts exactly these six
# (lowercase) values and nothing else -- an LLM typo/synonym that reaches
# insert_requirement raises pyodbc.IntegrityError and aborts the WHOLE
# sync, discarding every requirement and activity already extracted in the
# same run, not just the one bad row. See _coerce_nature.
_NATURES = ("mandatory", "prohibition", "conditional", "discretionary",
           "definition", "informational")

# A chunk over this many times _CHUNK_MAX_CHARS is worth knowing about, but
# NOT a reason to stop -- _chunk_document deliberately lets one unsplittable
# giant article become its own oversized chunk rather than cutting it
# mid-body (see its own docstring). This is visibility only.
_OVERSIZED_CHUNK_MULTIPLE = 3


class ChunkingError(RuntimeError):
    """_chunk_document produced zero chunks for non-empty input. Unlike an
    oversized chunk (a known, accepted tradeoff -- see _OVERSIZED_CHUNK_MULTIPLE
    above), this has no legitimate cause: every path in _chunk_document that
    receives non-empty text is built to return at least one chunk. Reaching
    zero means the chunker itself is broken, not that this particular
    document's content is unusual -- continuing to extraction would spend
    real LLM calls against nothing."""

# "Ar.{1,2}cle" not "Article": some PDF text extraction renders the "ti"
# ligature in "Article" as a single corrupted codepoint (observed: U+019F,
# "Ɵ", on a real SAMA document). Under the literal "Article" spelling, header
# detection found exactly ONE real header in a 26-article law and the whole
# document body collapsed into one ~30,000-char "section", losing four whole
# articles to truncation downstream.
#
# "\(?\s*<num>\s*\)?" -- a second real format, found auditing a live MOH
# regulation from the actual regulations table: every header written
# "Article (1):" with the number in parentheses, plain ASCII, no ligature.
# 56 real occurrences, zero detected under the plain digit-after-whitespace
# form. Both formats verified against real production documents, not
# invented defensively.
# article_colon/section_colon are separate optional groups, not folded into
# article/section, so a match's "did this have a trailing colon" question can
# be answered without disturbing what the number-extraction group captured.
_NUMBERED_HEADER_RE = re.compile(
    r'Chapter\s+(?P<chapter>\d+):'
    r'|Ar.{1,2}cle\s*\(?\s*(?P<article>\d+)\s*\)?(?P<article_colon>\s*:)?'
    r'|Section\s+(?P<section>\d+)(?P<section_colon>\s*:)?'
)
_UNCONDITIONAL_HEADER_RE = re.compile(
    r'(?=CHAPTER\s+[IVXLCDM]+)|(?=Part\s+[A-Z0-9]+)|(?=الفصل\s+)|(?=المادة\s+)'
)


def _classify_header_match(m: "re.Match") -> Optional[Tuple[str, int, bool]]:
    """(label, number, has_trailing_colon) for one _NUMBERED_HEADER_RE match,
    or None if it matched nothing (finditer can yield empty matches on the
    all-optional article branch). Shared by _find_header_positions and
    _mark_section_resets so the two never classify the same match two
    different ways."""
    if m.group("chapter") is not None:
        return "chapter", int(m.group("chapter")), True  # Chapter's colon is baked into its own pattern
    if m.group("article") is not None:
        return "article", int(m.group("article")), m.group("article_colon") is not None
    if m.group("section") is not None:
        return "section", int(m.group("section")), m.group("section_colon") is not None
    return None


def _find_header_positions(text: str) -> List[int]:
    """Character offsets of genuine Article/Chapter/Section headers. Two
    independent ways for a match to earn acceptance:

    1. Sequential -- gated against a running expected-next-number per marker
       type, so an inline cross-reference ("...as defined in Article 15...")
       is not mistaken for a real header. The FIRST match for a label must be
       numbered 1 to be accepted this way, not merely "the first one seen" --
       a preamble citing an unrelated law's article number (SAMA's own
       preamble: "Article (19) of the Council of Ministers' Charter") comes
       before the real Article 1 and would otherwise be accepted
       unconditionally as the baseline, corrupting every expected-number
       check after it.
    2. Colon-confirmed -- a match immediately followed by a colon is trusted
       REGARDLESS of sequence. THE BUG THIS FIXES: a real document (an MHRSD
       binder of ~11 separately-numbered HR "Regulatory Frameworks" and
       contract templates bound into one PDF) restarts "Article (1):" eleven
       times. Sequential-only gating accepts the very first restart (the
       real Article 1) and then rejects every later "Article (1)" as if it
       were an out-of-order citation -- exactly the failure mode gate #1
       exists to catch, except here the restart is genuine. The result:
       whole frameworks' worth of headers went undetected, collapsing them
       into two ~130-requirement chunks instead of a dozen normal-sized
       ones. Measured on this document: 190 of 195 "Article (N)" occurrences
       are immediately followed by a colon; the 5 that are not are
       genuine citations ("...in accordance with Article (32) of the
       Regulations..."). A colon is NOT a universal signal (SAMA's own
       documents never use one), so this is an additional acceptance path,
       not a replacement for gate #1."""
    positions = []
    expected: Dict[str, int] = {}
    for m in _NUMBERED_HEADER_RE.finditer(text):
        parsed = _classify_header_match(m)
        if parsed is None:
            continue
        label, num, has_colon = parsed
        nxt = expected.get(label)
        if (nxt is None and num == 1) or num == nxt or has_colon:
            positions.append(m.start())
            expected[label] = num + 1
    for m in _UNCONDITIONAL_HEADER_RE.finditer(text):
        positions.append(m.start())
    return sorted(set(positions))


def _mark_section_resets(text: str) -> str:
    """Insert an explicit "[SECTION N OF THIS DOCUMENT]" marker wherever
    numbering restarts back to 1 -- i.e. exactly the colon-confirmed-but-out-
    of-sequence case _find_header_positions now accepts as a genuine header.

    Chunking alone fixes WHERE the document splits; it does nothing about
    WHAT each restarted "Article (1)" should be called. Checked directly: the
    MHRSD document's framework titles ("The Regulatory Framework for Medical
    Examination...") appear exactly once each, in a table of contents at the
    very front -- they are never repeated inline before the section they
    name, so there is no nearby text for the model to read a real title from.
    Two completely unrelated obligations -- a medical-exam referral rule and
    a training-reward exclusion rule -- would otherwise both cite bare
    "Article (1)" with nothing distinguishing them. An ordinal marker is not
    as good as the real framework name, but it costs nothing to compute and
    makes every restarted section's citation unique instead of colliding."""
    positions = []
    expected: Dict[str, int] = {}
    for m in _NUMBERED_HEADER_RE.finditer(text):
        parsed = _classify_header_match(m)
        if parsed is None:
            continue
        label, num, has_colon = parsed
        nxt = expected.get(label)
        if has_colon and nxt is not None and num == 1 and nxt != 1:
            positions.append(m.start())
        if (nxt is None and num == 1) or num == nxt or has_colon:
            expected[label] = num + 1

    if not positions:
        return text
    out, section_no, last = [], 1, 0
    for pos in positions:
        section_no += 1
        out.append(text[last:pos])
        out.append(f"[SECTION {section_no} OF THIS DOCUMENT -- separately numbered from "
                   f"the section(s) before it] ")
        last = pos
    out.append(text[last:])
    return "".join(out)


_PAGE_MARKER_RE = re.compile(r'\bPAGE \d+\b\s*')


def _strip_page_markers(text: str) -> str:
    """processor/Text_Extractor.py joins PDF pages as "PAGE {n}\\n{text}" --
    a pipeline artifact, not regulation content. Left in, it becomes the
    nearest visible "landmark" text whenever an article's real header was
    lost to a chunk split (see _section_header_label below), and the model
    cites the page number as if it were a legal reference -- observed as
    source_reference values like "PAGE 9" instead of "Article 10(5)". Strip
    it before chunking so it can never be mistaken for a citation."""
    return _PAGE_MARKER_RE.sub('', text)


def _chunk_by_char_budget(text: str) -> List[str]:
    chunks = []
    start, n = 0, len(text)
    while start < n:
        end = min(start + _CHUNK_MAX_CHARS, n)
        if end < n:
            last_period = text.rfind(". ", start, end)
            if last_period > start + _CHUNK_MAX_CHARS // 2:
                end = last_period + 1
        chunks.append(text[start:end].strip())
        start = end
    return [c for c in chunks if c]


def _section_header_label(sec: str) -> str:
    """The header a section starts with, e.g. 'Article 23' or 'Chapter 4' --
    used only by _extraction_shard's truncation-retry fallback, which is the
    one remaining place text still gets cut (see its docstring)."""
    m = _NUMBERED_HEADER_RE.match(sec.strip())
    if m:
        return m.group(0).rstrip(':').strip()
    m2 = _UNCONDITIONAL_HEADER_RE.match(sec.strip())
    if m2:
        return sec.strip().split("\n", 1)[0][:40].strip()
    return ""


def _chunk_document(text: str) -> List[str]:
    """Split flat document text into ordered chunks anchored at
    article/section boundaries, capped at ~_CHUNK_MAX_CHARS or
    _CHUNK_MAX_SECTIONS sections, whichever comes first.

    An article is NEVER cut mid-body. Earlier this sub-split any section over
    _CHUNK_MAX_CHARS by raw character count -- so an oversized article's
    continuation lost its own header, got repacked next to whatever article
    followed, and the model cited the NEXT article because it was the only
    header still visible. Measured: a 5,600-char penalties clause (Article
    23) got its back half attributed to Article 24 on 7 consecutive
    extracted requirements. Fixed by never splitting a section here -- an
    oversized one just becomes its own single-section chunk, over budget but
    intact, rather than being cut."""
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= _CHUNK_MAX_CHARS:
        return [text]

    positions = _find_header_positions(text)
    if not positions:
        return _chunk_by_char_budget(text)

    bounds = sorted(set([0] + positions + [len(text)]))
    sections = [text[bounds[i]:bounds[i + 1]].strip()
                for i in range(len(bounds) - 1)]
    sections = [s for s in sections if s]

    chunks, current, current_len = [], [], 0
    for section in sections:
        if current and (current_len + len(section) > _CHUNK_MAX_CHARS
                         or len(current) >= _CHUNK_MAX_SECTIONS):
            chunks.append(" ".join(current))
            current, current_len = [], 0
        current.append(section)
        current_len += len(section)
    if current:
        chunks.append(" ".join(current))
    return chunks


class RequirementAnalyzer:
    """Extracts requirements from regulation text, then classifies each one
    against a RequirementType taxonomy supplied by the caller."""

    def __init__(self, model: str = "deepseek/deepseek-v3.2", max_workers: int = 4,
                 deterministic: Optional[bool] = None):
        self.max_workers = max_workers
        self.client = LLMClient(model=model, system_prompt=_SYSTEM_PROMPT,
                                 deterministic=deterministic)

    # ------------------------------------------------------------------ #
    #  PUBLIC ENTRY POINT                                                  #
    # ------------------------------------------------------------------ #

    def extract_and_classify(
        self,
        text: Optional[str] = None,
        document_title: str = "",
        requirement_types: List[str] = None,
        regulator: str = "",
        reference: str = "",
        publication_date: str = "",
        documents: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, object]:
        """Returns {"requirements": [...], "chunk_texts": {chunk_id: text}}.

        documents, when given, REPLACES `text`: a list of
        {"source_document": <label>, "text": <content>} -- e.g. document_html
        plus every PDF in extra_meta.attachment_links. Each document is
        chunked and cited independently, so a requirement's source_reference
        always resolves back to the ONE physical file it came from. Every
        existing single-document caller keeps working unchanged -- passing
        plain `text` wraps it as one document labelled "main_body".

        chunk_texts is handed to Stage B (activity design) so it can batch by
        source chunk and send each chunk's rule text once per batch instead of
        once per requirement -- see processor/activity_analyzer.py.
        """
        if not requirement_types:
            raise ValueError("requirement_types must be non-empty -- fetch it "
                              "from MSSQLRepository.get_requirement_types() "
                              "before calling this")
        if documents is None:
            documents = [{"source_document": "main_body", "text": text or ""}]

        # One language for the whole bundle -- every prompt in this pipeline
        # already threads a single `language` value through, so detecting per
        # document would ripple through every call. A bundle is normally one
        # language throughout; revisit only if that stops being true.
        iso = detect_language(" ".join(d.get("text", "") for d in documents))
        language = LANGUAGE_NAMES.get(iso, "English")

        raw, chunk_texts = self._run_extraction_sharded(
            documents, document_title, regulator, reference, publication_date, language)
        if not raw:
            logger.warning("Extraction returned no requirements")
            return {"requirements": [], "chunk_texts": {}}

        deduped = self._dedupe_exact(raw)
        if len(deduped) < len(raw):
            logger.info(f"Exact-duplicate requirements removed: {len(raw) - len(deduped)}")

        for i, r in enumerate(deduped, start=1):
            r["requirement_local_id"] = f"R{i:04d}"

        classified = self._run_classification(deduped, requirement_types, language)
        classified = self._flag_cross_chunk_duplicates(classified, language)
        return {"requirements": classified, "chunk_texts": chunk_texts}


    # ------------------------------------------------------------------ #
    #  EXTRACTION                                                          #
    # ------------------------------------------------------------------ #

    def _run_extraction_sharded(self, documents, document_title, regulator, reference,
                                publication_date, language):
        """Returns (requirements, chunk_texts). Each document is chunked
        independently -- boundaries and article/section numbering never cross
        a document boundary, since two different PDFs restart their own
        numbering independently of each other. chunk_id is a running counter
        across the WHOLE bundle, not reset per document, so
        activity_analyzer.py's grouping-by-chunk_id keeps working unchanged.
        Every requirement carries both chunk_id and source_document -- the
        latter is what lets a citation resolve back to the exact file it
        came from."""
        chunk_texts: Dict[int, str] = {}
        chunk_jobs = []  # (chunk_id, chunk_text, source_document)
        for doc in documents:
            source_document = doc.get("source_document") or "main_body"
            doc_text = _strip_page_markers(doc.get("text") or "")
            doc_text = _mark_section_resets(doc_text)
            doc_chunks = _chunk_document(doc_text)
            if doc_text.strip() and not doc_chunks:
                raise ChunkingError(
                    f"'{source_document}': {len(doc_text):,} chars of input produced "
                    f"zero chunks -- a chunker bug, not a content issue")
            oversized = [c for c in doc_chunks
                        if len(c) > _CHUNK_MAX_CHARS * _OVERSIZED_CHUNK_MULTIPLE]
            if oversized:
                logger.warning(
                    f"'{source_document}': {len(oversized)} chunk(s) over "
                    f"{_CHUNK_MAX_CHARS * _OVERSIZED_CHUNK_MULTIPLE:,} chars "
                    f"(largest {max(len(c) for c in oversized):,}) -- likely an "
                    f"unsplittable section, not necessarily a bug, but worth a look")
            for chunk in doc_chunks:
                chunk_id = len(chunk_texts)
                chunk_texts[chunk_id] = chunk
                chunk_jobs.append((chunk_id, chunk, source_document))

        if not chunk_jobs:
            return [], {}

        if len(chunk_jobs) == 1:
            chunk_id, chunk, source_document = chunk_jobs[0]
            reqs = self._extraction_shard(chunk, document_title, regulator,
                                          reference, publication_date, language)
            for r in reqs:
                r["chunk_id"] = chunk_id
                r["source_document"] = source_document
            return reqs, chunk_texts

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            results = list(pool.map(
                lambda job: self._extraction_shard(job[1], document_title, regulator,
                                                   reference, publication_date, language),
                chunk_jobs))

        merged = []
        for (chunk_id, _chunk, source_document), chunk_reqs in zip(chunk_jobs, results):
            for r in chunk_reqs:
                r["chunk_id"] = chunk_id
                r["source_document"] = source_document
            merged.extend(chunk_reqs)

        logger.info(f"Extraction: {len(documents)} document(s), {len(chunk_jobs)} chunk(s) "
                   f"-> {len(merged)} raw requirement(s)")
        return merged, chunk_texts

    def _extraction_shard(self, chunk, document_title, regulator, reference,
                          publication_date, language, depth: int = 0) -> List[dict]:
        try:
            raw = self.client.complete(
                self._prompt_extraction(chunk, document_title, regulator,
                                        reference, publication_date, language),
                temperature=0.1, max_tokens=8000, expect_json=True, label="req-extract",
            )
            reqs = self._parse_json(raw).get("r") or []
        except TruncatedResponseError:
            if len(chunk) > 500 and depth < 3:
                mid = len(chunk) // 2
                split_at = chunk.rfind(" ", 0, mid)
                split_at = split_at if split_at > 0 else mid
                logger.warning(f"Extraction truncated for a {len(chunk)}-char chunk; "
                               f"splitting and retrying")
                # This is the one remaining place a chunk gets cut mid-body --
                # _chunk_document no longer does it, but a chunk that IS one
                # oversized article can still truncate on output and land
                # here. Stamp the header onto the second half exactly like
                # _chunk_document used to, so this rare fallback stays
                # citation-safe instead of reintroducing the same bug.
                label = _section_header_label(chunk)
                second_half = chunk[split_at:]
                if label and not _NUMBERED_HEADER_RE.match(second_half.strip()):
                    second_half = f"[Continuing {label}]\n{second_half}"
                first = self._extraction_shard(chunk[:split_at], document_title, regulator,
                                               reference, publication_date, language, depth + 1)
                second = self._extraction_shard(second_half, document_title, regulator,
                                                reference, publication_date, language, depth + 1)
                return first + second
            logger.error(f"Extraction truncated and cannot be split further; "
                        f"{len(chunk)}-char chunk dropped")
            return []
        except StructuralLLMError:
            # Every retry inside LLMClient.complete() already failed the same
            # way (bad key, unreachable endpoint) -- not this chunk's problem,
            # every remaining chunk would fail identically. Let it propagate
            # out of extract_and_classify() rather than flagging this one
            # chunk's requirements as merely "missing", which is what every
            # other exception here does.
            raise
        except Exception as e:
            logger.error(f"Extraction failed for chunk: {e}")
            return []

        return [r for r in reqs if isinstance(r, dict) and r.get("t")]

    def _prompt_extraction(self, chunk_text, document_title, regulator, reference,
                           publication_date, language) -> str:
        # Built only when the marker is ACTUALLY present in this chunk, and
        # phrased with no literal "Article N"-shaped example -- that literal
        # string used to sit unconditionally in every call's context, and a
        # headerless document (a circular with no numbered articles at all)
        # echoed it back as a fabricated source_reference on 6 requirements,
        # because the model had nothing real to cite and this was the closest
        # citation-shaped text it had seen. Never hand the model an example
        # value it could mistake for a fallback.
        continuation_note = ""
        if chunk_text.strip().startswith("[Continuing "):
            marker_end = chunk_text.find("]")
            label = chunk_text[len("[Continuing "):marker_end] if marker_end > 0 else ""
            continuation_note = (
                f"\nThis excerpt opens with a bracketed marker naming the section it continues "
                f"({label!r}). That marker is NOT part of the regulation's text -- the section's own "
                f"heading appeared in an earlier excerpt you cannot see. Use that same section "
                f"identifier as source_reference for this excerpt's content, unless a genuinely new "
                f"heading appears partway through it."
            )
        reset_note = ""
        if "[SECTION " in chunk_text and "OF THIS DOCUMENT" in chunk_text:
            reset_note = (
                "\nOne or more bracketed \"[SECTION N OF THIS DOCUMENT -- separately numbered "
                "from...]\" markers appear in this excerpt. They are NOT part of the regulation's "
                "text -- this document is a bound compilation of several independently-numbered "
                "regulations/forms, each restarting its own \"Article (1)\", and the marker flags "
                "exactly where one ends and the next begins. For every requirement, prefix its "
                "source_reference with the section number from the LAST such marker before it in "
                "this excerpt (e.g. \"Section 3, Article (1)\"), so two requirements from different "
                "restarted sections never end up citing the identical bare article number."
            )
        return f"""<document>
{chunk_text}
</document>

<context>
Document title: {document_title}
Regulator: {regulator}
Reference number: {reference}
Publication date: {publication_date}
This is one excerpt of a larger document -- extract only what is in THIS excerpt.{continuation_note}{reset_note}
</context>

<task>
Extract every directive statement from the excerpt above -- every sentence that requires,
prohibits, permits-on-condition, defines a term, or is preamble/provenance about the document
itself. Record WHO the statement is addressed to. Do NOT decide whether it applies to the bank,
and do NOT decide whether the actor is the bank or someone else for the purpose of dropping it --
record the actor and let a later step decide what to do with it.
</task>

<extraction_rules>
- Extract statements that use directive language: must, shall, shall not, required to, obligated
  to, prohibited from, may (only when a condition, threshold or disclosure is attached).
- Record the actor EXACTLY as the text names it -- "the bank", "licensed financial institutions",
  "the Central Bank", "the Public Prosecution", "the Board of Directors". Do NOT drop a statement
  because the actor is a regulator, court or authority rather than the bank -- extract it with its
  actor recorded; a later step decides what to do with it.
- ALSO extract each distinct row of a table or enumerated list, ONLY when the sentence introducing
  it puts a "must/shall"-style instruction ON THE READER to correctly identify, select, or apply one
  of the rows -- e.g. "the KSA-FI MUST report an accurate TIN Code for each account (detailed
  below)", followed by a table of codes. There, each row is its own testable rule, because the
  reader has to get the selection right. Extract EVERY row of such a table consistently -- never
  extract some rows and silently skip sibling rows in the same list.
- Do NOT do this for a list that merely DEFINES or enumerates the scope of a term -- "X [is/are]/
  means/refers to/includes the following: [list]" with no instruction attached to the reader at all.
  That is scope-setting, not an obligation, regardless of how many items it names. Extract it as ONE
  statement describing what the term covers, with nature "definition" (see below), never one
  statement per list item. Contrast directly: "the financial activities referred to in Article
  1(14) of the Law are: (1) accepting deposits, (2) lending, (3) ..." defines a term used elsewhere
  in the law -- ONE statement, never twelve. "The KSA-FI must report the accurate code: 111=X,
  222=Y, ..." instructs the reader to pick correctly -- one statement PER code. The difference is
  whether the intro sentence contains an actual directive verb aimed at the reader (report, select,
  apply, identify, comply with the correct one of the following), not merely whether a list follows.
- ALSO extract a conditional permission or exemption that carries its own compliance boundary --
  e.g. "X is exempted from Y if Z, provided that [amount/ratio] does not exceed [limit]", or
  "A may do B, provided that C". The word "may" alone does not disqualify a sentence: if it attaches
  a condition, threshold or disclosure that must be satisfied to rely on the permission, that
  condition is itself a statement, extracted on its own terms (not paraphrased into a "must"), with
  nature "conditional".
- The document citing its OWN legal basis (e.g. "these instructions are issued pursuant to
  Resolution X, based on the Law issued by Royal Decree No. Y"), preambles, and other non-binding
  commentary ARE extracted, with nature "informational" -- but only ONCE per excerpt, as a single
  statement, even if similar provenance language appears more than once. This is what lets a
  reviewer see the excerpt was read, without inflating the count.
- If a sentence contains more than one distinct action by the same actor, split it into separate
  atomic statements.
- Do not split statements that share a single subject and are logically inseparable into one action.
- When a sentence gives two or more ALTERNATIVE paths for different categories of actor ("Category A
  participates through X; Category B participates under Y"), keep that as separate statements, one
  per category, each scoped to its own actor -- do not phrase one category's statement in a way
  that reads as if it also governs the other category.
- Preserve the exact regulatory meaning -- do not paraphrase or interpret beyond what is written.
- Do not invent any content.
- Each statement must be independently testable by an auditor.
- Capture any explicit condition, threshold, timeframe or deadline attached to the statement in its
  own field, verbatim, or "" if there is none.
- Capture cross-references to other articles or instruments (e.g. "as defined in Article 3", "in
  accordance with Circular 2/RB/2023") as an array of references, exactly as written; empty array
  if there are none.
- Record each statement's source reference exactly as it appears -- this may be a numbered
  article/section (e.g. "Article 6") or, if the document has no numbered articles, the nearest
  section heading in the text (e.g. "Annual Request for Missing Required U.S. TINs"). NEVER invent
  a number, and NEVER write a placeholder like "Article N" -- if genuinely nothing identifies the
  section, use the document title given above instead.
</extraction_rules>

<nature_values>
Assign exactly one nature per statement: mandatory | prohibition | conditional | discretionary |
definition | informational. This is a plain description of what the sentence itself does -- it is
NOT a judgment about whether the statement is binding on the bank; that comes later.
</nature_values>

<topic_tagging>
Tag each statement with a short 2-4 word compliance topic label, e.g. "Capital Adequacy",
"Licensing", "Credit Limits", "Reporting", "Governance". Do NOT decide category grouping here.
</topic_tagging>

<deduplication_rules>
Before returning, check every statement against all others in THIS excerpt. If two statements
share the same core action, actor and subject, keep only one.
</deduplication_rules>

<language_rules>
The document is in {language}. ALL output fields must be written in {language}. Do NOT translate.
</language_rules>

<output_format>
Return ONLY minified JSON on a single line: no line breaks, no indentation, no markdown, no
code fences, no explanation.
Fields: t = statement_text, s = source_reference, a = actor (verbatim), n = nature (mandatory |
prohibition | conditional | discretionary | definition | informational), c = condition/threshold/
deadline (verbatim, or ""), x = cross_references (array, may be empty), p = topic
Schema: {{"r":[{{"t":"","s":"","a":"","n":"","c":"","x":[],"p":""}}]}}
</output_format>"""

    # ------------------------------------------------------------------ #
    #  DEDUPLICATION -- exact string equality, Python, not the LLM         #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _norm(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip().casefold())

    def _dedupe_exact(self, requirements: List[dict]) -> List[dict]:
        seen, kept = set(), []
        for r in requirements:
            key = self._norm(r.get("t", ""))
            if not key or key in seen:
                continue
            seen.add(key)
            kept.append(r)
        return kept

    # ------------------------------------------------------------------ #
    #  CLASSIFICATION                                                      #
    # ------------------------------------------------------------------ #

    def _run_classification(self, requirements: List[dict], requirement_types: List[str],
                            language: str) -> List[Dict]:
        by_id = {r["requirement_local_id"]: r for r in requirements}
        batches = [requirements[i:i + _CLASSIFY_BATCH_SIZE]
                   for i in range(0, len(requirements), _CLASSIFY_BATCH_SIZE)]

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            results = list(pool.map(
                lambda b: self._classification_shard(b, requirement_types, language), batches))
        deltas = [d for batch_deltas in results for d in batch_deltas]

        rows = []
        coerced = 0
        seen = set()
        for d in deltas:
            if not isinstance(d, dict):
                continue
            rid = d.get("i")
            src = by_id.get(rid)
            if not src:
                logger.warning(f"Classification returned unknown requirement id {rid}; skipped")
                continue
            seen.add(rid)

            rtype, was_coerced = self._coerce_type(d.get("y"), requirement_types)
            coerced += was_coerced
            disposition, disp_coerced = self._coerce_disposition(d.get("d"))
            coerced += disp_coerced
            nature, nature_coerced = self._coerce_nature(src.get("n"))
            coerced += nature_coerced

            rows.append({
                "requirement_local_id": rid,
                "chunk_id": src.get("chunk_id"),
                "source_document": src.get("source_document", ""),
                "description": src.get("t", ""),
                "source_reference": src.get("s", ""),
                "source_refs": [{"source_document": src.get("source_document", ""),
                                 "source_reference": src.get("s", "")}],
                "title": d.get("h") or src.get("p", ""),
                "requirement_type": rtype,
                "actor": src.get("a", ""),
                "nature": nature,
                "condition": src.get("c", ""),
                "cross_references": src.get("x") or [],
                "disposition": disposition,
                "disposition_reason": d.get("w", "") if disposition != "OBL" else "",
                "needs_manual_review": bool(was_coerced) or bool(disp_coerced) or bool(nature_coerced),
            })

        # Same bug class as activity_analyzer.py's total-call-failure gap: if
        # a batch's classification call fails outright (not just returns an
        # incomplete response), _classification_shard returns [] and this
        # loop simply never produces a row for those ids -- a requirement
        # Stage A successfully extracted would vanish from the output
        # entirely, one step short of ever reaching requirements.json, with
        # nothing to indicate it ever existed. Backfill a flagged row instead
        # of silently dropping it.
        for rid, src in by_id.items():
            if rid in seen:
                continue
            logger.warning(f"Requirement {rid} missing from classification response")
            nature, _ = self._coerce_nature(src.get("n"))
            rows.append({
                "requirement_local_id": rid,
                "chunk_id": src.get("chunk_id"),
                "source_document": src.get("source_document", ""),
                "description": src.get("t", ""),
                "source_reference": src.get("s", ""),
                "source_refs": [{"source_document": src.get("source_document", ""),
                                 "source_reference": src.get("s", "")}],
                "title": src.get("p", ""),
                "requirement_type": (requirement_types[0] if requirement_types else ""),
                "actor": src.get("a", ""),
                "nature": nature,
                "condition": src.get("c", ""),
                "cross_references": src.get("x") or [],
                "disposition": "",
                "disposition_reason": "missing from classification response",
                "needs_manual_review": True,
            })

        if coerced:
            logger.warning(f"Classification produced {coerced} value(s) outside the "
                           f"permitted requirement_type list; coerced and flagged")
        return rows

    # ------------------------------------------------------------------ #
    #  CROSS-CHUNK DUPLICATE CHECK -- the one call that sees everything    #
    # ------------------------------------------------------------------ #

    def _flag_cross_chunk_duplicates(self, rows: List[dict], language: str) -> List[dict]:
        """Exact-dedup and the extraction prompt's own dedup rule both operate
        on ONE chunk at a time, so the same obligation restated in different
        wording in two different sections -- each handled by an independent
        extraction call -- survives as two separate requirements. Observed
        directly: "exert every effort possible to obtain..." (one chunk) and
        "required to make reasonable efforts to collect the TINs" (a
        different chunk) are the same instruction, worded too differently
        for a text-similarity check to catch, and went on to get OPPOSITE
        activity_needed verdicts because Stage B also batches per chunk and
        neither call could see the other's text.

        This is the only call in the pipeline that ever sees every extracted
        requirement at once. Feeding it a whole document bundle's combined
        requirement list (not just one document's) makes it bundle-scoped for
        free, with no change needed here beyond what the caller passes in.

        A "high" confidence pair is MERGED: the second requirement is dropped
        from the returned list and its source_refs are folded into the
        survivor's -- so a requirement repeated across two documents in the
        same bundle (e.g. the same clause in document_html and an attached
        PDF) ends up as ONE row with two clickable source locations instead
        of two near-identical rows. A "low" confidence pair is left as two
        separate rows, flagged via possible_duplicate_of for a human to
        decide -- merging on an uncertain signal risks silently deleting a
        genuinely distinct requirement, which is worse than a duplicate a
        reviewer has to dismiss.

        Candidate pairs (_candidate_dup_pairs) are generated BEFORE this
        call, not searched for by the model itself -- see the module-level
        comment on _DUP_USE_VECTORS for why the no-embeddings fallback is
        "every pair is a candidate" rather than a weaker text-similarity
        filter: this exact function's docstring above is the documented case
        a naive text filter would have missed."""
        if len(rows) < 2:
            return rows

        candidates = self._candidate_dup_pairs(rows)
        if not candidates:
            logger.info("Cross-chunk duplicate check: no candidate pairs above threshold, "
                       "skipping the AI call entirely")
            return rows

        by_text = {r["requirement_local_id"]: r["description"] for r in rows}
        compact_pairs = [{"a": a, "at": by_text[a], "b": b, "bt": by_text[b]}
                         for a, b in candidates]
        try:
            raw = self.client.complete(
                self._prompt_dup_check(
                    json.dumps(compact_pairs, ensure_ascii=False, separators=(",", ":")), language),
                # 12000, not 6000 -- 6000 truncated on every one of 5 real
                # documents at the old 0.55 threshold (worst case measured:
                # ~470 candidate pairs). 0.68 above brings real documents
                # down to ~2,100-3,800 estimated tokens; this leaves roughly
                # 3x headroom over that measured worst case, not just enough
                # to scrape by.
                temperature=0.1, max_tokens=12000, expect_json=True, label="req-dupcheck",
            )
            pairs = self._parse_json(raw).get("d") or []
        except Exception as e:
            logger.warning(f"Cross-chunk duplicate check failed, skipping: {e}")
            return rows

        by_id = {r["requirement_local_id"]: r for r in rows}
        to_drop = set()
        merged = 0
        flagged = 0
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            a, b = pair.get("a"), pair.get("b")
            if a not in by_id or b not in by_id or a == b:
                continue
            if not pair.get("dup"):
                continue  # candidate judged NOT a duplicate -- e.g. a narrower obligation
            if a in to_drop or b in to_drop:
                continue  # one side already absorbed elsewhere this pass
            confidence = str(pair.get("c") or "").strip().lower()
            if confidence == "high":
                survivor, absorbed = by_id[a], by_id[b]
                survivor["source_refs"].extend(absorbed.get("source_refs", []))
                to_drop.add(b)
                merged += 1
            else:
                by_id[a].setdefault("possible_duplicate_of", []).append(b)
                by_id[b].setdefault("possible_duplicate_of", []).append(a)
                by_id[a]["needs_manual_review"] = True
                by_id[b]["needs_manual_review"] = True
                flagged += 1
        if merged:
            logger.warning(f"Cross-chunk duplicate check merged {merged} high-confidence pair(s)")
        if flagged:
            logger.warning(f"Cross-chunk near-duplicate check flagged {flagged} pair(s) "
                           f"for manual review")
        return [r for r in rows if r["requirement_local_id"] not in to_drop]

    def _candidate_dup_pairs(self, rows: List[dict]) -> List[Tuple[str, str]]:
        """(id_a, id_b) pairs worth sending to the AI duplicate judge. See
        the module-level _DUP_USE_VECTORS comment for why the fallback,
        when sentence-transformers isn't installed, is every pair -- not a
        cruder similarity filter."""
        ids = [r["requirement_local_id"] for r in rows]
        all_pairs = [(ids[i], ids[j]) for i in range(len(ids)) for j in range(i + 1, len(ids))]
        if not _DUP_USE_VECTORS:
            return all_pairs

        texts = [r["description"] for r in rows]
        try:
            vecs = _dup_embed(texts)
        except Exception as e:
            logger.warning(f"Embedding candidate-pair generation failed, falling back to "
                           f"every pair as a candidate: {e}")
            return all_pairs

        return [(ids[i], ids[j]) for i in range(len(ids)) for j in range(i + 1, len(ids))
                if float(np.dot(vecs[i], vecs[j])) >= _DUP_CANDIDATE_THRESHOLD]

    def _prompt_dup_check(self, candidate_pairs_json: str, language: str) -> str:
        return f"""<candidate_pairs>
{candidate_pairs_json}
</candidate_pairs>

<task>
Each pair below was flagged by similarity search as a possible duplicate: two requirements
extracted independently from different sections of the same document that may express the same
underlying obligation in different wording. For EACH pair, decide whether it is a genuine
duplicate. Judge only the pairs given -- do not look for others.
</task>

<rules>
- Ask specifically: strip away the shared subject matter -- is the VERB the same action? "Report
  U.S. TINs to the IRS" and "make reasonable efforts to collect the TINs" share the word "TINs" and
  the general subject, but reporting and collecting are different actions with different evidence --
  NOT a duplicate.
- DO mark as duplicate when the action is the same even if the wording differs a lot: "exert every
  effort possible to obtain the above information" and "make reasonable efforts to collect the
  TINs" are the same action (trying to obtain/collect) described in different words -- duplicate.
- The test is never "do these two sentences mention the same noun" -- it is "would satisfying one
  of these automatically satisfy the other, because they're asking for the identical action by the
  identical actor."
- A statement that REPEATS an obligation but adds a condition, threshold or deadline the other
  lacks is NOT a duplicate -- it is a narrower obligation, and a compliance program must keep both.
  Set dup: false and say so in w.
- Only mark dup: true if a compliance officer would reasonably treat them as one obligation
  restated twice, not two distinct (even if topically related) obligations.
- For every pair marked dup: true, also rate confidence: "high" if you are near-certain a
  compliance officer would treat them as one obligation (same actor, same specific action, wording
  differs only cosmetically), "low" if they look related but you are not fully certain they are the
  identical action.
- Return exactly one entry per pair given, in the same order, even when dup is false.
</rules>

<language_rules>
Read the requirements in {language}; your output is ids, a boolean and a short reason only.
</language_rules>

<output_format>
Return ONLY minified JSON on a single line: no line breaks, no markdown, no explanation.
Fields: a, b = the two requirement ids (copy exactly), dup = true|false, c = confidence ("high" or
"low", only meaningful when dup is true), w = one-line reason
Schema: {{"d":[{{"a":"","b":"","dup":true,"c":"","w":""}}]}}
</output_format>"""

    def _classification_shard(self, batch: List[dict], requirement_types: List[str],
                              language: str, depth: int = 0) -> List[dict]:
        # a/n (actor, nature) ride along from extraction -- disposition below
        # is decided from these two fields, not re-derived from the text.
        compact = [{"i": r["requirement_local_id"], "t": r["t"],
                    "a": r.get("a", ""), "n": r.get("n", "")} for r in batch]
        try:
            raw = self.client.complete(
                self._prompt_classification(
                    json.dumps(compact, ensure_ascii=False, separators=(",", ":")),
                    requirement_types, language),
                temperature=0.1, max_tokens=4000, expect_json=True, label="req-classify",
            )
            return self._parse_json(raw).get("r") or []
        except TruncatedResponseError:
            if len(batch) > 1 and depth < 3:
                mid = len(batch) // 2
                logger.warning(f"Classification truncated for a {len(batch)}-item batch; "
                               f"splitting and retrying")
                first = self._classification_shard(batch[:mid], requirement_types, language, depth + 1)
                second = self._classification_shard(batch[mid:], requirement_types, language, depth + 1)
                return first + second
            logger.error(f"Classification truncated and cannot be split further; "
                        f"{len(batch)} requirement(s) will not be classified")
            return []
        except StructuralLLMError:
            raise  # same reasoning as _extraction_shard's identical re-raise
        except Exception as e:
            logger.error(f"Classification failed for batch: {e}")
            return []

    def _prompt_classification(self, requirements_json: str, requirement_types: List[str],
                               language: str) -> str:
        types_block = " | ".join(requirement_types)
        return f"""<statements>
{requirements_json}
</statements>

<task>
For every statement above, assign exactly one requirement_type, one disposition, and a short
title. Disposition decides whether the statement is an obligation on the bank or is retained as
context only -- it is not a filter that removes the statement, every statement gets a row either
way.
</task>

<requirement_types>
{types_block}
</requirement_types>

<disposition_rules>
Each statement already carries "a" (actor, verbatim) and "n" (nature) from extraction. Assign
exactly one disposition from those two fields:
- OBL  -- actor is the bank (or a category of regulated entity the bank belongs to) and nature is
          mandatory or prohibition. A real obligation.
- COND -- actor is the bank and nature is conditional or discretionary with a compliance boundary
          attached. An obligation only if the bank relies on the permission.
- REG  -- actor is the regulator, a court, the Public Prosecution, a ministry, a government
          committee, or any body OTHER than the bank. Nothing for the bank to do.
- DEF  -- nature is definition. The statement defines a term or sets scope, no directive on the
          reader.
- INFO -- nature is informational. Provenance, preamble, commentary, transitional narrative.
Verb mood does not change the actor: "the Central Bank SHALL" is REG, not OBL, even inside an
article that is mostly about the bank's own obligations.
Give a one-line reason (w) whenever disposition is anything OTHER than OBL, so a reviewer can see
why a statement isn't downstream work without re-reading the source text.
</disposition_rules>

<type_disambiguation>
"Principle" is for a broad statement of legal intent or a general standard with no specific
behavioral instruction (e.g. "the law aims to protect depositors"). A specific "shall not do X"
or "must do X" instruction is NEVER "Principle", even when it appears inside a penalties,
sentencing, or general-provisions article -- classify it by what the instruction itself requires
(most often "Policy and Procedure"). Article location does not determine type; the sentence's
own content does.
</type_disambiguation>

<classification_rules>
- requirement_type must be EXACTLY one value from the <requirement_types> list above.
  Any value not in that list is invalid.
- title is a short 3-6 word label summarizing the statement.
- Do NOT invent statements or change their meaning. Do NOT repeat the statement text back.
</classification_rules>

<language_rules>
The document is in {language}. ALL text you write must be in {language}. Do NOT translate.
</language_rules>

<output_format>
Return ONLY minified JSON on a single line: no line breaks, no indentation, no markdown,
no code fences, no explanation.
Fields: i = statement id (copy exactly), y = requirement_type, h = title,
d = disposition (OBL | COND | REG | DEF | INFO), w = one-line reason (required when d is not OBL)
Schema: {{"r":[{{"i":"","y":"","h":"","d":"","w":""}}]}}
</output_format>"""

    @staticmethod
    def _coerce_type(value, allowed: List[str]) -> tuple:
        """(valid_value, was_coerced). requirement_types is data, not a fixed
        enum baked into this file -- match case-insensitively against
        whatever list the caller supplied."""
        if isinstance(value, str):
            for a in allowed:
                if value.strip().casefold() == a.casefold():
                    return a, 0
        return (allowed[0] if allowed else ""), 1

    @staticmethod
    def _coerce_disposition(value) -> tuple:
        """(valid_value, was_coerced). Unlike requirement_type, the
        disposition set IS fixed (_DISPOSITIONS) -- it's the schema itself,
        not caller-supplied taxonomy data. An invalid/missing value falls
        back to "INFO" rather than "OBL": a statement that failed to get a
        disposition should default to the safest bucket (context-only,
        flagged for review), never silently become downstream work."""
        if isinstance(value, str) and value.strip().upper() in _DISPOSITIONS:
            return value.strip().upper(), 0
        return "INFO", 1

    @staticmethod
    def _coerce_nature(value) -> tuple:
        """(valid_value, was_coerced). Same reasoning as _coerce_disposition:
        nature is DB CHECK-constrained (CK_Requirement_Nature), not free
        text, so an unrecognised value must never reach the INSERT. Falls
        back to "informational" -- the safest, most conservative bucket --
        rather than guessing "mandatory"."""
        if isinstance(value, str) and value.strip().casefold() in _NATURES:
            return value.strip().casefold(), 0
        return "informational", 1

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
