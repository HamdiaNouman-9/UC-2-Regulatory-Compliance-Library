import os
import json
import logging
import re
import threading
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher
from processor.llm_client import LLMClient
from processor.quoted_source import quote_source   # CHG-007: data markers
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)
from utils.lang_detector import detect_language
import os
from dotenv import load_dotenv
load_dotenv(override=True)

LANGUAGE_NAMES = {
    "ar": "Arabic",
    "en": "English",
    "fr": "French",
    "de": "German",
    "es": "Spanish",
}

# Bounds total concurrent OpenRouter calls across every analyzer instance and
# every worker thread, so document-level parallelism cannot stampede into 429s.
_LLM_MAX_CONCURRENCY = int(os.getenv("LLM_MAX_CONCURRENCY", "8"))
_LLM_SEMAPHORE = threading.Semaphore(_LLM_MAX_CONCURRENCY)

# ---------------------------------------------------------------------- #
#  Reproducibility                                                        #
#                                                                         #
#  OpenRouter serves deepseek-v3.2 from 14 different providers at         #
#  different quantizations (fp4, fp8, unknown) and picks one per call by  #
#  price/latency/load. A single analysis could therefore be produced by   #
#  two different inference engines -- an observed run had stages 1-2 on   #
#  AtlasCloud and stage 3 on StreamLake. Pinning the provider and         #
#  quantization removes the largest source of run-to-run drift.           #
#                                                                         #
#  See docs/determinism.md.                                               #
# ---------------------------------------------------------------------- #

_LLM_DETERMINISTIC   = os.getenv("LLM_DETERMINISTIC", "1") not in ("0", "false", "False")
_LLM_PROVIDER        = os.getenv("LLM_PROVIDER", "AtlasCloud")
_LLM_QUANTIZATION    = os.getenv("LLM_QUANTIZATION", "fp8")
_LLM_ALLOW_FALLBACKS = os.getenv("LLM_ALLOW_FALLBACKS", "0") not in ("0", "false", "False")
_LLM_SEED            = int(os.getenv("LLM_SEED", "20250101"))

# ---------------------------------------------------------------------- #
#  Controlled vocabularies. The prompts state these; nothing used to      #
#  enforce them, so out-of-taxonomy values reached the database.          #
# ---------------------------------------------------------------------- #

OBLIGATION_TYPES = ["Preventive", "Detective", "Governance", "Reporting", "Documentation"]
CRITICALITIES    = ["High", "Medium", "Low"]
EVIDENCE_TYPES   = ["Policy", "Procedure", "System Configuration", "Log",
                    "Approval", "Report", "Contract", "Record", "Other"]
EXECUTION_CATS   = ["Ongoing_Control", "One_Time_Implementation", "One_Off_Reporting",
                    "Governance_Approval", "Informational_No_Action"]
CONTROL_TYPES    = ["Preventive", "Detective"]
EXECUTION_TYPES  = ["Manual", "Automated", "Hybrid"]
CONTROL_LEVELS   = ["Policy", "Process", "System"]
RESIDUAL_RISKS   = ["Low", "Medium", "High"]

# Values the model reaches for that map cleanly onto the permitted taxonomy.
_TYPE_ALIASES = {
    "corrective": "Preventive",
    "compliance": "Governance",
    "monitoring": "Detective",
    "record keeping": "Documentation",
    "recordkeeping": "Documentation",
    "disclosure": "Reporting",
}


class TruncatedResponseError(RuntimeError):
    """The model hit max_tokens; the payload is incomplete and must not be parsed."""


class StagedLLMAnalyzer:
    """
    4-stage regulatory analysis pipeline.
    Produces one structured row per extracted requirement.

    The four reasoning stages are unchanged -- each is still its own focused
    call. What changed is everything that was not reasoning: the model now
    emits compact deltas instead of re-transcribing its own earlier output,
    non-reasoning work (exact dedup, table rendering, enum validation) moved
    to Python, and independent calls run concurrently.

    The JSON persisted to stage1_json / stage2_json / stage3_json / analysis_json
    keeps exactly the shape it had before -- lean model output is rehydrated
    into the full structure here, so no downstream consumer changes.
    """

    def __init__(self, model: str = "deepseek/deepseek-v3.2", max_workers: int = 4,
                 deterministic: Optional[bool] = None,
                 provider: Optional[str] = None,
                 quantization: Optional[str] = None):
        self.model = model
        self.max_workers = max_workers
        # Deterministic mode: temperature 0, fixed seed, pinned provider and
        # quantization. On by default -- a compliance library needs the same
        # document to analyse the same way twice. Set LLM_DETERMINISTIC=0 to
        # restore free routing and the per-stage temperatures.
        self.deterministic = _LLM_DETERMINISTIC if deterministic is None else deterministic
        self.provider = provider or _LLM_PROVIDER
        self.quantization = quantization or _LLM_QUANTIZATION
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
        iso = detect_language(text)
        doc_language = LANGUAGE_NAMES.get(iso, "English")

        # ---- Stage 1: extract (sharded) + group (deterministic) -------
        # Sharded per article/section so each call's output stays short --
        # long single-shot generations do not reproduce reliably across
        # runs even with temperature=0 and a pinned provider/seed (GPU
        # batching non-determinism cascades through long autoregressive
        # output; see docs/determinism.md). Grouping used to be a
        # whole-document LLM judgment call with an unenforced "2-6 per
        # group" prompt rule -- it is now deterministic code, so the same
        # extracted obligations always produce the same groups.
        raw_obligations = self._run_stage1_sharded(
            text, document_title, regulator, reference, publication_date, doc_language)
        if not raw_obligations:
            logger.warning(f"Stage 1 returned no obligations for regulation {regulation_id}")
            return []

        # Exact-duplicate removal is string equality, not reasoning -- doing it
        # here is both cheaper and more reliable than asking the model. Runs
        # across the whole merged list now, since duplicates can occur across
        # chunk boundaries and not just within one requirement group.
        deduped = self._dedupe_exact_flat(raw_obligations)
        if len(deduped) < len(raw_obligations):
            logger.info(
                f"Stage 1 exact-duplicate obligations removed: "
                f"{len(raw_obligations) - len(deduped)}")

        s1_data = self._group_obligations(deduped)
        s1_data["regulator"] = regulator
        s1_data["reference"] = reference
        s1_data["publication_date"] = publication_date
        if not s1_data.get("requirements"):
            logger.warning(f"Stage 1 grouping produced no requirements for regulation {regulation_id}")
            return []

        # ---- Stage 2: normalize + classify ----------------------------
        s2_data = self._run_stage2(s1_data, doc_language)

        # ---- Stages 3 and 4 run concurrently --------------------------
        # Stage 4's prose needs only stages 1-2; its tables are rendered in
        # Python. So it no longer has to wait for stage 3.
        ongoing = self._ongoing_by_requirement(s2_data)

        with ThreadPoolExecutor(max_workers=2) as pool:
            f_s3 = pool.submit(self._run_stage3, ongoing, doc_language)
            f_s4 = pool.submit(self._run_stage4_prose, s1_data, s2_data,
                               document_title, doc_language)
            s3_data = f_s3.result()
            prose = f_s4.result()

        rows = self._assemble_rows(s1_data, s2_data, s3_data, "", regulation_id)
        # Stage 4 markdown is rendered from the assembled rows, so its tables
        # cannot contain anything the pipeline did not actually produce.
        if rows:
            rows[0]["stage4_md"] = self._render_stage4(
                rows, prose, document_title, s2_data, s3_data)
        return rows

    # ------------------------------------------------------------------ #
    #  STAGE RUNNERS                                                       #
    # ------------------------------------------------------------------ #

    def _run_stage2(self, s1_data: dict, language: str) -> dict:
        """Classify every obligation, sharded per requirement group -- one
        call per group (2-6 obligations each, per the Stage 1 grouping
        rule) instead of one whole-document call. The single-call version
        was itself long enough (~5,000 prompt / ~6,500-6,900 completion
        tokens, ~185s measured) to be the dominant remaining source of
        classification drift after Stage 1 was sharded: obligation
        extraction stayed largely stable between two verification runs
        while execution_category distribution still swung hard (see
        docs/determinism.md and the Stage 1 sharding comment above).
        Mirrors _stage3_shard's pattern exactly -- that function already
        shards per requirement group for the very same reason."""
        index = self._index_stage1(s1_data)
        requirements = s1_data.get("requirements", [])
        if not requirements:
            return {"requirements": []}

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            results = list(pool.map(
                lambda req: self._stage2_shard(req, language), requirements))

        deltas = [d for shard_deltas in results for d in shard_deltas]
        return self._rehydrate_stage2(s1_data, index, deltas)

    def _stage2_shard(self, req: dict, language: str, depth: int = 0) -> List[dict]:
        """One requirement group's obligations. On truncation, split in
        half and retry -- mirrors _stage3_shard's self-healing pattern."""
        obs = req.get("obligations", [])
        if not obs:
            return []
        compact = [{"i": o["obligation_id"], "t": o["obligation_text"]} for o in obs]

        try:
            raw = self._call_llm(
                self._prompt_stage2(json.dumps(compact, ensure_ascii=False,
                                               separators=(",", ":")), language),
                temperature=0.1,
                max_tokens=4000,
                expect_json=True,
            )
            deltas = self._parse_json(raw).get("o") or []
        except TruncatedResponseError:
            if len(obs) > 1 and depth < 3:
                mid = len(obs) // 2
                logger.warning(
                    f"Stage 2 truncated for {req.get('requirement_id')} "
                    f"({len(obs)} obligations); splitting and retrying")
                first  = self._stage2_shard({**req, "obligations": obs[:mid]}, language, depth + 1)
                second = self._stage2_shard({**req, "obligations": obs[mid:]}, language, depth + 1)
                return first + second
            logger.error(
                f"Stage 2 truncated for {req.get('requirement_id')} and cannot be "
                f"split further; {len(obs)} obligation(s) will not be classified")
            return []
        except Exception as e:
            logger.error(f"Stage 2 failed for {req.get('requirement_id')}: {e}")
            return []

        return [d for d in deltas if isinstance(d, dict)]

    def _run_stage3(self, ongoing: List[dict], language: str) -> dict:
        """Design controls. One concurrent call per requirement group, so a
        single oversized generation can no longer truncate the whole stage."""
        if not ongoing:
            return {"requirements": []}

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            results = list(pool.map(
                lambda grp: self._stage3_shard(grp, language), ongoing))

        data={"requirements":[r for r in results if r]}
        expected=sum(len(g["obligations"]) for g in ongoing)
        produced=sum(1 for r in data["requirements"] for ob in r["obligations"] if ob.get("control"))
        if produced<expected:
            logger.warning(f"Stage 3 produced {produced} control(s) for {expected} ongoing "
            f"obligation(s) -- {expected-produced} missing")
        return data    

    def _stage3_shard(self, group: dict, language: str, depth: int = 0) -> Optional[dict]:
        """One requirement group. On truncation, split in half and retry --
        this is what makes stage 3 self-healing instead of silently empty."""
        obs = group["obligations"]
        payload = [{"i": o["obligation_id"], "t": o["obligation_text"]} for o in obs]

        try:
            raw = self._call_llm(
                self._prompt_stage3(json.dumps(payload, ensure_ascii=False,
                                               separators=(",", ":")), language),
                temperature=0.25,
                max_tokens=12000,
                expect_json=True,
            )
            controls = self._parse_json(raw).get("c") or []
        except TruncatedResponseError:
            if len(obs) > 1 and depth < 3:
                mid = len(obs) // 2
                logger.warning(
                    f"Stage 3 truncated for {group['requirement_id']} "
                    f"({len(obs)} obligations); splitting and retrying")
                halves = [
                    self._stage3_shard({**group, "obligations": obs[:mid]}, language, depth + 1),
                    self._stage3_shard({**group, "obligations": obs[mid:]}, language, depth + 1),
                ]
                merged = []
                for h in halves:
                    if h:
                        merged.extend(h["obligations"])
                return {
                    "requirement_id":    group["requirement_id"],
                    "requirement_title": group["requirement_title"],
                    "obligations":       merged,
                }
            logger.error(
                f"Stage 3 truncated for {group['requirement_id']} and cannot be "
                f"split further; {len(obs)} obligation(s) will have no control")
            return None
        except Exception as e:
            logger.error(f"Stage 3 failed for {group['requirement_id']}: {e}")
            return None

        by_id = {c.get("i"): c.get("ctrl") for c in controls if isinstance(c, dict)}
        return {
            "requirement_id":    group["requirement_id"],
            "requirement_title": group["requirement_title"],
            "obligations": [
                {
                    "obligation_id":      o["obligation_id"],
                    "obligation_text":    o["obligation_text"],
                    "execution_category": "Ongoing_Control",
                    "control":            self._expand_control(by_id.get(o["obligation_id"])),
                }
                for o in obs
            ],
        }

    def _run_stage4_prose(self, s1: dict, s2: dict, document_title: str,
                          language: str) -> dict:
        """Only the three genuinely-written sections. The tables are rendered
        from real data in _render_stage4()."""
        try:
            raw = self._call_llm(
                self._prompt_stage4(self._stage4_digest(s1, s2, document_title), language),
                temperature=0.3,
                max_tokens=2500,
                expect_json=True,
            )
            return self._parse_json(raw) or {}
        except Exception as e:
            logger.error(f"Stage 4 prose generation failed: {e}")
            return {}

    # ------------------------------------------------------------------ #
    #  STAGE 1 SHARDING + DETERMINISTIC GROUPING                          #
    #                                                                      #
    #  Stage 1 used to be one whole-document call that both extracted     #
    #  obligations AND decided how to group them into 4-8 topic clusters. #
    #  Long single-shot generations do not reproduce reliably across      #
    #  runs even with temperature=0 and a pinned provider/seed -- GPU     #
    #  batching non-determinism on a shared inference provider cascades   #
    #  through long autoregressive output (measured: ~700-char outputs    #
    #  reproduced 3/3 identical, ~14,000-char outputs differed every      #
    #  time -- see docs/determinism.md). Splitting extraction into short  #
    #  per-article calls keeps each call in the empirically-reproducible  #
    #  range, and moving the grouping decision into code removes the      #
    #  other source of run-to-run drift: an unenforced "2-6 obligations   #
    #  per group" prompt rule that both real production runs violated    #
    #  independently (observed 7- and 1-obligation groups on one doc).    #
    # ------------------------------------------------------------------ #

    _CHUNK_MAX_CHARS           = 3500
    _CHUNK_MAX_SECTIONS        = 5
    _MIN_OBLIGATIONS_PER_GROUP = 2
    _MAX_OBLIGATIONS_PER_GROUP = 6

    # Ported from utils/text_chunker.py's (unused) _split_by_sections --
    # same pattern set, adapted to bare lookahead matching because the text
    # reaching here has already had every newline collapsed by
    # LlmAnalyzer.normalize_input_text before analyze() is ever called.
    #
    # Split on numbered "Article N"/"Chapter N:"/"Section N" and gate each
    # match against a running expected-next-number sequence per marker
    # type: real regulatory text is full of inline cross-references
    # ("...referred to in Article 15..." inside Article 6's own text), and
    # those cite article numbers completely out of order. An ungated regex
    # treats every citation as a new section boundary, fragmenting the
    # document at every "Article N" mention instead of every real header --
    # measured on regulation 27868: 6 of 12 "split points" found this way
    # were citations inside Article 23's penalty clause, not real headers,
    # and the genuine Article 6-14 span got merged into one 20,000-char
    # section because no citation-free boundary was found inside it. A
    # genuine header sequence is strictly 1, 2, 3, ...; a citation is not.
    # Arabic/Roman-numeral/Part markers have no reliable digit to gate on,
    # so they're accepted unconditionally, same as before.
    _NUMBERED_HEADER_RE = re.compile(
        r'Chapter\s+(?P<chapter>\d+):|Article\s+(?P<article>\d+)|Section\s+(?P<section>\d+)'
    )
    _UNCONDITIONAL_HEADER_RE = re.compile(
        r'(?=CHAPTER\s+[IVXLCDM]+)|(?=Part\s+[A-Z0-9]+)|(?=الفصل\s+)|(?=المادة\s+)'
    )
    _ARTICLE_NUM_RE = re.compile(r'(\d+)')

    def _find_header_positions(self, text: str) -> List[int]:
        """Character offsets of genuine, sequential Article/Chapter/Section
        headers -- see the class-comment above _NUMBERED_HEADER_RE for why
        a plain regex match isn't enough on its own."""
        positions = []
        expected: Dict[str, int] = {}
        for m in self._NUMBERED_HEADER_RE.finditer(text):
            label = m.lastgroup
            num = int(m.group(label))
            nxt = expected.get(label)
            if nxt is None or num == nxt:
                positions.append(m.start())
                expected[label] = num + 1
        for m in self._UNCONDITIONAL_HEADER_RE.finditer(text):
            positions.append(m.start())
        return sorted(set(positions))

    def _chunk_document(self, text: str) -> List[str]:
        """Split flat document text into ordered chunks anchored at
        article/section boundaries, capped at ~_CHUNK_MAX_CHARS or
        _CHUNK_MAX_SECTIONS sections, whichever comes first."""
        text = (text or "").strip()
        if not text:
            return []
        if len(text) <= self._CHUNK_MAX_CHARS:
            return [text]

        positions = self._find_header_positions(text)
        if not positions:
            # No recognizable Article/Chapter/Section markers.
            return self._chunk_by_char_budget(text)

        bounds = sorted(set([0] + positions + [len(text)]))
        raw_sections = []
        for i in range(len(bounds) - 1):
            sec = text[bounds[i]:bounds[i + 1]].strip()
            if sec:
                raw_sections.append(sec)

        # Break down any individually oversized section before bucketing --
        # a single genuinely long article (or, with header detection now
        # gated, any stretch with no accepted header inside it) would
        # otherwise still exceed the reproducible-output budget on its own.
        sections = []
        for sec in raw_sections:
            if len(sec) > self._CHUNK_MAX_CHARS:
                sections.extend(self._chunk_by_char_budget(sec))
            else:
                sections.append(sec)

        chunks, current, current_len = [], [], 0
        for section in sections:
            if current and (current_len + len(section) > self._CHUNK_MAX_CHARS
                             or len(current) >= self._CHUNK_MAX_SECTIONS):
                chunks.append(" ".join(current))
                current, current_len = [], 0
            current.append(section)
            current_len += len(section)
        if current:
            chunks.append(" ".join(current))
        return chunks

    def _chunk_by_char_budget(self, text: str) -> List[str]:
        """Fallback for documents with no Article/Chapter/Section markers:
        break at the nearest sentence boundary before the char budget."""
        chunks = []
        start, n = 0, len(text)
        while start < n:
            end = min(start + self._CHUNK_MAX_CHARS, n)
            if end < n:
                last_period = text.rfind(". ", start, end)
                if last_period > start + self._CHUNK_MAX_CHARS // 2:
                    end = last_period + 1
            chunks.append(text[start:end].strip())
            start = end
        return [c for c in chunks if c]

    def _run_stage1_sharded(self, text: str, document_title: str, regulator: str,
                            reference: str, publication_date: str,
                            language: str) -> List[dict]:
        """Extract obligations per chunk, concurrently. Global concurrency
        is already bounded by _LLM_SEMAPHORE, so no new control is needed."""
        chunks = self._chunk_document(text)
        if not chunks:
            return []
        if len(chunks) == 1:
            return self._stage1_shard(chunks[0], document_title, regulator,
                                      reference, publication_date, language)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            results = list(pool.map(
                lambda c: self._stage1_shard(c, document_title, regulator,
                                             reference, publication_date, language),
                chunks))
        merged = [ob for chunk_obs in results for ob in chunk_obs]
        logger.info(
            f"Stage 1 sharded extraction: {len(chunks)} chunk(s) -> "
            f"{len(merged)} raw obligation(s)")
        return merged

    def _stage1_shard(self, chunk: str, document_title: str, regulator: str,
                      reference: str, publication_date: str, language: str,
                      depth: int = 0) -> List[dict]:
        """One document chunk. On truncation, split the chunk text in half
        and retry -- mirrors _stage3_shard's self-healing pattern."""
        try:
            raw = self._call_llm(
                self._prompt_stage1_chunk(chunk, document_title, regulator,
                                          reference, publication_date, language),
                temperature=0.1,
                max_tokens=8000,
                expect_json=True,
            )
            obs = self._parse_json(raw).get("o") or []
        except TruncatedResponseError:
            if len(chunk) > 500 and depth < 3:
                mid = len(chunk) // 2
                split_at = chunk.rfind(" ", 0, mid)
                split_at = split_at if split_at > 0 else mid
                logger.warning(
                    f"Stage 1 truncated for a {len(chunk)}-char chunk; "
                    f"splitting and retrying")
                first  = self._stage1_shard(chunk[:split_at], document_title, regulator,
                                            reference, publication_date, language, depth + 1)
                second = self._stage1_shard(chunk[split_at:], document_title, regulator,
                                            reference, publication_date, language, depth + 1)
                return first + second
            logger.error(
                f"Stage 1 truncated and cannot be split further; "
                f"{len(chunk)}-char chunk dropped")
            return []
        except Exception as e:
            logger.error(f"Stage 1 chunk failed: {e}")
            return []

        return [o for o in obs if isinstance(o, dict) and o.get("t")]

    def _dedupe_exact_flat(self, obligations: List[dict]) -> List[dict]:
        """Same normalization as _dedupe_exact, applied once across the
        whole merged list -- duplicates can now occur across chunk
        boundaries, not just within one requirement group."""
        seen, kept = set(), []
        for ob in obligations:
            key = self._norm(ob.get("t", ""))
            if not key or key in seen:
                continue
            seen.add(key)
            kept.append(ob)
        return kept

    def _article_sort_key(self, source_reference: str) -> float:
        """Leading number in a source_reference like 'Article 9(2)(a)', for
        deterministic ordering. No match sorts last, never raises."""
        m = self._ARTICLE_NUM_RE.search(source_reference or "")
        return float(m.group(1)) if m else float("inf")

    def _group_obligations(self, obligations: List[dict]) -> dict:
        """Deterministic replacement for the LLM's former whole-document
        grouping judgment: cluster by the per-obligation topic tag assigned
        during the short, reproducible per-chunk extraction step, then
        enforce the 2-6-per-group rule in code instead of unenforced prompt
        text. Output matches the original s1_data schema exactly, so
        stages 2-4 and _assemble_rows need no changes."""
        if not obligations:
            return {"requirements": []}

        clusters: Dict[str, List[dict]] = {}
        labels: Dict[str, str] = {}
        for ob in obligations:
            topic = (ob.get("p") or "General").strip() or "General"
            key = self._norm(topic) or "general"
            clusters.setdefault(key, []).append(ob)
            labels.setdefault(key, topic)

        groups = [{"label": labels[k], "obligations": obs} for k, obs in clusters.items()]
        groups = self._merge_undersized_groups(groups)
        groups = self._split_oversized_groups(groups)

        if not (4 <= len(groups) <= 8):
            logger.info(
                f"Stage 1 grouping produced {len(groups)} requirement group(s) "
                f"(soft target 4-8; the 2-6-per-group rule is the one that's enforced)")

        groups.sort(key=lambda g: min(
            (self._article_sort_key(o.get("s", "")) for o in g["obligations"]),
            default=float("inf")))

        requirements = []
        for i, g in enumerate(groups, start=1):
            req_id = f"REQ-{i:03d}"
            requirements.append({
                "requirement_id":    req_id,
                "requirement_title": g["label"],
                "obligations": [
                    {
                        "obligation_id":    f"{req_id}-OB-{j:03d}",
                        "obligation_text":  o.get("t", ""),
                        "source_reference": o.get("s", ""),
                    }
                    for j, o in enumerate(g["obligations"], start=1)
                ],
            })
        return {"requirements": requirements}

    def _merge_undersized_groups(self, groups: List[dict]) -> List[dict]:
        """Fold any cluster with fewer than _MIN_OBLIGATIONS_PER_GROUP
        obligations into its most similarly-labeled neighbor. Terminates
        because every merge strictly reduces the group count."""
        if len(groups) <= 1:
            return groups
        changed = True
        while changed and len(groups) > 1:
            changed = False
            for i, g in enumerate(groups):
                if len(g["obligations"]) >= self._MIN_OBLIGATIONS_PER_GROUP:
                    continue
                others = [j for j in range(len(groups)) if j != i]
                best = max(others, key=lambda j: SequenceMatcher(
                    None, g["label"].casefold(), groups[j]["label"].casefold()).ratio())
                groups[best]["obligations"].extend(g["obligations"])
                del groups[i]
                changed = True
                break
        return groups

    def _split_oversized_groups(self, groups: List[dict]) -> List[dict]:
        """Split any cluster with more than _MAX_OBLIGATIONS_PER_GROUP
        obligations into balanced sub-groups (never a leftover 1-item
        tail), labeled with their article range to stay distinct."""
        out = []
        for g in groups:
            obs = g["obligations"]
            if len(obs) <= self._MAX_OBLIGATIONS_PER_GROUP:
                out.append(g)
                continue
            obs_sorted = sorted(obs, key=lambda o: self._article_sort_key(o.get("s", "")))
            n = len(obs_sorted)
            n_parts = -(-n // self._MAX_OBLIGATIONS_PER_GROUP)  # ceil division
            base, extra = divmod(n, n_parts)
            idx = 0
            for part in range(n_parts):
                size = base + (1 if part < extra else 0)
                sub = obs_sorted[idx: idx + size]
                idx += size
                refs = [o.get("s", "") for o in sub if o.get("s")]
                label = g["label"]
                if refs:
                    label = (f"{g['label']} ({refs[0]}–{refs[-1]})"
                             if len(refs) > 1 else f"{g['label']} ({refs[0]})")
                out.append({"label": label, "obligations": sub})
        return out

    # ------------------------------------------------------------------ #
    #  STAGE 1/2 SUPPORT                                                   #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _norm(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "")).strip().casefold()

    def _dedupe_exact(self, s1_data: dict) -> int:
        """Drop obligations whose text is identical after whitespace/case
        normalization. Pure string equality -- see docs/staged_analyzer_optimization.md §3a."""
        removed=0
        for req in s1_data.get("requirements", []):
            seen=set()
            kept=[]
            for ob in req.get("obligations", []):
                key = self._norm(ob.get("obligation_text", ""))
                if not key or key in seen:
                    removed += 1
                    continue
                seen.add(key)
                kept.append(ob)
            req["obligations"] = kept
        s1_data["requirements"] = [
            r for r in s1_data.get("requirements", []) if r.get("obligations")]
        return removed

    @staticmethod
    def _index_stage1(s1_data: dict) -> Dict[str, Tuple[dict, dict]]:
        return {
            ob["obligation_id"]: (ob, req)
            for req in s1_data.get("requirements", [])
            for ob in req.get("obligations", [])
            if ob.get("obligation_id")
        }

    def _rehydrate_stage2(self, s1_data: dict, index: Dict[str, Tuple[dict, dict]],
                          deltas: List[dict]) -> dict:
        """Merge classification deltas back onto stage 1 text, producing the
        exact normalized_obligations structure downstream already expects."""
        by_req: Dict[str, List[dict]] = {}
        coerced = 0

        for d in deltas:
            if not isinstance(d, dict):
                continue
            ob_id = d.get("i")
            if not ob_id:
                continue

            parent = index.get(ob_id) or self._find_parent(ob_id, index)
            if not parent:
                logger.warning(f"Stage 2 returned unknown obligation id {ob_id}; skipped")
                continue
            src_ob, src_req = parent

            # `n` is present only when the model split or rewrote the text.
            text = d.get("n") or src_ob.get("obligation_text", "")

            ob_type, c1 = self._coerce(d.get("t"), OBLIGATION_TYPES, "Documentation", _TYPE_ALIASES)
            crit,    c2 = self._coerce(d.get("c"), CRITICALITIES, "Medium")
            exec_c,  c3 = self._coerce(d.get("x"), EXECUTION_CATS, "Informational_No_Action")
            evidence = [self._coerce(e, EVIDENCE_TYPES, "Other")[0] for e in (d.get("e") or [])]
            coerced += c1 + c2 + c3

            clarity = d.get("q")
            if not isinstance(clarity, int) or not 1 <= clarity <= 5:
                clarity = 3

            by_req.setdefault(src_req["requirement_id"], []).append({
                "obligation_id":       ob_id,
                "obligation_text":     text,
                "obligation_type":     ob_type,
                "criticality":         crit,
                "evidence_expected":   evidence,
                "test_method":         d.get("m", ""),
                "clarity_score":       clarity,
                "needs_manual_review": bool(d.get("r")) or bool(c1 or c2 or c3),
                "source_reference":    src_ob.get("source_reference", ""),
                "execution_category":  exec_c,
            })

        if coerced:
            logger.warning(
                f"Stage 2 produced {coerced} value(s) outside the permitted taxonomy; "
                f"coerced and flagged needs_manual_review")

        return {
            "requirements": [
                {
                    "requirement_id":        req["requirement_id"],
                    "requirement_title":     req.get("requirement_title", ""),
                    "normalized_obligations": by_req.get(req["requirement_id"], []),
                }
                for req in s1_data.get("requirements", [])
            ]
        }

    @staticmethod
    def _find_parent(ob_id: str, index: Dict[str, Tuple[dict, dict]]):
        """Split obligations get suffixed ids (REQ-001-OB-003-A); resolve them
        back to the stage 1 obligation they came from."""
        best = None
        for known in index:
            if ob_id.startswith(known) and (best is None or len(known) > len(best)):
                best = known
        return index.get(best) if best else None

    @staticmethod
    def _coerce(value, allowed: List[str], default: str,
                aliases: Optional[dict] = None) -> Tuple[str, int]:
        """Return (valid_value, was_coerced). Nothing enforced these before."""
        if isinstance(value, str):
            for a in allowed:
                if value.strip().casefold() == a.casefold():
                    return a, 0
            if aliases:
                mapped = aliases.get(value.strip().casefold())
                if mapped:
                    return mapped, 1
        return default, 1

    def _ongoing_by_requirement(self, s2_data: dict) -> List[dict]:
        out = []
        for req in s2_data.get("requirements", []):
            obs = [ob for ob in req.get("normalized_obligations", [])
                   if ob.get("execution_category") == "Ongoing_Control"]
            if obs:
                out.append({
                    "requirement_id":    req["requirement_id"],
                    "requirement_title": req.get("requirement_title", ""),
                    "obligations":       obs,
                })
        return out

    _CONTROL_FIELDS = [
        ("title",    "control_title",            str,  ""),
        ("obj",      "control_objective",        str,  ""),
        ("desc",     "control_description",      str,  ""),
        ("owner",    "control_owner",            str,  ""),
        ("type",     "control_type",             str,  ""),
        ("exec",     "execution_type",           str,  ""),
        ("freq",     "frequency",                str,  ""),
        ("level",    "control_level",            str,  ""),
        ("evidence", "evidence_generated",       str,  ""),
        ("steps",    "key_steps",                list, None),
        ("risk",     "residual_risk_if_failed",  str,  ""),
    ]

    def _expand_control(self, c: Optional[dict]) -> Optional[dict]:
        """Short model keys -> the full field names downstream expects."""
        if not isinstance(c, dict):
            return None
        out = {}
        for short, full, kind, empty in self._CONTROL_FIELDS:
            v = c.get(short)
            if kind is list:
                out[full] = v if isinstance(v, list) else []
            else:
                out[full] = v if isinstance(v, str) else empty
        out["control_type"]            = self._coerce(out["control_type"], CONTROL_TYPES, "Preventive")[0]
        out["execution_type"]          = self._coerce(out["execution_type"], EXECUTION_TYPES, "Manual")[0]
        out["control_level"]           = self._coerce(out["control_level"], CONTROL_LEVELS, "Process")[0]
        out["residual_risk_if_failed"] = self._coerce(out["residual_risk_if_failed"], RESIDUAL_RISKS, "Medium")[0]
        return out

    # ------------------------------------------------------------------ #
    #  PROMPTS                                                             #
    # ------------------------------------------------------------------ #

    # Repeated in every JSON stage. ~26% of output used to be indentation.
    _COMPACT = ("Return MINIFIED JSON on a single line: no line breaks, no indentation, "
                "no spaces between tokens. No markdown, no code fences, no explanation.")

    def _prompt_stage1(
            self,
            text: str,
            document_title: str,
            regulator: str = "",
            reference: str = "",
            publication_date: str = "",
             language="English"
    ) -> str:
        q = quote_source(text, "document")                # CHG-007
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
- The document is in {language}. ALL output fields must be written in {language}.
- Do NOT translate. Preserve exact regulatory meaning in the original language.

━━━ OUTPUT ━━━
{self._COMPACT}
Schema:
{{"regulator":"","reference":"","publication_date":"","requirements":[{{"requirement_id":"REQ-001","requirement_title":"","obligations":[{{"obligation_id":"REQ-001-OB-001","obligation_text":"","source_reference":""}}]}}]}}

Document Title: {document_title}
Regulator: {regulator}
Reference Number: {reference}
Publication Date: {publication_date}

Regulation Text:
{q.block}
{q.notice}
"""

    def _prompt_stage1_chunk(
            self,
            chunk_text: str,
            document_title: str,
            regulator: str = "",
            reference: str = "",
            publication_date: str = "",
            language="English"
    ) -> str:
        """Per-chunk extraction only -- no grouping, no ID assignment.
        Grouping is decided deterministically in code (_group_obligations)
        after all chunks are merged, so this call stays short and, per
        docs/determinism.md, reproducibly so."""
        q = quote_source(chunk_text, "document")          # CHG-007
        return f"""You are a senior regulatory compliance analyst.
Extract atomic obligations from the excerpt of a regulatory circular below.
This is one part of a larger document -- extract only what is in THIS excerpt.

━━━ EXTRACTION RULES ━━━
- Extract ONLY binding obligations that use words like: must, shall, required to, obligated to.
- Ignore explanatory text, preambles, definitions, and non-binding guidance.
- If a sentence contains more than one distinct action, split it into separate atomic obligations.
- Do not split obligations that share a single subject and are logically inseparable into one action.
- Preserve the exact regulatory meaning — do not paraphrase or interpret beyond what is written.
- Do not invent any content.
- Each obligation must be independently testable by an auditor.
- Note each obligation's source reference (e.g. article/section number) exactly as it appears.

━━━ TOPIC TAGGING ━━━
- Tag each obligation with a short 2-4 word compliance topic label, e.g. "Capital Adequacy",
  "Licensing", "Credit Limits", "Reporting", "Governance", "Liquidity", "Consumer Protection",
  "Data Confidentiality", "Real Estate", "Investment Limits". Prefer one of these when it
  genuinely fits; otherwise write a short, precise label of your own.
- Do NOT decide how obligations should be grouped into requirement clusters — that happens
  separately, outside this call. Just tag the topic per obligation.

━━━ DEDUPLICATION RULES ━━━
- Before returning, check every obligation against all others in THIS excerpt.
- If two obligations share the same core action and subject, keep only one — discard the duplicate entirely.
- Do not extract the same sentence or list item twice even if it appears more than once in the source text.

━━━ LANGUAGE RULES ━━━
- The document is in {language}. ALL output fields must be written in {language}.
- Do NOT translate. Preserve exact regulatory meaning in the original language.

━━━ OUTPUT ━━━
{self._COMPACT}
Fields: t = obligation_text, s = source_reference, p = topic
Schema:
{{"o":[{{"t":"","s":"","p":""}}]}}

Document Title: {document_title}
Regulator: {regulator}
Reference Number: {reference}
Publication Date: {publication_date}

Regulation Excerpt:
{q.block}
{q.notice}
"""

    def _prompt_stage2(self, obligations_json: str, language="English") -> str:
        return f"""You are refining previously extracted regulatory obligations.

For EVERY obligation below, decide:
1. obligation_type  — exactly one of: {" | ".join(OBLIGATION_TYPES)}
2. criticality      — exactly one of: {" | ".join(CRITICALITIES)}
3. evidence_expected — array from: {" | ".join(EVIDENCE_TYPES)}
4. test_method      — ONE sentence: how an auditor verifies this.
5. clarity_score    — integer 1-5.
6. needs_manual_review — true only if the obligation cannot be made atomic.
7. execution_category — exactly one of: {" | ".join(EXECUTION_CATS)}

If an obligation still contains more than one distinct action, SPLIT it: emit one
entry per action, giving each a suffixed id ("<id>-A", "<id>-B", ...) and putting
the new wording in "n". Otherwise omit "n" entirely — do not repeat text back.

Rules:
- Do NOT invent obligations. Do NOT change regulatory meaning.
- Use ONLY the listed values. Any other value is invalid.
- The document is in {language}. ALL text you write must be in {language}. Do NOT translate.

━━━ OUTPUT ━━━
{self._COMPACT}
Emit one entry per obligation using these exact short keys:
  i = obligation_id, t = obligation_type, c = criticality, e = evidence_expected,
  m = test_method, q = clarity_score, r = needs_manual_review, x = execution_category,
  n = new text (ONLY when splitting or rewording)
Schema:
{{"o":[{{"i":"","t":"","c":"","e":[""],"m":"","q":5,"r":false,"x":""}}]}}

Obligations:
{obligations_json}
"""

    def _prompt_stage3(self, obligations_json: str, language="English") -> str:
        return f"""You are a senior banking internal controls architect.

Design exactly ONE internal control for each obligation below. Every obligation
listed is an ongoing control obligation and requires a control.

Each control must be auditable and specific, with these fields:
  title    = control_title
  obj      = control_objective (one sentence)
  desc     = control_description (2-3 sentences)
  owner    = control_owner (realistic bank department)
  type     = {" | ".join(CONTROL_TYPES)}
  exec     = {" | ".join(EXECUTION_TYPES)}
  freq     = frequency (e.g. Daily | Weekly | Per-Transaction | Event-Driven)
  level    = {" | ".join(CONTROL_LEVELS)}
  evidence = evidence_generated (the artifact proving this ran)
  steps    = key_steps (array of 3-5 short steps)
  risk     = {" | ".join(RESIDUAL_RISKS)}

Rules:
- Use ONLY the listed values for type, exec, level and risk.
- Do not echo the obligation text back.
- The document is in {language}. ALL text you write must be in {language}. Do NOT translate.

━━━ OUTPUT ━━━
{self._COMPACT}
Schema:
{{"c":[{{"i":"<obligation_id>","ctrl":{{"title":"","obj":"","desc":"","owner":"","type":"","exec":"","freq":"","level":"","evidence":"","steps":[""],"risk":""}}}}]}}

Obligations:
{obligations_json}
"""

    def _prompt_stage4(self, digest: str, language="English") -> str:
        return f"""You are compiling a formal enterprise regulatory impact document.

Using ONLY the summary data below, write THREE narrative sections. The statistical
tables are generated separately and are not your responsibility.

1. executive_summary  — 3-5 sentences on what this regulation requires and its impact.
2. requirement_overview — 3-5 sentences describing the requirement groups.
3. implications — 3-5 sentences on architectural and operational implications.

Rules:
- Use only the information given. Do NOT invent regulatory content.
- Do NOT invent controls, owners, systems or numbers that are not listed below.
- Professional, concise, enterprise tone.
- The document is in {language}. Write ALL text in {language}. Do NOT translate.

━━━ OUTPUT ━━━
{self._COMPACT}
Schema:
{{"executive_summary":"","requirement_overview":"","implications":""}}

{digest}
"""

    # ------------------------------------------------------------------ #
    #  STAGE 4 RENDERING — tables come from real data, never the model     #
    # ------------------------------------------------------------------ #

    def _stage4_digest(self, s1: dict, s2: dict, document_title: str) -> str:
        obs = [ob for r in s2.get("requirements", [])
               for ob in r.get("normalized_obligations", [])]
        titles = [f"- {r.get('requirement_title', '')} "
                  f"({len(r.get('normalized_obligations', []))} obligations)"
                  for r in s2.get("requirements", [])]
        return (
            f"Document: {document_title}\n"
            f"Regulator: {s1.get('regulator', '')}\n"
            f"Reference: {s1.get('reference', '')}\n"
            f"Requirement groups ({len(s2.get('requirements', []))}):\n"
            + "\n".join(titles) + "\n"
            f"Total obligations: {len(obs)}\n"
            f"By criticality: {json.dumps(self._tally(obs, 'criticality'), ensure_ascii=False)}\n"
            f"By execution category: {json.dumps(self._tally(obs, 'execution_category'), ensure_ascii=False)}\n"
            f"By obligation type: {json.dumps(self._tally(obs, 'obligation_type'), ensure_ascii=False)}\n"
        )

    @staticmethod
    def _tally(items: List[dict], field: str) -> dict:
        out: Dict[str, int] = {}
        for it in items:
            v = it.get(field)
            if v:
                out[v] = out.get(v, 0) + 1
        return dict(sorted(out.items(), key=lambda kv: -kv[1]))

    @staticmethod
    def _md_escape(text: str) -> str:
        return (text or "").replace("|", "\\|").replace("\n", " ").strip()

    _OWNER_BY_CATEGORY = {
        "Ongoing_Control":         "Compliance / Operations",
        "One_Time_Implementation": "Change / Programme Management",
        "One_Off_Reporting":       "Regulatory Reporting",
        "Governance_Approval":     "Board / Executive Governance",
        "Informational_No_Action": "Compliance (monitoring only)",
    }

    def _render_stage4(self, rows: List[dict], prose: dict, document_title: str,
                       s2: dict, s3: dict) -> str:
        obs = [ob for r in s2.get("requirements", [])
               for ob in r.get("normalized_obligations", [])]
        controls = [ob["control"]
                    for r in s3.get("requirements", [])
                    for ob in r.get("obligations", [])
                    if ob.get("control")]

        out = [f"# Regulatory Impact Analysis — {document_title}", ""]

        out += ["## 1. Executive Summary", "",
                prose.get("executive_summary") or "_Not generated._", ""]
        out += ["## 2. Requirement Overview", "",
                prose.get("requirement_overview") or "_Not generated._", ""]

        out += ["## 3. Obligation Inventory", "",
                "| Obligation ID | Text | Type | Criticality | Execution Category |",
                "| --- | --- | --- | --- | --- |"]
        for ob in obs:
            text = self._md_escape(ob.get("obligation_text", ""))
            if len(text) > 160:
                text = text[:157] + "..."
            out.append(
                f"| {self._md_escape(ob.get('obligation_id'))} | {text} "
                f"| {self._md_escape(ob.get('obligation_type'))} "
                f"| {self._md_escape(ob.get('criticality'))} "
                f"| {self._md_escape(ob.get('execution_category'))} |")
        out.append("")

        out += ["## 4. Execution Classification Summary", "",
                "| Category | Count | Primary Owner |", "| --- | ---: | --- |"]
        for cat, n in self._tally(obs, "execution_category").items():
            out.append(f"| {cat} | {n} | {self._OWNER_BY_CATEGORY.get(cat, 'Compliance')} |")
        out.append("")

        out += ["## 5. Control Engineering Summary", ""]
        if controls:
            out += ["| Control Title | Owner | Type | Execution | Frequency | Residual Risk |",
                    "| --- | --- | --- | --- | --- | --- |"]
            for c in controls:
                out.append(
                    f"| {self._md_escape(c.get('control_title'))} "
                    f"| {self._md_escape(c.get('control_owner'))} "
                    f"| {self._md_escape(c.get('control_type'))} "
                    f"| {self._md_escape(c.get('execution_type'))} "
                    f"| {self._md_escape(c.get('frequency'))} "
                    f"| {self._md_escape(c.get('residual_risk_if_failed'))} |")
        else:
            ongoing = sum(1 for ob in obs if ob.get("execution_category") == "Ongoing_Control")
            out.append(
                "_No controls were designed for this regulation._"
                if ongoing == 0 else
                f"_No controls were produced despite {ongoing} ongoing-control "
                f"obligation(s). This indicates a Stage 3 failure and requires review._")
        out.append("")

        out += ["## 6. Architectural & Operational Implications", "",
                prose.get("implications") or "_Not generated._", ""]

        return "\n".join(out)

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

            obligations = s2_req.get("normalized_obligations") or req.get("obligations", [])

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

    def _clean_stage4(self, raw: str) -> str:
        """Strip JSON wrappers and code fences from markdown output."""
        raw = raw.strip()
        raw = re.sub(r'^```(?:json)?\s*\n?', '', raw, flags=re.IGNORECASE)
        raw = re.sub(r'\n?```\s*$', '', raw)
        raw = raw.strip()
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for key in ("report", "markdown", "content", "output"):
                    if key in parsed and isinstance(parsed[key], str):
                        return parsed[key]
        except Exception:
            pass
        return raw

    def _parse_json(self, text: str) -> dict:
        text = text.strip()
        text = re.sub(r'^```(?:json)?\s*\n?', '', text, flags=re.IGNORECASE | re.MULTILINE)
        text = re.sub(r'\n?```\s*$', '', text, flags=re.MULTILINE)
        text = re.sub(r',(\s*[}\]])', r'\1', text)
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse failed: {e} | preview: {text[:200]}")
            return {}

    # Transient conditions worth another attempt.
    _RETRY_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}

    def _call_llm(self, prompt: str, temperature: float = 0.1,
                  max_tokens: int = 8000, expect_json: bool = False,
                  attempts: int = 3) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:3000",
            "X-Title": "Saudi Banking Compliance Copilot",
        }
        payload = {
            "model": self.model,
            # Sampling at temperature > 0 is itself a source of run-to-run
            # variance; in deterministic mode we take the greedy path.
            "temperature": 0 if self.deterministic else temperature,
            "max_tokens": max_tokens,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a senior banking compliance officer specialising in SAMA and CMA regulations. "
                        "Return only valid JSON unless explicitly told otherwise. Never hallucinate. "
                        "Always respond in the same language as the source document."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }
        if expect_json:
            payload["response_format"] = {"type": "json_object"}

        if self.deterministic:
            payload["top_p"] = 1
            payload["seed"] = _LLM_SEED
            # Pin the upstream engine. allow_fallbacks=False means a call fails
            # loudly rather than silently coming back from a different provider
            # (and possibly a different quantization) than every other call.
            payload["provider"] = {
                "order":          [self.provider],
                "allow_fallbacks": _LLM_ALLOW_FALLBACKS,
                "quantizations":  [self.quantization],
            }

        last_exc = None
        for attempt in range(attempts):
            if attempt:
                time.sleep(min(2 ** attempt, 8))
            try:
                with _LLM_SEMAPHORE:
                    resp = requests.post(
                        "https://openrouter.ai/api/v1/chat/completions",
                        headers=headers, json=payload, timeout=180
                    )
                if resp.status_code in self._RETRY_STATUS:
                    last_exc = RuntimeError(
                        f"OpenRouter HTTP {resp.status_code}: {resp.text[:200]}")
                    logger.warning(f"{last_exc} (attempt {attempt + 1}/{attempts})")
                    continue
                resp.raise_for_status()

                body = resp.json()
                choice = (body.get("choices") or [{}])[0]

                # Truncated JSON used to be parsed anyway, fail silently, and
                # discard the whole stage. Surface it instead.
                if choice.get("finish_reason") == "length":
                    raise TruncatedResponseError(
                        f"hit max_tokens={max_tokens}; response incomplete")

                content = (choice.get("message") or {}).get("content")
                return content.strip() if content else "{}"

            except TruncatedResponseError:
                raise
            except requests.exceptions.RequestException as e:
                last_exc = e
                logger.warning(f"OpenRouter request error: {e} (attempt {attempt + 1}/{attempts})")

        logger.error(f"OpenRouter API error after {attempts} attempts: {last_exc}")
        raise last_exc if last_exc else RuntimeError("OpenRouter call failed")
