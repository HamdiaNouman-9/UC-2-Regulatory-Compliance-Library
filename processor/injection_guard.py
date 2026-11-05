"""processor/injection_guard.py -- CHG-008: hold a document for a person when
it tries to instruct the model.

Why
---
CHG-006 removes hidden text and CHG-007 marks every source as quoted data,
which reduce how often an embedded instruction reaches the model. What gets
past them still goes to analysis unseen, and nobody is told. This module
is the step that tells someone: before analysis, the page text and each
attachment are scanned for instruction phrases, for copies of our own
prompt structure, and for a large share of hidden text. A hit does not stop
the crawl or lose the document; in enforce mode the row is held with status
"quarantine" and its analysis waits for a reviewer.

What it does
------------
``inspect(page_text, files, hidden_text)`` returns a ``Verdict``: the hits
(source, pattern, matched text, context), the hidden-text share that
triggered if any, and ``as_meta()`` for ``extra_meta["injection_guard"]``.
``mode()`` says whether a hit is enforced or only observed:
``INJECTION_GUARD_MODE`` in the environment, else ``mode:`` in
``config/injection_patterns.yml``, else observe.

Patterns come from ``config/injection_patterns.yml``; the detector is one
class behind a two-method interface so a classifier can be added beside it
later (``inspect(..., detectors=[...])``).

What it does not do
-------------------
It does not change a row, write anything, or call a model. The orchestrator
decides what to do with the verdict (CHG-008 step 3); the API releases a
held row through the accept/reject decisions (step 4).
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG = REPO_ROOT / "config" / "injection_patterns.yml"
ENV_MODE = "INJECTION_GUARD_MODE"
MODES = ("observe", "enforce")
STATUS_QUARANTINE = "quarantine"

CONTEXT_CHARS = 80          # either side of a match, in the recorded context
MAX_HITS_PER_SOURCE = 5     # enough for a reviewer; a page full of hits is one hit

# CHG-007's marker shape and the analyser prompts' own tag names. Kept here
# as patterns rather than imported from processor.quoted_source, so this
# module stands on its own on any branch and the list is visible in one
# place with the phrases.
_PROMPT_TAGS = ("document", "context", "task", "requirements", "extraction_rules",
                "output_format", "language_rules", "rules", "statements")
_MARKER_SHAPES = (
    r"\[\[\[[ \t]*(?:BEGIN|END)\b[^\]\n]{0,120}\]\]\]",
    r"<[ \t]*/?[ \t]*(?:" + "|".join(_PROMPT_TAGS) + r")\b[^<>\n]{0,100}>",
)


# ----------------------------------------------------------------- results

@dataclass(frozen=True)
class Hit:
    source: str         # "page" or the attachment's name
    pattern: str        # the pattern as written in the config, or "marker_shape"
    match: str          # the matched text
    context: str        # ... text around it ...

    def as_dict(self) -> dict:
        return {"source": self.source, "pattern": self.pattern,
                "match": self.match, "context": self.context}


@dataclass
class Verdict:
    hits: List[Hit] = field(default_factory=list)
    hidden: List[dict] = field(default_factory=list)     # [{"source", "ratio", "removed_chars", "total_chars"}]
    sources_scanned: List[str] = field(default_factory=list)

    @property
    def hit(self) -> bool:
        return bool(self.hits or self.hidden)

    @property
    def reasons(self) -> List[str]:
        out = []
        if self.hits:
            out.append("instruction_phrase" if any(h.pattern != "marker_shape" for h in self.hits) else "marker_copy")
            if any(h.pattern == "marker_shape" for h in self.hits) and "marker_copy" not in out:
                out.append("marker_copy")
        if self.hidden:
            out.append("hidden_text_share")
        return out

    def summary(self) -> str:
        if not self.hit:
            return "clean"
        parts = []
        if self.hits:
            h = self.hits[0]
            parts.append(f"{len(self.hits)} phrase hit(s), first in {h.source}: {h.match!r}")
        for x in self.hidden:
            parts.append(f"{x['source']}: {x['ratio']:.0%} hidden of {x['total_chars']:,} chars")
        return "; ".join(parts)

    def as_meta(self) -> dict:
        """What goes to extra_meta["injection_guard"]: enough for a reviewer
        to see what fired and where, never the whole document."""
        return {
            "reasons": self.reasons,
            "hits": [h.as_dict() for h in self.hits],
            "hidden": list(self.hidden),
            "sources_scanned": list(self.sources_scanned),
        }


# -------------------------------------------------------------- detectors

class Detector:
    """Two methods. Anything with them can be passed to inspect()."""
    name = "detector"

    def scan(self, text: str, source: str) -> List[Hit]:   # pragma: no cover - interface
        raise NotImplementedError


def _normalise(text: str) -> str:
    """One space for any run of whitespace, so a phrase split across a line
    break or padded by OCR still matches. Case is handled by the regex flag."""
    return re.sub(r"\s+", " ", text or "")


def _context(text: str, start: int, end: int) -> str:
    s, e = max(0, start - CONTEXT_CHARS), min(len(text), end + CONTEXT_CHARS)
    return ("..." if s > 0 else "") + text[s:e] + ("..." if e < len(text) else "")


class PatternDetector(Detector):
    """The phrase list and the marker shapes from the config file."""
    name = "patterns"

    def __init__(self, phrases: Sequence[str], marker_shapes: bool = True):
        self.phrases = [(p, re.compile(p, re.IGNORECASE)) for p in phrases]
        self.markers = [re.compile(p, re.IGNORECASE) for p in _MARKER_SHAPES] if marker_shapes else []

    def scan(self, text: str, source: str) -> List[Hit]:
        flat = _normalise(text)
        if not flat:
            return []
        hits: List[Hit] = []
        for written, rx in self.phrases:
            for m in rx.finditer(flat):
                hits.append(Hit(source, written, m.group(0), _context(flat, m.start(), m.end())))
                if len(hits) >= MAX_HITS_PER_SOURCE:
                    return hits
        for rx in self.markers:
            for m in rx.finditer(flat):
                hits.append(Hit(source, "marker_shape", m.group(0), _context(flat, m.start(), m.end())))
                if len(hits) >= MAX_HITS_PER_SOURCE:
                    return hits
        return hits


# ----------------------------------------------------------------- config

@dataclass(frozen=True)
class Config:
    mode: str
    phrases: Tuple[str, ...]
    marker_shapes: bool
    hidden_ratio: float
    hidden_min_chars: int


def load_config(path: Path = CONFIG) -> Config:
    import yaml
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    mode = str(raw.get("mode") or "observe").lower()
    if mode not in MODES:
        logger.warning("injection_patterns.yml mode %r unknown; using observe", mode)
        mode = "observe"
    phrases = tuple(str(p) for p in (raw.get("phrases") or []) if str(p).strip())
    for p in phrases:
        re.compile(p)                                    # a bad pattern fails here, at load, not mid-crawl
    hidden = raw.get("hidden_text") or {}
    return Config(mode=mode, phrases=phrases,
                  marker_shapes=bool(raw.get("marker_shapes", True)),
                  hidden_ratio=float(hidden.get("ratio", 0.10)),
                  hidden_min_chars=int(hidden.get("min_chars", 2000)))


_CONFIG: Optional[Config] = None


def config() -> Config:
    global _CONFIG
    if _CONFIG is None:
        _CONFIG = load_config()
    return _CONFIG


def mode(cfg: Optional[Config] = None) -> str:
    """observe or enforce: the environment wins, then the config file."""
    env = (os.getenv(ENV_MODE) or "").strip().lower()
    if env in MODES:
        return env
    if env:
        logger.warning("%s=%r unknown; using the config file", ENV_MODE, env)
    return (cfg or config()).mode


def default_detectors(cfg: Optional[Config] = None) -> List[Detector]:
    cfg = cfg or config()
    return [PatternDetector(cfg.phrases, cfg.marker_shapes)]


# ---------------------------------------------------------------- inspect

def _hidden_share(hidden_text: Optional[dict], cfg: Config) -> List[dict]:
    """CHG-006's counts: {"page": {...} | None, "files": [{...}]}. A source
    triggers when removed/(removed+kept) exceeds the ratio AND the source
    had at least min_chars in all."""
    out = []
    if not hidden_text:
        return out
    parts: List[Tuple[str, dict]] = []
    if hidden_text.get("page"):
        parts.append(("page", hidden_text["page"]))
    for f in hidden_text.get("files") or []:
        parts.append((str(f.get("file") or "file"), f))
    for source, counts in parts:
        removed = int(counts.get("removed_chars") or 0)
        kept = int(counts.get("kept_chars") or 0)
        total = removed + kept
        if total >= cfg.hidden_min_chars and total and removed / total > cfg.hidden_ratio:
            out.append({"source": source, "ratio": round(removed / total, 3),
                        "removed_chars": removed, "total_chars": total})
    return out


def inspect(page_text: str = "", files: Iterable[Tuple[str, str]] = (),
            hidden_text: Optional[dict] = None, *,
            detectors: Optional[Sequence[Detector]] = None,
            cfg: Optional[Config] = None) -> Verdict:
    """Scan what would go to the analysers. Pure: no I/O, no writes."""
    cfg = cfg or config()
    detectors = list(detectors) if detectors is not None else default_detectors(cfg)
    v = Verdict()
    sources = [("page", page_text or "")] + [(str(n), t or "") for n, t in files]
    for source, text in sources:
        if not text.strip():
            continue
        v.sources_scanned.append(source)
        for d in detectors:
            v.hits.extend(d.scan(text, source))
    v.hidden = _hidden_share(hidden_text, cfg)
    return v


def inspect_decision(dec, hidden_text: Optional[dict] = None, **kw) -> Verdict:
    """The orchestrator's entry: a textinput.Decision carries the page text
    (`html_text`) and the attachments (`file_parts`) already fetched."""
    return inspect(getattr(dec, "html_text", "") or "",
                   list(getattr(dec, "file_parts", None) or []),
                   hidden_text, **kw)


__all__ = ["Hit", "Verdict", "Detector", "PatternDetector", "Config", "load_config",
           "config", "mode", "default_detectors", "inspect", "inspect_decision",
           "STATUS_QUARANTINE", "ENV_MODE", "MODES"]
