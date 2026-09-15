"""WHAT TEXT GOES TO THE LLM — the gate, and the HTML-vs-PDF decision.

Replaces `extract_text_content_unified`'s "first tier that has enough text wins"
with two rules the lead specified:

  THE GATE — proceed only when there is something to analyse:
      document_html has real text,  OR
      document_url is a file (.pdf/.doc/…),  OR
      document_url is a page we can fetch
    Otherwise skip: insert the document, log it, analyse nothing. A regulator
    that publishes only an external link (MISA's 24 laws.boe.gov.sa entries) is
    stored and left alone rather than silently half-processed.

  THE INPUT — when both an HTML rendering and a file exist:
      they say the same thing  ->  send the HTML only
      they differ              ->  SEND BOTH
    Both, deliberately. A PDF is often the authoritative text while the page is a
    summary, and the reverse happens too — SAMA pages run 379 characters against
    a full PDF. Dropping either risks dropping a requirement, and a few hundred
    wasted tokens is the cheaper mistake.

  "SAY THE SAME THING" is not a hash. OCR never matches HTML byte for byte, so
  this compares 5-word shingles and asks how much of the shorter text appears in
  the longer one. Cheap, no LLM, tolerant of whitespace and OCR noise.

This module does no I/O. The caller passes in the two fetchers, so it is testable
without a network or a database — see tests/test_formfill_textinput.py.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple

# Matches orchestrator.py's MIN_TEXT_LEN. Kept as a parameter so the two cannot
# drift silently: the caller passes its own value in.
DEFAULT_MIN_TEXT_LEN = 200

# How much of the shorter text must appear in the longer one before we call them
# the same document. 0.8 tolerates OCR noise, headers and footers; 0.95 would
# treat almost every PDF as different and send both every time.
SAME_CONTENT_THRESHOLD = 0.8

SHINGLE_SIZE = 5

_FILE_EXT = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|rtf|txt)(\?|#|$)", re.I)
_DOWNLOAD_HINT = re.compile(r"wpdmdl=|/download/|/document/|attachment", re.I)

# The separator the model sees when both sources are sent. Explicit, because the
# model must know it is reading one regulation twice (or once plus N attachments),
# not several unrelated regulations. {total} is 1 (the page) + however many
# attachments actually yielded usable text -- almost always 2, but
# attachment_links can carry several files for one regulation (CMA, Ministry of
# Commerce rows commonly have 3-10), so this can no longer be hardcoded at "2".
BOTH_HEADER_HTML = "=== SOURCE 1 OF {total} — text of the published web page ==="
BOTH_HEADER_FILE = "=== SOURCE {n} OF {total} — text of an attached document ({name}) ==="


def is_file_url(url: str) -> bool:
    """Does this URL point at a document rather than a web page?"""
    u = (url or "").strip()
    return bool(u) and bool(_FILE_EXT.search(u) or _DOWNLOAD_HINT.search(u))


def _words(text: str) -> List[str]:
    return re.findall(r"[a-z0-9؀-ۿ]+", (text or "").lower())


def shingles(text: str, n: int = SHINGLE_SIZE) -> set:
    """Overlapping n-word groups. Word sets alone would call any two documents
    about the same subject identical; ordered groups will not."""
    w = _words(text)
    if len(w) < n:
        return {" ".join(w)} if w else set()
    return {" ".join(w[i:i + n]) for i in range(len(w) - n + 1)}


def containment(a: str, b: str) -> float:
    """How much of the SHORTER text appears in the longer one, 0.0–1.0.

    Containment, not Jaccard: a 5-page PDF that fully includes a 1-paragraph page
    summary should score high. Jaccard would score it low simply because the PDF
    is bigger, and we would send both when one would do.
    """
    sa, sb = shingles(a), shingles(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / min(len(sa), len(sb))


def same_content(a: str, b: str, threshold: float = SAME_CONTENT_THRESHOLD) -> bool:
    return containment(a, b) >= threshold


@dataclass
class Decision:
    """What to analyse, and why — the `why` is logged so a skipped document can
    always be explained without re-running anything.

    attempted_files/succeeded_files exist so a caller can tell "there were no
    attachments to begin with" apart from "there were attachments and every
    one of them failed to fetch" -- `sources` alone can't: an html-only
    Decision looks identical either way. That distinction matters most for an
    attachment_links bundle, where html/content_text is deliberately just a
    thin wrapper (see decide_for_document) -- falling back to it because
    every real attachment failed produces a plausible-looking but misleading
    result, which the caller needs to be able to detect and refuse rather
    than silently analyse.
    """
    skip: bool
    reason: str
    text: Optional[str] = None
    content_type: str = "html"
    sources: List[str] = field(default_factory=list)
    overlap: Optional[float] = None
    attempted_files: int = 0
    succeeded_files: int = 0
    # Raw pieces, before decide() combines them into `text`. Exists so a
    # caller wanting per-document granularity (the Requirement/Activity
    # pipeline's `documents=[{"source_document":..., "text":...}, ...]`
    # input -- see requirement_analyzer.py) can build it directly from a
    # Decision already computed for the old flow, instead of re-fetching
    # every attachment a second time.
    html_text: str = ""
    file_parts: List[Tuple[str, str]] = field(default_factory=list)  # (name, text)

    def __str__(self) -> str:
        if self.skip:
            return f"SKIP — {self.reason}"
        return (f"ANALYSE {'+'.join(self.sources)} ({len(self.text or ''):,} chars) "
                f"— {self.reason}")


def decide(
    *,
    document_html: str = "",
    content_text: str = "",
    document_url: str = "",
    attachment_url: str = "",
    attachment_urls: Optional[List[str]] = None,
    fetch_file_text: Optional[Callable[[str], Optional[str]]] = None,
    fetch_page_text: Optional[Callable[[str], Optional[str]]] = None,
    min_text_len: int = DEFAULT_MIN_TEXT_LEN,
    threshold: float = SAME_CONTENT_THRESHOLD,
) -> Decision:
    """Apply the gate, then choose the input.

    `content_text` is the crawler's already-extracted page text and is preferred
    over `document_html` — it is the same content with the markup already gone.

    `attachment_url` (single) and `attachment_urls` (list) are both accepted and
    combined -- `attachment_url` stays for any existing caller passing one URL
    positionally-by-name; `attachment_urls` is what extra_meta["attachment_links"]
    (pipe-separated, often 2+ files -- CMA/Ministry of Commerce rows commonly
    carry 3-10) actually needs. extra_meta["org_pdf_link"] used to be this
    function's only attachment source; checked against the live data (2026-09-07)
    it never carries a URL that attachment_links doesn't already contain as its
    first entry, so it is no longer read here at all -- decide_for_document below
    is where that switch happens. When `document_url` is itself a file (SAMA's
    actual mechanism -- a direct .pdf link, not a metadata field), it is folded
    into the same URL list rather than handled as a separate case.
    """
    html_text = (content_text or "").strip() or (document_html or "").strip()
    doc_is_file = is_file_url(document_url)
    urls: List[str] = []
    if attachment_url and attachment_url.strip():
        urls.append(attachment_url.strip())
    for u in (attachment_urls or []):
        if u and u.strip() and u.strip() not in urls:
            urls.append(u.strip())
    if doc_is_file and document_url not in urls:
        urls.insert(0, document_url)
    page_url = "" if doc_is_file else (document_url or "").strip()

    # ---- THE GATE ----------------------------------------------------------
    if len(html_text) < min_text_len and not urls and not page_url:
        return Decision(True, "no html text, no file, no page to fetch",
                        attempted_files=0, succeeded_files=0)

    # ---- gather what we can -----------------------------------------------
    # Each URL that is itself a file (not every entry needs to be -- a page
    # can link to a mix) is fetched independently; a fetch failure or a file
    # too short to be useful (e.g. a .docx we have no extractor for, which
    # fetch_file_text returns as None/empty for rather than raising) just
    # drops that one entry instead of losing every attachment on this row.
    file_parts: List[Tuple[str, str]] = []  # (name, text), in URL order
    attempted_files = 0
    if fetch_file_text:
        for u in urls:
            if not is_file_url(u):
                continue
            attempted_files += 1
            t = (fetch_file_text(u) or "").strip()
            if len(t) >= min_text_len:
                name = u.rsplit("/", 1)[-1][:80] or "attached file"
                file_parts.append((name, t))
    file_text = "\n\n".join(t for _, t in file_parts)
    succeeded_files = len(file_parts)

    if len(html_text) < min_text_len and page_url and fetch_page_text:
        # The document_url is a web page and nothing was captured at crawl time,
        # so read it now. This is what keeps SBP's circulars and MISA's external
        # law-portal links alive; without it the gate drops them.
        html_text = (fetch_page_text(page_url) or "").strip()

    html_ok = len(html_text) >= min_text_len
    file_ok = bool(file_parts)

    # ---- THE INPUT --------------------------------------------------------
    if html_ok and file_ok:
        ov = containment(html_text, file_text)
        if ov >= threshold:
            # The file(s) are the same regulation rendered as a document. Send
            # the HTML: it is already text, so it needs no OCR trust.
            return Decision(False, f"file duplicates the page (overlap {ov:.2f}) — html only",
                            html_text, "html", ["html"], ov,
                            attempted_files=attempted_files, succeeded_files=succeeded_files,
                            html_text=html_text, file_parts=file_parts)
        total = 1 + len(file_parts)
        pieces = [BOTH_HEADER_HTML.format(total=total), html_text]
        for i, (name, text) in enumerate(file_parts, start=2):
            pieces.append(BOTH_HEADER_FILE.format(n=i, total=total, name=name))
            pieces.append(text)
        combined = "\n\n".join(pieces)
        # "pdf_text" so the analyser's normaliser leaves it alone: the HTML half
        # is already plain text by this point, and running the HTML cleaner over
        # the combined string would mangle the file half(ves).
        return Decision(False, f"page and file(s) differ (overlap {ov:.2f}) — sending both",
                        combined, "pdf_text", ["html", "file"], ov,
                        attempted_files=attempted_files, succeeded_files=succeeded_files,
                        html_text=html_text, file_parts=file_parts)

    if html_ok:
        return Decision(False, "html only", html_text, "html", ["html"],
                        attempted_files=attempted_files, succeeded_files=succeeded_files,
                        html_text=html_text, file_parts=file_parts)
    if file_ok:
        if len(file_parts) > 1:
            pieces = []
            for i, (name, text) in enumerate(file_parts, start=1):
                pieces.append(f"=== ATTACHMENT {i} OF {len(file_parts)} ({name}) ===")
                pieces.append(text)
            file_text = "\n\n".join(pieces)
        return Decision(False, "file only", file_text, "pdf_text", ["file"],
                        attempted_files=attempted_files, succeeded_files=succeeded_files,
                        html_text=html_text, file_parts=file_parts)

    got = f"html {len(html_text)} chars, file {len(file_text)} chars"
    return Decision(True, f"nothing reached {min_text_len} chars ({got})",
                    attempted_files=attempted_files, succeeded_files=succeeded_files,
                    html_text=html_text, file_parts=file_parts)


def decide_for_document(doc, *, fetch_file_text=None, fetch_page_text=None,
                        min_text_len: int = DEFAULT_MIN_TEXT_LEN) -> Decision:
    """Convenience wrapper for a RegulatoryDocument.

    Reads extra_meta["attachment_links"] (pipe-separated, possibly several
    files) as the attachment source. extra_meta["org_pdf_link"] is no longer
    read: checked against the live regulations table (2026-09-07), every row
    where it ever holds a real value also has attachment_links containing that
    exact same URL as its first entry -- SAMA, the one regulator with heavy
    org_pdf_link *key* presence, always has it null there; SAMA's actual
    attachment is document_url itself being a direct .pdf link, which `decide`
    already picks up on its own via is_file_url(document_url)."""
    meta = getattr(doc, "extra_meta", None) or {}
    raw_links = meta.get("attachment_links") or ""
    attachment_urls = [u.strip() for u in str(raw_links).split("|") if u.strip()]
    return decide(
        document_html=getattr(doc, "document_html", "") or "",
        content_text=meta.get("content_text") or "",
        document_url=getattr(doc, "document_url", "") or "",
        attachment_urls=attachment_urls,
        fetch_file_text=fetch_file_text,
        fetch_page_text=fetch_page_text,
        min_text_len=min_text_len,
    )


__all__ = ["Decision", "decide", "decide_for_document", "is_file_url",
           "containment", "same_content", "shingles"]
