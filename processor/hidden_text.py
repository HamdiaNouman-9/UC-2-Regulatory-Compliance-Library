"""Visible text only, for the model's copy of a document (CHG-006).

Hidden text is the one way to plant an instruction that a human reviewer
will never notice: white on white, two points high, pushed off the page,
`display:none`. The analysers must not read what a reader cannot see. This
module produces that reader's view. It changes NOTHING stored: the html, the
file and the content hash stay exact; only the text handed to analysis is
reduced, and how much was removed is reported so the caller can record it
(extra_meta) and the quarantine step (CHG-008) can act on it.

Two entry points, one per medium:

    visible_html_text(html)        -> VisibleText
    visible_pdf_page_text(page)    -> VisibleText        (page: a fitz.Page)

Both return the text, the number of characters removed, a tally of WHY
(reason -> characters) and a few samples of what went, for the measurement
scan and for the row's extra_meta.

WHAT IS HIDDEN, pages (the register's list, nothing more)
    the `hidden` attribute; aria-hidden="true"; inline style with
    display:none, visibility:hidden, opacity:0, font-size:0 (or a size under
    FONT_PX_MIN), width:0/height:0, position absolute/fixed with a far
    negative left/top, text-indent far negative, clip/clip-path that
    collapses the box. Inline styles only: stylesheets are not evaluated, so
    a Bootstrap `.collapse` section a reader can open is KEPT, which is what
    the register asks for. <details> is kept whether open or closed.
    script, style, noscript, template are removed as code, not counted as
    hidden text (the fingerprint and the display cleaner already drop them).

WHAT IS HIDDEN, PDFs (the register's list, nothing more)
    a span under PT_MIN points; a span whose box lies outside the page; a
    span whose colour is within COLOUR_TOL of the background drawn under it
    (white by default, a filled rectangle when one sits under the span; an
    image under the span means the background is unknown and the span is
    kept). Render mode is deliberately NOT a criterion: an OCR layer over a
    scanned page is invisible text and is the only text that page has.

The text layout follows what the callers already produced: block text for
html, PyMuPDF's lines-and-blocks for PDFs.
"""
from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Tuple

__all__ = ["VisibleText", "strip_hidden_html", "visible_html_text", "visible_pdf_page_text",
           "pdf_visible_pool", "PT_MIN", "FONT_PX_MIN", "COLOUR_TOL", "OFFSCREEN_PX"]

PT_MIN = 4.0          # PDF: a span smaller than this is not readable
FONT_PX_MIN = 2.0     # HTML inline font-size below this (px) is not readable
COLOUR_TOL = 0.08     # PDF: max per-channel distance (0..1) to count as background
OFFSCREEN_PX = 1000   # HTML: left/top/text-indent at or beyond -this is off screen
SAMPLE_LIMIT = 8      # how many removed snippets to keep


@dataclass
class VisibleText:
    text: str
    removed_chars: int = 0
    reasons: Counter = field(default_factory=Counter)      # reason -> chars
    samples: List[Tuple[str, str]] = field(default_factory=list)   # (reason, snippet)
    #: Dropped text that is an exact copy of text a reader CAN see on the same
    #: page (PDF producers draw ghost copies off the page; a shadowed title is
    #: drawn twice). Not hidden content, so not in removed_chars or ratio().
    duplicate_chars: int = 0

    @property
    def total_chars(self) -> int:
        return len(self.text) + self.removed_chars

    def ratio(self) -> float:
        """Share of characters removed, 0..1. The quarantine trigger reads this."""
        t = self.total_chars
        return self.removed_chars / t if t else 0.0

    def _note(self, reason: str, snippet: str) -> None:
        n = len(snippet)
        if not n:
            return
        self.removed_chars += n
        self.reasons[reason] += n
        if len(self.samples) < SAMPLE_LIMIT:
            self.samples.append((reason, snippet[:120]))

    def as_meta(self) -> dict:
        """What goes into extra_meta['hidden_text']: small and JSON-able."""
        return {"removed_chars": self.removed_chars,
                "kept_chars": len(self.text),
                "duplicate_chars": self.duplicate_chars,
                "reasons": dict(self.reasons),
                "samples": [list(s) for s in self.samples[:3]]}


# --------------------------------------------------------------------------- #
#  HTML                                                                        #
# --------------------------------------------------------------------------- #

_CODE_TAGS = {"script", "style", "noscript", "template"}
_WS = re.compile(r"\s+")
_NUM = r"-?\d+(?:\.\d+)?"


def _style_map(style: str) -> dict:
    out = {}
    for part in (style or "").split(";"):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        out[k.strip().lower()] = v.strip().lower().replace("!important", "").strip()
    return out


def _px(value: str) -> Optional[float]:
    """A css length as pixels when it is px/pt/em-ish with a number; else None."""
    m = re.match(rf"^\s*({_NUM})\s*(px|pt|em|rem|%)?\s*$", value or "")
    if not m:
        return None
    n = float(m.group(1)); unit = m.group(2) or "px"
    if unit == "pt":
        return n * 96 / 72
    if unit in ("em", "rem"):
        return n * 16
    if unit == "%":
        return n            # only used for 0 / negative checks
    return n


def _hidden_reason(tag) -> Optional[str]:
    """Why this element is invisible to a reader, or None."""
    attrs = tag.attrs or {}
    if "hidden" in attrs:
        return "hidden_attr"
    if str(attrs.get("aria-hidden", "")).strip().lower() == "true":
        return "aria_hidden"
    if tag.name == "input" and str(attrs.get("type", "")).lower() == "hidden":
        return "hidden_attr"
    st = _style_map(attrs.get("style", "") if isinstance(attrs.get("style"), str) else "")
    if not st:
        return None
    if st.get("display") == "none":
        return "display_none"
    if st.get("visibility") in ("hidden", "collapse"):
        return "visibility_hidden"
    op = _px(st.get("opacity", ""))
    if op is not None and op <= 0:
        return "opacity_zero"
    fs = _px(st.get("font-size", ""))
    if fs is not None and fs < FONT_PX_MIN:
        return "font_size_zero"
    # A zero box hides its words only when overflow is clipped; with the
    # default (visible) the text spills out and a reader sees it.
    w, h = _px(st.get("width", "")), _px(st.get("height", ""))
    if ((w is not None and w <= 0) or (h is not None and h <= 0)) \
            and st.get("overflow", "visible") in ("hidden", "clip"):
        return "zero_size"
    if st.get("position") in ("absolute", "fixed"):
        for side in ("left", "top", "right", "bottom"):
            v = _px(st.get(side, ""))
            if v is not None and v <= -OFFSCREEN_PX:
                return "off_screen"
    ti = _px(st.get("text-indent", ""))
    if ti is not None and ti <= -OFFSCREEN_PX:
        return "off_screen"
    clip = st.get("clip", "")
    if re.search(r"rect\(\s*0(?:px)?\s*,?\s*0(?:px)?\s*,?\s*0(?:px)?\s*,?\s*0(?:px)?\s*\)", clip):
        return "clipped"
    cp = st.get("clip-path", "")
    if re.search(r"inset\(\s*(?:100%|50%)", cp) or cp.startswith("circle(0"):
        return "clipped"
    return None


def _text_of(tag) -> str:
    return _WS.sub(" ", tag.get_text(" ", strip=True)).strip()


def strip_hidden_html(html: Optional[str]) -> Tuple[str, VisibleText]:
    """(markup with the hidden elements removed, what was removed).

    The markup is returned, not just text, so the analyser's own html
    normaliser still sees tables and headings and renders them as it does
    today. When nothing is hidden the ORIGINAL string comes back untouched,
    byte for byte, so an unchanged page keeps the same analysis input and
    the same analysis_input_hash. Script/style/noscript/template are left in
    place in that case too (the normaliser drops them itself); they are only
    removed from the returned markup when a hidden element was found and the
    tree has to be re-serialised anyway."""
    from bs4 import BeautifulSoup
    vt = VisibleText(text="")
    if not html:
        return html or "", vt
    soup = BeautifulSoup(html, "html.parser")
    for t in soup.find_all(_CODE_TAGS):
        t.decompose()                                   # code, not hidden text

    def walk(node):
        for child in list(node.children):
            if getattr(child, "name", None) is None:   # NavigableString
                continue
            reason = _hidden_reason(child)
            if reason:
                vt._note(reason, _text_of(child))
                child.decompose()
            else:
                walk(child)

    walk(soup)
    text = soup.get_text("\n", strip=True)
    vt.text = re.sub(r"\n{3,}", "\n\n", text)
    return (str(soup) if vt.removed_chars else html), vt


def visible_html_text(html: Optional[str]) -> VisibleText:
    """The reader's view of an html fragment or page, as block text."""
    return strip_hidden_html(html)[1]


# --------------------------------------------------------------------------- #
#  PDF (PyMuPDF)                                                               #
# --------------------------------------------------------------------------- #

def _rgb(color_int: int) -> Tuple[float, float, float]:
    c = int(color_int or 0)
    return (((c >> 16) & 255) / 255.0, ((c >> 8) & 255) / 255.0, (c & 255) / 255.0)


def _close(a, b, tol=COLOUR_TOL) -> bool:
    return max(abs(x - y) for x, y in zip(a, b)) <= tol


def _background_under(page, bbox, fills, images) -> Optional[Tuple[float, float, float]]:
    """The colour drawn under a span: the last fill whose rect contains the
    span's centre, None when an image is there (unknown), white otherwise."""
    cx, cy = (bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2
    for r in images:
        if r[0] <= cx <= r[2] and r[1] <= cy <= r[3]:
            return None
    bg = (1.0, 1.0, 1.0)
    for rect, colour in fills:                         # painting order
        if rect[0] <= cx <= rect[2] and rect[1] <= cy <= rect[3]:
            bg = colour
    return bg


def _fills_and_images(page):
    fills = []
    try:
        for d in page.get_drawings():
            if d.get("fill") is not None and d.get("rect") is not None:
                r = d["rect"]
                fills.append(((r.x0, r.y0, r.x1, r.y1), tuple(d["fill"][:3])))
    except Exception:
        pass
    images = []
    try:
        for info in page.get_image_info():
            b = info.get("bbox")
            if b:
                images.append(tuple(b))
    except Exception:
        pass
    return fills, images


def _span_class(page, span, page_box, pt_min, state) -> Optional[str]:
    """Why a span is invisible, or None. `state` caches the page's fills and
    images between calls."""
    bb = span.get("bbox", page_box)
    if float(span.get("size", 10)) < pt_min:
        return "tiny"
    if bb[2] <= page_box[0] or bb[0] >= page_box[2] or bb[3] <= page_box[1] or bb[1] >= page_box[3]:
        return "off_page"
    if "fills" not in state:                            # once per page, only when needed
        state["fills"], state["images"] = _fills_and_images(page)
    bg = _background_under(page, bb, state["fills"], state["images"])
    if bg is not None and _close(_rgb(span.get("color", 0)), bg):
        return "near_background"
    return None


def _spans(page):
    import fitz
    # PyMuPDF clips extraction to the page box unless told otherwise, so text
    # placed outside it (or outside the crop box) would be dropped silently
    # and never counted. We want to see it, count it, and then drop it.
    d = page.get_text("dict", clip=fitz.INFINITE_RECT(),
                      flags=fitz.TEXTFLAGS_DICT & ~fitz.TEXT_MEDIABOX_CLIP)
    pr = page.rect
    return d, (pr.x0, pr.y0, pr.x1, pr.y1)


def pdf_visible_pool(doc, *, pt_min: float = PT_MIN) -> set:
    """Every span text a reader can see somewhere in the DOCUMENT. Pass it to
    visible_pdf_page_text so a ghost copy of page 8 drawn off page 10 is
    recognised as a duplicate. One extra extraction pass per page."""
    pool = set()
    for page in doc:
        d, page_box = _spans(page)
        state = {}
        for block in d.get("blocks", []):
            if block.get("type", 0) != 0:
                continue
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    s = span.get("text", "").strip()
                    if s and _span_class(page, span, page_box, pt_min, state) is None:
                        pool.add(s)
    return pool


def visible_pdf_page_text(page, *, pt_min: float = PT_MIN,
                          known_visible: Optional[set] = None) -> VisibleText:
    """The reader's view of one PDF page. `page` is a fitz.Page.
    `known_visible`: text visible elsewhere in the document (pdf_visible_pool);
    a dropped span found there counts as a duplicate, not as hidden."""
    vt = VisibleText(text="")
    d, page_box = _spans(page)
    state: dict = {}
    # Pass 1: classify every span. Pass 2: a dropped span whose exact text is
    # also visible on this page (or elsewhere in the document, via
    # known_visible) is a duplicate -- ghost copy, shadow -- not hidden
    # content. See VisibleText.duplicate_chars.
    classified: List[Tuple[int, int, str, Optional[str]]] = []   # (block, line, text, reason)
    visible_texts = set(known_visible or ())
    for bi, block in enumerate(d.get("blocks", [])):
        if block.get("type", 0) != 0:
            continue                                    # image block
        for li, line in enumerate(block.get("lines", [])):
            for span in line.get("spans", []):
                s = span.get("text", "")
                if not s.strip():
                    classified.append((bi, li, s, None))
                    continue
                reason = _span_class(page, span, page_box, pt_min, state)
                if reason is None:
                    visible_texts.add(s.strip())
                classified.append((bi, li, s, reason))

    out_blocks: List[str] = []
    cur_key = None; lines_out: List[str] = []; spans_out: List[str] = []

    def flush_line():
        nonlocal spans_out
        joined = "".join(spans_out)
        if joined.strip():
            lines_out.append(joined)
        spans_out = []

    def flush_block():
        nonlocal lines_out
        flush_line()
        if lines_out:
            out_blocks.append("\n".join(lines_out))
        lines_out = []

    for bi, li, s, reason in classified:
        key = (bi, li)
        if cur_key is not None and key != cur_key:
            if key[0] != cur_key[0]:
                flush_block()
            else:
                flush_line()
        cur_key = key
        if reason is None:
            spans_out.append(s)
        elif s.strip() in visible_texts:
            vt.duplicate_chars += len(s.strip())
        else:
            vt._note(reason, s.strip())
    flush_block()
    vt.text = "\n".join(out_blocks)
    return vt
