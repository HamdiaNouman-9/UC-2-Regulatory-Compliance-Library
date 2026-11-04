"""The one HTML allowlist for `regulations.document_html` (CHG-001).

Crawlers store the regulator's own markup, byte for byte, and the review UI
renders it. A page that carries a script, an inline event handler or a form
therefore reaches a reviewer's browser exactly as the regulator (or whoever
edited the regulator's page) wrote it. This module is the single answer to
"what markup may be stored": the storage layer calls `clean_html` on every
write, so no caller has to remember, and the display side uses the same list
so the two never disagree.

WHAT IS KEPT
    the document's content: headings, paragraphs, lists, tables, links,
    images, emphasis, and inline `style`/`class`/`id` so the page still looks
    like the page.

WHAT IS REMOVED, with its content
    script, style, noscript -- exactly the three the fingerprint already
    ignores (crawler/fingerprint.py `_NOISE`), so the visible text is the
    same before and after cleaning. MEASURED 2026-11-03 on tests/fixtures:
    word for word identical, with one serialisation difference -- a bare `&`
    comes back as `&amp;`. The content hash is taken from the LIVE page at
    crawl time and compared with the stored hash, so this never produces a
    false `modified`; it only matters to a script that re-hashes STORED html
    (scripts/backfill_mc_hashes.py), and the fix for that belongs in
    fingerprint.text_of_html (html.unescape), not here.

WHAT IS REMOVED, content kept
    iframe, object, embed, form, input, button, select, textarea, svg, math,
    meta, link, base and every other tag not on the list. The tag goes, the
    words inside it stay, so a notice wrapped in a <form> is still read.

ATTRIBUTES
    every `on*` handler, `srcdoc`, `target`, `formaction`. `href`/`src` keep
    only http, https, mailto, tel and relative urls; `javascript:` and `data:`
    are dropped (relative urls are left as they are, which is what
    scripts/absolutise_document_html.py expects to find). ONE exception,
    measured on the database 2026-11-04: `<img src="data:image/png;base64,…">`
    is kept, for raster image types only. SAMA pastes figures into its
    rulebook that way (63 rows) and MC once; without this they would render
    as broken images. A data: url anywhere else, and `data:image/svg+xml`
    anywhere (an svg can carry script), is still dropped.

It is IDEMPOTENT: cleaning clean markup returns it unchanged, so the call is
safe on the modified path and on rows that were never dirty.

The parser is nh3 (Rust, html5ever), chosen over a regex pass because the
browser's parser is what the attacker is aiming at, and only a real HTML5
parser sees the same tree the browser will -- an unquoted `onerror`, an
`<svg onload>` or an entity-encoded `javascript:` all pass a regex and none
pass this.
"""
from __future__ import annotations

import re
from collections import Counter
from typing import Dict, Optional, Set

import nh3

__all__ = ["ALLOWED_TAGS", "ALLOWED_ATTRIBUTES", "ALLOWED_URL_SCHEMES",
           "DATA_IMAGE_PREFIXES",
           "CLEAN_CONTENT_TAGS", "clean_html", "dropped_in"]

# Content tags only. nh3's own default list plus the few that regulator CMSes
# emit around real content. Anything not here is removed (content kept).
ALLOWED_TAGS: Set[str] = set(nh3.ALLOWED_TAGS) | {
    "tfoot", "section", "main", "address", "font",
}

# Removed TOGETHER WITH their content. Keep in step with fingerprint._NOISE.
CLEAN_CONTENT_TAGS: Set[str] = {"script", "style", "noscript"}

# Per-tag attributes. "*" applies to every allowed tag.
ALLOWED_ATTRIBUTES: Dict[str, Set[str]] = {
    "*": {"style", "class", "id", "title", "dir", "lang", "align",
          "width", "height", "valign", "bgcolor", "border", "bordercolor",
          "cellpadding", "cellspacing"},   # bordercolor: 12,500 QFC tables
    "a": {"href", "hreflang", "name"},   # rel is set by nh3 (link_rel)
    "img": {"src", "alt", "width", "height", "align", "loading"},
    "td": {"colspan", "rowspan", "headers", "scope", "nowrap"},
    "th": {"colspan", "rowspan", "headers", "scope", "nowrap"},
    "col": {"span"},
    "colgroup": {"span"},
    "ol": {"start", "type", "reversed"},
    "ul": {"type"},
    "li": {"value"},
    "table": {"summary"},
    "font": {"color", "face", "size"},
    "blockquote": {"cite"},
    "q": {"cite"},
    "del": {"cite", "datetime"},
    "ins": {"cite", "datetime"},
    "time": {"datetime"},
    "details": {"open"},
}

ALLOWED_URL_SCHEMES: Set[str] = {"http", "https", "mailto", "tel"}

#: The only place a `data:` url survives: an <img src> carrying a raster image.
DATA_IMAGE_PREFIXES = ("data:image/png", "data:image/jpeg", "data:image/jpg",
                       "data:image/gif", "data:image/webp", "data:image/bmp")


def _attribute_filter(element: str, attribute: str, value: str) -> Optional[str]:
    """nh3 calls this with the DECODED value of every surviving attribute.
    `data` is in the scheme set handed to nh3 only so that this function gets
    to see data: urls at all; it lets through img@src with a raster image
    type and drops every other data: url."""
    if value.lstrip().lower().startswith("data:"):
        if element == "img" and attribute == "src" and                 value.lstrip().lower().startswith(DATA_IMAGE_PREFIXES):
            return value
        return None
    return value


def clean_html(html: Optional[str]) -> Optional[str]:
    """Return `html` reduced to the allowlist above; None/empty pass through.

    The return type follows the input so the storage layer can call this on a
    column that may be NULL without turning NULL into "".
    """
    if not html:
        return html
    return nh3.clean(
        html,
        tags=ALLOWED_TAGS,
        clean_content_tags=CLEAN_CONTENT_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        attribute_filter=_attribute_filter,
        url_schemes=ALLOWED_URL_SCHEMES | {"data"},   # see _attribute_filter
        strip_comments=True,
        link_rel="noopener noreferrer",
    )


# --------------------------------------------------------------------------- #
#  Measurement, for the count-only scan and for tests                          #
# --------------------------------------------------------------------------- #

_OPEN_TAG = re.compile(r"<\s*([a-zA-Z][a-zA-Z0-9:-]*)((?:\s+[^<>]*?)?)\s*/?>")
# name, then an optional value (quoted or bare) that is consumed so a bare
# value such as `src=x` is not counted as a second attribute named `x`.
_ATTR = re.compile(r"([a-zA-Z_:][-a-zA-Z0-9_:.]*)"
                   r"(?:\s*=\s*(?:\"[^\"]*\"|'[^']*'|[^\s\"'>]+))?")


def _tags_and_attrs(html: str) -> Counter:
    """Count `tag` and `tag@attr` occurrences in a fragment. Regex on purpose:
    this is a tally over thousands of rows, and an off-by-one on tag soup does
    not matter when the answer is a ranked list of what would be lost."""
    out: Counter = Counter()
    for m in _OPEN_TAG.finditer(html):
        tag = m.group(1).lower()
        out[tag] += 1
        for a in _ATTR.finditer(m.group(2) or ""):
            out[f"{tag}@{a.group(1).lower()}"] += 1
    return out


def dropped_in(html: Optional[str]) -> Counter:
    """What `clean_html` would remove from `html`: a Counter keyed by `tag`
    or `tag@attr`, value = occurrences lost. Empty Counter means the fragment
    is already clean. Only ever counts; never writes."""
    if not html:
        return Counter()
    before = _tags_and_attrs(html)
    after = _tags_and_attrs(clean_html(html) or "")
    lost = before - after
    # nh3 adds rel="noopener noreferrer" to every link; an attribute that is
    # ADDED must not show up as a loss on something else, and `a@rel` that the
    # page already had is not lost either.
    lost.pop("a@rel", None)
    return lost
