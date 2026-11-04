"""CHG-006: the reader's view of a page or a PDF page.

  html hidden    each hiding technique on the register's list removes the
                 element's words and names the reason
  html kept      what a reader can see or open stays: normal text, a
                 Bootstrap-style collapsed section, <details>, faint but
                 visible text, eleven-pixel text
  html code      script/style/noscript/template go, uncounted
  pdf hidden     white on white, near-background on a filled box, tiny,
                 outside the page box
  pdf kept       white text on a dark filled box, text over an image
                 (background unknown), ordinary body text
  contract       None/"" -> empty result; ratio; as_meta shape; nested
                 hidden text counted once

    .venv/Scripts/python.exe -m pytest tests/test_hidden_text.py -v
"""
from __future__ import annotations

import pytest

from processor.hidden_text import (COLOUR_TOL, PT_MIN, VisibleText,
                                   visible_html_text, visible_pdf_page_text)

# --------------------------------------------------------------------------- #
#  HTML                                                                        #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("fragment, reason", [
    ('<div style="display:none">SECRET</div>',                      "display_none"),
    ('<div style="display: none !important">SECRET</div>',          "display_none"),
    ('<span style="visibility:hidden">SECRET</span>',               "visibility_hidden"),
    ('<p hidden>SECRET</p>',                                        "hidden_attr"),
    ('<p aria-hidden="true">SECRET</p>',                            "aria_hidden"),
    ('<p style="opacity:0">SECRET</p>',                             "opacity_zero"),
    ('<p style="font-size:0">SECRET</p>',                           "font_size_zero"),
    ('<p style="font-size:1px">SECRET</p>',                         "font_size_zero"),
    ('<div style="width:0;height:0;overflow:hidden">SECRET</div>',  "zero_size"),
    ('<div style="position:absolute;left:-9999px">SECRET</div>',    "off_screen"),
    ('<div style="position:fixed;top:-10000px">SECRET</div>',       "off_screen"),
    ('<p style="text-indent:-9999px">SECRET</p>',                   "off_screen"),
    ('<p style="position:absolute;clip:rect(0,0,0,0)">SECRET</p>',  "clipped"),
    ('<p style="clip-path:inset(100%)">SECRET</p>',                 "clipped"),
])
def test_html_hidden_removed_and_named(fragment, reason):
    v = visible_html_text("<p>visible</p>" + fragment)
    assert "SECRET" not in v.text
    assert "visible" in v.text
    assert v.reasons[reason] == len("SECRET")
    assert v.removed_chars == len("SECRET")


@pytest.mark.parametrize("fragment, kept", [
    ('<p>Plain words</p>',                                           "Plain words"),
    ('<div class="collapse" id="a">Collapsed section</div>',         "Collapsed section"),
    ('<details><summary>More</summary>Body of details</details>',    "Body of details"),
    ('<p style="opacity:0.6">Faint</p>',                             "Faint"),
    ('<p style="font-size:11px">Eleven</p>',                         "Eleven"),
    ('<p style="position:absolute;left:10px">Positioned</p>',        "Positioned"),
    ('<div style="width:0;height:0">Zero box, overflow visible</div>', "Zero box"),
    ('<p style="display:block">Block</p>',                           "Block"),
    ('<p aria-hidden="false">Not hidden</p>',                        "Not hidden"),
])
def test_html_visible_kept(fragment, kept):
    v = visible_html_text(fragment)
    assert kept in v.text
    assert v.removed_chars == 0, dict(v.reasons)


def test_html_code_is_dropped_but_not_counted():
    v = visible_html_text('<p>t</p><script>alert(1)</script><style>p{}</style>'
                          '<noscript>n</noscript><template>x</template>')
    assert v.text == "t"
    assert v.removed_chars == 0


def test_html_nested_hidden_counted_once():
    v = visible_html_text('<div style="display:none"><p style="display:none">ab</p><p>cd</p></div>')
    assert v.text == ""
    assert v.removed_chars == len("ab cd")
    assert v.reasons == {"display_none": len("ab cd")}


def test_html_ratio_and_meta():
    v = visible_html_text('<p>12345</p><p style="display:none">12345</p>')
    assert v.ratio() == pytest.approx(0.5)
    m = v.as_meta()
    assert m["removed_chars"] == 5 and m["kept_chars"] == 5
    assert m["reasons"] == {"display_none": 5}
    assert m["samples"] == [["display_none", "12345"]]


@pytest.mark.parametrize("value", [None, ""])
def test_html_empty(value):
    v = visible_html_text(value)
    assert v.text == "" and v.removed_chars == 0 and v.ratio() == 0.0


def test_html_keeps_block_structure():
    v = visible_html_text("<h1>Title</h1><p>One.</p><p>Two.</p>")
    assert v.text.split("\n") == ["Title", "One.", "Two."]


# --------------------------------------------------------------------------- #
#  PDF                                                                         #
# --------------------------------------------------------------------------- #

fitz = pytest.importorskip("fitz")


def _page():
    doc = fitz.open()
    return doc, doc.new_page(width=595, height=842)


def test_pdf_white_on_white_removed():
    doc, page = _page()
    page.insert_text((72, 72), "Visible heading", fontsize=12)
    page.insert_text((72, 100), "SECRET white", fontsize=11, color=(1, 1, 1))
    v = visible_pdf_page_text(page)
    assert "Visible heading" in v.text and "SECRET" not in v.text
    assert v.reasons == {"near_background": len("SECRET white")}


def test_pdf_near_background_on_filled_box_removed():
    doc, page = _page()
    page.draw_rect(fitz.Rect(72, 200, 400, 230), color=None, fill=(0.9, 0.9, 0.9))
    page.insert_text((80, 220), "SECRET grey", fontsize=11, color=(0.9 + COLOUR_TOL / 2,) * 3)
    page.insert_text((80, 300), "Body", fontsize=11)
    v = visible_pdf_page_text(page)
    assert "SECRET" not in v.text and "Body" in v.text
    assert "near_background" in v.reasons


def test_pdf_white_on_dark_box_kept():
    doc, page = _page()
    page.draw_rect(fitz.Rect(72, 160, 400, 190), color=None, fill=(0.1, 0.1, 0.4))
    page.insert_text((80, 180), "HEADER ON DARK", fontsize=12, color=(1, 1, 1))
    v = visible_pdf_page_text(page)
    assert "HEADER ON DARK" in v.text
    assert v.removed_chars == 0


def test_pdf_white_text_over_image_kept():
    doc, page = _page()
    pix = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 10, 10), 0)
    pix.clear_with(30)                                   # dark image
    page.insert_image(fitz.Rect(72, 160, 400, 190), pixmap=pix)
    page.insert_text((80, 180), "CAPTION ON IMAGE", fontsize=12, color=(1, 1, 1))
    v = visible_pdf_page_text(page)
    assert "CAPTION ON IMAGE" in v.text, "background under an image is unknown: keep"
    assert v.removed_chars == 0


def test_pdf_tiny_removed():
    doc, page = _page()
    page.insert_text((72, 72), "Normal", fontsize=10)
    page.insert_text((72, 90), "SECRET tiny", fontsize=PT_MIN - 1)
    v = visible_pdf_page_text(page)
    assert "Normal" in v.text and "SECRET" not in v.text
    assert v.reasons == {"tiny": len("SECRET tiny")}


def test_pdf_off_page_removed_and_counted():
    doc, page = _page()
    page.insert_text((72, 72), "Normal", fontsize=10)
    page.insert_text((72, 900), "SECRET below", fontsize=11)
    page.insert_text((-300, 140), "SECRET left", fontsize=11)
    v = visible_pdf_page_text(page)
    assert "SECRET" not in v.text and "Normal" in v.text
    assert v.reasons["off_page"] == len("SECRET below") + len("SECRET left")


def test_pdf_ghost_copy_of_visible_text_is_a_duplicate_not_hidden():
    """A producer drew the page a second time one page-width to the left
    (seen in a CBK Kuwait framework PDF, 99,018 chars). Dropped, but not
    counted as hidden: the same words are visible on the page."""
    doc, page = _page()
    page.insert_text((72, 72), "Section 2.2.1 Governance", fontsize=11)
    page.insert_text((72 - 595, 72), "Section 2.2.1 Governance", fontsize=11)   # ghost copy
    page.insert_text((72 - 595, 90), "SECRET only off page", fontsize=11)
    v = visible_pdf_page_text(page)
    assert v.text.strip() == "Section 2.2.1 Governance"
    assert v.duplicate_chars == len("Section 2.2.1 Governance")
    assert v.reasons == {"off_page": len("SECRET only off page")}
    assert v.removed_chars == len("SECRET only off page")
    assert v.as_meta()["duplicate_chars"] == v.duplicate_chars


def test_pdf_ghost_copy_of_another_page_needs_the_document_pool():
    """The Kuwait PDF's ghost copies were of OTHER pages: 2,552 off-page spans,
    every one visible somewhere else in the document. pdf_visible_pool makes
    them duplicates; without it they count as hidden."""
    from processor.hidden_text import pdf_visible_pool
    doc = fitz.open()
    p1 = doc.new_page(width=595, height=842)
    p1.insert_text((72, 72), "Page one body text", fontsize=11)
    p2 = doc.new_page(width=595, height=842)          # p1 is invalid from here on
    p2.insert_text((72, 72), "Page two body text", fontsize=11)
    p2.insert_text((72 - 595, 72), "Page one body text", fontsize=11)    # ghost of page 1
    assert visible_pdf_page_text(p2).removed_chars == len("Page one body text")
    pool = pdf_visible_pool(doc)
    # PyMuPDF invalidates a Page object once the document is iterated again,
    # so reload it; the extractor does the same (pdf_doc[page_num]).
    v = visible_pdf_page_text(doc[1], known_visible=pool)
    assert v.removed_chars == 0 and v.duplicate_chars == len("Page one body text")
    assert v.text.strip() == "Page two body text"


def test_pdf_layout_matches_plain_extraction_when_nothing_hidden():
    doc, page = _page()
    page.insert_text((72, 72), "Line one", fontsize=11)
    page.insert_text((72, 90), "Line two", fontsize=11)
    page.insert_text((72, 300), "Far block", fontsize=11)
    v = visible_pdf_page_text(page)
    plain = page.get_text("text").strip()
    assert v.text.strip() == plain
    assert v.removed_chars == 0


def test_pdf_empty_page():
    doc, page = _page()
    v = visible_pdf_page_text(page)
    assert v.text == "" and v.removed_chars == 0
