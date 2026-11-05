"""
doc_metadata_extractor.py
─────────────────────────
Uses an LLM to extract structured regulation metadata from raw document text.

Fields extracted:
    title, published_date, reference_no, year, status
"""

import os
import json
import logging
import re
from typing import Optional
from openai import OpenAI
from typing import Optional, Tuple

from processor.quoted_source import quote_source   # CHG-007: data markers

logger = logging.getLogger(__name__)

_CLIENT = None

def _get_client():
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = OpenAI(
            api_key=os.getenv("OPENROUTER_API_KEY"),
            base_url="https://openrouter.ai/api/v1"
        )
    return _CLIENT

# Valid status values — mirrors what the regulations table expects
VALID_STATUSES = {"active", "superseded", "draft", "archived", "withdrawn"}

_SYSTEM_PROMPT = """\
You are a regulatory document metadata extractor.
Given the first portion of a regulatory document, extract structured metadata.

Return ONLY a valid JSON object — no markdown fences, no commentary — with these keys:

{
  "title":          "<full official title — usually after 'الموضوع:'>",
  "published_date": "<if Hijri return as YYYY/MM/DD e.g. 1447/07/09, if Gregorian DD/MM/YYYY, null if not found>",
  "reference_no":   "<reference/circular number — in Saudi Central Bank docs this is a 
                      standalone number (6-12 digits) appearing near the top of the document,
                      often on the line just before or after the date. Look for patterns like
                      '472039139' appearing near 'الرقم' or near the date. null if not found>",
  "year":           "<4-digit year extracted from published_date, null if not found>",
  "status":         "<one of: active, superseded, draft, archived, withdrawn — default active>"
}

Rules:
- Never invent data. Use null for missing fields (except status defaults to "active").
- Arabic field labels: 'الرقم' = reference number, 'التاريخ' = date, 'الموضوع' = subject/title.
- The reference number in SAMA circulars is typically a 9-digit number near the top.
- OCR noise like 'sy', 'al', random chars may appear next to real values — ignore the noise.
- Hijri dates start with 14XX — return as-is in YYYY/MM/DD format.
- Return only the JSON object, nothing else.
"""

def _build_user_message(excerpt: str, filename: Optional[str]) -> str:
    """CHG-007: the excerpt is the whole user message here, so it is the one
    place in this file where document text enters a prompt. It goes between
    markers carrying a code drawn for this call, with the one sentence after
    it; the file-name hint stays outside the block, as it is ours."""
    hint = f"[Filename hint: {filename}]\n\n" if filename else ""
    q = quote_source(excerpt, "document")
    return f"{hint}{q.block}\n{q.notice}"


def extract_metadata_from_text(
    text: str,
    filename: Optional[str] = None,
    *,
    max_input_chars: int = 12_000,
) -> dict:
    """
    Run LLM metadata extraction on the first `max_input_chars` of the document.

    Returns a dict with keys: title, published_date, reference_no, year, status.
    Missing fields are set to None (status defaults to "active").

    Parameters
    ----------
    text            : raw extracted text from the uploaded document
    filename        : original filename – used as a fallback title hint
    max_input_chars : how much of the doc to send to the LLM (first N chars)
    """
    excerpt = text[:max_input_chars].strip()
    logger.info(f"[metadata] Text excerpt sent to LLM:\n{excerpt[:500]}")
    if not excerpt:
        logger.warning("extract_metadata_from_text called with empty text")
        return _empty_metadata(filename)

    user_msg = _build_user_message(excerpt, filename)

    try:
        response = _get_client().chat.completions.create(
            model="deepseek/deepseek-v3.2",  # or whatever model you prefer
            max_tokens=512,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        raw = response.choices[0].message.content.strip()

        # Strip accidental markdown fences
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()

        parsed = json.loads(raw)
        return _normalise(parsed, filename)

    except json.JSONDecodeError as e:
        logger.error(f"Metadata LLM returned invalid JSON: {e}\nRaw: {raw[:500]}")
        return _empty_metadata(filename)
    except Exception as e:
        logger.error(f"Metadata extraction failed: {e}")
        return _empty_metadata(filename)


# ── Internal helpers ─────────────────────────────────────────────────────────

def _normalise(parsed: dict, filename: Optional[str]) -> dict:
    """Sanitise and type-check the LLM output."""

    # Title fallback: derive from filename if LLM couldn't find one
    title = (parsed.get("title") or "").strip() or None
    if not title and filename:
        title = filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " ").strip()

    # published_date: must match DD/MM/YYYY
    pub_date = _validate_date(parsed.get("published_date"))

    # Year: must be exactly 4 digits (works for both 2024 Gregorian and 1447 Hijri)
    year_raw = str(parsed.get("year") or "").strip()
    year = year_raw if re.fullmatch(r"\d{4}", year_raw) else None
    # Status: must be one of the valid values, default to "active"
    status_raw = (parsed.get("status") or "active").strip().lower()
    status = status_raw if status_raw in VALID_STATUSES else "active"

    return {
        "title":          title,
        "published_date": pub_date,
        "reference_no":   _clean_str(parsed.get("reference_no")),
        "year":           year,
        "status":         status,
    }


def _empty_metadata(filename: Optional[str] = None) -> dict:
    title = None
    if filename:
        title = filename.rsplit(".", 1)[0].replace("_", " ").replace("-", " ").strip()
    return {
        "title":          title,
        "published_date": None,
        "reference_no":   None,
        "year":           None,
        "status":         "active",   # safe default
    }

def extract_document_content(tmp_path: str, suffix: str) -> Tuple[str, Optional[str]]:
    """
    Extract plain text AND HTML from an uploaded file.

    Returns: (text, document_html)
        - text         : plain text for LLM analysis and metadata extraction
        - document_html: HTML via pdfco (PDF only) or None if unavailable/failed

    This is the single place responsible for file → content conversion so the
    API endpoint does not need to know about pdfco or OCR directly.
    """
    # Lazy imports — keeps this module lightweight if callers only need metadata
    from processor.Text_Extractor import OCRProcessor
    import docx as python_docx

    text = ""
    document_html = None

    if suffix == ".pdf":
        text, _ = OCRProcessor.extract_text_from_pdf_smart(tmp_path)
        text = text or ""

        # HTML via pdfco — same pipeline as the normal crawler flow
        try:
            from utils.pdfco_utils import pdfco_pdf_to_html
            document_html = pdfco_pdf_to_html(tmp_path)
            logger.info(f"[doc_extractor] pdfco HTML conversion succeeded ({len(document_html)} chars)")
        except Exception as e:
            logger.warning(f"[doc_extractor] pdfco HTML conversion failed (non-fatal): {e}")
            document_html = None

    elif suffix in (".docx", ".doc"):
        doc = python_docx.Document(tmp_path)
        text = "\n\n".join(p.text for p in doc.paragraphs if p.text.strip())
        # No HTML conversion for DOCX — document_html stays None

    else:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Only PDF and DOCX are accepted.",
        )

    return text or "", document_html

def _validate_date(val) -> Optional[str]:
    if not val:
        return None
    s = str(val).strip()
    # Gregorian DD/MM/YYYY
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
        return s
    # Hijri YYYY/MM/DD (e.g. 1447/07/09)
    if re.fullmatch(r"1[3-5]\d{2}/\d{2}/\d{2}", s):
        return s  # store as-is, or convert below
    # Coerce ISO format
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    return None

def _clean_str(val) -> Optional[str]:
    if not val:
        return None
    s = str(val).strip()
    return s if s and s.lower() != "null" else None