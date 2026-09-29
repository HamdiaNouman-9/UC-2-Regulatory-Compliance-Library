import mimetypes
import re
import zipfile
from pathlib import PurePosixPath
from typing import Iterable, Optional
from urllib.parse import unquote, urlsplit

OLE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"   # old binary Office: .doc/.xls/.ppt
ZIP_MAGIC = b"PK\x03\x04"                         # new Office: .docx/.xlsx/.pptx

_OLE_EXTS = {".doc", ".xls", ".ppt"}
_OLE_CONTENT_TYPES = {
    "application/msword": ".doc",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.ms-powerpoint": ".ppt",
}
_HTML_STARTS = (b"<!doctype", b"<html", b"<head", b"<body", b"<script", b"<meta", b"<title")
_DISPOSITION_RE = re.compile(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)", re.I)


class NotADocument(Exception):
    """A web page came back where a file was expected -- usually a block,
    captcha or login page served with status 200."""


def filename_from_disposition(value: Optional[str]) -> Optional[str]:
    """`attachment; filename="law.docx"` -> law.docx"""
    if not value:
        return None
    m = _DISPOSITION_RE.search(value)
    return unquote(m.group(1)).strip() if m else None


def _ext(name: Optional[str]) -> str:
    if not name:
        return ""
    suffix = PurePosixPath(urlsplit(name).path).suffix.lower()
    return suffix if 1 < len(suffix) <= 6 else ""


def looks_like_html(head: bytes) -> bool:
    start = head.lstrip(b"\xef\xbb\xbf \t\r\n").lower()
    return start.startswith(_HTML_STARTS)


def detect_extension(path, name_hints: Iterable[Optional[str]], content_type: Optional[str]) -> str:
    """The file's real type from its first bytes. name_hints are the download's
    filename and the URL, most trusted first -- used only where the bytes can't
    tell (old Office formats, plain text types)."""
    hints = [e for e in (_ext(h) for h in name_hints) if e]

    with open(path, "rb") as f:
        head = f.read(2048)

    if looks_like_html(head):
        raise NotADocument(f"got a web page, not a file (content-type {content_type!r})")

    if b"%PDF" in head[:1024]:          # the spec allows junk before the marker
        return ".pdf"

    if head.startswith(ZIP_MAGIC):
        try:
            with zipfile.ZipFile(path) as z:
                names = z.namelist()
        except zipfile.BadZipFile:
            return hints[0] if hints else ".zip"
        for folder, ext in (("word/", ".docx"), ("xl/", ".xlsx"), ("ppt/", ".pptx")):
            if any(n.startswith(folder) for n in names):
                return ext
        return hints[0] if hints else ".zip"

    if head.startswith(OLE_MAGIC):
        for ext in hints:
            if ext in _OLE_EXTS:
                return ext
        return _OLE_CONTENT_TYPES.get(content_type or "", ".doc")

    return (hints[0] if hints else None) or mimetypes.guess_extension(content_type or "") or ".bin"
