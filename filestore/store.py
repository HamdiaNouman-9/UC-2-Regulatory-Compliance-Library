import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import requests

from .blobs import LocalBlobStore
from .config import FileStoreConfig
from .db import FileStoreDB
from .extractors import extract
from .filetypes import NotADocument, detect_extension, filename_from_disposition
from .urls import normalize_url, url_key

logger = logging.getLogger(__name__)

# (file path, extension) -> (text, metadata), or None for types without text.
Extractor = Callable[[str, str], Optional[Tuple[str, Dict]]]


@dataclass
class FileRef:
    sha256: str
    path: str        # absolute path on disk
    url: str         # normalized url
    is_new: bool     # True only the first time this exact file was stored
    source: str      # 'memo' | 'not_modified' | 'downloaded'


@dataclass
class FileText:
    sha256: str
    method: str
    text: str
    text_hash: str
    page_count: Optional[int] = None
    low_quality: Optional[bool] = None
    url: Optional[str] = None


def compute_text_hash(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class FileStore:
    def __init__(self, config: FileStoreConfig = None, session: requests.Session = None,
                 extractor: Extractor = None, db: FileStoreDB = None, blobs: LocalBlobStore = None):
        self.config = config or FileStoreConfig.from_env()
        # Pass the crawler's own session to reuse its cookies, headers and throttling.
        self.session = session or requests.Session()
        self.extractor = extractor or extract
        self.db = db or FileStoreDB(self.config.mssql)
        self.blobs = blobs or LocalBlobStore(self.config.root_dir)
        self._memo: Dict[str, Tuple[str, str]] = {}   # normalized url -> (sha256, rel path), this run only

    # ------------------------------------------------------------ public API

    def fetch(self, url: str, document_key: Optional[str] = None, role: str = "attachment",
              extract_text: bool = True) -> FileRef:
        """document_key links the file to a document for get_texts_for_document().
        Leave it out when the caller only has the URL -- the file and its text
        are stored and deduplicated all the same."""
        norm = normalize_url(url)
        ukey = url_key(norm)

        if norm in self._memo:
            (sha, rel), is_new, source = self._memo[norm], False, "memo"
        else:
            sha, rel, is_new, source = self._download(url, norm, ukey)
            self._memo[norm] = (sha, rel)

        if document_key:
            self.db.link_document(document_key, ukey, norm, sha, role)

        if extract_text:
            try:
                self.ensure_text(sha)
            except Exception as e:
                # The file is stored either way; text can be retried with ensure_text().
                logger.error(f"text extraction failed for {sha[:12]} ({norm}): {e}")

        logger.info(f"{source:<12} new={is_new!s:<5} {sha[:12]} {norm}")
        return FileRef(sha, self.blobs.absolute(rel), norm, is_new, source)

    def ensure_text(self, sha256: str, reextract: bool = False) -> Optional[FileText]:
        """Text for this file, extracting it only if we don't have it yet."""
        row = self.db.get_text(sha256)
        if row and not reextract:
            return FileText(row.sha256, row.method, row.full_text, row.text_hash,
                            row.page_count, row.low_quality)

        f = self.db.get_file(sha256)
        if f is None:
            raise KeyError(f"no stored file {sha256}")

        result = self.extractor(self.blobs.absolute(f.storage_path), f.extension or "")
        if result is None:
            return None   # a type we don't extract text from
        text, meta = result
        text = text or ""
        ft = FileText(sha256, meta.get("method", "native"), text, compute_text_hash(text),
                      meta.get("total_pages"), meta.get("low_quality"))
        self.db.save_text(sha256, ft.method, self.config.extractor_version, ft.page_count,
                          meta.get("ocr_pages"), ft.low_quality, ft.text_hash, text)
        return ft

    def get_text(self, sha256: str) -> Optional[FileText]:
        """Stored text only -- never triggers extraction."""
        row = self.db.get_text(sha256)
        if row is None:
            return None
        return FileText(row.sha256, row.method, row.full_text, row.text_hash,
                        row.page_count, row.low_quality)

    def get_texts_for_document(self, document_key: str) -> List[FileText]:
        """Text of every attachment linked to a document (several for multi-attachment rows)."""
        return [
            FileText(r.sha256, r.method, r.full_text, r.text_hash, r.page_count, r.low_quality, r.url)
            for r in self.db.get_document_files(document_key)
            if r.full_text is not None
        ]

    def close(self) -> None:
        self.db.close()

    # ------------------------------------------------------------ internals

    def _download(self, url: str, norm: str, ukey: str) -> Tuple[str, str, bool, str]:
        seen = self.db.get_url(ukey)
        known = None
        headers = {}
        if seen and seen.sha256:
            known = self.db.get_file(seen.sha256)
            if known and self.blobs.exists(known.storage_path):
                # Ask the server "has it changed since?" -- a 304 costs no download.
                if seen.etag:
                    headers["If-None-Match"] = seen.etag
                if seen.last_modified:
                    headers["If-Modified-Since"] = seen.last_modified
            else:
                known = None   # the file went missing on disk: download it again

        with self.session.get(url, headers=headers, stream=True, timeout=self.config.timeout) as resp:
            if resp.status_code == 304 and known:
                self.db.touch_url(ukey)
                return seen.sha256, known.storage_path, False, "not_modified"
            resp.raise_for_status()
            if resp.status_code != 200:
                raise requests.HTTPError(f"unexpected status {resp.status_code} for {url}")
            tmp, sha, size = self.blobs.write_temp(resp.iter_content(self.config.chunk_size))
            etag = resp.headers.get("ETag")
            last_modified = resp.headers.get("Last-Modified")
            content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip() or None
            disposition_name = filename_from_disposition(resp.headers.get("Content-Disposition"))

        try:
            ext = detect_extension(tmp, [disposition_name, url], content_type)
        except NotADocument:
            self.blobs.discard(tmp)   # block/login page: store nothing, retry next run
            raise

        existing = self.db.get_file(sha)
        if existing and self.blobs.exists(existing.storage_path):
            self.blobs.discard(tmp)   # same bytes already stored: this was a copy
            rel, is_new = existing.storage_path, False
        else:
            ext = existing.extension if existing else ext
            rel = self.blobs.commit(tmp, sha, ext)
            is_new = self.db.insert_file(sha, size, content_type, ext, rel, norm)

        self.db.upsert_url(ukey, norm, sha, etag, last_modified)
        return sha, rel, is_new, "downloaded"
