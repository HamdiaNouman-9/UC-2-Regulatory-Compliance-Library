import hashlib
import os
import uuid
from pathlib import Path
from typing import Iterable, Tuple


class LocalBlobStore:
    """Files named by their sha256 under root_dir. Swap this class for an
    Azure/S3 one later -- store.py only uses the methods below."""

    def __init__(self, root_dir: str):
        self.root = Path(root_dir)
        self.tmp_dir = self.root / "_tmp"
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def write_temp(self, chunks: Iterable[bytes]) -> Tuple[Path, str, int]:
        """Stream chunks to a temp file, hashing as they arrive.
        Returns (temp path, sha256, size)."""
        tmp = self.tmp_dir / f"{uuid.uuid4().hex}.part"
        h = hashlib.sha256()
        size = 0
        try:
            with open(tmp, "wb") as f:
                for chunk in chunks:
                    if not chunk:
                        continue
                    f.write(chunk)
                    h.update(chunk)
                    size += len(chunk)
        except BaseException:
            tmp.unlink(missing_ok=True)   # never leave a half file behind
            raise
        return tmp, h.hexdigest(), size

    @staticmethod
    def relative_path(sha256: str, extension: str) -> str:
        # Two folder levels so no single folder ends up with 100k files.
        return f"{sha256[:2]}/{sha256[2:4]}/{sha256}{extension}"

    def commit(self, tmp: Path, sha256: str, extension: str) -> str:
        rel = self.relative_path(sha256, extension)
        final = self.root / rel
        final.parent.mkdir(parents=True, exist_ok=True)
        if final.exists():
            tmp.unlink(missing_ok=True)
        else:
            os.replace(tmp, final)   # same disk -> instant, all-or-nothing
        return rel

    def discard(self, tmp: Path) -> None:
        tmp.unlink(missing_ok=True)

    def exists(self, rel: str) -> bool:
        return (self.root / rel).exists()

    def absolute(self, rel: str) -> str:
        return str(self.root / rel)
