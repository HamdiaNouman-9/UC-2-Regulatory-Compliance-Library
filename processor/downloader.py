import os
import re
import hashlib
from pathlib import Path
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse
import unicodedata
import subprocess
import sys


class Downloader:
    # Extensions that should be downloaded directly
    DIRECT_DOWNLOAD_EXTENSIONS = {
        "pdf", "doc", "docx", "xls", "xlsx", "csv", "zip", "rtf", "txt"
    }

    def __init__(self, download_dir="downloads", headless=True, retries=3, backoff=1.5):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)

        self.headless = headless
        self.retries = retries
        self.backoff = backoff

        # CREATE ROBUST SESSION WITH RETRY LOGIC
        self.session = self._create_robust_session()

    def _create_robust_session(self):
        """Create session with retry logic for network issues"""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })

        return session

    # FILENAME SANITIZER
    def _sanitize_filename(self, name: str) -> str:
        if not name:
            return "document"

        name = unicodedata.normalize("NFKD", name)
        name = name.replace("\r", " ").replace("\n", " ").replace("\t", " ")
        name = re.sub(r'[<>:"/\\|?*]', "_", name)
        name = re.sub(r"\s+", " ", name).strip()

        if not name:
            return "document"

        return name[:200]

    # FILE HASH
    def _compute_hash(self, file_path: Path) -> str:
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def download(self, document) -> tuple[str, str]:
        """Accepts either dict or object with attributes"""

        def get_attr_or_key(obj, *keys):
            """Try to get attribute or dict key in order."""
            for key in keys:
                # Try attribute
                val = getattr(obj, key, None)
                if val is not None:
                    return val
                # Try dictionary key
                if isinstance(obj, dict) and key in obj:
                    return obj[key]
            return None

        title = get_attr_or_key(document, "title") or "document"
        title = self._sanitize_filename(title)

        # Determine main working URL
        url = get_attr_or_key(
            document,
            "document_url",
            "english_url",
            "circular_url",
            "urdu_url"
        )

        if not url or url.lower().startswith("javascript"):
            raise ValueError(f"[Downloader] Invalid or unsupported URL: {url}")

        filename_safe = title
        ext = self._extract_extension(url)

        # Quick binary download check
        if ext in self.DIRECT_DOWNLOAD_EXTENSIONS:
            return self._download_binary(url, filename_safe, ext)

        # HEAD request to detect content type
        try:
            head = self.session.head(url, allow_redirects=True, timeout=15)
            content_type = head.headers.get("Content-Type", "").lower()
            if "pdf" in content_type:
                print(f"[Downloader] HEAD detected PDF → binary download: {url}")
                return self._download_binary(url, filename_safe, "pdf")
        except requests.exceptions.ConnectionError as e:
            print(f"[Downloader] HEAD request failed - DNS/Network error for {url}: {e}")
            # Try binary download anyway
            return self._download_binary(url, filename_safe, ext or "pdf")
        except Exception as e:
            print(f"[Downloader] HEAD request failed for {url}: {e}")

        # Default: HTML → PDF using subprocess to avoid asyncio conflicts
        return self._html_to_pdf_subprocess(url, filename_safe)

    def _extract_extension(self, url: str) -> str:
        path = urlparse(url).path
        if "." in path:
            return path.split(".")[-1].lower()
        return ""

    def _download_binary(self, url: str, filename: str, ext: str):
        """
        Download binary file with enhanced error handling and retry logic
        """
        file_path = self.download_dir / f"{filename}.{ext}"

        for attempt in range(1, self.retries + 1):
            try:
                print(f"[Downloader] Attempting download ({attempt}/{self.retries}): {url}")

                # ENHANCED ERROR HANDLING
                resp = self.session.get(url, stream=True, timeout=60)
                resp.raise_for_status()

                with open(file_path, "wb") as f:
                    for chunk in resp.iter_content(1024 * 1024):
                        f.write(chunk)

                file_hash = self._compute_hash(file_path)
                print(f"[Downloader] ✓ Saved binary: {file_path}")
                return str(file_path), file_hash

            except requests.exceptions.ConnectionError as e:
                print(f"[Downloader] ✗ Network/DNS error ({attempt}/{self.retries}): {e}")
                if "Failed to resolve" in str(e) or "getaddrinfo failed" in str(e):
                    print(f"[Downloader] DNS resolution failed for: {urlparse(url).hostname}")
                    print(f"[Downloader] Check: Internet connection, VPN, firewall, or domain accessibility")

                if attempt < self.retries:
                    wait_time = self.backoff * attempt
                    print(f"[Downloader] Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise RuntimeError(
                        f"Network error: Cannot reach {url}. "
                        f"DNS resolution failed for '{urlparse(url).hostname}'. "
                        f"Check internet connection or VPN requirements."
                    )

            except requests.exceptions.Timeout as e:
                print(f"[Downloader] ✗ Timeout error ({attempt}/{self.retries}): {e}")
                if attempt < self.retries:
                    wait_time = self.backoff * attempt
                    print(f"[Downloader] Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise RuntimeError(f"Timeout: Server took too long to respond for {url}")

            except requests.exceptions.HTTPError as e:
                print(f"[Downloader] ✗ HTTP error ({attempt}/{self.retries}): {e}")
                if e.response.status_code in [429, 500, 502, 503, 504]:
                    # Retryable errors
                    if attempt < self.retries:
                        wait_time = self.backoff * attempt
                        print(f"[Downloader] Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                else:
                    # Non-retryable HTTP errors (404, 403, etc.)
                    raise RuntimeError(f"HTTP {e.response.status_code}: {url}")

            except Exception as e:
                print(f"[Downloader] ✗ Unexpected error ({attempt}/{self.retries}): {e}")
                if attempt < self.retries:
                    wait_time = self.backoff * attempt
                    print(f"[Downloader] Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise

        raise RuntimeError(f"[Downloader] FAILED to download file after {self.retries} retries → {url}")

    def _html_to_pdf_subprocess(self, url: str, filename: str):
        """
        Use subprocess to run Playwright in a separate process.
        This avoids asyncio event loop conflicts with Twisted/Scrapy.
        """
        file_path = self.download_dir / f"{filename}.pdf"

        # Create a temporary Python script to run Playwright
        script_content = f'''
import sys
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

async def render_pdf(url, output_path):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle", timeout=200000)

        # Check for iframe
        frames = page.frames
        if len(frames) > 1:
            await frames[1].pdf(path=output_path, format="A4", print_background=True)
        else:
            await page.pdf(path=output_path, format="A4", print_background=True)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(render_pdf("{url}", r"{file_path}"))
'''

        for attempt in range(1, self.retries + 1):
            try:
                # Write temporary script
                temp_script = self.download_dir / f"_temp_playwright_{os.getpid()}.py"
                with open(temp_script, "w", encoding="utf-8") as f:
                    f.write(script_content)

                # Run in subprocess
                result = subprocess.run(
                    [sys.executable, str(temp_script)],
                    capture_output=True,
                    text=True,
                    timeout=300
                )

                # Clean up temp script
                temp_script.unlink(missing_ok=True)

                if result.returncode == 0 and file_path.exists():
                    file_hash = self._compute_hash(file_path)
                    print(f"[Downloader] Saved PDF: {file_path}")
                    return str(file_path), file_hash
                else:
                    raise RuntimeError(f"Subprocess failed: {result.stderr}")

            except Exception as e:
                print(f"[Downloader] HTML→PDF failed ({attempt}/{self.retries}): {e}")
                time.sleep(self.backoff * attempt)

        raise RuntimeError(f"[Downloader] FAILED to render PDF after retries → {url}")