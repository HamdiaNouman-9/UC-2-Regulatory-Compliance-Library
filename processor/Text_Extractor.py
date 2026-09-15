import os
import logging
import re
from typing import Dict, Tuple, List
from PIL import Image
import pytesseract
import platform

try:
    import fitz  # PyMuPDF — used for BOTH text extraction and rasterising

    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

logger = logging.getLogger(__name__)

# Which tesseract languages to try, most-likely first. Override per deployment:
#     OCR_LANGS=ara+eng+fra
# The library spans KSA, Egypt, Bahrain and more, so this must not be hardcoded
# to one country's scripts. Languages not installed are dropped with a warning
# rather than failing the whole call — tesseract errors out if you name a
# traineddata file it does not have.
OCR_LANGS = os.getenv("OCR_LANGS", "ara+eng")

# Rendering resolution for OCR. 300 is the usual floor for small Arabic type.
OCR_DPI = int(os.getenv("OCR_DPI", "300"))


class OCRProcessor:
    # TESSERACT_PATH wins on every platform. It was Windows-hardcoded, so a box
    # with tesseract anywhere else — a user-scope install, a different drive —
    # silently had no OCR at all.
    _default_exe = (r"C:\Program Files\Tesseract-OCR\tesseract.exe"
                    if platform.system() == "Windows" else "/usr/bin/tesseract")
    pytesseract.pytesseract.tesseract_cmd = os.getenv("TESSERACT_PATH", _default_exe)

    # The language files may live outside the install directory: on Windows
    # `tessdata` under Program Files needs admin to write, so extra languages go
    # in a user-owned folder and TESSDATA_PREFIX points at it. tesseract reads
    # that from the environment, so it only has to be present in os.environ.
    if os.getenv("TESSDATA_PREFIX"):
        logger.debug("TESSDATA_PREFIX=%s", os.getenv("TESSDATA_PREFIX"))

    _lang_cache = None

    @staticmethod
    def installed_languages() -> list:
        """Tesseract traineddata actually present, cached. Empty means tesseract
        is missing or unusable — which is a different problem from a bad scan and
        must be reported as such."""
        if OCRProcessor._lang_cache is None:
            try:
                OCRProcessor._lang_cache = list(pytesseract.get_languages())
            except Exception as e:
                logger.warning("tesseract not usable (%s) — OCR is unavailable. "
                               "Install it and set TESSERACT_PATH, or add it to PATH.", e)
                OCRProcessor._lang_cache = []
        return OCRProcessor._lang_cache

    @staticmethod
    def ocr_langs() -> str:
        """OCR_LANGS narrowed to what is installed.

        Naming a missing traineddata makes tesseract fail the whole page, so a
        deployment with only `eng` still OCRs English rather than returning
        nothing. What is missing is logged once.
        """
        have = set(OCRProcessor.installed_languages())
        want = [x for x in (OCR_LANGS or "").split("+") if x]
        usable = [x for x in want if x in have]
        missing = [x for x in want if x not in have]
        if missing:
            logger.warning("OCR language(s) not installed: %s. Using %s. "
                           "Arabic regulators need the 'ara' traineddata.",
                           "+".join(missing), "+".join(usable) or "(none)")
        return "+".join(usable)

    @staticmethod
    def is_ocr_available() -> bool:
        """Is OCR usable at all? Deliberately NOT "is Arabic installed" — that
        made an English-only box report OCR as entirely unavailable."""
        return bool(OCR_AVAILABLE and OCRProcessor.ocr_langs())

    @staticmethod
    def extract_text_from_pdf_smart(pdf_path: str) -> Tuple[str, Dict]:
        """
        Smart PDF extraction:
        1. Open PDF
        2. Try native text extraction on first few pages
        3. If mostly empty → PDF is scanned, use OCR on all pages
        4. If native extraction works → use it, OCR only broken pages
        5. Combine all good pages
        """

        pdf_doc = fitz.open(pdf_path)
        total_pages = len(pdf_doc)
        logger.info(f"Processing PDF: {total_pages} pages")

        # Check first 3 pages to determine if PDF is scanned
        is_scanned = OCRProcessor._is_pdf_scanned(pdf_doc)

        if is_scanned:
            logger.warning("PDF appears to be scanned images - will use OCR on all pages")
            pdf_doc.close()
            return OCRProcessor._ocr_entire_pdf(pdf_path, total_pages)

        # PDF has extractable text - proceed with smart filtering
        good_pages = []
        bad_pages = []
        ocr_pages = []

        for page_num in range(total_pages):
            page = pdf_doc[page_num]
            text = page.get_text("text")

            # Check 1: Is this page useful?
            if OCRProcessor._is_bad_page(text):
                bad_pages.append(page_num + 1)
                logger.info(f"Page {page_num + 1}: Skipped (bad quality)")
                continue

            # Check 2: Is the text readable?
            if OCRProcessor._is_text_broken(text):
                logger.info(f"Page {page_num + 1}: Text broken, using OCR...")
                text = OCRProcessor._ocr_single_page(pdf_path, page_num + 1)
                ocr_pages.append(page_num + 1)

            good_pages.append({
                'num': page_num + 1,
                'text': text
            })
            logger.info(f"Page {page_num + 1}: OK ({len(text)} chars)")

        pdf_doc.close()

        # Combine all good pages
        final_text = "\n\n".join([
            f"PAGE {p['num']}\n{p['text']}"
            for p in good_pages
        ])

        metadata = {
            'total_pages': total_pages,
            'good_pages': len(good_pages),
            'bad_pages': len(bad_pages),
            'ocr_pages': len(ocr_pages)
        }
        OCRProcessor._flag_low_quality(metadata)

        logger.info(
            f" Done: {len(good_pages)}/{total_pages} pages kept, "
            f"{len(ocr_pages)} needed OCR"
        )

        return final_text, metadata

    @staticmethod
    def _flag_low_quality(metadata: Dict) -> None:
        """Sets metadata['low_quality'] in place -- True when so few pages
        survived that the extraction is functionally worthless even though
        it isn't literally empty.

        good_pages == 0 already makes final_text empty, which callers (e.g.
        orchestrator.py's _download_and_extract_pdf) already treat as a
        failure via `if text_content:`. The gap this closes is the case where
        SOME pages survived -- final_text is non-empty, reads as a normal
        success -- but the fraction is so low the result is still not
        trustworthy, e.g. 1 good page out of 20 (a near-total OCR failure, or
        the "1 page, 123 chars" shape a WAF block page produces when it slips
        past the magic-bytes check some other way). Reuses _is_pdf_scanned's
        own 0.34 threshold rather than inventing an unvalidated second number
        for a very similar judgment call.
        """
        total = metadata.get('total_pages') or 0
        good = metadata.get('good_pages') or 0
        metadata['low_quality'] = bool(total) and (good / total) < 0.34

    @staticmethod
    def _is_pdf_scanned(pdf_doc) -> bool:
        """
        Check if PDF is scanned (images) or has extractable text
        Tests first 3 pages
        """
        n = len(pdf_doc)
        if n == 0:
            return False

        # Sample ACROSS the document, not the first three pages. A cover sheet, a
        # letterhead and a contents page were enough to send an 81-page
        # regulation — whose body extracts perfectly — to OCR on every page.
        idx = sorted({0, n // 4, n // 2, (3 * n) // 4, n - 1,
                      min(1, n - 1), min(2, n - 1)})
        lengths = [len(pdf_doc[i].get_text("text").strip()) for i in idx]

        # Judge on the FRACTION of sampled pages carrying text, not the average.
        # An average is dragged under the threshold by a couple of blank pages.
        with_text = sum(1 for L in lengths if L >= 100)
        fraction = with_text / len(lengths)

        logger.info("scanned-check: sampled pages %s -> lengths %s "
                    "(%d/%d carry text)", idx, lengths, with_text, len(lengths))

        # Under a third of sampled pages readable = treat it as a scan.
        return fraction < 0.34

    @staticmethod
    def _ocr_entire_pdf(pdf_path: str, total_pages: int) -> Tuple[str, Dict]:
        """
        OCR all pages of a scanned PDF with smart filtering
        """
        logger.info(f"Starting OCR on all {total_pages} pages...")

        good_pages = []
        bad_pages = []

        for page_num in range(1, total_pages + 1):
            logger.info(f"OCR processing page {page_num}/{total_pages}...")

            text = OCRProcessor._ocr_single_page(pdf_path, page_num)

            # Filter bad pages even after OCR
            if OCRProcessor._is_bad_page(text):
                bad_pages.append(page_num)
                if not text:
                    # Distinguish "OCR could not run" from "the scan is poor".
                    logger.info("Page %s: skipped — OCR produced nothing "
                                "(engine unavailable or blank page)", page_num)
                else:
                    logger.info("Page %s: skipped after OCR (low quality, "
                                "%d chars)", page_num, len(text))
                continue

            good_pages.append({
                'num': page_num,
                'text': text
            })
            logger.info(f"Page {page_num}: OK ({len(text)} chars)")

        # Combine all good pages
        final_text = "\n\n".join([
            f"PAGE {p['num']}\n{p['text']}"
            for p in good_pages
        ])

        metadata = {
            'total_pages': total_pages,
            'good_pages': len(good_pages),
            'bad_pages': len(bad_pages),
            'ocr_pages': len(good_pages)  # All good pages used OCR
        }
        OCRProcessor._flag_low_quality(metadata)

        logger.info(
            f" OCR complete: {len(good_pages)}/{total_pages} pages kept"
        )

        return final_text, metadata

    @staticmethod
    def _is_bad_page(text: str) -> bool:
        """Check if page is garbage (cover, metadata, etc)"""
        if len(text) < 200:
            return True

        # Count real words. Arabic words are short and Arabic PDFs often extract
        # with erratic spacing, so a ">3 characters" test under-counts them badly.
        # Words of any script count, and the floor is on LETTERS as well as words.
        words = [w for w in text.split() if len(w) > 1]
        letters = sum(1 for c in text if c.isalpha())
        if len(words) < 20 and letters < 200:
            return True

        # Too many numbers? Probably a table of figures rather than prose.
        numbers = sum(c.isdigit() for c in text)
        if numbers / max(len(text), 1) > 0.5:
            return True

        # Does it contain letters in ANY script?
        #
        # This previously required Arabic or English specifically, which quietly
        # discarded every page written in anything else. The library already spans
        # KSA, Egypt and Bahrain and is meant to grow \u2014 French for the Maghreb,
        # Turkish, Urdu for SBP \u2014 so a page must not be thrown away for being in a
        # language this function had not heard of. `str.isalpha()` is Unicode-aware
        # and covers all of them.
        if letters / max(len(text), 1) < 0.15:
            return True

        return False

    @staticmethod
    def _is_text_broken(text: str) -> bool:
        """Check if extracted text is garbled"""
        sample = text[:500]

        # Too many weird characters?
        if sample.count('\\u') > 15:
            return True

        # Can't read most characters?
        readable = sum(c.isprintable() or c.isspace() for c in sample)
        if len(sample) > 0 and readable / len(sample) < 0.7:
            return True

        # U+FFFD, the replacement character, is the real signal of a broken font
        # mapping — and unlike a script check it means the same thing in every
        # language. A PDF whose embedded encoding is wrong produces runs of these
        # and is a genuine candidate for OCR.
        if sample.count("�") > 5:
            return True

        return False

    @staticmethod
    def _ocr_single_page(pdf_path: str, page_num: int) -> str:
        """OCR one page.

        Rendered with PyMuPDF rather than pdf2image. pdf2image shells out to
        poppler (`pdftoppm`), which is not installed on the Windows boxes here —
        every page failed with "Unable to get page count. Is poppler installed?"
        and was then logged as "low quality", which is a different and misleading
        thing. fitz is already a dependency and needs no external binary.
        """
        langs = OCRProcessor.ocr_langs()
        if not langs:
            # Say the real reason once per page rather than blaming the scan.
            logger.error("page %s: OCR unavailable — tesseract missing or no "
                         "traineddata for %s", page_num, OCR_LANGS)
            return ""
        try:
            with fitz.open(pdf_path) as doc:
                page = doc[page_num - 1]
                # 72 dpi is fitz's default user space, so scale to reach OCR_DPI.
                pix = page.get_pixmap(matrix=fitz.Matrix(OCR_DPI / 72, OCR_DPI / 72))
                img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            return pytesseract.image_to_string(img, lang=langs).strip()
        except Exception as e:
            logger.error(f"OCR failed on page {page_num}: {e}")
            return ""