import os
import logging
import re
from typing import Dict, Tuple, List
from PIL import Image
import pytesseract
import platform

try:
    import fitz  # PyMuPDF
    import pdf2image

    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

logger = logging.getLogger(__name__)


class OCRProcessor:
    if platform.system() == "Windows":
        pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    else:
        pytesseract.pytesseract.tesseract_cmd = os.getenv("TESSERACT_PATH", "/usr/bin/tesseract")

    @staticmethod
    def is_ocr_available() -> bool:
        """Check if Tesseract OCR is properly installed with Arabic support"""
        if not OCR_AVAILABLE:
            return False

        try:
            langs = pytesseract.get_languages()
            return 'ara' in langs
        except Exception:
            return False

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

        logger.info(
            f" Done: {len(good_pages)}/{total_pages} pages kept, "
            f"{len(ocr_pages)} needed OCR"
        )

        return final_text, metadata

    @staticmethod
    def _is_pdf_scanned(pdf_doc) -> bool:
        """
        Check if PDF is scanned (images) or has extractable text
        Tests first 3 pages
        """
        pages_to_check = min(3, len(pdf_doc))
        total_text_length = 0

        for page_num in range(pages_to_check):
            page = pdf_doc[page_num]
            text = page.get_text("text").strip()
            total_text_length += len(text)

        # If first 3 pages have < 300 total chars, it's likely scanned
        avg_per_page = total_text_length / pages_to_check

        logger.info(f"First {pages_to_check} pages avg text: {avg_per_page:.0f} chars/page")

        if avg_per_page < 100:
            return True  # Scanned PDF
        return False  # Native text available

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
                logger.info(f"Page {page_num}: Skipped after OCR (low quality)")
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

        logger.info(
            f" OCR complete: {len(good_pages)}/{total_pages} pages kept"
        )

        return final_text, metadata

    @staticmethod
    def _is_bad_page(text: str) -> bool:
        """Check if page is garbage (cover, metadata, etc)"""
        if len(text) < 200:
            return True

        # Count real words
        words = [w for w in text.split() if len(w) > 3]
        if len(words) < 30:
            return True

        # Too many numbers? Probably metadata
        numbers = sum(c.isdigit() for c in text)
        if numbers / max(len(text), 1) > 0.5:
            return True

        # No Arabic or English? Probably garbage
        has_arabic = bool(re.search(r'[\u0600-\u06FF]{3,}', text))
        has_english = bool(re.search(r'[a-zA-Z]{3,}', text))
        if not (has_arabic or has_english):
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

        return False

    @staticmethod
    def _ocr_single_page(pdf_path: str, page_num: int) -> str:
        """Use OCR on one page"""
        try:
            images = pdf2image.convert_from_path(
                pdf_path,
                first_page=page_num,
                last_page=page_num,
                dpi=300
            )

            text = pytesseract.image_to_string(
                images[0],
                lang='ara+eng'
            )

            return text.strip()
        except Exception as e:
            logger.error(f"OCR failed on page {page_num}: {e}")
            return ""