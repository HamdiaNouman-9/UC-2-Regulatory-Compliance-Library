"""
Combines PDF.co primary conversion with local fallbacks
"""

import os
import logging
from dotenv import load_dotenv
from typing import Optional
import re

import fitz
from pdf2image import convert_from_path
import pytesseract
from bs4 import BeautifulSoup
import pdfplumber
import cv2
import numpy as np
from PIL import Image
from pathlib import Path
from html import escape

from utils.pdfco_utils import pdfco_pdf_to_html

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configure Tesseract (adjust path as needed)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"


class HTMLFallbackEngine:
    """
    HTML-first engine with fallback:
    PDF.co (primary)
    Advanced PyMuPDF → for digital PDFs (preserves formatting, links, tables)
    Advanced Tesseract OCR → for scanned PDFs (with preprocessing and smart grouping)
    """
    def __init__(self, pdfco_key: Optional[str] = None):
        self.pdfco_key = pdfco_key

    def is_scanned_pdf(self, pdf_path: str, pages_to_check: int = 3) -> bool:
        """Detect if PDF is scanned or digital"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text_count = 0
                for i, page in enumerate(pdf.pages[:pages_to_check]):
                    text = page.extract_text()
                    if text and len(text.strip()) > 50:
                        text_count += 1
                return text_count == 0
        except Exception as e:
            logger.warning(f"Failed to check PDF type: {e}")
            return True  # assume scanned if error occurs

    def pymupdf_to_html(self, pdf_path: str) -> str:
        """Advanced fallback for digital PDFs using PyMuPDF with formatting preservation"""
        logger.info("Using advanced PyMuPDF extraction...")
        html_parts = []
        html_parts.append(self._get_html_header())

        try:
            doc = fitz.open(pdf_path)

            for page_num, page in enumerate(doc, 1):
                html_parts.append(f'<div class="page" data-page="{page_num}">')

                # Build a map of links by their position
                link_map = self._build_link_map(page)

                # Extract text with formatting
                blocks = page.get_text("dict")["blocks"]

                for block in blocks:
                    if block["type"] == 0:  # Text block
                        self._process_text_block(block, html_parts, link_map)
                    elif block["type"] == 1:  # Image block
                        html_parts.append('<div class="image-placeholder">[Image]</div>')

                # Extract tables using pdfplumber
                with pdfplumber.open(pdf_path) as pdf:
                    plumber_page = pdf.pages[page_num - 1]
                    tables = plumber_page.extract_tables()

                    for table in tables:
                        html_parts.append(self._table_to_html(table))

                html_parts.append('</div>')  # Close page div

            doc.close()

        except Exception as e:
            logger.error(f"PyMuPDF advanced extraction failed: {e}")
            # Fallback to basic PyMuPDF
            doc = fitz.open(pdf_path)
            html_parts = ["<html><body>"]
            for page in doc:
                html_parts.append(page.get_text("html"))
            html_parts.append("</body></html>")
            doc.close()
            return "\n".join(html_parts)

        html_parts.append('</body></html>')
        return '\n'.join(html_parts)

    def _build_link_map(self, page):
        """Build a map of text positions to their URLs"""
        link_map = {}
        links = page.get_links()

        for link in links:
            if "uri" in link and "from" in link:
                rect = link["from"]
                link_map[id(link)] = {
                    'uri': link['uri'],
                    'rect': rect,
                    'x0': rect.x0,
                    'y0': rect.y0,
                    'x1': rect.x1,
                    'y1': rect.y1
                }

        return link_map

    def _find_link_for_text(self, span_bbox, link_map):
        """Find if text span overlaps with any link"""
        x0, y0, x1, y1 = span_bbox

        for link_id, link_data in link_map.items():
            lx0, ly0, lx1, ly1 = link_data['x0'], link_data['y0'], link_data['x1'], link_data['y1']

            if (x0 < lx1 and x1 > lx0 and y0 < ly1 and y1 > ly0):
                return link_data['uri']

        return None

    def _process_text_block(self, block, html_parts, link_map):
        """Process text block with formatting and inline links"""
        for line in block.get("lines", []):
            line_html = []

            for span in line.get("spans", []):
                text = span["text"]
                if not text.strip():
                    continue

                font = span.get("font", "")
                size = span.get("size", 12)
                flags = span.get("flags", 0)
                bbox = span.get("bbox", (0, 0, 0, 0))

                # Check if this text is part of a link
                link_url = self._find_link_for_text(bbox, link_map)

                # Detect URL patterns in text
                url_pattern = r'https?://[^\s<>"\']+'
                if re.search(url_pattern, text) and not link_url:
                    match = re.search(url_pattern, text)
                    if match:
                        link_url = match.group(0)

                # Detect formatting
                is_bold = flags & 2**4 or "bold" in font.lower()
                is_italic = flags & 2**1 or "italic" in font.lower()

                # Build formatted text
                formatted_text = escape(text)

                if is_bold and is_italic:
                    formatted_text = f'<strong><em>{formatted_text}</em></strong>'
                elif is_bold:
                    formatted_text = f'<strong>{formatted_text}</strong>'
                elif is_italic:
                    formatted_text = f'<em>{formatted_text}</em>'

                # Wrap in link if URL found
                if link_url:
                    formatted_text = f'<a href="{escape(link_url)}">{formatted_text}</a>'

                # Apply font size styling
                style = f'font-size: {size}px;'

                # Detect headings based on size
                if size > 16:
                    line_html.append(f'<span class="heading-large" style="{style}">{formatted_text}</span>')
                elif size > 14:
                    line_html.append(f'<span class="heading-medium" style="{style}">{formatted_text}</span>')
                else:
                    line_html.append(f'<span style="{style}">{formatted_text}</span>')

            if line_html:
                html_parts.append(f'<p>{"".join(line_html)}</p>')

    def _table_to_html(self, table):
        """Convert table data to HTML table"""
        if not table:
            return ""

        html = ['<table class="pdf-table">']

        for i, row in enumerate(table):
            html.append('<tr>')
            tag = 'th' if i == 0 else 'td'

            for cell in row:
                cell_text = escape(str(cell)) if cell else ""
                # Check for URLs in table cells
                url_pattern = r'(https?://[^\s<>"\'\)]+)'
                cell_text = re.sub(url_pattern, r'<a href="\1">\1</a>', cell_text)
                html.append(f'<{tag}>{cell_text}</{tag}>')

            html.append('</tr>')

        html.append('</table>')
        return '\n'.join(html)

    def tesseract_to_html(self, pdf_path: str) -> str:
        """Advanced fallback for scanned PDFs using Tesseract OCR with preprocessing"""
        logger.info("Using advanced Tesseract OCR extraction...")
        html_parts = []
        html_parts.append(self._get_html_header())

        try:
            logger.info("Converting PDF to images...")
            images = convert_from_path(pdf_path, dpi=300)

            for page_num, img in enumerate(images, 1):
                logger.info(f"OCR processing page {page_num}/{len(images)}...")
                html_parts.append(f'<div class="page" data-page="{page_num}">')

                # Preprocess image for better OCR
                img_processed = self._preprocess_image(img)

                # Get OCR data with position information
                ocr_data = pytesseract.image_to_data(
                    img_processed,
                    output_type=pytesseract.Output.DICT,
                    config='--oem 3 --psm 3'  # Automatic page segmentation
                )

                # Process OCR data with better structure detection
                html_parts.extend(self._process_ocr_data(ocr_data))

                html_parts.append('</div>')

            html_parts.append('</body></html>')
            return '\n'.join(html_parts)

        except Exception as e:
            logger.error(f"Advanced OCR failed: {e}, using basic OCR...")
            # Fallback to basic OCR
            pages = convert_from_path(pdf_path)
            html_parts = ["<html><body>"]
            for i, page_image in enumerate(pages, start=1):
                text = pytesseract.image_to_string(page_image)
                html_parts.append(f"<div><h4>Page {i}</h4><p>{text}</p></div>")
            html_parts.append("</body></html>")
            return "\n".join(html_parts)

    def _preprocess_image(self, img):
        """Preprocess image for better OCR accuracy"""
        # Convert PIL to OpenCV format
        img_cv = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)

        # Convert to grayscale
        gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)

        # Apply adaptive thresholding
        binary = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 11, 2
        )

        # Denoise
        denoised = cv2.fastNlMeansDenoising(binary, None, 10, 7, 21)

        # Convert back to PIL
        return Image.fromarray(denoised)

    def _process_ocr_data(self, data):
        """Process OCR data with intelligent grouping"""
        html_parts = []

        # Group words into text blocks
        blocks = self._group_into_blocks(data)

        for block in blocks:
            if block['type'] == 'heading':
                size = block.get('avg_height', 12) * 1.5
                html_parts.append(
                    f'<p><span class="heading-large" style="font-size: {size}px;">'
                    f'<strong>{escape(block["text"])}</strong></span></p>'
                )
            elif block['type'] == 'paragraph':
                text = block['text']
                url_pattern = r'(https?://[^\s<>"\'\)]+)'
                text_with_links = re.sub(
                    url_pattern,
                    r'<a href="\1">\1</a>',
                    escape(text)
                )
                html_parts.append(f'<p>{text_with_links}</p>')
            elif block['type'] == 'list_item':
                text = block['text']
                html_parts.append(f'<p>• {escape(text)}</p>')

        return html_parts

    def _group_into_blocks(self, data):
        """Group OCR words into semantic blocks"""
        blocks = []
        current_block = {
            'words': [],
            'heights': [],
            'tops': [],
            'lefts': []
        }

        last_top = None
        line_threshold = 20
        paragraph_threshold = 40

        for i in range(len(data["text"])):
            word = data["text"][i].strip()
            conf = data["conf"][i]

            if not word or conf < 40:
                continue

            top = data["top"][i]
            left = data["left"][i]
            height = data["height"][i]

            # Check if new paragraph/block
            if last_top is not None:
                vertical_gap = top - last_top

                if vertical_gap > paragraph_threshold:
                    if current_block['words']:
                        blocks.append(self._finalize_block(current_block))
                        current_block = {
                            'words': [],
                            'heights': [],
                            'tops': [],
                            'lefts': []
                        }

                elif vertical_gap > line_threshold:
                    current_block['words'].append('\n')

            current_block['words'].append(word)
            current_block['heights'].append(height)
            current_block['tops'].append(top)
            current_block['lefts'].append(left)

            last_top = top + height

        if current_block['words']:
            blocks.append(self._finalize_block(current_block))

        return blocks

    def _finalize_block(self, block):
        """Determine block type and finalize text"""
        text = ' '.join(block['words']).strip()
        text = re.sub(r'\n+', '\n', text)
        text = re.sub(r' +', ' ', text)

        avg_height = sum(block['heights']) / len(block['heights']) if block['heights'] else 12

        block_type = 'paragraph'

        if avg_height > 20:
            block_type = 'heading'
        elif text.startswith(('•', '-', '*', '1.', '2.', '3.', '4.', '5.')):
            block_type = 'list_item'
            text = text[1:].strip() if text[0] in '•-*' else text
        elif len(text.split()) <= 5 and avg_height > 15:
            block_type = 'heading'

        return {
            'type': block_type,
            'text': text,
            'avg_height': avg_height
        }

    def _get_html_header(self):
        """Generate HTML header with CSS"""
        return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PDF Conversion</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }
        .page {
            background: white;
            padding: 40px;
            margin-bottom: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            page-break-after: always;
        }
        p {
            margin: 0.5em 0;
            line-height: 1.6;
            white-space: pre-wrap;
        }
        .heading-large {
            font-weight: bold;
            display: block;
            margin: 1em 0 0.5em 0;
        }
        .heading-medium {
            font-weight: bold;
            display: block;
            margin: 0.8em 0 0.4em 0;
        }
        a {
            color: #0066cc;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        .pdf-table {
            border-collapse: collapse;
            width: 100%;
            margin: 1em 0;
        }
        .pdf-table th, .pdf-table td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        .pdf-table th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        .image-placeholder {
            background: #e9e9e9;
            padding: 20px;
            text-align: center;
            margin: 1em 0;
            color: #666;
        }
    </style>
</head>
<body>
"""

    def trim_html_by_percentage(self, html: str, top_percent: float = 15.0, bottom_percent: float = 10.0) -> str:
        """Trim HTML content by percentage from top and bottom."""
        soup = BeautifulSoup(html, "html.parser")

        for page in soup.select("div.page"):
            style = page.get("style", "")
            height_match = re.search(r"height:\s*([\d.]+)px", style)

            if not height_match:
                continue

            page_height = float(height_match.group(1))
            top_limit = page_height * (top_percent / 100)
            bottom_limit = page_height * (1 - bottom_percent / 100)

            for el in page.find_all(["span", "div"]):
                el_style = el.get("style", "")
                top_match = re.search(r"top:\s*([\d.]+)px", el_style)

                if not top_match:
                    continue

                top = float(top_match.group(1))

                # Remove header
                if top < top_limit:
                    el.decompose()
                    continue

                # Remove footer
                if top > bottom_limit:
                    el.decompose()

        return str(soup)

    def remove_blank_page(self, html: str) -> str:
        """
        Remove blank pages from the HTML using multiple heuristics.
        Production-ready with comprehensive blank page detection.
        """
        soup = BeautifulSoup(html, "html.parser")
        pages = soup.find_all("div", class_="page")
        removed_count = 0

        for idx, page in enumerate(pages, start=1):
            # 1. Check for meaningful text content
            text_content = page.get_text(strip=True)
            has_text = len(text_content) > 10

            # 2. Check for meaningful images
            images = page.find_all("img")
            has_real_images = any(
                img.get("class") and "dummyimg" not in img.get("class", [])
                for img in images
            )

            # 3. Check for interactive elements
            has_interactive = bool(
                page.find_all(["button", "input", "textarea"]) or
                page.find_all("a", href=True, string=lambda s: s and s.strip())
            )

            # 4. Check for visible annotations
            annotations = page.find_all("div", class_="annotation")
            has_annotations = any(
                ann.get_text(strip=True) or ann.find("img", class_=lambda x: x != "dummyimg")
                for ann in annotations
            )

            # Remove page if it has no meaningful content
            if not (has_text or has_real_images or has_interactive or has_annotations):
                page.decompose()
                removed_count += 1
                logger.info(f"Removed blank page {idx} (no meaningful content)")

        if removed_count > 0:
            logger.info(f"Total blank pages removed: {removed_count}")

        return str(soup)

    def process_pdf_to_html(self, pdf_path: str, regulator_name: str) -> str:
        """
        Main processing method with fallback chain:
        1. PDF.co (primary)
        2. Advanced PyMuPDF (digital PDFs)
        3. Advanced Tesseract OCR (scanned PDFs)
        """
        #Try PDF.co first
        html = ""
        try:
            html = pdfco_pdf_to_html(pdf_path)
            logger.info("PDF.co HTML generated successfully")
        except Exception as e:
            logger.warning(f"PDF.co failed: {e}")

        # Detect PDF type
        scanned = self.is_scanned_pdf(pdf_path)

        # Fallback for digital PDFs: Advanced PyMuPDF
        if not scanned and not html:
            try:
                logger.info("Using advanced PyMuPDF fallback for digital PDF...")
                html = self.pymupdf_to_html(pdf_path)
                logger.info("Advanced PyMuPDF HTML generated successfully")
            except Exception as e:
                logger.error(f"Advanced PyMuPDF fallback failed: {e}")

        # Fallback for scanned PDFs: Advanced Tesseract OCR
        if scanned and not html:
            try:
                logger.info("Using advanced Tesseract OCR fallback for scanned PDF...")
                html = self.tesseract_to_html(pdf_path)
                logger.info("Advanced Tesseract OCR HTML generated successfully")
            except Exception as e:
                logger.error(f"Advanced Tesseract fallback failed: {e}")

        # Apply regulator-specific processing
        if regulator_name.upper() == "SBP":
            logger.info("Applying SBP header/footer trimming...")
            html = self.trim_html_by_percentage(html)

        # Remove blank pages
        html = self.remove_blank_page(html)

        return html