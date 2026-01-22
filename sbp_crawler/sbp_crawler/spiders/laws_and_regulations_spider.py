import re
from urllib.parse import urljoin
import scrapy
from models.models import RegulatoryDocument

DOC_EXTS = (".pdf", ".doc", ".docx", ".htm", ".html")


def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


class SBPLawsSpider(scrapy.Spider):
    name = "sbp_laws"
    allowed_domains = ["sbp.org.pk", "pakistancode.gov.pk"]
    start_urls = ["https://www.sbp.org.pk/l_frame/index2.asp"]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS": 1,
        "ROBOTSTXT_OBEY": True,
        "LOG_LEVEL": "INFO",
        "USER_AGENT": "SBP-Compliance-Monitor/1.0",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def __init__(self, shared_items=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shared_items = shared_items if shared_items is not None else []
        self.logger.info("🚀 SBPLawsSpider initialized")

    def parse_pakistan_code_page(self, response):
        section = response.meta.get("section", "Law")
        parent_title = response.meta.get("title", "")
        source_page = response.meta.get("source_page", response.url)

        self.logger.info(f"🔍 Parsing Pakistan Code page: {response.url}")

        # Get title from the page
        title = response.xpath("//h2/text()").get(default="").strip()
        if not title:
            title = parent_title

        # Try multiple XPath patterns to find the PDF download link
        pdf_href = None

        # Pattern 1: Direct PDF link in href
        pdf_href = response.xpath("//a[contains(@href, '.pdf')]/@href").get()
        if pdf_href:
            self.logger.debug(f"Found PDF via Pattern 1: {pdf_href}")

        # Pattern 2: Look in download div
        if not pdf_href:
            pdf_href = response.xpath("//div[@id='download']//a/@href").get()
            if pdf_href:
                self.logger.debug(f"Found PDF via Pattern 2: {pdf_href}")

        # Pattern 3: Look for any anchor with onclick
        if not pdf_href:
            onclick = response.xpath("//a/@onclick").get()
            if onclick:
                match = re.search(r"['\"]([^'\"]+\.pdf)['\"]", onclick)
                if match:
                    pdf_href = match.group(1)
                    self.logger.debug(f"Found PDF via Pattern 3: {pdf_href}")

        # Pattern 4: Look in page source for pdffiles URL
        if not pdf_href:
            body_text = response.text
            match = re.search(r'(/?pdffiles/administrator[a-f0-9]+\.pdf)', body_text)
            if match:
                pdf_href = match.group(1)
                self.logger.debug(f"Found PDF via Pattern 4: {pdf_href}")

        # Pattern 5: Check for Download Now button
        if not pdf_href:
            download_link = response.xpath(
                "//a[contains(text(), 'Download') or contains(text(), 'DOWNLOAD')]/@href"
            ).get()
            if download_link:
                pdf_href = download_link

        if not pdf_href:
            self.logger.warning(f"⚠️ No PDF found for: {response.url}")
            self.logger.warning(f"Page title: {title}")
            return

        pdf_url = response.urljoin(pdf_href)
        self.logger.info(f"✅ PDF extracted: {pdf_url}")

        doc_path = ["SBP", "Laws & Regulations", section, title]

        reg_doc = RegulatoryDocument(
            regulator="SBP",
            source_system="SBP-LAW",
            category=section,
            title=title,
            document_url=pdf_url,
            urdu_url=None,
            published_date=None,
            reference_no=None,
            department=[section],
            year=None,
            source_page_url=response.url,
            doc_path=doc_path,
            extra_meta={
                "status": "being updated",
                "language": "EN",
                "source": "Pakistan Code"
            }
        )

        self.shared_items.append(reg_doc)
        self.logger.info(f"📦 Appended Pakistan Code document: {title}")

        yield reg_doc

    def parse(self, response):
        self.logger.info(f"📄 Parsing main Laws & Regulations page: {response.url}")

        current_section = None

        for row in response.xpath("//table//tr"):
            # Check if this is a section header
            section_header = row.xpath(".//td[@bgcolor='#ECF5FB']//span[@class='style34']/text()").get()
            if section_header:
                current_section = clean(section_header)
                if current_section:  # Only log non-empty sections
                    self.logger.info(f"📁 Section detected: {current_section}")
                continue

            # Skip navigation/footer rows
            row_text = clean(" ".join(row.xpath(".//text()").getall()))
            if any(t in row_text.lower() for t in [
                "home", "about sbp", "best view", "copyright", "contact",
                "library", "events", "sitemap"
            ]):
                continue

            # Find document links
            bullet = row.xpath(".//img[contains(@src,'square-bulit')]")
            if not bullet:
                continue

            links = row.xpath(".//a[@href]")
            if not links:
                continue

            first_link = links[0]
            title = clean(" ".join(first_link.xpath(".//text()").getall()))
            href = first_link.xpath("@href").get()

            if not title or not href:
                continue

            url = urljoin(response.url, href)

            # Special handling for Prudential Regulations and AML pages
            # These are .htm files but should be treated as pages with sub-documents
            is_prudential_page = "Prudential Regulations" in title
            is_aml_page = "Anti-Money Laundering" in title or "AML" in title

            if is_prudential_page or is_aml_page:
                # These are pages with multiple documents, not direct downloads
                self.logger.info(f"🔗 Following {'Prudential' if is_prudential_page else 'AML'} page: {title}")
                yield scrapy.Request(
                    url,
                    callback=self.parse_subpage,
                    meta={
                        "section": current_section or "Regulations",
                        "parent_title": title,
                        "parent_url": response.url,
                    },
                )
            elif url.lower().endswith(DOC_EXTS):
                # Check if it's a Foreign Exchange Manual page (which has sub-documents)
                if "Foreign Exchange Manual" in title:
                    self.logger.info(f"🔗 Following page: {title}")
                    yield scrapy.Request(
                        url,
                        callback=self.parse_subpage,
                        meta={
                            "section": current_section or "Regulations",
                            "parent_title": title,
                            "parent_url": response.url,
                        },
                    )
                else:
                    # Regular direct document link
                    self.logger.info(f"📄 Direct document: {title}")
                    doc_path = ["SBP", "Laws & Regulations", current_section or "Unknown", title]

                    reg_doc = RegulatoryDocument(
                        regulator="SBP",
                        source_system="SBP-LAW",
                        category=current_section or "Law",
                        title=title,
                        document_url=url,
                        urdu_url=None,
                        published_date=None,
                        reference_no=None,
                        department=[current_section] if current_section else [],
                        year=None,
                        source_page_url=response.url,
                        doc_path=doc_path,
                    )

                    self.shared_items.append(reg_doc)
                    self.logger.info(f"📦 Appended document: {title}")

                    yield reg_doc

            elif "pakistancode.gov.pk" in url:
                self.logger.info(f"🔗 Following Pakistan Code page: {title}")
                yield scrapy.Request(
                    url,
                    callback=self.parse_pakistan_code_page,
                    meta={
                        "section": current_section or "Law",
                        "title": title,
                        "source_page": response.url,
                    },
                )

            else:
                # Page with more content - follow it
                self.logger.info(f"🔗 Following page: {title}")
                yield scrapy.Request(
                    url,
                    callback=self.parse_subpage,
                    meta={
                        "section": current_section or "Unknown",
                        "parent_title": title,
                        "parent_url": response.url,
                    },
                )

    def parse_subpage(self, response):
        section = response.meta["section"]
        parent_title = response.meta["parent_title"]
        parent_url = response.meta["parent_url"]

        self.logger.info(f"📑 Parsing subpage: {parent_title}")
        self.logger.info(f"🔗 URL: {response.url}")

        # Case 1: Foreign Exchange Manual - Chapter-based structure
        if "fe_manual" in response.url or "Foreign Exchange Manual" in parent_title:
            yield from self.parse_fe_manual(response, section, parent_title, parent_url)
            return

        # Case 2: Prudential Regulations or AML/CFT/CPF - Pages with Download links
        documents_found = False

        # Special handling for Prudential Regulations and AML pages
        is_prudential = "prudential" in response.url.lower() or "Prudential Regulations" in parent_title
        is_aml = "aml" in response.url.lower() or "Anti-Money Laundering" in parent_title or "AML" in parent_title

        if is_prudential or is_aml:
            self.logger.info(f"🏦 Processing {'Prudential Regulations' if is_prudential else 'AML/CFT/CPF'} page")

            # Pattern for Prudential/AML pages: Look for rows with document titles and Download links
            for row in response.xpath("//table//tr[td]"):
                # Get all cells in the row
                cells = row.xpath("./td")

                if len(cells) < 2:
                    continue

                # First cell usually contains the document title
                title_cell = cells[0]
                title = clean(title_cell.xpath("string(.)").get())

                # Skip header rows and empty rows
                if not title or len(title) < 5:
                    continue

                # Skip if this is a header row
                if any(keyword in title.lower() for keyword in ["english", "urdu", "size", "download"]):
                    continue

                # Look for Download link in any cell of this row
                download_link = row.xpath(
                    ".//a[contains(text(), 'Download') or contains(@href, 'Download')]/@href").get()

                if download_link:
                    url = urljoin(response.url, download_link)
                    documents_found = True

                    self.logger.info(f"📄 Document found: {title}")
                    self.logger.info(f"   URL: {url}")

                    # Set doc_path based on whether it's Prudential or AML
                    if is_prudential:
                        doc_path = ["SBP", "Laws & Regulations", "Regulations", "Prudential Regulations", title]
                    else:
                        doc_path = ["SBP", "Laws & Regulations", "Regulations",
                                    "Anti-Money Laundering, Combating the Financing of Terrorism & Countering Proliferation Financing",
                                    title]

                    reg_doc = RegulatoryDocument(
                        regulator="SBP",
                        source_system="SBP-LAW",
                        category="Regulations",
                        title=title,
                        document_url=url,
                        urdu_url=None,
                        published_date=None,
                        reference_no=None,
                        department=["Regulations", parent_title],
                        year=None,
                        source_page_url=response.url,
                        doc_path=doc_path,
                    )

                    self.shared_items.append(reg_doc)
                    self.logger.info(f"📦 Appended document: {title}")

                    yield reg_doc

            if documents_found:
                return  # Exit early if we found documents

        # Case 3: Other subpages - Generic document extraction
        if not documents_found:
            # Pattern 1: Look for structured table with "Download" links
            for row in response.xpath("//table//tr | //div[contains(@class, 'content')]//tr"):
                row_text = clean(" ".join(row.xpath(".//text()").getall()))

                # Skip headers, navigation, and empty rows
                if not row_text or any(t in row_text.lower() for t in [
                    "home", "about sbp", "best view", "copyright", "contact",
                    "english", "size", "download"  # Skip header rows
                ]):
                    continue

                # Look for download links in the row
                download_links = row.xpath(
                    ".//a[contains(text(), 'Download') or contains(@href, 'Download')]/@href").getall()

                if download_links:
                    # Get the title from the row
                    title_element = row.xpath(".//td[1] | .//td[not(contains(., 'Download'))]")
                    if title_element:
                        title = clean(" ".join(title_element[0].xpath(".//text()").getall()))
                    else:
                        title = row_text.split('Download')[0].strip()

                    if not title or len(title) < 3:
                        continue

                    for href in download_links:
                        url = urljoin(response.url, href)

                        documents_found = True
                        self.logger.info(f"📄 Document found: {title}")
                        self.logger.info(f"   URL: {url}")

                        doc_path = ["SBP", "Laws & Regulations", section, parent_title, title]

                        reg_doc = RegulatoryDocument(
                            regulator="SBP",
                            source_system="SBP-LAW",
                            category=section,
                            title=title,
                            document_url=url,
                            urdu_url=None,
                            published_date=None,
                            reference_no=None,
                            department=[section, parent_title],
                            year=None,
                            source_page_url=response.url,
                            doc_path=doc_path,
                        )

                        self.shared_items.append(reg_doc)
                        self.logger.info(f"📦 Appended document: {title}")

                        yield reg_doc

        # Pattern 2: Look for direct PDF links (fallback)
        if not documents_found:
            for link in response.xpath("//a[@href]"):
                href = link.xpath("@href").get()
                title = clean(" ".join(link.xpath(".//text()").getall()))

                if not href or not title:
                    continue

                url = urljoin(response.url, href)

                # Only process document links
                if not url.lower().endswith(DOC_EXTS):
                    continue

                documents_found = True
                self.logger.info(f"📄 Document found (direct link): {title}")

                doc_path = ["SBP", "Laws & Regulations", section, parent_title, title]

                reg_doc = RegulatoryDocument(
                    regulator="SBP",
                    source_system="SBP-LAW",
                    category=section,
                    title=title,
                    document_url=url,
                    urdu_url=None,
                    published_date=None,
                    reference_no=None,
                    department=[section, parent_title],
                    year=None,
                    source_page_url=response.url,
                    doc_path=doc_path,
                )

                self.shared_items.append(reg_doc)
                self.logger.info(f"📦 Appended document: {title}")

                yield reg_doc

        if not documents_found:
            self.logger.warning(f"⚠️ No documents found on subpage: {parent_title}")
            self.logger.warning(f"⚠️ Page URL: {response.url}")

    def parse_fe_manual(self, response, section, parent_title, parent_url):
        """
        Parse Foreign Exchange Manual page with proper volume/appendix detection.
        """
        self.logger.info("📖 Parsing Foreign Exchange Manual")

        # Process each blockquote separately to maintain proper context
        for blockquote in response.xpath("//td[@valign='top'][@bgcolor='#F8F8F8']/blockquote"):
            current_volume = None
            in_appendices = False

            for element in blockquote.xpath("./*"):
                element_text = clean(element.xpath("string(.)").get())
                element_text_upper = element_text.upper()

                # Check for volume markers
                if element.xpath("self::p"):
                    strong_text = " ".join(element.xpath(".//strong//text()").getall()).upper()

                    if "VOLUME II" in strong_text or "VOLUME II" in element_text_upper:
                        current_volume = "Volume II"
                        in_appendices = False
                        self.logger.info(f"📚 Entered {current_volume}")
                        continue
                    elif "VOLUME I" in strong_text or (
                            "VOLUME I" in element_text_upper and "VOLUME II" not in element_text_upper):
                        current_volume = "Volume I"
                        in_appendices = False
                        self.logger.info(f"📚 Entered {current_volume}")
                        continue

                # Default to Volume I if no marker found yet
                if current_volume is None:
                    current_volume = "Volume I"

                # Process <ul> elements
                if element.xpath("self::ul"):
                    # FIRST: Check if this contains a nested table (Chapter table)
                    nested_tables = element.xpath(".//table")
                    if nested_tables:
                        for table in nested_tables:
                            headers = table.xpath(".//tr[1]//td//text()").getall()
                            headers_text = " ".join(clean(h) for h in headers).upper()

                            is_chapter_table = "CHAPTER" in headers_text and "SUBJECT" in headers_text

                            if is_chapter_table:
                                self.logger.info(f"📋 Processing chapter table ({current_volume})")

                                for row in table.xpath(".//tr[position()>1]"):
                                    cells = row.xpath("./td")
                                    if len(cells) < 2:
                                        continue

                                    chapter_num = clean(cells[0].xpath("string(.)").get())
                                    subject_cell = cells[1]

                                    for link in subject_cell.xpath(".//a[@href]"):
                                        href = link.xpath("@href").get()
                                        link_text = clean(link.xpath("string(.)").get())

                                        if not href or not link_text:
                                            continue

                                        url = urljoin(response.url, href)
                                        title = f"Chapter {chapter_num}: {link_text}" if chapter_num else link_text

                                        doc_path = [
                                            "SBP", "Laws & Regulations", section, parent_title,
                                            current_volume, title
                                        ]

                                        self.logger.info(f"📄 FE Manual chapter: {title}")

                                        reg_doc = RegulatoryDocument(
                                            regulator="SBP",
                                            source_system="SBP-LAW",
                                            category=section,
                                            title=title,
                                            document_url=url,
                                            urdu_url=None,
                                            published_date=None,
                                            reference_no=f"Chapter {chapter_num}" if chapter_num else None,
                                            department=[section, parent_title, current_volume],
                                            year=None,
                                            source_page_url=response.url,
                                            doc_path=doc_path,
                                            extra_meta={
                                                "chapter": chapter_num,
                                                "volume": current_volume,
                                                "type": "chapter"
                                            }
                                        )

                                        self.shared_items.append(reg_doc)
                                        yield reg_doc
                        continue  # Skip further processing of this <ul>

                    # SECOND: Check if this is "Appendices" section
                    appendix_label = clean(
                        element.xpath(".//li/font[normalize-space()='Appendices']/text()").get() or ""
                    )

                    if appendix_label.upper() == "APPENDICES":
                        self.logger.info(f"📂 Entered Appendices section ({current_volume})")

                        nested_tables = element.xpath(".//blockquote//table")
                        for table in nested_tables:
                            yield from self._process_appendix_table(
                                table, section, parent_title, current_volume, response
                            )
                        continue

                    # THIRD: Process as direct document links (Title, Disclaimer, Volume II links)
                    for li in element.xpath(".//li"):
                        for link in li.xpath(".//a[@href]"):
                            href = link.xpath("@href").get()
                            link_text = clean(link.xpath("string(.)").get())

                            if not href or not link_text:
                                continue

                            url = urljoin(response.url, href)

                            doc_path = [
                                "SBP", "Laws & Regulations", section, parent_title,
                                current_volume, link_text
                            ]
                            department = [section, parent_title, current_volume]

                            self.logger.info(f"📄 FE Manual document: {link_text}")
                            self.logger.info(f"   Location: {current_volume}")

                            reg_doc = RegulatoryDocument(
                                regulator="SBP",
                                source_system="SBP-LAW",
                                category=section,
                                title=link_text,
                                document_url=url,
                                urdu_url=None,
                                published_date=None,
                                reference_no=None,
                                department=department,
                                year=None,
                                source_page_url=response.url,
                                doc_path=doc_path,
                                extra_meta={
                                    "volume": current_volume
                                }
                            )

                            self.shared_items.append(reg_doc)
                            yield reg_doc
    def _process_appendix_table(self, table, section, parent_title, current_volume, response):
        """Helper method to process appendix tables"""
        # Get table headers
        headers = table.xpath(".//tr[1]//td//text()").getall()
        headers_text = " ".join(clean(h) for h in headers).upper()

        is_appendix_table = (
                ("SR. NO." in headers_text or "SR.NO." in headers_text)
                and "DESCRIPTION" in headers_text
        )

        if is_appendix_table:
            self.logger.info(f"📋 Processing appendix table ({current_volume})")

            for row in table.xpath(".//tr[position()>1]"):
                cells = row.xpath("./td")
                if len(cells) < 2:
                    continue

                sr_no = clean(cells[0].xpath("string(.)").get())
                description_cell = cells[1]

                for link in description_cell.xpath(".//a[@href]"):
                    href = link.xpath("@href").get()
                    link_text = clean(link.xpath("string(.)").get())

                    if not href:
                        continue

                    url = urljoin(response.url, href)
                    title = link_text

                    doc_path = [
                        "SBP", "Laws & Regulations", section, parent_title,
                        current_volume, "Appendices", title
                    ]

                    self.logger.info(f"📄 FE Manual appendix: {title}")
                    self.logger.info(f"   Location: {current_volume} / Appendices")

                    if url.lower().endswith(".htm") or url.lower().endswith(".html"):
                        # Follow appendix subpage (e.g. Appendix III)
                        yield scrapy.Request(
                            url,
                            callback=self.parse_subpage,
                            meta={
                                "section": section,
                                "parent_title": f"{parent_title} – Appendix {sr_no}",
                                "parent_url": response.url,
                            },
                        )
                    else:
                        reg_doc = RegulatoryDocument(
                            regulator="SBP",
                            source_system="SBP-LAW",
                            category=section,
                            title=title,
                            document_url=url,
                            urdu_url=None,
                            published_date=None,
                            reference_no=f"Appendix {sr_no}" if sr_no else None,
                            department=[section, parent_title, current_volume, "Appendices"],
                            year=None,
                            source_page_url=response.url,
                            doc_path=doc_path,
                            extra_meta={
                                "appendix": sr_no,
                                "volume": current_volume,
                                "type": "appendix"
                            }
                        )

                        self.shared_items.append(reg_doc)
                        yield reg_doc
