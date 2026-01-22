import re
from urllib.parse import urljoin
import scrapy
from models.models import RegulatoryDocument
from typing import List
import json

def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


class SBPRegulatoryReturnsSpider(scrapy.Spider):

    def __init__(self, shared_items=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shared_items = shared_items
        self.seen_rows = set()

    name = "sbp_regulatory_returns"
    allowed_domains = ["sbp.org.pk"]
    start_urls = ["https://www.sbp.org.pk/Regulatory_Returns/index.asp"]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS": 1,
        "ROBOTSTXT_OBEY": True,
        "LOG_LEVEL": "INFO",
        "USER_AGENT": "SBP-Compliance-Monitor/1.0",
    }

    seen_rows = set()

    def extract_named_links(self, response, cell):
        """
        Extracts <a> tags as:
        [
          {"title": "Annex-I", "url": "https://..."},
          ...
        ]
        """
        links = []
        for a in cell.xpath(".//a"):
            title = clean(" ".join(a.xpath(".//text()").getall()))
            href = a.xpath("@href").get()

            if title and href:
                links.append({
                    "title": title,
                    "url": response.urljoin(href)
                })
        return links

    # ENTRY: Discover ONLY Regulatory Returns departments

    def parse(self, response):
        """
        Extract department links ONLY from the
        'Regulatory Returns' section table.
        """

        dept_links = response.xpath(
            "//td[contains(normalize-space(),'Regulatory Returns')]"
            "/ancestor::table[1]//a[@href]"
        )

        for a in dept_links:
            name = clean("".join(a.xpath(".//text()").getall()))
            href = a.xpath("@href").get()

            if not name or not href:
                continue

            dept_url = urljoin(
                "https://www.sbp.org.pk/Regulatory_Returns/",
                href,
            )

            yield scrapy.Request(
                dept_url,
                callback=self.parse_department,
                meta={"department": name},
                dont_filter=True,
            )


    def parse_department(self, response):
        dept = response.meta["department"]

        if "Regulatory_Returns" not in response.url:
            self.logger.warning("Skipped non-regulatory page: %s", response.url)
            return

        for row in response.xpath("//table//tr"):
            cells = row.xpath("./td")
            if len(cells) < 6:
                continue

            statement_cell = cells[1]
            statement = clean(" ".join(statement_cell.xpath(".//text()").getall()))

            ref_cell = cells[2]
            frequency = clean(" ".join(cells[3].xpath(".//text()").getall()))
            due_date = clean(" ".join(cells[4].xpath(".//text()").getall()))
            submission = clean(" ".join(cells[5].xpath(".//text()").getall()))

            if statement.lower() in {"", "name of statement/returns", "statement/returns"}:
                continue
            if frequency.lower() == "frequency":
                continue
            if due_date.lower() == "due date":
                continue

            statement_documents = self.extract_named_links(response, statement_cell)
            reference_documents = self.extract_named_links(response, ref_cell)

            ref_text = clean(" ".join(ref_cell.xpath(".//text()").getall()))

            main_doc = reference_documents[0] if reference_documents else None

            annexes = reference_documents[1:] if len(reference_documents) > 1 else []

            main_doc_str = json.dumps(main_doc, sort_keys=True) if main_doc else None

            dedupe_key = (dept, statement, ref_text, frequency, due_date, submission, main_doc_str)
            if dedupe_key in self.seen_rows:
                continue
            self.seen_rows.add(dedupe_key)

            doc_path = [
                "SBP",
                "Circulars/Notifications",
                "Regulatory Returns",
                dept,
                statement or "Untitled"
            ]

            doc=RegulatoryDocument(
                regulator="SBP",
                source_system="SBP-REGULATORY-RETURNS",
                category="Regulatory Returns",
                department=dept,
                year=None,
                reference_no=ref_text or None,
                published_date=None,
                title=statement,
                document_url=main_doc["url"] if main_doc else None,
                urdu_url=None,
                source_page_url=response.url,
                extra_meta={
                    "frequency": frequency or None,
                    "due_date": due_date or None,
                    "submission_mode": submission or None,

                    # Reference column (circular + annexes)
                    "main_circular": main_doc,
                    "reference_annexes": annexes or None,

                    # Statement column (IRAF, Annex I–IX, Excel, etc.)
                    "statement_documents": statement_documents or None
                },
                doc_path=doc_path
            )
            if self.shared_items is not None:
                self.shared_items.append(doc)
            yield doc


