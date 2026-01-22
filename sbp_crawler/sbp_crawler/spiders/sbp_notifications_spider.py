import re
from urllib.parse import urljoin
import scrapy
from models.models import RegulatoryDocument

YEAR_RE = re.compile(r"(20\d{2})")
DATE_RE = re.compile(
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}",
    re.I,
)
DOC_EXTS = (".pdf", ".doc", ".docx", ".htm", ".html")


def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


class SBPNotificationsSpider(scrapy.Spider):
    name = "sbp_notifications"
    allowed_domains = ["sbp.org.pk"]
    start_urls = ["https://www.sbp.org.pk/circulars/notifications.asp"]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS": 1,
        "ROBOTSTXT_OBEY": True,
        "LOG_LEVEL": "INFO",
        "USER_AGENT": "SBP-Compliance-Monitor/1.0",
        "DOWNLOAD_MAXSIZE": 25 * 1024 * 1024,
    }

    STRICT_YEAR_FROM = 2020
    BLOCK_DEPTS = {"home", "library", "about sbp", "careers", "events", "contact"}

    def __init__(self, shared_items=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shared_items = shared_items

    def parse(self, response):
        print(f"Spider {self.name} started, shared_items is {self.shared_items}")
        for a in response.xpath("//table//a[@href]"):
            name = clean("".join(a.xpath(".//text()").getall()))
            href = a.xpath("@href").get()

            if not name or not href:
                continue
            if any(b in name.lower() for b in self.BLOCK_DEPTS):
                continue
            url = urljoin(response.url, href)
            if not url.endswith(("index.htm", "index.asp")):
                continue

            yield scrapy.Request(
                url,
                callback=self.parse_department,
                meta={"department": name},
            )

    def parse_department(self, response):
        dept = response.meta["department"]
        years = self._discover_years(response)

        if years:
            for y in years:
                yield scrapy.Request(
                    y["url"],
                    callback=self.parse_year,
                    meta={
                        "department": dept,
                        "year": y["year"],
                        "year_url": y["url"],
                    },
                )
        else:
            year = self._infer_year(response)
            yield scrapy.Request(
                response.url,
                callback=self.parse_year,
                meta={
                    "department": dept,
                    "year": year,
                    "year_url": response.url,
                },
                dont_filter=True,
            )

    def _discover_years(self, response):
        out, seen = [], set()
        for a in response.xpath("//a[@href]"):
            text = clean("".join(a.xpath(".//text()").getall()))
            m = YEAR_RE.search(text)
            if not m:
                continue
            year = m.group(1)
            url = urljoin(response.url, a.xpath("@href").get())
            if (year, url) in seen:
                continue
            seen.add((year, url))
            out.append({"year": year, "url": url})
        return sorted(out, key=lambda x: int(x["year"]), reverse=True)

    def _infer_year(self, response):
        text = " ".join(response.xpath("//text()").getall())
        m = YEAR_RE.search(text)
        return m.group(1) if m else None

    def parse_year(self, response):
        dept = response.meta["department"]
        year = response.meta["year"]
        year_url = response.meta["year_url"]

        strict = year and int(year) >= self.STRICT_YEAR_FROM

        for doc in self._extract_from_tables(response, dept, year, year_url):
            if self._valid(doc.__dict__, strict):
                if self.shared_items is not None:
                    self.shared_items.append(doc)
                    print(f"Appended document: {doc.title}")
                yield doc

    def _extract_from_tables(self, response, dept, year, year_url):
        for row in response.xpath("//table//tr"):
            cells = row.xpath("./td")
            if len(cells) < 3:
                continue

            ref = clean(" ".join(cells[0].xpath(".//text()").getall()))
            date = clean(" ".join(cells[1].xpath(".//text()").getall()))
            a = row.xpath(".//a[@href]")
            if not a:
                continue

            href = a.xpath("@href").get()
            url = urljoin(response.url, href)
            if not url.lower().endswith(DOC_EXTS):
                continue

            if not date:
                m = DATE_RE.search(" ".join(row.xpath(".//text()").getall()))
                if m:
                    date = m.group(0)

            doc_path = [
                "SBP",
                "Circulars/Notifications",
                "Notifications",
                dept,
                year or "NA",
                ref or "Untitled",
            ]

            yield RegulatoryDocument(
                regulator="SBP",
                source_system="SBP-NOTIFICATION",
                category="Notification",
                department=dept,
                year=year,
                reference_no=ref,
                published_date=date,
                title=ref,
                document_url=url,
                urdu_url=None,
                source_page_url=year_url,
                doc_path=doc_path,
            )

    def _valid(self, d, strict):
        required = ["reference_no", "published_date", "department", "year", "document_url"]
        return all(d.get(k) for k in required)
