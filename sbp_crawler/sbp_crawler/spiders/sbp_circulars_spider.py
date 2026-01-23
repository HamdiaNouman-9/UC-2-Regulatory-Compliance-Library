import re
from urllib.parse import urljoin
import scrapy
from models.models import RegulatoryDocument


YEAR_RE = re.compile(r"(19|20)\d{2}")
DOC_EXTS = (".pdf", ".doc", ".docx", ".htm", ".html")


def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()


TRASH_TITLES = (
    "home", "about sbp", "library", "publications", "economic data",
    "press releases", "circulars/notifications", "laws", "legislation",
    "monetary policy", "help desk", "sbp videos", "feedback",
    "contact us", "rupey", "zahid husain", "events", "careers", "web links",
)


class SBPCircularsSpider(scrapy.Spider):
    name = "sbp_circulars"
    allowed_domains = ["sbp.org.pk", "dpc.org.pk"]
    start_urls = ["https://www.sbp.org.pk/circulars/cir.asp"]

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
        self.shared_items = shared_items
        self.logger.info("SBPCircularsSpider initialized")

    # SPECIAL HANDLING FOR BSD-2 - BANKING SUPERVISION DEPARTMENT II
    def parse_bsd2_special(self, response):
        path = response.meta["path"]
        self.logger.info(f"[BSD-2 SPECIAL] Path: {' / '.join(path)}")
        self.logger.debug(f"URL: {response.url}")

        year = None

        for row in response.xpath("//table//tr"):
            cells = row.xpath("./td")
            if len(cells) < 3:
                continue

            # Skip empty rows or rows that are footer/navigation
            first_cell_text = cells[0].xpath("string(.)").get()
            first_cell_text_clean = clean(first_cell_text)
            if not first_cell_text_clean:
                continue
            if any(t in first_cell_text_clean.lower() for t in [
                "home", "about sbp", "best view", "copyright", "contact", "library", "events"
            ]):
                continue

            year_text = row.xpath(".//td[@colspan and contains(text(),'20')]/text()").get()
            if year_text and year_text.strip().isdigit():
                year = year_text.strip()
                self.logger.info(f"BSD-2 Year detected: {year}")
                continue

            circular_no = first_cell_text
            date = clean(cells[1].xpath("string(.)").get())
            title_link = cells[2].xpath(".//a")
            title = clean(title_link.xpath("string(.)").get()) if title_link else clean(
                cells[2].xpath("string(.)").get())
            doc_link = title_link.xpath("./@href").get() if title_link else None
            document_url = response.urljoin(doc_link) if doc_link else None
            if not document_url:
                self.logger.debug(
                    f"[BSD-2] Skipping row without document URL: title={title}"
                )
                continue
            if title:
                self.logger.info(f"BSD-2 Document found: {title}")
                doc_path = ["SBP", "Circulars/Notifications", "Circulars"] + path + [year or "NA", "Circulars", title]

                doc = RegulatoryDocument(
                    regulator="SBP",
                    source_system="SBP-CIRCULAR",
                    category="Circular",
                    title=title,
                    document_url=document_url,
                    urdu_url=None,
                    published_date=date,
                    reference_no=circular_no,
                    department=path,
                    year=year,
                    source_page_url=response.url,
                    doc_path=doc_path,
                )

                if self.shared_items is not None:
                    self.shared_items.append(doc)
                    print(f"Appended document: {doc.title}")

                yield doc

    # ENTRY
    def parse(self, response):
        print(f"Spider {self.name} started, shared_items is {self.shared_items}")
        self.logger.info(f"Parsing main page: {response.url}")
        current_heading = None

        for row in response.xpath("//table//tr"):
            has_bullet = row.xpath(".//img[contains(@src,'square-bulit')]")
            strong_text = row.xpath(".//strong/text()").get()
            a_links = row.xpath(".//a[@href]")

            if strong_text and not has_bullet and not a_links:
                current_heading = clean(strong_text)
                self.logger.info(f"Heading detected: {current_heading}")
                continue

            if has_bullet and a_links:
                first_link = a_links[0]
                name = clean(" ".join(first_link.xpath(".//text()").getall()))
                href = first_link.xpath("@href").get()

                if not name or not href:
                    continue
                if any(t in name.lower() for t in TRASH_TITLES):
                    continue

                url = urljoin(response.url, href)
                if not url.lower().endswith(("index.htm", "index.asp", ".htm")):
                    continue

                path = [current_heading, name] if current_heading else [name]
                self.logger.info(f"Following department: {' / '.join(path)}")
                self.logger.debug(f"URL: {url}")

                yield scrapy.Request(
                    url,
                    callback=self.parse_department,
                    meta={"path": path},
                )

            if not has_bullet and not a_links and not strong_text:
                if row.xpath(".//td[@colspan='2']"):
                    current_heading = None

    # DEPARTMENT PAGE
    def parse_department(self, response):
        path = response.meta["path"]
        self.logger.info(f"[DEPT] {' / '.join(path)}")

        if "dpc.org.pk" in response.url:
            self.logger.info(f"Reached DPC department page: {response.url}")
            years = self._discover_years_dpc(response)
            for y in years:
                yield scrapy.Request(
                    y["url"],
                    callback=self.parse_dpc_year,
                    meta={
                        "path": path,
                        "year": y["year"],
                        "year_url": y["url"],
                    },
                )
            return

        if any("Banking Supervision Department-1" in p for p in path):
            for a in response.xpath("//a[@href]"):
                text = clean(" ".join(a.xpath(".//text()").getall())).lower()
                href = a.xpath("@href").get()
                if "offsite supervision" in text:
                    ose_path = path + ["Offsite Supervision & Enforcement Department"]
                    self.logger.info("[BSD-1] Entering Offsite Supervision & Enforcement Department")
                    yield scrapy.Request(
                        response.urljoin(href),
                        callback=self.parse_department,
                        meta={"path": ose_path},
                        dont_filter=True,
                    )

        # --- BSD-2: try normal year folders FIRST ---
        if "Banking Supervision Department-2" in path:
            years = self._discover_years(response)

            if years:
                self.logger.info("[BSD-2] Normal year folders detected")
                for y in years:
                    yield scrapy.Request(
                        y["url"],
                        callback=self.parse_year,
                        meta={
                            "path": path,
                            "year": y["year"],
                            "year_url": y["url"],
                        },
                    )
                return

            # --- fallback to special layout ---
            self.logger.info("[BSD-2] No year folders found, using special parser")
            for a in response.xpath("//a[contains(text(),'Banking Supervision Department - II')]"):
                href = a.xpath("@href").get()
                if href:
                    yield scrapy.Request(
                        response.urljoin(href),
                        callback=self.parse_bsd2_special,
                        meta={"path": path},
                        dont_filter=True,
                    )
            return

        years = self._discover_years(response)
        if years:
            self.logger.info(f"{len(years)} years found")
            for y in years:
                self.logger.info(f"Year {y['year']} → {y['url']}")
                yield scrapy.Request(
                    y["url"],
                    callback=self.parse_year,
                    meta={
                        "path": path,
                        "year": y["year"],
                        "year_url": y["url"],
                    },
                )
        else:
            self.logger.warning("No year folders found, using same page")
            yield scrapy.Request(
                response.url,
                callback=self.parse_year,
                meta={"path": path, "year": None, "year_url": response.url},
                dont_filter=True,
            )
    def _discover_years(self, response):
        out, seen = [], set()
        for a in response.xpath("//a[@href]"):
            text = clean(" ".join(a.xpath(".//text()").getall()))
            m = YEAR_RE.search(text)
            if not m:
                continue
            year = m.group(0)
            url = response.urljoin(a.xpath("@href").get())
            if (year, url) in seen:
                continue
            seen.add((year, url))
            out.append({"year": year, "url": url})
        return sorted(out, key=lambda x: int(x["year"]), reverse=True)

    # YEAR DISCOVERY
    def _discover_years_dpc(self, response):
        years = []
        for a in response.xpath("//a[@href]"):
            href = a.xpath("@href").get()
            if href and href.startswith("Cir-"):
                m = re.search(r'Cir-(\d{4})\.htm', href)
                if m:
                    year = m.group(1)
                    years.append({"year": year, "url": response.urljoin(href)})
                    self.logger.info(f"[DPC] Found DPC Year link: {year} -> {href}")
        return sorted(years, key=lambda x: int(x["year"]), reverse=True)

    def parse_dpc_year(self, response):
        path = response.meta["path"]
        year = response.meta["year"]

        for li in response.xpath("//li"):
            links = li.xpath(".//a[@href]")
            if not links:
                continue

            main_link = None
            annexures = []

            for a in links:
                title = clean(a.xpath("text()").get())
                href = a.xpath("@href").get()
                url = response.urljoin(href)

                title_lower = title.lower()

                if title_lower.startswith("annex"):
                    annexures.append({
                        "title": title,
                        "url": url
                    })
                else:
                    main_link = {
                        "title": title,
                        "url": url
                    }

            # skip if no main circular found
            if not main_link:
                continue

            # classify circular vs letter
            category = "Circular"
            if "letter" in main_link["title"].lower():
                category = "Circular Letter"

            doc_path = [
                "SBP",
                "Circulars/Notifications",
                "Circulars",
                *path,
                year,
                category,
                main_link["title"]
            ]

            reg_doc = RegulatoryDocument(
                regulator="SBP",
                source_system="DPC-CIRCULAR",
                category=category,
                title=main_link["title"],
                document_url=main_link["url"],
                urdu_url=None,
                published_date=None,
                reference_no=None,
                department=path,
                year=year,
                source_page_url=response.url,
                doc_path=doc_path,
                extra_meta={
                    "annexures": annexures
                }
            )

            if self.shared_items is not None:
                self.shared_items.append(reg_doc)

            yield reg_doc

    # SBP YEAR / TABLE PAGE
    def parse_year(self, response):
        path = response.meta["path"]
        base_year = response.meta["year"]
        year_url = response.meta["year_url"]
        self.logger.info(f"Parsing year page: {base_year}")
        self.logger.debug(f"URL: {response.url}")

        current_year = base_year
        documents = {}

        def is_real_annexure(title):
            """
            Only treat as annexure if the title is standalone, like:
            'Annexure A', 'Annexure 1', 'Annex I', etc.
            Titles like 'Amendment in Annexure-I of ...' are NOT annexures.
            """
            title_lower = title.lower().strip()
            if title_lower.startswith(("annexure", "annex")) and len(title_lower.split()) <= 3:
                return True
            return False

        for row in response.xpath("//table//tr"):
            row_text = clean(" ".join(row.xpath(".//text()").getall()))
            if any(t in row_text.lower() for t in TRASH_TITLES):
                continue
            if "copyright" in row_text.lower():
                continue

            cells = row.xpath("./td")
            year_match = YEAR_RE.search(row_text)
            if year_match and len(cells) == 1:
                current_year = year_match.group(0)
                continue
            if not current_year or len(cells) < 2:
                continue

            ref = clean(" ".join(cells[0].xpath(".//text()").getall()))
            date = clean(" ".join(cells[1].xpath(".//text()").getall()))

            for a in row.xpath(".//a[@href]"):
                link_text = clean(" ".join(a.xpath(".//text()").getall())).lower()
                href = a.xpath("@href").get()
                title = clean(" ".join(a.xpath(".//text()").getall()))
                url = response.urljoin(href)
                if not url.lower().endswith(DOC_EXTS):
                    continue

                is_annexure = is_real_annexure(title)

                is_urdu = (
                        "urdu" in link_text
                        or "(u)" in link_text
                        or "/urdu/" in url.lower()
                )
                key = (ref, date, current_year)

                if key not in documents:
                    documents[key] = {
                        "regulator": "SBP",
                        "source_system": "SBP-CIRCULAR",
                        "category": "Circular",
                        "path": path,
                        "path_str": " / ".join(path),
                        "year": current_year,
                        "reference_no": ref,
                        "published_date": date,
                        "urdu_url": None,
                        "title": title if not is_annexure else None,
                        "document_url": url if not is_annexure else None,
                        "source_page_url": year_url,
                        "annexures": []
                    }

                if is_annexure:
                    documents[key]["annexures"].append({"title": title, "url": url})

                if is_urdu:
                    documents[key]["urdu_url"] = url

                elif documents[key]["title"] is None:
                    documents[key]["title"] = title
                    documents[key]["document_url"] = url

        for doc in documents.values():
            if not doc.get("document_url"):
                self.logger.debug(
                    f"[YEAR] Skipping document without URL: title={doc.get('title')}"
                )
                continue
            if not doc["annexures"]:
                del doc["annexures"]

            # Determine category based on URL pattern
            category = "Circular"
            document_url = doc.get("document_url")
            if document_url:
                url_lower = document_url.lower()
                if '/cl' in url_lower and ('.htm' in url_lower or '.pdf' in url_lower):
                    if re.search(r'/cl[_-]?\d+\.', url_lower):
                        category = "Circular Letter"

            doc["category"] = category  # override the category
            doc_path = ["SBP", "Circulars/Notifications", "Circulars"] + doc["path"] + [doc["year"] or "NA", doc["category"],doc["title"]]
            reg_doc=RegulatoryDocument(
                regulator=doc["regulator"],
                source_system=doc["source_system"],
                category=doc["category"],
                title=doc["title"],
                document_url=doc["document_url"],
                urdu_url=doc.get("urdu_url"),
                published_date=doc.get("published_date"),
                reference_no=doc.get("reference_no"),
                department=doc.get("path"),
                year=doc.get("year"),
                source_page_url=doc.get("source_page_url"),
                doc_path=doc_path,
                extra_meta={"annexures": doc.get("annexures", []),"urdu_url": doc.get("urdu_url")}
            )
            if self.shared_items is not None:
                self.shared_items.append(reg_doc)
            yield reg_doc



