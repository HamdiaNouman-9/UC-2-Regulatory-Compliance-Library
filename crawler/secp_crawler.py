from typing import List
from playwright.sync_api import sync_playwright, Page
from twocaptcha import TwoCaptcha
from crawler.crawler import BaseCrawler
from models.models import RegulatoryDocument
import time
import re
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()


class SECPCrawler(BaseCrawler):

    BASE_URLS = {
        "Rules": "https://www.secp.gov.pk/laws/rules/",
        "Regulations": "https://www.secp.gov.pk/laws/regulations/",
        "Notifications": "https://www.secp.gov.pk/laws/notifications/",
        "Acts":       "https://www.secp.gov.pk/laws/acts/",
        "Ordinances": "https://www.secp.gov.pk/laws/ordinances/",
        "Directives": "https://www.secp.gov.pk/laws/directives/",
        "Guidelines": "https://www.secp.gov.pk/laws/guidelines/",
        "Circulars": "https://www.secp.gov.pk/laws/circulars/"
    }

    def __init__(self, api_key: str = None, headless: bool = True, retries: int = 3, backoff: float = 1.5):
        self.api_key = os.getenv("CAPTCHA_API_KEY")
        self.headless = headless
        self.retries = retries
        self.backoff = backoff

    def solve_captcha(self, site_key: str, page_url: str):
        solver = TwoCaptcha(self.api_key)
        try:
            result = solver.recaptcha(sitekey=site_key, url=page_url)
            return result["code"]
        except Exception as e:
            return None

    def is_captcha_present(self, page: Page) -> bool:
        return page.locator("iframe[src*='recaptcha']").count() > 0

    def get_site_key(self, page: Page):
        iframe_src = page.locator("iframe[src*='recaptcha']").get_attribute("src")
        if iframe_src:
            match = re.search(r"sitekey=([a-zA-Z0-9-_]+)", iframe_src)
            if match:
                return match.group(1)
        return None

    def _safe_goto(self, page: Page, url: str, label: str):
        for attempt in range(1, self.retries + 1):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=150000)

                if self.is_captcha_present(page):
                    site_key = self.get_site_key(page)
                    solution = self.solve_captcha(site_key, url)
                    if solution:
                        page.fill("input[name='g-recaptcha-response']", solution)
                        page.click("button[type='submit']")
                        time.sleep(2)
                return

            except Exception as e:
                time.sleep(self.backoff * attempt)

        raise RuntimeError(f"[SECP] FAILED to load page after {self.retries} attempts → {url}")

    def _crawl_section(self, page: Page, base_url: str, category: str) -> List[RegulatoryDocument]:
        documents: List[RegulatoryDocument] = []

        self._safe_goto(page, base_url, category)

        try:
            page.wait_for_selector("table tbody", timeout=30000)
        except Exception:
            return documents

        try:
            dropdown = page.locator("select")
            dropdown.select_option("-1")
            page.wait_for_timeout(1500)
        except Exception:
            pass

        rows = page.locator("table tbody tr")
        total = rows.count()

        for i in range(total):
            try:
                row = rows.nth(i)

                raw_date = row.locator("td:nth-child(1)").inner_text()

                published_date = None
                year = None

                if raw_date:
                    cleaned_date = (
                        raw_date
                        .replace("\u200e", "")
                        .replace("\xa0", "")
                        .strip()
                    )

                    match = re.search(r"(\d{2}/\d{2}/\d{4})", cleaned_date)
                    if match:
                        try:
                            dt = datetime.strptime(match.group(1), "%d/%m/%Y")
                            published_date = dt.date().isoformat()  # YYYY-MM-DD
                            year = dt.year
                        except ValueError:
                            pass
                    else:
                        pass

                title = row.locator("td:nth-child(2)").inner_text().strip()
                if not title:
                    continue

                download_a = row.locator("a:has-text('Download')")
                if download_a.count() == 0:
                    continue

                href = download_a.first.get_attribute("href")
                if not href:
                    continue

                if href.startswith("/"):
                    href = "https://www.secp.gov.pk" + href

                doc = RegulatoryDocument(
                    regulator="SECP",
                    source_system="SECP-Laws",
                    category=category,
                    title=title,
                    document_url=href,
                    urdu_url=None,
                    published_date=published_date,
                    reference_no=None,
                    department=None,
                    year=year,
                    source_page_url=base_url,
                    file_type=None,
                    extra_meta={
                        "download_url": href,
                        "table_row": i,
                        "raw_date": raw_date
                    },
                    doc_path=["SECP","Laws", category, title]
                )

                documents.append(doc)

            except Exception as e:
                pass

        return documents

    def get_documents(self) -> List[RegulatoryDocument]:
        all_docs: List[RegulatoryDocument] = []

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.headless)
            context = browser.new_context()

            for category, url in self.BASE_URLS.items():

                page = context.new_page()
                docs = self._crawl_section(page, url, category)


                all_docs.extend(docs)
                page.close()

            browser.close()

        return all_docs

    def fetch_documents(self, timeout=None):
        """
        Unified interface for Orchestrator
        """
        return self.get_documents()
