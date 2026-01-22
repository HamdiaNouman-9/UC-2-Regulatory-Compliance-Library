from twocaptcha import TwoCaptcha
import time

class CaptchaSolver:
    def __init__(self, api_key: str, retries: int = 3, backoff: float = 1.5):
        self.api_key = api_key
        self.retries = retries
        self.backoff = backoff

    def solve_captcha(self, site_key: str, page_url: str) -> str:
        solver = TwoCaptcha(self.api_key)
        for attempt in range(1, self.retries + 1):
            try:
                result = solver.recaptcha(sitekey=site_key, url=page_url)
                return result['code']
            except Exception as e:
                if attempt == self.retries:
                    raise
                time.sleep(self.backoff * attempt)

    def is_captcha_present(self, page):
        #Check if CAPTCHA is present on the page
        return page.locator("iframe[src*='recaptcha']").count() > 0

    def get_site_key(self, page):
        #Extract the CAPTCHA site key from the page
        return page.locator("iframe[src*='recaptcha']").get_attribute("src")
