import os
import time
from io import BytesIO
from scrapy.http import Request
from PIL import Image
from twocaptcha import TwoCaptcha

class TwoCaptchaMiddleware:

    def __init__(self):
        self.api_key = os.getenv("TWOCAPTCHA_API_KEY")
        if not self.api_key:
            raise RuntimeError("2Captcha API key not found")

        self.solver = TwoCaptcha(self.api_key)

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_response(self, request, response, spider):
        """
        Detect captcha page and solve it
        """
        if self.is_captcha_page(response):
            spider.logger.warning("CAPTCHA detected — solving...")

            captcha_img_url = self.extract_captcha_image(response)
            formdata = self.extract_form_data(response)

            captcha_text = self.solve_captcha(captcha_img_url)

            if not captcha_text:
                spider.logger.error("Failed to solve captcha")
                return response

            formdata["captcha"] = captcha_text

            return request.replace(
                method="POST",
                body=None,
                dont_filter=True,
                meta=request.meta,
                callback=request.callback,
                headers=request.headers,
                formdata=formdata
            )

        return response

    def is_captcha_page(self, response):
        return (
            b"captcha" in response.body.lower()
            or response.xpath("//img[contains(@src,'captcha')]")
        )

    def extract_captcha_image(self, response):
        return response.urljoin(
            response.xpath("//img[contains(@src,'captcha')]/@src").get()
        )

    def extract_form_data(self, response):
        data = {}
        for inp in response.xpath("//form//input"):
            name = inp.xpath("@name").get()
            value = inp.xpath("@value").get(default="")
            if name:
                data[name] = value
        return data

    def solve_captcha(self, image_url):
        try:
            result = self.solver.normal(image_url)
            return result.get("code")
        except Exception as e:
            return None
