import os
import requests
from dotenv import load_dotenv

load_dotenv()

PDFCO_KEY = os.getenv("PDFCO_API_KEY")
PDFCO_UPLOAD_URL = "https://api.pdf.co/v1/file/upload"
PDFCO_CONVERT_URL = "https://api.pdf.co/v1/pdf/convert/to/html"


def pdfco_pdf_to_html(pdf_path: str) -> str:
    headers = {"x-api-key": PDFCO_KEY}

    # Upload PDF
    with open(pdf_path, "rb") as f:
        files = {"file": f}
        resp = requests.post(PDFCO_UPLOAD_URL, headers=headers, files=files)
    resp_json = resp.json()
    if resp_json.get("error"):
        raise RuntimeError(f"PDF.co upload failed: {resp_json.get('message')}")
    file_url = resp_json.get("url")

    # Convert to HTML
    params = {"url": file_url}
    resp2 = requests.post(PDFCO_CONVERT_URL, headers=headers, params=params)
    resp2_json = resp2.json()
    if resp2_json.get("error"):
        raise RuntimeError(f"PDF.co conversion failed: {resp2_json.get('message')}")

    html_url = resp2_json.get("url")
    html_resp = requests.get(html_url)
    html_resp.raise_for_status()

    return html_resp.text
