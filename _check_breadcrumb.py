import requests
from bs4 import BeautifulSoup

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

for url, label in [
    ("https://rulebook.sama.gov.sa/en/node/5636", "Banking Sector doc (should skip)"),
    ("https://rulebook.sama.gov.sa/en/finance-companies-control-law", "Finance Sector doc (should keep)"),
]:
    resp = session.get(url, timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")
    print(f"\n=== {label} ===")
    print(f"URL: {url}")
    # Try various breadcrumb patterns
    for sel in ["nav.breadcrumb", "ol.breadcrumb", ".breadcrumb", "nav[aria-label*='readcrumb']", ".block-system-breadcrumb-block"]:
        el = soup.select_one(sel)
        if el:
            print(f"Found breadcrumb via '{sel}':")
            print(" ", el.get_text(separator=" > ", strip=True))
            links = [a.get_text(strip=True) for a in el.find_all("a")]
            print("  Links:", links)
            break
    else:
        print("No breadcrumb found via CSS selectors")
        # Try to find any nav with breadcrumb-like content
        for nav in soup.find_all(["nav", "ol", "ul"]):
            text = nav.get_text(separator=" ", strip=True)
            if "Rulebook" in text and len(text) < 300:
                print(f"  Possible breadcrumb ({nav.name}.{nav.get('class')}):", text[:200])
