import requests
from bs4 import BeautifulSoup

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
resp = session.get("https://rulebook.sama.gov.sa/en/node/5636", timeout=15)
soup = BeautifulSoup(resp.text, "html.parser")

# Check breadcrumb - shows SAMA's own path for this document
breadcrumb = soup.find("nav", class_="breadcrumb") or soup.find("ol", class_="breadcrumb")
if breadcrumb:
    crumbs = [a.get_text(strip=True) for a in breadcrumb.find_all("a")]
    print("SAMA breadcrumb path:", " > ".join(crumbs))

# Check active trail in sidebar - the menu-item--active-trail shows the true tree path
nav = soup.find("nav", id="book-block-menu-1365")
if nav:
    active = nav.find_all("li", class_=lambda c: c and "active-trail" in c)
    print("\nSidebar active-trail items (tree path):")
    for li in active:
        a = li.find("a")
        if a:
            print(" ", a.get_text(strip=True))
else:
    print("No sidebar nav found")

# Also check page title
title_tag = soup.find("title")
print("\nPage title:", title_tag.get_text(strip=True) if title_tag else "N/A")
