import requests
from bs4 import BeautifulSoup

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})

resp = session.get("https://rulebook.sama.gov.sa/en/rulebook", timeout=15)
soup = BeautifulSoup(resp.text, "html.parser")

print("=== SAMA Rulebook — top-level sector links ===\n")

# Try sidebar nav first
for nav_id in ["block-mainnavigation", "block-sama-main-menu", "nav-primary", "main-menu"]:
    nav = soup.find(id=nav_id) or soup.find(class_=nav_id)
    if nav:
        print(f"Found nav: {nav_id}")

# Find all links containing book-category
links = soup.find_all("a", href=True)
book_links = [a for a in links if "book-category" in a["href"]]
print("book-category links:")
for a in book_links:
    print(f"  {a['href']:55s}  {a.get_text(strip=True)}")

# Also try the sidebar/left menu
sidebar = soup.find("nav", class_=lambda c: c and "menu" in str(c).lower())
if sidebar:
    print("\nSidebar links:")
    for a in sidebar.find_all("a", href=True):
        text = a.get_text(strip=True)
        if text and a["href"].startswith("/en/"):
            print(f"  {a['href']:55s}  {text}")
