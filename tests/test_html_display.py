"""CHG-002: the display side must use the SAME allowlist as the cleaner, and
the vendored sanitiser must be the file that was reviewed.

  allowlist   web/html_allowlist.json equals what utils/html_clean.py would
              write now. If this fails, run `python -m utils.html_clean`
              and commit the result.
  vendor      web/vendor/purify.min.js has the SHA-256 recorded in
              web/vendor/README.md and announces the version named there.

    .venv/Scripts/python.exe -m pytest tests/test_html_display.py -v
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from utils.html_clean import ALLOWLIST_JSON, allowlist_as_dict

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "web" / "vendor"


def test_allowlist_json_matches_module():
    on_disk = json.loads((ROOT / ALLOWLIST_JSON).read_text(encoding="utf-8"))
    assert on_disk == allowlist_as_dict(), \
        "web/html_allowlist.json is stale: run `python -m utils.html_clean`"


def test_allowlist_js_embeds_the_same_json():
    js = (ROOT / "web" / "html_allowlist.js").read_text(encoding="utf-8")
    body = js.split("window.HTML_ALLOWLIST = ", 1)[1].rstrip().rstrip(";")
    assert json.loads(body) == allowlist_as_dict(), \
        "web/html_allowlist.js is stale: run `python -m utils.html_clean`"


def test_allowlist_json_has_what_the_browser_needs():
    d = json.loads((ROOT / ALLOWLIST_JSON).read_text(encoding="utf-8"))
    assert "a" in d["tags"] and "img" in d["tags"] and "table" in d["tags"]
    assert "script" not in d["tags"] and "iframe" not in d["tags"]
    assert "href" in d["attributes"]["a"] and "rel" not in d["attributes"]["a"]
    assert d["url_schemes"] == ["http", "https", "mailto", "tel"]
    assert all(p.startswith("data:image/") for p in d["data_image_prefixes"])
    assert "data:image/svg+xml" not in d["data_image_prefixes"]


def _readme_row():
    text = (VENDOR / "README.md").read_text(encoding="utf-8")
    row = next(l for l in text.splitlines() if l.startswith("| `purify.min.js`"))
    cells = [c.strip().strip("`") for c in row.strip("|").split("|")]
    return {"version": cells[2], "sha256": cells[-1]}


def test_vendored_dompurify_matches_readme():
    want = _readme_row()
    js = (VENDOR / "purify.min.js").read_bytes()
    assert hashlib.sha256(js).hexdigest() == want["sha256"]
    header = js[:200].decode("utf-8", errors="replace")
    assert f"DOMPurify {want['version']}" in header


def test_index_html_loads_vendored_dompurify_before_app():
    html = (ROOT / "web" / "index.html").read_text(encoding="utf-8")
    scripts = re.findall(r'<script src="([^"]+)"', html)
    assert any(s.startswith("vendor/purify.min.js") for s in scripts)
    assert scripts.index(next(s for s in scripts if s.startswith("vendor/purify"))) \
        < scripts.index(next(s for s in scripts if s.startswith("app.js")))
    assert "cdn" not in html.lower().split("purify")[0][-200:], "DOMPurify must not come from a CDN"
