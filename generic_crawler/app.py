"""
Streamlit test harness for the standalone sidebar crawler.

Run it with the VENV python (it has playwright + streamlit):
  venv/Scripts/python.exe -m streamlit run generic_crawler/app.py

Paste a URL (e.g. https://rulebook.sama.gov.sa/en/regulatory-sandbox), pick how
far to go, press Start. You watch it walk the tree live, then see the results
table and download the Excel. The crawl runs as a subprocess so Playwright never
fights Streamlit's event loop.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
CRAWLER = HERE / "crawler.py"
DEFAULT_OUT = PROJECT_ROOT / "output" / "standalone_crawler"

st.set_page_config(page_title="Sidebar Crawler (test)", layout="wide")
st.title(" Sidebar Crawler — test harness")
st.caption("Standalone Playwright crawler. Walks a nested sidebar, grabs HTML content + PDF links per page. "
           "Fully separate from the live pipeline.")

with st.sidebar:
    st.header("Settings")
    url = st.text_input("Seed URL", "https://rulebook.sama.gov.sa/en/regulatory-sandbox")
    run_name = st.text_input("Run name (folder)", "sama_sandbox")
    max_pages = st.number_input("Max pages", min_value=5, max_value=20000, value=100, step=50,
                                help="For a full listing site set this high, e.g. SBP has ~3,989 "
                                     "circulars → use ~4,300+. Big runs take hours (slow servers).")
    max_depth = st.slider("Max depth (folder levels)", 1, 12, 8,
                          help="How many levels of real content pages to follow. Pagination/Next "
                               "links do NOT count toward this, so long paginated lists still finish.")
    scope = st.selectbox(
        "Scope",
        ["breadcrumb", "prefix", "host"],
        help="breadcrumb = stay inside this section using the breadcrumb trail (best for flat-URL sites like SAMA). "
             "prefix = only URLs under the seed's path (best for list sites like SBP /circulars). "
             "host = anything on the same domain.",
    )
    headful = st.checkbox("Show browser window (headful)", value=False)
    start = st.button(" Start crawl", type="primary")

out_dir = DEFAULT_OUT / run_name


def run_crawl():
    py = sys.executable  # whichever python launched streamlit (should be the venv)
    cmd = [py, str(CRAWLER), "--url", url, "--out", str(out_dir),
           "--max-pages", str(max_pages), "--max-depth", str(max_depth), "--scope", scope]
    if headful:
        cmd.append("--headful")

    log_box = st.empty()
    status_line = st.empty()
    logs = []
    recorded = 0

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, encoding="utf-8", bufsize=1, cwd=str(PROJECT_ROOT))
    for raw in proc.stdout:
        raw = raw.strip()
        if not raw:
            continue
        try:
            ev = json.loads(raw)
        except json.JSONDecodeError:
            logs.append(raw)
            log_box.code("\n".join(logs[-25:]))
            continue

        e = ev.get("event")
        if e == "visit":
            recorded = ev.get("recorded", recorded)
            logs.append(f" [{ev['depth']}] {ev.get('title','')[:70]}  "
                        f"(pdfs:{ev.get('n_pdfs',0)}, chars:{ev.get('text_len',0)})")
            status_line.info(f"Recorded **{recorded}** pages · queue {ev.get('queued','?')}")
        elif e == "skip":
            logs.append(f"  skip/{ev.get('reason','')}: {ev['url']}")
        elif e == "error":
            logs.append(f" error: {ev['url']} — {ev.get('message','')}")
        elif e == "start":
            logs.append(f" seed={ev['seed']} scope={ev['scope']}")
        elif e == "anchor":
            logs.append(f" section anchor = '{ev['section_anchor']}'")
        elif e == "blocked":
            logs.append(f" BLOCKED: {ev['url']} — {ev.get('reason','')}")
        elif e == "cap":
            logs.append(f"  page cap hit at {ev['pages']} pages, "
                        f"{ev.get('queued', 0)} URLs left unwalked")
        elif e == "done":
            # "done" no longer implies success — say which outcome it was.
            outcome["status"] = ev.get("status", "ok")
            outcome["stopped"] = ev.get("stopped", "")
            logs.append(f" {outcome['status'].upper()} — {ev['pages']} pages, "
                        f"{ev['documents']} documents, "
                        f"{ev.get('blocked_pages', 0)} blocked")
        log_box.code("\n".join(logs[-25:]))

    proc.wait()
    return proc.returncode


# Filled in from the `done` event so the banner below reports the run's OUTCOME
# rather than whether the process managed to exit.
outcome = {"status": "", "stopped": ""}

if start:
    with st.spinner("Crawling…"):
        rc = run_crawl()
    status = outcome["status"]
    if status == "ok":
        st.success("Crawl finished — OK.")
    elif status == "incomplete":
        # Not an error: the rows are worth keeping. But it is not coverage, and
        # the reason has to be in front of whoever reads the numbers below.
        st.warning(f"INCOMPLETE — part of the site was not walked. "
                   f"{outcome['stopped']} The counts below are a floor, not a total.")
    elif status in ("blocked", "zero"):
        st.error(f"{status.upper()} — this run cannot be used. "
                 f"{outcome['stopped'] or 'See the log above.'}")
    else:
        st.error(f"Crawler exited with code {rc} and reported no outcome. "
                 "See the log above.")

# ---- show results if present ----
xlsx = out_dir / "pages.xlsx"
pages_json = out_dir / "pages.json"
if xlsx.exists():
    st.subheader("Results")
    data = json.loads(pages_json.read_text(encoding="utf-8")) if pages_json.exists() else {}
    pages = data.get("pages", [])
    docs = data.get("documents", [])

    c1, c2 = st.columns(2)
    c1.metric("Pages", len(pages))
    c2.metric("Documents (PDF/DOCX/…)", len(docs))

    tab1, tab2 = st.tabs(["Pages", "Documents"])
    with tab1:
        if pages:
            df = pd.DataFrame([{k: p[k] for k in
                                ("section_path", "title", "url", "depth", "status",
                                 "n_pdfs", "text_len", "html_file")} for p in pages])
            st.dataframe(df, width="stretch", height=380)
            pick = st.selectbox("Preview a page's content", options=list(range(len(pages))),
                                format_func=lambda i: pages[i]["title"] or pages[i]["url"])
            st.text_area("Extracted text", pages[pick]["text"][:8000], height=250)
    with tab2:
        if docs:
            st.dataframe(pd.DataFrame(docs), width="stretch", height=380)
        else:
            st.info("No document links found.")

    st.download_button("Download Excel", data=xlsx.read_bytes(),
                       file_name=f"{run_name}_pages.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.caption(f"Full HTML per page saved under: {out_dir / 'html'}")
