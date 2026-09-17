"""
agent_app.py  --  Streamlit UI to test the autonomous crawling agent yourself.

Run it with:

    streamlit run agent_app.py

Then use the browser page. You don't write any code:

  * CRAWL tab     -- pick a regulator + tab + URL, press Run. The agent inspects
                     the site, writes its own crawler, tests it, cross-checks it
                     against the live site, then does the full crawl. You watch
                     the live log and get a browsable results table + Excel.
  * FEEDBACK tab  -- type a plain-English correction ("you're missing the older
                     years") and the agent rewrites its crawler and re-runs.
  * RESULTS tab   -- reopen the latest results for any regulator, download Excel.

Nothing is ever written to your production database -- results are local files.
"""

import json
import logging
import sys
import threading
import time
from dataclasses import asdict
from pathlib import Path

import pandas as pd
import streamlit as st

# --- make the project importable + load API key from .env -------------------
REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except Exception:
    pass

from dynamic_crawler.auto import onboard, review_report  # noqa: E402

# Friendly model names -> actual OpenRouter model ids.
MODELS = {
    "Claude (best quality)": "anthropic/claude-sonnet-4.5",
    "DeepSeek (cheaper)": "deepseek/deepseek-v3.2",
}

# Columns worth showing first in the results table (rest follow).
PREFERRED_COLS = [
    "title", "category", "department", "year", "reference_no",
    "published_date", "file_type", "document_url", "source_page_url",
]


# --- live-log plumbing ------------------------------------------------------
class ListLogHandler(logging.Handler):
    """A logging handler that appends formatted lines to a shared list, so the
    background worker thread's progress can be streamed into the Streamlit UI."""

    def __init__(self, sink: list):
        super().__init__()
        self.sink = sink

    def emit(self, record):
        try:
            self.sink.append(self.format(record))
        except Exception:
            pass


def _start_job(target, kwargs, log_sink, result_box):
    """Run `target(**kwargs)` in a background thread, capturing all logging into
    log_sink and the return value / error into result_box."""
    handler = ListLogHandler(log_sink)
    handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s", "%H:%M:%S"))

    def _wrapped():
        root = logging.getLogger()
        prev_level = root.level
        root.setLevel(logging.INFO)
        root.addHandler(handler)
        try:
            result_box["state"] = target(**kwargs)
        except Exception as e:  # surface any crash to the UI instead of dying silently
            result_box["error"] = str(e)
            log_sink.append(f"ERROR: {e}")
        finally:
            result_box["done"] = True
            root.removeHandler(handler)
            root.setLevel(prev_level)

    t = threading.Thread(target=_wrapped, daemon=True)
    t.start()
    return t


# --- results loading --------------------------------------------------------
def _work_dir(regulator: str, model_id: str, tab: str = None) -> Path:
    return onboard._work_dir(regulator, model_id, tab)


def _load_docs(regulator: str, model_id: str, tab: str = None):
    d = _work_dir(regulator, model_id, tab)
    docs_path = d / "docs.json"
    report_path = d / "onboarding_report.json"
    if not docs_path.exists():
        return None, None, d
    docs = json.loads(docs_path.read_text(encoding="utf-8"))
    report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
    return docs, report, d


def _docs_to_df(docs: list) -> pd.DataFrame:
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    # Render the hierarchy path as a readable breadcrumb.
    if "doc_path" in df.columns:
        df["hierarchy"] = df["doc_path"].apply(
            lambda p: " › ".join(str(x) for x in p) if isinstance(p, list) else "")
    cols = [c for c in PREFERRED_COLS if c in df.columns]
    if "hierarchy" in df.columns:
        cols = ["hierarchy"] + cols
    rest = [c for c in df.columns if c not in cols and c not in ("document_html", "extra_meta")]
    return df[cols + rest]


def _ensure_excel(work_dir: Path, docs: list) -> Path | None:
    xlsx = work_dir / "report.xlsx"
    docs_json = work_dir / "docs.json"
    # Rebuild whenever the Excel is missing OR older than the current docs.json,
    # so the download always matches the data on screen (never a stale Excel from
    # a previous crawl of a different tab).
    fresh = (xlsx.exists() and docs_json.exists()
             and xlsx.stat().st_mtime >= docs_json.stat().st_mtime)
    if fresh:
        return xlsx
    try:
        review_report.build(str(work_dir))
    except SystemExit:
        return xlsx if xlsx.exists() else None
    except Exception:
        return xlsx if xlsx.exists() else None
    return xlsx if xlsx.exists() else None


def _render_results(regulator: str, model_id: str, ctx: str = "", tab: str = None):
    docs, report, work_dir = _load_docs(regulator, model_id, tab)
    if docs is None:
        st.info(f"No results yet for **{regulator}** with this model. "
                f"Run a crawl first.")
        return
    if not docs:
        st.warning("The last run produced **0 documents** — the agent could not "
                   "verify a working crawler. Try the Feedback tab to nudge it.")
        return

    cc = report.get("crosscheck", {})
    val = report.get("validation", {})
    shape = report.get("shape")
    shape_ev = report.get("shape_evidence", {}) or {}

    if shape:
        pretty = {"flat_table": "flat list/table", "sidebar_tree": "nested tree"}.get(shape, shape)
        note = ""
        if shape == "flat_table" and shape_ev.get("total_count"):
            note = f" — the page has {shape_ev['total_count']} rows, so expect ~that many documents"
        st.caption(f"Detected layout: **{pretty}**{note}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Documents", report.get("doc_count", len(docs)))
    c2.metric("Pages fetched", report.get("fetch_count", "?"))
    hit = cc.get("field_hit_rate")
    c3.metric("Field match", f"{hit:.0%}" if isinstance(hit, (int, float)) else "?")
    c4.metric("Self-check", "PASS ✅" if cc.get("pass") else "needs review ⚠️")

    if not cc.get("pass") and cc.get("reason"):
        st.warning(f"Cross-check note: {cc.get('reason')}")

    df = _docs_to_df(docs)
    st.dataframe(df, use_container_width=True, height=460,
                 column_config={
                     "document_url": st.column_config.LinkColumn("document_url"),
                     "source_page_url": st.column_config.LinkColumn("source_page_url"),
                 })

    xlsx = _ensure_excel(work_dir, docs)
    if xlsx:
        tab_slug = (tab or "").replace(" ", "_")
        st.download_button("⬇ Download hierarchy Excel (report.xlsx)",
                           data=xlsx.read_bytes(),
                           file_name=f"{regulator}_{tab_slug}_report.xlsx".replace("__", "_"),
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key=f"dl_xlsx_{ctx}_{regulator}_{tab_slug}")
    st.caption(f"Local results folder: `{work_dir}`")


def _render_live_log(log_sink: list, placeholder):
    text = "\n".join(log_sink[-400:]) if log_sink else "(waiting for the agent to start…)"
    placeholder.code(text, language="text")


# --- app --------------------------------------------------------------------
st.set_page_config(page_title="Regulatory Crawling Agent", page_icon="🕸️", layout="wide")

for key, default in (("job", None), ("log_sink", None), ("result_box", None),
                     ("last_regulator", "SBP"), ("last_tab", "Circulars"),
                     ("last_model_id", MODELS["Claude (best quality)"])):
    st.session_state.setdefault(key, default)

st.title("🕸️ Regulatory Crawling Agent")
st.caption("Give it a regulator tab and it writes, tests, and verifies its own crawler. "
           "Results come back as a browsable table + Excel. Your production database is never touched.")

with st.sidebar:
    st.header("Model")
    model_label = st.radio("Which model writes the crawler?", list(MODELS.keys()), index=0)
    model_id = MODELS[model_label]
    st.divider()
    st.markdown(
        "**How it works**\n\n"
        "1. Looks at the site\n"
        "2. Writes a crawler\n"
        "3. Tests it (small sample)\n"
        "4. Cross-checks vs the live site\n"
        "5. Refines if needed\n"
        "6. Full crawl → Excel\n\n"
        "If it can't verify itself, it tells you instead of handing over "
        "unverified data.")

job = st.session_state["job"]
job_running = bool(job and job.is_alive())

tab_crawl, tab_feedback, tab_results = st.tabs(["① Crawl", "② Feedback", "③ Results"])

# ---------------------------------------------------------------- CRAWL tab
with tab_crawl:
    st.subheader("Crawl a regulator tab")
    col1, col2 = st.columns(2)
    regulator = col1.text_input("Regulator (short name)", value="SBP", key="crawl_reg")
    tab_name = col2.text_input("Tab / section name", value="Circulars", key="crawl_tab")
    url = st.text_input("Landing page URL of that tab",
                        value="https://www.sbp.org.pk/circulars/cir.asp", key="crawl_url")
    c1, c2 = st.columns(2)
    quick = c1.toggle("Quick mode (fast small sample first)", value=True,
                      help="Recommended for a first look — a minute or two instead of a full crawl.")
    source_system = c2.text_input("Label stored on each doc (optional)", value="", key="crawl_src")

    with st.expander("Advanced: crawl size"):
        small = st.toggle(
            "This is a small / single-document section (e.g. one law)", value=False,
            help="Turn ON when crawling one law or a short section. It lowers the "
                 "'≥8 documents' completeness check that is meant for big multi-page trees "
                 "and would otherwise reject a small section.")
        min_docs = st.number_input(
            "Minimum documents to expect", min_value=1, max_value=500,
            value=1 if small else 8,
            help="The agent rejects a crawler that returns fewer than this on its test run. "
                 "Lower it for small sections; keep it high (8+) for big regulator trees.")

    disabled = job_running or not (regulator and tab_name and url)
    if st.button("▶ Run agent", type="primary", disabled=disabled):
        # Apply the completeness floor for THIS run (module global read inside onboard).
        onboard.MIN_TEST_DOCS = int(min_docs)
        st.session_state["log_sink"] = []
        st.session_state["result_box"] = {"done": False, "state": None, "error": None}
        st.session_state["last_regulator"] = regulator
        st.session_state["last_tab"] = tab_name
        st.session_state["last_model_id"] = model_id
        st.session_state["job"] = _start_job(
            onboard.onboard,
            dict(regulator=regulator, tab_name=tab_name,
                 source_system=source_system or f"{regulator} {tab_name}",
                 seed_url=url, model=model_id, full_run=not quick),
            st.session_state["log_sink"], st.session_state["result_box"])
        st.rerun()

    if job_running:
        st.info("Agent is working… watch the live log below. This can take several minutes.")
        _render_live_log(st.session_state["log_sink"], st.empty())
        time.sleep(1.5)
        st.rerun()
    elif job is not None and st.session_state["result_box"] and st.session_state["result_box"]["done"]:
        box = st.session_state["result_box"]
        if box["error"]:
            st.error(f"The run hit an error: {box['error']}")
        else:
            state = box["state"]
            if state is not None and not getattr(state, "accepted", True):
                st.warning("The agent tried several times but its self-checks kept failing — "
                           "it won't hand you data it can't verify. Use the **Feedback** tab to "
                           "describe what the site looks like / what to do, and it will try again.")
            else:
                st.success("Done. See results below (also on the **Results** tab).")
        with st.expander("Show full run log"):
            _render_live_log(st.session_state["log_sink"], st.empty())
        st.divider()
        _render_results(st.session_state["last_regulator"], st.session_state["last_model_id"],
                        ctx="crawl", tab=st.session_state["last_tab"])

# ------------------------------------------------------------- FEEDBACK tab
with tab_feedback:
    st.subheader("Correct the agent in plain English")
    st.caption("If the results look wrong, tell the agent what to fix. It rewrites its "
               "crawler using your note, re-tests, cross-checks, and re-crawls.")
    fb_reg = st.text_input("Regulator", value=st.session_state["last_regulator"], key="fb_reg")
    note = st.text_area("What should it fix?",
                        placeholder="e.g. you're missing the older years; open every year "
                                    "folder for each department",
                        key="fb_note", height=100)
    fb_url = st.text_input("A page the feedback is about (optional)", value="", key="fb_url")
    fb_tab = st.text_input("Tab name (must match the crawl you're correcting)",
                           value=st.session_state["last_tab"], key="fb_tab")

    disabled_fb = job_running or not (fb_reg and note.strip() and fb_tab.strip())
    if st.button("▶ Apply feedback & re-run", type="primary", disabled=disabled_fb):
        st.session_state["log_sink"] = []
        st.session_state["result_box"] = {"done": False, "state": None, "error": None}
        st.session_state["last_regulator"] = fb_reg
        st.session_state["last_tab"] = fb_tab
        st.session_state["last_model_id"] = model_id
        st.session_state["job"] = _start_job(
            onboard.refine_with_feedback,
            dict(regulator=fb_reg, feedback=note, model=model_id,
                 sample_url=fb_url or None, tab_name=fb_tab),
            st.session_state["log_sink"], st.session_state["result_box"])
        st.rerun()

    if job_running:
        st.info("Applying your feedback… watch the live log below.")
        _render_live_log(st.session_state["log_sink"], st.empty())
        time.sleep(1.5)
        st.rerun()
    elif job is not None and st.session_state["result_box"] and st.session_state["result_box"]["done"]:
        box = st.session_state["result_box"]
        if box["error"]:
            st.error(f"The run hit an error: {box['error']}")
        else:
            st.success("Re-run complete. See the updated results below.")
        with st.expander("Show full run log"):
            _render_live_log(st.session_state["log_sink"], st.empty())
        st.divider()
        _render_results(st.session_state["last_regulator"], st.session_state["last_model_id"],
                        ctx="feedback", tab=st.session_state["last_tab"])

# -------------------------------------------------------------- RESULTS tab
with tab_results:
    st.subheader("Latest results for a regulator tab")
    rc1, rc2 = st.columns(2)
    r_reg = rc1.text_input("Regulator", value=st.session_state["last_regulator"], key="res_reg")
    r_tab = rc2.text_input("Tab / section name", value=st.session_state["last_tab"], key="res_tab")
    st.caption(f"Showing results produced with: **{model_label}**")
    if st.button("🔄 Load latest results"):
        pass  # button just triggers a rerun; rendering happens below
    _render_results(r_reg, model_id, ctx="results", tab=r_tab)
