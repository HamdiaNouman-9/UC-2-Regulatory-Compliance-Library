"""
SAMA Circulars -- Crawl & Pipeline Preview (Streamlit)

Two phases, in order:
  1. Crawl: opens a VISIBLE Chrome window (headful) and crawls
     https://rulebook.sama.gov.sa/en/sama-circulars, comparing against the
     regulations DB the same way run_sama_circulars_headless.py does --
     only new circulars / ones whose issue date changed come back.
  2. Pipeline preview: on request, runs each of those circulars through the
     same extraction -> LLM analysis -> requirement matching logic the
     production orchestrator uses, WITHOUT writing anything to the DB --
     existing requirements/controls/KPIs are only read (for matching), never
     inserted or updated, and no regulation/analysis rows are created. Results
     are shown live as requirement/obligation/control cards (styled after the
     production app's detail view) and exported to an Excel workbook.

Phase 2 still calls the LLM API per circular (has a real cost/time), it just
doesn't persist anything to the database. Theme is pinned to light in
.streamlit/config.toml.

Run:
    streamlit run sama_pipeline_ui.py
"""
import html
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from crawler.sama_circulars_crawler import SAMARulebookCrawler  # noqa: E402
from storage.mssql_repo import MSSQLRepository  # noqa: E402
from processor.downloader import Downloader  # noqa: E402
from orchestrator.orchestrator import Orchestrator, MIN_TEXT_LEN  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv()

REGULATOR = "SAMA"
CATEGORY = "SAMA Circulars"
OUTDIR = PROJECT_ROOT / "output" / "standalone_crawler" / "sama_circulars_only"
STATUS_ICON = {"SUCCESS": "✅", "ERROR": "❌", "PENDING": "⏳"}

st.set_page_config(page_title="SAMA Circulars Pipeline", layout="wide")

# ==================================================================== #
#  STYLE -- badges / cards modeled on the production detail view        #
# ==================================================================== #
st.markdown("""
<style>
.badge { display:inline-block; padding:2px 9px; border-radius:4px; font-size:10.5px;
         font-weight:700; margin-right:6px; letter-spacing:.3px; text-transform:uppercase; }
.badge-new       { background:#fef3c7; color:#92400e; }
.badge-crit-high { background:#fee2e2; color:#991b1b; }
.badge-crit-medium{background:#fce7f3; color:#9d174d; }
.badge-crit-low  { background:#dcfce7; color:#166534; }
.badge-type      { background:#f3e8ff; color:#6b21a8; }
.badge-exec      { background:#f1f5f9; color:#334155; }
.badge-count     { background:#ccfbf1; color:#0f766e; }
.badge-evidence  { background:#e0f2fe; color:#075985; margin-top:4px; }
.pill-status     { border:1px solid #0d9488; color:#0d9488; border-radius:12px;
                    padding:2px 12px; font-size:11px; font-weight:700; float:right; }

.req-card   { border:1px solid #e2e8f0; border-radius:8px; padding:16px 18px;
              margin-bottom:14px; background:#ffffff; }
.req-id     { font-size:10.5px; color:#64748b; font-weight:700; letter-spacing:.5px; }
.req-title  { font-size:15px; font-weight:700; color:#0f172a; margin:2px 0 8px 0; }

.obl-card   { border:1px solid #e2e8f0; border-radius:8px; padding:14px 16px;
              margin:10px 0; background:#ffffff; }
.obl-num    { display:inline-flex; align-items:center; justify-content:center; width:20px;
              height:20px; border-radius:50%; background:#0d9488; color:#fff;
              font-size:11px; font-weight:700; margin-right:8px; flex:none; }
.obl-text   { font-weight:600; color:#0f172a; font-size:13.5px; }

.field-grid { display:grid; grid-template-columns:repeat(3, 1fr); gap:16px; margin-top:12px; }
.field-label{ font-size:9.5px; color:#94a3b8; font-weight:700; letter-spacing:.5px;
              text-transform:uppercase; margin-bottom:3px; }
.field-value{ font-size:13px; color:#1e293b; }

.ctrl-card  { border:1px solid #99f6e4; background:#f0fdfa; border-radius:8px;
              padding:14px 16px; margin-top:12px; }
.ctrl-title { color:#0f766e; font-weight:700; font-size:12px; letter-spacing:.3px;
              text-transform:uppercase; }
.ctrl-sub   { color:#64748b; font-size:11px; margin-top:1px; }
.ctrl-grid  { display:grid; grid-template-columns:1fr 1fr; gap:14px; margin-top:10px; }
.ctrl-steps { margin:4px 0 0 0; padding-left:18px; }
.ctrl-steps li { font-size:13px; color:#1e293b; margin-bottom:4px; }

.reg-card   { border:1px solid #e2e8f0; border-radius:8px; padding:20px 22px; background:#fff; }
.reg-title  { font-size:18px; font-weight:700; color:#0f766e; margin:0; }
.reg-crumb  { font-size:12px; color:#64748b; margin-top:2px; }
.reg-ref    { font-size:12px; color:#475569; margin:14px 0 6px 0; }
.reg-desc   { max-height:340px; overflow-y:auto; border:1px solid #e2e8f0; border-radius:6px;
              padding:14px 16px; font-size:13px; line-height:1.65; background:#fafafa; color:#1e293b; }
.reg-footer { display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; margin-top:16px;
              border-top:1px solid #e2e8f0; padding-top:14px; }
.reg-footer .field-label { margin-bottom:5px; }
.reg-footer a { color:#0d9488; text-decoration:none; font-size:13px; }
</style>
""", unsafe_allow_html=True)

st.title("SAMA Circulars -- Crawl & Pipeline Preview")

if "crawled_docs" not in st.session_state:
    st.session_state.crawled_docs = None
if "pipeline_results" not in st.session_state:
    st.session_state.pipeline_results = None


def get_repo() -> MSSQLRepository:
    conn_params = {
        "server": os.getenv("MSSQL_SERVER"),
        "database": os.getenv("MSSQL_DATABASE"),
        "username": os.getenv("MSSQL_USERNAME"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 17 for SQL Server}"),
    }
    return MSSQLRepository(conn_params)


def get_known_circulars(repo: MSSQLRepository) -> list:
    """[{"title", "published_date"}, ...] already stored for SAMA Circulars.

    Matched by issue date (title as a same-day tiebreaker) inside
    SAMARulebookCrawler.fetch_documents -- see crawler/sama_circulars_crawler.py.
    """
    try:
        with repo._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT title, published_date FROM regulations "
                "WHERE regulator = ? AND category = ? AND published_date IS NOT NULL",
                [REGULATOR, CATEGORY],
            )
            return [{"title": row[0], "published_date": row[1]} for row in cursor.fetchall()]
    except Exception as e:
        st.warning(f"Could not read DB baseline ({e}). Treating every circular as new.")
        return []


# ==================================================================== #
#  PIPELINE (no DB writes) -- extraction -> LLM analysis -> matching     #
# ==================================================================== #

def run_pipeline_dry_run(doc, repo: MSSQLRepository, orchestrator: Orchestrator):
    """Extract + LLM-analyze + match one circular WITHOUT writing to the DB.

    Returns (summary_row, requirement_rows, match_rows, requirement_details).
    requirement_details carries the full obligation/control structure used by
    the card rendering below; the first three stay flat for the Excel export.
    Existing requirements/controls/KPIs are read (needed to classify matches
    as fully/partially/new) but nothing is inserted or updated.
    """
    row = {
        "Circular No": doc.reference_no,
        "Title": doc.title,
        "Issue Date": doc.published_date,
        "Extraction": "PENDING",
        "Extracted Chars": 0,
        "LLM Analysis": "PENDING",
        "Requirements Extracted": 0,
        "Matching": "PENDING",
        "Fully Matched": 0,
        "Partially Matched": 0,
        "New Requirements": 0,
        "Error": "",
    }
    requirement_rows, match_rows, requirement_details = [], [], []

    try:
        text_content, content_type = orchestrator.extract_text_content_unified(doc, regulation_id=None)
        if not text_content or len(text_content) < MIN_TEXT_LEN:
            row["Extraction"] = "ERROR"
            row["Error"] = f"Insufficient text ({len(text_content or '')} chars)"
            return row, requirement_rows, match_rows, requirement_details
        row["Extraction"] = "SUCCESS"
        row["Extracted Chars"] = len(text_content)
    except Exception as e:
        row["Extraction"] = "ERROR"
        row["Error"] = str(e)
        return row, requirement_rows, match_rows, requirement_details

    try:
        clean_text = orchestrator.llm_analyzer.normalize_input_text(text_content, content_type=content_type)
        analysis_rows = orchestrator.staged_analyzer.analyze(
            text=clean_text, regulation_id=0, document_title=doc.title
        )
        if not analysis_rows:
            row["LLM Analysis"] = "ERROR"
            row["Error"] = "4-stage analysis returned no requirements"
            return row, requirement_rows, match_rows, requirement_details
        row["LLM Analysis"] = "SUCCESS"
        row["Requirements Extracted"] = len(analysis_rows)
        for r in analysis_rows:
            requirement_rows.append({
                "Circular No": doc.reference_no,
                "Requirement ID": r.get("requirement_id"),
                "Requirement Title": r.get("requirement_title"),
                "Execution Category": r.get("execution_category"),
                "Criticality": r.get("criticality"),
                "Obligation Type": r.get("obligation_type"),
            })
    except Exception as e:
        row["LLM Analysis"] = "ERROR"
        row["Error"] = str(e)
        return row, requirement_rows, match_rows, requirement_details

    # ---- requirement matching (read-only) + assemble card data ----
    try:
        extracted_for_matcher = []
        for r in analysis_rows:
            s2 = r.get("stage2_json") or {}
            if isinstance(s2, str):
                s2 = json.loads(s2)
            for ob in s2.get("normalized_obligations", []):
                extracted_for_matcher.append({
                    "requirement_text": ob["obligation_text"],
                    "department": "",
                    "risk_level": ob.get("criticality", "Medium"),
                    "controls": [],
                    "kpis": [],
                    "_obligation_id": ob["obligation_id"],
                    "_requirement_id": r.get("requirement_id"),
                })

        match_results = orchestrator.requirement_matcher.match_requirements(
            regulation_id=0,
            extracted_requirements=extracted_for_matcher,
            existing_requirements=repo.get_all_compliance_requirements(),
            existing_controls=repo.get_all_demo_controls(),
            existing_kpis=repo.get_all_demo_kpis(),
            linked_controls_by_req=repo.get_linked_controls_by_requirement(),
            linked_kpis_by_req=repo.get_linked_kpis_by_requirement(),
        )
        mappings = match_results["requirement_mappings"]
        row["Matching"] = "SUCCESS"
        row["Fully Matched"] = sum(1 for m in mappings if m["match_status"] == "fully_matched")
        row["Partially Matched"] = sum(1 for m in mappings if m["match_status"] == "partially_matched")
        row["New Requirements"] = sum(1 for m in mappings if m["match_status"] == "new")
        for m in mappings:
            match_rows.append({
                "Circular No": doc.reference_no,
                "Extracted Requirement": m["extracted_requirement_text"],
                "Match Status": m["match_status"],
                "Matched Requirement ID": m.get("matched_requirement_id"),
                "Explanation": m.get("match_explanation"),
            })
        match_by_text = {m["extracted_requirement_text"]: m for m in mappings}
    except Exception as e:
        row["Matching"] = "ERROR"
        row["Error"] = str(e)
        match_by_text = {}

    # ---- assemble per-requirement obligation+control detail for the cards ----
    for r in analysis_rows:
        s2 = r.get("stage2_json") or {}
        s3 = r.get("stage3_json") or {}
        if isinstance(s2, str):
            s2 = json.loads(s2) if s2 else {}
        if isinstance(s3, str):
            s3 = json.loads(s3) if s3 else {}

        controls_by_obligation_id = {
            ob.get("obligation_id"): ob.get("control")
            for ob in s3.get("obligations", [])
            if ob.get("control")
        }

        obligations = []
        for ob in s2.get("normalized_obligations", []):
            match = match_by_text.get(ob.get("obligation_text"), {})
            obligations.append({
                **ob,
                "control": controls_by_obligation_id.get(ob.get("obligation_id")),
                "match_status": match.get("match_status", "new"),
                "match_explanation": match.get("match_explanation", ""),
            })

        requirement_details.append({
            "requirement_id": r.get("requirement_id"),
            "requirement_title": r.get("requirement_title"),
            "criticality": r.get("criticality"),
            "execution_category": r.get("execution_category"),
            "obligation_type": r.get("obligation_type"),
            "obligations": obligations,
        })

    return row, requirement_rows, match_rows, requirement_details


# ==================================================================== #
#  RENDERING -- badges / obligation cards / control cards / rule view   #
# ==================================================================== #

def _esc(s) -> str:
    return html.escape(str(s if s is not None else ""))


def _badge(text: str, css_class: str) -> str:
    return f'<span class="badge {css_class}">{_esc(text)}</span>'


def _crit_badge(criticality: str) -> str:
    cls = {"High": "badge-crit-high", "Medium": "badge-crit-medium", "Low": "badge-crit-low"}.get(criticality, "badge-crit-medium")
    return _badge(criticality or "Medium", cls)


def render_obligation_card(ob: dict, idx: int):
    control = ob.get("control")
    badges = (
        _badge(ob.get("match_status", "new"), "badge-new")
        + _crit_badge(ob.get("criticality"))
        + (_badge((control or {}).get("control_type", ""), "badge-type") if control else _badge(ob.get("obligation_type", ""), "badge-type"))
        + _badge((ob.get("execution_category") or "").replace("_", " "), "badge-exec")
    )

    evidence = ob.get("evidence_expected") or []
    evidence_html = "".join(_badge(e, "badge-evidence") for e in evidence) if evidence else '<span class="field-value">--</span>'

    parts = [
        '<div class="obl-card">',
        f'<div><span class="obl-num">{idx}</span><span class="obl-text">{_esc(ob.get("obligation_text"))}</span></div>',
        f'<div style="margin-top:8px;">{badges}</div>',
        '<div class="field-grid">',
        f'<div><div class="field-label">Test Method</div><div class="field-value">{_esc(ob.get("test_method") or "--")}</div></div>',
        f'<div><div class="field-label">Source Reference</div><div class="field-value">{_esc(ob.get("source_reference") or "--")}</div></div>',
        f'<div><div class="field-label">Clarity Score</div><div class="field-value">{_esc(ob.get("clarity_score", "--"))} / 5</div></div>',
        '</div>',
        f'<div style="margin-top:10px;"><div class="field-label">Evidence Expected</div>{evidence_html}</div>',
    ]

    if control:
        subtitle = " • ".join(filter(None, [control.get("control_type"), control.get("execution_type"), control.get("frequency")]))
        steps = "".join(f"<li>{_esc(s)}</li>" for s in (control.get("key_steps") or []))
        parts.append(
            '<div class="ctrl-card">'
            '<div style="display:flex; justify-content:space-between; align-items:flex-start;">'
            f'<div><div class="ctrl-title">🛡️ {_esc(control.get("control_title") or "Control")}</div>'
            f'<div class="ctrl-sub">{_esc(subtitle)}</div></div>'
            f'<div>{_badge(control.get("control_type", ""), "badge-type")}{_badge("Risk: " + (control.get("residual_risk_if_failed") or ""), "badge-crit-medium")}</div>'
            '</div>'
            '<div class="ctrl-grid">'
            f'<div><div class="field-label">Objective</div><div class="field-value">{_esc(control.get("control_objective") or "--")}</div></div>'
            f'<div><div class="field-label">Owner</div><div class="field-value">{_esc(control.get("control_owner") or "--")}</div></div>'
            f'<div><div class="field-label">Evidence Generated</div><div class="field-value">{_esc(control.get("evidence_generated") or "--")}</div></div>'
            f'<div><div class="field-label">Control Level</div><div class="field-value">{_esc(control.get("control_level") or "--")}</div></div>'
            '</div>'
            f'<div style="margin-top:10px;"><div class="field-label">Description</div><div class="field-value">{_esc(control.get("control_description") or "--")}</div></div>'
            + (f'<div style="margin-top:10px;"><div class="field-label">Key Steps</div><ol class="ctrl-steps">{steps}</ol></div>' if steps else "")
            + '</div>'
        )

    parts.append('</div>')
    st.markdown("".join(parts), unsafe_allow_html=True)


def render_requirement_card(req: dict):
    obligations = req.get("obligations", [])
    header_badges = (
        _badge("new", "badge-new")
        + _crit_badge(req.get("criticality"))
        + _badge(req.get("obligation_type", ""), "badge-type")
        + _badge((req.get("execution_category") or "").replace("_", " "), "badge-exec")
        + _badge(f"{len(obligations)} Obligations", "badge-count")
    )
    st.markdown(
        '<div class="req-card">'
        f'<div class="req-id">{_esc(req.get("requirement_id"))}</div>'
        f'<div class="req-title">{_esc(req.get("requirement_title"))}</div>'
        f'<div>{header_badges}</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    for i, ob in enumerate(obligations, start=1):
        render_obligation_card(ob, i)


def render_regulation_view(doc):
    status = (doc.extra_meta or {}).get("status") or "Active"
    breadcrumb = " / ".join(filter(None, [doc.regulator, doc.source_system]))
    pdf_link = (doc.extra_meta or {}).get("org_pdf_link") or doc.document_url
    desc_html = doc.document_html or "<p>No content extracted.</p>"

    st.markdown(
        '<div class="reg-card">'
        f'<span class="pill-status">{_esc(status)}</span>'
        f'<div class="reg-title">{_esc(doc.title)}</div>'
        f'<div class="reg-crumb">{_esc(breadcrumb)}</div>'
        f'<div class="reg-ref">Ref No: {_esc(doc.reference_no or "--")}</div>'
        f'<div class="reg-desc">{desc_html}</div>'
        '<div class="reg-footer">'
        f'<div><div class="field-label">Source</div><a href="{_esc(pdf_link)}" target="_blank">View Document</a></div>'
        f'<div><div class="field-label">Publication</div><div class="field-value">Published: {_esc(doc.published_date or "--")}</div></div>'
        f'<div><div class="field-label">Metadata</div><div class="field-value">Year: {_esc(doc.year or "--")}</div></div>'
        '</div>'
        '</div>',
        unsafe_allow_html=True,
    )


# ==================================================================== #
#  SIDEBAR -- crawl settings                                            #
# ==================================================================== #
st.sidebar.header("Crawl settings")
delay = st.sidebar.slider("Delay between requests (s)", 0.5, 5.0, 1.0, 0.5)
limit = st.sidebar.number_input("Limit (0 = all)", min_value=0, value=0, step=1)

st.sidebar.divider()
run_pipeline_after = st.sidebar.checkbox(
    "Also run a pipeline preview (extraction + LLM analysis + requirement matching) on new/changed circulars",
    value=False,
    help="Calls the LLM API per circular (real cost/time). No DB writes -- results go to Excel + cards below.",
)
confirmed = True
analysis_limit = 0
if run_pipeline_after:
    confirmed = st.sidebar.checkbox("I understand this calls the LLM API for each circular", value=False)
    analysis_limit = st.sidebar.number_input(
        "Analyze first N of the crawled circulars (0 = all)", min_value=0, value=1, step=1,
        help="Crawl can pull more circulars than you want to spend LLM calls on -- this caps the pipeline preview only.",
    )

if st.sidebar.button("Start crawl", type="primary"):
    repo = get_repo()
    known = get_known_circulars(repo)
    st.info(f"DB baseline: {len(known)} known SAMA circulars.")
    st.warning("A visible Chrome window will open for the crawl -- watch it there. This page updates once it's done.")

    crawler = SAMARulebookCrawler(headless=False, request_delay=delay)
    with st.spinner("Crawling rulebook.sama.gov.sa (see the Chrome window)..."):
        docs = crawler.fetch_documents(limit=(limit or None), known_documents=known)

    st.session_state.crawled_docs = docs
    st.session_state.pipeline_results = None
    st.success(f"Crawl complete: {len(docs)} new/changed circulars found.")

docs = st.session_state.crawled_docs

if docs is None:
    st.info("Click **Start crawl** in the sidebar to begin.")
else:
    st.subheader(f"New / changed circulars ({len(docs)})")

    if not docs:
        st.info("Nothing new -- DB is already up to date.")
    else:
        st.dataframe(
            [{
                "Circular No": d.reference_no,
                "Title": d.title,
                "Issue Date": d.published_date,
                "Status": (d.extra_meta or {}).get("status"),
            } for d in docs],
            use_container_width=True,
        )

        if run_pipeline_after and not confirmed:
            st.sidebar.error("Check the confirmation box to enable the pipeline preview.")

        if run_pipeline_after and confirmed and st.button("Run pipeline preview (no DB writes)"):
            docs_to_analyze = docs[:analysis_limit] if analysis_limit else docs
            if analysis_limit and analysis_limit < len(docs):
                st.info(f"Analyzing {len(docs_to_analyze)} of {len(docs)} crawled circulars (per the sidebar limit).")

            repo = get_repo()
            orchestrator = Orchestrator(crawler=None, repo=repo, downloader=Downloader())
            progress = st.progress(0.0)
            summary_rows, all_requirement_rows, all_match_rows = [], [], []
            per_doc_details = []  # [(doc, [requirement_details...]), ...]

            for i, doc in enumerate(docs_to_analyze, start=1):
                st.markdown(f"**[{i}/{len(docs_to_analyze)}] {doc.title[:90]}**")
                status_line = st.empty()

                row, req_rows, match_rows, req_details = run_pipeline_dry_run(doc, repo, orchestrator)
                summary_rows.append(row)
                all_requirement_rows.extend(req_rows)
                all_match_rows.extend(match_rows)
                per_doc_details.append((doc, req_details))

                status_line.write(
                    f"{STATUS_ICON.get(row['Extraction'], '•')} Extraction ({row['Extracted Chars']} chars) -- "
                    f"{STATUS_ICON.get(row['LLM Analysis'], '•')} LLM Analysis ({row['Requirements Extracted']} requirements) -- "
                    f"{STATUS_ICON.get(row['Matching'], '•')} Matching "
                    f"({row['Fully Matched']} fully / {row['Partially Matched']} partial / {row['New Requirements']} new)"
                )
                if row["Error"]:
                    st.error(row["Error"])

                progress.progress(i / len(docs_to_analyze))

            OUTDIR.mkdir(parents=True, exist_ok=True)
            xlsx_path = OUTDIR / f"pipeline_preview_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
            for attempt in range(3):
                try:
                    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
                        pd.DataFrame(summary_rows).to_excel(xw, sheet_name="summary", index=False)
                        pd.DataFrame(all_requirement_rows).to_excel(xw, sheet_name="requirements", index=False)
                        pd.DataFrame(all_match_rows).to_excel(xw, sheet_name="requirement_matches", index=False)
                    break
                except PermissionError:
                    xlsx_path = OUTDIR / f"pipeline_preview_{datetime.now():%Y%m%d_%H%M%S}_{attempt + 1}.xlsx"

            st.session_state.pipeline_results = {"xlsx_path": str(xlsx_path), "per_doc_details": per_doc_details}
            st.success(f"Pipeline preview complete -- no DB writes made. Saved to {xlsx_path}")

        results = st.session_state.pipeline_results
        if results:
            with open(results["xlsx_path"], "rb") as f:
                st.download_button("Download Excel", f, file_name=Path(results["xlsx_path"]).name)

            st.divider()
            st.subheader("Detailed view")

            per_doc_details = results["per_doc_details"]
            doc_labels = [f"{d.reference_no} -- {d.title[:60]}" for d, _ in per_doc_details]
            selected = st.selectbox("Circular", options=range(len(doc_labels)), format_func=lambda i: doc_labels[i])
            selected_doc, selected_reqs = per_doc_details[selected]

            tab_rule, tab_requirement, tab_validation = st.tabs(["Rule", "Requirement", "Requirement Validation"])

            with tab_rule:
                render_regulation_view(selected_doc)

            with tab_requirement:
                if not selected_reqs:
                    st.info("No requirements extracted for this circular.")
                for req in selected_reqs:
                    render_requirement_card(req)

            with tab_validation:
                val_rows = [
                    {
                        "Requirement": req["requirement_title"],
                        "Obligation": ob.get("obligation_text"),
                        "Match Status": ob.get("match_status"),
                        "Explanation": ob.get("match_explanation"),
                    }
                    for req in selected_reqs
                    for ob in req.get("obligations", [])
                ]
                if val_rows:
                    st.dataframe(val_rows, use_container_width=True)
                else:
                    st.info("No matching results for this circular.")
