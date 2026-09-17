# -*- coding: utf-8 -*-
"""
CBB Compliance Library Browser
Tab per source system; hierarchy shown directly without click-to-expand.
Run: python -m streamlit run cbb_browser.py
"""

import json
import os
import re
from urllib.parse import urljoin

import pandas as pd
import pyodbc
import streamlit as st
from dotenv import load_dotenv

load_dotenv(override=True)


def _absolutify_links(html: str, base_url: str) -> str:
    """Rewrite relative href/src values to absolute URLs using base_url."""
    from bs4 import BeautifulSoup
    if not html or not base_url:
        return html
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(True):
        for attr in ("href", "src"):
            val = tag.get(attr)
            if val and not val.startswith(("http", "mailto:", "tel:", "#", "javascript:")):
                tag[attr] = urljoin(base_url, val)
    return str(soup)


st.set_page_config(
    page_title="CBB Compliance Library",
    layout="wide",
    page_icon="⚖️",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
/* Slim tree buttons */
div[data-testid="stButton"] > button {
    background: none !important;
    border: none !important;
    border-radius: 3px !important;
    padding: 2px 4px !important;
    text-align: left !important;
    font-size: 13px !important;
    width: 100% !important;
    box-shadow: none !important;
    color: #202124 !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
}
div[data-testid="stButton"] > button:hover {
    background: #e8f0fe !important;
    color: #1967d2 !important;
}
/* Selected doc highlight */
div[data-testid="stButton"] > button.selected {
    background: #d2e3fc !important;
    font-weight: 600 !important;
}
/* Module header */
.mod-header {
    font-size: 12px;
    font-weight: 700;
    color: #5f6368;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    margin: 8px 0 2px 0;
    padding: 3px 4px;
    background: #f1f3f4;
    border-radius: 3px;
}
/* Detail title */
.det-title { font-size: 19px; font-weight: 700; color: #202124; margin-bottom: 4px; }
.det-badge {
    display: inline-block;
    background: #e8f0fe; color: #1967d2;
    border-radius: 12px; padding: 2px 10px;
    font-size: 12px; margin: 0 4px 4px 0;
}
.content-box {
    background: #f8f9fa; border: 1px solid #e0e0e0;
    border-radius: 8px; padding: 16px;
    font-size: 14px; line-height: 1.7;
    max-height: 420px; overflow-y: auto;
}
</style>
""", unsafe_allow_html=True)

# ── Source labels ─────────────────────────────────────────────────────────────
SOURCE_LABELS = {
    "CBB-Rulebook":                   "CBB Rulebook",
    "CBB-AML-LAW":                    "AML Law",
    "CBB-Compliance":                 "Compliance",
    "CBB-Capital-Market-Regulations": "Capital Market Regs",
    "CBB-CORPGOV":                    "Corporate Governance",
    "CBB-Laws-Regulations":           "Laws & Regulations",
}

DOCS_PER_PAGE = 30

# ── DB ────────────────────────────────────────────────────────────────────────
def _conn_str() -> str:
    return (
        f"DRIVER={os.getenv('MSSQL_DRIVER')};"
        f"SERVER={os.getenv('MSSQL_SERVER')};"
        f"DATABASE={os.getenv('MSSQL_DATABASE')};"
        f"UID={os.getenv('MSSQL_USERNAME')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')};"
        "TrustServerCertificate=yes;"
    )


@st.cache_data(ttl=600, show_spinner="Loading documents…")
def load_df() -> pd.DataFrame:
    conn = pyodbc.connect(_conn_str(), autocommit=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, source_system, category, title,
               document_url, published_date, reference_no, year
        FROM   regulations
        WHERE  regulator = 'Central Bank of Bahrain'
        ORDER  BY source_system, category, title
    """)
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()
    return pd.DataFrame(rows)


@st.cache_data(ttl=120, show_spinner=False)
def load_doc(doc_id: int) -> dict:
    conn = pyodbc.connect(_conn_str(), autocommit=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, source_system, category, title,
               document_url, published_date, reference_no, year,
               extra_meta, document_html, status, type
        FROM   regulations WHERE id = ?
    """, [doc_id])
    cols = [c[0] for c in cur.description]
    row = cur.fetchone()
    conn.close()
    if not row:
        return {}
    d = dict(zip(cols, row))
    if d.get("extra_meta"):
        try:
            d["extra_meta"] = json.loads(d["extra_meta"])
        except Exception:
            d["extra_meta"] = {}
    else:
        d["extra_meta"] = {}
    return d


@st.cache_data(ttl=300, show_spinner=False)
def load_analysis(doc_id: int) -> list:
    conn = pyodbc.connect(_conn_str(), autocommit=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT requirement_id, requirement_title,
               execution_category, criticality, obligation_type,
               stage2_json, stage3_json
        FROM   compliance_analysis
        WHERE  regulation_id = ? AND schema_version = 'v2' AND is_current = 1
        ORDER  BY requirement_id
    """, [doc_id])
    cols = [c[0] for c in cur.description]
    rows = []
    for r in cur.fetchall():
        d = dict(zip(cols, r))
        for f in ("stage2_json", "stage3_json"):
            if d.get(f) and isinstance(d[f], str):
                try:
                    d[f] = json.loads(d[f])
                except Exception:
                    pass
        rows.append(d)
    conn.close()
    return rows


# ── Tree structure ────────────────────────────────────────────────────────────
MODULE_RE  = re.compile(r"^([A-Z]{2,5})-")
CHAPTER_RE = re.compile(r"^([A-Za-z0-9]{1,4})")


def _mod_ch(title: str):
    m = MODULE_RE.match(title)
    if not m:
        return None, None
    mod  = m.group(1)
    rest = title[len(mod) + 1:]
    c    = CHAPTER_RE.match(rest)
    return mod, (c.group(1) if c else "Other")


@st.cache_data(show_spinner=False)
def build_tree(df: pd.DataFrame) -> dict:
    """
    tree[ss][cat][module] = [doc_dict, ...]
    For CBB-Rulebook, module is the code (CA, LR…) or 'Ad-hoc'.
    For others, module is '__flat__'.
    """
    tree: dict = {}
    for _, row in df.iterrows():
        ss  = row["source_system"] or "Other"
        cat = row["category"]      or "Uncategorized"
        ttl = str(row["title"]     or "")
        if ss == "CBB-Rulebook":
            mod, _ = _mod_ch(ttl)
            mod = mod if mod else "Ad-hoc"
        else:
            mod = "__flat__"
        (tree
         .setdefault(ss, {})
         .setdefault(cat, {})
         .setdefault(mod, [])
         .append(row.to_dict()))
    return tree


# ── Session state ─────────────────────────────────────────────────────────────
def _init():
    if "sel" not in st.session_state:
        st.session_state.sel = None
    if "pg" not in st.session_state:
        st.session_state.pg = {}


# ── Shared UI helpers ─────────────────────────────────────────────────────────
def _doc_btn(doc: dict):
    """Render one clickable document button."""
    title   = str(doc.get("title") or "")
    doc_id  = doc["id"]
    display = title if len(title) <= 52 else title[:49] + "..."
    icon    = ">" if st.session_state.sel == doc_id else " "
    if st.button(f"{icon} {display}", key=f"d_{doc_id}", use_container_width=True):
        st.session_state.sel = doc_id


def _doc_list(docs: list, pg_key: str):
    """Paginated list of doc buttons."""
    page    = st.session_state.pg.get(pg_key, 0)
    visible = docs[: (page + 1) * DOCS_PER_PAGE]
    for doc in visible:
        _doc_btn(doc)
    remaining = len(docs) - len(visible)
    if remaining > 0:
        if st.button(f"  ... {remaining:,} more", key=f"more_{pg_key}_{page}",
                     use_container_width=True):
            p = dict(st.session_state.pg)
            p[pg_key] = page + 1
            st.session_state.pg = p


def _mod_sort(m: str) -> str:
    if m in ("Ad-hoc", "__flat__"):
        return "zzz"
    return m


# ── Detail panel ──────────────────────────────────────────────────────────────
def detail_panel():
    if not st.session_state.sel:
        st.markdown("""
        <div style="text-align:center;padding:80px 20px;color:#9aa0a6">
          <div style="font-size:44px">📋</div>
          <div style="font-size:16px;margin-top:12px;font-weight:600">
            Select a document from the list
          </div>
        </div>""", unsafe_allow_html=True)
        return

    doc  = load_doc(st.session_state.sel)
    if not doc:
        st.warning("Document not found.")
        return

    meta  = doc.get("extra_meta") or {}
    title = doc.get("title", "—")

    st.markdown(f'<div class="det-title">{title}</div>', unsafe_allow_html=True)

    badges = ""
    if doc.get("source_system"):
        badges += f'<span class="det-badge">📚 {SOURCE_LABELS.get(doc["source_system"], doc["source_system"])}</span>'
    if doc.get("category"):
        badges += f'<span class="det-badge">📂 {doc["category"]}</span>'
    if doc.get("published_date"):
        badges += f'<span class="det-badge">📅 {str(doc["published_date"])[:10]}</span>'
    if doc.get("reference_no"):
        badges += f'<span class="det-badge">🔖 {doc["reference_no"]}</span>'
    if badges:
        st.markdown(badges, unsafe_allow_html=True)

    url = doc.get("document_url") or meta.get("pdf_link")
    if not url:
        pdf_links = meta.get("pdf_links") or []
        url = pdf_links[0] if pdf_links else None
    if url:
        st.markdown(f"[Open document]({url})")
    if meta.get("faq_link"):
        st.markdown(f"[FAQ]({meta['faq_link']})")

    st.divider()

    analysis = load_analysis(doc["id"])
    tab_labels = ["Content"]
    if analysis:
        tab_labels.append(f"Compliance Analysis ({len(analysis)})")
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        content_text = meta.get("content_text") or ""
        doc_html = doc.get("document_html") or ""
        if content_text:
            st.markdown(
                f'<div class="content-box">{content_text.replace(chr(10), "<br>")}</div>',
                unsafe_allow_html=True,
            )
        elif doc_html:
            base = doc.get("document_url") or ""
            rendered_html = _absolutify_links(doc_html, base) if base else doc_html
            st.markdown(f'<div class="content-box">{rendered_html}</div>', unsafe_allow_html=True)
        else:
            st.info("No text content available — use the document link above.")
        extra_pdfs = meta.get("pdf_links") or []
        if len(extra_pdfs) > 1:
            with st.expander("More PDF links"):
                for lnk in extra_pdfs:
                    st.markdown(f"- [{lnk}]({lnk})")

    if analysis and len(tabs) > 1:
        with tabs[1]:
            for req in analysis:
                with st.expander(
                    f"**{req['requirement_id']}** — {req['requirement_title']}"
                    f"  `{req['criticality'] or ''}` `{req['execution_category'] or ''}`"
                ):
                    s2 = req.get("stage2_json") or {}
                    obs = s2.get("normalized_obligations", []) if isinstance(s2, dict) else []
                    if not obs:
                        st.caption("No obligation detail.")
                        continue
                    for ob in obs:
                        st.markdown(f"**{ob.get('obligation_id','')}** — {ob.get('obligation_type','')}")
                        st.write(ob.get("obligation_text", ""))
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Criticality", ob.get("criticality", "—"))
                        c2.metric("Execution",   ob.get("execution_category", "—"))
                        c3.metric("Clarity",     ob.get("clarity_score", "—"))
                        ev = ob.get("evidence_expected")
                        if ev:
                            st.markdown("**Evidence:** " + ", ".join(ev))
                        tm = ob.get("test_method")
                        if tm:
                            st.markdown(f"**Test method:** {tm}")
                        st.divider()


# ── Tab renderers ─────────────────────────────────────────────────────────────
def _clean_cat(cat: str) -> str:
    """Strip 'Central Bank of Bahrain ' prefix from volume names."""
    return re.sub(r"^Central Bank of Bahrain\s*", "", cat).strip()


def render_rulebook(ss_tree: dict):
    """CBB Rulebook: volume expanders (first pre-opened) → module sections → docs."""
    left, right = st.columns([4, 6], gap="medium")

    with left:
        search = st.text_input("Search", placeholder="Filter by title…",
                               label_visibility="collapsed", key="search_rb")
        cats = sorted(ss_tree.keys())
        for i, cat in enumerate(cats):
            cat_label = _clean_cat(cat)
            cat_tree  = ss_tree[cat]
            cat_count = sum(len(docs) for docs in cat_tree.values())
            with st.expander(f"📚 {cat_label}  ({cat_count:,})", expanded=(i == 0)):
                for mod in sorted(cat_tree, key=_mod_sort):
                    docs = cat_tree[mod]
                    if search and len(search) >= 2:
                        docs = [d for d in docs
                                if search.lower() in str(d.get("title", "")).lower()]
                    if not docs:
                        continue
                    mod_label = "Ad-hoc Communications" if mod == "Ad-hoc" else mod
                    st.markdown(
                        f'<div class="mod-header">{mod_label} &nbsp;({len(docs):,})</div>',
                        unsafe_allow_html=True,
                    )
                    _doc_list(docs, f"{cat}|{mod}")

    with right:
        detail_panel()


def render_flat(ss: str, ss_tree: dict):
    """Small sources: category headers → doc list → detail panel."""
    left, right = st.columns([4, 6], gap="medium")

    with left:
        search = st.text_input("Search", placeholder="Filter…",
                               label_visibility="collapsed", key=f"search_{ss}")
        for cat, cat_tree in sorted(ss_tree.items()):
            all_docs = [d for mod_docs in cat_tree.values() for d in mod_docs]
            if search and len(search) >= 2:
                all_docs = [d for d in all_docs
                            if search.lower() in str(d.get("title", "")).lower()]
            if not all_docs:
                if search:
                    continue
            st.markdown(f"#### {cat}")
            _doc_list(all_docs, f"{ss}|{cat}")

    with right:
        detail_panel()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    _init()

    st.markdown("### ⚖️ CBB Compliance Library")
    st.caption("Central Bank of Bahrain — Regulatory Document Browser")

    df   = load_df()
    tree = build_tree(df)

    # Build tabs in a stable order
    order = [
        "CBB-Rulebook",
        "CBB-AML-LAW",
        "CBB-Compliance",
        "CBB-CORPGOV",
        "CBB-Capital-Market-Regulations",
        "CBB-Laws-Regulations",
    ]
    sources    = [s for s in order if s in tree] + \
                 [s for s in sorted(tree) if s not in order]
    tab_labels = [SOURCE_LABELS.get(s, s) for s in sources]
    tabs       = st.tabs(tab_labels)

    for tab, ss in zip(tabs, sources):
        with tab:
            if ss == "CBB-Rulebook":
                render_rulebook(tree[ss])
            else:
                render_flat(ss, tree[ss])


if __name__ == "__main__":
    main()
