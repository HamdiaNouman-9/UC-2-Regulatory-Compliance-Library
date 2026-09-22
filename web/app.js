"use strict";

/* ============================================================== *
 *  Regulatory Compliance Library -- demo UI                       *
 *  Talks directly to apis/pipeline_api.py (real DB, no mocking).  *
 * ============================================================== */

const DEFAULT_API_BASE = "http://localhost:8000";
const DEFAULT_FORMFILL_BASE = "http://localhost:8100";

const state = {
  apiBase: localStorage.getItem("rcl_api_base") || DEFAULT_API_BASE,
  formfillBase: localStorage.getItem("rcl_formfill_base") || DEFAULT_FORMFILL_BASE,
  formfillListLoaded: false,
  // browse (grid) state
  currentFolder: null,       // {id, title, type, has_regulations} | null = roots
  folderPath: [],            // breadcrumb stack of the same shape
  // detail state
  selectedRegulationId: null,
  activeTab: "regulation",
  sidebarManuallyHidden: false,
  mode: "library", // "library" | "pipeline" | "alerts" | "model"
  currentModel: null,
  modelList: [],
};

const $ = (sel, root) => (root || document).querySelector(sel);
const $$ = (sel, root) => Array.from((root || document).querySelectorAll(sel));

/* ---------------------------------------------------------------- *
 *  API client                                                       *
 * ---------------------------------------------------------------- */

async function api(path, params) {
  const url = new URL(state.apiBase.replace(/\/$/, "") + path);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v === undefined || v === null) return;
      url.searchParams.set(k, v);
    });
  }
  const res = await fetch(url.toString());
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText} -- ${body.slice(0, 200)}`);
  }
  return res.json();
}

async function apiPut(path, body) {
  const res = await fetch(state.apiBase.replace(/\/$/, "") + path, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const d = data.detail;
    throw new Error(typeof d === "string" ? d : `${res.status} ${res.statusText}`);
  }
  return data;
}

async function apiPost(path, opts) {
  const res = await fetch(state.apiBase.replace(/\/$/, "") + path, {
    method: "POST",
    ...opts,
  });
  return res.json().catch(() => ({}));
}

// dynamic_crawler/formfill/api.py -- a SEPARATE service, its own port, Excel
// output only. Never the same base as the main MSSQL-backed api().
async function formfillGet(path, params) {
  const url = new URL(state.formfillBase.replace(/\/$/, "") + path);
  if (params) Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) url.searchParams.set(k, v);
  });
  const res = await fetch(url.toString());
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.detail || `${res.status} ${res.statusText}`);
  return body;
}

async function formfillPost(path, params) {
  const url = new URL(state.formfillBase.replace(/\/$/, "") + path);
  if (params) Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) url.searchParams.set(k, v);
  });
  const res = await fetch(url.toString(), { method: "POST" });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body.detail || `${res.status} ${res.statusText}`);
  return body;
}

async function checkHealth() {
  const dot = $("#healthDot");
  try {
    const h = await api("/health");
    dot.classList.toggle("ok", !!h.success);
    dot.classList.toggle("bad", !h.success);
    dot.title = `DB: ${h.database}`;
  } catch (e) {
    dot.classList.add("bad");
    dot.title = "API unreachable: " + e.message;
  }
}

/* ---------------------------------------------------------------- *
 *  Small helpers                                                    *
 * ---------------------------------------------------------------- */

function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

function sanitizeHtml(html, maxLen) {
  let h = html || "";
  if (maxLen && h.length > maxLen) h = h.slice(0, maxLen);
  h = h.replace(/<script[\s\S]*?<\/script>/gi, "")
       .replace(/<style[\s\S]*?<\/style>/gi, "")
       .replace(/\son\w+\s*=\s*"(.*?)"/gi, "")
       .replace(/\son\w+\s*=\s*'(.*?)'/gi, "")
       .replace(/javascript:/gi, "");
  return h;
}

const DISPOSITIONS = {
  OBL:  { label: "Obligation",         cls: "badge-obl" },
  COND: { label: "Conditional",        cls: "badge-cond" },
  REG:  { label: "Regulator-Directed", cls: "badge-reg" },
  DEF:  { label: "Definition",         cls: "badge-def" },
  INFO: { label: "Informational",      cls: "badge-info" },
};

function priorityClass(p) {
  const key = (p || "").toLowerCase();
  if (key === "high") return "badge-high";
  if (key === "medium") return "badge-medium";
  if (key === "low") return "badge-low";
  return "badge-type";
}

function statusPillClass(statusText) {
  const s = (statusText || "").toLowerCase();
  if (s.includes("in-force") || s.includes("active")) return "pill-in-force";
  if (s.includes("supersed") || s.includes("withdraw") || s.includes("repeal")) return "pill-superseded";
  return "pill-neutral";
}

function fmtDate(d) {
  if (!d) return null;
  return String(d).slice(0, 10);
}

function fmtYear(y) {
  if (y === null || y === undefined || y === "") return null;
  const n = Number(y);
  return Number.isFinite(n) ? String(Math.trunc(n)) : String(y);
}

/* ---------------------------------------------------------------- *
 *  TEMP FIX: hide category branches that lead nowhere.               *
 *  Some regulator category trees (e.g. CBE) have dead-end folders  *
 *  -- no regulation of their own AND no real content a few levels  *
 *  down (crawl-time nav items that never got populated). Rather    *
 *  than show them and reveal "(empty)" after a click, look a few   *
 *  levels ahead and filter them out of the list before it renders. *
 *  Real fix belongs in the category data itself, not the UI.       *
 * ---------------------------------------------------------------- */

// Kept small on purpose: a naive recursive check here can fan out into
// hundreds or thousands of concurrent DB queries the moment it hits a
// wide branch (some category subtrees run to the thousands of rows), and
// did exactly that against the real DB during testing. depth=1 already
// catches both a plain dead-end (no children) and a one-hop dead chain
// (a folder whose only child is itself a dead end) -- deeper chains are
// rare enough to accept missing, for a temp fix.
const EMPTY_CHECK_MAX_DEPTH = 1;
// Never fan out past a node with more than this many children -- a wide
// branch is overwhelmingly real content, not worth the query volume to
// rule out the edge case.
const EMPTY_CHECK_MAX_FANOUT = 30;
const emptyCheckCache = new Map(); // compliancecategory_id -> Promise<boolean>

// Shared by the empty-branch check above AND the folder/document icon
// resolution below, so a category's children are never fetched twice.
const childrenFetchCache = new Map(); // compliancecategory_id -> Promise<node[]>

function fetchChildrenCached(categoryId) {
  if (childrenFetchCache.has(categoryId)) return childrenFetchCache.get(categoryId);
  const promise = api(`/categories/children/${categoryId}`)
    .then((r) => r.data || [])
    .catch(() => []); // fetch failed -- fail open (treat as childless rather than retry-loop)
  childrenFetchCache.set(categoryId, promise);
  return promise;
}

function isEmptyBranch(node, depth) {
  if (node.has_regulations) return Promise.resolve(false);
  const key = node.compliancecategory_id;
  if (emptyCheckCache.has(key)) return emptyCheckCache.get(key);
  const promise = (async () => {
    const kids = await fetchChildrenCached(key);
    if (!kids.length) return true;
    if (kids.length > EMPTY_CHECK_MAX_FANOUT) return false;
    if (depth >= EMPTY_CHECK_MAX_DEPTH) return false; // assume non-empty past the depth cap
    const results = await Promise.all(kids.map((k) => isEmptyBranch(k, depth + 1)));
    return results.every(Boolean);
  })();
  emptyCheckCache.set(key, promise);
  return promise;
}

async function filterNonEmpty(nodes) {
  const flags = await Promise.all(nodes.map((n) => isEmptyBranch(n, 0)));
  return nodes.filter((_, i) => !flags[i]);
}

/* ---------------------------------------------------------------- *
 *  Tree (left sidebar)                                              *
 * ---------------------------------------------------------------- */

// A category flagged type "R" (has a regulation of its own) can ALSO have
// real sub-categories underneath it -- e.g. "Banking Control Law" is both
// the whole-law document AND the parent of 26 Article sub-categories. Type
// alone can't tell the two apart, so an R-node's icon is only "document"
// once we've actually checked it has no children; otherwise it's a folder,
// same as any other node you can still drill into.
const ICON_CHECK_MAX_LIST = 60; // skip the lookahead for a very wide list of cards

async function resolveIconKind(node) {
  if (node.type !== "R") return "folder";
  const kids = await fetchChildrenCached(node.compliancecategory_id);
  return kids.length > 0 ? "folder" : "doc";
}

async function resolveIconKinds(nodes) {
  const map = new Map();
  if (nodes.length > ICON_CHECK_MAX_LIST) {
    // Too many cards to check individually without hammering the API --
    // fall back to the type-based default. In every wide list observed,
    // a type "R" node at this scale is a single document with nothing
    // beneath it, so this is a safe approximation, not a guess.
    nodes.forEach((n) => map.set(n.compliancecategory_id, n.type === "R" ? "doc" : "folder"));
    return map;
  }
  const kinds = await Promise.all(nodes.map(resolveIconKind));
  nodes.forEach((n, i) => map.set(n.compliancecategory_id, kinds[i]));
  return map;
}

const iconGlyph = (kind) => (kind === "doc" ? "&#128196;" : "&#128193;");

function treeNodeLi(node, iconKind) {
  const li = document.createElement("li");
  li.dataset.id = node.compliancecategory_id;
  li.dataset.type = node.type;

  const row = document.createElement("div");
  row.className = "tree-node";
  row.innerHTML = `
    <span class="tree-caret">&#9656;</span>
    <span class="tree-icon">${iconGlyph(iconKind || (node.type === "R" ? "doc" : "folder"))}</span>
    <span class="tree-label" title="${esc(node.title)}">${esc(node.title)}</span>
    ${node.regulation_count ? `<span class="tree-count">${node.regulation_count}</span>` : ""}
  `;
  li.appendChild(row);

  const caret = row.querySelector(".tree-caret");
  const childUl = document.createElement("ul");
  childUl.classList.add("hidden");
  li.appendChild(childUl);

  let loaded = false;

  async function toggle() {
    if (!childUl.classList.contains("hidden")) {
      childUl.classList.add("hidden");
      caret.style.transform = "rotate(0deg)";
      return;
    }
    if (!loaded) {
      childUl.innerHTML = '<li class="tree-loading">Loading&hellip;</li>';
      childUl.classList.remove("hidden");
      try {
        const resp = await api(`/categories/children/${node.compliancecategory_id}`);
        const kids = await filterNonEmpty(resp.data || []);

        // A leaf regulation with nothing beneath it: expanding to show a
        // single "itself" row is pointless -- open it directly instead.
        if (!kids.length && node.has_regulations) {
          childUl.classList.add("hidden");
          caret.style.visibility = "hidden";
          openRegulationForCategory(node);
          return;
        }

        // A category can be BOTH a folder (real sub-categories below it)
        // AND a regulation in its own right -- e.g. "Banking Control Law"
        // has 26 Article sub-categories but is also itself the whole-text
        // regulation. Once it has real children to show, only THOSE are
        // listed -- no same-named leaf for the folder's own document; its
        // own text is redundant with browsing the real children.
        childUl.innerHTML = "";
        if (!kids.length) {
          // Reachable only via the depth-cap fail-open path in
          // filterNonEmpty -- genuinely empty in the DB. Say so, or an
          // empty expand looks identical to a stuck/broken fetch.
          caret.style.visibility = "hidden";
          const emptyLi = document.createElement("li");
          emptyLi.className = "tree-loading";
          emptyLi.textContent = "(empty)";
          childUl.appendChild(emptyLi);
        }
        const iconKinds = await resolveIconKinds(kids);
        kids.forEach((k) => childUl.appendChild(treeNodeLi(k, iconKinds.get(k.compliancecategory_id))));
        loaded = true;
      } catch (e) {
        childUl.innerHTML = `<li class="tree-loading">Failed to load</li>`;
      }
    } else {
      childUl.classList.remove("hidden");
    }
    caret.style.transform = "rotate(90deg)";
  }

  caret.addEventListener("click", (ev) => { ev.stopPropagation(); toggle(); });

  row.addEventListener("click", () => {
    $$(".tree-node.selected").forEach((n) => n.classList.remove("selected"));
    row.classList.add("selected");
    toggle();
  });

  return li;
}

async function renderTreeRoots() {
  const root = $("#treeRoot");
  root.innerHTML = '<li class="tree-loading">Loading&hellip;</li>';
  try {
    const resp = await api("/categories/roots");
    root.innerHTML = "";
    (resp.data || []).forEach((n) => root.appendChild(treeNodeLi(n)));
  } catch (e) {
    root.innerHTML = `<li class="tree-loading">Could not load categories: ${esc(e.message)}</li>`;
  }
}

$("#treeFilter").addEventListener("input", (ev) => {
  const q = ev.target.value.trim().toLowerCase();
  $$("#treeRoot li").forEach((li) => {
    const label = li.querySelector(":scope > .tree-node .tree-label");
    if (!label) return;
    const match = !q || label.textContent.toLowerCase().includes(q);
    li.style.display = match ? "" : "none";
  });
});

/* ---------------------------------------------------------------- *
 *  Browse view (folder grid)                                        *
 * ---------------------------------------------------------------- */

function setCrumbs(parts) {
  $("#crumbs").innerHTML = parts.map(esc).join('<span class="sep">/</span>');
}

async function fetchRegulationsForCategory(categoryId) {
  const resp = await api(`/regulations/by-category/${categoryId}`);
  return resp.data || [];
}

async function renderBrowse(folder) {
  state.currentFolder = folder;
  state.selectedRegulationId = null;
  showBrowseView();

  const grid = $("#folderGrid");
  const emptyEl = $("#browseEmpty");
  grid.innerHTML = '<div class="loading">Loading&hellip;</div>';
  emptyEl.classList.add("hidden");

  $("#pageTitle").textContent = "COMPLIANCE LIBRARY REGULATIONS";
  const crumbParts = ["Compliance Radar", "Compliance Library Regulations",
    ...state.folderPath.map((f) => f.title)];
  setCrumbs(crumbParts);

  try {
    let kids = [];
    if (!folder) {
      kids = (await api("/categories/roots")).data || [];
    } else {
      kids = (await api(`/categories/children/${folder.id}`)).data || [];
    }
    kids = await filterNonEmpty(kids);

    // Leaf regulation category with nothing else under it -- skip the
    // redundant single-card screen and open the document directly.
    if (folder && folder.has_regulations && kids.length === 0) {
      const regs = await fetchRegulationsForCategory(folder.id);
      if (regs.length) {
        openRegulation(regs[0].id);
        return;
      }
    }

    // A category can be BOTH a folder (real sub-categories below it) AND a
    // regulation in its own right -- e.g. "Banking Control Law" also has 26
    // Article sub-categories. Once it has real children to show, only
    // THOSE are listed here, not a same-named card for the folder's own
    // document -- its own text is redundant with browsing the real
    // children.
    grid.innerHTML = "";
    const iconKinds = await resolveIconKinds(kids);
    kids.forEach((k) => {
      const card = document.createElement("div");
      card.className = "folder-card";
      card.innerHTML = `
        <span class="ic">${iconGlyph(iconKinds.get(k.compliancecategory_id))}</span>
        <span class="lbl">${esc(k.title)}</span>
        ${k.regulation_count ? `<span class="cnt">${k.regulation_count}</span>` : ""}
      `;
      card.addEventListener("click", () => {
        state.folderPath.push({ id: k.compliancecategory_id, title: k.title, type: k.type,
                                has_regulations: k.has_regulations });
        renderBrowse({ id: k.compliancecategory_id, title: k.title, type: k.type,
                       has_regulations: k.has_regulations });
      });
      grid.appendChild(card);
    });

    if (!kids.length) emptyEl.classList.remove("hidden");
  } catch (e) {
    grid.innerHTML = `<div class="error-box">Failed to load: ${esc(e.message)}</div>`;
  }
}

async function openRegulationForCategory(node) {
  try {
    const regs = await fetchRegulationsForCategory(node.compliancecategory_id);
    if (regs.length) {
      openRegulation(regs[0].id);
    }
  } catch (e) {
    alert("Could not load regulation for this category: " + e.message);
  }
}

function showBrowseView() {
  $("#browseView").classList.remove("hidden");
  $("#detailView").classList.add("hidden");
  $("#backBtn").classList.toggle("hidden", state.folderPath.length === 0);
  updateSidebarVisibility();
}

// The left tree and the browse grid are two independent navigators that
// don't stay in sync with each other (clicking one doesn't move the
// other) -- so, matching the reference screens, the tree only appears
// once a regulation is open (sibling-navigation context); the grid is
// the sole navigator on the plain browse screen.
function updateSidebarVisibility() {
  const show = state.selectedRegulationId !== null && !state.sidebarManuallyHidden;
  $("#sidebar").style.display = show ? "" : "none";
}

$("#backBtn").addEventListener("click", () => {
  state.selectedRegulationId = null;
  // Pop the folder we're leaving -- if it's a leaf-only category (the
  // common case: a folder whose only content is one regulation), it
  // auto-opens that regulation the moment it's rendered, so re-rendering
  // it here without popping first would bounce straight back into the
  // regulation Back was meant to leave.
  state.folderPath.pop();
  renderBrowse(state.folderPath.length ? state.folderPath[state.folderPath.length - 1] : null);
});

/* ---------------------------------------------------------------- *
 *  Detail view -- Regulation tab                                    *
 * ---------------------------------------------------------------- */

function attachmentsList(detail) {
  const meta = detail.extra_meta || {};
  const links = String(meta.attachment_links || "").split("|").map((s) => s.trim()).filter(Boolean);
  const titles = String(meta.file_titles || "").split("|").map((s) => s.trim()).filter(Boolean);
  const rows = [];
  if (detail.document_url) rows.push({ label: detail.title || "Document", url: detail.document_url });
  links.forEach((url, i) => rows.push({ label: titles[i] || url.split("/").pop(), url }));
  if (!rows.length) return "<p style='color:var(--muted); font-size:12.5px;'>No attached documents on file.</p>";
  return `<ul class="attach-list">${rows.map((r) =>
    `<li>&#128206; <a href="${esc(r.url)}" target="_blank" rel="noopener">${esc(r.label)}</a></li>`).join("")}</ul>`;
}

function metaChips(detail) {
  const meta = detail.extra_meta || {};
  const labelled = {
    status: "Regulator status", sector: "Sector", doc_type: "Type", law: "Parent law",
    resolution_number: "Resolution No.", scope_of_application: "Scope",
    beneficiaries: "Beneficiaries", issue_date_hijri: "Issue date (Hijri)",
    release_date: "Release date", execution_date: "Execution date",
    last_update: "Last update", superseded_by: "Superseded by",
  };
  const chips = Object.entries(labelled)
    .filter(([k]) => meta[k])
    .map(([k, label]) => `<span class="meta-chip"><b>${esc(label)}:</b>${esc(meta[k])}</span>`);
  return chips.length ? `<div class="meta-chip-row">${chips.join("")}</div>` : "";
}

function renderRegulationTab(detail) {
  const meta = detail.extra_meta || {};
  const statusText = meta.status || detail.status || "";
  const pillCls = statusPillClass(statusText);
  const html = sanitizeHtml(detail.document_html, 200000);
  const truncatedNote = (detail.document_html || "").length > 200000
    ? `<div class="desc-truncated-note">Showing first 200,000 of ${detail.document_html.length.toLocaleString()} characters.
       <a href="${esc(detail.document_url || '#')}" target="_blank" rel="noopener">Open full source document</a></div>`
    : "";

  $("#tab-regulation").innerHTML = `
    <div class="reg-header">
      <div>
        <div class="reg-title">${esc(detail.title || "(untitled)")}</div>
        <div class="reg-subline">&#127970; ${esc(detail.regulator || "")} ${detail.source_system ? " / " + esc(detail.source_system) : ""}</div>
        ${detail.reference_no ? `<div class="reg-refno">Ref No: ${esc(detail.reference_no)}</div>` : ""}
      </div>
      ${statusText ? `<span class="pill ${pillCls}">${esc(statusText)}</span>` : ""}
    </div>

    ${metaChips(detail)}

    <div class="section-label">&#128196; Description</div>
    <div class="desc-box">
      ${html ? html : '<span style="color:var(--muted);">No stored document content for this regulation.</span>'}
    </div>
    ${truncatedNote}

    <div class="footer-cards">
      <div class="foot-card">
        <div class="fc-title">&#128279; Source</div>
        ${detail.document_url ? `<a href="${esc(detail.document_url)}" target="_blank" rel="noopener">View document</a>` : "<span style='color:var(--muted); font-size:13px;'>No document URL</span>"}
        ${detail.source_page_url ? `<div class="fc-row"><a href="${esc(detail.source_page_url)}" target="_blank" rel="noopener">Source page</a></div>` : ""}
      </div>
      <div class="foot-card">
        <div class="fc-title">&#128197; Publication</div>
        <div class="fc-row"><b>Published:</b> ${esc(fmtDate(detail.published_date) || "-")}</div>
        <div class="fc-row"><b>Created:</b> ${esc(fmtDate(detail.created_at) || "-")}</div>
        <div class="fc-row"><b>Updated:</b> ${esc(fmtDate(detail.updated_at) || "-")}</div>
      </div>
      <div class="foot-card">
        <div class="fc-title">&#8505;&#65039; Metadata</div>
        <div class="fc-row"><b>Year:</b> ${esc(fmtYear(detail.year) || "-")}</div>
        <div class="fc-row"><b>Department:</b> ${esc(detail.department || "-")}</div>
        <div class="fc-row"><b>Category:</b> ${esc(detail.category || "-")}</div>
      </div>
    </div>

    <div class="section-label">&#128193; Attached documents</div>
    ${attachmentsList(detail)}
  `;
}

/* ---------------------------------------------------------------- *
 *  Detail view -- Requirements & Activities tab                     *
 * ---------------------------------------------------------------- */

function renderActivityCard(a, idx) {
  const evidence = a.evidence_expected || [];
  return `
    <div class="act-card">
      <div class="act-num">${idx}</div>
      <div class="act-body">
        <div class="act-title">${esc(a.title)}</div>
        ${a.description ? `<div class="act-desc">${esc(a.description)}</div>` : ""}
        <div class="act-badges">
          ${a.priority ? `<span class="badge ${priorityClass(a.priority)}">${esc(a.priority)}</span>` : ""}
          ${a.frequency_type ? `<span class="badge badge-type">${esc(a.frequency_type)}${a.frequency && a.frequency !== a.frequency_type ? " &middot; " + esc(a.frequency) : ""}</span>` : ""}
          ${a.suggested_activity_type ? `<span class="badge badge-type">${esc(a.suggested_activity_type)}</span>` : ""}
        </div>
        <div class="act-meta">${a.suggested_department ? "&#128100; " + esc(a.suggested_department) : ""}</div>
        ${evidence.length ? `<div class="act-evidence"><b>Evidence expected:</b><ul>${evidence.map((e) => `<li>${esc(e)}</li>`).join("")}</ul></div>` : ""}
      </div>
    </div>
  `;
}

function renderRequirementCard(req) {
  const disp = DISPOSITIONS[req.disposition] || null;
  const spanStatus = req.superseded_in_version_id ? "Superseded" : "Active";
  const activities = req.activities || [];

  const card = document.createElement("div");
  card.className = "req-card";
  card.innerHTML = `
    <div class="req-card-head">
      <span class="req-caret">&#9656;</span>
      <span class="req-refkey">${esc(req.ref_key || req.requirement_id)}</span>
      <span class="req-title">${esc(req.title)}</span>
      <div class="req-badges">
        ${disp ? `<span class="badge ${disp.cls}">${esc(disp.label)}</span>` : ""}
        ${req.requirement_type ? `<span class="badge badge-type">${esc(req.requirement_type)}</span>` : ""}
        <span class="pill ${statusPillClass(spanStatus)}">${spanStatus}</span>
      </div>
    </div>
    <div class="req-card-body">
      <div class="req-fact-row">
        ${req.actor ? `<span><b>Actor:</b> ${esc(req.actor)}</span>` : ""}
        ${req.nature ? `<span><b>Nature:</b> ${esc(req.nature)}</span>` : ""}
        ${req.disposition_reason ? `<span><b>Why:</b> ${esc(req.disposition_reason)}</span>` : ""}
      </div>
      ${req.description ? `<div class="req-desc">${esc(req.description)}</div>` : ""}
      ${req.condition_text ? `<div class="req-desc"><b>Condition:</b> ${esc(req.condition_text)}</div>` : ""}
      ${req.source_reference ? `<div class="req-desc" style="color:var(--muted); font-size:12px;">&#128278; ${esc(req.source_reference)}</div>` : ""}

      <div class="act-section-label">Activities (${activities.length})</div>
      ${activities.length
        ? `<div class="act-list">${activities.map((a, i) => renderActivityCard(a, i + 1)).join("")}</div>`
        : `<div class="no-activities">No activities designed for this requirement yet.</div>`}
    </div>
  `;
  card.querySelector(".req-card-head").addEventListener("click", () => {
    card.classList.toggle("open");
  });
  return card;
}

const capitalize = (s) => s.charAt(0).toUpperCase() + s.slice(1);

async function loadRequirementsTab(regId, regTitle) {
  const container = $("#tab-reqact");
  container.innerHTML = `
    <div class="reqact-toolbar">
      <label style="font-size:12.5px; color:var(--muted);">
        <input type="checkbox" id="activeOnlyToggle" checked> Active only (hide superseded spans)
      </label>
      <select id="natureFilter" class="btn-sm">
        <option value="">All natures</option>
      </select>
      <button class="btn-sm" id="refreshReqBtn">Refresh</button>
      <button class="btn-sm primary" id="runAnalysisBtn">Run requirement/activity analysis</button>
      <span class="count" id="reqCount"></span>
    </div>
    <div id="analysisLog" class="analysis-log hidden"></div>
    <div class="req-list" id="reqList"><div class="loading">Loading&hellip;</div></div>
  `;

  let allReqs = [];

  function renderList() {
    const listEl = $("#reqList");
    const natureVal = $("#natureFilter").value;
    const filtered = natureVal ? allReqs.filter((r) => r.nature === natureVal) : allReqs;
    $("#reqCount").textContent = natureVal
      ? `${filtered.length} of ${allReqs.length} requirement(s)`
      : `${allReqs.length} requirement(s)`;
    listEl.innerHTML = "";
    if (!filtered.length) {
      listEl.innerHTML = allReqs.length
        ? `<div class="empty-state">No requirements have nature "${esc(natureVal)}".</div>`
        : `<div class="empty-state">No requirements stored yet for "${esc(regTitle)}".
            Use "Run requirement/activity analysis" above to extract them from the stored document.</div>`;
      return;
    }
    filtered.forEach((r) => listEl.appendChild(renderRequirementCard(r)));
  }

  async function load() {
    const activeOnly = $("#activeOnlyToggle").checked;
    const listEl = $("#reqList");
    listEl.innerHTML = '<div class="loading">Loading&hellip;</div>';
    try {
      const resp = await api(`/regulation/${regId}/requirements`, { active_only: activeOnly });
      allReqs = resp.data || [];

      // Options reflect whatever nature values are actually present on
      // this regulation's requirements, not a hardcoded list.
      const natureSelect = $("#natureFilter");
      const keepVal = natureSelect.value;
      const natures = Array.from(new Set(allReqs.map((r) => r.nature).filter(Boolean))).sort();
      natureSelect.innerHTML = '<option value="">All natures</option>' +
        natures.map((n) => `<option value="${esc(n)}">${esc(capitalize(n))}</option>`).join("");
      if (natures.includes(keepVal)) natureSelect.value = keepVal;

      renderList();
    } catch (e) {
      listEl.innerHTML = `<div class="error-box">Failed to load requirements: ${esc(e.message)}</div>`;
    }
  }

  $("#activeOnlyToggle").addEventListener("change", load);
  $("#natureFilter").addEventListener("change", renderList);
  $("#refreshReqBtn").addEventListener("click", load);
  $("#runAnalysisBtn").addEventListener("click", () => runAnalysis(regId, load));

  await load();
}

// Mirrors the 3 stages the backend actually walks through in
// _run_requirement_activity_analysis_for_regulation (apis/pipeline_api.py):
// pull text out of the document/attachments, run Stage A (requirements),
// then Stage B (activities). "queued" is the state right after triggering,
// before the background thread has picked a stage yet.
const ANALYSIS_STEPS = [
  { key: "extracting_text", label: "Extracting text from PDF" },
  { key: "generating_requirements", label: "Generating requirements" },
  { key: "generating_activities", label: "Generating activities" },
];

function renderAnalysisLog(stage, note) {
  const el = $("#analysisLog");
  el.classList.remove("hidden");
  const order = ANALYSIS_STEPS.map((s) => s.key);
  const currentIdx = stage === "done" ? order.length : order.indexOf(stage); // -1 while "queued"
  el.innerHTML = ANALYSIS_STEPS.map((s, i) => {
    let cls = "pending", icon = "&#9675;";
    if (currentIdx > i) { cls = "done"; icon = "&#10003;"; }
    else if (i === currentIdx) { cls = "active"; icon = '<span class="log-spinner"></span>'; }
    return `<div class="log-step ${cls}"><span class="log-icon">${icon}</span><span class="log-text">${esc(s.label)}</span></div>`;
  }).join("") + (note ? `<div class="log-note">${esc(note)}</div>` : "");
}

function renderAnalysisError(message) {
  const el = $("#analysisLog");
  el.classList.remove("hidden");
  el.innerHTML = `<div class="log-step failed"><span class="log-icon">&#10007;</span><span class="log-text">${esc(message)}</span></div>`;
}

async function runAnalysis(regId, onDone) {
  const btn = $("#runAnalysisBtn");
  btn.disabled = true;
  renderAnalysisLog("queued", "Starting…");
  try {
    await apiPost(`/regulation/${regId}/analyze`);
    let attempts = 0;
    const poll = async () => {
      attempts += 1;
      const st = await api(`/regulation/${regId}/analyze`);
      if (st.state === "done") {
        renderAnalysisLog("done", `Done -- ${st.requirements_extracted || 0} requirement(s), ${st.activities_extracted || 0} activity(ies) extracted.`);
        btn.disabled = false;
        onDone();
        return;
      }
      if (st.state === "failed") {
        renderAnalysisError(`Analysis failed: ${st.error || "unknown error"}`);
        btn.disabled = false;
        return;
      }
      if (attempts > 150) {
        renderAnalysisLog(st.stage || "queued", "Still running after several minutes -- check back later or click Refresh.");
        btn.disabled = false;
        return;
      }
      renderAnalysisLog(st.stage || "queued");
      setTimeout(poll, 4000);
    };
    setTimeout(poll, 1500);
  } catch (e) {
    renderAnalysisError("Could not start analysis: " + e.message);
    btn.disabled = false;
  }
}

/* ---------------------------------------------------------------- *
 *  Detail view -- shell / tabs                                      *
 * ---------------------------------------------------------------- */

function switchTab(tabName) {
  state.activeTab = tabName;
  $$(".tab-btn").forEach((b) => b.classList.toggle("active", b.dataset.tab === tabName));
  $("#tab-regulation").classList.toggle("hidden", tabName !== "regulation");
  $("#tab-reqact").classList.toggle("hidden", tabName !== "reqact");
}

$$(".tab-btn").forEach((btn) => btn.addEventListener("click", () => switchTab(btn.dataset.tab)));

async function openRegulation(regId) {
  state.selectedRegulationId = regId;
  $("#browseView").classList.add("hidden");
  $("#detailView").classList.remove("hidden");
  $("#backBtn").classList.remove("hidden");
  $("#pageTitle").textContent = "REGULATION DETAILS";
  setCrumbs(["Compliance Radar", "Compliance Library Regulations", "Regulation Details"]);
  updateSidebarVisibility();
  switchTab("regulation");
  $("#tab-regulation").innerHTML = '<div class="loading">Loading&hellip;</div>';
  $("#tab-reqact").innerHTML = "";

  try {
    const resp = await api(`/regulation/${regId}`);
    const detail = resp.data;
    renderRegulationTab(detail);
    setCrumbs((detail.doc_path && detail.doc_path.length) ? detail.doc_path : ["Compliance Radar", detail.title || "Regulation"]);
    await loadRequirementsTab(regId, detail.title || "");
  } catch (e) {
    $("#tab-regulation").innerHTML = `<div class="error-box">Failed to load regulation ${regId}: ${esc(e.message)}</div>`;
  }
}

/* ---------------------------------------------------------------- *
 *  Formfill / generic-crawler sources (dynamic_crawler/formfill) -- *
 *  the ONLY trigger path now (the old direct SBP/SECP/SAMA/CBB      *
 *  MSSQL pipeline trigger was removed as stale). Separate service,  *
 *  Excel-only output -- see api.py's own docstring. Covers MISA,    *
 *  ZATCA, MOH, SDAIA, SIMAH, Tadawul, GOSI, CBB/CBE/CMA/LLOC/MC/     *
 *  MLCU/RERA/SIO's generic-crawler configs, etc. -- everything      *
 *  actually configured in the codebase.                             *
 * ---------------------------------------------------------------- */

const pipelineRunHistory = []; // session-only: {regulator, startedAt, status, message}

function renderPipelineHistory() {
  const el = $("#pipelineHistory");
  if (!pipelineRunHistory.length) {
    el.innerHTML = '<div class="empty-state">No pipelines triggered yet this session.</div>';
    return;
  }
  el.innerHTML = pipelineRunHistory.map((h) => `
    <div class="pipeline-run-row">
      <span class="pr-regulator">${esc(h.regulator)}</span>
      <span class="pr-status-${h.status}">${h.status.toUpperCase()}</span>
      <span class="pr-time">started ${esc(h.startedAt)}</span>
      <span class="pr-msg" title="${esc(h.message || "")}">${esc(h.message || "")}</span>
    </div>
  `).join("");
}

async function checkFormfillHealth() {
  const dot = $("#formfillHealthDot");
  try {
    await formfillGet("/");
    dot.classList.add("ok");
    dot.classList.remove("bad");
    dot.title = "Formfill API connected";
  } catch (e) {
    dot.classList.add("bad");
    dot.classList.remove("ok");
    dot.title = "Formfill API unreachable: " + e.message;
  }
}

// A run only reports new/modified/unchanged relative to whatever's already
// in ITS workbook -- a fresh filename has nothing to compare against, so
// the first run against any workbook always comes back "all new". Reusing
// the same name across runs is what makes "no new updates" reachable at
// all, so each source gets a stable default name instead of a random one.
function defaultFormfillWorkbook(name) {
  return `${name}.xlsx`;
}

async function loadFormfillSources() {
  const sel = $("#formfillSourceSelect");
  sel.innerHTML = '<option value="">Loading&hellip;</option>';
  await checkFormfillHealth();
  try {
    const [forms, sources] = await Promise.all([
      formfillGet("/forms"), formfillGet("/sources"),
    ]);
    const formOpts = Object.entries(forms)
      .map(([name, info]) => `<option value="form:${esc(name)}">${esc(info.regulator || name)} -- ${esc(name)}${info.approved ? "" : " (unapproved)"}</option>`)
      .join("");
    const sourceOpts = Object.entries(sources)
      .filter(([, info]) => !info.error)
      .map(([reg, info]) => `<option value="source:${esc(reg)}">${esc(reg)} (${info.n_sources} source${info.n_sources === 1 ? "" : "s"})${info.disabled ? " (disabled)" : ""}</option>`)
      .join("");
    sel.innerHTML =
      `<optgroup label="Form-shape sources (dynamic_crawler/hints)">${formOpts}</optgroup>` +
      `<optgroup label="Generic-crawler sources (config/sources)">${sourceOpts}</optgroup>`;
    state.formfillListLoaded = true;
    if (sel.value) {
      const [, name] = sel.value.split(/:(.+)/);
      $("#formfillWorkbookInput").value = defaultFormfillWorkbook(name);
    }
  } catch (e) {
    sel.innerHTML = `<option value="">Failed to load: ${esc(e.message)}</option>`;
  }
}

function renderFormfillResult(report) {
  const el = $("#formfillResult");
  const c = report.classified || {};
  const changed = (c.new || 0) + (c.modified || 0) + (c.disappeared || 0);
  const noUpdates = !!report.skipped || changed === 0;

  const summary = noUpdates
    ? `No new updates found for ${esc(report.regulator || "this source")} -- ` +
      `${report.crawled != null ? report.crawled : (c.unchanged || 0)} document(s) checked, all unchanged.`
    : `Found updates for ${esc(report.regulator || "this source")}: ` +
      `${c.new || 0} new, ${c.modified || 0} modified, ${c.disappeared || 0} disappeared ` +
      `(${c.unchanged || 0} unchanged).`;

  el.innerHTML = `
    <div class="ff-outcome ${noUpdates ? "no-updates" : "has-updates"}">
      ${noUpdates ? "&#10003;" : "&#9888;"} ${summary}
      <div class="ff-counts">
        crawled ${report.crawled ?? "?"} in ${report.seconds ?? "?"}s
        ${report.crawl_reused ? " -- reused an existing crawl" : " -- crawled the live site just now"}
        ${report.seeded_from_production_db ? ` -- checked against ${report.seeded_from_production_db} existing record(s) in the production DB` : ""}
        ${report.excel_download ? ` -- <a href="${state.formfillBase.replace(/\/$/, "")}${esc(report.excel_download)}" target="_blank" rel="noopener">download the Excel report</a>` : ""}
      </div>
    </div>
    <details class="ff-details">
      <summary>Full report JSON</summary>
      <pre>${esc(JSON.stringify(report, null, 2))}</pre>
    </details>
  `;
}

$("#formfillSourceSelect").addEventListener("change", () => {
  const val = $("#formfillSourceSelect").value;
  if (!val) return;
  const [kind, name] = val.split(/:(.+)/);
  $("#formfillWorkbookInput").value = defaultFormfillWorkbook(name);
  // The headed-browser option only exists for form-shape sources -- the
  // generic-crawler sources don't use Playwright, so there's no window to show.
  const headedCheckbox = $("#formfillHeaded");
  const isForm = kind === "form";
  headedCheckbox.disabled = !isForm;
  if (!isForm) headedCheckbox.checked = false;
  $("#formfillHeadedLabel").style.opacity = isForm ? "1" : ".5";
});

async function runFormfillTrigger() {
  const sel = $("#formfillSourceSelect");
  const val = sel.value;
  if (!val) return;
  const [kind, name] = val.split(/:(.+)/);
  const btn = $("#runFormfillBtn");
  const reuseLast = $("#formfillReuseLast").checked;
  const headed = $("#formfillHeaded").checked;
  const analyse = $("#formfillAnalyse").checked;
  const checkAgainstDb = $("#formfillCheckDb").checked;
  const workbook = $("#formfillWorkbookInput").value.trim() || undefined;

  const willShowBrowser = kind === "form" && headed && !reuseLast;
  const entry = {
    regulator: name, startedAt: new Date().toLocaleTimeString(), status: "running",
    message: reuseLast ? "replaying last crawl"
      : willShowBrowser ? "crawling live -- a browser window should open on this machine"
      : "crawling the live site -- can take a while",
  };
  pipelineRunHistory.unshift(entry);
  renderPipelineHistory();

  btn.disabled = true;
  $("#formfillResult").innerHTML = `<div class="loading">Running ${esc(name)}&hellip; ` +
    (willShowBrowser ? "a browser window should pop up on this machine any moment -- watch for it."
      : `this crawls the live site if "reuse last crawl" is off, so it can take a while.`) +
    `</div>`;
  try {
    const report = kind === "form"
      ? await formfillPost(`/trigger/${encodeURIComponent(name)}`, { reuse_last: reuseLast, analyse, workbook, headed, check_against_db: checkAgainstDb })
      : await formfillPost(`/trigger/source/${encodeURIComponent(name)}`, { analyse, workbook, check_against_db: checkAgainstDb }); // no reuse_last on this endpoint -- always crawls, no headed browser (no Playwright here)
    renderFormfillResult(report);
    const c = report.classified || {};
    const changed = (c.new || 0) + (c.modified || 0) + (c.disappeared || 0);
    entry.status = "done";
    entry.message = (!!report.skipped || changed === 0)
      ? "no new updates found"
      : `${c.new || 0} new, ${c.modified || 0} modified, ${c.disappeared || 0} disappeared`;
  } catch (e) {
    entry.status = "error";
    entry.message = e.message;
    $("#formfillResult").innerHTML = `<div class="error-box">Run failed: ${esc(e.message)}</div>`;
  }
  renderPipelineHistory();
  btn.disabled = false;
}

$("#runFormfillBtn").addEventListener("click", runFormfillTrigger);
$("#refreshFormfillListBtn").addEventListener("click", loadFormfillSources);

const formfillBaseInput = $("#formfillBaseInput");
formfillBaseInput.value = state.formfillBase;
formfillBaseInput.addEventListener("change", () => {
  state.formfillBase = formfillBaseInput.value.trim() || DEFAULT_FORMFILL_BASE;
  localStorage.setItem("rcl_formfill_base", state.formfillBase);
  state.formfillListLoaded = false;
  loadFormfillSources();
});

/* ---------------------------------------------------------------- *
 *  Alerts view                                                      *
 * ---------------------------------------------------------------- */

async function loadAlerts() {
  const box = $("#alertsResult");
  box.innerHTML = '<div class="muted-note">Checking&hellip;</div>';
  try {
    const r = await api("/monitoring/staleness");
    $("#alertsCheckedAt").textContent = "Checked " + new Date(r.checked_at).toLocaleString();
    if (!r.stale.length) {
      box.innerHTML = '<div class="ok-banner">Every regulator is within its interval.</div>';
      return;
    }
    box.innerHTML = r.stale.map(x => {
      const when = x.last_update
        ? `${x.days_since} days ago (${esc(x.last_update.slice(0, 10))})`
        : "never updated";
      return `<div class="alert-row">
        <span class="alert-name">${esc(x.regulator)}</span>
        <span>${when}</span>
        <span class="alert-meta">allowed ${esc(String(x.limit_days))} days</span></div>`;
    }).join("");
  } catch (e) {
    box.innerHTML = `<div class="err-banner">Could not load alerts: ${esc(e.message)}</div>`;
  }
}

/* ---------------------------------------------------------------- *
 *  Model & usage view                                               *
 * ---------------------------------------------------------------- */

function fillModelSelect() {
  const q = $("#modelSearchInput").value.trim().toLowerCase();
  const list = state.modelList.filter(m =>
    !q || m.id.toLowerCase().includes(q) || (m.name || "").toLowerCase().includes(q));
  const per1m = v => (v == null || v === "" ? "?" : "$" + (parseFloat(v) * 1e6).toFixed(2));
  $("#modelSelect").innerHTML = list.slice(0, 300).map(m =>
    `<option value="${esc(m.id)}"${m.id === state.currentModel ? " selected" : ""}>` +
    `${esc(m.id)}  (in ${per1m(m.prompt_price)} / out ${per1m(m.completion_price)} per 1M)</option>`
  ).join("") || '<option value="">No matches</option>';
}

async function loadModelPanel() {
  const msg = $("#modelMsg");
  msg.textContent = "";
  try {
    state.currentModel = (await api("/llm/settings")).model;
    $("#currentModel").textContent = state.currentModel;
  } catch (e) {
    msg.textContent = "Could not read current model: " + e.message;
  }
  try {
    if (!state.modelList.length) state.modelList = (await api("/llm/models")).models;
    fillModelSelect();
  } catch (e) {
    msg.textContent = "Could not load OpenRouter model list: " + e.message;
  }
  loadUsage();
}

async function saveModel() {
  const id = $("#modelSelect").value;
  const msg = $("#modelMsg");
  if (!id) return;
  const btn = $("#saveModelBtn");
  btn.disabled = true;
  try {
    const r = await apiPut("/llm/settings", { model: id });
    state.currentModel = r.model;
    $("#currentModel").textContent = r.model;
    msg.textContent = "Saved. " + (r.note || "");
  } catch (e) {
    msg.textContent = "Not saved: " + e.message;
  } finally {
    btn.disabled = false;
  }
}

async function loadUsage() {
  const box = $("#usageResult");
  box.innerHTML = '<div class="muted-note">Loading&hellip;</div>';
  try {
    const group = $("#usageGroupSelect").value;
    const u = await api("/llm/usage", { group_by: group });
    const usd = v => (v == null ? "n/a" : "$" + Number(v).toFixed(4));
    const num = v => Number(v || 0).toLocaleString();
    const tile = ([k, v]) =>
      `<div class="usage-tile"><div class="v">${esc(String(v))}</div><div class="k">${esc(k)}</div></div>`;

    // --- this app only
    const uc = u.uc, t = uc.total;
    let html = '<div class="section-label" style="margin-top:6px;">This app</div>';
    html += '<div class="usage-grid">' + [
      ["Calls", num(t.calls)], ["Prompt tokens", num(t.prompt_tokens)],
      ["Completion tokens", num(t.completion_tokens)], ["Cached tokens", num(t.cached_tokens)],
      ["Cost", usd(t.cost_usd)],
    ].map(tile).join("") + "</div>";
    html += `<div class="muted-note" style="margin:6px 0;">${uc.tracking_since
      ? "Recorded since " + esc(new Date(uc.tracking_since + "Z").toLocaleString())
      : "Nothing recorded yet -- usage appears after the next analysis."}</div>`;
    if (uc.rows.length) {
      html += `<table class="usage-table"><thead><tr><th>${esc(group)}</th><th>Calls</th>
        <th>Prompt</th><th>Completion</th><th>Cached</th><th>Cost</th></tr></thead><tbody>` +
        uc.rows.map(r => `<tr><td>${esc(r.group == null ? "(none)" : String(r.group))}</td>
          <td>${num(r.calls)}</td><td>${num(r.prompt_tokens)}</td><td>${num(r.completion_tokens)}</td>
          <td>${num(r.cached_tokens)}</td><td>${usd(r.cost_usd)}</td></tr>`).join("") +
        "</tbody></table>";
    }

    // --- whole key, per OpenRouter
    const o = u.openrouter || {};
    const tiles = [];
    if (o.key) {
      tiles.push(["Key total", usd(o.key.usage)], ["Today", usd(o.key.usage_daily)],
                 ["This week", usd(o.key.usage_weekly)], ["This month", usd(o.key.usage_monthly)]);
      if (o.key.limit != null) tiles.push(["Key limit", usd(o.key.limit)]);
    }
    if (o.credits) {
      tiles.push(["Credits bought", usd(o.credits.total_credits)],
                 ["Credits used", usd(o.credits.total_usage)]);
    }
    html += '<div class="section-label" style="margin-top:18px;">Whole API key (OpenRouter)</div>';
    html += tiles.length ? '<div class="usage-grid">' + tiles.map(tile).join("") + "</div>" : "";
    html += Object.entries(o.errors || {}).map(([k, v]) =>
      `<div class="muted-note" style="margin-top:8px;">${esc(k)} unavailable: ${esc(v)}</div>`).join("");
    box.innerHTML = html;
  } catch (e) {
    box.innerHTML = `<div class="err-banner">Could not load usage: ${esc(e.message)}</div>`;
  }
}

$("#refreshAlertsBtn").addEventListener("click", loadAlerts);
$("#saveModelBtn").addEventListener("click", saveModel);
$("#refreshUsageBtn").addEventListener("click", loadUsage);
$("#usageGroupSelect").addEventListener("change", loadUsage);
$("#modelSearchInput").addEventListener("input", fillModelSelect);

/* ---------------------------------------------------------------- *
 *  Mode switching                                                   *
 * ---------------------------------------------------------------- */

const OTHER_MODES = {
  pipeline: { nav: "#navPipelineBtn", view: "#pipelineView", title: "RUN PIPELINE", crumb: "Run Pipeline" },
  alerts:   { nav: "#navAlertsBtn",   view: "#alertsView",   title: "ALERTS", crumb: "Alerts" },
  model:    { nav: "#navModelBtn",    view: "#modelView",    title: "MODEL & USAGE", crumb: "Model & Usage" },
};

function setMode(mode) {
  state.mode = mode;
  $("#navLibraryBtn").classList.toggle("active", mode === "library");
  for (const [name, m] of Object.entries(OTHER_MODES)) {
    $(m.nav).classList.toggle("active", mode === name);
    $(m.view).classList.toggle("hidden", mode !== name);
  }

  if (mode !== "library") {
    const m = OTHER_MODES[mode];
    $("#browseView").classList.add("hidden");
    $("#detailView").classList.add("hidden");
    $("#backBtn").classList.add("hidden");
    $("#sidebar").style.display = "none";
    $("#pageTitle").textContent = m.title;
    setCrumbs(["Compliance Radar", m.crumb]);
    if (mode === "pipeline" && !state.formfillListLoaded) loadFormfillSources();
    if (mode === "alerts") loadAlerts();
    if (mode === "model") loadModelPanel();
  } else {
    // Restore whichever library view (browse or detail) was active before.
    if (state.selectedRegulationId !== null) {
      $("#detailView").classList.remove("hidden");
      $("#backBtn").classList.remove("hidden");
    } else {
      $("#browseView").classList.remove("hidden");
      $("#backBtn").classList.toggle("hidden", state.folderPath.length === 0);
    }
    updateSidebarVisibility();
  }
}

$("#navLibraryBtn").addEventListener("click", () => setMode("library"));
$("#navPipelineBtn").addEventListener("click", () => setMode("pipeline"));
$("#navAlertsBtn").addEventListener("click", () => setMode("alerts"));
$("#navModelBtn").addEventListener("click", () => setMode("model"));

/* ---------------------------------------------------------------- *
 *  Chrome: sidebar toggle, api base input                           *
 * ---------------------------------------------------------------- */

$("#sidebarToggle").addEventListener("click", () => {
  state.sidebarManuallyHidden = !state.sidebarManuallyHidden;
  updateSidebarVisibility();
});

const apiInput = $("#apiBaseInput");
apiInput.value = state.apiBase;
apiInput.addEventListener("change", () => {
  state.apiBase = apiInput.value.trim() || DEFAULT_API_BASE;
  localStorage.setItem("rcl_api_base", state.apiBase);
  checkHealth();
  state.folderPath = [];
  renderBrowse(null);
  renderTreeRoots();
});

/* ---------------------------------------------------------------- *
 *  Boot                                                             *
 * ---------------------------------------------------------------- */

checkHealth();
renderTreeRoots();
renderBrowse(null);
