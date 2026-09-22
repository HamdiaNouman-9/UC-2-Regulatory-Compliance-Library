"""THE EXECUTOR — our code, driven by the form. No LLM anywhere in this file.

This module deliberately does not import `propose.py`, and must not grow an
import of it. The whole safety argument rests on the crawl being deterministic:
a hints file is read from disk, and the same file produces the same walk every
time. Ask a model afresh on each run and two runs can pick different rows — then
change detection reports hundreds of documents as "disappeared" and the
monitoring you built the library for is worthless.

TWO PHASES, separable on purpose (same split as generic_crawler's list strategy):

  PHASE 1  the listing only. Harvest each row's link plus the fields the form
           names — reference number, date, department. On SBP that is 139 pages
           and ~20 minutes for a COMPLETE inventory of 4,160 circulars, and it
           produces structured metadata a link-walk can never recover.

  PHASE 2  open each entry for its HTML. On SBP that is 4,160 more page loads.

Phase 1 alone answers "what is new since last time" — the listing IS the change
feed — so phase 2 only ever has to run for rows that are actually new.

OUTPUT SCHEMA
    Identical to generic_crawler's `pages.json` (records + documents), so a run
    from here is directly comparable with a run from there, and the orchestrator
    adapter cannot tell which one produced it.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

from dynamic_crawler.formfill.schema import compile_field_regexes
from generic_crawler.crawler import (GENERIC_LINK_TEXT, disambiguate_titles,
                                     title_from_slug)

# "Press Here" is GOSI's; the rest come from the shared list.
_GENERIC_TEXT = GENERIC_LINK_TEXT | {"press here", "here", "click", "link"}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

_DOC_EXT_RE = re.compile(r"\.(pdf|docx?|xlsx?|pptx?|csv|zip)(\?|$)", re.I)

# A WAF challenge page is HTML, returns 200, and has a plausible amount of text.
# Nothing about it looks like a failure — SIMAH's Cloudflare block was stored as
# 1,054 characters of "law", passed the fill-rate check, and would have been
# indexed into the library as the Credit Information Law. Any regulator can put a
# WAF in front of their site, so this is checked on every page.
_BLOCK_RE = re.compile(
    r"(you have been blocked|attention required|cf-error-details|just a moment"
    r"|checking your browser|access denied|error 10\d\d|ddos protection"
    r"|请稍候|captcha-bypass|verify you are (a )?human)", re.I)


def _blocked(page) -> str:
    """Return a reason string when the page is a bot-protection wall, else ''."""
    try:
        probe = page.evaluate(
            "()=>({t: document.title || '',"
            "      x: (document.body ? document.body.innerText : '').slice(0, 3000)})")
    except Exception:
        return ""
    m = _BLOCK_RE.search(f"{probe['t']}\n{probe['x']}")
    return m.group(0)[:60] if m else ""


def emit(event: dict) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def content_key(text: str) -> str:
    """Same definition as generic_crawler.crawler.content_key, so hashes from the
    two engines are comparable."""
    norm = re.sub(r"\s+", " ", (text or "")).strip().lower()
    return hashlib.md5(norm.encode("utf-8")).hexdigest() if norm else ""


# --------------------------------------------------------------------------- #
# Extraction runs in the page: one evaluate per listing page, not one call per
# row. On a 30-row page that is the difference between 1 round trip and 150.
# --------------------------------------------------------------------------- #

# Whether JS_ROWS returns EVERY link in a row or only the first. Set once per
# run() from `attachment_is_document: "combined"`, and read inside _harvest,
# which has five call sites that have no other reason to know about it.
#
# A dict rather than a bare bool so it is rebindable from the nested scope
# without `global`, matching the PACE pattern used elsewhere in this codebase.
COLLECT_ROW_LINKS = {"on": False}

JS_ROWS = r"""(cfg) => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  // innerText returns "" for anything inside a hidden tab, because it reports
  // RENDERED text. MISA's "Basic Legislations" pane is hidden until clicked, so
  // reading innerText alone silently blanked every title and section name in it
  // — a wrong answer that looks like a legitimately empty field. textContent
  // does not care about rendering, so it is the fallback.
  const txt = el => clean(el.innerText || el.textContent);
  const out = [];
  // Rows are searched inside one panel when the form declares `panels`, so the
  // same selector yields a different row set per instrument on a tabbed page.
  let root = document;
  if (cfg.rootSel) {
    root = document.querySelector(cfg.rootSel);
    if (!root) return { rows: [], matched: 0 };
  }
  const rows = Array.from(root.querySelectorAll(cfg.rowSel));
  for (let ri = 0; ri < rows.length; ri++) {
    const row = rows[ri];
    // A row inside a panel has no URL of its own, so it is addressed by a stamp
    // and re-read from the same document in phase 2.
    let ffRow = null;
    if (cfg.stampBase !== null && cfg.stampBase !== undefined) {
      ffRow = cfg.stampBase + ri;
      row.setAttribute('data-ff-row', String(ffRow));
    }
    // The link that opens this entry.
    let a = null;
    if (cfg.linkSel) a = row.querySelector(cfg.linkSel) || (row.matches(cfg.linkSel) ? row : null);
    if (!a) a = row.matches('a[href]') ? row : row.querySelector('a[href]');

    // Where this row sits in the site's own hierarchy. Each level walks UP to
    // the nearest matching ancestor and reads a title inside it, so one listing
    // page can yield many different section paths — which is the point: the row
    // knows which sector block it lives in, the page as a whole does not.
    const section = [];
    for (const lv of (cfg.sectionLevels || [])) {
      // Two ways a page marks a group.
      //   ancestor+title — the rows sit INSIDE a block that names itself (MISA)
      //   preceding      — a heading sits BEFORE the rows as a sibling and the
      //                    rows are not wrapped at all (aml.gov.sa's two <h3>s,
      //                    and most SharePoint pages)
      if (lv.preceding) {
        let best = '';
        let hs = [];
        try { hs = Array.from(document.querySelectorAll(lv.preceding)); } catch (e) { hs = []; }
        for (const h of hs) {
          // 4 = DOCUMENT_POSITION_FOLLOWING: the row comes after this heading.
          if (h.compareDocumentPosition(row) & 4) best = txt(h);
        }
        section.push(best.slice(0, 120));
        continue;
      }
      let anc = null;
      try { anc = row.closest(lv.ancestor); } catch (e) { anc = null; }
      if (!anc) { section.push(''); continue; }
      let t = null;
      try { t = anc.querySelector(lv.title); } catch (e) { t = null; }
      section.push(t ? txt(t).slice(0, 120) : '');
    }
    // The panel is the outermost level: "…> OH benefits Regulation > SECTION VI".
    if (cfg.rootLabel) section.unshift(cfg.rootLabel);

    const fields = {};
    for (const f of cfg.cssFields) {
      let el = null;
      try {
        if (f.whereText) {
          // Pick the match whose own text matches. Bilingual rows (Tadawul:
          // "Arabic" and "English" side by side) are otherwise indistinguishable
          // — same tag, no class — and choosing by POSITION would silently swap
          // the languages on any row that offered only one of them.
          const rx = new RegExp(f.whereText, 'i');
          const all = Array.from(row.querySelectorAll(f.selector));
          el = all.find(x => rx.test(txt(x))) || null;
        } else {
          el = row.querySelector(f.selector) || (row.matches(f.selector) ? row : null);
        }
      }
      catch (e) { el = null; }                 // an invalid selector yields nothing, never throws
      if (!el) { fields[f.target] = ''; continue; }
      if (f.attr === 'text')      fields[f.target] = txt(el);
      else if (f.attr === 'href') fields[f.target] = el.href || '';
      else if (f.attr === 'src')  fields[f.target] = el.src || '';
      else                        fields[f.target] = el.getAttribute(f.attr) || '';
    }
    // EVERY link in the row, not just the first — only when the form asks.
    //
    // A row is normally one document, so `a` above is the whole answer. But a
    // card that groups an instrument with its implementing regulation carries
    // several PDFs, and taking the first silently drops the rest: SDAIA's 36
    // files collapse to 29 that way, and the result looks complete.
    //
    // Off by default. Reading every link for a listing that genuinely has one
    // per row would turn sibling navigation links into phantom documents.
    let allLinks = [];
    if (cfg.collectLinks) {
      let els = [];
      try { els = Array.from(row.querySelectorAll(cfg.linkSel || 'a[href]')); }
      catch (e) { els = []; }
      const seenHref = new Set();
      for (const el of els) {
        const h = el.href || '';
        if (!h || seenHref.has(h)) continue;
        seenHref.add(h);
        allLinks.push({ href: h, text: txt(el).slice(0, 300) });
      }
    }
    out.push({
      row_text: txt(row).slice(0, 4000),
      href: a ? a.href : '',
      link_text: a ? txt(a).slice(0, 400) : '',
      all_links: allLinks,
      section,
      ff_row: ffRow,
      fields
    });
  }
  return { rows: out, matched: rows.length };
}"""

# --------------------------------------------------------------------------- #
# PANELS — one URL holding several documents in fragment-addressed tabs
#
# A tab is <a href="#2">; the panel <div id="2"> is ALREADY in the DOM. Nothing
# navigates, so a link walk never follows one, and picking the longest
# main-content candidate keeps exactly one panel. GOSI's Social Insurance page is
# six separate legal instruments this way.
# --------------------------------------------------------------------------- #

JS_PANELS = r"""(sel) => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  let tabs = [];
  try { tabs = Array.from(document.querySelectorAll(sel)); } catch (e) { return []; }
  const out = [];
  for (const t of tabs) {
    const a = t.matches('a[href]') ? t : t.querySelector('a[href]');
    if (!a) continue;
    const frag = a.getAttribute('href') || '';
    if (frag.length < 2 || frag[0] !== '#') continue;
    let panel = null;
    try { panel = document.getElementById(decodeURIComponent(frag.slice(1))); } catch (e) {}
    // Two tabs pointing at one panel would double every row inside it.
    if (!panel || panel.hasAttribute('data-ff-panel')) continue;
    const i = out.length;
    panel.setAttribute('data-ff-panel', String(i));
    out.push({
      index: i,
      fragment: frag,
      label: clean(a.innerText || a.textContent).slice(0, 300),
      // textContent, never innerText — see _check_panels in schema.py.
      text_len: clean(panel.textContent).length,
      links: panel.querySelectorAll('a[href]').length
    });
  }
  return out;
}"""

# Content of one stamped element, for rows that live inside the page rather than
# at a URL of their own. textContent for the same reason as JS_DETAIL's fallback:
# a collapsed accordion renders as its headings and nothing else.
JS_IN_PAGE_DETAIL = r"""(sel) => {
  const el = document.querySelector(sel);
  if (!el) return null;
  const clone = el.cloneNode(true);
  clone.querySelectorAll('script,style,noscript').forEach(n => n.remove());
  // `ctx` is the nearest heading above the link. A PDF inside a panel is usually
  // anchored by "Press Here", and the heading is the document's real name.
  const ctxOf = a => {
    let n = a;
    while (n && n !== el) {
      let s = n.previousElementSibling;
      while (s) {
        if (/^H[1-6]$/.test(s.tagName)) return (s.textContent || '').replace(/\s+/g,' ').trim();
        s = s.previousElementSibling;
      }
      n = n.parentElement;
    }
    return '';
  };
  const links = Array.from(el.querySelectorAll('a[href]'))
    .map(a => ({href: a.href,
                text: (a.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 200),
                ctx: ctxOf(a).slice(0, 200)}));
  return {html: clone.innerHTML, text: (clone.textContent || '').trim(), links};
}"""

JS_DETAIL = r"""([strip, extract]) => {
  // querySelector with a comma list returns the first match in DOCUMENT ORDER, not
  // the first selector that matches: hrsd.gov.sa's stray <div class="content"> (7
  // chars) beat <main class="main-content"> (3,639). Take the LONGEST candidate;
  // empty ones are skipped so a page with no container still falls back to <body>.
  // Platform conventions belong here, the same way [id^="book-navigation"] encodes
  // Drupal: DeltaPlaceHolderMain is SharePoint's main-content placeholder. On
  // SIMAH it holds the law and nothing else (9,016 chars) while the next container
  // up adds the ribbon, "Sign In" and the footer (29,555).
  const SELS = ['main', '[role="main"]', 'article', '#content', '.content', '#main',
                '[id^="DeltaPlaceHolderMain"]', '.article-content'];
  let pick = null, best = 0;
  for (const sel of SELS) {
    for (const el of document.querySelectorAll(sel)) {
      const n = (el.innerText || '').trim().length;
      if (n > best) { best = n; pick = el; }
    }
  }
  // SECOND PASS, only when nothing scored: innerText reports RENDERED text, so a
  // page whose content sits in collapsed panels scores zero everywhere. SIMAH's law
  // is 17 articles in an EXCLUSIVE bootstrap accordion (data-bs-parent), so at most
  // one can be open at a time and clicking cannot defeat that — innerText sees one
  // article of seventeen. textContent does not care about rendering. Kept as a
  // fallback rather than the default so that on a normal page the visible main
  // content still wins, exactly as before.
  if (!pick) {
    for (const sel of SELS) {
      for (const el of document.querySelectorAll(sel)) {
        const n = (el.textContent || '').trim().length;
        if (n > best) { best = n; pick = el; }
      }
    }
  }
  const src = pick || document.body || document.documentElement;
  if (!src) return {html:'', text:'', links:[]};
  const clone = src.cloneNode(true);
  // iframe/object/embed are removed EVERYWHERE, for every site.
  //
  // Their contents belong to another document and are never captured, so what
  // survives into document_html is an empty element that renders as a bare
  // 300x150 bordered box at the end of the regulation. ZATCA carried one on all
  // 32 of its html rows (2026-08-14). An inline `display: none` does not save
  // us: anything that reads the stored html without applying inline styles —
  // a preview pane, a paste into a document — shows the box anyway.
  //
  // Nothing is lost. If an embed ever IS the instrument, it has to be captured
  // by following its src, which this never did.
  clone.querySelectorAll('script,style,noscript,nav,aside,header,footer,iframe,object,embed').forEach(n=>n.remove());
  // <form> is UNWRAPPED, not removed. SharePoint / ASP.NET WebForms wrap the entire
  // page in <form id="aspnetForm">, so removing forms deleted SIMAH's whole law:
  // 8,182 characters of articles became 0, with a 444-character husk of markup left
  // behind and every other check still passing. Unwrapping drops the form semantics
  // and keeps the content.
  clone.querySelectorAll('form').forEach(f => {
    while (f.firstChild) f.parentNode.insertBefore(f.firstChild, f);
    f.remove();
  });
  // THE PRICE OF UNWRAPPING THE FORM, PAID BACK.
  //
  // Unwrapping keeps everything the <form> contained — including ASP.NET's own
  // plumbing. On SharePoint that is __VIEWSTATE, __REQUESTDIGEST, __EVENTTARGET
  // and a wall of MSO* hidden inputs inside div.aspNetHidden. Measured on ZATCA
  // 2026-08-13: 178,991 characters of stored HTML carrying 4,111 characters of
  // text, nearly all of it scaffolding.
  //
  // A hidden input is never document content on any site, so this is safe to do
  // unconditionally. VISIBLE controls are left alone — a form deliberately
  // unwrapped may legitimately contain them.
  clone.querySelectorAll('input[type="hidden"], .aspNetHidden').forEach(n => n.remove());
  // The form's content.strip: the regulator's own furniture around the
  // document. Removed from the CLONE only, and after the link harvest
  // below reads `src`, so stripping the "Download Original PDF" button
  // still leaves its href for org_pdf_link.
  for (const sel of (strip || [])) {
    try { clone.querySelectorAll(sel).forEach(n => n.remove()); } catch (e) {}
  }
  // content.extract: LIFTED OUT rather than deleted. The block is removed from the
  // stored document_html — so what remains is the instrument itself — and its own
  // markup is handed back to be filed in extra_meta under the key that named it.
  //
  // MHRSD is the case: 68-92% of a detail page's text is an eight-card "related
  // regulations" view (config/change_signals.yml measured it, and cuts its confirm
  // hash at the same block). Left in, that block IS the document as far as any
  // reader or analyser is concerned, and it changes whenever the ministry
  // publishes anything at all.
  const extracted = {};
  for (const key of Object.keys(extract || {})) {
    const spec = extract[key];
    const sel  = (typeof spec === 'string') ? spec : (spec && spec.selector);
    const pairs = (typeof spec === 'string') ? null : (spec && spec.pairs);
    // `text: true` — store the block's TEXT, not its markup. For a one-value
    // block (ZATCA's "Last Update: 25 Feb 2026 02:25 PM Saudi Arabia Time")
    // outerHTML would put a div, a container class and a <p> into extra_meta
    // around a single date, which is no more readable than leaving it in the
    // document. Neither markup nor label/value pairs fit a lone value.
    const asText = (typeof spec === 'string') ? false : !!(spec && spec.text);
    if (!sel) continue;
    let nodes = [];
    try { nodes = Array.from(clone.querySelectorAll(sel)); } catch (e) { continue; }
    if (!nodes.length) continue;

    if (pairs && pairs.label && pairs.value) {
      // A LABELLED BLOCK, read as data rather than kept as markup. MHRSD's
      // sidebar is Drupal field markup — .field__label / .field__item — holding
      // "Resolution Number 137440", "Release Date 23-Shawwal-1446-21-April-2025"
      // and so on. Storing that as a slab of HTML in extra_meta would be no more
      // readable than leaving it in the document; storing it as pairs makes each
      // value addressable.
      const got = {};
      nodes.forEach(n => {
        let labels = [];
        try { labels = Array.from(n.querySelectorAll(pairs.label)); } catch (e) {}
        labels.forEach(lab => {
          const name = (lab.textContent || '').replace(/\s+/g, ' ').trim();
          if (!name) return;
          // The value is the labelled sibling, or the nearest one inside the
          // same field wrapper — Drupal nests them together.
          // EVERY value under this label, not the first. A label can head a
          // LIST: MHRSD's "Beneficiaries" is seven separate .field__item nodes
          // (Businessmen, Individuals, Job seekers, localization, Women, recent
          // graduates, youths), and taking querySelector alone stored just
          // "Businessmen ," — a truncation that looks like a complete value.
          let holder = lab.parentElement, vals = [];
          for (let i = 0; i < 3 && holder && !vals.length; i++) {
            try { vals = Array.from(holder.querySelectorAll(pairs.value)); } catch (e) {}
            if (!vals.length) holder = holder.parentElement;
          }
          const parts = vals
            .map(v => (v.textContent || '').replace(/\s+/g, ' ').trim())
            // The site prints the separator INSIDE each item ("Businessmen ,"),
            // so it has to come off or every value carries a trailing comma.
            .map(v => v.replace(/^[,;|،\s]+/, '').replace(/[,;|،\s]+$/, ''))
            .filter(Boolean)
            // A wrapper and its child both match, so the same text arrives twice.
            .filter((v, i, a) => a.indexOf(v) === i)
            // Separator-only nodes: the site renders the commas as their own elements
            .filter(v => !/^[,;|،\s]+$/.test(v));
          const text = parts.join(' | ');
          if (text) got[name] = text;
        });
      });
      if (Object.keys(got).length) extracted[key] = got;
    } else {
      extracted[key] = asText
        ? nodes.map(n => (n.innerText || n.textContent || '')
                          .replace(/\s+/g, ' ').trim())
               .filter(Boolean).join(' | ')
        : nodes.map(n => n.outerHTML).join('');
    }
    nodes.forEach(n => n.remove());
  }
  // A LINK INSIDE A STRIPPED REGION IS NOT AN ATTACHMENT.
  //
  // `strip` was applied to the clone only, so it shaped document_html and left
  // the link harvest untouched — and the harvest is what becomes pdf_docs. If a
  // form declared a block "not the content", a PDF inside it is site chrome, not
  // this instrument's file.
  //
  // Measured on ZATCA 2026-08-14: one footer link (ZATCA-Glossary.pdf, inside
  // div#footerContent.footer-main) and one megamenu copy of it (a.dropdown-item
  // inside div.dropdown-menu) attached themselves to 32 of 34 regulations.
  // Neither sits in a semantic <footer> or <nav>, so the tag filter below could
  // not see them. On the 8 pages that have no real attachment of their own the
  // Glossary became the whole of document_url, giving 8 different regulations
  // one identical url.
  const stripSel = (strip || []).filter(Boolean).join(', ');
  const links = Array.from(src.querySelectorAll('a[href]'))
    .filter(a => !a.closest('header, footer, nav, [role="banner"], [role="contentinfo"]'))
    .filter(a => !stripSel || !a.closest(stripSel))
    .map(a => ({href:a.href, text:(a.innerText||'').replace(/\s+/g,' ').trim().slice(0,200)}));
  // The clone is detached, so it has no layout and innerText is unreliable on it —
  // another reason textContent has to be the fallback here.
  // Returned as-is. Tidying happens in Python (_tidy_html), never here:
  // an escape sequence written into this snippet through a heredoc can
  // arrive as a REAL control character, which silently turns the whole
  // thing into a syntax error. Measured twice on 2026-08-13.
  return {html: clone.innerHTML,
          text:(clone.innerText || clone.textContent || '').trim(), links, extracted};
}"""

JS_HREFS = "() => Array.from(document.querySelectorAll('a[href]')).map(a => a.href)"

JS_CRUMBS = r"""(sel) => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  let els = [];
  try { els = Array.from(document.querySelectorAll(sel)); } catch (e) { return []; }
  return els.map(e => clean(e.innerText || e.textContent)).filter(Boolean);
}"""

# --------------------------------------------------------------------------- #
# TREE — a nested menu instead of a paginated listing
#
# The menu IS the index: every node is one document page, and the nesting is the
# section path. So a tree reuses everything the list path already does — the
# nodes become the rows, phase 2 opens them exactly as it opens list entries, and
# the same verify gate applies. Only the way rows are DISCOVERED differs.
# --------------------------------------------------------------------------- #

JS_EXPAND = r"""(sel) => {
  let clicked = 0;
  for (const el of document.querySelectorAll(sel)) {
    try { el.click(); clicked++; } catch (e) {}
  }
  return clicked;
}"""

# --- click pagination -------------------------------------------------------
# A JS pager offers no URL to plan with (MHRSD: <a href="#">, and ?page=N there
# silently returns page 1). So it is walked by clicking, under the same rule as
# generic_crawler's reveal_all_links(): CLICK AND VERIFY — a click counts as a
# page turn only if the ROW SET changed. Without that, a dead control re-reads
# page 1 forever and the run reports a clean multiple of the real count.

# Fingerprint of the row set: count + each row's link. Cheap to poll, and page 2
# cannot look like page 1.
JS_ROW_SIG = r"""(a) => {
  const rows = Array.from(document.querySelectorAll(a.rowSel));
  return rows.length + '|' + rows.map(r => {
    const l = r.querySelector(a.linkSel);
    return (l && l.getAttribute('href')) || (r.innerText || '').slice(0, 40);
  }).join('~');
}"""

# Some pagers keep "next" on the last page, hidden or disabled — existing is not
# enough. el.click() rather than a pointer click: the pager sits below the fold
# and these sites float chat widgets over it.
JS_CLICK_NEXT = r"""(sel) => {
  const usable = e => {
    const r = e.getBoundingClientRect();
    if (r.width === 0 && r.height === 0) return false;
    const st = getComputedStyle(e);
    if (st.display === 'none' || st.visibility === 'hidden') return false;
    if (e.disabled || e.getAttribute('aria-disabled') === 'true') return false;
    return !/\bdisabled\b/.test(e.className || '');
  };
  const el = Array.from(document.querySelectorAll(sel)).find(usable);
  if (!el) return 'no-control';
  try { el.scrollIntoView({block: 'center'}); } catch (e) {}
  try { el.click(); } catch (e) { return 'click-failed'; }
  return 'clicked';
}"""

CLICK_SETTLE_MS = 8000    # how long a click gets to change the rows
CLICK_POLL_MS = 250

JS_TREE = r"""(cfg) => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  const txt = el => clean(el.innerText || el.textContent);
  const out = [];
  for (const menu of document.querySelectorAll(cfg.menuSel)) {
    for (const node of menu.querySelectorAll(cfg.nodeSel)) {
      const a = node.querySelector(cfg.linkSel);
      if (!a || !a.href) continue;
      // The section path of a tree node is simply its ancestor nodes' labels.
      // Walk up to the menu root collecting them — "Regulatory Sandbox
      // Framework > Stage 1" falls out of the nesting, no selectors needed.
      const trail = [];
      let p = node.parentElement;
      while (p && p !== menu.parentElement) {
        if (p.matches && p.matches(cfg.nodeSel)) {
          const pa = p.querySelector(cfg.linkSel);
          if (pa) trail.unshift(txt(pa).slice(0, 120));
        }
        p = p.parentElement;
      }
      out.push({
        href: a.href,
        title: txt(a).slice(0, 300),
        trail: trail,
        depth: trail.length,
        // A node that has children is a folder as well as a page. Kept so a
        // reviewer can see the shape of what was found.
        children: node.querySelectorAll(cfg.nodeSel).length
      });
    }
  }
  return out;
}"""


# --------------------------------------------------------------------------- #
# PAGINATION — turn the form's pattern into the full page sequence
# --------------------------------------------------------------------------- #

def _pattern_regex(pattern: str, token: str) -> re.Pattern:
    """'https://x/P{offset}' -> regex matching https://x/P<digits>."""
    left, right = pattern.split(token, 1)
    return re.compile(re.escape(left) + r"(\d+)" + re.escape(right) + r"$")


def plan_pages(pagination: dict, seed_url: str, discovered_hrefs: list[str]) -> tuple[list[str], dict]:
    """Return (list of listing-page URLs, a note about how the plan was decided).

    `max_offset` in the form freezes the plan. When it is absent we discover the
    last page from the pager links on page 1 — which works, but is a number that
    can move between runs, so we report it and verify.py flags the difference.
    """
    mode = pagination.get("mode", "none")
    max_pages = int(pagination.get("max_pages", 200))
    note: dict = {"mode": mode, "frozen": False, "discovered_max": None}

    if mode == "none" or mode == "click":
        return [seed_url], note

    token = "{offset}" if mode == "url_offset" else "{page}"
    pattern = pagination["pattern"]
    step = int(pagination.get("step", 1))
    rx = _pattern_regex(pattern, token)

    declared = pagination.get("max_offset")
    if declared:
        last = int(declared)
        note["frozen"] = True
    else:
        nums = [int(m.group(1)) for h in discovered_hrefs if (m := rx.match(h))]
        last = max(nums) if nums else 0
        note["discovered_max"] = last

    urls = [seed_url]
    if last >= step:
        urls += [pattern.replace(token, str(n)) for n in range(step, last + 1, step)]
    # De-dupe in case the seed is itself the first pattern URL.
    wanted = len(dict.fromkeys(urls))
    urls = list(dict.fromkeys(urls))[:max_pages]
    note["planned_pages"] = len(urls)
    note["max_pages"] = max_pages
    # True only when the cap actually cut the plan short — a plan of 3 pages under
    # a cap of 200 is not capped, and saying so would train people to ignore the
    # warning that matters.
    note["capped_by_max_pages"] = wanted > max_pages
    note["pages_wanted"] = wanted
    return urls, note


# --------------------------------------------------------------------------- #
# THE RUN
# --------------------------------------------------------------------------- #

JS_PAGE_SIZE = r"""(cfg) => {
  const sel = document.querySelector(cfg.selector);
  if (!sel || !sel.options) return {ok: false, reason: 'select not found'};
  const want = String(cfg.value).trim().toLowerCase();
  const opt = Array.from(sel.options).find(
    o => (o.text || '').trim().toLowerCase() === want);
  if (!opt) return {ok: false, reason: 'option not found',
                    options: Array.from(sel.options).map(o => (o.text || '').trim())};
  sel.value = opt.value;
  // A plain .value assignment does not notify the table — DataTables listens for
  // a change event, so without this the page still shows ten rows.
  sel.dispatchEvent(new Event('change', {bubbles: true}));
  return {ok: true, chosen: (opt.text || '').trim()};
}"""


def _set_page_size(page, cfg: dict) -> dict:
    """Pick an option in a 'Show N entries' select, then wait for the redraw.

    Worth 69 page loads on SAMA circulars: 685 entries at 10 per page becomes one
    page at "All". A <select> cannot be handled by expand_selector — clicking it
    only opens the dropdown.
    """
    try:
        res = page.evaluate(JS_PAGE_SIZE, {"selector": cfg["selector"],
                                           "value": cfg["value"]})
    except Exception as e:
        return {"ok": False, "reason": str(e)[:150]}
    if res.get("ok"):
        page.wait_for_timeout(int(cfg.get("wait_ms", 5000)))
    return res


def _expand(page, selector: str, rounds: int = 6) -> int:
    """Click every match until the page stops growing.

    Accordions and "show more" controls hide content that is already in the DOM
    but collapsed. Reading it collapsed gives you the headings and none of the
    substance — SIMAH's page would be 17 lines saying "Article-N" and no law.
    Clicking is also what makes the SAVED HTML match what a person sees.
    """
    if not selector:
        return 0
    clicked = 0
    last = -1
    for _ in range(rounds):
        try:
            size = page.evaluate("()=>document.body.innerText.length")
            if size == last:
                break
            last = size
            clicked += page.evaluate(JS_EXPAND, selector)
        except Exception:
            break
        page.wait_for_timeout(400)
    return clicked


JS_TAB_LABELS = r"""(sel) => {
  const clean = s => (s || '').replace(/\s+/g, ' ').trim();
  return Array.from(document.querySelectorAll(sel))
    .map(e => clean(e.innerText || e.textContent).slice(0, 80));
}"""


#: Fingerprint of what a tab is currently showing — the row hrefs, in order.
#: Compared across clicks to tell "this tab really filtered" from "the click has
#: not taken effect yet".
JS_ROW_FINGERPRINT = r"""(sel) => Array.from(document.querySelectorAll(sel))
  .map(e => { const a = e.querySelector('a[href]'); return a ? a.getAttribute('href') : ''; })
  .join('|')"""


def _walk_tabs(page, tabs_cfg: dict, harvest, section_prefix,
               row_sel: str = "") -> list[dict]:
    """Click each tab in turn and harvest the rows it reveals.

    The tab's LABEL is the point. MOE renders all 119 documents in one flat
    container with no category attribute anywhere in the markup — the only place
    "Personnel" or "Scholarship Program" exists is the tab you clicked to see
    them. So the label becomes the section for every row harvested under it.

    Tabs are re-queried by index on every iteration because clicking one usually
    re-renders the list, which invalidates any element handles held across it.

    A CLICK THAT HAS NOT LANDED YET LOOKS EXACTLY LIKE A SHARED DOCUMENT
    -------------------------------------------------------------------
    This used to click, wait a fixed `wait_ms`, and harvest whatever was on
    screen. When the site had not re-filtered yet, the PREVIOUS tab's rows were
    harvested and filed under the NEW tab's label — a real document with a
    category it does not have, which is worse than a missing one because nothing
    downstream can tell it is wrong.

    Measured on MOE 2026-08-12: 20 such rows. "Scholarship Procedures" and
    others appeared under both `Scholarship Program` (true) and `Special
    Education` (false). The tab strip needs 14s to render, but the per-tab
    settle was 1500ms.

    Pagination has had this guard for a long time — it stops on "rows unchanged
    after click" (a disabled Next re-harvests page 1 forever). Tabs never got
    it. Now the row fingerprint is compared across clicks: unchanged means wait
    longer and retry, and if it is STILL unchanged the tab is recorded as
    `unchanged-after-click` and NOT harvested.

    Two tabs CAN legitimately share documents, and that is preserved — what is
    rejected is a row set byte-identical to the previous tab's, which on a
    16-tab site is a failed click rather than a coincidence.
    """
    sel = tabs_cfg["selector"]
    skip = {s.strip().lower() for s in (tabs_cfg.get("skip_labels") or [])}
    wait_ms = int(tabs_cfg.get("wait_ms", 1200))
    max_tabs = int(tabs_cfg.get("max_tabs", 50))

    try:
        labels = page.evaluate(JS_TAB_LABELS, sel)[:max_tabs]
    except Exception as e:
        emit({"event": "tabs_error", "error": str(e)[:200]})
        return []

    def _fingerprint() -> str:
        if not row_sel:
            return ""                     # no selector: behave as before
        try:
            return page.evaluate(JS_ROW_FINGERPRINT, row_sel)
        except Exception:
            return ""

    log = []
    prev_fp = _fingerprint()              # what the page shows before any click
    for i, label in enumerate(labels):
        if not label or label.strip().lower() in skip:
            # "All" is skipped by default on most sites: it repeats every other
            # tab's contents, and a row harvested under it would take "All" as
            # its category instead of the real one.
            emit({"event": "tab_skipped", "label": label})
            continue
        try:
            page.evaluate("(a)=>{const e=document.querySelectorAll(a[0])[a[1]];"
                          " if(e) e.click();}", [sel, i])
        except Exception:
            log.append({"tab": label, "matched": 0, "new": 0, "status": "click-failed"})
            continue
        page.wait_for_timeout(wait_ms)

        # Did the list actually change? If not, give it one more (longer) go
        # before concluding the click did not land.
        fp = _fingerprint()
        if row_sel and fp and fp == prev_fp:
            page.wait_for_timeout(max(wait_ms * 2, 3000))
            fp = _fingerprint()

        if row_sel and fp and fp == prev_fp:
            # UNCHANGED HAS TWO CAUSES AND ONLY ONE IS A FAULT.
            #
            #   (a) the click has not landed  -> harvesting files the PREVIOUS
            #       tab's rows under this label. Must skip.
            #   (b) this tab was ALREADY the active one -> the rows on screen
            #       are genuinely its own, and skipping loses them.
            #
            # (b) is the normal state of the FIRST tab: a site renders its
            # default tab on load, so clicking it changes nothing. Treating that
            # as a failure cost MOE its entire "General" section — 4 real
            # documents, 136 -> 132, while every other tab was untouched.
            #
            # Ask the DOM which tab is selected rather than inferring it from the
            # rows: aria-selected and the active class are what a tab strip uses
            # to say so.
            try:
                is_active = page.evaluate(
                    "(a)=>{const e=document.querySelectorAll(a[0])[a[1]];"
                    " if(!e) return false;"
                    " const s=e.getAttribute('aria-selected');"
                    " if(s==='true') return true;"
                    " const c=((e.className||'')+' '+((e.parentElement||{}).className||''))"
                    "   .toString().toLowerCase();"
                    " return /(^|\s)(active|selected|current)(\s|$)/.test(c);}",
                    [sel, i])
            except Exception:
                is_active = False

            if not is_active:
                # Harvesting here would file the PREVIOUS tab's rows under this
                # label. Skip, and say so loudly enough to be fixed in the hints —
                # the usual cause is tabs.wait_ms being too short for the site.
                log.append({"tab": label, "matched": 0, "new": 0,
                            "status": "unchanged-after-click"})
                emit({"event": "tab_unchanged", "label": label,
                      "hint": "rows identical to the previous tab after clicking; "
                              "raise tabs.wait_ms"})
                continue
            emit({"event": "tab_already_active", "label": label,
                  "note": "rows unchanged because this tab was already selected; "
                          "harvesting them as its own"})

        prev_fp = fp
        res = harvest(list(section_prefix) + [label])
        log.append({"tab": label, "matched": res["matched"], "new": res["new"],
                    "status": "ok"})
        emit({"event": "tab", "label": label, "matched": res["matched"],
              "new": res["new"]})
    return log


# Is the "Next" control still usable? A pager that has run out does not remove
# the control — it marks it disabled (MOH: class="disabled"). Clicking a disabled
# Next silently re-harvests page 1 forever, so this is the stop condition.
JS_NEXT_STATE = r"""(sel) => {
  const el = document.querySelector(sel);
  if (!el) return {found: false};
  const cls = (el.className && el.className.toString) ? el.className.toString() : '';
  const r = el.getBoundingClientRect();
  const cs = getComputedStyle(el);
  return {
    found: true,
    disabled: /\bdisabled\b/i.test(cls)
              || el.getAttribute('aria-disabled') === 'true'
              || el.hasAttribute('disabled'),
    hidden: r.width < 2 || r.height < 2 || cs.display === 'none' || cs.visibility === 'hidden',
    label: (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 40),
    page: el.getAttribute('data-page') || '',
  };
}"""


def _stated_total(page, cfg: dict) -> int | None:
    """The total the page states about itself, or None.

    DataTables writes "Showing 1 to 685 of 685 entries". That number comes from
    the site, so comparing it against what we harvested is the one coverage check
    that does not rely on our own selectors being right.
    """
    if not cfg:
        return None
    try:
        txt = page.evaluate(
            "(s)=>{const e=document.querySelector(s); return e ? (e.innerText||e.textContent||'') : ''}",
            cfg["selector"])
    except Exception:
        return None
    m = re.search(cfg["pattern"], txt or "")
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", "").replace(" ", ""))
    except ValueError:
        return None


def _row_signature(page, row_sel: str, link_sel: str) -> str:
    try:
        return page.evaluate(JS_ROW_SIG, {"rowSel": row_sel, "linkSel": link_sel})
    except Exception:
        return ""


def _click_through(page, pagination: dict, row_sel: str, link_sel: str,
                   css_fields: dict, rx_fields: dict, rows: list, seen: set,
                   section_prefix, section_levels, wait_ms: int,
                   page_log: list) -> dict:
    """Walk a JavaScript pager, harvesting each page. Page 1 is already harvested.

    Returns {"note": ..., "warnings": [...]}. Every turn is verified against the
    row fingerprint, so the walk stops rather than looping on a dead control.
    """
    next_sel = pagination.get("next_selector") or ""
    max_pages = int(pagination.get("max_pages", 200))
    note = {"mode": "click", "pages_walked": 1, "stopped": "", "max_pages": max_pages}
    warns: list[str] = []

    empty_streak = 0
    page_no = 1
    for page_no in range(2, max_pages + 1):
        sig_before = _row_signature(page, row_sel, link_sel)
        url_before = page.url
        try:
            outcome = page.evaluate(JS_CLICK_NEXT, next_sel)
        except Exception as e:
            outcome = f"click-error: {str(e)[:60]}"
        if outcome != "clicked":
            note["stopped"] = f"page {page_no}: {outcome}"
            if page_no == 2:
                warns.append(f"pagination.next_selector {next_sel!r} matched no usable "
                             f"control on the seed page ({outcome}) — only page 1 was "
                             "walked. Check it against `formfill inspect`.")
            break

        # A click gets AT LEAST as long as the form says the page needs to
        # render. CLICK_SETTLE_MS alone is a fixed 8s, and on a site slower than
        # that the row set has simply not changed yet when the poll gives up —
        # the walk then stops at page 1 and reports "rows unchanged after click",
        # which reads like a wrong selector rather than a race.
        #
        # Measured on MHRSD 2026-08-12: the same form and selector returned 122
        # rows on one run and 18 on the next, deciding on how fast the site
        # happened to respond. A form that declares `wait_ms: 14000` is stating
        # that this page needs 14s; a pagination click on it cannot need less.
        settle_ms = max(CLICK_SETTLE_MS, int(wait_ms or 0))
        changed, waited = False, 0
        while waited < settle_ms:
            page.wait_for_timeout(CLICK_POLL_MS)
            waited += CLICK_POLL_MS
            if _row_signature(page, row_sel, link_sel) != sig_before:
                changed = True
                break
        if not changed:
            # On the last page this is normal — the control is often still there.
            # On the FIRST turn it means the selector never worked at all.
            note["stopped"] = f"page {page_no}: rows unchanged after click"
            if page_no == 2:
                warns.append(f"pagination.next_selector {next_sel!r} was clicked but the "
                             "row set never changed — only page 1 was walked. Wrong "
                             "selector, or this pager needs mode: custom.")
            break

        page.wait_for_timeout(wait_ms)
        h = _harvest(page, row_sel, link_sel, css_fields, rx_fields,
                     page.url, rows, seen, section_prefix, section_levels)
        page_log.append({"url": f"{page.url}#page-{page_no}", "matched": h["matched"],
                         "new": h["new"],
                         "status": "ok-clicked" + ("-navigated" if page.url != url_before else "")})
        note["pages_walked"] = page_no
        emit({"event": "click_page", "page": page_no, "matched": h["matched"],
              "new": h["new"], "rows": len(rows)})

        if h["new"] == 0:
            empty_streak += 1
            if empty_streak >= 2:
                note["stopped"] = f"page {page_no}: two consecutive pages added no new rows"
                break
        else:
            empty_streak = 0
    else:
        # Ran the whole budget without the pager ending: the count is the cap.
        note["capped_by_max_pages"] = True
        note["planned_pages"] = note["pages_walked"]
        note["pages_wanted"] = f"more than {note['pages_walked']}"
        note["stopped"] = f"hit max_pages ({max_pages})"

    return {"note": note, "warnings": warns}


# SNAPSHOTS — develop a form against a page we already have.
#
# One full run of SIMAH's form is TWO loads of one URL, so volume never tripped
# Cloudflare: ITERATION did. Every selector fix and every `verify --runs 3` was
# more live traffic for no new information. Capture once, then run offline.
#
# `<base href>` is not optional. Fields read `el.href` (the RESOLVED property, see
# JS_ROWS), so without it every relative link resolves against about:blank and
# document_url comes out quietly wrong.
_BASE_RE = re.compile(r"<base\b", re.I)


def snapshot_html(html: str, base_url: str) -> str:
    """Saved page + a <base>, so relative links resolve as they did live."""
    if _BASE_RE.search(html or ""):
        return html
    tag = f'<base href="{base_url}">'
    m = re.search(r"<head[^>]*>", html or "", re.I)
    return (html[:m.end()] + tag + html[m.end():]) if m else tag + (html or "")


def _load(page, url: str, wait_ms: int, tries: int = 3, snap: str | None = None) -> bool:
    """These sites are flaky. An empty page must never be read as 'no rows' —
    that is exactly the silent failure that puts holes in the library.

    `snap` is saved HTML: serve it instead of fetching, and touch no network.
    """
    if snap is not None:
        page.set_content(snapshot_html(snap, url), wait_until="domcontentloaded")
        page.wait_for_timeout(min(wait_ms, 300))
        return True
    for _ in range(tries):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(wait_ms)
            for _ in range(2):
                page.mouse.wheel(0, 6000)
                page.wait_for_timeout(250)
            # Wait for the page to STOP GROWING rather than for a fixed time.
            # SIMAH renders its 17 articles late: at 1.7s the body is 686
            # characters and div.accordion does not exist yet, so a fixed wait
            # captured a page that had not arrived. Two identical samples means
            # it has settled; a fast page costs one extra 400ms.
            settled = -1
            for _ in range(12):
                size = page.evaluate("()=>document.body ? document.body.innerText.length : 0")
                if size == settled:
                    break
                settled = size
                page.wait_for_timeout(400)
            # "Enough links" alone is the wrong test. SIMAH's law page is a
            # single document with almost no links — it reports 1 anchor until
            # the nav finishes rendering — and a >15 threshold silently threw the
            # whole page away as a failed load. Text is the other evidence that a
            # page arrived, and a document page has plenty of it.
            state = page.evaluate(
                "()=>({a: document.querySelectorAll('a[href]').length,"
                "      t: (document.body ? document.body.innerText.length : 0)})")
            if state["a"] > 15 or state["t"] > 400:
                return True
        except Exception:
            pass
        page.wait_for_timeout(1500)
    return False


def _url_key(url: str) -> str:
    """A row's href against a targeted url. Trailing slash only: a fragment is
    part of the identity, since SDAIA files four documents at #page=N of one
    PDF."""
    return (url or "").strip().rstrip("/")


def _split_targets(rows: list, targeted: set | None, is_tree: bool) -> tuple:
    """(open, walk past, warnings). Pure, because the rule deciding what a
    targeted run does NOT read is the one that can empty a library."""
    if targeted is None:
        return list(rows), [], []
    if is_tree:
        # Same reason as --max-details: on a tree phase 2 IS the walk that
        # discovers deeper nodes, so targeting it stops the discovery.
        return list(rows), [], [
            "--only-urls is ignored for shape: tree (phase 2 is the walk that "
            "discovers deeper nodes) — the whole tree was crawled"]
    targets = [r for r in rows if _url_key(r.get("href")) in targeted]
    skipped = [r for r in rows if _url_key(r.get("href")) not in targeted]
    # An EMPTY target list is "nothing changed", the normal answer and not a
    # mismatch. Urls that match no row are.
    warns = ([f"--only-urls matched NO row of {len(rows)}: phase 2 did not run "
              "at all. Check the urls came from this source"]
             if targeted and not targets else [])
    return targets, skipped, warns


def _stamp_hashes(records: list) -> None:
    """The record hash, in place.

    An unopened row has no detail text and the row_text fallback would hash the
    LISTING — a different hash from the one a full crawl stored, which reads as
    an edit. Left empty instead.
    """
    for rec in records:
        rec["content_hash"] = ("" if rec.get("detail_skipped")
                               else content_key(rec.get("text")
                                                or rec.get("row_text") or ""))


def run(hints: dict, out_dir: str | Path, headless: bool = True, wait_ms: int = 1200,
        fetch_details: bool | None = None, max_details: int | None = None,
        max_pages: int | None = None, write_excel: bool = True,
        snapshot: str | Path | None = None, only_urls=None) -> dict:
    """Crawl `hints['seed_url']` exactly as the form says. Returns a run summary.

    `snapshot` is a saved copy of the seed page. Given one, the run makes NO
    network requests: the same form, the same extraction, replayed against the
    saved HTML. The summary records `source: snapshot` so a replay can never be
    mistaken for a crawl of the live site.

    `only_urls` narrows PHASE 2 to those urls. Phase 1 still walks the whole
    listing, so the inventory stays complete; the rows that were not opened are
    recorded as `detail_skipped` rather than dropped, because a row missing from
    a run is a row the completeness gate reports as disappeared.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # A slow site can declare its own settle time. Callers that construct a run
    # programmatically (pipeline.FormfillCrawler) do not pass wait_ms, so every
    # form used the 1200ms default -- and MOE renders its tab strip after ~12s,
    # so `_walk_tabs` queried the selector before the tabs existed, walked 0 tabs
    # and harvested nothing. The site was fine; the run simply looked too early.
    wait_ms = int(hints.get("wait_ms") or wait_ms)

    # Only `attachment_is_document: "combined"` wants every link in a row, and
    # it is the only mode that can consume them. Tying the two together means
    # there is no way to collect the links and then quietly drop them.
    COLLECT_ROW_LINKS["on"] = (hints.get("attachment_is_document") == "combined")

    started = time.time()
    pagination = dict(hints.get("pagination") or {"mode": "none"})
    if max_pages:
        pagination["max_pages"] = max_pages
    row_sel = hints.get("row_selector") or ""      # absent on shape: tree
    link_sel = hints.get("detail_link_selector") or ""
    css_fields = [{"target": t, "selector": r["selector"], "attr": r.get("attr", "text"),
                   "whereText": r.get("where_text") or ""}
                  for t, r in (hints.get("fields") or {}).items() if r.get("from") == "css"]
    rx_fields = compile_field_regexes(hints)
    do_details = hints.get("fetch_details", True) if fetch_details is None else fetch_details
    targeted = {_url_key(u) for u in only_urls} if only_urls is not None else None

    strip_sels = list((hints.get("content") or {}).get("strip") or [])
    # {extra_meta key: css selector} — moved OUT of document_html and INTO
    # extra_meta, rather than deleted like content.strip.
    extract_map = dict((hints.get("content") or {}).get("extract") or {})
    lib = hints.get("library") or {}

    sp = hints.get("section_path") or {}
    # library.regulator / library.source_system lead every path, so the
    # crawler's own Excel shows the same trail the library will use.
    # Kept separate: library_crumbs identify the SOURCE and are always wanted,
    # while section_path.prefix is this page's own contribution and is a FALLBACK
    # when from_breadcrumb supplies nothing.
    library_crumbs = [c for c in (lib.get("regulator"), lib.get("source_system")) if c]
    section_prefix = library_crumbs + list(sp.get("prefix") or [])
    expand_sel = hints.get("expand_selector") or ""
    tabs_cfg = hints.get("tabs") or {}
    page_size_cfg = hints.get("page_size") or {}
    row_count_cfg = hints.get("row_count_check") or {}
    crumb_sel = sp.get("from_breadcrumb") or ""
    crumb_drop_first = int(sp.get("drop_first", 0))
    crumb_drop_last = int(sp.get("drop_last", 0))
    section_levels = [
        {"preceding": lv["preceding"]} if lv.get("preceding")
        else {"ancestor": lv["ancestor"], "title": lv["title"]}
        for lv in (sp.get("levels") or [])]

    panels_cfg = hints.get("panels") or {}
    tabs_sel = panels_cfg.get("tabs") or ""
    include_panel = bool(panels_cfg.get("include_panel"))
    panels: list[dict] = []

    seed = hints["seed_url"]
    section = (hints.get("name") or urlparse(seed).netloc).split(".")[-1].title()
    is_tree = hints.get("shape") == "tree"
    tree_max_nodes = int((hints.get("tree") or {}).get("max_nodes", 1000))

    # Read the snapshot ONCE. A missing file is a hard error, not a silent
    # fallthrough to the live site: the whole point is that this run cannot
    # generate traffic, and quietly crawling instead is the surprise that costs
    # an IP block.
    snap_html: str | None = None
    if snapshot is not None:
        snap_path = Path(snapshot)
        if not snap_path.exists():
            raise FileNotFoundError(
                f"snapshot not found: {snap_path}. Capture one with "
                f"`formfill snapshot {hints.get('name')}` — this run will not go live.")
        snap_html = snap_path.read_text(encoding="utf-8")

        # A SNAPSHOT IS ONE PAGE, so it can only replay a one-page form.
        #
        # Refused rather than half-served, because both alternatives are worse than
        # an error: fetching pages 2..N would generate exactly the live traffic a
        # snapshot exists to avoid, and skipping them would report a fraction of the
        # site as if it were the whole thing. SBP's 139 listing pages and SAMA's
        # 40-node tree need the network; SIMAH's single law page does not.
        mode = (pagination.get("mode") or "none").lower()
        if mode != "none":
            raise ValueError(
                f"{hints.get('name')}: --snapshot only works on a single-page form, "
                f"and this one paginates (mode: {mode}). One saved page cannot stand "
                f"in for a walk of many, and serving it for page 1 while fetching the "
                f"rest would put live traffic behind a flag that promises none.")
        if hints.get("shape") == "tree":
            raise ValueError(
                f"{hints.get('name')}: --snapshot only works on a single-page form, "
                f"and shape: tree discovers its nodes by visiting them — the seed's "
                f"menu shows a fraction of the tree (20 of SAMA's 40), so a replay "
                f"would silently report that fraction as the whole rulebook.")

    rows: list[dict] = []
    seen: set[str] = set()
    page_log: list[dict] = []
    warnings: list[str] = []
    plan_note: dict = {"mode": pagination.get("mode", "none")}
    stated_totals: list = []   # (site says, we matched) per listing page
    list_total: int | None = None   # a WHOLE-LIST total, when the page states one
    blocked = 0        # pages that came back as a bot-protection wall

    emit({"event": "start", "name": hints.get("name"), "seed": seed,
          "row_selector": row_sel, "mode": pagination.get("mode")})

    with sync_playwright() as pw:
        # Some sites fingerprint headless Chromium and refuse it outright.
        # Tadawul returns 403 "Access Denied" headless and 200 headful, so a
        # scheduled headless run would quietly harvest nothing. A form can
        # declare that, and it OVERRIDES the caller — the site's requirement is
        # a fact about the site, not a preference of whoever launched the run.
        #
        # NOT ON A REPLAY. A snapshot run loads the saved page from disk and
        # never contacts the site, so there is nothing to fingerprint -- and
        # forcing a visible window there makes a scheduled, zero-traffic job
        # demand a display it has no use for (a headless server has none).
        # Saudi Exchange is why: it is the one form that declares this AND is
        # replayed on a schedule. Only capture() ever needs a real window.
        if hints.get("requires_headed") and headless and snapshot is None:
            headless = False
            print(json.dumps({"event": "note",
                              "message": "requires_headed: forcing a visible browser "
                                         "(this site rejects headless)"}), flush=True)
        browser = pw.chromium.launch(
            headless=headless,
            args=["--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(user_agent=UA, locale="en-US")
        page = ctx.new_page()

        # ---------------- PHASE 1: the listing ----------------
        try:
            if not _load(page, seed, wait_ms, snap=snap_html):
                warnings.append("seed page failed to load after 3 attempts")
            blocked_reason = _blocked(page)
            if blocked_reason:
                blocked += 1
                warnings.append(
                    f"BLOCKED BY BOT PROTECTION on the seed page ({blocked_reason!r}). "
                    "Everything below is the challenge page, not the site. Slow the "
                    "crawl down, or this source needs mode: custom.")
                emit({"event": "blocked", "url": seed, "reason": blocked_reason})
            if page_size_cfg:
                res = _set_page_size(page, page_size_cfg)
                emit({"event": "page_size", **res})
                if not res.get("ok"):
                    warnings.append(
                        f"page_size did NOT apply ({res.get('reason')}) "
                        "— the crawl is limited to the default page size, so the "
                        "count below is a fraction of the site")

            if expand_sel:
                n = _expand(page, expand_sel)
                emit({"event": "expand", "selector": expand_sel, "clicked": n})

            try:
                h1 = page.evaluate(
                    "()=>{const h=document.querySelector('h1');return h?(h.innerText||''):''}")
                section = re.sub(r"\s+", " ", h1).strip() or section
            except Exception:
                pass

            if tabs_sel:
                panels = _find_panels(page, tabs_sel)
                emit({"event": "panels", "found": len(panels),
                      "labels": [p["label"] for p in panels]})
                if not panels:
                    warnings.append(
                        f"panels.tabs {tabs_sel!r} matched no tab whose href is a "
                        "fragment pointing at an element on this page — check it "
                        "against `formfill inspect`")

            # Each panel is one document: the tab names it, the panel's text IS
            # its content. Same idea as include_page, one level in.
            if include_panel:
                for p in panels:
                    url = f"{seed}{p['fragment']}"
                    rows.append({
                        "title": p["label"],
                        "href": url,
                        "row_text": "",
                        "found_on": seed,
                        "in_page": f"[data-ff-panel=\"{p['index']}\"]",
                        "section_trail": list(section_prefix),
                        "fields": {"title": p["label"], "document_url": url},
                    })
                    seen.add(url)

            # The seed page as a document in its own right, added FIRST so it
            # leads the inventory. Phase 2 opens it like any other row, which is
            # what captures its text and HTML.
            if hints.get("include_page"):
                rows.append({
                    "title": section,
                    "href": seed,
                    "row_text": "",
                    "found_on": seed,
                    "section_trail": list(section_prefix) or [section],
                    "fields": {"title": section, "document_url": seed},
                })
                seen.add(seed)

            if tabs_cfg:
                # One row per (document, tab): a file offered under two
                # categories is two placements, exactly as with cross-listed
                # documents elsewhere. `documents` still de-duplicates on URL
                # and records the extra placements in `also_in`.
                def _tab_harvest(trail):
                    return _harvest(page, row_sel, link_sel, css_fields, rx_fields,
                                    seed, rows, seen, trail, section_levels,
                                    dedupe_scope=trail[-1] if trail else "")

                tab_log = _walk_tabs(page, tabs_cfg, _tab_harvest, section_prefix,
                                     row_sel=row_sel)
                page_log.extend([{"url": f"{seed}#{t['tab']}", **t} for t in tab_log])
                plan_note = {"mode": "tabs", "tabs_walked": len(tab_log)}
                if not rows:
                    warnings.append(
                        f"no rows harvested from any tab — check tabs.selector "
                        f"{tabs_cfg['selector']!r} and row_selector {row_sel!r}")
                listing_urls = []
            elif is_tree:
                # One page, one menu. Everything below the listing loop is the
                # same for both shapes, which is the whole reason a tree is a
                # shape here rather than a second crawler.
                res = _walk_tree(page, hints["tree"], rx_fields, seed, rows, seen,
                                 section_prefix)
                page_log.append({"url": seed, "matched": res["matched"],
                                 "new": res["new"], "status": "ok"})
                plan_note = {"mode": "tree", "nodes_found": res["matched"]}
                emit({"event": "tree", "nodes": res["matched"], "rows": len(rows)})
                if res["new"] == 0:
                    warnings.append(
                        f"the menu {hints['tree']['menu_selector']!r} yielded no nodes — "
                        "check menu_selector / node_selector against `formfill inspect`")
                listing_urls = []
            else:
                hrefs = (page.evaluate(JS_HREFS)
                         if pagination.get("mode") in ("url_offset", "url_page") else [])
                listing_urls, plan_note = plan_pages(pagination, seed, hrefs)
                emit({"event": "plan", "pages": len(listing_urls), **plan_note})

            empty_streak = 0
            for i, lurl in enumerate(listing_urls, 1):
                if i > 1 and not _load(page, lurl, wait_ms):
                    page_log.append({"url": lurl, "matched": 0, "new": 0, "status": "load-failed"})
                    warnings.append(f"listing page failed to load: {lurl}")
                    continue
                if i > 1 and tabs_sel:
                    panels = _find_panels(page, tabs_sel)
                new = _harvest_page(page, row_sel, link_sel, css_fields, rx_fields,
                                    lurl, rows, seen, section_prefix, section_levels,
                                    panels if tabs_sel else None)
                matched = new["matched"]

                # Does the page agree with us about how many rows it has?
                stated = _stated_total(page, row_count_cfg)
                # A whole-list total says nothing about THIS page, so it must not
                # drive the per-page shortfall retry or the per-page comparison.
                # It is checked once, against the finished inventory.
                if stated is not None and row_count_cfg.get("total") == "list":
                    if list_total is None:
                        list_total = stated
                        emit({"event": "row_count", "stated": stated,
                              "counts": "whole list", "matched": matched})
                    stated = None
                if stated is not None:
                    tol = int(row_count_cfg.get("tolerance", 0))
                    if matched < stated - tol:
                        # A redraw that had not settled, or rows still arriving.
                        # Wait and harvest again before believing the shortfall —
                        # the retry is cheap and the alternative is losing rows.
                        emit({"event": "row_count_short", "matched": matched,
                              "stated": stated, "action": "retrying"})
                        page.wait_for_timeout(6000)
                        again = _harvest(page, row_sel, link_sel, css_fields,
                                         rx_fields, lurl, rows, seen,
                                         section_prefix, section_levels)
                        matched = max(matched, again["matched"])
                        new = {"matched": matched,
                               "new": new["new"] + again["new"]}
                    stated_totals.append((stated, matched))
                    emit({"event": "row_count", "stated": stated, "matched": matched})
                page_log.append({"url": lurl, "matched": matched, "new": new["new"], "status": "ok"})

                if matched == 0:
                    empty_streak += 1
                    # Two empty pages in a row means the pattern has run past the
                    # end (or the selector is wrong). Keep going and it invents
                    # hundreds of pointless loads.
                    if empty_streak >= 2:
                        warnings.append(f"stopped at page {i}/{len(listing_urls)}: "
                                        "two consecutive pages matched no rows")
                        emit({"event": "stop_early", "page": i, "of": len(listing_urls)})
                        break
                else:
                    empty_streak = 0

                if i == 1 or i == len(listing_urls) or i % 10 == 0:
                    emit({"event": "list_page", "page": i, "of": len(listing_urls),
                          "matched": matched, "rows": len(rows)})

            # PHASE 1 over. This is the inventory, and on its own it is what
            # change detection consumes.
            for stated, matched in stated_totals:
                if matched < stated:
                    warnings.append(
                        f"COVERAGE GAP: the page states {stated} rows, we harvested "
                        f"{matched} — {stated - matched} row(s) were not captured "
                        "even after a retry")
                elif matched > stated:
                    warnings.append(
                        f"harvested {matched} rows but the page states {stated} — "
                        "the row selector may be matching extra elements")

            # ---------- click pagination ----------
            # Some pagers have no URL to construct: every link is
            # javascript:void(0) and the list is redrawn in place (MOH). So walk
            # it by clicking, and stop on the pager's OWN signal rather than a
            # guess — a disabled Next, a missing Next, or a redraw that adds no
            # rows. All three are real endings; a click budget alone is not.
            if pagination.get("mode") == "click":
                next_sel = pagination.get("next_selector") or ""
                max_click_pages = int(pagination.get("max_pages", 200))
                # The strongest stop signal is the site's OWN total, when it
                # publishes one ("Showing 1-10 of 75 items"). It beats reading the
                # pager's disabled state, which some sites — MOH included — apply
                # with their own JS AFTER the list initialises, so a check can win
                # the race and see an enabled Next on a single-page list.
                target = list_total or (stated_totals[0][0] if stated_totals else None)
                page_no, barren = 1, 0
                while page_no < max_click_pages:
                    if target is not None and len(rows) >= target:
                        emit({"event": "pager_end", "page": page_no,
                              "reason": f"reached the stated total of {target} rows"})
                        break
                    try:
                        state = page.evaluate(JS_NEXT_STATE, next_sel)
                    except Exception:
                        state = {"found": False}
                    if not state.get("found"):
                        warnings.append(
                            f"pagination.next_selector matched nothing after page "
                            f"{page_no} ({next_sel!r}) — the walk may be incomplete")
                        break
                    if state.get("disabled") or state.get("hidden"):
                        emit({"event": "pager_end", "page": page_no,
                              "reason": "next is disabled"})
                        break

                    before = len(rows)
                    try:
                        page.click(next_sel, timeout=8000)
                    except Exception as e:
                        warnings.append(f"clicking next failed on page {page_no}: "
                                        f"{str(e)[:80]}")
                        break
                    page.wait_for_timeout(max(wait_ms, 1200))

                    got = _harvest(page, row_sel, link_sel, css_fields, rx_fields,
                                   seed, rows, seen, section_prefix, section_levels)
                    page_no += 1
                    gained = len(rows) - before
                    page_log.append({"url": f"{seed}#page={page_no}",
                                     "matched": got["matched"], "new": gained,
                                     "status": "ok"})
                    if target is not None and len(rows) >= target:
                        emit({"event": "pager_end", "page": page_no,
                              "reason": f"reached the stated total of {target} rows"})
                        break
                    if gained == 0:
                        # A redraw that adds nothing is either the end or a pager
                        # that looped back. One is tolerable, two is a stop.
                        barren += 1
                        if barren >= 2:
                            emit({"event": "pager_end", "page": page_no,
                                  "reason": "two redraws added no rows"})
                            break
                    else:
                        barren = 0
                    if page_no == 2 or page_no % 10 == 0:
                        emit({"event": "list_page", "page": page_no,
                              "of": max_click_pages, "matched": got["matched"],
                              "rows": len(rows)})
                emit({"event": "pager_done", "pages_walked": page_no,
                      "rows": len(rows)})
                if page_no >= max_click_pages:
                    warnings.append(
                        f"click pagination stopped at max_pages={max_click_pages} "
                        "— this is a CAP, not the end of the list")

            # Coverage is judged on the FINISHED inventory, which is only
            # complete here: with click pagination the walk above adds most of
            # the rows, so checking earlier compared 75 against page one's 10.
            if list_total is not None:
                if len(rows) < list_total:
                    warnings.append(
                        f"COVERAGE GAP: the site states {list_total} rows in this "
                        f"list, the walk finished with {len(rows)} — "
                        f"{list_total - len(rows)} row(s) missing")
                elif len(rows) > list_total:
                    warnings.append(
                        f"harvested {len(rows)} rows but the site states "
                        f"{list_total} — the row selector may be matching extras")
                else:
                    emit({"event": "coverage_ok", "stated": list_total,
                          "harvested": len(rows)})
            if pagination.get("mode") == "click" and not is_tree:
                res = _click_through(page, pagination, row_sel, link_sel, css_fields,
                                     rx_fields, rows, seen, section_prefix,
                                     section_levels, wait_ms, page_log)
                plan_note = res["note"]
                warnings.extend(res["warnings"])
                emit({"event": "plan", "pages": plan_note["pages_walked"], **plan_note})
        finally:
            page.close()

        # ---------------- PHASE 2: the detail pages ----------------
        records, documents = [], {}
        # NOTE: for a tree this is the SAME list object as `rows`, not a copy —
        # that is what lets the walk below discover new nodes while iterating.
        targets = rows if do_details else []
        skipped: list[dict] = []
        if targets and targeted is not None:
            targets, skipped, warns = _split_targets(rows, targeted, is_tree)
            warnings.extend(warns)
            emit({"event": "targeted", "asked": len(targeted),
                  "matched": len(targets), "skipped": len(skipped)})
        if max_details and not is_tree:
            targets = targets[:max_details]
        elif max_details and is_tree:
            warnings.append("--max-details is ignored for shape: tree (slicing the list "
                            "would stop the walk discovering deeper nodes) — "
                            "use tree.max_nodes instead")

        if targets:
            dp = ctx.new_page()
            # Rows that live inside the seed document share one load of it. The
            # stamps from phase 1 were on a page that is now closed, so the same
            # deterministic passes are replayed here to put them back.
            in_page_ready: bool | None = None       # None = not attempted yet

            def _prepare_in_page() -> bool:
                if not _load(dp, seed, wait_ms, tries=2, snap=snap_html):
                    warnings.append("could not reload the seed page for its panels — "
                                    "panel text is missing from this run")
                    return False
                if expand_sel:
                    _expand(dp, expand_sel)
                again = _find_panels(dp, tabs_sel) if tabs_sel else []
                if row_sel and tabs_sel:
                    _harvest_page(dp, row_sel, link_sel, css_fields, rx_fields, seed,
                                  [], set(), section_prefix, section_levels, again)
                return True

            try:
                for i, r in enumerate(targets, 1):
                    # A row that already points AT a file has no detail page to
                    # open. Trying anyway loads a PDF in a browser tab, finds no
                    # HTML, and books it as a failed fetch — so the file never
                    # reaches `documents` at all. Register it and move on.
                    if r["href"] and _is_doc(r["href"]):
                        _add_document(documents, r["href"], r["title"],
                                      r.get("found_on") or seed,
                                      " > ".join(r.get("section_trail") or []) or section,
                                      declared=True)      # the form named this row
                        records.append(_record(r, section, seed))
                        continue
                    if not r["href"] or not _load(dp, r["href"], wait_ms, tries=2):
                        records.append(_record(r, section, seed))
                        continue
                    # Phase 2 loads a fresh tab, so anything the form expanded in
                    # phase 1 is collapsed again here. Expand before capturing or
                    # the saved HTML is the closed version of the page.
                    if expand_sel:
                        _expand(dp, expand_sel)
                    reason = _blocked(dp)
                    if reason:
                        # Store the row, never the challenge page as its content.
                        blocked += 1
                        records.append(_record(r, section, seed))
                        if blocked <= 3:
                            warnings.append(
                                f"BLOCKED BY BOT PROTECTION at {r['href']}: {reason!r}")
                        continue
                    try:
                        d = dp.evaluate(JS_DETAIL, [strip_sels, extract_map])
                    except Exception:
                        records.append(_record(r, section, seed))
                        continue
                    if r.get("in_page"):
                        # No page to fetch: the content is an element of the seed
                        # document, which is loaded once for every such row.
                        if in_page_ready is None:
                            in_page_ready = _prepare_in_page()

                        # THE STAMPS DO NOT SURVIVE THE NAVIGATION ABOVE.
                        #
                        # These rows are addressed as `seed#fragment`, and the
                        # `_load` at the top of this loop goes there. On a
                        # JavaScript app that is a FULL RELOAD, not a same-document
                        # jump — measured on GOSI 2026-08-12: after stamping two
                        # panels, `goto(seed + "#1")` leaves
                        # `document.querySelectorAll('[data-ff-panel]').length == 0`.
                        #
                        # So every row after the first found nothing. Row 1 worked
                        # only by accident of ordering: for it `in_page_ready` was
                        # still None, so _prepare_in_page() ran AFTER the navigation
                        # and re-stamped. From row 2 the flag was cached True and
                        # nothing re-stamped, which is why five of six GOSI Social
                        # Insurance instruments and one of two Saned instruments
                        # stored NO html while the first row held the whole page.
                        #
                        # Re-prepare whenever the stamp this row needs is absent.
                        # Checking the selector rather than tracking navigations
                        # means it does not matter WHY the stamps went away.
                        if in_page_ready:
                            try:
                                present = dp.evaluate(
                                    "(s) => !!document.querySelector(s)", r["in_page"])
                            except Exception:
                                present = False
                            if not present:
                                in_page_ready = _prepare_in_page()

                        if not in_page_ready:
                            records.append(_record(r, section, seed))
                            continue
                        try:
                            d = dp.evaluate(JS_IN_PAGE_DETAIL, r["in_page"])
                        except Exception:
                            d = None
                        if not d:
                            warnings.append(
                                f"panel content not found for {r['title']!r} "
                                f"({r['in_page']}) even after re-preparing the seed "
                                f"— the panel selector may no longer match")
                            records.append(_record(r, section, seed))
                            continue
                    else:
                        # On a snapshot run the only page we hold is the seed, so a
                        # row pointing at it (include_page) replays; any other row
                        # would need the network and is left without detail content
                        # rather than quietly fetched.
                        row_snap = snap_html if (snap_html is not None
                                                 and r["href"] == seed) else None
                        if snap_html is not None and row_snap is None:
                            records.append(_record(r, section, seed))
                            continue
                        if not r["href"] or not _load(dp, r["href"], wait_ms, tries=2,
                                                      snap=row_snap):
                            records.append(_record(r, section, seed))
                            continue
                        # Phase 2 loads a fresh tab, so anything the form expanded in
                        # phase 1 is collapsed again here. Expand before capturing or
                        # the saved HTML is the closed version of the page.
                        if expand_sel:
                            _expand(dp, expand_sel)
                        reason = _blocked(dp)
                        if reason:
                            # Store the row, never the challenge page as its content.
                            blocked += 1
                            records.append(_record(r, section, seed))
                            if blocked <= 3:
                                warnings.append(
                                    f"BLOCKED BY BOT PROTECTION at {r['href']}: {reason!r}")
                            continue
                        try:
                            d = dp.evaluate(JS_DETAIL, [strip_sels, extract_map])
                        except Exception:
                            records.append(_record(r, section, seed))
                            continue
                    # A tree is discovered as it is walked. Drupal book menus
                    # (and most rulebook sidebars) only render the branch you
                    # are currently in, so the seed page shows 20 nodes and the
                    # rest only appear once you are standing on a child page.
                    # Re-reading the menu here turns phase 2 into a breadth-first
                    # walk: `rows` grows while the loop iterates it.
                    if is_tree and len(rows) < tree_max_nodes:
                        _walk_tree(dp, hints["tree"], rx_fields, r["href"], rows, seen,
                                   section_prefix)

                    # The page's own breadcrumb wins over anything inferred from a
                    # menu. On a tree the menu only shows the branch you are in —
                    # a node discovered three levels down inherits whatever
                    # nesting that one page happened to render, which is how
                    # "A1 Identification/Contact Details" ended up filed directly
                    # under "Regulatory Sandbox". The breadcrumb states the real
                    # path, in the site's own words.
                    if crumb_sel:
                        try:
                            crumbs = dp.evaluate(JS_CRUMBS, crumb_sel)
                        except Exception:
                            crumbs = []
                        crumbs = crumbs[crumb_drop_first:len(crumbs) - crumb_drop_last
                                        if crumb_drop_last else None]
                        if len(crumbs) >= 2:
                            # The breadcrumb REPLACES the trail, which would drop
                            # the library crumbs with it — SAMA's breadcrumb starts
                            # at "SAMA Rulebook" and never names the regulator. Put
                            # them back in front; the de-dupe drops whichever the
                            # breadcrumb already supplies.
                            # Only the library crumbs, and only the ones the
                            # breadcrumb does not already name ANYWHERE — not just
                            # adjacently. SAMA's breadcrumb is "SAMA Rulebook >
                            # Regulatory Sandbox > …", so prepending the form's
                            # fallback prefix too produced "SAMA > SAMA Rulebook >
                            # Regulatory Sandbox > SAMA Rulebook > Regulatory
                            # Sandbox > …" — a consecutive-only de-dupe cannot see
                            # a repeat that has something between the copies.
                            have = {c.strip().lower() for c in crumbs if c}
                            head = [c for c in library_crumbs
                                    if c.strip().lower() not in have]
                            # And the panel label behind it: the breadcrumb
                            # says where the PAGE sits, the panel says where
                            # inside it. GOSI Social Insurance is six legal
                            # instruments on one URL, which without this all
                            # share a single folder.
                            r["section_trail"] = head + list(crumbs) + (
                                [r["panel_label"]] if r.get("panel_label") else [])

                    row_path = " > ".join(r.get("section_trail") or []) or section
                    doc_links, doc_files = [], []
                    for l in d.get("links", []):
                        h = l.get("href") or ""
                        if h.startswith("http") and _is_doc(h):
                            doc_links.append(h)
                            doc_files.append({"href": h, "text": (l.get("text") or "").strip()})
                            # "Download Original PDF" is a button label, not a
                            # title. Left as-is, every SAMA circular's PDF was
                            # filed under that name.
                            # Scraped from an anchor, so a row the FORM declared
                            # wins over this one (declared=False). Failing that,
                            # a generic button label falls back to the row title —
                            # "Download Original PDF" is not a document name.
                            _add_document(documents, h,
                                          _doc_title(l.get("text"), r["title"],
                                                     l.get("href") or ""),
                                          r["href"], row_path, declared=False)
                    records.append(_record(r, section, seed, {
                        "n_pdfs": len(doc_links),
                        "pdf_links": " | ".join(doc_links),
                        # href + label per attached file. `pdf_links` keeps only
                        # the urls (and the pipeline's tier-3 fallback reads it),
                        # but a page with several attachments needs each file's
                        # own label to be nameable — SAMA circulars go up to 7.
                        "pdf_docs": doc_files,
                        "text_len": len(d.get("text") or ""),
                        "text": d.get("text") or "",
                        "html": _tidy_html(d.get("html") or ""),
                        # Blocks lifted out by content.extract, keyed by the name
                        # the form gave them. The pipeline files each under that
                        # key in extra_meta, so nothing captured is lost — it is
                        # simply no longer part of the document.
                        "extracted": d.get("extracted") or {},
                    }))
                    if i % 25 == 0 or i == len(targets):
                        emit({"event": "detail_page", "done": i, "of": len(targets)})
            finally:
                dp.close()
        else:
            records = [_record(r, section, seed) for r in rows]
            # A row whose link is already a PDF is a document in its own right;
            # without phase 2 it would otherwise be lost.
            for r in rows:
                if r["href"] and _is_doc(r["href"]):
                    _add_document(documents, r["href"], r["title"], seed,
                                  " > ".join(r.get("section_trail") or []) or section,
                                  declared=True)

        # A targeted run still has to account for every row it walked past. The
        # record says the detail page was not opened, so nothing downstream reads
        # an unopened page as an emptied one.
        for r in skipped:
            records.append(_record(r, section, seed, {"detail_skipped": True}))
            if r["href"] and _is_doc(r["href"]):
                _add_document(documents, r["href"], r["title"], seed,
                              " > ".join(r.get("section_trail") or []) or section,
                              declared=True)

        browser.close()

    _stamp_hashes(records)
    docs = list(documents.values())
    # A title shared by several different documents is not a title: GOSI files
    # four occupational-hazard policies under one "Insurance coverage document"
    # heading. Declared titles are the form's answer and are left alone.
    retitled = disambiguate_titles([d for d in docs if not d.get("title_declared")])
    for d in docs:
        d["content_hash"] = content_key(f"{d.get('doc_url','')}|{d.get('title','')}")

    summary = {
        "name": hints.get("name"),
        "seed": seed,
        "engine": "formfill",
        "hints_version": hints.get("version"),
        "started_at": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(timespec="seconds"),
        "seconds": round(time.time() - started, 1),
        "listing_pages": len(page_log),
        # How the page sequence was decided. verify.py warns when it was
        # discovered from the site instead of frozen in the form, because a
        # discovered number can move between runs.
        "plan": plan_note,
        # What the site said about itself versus what we took. A gap here is a
        # coverage gap, and it is invisible to a stability check.
        "stated_vs_matched": stated_totals,
        "panels": [{"label": p["label"], "fragment": p["fragment"],
                    "text_len": p["text_len"]} for p in panels],
        "rows": len(rows),
        "records": len(records),
        "documents": len(docs),
        "titles_disambiguated": retitled,
        "phase2_ran": bool(targets),
        # Provenance, for the same reason as `source` below: a targeted run
        # re-read some detail pages and not others, so its records are not a
        # full crawl and nothing downstream may treat them as one.
        "targeted": targeted is not None,
        "detail_skipped": len(skipped),
        "fill_rates": _fill_rates(rows, hints),
        "blocked_pages": blocked,
        # Provenance. A replay produces the same rows as a crawl, which is what
        # makes it useful and also what makes it dangerous: unlabelled, a snapshot
        # run would tell change detection "unchanged" forever while the site moved
        # on. Everything downstream reads this.
        "source": "snapshot" if snap_html is not None else "live",
        "snapshot": str(snapshot) if snap_html is not None else "",
        "warnings": warnings,
        "pages": page_log,
    }

    # Write the HTML files BEFORE pages.json, so the html_file each record
    # points at is already stamped into the JSON too.
    html_written = _write_html_files(out, records) if any(r.get("html") for r in records) else 0
    summary["html_files"] = html_written

    if write_excel:
        # Written BEFORE run.json, or a failure here never reaches the file that
        # reports it. That is exactly what happened: results.xlsx was open in
        # Excel, the write raised PermissionError, the warning went into an
        # in-memory summary that had already been serialised, and the run
        # reported success next to a stale spreadsheet from a previous run.
        target = out / "results.xlsx"
        try:
            _write_excel(target, rows, records, docs)
            summary["xlsx"] = str(target)
        except PermissionError:
            # Almost always "open in Excel". Do not lose the data — write beside
            # it and say so loudly.
            alt = out / "results.locked.xlsx"
            try:
                _write_excel(alt, rows, records, docs)
                summary["xlsx"] = str(alt)
                summary["warnings"].append(
                    f"{target.name} is LOCKED (open in Excel?) — the file you are "
                    f"looking at is STALE. This run was written to {alt.name}.")
            except Exception as e:
                summary["warnings"].append(
                    f"excel write failed and the fallback failed too: {e}. "
                    "rows.json has the data.")
        except Exception as e:                    # never lose a crawl to a spreadsheet
            summary["warnings"].append(f"excel write failed: {e}. rows.json has the data.")

    (out / "pages.json").write_text(json.dumps(
        {"seed": seed, "shape": hints.get("shape"), "engine": "formfill",
         "source": summary["source"], "snapshot": summary["snapshot"],
         "pages": records, "documents": docs}, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "rows.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "run.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    emit({"event": "done", **{k: summary[k] for k in
                              ("rows", "records", "documents", "html_files", "seconds")},
          "warnings": len(summary["warnings"]), "out_dir": str(out)})
    for w in summary["warnings"]:
        emit({"event": "warning", "message": w})
    return summary


def _walk_tree(page, tree: dict, rx_fields: dict, page_url: str, rows: list, seen: set,
               section_prefix=()) -> dict:
    """Expand the menu, then read every node out of it.

    Returns the same {matched, new} the listing harvest returns, and appends the
    same row dicts — so everything downstream is unchanged.
    """
    cfg = {"menuSel": tree["menu_selector"], "nodeSel": tree["node_selector"],
           "linkSel": tree["link_selector"]}
    max_depth = int(tree.get("max_depth", 8))
    max_nodes = int(tree.get("max_nodes", 1000))

    # Most rulebook menus render collapsed: the deep nodes do not exist in the
    # DOM until something is clicked. Keep clicking until the node count stops
    # growing — that, not a fixed number of rounds, is when the tree is open.
    expand_sel = tree.get("expand_selector")
    rounds, before = 0, -1
    while expand_sel and rounds < 20:
        try:
            count = page.evaluate(
                "(s)=>document.querySelectorAll(s).length", cfg["nodeSel"])
        except Exception:
            break
        if count == before:
            break
        before = count
        try:
            page.evaluate(JS_EXPAND, expand_sel)
        except Exception:
            break
        page.wait_for_timeout(600)
        rounds += 1
    if expand_sel:
        emit({"event": "tree_expand", "rounds": rounds, "nodes": before})

    try:
        nodes = page.evaluate(JS_TREE, cfg)
    except Exception as e:
        emit({"event": "tree_error", "error": str(e)[:200]})
        return {"matched": 0, "new": 0}

    new = 0
    for n in nodes:
        if n["depth"] > max_depth or len(rows) >= max_nodes:
            continue
        href = n["href"]
        if not href.startswith("http") or href in seen:
            continue
        seen.add(href)
        fields = {"title": n["title"], "document_url": href}
        for target, rx in rx_fields.items():
            m = rx.search(n["title"])
            fields[target] = (m.group(1) if m and rx.groups else (m.group(0) if m else "")).strip()
        rows.append({
            "title": n["title"],
            "href": href,
            "row_text": n["title"],
            "found_on": page_url,
            "section_trail": [c for c in (list(section_prefix) + n["trail"]) if c],
            "fields": fields,
        })
        new += 1
    return {"matched": len(nodes), "new": new}


def _harvest(page, row_sel, link_sel, css_fields, rx_fields, page_url, rows, seen,
             section_prefix=(), section_levels=(), dedupe_scope="",
             root_sel=None, root_label="", root_fragment="",
             stamp_base=None) -> dict:
    """`root_sel` scopes the search to one panel; `stamp_base` marks each row so
    phase 2 can find it again in the same document."""
    try:
        res = page.evaluate(JS_ROWS, {"rowSel": row_sel, "linkSel": link_sel,
                                      "cssFields": css_fields,
                                      "sectionLevels": list(section_levels),
                                      "rootSel": root_sel, "rootLabel": root_label,
                                      "stampBase": stamp_base,
                                      # Set once per run from the hints, rather
                                      # than threaded through five _harvest call
                                      # sites that do not otherwise care.
                                      "collectLinks": COLLECT_ROW_LINKS["on"]})
    except Exception as e:
        emit({"event": "harvest_error", "url": page_url, "error": str(e)[:200]})
        return {"matched": 0, "new": 0}

    new = 0
    for raw in res.get("rows", []):
        fields = dict(raw.get("fields") or {})
        # Regex fields read the row's own visible text — the line that carries
        # the reference number, date and department the detail page often omits.
        for target, rx in rx_fields.items():
            m = rx.search(raw.get("row_text", ""))
            fields[target] = (m.group(1) if m and rx.groups else (m.group(0) if m else "")).strip()

        href = fields.get("document_url") or raw.get("href") or ""
        title = fields.get("title") or raw.get("link_text") or ""
        # Keyed on the link AND the title, not the link alone.
        #
        # SAMA lists circular 410333430000 twice at the same URL, once as "Rules on
        # Management of Problem Loans" and once as "Guidelines on Management of
        # Problem Loans". Keying on the URL threw the second away; they are two
        # entries the regulator chose to publish, so both come in. An exact repeat
        # of link AND title is still a duplicate and is still dropped.
        #
        # dedupe_scope additionally keeps the same file appearing under two tabs
        # as two placements (MOE's categories).
        key = (href or page_url, (title or "").strip().lower(), dedupe_scope)
        if not key or key in seen:
            continue
        seen.add(key)
        # Blank levels are dropped rather than left as empty crumbs: a row that
        # sits outside one of the groups should read "Laws > Financial Sector",
        # not "Laws >  > Financial Sector".
        trail = [c for c in (list(section_prefix) + list(raw.get("section") or [])) if c]
        # A row with no URL is identified by its place as well as its title: GOSI
        # panels 4 and 5 both hold a "SECTION III: PROVISIONS CONCERNING
        # VOLUNTARILY CONTRIBUTORS", and they are different documents.
        key = href or f"{page_url}::{'/'.join(trail)}::{title}"
        if not key.strip("::") or key in seen:
            continue
        seen.add(key)
        row = {
            "title": title,
            "href": href,
            "row_text": raw.get("row_text", ""),
            "found_on": page_url,
            "section_trail": trail,
            "fields": fields,
            # Only populated when the form asked for every link (see
            # COLLECT_ROW_LINKS). _record turns these into pdf_docs so the
            # pipeline can treat one row as an instrument with several files.
            "all_links": raw.get("all_links") or [],
        }
        if raw.get("ff_row") is not None and not href:
            row["in_page"] = f"[data-ff-row=\"{raw['ff_row']}\"]"
            row["panel_label"] = root_label
            # It has no URL of its own; the tab that reveals it is the closest
            # thing a reader can open. Identity comes from section_trail.
            row["href"] = fields["document_url"] = f"{page_url}{root_fragment}"
        rows.append(row)
        new += 1
    return {"matched": res.get("matched", 0), "new": new}


def _harvest_page(page, row_sel, link_sel, css_fields, rx_fields, page_url, rows, seen,
                  section_prefix, section_levels, panels) -> dict:
    """One harvest per panel, or one for the whole page when the form declares no
    panels. A form may declare panels and no row_selector (include_panel alone),
    in which case there is nothing to harvest."""
    if not row_sel:
        return {"matched": 0, "new": 0}
    if panels is None:
        return _harvest(page, row_sel, link_sel, css_fields, rx_fields, page_url,
                        rows, seen, section_prefix, section_levels)
    total = {"matched": 0, "new": 0}
    for p in panels:
        r = _harvest(page, row_sel, link_sel, css_fields, rx_fields, page_url, rows,
                     seen, section_prefix, section_levels,
                     root_sel=f"[data-ff-panel=\"{p['index']}\"]", root_label=p["label"],
                     root_fragment=p["fragment"], stamp_base=p["index"] * 100_000)
        total["matched"] += r["matched"]
        total["new"] += r["new"]
    return total


def _find_panels(page, tabs_sel: str) -> list[dict]:
    """Resolve the tab strip to panels, stamping each one. Deterministic, so
    running it again on a fresh load of the same page gives the same stamps."""
    try:
        return page.evaluate(JS_PANELS, tabs_sel)
    except Exception as e:
        emit({"event": "panels_error", "error": str(e)[:200]})
        return []


def _record(r: dict, section: str, seed: str, extra: dict | None = None) -> dict:
    """One row in generic_crawler's `pages.json` schema — same keys, same meaning."""
    trail = r.get("section_trail") or []
    # " > " is generic_crawler's separator (crawler.py::doc_section_path), so a
    # path from either engine reads the same downstream.
    path = " > ".join(trail) if trail else section
    # Empty unless the form asked for every link in the row. One link is left
    # empty on purpose: `url` already carries it, and listing it here as well
    # would make the pipeline explode a one-file row into a duplicate.
    _all = [f for f in (r.get("all_links") or [])
            if (f.get("href") or "").startswith("http")]
    _row_files = _all if len(_all) > 1 else []
    rec = {
        "section_path": path,
        "title": r["title"],
        "url": r["href"],
        "depth": 1,
        "linked_from_title": r["title"],
        "parent_page_url": r.get("found_on") or seed,
        "status": "",
        # A listing row used to hardcode these empty, because one row was one
        # file and its url was already `href`. A row that groups several files
        # has to carry them, or everything after the first is lost before the
        # pipeline ever sees it.
        "n_pdfs": len(_row_files),
        "pdf_links": " | ".join(f["href"] for f in _row_files),
        "text_len": 0,
        "html_file": "",
        "text": "",
        "html": "",
        "breadcrumb": trail or ([section] if section else []),
        "row_text": r.get("row_text", ""),
        "pdf_docs": _row_files,
        # The form's extracted fields, kept alongside so the orchestrator adapter
        # can map them onto RegulatoryDocument without re-parsing anything.
        "fields": r.get("fields", {}),
    }
    rec.update(extra or {})
    return rec


def _fill_rates(rows: list[dict], hints: dict) -> dict:
    """What fraction of rows actually got each field. The single most useful
    number for spotting a selector that matched the wrong thing: a regex that
    fires on 3% of rows is wrong, not 'partially working'."""
    # Always include title and document_url even when the form does not declare
    # them: a tree fills those from the menu node itself, and reporting "0%" for
    # a field that is in fact 100% populated would fail the gate on a bookkeeping
    # detail rather than on anything wrong with the crawl.
    targets = list(dict.fromkeys(["title", "document_url"]
                                 + list((hints.get("fields") or {}).keys())))
    n = len(rows) or 1
    return {t: round(100.0 * sum(1 for r in rows if (r["fields"].get(t) or "").strip()) / n, 1)
            for t in targets}


#: Anchor text that names an ACTION rather than a document. "Press Here" is the
#: one that reached the library — GOSI Social Insurance stored a whole
#: regulation under the title `Press Here` because the pattern below listed
#: "click here" but not "press here". ZATCA produced a `More` for the same
#: reason. These read as real titles in a library and are impossible to search.
_GENERIC_LINK_TEXT = re.compile(
    r"^((press|click|tap)(\s+(here|now))?"
    r"|download(\s+(the\s+)?(original\s+)?(pdf|file|document|here|now))?"
    r"|pdf|view|view\s+more|open|see\s+more|show\s+more|read(\s+more)?"
    r"|here|more|details?|link|attachment|file|document"
    r"|اضغط\s*هنا"          # "press here"
    r"|تحميل"                            # "download"
    r"|المزيد)\.?$",              # "more"
    re.I)


def _filename_title(url: str) -> str:
    """A readable name from the file itself — the last resort before storing an
    action word as a title. `.../tra00856_en-b-d-altrjmt.pdf` beats "Press Here"
    because it is at least specific to this document."""
    from urllib.parse import unquote, urlparse
    stem = unquote((urlparse(url or "").path or "").rsplit("/", 1)[-1])
    stem = re.sub(r"\.(pdf|docx?|xlsx?|pptx?|zip)$", "", stem, flags=re.I)
    stem = re.sub(r"[-_+]+", " ", stem)
    stem = re.sub(r"\s{2,}", " ", stem).strip()
    return stem


def _doc_title(link_text: str, row_title: str, url: str = "") -> str:
    """Prefer the row's title when the link's own text says nothing.

    Order: a meaningful link text, else the row/panel title, else the filename,
    and only if all three are empty does the action word stand.
    """
    t = (link_text or "").strip()
    if t and not _GENERIC_LINK_TEXT.match(t):
        return t
    row = (row_title or "").strip()
    if row:
        return row
    return _filename_title(url) or t


def _add_document(documents: dict, url: str, title: str, found_on: str, section: str,
                  declared: bool = False) -> None:
    """One row per FILE, not one per place the file is linked from.

    generic_crawler keys documents on (url, section_path) so a document
    cross-listed under two sections shows up under both. On a tree that backfires:
    the SAMA sandbox's "Print / Save as PDF" link appears on every page of a
    section, so three actual PDFs were reported as thirty-nine documents — a
    number that reads like coverage and is really repetition.

    So the key is the URL, the first section wins (breadth-first order means that
    is the shallowest one), and the other placements are kept in `also_in` rather
    than thrown away. Deduplicated, the sandbox reports 3 — which is what
    generic_crawler's baseline says too.

    `declared` says WHERE the title came from, and it is the one thing that can
    overwrite an existing one:

        declared=True   the form named this row a document entry and the gate
                        measured how often that field fills. SIMAH's <h6> is
                        "The Implementing Regulations for Credit Information Law".
        declared=False  scraped from an anchor while scanning a page. SIMAH's
                        anchor text is "Download PDF", which is not a title — the
                        form file says so in a comment.

    First-sighting-wins gave the anchor the last word whenever a page linked its own
    declared row (`include_page`), so the library got "Download PDF". A declared
    title now replaces a scraped one; nothing else about precedence changes, and a
    declared title is never downgraded. Where a URL has only one kind of sighting —
    every other form today — this is a no-op.
    """
    d = documents.get(url)
    if d is None:
        documents[url] = {
            "title": title,
            "doc_url": url,
            "type": _ext_type(url),
            "found_on": found_on,
            "section_path": section,
            "times_linked": 1,
            "also_in": "",
            "title_declared": bool(declared),
        }
        return
    d["times_linked"] += 1
    if declared and not d.get("title_declared") and (title or "").strip():
        d["title"] = title
        d["title_declared"] = True
    if section and section != d["section_path"]:
        others = [s for s in d["also_in"].split(" | ") if s]
        if section not in others and len(others) < 20:
            others.append(section)
            d["also_in"] = " | ".join(others)


def _scraped_doc_title(link: dict, fallback: str) -> str:
    """Anchor text unless it is a call to action, then the nearest heading, then
    the URL slug — generic_crawler's `best_doc_title` order, reusing its word
    list. Only the in-page path uses this; changing the ordinary detail path
    would move titles on six approved forms nothing has re-measured.
    """
    t = (link.get("text") or "").strip().replace("\xa0", " ")
    if t.lower() not in _GENERIC_TEXT and len(t) > 3:
        return t[:200]
    ctx = (link.get("ctx") or "").strip()
    if len(ctx) > 3:
        return ctx[:200]
    return title_from_slug(link.get("href") or "") or fallback


_BLANK_RUN_RE = re.compile(r"(?:[ \t]*\r?\n){2,}")


def _tidy_html(html: str) -> str:
    """Trim the template indentation that makes a full cell look empty.

    innerHTML keeps the page template's own whitespace, and an ASP.NET master
    page opens with dozens of blank lines. Measured on ZATCA: every
    document_html began with 133 whitespace characters containing 37 newlines,
    so Excel rendered the cell as EMPTY while it held 11,927 characters and the
    workbook read as a failed crawl.

    Done in PYTHON rather than inside JS_DETAIL deliberately. The JS version
    needs escaped tab and newline inside a regex literal; written through a
    heredoc those became REAL control characters, the regex literal spanned a
    line, the whole snippet turned into a syntax error, and every evaluate()
    threw — all 34 rows silently stored no HTML at all.

    Content is never touched: only runs of blank lines collapse, and a document
    containing <pre> is returned with its whitespace intact.
    """
    if not isinstance(html, str) or not html:
        return html or ""
    if "<pre" in html.lower():
        return html.strip()
    return _BLANK_RUN_RE.sub("\n", html).strip()



#: Query parameters that change between requests for the SAME file, and so must
#: never reach a stored url.
#
# A url is not just a link here: for a multi-attachment row it is part of the
# IDENTITY (doc_path + extra_meta.attachment_links), so a parameter that moves
# per request makes the row look new on every crawl. That has now been measured
# on two regulators:
#
#   Ministry of Commerce  dt=<DDMMYYYYHHMMSS>  the moment of the crawl.
#                         Duplicated all 16 attachment rows on one re-crawl,
#                         and would have added 16 more every run after that.
#   CMA                   csrt=<digits>        a CSRF token, plus a literal
#                         `undefined=undefined` from a bug in their page.
#
# Verified on MC before removing anything: with and without `dt` the endpoint
# returned the same 261,632-byte PDF. These are cache-busters and session
# tokens, not content selectors — `attId`, `lawId` and the like are NOT here and
# must never be added, because those DO name the document.
_VOLATILE_PARAMS = re.compile(
    r"[?&](?:dt|csrt|_|ts|nonce|sid|session|token|undefined)=[^&]*", re.I)


def stable_url(url: str) -> str:
    """A url with per-request parameters removed, safe to store and compare."""
    if not url:
        return url
    out = _VOLATILE_PARAMS.sub("", str(url))
    # The first surviving parameter has to start the query again.
    if "?" not in out and "&" in out:
        out = out.replace("&", "?", 1)
    return out.replace("?&", "?", 1).rstrip("?&")


def _is_doc(url: str) -> bool:
    """Is this link a FILE rather than a page?

    Three ways a site can say so, and a document endpoint often has no file
    extension at all:

      1. an extension            .../law.pdf
      2. a download PATH         .../download/123, .../document/abc, wpdmdl=
      3. a download PARAMETER    /regapis?...&op=Download&attId=<guid>

    (3) was missing, and Ministry of Commerce is built entirely on it: every one
    of its 144 files is a `regapis` endpoint with no extension and no download
    path segment. Under the old test none of them counted as a document, so
    moving MC to a form produced 20 correctly-named laws carrying ZERO files —
    while the generic engine, whose is_document_link() checks the query string,
    had been finding all 144.

    Matched on the PARAMETER, not on the substring "download" anywhere in the
    url, so a page that merely mentions the word is not mistaken for a file.
    """
    return bool(_DOC_EXT_RE.search(url)) or bool(
        re.search(r"wpdmdl=|/document/|/download/", url, re.I)) or bool(
        re.search(r"[?&](?:op|action|mode|type)=download(?:&|$)|[?&]download=", url, re.I))


def _ext_type(url: str) -> str:
    m = _DOC_EXT_RE.search(url)
    return m.group(1).upper() if m else "DOC"


def _write_html_files(out: Path, records: list[dict]) -> int:
    """Save each page's HTML next to the data and record the filename.

    The HTML is always captured — it just used to live only inside pages.json,
    where nobody looking at the spreadsheet could tell it existed. A file per
    page makes it visible and lets you open one without parsing JSON.
    """
    hdir = out / "html"
    hdir.mkdir(parents=True, exist_ok=True)
    written = 0
    for i, rec in enumerate(records, 1):
        html = rec.get("html") or ""
        if not html:
            continue
        slug = re.sub(r"[^a-z0-9]+", "-", (rec.get("title") or "page").lower()).strip("-")[:60]
        name = f"{i:04d}_{slug or 'page'}.html"
        # Wrapped in a minimal document with <base href>. The captured fragment
        # keeps the site's own relative paths — SAMA's icons are
        # src="/sites/default/files/en_net_file_store/2022-1.jpg" — which resolve
        # against file:// when the saved page is opened and render as broken
        # images. <base> points them back at the origin.
        url = rec.get("url", "")
        origin = ""
        try:
            u = urlparse(url)
            origin = f"{u.scheme}://{u.netloc}/" if u.scheme and u.netloc else ""
        except Exception:
            pass
        head = (f'<!doctype html>\n<html><head><meta charset="utf-8">\n'
                f'<base href="{origin}">\n<title>{(rec.get("title") or "")[:150]}</title>\n'
                f'</head><body>\n<!-- source: {url} -->\n')
        (hdir / name).write_text(head + html + "\n</body></html>\n", encoding="utf-8")
        rec["html_file"] = f"html/{name}"
        written += 1
    return written


def _write_excel(path: Path, rows: list[dict], records: list[dict], documents: list[dict]) -> None:
    """Three sheets. `inventory` is the one a reviewer reads: one line per
    document, with the extracted fields, the section path, and — when phase 2
    ran — how much text was captured and which file holds the HTML.

    The raw HTML itself is deliberately NOT a column: it averages 8 KB a page,
    which blows past Excel's 32k cell limit and makes the sheet unreadable. The
    `html_file` column points at it instead.
    """
    import pandas as pd
    CELL = 32000
    TEXT_PREVIEW = 2000

    # Prefer records: after phase 2 they carry everything the rows carry PLUS
    # the page text, the PDF links and the HTML filename.
    if records:
        inventory = [{
            "section_path": rec.get("section_path", ""),
            "title": rec.get("title", ""),
            **{k: v for k, v in (rec.get("fields") or {}).items() if k != "title"},
            "url": rec.get("url", ""),
            "text_len": rec.get("text_len", 0),
            "n_pdfs": rec.get("n_pdfs", 0),
            "pdf_links": (rec.get("pdf_links") or "")[:CELL],
            "html_file": rec.get("html_file", ""),
            "text_preview": (rec.get("text") or "")[:TEXT_PREVIEW],
            "row_text": (rec.get("row_text") or "")[:CELL],
        } for rec in records]
    else:
        inventory = [{"section_path": " > ".join(r.get("section_trail") or []),
                      "title": r["title"],
                      **{k: v for k, v in r["fields"].items() if k != "title"},
                      "url": r["href"], "found_on": r["found_on"],
                      "row_text": r["row_text"][:CELL]} for r in rows]

    pages = [{k: (str(v)[:CELL] if isinstance(v, (str, list)) else v)
              for k, v in rec.items() if k not in ("html", "fields")} for rec in records]

    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        pd.DataFrame(inventory or [{"note": "no rows"}]).to_excel(
            xl, sheet_name="inventory", index=False)
        pd.DataFrame(pages or [{"note": "no pages"}]).to_excel(
            xl, sheet_name="pages", index=False)
        pd.DataFrame(documents or [{"note": "no documents"}]).to_excel(
            xl, sheet_name="documents", index=False)
