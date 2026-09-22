"""
CMA — Capital Market Authority, Laws & Regulations.

    venv/Scripts/python.exe site_runners/cma_laws.py                    # Capital Market Law
    venv/Scripts/python.exe site_runners/cma_laws.py --max-chapters 2   # quick check
    venv/Scripts/python.exe site_runners/cma_laws.py --headed

WHY THIS IS A RUNNER AND NOT A FORM
-----------------------------------
Everything else we onboard goes through generic_crawler (no config) or a formfill
form (twelve fields). CMA needs neither to bend, for three reasons that compound:

1. ARTICLES ARE NOT LINKS. A chapter page looks like an accordion, but the bodies
   are genuinely empty — 0 characters in textContent, not merely hidden. The real
   destination sits in an onclick handler:

       <button onclick="window.location.href='/en/.../CH1/Pages/CH1Article2.aspx'"
               data-bs-target="#flush-collapse/en/.../ch1/pages/ch1article2.aspx">

   Both engines discover links via `a[href]`, so every article is invisible to
   them. Worse, the id in data-bs-target is LOWERCASE and 404s — only the
   onclick URL works. (generic_crawler now recognises this onclick pattern for
   ordinary link-walking; that fix does not give us the rest of this file.)

2. TEN TABS, ONE SHAPE. Capital Market Law is the first of ten
   (Implementing Regulations, Circulars, Guides, Forms, ...) and they share this
   structure. One runner covers them; ten-plus forms would not.

   (An earlier draft also cited the folder naming: the agreed tree showed
   "Chapter 1-Definitions" where the site says "Chapter One Definitions". That
   was settled in favour of the site's own wording, so it is no longer a reason
   for anything — reason 1 alone is what keeps this out of the two engines.)

OUTPUT
------
`output/site_runners/cma_<tab>/pages.json` in EXACTLY the schema generic_crawler
and formfill emit, so the same pipeline adapter reads all three without caring
which produced it.
"""

import argparse
import json
import os
import re
import sys
import time
import pathlib
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE = "https://cma.gov.sa"

# The Laws & Regulations tabs. Only the first is wired up so far; the others are
# listed because they are the reason this is a runner, and adding one should be a
# line here plus a check that the chapter/article shape holds.
# Every Laws & Regulations tab, with the SHAPE its handler uses. URLs were read
# off the site's own nav rather than guessed.
#
# Six shapes cover nine tabs — which is the argument for one runner over nine
# forms, and also the warning: each shape is a separate thing that can break.
TABS = {
    "capital_market_law": {
        "url": f"{BASE}/en/RulesRegulations/CMALaw/Pages/default.aspx",
        "label": "Capital Market Law", "shape": "law_chapters",
        "chapter_re": re.compile(r"/CMALaw/CH(\d+)/", re.I),
    },
    "sifi": {
        "url": f"{BASE}/en/RulesRegulations/Law-of-Systemically-Important-"
               f"Financial-Institutions/Pages/default.aspx",
        "label": "Law of Systemically Important Financial Institutions",
        "shape": "single_page",
    },
    "forms": {
        "url": f"{BASE}/en/RulesRegulations/FormsSite/Pages/default.aspx",
        "label": "Forms", "shape": "single_page",
    },
    "cpe": {
        "url": f"{BASE}/en/Market/Pages/education.aspx",
        "label": "Continuous Professional Education (CPE) Policy",
        "shape": "single_page",
    },
    # --- not implemented yet; listed so the gap is visible rather than implied
    "guides": {
        "url": f"{BASE}/en/RulesRegulations/Guides/Pages/default.aspx",
        "label": "Guides", "shape": "cards",
    },
    "circulars": {
        "url": f"{BASE}/en/RulesRegulations/circulars/Pages/default.aspx",
        "label": "Circulars", "shape": "cards_grouped",
    },
    "public_consultation": {
        "url": f"{BASE}/en/RulesRegulations/Consulting/Pages/default.aspx",
        "label": "Public Consultation", "shape": "tabs_cards_detail",
    },
    "implementing_regulations": {
        "url": f"{BASE}/en/RulesRegulations/Regulations/Pages/default.aspx",
        "label": "Implementing Regulations", "shape": "subtabs_paginated_detail",
    },
    "faqs": {
        "url": f"{BASE}/en/RulesRegulations/FAQ/Pages/FAQs.aspx",
        "label": "FAQs for Implementing Regulations", "shape": "faq_paginated",
    },

    # ---------------- Media Center ----------------
    #
    # 3,297 announcements over 550 pages of six. Unlike every Laws & Regulations
    # tab, this one does NOT hold the whole list in the DOM — see crawl_paged().
    "announcements": {
        "url": f"{BASE}/en/MediaCenter/NEWS/Pages/default.aspx",
        "section": "Media Center", "label": "Announcements",
        "shape": "cards_paged", "detail": True,
        # Most announcements are plain-text press releases with no attached
        # PDF, so keying `documents` on attachments alone (the default for
        # this shape) dropped ~92% of them. Here the article page IS the
        # regulatory content, so crawl_paged() also emits the page itself as
        # a document. See the note beside its use in crawl_paged().
        "text_as_document": True,
        # Full history is 3,299 items over 550 pages -- 1.5+ hours and CMA
        # throttles hard against it. Ran the 12-month window (365) first,
        # 2026-08-12/13. FULL BACKFILL requested 2026-08-13: since_days
        # removed below so the cutoff never fires and the whole 3,299 gets
        # walked in one pass, via the resumable CLI (checkpoints every 100
        # records) rather than the production wrapper, since a run this long
        # losing all progress to one crash or one bad throttle is a real risk
        # the wrapper's single monolithic call does not protect against.
        # MONITORING READS A WINDOW, NOT THE HISTORY.
        #
        # None means "walk all 3,299 over 550 pages" — right for the one-off
        # backfill, wrong for a scheduled run: measured 2026-08-16 that walk took
        # 2h49m and STILL returned 300 of the 1,053 announcements we hold, which
        # would rule the other 753 `disappeared` and raise them as withdrawal
        # proposals.
        #
        # Announcements are ordered NEWEST FIRST, so monitoring only needs to
        # reach the newest one already stored — a page or two. CMA_SINCE_DAYS
        # sets that window (jobs/monitor_jobs.monitor_cma passes it); leave it
        # unset for the full backfill.
        "since_days": int(os.environ["CMA_SINCE_DAYS"])
                      if os.environ.get("CMA_SINCE_DAYS") else None,
    },

    # ---------------- Capital Market ----------------
    "prospectuses": {
        "url": f"{BASE}/en/Market/Prospectuses/Pages/default.aspx",
        "section": "Capital Market", "label": "Prospectuses",
        "shape": "cards_paged", "detail": False, "subtab_paths": True,
    },
    "shareholder_circulars": {
        "url": f"{BASE}/en/Market/Circulars/Pages/default.aspx",
        "section": "Capital Market", "label": "Shareholder Circulars",
        "shape": "cards_paged", "detail": False, "subtab_paths": True,
    },
    "facilitating_accounts": {
        "url": f"{BASE}/en/Market/Pages/"
               f"Facilitating_the_Opening_of_Investment_Accounts_Initiative.aspx",
        "section": "Capital Market",
        "label": "Facilitating Opening Investment Accounts Initiative",
        "shape": "single_page",
    },
    "fintech_lab": {
        "url": f"{BASE}/en/Market/FinTech/Pages/Default.aspx",
        "section": "Capital Market", "label": "FinTech Lab",
        "shape": "single_page",
    },
    "foreign_investors": {
        "url": f"{BASE}/en/Market/QFI/Pages/default.aspx",
        "section": "Capital Market", "label": "Foreign Investors",
        "shape": "single_page",
    },

    # --- REGISTERS, deliberately not implemented as document crawls.
    # These are lists of licensed entities, not regulatory documents: 237 market
    # institutions, 890 special purpose entities, 382 funds, ~100 accounting
    # offices, 1 real estate contribution. Running 890 table rows through the
    # 4-stage LLM extraction and requirement matching costs real money and
    # produces nothing a compliance library can use — a row saying
    # "Sukuk Morabha 2409 | Effective | Debt-Based" contains no requirement.
    # They belong in a table, with the documents they LINK TO (articles of
    # association, fund rules, transparency reports, issuance brochures)
    # crawled as documents. See CAPITAL_MARKET.md for the proposal.
    "cm_institutions": {
        "url": f"{BASE}/en/Market/AuthorisedPersons/Pages/default.aspx",
        "section": "Capital Market", "label": "Financial Market Institutions",
        "shape": "register",
    },
    "spes": {
        "url": f"{BASE}/en/Market/SPEs/Pages/default.aspx",
        "section": "Capital Market", "label": "Special Purpose Entities",
        "shape": "register",
    },
    "accounting_firms": {
        "url": f"{BASE}/en/Market/rafs/Pages/default.aspx",
        "section": "Capital Market", "label": "Registered Accounting Offices",
        "shape": "register",
    },
    "investment_funds": {
        "url": f"{BASE}/en/Market/imf/Pages/default.aspx",
        "section": "Capital Market", "label": "Investment Funds",
        "shape": "register",
    },
    "real_estate_contributions": {
        "url": f"{BASE}/en/Market/RealestateContributions/Pages/default.aspx",
        "section": "Capital Market", "label": "Real Estate Contributions",
        "shape": "register",
    },
}

IMPLEMENTED = {"law_chapters", "single_page", "cards", "cards_grouped",
               "tabs_cards_detail", "subtabs_paginated_detail", "faq_paginated",
               "cards_paged", "register"}

# Public Consultation is an IFRAME. /en/RulesRegulations/Consulting/Pages/
# default.aspx renders an empty shell whose only real content is an iframe
# pointing at the URL below — note it has NO /en/ segment, and adding one
# returns "Access Error". Crawling default.aspx therefore finds nothing at all,
# which is exactly the silent-zero this runner is built to avoid.
CONSULT_INNER = f"{BASE}/RulesRegulations/Consulting/Pages/ENPublicConsultion.aspx"

# The path the library must end up with, per the agreed tree:
#   Capital Market Authority (CMA) > Laws & Regulations > Capital Market Law
#     > Chapter One Definitions > Article One
# Hierarchy as agreed; every NAME is the site's own wording.
#
# The second level is the site's own top-level menu — "Laws & Regulations",
# "Media Center" or "Capital Market" — so a tab's folder matches where a person
# would have found it. Tabs default to Laws & Regulations, which is where this
# runner started.
REGULATOR = "Capital Market Authority (CMA)"
DEFAULT_SECTION = "Laws & Regulations"


def tab_root(tab) -> list:
    return [REGULATOR, tab.get("section", DEFAULT_SECTION), tab["label"]]

def chapter_folder(link_text: str, url: str) -> str:
    """The chapter folder name, in the SITE'S OWN WORDING.

    So "Chapter One Definitions" stays exactly that — it is not normalised to
    "Chapter 1-Definitions". Whitespace is collapsed and nothing else is touched:
    the regulator's wording is the thing we are mirroring, and any rewriting rule
    is one more thing to maintain and to disagree with the source about.
    """
    return re.sub(r"\s+", " ", link_text or "").strip()


_REF_NO_RE = re.compile(r"/([^/]+)\.aspx$", re.I)


def _reference_no_from_url(url: str) -> str:
    """CMA's detail pages are named after the reference itself,
    e.g. '.../CMA_N_4088.aspx' -> 'CMA_N_4088'. Empty when the url is not
    that shape (a chapter/article page, a bare tab url as fallback, etc.)."""
    m = _REF_NO_RE.search(url or "")
    return m.group(1) if m else ""


def _one_or_many(file_hrefs, fallback_url=""):
    """document_url / attachment_links for one item's attached files, per the
    convention in models.RegulatoryDocument: one file names the document
    directly; more than one leaves document_url empty and lists them in
    extra_meta["attachment_links"] instead, because naming a multi-file item
    by whichever file happened to be listed first makes the item's identity
    depend on the site's ordering. `fallback_url` is used only when there is
    no file at all -- the page/card's own url, so the row still identifies.
    """
    hrefs = list(dict.fromkeys(h for h in file_hrefs if h))    # de-dup, order kept
    if len(hrefs) == 1:
        return hrefs[0], ""
    if len(hrefs) > 1:
        return "", " | ".join(hrefs)
    return fallback_url, ""


# ---------------------------------------------------------------------------
# browser-side readers
# ---------------------------------------------------------------------------

JS_CHAPTER_LINKS = r"""(sel) => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  const rx = new RegExp(sel, 'i');
  const out = [], seen = new Set();
  for (const a of document.querySelectorAll('a[href]')) {
    if (!rx.test(a.href)) continue;
    const t = clean(a.innerText);
    if (!t) continue;
    const key = a.href.replace(/\/$/, '').toLowerCase();
    if (seen.has(key)) continue;            // the page lists each chapter twice
    seen.add(key);
    out.push({text: t, href: a.href});
  }
  return out;
}"""

# An article is a button whose onclick navigates. The data-bs-target id is
# lowercase and 404s, so the onclick URL is the only usable one.
JS_ARTICLES = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  const out = [];
  for (const it of document.querySelectorAll('.accordion-item')) {
    const btn = it.querySelector('.accordion-button');
    if (!btn) continue;
    const oc = btn.getAttribute('onclick') || '';
    const m = oc.match(/location\.href\s*=\s*'([^']+)'/i)
           || oc.match(/location\.href\s*=\s*"([^"]+)"/i);
    const body = it.querySelector('.accordion-collapse');
    out.push({
      name: clean(btn.innerText),
      href: m ? m[1] : '',                       // empty for the open one
      open: body ? /\bshow\b/.test(body.className) : false,
      chars: body ? clean(body.textContent).length : 0,
    });
  }
  return out;
}"""

# The article body is the accordion pane that is open on its own page.
JS_ARTICLE_BODY = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  let el = document.querySelector('.accordion-collapse.show');
  if (!el) {
    const items = document.querySelectorAll('.accordion-collapse');
    for (const c of items) { if (clean(c.textContent).length > 40) { el = c; break; } }
  }
  // Some chapter pages render their current article OUTSIDE the accordion
  // entirely (CH8 shows Article Forty Nine with no open pane at all), so the
  // rich-text field is the only place the text exists.
  if (!el) {
    for (const c of document.querySelectorAll('.ms-rtestate-field')) {
      if (clean(c.textContent).length > 40) { el = c; break; }
    }
  }
  if (!el) return {html: '', text: '', title: ''};
  const clone = el.cloneNode(true);
  clone.querySelectorAll('script,style,noscript,nav,header,footer,button').forEach(n => n.remove());
  let text = clean(clone.textContent);
  // Every pane opens with the literal words "Page Content" — chrome, not law.
  text = text.replace(/^Page Content\s*/i, '');
  return {html: clone.innerHTML, text: text,
          title: clean((document.querySelector('h1')||{}).innerText||'')};
}"""


JS_PAGE_CONTENT = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  // The page's own rich-text field, not <body>: body would drag in the mega-menu,
  // the ticker and the footer, which on CMA is most of the character count.
  let el = null;
  for (const sel of ['.ms-rtestate-field', 'main', '[role=main]', '#content', '.content']) {
    for (const c of document.querySelectorAll(sel)) {
      if (clean(c.textContent).length > 80) { el = c; break; }
    }
    if (el) break;
  }
  if (!el) el = document.body;
  const clone = el.cloneNode(true);
  clone.querySelectorAll('script,style,noscript,nav,header,footer').forEach(n => n.remove());
  const docs = Array.from(el.querySelectorAll('a[href]'))
    .filter(a => /\.(pdf|docx?|xlsx?)(\?|$)/i.test(a.href))
    .map(a => ({title: clean(a.innerText), href: a.href}));
  // "Last modified date:12/11/2025 - 12:01 AM Saudi Arabia time"
  const m = clean(document.body.innerText)
              .match(/Last modified date:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})/i);
  return {title: clean((document.querySelector('h1')||{}).innerText||''),
          text: clean(clone.textContent), html: clone.innerHTML,
          docs, last_modified: m ? m[1] : ''};
}"""


# --- cards -----------------------------------------------------------------
#
# Guides, Circulars, Implementing Regulations and Public Consultation all render
# `div.card-wrapper`, and — this is the useful part — the card's PARENT carries
# the metadata as data-* attributes:
#
#   Guides      data-id data-page
#   Circulars   data-id data-page data-categoryid data-categoryname
#   Regulations data-id data-page data-title data-year data-month data-category
#
# So the category a card belongs to is readable from the DOM. That matters:
# the obvious implementation is to click each sub-tab or pick each dropdown
# option and record what becomes visible, which is 6-30 round trips, races with
# the site's own filter JS, and gives a wrong answer if a click silently fails.
# Reading the attribute cannot half-work.
#
# One trap: these pages ship the SharePoint list-view web part that feeds the
# grid, and it renders the SAME cards a second time inside
# `table#onetidDoclibViewTbl0`. Circulars therefore reports 12 card-wrappers for
# 6 circulars. Filtering on visibility would be wrong — Guides and Regulations
# legitimately keep pages 2+ hidden — so the raw source table is excluded by
# LOCATION instead: a card inside a <table> is the data source, not the list.
JS_CARDS = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  const abs = h => { try { return new URL(h, location.href).href } catch(e) { return '' } };
  return Array.from(document.querySelectorAll('div.card-wrapper'))
    .filter(c => !c.closest('table'))
    .map(c => {
    const data = {};
    const p = c.parentElement;
    if (p) for (const a of p.attributes)
      if (a.name.startsWith('data-')) data[a.name.slice(5)] = a.value;
    // innerText is EMPTY on a display:none element, and the pager hides every
    // card except the current page — so reading the whole DOM window silently
    // lost the text of the hidden ones. Prospectuses kept 240 rows but only 63
    // dates. textContent has no such rule; it is the fallback, not the default,
    // because innerText respects line breaks and reads better.
    const txt = el => clean(el.innerText) || clean(el.textContent);
    const h = c.querySelector('h2,h3,h4,h5');
    const links = Array.from(c.querySelectorAll('a[href]')).map(a => ({
        text: clean(a.innerText) || clean(a.textContent),
        title: a.getAttribute('title') || '',
        href: abs(a.getAttribute('href') || '')}))
      .filter(l => l.href && !/^javascript:/i.test(l.href));
    const dt = c.querySelector('.date, .info-wrapper');
    return {
      data,
      // data-title is the site's own clean title; the h-tag is next best; the
      // card's flattened text is the last resort and carries button labels.
      title: data.title || (h ? txt(h) : '') || txt(c),
      text: txt(c),
      date_text: dt ? txt(dt) : '',
      files: links.filter(l => /\.(pdf|docx?|xlsx?|zip)(\?|$)/i.test(l.href)),
      // "Read More" on Announcements, "More" on consultations, a details.aspx
      // href on Implementing Regulations. Omitting the first found no detail
      // link on any of 300 announcements and captured no body text at all.
      detail: (links.find(l => /^(read more|more|details)$/i.test(l.text))
            || links.find(l => /details?\.aspx/i.test(l.href))
            || {}).href || '',
      links,
    };
  });
}"""

# "1 2 3 Total 3 Pages" — the site states its own page count. Every one of these
# tabs paginates by toggling a `hideRowItem` class rather than fetching, so all
# rows are already in the DOM; the stated total is what proves we are not
# looking at a partially-rendered list.
JS_PAGER_TOTAL = r"""() => {
  const e = document.querySelector('ul.pagination.pagination-container');
  if (!e) return 0;
  const m = (e.innerText||'').replace(/\s+/g,' ').match(/Total\s+(\d+)\s+Pages?/i);
  return m ? parseInt(m[1], 10) : 0;
}"""

# The excluded source table is worth one last read: it prints its own row count
# ("Count= 13"), which is the list length straight from SharePoint. That is a
# free, independent check on the rendered grid — the kind of ground truth almost
# no regulator gives us, and the only thing here that can catch a card the
# page's own JS failed to draw.
JS_SOURCE_COUNT = r"""() => {
  for (const t of document.querySelectorAll('table.ms-listviewtable, table[id^=onetidDoclibViewTbl]')) {
    const m = (t.innerText||'').replace(/\s+/g,' ').match(/Count=\s*(\d+)/i);
    if (m) return parseInt(m[1], 10);
  }
  return 0;
}"""

# A Regulation Details page is metadata, not prose: title, publishing date, one
# PDF, last-modified. Take the SMALLEST element containing "Publishing date"
# so the mega-menu and footer stay out — <body> on CMA is mostly chrome.
JS_REG_DETAIL = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  let el = null, best = 1e9;
  for (const c of document.querySelectorAll('div,section,article')) {
    const t = clean(c.innerText);
    if (t.length > 30 && t.length < best && /Publishing date\s*:/i.test(t)) { best = t.length; el = c; }
  }
  const body = clean(document.body.innerText);
  const pub = body.match(/Publishing date\s*:\s*([0-9]{4}\/[0-9]{1,2}\/[0-9]{1,2}|[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})/i);
  const lm  = body.match(/Last modified date\s*:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})/i);
  let text = '', html = '', title = '';
  if (el) {
    const clone = el.cloneNode(true);
    clone.querySelectorAll('script,style,noscript,nav,header,footer,svg').forEach(n => n.remove());
    html = clone.innerHTML;
    // innerText, not textContent: this element is rendered, and textContent
    // welds adjacent blocks together ("...FundsPublishing date:"), which is what
    // the extraction and the LLM downstream actually read.
    text = clean(el.innerText) || clean(clone.textContent);
    const h = el.querySelector('h1,h2,h3,h4');
    title = clean(h ? h.innerText : '');
  }
  if (!title) {
    // "… Regulation Details <TITLE> Publishing date: …"
    const m = body.match(/Regulation Details\s+(.+?)\s+Publishing date\s*:/i);
    title = m ? clean(m[1]) : '';
  }
  const files = Array.from(document.querySelectorAll('a[href]'))
    .filter(a => /\.(pdf|docx?|xlsx?)(\?|$)/i.test(a.href))
    .map(a => ({title: clean(a.innerText) || clean(a.getAttribute('title')||''), href: a.href}));
  return {title, text, html, files,
          published: pub ? pub[1] : '', last_modified: lm ? lm[1] : ''};
}"""

# The consultation page's own tab strip: label -> pane id. Read rather than
# assumed, because the site's ids do not match its labels — "Active
# Consultation" targets #first-tab-pane, whose class is `expiresectionContainer`.
# Trusting the class name would file every active consultation as expired.
JS_CONSULT_TABS = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  return Array.from(document.querySelectorAll('[data-bs-toggle=tab],[data-bs-toggle=pill]'))
    .map(a => ({label: clean(a.innerText),
                pane: (a.getAttribute('data-bs-target') || a.getAttribute('href') || '')}))
    .filter(x => x.label && x.pane.startsWith('#'));
}"""

JS_PANE_CARDS = r"""(paneSel) => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  const abs = h => { try { return new URL(h, location.href).href } catch(e) { return '' } };
  const pane = document.querySelector(paneSel);
  if (!pane) return null;
  return Array.from(pane.querySelectorAll('div.card-wrapper')).map(c => {
    const h = c.querySelector('h2,h3,h4,h5');
    const d = c.querySelector('.info-wrapper, .date');
    const links = Array.from(c.querySelectorAll('a[href]'))
      .map(a => ({text: clean(a.innerText), title: a.getAttribute('title')||'',
                  href: abs(a.getAttribute('href')||'')}))
      .filter(l => l.href && !/^javascript:/i.test(l.href));
    return {title: clean(h ? h.innerText : '') || clean(c.innerText),
            date_text: clean(d ? d.innerText : ''),
            detail: (links.find(l => /^more$/i.test(l.text)) || {}).href || '',
            files: links.filter(l => /\.(pdf|docx?|xlsx?)(\?|$)/i.test(l.href))};
  });
}"""

# The FAQ accordion answers are in the DOM whether or not the item is open, so
# nothing has to be clicked — 418 questions read in one evaluate().
JS_FAQ_ITEMS = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  return Array.from(document.querySelectorAll('.accordion-item')).map(it => {
    const b = it.querySelector('.accordion-button, button');
    const body = it.querySelector('.accordion-body') || it.querySelector('.accordion-collapse');
    let html = '';
    if (body) {
      const clone = body.cloneNode(true);
      clone.querySelectorAll('script,style,svg').forEach(n => n.remove());
      html = clone.innerHTML;
    }
    return {id: it.getAttribute('data-id') || '',
            page: it.getAttribute('data-page') || '',
            q: clean(b ? b.innerText : ''),
            a: clean(body ? body.textContent : ''),
            html};
  }).filter(x => x.q);
}"""

# Left-hand filter: one checkbox per regulation, plus "All". Used only to LABEL
# the questions — coverage comes from the unfiltered read above, so a filter that
# misbehaves costs a folder name, never a document.
JS_FAQ_BOXES = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  return Array.from(document.querySelectorAll('input[type=checkbox]')).map(c => {
    const lab = document.querySelector('label[for="' + c.id + '"]') || c.closest('label') || c.parentElement;
    return {id: c.id, label: clean(lab ? lab.innerText : '')};
  }).filter(x => x.id && x.label);
}"""

JS_FAQ_SHOWN = r"""() => Array.from(document.querySelectorAll('.accordion-item'))
  .filter(e => !/hideRowItem/.test(e.getAttribute('class') || ''))
  .map(e => e.getAttribute('data-id') || '')"""


# Every coverage_gap the runner reports, in the order reported. The runner only
# ever PRINTED them, and nothing reads a runner's stdout, so a tab that returned
# 81 of 266 rows was called trustworthy: the crawler knew, the gate never did.
# crawler/cma_crawler_wrapper.py drains this after each tab and hands it to the
# completeness gate. Cleared by the wrapper before a run, not here.
GAPS: list = []


def report_gap(**fields):
    """Print a coverage_gap event AND record it where the gate can see it."""
    event = {"event": "coverage_gap", **fields}
    GAPS.append(event)
    print(json.dumps(event), flush=True)


def load(page, url, wait_ms=1500, tries=3):
    """CMA is generally well behaved, but a blip must not be read as 'no articles'."""
    for _ in range(tries):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass
            page.wait_for_timeout(wait_ms)
            if page.evaluate("()=>document.querySelectorAll('a[href]').length") > 10:
                return True
        except Exception as e:
            print(json.dumps({"event": "retry", "url": url, "message": str(e)[:120]}),
                  flush=True)
        time.sleep(2)
    return False


def crawl_single_page(page, tab, trail_root):
    """A tab that is ONE page: some prose and usually attached PDFs.

    SIFI, Forms and CPE. THE PAGE ITSELF IS THE DOCUMENT — one row, identified
    by the tab's own url, carrying the page's full text/html. Forms in
    particular is nothing but prose plus a pile of attachments (10 of them);
    those attachments are not separate documents in their own right, they are
    what the one page links to, so they go in extra_meta["attachment_links"]
    (pipe-joined) on that single row rather than exploding into 10 rows whose
    title is just whatever text the link happened to use ("link", "Click
    Here" ...) instead of the actual page title.
    """
    records, documents = [], {}
    if not load(page, tab["url"]):
        print(json.dumps({"event": "error", "url": tab["url"],
                          "message": "tab page did not load"}), flush=True)
        return records, documents

    d = page.evaluate(JS_PAGE_CONTENT)
    trail = trail_root                      # the tab IS the leaf here
    records.append({
        "section_path": " > ".join(trail),
        "title": d["title"] or tab["label"],
        "url": tab["url"],
        "depth": len(trail) - 1,
        "linked_from_title": tab["label"],
        "parent_page_url": "",
        "status": "",
        "n_pdfs": len(d["docs"]),
        "pdf_links": " | ".join(x["href"] for x in d["docs"]),
        "text_len": len(d["text"]),
        "html_file": "",
        "text": d["text"],
        "html": d["html"],
        "breadcrumb": trail,
        "row_text": "",
        "page_title": d["title"],
        # Whatever the page states is a LAST MODIFIED date, not an issue date, so
        # it must never become published_date — that feeds document identity.
        # Site stamp, not a document date — see PAGE_STAMP_FIELD.
        PAGE_STAMP_FIELD: d["last_modified"],
    })
    title = (d["title"] or tab["label"]).strip()
    doc_url, attachment_links = _one_or_many(
        [x["href"] for x in d["docs"]], fallback_url=tab["url"])
    documents[(doc_url or tab["url"], " > ".join(trail))] = {
        "title": title, "doc_url": doc_url, "type": tab["label"],
        "found_on": tab["url"], "section_path": " > ".join(trail),
        "content_text": d["text"], "content_html": d["html"],
        "attachment_links": attachment_links,
    }
    print(json.dumps({"event": "single_page", "tab": tab["label"],
                      "chars": len(d["text"]), "pdfs": len(d["docs"]),
                      "last_modified": d["last_modified"]}, ensure_ascii=False),
          flush=True)
    return records, list(documents.values())


# Where a long run writes its partial results. Announcements is ~4 hours of
# detail pages; without this, a crash at hour three leaves nothing on disk — the
# exact failure this project already has on SBP. main() fills this in.
CHECKPOINT = {"path": None, "meta": {}, "every": 100}

# Pace, and what we already have.
#
# CMA throttles progressively: the last Announcements run went from 3.3s to 21s
# per record and 159 to 853 retries, then the list walk itself stalled at 1,050
# of 3,297. Hammering it harder makes it slower — a small deliberate gap between
# requests finishes sooner than fighting the throttle, and RESUME means a run
# that dies at hour three costs an hour, not three.
PACE = {"detail_ms": 0, "page_ms": 0}
PRIOR = {}          # url -> record kept from a previous run


ALL_PRIOR = {}      # url -> every record from the previous run, for merging


def load_prior(path):
    """Records from a previous run: the reusable detail pages, and all of them.

    Both matter. The detail pages save requests; the full set stops a bad run
    from destroying a good one.
    """
    try:
        d = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    ALL_PRIOR.clear()
    ALL_PRIOR.update({r["url"]: r for r in d.get("pages", []) if r.get("url")})
    ALL_PRIOR["__documents__"] = d.get("documents", [])
    keep = {r["url"]: r for r in d.get("pages", [])
            if r.get("body_source") == "detail_page" and r.get("text_len")}
    if keep:
        print(json.dumps({"event": "resume", "reusable_records": len(keep),
                          "prior_records": len(ALL_PRIOR) - 1,
                          "from": str(path)}), flush=True)
    return keep


def merge_prior(recs, docs):
    """Union this run's results with the previous run's. NEVER shrink.

    The list walk on a throttled tab is not deterministic — Announcements
    returned 1,050 rows on one run and 240 on the next, from identical code.
    Writing whichever the latest run happened to get DELETED 810 good records
    that had cost three hours. A record only improves: a real detail page beats
    a card summary, and anything beats nothing.
    """
    if not ALL_PRIOR:
        return recs, docs
    rank = {"detail_page": 2, "card_summary": 1, "": 0, None: 0}
    out = {r["url"]: r for r in ALL_PRIOR.values() if isinstance(r, dict)}
    for r in recs:
        old = out.get(r["url"])
        if old is None or rank.get(r.get("body_source")) >= rank.get(old.get("body_source")):
            out[r["url"]] = r
    seen, merged_docs = set(), []
    for x in list(ALL_PRIOR.get("__documents__", [])) + list(docs):
        k = (x.get("doc_url"), x.get("section_path"))
        if k in seen:
            continue
        seen.add(k); merged_docs.append(x)
    added = len(out) - (len(ALL_PRIOR) - 1)
    print(json.dumps({"event": "merged", "prior": len(ALL_PRIOR) - 1,
                      "this_run": len(recs), "total": len(out),
                      "new_this_run": max(added, 0),
                      "documents": len(merged_docs)}), flush=True)
    return list(out.values()), merged_docs


def checkpoint(records, documents):
    """Write what we have so far. Best effort — never kill a run over it."""
    p = CHECKPOINT.get("path")
    if not p:
        return
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".partial")
        tmp.write_text(json.dumps(
            {**CHECKPOINT["meta"], "partial": True,
             "pages": records, "documents": list(documents)},
            ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"event": "checkpoint", "records": len(records),
                          "documents": len(documents), "file": str(tmp)}), flush=True)
    except Exception as e:
        print(json.dumps({"event": "warning",
                          "message": f"checkpoint failed: {str(e)[:90]}"}), flush=True)


def mk_record(trail, title, url, *, text="", html="", files=(), parent="",
              row_text="", page_title="", **extra):
    """One page record in the schema generic_crawler and formfill both emit."""
    r = {
        "section_path": " > ".join(trail),
        "title": title,
        "url": url,
        "depth": len(trail) - 1,
        "linked_from_title": title,
        "parent_page_url": parent,
        "status": "",
        "n_pdfs": len(files),
        "pdf_links": " | ".join(f["href"] for f in files),
        "text_len": len(text),
        "html_file": "",
        "text": text,
        "html": html,
        "breadcrumb": list(trail),
        "row_text": row_text,
        "page_title": page_title or title,
    }
    r.update({k: v for k, v in extra.items() if v not in ("", None)})
    return r


def page_span(cards) -> str:
    """'1..3 (36 rows)' — what data-page actually covered, for the done event."""
    pages = sorted({int(c["data"].get("page", 0) or 0) for c in cards} - {0})
    if not pages:
        return f"(no data-page; {len(cards)} rows)"
    gaps = [p for p in range(pages[0], pages[-1] + 1) if p not in pages]
    return (f"{pages[0]}..{pages[-1]} ({len(cards)} rows"
            + (f", MISSING {gaps}" if gaps else "") + ")")


def check_total(page, cards, tab_label):
    """The pager states its own page count. Cross-check it.

    These tabs paginate by toggling a `hideRowItem` class, so every row is in
    the DOM from the start and the stated total is a free completeness proof:
    if the highest data-page we saw is below the stated total, the list was
    still rendering and the run is short. Reported, never silently ignored.
    """
    stated = page.evaluate(JS_PAGER_TOTAL)
    seen = max([int(c["data"].get("page", 0) or 0) for c in cards] or [0])
    if stated and seen and seen != stated:
        report_gap(tab=tab_label, stated_pages=stated, pages_seen=seen,
                   rows=len(cards), why="list may still have been rendering")
    src = page.evaluate(JS_SOURCE_COUNT)
    if src and src != len(cards):
        report_gap(tab=tab_label, source_list_count=src, rows=len(cards),
                   why="SharePoint list holds more rows than the grid drew")
    elif src:
        print(json.dumps({"event": "source_count_ok", "tab": tab_label,
                          "count": src}), flush=True)
    return stated


def crawl_cards(page, tab, trail_root, grouped=False):
    """A flat grid of cards, each a title plus a file. Guides and Circulars.

    `grouped` puts the card under its own category folder, taken from the
    parent's data-categoryname. Circulars has a category dropdown, but the
    dropdown only FILTERS what is already tagged in the DOM, so selecting each
    option would be a slower way of reading an attribute we can read directly.
    """
    records, documents = [], {}
    if not load(page, tab["url"], wait_ms=3000):
        print(json.dumps({"event": "error", "url": tab["url"],
                          "message": "tab page did not load"}), flush=True)
        return records, []

    cards = page.evaluate(JS_CARDS)
    if not cards:
        # Loud, because "no cards" and "cards did not render" look identical.
        raise SystemExit(f"{tab['label']}: 0 cards found — the page rendered but "
                         f"div.card-wrapper matched nothing. Refusing to report "
                         f"an empty tab as success.")
    stated = check_total(page, cards, tab["label"])

    seen_cat, no_cat = set(), 0
    for c in cards:
        cat = (c["data"].get("categoryname") or c["data"].get("category") or "").strip()
        if grouped and cat:
            trail = trail_root + [cat]
            seen_cat.add(cat)
        else:
            trail = trail_root
            if grouped:
                no_cat += 1
        title = re.sub(r"\s*(Details|More|Download)\s*$", "", c["title"]).strip()
        records.append(mk_record(
            trail, title, c["files"][0]["href"] if c["files"] else tab["url"],
            text=c["text"], files=c["files"], parent=tab["url"],
            row_text=c["text"], cma_id=c["data"].get("id", "")))
        for f in c["files"]:
            documents[(f["href"], " > ".join(trail))] = {
                "title": title, "doc_url": f["href"], "type": "PDF",
                "found_on": tab["url"], "section_path": " > ".join(trail)}

    if grouped:
        # The dropdown is the site's own list of categories. If it offers one we
        # never saw on a card, either a category is empty or our read is short.
        opts = {o.strip() for o in page.evaluate(
            "()=>Array.from(document.querySelectorAll('select option'))"
            ".map(o=>o.text.trim())") if o.strip() and not o.lower().startswith("select")}
        print(json.dumps({"event": "categories", "tab": tab["label"],
                          "from_cards": sorted(seen_cat),
                          "from_dropdown": sorted(opts),
                          "only_in_dropdown": sorted(opts - seen_cat),
                          "uncategorised_cards": no_cat}, ensure_ascii=False), flush=True)

    print(json.dumps({"event": "cards", "tab": tab["label"], "cards": len(cards),
                      "documents": len(documents), "stated_pages": stated,
                      "pages": page_span(cards)}, ensure_ascii=False), flush=True)
    return records, list(documents.values())


def crawl_regs(page, tab, trail_root, limit=None):
    """Implementing Regulations: 36 cards, each with its own Details page.

    The six sub-tabs (All / Glossary / Guidelines / Instructions & Procedures /
    Regulations / Rules) ALL target the same pane, `#all-tab-pane`, and filter it
    client-side — so every card is present at once and its sub-tab is readable
    from data-category. No tab clicking, and no chance of a missed click quietly
    dropping a category.

    The card gives title, date and the PDF; the Details page adds the publishing
    date and the last-modified date, which is why each one is still opened.
    """
    records, documents = [], {}
    if not load(page, tab["url"], wait_ms=3500):
        print(json.dumps({"event": "error", "url": tab["url"],
                          "message": "tab page did not load"}), flush=True)
        return records, []

    cards = page.evaluate(JS_CARDS)
    if not cards:
        raise SystemExit(f"{tab['label']}: 0 cards found.")
    stated = check_total(page, cards, tab["label"])

    # The sub-tab strip is the site's own category list; compare it with what the
    # cards claim. A category present as a tab but on no card means either an
    # empty category or a short read — worth seeing either way.
    subtabs = [t for t in page.evaluate(
        "()=>Array.from(document.querySelectorAll('#SearchResultsTab a,#SearchResultsTab button'))"
        ".map(a=>(a.innerText||'').replace(/\\s+/g,' ').trim())") if t and t.lower() != "all"]
    from_cards = sorted({(c["data"].get("category") or "").strip() for c in cards} - {""})
    print(json.dumps({"event": "subtabs", "tab": tab["label"],
                      "from_tabs": subtabs, "from_cards": from_cards,
                      "only_in_tabs": sorted(set(subtabs) - set(from_cards)),
                      "only_on_cards": sorted(set(from_cards) - set(subtabs)),
                      "cards": len(cards), "stated_pages": stated,
                      "pages": page_span(cards)}, ensure_ascii=False), flush=True)

    uncat = [c for c in cards if not (c["data"].get("category") or "").strip()]
    if uncat:
        print(json.dumps({"event": "uncategorised", "tab": tab["label"],
                          "n": len(uncat),
                          "titles": [c["data"].get("title") or c["title"][:50]
                                     for c in uncat[:5]],
                          "why": "no data-category; filed directly under the tab"},
                         ensure_ascii=False), flush=True)

    if limit:
        cards = cards[:limit]
    n_pub = n_lm = n_detail = n_fallback = 0
    for i, c in enumerate(cards, 1):
        cat = (c["data"].get("category") or "").strip()
        trail = trail_root + ([cat] if cat else [])
        title = c["data"].get("title") or c["title"]
        files = list(c["files"])
        text = html = published = last_mod = ""
        # mk_record(body_source=...) is passed further down but was never
        # assigned here, so the whole Implementing Regulations tab died with
        # `NameError: name 'body_source' is not defined` — 0 documents from a
        # tab that otherwise works. crawl_paged() sets it before use; this
        # function did not.
        #
        # "" rather than "card": merge_prior ranks with
        # {"detail_page": 2, "card_summary": 1, "": 0, None: 0}, and an unknown
        # value makes rank.get() return None, which then raises on the >=
        # comparison. Nothing was fetched when the detail page does not load, so
        # "" — "anything beats nothing" — is also the honest value.
        body_source = ""
        url = c["detail"] or (files[0]["href"] if files else tab["url"])

        if c["detail"] and load(page, c["detail"], wait_ms=2500):
            d = page.evaluate(JS_REG_DETAIL)
            n_detail += 1
            body_source = "detail_page"
            text, html = d["text"], d["html"]
            published, last_mod = d["published"], d["last_modified"]
            # Same rule as the consultations: data-title is the site's own clean
            # title for this row, so the detail page only fills a gap.
            title = title or d["title"]
            for f in d["files"]:
                if f["href"] not in {x["href"] for x in files}:
                    files.append(f)
        if not published:
            # The card prints the same date ("02-March-2026"). Falling back to it
            # matters less for the two entries CMA itself leaves as "----" than
            # for the case this protects against: a Details page that failed to
            # load looks exactly like one with no date on it.
            cd = re.search(r"\b(\d{1,2}-[A-Za-z]{3,9}-\d{4})\b", c["text"])
            if cd:
                published = cd.group(1)
                n_fallback += 1
        n_pub += bool(published)
        n_lm += bool(last_mod)

        records.append(mk_record(
            trail + [title], title, url, text=text, html=html, files=files,
            parent=tab["url"], row_text=c["text"],
            # published_date is the ISSUE date and is part of document identity;
            # the last-modified stamp is a revision date and must not touch it,
            # or an amended regulation reads as a brand-new document each time.
            published_date=published, **{PAGE_STAMP_FIELD: last_mod},
            body_source=body_source, cma_id=c["data"].get("id", "")))
        for f in files:
            documents[(f["href"], " > ".join(trail))] = {
                "title": title, "doc_url": f["href"], "type": "PDF",
                "found_on": url, "section_path": " > ".join(trail),
                "published_date": published, PAGE_STAMP_FIELD: last_mod,
                # The Details page (opened above into `text`/`html`) is what
                # carries the regulation's actual body -- the card alone is
                # just a title and a date. Without this, every one of these
                # rows had a PDF link and NOTHING ELSE for the orchestrator to
                # analyse from, despite the detail page having already been
                # fetched and thrown away.
                "content_text": text, "content_html": html}
        if i % 10 == 0:
            print(json.dumps({"event": "progress", "tab": tab["label"],
                              "done": i, "of": len(cards)}), flush=True)

    print(json.dumps({"event": "fill_rates", "tab": tab["label"],
                      "cards": len(cards), "details_opened": n_detail,
                      "published_date": f"{n_pub}/{len(cards)}",
                      "from_card_fallback": n_fallback,
                      PAGE_STAMP_FIELD: f"{n_lm}/{len(cards)}"}), flush=True)
    return records, list(documents.values())


def crawl_consult(page, tab, trail_root, limit=None):
    """Public Consultation: Active / Expired tabs, each card opening a detail page.

    Crawled at CONSULT_INNER, not at the tab URL — see the note there.
    """
    records, documents = [], {}
    if not load(page, CONSULT_INNER, wait_ms=4000):
        print(json.dumps({"event": "error", "url": CONSULT_INNER,
                          "message": "consultation iframe page did not load"}), flush=True)
        return records, []

    tabs = page.evaluate(JS_CONSULT_TABS)
    if not tabs:
        raise SystemExit("Public Consultation: no Active/Expired tab strip found.")
    print(json.dumps({"event": "consult_tabs",
                      "tabs": [t["label"] for t in tabs]}), flush=True)

    todo = []
    for t in tabs:
        # Click the tab before reading it. Both panes are in the DOM from the
        # start so this changes nothing for a populated one — but for an empty
        # pane it is the difference between "this tab has no consultations" and
        # "this tab never rendered", which otherwise both read as zero.
        clicked = False
        try:
            page.click(f'[data-bs-target="{t["pane"]}"]', timeout=8000)
            page.wait_for_timeout(1500)
            clicked = True
        except Exception:
            pass
        rows = page.evaluate(JS_PANE_CARDS, t["pane"])
        if rows is None:
            print(json.dumps({"event": "error", "pane": t["pane"],
                              "message": "pane not found"}), flush=True)
            continue
        print(json.dumps({"event": "consult_pane", "label": t["label"],
                          "pane": t["pane"], "cards": len(rows),
                          "tab_clicked": clicked},
                         ensure_ascii=False), flush=True)
        if not rows:
            print(json.dumps({"event": "warning", "tab": tab["label"],
                              "message": f"{t['label']}: pane is present but holds "
                                         f"0 cards — treated as genuinely empty"},
                             ensure_ascii=False), flush=True)
        for r in rows:
            todo.append((t["label"], r))
    if limit:
        todo = todo[:limit]

    n_detail = n_date = 0
    for i, (label, r) in enumerate(todo, 1):
        trail = trail_root + [label]
        # "Expire in 2026/06/17" — a deadline, not a publication date.
        m = re.search(r"(\d{4}/\d{1,2}/\d{1,2}|\d{1,2}/\d{1,2}/\d{4})", r["date_text"])
        expiry = m.group(1) if m else ""
        n_date += bool(expiry)
        title, text, html, last_mod = r["title"], "", "", ""
        files = list(r["files"])
        url = r["detail"] or CONSULT_INNER
        detail_title = ""
        if r["detail"] and load(page, r["detail"], wait_ms=2500):
            d = page.evaluate(JS_PAGE_CONTENT)
            n_detail += 1
            text, html, last_mod = d["text"], d["html"], d["last_modified"]
            detail_title = d["title"]
            # KEEP THE CARD'S TITLE. Several consultations share one announcement
            # page — CMA_N_2739 announces both the Investment Funds and the Real
            # Estate amendments — so taking the detail page's <h1> gives two
            # distinct consultations the same name. The orchestrator dedupes on
            # title, so that quietly merges them into one document. The card
            # title is the consultation; the page <h1> is the announcement, and
            # it travels as page_title instead.
            title = title or detail_title
            for f in d["docs"]:
                if f["href"] not in {x["href"] for x in files}:
                    files.append({"href": f["href"], "text": f["title"], "title": ""})
        records.append(mk_record(
            trail + [title], title, url, text=text, html=html, files=files,
            parent=CONSULT_INNER, row_text=r["title"], page_title=detail_title,
            expiry_date=expiry, **{PAGE_STAMP_FIELD: last_mod}))
        # One document per CONSULTATION, not per file. Keyed on trail+[title]
        # (not just trail): two consultations under the same Active/Expired
        # label can share a file -- CMA_N_2739 announces both the Investment
        # Funds and the Real Estate amendments -- and keying on the shared
        # href with no title in the path let the second overwrite the first
        # in this dict outright, which is how a real consultation went
        # missing from the workbook entirely rather than merely losing an
        # attachment. Multi-attachment convention: models.RegulatoryDocument.
        full_trail = trail + [title]
        doc_url, attachment_links = _one_or_many(
            [f["href"] for f in files], fallback_url=url)
        documents[(doc_url or url, " > ".join(full_trail))] = {
            "title": title, "doc_url": doc_url,
            "type": "PDF" if doc_url or attachment_links else "Consultation",
            "found_on": url, "section_path": " > ".join(full_trail),
            "expiry_date": expiry, "content_text": text, "content_html": html,
            "attachment_links": attachment_links}
        if i % 10 == 0:
            print(json.dumps({"event": "progress", "tab": tab["label"],
                              "done": i, "of": len(todo)}), flush=True)

    print(json.dumps({"event": "fill_rates", "tab": tab["label"],
                      "cards": len(todo), "details_opened": n_detail,
                      "expiry_date": f"{n_date}/{len(todo)}"}), flush=True)
    return records, list(documents.values())


def crawl_faqs(page, tab, trail_root, limit=None):
    """FAQs: 418 question/answer pairs across 35 CSS-paginated pages.

    The answers sit in the DOM whether or not the accordion is open, and the
    pager hides rows with a class rather than fetching, so the whole set is one
    evaluate() away — no 418 clicks and no pagination walk.

    Each question is its own record: they are independent statements of the
    regulator's position, and a single 418-question blob would be one document
    for requirement matching to chew on.
    """
    records = []
    if not load(page, tab["url"], wait_ms=4000):
        print(json.dumps({"event": "error", "url": tab["url"],
                          "message": "FAQ page did not load"}), flush=True)
        return records, []

    items = page.evaluate(JS_FAQ_ITEMS)
    if not items:
        raise SystemExit("FAQs: 0 accordion items found.")
    stated = page.evaluate(JS_PAGER_TOTAL)
    pages = sorted({int(x["page"]) for x in items if x["page"].isdigit()})
    gaps = [p for p in range(1, (stated or (pages[-1] if pages else 0)) + 1)
            if p not in pages]
    print(json.dumps({"event": "faq_items", "items": len(items),
                      "stated_pages": stated, "pages_seen": len(pages),
                      "missing_pages": gaps}), flush=True)
    if gaps:
        report_gap(tab=tab["label"], missing_pages=gaps,
                   why=f"{len(gaps)} page(s) of the list were not read")

    # NO PER-REGULATION FOLDERS, and this is deliberate.
    #
    # The page has a left-hand filter with one checkbox per regulation, which
    # would have given each question a proper folder. It does not work: the
    # checkboxes carry no onclick, no onchange and tabindex="-1", there is no
    # Apply or Search button, and ticking one changes NOTHING in the DOM —
    # measured 2026-08-06, still 418 items, same classes, same ids, same pager.
    #
    # An earlier version tried to infer the grouping from what stayed visible
    # after each tick. That read the PAGINATION hide class, not a filter, so it
    # "labelled" 12 questions and produced 288 contradictions — 30 different
    # regulation names competing for the same 12 page-one slots. Wrong folder
    # names are worse than no folder names, so the questions stay flat under the
    # tab until the site's filter actually does something.
    inert = check_faq_filter(page, len(items))
    print(json.dumps({"event": "faq_grouping", "grouped": False,
                      "filter_is_inert": inert,
                      "why": "filter checkboxes do not alter the DOM"}), flush=True)

    if limit:
        items = items[:limit]
    documents = {}
    for x in items:
        url = f"{tab['url']}#flush-collapse{x['id']}"
        section_path = " > ".join(trail_root + [x["q"]])
        records.append(mk_record(
            trail_root + [x["q"]], x["q"], url,
            text=x["a"], html=x["html"], parent=tab["url"], row_text=x["q"],
            cma_id=x["id"]))
        # One document per Q&A pair — each is an independent statement of the
        # regulator's position (see the docstring above), so it gets its own
        # row rather than being folded into a 418-question blob.
        documents[(url, section_path)] = {
            "title": x["q"], "doc_url": url, "type": "FAQ",
            "found_on": tab["url"], "section_path": section_path,
            "content_text": x["a"], "content_html": x["html"],
        }
    return records, list(documents.values())


def check_faq_filter(page, n_items) -> bool:
    """Tick one filter and confirm it still does nothing.

    Kept as a live check rather than a comment: the day CMA wires these
    checkboxes up is the day this tab should gain its per-regulation folders,
    and this is what will say so instead of us finding out by accident.
    """
    try:
        boxes = [b for b in page.evaluate(JS_FAQ_BOXES) if b["id"] != "checkboxAll"]
        if not boxes:
            return True
        before = page.evaluate(JS_FAQ_SHOWN)
        page.check(f"#{boxes[0]['id']}", timeout=6000)
        page.wait_for_timeout(1500)
        after = page.evaluate(JS_FAQ_SHOWN)
        total_after = page.evaluate("()=>document.querySelectorAll('.accordion-item').length")
        page.uncheck(f"#{boxes[0]['id']}", timeout=6000)
        if before == after and total_after == n_items:
            return True
        print(json.dumps({"event": "warning",
                          "message": "FAQ filter now CHANGES the DOM — "
                                     "per-regulation folders are available, "
                                     "revisit crawl_faqs()",
                          "filter": boxes[0]["label"][:60],
                          "items_before": n_items, "items_after": total_after,
                          "shown_before": len(before), "shown_after": len(after)},
                         ensure_ascii=False), flush=True)
        return False
    except Exception as e:
        print(json.dumps({"event": "warning",
                          "message": f"FAQ filter check failed: {str(e)[:100]}"}),
              flush=True)
        return True


# Only the cards currently on screen. On the Media Center and Capital Market
# lists the DOM holds a rolling window (30 nodes for 3,297 announcements) and
# clicking a page REWRITES those nodes in place, so "everything in the DOM" —
# which is how every Laws & Regulations tab works — is false here. Reading the
# hidden ones would duplicate the neighbouring pages.
JS_VISIBLE_CARDS = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  const abs = h => { try { return new URL(h, location.href).href } catch(e) { return '' } };
  return Array.from(document.querySelectorAll('div.card-wrapper'))
    .filter(c => !c.closest('table') && c.offsetParent)
    .map(c => {
      const data = {};
      const p = c.parentElement;
      if (p) for (const a of p.attributes)
        if (a.name.startsWith('data-')) data[a.name.slice(5)] = a.value;
      const txt = el => clean(el.innerText) || clean(el.textContent);
      const h = c.querySelector('h2,h3,h4,h5');
      const links = Array.from(c.querySelectorAll('a[href]')).map(a => ({
          text: clean(a.innerText) || clean(a.textContent),
          title: a.getAttribute('title') || '',
          href: abs(a.getAttribute('href') || '')}))
        .filter(l => l.href && !/^javascript:/i.test(l.href));
      return {
        data,
        title: data.title || (h ? txt(h) : ''),
        text: txt(c),
        files: links.filter(l => /\.(pdf|docx?|xlsx?|zip)(\?|$)/i.test(l.href)),
        detail: (links.find(l => /^(read more|more|details)$/i.test(l.text))
              || links.find(l => /details?\.aspx/i.test(l.href)) || {}).href || '',
        links,
      };
    });
}"""

# The pager renders as:
#   [prev] [1] [2] [3] [4] [5] [...] [next] [Total 550 Pages]
# so the next arrow is the SECOND-TO-LAST li, not the last. Both the numbered
# "2" and the next arrow carry class `page-item2`, so selecting by class clicks
# the wrong one — measured: it jumped to page 2 and stayed there.
#
# FIND THE ARROW BY WHAT IT IS, NOT BY WHERE IT SITS. "Second-to-last li" was
# right on a fresh load and wrong on the run that mattered: on 2026-09-18 every
# paged tab (Announcements, Prospectuses, Shareholder Circulars) stopped after
# ONE chunk with
#     Page.click: Timeout 10000ms exceeded ... locator resolved to
#     <a href="#" class="page-link">…</a>
# i.e. the second-to-last item was the ellipsis, not the arrow, and the click
# waited 10s for an element that was never going to accept it. Prospectuses
# returned 60 (81 with sub-tabs) of 266 and the run was still called trustworthy.
#
# The arrow is the LAST <li> that carries an icon. The previous arrow also has
# one but comes first in the DOM, the ellipsis and "Total N Pages" carry text
# only, and a list with a single icon-bearing item at index 0 has no next arrow.
_JS_NEXT_LI = r"""
  const p = document.querySelector('ul.pagination.pagination-container');
  const lis = p ? Array.from(p.children).filter(e => e.tagName === 'LI') : [];
  let idx = -1;
  lis.forEach((li, i) => { if (li.querySelector('svg, img')) idx = i; });
  const nxt = idx > 0 ? lis[idx] : null;
"""

JS_PAGER_STATE = r"""() => {
""" + _JS_NEXT_LI + r"""
  if (!p) return null;
  const m = (p.innerText||'').replace(/\s+/g,' ').match(/Total\s+(\d+)\s+[Pp]ages?/);
  return {active: ((p.querySelector('.active')||{}).innerText||'').trim(),
          total: m ? parseInt(m[1],10) : 0,
          nextDisabled: nxt ? /\bdisabled\b/.test(nxt.getAttribute('class')||'') : true};
}"""

# Clicks the arrow from inside the page, so it cannot time out on visibility or
# be intercepted by an overlay, and says what it found so the caller can retry.
JS_CLICK_NEXT = r"""() => {
  if (!document.querySelector('ul.pagination.pagination-container')) return 'no_pager';
""" + _JS_NEXT_LI + r"""
  if (!nxt) return 'no_next';
  if (/\bdisabled\b/.test(nxt.getAttribute('class')||'')) return 'disabled';
  const a = nxt.querySelector('a');
  if (!a) return 'no_anchor';
  a.click();
  return 'clicked';
}"""

# Kept for callers outside walk_pages; do not use it to page.
NEXT_SEL = "ul.pagination.pagination-container li:nth-last-child(2) > a"

# THE CURSOR. This is what makes the big lists tractable.
#
# The visible pager is a lie about how this site pages. Reading the site's own
# pager.js: `.page-N` classes cover only the ~30 rows already loaded, and asking
# for a page beyond them falls through to `$('.nxtbtn').click()` — a SharePoint
# web-part POSTBACK. Clicking that 550 times in one session is what killed the
# first Announcements run at page 55, and almost certainly what failed 68 of its
# detail loads too: the session accumulates state until the server stops
# answering.
#
# But the button's onclick carries a plain, stateless GET:
#
#   default.aspx?Paged=TRUE&p_SortBehavior=0&p_ArticleStartDate=…
#               &p_ID=3349&PageFirstRow=31&View={9980B6A2-…}
#
# So the list is really cursor-paged, 30 rows at a time. Following the cursor
# turns 550 stateful clicks into 110 independent page loads — each a fresh
# session that cannot degrade, and each cursor a string we could persist to
# resume. PageFirstRow on its own does NOT work; the whole token is required.
JS_NEXT_URL = r"""(scopeSel) => {
  const root = scopeSel ? document.querySelector(scopeSel) : document;
  if (!root) return '';
  const b = root.querySelector('.nxtbtn');
  if (!b) return '';
  const m = (b.getAttribute('onclick') || '')
              .match(/RefreshPageTo\(\s*event\s*,\s*["']([^"']+)["']/i);
  if (!m) return '';
  try { return new URL(m[1].replace(/&amp;/g, '&'), location.href).href; }
  catch (e) { return ''; }
}"""

# One entry per pageable web part: its rows, its columns, and its own cursor.
#
# Iterating WEB PARTS rather than tabs is what keeps the multi-tab registers
# honest — Special Purpose Entities has two, each with its own View GUID and its
# own next button, and pairing the wrong table with the wrong cursor would page
# one tab while reading the other.
JS_REGISTER = r"""() => {
  const clean = s => (s||'').replace(/\s+/g,' ').trim();
  const abs = h => { try { return new URL(h, location.href).href } catch(e) { return '' } };
  const partOf = el => el.closest('.tab-pane, [id^=MSOZoneCell_WebPart], [id^=WebPartWPQ]')
                    || document.body;

  const conts = new Set();
  for (const t of document.querySelectorAll('table.table-striped')) conts.add(partOf(t));
  for (const c of document.querySelectorAll('div.card-wrapper'))
    if (!c.closest('table')) conts.add(partOf(c));

  // These tables carry their column name inside every cell, for the responsive
  // layout — and in the ARABIC of the site's other language:
  //   "اسم المحاسبBader Hatim Yousef Altamimi"   -> "Bader Hatim Yousef Altamimi"
  //   "رقم الترخيص489"                            -> "489"
  // Only strip when something non-Arabic follows, so a genuinely Arabic value is
  // left alone rather than emptied.
  const stripLabel = (t, colName) => {
    t = clean(t);
    const c = clean(colName || '');
    if (c && t.length > c.length && t.slice(0, c.length) === c) t = t.slice(c.length).trim();
    const m = t.match(/^[؀-ۿ،؛؟\s:]+/);
    if (m) { const rest = t.slice(m[0].length).trim(); if (rest) t = rest; }
    return t;
  };

  const readTable = (tb) => {
    // ":scope >" everywhere: these tables NEST (each accounting office row holds
    // a table of its accountants), and a plain querySelectorAll('th') pulls the
    // child table's headers up into the parent's column list.
    const cols = Array.from(tb.querySelectorAll(':scope > thead > tr > th'))
                      .map(h => clean(h.innerText));
    const rows = [];
    for (const tr of tb.querySelectorAll(':scope > tbody > tr')) {
      const tds = Array.from(tr.querySelectorAll(':scope > td'));
      if (!tds.length) continue;
      const links = [];
      for (const td of tds)
        for (const a of td.querySelectorAll(':scope a[href]')) {
          const h = a.getAttribute('href') || '';
          if (!h || /^javascript:/i.test(h)) continue;
          links.push({text: clean(a.innerText) || a.getAttribute('title') || '',
                      href: abs(h)});
        }
      const nested = [];
      for (const nt of tr.querySelectorAll('table')) {
        const ncols = Array.from(nt.querySelectorAll('th')).map(h => clean(h.innerText));
        const nrows = Array.from(nt.querySelectorAll('tbody tr'))
          .map(r => Array.from(r.querySelectorAll('td'))
                      .map((c, i) => stripLabel(c.innerText, ncols[i])))
          .filter(r => r.length && r.some(Boolean));
        if (nrows.length) nested.push({cols: ncols, rows: nrows});
      }

      // Responsive cells repeat their column name — "Entity Name Sukuk Morabha
      // 139-0202". Left in, the label lands in the entity name and in every
      // document title built from it.
      const cells = tds.map((td, i) => stripLabel(td.innerText, cols[i]));

      // An EXPANDER row holds nothing but the nested table — it is the "show the
      // accountants" drawer under an office, not an office of its own. Counted
      // as a row it doubled Registered Accounting Offices and gave half of them
      // a key made of the whole accountant table's text.
      const ownText = clean(tr.innerText);
      const nestedText = nested.map(n =>
        n.cols.join(' ') + ' ' + n.rows.map(r => r.join(' ')).join(' ')).join(' ');
      const isExpander = nested.length > 0 &&
        (tds.length === 1 ||
         clean(ownText).replace(/\s/g,'').length <=
           clean(nestedText).replace(/\s/g,'').length + 8);
      if (isExpander) {
        // Attach to the office it belongs to rather than dropping it.
        if (rows.length) {
          rows[rows.length - 1].nested =
            (rows[rows.length - 1].nested || []).concat(nested);
        }
        continue;
      }
      rows.push({cells, links, nested});
    }
    return {cols, rows};
  };

  const out = [];
  for (const cont of conts) {
    // Look for the next button in the container AND in the enclosing pane.
    // On Investment Funds the tables sit inside an accordion inside a web part,
    // so closest() stops at the web part while the paging cell
    // (td#bottomPagingCellWPQ6) hangs off the pane — the cursor came back empty
    // and Private Funds stopped after one chunk.
    let next = '';
    const paneUp = cont.closest('.tab-pane');
    const b = cont.querySelector('.nxtbtn') ||
              (paneUp ? paneUp.querySelector('.nxtbtn') : null);
    if (b) {
      const m = (b.getAttribute('onclick')||'')
                  .match(/RefreshPageTo\(\s*event\s*,\s*["']([^"']+)["']/i);
      if (m) { try { next = new URL(m[1].replace(/&amp;/g,'&'), location.href).href; }
               catch(e) {} }
    }
    // The pane's own tab label is the folder name.
    //
    // Look for the enclosing .tab-pane, not the container itself: a table sits
    // inside a web part inside a pane, and closest() returns the WEB PART, whose
    // id no tab points at. Reading the container's own id gave every pane the
    // same empty label, so Real Estate Contributions filed its Public and
    // Private Offering tables under one identical name.
    // A tab may point at its pane through ANY of data-bs-target, href or
    // aria-controls — on Real Estate Contributions data-bs-target is empty, so
    // matching only on that gave both panes the same name and filed the Public
    // and Private Offering tables into one folder.
    let label = '';
    const pane = cont.closest('.tab-pane') ||
                 (cont.classList.contains('tab-pane') ? cont : null);
    if (pane && pane.id) {
      const t = document.querySelector(
        '[data-bs-target="#' + pane.id + '"], a[href="#' + pane.id + '"], ' +
        '[aria-controls="' + pane.id + '"]');
      if (t) label = clean(t.innerText);
    }
    if (!label && pane) {
      // Last resort: pane order against tab order.
      const panes = Array.from(document.querySelectorAll('.tab-pane'));
      const tabs = Array.from(document.querySelectorAll(
        '[data-bs-toggle=tab],[data-bs-toggle=pill],[role=tab]'));
      const i = panes.indexOf(pane);
      if (i >= 0 && i < tabs.length) label = clean(tabs[i].innerText);
    }
    if (!label) {
      const h = cont.querySelector('h2,h3,h4');
      if (h) label = clean(h.innerText);
    }

    // EVERY table in the container, not just the first.
    //
    // Investment Funds is GROUPED BY FUND MANAGER — one table per manager, which
    // is what `p_GroupCol1=Hasseef Investment Company` in its cursor means.
    // Reading only the first table returned 1 fund out of 382, and looked
    // entirely healthy doing it: right columns, real row, no error.
    let cols = [], rows = [];
    for (const tb of cont.querySelectorAll('table.table-striped')) {
      const r = readTable(tb);
      if (!cols.length) cols = r.cols;
      // The group name. On Investment Funds each fund manager is an accordion
      // item whose button carries the manager's name, and the table sits inside
      // the collapse — so there is no heading as a previous sibling to find.
      let group = '';
      const item = tb.closest('.accordion-item');
      if (item) {
        const btn = item.querySelector('.accordion-button, [data-bs-toggle=collapse]');
        if (btn) group = clean(btn.innerText).slice(0, 90);
      }
      for (let e = tb.previousElementSibling; e && !group; e = e.previousElementSibling) {
        const t = clean(e.innerText);
        if (t && t.length < 90 && !/^loading/i.test(t)) group = t;
      }
      if (!group) {
        const par = tb.closest('.group, .accordion-item, .fund-group');
        if (par) {
          const h = par.querySelector('h2,h3,h4,h5,.group-title,button');
          if (h) group = clean(h.innerText).slice(0, 90);
        }
      }
      for (const row of r.rows) {
        // "Loading..." is a placeholder the page draws before its AJAX lands.
        if (row.cells.length && /^loading\.{0,3}$/i.test(clean(row.cells.join(' '))))
          continue;
        row.group = group;
        rows.push(row);
      }
    }

    // Card-shaped registers (Financial Market Institutions) have no table.
    const cards = Array.from(cont.querySelectorAll('div.card-wrapper'))
      .filter(c => !c.closest('table'))
      .map(c => {
        const d = {};
        const p = c.parentElement;
        if (p) for (const a of p.attributes)
          if (a.name.startsWith('data-')) d[a.name.slice(5)] = a.value;
        return {data: d, text: clean(c.innerText),
                links: Array.from(c.querySelectorAll('a[href]'))
                  .map(a => ({text: clean(a.innerText) || a.getAttribute('title') || '',
                              href: abs(a.getAttribute('href')||'')}))
                  .filter(l => l.href && !/^javascript:/i.test(l.href))};
      });

    if (!rows.length && !cards.length) continue;
    // paneId identifies the WEB PART, not the pane: it is what re-locates this
    // table after a cursor reload, and two web parts can share one pane.
    out.push({label, next, cols, rows, cards,
              paneId: cont.id || (pane && pane.id) || '', hasNext: !!next});
  }
  return out;
}"""


def list_subtabs(page):
    """The list's own category tabs, with the item count each one claims."""
    return page.evaluate(
        "()=>Array.from(document.querySelectorAll('[data-bs-toggle=tab],[data-bs-toggle=pill]'))"
        ".map(a=>({label:(a.innerText||'').replace(/\\s+/g,' ').trim(),"
        " target:a.getAttribute('data-bs-target')||''})).filter(x=>x.label)")


# A fingerprint of what the list is currently showing. Comparing this before
# and after a click is how the walk knows the page actually turned.
JS_PAGE_SIG = r"""() => {
  const a = Array.from(document.querySelectorAll('div.card-wrapper'))
    .filter(c => !c.closest('table') && c.offsetParent);
  const ids = a.map(c => (c.parentElement && c.parentElement.getAttribute('data-id')) ||
                         (c.innerText||'').slice(0, 40));
  const p = document.querySelector('ul.pagination.pagination-container');
  const act = p ? ((p.querySelector('.active')||{}).innerText||'').trim() : '';
  return act + '|' + ids.join('~');
}"""


def wait_for_cards(page, tries=10, gap_ms=900):
    """Wait until the card grid has actually drawn and stopped growing."""
    last = -1
    for _ in range(tries):
        n = page.evaluate(
            "()=>Array.from(document.querySelectorAll('div.card-wrapper'))"
            ".filter(c=>!c.closest('table')).length")
        if n and n == last:
            return n
        last = n
        page.wait_for_timeout(gap_ms)
    return last


def walk_pages(page, cap=0, read=JS_CARDS, cutoff_date=None):
    """Collect a whole list by following the .nxtbtn CURSOR, chunk by chunk.

    Each load brings 30-60 rows and a fresh next-URL. This replaced clicking the
    visible pager, which was never reliable: that pager only reveals the rows
    already in the DOM, and going past them depends on a SharePoint postback
    landing within the wait. When it did not, the walk saw the same rows twice
    and stopped — Prospectuses silently returned 60 of 266 rows, five pages, no
    error. The same failure killed Announcements at page 55 of 550.

    Following the cursor makes every chunk an independent page load: nothing
    accumulates, nothing races, and a missed hop cannot masquerade as the end of
    the list. Falls back to the pager click for lists with no .nxtbtn.

    NOTE 2026-09-21: the body below CLICKS the pager; it does not follow the
    cursor (JS_NEXT_URL is defined and unused). Measured on Prospectuses: the
    cursor url loads the same 60 rows again and offers the same next url, so it
    does not advance there, while the click reaches all 266 rows in 23 chunks.
    What made the click fail on 2026-09-18 was finding the arrow by position;
    it is now found by its icon and clicked from inside the page, with retries.

    `cutoff_date`, when given, stops the walk once the list has clearly moved
    past it — the list is newest-first, so nothing further out can be newer.

    A SINGLE chunk is not trusted as that evidence. The DOM window quirk
    described below means a chunk can bring as few as one or two genuinely
    fresh rows, sitting right at the edge of the window rather than at the
    front of the list — and one stray old date among two, mistaken for "this
    chunk's newest row", stopped a 365-day request at 6 weeks in on its first
    measurement (32 documents instead of several hundred). Fixed by requiring
    BOTH a real sample (>=5 dated rows) AND that sample missing the cutoff
    TWICE IN A ROW before believing it. A tab with no dates on its cards at
    all never accumulates that evidence and never stops early.
    """
    seen, rows, chunks, barren, stale_streak = set(), [], 0, 0, 0
    while True:
        chunks += 1
        fresh = 0
        fresh_dates = []
        for c in page.evaluate(read):
            key = c["data"].get("id") or c["title"] or c["text"][:80]
            if key in seen:
                continue
            seen.add(key); rows.append(c); fresh += 1
            d = _card_date(c.get("text", ""))
            if d:
                fresh_dates.append(d)
        if cutoff_date:
            if len(fresh_dates) >= 5 and max(fresh_dates) < cutoff_date:
                stale_streak += 1
            elif fresh_dates:
                # Any real sample with something newer than cutoff resets it —
                # a genuinely-past-cutoff list does not un-age.
                stale_streak = 0
            if stale_streak >= 2:
                print(json.dumps({"event": "walk_stop", "reason": "stale_streak",
                                  "chunks": chunks, "rows": len(rows)}), flush=True)
                break
        # BARREN CLICKS ARE NORMAL HERE, and getting this number wrong is what
        # capped Prospectuses at 60 rows.
        #
        # The DOM holds a window of 30-60 cards while the pager shows 6-12, so
        # four or five consecutive clicks move within the window and add nothing
        # new; only the click that leaves it triggers the fetch. A cutoff of two
        # or three stops the walk before it ever reaches that click, on the
        # first page, with no error. The real end of the list is the pager's
        # disabled next arrow — this counter is only a runaway guard, so it sits
        # well above the window size.
        barren = barren + 1 if not fresh else 0
        if (cap and chunks >= cap) or barren >= 10:
            print(json.dumps({"event": "walk_stop",
                              "reason": "cap" if (cap and chunks >= cap) else "barren",
                              "chunks": chunks, "rows": len(rows), "barren": barren}),
                  flush=True)
            break
        st = page.evaluate(JS_PAGER_STATE)
        if st and st["nextDisabled"]:
            print(json.dumps({"event": "walk_stop", "reason": "nextDisabled",
                              "chunks": chunks, "rows": len(rows), "pager_state": st}),
                  flush=True)
            break
        if PACE["page_ms"]:
            page.wait_for_timeout(PACE["page_ms"])
        before = page.evaluate(JS_PAGE_SIG)
        # Three attempts, re-reading the pager each time: the control can be
        # absent for a moment while the site redraws it, and giving up on the
        # first miss ended every paged tab after one chunk on 2026-09-18. Only a
        # control that STAYS missing is a reason to stop, and it is reported as
        # a fault below -- never as the end of the list.
        outcome = ""
        for attempt in range(3):
            try:
                outcome = page.evaluate(JS_CLICK_NEXT)
            except Exception as e:
                outcome = f"exception: {str(e)[:120]}"
            if outcome in ("clicked", "disabled"):
                break
            page.wait_for_timeout(1500)
        if outcome == "disabled":
            print(json.dumps({"event": "walk_stop", "reason": "nextDisabled",
                              "chunks": chunks, "rows": len(rows)}), flush=True)
            break
        if outcome != "clicked":
            print(json.dumps({"event": "walk_stop", "reason": "next_control_missing",
                              "chunks": chunks, "rows": len(rows),
                              "message": outcome}), flush=True)
            break
        # WAIT FOR THE CONTENT TO CHANGE, not for a fixed delay.
        #
        # Every fifth click leaves the DOM window and triggers a SharePoint
        # postback, which sometimes takes six seconds and sometimes one. With a
        # flat 1.4s wait the walk read the OLD rows, saw nothing new, and
        # concluded the list had ended — Prospectuses returned 60 of 266 rows on
        # one run and the full 266 on another, from identical code. Polling for
        # the change makes the walk depend on the site's response, not on a
        # guess about it.
        changed = False
        for _ in range(16):
            page.wait_for_timeout(500)
            if page.evaluate(JS_PAGE_SIG) != before:
                changed = True
                break
        if not changed:
            print(json.dumps({"event": "click_no_change", "chunks": chunks,
                              "why": "content did not change within 8s of the click "
                                     "-- next chunk read is likely the same page again"}),
                  flush=True)
    return rows, chunks


def subtab_categories(page, tab, all_rows):
    """Which sub-tab each row belongs to — Main Market vs Nomu, Tasi vs Nomoo.

    The sub-tabs are real filters here, so the category has to come from walking
    each one; there is no data-category on these cards. Prospectuses at least
    prints the market in the card text, but Shareholder Circulars does not
    mention Tasi or Nomoo anywhere on the card, so text matching would work for
    one tab and silently mislabel the other.

    Coverage still comes from the "All" walk, so a sub-tab that fails to filter
    costs a folder name and never a document.
    """
    subs = [s for s in list_subtabs(page) if s["label"].strip().lower() != "all"]
    if not subs:
        return {}, [], []
    key2cat, reports, extra = {}, [], []
    have = {(c["data"].get("id") or c["title"] or c["text"][:80]) for c in all_rows}
    for s in subs:
        # Use load(), not a bare goto: these panes need several seconds before
        # their cards exist. With a 2.5s wait the click landed on an empty pane
        # and every sub-tab reported zero rows — which read exactly like "the
        # tabs do not filter", when in fact Tasi filters 22 down to 8.
        try:
            if not load(page, tab["url"], wait_ms=4000):
                reports.append({"subtab": s["label"], "error": "reload failed"})
                continue
            # Click in the DOM by exact label. Playwright's :has-text locator
            # timed out on these tabs even though the same click works from JS,
            # and a timeout here is indistinguishable from "the tab does not
            # filter" — which is the wrong conclusion, Tasi filters 22 to 8.
            hit = page.evaluate(
                """(label)=>{
                  const els=[...document.querySelectorAll(
                    '[data-bs-toggle=tab],[data-bs-toggle=pill],[role=tab],.nav-link')];
                  const norm=e=>(e.innerText||'').replace(/\\s+/g,' ').trim();
                  const t=els.find(e=>norm(e)===label) || els.find(e=>norm(e).startsWith(label));
                  if(!t) return false; t.click(); return true;}""", s["label"])
            if not hit:
                reports.append({"subtab": s["label"], "error": "tab not found"})
                continue
            page.wait_for_timeout(1500)
            # Same trap as the initial load: the pane redraws asynchronously, so
            # reading straight after the click found 0 rows on Main Market one
            # run and 237 the next, from identical code.
            wait_for_cards(page)
        except Exception as e:
            reports.append({"subtab": s["label"], "error": str(e)[:70]})
            continue
        st = page.evaluate(JS_PAGER_STATE)
        rows, walked = walk_pages(page, 0, JS_CARDS)
        n_new = n_extra = 0
        for c in rows:
            k = c["data"].get("id") or c["title"] or c["text"][:80]
            if k not in key2cat:
                key2cat[k] = s["label"]; n_new += 1
            # A sub-tab can reach rows the "All" walk missed — measured on
            # Prospectuses, where Main Market + Nomu together found 254 unique
            # rows against All's 240. Coverage is the UNION, so these are kept
            # as data rather than thrown away after being used as a label.
            if k not in have:
                have.add(k); extra.append(c); n_extra += 1
        reports.append({"subtab": s["label"], "pages": walked,
                        "rows": len(rows), "labelled": n_new,
                        "not_in_all": n_extra})
    # A sub-tab that returns the whole list has not filtered.
    if len(set(key2cat.values())) == 1 and len(key2cat) >= len(all_rows):
        reports.append({"warning": "every sub-tab returned the same set — "
                                   "not filtering; falling back to a flat path"})
        return {}, reports, []
    return key2cat, reports, extra


def crawl_paged(page, tab, trail_root, max_pages=None, limit=None):
    """A card list that pages by REWRITING the DOM, six or twelve at a time.

    Announcements, Prospectuses and Shareholder Circulars. The difference from
    `cards` is not cosmetic: those tabs ship every row and hide the extras, so
    one read is the whole list. Here a read is one page, and the only way to the
    rest is to click. Announcements is 550 pages of six.

    Stops on whichever comes first: the stated page total, the next arrow going
    disabled, or two consecutive pages that add nothing new — that last one is
    the guard against a click that silently failed, which would otherwise re-read
    page 1 until max_pages and report a full-looking crawl of one page.
    """
    records, documents = [], {}
    if not load(page, tab["url"], wait_ms=4000):
        print(json.dumps({"event": "error", "url": tab["url"],
                          "message": "tab page did not load"}), flush=True)
        return records, []

    # load() only proves the chrome arrived. Without waiting for the grid the
    # first read found no cards and no pager at all — the walk reported zero
    # rows and "stated_pages: 0", which reads exactly like an empty list.
    wait_for_cards(page)
    st = page.evaluate(JS_PAGER_STATE)
    stated = st["total"] if st else 0
    src = page.evaluate(JS_SOURCE_COUNT)
    subs = list_subtabs(page)
    print(json.dumps({"event": "paged_start", "tab": tab["label"],
                      "stated_pages": stated, "source_list_count": src,
                      "subtabs": [s["label"] for s in subs],
                      "wants_detail": bool(tab.get("detail"))},
                     ensure_ascii=False), flush=True)

    # since_days: a deliberate, PARTIAL walk of a newest-first list — see
    # TABS["announcements"]. Not a completeness problem, so it must not trip
    # the coverage_gap check below the way a truncated crawl should.
    cutoff = (datetime.now() - timedelta(days=tab["since_days"])
              if tab.get("since_days") else None)

    # Follow the cursor. `max_pages` is now a CHUNK cap, and a chunk is 30-60
    # rows rather than the pager's 6-12, so the same number goes much further.
    rows, walked = walk_pages(page, max_pages or 0, JS_CARDS, cutoff_date=cutoff)

    if cutoff:
        # walk_pages() can only stop AFTER a chunk that is already past the
        # cutoff, so that chunk's own earlier rows are still in `rows`.
        # Trim them; an undated row is kept rather than risk losing content.
        before_n = len(rows)
        rows = [c for c in rows
                if (_card_date(c.get("text", "")) or cutoff) >= cutoff]
        print(json.dumps({"event": "since_window", "tab": tab["label"],
                          "since_days": tab["since_days"],
                          "cutoff": cutoff.date().isoformat(),
                          "kept": len(rows), "trimmed": before_n - len(rows)}),
              flush=True)

    print(json.dumps({"event": "paged_done", "tab": tab["label"],
                      "chunks_walked": walked, "stated_pages": stated,
                      "rows": len(rows), "source_list_count": src},
                     ensure_ascii=False), flush=True)
    if src and len(rows) != src and not cutoff:
        report_gap(tab=tab["label"], rows=len(rows), source_list_count=src,
                   why="row count differs from the SharePoint list")

    # Sub-tab folders. Opt-in per tab, because it means walking every sub-tab in
    # addition to "All": cheap for Prospectuses (23 more pages) and Shareholder
    # Circulars (2), but Announcements would add General's 245 pages for a folder
    # name, so that one stays flat with its totals logged instead.
    key2cat = {}
    if tab.get("subtab_paths"):
        key2cat, reports, extra = subtab_categories(page, tab, rows)
        if extra:
            rows.extend(extra)
        print(json.dumps({"event": "subtab_walk", "tab": tab["label"],
                          "reports": reports, "rows_labelled": len(key2cat),
                          "recovered_from_subtabs": len(extra),
                          "rows_total": len(rows)}, ensure_ascii=False), flush=True)
        unl = len(rows) - sum(
            1 for c in rows
            if (c["data"].get("id") or c["title"] or c["text"][:80]) in key2cat)
        if unl:
            print(json.dumps({"event": "warning", "tab": tab["label"],
                              "message": f"{unl} row(s) matched no sub-tab; "
                                         f"filed directly under the tab"}), flush=True)

    if limit:
        rows = rows[:limit]
    n_detail = n_date = 0
    for i, c in enumerate(rows, 1):
        # "05-August-2026" on the card. This is the announcement/issue date, and
        # the card is the only place several of these carry one.
        m = re.search(r"\b(\d{1,2}-[A-Za-z]{3,9}-\d{4})\b", c["text"])
        published = m.group(1) if m else ""
        n_date += bool(published)
        title = c["title"] or re.sub(
            CARD_DATE_LEAD, "", c["text"])[:300].strip()
        files, text, html, last_mod = list(c["files"]), "", "", ""
        url = c["detail"] or (files[0]["href"] if files else tab["url"])

        prior = PRIOR.get(c["detail"]) if c["detail"] else None
        if prior:
            # Already fetched in a previous run — do not spend a request on it.
            records.append(prior)
            pdf_links = [f for f in (prior.get("pdf_links") or "").split(" | ") if f]
            if tab.get("text_as_document") and prior.get("text"):
                # ONE row for the announcement, not one for the text plus one
                # per attachment -- see the live-fetch branch below for why.
                documents[(prior["url"], prior["section_path"])] = {
                    "title": prior["title"], "doc_url": prior["url"],
                    "type": tab["label"].rstrip("s"), "found_on": tab["url"],
                    "section_path": prior["section_path"],
                    "published_date": prior.get("published_date", ""),
                    "content_text": prior["text"], "content_html": prior.get("html", ""),
                    "reference_no": _reference_no_from_url(prior["url"]),
                    "attachment_links": " | ".join(pdf_links),
                }
            else:
                doc_url, attachment_links = _one_or_many(
                    pdf_links, fallback_url=prior["url"])
                if doc_url or attachment_links:
                    documents[(doc_url or prior["url"], prior["section_path"])] = {
                        "title": prior["title"], "doc_url": doc_url, "type": "PDF",
                        "found_on": prior["url"], "section_path": prior["section_path"],
                        "attachment_links": attachment_links}
            continue
        if tab.get("detail") and c["detail"] and PACE["detail_ms"]:
            page.wait_for_timeout(PACE["detail_ms"])
        if tab.get("detail") and c["detail"] and load(page, c["detail"], wait_ms=2200):
            d = page.evaluate(JS_PAGE_CONTENT)
            n_detail += 1
            text, html, last_mod = d["text"], d["html"], d["last_modified"]
            # Keep the card's title — see the Public Consultation note; several
            # cards can share one page, and the orchestrator dedupes on title.
            title = title or d["title"]
            for f in d["docs"]:
                if f["href"] not in {x["href"] for x in files}:
                    files.append({"href": f["href"], "text": f["title"], "title": ""})
            # No go_back(). The page walk has already finished and `rows` is
            # complete, so returning to the list buys nothing and cost ~2s per
            # item — a little over 1.5 hours across the 3,297 announcements.
        elif not tab.get("detail"):
            # Prospectuses and Shareholder Circulars have no detail page at all:
            # the card links straight at the PDF, and the card text is the record.
            text = c["text"]
        if not text and c["text"]:
            # FALL BACK TO THE CARD when the detail page could not be fetched.
            #
            # CMA throttles a long Announcements crawl hard — 620 of 1,050 detail
            # fetches failed after 853 retries. Those records were written with
            # no text at all, even though the card itself carries the
            # announcement's opening paragraph (~300 chars). Storing a title with
            # no body loses information we already had in hand; body_source says
            # which one this is, so a later pass can top up just the summaries
            # instead of re-fetching all 1,050.
            text = c["text"]
            body_source = "card_summary"
        else:
            body_source = "detail_page" if c.get("detail") else "card"

        rk = c["data"].get("id") or c["title"] or c["text"][:80]
        cat = key2cat.get(rk, "")
        trail = trail_root + ([cat] if cat else [])
        records.append(mk_record(
            trail + [title], title, url, text=text, html=html, files=files,
            parent=tab["url"], row_text=c["text"],
            published_date=published, **{PAGE_STAMP_FIELD: last_mod},
            body_source=body_source, cma_id=c["data"].get("id", "")))
        if tab.get("text_as_document") and text:
            # Most Announcements are plain-text press releases with no
            # attached file. Keying only on `files` dropped ~92% of them —
            # measured 87/1049 documents on a full run. The article page
            # itself IS the regulatory content here, so it becomes the
            # document: content_text/content_html flow through extra_meta and
            # document_html (see cma_crawler_wrapper._to_document) so the
            # orchestrator's Tier 1b reads it directly instead of trying to
            # download and extract a file that does not exist.
            #
            # ONE row per announcement, not one for the text plus one more
            # per attachment: any PDFs are secondary to the announcement's
            # own text (the regulatory content), so they go in
            # attachment_links rather than spawning their own PDF-typed rows
            # under the same section_path -- the earlier version did exactly
            # that and duplicated every attached announcement.
            documents[(url, " > ".join(trail))] = {
                "title": title, "doc_url": url,
                "type": tab["label"].rstrip("s"), "found_on": tab["url"],
                "section_path": " > ".join(trail), "published_date": published,
                "content_text": text, "content_html": html,
                "reference_no": _reference_no_from_url(url),
                "attachment_links": " | ".join(dict.fromkeys(f["href"] for f in files)),
            }
        else:
            # Prospectuses / Shareholder Circulars: no detail page, the
            # file(s) ARE the record. Multi-attachment convention applies
            # here too -- one file names the document, more than one leaves
            # document_url empty with attachment_links holding the set.
            doc_url, attachment_links = _one_or_many(
                [f["href"] for f in files], fallback_url=url)
            if doc_url or attachment_links:
                documents[(doc_url or url, " > ".join(trail))] = {
                    "title": title, "doc_url": doc_url, "type": "PDF",
                    "found_on": url, "section_path": " > ".join(trail),
                    "published_date": published, "attachment_links": attachment_links}
        if tab.get("detail") and i % 25 == 0:
            print(json.dumps({"event": "progress", "tab": tab["label"],
                              "details": i, "of": len(rows)}), flush=True)
        if tab.get("detail") and i % CHECKPOINT["every"] == 0:
            checkpoint(records, documents.values())

    print(json.dumps({"event": "fill_rates", "tab": tab["label"],
                      "rows": len(rows), "details_opened": n_detail,
                      "published_date": f"{n_date}/{len(rows)}"}), flush=True)
    return records, list(documents.values())


# Columns whose value is a file rather than a fact. These are the regulatory
# documents hiding inside a register, and the reason a register is worth
# crawling at all.
DOC_COLUMNS = re.compile(
    r"articles?\s+of\s+association|transparency\s+report|issuance\s+brochure|"
    r"^rules?$|terms\s*(and|&)\s*conditions|prospectus|brochure|document", re.I)

REGISTERS = []          # filled by crawl_register(); written beside pages.json

# "Last modified date:12/11/2025 - 04:12 AM Saudi Arabia time" IS NOT A DOCUMENT
# DATE, and it must not be written to last_updated_date.
#
# Measured across a full crawl:
#     Implementing Regulations   36 records, ONE distinct value (12/11/2025)
#     Announcements             262 records, 146 of them 07/11/2025
#     Public Consultation        90 records,  80 of them 07/11/2025
#
# Those clusters are CMA touching its whole site on two days — a content
# migration — not 36 regulations being revised in one minute. As
# last_updated_date it would be read as a revision date, and the first
# monitoring run after any future migration would report every CMA document as
# changed on the same day. It is kept, under a name that says what it is.
PAGE_STAMP_FIELD = "page_last_modified"

# "05-August-2026" on a card — the announcement's own date.
#
# NO TRAILING \b. The card runs the date straight into the title with no space:
#   "14-July-2026Change the name of Pinnacle Capital Company"
# and there is no word boundary between "6" and "C", so a trailing \b refused to
# match. That silently cost published_date on 814 of 1,050 announcements — and
# published_date is part of a document's identity, so the pipeline would have
# deduped two different announcements that happen to share a title. Forty-four
# titles in this tab are shared by more than one announcement.
CARD_DATE = re.compile(r"\b(\d{1,2}-[A-Za-z]{3,9}-\d{4})")
CARD_DATE_LEAD = re.compile(r"^\s*\d{1,2}-[A-Za-z]{3,9}-\d{4}\s*")


def _card_date(text: str):
    """Parse the card's own "05-August-2026" date, or None. Used for the
    `since_days` cutoff below -- separate from the published_date string
    stored on the record, which stays the raw text."""
    m = CARD_DATE.search(text or "")
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%d-%B-%Y")
    except ValueError:
        return None


def expand_groups(page, batch=8, settle_ms=1200, rounds=60):
    """Open every collapsed group so its rows load.

    Investment Funds is 108 accordion items, one per fund manager, each holding
    a table that renders the literal word "Loading..." until the item is
    expanded. Scrolling does not trigger it and neither does the (hidden)
    manager dropdown — the group has to be clicked. Unexpanded, the page reports
    108 tables and 3 usable funds out of 382.

    Clicked in batches: firing 108 AJAX requests at once is the sort of thing
    that got this crawler throttled off the Announcements list.
    """
    n = page.evaluate(
        "()=>document.querySelectorAll('.accordion-button.collapsed,"
        "[data-bs-toggle=collapse].collapsed').length")
    if not n:
        return 0
    print(json.dumps({"event": "expanding_groups", "collapsed": n}), flush=True)
    opened = 0
    for _ in range(rounds):
        got = page.evaluate(
            """(k)=>{const b=[...document.querySelectorAll('.accordion-button.collapsed,'
               +'[data-bs-toggle=collapse].collapsed')].slice(0,k);
               b.forEach(x=>x.click()); return b.length;}""", batch)
        if not got:
            break
        opened += got
        page.wait_for_timeout(settle_ms)
    # Wait for the last batch, then wait for the ROW COUNT to settle.
    #
    # "no table still says Loading" is not the same as "every table has filled":
    # a group that has swapped the placeholder out but not yet drawn its rows
    # passes that check. Reading there gave 57 public funds on one run and 43 on
    # the next from identical code, which looked like site flakiness and was
    # actually this.
    for _ in range(20):
        left = page.evaluate(
            "()=>Array.from(document.querySelectorAll('table.table-striped'))"
            ".filter(t=>/loading/i.test(t.innerText||'')).length")
        if not left:
            break
        page.wait_for_timeout(1200)
    last, stable = -1, 0
    for _ in range(25):
        n = page.evaluate(
            "()=>Array.from(document.querySelectorAll('table.table-striped'))"
            ".reduce((k,t)=>k+t.querySelectorAll(':scope > tbody > tr').length,0)")
        stable = stable + 1 if n == last else 0
        if stable >= 3 and n:
            break
        last = n
        page.wait_for_timeout(1000)
    still = page.evaluate(
        "()=>Array.from(document.querySelectorAll('table.table-striped'))"
        ".filter(t=>/loading/i.test(t.innerText||'')).length")
    print(json.dumps({"event": "groups_expanded", "opened": opened,
                      "still_loading": still}), flush=True)
    return opened


def wait_for_rows(page, tries=12, gap_ms=1200):
    """Wait for an AJAX table to actually hold data.

    Investment Funds renders its tables immediately with a single "Loading…"
    row and fills them a moment later. Reading on load returned 3 funds out of
    382 — and nothing about that looked like a failure: real columns, real
    table, plausible-looking content. So wait until no table says Loading and
    the row count has stopped growing.
    """
    last = -1
    for _ in range(tries):
        # Scroll: the grouped tables load as they come into view.
        try:
            page.mouse.wheel(0, 9000)
        except Exception:
            pass
        st = page.evaluate("""()=>{
          const t = Array.from(document.querySelectorAll('table.table-striped'));
          const rows = t.reduce((n,x)=>n + x.querySelectorAll(':scope > tbody > tr').length, 0);
          const loading = t.filter(x => /loading/i.test(x.innerText || '')).length;
          return {rows, loading};}""")
        if not st["loading"] and st["rows"] == last and st["rows"] > 0:
            return st["rows"]
        last = st["rows"]
        page.wait_for_timeout(gap_ms)
    return last


def _row_key(cells, cols):
    """A stable identity for a register row.

    Deliberately NOT the row's position or its name. Fund names get amended and
    rows get re-sorted; an id or licence number does not. Monitoring diffs on
    this, so a bad key turns "one fund renamed" into "382 rows changed".
    """
    pref = ("fund id", "unified national number", "registration number",
            "license number", "licence number", "id")
    low = [c.lower().strip() for c in cols]
    for p in pref:
        if p in low:
            v = cells[low.index(p)] if low.index(p) < len(cells) else ""
            if v.strip():
                return v.strip()
    return cells[0].strip() if cells else ""


def _esc(s) -> str:
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def render_register(trail, label, cols, entries, source_url):
    """Turn a register into one readable document.

    The frontend shows a document's HTML in its Description pane, the same way
    it shows an article of the Saudi Central Bank Law. A register has no such
    body, so this builds one: a plain table with the site's own column names,
    plus each entity's nested rows underneath it where there are any.

    `cols` is the TABLE-shaped register's header row and is empty for a
    CARD-shaped one (Financial Market Institutions has no <table> at all) --
    entries there carry `fields = {"text": ..., **card_data}` instead of one
    key per column. Falling back to a hardcoded `["value"]` header when cols
    is empty rendered every cell blank, since nothing in a card entry is keyed
    "value" -- 238 institutions became a 289-character empty table. Derive the
    header from what the entries actually carry instead.
    """
    if not cols:
        seen_keys = []
        for e in entries[:50]:
            for k in (e.get("fields") or {}):
                if k not in seen_keys:
                    seen_keys.append(k)
        cols = seen_keys or ["value"]
    head = "".join(f"<th>{_esc(c)}</th>" for c in cols)
    body, lines = [], []
    for e in entries:
        f = e.get("fields") or {}
        cells = "".join(f"<td>{_esc(f.get(c, ''))}</td>" for c in cols)
        body.append(f"<tr>{cells}</tr>")
        lines.append(" | ".join(f"{c}: {f.get(c, '')}" for c in cols if f.get(c)))
        for n in e.get("nested", []):
            ncols = n.get("cols") or []
            nh = "".join(f"<th>{_esc(c)}</th>" for c in ncols)
            nb = "".join("<tr>" + "".join(f"<td>{_esc(v)}</td>" for v in r) + "</tr>"
                         for r in n.get("rows", []))
            body.append(
                f'<tr><td colspan="{len(cols)}">'
                f'<table><thead><tr>{nh}</tr></thead><tbody>{nb}</tbody></table>'
                f'</td></tr>')
            for r in n.get("rows", []):
                lines.append("    " + " | ".join(
                    f"{ncols[i] if i < len(ncols) else ''}: {v}"
                    for i, v in enumerate(r) if v))
    html = (f"<h2>{_esc(label)}</h2>"
            f"<p>{len(entries)} entries. Source: "
            f'<a href="{_esc(source_url)}">{_esc(source_url)}</a></p>'
            f"<table><thead><tr>{head}</tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table>")
    text = f"{label}\n{len(entries)} entries.\n" + "\n".join(lines)
    return mk_record(
        trail, label, source_url, text=text, html=html, parent=source_url,
        page_title=label,
        # Not a rule. The pipeline should index and display it, not mine it for
        # requirements — there are none in a list of licence holders.
        record_type="register_table", entity_count=len(entries))


def crawl_register(page, tab, trail_root, max_chunks=None, limit=None):
    """A list of licensed ENTITIES, stored as a table rather than as documents.

    Special Purpose Entities (890), Investment Funds (382), Financial Market
    Institutions (237+6), Registered Accounting Offices (~100) and Real Estate
    Contributions (1).

    Two outputs, and the split is the whole point:

      registers[]  one row per entity, every column kept. Marked
                   record_type="register_row" so the orchestrator does NOT run
                   the 4-stage LLM over it — a row reading
                   "Sukuk Morabha 2409 | Effective | Debt-Based" holds no
                   requirement, and ~1,600 of them would cost real money to
                   learn that. Monitoring diffs these on _row_key and reports
                   "3 licences lapsed" instead of "3 documents changed".

      documents[]  the file each row links to — articles of association, fund
                   rules, transparency reports, issuance brochures. Those ARE
                   regulatory documents and go through the normal pipeline,
                   filed under the entity they belong to.
    """
    records, documents = [], {}
    if not load(page, tab["url"], wait_ms=4500):
        print(json.dumps({"event": "error", "url": tab["url"],
                          "message": "register page did not load"}), flush=True)
        return records, []

    expand_groups(page)
    wait_for_rows(page)
    parts = page.evaluate(JS_REGISTER)
    if not parts:
        raise SystemExit(f"{tab['label']}: no register table or cards found.")
    stated = page.evaluate(JS_PAGER_TOTAL)
    # Read the list's own row count NOW, on the landing page. Reading it at the
    # end silently returned 0, because by then the page has followed a cursor
    # (or failed to) and the source table is gone — so the completeness check
    # passed by default on exactly the run that needed it.
    src_count = page.evaluate(JS_SOURCE_COUNT)
    print(json.dumps({"event": "register_start", "tab": tab["label"],
                      "parts": [{"label": p["label"], "cols": p["cols"],
                                 "rows": len(p["rows"]), "cards": len(p["cards"]),
                                 "pageable": p["hasNext"]} for p in parts],
                      "stated_pages": stated}, ensure_ascii=False), flush=True)

    for idx, part in enumerate(parts):
        label = part["label"] or tab["label"]
        cols = part["cols"]
        seen, rows, cards, chunks = set(), [], [], 0
        cursor, cur_part = tab["url"], part

        while True:
            chunks += 1
            fresh = 0
            for r in cur_part["rows"]:
                k = _row_key(r["cells"], cols) or "|".join(r["cells"])[:120]
                if k in seen:
                    continue
                seen.add(k); rows.append(r); fresh += 1
            for c in cur_part["cards"]:
                k = c["data"].get("id") or c["text"][:120]
                if k in seen:
                    continue
                seen.add(k); cards.append(c); fresh += 1

            nxt = cur_part.get("next") or ""
            if not nxt or (max_chunks and chunks >= max_chunks):
                break
            if not fresh:
                # Same guard as the paged lists: a cursor that stops advancing
                # would otherwise loop on one chunk until the cap.
                print(json.dumps({"event": "register_stop", "tab": tab["label"],
                                  "part": label, "chunk": chunks,
                                  "why": "chunk added no new rows"}), flush=True)
                break
            if not load(page, nxt, wait_ms=2500):
                print(json.dumps({"event": "warning", "tab": tab["label"],
                                  "part": label,
                                  "message": f"cursor load failed at chunk {chunks}"}),
                      flush=True)
                break
            # Re-locate this web part after the reload. Match on the pane id;
            # index is a fallback and the labels are checked either way.
            expand_groups(page)
            wait_for_rows(page)
            after = page.evaluate(JS_REGISTER)
            cur_part = next((x for x in after
                             if x["paneId"] and x["paneId"] == part["paneId"]), None)
            if cur_part is None:
                cur_part = after[idx] if idx < len(after) else None
            if cur_part is None:
                print(json.dumps({"event": "warning", "tab": tab["label"],
                                  "part": label,
                                  "message": "web part vanished after paging"}),
                      flush=True)
                break
            if chunks % 10 == 0:
                print(json.dumps({"event": "progress", "tab": tab["label"],
                                  "part": label, "chunks": chunks,
                                  "rows": len(rows) + len(cards)}), flush=True)

        # --- which columns hold documents
        doc_idx = [i for i, c in enumerate(cols) if DOC_COLUMNS.search(c or "")]
        trail = trail_root + ([label] if label != tab["label"] else [])
        entries, n_docs, n_nested = [], 0, 0

        for r in rows:
            cells = r["cells"]
            rec = {c: (cells[i] if i < len(cells) else "")
                   for i, c in enumerate(cols)} if cols else {"value": " | ".join(cells)}
            key = _row_key(cells, cols)
            entry = {"register": label, "key": key, "record_type": "register_row",
                     "section_path": " > ".join(trail), "fields": rec,
                     "source_url": tab["url"]}
            if r["nested"]:
                # Accounting Offices nests a table of accountants inside each
                # office row. Flattening loses which accountant works where, so
                # they stay attached as their own rows.
                entry["nested"] = r["nested"]
                n_nested += sum(len(n["rows"]) for n in r["nested"])
            entries.append(entry)
            name = cells[0] if cells else key
            for l in r["links"]:
                if not re.search(r"\.(pdf|docx?|xlsx?)(\?|$)", l["href"], re.I):
                    continue
                dtrail = trail + [name]
                documents[(l["href"], " > ".join(dtrail))] = {
                    "title": l["text"] or name, "doc_url": l["href"], "type": "PDF",
                    "found_on": tab["url"], "section_path": " > ".join(dtrail),
                    "entity": name, "register": label}
                n_docs += 1

        for c in cards:
            entry = {"register": label, "key": c["data"].get("id", ""),
                     "record_type": "register_row",
                     "section_path": " > ".join(trail),
                     "fields": {"text": c["text"], **c["data"]},
                     "source_url": tab["url"]}
            entries.append(entry)
            for l in c["links"]:
                if not re.search(r"\.(pdf|docx?|xlsx?)(\?|$)", l["href"], re.I):
                    continue
                dtrail = trail + [c["text"][:60]]
                documents[(l["href"], " > ".join(dtrail))] = {
                    "title": l["text"] or c["text"][:60], "doc_url": l["href"],
                    "type": "PDF", "found_on": tab["url"],
                    "section_path": " > ".join(dtrail), "register": label}
                n_docs += 1

        if limit:
            entries = entries[:limit]

        # A VIEWABLE version of the register, as one page record.
        #
        # The structured rows above are for monitoring; they render as nothing in
        # a UI that displays a document body. Financial Market Institutions and
        # Registered Accounting Offices link to no files at all, so without this
        # they would appear in the library as empty folders — 243 licensed firms
        # and 16 audit offices that the crawl found and the user cannot see.
        #
        # One record per register, not per entity: 1,298 single-row documents
        # would bury the real regulations in the tree, and a row is not a rule.
        # record_type keeps the 4-stage extraction off it.
        #
        # It also becomes ONE document row -- render_register() already builds
        # exactly this (a table of every entity, meant for the frontend's
        # Description pane per its own docstring), it just never reached
        # `documents` before. Decided 2026-08-13: a combined row per register,
        # not one row per entity -- browsable and approvable as a single
        # directory entry, not searchable per institution. That trade only
        # matters if this register ever needs per-entity filtering later.
        if entries:
            rec = render_register(trail, label, cols, entries, tab["url"])
            records.append(rec)
            documents[(tab["url"], " > ".join(trail))] = {
                "title": label, "doc_url": tab["url"], "type": "Register",
                "found_on": tab["url"], "section_path": " > ".join(trail),
                "content_text": rec["text"], "content_html": rec["html"],
            }

        REGISTERS.append({"register": label, "section_path": " > ".join(trail),
                          "columns": cols, "source_url": tab["url"],
                          "document_columns": [cols[i] for i in doc_idx],
                          "rows": entries})
        blank = sum(1 for e in entries if not e["key"])
        print(json.dumps({"event": "register_part", "tab": tab["label"],
                          "part": label, "chunks": chunks,
                          "entities": len(entries), "nested_rows": n_nested,
                          "documents": n_docs,
                          "document_columns": [cols[i] for i in doc_idx],
                          "rows_without_key": blank}, ensure_ascii=False), flush=True)
        if blank:
            print(json.dumps({"event": "warning", "tab": tab["label"],
                              "part": label,
                              "message": f"{blank} row(s) have no stable key — "
                                         f"monitoring cannot diff these"}), flush=True)

    # The register's own claim about its size, against what we actually read.
    # Without this, Investment Funds reports 3 funds out of 382 and looks
    # perfectly healthy: real columns, real rows, no error anywhere.
    src = src_count
    total = sum(len(r["rows"]) for r in REGISTERS
                if r["source_url"] == tab["url"])
    if src and total < src:
        report_gap(tab=tab["label"], rows=total, source_list_count=src,
                   why="register holds fewer rows than the list claims")
        if total < src * 0.5:
            raise SystemExit(
                f"{tab['label']}: read {total} of {src} entities. Refusing to "
                f"write a register that is less than half the list — a partial "
                f"register is indistinguishable from a shrinking one, and "
                f"monitoring would report the missing rows as deletions.")
    return records, list(documents.values())


def crawl_tab(ctx, tab_key: str, max_chapters=None, max_articles=None):
    tab = TABS[tab_key]
    trail_root = tab_root(tab)
    shape = tab.get("shape", "law_chapters")

    if shape not in IMPLEMENTED:
        # Fail loudly. A tab that quietly returns nothing looks exactly like a
        # tab with no documents, and that is the failure mode this whole project
        # keeps tripping over.
        raise SystemExit(
            f"tab '{tab_key}' has shape '{shape}', which is not implemented "
            f"yet. Implemented: {', '.join(sorted(IMPLEMENTED))}")

    page = ctx.new_page()
    simple = {
        "single_page":              lambda: crawl_single_page(page, tab, trail_root),
        "cards":                    lambda: crawl_cards(page, tab, trail_root),
        "cards_grouped":            lambda: crawl_cards(page, tab, trail_root, grouped=True),
        "subtabs_paginated_detail": lambda: crawl_regs(page, tab, trail_root, max_articles),
        "tabs_cards_detail":        lambda: crawl_consult(page, tab, trail_root, max_articles),
        "faq_paginated":            lambda: crawl_faqs(page, tab, trail_root, max_articles),
        "cards_paged":              lambda: crawl_paged(page, tab, trail_root,
                                                        max_chapters, max_articles),
        "register":                 lambda: crawl_register(page, tab, trail_root,
                                                           max_chapters, max_articles),
    }
    if shape in simple:
        try:
            return simple[shape]()
        finally:
            page.close()

    records, documents = [], {}
    try:
        if not load(page, tab["url"]):
            print(json.dumps({"event": "error", "message": "tab page did not load"}))
            return records, documents

        chapters = page.evaluate(JS_CHAPTER_LINKS, tab["chapter_re"].pattern)
        # The tab page also links the whole law as one PDF. NOT its own document
        # row: every article inside it already gets one below, and the PDF is the
        # same text again under one undifferentiated title — a duplicate of the
        # law, not a document in its own right.
        n_pdfs_on_page = page.evaluate(
            "()=>document.querySelectorAll('a[href$=\".pdf\" i]').length")
        print(json.dumps({"event": "chapters", "found": len(chapters),
                          "pdfs_on_page_not_captured": n_pdfs_on_page}), flush=True)
        if max_chapters:
            chapters = chapters[:max_chapters]

        for ci, ch in enumerate(chapters, 1):
            folder = chapter_folder(ch["text"], ch["href"])
            ch_trail = trail_root + [folder]
            if not load(page, ch["href"]):
                print(json.dumps({"event": "error", "url": ch["href"],
                                  "message": "chapter did not load"}), flush=True)
                continue
            arts = page.evaluate(JS_ARTICLES)
            # A chapter page always DISPLAYS one article, and normally that one
            # is also the open accordion item. Not always: CH8 lists only
            # "Article Fifty" while displaying "Article Forty Nine", which has no
            # accordion entry at all — so iterating the accordion alone silently
            # dropped a whole article. When nothing is open, take the displayed
            # one from the <h1> ("Chapter Eight ... - Article Forty Nine").
            if arts and not any(a["open"] for a in arts):
                h1 = page.evaluate(
                    "()=>{const h=document.querySelector('h1');"
                    "return h?(h.innerText||'').replace(/\s+/g,' ').trim():''}")
                shown = h1.rsplit(" - ", 1)[-1].strip() if " - " in h1 else ""
                if shown and shown.lower() not in {a["name"].lower() for a in arts}:
                    arts.insert(0, {"name": shown, "href": "", "open": True, "chars": 0})
                    print(json.dumps({"event": "recovered_article",
                                      "chapter": folder, "article": shown,
                                      "why": "displayed but absent from the accordion"}),
                          flush=True)
            if max_articles:
                arts = arts[:max_articles]
            print(json.dumps({"event": "chapter", "n": ci, "of": len(chapters),
                              "folder": folder, "articles": len(arts)}), flush=True)

            for art in arts:
                # The open one IS this page (a chapter opens on its first
                # article), so it has no onclick URL of its own.
                url = urljoin(BASE, art["href"]) if art["href"] else ch["href"]
                if art["href"] and not load(page, url):
                    print(json.dumps({"event": "error", "url": url,
                                      "message": "article did not load"}), flush=True)
                    continue
                body = page.evaluate(JS_ARTICLE_BODY)
                records.append({
                    "section_path": " > ".join(ch_trail + [art["name"]]),
                    "title": art["name"],
                    "url": url,
                    "depth": len(ch_trail),
                    "linked_from_title": art["name"],
                    "parent_page_url": ch["href"],
                    "status": "",
                    "n_pdfs": 0,
                    "pdf_links": "",
                    "text_len": len(body["text"]),
                    "html_file": "",
                    "text": body["text"],
                    "html": body["html"],
                    "breadcrumb": ch_trail + [art["name"]],
                    "row_text": "",
                    # The page title carries both, e.g.
                    # "Chapter One Definitions - Article Two".
                    "page_title": body["title"],
                })
                # One row per Article, so a single-article amendment can be
                # approved/versioned on its own instead of only through the
                # whole-law PDF above. Keyed on (url, section_path) like every
                # other document dict in this file — the chapter's open
                # article shares the chapter's own url, which is fine: it is
                # still unique once paired with its section_path.
                documents[(url, " > ".join(ch_trail + [art["name"]]))] = {
                    "title": art["name"], "doc_url": url, "type": "Article",
                    "found_on": ch["href"],
                    "section_path": " > ".join(ch_trail + [art["name"]]),
                    "content_text": body["text"], "content_html": body["html"],
                }
    finally:
        page.close()
    return records, list(documents.values())


def main():
    ap = argparse.ArgumentParser(
        description="CMA runner — Laws & Regulations, Media Center, Capital Market")
    ap.add_argument("--tab", default="capital_market_law", choices=sorted(TABS))
    ap.add_argument("--all", action="store_true",
                    help="run every IMPLEMENTED tab in turn")
    ap.add_argument("--section", default="",
                    help="run every implemented tab in one section, e.g. "
                         "'Capital Market'")
    ap.add_argument("--out", default="")
    # --max-chapters doubles as the page cap for cards_paged, and
    # --max-articles as the row cap. Same meaning: how deep to go.
    ap.add_argument("--max-chapters", type=int, default=0,
                    help="chapters, or pages to walk for a paged list")
    ap.add_argument("--max-articles", type=int, default=0,
                    help="articles, or rows to keep for a paged list")
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--delay-ms", type=int, default=0,
                    help="pause between detail fetches (and half that between "
                         "page clicks). CMA throttles; 600-1000 finishes sooner "
                         "than 0 on the long tabs.")
    ap.add_argument("--resume", action="store_true",
                    help="reuse detail pages already captured in the existing "
                         "pages.json instead of re-fetching them")
    ap.add_argument("--save-html", action="store_true",
                    help="write each record's HTML to out/html/ and record the "
                         "path in html_file")
    a = ap.parse_args()

    if a.section:
        tabs = [k for k, v in TABS.items()
                if v.get("shape") in IMPLEMENTED
                and v.get("section", DEFAULT_SECTION).lower() == a.section.lower()]
        if not tabs:
            raise SystemExit(
                f"no implemented tab in section {a.section!r}. Sections: "
                + ", ".join(sorted({v.get("section", DEFAULT_SECTION)
                                    for v in TABS.values()})))
    elif a.all:
        tabs = [k for k, v in TABS.items() if v.get("shape") in IMPLEMENTED]
    else:
        tabs = [a.tab]

    for tab_key in tabs:
        # With several tabs, --out is the PARENT directory, not the destination.
        # Treating it as the destination made every tab overwrite the last and
        # left one folder holding only the final tab's results.
        out = (Path(a.out) / f"cma_{tab_key}" if a.out and len(tabs) > 1
               else Path(a.out or f"output/site_runners/cma_{tab_key}"))
        out.mkdir(parents=True, exist_ok=True)
        PACE["detail_ms"] = a.delay_ms
        PACE["page_ms"] = a.delay_ms // 2
        PRIOR.clear()
        if a.resume:
            PRIOR.update(load_prior(out / "pages.json"))
        CHECKPOINT["path"] = out / "pages.json"
        CHECKPOINT["meta"] = {"seed": TABS[tab_key]["url"],
                              "shape": TABS[tab_key].get("shape"),
                              "engine": "site_runner",
                              "tab": TABS[tab_key]["label"]}
        t0 = time.time()
        with sync_playwright() as pw:
            b = pw.chromium.launch(headless=not a.headed,
                                   args=["--disable-dev-shm-usage", "--disable-gpu"])
            ctx = b.new_context(
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
                locale="en-US", viewport={"width": 1600, "height": 1000})
            recs, docs = crawl_tab(ctx, tab_key,
                                   a.max_chapters or None, a.max_articles or None)
            try:
                b.close()
            except Exception:
                pass

        if a.resume:
            recs, docs = merge_prior(recs, docs)

        if a.save_html:
            # One file per record, and html_file points at it. The schema has
            # always carried that field; until now it was empty because the
            # markup lived only inside pages.json, where it is unreadable and
            # cannot be opened in a browser to check what was captured.
            hd = out / "html"
            hd.mkdir(parents=True, exist_ok=True)
            written = 0
            for i, r in enumerate(recs, 1):
                if not r.get("html"):
                    continue
                slug = re.sub(r"[^A-Za-z0-9]+", "-",
                              (r.get("title") or "record"))[:70].strip("-") or "record"
                fp = hd / f"{i:04d}_{slug}.html"
                fp.write_text(
                    "<!doctype html><meta charset='utf-8'>"
                    f"<title>{_esc(r.get('title',''))}</title>"
                    f"<!-- {_esc(r.get('section_path',''))} -->\n"
                    f"<!-- source: {_esc(r.get('url',''))} -->\n"
                    + r["html"], encoding="utf-8")
                r["html_file"] = str(fp)
                written += 1
            print(json.dumps({"event": "html_files", "tab": tab_key,
                              "written": written, "dir": str(hd)}))

        (out / "pages.json").write_text(json.dumps(
            {"seed": TABS[tab_key]["url"], "shape": TABS[tab_key].get("shape"),
             "engine": "site_runner", "tab": TABS[tab_key]["label"],
             "pages": recs, "documents": docs,
             # Registers travel in the SAME file so one adapter reads everything,
             # but under their own key — a register row is not a page and must
             # not be fed to the extraction pipeline as one.
             "registers": REGISTERS}, ensure_ascii=False, indent=2),
            encoding="utf-8")
        n_ent = sum(len(r["rows"]) for r in REGISTERS)
        if REGISTERS:
            print(json.dumps({"event": "registers", "tab": tab_key,
                              "tables": len(REGISTERS), "entities": n_ent,
                              "names": [r["register"] for r in REGISTERS]},
                             ensure_ascii=False))
        REGISTERS.clear()          # per-tab, not cumulative across --all

        # Two different things, kept apart on purpose.
        #
        # EMPTY is the alarm: no text and no file means the record carries
        # nothing at all, and something failed to extract.
        #
        # SHORT is usually not a problem. A card that is a title plus a PDF has
        # its content in the file; an FAQ answer of "Yes, you can after getting
        # a license" is 80 characters and complete. Reporting those as "thin"
        # trained us to ignore the warning, which is how a real empty record
        # would get through.
        empty = [r for r in recs if not r["text_len"] and not r["n_pdfs"]]
        short = [r for r in recs if 0 < r["text_len"] < 100 and not r["n_pdfs"]]
        print(json.dumps({"event": "done", "tab": tab_key,
                          "records": len(recs), "documents": len(docs),
                          "empty": len(empty), "short": len(short),
                          "seconds": round(time.time() - t0, 1),
                          "out": str(out / "pages.json")}, ensure_ascii=False))
        if empty:
            print(json.dumps({"event": "warning", "tab": tab_key,
                              "message": f"{len(empty)} record(s) with NO text and NO file",
                              "examples": [r["title"][:60] for r in empty[:3]]},
                             ensure_ascii=False))


if __name__ == "__main__":
    main()
