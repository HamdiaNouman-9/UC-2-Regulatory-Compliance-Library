# NCA onboarding: Cyber Regulations and Operations (2026-09-24)

Regulator: **National Cybersecurity Authority (NCA)**, https://nca.gov.sa/en
Scope asked for: every tab under the **Cyber Regulations and Operations** menu.

| Tab | URL | Status |
|---|---|---|
| Laws and Regulations | `/en/laws-and-regulations/` | **wired**, 2 documents |
| Regulatory Documents | `/en/regulatory-documents/` | **wired**, 23 entries from 17 cards |
| Registrations and Licensing | `/en/registration-and-licensing/` | **wired (custom page)**, 1 row with 2 PDFs attached |
| Cyber Enablement | `/en/enablement/` | **wired (generic)**, 1 page |
| Cyber Threats and Incidents | `/en/cyber-operations/` | **wired (generic)**, 1 page |

## What the page looks like

One CMS rich-text block (`div.html-content`). Each instrument is a flat run of
siblings with **no wrapper per instrument**:

```html
<p><strong>Statute of The National Cybersecurity Authority:</strong></p>
<p>The National Cybersecurity Authority (NCA) was established under ...</p>
<p>&nbsp;</p>
<div><strong><span class="custom-dga-button">
  <a href="https://cdn.nca.gov.sa/api/files/public/upload/...pdf">View the Statute ...</a>
</span></strong></div>
```

The "View" buttons are plain links to the PDF, so nothing has to be clicked. The site
is server-rendered, so a plain GET returns everything and no browser is needed.
There was no bot wall (HTTP 200).

## Why not the generic crawler

The first run of `generic_crawler/crawler.py` returned `status: ok`, 2 PDFs, but:

- titles were the **button text** ("View the Statute of ..."), not the bold heading
- the only HTML it could attach was the **whole page**, not the text under each heading
- `section_path` was empty

A formfill form can't express this either, because it needs one DOM element per row.

## What was added

| File | Change |
|---|---|
| `crawler/nca_crawler.py` | `NCACrawler` (rich-text page, split on bold headings), `NCACardListCrawler` (card listing), `NCAPageCrawler` (one page with its files attached) |
| `config/sources/nca.yml` | source config, one entry per tab (three custom, two generic) |
| `config/countries.yml` | NCA added under Kingdom of Saudi Arabia |
| `generic_crawler/crawler.py` | `nca.gov.sa` site profile: HTML cleanup for the two generic tabs |

Each entry maps to the library like this:

| Field | Value |
|---|---|
| `title` | the bold heading, trailing colon dropped |
| `document_html` | the paragraphs under that heading, excluding the View button and empty spacers |
| `document_url` | the PDF the View button links to (if a heading has several files, they go in `extra_meta.attachment_links`) |
| `doc_path` | NCA › Cyber Regulations and Operations › Laws and Regulations › title |
| `content_hash` | title + text + file link, so an edited description or a re-uploaded PDF reads as `modified` |

## Regulatory Documents

Cards (title, date, type tag, "Read More") are paginated **by JavaScript**, 8 per
page over 3 pages. The page links have no `href`. The page's own script fetches the
cards from a public API:

```
POST https://backend.nca.gov.sa/api/public/cms/content/slugs?size=8&page=N
     {"slugs": ["controls-list", "frameworks-and-standard-list", "guidelines-list", "cyber-security-tools"]}
```

That response already contains each **detail page's content**. Two detail pages
(osmacc, scyberedu) were checked against the rendered page: same title, same file
links, date = `publishDate`, and "Last Update" = `modifiedAt`. The crawler therefore
makes **one request** (`size=50`) instead of loading 3 listing pages and 17 detail pages.

| Field | Value |
|---|---|
| `title` | card title |
| `published_date` | `publishDate`, the date on the card and the detail page (ISO) |
| `document_html` | detail content minus its file buttons, link-only paragraphs and spacers |
| `document_url` | the button's file; several files go in `extra_meta.attachment_links` |
| `doc_path` | NCA › Cyber Regulations and Operations › Regulatory Documents › *type tag* › title |
| `extra_meta` | `nca_type` (card tag), `nca_modified_at` ("Last Update"), slugs, CMS id |

Card tag, from the API's `schemaSlug`: `controls-list` = Policies and controls,
`frameworks-and-standard-list` = Frameworks and standards, `guidelines-list` =
Guidelines and support tools.

Two cards are tables of documents. They are handled per slug in the YAML
(business decision, 2026-09-24):

- **Cybersecurity Toolkits** (`html_only`): one entry. The whole 84-row template
  table stays in `document_html` with its 162 links, and no files are stored. Its
  links still feed the fingerprint.
- **Implementation Guides** (`split_tables`): the card keeps its description, and
  each of the 6 guides becomes its own entry under it.

The API's `values.attachments` list is **not shown** on the detail page, so it is
ignored.

## Cyber Enablement and Cyber Threats and Incidents: generic crawler

Both are single information pages with no sub-pages, and the **generic crawler
handles them without changes** (`mode: generic` in the YAML).

- **Cyber Enablement**: the page itself, stored as HTML. It describes 13 NCA service
  programmes (MDR, Deem, CISO Office Support, DDoS protection, ...). No files.
- **Cyber Threats and Incidents**: the page as HTML. Its **RFC 2350** link (NCA's CERT
  profile, PDF) stays inside the HTML and is **not** its own entry
  (`exclude_documents: ["RFC-2350"]`, business decision). "Public Key" is listed on
  the page but not linked.

**HTML cleanup.** A `nca.gov.sa` entry in `SITE_PROFILES` (`generic_crawler/crawler.py`)
trims the stored HTML to the content. Before, it opened with the breadcrumb ("Home"),
title and share buttons, showed empty gaps where images were, and ended with "Last
Update at …" and "Was this page useful? … Feedbacks".

- `content_selector: main > div.py-10`: `<main>` has three children (header,
  content, feedback widget). This keeps only the middle one.
- `drop_selectors`: the Next.js `<img>` tags and the `<div>` each one sits in
  alone (these rendered as empty gaps once stored), plus the "Last Update" line.

Enablement HTML went from 20,958 to 9,949 characters. The profile only affects
generic crawls of `nca.gov.sa`, and the two custom NCA tabs don't use it.

`doc_path_category: true` and `doc_path_title: true` give them the same folder shape
as the custom tabs: NCA › Cyber Regulations and Operations › *tab* › *title*.

## Registration and Licensing: one row, files attached (2026-09-25)

One information page: intro, registration goals, "Who should register?", the MSOC
licences section with the Tier 1 and Tier 2 licensed-company tables, and two PDFs
under "To View".

**Business decision:** the page is **one row**, and both PDFs are **attached** to it
in `extra_meta.attachment_links` (`document_url` empty, the multi-file rule). They
are the *Regulatory Framework for Licensing the Provision of MSOC Services* (the same
file as the Regulatory Documents card) and the *Investor's Guide for Providers of
Cybersecurity Services, Solutions or Products*. Neither is a row of its own.

**Why custom (`NCAPageCrawler`), not generic:** the generic crawler stores each linked
file as its own row. Its `merge_files_at_same_path` only fuses rows that share a
title, which a page and its PDFs never do. Folding files into the page row would
have meant changing shared code every regulator uses. The page crawler makes one
GET request and no browser.

- **Content:** this page uses a different layout from Enablement and Cyber Threats
  (content in `main > div.full-container`, not `div.py-10`). The selector
  `main > div.py-10, main > div.full-container:not(.py-6)` picks the right block on
  all three.
- **Cleanup:** same as the generic tabs: no breadcrumb, share buttons, images (incl.
  the table checkmark icons), "Last Update" line or feedback widget.
- **Not crawled:** the "List of Registered Service Providers" sub-page (a company
  directory, business decision). The page crawler follows no links, so nothing needs
  excluding.
- Its HTML is 47,799 characters, more than an Excel cell holds, so the full text is
  in the `.fulltext.json` sidecar.

## Verified

- `python -m tools.workbook export nca` (2026-09-25): **28 documents** (2 laws + 23 regulatory documents + 1 registration and licensing + 1 enablement + 1 cyber threats), 39 folders, all five tabs
- `check`: `verdict: OK`, no errors, no warnings
- The Toolkits HTML is 76k characters, more than an Excel cell holds, so the export
  writes a `.fulltext.json` sidecar. **Send it with the `.xlsx`.**
- Two back-to-back crawls: identical paths, URLs, hashes and HTML

Note: `export` always reports `new` because `ExcelRepo` starts empty on every run.
Stability was checked by comparing hashes across two crawls instead.

## Monitoring (wired 2026-09-25, **switched on**)

**Weekly re-crawl, Sunday 22:00.** Job `monitor_nca` (`jobs/monitor_jobs.py`), listed as
crawl-as-signal, like MOH and CBE. Registered in `scheduler/scheduler.py` (both
execution modes), `apis/pipeline_api.py` (`/trigger/monitor/monitor_nca`, registry key
`NCA`), `config/scheduler.yml` and `config/change_signals.yml`.

- **Cost:** about 41 requests a week. That is 3 page GETs, 1 CMS API call, 2 browser
  page loads, and 35 one-byte file-size probes.
- **Each run:** new → inserted, modified → new version, unchanged → skipped.
  Disappeared → flagged for withdrawal only after two trustworthy runs at least 20
  hours apart, and never deleted.
- **Weekly, not daily:** NCA publishes rarely, and it blocked this machine for some
  hours on 2026-09-24.

**File replacements are caught.** The NCA crawlers now read each stored file's
**size** (a 1-byte range request) and fold it into `content_hash`. A PDF replaced at
the same URL, which covers the fixed `nca.gov.sa/*.pdf` links, therefore reads as
`modified`. Size is used rather than `ETag` because nginx's ETag includes the file
time, which can differ between CDN servers holding the same file. If a size can't be
read after retries, the crawl stops instead of guessing.

**Why not a header probe (`stored-inventory`):** it checks `document_url` only, and 9 of
the 28 rows keep their files in `attachment_links`. It also can't see new cards. The
sitemap is useless: every `<lastmod>` is the build time.

**Switched on before review, by explicit decision.** No workbook was promoted, so **the
first scheduled run is the ingest**: 28 rows written straight to MSSQL with
`status = ''`, each run through the LLM analysis.

- **Do not promote `output/workbooks/nca.xlsx`.** It was exported before the
  fingerprint gained file sizes, so every row would read `modified`.
- **The scheduler machine needs Playwright's `chromium-headless-shell`** for the two
  generic tabs.

**Still not caught:** a template swapped inside the Cybersecurity Toolkits table at the
same URL. Its 161 links are part of the HTML, not stored files.

## Known limits

- *(Closed 2026-09-25 by the file-size fingerprint, see Monitoring.)* Originally:
  the fingerprint did not include the files, so a file replaced at the **same** URL
  was not detected. Files on `cdn.nca.gov.sa/api/files/...`
  get a new ID in the URL on every upload, so a replacement there shows up as a
  changed link. Several Regulatory Documents files have **fixed** URLs, though
  (`nca.gov.sa/osmacc-en.pdf`, `ncs_en.pdf`, `otcc_en.pdf`, ...). For those, only
  an edit to the page text would show that something changed (`modifiedAt` is stored but not part of the fingerprint).

## First ingest (2026-09-25, via the API)

`POST /trigger/monitor/monitor_nca`, 11:12–11:54. Crawl and database write succeeded:
**28 regulations, ids 47872–47899**, all `status = ''` and awaiting review. Run
trustworthy, baseline PASS.

**The LLM analysis only fully succeeded for 9 of 28.** Data Cybersecurity Controls
(47881) is partial and 18 have none:

- **47881 Data Cybersecurity Controls: partial (33 of 141 requirements).** One
  activity's `frequency` was longer than `Activity.frequency` (`nvarchar(100)`), and
  SQL Server's rejection aborted the rest of the regulation. **Fixed** in
  `storage/mssql_repo.py`: `_fit_column` now clips model text to the column width in
  `insert_activity` and `insert_requirement` and logs the full value.
- **18 regulations: no analysis.** From about 11:51 every new outgoing connection from
  this machine failed with `WinError 10013` ("access a socket in a way forbidden by its
  access permissions"). It hit `openrouter.ai` **and** `cdn.nca.gov.sa` at the same
  moment, so this was **local** (Windows or a firewall refusing new sockets, likely
  under the analysis's parallel connections), **not** an OpenRouter or NCA outage.
  OpenRouter answered normally at the next check.
- **The regulation rows themselves are complete and correct** (checked 2026-09-25): 28
  rows, one version each, fingerprint, folder, file URL or `attachment_links`, and HTML
  wherever the site has text. The 6 implementation-guide rows have no HTML by design
  (the site gives only a title and a PDF). Their text lives in the PDF and is only read
  during analysis.
- **Also seen:** the cross-chunk duplicate check was skipped on large documents (it hit
  its output limit, and once got HTTP 400). Those may keep a few duplicate
  requirements.

**To do:** re-run the analysis for 47881 and the 18 empty regulations once OpenRouter
is reachable, with the API restarted so it loads the fix.
