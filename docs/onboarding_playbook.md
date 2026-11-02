# Onboarding playbook — what the OCC and HIPAA builds cost to learn

Companion to [ONBOARDING.md](ONBOARDING.md), which is the canonical process.
This is the part that process does not tell you: the order to do things in, and
the mistakes that are cheap to repeat. Written 2026-10-12 after onboarding
**OCC** (three rebuilds) and **Health Care Regulations / HIPAA** (wrong source
surveyed first).

Every number here was measured, not estimated.

---

## 0. Before anything: read the manual library

**This is the single highest-value step and it was skipped twice today.**

The manual library is the target. The regulator's website is only the raw
material. Ask for a screenshot of the branch you are about to build, and read
the *shape* off it before you read the site.

What skipping it cost:

| | |
|---|---|
| OCC, build 1 | subtopic as a folder of bare links — 349 rows, 23 with text (7%) |
| OCC, build 2 | the same, every link fetched — 349 rows, 283 with text (81%) |
| OCC, build 3 | **the subtopic page IS the row** — 23 rows, 23 with text (100%) |
| HHS | surveyed `hhs.gov/regulations` for an hour; the manual library held a **Federal Register document** |

Builds 1 and 2 were each a full export, audit and artifact publish. Build 3 was
what the request had said from the start.

### The sentence that keeps being misread

> *"the structure is the main topic and its subtopic htmls/pdfs/docs right under
> it. This is the last level."*

This means the **subtopic items are the last level** — each one a readable page,
one row. It does **not** mean "the subtopic's links are the last level".

If the manual library shows one readable document under a heading, build one
row. The links on that page stay **inside the stored body**, live and absolute,
recorded but not promoted. That is the whole difference between recording a link
and making it a document.

---

## 1. Find the real source

The page the user names is not always the source.

**HHS**: `www.hhs.gov/regulations/index.html` holds 16 links — 12 explainers
about how rulemaking works, 11 regulations.gov **search queries**, 12 links to
other agencies' sites. HHS does not host its own rules. The manual library's
HIPAA branch was `federalregister.gov` document `2024-08503` (89 FR 32976).

Signs you are on a hub rather than a source:

- links are `?search=` / `?agencyIds=` query strings — those are searches, not
  documents, and the library already tags that shape *"search query, not a
  document"* on FEMA
- the page's own text explains a process rather than stating a rule
- most links leave the host

**Prefer an API over a page.** `federalregister.gov` gives `citation`,
`effective_on`, `publication_date` and `cfr_references` as real fields. That
populated `published_date` and `reference_no` properly — better than anything
scraped for OCC. `regulations.gov` has one too.

---

## 2. Access: measure the wall, then stop

Check `skip_hosts` in `config/change_signals.yml` first. The
`saudiexchange.sa` entry is the warning: that Akamai block *was created by our
own repeated probing*, two hours after a clean crawl.

| host | wall | what cleared it |
|---|---|---|
| `dfpi.ca.gov` | Cloudflare managed challenge | headed persistent Chrome, 4.5s |
| `www.hhs.gov` | Akamai 403 (`ak_p`, `errors.edgesuite.net`) | the same, first try |
| `www.occ.gov` | none | plain `requests`, 200 |
| `federalregister.gov` | none | public API |

**Two attempts, then ask.** If a plain GET 403s, ask the user whether the page
opens in *their* browser. If yes it is the client being judged and one headed
attempt is justified; if no, the problem is elsewhere and probing is pure risk.

The headed recipe that works (copy it, never import another regulator's
crawler — see `copy-over-refactor-across-regulators`):

```python
ctx = p.chromium.launch_persistent_context(
    PROFILE, headless=False, channel="chrome",
    args=["--disable-blink-features=AutomationControlled",
          "--no-first-run", "--no-default-browser-check"],
    ignore_default_args=["--enable-automation"])
ctx.add_init_script(STEALTH)      # navigator.webdriver, plugins, languages
```

`headless=False` is the setting that decides it. A warm profile does **not**
rescue a headless run — tested on DFPI.

The Wayback fallback (`dfa.yml` precedent) is worth one try, but archive.org
rate-limits this address: both its availability API and direct snapshot URLs
returned `429 … suspected abusive bot traffic` today.

---

## 3. Survey before you build

Produce counts, not impressions. For OCC this meant: 23 subtopics, 586 links,
253 of them off-site, 196 of those from one page. That survey is what made the
"on-host only" decision possible, and it took one pass over cached HTML.

Cache every fetch to the scratchpad and re-read from disk. Today's surveys were
re-run a dozen times against saved files and cost the hosts nothing.

**Put materially different readings to the user with numbers.** "333 rows vs
586 rows" is a decision they can make in seconds; "should we include external
links?" is not.

---

## 4. Build

Model on the closest existing crawler — `crawler/occ_crawler.py` for a
site-with-sections, `crawler/federalregister_crawler.py` for one long
instrument. Copy the logic; never edit another regulator's crawler.

### Parse with `lxml`, not `html.parser`

`html.parser` **mis-nests real markup**. On `project-reach.html` a well-formed
self-closing `<img … />` came out *containing* 3,037 characters — the whole
article — so removing images decomposed the page. Stored text: 194 characters of
a 3,416-character page. `lxml` gives that image no children, and it is pinned at
`requirements.txt:62`.

**Changing the parser shifts DOM depth.** `lxml` recovers one unclosed `<ul>`
that `html.parser` drops, so every `ul` depth moved by one and a hardcoded
`len(a.find_parents("ul")) != 1` silently returned **3 rows — the topic pages
themselves** — with no error. Measure depth *relative* to a container:

```python
base_depth = min(d for d, _n, _u in links)
want = base_depth + 1 if (len(top) == 1 and top[0] == self.topic) else base_depth
```

Then assert both parsers agree.

### Headings can skip levels

The HIPAA rule goes `h1 "List of Subjects"` → `h3 "45 CFR Part 160"` with no
`h2`. A "children are one level down" rule found none and misfiled both parts
under the previous section. Take the **shallowest level present** instead.

### Not every top-level heading is a section

That same document uses `h1` for the preamble fields (`AGENCY:`, `ACTION:`,
`SUMMARY:`) **and** for `PART 160`, `PART 164`, `Footnotes`. Walking from `h2`
lost the 25,008 characters of amendatory text that *are* the rule. The
discriminator turned out to be the trailing colon, not the absence of children.

### Furniture: match by shape, not by name

Naming blocks individually does not scale. Three passes were needed on OCC:

```python
FURNITURE_HEADINGS  = {...}            # exact names
FURNITURE_PREFIXES  = ("contact", "connect with", "subscribe", ...)
FURNITURE_PATTERNS  = (re.compile(r"^get .+ updates$"),)
FURNITURE_SELECTORS = ("section.occgov-section__share", "div.share-icons")
FURNITURE_URLS      = ("/connect-with-us/subscribe-to-occ-email-updates", ...)
```

- **Normalise both sides.** `_heading_key` strips punctuation, so
  `"visit helpwithmybank.gov"` in the set could never match
  `"visit helpwithmybank gov"` from the page. The entries *without* punctuation
  matched, so the removal looked like it worked while Consumer Protection kept
  all four of its customer-service cards.
- **Prefix beats enumeration.** Five spellings of "contact" still missed
  `Connect with ERSP` and `Contact Project REACh`, because the site names each
  block after the team it reaches.
- **Never remove a container wholesale without censusing it.** `div.usa-summary-box`
  held 12 groups across 23 pages: 11 furniture, and **one** — Dispute
  Resolution's "What's New" — holding two OCC publications found nowhere else.
- **Rank matters on both sibling tests.** A furniture `h2` owns its `h3`
  subsections; a walk that stopped at *any* heading removed the title and left
  the body standing.

### Do not strip `<button>`

USWDS accordions put the section name **inside** a button:

```html
<h3 class="usa-accordion__heading">
  <button class="occgov-accordion__button">
    <span class="occgov-accordion__button--text">Bank-Fintech Arrangements</span>
    <span class="…--icon-container"> … Show </span>
  </button>
</h3>
```

Adding `button` to the tags removed alongside `<script>` emptied every accordion
heading on three pages. Unwrap to the label span and decompose only genuine
controls (`Expand All`, `Collapse All`).

### A row with no body is a defect

Compare against the library, not against nothing:

```
fdic   991 rows   984 with stored text (99%)
qfcl  8034 rows  8004 (100%)     mc 51/51     mhrsd.regs 63/63
occ (build 1)     23 of 349 (7%)   <- the bug
```

Low coverage is only legitimate where the rows are *files* — DFPI 5%, QCB 3%,
MOH/CBJ/ZATCA 0%, because nothing here fetches a PDF for its text.

### `doc_path` must be set even when the trail is empty

If `_apply_trail` only assigns `doc.doc_path` when the trail is non-empty, rows
with no section above them fall through to the inherited builder, which derives
the path from `section_path` — the **site's** trail. Two HIPAA rows landed at:

```
Health Care Regulations | HIPAA | Federal Register |
Health and Human Services Department | <rule title> | Table of Acronyms
```

Six deep, under three folders the library never meant to have. An empty trail is
a real answer: "directly under the source system".

### Keep `section_path` and `doc_path` separate

`section_path` is what the **site** calls the place, recorded verbatim even
where the tree differs — the user's standing rule. The library's trail travels
in a key of its own (`occ_subtopic`, `dfpi_trail`, `fr_section`,
`fdic_heading`). Set `extra_meta["section_path"]` unconditionally in
`_apply_trail`; the wrapper only sets it when `drop_sections` is declared.

---

## 5. Export, check, and prove stability

```
python -m tools.workbook export <name>
python -m tools.workbook check output/workbooks/<name>.xlsx     # verdict OK
```

**`check` is necessary and nowhere near sufficient.** It passed a workbook in
which every accordion heading was empty.

**The second export does not report `unchanged`.** `ExcelRepo.__init__` starts
with empty tables and never loads the prior workbook, so workbook mode reports
everything `new`, always. That is not a defect and not a stability test. Copy
the workbook aside and **diff two consecutive exports** on the identity tuple
and the hash:

```python
key = (document_url, doc_path, title)      # changesignal.DEFAULT_IDENTITY
# expect: same count, 0 only-in-A, 0 only-in-B, 0 content_hash moved
```

That is the step that proves the fingerprint is stable, and it matters most once
rows hash visible text rather than `url|title`.

---

## 6. Audit for what went MISSING

This is the lesson that cost the most. An audit that scans for furniture
phrases is a whitelist — **blind by construction to anything that disappeared.**
Mine passed OCC twice while the user, reading the page, found two real defects.

The check that would have caught both on the first pass:

> fetch each page live, diff its headings against the stored body, and flag
> anything lost that is **not** on a furniture rule.

```
Dispute Resolution   live= 5 stored= 2 dropped= 3  <-- CONTENT HEADING LOST
        LOST: ["What's New"]
```

Add the cheap invariants too, all of which were silent failures at some point
today:

- no empty headings
- no heading reduced to a control word (`Show`, `Expand All`)
- every body opens with its `<h1>`
- no empty tables, no stranded `Show`/`Hide` text
- no relative links, no `<img>`, no `script/style/iframe/form/button`
- no empty containers (they render as framed boxes)
- no body under ~300 characters without a reason

`scratchpad/.../audit_occ2.py` is a working version — 20 checks, structure,
content and formatting.

### Always resolve the sidecar

Excel cells stop at 32,767 characters and **`extra_meta` overflows too**. A
truncated `extra_meta` parses as nothing, which silently reports a row as having
no `section_path`. I reported that false defect **twice**:

```python
from dynamic_crawler.formfill.excel_repo import resolve_overflow
meta = json.loads(resolve_overflow(row["extra_meta"], sidecar))
```

Also: `section_path` is **not a column**. It lives inside `extra_meta`, behind a
long `content_text`. Users will not find it by scrolling the workbook.

---

## 7. Monitoring (ONBOARDING Step 8)

`settings_for` keys on the **pair** `(regulator, source_system)` and a near-miss
falls back silently to the file default. FDIC ran with three of its four source
systems unlisted for exactly that reason. **One entry per source system.**

Measure every option and write down what failed:

| signal | how it failed on OCC |
|---|---|
| `stored-inventory` | HEAD on the pages returns **no ETag and no Last-Modified**; only static PDFs carry them, so a probe would watch the files and be blind to the pages that list them |
| `sitemap` | **passed every test and still unusable** — flat `<urlset>`, 15,283 urls, every one with a `<lastmod>`, 91.4% overlap, over the 80% `min_overlap`. But Financial Technology is listed `lastmod 2024-04-17` while its HTML links 2026 news releases |
| `sama-feed` | no what-changed page; the newsroom is press output |
| `crawl` | 26 requests, ~30s, plain `requests` — chosen |

**A token that lies quietly is worse than one that lies loudly.** CMA's
`Last-Modified` is the current time — 1,134 false changes, answered by
`confirm: true`. A stale token reports *unchanged* while the regulator
publishes, and `confirm` cannot reach it, because confirm only re-checks rows
the signal already flagged.

Ship the scheduler slot `enabled: false` until a human has read the workbook — a
detected change writes through `_crawl_into_db` straight to MSSQL.

Delete the regulator's `output/change_state/crawl/*.json` before enabling a job
if the shape changed during the build; otherwise stale records start a
withdrawal clock on rows that no longer exist.

### `content_hash_basis` is documentary — do not "fix" it

Change-state files read `url+title (identity only — cannot detect an edit)` on
rows whose hash is really over visible text. Eleven crawlers write
`extra_meta["content_hash_basis"]`; `changesignal.observation_for` reads a
different key, `hash_basis`. Nothing branches on it — the only decision is
`if obs.basis == BASIS_FAILED`, which both values fail equally — and on the
`CRAWL_AS_SIGNAL` path `verdict_for` is never called at all. Cosmetic. Leave it.

---

## 8. Claims in config comments

These outlive the session and are read as measurements.

I wrote that OCC hashing every row over visible text was something *"none of the
other US sources can claim"*. The user asked whether previous regulators might
do the same. **They do** — `qfcl` 8004/8034, `mc` 51/51, `mhrsd.regs` 63/63,
`fdic` 99%, `sama` 98%.

Before writing "unlike the others" or "the only one", loop
`output/workbooks/*.xlsx` and count. Prefer the narrower true statement.

---

## The order, condensed

1. Read the manual library screenshot. Settle the shape before the source.
2. Find the real source. Prefer an API.
3. Probe access twice, then ask.
4. Survey from cached HTML; put material forks to the user with counts.
5. Build with `lxml`; relative depths; furniture by shape.
6. Export → `check` → **diff two exports**.
7. Audit for what went missing, resolving the sidecar.
8. One signal per source system, measured; slot disabled.
9. Rebuild the country artifact.
10. Re-read the manual library and compare, before saying it is done.

---

## Files worth copying from

| | |
|---|---|
| `crawler/occ_crawler.py` | a site with sections; furniture by selector/prefix/pattern; parser-independent depth |
| `crawler/federalregister_crawler.py` | one long instrument split at its own headings; API metadata |
| `crawler/dfpi_crawler.py` | the headed-browser recipe for a challenged host |
| `crawler/fdic_crawler.py` | injecting rows a crawl cannot reach; the heading/`section_path` split |
| `config/sources/occ.yml` | a config that records what was measured and rejected |
