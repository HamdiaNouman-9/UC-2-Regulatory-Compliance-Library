# HHS onboarding: handoff (started 2026-10-13, updated 2026-10-19 evening)

Where the Department of Health and Human Services build stands, what the user
decided and why, and what is left. Read [onboarding_playbook.md](onboarding_playbook.md)
first for the general traps; this file covers only HHS.

**Status:** all ten sections are built and audited. On 2026-10-19 the FR/eCFR
block had lifted and one full export (`hhs.xlsx`, 6,037 rows, `check` OK) has
all ten sections complete again; the artifact (version 56) shows it. What is
not done:
- **The Guidance Portal loses ~220 items per run, silently** (§3.1, "Refused
  detail pages"). hhs.gov's bot protection (Akamai) answers 403 on a different
  ~220 detail pages each run, and the crawler drops those items with no trace
  in the workbook. **Corrected 2026-10-20:** not refusals. HHS unpublished about 221 items
  and published about 221 others between 10-16 and 10-19 (§3.1). The retry
  fix is in anyway. FR/eCFR was blocked again on 10-20, so `hhs.xlsx` stays
  current.
- **Second export done, 2026-10-22 (`hhs_2nd.xlsx`, 10:02–16:22, via
  `scripts/hhs_export.py`):** `check` OK, 6,039 rows, run_history PASS, all
  37 FR/eCFR rows. Against `hhs.xlsx` (10-19): every one of its 6,037 rows
  present (0 lost), 2 new Guidance Portal items (CMS, issued 2012 and 2017,
  newly listed by HHS), and 4 hashes moved, each explained:
  - 3 by the "[PDF…]" label strip (§5), its one-time change: HITECH Act,
    Health IT Legislation ("[PDF – 266 KB]"), the HIPAA Enforcement Rule
    ("[PDF, 811 KB]"); the label is the only difference. The privacy page
    whose label came and went did NOT move -- the fix holds.
  - "Regulatory actions", the Federal Register's live feed of HHS documents
    (10/16 entries -> 10/21): real change, and expected most weeks.
  `hhs_2nd.xlsx` is built with today's code, so it is the one to send; it
  also wrote `~/.cache/hhs_progress/guidance_details.json`, so the
  monitor-mode test (§9 item 7) can run. Leave a day or two first: two full
  exports on consecutive days set off the FR/eCFR block on 10-20.
- **Coverage-gap warnings did not reach the gate until 2026-10-22** (§3.1).
- **Nothing is committed.**

---

## 0. Where things are right now

| Item | State |
|---|---|
| `config/sources/hhs.yml` | all ten sections configured; **Guidance Portal expansion ON** (user, 2026-10-15 evening) |
| `crawler/hhs_crawler.py` | all features below; FR/eCFR block guard in `_get` now SKIPS (2026-10-16) |
| `scripts/hhs_export.py` | NEW 2026-10-16: the HHS export, with a pypdfium2 fallback when PyMuPDF will not load (§6) |
| `output/workbooks/hhs.xlsx` (+ `.fulltext.json`, 4.5 MB) | **CURRENT.** 2026-10-19 09:58–16:40, `python -m tools.workbook export hhs` (PyMuPDF loaded again). All ten sections, 6,037 rows, `check` OK, 37 FR/eCFR rows, no block-page text. Guidance is missing ~223 items refused by Akamai (§3.1). |
| `output/workbooks/hhs_noFR.xlsx` (+ `.fulltext.json`) | good fallback. 2026-10-16, 6,003 rows; lacks the 36 FR/eCFR rows and Regulatory actions. Its guidance holds ~223 items that `hhs.xlsx` does not (and lacks ~221 that `hhs.xlsx` has). |
| `output/workbooks/hhs_9sec.xlsx` (+ `.fulltext.json`) | **BROKEN.** The 2026-10-15 21:00 export stored the FR/eCFR CAPTCHA page as 36 rows. Do not use. |
| `output/workbooks/hhs_guidance.xlsx` | good: Guidance Portal alone, 5,845 rows (2026-10-15); superseded by `hhs_noFR.xlsx` |
| `output/workbooks/hhs.partial.xlsx` | empty, from the 2026-10-16 04:03 run the old guard aborted; ignore |
| `output/workbooks/hhs_old.xlsx` | the 10-row build of 2026-10-13; history only |
| Artifact "United States Library" | https://claude.ai/artifact/1KG8GuN7L1fpewUbxu1GkZ, **version 56** (2026-10-19): HHS 6,037 rows from `hhs.xlsx` (plus NCUA, added the same day), with a dated note on the fixes tab describing the guidance churn and its cause. Earlier data kept in the scratchpad: v47 (6,039 HHS rows, FR included) as `us/*.v47.*`, v54 (HHS from `hhs_noFR`) as `us/*.v54.*`. |
| federalregister.gov / ecfr.gov | **unblocked**: checked 2026-10-19 morning (`check_fr.py`, both land on their home pages); the full export that day read them without a skip. |
| PyMuPDF | **loads again** (2026-10-19, 1.27.2, both shells), though Smart App Control is still in enforce mode and may block it again (§6). `tools.workbook export hhs` works; `scripts/hhs_export.py` remains the fallback. |
| Commit | nothing committed on `feature/usa-regulators`; ask first |

---

## 1. Shape

```
United States of America
  Department of Health and Human Services (HHS)        regulator
    Laws and Regulations                               source_system (landing page <h1>)
      Explore Laws and Regulations                     folder (the block's <h2>)
        <one FOLDER per link in the block>             10 sections; each holds its own page first
```

- **Source:** `https://www.hhs.gov/regulations/index.html`, block "Explore
  Laws and Regulations" (10 links). It is read live every run, so a link HHS
  adds becomes a new section, and one it removes shows as `disappeared`.
- **Every section is a folder,** including the single-page ones (user,
  2026-10-15). Its own page is the first row inside, unless `include_page: false`.
- **`section_path`** is where the row is LISTED on hhs.gov:
  `Home > Laws and Regulations > Explore Laws and Regulations > <section> > …`.
  The landed page's own breadcrumb goes in `extra_meta.page_breadcrumb`.
- **`category`** is `Laws and Regulations`, set explicitly. Left unset, it falls
  back to the last crumb.
- **`doc_path`** comes from the `hhs_trail` list (`_apply_trail`), never from
  `section_path`. A `>` inside a folder name becomes `›`.
- **Replaced:** the 2026-10-12 "Health Care Regulations › HIPAA" build. The
  user removed `federalregister_crawler.py`, its workbook and its change state.

## 2. Rules the user set (every section)

| Rule | Detail |
|---|---|
| The linked page is the last level | One level only, unless the user asks for more (HIPAA's `deep`). A page's own links stay inside its stored body. |
| PDFs at the last level are rows | Every file link on the regulator's host becomes a file row (PDF, DOCX, XLSX, ZIP …). |
| No external links | Off-site links get NO row; they are counted in the warnings. |
| Exceptions | federalregister.gov (pages as HTML; govinfo.gov / public-inspection PDFs as files) and ecfr.gov. Per-section `hosts` add more (healthit.gov, aspr.gov). |
| Never an empty folder | A folder exists only because a row sits in it. Verified on both workbooks: 0 empty folders. Applies to guidance items: no hhs.gov document means no folder. Each run re-derives this, so items that gain or lose a document appear or disappear. |
| Exclude (HIPAA) | FAQs, Other Languages, About Us, Filing a Complaint, Spanish, `/es/`. Matched on the title, every folder above, and the url path, every run. |
| Other languages | Removed from every stored body (`_strip_other_languages`); see §5. |
| Weak file titles | Renamed (`_retitle_files`); see §5. Page titles stay the page's own `<h1>`, even a slogan ("Get it. Check it. Use it."), by the user's choice. |

## 3. The ten sections

| # | Link text (exact; the `expand` key) | Config | Rows, v47 (with FR) | Rows, 2026-10-16 `hhs_noFR` | Rows, 2026-10-19 `hhs` |
|---|---|---|---|---|---|
| 1 | HHS Guidance Portal | `guidance_portal` | 5,845 (5,844 items + the portal page) | 5,845 | 5,843 (≈223 out, 221 in; §3.1) |
| 2 | HHS Regulations Review | `page_tree` | 8 pages + 16 PDFs | 21 (−3 FR) | 24 |
| 3 | Whistleblower complaints on the chemical and surgical mutilation of children | none, folder + page | 1 | 1 | 1 |
| 4 | Policy on unlawful, unconstitutional, or burdensome regulations | `page_links` (flat) | 2 pages + 1 PDF | 3 | 3 |
| 5 | Complaints and Appeals | none | 1 | 1 | 1 |
| 6 | Regulatory actions | none (FR agency page) | 1 | 0 (FR) | 1 |
| 7 | HIPAA Privacy Rule | `page_tree` + `include_page: false`, `card_buttons`, `no_folder`, `deep`, `exclude` | 31 pages + 67 files | 98 | 98 |
| 8 | Human subjects research protections | `page_tree` + `buttons`, `include_page: false`, eCFR `hosts` | 8 pages | 5 (−3 eCFR) | 8 |
| 9 | Health IT Legislation | `page_tree` + `group_level: 3`, `group_rows`, `prose_links` | 12 pages + 2 PDFs | 13 (−1 FR) | 14 |
| 10 | Public Readiness and Emergency Preparedness (PREP) Act | `page_tree` + `prose_links`, `para_titles`, aspr `hosts`, govinfo `file_hosts_extra` | 33 pages + 11 files | 16 (−28 FR) | 44 |
| | **Total** | | **6,039** | **6,003** | **6,037** |

**2026-10-19 (`hhs.xlsx`):** sections 2–10 match v47 row for row (non-guidance
rows that differ by identity are the same intended fixes as below). 37 FR/eCFR
rows as in v47; no row contains "unblock.federalregister", "Request Access",
"Access Denied" or "errors.edgesuite". 6 HTML rows carry new text since v47
(Summary of the HIPAA Privacy Rule, HITAC, Cyber Security Guidance Material,
Regulatory actions, HIPAA and Reproductive Health, The HIPAA Enforcement Rule).
The 4.5 MB sidecar is the FR pages; PDF text is still 0 on every PDF row.

In the 2026-10-16 export every hhs.gov row matched v47 except intended fixes: about 20 HIPAA
Security PDFs lost `?language=es` from their url, and "Download …" / "access"
titles were renamed (§5). The one FR-family row left is a
`public-inspection.federalregister.gov` PDF, which is not blocked.

**Sign-off:**
- **Signed off section by section:** 3, 4, 5, 6.
- **The rest:** after the 2026-10-15 audit (§7), the user said "it is fine
  for now".

### 1. Guidance Portal
- **What is walked:** all 1,139 listing pages, then each item's detail page,
  where "Download the Guidance Document" links the PDF. It uses in-page
  `fetch()` (`_FETCH_JS`), about 1 s per item.
- **Shape:** one folder per item (its title) holding its hhs.gov file.
- **Metadata:** `published_date`, `year` and `department` come from the
  listing. `extra_meta` holds `guidance_status`, `issue_date` and
  `detail_page`.
- **Hash:** `url|title|status|issue_date`.
- **Result (2026-10-15):** 5,844 item rows.
  - **Divisions:** CMS 5,720, FDA 108, OGC 11, a few others.
  - **Status:** 5,765 Final, 79 Proposed.
  - **Shared PDFs:** 378 PDFs are shared by two or more items (772 rows).
  - **No row (~5,546):** mostly FDA and ACF items whose document is on
    fda.gov or acf.gov, plus **745+ detail pages with no document link at
    all**, plus 2 rescinded. **Undecided** (§9).
- **Timing:** crawl about 3 h 20 min (12:33 to about 15:50), then the
  refused-PDF phase about 4 h (to 19:50).
- **Full ten-section run (2026-10-16):** started 10:32; guidance done by about
  13:55 (about 55 items/min); Chrome closed by 14:41; PDF phase to 16:53.
  About 6 h 20 min in all.
- **Full ten-section run (2026-10-19):** started 09:58; guidance done 13:29
  (its progress file had 8,572 items at 13:19); Chrome closed by 14:09; PDF
  phase to 16:40. About 6 h 40 min.
- **Resumable:** `~/.cache/hhs_progress/guidance.jsonl` is kept until success.
  Listing pages are NOT checkpointed, only items.

#### Refused detail pages: ~220 items lost per run, silently (found 2026-10-19)

> **CORRECTION 2026-10-20: the diagnosis below was wrong. HHS changed the
> portal once; nothing was refused.** The retry export `hhs_retry.xlsx`
> (scratchpad `scripts/cmp_retry.py`, `churn_match.py`) shows:
> - It read every listed detail page: no `guidance_refused.json` was left,
>   and `run_history` is PASS with no "coverage gap".
> - It still has 10-19's 221 new items and only 2 of the 223 "lost" ones
>   (5,844 items).
>
> So the lost items are no longer on the listing at all. Their pages answer
> 403 "Access Denied", which Drupal also gives for an UNPUBLISHED node, while
> the PDF stays on the file server. That fits a page being withdrawn, not one
> being refused. The two sets are different documents, not renamed urls
> (2 shared titles, 4 shared PDFs). Between 10-16 and 10-19 HHS unpublished
> about 221 CMS items and published about 221 others; 10-19 and 10-20 agree
> within 2 items. Monitoring would correctly report that as about 221
> withdrawn and 221 new. The retry fix stays: it is harmless, and a real
> refusal would now be retried, kept and reported instead of vanishing. The
> target is NOT the 6,065-item union.

- **Symptom.** The 10-15 and 10-16 runs agreed item for item. `hhs.xlsx`
  (10-19) lacks **223** guidance items both had and has **221** neither had.
  All 444 are CMS, issue dates 2012–2020. Same total ±2, different members.
- **Cause, checked live 2026-10-19 evening** (scratchpad
  `scripts/check_churn.py`, `check_churn2.py`; 15 page loads, one at a time, in
  the crawl profile):
  - 4 missing items: detail page **403 "Access Denied" (Akamai)**; their PDFs
    **200**. HHS has not removed them.
  - 4 new items: detail page 200 with its PDF link, in the same session. So the
    refusal is per page, not per session.
  - The listing is fine: 1,140 pages × 10 (last index 1139). It offers sort
    links but is read unsorted.
- **Why it is silent.** `_expand_guidance` step 2: a refused detail page
  (`_fetch_in_page` returns None) is skipped and never written to the progress
  file; step 3 files it as "detail page unread" in `skipped`, so the item gets
  no row. The `HTTP 403 (Akamai)` warnings exist only in the run log: the
  workbook's `run_history` has none, and `check` passes. The completeness gate
  cannot see it because the count barely moves.
- **Consequence.** Both sets are genuine: the portal holds about 6,060 items
  with an hhs.gov document, and every export misses a different ~220 (about
  4%). Once monitored, each run would propose the dropped ones for withdrawal
  and re-add the returning ones.
- **Fix, MADE 2026-10-20** (`_expand_guidance` only; tested offline with a
  stubbed fetch, scratchpad `test_retry.py`; **not yet run live**):
  1. **Retry (step 2b):** after the main detail pass it waits `retry_pause`
     (120 s), then retries each refused page once, `retry_delay` (3 s) apart,
     at most `retry_max` (500). Five consecutive refusals end the retry, not
     the export.
  2. **Kept across runs (step 2c):** still-unread detail urls go to
     `~/.cache/hhs_progress/guidance_refused.json`, which is NOT deleted on
     success. The next run reads those pages first, while the session is
     fresh, and deletes the file once nothing is unread.
  3. **Reported:** one warning, `coverage gap: HHS Guidance Portal: N of M
     detail page(s) unread (refused, K recovered on retry) …`. "coverage gap"
     is the orchestrator's existing word (`check_run_trustworthy`): the run is
     untrusted, so the missing items cannot be proposed for withdrawal, the
     short count does not become the baseline (QUARANTINED in `run_history`,
     with the reason in its notes), and new and modified rows still ingest.
     **Fixed 2026-10-22, and until then it did NOT reach the gate.** The
     orchestrator reads the CompositeCrawler that `build_regulator_crawler`
     always wraps a config in, and that wrapper forwards "coverage gap"
     warnings only from a source whose `last_result` has a non-empty
     `coverage_gaps` list (the CMA fix). `fetch_documents` now sets one from
     its own gap warnings; before, the warning stopped at the crawler and a
     short run was trusted. Tested by stubbing the crawl: the composite now
     forwards the warning. Not yet seen in a real short run.
  - **Not known until a live run:** whether a page refused mid-walk reads
    after a 2-minute pause. If it does not, items 2 and 3 still hold: nothing
    is lost silently, and the next run starts with those pages.
- **Side finding.** In-browser `fetch()` got the guidance PDFs (200), while the
  orchestrator's plain-`requests` PDF phase is refused on every one. Storing
  PDF text through the browser is therefore possible: a separate decision
  (about 5,900 downloads).

### 2. Regulations Review
`Retrospective Review of Existing Rules` (the page's `<h1>`) holds the page,
EO 14094 / 13563 / 12866 (FR, HTML), and `Regulatory Review Progress (2012 – 2016)`.
That holds 10 bold-paragraph date folders, each with 2 rows (4 hhs.gov HTML pages,
16 PDFs). EO 12866 is FR's 1993 *listing*, stored as HHS links it (user's choice).

### 4. Policy on unlawful…
Flat: the page ("HHS 2025 Deregulatory Agenda"), Press Release (May 13, 2025),
and the Memo PDF.

### 7. HIPAA Privacy Rule
```
HIPAA Privacy Rule                          (hhs.gov/hipaa page NOT stored)
  Parental Access (PDF), Confidentiality of SUD Patient Records   ("I would like info on" = no_folder)
  Security                                  deep: sidenav branch of Security Rule NPRM
    The Security Rule (page) + its 3 PDFs
    Security Rule NPRM / Summary of the Security Rule / Security Guidance (+16 PDFs) / Cyber Security Guidance (+2)
  HIPAA for Individuals                     deep: sidenav
    page; HIPAA & Reproductive Health; Mental Health & SUD; Your Rights Under HIPAA > 8 menu children …
  Conscience and Religious Freedom          deep: sidenav (page + 13 PDFs, Newsroom, Speaker Request)
  Civil Rights                              deep: cards (one folder per learn-more card)
  HIPAA for Professionals / SUD card / Office for Civil Rights / Newsroom   one page each
```
- **Every menu item is a subfolder** holding its one page and that page's
  hhs.gov files (the user's correction). The branch's own page sits in the
  branch folder.
- **One copy per walk:** a page is stored once per section's deep walks. That
  is why Civil Rights' Conscience card is not stored twice.
- **Excluded:** FAQs, the three "Filing a Complaint" links, Other Languages,
  and the link back to /hipaa.

### 8. Human subjects (45 CFR 46)
Eight blue buttons, each a folder holding the page it opens (3 eCFR, 5 hhs.gov).
The main page is not stored. eCFR Part 46 is over 32,767 characters, so the
export writes a `.fulltext.json` sidecar, which must travel with the `.xlsx`.

### 9. Health IT Legislation
- **Folders:** six acts (`<h3>`), each a folder. `group_rows` stores each act's
  own block of the page as a row (`legislation/#macra`), so acts with only
  external links (Cures, MACRA, ACA) still exist.
- **Also stored:** the whole page. HITECH has its PDF and HITAC; HIPAA has 3
  rule pages; FDASIA has its PDF and FR Section 618.

### 10. PREP Act
- **Folders:** `<h2>`, then accordion panel, then item, titled by the whole
  paragraph.
- **No row:** 5 web.archive.org links.

## 4. Code map: `crawler/hhs_crawler.py`

| Piece | What it does |
|---|---|
| `_get` | headed-Chrome page load past interstitials; on `_BLOCK_HOSTS` (FR CAPTCHA) **skips every FR/eCFR url for the rest of the run** with a warning (user, 2026-10-16) |
| `_links` / `_listed_at` | the block's 10 links / the listing breadcrumb |
| `_page_row(…, whole=False)` | one stored page: region → furniture → other languages → tidy → inline colours → text hash. `whole=True` for a pre-cut block (group_rows) |
| `_expand_guidance` | section 1; refused detail pages get one paced retry, then `guidance_refused.json` + a "coverage gap" warning (§3.1); writes `guidance_details.json` and, with `detail_refresh_days` (monitoring), reuses it (§9 item 7) |
| `fetch_documents` | adds a "coverage gap" warning when any section page went unread (FR/eCFR block, Akamai) |
| `_expand_page_tree` | sections 2, 4, 7, 8, 9, 10 |
| `_deep_landed` | HIPAA `deep`: `sidenav` (menu branch, every item a folder) or `cards` |
| `_retitle_files` | after the crawl: weak file titles (not guidance rows) |
| `_apply_trail` / `_doc_from_document_row` | doc_path from `hhs_trail`; guidance metadata |
| `_nolang`, `_strip_other_languages`, `_weak_title` | module helpers (§5) |

**`page_tree` options** (YAML under `expand:`):

| Option | Meaning |
|---|---|
| `flat` | no heading folders (`page_links` = flat page_tree) |
| `group_level: N` | only `<hN>` make folders |
| `group_rows` | with group_level: each heading's block of the page is a row (`#anchor` url) |
| `prose_links` | links inside sentences are rows; such pages are titled by the landed `<h1>` |
| `para_titles` | a short item paragraph (no closing full stop) is the title |
| `buttons` | only `a.usa-button`, each a folder |
| `card_buttons` | "learn more" buttons inside `.usa-card` are items under the card heading |
| `include_page: false` | do not store the section's own page |
| `no_folder: [...]` | heading names that make no folder |
| `deep: {item: sidenav\|cards}` | walk that item's landed page one structure further |
| `exclude: [regex…]` | no row when title, folder or url path matches |
| `hosts` / `file_hosts_extra` | extra internal hosts (pages+files / files only) |

**Always on:** bold-only paragraphs and USWDS accordion headings are folders. A
folder repeating the section name is skipped.

## 5. Cleaning rules (all measured)

- **Furniture:**
  - **hhs.gov:** sidenav and subnav, the "Navigate to:" toggle (selectors run
    BEFORE buttons are unwrapped), `.usa-tooltip__body`, "Back to top", and
    short jump lists.
  - **federalregister.gov chrome:** `#print-disclaimer`, `div.main-title-bar`,
    add-to-folder, `div.content-nav-wrapper`, `div.col-md-3.navigation`,
    `span.header-link`, `#suggestions`, `div.modal`.
  - **Inline `background`/`color`:** stripped (the white-box bug).
- **Two removals that deleted real content, and were reverted:** `[hidden]` (PREP
  accordions) and `div.fr-box-reader-aid` (the EO listing). Diff against the
  live page after every new rule.
- **`_nolang`:** hhs.gov puts `?language=es` on its own ENGLISH links. It is
  dropped from page_tree item urls, deep-walk urls and every file url.
- **`_strip_other_languages`:**
  - **Removed:** links whose whole text is a language name ("en español",
    "中文" …) or that go to `/es/` or `*-spanish.*`, with the "( )" and "|"
    they leave, plus a short (≤300 chars) linked `<p>`/`<li>` saying "in
    Spanish/…".
  - **Kept:** "available in English and Spanish" (no link).
- **`_retitle_files`, file rows only, not guidance:**
  - **Fragments:** a weak title ("access", "paper or electronically" …) takes
    the real title of the same file found elsewhere in the crawl.
  - **"Download X":** becomes "<folder> – X".
  - **Link text:** the original is kept in `extra_meta.hhs_link_text`.
  - **Result:** 10 renamed in the 21:00 run.
- **Titles:** cleaned of ", opens in a new tab", ", links to an external
  website" and "(PDF, 265 KB)"-style labels. "(PDF Presentation)" is kept.
- **Link labels in stored bodies (2026-10-22):** `_strip_link_file_labels`
  removes a trailing "[PDF, 381 KB]"-style label from every link's text in a
  page body.
  - **Why:** hhs.gov served it on and off on two HIPAA pages, which moved
    their hash with nothing changed.
  - **Test:** on full text, 10-19 and 10-20 differed on 2 pages before the
    strip and 0 after.
  - **One-time change:** two links always carry a label and will lose it
    (HITECH "[PDF – 266 KB]", HIPAA "PDF [PDF, 811 KB]").
  - **Kept:** a link whose whole text is the label keeps it.

## 6. Access and fetch

- **www.hhs.gov:** Akamai refuses plain clients. A headed persistent Chrome
  (`~/.cache/hhs_profile`) passes. **One crawl per profile:** a second launch
  dies with "Opening in existing browser session". Probes use their own
  `profile_dir`.
- **federalregister.gov / ecfr.gov, blocked 2026-10-15 about 21:00, LIFTED by
  2026-10-19** (checked that morning; the full export that day read every
  FR/eCFR page). The guard below stays in place in case it returns:
  - **What happens:** every request (headed browser and curl alike) gets a 302
    to `unblock.federalregister.gov`, "IP Access Help", with a CAPTCHA and
    "Request Access". Their stated policy is that programmatic access should
    use the FederalRegister.gov and eCFR APIs.
  - **Likely trigger:** that day's 89-page live audit plus repeated exports.
  - **Damage:** the 21:00 export stored that page as 36 rows and overwrote the
    good `hhs_9sec.xlsx`.
  - **Guard (2026-10-16, user's choice: skip, not abort):** the first time
    `_get` lands on `_BLOCK_HOSTS`, it sets `_fr_blocked`, warns, and from then
    on returns None for every `_FR_HOSTS` url without visiting it. Those rows
    become "unreadable" skips and are listed in the run warnings. Before this,
    the guard raised, which killed the whole export with nothing saved (the
    empty 04:03 `hhs.partial.xlsx`). A blocked export lacks about 36 rows:
    export to a NEW name, never over a good workbook.
  - **To clear it:** run the scratchpad `scripts/open_profile.py` (the crawl
    profile at federalregister.gov); a human completes the CAPTCHA and
    "Request Access". Done 2026-10-16 morning; still blocked that evening, so
    the request is probably reviewed, not instant. `scripts/check_fr.py`
    checks once from the crawl browser; one plain `requests.get` checks the
    IP. Check at most once or twice a day.
  - **Rules from now on:** one export at a time, no repeated live audits of FR,
    and export to a NEW name and compare sizes before replacing a workbook the
    user keeps.
- **Refused-PDF phase (accepted, "run it as is"):** the shared orchestrator
  downloads every file row with plain `requests` and 3 retries, and hhs.gov
  refuses each one. That is about 7 s per file across 4 workers, about 1.75 s
  per file overall: about 10 min for the nine sections and about 4 h for the
  guidance. PDF rows have metadata and no text.
- **Windows Smart App Control blocked PyMuPDF (2026-10-16; loading again on
  2026-10-19, but the policy is still in enforce mode, so it may recur):**
  - **What happens:** "DLL load failed while importing _mupdf: An Application
    Control policy has blocked this file". The orchestrator imports `fitz` at
    load time, so `tools.workbook export` dies before crawling, for EVERY
    regulator on this machine.
  - **Cause:** Smart App Control is in enforce mode
    (`HKLM:\SYSTEM\CurrentControlSet\Control\CI\Policy`
    `VerifiedAndReputablePolicyState = 1`). It blocked PyMuPDF only once on
    2026-10-15 (Claude's Bash shell; PowerShell exports worked all day). After
    Windows Update installed KB5121794, KB5127648 and KB5127694 and restarted
    (05:36 on 2026-10-16), it blocks every load. Evidence: the
    `Microsoft-Windows-CodeIntegrity/Operational` log, events 3033/3077.
  - **Workaround, HHS only (user's choice):** `python scripts/hhs_export.py
    --out output/workbooks/<name>.xlsx`. It imports PyMuPDF when Windows
    allows it, and otherwise puts a pypdfium2 stand-in `fitz` in place for
    that run (pypdfium2 still loads). The first output line says which reader
    is in use. It refuses `--source` and `--form`. Other regulators keep
    `tools.workbook export` and stay blocked until the policy changes.
  - **Effect on data: none for HHS.** hhs.gov refuses the PDF downloads
    (below), so no PDF text is stored with either reader, and PDF hashes are
    `url|title|status|issue_date`. The processing log for 2026-10-16:
    5,927 "SKIP — nothing reached 200 chars (file 0 chars)", 76 html-only.
  - **Real fix:** turn Smart App Control off (Windows Security, App & browser
    control; it cannot be turned back on without reinstalling Windows) or have
    IT allow the PyMuPDF files. It is a security setting: the user decides.

## 7. Audit of 2026-10-15

- **Method:** all 89 stored pages were fetched live and every block compared
  (`scripts/audit_all.py`, with screen-reader labels removed on the live
  side).
- **Clean:** structure as agreed; formatting clean (no leftover clutter,
  inline colours, images, scripts, forms or relative links); no text lost
  beyond deliberate removals.
- **Left as they are ("fine for now"):**
  1. Whistleblower's in-form instructions (6 blocks) are dropped with the form.
  2. OCR Speaker Request is form-only (39 chars).
  3. Duplicates across folders: OCR ×2, Part 2 ×2, Right to Access Memo ×3.
  4. Jump lists left on 4 pages ("Reviews on this page: ACF | …").
  5. The Ninth Amendment FR row is titled "… (PDF Presentation)".
  6. PREP FAQ pages are kept (the exclude list is HIPAA-only).
  7. The Right to Access banner "embed code" table is dropped.
  8. 5 web.archive.org links.

## 8. Artifact pipeline (scratchpad `us/`)

1. **`add_hhs_to_us.py [stems…]`:** replaces every `stem == "hhs"` row.
   - **Default stems:** `hhs_9sec hhs_guidance`. The first one read wins on
     identity, and the shared portal row is dropped once.
   - **For a full export:** run it with that workbook's stem. v48 was built
     with `add_hhs_to_us.py hhs_noFR`; v55/v56 with `add_hhs_to_us.py hhs`.
   - NCUA has its own copy, `add_ncua_to_us.py` (stem `ncua`).
   - `gen_us_page.py` fills its text with `%` formatting: a literal percent
     sign in a note must be written `%%`.
2. **`gen_us_page.py`:** writes `us_library.html`. Row data and stored bodies
   ship as **gzip + base64**, inflated in the browser with
   `DecompressionStream`: 18.9 MB plain, 3.2 MB packed.
3. **`scripts/verify_page.py us`:** checks the byte-identical round trip and
   renders in headless Chromium (no errors, HHS visible).
4. **Publish** to the URL in §0.

`*.bak.py` copies of both scripts sit beside them. If the scratchpad is gone,
read the live artifact (`Artifact read`) and rebuild the data from it.

## 9. What is left, in order

1. ~~Wait for the FR/eCFR block to lift~~: lifted by 2026-10-19.
2. ~~Re-export~~: done, `hhs.xlsx` on 2026-10-19.
3. ~~Verify~~: done with scratchpad `scripts/cmp_full.py` (vs v47 and vs
   `hhs_noFR`).
4. ~~Rebuild the artifact~~: done, v55/v56; the 10-16 note is replaced by the
   10-19 churn note.
5. ~~Guidance refusals~~: retry fix made 2026-10-20 and re-exported as
   `hhs_retry.xlsx` (6,003 rows, finished 18:07). The churn was HHS's own
   change, not refusals (§3.1 correction). `hhs_retry` has no FR/eCFR rows
   (1 against 37; its non-guidance rows equal `hhs_noFR`'s exactly), so
   **FR/eCFR was blocked again on 2026-10-20** and `hhs.xlsx` (10-19) stays
   CURRENT. Still to do: the artifact's 10-19 churn note says "refused";
   reword it to "HHS unpublished about 221 items".
   - **Re-export commands:** `python -m tools.workbook export hhs --out
     output/workbooks/<new name>.xlsx`, or `scripts/hhs_export.py` if PyMuPDF
     is blocked again. Guidance only: scratchpad `scripts/guidance_export.py
     <out.xlsx>`. Nine sections only: comment the portal line out in `hhs.yml`.
6. **Decisions for the user:**
   - Store guidance PDF text through the browser (side finding in §3.1)?
   - Smart App Control: turn it off / ask IT, or keep using `hhs_export.py`.
   - Whether to move FR/eCFR fetching to their APIs (the lasting fix for the
     block).
   - Guidance items with no row (off-site FDA/ACF; the 745+ with no document
     link, sample them first).
   - Move `guidance_export.py` into the repo if the user wants to keep running
     it.
7. ~~Monitoring is stale~~: **done 2026-10-20, not yet run.**
   - **Signal:** still `crawl`, `confirm: false`.
   - **How a monitor run differs:** `monitor_hhs` sets
     `HHS_DETAIL_REFRESH_DAYS=28`. Each run reads the whole guidance listing
     (additions, withdrawals and status/date changes all show there). It reads
     a detail page only for new items, plus the oldest 7/28 of the kept ones
     (`guidance_details.json`, written by every run and export). The nine
     other sections are read in full. About an hour, estimated; the first run
     on a machine with no kept file takes ~4 h.
   - **Short runs are flagged:** an FR/eCFR block or an Akamai-refused section
     page now adds a "coverage gap" warning (`fetch_documents`), so missing
     rows are never proposed for withdrawal.
   - **Slot:** moved to **sat 12:00** (Sunday has no free hour), still
     `enabled: false`. A test run before enabling:
     `HHS_DETAIL_REFRESH_DAYS=28 python -m tools.workbook export hhs --out
     output/workbooks/hhs_monitor_test.xlsx`. This needs the kept file, which
     only exists after an export made with this code: the 10-20 export ran
     before it. Compare the result with the last export.
8. **Commit**, only when the user asks.

## 10. Scratchpad material (temporary; may be gone)

`C:\Users\<user>\AppData\Local\Temp\claude\d--AI-UC02-Latest-UC-2-Regulatory-Compliance-Library\4260a838-ea89-48fe-b2f8-64927f1a875a\scratchpad\`

| Path | Contents |
|---|---|
| `live/` | 89 stored pages fetched live 2026-10-15 (`index.json`: url → file, final) |
| `raw/`, `hipaa/`, `guid/`, `retro/`, `ohrp/`, `ecfr*/` | earlier cached pages |
| `scripts/rows.py` | read a workbook with overflow resolved |
| `scripts/guidance_export.py` | export ONLY the Guidance Portal, fully expanded |
| `scripts/open_profile.py` | open the crawl profile at federalregister.gov for the CAPTCHA |
| `scripts/check_fr.py` | one visit each to FR and eCFR in the crawl profile; prints where they land |
| `scripts/cmp_noFR.py` | `hhs_noFR.xlsx` vs v47: section counts, gone/added rows, FR rows, file types |
| `scripts/run_export.py` | first version of the pypdfium2 launcher; superseded by repo `scripts/hhs_export.py` |
| `us/*.v47.*`, `us/us_html.v47/` | the v47 artifact data, kept before the v48 rebuild |
| `us/*.v48.*`, `us/*.v54.*` (+ `us_html.v48/`, `us_html.v54/`) | artifact data before the NCUA add (v48) and before the `hhs.xlsx` rebuild (v54) |
| `scripts/cmp_full.py` | `hhs.xlsx` vs v47 and vs `hhs_noFR.xlsx`: sections, FR rows, block text, gone/added, PDF text |
| `scripts/check_churn.py`, `check_churn2.py`, `churn_check.json` | the 2026-10-19 live check of the guidance churn (listing page 0, 4 missing + 4 new items, status codes) |
| `scripts/audit_all.py <live>` | structure, content-lost and formatting audit per section |
| `scripts/cmp_sizes.py` | per-page stored size: last build vs new workbook |
| `scripts/test_section.py`, `test_healthit.py` | offline section tests against cached pages |
| `scripts/fetch_live.py <out>` | fetch every stored page live (careful: this load helped trigger the FR block) |
| `scripts/weak_lang.py`, `empty_folders.py`, `guid_stats.py`, `verify_page.py` | checks used on 2026-10-15 |
| `us/` | artifact pipeline (`add_hhs_to_us.py`, `gen_us_page.py`, `us_data.json`, `us_html/`) |
