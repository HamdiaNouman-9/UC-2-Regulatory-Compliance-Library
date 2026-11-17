# CHG-026: a law published as one PDF plus a page per article is captured once (started 2026-11-17)

Branch `feature/chg-026-split-law-single-capture`, from `main` at 968f3cf.
The user's words: "for such laws that are divided into articles we only
capture it once", then, after the SAMA-specific version was built and
tested: "we would like to go to a generic approach so that all regulators
follow the same logic whenever experienced with this kind of duplication
case". The rule lives in the code every crawler's documents pass through.

## What was wrong

The SAMA rulebook publishes a law as one page per article and shows the
whole law's PDF link on every one of those pages. The crawler read each
article page, took the PDF it saw as the article's file, and the
orchestrator's file rule (`utils/file_links.py`, 2026-08-24: one file goes
in `document_url`) made that PDF each article row's document link. The
law's own page, stored as a hub row, carried no file at all.

Measured 2026-11-17 on the local copy of the database (2026-09-22 data):

| | |
|---|---|
| SAMA rows | 6,105 |
| rows whose document link is shared with another SAMA row | 4,899 (741 PDFs) |
| hub rows that own a document and have rows beneath them | 909 |
| of which the rows beneath share one PDF | 595 |
| article rows carrying a PDF that siblings carry too | 3,285 |
| Banking Control Law | 1 hub row, 26 article rows, one PDF |
| most shared PDF | 165 rows |

The file itself was never stored 165 times: there is no file table, the
article versions hold only their own page text, and the downloads folder is
empty. The cost is at analysis time. The text decision
(`dynamic_crawler/formfill/textinput.py`) downloads and extracts every file
link of every document, so a 26-article law was downloaded and extracted
26 times per run, and where the extraction did not contain the article's
text the whole law went to the model with the article. Only 4 SAMA rows
have been analysed on the local copy, so almost none of this has been paid
yet; it would have been paid on the first full analysis.

The same measurement for every other regulator: CBE 26 such hubs, CMA 4,
CBB 3, every other regulator 0. The other regulators that share file links
(CBB 1,048 rows, LLOC 268, Ministry of Commerce 56) share them across
folders: one circular filed under seven rulebook volumes, one law under
several topics. That is one document in several places, which the user
allows, not one law stored once per article, and this change leaves it
alone.

## What it does not do

- No change to what a document is or to the identity rule. The law row and
  the article rows stay separate rows with separate versions; only who
  carries the file changes.
- No per-run download cache. After the fix only one row asks for a law's
  PDF, so there is nothing left to cache.
- Nothing for the "one document in several folders" shape.
- No change to any crawler. The SAMA rulebook crawler keeps emitting what
  it sees (the page, and the PDF beside it under `org_pdf_link`); the
  rule reads that shape at the orchestrator.

## Design

1. **The rule** (`utils/file_links.py`, `claim_shared_files`), in the
   module that already owns where a document's files live. A *carrier* is
   a document whose `document_url` is a page and that names exactly one
   file beside it (a legacy key such as `org_pdf_link`, or one
   `attachment_links` entry). *Hubs* are the documents whose `doc_path`
   is a strict prefix of other documents' paths. Hubs are taken deepest
   first; a hub claims the file that two or more documents beneath it
   carry: the file becomes the hub's one attachment, which the file rule
   then makes the hub's `document_url`, and the carriers drop it and keep
   their page and their text. A chapter claims its articles' file first
   and the law above then claims it from its chapters, so the file ends
   on the top-most hub under which two or more documents carried it. A
   hub with one carrier claims nothing; a hub that already names a
   different file claims nothing; two documents at one path are ambiguous
   and neither claims; a document whose `document_url` is itself the file
   (CBB's shape: no page) is never a carrier; nor is a document whose
   page another document under the hub shares (CBE's cheque clearing
   house lists the same two laws on each clearing page: two files on one
   page are two documents, not two articles); a document without a path
   is left alone. A file that two laws under one topic share goes to the
   topic, which is rare and the same answer the stored-row script gives.
2. **The document that arrives alone.** A discovery job (SAMA's daily
   feed ingest, `benchmarks/sama_feed_ingest.py`, hands single documents
   to the orchestrator with no hub in the run) would re-attach the law's
   PDF to a new article. `holders_of` reads the stored rows of the run's
   sources and maps each file to the paths of the rows that carry it as
   `document_url`; a carrier whose file a stored row above it already
   holds gives it up. The first article of a law the library does not
   hold still arrives with the PDF, and the next full crawl re-identifies
   that one row.
3. **Where it runs** (`orchestrator/orchestrator.py`, `_shape_files`,
   called by `run_for_regulator` on every crawl before the file rule).
   The stored rows come from `_stored_for_source`, which already scopes by
   the run's sources and regulator; if they cannot be read the in-run
   rule still applies and the run goes on. One rule, one place, every
   regulator, for the same reason the file rule is there and not in each
   crawler.
4. **The stored rows** (`scripts/chg026_split_laws.py --regulator NAME`,
   dry run by default, `--apply` to write). The same rule over the stored
   rows, keyed the same way, on `doc_path`: a row whose path is above
   other rows' paths is a hub, hubs are taken deepest first, a hub claims
   the PDF that two or more rows beneath it still carry. The category
   tree is not consulted: it and `doc_path` can disagree (the leaf rule
   files a same-named document on a sibling node), and the orchestrator
   sees only paths. On the local copy the two keys gave the same answer
   for every SAMA row (measured: 1,065 PDF rows whose nearest path-prefix
   row is also their tree ancestor, 0 where it is not). The hub row takes the PDF as `document_url` and keeps its page
   in `extra_meta.found_on`; each carrier takes its page
   (`source_page_url`, what the crawler emits for it) as `document_url`,
   takes the listing back from `found_on` as `source_page_url`, and drops
   `found_on`. This reverses, for those rows only, the shuffle of
   2026-08-24 (`scripts/sama_pdf_as_document_url.py`). A hub node that
   holds several rows is ambiguous and is left alone and counted; so is a
   hub that already carries a different file; a carrier with no page of
   its own stays as it is. The older SAMA crawl's sector landing rows
   never claim (the current crawler stores none). Every original value is
   written to `output/backup_chg026_<regulator>_<stamp>.json` before the
   update, and the update is verified by re-reading the rows and
   re-running the plan, which must then find nothing to move.
5. **Identity.** `document_url` is one of the identity fields, so the rows
   and the rule move together: the script and the orchestrator rule are
   one change, as in August. After both, a crawl yields for an article
   (page, doc_path) and the row holds (page, doc_path); for a law (PDF,
   doc_path) on both sides. On the scheduler host the stored-inventory
   change state keys on identity, which moved; the first sweep after the
   fix reconciles and the second is clean. SAMA's daily feed signal
   matches on the node url, which stays reachable from both columns.
6. **The library page.** A hub that owns a document and has children was
   drawn as a folder and its own document was not listed. With the PDF
   on the law's row, that row must be reachable: the category endpoints
   return `child_count` (`apis/pipeline_api.py`,
   `_attach_regulation_counts`, one grouped query), and the page lists a
   hub's own document(s) before its children in the browse grid and the
   tree (`web/app.js`, `ownDocuments`, `ownDocLi`; a failed fetch lists
   nothing and the children still render). The dead-end check and the
   icon choice read `child_count` when present.
7. **The reference-number tiebreak, guarded** (`orchestrator/orchestrator.py`,
   `_by_reference`). When identity misses, the orchestrator has long
   looked for a stored row with the document's reference number, so a
   document republished at a new url is a modification rather than a new
   document plus a disappearance. Every SAMA article page carries its
   law's number, so when a law is restructured and its articles' paths no
   longer match, that tiebreak folded every article into the first stored
   row carrying the number. Found by the live run of step 9: 106 articles
   became 106 versions of one stored article. The tiebreak now matches
   only a row whose title agrees (where both sides have one), and a row
   at most once per run; the rest are new, which is what they are. One
   existing test (`tests/test_identity.py`,
   `test_version_key_comes_from_the_source`) gave its stored row the
   fixture's default title and the document another; it now gives both
   the same title, as a republished document has.

## Steps

1. Measurement (above), 2026-11-17, from the local database copy. Read
   before writing: `utils/file_links.py` (the file rule and its identity
   warning), `scripts/sama_pdf_as_document_url.py` (the August shuffle
   this partly reverses), `dynamic_crawler/changesignal.py`
   (`find_existing`, `_by_files`), `jobs/monitor_jobs.py` (`monitor_sama`
   is a feed sweep plus the discovery ingest; the full crawl is
   `sama_pipeline`), `dynamic_crawler/sama_feed_signal.py` and
   `dynamic_crawler/cli/sweep.py` (what the feed matches on).
2. The SAMA-specific version first, by the user's decision: the rule as a
   hook inside the SAMA rulebook crawler's recursion, a guard in the feed
   ingest, a SAMA-only script. The first draft of the stored-row rule
   found the nearest row above all carriers of a PDF; the dry run showed
   that a regulation often exists twice in the rulebook, once as a
   circular row under "SAMA Circulars" and once split into articles under
   a law, so the two copies had no common row and 3,073 article rows
   under laws such as the Anti-Money Laundering Law were left alone. The
   rule was rewritten to be local to each hub, which is what a crawl can
   see, and the circular copy is left as it is. Applied on the local copy
   (step 5), reviewed (step 7).
3. The generic version, the same day, on the user's word: the hook left
   the crawler and the guard left the feed ingest (both files are as on
   `main`); the rule and the stored-row check went into
   `utils/file_links.py` and the orchestrator's `_shape_files`; the
   script took a `--regulator` argument. Two documents at one path became
   ambiguous on the crawl side too, so the two sides agree.
4. Tests, `tests/test_chg026_split_law.py` (32, three of them CBE's shape:
   two files on one page keep their files in a crawl and in the script,
   a hub carrying a spreadsheet already carries a file): what a carrier is and
   the path shapes; the rule over a crawl (a shared PDF moves to the law
   and the file rule then places it, one carrier is not enough, a
   different file stays, chapters claim first and the law takes over and
   they are pages again, a one-chapter law, a hub naming another file, a
   hub naming the same file, CBB's shape untouched, two documents at one
   path, no path, idempotent); the lone document with and without a
   stored holder; the orchestrator step with a fake repository (the
   stored rows asked for by source and regulator; still shaping when the
   repository fails); the guarded tiebreak (three articles sharing a
   law's number: the same-titled one matches once, the others are new; a
   circular republished at a new url is still found); the script's plan
   (law and articles, the owner's
   provenance, one-chapter law, one carrier, two laws sharing a PDF with
   the landing row silent, a circular copy elsewhere left alone, a hub
   carrying another file, a carrier without a page counted once, a hub
   node with two rows, rows that are neither PDFs nor shared).
5. Dry run and apply on the local copy, 2026-11-17:

   | regulator | rows | law rows taking their PDF | article rows taking their page | left alone |
   |---|---|---|---|---|
   | SAMA | 6,105 | 155 | 3,373 | 2 hubs with two rows at one path, 11 rows beneath them |
   | CBE | 593 | 0 | 0 | |
   | CMA | 1,998 | 0 | 0 | |
   | CBB | 34,859 | 0 | 0 | |

   SAMA's hubs claim at depths 1 to 5 below them (46, 75, 26, 6, 2); the
   largest is Minimum Capital Requirements for Credit Risk with 165
   articles; the hubs were read by title: laws, regulations, rules,
   frameworks. Every other regulator: nothing to do. An earlier version
   of the rule moved six CBE rows (two laws listed on each cheque
   clearing page, and a statistics row carrying a spreadsheet the script
   did not recognise as a file); the user read the rows and ruled them
   two different documents, which is what they are. Those rows were put
   back from their backup, the rule gained the own-page condition on both
   sides and the script the orchestrator's file test, and SAMA was put
   back to its pre-fix state and re-applied under the corrected rule
   (one row fewer than the first apply, a hub whose listing page is
   shared). Backup
   `output/backup_chg026_Saudi_Arabian_Monetary_Authority__SAMA_2026-11-17_1514.json`;
   the apply verified; a second run finds nothing to move.
6. Replay. The crawler's saved output of 2026-09-15
   (`output/sama_rulebook/*.json`, 5,421 documents as the current crawler
   emits them) matched against the stored rows by the pipeline's own
   identity lookup (`changesignal.find_existing`):

   | | matched | not matched |
   |---|---|---|
   | before the fix, documents as the crawler emitted them | 5,411 | 10 |
   | after the fix, SAMA-specific rule (step 2) | 5,403 | 18 |
   | after the fix, generic rule through `_shape_files` (step 3) | 5,407 | 14 |
   | final: own-page condition, rows re-applied, after the live runs of step 9 | 5,411 | 10 |

   The final replay matches exactly what the crawler matched before any
   change: the same 10 misses, all site changes between the snapshots.
   The four hub-and-article pairs are gone because the live runs stored
   the inner hub pages the older crawl lacked.

   The 10 are site changes between the two snapshots and are the same on
   both sides. The generic rule does better than the SAMA-specific one
   because the stored holders let a lone article under a held law drop
   its file where the crawl alone had one carrier. The extra misses
   (8 with the SAMA-specific rule, 4 with the generic one) are
   hub-and-article pairs with one cause:
   the older crawl never stored the inner hub page (for example "Guide To
   Financial Institutions Services" under "Consumer Protection and
   Financial Conduct"), so the script gave the PDF to the topic hub above,
   while a live crawl stores that inner hub and claims there. Each such
   topic row re-identifies once on the next crawl, alongside the inner
   hub row that would have been new anyway.
7. The library page, checked headless against the local API with the
   fixed rows: the Banking Control Law shows one own-document card first
   and 26 article cards, clicking it opens regulation 1213, the tree
   shows the same 27 rows, no page errors. An independent review of the
   SAMA-specific diff found the crawler claiming at depth 1 and above
   while the script claimed at depth 0 as well (depth 0 is a topic box,
   not a sector root), a taken-over chapter losing its file type, the
   listing branch never claiming, an unordered load and counters keyed
   by carrier count; all fixed before the generic version replaced the
   crawler hook. The review of the generic diff stopped on the
   reviewer's usage limit after naming two checks, both done here: the
   hub scan compared every document with every other and is now linear
   in documents times depth (a synthetic 35,350-document crawl of CBB's
   shape, 7 volumes by 50 chapters by 100 articles, takes 1.1 s), and an
   unused helper was removed from the tests. The other questions put to
   the reviewer were checked by hand: a hub that already names the same
   file only strips its carriers; a carrier naming one file under two
   spellings counts once (`explicit_files` de-duplicates); stored paths
   as JSON or arrow-joined both resolve (`doc_path_of`, tested); a hub
   with no url of its own still claims and the file rule gives it the
   file; the orchestrator reads the stored rows once more per run than
   before, one query.
8. Full suite on this branch against `main` (968f3cf), both run on this
   laptop on 2026-11-17:

   | | failed | passed | errors | skipped |
   |---|---|---|---|---|
   | `main` at 968f3cf, throwaway worktree | 19 | 547 | 32 | 1 |
   | this branch, SAMA-specific version (20 tests) | 19 | 567 | 32 | 1 |
   | this branch, generic version (27 tests) | 19 | 574 | 32 | 1 |
   | this branch, with the guarded tiebreak (29 tests) | 19 | 576 | 32 | 1 |
   | this branch, final, with the own-page condition (32 tests) | 19 | 579 | 32 | 1 |

   The same 19 failures and 32 errors by name as `main` (the only
   difference in the name lists is two asyncio log lines); the extra
   passes are the new tests, exactly; the skip, on both, is the stub-site
   file skipping itself inside the full suite. `main` measured
   2026-11-12 for CHG-009 gave 531 passes; today's 547 on the same commit
   is the environment, not the code, which is why the baseline was
   re-measured. Logs `full_suite_main_today.log` and
   `full_suite_026d.log` in the session scratchpad.

   Known limits. A hub's identity rests on two or more rows beneath it
   carrying the file: if the regulator reduces a split law to a single
   article page, the law row and that article row re-identify once (one
   new plus one disappeared pair). Rare, bounded, and visible in the run
   review.
9. Live run, the user's word, 2026-11-17, against the local copy: the
   Laws and Implementing Regulations sector read by the unmodified
   rulebook crawler at its normal pace (984 documents, 780 of them with
   the law's PDF beside the page, 33 minutes) and handed to the
   orchestrator as the SAMA job does. After shaping, 19 documents carry a
   PDF as `document_url`, the sector's 19 laws, which is also the count
   of stored rows in the sector carrying one; the 761 article documents
   gave the law's file up on the live site. Classified: 852 unchanged,
   25 new, 107 modified, 0 disappeared, run trusted. The 25 new are the
   chapter pages of the Implementing Regulation of the Finance Companies
   Control Law, which the regulator has restructured into chapters since
   the copy was taken; the older crawl had stored its 106 articles with
   the law's path and no article segment, so no rule could match them.
   The 107 modified were the finding: the restructured law's row (a real
   change) and its 106 articles, which the reference-number tiebreak had
   matched to one stored article (row 1292) and written as 106 versions
   of it. The row was repaired on the local copy (the 112 versions of
   the run removed, its first version active again, its text restored),
   the tiebreak guarded (design 7), and the sector crawled again, 32
   minutes, 984 documents, 19 laws carrying their PDF after shaping:

   | run | unchanged | new | modified | disappeared | rows with more than one new version |
   |---|---|---|---|---|---|
   | first, tiebreak unguarded | 852 | 25 | 107 | 0 | 1 (row 1292, 106 versions) |
   | second, tiebreak guarded | 877 | 106 | 1 | 0 | 0 |

   The second run's 877 are the first run's 852 plus the 25 chapter
   pages it stored; its 106 new are the restructured law's row and 105
   of its articles, at paths the library did not hold; its 1 modified
   is "Article 106", the one article whose title the guarded tiebreak
   let match row 1292, written as one version. Today's site agrees with
   the fixed rows and the generic rule for every law in the sector that
   the regulator has not restructured.

## Decisions

- Hub-local, not tree-global. The first stored-row rule looked for the
  nearest row above every carrier of a PDF; the rulebook's circular copies
  made that find nothing for most laws. A crawl cannot see the rest of
  the tree while it claims, so the stored-row rule must not either, or
  the two disagree and the next crawl reports a law as new.
- At the orchestrator, not in the crawlers. The SAMA-specific version
  worked and was tested; it would have left the next regulator with the
  same shape to repeat it, and the lone-document case needs the stored
  rows, which a crawler does not have. The rule sits beside the file rule
  it feeds.
- Every hub claims, depth 0 included. A crawl's depth 0 is a topic box on
  the sector page (the SAMA-specific draft skipped it as if it were the
  sector root, and the script then claimed where the crawler would not;
  the review caught it). The only rows left out are the sector landing
  rows of the older combined crawl, which no current crawler stores.
- The law's row takes the PDF as `document_url`, per the file rule of
  2026-08-24 (exactly one file: `document_url`), rather than keeping its
  page there and the PDF as an attachment. One rule, one place.
- Articles take their page as `document_url`. It is what every crawler
  emits for them and what SAMA's feed signal resolves a changed article
  to.
- Ambiguity is left to a person on both sides: a hub node holding two
  rows (stored) and two documents at one path (crawl) claim nothing.
- A carrier must have a page of its own. The first generic rule moved
  six CBE rows that were two laws listed on each cheque clearing page;
  the user read them and ruled them two different documents. The test
  that tells the shapes apart is the page: an article has one of its own,
  a file listed on a page shares that page with the other files listed
  there. It is the same condition on both sides, so rows and rule agree.

## Register row text

**Change (what / where).** `utils/file_links.py`: a shared-file rule
beside the file rule. A document that names one file beside a page of its
own is a carrier; a document whose path is above other documents' paths
is a hub; hubs are taken deepest first and a hub claims the file that two
or more documents beneath it carry, so the file ends on the law's row and
the articles keep their pages; a lone document whose file a stored row
above it already carries gives it up. `orchestrator/orchestrator.py`:
`_shape_files` applies it on every crawl, for every regulator, before the
file rule. `scripts/chg026_split_laws.py`: the same rule over the stored
rows of one regulator, dry run by default, backed up and verified; applied
to SAMA (155 law rows, 3,373 article rows) on the local copy; CBE, CMA and
CBB have nothing to move. `apis/pipeline_api.py`: the category endpoints return a child
count. `web/app.js`: the library page lists a hub's own document before
its children. `orchestrator/orchestrator.py`, `_by_reference`: the
reference-number tiebreak matches only a stored row with the same title,
and a row at most once per run (a restructured SAMA law had folded 106
articles into one row). No crawler changed.

**Justification for Change.** Problem: a regulator that publishes a law
as one PDF plus a page per article shows the PDF on every article page,
so the crawl stored the whole-law file on every article row (SAMA: 3,285
rows under 595 laws; Banking Control Law 26 times) and the law's own row
carried none. The file was not stored 26 times, but at analysis time it
was downloaded and extracted once per article, and where the extraction
did not contain the article the whole law went to the model with every
article. The law's own row was hidden on the library page once it had
article children. Proposed Change: capture a divided law once, at the one
place every crawler's documents pass through, so any regulator with this
shape, now or onboarded later, gets the same answer. One download and one
extraction per law; existing rows corrected in place with a measured,
reversible script so change detection stays stable; the library page
reaches the law's document and its articles from the same node.
