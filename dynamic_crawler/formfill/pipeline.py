"""FormfillCrawler — makes a form answer `fetch_documents() -> List[RegulatoryDocument]`.

The orchestrator asks exactly one thing of any crawler (orchestrator.py,
run_for_regulator):

    docs = self.crawler.fetch_documents()      # -> List[RegulatoryDocument]

`crawler/generic_crawler_wrapper.py::GenericSiteCrawler` already answers that for
the generic engine, and it does the whole pages.json -> RegulatoryDocument
mapping. Since formfill emits pages.json in the SAME schema on purpose, the only
thing that differs is which engine produced the file.

So this subclasses it and overrides one method. No mapping logic is copied: if
the pipeline's idea of a document changes, both engines change together.

WHAT A FORM ADDS OVER THE GENERIC WALK
--------------------------------------
The generic wrapper leaves `published_date` as None ("a link walk cannot reliably
read issue dates") and regex-guesses `reference_no` from the row text. A form
DECLARES those fields, and the verify gate measures how often they actually fill
— on SBP: published_date 99.6%, department 90.9%, reference_no 89.1%. This class
prefers the form's extracted values and only falls back to the generic guess.

    from dynamic_crawler.formfill.pipeline import FormfillCrawler
    docs = FormfillCrawler("dynamic_crawler/hints/sbp.circulars.yml",
                           regulator="SBP", source_system="SBP-CIRCULARS").fetch_documents()

Refusing to run an unapproved form is the default. The gate only means something
if the pipeline honours it.
"""

from __future__ import annotations

import copy
import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

try:
    from crawler.generic_crawler_wrapper import (GenericSiteCrawler, _dedupe_keep_order,
                                             _parse_row_date, _split_section_path)
except ImportError as _e:                      # pragma: no cover - environment issue
    # This module is the ONLY part of formfill that needs the generic wrapper —
    # deliberately, since it reuses that file's pages.json -> RegulatoryDocument
    # mapping instead of copying it. If the wrapper has not landed in your branch
    # yet, everything else (inspect / propose / run / verify / approve) still
    # works; only the pipeline adapter is unavailable.
    raise ImportError(
        "dynamic_crawler.formfill.pipeline needs crawler/generic_crawler_wrapper.py, "
        "which is not present in this checkout. Pull the branch that adds it. "
        "The formfill CLI does not depend on it and works without.") from _e

from dynamic_crawler import fingerprint
from dynamic_crawler.formfill.runner import _doc_title, _ext_type, content_key
from dynamic_crawler.formfill.schema import approval_state, load_hints

logger = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]


class FormfillCrawler(GenericSiteCrawler):
    """One approved hints file -> a list of RegulatoryDocument."""

    def __init__(self, hints_path: str | Path, regulator: str, source_system: str,
                 category: Optional[str] = None, out_dir: Optional[str] = None,
                 require_approved: bool = True, fetch_details: Optional[bool] = None,
                 in_process: bool = False, timeout: int = 14400,
                 only_urls: Optional[list] = None, headed: bool = False):
        self.hints_path = str(hints_path)
        self.hints = load_hints(self.hints_path)
        ok, why = approval_state(self.hints)
        if require_approved and not ok:
            raise RuntimeError(
                f"{self.hints_path}: {why}\n"
                "The pipeline will not run a form that has not passed the gate. "
                "Run `formfill verify` then `formfill approve`, or pass "
                "require_approved=False for a deliberate one-off.")
        if not ok:
            logger.warning("running UNAPPROVED form %s (%s)", self.hints_path, why)

        super().__init__(
            seed_url=self.hints["seed_url"],
            regulator=regulator,
            source_system=source_system,
            category=category,
            scope=self.hints.get("scope", "auto"),
            out_dir=out_dir,
            in_process=in_process,
            timeout=timeout,
        )
        self.fetch_details = fetch_details
        # THE MONITORING PATH. A sweep names the urls that actually changed;
        # this narrows PHASE 2 to those, so a re-crawl opens the documents that
        # moved instead of all of them. Phase 1 still walks the listing — that is
        # what finds documents the sweep cannot see, because a stored-inventory
        # probe only knows about rows we already hold.
        #
        # `formfill run --only-urls` already existed; it stopped at the runner
        # and never reached the orchestrator, so a targeted crawl produced no
        # versioned rows and nothing was stored. This carries it through.
        self.only_urls = list(only_urls) if only_urls else None
        # For a live demo: --headed pops a real, visible browser window on
        # THIS machine's desktop instead of running invisibly. Only takes
        # effect on an actual crawl -- reuse_last skips _run_crawl entirely
        # (it reads a stored pages.json instead), so no browser opens then.
        self.headed = headed

    def _run_crawl(self) -> dict:
        """Run the form instead of the generic engine. Everything downstream —
        the mapping, the folder trail, the dedupe — is inherited unchanged."""
        import json

        out = Path(self.out_dir) if self.out_dir else Path(
            tempfile.mkdtemp(prefix="formfill_"))

        if self.in_process:
            from dynamic_crawler.formfill import runner
            runner.run(self.hints, out, fetch_details=self.fetch_details,
                       only_urls=self.only_urls)
        else:
            # Same reason as the parent class: Playwright's sync API refuses to
            # start inside the asyncio/twisted reactor the scheduler installs.
            cmd = [sys.executable, "-m", "dynamic_crawler.formfill", "run",
                   self.hints_path, "--out", str(out)]
            if self.fetch_details is False:
                cmd.append("--no-details")
            if self.headed:
                cmd.insert(3, "--headed")  # a top-level flag, before the "run" subcommand
            if self.only_urls:
                # --only-urls takes a FILE of urls, one per line. Written beside
                # the run's output so a failed run can be re-read to see exactly
                # which documents were targeted.
                tf = out / "_only_urls.txt"
                out.mkdir(parents=True, exist_ok=True)
                tf.write_text("\n".join(self.only_urls), encoding="utf-8")
                cmd += ["--only-urls", str(tf)]
            proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT),
                                  encoding="utf-8", errors="replace", timeout=self.timeout)
            if proc.returncode != 0:
                tail = (proc.stderr or proc.stdout or "").strip().splitlines()
                raise RuntimeError(f"formfill failed for {self.hints_path}: "
                                   f"{tail[-1] if tail else 'no output'}")

        pages_json = out / "pages.json"
        if not pages_json.exists():
            raise RuntimeError(f"formfill produced no pages.json in {out}")
        return json.loads(pages_json.read_text(encoding="utf-8"))

    # ---- a declared row IS a document ---------------------------------------
    #
    # The parent has to GUESS whether a page is a document or just a folder in
    # the site tree, because a link walk cannot know: it uses "has it got 200+
    # characters of prose?" as the test.
    #
    # That guess is wrong for a form, and expensively so. SBP's 4,160-circular
    # inventory is phase 1 only — every row has a title, a URL, a date and a
    # reference number, but no page text yet, and no attached PDF. Under the
    # parent's rule all 4,160 were discarded and fetch_documents() returned ZERO.
    # The most valuable output we have — the complete inventory that drives
    # change detection — vanished at the pipeline boundary.
    #
    # A form does not have to guess. A human declared "this selector is one
    # document entry" and the gate verified it fills. So: a row with a URL is a
    # document.

    # ---- document_url is the FILE; source_page_url is the page ---------------
    #
    # The parent maps a page row to a single document whose `document_url` is the
    # PAGE, and hangs the attached PDF off `extra_meta["org_pdf_link"]`. That
    # conflicts with what the columns mean everywhere else in the schema —
    # `source_page_url` is "the page we found it on", `document_url` is the file —
    # and it produced the same regulation twice: once as an HTML page record and
    # again as a file record for its own PDF.
    #
    # So a page with k attached files becomes k documents, each with its own
    # `document_url` (the file) and its own `content_hash`, all sharing
    # `source_page_url` (the page) and the page's text and HTML. A page with no
    # attachment stays one HTML document, as before.
    #
    # k varies: SAMA circulars run 0–7 attachments per circular.

    def _explode_page(self, r: dict, shape: str) -> List:
        base = self._doc_from_page_row(r, shape)
        if base is None:
            return []
        files = [f for f in (r.get("pdf_docs") or []) if (f.get("href") or "").startswith("http")]
        if not files:
            return [base]                      # the page IS the document

        mode = self.hints.get("attachment_is_document", False)

        if mode == "combined":
            # SDAIA-style: the ROW is one instrument and the files ARE it.
            #
            # "Personal Data Protection Law and The implementing Regulation"
            # attaches three PDFs — the law, its implementing regulation, and the
            # transfer regulation. The existing manual library shows that as ONE
            # entry with three attachments, and this has to match.
            #
            # Distinct from `false`: there the page IS the regulation and the
            # files are annexures that must NOT be analysed (one SBP circular has
            # 40). Here the files are the regulation, so every one is analysed —
            # preprocessing concatenates the text of the whole list.
            #
            # document_url IS LEFT EMPTY. The files are the content and they live
            # in extra_meta["attachment_links"]; no single one of them names the
            # row. An earlier revision put the FIRST file here, which made the
            # row's identity depend on the order the site happened to list them.
            hrefs = [f["href"] for f in files]
            # ONLY a row with SEVERAL files leaves document_url empty. With one
            # attachment there is exactly one url that names the row, so it goes
            # where it belongs and the row keeps the ordinary
            # (document_url, doc_path) identity.
            #
            # Measured 2026-08-12: without this, 54 of MHRSD's 57 attachment rows
            # carried ONE file each and were still emptied — putting 54 rows on
            # the declared-identity path for no reason, and leaving the column a
            # reader would look in blank.
            base.document_url = "" if len(hrefs) > 1 else hrefs[0]
            base.file_type = _ext_type(hrefs[0])
            base.source_page_url = (r.get("url") or "").strip()
            # Every file, so a change to ANY of them changes the row's hash.
            base.content_hash = content_key("|".join(hrefs))
            base.extra_meta["record_kind"] = "combined_attachments"
            base.extra_meta["n_files"] = len(hrefs)
            base.extra_meta["file_titles"] = " | ".join(
                (f.get("text") or "").strip() for f in files)
            # Also in extra_meta, which is ALREADY persisted as JSON on the
            # regulations row. So a multi-file instrument survives into the
            # database today, before any schema change — and the API's analyse
            # path can read the whole list back rather than only document_url.
            base.extra_meta["attachment_links"] = " | ".join(hrefs)

            # IDENTITY, since document_url is empty and the default
            # (document_url, doc_path) would make every card in this folder share
            # ("", doc_path). The folder plus the set of files it carries.
            # `_identity_for` in formfill/orch.py reads this per document, so
            # single- and multi-file sources coexist in one run.
            #
            # Trade: a card gaining or losing a PDF changes its identity, so it
            # reads as one `new` plus one `disappeared` rather than `modified`.
            # Declared only when document_url is actually empty. A single-file
            # row has a perfectly good default identity and must keep it.
            if not base.document_url:
                base.extra_meta["identity_fields"] = [
                    "doc_path", "extra_meta.attachment_links", "title"]
            return [base]

        if not mode:
            # SBP-style: the page is the regulation and these are annexures.
            # Recorded, not promoted — one 2022 circular has 40 of them.
            base.extra_meta["annexures"] = " | ".join(f["href"] for f in files[:40])
            base.extra_meta["n_annexures"] = len(files)
            return [base]

        page_url = (r.get("url") or "").strip()
        page_title = (base.title or "").strip()
        out, used_titles = [], set()
        for f in files:
            d = copy.copy(base)
            d.extra_meta = dict(base.extra_meta or {})
            d.document_url = f["href"]
            d.source_page_url = page_url
            d.file_type = _ext_type(f["href"])
            # The file's own label when it says something; otherwise the page
            # title, suffixed only when one page contributes several files, so two
            # rows never collide on (title, folder).
            label = _doc_title(f.get("text"), page_title, f.get("href") or "")
            if label in used_titles:
                label = f"{page_title} — {f['href'].rsplit('/', 1)[-1]}"
            used_titles.add(label)
            d.title = label
            d.doc_path = self._folder_trail(r.get("section_path") or "", label)
            d.content_hash = content_key(f"{f['href']}|{label}")
            d.extra_meta["record_kind"] = "page_attachment"
            d.extra_meta["page_title"] = page_title
            out.append(d)
        return out

    def _want_pages(self, pages: List[dict]) -> bool:
        return True

    @staticmethod
    def _page_is_document(r: dict) -> bool:
        return bool((r.get("url") or "").strip())

    # ---- the folder trail ----------------------------------------------------

    def _folder_trail(self, section_path: str, title: str) -> List[str]:
        """regulator -> source -> the site's own sections -> this document.

        Identical to the parent's `_doc_path` except for the last element.

        **The document's title is the leaf.**
        `_get_or_create_compliance_category` refuses to reuse a leaf folder that
        already has a regulation attached, and creates a same-named sibling
        instead — correct when the leaf IS the document's node, which is what
        CBB and tree sites produce. On a listing the leaf is a CATEGORY shared
        by many documents, so the rule fires on every document after the first:
        MOE's 136 documents produced 140 folders, including 28 siblings all
        called "Special Education". With the title as the leaf, each category is
        one shared folder and each document gets its own node underneath.

        `source_system` stays in the trail. An earlier version dropped it,
        which was fixing the wrong thing: the level is legitimate — it is what
        separates two sources of one regulator — the problem was only that some
        values are codes that read badly as a folder (`MISA-LAWS`) while others
        read fine (`SAMA RULEBOOK`). Name the source for a human in the source
        YAML and the level costs nothing.

        Consequence worth knowing: whatever `source_system` says becomes a
        folder, so it must not repeat what `section_path.prefix` already says.
        """
        # When the form declares `library`, the runner has ALREADY put the
        # regulator and source_system at the head of every section_path — so
        # prepending them again here would only work by luck: _dedupe_keep_order
        # collapses consecutive duplicates, which saves you when the two configs
        # agree on the exact string and silently doubles the folders when they
        # do not. Trust the form when it speaks.
        head = [] if (self.hints.get("library") or {}) else [self.regulator,
                                                            self.source_system]
        parts = head + _split_section_path(section_path) + [title]
        return _dedupe_keep_order([p for p in parts if p])

    # ---- the fields a form knows and a link walk does not -------------------

    def _doc_from_page_row(self, r: dict, shape: str):
        """Let the form's declared fields win over the parent's regex guesses.

        The parent parses `published_date` and `reference_no` out of the raw row
        text, which is the best it can do without a form. When the form named
        those fields explicitly, that value is the better one — it was written
        against this site and its fill rate was measured before approval.
        """
        doc = super()._doc_from_page_row(r, shape)
        if doc is None:
            return None
        fields = r.get("fields") or {}

        # The site's own status ("In-Force", "Superseded") is the REGULATOR's
        # claim about its document, not our record's state. `status` on the
        # regulation belongs to the monitoring lifecycle (new / modified /
        # unchanged), so the site's value is kept separately rather than fighting
        # over one column.
        site_status = (fields.get("status") or "").strip()
        if site_status:
            doc.extra_meta = dict(doc.extra_meta or {})
            doc.extra_meta["regulator_status"] = site_status

        for target in ("reference_no", "department", "year", "category", "urdu_url"):
            value = (fields.get(target) or "").strip()
            if value:
                setattr(doc, target, value)

        # Dates go through the pipeline's own parser rather than straight in. A
        # form extracts what the page says — "July 31 2026" — but the pipeline
        # dedupes on published_date, so a mix of site formats and ISO would stop
        # two records of the same document matching. If it will not parse, keep
        # whatever the parent worked out rather than overwriting good with bad.
        raw_date = (fields.get("published_date") or "").strip()
        if raw_date:
            doc.published_date = _parse_row_date(raw_date) or doc.published_date or raw_date
        doc.doc_path = self._folder_trail(r.get("section_path") or "", doc.title)
        # `crawler`, `hints` and `form_approved_by` removed: none had a reader,
        # and `hints` wrote an absolute developer path onto every stored row.
        # extra_meta is what a person reads about the DOCUMENT, not a log of how
        # it was fetched.
        return doc

    def _doc_from_document_row(self, d: dict, shape: str):
        doc = super()._doc_from_document_row(d, shape)
        if doc is None:
            return None
        doc.doc_path = self._folder_trail(d.get("section_path") or "", doc.title)
        # A file linked from many pages is still ONE document; keep the evidence
        # of where else it appeared rather than dropping it.
        if d.get("times_linked", 1) > 1:
            doc.extra_meta["times_linked"] = d["times_linked"]
            doc.extra_meta["also_in"] = d.get("also_in", "")
        return doc


def build_formfill_source(cfg: dict):
    """A `mode: formfill` entry in config/sources/<regulator>.yml.

        - name: "Circulars"
          mode: formfill
          hints: dynamic_crawler/hints/sbp.circulars.yml
          source_system: "SBP-CIRCULARS"
          category: "Circulars"
    """
    missing = [k for k in ("hints", "regulator", "source_system") if not cfg.get(k)]
    if missing:
        raise ValueError(f"source '{cfg.get('name')}': missing {missing}")
    return FormfillCrawler(
        hints_path=cfg["hints"],
        regulator=cfg["regulator"],
        source_system=cfg["source_system"],
        category=cfg.get("category"),
        out_dir=cfg.get("out_dir"),
        require_approved=bool(cfg.get("require_approved", True)),
        fetch_details=cfg.get("fetch_details"),
    )


def _fetch_documents_formfill(self, limit=None):
    """Same contract as the parent, different assembly.

    1. every page becomes one document per attached file (or one HTML document
       when it has none), and
    2. a file record is dropped when a page already claimed that URL — otherwise
       the same regulation is inserted and analysed twice, once from the page and
       once from its own PDF. On SAMA circulars that was 701 duplicate records.
    """
    result = self._run_crawl()
    self.last_result = result
    shape = result.get("shape", "generic")
    pages = result.get("pages", []) or []
    files = result.get("documents", []) or []

    out, seen = [], set()

    def _add(doc):
        if not doc:
            return
        key = (doc.document_url, " > ".join(doc.doc_path or []))
        if key not in seen:
            seen.add(key)
            out.append(doc)

    claimed = set()
    combined = self.hints.get("attachment_is_document") == "combined"
    for r in pages:
        for doc in self._explode_page(r, shape):
            # A PAGE ROW can also be a duplicate of something already attached.
            #
            # `claimed` below suppresses files that a page already carries, but a
            # row produced by row_selector arrives here as a PAGE, not a file, so
            # nothing checked it. SIMAH: the seed page carries the Implementing
            # Regulations PDF as its attachment, AND the download block that
            # points at that PDF is itself a row — so the same instrument came
            # out twice, once with the law's text and once as an empty shell with
            # the identical url.
            #
            # Only under `combined`, where the whole point is one row per
            # instrument. Elsewhere two rows sharing a url are legitimately two
            # placements and the de-dupe on (url, doc_path) already handles them.
            #
            # AND ONLY WHEN THE ROW BRINGS NOTHING OF ITS OWN. The SIMAH row this
            # was written for was an EMPTY SHELL — the download block repeating a
            # url the seed page already attached, with no text behind it. A row
            # that carries its own page is a different instrument that merely
            # shares a file, and suppressing it deletes a regulation.
            #
            # Measured on ZATCA 2026-08-14: 8 regulations (Bonded Zones, Income
            # Tax, Customs Brokers, ...) had the footer Glossary as their only
            # attachment, so `combined` put that one shared url in document_url
            # for all 8. Without this test the guard dropped 8 of 34 rows, each
            # with ~11,000 characters of its own html. The footer link is fixed
            # at source (runner.JS_DETAIL now honours `strip` when harvesting
            # links); this keeps the guard from ever making that trade again.
            if (combined and doc.document_url
                    and doc.document_url in claimed
                    and not (doc.document_html or "").strip()):
                continue
            claimed.add(doc.document_url)
            # A `combined` row carries its files in extra_meta and leaves
            # document_url EMPTY, so claiming only document_url claims nothing
            # and every attachment is then emitted AGAIN as its own row.
            #
            # Measured on Ministry of Commerce 2026-08-13: 20 laws plus 150
            # attachment rows = 170, where the whole point of `combined` is 20.
            # The rows were titled from link text — "Board03en", "Glossary",
            # "here." — because a file has no name of its own.
            #
            # This worked before `document_urls` was removed only by accident:
            # document_url then held the FIRST file, so exactly one attachment
            # per row was claimed and the rest already leaked.
            for u in str((doc.extra_meta or {}).get("attachment_links") or "").split("|"):
                u = u.strip()
                if u:
                    claimed.add(u)
            _add(doc)

    dropped = 0
    for d in files:
        if (d.get("doc_url") or "") in claimed:
            dropped += 1                      # a page already carries this file
            continue
        _add(self._doc_from_document_row(d, shape))

    # Both paths above end here, so one pass covers exploded attachments and
    # standalone files alike. Off unless the form asks for it: it costs one
    # request per document.
    probe = None
    if self.hints.get("version_probe"):
        probe = fingerprint.annotate(
            out, workers=self.hints.get("version_probe_workers"))
        logger.info("FormfillCrawler[%s] version probe: %s",
                    self.source_system, probe)
    self.last_version_probe = probe

    logger.info(
        "FormfillCrawler[%s/%s] shape=%s -> %d documents "
        "(%d pages exploded, %d standalone files, %d duplicates dropped)",
        self.regulator, self.source_system, shape, len(out),
        len(pages), len(files) - dropped, dropped)
    return out[:limit] if limit else out


FormfillCrawler.fetch_documents = _fetch_documents_formfill

__all__ = ["FormfillCrawler", "build_formfill_source"]
