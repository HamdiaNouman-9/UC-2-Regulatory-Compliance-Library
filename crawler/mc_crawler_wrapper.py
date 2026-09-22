"""Ministry of Commerce — one law, THREE regulatory documents.

WHY THIS EXISTS

MC publishes each law as one page with three collapsible sections: the law
itself, its implementing regulation, and its attachments. The manual library
models those as three separate entries, and the lead asked for the same
(2026-08-13: "i need three folder within each law of law, regulations and
attachment so they all will be three separate entries").

`mode: generic` in config/sources/mc.yml could not produce that. A generic link
walk sees one page per law, so it produced one row whose title came from anchor
text — "2", "Regapis", a zero-width-space "click here" — and put all 72 rows
under a single doc_path. The urls were right and nothing else was.

`generic_crawler/crawler_MISA_MC_ZATCA_v2.py` already splits the page correctly:
it recognises the three containers and writes each one out as its own record and
its own file (`mc/<law-slug>/law.html`, `regulation.html`, `attachment.html`).
Measured 2026-08-14 over 16 laws: 10 with all three parts, 6 with law +
attachments and no separate regulation section.

What was missing was the LAST STEP. That script stops at `pages.xlsx` — a
pages/documents dump, not the regulatory-document columns — so MC could not be
approved into MSSQL the way SAMA and ZATCA can. This class is that step: it
turns those part records into `RegulatoryDocument` objects, so MC goes through
the same NewOrchestrator -> ExcelRepo -> promote path as every other regulator
and approval pushes the stored rows instead of re-crawling.

WHAT MAKES THE THREE ROWS DISTINCT

Nothing in the record itself. All three parts of a law share the same title, the
same `?lawId=` url and the same breadcrumb — the part is recorded ONLY in the
html filename. So identity comes from `doc_path`, whose last crumb is the part:

    Ministry of Commerce > Regulations and Laws > Commercial Register Law > Law
                                                                         > Implementing Regulation
                                                                         > Attachments

`(document_url, doc_path)` is therefore unique per part without widening the
identity tuple — which is what docs/review_fakih_merge_2026-08-11.md (B9) says
not to do.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from models.models import RegulatoryDocument
from crawler.fingerprint import text_of_html
from dynamic_crawler.formfill.runner import (_ext_type, _is_doc, content_key,
                                             stable_url)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
V2_SCRIPT = REPO_ROOT / "generic_crawler" / "crawler_MISA_MC_ZATCA_v2.py"

# The DETAILS page, not default.aspx. details.aspx 302s to the listing and the
# walk proceeds from there; seeding the listing directly also works but this is
# the url the split was measured against.
SEED_URL = "https://mc.gov.sa/en/Regulations/pages/details.aspx"

# Every law lives under /en/Regulations/, and the walk is scoped to it.
#
# PASSED AS A LIST ARGUMENT, NEVER THROUGH A SHELL. Git Bash's MSYS layer
# rewrites a leading-slash argument into a Windows path — measured 2026-08-14,
# `--prefix-root /en/Regulations/` arrived as "C:/Program Files/Git/en/
# Regulations", the seed failed its own prefix test, and the crawl returned 0
# pages while looking like a crawler bug. subprocess with a list bypasses the
# shell entirely, so this cannot recur here.
PREFIX_ROOT = "/en/Regulations/"

PART_ORDER = ("law", "regulation", "attachment")

# THE FOLDER NAMES, and they carry the part on their own.
#
# The law's name is the folder ABOVE these three, so repeating it inside them —
# "Accredited Valuers Law > Attachments", titled "Accredited Valuers Law —
# Attachments" — says it twice and reads worse than the site does. Named by the
# lead 2026-08-14: the law is the folder, and `Laws` / `Regulation` /
# `Attachments` are the folders inside it.
PART_LABEL = {
    "law": "Laws",
    "regulation": "Regulation",
    "attachment": "Attachments",
}

# mc\commercial-register-law\law.html  (the script writes native separators)
_PART_RE = re.compile(
    r"[\\/]?mc[\\/]([^\\/]+)[\\/](law|regulation|attachment)\.html$", re.I)
_HREF_RE = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.I)


# MC stamps every download link with `dt=<DDMMYYYYHHMMSS>` — the moment of the
# crawl — which changed an Attachments row's identity on every run and
# duplicated all 16 of them. The rule now lives in runner.stable_url, shared
# with CMA, which has the same problem with a different parameter name.
_stable = stable_url


class MCLawsCrawler:
    """Adapts the MC three-part crawl to the regulatory-document contract.

    `fetch_documents` is the whole interface the orchestrator needs, which is
    why this is a plain class rather than a subclass of anything.
    """

    def __init__(self,
                 regulator: str = "Ministry of Commerce",
                 source_system: str = "Regulations and Laws",
                 category: str = "Laws and Regulations",
                 out_dir: Optional[str] = None,
                 reuse: bool = True,
                 headless: bool = True,
                 max_pages: int = 300,
                 timeout: int = 3600):
        self.regulator = regulator
        self.source_system = source_system
        self.category = category
        # Default is the directory the 2026-08-14 run already wrote, so a rebuild
        # of the workbook costs nothing. `reuse=False` forces a fresh crawl.
        self.out_dir = Path(out_dir) if out_dir else (
            REPO_ROOT / "output" / "mc_three_part")
        self.reuse = reuse
        self.headless = headless
        self.max_pages = max_pages
        self.timeout = timeout
        logger.info("Initialized MCLawsCrawler (out_dir=%s, reuse=%s)",
                    self.out_dir, self.reuse)

    # ---- the crawl, or the crawl we already have -------------------------- #

    def _pages(self) -> List[dict]:
        pages_json = self.out_dir / "pages.json"
        if self.reuse and pages_json.exists():
            logger.info("MC: reusing %s (no re-crawl)", pages_json)
        else:
            self._crawl()
        if not pages_json.exists():
            raise RuntimeError(f"MC crawl produced no pages.json in {self.out_dir}")
        data = json.loads(pages_json.read_text(encoding="utf-8"))
        return data.get("pages") or []

    def _crawl(self) -> None:
        cmd = [sys.executable, "-u", "-B", str(V2_SCRIPT),
               "--url", SEED_URL,
               "--out", str(self.out_dir),
               "--scope", "prefix",
               "--prefix-root", PREFIX_ROOT,
               "--max-pages", str(self.max_pages)]
        if not self.headless:
            cmd.append("--headful")
        logger.info("MC: crawling -> %s", self.out_dir)
        # shell=False (the default) is load-bearing here — see PREFIX_ROOT.
        #
        # encoding/errors ARE load-bearing too, and their absence is why this
        # crawl reported 0 documents every time it ran under the API on
        # 2026-09-17: `text=True` alone decodes the subprocess's stdout/stderr
        # with the LOCALE codec -- cp1252 ("charmap") on this machine -- and
        # mc.gov.sa's Arabic/UTF-8 page content crashes Popen's reader thread
        # with UnicodeDecodeError partway through, which surfaces here as
        # returncode=1 and an empty pages.json, indistinguishable from a real
        # crawl failure. jobs/monitor_jobs.py's own `_run()` documents the
        # identical failure on a CMA sweep and fixes it the same way.
        r = subprocess.run(cmd, cwd=str(REPO_ROOT), timeout=self.timeout,
                           capture_output=True, text=True,
                           encoding="utf-8", errors="replace")
        if r.returncode != 0:
            raise RuntimeError(
                f"MC crawl failed (exit {r.returncode}): {(r.stderr or '')[-800:]}")

    # ---- records -> documents -------------------------------------------- #

    def fetch_documents(self, limit: int = 0, **_) -> List[RegulatoryDocument]:
        docs = []
        for rec in self._pages():
            m = _PART_RE.search(str(rec.get("html_file") or ""))
            if not m:
                # Not a part record: the listing page, and one /advancedsearch/
                # page that answers "401 - Unauthorized". Neither is a document,
                # and filtering on the part pattern drops both without a
                # special case for either.
                continue
            docs.append(self._to_doc(rec, law_slug=m.group(1),
                                     part=m.group(2).lower()))

        # Grouped by law, then law -> regulation -> attachments, so the workbook
        # reads the way the site does rather than in crawl order.
        docs.sort(key=lambda d: (
            (d.extra_meta or {}).get("law_slug", ""),
            PART_ORDER.index((d.extra_meta or {}).get("mc_part", "law"))))

        logger.info("MCLawsCrawler -> %d documents across %d laws",
                    len(docs), len({(d.extra_meta or {}).get("law_slug")
                                    for d in docs}))
        return docs[:limit] if limit else docs

    def _to_doc(self, rec: dict, law_slug: str, part: str) -> RegulatoryDocument:
        html = rec.get("html") or ""
        text = rec.get("text") or ""
        page_url = (rec.get("url") or "").strip()
        law = (rec.get("title") or law_slug.replace("-", " ")).strip()
        label = PART_LABEL[part]

        # THE TITLE IS THE PART, and the law's name is the folder above it. So
        # doc_path ends ON the title, which is how every other source here reads
        # (GOSI: "... | Laws and Regulations | Social Insurance Law | <title>"):
        #
        #   Ministry of Commerce | Regulations and Laws | Commercial Books Law | Laws
        #                                                                     | Regulation
        #                                                                     | Attachments
        #
        # Named by the lead 2026-08-14. Earlier revisions titled these "Commercial
        # Books Law" and "Commercial Books Law — Regulation"; both repeated the
        # law's name, which the folder above already carries.
        title = label

        files = self._files(html)
        extra = {
            # REQUIRED, not decorative: the orchestrator's tier-1b extraction
            # reads extra_meta["content_text"] instead of re-fetching the page,
            # and the versioning path reads it to snapshot previous content.
            "content_text": text,
            "record_kind": "mc_part",
            "mc_part": part,
            "law": law,
            # Sort key above, and the join back to the on-disk part files.
            "law_slug": law_slug,
        }
        # THE FILE RULE (lead, 2026-08-24): exactly one file -> document_url;
        # more than one -> attachment_links with document_url empty. It counts
        # FILES, and a page is not a file. This used to put the PAGE in
        # document_url and the file(s) beside it in attachment_links, so 16 MC
        # rows carried both -- see scripts/normalise_file_links.py, which
        # migrated the stored rows to match this.
        if len(files) == 1:
            document_url = files[0]
        elif len(files) > 1:
            extra["attachment_links"] = " | ".join(files)
            document_url = ""
        else:
            document_url = page_url

        # The page itself is still recorded, as the place the file came from.
        extra.setdefault("found_on", page_url)
        file_type = "HTML"
        # The page's TEXT, not its HTML. Hashing markup makes every CMS deploy —
        # rotated build ids, cache-busting query strings, re-rendered widgets —
        # look like a content change, and each false `modified` costs a version
        # row and an LLM re-analysis. Same rule as generic_crawler/crawler.py:1901.
        # Safe to change here: all 48 stored MC rows have a NULL hash, so no
        # comparison baseline is invalidated by switching.
        content_hash = content_key(text_of_html(html)) if html else None

        if part == "attachment" and files:
            # THE ATTACHMENTS SECTION IS ITS FILES. Its html is a <ul> of links
            # (205 characters on Commercial Register Law) — storing that as the
            # document would store the list, not the documents.
            #
            # Same rule the formfill pipeline applies under `combined`: several
            # files leave document_url EMPTY, because no single one of them names
            # the row and picking the first makes identity depend on the order
            # the site happened to list them. Exactly one file goes in the
            # column where a reader will look for it.
            if len(files) > 1:
                document_url = ""
                extra["identity_fields"] = [
                    "doc_path", "extra_meta.attachment_links", "title"]
            else:
                document_url = files[0]
            file_type = _ext_type(files[0])
            content_hash = content_key(" | ".join(files))

        return RegulatoryDocument(
            regulator=self.regulator,
            source_system=self.source_system,
            category=self.category,
            title=title,
            document_url=document_url,
            source_page_url=page_url,
            file_type=file_type,
            # The part is the LAST crumb, which is what makes the three rows
            # distinct under the default (document_url, doc_path) identity.
            doc_path=[self.regulator, self.source_system, law, label],
            document_html=html or None,
            content_hash=content_hash,
            extra_meta=extra,
        )

    @staticmethod
    def _files(html: str) -> List[str]:
        """Absolute document links in one part's html, in page order.

        MC serves files through a proxy endpoint with no extension —
        `/regapis?...&op=Download&attId=<uuid>` — so extension matching alone
        finds nothing. `_is_doc` recognises the download parameter, which is the
        fix that made these urls visible at all.
        """
        out, seen = [], set()
        for href in _HREF_RE.findall(html or ""):
            u = _stable(href.replace("&amp;", "&").strip())
            if not u.startswith("http") or u in seen:
                continue
            if _is_doc(u):
                seen.add(u)
                out.append(u)
        return out


__all__ = ["MCLawsCrawler"]
