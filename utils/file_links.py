"""Where a document's files live. One rule, one place, every regulator.

THE RULE (lead, 2026-08-24)

    exactly one file   ->  document_url,     attachment_links absent
    more than one      ->  attachment_links, document_url empty

It counts FILES. A PAGE IS NOT A FILE: several crawlers used to put the page in
`document_url` and the real file beside it in `extra_meta`, which is why 141
stored rows carried both. Under this rule such a row holds one file, the file
goes in `document_url`, and the page moves to `source_page_url`.

WHY THIS IS NOT DONE PER CRAWLER
--------------------------------
It was, and that is the problem it fixes. Every crawler invented its own spelling
and they drifted:

    org_pdf_link      SAMA 4,957 · MHRSD 57 · CBE 32 · ZATCA 24
    arabic_pdf        CBB 52
    pdf_link          CBB 5
    attachment_links  CMA 118 · MHRSD 57 · MoC 34 · ZATCA 24

A frontend then has to know all four and their precedence. So this runs at the
ORCHESTRATOR, which every document passes through on its way to storage, for the
same reason `changesignal.find_existing` is the single answer to "is this the
same document?" -- a rule with several copies is a rule that disagrees with
itself.

IT IS IDEMPOTENT. A document already in the right shape is returned untouched,
so it is safe on every run and on rows that never had the problem.

CAUTION -- THIS TOUCHES IDENTITY. `document_url` is one of the three identity
fields, and a row with several files identifies on
`extra_meta.attachment_links` instead. So a crawler emitting the OLD shape while
the database holds the NEW one makes every affected document read as `new` and
every stored row as `disappeared`. The stored rows were migrated to match by
scripts/normalise_file_links.py and scripts/sama_pdf_as_document_url.py on
2026-08-24; anything crawled after that arrives here and is normalised on the
way in.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

#: extra_meta keys that have historically held a FILE url. Read so a crawler
#: that still writes its own spelling is folded into the rule rather than
#: silently keeping a second copy. Order matters only for readability; the
#: files are de-duplicated below.
LEGACY_FILE_KEYS = ("org_pdf_link", "arabic_pdf", "pdf_link", "english_pdf")

#: What `attachment_links` is joined with. mc_crawler_wrapper writes " | ",
#: others newlines, a few rows commas. Splitting on one of the three reads a
#: multi-file value as a single very long url, and the row then looks
#: single-file when it is not.
SEPARATORS = ("|", ",", "\n")

#: NOT a file -- the listing or page a document was found on. Never promoted.
PROVENANCE_KEYS = ("found_on", "source_page_url", "mlcu_section_url")


def split_links(value) -> List[str]:
    """The http urls in a joined attachment_links value, order preserved."""
    s = str(value or "")
    for sep in SEPARATORS:
        s = s.replace(sep, "\n")
    out, seen = [], set()
    for part in s.split("\n"):
        part = part.strip()
        if part.lower().startswith("http") and part not in seen:
            seen.add(part)
            out.append(part)
    return out


def explicit_files(doc) -> List[str]:
    """The files a crawler NAMED as files -- attachment_links or a legacy key.

    Separate from `document_url` on purpose. A crawler that filled one of these
    was saying "here are the files"; whatever it left in `document_url` was the
    PAGE it found them on. That is why 141 stored rows carried both, and it is
    the only way to tell a page from a file without fetching either.
    """
    meta = getattr(doc, "extra_meta", None) or {}
    out, seen = [], set()

    def add(u):
        u = str(u or "").strip()
        if u.lower().startswith("http") and u not in seen:
            seen.add(u)
            out.append(u)

    for u in split_links(meta.get("attachment_links")):
        add(u)
    for key in LEGACY_FILE_KEYS:
        add(meta.get(key))
    return out


def files_of(doc) -> List[str]:
    """Every distinct file this document carries, whichever field holds it.

    `document_url` counts as a file ONLY IF it looks like one. That test is
    `dynamic_crawler.formfill.runner._is_doc`, reused rather than re-invented --
    it already knows the three ways a site says "this is a file", including
    Ministry of Commerce's extensionless `regapis?...&op=Download` endpoints,
    which no extension check would catch.

    Without it the rule mis-reads two real shapes in opposite directions:

        SAMA   document_url = /en/node/11105  + org_pdf_link = the pdf
               -> the node is a PAGE, so this document has ONE file
        CBB    document_url = en.pdf          + arabic_pdf   = ar.pdf
               -> both are FILES, so this document has TWO

    An earlier version simply let any explicit key win, which silently dropped
    CBB's English pdf.
    """
    try:
        from dynamic_crawler.formfill.runner import _is_doc
    except Exception:                       # pragma: no cover - import guard
        def _is_doc(u):                     # noqa: D401 - fallback only
            return str(u or "").lower().rsplit("?", 1)[0].endswith(
                (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"))

    out, seen = [], []
    url = str(getattr(doc, "document_url", "") or "").strip()
    if url.lower().startswith("http") and _is_doc(url):
        out.append(url)
        seen.append(url)
    for u in explicit_files(doc):
        if u not in seen:
            seen.append(u)
            out.append(u)
    if not out and url.lower().startswith("http"):
        # Nothing looked like a file and no key named one. The url is all this
        # document has, so it IS the document -- a landing page with the text on
        # it, which is most of CBE's html rows.
        out.append(url)
    return out


def normalise(doc):
    """Put `doc`'s files where the rule says, and return it.

    Mutates in place and returns the same object, so it can be used inline in a
    list comprehension the way `stamp_content_hashes` is.
    """
    meta = dict(getattr(doc, "extra_meta", None) or {})
    files = files_of(doc)
    old_url = str(getattr(doc, "document_url", "") or "").strip()

    # A crawler's own spelling is folded in above; drop the duplicates so the
    # answer lives in exactly one field.
    for key in LEGACY_FILE_KEYS:
        meta.pop(key, None)
    meta.pop("attachment_links", None)

    if len(files) == 1:
        doc.document_url = files[0]
        # The page the file came from is provenance, not a file. Keep it, but
        # only where nothing better is already recorded.
        if old_url and old_url != files[0]:
            if not str(getattr(doc, "source_page_url", "") or "").strip():
                doc.source_page_url = old_url
            else:
                meta.setdefault("found_on", old_url)
    elif len(files) > 1:
        # Several files: document_url has no single right answer, so it is left
        # empty ON PURPOSE and identity moves to the attachment list. See
        # changesignal.DEFAULT_IDENTITY and cma_crawler_wrapper's override.
        doc.document_url = ""
        meta["attachment_links"] = " | ".join(files)
        meta.setdefault("identity_fields",
                        ["doc_path", "extra_meta.attachment_links", "title"])
        if old_url and old_url not in files:
            meta.setdefault("found_on", old_url)
    # len(files) == 0: nothing to place. A row with no file is a real thing --
    # a rulebook SECTION has no url of its own -- and is left exactly as it was.

    doc.extra_meta = meta
    return doc


def _is_file(url: str) -> bool:
    try:
        from dynamic_crawler.formfill.runner import _is_doc
        return bool(_is_doc(url))
    except Exception:                       # pragma: no cover - import guard
        return str(url or "").lower().rsplit("?", 1)[0].endswith(
            (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"))


def doc_path_of(obj) -> List[str]:
    """A document's or a stored row's doc_path as a list: the model holds a
    list, the database JSON text, the workbook an arrow-joined string."""
    v = obj.get("doc_path") if isinstance(obj, dict) else getattr(obj, "doc_path", None)
    if isinstance(v, (list, tuple)):
        return [str(x) for x in v]
    s = str(v or "").strip()
    if s.startswith("["):
        try:
            return [str(x) for x in json.loads(s)]
        except ValueError:
            return []
    return [p.strip() for p in s.split(" > ") if p.strip()] if s else []


def carried_file(doc) -> str:
    """The one file a document carries BESIDE its page, or ''.

    The shape of a law split into article pages: `document_url` is the page
    and the crawler named exactly one file next to it. A document whose
    `document_url` is itself a file has no page to fall back to and is not
    a carrier; a document naming several files is a bundle and is left alone.
    """
    url = str(getattr(doc, "document_url", "") or "").strip()
    if not url.lower().startswith("http") or _is_file(url):
        return ""
    files = explicit_files(doc)
    return files[0] if len(files) == 1 else ""


def _drop_file(doc, url: str) -> None:
    meta = dict(getattr(doc, "extra_meta", None) or {})
    for key in LEGACY_FILE_KEYS:
        if str(meta.get(key) or "").strip() == url:
            meta.pop(key, None)
    links = [u for u in split_links(meta.get("attachment_links")) if u != url]
    if "attachment_links" in meta:
        if links:
            meta["attachment_links"] = " | ".join(links)
        else:
            meta.pop("attachment_links", None)
    doc.extra_meta = meta
    doc.file_type = "HTML" if getattr(doc, "document_html", None) else None


def holders_of(rows) -> Dict[str, List[List[str]]]:
    """file url -> the doc_paths of the stored rows whose `document_url` it
    is: the laws that already carry their file, for `claim_shared_files`."""
    out: Dict[str, List[List[str]]] = {}
    for r in rows or []:
        url = str((r.get("document_url") if isinstance(r, dict)
                   else getattr(r, "document_url", "")) or "").strip()
        if url and _is_file(url):
            out.setdefault(url, []).append(doc_path_of(r))
    return out


def claim_shared_files(docs, holders: Optional[Dict[str, List[List[str]]]] = None) -> int:
    """CHG-026: a file that two or more documents under one hub carry beside
    their pages is the hub's file, not theirs. Returns how many documents
    gave a file up.

    A regulator that publishes a law as one PDF plus a page per article
    shows the PDF on every article page, so the crawl brought back one
    document per article each carrying the whole-law file, and the law's
    own page (the hub) carried none; the pipeline then fetched and
    extracted that file once per article. Measured 2026-11-17: 3,285 SAMA
    rows under 595 laws, a trace of the same in CBE, CMA and CBB.

    Hubs are the documents whose doc_path is a strict prefix of other
    documents' paths; they are taken deepest first, so a chapter claims its
    articles' file and the law above then claims it from its chapters: the
    file ends on the top-most hub under which two or more documents carried
    it. The hub takes the file as its one attachment (the file rule below
    then makes it the hub's `document_url`); the carriers keep their page
    and their text. A hub with one carrier claims nothing; a hub that
    already carries a different file claims nothing; two documents at the
    same path are ambiguous and neither claims; a document whose
    `document_url` is itself the file (no page) is never a carrier; nor is
    a document whose page another document under the hub shares (two
    files listed on one page are two documents, not two articles).

    `holders` (`holders_of` over the stored rows of the run) covers the
    document that arrives alone, with no hub in the run: a discovery job
    that ingests one new article of a law the library already holds. Its
    file is dropped when a stored row above it in the tree already carries
    that file. Runs here, at the orchestrator, for the same reason the file
    rule does: one rule, one place, every regulator.
    scripts/chg026_split_laws.py applies the same rule to rows already
    stored.
    """
    docs = list(docs or [])
    paths = [tuple(doc_path_of(d)) for d in docs]
    # Every document registers under each of its strict prefixes, so a hub's
    # descendants are one lookup: linear in documents times depth, which a
    # 35,000-document crawl (CBB) can afford where a scan of every pair cannot.
    at_path: Dict[tuple, List[int]] = {}
    under: Dict[tuple, List[int]] = {}
    for i, p in enumerate(paths):
        if not p:
            continue
        at_path.setdefault(p, []).append(i)
        for k in range(1, len(p)):
            under.setdefault(p[:k], []).append(i)
    hubs = sorted((p for p in at_path if p in under), key=len, reverse=True)
    changed = 0
    for p in hubs:
        if len(at_path[p]) > 1:
            continue                                    # two documents at one path: whose file? a person's call
        hub = docs[at_path[p][0]]
        below = under[p]
        # A carrier's page must be its own: two documents under the hub with
        # the same page are file links listed on that page (CBE's cheque
        # clearing house: two laws linked from one page), not articles of a
        # law with a page each. They keep their files.
        pages = Counter(str(getattr(docs[j], "document_url", "") or "").strip() for j in below)
        own_page = {j for j in below if pages[str(getattr(docs[j], "document_url", "") or "").strip()] == 1}
        carried = Counter(carried_file(docs[j]) for j in below if j in own_page and carried_file(docs[j]))
        url, n = next(iter(carried.most_common(1)), ("", 0))
        if n < 2:
            continue
        own = explicit_files(hub)
        if own and own != [url]:
            continue                                    # the hub has a file of its own
        if not own:
            hub.extra_meta = dict(getattr(hub, "extra_meta", None) or {})
            hub.extra_meta["attachment_links"] = url
            hub.file_type = "PDF" if url.lower().split("?")[0].endswith(".pdf") else "FILE"
        for j in below:
            if j in own_page and carried_file(docs[j]) == url:
                _drop_file(docs[j], url)
                changed += 1
    if holders:
        for d, p in zip(docs, paths):
            url = carried_file(d)
            if url and any(len(h) < len(p) and tuple(h) == p[:len(h)] for h in holders.get(url, [])):
                _drop_file(d, url)
                changed += 1
    if changed:
        logger.info("shared-file rule: %d document(s) left their file to the hub above", changed)
    return changed


def normalise_all(docs):
    """Apply the rule to every document. Call at the orchestrator, not per crawler."""
    out = [normalise(d) for d in (docs or [])]
    if out:
        multi = sum(1 for d in out
                    if (getattr(d, "extra_meta", None) or {}).get("attachment_links"))
        logger.debug("file rule applied to %d document(s); %d carry several files",
                     len(out), multi)
    return out


__all__ = ["normalise", "normalise_all", "files_of", "split_links", "claim_shared_files",
           "holders_of", "carried_file", "doc_path_of", "LEGACY_FILE_KEYS", "PROVENANCE_KEYS"]
