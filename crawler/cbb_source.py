"""CBBSource — one CBB crawl mode, wearing the contract `config/sources/*.yml` expects.

WHY THIS FILE EXISTS
--------------------
`CBBCrawlerV2` predates the source-config flow and does not fit it in three ways:

  1. `fetch_documents(mode=...)` takes a MODE, not a `limit`. The composite calls
     `fetch_documents()` with no arguments, so an unwrapped CBBCrawlerV2 would run
     all seven modes as ONE source — one baseline, one gate, one change signal for
     the whole regulator. ONBOARDING's rule is the opposite: several small sources
     beat one large one, because each keeps its own baseline and one section
     breaking stays visible instead of being absorbed into a bigger number.

  2. It exposes NO `source_system` / `source_systems` attribute. Without one,
     `formfill/orch.py::_stored_for_source` logs "exposes no source_system --
     `disappeared` will be empty and the completeness gate is inert" and carries
     on. The export would work and be blind, which is the worst combination.

  3. It hashes with an inline `hashlib.md5(text)` rather than the library's single
     definition. `crawler/fingerprint.py` exists so there is exactly one answer to
     "what is a content_hash"; CBB was a fourth.

This adapter fixes all three WITHOUT touching `cbb_crawler.py`, so the existing
`cbb_monitoring` job keeps behaving exactly as it does today while the new path is
proven alongside it.

MODE 1 AND MODE 5 BOTH WROTE "CBB-Compliance"
---------------------------------------------
MEASURED by reading the code, 2026-08-20: mode 1 is Thomson Reuters *Regulations
and Resolutions*, but `_scrape_resolution` (cbb_crawler.py:196) stamps it
`source_system = "CBB-Compliance"` -- the same value mode 5 uses. It reads as a
copy-paste.

That is fatal to the new flow, which scopes `disappeared` and the completeness
gate on (regulator, source_system): split into two sources under one name, each
one's gate sees the other's stored rows and reports them missing. `change_signals
.yml` records MHRSD hitting exactly this.

Normally renaming a `source_system` is expensive -- it is an identity key, so
stored rows move. MEASURED 2026-08-20: **CBB has 0 rows in this database**, 0
`CBB-*` source_systems and 0 entries in run_history. So the rename costs nothing
today and cannot be done free again. `SOURCE_SYSTEM_OVERRIDE` below applies it.

If this repo is ever pointed at a database that DOES hold CBB rows under
"CBB-Compliance" from mode 1, drop the override rather than migrating the rows --
a wrong-but-consistent name is safer than a rename that splits a document's
history in two.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from crawler.fingerprint import stamp_content_hashes
from models.models import RegulatoryDocument

logger = logging.getLogger(__name__)

#: Regulator name. "Full Name (ACRONYM)" is the library's rule, but CBB's existing
#: crawler already writes the bare form and `cbb_monitoring_crawler.py` agrees, so
#: this matches what is already there rather than inventing a third spelling.
REGULATOR = "Central Bank of Bahrain"

#: Mode 1 is Regulations and Resolutions, not Compliance. See the module docstring.
#: Keyed by mode so the rewrite is visible and reversible in one place.
SOURCE_SYSTEM_OVERRIDE: Dict[str, str] = {
    "1": "CBB-Regulations-and-Resolutions",
}

#: Link texts on cbb.gov.bh/compliance that name a FORMAT, not an instrument.
#: A row titled with one of these has to take its name from the paragraph that
#: holds the link -- see `CBBSource._fetch_compliance`. Kept as an explicit list
#: rather than a heuristic (short text, no digits, ...) because the page also
#: carries genuinely short real titles like "FATF Recommendations", and a
#: heuristic that caught those would rename rows that are already right.
#: MEASURED on the page, both sections: these are every anchor text that names
#: a format or a navigation gesture rather than an instrument. Compared with the
#: list, the anchors that must NOT be rewritten are obvious -- "High-Risk
#: Jurisdictions" has descriptive prose in its paragraph and would be RUINED by
#: taking the paragraph, and "FATF Recommendations" / "Mutual Evaluation Report"
#: / "EDFS - Directive" have no paragraph text at all.
#:
#: Matched after lowercasing and stripping a trailing full stop, because the
#: page writes both "Link" and "link.".
_CM_FORMAT_WORDS = {"english", "arabic", "letter", "guidance paper",
                    "click here", "link", "here"}

#: The folder both compliance sections live in. The site splits Compliance into
#: #aml and #eofi and they were becoming two folders directly under the
#: regulator; the manual library files them together under one "CBB Compliance"
#: heading, with AML and EOFI beneath it.
_CM_COMPLIANCE_FOLDER = "CBB Compliance"

# ---------------------------------------------------------------------------
# MODE 4 IS WALKED HERE, NOT IN cbb_crawler.py
# ---------------------------------------------------------------------------
# MEASURED on output/workbooks/cbb.xlsx, 2026-09-06, 1,095 rows: mode 4's
# doc_path is not the site's tree. The folder level under Bahrain came out as
# 174 flat siblings ("Legal Basis", "Responsibility of the CBB", ...) with no
# regulator crumb, no source crumb and no module crumb, and a depth histogram of
# 1/2/3/4/5/6/8 for what is one uniform tree.
#
# THE CAUSE IS `_prune_path` (cbb_test_crawlers/Aml_crawler_v2.py), applied at
# BUILD time to every node, and it fails two ways here:
#
#   1. `_code_of("CBB Capital Market Regulations")` returns "CBB" -- its regex
#      cannot tell a rule code from an ordinary capitalised word -- so the
#      seeded root is classified as a coded PEER and dropped from every leaf
#      whose code is not CBB.*, which is all of them.
#
#   2. Pruning per node means the "leaf anchor" is whichever node is being
#      built. For an UNCODED folder -- "Legal Basis", "Part A", "Executive
#      Summary", all real headings on this site -- `leaf_code` is "" and the
#      `elif leaf_code and ...` guard fails for every coded segment, so ALL
#      coded ancestors are deleted and the truncation is inherited by every
#      descendant. Traced:
#
#        ['CBB Capital Market Regulations']
#         + 'OFS - Offering of Securities' -> [root, 'OFS - Offering of Securities']
#         + 'OFS-A Introduction'           -> ['OFS - Offering...', 'OFS-A Introduction']
#         + 'Legal Basis'                  -> ['Legal Basis']          <- all ancestors gone
#         + 'OFS-A.1.1'                    -> ['Legal Basis', 'OFS-A.1.1']
#
#      That is 559 of the 1,095 rows -- the majority of the workbook.
#
# `_prune_path` ITSELF IS SOUND, and it is still needed. Volume 7 really does
# chain peers as ancestors (see `_cm_leaf_path`), which is what it was written
# for. What was wrong is only WHERE it ran: at every node instead of once at the
# leaf, and over a seed containing the word "CBB". Both are fixed by calling it
# from one place, at the leaf, with the seed held out -- so mode 4 is walked in
# this file instead of calling `_scrape_capital_market_regulations`.
#
# The other six modes are untouched and still go through CBBCrawlerV2 exactly
# as before.
#
# WHY THE WALK CANNOT BE A POST-PROCESSING STEP: `cbb_crawler.py` keeps only
# `row_type == "R"` leaves, and `_prune_path` has already deleted the folder
# titles before that. MEASURED on the same workbook: rebuilding the ancestry
# from what mode 4 RETURNS recovers 2,659 of 3,042 ancestor slots and cannot
# recover 383 -- including every BDE row and the whole of OFS-A. The labels only
# exist during the walk, so the walk is where this has to be fixed.
#
# ---------------------------------------------------------------------------
# KNOWN GAP: VOLUME 7 IS INCOMPLETE **AND** SUPERSEDED. NOT FIXED HERE.
# ---------------------------------------------------------------------------
# This source under-reports Collective Investment Undertakings, and what it does
# report is the wrong module. The crawl WARNS and continues; it does not repair.
#
# MEASURED 2026-09-06, against `<div id="book-navigation-1">` -- the site's own
# navigation, which is what a person browsing the rulebook sees:
#
#   section                      nav items missing from our tree
#   OFS / Disclosure / MAM       0        <- these four are complete
#   AML                          1        <- "AML-2A: Money Transfers ..." is a
#                                            colon-vs-space difference in a
#                                            FOLDER TITLE, not a missing document
#   Volume 7 CIU                 44
#
# Two separate defects, both in Volume 7 only:
#
#   1. INCOMPLETE. /entiresection/700001 returns ONE of the four parts the nav
#      lists. Part B (58 leaves), Quarterly Updates (14) and Ad hoc
#      Communications (21) have never been crawled. 33 held where 126 exist.
#
#   2. SUPERSEDED. The endpoint is stale at EVERY ancestor level, not merely
#      short:
#
#        /entiresection/700002   "Part A"                          31 BDE-* rules
#        /entiresection/700720   "Classification of Undertakings"  31 BDE-* rules
#        /entiresection/2303025  "CIU ... Module"                  72 CIU-* rules
#
#      The CIU module is a CHILD of Classification of Undertakings in the nav,
#      and NEITHER ancestor's endpoint contains it. So `entiresection` renders a
#      node's subtree as it stood when that node was built, and a module added
#      later is reachable only by walking the nav down to it. The 31 rules this
#      source holds are BDE -- a superseded module, its own pages headed
#      "Superseded Requirements" -- standing where the site publishes CIU.
#      Roughly 49 of our 53 Volume 7 path segments do not appear in the nav.
#
# WHY IT IS NOT REPAIRED HERE. The obvious fix -- recurse the nav and read
# `entiresection` at each nav leaf -- OVER-COLLECTS badly, because a nav leaf's
# endpoint often covers all of its siblings. MEASURED on that attempt: the
# glossary's letter pages [A]..[Z] each returned the SAME 40 entries (26 x 40
# duplicates), and each of the five authorisation Forms returned the same 8.
# Descending is right for the CIU module and wrong for the glossary, and the
# nav alone does not say which is which.
#
# A correct repair almost certainly takes content from each nav-leaf PAGE rather
# than from `entiresection` -- nav for structure, page for content, so nothing
# can be counted twice. That is a bigger change than the structure fix above and
# it MOVES ROWS: Volume 7 goes from 33 to ~165, and its content changes from BDE
# to CIU. It is deliberately left as its own piece of work rather than smuggled
# in beside a structure fix.

#: The Capital Market Regulations index. Its <ul class="marketlist"> is the
#: site's own top level: five rulebook sections plus loose legalaffairs.gov.bh
#: PDFs.
CAPITAL_MARKET_URL = "https://cbben.thomsonreuters.com/cbb-capital-market-regulations"

#: The `category` column, and the crumb below the regulator. Unchanged from
#: `cbb_crawler.py` so this is a structure fix and not also a rename.
CAPITAL_MARKET_CATEGORY = "CBB Capital Market Regulations"


def _cm_entire_url(soup, base: str):
    """The /entiresection/<id> link on a rulebook page, absolute. None if absent."""
    from urllib.parse import urljoin

    href = next((a["href"] for a in soup.find_all("a", href=True)
                 if "entiresection" in a["href"]), None)
    return urljoin(base, href) if href else None


def _cm_sidebar_parts(soup, base: str):
    """The site's OWN book navigation as a nested tree: (title, url, children).

    `<div id="book-navigation-1">` is how a person browses a volume, and it is
    the only statement of a volume's real structure. It is used as a CHECK on
    the entiresection endpoint, and as the walk when that check fails -- see
    `_fetch_capital_market`.

    The nav on a volume's root page carries more than one level, so this parses
    the nesting rather than the top row only. Only the top row is used today --
    to count it against the endpoint -- but the shape is what a repair for the
    KNOWN GAP would need, and parsing it costs nothing extra.
    """
    from urllib.parse import urljoin

    from cbb_test_crawlers.Aml_crawler_v2 import _clean_title

    nav = soup.find(id="book-navigation-1")
    if nav is None:
        return []

    def parse(ul):
        out = []
        for li in ul.find_all("li", recursive=False):
            a = li.find("a")
            if a is None or not a.has_attr("href"):
                continue
            title = _clean_title(a.get_text(strip=True))
            if not title:
                continue
            kids = []
            for sub in li.find_all("ul", recursive=False):
                kids.extend(parse(sub))
            out.append((title, urljoin(base, a["href"]), kids))
        return out

    res = []
    for ul in nav.find_all("ul", recursive=False):
        res.extend(parse(ul))
    return res


def _cm_top_of(viewall):
    """(root <ul>, its root <li> title, number of level-1 child <ul>s)."""
    top = viewall.find("ul", recursive=False) or viewall
    children = [c for c in top.children if getattr(c, "name", None)]
    lis = [c for c in children if c.name == "li"]
    title = _cm_node_title(lis[0]) if lis else ""
    return top, title, len([c for c in children if c.name == "ul"])


def _cm_node_title(li) -> str:
    """The heading a <li> carries, with CBB's doubled code repaired.

    Reuses `_clean_title` rather than restating it: CBB prints the section code
    in its own element inside the <h2> AND again at the start of the title, so
    `get_text` yields "OFS-A OFS-A Introduction". That repair is already written
    and already measured; a second copy of it here is the duplicated-definition
    mistake ONBOARDING section 4 warns about.
    """
    from cbb_test_crawlers.Aml_crawler_v2 import _clean_title

    h2 = li.find("h2")
    raw = h2.get_text(strip=True) if h2 else li.get_text(strip=True)
    return _clean_title(raw)


def _cm_leaf_path(seed: List[str], walked: List[str]) -> List[str]:
    """The final doc_path for one leaf: `seed` verbatim, `walked` repaired.

    TWO SHAPES SHARE THIS TREE, and only one of them needs repair.

    Most of it is faithful. OFS, MAM, AML and Disclosure nest a subsection
    inside its parent and nothing else, so the walked path is already right and
    pruning finds nothing to remove -- MEASURED 2026-09-06: 1,062 of 1,095 rows
    come through unchanged, 3 to 8 segments deep.

    Volume 7 (Collective Investment Undertakings) is not. From "Classification
    of Undertakings" down, each node holds exactly ONE child <ul> and the
    sections chain linearly:

        Classification of Undertakings
          BDE-A.1 Purpose
            BDE-B.1 Definition
              BDE-1.1 General Requirements
                BDE-2.1 General Requirements ...   -> 76 segments deep

    Every BDE-n.1 is a PEER of the others, not a parent. That is the shape
    `_prune_path` was written for, and it is reused here rather than restated:
    ONBOARDING section 4 is explicit that a definition kept in two places is a
    definition that will disagree with itself, and this one has drifted once
    already.

    TWO THINGS THAT MUST BE TRUE FOR THAT REUSE TO BE SAFE, both of which are
    why `_prune_path` damages the path when `cbb_crawler.py` calls it:

    1. IT RUNS ONCE, AT THE LEAF. `_prune_path` anchors on the LAST segment, so
       it is only meaningful when that segment is the actual leaf. Called at
       every node during the walk -- as `_parse_viewall_tree` does -- the anchor
       is whatever folder is being built, and an uncoded one ("Legal Basis")
       has no code at all, so every coded ancestor is dropped and the loss is
       inherited by everything below. That single mistake truncated 559 rows.

    2. NOTHING THAT CANNOT BE A PEER IS PASSED IN. `_code_of` cannot tell a rule
       code from a capitalised word: it reads "CBB Capital Market Regulations"
       as the code "CBB", and would then drop that crumb from every OFS/AML/MAM
       leaf as a foreign peer. The same trap catches the section heading "CBB
       Disclosure Standards".

       So the two seed crumbs AND the section's own root heading (`walked[0]`)
       are held out and re-attached. That is not a special case for those three
       strings -- the root heading is the ancestor of everything the section
       contains, so it is structurally incapable of being a peer of a leaf
       inside it, and pruning can only ever be wrong about it.

    AN UNCODED LEAF IS LEFT ALONE. `_prune_path` keeps uncoded segments and
    matches coded ones against the leaf's code; with no code to match, it drops
    every coded ancestor instead. There is nothing to anchor on, so the faithful
    path is the honest answer. MEASURED: 81 leaves have no code -- the whole of
    CBB Disclosure Standards, which is titled "Article 1", "Article 2", ...

    KNOWN RESIDUAL, 33 OF 1,095 ROWS (Volume 7 only, MEASURED 2026-09-06).
    `_prune_path` identifies a peer by its CODE, so the UNCODED peers in Volume
    7's chain survive as folders: "Executive Summary", "Bahrain Domiciled Expert
    CIUs", and below them "Legal Basis" / "Authorisation Requirements". Those
    rows land 5-11 deep where 6-7 is the norm. Everything coded about them is
    now right, and 1,062 rows are clean.

    IT IS LEFT THAT WAY DELIBERATELY. Volume 7's markup is a linked list, not a
    tree -- every node from "BDE-A.1 Purpose" down holds exactly one child <ul>
    -- so it does not say where a folder ends, and no rule available here can
    tell a real uncoded heading from an uncoded peer. The same "Legal Basis"
    that is a chain peer in Volume 7 is a genuine folder under OFS-A.1 Purpose.
    Separating them would take a threshold on chain length, which is a guess
    that will quietly disagree with the site the first time CBB reflows a
    volume. A person reading the workbook can see it; a heuristic cannot.
    """
    from cbb_test_crawlers.Aml_crawler_v2 import _code_of, _prune_path

    if len(walked) < 2 or not _code_of(walked[-1]):
        return seed + walked
    return seed + walked[:1] + _prune_path(walked[1:])


def _cm_walk(ul, seed: List[str], walked: List[str], out: List[dict]) -> None:
    """Walk one <ul> node of the Capital Market tree.

    Each <ul> is ONE node: a single <li> holding its heading and body, followed
    by a child <ul> per subsection. A <ul> with child <ul>s is a folder; one
    without them is a leaf.

    `walked` accumulates the site's own headings and is repaired only at the
    leaf, by `_cm_leaf_path`. Depth varies with the site's real shape --
    OFS-B.1.1 sits three levels under its module and OFS-A.1.1 four, because
    OFS-A.1 has the named sub-heading "Legal Basis" and OFS-B.1 does not. That
    variation is the site. What must not vary is the root, and `seed` is passed
    through untouched so it cannot.
    """
    children = [c for c in ul.children if getattr(c, "name", None)]
    lis = [c for c in children if c.name == "li"]
    if not lis:
        return
    li = lis[0]

    title = _cm_node_title(li)
    if not title:
        return

    child_uls = [c for c in children if c.name == "ul"]
    here = walked + [title]

    if child_uls:
        for child in child_uls:
            _cm_walk(child, seed, here, out)
        return

    # Leaf. `_extract_li_content` strips the <h2> and CBB's empty red
    # paragraphs, so content_text is the rule itself.
    from cbb_test_crawlers.Aml_crawler_v2 import _extract_li_content

    content_html, content_text = _extract_li_content(li)
    out.append({"title": title, "path": _cm_leaf_path(seed, here),
                "content_html": content_html, "content_text": content_text})


class CBBSource:
    """One mode of `CBBCrawlerV2`, as a source the config flow can build.

    Declared in `config/sources/cbb.yml` with `mode: custom`, so
    `build_source` imports and instantiates it per source with `init_kwargs`.
    """

    def __init__(
        self,
        mode: str,
        source_system: str,
        regulator: str = REGULATOR,
        max_volumes: Optional[int] = None,
        volume: Optional[str] = None,
    ):
        if not mode:
            raise ValueError("CBBSource needs a mode ('1', '2a', ... '5')")
        self.mode = str(mode)
        # Mode 2c ONLY. Names ONE rulebook volume, so each volume is its own
        # source with its own baseline and gate. See `_fetch_rulebook_volume`.
        self.volume = volume
        if volume and self.mode != "2c":
            raise ValueError(
                f"CBBSource: `volume` is mode 2c only, got mode {self.mode!r}.")
        # Declared in the YAML rather than discovered, so the completeness gate
        # has an answer BEFORE the crawl runs -- a source that returns nothing
        # still has to be able to say what it would have returned under.
        self.source_system = source_system
        self.regulator = regulator
        # Mode 2c ONLY. The rulebook sidebar is thousands of sequential requests
        # at 1.2s each; uncapped it ran 80 minutes without finishing on
        # 2026-08-20 and the export had to be killed with nothing to show. Set it
        # in config/sources/cbb.yml to prove the flow, then remove it for the
        # real run. Ignored by every other mode.
        self.max_volumes = max_volumes
        self.last_result: dict = {}

    @property
    def source_systems(self) -> List[str]:
        """What this source writes under. Read by CompositeCrawler and by the
        completeness gate; a list because the contract allows several."""
        return [self.source_system]

    def _fetch_capital_market(self) -> List[RegulatoryDocument]:
        """Mode 4, walked here. See the MODE 4 note at the top of this file."""
        import time
        from urllib.parse import urljoin

        from cbb_test_crawlers.Aml_crawler_v2 import (
            BASE_URL, REQUEST_DELAY, _fetch)

        index = _fetch(CAPITAL_MARKET_URL)
        if index is None:
            raise RuntimeError(
                f"CBB mode 4: could not fetch {CAPITAL_MARKET_URL}. That is a "
                f"failed read, not an empty section.")

        marketlist = index.find("ul", class_="marketlist")
        if marketlist is None:
            # The whole source hangs off this one element. If the page is
            # rebuilt without it, every downstream number silently becomes 0.
            raise RuntimeError(
                "CBB mode 4: <ul class='marketlist'> is gone from "
                f"{CAPITAL_MARKET_URL}. The page has been restructured; the "
                f"walk needs rewriting rather than retrying.")

        docs: List[RegulatoryDocument] = []
        base_path = [self.regulator, CAPITAL_MARKET_CATEGORY]

        for anchor in marketlist.find_all("a", href=True):
            href = anchor["href"]
            entry_title = anchor.get_text(strip=True)
            if not entry_title or href.startswith("#"):
                continue

            # ---- COLLECTIVE INVESTMENT UNDERTAKINGS IS NOT CRAWLED HERE ----
            # It is a RULEBOOK VOLUME, and `config/sources/cbb.yml` already
            # crawls it correctly as its own source. This entry links the same
            # volume by a different door -- the marketlist points at
            # /node/700001 and the rulebook sidebar at
            # /rulebook/central-bank-bahrain-volume-7-..., verified 2026-09-10
            # to be the same content -- and the door taken here is broken:
            #
            #   this source, via /entiresection/700001    33 rows, all BDE-*
            #   CBB-Rulebook-Vol-7, via the sidebar      170 rows, all CIU-*
            #
            # The endpoint is stale at every ancestor level and serves the
            # SUPERSEDED "Bahrain Domiciled Expert CIUs" module -- pages headed
            # "Superseded Requirements" -- in place of the Collective
            # Investment Undertakings module the site publishes today. It is
            # also short: one of the four parts the volume's own navigation
            # lists. See the KNOWN GAP note at the top of this file.
            #
            # SKIPPED RATHER THAN REPAIRED, deliberately. Reading it correctly
            # here would produce the right 170 rows under a SECOND
            # source_system, so one instrument would hold two identities and
            # appear twice in the `status = ''` queue. `cbb.yml` section 2
            # records what two sources sharing one document costs. One volume,
            # one owner; the owner is the rulebook.
            # MATCHED ON THE EXACT NODE ID, not a substring. `"700001" in href`
            # also matches CBB Disclosure Standards, whose href is
            # /node/1700001 -- caught on the first run of this skip, which
            # dropped 72 Disclosure rows instead of the 33 CIU ones.
            if (href.rstrip("/").rsplit("/", 1)[-1] == "700001"
                    or "collective investment" in entry_title.lower()):
                logger.info(
                    "CBB mode 4 - skipping %r: it is rulebook Volume 7 and is "
                    "crawled by CBB-Rulebook-Vol-7, which reads the live CIU "
                    "module rather than this endpoint's superseded BDE one.",
                    entry_title)
                continue

            # Loose PDFs on legalaffairs.gov.bh. No tree to walk, and the file
            # IS the document, so the marketlist title is the only name it has.
            if href.startswith("http") and "legalaffairs" in href:
                docs.append(RegulatoryDocument(
                    regulator       = self.regulator,
                    source_system   = self.source_system,
                    category        = CAPITAL_MARKET_CATEGORY,
                    title           = entry_title,
                    document_url    = href,
                    source_page_url = CAPITAL_MARKET_URL,
                    document_html   = f'<a href="{href}">{entry_title}</a>',
                    doc_path        = base_path + [entry_title],
                    extra_meta      = {"content_text": entry_title},
                ))
                continue

            section_url = urljoin(BASE_URL, href)
            section = _fetch(section_url)
            time.sleep(REQUEST_DELAY)
            if section is None:
                raise RuntimeError(
                    f"CBB mode 4: could not fetch section {entry_title!r} at "
                    f"{section_url}. Skipping it would under-report the source "
                    f"and the completeness gate would read that as withdrawal.")

            entire_url = _cm_entire_url(section, BASE_URL)
            if not entire_url:
                raise RuntimeError(
                    f"CBB mode 4: no /entiresection link on {section_url} "
                    f"({entry_title!r}). The whole tree comes from that one "
                    f"endpoint, so there is nothing to fall back to.")

            entire = _fetch(entire_url)
            time.sleep(REQUEST_DELAY)
            if entire is None:
                raise RuntimeError(
                    f"CBB mode 4: could not fetch entiresection for "
                    f"{entry_title!r}.")

            viewall = entire.find("div", id="viewall")
            if viewall is None:
                raise RuntimeError(
                    f"CBB mode 4: no <div id='viewall'> for {entry_title!r}.")

            # The marketlist title is NOT used as a crumb. The tree's own root
            # <li> names the section, and the two disagree: the marketlist says
            # "Collective Investment Undertakings" where the tree says "Central
            # Bank of Bahrain Volume 7-Collective Investment Undertakings".
            # Seeding with the marketlist title would stack both.
            top, _root_title, level1 = _cm_top_of(viewall)

            # DOES THE ENDPOINT RETURN THE WHOLE SECTION? Ask the site.
            #
            # `<div id="book-navigation-1">` is how a person browses the volume,
            # and it states the real top level. MEASURED 2026-09-06: for OFS,
            # MAM, AML and Disclosure it agrees with the endpoint exactly
            # (10/10, 7/7, 15/15, 5/5) and this costs one comparison. For Volume
            # 7 the sidebar lists FOUR parts and /entiresection/700001 returns
            # ONE -- so Part B (58 leaves), Quarterly Updates (14) and Ad hoc
            # Communications (21) were never crawled at all. 33 held where 126
            # exist.
            #
            # That is a COMPLETENESS bug, and it predates the structure fix --
            # the old mode 4 read the same endpoint. It stayed invisible because
            # Volume 7 sits inside this source, so its shortfall is absorbed
            # into a bigger number, which is the exact failure `cbb.yml` argues
            # against for the regulator as a whole.
            #
            # Each part has its own honest endpoint, so when the endpoint is
            # short we walk the parts instead. The check is a comparison rather
            # than a rule about Volume 7, so it catches the next volume CBB
            # reflows without anyone remembering this happened.
            parts = _cm_sidebar_parts(section, BASE_URL)
            leaves: List[dict] = []

            if parts and len(parts) > level1:
                # REPORTED, NOT REPAIRED. See KNOWN GAP at the top of this file.
                logger.warning(
                    "CBB mode 4 - %s: INCOMPLETE. entiresection returns %d "
                    "top-level part(s) where the site's own navigation lists "
                    "%d (%s). This source under-reports that section; see the "
                    "KNOWN GAP note in crawler/cbb_source.py.",
                    entry_title, level1, len(parts),
                    ", ".join(t for t, _, _ in parts))

            _cm_walk(top, base_path, [], leaves)

            if not leaves:
                raise RuntimeError(
                    f"CBB mode 4: section {entry_title!r} yielded no leaves. "
                    f"An empty section is a failed parse, not a result.")

            logger.info("CBB mode 4 - %s: %d leaf document(s)",
                        entry_title, len(leaves))

            for leaf in leaves:
                docs.append(RegulatoryDocument(
                    regulator       = self.regulator,
                    source_system   = self.source_system,
                    category        = CAPITAL_MARKET_CATEGORY,
                    title           = leaf["title"],
                    # DELIBERATELY EMPTY. A rule <li> carries no canonical link
                    # -- MEASURED 2026-09-06, the only <a> tags inside one are
                    # cross-references from its own body text. The previous code
                    # took the first of them, so OFS-A.1.1 was stamped
                    # /node/900112, which is "Article 80" of the law it cites.
                    # document_url is a third of the default identity, so that
                    # is a wrong identity, not just a wrong link. The rule's
                    # real address is its position in the tree, and doc_path now
                    # carries that faithfully.
                    document_url    = "",
                    source_page_url = leaf.get("src_url") or section_url,
                    document_html   = leaf["content_html"],
                    doc_path        = leaf["path"],
                    extra_meta      = {"content_text": leaf["content_text"]},
                ))

        return docs

    def _fetch_rulebook_volume(self) -> List[RegulatoryDocument]:
        """ONE rulebook volume, so a volume is a source rather than a slice.

        WHY THIS EXISTS. `crawl_rulebook_sidebar` walks all eight volumes into
        one flat list, and `cbb.yml` declared them as a single source
        ("CBB-Rulebook"). That is one baseline and one completeness gate for the
        whole rulebook, which is the arrangement section 3 of that config argues
        against for the regulator -- the argument simply was not carried one
        level down. Consequences, all three seen in a 2026-09-07 run:

          * the run is the SUM of eight volumes, so nothing is usable until the
            last one lands, and Volume 1 alone is tens of minutes;
          * a failure anywhere quarantines the single baseline, so seven good
            volumes are lost with the eighth;
          * a volume that silently returns nothing disappears into a bigger
            number instead of being a finding.

        `max_volumes` cannot substitute: it is `volumes[:n]`, a PREFIX, so there
        is no way to express "Volume 5". This selects by name instead, and the
        eight sources in `cbb.yml` each name one.

        The composite stamps `extra_meta.crawl_source` with the source's name
        (generic_crawler_wrapper.py:771), and `orch._docs_by_source` groups on
        it, so each volume gets its own `_history_key`, its own baseline, its own
        verdict in `gate_by_source` -- AND a row even when it returns nothing,
        which is the case the grouping exists to make visible.
        """
        # crawler/, NOT cbb_test_crawlers/. Repointed 2026-09-08 to the rewritten
        # crawler, which carries per-volume checkpointing: its header records the
        # uncapped walk dying mid-"Volume 1" twice, 8+ hours in, killed rather
        # than crashed, losing every volume already walked with it.
        #
        # `_collect_volumes`, `_process`, SIDEBAR_SEED and REQUEST_DELAY are the
        # same names with the same signatures in both modules, so this is the
        # whole change and config/sources/cbb.yml is untouched.
        #
        # THE CHECKPOINT IS NOT REACHED FROM HERE, and that is deliberate.
        # Resume lives in `crawl_rulebook_sidebar`, which walks ALL volumes into
        # one list -- the single-source shape cbb.yml section 3 argues against.
        # This method calls `_collect_volumes` + `_process` directly so each
        # volume stays its own source with its own baseline and gate, and
        # `export --source "vol 1"` already gives the granularity the checkpoint
        # would: a kill costs one volume either way. What the checkpoint would
        # add that `--source` cannot is resume WITHIN a volume, and its own
        # docstring says it does not do that.
        from crawler.cbb_rulebook_crawler import (
            REQUEST_DELAY, SIDEBAR_SEED, _collect_volumes, _process)

        volumes = _collect_volumes(SIDEBAR_SEED)
        if not volumes:
            raise RuntimeError(
                "CBB mode 2c: no volumes found in the sidebar. That is a failed "
                "read of the seed page, not an empty rulebook.")

        wanted = self.volume.strip().lower()
        matches = [v for v in volumes if wanted in v.text.strip().lower()]
        if len(matches) != 1:
            # Naming one volume and getting none or several is a config error
            # that would otherwise crawl the wrong thing under this source's
            # name -- and the gate scopes on that name.
            raise RuntimeError(
                f"CBB mode 2c: volume {self.volume!r} matched "
                f"{len(matches)} of {len(volumes)} volumes. Available: "
                + "; ".join(repr(v.text) for v in volumes))

        vol = matches[0]
        logger.info("CBB mode 2c - walking volume %r", vol.text)

        raw: List = []
        _process(node=vol,
                 # SEEDED WITH THE REGULATOR, unlike `_crawl_rulebook`, which
                 # starts at "CBB Rulebook" and so hangs the whole rulebook off
                 # the country with no regulator crumb -- the same defect mode 4
                 # had. doc_path is [regulator, source, ...folders, title].
                 path=[self.regulator, "CBB Rulebook"],
                 depth=0, visited=set(), results=raw,
                 request_delay=REQUEST_DELAY)

        # LEAVES ONLY -- and this filter has to test `is_folder`.
        # `cbb_crawler._crawl_rulebook` filters on `getattr(d, "row_type", "R")`,
        # but `RulebookDoc` (cbb_rulebook_crawler.py:77) has NO `row_type`
        # field, so the default "R" is returned for every document and the
        # filter passes everything. Its own comment says folders were removed
        # because a folder stored as a regulation "gives a person an entry to
        # open with no instrument behind it"; they were not. MEASURED on the
        # 2026-09-07 run: Common Volume reported "153 docs (56 folders, 97
        # leaves)" and all 153 were kept.
        leaves = [d for d in raw if not getattr(d, "is_folder", False)]
        logger.info("CBB mode 2c - %s: %d leaf document(s) from %d node(s)",
                    vol.text, len(leaves), len(raw))
        if not leaves:
            raise RuntimeError(
                f"CBB mode 2c: volume {vol.text!r} yielded no leaf documents. "
                f"An empty volume is a failed parse, not a result.")

        docs: List[RegulatoryDocument] = []
        for d in leaves:
            meta = dict(getattr(d, "extra_meta", None) or {})
            docs.append(RegulatoryDocument(
                regulator       = self.regulator,
                source_system   = self.source_system,
                category        = d.doc_path[1] if len(d.doc_path) > 1
                                  else "CBB Rulebook",
                title           = d.title,
                document_url    = d.url,
                source_page_url = d.url,
                document_html   = d.document_html,
                doc_path        = d.doc_path,
                extra_meta      = {
                    "pdf_link":     meta.get("pdf_link"),
                    "pdf_links":    meta.get("pdf_links", []),
                    "faq_link":     meta.get("faq_link"),
                    "content_text": d.content_text,
                    "rulebook_volume": vol.text,
                },
            ))
        return docs

    def _fetch_compliance(self) -> List[RegulatoryDocument]:
        """Mode 5, walked here so the TITLE comes from the instrument.

        `cbb_crawler._scrape_compliance_section` titles each row with the link's
        own text. MEASURED on output/workbooks/cbb_others.xlsx, 2026-09-10, 76
        rows: 25 of them are named by a word that identifies a FORMAT, not an
        instrument --

            9  "English" / "Arabic"
            8  "Letter"
            8  "Guidance Paper"

        -- and doc_path ends on that same word, so the library offers
        `Central Bank of Bahrain | AML Compliance | English` and a reviewer
        working the `status = ''` queue cannot tell which law it is. Several
        rows share that trail, so they land on ONE tree node: distinct
        documents stacked at a position labelled "English".

        THIS IS THE FAILURE MODE 3 ALREADY DOCUMENTS. `_scrape_laws_and_regulations`
        carries the note "the links, whose own text is only `English` / `Arabic`,
        so the TITLE MUST COME FROM THE SECTION, not the anchor". That fix was
        applied to mode 3 and never to mode 5, which reads the same CMS on the
        same site.

        WHERE THE NAME ACTUALLY IS. MEASURED on cbb.gov.bh/compliance, both
        sections, every variant link: the containing <p>'s OWN text (the strings
        that are not inside the anchor) is the instrument, and the anchor is the
        qualifier:

            <p>1.Decree Law No. 4 of 2001 ... Money Laundering /
               <a>English</a> <a>Arabic</a></p>

        so the row becomes `Decree Law No. 4 of 2001 ... (English)`, which is the
        shape `CBB-Laws-Regulations` already produces.

        Written here rather than in cbb_crawler.py for the reason the module
        docstring gives: the legacy `cbb_monitoring` job keeps its exact
        behaviour while this path is proven beside it.
        """
        import hashlib
        import re
        from urllib.parse import urljoin

        from crawler.cbb_crawler import (CBB_GOV_BASE, COMPLIANCE_URL, REGULATOR,
                                         _fetch)

        soup = _fetch(COMPLIANCE_URL)
        if soup is None:
            raise RuntimeError(
                f"CBB mode 5: could not fetch {COMPLIANCE_URL}. That is a failed "
                f"read, not an empty section.")

        def instrument_name(anchor) -> str:
            """The <p>'s own text, minus the CMS's list numbering."""
            p = anchor.parent
            if p is None:
                return ""
            own = " ".join(t.strip() for t in p.find_all(string=True, recursive=False)
                           if t.strip())
            own = re.sub(r"\s+", " ", own).strip()
            # "1.Decree Law No. 4" / "2. 21st March 2021 ..." -- the leading
            # number is the CMS list marker, not part of the instrument.
            own = re.sub(r"^\d+\s*[.)]\s*", "", own)
            # The paragraph runs INTO the link ("For more information on BEPS,"
            # / "... Money Laundering /"), so its trailing joiner is punctuation
            # that belongs to the sentence, not to the instrument's name.
            return own.strip(" /:,-").strip()

        docs: List[RegulatoryDocument] = []
        for section_id, category in (("aml", "AML Compliance"),
                                     ("eofi", "EOFI Compliance")):
            section = (soup.find("div", id=section_id)
                       or soup.find("section", id=section_id))
            if section is None:
                raise RuntimeError(
                    f"CBB mode 5: section #{section_id} is gone from "
                    f"{COMPLIANCE_URL}. The page has been restructured.")

            seen = set()
            before = len(docs)
            for a in section.find_all("a", href=True):
                href = a["href"]
                if href.startswith("#") or href in seen:
                    continue
                seen.add(href)
                variant = a.get_text(strip=True)
                if not variant:
                    continue
                # ONLY THE FORMAT WORDS ARE REWRITTEN. MEASURED on the page,
                # both sections: 55 distinct anchor texts, of which exactly four
                # name a format rather than an instrument --
                #
                #     Letter 8, Guidance Paper 8, English 6, Arabic 3   = 25 rows
                #
                # The other 51 are real titles ("High-Risk Jurisdictions",
                # "FATF Recommendations"), and one of them proves the rule has
                # to be narrow: "1.AML/CFT Key Features - 2005 Regulations:" is
                # the ANCHOR while its paragraph holds the prose "A pamphlet on
                # CBB's AML/CFT regulation". Rewriting every row from its
                # paragraph put that one backwards.
                key = variant.strip().lower().rstrip(".")
                name = instrument_name(a) if key in _CM_FORMAT_WORDS else ""
                title = f"{name} ({variant})" if name else variant
                text = title
                docs.append(RegulatoryDocument(
                    regulator       = REGULATOR,
                    source_system   = self.source_system,
                    category        = category,
                    title           = title,
                    document_url    = urljoin(CBB_GOV_BASE, href),
                    source_page_url = f"{COMPLIANCE_URL}#{section_id}",
                    document_html   = str(a.parent) if a.parent else "",
                    doc_path        = [REGULATOR, _CM_COMPLIANCE_FOLDER,
                                       category, title],
                    extra_meta      = {"section_id": section_id,
                                       "content_text": text,
                                       "link_text": variant},
                    content_hash    = hashlib.md5(text.encode()).hexdigest(),
                ))
            if len(docs) == before:
                raise RuntimeError(
                    f"CBB mode 5: section #{section_id} yielded no documents. "
                    f"An empty section is a failed parse, not a result.")
            logger.info("CBB mode 5 - %s: %d document(s)",
                        category, len(docs) - before)
        return docs

    def fetch_documents(self, limit: Optional[int] = None) -> List[RegulatoryDocument]:
        if self.mode == "4":
            docs = self._fetch_capital_market()
        elif self.mode == "5":
            docs = self._fetch_compliance()
        elif self.mode == "2c" and self.volume:
            docs = self._fetch_rulebook_volume()
        else:
            # Imported here, not at module scope: cbb_crawler.py pulls in the
            # sub-crawlers under cbb_test_crawlers/ at import time, and a config
            # listing CBB sources should not pay that cost just to be read.
            from crawler.cbb_crawler import CBBCrawlerV2

            docs = CBBCrawlerV2().fetch_documents(
                mode=self.mode, max_volumes=self.max_volumes) or []

        # A mode that returns nothing is a FINDING, not a result. Every other
        # crawler in the library says this; CBB never did, which is one reason a
        # scheduled job could produce 0 rows without anyone noticing.
        if not docs:
            raise RuntimeError(
                f"CBB mode {self.mode} ({self.source_system}) returned no "
                f"documents. That is a failed read, not an empty section.")

        # ---- doc_path MUST START AT THE REGULATOR ---------------------------
        # The country tree is built from `doc_path`, not from the `regulator`
        # column, so a path that starts anywhere else puts the source BESIDE the
        # regulator instead of under it.
        #
        # MEASURED on output/workbooks/cbb_others.xlsx, 2026-09-10: all 1,240
        # rows carry `regulator = "Central Bank of Bahrain"`, and yet
        #
        #     CBB-AML-LAW   43   doc_path[0] = 'Bahrain Anti Money Laundering Law 2001'
        #     CBB-CORPGOV   16   doc_path[0] = 'The Corporate Governance Code ...'
        #
        # so the reader showed both as siblings of Central Bank of Bahrain under
        # Bahrain. The cause is `_aml_doc_to_regulatory` (cbb_crawler.py), which
        # assigns `doc_path = doc.path` verbatim, and `Aml_crawler_v2.SOURCES`
        # sets `root_path: ""` for both -- correctly, since the old value was an
        # invented crumb -- leaving the tree's own root heading at position 0.
        #
        # This is the SAME defect modes 4 and 2c had, fixed there by seeding the
        # walk with the regulator. 2a and 2b are the two that still go straight
        # through CBBCrawlerV2, so they are repaired here instead.
        #
        # A PREFIX, NOT A REWRITE. Unlike mode 4's pruned ancestry, nothing is
        # missing from these paths -- they are complete and merely unrooted, so
        # adding the crumb is enough and no label has to be reconstructed.
        #
        # Written as a guard over every mode rather than a 2a/2b branch: modes
        # 1, 3, 4, 5 and 2c already start at the regulator, so it is a no-op for
        # them, and a future mode cannot reintroduce this silently.
        for d in docs:
            trail = list(getattr(d, "doc_path", None) or [])
            if trail and str(trail[0]).strip() != self.regulator:
                d.doc_path = [self.regulator] + trail

        override = SOURCE_SYSTEM_OVERRIDE.get(self.mode)
        for d in docs:
            if override:
                d.source_system = override
            # The mode is what was actually run; keep it so a row can be traced
            # back to the code that produced it without guessing from the name.
            meta = dict(getattr(d, "extra_meta", None) or {})
            meta.setdefault("cbb_mode", self.mode)
            d.extra_meta = meta

        wrong = sorted({d.source_system for d in docs} - {self.source_system})
        if wrong:
            # Loud, because a source writing under a name the config did not
            # declare is invisible to the gate scoped on that name.
            raise RuntimeError(
                f"CBB mode {self.mode} declared source_system "
                f"{self.source_system!r} but produced {wrong!r}. Fix the config "
                f"or SOURCE_SYSTEM_OVERRIDE -- a mismatch here makes the "
                f"completeness gate scope onto rows that do not exist.")

        cap = limit if isinstance(limit, int) and limit > 0 else None
        if cap:
            docs = docs[:cap]

        self.last_result = {
            "run": {"blocked_pages": 0, "warnings": []},
            "by_source": {self.source_system: len(docs)},
        }
        logger.info("CBBSource[mode %s / %s] -> %d document(s)",
                    self.mode, self.source_system, len(docs))

        # The single exit.
        #
        # For modes 1-3 and 5 this is a backstop: they set their own md5 and
        # `stamp_` never overwrites one, so it only fills the branches that
        # leave it empty (measured: `_scrape_resolution` sets "" whenever the
        # page yielded no text).
        #
        # MODE 4 SETS NO HASH ON PURPOSE, so this is where its fingerprint comes
        # from. `hash_for` hashes the VISIBLE TEXT of document_html, which is
        # ONBOARDING's first preference; the mode-4 code it replaced hashed
        # `content_text` with its own inline md5, a fourth definition of a thing
        # this repo says must have exactly one.
        return stamp_content_hashes(docs)


__all__ = ["CBBSource", "REGULATOR", "SOURCE_SYSTEM_OVERRIDE"]
