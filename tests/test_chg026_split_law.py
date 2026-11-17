"""CHG-026: a law published as one PDF plus a page per article is captured
once, for every regulator. No network, no database: the shared-file rule,
the orchestrator step that applies it, and the fix script's plan are pure
functions driven with small trees.

    .venv/Scripts/python.exe -m pytest tests/test_chg026_split_law.py -q
"""
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from models.models import RegulatoryDocument  # noqa: E402
from scripts.chg026_split_laws import owner_update, plan  # noqa: E402
from utils.file_links import (carried_file, claim_shared_files, doc_path_of,  # noqa: E402
                              files_of, holders_of, normalise_all)

LAW_PDF = "https://rulebook.sama.gov.sa/sites/default/files/en_net_file_store/SAMA_EN_996_VER1.pdf"
OTHER_PDF = "https://rulebook.sama.gov.sa/sites/default/files/en_net_file_store/SAMA_EN_1293_VER1.pdf"
SAMA = "Saudi Arabian Monetary Authority (SAMA)"
BASE = "https://rulebook.sama.gov.sa/en/"


def _doc(title, path, url=None, pdf=None, html="<p>text</p>", key="org_pdf_link", **meta):
    """A crawled document: a page in document_url and, when `pdf` is given,
    the file beside it under a crawler's own key (SAMA's org_pdf_link)."""
    d = RegulatoryDocument(regulator=SAMA, source_system="SAMA RULEBOOK", category="Banking",
                           title=title, document_url=url or BASE + title.lower().replace(" ", "-"),
                           source_page_url=BASE + "banking-sector",       # the listing, as the crawler sets it
                           document_html=html, extra_meta=dict(meta))
    if pdf:
        d.extra_meta[key] = pdf
        d.file_type = "PDF"
    elif html:
        d.file_type = "HTML"
    d.doc_path = [SAMA, "SAMA RULEBOOK", "Banking"] + list(path)
    return d


# --------------------------------------------------------------------------- #
# what a carrier is
# --------------------------------------------------------------------------- #

def test_a_carrier_is_a_page_with_exactly_one_file_named_beside_it():
    assert carried_file(_doc("Article 1", ["Law", "Article 1"], pdf=LAW_PDF)) == LAW_PDF
    assert carried_file(_doc("Article 1", ["Law", "Article 1"], pdf=LAW_PDF, key="pdf_link")) == LAW_PDF
    assert carried_file(_doc("Law", ["Law"])) == ""                                    # no file
    assert carried_file(_doc("Circular", ["C"], url="https://x/c.pdf")) == ""        # the file IS the document
    bundle = _doc("Bundle", ["B"], attachment_links="https://x/a.pdf | https://x/b.pdf")
    assert carried_file(bundle) == ""                                                 # several files: a bundle


def test_doc_path_comes_back_as_a_list_from_every_shape():
    assert doc_path_of({"doc_path": '["A", "B"]'}) == ["A", "B"]
    assert doc_path_of({"doc_path": "A > B"}) == ["A", "B"]
    assert doc_path_of({"doc_path": None}) == []
    assert doc_path_of(_doc("X", ["L", "X"])) == [SAMA, "SAMA RULEBOOK", "Banking", "L", "X"]


# --------------------------------------------------------------------------- #
# the rule over a crawl
# --------------------------------------------------------------------------- #

def test_a_pdf_two_articles_carry_moves_to_the_law():
    law = _doc("Banking Control Law", ["BCL"])
    arts = [_doc(f"Article {i}", ["BCL", f"Article {i}"], pdf=LAW_PDF, status="In-Force") for i in (1, 2, 3)]
    assert claim_shared_files([law] + arts) == 3
    assert law.extra_meta["attachment_links"] == LAW_PDF and law.file_type == "PDF"
    for a in arts:
        assert "org_pdf_link" not in a.extra_meta and a.file_type == "HTML"
        assert a.document_url.startswith(BASE)                     # the page stays the document
    assert arts[0].extra_meta == {"status": "In-Force"}            # the rest of the metadata stays
    normalise_all([law] + arts)                                    # then the file rule, as the orchestrator does
    assert law.document_url == LAW_PDF and law.extra_meta["found_on"] == BASE + "banking-control-law"
    assert all(a.document_url.startswith(BASE) for a in arts)


def test_one_carrier_is_not_enough():
    law, only = _doc("Law", ["Law"]), _doc("Article 1", ["Law", "Article 1"], pdf=LAW_PDF)
    assert claim_shared_files([law, only]) == 0
    assert "attachment_links" not in law.extra_meta and carried_file(only) == LAW_PDF


def test_a_descendant_with_a_different_file_keeps_it():
    docs = [_doc("Law", ["Law"]), _doc("A1", ["Law", "A1"], pdf=LAW_PDF),
            _doc("A2", ["Law", "A2"], pdf=LAW_PDF), _doc("Annex", ["Law", "Annex"], pdf=OTHER_PDF)]
    assert claim_shared_files(docs) == 2
    assert carried_file(docs[3]) == OTHER_PDF


def test_chapters_claim_first_and_the_law_takes_over():
    """Hubs are taken deepest first: each chapter takes its articles' file,
    then the law takes it from the two chapters, which are pages again."""
    law, ch1, ch2 = _doc("Law", ["Law"]), _doc("Chapter 1", ["Law", "Ch1"]), _doc("Chapter 2", ["Law", "Ch2"])
    arts = [_doc("A1", ["Law", "Ch1", "A1"], pdf=LAW_PDF), _doc("A2", ["Law", "Ch1", "A2"], pdf=LAW_PDF),
            _doc("A3", ["Law", "Ch2", "A3"], pdf=LAW_PDF), _doc("A4", ["Law", "Ch2", "A4"], pdf=LAW_PDF)]
    assert claim_shared_files([law, ch1, ch2] + arts) == 6                   # 4 articles, then 2 chapters
    assert law.extra_meta["attachment_links"] == LAW_PDF
    for d in [ch1, ch2] + arts:
        assert not carried_file(d) and d.file_type == "HTML", d.title


def test_a_law_with_one_chapter_leaves_the_file_on_the_chapter():
    law, ch1 = _doc("Law", ["Law"]), _doc("Chapter 1", ["Law", "Ch1"])
    arts = [_doc("A1", ["Law", "Ch1", "A1"], pdf=LAW_PDF), _doc("A2", ["Law", "Ch1", "A2"], pdf=LAW_PDF)]
    claim_shared_files([law, ch1] + arts)
    assert ch1.extra_meta["attachment_links"] == LAW_PDF and "attachment_links" not in law.extra_meta


def test_a_hub_that_already_names_another_file_claims_nothing():
    law = _doc("Law", ["Law"], pdf=OTHER_PDF)
    arts = [_doc("A1", ["Law", "A1"], pdf=LAW_PDF), _doc("A2", ["Law", "A2"], pdf=LAW_PDF)]
    assert claim_shared_files([law] + arts) == 0
    assert all(carried_file(a) == LAW_PDF for a in arts)


def test_a_hub_that_already_names_the_same_file_just_strips_its_articles():
    law = _doc("Law", ["Law"], pdf=LAW_PDF)
    arts = [_doc("A1", ["Law", "A1"], pdf=LAW_PDF), _doc("A2", ["Law", "A2"], pdf=LAW_PDF)]
    assert claim_shared_files([law] + arts) == 2
    assert files_of(law) == [LAW_PDF] and not any(carried_file(a) for a in arts)


def test_documents_whose_file_is_the_document_are_never_carriers():
    """CBB's shape: the PDF is the document and there is no page. A hub
    above two such rows claims nothing, so a circular filed under a volume
    keeps its file."""
    hub = _doc("Volume 1", ["V1"])
    circs = [_doc("C1", ["V1", "C1"], url="https://x/c.pdf"), _doc("C2", ["V1", "C2"], url="https://x/c.pdf")]
    assert claim_shared_files([hub] + circs) == 0
    assert all(c.document_url == "https://x/c.pdf" for c in circs)


def test_two_documents_at_one_path_are_ambiguous_and_neither_claims():
    law, copy = _doc("Law", ["Law"]), _doc("Law (copy)", ["Law"], url=BASE + "law-copy")
    arts = [_doc("A1", ["Law", "A1"], pdf=LAW_PDF), _doc("A2", ["Law", "A2"], pdf=LAW_PDF)]
    assert claim_shared_files([law, copy] + arts) == 0
    assert all(carried_file(a) == LAW_PDF for a in arts)


def test_documents_without_a_path_are_left_alone():
    a, b = _doc("A", [], pdf=LAW_PDF), _doc("B", [], pdf=LAW_PDF)
    a.doc_path = b.doc_path = None
    assert claim_shared_files([a, b, _doc("Hub", [])]) == 0


def test_the_rule_is_idempotent():
    docs = [_doc("Law", ["Law"])] + [_doc(f"A{i}", ["Law", f"A{i}"], pdf=LAW_PDF) for i in (1, 2)]
    assert claim_shared_files(docs) == 2
    assert claim_shared_files(docs) == 0


# --------------------------------------------------------------------------- #
# a document that arrives alone: the stored rows say who holds the file
# --------------------------------------------------------------------------- #

def test_a_lone_article_of_a_held_law_gives_its_file_up():
    stored = [{"document_url": LAW_PDF, "doc_path": '["%s", "SAMA RULEBOOK", "Banking", "Law"]' % SAMA},
              {"document_url": BASE + "other", "doc_path": "x"}]
    holders = holders_of(stored)
    assert holders == {LAW_PDF: [[SAMA, "SAMA RULEBOOK", "Banking", "Law"]]}
    new = _doc("Article 27", ["Law", "Article 27"], pdf=LAW_PDF)
    assert claim_shared_files([new], holders) == 1
    assert not carried_file(new) and new.file_type == "HTML"


def test_a_lone_article_of_an_unheld_law_keeps_its_file():
    holders = holders_of([{"document_url": LAW_PDF, "doc_path": '["%s", "SAMA RULEBOOK", "Finance", "Law"]' % SAMA}])
    new = _doc("Article 1", ["Other Law", "Article 1"], pdf=LAW_PDF)     # not beneath the holder
    assert claim_shared_files([new], holders) == 0
    assert carried_file(new) == LAW_PDF


# --------------------------------------------------------------------------- #
# the orchestrator applies it before the file rule, for every regulator
# --------------------------------------------------------------------------- #

class _Crawler:
    source_system = "SAMA RULEBOOK"

    def __init__(self, docs):
        self.docs = docs

    def fetch_documents(self, limit=None):
        return self.docs


class _Repo:
    def __init__(self, stored):
        self.stored = stored
        self.asked = []

    def find_regulations_by_source(self, source_system, regulator=None):
        self.asked.append((source_system, regulator))
        return self.stored


def test_the_orchestrator_shapes_files_with_the_stored_rows_in_hand():
    from orchestrator.orchestrator import Orchestrator
    law = _doc("Law", ["Law"])
    arts = [_doc("A1", ["Law", "A1"], pdf=LAW_PDF), _doc("A2", ["Law", "A2"], pdf=LAW_PDF)]
    lone = _doc("Article 9", ["Held Law", "Article 9"], pdf=OTHER_PDF)
    stored = [{"document_url": OTHER_PDF, "doc_path": '["%s", "SAMA RULEBOOK", "Banking", "Held Law"]' % SAMA}]
    repo = _Repo(stored)
    o = Orchestrator(crawler=_Crawler([law] + arts + [lone]), repo=repo, source_name="SAMA RULEBOOK")
    out = o._shape_files([law] + arts + [lone])
    assert [d.title for d in out] == ["Law", "A1", "A2", "Article 9"]
    assert repo.asked == [("SAMA RULEBOOK", SAMA)]
    assert law.document_url == LAW_PDF                              # claimed, then the file rule placed it
    assert all(a.document_url.startswith(BASE) and not files_of(a)[0].endswith(".pdf") for a in arts)
    assert lone.document_url == BASE + "article-9"                   # the stored law above it holds the file


def test_the_orchestrator_still_shapes_files_when_the_stored_rows_cannot_be_read():
    from orchestrator.orchestrator import Orchestrator

    class _Broken(_Repo):
        def find_regulations_by_source(self, *a, **kw):
            raise RuntimeError("db down")

    law = _doc("Law", ["Law"])
    arts = [_doc("A1", ["Law", "A1"], pdf=LAW_PDF), _doc("A2", ["Law", "A2"], pdf=LAW_PDF)]
    o = Orchestrator(crawler=_Crawler([law] + arts), repo=_Broken([]), source_name="SAMA RULEBOOK")
    out = o._shape_files([law] + arts)
    assert out[0].document_url == LAW_PDF and all(a.document_url.startswith(BASE) for a in out[1:])


# --------------------------------------------------------------------------- #
# the reference-number tiebreak, guarded
# --------------------------------------------------------------------------- #

class _RefRepo(_Repo):
    """Identity never matches; one stored row carries the law's reference number."""

    def __init__(self, row):
        super().__init__([])
        self.row = row

    def find_by_identity(self, document_url, doc_path):
        return None

    def find_by_identity_fields(self, fields, accept=None):
        return None

    @staticmethod
    def _norm_doc_path(v):                       # the repository's canonical form, as classify compares it
        import json
        if isinstance(v, str) and v.startswith("["):
            v = json.loads(v)
        return " > ".join(v) if isinstance(v, list) else str(v or "")

    def find_by_reference(self, ref):
        return dict(self.row) if ref == self.row["reference_no"] else None


def test_the_reference_tiebreak_matches_the_same_title_once_and_never_the_rest():
    """Every SAMA article page carries its law's number. When a law is
    restructured and the articles' paths no longer match, the tiebreak used
    to fold every article into the first stored row with that number (106
    versions on one row, measured live 2026-11-17). Now the row with the
    same title is matched once; the others are new."""
    from orchestrator.orchestrator import Orchestrator
    stored = {"id": 1292, "title": "Article 106", "reference_no": "10698", "document_url": LAW_PDF,
              "doc_path": '["%s", "SAMA RULEBOOK", "Laws", "The Law"]' % SAMA, "content_hash": "", "extra_meta": {}}
    docs = [_doc("Article 1", ["The Law", "Chapter 1", "Article 1"]),
            _doc("Article 106", ["The Law", "Chapter 9", "Article 106"]),
            _doc("article 106", ["The Law", "Chapter 9", "Article 106 (copy)"])]
    for d in docs:
        d.reference_no = "10698"
    o = Orchestrator(crawler=_Crawler(docs), repo=_RefRepo(stored), source_name="SAMA RULEBOOK")
    buckets = o.classify_documents(docs)
    by_bucket = {k: [d.title for d in v] for k, v in buckets.items() if k in ("new", "modified", "unchanged")}
    assert by_bucket["new"] == ["Article 1", "article 106"]                   # the second same-title doc: the row is taken
    assert by_bucket["modified"] + by_bucket["unchanged"] == ["Article 106"]


def test_the_reference_tiebreak_still_finds_a_document_republished_at_a_new_url():
    from orchestrator.orchestrator import Orchestrator
    stored = {"id": 7, "title": "Circular 42", "reference_no": "C-42", "document_url": "https://x/old.pdf",
              "doc_path": '["%s", "SAMA RULEBOOK", "Circulars", "Circular 42"]' % SAMA, "content_hash": "", "extra_meta": {}}
    moved = _doc("Circular 42", ["Circulars", "Circular 42"], url="https://x/new.pdf")
    moved.reference_no = "C-42"
    o = Orchestrator(crawler=_Crawler([moved]), repo=_RefRepo(stored), source_name="SAMA RULEBOOK")
    buckets = o.classify_documents([moved])
    assert not buckets["new"] and (buckets["modified"] or buckets["unchanged"])


# --------------------------------------------------------------------------- #
# the fix script's plan, over the stored tree
# --------------------------------------------------------------------------- #

ROOT_PATH = [SAMA, "SAMA RULEBOOK", "Banking"]
LANDING = ROOT_PATH + ["Banking"]                          # the older crawl's sector landing row


def _row(rid, title, du, spu="", path=(), **meta):
    return {"id": rid, "title": title, "document_url": du, "source_page_url": spu,
            "extra_meta": dict(meta), "doc_path": ROOT_PATH + list(path)}


def _law_rows():
    """A law with two chapters of two articles each and one direct article,
    under the sector's landing row."""
    page, lst = BASE, BASE + "book-category/1361"
    return [
        _row(100, "Banking", page + "banking", path=["Banking"]),
        _row(101, "Banking Control Law", page + "banking-control-law", lst, path=["BCL"]),
        _row(102, "Chapter 1", page + "chapter-1", lst, path=["BCL", "Ch1"]),
        _row(103, "Chapter 2", page + "chapter-2", lst, path=["BCL", "Ch2"]),
        _row(104, "Article 1", LAW_PDF, page + "article-1", path=["BCL", "Ch1", "A1"], found_on=lst, status="In-Force"),
        _row(105, "Article 2", LAW_PDF, page + "article-2", path=["BCL", "Ch1", "A2"], found_on=lst),
        _row(106, "Article 3", LAW_PDF, page + "article-3", path=["BCL", "Ch2", "A3"], found_on=lst),
        _row(107, "Article 4", LAW_PDF, page + "node/4444", path=["BCL", "Ch2", "A4"], found_on=lst),
        _row(108, "Article 5", LAW_PDF, page + "article-5", path=["BCL", "A5"], found_on=lst),
    ]


def test_the_law_takes_the_pdf_and_every_article_takes_its_page():
    p = plan(_law_rows())
    assert p["owners"] == {101: LAW_PDF}
    moved = {c[0]: c for c in p["carriers"]}
    assert set(moved) == {104, 105, 106, 107, 108}
    rid, du, spu, meta = moved[104]
    assert du == BASE + "article-1"
    assert spu == BASE + "book-category/1361"                              # the listing, back where it was
    assert meta == {"status": "In-Force"}                                  # found_on removed, the rest kept
    assert not p["skipped"] and not p["left"]
    assert p["groups"] == [{"owner": 101, "title": "Banking Control Law", "pdf": LAW_PDF,
                            "carriers": 5, "depth": 2}]
    assert 102 not in moved and 103 not in moved                           # the chapters are untouched


def test_the_owner_row_keeps_its_page_as_provenance():
    du, meta = owner_update(_law_rows()[1], LAW_PDF)
    assert du == LAW_PDF and meta["found_on"] == BASE + "banking-control-law"
    assert owner_update({"document_url": LAW_PDF, "extra_meta": {}}, LAW_PDF) == (LAW_PDF, {})


def test_a_single_chapter_law_gives_the_pdf_to_the_chapter():
    rows = [r for r in _law_rows() if r["id"] in (100, 101, 102, 104, 105)]
    assert plan(rows)["owners"] == {102: LAW_PDF}


def test_one_carrier_is_left_alone_by_the_script_too():
    rows = [r for r in _law_rows() if r["id"] in (100, 101, 104)]
    p = plan(rows)
    assert p["owners"] == {} and p["carriers"] == [] and not p["skipped"]


def test_two_laws_sharing_one_pdf_each_keep_it_and_the_landing_row_is_silent():
    rows = [_law_rows()[0],
            _row(101, "Banking Control Law", BASE + "bcl", path=["BCL"]),
            _row(104, "Art 1", LAW_PDF, BASE + "a1", path=["BCL", "A1"]),
            _row(105, "Art 2", LAW_PDF, BASE + "a2", path=["BCL", "A2"]),
            _row(201, "Other Law", BASE + "other-law", path=["Other"]),
            _row(202, "Other Art 1", LAW_PDF, BASE + "o1", path=["Other", "A1"]),
            _row(203, "Other Art 2", LAW_PDF, BASE + "o2", path=["Other", "A2"])]
    p = plan(rows)
    assert p["owners"] == {101: LAW_PDF, 201: LAW_PDF}
    assert {c[0] for c in p["carriers"]} == {104, 105, 202, 203}
    assert not p["skipped"]


def test_a_copy_of_the_law_filed_elsewhere_is_left_as_it_is():
    rows = _law_rows() + [_row(201, "Circulars", BASE + "circulars", path=["Circulars"]),
                          _row(202, "Banking Control Law (circular)", LAW_PDF, BASE + "c1", path=["Circulars", "BCL"])]
    p = plan(rows)
    assert p["owners"] == {101: LAW_PDF} and 202 not in {c[0] for c in p["carriers"]}


def test_a_hub_that_already_carries_another_file_claims_nothing_so_the_chapters_keep_it():
    rows = _law_rows()
    rows[1]["document_url"] = OTHER_PDF
    p = plan(rows)
    assert p["owners"] == {102: LAW_PDF, 103: LAW_PDF}
    assert p["skipped"] == {"hub already carries another file": 1}
    assert p["left"] == {"hub already carries another file": 3}


def test_a_carrier_without_a_page_url_stays_and_the_others_move():
    rows = _law_rows()
    rows[5]["source_page_url"] = ""
    p = plan(rows)
    assert p["owners"] == {101: LAW_PDF}
    assert {c[0] for c in p["carriers"]} == {104, 106, 107, 108}
    assert p["skipped"] == {} and p["left"] == {"carrier has no page url": 1}


def test_two_rows_at_one_path_are_ambiguous_so_the_chapters_keep_the_pdf():
    rows = _law_rows() + [_row(109, "Banking Control Law (copy)", BASE + "bcl-copy", path=["BCL"])]
    p = plan(rows)
    assert p["owners"] == {102: LAW_PDF, 103: LAW_PDF}
    assert p["skipped"] == {"two rows at one path": 1} and p["left"] == {"two rows at one path": 3}


def test_rows_that_are_not_pdfs_or_not_shared_are_ignored():
    rows = [_row(301, "Law", BASE + "law", path=["Law"]),
            _row(302, "Circular A", "https://x/a.pdf", BASE + "a", path=["Law", "A"]),
            _row(303, "Circular B", "https://x/b.pdf", BASE + "b", path=["Law", "B"]),
            _row(304, "Note", BASE + "note", BASE + "note", path=["Law", "N"])]
    p = plan(rows)
    assert p["owners"] == {} and p["carriers"] == [] and not p["skipped"]


# --------------------------------------------------------------------------- #
# CBE's shape: two files listed on one page are two documents, not two articles
# --------------------------------------------------------------------------- #

def test_two_files_listed_on_one_page_keep_their_files_in_a_crawl():
    """CBE's cheque clearing house: each clearing page lists the same two
    laws. The documents share a page, so neither is an article with a page
    of its own, and the hub above claims nothing."""
    hub = _doc("Cheque Clearing House", ["CCH"])
    egp = [_doc("Trade Law 17", ["CCH", "EGP", "Trade Law 17"], url=BASE + "egp-cch", pdf="https://x/trade.pdf"),
           _doc("CBE Law 194", ["CCH", "EGP", "CBE Law 194"], url=BASE + "egp-cch", pdf="https://x/law194.pdf")]
    multi = [_doc("Trade Law 17", ["CCH", "Multi", "Trade Law 17"], url=BASE + "multi-cch", pdf="https://x/trade.pdf"),
             _doc("CBE Law 194", ["CCH", "Multi", "CBE Law 194"], url=BASE + "multi-cch", pdf="https://x/law194.pdf")]
    assert claim_shared_files([hub] + egp + multi) == 0
    assert all(carried_file(d) for d in egp + multi) and "attachment_links" not in hub.extra_meta


def test_two_rows_with_one_page_keep_their_files_in_the_script_too():
    rows = [_row(900, "Cheque Clearing House", BASE + "cch", path=["CCH"]),
            _row(901, "Trade Law 17", "https://x/trade.pdf", BASE + "egp-cch", path=["CCH", "EGP", "Trade Law 17"]),
            _row(902, "CBE Law 194", "https://x/law194.pdf", BASE + "egp-cch", path=["CCH", "EGP", "CBE Law 194"]),
            _row(903, "Trade Law 17", "https://x/trade.pdf", BASE + "multi-cch", path=["CCH", "Multi", "Trade Law 17"]),
            _row(904, "CBE Law 194", "https://x/law194.pdf", BASE + "multi-cch", path=["CCH", "Multi", "CBE Law 194"])]
    p = plan(rows)
    assert p["owners"] == {} and p["carriers"] == [] and not p["skipped"]


def test_a_hub_carrying_a_spreadsheet_already_carries_a_file():
    rows = [_row(910, "Statistics", "https://x/stats-jul-2026.xlsx", BASE + "cch", path=["CCH"]),
            _row(911, "Article 1", LAW_PDF, BASE + "a1", path=["CCH", "A1"]),
            _row(912, "Article 2", LAW_PDF, BASE + "a2", path=["CCH", "A2"])]
    p = plan(rows)
    assert p["owners"] == {} and p["skipped"] == {"hub already carries another file": 1}
