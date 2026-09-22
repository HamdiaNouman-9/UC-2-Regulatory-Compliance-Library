"""
regression_check.py — did a crawler change break another regulator?
===================================================================

`crawler.py` is shared by every regulator, so a fix aimed at one site can
silently change what we extract for the others. That is not hypothetical: the
aml.gov.sa breadcrumb fix moved CMA's section anchor from "Laws & Regulations"
to "Implementing Regulations" — i.e. changed which pages CMA would crawl at all —
and nothing in the AML work would have revealed it.

This runs the extraction JS over a FROZEN COPY of each regulator's seed page and
compares the result to a stored baseline.

Why frozen copies? A regression check must answer "did MY change alter the
output?". Hitting the live sites cannot answer that: the regulator publishes a
new circular, the text length moves, and the check cries wolf. Saving the
rendered HTML once removes the site as a variable, and makes the check offline,
deterministic and fast (seconds, not the ~26 s per live page load).

    # one-time, and after any DELIBERATE change to what we extract
    venv/Scripts/python.exe generic_crawler/regression_check.py --save-baseline

    # before committing a crawler change
    venv/Scripts/python.exe generic_crawler/regression_check.py

Exit code 0 = every regulator unchanged, 1 = something moved (details printed).

Note this checks the EXTRACTION layer (breadcrumb, scope anchor, main content,
document links, headings) — the part shared by all sites and the part that
actually broke. It is not a full multi-page crawl of each regulator.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from playwright.sync_api import sync_playwright

import crawler as C
from strategies import detect_shape

HERE = Path(__file__).resolve().parent
PAGES_DIR = HERE / "regression_pages"          # frozen seed HTML (regenerate, don't edit)
BASELINE = HERE / "regression_baseline.json"   # expected extraction per seed

# One representative seed per regulator. Add a site here when you onboard it —
# a site with no entry is a site nobody will notice you broke.
SEEDS = {
    "sama_rulebook":  "https://rulebook.sama.gov.sa/en/book-category/1365",
    "sama_circulars": "https://rulebook.sama.gov.sa/en/sama-circulars",
    "cbb":            "https://cbben.thomsonreuters.com/rulebook/common-volume",
    "secp_acts":      "https://www.secp.gov.pk/laws/acts/",
    "sbp_circulars":  "https://www.sbp.org.pk/circulars/cir.asp",
    "cma_regs":       "https://cma.org.sa/en/RulesRegulations/Regulations/Pages/default.aspx",
    "aml_rules":      "https://www.aml.gov.sa/en-us/Pages/RulesandRegulations.aspx",
    # Added 2026-08-18 when CBE was onboarded. One representative seed: the
    # laws-regulations landing page, which is the shape the eleven generic CBE
    # sources all share (breadcrumb + card grid + /-/media/ document links).
    # Its own crawl is NOT a source — cbe.yml seeds the three subtrees beneath
    # it instead, because prefix scope here sweeps in circulars the API owns.
    "cbe_lawsreg":    "https://www.cbe.org.eg/en/laws-regulations",
    # Added 2026-08-27. Every one of these hosts has a SITE_PROFILES entry, and
    # until now nothing guarded them: a change to the shared junk list, the
    # breadcrumb reader or _SIZE_TAIL could move their output and this check
    # would still have said "All 8 regulators unchanged".
    #
    # One representative seed each, chosen as the page whose shape the rest of
    # that regulator's sources share:
    #
    #   rera        content_selector #page-content + drop_selectors
    #   sio         keep_modals — a section page whose laws are Bootstrap modals
    #   lloc        content_selector div.bodycontent + drop_selectors; the
    #               narrative page, not /Legislation/Latest, because that one is
    #               a POST-driven listing a browser walk cannot read
    #   pdpa        breadcrumb_li + keep_buttons + unhide_animated, all three at
    #               once on the accordion page
    #   moic        group_headings + drop_selectors .Itemlang
    #
    # NOTE lloc.gov.bh THROTTLES WITH A 1,245-BYTE 404. If this seed comes back
    # `CHANGED` with an empty content_text_len, re-run after a minute before
    # believing it — that is the host, not the crawler.
    "rera_regs":      "https://www.rera.gov.bh/en/regulations",
    "sio_orders":     "https://www.sio.gov.bh/en/ministerial-orders",
    "lloc_page":      "https://www.lloc.gov.bh/en/page/Legal%20instruments%20in%20Bahraini%20legislation",
    "pdpa_law":       "https://www.pdp.gov.bh/en/regulations.html",
    "moic_regs":      "https://www.moic.gov.bh/en/regulations",
}


def fingerprint(page, url):
    """The extraction facts worth guarding, for one page.

    Everything here is something a crawler change has already been seen to move,
    or would change which pages get crawled. `section_anchor` matters most: it is
    the last breadcrumb crumb, and breadcrumb scope keeps only pages whose trail
    contains it — so a change here silently redraws the crawl boundary.
    """
    prof = C.profile_for(url)
    try:
        shape = detect_shape(page, None)
    except Exception as e:
        shape = f"error:{type(e).__name__}"

    bc = page.evaluate(C.JS_BREADCRUMB, prof) or []
    content = page.evaluate(C.JS_MAIN_CONTENT, prof) or {"text": ""}
    links = page.evaluate(C.JS_LINKS) or []
    docs = [l for l in links if C.is_document_link(l.get("href", ""))]

    return {
        "shape": shape,
        "breadcrumb": bc,
        "section_anchor": (bc[-1] if bc else "").strip().lower(),
        "content_text_len": len(content.get("text", "")),
        "n_links": len(links),
        "n_doc_links": len(docs),
        # section_path exactly as a document row would receive it
        "doc_section_paths": sorted({
            C.doc_section_path(bc, l.get("group") if prof["group_headings"] else "")
            for l in docs
        })[:8],
        "profile": prof,
    }


def freeze(pw, name, url):
    """Load a seed for real and save the RENDERED DOM for later offline replay.

    Deliberately does NOT record the fingerprint from this live page. Reopening
    the saved DOM re-runs its scripts, so the replayed page is never byte-identical
    to the live one (measured: a link count off by one, text lengths off by a few
    hundred chars). Baselining the live numbers would make the very first check
    fail on all four such sites. The baseline is taken from a replay instead, so
    it is compared against exactly how it will be reproduced.
    """
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        locale="en-US")
    page = ctx.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(3500)
        try:
            page.mouse.wheel(0, 2500)
            page.wait_for_timeout(700)
        except Exception:
            pass
        html = page.content()
        # <base> keeps relative hrefs resolving to the real site once the file is
        # reopened from disk — without it every link becomes file:/// and the
        # document-link counts collapse to zero.
        if "<base " not in html[:2000].lower():
            html = html.replace("<head>", f'<head><base href="{url}">', 1)
        (PAGES_DIR / f"{name}.html").write_text(html, encoding="utf-8")
    finally:
        browser.close()


def replay(pw, name, url):
    """Re-run extraction against the frozen copy — no network involved."""
    path = PAGES_DIR / f"{name}.html"
    if not path.exists():
        return None
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page()
    try:
        page.goto(path.as_uri(), wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1200)
        return fingerprint(page, url)
    finally:
        browser.close()


def diff(name, old, new):
    """Report every field that moved. Returns True if anything differs."""
    if new is None:
        print(f"  {name}: NO FROZEN PAGE — run --save-baseline")
        return True
    bad = False
    for key in sorted(old):
        if old[key] != new.get(key):
            if not bad:
                print(f"\n  {name}: CHANGED")
                bad = True
            print(f"      {key}")
            print(f"        was: {old[key]!r}")
            print(f"        now: {new.get(key)!r}")
    if not bad:
        print(f"  {name}: ok")
    return bad


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--save-baseline", action="store_true",
                    help="Re-fetch every seed, freeze its HTML, and record the "
                         "current extraction as the new expected result.")
    ap.add_argument("--only", help="Limit to one seed name (see SEEDS).")
    args = ap.parse_args()

    seeds = SEEDS if not args.only else {args.only: SEEDS[args.only]}
    PAGES_DIR.mkdir(exist_ok=True)

    with sync_playwright() as pw:
        if args.save_baseline:
            base = json.loads(BASELINE.read_text(encoding="utf-8")) if BASELINE.exists() else {}
            for name, url in seeds.items():
                print(f"freezing {name} ...", flush=True)
                try:
                    freeze(pw, name, url)
                    base[name] = replay(pw, name, url)   # baseline == how it replays
                except Exception as e:
                    print(f"  FAILED: {type(e).__name__}: {str(e)[:90]}")
            BASELINE.write_text(json.dumps(base, indent=2, ensure_ascii=False),
                                encoding="utf-8")
            print(f"\nbaseline written: {BASELINE}")
            print(f"frozen pages    : {PAGES_DIR}")
            return 0

        if not BASELINE.exists():
            print("No baseline yet — run with --save-baseline first.")
            return 1
        base = json.loads(BASELINE.read_text(encoding="utf-8"))

        print("Replaying frozen seed pages\n")
        changed = []
        for name, url in seeds.items():
            if name not in base:
                print(f"  {name}: no baseline entry (run --save-baseline)")
                changed.append(name)
                continue
            if diff(name, base[name], replay(pw, name, url)):
                changed.append(name)

    if changed:
        print(f"\nCHANGED: {', '.join(changed)}")
        print("If the change is intended, re-run with --save-baseline to accept it.")
        return 1
    print(f"\nAll {len(seeds)} regulators unchanged.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
