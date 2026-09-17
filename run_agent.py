"""
run_agent.py  --  Simple runner to test the autonomous crawling agent yourself.

You don't need to write any code. Three commands:

1) CRAWL a regulator tab (the agent inspects the site, writes its own crawler,
   tests it, cross-checks it against the live site, then does a full crawl):

     python run_agent.py crawl --regulator SBP --tab "Circulars" \
         --url "https://www.sbp.org.pk/circulars/cir.asp"

   Optional:
     --model claude    (default; best quality)  |  --model deepseek  (cheaper)
     --quick           (fast test crawl only, a small sample -- good for a first look)

2) Give FEEDBACK in plain English and let the agent fix its crawler and re-run:

     python run_agent.py feedback --regulator SBP \
         --note "you're missing the older years; open every year folder for each department"

   Optional: --url "<a page the feedback is about>"  (helps the agent look again)

3) Rebuild/locate the EXCEL for the latest results of a regulator:

     python run_agent.py excel --regulator SBP

Every command finishes by pointing you to an Excel file you can open and review.
Nothing is ever written to your production database -- results are local files only.
"""

import argparse
import sys
from pathlib import Path

# Make sure the project is importable no matter where this is run from.
REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Friendly model names -> actual OpenRouter model ids.
MODELS = {
    "claude": "anthropic/claude-sonnet-4.5",
    "deepseek": "deepseek/deepseek-v3.2",
}


def _resolve_model(name: str) -> str:
    return MODELS.get(name.lower(), name)  # allow passing a full id too


def _print_header(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def _summarize_and_excel(regulator: str, model_id: str, tab: str = None):
    """Print a plain-English summary of the latest run and build the Excel."""
    from dynamic_crawler.auto import onboard, review_report

    work_dir = onboard._work_dir(regulator, model_id, tab)
    docs_json = work_dir / "docs.json"
    if not docs_json.exists():
        print(f"\n(No results found yet in {work_dir}.)")
        return

    _print_header(f"RESULTS  --  {regulator} / {tab}" if tab else f"RESULTS  --  {regulator}")
    # review_report prints a reviewer summary AND builds report.xlsx
    try:
        review_report.build(str(work_dir))
    except SystemExit as e:
        print(str(e))
        return

    xlsx = work_dir / "report.xlsx"
    print("\nOPEN THIS TO REVIEW THE RESULTS:")
    print(f"   {xlsx}")
    print("\nIf something looks wrong, correct it in plain English:")
    print(f'   python run_agent.py feedback --regulator {regulator} --note "what to fix"')
    print()


def cmd_crawl(args):
    from dynamic_crawler.auto import onboard

    model_id = _resolve_model(args.model)
    source_system = args.source_system or f"{args.regulator} {args.tab}"

    _print_header(f"AGENT CRAWL  --  {args.regulator} / {args.tab}")
    print(f"Site        : {args.url}")
    print(f"Model       : {args.model}  ({model_id})")
    print(f"Mode        : {'quick test sample' if args.quick else 'full crawl'}")
    print("\nThe agent will now: look at the site -> write a crawler -> test it ->")
    print("cross-check it against the live site -> refine if needed" +
          ("" if args.quick else " -> full crawl") + ".")
    print("Watch the log lines below. This can take several minutes.\n")

    state = onboard.onboard(
        regulator=args.regulator,
        tab_name=args.tab,
        source_system=source_system,
        seed_url=args.url,
        model=model_id,
        full_run=not args.quick,
    )

    if not state.accepted:
        _print_header(f"AGENT COULD NOT PRODUCE A PASSING CRAWLER  --  {args.regulator}")
        print("The agent tried several times but its self-checks kept failing.")
        print("This is the safety net working -- it won't hand you data it can't verify.")
        print("You can nudge it with feedback and it will try again:")
        print(f'   python run_agent.py feedback --regulator {args.regulator} '
              f'--note "describe what the site looks like / what to do"')
        print("\n(Partial diagnostics saved under output/dynamic_crawler/"
              f"{args.regulator}/ for inspection.)")
        return

    _summarize_and_excel(args.regulator, model_id, args.tab)


def cmd_feedback(args):
    from dynamic_crawler.auto import onboard

    model_id = _resolve_model(args.model)
    _print_header(f"APPLYING YOUR FEEDBACK  --  {args.regulator}")
    print(f'Your note   : "{args.note}"')
    print(f"Model       : {args.model}")
    print("\nThe agent will rewrite its crawler using your feedback, then re-test,")
    print("cross-check, and re-crawl. Watch the log lines below.\n")

    onboard.refine_with_feedback(
        regulator=args.regulator,
        feedback=args.note,
        model=model_id,
        sample_url=args.url,
        tab_name=args.tab,
    )
    _summarize_and_excel(args.regulator, model_id, args.tab)


def cmd_excel(args):
    model_id = _resolve_model(args.model)
    _summarize_and_excel(args.regulator, model_id, args.tab)


def main():
    p = argparse.ArgumentParser(
        description="Test the autonomous crawling agent. Results come back as Excel.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("crawl", help="Onboard + crawl a regulator tab")
    c.add_argument("--regulator", required=True, help="Short name, e.g. SBP")
    c.add_argument("--tab", required=True, help="Section/tab name, e.g. Circulars")
    c.add_argument("--url", required=True, help="Landing page URL of that tab")
    c.add_argument("--model", default="claude", help="claude (default) or deepseek")
    c.add_argument("--source-system", default="", help="Optional label stored on each doc")
    c.add_argument("--quick", action="store_true", help="Fast small test sample only")
    c.set_defaults(func=cmd_crawl)

    f = sub.add_parser("feedback", help="Correct the agent in plain English and re-run")
    f.add_argument("--regulator", required=True)
    f.add_argument("--tab", required=True, help="Tab you're correcting (must match the crawl)")
    f.add_argument("--note", required=True, help="Plain-English correction")
    f.add_argument("--url", help="Optional page the feedback refers to")
    f.add_argument("--model", default="claude")
    f.set_defaults(func=cmd_feedback)

    e = sub.add_parser("excel", help="Rebuild/locate the Excel for the latest results")
    e.add_argument("--regulator", required=True)
    e.add_argument("--tab", required=True, help="Section/tab name, e.g. Circulars")
    e.add_argument("--model", default="claude")
    e.set_defaults(func=cmd_excel)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
