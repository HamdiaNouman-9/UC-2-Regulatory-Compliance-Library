"""
GapAnalyzer benchmark harness.

Feeds a fixed document + a fixed obligation list through `analyze_gaps()` and
records timing, tokens and the coverage verdicts, so a refactor can be shown to
change nothing.

The analyzer is NOT modified -- instrumentation wraps `requests.post`. Both the
gap_analyzer module and the shared llm_client are patched (deduped on the
requests module itself), so this keeps working after the client refactor.

Usage
-----
  python benchmarks/gap_bench.py --label gap_base1
  python benchmarks/gap_bench.py --compare gap_base1 gap_client
"""

import argparse
import collections
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
os.chdir(REPO)

from dotenv import load_dotenv
load_dotenv(REPO / ".env", override=True)

RUNS_DIR = Path(__file__).resolve().parent / "runs"
PRICE_PROMPT_PER_M = float(os.getenv("BENCH_PRICE_PROMPT_PER_M", "0.247"))
PRICE_COMPLETION_PER_M = float(os.getenv("BENCH_PRICE_COMPLETION_PER_M", "0.384"))


class CallRecorder:
    def __init__(self):
        self.calls = []
        self._patched = []

    def install(self):
        import processor.gap_analyzer as gap_mod
        try:
            import processor.llm_client as client_mod
        except ImportError:
            client_mod = None
        rec = self

        def make(orig):
            def instrumented(url, *args, **kwargs):
                payload = kwargs.get("json") or {}
                prompt = ""
                for m in payload.get("messages") or []:
                    if m.get("role") == "user":
                        prompt = m.get("content", "")
                t0 = time.perf_counter()
                resp = orig(url, *args, **kwargs)
                rec.calls.append(rec._record(resp, prompt, time.perf_counter() - t0, payload))
                return resp
            return instrumented

        # Both modules `import requests`, so mod.requests is the same object.
        # Patch it once or every call gets counted twice.
        seen = set()
        for mod in (gap_mod, client_mod):
            if mod is None or not hasattr(mod, "requests"):
                continue
            rq = mod.requests
            if id(rq) in seen:
                continue
            seen.add(id(rq))
            orig = rq.post
            self._patched.append((rq, orig))
            rq.post = make(orig)

    def _record(self, resp, prompt, elapsed, payload):
        r = {
            "index": len(self.calls) + 1,
            "seconds": round(elapsed, 2),
            "http_status": resp.status_code,
            "max_tokens_requested": payload.get("max_tokens"),
            "prompt_chars": len(prompt),
            "prompt": prompt,
            "completion": "",
            "prompt_tokens": None,
            "completion_tokens": None,
            "finish_reason": None,
        }
        try:
            b = resp.json()
            u = b.get("usage") or {}
            r["prompt_tokens"] = u.get("prompt_tokens")
            r["completion_tokens"] = u.get("completion_tokens")
            ch = (b.get("choices") or [{}])[0]
            r["finish_reason"] = ch.get("finish_reason")
            r["completion"] = (ch.get("message") or {}).get("content") or ""
            r["provider"] = b.get("provider")
        except Exception as e:
            r["instrumentation_error"] = str(e)
        return r

    def uninstall(self):
        for rq, orig in self._patched:
            rq.post = orig
        self._patched = []


def build_inputs(from_run: str, doc_file: str):
    """Obligations from a saved analyzer run, checked against a fixed document."""
    rows = json.loads((RUNS_DIR / from_run / "rows.json").read_text(encoding="utf-8"))
    requirements = []
    for r in rows:
        s2 = r.get("stage2_json") or {}
        if isinstance(s2, str):
            try:
                s2 = json.loads(s2)
            except Exception:
                s2 = {}
        for ob in s2.get("normalized_obligations", []):
            requirements.append({
                "requirement_text": ob["obligation_text"],
                "obligation_id":    ob["obligation_id"],
            })
    text = Path(doc_file).read_text(encoding="utf-8")
    return text, requirements


def run(label: str, from_run: str, doc_file: str, model: str = None):
    out = RUNS_DIR / label
    (out / "calls").mkdir(parents=True, exist_ok=True)

    text, requirements = build_inputs(from_run, doc_file)
    print(f"  uploaded doc chars : {len(text):,}")
    print(f"  obligations to check: {len(requirements)}")

    from processor.gap_analyzer import GapAnalyzer

    rec = CallRecorder()
    rec.install()
    analyzer = GapAnalyzer(model=model) if model else GapAnalyzer()
    print(f"  model              : {analyzer.model}")
    print(f"  max_chunk_size     : {analyzer.max_chunk_size:,} "
          f"-> {'CHUNKED' if len(text) > analyzer.max_chunk_size else 'single pass'}")

    print(f"Running analyze_gaps() [label={label}] ...")
    t0 = time.perf_counter()
    error, results = None, []
    try:
        results = analyzer.analyze_gaps(uploaded_text=text, requirements=requirements)
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        print(f"  !! raised: {error}")
    finally:
        wall = time.perf_counter() - t0
        rec.uninstall()

    print(f"  wall clock: {wall:.1f}s across {len(rec.calls)} LLM call(s)")

    for c in rec.calls:
        stem = f"{c['index']:03d}"
        (out / "calls" / f"{stem}_prompt.txt").write_text(c["prompt"], encoding="utf-8")
        (out / "calls" / f"{stem}_completion.txt").write_text(c["completion"], encoding="utf-8")

    pt = sum(c["prompt_tokens"] or 0 for c in rec.calls)
    ct = sum(c["completion_tokens"] or 0 for c in rec.calls)
    statuses = collections.Counter(r.get("coverage_status") for r in results)

    metrics = {
        "label": label,
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "from_run": from_run,
        "doc_file": str(doc_file),
        "error": error,
        "inputs": {"doc_chars": len(text), "obligations": len(requirements)},
        "totals": {
            "wall_seconds": round(wall, 2),
            "llm_calls": len(rec.calls),
            "prompt_tokens": pt,
            "completion_tokens": ct,
            "total_tokens": pt + ct,
            "cost_estimated_usd": round(pt * PRICE_PROMPT_PER_M / 1e6
                                        + ct * PRICE_COMPLETION_PER_M / 1e6, 6),
            "truncated_calls": sum(1 for c in rec.calls if c["finish_reason"] == "length"),
        },
        "output": {
            "results_returned": len(results),
            "obligations_in": len(requirements),
            "missing_results": len(requirements) - len(results),
            "coverage_status": dict(statuses),
            "with_evidence": sum(1 for r in results if (r.get("evidence_text") or "").strip()),
            "empty_gap_descriptions": sum(
                1 for r in results if not (r.get("gap_description") or "").strip()),
        },
        "calls": [{k: v for k, v in c.items() if k not in ("prompt", "completion")}
                  for c in rec.calls],
    }

    (out / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
    (out / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
    (out / "summary.md").write_text(render(metrics), encoding="utf-8")
    print("\n" + render(metrics))
    print(f"Written to {out}")


def render(m):
    t, o = m["totals"], m["output"]
    lines = [f"# Gap analyzer run: {m['label']}", ""]
    if m.get("error"):
        lines.append(f"- **ERROR**: {m['error']}")
    lines += [
        f"- Document       : {m['inputs']['doc_chars']:,} chars",
        f"- Obligations    : {m['inputs']['obligations']}",
        f"- Run at         : {m['run_at_utc']}",
        "", "## Cost and latency", "", "| Metric | Value |", "|---|---:|",
        f"| Wall clock (s) | {t['wall_seconds']:,.1f} |",
        f"| LLM calls | {t['llm_calls']} |",
        f"| Prompt tokens | {t['prompt_tokens']:,} |",
        f"| Completion tokens | {t['completion_tokens']:,} |",
        f"| Total tokens | {t['total_tokens']:,} |",
        f"| Cost USD (est) | {t['cost_estimated_usd']:.4f} |",
        f"| **Truncated calls** | **{t['truncated_calls']}** |",
        "", "## Output", "",
        f"- Results returned : {o['results_returned']} of {o['obligations_in']}",
        f"- Missing results  : {o['missing_results']}",
        f"- Coverage status  : {json.dumps(o['coverage_status'], ensure_ascii=False)}",
        f"- With evidence    : {o['with_evidence']}",
    ]
    return "\n".join(lines) + "\n"


def compare(a, b):
    ma = json.loads((RUNS_DIR / a / "metrics.json").read_text(encoding="utf-8"))
    mb = json.loads((RUNS_DIR / b / "metrics.json").read_text(encoding="utf-8"))
    lines = [f"# Gap analyzer comparison: {a} -> {b}", ""]
    for lbl, m in ((a, ma), (b, mb)):
        if m.get("error"):
            lines += [f"> **{lbl} FAILED — numbers meaningless.** `{m['error']}`", ""]
        elif m["totals"]["llm_calls"] == 0:
            lines += [f"> **{lbl} made zero LLM calls.**", ""]

    def d(x, y):
        return "-" if not x else f"{(y - x) / x * 100:+.1f}%"

    lines += ["## Cost and latency", "", f"| Metric | {a} | {b} | Change |", "|---|---:|---:|---:|"]
    for k, n in [("wall_seconds", "Wall clock (s)"), ("llm_calls", "LLM calls"),
                 ("prompt_tokens", "Prompt tokens"), ("completion_tokens", "Completion tokens"),
                 ("total_tokens", "Total tokens"), ("cost_estimated_usd", "Cost USD"),
                 ("truncated_calls", "Truncated")]:
        va, vb = ma["totals"].get(k), mb["totals"].get(k)
        lines.append(f"| {n} | {va:,} | {vb:,} | {d(va, vb)} |")

    ra = {r.get("obligation_text"): r.get("coverage_status")
          for r in json.loads((RUNS_DIR / a / "results.json").read_text(encoding="utf-8"))}
    rb = {r.get("obligation_text"): r.get("coverage_status")
          for r in json.loads((RUNS_DIR / b / "results.json").read_text(encoding="utf-8"))}
    shared = set(ra) & set(rb)
    agree = sum(1 for k in shared if ra[k] == rb[k])

    lines += ["", "## Coverage verdicts", "",
              f"- {a}: {json.dumps(ma['output']['coverage_status'], ensure_ascii=False)}",
              f"- {b}: {json.dumps(mb['output']['coverage_status'], ensure_ascii=False)}",
              "", "## Per-obligation agreement", "",
              f"- Present in both: {len(shared)}",
              f"- Identical verdict: {agree}"
              + (f" ({agree/len(shared)*100:.0f}%)" if shared else ""),
              f"- Differing: {len(shared) - agree}"]
    if shared and agree < len(shared):
        lines += ["", "Differences:", ""]
        for k in shared:
            if ra[k] != rb[k]:
                lines.append(f"- `{k[:70]}...`  {a}={ra[k]}  {b}={rb[k]}")

    text = "\n".join(lines) + "\n"
    dest = RUNS_DIR / f"compare_{a}_vs_{b}.md"
    dest.write_text(text, encoding="utf-8")
    print(text)
    print(f"Written to {dest}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--model", default=None,
                   help="model id to test, e.g. qwen/qwen-2.5-72b-instruct. "
                        "Defaults to GapAnalyzer's own default. Lets a CANDIDATE "
                        "model be measured through OpenRouter before any hosting "
                        "decision is made -- quality first, infrastructure second.")
    p.add_argument("--label")
    p.add_argument("--from-run", default="optimized")
    p.add_argument("--doc-file",
                   default=str(RUNS_DIR / "baseline" / "input_clean_text.txt"))
    p.add_argument("--compare", nargs=2, metavar=("A", "B"))
    a = p.parse_args()
    if a.compare:
        compare(*a.compare)
        return
    if not a.label:
        p.error("--label is required (or --compare A B)")
    run(a.label, a.from_run, a.doc_file, a.model)


if __name__ == "__main__":
    main()
