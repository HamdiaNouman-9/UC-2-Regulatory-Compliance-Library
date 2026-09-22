"""Read-only sweep of every GET endpoint on a running server.

    python tests/api_smoke.py [--base http://127.0.0.1:8000] [--out output/api_report.json]

Writes nothing to the database and starts no job. For each GET route it records the
request actually sent, the status, the latency and the SHAPE of the response (keys and
types, lists shown by their first element), which is what an API document needs.
Path parameters are filled with real ids read from the server itself.
"""
import argparse
import json
import re
import time
import urllib.error
import urllib.request


def shape(v, depth=0):
    if isinstance(v, dict):
        if depth >= 3:
            return "{...}"
        return {k: shape(x, depth + 1) for k, x in list(v.items())[:25]}
    if isinstance(v, list):
        return [f"list[{len(v)}]", shape(v[0], depth + 1)] if v else "list[0]"
    if isinstance(v, str):
        return f"str({len(v)})"
    return type(v).__name__ if v is not None else "null"


def call(base, path, timeout=60):
    t0 = time.time()
    try:
        with urllib.request.urlopen(base + path, timeout=timeout) as r:
            body, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        body, status = e.read(), e.code
    except Exception as e:
        return {"path": path, "status": None, "error": f"{type(e).__name__}: {e}"[:200],
                "seconds": round(time.time() - t0, 2)}
    try:
        data = json.loads(body)
    except ValueError:
        data = None
    return {"path": path, "status": status, "seconds": round(time.time() - t0, 2),
            "shape": shape(data) if data is not None else f"non-json({len(body)} bytes)",
            "_data": data}


def first(data, *keys):
    """First id-looking value found in a list response."""
    rows = data if isinstance(data, list) else next(
        (data[k] for k in ("regulations", "items", "data", "results") if isinstance(data, dict) and k in data), [])
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        for k in keys:
            if k in rows[0]:
                return rows[0][k]
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--out", default="output/api_report.json")
    ap.add_argument("--regulation-id", type=int, default=9996, help="a regulation with versions/requirements")
    a = ap.parse_args()

    spec = json.load(urllib.request.urlopen(a.base + "/openapi.json"))
    gets = sorted(p for p, ms in spec["paths"].items() if "get" in ms)

    ids = {}
    ap_id = a.regulation_id
    if not ap_id:
        r = call(a.base, "/regulations?limit=1")
        groups = ((r.get("_data") or {}).get("data") or {}).values()
        for g in groups:
            if g.get("regulations"):
                ap_id = g["regulations"][0]["id"]
                break
    ids["regulation_id"] = ap_id
    r = call(a.base, "/categories/root")
    rows = (r.get("_data") or {}).get("data") or []
    ids["category_id"] = rows[0]["compliancecategory_id"] if rows else None
    ids["parent_id"] = ids["category_id"]
    if ap_id:
        v = (call(a.base, f"/regulation/{ap_id}/versions").get("_data") or {})
        vs = v.get("versions") or v.get("data") or []
        ids["version_id"] = vs[0].get("version_id") if vs and isinstance(vs[0], dict) else None
        rq = (call(a.base, f"/regulation/{ap_id}/requirements").get("_data") or {})
        rl = rq.get("requirements") or rq.get("data") or []
        ids["requirement_id"] = (rl[0].get("requirement_id") or rl[0].get("id")) if rl and isinstance(rl[0], dict) else None
    print("ids:", ids)

    fill = {**ids, "regulator": "SAMA", "job": "monitor_cbb", "session_id": 1}
    out, skipped = [], []
    for path in gets:
        params = re.findall(r"\{(\w+)\}", path)
        if any(fill.get(p) is None for p in params):
            skipped.append(path)
            continue
        url = re.sub(r"\{(\w+)\}", lambda m: str(fill[m.group(1)]), path)
        res = call(a.base, url, timeout=120)
        res["route"] = path
        out.append(res)
        print(f'{res["status"]!s:5} {res["seconds"]:>6}s  {url}')
    for path in skipped:
        print("skip ", path, "(needs an id this sweep could not discover)")

    for x in out:
        x.pop("_data", None)
    json.dump({"ids_used": ids, "results": out, "skipped": skipped}, open(a.out, "w", encoding="utf-8"),
              indent=1, default=str)
    bad = [x for x in out if x["status"] is None or x["status"] >= 500]
    print(f"\n{len(out)} called, {len(skipped)} skipped, {len(bad)} failing (5xx / no response) -> {a.out}")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
