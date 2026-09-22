"""Which OpenRouter model the analysis uses, and what the analysis spent.

Two tables, created on first use (same pattern as run_history):

    llm_settings   key/value. One row today: key='model'.
    llm_usage      one row per successful OpenRouter call made BY THIS APP.

Two views of usage, deliberately separate:
    usage_summary      this app only (llm_usage), counting from when it was added
    openrouter_usage   everything on the API key, per OpenRouter, all-time

Lives beside mssql_repo rather than in it and takes the repo as an argument, so
processor/llm_client.py stays DB-free -- it reports each call through `on_usage`.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Callable, Optional

import requests

from processor.llm_client import DEFAULT_MODEL

logger = logging.getLogger(__name__)

MODELS_URL = "https://openrouter.ai/api/v1/models"
_MODELS_TTL = 3600
_models_cache: dict = {"at": 0.0, "models": []}


def _ensure(cur) -> None:
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='llm_settings' AND xtype='U')
        CREATE TABLE llm_settings (
            [key]      NVARCHAR(50)  NOT NULL PRIMARY KEY,
            [value]    NVARCHAR(200) NOT NULL,
            updated_at DATETIME      NOT NULL DEFAULT GETUTCDATE())""")
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM sysobjects WHERE name='llm_usage' AND xtype='U')
        CREATE TABLE llm_usage (
            id                INT IDENTITY(1,1) PRIMARY KEY,
            created_at        DATETIME      NOT NULL DEFAULT GETUTCDATE(),
            model             NVARCHAR(200) NOT NULL,
            provider          NVARCHAR(100) NULL,
            prompt_tokens     INT           NOT NULL DEFAULT 0,
            completion_tokens INT           NOT NULL DEFAULT 0,
            cached_tokens     INT           NOT NULL DEFAULT 0,
            cost_usd          DECIMAL(18,8) NULL,
            step              NVARCHAR(100) NULL,
            regulation_id     INT           NULL,
            version_id        INT           NULL)""")


# ------------------------------------------------------------------ model --

def get_model(repo) -> str:
    """Saved choice, else LLM_MODEL from the environment, else the default.
    Never raises: a settings-table problem must not stop an analysis run."""
    try:
        with repo._get_conn() as conn:
            cur = conn.cursor()
            _ensure(cur)
            cur.execute("SELECT [value] FROM llm_settings WHERE [key] = 'model'")
            row = cur.fetchone()
            conn.commit()
            if row and row[0]:
                return row[0]
    except Exception as e:
        logger.error("get_model failed, using fallback: %s", e)
    return os.getenv("LLM_MODEL") or DEFAULT_MODEL


def set_model(repo, model: str) -> None:
    with repo._get_conn() as conn:
        cur = conn.cursor()
        _ensure(cur)
        cur.execute("UPDATE llm_settings SET [value]=?, updated_at=GETUTCDATE() "
                    "WHERE [key]='model'", (model,))
        if cur.rowcount == 0:
            cur.execute("INSERT INTO llm_settings ([key],[value]) VALUES ('model', ?)",
                        (model,))
        conn.commit()


def list_models(refresh: bool = False) -> list:
    """OpenRouter's public catalogue, cached for an hour. Raises on a failed
    fetch when there is nothing cached, so a caller never validates against an
    empty list."""
    if not refresh and _models_cache["models"] and \
            time.time() - _models_cache["at"] < _MODELS_TTL:
        return _models_cache["models"]
    try:
        r = requests.get(MODELS_URL, timeout=20)
        r.raise_for_status()
        models = [{"id": m["id"], "name": m.get("name"),
                   "context_length": m.get("context_length"),
                   # USD per token, as strings, straight from OpenRouter
                   "prompt_price": (m.get("pricing") or {}).get("prompt"),
                   "completion_price": (m.get("pricing") or {}).get("completion")}
                  for m in r.json().get("data", [])]
    except Exception:
        if _models_cache["models"]:      # stale beats nothing
            return _models_cache["models"]
        raise
    _models_cache.update(at=time.time(), models=models)
    return models


# ------------------------------------------------------------------ usage --

def openrouter_usage() -> dict:
    """Spend as OpenRouter reports it, for the API key in OPENROUTER_API_KEY.

    /key: this key's usage (total/daily/weekly/monthly) and limit.
    /credits: the account's credits bought and used. Each is fetched on its own
    and a failure is reported under `errors` rather than failing the other.
    This is EVERYTHING that uses the key, not only analysis.
    """
    headers = {"Authorization": f"Bearer {os.getenv('OPENROUTER_API_KEY', '')}"}
    out: dict = {"errors": {}}
    for name, path in (("key", "key"), ("credits", "credits")):
        try:
            r = requests.get(f"https://openrouter.ai/api/v1/{path}",
                             headers=headers, timeout=20)
            r.raise_for_status()
            out[name] = r.json().get("data")
        except Exception as e:
            out["errors"][name] = str(e)
    return out


def make_usage_recorder(repo, **context) -> Callable[[dict], None]:
    """A callback for LLMClient.on_usage. `context` (step, regulation_id,
    version_id) is stamped on every row. Swallows its own failures: losing a
    usage row is better than losing the analysis it describes."""
    def record(u: dict) -> None:
        try:
            with repo._get_conn() as conn:
                cur = conn.cursor()
                _ensure(cur)
                cur.execute(
                    "INSERT INTO llm_usage (model, provider, prompt_tokens, "
                    "completion_tokens, cached_tokens, cost_usd, step, "
                    "regulation_id, version_id) VALUES (?,?,?,?,?,?,?,?,?)",
                    (u.get("model"), u.get("provider"),
                     int(u.get("prompt_tokens") or 0),
                     int(u.get("completion_tokens") or 0),
                     int(u.get("cached_tokens") or 0),
                     u.get("cost"), context.get("step"),
                     context.get("regulation_id"), context.get("version_id")))
                conn.commit()
        except Exception as e:
            logger.error("could not record llm usage: %s", e)
    return record


def usage_summary(repo, since: Optional[str] = None, until: Optional[str] = None,
                  group_by: str = "model") -> dict:
    """This app's calls, grouped by model, day, step or regulation. `since` and
    `until` are ISO dates; `until` is exclusive."""
    keys = {"model": "model",
            "day": "CONVERT(VARCHAR(10), created_at, 23)",
            "step": "step",
            "regulation": "CAST(regulation_id AS VARCHAR(20))"}
    if group_by not in keys:
        raise ValueError(f"group_by must be one of {sorted(keys)}")
    where, params = [], []
    if since:
        where.append("created_at >= ?"); params.append(since)
    if until:
        where.append("created_at < ?"); params.append(until)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    with repo._get_conn() as conn:
        cur = conn.cursor()
        _ensure(cur)
        cur.execute(
            f"SELECT {keys[group_by]}, COUNT(*), SUM(prompt_tokens), "
            f"SUM(completion_tokens), SUM(cached_tokens), SUM(cost_usd), "
            f"MIN(created_at) FROM llm_usage {clause} "
            f"GROUP BY {keys[group_by]} ORDER BY 1", tuple(params))
        raw = cur.fetchall()
        cur.execute("SELECT MIN(created_at) FROM llm_usage")
        first = cur.fetchone()[0]
        conn.commit()
    rows = [{"group": r[0], "calls": r[1], "prompt_tokens": int(r[2] or 0),
             "completion_tokens": int(r[3] or 0), "cached_tokens": int(r[4] or 0),
             "cost_usd": float(r[5]) if r[5] is not None else None} for r in raw]
    total = {k: sum(r[k] for r in rows)
             for k in ("calls", "prompt_tokens", "completion_tokens", "cached_tokens")}
    costs = [r["cost_usd"] for r in rows if r["cost_usd"] is not None]
    total["cost_usd"] = round(sum(costs), 6) if costs else None
    return {"group_by": group_by, "since": since, "until": until,
            "tracking_since": first.isoformat() if first else None,
            "rows": rows, "total": total}
