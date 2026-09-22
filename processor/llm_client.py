"""
Shared OpenRouter client.

`staged_LLM_Analyzer.py`, `requirement_matcher.py` and `gap_analyzer.py` each
grew their own `_call_llm`. Only one of the three had retry, truncation
detection, a concurrency bound or determinism settings -- so the other two were
exposed to exactly the failure modes that were fixed in the analyzer:

  * no retry            -> one 429 loses the whole document
  * no finish_reason    -> a truncated reply is parsed as if it were complete
  * no concurrency cap  -> parallel callers stampede into rate limits
  * no provider pinning -> calls land on different engines and quantizations
  * env assumed loaded  -> matcher/gap_analyzer only work when imported after
                           pipeline_api has already called load_dotenv()

This module is the single place all of that lives.

Usage
-----
    from processor.llm_client import LLMClient, TruncatedResponseError

    client = LLMClient()
    text = client.complete(prompt, max_tokens=800, expect_json=True)

Configuration (environment, all optional):
    OPENROUTER_API_KEY     required
    LLM_MAX_CONCURRENCY    default 8    total in-flight calls, process-wide
    LLM_DETERMINISTIC      default 1    temperature 0, top_p 1, seed, pinned provider
    LLM_PROVIDER           default AtlasCloud
    LLM_QUANTIZATION       default fp8
    LLM_ALLOW_FALLBACKS    default 0    0 = fail loudly rather than silently reroute
    LLM_SEED               default 20250101

See docs/determinism.md for why pinning matters and, more importantly, what it
does not fix.
"""

import logging
import os
import threading
import time
from typing import Optional

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# No override= here on purpose: a stale .env on disk must not beat real
# environment variables injected by a container, CI or systemd.
load_dotenv()

ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "deepseek/deepseek-v3.2"

# Process-wide, shared by every LLMClient instance and every worker thread, so
# document-level and stage-level parallelism together cannot exceed this.
_MAX_CONCURRENCY = int(os.getenv("LLM_MAX_CONCURRENCY", "8"))
_SEMAPHORE = threading.Semaphore(_MAX_CONCURRENCY)

_DETERMINISTIC   = os.getenv("LLM_DETERMINISTIC", "1") not in ("0", "false", "False")
_PROVIDER        = os.getenv("LLM_PROVIDER", "AtlasCloud")
_QUANTIZATION    = os.getenv("LLM_QUANTIZATION", "fp8")
_ALLOW_FALLBACKS = os.getenv("LLM_ALLOW_FALLBACKS", "0") not in ("0", "false", "False")
_SEED            = int(os.getenv("LLM_SEED", "20250101"))

# Transient conditions worth another attempt.
RETRY_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}

DEFAULT_SYSTEM_PROMPT = (
    "You are a senior banking compliance officer specialising in SAMA and CMA regulations. "
    "Return only valid JSON unless explicitly told otherwise. Never hallucinate. "
    "Always respond in the same language as the source document."
)


class TruncatedResponseError(RuntimeError):
    """The model hit max_tokens; the payload is incomplete and must not be parsed."""


class StructuralLLMError(RuntimeError):
    """Every one of `attempts` retries failed with the same auth/connectivity-
    class error -- not a one-off. A bad API key or an unreachable endpoint
    will fail every subsequent call identically, so a caller processing many
    chunks/batches should stop rather than spend the rest of the run on calls
    equally certain to fail. Distinct from every other failure this client
    raises, which stays a per-call problem a caller can reasonably treat as
    "this one chunk had an issue" -- see requirement_analyzer.py and
    activity_analyzer.py, which re-raise this specifically rather than
    swallowing it like every other exception."""


def _is_structural(exc: Exception) -> bool:
    """Bad credentials or an unreachable endpoint -- conditions where retrying
    the SAME call again is certain to fail again, unlike a rate limit or a
    transient 5xx (both already in RETRY_STATUS and retried before this is
    ever consulted)."""
    if isinstance(exc, requests.exceptions.ConnectionError):
        return True
    if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        return exc.response.status_code in (401, 403)
    return False


class LLMClient:
    """One OpenRouter caller with retry, truncation detection and a shared
    concurrency bound. Stateless apart from configuration -- safe to share
    across threads."""

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        system_prompt: str = DEFAULT_SYSTEM_PROMPT,
        deterministic: Optional[bool] = None,
        provider: Optional[str] = None,
        quantization: Optional[str] = None,
        seed: Optional[int] = None,
        allow_fallbacks: Optional[bool] = None,
        referer: str = "http://localhost:3000",
        title: str = "Saudi Banking Compliance Copilot",
        timeout: int = 180,
    ):
        self.model = model
        self.system_prompt = system_prompt
        self.deterministic = _DETERMINISTIC if deterministic is None else deterministic
        self.provider = provider or _PROVIDER
        self.quantization = quantization or _QUANTIZATION
        self.seed = _SEED if seed is None else seed
        self.allow_fallbacks = _ALLOW_FALLBACKS if allow_fallbacks is None else allow_fallbacks
        self.referer = referer
        self.title = title
        self.timeout = timeout
        # Called with one dict per successful response -- see complete(). Set by
        # whoever owns a database; this module stays DB-free.
        self.on_usage = None
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError("Missing OPENROUTER_API_KEY environment variable")

    # ------------------------------------------------------------------ #

    def _build_payload(self, prompt: str, temperature: float, max_tokens: int,
                       expect_json: bool) -> dict:
        payload = {
            "model": self.model,
            # Sampling above 0 is itself a source of run-to-run variance.
            "temperature": 0 if self.deterministic else temperature,
            "max_tokens": max_tokens,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": prompt},
            ],
        }
        if expect_json:
            payload["response_format"] = {"type": "json_object"}
        # Ask OpenRouter to include the dollar cost in `usage`.
        payload["usage"] = {"include": True}
        if self.deterministic:
            payload["top_p"] = 1
            payload["seed"] = self.seed
        # The provider pin (AtlasCloud, fp8) was chosen for DEFAULT_MODEL. Another
        # model is usually not served there, so pinning it fails every call with
        # "no endpoints found". Seed and temperature 0 still apply; only the
        # engine is left to OpenRouter, so run-to-run determinism is weaker.
        if self.deterministic and self.model == DEFAULT_MODEL:
            payload["provider"] = {
                "order": [self.provider],
                "allow_fallbacks": self.allow_fallbacks,
                "quantizations": [self.quantization],
            }
        return payload

    def complete(
        self,
        prompt: str,
        temperature: float = 0.1,
        max_tokens: int = 8000,
        expect_json: bool = False,
        attempts: int = 3,
        label: str = "",
    ) -> str:
        """Return the model's text. Raises TruncatedResponseError if the reply
        hit max_tokens, so callers can shard and retry rather than parse a
        half-written payload."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": self.referer,
            "X-Title": self.title,
        }
        payload = self._build_payload(prompt, temperature, max_tokens, expect_json)
        tag = f"[{label}] " if label else ""
        last_exc = None

        for attempt in range(attempts):
            if attempt:
                time.sleep(min(2 ** attempt, 8))
            try:
                with _SEMAPHORE:
                    resp = requests.post(ENDPOINT, headers=headers, json=payload,
                                         timeout=self.timeout)

                if resp.status_code in RETRY_STATUS:
                    last_exc = RuntimeError(
                        f"OpenRouter HTTP {resp.status_code}: {resp.text[:200]}")
                    logger.warning(f"{tag}{last_exc} (attempt {attempt + 1}/{attempts})")
                    continue
                resp.raise_for_status()

                body = resp.json()
                choice = (body.get("choices") or [{}])[0]

                usage = body.get("usage") or {}
                if usage:
                    cached = (usage.get("prompt_tokens_details") or {}).get("cached_tokens")
                    logger.info(
                        f"{tag}tokens in={usage.get('prompt_tokens')} "
                        f"out={usage.get('completion_tokens')}"
                        + (f" cached={cached}" if cached else "")
                        + f" provider={body.get('provider')}")
                    if self.on_usage:
                        try:
                            self.on_usage({
                                "model": body.get("model") or self.model,
                                "provider": body.get("provider"),
                                "prompt_tokens": usage.get("prompt_tokens"),
                                "completion_tokens": usage.get("completion_tokens"),
                                "cached_tokens": cached,
                                "cost": usage.get("cost")})
                        except Exception as e:
                            logger.warning(f"{tag}usage callback failed: {e}")

                if choice.get("finish_reason") == "length":
                    raise TruncatedResponseError(
                        f"{tag}hit max_tokens={max_tokens}; response incomplete")

                content = (choice.get("message") or {}).get("content")
                return content.strip() if content else ""

            except TruncatedResponseError:
                raise
            except requests.exceptions.RequestException as e:
                last_exc = e
                logger.warning(f"{tag}OpenRouter request error: {e} "
                               f"(attempt {attempt + 1}/{attempts})")

        logger.error(f"{tag}OpenRouter failed after {attempts} attempts: {last_exc}")
        if last_exc is not None and _is_structural(last_exc):
            raise StructuralLLMError(
                f"{tag}every attempt failed the same way (likely a bad API key or "
                f"unreachable endpoint), not a transient error: {last_exc}"
            ) from last_exc
        raise last_exc if last_exc else RuntimeError("OpenRouter call failed")
