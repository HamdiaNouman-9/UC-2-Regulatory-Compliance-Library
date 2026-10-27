"""Is this page the site, or a bot-protection wall pretending to be one?

A WAF challenge page returns HTTP 200, is valid HTML, and carries a plausible
amount of text. Nothing about it looks like a failure — SIMAH's Cloudflare block
was stored as 1,054 characters of "law", which is over the 200-character
threshold that decides a page is a document. So the check cannot be "did the
request fail"; it has to read the page.

Lives in its own module because two engines need it and the dependency may only
run one way: `generic_crawler` must not import from `dynamic_crawler`
(README: this folder is self-contained). `crawler.py` imports `strategies.py`,
never the reverse, so a helper both of them use cannot live in either one.

The outcome vocabulary that consumes this lives in `crawler.py::run_status`.
"""

import re

# Deliberately broad and case-insensitive. A false NEGATIVE stores a challenge
# page as a regulation; a false POSITIVE fails a run that someone then re-reads.
# The second is cheap, so the patterns lean towards firing.
BLOCK_RE = re.compile(
    r"(you have been blocked|attention required|cf-error-details|just a moment"
    r"|checking your browser|access denied|error 10\d\d|ddos protection"
    r"|request rejected"   
    r"|请稍候|captcha-bypass|verify you are (a )?human)", re.I)

THROTTLE_RE = re.compile(
    r"(too many requests|you are being rate limited|rate limit exceeded"
    r"|error 1015)",re.I
)    

# What we read off the page. The title matters as much as the body: Cloudflare's
# is "Attention Required! | Cloudflare" while the body can be nearly empty.
_PROBE_JS = ("()=>({t: document.title || '',"
             "      x: (document.body ? document.body.innerText : '').slice(0, 3000)})")


def blocked_reason(page) -> str:
    """A short reason when `page` is a bot-protection wall, else "".

    Takes a Playwright page (or anything with .evaluate). A probe that throws
    returns "" — the page could not be read, which is a load failure for the
    caller to record, not evidence of a block.
    """
    try:
        probe = page.evaluate(_PROBE_JS)
    except Exception:
        return ""
    m = BLOCK_RE.search(f"{probe.get('t', '')}\n{probe.get('x', '')}")
    return m.group(0)[:60] if m else ""


def text_is_blocked(title: str, body: str) -> str:
    """Same test against text already in hand — for tests, and for callers that
    have read the page for another reason."""
    m = BLOCK_RE.search(f"{title or ''}\n{body or ''}")
    return m.group(0)[:60] if m else ""

def text_is_throttled(title: str,body: str)-> str:
    m=THROTTLE_RE.search(f"{title or ''}\n{body or ''}")
    return m.group(0)[:60] if m else ""


__all__ = ["BLOCK_RE","THROTTLE_RE", "blocked_reason", "text_is_blocked","text_is_throttled"]
