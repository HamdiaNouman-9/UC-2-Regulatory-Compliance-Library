"""SaudiExchangeCrawler -- Saudi Exchange (Tadawul) under the SAME fetch policy as SIMAH.

WHY THIS IS A SUBCLASS AND NOT A COPY

crawler/simah_wrapper.py owns a fetch POLICY -- serve a saved page, spend at most
one live visit per run and only when the saved page is due, never retry a block,
back off 6h/24h/72h/7d/14d, and refuse loudly once the saved page is past its
grace period. None of that is about SIMAH; it keys on the form's own name. Saudi
Exchange needs exactly the same protection for exactly the same reason:
saudiexchange.sa went from crawling cleanly to an Akamai 403 within two hours on
2026-08-15, after one crawl plus repeated probes from one address (see
config/change_signals.yml skip_hosts). A second copy of the policy would drift
from the first, and the policy is the one thing here that must not.

So this class changes only the defaults: which form, which names, and the label
that appears in logs.

WHAT IS DIFFERENT ABOUT THIS SITE

  * Akamai fingerprints headless Chromium (403 headless, 200 headed), and the
    form declares `requires_headed`. That applies to the ONE live capture only.
    A replay reads the saved file from disk and needs no window at all --
    dynamic_crawler/formfill/runner.py no longer forces one there -- so the
    scheduled job runs on a headless server. Only `capture()` needs a display
    (Xvfb on Linux), and that is run by hand the first time; see
    config/sources/saudi_exchange.yml.
  * The page is one screen holding Rules (10) and Procedures (9), each published
    as an Arabic and an English PDF, so a single saved page stands in for the
    whole source.

THERE IS NO SNAPSHOT UNTIL SOMEONE CAPTURES ONE. Until then every run raises
"has no snapshot ... Capture one" -- loudly, which is the point, and the reason
its scheduler slot ships disabled.
"""

from __future__ import annotations

from pathlib import Path

from crawler.simah_wrapper import REPO_ROOT, SimahCrawler

DEFAULT_HINTS = str(REPO_ROOT / "dynamic_crawler" / "hints" / "tadawul.rules.yml")


class SaudiExchangeCrawler(SimahCrawler):
    """The approved Saudi Exchange form + the snapshot fetch policy."""

    def __init__(self, regulator: str = "Saudi Exchange",
                 source_system: str = "Exchange Rules And Procedures",
                 hints_path: str = DEFAULT_HINTS,
                 label: str = "Saudi Exchange",
                 **kwargs):
        super().__init__(regulator=regulator, source_system=source_system,
                         hints_path=hints_path, label=label, **kwargs)


__all__ = ["SaudiExchangeCrawler"]
