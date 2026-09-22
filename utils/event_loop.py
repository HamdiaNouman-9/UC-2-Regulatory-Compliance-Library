"""One place for the Windows event-loop workaround Playwright needs in-process.

Playwright's sync API creates its own loop with `asyncio.new_event_loop()`, which
uses whatever policy is current. `scheduler/scheduler.py` forces
WindowsSelectorEventLoopPolicy at import (Twisted/Scrapy's asyncioreactor needs
it), and `apis/pipeline_api.py` imports that module -- so the policy is active in
every thread of the API process. A SelectorEventLoop cannot launch a subprocess on
Windows, so launching Chromium from inside the API process fails with
NotImplementedError from asyncio.base_events._make_subprocess_transport, before a
browser ever opens.

Measured 2026-09-18 on CMA: 0 documents in 5 seconds, reported as a clean empty run.
Measured 2026-09-21 on SIMAH's snapshot replay, which calls the same runner
in-process: the identical error.

Scoped to the `with` block and restored in `finally`, rather than swapped
process-wide. Verified not to disturb Twisted's already-installed reactor: it
captured its own loop at import time and never re-reads the policy. On any other
platform this does nothing.

Anything that launches Playwright IN the API process needs this. Crawls that run
as their own subprocess (MC, and every `mode: generic` source) do not.
"""

from __future__ import annotations

import asyncio
import sys
from contextlib import contextmanager


@contextmanager
def proactor_event_loop():
    if not sys.platform.startswith("win"):
        yield
        return
    old_policy = asyncio.get_event_loop_policy()
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        yield
    finally:
        asyncio.set_event_loop_policy(old_policy)


__all__ = ["proactor_event_loop"]
