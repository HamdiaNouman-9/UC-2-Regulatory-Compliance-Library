"""The country a regulator files under, for the folder tree only.

READ config/countries.yml FIRST — it carries the reasoning, and the one rule
that matters: the country is prepended when BUILDING THE TREE and is never put
into `doc_path`, because `doc_path` is an identity field and changing it would
reclassify the entire library in a single run.

Three call sites build folders and all three come here, for the same reason
`changesignal.find_existing` is the single implementation of "is this the same
document?": two copies of a rule drift, and the drift is invisible until the
tree has duplicates in it.

    orchestrator/orchestrator.py     the direct-write crawl path
    dynamic_crawler/formfill/orch.py the formfill/monitoring path
    dynamic_crawler/formfill/promote.py  replaying a workbook
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG = REPO_ROOT / "config" / "countries.yml"

_CACHE: Optional[Dict[str, str]] = None
_WARNED: set = set()


def _load() -> Dict[str, str]:
    """{regulator name -> country}, inverted from the country->regulators file.

    Cached: this is read once per document otherwise, and the file does not
    change while a run is in flight.
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    mapping: Dict[str, str] = {}
    try:
        raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8")) or {}
    except FileNotFoundError:
        logger.warning("config/countries.yml missing — every regulator will "
                       "stay at the tree root")
        _CACHE = {}
        return _CACHE
    for country, regulators in (raw.get("countries") or {}).items():
        for reg in (regulators or []):
            name = str(reg).strip()
            if not name:
                continue
            if name in mapping and mapping[name] != country:
                # Two countries claiming one regulator is a config error, not a
                # thing to resolve silently — the folder would land in whichever
                # happened to be parsed last.
                raise ValueError(
                    f"config/countries.yml lists {name!r} under both "
                    f"{mapping[name]!r} and {country!r}")
            mapping[name] = str(country).strip()
    _CACHE = mapping
    return _CACHE


def country_for(regulator: str) -> Optional[str]:
    """The country this regulator files under, or None if it is not listed.

    None means "leave it where it is". A regulator missing from the config keeps
    its old place at the tree root rather than being guessed at — a wrong
    country is harder to notice than a missing one, because the folder still
    exists and still holds the documents.
    """
    name = str(regulator or "").strip()
    hit = _load().get(name)
    if not hit and name and name not in _WARNED:
        # Once per regulator per process. Silence here is what lets a regulator
        # sit at the tree root for months: the documents are all fine, so nothing
        # else complains. `tools/workbook check` says the same thing louder, at
        # the moment a person is actually looking.
        _WARNED.add(name)
        logger.warning("%r is not in config/countries.yml — its folders will "
                       "stay at the tree root. Add it under a country, matching "
                       "this name exactly.", name)
    return hit


def tree_path(doc_path: List[str], regulator: str = "") -> List[str]:
    """`doc_path` with its country prepended — the hierarchy for the TREE.

    Pass the result to `_get_or_create_compliance_category`. Do NOT assign it
    back to `doc.doc_path`: that column is an identity field and must keep
    starting at the regulator.

    The regulator is taken from `doc_path[0]` when not given, which is what
    every crawler already puts there (`generic_crawler_wrapper` documents that
    doc_path "ALWAYS starts with the regulator").
    """
    path = [p for p in (doc_path or []) if str(p).strip()]
    if not path:
        return path
    country = country_for(regulator or path[0])
    if not country:
        return path
    if path[0] == country:            # already prefixed; do not double it
        return path
    return [country] + path


def countries() -> List[str]:
    """Every country named in the config, for the migration's own assertions."""
    return sorted(set(_load().values()))


def regulators() -> Dict[str, str]:
    """The full {regulator -> country} mapping."""
    return dict(_load())


def regulators_for_country(country: str) -> List[str]:
    """Every regulator filed under one country name (exact, as it appears in
    countries.yml -- resolve with resolve_country() first if the value might
    be a code or a different case)."""
    return [reg for reg, c in _load().items() if c == country]


def resolve_country(value: str) -> Optional[str]:
    """A caller-supplied country filter value -- either the exact name from
    countries.yml or its alpha-3 code, either case-insensitive -- resolved to
    the canonical name _load()/regulators_for_country() key on. None if it
    matches neither, so the caller (an API endpoint) can tell a typo apart
    from a real, just-empty country."""
    v = (value or "").strip()
    if not v:
        return None
    for name in countries():
        if name.casefold() == v.casefold():
            return name
    v_upper = v.upper()
    for name, code in _COUNTRY_CODES.items():
        if code == v_upper:
            return name
    return None


# ISO 3166-1 alpha-3, keyed on the exact country names used in
# config/countries.yml -- for storage/mssql_repo.py::compute_regulation_ref_key.
# Add the new country's alpha-3 code here in the SAME change that adds it to
# countries.yml; a country present in one but not the other silently falls
# back to _UNKNOWN_COUNTRY_CODE below instead of erroring, which is easy to
# miss -- same failure shape country_for() itself warns about for regulators.
_COUNTRY_CODES: Dict[str, str] = {
    "Kingdom of Saudi Arabia": "SAU",
    "Egypt": "EGY",
    "Bahrain": "BHR",
    "Qatar": "QAT",
}

# Matches regulator_acronym()'s own "UNK" convention in mssql_repo.py, for a
# regulator with no entry in countries.yml at all -- e.g. SBP/SECP today,
# since Pakistan is not yet listed there. Not a country code; a placeholder
# that says "unlisted", so it reads as obviously wrong rather than as a real
# code for the wrong country.
_UNKNOWN_COUNTRY_CODE = "UNK"


def country_code_for(regulator: str) -> str:
    """ISO 3166-1 alpha-3 for the country this regulator files under, or
    _UNKNOWN_COUNTRY_CODE if the regulator (or its country) isn't in
    countries.yml / _COUNTRY_CODES yet. Never None -- callers building a
    ref_key need a segment to put there regardless."""
    country = country_for(regulator)
    if not country:
        return _UNKNOWN_COUNTRY_CODE
    return _COUNTRY_CODES.get(country, _UNKNOWN_COUNTRY_CODE)


# Same shape as _NUMBERED_HEADER_RE-style acronym extraction elsewhere --
# deliberately a small local copy rather than importing
# storage.mssql_repo.regulator_acronym: that function lives in the storage
# layer for ref_key building specifically, and utils/ (lower-level, no DB
# concerns) importing FROM storage would invert the dependency direction the
# rest of this codebase keeps everywhere else.
_ACRONYM_RE = re.compile(r'\(([A-Za-z0-9]+)\)\s*$')


def resolve_regulator(value: str) -> Optional[str]:
    """A caller-supplied regulator filter value -- either the exact full
    name ("Central Bank of Egypt (CBE)") or just its parenthesized acronym
    ("CBE"), case-insensitive either way -- resolved to the canonical full
    name stored in regulations.regulator. None if it matches neither: unlike
    resolve_country, that is NOT an error here, since countries.yml is not a
    closed list of every regulator that will ever exist (SBP/SECP, for
    instance, are real stored values with no entry in it at all) -- the
    caller should fall back to using the raw value as-is for an exact match
    rather than rejecting it."""
    v = (value or "").strip()
    if not v:
        return None
    names = _load()
    for name in names:
        if name.casefold() == v.casefold():
            return name
    v_upper = v.upper()
    for name in names:
        m = _ACRONYM_RE.search(name)
        if m and m.group(1).upper() == v_upper:
            return name
    return None


__all__ = ["country_for", "country_code_for", "tree_path", "countries", "regulators",
          "regulators_for_country", "resolve_country", "resolve_regulator", "CONFIG"]
