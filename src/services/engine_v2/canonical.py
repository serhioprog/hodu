"""
Canonical category mapping — engine v2 copy.

Per the engine-isolation rule (memory: feedback_engine_isolation), this
module is a copy of experiments/new_engine_v2/tools/canonical.py rather
than an import. The synonym JSON itself is shared at
config/category_synonyms.json (single source of truth for the mapping
content).

If you change the synonym map, only the JSON file changes; this Python
loader is duplicated by design so the engine can be deployed
independently of tools/.

See research/SYNONYM_MAP_REASONING.md for why each label was grouped.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

# This file lives at experiments/new_engine_v2/src/canonical.py.
# Synonym JSON: experiments/new_engine_v2/config/category_synonyms.json.
_HERE = Path(__file__).resolve().parent  # src/
_SYNONYM_PATH = _HERE / "config" / "category_synonyms.json"


@lru_cache(maxsize=1)
def _load_map() -> dict[str, list[str | None]]:
    if not _SYNONYM_PATH.exists():
        raise FileNotFoundError(
            f"Category synonym map not found at {_SYNONYM_PATH}. "
            "See research/SYNONYM_MAP_REASONING.md."
        )
    with _SYNONYM_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _build_lookup() -> dict[str, str]:
    """Pre-compute lowercase-alias -> canonical mapping for O(1) lookup."""
    out: dict[str, str] = {}
    for canonical, aliases in _load_map().items():
        for alias in aliases:
            if alias is None:
                continue  # `unknown` canonical handles None category at lookup time
            key = alias.lower().strip()
            if key in out and out[key] != canonical:
                raise ValueError(
                    f"Alias {alias!r} maps to both {out[key]!r} and {canonical!r}"
                )
            out[key] = canonical
    return out


def to_canonical(raw: str | None) -> str:
    """
    Map a raw `properties.category` value to its canonical key.

    - None / empty       → 'unknown'
    - Mapped alias       → its canonical key (case-insensitive)
    - Unmapped non-empty → 'unknown_label_<raw_lower>' so the engine
                           logs it and the architect can extend the map.
    """
    if raw is None:
        return "unknown"
    stripped = raw.strip()
    if not stripped:
        return "unknown"
    return _build_lookup().get(stripped.lower(), f"unknown_label_{stripped.lower()}")


def all_canonicals() -> list[str]:
    """Return every canonical key, for diagnostics / testing."""
    return list(_load_map().keys())
