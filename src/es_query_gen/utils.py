"""Utility helpers for the es_query_gen package."""

from datetime import datetime, timezone
from typing import Callable, Dict, Optional


def _to_epoch(value: datetime | int | float) -> int:
    """Convert a ``datetime`` or numeric value to a UTC Unix epoch (``int``).

    String inputs are not handled here — callers parse the string with
    ``datetime.strptime`` first and pass the resulting ``datetime`` object here.
    """
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if isinstance(value, (int, float)):
        return int(value)
    raise TypeError(f"Cannot convert {type(value).__name__!r} to epoch")


def _apply_casing(text: str, casing: Optional[str]) -> str:
    """Apply ``casing`` to ``text``, or return ``text`` unchanged if ``None``."""

    # Casing dispatch — maps Literal values to str methods
    _CASING_FNS: Dict[str, Callable[[str], str]] = {
        "LOWER": str.lower,
        "UPPER": str.upper,
        "TITLE": str.title,
    }

    if casing is None:
        return text
    return _CASING_FNS[casing](text)
