"""Internal constants for the transformer module."""

from typing import Any, Callable, Dict

from .utils import _to_epoch

# Type-cast dispatch — O(1) lookup, no if/elif chain per document
_TYPE_CASTERS: Dict[str, Callable[..., Any]] = {
    "STR": str,
    "INT": int,
    "FLOAT": float,
    "BOOL": bool,
    "NUMBER": float,  # int/float → float (generic "number" type)
    "EPOCH": _to_epoch,
}
