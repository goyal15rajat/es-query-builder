import logging
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, model_validator

from .constants import _TYPE_CASTERS
from .utils import _apply_casing, _to_epoch

logger = logging.getLogger(__name__)


class FieldTransformRule(BaseModel):
    """Transformation rule for a single field in a parsed document.

    Attributes:
        source_key: The key name as it appears in the parser output.
        target_key: Completely override the output key name.  When provided, the
            name is used **verbatim** — ``key_casing`` is NOT applied to it,
            because you have already chosen the exact name you want.
            Defaults to ``source_key`` when omitted (casing will then apply).
        default: Value to use when the field is ``None`` or absent.
        type_cast: Optional coercion applied to the (possibly-defaulted) value.
            Supported values:

            - ``"STR"`` / ``"INT"`` / ``"FLOAT"`` / ``"BOOL"`` — standard Python casts.
            - ``"NUMBER"`` — coerces to ``float``.
            - ``"EPOCH"`` — converts a datetime object or ISO-8601 string to
              a UTC Unix timestamp (``int``).

        key_casing: Per-field key-name casing override. Takes priority over
            ``TransformConfig.key_casing``. Ignored when ``target_key`` is
            explicitly set.  Leave as ``None`` to inherit the global setting.
        value_casing: Apply string casing to the **value** of this field
            (only effective when the final value is a ``str``).
            One of ``"LOWER"``, ``"UPPER"``, ``"TITLE"``.

    Example::

        # Explicit rename + value uppercased
        FieldTransformRule(
            source_key="status",
            target_key="Status",       # used as-is, verbatim
            value_casing="UPPER",      # "active" → "ACTIVE"
        )

        # No rename — source_key used as output key
        FieldTransformRule(
            source_key="created_at",
            type_cast="EPOCH",
        )
    """

    source_key: str
    target_key: Optional[str] = None
    default: Any = None
    type_cast: Optional[Literal["STR", "INT", "FLOAT", "BOOL", "NUMBER", "EPOCH", "DATE"]] = None
    value_casing: Optional[Literal["LOWER", "UPPER", "TITLE"]] = None
    """Apply string casing to the **value** of this field (only when the value is a ``str``)."""
    date_format: Optional[str] = None
    """strptime format for **parsing** the input value when ``type_cast='DATE'``.
    Defaults to ``'%Y-%m-%dT%H:%M:%S'`` when not supplied."""
    target_date_format: Optional[str] = None
    """strftime format for the **output** string when ``type_cast='DATE'``.
    - ``datetime`` input  → formatted with this pattern.
    - ``str`` input       → re-formatted from ``date_format`` to this pattern.
    Defaults to ``date_format`` (no-op reformat) when not supplied."""

    @model_validator(mode="after")
    def _resolve_target_key(self) -> "FieldTransformRule":
        """Default target_key to source_key when not supplied."""
        if self.target_key is None:
            self.target_key = self.source_key
        return self


class TransformConfig(BaseModel):
    """Configuration for ``ResponseTransformer``.

    Attributes:
        rules: Per-field transformation rules (rename, default, type cast, value casing).
            Fields not listed in rules are passed through unchanged when
            ``include_unmapped`` is ``True``.
        include_unmapped: When ``True`` (default), keys not covered by any rule
            are copied to the output as-is.
            Set to ``False`` to strip unmapped fields from the output.

    Example::

        TransformConfig(
            rules=[
                FieldTransformRule(source_key="user_id", target_key="userId"),
                FieldTransformRule(source_key="score", type_cast="FLOAT", default=0.0),
                FieldTransformRule(source_key="status", value_casing="UPPER"),
            ],
        )
    """

    rules: List[FieldTransformRule] = []
    include_unmapped: bool = True


class ResponseTransformer:
    """Apply a ``TransformConfig`` to a list of parsed documents.

    All config is validated and pre-processed in ``__init__`` so the hot path
    (``transform``) is pure Python dict operations with no Pydantic overhead.

    Usage::

        config = TransformConfig(
            rules=[
                FieldTransformRule(source_key="user_id", target_key="userId"),
                FieldTransformRule(source_key="created_at", type_cast="EPOCH"),
                FieldTransformRule(source_key="score", default=0.0, type_cast="FLOAT"),
            ],
            key_casing="LOWER",
        )
        transformer = ResponseTransformer(config)
        output = transformer.transform(parser.parse_data(response))
    """

    def __init__(self, config: TransformConfig) -> None:
        self.config = config
        # Pre-build O(1) lookup: source_key → rule
        self._rules: Dict[str, FieldTransformRule] = {r.source_key: r for r in config.rules}

    def transform(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply the transform config to every document in *docs*.

        Args:
            docs: List of dicts as returned by ``ESResponseParser.parse_data``.

        Returns:
            New list of transformed dicts — the originals are not mutated.
        """
        logger.debug(f"Transforming {len(docs)} documents")
        return [self._transform_doc(doc) for doc in docs]

    def _transform_doc(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single document according to the config.

        Starts with a shallow copy of the full document (or an empty dict when
        ``include_unmapped=False``) so unmapped fields are already present in
        one C-level copy — no second Python-level loop needed.
        """
        # Unmapped keys are included for free via the initial copy.
        # include_unmapped=False → start empty; only rule-matched keys will be written.
        result: Dict[str, Any] = dict(doc) if self.config.include_unmapped else {}

        for source_key, rule in self._rules.items():
            # If source_key is not present in doc and no default is provided, skip it
            if source_key not in doc and rule.default is None:
                continue

            raw = doc.get(source_key)
            try:
                value = rule.default if (raw is None and rule.default is not None) else raw

                if value is not None:
                    if isinstance(value, datetime):
                        # datetime input: direction depends on type_cast
                        if rule.type_cast == "EPOCH":
                            value = _to_epoch(value)
                        elif rule.type_cast == "DATE":
                            # datetime → formatted string
                            out_fmt = rule.target_date_format or rule.date_format or "%Y-%m-%dT%H:%M:%S"
                            value = value.strftime(out_fmt)
                        # else no type_cast or other cast: leave datetime unchanged

                    elif isinstance(value, str) and (rule.type_cast == "DATE" or rule.type_cast == "EPOCH"):
                        # string → parse with date_format → reformat with target_date_format
                        parse_fmt = rule.date_format or "%Y-%m-%dT%H:%M:%S"
                        out_fmt = rule.target_date_format or parse_fmt
                        date_obj = datetime.strptime(value, parse_fmt)
                        if rule.type_cast == "DATE":
                            value = date_obj.strftime(out_fmt)
                        elif rule.type_cast == "EPOCH":
                            value = _to_epoch(date_obj)

                    elif rule.type_cast == "DATE":
                        # non-string, non-datetime: not supported
                        raise TypeError(f"type_cast='DATE' expects str or datetime, " f"got {type(value).__name__!r}")

                    elif rule.type_cast is not None:
                        value = _TYPE_CASTERS[rule.type_cast](value)

                    if isinstance(value, str):
                        value = _apply_casing(value, rule.value_casing)
                # For renames: remove the original key AFTER successful transformation
                # so the error fallback below never operates on a half-mutated result.
                if rule.target_key != source_key:
                    result.pop(source_key, None)
                result[rule.target_key] = value  # type: ignore[index]
            except Exception as exc:
                logger.error(
                    "Rule failed for field '%s' (type_cast=%r, target_key=%r): %s. ",
                    source_key,
                    rule.type_cast,
                    rule.target_key,
                    exc,
                )
                # Only restore source_key if it was present in the original doc.
                # If the rule was triggered solely via rule.default (source_key absent),
                # inserting source_key=None would silently corrupt the output.
                if source_key in doc:
                    result[source_key] = raw

        return result
