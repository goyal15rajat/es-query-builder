"""Test suite for transformer.py — ResponseTransformer."""

from datetime import datetime, timezone

import pytest

from src.es_query_gen.transformer import (
    FieldTransformRule,
    ResponseTransformer,
    TransformConfig,
)
from src.es_query_gen.utils import _to_epoch

# ---------------------------------------------------------------------------
# _to_epoch helper
# ---------------------------------------------------------------------------


class TestToEpoch:
    def test_datetime_object_aware(self):
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert _to_epoch(dt) == 1704067200

    def test_datetime_object_naive_treated_as_utc(self):
        dt = datetime(2024, 1, 1, 0, 0, 0)
        assert _to_epoch(dt) == 1704067200

    def test_int_passthrough(self):
        assert _to_epoch(1704067200) == 1704067200

    def test_float_truncates(self):
        assert _to_epoch(1704067200.9) == 1704067200

    def test_unsupported_type_raises(self):
        with pytest.raises(TypeError):
            _to_epoch("2024-01-01")  # strings no longer accepted here


# ---------------------------------------------------------------------------
# FieldTransformRule
# ---------------------------------------------------------------------------


class TestFieldTransformRule:
    def test_target_key_defaults_to_source_key(self):
        rule = FieldTransformRule(source_key="user_id")
        assert rule.target_key == "user_id"

    def test_explicit_target_key(self):
        rule = FieldTransformRule(source_key="user_id", target_key="userId")
        assert rule.target_key == "userId"

    def test_default_is_none(self):
        rule = FieldTransformRule(source_key="score")
        assert rule.default is None

    def test_type_cast_none_by_default(self):
        rule = FieldTransformRule(source_key="score")
        assert rule.type_cast is None

    def test_invalid_type_cast_raises(self):
        with pytest.raises(Exception):
            FieldTransformRule(source_key="x", type_cast="uuid")


# ---------------------------------------------------------------------------
# TransformConfig
# ---------------------------------------------------------------------------


class TestTransformConfig:
    def test_empty_config_defaults(self):
        cfg = TransformConfig()
        assert cfg.rules == []
        assert cfg.include_unmapped is True

    def test_invalid_rules_raises(self):
        with pytest.raises(Exception):
            TransformConfig(rules="not-a-list")


# ---------------------------------------------------------------------------
# ResponseTransformer — rename
# ---------------------------------------------------------------------------


class TestResponseTransformerRename:
    def test_rename_single_key(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="user_id", target_key="userId")])
        result = ResponseTransformer(cfg).transform([{"user_id": "abc", "name": "Alice"}])
        assert result[0]["userId"] == "abc"
        assert "user_id" not in result[0]

    def test_no_rename_when_target_key_same(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="name")])
        result = ResponseTransformer(cfg).transform([{"name": "Alice"}])
        assert result[0]["name"] == "Alice"

    def test_unmapped_keys_included_by_default(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="user_id", target_key="userId")])
        result = ResponseTransformer(cfg).transform([{"user_id": "x", "extra": "keep"}])
        assert "extra" in result[0]

    def test_unmapped_keys_excluded_when_disabled(self):
        cfg = TransformConfig(
            rules=[FieldTransformRule(source_key="user_id", target_key="userId")],
            include_unmapped=False,
        )
        result = ResponseTransformer(cfg).transform([{"user_id": "x", "extra": "drop"}])
        assert "extra" not in result[0]
        assert result[0]["userId"] == "x"

    def test_original_doc_not_mutated(self):
        doc = {"user_id": "abc"}
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="user_id", target_key="userId")])
        ResponseTransformer(cfg).transform([doc])
        assert "user_id" in doc  # original unchanged


# ---------------------------------------------------------------------------
# ResponseTransformer — keys are always verbatim
# ---------------------------------------------------------------------------


class TestResponseTransformerKeyVerbatim:
    def test_source_key_used_verbatim_when_no_target(self):
        """When no target_key is given, source_key is emitted as-is."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="UserName")])
        result = ResponseTransformer(cfg).transform([{"UserName": "Alice"}])
        assert "UserName" in result[0]

    def test_explicit_target_key_verbatim(self):
        """Explicit target_key is emitted exactly as written."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="user_id", target_key="userId")])
        result = ResponseTransformer(cfg).transform([{"user_id": "x"}])
        assert "userId" in result[0]
        assert "user_id" not in result[0]

    def test_unmapped_keys_verbatim(self):
        """Unmapped keys are passed through with their original casing."""
        cfg = TransformConfig(include_unmapped=True)
        result = ResponseTransformer(cfg).transform([{"UserName": "Alice", "AGE": 30}])
        assert "UserName" in result[0]
        assert "AGE" in result[0]


# ---------------------------------------------------------------------------
# ResponseTransformer — value_casing
# ---------------------------------------------------------------------------


class TestResponseTransformerValueCasing:
    def test_value_casing_lower(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="name", value_casing="LOWER")])
        result = ResponseTransformer(cfg).transform([{"name": "ALICE"}])
        assert result[0]["name"] == "alice"

    def test_value_casing_upper(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="status", value_casing="UPPER")])
        result = ResponseTransformer(cfg).transform([{"status": "active"}])
        assert result[0]["status"] == "ACTIVE"

    def test_value_casing_title(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="city", value_casing="TITLE")])
        result = ResponseTransformer(cfg).transform([{"city": "new york"}])
        assert result[0]["city"] == "New York"

    def test_value_casing_not_applied_to_non_string(self):
        """value_casing is silently skipped when the value is not a string."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="score", value_casing="UPPER")])
        result = ResponseTransformer(cfg).transform([{"score": 42}])
        assert result[0]["score"] == 42  # int unchanged

    def test_value_casing_applied_after_type_cast(self):
        """value_casing applies to the value AFTER type_cast so cast-to-str is covered."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="code", type_cast="STR", value_casing="UPPER")])
        result = ResponseTransformer(cfg).transform([{"code": "ok"}])
        assert result[0]["code"] == "OK"

    def test_value_casing_with_explicit_target_key(self):
        """value_casing + explicit target_key: key is verbatim, value is cased."""
        cfg = TransformConfig(
            rules=[FieldTransformRule(source_key="status", target_key="Status", value_casing="UPPER")]
        )
        result = ResponseTransformer(cfg).transform([{"status": "active"}])
        doc = result[0]
        assert "Status" in doc  # explicit target_key verbatim
        assert "status" not in doc
        assert doc["Status"] == "ACTIVE"  # value uppercased


# ---------------------------------------------------------------------------
# ResponseTransformer — defaults
# ---------------------------------------------------------------------------


class TestResponseTransformerDefaults:
    def test_default_applied_when_value_is_none(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="score", default=0.0)])
        result = ResponseTransformer(cfg).transform([{"score": None}])
        assert result[0]["score"] == 0.0

    def test_default_applied_when_key_absent(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="score", default=42)])
        result = ResponseTransformer(cfg).transform([{}])
        assert result[0]["score"] == 42

    def test_default_not_applied_when_value_present(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="score", default=0)])
        result = ResponseTransformer(cfg).transform([{"score": 99}])
        assert result[0]["score"] == 99

    def test_default_zero_not_overridden_by_none_default(self):
        """A value of 0 (falsy) must NOT be replaced by the default."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="count", default=10)])
        result = ResponseTransformer(cfg).transform([{"count": 0}])
        assert result[0]["count"] == 0


# ---------------------------------------------------------------------------
# ResponseTransformer — type casting
# ---------------------------------------------------------------------------


class TestResponseTransformerTypeCast:
    def test_cast_to_int(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="age", type_cast="INT")])
        result = ResponseTransformer(cfg).transform([{"age": "30"}])
        assert result[0]["age"] == 30
        assert isinstance(result[0]["age"], int)

    def test_cast_to_float(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="price", type_cast="FLOAT")])
        result = ResponseTransformer(cfg).transform([{"price": "9.99"}])
        assert result[0]["price"] == pytest.approx(9.99)

    def test_cast_to_number(self):
        """'number' coerces to float."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="val", type_cast="NUMBER")])
        result = ResponseTransformer(cfg).transform([{"val": 5}])
        assert isinstance(result[0]["val"], float)

    def test_cast_to_str(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="code", type_cast="STR")])
        result = ResponseTransformer(cfg).transform([{"code": 404}])
        assert result[0]["code"] == "404"

    def test_cast_to_bool(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="active", type_cast="BOOL")])
        result = ResponseTransformer(cfg).transform([{"active": 1}])
        assert result[0]["active"] is True

    def test_cast_to_epoch_from_string(self):
        """String + type_cast='EPOCH': requires date_format for parsing."""
        cfg = TransformConfig(
            rules=[
                FieldTransformRule(
                    source_key="created_at",
                    type_cast="EPOCH",
                    date_format="%Y-%m-%dT%H:%M:%SZ",
                )
            ]
        )
        result = ResponseTransformer(cfg).transform([{"created_at": "2024-01-01T00:00:00Z"}])
        assert result[0]["created_at"] == 1704067200

    def test_cast_to_epoch_from_datetime(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="ts", type_cast="EPOCH")])
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = ResponseTransformer(cfg).transform([{"ts": dt}])
        assert result[0]["ts"] == 1704067200

    def test_no_cast_when_value_is_none(self):
        """Type cast must not be applied to None — let the default handle it."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="score", type_cast="INT", default=0)])
        result = ResponseTransformer(cfg).transform([{"score": None}])
        # default=0 fills in, then type_cast="INT" casts 0 → int
        assert result[0]["score"] == 0
        assert isinstance(result[0]["score"], int)

    def test_cast_not_applied_to_none_without_default(self):
        """If value is None and no default, leave it as None without casting."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="score", type_cast="INT")])
        result = ResponseTransformer(cfg).transform([{"score": None}])
        assert result[0]["score"] is None


# ---------------------------------------------------------------------------
# ResponseTransformer — datetime handling
# ---------------------------------------------------------------------------


class TestResponseTransformerDatetime:
    def test_datetime_without_type_cast_passes_through(self):
        """A datetime with no type_cast is left as-is (no auto-conversion)."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="ts")])
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = ResponseTransformer(cfg).transform([{"ts": dt}])
        assert result[0]["ts"] is dt  # unchanged

    def test_datetime_plus_epoch_gives_int(self):
        """datetime + type_cast='EPOCH' → Unix epoch int."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="ts", type_cast="EPOCH")])
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        result = ResponseTransformer(cfg).transform([{"ts": dt}])
        assert result[0]["ts"] == 1704067200

    def test_naive_datetime_epoch_treated_as_utc(self):
        """A naive datetime with type_cast='EPOCH' is assumed UTC."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="ts", type_cast="EPOCH")])
        dt = datetime(2024, 1, 1, 0, 0, 0)  # no tzinfo
        result = ResponseTransformer(cfg).transform([{"ts": dt}])
        assert result[0]["ts"] == 1704067200

    def test_datetime_plus_date_gives_formatted_string(self):
        """datetime + type_cast='DATE' → formatted string using target_date_format."""
        cfg = TransformConfig(
            rules=[FieldTransformRule(source_key="ts", type_cast="DATE", target_date_format="%Y-%m-%d")]
        )
        dt = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
        result = ResponseTransformer(cfg).transform([{"ts": dt}])
        assert result[0]["ts"] == "2024-01-01"

    def test_datetime_date_cast_default_format(self):
        """datetime + type_cast='DATE' with no target_date_format uses %Y-%m-%dT%H:%M:%S."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="ts", type_cast="DATE")])
        dt = datetime(2024, 1, 1, 0, 0, 0)
        result = ResponseTransformer(cfg).transform([{"ts": dt}])
        assert result[0]["ts"] == "2024-01-01T00:00:00"

    def test_datetime_date_cast_uses_date_format_as_fallback(self):
        """target_date_format falls back to date_format if not set."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="ts", type_cast="DATE", date_format="%d/%m/%Y")])
        dt = datetime(2024, 1, 1)
        result = ResponseTransformer(cfg).transform([{"ts": dt}])
        assert result[0]["ts"] == "01/01/2024"


# ---------------------------------------------------------------------------
# ResponseTransformer — type_cast="DATE" (string reformatting)
# ---------------------------------------------------------------------------


class TestResponseTransformerDateCast:
    def test_str_date_reformat_with_both_formats(self):
        """str + type_cast='DATE': parse date_format, reformat to target_date_format."""
        cfg = TransformConfig(
            rules=[
                FieldTransformRule(
                    source_key="date",
                    type_cast="DATE",
                    date_format="%d/%m/%Y",
                    target_date_format="%Y-%m-%d",
                )
            ]
        )
        result = ResponseTransformer(cfg).transform([{"date": "01/01/2024"}])
        assert result[0]["date"] == "2024-01-01"

    def test_str_date_no_target_format_is_noop(self):
        """Without target_date_format, the string is parsed and re-formatted with same format."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="ts", type_cast="DATE", date_format="%Y-%m-%d")])
        result = ResponseTransformer(cfg).transform([{"ts": "2024-01-01"}])
        assert result[0]["ts"] == "2024-01-01"  # same string out

    def test_str_date_default_parse_format(self):
        """date_format defaults to %Y-%m-%dT%H:%M:%S when not supplied."""
        cfg = TransformConfig(
            rules=[FieldTransformRule(source_key="ts", type_cast="DATE", target_date_format="%Y-%m-%d")]
        )
        result = ResponseTransformer(cfg).transform([{"ts": "2024-01-01T00:00:00"}])
        assert result[0]["ts"] == "2024-01-01"

    def test_str_date_invalid_string_triggers_fallback(self):
        """A string not matching date_format triggers the error fallback."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="ts", type_cast="DATE", date_format="%Y-%m-%d")])
        result = ResponseTransformer(cfg).transform([{"ts": "not-a-date"}])
        assert result[0]["ts"] == "not-a-date"

    def test_date_cast_non_str_non_datetime_triggers_fallback(self):
        """type_cast='DATE' on an int (non-str, non-datetime) triggers the error fallback."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="ts", type_cast="DATE")])
        result = ResponseTransformer(cfg).transform([{"ts": 42}])
        assert result[0]["ts"] == 42

    def test_date_format_ignored_for_other_type_casts(self):
        """date_format / target_date_format have no effect for non-DATE type_casts."""
        cfg = TransformConfig(
            rules=[
                FieldTransformRule(
                    source_key="val", type_cast="INT", date_format="%Y-%m-%d", target_date_format="%d/%m/%Y"
                )
            ]
        )
        result = ResponseTransformer(cfg).transform([{"val": "42"}])
        assert result[0]["val"] == 42


# ---------------------------------------------------------------------------
# ResponseTransformer — combined / end-to-end
# ---------------------------------------------------------------------------


class TestResponseTransformerCombined:
    def test_rename_and_cast_and_value_casing(self):
        """target_key verbatim, type_cast, default, and value_casing all together."""
        cfg = TransformConfig(
            rules=[
                FieldTransformRule(source_key="user_id", target_key="userId", type_cast="STR"),
                FieldTransformRule(source_key="score", type_cast="FLOAT", default=0.0),
                # string EPOCH requires explicit date_format
                FieldTransformRule(
                    source_key="created_at", target_key="createdAt", type_cast="EPOCH", date_format="%Y-%m-%dT%H:%M:%SZ"
                ),
                FieldTransformRule(source_key="status", value_casing="UPPER"),
            ],
        )
        docs = [
            {"user_id": 42, "score": None, "created_at": "2024-01-01T00:00:00Z", "status": "active", "extra": "yes"}
        ]
        result = ResponseTransformer(cfg).transform(docs)
        doc = result[0]

        assert doc["userId"] == "42"  # target_key verbatim
        assert doc["score"] == 0.0  # None → default → float
        assert doc["createdAt"] == 1704067200  # string → strptime → epoch
        assert doc["status"] == "ACTIVE"  # source_key verbatim, value uppercased
        assert doc["extra"] == "yes"  # unmapped, verbatim

    def test_empty_docs_list(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="x")])
        assert ResponseTransformer(cfg).transform([]) == []

    def test_multiple_docs_transformed_independently(self):
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="val", default=0, type_cast="INT")])
        docs = [{"val": None}, {"val": "5"}, {"val": 3}]
        result = ResponseTransformer(cfg).transform(docs)
        assert result[0]["val"] == 0
        assert result[1]["val"] == 5
        assert result[2]["val"] == 3


# ---------------------------------------------------------------------------
# ResponseTransformer — rule error handling (try/except fallback)
# ---------------------------------------------------------------------------


class TestResponseTransformerRuleError:
    def test_fallback_to_original_key_on_cast_failure(self):
        """If type_cast fails, the field must appear under its source_key with the raw value."""
        cfg = TransformConfig(
            rules=[
                FieldTransformRule(source_key="created_at", target_key="createdAt", type_cast="EPOCH"),
            ]
        )
        # "not-a-date" will cause _to_epoch to raise ValueError
        result = ResponseTransformer(cfg).transform([{"created_at": "not-a-date"}])
        doc = result[0]

        # Fallback: source_key with raw value — no partial transformation
        assert doc["created_at"] == "not-a-date"
        # Target key must NOT appear (no partial rename)
        assert "createdAt" not in doc

    def test_no_partial_rename_on_failure(self):
        """A failed rule must not write the renamed target_key at all."""
        cfg = TransformConfig(
            rules=[
                FieldTransformRule(source_key="ts", target_key="timestamp", type_cast="EPOCH"),
            ]
        )
        result = ResponseTransformer(cfg).transform([{"ts": [1, 2, 3]}])  # list → epoch raises TypeError
        doc = result[0]
        assert "timestamp" not in doc
        assert doc["ts"] == [1, 2, 3]

    def test_other_rules_succeed_despite_one_failure(self):
        """A rule failure must not abort other rules in the same document."""
        cfg = TransformConfig(
            rules=[
                FieldTransformRule(source_key="bad_field", type_cast="EPOCH"),
                FieldTransformRule(source_key="good_field", type_cast="INT"),
            ]
        )
        result = ResponseTransformer(cfg).transform([{"bad_field": "oops", "good_field": "42"}])
        doc = result[0]

        # bad_field falls back, good_field is successfully cast
        assert doc["bad_field"] == "oops"
        assert doc["good_field"] == 42

    def test_error_is_logged(self, caplog):
        """A rule failure must emit an ERROR-level log."""
        import logging

        cfg = TransformConfig(
            rules=[
                FieldTransformRule(source_key="ts", type_cast="EPOCH"),
            ]
        )
        with caplog.at_level(logging.ERROR, logger="src.es_query_gen.transformer"):
            ResponseTransformer(cfg).transform([{"ts": "bad-date"}])
        assert any("Rule failed" in r.message for r in caplog.records)
        assert any("ts" in r.message for r in caplog.records)

    def test_value_casing_empty_string(self):
        """Value casing should safely apply to an empty string."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="val", value_casing="UPPER")])
        result = ResponseTransformer(cfg).transform([{"val": ""}])
        assert result[0]["val"] == ""

    def test_type_cast_empty_string_fallback(self):
        """Casting an empty string to INT fails gracefully (ValueError) and falls back to original."""
        cfg = TransformConfig(rules=[FieldTransformRule(source_key="val", type_cast="INT")])
        result = ResponseTransformer(cfg).transform([{"val": ""}])
        # falls back to original string due to ValueError
        assert result[0]["val"] == ""

    def test_nested_dicts_unmapped_keys(self):
        """Unmapped complex structures (like nested dicts/lists) should be passed through safely."""
        cfg = TransformConfig()
        complex_doc = {"metadata": {"source": "web", "tags": ["a", "b"]}, "users": [{"id": 1}, {"id": 2}]}
        result = ResponseTransformer(cfg).transform([complex_doc])
        doc = result[0]
        assert doc["metadata"]["source"] == "web"
        assert len(doc["users"]) == 2
