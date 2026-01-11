"""Test cases for schema validator."""

from unittest.mock import Mock

import pytest

from es_utils.connection import ESClientSingleton
from es_utils.schema_validatior import (
    SchemaValidationResult,
    SchemaValidator,
    validate_index,
    validate_schema,
)


class TestSchemaValidationResult:
    """Test SchemaValidationResult class."""

    def test_initialization(self):
        """Test that SchemaValidationResult initializes correctly."""
        result = SchemaValidationResult()
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == []
        assert result.missing_fields == []
        assert result.extra_fields == []
        assert result.type_mismatches == {}
        assert result.setting_mismatches == {}

    def test_add_error(self):
        """Test adding an error marks validation as failed."""
        result = SchemaValidationResult()
        result.add_error("Test error")
        assert result.is_valid is False
        assert "Test error" in result.errors

    def test_add_warning(self):
        """Test adding a warning doesn't mark validation as failed."""
        result = SchemaValidationResult()
        result.add_warning("Test warning")
        assert result.is_valid is True
        assert "Test warning" in result.warnings

    def test_str_representation_passed(self):
        """Test string representation for passed validation."""
        result = SchemaValidationResult()
        output = str(result)
        assert "Schema Validation: PASSED" in output

    def test_str_representation_failed(self):
        """Test string representation for failed validation."""
        result = SchemaValidationResult()
        result.add_error("Missing field: name")
        result.missing_fields = ["name"]
        output = str(result)
        assert "Schema Validation: FAILED" in output
        assert "Missing field: name" in output
        assert "Missing Fields (1):" in output

    def test_str_representation_with_mismatches(self):
        """Test string representation includes all mismatch details."""
        result = SchemaValidationResult()
        result.type_mismatches = {"age": ("integer", "text")}
        result.setting_mismatches = {"number_of_shards": (1, 2)}
        result.extra_fields = ["extra_field"]
        result.add_warning("Extra field in schema: extra_field")

        output = str(result)
        assert "Type Mismatches (1):" in output
        assert "age: expected 'integer', got 'text'" in output
        assert "Setting Mismatches (1):" in output
        assert "number_of_shards: expected '1', got '2'" in output
        assert "Extra Fields (1):" in output
        assert "extra_field" in output


class TestSchemaValidator:
    """Test SchemaValidator class."""

    def test_initialization_default(self):
        """Test default initialization."""
        validator = SchemaValidator()
        assert validator.strict_mode is True
        assert validator.ignore_extra_fields is False

    def test_initialization_custom(self):
        """Test custom initialization."""
        validator = SchemaValidator(strict_mode=False, ignore_extra_fields=True)
        assert validator.strict_mode is False
        assert validator.ignore_extra_fields is True

    def test_validate_schema_identical(self):
        """Test validation passes for identical schemas."""
        validator = SchemaValidator()
        expected = {
            "settings": {"number_of_shards": 1},
            "mappings": {"properties": {"name": {"type": "text"}, "age": {"type": "integer"}}},
        }
        actual = expected.copy()

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_validate_schema_missing_field(self):
        """Test validation fails when a field is missing."""
        validator = SchemaValidator()
        expected = {"mappings": {"properties": {"name": {"type": "text"}, "age": {"type": "integer"}}}}
        actual = {"mappings": {"properties": {"name": {"type": "text"}}}}

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is False
        assert "age" in result.missing_fields
        assert any("Missing field in schema: age" in error for error in result.errors)

    def test_validate_schema_extra_field_strict(self):
        """Test validation fails for extra fields in strict mode."""
        validator = SchemaValidator(strict_mode=True)
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}
        actual = {"mappings": {"properties": {"name": {"type": "text"}, "age": {"type": "integer"}}}}

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is False
        assert "age" in result.extra_fields
        assert any("Unexpected field in schema: age" in error for error in result.errors)

    def test_validate_schema_extra_field_non_strict(self):
        """Test validation warns for extra fields in non-strict mode."""
        validator = SchemaValidator(strict_mode=False)
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}
        actual = {"mappings": {"properties": {"name": {"type": "text"}, "age": {"type": "integer"}}}}

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is True  # No errors in non-strict mode
        assert "age" in result.extra_fields
        assert any("Extra field in schema: age" in warning for warning in result.warnings)

    def test_validate_schema_ignore_extra_fields(self):
        """Test validation ignores extra fields when configured."""
        validator = SchemaValidator(ignore_extra_fields=True)
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}
        actual = {"mappings": {"properties": {"name": {"type": "text"}, "age": {"type": "integer"}}}}

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is True
        assert len(result.extra_fields) == 0

    def test_validate_schema_field_type_mismatch(self):
        """Test validation detects field type mismatches."""
        validator = SchemaValidator()
        expected = {"mappings": {"properties": {"age": {"type": "integer"}}}}
        actual = {"mappings": {"properties": {"age": {"type": "text"}}}}

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is False
        assert "age" in result.type_mismatches
        assert any("Field configuration mismatch for 'age'" in error for error in result.errors)

    def test_validate_schema_field_config_mismatch(self):
        """Test validation detects field configuration differences."""
        validator = SchemaValidator()
        expected = {"mappings": {"properties": {"timestamp": {"type": "date", "format": "yyyy-MM-dd"}}}}
        actual = {"mappings": {"properties": {"timestamp": {"type": "date", "format": "epoch_millis"}}}}

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is False
        assert "timestamp" in result.type_mismatches

    def test_validate_settings_match(self):
        """Test settings validation passes for matching settings."""
        validator = SchemaValidator()
        expected = {"settings": {"number_of_shards": 1, "number_of_replicas": 2}}
        actual = expected.copy()

        result = validator.validate_schema(expected, actual, validate_mappings=False)
        assert result.is_valid is True

    def test_validate_settings_mismatch_strict(self):
        """Test settings mismatch in strict mode."""
        validator = SchemaValidator(strict_mode=True)
        expected = {"settings": {"number_of_shards": 1}}
        actual = {"settings": {"number_of_shards": 2}}

        result = validator.validate_schema(expected, actual, validate_mappings=False)
        assert result.is_valid is False
        assert "number_of_shards" in result.setting_mismatches
        assert result.setting_mismatches["number_of_shards"] == (1, 2)

    def test_validate_settings_mismatch_non_strict(self):
        """Test settings mismatch in non-strict mode."""
        validator = SchemaValidator(strict_mode=False)
        expected = {"settings": {"number_of_shards": 1}}
        actual = {"settings": {"number_of_shards": 2}}

        result = validator.validate_schema(expected, actual, validate_mappings=False)
        assert result.is_valid is True  # Warnings only
        assert "number_of_shards" in result.setting_mismatches
        assert len(result.warnings) > 0

    def test_validate_settings_missing(self):
        """Test missing settings are detected."""
        validator = SchemaValidator()
        expected = {
            "settings": {"number_of_shards": 1, "analysis": {"analyzer": {"custom_analyzer": {"type": "standard"}}}}
        }
        actual = {"settings": {"number_of_shards": 1}}

        result = validator.validate_schema(expected, actual, validate_mappings=False)
        assert result.is_valid is False
        assert any("Missing setting" in error for error in result.errors)

    def test_validate_nested_settings(self):
        """Test validation of nested settings structure."""
        validator = SchemaValidator()
        expected = {
            "settings": {
                "analysis": {"normalizer": {"lowercase_normalizer": {"type": "custom", "filter": ["lowercase"]}}}
            }
        }
        actual = expected.copy()

        result = validator.validate_schema(expected, actual, validate_mappings=False)
        assert result.is_valid is True

    def test_validate_only_mappings(self):
        """Test validating only mappings without settings."""
        validator = SchemaValidator()
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}
        actual = expected.copy()

        result = validator.validate_schema(expected, actual, validate_settings=False)
        assert result.is_valid is True

    def test_validate_only_settings(self):
        """Test validating only settings without mappings."""
        validator = SchemaValidator()
        expected = {"settings": {"number_of_shards": 1}}
        actual = expected.copy()

        result = validator.validate_schema(expected, actual, validate_mappings=False)
        assert result.is_valid is True

    def test_validate_complex_nested_fields(self):
        """Test validation with complex nested field structures."""
        validator = SchemaValidator()
        expected = {
            "mappings": {
                "properties": {
                    "name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "normalizer": "lowercase_normalizer"}},
                    }
                }
            }
        }
        actual = expected.copy()

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is True

    def test_validate_index_with_mock(self, mock_es_client):
        """Test validate_index method with mocked ES client."""
        validator = SchemaValidator()
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}

        # Configure mock indices
        mock_indices = Mock()
        mock_es_client.indices = mock_indices
        mock_indices.get_mapping.return_value = {"test_index": {"mappings": {"properties": {"name": {"type": "text"}}}}}
        mock_indices.get_settings.return_value = {"test_index": {"settings": {}}}

        # Set mock client as default
        ESClientSingleton.set(mock_es_client)

        try:
            result = validator.validate_index("test_index", expected)
            assert result.is_valid is True
        finally:
            ESClientSingleton.clear()


class TestValidateFunctions:
    """Test convenience functions."""

    def test_validate_schema_function(self):
        """Test validate_schema convenience function."""
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}
        actual = expected.copy()

        result = validate_schema(expected, actual)
        assert isinstance(result, SchemaValidationResult)
        assert result.is_valid is True

    def test_validate_schema_function_with_options(self):
        """Test validate_schema with custom options."""
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}
        actual = {"mappings": {"properties": {"name": {"type": "text"}, "age": {"type": "integer"}}}}

        result = validate_schema(expected, actual, strict_mode=False, ignore_extra_fields=True)
        assert result.is_valid is True

    def test_validate_index_function(self, mock_es_client):
        """Test validate_index convenience function."""
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}

        # Configure mock indices
        mock_indices = Mock()
        mock_es_client.indices = mock_indices
        mock_indices.get_mapping.return_value = {"test_index": {"mappings": {"properties": {"name": {"type": "text"}}}}}
        mock_indices.get_settings.return_value = {"test_index": {"settings": {}}}

        ESClientSingleton.set(mock_es_client)

        try:
            result = validate_index("test_index", expected)
            assert isinstance(result, SchemaValidationResult)
            assert result.is_valid is True
        finally:
            ESClientSingleton.clear()

    def test_validate_index_function_with_options(self, mock_es_client):
        """Test validate_index with custom options."""
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}

        # Configure mock indices
        mock_indices = Mock()
        mock_es_client.indices = mock_indices
        mock_indices.get_mapping.return_value = {
            "test_index": {"mappings": {"properties": {"name": {"type": "text"}, "extra": {"type": "keyword"}}}}
        }
        mock_indices.get_settings.return_value = {"test_index": {"settings": {}}}

        ESClientSingleton.set(mock_es_client)

        try:
            result = validate_index("test_index", expected, strict_mode=False, ignore_extra_fields=True)
            assert result.is_valid is True
        finally:
            ESClientSingleton.clear()


class TestEdgeCases:
    """Test edge cases and error scenarios."""

    def test_empty_schemas(self):
        """Test validation with empty schemas."""
        validator = SchemaValidator()
        result = validator.validate_schema({}, {})
        assert result.is_valid is True

    def test_missing_mappings_key(self):
        """Test schemas without mappings key."""
        validator = SchemaValidator()
        expected = {"settings": {"number_of_shards": 1}}
        actual = {"settings": {"number_of_shards": 1}}

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is True

    def test_missing_settings_key(self):
        """Test schemas without settings key."""
        validator = SchemaValidator()
        expected = {"mappings": {"properties": {"name": {"type": "text"}}}}
        actual = expected.copy()

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is True

    def test_flatten_dict(self):
        """Test _flatten_dict helper method."""
        validator = SchemaValidator()
        nested = {"a": 1, "b": {"c": 2, "d": {"e": 3}}}

        flattened = validator._flatten_dict(nested)
        assert flattened["a"] == 1
        assert flattened["b.c"] == 2
        assert flattened["b.d.e"] == 3

    def test_complex_real_world_schema(self):
        """Test with a real-world complex schema."""
        validator = SchemaValidator()
        expected = {
            "settings": {
                "analysis": {"normalizer": {"lowercase_normalizer": {"type": "custom", "filter": ["lowercase"]}}}
            },
            "mappings": {
                "properties": {
                    "name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "normalizer": "lowercase_normalizer"}},
                    },
                    "dob": {"type": "date", "format": "MM/dd/yyyy"},
                    "age": {"type": "integer"},
                    "address": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "normalizer": "lowercase_normalizer"}},
                    },
                    "phone": {"type": "keyword", "normalizer": "lowercase_normalizer"},
                }
            },
        }
        actual = expected.copy()

        result = validator.validate_schema(expected, actual)
        assert result.is_valid is True
