"""Schema validator for Elasticsearch indices.

This module provides utilities to validate Elasticsearch index schemas,
ensuring that settings and field mappings match expected configurations.
"""

from typing import Any, Dict, List, Optional, Set, Tuple

from elasticsearch import Elasticsearch

from .connection import get_index_schema, requires_es_client


class SchemaValidationResult:
    """Result object containing schema validation details.

    Attributes:
        is_valid (bool): Whether the schema validation passed.
        errors (List[str]): List of validation error messages.
        warnings (List[str]): List of validation warning messages.
        missing_fields (List[str]): Fields present in expected but missing in actual.
        extra_fields (List[str]): Fields present in actual but not in expected.
        type_mismatches (Dict[str, Tuple[str, str]]): Fields with type mismatches (field -> (expected, actual)).
        setting_mismatches (Dict[str, Tuple[Any, Any]]): Settings with mismatches (setting -> (expected, actual)).
    """

    def __init__(self):
        self.is_valid: bool = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.missing_fields: List[str] = []
        self.extra_fields: List[str] = []
        self.type_mismatches: Dict[str, Tuple[str, str]] = {}
        self.setting_mismatches: Dict[str, Tuple[Any, Any]] = {}

    def add_error(self, message: str):
        """Add an error message and mark validation as failed."""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)

    def __str__(self) -> str:
        """Return a formatted string representation of the validation result."""
        lines = [f"Schema Validation: {'PASSED' if self.is_valid else 'FAILED'}"]

        if self.errors:
            lines.append(f"\nErrors ({len(self.errors)}):")
            for error in self.errors:
                lines.append(f"  - {error}")

        if self.warnings:
            lines.append(f"\nWarnings ({len(self.warnings)}):")
            for warning in self.warnings:
                lines.append(f"  - {warning}")

        if self.missing_fields:
            lines.append(f"\nMissing Fields ({len(self.missing_fields)}):")
            for field in self.missing_fields:
                lines.append(f"  - {field}")

        if self.extra_fields:
            lines.append(f"\nExtra Fields ({len(self.extra_fields)}):")
            for field in self.extra_fields:
                lines.append(f"  - {field}")

        if self.type_mismatches:
            lines.append(f"\nType Mismatches ({len(self.type_mismatches)}):")
            for field, (expected, actual) in self.type_mismatches.items():
                lines.append(f"  - {field}: expected '{expected}', got '{actual}'")

        if self.setting_mismatches:
            lines.append(f"\nSetting Mismatches ({len(self.setting_mismatches)}):")
            for setting, (expected, actual) in self.setting_mismatches.items():
                lines.append(f"  - {setting}: expected '{expected}', got '{actual}'")

        return "\n".join(lines)


class SchemaValidator:
    """Validator for Elasticsearch index schemas.

    This class provides methods to validate that an index schema matches
    expected configurations, including settings and field mappings.
    """

    def __init__(self, strict_mode: bool = True, ignore_extra_fields: bool = False):
        """Initialize the schema validator.

        Args:
            strict_mode (bool): If True, treat all mismatches as errors.
                              If False, some mismatches may be warnings.
            ignore_extra_fields (bool): If True, don't report extra fields in actual schema.
        """
        self.strict_mode = strict_mode
        self.ignore_extra_fields = ignore_extra_fields

    def validate_schema(
        self,
        expected_schema: Dict[str, Any],
        actual_schema: Dict[str, Any],
        validate_settings: bool = True,
        validate_mappings: bool = True,
    ) -> SchemaValidationResult:
        """Validate that actual schema matches expected schema.

        Args:
            expected_schema (Dict[str, Any]): The expected schema configuration.
            actual_schema (Dict[str, Any]): The actual schema from Elasticsearch.
            validate_settings (bool): Whether to validate index settings.
            validate_mappings (bool): Whether to validate field mappings.

        Returns:
            SchemaValidationResult: Detailed validation results.

        Example:
            validator = SchemaValidator()
            expected = {
                "settings": {"number_of_shards": 1},
                "mappings": {
                    "properties": {
                        "name": {"type": "text"},
                        "age": {"type": "integer"}
                    }
                }
            }
            result = validator.validate_schema(expected, actual)
            if not result.is_valid:
                print(result)
        """
        result = SchemaValidationResult()

        # Validate settings
        if validate_settings:
            self._validate_settings(
                expected_schema.get("settings", {}),
                actual_schema.get("settings", {}),
                result,
            )

        # Validate mappings
        if validate_mappings:
            self._validate_mappings(
                expected_schema.get("mappings", {}),
                actual_schema.get("mappings", {}),
                result,
            )

        return result

    @requires_es_client
    def validate_index(
        self,
        index: str,
        expected_schema: Dict[str, Any],
        es: Optional[Elasticsearch] = None,
        validate_settings: bool = True,
        validate_mappings: bool = True,
    ) -> SchemaValidationResult:
        """Validate that an Elasticsearch index matches expected schema.

        Args:
            index (str): Name of the index to validate.
            expected_schema (Dict[str, Any]): The expected schema configuration.
            es (Optional[Elasticsearch]): Elasticsearch client (auto-injected if not provided).
            validate_settings (bool): Whether to validate index settings.
            validate_mappings (bool): Whether to validate field mappings.

        Returns:
            SchemaValidationResult: Detailed validation results.

        Example:
            validator = SchemaValidator()
            expected = {
                "settings": {"number_of_shards": 1},
                "mappings": {
                    "properties": {
                        "name": {"type": "text"},
                        "age": {"type": "integer"}
                    }
                }
            }
            result = validator.validate_index("my_index", expected, es=client)
            if not result.is_valid:
                print(result)
        """
        # Get actual schema from Elasticsearch
        actual_schema = get_index_schema(index=index, es=es)

        return self.validate_schema(
            expected_schema=expected_schema,
            actual_schema=actual_schema,
            validate_settings=validate_settings,
            validate_mappings=validate_mappings,
        )

    def _validate_settings(
        self,
        expected_settings: Dict[str, Any],
        actual_settings: Dict[str, Any],
        result: SchemaValidationResult,
    ):
        """Validate that index settings match expected configuration."""
        # Flatten nested settings for comparison
        expected_flat = self._flatten_dict(expected_settings)
        actual_flat = self._flatten_dict(actual_settings)

        # Check for missing and mismatched settings
        for key, expected_value in expected_flat.items():
            if key not in actual_flat:
                result.add_error(f"Missing setting: {key}")
            elif actual_flat[key] != expected_value:
                result.setting_mismatches[key] = (expected_value, actual_flat[key])
                if self.strict_mode:
                    result.add_error(
                        f"Setting mismatch for '{key}': expected '{expected_value}', got '{actual_flat[key]}'"
                    )
                else:
                    result.add_warning(
                        f"Setting mismatch for '{key}': expected '{expected_value}', got '{actual_flat[key]}'"
                    )

    def _validate_mappings(
        self,
        expected_mappings: Dict[str, Any],
        actual_mappings: Dict[str, Any],
        result: SchemaValidationResult,
    ):
        """Validate that field mappings match expected configuration."""
        expected_properties = expected_mappings.get("properties", {})
        actual_properties = actual_mappings.get("properties", {})

        # Get field sets
        expected_fields = set(expected_properties.keys())
        actual_fields = set(actual_properties.keys())

        # Check for missing fields
        missing = expected_fields - actual_fields
        if missing:
            result.missing_fields = sorted(missing)
            for field in missing:
                result.add_error(f"Missing field in schema: {field}")

        # Check for extra fields
        if not self.ignore_extra_fields:
            extra = actual_fields - expected_fields
            if extra:
                result.extra_fields = sorted(extra)
                for field in extra:
                    if self.strict_mode:
                        result.add_error(f"Unexpected field in schema: {field}")
                    else:
                        result.add_warning(f"Extra field in schema: {field}")

        # Check field types and settings for common fields
        common_fields = expected_fields & actual_fields
        for field in common_fields:
            self._validate_field(
                field,
                expected_properties[field],
                actual_properties[field],
                result,
            )

    def _validate_field(
        self,
        field_name: str,
        expected_config: Dict[str, Any],
        actual_config: Dict[str, Any],
        result: SchemaValidationResult,
    ):
        """Validate a single field's configuration by comparing dictionaries."""
        # Check if the configurations match exactly
        if expected_config != actual_config:

            result.type_mismatches[field_name] = (expected_config, actual_config)

            if self.strict_mode:
                result.add_error(
                    f"Field configuration mismatch for '{field_name}': "
                    f"expected {expected_config}, got {actual_config}"
                )
            else:
                result.add_warning(
                    f"Field configuration mismatch for '{field_name}': "
                    f"expected {expected_config}, got {actual_config}"
                )

    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        """Flatten a nested dictionary into a single-level dictionary with dot notation keys."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict) and v:
                # Only flatten if it's not a special Elasticsearch object
                # (like analyzers, normalizers which should be compared as objects)
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


def validate_schema(
    expected_schema: Dict[str, Any],
    actual_schema: Dict[str, Any],
    strict_mode: bool = True,
    ignore_extra_fields: bool = False,
) -> SchemaValidationResult:
    """Convenience function to validate schemas.

    Args:
        expected_schema (Dict[str, Any]): The expected schema configuration.
        actual_schema (Dict[str, Any]): The actual schema from Elasticsearch.
        strict_mode (bool): If True, treat all mismatches as errors.
        ignore_extra_fields (bool): If True, don't report extra fields.

    Returns:
        SchemaValidationResult: Detailed validation results.

    Example:
        expected = {
            "mappings": {
                "properties": {
                    "name": {"type": "text"},
                    "age": {"type": "integer"}
                }
            }
        }
        result = validate_schema(expected, actual)
        print(result)
    """
    validator = SchemaValidator(strict_mode=strict_mode, ignore_extra_fields=ignore_extra_fields)
    return validator.validate_schema(expected_schema, actual_schema)


@requires_es_client
def validate_index(
    index: str,
    expected_schema: Dict[str, Any],
    es: Optional[Elasticsearch] = None,
    strict_mode: bool = True,
    ignore_extra_fields: bool = False,
) -> SchemaValidationResult:
    """Convenience function to validate an Elasticsearch index.

    Args:
        index (str): Name of the index to validate.
        expected_schema (Dict[str, Any]): The expected schema configuration.
        es (Optional[Elasticsearch]): Elasticsearch client (auto-injected if not provided).
        strict_mode (bool): If True, treat all mismatches as errors.
        ignore_extra_fields (bool): If True, don't report extra fields.

    Returns:
        SchemaValidationResult: Detailed validation results.

    Example:
        from es_utils.connection import connect_es

        client = connect_es(host='localhost')
        expected = {
            "mappings": {
                "properties": {
                    "name": {"type": "text"},
                    "age": {"type": "integer"}
                }
            }
        }
        result = validate_index("my_index", expected, es=client)
        if not result.is_valid:
            print(result)
    """
    validator = SchemaValidator(strict_mode=strict_mode, ignore_extra_fields=ignore_extra_fields)
    return validator.validate_index(index, expected_schema, es=es)
