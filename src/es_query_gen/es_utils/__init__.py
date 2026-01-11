"""Elasticsearch utilities package."""

from .connection import (
    ESClientSingleton,
    connect_es,
    connect_es_async,
    get_es_version,
    get_index_schema,
    get_index_schema_async,
    get_index_settings,
    get_index_settings_async,
    search,
    search_async,
)
from .schema_validator import (
    SchemaValidationResult,
    SchemaValidator,
    validate_index,
    validate_schema,
)

__all__ = [
    # Connection
    "ESClientSingleton",
    "connect_es",
    "connect_es_async",
    "get_es_version",
    "get_index_schema",
    "get_index_schema_async",
    "get_index_settings",
    "get_index_settings_async",
    "search",
    "search_async",
    # Schema Validation
    "SchemaValidationResult",
    "SchemaValidator",
    "validate_index",
    "validate_schema",
]
