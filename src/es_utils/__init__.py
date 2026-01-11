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
    requires_es_client,
    requires_es_client_async,
    search,
    search_async,
)
from .schema_validatior import (
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
    "requires_es_client",
    "requires_es_client_async",
    "search",
    "search_async",
    # Schema Validation
    "SchemaValidationResult",
    "SchemaValidator",
    "validate_index",
    "validate_schema",
]
