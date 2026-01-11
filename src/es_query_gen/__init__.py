"""ES Query Generator - Build complex Elasticsearch queries from simple Python dictionaries."""

from .builder import QueryBuilder
from .es_utils import (
    ESClientSingleton,
    SchemaValidationResult,
    SchemaValidator,
    connect_es,
    connect_es_async,
    search,
    search_async,
    validate_index,
    validate_schema,
)
from .parser import ESResponseParser

__all__ = [
    # Main classes
    "QueryBuilder",
    "ESResponseParser",
    # Connection utilities
    "ESClientSingleton",
    "connect_es",
    "connect_es_async",
    "search",
    "search_async",
    # Schema validation
    "SchemaValidationResult",
    "SchemaValidator",
    "validate_index",
    "validate_schema",
]
