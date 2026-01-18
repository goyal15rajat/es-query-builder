"""ES Query Generator - Build complex Elasticsearch queries from simple Python dictionaries."""

import logging

from .builder import QueryBuilder
from .es_utils.connection import (
    ESClientSingleton,
    connect_es,
    connect_es_async,
    es_search,
    es_search_async,
    get_es_version,
    get_index_schema,
    get_index_schema_async,
    get_index_settings,
    get_index_settings_async,
)
from .es_utils.schema_validator import validate_index
from .parser import ESResponseParser

__all__ = [
    # Main classes
    "QueryBuilder",
    "ESResponseParser",
    # Connection utilities
    "ESClientSingleton",
    "connect_es",
    "connect_es_async",
    "es_search",
    "es_search_async",
    "get_es_version",
    "get_index_schema",
    "get_index_schema_async",
    "get_index_settings",
    "get_index_settings_async",
    # Schema validation
    "validate_index",
]

# Add NullHandler to prevent "No handler found" warnings if the consuming
# application doesn't configure logging. The consuming application can
# configure logging for 'es_query_gen' to capture logs from this library.
logging.getLogger(__name__).addHandler(logging.NullHandler())
