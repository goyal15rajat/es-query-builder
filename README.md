# es-query-gen
[![Tests](https://github.com/goyal15rajat/es-query-builder/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/goyal15rajat/es-query-builder/actions/workflows/tests.yml)

**A no-code Elasticsearch query generator** - Build complex ES queries from simple Python dictionaries using typed Pydantic models.

## Overview

This library provides four main components:

1. **Query Generator** - Convert simple configuration dictionaries into complex Elasticsearch DSL queries
2. **ES Client Wrapper** - Simplified connection management with retry logic, timeouts, and both sync/async support
3. **Response Parser** - Parse complex Elasticsearch responses including nested aggregations into clean Python objects
4. **Schema Validator** - Validate Elasticsearch index schemas to ensure settings and field mappings match expected configurations

## Features

- 🎯 **No-code query building** - Define queries using simple Python dicts or JSON
- 📝 **Typed models** - Full Pydantic validation for filters, aggregations, and configurations
- 🔄 **Query Builder** - Convert models into Elasticsearch DSL with support for:
  - Equality and inequality filters
  - Numeric and date range filters (with relative date offsets)
  - Sorting and pagination
  - Nested aggregations (unlimited depth)
  - Field selection
- 🔌 **ES Client Management** - Singleton pattern with connection pooling, retries, and error handling
- 📊 **Response Parser** - Extract documents from complex nested aggregations
- ⚡ **Async Support** - Full async/await support for all ES operations (`connect_es_async`, `search_async`, `get_index_schema_async`, etc.)
- ✅ **Schema Validator** - Validate index schemas against expected configurations with detailed reporting
- ✅ **100+ Tests** - Comprehensive test coverage with fixtures and integration tests

## Requirements

- Python 3.10+ (uses `match` statement)
- Pydantic 2.x
- Elasticsearch Python client 9.x

## Installation

### Using pip

Install from source in editable mode:

```bash
pip install -e .
```

### Using uv (recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package installer:

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the package
uv pip install -e .

# Or install with development dependencies
uv pip install -e ".[dev]"
```

## Quick Start

### 1. Build a Query (No Code!)

Define your query using a simple Python dictionary:

```python
from es_query_gen import QueryBuilder

# Define query using a simple dict - no ES DSL knowledge needed!
config = {
    "searchFilters": {
        "equals": [{"field": "status", "value": "active"}],
        "rangeFilters": [{"field": "age", "gte": 18, "lte": 65, "rangeType": "number"}]
    },
    "sortList": [{"field": "created_at", "order": "desc"}],
    "size": 10,
    "returnFields": ["id", "name", "email"]
}

# Build ES query automatically
query = QueryBuilder().build(config)
print(query)
# Output: Full Elasticsearch DSL query ready to execute!
```

### 2. Connect to Elasticsearch

**Synchronous:**

```python
from es_query_gen import connect_es, search

# Connect with automatic client management
client = connect_es(
    host='localhost',
    username='elastic',
    password='changeme',
    request_timeout=30
)

# Execute the query with retry logic
response = search(index='my_index', query=query)
```

**Asynchronous:**

```python
import asyncio
from es_query_gen import connect_es_async, search_async

async def main():
    # Connect with async client
    client = await connect_es_async(
        host='localhost',
        username='elastic',
        password='changeme',
        request_timeout=30
    )
    
    # Execute the query asynchronously
    response = await search_async(index='my_index', query=query)
    return response

# Run async code
response = asyncio.run(main())
```

### 3. Parse Complex Responses

```python
from es_query_gen import ESResponseParser

# Parse search results or complex nested aggregations
parser = ESResponseParser(config)
results = parser.parse_data(response)

# Results are clean Python dicts, even from nested aggregations!
for doc in results:
    print(doc['name'], doc['email'])
```

### 4. Validate Index Schemas

Ensure your Elasticsearch indices match expected configurations:

```python
from es_query_gen import validate_index, SchemaValidator

# Define expected schema
expected_schema = {
    "settings": {
        "number_of_shards": 1,
        "analysis": {
            "normalizer": {
                "lowercase_normalizer": {
                    "type": "custom",
                    "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "name": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword", "normalizer": "lowercase_normalizer"}
                }
            },
            "age": {"type": "integer"},
            "email": {"type": "keyword"}
        }
    }
}

# Validate against live index
result = validate_index("my_index", expected_schema, es=client)

if not result.is_valid:
    print(result)  # Detailed error report
    # Output:
    # Schema Validation: FAILED
    #
    # Errors (2):
    #   - Missing field in schema: email
    #   - Type mismatch for field 'age': expected 'integer', got 'text'
    #
    # Missing Fields (1):
    #   - email

# Use custom validator with options
validator = SchemaValidator(strict_mode=False, ignore_extra_fields=True)
result = validator.validate_index("my_index", expected_schema)

# Check specific validation details
if result.type_mismatches:
    for field, (expected, actual) in result.type_mismatches.items():
        print(f"Field {field}: expected {expected}, got {actual}")
```

**Schema Validator Features:**
- ✅ **Settings validation** - Compare index settings (shards, replicas, analyzers, normalizers)
- ✅ **Field type validation** - Ensure field types match (text, keyword, integer, date, etc.)
- ✅ **Field properties validation** - Validate format, analyzer, normalizer, and other field settings
- ✅ **Nested fields support** - Handle multi-fields and nested object properties
- ✅ **Detailed reporting** - Get comprehensive results with errors, warnings, and specific mismatches
- ✅ **Flexible modes** - Strict mode for errors, non-strict for warnings, ignore extra fields option
- ✅ **Convenience functions** - Simple one-line validation with `validate_index()` or `validate_schema()`

### Advanced Example: Nested Aggregations

Build complex aggregation queries without writing ES DSL:

```python
{
    "size": 10,
    "searchFilters": {
        "equals": [
            {
                "field": "age",
                "value": "35"
            }
        ],
        "rangeFilters": [
            {
                "field": "dob",
                "rangeType": "date",
                "dateFormat": "%m/%d/%Y",
                "gte": {
                    "month": 2,
                    "years": -60
                },
                "lt": {
                    "month": 9,
                    "day": 10,
                    "years": -20
                }
            }
        ]
    },
    "sortList": [
        {
            "field": "dob",
            "order": "asc"
        }
    ],
    "returnFields": ["name", "dob", "phone"],
    "aggs": [
        {
            "name": "address_bucket",
            "field": "address.keyword",
            "size": 100,
            "order": "asc"
        },
        {
            "name": "dob_bucket",
            "field": "dob",
            "size": 100,
            "order": "asc"
        }
    ]
}
```

## Testing

This library includes a comprehensive test suite with 100+ tests covering all components.

### Quick Start

Run all unit tests (excludes integration tests that require ES):

```bash
# Using pip
pip install -e ".[dev]"
pytest -m "not integration"

# Using uv (faster)
uv pip install -e ".[dev]"
pytest -m "not integration"

# Run with coverage report
pytest -m "not integration" --cov=src/es_query_gen --cov-report=html
```

Or use the provided test runner script:

```bash
./run_tests.sh
```

### Test Structure

- **tests/test_models.py** - Tests for Pydantic models and validators (50+ tests)
- **tests/test_builder.py** - Tests for QueryBuilder query construction (40+ tests)
- **tests/test_parser.py** - Tests for ESResponseParser (35+ tests)
- **tests/test_connection.py** - Tests for ES connection management (40+ tests)
- **tests/test_schema_validator.py** - Tests for schema validation (40+ tests)
- **tests/test_integration.py** - Integration tests (requires running ES)
- **tests/conftest.py** - Shared fixtures and test configuration

### Integration Tests

Integration tests require a running Elasticsearch instance:

```bash
# Start local ES (using provided docker-compose)
cd elastic-start-local && ./start.sh

# Configure connection (optional, defaults shown)
export ES_HOST=localhost
export ES_PORT=9200
export ES_USERNAME=elastic
export ES_PASSWORD=changeme

# Run integration tests
pytest -m integration
```

### Coverage

The test suite provides comprehensive coverage:
- Models: 100% - All validation logic and edge cases
- Builder: 100% - All query building paths
- Parser: 100% - Search and aggregation parsing
- Connection: High - All connection and operation flows
- Schema Validator: High - All validation scenarios and edge cases

View detailed coverage report:
```bash
pytest --cov=src/es_query_gen --cov-report=html
open htmlcov/index.html
```

See [tests/README.md](tests/README.md) for detailed testing documentation.

## API Components

### QueryBuilder
- Converts configuration dicts to ES DSL
- Supports filters, sorting, pagination, aggregations
- Handles date math and relative date ranges

### ES Client Wrapper
- Singleton pattern for connection management
- Automatic retry with exponential backoff
- Both sync and async support:
  - Sync: `connect_es()`, `search()`, `get_index_schema()`, `get_index_settings()`
  - Async: `connect_es_async()`, `search_async()`, `get_index_schema_async()`, `get_index_settings_async()`
- Decorators for client injection (`@requires_es_client`, `@requires_es_client_async`)

### Response Parser
- Extracts documents from search results
- Parses nested aggregations (any de

### Schema Validator
- Validates index schemas against expected configurations
- Compares settings and field mappings
- Supports strict and non-strict validation modes
- Detailed error and warning reporting
- Handles nested fields and complex structurespth)
- Preserves all _source fields + _id

## Contributing

Contributions are welcome! Here's how to get started:

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/es-query-gen.git
cd es-query-gen

# Using uv (recommended - much faster)
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e ".[dev]"

# Or using pip
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Run Tests

```bash
# Run unit tests
pytest -m "not integration"

# Run with coverage
pytest --cov=src/es_query_gen --cov-report=html

# Run all tests including integration (requires ES)
pytest
```

### Development Guidelines

1. **Keep changes small** - Focus on one feature/fix per PR
2. **Add tests** - All new features must include tests
3. **Follow existing patterns** - Match the code style
4. **Update documentation** - Keep README and docstrings current
5. **Type hints** - Use Pydantic models and type annotations

### Making Changes

```bash
# Create a feature branch
git checkout -b feature/your-feature-name

# Make your changes and add tests
# ...

# Run tests
pytest

# Commit with clear message
git commit -m "Add feature: description"

# Push and create PR
git push origin feature/your-feature-name
```

## License

MIT

## Notes

- Requires Python 3.10+ (uses `match` statement)
- The library is designed to be extended - you can add custom query builders or parsers
- For production use, consider implementing connection pooling based on your needs
