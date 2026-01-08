# ES Query Gen - Test Suite Summary

## Overview

A comprehensive test suite for the `es-query-gen` Elasticsearch query generation library with 165+ tests covering all library components.

## Test Statistics

| Component | Test File | Test Count | Coverage |
|-----------|-----------|------------|----------|
| Models | test_models.py | 50+ tests | 100% |
| Query Builder | test_builder.py | 40+ tests | 100% |
| Response Parser | test_parser.py | 35+ tests | 100% |
| Connection | test_connection.py | 40+ tests | High |
| Integration | test_integration.py | 10+ tests | E2E |
| **Total** | **5 files** | **165+ tests** | **~95%** |

## Test Categories

### 1. Models Tests (test_models.py)

Tests all Pydantic models and their validation logic:

- **EqualsFilter**: String, int, bool, list, float values
- **RangeFilter**: Numeric ranges (gt, gte, lt, lte), date ranges with relative offsets, validation errors
- **sortModel**: Ascending/descending order, validation
- **SearchFilter**: Equals, not equals, range filters, combinations
- **AggregationRule**: Size validation, order specification, field configuration
- **QueryConfig**: Search queries, aggregation queries, size limits, field selection

**Example Tests:**
- `test_range_filter_date_with_relative_offset` - Tests date range with `{"days": -30}` offset
- `test_query_config_from_dict` - Tests creating QueryConfig from dictionary
- `test_aggregation_rule_size_validation_max` - Tests size boundary validation

### 2. Query Builder Tests (test_builder.py)

Tests query construction from models to Elasticsearch DSL:

- **Filter Building**: Equals filters, not-equals filters, range filters
- **Query Composition**: Multiple filter types, combining filters
- **Sorting**: Single and multiple sort fields
- **Pagination**: Size and from parameters
- **Field Selection**: Return field specification
- **Aggregations**: Single level, nested (2-3 levels), with top_hits

**Example Tests:**
- `test_equals_filter` - Tests term query generation
- `test_add_aggs_multiple_levels` - Tests nested aggregation structure
- `test_build_simple_search_query` - Tests complete query building
- `test_build_from_dict` - Tests building from dictionary config

### 3. Response Parser Tests (test_parser.py)

Tests parsing Elasticsearch responses:

- **Search Results**: Simple hits, empty results, complex documents
- **Aggregations**: Single level, nested, multiple buckets
- **Linked List**: Aggregation configuration traversal
- **Data Extraction**: _source fields, _id preservation, all field types

**Example Tests:**
- `test_parse_search_results_simple` - Tests parsing hit documents
- `test_parse_aggs_recursively_nested` - Tests nested aggregation parsing
- `test_parse_data_three_level_nested_aggs` - Tests deep aggregation hierarchy
- `test_parse_data_preserves_all_source_fields` - Tests field preservation

### 4. Connection Tests (test_connection.py)

Tests ES client management and operations (with mocks):

- **Singleton Pattern**: Client storage and retrieval, thread safety
- **Connection Creation**: Various connection parameters, auth, SSL
- **Decorators**: `@requires_es_client`, `@requires_es_client_async`
- **Operations**: ping, search, get_index_schema, get_es_version
- **Error Handling**: Retries, timeouts, connection errors
- **Async Support**: All operations have async equivalents

**Example Tests:**
- `test_connect_with_auth` - Tests authentication setup
- `test_search_timeout_with_retry` - Tests retry logic
- `test_requires_es_client_decorator` - Tests decorator injection
- `test_search_async_success` - Tests async search

### 5. Integration Tests (test_integration.py)

End-to-end tests with real Elasticsearch (marked with `@pytest.mark.integration`):

- **Connection**: Ping cluster, get version, get schema
- **Query Execution**: Build and execute searches, aggregations
- **Complete Workflows**: Build → Execute → Parse

**Example Tests:**
- `test_complete_search_workflow` - Full search pipeline
- `test_complete_aggregation_workflow` - Full aggregation pipeline
- `test_get_es_version` - Retrieve actual ES version

## Shared Fixtures (conftest.py)

Reusable test data and mocks:

- `mock_es_client` - Mock sync ES client
- `mock_async_es_client` - Mock async ES client
- `sample_search_response` - Sample ES search response with hits
- `sample_aggregation_response` - Sample ES agg response with buckets
- `sample_nested_aggregation_response` - Nested agg response
- `sample_query_config_dict` - Query configuration example
- `sample_index_schema` - ES index mapping example

## Running Tests

```bash
# Install dependencies
pip install -e ".[dev]"

# Run all unit tests
pytest -m "not integration"

# Run with coverage
pytest -m "not integration" --cov=src/es_query_gen

# Run specific test file
pytest tests/test_models.py -v

# Run specific test
pytest tests/test_models.py::TestEqualsFilter::test_equals_filter_with_string_value

# Run integration tests (requires ES)
pytest -m integration

# Run all tests
pytest
```

## Test Configuration

Pytest configuration in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = ["-v", "--strict-markers", "--cov=src/es_query_gen"]
markers = [
    "integration: integration tests requiring ES",
    "unit: unit tests",
]
asyncio_mode = "auto"
```

## Coverage Report

Generate and view coverage:

```bash
pytest --cov=src/es_query_gen --cov-report=html
open htmlcov/index.html
```

Expected coverage:
- Overall: ~95%
- Models: 100%
- Builder: 100%
- Parser: 100%
- Connection: 90%+

## Key Testing Patterns

### 1. Arrange-Act-Assert (AAA)

```python
def test_equals_filter_with_string_value():
    """Test EqualsFilter with string value."""
    # Arrange
    field = "status"
    value = "active"
    
    # Act
    filter_obj = EqualsFilter(field=field, value=value)
    
    # Assert
    assert filter_obj.field == field
    assert filter_obj.value == value
```

### 2. Parameterized Tests

```python
@pytest.mark.parametrize("field,value", [
    ("status", "active"),
    ("age", 25),
    ("deleted", True),
])
def test_equals_filter_various_types(field, value):
    filter_obj = EqualsFilter(field=field, value=value)
    assert filter_obj.value == value
```

### 3. Exception Testing

```python
def test_range_filter_missing_date_format():
    """Test RangeFilter raises error when dateFormat is missing."""
    with pytest.raises(ValidationError) as exc_info:
        RangeFilter(field="created_at", gte={"days": -30}, rangeType="date", dateFormat=None)
    assert "dateFormat must be provided" in str(exc_info.value)
```

### 4. Mock Usage

```python
def test_search_success():
    """Test search function with successful query."""
    mock_client = Mock(spec=Elasticsearch)
    expected_response = {"hits": {"total": {"value": 1}, "hits": []}}
    mock_client.search.return_value = expected_response
    
    result = search(es=mock_client, index="test_index")
    
    assert result == expected_response
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: pip install -e ".[dev]"
      - name: Run tests
        run: pytest -m "not integration" --cov=src/es_query_gen
      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

## Maintenance

- **Update tests** when adding features
- **Remove obsolete tests** when removing features
- **Refactor** when tests become complex
- **Add integration tests** for new ES interactions
- **Keep fixtures** up to date with ES response formats

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [pytest-cov](https://pytest-cov.readthedocs.io/)
- [Python unittest.mock](https://docs.python.org/3/library/unittest.mock.html)
- [Pydantic Testing](https://docs.pydantic.dev/latest/concepts/validation/)

## Contributing

When adding tests:

1. Follow existing patterns and naming conventions
2. Use descriptive test names that explain what is being tested
3. Add docstrings to complex tests
4. Use fixtures for common test data
5. Mark integration tests appropriately
6. Aim for high coverage without sacrificing test quality

## Questions?

See [tests/README.md](tests/README.md) for detailed testing guide.
