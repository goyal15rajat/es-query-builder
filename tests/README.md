# ES Query Gen - Test Suite

This directory contains the comprehensive test suite for the `es-query-gen` library.

## Test Structure

```
tests/
├── __init__.py                 # Test package initialization
├── conftest.py                 # Shared pytest fixtures and configuration
├── test_models.py              # Tests for Pydantic models
├── test_builder.py             # Tests for QueryBuilder class
├── test_parser.py              # Tests for ESResponseParser class
├── test_connection.py          # Tests for ES connection and operations
├── test_integration.py         # Integration tests (require running ES)
└── README.md                   # This file
```

## Running Tests

### Install Test Dependencies

First, install the development dependencies:

```bash
# Using pip
pip install -e ".[dev]"

# Using uv (recommended - much faster)
uv pip install -e ".[dev]"
```

### Run All Unit Tests

Run all unit tests (excluding integration tests):

```bash
pytest
```

Or explicitly exclude integration tests:

```bash
pytest -m "not integration"
```

### Run Specific Test Files

```bash
# Test models only
pytest tests/test_models.py

# Test builder only
pytest tests/test_builder.py

# Test parser only
pytest tests/test_parser.py

# Test connection only
pytest tests/test_connection.py
```

### Run Tests with Coverage

```bash
# Run with coverage report
pytest --cov=src/es_query_gen --cov-report=html

# View HTML coverage report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
start htmlcov/index.html  # Windows
```

### Run Integration Tests

Integration tests require a running Elasticsearch instance. Set up your local ES instance first (you can use the provided docker-compose in `elastic-start-local/`):

```bash
# Start local Elasticsearch
cd elastic-start-local
./start.sh

# Run integration tests
pytest -m integration

# Or run all tests including integration
pytest -m ""
```

#### Configure Integration Tests

Set environment variables to configure the ES connection for integration tests:

```bash
export ES_HOST=localhost
export ES_PORT=9200
export ES_USERNAME=elastic
export ES_PASSWORD=your_password
export ES_SCHEME=http

# Run integration tests
pytest -m integration
```

### Run Tests in Verbose Mode

```bash
pytest -v
```

### Run Tests with Different Output Formats

```bash
# Short traceback format
pytest --tb=short

# No traceback
pytest --tb=no

# Show local variables in tracebacks
pytest -l
```

## Test Categories

### Unit Tests

Unit tests mock external dependencies (like Elasticsearch) and test individual components in isolation:

- **test_models.py**: Tests Pydantic model validation, field validators, and data transformations
- **test_builder.py**: Tests query building logic without connecting to ES
- **test_parser.py**: Tests response parsing logic with mock ES responses
- **test_connection.py**: Tests connection management with mocked ES clients

### Integration Tests

Integration tests require a running Elasticsearch instance and test the library end-to-end:

- **test_integration.py**: Tests actual ES connections, query execution, and response parsing

Run integration tests separately:

```bash
pytest -m integration
```

Skip integration tests:

```bash
pytest -m "not integration"
```

## Test Fixtures

Common test fixtures are defined in `conftest.py`:

- `mock_es_client`: Mock synchronous Elasticsearch client
- `mock_async_es_client`: Mock asynchronous Elasticsearch client
- `sample_search_response`: Sample ES search response
- `sample_aggregation_response`: Sample ES aggregation response
- `sample_nested_aggregation_response`: Sample nested aggregation response
- `sample_query_config_dict`: Sample query configuration
- `sample_aggregation_config_dict`: Sample aggregation configuration
- `sample_index_schema`: Sample ES index mapping

## Coverage Goals

The test suite aims for high code coverage:

- **Models**: 100% coverage of validation logic
- **Builder**: 100% coverage of query building methods
- **Parser**: 100% coverage of response parsing
- **Connection**: High coverage of connection management and operations

## Writing New Tests

When adding new features, follow these guidelines:

1. **Write unit tests first** - Test new functionality in isolation
2. **Use descriptive test names** - Name should describe what is being tested
3. **Follow AAA pattern** - Arrange, Act, Assert
4. **Use fixtures** - Reuse common test data from conftest.py
5. **Add integration tests** - For features that interact with ES
6. **Mark tests appropriately** - Use `@pytest.mark.integration` for integration tests

### Example Test

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

## Continuous Integration

Tests should be run in CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    pytest -m "not integration" --cov=src/es_query_gen
```

## Troubleshooting

### Tests Fail to Import Module

Make sure the package is installed in development mode:

```bash
pip install -e .
```

### Integration Tests Skip

Integration tests will skip if:
- Elasticsearch is not running
- Connection parameters are incorrect
- Environment variables are not set

Check the test output for skip reasons.

### Async Tests Fail

Make sure `pytest-asyncio` is installed:

```bash
pip install pytest-asyncio
```

## Test Maintenance

- Keep tests up to date with code changes
- Remove obsolete tests when features are removed
- Refactor tests when they become too complex
- Update fixtures when response formats change
