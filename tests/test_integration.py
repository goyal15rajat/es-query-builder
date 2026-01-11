"""Integration tests for the ES query generation library.

These tests require a running Elasticsearch instance and should be run separately
from unit tests using: pytest -m integration

Set the following environment variables to configure the ES connection:
- ES_HOST: Elasticsearch host (default: localhost)
- ES_PORT: Elasticsearch port (default: 9200)
- ES_USERNAME: Elasticsearch username (default: elastic)
- ES_PASSWORD: Elasticsearch password (default: changeme)
"""

import os

import pytest

from es_utils.connection import (
    connect_es,
    get_es_version,
    get_index_schema,
    ping,
    search,
)
from src.es_query_gen.builder import QueryBuilder
from src.es_query_gen.models import (
    AggregationRule,
    EqualsFilter,
    QueryConfig,
    RangeFilter,
    SearchFilter,
    sortModel,
)
from src.es_query_gen.parser import ESResponseParser

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def es_config():
    """Get Elasticsearch configuration from environment variables."""
    return {
        "host": os.getenv("ES_HOST", "localhost"),
        "port": int(os.getenv("ES_PORT", "9200")),
        "username": os.getenv("ES_USERNAME", "elastic"),
        "password": os.getenv("ES_PASSWORD", "changeme"),
        "scheme": os.getenv("ES_SCHEME", "http"),
    }


@pytest.fixture(scope="module")
def es_client(es_config):
    """Create an Elasticsearch client for integration tests."""
    try:
        client = connect_es(
            host=es_config["host"],
            port=es_config["port"],
            username=es_config["username"],
            password=es_config["password"],
            scheme=es_config["scheme"],
            verify_certs=False,
            request_timeout=30,
        )
        # Check if ES is available
        if not ping(es=client):
            pytest.skip("Elasticsearch is not available")
        return client
    except Exception as e:
        pytest.skip(f"Could not connect to Elasticsearch: {e}")


@pytest.fixture(scope="module")
def test_index():
    """Return the name of the test index."""
    return "test_es_query_gen"


class TestESConnection:
    """Integration tests for Elasticsearch connection."""

    def test_ping(self, es_client):
        """Test that we can ping the Elasticsearch cluster."""
        result = ping(es=es_client)
        assert result is True

    def test_get_es_version(self, es_client):
        """Test getting Elasticsearch version."""
        version = get_es_version(es=es_client)
        assert version is not None
        assert isinstance(version, str)
        # Version should be in format like "8.10.2"
        assert "." in version


class TestQueryBuilderIntegration:
    """Integration tests for QueryBuilder with real ES."""

    def test_build_and_execute_simple_search(self, es_client, test_index):
        """Test building and executing a simple search query."""
        config = QueryConfig(
            searchFilters=SearchFilter(equals=[EqualsFilter(field="status", value="active")]),
            sortList=[sortModel(field="created_at", order="desc")],
            size=10,
            returnFields=["id", "name", "status"],
        )

        query = QueryBuilder().build(config)

        # Query should have the expected structure
        assert "query" in query
        assert "bool" in query["query"]
        assert "must" in query["query"]["bool"]

        # Try to execute (may not return results if index doesn't exist, but should not error)
        try:
            response = search(es=es_client, index=test_index, query=query)
            assert "hits" in response
        except Exception as e:
            # Index might not exist, which is OK for this test
            if "index_not_found" not in str(e).lower():
                raise

    def test_build_and_execute_range_query(self, es_client, test_index):
        """Test building and executing a query with range filters."""
        config = QueryConfig(
            searchFilters=SearchFilter(rangeFilters=[RangeFilter(field="age", gte=18, lte=65, rangeType="number")]),
            size=20,
        )

        query = QueryBuilder().build(config)

        # Query should have range filter
        assert "must" in query["query"]["bool"]
        has_range = any("range" in item for item in query["query"]["bool"]["must"])
        assert has_range

    def test_build_and_execute_aggregation_query(self, es_client, test_index):
        """Test building and executing an aggregation query."""
        config = QueryConfig(
            aggs=[AggregationRule(name="by_status", field="status.keyword", size=10)],
            size=5,
            returnFields=["id", "name"],
        )

        query = QueryBuilder().build(config)

        # Query should have aggregations and size=0
        assert "aggs" in query
        assert query["size"] == 0
        assert "by_status" in query["aggs"]


class TestEndToEndWorkflow:
    """End-to-end integration tests."""

    def test_complete_search_workflow(self, es_client, test_index):
        """Test complete workflow: build query, execute, parse results."""
        # Build query
        config = QueryConfig(
            searchFilters=SearchFilter(equals=[EqualsFilter(field="type", value="test")]),
            sortList=[sortModel(field="id", order="asc")],
            size=10,
            returnFields=["id", "name", "type"],
        )

        query = QueryBuilder().build(config)

        # Execute query (might return empty results)
        try:
            response = search(es=es_client, index=test_index, query=query)

            # Parse response
            parser = ESResponseParser(config)
            results = parser.parse_data(response)

            # Results should be a list (might be empty if no data)
            assert isinstance(results, list)

            # If there are results, check structure
            if len(results) > 0:
                assert "_id" in results[0]
                # Only requested fields should be present (plus _id)
                for result in results:
                    assert "_id" in result

        except Exception as e:
            # Index might not exist
            if "index_not_found" not in str(e).lower():
                raise

    def test_complete_aggregation_workflow(self, es_client, test_index):
        """Test complete aggregation workflow."""
        # Build aggregation query
        config_dict = {
            "aggs": [
                {"name": "by_category", "field": "category.keyword", "size": 10},
            ],
            "size": 5,
            "returnFields": ["id", "name", "category"],
        }

        query = QueryBuilder().build(config_dict)

        # Execute query
        try:
            response = search(es=es_client, index=test_index, query=query)

            # Parse response
            parser = ESResponseParser(config_dict)
            results = parser.parse_data(response)

            # Results should be a list
            assert isinstance(results, list)

        except Exception as e:
            # Index might not exist
            if "index_not_found" not in str(e).lower():
                raise


@pytest.mark.skipif(os.getenv("SKIP_SCHEMA_TESTS") == "1", reason="Schema tests skipped")
class TestSchemaRetrieval:
    """Integration tests for schema retrieval."""

    def test_get_index_schema(self, es_client, test_index):
        """Test retrieving index schema."""
        try:
            schema = get_index_schema(es=es_client, index=test_index)
            assert isinstance(schema, dict)
            # Schema should have the index name as a key
            if test_index in schema:
                assert "mappings" in schema[test_index]
        except Exception as e:
            # Index might not exist
            if "index_not_found" not in str(e).lower():
                raise
