"""Pytest configuration and shared fixtures for es-query-gen tests."""

from unittest.mock import Mock

import pytest
from elasticsearch import AsyncElasticsearch, Elasticsearch


@pytest.fixture
def mock_es_client():
    """Provide a mock Elasticsearch client for testing."""
    return Mock(spec=Elasticsearch)


@pytest.fixture
def mock_async_es_client():
    """Provide a mock AsyncElasticsearch client for testing."""
    return Mock(spec=AsyncElasticsearch)


@pytest.fixture
def sample_search_response():
    """Provide a sample Elasticsearch search response."""
    return {
        "took": 5,
        "timed_out": False,
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": {
            "total": {"value": 3, "relation": "eq"},
            "max_score": 1.0,
            "hits": [
                {
                    "_index": "test_index",
                    "_id": "1",
                    "_score": 1.0,
                    "_source": {"name": "Alice", "age": 30, "city": "New York"},
                },
                {
                    "_index": "test_index",
                    "_id": "2",
                    "_score": 0.9,
                    "_source": {"name": "Bob", "age": 25, "city": "San Francisco"},
                },
                {
                    "_index": "test_index",
                    "_id": "3",
                    "_score": 0.8,
                    "_source": {"name": "Charlie", "age": 35, "city": "Boston"},
                },
            ],
        },
    }


@pytest.fixture
def sample_aggregation_response():
    """Provide a sample Elasticsearch aggregation response."""
    return {
        "took": 10,
        "timed_out": False,
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": {"total": {"value": 100, "relation": "eq"}, "max_score": None, "hits": []},
        "aggregations": {
            "by_city": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
                "buckets": [
                    {
                        "key": "New York",
                        "doc_count": 40,
                        "top_hits_bucket": {
                            "hits": {
                                "total": {"value": 40, "relation": "eq"},
                                "max_score": 1.0,
                                "hits": [
                                    {"_id": "1", "_source": {"name": "Alice", "age": 30, "city": "New York"}},
                                    {"_id": "4", "_source": {"name": "David", "age": 28, "city": "New York"}},
                                ],
                            }
                        },
                    },
                    {
                        "key": "San Francisco",
                        "doc_count": 35,
                        "top_hits_bucket": {
                            "hits": {
                                "total": {"value": 35, "relation": "eq"},
                                "max_score": 1.0,
                                "hits": [
                                    {"_id": "2", "_source": {"name": "Bob", "age": 25, "city": "San Francisco"}},
                                ],
                            }
                        },
                    },
                    {
                        "key": "Boston",
                        "doc_count": 25,
                        "top_hits_bucket": {
                            "hits": {
                                "total": {"value": 25, "relation": "eq"},
                                "max_score": 1.0,
                                "hits": [
                                    {"_id": "3", "_source": {"name": "Charlie", "age": 35, "city": "Boston"}},
                                ],
                            }
                        },
                    },
                ],
            }
        },
    }


@pytest.fixture
def sample_nested_aggregation_response():
    """Provide a sample Elasticsearch nested aggregation response."""
    return {
        "took": 15,
        "timed_out": False,
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": {"total": {"value": 100, "relation": "eq"}, "max_score": None, "hits": []},
        "aggregations": {
            "by_city": {
                "buckets": [
                    {
                        "key": "New York",
                        "doc_count": 40,
                        "by_age_group": {
                            "buckets": [
                                {
                                    "key": "20-30",
                                    "doc_count": 20,
                                    "top_hits_bucket": {
                                        "hits": {
                                            "hits": [
                                                {
                                                    "_id": "1",
                                                    "_source": {
                                                        "name": "Alice",
                                                        "age": 25,
                                                        "city": "New York",
                                                        "age_group": "20-30",
                                                    },
                                                }
                                            ]
                                        }
                                    },
                                },
                                {
                                    "key": "30-40",
                                    "doc_count": 20,
                                    "top_hits_bucket": {
                                        "hits": {
                                            "hits": [
                                                {
                                                    "_id": "2",
                                                    "_source": {
                                                        "name": "Bob",
                                                        "age": 35,
                                                        "city": "New York",
                                                        "age_group": "30-40",
                                                    },
                                                }
                                            ]
                                        }
                                    },
                                },
                            ]
                        },
                    }
                ]
            }
        },
    }


@pytest.fixture
def sample_query_config_dict():
    """Provide a sample query configuration dictionary."""
    return {
        "searchFilters": {
            "equals": [{"field": "status", "value": "active"}, {"field": "type", "value": "premium"}],
            "notEquals": [{"field": "deleted", "value": True}],
            "rangeFilters": [{"field": "age", "gte": 18, "lte": 65, "rangeType": "number"}],
        },
        "sortList": [{"field": "created_at", "order": "desc"}],
        "size": 20,
        "returnFields": ["id", "name", "email", "created_at"],
    }


@pytest.fixture
def sample_aggregation_config_dict():
    """Provide a sample aggregation configuration dictionary."""
    return {
        "searchFilters": {"equals": [{"field": "type", "value": "user"}]},
        "aggs": [
            {"name": "by_city", "field": "city.keyword", "size": 10, "order": "desc"},
            {"name": "by_age_group", "field": "age_group.keyword", "size": 5, "order": "asc"},
        ],
        "size": 5,
        "returnFields": ["id", "name", "city", "age_group"],
    }


@pytest.fixture
def sample_date_range_config_dict():
    """Provide a sample configuration with date range filters."""
    return {
        "searchFilters": {
            "equals": [{"field": "type", "value": "event"}],
            "rangeFilters": [
                {
                    "field": "created_at",
                    "gte": {"days": -30},
                    "lte": {"days": 0},
                    "rangeType": "date",
                    "dateFormat": "%Y-%m-%d",
                },
                {
                    "field": "start_date",
                    "gte": {"months": -6},
                    "rangeType": "date",
                    "dateFormat": "%Y-%m-%d",
                },
            ],
        },
        "sortList": [{"field": "created_at", "order": "desc"}],
        "size": 50,
        "returnFields": ["id", "title", "created_at", "start_date"],
    }


@pytest.fixture
def sample_empty_response():
    """Provide an empty Elasticsearch response."""
    return {
        "took": 1,
        "timed_out": False,
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": {"total": {"value": 0, "relation": "eq"}, "max_score": None, "hits": []},
    }


@pytest.fixture
def sample_index_schema():
    """Provide a sample Elasticsearch index schema/mapping."""
    return {
        "test_index": {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "age": {"type": "integer"},
                    "email": {"type": "keyword"},
                    "city": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "created_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                    "status": {"type": "keyword"},
                    "tags": {"type": "keyword"},
                    "deleted": {"type": "boolean"},
                }
            }
        }
    }


# Configure pytest-asyncio
pytest_plugins = ["pytest_asyncio"]
