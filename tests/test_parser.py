"""Test suite for parser.py - ESResponseParser class."""

import pytest

from src.es_query_gen.models import AggregationRule, QueryConfig
from src.es_query_gen.parser import ESResponseParser, Node


class TestNode:
    """Test cases for Node class."""

    def test_node_creation(self):
        """Test Node creation with data."""
        data = {"name": "test", "field": "test.keyword"}
        node = Node(next=None, data=data)
        assert node.next is None
        assert node.data == data

    def test_node_linking(self):
        """Test linking nodes together."""
        node1 = Node(next=None, data={"name": "node1"})
        node2 = Node(next=None, data={"name": "node2"})
        node1.next = node2

        assert node1.next == node2
        assert node2.next is None


class TestESResponseParser:
    """Test cases for ESResponseParser class."""

    def test_parser_initialization(self):
        """Test ESResponseParser initialization."""
        config = QueryConfig(size=10)
        parser = ESResponseParser(config)
        assert parser.query_config == config
        assert parser.results == []

    def test_parse_search_results_simple(self):
        """Test parsing simple search results."""
        config = QueryConfig(size=10)
        parser = ESResponseParser(config)

        response = {
            "hits": {
                "total": {"value": 2},
                "hits": [
                    {"_id": "1", "_source": {"name": "Alice", "age": 30}},
                    {"_id": "2", "_source": {"name": "Bob", "age": 25}},
                ],
            }
        }

        parser.parse_search_results(response)

        assert len(parser.results) == 2
        assert parser.results[0] == {"name": "Alice", "age": 30, "_id": "1"}
        assert parser.results[1] == {"name": "Bob", "age": 25, "_id": "2"}

    def test_parse_search_results_empty(self):
        """Test parsing empty search results."""
        config = QueryConfig(size=10)
        parser = ESResponseParser(config)

        response = {"hits": {"total": {"value": 0}, "hits": []}}

        parser.parse_search_results(response)

        assert len(parser.results) == 0

    def test_parse_search_results_no_hits_key(self):
        """Test parsing response with missing hits key."""
        config = QueryConfig(size=10)
        parser = ESResponseParser(config)

        response = {}

        parser.parse_search_results(response)

        assert len(parser.results) == 0

    def test_parse_search_results_with_additional_fields(self):
        """Test parsing search results with additional ES fields."""
        config = QueryConfig(size=10)
        parser = ESResponseParser(config)

        response = {
            "hits": {
                "hits": [
                    {
                        "_id": "1",
                        "_index": "test_index",
                        "_score": 1.0,
                        "_source": {"name": "Alice", "email": "alice@example.com"},
                    }
                ]
            }
        }

        parser.parse_search_results(response)

        assert len(parser.results) == 1
        assert parser.results[0]["_id"] == "1"
        assert parser.results[0]["name"] == "Alice"
        assert "_index" not in parser.results[0]  # Only _source and _id are included

    def test_generate_aggs_linked_list_single(self):
        """Test generating linked list from single aggregation."""
        config = QueryConfig()
        parser = ESResponseParser(config)

        aggs_list = [{"name": "by_category", "field": "category.keyword"}]

        head = parser._generate_aggs_linked_list(aggs_list)

        assert head is not None
        assert head.data == aggs_list[0]
        assert head.next is None

    def test_generate_aggs_linked_list_multiple(self):
        """Test generating linked list from multiple aggregations."""
        config = QueryConfig()
        parser = ESResponseParser(config)

        aggs_list = [
            {"name": "by_category", "field": "category.keyword"},
            {"name": "by_status", "field": "status.keyword"},
            {"name": "by_date", "field": "date"},
        ]

        head = parser._generate_aggs_linked_list(aggs_list)

        assert head is not None
        assert head.data == aggs_list[0]
        assert head.next is not None
        assert head.next.data == aggs_list[1]
        assert head.next.next is not None
        assert head.next.next.data == aggs_list[2]
        assert head.next.next.next is None

    def test_generate_aggs_linked_list_empty(self):
        """Test generating linked list from empty list."""
        config = QueryConfig()
        parser = ESResponseParser(config)

        head = parser._generate_aggs_linked_list([])

        # With empty list, head should be None or the last current which would be None
        assert head is None

    def test_parse_aggs_recursively_single_level(self):
        """Test parsing single level aggregation."""
        config = QueryConfig(aggs=[AggregationRule(name="by_category", field="category.keyword", size=10)])
        parser = ESResponseParser(config)

        node = Node(
            next=None,
            data={"name": "by_category", "field": "category.keyword"},
        )

        aggs_data = {
            "by_category": {
                "buckets": [
                    {
                        "key": "electronics",
                        "doc_count": 5,
                        "top_hits_bucket": {
                            "hits": {
                                "hits": [
                                    {"_id": "1", "_source": {"name": "Product 1", "category": "electronics"}},
                                    {"_id": "2", "_source": {"name": "Product 2", "category": "electronics"}},
                                ]
                            }
                        },
                    }
                ]
            }
        }

        parser._parse_aggs_recursively(node, aggs_data)

        assert len(parser.results) == 2
        assert parser.results[0] == {"name": "Product 1", "category": "electronics", "_id": "1"}
        assert parser.results[1] == {"name": "Product 2", "category": "electronics", "_id": "2"}

    def test_parse_aggs_recursively_nested(self):
        """Test parsing nested aggregations."""
        config = QueryConfig(
            aggs=[
                AggregationRule(name="by_category", field="category.keyword", size=10),
                AggregationRule(name="by_status", field="status.keyword", size=5),
            ]
        )
        parser = ESResponseParser(config)

        # Create linked list
        node2 = Node(next=None, data={"name": "by_status", "field": "status.keyword"})
        node1 = Node(next=node2, data={"name": "by_category", "field": "category.keyword"})

        aggs_data = {
            "by_category": {
                "buckets": [
                    {
                        "key": "electronics",
                        "by_status": {
                            "buckets": [
                                {
                                    "key": "active",
                                    "top_hits_bucket": {
                                        "hits": {
                                            "hits": [
                                                {
                                                    "_id": "1",
                                                    "_source": {
                                                        "name": "Product 1",
                                                        "category": "electronics",
                                                        "status": "active",
                                                    },
                                                }
                                            ]
                                        }
                                    },
                                }
                            ]
                        },
                    }
                ]
            }
        }

        parser._parse_aggs_recursively(node1, aggs_data)

        assert len(parser.results) == 1
        assert parser.results[0]["name"] == "Product 1"
        assert parser.results[0]["_id"] == "1"

    def test_parse_aggs_recursively_multiple_buckets(self):
        """Test parsing aggregations with multiple buckets."""
        config = QueryConfig()
        parser = ESResponseParser(config)

        node = Node(next=None, data={"name": "by_category", "field": "category.keyword"})

        aggs_data = {
            "by_category": {
                "buckets": [
                    {
                        "key": "electronics",
                        "top_hits_bucket": {
                            "hits": {
                                "hits": [
                                    {"_id": "1", "_source": {"name": "Product 1"}},
                                ]
                            }
                        },
                    },
                    {
                        "key": "books",
                        "top_hits_bucket": {
                            "hits": {
                                "hits": [
                                    {"_id": "2", "_source": {"name": "Product 2"}},
                                    {"_id": "3", "_source": {"name": "Product 3"}},
                                ]
                            }
                        },
                    },
                ]
            }
        }

        parser._parse_aggs_recursively(node, aggs_data)

        assert len(parser.results) == 3

    def test_parse_aggs_recursively_empty_buckets(self):
        """Test parsing aggregations with empty buckets."""
        config = QueryConfig()
        parser = ESResponseParser(config)

        node = Node(next=None, data={"name": "by_category", "field": "category.keyword"})

        aggs_data = {"by_category": {"buckets": []}}

        parser._parse_aggs_recursively(node, aggs_data)

        assert len(parser.results) == 0

    def test_parse_aggs_recursively_missing_bucket_name(self):
        """Test parsing aggregations with missing bucket name."""
        config = QueryConfig()
        parser = ESResponseParser(config)

        node = Node(next=None, data={"name": "by_category", "field": "category.keyword"})

        aggs_data = {"different_name": {"buckets": []}}

        parser._parse_aggs_recursively(node, aggs_data)

        assert len(parser.results) == 0

    def test_parse_aggs_recursively_none_node(self):
        """Test parsing aggregations with None node."""
        config = QueryConfig()
        parser = ESResponseParser(config)

        parser._parse_aggs_recursively(None, {})

        assert len(parser.results) == 0

    def test_parse_aggregations(self):
        """Test parse_aggregations method."""
        config = {
            "aggs": [
                {"name": "by_category", "field": "category.keyword", "size": 10},
            ]
        }
        parser = ESResponseParser(config)

        response = {
            "aggregations": {
                "by_category": {
                    "buckets": [
                        {
                            "key": "electronics",
                            "top_hits_bucket": {
                                "hits": {
                                    "hits": [
                                        {"_id": "1", "_source": {"name": "Product 1"}},
                                    ]
                                }
                            },
                        }
                    ]
                }
            }
        }

        parser.parse_aggregations(response)

        assert len(parser.results) == 1
        assert parser.results[0]["_id"] == "1"

    def test_parse_aggregations_empty_response(self):
        """Test parse_aggregations with empty aggregations."""
        config = {"aggs": [{"name": "by_category", "field": "category.keyword", "size": 10}]}
        parser = ESResponseParser(config)

        response = {"aggregations": {}}

        parser.parse_aggregations(response)

        assert len(parser.results) == 0

    def test_parse_aggregations_no_aggregations_key(self):
        """Test parse_aggregations with missing aggregations key."""
        config = {"aggs": [{"name": "by_category", "field": "category.keyword", "size": 10}]}
        parser = ESResponseParser(config)

        response = {}

        parser.parse_aggregations(response)

        assert len(parser.results) == 0

    def test_parse_data_without_aggs(self):
        """Test parse_data for search results (no aggregations)."""
        config = QueryConfig(size=10)
        parser = ESResponseParser(config)

        response = {
            "hits": {
                "hits": [
                    {"_id": "1", "_source": {"name": "Alice"}},
                    {"_id": "2", "_source": {"name": "Bob"}},
                ]
            }
        }

        results = parser.parse_data(response)

        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Bob"

    def test_parse_data_with_aggs(self):
        """Test parse_data for aggregation results."""
        config = {"aggs": [{"name": "by_status", "field": "status.keyword", "size": 10}]}
        parser = ESResponseParser(config)

        response = {
            "aggregations": {
                "by_status": {
                    "buckets": [
                        {
                            "key": "active",
                            "top_hits_bucket": {
                                "hits": {
                                    "hits": [
                                        {"_id": "1", "_source": {"status": "active"}},
                                    ]
                                }
                            },
                        }
                    ]
                }
            }
        }

        results = parser.parse_data(response)

        assert len(results) == 1
        assert results[0]["status"] == "active"

    def test_parse_data_three_level_nested_aggs(self):
        """Test parse_data with three levels of nested aggregations."""
        config = {
            "aggs": [
                {"name": "level1", "field": "field1.keyword", "size": 10},
                {"name": "level2", "field": "field2.keyword", "size": 5},
                {"name": "level3", "field": "field3.keyword", "size": 3},
            ]
        }
        parser = ESResponseParser(config)

        response = {
            "aggregations": {
                "level1": {
                    "buckets": [
                        {
                            "key": "value1",
                            "level2": {
                                "buckets": [
                                    {
                                        "key": "value2",
                                        "level3": {
                                            "buckets": [
                                                {
                                                    "key": "value3",
                                                    "top_hits_bucket": {
                                                        "hits": {
                                                            "hits": [
                                                                {
                                                                    "_id": "1",
                                                                    "_source": {
                                                                        "field1": "value1",
                                                                        "field2": "value2",
                                                                        "field3": "value3",
                                                                    },
                                                                },
                                                                {
                                                                    "_id": "2",
                                                                    "_source": {
                                                                        "field1": "value1",
                                                                        "field2": "value2",
                                                                        "field3": "value3",
                                                                    },
                                                                },
                                                            ]
                                                        }
                                                    },
                                                }
                                            ]
                                        },
                                    }
                                ]
                            },
                        }
                    ]
                }
            }
        }

        results = parser.parse_data(response)

        assert len(results) == 2
        assert results[0]["field1"] == "value1"
        assert results[1]["field1"] == "value1"

    def test_parse_data_preserves_all_source_fields(self):
        """Test that parse_data preserves all _source fields."""
        config = QueryConfig(size=10)
        parser = ESResponseParser(config)

        response = {
            "hits": {
                "hits": [
                    {
                        "_id": "1",
                        "_source": {
                            "name": "Alice",
                            "age": 30,
                            "email": "alice@example.com",
                            "address": {"city": "New York", "country": "USA"},
                            "tags": ["python", "elasticsearch"],
                        },
                    }
                ]
            }
        }

        results = parser.parse_data(response)

        assert len(results) == 1
        result = results[0]
        assert result["name"] == "Alice"
        assert result["age"] == 30
        assert result["email"] == "alice@example.com"
        assert result["address"] == {"city": "New York", "country": "USA"}
        assert result["tags"] == ["python", "elasticsearch"]
        assert result["_id"] == "1"
