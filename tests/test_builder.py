"""Test suite for builder.py - QueryBuilder class."""

import pytest

from src.es_query_gen.builder import QueryBuilder
from src.es_query_gen.models import (
    AggregationRule,
    EqualsFilter,
    QueryConfig,
    RangeFilter,
    SearchFilter,
    sortModel,
)


class TestQueryBuilder:
    """Test cases for QueryBuilder class."""

    def test_query_builder_initialization(self):
        """Test QueryBuilder initializes with empty bool query."""
        builder = QueryBuilder()
        assert builder.query == {"query": {"bool": {}}}

    def test_equals_filter(self):
        """Test _equals_filter adds term queries to must clause."""
        builder = QueryBuilder()
        equals_filters = [
            EqualsFilter(field="status", value="active"),
            EqualsFilter(field="type", value="premium"),
        ]
        builder._equals_filter(equals_filters)

        assert "must" in builder.query["query"]["bool"]
        assert len(builder.query["query"]["bool"]["must"]) == 2
        assert {"term": {"status": "active"}} in builder.query["query"]["bool"]["must"]
        assert {"term": {"type": "premium"}} in builder.query["query"]["bool"]["must"]

    def test_equals_filter_empty_list(self):
        """Test _equals_filter with empty list."""
        builder = QueryBuilder()
        builder._equals_filter([])
        assert "must" not in builder.query["query"]["bool"]

    def test_not_equals_filter(self):
        """Test _not_equals_filter adds term queries to must_not clause."""
        builder = QueryBuilder()
        not_equals_filters = [
            EqualsFilter(field="deleted", value=True),
            EqualsFilter(field="archived", value=True),
        ]
        builder._not_equals_filter(not_equals_filters)

        assert "must_not" in builder.query["query"]["bool"]
        assert len(builder.query["query"]["bool"]["must_not"]) == 2
        assert {"term": {"deleted": True}} in builder.query["query"]["bool"]["must_not"]
        assert {"term": {"archived": True}} in builder.query["query"]["bool"]["must_not"]

    def test_not_equals_filter_empty_list(self):
        """Test _not_equals_filter with empty list."""
        builder = QueryBuilder()
        builder._not_equals_filter([])
        assert "must_not" not in builder.query["query"]["bool"]

    def test_range_filter_all_operators(self):
        """Test _range_filter with all operators."""
        builder = QueryBuilder()
        range_filters = [
            RangeFilter(field="age", gte=18, lte=65),
            RangeFilter(field="price", gt=10, lt=100),
        ]
        builder._range_filter(range_filters)

        assert "must" in builder.query["query"]["bool"]
        assert len(builder.query["query"]["bool"]["must"]) == 2

        # Check first range filter
        range1 = builder.query["query"]["bool"]["must"][0]
        assert "range" in range1
        assert "age" in range1["range"]
        assert range1["range"]["age"]["gte"] == 18
        assert range1["range"]["age"]["lte"] == 65

        # Check second range filter
        range2 = builder.query["query"]["bool"]["must"][1]
        assert "range" in range2
        assert "price" in range2["range"]
        assert range2["range"]["price"]["gt"] == 10
        assert range2["range"]["price"]["lt"] == 100

    def test_range_filter_extends_existing_must(self):
        """Test _range_filter extends existing must clause."""
        builder = QueryBuilder()
        # First add an equals filter
        builder._equals_filter([EqualsFilter(field="status", value="active")])
        # Then add range filter
        builder._range_filter([RangeFilter(field="age", gte=18)])

        assert len(builder.query["query"]["bool"]["must"]) == 2
        assert {"term": {"status": "active"}} in builder.query["query"]["bool"]["must"]

    def test_range_filter_empty_list(self):
        """Test _range_filter with empty list."""
        builder = QueryBuilder()
        builder._range_filter([])
        assert "must" not in builder.query["query"]["bool"]

    def test_add_filter_with_all_filter_types(self):
        """Test _add_filter with all filter types."""
        builder = QueryBuilder()
        search_filter = SearchFilter(
            equals=[EqualsFilter(field="status", value="active")],
            notEquals=[EqualsFilter(field="deleted", value=True)],
            rangeFilters=[RangeFilter(field="age", gte=18, lte=65)],
        )
        builder._add_filter(search_filter)

        assert "must" in builder.query["query"]["bool"]
        assert "must_not" in builder.query["query"]["bool"]
        assert len(builder.query["query"]["bool"]["must"]) == 2  # equals + range
        assert len(builder.query["query"]["bool"]["must_not"]) == 1

    def test_add_filter_empty(self):
        """Test _add_filter with empty SearchFilter."""
        builder = QueryBuilder()
        builder._add_filter(SearchFilter())
        assert builder.query == {"query": {"bool": {}}}

    def test_add_sort_single(self):
        """Test _add_sort with single sort field."""
        builder = QueryBuilder()
        sort_list = [sortModel(field="created_at", order="desc")]
        builder._add_sort(sort_list)

        assert "sort" in builder.query
        assert builder.query["sort"] == [{"created_at": {"order": "desc"}}]

    def test_add_sort_multiple(self):
        """Test _add_sort with multiple sort fields."""
        builder = QueryBuilder()
        sort_list = [
            sortModel(field="priority", order="desc"),
            sortModel(field="created_at", order="asc"),
        ]
        builder._add_sort(sort_list)

        assert "sort" in builder.query
        assert len(builder.query["sort"]) == 2
        assert builder.query["sort"][0] == {"priority": {"order": "desc"}}
        assert builder.query["sort"][1] == {"created_at": {"order": "asc"}}

    def test_add_sort_none(self):
        """Test _add_sort with None."""
        builder = QueryBuilder()
        builder._add_sort(None)
        assert "sort" not in builder.query

    def test_add_size(self):
        """Test _add_size sets size value."""
        builder = QueryBuilder()
        builder._add_size(25)
        assert builder.query["size"] == 25

    def test_add_size_none(self):
        """Test _add_size with None."""
        builder = QueryBuilder()
        builder._add_size(None)
        assert "size" not in builder.query

    def test_add_include(self):
        """Test _add_include sets _source includes."""
        builder = QueryBuilder()
        return_fields = ["id", "name", "email"]
        builder._add_include(return_fields)

        assert "_source" in builder.query
        assert builder.query["_source"] == {"includes": ["id", "name", "email"]}

    def test_add_include_none(self):
        """Test _add_include with None."""
        builder = QueryBuilder()
        builder._add_include(None)
        assert "_source" not in builder.query

    def test_add_aggs_single_level(self):
        """Test _add_aggs with single aggregation level."""
        builder = QueryBuilder()
        aggs_list = [AggregationRule(name="by_category", field="category.keyword", size=10)]
        return_fields = ["id", "name"]
        size = 5

        builder._add_aggs(aggs_list, return_fields, size)

        assert builder.query["size"] == 0
        assert "sort" not in builder.query
        assert "aggs" in builder.query

        # Check aggregation structure
        assert "by_category" in builder.query["aggs"]
        assert "terms" in builder.query["aggs"]["by_category"]
        assert builder.query["aggs"]["by_category"]["terms"]["field"] == "category.keyword"
        assert builder.query["aggs"]["by_category"]["terms"]["size"] == 10

        # Check top_hits
        assert "aggs" in builder.query["aggs"]["by_category"]
        assert "top_hits_bucket" in builder.query["aggs"]["by_category"]["aggs"]
        top_hits = builder.query["aggs"]["by_category"]["aggs"]["top_hits_bucket"]["top_hits"]
        assert top_hits["size"] == 5
        assert top_hits["_source"]["includes"] == ["id", "name"]

    def test_add_aggs_multiple_levels(self):
        """Test _add_aggs with nested aggregations."""
        builder = QueryBuilder()
        aggs_list = [
            AggregationRule(name="by_category", field="category.keyword", size=10, order="desc"),
            AggregationRule(name="by_status", field="status.keyword", size=5, order="asc"),
        ]
        return_fields = ["id", "name"]
        size = 3

        builder._add_aggs(aggs_list, return_fields, size)

        # Check first level
        assert "by_category" in builder.query["aggs"]
        assert builder.query["aggs"]["by_category"]["terms"]["order"] == {"_key": "desc"}

        # Check nested level
        nested_aggs = builder.query["aggs"]["by_category"]["aggs"]
        assert "by_status" in nested_aggs
        assert nested_aggs["by_status"]["terms"]["field"] == "status.keyword"
        assert nested_aggs["by_status"]["terms"]["order"] == {"_key": "asc"}

        # Check top_hits at deepest level
        top_hits_aggs = nested_aggs["by_status"]["aggs"]
        assert "top_hits_bucket" in top_hits_aggs

    def test_add_aggs_three_levels(self):
        """Test _add_aggs with three aggregation levels."""
        builder = QueryBuilder()
        aggs_list = [
            AggregationRule(name="level1", field="field1.keyword", size=10),
            AggregationRule(name="level2", field="field2.keyword", size=5),
            AggregationRule(name="level3", field="field3.keyword", size=3),
        ]
        return_fields = ["id"]
        size = 2

        builder._add_aggs(aggs_list, return_fields, size)

        # Navigate through nested structure
        assert "level1" in builder.query["aggs"]
        level2 = builder.query["aggs"]["level1"]["aggs"]
        assert "level2" in level2
        level3 = level2["level2"]["aggs"]
        assert "level3" in level3
        assert "top_hits_bucket" in level3["level3"]["aggs"]

    def test_add_aggs_empty_list(self):
        """Test _add_aggs with empty list does nothing."""
        builder = QueryBuilder()
        builder.query["size"] = 10
        builder.query["sort"] = [{"field": {"order": "asc"}}]

        builder._add_aggs([], ["id"], 5)

        # Should not modify the query
        assert builder.query["size"] == 10
        assert "sort" in builder.query
        assert "aggs" not in builder.query

    def test_build_simple_search_query(self):
        """Test build method for a simple search query."""
        builder = QueryBuilder()
        config = QueryConfig(
            searchFilters=SearchFilter(equals=[EqualsFilter(field="status", value="active")]),
            sortList=[sortModel(field="created_at", order="desc")],
            size=10,
            returnFields=["id", "name"],
        )

        query = builder.build(config)

        assert query["size"] == 10
        assert "sort" in query
        assert "_source" in query
        assert "must" in query["query"]["bool"]
        assert "aggs" not in query

    def test_build_aggregation_query(self):
        """Test build method for an aggregation query."""
        builder = QueryBuilder()
        config = QueryConfig(
            aggs=[AggregationRule(name="by_status", field="status.keyword", size=10)],
            size=5,
            returnFields=["id", "name"],
        )

        query = builder.build(config)

        assert query["size"] == 0  # Should be set to 0 for aggregations
        assert "sort" not in query  # Sort should be removed
        assert "aggs" in query
        assert "_source" not in query  # Source is in top_hits

    def test_build_with_all_filters(self):
        """Test build method with all filter types."""
        builder = QueryBuilder()
        config = QueryConfig(
            searchFilters=SearchFilter(
                equals=[EqualsFilter(field="status", value="active")],
                notEquals=[EqualsFilter(field="deleted", value=True)],
                rangeFilters=[RangeFilter(field="age", gte=18, lte=65)],
            ),
            sortList=[sortModel(field="created_at", order="desc")],
            size=20,
            returnFields=["id", "name", "email"],
        )

        query = builder.build(config)

        assert "must" in query["query"]["bool"]
        assert "must_not" in query["query"]["bool"]
        assert len(query["query"]["bool"]["must"]) == 2  # equals + range

    def test_build_from_dict(self):
        """Test build method can accept dict and validate it."""
        builder = QueryBuilder()
        config_dict = {
            "searchFilters": {
                "equals": [{"field": "status", "value": "active"}],
            },
            "sortList": [{"field": "created_at", "order": "desc"}],
            "size": 10,
            "returnFields": ["id", "name"],
        }

        query = builder.build(config_dict)

        assert query["size"] == 10
        assert "sort" in query
        assert "_source" in query

    def test_build_minimal_config(self):
        """Test build method with minimal config."""
        builder = QueryBuilder()
        config = QueryConfig()

        query = builder.build(config)

        # Should have basic structure
        assert "query" in query
        assert "bool" in query["query"]
        assert query["size"] == 1  # default size

    def test_build_complex_nested_aggs(self):
        """Test build method with complex nested aggregations."""
        builder = QueryBuilder()
        config = QueryConfig(
            searchFilters=SearchFilter(equals=[EqualsFilter(field="type", value="user")]),
            aggs=[
                AggregationRule(name="by_country", field="country.keyword", size=20),
                AggregationRule(name="by_city", field="city.keyword", size=10),
                AggregationRule(name="by_age_group", field="age_group.keyword", size=5),
            ],
            size=3,
            returnFields=["id", "name", "country", "city"],
        )

        query = builder.build(config)

        # Should have filters even with aggregations
        assert "must" in query["query"]["bool"]
        assert {"term": {"type": "user"}} in query["query"]["bool"]["must"]

        # Should have nested aggregations
        assert query["size"] == 0
        assert "aggs" in query
        assert "by_country" in query["aggs"]

    def test_build_preserves_range_filter_with_date(self):
        """Test build method preserves date range filters correctly."""
        builder = QueryBuilder()
        config = QueryConfig(
            searchFilters=SearchFilter(
                rangeFilters=[
                    RangeFilter(
                        field="created_at",
                        gte={"days": -30},
                        lte={"days": 0},
                        rangeType="date",
                        dateFormat="%Y-%m-%d",
                    )
                ]
            ),
            size=10,
        )

        query = builder.build(config)

        # Check that range filter exists with string dates
        range_filter = query["query"]["bool"]["must"][0]
        assert "range" in range_filter
        assert "created_at" in range_filter["range"]
        assert isinstance(range_filter["range"]["created_at"]["gte"], str)
        assert isinstance(range_filter["range"]["created_at"]["lte"], str)
