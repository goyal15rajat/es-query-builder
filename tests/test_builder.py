"""Test suite for builder.py - QueryBuilder class."""

import pytest

from src.es_query_gen.builder import QueryBuilder
from src.es_query_gen.models import (
    AggregationRule,
    EqualsFilter,
    FullTextFilter,
    QueryConfig,
    RangeFilter,
    SearchFilter,
    sortModel,
)

# ---------------------------------------------------------------------------
# Shared helper — fresh query skeleton used by unit tests for private methods
# ---------------------------------------------------------------------------


def _q():
    """Return a fresh empty bool query dict."""
    return {"query": {"bool": {}}}


class TestQueryBuilder:
    """Test cases for QueryBuilder static methods."""

    # -- _create_term_query --------------------------------------------------

    def test_create_term_query_with_scalar_value(self):
        """_create_term_query returns a term query for scalar values."""
        result = QueryBuilder._create_term_query("status", "active")
        assert result == {"term": {"status": "active"}}

    def test_create_term_query_with_list_value(self):
        """_create_term_query returns a terms query for list values."""
        result = QueryBuilder._create_term_query("status", ["active", "pending"])
        assert result == {"terms": {"status": ["active", "pending"]}}

    # -- _equals_filter → bool.filter ----------------------------------------

    def test_equals_filter_goes_to_filter_clause(self):
        """_equals_filter puts term queries in bool.filter (not must)."""
        q = _q()
        QueryBuilder._equals_filter(
            q,
            [
                EqualsFilter(field="status", value="active"),
                EqualsFilter(field="type", value="premium"),
            ],
        )

        assert "filter" in q["query"]["bool"]
        assert "must" not in q["query"]["bool"]
        assert len(q["query"]["bool"]["filter"]) == 2
        assert {"term": {"status": "active"}} in q["query"]["bool"]["filter"]
        assert {"term": {"type": "premium"}} in q["query"]["bool"]["filter"]

    def test_equals_filter_empty_list(self):
        """_equals_filter with empty list adds nothing."""
        q = _q()
        QueryBuilder._equals_filter(q, [])
        assert "filter" not in q["query"]["bool"]

    # -- _not_equals_filter → bool.must_not ----------------------------------

    def test_not_equals_filter_goes_to_must_not(self):
        """_not_equals_filter puts term queries in bool.must_not."""
        q = _q()
        QueryBuilder._not_equals_filter(
            q,
            [
                EqualsFilter(field="deleted", value=True),
                EqualsFilter(field="archived", value=True),
            ],
        )

        assert "must_not" in q["query"]["bool"]
        assert len(q["query"]["bool"]["must_not"]) == 2
        assert {"term": {"deleted": True}} in q["query"]["bool"]["must_not"]

    def test_not_equals_filter_empty_list(self):
        """_not_equals_filter with empty list adds nothing."""
        q = _q()
        QueryBuilder._not_equals_filter(q, [])
        assert "must_not" not in q["query"]["bool"]

    # -- _range_filter → bool.filter -----------------------------------------

    def test_range_filter_goes_to_filter_clause(self):
        """_range_filter puts range queries in bool.filter (not must)."""
        q = _q()
        QueryBuilder._range_filter(
            q,
            [
                RangeFilter(field="age", gte=18, lte=65),
                RangeFilter(field="price", gt=10, lt=100),
            ],
        )

        assert "filter" in q["query"]["bool"]
        assert "must" not in q["query"]["bool"]
        assert len(q["query"]["bool"]["filter"]) == 2

        range1 = q["query"]["bool"]["filter"][0]
        assert range1["range"]["age"]["gte"] == 18
        assert range1["range"]["age"]["lte"] == 65

        range2 = q["query"]["bool"]["filter"][1]
        assert range2["range"]["price"]["gt"] == 10
        assert range2["range"]["price"]["lt"] == 100

    def test_range_filter_extends_existing_filter_clause(self):
        """_range_filter extends the filter list when equals already added some."""
        q = _q()
        QueryBuilder._equals_filter(q, [EqualsFilter(field="status", value="active")])
        QueryBuilder._range_filter(q, [RangeFilter(field="age", gte=18)])

        # Both equal and range should share the same filter list
        assert len(q["query"]["bool"]["filter"]) == 2
        assert {"term": {"status": "active"}} in q["query"]["bool"]["filter"]

    def test_range_filter_empty_list(self):
        """_range_filter with empty list adds nothing."""
        q = _q()
        QueryBuilder._range_filter(q, [])
        assert "filter" not in q["query"]["bool"]

    # -- _add_exists_filter → bool.filter ------------------------------------

    def test_exists_filter_goes_to_filter_clause(self):
        """_add_exists_filter puts exists queries in bool.filter (not must)."""
        q = _q()
        QueryBuilder._add_exists_filter(q, ["field1", "field2"])

        assert "filter" in q["query"]["bool"]
        assert "must" not in q["query"]["bool"]
        assert {"exists": {"field": "field1"}} in q["query"]["bool"]["filter"]
        assert {"exists": {"field": "field2"}} in q["query"]["bool"]["filter"]

    def test_exists_filter_empty_list(self):
        """_add_exists_filter with empty list adds nothing."""
        q = _q()
        QueryBuilder._add_exists_filter(q, [])
        assert "filter" not in q["query"]["bool"]

    # -- _add_not_exists_filter → bool.must_not ------------------------------

    def test_not_exists_filter_goes_to_must_not(self):
        """_add_not_exists_filter puts exists queries in bool.must_not."""
        q = _q()
        QueryBuilder._add_not_exists_filter(q, ["field3", "field4"])

        assert "must_not" in q["query"]["bool"]
        assert {"exists": {"field": "field3"}} in q["query"]["bool"]["must_not"]
        assert {"exists": {"field": "field4"}} in q["query"]["bool"]["must_not"]

    def test_not_exists_filter_empty_list(self):
        """_add_not_exists_filter with empty list adds nothing."""
        q = _q()
        QueryBuilder._add_not_exists_filter(q, [])
        assert "must_not" not in q["query"]["bool"]

    # -- _add_filter (combined) ----------------------------------------------

    def test_add_filter_routes_to_correct_clauses(self):
        """_add_filter puts equals+range+exists in filter, notEquals in must_not."""
        q = _q()
        QueryBuilder._add_filter(
            q,
            SearchFilter(
                equals=[EqualsFilter(field="status", value="active")],
                notEquals=[EqualsFilter(field="deleted", value=True)],
                rangeFilters=[RangeFilter(field="age", gte=18, lte=65)],
            ),
        )

        # equals + range → filter
        assert "filter" in q["query"]["bool"]
        assert len(q["query"]["bool"]["filter"]) == 2
        # notEquals → must_not
        assert "must_not" in q["query"]["bool"]
        assert len(q["query"]["bool"]["must_not"]) == 1
        # must should NOT be set by these filter types
        assert "must" not in q["query"]["bool"]

    def test_add_filter_empty(self):
        """_add_filter with empty SearchFilter leaves query unchanged."""
        q = _q()
        QueryBuilder._add_filter(q, SearchFilter())
        assert q == {"query": {"bool": {}}}

    # -- _add_sort -----------------------------------------------------------

    def test_add_sort_single(self):
        q = _q()
        QueryBuilder._add_sort(q, [sortModel(field="created_at", order="desc")])
        assert q["sort"] == [{"created_at": {"order": "desc"}}]

    def test_add_sort_multiple(self):
        q = _q()
        QueryBuilder._add_sort(
            q,
            [
                sortModel(field="priority", order="desc"),
                sortModel(field="created_at", order="asc"),
            ],
        )
        assert q["sort"][0] == {"priority": {"order": "desc"}}
        assert q["sort"][1] == {"created_at": {"order": "asc"}}

    def test_add_sort_none(self):
        q = _q()
        QueryBuilder._add_sort(q, None)
        assert "sort" not in q

    # -- _add_size -----------------------------------------------------------

    def test_add_size(self):
        q = _q()
        QueryBuilder._add_size(q, 25)
        assert q["size"] == 25

    def test_add_size_none(self):
        q = _q()
        QueryBuilder._add_size(q, None)
        assert "size" not in q

    # -- _add_include --------------------------------------------------------

    def test_add_include(self):
        q = _q()
        QueryBuilder._add_include(q, ["id", "name", "email"])
        assert q["_source"] == {"includes": ["id", "name", "email"]}

    def test_add_include_none(self):
        q = _q()
        QueryBuilder._add_include(q, None)
        assert "_source" not in q

    # -- _add_aggs -----------------------------------------------------------

    def test_add_aggs_single_level(self):
        """_add_aggs builds a single-level aggregation with top_hits."""
        q = _q()
        QueryBuilder._add_aggs(
            q, [AggregationRule(name="by_category", field="category.keyword", size=10)], ["id", "name"], 5
        )

        assert q["size"] == 0
        assert "sort" not in q
        assert "by_category" in q["aggs"]
        assert q["aggs"]["by_category"]["terms"]["field"] == "category.keyword"

        top_hits = q["aggs"]["by_category"]["aggs"]["top_hits_bucket"]["top_hits"]
        assert top_hits["size"] == 5
        assert top_hits["_source"]["includes"] == ["id", "name"]

    def test_add_aggs_multiple_levels(self):
        q = _q()
        QueryBuilder._add_aggs(
            q,
            [
                AggregationRule(name="by_category", field="category.keyword", size=10, order="desc"),
                AggregationRule(name="by_status", field="status.keyword", size=5, order="asc"),
            ],
            ["id", "name"],
            3,
        )

        assert q["aggs"]["by_category"]["terms"]["order"] == {"_key": "desc"}
        nested = q["aggs"]["by_category"]["aggs"]
        assert "by_status" in nested
        assert "top_hits_bucket" in nested["by_status"]["aggs"]

    def test_add_aggs_three_levels(self):
        q = _q()
        QueryBuilder._add_aggs(
            q,
            [
                AggregationRule(name="level1", field="field1.keyword", size=10),
                AggregationRule(name="level2", field="field2.keyword", size=5),
                AggregationRule(name="level3", field="field3.keyword", size=3),
            ],
            ["id"],
            2,
        )

        level2 = q["aggs"]["level1"]["aggs"]
        assert "level2" in level2
        assert "top_hits_bucket" in level2["level2"]["aggs"]["level3"]["aggs"]

    def test_add_aggs_empty_list(self):
        q = _q()
        q["size"] = 10
        q["sort"] = [{"field": {"order": "asc"}}]
        QueryBuilder._add_aggs(q, [], ["id"], 5)
        assert q["size"] == 10
        assert "sort" in q
        assert "aggs" not in q

    # -- build() end-to-end --------------------------------------------------

    def test_build_simple_search_query(self):
        """build() puts equals filter in bool.filter, not must."""
        query = QueryBuilder.build(
            QueryConfig(
                searchFilters=SearchFilter(equals=[EqualsFilter(field="status", value="active")]),
                sortList=[sortModel(field="created_at", order="desc")],
                size=10,
                returnFields=["id", "name"],
            )
        )

        assert query["size"] == 10
        assert "sort" in query
        assert "_source" in query
        assert "filter" in query["query"]["bool"]
        assert "must" not in query["query"]["bool"]
        assert "aggs" not in query

    def test_build_aggregation_query(self):
        query = QueryBuilder.build(
            QueryConfig(
                aggs=[AggregationRule(name="by_status", field="status.keyword", size=10)],
                size=5,
                returnFields=["id", "name"],
            )
        )

        assert query["size"] == 0
        assert "sort" not in query
        assert "aggs" in query
        assert "_source" not in query

    def test_build_with_all_filters(self):
        """build() routes each filter type to the right bool clause."""
        query = QueryBuilder.build(
            QueryConfig(
                searchFilters=SearchFilter(
                    equals=[EqualsFilter(field="status", value="active")],
                    notEquals=[EqualsFilter(field="deleted", value=True)],
                    rangeFilters=[RangeFilter(field="age", gte=18, lte=65)],
                ),
                existsFilters=["field1"],
                notExistsFilter=["field2"],
                sortList=[sortModel(field="created_at", order="desc")],
                size=20,
                returnFields=["id", "name", "email"],
            )
        )

        # equals + range + existsFilter → filter (3 clauses)
        assert len(query["query"]["bool"]["filter"]) == 3
        assert {"term": {"status": "active"}} in query["query"]["bool"]["filter"]
        assert {"exists": {"field": "field1"}} in query["query"]["bool"]["filter"]

        # notEquals + notExistsFilter → must_not (2 clauses)
        assert len(query["query"]["bool"]["must_not"]) == 2
        assert {"exists": {"field": "field2"}} in query["query"]["bool"]["must_not"]

        # no full-text → must should NOT be present
        assert "must" not in query["query"]["bool"]

    def test_build_from_dict(self):
        query = QueryBuilder.build(
            {
                "searchFilters": {"equals": [{"field": "status", "value": "active"}]},
                "sortList": [{"field": "created_at", "order": "desc"}],
                "size": 10,
                "returnFields": ["id", "name"],
            }
        )

        assert query["size"] == 10
        assert "filter" in query["query"]["bool"]

    def test_build_minimal_config(self):
        query = QueryBuilder.build(QueryConfig())
        assert "query" in query
        assert query["size"] == 1

    def test_build_complex_nested_aggs(self):
        query = QueryBuilder.build(
            QueryConfig(
                searchFilters=SearchFilter(equals=[EqualsFilter(field="type", value="user")]),
                aggs=[
                    AggregationRule(name="by_country", field="country.keyword", size=20),
                    AggregationRule(name="by_city", field="city.keyword", size=10),
                ],
                size=3,
                returnFields=["id", "name"],
            )
        )

        assert {"term": {"type": "user"}} in query["query"]["bool"]["filter"]
        assert query["size"] == 0
        assert "by_country" in query["aggs"]

    def test_build_preserves_range_filter_with_date(self):
        query = QueryBuilder.build(
            QueryConfig(
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
        )
        range_clause = query["query"]["bool"]["filter"][0]["range"]["created_at"]
        assert isinstance(range_clause["gte"], str)
        assert isinstance(range_clause["lte"], str)

    def test_build_callable_on_class_without_instantiation(self):
        """build() works directly on the class — no QueryBuilder() needed."""
        query = QueryBuilder.build({"size": 5})
        assert query["size"] == 5

    # -- _full_text_filter → bool.must ---------------------------------------

    def test_match_simple_goes_to_must(self):
        """_full_text_filter puts match queries in bool.must (not filter)."""
        q = _q()
        QueryBuilder._full_text_filter(q, [FullTextFilter(field="description", query="fast delivery")])

        assert "must" in q["query"]["bool"]
        assert "filter" not in q["query"]["bool"]
        assert q["query"]["bool"]["must"] == [{"match": {"description": {"query": "fast delivery"}}}]

    def test_match_with_and_operator(self):
        q = _q()
        QueryBuilder._full_text_filter(q, [FullTextFilter(field="title", query="red shoes", operator="and")])
        clause = q["query"]["bool"]["must"][0]["match"]["title"]
        assert clause["operator"] == "and"

    def test_match_with_minimum_should_match(self):
        q = _q()
        QueryBuilder._full_text_filter(q, [FullTextFilter(field="body", query="a b c d", minimum_should_match="75%")])
        clause = q["query"]["bool"]["must"][0]["match"]["body"]
        assert clause["minimum_should_match"] == "75%"

    def test_match_phrase_goes_to_must(self):
        q = _q()
        QueryBuilder._full_text_filter(
            q, [FullTextFilter(field="address", query="Baker Street", textFilterType="match_phrase")]
        )
        assert q["query"]["bool"]["must"] == [{"match_phrase": {"address": {"query": "Baker Street"}}}]

    def test_full_text_and_equals_use_separate_clauses(self):
        """Equals → filter, full-text → must — they coexist independently."""
        q = _q()
        QueryBuilder._equals_filter(q, [EqualsFilter(field="status", value="active")])
        QueryBuilder._full_text_filter(q, [FullTextFilter(field="description", query="fast")])

        assert {"term": {"status": "active"}} in q["query"]["bool"]["filter"]
        assert {"match": {"description": {"query": "fast"}}} in q["query"]["bool"]["must"]
        assert len(q["query"]["bool"]["filter"]) == 1
        assert len(q["query"]["bool"]["must"]) == 1

    def test_full_text_empty_list_does_not_add_must(self):
        q = _q()
        QueryBuilder._full_text_filter(q, [])
        assert "must" not in q["query"]["bool"]

    def test_build_with_full_text_from_config(self):
        """build() puts fullText in must, equals in filter — separate clauses."""
        query = QueryBuilder.build(
            {
                "searchFilters": {
                    "equals": [{"field": "status", "value": "active"}],
                    "fullText": [
                        {"field": "description", "query": "express shipping"},
                        {"field": "title", "query": "next day", "textFilterType": "match_phrase"},
                    ],
                },
                "size": 10,
            }
        )

        assert {"term": {"status": "active"}} in query["query"]["bool"]["filter"]
        assert len(query["query"]["bool"]["must"]) == 2
        assert {"match": {"description": {"query": "express shipping"}}} in query["query"]["bool"]["must"]
        assert {"match_phrase": {"title": {"query": "next day"}}} in query["query"]["bool"]["must"]

    def test_build_is_safely_reusable(self):
        """Each build() call is fully independent — no state bleeds."""
        q1 = QueryBuilder.build(
            QueryConfig(
                searchFilters=SearchFilter(equals=[EqualsFilter(field="status", value="active")]),
                size=5,
            )
        )
        q2 = QueryBuilder.build(
            QueryConfig(
                searchFilters=SearchFilter(equals=[EqualsFilter(field="type", value="premium")]),
                size=10,
            )
        )

        assert q1["query"]["bool"]["filter"] == [{"term": {"status": "active"}}]
        assert q1["size"] == 5
        assert q2["query"]["bool"]["filter"] == [{"term": {"type": "premium"}}]
        assert q2["size"] == 10
