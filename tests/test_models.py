"""Test suite for models.py - Pydantic models and validators."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from src.es_query_gen.models import (
    AggregationRule,
    EqualsFilter,
    QueryConfig,
    RangeFilter,
    SearchFilter,
    sortModel,
)


class TestEqualsFilter:
    """Test cases for EqualsFilter model."""

    def test_equals_filter_with_string_value(self):
        """Test EqualsFilter with string value."""
        filter_obj = EqualsFilter(field="status", value="active")
        assert filter_obj.field == "status"
        assert filter_obj.value == "active"

    def test_equals_filter_with_int_value(self):
        """Test EqualsFilter with integer value."""
        filter_obj = EqualsFilter(field="age", value=25)
        assert filter_obj.field == "age"
        assert filter_obj.value == 25

    def test_equals_filter_with_bool_value(self):
        """Test EqualsFilter with boolean value."""
        filter_obj = EqualsFilter(field="is_active", value=True)
        assert filter_obj.field == "is_active"
        assert filter_obj.value is True

    def test_equals_filter_with_list_value(self):
        """Test EqualsFilter with list value."""
        filter_obj = EqualsFilter(field="tags", value=["python", "elasticsearch"])
        assert filter_obj.field == "tags"
        assert filter_obj.value == ["python", "elasticsearch"]

    def test_equals_filter_with_float_value(self):
        """Test EqualsFilter with float value."""
        filter_obj = EqualsFilter(field="price", value=19.99)
        assert filter_obj.field == "price"
        assert filter_obj.value == 19.99


class TestRangeFilter:
    """Test cases for RangeFilter model."""

    def test_range_filter_numeric_gte_lte(self):
        """Test RangeFilter with numeric gte and lte."""
        filter_obj = RangeFilter(field="age", gte=18, lte=65, rangeType="number")
        assert filter_obj.field == "age"
        assert filter_obj.gte == 18
        assert filter_obj.lte == 65
        assert filter_obj.rangeType == "number"

    def test_range_filter_numeric_gt_lt(self):
        """Test RangeFilter with numeric gt and lt."""
        filter_obj = RangeFilter(field="price", gt=10.5, lt=99.99, rangeType="number")
        assert filter_obj.field == "price"
        assert filter_obj.gt == 10.5
        assert filter_obj.lt == 99.99

    def test_range_filter_date_with_relative_offset(self):
        """Test RangeFilter with date type and relative offset."""
        filter_obj = RangeFilter(
            field="created_at",
            gte={"days": -30},
            lte={"days": 0},
            rangeType="date",
            dateFormat="%Y-%m-%d",
        )
        assert filter_obj.field == "created_at"
        assert filter_obj.rangeType == "date"
        # The values should be converted to date strings
        assert isinstance(filter_obj.gte, str)
        assert isinstance(filter_obj.lte, str)

    def test_range_filter_date_with_complex_offset(self):
        """Test RangeFilter with complex date offset."""
        filter_obj = RangeFilter(
            field="dob",
            gte={"years": -60, "months": 2},
            lt={"years": -20, "months": 9, "days": 10},
            rangeType="date",
            dateFormat="%m/%d/%Y",
        )
        assert filter_obj.field == "dob"
        assert isinstance(filter_obj.gte, str)
        assert isinstance(filter_obj.lt, str)
        # Check format includes slashes
        assert "/" in filter_obj.gte
        assert "/" in filter_obj.lt

    def test_range_filter_missing_date_format(self):
        """Test RangeFilter raises error when dateFormat is missing for date rangeType."""
        with pytest.raises(ValidationError) as exc_info:
            RangeFilter(
                field="created_at",
                gte={"days": -30},
                rangeType="date",
                dateFormat=None,
            )
        assert "dateFormat must be provided" in str(exc_info.value)

    def test_range_filter_date_with_non_dict_value(self):
        """Test RangeFilter raises error when date rangeType gets non-dict value."""
        with pytest.raises(ValidationError) as exc_info:
            RangeFilter(
                field="created_at",
                gte="2024-01-01",
                rangeType="date",
                dateFormat="%Y-%m-%d",
            )
        assert "relative date offsets" in str(exc_info.value)

    def test_range_filter_no_operators(self):
        """Test RangeFilter raises error when no operators are provided."""
        with pytest.raises(ValidationError) as exc_info:
            RangeFilter(field="age", rangeType="number")
        assert "At least one of 'gt', 'gte', 'lt', or 'lte' must be provided" in str(exc_info.value)

    def test_range_filter_only_gte(self):
        """Test RangeFilter with only gte operator."""
        filter_obj = RangeFilter(field="age", gte=18, rangeType="number")
        assert filter_obj.gte == 18
        assert filter_obj.gt is None
        assert filter_obj.lte is None
        assert filter_obj.lt is None

    def test_range_filter_all_operators(self):
        """Test RangeFilter with all operators (edge case)."""
        filter_obj = RangeFilter(field="score", gte=0, gt=0, lte=100, lt=100, rangeType="number")
        assert filter_obj.gte == 0
        assert filter_obj.gt == 0
        assert filter_obj.lte == 100
        assert filter_obj.lt == 100


class TestSortModel:
    """Test cases for sortModel."""

    def test_sort_model_asc(self):
        """Test sortModel with ascending order."""
        sort_obj = sortModel(field="created_at", order="asc")
        assert sort_obj.field == "created_at"
        assert sort_obj.order == "asc"

    def test_sort_model_desc(self):
        """Test sortModel with descending order."""
        sort_obj = sortModel(field="price", order="desc")
        assert sort_obj.field == "price"
        assert sort_obj.order == "desc"

    def test_sort_model_invalid_order(self):
        """Test sortModel raises error with invalid order."""
        with pytest.raises(ValidationError):
            sortModel(field="name", order="invalid")


class TestSearchFilter:
    """Test cases for SearchFilter model."""

    def test_search_filter_empty(self):
        """Test SearchFilter with no filters."""
        search_filter = SearchFilter()
        assert search_filter.equals_filter == []
        assert search_filter.not_equals_filter == []
        assert search_filter.range_filter == []

    def test_search_filter_with_equals(self):
        """Test SearchFilter with equals filters."""
        search_filter = SearchFilter(equals=[EqualsFilter(field="status", value="active")])
        assert len(search_filter.equals_filter) == 1
        assert search_filter.equals_filter[0].field == "status"

    def test_search_filter_with_not_equals(self):
        """Test SearchFilter with notEquals filters."""
        search_filter = SearchFilter(notEquals=[EqualsFilter(field="deleted", value=True)])
        assert len(search_filter.not_equals_filter) == 1
        assert search_filter.not_equals_filter[0].field == "deleted"

    def test_search_filter_with_range(self):
        """Test SearchFilter with range filters."""
        search_filter = SearchFilter(rangeFilters=[RangeFilter(field="age", gte=18, lte=65)])
        assert len(search_filter.range_filter) == 1
        assert search_filter.range_filter[0].field == "age"

    def test_search_filter_all_types(self):
        """Test SearchFilter with all filter types."""
        search_filter = SearchFilter(
            equals=[EqualsFilter(field="status", value="active")],
            notEquals=[EqualsFilter(field="deleted", value=True)],
            rangeFilters=[RangeFilter(field="age", gte=18, lte=65)],
        )
        assert len(search_filter.equals_filter) == 1
        assert len(search_filter.not_equals_filter) == 1
        assert len(search_filter.range_filter) == 1

    def test_search_filter_multiple_equals(self):
        """Test SearchFilter with multiple equals filters."""
        search_filter = SearchFilter(
            equals=[
                EqualsFilter(field="status", value="active"),
                EqualsFilter(field="type", value="premium"),
            ]
        )
        assert len(search_filter.equals_filter) == 2


class TestAggregationRule:
    """Test cases for AggregationRule model."""

    def test_aggregation_rule_basic(self):
        """Test AggregationRule with basic configuration."""
        agg = AggregationRule(name="by_category", field="category.keyword", size=10)
        assert agg.name == "by_category"
        assert agg.field == "category.keyword"
        assert agg.size == 10
        assert agg.aggType == "terms"
        assert agg.order is None

    def test_aggregation_rule_with_order(self):
        """Test AggregationRule with order specified."""
        agg = AggregationRule(name="by_status", field="status.keyword", size=5, order="desc")
        assert agg.name == "by_status"
        assert agg.order == "desc"

    def test_aggregation_rule_default_size(self):
        """Test AggregationRule with default size."""
        agg = AggregationRule(name="by_tag", field="tag.keyword")
        assert agg.size == 1

    def test_aggregation_rule_size_validation_min(self):
        """Test AggregationRule size validation (minimum)."""
        with pytest.raises(ValidationError):
            AggregationRule(name="test", field="test.keyword", size=0)

    def test_aggregation_rule_size_validation_max(self):
        """Test AggregationRule size validation (maximum)."""
        with pytest.raises(ValidationError):
            AggregationRule(name="test", field="test.keyword", size=501)

    def test_aggregation_rule_size_boundary(self):
        """Test AggregationRule size at boundaries."""
        agg1 = AggregationRule(name="test1", field="test.keyword", size=1)
        agg2 = AggregationRule(name="test2", field="test.keyword", size=500)
        assert agg1.size == 1
        assert agg2.size == 500

    def test_aggregation_rule_explicit_aggtype(self):
        """Test AggregationRule with explicit aggType."""
        agg = AggregationRule(name="test", field="test.keyword", aggType="terms", size=10)
        assert agg.aggType == "terms"


class TestQueryConfig:
    """Test cases for QueryConfig model."""

    def test_query_config_minimal(self):
        """Test QueryConfig with minimal configuration."""
        config = QueryConfig()
        assert config.size == 1
        assert config.aggs == []
        assert config.sortList is None
        assert config.returnFields is None
        assert config.existsFilters is None

    def test_query_config_search_query(self):
        """Test QueryConfig for a search query."""
        config = QueryConfig(
            searchFilters=SearchFilter(equals=[EqualsFilter(field="status", value="active")]),
            sortList=[sortModel(field="created_at", order="desc")],
            size=20,
            returnFields=["id", "name", "status"],
        )
        assert config.size == 20
        assert len(config.returnFields) == 3
        assert len(config.sortList) == 1
        assert len(config.searchFilters.equals_filter) == 1

    def test_query_config_aggregation_query(self):
        """Test QueryConfig for an aggregation query."""
        config = QueryConfig(
            aggs=[AggregationRule(name="by_status", field="status.keyword", size=10)],
            size=5,
            returnFields=["id", "name"],
        )
        assert len(config.aggs) == 1
        assert config.size == 5
        assert len(config.returnFields) == 2

    def test_query_config_with_exists_filters(self):
        """Test QueryConfig with existsFilters."""
        config = QueryConfig(existsFilters=["field1", "field2"], size=10)
        assert config.existsFilters == ["field1", "field2"]

    def test_query_config_size_validation_min(self):
        """Test QueryConfig size validation (minimum)."""
        with pytest.raises(ValidationError):
            QueryConfig(size=0)

    def test_query_config_size_validation_max(self):
        """Test QueryConfig size validation (maximum)."""
        with pytest.raises(ValidationError):
            QueryConfig(size=501)

    def test_query_config_size_boundary(self):
        """Test QueryConfig size at boundaries."""
        config1 = QueryConfig(size=1)
        config2 = QueryConfig(size=500)
        assert config1.size == 1
        assert config2.size == 500

    def test_query_config_from_dict(self):
        """Test QueryConfig creation from dictionary."""
        config_dict = {
            "size": 10,
            "searchFilters": {
                "equals": [{"field": "status", "value": "active"}],
                "rangeFilters": [{"field": "age", "gte": 18, "lte": 65}],
            },
            "sortList": [{"field": "created_at", "order": "desc"}],
            "returnFields": ["id", "name"],
        }
        config = QueryConfig.model_validate(config_dict)
        assert config.size == 10
        assert len(config.searchFilters.equals_filter) == 1
        assert len(config.searchFilters.range_filter) == 1
        assert len(config.sortList) == 1
        assert len(config.returnFields) == 2

    def test_query_config_complex_with_multiple_aggs(self):
        """Test QueryConfig with multiple aggregations."""
        config = QueryConfig(
            aggs=[
                AggregationRule(name="by_category", field="category.keyword", size=10),
                AggregationRule(name="by_status", field="status.keyword", size=5),
                AggregationRule(name="by_date", field="date", size=20, order="asc"),
            ],
            size=10,
            returnFields=["id", "name", "category"],
        )
        assert len(config.aggs) == 3
        assert config.aggs[0].name == "by_category"
        assert config.aggs[1].name == "by_status"
        assert config.aggs[2].name == "by_date"
        assert config.aggs[2].order == "asc"
