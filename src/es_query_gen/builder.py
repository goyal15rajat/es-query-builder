from typing import List

from src.es_query_gen.models import EqualsFilter, QueryConfig, RangeFilter, SearchFilter


class QueryBuilder:
    """Build Elasticsearch queries from QueryConfig objects.

    This class provides methods to construct complex Elasticsearch queries
    including filters, sorting, field selection, and aggregations.
    """

    def __init__(self):
        """Initialize a new QueryBuilder with an empty bool query structure."""
        self.query = {"query": {"bool": {}}}

    def _equals_filter(self, equals_filters: List[EqualsFilter]):
        """Add equality filters to the query as 'must' clauses.

        Args:
            equals_filters: List of EqualsFilter objects to add as term queries.
        """
        must_list = []

        for filter_item in equals_filters:
            must_list.append({"term": {filter_item.field: filter_item.value}})

        if must_list:
            self.query["query"]["bool"]["must"] = must_list

    def _not_equals_filter(self, not_equals_filters: List[EqualsFilter]):
        """Add inequality filters to the query as 'must_not' clauses.

        Args:
            not_equals_filters: List of EqualsFilter objects to add as negated term queries.
        """
        must_not_list = []

        for filter_item in not_equals_filters:
            must_not_list.append({"term": {filter_item.field: filter_item.value}})

        if must_not_list:
            self.query["query"]["bool"]["must_not"] = must_not_list

    def _range_filter(self, range_filter: List[RangeFilter]):
        """Add range filters to the query.

        Builds range queries supporting gte, gt, lte, lt operators for both
        numeric and date ranges.

        Args:
            range_filter: List of RangeFilter objects to add as range queries.
        """
        range_list = []

        for range_filter_obj in range_filter:
            range_dict = {}
            if range_filter_obj.gte is not None:
                range_dict["gte"] = range_filter_obj.gte
            if range_filter_obj.gt is not None:
                range_dict["gt"] = range_filter_obj.gt
            if range_filter_obj.lte is not None:
                range_dict["lte"] = range_filter_obj.lte
            if range_filter_obj.lt is not None:
                range_dict["lt"] = range_filter_obj.lt

            range_list.append({"range": {range_filter_obj.field: range_dict}})

        if range_list:
            if self.query["query"]["bool"].get("must"):
                self.query["query"]["bool"]["must"].extend(range_list)
            else:
                self.query["query"]["bool"]["must"] = range_list

    def _add_filter(self, search_filter_object: SearchFilter):
        """Add all search filters from a SearchFilter object to the query.

        Args:
            search_filter_object: SearchFilter containing equals, not_equals, and range filters.
        """
        if search_filter_object.equals_filter:
            self._equals_filter(search_filter_object.equals_filter)

        if search_filter_object.not_equals_filter:
            self._not_equals_filter(search_filter_object.not_equals_filter)

        if search_filter_object.range_filter:
            self._range_filter(search_filter_object.range_filter)

    def _add_sort(self, sort_list):
        """Add sorting configuration to the query.

        Args:
            sort_list: List of sortModel objects defining field and order.
        """
        if sort_list:
            self.query["sort"] = [{sort_object.field: {"order": sort_object.order}} for sort_object in sort_list]

    def _add_size(self, size_value):
        """Set the number of results to return.

        Args:
            size_value: Maximum number of documents to return.
        """
        if size_value:
            self.query["size"] = size_value

    def _add_include(self, return_fields):
        """Configure which fields to include in the response.

        Args:
            return_fields: List of field names to include in _source.
        """
        if return_fields:
            self.query["_source"] = {"includes": return_fields}

    def _add_aggs(self, aggs_list, return_fields, size):
        """Add aggregations to the query with nested structure.

        Builds nested aggregations from the provided list, with a top_hits sub-aggregation
        at the deepest level to retrieve documents. Sets query size to 0 and removes
        sorting when aggregations are present.

        Args:
            aggs_list: List of AggregationRule objects defining the aggregation hierarchy.
            return_fields: Fields to include in the top_hits aggregation results.
            size: Number of documents to return per aggregation bucket.
        """
        if aggs_list:
            self.query["size"] = 0
            self.query.pop("sort", None)
        else:
            return
        es_aggs = {}
        agg_internal_pointer = es_aggs
        l = len(aggs_list)
        for i, agg_item in enumerate(aggs_list):
            agg_internal_pointer["aggs"] = {}
            if agg_item.aggType == "terms":
                aggs_dict = {"terms": {"field": agg_item.field, "size": agg_item.size}}
                if agg_item.order:
                    aggs_dict["terms"]["order"] = {"_key": agg_item.order}
            agg_internal_pointer["aggs"][agg_item.name] = aggs_dict
            agg_internal_pointer = agg_internal_pointer["aggs"][agg_item.name]
            if i == l - 1:
                agg_internal_pointer["aggs"] = {
                    "top_hits_bucket": {"top_hits": {"size": size, "_source": {"includes": return_fields}}}
                }

        self.query["aggs"] = es_aggs["aggs"]

    def build(self, es_query_config: QueryConfig) -> dict:
        """Build a complete Elasticsearch query from a QueryConfig object.

        Constructs the final query by applying filters, sorting, pagination,
        field selection, or aggregations based on the configuration.

        Args:
            es_query_config: QueryConfig object containing all query parameters.

        Returns:
            Dictionary representing a complete Elasticsearch query DSL.
        """
        es_query_config = QueryConfig.model_validate(es_query_config)

        self._add_filter(es_query_config.searchFilters)

        if not es_query_config.aggs:
            self._add_sort(es_query_config.sortList)
            self._add_size(es_query_config.size)
            self._add_include(es_query_config.returnFields)

        else:
            self._add_aggs(es_query_config.aggs, es_query_config.returnFields, es_query_config.size)

        return self.query
