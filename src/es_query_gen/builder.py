import logging
from typing import Any, Dict, List, Union

from .models import EqualsFilter, FullTextFilter, QueryConfig, RangeFilter, SearchFilter

logger = logging.getLogger(__name__)


class QueryBuilder:
    """Build Elasticsearch queries from QueryConfig objects.

    All methods are static — the class holds no instance state and does not
    need to be instantiated.  Both calling styles work:

        # Preferred (no instantiation needed)
        query = QueryBuilder.build(config)

        # Also fine (throwaway instance, same result)
        query = QueryBuilder().build(config)
    """

    @staticmethod
    def _create_term_query(field: str, value: Any) -> Dict[str, Any]:
        """Build a term-style query for a field/value pair.

        Returns a ``terms`` query when ``value`` is a list and a ``term``
        query for all other value types.

        Args:
            field: Field name to query.
            value: Field value to match (scalar or list of values).

        Returns:
            Elasticsearch query clause as a dictionary.
        """
        if isinstance(value, list):
            return {"terms": {field: value}}
        return {"term": {field: value}}

    @staticmethod
    def _equals_filter(query: Dict[str, Any], equals_filters: List[EqualsFilter]) -> None:
        """Add equality filters to the query as ``bool.filter`` clauses.

        Using ``filter`` context skips relevance scoring and enables ES query
        caching, making exact-match filters faster than ``must``.

        Args:
            query: The in-progress ES query dict to mutate.
            equals_filters: List of EqualsFilter objects to add as term queries.
        """
        items = [QueryBuilder._create_term_query(f.field, f.value) for f in equals_filters]
        if items:
            query["query"]["bool"].setdefault("filter", []).extend(items)

    @staticmethod
    def _not_equals_filter(query: Dict[str, Any], not_equals_filters: List[EqualsFilter]) -> None:
        """Add inequality filters to the query as ``bool.must_not`` clauses.

        Args:
            query: The in-progress ES query dict to mutate.
            not_equals_filters: List of EqualsFilter objects to add as negated term queries.
        """
        items = [QueryBuilder._create_term_query(f.field, f.value) for f in not_equals_filters]
        if items:
            query["query"]["bool"].setdefault("must_not", []).extend(items)

    @staticmethod
    def _range_filter(query: Dict[str, Any], range_filters: List[RangeFilter]) -> None:
        """Add range filters to the query as ``bool.filter`` clauses.

        Using ``filter`` context skips relevance scoring and enables ES query
        caching for range queries.

        Args:
            query: The in-progress ES query dict to mutate.
            range_filters: List of RangeFilter objects to add as range queries.
        """
        items = []
        for rf in range_filters:
            range_dict: Dict[str, Any] = {}
            if rf.gte is not None:
                range_dict["gte"] = rf.gte
            if rf.gt is not None:
                range_dict["gt"] = rf.gt
            if rf.lte is not None:
                range_dict["lte"] = rf.lte
            if rf.lt is not None:
                range_dict["lt"] = rf.lt
            items.append({"range": {rf.field: range_dict}})

        if items:
            query["query"]["bool"].setdefault("filter", []).extend(items)

    @staticmethod
    def _full_text_filter(query: Dict[str, Any], full_text_filters: List[FullTextFilter]) -> None:
        """Add full-text search filters to the query as ``bool.must`` clauses.

        Full-text filters live in ``must`` (not ``filter``) because they
        contribute to the relevance ``_score`` — documents matching more of the
        query text are ranked higher.

        Args:
            query: The in-progress ES query dict to mutate.
            full_text_filters: List of FullTextFilter objects.
        """
        items = []
        for ft in full_text_filters:
            if ft.textFilterType == "match":
                clause: Dict[str, Any] = {"query": ft.query}
                if ft.operator is not None:
                    clause["operator"] = ft.operator
                if ft.minimum_should_match is not None:
                    clause["minimum_should_match"] = ft.minimum_should_match
                items.append({"match": {ft.field: clause}})
            else:  # match_phrase
                items.append({"match_phrase": {ft.field: {"query": ft.query}}})

        if items:
            query["query"]["bool"].setdefault("must", []).extend(items)

    @staticmethod
    def _add_filter(query: Dict[str, Any], search_filter_object: SearchFilter) -> None:
        """Add all search filters from a SearchFilter object to the query.

        Args:
            query: The in-progress ES query dict to mutate.
            search_filter_object: SearchFilter containing equals, not_equals, range,
                and full-text filters.
        """
        if search_filter_object.equals_filter:
            QueryBuilder._equals_filter(query, search_filter_object.equals_filter)

        if search_filter_object.not_equals_filter:
            QueryBuilder._not_equals_filter(query, search_filter_object.not_equals_filter)

        if search_filter_object.range_filter:
            QueryBuilder._range_filter(query, search_filter_object.range_filter)

        if search_filter_object.full_text_filter:
            QueryBuilder._full_text_filter(query, search_filter_object.full_text_filter)

    @staticmethod
    def _add_exists_filter(query: Dict[str, Any], exists_filters: List[str]) -> None:
        """Add exists filters to the query as ``bool.filter`` clauses.

        Args:
            query: The in-progress ES query dict to mutate.
            exists_filters: List of fields that must exist.
        """
        if exists_filters:
            query["query"]["bool"].setdefault("filter", []).extend([{"exists": {"field": f}} for f in exists_filters])

    @staticmethod
    def _add_not_exists_filter(query: Dict[str, Any], not_exists_filter: List[str]) -> None:
        """Add not-exists filters to the query as ``bool.must_not`` clauses.

        Args:
            query: The in-progress ES query dict to mutate.
            not_exists_filter: List of fields that must not exist.
        """
        if not_exists_filter:
            query["query"]["bool"].setdefault("must_not", []).extend(
                [{"exists": {"field": f}} for f in not_exists_filter]
            )

    @staticmethod
    def _add_sort(query: Dict[str, Any], sort_list) -> None:
        """Add sorting configuration to the query.

        Args:
            query: The in-progress ES query dict to mutate.
            sort_list: List of sortModel objects defining field and order.
        """
        if sort_list:
            query["sort"] = [{s.field: {"order": s.order}} for s in sort_list]

    @staticmethod
    def _add_size(query: Dict[str, Any], size_value) -> None:
        """Set the number of results to return.

        Args:
            query: The in-progress ES query dict to mutate.
            size_value: Maximum number of documents to return.
        """
        if size_value:
            query["size"] = size_value

    @staticmethod
    def _add_include(query: Dict[str, Any], return_fields) -> None:
        """Configure which fields to include in the response.

        Args:
            query: The in-progress ES query dict to mutate.
            return_fields: List of field names to include in _source.
        """
        if return_fields:
            query["_source"] = {"includes": return_fields}

    @staticmethod
    def _add_aggs(query: Dict[str, Any], aggs_list, return_fields, size) -> None:
        """Add aggregations to the query with nested structure.

        Builds nested aggregations from the provided list, with a top_hits sub-aggregation
        at the deepest level to retrieve documents. Sets query size to 0 and removes
        sorting when aggregations are present.

        Args:
            query: The in-progress ES query dict to mutate.
            aggs_list: List of AggregationRule objects defining the aggregation hierarchy.
            return_fields: Fields to include in the top_hits aggregation results.
            size: Number of documents to return per aggregation bucket.
        """
        if not aggs_list:
            return

        query["size"] = 0
        query.pop("sort", None)

        es_aggs: Dict[str, Any] = {}
        pointer = es_aggs
        num_aggs = len(aggs_list)
        for i, agg_item in enumerate(aggs_list):
            pointer["aggs"] = {}
            if agg_item.aggType == "terms":
                aggs_dict: Dict[str, Any] = {"terms": {"field": agg_item.field, "size": agg_item.size}}
                if agg_item.order:
                    aggs_dict["terms"]["order"] = {"_key": agg_item.order}
            pointer["aggs"][agg_item.name] = aggs_dict
            pointer = pointer["aggs"][agg_item.name]
            if i == num_aggs - 1:
                pointer["aggs"] = {
                    "top_hits_bucket": {"top_hits": {"size": size, "_source": {"includes": return_fields}}}
                }

        query["aggs"] = es_aggs["aggs"]

    @staticmethod
    def build(es_query_config: Union[QueryConfig, Dict[str, Any]]) -> Dict[str, Any]:
        """Build a complete Elasticsearch query from a QueryConfig object.

        Creates a fresh local query dict on every call — no shared state, fully
        thread-safe, and safely callable as ``QueryBuilder.build(config)`` without
        instantiation.

        Args:
            es_query_config: ``QueryConfig`` instance or a plain dict that will be
                coerced via ``model_validate``.

        Returns:
            Dictionary representing a complete Elasticsearch query DSL.
        """
        query: Dict[str, Any] = {"query": {"bool": {}}}

        logger.debug("Building Elasticsearch query from QueryConfig")
        config = QueryConfig.model_validate(es_query_config)

        QueryBuilder._add_filter(query, config.searchFilters)
        QueryBuilder._add_exists_filter(query, config.existsFilters)
        QueryBuilder._add_not_exists_filter(query, config.notExistsFilter)

        if not config.aggs:
            QueryBuilder._add_sort(query, config.sortList)
            QueryBuilder._add_size(query, config.size)
            QueryBuilder._add_include(query, config.returnFields)
        else:
            logger.debug(f"Adding aggregations: {len(config.aggs)} levels")
            QueryBuilder._add_aggs(query, config.aggs, config.returnFields, config.size)

        logger.debug(f"Built query with size={query.get('size', 'default')}")
        return query
