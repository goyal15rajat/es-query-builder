from typing import List

from src.es_query_gen.models import EqualsFilter, QueryConfig, RangeFilter, SearchFilter


class QueryBuilder:
    def __init__(self):
        self.query = {"query": {"bool": {}}}

    def _equals_filter(self, equals_filters: List[EqualsFilter]):

        must_list = []

        for filter_item in equals_filters:
            must_list.append({"term": {filter_item.field: filter_item.value}})

        if must_list:
            self.query["query"]["bool"]["must"] = must_list

    def _not_equals_filter(self, not_equals_filters: List[EqualsFilter]):

        must_not_list = []

        for filter_item in not_equals_filters:
            must_not_list.append({"term": {filter_item.field: filter_item.value}})

        if must_not_list:
            self.query["query"]["bool"]["must_not"] = must_not_list

    def _range_filter(self, range_filter: RangeFilter):
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

        if search_filter_object.equals_filter:
            self._equals_filter(search_filter_object.equals_filter)

        if search_filter_object.not_equals_filter:
            self._not_equals_filter(search_filter_object.not_equals_filter)

        if search_filter_object.range_filter:
            self._range_filter(search_filter_object.range_filter)

    def _add_sort(self, sort_list):
        if sort_list:
            self.query["sort"] = [{sort_object.field: {"order": sort_object.order}} for sort_object in sort_list]

    def _add_size(self, size_value):
        if size_value:
            self.query["size"] = size_value

    def _add_include(self, return_fields):
        if return_fields:
            self.query["_source"] = {"includes": return_fields}

    def _add_aggs(self, aggs_list, return_fields, size):
        if aggs_list:
            self.query.pop("size", None)
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
                    aggs_dict["terms"]["order"] = {"_term": agg_item.order}
            agg_internal_pointer["aggs"][agg_item.name] = aggs_dict
            agg_internal_pointer = agg_internal_pointer[agg_item.name]
            if i == l - 1:
                agg_internal_pointer["aggs"] = {
                    "latest_documents": {"top_hits": {"size": size, "_source": {"includes": return_fields}}}
                }

        return es_aggs

    def build(self, es_query_config: QueryConfig) -> dict:

        es_query_config = QueryConfig.model_validate(es_query_config)

        self._add_filter(es_query_config.searchFilters)

        if not es_query_config.aggs:
            self._add_sort(es_query_config.sortList)
            self._add_size(es_query_config.size)
            self._add_include(es_query_config.returnFields)

        else:
            self._add_aggs(es_query_config.aggs, es_query_config.returnFields, es_query_config.size)

        return self.query
