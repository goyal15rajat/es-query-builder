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

    # TODO: RAJAT to check
    def _range_filter(self, range_filter: RangeFilter):
        range_list = []

        for range_filter_obj in range_filter:
            range_dict = {}
            if range_filter_obj.min is not None:
                if range_filter_obj.includeMin:
                    range_dict["gte"] = range_filter_obj.min
                else:
                    range_dict["gt"] = range_filter_obj.min
            if range_filter_obj.max is not None:
                if range_filter_obj.includeMax:
                    range_dict["lte"] = range_filter_obj.max
                else:
                    range_dict["lt"] = range_filter_obj.max

            range_list.append({"range": {range_filter_obj.field: range_dict}})

        if self.query["query"]["bool"].get("must"):

            self.query["query"]["bool"]["must"].extend(range_list)
        else:
            self.query["query"]["bool"]["must"] = range_list

    def _add_filter(self, search_filter_object: SearchFilter):

        if search_filter_object.equals_filter:
            self._equals_filter(search_filter_object.equals_filter)

        if search_filter_object.range_filter:
            for range_filter in search_filter_object.range_filter:
                self._range_filter(range_filter)

    def build(self):
        return self.query
