
from src.es_query_gen.models import EqualsFilter, RangeFilter, sortModel, QueryConfig
class QueryBuilder:
    def __init__(self):
        self.query = {
            "query": {
                "bool": {}
            },
            "sort": []
        }

    def _equals_filter(self, equals_filter: EqualsFilter):
        match equals_filter.operator:
            case "Equals":
                if isinstance(equals_filter.value) == list:
                    print("is null")

                print("equals")
            case "NotEquals":
                print("not equals")
            case _:
                ValueError("Unsupported operator")
        return {}
    
    def _range_filter(self, range_filter: RangeFilter):
        range_condition = {}
        if range_filter.min is not None:
            key = "gte" if range_filter.includeMin else "gt"
            range_condition[key] = range_filter.min
        if range_filter.max is not None:
            key = "lte" if range_filter.includeMax else "lt"
            range_condition[key] = range_filter.max
        condition = {"range": {range_filter.field: range_condition}}
        return condition
    

    def _add_filter(self, filter_list):
        for filter_item in filter_list:
            match filter_item:
                case EqualsFilter():
                    condition = self._equals_filter(filter_item)
                case RangeFilter():
                    condition = self._range_filter(filter_item)
                case _:
                    raise ValueError("Unsupported filter type")

    def build(self):
        return self.query