from datetime import datetime
from typing import Any, List, Literal, Optional, Union

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field, field_validator, model_validator


class EqualsFilter(BaseModel):
    """
    Represents a filter that checks for equality.

    Attributes:
        field (str): The field to apply the filter on.
        value (Union[str, int, float, bool, list]): The value to compare against.

    Example:
        filter = EqualsFilter(field='status', value='active')
    """

    field: str
    value: Union[str, int, float, bool, list]


class RangeFilter(BaseModel):
    """
    Represents a range filter for querying.

    Attributes:
        field (str): The field to apply the filter on.
        rangeType (Literal['date', 'number']): The type of range, either 'date' or 'number' (default: 'number').
        dateFormat (Optional[str]): The date format string (e.g., '%Y-%m-%d'). Required when rangeType is 'date'.
        gt (Union[float, str, int, None, dict]): Greater than value.
        lt (Union[float, str, int, None, dict]): Less than value.
        gte (Union[float, str, int, None, dict]): Greater than or equal to value.
        lte (Union[float, str, int, None, dict]): Less than or equal to value.

    Example:
        # Numeric range
        filter = RangeFilter(field='age', gte=18, lte=30, rangeType='number')

        # Date range with relative offset (dict format)
        date_filter = RangeFilter(
            field='created_at',
            gte={'days': -30},
            rangeType='date',
            dateFormat='%Y-%m-%d'
        )
    """

    # make sure keys used in validator logic are defined before the fields being validated
    field: str
    rangeType: Literal["date", "number"] = "number"
    dateFormat: Optional[str] = "%Y-%m-%d"
    gt: Union[float, str, int, None, dict] = None
    lt: Union[float, str, int, None, dict] = None
    gte: Union[float, str, int, None, dict] = None
    lte: Union[float, str, int, None, dict] = None

    @field_validator("dateFormat")
    @classmethod
    def validate_date_format(cls, v: Optional[str], info) -> Optional[str]:
        """Ensure dateFormat is provided when rangeType is 'date'."""
        if info.data.get("rangeType") == "date" and not v:
            raise ValueError("dateFormat must be provided when rangeType is 'date'")
        return v

    @field_validator("gt", "gte", "lt", "lte", mode="after")
    @classmethod
    def validate_date_range(cls, v: Union[float, str, int, None, dict], info) -> Union[float, str, int, None, dict]:
        """Validate and format min/max values against dateFormat when rangeType is 'date'."""
        if info.data.get("rangeType") == "date" and v is not None:
            if not isinstance(v, dict):
                raise ValueError(
                    "For date rangeType, min and max should be provided as dicts representing relative date offsets."
                )
            date_format = info.data.get("dateFormat", "%Y-%m-%d")
            v = (datetime.now() + relativedelta(**v)).strftime(date_format)

        return v

    @model_validator(mode="after")
    def validate_at_least_one_operator(self) -> "RangeFilter":
        """Ensure at least one of gt, gte, lt, lte is present."""
        if not any([self.gt is not None, self.gte is not None, self.lt is not None, self.lte is not None]):
            raise ValueError("At least one of 'gt', 'gte', 'lt', or 'lte' must be provided")
        return self


class sortModel(BaseModel):
    """
    Represents the sorting configuration for queries.

    Attributes:
        field (str): The field to sort by.
        order (Literal): The order of sorting, either 'asc' or 'desc'.

    Example:
        sort = sortModel(field='created_at', order='asc')
    """

    field: str
    order: Literal["asc", "desc"]


class FullTextFilter(BaseModel):
    """
    Represents a full-text search filter using ``match`` or ``match_phrase``.

    Attributes:
        field (str): The field to run the full-text query against.
        query (str): The search text.
        type (Literal['match', 'match_phrase']): The ES query type (default: 'match').
            - ``match`` — standard full-text search with analysis.
            - ``match_phrase`` — requires all terms to appear in order.
        operator (Optional[Literal['and', 'or']]): For ``match`` only. Controls whether
            all terms (``and``) or any term (``or``) must match (default: 'or').
        minimum_should_match (Optional[Union[int, str]]): Minimum number / percentage of
            terms that must match. Only applies when ``operator`` is ``or``.
            Accepts an integer (e.g. 2) or a percentage string (e.g. "75%").

    Examples:
        # Simple match
        FullTextFilter(field='description', query='fast delivery')

        # All tokens must match
        FullTextFilter(field='title', query='red shoes', operator='and')

        # Exact phrase match
        FullTextFilter(field='address', query='Baker Street', textFilterType='match_phrase')
    """

    field: str
    query: str
    textFilterType: Literal["match", "match_phrase"] = "match"
    operator: Optional[Literal["and", "or"]] = None
    minimum_should_match: Optional[Union[int, str]] = None

    @model_validator(mode="after")
    def validate_match_phrase_options(self) -> "FullTextFilter":
        """Ensure match_phrase-incompatible options are not set."""
        if self.textFilterType == "match_phrase":
            if self.operator is not None:
                raise ValueError("'operator' is not supported for match_phrase queries.")
            if self.minimum_should_match is not None:
                raise ValueError("'minimum_should_match' is not supported for match_phrase queries.")
        return self


class SearchFilter(BaseModel):
    """
    Container for different types of search filters.

    Attributes:
        equals_filter (List[EqualsFilter]): List of equality filters (aliased as 'equals').
        not_equals_filter (List[EqualsFilter]): List of inequality filters (aliased as 'notEquals').
        range_filter (List[RangeFilter]): List of range filters (aliased as 'rangeFilters').
        full_text_filter (List[FullTextFilter]): List of full-text filters (aliased as 'fullText').

    Example:
        filters = SearchFilter(
            equals=[EqualsFilter(field='status', value='active')],
            notEquals=[EqualsFilter(field='deleted', value=True)],
            rangeFilters=[RangeFilter(field='age', gte=18, lte=65)],
            fullText=[FullTextFilter(field='description', query='fast delivery')],
        )
    """

    equals_filter: List[EqualsFilter] = Field(default_factory=list, alias="equals")
    not_equals_filter: List[EqualsFilter] = Field(default_factory=list, alias="notEquals")
    range_filter: List[RangeFilter] = Field(default_factory=list, alias="rangeFilters")
    full_text_filter: List[FullTextFilter] = Field(default_factory=list, alias="fullText")


class AggregationRule(BaseModel):
    """
    Defines a single aggregation rule for grouping and analyzing data.

    Attributes:
        name (str): The name of the aggregation.
        aggType (Literal['terms']): The type of aggregation (currently only 'terms' is supported).
        field (str): The field to aggregate on.
        size (int): Maximum number of aggregation buckets to return (1-500, default: 1).
        order (Optional[Literal['asc', 'desc']]): Sort order for aggregation buckets (default: None).

    Example:
        agg = AggregationRule(
            name='by_category',
            aggType='terms',
            field='category.keyword',
            size=10,
            order='desc'
        )
    """

    name: str
    aggType: Literal["terms"] = "terms"
    field: str
    size: int = Field(default=1, ge=1, le=500)
    order: Optional[Literal["asc", "desc"]] = None


class QueryConfig(BaseModel):
    """
    Configuration for a query including filters, sorting, and aggregations.

    Attributes:
        searchFilters (SearchFilter): Container for equals, not_equals, and range filters.
        existsFilters (Optional[List[str]]): List of fields that must exist in documents.
        sortList (Optional[List[sortModel]]): List of sorting configurations.
        size (Optional[int]): Number of results to return, must be between 1 and 500 (default: 1).
        returnFields (Optional[List[str]]): List of fields to return in the results.
        aggs (Optional[List[AggregationRule]]): List of aggregation rules to apply.

    Example:
        # Search query
        config = QueryConfig(
            searchFilters=SearchFilter(
                equals=[EqualsFilter(field='status', value='active')]
            ),
            existsFilters=['field1', 'field2'],
            sortList=[sortModel(field='created_at', order='desc')],
            size=20,
            returnFields=['field1', 'field2']
        )

        # Aggregation query
        agg_config = QueryConfig(
            aggs=[AggregationRule(name='by_status', field='status.keyword', size=10)],
            size=5,
            returnFields=['id', 'name']
        )
    """

    searchFilters: SearchFilter = Field(default_factory=SearchFilter)
    existsFilters: Optional[List[str]] = None
    notExistsFilter: Optional[List[str]] = None
    sortList: Optional[List[sortModel]] = None
    size: Optional[int] = Field(default=1, ge=1, le=500)
    returnFields: Optional[List[str]] = None
    aggs: Optional[List[AggregationRule]] = []
