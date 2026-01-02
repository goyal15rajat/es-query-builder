from typing import List, Optional, Any, Union, Literal
from pydantic import BaseModel, Field

class EqualsFilter(BaseModel):
    """
    Represents a filter that checks for equality or inequality.

    Attributes:
        operator (Literal): The operator to use, either 'Equals' or 'NotEquals'.
        field (str): The field to apply the filter on.
        value (Union[str, int, float, bool, list]): The value to compare against.

    Example:
        filter = EqualsFilter(operator='Equals', field='status', value='active')
    """
    operator: Literal["Equals", "NotEquals"]
    field: str
    value: Union[str, int, float, bool, list]

class RangeFilter(BaseModel):
    """
    Represents a range filter for querying.

    Attributes:
        field (str): The field to apply the filter on.
        operator (Literal): The operator, must be 'range'.
        min (Union[float, str, int, None]): The minimum value of the range.
        max (Union[float, str, int, None]): The maximum value of the range.
        rangeType (Literal): The type of range, either 'date' or 'number'.
        includeMin (bool): Whether to include the minimum value.
        includeMax (bool): Whether to include the maximum value.

    Example:
        filter = RangeFilter(field='age', operator='range', min=18, max=30, rangeType='number')
    """
    field: str
    operator: Literal["range"]
    min: Union[float, str, int, None] = None
    max: Union[float, str, int, None] = None
    rangeType: Literal["date", "number"]
    includeMin: bool = True
    includeMax: bool = True

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

class QueryConfig(BaseModel):
    """
    Configuration for a query including filters and sorting.

    Attributes:
        searchFilters (List[Union[EqualsFilter, RangeFilter]]): List of filters to apply to the search.
        existsFilters (Optional[List[str]]): List of fields that must exist.
        sort (Optional[sortModel]): Sorting configuration.
        size (Optional[int]): Number of results to return, must be between 1 and 500.
        returnFields (Optional[List[str]]): List of fields to return in the results.

    Example:
        config = QueryConfig(
            searchFilters=[filter],
            existsFilters=['field1', 'field2'],
            sort=sortModel(field='created_at', order='desc'),
            size=20,
            returnFields=['field1', 'field2']
        )
    """
    searchFilters: List[Union[EqualsFilter, RangeFilter]]
    existsFilters: Optional[List[str]] = None
    sort: Optional[sortModel] = None
    size: Optional[int] = Field(default=10, ge=1, le=500)
    returnFields: Optional[List[str]] = None


