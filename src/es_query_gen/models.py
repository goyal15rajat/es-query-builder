from datetime import datetime
from typing import Any, List, Literal, Optional, Union

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field, field_validator


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
        dateFormat (Optional[str]): The date format string (e.g., 'YYYY-MM-DD'). Required when rangeType is 'date'.
        includeMin (bool): Whether to include the minimum value.
        includeMax (bool): Whether to include the maximum value.

    Example:
        filter = RangeFilter(field='age', operator='range', min=18, max=30, rangeType='number')
        date_filter = RangeFilter(field='created_at', operator='range', min='2024-01-01', max='2024-12-31', rangeType='date', dateFormat='YYYY-MM-DD')
    """

    field: str
    operator: Literal["range"]
    min: Union[float, str, int, None, dict] = None
    max: Union[float, str, int, None, dict] = None
    rangeType: Literal["date", "number"]
    dateFormat: Optional[str] = "YYYY-MM-DD"
    includeMin: bool = True
    includeMax: bool = True

    @field_validator("dateFormat")
    @classmethod
    def validate_date_format(cls, v: Optional[str], info) -> Optional[str]:
        """Ensure dateFormat is provided when rangeType is 'date'."""
        if info.data.get("rangeType") == "date" and not v:
            raise ValueError("dateFormat must be provided when rangeType is 'date'")
        return v

    @field_validator("min", "max", mode="after")
    @classmethod
    def validate_date_range(cls, v: Union[float, str, int, None, dict], info) -> Union[float, str, int, None, dict]:
        """Validate and format min/max values against dateFormat when rangeType is 'date'."""
        if info.data.get("rangeType") == "date" and v is not None:
            if not isinstance(v, dict):
                raise ValueError(
                    "For date rangeType, min and max should be provided as dicts representing relative date offsets."
                )
            date_format = info.data.get("dateFormat", "YYYY-MM-DD")
            now = datetime.now()
            now = relativedelta(**v) if isinstance(v, dict) else now

            return now.strftime(date_format)

        return v


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


class SearchFilter(BaseModel):

    equals_filter: List[EqualsFilter] = Field(default_factory=list)
    not_equals_filter: List[EqualsFilter] = Field(default_factory=list, alias="notEquals")
    range_filter: List[RangeFilter] = Field(default_factory=list)


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
            searchFilters={},
            existsFilters=['field1', 'field2'],
            sort=sortModel(field='created_at', order='desc'),
            size=20,
            returnFields=['field1', 'field2']
        )
    """

    searchFilters: SearchFilter = Field(default_factory=SearchFilter)
    existsFilters: Optional[List[str]] = None
    sort: Optional[sortModel] = None
    size: Optional[int] = Field(default=10, ge=1, le=500)
    returnFields: Optional[List[str]] = None
