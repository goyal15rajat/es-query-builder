# es-query-gen

Lightweight helpers to build Elasticsearch-style query dicts from typed Pydantic models.

Features
- Typed models for equality and range filters (`EqualsFilter`, `RangeFilter`).
- Small `QueryBuilder` to convert model instances into query dictionaries.

Requirements
- Python 3.12+ (uses `match` statement)
- Pydantic

Install

Install from source in editable mode:

```bash
pip install -e .
```

Quickstart

Example usage:

```python
from es_query_gen.models import EqualsFilter, RangeFilter, sortModel, QueryConfig
from es_query_gen.builder import QueryBuilder

# build a query config
cfg = QueryConfig(
	searchFilters=[
		EqualsFilter(operator="Equals", field="status", value="active"),
		RangeFilter(field="age", operator="range", min=18, max=30, rangeType="number"),
	],
	sort=sortModel(field="created_at", order="desc"),
	size=10,
	returnFields=["id", "name"],
)

qb = QueryBuilder()
# add filters from the config (builder currently exposes `_add_filter`)
qb._add_filter(cfg.searchFilters)
print(qb.build())
```

Notes
- `QueryBuilder._add_filter` expects a list of model instances and uses a `match` statement; ensure your environment runs Python 3.10 or newer.
- The library focuses on creating the query dictionary; you can extend `QueryBuilder` to add clauses like `must`, `should`, `filter`, or to produce JSON for Elasticsearch.

Contributing
- Pull requests welcome. Keep changes small and include tests for logic changes.

License
- MIT

