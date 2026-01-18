Installation
============

Supported Versions
------------------

- **Python:** 3.10, 3.11, 3.12 (package requires Python >= 3.10)
- **Elasticsearch:** 7.x and 8.x (the package depends on `elasticsearch[async] >=7.0.0,<9.0.0`)

Install
-------

Install the library from PyPI (when published) or locally using pip:

.. code-block:: bash

   # install from local checkout
   pip install -e .

   # or install from PyPI (replace with actual package name/version)
   pip install es-query-gen

Dependencies
------------

The project requires an async-capable Elasticsearch client and Pydantic v2. Example key dependencies are listed in `pyproject.toml`:

- `elasticsearch[async] >=7.0.0,<9.0.0`
- `pydantic >=2.12.5`

Quickstart — Basic Usage Examples
---------------------------------

Import the main helpers and classes exported by the package:

.. code-block:: python

   from es_query_gen import (
       QueryBuilder,
       ESResponseParser,
       ESClientSingleton,
       connect_es,
       connect_es_async,
       es_search,
       es_search_async,
       get_es_version,
       get_index_schema,
       validate_index,
   )

Synchronous example:

.. code-block:: python

   # create/connect client (this sets the module-level default client)
   client = connect_es(host="localhost", port=9200, username="elastic", password="changeme")

   # simple ping
   ok = client.ping()

   # get ES version
   print(get_es_version(es=client))

   # build a query
   qb = QueryBuilder()
   query = qb.build({"searchFilters": {}})

   # execute search
   resp = es_search(es=client, index="my_index", query=query)

   # parse results
   parser = ESResponseParser({})
   results = parser.parse_data(resp)

Asynchronous example (async/await):

.. code-block:: python

   import asyncio

   async def main():
       client = connect_es_async(host="localhost", port=9200, username="elastic", password="changeme")
       # client may be an AsyncElasticsearch instance; use the async helpers
       ok = await client.ping()
       version = await get_es_version(es=client)

       query = QueryBuilder().build({"searchFilters": {}})
       resp = await es_search_async(es=client, index="my_index", query=query)
       results = ESResponseParser({}).parse_data(resp)

   asyncio.run(main())

Notes
-----

- The package exposes a small set of top-level helpers via the package `__init__` for convenience. See the code in `src/es_query_gen/__init__.py` for the complete exported list.
- If you need to support Elasticsearch 9+, update the `elasticsearch` dependency in `pyproject.toml` and verify compatibility of the client API.
