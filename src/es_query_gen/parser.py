import logging
from typing import Any, Dict, List, Optional, Union

from .models import QueryConfig

logger = logging.getLogger(__name__)


class Node:
    """A simple linked list node for traversing aggregation configurations.

    Attributes:
        next: Reference to the next node in the linked list.
        data: The data stored in this node (aggregation configuration dict).
    """

    def __init__(self, next, data):
        """Initialize a new Node.

        Args:
            next: The next node in the linked list or None.
            data: The data to store in this node.
        """
        self.next = next
        self.data = data


class ESResponseParser:
    """Parse Elasticsearch responses according to a QueryConfig.

    Usage::

        parser = ESResponseParser(config)
        results = parser.parse_data(es_response)

    Two parsing strategies are chosen automatically based on ``QueryConfig.aggs``:

    - **No aggs** — extracts each document from ``hits.hits``, merging
      ``_source`` fields with ``_id``.
    - **With aggs** — walks the nested aggregation tree and extracts documents
      from the ``top_hits_bucket`` at the leaf level.

    A new ``ESResponseParser`` instance should be created per request, **or**
    ``parse_data`` can be called multiple times on the same instance — results
    are reset at the start of every call.
    """

    def __init__(self, query_config: Union[QueryConfig, Dict[str, Any]]):
        """Initialize the parser with a query configuration.

        Args:
            query_config: A ``QueryConfig`` instance **or** a plain dict that will
                be coerced into one via ``model_validate``.  Accepting a dict keeps
                the API consistent with ``QueryBuilder.build()`` and lets callers
                pass the same config object they use to build the query.
        """
        self.query_config: QueryConfig = QueryConfig.model_validate(query_config)
        self.results: List[Dict[str, Any]] = []

    def parse_data(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse an ES response and return a list of documents.

        Resets the internal results list on every call, so the same
        ``ESResponseParser`` instance can be safely reused across requests.

        Args:
            response: The raw JSON response from Elasticsearch (as a dict).

        Returns:
            List of dicts representing documents or aggregation results.
        """
        self.results = []  # reset so repeated calls don't accumulate stale data
        logger.debug("Parsing Elasticsearch response")
        if self.query_config.aggs:
            logger.debug("Parsing aggregation results")
            self.parse_aggregations(response)
        else:
            logger.debug("Parsing search hits")
            self.parse_search_results(response)

        logger.debug(f"Parsed {len(self.results)} results")
        return self.results

    def parse_search_results(self, response: Dict[str, Any]) -> None:
        """Extract hits from an ES response and append them to ``self.results``.

        Each document includes its ``_source`` fields merged with ``_id``.

        Args:
            response: The raw Elasticsearch response containing hits.
        """
        hits = response.get("hits", {}).get("hits", [])

        for hit in hits:
            source = hit.get("_source", {})
            doc = dict(source)
            doc["_id"] = hit.get("_id")
            self.results.append(doc)

    def _generate_aggs_linked_list(self, aggs_list: List[Dict[str, Any]]) -> Optional[Node]:
        """Generate a linked list from a list of aggregation configurations.

        Args:
            aggs_list: List of aggregation configuration dictionaries.

        Returns:
            The head node of the linked list or None if aggs_list is empty.
        """
        head = None
        current = None

        for item in aggs_list:
            node = Node(next=None, data=item)
            if not head:
                head = node
            else:
                current.next = node
            current = node
        current = head

        return head

    # Didnt used just recusrsive so that its easy to predict which key to use as bucket
    def _parse_aggs_recursively(self, node: Node, aggs_data: Dict[str, Any]):
        """Recursively parse aggregation buckets and extract documents.

        Traverses the aggregation tree using the linked list structure,
        extracting top_hits documents from leaf nodes and appending them
        to self.results.

        Args:
            node: Current node in the aggregation configuration linked list.
            aggs_data: The current level of aggregation data from the ES response.
        """
        if node is None:
            return

        # node.data is an AggregationRule (from QueryConfig) or a plain dict
        # (as used when tests call this method directly with raw  dict nodes).
        data = node.data
        aggs_bucket_name = data.name if hasattr(data, "name") else data["name"]

        if not aggs_data.get(aggs_bucket_name):
            return

        aggs_bucket_data = aggs_data[aggs_bucket_name]

        if not aggs_bucket_data.get("buckets", []):
            return
        bucket_item_list = aggs_bucket_data["buckets"]

        if node.next is None:
            # Leafs node - extract top_hits
            if len(bucket_item_list) == 0:
                return

            for data_obj in bucket_item_list:
                if "top_hits_bucket" not in data_obj:
                    logger.warning(
                        "Bucket in '%s' is missing 'top_hits_bucket' — skipping. "
                        "Ensure the aggregation includes a top_hits sub-aggregation.",
                        aggs_bucket_name,
                    )
                    continue
                hits_list = data_obj["top_hits_bucket"].get("hits", {}).get("hits", [])
                for each_obj in hits_list:
                    source = each_obj.get("_source", {})
                    doc = dict(source)
                    doc["_id"] = each_obj.get("_id")
                    self.results.append(doc)

        else:
            for bucket_item in bucket_item_list:
                self._parse_aggs_recursively(node.next, bucket_item)

    def parse_aggregations(self, response: Dict[str, Any]) -> None:
        """Walk the nested aggregation tree and append leaf documents to ``self.results``.

        Builds a linked list from ``self.query_config.aggs``, then recursively
        traverses bucket levels, extracting documents from the ``top_hits_bucket``
        at the deepest aggregation level.

        Args:
            response: The raw Elasticsearch response containing aggregations.
        """
        aggs_response = response.get("aggregations", {}) or {}

        aggs_config_ll = self._generate_aggs_linked_list(self.query_config.aggs)

        self._parse_aggs_recursively(aggs_config_ll, aggs_response)
