from typing import Any, Dict, List, Optional

from .models import QueryConfig


class ESResponseParser:
    """Parse Elasticsearch responses according to a QueryConfig.

    - If `QueryConfig.aggs` is empty or None, parse hits and return a list of objects (dicts).
    - If `QueryConfig.aggs` is present, print a placeholder and return an empty list.
    """

    def __init__(self, query_config: QueryConfig):
        self.query_config = query_config

    def parse(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse an ES response and return a list of objects.

        Args:
            response: The raw JSON response from Elasticsearch (as a dict).

        Returns:
            List of dict objects representing documents or aggregation results.
        """
        aggs = getattr(self.query_config, "aggs", None)
        if aggs:
            return self.parse_aggregations(response)

        return self.parse_search_results(response)

    def parse_search_results(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse search (hits) portion of an ES response into list of objects.

        Always returns a list of dicts. If `returnFields` is set on the
        `QueryConfig`, only those fields are returned for each hit.
        """
        hits = response.get("hits", {}).get("hits", [])
        results: List[Dict[str, Any]] = []

        for hit in hits:
            source = hit.get("_source", {})
            doc = dict(source)
            doc["_id"] = hit.get("_id")
            results.append(doc)

        return results

    def parse_aggregations(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse aggregation results into a list of objects.

        This implements a lightweight, generic conversion of common ES
        aggregation shapes (e.g., `terms` with `buckets`). The function
        always returns a list of dicts so callers can rely on a uniform
        return type. For unknown shapes the raw aggregation value is
        returned under the aggregation name.
        """
        aggs = response.get("aggregations", {}) or {}
        results: List[Dict[str, Any]] = []

        for agg_name, agg_value in aggs.items():
            if isinstance(agg_value, dict) and "buckets" in agg_value:
                for bucket in agg_value.get("buckets", []):
                    entry: Dict[str, Any] = {"_agg": agg_name}
                    # key can be composite; include common fields
                    if "key" in bucket:
                        entry["key"] = bucket.get("key")
                    if "key_as_string" in bucket:
                        entry["key_as_string"] = bucket.get("key_as_string")
                    if "doc_count" in bucket:
                        entry["doc_count"] = bucket.get("doc_count")
                    # include any other top-level bucket fields
                    for k, v in bucket.items():
                        if k not in ("key", "key_as_string", "doc_count"):
                            entry[k] = v
                    results.append(entry)
            else:
                # Fallback: return the aggregation value as a single object
                results.append({"_agg": agg_name, "value": agg_value})

        return results
