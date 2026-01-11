"""Test suite for connection.py - ES client management and operations."""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from elasticsearch import AsyncElasticsearch, Elasticsearch
from elasticsearch.exceptions import BadRequestError
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from elasticsearch.exceptions import ConnectionTimeout as ESTimeoutError
from elasticsearch.exceptions import NotFoundError, RequestError

from es_utils.connection import (
    ESClientSingleton,
    clear_default_es,
    clear_default_es_async,
    connect_es,
    connect_es_async,
    get_es_version,
    get_es_version_async,
    get_index_schema,
    get_index_schema_async,
    ping,
    ping_async,
    requires_es_client,
    requires_es_client_async,
    search,
    search_async,
    set_default_es,
    set_default_es_async,
)


class TestESClientSingleton:
    """Test cases for ESClientSingleton class."""

    def setup_method(self):
        """Clear singleton before each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    def teardown_method(self):
        """Clear singleton after each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    def test_singleton_initially_empty(self):
        """Test singleton is initially empty."""
        assert ESClientSingleton.get() is None
        assert ESClientSingleton.get_async() is None

    def test_set_and_get_sync_client(self):
        """Test setting and getting sync client."""
        mock_client = Mock(spec=Elasticsearch)
        ESClientSingleton.set(mock_client)
        assert ESClientSingleton.get() == mock_client

    def test_set_and_get_async_client(self):
        """Test setting and getting async client."""
        mock_client = Mock(spec=AsyncElasticsearch)
        ESClientSingleton.set_async(mock_client)
        assert ESClientSingleton.get_async() == mock_client

    def test_clear_sync_client(self):
        """Test clearing sync client."""
        mock_client = Mock(spec=Elasticsearch)
        ESClientSingleton.set(mock_client)
        ESClientSingleton.clear()
        assert ESClientSingleton.get() is None

    def test_clear_async_client(self):
        """Test clearing async client."""
        mock_client = Mock(spec=AsyncElasticsearch)
        ESClientSingleton.set_async(mock_client)
        ESClientSingleton.clear_async()
        assert ESClientSingleton.get_async() is None

    @patch("es_utils.connection.Elasticsearch")
    def test_connect_with_defaults(self, mock_es_class):
        """Test connect method with default parameters."""
        mock_client = Mock(spec=Elasticsearch)
        mock_es_class.return_value = mock_client

        client = ESClientSingleton.connect()

        assert client == mock_client
        assert ESClientSingleton.get() == mock_client
        mock_es_class.assert_called_once()

    @patch("es_utils.connection.Elasticsearch")
    def test_connect_with_custom_host_port(self, mock_es_class):
        """Test connect method with custom host and port."""
        mock_client = Mock(spec=Elasticsearch)
        mock_es_class.return_value = mock_client

        client = ESClientSingleton.connect(host="es.example.com", port=9300, scheme="https")

        assert client == mock_client
        call_args = mock_es_class.call_args
        assert "https://es.example.com:9300" in call_args[0][0]

    @patch("es_utils.connection.Elasticsearch")
    def test_connect_with_auth(self, mock_es_class):
        """Test connect method with authentication."""
        mock_client = Mock(spec=Elasticsearch)
        mock_es_class.return_value = mock_client

        client = ESClientSingleton.connect(username="user", password="pass")

        assert client == mock_client
        call_args = mock_es_class.call_args
        assert call_args[1]["http_auth"] == ("user", "pass")

    @patch("es_utils.connection.Elasticsearch")
    def test_connect_with_connection_string(self, mock_es_class):
        """Test connect method with connection string."""
        mock_client = Mock(spec=Elasticsearch)
        mock_es_class.return_value = mock_client

        connection_string = "https://user:pass@es.example.com:9200"
        client = ESClientSingleton.connect(connection_string=connection_string)

        assert client == mock_client
        mock_es_class.assert_called_once_with(connection_string, verify_certs=True)

    @patch("es_utils.connection.AsyncElasticsearch")
    def test_connect_async_with_defaults(self, mock_es_class):
        """Test connect_async method with default parameters."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_es_class.return_value = mock_client

        client = ESClientSingleton.connect_async()

        assert client == mock_client
        assert ESClientSingleton.get_async() == mock_client

    @patch("es_utils.connection.AsyncElasticsearch")
    def test_connect_async_with_connection_string(self, mock_es_class):
        """Test connect_async method with connection string."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_es_class.return_value = mock_client

        connection_string = "https://es.example.com:9200"
        client = ESClientSingleton.connect_async(connection_string=connection_string)

        assert client == mock_client
        mock_es_class.assert_called_once_with(connection_string, verify_certs=True)


class TestConnectionHelpers:
    """Test cases for connection helper functions."""

    def setup_method(self):
        """Clear singleton before each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    def teardown_method(self):
        """Clear singleton after each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    @patch("es_utils.connection.Elasticsearch")
    def test_connect_es(self, mock_es_class):
        """Test connect_es function."""
        mock_client = Mock(spec=Elasticsearch)
        mock_es_class.return_value = mock_client

        client = connect_es(host="localhost", username="elastic", password="changeme")

        assert client == mock_client
        assert ESClientSingleton.get() == mock_client

    def test_set_default_es(self):
        """Test set_default_es function."""
        mock_client = Mock(spec=Elasticsearch)
        set_default_es(mock_client)
        assert ESClientSingleton.get() == mock_client

    def test_clear_default_es(self):
        """Test clear_default_es function."""
        mock_client = Mock(spec=Elasticsearch)
        set_default_es(mock_client)
        clear_default_es()
        assert ESClientSingleton.get() is None

    def test_set_default_es_async(self):
        """Test set_default_es_async function."""
        mock_client = Mock(spec=AsyncElasticsearch)
        set_default_es_async(mock_client)
        assert ESClientSingleton.get_async() == mock_client

    def test_clear_default_es_async(self):
        """Test clear_default_es_async function."""
        mock_client = Mock(spec=AsyncElasticsearch)
        set_default_es_async(mock_client)
        clear_default_es_async()
        assert ESClientSingleton.get_async() is None


class TestDecorators:
    """Test cases for decorator functions."""

    def setup_method(self):
        """Clear singleton before each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    def teardown_method(self):
        """Clear singleton after each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    def test_requires_es_client_with_explicit_client(self):
        """Test requires_es_client decorator with explicit client."""

        @requires_es_client
        def test_func(es=None):
            return es

        mock_client = Mock(spec=Elasticsearch)
        result = test_func(es=mock_client)
        assert result == mock_client

    def test_requires_es_client_with_singleton(self):
        """Test requires_es_client decorator with singleton client."""

        @requires_es_client
        def test_func(es=None):
            return es

        mock_client = Mock(spec=Elasticsearch)
        ESClientSingleton.set(mock_client)
        result = test_func()
        assert result == mock_client

    def test_requires_es_client_no_client_raises_error(self):
        """Test requires_es_client decorator raises error when no client."""

        @requires_es_client
        def test_func(es=None):
            return es

        with pytest.raises(RuntimeError, match="Elasticsearch client not provided"):
            test_func()

    @pytest.mark.asyncio
    async def test_requires_es_client_async_with_explicit_client(self):
        """Test requires_es_client_async decorator with explicit client."""

        @requires_es_client_async
        async def test_func(es=None):
            return es

        mock_client = Mock(spec=AsyncElasticsearch)
        result = await test_func(es=mock_client)
        assert result == mock_client

    @pytest.mark.asyncio
    async def test_requires_es_client_async_with_singleton(self):
        """Test requires_es_client_async decorator with singleton client."""

        @requires_es_client_async
        async def test_func(es=None):
            return es

        mock_client = Mock(spec=AsyncElasticsearch)
        ESClientSingleton.set_async(mock_client)
        result = await test_func()
        assert result == mock_client

    @pytest.mark.asyncio
    async def test_requires_es_client_async_no_client_raises_error(self):
        """Test requires_es_client_async decorator raises error when no client."""

        @requires_es_client_async
        async def test_func(es=None):
            return es

        with pytest.raises(RuntimeError, match="Async Elasticsearch client not provided"):
            await test_func()


class TestESOperations:
    """Test cases for Elasticsearch operations."""

    def setup_method(self):
        """Clear singleton before each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    def teardown_method(self):
        """Clear singleton after each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    def test_ping_success(self):
        """Test ping function with successful connection."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.ping.return_value = True

        result = ping(es=mock_client)

        assert result is True
        mock_client.ping.assert_called_once()

    def test_ping_failure(self):
        """Test ping function with failed connection."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.ping.side_effect = Exception("Connection failed")

        result = ping(es=mock_client)

        assert result is False

    def test_get_index_schema(self):
        """Test get_index_schema function."""
        mock_client = Mock(spec=Elasticsearch)
        mock_indices = Mock()
        mock_client.indices = mock_indices

        # Mock both get_mapping and get_settings
        mock_indices.get_mapping.return_value = {
            "test_index": {"mappings": {"properties": {"field1": {"type": "text"}}}}
        }
        mock_indices.get_settings.return_value = {"test_index": {"settings": {"number_of_shards": "1"}}}

        result = get_index_schema(index="test_index", es=mock_client)

        assert "mappings" in result
        assert "settings" in result
        mock_indices.get_mapping.assert_called_once_with(index="test_index")
        mock_indices.get_settings.assert_called_once_with(index="test_index")

    def test_get_index_schema_not_found(self):
        """Test get_index_schema raises NotFoundError for missing index."""
        mock_client = Mock(spec=Elasticsearch)
        mock_indices = Mock()
        mock_client.indices = mock_indices
        mock_meta = Mock()
        mock_meta.status = 404
        mock_indices.get_mapping.side_effect = NotFoundError("Index not found", meta=mock_meta, body={})

        with pytest.raises(NotFoundError):
            get_index_schema(es=mock_client, index="missing_index")

    def test_get_es_version(self):
        """Test get_es_version function."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.info.return_value = {"version": {"number": "8.10.2"}}

        result = get_es_version(es=mock_client)

        assert result == "8.10.2"

    def test_get_es_version_no_version(self):
        """Test get_es_version when version info is missing."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.info.return_value = {}

        result = get_es_version(es=mock_client)

        assert result is None

    def test_search_success(self):
        """Test search function with successful query."""
        mock_client = Mock(spec=Elasticsearch)
        expected_response = {"hits": {"total": {"value": 1}, "hits": [{"_id": "1", "_source": {"name": "test"}}]}}
        mock_client.search.return_value = expected_response

        query = {"query": {"match_all": {}}}
        result = search(es=mock_client, index="test_index", query=query)

        assert result == expected_response
        mock_client.search.assert_called_once()

    def test_search_default_query(self):
        """Test search function with default match_all query."""
        mock_client = Mock(spec=Elasticsearch)
        expected_response = {"hits": {"total": {"value": 0}, "hits": []}}
        mock_client.search.return_value = expected_response

        result = search(es=mock_client, index="test_index")

        assert result == expected_response
        call_args = mock_client.search.call_args
        assert call_args[1]["body"] == {"match_all": {}}

    def test_search_not_found_error(self):
        """Test search raises NotFoundError for missing index."""
        mock_client = Mock(spec=Elasticsearch)
        mock_meta = Mock()
        mock_meta.status = 404
        mock_client.search.side_effect = NotFoundError("Index not found", meta=mock_meta, body={})

        with pytest.raises(NotFoundError):
            search(es=mock_client, index="missing_index")

    def test_search_bad_request_error(self):
        """Test search raises BadRequestError for malformed query."""
        mock_client = Mock(spec=Elasticsearch)
        mock_meta = Mock()
        mock_meta.status = 400
        mock_client.search.side_effect = BadRequestError("Malformed query", meta=mock_meta, body={})

        with pytest.raises(BadRequestError):
            search(es=mock_client, index="test_index", query={"invalid": "query"})

    def test_search_timeout_with_retry(self):
        """Test search retries on timeout."""
        mock_client = Mock(spec=Elasticsearch)
        # First call times out, second succeeds
        mock_client.search.side_effect = [
            ESTimeoutError("Timeout"),
            {"hits": {"total": {"value": 0}, "hits": []}},
        ]

        result = search(es=mock_client, index="test_index", max_retries=3, retry_delay=0.01)

        assert result == {"hits": {"total": {"value": 0}, "hits": []}}
        assert mock_client.search.call_count == 2

    def test_search_connection_error_with_retry(self):
        """Test search retries on connection error."""
        mock_client = Mock(spec=Elasticsearch)
        # First two calls fail, third succeeds
        mock_client.search.side_effect = [
            ESConnectionError("Connection failed"),
            ESConnectionError("Connection failed"),
            {"hits": {"total": {"value": 0}, "hits": []}},
        ]

        result = search(es=mock_client, index="test_index", max_retries=3, retry_delay=0.01)

        assert result == {"hits": {"total": {"value": 0}, "hits": []}}
        assert mock_client.search.call_count == 3

    def test_search_exhausted_retries(self):
        """Test search raises error after exhausting retries."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.search.side_effect = ESTimeoutError("Timeout")

        with pytest.raises(ESTimeoutError):
            search(es=mock_client, index="test_index", max_retries=2, retry_delay=0.01)

        assert mock_client.search.call_count == 2

    def test_search_request_error_no_retry(self):
        """Test search doesn't retry on RequestError."""
        mock_client = Mock(spec=Elasticsearch)
        mock_meta = Mock()
        mock_meta.status = 500
        mock_client.search.side_effect = RequestError("Request error", meta=mock_meta, body={})

        with pytest.raises(RequestError):
            search(es=mock_client, index="test_index", max_retries=3)

        assert mock_client.search.call_count == 1


class TestAsyncESOperations:
    """Test cases for async Elasticsearch operations."""

    def setup_method(self):
        """Clear singleton before each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    def teardown_method(self):
        """Clear singleton after each test."""
        ESClientSingleton.clear()
        ESClientSingleton.clear_async()

    @pytest.mark.asyncio
    async def test_ping_async_success(self):
        """Test ping_async function with successful connection."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_client.ping = AsyncMock(return_value=True)

        result = await ping_async(es=mock_client)

        assert result is True
        mock_client.ping.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ping_async_failure(self):
        """Test ping_async function with failed connection."""
        mock_client = AsyncMock(spec=AsyncElasticsearch)
        mock_client.ping.side_effect = Exception("Connection failed")

        result = await ping_async(es=mock_client)

        assert result is False

    @pytest.mark.asyncio
    async def test_get_index_schema_async(self):
        """Test get_index_schema_async function."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_indices = Mock()
        mock_client.indices = mock_indices
        expected_schema = {"test_index": {"mappings": {}}}
        mock_indices.get_mapping = AsyncMock(return_value=expected_schema)

        result = await get_index_schema_async(es=mock_client, index="test_index")

        assert result == expected_schema

    @pytest.mark.asyncio
    async def test_get_es_version_async(self):
        """Test get_es_version_async function."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_client.info = AsyncMock(return_value={"version": {"number": "8.10.2"}})

        result = await get_es_version_async(es=mock_client)

        assert result == "8.10.2"

    @pytest.mark.asyncio
    async def test_search_async_success(self):
        """Test search_async function with successful query."""
        mock_client = Mock(spec=AsyncElasticsearch)
        expected_response = {"hits": {"total": {"value": 1}, "hits": []}}
        mock_client.search = AsyncMock(return_value=expected_response)

        result = await search_async(es=mock_client, index="test_index")

        assert result == expected_response

    @pytest.mark.asyncio
    async def test_search_async_with_retry(self):
        """Test search_async retries on timeout."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_client.search = AsyncMock(
            side_effect=[
                ESTimeoutError("Timeout"),
                {"hits": {"total": {"value": 0}, "hits": []}},
            ]
        )

        result = await search_async(es=mock_client, index="test_index", max_retries=3, retry_delay=0.01)

        assert result == {"hits": {"total": {"value": 0}, "hits": []}}
        assert mock_client.search.call_count == 2

    @pytest.mark.asyncio
    async def test_search_async_exhausted_retries(self):
        """Test search_async raises error after exhausting retries."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_client.search = AsyncMock(side_effect=ESConnectionError("Connection failed"))

        with pytest.raises(ESConnectionError):
            await search_async(es=mock_client, index="test_index", max_retries=2, retry_delay=0.01)

        assert mock_client.search.call_count == 2
