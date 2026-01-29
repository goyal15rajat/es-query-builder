"""Test suite for connection.py - ES client management and operations."""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from elasticsearch import AsyncElasticsearch, Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from elasticsearch.exceptions import ConnectionTimeout as ESTimeoutError
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import RequestError
from elasticsearch.exceptions import RequestError as BadRequestError

# For testing across ES 7/8/9 where exception constructors differ,
# replace the exception classes used inside the connection module with
# simple Exception subclasses that are easy to instantiate.
from src.es_query_gen.es_utils import connection as conn_mod
from src.es_query_gen.es_utils.connection import (
    ESClientSingleton,
    clear_es_client,
    clear_es_client_async,
    close_all_es_clients,
    close_all_es_clients_async,
    close_es_client,
    close_es_client_async,
    connect_es,
    connect_es_async,
    es_search,
    es_search_async,
    get_es_client,
    get_es_client_async,
    get_es_version,
    get_es_version_async,
    get_index_schema,
    get_index_schema_async,
    ping,
    ping_async,
    requires_es_client,
    requires_es_client_async,
    set_es_client,
    set_es_client_async,
)


class SimpleESConnectionError(Exception):
    pass


class SimpleESTimeoutError(Exception):
    pass


class SimpleNotFoundError(Exception):
    pass


class SimpleRequestError(Exception):
    pass


conn_mod.ESConnectionError = SimpleESConnectionError
conn_mod.ESTimeoutError = SimpleESTimeoutError
conn_mod.NotFoundError = SimpleNotFoundError
conn_mod.RequestError = SimpleRequestError
conn_mod.BadRequestError = SimpleRequestError


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

    @patch("src.es_query_gen.es_utils.connection.Elasticsearch")
    def test_connect_with_defaults(self, mock_es_class):
        """Test connect method with default parameters."""
        mock_client = Mock(spec=Elasticsearch)
        mock_es_class.return_value = mock_client

        client = ESClientSingleton.connect()

        assert client == mock_client
        assert ESClientSingleton.get() == mock_client
        mock_es_class.assert_called_once()

    @patch("src.es_query_gen.es_utils.connection.Elasticsearch")
    def test_connect_with_custom_host_port(self, mock_es_class):
        """Test connect method with custom host and port."""
        mock_client = Mock(spec=Elasticsearch)
        mock_es_class.return_value = mock_client

        client = ESClientSingleton.connect(host="es.example.com", port=9300, scheme="https")

        assert client == mock_client
        call_args = mock_es_class.call_args
        assert "https://es.example.com:9300" in call_args[0][0]

    @patch("src.es_query_gen.es_utils.connection.Elasticsearch")
    def test_connect_with_auth(self, mock_es_class):
        """Test connect method with authentication."""
        mock_client = Mock(spec=Elasticsearch)
        mock_es_class.return_value = mock_client

        client = ESClientSingleton.connect(username="user", password="pass")

        assert client == mock_client
        call_args = mock_es_class.call_args
        assert call_args[1]["http_auth"] == ("user", "pass")

    @patch("src.es_query_gen.es_utils.connection.Elasticsearch")
    def test_connect_with_connection_string(self, mock_es_class):
        """Test connect method with connection string."""
        mock_client = Mock(spec=Elasticsearch)
        mock_es_class.return_value = mock_client

        connection_string = "https://user:pass@es.example.com:9200"
        client = ESClientSingleton.connect(connection_string=connection_string)

        assert client == mock_client
        mock_es_class.assert_called_once_with(connection_string, verify_certs=True)

    @patch("src.es_query_gen.es_utils.connection.AsyncElasticsearch")
    def test_connect_async_with_defaults(self, mock_es_class):
        """Test connect_async method with default parameters."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_es_class.return_value = mock_client

        client = ESClientSingleton.connect_async()

        assert client == mock_client
        assert ESClientSingleton.get_async() == mock_client

    @patch("src.es_query_gen.es_utils.connection.AsyncElasticsearch")
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

    @patch("src.es_query_gen.es_utils.connection.Elasticsearch")
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
        set_es_client(mock_client)
        assert ESClientSingleton.get() == mock_client

    def test_clear_default_es(self):
        """Test clear_default_es function."""
        mock_client = Mock(spec=Elasticsearch)
        set_es_client(mock_client)
        clear_es_client()
        assert ESClientSingleton.get() is None

    def test_set_default_es_async(self):
        """Test set_default_es_async function."""
        mock_client = Mock(spec=AsyncElasticsearch)
        ESClientSingleton.set_async(mock_client)
        assert ESClientSingleton.get_async() == mock_client

    def test_clear_default_es_async(self):
        """Test clear_default_es_async function."""
        mock_client = Mock(spec=AsyncElasticsearch)
        ESClientSingleton.set_async(mock_client)
        ESClientSingleton.clear_async()
        assert ESClientSingleton.get_async() is None

    def test_set_es_client_async_helper(self):
        """Test `set_es_client_async` helper registers async client."""
        mock_client = Mock(spec=AsyncElasticsearch)

        set_es_client_async(mock_client)

        assert ESClientSingleton.get_async() == mock_client

    def test_get_es_client_async_helper_and_key(self):
        """Test `get_es_client_async` returns correct client for default and keyed clients."""
        mock_default = Mock(spec=AsyncElasticsearch)
        mock_other = Mock(spec=AsyncElasticsearch)

        set_es_client_async(mock_default)
        set_es_client_async(mock_other, client_key="other_async")

        assert get_es_client_async() == mock_default
        assert get_es_client_async("other_async") == mock_other
        assert get_es_client_async("missing") is None

    def test_clear_es_client_async_with_key(self):
        """Test `clear_es_client_async` removes only the specified async client key."""
        mock_a = Mock(spec=AsyncElasticsearch)
        mock_b = Mock(spec=AsyncElasticsearch)

        set_es_client_async(mock_a, client_key="a_async")
        set_es_client_async(mock_b, client_key="b_async")

        clear_es_client_async(client_key="a_async")

        assert get_es_client_async("a_async") is None
        assert get_es_client_async("b_async") == mock_b

    def test_get_es_client_default_and_key(self):
        """Test get_es_client returns correct client for default and keyed clients."""
        mock_client_default = Mock(spec=Elasticsearch)
        mock_client_other = Mock(spec=Elasticsearch)

        # Set default and a named client
        set_es_client(mock_client_default)
        set_es_client(mock_client_other, client_key="other")

        assert get_es_client() == mock_client_default
        assert get_es_client("other") == mock_client_other
        assert get_es_client("missing") is None

    def test_clear_es_client_with_key(self):
        """Test clearing a specific client key removes only that client."""
        mock_client_a = Mock(spec=Elasticsearch)
        mock_client_b = Mock(spec=Elasticsearch)

        set_es_client(mock_client_a, client_key="a")
        set_es_client(mock_client_b, client_key="b")

        # Clear only key 'a'
        clear_es_client(client_key="a")

        assert get_es_client("a") is None
        assert get_es_client("b") == mock_client_b

    def test_close_es_client_default(self):
        """Test closing default synchronous client."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.close = Mock()

        set_es_client(mock_client)
        assert get_es_client() == mock_client

        close_es_client()

        mock_client.close.assert_called_once()
        assert get_es_client() is None

    def test_close_es_client_with_key(self):
        """Test closing a specific synchronous client by key."""
        mock_client_a = Mock(spec=Elasticsearch)
        mock_client_a.close = Mock()
        mock_client_b = Mock(spec=Elasticsearch)
        mock_client_b.close = Mock()

        set_es_client(mock_client_a, client_key="a")
        set_es_client(mock_client_b, client_key="b")

        close_es_client(client_key="a")

        mock_client_a.close.assert_called_once()
        mock_client_b.close.assert_not_called()
        assert get_es_client("a") is None
        assert get_es_client("b") == mock_client_b

    def test_close_es_client_nonexistent(self):
        """Test closing a nonexistent client doesn't raise error."""
        close_es_client(client_key="nonexistent")  # Should not raise

    def test_close_es_client_error_handling(self):
        """Test close_es_client handles errors during close."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.close = Mock(side_effect=Exception("Close failed"))

        set_es_client(mock_client)

        close_es_client()  # Should not raise

        assert get_es_client() is None

    def test_close_all_es_clients(self):
        """Test closing all synchronous clients."""
        mock_client_a = Mock(spec=Elasticsearch)
        mock_client_a.close = Mock()
        mock_client_b = Mock(spec=Elasticsearch)
        mock_client_b.close = Mock()

        set_es_client(mock_client_a, client_key="a")
        set_es_client(mock_client_b, client_key="b")

        close_all_es_clients()

        mock_client_a.close.assert_called_once()
        mock_client_b.close.assert_called_once()
        assert get_es_client("a") is None
        assert get_es_client("b") is None

    @pytest.mark.asyncio
    async def test_close_es_client_async_default(self):
        """Test closing default asynchronous client."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_client.close = AsyncMock()

        set_es_client_async(mock_client)
        assert get_es_client_async() == mock_client

        await close_es_client_async()

        mock_client.close.assert_called_once()
        assert get_es_client_async() is None

    @pytest.mark.asyncio
    async def test_close_es_client_async_with_key(self):
        """Test closing a specific asynchronous client by key."""
        mock_client_a = Mock(spec=AsyncElasticsearch)
        mock_client_a.close = AsyncMock()
        mock_client_b = Mock(spec=AsyncElasticsearch)
        mock_client_b.close = AsyncMock()

        set_es_client_async(mock_client_a, client_key="a")
        set_es_client_async(mock_client_b, client_key="b")

        await close_es_client_async(client_key="a")

        mock_client_a.close.assert_called_once()
        mock_client_b.close.assert_not_called()
        assert get_es_client_async("a") is None
        assert get_es_client_async("b") == mock_client_b

    @pytest.mark.asyncio
    async def test_close_es_client_async_nonexistent(self):
        """Test closing a nonexistent async client doesn't raise error."""
        await close_es_client_async(client_key="nonexistent")  # Should not raise

    @pytest.mark.asyncio
    async def test_close_es_client_async_error_handling(self):
        """Test close_es_client_async handles errors during close."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_client.close = AsyncMock(side_effect=Exception("Close failed"))

        set_es_client_async(mock_client)

        await close_es_client_async()  # Should not raise

        assert get_es_client_async() is None

    @pytest.mark.asyncio
    async def test_close_all_es_clients_async(self):
        """Test closing all asynchronous clients."""
        mock_client_a = Mock(spec=AsyncElasticsearch)
        mock_client_a.close = AsyncMock()
        mock_client_b = Mock(spec=AsyncElasticsearch)
        mock_client_b.close = AsyncMock()

        set_es_client_async(mock_client_a, client_key="a")
        set_es_client_async(mock_client_b, client_key="b")

        await close_all_es_clients_async()

        mock_client_a.close.assert_called_once()
        mock_client_b.close.assert_called_once()
        assert get_es_client_async("a") is None
        assert get_es_client_async("b") is None


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

        assert "test_index" in result
        assert "mappings" in result["test_index"]
        assert "settings" in result["test_index"]
        mock_indices.get_mapping.assert_called_once_with(index="test_index")
        mock_indices.get_settings.assert_called_once_with(index="test_index")

    def test_get_index_schema_not_found(self):
        """Test get_index_schema raises NotFoundError for missing index."""
        mock_client = Mock(spec=Elasticsearch)
        mock_indices = Mock()
        mock_client.indices = mock_indices
        mock_indices.get_mapping.side_effect = conn_mod.NotFoundError("Index not found")

        with pytest.raises(conn_mod.NotFoundError):
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
        result = es_search(es=mock_client, index="test_index", query=query)

        assert result == expected_response
        mock_client.search.assert_called_once()

    def test_search_default_query(self):
        """Test search function with default match_all query."""
        mock_client = Mock(spec=Elasticsearch)
        expected_response = {"hits": {"total": {"value": 0}, "hits": []}}
        mock_client.search.return_value = expected_response

        result = es_search(es=mock_client, index="test_index")

        assert result == expected_response
        call_args = mock_client.search.call_args
        assert call_args[1]["body"] == {"match_all": {}}

    def test_search_not_found_error(self):
        """Test search raises NotFoundError for missing index."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.search.side_effect = conn_mod.NotFoundError("Index not found")

        with pytest.raises(conn_mod.NotFoundError):
            es_search(es=mock_client, index="missing_index")

    def test_search_bad_request_error(self):
        """Test search raises BadRequestError for malformed query."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.search.side_effect = [conn_mod.BadRequestError("Malformed query")]

        with pytest.raises(conn_mod.BadRequestError):
            es_search(es=mock_client, index="test_index", query={"invalid": "query"})

    def test_search_timeout_with_retry(self):
        """Test search retries on timeout."""
        mock_client = Mock(spec=Elasticsearch)
        # First call times out, second succeeds
        mock_client.search.side_effect = [
            conn_mod.ESTimeoutError("Timeout"),
            {"hits": {"total": {"value": 0}, "hits": []}},
        ]

        result = es_search(es=mock_client, index="test_index", max_retries=3, retry_delay=0.01)

        assert result == {"hits": {"total": {"value": 0}, "hits": []}}
        assert mock_client.search.call_count == 2

    def test_search_connection_error_with_retry(self):
        """Test search retries on connection error."""
        mock_client = Mock(spec=Elasticsearch)
        # First two calls fail, third succeeds
        mock_client.search.side_effect = [
            conn_mod.ESConnectionError("Connection failed"),
            conn_mod.ESConnectionError("Connection failed"),
            {"hits": {"total": {"value": 0}, "hits": []}},
        ]

        result = es_search(es=mock_client, index="test_index", max_retries=3, retry_delay=0.01)

        assert result == {"hits": {"total": {"value": 0}, "hits": []}}
        assert mock_client.search.call_count == 3

    def test_search_exhausted_retries(self):
        """Test search raises error after exhausting retries."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.search.side_effect = [
            conn_mod.ESTimeoutError("Timeout"),
            conn_mod.ESTimeoutError("Timeout"),
        ]

        with pytest.raises(conn_mod.ESTimeoutError):
            es_search(es=mock_client, index="test_index", max_retries=2, retry_delay=0.01)

        assert mock_client.search.call_count == 2

    def test_search_request_error_no_retry(self):
        """Test search doesn't retry on RequestError."""
        mock_client = Mock(spec=Elasticsearch)
        mock_client.search.side_effect = [conn_mod.RequestError("Request error")]

        with pytest.raises(conn_mod.RequestError):
            es_search(es=mock_client, index="test_index", max_retries=3)

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
        expected_mapping = {"test_index": {"mappings": {}}}
        expected_settings = {"test_index": {"settings": {}}}
        mock_indices.get_mapping = AsyncMock(return_value=expected_mapping)
        mock_indices.get_settings = AsyncMock(return_value=expected_settings)

        result = await get_index_schema_async(es=mock_client, index="test_index")

        assert result == {"test_index": {**expected_mapping["test_index"], **expected_settings["test_index"]}}

    @pytest.mark.asyncio
    async def test_get_es_version_async(self):
        """Test get_es_version_async function."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_client.info = AsyncMock(return_value={"version": {"number": "8.10.2"}})

        result = await get_es_version_async(es=mock_client)

        assert result == "8.10.2"

    @pytest.mark.asyncio
    async def test_es_search_async_success(self):
        """Test es_search_async function with successful query."""
        mock_client = Mock(spec=AsyncElasticsearch)
        expected_response = {"hits": {"total": {"value": 1}, "hits": []}}
        mock_client.search = AsyncMock(return_value=expected_response)

        result = await es_search_async(es=mock_client, index="test_index")

        assert result == expected_response

    @pytest.mark.asyncio
    async def test_es_search_async_with_retry(self):
        """Test es_search_async retries on timeout."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_client.search = AsyncMock(
            side_effect=[
                conn_mod.ESTimeoutError("Timeout"),
                {"hits": {"total": {"value": 0}, "hits": []}},
            ]
        )

        result = await es_search_async(es=mock_client, index="test_index", max_retries=3, retry_delay=0.01)

        assert result == {"hits": {"total": {"value": 0}, "hits": []}}
        assert mock_client.search.call_count == 2

    @pytest.mark.asyncio
    async def test_es_search_async_exhausted_retries(self):
        """Test es_search_async raises error after exhausting retries."""
        mock_client = Mock(spec=AsyncElasticsearch)
        mock_client.search = AsyncMock(
            side_effect=[
                conn_mod.ESConnectionError("Connection failed"),
                conn_mod.ESConnectionError("Connection failed"),
            ]
        )

        with pytest.raises(conn_mod.ESConnectionError):
            await es_search_async(es=mock_client, index="test_index", max_retries=2, retry_delay=0.01)

        assert mock_client.search.call_count == 2
