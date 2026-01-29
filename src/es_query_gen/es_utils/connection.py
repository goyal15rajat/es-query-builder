import asyncio
import logging
import threading
import time
from functools import wraps
from typing import Any, Dict, Optional

from elasticsearch import AsyncElasticsearch, Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError
from elasticsearch.exceptions import ConnectionTimeout as ESTimeoutError
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import RequestError
from elasticsearch.exceptions import RequestError as BadRequestError

logger = logging.getLogger(__name__)


def requires_es_client(func):
    """Decorator to ensure an Elasticsearch client is available.

    This decorator checks if an Elasticsearch client is provided as an argument.
    If not, it attempts to retrieve the default client from the singleton.

    Args:
        func: The function to wrap.

    Returns:
        The wrapped function with the Elasticsearch client injected.

    Raises:
        RuntimeError: If no Elasticsearch client is available.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        # 1. Check if 'es' was passed explicitly in kwargs
        client = kwargs.get("es")

        # 2. If not, try to fetch it from the Singleton
        if client is None:
            client = ESClientSingleton.get()

        # 3. Validation Logic (The "Guard Clause")
        if client is None:
            raise RuntimeError("Elasticsearch client not provided and no default is set")

        # 4. Inject the valid client into the function
        # We update kwargs so the original function receives the actual client object
        kwargs["es"] = client

        return func(*args, **kwargs)

    return wrapper


def requires_es_client_async(func):
    """Decorator to ensure an asynchronous Elasticsearch client is available.

    This decorator checks if an asynchronous Elasticsearch client is provided as an argument.
    If not, it attempts to retrieve the default async client from the singleton.

    Args:
        func: The asynchronous function to wrap.

    Returns:
        The wrapped asynchronous function with the Elasticsearch client injected.

    Raises:
        RuntimeError: If no asynchronous Elasticsearch client is available.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        # 1. Check if 'es' was passed explicitly in kwargs
        client = kwargs.get("es")

        # 2. If not, try to fetch async client from Singleton
        if client is None:
            client = ESClientSingleton.get_async()

        # 3. Validation Logic (The "Guard Clause")
        if client is None:
            raise RuntimeError("Async Elasticsearch client not provided and no default is set")

        # 4. Inject the valid client into the function
        kwargs["es"] = client

        return await func(*args, **kwargs)

    return wrapper


class ESClientSingleton:
    """Lightweight singleton/registry for Elasticsearch clients (sync and async).

    Use `connect()` to create and register a default sync client.
    Use `connect_async()` to create and register a default async client.
    Use `set()`, `get()`, `clear()` for sync client management.
    Use `set_async()`, `get_async()`, `clear_async()` for async client management.
    """

    _lock = threading.Lock()
    _clients: Dict[str, Elasticsearch] = {}
    _async_clients: Dict[str, AsyncElasticsearch] = {}

    @classmethod
    def set(cls, client: Elasticsearch, client_key: str = "default") -> None:
        """Set the default synchronous Elasticsearch client.

        Args:
            client: The Elasticsearch client instance to set as default.
            client_key: Key to identify the synchronous client (default: 'default').
        """
        with cls._lock:
            cls._clients[client_key] = client

    @classmethod
    def get(cls, client_key: str = "default") -> Optional[Elasticsearch]:
        """Get the default synchronous Elasticsearch client.

        Args:
            client_key: Key to identify the synchronous client (default: 'default').

        Returns:
            The registered Elasticsearch client or None if not set.
        """
        with cls._lock:
            return cls._clients.get(client_key)

    @classmethod
    def clear(cls, client_key: Optional[str] = None) -> None:
        """Clear the default synchronous Elasticsearch client.
        Args:
            client_key: Key to identify the synchronous client (default: None, clears all).
        """
        with cls._lock:
            if client_key:
                cls._clients.pop(client_key, None)
            else:
                cls._clients = {}

    @classmethod
    def set_async(cls, client: AsyncElasticsearch, client_key: str = "default") -> None:
        """Set the default asynchronous Elasticsearch client.

        Args:
            client: The AsyncElasticsearch client instance to set as default.
            client_key: Key to identify the asynchronous client (default: 'default').
        """
        with cls._lock:
            cls._async_clients[client_key] = client

    @classmethod
    def get_async(cls, client_key: str = "default") -> Optional[AsyncElasticsearch]:
        """Get the default asynchronous Elasticsearch client.

        Args:
            client_key: Key to identify the asynchronous client (default: 'default').

        Returns:
            The registered AsyncElasticsearch client or None if not set.
        """
        with cls._lock:
            return cls._async_clients.get(client_key)

    @classmethod
    def clear_async(cls, client_key: Optional[str] = None) -> None:
        """Clear the default asynchronous Elasticsearch client.
        Args:
            client_key: Key to identify the asynchronous client (default: None, clears all).
        """
        with cls._lock:
            if client_key:
                cls._async_clients.pop(client_key, None)
            else:
                cls._async_clients = {}

    @classmethod
    def close(cls, client_key: Optional[str] = None) -> None:
        """Close Elasticsearch client connection(s).

        If client_key is provided, closes that specific client.
        Otherwise, closes all registered synchronous clients.

        Args:
            client_key: Key to identify the synchronous client (default: None, closes all).
        """
        with cls._lock:
            clients_to_close = {}
            if client_key:
                if client_key in cls._clients:
                    clients_to_close[client_key] = cls._clients[client_key]
            else:
                clients_to_close = dict(cls._clients)

        for key, client in clients_to_close.items():
            try:
                client.close()
                logger.info(f"Closed synchronous Elasticsearch client '{key}'")
            except Exception as e:
                logger.warning(f"Error closing synchronous client '{key}': {e}")
            finally:
                with cls._lock:
                    cls._clients.pop(key, None)

    @classmethod
    async def close_async(cls, client_key: Optional[str] = None) -> None:
        """Close asynchronous Elasticsearch client connection(s).

        If client_key is provided, closes that specific async client.
        Otherwise, closes all registered asynchronous clients.

        Args:
            client_key: Key to identify the asynchronous client (default: None, closes all).
        """
        with cls._lock:
            clients_to_close = {}
            if client_key:
                if client_key in cls._async_clients:
                    clients_to_close[client_key] = cls._async_clients[client_key]
            else:
                clients_to_close = dict(cls._async_clients)

        for key, client in clients_to_close.items():
            try:
                await client.close()
                logger.info(f"Closed asynchronous Elasticsearch client '{key}'")
            except Exception as e:
                logger.warning(f"Error closing asynchronous client '{key}': {e}")
            finally:
                with cls._lock:
                    cls._async_clients.pop(key, None)

    @classmethod
    def connect(
        cls,
        connection_string: Optional[str] = None,
        host: str = "localhost",
        port: int = 9200,
        scheme: str = "http",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_certs: bool = True,
        client_key: str = "default",
        **kwargs: Any,
    ) -> Elasticsearch:
        """Create and register a synchronous Elasticsearch client as default.

        Args:
            connection_string: Full connection string (takes precedence over other params).
            host: Elasticsearch host (default: 'localhost').
            port: Elasticsearch port (default: 9200).
            scheme: Connection scheme, 'http' or 'https' (default: 'http').
            username: Username for authentication (optional).
            password: Password for authentication (optional).
            verify_certs: Whether to verify SSL certificates (default: True).
            client_key: Key to identify the synchronous client (default: 'default').
            kwargs: Additional arguments to pass to Elasticsearch client.

        Returns:
            The created Elasticsearch client instance.
        """
        logger.info(f"Connecting to Elasticsearch at {scheme}://{host}:{port}")
        client_kwargs: Dict[str, Any] = {"verify_certs": verify_certs}

        client_kwargs.update(kwargs)

        if connection_string:
            logger.debug("Using connection string")
            client = Elasticsearch(connection_string, **client_kwargs)
            cls.set(client, client_key=client_key)
            return client

        url = f"{scheme}://{host}:{port}"
        auth = None
        if username is not None and password is not None:
            auth = (username, password)
            logger.debug("Using authentication")

        client = Elasticsearch([url], http_auth=auth, **client_kwargs)
        cls.set(client, client_key=client_key)
        logger.info(f"Elasticsearch client connected and set as {client_key}")
        return client

    @classmethod
    def connect_async(
        cls,
        connection_string: Optional[str] = None,
        host: str = "localhost",
        port: int = 9200,
        scheme: str = "http",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_certs: bool = True,
        client_key: str = "default",
        **kwargs: Any,
    ) -> AsyncElasticsearch:
        """Create and register an asynchronous Elasticsearch client as default.

        Args:
            connection_string: Full connection string (takes precedence over other params).
            host: Elasticsearch host (default: 'localhost').
            port: Elasticsearch port (default: 9200).
            scheme: Connection scheme, 'http' or 'https' (default: 'http').
            username: Username for authentication (optional).
            password: Password for authentication (optional).
            verify_certs: Whether to verify SSL certificates (default: True).
            client_key: Key to identify the asynchronous client (default: 'default').
            kwargs: Additional arguments to pass to AsyncElasticsearch client.

        Returns:
            The created AsyncElasticsearch client instance.
        """
        logger.info(f"Connecting to Elasticsearch (async) at {scheme}://{host}:{port}")
        client_kwargs: Dict[str, Any] = {"verify_certs": verify_certs}

        client_kwargs.update(kwargs)

        if connection_string:
            logger.debug("Using connection string (async)")
            client = AsyncElasticsearch(connection_string, **client_kwargs)
            cls.set_async(client, client_key=client_key)
            return client

        url = f"{scheme}://{host}:{port}"
        auth = None
        if username is not None and password is not None:
            auth = (username, password)
            logger.debug("Using authentication (async)")

        client = AsyncElasticsearch([url], http_auth=auth, **client_kwargs)
        cls.set_async(client, client_key=client_key)
        logger.info(f"Async Elasticsearch client connected and set as {client_key}")
        return client


def connect_es(
    connection_string: Optional[str] = None,
    host: str = "localhost",
    port: int = 9200,
    scheme: str = "http",
    username: Optional[str] = None,
    password: Optional[str] = None,
    verify_certs: bool = True,
    client_key: str = "default",
    **kwargs: Any,
) -> Elasticsearch:
    """Create and return a synchronous Elasticsearch client and register it as default.

    Args:
        connection_string: Full connection string (takes precedence over other params).
        host: Elasticsearch host (default: 'localhost').
        port: Elasticsearch port (default: 9200).
        scheme: Connection scheme, 'http' or 'https' (default: 'http').
        username: Username for authentication (optional).
        password: Password for authentication (optional).
        verify_certs: Whether to verify SSL certificates (default: True).
        client_key: Key to identify the synchronous client (default: 'default').
        kwargs: Additional arguments to pass to Elasticsearch client.

    Returns:
        The created Elasticsearch client instance.
    """
    return ESClientSingleton.connect(
        connection_string=connection_string,
        host=host,
        port=port,
        scheme=scheme,
        username=username,
        password=password,
        verify_certs=verify_certs,
        client_key=client_key,
        **kwargs,
    )


def set_es_client(es: Elasticsearch, client_key: str = "default") -> None:
    """Set the module-level default synchronous Elasticsearch client.

    Args:
        es: The Elasticsearch client instance to set as default.
        client_key: Key to identify the synchronous client (default: 'default').
    """
    ESClientSingleton.set(es, client_key=client_key)


def get_es_client(client_key: str = "default") -> Optional[Elasticsearch]:
    """Get the module-level default synchronous Elasticsearch client.

    Args:
        client_key: Key to identify the synchronous client (default: 'default').
    """
    return ESClientSingleton.get(client_key=client_key)


def clear_es_client(client_key: str = "default") -> None:
    """Clear the module-level default synchronous Elasticsearch client.
    Args:
        client_key: Key to identify the synchronous client (default: 'default').
    """
    ESClientSingleton.clear(client_key=client_key)


def set_es_client_async(es: AsyncElasticsearch, client_key: str = "default") -> None:
    """Set the module-level default asynchronous Elasticsearch client.

    Args:
        es: The AsyncElasticsearch client instance to set as default.
        client_key: Key to identify the asynchronous client (default: 'default').
    """
    ESClientSingleton.set_async(es, client_key=client_key)


def get_es_client_async(client_key: str = "default") -> Optional[AsyncElasticsearch]:
    """Get the module-level default asynchronous Elasticsearch client.

    Args:
        client_key: Key to identify the asynchronous client (default: 'default').
    """
    return ESClientSingleton.get_async(client_key=client_key)


def clear_es_client_async(client_key: str = "default") -> None:
    """Clear the module-level default asynchronous Elasticsearch client.

    Args:
        client_key: Key to identify the asynchronous client (default: 'default').
    """
    ESClientSingleton.clear_async(client_key=client_key)


def close_es_client(client_key: Optional[str] = "default") -> None:
    """Close Elasticsearch client connection(s).

    If client_key is provided, closes that specific client.
    Otherwise, closes all registered synchronous clients.

    Args:
        client_key: Key to identify the synchronous client (default: 'default').
    """
    ESClientSingleton.close(client_key=client_key)


async def close_es_client_async(client_key: Optional[str] = "default") -> None:
    """Close asynchronous Elasticsearch client connection(s).

    If client_key is provided, closes that specific async client.
    Otherwise, closes all registered asynchronous clients.

    Args:
        client_key: Key to identify the asynchronous client (default: 'default').
    """
    await ESClientSingleton.close_async(client_key=client_key)


def close_all_es_clients() -> None:
    """Close all synchronous Elasticsearch client connections."""
    ESClientSingleton.close(client_key=None)


async def close_all_es_clients_async() -> None:
    """Close all asynchronous Elasticsearch client connections."""
    await ESClientSingleton.close_async(client_key=None)


@requires_es_client
def ping(es: Optional[Elasticsearch] = None) -> bool:
    """Ping the Elasticsearch cluster to check connectivity.

    Args:
        es: Elasticsearch client (auto-injected if not provided).

    Returns:
        True if the cluster is reachable, False otherwise.
    """
    start_time = time.perf_counter()
    try:
        result = bool(es.ping())
        elapsed = (time.perf_counter() - start_time) * 1000
        logger.info(f"Elasticsearch ping completed in {elapsed:.2f}ms")
        return result
    except Exception as e:
        elapsed = (time.perf_counter() - start_time) * 1000
        logger.warning(f"Elasticsearch ping failed after {elapsed:.2f}ms: {e}")
        return False


@requires_es_client
def get_index_schema(index: str, es: Optional[Elasticsearch] = None) -> Dict[str, Any]:
    """Return the mapping/schema for the given index.

    Args:
        es: Elasticsearch client (auto-injected if not provided).
        index: The name of the index or alias to retrieve the schema for.

    Returns:
        Dictionary containing the index mapping/schema keyed by index name.

    Raises:
        NotFoundError: If the index does not exist.
    """

    es_mapping = get_index_mapping(index=index, es=es)
    es_settings = get_index_settings(index=index, es=es)

    return {index: {**es_mapping[index], **es_settings[index]} for index in es_mapping.keys()}


@requires_es_client
def get_index_mapping(index: str, es: Optional[Elasticsearch] = None) -> Dict[str, Any]:
    """Return the mapping for the given index.

    Args:
        es: Elasticsearch client (auto-injected if not provided).
        index: The name of the index to retrieve the for.

    Returns:
        Dictionary containing the index mapping.

    Raises:
        NotFoundError: If the index does not exist.
    """
    return es.indices.get_mapping(index=index)


@requires_es_client
def get_index_settings(index: str, es: Optional[Elasticsearch] = None) -> Dict[str, Any]:
    """Return the settings for the given index.

    Args:
        es: Elasticsearch client (auto-injected if not provided).
        index: The name of the index to retrieve the settings for.

    Returns:
        Dictionary containing the index settings.

    Raises:
        NotFoundError: If the index does not exist.
    """
    return es.indices.get_settings(index=index)


@requires_es_client
def get_es_version(es: Optional[Elasticsearch] = None) -> Optional[str]:
    """Return the Elasticsearch version string.

    Args:
        es: Elasticsearch client (auto-injected if not provided).

    Returns:
        Version string (e.g., '7.10.2') or None if unavailable.
    """
    info = es.info()
    version = info.get("version", {})
    return version.get("number")


@requires_es_client
def es_search(
    es: Optional[Elasticsearch] = None,
    index: str = "*",
    query: Optional[Dict[str, Any]] = None,
    from_: int = 0,
    timeout: int = 10,
    max_retries: int = 3,
    retry_delay: float = 0.5,
) -> Dict[str, Any]:
    """Execute a search query against Elasticsearch with retry and timeout support.

    Args:
        es: Elasticsearch client (injected by @requires_es_client or passed explicitly).
        index: Index name or pattern (default: "*" for all indices).
        query: Elasticsearch query dict (default: empty match_all query).
        ``from_``: Offset for pagination (default: 0).
        timeout: Server-side timeout (default: "10s").
        max_retries: Number of retry attempts (default: 3).
        retry_delay: Initial delay between retries in seconds (default: 0.5, with exponential backoff).

    Returns:
        The full Elasticsearch search response dict.

    Raises:
        NotFoundError: If the index does not exist.
        BadRequestError: If the query is malformed.
        ESTimeoutError: If the server or client timeout is exceeded after retries.
        ESConnectionError: If unable to connect to Elasticsearch after retries.
        RequestError: For other Elasticsearch request errors.
        RuntimeError: If no client is available.
    """
    if query is None:
        query = {"match_all": {}}

    logger.debug(f"Searching index '{index}' from offset {from_} with query: {query}")
    last_exception = None
    start_time = time.perf_counter()

    for attempt in range(max_retries):
        try:
            response = es.search(
                index=index,
                body=query,
                from_=from_,
                request_timeout=timeout,
            )
            elapsed = (time.perf_counter() - start_time) * 1000
            logger.info(f"Search completed in {elapsed:.2f}ms (index='{index}', from={from_}, query={query})")
            return response
        except NotFoundError:
            logger.error(f"Index '{index}' not found")
            raise
        except BadRequestError as e:
            logger.error(f"Malformed query: {e}")
            raise
        except (ESTimeoutError, ESConnectionError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                backoff = retry_delay * (2**attempt)
                logger.warning(f"Search failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {backoff}s...")
                time.sleep(backoff)
            continue
        except RequestError as e:
            logger.error(f"Elasticsearch request error: {e}")
            raise

    # All retries exhausted
    if last_exception:
        logger.error(f"Search failed after {max_retries} attempts: {last_exception}")
        raise last_exception
    # Fallback (shouldn't reach here)
    raise RuntimeError("Search failed after all retries")


# ============================================================================
# ASYNC VERSIONS
# ============================================================================


async def connect_es_async(
    connection_string: Optional[str] = None,
    host: str = "localhost",
    port: int = 9200,
    scheme: str = "http",
    username: Optional[str] = None,
    password: Optional[str] = None,
    verify_certs: bool = True,
    client_key: str = "default",
    **kwargs: Any,
) -> AsyncElasticsearch:
    """Create and return an asynchronous Elasticsearch client and register it as default.

    Args:
        connection_string: Full connection string (takes precedence over other params).
        host: Elasticsearch host (default: 'localhost').
        port: Elasticsearch port (default: 9200).
        scheme: Connection scheme, 'http' or 'https' (default: 'http').
        username: Username for authentication (optional).
        password: Password for authentication (optional).
        verify_certs: Whether to verify SSL certificates (default: True).
        kwargs: Additional arguments to pass to AsyncElasticsearch client.

    Returns:
        The created AsyncElasticsearch client instance.
    """
    return ESClientSingleton.connect_async(
        connection_string=connection_string,
        host=host,
        port=port,
        scheme=scheme,
        username=username,
        password=password,
        verify_certs=verify_certs,
        client_key=client_key,
        **kwargs,
    )


@requires_es_client_async
async def ping_async(es: Optional[AsyncElasticsearch] = None) -> bool:
    """Ping the Elasticsearch cluster asynchronously to check connectivity.

    Args:
        es: AsyncElasticsearch client (auto-injected if not provided).

    Returns:
        True if the cluster is reachable, False otherwise.
    """
    start_time = time.perf_counter()
    try:
        result = bool(await es.ping())
        elapsed = (time.perf_counter() - start_time) * 1000
        logger.info(f"Elasticsearch ping (async) completed in {elapsed:.2f}ms")
        return result
    except Exception as e:
        elapsed = (time.perf_counter() - start_time) * 1000
        logger.warning(f"Elasticsearch ping (async) failed after {elapsed:.2f}ms: {e}")
        return False


@requires_es_client_async
async def get_index_schema_async(es: Optional[AsyncElasticsearch] = None, index: str = "") -> Dict[str, Any]:
    """Return the mapping/schema for the given index asynchronously.

    Args:
        es: AsyncElasticsearch client (auto-injected if not provided).
        index: The name of the index or alias to retrieve the schema for.

    Returns:
        Dictionary containing the index mapping/schema keyed by index name.

    Raises:
        NotFoundError: If the index does not exist.
    """
    es_mapping = await es.indices.get_mapping(index=index)
    es_settings = await es.indices.get_settings(index=index)

    return {index: {**es_mapping[index], **es_settings[index]} for index in es_mapping.keys()}


@requires_es_client_async
async def get_index_settings_async(es: Optional[AsyncElasticsearch] = None, index: str = "") -> Dict[str, Any]:
    """Return the settings for the given index asynchronously.

    Args:
        es: AsyncElasticsearch client (auto-injected if not provided).
        index: The name of the index to retrieve the settings for.

    Returns:
        Dictionary containing the index settings.

    Raises:
        NotFoundError: If the index does not exist.
    """
    return await es.indices.get_settings(index=index)


@requires_es_client_async
async def get_es_version_async(es: Optional[AsyncElasticsearch] = None) -> Optional[str]:
    """Return the Elasticsearch version string asynchronously.

    Args:
        es: AsyncElasticsearch client (auto-injected if not provided).

    Returns:
        Version string (e.g., '7.10.2') or None if unavailable.
    """
    info = await es.info()
    version = info.get("version", {})
    return version.get("number")


@requires_es_client_async
async def es_search_async(
    es: Optional[AsyncElasticsearch] = None,
    index: str = "*",
    query: Optional[Dict[str, Any]] = None,
    from_: int = 0,
    timeout: int = 10,
    max_retries: int = 3,
    retry_delay: float = 0.5,
) -> Dict[str, Any]:
    """Execute a search query against Elasticsearch (async) with retry and timeout support.

    Args:
        es: Async Elasticsearch client (injected by @requires_es_client_async or passed explicitly).
        index: Index name or pattern (default: "*" for all indices).
        query: Elasticsearch query dict (default: empty match_all query).
        ``from_``: Offset for pagination (default: 0).
        timeout: Server-side timeout in seconds (default: 10).
        max_retries: Number of retry attempts (default: 3).
        retry_delay: Initial delay between retries in seconds (default: 0.5, with exponential backoff).

    Returns:
        The full Elasticsearch search response dict.

    Raises:
        NotFoundError: If the index does not exist.
        BadRequestError: If the query is malformed.
        ESTimeoutError: If the server or client timeout is exceeded after retries.
        ESConnectionError: If unable to connect to Elasticsearch after retries.
        RequestError: For other Elasticsearch request errors.
        RuntimeError: If no async client is available.
    """
    if query is None:
        query = {"match_all": {}}

    logger.debug(f"Searching index '{index}' (async) from offset {from_} with query: {query}")
    last_exception = None
    start_time = time.perf_counter()

    for attempt in range(max_retries):
        try:
            response = await es.search(
                index=index,
                body=query,
                from_=from_,
                request_timeout=timeout,
            )
            elapsed = (time.perf_counter() - start_time) * 1000
            logger.info(f"Search (async) completed in {elapsed:.2f}ms (index='{index}', from={from_}, query={query})")
            return response
        except NotFoundError:
            logger.error(f"Index '{index}' not found (async)")
            raise
        except BadRequestError as e:
            logger.error(f"Malformed query (async): {e}")
            raise
        except (ESTimeoutError, ESConnectionError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                backoff = retry_delay * (2**attempt)
                logger.warning(
                    f"Async search failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {backoff}s..."
                )
                await asyncio.sleep(backoff)
            continue
        except RequestError as e:
            logger.error(f"Elasticsearch request error (async): {e}")
            raise

    # All retries exhausted
    if last_exception:
        raise last_exception
    # Fallback (shouldn't reach here)
    raise RuntimeError("Async search failed after all retries")
