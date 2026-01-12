import asyncio
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
    _client: Optional[Elasticsearch] = None
    _async_client: Optional[AsyncElasticsearch] = None

    @classmethod
    def set(cls, client: Elasticsearch) -> None:
        """Set the default synchronous Elasticsearch client.

        Args:
            client: The Elasticsearch client instance to set as default.
        """
        with cls._lock:
            cls._client = client

    @classmethod
    def get(cls) -> Optional[Elasticsearch]:
        """Get the default synchronous Elasticsearch client.

        Returns:
            The registered Elasticsearch client or None if not set.
        """
        with cls._lock:
            return cls._client

    @classmethod
    def clear(cls) -> None:
        """Clear the default synchronous Elasticsearch client."""
        with cls._lock:
            cls._client = None

    @classmethod
    def set_async(cls, client: AsyncElasticsearch) -> None:
        """Set the default asynchronous Elasticsearch client.

        Args:
            client: The AsyncElasticsearch client instance to set as default.
        """
        with cls._lock:
            cls._async_client = client

    @classmethod
    def get_async(cls) -> Optional[AsyncElasticsearch]:
        """Get the default asynchronous Elasticsearch client.

        Returns:
            The registered AsyncElasticsearch client or None if not set.
        """
        with cls._lock:
            return cls._async_client

    @classmethod
    def clear_async(cls) -> None:
        """Clear the default asynchronous Elasticsearch client."""
        with cls._lock:
            cls._async_client = None

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
            **kwargs: Additional arguments to pass to Elasticsearch client.

        Returns:
            The created Elasticsearch client instance.
        """
        client_kwargs: Dict[str, Any] = {"verify_certs": verify_certs}

        client_kwargs.update(kwargs)

        if connection_string:
            client = Elasticsearch(connection_string, **client_kwargs)
            cls.set(client)
            return client

        url = f"{scheme}://{host}:{port}"
        auth = None
        if username is not None and password is not None:
            auth = (username, password)

        client = Elasticsearch([url], http_auth=auth, **client_kwargs)
        cls.set(client)
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
            **kwargs: Additional arguments to pass to AsyncElasticsearch client.

        Returns:
            The created AsyncElasticsearch client instance.
        """
        client_kwargs: Dict[str, Any] = {"verify_certs": verify_certs}

        client_kwargs.update(kwargs)

        if connection_string:
            client = AsyncElasticsearch(connection_string, **client_kwargs)
            cls.set_async(client)
            return client

        url = f"{scheme}://{host}:{port}"
        auth = None
        if username is not None and password is not None:
            auth = (username, password)

        client = AsyncElasticsearch([url], http_auth=auth, **client_kwargs)
        cls.set_async(client)
        return client


def connect_es(
    connection_string: Optional[str] = None,
    host: str = "localhost",
    port: int = 9200,
    scheme: str = "http",
    username: Optional[str] = None,
    password: Optional[str] = None,
    verify_certs: bool = True,
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
        **kwargs: Additional arguments to pass to Elasticsearch client.

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
        **kwargs,
    )


def set_default_es(es: Elasticsearch) -> None:
    """Set the module-level default synchronous Elasticsearch client.

    Args:
        es: The Elasticsearch client instance to set as default.
    """
    ESClientSingleton.set(es)


def clear_default_es() -> None:
    """Clear the module-level default synchronous Elasticsearch client."""
    ESClientSingleton.clear()


def set_default_es_async(es: AsyncElasticsearch) -> None:
    """Set the module-level default asynchronous Elasticsearch client.

    Args:
        es: The AsyncElasticsearch client instance to set as default.
    """
    ESClientSingleton.set_async(es)


def clear_default_es_async() -> None:
    """Clear the module-level default asynchronous Elasticsearch client."""
    ESClientSingleton.clear_async()


@requires_es_client
def ping(es: Optional[Elasticsearch] = None) -> bool:
    """Ping the Elasticsearch cluster to check connectivity.

    Args:
        es: Elasticsearch client (auto-injected if not provided).

    Returns:
        True if the cluster is reachable, False otherwise.
    """
    try:
        return bool(es.ping())
    except Exception:
        return False


@requires_es_client
def get_index_schema(index: str, es: Optional[Elasticsearch] = None) -> Dict[str, Any]:
    """Return the mapping/schema for the given index.

    Args:
        es: Elasticsearch client (auto-injected if not provided).
        index: The name of the index to retrieve the schema for.

    Returns:
        Dictionary containing the index mapping/schema.

    Raises:
        NotFoundError: If the index does not exist.
    """
    return {
        **get_index_mapping(index=index, es=es)[index],
        **get_index_settings(index=index, es=es)[index],
    }


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
def search(
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
        size: Number of results to return (default: 10).
        from_: Offset for pagination (default: 0).
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

    last_exception = None

    for attempt in range(max_retries):
        try:
            response = es.search(
                index=index,
                body=query,
                from_=from_,
                request_timeout=timeout,
            )
            return response
        except NotFoundError:
            # Index doesn't exist — no point retrying
            raise
        except BadRequestError as e:
            # Malformed query — no point retrying
            raise
        except (ESTimeoutError, ESConnectionError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                # Exponential backoff: delay * (2 ^ attempt)
                backoff = retry_delay * (2**attempt)
                time.sleep(backoff)
            continue
        except RequestError as e:
            # Other request errors — re-raise
            raise

    # All retries exhausted
    if last_exception:
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
        **kwargs: Additional arguments to pass to AsyncElasticsearch client.

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
    try:
        return bool(await es.ping())
    except Exception:
        return False


@requires_es_client_async
async def get_index_schema_async(es: Optional[AsyncElasticsearch] = None, index: str = "") -> Dict[str, Any]:
    """Return the mapping/schema for the given index asynchronously.

    Args:
        es: AsyncElasticsearch client (auto-injected if not provided).
        index: The name of the index to retrieve the schema for.

    Returns:
        Dictionary containing the index mapping/schema.

    Raises:
        NotFoundError: If the index does not exist.
    """
    return await es.indices.get_mapping(index=index)


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
async def search_async(
    es: Optional[AsyncElasticsearch] = None,
    index: str = "*",
    query: Optional[Dict[str, Any]] = None,
    size: int = 10,
    from_: int = 0,
    timeout: str = "10s",
    max_retries: int = 3,
    retry_delay: float = 0.5,
) -> Dict[str, Any]:
    """Execute a search query against Elasticsearch (async) with retry and timeout support.

    Args:
        es: Async Elasticsearch client (injected by @requires_es_client_async or passed explicitly).
        index: Index name or pattern (default: "*" for all indices).
        query: Elasticsearch query dict (default: empty match_all query).
        size: Number of results to return (default: 10).
        from_: Offset for pagination (default: 0).
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
        RuntimeError: If no async client is available.
    """
    if query is None:
        query = {"match_all": {}}

    last_exception = None

    for attempt in range(max_retries):
        try:
            response = await es.search(
                index=index,
                query=query,
                size=size,
                from_=from_,
                request_timeout=timeout,
            )
            return response
        except NotFoundError:
            # Index doesn't exist — no point retrying
            raise
        except BadRequestError as e:
            # Malformed query — no point retrying
            raise
        except (ESTimeoutError, ESConnectionError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                # Exponential backoff: delay * (2 ^ attempt)
                backoff = retry_delay * (2**attempt)
                await asyncio.sleep(backoff)
            continue
        except RequestError as e:
            # Other request errors — re-raise
            raise

    # All retries exhausted
    if last_exception:
        raise last_exception
    # Fallback (shouldn't reach here)
    raise RuntimeError("Async search failed after all retries")
