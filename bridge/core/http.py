"""Shared HTTP client for bridge modules.

Usage:
    import bridge.core.http as _http_mod
    _http_mod._http_client = client  # set from lifespan

Or access via:
    from bridge.core.http import get_http_client
"""
import httpx

_http_client: httpx.AsyncClient = None  # type: ignore  # initialized in lifespan


def get_http_client() -> httpx.AsyncClient:
    """Return the shared HTTP client. Must be called after lifespan startup."""
    return _http_client
