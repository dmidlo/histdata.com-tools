"""Explicit host resource API: bounded ephemeral cache, never publication."""

from __future__ import annotations

import threading
from .contracts import BrokerSecurityReason as Reason, refuse


class BrokerHostResources:
    """No scientific paths, file descriptors, experiment or product authority.

    This API boundary is not a replacement for kernel isolation. Trusted
    in-process code still has the caller's ambient Python/OS authority.
    """

    __slots__ = ("_cache", "_bytes", "_limit", "_lock")

    def __init__(self, *, cache_bytes: int = 65_536) -> None:
        if type(cache_bytes) is not int or not 0 <= cache_bytes <= 1_048_576:
            refuse(Reason.RESOURCE)
        self._cache: dict[str, bytes] = {}
        self._bytes = 0
        self._limit = cache_bytes
        self._lock = threading.Lock()

    def put_cache(self, key: str, value: bytes) -> None:
        if (
            type(key) is not str
            or not 1 <= len(key) <= 128
            or type(value) is not bytes
        ):
            refuse(Reason.RESOURCE)
        with self._lock:
            size = self._bytes - len(self._cache.get(key, b"")) + len(value)
            if size > self._limit or (
                key not in self._cache and len(self._cache) >= 128
            ):
                refuse(Reason.RESOURCE)
            self._cache[key] = value
            self._bytes = size

    def get_cache(self, key: str) -> bytes | None:
        if type(key) is not str or not 1 <= len(key) <= 128:
            refuse(Reason.RESOURCE)
        with self._lock:
            return self._cache.get(key)

    def publish_scientific_product(self, product: object) -> None:
        refuse(Reason.PUBLICATION)

    def request(self, operation: str) -> None:
        # Unknown/grant-like operation names cannot confer new authority.
        refuse(
            Reason.PUBLICATION
            if operation == "publish_scientific_product"
            else Reason.RESOURCE
        )
