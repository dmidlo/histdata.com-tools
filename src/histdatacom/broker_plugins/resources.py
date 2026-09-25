"""Opt-in factory resource ABI; no host implementation or secret export.

A factory declaring ``host_resources_v1`` receives this interface as its one
argument. The ordinary V1 plugin/session and primitive configuration contracts
remain unchanged. In-process plugins remain trusted Python, not sandboxed code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NoReturn, Protocol


@dataclass(frozen=True, slots=True)
class BrokerHostHTTPResponseV1:
    """Bounded body and status only; authentication headers are never returned."""

    status: int
    body: bytes

    def __post_init__(self) -> None:
        if (
            type(self.status) is not int
            or not 100 <= self.status <= 599
            or type(self.body) is not bytes
            or len(self.body) > 1_048_576
        ):
            raise ValueError("invalid broker host HTTP response")


class BrokerHostResourcesV1(Protocol):
    """Only declared resources; no generic file, credential, or store handle."""

    def available_permissions(self) -> tuple[str, ...]: ...

    def request(
        self,
        endpoint_id: str,
        method: str,
        path: str,
        *,
        body: bytes = b"",
        secret_profile: str | None = None,
    ) -> BrokerHostHTTPResponseV1: ...

    def put_cache(self, cache_id: str, key: str, value: bytes) -> None: ...

    def get_cache(self, cache_id: str, key: str) -> bytes | None: ...

    def request_subprocess(self, operation: str) -> NoReturn: ...
