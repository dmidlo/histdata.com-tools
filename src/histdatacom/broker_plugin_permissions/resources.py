"""Host-mediated bounded transport/authentication and ephemeral named caches.

Only this host object resolves secrets. Plugin-facing methods never export
them, accept arbitrary headers, follow redirects, or expose filesystem paths.
Trusted in-process Python can still reach ambient authority outside this API.
"""

from __future__ import annotations

import base64
import json
import re
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn

from histdatacom.broker_plugins.resources import BrokerHostHTTPResponseV1

from .contracts import BrokerPermissionCacheV1, permission_name
from .decisions import BrokerPermissionAuthorityV1

if TYPE_CHECKING:
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugin_security.secrets import BrokerSecretProvider


_DEADLINE: ContextVar[float | None] = ContextVar(
    "broker_resource_deadline", default=None
)


@contextmanager
def resource_deadline(deadline: float) -> Iterator[None]:
    token = _DEADLINE.set(deadline)
    try:
        yield
    finally:
        _DEADLINE.reset(token)


# A trusted one-request helper bounds DNS, TLS, headers and response reads with
# a parent deadline. No secret is placed in argv/environment or an exception.
# This is host transport implementation, not plugin subprocess permission.
_HTTP_HELPER = r"""
import base64, http.client, json, sys
from urllib.parse import urlsplit
try:
    value = json.loads(sys.stdin.buffer.read(2097152))
    origin = urlsplit(value['origin'])
    connection_type = http.client.HTTPSConnection if origin.scheme == 'https' else http.client.HTTPConnection
    connection = connection_type(origin.hostname, origin.port, timeout=value['timeout'])
    headers = {'Accept': 'application/octet-stream', 'Connection': 'close'}
    if value['authorization'] is not None:
        headers['Authorization'] = value['authorization']
    connection.request(value['method'], value['path'], base64.b64decode(value['body'], validate=True), headers)
    response = connection.getresponse()
    if 300 <= response.status < 400:
        raise ValueError()
    body = response.read(value['maximum'] + 1)
    if len(body) > value['maximum']:
        raise ValueError()
    print(json.dumps({'status': response.status, 'body': base64.b64encode(body).decode('ascii')}))
    connection.close()
except BaseException:
    sys.exit(70)
"""


@dataclass(frozen=True, slots=True, repr=False)
class BrokerHostSecretProfileV1:
    """Operator-only binding; never serialized into plugin/capture evidence."""

    profile_id: str
    handle: str
    bearer: bool = True

    def __post_init__(self) -> None:
        permission_name(self.profile_id)
        if (
            type(self.handle) is not str
            or not 1 <= len(self.handle) <= 512
            or any(ord(char) < 33 or ord(char) == 127 for char in self.handle)
            or type(self.bearer) is not bool
        ):
            raise ValueError("invalid host secret profile")


class BrokerPermissionResourcesV1:
    """Concrete host implementation of the explicit SDK resource extension."""

    def __init__(
        self,
        authority: BrokerPermissionAuthorityV1,
        *,
        provider_request: BrokerSDKInvocationV1,
        secret_profiles: tuple[BrokerHostSecretProfileV1, ...] = (),
        secret_provider: BrokerSecretProvider | None = None,
    ) -> None:
        if type(authority) is not BrokerPermissionAuthorityV1:
            raise ValueError("exact permission authority required")
        authority.require_admission()
        if authority.manifest.resource_abi != "host_resources_v1":
            raise ValueError("host resources not declared")
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerSDKInvocationV1,
        )

        if type(provider_request) is not BrokerSDKInvocationV1:
            raise ValueError("exact provider resource invocation required")
        request = BrokerSDKInvocationV1.from_json(provider_request.to_json())
        if (
            request.plan.candidate.artifact_id != authority.binding.candidate_id
            or request.configuration_profile.artifact_id
            != authority.binding.configuration_id
            or request.configuration_profile.provider_id
            != authority.binding.provider_id
            or request.configuration_profile.private_field_names
        ):
            raise ValueError("provider resource invocation binding mismatch")
        self._request = request
        if (
            type(secret_profiles) is not tuple
            or any(
                type(item) is not BrokerHostSecretProfileV1
                for item in secret_profiles
            )
            or len({item.profile_id for item in secret_profiles})
            != len(secret_profiles)
            or {item.profile_id for item in secret_profiles}
            - set(authority.manifest.secret_profiles)
            or (secret_profiles and secret_provider is None)
        ):
            raise ValueError("invalid operator secret bindings")
        self._authority: BrokerPermissionAuthorityV1 = authority
        self._profiles = {item.profile_id: item for item in secret_profiles}
        self._provider = secret_provider
        self._cache: dict[str, dict[str, bytes]] = {}
        self._lock = threading.Lock()
        # Resolved values are retained only host-side for leakage refusal over
        # later responses. This is known-value guarding, not covert-channel DLP.
        self._private: set[str] = {item.handle for item in secret_profiles}

    @property
    def authority(self) -> BrokerPermissionAuthorityV1:
        return self._authority

    def available_permissions(self) -> tuple[str, ...]:
        return tuple(self._authority.require_admission().effective_atoms)

    def _provider_rights(self) -> None:
        from histdatacom.broker_plugin_policy.contracts import (
            BrokerPolicyOperation,
        )
        from histdatacom.broker_plugin_policy.scope import (
            require_provider_operation,
        )

        require_provider_operation(self._request, BrokerPolicyOperation.INVOKE)
        require_provider_operation(self._request, BrokerPolicyOperation.CAPTURE)

    def check_public_text(self, text: str) -> None:
        from histdatacom.broker_plugin_security.secrets import (
            BrokerPrivateMaterialGuard,
        )

        BrokerPrivateMaterialGuard(tuple(sorted(self._private))).check(text)

    def _remember_private(self, value: str) -> None:
        from histdatacom.broker_plugin_security.secrets import (
            BrokerPrivateMaterialGuard,
        )

        # Bound both retained values and their encoded search forms before
        # performing an effect. Never grow an unusable, unbounded canary set.
        with self._lock:
            candidate = self._private | {value}
            BrokerPrivateMaterialGuard(tuple(sorted(candidate)))
            self._private = candidate

    def _check_bytes(self, value: bytes) -> None:
        self.check_public_text(value.decode("utf-8", errors="replace"))
        self.check_public_text(base64.b64encode(value).decode("ascii"))

    def request(
        self,
        endpoint_id: str,
        method: str,
        path: str,
        *,
        body: bytes = b"",
        secret_profile: str | None = None,
    ) -> BrokerHostHTTPResponseV1:
        self._provider_rights()
        permission_name(endpoint_id)
        endpoint = next(
            (
                item
                for item in self._authority.manifest.endpoints
                if item.endpoint_id == endpoint_id
            ),
            None,
        )
        if (
            endpoint is None
            or endpoint.provider_id != self._authority.binding.provider_id
        ):
            raise ValueError("undeclared broker endpoint")
        self._authority.require("network:provider:" + endpoint.provider_id)
        if (
            type(method) is not str
            or method not in endpoint.methods
            or type(path) is not str
            or len(path) > 2048
            or not path.startswith(endpoint.path_prefix)
            or not path.isascii()
            or any(char in path for char in ("%", "?", "#", "\\"))
            or "//" in path
            or any(part in (".", "..") for part in path.split("/"))
            or any(ord(char) < 33 or ord(char) == 127 for char in path)
            or type(body) is not bytes
            or len(body) > endpoint.max_request_bytes
        ):
            raise ValueError("broker endpoint request outside declared bounds")
        authorization: str | None = None
        if secret_profile is not None:
            permission_name(secret_profile)
            self._authority.require("secrets:read:" + secret_profile)
            profile = self._profiles.get(secret_profile)
            if (
                secret_profile not in endpoint.secret_profiles
                or profile is None
                or self._provider is None
            ):
                raise ValueError("broker endpoint secret profile unavailable")
            try:
                value = self._provider.resolve(profile.handle)
                if (
                    type(value) is not str
                    or not 1 <= len(value) <= 4096
                    or not value.isascii()
                    or any(ord(char) < 33 or ord(char) == 127 for char in value)
                ):
                    raise ValueError
                self._remember_private(value)
                authorization = ("Bearer " if profile.bearer else "") + value
            except BaseException:  # noqa: BLE001
                # Operator resolvers may raise with private values attached.
                raise ValueError(
                    "broker endpoint secret resolution refused"
                ) from None
        # Recheck after resolving an operator callback, immediately before IO.
        self._authority.require("network:provider:" + endpoint.provider_id)
        if secret_profile is not None:
            self._authority.require("secrets:read:" + secret_profile)
        self._provider_rights()
        # Authentication belongs only in the trusted header for this profile.
        # Independently known private values may not escape through URL/body.
        self.check_public_text(path)
        self._check_bytes(body)
        timeout = endpoint.timeout_ms / 1000
        deadline = _DEADLINE.get()
        if deadline is not None:
            timeout = min(timeout, deadline - time.monotonic())
        if timeout <= 0:
            raise ValueError("broker resource operation deadline expired")
        payload = json.dumps(
            {
                "origin": endpoint.origin,
                "method": method,
                "path": path,
                "body": base64.b64encode(body).decode("ascii"),
                "authorization": authorization,
                "maximum": endpoint.max_response_bytes,
                "timeout": timeout,
            }
        ).encode("ascii")
        try:
            result = subprocess.run(
                [sys.executable, "-I", "-S", "-B", "-c", _HTTP_HELPER],
                input=payload,
                capture_output=True,
                check=True,
                timeout=timeout,
            )
            if (
                result.stderr
                or len(result.stdout) > 2 * endpoint.max_response_bytes + 128
            ):
                raise ValueError
            response = json.loads(result.stdout)
            if type(response) is not dict or set(response) != {
                "status",
                "body",
            }:
                raise ValueError
            output = BrokerHostHTTPResponseV1(
                response["status"],
                base64.b64decode(response["body"], validate=True),
            )
            if len(output.body) > endpoint.max_response_bytes:
                raise ValueError
            self._check_bytes(output.body)
            # An operation may outlive a grant or provider-policy revision.
            # Recheck at response release, not only before blocking transport.
            self._authority.require("network:provider:" + endpoint.provider_id)
            if secret_profile is not None:
                self._authority.require("secrets:read:" + secret_profile)
            self._provider_rights()
            return output
        except BaseException:  # noqa: BLE001
            # Process exceptions retain stdin, which contains authentication.
            raise ValueError("broker host transport refused") from None

    def _cache_descriptor(
        self, cache_id: str, key: str
    ) -> BrokerPermissionCacheV1:
        self._provider_rights()
        permission_name(cache_id)
        self._authority.require("cache:plugin:" + cache_id)
        descriptor = next(
            (
                item
                for item in self._authority.manifest.caches
                if item.cache_id == cache_id
            ),
            None,
        )
        if (
            descriptor is None
            or type(key) is not str
            or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", key) is None
        ):
            raise ValueError("broker cache namespace or key refused")
        return descriptor

    def put_cache(self, cache_id: str, key: str, value: bytes) -> None:
        descriptor = self._cache_descriptor(cache_id, key)
        if type(value) is not bytes or len(value) > descriptor.max_item_bytes:
            raise ValueError("broker cache item bound")
        self._check_bytes(value)
        with self._lock:
            items = self._cache.setdefault(cache_id, {})
            if sum(len(item) for item in items.values()) - len(
                items.get(key, b"")
            ) + len(value) > descriptor.max_bytes or (
                key not in items and len(items) >= descriptor.max_items
            ):
                raise ValueError("broker cache quota exceeded")
            items[key] = value

    def get_cache(self, cache_id: str, key: str) -> bytes | None:
        self._cache_descriptor(cache_id, key)
        with self._lock:
            value = self._cache.get(cache_id, {}).get(key)
        if value is not None:
            self._check_bytes(value)
        return value

    def request_subprocess(self, operation: str) -> NoReturn:
        self._authority.require("subprocess:requested")
        # A grant cannot create an implementation. No arbitrary executable or
        # command-string escape hatch exists until an isolated ABI is qualified.
        raise ValueError("isolated broker subprocess resource unsupported")
