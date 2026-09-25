"""Parent-only dispatcher, closed operations and exact payload shapes."""

from __future__ import annotations

import base64
from typing import cast

from .decisions import BrokerPermissionError
from ._wire import canonical_json, load_json
from .resources import BrokerPermissionResourcesV1, resource_deadline
from .scope import current_host_resources, current_permission_authority


def dispatch_worker_permission(
    operation: str, payload_json: str, *, deadline: float
) -> str:
    authority = current_permission_authority()
    authority.require_admission()
    value = load_json(payload_json)
    if type(value) is not dict:
        raise BrokerPermissionError("invalid_resource_payload")
    payload = cast(dict[str, object], value)
    if operation == "context" and not payload:
        result: object = authority.read_context().to_json()
    elif operation == "available" and not payload:
        result = authority.require_admission().effective_atoms
    else:
        resources = current_host_resources()
        if (
            type(resources) is not BrokerPermissionResourcesV1
            or resources.authority is not authority
        ):
            raise BrokerPermissionError("exact_parent_host_resources_required")
        with resource_deadline(deadline):
            if operation == "request" and set(payload) == {
                "endpoint_id",
                "method",
                "path",
                "body",
                "secret_profile",
            }:
                if any(
                    type(payload[key]) is not str
                    for key in ("endpoint_id", "method", "path", "body")
                ):
                    raise BrokerPermissionError("invalid_resource_request")
                if (
                    payload["secret_profile"] is not None
                    and type(payload["secret_profile"]) is not str
                ):
                    raise BrokerPermissionError(
                        "invalid_resource_secret_profile"
                    )
                response = resources.request(
                    cast(str, payload["endpoint_id"]),
                    cast(str, payload["method"]),
                    cast(str, payload["path"]),
                    body=base64.b64decode(
                        cast(str, payload["body"]), validate=True
                    ),
                    secret_profile=payload["secret_profile"],
                )
                result = {
                    "status": response.status,
                    "body": base64.b64encode(response.body).decode("ascii"),
                }
            elif operation in ("put_cache", "get_cache") and set(payload) == (
                {"cache_id", "key", "value"}
                if operation == "put_cache"
                else {"cache_id", "key"}
            ):
                if any(type(item) is not str for item in payload.values()):
                    raise BrokerPermissionError(
                        "invalid_resource_cache_request"
                    )
                cache_id, key = cast(str, payload["cache_id"]), cast(
                    str, payload["key"]
                )
                if operation == "put_cache":
                    resources.put_cache(
                        cache_id,
                        key,
                        base64.b64decode(
                            cast(str, payload["value"]), validate=True
                        ),
                    )
                    result = None
                else:
                    data = resources.get_cache(cache_id, key)
                    result = (
                        None
                        if data is None
                        else base64.b64encode(data).decode("ascii")
                    )
            else:
                raise BrokerPermissionError("unknown_resource_operation")
    text = canonical_json(result)
    if authority.manifest.resource_abi == "host_resources_v1":
        host = current_host_resources()
        if type(host) is not BrokerPermissionResourcesV1:
            raise BrokerPermissionError("exact_parent_host_resources_required")
        host.check_public_text(text)
    return text
