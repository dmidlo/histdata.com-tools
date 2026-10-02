"""Closed native bindings shared by host capture and independent replay.

These helpers never activate a plugin, read credentials, or confer permission.
The environment description intentionally excludes paths, host names and ambient
environment variables. It describes the recording runtime, not a full SBOM.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
import sys
from typing import Any, cast


def canonical_native_json(value: object) -> str:
    text = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(text) > 8 * 1024 * 1024:
        raise ValueError("native provenance JSON exceeds byte bound")
    return text


def native_sha256(text: str) -> str:
    if type(text) is not str or not text.isascii():
        raise ValueError("native provenance requires canonical ASCII bytes")
    return hashlib.sha256(text.encode("ascii")).hexdigest()


def parse_native_json(text: str) -> dict[str, Any]:
    if type(text) is not str or len(text) > 8 * 1024 * 1024:
        raise ValueError("native provenance JSON exceeds byte bound")

    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate native provenance field")
            result[key] = value
        return result

    value = json.loads(text, object_pairs_hook=pairs)
    if type(value) is not dict or canonical_native_json(value) != text:
        raise ValueError("noncanonical native provenance JSON")
    return value


def host_environment_json() -> str:
    """Bound public runtime facts only; never inspect host/user environment."""
    return canonical_native_json(
        {
            "schema_version": "histdatacom.broker-provenance-environment.v1",
            "host_distribution": "histdatacom",
            "host_version": importlib.metadata.version("histdatacom"),
            "python_implementation": platform.python_implementation(),
            "python_version": platform.python_version(),
            "python_cache_tag": sys.implementation.cache_tag,
            "python_compiler": platform.python_compiler(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "byteorder": sys.byteorder,
            "scope": "public_recording_runtime_not_full_dependency_sbom",
        }
    )


def legacy_invocation_json(request: object) -> str:
    from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerLegacyCaptureV1,
        BrokerProviderOutputContractV1,
    )

    if (
        type(request) is not BrokerLegacyCaptureV1
        or type(request.session) is not BrokerCaptureSessionV1
        or type(request.output_contract) is not BrokerProviderOutputContractV1
    ):
        raise ValueError("exact native legacy invocation required")
    return canonical_native_json(
        {
            "schema_version": "histdatacom.broker-provider-legacy-invocation.v1",
            "session": request.session.to_dict(),
            "output_contract": request.output_contract.to_dict(),
        }
    )


def ingress_json(
    payload: object,
    *,
    family: str,
    ingress_sequence: int,
    delivery_sequence: int | None,
) -> str:
    """Retain admitted canonical input, never malformed or forbidden raw bytes."""
    if (
        type(ingress_sequence) is not int
        or not 0 <= ingress_sequence < 2**63
        or (
            delivery_sequence is not None
            and (
                type(delivery_sequence) is not int
                or not 0 <= delivery_sequence < 2**63
            )
        )
    ):
        raise ValueError("invalid native provenance ingress sequence")
    if family == "lifecycle_v1":
        from histdatacom.broker_plugin_capabilities import BrokerAdmittedEventV1

        if type(payload) is not BrokerAdmittedEventV1:
            raise ValueError("exact admitted SDK event required")
        restored: Any = cast(Any, BrokerAdmittedEventV1).from_json(
            cast(Any, payload).to_json()
        )
    elif family == "legacy_capture_v1":
        from histdatacom.broker_capture.contracts import BrokerAdapterMessageV1

        if type(payload) is not BrokerAdapterMessageV1:
            raise ValueError("exact admitted legacy message required")
        restored = BrokerAdapterMessageV1.from_json(
            cast(Any, payload).to_json()
        )
    else:
        raise ValueError("unsupported provenance ingress family")
    return canonical_native_json(
        {
            "schema_version": "histdatacom.broker-provenance-ingress.v1",
            "family": family,
            "ingress_sequence": ingress_sequence,
            "delivery_sequence": delivery_sequence,
            "payload": json.loads(restored.to_json()),
        }
    )


def parse_ingress(text: str, *, family: str) -> tuple[object, int, int | None]:
    data = parse_native_json(text)
    if set(data) != {
        "schema_version",
        "family",
        "ingress_sequence",
        "delivery_sequence",
        "payload",
    } or (
        data["schema_version"] != "histdatacom.broker-provenance-ingress.v1"
        or data["family"] != family
        or type(data["payload"]) is not dict
    ):
        raise ValueError("invalid native provenance ingress envelope")
    payload_json = canonical_native_json(data["payload"])
    payload: object
    if family == "lifecycle_v1":
        from histdatacom.broker_plugin_capabilities import BrokerAdmittedEventV1

        payload = BrokerAdmittedEventV1.from_json(payload_json)
    elif family == "legacy_capture_v1":
        from histdatacom.broker_capture.contracts import BrokerAdapterMessageV1

        payload = BrokerAdapterMessageV1.from_json(payload_json)
    else:
        raise ValueError("unsupported provenance ingress family")
    rebuilt = ingress_json(
        payload,
        family=family,
        ingress_sequence=data["ingress_sequence"],
        delivery_sequence=data["delivery_sequence"],
    )
    if rebuilt != text:
        raise ValueError("native ingress does not round-trip exactly")
    return payload, data["ingress_sequence"], data["delivery_sequence"]
