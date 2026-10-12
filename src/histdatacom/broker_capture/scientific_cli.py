"""Explicit offline SDK-native science CLI; never provider acquisition.

``inspect`` is metadata-only. ``fit`` and ``verify`` require an operator-supplied
current policy context, re-read at every policy boundary, and explicit source
locators. Retained decisions, compatibility reports and paths grant no rights.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from histdatacom.broker_plugin_policy.contracts import BrokerPolicyContextV1


@dataclass(frozen=True, slots=True)
class _CurrentPolicyFile:
    path: Path

    def read_policy_context(self) -> BrokerPolicyContextV1:
        from histdatacom.broker_plugin_policy.contracts import (
            BrokerPolicyContextV1,
        )
        from histdatacom.broker_plugin_policy.storage import _read

        return BrokerPolicyContextV1.from_json(_read(self.path).decode("ascii"))


def main(argv: Sequence[str] | None = None) -> int:
    from histdatacom.broker_capture import (
        BrokerDeliveryFingerprintV3,
        BrokerLifecycleFingerprintFitConfigV1,
        broker_fingerprint_sources,
        fit_broker_delivery_fingerprint,
        parse_broker_delivery_fingerprint,
        write_broker_delivery_fingerprint,
    )
    from histdatacom.broker_plugin_lifecycle import inspect_broker_lifecycle
    from histdatacom.broker_plugin_policy import (
        BrokerPolicyOperation,
        BrokerSDKInvocationV1,
        provider_policy_scope,
        require_provider_operation,
    )
    from histdatacom.broker_plugin_policy.storage import _read

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    inspect = sub.add_parser(
        "inspect", help="Read canonical metadata; no material admission claim"
    )
    inspect.add_argument("fingerprint", type=Path)
    verify = sub.add_parser(
        "verify",
        help="Reproduce native projection from actual retained sources under current rights",
    )
    verify.add_argument("fingerprint", type=Path)
    verify.add_argument("--source", action="append", type=Path, required=True)
    verify.add_argument("--policy-context", type=Path, required=True)
    fit = sub.add_parser(
        "fit", help="Fit one complete SDK capture without importing its plugin"
    )
    fit.add_argument("directory", type=Path)
    fit.add_argument("--invocation", type=Path, required=True)
    fit.add_argument("--policy-context", type=Path, required=True)
    fit.add_argument("--config", type=Path)
    fit.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    for name in (
        "fingerprint",
        "invocation",
        "policy_context",
        "config",
        "output",
    ):
        value = getattr(args, name, None)
        if value is not None:
            setattr(
                args,
                name,
                (
                    value.absolute()
                    if name == "output"
                    else value.parent.resolve(strict=True) / value.name
                ),
            )
    if args.operation == "inspect":
        value = parse_broker_delivery_fingerprint(
            _read(args.fingerprint).decode("ascii")
        )
        print(
            json.dumps(
                {
                    "schema_version": value.schema_version,
                    "fingerprint_id": value.fingerprint_id,
                    "material_admission": "not_checked",
                },
                sort_keys=True,
            )
        )
        return 0
    with provider_policy_scope(_CurrentPolicyFile(args.policy_context)):
        if args.operation == "verify":
            value = parse_broker_delivery_fingerprint(
                _read(args.fingerprint).decode("ascii")
            )
            if type(value) is not BrokerDeliveryFingerprintV3:
                raise ValueError("SDK scientific verify requires native V3")
            with broker_fingerprint_sources(*args.source):
                require_provider_operation(
                    value, BrokerPolicyOperation.MATERIAL_USE
                )
        else:
            request = BrokerSDKInvocationV1.from_json(
                _read(args.invocation).decode("ascii").removesuffix("\n")
            )
            require_provider_operation(
                request, BrokerPolicyOperation.MATERIAL_USE
            )
            config = (
                None
                if args.config is None
                else BrokerLifecycleFingerprintFitConfigV1.from_json(
                    _read(args.config).decode("ascii").removesuffix("\n")
                )
            )
            with broker_fingerprint_sources(args.directory):
                value = fit_broker_delivery_fingerprint(
                    args.directory,
                    (inspect_broker_lifecycle(args.directory).manifest,),
                    provider_requests=(request,),
                    config=config,
                )
                write_broker_delivery_fingerprint(args.output, value)
        print(
            json.dumps(
                {
                    "schema_version": value.schema_version,
                    "fingerprint_id": value.fingerprint_id,
                    "material_admission": "current_rights_and_actual_native_projection_verified",
                    "empirical_qualification": "not_attested",
                },
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
