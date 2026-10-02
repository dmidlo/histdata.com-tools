"""Generated installed-fixture gate observation, not product certification.

The two scoped wrappers call the actual original validators unchanged. They
record only an existing plan identity and closed field/capability/reason names,
never a rejected event, event hash, quote value or plugin exception text.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from unittest.mock import patch

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityError,
    BrokerCapabilityReason,
    execution,
    validation,
)
from histdatacom.broker_plugin_conformance import BrokerConformancePlanV1
from histdatacom.broker_plugin_conformance.execution import (
    execute_conformance_case,
)
from histdatacom.broker_plugin_conformance.storage import write_artifact


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("plan", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    plan = BrokerConformancePlanV1.from_json(args.plan.read_text("ascii"))
    observed: list[dict[str, str]] = []
    active: list[str] = []
    original_field = validation._field
    original_event = execution.validate_broker_capability_event

    def event_gate(native_plan, event):
        active.append(native_plan.artifact_id)
        try:
            return original_event(native_plan, event)
        finally:
            active.pop()

    def field_gate(
        native_plan, field, actual, alternatives, *, applicable=True
    ):
        try:
            return original_field(
                native_plan, field, actual, alternatives, applicable=applicable
            )
        except BrokerCapabilityError as error:
            if (
                active == [native_plan.artifact_id]
                and not observed
                and error.reason is BrokerCapabilityReason.CAPABILITY_VIOLATION
                and field == "bid_size"
                and actual == "sizes.quoted.v1"
                and actual
                not in native_plan.candidate.registration.capabilities
                and actual not in native_plan.required
                and actual not in native_plan.enabled_optional
            ):
                observed.append(
                    {
                        "plan_id": native_plan.artifact_id,
                        "gate": "undeclared_quoted_size",
                        "field": "bid_size",
                        "capability": "sizes.quoted.v1",
                        "reason": "capability_violation",
                    }
                )
            raise

    with (
        patch.object(validation, "_field", field_gate),
        patch.object(execution, "validate_broker_capability_event", event_gate),
    ):
        evidence = execute_conformance_case(
            plan, "lifecycle.finite", args.output
        )
    assert validation._field is original_field
    assert execution.validate_broker_capability_event is original_event
    write_artifact(args.output / "evidence.json", evidence)
    print(
        json.dumps(
            {"instrumented_noncertifying_observations": observed},
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
