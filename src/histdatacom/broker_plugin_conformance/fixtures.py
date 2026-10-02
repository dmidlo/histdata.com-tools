"""Deterministic separately installable generated wheels, never host edits."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile, ZipInfo

from histdatacom.broker_plugin_permissions import (
    BrokerPermissionManifestV1,
    permission_resource_path,
)
from histdatacom.broker_plugin_registry import (
    BROKER_PLUGIN_ENTRY_POINT_GROUP,
    BrokerPluginCandidateV1,
    BrokerPluginRegistrationV1,
    registration_resource_path,
)
from histdatacom.broker_plugins import (
    BrokerConfigurationFieldV1,
    BrokerConfigurationSchemaV1,
    BrokerConfigurationType,
)

from ._wire import canonical_conformance_json
from .catalog import broker_conformance_catalog
from .contracts import BrokerConformanceDriverV1, BrokerConformanceScenarioV1

FIXTURE_FAULTS = (
    "none",
    "fabricated-optional",
    "missing-capability",
    "overprivilege",
    "secret-leak",
    "reconnect-duplication",
    "false-health",
    "malformed-event",
    "nondeterministic-replay",
    "reused-session-nonce",
    "reused-session-nonce-changing-time",
)


@dataclass(frozen=True, slots=True)
class BrokerConformanceFixtureV1:
    wheel: Path
    candidate: BrokerPluginCandidateV1
    driver: BrokerConformanceDriverV1
    fault: str


def build_broker_conformance_fixture(
    output: Path, *, fault: str = "none"
) -> BrokerConformanceFixtureV1:
    """Build only generated fixture code, with fixed ZIP metadata and RECORD."""
    if fault not in FIXTURE_FAULTS:
        raise ValueError("unknown generated conformance fault")
    capabilities = {
        "session.v1",
        "events.v1",
        "instruments.v1",
        "instruments.price-increment.v1",
        "quotes.v1",
        "health.v1",
        "timestamps.broker-event.v1",
        "timestamps.receive.v1",
        "gaps.v1",
        "heartbeat.v1",
        "connection.v1",
        "sizes.quoted.v1",
        "raw-hashes.v1",
    }
    if fault == "fabricated-optional":
        capabilities.remove("sizes.quoted.v1")
    if fault == "missing-capability":
        capabilities.remove("quotes.v1")
    registration = BrokerPluginRegistrationV1(
        "org.histdatacom.conformance-fixture",
        "1.0.0",
        "Generated conformance fixture",
        "histdatacom-conformance-fixture",
        "1.0.0",
        "conformance_fixture.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("generated",),
        tuple(sorted(capabilities)),
    )
    source = Path(__file__).with_name("fixture_plugin.py").read_bytes()
    source += ("\nFAULT = " + repr(fault) + "\n").encode("ascii")
    registration_bytes = registration.to_json().encode("ascii")
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(registration_bytes).hexdigest(),
        hashlib.sha256(source).hexdigest(),
    )
    required = ["emit:health", "emit:quotes"]
    if fault == "overprivilege":
        required.append("subprocess:requested")
    permissions = BrokerPermissionManifestV1(
        candidate.artifact_id,
        registration.distribution_name,
        "1.0.0",
        "1.0.0",
        ("generated",),
        tuple(sorted(required)),
        ("emit:sizes", "raw_payload:emit"),
        resource_abi="host_resources_v1",
        subprocess_mode="isolated" if fault == "overprivilege" else "none",
    )
    schema = BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "mode",
                BrokerConfigurationType.STRING,
                "Canonical generated scenario",
            ),
        )
    )
    driver = BrokerConformanceDriverV1(
        candidate.artifact_id,
        permissions.artifact_id,
        "generated",
        schema.to_json(),
        ("EURUSD",),
        tuple(
            BrokerConformanceScenarioV1(
                scenario, canonical_conformance_json({"mode": scenario})
            )
            for scenario in sorted(
                {
                    case.scenario_id
                    for case in broker_conformance_catalog().cases
                }
            )
        ),
    )
    dist = "histdatacom_conformance_fixture-1.0.0.dist-info"
    entries = {
        "conformance_fixture/__init__.py": b'"""Generated external broker fixture."""\n',
        "conformance_fixture/plugin.py": source,
        registration_resource_path(
            registration.plugin_id, registration.entry_point
        ): registration_bytes,
        permission_resource_path(
            registration.plugin_id, registration.entry_point
        ): permissions.to_json().encode("ascii"),
        "conformance_fixture/driver.json": driver.to_json().encode("ascii"),
        f"{dist}/METADATA": b"Metadata-Version: 2.1\nName: histdatacom-conformance-fixture\nVersion: 1.0.0\nRequires-Dist: histdatacom\n",
        f"{dist}/WHEEL": b"Wheel-Version: 1.0\nGenerator: histdatacom-conformance\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        f"{dist}/entry_points.txt": f"[{BROKER_PLUGIN_ENTRY_POINT_GROUP}]\n{registration.plugin_id} = {registration.entry_point}\n".encode(
            "ascii"
        ),
    }
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    for name, data in sorted(entries.items()):
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(data).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        writer.writerow((name, "sha256=" + digest, len(data)))
    writer.writerow((f"{dist}/RECORD", "", ""))
    entries[f"{dist}/RECORD"] = stream.getvalue().encode("ascii")
    output.mkdir(parents=True, exist_ok=True)
    wheel = output / "histdatacom_conformance_fixture-1.0.0-py3-none-any.whl"
    with (
        wheel.open("xb") as destination,
        ZipFile(destination, "w", compression=ZIP_STORED) as archive,
    ):
        for name, data in sorted(entries.items()):
            info = ZipInfo(name, (2020, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data)
    return BrokerConformanceFixtureV1(wheel, candidate, driver, fault)
