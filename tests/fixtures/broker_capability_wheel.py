"""Build an offline executable third-party wheel for capability acceptance."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

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

CAPABILITIES = tuple(
    sorted(
        (
            "session.v1",
            "instruments.v1",
            "instruments.price-increment.v1",
            "events.v1",
            "quotes.v1",
            "timestamps.receive.v1",
        )
    )
)


def capability_registration(
    capabilities: tuple[str, ...] = CAPABILITIES,
) -> BrokerPluginRegistrationV1:
    return BrokerPluginRegistrationV1(
        "org.example.capabilities",
        "1.0.0",
        "Offline capability fixture",
        "histdatacom-capability-fixture",
        "1.0.0",
        "capability_fixture.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("offline",),
        capabilities,
    )


def build_capability_wheel(
    output: Path, *, capabilities: tuple[str, ...] = CAPABILITIES
) -> Path:
    registration = capability_registration(capabilities)
    dist = "histdatacom_capability_fixture-1.0.0.dist-info"
    entries = {
        "capability_fixture/__init__.py": b'"""Offline external plugin package."""\n',
        "capability_fixture/plugin.py": Path(__file__)
        .with_name("broker_capability_external.py")
        .read_bytes(),
        registration_resource_path(
            registration.plugin_id, registration.entry_point
        ): registration.to_json().encode("ascii"),
        f"{dist}/METADATA": b"Metadata-Version: 2.1\nName: histdatacom-capability-fixture\nVersion: 1.0.0\nRequires-Dist: histdatacom>=2.5.0\n",
        f"{dist}/WHEEL": b"Wheel-Version: 1.0\nGenerator: capability-fixture\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        f"{dist}/entry_points.txt": f"[{BROKER_PLUGIN_ENTRY_POINT_GROUP}]\n{registration.plugin_id} = {registration.entry_point}\n".encode(),
    }
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(registration.to_json().encode("ascii")).hexdigest(),
        hashlib.sha256(entries["capability_fixture/plugin.py"]).hexdigest(),
    )
    permissions = BrokerPermissionManifestV1(
        candidate.artifact_id,
        registration.distribution_name,
        registration.distribution_version,
        "1.0.0",
        registration.provider_ids,
        ("emit:health", "emit:quotes", "emit:sizes", "raw_payload:emit"),
        resource_abi="none",
    )
    entries[
        permission_resource_path(
            registration.plugin_id, registration.entry_point
        )
    ] = permissions.to_json().encode("ascii")
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for path, content in sorted(entries.items()):
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(content).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        writer.writerow((path, "sha256=" + digest, len(content)))
    writer.writerow((f"{dist}/RECORD", "", ""))
    entries[f"{dist}/RECORD"] = record.getvalue().encode("ascii")
    output.mkdir(parents=True, exist_ok=True)
    target = output / "histdatacom_capability_fixture-1.0.0-py3-none-any.whl"
    with ZipFile(target, "w", compression=ZIP_DEFLATED) as archive:
        for path, content in sorted(entries.items()):
            archive.writestr(path, content)
    return target
