"""Build a self-contained offline external lifecycle conformance wheel."""

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
            "connection.v1",
            "health.v1",
        )
    )
)


def build_lifecycle_wheel(output: Path, *, block_import: bool = False) -> Path:
    registration = BrokerPluginRegistrationV1(
        "org.example.lifecycle",
        "1.0.0",
        "Offline lifecycle fixture",
        "histdatacom-lifecycle-fixture",
        "1.0.0",
        "lifecycle_fixture.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("offline",),
        CAPABILITIES,
    )
    dist = "histdatacom_lifecycle_fixture-1.0.0.dist-info"
    source = (
        Path(__file__).with_name("broker_lifecycle_external.py").read_bytes()
    )
    if block_import:
        source += b"\nimport time\ntime.sleep(60)\n"
    entries = {
        "lifecycle_fixture/__init__.py": b'"""Offline fixture."""\n',
        "lifecycle_fixture/plugin.py": source,
        registration_resource_path(
            registration.plugin_id, registration.entry_point
        ): registration.to_json().encode("ascii"),
        f"{dist}/METADATA": b"Metadata-Version: 2.1\nName: histdatacom-lifecycle-fixture\nVersion: 1.0.0\nRequires-Dist: histdatacom>=2.5.0\n",
        f"{dist}/WHEEL": b"Wheel-Version: 1.0\nGenerator: lifecycle-fixture\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        f"{dist}/entry_points.txt": f"[{BROKER_PLUGIN_ENTRY_POINT_GROUP}]\n{registration.plugin_id} = {registration.entry_point}\n".encode(),
    }
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(registration.to_json().encode("ascii")).hexdigest(),
        hashlib.sha256(source).hexdigest(),
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
    for name, content in sorted(entries.items()):
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(content).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        writer.writerow((name, "sha256=" + digest, len(content)))
    writer.writerow((f"{dist}/RECORD", "", ""))
    entries[f"{dist}/RECORD"] = record.getvalue().encode("ascii")
    output.mkdir(parents=True, exist_ok=True)
    target = output / "histdatacom_lifecycle_fixture-1.0.0-py3-none-any.whl"
    with ZipFile(target, "w", compression=ZIP_DEFLATED) as archive:
        for name, content in sorted(entries.items()):
            archive.writestr(name, content)
    return target
