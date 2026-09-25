"""Build a separately installed generated permission adversary offline."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
from dataclasses import replace
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from histdatacom.broker_plugin_permissions import (
    BrokerPermissionCacheV1,
    BrokerPermissionEndpointV1,
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


def permission_fixture_schema() -> BrokerConfigurationSchemaV1:
    """Independent host-owned review schema, never obtained by activation."""
    return BrokerConfigurationSchemaV1(
        (
            BrokerConfigurationFieldV1(
                "mode", BrokerConfigurationType.STRING, "Generated scenario"
            ),
            BrokerConfigurationFieldV1(
                "target",
                BrokerConfigurationType.STRING,
                "Generated protected target",
                required=False,
            ),
            BrokerConfigurationFieldV1(
                "port",
                BrokerConfigurationType.INTEGER,
                "Generated localhost port",
                required=False,
            ),
        )
    )


def permission_registration() -> BrokerPluginRegistrationV1:
    return BrokerPluginRegistrationV1(
        "org.example.permissions",
        "1.0.0",
        "Generated permission fixture",
        "histdatacom-permission-fixture",
        "1.0.0",
        "permission_fixture.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("fixture",),
        tuple(
            sorted(
                (
                    "session.v1",
                    "instruments.v1",
                    "instruments.price-increment.v1",
                    "events.v1",
                    "quotes.v1",
                    "timestamps.receive.v1",
                    "health.v1",
                    "sizes.quoted.v1",
                    "raw-hashes.v1",
                )
            )
        ),
    )


def build_permission_wheel(
    output: Path,
    *,
    port: int = 1,
    required_atoms: tuple[str, ...] = ("emit:health", "emit:quotes"),
    include_subprocess: bool = False,
    block_import: bool = False,
    implementation_path: Path | None = None,
    extra_capabilities: tuple[str, ...] = (),
) -> Path:
    registration = permission_registration()
    if extra_capabilities:
        registration = replace(
            registration,
            capabilities=tuple(
                sorted(set(registration.capabilities + extra_capabilities))
            ),
        )
    registration_bytes = registration.to_json().encode("ascii")
    implementation = (
        implementation_path
        or Path(__file__).with_name("broker_permission_external.py")
    ).read_bytes()
    if block_import:
        implementation += b"\nraise RuntimeError('generated plugin import must not happen during offline inspection')\n"
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(registration_bytes).hexdigest(),
        hashlib.sha256(implementation).hexdigest(),
    )
    declared = {
        "emit:health",
        "emit:quotes",
        "emit:sizes",
        "raw_payload:emit",
        "network:provider:fixture",
        "secrets:read:paper",
        "cache:plugin:prices",
    }
    if include_subprocess:
        declared.add("subprocess:requested")
    if not set(required_atoms) <= declared:
        raise ValueError("unknown generated required permission")
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        registration.distribution_name,
        registration.distribution_version,
        "1.0.0",
        registration.provider_ids,
        required_atoms,
        tuple(sorted(declared - set(required_atoms))),
        endpoints=(
            BrokerPermissionEndpointV1(
                "quotes",
                "fixture",
                f"http://127.0.0.1:{port}",
                "/v1/",
                ("GET", "POST"),
                64,
                64,
                1000,
                ("paper",),
            ),
        ),
        secret_profiles=("paper",),
        caches=(BrokerPermissionCacheV1("prices", 128, 2, 64),),
        subprocess_mode="isolated" if include_subprocess else "none",
    )
    dist = "histdatacom_permission_fixture-1.0.0.dist-info"
    entries = {
        "permission_fixture/__init__.py": b'"""Generated offline plugin."""\n',
        "permission_fixture/plugin.py": implementation,
        registration_resource_path(
            registration.plugin_id, registration.entry_point
        ): registration_bytes,
        permission_resource_path(
            registration.plugin_id, registration.entry_point
        ): manifest.to_json().encode("ascii"),
        f"{dist}/METADATA": b"Metadata-Version: 2.1\nName: histdatacom-permission-fixture\nVersion: 1.0.0\nRequires-Dist: histdatacom>=2.5.0\n",
        f"{dist}/WHEEL": b"Wheel-Version: 1.0\nGenerator: permission-fixture\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        f"{dist}/entry_points.txt": f"[{BROKER_PLUGIN_ENTRY_POINT_GROUP}]\n{registration.plugin_id} = {registration.entry_point}\n".encode(),
    }
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for path, data in sorted(entries.items()):
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(data).digest())
            .rstrip(b"=")
            .decode()
        )
        writer.writerow((path, "sha256=" + digest, len(data)))
    writer.writerow((f"{dist}/RECORD", "", ""))
    entries[f"{dist}/RECORD"] = record.getvalue().encode("ascii")
    output.mkdir(parents=True, exist_ok=True)
    target = output / "histdatacom_permission_fixture-1.0.0-py3-none-any.whl"
    with ZipFile(target, "w", compression=ZIP_DEFLATED) as archive:
        for path, data in sorted(entries.items()):
            archive.writestr(path, data)
    return target
