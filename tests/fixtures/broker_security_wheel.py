"""Build the offline public-SDK fixture as a genuine distributable wheel."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import socket
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from histdatacom.broker_plugin_permissions import (
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


def build_security_wheel(
    output: Path,
    *,
    direct_url: dict[str, object] | None = None,
    installer: str | None = None,
    misclassify_mode_secret: bool = False,
    block_import: bool = False,
    fail_before_schema: str | None = None,
    endpoint_origin: str | None = None,
) -> Path:
    registration = BrokerPluginRegistrationV1(
        "org.example.security",
        "1.0.0",
        "Offline security fixture",
        "histdatacom-security-fixture",
        "1.0.0",
        "security_fixture.plugin:factory",
        "1.0.0",
        "2.0.0",
        ("offline",),
        tuple(
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
        ),
    )
    dist = "histdatacom_security_fixture-1.0.0.dist-info"
    entries = {
        "security_fixture/__init__.py": b'"""Offline SDK fixture."""\n',
        "security_fixture/plugin.py": Path(__file__)
        .with_name("broker_security_external.py")
        .read_bytes(),
        registration_resource_path(
            registration.plugin_id, registration.entry_point
        ): registration.to_json().encode("ascii"),
        f"{dist}/METADATA": b"Metadata-Version: 2.1\nName: histdatacom-security-fixture\nVersion: 1.0.0\nRequires-Dist: histdatacom>=2.5.0\n",
        f"{dist}/WHEEL": b"Wheel-Version: 1.0\nGenerator: security-fixture\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        f"{dist}/entry_points.txt": f"[{BROKER_PLUGIN_ENTRY_POINT_GROUP}]\n{registration.plugin_id} = {registration.entry_point}\n".encode(),
    }
    # INSTALLER belongs to installed metadata; pip creates it itself. Only
    # extracted-metadata tests request a synthetic installer declaration.
    if installer is not None:
        entries[f"{dist}/INSTALLER"] = (installer + "\n").encode()
    if direct_url is not None:
        entries[f"{dist}/direct_url.json"] = json.dumps(direct_url).encode()
    if misclassify_mode_secret:
        entries["security_fixture/plugin.py"] = entries[
            "security_fixture/plugin.py"
        ].replace(b'"Offline scenario"', b'"Offline scenario", secret=True')
    if block_import:
        entries[
            "security_fixture/plugin.py"
        ] += b"\nimport time\ntime.sleep(60)\n"
    if fail_before_schema == "factory":
        entries[
            "security_fixture/plugin.py"
        ] += b"\ndef factory(resources):\n    raise RuntimeError('closed fixture failure')\n"
    elif fail_before_schema in ("metadata", "schema"):
        constructor = (
            b"BrokerPluginMetadataV1"
            if fail_before_schema == "metadata"
            else b"BrokerConfigurationSchemaV1"
        )
        entries["security_fixture/plugin.py"] = entries[
            "security_fixture/plugin.py"
        ].replace(
            b"return " + constructor + b"(",
            b"raise RuntimeError('closed fixture failure')\n        return "
            + constructor
            + b"(",
        )
    # Generated loopback-only descriptor; qualifiers bind this exact declared
    # port, never manufacture a wider runtime grant. A collision fails loudly.
    if endpoint_origin is None:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            endpoint_origin = f"http://127.0.0.1:{listener.getsockname()[1]}"
    candidate = BrokerPluginCandidateV1(
        registration,
        hashlib.sha256(registration.to_json().encode("ascii")).hexdigest(),
        hashlib.sha256(entries["security_fixture/plugin.py"]).hexdigest(),
    )
    manifest = BrokerPermissionManifestV1(
        candidate.artifact_id,
        registration.distribution_name,
        registration.distribution_version,
        "1.0.0",
        registration.provider_ids,
        ("emit:health", "emit:quotes", "emit:sizes", "raw_payload:emit"),
        ("network:provider:offline", "secrets:read:fixture-login"),
        endpoints=(
            BrokerPermissionEndpointV1(
                "fixture-auth",
                "offline",
                endpoint_origin,
                "/auth/",
                ("POST",),
                0,
                64,
                2000,
                ("fixture-login",),
            ),
        ),
        secret_profiles=("fixture-login",),
    )
    entries[
        permission_resource_path(
            registration.plugin_id, registration.entry_point
        )
    ] = manifest.to_json().encode("ascii")
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for name, data in sorted(entries.items()):
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(data).digest())
            .rstrip(b"=")
            .decode()
        )
        writer.writerow((name, "sha256=" + digest, len(data)))
    writer.writerow((f"{dist}/RECORD", "", ""))
    entries[f"{dist}/RECORD"] = record.getvalue().encode()
    output.mkdir(parents=True, exist_ok=True)
    target = output / "histdatacom_security_fixture-1.0.0-py3-none-any.whl"
    with ZipFile(target, "w", compression=ZIP_DEFLATED) as archive:
        for name, data in sorted(entries.items()):
            archive.writestr(name, data)
    return target
