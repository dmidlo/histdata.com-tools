"""Build a minimal standards-shaped external wheel without host edits."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from histdatacom.broker_plugin_registry import (
    BROKER_PLUGIN_ENTRY_POINT_GROUP,
    BrokerPluginRegistrationV1,
    registration_resource_path,
)


def fixture_registration(
    plugin_version: str = "1.0.0",
    distribution_name: str = "histdatacom-fixture-broker",
) -> BrokerPluginRegistrationV1:
    return BrokerPluginRegistrationV1(
        plugin_id="org.example.fixture",
        plugin_version=plugin_version,
        display_name="Offline broker fixture",
        distribution_name=distribution_name,
        distribution_version="1.0.0",
        entry_point="fixture_broker.plugin:factory",
        sdk_min_version="1.0.0",
        sdk_max_version="2.0.0",
        provider_ids=("example",),
        capabilities=("health.v1", "quotes.v1"),
        source_revision="abc1234",
        source_repository="https://example.org/fixture",
        build_id="fixture-1",
    )


def build_fixture_plugin_wheel(
    output: Path,
    registration: BrokerPluginRegistrationV1 | None = None,
) -> Path:
    registration = registration or fixture_registration()
    name = registration.distribution_name.replace("-", "_")
    version = registration.distribution_version
    dist_info = f"{name}-{version}.dist-info"
    module = registration.entry_point.split(":", 1)[0].replace(".", "/")
    package = module.rsplit("/", 1)[0]
    entries = {
        f"{package}/__init__.py": b'"""External fixture package."""\n',
        module
        + ".py": (
            b'"""Offline discovery must not execute this module."""\n'
            b'raise RuntimeError("PLUGIN_IMPORT_CANARY")\n'
            b'def factory():\n    raise RuntimeError("PLUGIN_FACTORY_CANARY")\n'
        ),
        registration_resource_path(
            registration.plugin_id, registration.entry_point
        ): registration.to_json().encode("ascii"),
        f"{dist_info}/METADATA": (
            f"Metadata-Version: 2.1\nName: {registration.distribution_name}\n"
            f"Version: {version}\nRequires-Dist: histdatacom>=2.5.0\n"
        ).encode(),
        f"{dist_info}/WHEEL": b"Wheel-Version: 1.0\nGenerator: histdatacom-fixture\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        f"{dist_info}/entry_points.txt": (
            f"[{BROKER_PLUGIN_ENTRY_POINT_GROUP}]\n{registration.plugin_id} = {registration.entry_point}\n"
        ).encode(),
    }
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for path, data in sorted(entries.items()):
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(data).digest())
            .rstrip(b"=")
            .decode()
        )
        writer.writerow((path, f"sha256={digest}", len(data)))
    writer.writerow((f"{dist_info}/RECORD", "", ""))
    entries[f"{dist_info}/RECORD"] = record.getvalue().encode()
    output.mkdir(parents=True, exist_ok=True)
    wheel = output / f"{name}-{version}-py3-none-any.whl"
    with ZipFile(wheel, "w", compression=ZIP_DEFLATED) as archive:
        for path, data in sorted(entries.items()):
            archive.writestr(path, data)
    return wheel
