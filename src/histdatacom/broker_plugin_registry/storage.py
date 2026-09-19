"""Exact bytes and immutable local persistence for software inventory."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat
import tempfile
from typing import TypeVar

from histdatacom.runtime_contracts import ArtifactRef

from .bindings import BrokerPluginExperimentInventoryV1
from .contracts import (
    MAX_INVENTORY_BYTES,
    BrokerPluginInventoryV1,
    BrokerPluginRegistryError,
    BrokerPluginRegistryReason,
)

_Persisted = TypeVar(
    "_Persisted", BrokerPluginInventoryV1, BrokerPluginExperimentInventoryV1
)


def _read_regular_bytes(path: Path, limit: int) -> bytes:
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.RESOURCE_INTEGRITY
        )
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or (
            before.st_dev,
            before.st_ino,
        ) != (opened.st_dev, opened.st_ino):
            raise BrokerPluginRegistryError(
                BrokerPluginRegistryReason.RESOURCE_INTEGRITY
            )
        return stream.read(limit + 1)


def write_plugin_inventory(
    inventory: BrokerPluginInventoryV1 | BrokerPluginExperimentInventoryV1,
    path: str | Path,
) -> ArtifactRef:
    """Create once; replay identical bytes, never replace differing evidence."""
    if type(inventory) not in (
        BrokerPluginInventoryV1,
        BrokerPluginExperimentInventoryV1,
    ):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.INVALID_INVENTORY
        )
    encoded = inventory.to_json().encode("ascii")
    temporary: Path | None = None
    try:
        output = Path(path).absolute()
        output.parent.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(
            prefix=".plugin-inventory-", dir=output.parent
        )
        temporary = Path(name)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, output)
        except FileExistsError:
            if _read_regular_bytes(output, MAX_INVENTORY_BYTES) != encoded:
                raise BrokerPluginRegistryError(
                    BrokerPluginRegistryReason.PERSISTENCE_FAILURE
                )
        return ArtifactRef(
            kind=inventory.schema_version,
            path=str(output),
            size_bytes=len(encoded),
            sha256=hashlib.sha256(encoded).hexdigest(),
            metadata={"artifact_id": inventory.artifact_id},
        )
    except (OSError, ValueError, TypeError):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.PERSISTENCE_FAILURE
        ) from None
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def read_plugin_inventory(
    reference: ArtifactRef,
    *,
    artifact_type: type[_Persisted] = BrokerPluginInventoryV1,  # type: ignore[assignment]
) -> _Persisted:
    """Verify exact file bytes, reference metadata, and complete canonical IDs."""
    try:
        if type(reference) is not ArtifactRef or artifact_type not in (
            BrokerPluginInventoryV1,
            BrokerPluginExperimentInventoryV1,
        ):
            raise ValueError
        data = _read_regular_bytes(Path(reference.path), MAX_INVENTORY_BYTES)
        if (
            len(data) > MAX_INVENTORY_BYTES
            or type(reference.size_bytes) is not int
            or len(data) != reference.size_bytes
            or hashlib.sha256(data).hexdigest() != reference.sha256
        ):
            raise ValueError
        result = artifact_type.from_json(data.decode("ascii"))
        if (
            result.to_json().encode("ascii") != data
            or reference.kind != result.schema_version
            or reference.metadata != {"artifact_id": result.artifact_id}
        ):
            raise ValueError
        return result
    except (OSError, ValueError, TypeError, KeyError):
        raise BrokerPluginRegistryError(
            BrokerPluginRegistryReason.RESOURCE_INTEGRITY
        ) from None
