"""Separate, no-follow host-health journals and independently replayed audits.

The native V1 capture directory inventory and bytes are not modified. Missing
or interrupted evidence cannot be promoted to a qualified health audit.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
import os
from pathlib import Path
import stat
from typing import TYPE_CHECKING

from .collector import MAX_OBSERVATION_BYTES
from .contracts import (
    BrokerHostHealthAuditV1,
    BrokerHostHealthHeaderV1,
    BrokerHostHealthObservationV1,
)

if TYPE_CHECKING:
    from histdatacom.broker_plugin_permissions import (
        BrokerPermissionExecutionV1,
    )
    from histdatacom.broker_plugin_lifecycle.contracts import (
        BrokerLifecycleManifestV1,
        BrokerLifecycleRecordV1,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1

_METADATA_NAMES = frozenset(
    {
        "invocation.json",
        "native-header.json",
        "permission-manifest.json",
        "permission-context.json",
        "permission-decision.json",
        "provider-decision.json",
    }
)
_MAX_METADATA_BYTES = 8 * 1024 * 1024


def _sync(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _read(path: Path, maximum: int = _MAX_METADATA_BYTES) -> str:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
            raise ValueError("host health file shape or byte bound")
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise ValueError("host health file byte bound")
    return data.decode("ascii")


class HostHealthEvidenceWriter:
    def __init__(
        self,
        directory: Path,
        header: BrokerHostHealthHeaderV1,
        metadata: Mapping[str, str],
        *,
        authorize_artifact: Callable[[object], None],
        guard_text: Callable[[str], None] = lambda _: None,
    ) -> None:
        if (
            set(metadata) - _METADATA_NAMES
            or "provider-decision.json" not in metadata
        ):
            raise ValueError("unknown host health metadata inventory")
        self.header = BrokerHostHealthHeaderV1.from_json(header.to_json())
        self.authorize_artifact = authorize_artifact
        self.guard_text = guard_text
        self._bytes = 0
        self._count = 0
        self._fd: int | None = None
        self.authorize_artifact(self.header)
        # Resolve the caller-controlled parent once, never the new leaf.
        self.directory = directory.parent.resolve(strict=True) / directory.name
        self.directory.mkdir(mode=0o700)
        _sync(self.directory.parent)
        self._write("header.json", self.header.to_json())
        for name, value in sorted(metadata.items()):
            self._write(name, value)
        self._fd = os.open(
            self.directory / "observations.jsonl",
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW,
            0o600,
        )
        _sync(self.directory)

    def _write(self, name: str, text: str) -> None:
        if type(text) is not str or len(text) > _MAX_METADATA_BYTES:
            raise ValueError("host health metadata bound")
        self.guard_text(text)
        # A trusted guard may observe a changed ledger. Authorize the actual
        # invocation envelope again immediately before publishing metadata.
        self.authorize_artifact(self.header)
        encoded = text.encode("ascii")
        fd = os.open(
            self.directory / name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
        )
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        _sync(self.directory)

    def append(self, observation: BrokerHostHealthObservationV1) -> None:
        if (
            self._fd is None
            or type(observation) is not BrokerHostHealthObservationV1
            or observation.sequence != self._count
            or self._count >= self.header.policy.max_observations
        ):
            raise ValueError("host health append sequence or bound")
        self.authorize_artifact(observation)
        text = observation.to_json()
        self.guard_text(text)
        self.authorize_artifact(observation)
        data = memoryview((text + "\n").encode("ascii"))
        if self._bytes + len(data) > MAX_OBSERVATION_BYTES:
            raise ValueError("host health observation byte bound")
        size = len(data)
        try:
            while data:
                written = os.write(self._fd, data)
                if written <= 0:
                    raise OSError("host health write failed")
                data = data[written:]
            os.fsync(self._fd)
        except BaseException:
            self.close()
            raise
        self._bytes += size
        self._count += 1

    def finish(
        self,
        audit: BrokerHostHealthAuditV1,
        *,
        permission_execution: BrokerPermissionExecutionV1 | None = None,
    ) -> None:
        if (
            audit.header != self.header
            or audit.observation_count != self._count
        ):
            raise ValueError("host health final inventory mismatch")
        self.authorize_artifact(audit)
        self.close()
        if permission_execution is not None:
            self._write(
                "permission-execution.json", permission_execution.to_json()
            )
        self._write("audit.json", audit.to_json())

    def close(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)


def _observations(
    directory: Path, maximum: int
) -> Iterator[BrokerHostHealthObservationV1]:
    fd = os.open(
        directory / "observations.jsonl",
        os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
    )
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_size > MAX_OBSERVATION_BYTES
        ):
            raise ValueError("host health observation file bound")
        count = 0
        while data := stream.readline(8193):
            if count >= maximum or len(data) > 8192 or not data.endswith(b"\n"):
                raise ValueError(
                    "host health observation line bound or interrupted tail"
                )
            yield BrokerHostHealthObservationV1.from_json(
                data[:-1].decode("ascii")
            )
            count += 1


def read_lifecycle_host_health(
    directory: Path,
    manifest: BrokerLifecycleManifestV1,
    records: Iterable[BrokerLifecycleRecordV1],
    invocation: BrokerSDKInvocationV1,
) -> BrokerHostHealthAuditV1:
    """Fresh provider rights and exact replay, not a structural JSON claim."""
    from histdatacom.broker_plugin_permissions.contracts import (
        BrokerPermissionManifestV1,
        BrokerPermissionContextV1,
        BrokerPermissionDecisionV1,
    )
    from histdatacom.broker_plugin_permissions import (
        BrokerPermissionExecutionV1,
        verify_permission_execution,
    )
    from histdatacom.broker_plugin_policy.bindings import BrokerSDKInvocationV1
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyDecisionV1,
        BrokerPolicyOperation,
    )
    from histdatacom.broker_plugin_policy.health_bindings import (
        BrokerHostHealthEvidenceV1,
    )
    from histdatacom.broker_plugin_policy.scope import (
        require_provider_operation,
    )
    from .replay import replay_lifecycle_host_health

    # Reject revoked source use before reading provider-derived evidence.
    require_provider_operation(invocation, BrokerPolicyOperation.MATERIAL_USE)
    if directory.is_symlink():
        raise ValueError("host health directory may not be a symlink")
    directory = directory.resolve(strict=True)
    expected = _METADATA_NAMES | {
        "header.json",
        "observations.jsonl",
        "audit.json",
        "permission-execution.json",
    }
    names: set[str] = set()
    with os.scandir(directory) as entries:
        for entry in entries:
            if len(names) >= len(expected) or not entry.is_file(
                follow_symlinks=False
            ):
                raise ValueError("host health foreign or interrupted inventory")
            names.add(entry.name)
    if names != expected:
        raise ValueError("host health evidence incomplete")
    header = BrokerHostHealthHeaderV1.from_json(
        _read(directory / "header.json")
    )
    require_provider_operation(
        BrokerHostHealthEvidenceV1(invocation, manifest.header, header, header),
        BrokerPolicyOperation.MATERIAL_USE,
    )
    stored_invocation = BrokerSDKInvocationV1.from_json(
        _read(directory / "invocation.json")
    )
    if (
        stored_invocation.to_json() != invocation.to_json()
        or _read(directory / "native-header.json") != manifest.header.to_json()
    ):
        raise ValueError("host health native binding substitution")
    actual = replay_lifecycle_host_health(
        header,
        _observations(directory, header.policy.max_observations),
        manifest,
        records,
        invocation,
        BrokerPermissionManifestV1.from_json(
            _read(directory / "permission-manifest.json")
        ),
        BrokerPermissionContextV1.from_json(
            _read(directory / "permission-context.json")
        ),
        BrokerPermissionDecisionV1.from_json(
            _read(directory / "permission-decision.json")
        ),
        BrokerPolicyDecisionV1.from_json(
            _read(directory / "provider-decision.json")
        ),
    )
    claimed = BrokerHostHealthAuditV1.from_json(_read(directory / "audit.json"))
    if claimed.to_json() != actual.to_json():
        raise ValueError("host health audit differs from independent replay")
    verify_permission_execution(
        BrokerPermissionExecutionV1.from_json(
            _read(directory / "permission-execution.json")
        ),
        invocation,
        manifest,
    )
    # Replay advances caller-provided iterables and may span a ledger update.
    # Historical decisions verify provenance; they cannot authorize release.
    require_provider_operation(invocation, BrokerPolicyOperation.MATERIAL_USE)
    require_provider_operation(
        BrokerHostHealthEvidenceV1(invocation, manifest.header, header, actual),
        BrokerPolicyOperation.MATERIAL_USE,
    )
    return actual
