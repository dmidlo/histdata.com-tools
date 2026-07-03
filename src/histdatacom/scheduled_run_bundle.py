"""Versioned scheduled-run bundle contract."""

from __future__ import annotations

from dataclasses import dataclass, replace
import json
from typing import Any, Mapping

from histdatacom.runtime_contracts import JSONValue, RunRequest

SCHEDULED_RUN_BUNDLE_KIND = "histdatacom.scheduled-run-bundle"
SCHEDULED_RUN_BUNDLE_SCHEMA_VERSION = "histdatacom.scheduled-run-bundle.v1"

_SCHEDULE_METADATA_KEYS = {"no_overlap", "schedule_key"}


class ScheduledRunBundleError(ValueError):
    """Raised when a scheduled-run bundle payload is malformed."""


@dataclass(frozen=True, slots=True)
class ScheduledRunSchedule:
    """Schedule metadata carried next to a raw RunRequest."""

    no_overlap: bool = False
    schedule_key: str = ""

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "no_overlap": self.no_overlap,
            "schedule_key": self.schedule_key,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScheduledRunSchedule":
        """Create schedule metadata from JSON-compatible data."""
        no_overlap = data.get("no_overlap", False)
        if not isinstance(no_overlap, bool):
            raise ScheduledRunBundleError(
                "scheduled-run bundle schedule.no_overlap must be a boolean"
            )
        schedule_key = data.get("schedule_key", "")
        if not isinstance(schedule_key, str):
            raise ScheduledRunBundleError(
                "scheduled-run bundle schedule.schedule_key must be a string"
            )
        return cls(
            no_overlap=no_overlap,
            schedule_key=schedule_key.strip(),
        )


@dataclass(frozen=True, slots=True)
class ScheduledRunBundle:
    """A raw RunRequest plus scheduling metadata for jobs commands."""

    request: RunRequest
    schedule: ScheduledRunSchedule

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "kind": SCHEDULED_RUN_BUNDLE_KIND,
            "schema_version": SCHEDULED_RUN_BUNDLE_SCHEMA_VERSION,
            "request": self.request.to_dict(),
            "schedule": self.schedule.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScheduledRunBundle":
        """Create a scheduled-run bundle from JSON-compatible data."""
        _validate_bundle_identity(data)
        request_payload = data.get("request")
        if not isinstance(request_payload, Mapping):
            raise ScheduledRunBundleError(
                "scheduled-run bundle request must be a JSON object"
            )
        schedule_payload = data.get("schedule")
        if not isinstance(schedule_payload, Mapping):
            raise ScheduledRunBundleError(
                "scheduled-run bundle schedule must be a JSON object"
            )
        return cls(
            request=RunRequest.from_dict(request_payload),
            schedule=ScheduledRunSchedule.from_dict(schedule_payload),
        )

    def request_for_jobs(self) -> RunRequest:
        """Return the request with bundled schedule metadata applied."""
        return request_with_schedule_metadata(self.request, self.schedule)


def build_scheduled_run_bundle(request: RunRequest) -> ScheduledRunBundle:
    """Build a bundle while keeping schedule metadata out of raw request JSON."""
    return ScheduledRunBundle(
        request=request_without_schedule_metadata(request),
        schedule=schedule_from_request(request),
    )


def load_scheduled_run_bundle_json(payload: str) -> ScheduledRunBundle:
    """Load a scheduled-run bundle from a JSON string."""
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ScheduledRunBundleError(
            f"scheduled-run bundle is not valid JSON: {exc.msg}"
        ) from exc
    if not isinstance(data, Mapping):
        raise ScheduledRunBundleError(
            "scheduled-run bundle must be a JSON object"
        )
    return ScheduledRunBundle.from_dict(data)


def request_without_schedule_metadata(request: RunRequest) -> RunRequest:
    """Return a request with bundle-owned schedule metadata removed."""
    metadata = dict(request.metadata)
    for key in _SCHEDULE_METADATA_KEYS:
        metadata.pop(key, None)
    if metadata == request.metadata:
        return request
    return replace(request, metadata=metadata)


def request_with_schedule_metadata(
    request: RunRequest,
    schedule: ScheduledRunSchedule,
) -> RunRequest:
    """Return a request with schedule metadata applied authoritatively."""
    metadata = dict(request.metadata)
    for key in _SCHEDULE_METADATA_KEYS:
        metadata.pop(key, None)
    if schedule.no_overlap:
        metadata["no_overlap"] = True
    if schedule.schedule_key:
        metadata["schedule_key"] = schedule.schedule_key
    if metadata == request.metadata:
        return request
    return replace(request, metadata=metadata)


def schedule_from_request(request: RunRequest) -> ScheduledRunSchedule:
    """Extract scheduled-run metadata from a RunRequest."""
    schedule_key = request.metadata.get("schedule_key", "")
    return ScheduledRunSchedule(
        no_overlap=bool(request.metadata.get("no_overlap", False)),
        schedule_key=str(schedule_key or "").strip(),
    )


def _validate_bundle_identity(data: Mapping[str, Any]) -> None:
    kind = data.get("kind")
    if kind != SCHEDULED_RUN_BUNDLE_KIND:
        raise ScheduledRunBundleError(
            "scheduled-run bundle kind is not supported"
        )
    schema_version = data.get("schema_version")
    if schema_version != SCHEDULED_RUN_BUNDLE_SCHEMA_VERSION:
        raise ScheduledRunBundleError(
            "scheduled-run bundle schema_version is not supported"
        )
