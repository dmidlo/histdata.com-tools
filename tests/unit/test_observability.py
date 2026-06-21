"""Tests for renderer-neutral observability helpers."""

from __future__ import annotations

import json

from histdatacom import observability
from histdatacom.observability import (
    ProgressState,
    attach_progress_metadata,
    progress_increment,
)
from histdatacom.runtime_contracts import StatusEvent, WorkStatus


def test_progress_state_emits_serializable_events(monkeypatch) -> None:
    """Progress state should be usable without a terminal renderer."""
    timestamps = iter(
        (
            "2026-06-21T00:00:01Z",
            "2026-06-21T00:00:03Z",
        )
    )
    emitted: list[StatusEvent] = []
    monkeypatch.setattr(
        observability,
        "utc_now_iso",
        lambda: next(timestamps),
    )
    state = ProgressState(
        stage="download_archives",
        total=4,
        unit="records",
        started_at_utc="2026-06-21T00:00:00Z",
        event_sink=emitted.append,
    )

    first = state.advance(
        2,
        status=WorkStatus.CSV_ZIP,
        message="Downloaded archive batch.",
    )
    second = state.fail("network timeout", increment=1)
    payload = json.loads(json.dumps(state.to_dict()))

    assert emitted == [first, second]
    assert progress_increment(first) == 2.0
    assert first.metadata["event_type"] == "progress"
    assert first.metadata["completed"] == 2.0
    assert first.metadata["total"] == 4
    assert first.metadata["percent_complete"] == 50.0
    assert first.metadata["rate_per_second"] == 2.0
    assert second.status is WorkStatus.FAILED
    assert second.metadata["last_error"] == "network timeout"
    assert payload["completed"] == 3.0
    assert payload["last_error"] == "network timeout"


def test_attach_progress_metadata_round_trips_status_event() -> None:
    """Status events should carry structured progress data through JSON."""
    event = attach_progress_metadata(
        StatusEvent(
            status=WorkStatus.COMPLETED,
            stage="merge_cache",
            message="Merged cache artifacts.",
            timestamp_utc="2026-06-21T00:00:02Z",
        ),
        total=10,
        completed=5,
        unit="records",
        increment=5,
        started_at_utc="2026-06-21T00:00:00Z",
    )

    restored = StatusEvent.from_dict(json.loads(json.dumps(event.to_dict())))

    assert restored.metadata["event_type"] == "progress"
    assert restored.metadata["percent_complete"] == 50.0
    assert restored.metadata["rate_per_second"] == 2.5
    assert restored.metadata["unit"] == "records"
