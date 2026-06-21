"""Pytest unit tests for histdatacom.concurrency.py."""

from __future__ import annotations

from types import SimpleNamespace

import pytest


def test_concurrency() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


class _CompletedFuture:
    """Minimal future shape consumed by `_complete_future`."""

    def __init__(self, result: object = None) -> None:
        """Store a synchronous result."""
        self._result = result

    def result(self) -> object:
        """Return the stored result."""
        return self._result


class _SynchronousExecutor:
    """Executor test double that runs submitted callables immediately."""

    def __init__(
        self,
        max_workers: int,
        initializer: object | None = None,
        initargs: tuple[object, ...] = (),
    ) -> None:
        """Initialize and run the optional pool initializer."""
        self.max_workers = max_workers
        self.shutdown_called_with: dict[str, object] | None = None
        if initializer is not None:
            initializer(*initargs)  # type: ignore[operator]

    def __enter__(self) -> "_SynchronousExecutor":
        """Return the executor context."""
        return self

    def __exit__(self, *args: object) -> None:
        """Exit the executor context."""
        return None

    def submit(self, func: object, *args: object) -> _CompletedFuture:
        """Execute the submitted callable synchronously."""
        return _CompletedFuture(func(*args))  # type: ignore[operator]

    def shutdown(self, **kwargs: object) -> None:
        """Record shutdown arguments."""
        self.shutdown_called_with = kwargs


def _drain_statuses(records: object) -> list[str]:
    """Return statuses from a Records queue without requeuing items."""
    statuses = []
    while not records.empty():  # type: ignore[attr-defined]
        statuses.append(records.get().status)  # type: ignore[attr-defined]
    return statuses


def test_get_pool_cpu_count_uses_existing_cpu_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Document the current CLI CPU policy before replacing the scheduler."""
    import histdatacom.concurrency as concurrency

    monkeypatch.setattr(concurrency, "cpu_count", lambda: 8)

    assert concurrency.get_pool_cpu_count(None) == 7
    assert concurrency.get_pool_cpu_count("low") == 3
    assert concurrency.get_pool_cpu_count("medium") == 5
    assert concurrency.get_pool_cpu_count("high") == 7
    assert concurrency.get_pool_cpu_count("50") == 3
    assert concurrency.get_pool_cpu_count("200") == 15


def test_get_pool_cpu_count_rejects_bad_cpu_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed CPU policy values should keep exiting nonzero."""
    import histdatacom.concurrency as concurrency

    monkeypatch.setattr(concurrency, "cpu_count", lambda: 8)

    with pytest.raises(SystemExit):
        concurrency.get_pool_cpu_count("too-much")


def test_thread_pool_moves_processed_records_back_to_current_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ThreadPool should preserve the current next-to-current queue handoff."""
    import histdatacom.concurrency as concurrency
    from histdatacom.records import Record, Records

    current = Records()
    next_records = Records()
    for status in ("URL_NEW", "URL_VALID"):
        current.put(Record(status=status))

    def worker(record: Record, args: dict) -> None:
        record.status = args["status"]
        next_records.put(record)
        current.task_done()

    monkeypatch.setattr(concurrency, "ThreadPoolExecutor", _SynchronousExecutor)
    monkeypatch.setattr(
        concurrency, "as_completed", lambda futures: tuple(futures)
    )

    concurrency.ThreadPool(
        worker,
        {"status": "CSV_ZIP"},
        "Testing",
        "records",
        2,
    )(current, next_records)

    assert _drain_statuses(current) == ["CSV_ZIP", "CSV_ZIP"]
    assert next_records.empty()


def test_thread_pool_emits_structured_progress_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ThreadPool progress should be observable without inspecting Rich."""
    import histdatacom.concurrency as concurrency
    from histdatacom.records import Record, Records
    from histdatacom.runtime_contracts import StatusEvent

    current = Records()
    next_records = Records()
    for status in ("URL_NEW", "URL_VALID"):
        current.put(Record(status=status))
    events: list[StatusEvent] = []

    def worker(record: Record, args: dict) -> None:  # noqa: ARG001
        next_records.put(record)
        current.task_done()

    monkeypatch.setattr(concurrency, "ThreadPoolExecutor", _SynchronousExecutor)
    monkeypatch.setattr(
        concurrency, "as_completed", lambda futures: tuple(futures)
    )

    concurrency.ThreadPool(
        worker,
        {},
        "Testing",
        "records",
        2,
        event_sink=events.append,
    )(current, next_records)

    assert [event.metadata["increment"] for event in events] == [
        0.25,
        0.25,
        0.75,
        0.75,
    ]
    assert events[-1].metadata["completed"] == 2.0
    assert events[-1].metadata["event_type"] == "progress"


def test_process_pool_moves_processed_records_back_to_current_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ProcessPool should preserve the current next-to-current queue handoff."""
    import histdatacom.concurrency as concurrency
    from histdatacom.records import Record, Records

    current = Records()
    next_records = Records()
    for status in ("CSV_ZIP", "CSV_FILE"):
        current.put(Record(status=status))

    def worker(
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
    ) -> None:
        record.status = args["status"]
        records_next.put(record)
        records_current.task_done()

    monkeypatch.setattr(
        concurrency, "ProcessPoolExecutor", _SynchronousExecutor
    )
    monkeypatch.setattr(concurrency, "as_completed", lambda futures: futures)

    concurrency.ProcessPool(
        worker,
        {"status": "CACHE_READY"},
        "Testing",
        "records",
        2,
    )(current, next_records)

    assert _drain_statuses(current) == ["CACHE_READY", "CACHE_READY"]
    assert next_records.empty()


def test_on_keyboard_interrupt_shuts_down_executor_and_writer() -> None:
    """Keyboard interrupts should terminate writer and cancel pending futures."""
    from histdatacom.concurrency import _on_keyboard_interrupt

    writer = SimpleNamespace(terminated=False)
    executor = _SynchronousExecutor(max_workers=1)
    progress = SimpleNamespace(stopped=False)

    def terminate() -> None:
        writer.terminated = True

    def stop() -> None:
        progress.stopped = True

    writer.terminate = terminate
    progress.stop = stop

    with pytest.raises(SystemExit):
        _on_keyboard_interrupt(
            executor,  # type: ignore[arg-type]
            progress,  # type: ignore[arg-type]
            KeyboardInterrupt(),
            writer,  # type: ignore[arg-type]
        )

    assert progress.stopped
    assert writer.terminated
    assert executor.shutdown_called_with == {
        "wait": False,
        "cancel_futures": True,
    }
