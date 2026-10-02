"""Explicit reference-host fault injection, never installed-candidate proof."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

from .contracts import BrokerConformancePlanV1, BrokerConformanceSubject


@contextmanager
def reference_host_fault(
    plan: BrokerConformancePlanV1, case_id: str
) -> Iterator[None]:
    """Bounded ACK loss plus slow durable sink provokes actual host overflow.

    A compliant one-outstanding-delivery plugin cannot intentionally saturate
    its host. This separate selftest alters host delivery conditions, records
    the real refusal, and is therefore categorically not candidate proof.
    """
    if case_id != "queue.overflow":
        yield
        return
    if plan.subject is not BrokerConformanceSubject.REFERENCE:
        raise ValueError(
            "host fault injection requires explicit reference subject"
        )
    from histdatacom.broker_plugin_lifecycle import storage, supervisor

    original_encode = supervisor.encode_frame
    original_append = storage.Journal.append

    def encode(value: dict[str, object], maximum: int = 524288) -> bytes:
        return (
            b""
            if value.get("type") == "ack"
            else original_encode(value, maximum)
        )

    def append(journal: Any, record: Any) -> None:
        original_append(journal, record)
        if record.kind == "event":
            time.sleep(0.15)

    with (
        patch.object(supervisor, "encode_frame", encode),
        patch.object(storage.Journal, "append", append),
    ):
        yield
