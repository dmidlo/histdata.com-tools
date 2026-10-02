"""Explicit IO locators and mandatory native replay of retained V2 roots.

The scope carries locations only. It grants neither provider rights nor a
stored verification claim; every material boundary replays actual source bytes.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path

from .fingerprint_v2 import (
    BrokerDeliveryFingerprintV2,
    require_qualified_broker_fingerprint,
)


@dataclass(frozen=True, slots=True)
class _Sources:
    process_id: int
    roots: tuple[Path, ...]


_CURRENT: ContextVar[_Sources | None] = ContextVar(
    "broker_fingerprint_source_locators", default=None
)


@contextmanager
def broker_fingerprint_sources(*roots: str | Path) -> Iterator[None]:
    """Declare 1..64 candidate capture roots for current-process source IO.

    No recursive search or automatic provider selection occurs. Each retained
    session must occur below exactly one declared root; multiple copies are
    ambiguous even if their current bytes match. Nested scopes may append
    explicit locators but cannot replace or remove the outer scope's roots.
    """
    previous = _CURRENT.get()
    if previous is not None and previous.process_id != os.getpid():
        raise ValueError(
            "broker fingerprint locators belong to another process"
        )
    if not 1 <= len(roots) <= 64 or any(
        not isinstance(item, (str, Path)) for item in roots
    ):
        raise ValueError("fingerprint sources require1..64 explicit roots")
    paths = tuple(Path(item).resolve(strict=True) for item in roots)
    if len(set(paths)) != len(paths) or any(
        not item.is_dir() for item in paths
    ):
        raise ValueError("fingerprint source roots must be unique directories")
    paths = tuple(
        dict.fromkeys((*(previous.roots if previous else ()), *paths))
    )
    if len(paths) > 64:
        raise ValueError("aggregate fingerprint source locator bound exceeded")
    token = _CURRENT.set(_Sources(os.getpid(), paths))
    try:
        yield
    finally:
        _CURRENT.reset(token)


def verify_broker_fingerprint_sources(
    value: object,
) -> BrokerDeliveryFingerprintV2:
    """Replay complete exact sources and current rights against retained seals."""
    from histdatacom.broker_plugin_policy.bindings import BrokerLegacyCaptureV1
    from histdatacom.broker_plugin_provenance.native import (
        require_legacy_capture_provenance,
    )

    fingerprint = require_qualified_broker_fingerprint(value)
    state = _CURRENT.get()
    if state is None or state.process_id != os.getpid():
        raise ValueError(
            "current-process broker fingerprint source locators required"
        )
    for reference in fingerprint.capture_roots:
        manifest = reference.manifest
        candidates = tuple(
            root
            for root in state.roots
            if (root / manifest.session.session_id).exists()
        )
        if len(candidates) != 1:
            raise ValueError(
                "fingerprint source locator is absent or ambiguous"
            )
        actual = require_legacy_capture_provenance(
            candidates[0],
            manifest,
            provider_request=BrokerLegacyCaptureV1(
                manifest.session, reference.output_contract
            ),
            expected_root=reference.seal,
        )
        if (
            not actual.complete
            or not actual.anchored
            or actual.header.to_json() != reference.header.to_json()
            or actual.seal is None
            or actual.seal.to_json() != reference.seal.to_json()
        ):
            raise ValueError(
                "actual fingerprint source differs from retained anchored provenance"
            )
    return fingerprint


def _fit_source_scope(root: str | Path) -> AbstractContextManager[None]:
    """The fitter's explicit input root may supply its own bounded IO scope."""
    return broker_fingerprint_sources(root)
