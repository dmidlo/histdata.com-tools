"""Unit guard witnesses, not native or empirical publication qualification.

Only dependency results are doubled. The actual production completeness
guard executes, and an explicit tripwire prevents subsequent artifact I/O.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, NoReturn

import pytest

from histdatacom import campaign_verification
from histdatacom import reconstruction as public


class _ArtifactBindingReached(AssertionError):
    """Controlled next stage; never a successful publication result."""


def _publication_witness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    verification_status: str = "complete",
    index_status: str = "complete",
    missing_product_count: int = 0,
) -> tuple[Callable[[], None], list[str], Path]:
    index_path = tmp_path / "unit guard index.json"
    index_target = index_path.resolve()
    output_directory = tmp_path / "publication"
    calls: list[str] = []
    verification = SimpleNamespace(status=verification_status)
    # Populate every field evaluated before the controlled next stage. If a
    # guard term is bypassed, failure must be the intentional tripwire, not
    # a missing attribute, file, import, or invalid native contract.
    index = SimpleNamespace(
        status=index_status,
        missing_product_count=missing_product_count,
        product_index_id="unit-guard:index",
        plan_set_id="unit-guard:plan-set",
        support_artifact_id="unit-guard:support",
        shard_refs=("unit-guard:shard",),
        support_window_count=1,
        verified_product_count=2,
        empty_window_count=0,
        refused_window_count=0,
        observed_event_count=6,
        synthetic_event_count=2,
    )

    def fresh_verification(path: str | Path) -> SimpleNamespace:
        assert Path(path) == index_target
        assert calls == []
        calls.append("fresh_verification")
        return verification

    def read_index(path: str | Path) -> SimpleNamespace:
        assert Path(path) == index_target
        assert calls == ["fresh_verification"]
        calls.append("read_index")
        return index

    def artifact_binding(
        path: str | Path,
        *,
        kind: str,
        metadata: Mapping[str, Any],
    ) -> NoReturn:
        assert Path(path) == index_target
        assert kind == "reconstruction_campaign_product_index_v1"
        assert calls == ["fresh_verification", "read_index"]
        assert metadata == {
            "product_index_id": index.product_index_id,
            "plan_set_id": index.plan_set_id,
            "support_artifact_id": index.support_artifact_id,
            "shard_count": 1,
            "support_window_count": 1,
            "verified_product_count": 2,
            "missing_product_count": missing_product_count,
            "empty_window_count": 0,
            "refused_window_count": 0,
            "observed_event_count": 6,
            "synthetic_event_count": 2,
            "status": index_status,
        }
        calls.append("artifact_binding")
        raise _ArtifactBindingReached(
            "publication crossed the completeness guard into artifact binding"
        )

    # The verifier is imported inside the production function; patch its
    # defining module rather than a nonexistent public-module alias.
    monkeypatch.setattr(
        campaign_verification,
        "verify_campaign_product_index",
        fresh_verification,
    )
    monkeypatch.setattr(
        public, "read_reconstruction_campaign_product_index", read_index
    )
    monkeypatch.setattr(public, "artifact_ref_for_file", artifact_binding)

    def publish() -> None:
        public._publish_reconstruction_campaign_dataset(
            index_path,
            output_directory=output_directory,
            dataset_id="unit-guard:dataset",
        )

    return publish, calls, output_directory


def _assert_refused_before_artifact_binding(
    publish: Callable[[], None], calls: list[str], output_directory: Path
) -> None:
    with pytest.raises(
        public.ReconstructionRefusedError,
        match="^campaign dataset publication requires every retained product$",
    ) as caught:
        publish()
    assert type(caught.value) is public.ReconstructionRefusedError
    assert calls == ["fresh_verification", "read_index"]
    assert not output_directory.exists()


def test_publication_refuses_incomplete_fresh_verification(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    witness = _publication_witness(
        monkeypatch, tmp_path, verification_status="incomplete"
    )
    _assert_refused_before_artifact_binding(*witness)


def test_publication_refuses_incomplete_index(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    witness = _publication_witness(
        monkeypatch, tmp_path, index_status="incomplete"
    )
    _assert_refused_before_artifact_binding(*witness)


def test_publication_refuses_missing_retained_product(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    witness = _publication_witness(
        monkeypatch, tmp_path, missing_product_count=1
    )
    _assert_refused_before_artifact_binding(*witness)


def test_complete_publication_inputs_reach_artifact_binding(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    publish, calls, output_directory = _publication_witness(
        monkeypatch, tmp_path
    )
    with pytest.raises(
        _ArtifactBindingReached,
        match="crossed the completeness guard into artifact binding",
    ):
        publish()
    assert calls == ["fresh_verification", "read_index", "artifact_binding"]
    assert not output_directory.exists()
