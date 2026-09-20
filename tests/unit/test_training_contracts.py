"""Strict canonical substrate boundaries and closed admission policy."""

import json
from dataclasses import FrozenInstanceError, replace

import pytest

from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingBatchV1,
    TrainingConsumerMode,
    TrainingInformationMode,
    TrainingOrigin,
    TrainingRequestV1,
    admitted_modes,
    training_json,
    training_load,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_views import materialize_training_rows
from tests.fixtures.training_substrate_v1 import BASE, SYMBOLS, observed_source


@pytest.fixture
def batch(tmp_path):
    source, _ = observed_source(tmp_path)
    ownership = build_training_ownership(source)
    request = TrainingRequestV1(
        TrainingConsumerMode.DESCRIPTIVE, BASE, BASE + DAY_NS, SYMBOLS
    )
    return materialize_training_rows(source, ownership, request)


def test_canonical_deep_immutable_roundtrip(batch):
    assert TrainingBatchV1.from_json(batch.to_json()) == batch
    assert json.loads(batch.to_json())["artifact_id"] == batch.artifact_id
    with pytest.raises(FrozenInstanceError):
        batch.rows[0].origin = TrainingOrigin.SYNTHETIC_RECONSTRUCTION
    wire = batch.to_dict()
    wire["rows"][0]["origin"] = "synthetic_reconstruction"
    assert batch.rows[0].origin is TrainingOrigin.OBSERVED
    with pytest.raises(ValueError):
        TrainingBatchV1.from_dict(wire)


@pytest.mark.parametrize(
    "change",
    [
        lambda d: d.update(extra="ignored?"),
        lambda d: d["rows"][0].update(extra="ignored?"),
        lambda d: d["ownership"]["units"][0].update(extra="ignored?"),
        lambda d: d["rows"][0].update(event_time_ns=str(BASE)),
        lambda d: d["rows"][0].update(event_time_ns=True),
        lambda d: d["rows"][0].update(available_at_ns=float("inf")),
        lambda d: d["rows"][0].update(nonclaims=[]),
    ],
)
def test_unknown_fields_coercions_and_relabeling_refuse(batch, change):
    data = batch.to_dict()
    change(data)
    with pytest.raises(ValueError):
        TrainingBatchV1.from_dict(data)


@pytest.mark.parametrize("origin", list(TrainingOrigin))
@pytest.mark.parametrize("information", list(TrainingInformationMode))
def test_complete_origin_information_admissibility_matrix(origin, information):
    modes = admitted_modes(origin, information)
    if information is not TrainingInformationMode.EX_POST:
        assert modes == ()
    elif origin in (
        TrainingOrigin.OFFICIAL_CONTEXT,
        TrainingOrigin.MACHINE_FORECAST,
    ):
        assert modes == (TrainingConsumerMode.DESCRIPTIVE,)
    elif origin in (
        TrainingOrigin.OBSERVED,
        TrainingOrigin.SYNTHETIC_RECONSTRUCTION,
        TrainingOrigin.BROKER_CONDITIONED_COUNTERFACTUAL,
    ):
        assert set(modes) == set(TrainingConsumerMode) - {
            TrainingConsumerMode.CAUSAL
        }
    else:
        assert modes == ()


def test_json_dags_cycles_duplicates_and_depth_fail_before_expansion():
    branch = ["x" * 1024]
    for _ in range(13):
        branch = [branch, branch]
    with pytest.raises(ValueError, match="budget"):
        training_json({"dag": branch})
    cyclic = []
    cyclic.append(cyclic)
    with pytest.raises(ValueError, match="nesting"):
        training_json({"cycle": cyclic})
    with pytest.raises(ValueError, match="JSON"):
        training_load('{"x":1,"x":2}')


def test_row_contract_binds_clock_schema_and_mode(batch):
    row = batch.rows[0]
    with pytest.raises(ValueError, match="predates"):
        replace(row, decision_time_ns=row.event_time_ns - 1)
    with pytest.raises(ValueError, match="admissibility"):
        replace(row, admissible_modes=(TrainingConsumerMode.CAUSAL,))
    with pytest.raises(ValueError, match="replicate"):
        replace(batch, rows=(row, row))
    assert (
        replace(row, feature_schema_version="successor.v2").artifact_id
        != row.artifact_id
    )


def test_identity_cache_does_not_rehash_whole_ownership_for_every_row(
    batch, monkeypatch
):
    # The immutable cache is observable by making any subsequent serialization
    # fail. Existing identities/JSON remain available without walking the graph.
    from histdatacom.data_quality import training_contracts as module

    identity, text = batch.ownership.artifact_id, batch.ownership.to_json()
    monkeypatch.setattr(
        module,
        "training_json",
        lambda value: pytest.fail("cached identity recomputed"),
    )
    assert batch.ownership.artifact_id == identity
    assert batch.ownership.to_json() == text
