"""Preregistration canaries only: no real windows, candidates, or outcomes."""

import json
from dataclasses import FrozenInstanceError

import pytest

from histdatacom.data_quality.training_weight_contracts import (
    TrainingWeightPreregistrationV1,
    read_training_weight_preregistration,
)


def test_complete_frozen_preregistration_roundtrip_and_immutability():
    policy = read_training_weight_preregistration()
    assert TrainingWeightPreregistrationV1.from_json(policy.to_json()) == policy
    assert TrainingWeightPreregistrationV1.from_dict(policy.to_dict()) == policy
    assert not policy.execution_approved
    with pytest.raises(FrozenInstanceError):
        policy.policy_json = "{}"
    changed = policy.to_dict()
    changed["uncertainty"]["minimum_calibration_units"] = 2
    assert policy.to_dict()["uncertainty"]["minimum_calibration_units"] == 30


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(ignored=True),
        lambda d: d["fixed_model"].update(ignored=True),
        lambda d: d["uncertainty"].update(minimum_calibration_units=2),
        lambda d: d["uncertainty"].update(minimum_calibration_units=30.0),
        lambda d: d["uncertainty"].update(nominal_coverage=float("nan")),
        lambda d: d["epoch_mapping"].update(
            query_feed_epoch_value="technology_epoch_04"
        ),
        lambda d: d["fixed_model"].update(
            query_excluded_source_window_ids=["x"]
        ),
        lambda d: d["fixed_model"]["training_sources"][0].update(
            sha256="0" * 64
        ),
        lambda d: d["source_partitions"][0].update(sha256="0" * 64),
        lambda d: d["evaluation"]["significance_test"].update(
            fixed_family_size=6
        ),
        lambda d: d["execution_gate"].update(state="approved"),
    ],
)
def test_caller_cannot_reseal_or_coerce_protocol(mutate):
    data = read_training_weight_preregistration().to_dict()
    mutate(data)
    with pytest.raises(ValueError):
        TrainingWeightPreregistrationV1.from_dict(data)


def test_duplicate_keys_oversize_and_cyclic_inputs_refuse():
    with pytest.raises(ValueError, match="JSON"):
        TrainingWeightPreregistrationV1('{"issue":607,"issue":607}')
    with pytest.raises(ValueError, match="bound"):
        TrainingWeightPreregistrationV1(" " * (64 * 1024 + 1))
    data = {}
    data["cycle"] = data
    with pytest.raises(ValueError, match="nesting"):
        TrainingWeightPreregistrationV1.from_dict(data)


def test_roles_complete_calendar_and_shards_are_not_support_counts():
    policy = read_training_weight_preregistration()
    assert len(policy.scheduled_dates("calibration")) == 42
    assert len(policy.scheduled_dates("application")) == 20
    assert not set(policy.scheduled_dates("calibration")) & set(
        policy.scheduled_dates("application")
    )
    data = policy.to_dict()
    for month in data["schedule"]:
        assert [
            d for shard in month["shards"] for d in shard["dates"]
        ] == month["dates"]
        assert all(len(s["dates"]) <= 8 for s in month["shards"])
    assert data["evidence_ownership"]["symbols"] == [
        "EURGBP",
        "EURUSD",
        "GBPUSD",
    ]
    with pytest.raises(ValueError, match="role"):
        policy.scheduled_dates("final_holdout")


def test_holm_family_and_matrix_product_are_frozen():
    policy = read_training_weight_preregistration()
    family = policy.holm_coordinates()
    assert len(family) == len(set(family)) == 7
    assert sum("one-member-per-epoch" in name for name in family) == 4
    assert not any("negative-control" in name for name in family)
    assert (
        "matrix multiplication"
        in policy.to_dict()["evaluation"]["correlation"]["participation_ratio"]
    )


def test_fixed_model_boundary_keeps_ancestry_and_historical_query_epoch():
    data = read_training_weight_preregistration().to_dict()
    model = data["fixed_model"]
    assert model["fragment_count"] == 256
    assert model["training_day_count"] == 20
    assert len(model["training_sources"]) == 15
    assert {s["metadata"]["period"] for s in model["training_sources"]} == set(
        data["roles"]["model_train_periods"]
    )
    assert data["epoch_mapping"]["query_feed_epoch_value"] == (
        "technology_epoch_03"
    )
    assert model["event_tags"] == ["context:not-admitted"]
    assert "external-market-context" in model["forbidden_inputs"]
    assert "application-losses" in model["forbidden_inputs"]


@pytest.mark.parametrize(
    ("role", "path"),
    [
        ("historical-source", "eurgbp/2025/10/.data"),
        ("historical-source", "eurgbp/2010/1/.data"),
        ("model-training-source", "eurgbp/2019/1/.data"),
        ("final_holdout", "anything"),
    ],
)
def test_locator_and_report_id_never_establish_source_verification(role, path):
    policy = read_training_weight_preregistration()
    with pytest.raises(ValueError):
        policy.verify_declared_input_bytes(b"{}", role=role, relative_path=path)
    # Persisting an ordinary parsed dictionary cannot manufacture approval.
    restored = TrainingWeightPreregistrationV1(json.dumps(policy.to_dict()))
    assert not restored.execution_approved
