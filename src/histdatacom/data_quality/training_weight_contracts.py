"""Immutable training-weight protocol; construction is never execution approval.

This module deliberately has no source materializer or candidate runner. The
real experiment requires a separately reviewed policy commit and adapter
fingerprint. Its frozen protocol cannot be changed through caller input.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from importlib import resources
from typing import cast

from .training_contracts import training_json, training_load

PREREGISTRATION_FILENAME = "training_weight_preregistration_v1.json"
PREREGISTRATION_FILE_SHA256 = (
    "025178395cc8da1224132e8a480428e7ea7324e56c2859a2cf9a9f1d5d703b55"
)
PREREGISTRATION_CANONICAL_SHA256 = (
    "81ea99e7fe6dbef60aa566fbd4173b05fa479a25f8fee130bc9642a9088eaaf0"
)
MAX_PREREGISTRATION_BYTES = 64 * 1024


def _object(value: object) -> dict[str, object]:
    if type(value) is not dict:
        raise ValueError("expected protocol object")
    return cast(dict[str, object], value)


def _array(value: object) -> list[object]:
    if type(value) is not list:
        raise ValueError("expected protocol array")
    return cast(list[object], value)


def _text(value: object) -> str:
    if type(value) is not str:
        raise ValueError("expected protocol string")
    return value


def _integer(value: object) -> int:
    if type(value) is not int:
        raise ValueError("expected protocol integer")
    return value


def _preregistered_input_hashes() -> frozenset[str]:
    """Known empirical inputs must never be relabelled as contract fixtures.

    This is accidental-label protection, not same-user authentication. It
    checks declared identities without opening any empirical input path.
    """
    policy = read_training_weight_preregistration().to_dict()
    model = _object(policy["fixed_model"])
    return frozenset(
        [
            _text(model["index_sha256"]),
            _text(_object(policy["epoch_mapping"])["sha256"]),
        ]
        + [
            _text(_object(item)["sha256"])
            for item in (
                _array(policy["source_partitions"])
                + _array(model["training_sources"])
            )
        ]
    )


@dataclass(frozen=True, slots=True)
class TrainingWeightPreregistrationV1:
    """Exact, immutable preregistration, not a mutable policy configuration.

    The digest binds every nested field, including unknown-field refusal and
    exact scalar types. A changed policy needs a successor, not resealing.
    Returned dictionaries are copies; no mutable nested state is retained.
    """

    policy_json: str
    _canonical: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.policy_json) is not str
            or len(self.policy_json) > MAX_PREREGISTRATION_BYTES
        ):
            raise ValueError("preregistration exceeds byte bound")
        value = training_load(self.policy_json)
        canonical = training_json(value)
        if len(canonical) > MAX_PREREGISTRATION_BYTES:
            raise ValueError("preregistration exceeds canonical byte bound")
        digest = hashlib.sha256(canonical.encode("ascii")).hexdigest()
        if digest != PREREGISTRATION_CANONICAL_SHA256:
            raise ValueError("policy differs from frozen preregistration")
        object.__setattr__(self, "policy_json", canonical)
        object.__setattr__(self, "_canonical", canonical)

    @property
    def artifact_id(self) -> str:
        return (
            "training-weight-preregistration:sha256:"
            + PREREGISTRATION_CANONICAL_SHA256
        )

    @property
    def execution_approved(self) -> bool:
        """Loading an asset never authorizes the real experiment."""
        return False

    def to_json(self) -> str:
        return self._canonical

    def to_dict(self) -> dict[str, object]:
        return _object(training_load(self._canonical))

    @classmethod
    def from_dict(
        cls, value: dict[str, object]
    ) -> TrainingWeightPreregistrationV1:
        return cls(training_json(value))

    @classmethod
    def from_json(cls, text: str) -> TrainingWeightPreregistrationV1:
        return cls(text)

    def scheduled_dates(self, role: str) -> tuple[str, ...]:
        if role not in ("calibration", "application"):
            raise ValueError("unsupported experiment role")
        return tuple(
            _text(day)
            for item in _array(self.to_dict()["schedule"])
            if _object(item)["role"] == role
            for day in _array(_object(item)["dates"])
        )

    def holm_coordinates(self) -> tuple[str, ...]:
        evaluation = _object(self.to_dict()["evaluation"])
        test = _object(evaluation["significance_test"])
        return tuple(
            _text(item) for item in _array(test["fixed_family_coordinates"])
        )

    def verify_declared_input_bytes(
        self, data: bytes, *, role: str, relative_path: str
    ) -> str:
        """Check one allowlisted artifact's full bytes, not its locator alone.

        No rows are decoded. This is an input-byte check, not scientific
        qualification, calibration support, or permission to execute.
        """
        if type(data) is not bytes or type(relative_path) is not str:
            raise ValueError("input verification requires bytes and path")
        policy = self.to_dict()
        expected: dict[str, object] | None = None
        if role == "historical-source":
            for raw in _array(policy["source_partitions"]):
                item = _object(raw)
                if item["relative_path"] == relative_path:
                    expected = item
                    break
        elif role == "model-training-source":
            model = _object(policy["fixed_model"])
            for raw in _array(model["training_sources"]):
                item = _object(raw)
                if item["path"] == relative_path:
                    expected = item
                    break
        elif role == "fixed-index":
            model = _object(policy["fixed_model"])
            if model["index_path"] == relative_path:
                expected = {
                    "sha256": model["index_sha256"],
                    "size_bytes": model["index_size_bytes"],
                }
        elif role == "epoch-model":
            epoch = _object(policy["epoch_mapping"])
            if epoch["path"] == relative_path:
                expected = {"sha256": epoch["sha256"], "size_bytes": 246353}
        else:
            raise ValueError("unsupported protocol input role")
        if expected is None:
            raise ValueError("input path is outside preregistered allowlist")
        if len(data) != _integer(expected["size_bytes"]):
            raise ValueError("input byte count differs from preregistration")
        digest = hashlib.sha256(data).hexdigest()
        if digest != expected["sha256"]:
            raise ValueError("input SHA-256 differs from preregistration")
        return digest


def read_training_weight_preregistration() -> TrainingWeightPreregistrationV1:
    """Read the bounded installed asset and verify exact file/canonical bytes."""
    asset = (
        resources.files("histdatacom.data_quality")
        .joinpath("assets")
        .joinpath(PREREGISTRATION_FILENAME)
    )
    with asset.open("rb") as stream:
        data = stream.read(MAX_PREREGISTRATION_BYTES + 1)
    if len(data) > MAX_PREREGISTRATION_BYTES:
        raise ValueError("preregistration file exceeds byte bound")
    if hashlib.sha256(data).hexdigest() != PREREGISTRATION_FILE_SHA256:
        raise ValueError("installed preregistration file SHA-256 differs")
    return TrainingWeightPreregistrationV1(data.decode("utf-8"))
