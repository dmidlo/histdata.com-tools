"""Qualified diagonal coalition mapping; latent/PCA inversion is not implied."""

from __future__ import annotations

from dataclasses import dataclass, replace
from fractions import Fraction
from typing import ClassVar

from ._wire import Artifact, Record, ordered, text
from .contracts import (
    AttributionBackgroundV1,
    AttributionSnapshotV1,
    DecisionAttributionV1,
    FeatureSpace,
    ContributionV1,
    reference,
)
from .reference import _float


@dataclass(frozen=True, slots=True)
class DiagonalFeatureMapV1(Record):
    raw_name: str
    transformed_name: str
    scale: float
    offset: float
    raw_unit: str = "dimensionless"
    transformed_unit: str = "dimensionless"

    def _validate(self) -> None:
        text(self.raw_name)
        text(self.transformed_name)
        text(self.raw_unit)
        text(self.transformed_unit)
        if self.scale == 0:
            raise ValueError("qualified diagonal map must be invertible")


@dataclass(frozen=True, slots=True)
class AttributionMappingV1(Artifact):
    KIND: ClassVar[str] = "diagonal-mapping"
    axes: tuple[DiagonalFeatureMapV1, ...]
    known_at_ns: int

    def _validate(self) -> None:
        ordered(tuple(a.transformed_name for a in self.axes), nonempty=True)
        if (
            len({a.raw_name for a in self.axes}) != len(self.axes)
            or len(self.axes) > 8
            or self.known_at_ns < 0
        ):
            raise ValueError("one-to-one bounded mapping required")


def _verify_pair(
    raw: AttributionSnapshotV1,
    transformed: AttributionSnapshotV1,
    mapping: AttributionMappingV1,
) -> None:
    if (
        raw.space is not FeatureSpace.RAW
        or transformed.space is not FeatureSpace.TRANSFORMED
    ):
        raise ValueError("diagonal mapping cannot relabel latent/PCA as raw")
    if (raw.cutoff_at_ns, raw.universe, raw.session, raw.domain) != (
        transformed.cutoff_at_ns,
        transformed.universe,
        transformed.session,
        transformed.domain,
    ):
        raise ValueError("mapping changes cutoff/domain/universe/session")
    if mapping.known_at_ns > raw.cutoff_at_ns:
        raise ValueError("future preprocessing mapping")
    if transformed.preprocessing != reference(mapping):
        raise ValueError(
            "transformed snapshot lacks exact diagonal mapping identity"
        )
    originals = {v.feature.name: v for v in raw.values}
    mapped = {v.feature.name: v for v in transformed.values}
    if set(originals) != {a.raw_name for a in mapping.axes} or set(mapped) != {
        a.transformed_name for a in mapping.axes
    }:
        raise ValueError("mapping lacks complete raw/transformed projection")
    renaming = {a.raw_name: a.transformed_name for a in mapping.axes}
    for axis in mapping.axes:
        before, after = originals[axis.raw_name], mapped[axis.transformed_name]
        if (
            replace(
                before.feature,
                name=axis.transformed_name,
                unit=axis.transformed_unit,
            )
            != after.feature
        ):
            raise ValueError("mapping changes feature or mask semantics")
        expected_mask = (
            None if before.mask_name is None else renaming[before.mask_name]
        )
        if after.mask_name != expected_mask:
            raise ValueError("mapping changes missingness mask ownership")
        if before.feature.plane == "missingness" and (
            axis.scale != 1.0
            or axis.offset != 0.0
            or axis.raw_unit != axis.transformed_unit
        ):
            raise ValueError("missingness masks require identity value mapping")
        if (before.feature.unit, after.feature.unit) != (
            axis.raw_unit,
            axis.transformed_unit,
        ):
            raise ValueError("mapping unit conventions differ")
        if before.value is None or after.value is None:
            raise ValueError("mapping requires retained numeric inputs")
        if after.value != _float(
            Fraction(before.value) * Fraction(axis.scale)
            + Fraction(axis.offset)
        ):
            raise ValueError(
                "transformed value differs from executed diagonal map"
            )
        if (before.imputed, before.available_at_ns) != (
            after.imputed,
            after.available_at_ns,
        ):
            raise ValueError("mapping loses imputation/availability state")


@dataclass(frozen=True, slots=True)
class MappedAttributionV1(Artifact):
    KIND: ClassVar[str] = "mapped"
    source: DecisionAttributionV1
    mapping: AttributionMappingV1
    raw_snapshot: AttributionSnapshotV1
    raw_background: AttributionBackgroundV1
    contributions: tuple[ContributionV1, ...]

    def _validate(self) -> None:
        _verify_pair(self.raw_snapshot, self.source.snapshot, self.mapping)
        if (
            self.raw_background.role,
            self.raw_background.declared_at_ns,
        ) != (
            self.source.background.role,
            self.source.background.declared_at_ns,
        ):
            raise ValueError("mapping changes background role or declaration")
        if len(self.raw_background.snapshots) != len(
            self.source.background.snapshots
        ):
            raise ValueError("mapping requires all background samples")
        # Pair by the unchanged cutoff, not unrelated content-ID ordering.
        original = {s.cutoff_at_ns: s for s in self.raw_background.snapshots}
        if len(original) != len(self.raw_background.snapshots):
            raise ValueError("ambiguous raw background cutoff")
        for transformed in self.source.background.snapshots:
            if transformed.cutoff_at_ns not in original:
                raise ValueError("missing raw background counterpart")
            _verify_pair(
                original[transformed.cutoff_at_ns], transformed, self.mapping
            )
        names = {a.transformed_name: a.raw_name for a in self.mapping.axes}
        expected = tuple(
            sorted(
                (
                    ContributionV1(names[c.name], c.value)
                    for c in self.source.contributions
                ),
                key=lambda c: c.name,
            )
        )
        if self.contributions != expected:
            raise ValueError(
                "mapped contributions differ from coordinate-preserving coalition accounting"
            )
