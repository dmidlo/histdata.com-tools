"""Controlled actual IPC/native publications; never historical market evidence."""

from dataclasses import replace
from unittest.mock import patch

from histdatacom.data_quality.training_contracts import DAY_NS
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_overlap_contracts import (
    TrainingOverlapGeometryV1,
    TrainingOverlapPlanV1,
)
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalPartition,
    TrainingTemporalAssignmentV1,
    TrainingTemporalSplitV1,
)
from tests.fixtures import training_substrate_v1 as substrate

BASE = substrate.BASE
TIMES = tuple(
    BASE + day * DAY_NS + fraction * DAY_NS // 10
    for day in range(3)
    for fraction in (6, 7)
)


def overlap_fixture(
    tmp_path, *, native=False, multiple_splits=False, asynchronous=False
):
    """Three whole original days with genuine source-backed middle-day support."""
    with patch.object(substrate, "TIMES", TIMES):
        source, version = substrate.observed_source(tmp_path)
        if native:
            original = substrate.SyntheticEventV1.generated

            def generated(**kwargs):
                if asynchronous:
                    kwargs["event_time_ns"] += {
                        "EURGBP": 0,
                        "EURUSD": 2,
                        "GBPUSD": 1,
                    }[kwargs["symbol"].upper()]
                return original(**kwargs)

            with patch.object(
                substrate.SyntheticEventV1, "generated", side_effect=generated
            ):
                products = tuple(
                    substrate.published_product(
                        tmp_path / f"product-{day}-{member}",
                        version,
                        member=member,
                        seed=656 + day,
                        indices=(day * 2, day * 2 + 1),
                    )[0]
                    for day in range(3)
                    for member in ("member-a", "member-b")
                )
            source = replace(
                source,
                product_manifest_paths=tuple(
                    sorted(str(p.manifest_path) for p in products)
                ),
            )
    ownership = build_training_ownership(source)
    parts = tuple(
        (
            (
                TemporalPartition.TRAIN
                if u.start_ns < BASE + DAY_NS
                else (
                    TemporalPartition.VALIDATION
                    if u.start_ns < BASE + 2 * DAY_NS
                    else TemporalPartition.TEST
                )
            )
            if multiple_splits
            else TemporalPartition.TRAIN
        )
        for u in ownership.units
    )
    split = TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(u.artifact_id, parts[i])
            for i, u in enumerate(ownership.units)
        ),
    )
    plan = TrainingOverlapPlanV1(
        source,
        ownership,
        split,
        TrainingOverlapGeometryV1(
            BASE, BASE + 3 * DAY_NS, DAY_NS // 2, DAY_NS // 4
        ),
    )
    return plan
