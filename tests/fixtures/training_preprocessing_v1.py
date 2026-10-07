"""Invented native wide sources, not market history or model qualification.

No native verifier is replaced.  The ordinary numerical fixture has six
whole-day evidence units.  Its independently declared protected values are
deliberately unlike TRAIN.  The revision fixture is separate: a feature's
complete dependency interval genuinely participates in native ownership.
"""

from __future__ import annotations

from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingConsumerMode,
    TrainingRequestV1,
    TrainingSourceV1,
    training_json,
)
from histdatacom.data_quality.training_join_contracts import (
    JoinDirection,
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    TrainingJoinEntityV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_temporal_contracts import (
    TemporalPartition,
    TrainingTemporalAssignmentV1,
    TrainingTemporalSplitV1,
)
from histdatacom.data_quality.training_views import materialize_training_rows
from histdatacom.data_quality.training_wide_contracts import (
    TrainingWideGrainV1,
    WideRowGrain,
)
from histdatacom.data_quality.training_wide_views import (
    build_training_wide_plan,
    materialize_training_wide_view,
    project_training_wide_view,
)
from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    HistDataProviderAdapter,
    build_observed_dataset_version,
    histdata_cache_path,
)
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from tests.fixtures.training_join_v1 import bound_column
from tests.fixtures.training_substrate_v1 import BASE, SYMBOLS
from tests.fixtures.training_temporal_v1 import SECOND

# These are source inputs, not expectations obtained from a fitted transform.
DAY_BIDS = (
    (1.0, 2.0, 3.0, 4.0),
    (2.0, 4.0, 6.0, 8.0),
    (100.0, 110.0, 120.0, 130.0),
    (200.0, 210.0, 220.0, 230.0),
    (300.0, 310.0, 320.0, 330.0),
    (400.0, 410.0, 420.0, 430.0),
)
SPREADS = (0.5, 1.0, 1.5, 0.5)
BID = "market.tick.EURUSD.bid"
ASK = "market.tick.EURUSD.ask"
SPREAD = "market.tick.EURUSD.spread"
ABSENT = "uncertainty.EURUSD.unavailable"
OTHER_SYMBOL = "market.tick.GBPUSD.bid"
MACRO = "calendar.fixture.level"


def _source(
    directory: Path,
    times: tuple[int, ...],
    bids: tuple[float, ...],
    *,
    derived: tuple[str, ...] = (),
) -> tuple[TrainingSourceV1, Any]:
    """Write exact small native IPC, then derive the actual catalog roots."""
    assert len(times) == len(bids) and times == tuple(sorted(set(times)))
    root = directory / "ASCII" / "T"
    adapter = HistDataProviderAdapter()
    for symbol in SYMBOLS:
        # A deterministic, independently declared graph, not extra evidence.
        multiplier = {"EURUSD": 1.0, "GBPUSD": 2.0, "EURGBP": 0.5}[symbol]
        path = histdata_cache_path(root, symbol, "202001")
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "datetime": [clock // 1_000_000 for clock in times],
                "bid": [value * multiplier for value in bids],
                "ask": [
                    (value + SPREADS[index % 4]) * multiplier
                    for index, value in enumerate(bids)
                ],
                "vol": [0] * len(times),
            },
            schema={
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            },
        ).write_ipc(path)
    evidence = directory / "invented-source.json"
    evidence.write_text(
        '{"scope":"synthetic_software_fixture_not_market_evidence"}',
        encoding="utf-8",
    )
    descriptor = DatasetDescriptorV1(
        "preprocessing-invented",
        "Preprocessing invented source",
        "Fixed generated quotes; no historical or empirical qualification.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=SYMBOLS,
        periods=("202001",),
        qualification_evidence=(
            artifact_ref_for_file(evidence, kind="synthetic-software-fixture"),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    return (
        TrainingSourceV1(
            catalog.to_json(),
            version.dataset_version_id,
            derived_artifact_paths=derived,
        ),
        version,
    )


def chronological_split(ownership: Any) -> TrainingTemporalSplitV1:
    """Two TRAIN, two VALIDATION and two TEST original day units."""
    return TrainingTemporalSplitV1(
        ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(
                unit.artifact_id,
                (
                    TemporalPartition.TRAIN
                    if unit.start_ns < BASE + 2 * DAY_NS
                    else (
                        TemporalPartition.VALIDATION
                        if unit.start_ns < BASE + 4 * DAY_NS
                        else TemporalPartition.TEST
                    )
                ),
            )
            for unit in ownership.units
        ),
    )


def build_preprocessing_fixture(
    directory: Path, *, sparse_second_train_day: bool = False
) -> dict[str, Any]:
    from histdatacom.forecasting.feature_artifacts import write_feature_artifact

    samples = tuple(
        (BASE + day * DAY_NS + second * SECOND, value)
        for day, values in enumerate(DAY_BIDS)
        for second, value in enumerate(values, start=1)
        if not sparse_second_train_day or day != 1 or second in (1, 4)
    )
    times = tuple(clock for clock, _ in samples)
    bids = tuple(value for _, value in samples)
    matrix = _revision_matrix(future_value=100.0, include_future=True)
    artifact = write_feature_artifact(matrix, directory / "vintage")
    source, version = _source(directory, times, bids, derived=(str(artifact),))
    ownership = build_training_ownership(source)
    batch = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE,
            times[0],
            times[-1] + 1,
            ("EURUSD",),
        ),
    )
    quotes = TrainingJoinSourceV1(
        "quotes",
        JoinFamily.TICK,
        configuration_json=training_json(
            {"product_manifest_id": None, "ensemble_member_id": None}
        ),
    )
    absent = TrainingJoinSourceV1("unavailable", JoinFamily.UNSUPPORTED)
    macro = TrainingJoinSourceV1(
        "macro", JoinFamily.VINTAGE, paths=(str(artifact),)
    )
    columns = (
        *(
            bound_column(quotes, name, TrainingJoinEntityV1("EURUSD"), field)
            for name, field in ((BID, "bid"), (ASK, "ask"), (SPREAD, "spread"))
        ),
        bound_column(
            quotes, OTHER_SYMBOL, TrainingJoinEntityV1("GBPUSD"), "bid"
        ),
        bound_column(
            absent,
            ABSENT,
            TrainingJoinEntityV1("EURUSD"),
            "deliberately_unsupported",
        ),
        bound_column(
            macro,
            MACRO,
            TrainingJoinEntityV1("EURUSD", series="preprocessing.macro"),
            "macro",
            coordinate="fixed-reference",
            meaning=JoinMeaning.SNAPSHOT,
        ),
    )
    join = TrainingJoinPlanV1(
        batch,
        JoinInformationMode.EX_POST,
        (quotes, absent, macro),
        tuple(sorted(columns, key=lambda item: item.name)),
    )
    plan = build_training_wide_plan(
        (join,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )
    view = materialize_training_wide_view(plan)
    return {
        "source": source,
        "version": version,
        "ownership": ownership,
        "split": chronological_split(ownership),
        "wide_plan": plan,
        "wide_view": view,
        "records": project_training_wide_view(view),
        "times": times,
        "bids": bids,
        "matrix": matrix,
        "artifact_path": artifact,
        "directory": directory,
        "scope": "invented_native_software_inputs_not_empirical_utility",
    }


def _revision_matrix(
    *, future_value: float, include_future: bool, base_ns: int = BASE
) -> Any:
    """Four known revisions plus one later sentinel, in one native unit."""
    from histdatacom.forecasting import (
        FeatureColumnV1,
        FeatureDefinitionV1,
        FeatureEvidenceV1,
        FeatureKind,
        FeatureMatrixSnapshotV1,
        FeatureObservationV1,
        FeaturePeriodV1,
        FeatureRequestV1,
        FeatureSourceMode,
    )

    period = FeaturePeriodV1("fixed-reference", base_ns, base_ns + SECOND)
    definition = FeatureDefinitionV1(
        "preprocessing.macro",
        FeatureKind.MACRO,
        "index",
        1.0,
        "unadjusted",
        "invented constant reference period",
        "fixture",
        base_ns,
    )
    observations = []
    values = (1.0, 2.0, 3.0, 4.0) + ((future_value,) if include_future else ())
    for index, value in enumerate(values):
        available = base_ns + (index + 1) * SECOND
        evidence = FeatureEvidenceV1(
            "invented source",
            "fixture://preprocessing-vintage",
            "a" * 64,
            f"fixed-revision-{index}",
            "controlled-test",
            "1.0.0",
            "normalized simulated availability, not historical authenticity",
            base_ns + 7 * SECOND,
            FeatureSourceMode.OBSERVED,
            '{"synthetic_software_only":true}',
        )
        observations.append(
            FeatureObservationV1(
                definition,
                period,
                value,
                available,
                available,
                index,
                observations[-1].observation_id if observations else None,
                evidence,
            )
        )
    return FeatureMatrixSnapshotV1(
        FeatureRequestV1(
            base_ns + 6 * SECOND,
            (period,),
            (FeatureColumnV1("macro", "preprocessing.macro"),),
        ),
        tuple(observations),
    )


def build_preprocessing_vintage_fixture(
    directory: Path,
    *,
    future_value: float = 100.0,
    include_future: bool = True,
    extra_future_quotes: int = 0,
    base_ns: int = BASE,
    include_initial_missing: bool = False,
    grid_stop_second: int = 5,
) -> dict[str, Any]:
    """A fixed 1..4s decision grid; later quotes cannot change its geometry."""
    from histdatacom.forecasting.feature_artifacts import write_feature_artifact

    matrix = _revision_matrix(
        future_value=future_value,
        include_future=include_future,
        base_ns=base_ns,
    )
    artifact = write_feature_artifact(matrix, directory / "vintage")
    times = tuple(
        base_ns + i * SECOND
        for i in range(0 if include_initial_missing else 1, 8)
    ) + tuple(
        base_ns + 8 * SECOND + i * 1_000_000 for i in range(extra_future_quotes)
    )
    source, version = _source(
        directory,
        times,
        tuple(float(i + 1) for i in range(len(times))),
        derived=(str(artifact),),
    )
    ownership = build_training_ownership(source)
    macro = TrainingJoinSourceV1(
        "macro", JoinFamily.VINTAGE, paths=(str(artifact),)
    )
    column = bound_column(
        macro,
        MACRO,
        TrainingJoinEntityV1("EURUSD", series="preprocessing.macro"),
        "macro",
        coordinate="fixed-reference",
        meaning=JoinMeaning.SNAPSHOT,
    )
    grain = TrainingWideGrainV1(
        WideRowGrain.GRID,
        base_ns + (0 if include_initial_missing else SECOND),
        base_ns + grid_stop_second * SECOND,
        SECOND,
        0,
    )
    joins = tuple(
        TrainingJoinPlanV1(
            materialize_training_rows(
                source,
                ownership,
                TrainingRequestV1(
                    TrainingConsumerMode.DESCRIPTIVE,
                    cutoff,
                    cutoff + 1,
                    ("EURUSD",),
                    decision_time_ns=cutoff,
                ),
            ),
            JoinInformationMode.NORMALIZED_AS_OF,
            (macro,),
            (column,),
        )
        for cutoff in grain.cutoffs
    )
    plan = build_training_wide_plan(joins, grain=grain)
    view = materialize_training_wide_view(plan)
    return {
        "source": source,
        "version": version,
        "ownership": ownership,
        "split": chronological_split(ownership),
        "wide_plan": plan,
        "wide_view": view,
        "records": project_training_wide_view(view),
        "matrix": matrix,
        "artifact_path": artifact,
        "directory": directory,
        "base_ns": base_ns,
        "scope": "normalized_added_feature_research_not_causal_raw_quotes",
    }


def preprocessing_plan(
    native: dict[str, Any],
    *,
    family: str = "zscale",
    columns: tuple[str, ...] = (BID,),
    fit_unit_ids: tuple[str, ...] | None = None,
    fit_mode: str = "train_fit",
    fold_id: str = "fixed-outer-fold-0",
    options_json: str = "{}",
    rolling_window_ns: int | None = None,
) -> Any:
    """Construct the real public plan; this helper grants no fit authority."""
    from histdatacom.data_quality.training_preprocessing_contracts import (
        PreprocessingFitMode,
        TrainingPreprocessingPlanV1,
        TrainingPreprocessingStepV1,
    )

    if fit_unit_ids is None:
        first = next(
            assignment.evidence_unit_id
            for assignment in native["split"].assignments
            if assignment.partition is TemporalPartition.TRAIN
            and any(
                row["spine"] is not None
                and row["spine"]["evidence_unit_id"]
                == assignment.evidence_unit_id
                for row in native["records"]
            )
        )
        fit_unit_ids = (first,)
    return TrainingPreprocessingPlanV1(
        native["wide_plan"],
        native["split"],
        fold_id,
        PreprocessingFitMode(fit_mode),
        tuple(sorted(fit_unit_ids)),
        tuple(sorted(columns)),
        (
            TrainingPreprocessingStepV1(
                "fixed-step", family, columns, options_json
            ),
        ),
        rolling_window_ns,
    )


def build_preprocessing_sibling_fixture(directory: Path) -> dict[str, Any]:
    """Two actual native publications sharing original anchors and law.

    The existing tiny publisher executes native cross validation, delivery,
    staging and commit.  No reader/producer is mocked and no empirical
    qualification is asserted by its explicitly invented benchmark metadata.
    """
    from tests.fixtures.training_substrate_v1 import (
        TIMES,
        observed_source,
        published_product,
        with_products,
    )

    source, version = observed_source(directory / "observed")
    products = tuple(
        published_product(
            directory / member,
            version,
            member=member,
            seed=697,
            storage_version=3,
        )[0]
        for member in ("preprocessing-member-a", "preprocessing-member-b")
    )
    source = with_products(source, *products)
    ownership = build_training_ownership(source)
    split = chronological_split(ownership)
    joins = []
    for product in products:
        manifest = product.manifest
        batch = materialize_training_rows(
            source,
            ownership,
            TrainingRequestV1(
                TrainingConsumerMode.AUGMENTATION,
                TIMES[0],
                TIMES[1] + 1,
                ("EURUSD",),
                product_manifest_id=manifest.manifest_id,
            ),
        )
        quotes = TrainingJoinSourceV1(
            "quotes",
            JoinFamily.TICK,
            configuration_json=training_json(
                {
                    "product_manifest_id": manifest.manifest_id,
                    "ensemble_member_id": manifest.ensemble_member_id,
                }
            ),
        )
        column = bound_column(
            quotes,
            BID,
            TrainingJoinEntityV1("EURUSD"),
            "bid",
            direction=JoinDirection.PRIOR,
            max_age_ns=SECOND,
        )
        joins.append(
            TrainingJoinPlanV1(
                batch, JoinInformationMode.EX_POST, (quotes,), (column,)
            )
        )
    plan = build_training_wide_plan(
        tuple(joins), grain=TrainingWideGrainV1(WideRowGrain.PANEL)
    )
    view = materialize_training_wide_view(plan)
    selected_plan = build_training_wide_plan(
        (joins[0],), grain=TrainingWideGrainV1(WideRowGrain.PANEL)
    )
    return {
        "source": source,
        "version": version,
        "ownership": ownership,
        "split": split,
        "wide_plan": plan,
        "wide_view": view,
        "records": project_training_wide_view(view),
        "selected_plan": selected_plan,
        "products": products,
        "directory": directory,
    }


def build_preprocessing_calendar_fixture(
    directory: Path, *, train_gaps: bool = True
) -> dict[str, Any]:
    """Real replayed normalized STATE observations with exact-join gaps.

    Two declared releases per day, four quote coordinates per day, three
    separate day units.  TRAIN/VALIDATION/TEST are frozen by whole original
    day.  Each exact join omits the one-second-later coordinates deliberately;
    bounded prior-state imputation must preserve that native missingness.
    """
    from histdatacom.market_context.economic_calendar import (
        write_economic_calendar_corpus,
    )
    from tests.fixtures.forecast_contracts_v1 import (
        RELEASE_TIME,
        calendar_fixture,
        utc_text,
    )
    from tests.fixtures.training_join_v1 import controlled_spine

    corpus = calendar_fixture()
    template = next(
        release for release in corpus.releases if release.actual_value == 3.0
    )
    releases = []
    for day, values in enumerate(((3.0, 9.0), (4.0, 12.0), (2.0, 8.0))):
        for position, value in zip((0, 3), values):
            clock = RELEASE_TIME + day * DAY_NS + position * SECOND
            key = f"preprocessing-day-{day}-release-{position}"
            releases.append(
                replace(
                    template,
                    logical_event_key=key,
                    source_release_id=key,
                    source_request_id=key,
                    reference_period=key,
                    scheduled_for_ns=clock,
                    scheduled_lexical=utc_text(clock),
                    released_at_ns=clock,
                    released_lexical=utc_text(clock),
                    first_observed_at_ns=clock,
                    available_at_ns=clock,
                    actual_value=value,
                    actual_lexical=str(value),
                    revision_sequence=0,
                    supersedes_release_id=None,
                    release_id="",
                )
            )
    corpus = replace(
        corpus, releases=tuple(releases), forecasts=(), corpus_id=""
    )
    artifact = write_economic_calendar_corpus(corpus, directory / "calendar")
    clocks = tuple(
        RELEASE_TIME + day * DAY_NS + second * SECOND
        for day in range(3)
        for second in ((0, 1, 3, 4) if train_gaps or day else (0, 3))
    )
    batch = controlled_spine(directory / "spine", clocks)
    binding = TrainingJoinSourceV1(
        "calendar", JoinFamily.CALENDAR, paths=(str(artifact.path),)
    )
    name = "calendar.preprocessing.actual"
    column = bound_column(
        binding,
        name,
        TrainingJoinEntityV1("EURUSD", "USD", "US", "fixture.cpi.mom"),
        "actual_value",
    )
    join = TrainingJoinPlanV1(
        batch, JoinInformationMode.NORMALIZED_AS_OF, (binding,), (column,)
    )
    plan = build_training_wide_plan(
        (join,), grain=TrainingWideGrainV1(WideRowGrain.EVENT)
    )
    view = materialize_training_wide_view(plan)
    first_day = RELEASE_TIME // DAY_NS * DAY_NS
    split = TrainingTemporalSplitV1(
        batch.ownership.artifact_id,
        tuple(
            TrainingTemporalAssignmentV1(
                unit.artifact_id,
                (
                    TemporalPartition.TRAIN
                    if unit.start_ns < first_day + DAY_NS
                    else (
                        TemporalPartition.VALIDATION
                        if unit.start_ns < first_day + 2 * DAY_NS
                        else TemporalPartition.TEST
                    )
                ),
            )
            for unit in batch.ownership.units
        ),
    )
    return {
        "source": batch.source,
        "ownership": batch.ownership,
        "split": split,
        "wide_plan": plan,
        "wide_view": view,
        "records": project_training_wide_view(view),
        "column": name,
        "artifact_path": Path(artifact.path),
        "clocks": clocks,
        "values": ((3.0, None, 9.0, None) if train_gaps else (3.0, 9.0))
        + (
            4.0,
            None,
            12.0,
            None,
            2.0,
            None,
            8.0,
            None,
        ),
        "labels": ((0, 0, 1, 1) if train_gaps else (0, 1)) + (0, 0, 1, 1) * 2,
        "directory": directory,
        "scope": "fixed_neutral_learner_functionality_not_empirical_utility",
    }


def weighted_moments(
    values: tuple[Fraction, ...], weights: tuple[Fraction, ...]
) -> tuple[Fraction, Fraction]:
    """Independent elementary weighted population oracle, no fit imports."""
    assert len(values) == len(weights) and values
    assert all(weight >= 0 for weight in weights)
    total = sum(weights, Fraction())
    assert total > 0
    mean = sum((w * x for x, w in zip(values, weights)), Fraction()) / total
    variance = (
        sum((w * (x - mean) ** 2 for x, w in zip(values, weights)), Fraction())
        / total
    )
    return mean, variance


def weighted_quantile(
    values: tuple[Fraction, ...], weights: tuple[Fraction, ...], p: Fraction
) -> Fraction:
    """First positive-mass value reaching p of the complete retained mass."""
    assert len(values) == len(weights) and 0 <= p <= 1
    assert all(weight >= 0 for weight in weights)
    pairs = sorted((v, w) for v, w in zip(values, weights) if w > 0)
    total = sum((w for _, w in pairs), Fraction())
    assert total > 0
    cumulative = Fraction()
    for value, weight in pairs:
        cumulative += weight
        if cumulative >= p * total:
            return value
    raise AssertionError("positive finite weights must reach probability")


@pytest.fixture(scope="module")
def preprocessing_native(tmp_path_factory: Any) -> dict[str, Any]:
    return build_preprocessing_fixture(
        tmp_path_factory.mktemp("preprocessing-native")
    )


@pytest.fixture(scope="module")
def preprocessing_vintage(tmp_path_factory: Any) -> dict[str, Any]:
    return build_preprocessing_vintage_fixture(
        tmp_path_factory.mktemp("preprocessing-vintage")
    )


@pytest.fixture(scope="module")
def preprocessing_siblings(tmp_path_factory: Any) -> dict[str, Any]:
    return build_preprocessing_sibling_fixture(
        tmp_path_factory.mktemp("preprocessing-siblings")
    )


@pytest.fixture(scope="module")
def preprocessing_calendar(tmp_path_factory: Any) -> dict[str, Any]:
    return build_preprocessing_calendar_fixture(
        tmp_path_factory.mktemp("preprocessing-calendar")
    )
