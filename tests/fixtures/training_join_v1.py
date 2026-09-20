"""Generated quote/vintage fixtures for the public join consumer, not markets."""

from histdatacom.data_quality.training_contracts import (
    TrainingConsumerMode,
    TrainingRequestV1,
    training_json,
)
from histdatacom.data_quality.training_join_contracts import (
    JoinDirection,
    JoinFamily,
    JoinInformationMode,
    JoinMeaning,
    TrainingJoinColumnV1,
    TrainingJoinEntityV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
    training_join_parent_namespaces,
)
from histdatacom.data_quality.training_views import materialize_training_rows
from tests.fixtures.training_substrate_v1 import BASE
from tests.fixtures.training_temporal_v1 import SECOND, temporal_source


def join_fixture(
    tmp_path, *, matrix=False, decision=None, duplicate=False, empty=False
):
    source, ownership, _ = temporal_source(
        tmp_path, matrix=matrix, duplicate_crossed=duplicate
    )
    batch = materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE,
            BASE + (100 if empty else 1) * SECOND,
            (
                min(BASE + (101 if empty else 4) * SECOND, decision + 1)
                if decision is not None
                else BASE + (101 if empty else 4) * SECOND
            ),
            ("EURUSD",),
            decision_time_ns=decision,
        ),
    )
    binding = TrainingJoinSourceV1(
        "quotes",
        JoinFamily.TICK,
        configuration_json=training_json(
            {"product_manifest_id": None, "ensemble_member_id": None}
        ),
    )
    column = TrainingJoinColumnV1(
        "market.tick.EURUSD.bid",
        "quotes",
        TrainingJoinEntityV1("EURUSD"),
        "bid",
        JoinDirection.EXACT,
        JoinMeaning.STATE,
        0,
    )
    from dataclasses import replace

    column = replace(
        column,
        parent_namespaces=training_join_parent_namespaces(
            binding.family, column
        ),
    )
    return TrainingJoinPlanV1(
        batch, JoinInformationMode.EX_POST, (binding,), (column,)
    )


def vintage_join(plan):
    binding = TrainingJoinSourceV1(
        "macro",
        JoinFamily.VINTAGE,
        paths=plan.spine.source.derived_artifact_paths,
    )
    column = TrainingJoinColumnV1(
        "calendar.macro.level",
        "macro",
        TrainingJoinEntityV1("EURUSD", series="macro"),
        "macro",
        JoinDirection.EXACT,
        JoinMeaning.SNAPSHOT,
        0,
        coordinate="1",
    )
    from dataclasses import replace

    column = replace(
        column,
        parent_namespaces=training_join_parent_namespaces(
            binding.family, column
        ),
    )
    return TrainingJoinPlanV1(
        plan.spine, JoinInformationMode.NORMALIZED_AS_OF, (binding,), (column,)
    )


def bound_column(
    source,
    name,
    entity,
    field,
    *,
    coordinate="",
    meaning=JoinMeaning.STATE,
    direction=JoinDirection.EXACT,
    max_age_ns=0,
):
    from dataclasses import replace

    column = TrainingJoinColumnV1(
        name,
        source.key,
        entity,
        field,
        direction,
        meaning,
        max_age_ns,
        coordinate=coordinate,
    )
    return replace(
        column,
        parent_namespaces=training_join_parent_namespaces(
            source.family, column
        ),
    )


def controlled_spine(tmp_path, clocks, *, derived=(), decision=None):
    """Actual canonical IPC at controlled clocks, with no historical claim."""
    from datetime import datetime, timezone
    import polars as pl
    from histdatacom.datasets import (
        DatasetCatalog,
        DatasetDescriptorV1,
        DatasetOrigin,
        HistDataProviderAdapter,
        build_observed_dataset_version,
        histdata_cache_path,
    )
    from histdatacom.orchestration.reconstruction import artifact_ref_for_file
    from histdatacom.data_quality.training_contracts import TrainingSourceV1
    from histdatacom.data_quality.training_lineage import (
        build_training_ownership,
    )

    adapter = HistDataProviderAdapter()
    root = tmp_path / "ASCII" / "T"
    from histdatacom.forecasting.feature_artifacts import read_feature_artifact
    from histdatacom.data_quality.training_lineage import _feature_span

    all_clocks = set(clocks)
    for artifact in derived:
        lo, hi = _feature_span(read_feature_artifact(artifact))
        all_clocks.update((lo, hi - 1))
    groups = {}
    for time in sorted(all_clocks):
        period = datetime.fromtimestamp(time // SECOND, timezone.utc).strftime(
            "%Y%m"
        )
        groups.setdefault(period, []).append(time)
    for period, times in groups.items():
        path = histdata_cache_path(root, "EURUSD", period)
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "datetime": [t // 1_000_000 for t in times],
                "bid": [1.0] * len(times),
                "ask": [1.0002] * len(times),
                "vol": [0] * len(times),
            },
            schema={
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            },
        ).write_ipc(path)
    evidence = tmp_path / "controlled-simulation.json"
    evidence.write_text('{"controlled_synthetic_quotes_not_historical":true}')
    descriptor = DatasetDescriptorV1(
        "join-controlled",
        "Join controlled quotes",
        "Generated synthetic clock fixtures only",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=("EURUSD",),
        periods=tuple(sorted(groups)),
        qualification_evidence=(
            artifact_ref_for_file(evidence, kind="fixture"),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    source = TrainingSourceV1(
        catalog.to_json(),
        version.dataset_version_id,
        derived_artifact_paths=tuple(sorted(str(p) for p in derived)),
    )
    ownership = build_training_ownership(source)
    return materialize_training_rows(
        source,
        ownership,
        TrainingRequestV1(
            TrainingConsumerMode.DESCRIPTIVE,
            min(clocks),
            max(clocks) + 1,
            ("EURUSD",),
            decision_time_ns=decision,
        ),
    )


def native_observed_source(tmp_path, times):
    """Actual three-leg IPC with arbitrary controlled January clock geometry."""
    import polars as pl
    from histdatacom.datasets import (
        DatasetCatalog,
        DatasetDescriptorV1,
        DatasetOrigin,
        HistDataProviderAdapter,
        build_observed_dataset_version,
        histdata_cache_path,
    )
    from histdatacom.orchestration.reconstruction import artifact_ref_for_file
    from histdatacom.data_quality.training_contracts import TrainingSourceV1
    from tests.fixtures.training_substrate_v1 import SYMBOLS, QUOTES

    root = tmp_path / "ASCII" / "T"
    adapter = HistDataProviderAdapter()
    for symbol in SYMBOLS:
        bid, ask = QUOTES[symbol]
        path = histdata_cache_path(root, symbol, "202001")
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "datetime": [t // 1_000_000 for t in times],
                "bid": [bid] * len(times),
                "ask": [ask] * len(times),
                "vol": [0] * len(times),
            },
            schema={
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            },
        ).write_ipc(path)
    evidence = tmp_path / "controlled-simulation.json"
    evidence.write_text('{"controlled_synthetic_quotes_not_historical":true}')
    descriptor = DatasetDescriptorV1(
        "join-native-controlled",
        "Join native controlled quotes",
        "Generated synthetic three-leg fixtures only",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=SYMBOLS,
        periods=("202001",),
        qualification_evidence=(
            artifact_ref_for_file(evidence, kind="fixture"),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    return (
        TrainingSourceV1(catalog.to_json(), version.dataset_version_id),
        version,
    )
