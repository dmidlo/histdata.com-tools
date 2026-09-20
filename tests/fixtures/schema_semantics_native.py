"""Small current native artifacts; generated quotes only, no actual sources."""

from pathlib import Path

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityWorkflowV1,
    broker_capability_catalog,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_lifecycle.contracts import (
    BrokerLifecycleCompletion,
    BrokerLifecycleHeaderV1,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePolicyV1,
    BrokerLifecycleState,
)
from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    BrokerPluginInventoryV1,
)
from histdatacom.broker_plugins import (
    BrokerEventKind,
    BrokerEventV1,
    BrokerPluginMetadataV1,
    BrokerQuoteV1,
    BrokerReceiveTimeV1,
    BrokerSessionV1,
    BrokerSourceTimeSemantics,
    BrokerSourceTimeV1,
)
from histdatacom.data_quality.training_temporal_views import (
    materialize_training_temporal,
)
from histdatacom.forecasting import ForecastScoreV1
from histdatacom.reconstruction_evidence import (
    PointInTimeEvidenceProjectionV1,
    ReconstructionEvidenceInformationMode,
    ReconstructionEvidenceReadiness,
)
from histdatacom.synthetic.contracts import (
    SyntheticEventStreamV1,
    SyntheticEventV1,
)
from tests.fixtures.broker_capability_wheel import capability_registration
from tests.fixtures.forecast_contracts_v1 import (
    calendar_fixture,
    snapshot_fixture,
)
from tests.fixtures.training_temporal_v1 import temporal_plan, temporal_source


def native_artifacts(directory: Path) -> dict[str, object]:
    event = SyntheticEventV1.observed(
        symbol="EURUSD",
        event_time_ns=1_000_000_001,
        event_sequence=0,
        bid=1.0,
        ask=1.001,
        run_id="fixture-run",
        ensemble_member_id="member-1",
        source_version_id="source-1",
        source_series_id="series-1",
        source_period="202001",
        source_row_id=1,
    )
    stream = SyntheticEventStreamV1(
        "fixture-run", "member-1", "EURUSD", (event,)
    )
    projection = PointInTimeEvidenceProjectionV1(
        evidence_window_id="window-1",
        source_provider_id="fixture-provider",
        source_partition_id="partition-1",
        source_artifact_id="artifact-1",
        source_artifact_sha256="a" * 64,
        symbol="EURUSD",
        period="202001",
        support_start_ns=0,
        support_end_ns=10_000_000_000,
        available_at_ns=11_000_000_000,
        as_of_ns=12_000_000_000,
        information_mode=ReconstructionEvidenceInformationMode.EX_POST_RECONSTRUCTION,
        policy_id="policy-1",
        row_records=(),
        sidecar_records=(),
        status=ReconstructionEvidenceReadiness.READY,
        limitations=("Synthetic test evidence only.",),
        omitted_record_count=0,
    )
    metadata = BrokerPluginMetadataV1(
        "org.example.semantic", "1.0.0", "Synthetic plugin"
    )
    session = BrokerSessionV1(metadata.artifact_id, "a" * 32, 100, "clock-1")
    broker = BrokerEventV1(
        session.artifact_id,
        0,
        BrokerEventKind.QUOTE,
        "connection-1",
        instrument="EURUSD",
        quote=BrokerQuoteV1("EURUSD", "1.00000", "1.00100"),
        source_time=BrokerSourceTimeV1(
            100, 1, BrokerSourceTimeSemantics.BROKER_EVENT
        ),
        receive_time=BrokerReceiveTimeV1(101, 10, "clock-1"),
    )
    registration = capability_registration(
        tuple(
            item.capability_id
            for item in broker_capability_catalog().definitions
        )
    )
    inventory = BrokerPluginInventoryV1(
        (BrokerPluginCandidateV1(registration, "a" * 64, "b" * 64),)
    )
    plan = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(
            ("configuration_schema", "iter_events", "open_session")
        ),
        plugin_id=registration.plugin_id,
    )
    manifest = BrokerLifecycleManifestV1(
        BrokerLifecycleHeaderV1(
            inventory, plan, BrokerLifecyclePolicyV1(), (), "a" * 32, "3.10.19"
        ),
        BrokerLifecycleState.DISCOVERED,
        BrokerLifecycleCompletion.OPEN,
    )
    corpus = calendar_fixture()
    snapshot = snapshot_fixture(corpus)
    score = ForecastScoreV1(
        snapshot.to_json(), corpus.to_json(), corpus.coverage_end_ns
    )
    source, ownership, _ = temporal_source(directory)
    batch = materialize_training_temporal(temporal_plan(source, ownership))
    return {
        "synthetic-event-v1": event,
        "synthetic-stream-v1": stream,
        "evidence-projection-v1": projection,
        "broker-metadata-v1": metadata,
        "broker-event-v1": broker,
        "broker-lifecycle-manifest-v1": manifest,
        "economic-release-v1": corpus.releases[1],
        "forecast-score-v1": score,
        "training-temporal-batch-v1": batch,
    }
