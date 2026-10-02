"""Standalone installed-package synthetic science/provenance qualification.

Copy this one file to a neutral directory. Run with a clean installed host and
an empty output directory; no checkout or tests-package import is used. The
explicit --allow-source switch is only for developing this diagnostic fixture.
All Arrow rows, broker events, review terms, and clocks below are generated.
They are not provider data, provider permission, or empirical qualification.
"""

from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import replace
from pathlib import Path

import polars as pl

import histdatacom
from histdatacom.broker_capture import (
    BrokerAdapterMessageV1,
    BrokerCapturePriceTextSemantics,
    BrokerCaptureSessionV1,
    BrokerCaptureSourceTimestampSemantics,
    BrokerCaptureStoragePolicyV1,
    BrokerDeliveryFingerprintV2,
    SequenceBrokerCaptureAdapterV1,
    broker_fingerprint_sources,
    fit_broker_delivery_fingerprint,
    load_broker_delivery_fingerprint,
    verify_broker_fingerprint_sources,
    write_broker_delivery_fingerprint,
)
from histdatacom.broker_capture import (
    BrokerCaptureEventKind as Kind,
)
from histdatacom.broker_plugin_health.runtime_legacy import (
    capture_legacy_with_host_health,
)
from histdatacom.broker_plugin_policy import (
    BrokerLegacyCaptureV1,
    BrokerPolicyAcknowledgementV1,
    BrokerPolicyConstraintMode,
    BrokerPolicyConstraintV1,
    BrokerPolicyContextV1,
    BrokerPolicyDataClass,
    BrokerPolicyEvidenceKind,
    BrokerPolicyEvidenceV1,
    BrokerPolicyExecutionV1,
    BrokerPolicyOperation,
    BrokerPolicyReferenceV1,
    BrokerPolicyRetention,
    BrokerPolicyRuleV1,
    BrokerPolicyStatus,
    BrokerProviderOutputContractV1,
    BrokerProviderPolicyV1,
    provider_native_inputs,
    provider_policy_scope,
    read_broker_scientific_lineage,
    resolve_provider_subject,
)
from histdatacom.data_quality.training_artifacts import (
    read_training_artifact,
    write_training_artifact,
)
from histdatacom.data_quality.training_contracts import (
    TrainingConsumerMode,
    TrainingOrigin,
    TrainingRequestV1,
    TrainingSourceV1,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_views import materialize_training_rows
from histdatacom.datasets import (
    DatasetCatalog,
    DatasetDescriptorV1,
    DatasetOrigin,
    HistDataProviderAdapter,
    build_observed_dataset_version,
    histdata_cache_path,
)
from histdatacom.orchestration.reconstruction import artifact_ref_for_file
from histdatacom.synthetic import (
    BrokerTransferConfigV1,
    HistoricalCarvingConstraintSetV1,
    ReconstructionRunV1,
    ReconstructionWindowV1,
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    estimate_reconstruction_retention,
    eurusd_triangle_reconciliation_config,
    publish_reconstruction_group,
    read_reconstruction_streams,
    reconcile_cross_currency_window,
    render_broker_delivery,
)

BASE = 1_577_923_200_000_000_000
SECOND = 1_000_000_000
SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
QUOTES = {
    "EURUSD": (1.1999, 1.2001),
    "GBPUSD": (1.4999, 1.5001),
    "EURGBP": (1.1999 / 1.5001, 1.2001 / 1.4999),
}


class Clock:
    def __init__(self):
        self.wall = BASE
        self.monotonic = 10 * SECOND

    def sample(self):
        self.wall += 1_000_000
        self.monotonic += 1_000_000
        return self.wall, self.monotonic


class Review:
    def __init__(self, binding):
        terms = b"Generated installed smoke terms; not provider permission."
        reference = BrokerPolicyReferenceV1(
            "generated-terms",
            "installed-smoke-terms",
            hashlib.sha256(terms).hexdigest(),
            len(terms),
        )
        evidence = BrokerPolicyEvidenceV1(
            reference,
            binding.provider_id,
            "generated-terms",
            "1",
            "2020-01-01",
            "synthetic-issuer",
            1,
            BrokerPolicyEvidenceKind.DECLARED,
            "fixture:installed-smoke",
        )
        policy = BrokerProviderPolicyV1(
            "1.0.0",
            binding,
            "MIT",
            "MIT",
            (evidence.artifact_id,),
            tuple(
                BrokerPolicyRuleV1(
                    operation,
                    data_class,
                    BrokerPolicyStatus.ALLOWED,
                    (evidence.artifact_id,),
                    (
                        BrokerPolicyRetention.UNBOUNDED
                        if operation is BrokerPolicyOperation.RETAIN_LOCAL
                        else BrokerPolicyRetention.NOT_APPLICABLE
                    ),
                    None,
                )
                for operation in sorted(BrokerPolicyOperation)
                for data_class in sorted(BrokerPolicyDataClass)
            ),
            tuple(
                BrokerPolicyConstraintV1(name, BrokerPolicyConstraintMode.ANY)
                for name in (
                    "account_class",
                    "commercial_use",
                    "eligibility",
                    "feed_type",
                    "geography",
                )
            ),
            (),
            2,
            2,
            2**63 - 1,
        )
        acknowledgement = BrokerPolicyAcknowledgementV1(
            policy.artifact_id,
            (evidence.artifact_id,),
            reference,
            3,
            2**63 - 1,
        )
        self.context = BrokerPolicyContextV1(
            (policy,),
            (evidence,),
            (acknowledgement,),
            (),
            (policy.artifact_id,),
            BrokerPolicyExecutionV1(False, "synthetic", "fixture", "fixture"),
        )

    def read_policy_context(self):
        return self.context


def capture(root):
    session = BrokerCaptureSessionV1(
        adapter_id="fixture.installed-science",
        adapter_version="1.0.0",
        adapter_config_sha256=hashlib.sha256(
            b"generated-science-config"
        ).hexdigest(),
        protocol="generated",
        environment_id="synthetic",
        server_id="fixture",
        started_at_utc_ns=BASE,
        started_at_monotonic_ns=10 * SECOND,
    )
    messages = [
        BrokerAdapterMessageV1(
            kind=Kind.PROCESS_START, reason_code="collector_started"
        ),
        BrokerAdapterMessageV1(
            kind=Kind.CONNECTION_OPEN, connection_id="generated"
        ),
        BrokerAdapterMessageV1(
            kind=Kind.SUBSCRIPTION_ADD,
            connection_id="generated",
            subscription_id="eurusd",
            symbol="EURUSD",
        ),
    ]
    for index in range(4):
        bid = f"{1.1 + index * 0.0001:.4f}"
        ask = f"{float(bid) + 0.0002:.4f}"
        messages.append(
            BrokerAdapterMessageV1(
                kind=Kind.QUOTE,
                symbol="EURUSD",
                bid=float(bid),
                ask=float(ask),
                bid_text=bid,
                ask_text=ask,
                source_event_time_ns=BASE + index * 1_000_000,
                source_timestamp_semantics=BrokerCaptureSourceTimestampSemantics.BROKER_EVENT,
                source_timestamp_precision_ns=1_000_000,
                source_sequence=index,
                source_message_id=f"generated-{index}",
                price_text_semantics=BrokerCapturePriceTextSemantics.SOURCE_LEXEME,
            )
        )
    messages.append(
        BrokerAdapterMessageV1(
            kind=Kind.PROCESS_STOP, reason_code="collector_stopped"
        )
    )
    request = BrokerLegacyCaptureV1(
        session,
        BrokerProviderOutputContractV1(
            "legacy-capture-v1",
            tuple(sorted({message.kind.value for message in messages})),
            allow_raw_hashes=False,
            allow_opaque_metadata=False,
            allow_private_account_metadata=False,
        ),
    )
    review = Review(resolve_provider_subject(request).bindings[0])
    with provider_policy_scope(review):
        result = capture_legacy_with_host_health(
            root,
            provider_request=request,
            adapter=SequenceBrokerCaptureAdapterV1(
                session.adapter_id, session.adapter_version, tuple(messages)
            ),
            clock=Clock(),
            storage_policy=BrokerCaptureStoragePolicyV1(
                max_partition_events=32, fsync_each_event=True
            ),
            symbols=("EURUSD",),
        )
        fingerprint = fit_broker_delivery_fingerprint(
            root,
            (result.manifest,),
            provider_requests=(request,),
            effective_start_utc_ns=0,
        )
    return fingerprint, review


def observed_source(output):
    root = output / "generated-observed" / "ASCII" / "T"
    times = (BASE + SECOND, BASE + 3 * SECOND)
    adapter = HistDataProviderAdapter()
    for symbol in SYMBOLS:
        bid, ask = QUOTES[symbol]
        frame = pl.DataFrame(
            {
                "datetime": [time // 1_000_000 for time in times],
                "bid": [bid, bid],
                "ask": [ask, ask],
                "vol": [0, 0],
            },
            schema={
                "datetime": pl.Int64,
                "bid": pl.Float64,
                "ask": pl.Float64,
                "vol": pl.Int32,
            },
        )
        path = histdata_cache_path(root, symbol, "202001")
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_ipc(path)
    evidence = output / "generated-only.json"
    evidence.write_text(
        '{"generated":true,"empirical":false}', encoding="utf-8"
    )
    descriptor = DatasetDescriptorV1(
        "installed-science-generated",
        "Generated installed smoke",
        "Generated rows through the observed-source API; not empirical data.",
        (DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        root,
        descriptor,
        symbols=SYMBOLS,
        periods=("202001",),
        qualification_evidence=(
            artifact_ref_for_file(evidence, kind="generated-only"),
        ),
    )
    catalog = DatasetCatalog(
        (adapter.provider,), (adapter.descriptor,), (descriptor,), (version,)
    )
    return (
        TrainingSourceV1(catalog.to_json(), version.dataset_version_id),
        version,
    )


def product(output, fingerprint, version):
    config = eurusd_triangle_reconciliation_config()
    constraints = HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id="generated"
    )
    run = ReconstructionRunV1(
        SYMBOLS,
        (version.dataset_version_id,),
        (config.config_id,),
        ("generated-member",),
        626,
    )
    window = ReconstructionWindowV1(
        run.run_id,
        "generated-member",
        SYMBOLS,
        BASE + SECOND,
        BASE + 3 * SECOND + 1,
    )
    streams = []
    for symbol in SYMBOLS:
        partition = next(
            item for item in version.partitions if item.symbol == symbol
        )
        bid, ask = QUOTES[symbol]
        anchors = tuple(
            SyntheticEventV1.observed(
                symbol=symbol,
                event_time_ns=BASE + second * SECOND,
                event_sequence=0,
                bid=bid,
                ask=ask,
                run_id=run.run_id,
                ensemble_member_id="generated-member",
                source_version_id=version.dataset_version_id,
                source_series_id=partition.series_id,
                source_period="202001",
                source_row_id=index + 1,
            )
            for index, second in enumerate((1, 3))
        )
        middle = SyntheticEventV1.generated(
            symbol=symbol,
            event_time_ns=BASE + 2 * SECOND,
            event_sequence=1,
            bid=bid,
            ask=ask,
            run_id=run.run_id,
            ensemble_member_id="generated-member",
            source_version_id=version.dataset_version_id,
            left_anchor_event_id=anchors[0].event_id,
            right_anchor_event_id=anchors[1].event_id,
            generator_id="generated",
            generator_version="1.0.0",
            generator_config_id="generated-config",
            constraint_set_id=constraints.constraint_set_id,
        )
        streams.append(
            SyntheticEventStreamV1(
                run.run_id,
                "generated-member",
                symbol,
                (anchors[0], middle, anchors[1]),
            )
        )
    group = reconcile_cross_currency_window(
        run=run,
        window=window,
        streams={item.symbol: item for item in streams},
        config=config,
    )
    rendered = render_broker_delivery(
        run=run,
        window=window,
        group=group,
        fingerprint=fingerprint,
        constraints=constraints,
        selected_at_utc_ns=0,
        config=BrokerTransferConfigV1(
            strength=0.0, apply_exact_duplicates=False
        ),
        quality_period="202001",
    )
    assert len(rendered.streams) == 3
    anchors = tuple(
        event
        for stream in group.streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    )
    retention = estimate_reconstruction_retention(
        run_id=run.run_id,
        primary_member_id="generated-member",
        retained_member_event_counts={"generated-member": 9},
        estimated_partition_count=9,
        storage_policy=run.storage_policy,
    )
    result = publish_reconstruction_group(
        output / "product",
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="generated",
        retention_plan=retention,
        storage_policy=run.storage_policy,
    )
    return result, rendered


def refuses(call):
    try:
        call()
    except (ValueError, OSError):
        return
    raise AssertionError("tampered or missing retained lineage was accepted")


def main():
    allow_source = "--allow-source" in sys.argv[2:]
    assert (
        allow_source or "site-packages" in histdatacom.__file__
    ), histdatacom.__file__
    assert not any(
        name == "tests" or name.startswith("tests.") for name in sys.modules
    )
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    assert not any(output.iterdir()), "use an empty generated output directory"
    capture_root = output / "capture"
    fingerprint, review = capture(capture_root)
    assert type(fingerprint) is BrokerDeliveryFingerprintV2
    with (
        provider_policy_scope(review),
        broker_fingerprint_sources(capture_root),
        provider_native_inputs(fingerprint),
    ):
        fingerprint_path = output / "fingerprint.json"
        write_broker_delivery_fingerprint(fingerprint_path, fingerprint)
        assert load_broker_delivery_fingerprint(fingerprint_path) == fingerprint
        source, version = observed_source(output)
        published, rendered = product(output, fingerprint, version)
        assert (
            read_reconstruction_streams(published.manifest_path)
            == rendered.streams
        )
        parents = read_broker_scientific_lineage(published.manifest_path)
        assert parents.fingerprints == (fingerprint,)
        source = replace(
            source, product_manifest_paths=(str(published.manifest_path),)
        )
        batch = materialize_training_rows(
            source,
            build_training_ownership(source),
            TrainingRequestV1(
                TrainingConsumerMode.DESCRIPTIVE,
                BASE,
                BASE + 4 * SECOND,
                SYMBOLS,
                product_manifest_id=published.manifest.manifest_id,
            ),
        )
        assert len(batch.rows) == 9
        assert {item.origin for item in batch.rows} == {
            TrainingOrigin.OBSERVED,
            TrainingOrigin.BROKER_CONDITIONED_COUNTERFACTUAL,
        }
        training_path = write_training_artifact(batch, output / "training")
        assert read_broker_scientific_lineage(training_path).fingerprints == (
            fingerprint,
        )
    # Reopen complete persisted parents in fresh scopes, not a retained permit.
    restored = read_broker_scientific_lineage(training_path).fingerprints[0]
    with (
        provider_policy_scope(review),
        broker_fingerprint_sources(capture_root),
        provider_native_inputs(restored),
    ):
        reopened = read_training_artifact(
            training_path, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
        )
        assert reopened == batch
        lineage_path = training_path.with_name(
            training_path.name + ".broker-provenance.json"
        )
        original = lineage_path.read_bytes()
        receipt_path = training_path.with_name(
            training_path.name + ".provider-policy.json"
        )
        receipt_bytes = receipt_path.read_bytes()
        for missing in (
            (receipt_path,),
            (lineage_path,),
            (receipt_path, lineage_path),
        ):
            for companion in missing:
                companion.unlink()
            refuses(
                lambda: read_training_artifact(
                    training_path,
                    consumer_mode=TrainingConsumerMode.DESCRIPTIVE,
                )
            )
            receipt_path.write_bytes(receipt_bytes)
            lineage_path.write_bytes(original)
        lineage_path.write_bytes(b"{}")
        refuses(
            lambda: read_training_artifact(
                training_path, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
            )
        )
        lineage_path.write_bytes(original)
        assert verify_broker_fingerprint_sources(restored) == restored
        chain_path = next(
            path
            for path in capture_root.rglob("chain.jsonl")
            if "provenance" in path.parent.name
        )
        with chain_path.open("ab") as stream:
            stream.write(b'{"partial":')
        refuses(
            lambda: read_training_artifact(
                training_path, consumer_mode=TrainingConsumerMode.DESCRIPTIVE
            )
        )
    assert not any(
        name == "tests" or name.startswith("tests.") for name in sys.modules
    )
    print(
        json.dumps(
            {
                "installed": not allow_source,
                "host": histdatacom.__file__,
                "fingerprint_schema": fingerprint.schema_version,
                "fingerprint_id": fingerprint.fingerprint_id,
                "capture_roots": [
                    item.seal.root_sha256 for item in fingerprint.capture_roots
                ],
                "product_id": published.manifest.manifest_id,
                "training_rows": len(batch.rows),
                "persisted_lineage_reopened": True,
                "lineage_tamper_refused": True,
                "missing_companions_refused": True,
                "source_chain_tail_refused": True,
                "generated_only": True,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
