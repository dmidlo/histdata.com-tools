"""Genuine generated native sources, bounded projection and material gates."""

from dataclasses import replace
from decimal import Decimal
import json
import shutil

import pytest

from histdatacom.broker_capture import (
    BrokerDeliveryFingerprintV3,
    BrokerLifecycleFingerprintFitConfigV1,
    assess_broker_capture_eligibility,
    broker_fingerprint_sources,
    fit_broker_delivery_fingerprint,
    load_broker_delivery_fingerprint,
    parse_broker_delivery_fingerprint,
    verify_broker_fingerprint_sources,
    write_broker_delivery_fingerprint,
)
from histdatacom.broker_plugin_capabilities import BrokerAdmittedEventV1
from histdatacom.broker_plugin_lifecycle import replay_broker_lifecycle
from histdatacom.broker_plugin_policy import (
    BrokerPolicyOperation,
    require_provider_operation,
)
from tests.fixtures.broker_provider_policy import generated_provider_scope
from tests.fixtures.broker_sdk_science import qualified_sdk_capture


@pytest.fixture(scope="module")
def native_science(tmp_path_factory):
    return qualified_sdk_capture(tmp_path_factory.mktemp("sdk-native-science"))


def _copy_sources(result, target):
    for source in (
        result.directory,
        result.health_directory,
        result.provenance_directory,
    ):
        shutil.copytree(source, target / source.name)
    return target / result.directory.name


@pytest.mark.parametrize("mode", ("honest", "source", "mixed"))
def test_scientific_fixture_uses_receive_clock_for_declared_source_times(
    monkeypatch, mode
):
    from itertools import count
    from types import SimpleNamespace

    from histdatacom.broker_plugins import BrokerSourceTimeSemantics
    from tests.fixtures import broker_science_external

    # Constructor-only synthetic clock; this does not claim host qualification.
    utc = count(100_000_000_000, 5_000_000_000)
    monotonic = count(10_000_000_000, 5_000_000_000)
    monkeypatch.setattr(
        broker_science_external,
        "time",
        SimpleNamespace(
            time_ns=lambda: next(utc), monotonic_ns=lambda: next(monotonic)
        ),
    )
    fixture = broker_science_external.ScienceFixture()
    session = fixture.open_session({"mode": mode})
    quotes = tuple(
        e for e in fixture.iter_events(session) if e.quote is not None
    )
    assert len(quotes) == 3
    if mode == "honest":
        assert all(event.source_time is None for event in quotes)
    else:
        assert all(
            event.source_time.timestamp_ns == event.receive_time.utc_ns
            for event in quotes
        )
        assert tuple(event.source_time.semantics for event in quotes) == (
            BrokerSourceTimeSemantics.BROKER_EVENT,
            BrokerSourceTimeSemantics.BROKER_EVENT,
            (
                BrokerSourceTimeSemantics.ADAPTER_RECEIVE
                if mode == "mixed"
                else BrokerSourceTimeSemantics.BROKER_EVENT
            ),
        )


def test_scientific_fixture_policy_is_bounded_without_delivery_retries():
    from tests.fixtures.broker_sdk_science import scientific_lifecycle_policy

    policy = scientific_lifecycle_policy()
    assert policy.acknowledgement_timeout_ms == 10000
    assert policy.delivery_retries == 0
    assert policy.retry_delays_ms == ()
    assert policy.startup_timeout_ms == 15000
    assert policy.run_timeout_ms == 120000


def test_native_fit_configuration_rejects_duplicate_keys_and_boolean_counts():
    config = BrokerLifecycleFingerprintFitConfigV1()
    assert (
        BrokerLifecycleFingerprintFitConfigV1.from_json(config.to_json())
        == config
    )
    duplicate = '{"min_quote_events":999,' + config.to_json()[1:]
    with pytest.raises(ValueError):
        BrokerLifecycleFingerprintFitConfigV1.from_json(duplicate)
    with pytest.raises(ValueError):
        BrokerLifecycleFingerprintFitConfigV1(min_quote_events=True)


@pytest.mark.parametrize("public_dispatch", (False, True))
@pytest.mark.parametrize(
    ("count", "config"),
    (
        (17, None),
        (2, BrokerLifecycleFingerprintFitConfigV1(max_capture_manifests=1)),
    ),
)
def test_native_manifest_count_admission_precedes_hashing_and_family_scan(
    tmp_path, public_dispatch, count, config
):
    from histdatacom.broker_capture import BrokerDeliveryResourceLimitError
    from histdatacom.broker_capture.lifecycle_fingerprints import (
        fit_lifecycle_delivery_fingerprint,
    )
    from histdatacom.broker_plugin_lifecycle import BrokerLifecycleManifestV1

    # Only the exact-family sentinel may be inspected; it has no hashable
    # fields. Any inventory traversal before the count gate is a test failure.
    sentinel = object.__new__(BrokerLifecycleManifestV1)

    class OverLimitInventory:
        def __len__(self):
            return count

        def __getitem__(self, index):
            assert public_dispatch and index == 0
            return sentinel

        def __iter__(self):
            raise AssertionError("inventory traversed before count admission")

    fitter = (
        fit_broker_delivery_fingerprint
        if public_dispatch
        else fit_lifecycle_delivery_fingerprint
    )
    with pytest.raises(
        BrokerDeliveryResourceLimitError,
        match="capture manifests exceed max_capture_manifests",
    ):
        fitter(
            tmp_path,
            OverLimitInventory(),
            provider_requests=OverLimitInventory(),
            config=config,
        )


@pytest.mark.parametrize(
    "violation", ("configuration", "missing_requests", "request_count")
)
def test_native_dispatch_rejects_bad_admission_before_inventory_traversal(
    tmp_path, violation
):
    from histdatacom.broker_capture import BrokerDeliveryFitConfigV1
    from histdatacom.broker_plugin_lifecycle import BrokerLifecycleManifestV1

    sentinel = object.__new__(BrokerLifecycleManifestV1)

    class UnscannableInventory:
        def __init__(self, count):
            self.count = count

        def __len__(self):
            return self.count

        def __getitem__(self, index):
            assert index == 0
            return sentinel

        def __iter__(self):
            raise AssertionError("inventory traversed before count admission")

    config = (
        BrokerDeliveryFitConfigV1() if violation == "configuration" else None
    )
    requests = (
        None
        if violation == "missing_requests"
        else UnscannableInventory(
            1_000_000 if violation == "request_count" else 1
        )
    )
    message = (
        "inventories differ or repeat"
        if violation == "request_count"
        else "exact SDK inputs"
    )
    with pytest.raises(ValueError, match=message):
        fit_broker_delivery_fingerprint(
            tmp_path,
            UnscannableInventory(1),
            provider_requests=requests,
            config=config,
        )


def test_actual_native_projection_matches_independent_quote_oracle(
    native_science,
):
    result, request, fp = native_science
    with generated_provider_scope(request, capture_roots=(result.directory,)):
        records = tuple(
            replay_broker_lifecycle(result.directory, provider_request=request)
        )
        quotes = [
            (r, BrokerAdmittedEventV1.from_json(r.payload_json).event)
            for r in records
            if r.kind == "event"
        ]
        quotes = [(r, e) for r, e in quotes if e.quote is not None]
        assert len(quotes) == 3
        spread = (
            sum(
                float(Decimal(e.quote.ask) - Decimal(e.quote.bid))
                for _, e in quotes
            )
            / 3
        )
        deltas = [
            b[0].receive_monotonic_ns - a[0].receive_monotonic_ns
            for a, b in zip(quotes, quotes[1:])
        ]
        for cell in fp.cells:
            metrics = {m.name: m for m in cell.metrics}
            assert cell.support_count == 3
            assert metrics["spread"].estimate == pytest.approx(spread)
            assert metrics["spread"].support_count == 3
            assert metrics["price_decimal_places"].estimate == 5
            assert metrics["quote_interarrival_ns"].estimate == sum(deltas) / 2
            for name in (
                "source_timestamp_precision_ns",
                "source_batch_quote_count",
            ):
                assert metrics[name].estimate is None
                assert metrics[name].support_count == 0
                assert metrics[name].limitations
        assert fp.support_start_utc_ns == min(
            r.receive_utc_ns for r, _ in quotes
        )
        assert fp.support_end_utc_ns == max(r.receive_utc_ns for r, _ in quotes)
        decision = assess_broker_capture_eligibility(
            result.directory, result.manifest, provider_request=request
        )
        assert decision == fp.eligibility_decisions[0]
        assert (
            fit_broker_delivery_fingerprint(
                result.directory,
                (result.manifest,),
                provider_requests=(request,),
            )
            == fp
        )
    assert type(fp) is BrokerDeliveryFingerprintV3
    assert not hasattr(fp, "protocol") and not hasattr(fp, "server_id")
    assert not hasattr(fp, "statistics")
    assert fp.capture_roots[0].compatibility_json is not None
    assert parse_broker_delivery_fingerprint(fp.to_json() + "\n") == fp


def test_offline_source_use_needs_neither_worker_nor_discovery(
    native_science, monkeypatch
):
    result, request, fp = native_science
    import histdatacom.broker_plugin_registry as registry

    monkeypatch.setattr(
        registry,
        "discover_broker_plugins",
        lambda *a, **k: pytest.fail(
            "offline source replay attempted discovery"
        ),
    )
    with generated_provider_scope(request, capture_roots=(result.directory,)):
        assert verify_broker_fingerprint_sources(fp) == fp


def test_native_persistence_and_cli_roundtrip(native_science, tmp_path, capsys):
    from histdatacom.broker_capture.scientific_cli import main

    result, request, fp = native_science
    path = tmp_path / "fingerprint.json"
    with generated_provider_scope(
        request, capture_roots=(result.directory,)
    ) as source:
        write_broker_delivery_fingerprint(path, fp)
        assert load_broker_delivery_fingerprint(path) == fp
        context_path = tmp_path / "current-policy.json"
        context_path.write_text(source.current.to_json())
    assert main(["inspect", str(path)]) == 0
    assert (
        json.loads(capsys.readouterr().out)["material_admission"]
        == "not_checked"
    )
    assert (
        main(
            [
                "verify",
                str(path),
                "--source",
                str(result.directory),
                "--policy-context",
                str(context_path),
            ]
        )
        == 0
    )
    assert (
        json.loads(capsys.readouterr().out)["empirical_qualification"]
        == "not_attested"
    )
    output = tmp_path / "cli-fingerprint.json"
    assert (
        main(
            [
                "fit",
                str(result.directory),
                "--invocation",
                str(result.health_directory / "invocation.json"),
                "--policy-context",
                str(context_path),
                "--output",
                str(output),
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert load_broker_delivery_fingerprint(output) == fp


@pytest.mark.parametrize(
    "change",
    [
        "partition",
        "manifest",
        "missing-health",
        "missing-seal",
        "foreign",
        "symlink",
    ],
)
def test_material_use_rejects_current_native_byte_tampering(
    native_science, tmp_path, change
):
    result, request, fp = native_science
    directory = _copy_sources(result, tmp_path)
    if change == "partition":
        path = next(directory.glob("partition-*"))
        path.write_bytes(path.read_bytes() + b" ")
    elif change == "manifest":
        path = directory / "manifest.json"
        path.write_text(
            path.read_text().replace(
                '"worker_reaped":true', '"worker_reaped":false'
            )
        )
    elif change == "missing-health":
        (tmp_path / result.health_directory.name / "audit.json").unlink()
    elif change == "missing-seal":
        (tmp_path / result.provenance_directory.name / "seal.json").unlink()
    elif change == "foreign":
        (directory / "foreign").write_text("foreign")
    else:
        path = next(directory.glob("partition-*"))
        original = result.directory / path.name
        path.unlink()
        path.symlink_to(original)
    with (
        generated_provider_scope(request, capture_roots=(directory,)),
        pytest.raises((ValueError, RuntimeError, OSError)),
    ):
        require_provider_operation(fp, BrokerPolicyOperation.MATERIAL_USE)


def test_rehashed_statistics_are_not_actual_source_projection(native_science):
    result, request, fp = native_science
    cell = fp.cells[0]
    metrics = tuple(
        (
            replace(
                m,
                estimate=0.1,
                lower=0.1,
                upper=0.1,
                minimum=0.1,
                maximum=0.1,
                quantiles={key: 0.1 for key in m.quantiles},
                metric_id="",
            )
            if m.name == "spread"
            else m
        )
        for m in cell.metrics
    )
    changed = replace(
        fp,
        cells=(replace(cell, metrics=metrics, cell_id=""), *fp.cells[1:]),
        fingerprint_id="",
    )
    assert changed.fingerprint_id != fp.fingerprint_id
    with (
        generated_provider_scope(request, capture_roots=(result.directory,)),
        pytest.raises(ValueError, match="statistics differ"),
    ):
        require_provider_operation(changed, BrokerPolicyOperation.MATERIAL_USE)


def test_no_rights_no_locators_and_ambiguous_copies_refuse(
    native_science, tmp_path
):
    result, request, fp = native_science
    with pytest.raises(
        ValueError, match="current_process_policy_scope_required"
    ):
        require_provider_operation(fp, BrokerPolicyOperation.MATERIAL_USE)
    with (
        generated_provider_scope(request),
        pytest.raises(ValueError, match="locators required"),
    ):
        require_provider_operation(fp, BrokerPolicyOperation.MATERIAL_USE)
    other = _copy_sources(result, tmp_path)
    with (
        generated_provider_scope(request),
        broker_fingerprint_sources(result.directory, other),
        pytest.raises(ValueError, match="ambiguous"),
    ):
        require_provider_operation(fp, BrokerPolicyOperation.MATERIAL_USE)


def test_stream_bounds_and_sparse_support_are_explicit(native_science):
    from histdatacom.broker_capture import BrokerDeliveryResourceLimitError

    result, request, fp = native_science
    with generated_provider_scope(request, capture_roots=(result.directory,)):
        for config in (
            BrokerLifecycleFingerprintFitConfigV1(max_input_records=1),
            BrokerLifecycleFingerprintFitConfigV1(max_input_bytes=1),
        ):
            with pytest.raises(BrokerDeliveryResourceLimitError):
                fit_broker_delivery_fingerprint(
                    result.directory,
                    (result.manifest,),
                    provider_requests=(request,),
                    config=config,
                )
        with pytest.raises(ValueError, match="ineligible"):
            fit_broker_delivery_fingerprint(
                result.directory,
                (result.manifest,),
                provider_requests=(request,),
                config=BrokerLifecycleFingerprintFitConfigV1(
                    min_quote_events=4
                ),
            )


def test_native_generation_rejects_legacy_config_and_mixed_sources(
    native_science,
):
    from histdatacom.broker_capture import BrokerDeliveryFitConfigV1

    result, request, _ = native_science
    with pytest.raises(ValueError, match="exact SDK inputs"):
        fit_broker_delivery_fingerprint(
            result.directory,
            (result.manifest,),
            provider_requests=(request,),
            config=BrokerDeliveryFitConfigV1(),
        )
    with pytest.raises(ValueError, match="exact SDK inputs"):
        fit_broker_delivery_fingerprint(
            result.directory,
            (result.manifest, object()),
            provider_requests=(request, request),
        )


def test_supersession_requires_advancing_effective_time(native_science):
    result, request, fp = native_science
    with generated_provider_scope(request, capture_roots=(result.directory,)):
        with pytest.raises(ValueError, match="advance effective start"):
            fit_broker_delivery_fingerprint(
                result.directory,
                (result.manifest,),
                provider_requests=(request,),
                supersedes=fp,
            )
        successor = fit_broker_delivery_fingerprint(
            result.directory,
            (result.manifest,),
            provider_requests=(request,),
            supersedes=fp,
            effective_start_utc_ns=fp.effective_start_utc_ns + 1,
        )
        assert successor.supersedes_fingerprint_id == fp.fingerprint_id


def test_mixed_actual_source_time_semantics_are_not_pooled(tmp_path):
    _, _, fp = qualified_sdk_capture(tmp_path, mode="mixed")
    for cell in fp.cells:
        metric = next(
            x for x in cell.metrics if x.name == "source_timestamp_precision_ns"
        )
        assert metric.estimate is None
        assert metric.limitations == ("mixed_source_time_semantics_not_pooled",)


@pytest.mark.parametrize(
    "change",
    [
        {"eligible": True, "quote_count": 0},
        {"first_receive_time_utc_ns": None, "last_receive_time_utc_ns": None},
    ],
)
def test_eligibility_cannot_claim_empty_timed_support(native_science, change):
    with pytest.raises(ValueError):
        replace(native_science[2].eligibility_decisions[0], **change)


def test_root_cannot_substitute_unrelated_compatibility_json(native_science):
    with pytest.raises(ValueError):
        replace(native_science[2].capture_roots[0], compatibility_json="{}")


def test_native_spread_is_independent_of_decimal_context(native_science):
    from decimal import Inexact, localcontext
    from histdatacom.broker_capture.lifecycle_fingerprints import _quote_spread

    result, request, fp = native_science
    with localcontext() as context:
        context.prec = 2
        context.traps[Inexact] = True
        assert _quote_spread("1.10000", "1.12345") == 0.02345
        with generated_provider_scope(
            request, capture_roots=(result.directory,)
        ):
            assert (
                fit_broker_delivery_fingerprint(
                    result.directory,
                    (result.manifest,),
                    provider_requests=(request,),
                )
                == fp
            )
            assert verify_broker_fingerprint_sources(fp) == fp


def test_native_physical_training_join_has_explicit_unsupported_boundary(
    native_science,
):
    from histdatacom.data_quality.training_contracts import training_json
    from histdatacom.data_quality.training_join_contracts import (
        JoinFamily,
        TrainingJoinSourceV1,
    )
    from histdatacom.data_quality.training_join_context import _broker

    result, _, fp = native_science
    source = TrainingJoinSourceV1(
        "sdk-native",
        JoinFamily.BROKER,
        paths=(str(result.directory), str(result.directory / "manifest.json")),
        evidence_json=(training_json(fp.to_dict()),),
    )
    with pytest.raises(
        ValueError, match="SDK-native physical training join is unsupported"
    ):
        _broker(source)
