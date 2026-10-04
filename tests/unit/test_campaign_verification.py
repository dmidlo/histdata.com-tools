"""Pure receipt and file-boundary controls; not a native positive substitute."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import FrozenInstanceError, replace

import pytest

from histdatacom import campaign_index_contracts as contracts
from histdatacom import campaign_verification as verification
from histdatacom.runtime_contracts import ArtifactRef


def _hash_domain_streams():
    from histdatacom.synthetic.contracts import (
        SyntheticEventStreamV1,
        SyntheticEventV1,
    )

    return tuple(
        SyntheticEventStreamV1(
            run_id="declared-run",
            ensemble_member_id="member-0",
            symbol=symbol,
            events=(
                SyntheticEventV1.observed(
                    symbol=symbol,
                    event_time_ns=100,
                    event_sequence=0,
                    bid=1.1,
                    ask=1.2,
                    run_id="declared-run",
                    ensemble_member_id="member-0",
                    source_version_id="declared-source",
                    source_series_id=symbol,
                    source_period="201506",
                    source_row_id=1,
                ),
            ),
        )
        for symbol in ("gbpusd", "eurusd")
    )


def _literal_json_sha256(payload):
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def test_delivery_output_hash_uses_flat_event_rows_not_cross_report_domain():
    from histdatacom.synthetic.cross_currency import _streams_content_sha256
    from histdatacom.synthetic.delivery import (
        reconstruction_streams_content_sha256,
    )

    streams = _hash_domain_streams()
    ordered = sorted(streams, key=lambda item: item.symbol)
    flattened = [event.to_dict() for row in ordered for event in row.events]
    grouped = [
        {
            "symbol": row.symbol,
            "events": [event.to_dict() for event in row.events],
        }
        for row in ordered
    ]
    delivery_hash = _literal_json_sha256(flattened)
    cross_hash = _literal_json_sha256(grouped)
    assert delivery_hash != cross_hash
    assert reconstruction_streams_content_sha256(streams) == delivery_hash
    assert _streams_content_sha256(streams) == cross_hash
    verification._verify_delivery_output_hash(streams, delivery_hash)
    with pytest.raises(ValueError, match="actual delivery output hash differs"):
        verification._verify_delivery_output_hash(streams, cross_hash)


@pytest.mark.parametrize("tamper", ["event_price", "row_omission"])
def test_delivery_output_hash_recomputes_actual_event_content(tamper):
    streams = _hash_domain_streams()
    rows = [
        event.to_dict()
        for stream in sorted(streams, key=lambda item: item.symbol)
        for event in stream.events
    ]
    if tamper == "event_price":
        rows[0]["bid"] = 1.09
    else:
        rows.pop()
    with pytest.raises(ValueError, match="actual delivery output hash differs"):
        verification._verify_delivery_output_hash(
            streams, _literal_json_sha256(rows)
        )


def _ref(path: str = "/declared/index.json"):
    return contracts.CampaignArtifactRefV1(
        "reconstruction_campaign_product_index_v1", path, 2, "a" * 64
    )


def _receipt(cls=contracts.CampaignStructuralVerificationV1, **updates):
    fields = {
        "index_ref": _ref(),
        "index_id": "index:declared",
        "plan_set_id": "plan:declared",
        "support_artifact_id": "support:declared",
        "status": "complete",
        "shard_count": 1,
        "support_window_count": 1,
        "product_count": 2,
        "missing_product_count": 0,
        "empty_window_count": 0,
        "refused_window_count": 0,
        "observed_event_count": 6,
        "synthetic_event_count": 2,
        "shard_rows_sha256": "b" * 64,
        "verified_inputs_sha256": "c" * 64,
    }
    if cls is contracts.CampaignDeepVerificationV1:
        fields["product_verifications_sha256"] = "d" * 64
    fields.update(updates)
    return cls(**fields)


@pytest.mark.parametrize(
    "cls",
    [
        contracts.CampaignStructuralVerificationV1,
        contracts.CampaignDeepVerificationV1,
    ],
)
def test_receipts_roundtrip_as_historical_records_only(cls):
    receipt = _receipt(cls)
    assert cls.from_json(receipt.to_json()) == receipt
    assert (
        receipt.to_dict()["authority"]
        == "historical_record_only_fresh_verification_required"
    )
    with pytest.raises(FrozenInstanceError):
        receipt.status = "incomplete"


def test_structural_and_deep_receipts_have_disjoint_schemas_and_ids():
    structural = _receipt()
    deep = _receipt(contracts.CampaignDeepVerificationV1)
    assert structural.schema_version != deep.schema_version
    assert structural.verification_id != deep.verification_id
    assert "unverified" in structural.verification_level
    with pytest.raises(ValueError):
        contracts.CampaignDeepVerificationV1.from_dict(structural.to_dict())
    with pytest.raises(ValueError):
        contracts.CampaignStructuralVerificationV1.from_dict(deep.to_dict())


@pytest.mark.parametrize(
    "field,value",
    [
        ("verification_id", "forged"),
        ("schema_version", "unknown"),
        ("verification_level", "certified"),
        ("authority", "true"),
        ("verifier_version", "99.0.0"),
        ("product_count", True),
        ("observed_event_count", -1),
        ("shard_count", 0),
        ("shard_rows_sha256", "g" * 64),
        ("status", "qualified"),
    ],
)
def test_receipt_rejects_resealed_or_mistyped_fields(field, value):
    payload = _receipt().to_dict()
    payload[field] = value
    with pytest.raises((ValueError, TypeError)):
        contracts.CampaignStructuralVerificationV1.from_dict(payload)


def test_receipt_rejects_unknown_fields_and_noncanonical_text():
    receipt = _receipt()
    with pytest.raises(ValueError):
        type(receipt).from_dict({**receipt.to_dict(), "passed": True})
    with pytest.raises(ValueError):
        type(receipt).from_json(receipt.to_json() + "\n")


def test_missing_empty_refused_denominators_remain_distinct():
    receipt = _receipt(
        status="incomplete",
        product_count=0,
        missing_product_count=2,
        empty_window_count=1,
        refused_window_count=1,
        support_window_count=3,
    )
    assert (
        receipt.missing_product_count,
        receipt.empty_window_count,
        receipt.refused_window_count,
    ) == (2, 1, 1)
    with pytest.raises(ValueError, match="completion"):
        replace(receipt, status="complete")


def test_artifact_metadata_is_detached_and_exact():
    original = ArtifactRef(
        "example", "/declared/input", 2, "a" * 64, {"nested": [1]}
    )
    admitted = contracts.CampaignArtifactRefV1.from_artifact_ref(original)
    original.metadata["nested"] = [2]
    detached = admitted.to_artifact_ref()
    detached.metadata["nested"] = [3]
    assert admitted.metadata_json == '{"nested":[1]}'
    assert admitted.to_artifact_ref().metadata == {"nested": [1]}


@pytest.mark.parametrize("size", [None, True, -1, 2.0])
def test_artifact_requires_exact_byte_size(size):
    with pytest.raises((ValueError, TypeError)):
        contracts.CampaignArtifactRefV1.from_artifact_ref(
            ArtifactRef("x", "/x", size, "a" * 64)
        )


def test_exact_reference_type_precedes_serializer():
    class Hostile(ArtifactRef):
        def to_dict(self):
            pytest.fail("hostile serializer executed")

    with pytest.raises(TypeError):
        contracts.CampaignArtifactRefV1.from_artifact_ref(
            Hostile("x", "/x", 1, "a" * 64)
        )


@pytest.mark.parametrize(
    "payload",
    [
        '{"x":1,"x":2}',
        '{"x":NaN}',
        '{"x":Infinity}',
        '{"x":1e999}',
        '{"x":9223372036854775808}',
    ],
)
def test_receipt_json_rejects_duplicate_nonfinite_and_huge_numbers(payload):
    with pytest.raises((ValueError, TypeError)):
        contracts.load_json(payload)


def test_receipt_depth_preflight_precedes_decoder(monkeypatch):
    monkeypatch.setattr(
        contracts.json, "loads", lambda *a, **k: pytest.fail("decoder executed")
    )
    with pytest.raises(ValueError, match="structural"):
        contracts.load_json("[" * 25 + "0" + "]" * 25)


@pytest.mark.parametrize(
    "character,repeat", [("\x7f", 700000), ("\U0001f680", 350000)]
)
def test_escaped_byte_preflight_precedes_encoder(
    monkeypatch, character, repeat
):
    monkeypatch.setattr(
        contracts.json, "dumps", lambda *a, **k: pytest.fail("encoder executed")
    )
    with pytest.raises(ValueError, match="byte bound"):
        contracts.canonical(character * repeat)


def test_mutated_aggregate_ref_budget_precedes_nested_parse(monkeypatch):
    receipt = _receipt(contracts.CampaignDeepVerificationV1)
    ref = _ref()
    object.__setattr__(ref, "metadata_json", "x" * 1024 * 1024)
    object.__setattr__(receipt, "out_of_plan_products", (ref,) * 5)
    monkeypatch.setattr(
        contracts.json,
        "loads",
        lambda *a, **k: pytest.fail("nested decoder executed"),
    )
    with pytest.raises(ValueError, match="byte bound"):
        receipt.to_json()


@pytest.mark.parametrize("items", [[], (_ref(), _ref())])
def test_out_of_plan_roster_requires_exact_unique_tuple(items):
    with pytest.raises((ValueError, TypeError)):
        _receipt(
            contracts.CampaignDeepVerificationV1, out_of_plan_products=items
        )


def test_control_reader_admits_stable_plain_json_without_authority(tmp_path):
    path = tmp_path / "input.json"
    path.write_text('{"declared":false}', encoding="utf-8")
    assert verification.read_campaign_control_json(path) == {"declared": False}


def test_control_snapshot_retains_exact_decoded_bytes(tmp_path, monkeypatch):
    path = tmp_path / "input.json"
    encoded = b'{ "declared": false, "number": 1.00 }\r\n'
    path.write_bytes(encoded)
    monkeypatch.setattr(
        type(path),
        "open",
        lambda *a, **k: pytest.fail("unguarded path reopen"),
    )
    payload, actual = verification.read_campaign_control_snapshot(path)
    assert payload == {"declared": False, "number": 1.0}
    assert actual == encoded


def test_control_snapshot_refuses_changed_descriptor(tmp_path, monkeypatch):
    path = tmp_path / "input.json"
    path.write_bytes(b"{}")
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"{}")
    actual_open = verification.os.open

    def changed_open(target, flags):
        replacement.replace(path)
        return actual_open(target, flags)

    monkeypatch.setattr(verification.os, "open", changed_open)
    with pytest.raises(ValueError, match="changed while opening"):
        verification.read_campaign_control_snapshot(path)


def test_control_snapshot_rechecks_after_decode(tmp_path, monkeypatch):
    path = tmp_path / "input.json"
    path.write_bytes(b"{}")
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"{}")
    actual_json = verification._json

    def changed_json(encoded):
        result = actual_json(encoded)
        replacement.replace(path)
        return result

    monkeypatch.setattr(verification, "_json", changed_json)
    with pytest.raises(ValueError, match="changed during verification"):
        verification.read_campaign_control_snapshot(path)


def test_unsupported_descriptor_platform_refuses_explicitly(
    tmp_path, monkeypatch
):
    monkeypatch.delattr(verification.os, "O_NOFOLLOW")
    with pytest.raises(ValueError, match="POSIX"):
        verification.read_campaign_control_json(tmp_path / "never-opened")


@pytest.mark.parametrize(
    "text", ['{"x":0,"x":1}', '{"x":NaN}', "[]", "[" * 65 + "0" + "]" * 65]
)
def test_control_reader_rejects_malformed_unbounded_shape(tmp_path, text):
    path = tmp_path / "input.json"
    path.write_text(text, encoding="utf-8")
    with pytest.raises((ValueError, TypeError)):
        verification.read_campaign_control_json(path)


def test_control_reader_refuses_oversize_before_open(tmp_path, monkeypatch):
    path = tmp_path / "huge.json"
    path.write_bytes(b"{} ")
    monkeypatch.setattr(verification, "MAX_CONTROL_BYTES", 2)
    monkeypatch.setattr(
        verification.os,
        "open",
        lambda *a, **k: pytest.fail("opened oversized file"),
    )
    with pytest.raises(ValueError, match="byte bound"):
        verification.read_campaign_control_json(path)


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory"])
@pytest.mark.parametrize(
    "reader",
    [
        verification.read_campaign_control_json,
        verification.read_campaign_control_snapshot,
    ],
)
def test_control_reader_refuses_nonregular_without_open(
    tmp_path, monkeypatch, kind, reader
):
    path = tmp_path / "input.json"
    if kind == "symlink":
        target = tmp_path / "actual"
        target.write_text("{}", encoding="ascii")
        path.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.mkdir()
    monkeypatch.setattr(
        verification.os,
        "open",
        lambda *a, **k: pytest.fail("opened nonregular file"),
    )
    with pytest.raises(ValueError, match="regular"):
        reader(path)


def test_guard_detects_replacement_even_same_bytes(tmp_path):
    path = tmp_path / "input.json"
    path.write_bytes(b"{}")
    guard = verification._Guard()
    guard.document(path)
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"{}")
    replacement.replace(path)
    with pytest.raises(ValueError, match="changed"):
        guard.finish()


def test_guard_requires_actual_hash_and_not_only_metadata(tmp_path):
    path = tmp_path / "input.json"
    path.write_bytes(b"{}")
    ref = ArtifactRef("input", str(path), 2, "a" * 64)
    with pytest.raises(ValueError, match="SHA-256"):
        verification._Guard().read(path, ref=ref)


def test_relative_source_graph_requires_explicit_native_bundle(tmp_path):
    path = tmp_path / "source.arrow"
    path.write_bytes(b"synthetic bytes, not an Arrow positive")
    ref = ArtifactRef(
        "source",
        path.name,
        path.stat().st_size,
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    with pytest.raises(ValueError, match="relative artifact"):
        verification._Guard().graph(ref.to_dict())
    guard = verification._Guard()
    guard.graph(ref.to_dict(), relative_root=tmp_path)
    assert set(guard.files) == {path}
    assert len(guard.finish()) == 64


def test_relative_source_graph_refuses_parent_escape(tmp_path):
    ref = ArtifactRef("source", "../source.arrow", 1, "a" * 64)
    with pytest.raises(ValueError, match="relative artifact"):
        verification._Guard().graph(ref.to_dict(), relative_root=tmp_path)


def _manifest_path(root):
    from histdatacom.synthetic.persistence import (
        RECONSTRUCTION_PRODUCT_DIRECTORY,
    )

    return (
        root
        / RECONSTRUCTION_PRODUCT_DIRECTORY
        / "axes"
        / "commits"
        / "id"
        / "manifest.json"
    )


def test_discovery_reports_paths_without_claiming_manifest_validity(tmp_path):
    path = _manifest_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text("not a product", encoding="ascii")
    assert tuple(verification.iter_campaign_manifest_paths(tmp_path)) == (path,)
    assert (
        tuple(verification.iter_campaign_manifest_paths(tmp_path / "absent"))
        == ()
    )


def test_discovery_bound_refuses_instead_of_truncating(tmp_path, monkeypatch):
    path = _manifest_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text("{}", encoding="ascii")
    monkeypatch.setattr(verification, "MAX_DISCOVERED_PRODUCTS", 0)
    with pytest.raises(ValueError, match="bound"):
        tuple(verification.iter_campaign_manifest_paths(tmp_path))


def test_discovery_refuses_symlink_tree(tmp_path):
    path = _manifest_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.symlink_to(tmp_path / "foreign")
    with pytest.raises(ValueError, match="symlink"):
        tuple(verification.iter_campaign_manifest_paths(tmp_path))


def test_receipt_text_cannot_be_supplied_as_fresh_verification_authority(
    tmp_path,
):
    from histdatacom.reconstruction import ReconstructionPlanError

    path = tmp_path / "receipt.json"
    path.write_text(
        _receipt(contracts.CampaignDeepVerificationV1).to_json(),
        encoding="ascii",
    )
    with pytest.raises(
        ReconstructionPlanError,
        match="campaign product index scientific nonclaim differs",
    ):
        verification.verify_campaign_product_index(path)


def test_no_boolean_or_callback_verification_switch():
    import inspect

    assert tuple(
        inspect.signature(verification.verify_campaign_product_index).parameters
    ) == ("index_path",)
    assert tuple(
        inspect.signature(
            verification.inspect_campaign_product_index
        ).parameters
    ) == ("index_path",)


@pytest.mark.parametrize("event_time", [10, 19, 30, 39])
def test_terminal_event_intervals_refuse_inclusive_start_and_interior(
    event_time,
):
    # Pure interval arithmetic, not a substituted successful product verifier.
    with pytest.raises(ValueError, match="same-run empty/refused"):
        verification._refuse_terminal_event_times(
            "planned-run", {"planned-run": ((10, 20), (30, 40))}, [event_time]
        )


@pytest.mark.parametrize("event_time", [0, 9, 20, 29, 40, 41])
def test_terminal_event_intervals_preserve_exclusive_end_and_gaps(event_time):
    verification._refuse_terminal_event_times(
        "planned-run", {"planned-run": ((10, 20), (30, 40))}, [event_time]
    )


def test_terminal_event_intervals_do_not_invent_events_between_actual_rows():
    verification._refuse_terminal_event_times(
        "planned-run", {"planned-run": ((10, 20),)}, [9, 20]
    )


def test_terminal_event_intervals_check_all_actual_rows():
    with pytest.raises(ValueError, match="same-run empty/refused"):
        verification._refuse_terminal_event_times(
            "planned-run", {"planned-run": ((10, 20),)}, iter([9, 20, 10])
        )


def test_foreign_run_terminal_scope_never_consumes_event_iterator():
    def forbidden():
        pytest.fail("foreign-run scope consumed event bytes")
        yield 10

    verification._refuse_terminal_event_times(
        "foreign-run", {"planned-run": ((10, 20),)}, forbidden()
    )
