"""Invented wire records only; native execution is tested separately."""

from dataclasses import replace
import hashlib
import json

import pytest

from histdatacom import campaign_receipt_contracts as c
from histdatacom.campaign_index_contracts import (
    CampaignArtifactRefV1,
    CampaignProductVerificationV1,
    canonical,
)


def run_record(**changes):
    values = dict(
        index_path="/invented/index.json",
        index_id="invented-index",
        index_sha256="a" * 64,
        implementation_sha256="b" * 64,
        environment_json='{"python":"invented"}',
        forbidden_roots=("/invented/products",),
        products_per_shard=1,
    )
    values.update(changes)
    return c.CampaignVerificationRunV1(**values)


def ref(record):
    data = record.to_json().encode("ascii")
    return c.CampaignReceiptRefV1(
        record.KIND,
        record.artifact_id,
        len(data),
        hashlib.sha256(data).hexdigest(),
    )


def product_record():
    manifest = CampaignArtifactRefV1(
        "native-product", "/invented/products/manifest.json", 12, "a" * 64
    )
    core = CampaignProductVerificationV1(
        "plan",
        "shard",
        "support",
        "run",
        "window",
        "member",
        manifest,
        "manifest",
        "config",
        "engine",
        "generator",
        "{}",
        "b" * 64,
        "c" * 64,
        2,
        3,
        "d" * 64,
        "{}",
        "e" * 64,
    )
    return c.CampaignProductReceiptV1(
        run_record().artifact_id,
        0,
        core.to_json(),
        (
            c.CampaignVerifiedFileV1(
                "/invented/products/data.parquet", 20, "b" * 64, "parquet"
            ),
            c.CampaignVerifiedFileV1(
                manifest.path, manifest.size_bytes, manifest.sha256, "product"
            ),
        ),
        ("/invented/products/data.parquet",),
        '{"source":"invented"}',
        17,
        32,
    )


def summary_record(**changes):
    index_ref = CampaignArtifactRefV1(
        "reconstruction_campaign_product_index_v1",
        "/invented/index.json",
        2,
        "a" * 64,
    )
    values = dict(
        index_ref_json=canonical(index_ref.to_dict()),
        index_id="index",
        plan_set_id="plan",
        support_artifact_id="support",
        status="complete",
        shard_count=1,
        support_window_count=1,
        product_count=1,
        missing_product_count=0,
        empty_window_count=0,
        refused_window_count=0,
        observed_event_count=2,
        synthetic_event_count=3,
        shard_rows_sha256="a" * 64,
        control_inputs_sha256="b" * 64,
        product_verifications_sha256="c" * 64,
        product_inputs_sha256="d" * 64,
    )
    values.update(changes)
    return c.CampaignVerificationSummaryV1(**values)


def records():
    run, product, summary = run_record(), product_record(), summary_record()
    shard = c.CampaignVerificationShardV1(
        run.artifact_id, 0, 0, (ref(product),), 2, 3, 17, 32, 1
    )
    journal = c.CampaignVerificationJournalV1(
        run.artifact_id, 0, None, "started", ref(run), None
    )
    checkpoint = c.CampaignVerificationCheckpointV1(
        run.artifact_id, ref(journal), (ref(shard),), (), 1, "e" * 64, 1
    )
    control = c.CampaignControlReceiptV1(
        run.artifact_id,
        0,
        "global",
        None,
        None,
        (
            c.CampaignVerifiedFileV1(
                "/invented/index.json", 2, "a" * 64, "control"
            ),
        ),
        "b" * 64,
        1,
        2,
    )
    plan_control = replace(
        control, ordinal=1, scope="plan", plan_id="plan", shard_id="shard"
    )
    root = c.CampaignVerificationRootV1(
        run.artifact_id,
        (ref(shard),),
        summary.to_json(),
        "e" * 64,
        "e" * 64,
        1,
        19,
        34,
        1,
        (ref(control),),
        (ref(plan_control),),
    )
    failure = c.CampaignVerificationFailureV1(
        run.artifact_id,
        None,
        ref(checkpoint),
        "changed_input",
        "Invented",
        19,
        34,
    )
    sample = c.CampaignVerificationSampleV1(
        run.artifact_id,
        root.artifact_id,
        (0,),
        (ref(product),),
        "e" * 64,
        "f" * 64,
        17,
        32,
    )
    return (
        run,
        product,
        shard,
        checkpoint,
        summary,
        root,
        journal,
        failure,
        sample,
        control,
    )


@pytest.mark.parametrize("ordinal", range(10))
def test_each_record_roundtrips_and_has_independent_domain_hash(ordinal):
    record = records()[ordinal]
    assert type(record).from_json(record.to_json()) == record
    wire = json.loads(record.to_json())
    expected = hashlib.sha256(
        json.dumps(
            {
                "schema_version": wire["schema_version"],
                "payload": wire["payload"],
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()
    assert (
        wire["artifact_id"]
        == f"campaign-receipt-{record.KIND}:sha256:{expected}"
    )


@pytest.mark.parametrize("size", (True, 0, -1, 65, 1.0))
def test_shard_grouping_is_exact_frozen_bounded_integer(size):
    with pytest.raises(ValueError):
        run_record(products_per_shard=size)


def test_grouping_and_runtime_change_execution_but_not_native_core():
    assert (
        run_record().artifact_id != run_record(products_per_shard=2).artifact_id
    )
    first = product_record()
    second = replace(first, elapsed_ns=first.elapsed_ns + 1)
    assert first.artifact_id != second.artifact_id
    assert (
        first.verification.verification_id
        == second.verification.verification_id
    )


@pytest.mark.parametrize(
    "field,value",
    (
        ("read_bytes", 31),
        ("input_files", ()),
        ("parquet_paths", ()),
        ("lineage_json", "{}"),
        ("ordinal", 262144),
    ),
)
def test_product_refuses_missing_inventory_and_false_read_accounting(
    field, value
):
    with pytest.raises(ValueError):
        replace(product_record(), **{field: value})


def test_product_requires_manifest_exact_hash_and_each_parquet_leaf():
    product = product_record()
    with pytest.raises(ValueError, match="manifest"):
        replace(product, input_files=product.input_files[:1])
    with pytest.raises(ValueError, match="Parquet"):
        replace(product, parquet_paths=("/invented/not-retained.parquet",))
    with pytest.raises(ValueError, match="sorted unique"):
        replace(product, input_files=tuple(reversed(product.input_files)))


def test_control_receipts_require_scope_coordinates_and_measured_inputs():
    control = records()[9]
    for changes in (
        {"scope": "invented"},
        {"plan_id": "not-global"},
        {"scope": "plan"},
        {"read_bytes": 1},
        {"input_files": ()},
        {"ordinal": c.MAX_CONTROL_RECEIPTS},
    ):
        with pytest.raises(ValueError):
            replace(control, **changes)


def test_root_cannot_omit_global_or_productless_plan_controls():
    root = records()[5]
    for changes in ({"global_controls": ()}, {"plan_controls": ()}):
        with pytest.raises(ValueError, match="control coverage"):
            replace(root, **changes)


def test_recovery_root_and_journal_explicitly_bind_historical_checkpoint():
    values = records()
    checkpoint, root, journal = values[3], values[5], values[6]
    recovered = replace(root, reused_checkpoint=ref(checkpoint))
    assert recovered.elapsed_scope == c.ELAPSED_SCOPE
    assert recovered.artifact_id != root.artifact_id
    resumed = c.CampaignVerificationJournalV1(
        root.run_id, 1, ref(journal), "resumed", ref(checkpoint), None
    )
    assert (
        c.CampaignVerificationJournalV1.from_json(resumed.to_json()) == resumed
    )
    with pytest.raises(ValueError):
        replace(root, reused_checkpoint=ref(journal))
    with pytest.raises(ValueError):
        replace(
            root,
            elapsed_scope="historical_cost_misattributed_to_current_attempt",
        )
    with pytest.raises(ValueError):
        replace(resumed, ordinal=0, previous=None)


@pytest.mark.parametrize(
    "path", ("relative", "/a/../b", "/a//b", "/a/./b", "/a\n")
)
def test_file_paths_are_canonical_absolute_without_traversal(path):
    with pytest.raises(ValueError):
        c.CampaignVerifiedFileV1(path, 0, "a" * 64, "input")


def test_checkpoint_and_shard_cover_a_bounded_unambiguous_prefix():
    _, product, shard, checkpoint, *_ = records()
    with pytest.raises(ValueError, match="geometry"):
        replace(shard, first_product_ordinal=1)
    with pytest.raises(ValueError, match="geometry"):
        replace(checkpoint, next_product_ordinal=2)
    with pytest.raises(ValueError):
        replace(checkpoint, pending_products=(ref(product),))
    with pytest.raises(ValueError, match="duplicate"):
        replace(
            shard, products=(ref(product), ref(product)), products_per_shard=2
        )


def test_successful_root_refuses_missing_changed_or_extra_product_inventory():
    root = records()[5]
    with pytest.raises(ValueError, match="exact complete"):
        replace(root, final_inventory_sha256="f" * 64)
    incomplete = summary_record(status="incomplete", missing_product_count=1)
    with pytest.raises(ValueError, match="exact complete"):
        replace(root, summary_json=incomplete.to_json())
    extra = CampaignArtifactRefV1("product", "/extra.json", 1, "f" * 64)
    outside = summary_record(out_of_plan_json=canonical([extra.to_dict()]))
    with pytest.raises(ValueError, match="exact complete"):
        replace(root, summary_json=outside.to_json())
    with pytest.raises(ValueError, match="exact complete"):
        replace(root, product_count=2)


def test_journal_chain_coordinates_and_event_types_are_closed():
    journal = records()[6]
    with pytest.raises(ValueError, match="predecessor"):
        replace(journal, ordinal=1)
    with pytest.raises(ValueError, match="subject"):
        replace(journal, event="completed")
    with pytest.raises(ValueError, match="coordinate"):
        replace(journal, product_ordinal=0)


def test_sample_cannot_claim_full_authority_or_duplicate_selection():
    sample = records()[8]
    with pytest.raises(ValueError, match="scope"):
        replace(sample, authority=c.RECEIPT_AUTHORITY)
    with pytest.raises(ValueError, match="scope"):
        replace(sample, product_ordinals=(0, 0))
    with pytest.raises(ValueError, match="scope"):
        replace(sample, product_ordinals=())


def test_wire_refuses_unknown_duplicate_noncanonical_and_resealed_bad_fields():
    run = run_record()
    wire = run.to_dict()
    for changed in ({**wire, "extra": 1}, {**wire, "artifact_id": "forged"}):
        with pytest.raises(ValueError):
            c.CampaignVerificationRunV1.from_json(canonical(changed))
    with pytest.raises(ValueError, match="duplicate"):
        c.CampaignVerificationRunV1.from_json('{"x":1,"x":2}')
    with pytest.raises(ValueError, match="noncanonical"):
        c.CampaignVerificationRunV1.from_json(run.to_json() + "\n")
    with pytest.raises(ValueError):
        run_record(environment_json='{"x":NaN}')


def test_depth_is_refused_before_json_allocation(monkeypatch):
    import histdatacom.campaign_index_contracts as wire

    def forbidden(*args, **kwargs):
        raise AssertionError("decoder must not be called")

    monkeypatch.setattr(wire.json, "loads", forbidden)
    with pytest.raises(ValueError, match="structural bound"):
        c.CampaignVerificationRunV1.from_json("[" * 25 + "0" + "]" * 25)
