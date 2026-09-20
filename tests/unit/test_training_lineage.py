"""Source-byte and full-graph dependency closure, not caller provenance labels."""

from dataclasses import replace

import polars as pl
import pytest

from histdatacom.datasets import HistDataProviderAdapter
from histdatacom.data_quality.training_contracts import DAY_NS, TrainingSourceV1
from histdatacom.data_quality.training_lineage import (
    build_training_ownership,
    verify_training_ownership,
)
from histdatacom.forecasting.feature_artifacts import write_feature_artifact
from tests.fixtures.training_substrate_v1 import (
    BASE,
    macro_matrix,
    observed_source,
    published_product,
    with_products,
)


def containing(ownership, time):
    return next(u for u in ownership.units if u.start_ns <= time < u.end_ns)


def test_actual_sibling_products_share_unit_not_dependency_map(tmp_path):
    source, version = observed_source(tmp_path)
    a, _ = published_product(tmp_path / "a", version)
    b, _ = published_product(
        tmp_path / "b", version, member="member-b", seed=607
    )
    first = build_training_ownership(with_products(source, a))
    both = build_training_ownership(with_products(source, a, b))
    assert first.artifact_id != both.artifact_id
    assert containing(first, BASE) == containing(both, BASE)
    assert len(first.units) == len(both.units) == 31
    assert containing(both, BASE).graph_symbols == (
        "EURGBP",
        "EURUSD",
        "GBPUSD",
    )


@pytest.mark.parametrize("indices,days", [((2, 3), 2), ((0, 5), 3)])
def test_cross_midnight_and_whole_product_spans_merge_all_dependencies(
    tmp_path, indices, days
):
    source, version = observed_source(tmp_path)
    product, _ = published_product(
        tmp_path / "product", version, indices=indices
    )
    ownership = build_training_ownership(with_products(source, product))
    unit = containing(ownership, BASE)
    assert (unit.start_ns, unit.end_ns) == (BASE, BASE + days * DAY_NS)
    assert len(ownership.units) == 32 - days


def test_later_product_cannot_redefine_a_frozen_map(tmp_path):
    source, version = observed_source(tmp_path)
    frozen = build_training_ownership(source)
    product, _ = published_product(
        tmp_path / "product", version, indices=(2, 3)
    )
    with pytest.raises(ValueError, match="frozen ownership"):
        verify_training_ownership(with_products(source, product), frozen)


def test_changed_source_bytes_and_during_read_race_fail_closed(
    tmp_path, monkeypatch
):
    source, version = observed_source(tmp_path)
    ownership = build_training_ownership(source)
    original = HistDataProviderAdapter.read_partition
    changed = False

    def mutate_after_read(self, partition):
        nonlocal changed
        frame = original(self, partition)
        if not changed:
            changed = True
            frame.with_columns((pl.col("ask") + 0.0001).alias("ask")).write_ipc(
                partition.artifact.path
            )
        return frame

    monkeypatch.setattr(
        HistDataProviderAdapter, "read_partition", mutate_after_read
    )
    with pytest.raises(ValueError, match="hash|mismatch"):
        verify_training_ownership(source, ownership)
    assert version.partitions


def test_product_source_labels_and_values_do_not_replace_source_verification(
    tmp_path,
):
    source, version = observed_source(tmp_path)
    product, _ = published_product(
        tmp_path / "product", version, wrong_anchor=True
    )
    with pytest.raises(ValueError, match="differs from source values"):
        build_training_ownership(with_products(source, product))


def test_source_preflight_precedes_catalog_or_product_deep_work(
    tmp_path, monkeypatch
):
    from histdatacom.data_quality import training_lineage as module

    source, _ = observed_source(tmp_path)
    monkeypatch.setattr(module, "MAX_TRAINING_SOURCE_ROWS", 1)
    monkeypatch.setattr(
        module.DatasetCatalog,
        "verify",
        lambda *a: pytest.fail("catalog work before budget"),
    )
    monkeypatch.setattr(
        module,
        "read_reconstruction_streams",
        lambda *a: pytest.fail("product replay before budget"),
    )
    with pytest.raises(ValueError, match="work budget"):
        build_training_ownership(source)


def test_context_identity_changes_units_but_derived_identity_changes_only_map(
    tmp_path,
):
    source, _ = observed_source(tmp_path)
    a = write_feature_artifact(macro_matrix(value=2.0), tmp_path / "features")
    b = write_feature_artifact(macro_matrix(value=3.0), tmp_path / "features")
    left = build_training_ownership(
        replace(source, context_artifact_paths=(str(a),))
    )
    right = build_training_ownership(
        replace(source, context_artifact_paths=(str(b),))
    )
    assert (
        containing(left, BASE).artifact_id
        != containing(right, BASE).artifact_id
    )
    left = build_training_ownership(
        replace(source, derived_artifact_paths=(str(a),))
    )
    right = build_training_ownership(
        replace(source, derived_artifact_paths=(str(b),))
    )
    assert containing(left, BASE) == containing(right, BASE)
    assert left.artifact_id != right.artifact_id


def test_context_dependency_spans_merge_and_out_of_scope_refuses(tmp_path):
    source, _ = observed_source(tmp_path)
    path = write_feature_artifact(
        macro_matrix(end=BASE + DAY_NS + 1), tmp_path / "features"
    )
    ownership = build_training_ownership(
        replace(source, context_artifact_paths=(str(path),))
    )
    assert containing(ownership, BASE).end_ns == BASE + 2 * DAY_NS
    path = write_feature_artifact(
        macro_matrix(end=BASE + 40 * DAY_NS), tmp_path / "features"
    )
    with pytest.raises(ValueError, match="escapes observed coverage"):
        build_training_ownership(
            replace(source, context_artifact_paths=(str(path),))
        )


def test_catalog_unknown_nested_fields_and_alias_reference_refuse(tmp_path):
    import json

    source, _ = observed_source(tmp_path)
    data = json.loads(source.catalog_json)
    data["versions"][0]["unknown"] = "not ignored"
    changed = replace(source, catalog_json=json.dumps(data))
    with pytest.raises(ValueError, match="unknown|identity"):
        build_training_ownership(changed)
    with pytest.raises(ValueError, match="immutable"):
        build_training_ownership(
            TrainingSourceV1(source.catalog_json, "training-observed-fixture")
        )


def test_generated_dependency_must_resolve_to_actual_verified_anchor(tmp_path):
    source, version = observed_source(tmp_path)
    product, _ = published_product(
        tmp_path / "product", version, wrong_dependency=True
    )
    with pytest.raises(ValueError, match="enclosing observed anchors"):
        build_training_ownership(with_products(source, product))


def test_unknown_product_manifest_field_cannot_hide_outside_root_identity(
    tmp_path,
):
    import json

    source, version = observed_source(tmp_path)
    product, _ = published_product(tmp_path / "product", version)
    data = json.loads(product.manifest_path.read_text())
    data["source"]["unrecognized"] = "later metadata"
    product.manifest_path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown|differs|identity"):
        build_training_ownership(with_products(source, product))
