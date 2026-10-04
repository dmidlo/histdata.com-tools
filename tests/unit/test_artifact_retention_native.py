"""Actual generated native cache/catalog replay, never scientific release."""

from dataclasses import replace
import hashlib
import json
import os

import pytest

from histdatacom.artifact_retention import native as api
from histdatacom.artifact_retention.contracts import (
    AsciiTickRecipeV1,
    DependencyRelation as Relation,
    DependencyV1,
    ManagedPayloadRefV1,
    NativeObjectV1,
    RetentionClass as Class,
)

RAW = (
    b"20200102 000000000,1.100000,1.100200,0\n"
    b"20200102 000001000,1.100100,1.100300,1\n"
    b"20200102 000002000,1.100200,1.100400,2\n"
)


def _edges(*values):
    return tuple(
        DependencyV1(identity, relation)
        for identity, relation in sorted(
            values, key=lambda value: (value[0], value[1].value)
        )
    )


def _payload(root, token, extension, raw):
    relative = f"objects/{token:032x}.{extension}"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(raw)
    return ManagedPayloadRefV1(
        relative, hashlib.sha256(raw).hexdigest(), len(raw)
    )


def _object(adapter, admission, ref, retention_class, token=1):
    return NativeObjectV1(
        adapter,
        admission.native_schema,
        admission.native_id,
        ref,
        retention_class,
        admission.live_dependencies,
        admission.historical_subject_ids,
        f"{token:032x}",
        token,
    )


@pytest.fixture
def bundle(tmp_path):
    root = (tmp_path / "managed").resolve()
    root.mkdir()
    source = _object(
        api.ASCII_SOURCE_ADAPTER,
        api.source_admission(RAW, symbol="EURUSD", period="202001"),
        _payload(root, 1, "csv", RAW),
        Class.IMMUTABLE_SOURCE,
    )
    recipe = AsciiTickRecipeV1(
        source.artifact_id,
        "EURUSD",
        "202001",
        api.current_cache_implementation_sha256(),
        api.current_cache_backend_id(),
    )
    recipe_object = _object(
        api.ASCII_RECIPE_ADAPTER,
        api.NativeAdmission(
            recipe.SCHEMA,
            recipe.artifact_id,
            _edges((source.artifact_id, Relation.SOURCE)),
        ),
        _payload(root, 2, "json", recipe.to_json().encode("ascii")),
        Class.REPRODUCIBILITY_DEPENDENCY,
    )
    execution = api.execute_ascii_cache_recipe(
        RAW, recipe, scratch_directory=tmp_path / "original-execution"
    )
    partition = json.loads(execution.partition_json)
    cache = _object(
        api.CACHE_ADAPTER,
        api.NativeAdmission(
            partition["schema_version"],
            partition["partition_id"],
            _edges(
                (source.artifact_id, Relation.SOURCE),
                (recipe_object.artifact_id, Relation.RECIPE),
            ),
        ),
        _payload(root, 3, "data", execution.cache_bytes),
        Class.REPLACEABLE_CACHE,
    )
    return root, source, recipe_object, cache, recipe, execution


def _catalog(bundle, tmp_path):
    root, source, recipe_object, cache, _, _ = bundle
    inventory = (source, recipe_object, cache)
    built = api.build_native_catalog(
        root,
        (cache.artifact_id,),
        inventory,
        dataset_id="generated-retention-native",
        scratch_directory=tmp_path / "catalog-execution",
    )
    catalog = _object(
        api.CATALOG_ADAPTER,
        built.admission,
        _payload(root, 4, "json", built.canonical_bytes),
        Class.PUBLISHED_DERIVED,
    )
    return catalog, built, (*inventory, catalog)


def test_actual_fresh_regeneration_and_managed_catalog_replay(bundle, tmp_path):
    from histdatacom.datasets import DatasetCatalog, DatasetContractError

    root, source, recipe_object, cache, _, first = bundle
    catalog, built, inventory = _catalog(bundle, tmp_path)
    loaded = DatasetCatalog.from_json(built.canonical_bytes.decode("ascii"))
    partition = loaded.versions[0].partitions[0]
    assert partition.artifact.path == str(
        root / cache.payload_ref.relative_path
    )
    assert partition.source_artifact_sha256 == cache.payload_ref.sha256
    assert partition.source_artifact_sha256 != source.payload_ref.sha256
    assert loaded.versions[0].qualification_status.value == "unqualified"
    with pytest.raises(DatasetContractError, match="not qualified"):
        loaded.verify(loaded.versions[0].dataset_version_id)
    assert catalog.live_dependencies == _edges(
        (cache.artifact_id, Relation.NATIVE_ARTIFACT),
        (source.artifact_id, Relation.SOURCE),
    )
    before = {path: path.read_bytes() for path in (root / "objects").iterdir()}
    actual = api.admit_native_object(
        root, catalog, inventory, scratch_directory=tmp_path / "replay"
    )
    assert actual == built.admission
    proof = api.prove_cache_regeneration(
        root,
        cache.artifact_id,
        inventory,
        scratch_directory=tmp_path / "regeneration",
    )
    assert proof.historical_output_sha256 == proof.rebuilt_sha256
    assert proof.rebuilt_sha256 == hashlib.sha256(first.cache_bytes).hexdigest()
    assert proof.source_object_id == source.artifact_id
    assert proof.recipe_object_id == recipe_object.artifact_id
    assert all(path.read_bytes() == raw for path, raw in before.items())


def test_orphan_identical_cache_can_disappear_without_dangling_proof(
    bundle, tmp_path
):
    root, source, recipe_object, cache, _, first = bundle
    catalog, built, inventory = _catalog(bundle, tmp_path)
    orphan = replace(
        cache,
        payload_ref=_payload(root, 5, "data", first.cache_bytes),
        transaction_id="2" * 32,
        created_ns=2,
    )
    inventory = (*inventory, orphan)
    proof = api.prove_cache_regeneration(
        root,
        orphan.artifact_id,
        inventory,
        scratch_directory=tmp_path / "orphan-proof",
    )
    proof_object = _object(
        api.REGENERATION_ADAPTER,
        api.NativeAdmission(
            proof.SCHEMA,
            proof.artifact_id,
            _edges(
                (source.artifact_id, Relation.SOURCE),
                (recipe_object.artifact_id, Relation.RECIPE),
            ),
            (orphan.artifact_id,),
        ),
        _payload(root, 6, "json", proof.to_json().encode("ascii")),
        Class.REPRODUCIBILITY_DEPENDENCY,
    )
    # Only this test's newly created orphan; no managed collection authority
    # is claimed by this native seam test. Store lifecycle is tested elsewhere.
    (root / orphan.payload_ref.relative_path).unlink()
    inventory = (*inventory, proof_object)
    assert api.admit_native_object(
        root,
        proof_object,
        inventory,
        scratch_directory=tmp_path / "proof-replay",
    ).historical_subject_ids == (orphan.artifact_id,)
    assert (
        api.admit_native_object(
            root,
            catalog,
            inventory,
            scratch_directory=tmp_path / "catalog-replay",
        )
        == built.admission
    )
    assert (
        root / cache.payload_ref.relative_path
    ).read_bytes() == first.cache_bytes
    with pytest.raises(FileNotFoundError):
        api.admit_native_object(
            root, orphan, inventory, scratch_directory=tmp_path / "live-missing"
        )


@pytest.mark.parametrize(
    "adapter", ("unknown.v1", "dataset-release.v1", "zip.v1")
)
def test_unknown_adapter_never_admits_an_empty_graph(bundle, tmp_path, adapter):
    root, source, _, _, _, _ = bundle
    unknown = replace(source, adapter_id=adapter)
    with pytest.raises(ValueError, match="unsupported native"):
        api.admit_native_object(
            root, unknown, (unknown,), scratch_directory=tmp_path / "unused"
        )
    assert not (tmp_path / "unused").exists()


@pytest.mark.parametrize("change", ("class", "source", "recipe", "native_id"))
def test_cache_admission_refuses_forged_descriptor(bundle, tmp_path, change):
    root, source, recipe_object, cache, _, _ = bundle
    if change == "class":
        changed = replace(cache, retention_class=Class.SCRATCH)
    elif change == "native_id":
        changed = replace(cache, native_id="foreign:sha256:" + "f" * 64)
    else:
        relation = Relation.SOURCE if change == "source" else Relation.RECIPE
        changed = replace(
            cache,
            live_dependencies=tuple(
                edge
                for edge in cache.live_dependencies
                if edge.relation is not relation
            ),
        )
    with pytest.raises(ValueError):
        api.admit_native_object(
            root,
            changed,
            (source, recipe_object, changed),
            scratch_directory=tmp_path / "negative",
        )


@pytest.mark.parametrize(
    "change", ("external_path", "opaque_metadata", "unknown_field")
)
def test_catalog_requires_exact_complete_managed_wire(bundle, tmp_path, change):
    root, _, _, _, _, _ = bundle
    catalog, built, inventory = _catalog(bundle, tmp_path)
    value = json.loads(built.canonical_bytes)
    if change == "external_path":
        value["versions"][0]["partitions"][0]["artifact"]["path"] = str(
            tmp_path / "original-outside-store.data"
        )
    elif change == "opaque_metadata":
        value["versions"][0]["partitions"][0]["artifact"]["metadata"][
            "opaque_dependency"
        ] = ("unresolved:sha256:" + "e" * 64)
    else:
        value["foreign_parent"] = {"path": "/unmanaged/evidence.json"}
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    changed = replace(catalog, payload_ref=_payload(root, 7, "json", raw))
    with pytest.raises(ValueError):
        api.admit_native_object(
            root,
            changed,
            (*inventory[:-1], changed),
            scratch_directory=tmp_path / "catalog-negative",
        )


def test_existing_output_is_not_fresh_regeneration(bundle, tmp_path):
    _, _, _, _, recipe, execution = bundle
    with pytest.raises(ValueError, match="fresh nonexistent"):
        api.execute_ascii_cache_recipe(
            RAW, recipe, scratch_directory=tmp_path / "original-execution"
        )
    reused = replace(
        execution,
        decision="reuse_existing",
        cache_created=False,
        reused_existing=True,
    )
    with pytest.raises(ValueError, match="fresh native regeneration"):
        api.verify_regeneration_match(reused, execution.cache_bytes)
    with pytest.raises(ValueError, match="fresh native regeneration"):
        api.verify_regeneration_match(execution, execution.cache_bytes + b"x")


def test_native_reuse_result_is_refused_after_real_call(
    bundle, tmp_path, monkeypatch
):
    import histdatacom.activity_stages as stages

    _, _, _, _, recipe, _ = bundle
    original = stages.build_cache_work_item

    def observed_reuse(*args, **kwargs):
        actual = original(*args, **kwargs)
        actual.result.metrics["reused_existing"] = True
        return actual

    monkeypatch.setattr(stages, "build_cache_work_item", observed_reuse)
    with pytest.raises(ValueError, match="not a fresh regeneration"):
        api.execute_ascii_cache_recipe(
            RAW, recipe, scratch_directory=tmp_path / "reuse-result"
        )


@pytest.mark.parametrize("field", ("implementation_sha256", "backend_id"))
def test_stale_recipe_refuses_before_native_work(
    bundle, tmp_path, monkeypatch, field
):
    import histdatacom.activity_stages as stages

    _, _, _, _, recipe, _ = bundle
    changed = replace(recipe, **{field: "e" * 64})
    monkeypatch.setattr(
        stages,
        "build_cache_work_item",
        lambda *args, **kwargs: pytest.fail("native work preceded admission"),
    )
    with pytest.raises(ValueError, match="stale"):
        api.execute_ascii_cache_recipe(
            RAW, changed, scratch_directory=tmp_path / "must-not-exist"
        )
    assert not (tmp_path / "must-not-exist").exists()


def test_producer_refuses_managed_namespace_before_writes(bundle, tmp_path):
    root, _, _, _, recipe, _ = bundle
    (root / ".histdatacom-retention.json").write_text("closed-store-marker")
    with pytest.raises(ValueError, match="managed store"):
        api.execute_ascii_cache_recipe(
            RAW, recipe, scratch_directory=root / "forbidden-scratch"
        )
    assert not (root / "forbidden-scratch").exists()


@pytest.mark.parametrize(
    "raw",
    (
        b"",
        b"ZIP-not-supported",
        b"timestamp,bid,ask,vol\n",
        b"20200102 000000000,nan,1.2,0\n",
        b"20200102 000000000,1.1,inf,0\n",
        b"20200102 000000000,1.1,1.2,2147483648\n",
        RAW + b"\xff",
        b"\n" * (api.MAX_ASCII_ROWS + 1),
        b"x" * (api.MAX_ASCII_BYTES + 1),
    ),
    ids=(
        "empty",
        "unsupported-format",
        "header",
        "nan-bid",
        "infinite-ask",
        "volume-overflow",
        "non-ascii",
        "too-many-rows",
        "too-many-bytes",
    ),
)
def test_source_closed_bounds_and_numeric_admission(raw):
    with pytest.raises((ValueError, UnicodeError)):
        api.inspect_ascii_source(raw, symbol="EURUSD", period="202001")


@pytest.mark.parametrize(
    ("dataset_id", "cache_ids"),
    (
        (None, ("retention-object:sha256:" + "a" * 64,)),
        ("a" * 257, ("retention-object:sha256:" + "a" * 64,)),
        ("Non Canonical", ("retention-object:sha256:" + "a" * 64,)),
        ("fixture", ([],)),
        ("fixture", (1,)),
        ("fixture", ("foreign",)),
    ),
    ids=("type", "size", "canonical", "unhashable", "numeric", "foreign"),
)
def test_catalog_inputs_refuse_before_any_scratch_or_native_work(
    tmp_path, monkeypatch, dataset_id, cache_ids
):
    monkeypatch.setattr(
        api, "_fresh_scratch", lambda *args: pytest.fail("early scratch work")
    )
    monkeypatch.setattr(
        api, "_verify_cache", lambda *args: pytest.fail("early native work")
    )
    with pytest.raises(ValueError):
        api.build_native_catalog(
            tmp_path,
            cache_ids,
            (),
            dataset_id=dataset_id,
            scratch_directory=tmp_path / "forbidden",
        )


@pytest.mark.parametrize(
    "timestamp",
    ("20191231 235959999", "20200201 000000000"),
)
def test_raw_source_month_is_not_replaced_by_a_caller_label(timestamp):
    raw = (timestamp + ",1.1,1.2,0\n").encode("ascii")
    with pytest.raises(ValueError, match="declared period"):
        api.inspect_ascii_source(raw, symbol="EURUSD", period="202001")


@pytest.mark.parametrize(
    "timestamp",
    ("20200101 000000000", "20200131 235959999"),
)
def test_actual_source_clock_boundaries_allow_native_utc_month_spill(
    tmp_path, timestamp
):
    raw = (timestamp + ",1.1,1.2,0\n").encode("ascii")
    assert (
        api.inspect_ascii_source(
            raw, symbol="EURUSD", period="202001"
        ).row_count
        == 1
    )
    recipe = AsciiTickRecipeV1(
        "retention-object:sha256:" + "a" * 64,
        "EURUSD",
        "202001",
        api.current_cache_implementation_sha256(),
        api.current_cache_backend_id(),
    )
    execution = api.execute_ascii_cache_recipe(
        raw, recipe, scratch_directory=tmp_path / "boundary"
    )
    assert json.loads(execution.partition_json)["row_count"] == 1


@pytest.mark.parametrize("kind", ("symlink", "hardlink", "fifo"))
def test_native_reader_refuses_nonregular_or_aliased_payload(tmp_path, kind):
    root = tmp_path.resolve()
    ref = _payload(root, 1, "csv", RAW)
    path = root / ref.relative_path
    other = root / "other"
    if kind == "hardlink":
        os.link(path, other)
    else:
        path.rename(other)
        if kind == "symlink":
            path.symlink_to(other)
        else:
            os.mkfifo(path)
    with pytest.raises(ValueError):
        api.read_managed_payload(root, ref)


def test_exact_source_and_recipe_have_real_live_edges(bundle, tmp_path):
    root, source, recipe_object, _, _, _ = bundle
    inventory = (source, recipe_object)
    for item in inventory:
        actual = api.admit_native_object(
            root, item, inventory, scratch_directory=tmp_path / "unused"
        )
        assert actual.live_dependencies == item.live_dependencies
    assert not (tmp_path / "unused").exists()
    path = root / source.payload_ref.relative_path
    path.write_bytes(RAW.replace(b"1.100000", b"1.200000"))
    with pytest.raises(ValueError, match="payload bytes differ"):
        api.admit_native_object(
            root,
            recipe_object,
            inventory,
            scratch_directory=tmp_path / "unused",
        )
