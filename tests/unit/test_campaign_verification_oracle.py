"""Native campaign integrity oracle; no historical/model-promotion claim.

The one shared cohort is generated through real data-plane/native publication
operations. Mutations are adversarial inputs, never successful verifier doubles.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from contextlib import contextmanager
from dataclasses import fields, replace
from pathlib import Path
from typing import Any, Iterator

import pytest

from histdatacom import reconstruction as public
from histdatacom.campaign_index_contracts import CampaignDeepVerificationV1
from histdatacom.campaign_verification import (
    inspect_campaign_product_index,
    verify_campaign_product_index,
)
from histdatacom.synthetic import persistence

pytest_plugins = ("tests.fixtures.campaign_verification",)
REFUSAL = (ValueError, OSError, RuntimeError)

PROTECTED_CHECKS = {
    "campaign_product_index_valid": True,
    "campaign_dataset_publication_valid": True,
    "executable_retained_product_missing_count": 0,
    "fabricated_liquidity_terminal_outcome_count": 0,
}


def _json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


@contextmanager
def _changed_bytes(path: Path, content: bytes) -> Iterator[None]:
    """Restore exactly the one test-owned payload even on assertion failure."""
    original = path.read_bytes()
    try:
        path.write_bytes(content)
        yield
    finally:
        path.write_bytes(original)
    assert path.read_bytes() == original


def _index(fixture: Any) -> Any:
    return public.read_reconstruction_campaign_product_index(
        fixture.index_ref.path
    )


def _shard(fixture: Any) -> Any:
    return public.read_reconstruction_campaign_product_shard(
        _index(fixture).shard_refs[0].path
    )


def _with_entries(
    fixture: Any, directory: Path, entries: tuple[Any, ...]
) -> Any:
    shard = replace(_shard(fixture), entries=entries, product_shard_id="")
    ref = public.write_reconstruction_campaign_product_shard(shard, directory)
    index = replace(_index(fixture), shard_refs=(ref,), product_index_id="")
    return public.write_reconstruction_campaign_product_index(index, directory)


def _resealed_publication(fixture: Any, directory: Path, mutation: str) -> Any:
    """Build internally hashed native DTOs, without granting them authority."""
    from histdatacom.datasets import DatasetCatalog, DatasetVersionManifestV1
    from tests.fixtures.campaign_verification import write_json

    old = public.read_reconstruction_campaign_dataset_publication(
        fixture.publication_ref.path
    )
    version = DatasetVersionManifestV1.from_dict(
        _json(old.dataset_version_ref.path)
    )
    changes: dict[str, Any] = {}
    evidence = version.qualification_evidence
    proof_ref = next(
        ref for ref in evidence if ref.kind == "campaign_deep_verification_v1"
    )
    if mutation == "normalization":
        changes["normalization_policy_id"] = "invented-normalization-v1"
    elif mutation in {"parent_role", "parent_ordinal"}:
        changes["parents"] = (
            replace(
                version.parents[0],
                **(
                    {"role": "invented-parent-role"}
                    if mutation == "parent_role"
                    else {"ordinal": 1}
                ),
                lineage_id="",
            ),
        )
    elif mutation == "removed_proof":
        changes["qualification_evidence"] = tuple(
            ref for ref in evidence if ref != proof_ref
        )
    elif mutation == "extra_evidence":
        extra = write_json(
            directory,
            "unrelated",
            {"schema_version": "synthetic.unrelated.v1", "claim": False},
            kind="synthetic_unrelated_v1",
        )
        changes["qualification_evidence"] = (*evidence, extra)
    elif mutation in {"foreign_proof", "stale_proof"}:
        proof = replace(
            fixture.verification,
            **(
                {
                    "index_id": "reconstruction-campaign-product-index:sha256:"
                    + "f" * 64
                }
                if mutation == "foreign_proof"
                else {"verified_inputs_sha256": "f" * 64}
            ),
        )
        assert proof != fixture.verification
        ref = write_json(
            directory,
            mutation,
            proof,
            kind="campaign_deep_verification_v1",
        )
        changes["qualification_evidence"] = tuple(
            ref if item == proof_ref else item for item in evidence
        )
    else:
        raise AssertionError(mutation)
    forged = replace(
        version, **changes, dataset_version_id="", manifest_sha256=""
    )
    assert DatasetVersionManifestV1.from_dict(forged.to_dict()) == forged
    version_ref = write_json(
        directory,
        "dataset-version",
        forged,
        kind="dataset_version_manifest_v1",
        metadata={"dataset_version_id": forged.dataset_version_id},
    )
    catalog = DatasetCatalog.read(old.catalog_ref.path)
    catalog = replace(
        catalog,
        versions=tuple(
            forged if item == version else item for item in catalog.versions
        ),
        aliases=tuple(
            (
                replace(
                    alias,
                    dataset_version_id=forged.dataset_version_id,
                    alias_id="",
                )
                if alias.dataset_version_id == version.dataset_version_id
                else alias
            )
            for alias in catalog.aliases
        ),
        catalog_id="",
    )
    catalog_ref = write_json(
        directory, "catalog", catalog, kind="dataset_catalog_v1"
    )
    return replace(
        old,
        dataset_version_ref=version_ref,
        catalog_ref=catalog_ref,
        synthetic_dataset_version_id=forged.dataset_version_id,
        publication_id="",
    )


@pytest.mark.parametrize(
    "mutation",
    (
        "normalization",
        "parent_role",
        "parent_ordinal",
        "removed_proof",
        "extra_evidence",
        "foreign_proof",
        "stale_proof",
    ),
)
def test_resealed_publication_graph_never_grants_material_authority(
    verified_campaign: Any, tmp_path: Path, mutation: str
) -> None:
    from tests.fixtures.campaign_verification import write_json

    forged = _resealed_publication(
        verified_campaign, tmp_path / "inputs", mutation
    )
    if mutation in {"removed_proof", "stale_proof"}:
        # A legacy envelope/current-looking receipt is structurally readable,
        # but material publication independently requires fresh exact replay.
        raw_ref = write_json(
            tmp_path / "inputs",
            "structural-only",
            forged,
            kind="reconstruction_campaign_dataset_publication_v1",
        )
        assert (
            public.read_reconstruction_campaign_dataset_publication(
                raw_ref.path
            )
            == forged
        )
    output = tmp_path / "unauthorized-publication"
    with pytest.raises(REFUSAL):
        public.write_reconstruction_campaign_dataset_publication(forged, output)
    assert not output.exists()


@pytest.mark.parametrize("kind", ("publication", "artifact_ref"))
def test_publication_writer_rejects_overridden_native_serializers(
    verified_campaign: Any, tmp_path: Path, kind: str
) -> None:
    from histdatacom.runtime_contracts import ArtifactRef

    called = []

    class PoisonPublication(public.ReconstructionCampaignDatasetPublicationV1):
        def to_dict(self) -> Any:
            called.append("publication")
            raise AssertionError("publication serializer must not run")

    class PoisonArtifactRef(ArtifactRef):
        def to_dict(self) -> Any:
            called.append("artifact_ref")
            raise AssertionError("artifact serializer must not run")

    publication = public.read_reconstruction_campaign_dataset_publication(
        verified_campaign.publication_ref.path
    )
    original = (
        publication if kind == "publication" else publication.product_index_ref
    )
    poisoned = object.__new__(
        PoisonPublication if kind == "publication" else PoisonArtifactRef
    )
    for field in fields(original):
        object.__setattr__(poisoned, field.name, getattr(original, field.name))
    if kind == "publication":
        publication = poisoned
        message = "exact native publication type"
    else:
        # A frozen-object bypass is adversarial input, not constructor authority.
        object.__setattr__(publication, "product_index_ref", poisoned)
        message = "exact native artifact references"
    output = tmp_path / "unauthorized-publication"
    with pytest.raises(public.ReconstructionPlanError, match=message):
        public.write_reconstruction_campaign_dataset_publication(
            publication, output
        )
    assert called == []
    assert not output.exists()


def test_publication_writer_readmits_bypass_mutated_exact_native_identity(
    verified_campaign: Any, tmp_path: Path
) -> None:
    publication = public.read_reconstruction_campaign_dataset_publication(
        verified_campaign.publication_ref.path
    )
    object.__setattr__(publication, "publication_id", "stale-publication-id")
    output = tmp_path / "unauthorized-publication"
    with pytest.raises(
        public.ReconstructionPlanError,
        match="campaign dataset publication identity differs",
    ):
        public.write_reconstruction_campaign_dataset_publication(
            publication, output
        )
    assert not output.exists()


def _certification_spec(
    fixture: Any, *, index_ref: Any = None, support_ref: Any = None
) -> Any:
    from histdatacom.synthetic.certification_campaign import (
        CertificationCampaignArtifactV1,
        CertificationCampaignObservationV1,
        ModernReferenceCertificationCampaignSpecV1,
    )
    from tests.fixtures.campaign_verification import NONCLAIM

    index_ref = fixture.index_ref if index_ref is None else index_ref
    support_ref = (
        _index(fixture).support_map_ref if support_ref is None else support_ref
    )
    artifacts = []
    from histdatacom.datasets import DatasetVersionManifestV1

    publication = public.read_reconstruction_campaign_dataset_publication(
        fixture.publication_ref.path
    )
    version = DatasetVersionManifestV1.from_dict(
        _json(publication.dataset_version_ref.path)
    )
    root_ref = next(
        ref
        for ref in version.qualification_evidence
        if ref.kind == "campaign_verification_root_v1"
    )
    for key, kind, ref, identity in (
        (
            "index",
            "reconstruction-campaign-product-index",
            index_ref,
            "product_index_id",
        ),
        (
            "publication",
            "reconstruction-campaign-dataset-publication",
            fixture.publication_ref,
            "publication_id",
        ),
        (
            "root",
            "campaign-verification-root",
            root_ref,
            "artifact_id",
        ),
        (
            "support",
            "reconstruction-plan-support-map",
            support_ref,
            "support_map_id",
        ),
    ):
        payload = _json(ref.path)
        artifacts.append(
            CertificationCampaignArtifactV1(
                evidence_key=key,
                kind=kind,
                path=ref.path,
                content_sha256=ref.sha256,
                subject_id=payload[identity],
                subject_id_pointer=f"/{identity}",
                subject_schema_version=payload["schema_version"],
                relative_path=f"inputs/{key}.json",
                metadata={"synthetic_integrity_fixture": True},
            )
        )
    keys = {
        "campaign_product_index_valid": ("index", "root"),
        "campaign_dataset_publication_valid": ("index", "publication", "root"),
        "executable_retained_product_missing_count": (
            "index",
            "support",
            "root",
        ),
        "fabricated_liquidity_terminal_outcome_count": (
            "index",
            "support",
            "root",
        ),
    }
    return ModernReferenceCertificationCampaignSpecV1(
        common_end_period="201506",
        peak_memory_budget_bytes=1024**3,
        scratch_budget_bytes=1024**3,
        runtime_budget_seconds=3600.0,
        storage_budget_bytes=1024**3,
        candidate_amplification_budget=8.0,
        artifacts=tuple(artifacts),
        observations=tuple(
            CertificationCampaignObservationV1(
                check_id=check,
                measurement_evidence_key="index",
                measurement_pointer="/nonexistent_caller_scalar",
                artifact_evidence_keys=values,
                note="This declaration cannot establish product verification.",
            )
            for check, values in keys.items()
        ),
        methodology=NONCLAIM,
        accepted_limitations=(NONCLAIM,),
        blocking_limitations=(),
    )


def _run_certification(spec: Any, directory: Path) -> Any:
    from histdatacom.synthetic.certification_campaign import (
        run_modern_reference_certification_campaign,
    )
    from tests.fixtures.campaign_verification import write_json

    ref = write_json(directory, "spec", spec, kind="synthetic_campaign_spec")
    return run_modern_reference_certification_campaign(
        ref.path, output_directory=directory / "result"
    )


def test_real_certification_derives_four_checks_not_caller_scalars(
    verified_campaign: Any, tmp_path: Path
) -> None:
    from histdatacom.synthetic.certification import (
        CertificationCheckStatus,
        CertificationState,
    )

    spec = _certification_spec(verified_campaign)
    assert "nonexistent_caller_scalar" not in _json(
        verified_campaign.index_ref.path
    )
    dossier, _ = _run_certification(spec, tmp_path)
    checks = {
        check.check_id: check
        for gate in dossier.gate_results
        for check in gate.check_results
    }
    for name, actual in PROTECTED_CHECKS.items():
        assert checks[name].status is CertificationCheckStatus.PASSED
        assert type(checks[name].actual) is type(actual)
        assert checks[name].actual == actual
    # Product integrity never supplies missing scientific/promotion evidence.
    assert dossier.state is CertificationState.INCOMPLETE
    assert any(
        check.status is CertificationCheckStatus.MISSING
        for check in checks.values()
    )


@pytest.mark.parametrize("mutation", ("foreign_support", "mixed_indexes"))
def test_certification_requires_one_exact_index_support_publication_graph(
    verified_campaign: Any, tmp_path: Path, mutation: str
) -> None:
    from tests.fixtures.campaign_verification import write_json

    fixture = verified_campaign
    if mutation == "foreign_support":
        old = public.read_reconstruction_plan_support_map(
            _index(fixture).support_map_ref.path
        )
        changed = replace(
            old,
            resource_summary={**old.resource_summary, "unrelated_note": 1},
            support_map_id="",
        )
        ref = write_json(
            tmp_path / "inputs",
            "foreign-support",
            changed,
            kind="reconstruction_plan_support_map_v1",
        )
        spec = _certification_spec(fixture, support_ref=ref)
    else:
        # Relocate the exact existing native shard; both complete indexes
        # independently verify, but their content-bound graph roots differ.
        shard_ref = public.write_reconstruction_campaign_product_shard(
            _shard(fixture), tmp_path / "other-index"
        )
        other = replace(
            _index(fixture), shard_refs=(shard_ref,), product_index_id=""
        )
        ref = public.write_reconstruction_campaign_product_index(
            other, tmp_path / "other-index"
        )
        assert other.product_index_id != _index(fixture).product_index_id
        assert verify_campaign_product_index(ref.path).status == "complete"
        spec = _certification_spec(fixture, index_ref=ref)
    with pytest.raises(REFUSAL):
        _run_certification(spec, tmp_path / "campaign")
    assert not (tmp_path / "campaign" / "result").exists()


def test_actual_same_run_product_cannot_hide_in_resealed_refused_support(
    verified_campaign: Any, tmp_path: Path
) -> None:
    """Adversarial terminal-plan replacement, not a real refusal calibration.

    The retained products and their actual reconstructed rows are unchanged.
    Rehashing the plan as zero-work cannot relabel those rows as harmless
    out-of-plan evidence, even though its terminal entry has no member roster.
    """
    from histdatacom.synthetic import reconstruction_plan as plans
    from tests.fixtures.campaign_verification import write_json

    fixture = verified_campaign
    old = fixture.native.plan
    refusal = plans.ReconstructionPlanRefusalV1(
        start_ns=old.requested_start_ns,
        end_ns=old.requested_end_ns,
        code=plans.ReconstructionPlanRefusalCode.OBSERVATION_CARDINALITY_UNSUPPORTED,
        reason="Adversarial replacement of an already materialized window.",
    )
    resources = replace(
        old.resources,
        executable_window_count=0,
        refused_window_count=1,
        workflow_request_count=0,
        estimated_input_event_count=0,
        estimated_candidate_event_count=0,
        estimated_candidate_bytes=0,
        estimated_peak_memory_bytes=0,
        estimated_peak_scratch_bytes=0,
        estimated_output_bytes=0,
        estimated_partition_count=0,
        summary_id="",
    )
    execution = plans.read_reconstruction_plan_execution_manifest(
        old.artifact_graph["execution_manifest"].path
    )
    execution = replace(
        execution,
        executable_window_count=0,
        refusal_ids=(refusal.refusal_id,),
        manifest_id="",
    )
    execution_ref = write_json(
        tmp_path,
        "reconstruction-plan-execution",
        execution,
        kind=plans.PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND,
        metadata={"manifest_id": execution.manifest_id},
    )
    terminal = replace(
        old,
        workflow_requests=(),
        execution_manifest_id=execution.manifest_id,
        artifact_graph={
            **old.artifact_graph,
            "execution_manifest": execution_ref,
        },
        resources=resources,
        refusals=(refusal,),
        plan_id="",
    )
    plan_ref = plans.write_synthetic_infill_plan(terminal, tmp_path)
    original_set = public.read_reconstruction_plan_set(
        fixture.native.plan_set_path
    )
    shard = replace(
        original_set.shards[0],
        plan_id=terminal.plan_id,
        plan_ref=plan_ref,
        preflight_status=terminal.status,
        refusal_count=1,
        resource_summary=resources.to_dict(),
        shard_id="",
    )
    summaries, partitions = [], {}
    public._accumulate_plan_set_resources(
        terminal, resource_summaries=summaries, source_partitions=partitions
    )
    plan_set = replace(
        original_set,
        shards=(shard,),
        status=terminal.status,
        resource_summary=public._aggregate_plan_set_resources(
            summaries, partitions
        ),
        plan_set_id="",
    )
    plan_set_ref = public.write_reconstruction_plan_set(plan_set, tmp_path)
    client = public.ReconstructionClient()
    support_ref = client.construct_plan_support_map(
        plan_set_ref.path, output_directory=tmp_path
    )
    support = public.read_reconstruction_plan_support_map(support_ref.path)
    assert len(support.windows) == 1
    assert support.windows[0].status == "refused"
    assert support.windows[0].member_ids == ()
    for product in fixture.native.products:
        actual = persistence.verify_reconstruction_publication(
            product.manifest_path
        )
        assert actual.run_id == terminal.run.run_id
        assert any(
            terminal.requested_start_ns
            <= event.event_time_ns
            < terminal.requested_end_ns
            for stream in persistence.read_reconstruction_streams(
                product.manifest_path
            )
            for event in stream.events
        )
    with pytest.raises(REFUSAL, match="same-run empty/refused support"):
        client.construct_verified_campaign_product_index(
            plan_set_ref.path,
            support_ref.path,
            output_directory=tmp_path / "unauthorized-index",
        )


def test_genuine_two_member_native_v3_local_cross_and_publication(
    verified_campaign: Any,
) -> None:
    from histdatacom.datasets import DatasetVersionManifestV1

    fixture = verified_campaign
    assert len(fixture.native.products) == 2
    assert len(fixture.native.local_results) == 6
    expected_observed = expected_synthetic = 0
    for product, final in zip(
        fixture.native.products, fixture.native.final_validations
    ):
        manifest = persistence.verify_reconstruction_publication(
            product.manifest_path
        )
        assert type(manifest) is persistence.ReconstructionProductManifestV3
        assert (
            final.passed
            and manifest.quality.final_validation_id == final.validation_id
        )
        assert (
            manifest.quality.benchmark_evidence[
                "scientific_promotion_evaluated"
            ]
            is False
        )
        assert (
            manifest.quality.benchmark_evidence["candidate_promotion_eligible"]
            is False
        )
        streams = persistence.read_reconstruction_streams(product.manifest_path)
        assert {stream.symbol for stream in streams} == {
            "eurgbp",
            "eurusd",
            "gbpusd",
        }
        for stream in streams:
            # Literal input denominator: exactly two immutable core anchors.
            assert stream.observed_event_count == 2
            assert stream.synthetic_event_count > 0
            assert all(event.bid <= event.ask for event in stream.events)
        expected_observed += sum(
            stream.observed_event_count for stream in streams
        )
        expected_synthetic += sum(
            stream.synthetic_event_count for stream in streams
        )
    assert expected_observed == 12
    receipt = verify_campaign_product_index(fixture.index_ref.path)
    assert receipt == fixture.verification
    assert receipt.status == "complete"
    assert receipt.product_count == 2
    assert (
        receipt.missing_product_count
        == receipt.refused_window_count
        == receipt.empty_window_count
        == 0
    )
    assert receipt.observed_event_count == expected_observed
    assert receipt.synthetic_event_count == expected_synthetic > 0
    publication = public.read_reconstruction_campaign_dataset_publication(
        fixture.publication_ref.path
    )
    version = DatasetVersionManifestV1.from_dict(
        _json(publication.dataset_version_ref.path)
    )
    assert (
        version.normalization_policy_id
        == "reconstruction-campaign-product-index-v1"
    )
    assert len(version.parents) == 1
    assert version.parents[0].role == "immutable-observed-histdata-anchor"
    assert version.parents[0].ordinal == 0
    assert len(version.qualification_evidence) == 6
    proofs = [
        item
        for item in version.qualification_evidence
        if item.kind == "campaign_deep_verification_v1"
    ]
    assert len(proofs) == 1
    assert (
        CampaignDeepVerificationV1.from_dict(_json(proofs[0].path)) == receipt
    )


def test_structural_inspection_is_distinct_from_fresh_authority(
    verified_campaign: Any,
) -> None:
    structural = inspect_campaign_product_index(
        verified_campaign.index_ref.path
    )
    assert structural.LEVEL == "structural_only_products_unverified"
    assert (
        verified_campaign.verification.LEVEL == "fresh_deep_native_validation"
    )
    assert (
        structural.to_dict()["schema_version"]
        != verified_campaign.verification.to_dict()["schema_version"]
    )
    with pytest.raises((TypeError, ValueError)):
        CampaignDeepVerificationV1.from_dict(structural.to_dict())


def test_real_manifest_inventory_never_substitutes_for_index(
    verified_campaign: Any, tmp_path: Path
) -> None:
    inventory = public.ReconstructionClient().inventory_campaign_products(
        verified_campaign.native.plan_set_path,
        output_directory=tmp_path / "inventory",
    )
    assert inventory.kind != verified_campaign.index_ref.kind
    payload = _json(inventory.path)
    assert payload["publication_eligible"] is False
    with pytest.raises(REFUSAL):
        public.ReconstructionClient().publish_campaign_dataset(
            inventory.path, output_directory=tmp_path / "forbidden"
        )
    assert not (tmp_path / "forbidden").exists()


def test_deep_replay_does_not_depend_on_deleted_stage_scratch(
    verified_campaign: Any,
) -> None:
    from histdatacom.reconstruction_storage import (
        RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER,
    )

    root = verified_campaign.native.root
    scratch, retained = root / "scratch", root / "retained-scratch-oracle"
    assert scratch.is_dir() and not retained.exists()
    marker = scratch / RECONSTRUCTION_STORAGE_ROOT_GUARD_MARKER
    marker_bytes = marker.read_bytes()
    marker_identity = (marker.stat().st_dev, marker.stat().st_ino)
    root_identity = (scratch.stat().st_dev, scratch.stat().st_ino)
    directories = tuple(
        sorted(
            {
                Path(task.scratch_directory)
                for request in verified_campaign.native.plan.workflow_requests
                for task in request.tasks
            }
        )
    )
    assert len(directories) == 2
    assert all(path.parent == scratch and path.is_dir() for path in directories)
    retained.mkdir()
    moved = []
    try:
        for path in directories:
            destination = retained / path.name
            path.rename(destination)
            moved.append((path, destination))
        assert all(not path.exists() for path in directories)
        # Root admission is durable authority, not disposable stage output.
        assert marker.read_bytes() == marker_bytes
        assert (marker.stat().st_dev, marker.stat().st_ino) == marker_identity
        assert (scratch.stat().st_dev, scratch.stat().st_ino) == root_identity
        actual = verify_campaign_product_index(verified_campaign.index_ref.path)
        assert actual == verified_campaign.verification
    finally:
        for path, destination in reversed(moved):
            destination.rename(path)
        retained.rmdir()
    assert marker.read_bytes() == marker_bytes
    assert (marker.stat().st_dev, marker.stat().st_ino) == marker_identity


@pytest.mark.parametrize(
    "kind", ["parquet", "source", "manifest", "configuration", "support"]
)
def test_changed_actual_bytes_after_receipt_are_not_authorized(
    verified_campaign: Any, tmp_path: Path, kind: str
) -> None:
    fixture = verified_campaign
    product = fixture.native.products[0]
    if kind == "parquet":
        assert product.manifest.partitions
        path = (
            product.manifest_path.parent
            / product.manifest.partitions[0].relative_path
        )
    elif kind == "source":
        path = fixture.native.sources.root / "eurusd" / "2015" / "6" / ".data"
    elif kind == "manifest":
        path = product.manifest_path
    elif kind == "configuration":
        path = Path(fixture.native.plan.artifact_graph["configuration"].path)
    else:
        path = fixture.native.support_map_path
    content = path.read_bytes()
    offset = len(content) // 2
    changed = (
        content[:offset] + bytes([content[offset] ^ 1]) + content[offset + 1 :]
    )
    assert (
        hashlib.sha256(changed).hexdigest()
        != hashlib.sha256(content).hexdigest()
    )
    with _changed_bytes(path, changed):
        # A previously obtained success object is deliberately left in scope.
        assert fixture.verification.status == "complete"
        with pytest.raises(REFUSAL):
            verify_campaign_product_index(fixture.index_ref.path)
        with pytest.raises(REFUSAL):
            public.ReconstructionClient().publish_campaign_dataset(
                fixture.index_ref.path, output_directory=tmp_path / "blocked"
            )
        assert not (tmp_path / "blocked").exists()


@pytest.mark.parametrize(
    "name",
    [
        "support_window_count",
        "verified_product_count",
        "observed_event_count",
        "synthetic_event_count",
    ],
)
def test_resigned_descriptor_counts_cannot_replace_actual_shard(
    verified_campaign: Any, tmp_path: Path, name: str
) -> None:
    index = _index(verified_campaign)
    original = index.shard_refs[0]
    forged_ref = replace(
        original,
        metadata={**original.metadata, name: original.metadata[name] + 1},
    )
    forged = replace(index, shard_refs=(forged_ref,), product_index_id="")
    ref = public.write_reconstruction_campaign_product_index(forged, tmp_path)
    with pytest.raises(REFUSAL):
        verify_campaign_product_index(ref.path)


@pytest.mark.parametrize(
    "field",
    [
        "observed_event_count",
        "synthetic_event_count",
        "support_id",
        "window_id",
        "ensemble_member_id",
    ],
)
def test_resealed_actual_row_projection_must_equal_plan_and_product(
    verified_campaign: Any, tmp_path: Path, field: str
) -> None:
    rows = _shard(verified_campaign).entries
    value = getattr(rows[0], field)
    altered = value + 1 if type(value) is int else str(value) + ":foreign"
    if field == "support_id":
        # All members share one support window. Keep that structural grouping
        # intact while forging its claimed binding to actual native support.
        forged_rows = tuple(
            replace(row, support_id=altered, entry_id="") for row in rows
        )
    else:
        product_ref = rows[0].product_ref
        assert product_ref is not None
        product_ref = replace(
            product_ref,
            metadata={**product_ref.metadata, field: altered},
        )
        row = replace(
            rows[0],
            **{field: altered, "product_ref": product_ref, "entry_id": ""},
        )
        forged_rows = (row, *rows[1:])
    for row in forged_rows:
        assert (
            public.ReconstructionCampaignProductEntryV1.from_dict(row.to_dict())
            == row
        )
    ref = _with_entries(verified_campaign, tmp_path, forged_rows)
    # The native index/shard/row graph is internally consistent, not fresh proof.
    admitted = public.read_reconstruction_campaign_product_index(ref.path)
    admitted_shard = public.read_reconstruction_campaign_product_shard(
        admitted.shard_refs[0].path
    )
    assert sorted(
        admitted_shard.entries, key=lambda row: row.entry_id
    ) == sorted(forged_rows, key=lambda row: row.entry_id)
    reason = {
        "observed_event_count": "product metadata observed_event_count differs",
        "synthetic_event_count": "product metadata synthetic_event_count differs",
        "support_id": "campaign row is duplicate or outside actual support",
        "window_id": "campaign row window differs from executable task",
        "ensemble_member_id": "campaign row is duplicate or outside actual support",
    }[field]
    with pytest.raises(ValueError, match=reason):
        verify_campaign_product_index(ref.path)


def test_fully_resealed_omission_cannot_shrink_required_rectangle(
    verified_campaign: Any, tmp_path: Path
) -> None:
    rows = _shard(verified_campaign).entries
    assert len(rows) == 2
    ref = _with_entries(verified_campaign, tmp_path, rows[:1])
    # The remaining entry and newly derived hashes are structurally genuine.
    public.read_reconstruction_campaign_product_index(ref.path)
    with pytest.raises(REFUSAL, match="denominator|rectangle|omits"):
        verify_campaign_product_index(ref.path)


def test_duplicate_member_coordinate_is_not_extra_support(
    verified_campaign: Any, tmp_path: Path
) -> None:
    rows = _shard(verified_campaign).entries
    # Alter a descriptive count to give the duplicate a distinct row ID.
    product_ref = rows[0].product_ref
    assert product_ref is not None
    altered_count = rows[0].synthetic_event_count + 1
    duplicate = replace(
        rows[0],
        synthetic_event_count=altered_count,
        product_ref=replace(
            product_ref,
            metadata={
                **product_ref.metadata,
                "synthetic_event_count": altered_count,
            },
        ),
        entry_id="",
    )
    assert duplicate.entry_id != rows[0].entry_id
    assert (
        public.ReconstructionCampaignProductEntryV1.from_dict(
            duplicate.to_dict()
        )
        == duplicate
    )
    # The native shard explicitly forbids duplicate member coordinates before
    # it can become persisted input to the fresh verifier.
    with pytest.raises(
        public.ReconstructionPlanError,
        match="campaign product support rectangle has duplicate members",
    ):
        _with_entries(verified_campaign, tmp_path, (*rows, duplicate))


@pytest.mark.parametrize(
    "field",
    [
        "selected_proposal_engine_ids",
        "observed_dataset_version_id",
        "delivery_profile_id",
    ],
)
def test_resealed_index_cannot_change_native_assignment(
    verified_campaign: Any, tmp_path: Path, field: str
) -> None:
    values = {
        "selected_proposal_engine_ids": ("unqualified-foreign-engine",),
        "observed_dataset_version_id": "dataset-version:sha256:" + "f" * 64,
        "delivery_profile_id": "foreign-delivery",
    }
    forged = replace(
        _index(verified_campaign),
        **{field: values[field], "product_index_id": ""},
    )
    ref = public.write_reconstruction_campaign_product_index(forged, tmp_path)
    with pytest.raises(REFUSAL):
        verify_campaign_product_index(ref.path)


@contextmanager
def _product_variant(fixture: Any, manifest: Any) -> Iterator[Path]:
    """Reseal a test-owned product at the real native commit-name boundary."""
    original = fixture.native.products[0].manifest_path.parent
    target = original.parent / persistence._path_component(
        manifest.publication_id
    )
    # The full manifest binds min/max but the native publication ID does not.
    # Keep the required commit name even when this variant has the same ID,
    # preserving the pristine directory outside the scanned output root.
    retained = fixture.native.root / (
        "retained-original-product-oracle-"
        + manifest.manifest_id.rsplit(":", 1)[-1]
    )
    assert (target == original or not target.exists()) and not retained.exists()
    original.rename(retained)
    try:
        shutil.copytree(retained, target)
        (target / "manifest.json").write_text(
            manifest.to_json(), encoding="utf-8"
        )
        yield target / "manifest.json"
    finally:
        # Only the exact new test-owned directory is removed.
        if target.exists():
            shutil.rmtree(target)
        retained.rename(original)


@pytest.mark.parametrize(
    "field",
    [
        "final_validation_id",
        "delivery_output_content_sha256",
        "generator_config_id",
        "proposal_engine_id",
        "conditioning_state_ids",
        "transition_scenario_id",
        "logical_min_event_time_ns",
        "logical_max_event_time_ns",
    ],
)
def test_native_valid_resealed_claims_require_actual_recomputation(
    verified_campaign: Any, tmp_path: Path, field: str
) -> None:
    old = verified_campaign.native.products[0].manifest
    benchmark = json.loads(json.dumps(old.quality.benchmark_evidence))
    quality_changes: dict[str, Any] = {}
    manifest_changes: dict[str, Any] = {}
    if field == "final_validation_id":
        quality_changes[field] = "cross-currency-validation:foreign"
    elif field == "delivery_output_content_sha256":
        quality_changes[field] = "f" * 64
    elif field == "logical_min_event_time_ns":
        manifest_changes[field] = old.logical_min_event_time_ns - 1
    elif field == "logical_max_event_time_ns":
        manifest_changes[field] = old.logical_max_event_time_ns + 1
    elif field == "conditioning_state_ids":
        benchmark[field] = {
            "market_context": "foreign",
            "cftc_positioning": "foreign",
        }
    else:
        benchmark["runtime_proposal_evidence"][field] = "foreign"
    quality = replace(
        old.quality,
        **quality_changes,
        benchmark_evidence=benchmark,
        quality_manifest_id="",
    )
    forged = replace(
        old,
        quality=quality,
        **manifest_changes,
        publication_id="",
        manifest_id="",
    )
    with _product_variant(verified_campaign, forged) as path:
        # Native persisted-byte validity is necessary but not campaign authority.
        assert persistence.verify_reconstruction_publication(path) == forged
        reason = (
            "product logical minimum differs from actual events"
            if field == "logical_min_event_time_ns"
            else (
                "product logical maximum differs from actual events"
                if field == "logical_max_event_time_ns"
                else None
            )
        )
        with pytest.raises(REFUSAL, match=reason):
            public.ReconstructionClient().construct_verified_campaign_product_index(
                verified_campaign.native.plan_set_path,
                verified_campaign.native.support_map_path,
                output_directory=tmp_path / "blocked-index",
            )


def test_missing_actual_product_cannot_reuse_old_complete_receipt(
    verified_campaign: Any, tmp_path: Path
) -> None:
    original = verified_campaign.native.products[0].manifest_path.parent
    retained = verified_campaign.native.root / "retained-missing-product-oracle"
    original.rename(retained)
    try:
        with pytest.raises(REFUSAL):
            verify_campaign_product_index(verified_campaign.index_ref.path)
        current = public.ReconstructionClient().construct_verified_campaign_product_index(
            verified_campaign.native.plan_set_path,
            verified_campaign.native.support_map_path,
            output_directory=tmp_path / "incomplete",
        )
        receipt = verify_campaign_product_index(current.path)
        assert (
            receipt.status != "complete" and receipt.missing_product_count == 1
        )
        assert receipt.product_count == 1
        with pytest.raises(REFUSAL):
            public.ReconstructionClient().publish_campaign_dataset(
                current.path, output_directory=tmp_path / "blocked"
            )
    finally:
        retained.rename(original)


def test_fully_rehashed_numerical_delta_requires_fresh_cross_validation(
    verified_campaign: Any, tmp_path: Path
) -> None:
    """Native storage integrity is necessary, not numerical validation.

    This adversarial product retains the original now-stale quality report.
    No new passing report, producer, seed, or relaxed constraint is supplied.
    """
    from histdatacom.orchestration.reconstruction import (
        ReconstructionStage,
        ReconstructionStageInvocationV1,
        ReconstructionStageOutcomeV1,
    )
    from histdatacom.synthetic import reconstruction_handlers as handlers
    from histdatacom.synthetic.contracts import SyntheticEventOrigin
    from histdatacom.synthetic.cross_currency import (
        CrossCurrencyConditionV1,
        CrossCurrencyValidationStage,
        validate_cross_currency_output,
    )
    from histdatacom.synthetic.reconstruction_plan import (
        load_reconstruction_stage_plan,
    )

    fixture = verified_campaign
    product = fixture.native.products[0]
    old = product.manifest
    original_streams = persistence.read_reconstruction_streams(
        product.manifest_path
    )
    target_stream = next(
        stream for stream in original_streams if stream.symbol == "eurgbp"
    )
    selected = tuple(
        event
        for event in target_stream.events
        if event.origin is SyntheticEventOrigin.SYNTHETIC
    )
    assert len(selected) == 1
    target_event = selected[0]
    changed = replace(
        target_event,
        bid=target_event.bid + 0.1,
        ask=target_event.ask + 0.1,
        event_id="",
    )
    # Native event identity commits lineage, not quote content. A numerical
    # alteration therefore preserves the correct ID but changes logical bytes.
    assert changed.event_id == target_event.event_id
    assert (changed.bid, changed.ask) != (target_event.bid, target_event.ask)
    assert 0 < changed.bid <= changed.ask
    changed_stream = replace(
        target_stream,
        events=tuple(
            changed if event == target_event else event
            for event in target_stream.events
        ),
        stream_id="",
    )
    changed_streams = tuple(
        changed_stream if stream == target_stream else stream
        for stream in original_streams
    )
    observed = tuple(
        event
        for stream in original_streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    )
    task = next(
        task
        for request in fixture.native.plan.workflow_requests
        for task in request.tasks
        if task.window.window_id == old.window_id
    )
    command = next(
        command
        for command in task.commands
        if command.stage is ReconstructionStage.VALIDATION
    )
    retained_outcomes = tuple(
        (Path(task.scratch_directory) / "fixture-outcomes").glob(
            ReconstructionStage.SOURCE_ENRICHMENT.value + "-*.json"
        )
    )
    assert len(retained_outcomes) == 1
    outcome = ReconstructionStageOutcomeV1.from_dict(
        _json(retained_outcomes[0])
    )
    invocation = ReconstructionStageInvocationV1(
        fixture.native.plan.run, task, command, (outcome,)
    )
    source = handlers._prior_manifest(
        invocation, handlers.SOURCE_STAGE_ARTIFACT_KIND
    )
    stage = load_reconstruction_stage_plan(command)
    original_validation = fixture.native.final_validations[0]

    def actual_validation(streams: Any) -> Any:
        return validate_cross_currency_output(
            run=fixture.native.plan.run,
            window=task.window,
            streams={stream.symbol: stream for stream in streams},
            config=stage.configuration.cross_currency_config,
            stage=CrossCurrencyValidationStage.POST_BROKER,
            observed_anchors=observed,
            conditions=(
                CrossCurrencyConditionV1.from_dict(source["cross_condition"]),
            ),
            join_policy=original_validation.join_policy,
            nearest_prior_max_age_ns=original_validation.nearest_prior_max_age_ns,
        )

    assert actual_validation(original_streams) == original_validation
    failed = actual_validation(changed_streams)
    assert failed.anchor_preserved
    assert not failed.passed
    assert any(
        reason.startswith("relationship_residual_exceeded:")
        for reason in failed.failure_reasons
    )
    deltas = tuple(
        replace(
            stream,
            events=tuple(
                event
                for event in stream.events
                if event.origin is SyntheticEventOrigin.SYNTHETIC
            ),
            stream_id="",
        )
        for stream in changed_streams
    )
    partition_root = tmp_path / "adversarial-deltas"
    partitions = persistence._write_product_partitions(
        partition_root,
        deltas,
        row_group_size=old.replay.row_group_size,
        synthetic_delta=True,
        anchor_event_ids=tuple(event.event_id for event in observed),
    )
    logical = tuple(
        event for stream in changed_streams for event in stream.events
    )
    replay = replace(
        old.replay,
        logical_content_sha256=persistence.reconstruction_logical_content_sha256(
            logical
        ),
        partition_byte_sha256=persistence._partition_byte_digest(partitions),
        replay_manifest_id="",
    )
    assert replay.logical_content_sha256 != old.replay.logical_content_sha256
    assert replay.partition_byte_sha256 != old.replay.partition_byte_sha256
    forged = replace(
        old,
        partitions=partitions,
        constraints=persistence._constraint_manifest(logical),
        replay=replay,
        publication_id="",
        manifest_id="",
    )
    assert forged.quality == old.quality
    assert forged.source == old.source
    with _product_variant(fixture, forged) as path:
        for partition in partitions:
            shutil.copyfile(
                partition_root / partition.relative_path,
                path.parent / partition.relative_path,
            )
        # This positive is only real byte/logical/native storage admission of
        # an adversarial artifact; its numerical authority must still fail.
        assert persistence.verify_reconstruction_publication(path) == forged
        readback = persistence.read_reconstruction_streams(path)
        assert readback == changed_streams
        with pytest.raises(
            REFUSAL, match="fresh final cross-currency validation differs"
        ):
            public.ReconstructionClient().construct_verified_campaign_product_index(
                fixture.native.plan_set_path,
                fixture.native.support_map_path,
                output_directory=tmp_path / "unauthorized-index",
            )


def test_out_of_plan_product_is_reported_but_not_counted(
    verified_campaign: Any,
) -> None:
    old = verified_campaign.native.products[0].manifest
    foreign = replace(
        old,
        window_id="foreign-window-not-in-plan",
        publication_id="",
        manifest_id="",
    )
    original = verified_campaign.native.products[0].manifest_path.parent
    target = original.parent / persistence._path_component(
        foreign.publication_id
    )
    assert not target.exists()
    shutil.copytree(original, target)
    (target / "manifest.json").write_text(foreign.to_json(), encoding="utf-8")
    try:
        assert (
            persistence.verify_reconstruction_publication(
                target / "manifest.json"
            )
            == foreign
        )
        receipt = verify_campaign_product_index(
            verified_campaign.index_ref.path
        )
        assert receipt.product_count == 2 and receipt.missing_product_count == 0
        assert receipt.observed_event_count == 12
        assert len(receipt.out_of_plan_products) == 1
        assert receipt.out_of_plan_products[0].path == str(
            target / "manifest.json"
        )
    finally:
        shutil.rmtree(target)
