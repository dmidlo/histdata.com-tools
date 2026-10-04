"""Genuine generated campaign publication, never empirical qualification.

The shared native fixture uses real persistence and final validation. Negatives
modify retained bytes/graphs, not a mocked successful verifier or product.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from histdatacom import reconstruction as public
from histdatacom.campaign_index_contracts import (
    CampaignArtifactRefV1,
    canonical,
    load_json,
)
from histdatacom.campaign_receipt_contracts import CampaignVerificationSummaryV1
from histdatacom.campaign_receipt_publication import (
    retain_publication_root,
    validate_publication_root,
)
from histdatacom.campaign_receipt_runner import audit_campaign_verification_tree
from histdatacom.campaign_verification import verify_campaign_product_index
from histdatacom.datasets import DatasetCatalog, DatasetVersionManifestV1

pytest_plugins = ("tests.fixtures.campaign_verification",)
REFUSAL = (ValueError, OSError, RuntimeError, public.ReconstructionPublicError)


def _json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _publication(fixture: Any) -> tuple[Any, DatasetVersionManifestV1]:
    publication = public.read_reconstruction_campaign_dataset_publication(
        fixture.publication_ref.path
    )
    version = DatasetVersionManifestV1.from_dict(
        _json(publication.dataset_version_ref.path)
    )
    return publication, version


def _root_reference(fixture: Any) -> CampaignArtifactRefV1:
    _, version = _publication(fixture)
    refs = tuple(
        ref
        for ref in version.qualification_evidence
        if ref.kind == "campaign_verification_root_v1"
    )
    assert len(refs) == 1
    return CampaignArtifactRefV1.from_artifact_ref(refs[0])


@contextmanager
def _changed(path: Path) -> Iterator[None]:
    original = path.read_bytes()
    assert original
    offset = len(original) // 2
    changed = (
        original[:offset]
        + bytes([original[offset] ^ 1])
        + original[offset + 1 :]
    )
    assert hashlib.sha256(changed).digest() != hashlib.sha256(original).digest()
    try:
        path.write_bytes(changed)
        yield
    finally:
        path.write_bytes(original)
    assert path.read_bytes() == original


def _with_evidence(
    fixture: Any, directory: Path, evidence: tuple[Any, ...]
) -> Any:
    """Reseal a native graph; this does not grant it publication authority."""
    from tests.fixtures.campaign_verification import write_json

    publication, old = _publication(fixture)
    version = replace(
        old,
        qualification_evidence=evidence,
        dataset_version_id="",
        manifest_sha256="",
    )
    version_ref = write_json(
        directory,
        "version",
        version,
        kind="dataset_version_manifest_v1",
        metadata={"dataset_version_id": version.dataset_version_id},
    )
    catalog = DatasetCatalog.read(publication.catalog_ref.path)
    catalog = replace(
        catalog,
        versions=tuple(
            version if item == old else item for item in catalog.versions
        ),
        aliases=tuple(
            (
                replace(
                    alias,
                    dataset_version_id=version.dataset_version_id,
                    alias_id="",
                )
                if alias.dataset_version_id == old.dataset_version_id
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
        publication,
        dataset_version_ref=version_ref,
        catalog_ref=catalog_ref,
        synthetic_dataset_version_id=version.dataset_version_id,
        publication_id="",
    )


def test_native_dataset_version_binds_one_exact_completed_root(
    verified_campaign: Any,
):
    fixture = verified_campaign
    publication, version = _publication(fixture)
    reference = _root_reference(fixture)
    root = validate_publication_root(reference)
    summary = CampaignVerificationSummaryV1.from_json(root.summary_json)
    index = public.read_reconstruction_campaign_product_index(
        fixture.index_ref.path
    )
    assert summary.index_id == index.product_index_id
    assert summary.plan_set_id == index.plan_set_id
    assert summary.support_artifact_id == index.support_artifact_id
    assert (
        root.product_count,
        summary.observed_event_count,
        summary.synthetic_event_count,
    ) == (2, 12, 6)
    metadata = load_json(reference.metadata_json)
    assert metadata["verification_root_id"] == root.artifact_id
    assert metadata["standalone_publication_authority"] is False
    assert metadata["verification_level"] == "structural_receipt_tree_only"
    assert reference.to_artifact_ref() in version.qualification_evidence
    assert (
        publication.synthetic_dataset_version_id == version.dataset_version_id
    )
    assert len(version.qualification_evidence) == 6
    assert (
        validate_publication_root(reference, verification=fixture.verification)
        == root
    )


def test_legacy_graph_without_root_is_readable_but_cannot_be_republished(
    verified_campaign: Any,
    tmp_path: Path,
):
    from tests.fixtures.campaign_verification import write_json

    _, version = _publication(verified_campaign)
    evidence = tuple(
        ref
        for ref in version.qualification_evidence
        if ref.kind != "campaign_verification_root_v1"
    )
    legacy = _with_evidence(verified_campaign, tmp_path / "legacy", evidence)
    assert legacy.synthetic_dataset_version_id != version.dataset_version_id
    retained = write_json(
        tmp_path / "legacy",
        "publication",
        legacy,
        kind="reconstruction_campaign_dataset_publication_v1",
    )
    assert (
        public.read_reconstruction_campaign_dataset_publication(retained.path)
        == legacy
    )
    output = tmp_path / "unauthorized"
    with pytest.raises(REFUSAL, match="exact verification root"):
        public.write_reconstruction_campaign_dataset_publication(legacy, output)
    assert not output.exists()


@pytest.mark.parametrize("kind", ["parquet", "source"])
def test_historical_root_and_passed_scalars_cannot_authorize_changed_bytes(
    verified_campaign: Any,
    kind: str,
):
    fixture = verified_campaign
    reference = _root_reference(fixture)
    root = validate_publication_root(reference)
    if kind == "parquet":
        product = fixture.native.products[0]
        path = (
            product.manifest_path.parent
            / product.manifest.partitions[0].relative_path
        )
    else:
        path = fixture.native.sources.root / "eurusd" / "2015" / "6" / ".data"
    with _changed(path):
        # Historical structural inspection stays explicitly historical, while
        # the very same real root + previously passed native receipt cannot
        # substitute for current product/source bytes.
        assert validate_publication_root(reference) == root
        assert load_json(reference.metadata_json)["status"] == "complete"
        assert fixture.verification.status == "complete"
        with pytest.raises(REFUSAL):
            validate_publication_root(
                reference, verification=fixture.verification
            )


def test_foreign_native_index_path_root_cannot_bind_original_publication(
    verified_campaign: Any,
    tmp_path: Path,
):
    fixture = verified_campaign
    # Same genuine immutable products, but an independently persisted index
    # path. Fresh native verification is performed on it; no resealed fake
    # successful tree is used as the foreign-root fixture.
    index = public.read_reconstruction_campaign_product_index(
        fixture.index_ref.path
    )
    foreign_index = public.write_reconstruction_campaign_product_index(
        index, tmp_path / "foreign-index"
    )
    assert foreign_index.path != fixture.index_ref.path
    foreign_verification = verify_campaign_product_index(foreign_index.path)
    (tmp_path / "foreign-tree").mkdir()
    foreign_root = retain_publication_root(
        foreign_index.path,
        output_directory=tmp_path / "foreign-tree",
        verification=foreign_verification,
    )
    assert validate_publication_root(foreign_root)
    with pytest.raises(REFUSAL):
        validate_publication_root(
            foreign_root, verification=fixture.verification
        )
    _, version = _publication(fixture)
    forged = _with_evidence(
        fixture,
        tmp_path / "foreign-publication",
        tuple(
            (
                foreign_root.to_artifact_ref()
                if ref.kind == "campaign_verification_root_v1"
                else ref
            )
            for ref in version.qualification_evidence
        ),
    )
    output = tmp_path / "blocked"
    with pytest.raises(REFUSAL):
        public.write_reconstruction_campaign_dataset_publication(forged, output)
    assert not output.exists()


def test_actual_sample_report_cannot_replace_a_completed_root_reference(
    verified_campaign: Any,
    tmp_path: Path,
):
    from tests.fixtures.campaign_verification import write_json

    reference = _root_reference(verified_campaign)
    root = validate_publication_root(reference)
    sample = audit_campaign_verification_tree(
        verified_campaign.index_ref.path,
        Path(reference.path).parent.parent,
        expected_root_id=root.artifact_id,
        product_ordinals=(0,),
    )
    sample_path = write_json(
        tmp_path,
        "sample",
        sample,
        kind="campaign_verification_sample_v1",
        metadata={
            "status": "complete",
            "standalone_publication_authority": True,
        },
    )
    with pytest.raises(REFUSAL, match="exact campaign verification root"):
        validate_publication_root(
            CampaignArtifactRefV1.from_artifact_ref(sample_path),
            verification=verified_campaign.verification,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("standalone_publication_authority", True),
        ("verification_level", "fresh_deep"),
        ("verification_root_id", "campaign-receipt-root:sha256:" + "f" * 64),
    ],
)
def test_resealed_root_reference_metadata_cannot_change_binding(
    verified_campaign: Any,
    field: str,
    value: Any,
):
    reference = _root_reference(verified_campaign)
    metadata = load_json(reference.metadata_json)
    metadata[field] = value
    altered = replace(reference, metadata_json=canonical(metadata))
    with pytest.raises(REFUSAL):
        validate_publication_root(
            altered, verification=verified_campaign.verification
        )


@pytest.mark.parametrize(
    "check_id",
    [
        "campaign_product_index_valid",
        "campaign_dataset_publication_valid",
        "executable_retained_product_missing_count",
        "fabricated_liquidity_terminal_outcome_count",
    ],
)
def test_certification_index_alone_cannot_replace_exact_root(
    verified_campaign: Any, tmp_path: Path, check_id: str
):
    from tests.unit.test_campaign_verification_oracle import (
        _certification_spec,
        _run_certification,
    )

    spec = _certification_spec(verified_campaign)
    observation = next(
        item for item in spec.observations if item.check_id == check_id
    )
    observation = replace(
        observation,
        artifact_evidence_keys=tuple(
            key
            for key in observation.artifact_evidence_keys
            if key not in {"root", "publication"}
        ),
    )
    altered = replace(
        spec,
        artifacts=tuple(
            item
            for item in spec.artifacts
            if item.evidence_key not in {"root", "publication"}
        ),
        observations=(observation,),
        campaign_id="",
    )
    # The genuine index still passes fresh native product verification, but
    # neither that fact nor a scalar extraction declaration supplies a root.
    with pytest.raises(ValueError, match="one exact fully reverified root"):
        _run_certification(altered, tmp_path / "campaign")
    assert not (tmp_path / "campaign" / "result").exists()
