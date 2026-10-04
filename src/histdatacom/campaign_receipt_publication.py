"""Exact tree binding for publication; hashes alone are not current authority."""

from __future__ import annotations

from pathlib import Path

from histdatacom.campaign_index_contracts import (
    CampaignArtifactRefV1,
    CampaignDeepVerificationV1,
    canonical,
    load_json,
)
from histdatacom.campaign_receipt_contracts import (
    CampaignVerificationRootV1,
    CampaignVerificationSummaryV1,
)
from histdatacom.campaign_receipt_runner import (
    assert_current_campaign_verification_tree,
    resume_campaign_verification,
    run_campaign_verification,
)
from histdatacom.campaign_receipt_store import (
    get_campaign_verification_root_ref,
    read_campaign_verification_tree,
)


def retain_publication_root(
    index_path: str | Path,
    *,
    output_directory: str | Path,
    verification: CampaignDeepVerificationV1,
) -> CampaignArtifactRefV1:
    """Create or fully reverify a dedicated retained publication tree."""
    if type(verification) is not CampaignDeepVerificationV1:
        raise ValueError("exact native campaign verification required")
    verification = CampaignDeepVerificationV1.from_dict(verification.to_dict())
    store = Path(output_directory).expanduser().absolute() / (
        "campaign-verification-" + verification.index_ref.sha256
    )
    # A present but incomplete/foreign directory must pass store admission;
    # never replace, clean, or bless it by choosing another name.
    if store.exists() or store.is_symlink():
        root = resume_campaign_verification(index_path, store_directory=store)
    else:
        root = run_campaign_verification(index_path, output_directory=store)
    _reconcile_summary(root, verification)
    return get_campaign_verification_root_ref(
        store, expected_root_id=root.artifact_id
    )


def _reconcile_summary(
    root: CampaignVerificationRootV1,
    verification: CampaignDeepVerificationV1,
) -> None:
    summary = CampaignVerificationSummaryV1.from_json(root.summary_json)
    # Input digests intentionally have different domains. Do not compare the
    # bounded control/product inventories with the legacy global file union.
    fields = (
        "index_id",
        "plan_set_id",
        "support_artifact_id",
        "status",
        "shard_count",
        "support_window_count",
        "product_count",
        "missing_product_count",
        "empty_window_count",
        "refused_window_count",
        "observed_event_count",
        "synthetic_event_count",
        "shard_rows_sha256",
        "product_verifications_sha256",
    )
    if any(
        getattr(summary, key) != getattr(verification, key) for key in fields
    ):
        raise ValueError("campaign verification root differs from fresh replay")
    if CampaignArtifactRefV1.from_dict(
        load_json(summary.index_ref_json)
    ) != verification.index_ref or summary.out_of_plan_json != canonical(
        [ref.to_dict() for ref in verification.out_of_plan_products]
    ):
        raise ValueError("campaign root index bytes/reference differ")


def validate_publication_root(
    reference: CampaignArtifactRefV1,
    *,
    verification: CampaignDeepVerificationV1 | None = None,
) -> CampaignVerificationRootV1:
    """Validate exact retained binding; optionally repeat the full native pass.

    Omitting ``verification`` is explicitly structural, for historical readers.
    A fresh caller supplies a native receipt, but that receipt is not used as
    bypass authority: the retained tree is independently fully replayed too.
    """
    if (
        type(reference) is not CampaignArtifactRefV1
        or reference.kind != "campaign_verification_root_v1"
    ):
        raise ValueError("exact campaign verification root reference required")
    reference = CampaignArtifactRefV1.from_dict(
        CampaignArtifactRefV1.to_dict(reference)
    )
    if verification is not None:
        if type(verification) is not CampaignDeepVerificationV1:
            raise ValueError("exact native campaign verification required")
        verification = CampaignDeepVerificationV1.from_dict(
            verification.to_dict()
        )
    metadata = load_json(reference.metadata_json)
    identity = metadata.get("verification_root_id")
    if type(identity) is not str:
        raise ValueError("campaign verification root identity is absent")
    path = Path(reference.path)
    if path.parent.name != "root":
        raise ValueError("campaign verification root store path differs")
    store = path.parent.parent
    actual = get_campaign_verification_root_ref(
        store, expected_root_id=identity
    )
    if actual != reference:
        raise ValueError("campaign verification root reference differs")
    root = read_campaign_verification_tree(store, expected_root_id=identity)
    if verification is not None:
        current = assert_current_campaign_verification_tree(
            verification.index_ref.path,
            store,
            expected_root_id=identity,
        )
        if current != root:
            raise ValueError(
                "publication requires the exact fully reverified root"
            )
        _reconcile_summary(root, verification)
        if (
            get_campaign_verification_root_ref(store, expected_root_id=identity)
            != reference
        ):
            raise ValueError("campaign verification root changed during replay")
    return root
