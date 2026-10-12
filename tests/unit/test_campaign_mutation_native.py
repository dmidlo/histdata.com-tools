"""Generated native integrity witness, not historical campaign qualification.

The fixture uses real native products and readers. No successful verifier
double substitutes for product integrity or fresh campaign verification.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from histdatacom import reconstruction as public
from histdatacom.synthetic import persistence

pytest_plugins = ("tests.fixtures.campaign_verification",)


def test_native_products_are_verified_before_index_emission(
    native_campaign: Any, tmp_path: Path
) -> None:
    """Reject changed product bytes before emitting any campaign index files."""
    client = public.ReconstructionClient()
    positive = client.construct_verified_campaign_product_index(
        native_campaign.plan_set_path,
        native_campaign.support_map_path,
        output_directory=tmp_path / "positive-index",
    )
    index = public.read_reconstruction_campaign_product_index(positive.path)
    assert positive.metadata["verification_level"] == "fresh_deep"
    assert index.status == "complete"
    assert index.missing_product_count == 0
    assert index.verified_product_count == len(native_campaign.products) == 2

    product = native_campaign.products[0]
    manifest = persistence.load_reconstruction_manifest(product.manifest_path)
    assert manifest == product.manifest
    assert manifest.partitions
    partition = (
        product.manifest_path.parent / manifest.partitions[0].relative_path
    )
    original = partition.read_bytes()
    assert original
    assert hashlib.sha256(original).hexdigest() == (
        manifest.partitions[0].byte_sha256
    )
    offset = len(original) // 2
    changed = (
        original[:offset]
        + bytes([original[offset] ^ 1])
        + original[offset + 1 :]
    )
    assert len(changed) == len(original) and changed != original
    blocked = tmp_path / "blocked-index"
    assert not blocked.exists()

    try:
        partition.write_bytes(changed)
        # Canonical manifest parsing cannot establish the retained data bytes.
        assert (
            persistence.load_reconstruction_manifest(product.manifest_path)
            == manifest
        )
        with pytest.raises(
            persistence.ReconstructionPersistenceError,
            match="^partition byte hash differs$",
        ):
            persistence.verify_reconstruction_publication(product.manifest_path)

        # Catch both early native refusal and the mutant's later deep refusal.
        # The first assertion then identifies the intended boundary: the latter
        # is too late because it has already written product shards and index.
        with pytest.raises(
            (ValueError, OSError, public.ReconstructionPublicError)
        ) as refusal:
            client.construct_verified_campaign_product_index(
                native_campaign.plan_set_path,
                native_campaign.support_map_path,
                output_directory=blocked,
            )
        assert (
            not blocked.exists() and not blocked.is_symlink()
        ), "native product verification must precede index emission"
        assert refusal.type is persistence.ReconstructionPersistenceError
        assert str(refusal.value) == "partition byte hash differs"
    finally:
        # This is the exact generated, session-shared fixture payload only.
        partition.write_bytes(original)
        assert partition.read_bytes() == original
