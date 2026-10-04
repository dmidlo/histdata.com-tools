"""Routing/refusal tests; none of these stubs establish product validity."""

from __future__ import annotations

import inspect
import json
import os
from pathlib import Path
from typing import Any

import pytest

from histdatacom import reconstruction_cli
from histdatacom.cli_config import (
    CliConfigError,
    configured_reconstruction_argv,
)
from histdatacom.reconstruction import (
    ReconstructionClient,
    ReconstructionUnsupportedError,
    _build_reconstruction_campaign_product_index,
    _publish_reconstruction_campaign_dataset,
    _write_campaign_json,
)
from histdatacom.runtime_contracts import ArtifactRef


@pytest.mark.parametrize("value", [False, None, 0, 1, "true", [], {}])
def test_legacy_false_flag_refuses_before_reading_or_writing(
    value: Any,
) -> None:
    with pytest.raises(
        ReconstructionUnsupportedError, match="unverified exploration"
    ):
        ReconstructionClient().construct_campaign_product_index(
            "/does-not-exist/plan.json",
            "/does-not-exist/support.json",
            output_directory="/does-not-exist/output",
            verify_products=value,
        )


def test_certification_grade_and_publication_surfaces_have_no_boolean_bypass() -> (
    None
):
    for function in (
        ReconstructionClient.construct_verified_campaign_product_index,
        ReconstructionClient.publish_campaign_dataset,
        ReconstructionClient.verify_campaign_products,
        _build_reconstruction_campaign_product_index,
        _publish_reconstruction_campaign_dataset,
    ):
        assert "verify_products" not in inspect.signature(function).parameters
        assert "receipt" not in inspect.signature(function).parameters
    with pytest.raises(TypeError, match="verify_products"):
        ReconstructionClient().construct_verified_campaign_product_index(
            "plan.json",
            "support.json",
            output_directory="output",
            verify_products=False,  # type: ignore[call-arg]
        )


@pytest.mark.parametrize("legacy_alias", [False, True])
def test_cli_manifest_only_routes_only_to_explicit_inventory(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    legacy_alias: bool,
) -> None:
    calls = []
    inventory = ArtifactRef(
        kind="campaign_product_inventory_v1",
        path="/inventory.json",
        size_bytes=20,
        sha256="a" * 64,
        metadata={"status": "unverified", "publication_eligible": False},
    )

    def inventory_only(
        self: ReconstructionClient, plan: str, *, output_directory: str
    ) -> ArtifactRef:
        calls.append((plan, output_directory))
        return inventory

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail(
            "manifest-only inventory reached certification-grade builder"
        )

    monkeypatch.setattr(
        ReconstructionClient, "inventory_campaign_products", inventory_only
    )
    monkeypatch.setattr(
        ReconstructionClient,
        "construct_verified_campaign_product_index",
        forbidden,
    )
    argv = [
        "--json",
        "product-inventory",
        "--plan-set",
        "plan.json",
        "--output-directory",
        "inventory",
    ]
    if legacy_alias:
        argv[1] = "product-index"
        argv.extend(["--support-map", "support.json", "--manifest-only"])
    assert reconstruction_cli.main(argv) == 0
    assert calls == [("plan.json", "inventory")]
    result = json.loads(capsys.readouterr().out)
    assert result == inventory.to_dict()
    assert result["kind"] != "reconstruction_campaign_product_index_v1"


def test_cli_product_index_passes_no_verification_switch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls = []
    ref = ArtifactRef(
        kind="test-routing-only",
        path="/not-a-product-index.json",
        size_bytes=1,
        sha256="b" * 64,
        metadata={},
    )

    def build(
        self: ReconstructionClient,
        plan: str,
        support: str,
        *,
        output_directory: str,
    ) -> ArtifactRef:
        calls.append((plan, support, output_directory))
        return ref

    monkeypatch.setattr(
        ReconstructionClient, "construct_verified_campaign_product_index", build
    )
    assert (
        reconstruction_cli.main(
            [
                "--json",
                "product-index",
                "--plan-set",
                "plan.json",
                "--support-map",
                "support.json",
                "--output-directory",
                "index",
            ]
        )
        == 0
    )
    assert calls == [("plan.json", "support.json", "index")]
    assert json.loads(capsys.readouterr().out) == ref.to_dict()


def test_cli_deep_verification_is_a_distinct_route(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls = []

    def verify(self: ReconstructionClient, path: str) -> dict[str, Any]:
        calls.append(path)
        return {"verification_level": "fresh_deep", "routing_stub": True}

    monkeypatch.setattr(
        ReconstructionClient, "verify_campaign_products", verify
    )
    assert (
        reconstruction_cli.main(
            [
                "--json",
                "product-verify",
                "--product-index",
                "index.json",
            ]
        )
        == 0
    )
    assert calls == ["index.json"]
    assert (
        json.loads(capsys.readouterr().out)["verification_level"]
        == "fresh_deep"
    )


@pytest.mark.parametrize(
    "command",
    [
        "product-inventory",
        "product-verify",
        "product-index",
        "product-inspect",
        "dataset-publish",
    ],
)
def test_yaml_exposes_the_same_explicit_campaign_commands(
    tmp_path: Path, command: str
) -> None:
    options = (
        "    plan_set: plan.json\n    output_directory: output\n"
        if command in {"product-inventory", "product-index"}
        else "    product_index: index.json\n"
    )
    if command == "product-index":
        options += "    support_map: support.json\n"
    if command == "dataset-publish":
        options += "    output_directory: output\n"
    config = tmp_path / "commands.yaml"
    config.write_text(
        "histdatacom:\n  reconstruction:\n    command: "
        + command
        + "\n"
        + options
    )
    argv = configured_reconstruction_argv(["--config", str(config)])
    args = reconstruction_cli.build_parser().parse_args(argv)
    assert args.reconstruction_command == command
    assert not hasattr(args, "verify_products")
    config.write_text(config.read_text() + "    verify_products: false\n")
    with pytest.raises(CliConfigError):
        configured_reconstruction_argv(["--config", str(config)])


def test_campaign_output_preserves_managed_artifact_namespace(
    tmp_path: Path,
) -> None:
    from histdatacom.managed_artifact_boundary import (
        ManagedArtifactBoundaryError,
    )

    managed = tmp_path / "managed"
    managed.mkdir()
    (managed / ".histdatacom-retention.json").write_text("{}")
    target = managed / "new" / "inventory.json"
    with pytest.raises(ManagedArtifactBoundaryError):
        _write_campaign_json(target, {"status": "unverified"})
    assert not target.parent.exists()


def test_inventory_requires_explicit_supported_descriptor_platform(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from histdatacom.campaign_inventory import _manifest_bytes

    monkeypatch.delattr(os, "O_NOFOLLOW", raising=False)
    with pytest.raises(ValueError, match="requires POSIX"):
        _manifest_bytes(tmp_path / "not-opened.json")


def test_inventory_refuses_tree_bound_without_silent_truncation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from histdatacom import campaign_inventory
    from histdatacom.synthetic.persistence import (
        RECONSTRUCTION_PRODUCT_DIRECTORY,
    )

    root = tmp_path / RECONSTRUCTION_PRODUCT_DIRECTORY
    for name in ("one", "two"):
        manifest = root / "commits" / name / "manifest.json"
        manifest.parent.mkdir(parents=True)
        manifest.write_text("{}")
    monkeypatch.setattr(campaign_inventory, "MAX_INVENTORY_PRODUCTS", 1)
    with pytest.raises(ValueError, match="product count exceeds"):
        campaign_inventory.bounded_campaign_manifest_paths(tmp_path)
