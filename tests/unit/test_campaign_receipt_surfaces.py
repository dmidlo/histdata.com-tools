"""Explicit command routing only, never substitutes for native evidence."""

import inspect
import json

import pytest

from histdatacom import reconstruction_cli
from histdatacom.cli_config import configured_reconstruction_argv
from histdatacom.reconstruction import ReconstructionClient


@pytest.mark.parametrize(
    "command,method,options,args,kwargs",
    (
        (
            "product-verify-tree",
            "verify_campaign_receipts",
            [
                "--product-index",
                "index",
                "--output-directory",
                "store",
                "--products-per-shard",
                "1",
            ],
            ("index",),
            {"output_directory": "store", "products_per_shard": 1},
        ),
        (
            "product-resume-verification",
            "resume_campaign_receipts",
            [
                "--product-index",
                "index",
                "--store-directory",
                "store",
                "--expected-checkpoint-id",
                "expected",
            ],
            ("index",),
            {"store_directory": "store", "expected_checkpoint_id": "expected"},
        ),
        (
            "product-inspect-verification",
            "inspect_campaign_receipts",
            ["--store-directory", "store", "--expected-root-id", "expected"],
            ("store",),
            {"expected_root_id": "expected"},
        ),
        (
            "product-audit-verification",
            "audit_campaign_receipts",
            [
                "--product-index",
                "index",
                "--store-directory",
                "store",
                "--expected-root-id",
                "expected",
                "--product-ordinal",
                "0",
                "2",
            ],
            ("index", "store"),
            {"expected_root_id": "expected", "product_ordinals": (0, 2)},
        ),
    ),
)
def test_explicit_receipt_routes(
    monkeypatch, capsys, command, method, options, args, kwargs
):
    calls = []

    def route(self, *received, **named):
        calls.append((received, named))
        return {"routing_stub_not_evidence": True}

    monkeypatch.setattr(ReconstructionClient, method, route)
    assert reconstruction_cli.main(["--json", command, *options]) == 0
    assert calls == [(args, kwargs)]
    assert json.loads(capsys.readouterr().out) == {
        "routing_stub_not_evidence": True
    }


def test_receipt_surfaces_have_no_verified_or_skip_native_flag():
    for name in (
        "verify_campaign_receipts",
        "resume_campaign_receipts",
        "inspect_campaign_receipts",
        "audit_campaign_receipts",
    ):
        parameters = inspect.signature(
            getattr(ReconstructionClient, name)
        ).parameters
        assert not {
            "verified",
            "skip_native",
            "trust_receipt",
            "verify_products",
        } & set(parameters)


def test_yaml_exposes_explicit_sample_scope(tmp_path):
    config = tmp_path / "audit.yaml"
    config.write_text(
        "histdatacom:\n  reconstruction:\n"
        "    command: product-audit-verification\n"
        "    product_index: index\n    store_directory: store\n"
        "    expected_root_id: expected\n    product_ordinals: [0, 2]\n"
    )
    argv = configured_reconstruction_argv(["--config", str(config)])
    parsed = reconstruction_cli.build_parser().parse_args(argv)
    assert parsed.reconstruction_command == "product-audit-verification"
    assert parsed.product_ordinal == [0, 2]
    assert parsed.expected_root_id == "expected"
    assert not hasattr(parsed, "skip_native")


def test_yaml_exposes_frozen_receipt_grouping(tmp_path):
    config = tmp_path / "tree.yaml"
    config.write_text(
        "histdatacom:\n  reconstruction:\n"
        "    command: product-verify-tree\n"
        "    product_index: index\n    output_directory: store\n"
        "    products_per_shard: 1\n"
    )
    argv = configured_reconstruction_argv(["--config", str(config)])
    parsed = reconstruction_cli.build_parser().parse_args(argv)
    assert parsed.products_per_shard == 1
    assert parsed.output_directory == "store"
