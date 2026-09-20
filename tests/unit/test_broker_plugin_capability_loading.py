"""Exact installed candidate association and zero-execution refusal canaries."""

from __future__ import annotations

from dataclasses import replace
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import py_compile
import runpy
import sys
from types import ModuleType
from zipfile import ZipFile

import pytest

from histdatacom.broker_plugin_capabilities import (
    BrokerCapabilityError,
    BrokerCapabilityWorkflowV1,
    BrokerInvocationAssociation,
    invoke_authorized_installed_broker_plugin,
    negotiate_broker_capabilities,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins

ROOT = Path(__file__).resolve().parents[2]
WHEEL = runpy.run_path(str(ROOT / "tests/fixtures/broker_capability_wheel.py"))
OPERATIONS = tuple(
    sorted(
        (
            "metadata",
            "configuration_schema",
            "open_session",
            "instruments",
            "subscribe",
            "iter_events",
            "unsubscribe",
            "close_session",
        )
    )
)


@pytest.fixture(autouse=True)
def _clear_fixture_modules():
    assert not any(
        name.startswith("capability_fixture") for name in sys.modules
    )
    yield
    for name in list(sys.modules):
        if name == "capability_fixture" or name.startswith(
            "capability_fixture."
        ):
            del sys.modules[name]


def _installed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    wheel = WHEEL["build_capability_wheel"](tmp_path / "wheels")
    site = (tmp_path / "site").resolve()
    with ZipFile(wheel) as archive:
        archive.extractall(site)
    distribution = next(metadata.distributions(path=[str(site)]))
    monkeypatch.setattr(
        metadata, "distributions", lambda: iter((distribution,))
    )
    monkeypatch.syspath_prepend(str(site))
    inventory = discover_broker_plugins()
    plan = negotiate_broker_capabilities(
        inventory,
        BrokerCapabilityWorkflowV1(OPERATIONS),
        plugin_id="org.example.capabilities",
    )
    return site, inventory, plan


def test_real_entrypoint_invocation_checks_identity_and_all_sdk_operations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    site, inventory, plan = _installed(tmp_path, monkeypatch)
    assert "capability_fixture" not in sys.modules
    invocation = invoke_authorized_installed_broker_plugin(
        inventory, plan, authorize=lambda _: True
    )
    assert (
        invocation.binding.association
        is BrokerInvocationAssociation.INSTALLED_ENTRYPOINT
    )
    assert (
        invocation.binding.module_sha256
        == hashlib.sha256(
            (site / "capability_fixture/plugin.py").read_bytes()
        ).hexdigest()
    )
    assert invocation.configuration_schema.fields == ()
    invocation.open_session({})
    invocation.instruments()
    invocation.subscribe(("EURUSD",))
    event = list(invocation.iter_events())[0]
    assert event.event.quote.bid == "1.1"
    invocation.unsubscribe(("EURUSD",))
    invocation.close_session()
    assert sys.modules["capability_fixture.plugin"].CALLS == [
        "module_import",
        "factory",
        "metadata",
        "configuration_schema",
        "configuration_schema",
        "open_session",
        "configuration_schema",
        "metadata",
        "instruments",
        "subscribe",
        "iter_events",
        "unsubscribe",
        "close_session",
    ]
    # Reuse is refused: loaded runtime state cannot be assumed equal to disk.
    with pytest.raises(
        BrokerCapabilityError, match="runtime_identity_mismatch"
    ):
        invoke_authorized_installed_broker_plugin(
            inventory, plan, authorize=lambda _: True
        )


@pytest.mark.parametrize("refusal", ["unauthorized", "missing", "dependency"])
def test_preflight_refusals_make_zero_imports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, refusal: str
) -> None:
    _, inventory, plan = _installed(tmp_path, monkeypatch)
    if refusal == "missing":
        plan = negotiate_broker_capabilities(
            inventory,
            replace(plan.workflow, required=("history.replay.v1",)),
            plugin_id="org.example.capabilities",
        )
    if refusal == "dependency":
        plan = negotiate_broker_capabilities(
            inventory,
            BrokerCapabilityWorkflowV1(("subscribe",)),
            plugin_id="org.example.capabilities",
        )
    with pytest.raises(BrokerCapabilityError):
        invoke_authorized_installed_broker_plugin(
            inventory, plan, authorize=lambda _: refusal != "unauthorized"
        )
    assert not any(
        name.startswith("capability_fixture") for name in sys.modules
    )


@pytest.mark.parametrize("change", ["module", "descriptor", "removed"])
def test_fresh_bytes_and_descriptor_revalidation_refuse_stale_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    site, inventory, plan = _installed(tmp_path, monkeypatch)
    target = site / "capability_fixture/plugin.py"
    if change == "descriptor":
        target = (
            site
            / "capability_fixture/_histdatacom_broker_plugins/org.example.capabilities.json"
        )
    before = target.stat()
    if change == "removed":
        target.unlink()
    else:
        data = target.read_bytes()
        target.write_bytes(data.replace(b"Offline", b"Changed", 1))
        os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
        assert target.stat().st_size == before.st_size
    with pytest.raises(BrokerCapabilityError):
        invoke_authorized_installed_broker_plugin(
            inventory, plan, authorize=lambda _: True
        )
    assert "capability_fixture" not in sys.modules


def test_shadow_parent_package_is_refused_before_canary_executes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, inventory, plan = _installed(tmp_path, monkeypatch)
    shadow = tmp_path / "shadow"
    (shadow / "capability_fixture").mkdir(parents=True)
    (shadow / "capability_fixture/__init__.py").write_text(
        'raise RuntimeError("SHADOW_IMPORT_CANARY")\n'
    )
    monkeypatch.syspath_prepend(str(shadow))
    with pytest.raises(
        BrokerCapabilityError, match="runtime_identity_mismatch"
    ):
        invoke_authorized_installed_broker_plugin(
            inventory, plan, authorize=lambda _: True
        )
    assert "capability_fixture" not in sys.modules


@pytest.mark.parametrize(
    "name", ["capability_fixture", "capability_fixture.plugin"]
)
def test_already_loaded_foreign_origin_is_refused_before_parent_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    _, inventory, plan = _installed(tmp_path, monkeypatch)
    module = ModuleType(name)
    module.__file__ = "/unrelated/foreign.py"
    monkeypatch.setitem(sys.modules, name, module)
    with pytest.raises(
        BrokerCapabilityError, match="runtime_identity_mismatch"
    ):
        invoke_authorized_installed_broker_plugin(
            inventory, plan, authorize=lambda _: True
        )
    assert sys.modules[name] is module


def test_verified_source_not_stale_same_size_mtime_bytecode_executes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    site, inventory, plan = _installed(tmp_path, monkeypatch)
    target = site / "capability_fixture/plugin.py"
    data = target.read_bytes()
    info = target.stat()
    stale = data.replace(
        b'CALLS: list[str] = ["module_import"]',
        b'raise RuntimeError("PYC_CANARY!!!!")',
    )
    assert len(stale) == len(data)
    target.write_bytes(stale)
    os.utime(target, ns=(info.st_atime_ns, info.st_mtime_ns))
    py_compile.compile(str(target), doraise=True)
    target.write_bytes(data)
    os.utime(target, ns=(info.st_atime_ns, info.st_mtime_ns))
    invocation = invoke_authorized_installed_broker_plugin(
        inventory, plan, authorize=lambda _: True
    )
    assert invocation.metadata.metadata.plugin_id == "org.example.capabilities"
    assert "PYC_CANARY" not in json.dumps(invocation.binding.to_dict())


def test_nonregular_module_refuses_without_blocking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    site, inventory, plan = _installed(tmp_path, monkeypatch)
    target = site / "capability_fixture/plugin.py"
    target.unlink()
    os.mkfifo(target)
    with pytest.raises(BrokerCapabilityError):
        invoke_authorized_installed_broker_plugin(
            inventory, plan, authorize=lambda _: True
        )
    assert "capability_fixture" not in sys.modules


def test_equivalent_site_path_alias_is_not_a_shadow_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    site, inventory, plan = _installed(tmp_path, monkeypatch)
    alias = tmp_path / "site-alias"
    alias.symlink_to(site, target_is_directory=True)
    monkeypatch.syspath_prepend(str(alias))
    invocation = invoke_authorized_installed_broker_plugin(
        inventory, plan, authorize=lambda _: True
    )
    assert (
        invocation.binding.association
        is BrokerInvocationAssociation.INSTALLED_ENTRYPOINT
    )


def test_correct_preloaded_parent_alias_is_permitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import importlib

    site, inventory, plan = _installed(tmp_path, monkeypatch)
    alias = tmp_path / "site-alias"
    alias.symlink_to(site, target_is_directory=True)
    monkeypatch.syspath_prepend(str(alias))
    parent = importlib.import_module("capability_fixture")
    assert str(alias) in parent.__file__
    invocation = invoke_authorized_installed_broker_plugin(
        inventory, plan, authorize=lambda _: True
    )
    assert invocation.metadata.metadata.plugin_id == "org.example.capabilities"
