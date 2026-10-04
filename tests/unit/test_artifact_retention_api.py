"""Lazy public facade checks; native lifecycle authority is tested separately."""

import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

import histdatacom.artifact_retention as api
from histdatacom.artifact_retention import contracts, lifecycle_contracts


def test_package_and_passive_contract_imports_do_not_load_store_or_backends():
    code = """
import importlib.abc
import sys

blocked = (
    "polars", "pyarrow", "pandas", "numpy", "scipy", "arch",
    "histdatacom.activity_stages",
    "histdatacom.artifact_retention.native",
    "histdatacom.artifact_retention.secure_fs",
    "histdatacom.artifact_retention.storage",
    "histdatacom.artifact_retention.apply",
)

class RejectBackend(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise AssertionError("eager backend import: " + fullname)
        return None

sys.meta_path.insert(0, RejectBackend())
import histdatacom.artifact_retention as api
assert api.RetentionPolicyV1().scratch_ttl_ns > 0
assert api.CollectionPlanV1.SCHEMA.endswith(".v1")
assert api.RetentionClass.IMMUTABLE_SOURCE.value == "immutable_source"
assert api.AdmissionReceiptV1.SCHEMA.endswith(".v1")
assert api.RootRegistrationV1.SCHEMA.endswith(".v1")
assert api.CollectionInterruptedEvidenceV1.SCHEMA.endswith(".v1")
assert not any(name in sys.modules for name in blocked)
print("lazy facade verified")
"""
    package_root = Path(api.__file__).resolve().parents[2]
    environment = dict(
        os.environ,
        PYTHONPATH=str(package_root),
        PYTHONDONTWRITEBYTECODE="1",
    )
    completed = subprocess.run(
        [sys.executable, "-B", "-c", code],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "lazy facade verified\n"


@pytest.mark.parametrize("name", sorted(api._CONTRACT_EXPORTS))
def test_public_contract_exports_are_the_actual_owners(name):
    assert getattr(api, name) is getattr(contracts, name)


@pytest.mark.parametrize("name", sorted(api._LIFECYCLE_EXPORTS))
def test_public_lifecycle_exports_are_the_actual_owners(name):
    assert getattr(api, name) is getattr(lifecycle_contracts, name)


@pytest.mark.parametrize(
    "name,module",
    [
        *((name, ".storage") for name in sorted(api._STORAGE_EXPORTS)),
        *((name, ".apply") for name in sorted(api._APPLY_EXPORTS)),
    ],
)
def test_operation_dispatch_is_closed_lazy_and_cached(
    monkeypatch, name, module
):
    sentinel = object()
    calls = []
    monkeypatch.setitem(api.__dict__, name, None)

    def import_owner(module_name, package):
        calls.append((module_name, package))
        return SimpleNamespace(**{name: sentinel})

    monkeypatch.setattr(api, "import_module", import_owner)
    assert api.__getattr__(name) is sentinel
    assert calls == [(module, "histdatacom.artifact_retention")]
    assert getattr(api, name) is sentinel
    assert len(calls) == 1


@pytest.mark.parametrize(
    "name", ["force_delete", "replace_policy", "import_graph", "StoreSession"]
)
def test_private_or_unsupported_authority_not_exported(monkeypatch, name):
    monkeypatch.setattr(
        api, "import_module", lambda *_a: pytest.fail("unexpected import")
    )
    assert name not in api.__all__
    with pytest.raises(AttributeError):
        api.__getattr__(name)


def test_introspection_lists_the_exact_supported_facade_without_importing(
    monkeypatch,
):
    monkeypatch.setattr(
        api, "import_module", lambda *_a: pytest.fail("unexpected import")
    )
    assert api.__all__ == sorted(set(api.__all__))
    assert set(api.__all__) == (
        api._CONTRACT_EXPORTS
        | api._LIFECYCLE_EXPORTS
        | api._STORAGE_EXPORTS
        | api._APPLY_EXPORTS
    )
    assert set(api.__all__).issubset(dir(api))
