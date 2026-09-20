"""Explicitly authorized loading of freshly verified, installed entry points.

This is not a sandbox. Interpreter/import hooks, metadata providers, parent
package code and dependencies are trusted. Only the registered entry module's
exact source bytes are attested, not a distribution's full dependency tree.
"""

from __future__ import annotations

from collections.abc import Callable
import hashlib
from importlib import machinery, metadata, util
import os
from pathlib import Path
import stat
import sys
from types import ModuleType
from typing import NoReturn, cast

from histdatacom.broker_plugin_registry import (
    BROKER_PLUGIN_ENTRY_POINT_GROUP,
    BrokerPluginInventoryV1,
    discover_broker_plugins,
    normalized_distribution_name,
)
from histdatacom.broker_plugins import BrokerPluginV1

from .contracts import (
    BrokerCapabilityError,
    BrokerCapabilityPlanV1,
    BrokerCapabilityReason,
    BrokerInvocationAssociation,
)
from .execution import GatedBrokerPluginV1, _CONSTRUCTION_KEY, _authorize, _call
from .negotiation import verify_broker_capability_plan

_MAX_MODULE_BYTES = 8 * 1024 * 1024
_MAX_FILES = 16_384


def _refuse() -> NoReturn:
    raise BrokerCapabilityError(BrokerCapabilityReason.RUNTIME_IDENTITY)


def _read_module(path: Path) -> bytes:
    if path.is_symlink():
        _refuse()
    descriptor = os.open(
        path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    )
    with os.fdopen(descriptor, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_MODULE_BYTES:
            _refuse()
        data = source.read(_MAX_MODULE_BYTES + 1)
        if len(data) > _MAX_MODULE_BYTES:
            _refuse()
        return data


def _installed_origin(plan: BrokerCapabilityPlanV1) -> Path:
    registration = plan.candidate.registration
    matches: list[Path] = []
    for distribution in metadata.distributions():
        if type(distribution) is not metadata.PathDistribution:
            _refuse()
        if (
            normalized_distribution_name(distribution.metadata.get("Name", ""))
            != registration.distribution_name
            or distribution.version != registration.distribution_version
        ):
            continue
        entries = [
            entry
            for entry in distribution.entry_points
            if entry.group == BROKER_PLUGIN_ENTRY_POINT_GROUP
            and entry.name == registration.plugin_id
            and entry.value == registration.entry_point
        ]
        if not entries:
            continue
        files = distribution.files
        if len(entries) != 1 or files is None or len(files) > _MAX_FILES:
            _refuse()
        module = registration.entry_point.split(":", 1)[0].replace(".", "/")
        paths = {file.as_posix() for file in files}
        choices = [
            path
            for path in (module + ".py", module + "/__init__.py")
            if path in paths
        ]
        if len(choices) != 1:
            _refuse()
        root = distribution.locate_file("")
        if not isinstance(root, Path):
            _refuse()
        root = root.resolve()
        origin = root / choices[0]
        if origin.resolve() != origin or not origin.is_relative_to(root):
            _refuse()
        matches.append(origin)
        if len(matches) > 1:
            _refuse()
    if len(matches) != 1:
        _refuse()
    return matches[0]


def _origins(module: str, origin: Path) -> tuple[tuple[str, Path], ...]:
    parts = module.split(".")
    if len(parts) > 16:
        _refuse()
    root = (
        origin.parent if origin.name != "__init__.py" else origin.parent.parent
    )
    for _ in parts[:-1]:
        root = root.parent
    expected: list[tuple[str, Path]] = []
    search: list[str] | None = None
    for index in range(len(parts)):
        name = ".".join(parts[: index + 1])
        path = (
            origin
            if index == len(parts) - 1
            else root.joinpath(*parts[: index + 1], "__init__.py")
        )
        # PathFinder with explicit search paths does not import parent modules.
        # Namespace/extension/custom-loader packages are deliberately refused.
        spec = machinery.PathFinder.find_spec(name, search)
        if (
            spec is None
            or type(spec.loader) is not machinery.SourceFileLoader
            or spec.origin is None
            or Path(spec.origin).resolve() != path
            or path.resolve() != path
        ):
            _refuse()
        if index < len(parts) - 1 and tuple(
            Path(item).resolve()
            for item in (spec.submodule_search_locations or ())
        ) != (path.parent,):
            _refuse()
        loaded = sys.modules.get(name)
        if name in sys.modules:
            if (
                index == len(parts) - 1
                or not isinstance(loaded, ModuleType)
                or not _same_path(getattr(loaded, "__file__", None), path)
                or not _same_path(
                    getattr(getattr(loaded, "__spec__", None), "origin", None),
                    path,
                )
                or tuple(
                    Path(item).resolve()
                    for item in getattr(loaded, "__path__", ())
                )
                != (path.parent,)
            ):
                _refuse()
        expected.append((name, path))
        search = [str(path.parent)]
    return tuple(expected)


def _same_path(value: object, expected: Path) -> bool:
    return type(value) is str and Path(value).resolve() == expected


def _execute_source(name: str, path: Path, source: bytes) -> ModuleType:
    # Compile the bytes just verified, never a possibly stale same-mtime pyc.
    spec = util.spec_from_file_location(name, path)
    if spec is None:
        _refuse()
    module = util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        exec(
            compile(source, str(path), "exec", dont_inherit=True),
            module.__dict__,
        )
    except BaseException:
        if sys.modules.get(name) is module:
            del sys.modules[name]
        raise
    if (
        sys.modules.get(name) is not module
        or module.__file__ != str(path)
        or getattr(module.__spec__, "origin", None) != str(path)
    ):
        _refuse()
    if "." in name:
        parent, child = name.rsplit(".", 1)
        setattr(sys.modules[parent], child, module)
    return module


def invoke_authorized_installed_broker_plugin(
    inventory: BrokerPluginInventoryV1,
    plan: BrokerCapabilityPlanV1,
    *,
    authorize: Callable[[BrokerCapabilityPlanV1], bool],
) -> GatedBrokerPluginV1:
    """Fresh metadata/RECORD check, then authorized exact source invocation.

    A previously loaded entry module is refused rather than re-executed or
    presumed to match disk. Use a fresh caller-owned process for another
    installed invocation. Filesystem checks are non-atomic; no rights,
    credential, deadline, network or scientific-admission policy is supplied.
    """
    _authorize(inventory, plan, authorize)
    try:
        fresh = discover_broker_plugins()
        verify_broker_capability_plan(plan, fresh)
        module_name, attribute = plan.candidate.registration.entry_point.split(
            ":", 1
        )
        origin = _installed_origin(plan)
        expected = _origins(module_name, origin)
        sources = tuple(_read_module(path) for _, path in expected)
        if (
            hashlib.sha256(sources[-1]).hexdigest()
            != plan.candidate.implementation_sha256
        ):
            _refuse()
        # Recheck metadata after all path reads, before any parent execution.
        verify_broker_capability_plan(plan, discover_broker_plugins())
        for (name, path), source in zip(expected, sources):
            if name == module_name or name not in sys.modules:
                if name in sys.modules:
                    _refuse()
                loaded = _call(lambda: _execute_source(name, path, source))
        factory: object = loaded
        for component in attribute.split("."):
            factory = getattr(factory, component)
        if not callable(factory):
            _refuse()
        plugin = _call(cast(Callable[[], BrokerPluginV1], factory))
    except BrokerCapabilityError:
        raise
    except (Exception, SystemExit):
        raise BrokerCapabilityError(
            BrokerCapabilityReason.RUNTIME_IDENTITY
        ) from None
    return GatedBrokerPluginV1(
        plugin,
        plan,
        association=BrokerInvocationAssociation.INSTALLED_ENTRYPOINT,
        module_sha256=plan.candidate.implementation_sha256,
        _key=_CONSTRUCTION_KEY,
    )
