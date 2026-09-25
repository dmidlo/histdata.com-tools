"""Offline source audit/generation. AST inspection, never producer imports.

This module is deliberately not imported by the public query module. The audit
over-approximates serialization surfaces: unversioned dictionary serializers
are registered too, and abstract templates are individually exempted. Source
evidence establishes implemented entry points, not historical golden replay.
"""

from __future__ import annotations

import ast
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from .contracts import (
    CompatibilityRegistryV1,
    EvidenceKind,
    EvidenceV1,
    ExemptionV1,
    ImplementationV1,
    SchemaV1,
    SupportStatus,
)

WRITERS = frozenset(
    {"to_dict", "to_json", "as_dict", "to_mapping", "to_payload"}
)
METADATA_WRITERS = frozenset(
    {"payload", "metadata", "to_metadata", "evidence_dict", "identity_payload"}
)
READERS = frozenset(
    {"from_dict", "from_json", "from_mapping", "from_payload", "restore"}
)
PERSISTENCE_CALLS = frozenset(
    {
        "write_ipc",
        "write_parquet",
        "write_csv",
        "write_ndjson",
        "write_json",
        "write_table",
        "new_file",
        "new_stream",
        "ParquetWriter",
        "json.dump",
    }
)
SCHEMA_CALLS = frozenset({"pa.schema", "pyarrow.schema", "pl.Schema"})

# Reviewed non-class formats. Each tuple is (reader entry points, writer entry
# points, scope note), relative to the constant's declaring module. These are
# implementation associations, not migrations or assertions of round-trip
# equivalence. Referenced functions are independently checked in the AST.
FORMAT_SUPPORT: dict[str, tuple[tuple[str, ...], tuple[str, ...], str]] = {
    "STATE_SPACE_STATE_RESULT_SCHEMA_VERSION": (
        (),
        ("_fit_sample",),
        "Embedded filtered/smoothed state-result rows; no standalone direct reader.",
    ),
    "CANONICAL_TICK_PROJECTION_SCHEMA_VERSION": (
        (),
        (
            "HistDataProviderAdapter.read_partition",
            "FixtureProviderAdapter.read_partition",
        ),
        "Canonical projection emitted by provider adapters; not their raw input format.",
    ),
    "DATASET_TICK_PROJECTION_SCHEMA_VERSION": (
        (),
        ("project_observed_ascii_ticks_v2",),
        "Dataset companion projection; does not replace the immutable V1 event Arrow schema.",
    ),
    "OPERATOR_MARKET_CONTEXT_CATALOG_SCHEMA_VERSION": (
        ("OperatorMarketContextCatalogAdapterV1.load_events",),
        (),
        "External operator catalog accepted by the declared adapter; no package-owned writer.",
    ),
    "ECONOMIC_CALENDAR_ARROW_SCHEMA_VERSION": (
        ("economic_calendar_corpus_from_arrow",),
        ("economic_calendar_corpus_to_arrow",),
        "Explicit Arrow corpus reader/writer; this relation does not qualify a graph migration.",
    ),
    "DURABLE_INDEX_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_census_durable_index_snapshot",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "BEA_GDP_INDEX_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_bea_gdp_index_snapshots",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "H6_INDEX_PAGES_SCHEMA_VERSION": (
        ("load_packaged_federal_reserve_h6_index_pages",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "HOUSING_INDEX_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_census_housing_index_snapshot",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "INITIAL_CLAIMS_INDEX_PAGES_SCHEMA_VERSION": (
        ("load_packaged_dol_initial_claims_index_pages",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "INTERNATIONAL_TRADE_INDEX_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_census_international_trade_index_snapshot",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "MTIS_INDEX_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_census_mtis_index_evidence",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "MTS_CATALOG_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_treasury_mts_catalog",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "NEW_HOME_SALES_INDEX_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_census_new_home_sales_index_snapshot",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "BEA_PIO_INDEX_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_bea_pio_index_snapshots",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "CENSUS_RETAIL_INDEX_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_census_retail_index_snapshot",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "CENSUS_RETAIL_BENCHMARK_INDEX_ENVELOPE_SCHEMA_VERSION": (
        ("load_packaged_census_retail_benchmark_index_snapshot",),
        (),
        "Packaged external snapshot envelope; exact declared loader, no package-owned writer.",
    ),
    "_SYNTHETIC_DELTA_STORAGE_SCHEMA_VERSION": (
        (),
        ("_synthetic_delta_arrow_schema", "_synthetic_delta_stream_to_arrow"),
        "Physical V3 delta storage V2 omits derived IDs and requires retained anchor dictionary/product scope; standalone direct read is unsupported.",
    ),
    "CAMPAIGN_RESOURCE_AUDIT_SPEC_SCHEMA_VERSION": (
        ("build_campaign_resource_audit_from_spec",),
        (),
        "Operator-authored resource specification accepted by its explicit builder; no package-owned writer or migration.",
    ),
}
CONSTANT_EXEMPTIONS = {
    "histdatacom.data_analytics.feed_epochs_v2.FEED_EPOCH_ASSIGNMENT_V2_SCHEMA_VERSION": "FeedEpochAssignmentV2 is a process-local assignment result, with no serializer/persistence surface; durable definition and boundary contracts are separately registered.",
    "histdatacom.orchestration.reconstruction.RECONSTRUCTION_ORCHESTRATION_SCHEMA_VERSION": "Reserved module-level orchestration marker, not referenced by a serializer or durable field; concrete orchestration request/state/result contracts are registered.",
    "histdatacom.reconstruction_evidence.HISTDATA_LEGACY_CACHE_SCHEMA_VERSION": "Legacy external cache provenance label, not a separately emitted synthetic wire schema. Current HistData adapter physical readers and provider descriptors retain actual input-format authority; no historical translation is qualified.",
    "histdatacom.reconstruction_schema.LEGACY_HISTDATA_CACHE_SCHEMA_VERSION": "Legacy external cache provenance label duplicated by the older reconstruction inventory; not a new durable synthetic schema or a migration qualification.",
}
PROCESS_LOCAL_MODULES = {
    "histdatacom.synthetic.traders.inputs": "Canonical provider-neutral callable protocol; no serialization or persistence is introduced. Existing source contracts retain their wire ownership.",
    "histdatacom.synthetic.traders.adapters": "Process-local adapter delegating to canonical provider contracts; no new durable schema or serialization.",
    "histdatacom.synthetic.traders.triangle": "Process-local triangle request/result/reader/state; replay delegates to native committed product contracts, with no new durable serialization.",
}
PHYSICAL_READERS = {
    "histdatacom.synthetic.contracts.synthetic_event_arrow_schema": (
        "synthetic_event_stream_from_arrow",
    ),
    "histdatacom.synthetic.contracts._write_parquet_table": (
        "synthetic_event_stream_from_parquet_bytes",
        "read_synthetic_event_stream_parquet",
    ),
    "histdatacom.synthetic.benchmark_source_projection._write_projection": (
        "inspect_benchmark_source_projection",
    ),
}
PHYSICAL_WRITERS = {
    "histdatacom.synthetic.contracts.synthetic_event_arrow_schema": (
        "synthetic_event_stream_to_arrow",
    ),
    "histdatacom.synthetic.contracts._write_parquet_table": (
        "synthetic_event_stream_to_parquet_bytes",
        "write_synthetic_event_stream_parquet",
    ),
}
CLASS_READERS = {
    "histdatacom.data_quality.calendar_profiles.HistDataCalendarProfile": (
        "calendar_profile_from_mapping",
    ),
}
SERIALIZER_EXEMPTIONS = {
    "histdatacom.broker_plugin_permissions._wire.Record": "Abstract frozen-dataclass serializer template; it has no standalone fields or payload. Concrete embedded permission records and versioned artifact subclasses are inventoried separately.",
    "histdatacom.broker_plugin_policy._wire.Record": "Abstract frozen-dataclass serializer template; it has no standalone fields or payload. Concrete embedded provider-policy records and versioned artifact subclasses are inventoried separately.",
    "histdatacom.attribution._wire.Record": "Abstract frozen-dataclass serializer template; it has no standalone fields or payload. Concrete embedded attribution records and versioned artifact subclasses are inventoried separately.",
    "histdatacom.experiments._wire.Record": "Abstract frozen-dataclass serializer template; it has no standalone fields or payload. Concrete embedded records and versioned artifact subclasses are inventoried separately.",
}
FUNCTION_EXEMPTIONS = {
    "histdatacom.broker_plugin_permissions._wire.Artifact.artifact_id": "Derived digest property for the abstract permission envelope, not an independent serialized family. Concrete permission artifacts retain their own envelope reader/writer inventory.",
    "histdatacom.broker_plugin_health._wire.Artifact.artifact_id": "Derived digest property for the abstract host-health envelope, not an independent serialized family. Concrete host-health artifacts retain their own envelope reader/writer inventory.",
    "histdatacom.broker_plugin_policy._wire.Artifact.artifact_id": "Derived digest property for the abstract provider-policy envelope, not an independent serialized family. Every concrete provider-policy artifact's actual envelope reader/writer is inventoried separately.",
    "histdatacom.attribution._wire.Artifact.artifact_id": "Derived digest property for the abstract attribution envelope, not an independent serialized family. Every concrete attribution artifact's actual envelope reader/writer is inventoried separately.",
    "histdatacom.experiments._wire.Artifact.artifact_id": "Derived digest property for the abstract artifact envelope, not an independent serialized family. Every concrete experiment artifact's actual envelope reader/writer is inventoried separately.",
}


@dataclass(frozen=True)
class _Module:
    name: str
    path: Path
    tree: ast.Module
    sha256: str
    names: dict[str, ast.expr]
    imports: dict[str, str]


def _assignments(nodes: list[ast.stmt]) -> dict[str, ast.expr]:
    result: dict[str, ast.expr] = {}
    for node in nodes:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    result[target.id] = node.value
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.value is not None
        ):
            result[node.target.id] = node.value
    return result


def _imports(module: str, tree: ast.Module, package: bool) -> dict[str, str]:
    result: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            prefix = node.module or ""
            if node.level:
                parts = module.split(".") if package else module.split(".")[:-1]
                prefix = ".".join(parts[: len(parts) - node.level + 1]) + (
                    "." + prefix if prefix else ""
                )
            for item in node.names:
                result[item.asname or item.name] = prefix + "." + item.name
    return result


def _modules(root: Path) -> dict[str, _Module]:
    result: dict[str, _Module] = {}
    for path in sorted((root / "src" / "histdatacom").rglob("*.py")):
        relative = path.relative_to(root / "src").with_suffix("")
        parts = relative.parts
        package = parts[-1] == "__init__"
        name = ".".join(parts[:-1] if package else parts)
        data = path.read_bytes()
        if len(data) > 8 * 1024 * 1024:
            raise ValueError("source audit file bound")
        tree = ast.parse(data.decode("utf-8"), filename=str(relative))
        result[name] = _Module(
            name,
            path,
            tree,
            hashlib.sha256(data).hexdigest(),
            _assignments(tree.body),
            _imports(name, tree, package),
        )
    if len(result) > 2048:
        raise ValueError("source audit module bound")
    return result


def build_registry(root: Path) -> CompatibilityRegistryV1:
    """Inventory code only. No dataset, cache, model, or artifact reads."""
    modules = _modules(root)
    classes: dict[str, tuple[_Module, ast.ClassDef]] = {}
    for module in modules.values():
        for node in module.tree.body:
            if isinstance(node, ast.ClassDef):
                classes[module.name + "." + node.name] = (module, node)

    def resolve_name(module: _Module, name: str) -> str:
        result = module.imports.get(name, module.name + "." + name)
        seen: set[str] = set()
        while result not in seen:
            seen.add(result)
            prefix, _, tail = result.rpartition(".")
            if prefix not in modules or tail not in modules[prefix].imports:
                break
            result = modules[prefix].imports[tail]
        return result

    def evaluate(
        module: _Module,
        node: ast.AST | None,
        local: dict[str, ast.expr] | None = None,
        depth: int = 0,
    ) -> str | None:
        if depth > 24 or node is None:
            return None
        if isinstance(node, ast.Constant) and type(node.value) is str:
            return node.value
        if isinstance(node, ast.Name):
            if local and node.id in local:
                return evaluate(module, local[node.id], None, depth + 1)
            if node.id in module.names:
                return evaluate(module, module.names[node.id], local, depth + 1)
            full = resolve_name(module, node.id)
            prefix, _, tail = full.rpartition(".")
            if prefix in modules and tail in modules[prefix].names:
                return evaluate(
                    modules[prefix],
                    modules[prefix].names[tail],
                    local,
                    depth + 1,
                )
        if isinstance(node, ast.Attribute) and local and node.attr in local:
            return evaluate(module, local[node.attr], None, depth + 1)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = evaluate(module, node.left, local, depth + 1)
            right = evaluate(module, node.right, local, depth + 1)
            return (
                left + right if left is not None and right is not None else None
            )
        if isinstance(node, ast.JoinedStr):
            pieces = [
                evaluate(
                    module,
                    (
                        value.value
                        if isinstance(value, ast.FormattedValue)
                        else value
                    ),
                    local,
                    depth + 1,
                )
                for value in node.values
            ]
            if all(piece is not None for piece in pieces):
                return "".join(piece for piece in pieces if piece is not None)
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg == "default":
                    return evaluate(module, keyword.value, local, depth + 1)
        return None

    def lineage(
        key: str, seen: frozenset[str] = frozenset()
    ) -> list[tuple[_Module, ast.ClassDef]]:
        if key not in classes or key in seen:
            return []
        module, node = classes[key]
        ancestors: list[tuple[_Module, ast.ClassDef]] = []
        for base in node.bases:
            if isinstance(base, ast.Name):
                ancestors.extend(
                    lineage(resolve_name(module, base.id), seen | {key})
                )
        return [*ancestors, (module, node)]

    def methods(
        chain: list[tuple[_Module, ast.ClassDef]],
    ) -> dict[str, tuple[_Module, ast.FunctionDef]]:
        return {
            child.name: (module, child)
            for module, node in chain
            for child in node.body
            if isinstance(child, ast.FunctionDef)
        }

    def wire_schema(chain: list[tuple[_Module, ast.ClassDef]]) -> str | None:
        local: dict[str, ast.expr] = {}
        for _module, node in chain:
            local.update(_assignments(node.body))
        for module, node in reversed(chain):
            assignments = _assignments(node.body)
            for field in ("schema_version", "schema"):
                if field in assignments:
                    value = evaluate(module, assignments[field], local)
                    if value:
                        return value
        for module, node in reversed(chain):
            for child in ast.walk(node):
                if isinstance(child, ast.Dict):
                    for key, expression in zip(child.keys, child.values):
                        if (
                            isinstance(key, ast.Constant)
                            and key.value == "schema_version"
                        ):
                            resolved = evaluate(module, expression, local)
                            if resolved:
                                return resolved
                elif (
                    isinstance(child, ast.FunctionDef)
                    and child.name == "schema_version"
                ):
                    for sub in ast.walk(child):
                        if isinstance(sub, ast.Return):
                            resolved = evaluate(module, sub.value, local)
                            if resolved:
                                return resolved
        # Forecasting uses a shared _seal(schema, contract_type) encoder.
        module, node = chain[-1]
        for child in ast.walk(node):
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Name)
                and child.func.id == "_seal"
            ):
                return evaluate(module, ast.Name(id="SCHEMA_VERSION")) or "1.0"
        return None

    version = evaluate(modules["histdatacom"], ast.Name(id="__version__"))
    if version is None:
        raise ValueError("package version unavailable")
    implementations: dict[str, ImplementationV1] = {}
    evidence: dict[str, EvidenceV1] = {}
    schemas: list[SchemaV1] = []
    exemptions: list[ExemptionV1] = []
    test_sources: dict[Path, tuple[str, set[str]]] = {}
    for path in sorted((root / "tests").rglob("test_*.py")):
        body = path.read_bytes()
        test_sources[path] = (
            hashlib.sha256(body).hexdigest(),
            set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", body.decode("utf-8"))),
        )

    def implementation(module: _Module, name: str) -> str:
        identity = (
            "implementation:sha256:"
            + hashlib.sha256((name + "\n" + module.sha256).encode()).hexdigest()
        )
        implementations[identity] = ImplementationV1(
            identity, name, version, module.sha256
        )
        return identity

    def source_evidence(module: _Module) -> str:
        identity = "source:sha256:" + module.sha256
        evidence[identity] = EvidenceV1(
            identity,
            EvidenceKind.SOURCE_DEFINITION,
            module.path.relative_to(root).as_posix(),
            module.sha256,
        )
        return identity

    def test_evidence(name: str) -> set[str]:
        result: set[str] = set()
        symbol = name.rsplit(".", 1)[-1].split("#", 1)[0]
        for path, (sha256, tokens) in test_sources.items():
            if (
                symbol not in tokens
                and path.name != "test_schema_compatibility_inventory.py"
            ):
                continue
            identity = "test-definition:sha256:" + sha256
            evidence[identity] = EvidenceV1(
                identity,
                EvidenceKind.TEST_DEFINITION,
                path.relative_to(root).as_posix(),
                sha256,
            )
            result.add(identity)
        return result

    def add_schema(
        module: _Module,
        name: str,
        wire: str | None,
        readers: tuple[str, ...],
        writers: tuple[str, ...],
        chain: list[tuple[_Module, ast.ClassDef]],
        note: str,
    ) -> None:
        match = re.search(r"(?:[.-]v|V)([0-9]+)(?:\Z|[.-])", wire or "")
        schema_version = (
            match.group(1) + ".0.0"
            if match
            else (
                wire + ".0"
                if wire and re.fullmatch(r"[0-9]+\.[0-9]+", wire)
                else "unversioned"
            )
        )
        sources = tuple(
            sorted(
                {source_evidence(item) for item, _ in chain}
                | {source_evidence(module)}
                | test_evidence(name)
            )
        )
        schemas.append(
            SchemaV1(
                name + "@" + schema_version,
                name,
                schema_version,
                wire or "unversioned:" + name,
                (
                    (
                        SupportStatus.SUPPORTED
                        if writers
                        else SupportStatus.READ_ONLY
                    )
                    if readers
                    else SupportStatus.WRITE_ONLY
                ),
                tuple(sorted(readers)),
                tuple(sorted(writers)),
                ("current-writer-shape:" + name,),
                sources,
                note,
            )
        )

    for name, (module, node) in sorted(classes.items()):
        chain = lineage(name)
        entrypoints = methods(chain)
        writers = sorted(WRITERS & entrypoints.keys()) or sorted(
            METADATA_WRITERS & entrypoints.keys()
        )
        readers = sorted(READERS & entrypoints.keys())
        # A component payload is not an alternate reader/writer for its full
        # versioned envelope. Embedded-only records retain their payload API.
        if "to_dict" in writers and "to_payload" in writers:
            writers.remove("to_payload")
        if "from_dict" in readers and "from_payload" in readers:
            readers.remove("from_payload")
        if name in SERIALIZER_EXEMPTIONS:
            exemptions.append(
                ExemptionV1(name, SERIALIZER_EXEMPTIONS[name], module.sha256)
            )
            continue
        if not writers:
            if module.name in PROCESS_LOCAL_MODULES:
                exemptions.append(
                    ExemptionV1(
                        name, PROCESS_LOCAL_MODULES[module.name], module.sha256
                    )
                )
            elif (
                node.name in {"_TemporalPoint", "_TemporalVerifiedSource"}
                and module.name
                == "histdatacom.data_quality.training_temporal_sources"
            ):
                exemptions.append(
                    ExemptionV1(
                        name,
                        "Process-local verified source points; no durable serialization; temporal source and row contracts retain wire ownership.",
                        module.sha256,
                    )
                )
            continue
        assignments = _assignments(node.body)
        generic = (
            any(
                isinstance(child, ast.AnnAssign)
                and isinstance(child.target, ast.Name)
                and child.target.id == "KIND"
                and child.value is None
                for child in node.body
            )
            or node.name == "Contract"
        )
        if generic:
            exemptions.append(
                ExemptionV1(
                    name,
                    "Abstract serializer template; concrete subclasses are inventoried separately; no standalone payload.",
                    module.sha256,
                )
            )
            continue
        if "KIND" not in assignments and any(
            "KIND" in _assignments(base.body) for _, base in chain[:-1]
        ):
            # Inherited concrete kind is still a real typed serializer.
            pass
        writer_ids = tuple(
            implementation(entrypoints[method][0], name + "." + method)
            for method in writers
        )
        reader_ids = tuple(
            implementation(entrypoints[method][0], name + "." + method)
            for method in readers
        )
        for function in CLASS_READERS.get(name, ()):
            if not any(
                isinstance(child, ast.FunctionDef) and child.name == function
                for child in module.tree.body
            ):
                raise ValueError("missing explicit class reader: " + name)
            reader_ids += (
                implementation(module, module.name + "." + function),
            )
        add_schema(
            module,
            name,
            wire_schema(chain),
            reader_ids,
            writer_ids,
            chain,
            (
                "Current declared serializer/reader only; source-bound implementation evidence, not a migration or historical-corpus qualification."
                if readers
                else "Current emitted representation; no class-owned direct reader is declared. No migration inferred."
            ),
        )

    # Independent functional-emitter sweep, including builders outside the old
    # reconstruction registry. Every top-level function with a schema-bearing
    # dictionary is accounted for, even when it is only a validation helper.
    for module in modules.values():
        scopes: list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef]] = []
        for node in module.tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                scopes.append((module.name + "." + node.name, node))
            elif isinstance(node, ast.ClassDef):
                scopes.extend(
                    (module.name + "." + node.name + "." + child.name, child)
                    for child in node.body
                    if isinstance(child, ast.FunctionDef)
                    and child.name not in WRITERS | READERS | METADATA_WRITERS
                )
        for name, scope in scopes:
            if name in FUNCTION_EXEMPTIONS:
                exemptions.append(
                    ExemptionV1(name, FUNCTION_EXEMPTIONS[name], module.sha256)
                )
                continue
            wires: set[str] = set()
            dynamic = False
            for child in ast.walk(scope):
                if isinstance(child, ast.Dict):
                    for key, value in zip(child.keys, child.values):
                        if (
                            isinstance(key, ast.Constant)
                            and key.value == "schema_version"
                        ):
                            resolved = evaluate(module, value)
                            if resolved:
                                wires.add(resolved)
                            else:
                                dynamic = True
            if not wires and not dynamic:
                continue
            known_wires = {item.wire_schema for item in schemas}
            if (
                scope.name.startswith("_")
                or scope.name.startswith(("validate", "verify"))
            ) and wires <= known_wires:
                exemptions.append(
                    ExemptionV1(
                        name,
                        "Internal shared schema assembler/checker, not an independent durable family; concrete public serializers and schema-bearing builders are inventoried.",
                        module.sha256,
                    )
                )
            else:
                for index, wire in enumerate(
                    sorted(wires) or ["dynamic:" + name]
                ):
                    add_schema(
                        module,
                        name + ("#" + str(index) if len(wires) > 1 else ""),
                        wire,
                        (),
                        (implementation(module, name),),
                        [],
                        "Functional schema-bearing builder. Direct parser compatibility is not inferred from validation or unrelated JSON readers.",
                    )

    # Independent physical-format sweep. Arrow/Parquet/CSV writers and schema
    # constructors may carry no JSON schema_version at all. Register each
    # concrete source surface; do not silently equate its physical rows with a
    # JSON envelope or certify a projection as lossless.
    registered = {item.family for item in schemas}
    exempted = {item.qualified_name for item in exemptions}
    for module in modules.values():
        for node in module.tree.body:
            physical_scopes: list[tuple[str, ast.AST]] = []
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                physical_scopes.append((module.name + "." + node.name, node))
            elif isinstance(node, ast.ClassDef):
                physical_scopes.extend(
                    (module.name + "." + node.name + "." + child.name, child)
                    for child in node.body
                    if isinstance(child, ast.FunctionDef)
                )
            for name, physical_scope in physical_scopes:
                calls = {
                    ast.unparse(child.func)
                    for child in ast.walk(physical_scope)
                    if isinstance(child, ast.Call)
                }
                formats = sorted(
                    call
                    for call in calls
                    if call in SCHEMA_CALLS
                    or call in PERSISTENCE_CALLS
                    or call.rsplit(".", 1)[-1] in PERSISTENCE_CALLS
                )
                if not formats or name in registered or name in exempted:
                    continue
                external_readers = PHYSICAL_READERS.get(name, ())
                external_writers = PHYSICAL_WRITERS.get(name, ())
                available = {
                    child.name
                    for child in module.tree.body
                    if isinstance(child, ast.FunctionDef)
                }
                if not set(external_readers + external_writers) <= available:
                    raise ValueError("missing physical reader: " + name)
                add_schema(
                    module,
                    name,
                    None,
                    tuple(
                        implementation(module, module.name + "." + reader)
                        for reader in external_readers
                    ),
                    (
                        tuple(
                            implementation(module, module.name + "." + writer)
                            for writer in external_writers
                        )
                        if external_writers
                        else (implementation(module, name),)
                    ),
                    [],
                    "Physical-format/schema surface ("
                    + ", ".join(formats)
                    + "). Shape remains owned by this source implementation. Explicit readers accept this physical representation with their existing scope checks; no migration is inferred.",
                )
                registered.add(name)

    # A separate declaration sweep must close every literal schema constant,
    # not merely repeat the writer-name heuristic. Unknown additions refuse
    # regeneration until explicitly classified; no catch-all exemption.
    known_wires = {item.wire_schema for item in schemas}
    for module in modules.values():
        for constant, expression in sorted(module.names.items()):
            if "SCHEMA_VERSION" not in constant:
                continue
            declared_wire = evaluate(module, expression)
            if declared_wire is None or declared_wire in known_wires:
                continue
            name = module.name + "." + constant
            if name in CONSTANT_EXEMPTIONS:
                exemptions.append(
                    ExemptionV1(
                        name,
                        CONSTANT_EXEMPTIONS[name],
                        module.sha256,
                        declared_wire,
                    )
                )
                continue
            if constant not in FORMAT_SUPPORT:
                raise ValueError(
                    "unclassified schema declaration: "
                    + name
                    + " = "
                    + declared_wire
                )
            reader_names, writer_names, note = FORMAT_SUPPORT[constant]
            available = {
                node.name
                for node in module.tree.body
                if isinstance(node, ast.FunctionDef)
            }
            available.update(
                node.name + "." + method.name
                for node in module.tree.body
                if isinstance(node, ast.ClassDef)
                for method in node.body
                if isinstance(method, ast.FunctionDef)
            )
            if not set(reader_names + writer_names) <= available:
                raise ValueError(
                    "missing reviewed format implementation: " + name
                )
            add_schema(
                module,
                name,
                declared_wire,
                tuple(
                    implementation(module, module.name + "." + key)
                    for key in reader_names
                ),
                tuple(
                    implementation(module, module.name + "." + key)
                    for key in writer_names
                ),
                [],
                note,
            )
            known_wires.add(declared_wire)

    return CompatibilityRegistryV1(
        tuple(sorted(schemas, key=lambda item: item.schema_id)),
        tuple(
            sorted(
                implementations.values(),
                key=lambda item: item.implementation_id,
            )
        ),
        tuple(sorted(evidence.values(), key=lambda item: item.evidence_id)),
        (),
        (),
        tuple(sorted(exemptions, key=lambda item: item.qualified_name)),
    )


def render_documentation(registry: CompatibilityRegistryV1) -> str:
    """The same frozen metadata drives public queries and these tables."""
    lines = [
        "# Schema compatibility and migration registry",
        "",
        "Generated by `scripts/generate_schema_compatibility.py`; do not edit the tables.",
        "",
        f"Registry identity: `{registry.registry_id}`.",
        "",
        "## Public metadata queries",
        "",
        "Import `schema_compatibility_registry`, `can_read`, `can_migrate`, `migration_path` and `exact_semantics` from `histdatacom.schema_compatibility`. Queries load only packaged metadata, never producer/migration implementations. Use the full schema ID below; a wire label is accepted only when unique.",
        "",
        "`can_read` describes the registered current reader (optionally constrain its exact implementation ID/qualified name and package version). It does not deserialize, validate an artifact, attest scientific fitness, or certify historical corpus replay. Reader support and migration reachability are different relations. Unversioned dictionary shapes are explicitly identified, not silently called a versioned wire standard.",
        "",
        "Migration paths require qualified edges, exact source/destination invariant coverage, implementation IDs, golden-corpus and invariant evidence, and an explicit retained whole-path composition test for multi-edge paths. Paths rank by edge count, then priorities and declared destination versions; equal-ranked alternatives refuse. Cycles and missing implementations refuse at registry construction. Exact queries permit only identity/direct-read or lossless-representation paths; semantic successors and lossy/advisory edges do not preserve exact semantics.",
        "",
        "The current registry declares **no qualified historical migration edges**: existing `compatible_translation` labels, constructors and successful deserialization are not migration qualification. Every absent transition is unsupported; no artifact is rewritten. Positive migration and composition behavior is tested with separately identified synthetic graphs, not passed off as production migration evidence.",
        "",
        "Offline registry publication and drift checking additionally run the #634 semantic-proof gate before writing or comparing assets. Every qualified lossless edge and applicable complete composition requires native-reader/projector evidence and re-execution of an explicitly admitted implementation. Equivalent supplied payloads or an implementation name alone are insufficient. Unreviewed families and executors cannot acquire lossless qualification. See [semantic proof contracts](schema-semantic-proofs.md). Runtime metadata queries remain producer-free; custom metadata graph declarations are not a substitute for executable semantic proof.",
        "",
        "## Inventory and evidence boundaries",
        "",
        "The offline generator independently inspects every package Python module for concrete serializers (including inherited templates), schema-bearing functional emitters and physical Arrow/Parquet/CSV writer/schema surfaces; it does not reuse the older reconstruction-only registry. It records exact source-byte identities and implementation entry points. This conservative inventory also includes supporting broker, market-context, forecasting and unversioned report contracts. Private shared assemblers and abstract templates have individual exemptions below. The checker regenerates metadata and this document from current source and refuses drift. Each retained test-definition identity binds exact test bytes, not a claim that every fixture is a golden corpus. Source-definition evidence is not golden-corpus qualification; version support is only the current source-bound implementation, not every release with the same package SemVer. `unversioned` is explicit missing wire-version metadata, never a fabricated v1 wire format. Physical-format surfaces have no direct reader asserted unless separately registered; a same-module reader does not by itself prove an exact inverse.",
        "",
        f"{len(registry.schemas)} registered shapes; {sum(bool(item.readers) for item in registry.schemas)} have declared direct readers; {len(registry.exemptions)} individually recorded template/helper exemptions.",
        "",
        "## Current schema families",
        "",
        "| Schema ID | Wire schema | Status | Reader / writer entry points |",
        "| --- | --- | --- | --- |",
    ]
    implementations = {
        item.implementation_id: item for item in registry.implementations
    }
    for item in registry.schemas:
        readers = (
            ", ".join(
                implementations[key].qualified_name.rsplit(".", 1)[-1]
                for key in item.readers
            )
            or "none"
        )
        writers = ", ".join(
            implementations[key].qualified_name.rsplit(".", 1)[-1]
            for key in item.writers
        )
        lines.append(
            f"| `{item.schema_id}` | `{item.wire_schema}` | {item.status.value} | {readers} / {writers} |"
        )
    lines.extend(
        [
            "",
            "## Individual exemptions",
            "",
            "| Source symbol | Reason |",
            "| --- | --- |",
        ]
    )
    lines.extend(
        f"| `{item.qualified_name}` | {item.reason} |"
        for item in registry.exemptions
    )
    lines.extend(
        [
            "",
            "## Rebuild / drift check",
            "",
            "Run `python scripts/generate_schema_compatibility.py --check` from a source checkout. `--write` intentionally updates the canonical asset and generated documentation after review. Neither mode reads historical datasets, model inputs, or empirical artifacts. Adding a durable serializer requires regeneration and focused qualification. New executable migrations additionally require real edge and composition evidence; this generator does not invent them.",
            "",
        ]
    )
    return "\n".join(lines)
