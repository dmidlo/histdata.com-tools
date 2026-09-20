"""Reviewed current-writer domains; no permissive future-field projection."""

from __future__ import annotations

import hashlib
import importlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from .canonical import canonical_json, exact_tree, load_json, sha256
from .contracts import SemanticProjectionV1

# These are reviewed source pins, not automatically regenerated declarations.
# Changes require reviewing the complete reader/writer domain before re-pinning.
_SOURCES = {
    "synthetic/contracts.py": "f7e9430905bedb71344336be0c5388a6e49197ad1bc1c30bd29d1eb792b5e933",
    "reconstruction_evidence.py": "a1ca4dd5705cbdd0adffa935272d10fc1ec42119ca95062cf0cf475484a552ab",
    "broker_plugins/contracts.py": "e17a51c0b406fe96cf73066abd5e9ae25804f9bb944ec5358d6fb17f7fdba18c",
    "broker_plugin_lifecycle/contracts.py": "ece9eb834a271bf3387fc85677fcb9f30b377a40a319ecfd7c1cd1de4b00b2bb",
    "broker_plugin_registry/contracts.py": "c15f3f0dfca961fb086018b65dd4c675c2c66026067b606e3b9d3d25468cfee3",
    "broker_plugin_capabilities/contracts.py": "d80c6f3773eb92788a5cdefd54c8379b03d14b9e7ed2deaecef960b21ffafed9",
    "market_context/economic_calendar.py": "e71254fdb69032fe032e1559ae89b19409d72d8e72c464bab7d2b42bc9db9761",
    "market_context/contracts.py": "6319014257ab8852f19626c0d6f71f45c9630371f2c8e65d94e9d60c76093bed",
    "forecasting/scoring.py": "97ef6d6fdeb1b2aa5a8033abac4970ec599ac871e334a89ac238ff7632070456",
    "forecasting/contracts.py": "88929cf1661eebc51e52b98286c119e1164fa167727b67d1592d023a87e29ba2",
    "data_quality/training_temporal_contracts.py": "225c8e90e15f2750bbb7d8c34e16734b09a5d20129ed03ad55855d449958ebc5",
    "data_quality/training_contracts.py": "a0226e3d274a0fd6e35455611e243228a43d3f044ca9b42ecd06ffe1dcf12aef",
}


def module_fingerprint(paths: tuple[str, ...]) -> str:
    """Fresh bounded source bytes, not Git ancestry or loaded-code attestation."""
    root = Path(__file__).resolve().parents[1]
    result = []
    for name in sorted(set(paths)):
        if name not in _SOURCES and name not in (
            "schema_semantics/canonical.py",
            "schema_semantics/profiles.py",
            "schema_semantics/executors.py",
            "schema_semantics/proofs.py",
        ):
            raise ValueError("unreviewed semantic implementation path")
        path = root / name
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size > 4 * 1024 * 1024:
            raise ValueError(
                "semantic implementation is not bounded regular source"
            )
        fd = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        with os.fdopen(fd, "rb") as handle:
            opened = os.fstat(handle.fileno())
            data = handle.read(4 * 1024 * 1024 + 1)
            after = os.fstat(handle.fileno())

        def stamp(value: os.stat_result) -> tuple[int, ...]:
            return (
                value.st_dev,
                value.st_ino,
                value.st_mode,
                value.st_size,
                value.st_mtime_ns,
                value.st_ctime_ns,
            )

        if (
            stamp(info) != stamp(opened)
            or stamp(opened) != stamp(after)
            or len(data) != info.st_size
        ):
            raise ValueError("semantic implementation changed during read")
        digest = hashlib.sha256(data).hexdigest()
        if name in _SOURCES and _SOURCES[name] != digest:
            raise ValueError(
                "native reader changed; semantic profile review required"
            )
        result.append([name, digest])
    return sha256(canonical_json(result))


@dataclass(frozen=True, slots=True)
class SemanticProfileV1:
    """Process-local reviewed policy, not a caller-extensible schema loader."""

    name: str
    family: str
    wire_schema: str
    fields: tuple[str, ...]
    self_identity_field: str
    sources: tuple[str, ...]

    @property
    def profile_id(self) -> str:
        return "semantic-profile:sha256:" + sha256(
            canonical_json(
                {
                    "name": self.name,
                    "family": self.family,
                    "wire_schema": self.wire_schema,
                    "fields": self.fields,
                    "self_identity_field": self.self_identity_field,
                    "source_pins": [[p, _SOURCES[p]] for p in self.sources],
                    "policy": "complete-writer-exact-typed-tree.v1",
                }
            )
        )


def _profile(
    name: str,
    family: str,
    schema: str,
    fields: str,
    identity: str,
    sources: str,
) -> SemanticProfileV1:
    return SemanticProfileV1(
        name,
        "histdatacom." + family,
        schema,
        tuple(sorted(fields.split())),
        identity,
        tuple(sources.split()),
    )


_PROFILES = (
    _profile(
        "synthetic-event-v1",
        "synthetic.contracts.SyntheticEventV1",
        "histdatacom.synthetic-event.v1",
        "anchor_interval_id ask bid broker_profile_id confidence constraint_set_id ensemble_member_id event_id event_sequence event_time_ns feed_epoch_id generator_config_id generator_id generator_version left_anchor_event_id motif_id origin reference_id right_anchor_event_id run_id schema_version source_period source_row_id source_series_id source_version_id symbol",
        "event_id",
        "synthetic/contracts.py",
    ),
    _profile(
        "synthetic-stream-v1",
        "synthetic.contracts.SyntheticEventStreamV1",
        "histdatacom.synthetic-event-stream.v1",
        "ensemble_member_id event_count event_schema_version events observed_event_count run_id schema_version source_version_ids stream_id symbol synthetic_event_count",
        "stream_id",
        "synthetic/contracts.py",
    ),
    _profile(
        "evidence-projection-v1",
        "reconstruction_evidence.PointInTimeEvidenceProjectionV1",
        "histdatacom.reconstruction-evidence-projection.v1",
        "as_of_ns available_at_ns bounded_sidecar evidence_window_id full_reports_embedded full_tick_rows_embedded information_mode limitations omitted_record_count period policy_id projection_id row_records schema_version sidecar_records source_artifact_id source_artifact_sha256 source_partition_id source_provider_id status support_end_ns support_start_ns symbol",
        "projection_id",
        "reconstruction_evidence.py synthetic/contracts.py",
    ),
    _profile(
        "broker-metadata-v1",
        "broker_plugins.contracts.BrokerPluginMetadataV1",
        "histdatacom.broker-plugin.metadata.v1",
        "artifact_id display_name extensions plugin_id plugin_version schema_version sdk_version",
        "artifact_id",
        "broker_plugins/contracts.py",
    ),
    _profile(
        "broker-event-v1",
        "broker_plugins.contracts.BrokerEventV1",
        "histdatacom.broker-plugin.event.v1",
        "artifact_id connection_id diagnostic extensions gap instrument kind quote raw_provenance receive_time schema_version sequence session_id source_time",
        "artifact_id",
        "broker_plugins/contracts.py",
    ),
    _profile(
        "broker-lifecycle-manifest-v1",
        "broker_plugin_lifecycle.contracts.BrokerLifecycleManifestV1",
        "histdatacom.broker-lifecycle.manifest.v1",
        "appended_events appended_records artifact_id completion configuration_material_retained discarded_known duplicate_deliveries error_diagnostics forced_terminations header partial_partition partitions received_events schema_version source_continuity_verified state stderr_discarded_bytes stdout_discarded_bytes unknown_loss worker_reaped",
        "artifact_id",
        "broker_plugin_lifecycle/contracts.py broker_plugin_registry/contracts.py broker_plugin_capabilities/contracts.py broker_plugins/contracts.py",
    ),
    _profile(
        "economic-release-v1",
        "market_context.economic_calendar.EconomicCalendarReleaseV1",
        "histdatacom.economic-calendar-release.v1",
        "actual_lexical actual_value affected_currencies affected_symbols available_at_ns base comparability_bridge_id content_sha256 currency economy economy_code event_family first_observed_at_ns frequency indicator_id institution limitations logical_event_key market_context_kind precision reference_period reference_period_end_ns release_id released_at_ns released_lexical revision_sequence scale schedule_change_reason scheduled_for_ns scheduled_lexical schema_version seasonality series_id series_key series_version source source_release_id source_request_id source_series_id source_table_id source_timezone stage status supersedes_release_id time_precision timezone_evidence title unit value_conversion",
        "release_id",
        "market_context/economic_calendar.py market_context/contracts.py",
    ),
    _profile(
        "forecast-score-v1",
        "forecasting.scoring.ForecastScoreV1",
        "1.0",
        "contract_type id metrics normalizer outcome_calendar schema_version scored_at_ns snapshot",
        "id",
        "forecasting/scoring.py forecasting/contracts.py market_context/economic_calendar.py market_context/contracts.py",
    ),
    _profile(
        "training-temporal-batch-v1",
        "data_quality.training_temporal_contracts.TrainingTemporalBatchV1",
        "histdatacom.training-temporal-batch.v1",
        "artifact_id maximum_dependency_span_ns nonclaims plan rows schema_version split_boundaries_ns",
        "artifact_id",
        "data_quality/training_temporal_contracts.py data_quality/training_contracts.py",
    ),
)


def semantic_profiles() -> tuple[SemanticProfileV1, ...]:
    return _PROFILES


def semantic_profile(name_or_id: str) -> SemanticProfileV1:
    for profile in _PROFILES:
        if name_or_id in (profile.name, profile.profile_id):
            return profile
    raise ValueError("unreviewed semantic profile")


def project_semantics(profile_id: str, input_json: str) -> SemanticProjectionV1:
    """Native verification + independent exact projection of admitted raw data.

    Native writer output is used ONLY to reject ignored/coerced fields, not as
    the semantic projection. This verifies embedded evidence self-consistency,
    not external source authenticity, live capture replay or scientific fitness.
    """
    profile = semantic_profile(profile_id)
    reader_sha = module_fingerprint(profile.sources)
    data = load_json(input_json)
    if (
        type(data) is not dict
        or tuple(sorted(data)) != profile.fields
        or data["schema_version"] != profile.wire_schema
    ):
        raise ValueError("input outside closed current-writer profile")
    module_name, class_name = profile.family.rsplit(".", 1)
    try:
        native = getattr(
            importlib.import_module(module_name), class_name
        ).from_dict(data)
        written = native.to_dict()
    except (
        KeyError,
        TypeError,
        IndexError,
        AttributeError,
        OverflowError,
        RecursionError,
    ):
        raise ValueError(
            "native artifact is outside reviewed reader domain"
        ) from None
    # Type-sensitive, recursive equality refuses normalizations, ignored future
    # keys, dropped embedded fields, missing defaults and reordered events.
    if canonical_json(exact_tree(data)) != canonical_json(exact_tree(written)):
        raise ValueError(
            "native reader normalized/ignored a semantic input field"
        )
    semantic = {
        key: value
        for key, value in data.items()
        if key != profile.self_identity_field
    }
    projector_sha = module_fingerprint(
        ("schema_semantics/canonical.py", "schema_semantics/profiles.py")
    )
    return SemanticProjectionV1(
        profile.profile_id,
        reader_sha,
        projector_sha,
        sha256(input_json),
        canonical_json(
            {"domain": profile.profile_id, "meaning": exact_tree(semantic)}
        ),
    )
