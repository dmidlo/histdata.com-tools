"""Tests for archive-first official economic-vintage reconstruction."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from histdatacom.market_context import (
    EconomicArchiveArtifactKind,
    EconomicArchiveArtifactV1,
    EconomicArchiveSourceEraV1,
    EconomicArchiveVintageAuditV1,
    EconomicArchiveVintageBindingV1,
    EconomicArchiveVintageCorpusV1,
    EconomicCalendarReleaseV1,
    EconomicEventFamily,
    EconomicLatestValueCrossCheckV1,
    EconomicReleaseAvailabilityBasis,
    EconomicReleaseRevisionKind,
    EconomicReleaseStage,
    EconomicReleaseStatus,
    EconomicReleaseTimeEvidenceV1,
    EconomicReleaseVintageChainV1,
    EconomicReleaseVintageMutationV1,
    EconomicTimePrecision,
    EconomicVintageCoverageDeclarationV1,
    EconomicVintageGapReason,
    EconomicVintageGapV1,
    EconomicVintageRecoveryMode,
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
    OfficialFetchPolicyV1,
    OfficialRawSnapshotV1,
    OfficialSourceRegistryV1,
    OfficialSourceRole,
    audit_economic_archive_vintages,
    build_economic_archive_artifact,
    build_economic_archive_vintage_corpus,
    build_economic_release_vintage_chain,
    conservative_official_vintage_coverage,
    load_packaged_official_source_registry,
    plan_official_source_requests,
    read_economic_archive_vintage_corpus,
    resolve_economic_archive_mirrors,
    write_economic_archive_vintage_corpus,
)

SECOND_NS = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND_NS
PUB_1 = BASE_NS + 100 * SECOND_NS
PUB_2 = BASE_NS + 200 * SECOND_NS
SOURCE_KEY = "us.bls.public-data"
SERIES_KEY = "us.cpi.all-items.sa.mom"


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _utc_text(value: int) -> str:
    return datetime.fromtimestamp(
        value / SECOND_NS, tz=timezone.utc
    ).isoformat()


def _snapshot(
    registry: OfficialSourceRegistryV1,
    content: bytes,
    retrieved_at_ns: int,
    *,
    source_key: str = SOURCE_KEY,
) -> OfficialRawSnapshotV1:
    source = registry.source(source_key)
    request = plan_official_source_requests(
        source,
        OfficialFetchPolicyV1(max_response_bytes=4096, max_total_bytes=8192),
    )[0]
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=retrieved_at_ns,
        completed_at_ns=retrieved_at_ns + 1,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"Content-Type": "application/json"},
        content=content,
        content_type="application/json",
    )


def _artifact(
    registry: OfficialSourceRegistryV1,
    content: bytes,
    retrieved_at_ns: int,
    kind: EconomicArchiveArtifactKind,
    *,
    source_key: str = SOURCE_KEY,
) -> EconomicArchiveArtifactV1:
    return build_economic_archive_artifact(
        _snapshot(
            registry,
            content,
            retrieved_at_ns,
            source_key=source_key,
        ),
        kind,
        limitations=("Fixture artifact proves archive governance only.",),
    )


def _source(observed_at_ns: int, digest: str) -> MarketContextSourceV1:
    return MarketContextSourceV1(
        name="Official release archive fixture",
        source_version="2026-09-14",
        retrieved_at_ns=observed_at_ns,
        content_sha256=digest,
        adapter_name="official-release-archive-fixture",
        adapter_version="1",
        license_name="Public official fixture",
        redistribution_allowed=True,
        redistribution_constraints=("Attribute the producer.",),
        limitations=("Fixture proves archive governance only.",),
        source_uri="https://example.invalid/release-archive",
    )


def _time(
    published_at_ns: int,
    observed_at_ns: int,
) -> EconomicReleaseTimeEvidenceV1:
    return EconomicReleaseTimeEvidenceV1(
        scheduled_for_ns=published_at_ns,
        scheduled_lexical=_utc_text(published_at_ns),
        actual_published_at_ns=published_at_ns,
        actual_published_lexical=_utc_text(published_at_ns),
        first_observed_at_ns=observed_at_ns,
        source_updated_at_ns=None,
        source_updated_lexical=None,
        available_at_ns=published_at_ns,
        publication_not_before_ns=None,
        publication_not_after_ns=None,
        source_timezone="UTC",
        timezone_evidence="Official UTC timestamp fixture.",
        time_precision=EconomicTimePrecision.EXACT_SECOND,
        precision=MarketContextPrecision.EXACT,
        availability_basis=EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION,
    )


def _initial_release(
    logical_event_key: str,
    reference_period: str,
    reference_period_end_ns: int,
    published_at_ns: int,
    value: float,
    digest: str,
) -> tuple[EconomicCalendarReleaseV1, EconomicReleaseTimeEvidenceV1]:
    evidence = _time(published_at_ns, published_at_ns + 10 * SECOND_NS)
    release = EconomicCalendarReleaseV1(
        logical_event_key=logical_event_key,
        series_key=SERIES_KEY,
        series_version="v1",
        comparability_bridge_id=None,
        economy="United States",
        economy_code="US",
        currency="USD",
        institution="U.S. Bureau of Labor Statistics",
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_id="cpi-all-items",
        source_series_id="CUUR0000SA0",
        source_table_id="CPI-TABLE-1",
        source_release_id=f"release.{digest[:8]}",
        source_request_id=f"request.{digest[:8]}",
        title="Consumer Price Index",
        reference_period=reference_period,
        reference_period_end_ns=reference_period_end_ns,
        frequency="monthly",
        seasonality="seasonally-adjusted",
        unit="index",
        scale=1.0,
        base="1982-84=100",
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.RELEASED,
        scheduled_for_ns=evidence.scheduled_for_ns,
        scheduled_lexical=evidence.scheduled_lexical,
        released_at_ns=evidence.actual_published_at_ns,
        released_lexical=evidence.actual_published_lexical,
        first_observed_at_ns=evidence.first_observed_at_ns,
        available_at_ns=evidence.available_at_ns,
        source_timezone=evidence.source_timezone,
        timezone_evidence=evidence.timezone_evidence,
        time_precision=evidence.time_precision,
        precision=evidence.precision,
        market_context_kind=MarketContextKind.MACRO_RELEASE,
        source=_source(evidence.first_observed_at_ns, digest),
        affected_currencies=("USD",),
        affected_symbols=("EURUSD",),
        limitations=("Fixture release proves archive governance only.",),
        actual_value=value,
        actual_lexical=str(value),
        content_sha256=digest,
    )
    return release, evidence


def _direct_chain(
    logical_event_key: str = "us.cpi.2024-01",
    reference_period: str = "2024-01",
    reference_period_end_ns: int = BASE_NS + 50 * SECOND_NS,
    published_at_ns: int = PUB_1,
    value: float = 308.1,
    digest: str = "1" * 64,
) -> EconomicReleaseVintageChainV1:
    release, evidence = _initial_release(
        logical_event_key,
        reference_period,
        reference_period_end_ns,
        published_at_ns,
        value,
        digest,
    )
    return build_economic_release_vintage_chain(
        release,
        initial_time_evidence=evidence,
        limitations=("Fixture chain is intentionally bounded.",),
    )


def _revision(
    previous: EconomicCalendarReleaseV1,
    *,
    published_at_ns: int,
    value: float,
    digest: str,
    kind: EconomicReleaseRevisionKind,
    periods: tuple[str, ...],
    trigger: str | None = None,
) -> EconomicReleaseVintageMutationV1:
    evidence = _time(published_at_ns, published_at_ns + 10 * SECOND_NS)
    return EconomicReleaseVintageMutationV1(
        previous_release_id=previous.release_id,
        logical_event_key=previous.logical_event_key,
        series_id=previous.series_id,
        source_release_id=f"release.{digest[:8]}",
        source_request_id=f"request.{digest[:8]}",
        stage=EconomicReleaseStage.REVISION,
        status=EconomicReleaseStatus.RELEASED,
        time_evidence=evidence,
        source=_source(evidence.first_observed_at_ns, digest),
        limitations=("Fixture revision proves archive governance only.",),
        actual_value=value,
        actual_lexical=str(value),
        content_sha256=digest,
        revision_kind=kind,
        affected_reference_periods=periods,
        triggering_logical_event_key=trigger,
    )


def _binding(
    release: EconomicCalendarReleaseV1,
    artifact: EconomicArchiveArtifactV1,
    normalized_value: str,
    precedence: int = 0,
) -> EconomicArchiveVintageBindingV1:
    return EconomicArchiveVintageBindingV1(
        release_id=release.release_id,
        logical_event_key=release.logical_event_key,
        revision_sequence=release.revision_sequence,
        artifact_id=artifact.artifact_id,
        source_key=artifact.source_key,
        source_id=artifact.source_id,
        normalized_record_sha256=_sha(normalized_value),
        record_locator=f"$.releases[{release.revision_sequence}]",
        source_precedence=precedence,
    )


def _coverage(
    registry: OfficialSourceRegistryV1,
    artifact_ids: tuple[str, ...],
    *,
    recovery_mode: EconomicVintageRecoveryMode = (
        EconomicVintageRecoveryMode.ARCHIVE_RECONSTRUCTED
    ),
) -> tuple[EconomicVintageCoverageDeclarationV1, ...]:
    values = conservative_official_vintage_coverage(registry)
    return tuple(
        (
            replace(
                item,
                recovery_mode=recovery_mode,
                initial_actual_qualified=True,
                previous_as_known_qualified=True,
                coverage_start_ns=0,
                coverage_end_ns=2**63 - 1,
                evidence_artifact_ids=artifact_ids,
                qualification_evidence=("Contemporaneous archive fixture.",),
                limitations=("Qualification is fixture-scoped.",),
                declaration_id="",
            )
            if item.cell == ("US", EconomicEventFamily.INFLATION_PRICES)
            else item
        )
        for item in values
    )


def _era(
    artifact: EconomicArchiveArtifactV1,
    *,
    start: int = 0,
    end: int | None = None,
) -> EconomicArchiveSourceEraV1:
    return EconomicArchiveSourceEraV1(
        economy_code="US",
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_key=SERIES_KEY,
        source_key=artifact.source_key,
        source_id=artifact.source_id,
        effective_from_ns=start,
        effective_to_ns=end,
        parser_id=artifact.parser_id,
        parser_version=artifact.parser_version,
        evidence_artifact_ids=(artifact.artifact_id,),
        limitations=("Fixture source era.",),
    )


def _corpus(
    registry: OfficialSourceRegistryV1,
    chains: tuple[EconomicReleaseVintageChainV1, ...],
    artifacts: tuple[EconomicArchiveArtifactV1, ...],
    bindings: tuple[EconomicArchiveVintageBindingV1, ...],
    eras: tuple[EconomicArchiveSourceEraV1, ...],
    *,
    coverage: tuple[EconomicVintageCoverageDeclarationV1, ...] | None = None,
    gaps: tuple[EconomicVintageGapV1, ...] = (),
    cross_checks: tuple[EconomicLatestValueCrossCheckV1, ...] = (),
) -> EconomicArchiveVintageCorpusV1:
    grouped: dict[tuple[str, int], list[EconomicArchiveVintageBindingV1]] = {}
    for binding in bindings:
        grouped.setdefault(binding.record_key, []).append(binding)
    return build_economic_archive_vintage_corpus(
        registry,
        coverage
        or _coverage(registry, tuple(item.artifact_id for item in artifacts)),
        artifacts=artifacts,
        bindings=bindings,
        mirror_resolutions=tuple(
            resolve_economic_archive_mirrors(values)
            for _, values in sorted(grouped.items())
        ),
        source_eras=eras,
        vintage_chains=chains,
        gaps=gaps,
        latest_value_cross_checks=cross_checks,
        complete=False,
        limitations=("Fixture corpus is intentionally incomplete.",),
    )


def test_conservative_matrix_declares_all_cells_without_vintage_claims() -> (
    None
):
    registry = load_packaged_official_source_registry()
    declarations = conservative_official_vintage_coverage(registry)
    assert len(declarations) == 21 * len(EconomicEventFamily) == 252
    assert len({item.cell for item in declarations}) == 252
    assert {item.recovery_mode for item in declarations} == {
        EconomicVintageRecoveryMode.HISTORICALLY_INCOMPLETE
    }
    assert sum(item.api_latest_only for item in declarations) == 250
    assert all(
        not item.initial_actual_qualified
        and not item.previous_as_known_qualified
        and item.coverage_start_ns is None
        for item in declarations
    )


def test_latest_only_current_api_cannot_support_a_historical_release() -> None:
    registry = load_packaged_official_source_registry()
    artifact = _artifact(
        registry,
        b'{"latest": 309.2}',
        PUB_2,
        EconomicArchiveArtifactKind.CURRENT_API,
    )
    chain = _direct_chain()
    binding = _binding(chain.releases[0], artifact, "309.2")
    with pytest.raises(ValueError, match="latest-only API"):
        _corpus(
            registry,
            (chain,),
            (artifact,),
            (binding,),
            (_era(artifact),),
        )


def test_current_api_difference_is_diagnostic_and_never_rewrites_history(
    tmp_path: Path,
) -> None:
    registry = load_packaged_official_source_registry()
    archive = _artifact(
        registry,
        b'{"release": 308.1}',
        PUB_1 + 20 * SECOND_NS,
        EconomicArchiveArtifactKind.CONTEMPORANEOUS_RELEASE,
    )
    mirror = _artifact(
        registry,
        b'{"mirror": 308.1}',
        PUB_1 + 30 * SECOND_NS,
        EconomicArchiveArtifactKind.OFFICIAL_MIRROR,
    )
    current = _artifact(
        registry,
        b'{"latest": 309.2}',
        PUB_2 + 20 * SECOND_NS,
        EconomicArchiveArtifactKind.CURRENT_API,
    )
    chain = _direct_chain()
    release = chain.releases[0]
    primary_binding = _binding(release, archive, "308.1")
    mirror_binding = _binding(release, mirror, "308.1", precedence=1)
    cross_check = EconomicLatestValueCrossCheckV1(
        logical_event_key=release.logical_event_key,
        historical_release_id=release.release_id,
        current_artifact_id=current.artifact_id,
        current_record_locator="$.latest",
        historical_normalized_sha256=_sha("308.1"),
        current_normalized_sha256=_sha("309.2"),
        checked_at_ns=PUB_2 + 30 * SECOND_NS,
    )
    corpus = _corpus(
        registry,
        (chain,),
        (archive, mirror, current),
        (primary_binding, mirror_binding),
        (_era(archive),),
        coverage=_coverage(registry, (archive.artifact_id, mirror.artifact_id)),
        cross_checks=(cross_check,),
    )
    assert corpus.vintage_chains[0].releases[0].actual_value == 308.1
    assert corpus.mirror_resolutions[0].equivalent_binding_ids == (
        mirror_binding.binding_id,
    )
    audit = audit_economic_archive_vintages(corpus)
    assert audit.passed
    assert audit.current_value_mismatch_event_keys == ("us.cpi.2024-01",)
    assert EconomicArchiveVintageAuditV1.from_json(audit.to_json()) == audit
    assert EconomicArchiveVintageCorpusV1.from_json(corpus.to_json()) == corpus
    path = write_economic_archive_vintage_corpus(
        corpus, tmp_path / "archive-vintages.json"
    )
    assert read_economic_archive_vintage_corpus(path) == corpus

    foreign_current = _artifact(
        registry,
        b'{"latest": 309.2}',
        PUB_2 + 20 * SECOND_NS,
        EconomicArchiveArtifactKind.CURRENT_API,
        source_key="gb.ons.api",
    )
    foreign_check = replace(
        cross_check,
        current_artifact_id=foreign_current.artifact_id,
        cross_check_id="",
    )
    with pytest.raises(ValueError, match="cross-check source differs"):
        _corpus(
            registry,
            (chain,),
            (archive, foreign_current),
            (primary_binding,),
            (_era(archive),),
            coverage=_coverage(registry, (archive.artifact_id,)),
            cross_checks=(foreign_check,),
        )


def test_retained_snapshot_cannot_be_backdated_before_its_capture() -> None:
    registry = load_packaged_official_source_registry()
    snapshot = _artifact(
        registry,
        b'{"captured": 308.1}',
        PUB_2,
        EconomicArchiveArtifactKind.RETAINED_SNAPSHOT,
    )
    chain = _direct_chain()
    binding = _binding(chain.releases[0], snapshot, "308.1")
    with pytest.raises(ValueError, match="backdated before capture"):
        _corpus(
            registry,
            (chain,),
            (snapshot,),
            (binding,),
            (_era(snapshot),),
            coverage=_coverage(
                registry,
                (snapshot.artifact_id,),
                recovery_mode=EconomicVintageRecoveryMode.SNAPSHOT_DEPENDENT,
            ),
        )


def test_mirror_resolution_uses_precedence_hashes_and_rejects_ambiguity() -> (
    None
):
    registry = load_packaged_official_source_registry()
    first = _artifact(
        registry,
        b'{"first": 308.1}',
        PUB_1 + 10 * SECOND_NS,
        EconomicArchiveArtifactKind.CONTEMPORANEOUS_RELEASE,
    )
    second = _artifact(
        registry,
        b'{"second": 999.0}',
        PUB_1 + 20 * SECOND_NS,
        EconomicArchiveArtifactKind.OFFICIAL_MIRROR,
    )
    release = _direct_chain().releases[0]
    preferred = _binding(release, first, "308.1", precedence=0)
    conflicting = _binding(release, second, "999.0", precedence=10)
    resolution = resolve_economic_archive_mirrors((conflicting, preferred))
    assert resolution.selected_binding_id == preferred.binding_id
    assert resolution.conflicting_binding_ids == (conflicting.binding_id,)

    ambiguous = replace(conflicting, source_precedence=0, binding_id="")
    with pytest.raises(ValueError, match="equal-precedence"):
        resolve_economic_archive_mirrors((preferred, ambiguous))

    different_release = replace(release, content_sha256="e" * 64, release_id="")
    wrong_binding = _binding(different_release, second, "308.1", precedence=1)
    with pytest.raises(ValueError, match="binding differs from release chain"):
        _corpus(
            registry,
            (_direct_chain(),),
            (first, second),
            (preferred, wrong_binding),
            (_era(first),),
        )


def test_archive_source_migration_uses_explicit_nonoverlapping_eras() -> None:
    registry = load_packaged_official_source_registry()
    current_source = registry.source(SOURCE_KEY)
    legacy_source = replace(
        current_source,
        source_key="us.bls.legacy-archive",
        roles=(OfficialSourceRole.OFFICIAL_ARCHIVE,),
        parser_id="official.bls.legacy",
        parser_version="0",
        fallback_source_keys=(),
    )
    migrated_registry = replace(
        registry,
        sources=registry.sources + (legacy_source,),
    )
    initial, evidence = _initial_release(
        "us.cpi.migrated",
        "2024-01",
        BASE_NS + 50 * SECOND_NS,
        PUB_1,
        308.1,
        "2" * 64,
    )
    mutation = _revision(
        initial,
        published_at_ns=PUB_2,
        value=308.3,
        digest="3" * 64,
        kind=EconomicReleaseRevisionKind.CORRECTION,
        periods=("2024-01",),
    )
    chain = build_economic_release_vintage_chain(
        initial,
        initial_time_evidence=evidence,
        mutations=(mutation,),
        limitations=("Source migration fixture.",),
    )
    legacy_artifact = _artifact(
        migrated_registry,
        b'{"legacy": 308.1}',
        PUB_1 + 50 * SECOND_NS,
        EconomicArchiveArtifactKind.ARCHIVED_TABLE,
        source_key=legacy_source.source_key,
    )
    current_artifact = _artifact(
        migrated_registry,
        b'{"current": 308.3}',
        PUB_2 + 50 * SECOND_NS,
        EconomicArchiveArtifactKind.ARCHIVED_TABLE,
    )
    bindings = (
        _binding(chain.releases[0], legacy_artifact, "308.1"),
        _binding(chain.releases[1], current_artifact, "308.3"),
    )
    corpus = _corpus(
        migrated_registry,
        (chain,),
        (legacy_artifact, current_artifact),
        bindings,
        (
            _era(legacy_artifact, end=PUB_2),
            _era(current_artifact, start=PUB_2),
        ),
        coverage=_coverage(
            migrated_registry,
            (legacy_artifact.artifact_id, current_artifact.artifact_id),
        ),
    )
    assert audit_economic_archive_vintages(corpus).passed
    assert tuple(item.source_key for item in corpus.bindings) == (
        legacy_source.source_key,
        SOURCE_KEY,
    )

    overlapping = replace(
        corpus.source_eras[1], effective_from_ns=PUB_1, era_id=""
    )
    with pytest.raises(ValueError, match="source eras overlap"):
        _corpus(
            migrated_registry,
            (chain,),
            (legacy_artifact, current_artifact),
            bindings,
            (corpus.source_eras[0], overlapping),
            coverage=corpus.coverage_declarations,
        )


def test_missing_old_artifact_is_an_explicit_gap_not_a_current_value() -> None:
    registry = load_packaged_official_source_registry()
    archive_index = _artifact(
        registry,
        b'{"releases": []}',
        PUB_2,
        EconomicArchiveArtifactKind.ARCHIVE_INDEX,
    )
    gap = EconomicVintageGapV1(
        economy_code="US",
        event_family=EconomicEventFamily.INFLATION_PRICES,
        indicator_key=SERIES_KEY,
        logical_event_key="us.cpi.missing-2020-01",
        reference_period="2020-01",
        reason=EconomicVintageGapReason.MISSING_ARTIFACT,
        attempted_artifact_ids=(archive_index.artifact_id,),
        details="Official release index contains no retained contemporaneous artifact.",
    )
    corpus = _corpus(
        registry,
        (),
        (archive_index,),
        (),
        (),
        coverage=conservative_official_vintage_coverage(registry),
        gaps=(gap,),
    )
    audit = audit_economic_archive_vintages(corpus)
    assert audit.passed
    assert audit.gap_count == 1
    assert audit.incomplete_cells
    assert corpus.vintage_chains == ()
    with pytest.raises(ValueError, match="unqualified coverage or gaps"):
        replace(corpus, complete=True, corpus_id="")


def test_coverage_evidence_cannot_cross_an_economy_family_cell() -> None:
    registry = load_packaged_official_source_registry()
    foreign = _artifact(
        registry,
        b'{"releases": []}',
        PUB_2,
        EconomicArchiveArtifactKind.ARCHIVE_INDEX,
        source_key="gb.ons.api",
    )
    with pytest.raises(ValueError, match="outside its economy/family cell"):
        _corpus(
            registry,
            (),
            (foreign,),
            (),
            (),
            coverage=_coverage(registry, (foreign.artifact_id,)),
        )


def test_benchmark_revision_artifact_covers_every_affected_period() -> None:
    registry = load_packaged_official_source_registry()
    artifact = _artifact(
        registry,
        b'{"benchmark": ["2024-01", "2024-02"]}',
        PUB_2 + 50 * SECOND_NS,
        EconomicArchiveArtifactKind.ARCHIVED_TABLE,
    )
    chains = []
    bindings = []
    for index, (period, value) in enumerate(
        (("2024-01", 308.1), ("2024-02", 309.0))
    ):
        initial, evidence = _initial_release(
            f"us.cpi.{period}",
            period,
            BASE_NS + (50 + index * 10) * SECOND_NS,
            PUB_1 + index * 10 * SECOND_NS,
            value,
            str(4 + index) * 64,
        )
        mutation = _revision(
            initial,
            published_at_ns=PUB_2,
            value=value + 0.2,
            digest=str(6 + index) * 64,
            kind=EconomicReleaseRevisionKind.BENCHMARK,
            periods=("2024-01", "2024-02"),
        )
        chain = build_economic_release_vintage_chain(
            initial,
            initial_time_evidence=evidence,
            mutations=(mutation,),
            limitations=("Benchmark fixture.",),
        )
        chains.append(chain)
        bindings.extend(
            _binding(release, artifact, str(release.actual_value))
            for release in chain.releases
        )
    corpus = _corpus(
        registry,
        tuple(chains),
        (artifact,),
        tuple(bindings),
        (_era(artifact),),
    )
    audit = audit_economic_archive_vintages(corpus)
    assert audit.passed
    assert audit.benchmark_batch_violations == ()

    second_artifact = _artifact(
        registry,
        b'{"benchmark-mirror": ["2024-01", "2024-02"]}',
        PUB_2 + 60 * SECOND_NS,
        EconomicArchiveArtifactKind.ARCHIVED_TABLE,
    )
    split_bindings = tuple(
        (
            _binding(release, second_artifact, str(release.actual_value))
            if chain_index == 1 and release.revision_sequence == 1
            else _binding(release, artifact, str(release.actual_value))
        )
        for chain_index, chain in enumerate(chains)
        for release in chain.releases
    )
    split_corpus = _corpus(
        registry,
        tuple(chains),
        (artifact, second_artifact),
        split_bindings,
        (_era(artifact),),
    )
    split_audit = audit_economic_archive_vintages(split_corpus)
    assert not split_audit.passed
    assert len(split_audit.benchmark_batch_violations) == 2


def test_simultaneous_previous_revision_requires_matching_newer_release() -> (
    None
):
    registry = load_packaged_official_source_registry()
    artifact = _artifact(
        registry,
        b'{"release": 309.0, "revised_previous": 308.3}',
        PUB_2 + 50 * SECOND_NS,
        EconomicArchiveArtifactKind.CONTEMPORANEOUS_RELEASE,
    )
    prior, prior_evidence = _initial_release(
        "us.cpi.2024-01",
        "2024-01",
        BASE_NS + 50 * SECOND_NS,
        PUB_1,
        308.1,
        "8" * 64,
    )
    trigger_key = "us.cpi.2024-02"
    simultaneous = _revision(
        prior,
        published_at_ns=PUB_2,
        value=308.3,
        digest="9" * 64,
        kind=EconomicReleaseRevisionKind.SIMULTANEOUS_PREVIOUS,
        periods=("2024-01",),
        trigger=trigger_key,
    )
    prior_chain = build_economic_release_vintage_chain(
        prior,
        initial_time_evidence=prior_evidence,
        mutations=(simultaneous,),
        limitations=("Simultaneous previous fixture.",),
    )
    current_chain = _direct_chain(
        logical_event_key=trigger_key,
        reference_period="2024-02",
        reference_period_end_ns=BASE_NS + 60 * SECOND_NS,
        published_at_ns=PUB_2,
        value=309.0,
        digest="a" * 64,
    )
    bindings = tuple(
        _binding(release, artifact, str(release.actual_value))
        for chain in (prior_chain, current_chain)
        for release in chain.releases
    )
    corpus = _corpus(
        registry,
        (prior_chain, current_chain),
        (artifact,),
        bindings,
        (_era(artifact),),
    )
    assert audit_economic_archive_vintages(corpus).passed

    bad_mutation = replace(
        simultaneous,
        triggering_logical_event_key="us.cpi.absent",
        mutation_id="",
    )
    bad_chain = build_economic_release_vintage_chain(
        prior,
        initial_time_evidence=prior_evidence,
        mutations=(bad_mutation,),
        limitations=("Invalid trigger fixture.",),
    )
    bad_bindings = tuple(
        _binding(release, artifact, str(release.actual_value))
        for chain in (bad_chain, current_chain)
        for release in chain.releases
    )
    bad_corpus = _corpus(
        registry,
        (bad_chain, current_chain),
        (artifact,),
        bad_bindings,
        (_era(artifact),),
    )
    bad_audit = audit_economic_archive_vintages(bad_corpus)
    assert not bad_audit.passed
    assert bad_audit.simultaneous_revision_violations == ("us.cpi.2024-01",)


def test_coverage_modes_and_corpus_identity_fail_closed() -> None:
    registry = load_packaged_official_source_registry()
    declaration = conservative_official_vintage_coverage(registry)[0]
    with pytest.raises(ValueError, match="both qualified claims"):
        replace(
            declaration,
            recovery_mode=EconomicVintageRecoveryMode.API_VINTAGE_COMPLETE,
            declaration_id="",
        )
    with pytest.raises(ValueError, match="artifact evidence"):
        replace(
            declaration,
            recovery_mode=EconomicVintageRecoveryMode.SNAPSHOT_DEPENDENT,
            declaration_id="",
        )

    archive = _artifact(
        registry,
        b'{"release": 308.1}',
        PUB_1 + 20 * SECOND_NS,
        EconomicArchiveArtifactKind.CONTEMPORANEOUS_RELEASE,
    )
    chain = _direct_chain()
    corpus = _corpus(
        registry,
        (chain,),
        (archive,),
        (_binding(chain.releases[0], archive, "308.1"),),
        (_era(archive),),
    )
    payload = corpus.to_dict()
    payload["limitations"] = ["Tampered limitation."]
    with pytest.raises(ValueError, match="corpus_id"):
        EconomicArchiveVintageCorpusV1.from_dict(payload)
