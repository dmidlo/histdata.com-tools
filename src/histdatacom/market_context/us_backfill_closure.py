"""Aggregate closure receipt for the packaged United States backfill.

Individual archive manifests retain their own evidence-capture dates.  The
aggregate receipt uses the earliest of those dates as its conservative common
window end, then applies profile-level forecast-gap semantics before running
the fail-closed 22-program audit.
"""

from __future__ import annotations

from dataclasses import replace

from histdatacom.market_context.official_sources import (
    load_packaged_official_source_registry,
)
from histdatacom.market_context.us_backfill import (
    UnitedStatesBackfillAuditV1,
    UnitedStatesBackfillProfileV1,
    UnitedStatesCoverageGapReason,
    UnitedStatesForecastStrategy,
    UnitedStatesProgramCoverageV1,
    audit_united_states_backfill,
    built_in_united_states_backfill_profile,
)
from histdatacom.market_context.us_cpi_archive import (
    bls_cpi_coverage_from_manifest,
    load_packaged_bls_cpi_archive_manifest,
)
from histdatacom.market_context.us_durable_goods_archive import (
    census_durable_coverage_from_manifest,
    load_packaged_census_durable_archive_manifest,
)
from histdatacom.market_context.us_employment_situation_archive import (
    bls_employment_situation_coverage_from_manifest,
    load_packaged_bls_employment_situation_archive_manifest,
)
from histdatacom.market_context.us_fomc_archive import (
    federal_reserve_fomc_coverages_from_manifest,
    load_packaged_federal_reserve_fomc_archive_manifest,
)
from histdatacom.market_context.us_g17_archive import (
    federal_reserve_g17_coverage_from_manifest,
    load_packaged_federal_reserve_g17_archive_manifest,
)
from histdatacom.market_context.us_gdp_archive import (
    bea_gdp_coverage_from_manifest,
    load_packaged_bea_gdp_archive_manifest,
)
from histdatacom.market_context.us_h6_archive import (
    federal_reserve_h6_coverage_from_manifest,
    load_packaged_federal_reserve_h6_archive_manifest,
)
from histdatacom.market_context.us_housing_construction_archive import (
    census_housing_coverage_from_manifest,
    load_packaged_census_housing_archive_manifest,
)
from histdatacom.market_context.us_initial_claims_archive import (
    dol_initial_claims_coverage_from_manifest,
    load_packaged_dol_initial_claims_archive_manifest,
)
from histdatacom.market_context.us_international_trade_archive import (
    census_international_trade_coverage_from_manifest,
    load_packaged_census_international_trade_archive_manifest,
)
from histdatacom.market_context.us_jolts_archive import (
    bls_jolts_coverage_from_manifest,
    load_packaged_bls_jolts_archive_manifest,
)
from histdatacom.market_context.us_mbos_archive import (
    load_packaged_philadelphia_fed_mbos_archive_manifest,
    philadelphia_fed_mbos_coverage_from_manifest,
)
from histdatacom.market_context.us_mtis_archive import (
    census_mtis_coverage_from_manifest,
    load_packaged_census_mtis_archive_manifest,
)
from histdatacom.market_context.us_mts_archive import (
    load_packaged_treasury_mts_archive_manifest,
    treasury_mts_coverage_from_manifest,
)
from histdatacom.market_context.us_new_home_sales_archive import (
    census_new_home_sales_coverage_from_manifest,
    load_packaged_census_new_home_sales_archive_manifest,
)
from histdatacom.market_context.us_personal_income_outlays_archive import (
    bea_pio_coverage_from_manifest,
    load_packaged_bea_pio_archive_manifest,
)
from histdatacom.market_context.us_ppi_archive import (
    bls_ppi_coverage_from_manifest,
    load_packaged_bls_ppi_archive_manifest,
)
from histdatacom.market_context.us_productivity_costs_archive import (
    bls_productivity_costs_coverage_from_manifest,
    load_packaged_bls_productivity_costs_archive_manifest,
)
from histdatacom.market_context.us_retail_sales_archive import (
    census_retail_coverage_from_manifest,
    load_packaged_census_retail_archive_manifest,
)
from histdatacom.market_context.us_spf_archive import (
    load_packaged_philadelphia_fed_spf_archive_manifest,
    philadelphia_fed_spf_coverage_from_manifest,
)


def _closure_coverage(
    profile: UnitedStatesBackfillProfileV1,
    coverage: UnitedStatesProgramCoverageV1,
    *,
    window_end_date: str,
) -> UnitedStatesProgramCoverageV1:
    gaps = set(coverage.gap_reasons)
    if (
        profile.by_key[coverage.program_key].forecast_strategy
        is UnitedStatesForecastStrategy.UNAVAILABLE
    ):
        gaps.add(UnitedStatesCoverageGapReason.NO_EVENT_FORECAST)
    return replace(
        coverage,
        window_end_date=window_end_date,
        gap_reasons=tuple(gaps),
        notes=(
            *coverage.notes,
            "The aggregate closure window ends on the earliest packaged manifest evidence date.",
        ),
        coverage_id="",
    )


def _load_packaged_closure_inputs() -> tuple[
    UnitedStatesBackfillProfileV1,
    tuple[UnitedStatesProgramCoverageV1, ...],
]:
    profile = built_in_united_states_backfill_profile(
        load_packaged_official_source_registry()
    )
    g17 = load_packaged_federal_reserve_g17_archive_manifest()
    cpi = load_packaged_bls_cpi_archive_manifest()
    ppi = load_packaged_bls_ppi_archive_manifest()
    employment = load_packaged_bls_employment_situation_archive_manifest()
    jolts = load_packaged_bls_jolts_archive_manifest()
    productivity = load_packaged_bls_productivity_costs_archive_manifest()
    gdp = load_packaged_bea_gdp_archive_manifest()
    pio = load_packaged_bea_pio_archive_manifest()
    retail = load_packaged_census_retail_archive_manifest()
    durable = load_packaged_census_durable_archive_manifest()
    trade = load_packaged_census_international_trade_archive_manifest()
    housing = load_packaged_census_housing_archive_manifest()
    new_homes = load_packaged_census_new_home_sales_archive_manifest()
    mtis = load_packaged_census_mtis_archive_manifest()
    mbos = load_packaged_philadelphia_fed_mbos_archive_manifest()
    spf = load_packaged_philadelphia_fed_spf_archive_manifest()
    fomc = load_packaged_federal_reserve_fomc_archive_manifest()
    h6 = load_packaged_federal_reserve_h6_archive_manifest()
    claims = load_packaged_dol_initial_claims_archive_manifest()
    mts = load_packaged_treasury_mts_archive_manifest()
    window_end_date = min(
        item.release_index.as_of_date
        for item in (
            g17,
            cpi,
            ppi,
            employment,
            jolts,
            productivity,
            gdp,
            pio,
            retail,
            durable,
            trade,
            housing,
            new_homes,
            mtis,
            mbos,
            spf,
            fomc,
            h6,
            claims,
            mts,
        )
    )
    raw = (
        federal_reserve_g17_coverage_from_manifest(
            profile, g17, window_end_date=window_end_date
        ),
        bls_cpi_coverage_from_manifest(
            profile, cpi, window_end_date=window_end_date
        ),
        bls_ppi_coverage_from_manifest(
            profile, ppi, window_end_date=window_end_date
        ),
        bls_employment_situation_coverage_from_manifest(
            profile, employment, window_end_date=window_end_date
        ),
        bls_jolts_coverage_from_manifest(
            profile, jolts, window_end_date=window_end_date
        ),
        bls_productivity_costs_coverage_from_manifest(
            profile, productivity, window_end_date=window_end_date
        ),
        bea_gdp_coverage_from_manifest(gdp),
        bea_pio_coverage_from_manifest(pio),
        census_retail_coverage_from_manifest(retail),
        census_durable_coverage_from_manifest(durable),
        census_international_trade_coverage_from_manifest(trade),
        census_housing_coverage_from_manifest(housing),
        census_new_home_sales_coverage_from_manifest(new_homes),
        census_mtis_coverage_from_manifest(mtis),
        philadelphia_fed_mbos_coverage_from_manifest(mbos),
        philadelphia_fed_spf_coverage_from_manifest(spf),
        *federal_reserve_fomc_coverages_from_manifest(fomc),
        federal_reserve_h6_coverage_from_manifest(profile, h6),
        dol_initial_claims_coverage_from_manifest(profile, claims),
        treasury_mts_coverage_from_manifest(profile, mts),
    )
    coverages = tuple(
        sorted(
            (
                _closure_coverage(
                    profile, item, window_end_date=window_end_date
                )
                for item in raw
            ),
            key=lambda item: item.program_key,
        )
    )
    return profile, coverages


def load_packaged_united_states_backfill_coverages() -> (
    tuple[UnitedStatesProgramCoverageV1, ...]
):
    """Load all packaged manifests as common-window closure slices."""
    return _load_packaged_closure_inputs()[1]


def load_packaged_united_states_backfill_audit() -> UnitedStatesBackfillAuditV1:
    """Recompute the fail-closed aggregate receipt from packaged manifests."""
    profile, coverages = _load_packaged_closure_inputs()
    return audit_united_states_backfill(profile, coverages)


__all__ = [
    "load_packaged_united_states_backfill_audit",
    "load_packaged_united_states_backfill_coverages",
]
