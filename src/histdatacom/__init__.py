"""Entry point for histdatacom api.

histdatacom(options)

Returns:
    data: returns a data frame or a list of data frames and metadata
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from histdatacom.fx_enums import Format, Pairs, Timeframe
from histdatacom.options import Options

if TYPE_CHECKING:
    from pandas import DataFrame  # type: ignore
    from polars import DataFrame as PolarsDataFrame
    from pyarrow import Table

__all__ = [
    "CrossSeriesConstraintBundleV1",
    "CrossSeriesConstraintPolicyV1",
    "CrossSeriesConstraintUseV1",
    "CrossSeriesConstraintWindowV1",
    "DatasetCatalog",
    "DatasetQueryScopeV1",
    "DatasetResolutionV1",
    "DatasetVersionManifestV1",
    "FixtureProviderAdapter",
    "Format",
    "HistDataProviderAdapter",
    "Options",
    "Pairs",
    "ReconstructionCampaignDatasetPublicationV1",
    "ReconstructionCampaignProductEntryV1",
    "ReconstructionCampaignProductIndexV1",
    "ReconstructionCampaignProductShardV1",
    "ReconstructionClient",
    "ReconstructionExecutionRequestV1",
    "ReconstructionExitCode",
    "ReconstructionExperimentManifestV1",
    "ReconstructionExperimentRole",
    "ReconstructionExperimentSelectionV1",
    "ReconstructionExperimentVerificationV1",
    "ReconstructionOperationReceiptV1",
    "ReconstructionPlanSetPreflightV1",
    "ReconstructionPlanSetExecutionRequestV1",
    "ReconstructionPlanSetReceiptIndexV1",
    "ReconstructionPlanSetV1",
    "ReconstructionPlanShardV1",
    "ReconstructionPlanSpecV1",
    "ReconstructionPlanSpecV2",
    "ReconstructionPlanSupportMapIndexV2",
    "ReconstructionPlanSupportMapV1",
    "ReconstructionPlanSupportWindowV1",
    "ReconstructionPreflightV1",
    "Timeframe",
]


__version__ = "2.5.0"
__author__ = "David Midlo"

_RECONSTRUCTION_EXPORTS = frozenset(
    {
        "ReconstructionClient",
        "ReconstructionCampaignDatasetPublicationV1",
        "ReconstructionCampaignProductEntryV1",
        "ReconstructionCampaignProductIndexV1",
        "ReconstructionCampaignProductShardV1",
        "ReconstructionExecutionRequestV1",
        "ReconstructionExitCode",
        "ReconstructionOperationReceiptV1",
        "ReconstructionPlanSetPreflightV1",
        "ReconstructionPlanSetExecutionRequestV1",
        "ReconstructionPlanSetReceiptIndexV1",
        "ReconstructionPlanSetV1",
        "ReconstructionPlanShardV1",
        "ReconstructionPlanSpecV1",
        "ReconstructionPlanSpecV2",
        "ReconstructionPlanSupportMapIndexV2",
        "ReconstructionPlanSupportMapV1",
        "ReconstructionPlanSupportWindowV1",
        "ReconstructionPreflightV1",
    }
)

_EXPERIMENT_EXPORTS = frozenset(
    {
        "ReconstructionExperimentManifestV1",
        "ReconstructionExperimentRole",
        "ReconstructionExperimentSelectionV1",
        "ReconstructionExperimentVerificationV1",
    }
)

_DATASET_EXPORTS = frozenset(
    {
        "DatasetCatalog",
        "DatasetQueryScopeV1",
        "DatasetResolutionV1",
        "DatasetVersionManifestV1",
        "FixtureProviderAdapter",
        "HistDataProviderAdapter",
    }
)

_CROSS_SERIES_EXPORTS = frozenset(
    {
        "CrossSeriesConstraintBundleV1",
        "CrossSeriesConstraintPolicyV1",
        "CrossSeriesConstraintUseV1",
        "CrossSeriesConstraintWindowV1",
    }
)


def __getattr__(name: str) -> Any:
    """Lazily expose the typed reconstruction facade without import cycles."""
    if name not in (
        _RECONSTRUCTION_EXPORTS
        | _EXPERIMENT_EXPORTS
        | _DATASET_EXPORTS
        | _CROSS_SERIES_EXPORTS
    ):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    if name in _RECONSTRUCTION_EXPORTS:
        module_name = "histdatacom.reconstruction"
    elif name in _EXPERIMENT_EXPORTS:
        module_name = "histdatacom.reconstruction_experiment"
    elif name in _DATASET_EXPORTS:
        module_name = "histdatacom.datasets"
    else:
        module_name = "histdatacom.cross_series_constraints"
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


class APICaller(sys.modules[__name__].__class__):  # type: ignore # noqa:H601
    """APICaller. A Masquerade class.

    A class that extends sys.modules[__name__].__class__ (the histdatacom class)
    extends/overwrites with a __call__ method to allow the module to be callable.

    Returns:
        data: returns a data frame or a list of data frames and metadata
    """

    def __call__(  # noqa:BLK001
        self, options: Options
    ) -> list | PolarsDataFrame | DataFrame | Table:
        """Run histdatacom -h for help.

        Args:
            options (Options): a histdatacom Options object.

        Returns:
            "list" | "PolarsDataFrame" | "DataFrame" | "Table":
                - (list) if called with -A or -U
                - (PolarsDataFrame) if options.api_return_type = "polars"
                - (DataFrame) if options.api_return_type = "pandas"
                - (Table) if options.api_return_type = "arrow"
        """
        from . import histdata_com

        return histdata_com.main(options)


sys.modules[__name__].__class__ = APICaller
