"""Pytest unit tests for histdatacom.histdatacom__init__.py."""

import sys

import pytest

import histdatacom
from histdatacom import Options


def test_histdatacom__init__() -> None:
    """Test pytest path resolution."""
    assert True  # sourcery skip # act


def test_exposed_classes() -> None:
    """Expose only what is necessary for end-user."""
    assert set(histdatacom.__all__) == {  # act
        "Options",
        "Pairs",
        "Timeframe",
        "Format",
        "DatasetCatalog",
        "DatasetQueryScopeV1",
        "DatasetResolutionV1",
        "DatasetVersionManifestV1",
        "FixtureProviderAdapter",
        "HistDataProviderAdapter",
        "CrossSeriesConstraintBundleV1",
        "CrossSeriesConstraintPolicyV1",
        "CrossSeriesConstraintUseV1",
        "CrossSeriesConstraintWindowV1",
        "ReconstructionClient",
        "ReconstructionCampaignDatasetPublicationV1",
        "ReconstructionCampaignProductEntryV1",
        "ReconstructionCampaignProductIndexV1",
        "ReconstructionCampaignProductShardV1",
        "ReconstructionExecutionRequestV1",
        "ReconstructionExitCode",
        "ReconstructionExperimentManifestV1",
        "ReconstructionExperimentRole",
        "ReconstructionExperimentSelectionV1",
        "ReconstructionExperimentVerificationV1",
        "ReconstructionMathVerificationReportV1",
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
        "ReconstructionScientificLedgerV1",
        "current_histdata_reconstruction_scientific_ledger",
        "current_reconstruction_math_verification_report",
        "read_reconstruction_math_verification_report",
        "read_reconstruction_scientific_ledger",
    }


def test_masquerade_class() -> None:
    """Make histdatacom callable by extending itself with a call method."""
    assert sys.modules["histdatacom"].__class__.__name__ == "APICaller"  # act


def test_histdatacom_is_callable() -> None:
    """Ensure histdatacom's masquerade class adds __call__."""
    assert callable(histdatacom)  # act


def test_histdatacom_call() -> None:
    # pylint: disable=not-callable
    """Call histdatacom with minimal arguments."""
    options = Options()
    options.version = True

    assert histdatacom(options)  # type: ignore  # act


def test_histdatacom_version_allows_polars_return_type() -> None:
    # pylint: disable=not-callable
    """Validate Polars return requests without importing the legacy backend."""
    options = Options()
    options.version = True
    options.api_return_type = "polars"

    assert histdatacom(options) == histdatacom.__version__  # type: ignore


def test_histdatacom_rejects_unsupported_api_return_type() -> None:
    # pylint: disable=not-callable
    """Reject unsupported return types before module availability checks."""
    options = Options()
    options.version = True
    options.api_return_type = "numpy"

    with pytest.raises(ValueError, match="unsupported api_return_type 'numpy'"):
        histdatacom(options)  # type: ignore
