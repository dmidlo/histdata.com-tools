"""Tests for the central histdatacom exception taxonomy."""

from __future__ import annotations

from histdatacom.activity_stages import ArchiveDownloadError, UrlValidationError
from histdatacom.exceptions import (
    ErrorCategory,
    HistDataOperationError,
    InfluxConfigurationError,
    InfluxDependencyError,
    NetworkOperationError,
    RetryPolicyName,
    failure_info_from_exception,
    influx_failure_info,
    retry_policy_for_error,
)
from histdatacom.runtime_contracts import FailureInfo


def test_activity_stage_errors_are_central_operation_errors() -> None:
    """Legacy activity imports should resolve to central typed exceptions."""
    err = UrlValidationError(
        "URL_FETCH_RETRYABLE",
        "timeout",
        retryable=True,
        detail={"url": "https://example.test"},
    )

    assert isinstance(err, HistDataOperationError)
    assert err.category is ErrorCategory.NETWORK
    failure = err.to_failure_info()
    assert failure.code == "URL_FETCH_RETRYABLE"
    assert failure.retryable is True
    assert failure.detail["category"] == ErrorCategory.NETWORK.value


def test_archive_error_converts_to_failure_info() -> None:
    """Archive operation exceptions should preserve code and retryability."""
    err = ArchiveDownloadError(
        "ARCHIVE_NETWORK_ERROR",
        "temporary network failure",
        retryable=True,
        detail={"url": "https://example.test/archive"},
    )

    failure = failure_info_from_exception(err)

    assert failure.code == "ARCHIVE_NETWORK_ERROR"
    assert failure.message == "temporary network failure"
    assert failure.retryable is True
    assert failure.detail["category"] == ErrorCategory.ARCHIVE.value


def test_retry_policy_can_be_selected_from_exception_or_failure() -> None:
    """Workflow code should not need string parsing to select retry policy."""
    err = NetworkOperationError(
        "connect timeout",
        code="URL_FETCH_RETRYABLE",
    )
    failure = err.to_failure_info()

    assert retry_policy_for_error(err).name is RetryPolicyName.NETWORK
    assert retry_policy_for_error(failure).name is RetryPolicyName.NETWORK
    assert retry_policy_for_error(FailureInfo("CACHE_EMPTY", "empty")).name is (
        RetryPolicyName.NONE
    )


def test_influx_failures_have_explicit_result_codes() -> None:
    """Influx dependency and write failures should be distinguishable."""
    retryable = influx_failure_info(OSError("temporary write failure"))
    missing_dependency = influx_failure_info(
        InfluxDependencyError("InfluxDB import not installed.")
    )
    bad_config = influx_failure_info(
        InfluxConfigurationError("influxdb.yaml is missing required keys.")
    )

    assert retryable.code == "INFLUX_IMPORT_RETRYABLE"
    assert retryable.retryable is True
    assert retryable.detail["idempotent_retry"] is True
    assert retry_policy_for_error(retryable).name is (
        RetryPolicyName.IDEMPOTENT_WRITE
    )
    assert missing_dependency.code == "INFLUX_OPTIONAL_DEPENDENCY_MISSING"
    assert missing_dependency.retryable is False
    assert bad_config.code == "INFLUX_CONFIGURATION_ERROR"
    assert isinstance(InfluxConfigurationError("bad"), ValueError)


def test_system_exit_is_mapped_to_non_retryable_failure() -> None:
    """Core activity paths can convert legacy exits instead of propagating them."""
    failure = failure_info_from_exception(
        SystemExit("operator cancelled"),
        default_code="CACHE_BUILD_INTERRUPTED",
        detail={"stage": "build_cache"},
    )

    assert failure.code == "CACHE_BUILD_INTERRUPTED"
    assert failure.message == "operator cancelled"
    assert failure.retryable is False
    assert failure.detail["category"] == ErrorCategory.CANCELLATION.value
