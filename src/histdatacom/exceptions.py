"""Central exception taxonomy and retry mapping for histdatacom."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping

from histdatacom.runtime_contracts import FailureInfo, JSONValue


class ErrorCategory(str, Enum):
    """High-level operation failure categories used by workflows and CLIs."""

    ARCHIVE = "archive"
    CACHE = "cache"
    CANCELLATION = "cancellation"
    CONFIGURATION = "configuration"
    DEPENDENCY = "dependency"
    FILESYSTEM = "filesystem"
    INFLUX = "influx"
    NETWORK = "network"
    NO_DATA = "no_data"
    PARSE = "parse"
    REPOSITORY = "repository"
    UNKNOWN = "unknown"
    VALIDATION = "validation"


class RetryPolicyName(str, Enum):
    """Stable retry-policy names independent of Temporal being installed."""

    NONE = "none"
    STANDARD = "standard"
    NETWORK = "network"
    IDEMPOTENT_WRITE = "idempotent_write"


@dataclass(frozen=True, slots=True)
class ActivityRetryPolicy:
    """Temporal-compatible retry policy metadata for a failure class."""

    name: RetryPolicyName
    retryable: bool
    maximum_attempts: int
    initial_interval_seconds: float = 1.0
    backoff_coefficient: float = 2.0
    maximum_interval_seconds: float = 30.0
    non_retryable_error_types: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-safe policy representation for manifests/status."""
        return {
            "name": self.name.value,
            "retryable": self.retryable,
            "maximum_attempts": self.maximum_attempts,
            "initial_interval_seconds": self.initial_interval_seconds,
            "backoff_coefficient": self.backoff_coefficient,
            "maximum_interval_seconds": self.maximum_interval_seconds,
            "non_retryable_error_types": list(self.non_retryable_error_types),
        }


NO_RETRY_POLICY = ActivityRetryPolicy(
    name=RetryPolicyName.NONE,
    retryable=False,
    maximum_attempts=1,
    non_retryable_error_types=("HistDataOperationError",),
)
STANDARD_RETRY_POLICY = ActivityRetryPolicy(
    name=RetryPolicyName.STANDARD,
    retryable=True,
    maximum_attempts=3,
)
NETWORK_RETRY_POLICY = ActivityRetryPolicy(
    name=RetryPolicyName.NETWORK,
    retryable=True,
    maximum_attempts=5,
    initial_interval_seconds=2.0,
    maximum_interval_seconds=60.0,
)
IDEMPOTENT_WRITE_RETRY_POLICY = ActivityRetryPolicy(
    name=RetryPolicyName.IDEMPOTENT_WRITE,
    retryable=True,
    maximum_attempts=4,
    initial_interval_seconds=2.0,
    maximum_interval_seconds=45.0,
)


class HistDataOperationError(Exception):
    """Base class for structured operation failures."""

    category = ErrorCategory.UNKNOWN
    code = "HISTDATA_OPERATION_ERROR"
    retryable = False
    exit_code: int | None = None

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        retryable: bool | None = None,
        category: ErrorCategory | None = None,
        detail: Mapping[str, JSONValue] | None = None,
        exit_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code or self.code
        self.retryable = self.retryable if retryable is None else retryable
        self.category = category or self.category
        self.detail = dict(detail or {})
        self.exit_code = self.exit_code if exit_code is None else exit_code

    def to_failure_info(
        self,
        *,
        detail: Mapping[str, JSONValue] | None = None,
    ) -> FailureInfo:
        """Return workflow/activity failure metadata for this exception."""
        failure_detail = dict(self.detail)
        if detail:
            failure_detail.update(detail)
        failure_detail.setdefault("category", self.category.value)
        return FailureInfo(
            code=self.code,
            message=self.message,
            retryable=self.retryable,
            detail=failure_detail,
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-safe representation of this failure."""
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "category": self.category.value,
            "detail": dict(self.detail),
            "exit_code": self.exit_code,
        }


class CliValidationError(HistDataOperationError, ValueError):
    """CLI input validation failure that should exit at the CLI boundary."""

    category = ErrorCategory.VALIDATION
    code = "CLI_VALIDATION_ERROR"
    retryable = False
    exit_code = 1


class ConfigurationError(HistDataOperationError, ValueError):
    """Configuration is missing or malformed."""

    category = ErrorCategory.CONFIGURATION
    code = "CONFIGURATION_ERROR"
    retryable = False


class DependencyOperationError(HistDataOperationError, RuntimeError):
    """An optional runtime dependency is missing or unusable."""

    category = ErrorCategory.DEPENDENCY
    code = "DEPENDENCY_ERROR"
    retryable = False


class NetworkOperationError(HistDataOperationError):
    """A network operation failed and can usually be retried."""

    category = ErrorCategory.NETWORK
    code = "NETWORK_ERROR"
    retryable = True


class NoDataOperationError(HistDataOperationError):
    """The vendor has no data for the requested operation."""

    category = ErrorCategory.NO_DATA
    code = "NO_DATA"
    retryable = False


class ParseDataError(HistDataOperationError, ValueError):
    """Input data could not be parsed into the expected domain model."""

    category = ErrorCategory.PARSE
    code = "PARSE_ERROR"
    retryable = False


class FileSystemOperationError(HistDataOperationError, OSError):
    """A filesystem operation failed."""

    category = ErrorCategory.FILESYSTEM
    code = "FILESYSTEM_ERROR"
    retryable = False


class CancellationOperationError(HistDataOperationError):
    """An operation was cancelled or interrupted."""

    category = ErrorCategory.CANCELLATION
    code = "OPERATION_CANCELLED"
    retryable = False


class UrlValidationError(HistDataOperationError):
    """Structured URL validation failure."""

    category = ErrorCategory.PARSE
    code = "URL_VALIDATION_FAILED"
    retryable = False

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        no_data: bool = False,
        detail: Mapping[str, JSONValue] | None = None,
    ) -> None:
        category = ErrorCategory.NETWORK if retryable else ErrorCategory.PARSE
        super().__init__(
            message,
            code=code,
            retryable=retryable,
            category=category,
            detail=detail,
        )
        self.no_data = no_data


class HistDataNoDataError(UrlValidationError):
    """HistData returned a page without downloadable archive metadata."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "HISTDATA_NO_DATA",
        detail: Mapping[str, JSONValue] | None = None,
    ) -> None:
        super().__init__(
            code,
            message,
            retryable=False,
            no_data=True,
            detail=detail,
        )
        self.category = ErrorCategory.NO_DATA


class ArchiveError(HistDataOperationError):
    """Archive operation failure."""

    category = ErrorCategory.ARCHIVE
    code = "ARCHIVE_ERROR"
    retryable = False

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        detail: Mapping[str, JSONValue] | None = None,
    ) -> None:
        super().__init__(
            message,
            code=code,
            retryable=retryable,
            detail=detail,
        )


class ArchiveDownloadError(ArchiveError):
    """Structured archive download failure."""


class ArchiveExtractionError(ArchiveError):
    """Structured archive extraction failure."""


class CacheBuildError(HistDataOperationError):
    """Structured Polars cache build/validation failure."""

    category = ErrorCategory.CACHE
    code = "CACHE_BUILD_FAILED"
    retryable = False

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        detail: Mapping[str, JSONValue] | None = None,
    ) -> None:
        super().__init__(
            message,
            code=code,
            retryable=retryable,
            detail=detail,
        )


class RepositoryError(HistDataOperationError):
    """Repository metadata operation failure."""

    category = ErrorCategory.REPOSITORY
    code = "REPOSITORY_ERROR"
    retryable = False


class InfluxError(HistDataOperationError):
    """InfluxDB operation failure."""

    category = ErrorCategory.INFLUX
    code = "INFLUX_ERROR"
    retryable = False


class InfluxConfigurationError(InfluxError, ValueError):
    """InfluxDB configuration is missing or malformed."""

    category = ErrorCategory.CONFIGURATION
    code = "INFLUX_CONFIGURATION_ERROR"
    retryable = False


class InfluxDependencyError(InfluxError, RuntimeError):
    """InfluxDB optional dependency is missing."""

    category = ErrorCategory.DEPENDENCY
    code = "INFLUX_OPTIONAL_DEPENDENCY_MISSING"
    retryable = False


class InfluxImportError(InfluxError):
    """InfluxDB import/write operation failed."""

    code = "INFLUX_IMPORT_FAILED"


def failure_info_from_exception(
    err: BaseException,
    *,
    default_code: str = "OPERATION_FAILED",
    default_retryable: bool = False,
    detail: Mapping[str, JSONValue] | None = None,
) -> FailureInfo:
    """Return structured failure metadata for known and unknown exceptions."""
    if isinstance(err, HistDataOperationError):
        return err.to_failure_info(detail=detail)
    if isinstance(err, OSError):
        filesystem_failure = FileSystemOperationError(
            str(err),
            code=default_code,
            retryable=default_retryable,
            detail=detail,
        )
        return filesystem_failure.to_failure_info()
    if isinstance(err, SystemExit):
        cancellation_failure = CancellationOperationError(
            _system_exit_message(err),
            code=default_code,
            detail=detail,
        )
        return cancellation_failure.to_failure_info()
    return FailureInfo(
        code=default_code,
        message=str(err),
        retryable=default_retryable,
        detail=dict(detail or {}),
    )


def is_retryable_exception(err: BaseException) -> bool:
    """Return whether a failure should be retried by default."""
    if isinstance(err, HistDataOperationError):
        return err.retryable
    return False


def retry_policy_for_error(
    err: BaseException | FailureInfo | None = None,
    *,
    retryable: bool | None = None,
    category: ErrorCategory | None = None,
) -> ActivityRetryPolicy:
    """Select a retry policy from exception class, failure metadata, or flags."""
    if isinstance(err, HistDataOperationError):
        if err.retryable:
            return _policy_for_category(err.category)
        return NO_RETRY_POLICY
    if isinstance(err, FailureInfo):
        if not err.retryable:
            return NO_RETRY_POLICY
        return _policy_for_code(err.code)
    if category is not None:
        return _policy_for_category(category) if retryable else NO_RETRY_POLICY
    if retryable:
        return STANDARD_RETRY_POLICY
    return NO_RETRY_POLICY


def influx_failure_info(err: BaseException) -> FailureInfo:
    """Classify Influx failures consistently across legacy and activity paths."""
    message = (
        _system_exit_message(err) if isinstance(err, SystemExit) else str(err)
    )
    if isinstance(err, InfluxError):
        return err.to_failure_info()
    if isinstance(err, OSError):
        return InfluxImportError(
            message,
            code="INFLUX_IMPORT_RETRYABLE",
            retryable=True,
            detail={"idempotent_retry": True},
        ).to_failure_info()
    if _is_influx_dependency_error(err, message):
        return InfluxDependencyError(message).to_failure_info()
    if isinstance(err, SystemExit):
        return InfluxError(
            message,
            code="INFLUX_IMPORT_PRECONDITION_FAILED",
        ).to_failure_info()
    return InfluxImportError(message).to_failure_info()


def _policy_for_category(category: ErrorCategory) -> ActivityRetryPolicy:
    if category is ErrorCategory.NETWORK:
        return NETWORK_RETRY_POLICY
    if category is ErrorCategory.INFLUX:
        return IDEMPOTENT_WRITE_RETRY_POLICY
    return STANDARD_RETRY_POLICY


def _policy_for_code(code: str) -> ActivityRetryPolicy:
    if code.endswith("_RETRYABLE") or "NETWORK" in code:
        if code.startswith("INFLUX_"):
            return IDEMPOTENT_WRITE_RETRY_POLICY
        return NETWORK_RETRY_POLICY
    return STANDARD_RETRY_POLICY


def _is_influx_dependency_error(err: BaseException, message: str) -> bool:
    return (
        isinstance(err, (ModuleNotFoundError, ImportError))
        or "histdatacom[influx]" in message
        or "InfluxDB import not installed" in message
    )


def _system_exit_message(err: SystemExit) -> str:
    code = err.code
    if code is None:
        return ""
    return str(code)
