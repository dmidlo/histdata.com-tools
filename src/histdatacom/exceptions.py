"""Central exception taxonomy and retry mapping for histdatacom."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
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


_SENSITIVE_DETAIL_KEYS = (
    "api_key",
    "apikey",
    "authorization",
    "password",
    "secret",
    "token",
)

_CATEGORY_ACTIONS = {
    ErrorCategory.ARCHIVE: (
        "Check the downloaded archive path and preserve the failing ZIP if "
        "you report this."
    ),
    ErrorCategory.CACHE: (
        "Rebuild the affected cache and include the source file, period, "
        "symbol, and cache path if the failure repeats."
    ),
    ErrorCategory.CANCELLATION: (
        "The operation was interrupted. Retry when you are ready to resume."
    ),
    ErrorCategory.CONFIGURATION: (
        "Check the command options, YAML config, and required environment "
        "values."
    ),
    ErrorCategory.DEPENDENCY: (
        "Install or repair the required runtime dependency, then rerun the "
        "command."
    ),
    ErrorCategory.FILESYSTEM: (
        "Check that the referenced path exists and that the process has "
        "permission and disk space."
    ),
    ErrorCategory.INFLUX: (
        "Check the InfluxDB service and influxdb.yaml values, especially org, "
        "bucket, URL, and token."
    ),
    ErrorCategory.NETWORK: (
        "Retry the command. If it keeps failing, include the URL, status code, "
        "and error code in the bug report."
    ),
    ErrorCategory.NO_DATA: (
        "Verify the requested symbol, format, timeframe, and period with "
        "-A/--available_remote_data."
    ),
    ErrorCategory.PARSE: (
        "The vendor response or local file did not match the expected HistData "
        "shape. Include the failing file or URL."
    ),
    ErrorCategory.REPOSITORY: (
        "Refresh repository metadata with -U and include the repository path "
        "if the problem repeats."
    ),
    ErrorCategory.UNKNOWN: (
        "Rerun with -vvv and include this report block with the traceback or "
        "GitHub issue."
    ),
    ErrorCategory.VALIDATION: (
        "Fix the invalid command input or configuration value and rerun."
    ),
}


@dataclass(frozen=True, slots=True)
class ReportableError:
    """End-user and bug-report friendly description of an operation failure."""

    code: str
    message: str
    category: ErrorCategory
    retryable: bool = False
    action: str = ""
    detail: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible, redacted report payload."""
        return {
            "code": self.code,
            "message": self.message,
            "category": self.category.value,
            "retryable": self.retryable,
            "action": self.action,
            "detail": _redact_report_detail(self.detail),
        }


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

    def to_reportable_error(self) -> ReportableError:
        """Return an end-user friendly error report for this failure."""
        detail = dict(self.detail)
        detail.setdefault("category", self.category.value)
        detail.setdefault("exception_type", self.__class__.__name__)
        return ReportableError(
            code=self.code,
            message=self.message,
            category=self.category,
            retryable=self.retryable,
            action=_action_for_category(self.category, detail),
            detail=_redact_report_detail(detail),
        )


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
    failure_detail = dict(detail or {})
    failure_detail.setdefault("category", ErrorCategory.UNKNOWN.value)
    failure_detail.setdefault("exception_type", err.__class__.__name__)
    return FailureInfo(
        code=default_code,
        message=str(err),
        retryable=default_retryable,
        detail=failure_detail,
    )


def reportable_error_from_exception(
    err: BaseException,
    *,
    default_code: str = "OPERATION_FAILED",
    default_retryable: bool = False,
    detail: Mapping[str, JSONValue] | None = None,
) -> ReportableError:
    """Return a bug-report friendly error object for any exception."""
    if isinstance(err, HistDataOperationError):
        if detail is None:
            return err.to_reportable_error()
        failure = err.to_failure_info(detail=detail)
    else:
        failure = failure_info_from_exception(
            err,
            default_code=default_code,
            default_retryable=default_retryable,
            detail=detail,
        )
    return reportable_error_from_failure_info(failure)


def reportable_error_from_failure_info(
    failure: FailureInfo,
) -> ReportableError:
    """Return product-grade failure reporting from workflow failure metadata."""
    detail = dict(failure.detail)
    category = _category_from_detail(detail)
    detail.setdefault("category", category.value)
    return ReportableError(
        code=failure.code or "OPERATION_FAILED",
        message=failure.message or "Operation failed without a message.",
        category=category,
        retryable=failure.retryable,
        action=_action_for_category(category, detail),
        detail=_redact_report_detail(detail),
    )


def format_reportable_error(
    error: ReportableError,
    *,
    title: str = "HistData operation failed",
    include_detail: bool = True,
) -> str:
    """Return a concise multiline CLI error report."""
    retryable = "yes" if error.retryable else "no"
    lines = [
        title,
        f"code: {error.code}",
        f"category: {error.category.value}",
        f"retryable: {retryable}",
        f"message: {error.message}",
        f"action: {error.action}",
    ]
    if include_detail and error.detail:
        lines.append("details:")
        for key in sorted(error.detail):
            lines.append(f"  {key}: {_format_report_detail(error.detail[key])}")
    return "\n".join(lines)


def format_exception_for_cli(
    err: BaseException,
    *,
    title: str = "HistData operation failed",
    default_code: str = "OPERATION_FAILED",
    default_retryable: bool = False,
    detail: Mapping[str, JSONValue] | None = None,
    include_detail: bool = True,
) -> str:
    """Return a CLI-ready report block for an exception."""
    return format_reportable_error(
        reportable_error_from_exception(
            err,
            default_code=default_code,
            default_retryable=default_retryable,
            detail=detail,
        ),
        title=title,
        include_detail=include_detail,
    )


def format_failure_info_for_cli(
    failure: FailureInfo,
    *,
    title: str = "HistData operation failed",
    include_detail: bool = True,
) -> str:
    """Return a CLI-ready report block for workflow failure metadata."""
    return format_reportable_error(
        reportable_error_from_failure_info(failure),
        title=title,
        include_detail=include_detail,
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


def _category_from_detail(detail: Mapping[str, JSONValue]) -> ErrorCategory:
    raw_category = detail.get("category")
    if isinstance(raw_category, str):
        try:
            return ErrorCategory(raw_category)
        except ValueError:
            return ErrorCategory.UNKNOWN
    return ErrorCategory.UNKNOWN


def _action_for_category(
    category: ErrorCategory,
    detail: Mapping[str, JSONValue],
) -> str:
    for key in ("operator_action", "action", "hint", "remediation"):
        value = detail.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return _CATEGORY_ACTIONS[category]


def _redact_report_detail(
    detail: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    return {
        str(key): _redact_report_value(str(key), value)
        for key, value in detail.items()
    }


def _redact_report_value(key: str, value: JSONValue) -> JSONValue:
    if _is_sensitive_detail_key(key):
        return "[redacted]"
    if isinstance(value, Mapping):
        return {
            str(child_key): _redact_report_value(str(child_key), child_value)
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [_redact_report_value(key, item) for item in value]
    return value


def _is_sensitive_detail_key(key: str) -> bool:
    normalized = key.casefold()
    return any(part in normalized for part in _SENSITIVE_DETAIL_KEYS)


def _format_report_detail(value: JSONValue) -> str:
    if isinstance(value, (dict, list)):
        text = json.dumps(value, sort_keys=True)
    else:
        text = str(value)
    if len(text) > 500:
        return text[:497] + "..."
    return text


def _system_exit_message(err: SystemExit) -> str:
    code = err.code
    if code is None:
        return ""
    return str(code)
