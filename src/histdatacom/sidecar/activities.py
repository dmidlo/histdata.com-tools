"""Temporal activity functions for sidecar operation migration."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Mapping, TypeVar, cast

from histdatacom.activity_stages import (
    dataset_plan_stage,
    repository_refresh_stage,
)
from histdatacom.runtime_contracts import JSONValue, RunRequest
from histdatacom.utils import set_working_data_dir


class _NoopActivityApi:
    """No-op decorator shim used when temporalio is not installed."""

    def defn(self, decorated: Any | None = None, **kwargs: Any) -> Any:
        """Return a decorator compatible with temporalio.activity.defn."""

        def decorator(value: Any) -> Any:
            return value

        return decorator if decorated is None else decorated


def _load_activity_api() -> Any:
    try:
        return import_module("temporalio.activity")
    except ModuleNotFoundError as err:
        if (err.name or "").split(".")[0] == "temporalio":
            return _NoopActivityApi()
        raise


activity = _load_activity_api()
_Callable = TypeVar("_Callable", bound=Callable[..., Any])


def activity_defn(**kwargs: Any) -> Callable[[_Callable], _Callable]:
    """Apply Temporal's activity decorator with local typing."""
    raw_decorator = activity.defn(**kwargs)

    def decorator(decorated: _Callable) -> _Callable:
        return cast(_Callable, raw_decorator(decorated))

    return decorator


@activity_defn(name="repository_refresh")
def repository_refresh_activity(
    payload: dict[str, JSONValue],
) -> dict[str, Any]:
    """Run repository refresh/listing as a Temporal activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    repo_path = _repo_local_path(request)
    output = repository_refresh_stage(
        repo_data={},
        repo_file_exists=repo_path.exists(),
        repo_local_path=repo_path,
        pairs=request.pairs,
        by=str(request.metadata.get("repo_sort", "") or ""),
        available_remote_data=request.available_remote_data,
        update_remote_data=request.update_remote_data,
    )
    return cast(dict[str, Any], output.result.to_dict())


@activity_defn(name="dataset_plan")
def dataset_plan_activity(payload: dict[str, JSONValue]) -> dict[str, Any]:
    """Run deterministic URL and dataset planning as a Temporal activity."""
    request = RunRequest.from_dict(_mapping(payload.get("request", {})))
    output = dataset_plan_stage(
        start_yearmonth=request.start_yearmonth,
        end_yearmonth=request.end_yearmonth,
        formats=request.formats,
        pairs=request.pairs,
        timeframes=request.timeframes,
        default_download_dir=set_working_data_dir(request.data_directory),
        zip_persist=request.zip_persist,
    )
    return output.to_dict()


def default_activities() -> tuple[Callable[..., Any], ...]:
    """Return default sidecar activities for worker registration."""
    return (repository_refresh_activity, dataset_plan_activity)


def _repo_local_path(request: RunRequest) -> Path:
    data_dir = Path(set_working_data_dir(request.data_directory))
    return data_dir / ".repo"


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}
