"""Queue-free foreground runtime for local HistData runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Sequence

from rich import box, print
from rich.table import Table

from histdatacom import config
from histdatacom.activity_stages import (
    DEFAULT_REPOSITORY_URL,
    ActivityStageOutput,
    build_cache_work_item,
    dataset_plan_stage,
    download_archive_work_item,
    extract_csv_work_item,
    fetch_repository_data_from_url,
    import_to_influx_work_item,
    merge_cache_work_items,
    read_repository_data_file,
    repository_data_with_record,
    repository_missing_pairs,
    repository_refresh_stage,
    repository_should_create_or_update,
    repository_validation_needed,
    RepositoryStageOutput,
    sort_repository_data,
    validate_url_work_item,
    write_repository_data_file,
)
from histdatacom.influx import InfluxBatchWriter
from histdatacom.records import Record
from histdatacom.runtime_contracts import (
    RunRequest,
    StageResult,
    WorkItem,
    WorkStatus,
)
from histdatacom.utils import get_month_from_datemonth, get_year_from_datemonth


def print_repository_table(repo_data: dict[str, Any]) -> None:
    """Render repository metadata using the legacy CLI table contract."""
    table = Table(
        title="Data and date ranges available from HistData.com",
        box=box.MARKDOWN,
    )
    table.add_column("Pair -p")
    table.add_column("Start -s")
    table.add_column("End -e")

    for row, value in repo_data.items():
        start = str(value["start"])
        end = str(value["end"])
        table.add_row(
            row.lower(),
            f"{get_year_from_datemonth(start)}-{get_month_from_datemonth(start)}",
            f"{get_year_from_datemonth(end)}-{get_month_from_datemonth(end)}",
        )
    print(table)  # noqa:T201


def print_repository_failure(code: str) -> None:
    """Render the legacy repository failure message."""
    if code == "REPOSITORY_NETWORK_ERROR":
        print(r"""[red]Unable to fetch repo list from github.
                - You can manually update using `-U \[pair(s)]`""")  # noqa:T201
        return
    print("""[red]Unable to fetch repo list from github.
                        - Please install certifi package with:
                            pip install certifi`""")  # noqa:T201


class ForegroundRun:
    """Run HistData operations locally without manager-backed queues."""

    def __init__(self, request: RunRequest, args: dict[str, Any]) -> None:
        """Initialize one foreground run."""
        self.request = request
        self.args = dict(args)
        self.repo_url = DEFAULT_REPOSITORY_URL
        self.repo_local_path = Path(self.args["default_download_dir"], ".repo")
        self.stage_results: list[StageResult] = []

    def run(self) -> Any:
        """Run the configured request."""
        if (
            self.request.available_remote_data
            or self.request.update_remote_data
        ):
            repository_output = self._refresh_repository_metadata()
            return self._finish_repository_request(repository_output)

        config.FILTER_PAIRS = set(self.request.pairs)
        work_items = self._plan_dataset_work()
        if self.request.validate_urls:
            work_items = self._run_item_stage(
                work_items,
                validate_url_work_item,
            )
        if self.request.download_data_archives:
            work_items = self._run_item_stage(
                work_items,
                download_archive_work_item,
            )
        if self.request.api_return_type:
            cache_items = self._run_item_stage(
                work_items, build_cache_work_item
            )
            return self._merge_cache_items(cache_items)
        if self.request.extract_csvs:
            work_items = self._run_item_stage(work_items, extract_csv_work_item)
        if self.request.import_to_influxdb:
            self._import_to_influx(work_items)
        return None

    def _refresh_repository_metadata(self) -> RepositoryStageOutput:
        repo_file_exists = self.repo_local_path.exists()
        repo_data = (
            read_repository_data_file(self.repo_local_path)
            if repo_file_exists
            else {}
        )
        output = repository_refresh_stage(
            repo_data=repo_data,
            repo_file_exists=repo_file_exists,
            repo_local_path=self.repo_local_path,
            repo_url=self.repo_url,
            pairs=self.request.pairs,
            by=str(self.args.get("by", "") or ""),
            available_remote_data=self.request.available_remote_data,
            update_remote_data=self.request.update_remote_data,
            fetch_remote_repository=fetch_repository_data_from_url,
        )
        self.stage_results.append(output.result)
        config.REPO_DATA = output.repo_data
        config.REPO_DATA_FILE_EXISTS = output.repo_file_exists
        config.FILTER_PAIRS = self._filter_pairs(output.repo_data)
        return output

    def _finish_repository_request(
        self,
        repository_output: RepositoryStageOutput,
    ) -> dict[str, Any] | None:
        if repository_output.result.failure is not None:
            self._print_repository_failure(
                repository_output.result.failure.code
            )
            if self.args.get("from_api"):
                failure_available_data: dict[str, Any] = (
                    repository_output.available_data
                )
                return failure_available_data
            raise SystemExit(1)

        if self._repository_needs_validation(repository_output):
            self._validate_repository_coverage()
            if self._repository_should_write():
                write_repository_data_file(
                    config.REPO_DATA, self.repo_local_path
                )
                config.REPO_DATA = read_repository_data_file(
                    self.repo_local_path
                )
                config.REPO_DATA_FILE_EXISTS = True

        available_data = sort_repository_data(
            config.REPO_DATA,
            set(self.request.pairs),
            str(self.args.get("by", "") or ""),
        )
        if self.args.get("from_api"):
            api_available_data: dict[str, Any] = available_data
            return api_available_data

        self._print_repository_table(available_data)
        raise SystemExit(0)

    def _repository_needs_validation(
        self,
        repository_output: RepositoryStageOutput,
    ) -> bool:
        return bool(
            repository_validation_needed(
                self.args,
                repo_file_exists=repository_output.repo_file_exists,
                filter_pairs=repository_output.filter_pairs,
            )
        )

    def _repository_should_write(self) -> bool:
        return bool(
            repository_should_create_or_update(
                self.args,
                repo_file_exists=config.REPO_DATA_FILE_EXISTS,
                filter_pairs=config.FILTER_PAIRS,
            )
        )

    def _validate_repository_coverage(self) -> None:
        for output in self._validate_work_items(self._plan_dataset_work()):
            self.stage_results.append(output.result)
            if output.forward:
                config.REPO_DATA = repository_data_with_record(
                    config.REPO_DATA,
                    Record(**output.work_item.to_record_kwargs()),
                )

    def _plan_dataset_work(self) -> tuple[WorkItem, ...]:
        output = dataset_plan_stage(
            start_yearmonth=self.request.start_yearmonth,
            end_yearmonth=self.request.end_yearmonth,
            formats=self.request.formats,
            pairs=config.FILTER_PAIRS or self.request.pairs,
            timeframes=self.request.timeframes,
            default_download_dir=self.args["default_download_dir"],
            zip_persist=self.request.zip_persist,
        )
        self.stage_results.append(output.result)
        work_items: tuple[WorkItem, ...] = output.work_items
        return work_items

    def _validate_work_items(
        self,
        work_items: Sequence[WorkItem],
    ) -> tuple[ActivityStageOutput, ...]:
        return tuple(
            validate_url_work_item(work_item, args=self.args)
            for work_item in work_items
        )

    def _run_item_stage(
        self,
        work_items: Sequence[WorkItem],
        stage: Callable[..., ActivityStageOutput],
    ) -> tuple[WorkItem, ...]:
        forwarded: list[WorkItem] = []
        for output in (stage(item, args=self.args) for item in work_items):
            self.stage_results.append(output.result)
            if output.forward:
                forwarded.append(output.work_item)
        return tuple(forwarded)

    def _merge_cache_items(self, work_items: Sequence[WorkItem]) -> Any:
        output = merge_cache_work_items(
            work_items,
            return_type=self.request.api_return_type,
            materialize=True,
        )
        self.stage_results.append(output.result)
        return [] if output.result.status is WorkStatus.SKIPPED else output.data

    def _import_to_influx(self, work_items: Sequence[WorkItem]) -> None:
        with InfluxBatchWriter(self.args) as writer:
            for output in (
                import_to_influx_work_item(
                    item,
                    args=self.args,
                    emit_lines=writer.write_lines,
                )
                for item in work_items
            ):
                self.stage_results.append(output.result)

    def _filter_pairs(self, repo_data: dict[str, Any]) -> set[str] | None:
        missing = repository_missing_pairs(repo_data, set(self.request.pairs))
        return None if not missing else set(missing)

    def _print_repository_table(self, repo_data: dict[str, Any]) -> None:
        print_repository_table(repo_data)

    def _print_repository_failure(self, code: str) -> None:
        print_repository_failure(code)


def run_foreground(
    request: RunRequest,
    args: dict[str, Any],
) -> Any:
    """Run a foreground request through the queue-free runtime."""
    return ForegroundRun(request, args).run()
