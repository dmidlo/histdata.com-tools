"""Provide multi-threading and multi-processes facilities.

Raises:
    ValueError: -c cpu must be str:
                    low, medium, or high. or integer percent 1-200
    SystemExit: exit on error
"""
# pylint: disable=redefined-outer-name
from __future__ import annotations

from concurrent.futures import (
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
from math import ceil
from multiprocessing import Queue, cpu_count, managers
from typing import TYPE_CHECKING, Callable, Optional, Type

from rich.progress import (
    BarColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)

from histdatacom import config
from histdatacom.records import Records

if TYPE_CHECKING:
    from datatable import Frame  # noqa:I900
    from pandas import DataFrame
    from pyarrow import Table

    from histdatacom.histdata_com import _HistDataCom
    from histdatacom.options import Options


class ThreadPool:
    """Standardize thread pool execution for histdatacom."""

    def __init__(
        self,
        exec_func: Callable,
        args: dict,
        progress_pre_text: str,
        progress_post_text: str,
        cpu_count: int,
    ) -> None:
        """Initialize attributes for thread pool.

        Args:
            exec_func (Callable): function to be executed by pool.
            args (dict): global args from config.ARGS
            progress_pre_text (str): display for rich.Progress
            progress_post_text (str): display for rich.Progress
            cpu_count (int): CPU count to use for pool.
        """
        self.exec_func = exec_func
        self.args = args
        self.progress_pre_text = progress_pre_text
        self.progress_post_text = progress_post_text
        self.cpu_count = cpu_count

    def __call__(
        self,
        records_current: Optional[Records],
        records_next: Optional[Records],
    ) -> None:
        """Execute Thread pool with rich.Progress bar.

        Args:
            records_current (Optional[Records]): from config.CURRENT_QUEUE
            records_next (Optional[Records]): from config.NEXT_QUEUE
        """
        records_count = records_current.qsize()  # type: ignore
        with Progress(
            TextColumn(
                text_format=(
                    f"[cyan]{self.progress_pre_text} "
                    f"{records_count} "
                    f"{self.progress_post_text}."
                )
            ),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
        ) as progress:
            task_id = progress.add_task("Validating URLs", total=records_count)

            with ThreadPoolExecutor(
                max_workers=self.cpu_count,
                initializer=_init_counters,
                initargs=(records_current, records_next, self.args.copy()),
            ) as executor:
                futures = []

                while not records_current.empty():  # type: ignore
                    record = records_current.get()  # type: ignore

                    if record is None:
                        return

                    future = executor.submit(self.exec_func, record, self.args)
                    progress.advance(task_id, 0.25)
                    futures.append(future)

                for future in as_completed(futures):
                    _complete_future(progress, task_id, futures, future)

        records_current.join()  # type: ignore
        records_next.dump_to_queue(records_current)  # type: ignore


class ProcessPool:
    """Standardize process pool execution for histdatacom."""

    def __init__(  # noqa:CFQ002
        self,
        exec_func: Callable,
        args: dict,
        progress_pre_text: str,
        progress_post_text: str,
        cpu_count: int,
        join: bool = True,
        dump: bool = True,
    ) -> None:
        """Initialize attributes for process pool.

        Args:
            exec_func (Callable): function to be executed by pool.
            args (dict): global args from config.ARGS
            progress_pre_text (str): display for rich.Progress
            progress_post_text (str): display for rich.Progress
            cpu_count (int): CPU count to use for pool.
            join (bool): disable join waits. Defaults to True.
            dump (bool): enable queue dumps. Defaults to True.
        """
        self.exec_func = exec_func
        self.args = args
        self.progress_pre_text = progress_pre_text
        self.progress_post_text = progress_post_text
        self.cpu_count = cpu_count
        self.join = join
        self.dump = dump

    def __call__(  # noqa:CCR001
        self,
        records_current: Optional[Records],
        records_next: Optional[Records],
        influx_chunks_queue: Optional[Queue] = None,
    ) -> None:
        """Execute Process pool with rich.Progress bar.

        Args:
            records_current (Optional[Records]): _description_
            records_next (Optional[Records]): _description_
            influx_chunks_queue (Optional[Queue], optional):
                                    used for RxPY queue. Defaults to None.
        """
        records_count = records_current.qsize()  # type: ignore
        with Progress(
            TextColumn(
                text_format=(
                    f"[cyan]{self.progress_pre_text} "
                    f"{records_count} "
                    f"{self.progress_post_text}."
                )
            ),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
        ) as progress:
            task_id = progress.add_task("Validating URLs", total=records_count)

            with ProcessPoolExecutor(
                max_workers=self.cpu_count,
                initializer=_init_counters,
                initargs=(
                    records_current,
                    records_next,
                    self.args.copy(),
                    influx_chunks_queue,
                ),
            ) as executor:
                futures = []

                while not records_current.empty():  # type: ignore
                    record = records_current.get()  # type: ignore

                    if record is None:
                        return

                    if influx_chunks_queue is None:
                        future = executor.submit(
                            self.exec_func,
                            record,
                            self.args,
                            records_current,
                            records_next,
                        )
                    else:
                        future = executor.submit(
                            self.exec_func,
                            record,
                            self.args,
                            records_current,
                            records_next,
                            influx_chunks_queue,
                        )

                    progress.advance(task_id, 0.25)
                    futures.append(future)

                for future in as_completed(futures):
                    _complete_future(progress, task_id, futures, future)

        if self.join:
            records_current.join()  # type: ignore

        if self.dump:
            records_next.dump_to_queue(records_current)  # type: ignore


def get_pool_cpu_count(count: str | int | None = None) -> int:  # noqa:CCR001
    """Set cpu_count.  Adjusted by -c config.ARGS["cpu_utilization"].

    Args:
        count (str | int | None, optional):
                Defaults to multiprocessing.cpu_count().

    # noqa: DAR402

    Raises:
        ValueError: malformed command, bad -c cpu_count
        SystemExit: exit on error.

    Returns:
        int: cpu_count for thread & process pool.
    """
    try:
        real_vcpu_count = cpu_count()

        if count is None:
            count = real_vcpu_count
        else:
            err_text_cpu_level_err = f"""
                    ERROR on -c {count}  ERROR
                        * Malformed command:
                            - -c cpu must be str:
                                low, medium, or high. or integer percent 1-200
            """
            count = str(count)
            match count:
                case "low":
                    count = ceil(real_vcpu_count / 2.5)
                case "medium":
                    count = ceil(real_vcpu_count / 1.5)
                case "high":
                    count = real_vcpu_count
                case _:
                    if count.isnumeric() and 1 <= int(count) <= 200:
                        count = ceil(real_vcpu_count * (int(count) / 100))
                    else:
                        raise ValueError(err_text_cpu_level_err)

        return count - 1 if count > 2 else ceil(count / 2)  # noqa:TC300
    except ValueError as err:
        raise SystemExit from err


class QueueManager:
    """Configure SyncManager with Queues and Callable: histdatacom."""

    # pylint: disable=no-member
    def __init__(self, options: Options):
        """Initialize SyncManager and register custom queue class 'Records'.

        Args:
            options (Options): a histdatacom.options Options object.
        """
        self.options = options
        config.QUEUE_MANAGER = managers.SyncManager()
        config.QUEUE_MANAGER.register(  # noqa:BLK100
            "Records", Records
        )  # pylint: disable=no-member

    # pylint: disable-next=inconsistent-return-statements
    def __call__(  # type: ignore
        self, runner_: Type[_HistDataCom]
    ) -> list | dict | Frame | DataFrame | Table:
        """Configure global queues and execute.

        Args:
            runner_ (Type[_HistDataCom]): _description_

        Returns:
            list | dict | Frame | DataFrame | Table: _description_
        """
        # pylint: disable-next=consider-using-with
        config.QUEUE_MANAGER.start()  # type: ignore

        config.CURRENT_QUEUE = config.QUEUE_MANAGER.Records()  # type: ignore
        config.NEXT_QUEUE = config.QUEUE_MANAGER.Records()  # type: ignore
        config.INFLUX_CHUNKS_QUEUE = config.QUEUE_MANAGER.Queue()  # type: ignore

        histdatacom_runner = runner_(self.options)

        if self.options.from_api:
            return histdatacom_runner.run()

        histdatacom_runner.run()  # noqa:R503


def _init_counters(
    # pylint: disable=unused-argument,unused-variable
    records_current_: Records,
    records_next_: Records,
    args_: dict,
    influx_chunks_queue_: Queue | None = None,
) -> None:
    """Initialize pool with access to these global variables.

    Args:
        records_current_ (Records): config.CURRENT_QUEUE
        records_next_ (Records): config.NEXT_QUEUE
        args_ (dict): config.ARGS
        influx_chunks_queue_ (Queue | None, optional): config.INFLUX_CHUNKS_QUEUE
    """
    # pylint: disable=global-variable-undefined
    args = args_  # noqa:F841

    if influx_chunks_queue_ is not None:
        global INFLUX_CHUNKS_QUEUE  # noqa:WPS100
        INFLUX_CHUNKS_QUEUE = influx_chunks_queue_  # type: ignore


def _complete_future(
    progress: Progress, task_id: TaskID, futures: list, future: Future
) -> None:
    """Finalize future and rich.Progress task.

    Args:
        progress (Progress): progress bar instance.
        task_id (TaskID): progress instance task id.
        futures (list): list of futures.
        future (Future): future from pool.
    """
    progress.advance(task_id, 0.75)
    futures.remove(future)
    del future  # noqa:WPS100
