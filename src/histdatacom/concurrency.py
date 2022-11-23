# pylint: disable=redefined-outer-name
from __future__ import annotations
from typing import Callable
from typing import Optional
from typing import Type
from typing import TYPE_CHECKING

from math import ceil

from multiprocessing import managers
from multiprocessing import cpu_count
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from concurrent.futures import Future

from pyarrow import Table
from datatable import Frame
from pandas import DataFrame

from rich.progress import Progress
from rich.progress import TextColumn
from rich.progress import BarColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TaskID

from histdatacom import config
from histdatacom.records import Records

if TYPE_CHECKING:
    from histdatacom.options import Options
    from histdatacom.histdata_com import _HistDataCom


def init_counters(
    records_current_: Records,
    records_next_: Records,
    args_: dict,
    influx_chunks_queue_: Queue | None = None,
) -> None:
    # pylint: disable=global-variable-undefined

    global RECORDS_CURRENT
    RECORDS_CURRENT = records_current_  # type: ignore
    global RECORDS_NEXT
    RECORDS_NEXT = records_next_  # type: ignore
    global ARGS
    ARGS = args_  # type: ignore

    if influx_chunks_queue_ is not None:
        global INFLUX_CHUNKS_QUEUE  # pylint: disable=global-variable-undefined
        INFLUX_CHUNKS_QUEUE = influx_chunks_queue_  # type: ignore


def complete_future(
    progress: Progress, task_id: TaskID, futures: list, future: Future
) -> None:
    progress.advance(task_id, 0.75)
    futures.remove(future)
    del future


class ThreadPool:
    def __init__(
        self,
        exec_func: Callable,
        args: dict,
        progress_pre_text: str,
        progress_post_text: str,
        cpu_count: int,
    ) -> None:

        self.exec_func = exec_func
        self.args = args
        self.progress_pre_text = progress_pre_text
        self.progress_post_text = progress_post_text
        self.cpu_count = cpu_count

    def __call__(
        self, records_current: Optional[Records], records_next: Optional[Records]
    ) -> None:

        records_count = records_current.qsize()  # type: ignore
        with Progress(
            TextColumn(
                text_format=f"[cyan]{self.progress_pre_text} {records_count} {self.progress_post_text}."
            ),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
        ) as progress:
            task_id = progress.add_task("Validating URLs", total=records_count)

            with ThreadPoolExecutor(
                max_workers=self.cpu_count,
                initializer=init_counters,
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
                    complete_future(progress, task_id, futures, future)

        records_current.join()  # type: ignore
        records_next.dump_to_queue(records_current)  # type: ignore


class ProcessPool:
    def __init__(
        self,
        exec_func: Callable,
        args: dict,
        progress_pre_text: str,
        progress_post_text: str,
        cpu_count: int,
        join: bool = True,
        dump: bool = True,
    ) -> None:

        self.exec_func = exec_func
        self.args = args
        self.progress_pre_text = progress_pre_text
        self.progress_post_text = progress_post_text
        self.cpu_count = cpu_count
        self.join = join
        self.dump = dump

    def __call__(
        self,
        records_current: Optional[Records],
        records_next: Optional[Records],
        influx_chunks_queue: Optional[Queue] = None,
    ) -> None:

        records_count = records_current.qsize()  # type: ignore
        with Progress(
            TextColumn(
                text_format=f"[cyan]{self.progress_pre_text} {records_count} {self.progress_post_text}."
            ),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
        ) as progress:
            task_id = progress.add_task("Validating URLs", total=records_count)

            with ProcessPoolExecutor(
                max_workers=self.cpu_count,
                initializer=init_counters,
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
                    complete_future(progress, task_id, futures, future)

        if self.join:
            records_current.join()  # type: ignore

        if self.dump:
            records_next.dump_to_queue(records_current)  # type: ignore


def get_pool_cpu_count(count: str | int | None = None) -> int:

    try:
        real_vcpu_count = cpu_count()

        if count is None:
            count = real_vcpu_count
        else:
            err_text_cpu_level_err = f"""
                    ERROR on -c {count}  ERROR
                        * Malformed command:
                            - -c cpu must be str: low, medium, or high. or integer percent 1-200
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

        return count - 1 if count > 2 else ceil(count / 2)
    except ValueError as err:
        print(err)
        raise SystemExit from err


class QueueManager:
    def __init__(self, options: Options):
        self.options = options
        config.QUEUE_MANAGER = managers.SyncManager()
        config.QUEUE_MANAGER.register("Records", Records)  # pylint: disable=no-member

    # pylint: disable-next=inconsistent-return-statements
    def __call__(  # type: ignore
        self, runner_: Type[_HistDataCom]
    ) -> list | dict | Frame | DataFrame | Table:
        config.QUEUE_MANAGER.start()  # type: ignore

        config.CURRENT_QUEUE = config.QUEUE_MANAGER.Records()  # type: ignore # pylint: disable=no-member
        config.NEXT_QUEUE = config.QUEUE_MANAGER.Records()  # type: ignore # pylint: disable=no-member
        config.INFLUX_CHUNKS_QUEUE = config.QUEUE_MANAGER.Queue()  # type: ignore

        histdatacom_runner = runner_(self.options)

        if self.options.from_api:
            return histdatacom_runner.run()
        else:
            histdatacom_runner.run()
