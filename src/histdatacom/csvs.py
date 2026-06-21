"""Extract CSVs from zip archive."""

from __future__ import annotations

from typing import TYPE_CHECKING

from histdatacom import config
from histdatacom.activity_stages import (
    apply_stage_output_to_record,
    extract_csv_work_item,
)
from histdatacom.concurrency import ProcessPool, get_pool_cpu_count
from histdatacom.runtime_contracts import WorkItem

if TYPE_CHECKING:
    from histdatacom.records import Record, Records


class Csv:  # noqa:H601
    """Extract CSV documents from zip archives."""

    def extract_csvs(self) -> None:
        """Execute process pool with extract_csv."""
        pool = ProcessPool(
            self._extract_csv,
            config.ARGS,
            "Extracting",
            "CSVs...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]),
        )

        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE)

    def _extract_csv(
        self,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
    ) -> None:
        """Extract single csv file. Called by extract_csvs.

        # noqa: DAR402

        Args:
            record (Record): a record from the work queue.
            args (dict): from config.ARGS
            records_current (Records): config.CURRENT_QUEUE
            records_next (Records): config.NEXT_QUEUE
        """
        try:
            output = extract_csv_work_item(
                WorkItem.from_record(record),
                args=args,
            )
            apply_stage_output_to_record(output, record)
            if output.forward:
                records_next.put(record)
        finally:
            records_current.task_done()
