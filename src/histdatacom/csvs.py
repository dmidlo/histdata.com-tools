"""Extract CSVs from zip archive."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Iterable

from histdatacom.activity_stages import (
    apply_stage_output_to_record,
    extract_csv_work_item,
)
from histdatacom.legacy_runtime import helper_runtime_args
from histdatacom.runtime_contracts import WorkItem

if TYPE_CHECKING:
    from histdatacom.records import Record


class Csv:  # noqa:H601
    """Extract CSV documents from zip archives."""

    def __init__(self, args: Mapping[str, Any] | None = None) -> None:
        """Initialize the helper with explicit runtime arguments."""
        self.args: dict[str, Any] = helper_runtime_args(args)

    def extract_csvs(
        self,
        records: Iterable[Record],
        args: Mapping[str, Any] | None = None,
    ) -> list[Record]:
        """Extract CSVs and return forwarded records."""
        runtime_args = helper_runtime_args(self.args, args)
        return [
            output
            for record in records
            if (output := self._extract_csv(record, runtime_args)) is not None
        ]

    def _extract_csv(
        self,
        record: Record,
        args: Mapping[str, Any],
    ) -> Record | None:
        """Extract single csv file. Called by extract_csvs.

        # noqa: DAR402

        Args:
            record (Record): a record to extract.
            args (Mapping[str, Any]): explicit runtime arguments
        """
        output = extract_csv_work_item(
            WorkItem.from_record(record),
            args=args,
        )
        apply_stage_output_to_record(output, record)
        return record if output.forward else None
