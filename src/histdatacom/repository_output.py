"""Repository CLI rendering helpers shared across runtimes."""

from __future__ import annotations

from typing import Any

from rich import box, print
from rich.table import Table

from histdatacom.repository_quality import repository_quality_columns
from histdatacom.utils import get_month_from_datemonth, get_year_from_datemonth


def print_repository_table(
    repo_data: dict[str, Any],
    *,
    include_quality: bool = False,
) -> None:
    """Render repository metadata using the legacy CLI table contract."""
    table = Table(
        title="Data and date ranges available from HistData.com",
        box=box.MARKDOWN,
    )
    table.add_column("Pair -p")
    table.add_column("Start -s")
    table.add_column("End -e")
    if include_quality:
        table.add_column("Quality")
        table.add_column("Q Targets")
        table.add_column("Q Findings")

    for row, value in repo_data.items():
        start = str(value["start"])
        end = str(value["end"])
        cells = [
            row.lower(),
            f"{get_year_from_datemonth(start)}-{get_month_from_datemonth(start)}",
            f"{get_year_from_datemonth(end)}-{get_month_from_datemonth(end)}",
        ]
        if include_quality:
            quality = repository_quality_columns(value)
            cells.extend(
                [
                    quality["status"],
                    quality["targets"],
                    quality["findings"],
                ]
            )
        table.add_row(*cells)
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
