"""Repository CLI rendering helpers shared across runtimes."""

from __future__ import annotations

from typing import Any

from rich import box, print
from rich.table import Table

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
