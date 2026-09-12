"""Tests for repository CLI rendering helpers."""

from __future__ import annotations

from typing import Any

from rich.table import Table

from histdatacom.repository_output import print_repository_table


def _captured_table(monkeypatch: Any, repo: dict[str, Any]) -> Table:
    rendered: list[Table] = []
    monkeypatch.setattr(
        "histdatacom.repository_output.print",
        lambda table: rendered.append(table),
    )

    print_repository_table(repo, include_quality=True)

    assert len(rendered) == 1
    return rendered[0]


def test_repository_table_excludes_integrity_metadata(monkeypatch: Any) -> None:
    """Hash metadata should not be interpreted as pair range rows."""
    table = _captured_table(
        monkeypatch,
        {
            "eurusd": {
                "start": "200005",
                "end": "202606",
                "quality": {
                    "status": "clean",
                    "target_count": 4,
                    "finding_count": 0,
                },
            },
            "hash": "digest",
            "hash_utc": 1.0,
        },
    )

    assert table.row_count == 1
    assert [column._cells for column in table.columns] == [  # noqa:SLF001
        ["eurusd"],
        ["2000-05"],
        ["2026-06"],
        ["clean"],
        ["4"],
        [""],
    ]


def test_repository_table_excludes_malformed_non_pair_entries(
    monkeypatch: Any,
) -> None:
    """The display path should use the existing valid-pair shape contract."""
    table = _captured_table(
        monkeypatch,
        {
            "eurusd": {"start": "200005", "end": "202606"},
            "metadata": {"schema_version": "example.v1"},
            "malformed": 42,
        },
    )

    assert table.row_count == 1
    assert table.columns[0]._cells == ["eurusd"]  # noqa:SLF001
