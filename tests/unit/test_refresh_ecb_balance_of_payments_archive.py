"""Tests for the ECB balance-of-payments refresh boundary."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import requests

from histdatacom.market_context import (
    OfficialSourceFormat,
    build_ecb_balance_of_payments_foedb_chunk_requests,
    build_ecb_balance_of_payments_foedb_versions_request,
    build_ecb_balance_of_payments_index_requests,
    load_packaged_ecb_balance_of_payments_archive_manifest,
    load_packaged_official_source_registry,
)
from scripts import refresh_ecb_balance_of_payments_archive as refresh


class _Response:
    def __init__(
        self,
        url: str,
        *,
        status_code: int = 200,
        content: bytes = b"{}",
        content_type: str = "application/json; charset=utf-8",
    ) -> None:
        self.url = url
        self.status_code = status_code
        self.content = content
        self.headers = {"Content-Type": content_type}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


def test_refresh_maps_official_uris_to_stable_corpus_paths(
    tmp_path: Path,
) -> None:
    registry = load_packaged_official_source_registry()
    index = (
        load_packaged_ecb_balance_of_payments_archive_manifest().release_index
    )
    versions = build_ecb_balance_of_payments_foedb_versions_request(registry)
    chunks = build_ecb_balance_of_payments_foedb_chunk_requests(
        registry,
        index.database_version,
        index.database_version_hash,
        total_records=index.database_total_records,
        chunk_size=index.database_chunk_size,
        chunk_group_size=index.database_chunk_group_size,
    )
    current, legacy = build_ecb_balance_of_payments_index_requests(registry)

    assert refresh._local_path(tmp_path, versions) == (
        tmp_path
        / "www.ecb.europa.eu"
        / "foedb/dbs/foedb/publications.en/versions.json"
    )
    assert refresh._local_path(tmp_path, chunks[-1]).relative_to(
        tmp_path
    ) == Path(
        "www.ecb.europa.eu/foedb/dbs/foedb/publications.en/"
        "1789724583/gA8XCxPS/data/0/chunk_80.json"
    )
    assert refresh._local_path(tmp_path, current).name == "index_bop.en.html"
    assert refresh._local_path(tmp_path, legacy).name == (
        "previous_releases.en.html"
    )


def test_refresh_rejects_traversal_in_corpus_path(tmp_path: Path) -> None:
    request = build_ecb_balance_of_payments_foedb_versions_request(
        load_packaged_official_source_registry()
    )

    with pytest.raises(ValueError, match="unsafe ECB corpus path"):
        refresh._local_path(
            tmp_path,
            replace(request, uri="https://www.ecb.europa.eu/a/../secret.json"),
        )


@pytest.mark.parametrize(  # type: ignore[untyped-decorator]
    ("source_format", "reported", "expected"),
    (
        (
            OfficialSourceFormat.JSON,
            "application/json; charset=utf-8",
            "application/json",
        ),
        (OfficialSourceFormat.HTML, "text/html; charset=utf-8", "text/html"),
    ),
)
def test_refresh_snapshot_uses_registered_media_type(
    source_format: OfficialSourceFormat,
    reported: str,
    expected: str,
) -> None:
    registry = load_packaged_official_source_registry()
    request = build_ecb_balance_of_payments_foedb_versions_request(registry)
    if source_format is OfficialSourceFormat.HTML:
        request = build_ecb_balance_of_payments_index_requests(registry)[0]

    snapshot = refresh._snapshot(
        request,
        (
            b"{}"
            if source_format is OfficialSourceFormat.JSON
            else b"<html></html>"
        ),
        request.uri,
        reported,
        1,
    )

    assert snapshot.content_type == expected
    assert snapshot.response_headers == {"content-type": expected}
    assert snapshot.retrieved_at_ns == snapshot.completed_at_ns == 1


def test_refresh_snapshot_rejects_redirect_and_media_type_drift() -> None:
    request = build_ecb_balance_of_payments_foedb_versions_request(
        load_packaged_official_source_registry()
    )

    with pytest.raises(ValueError, match="resolved to another host"):
        refresh._snapshot(
            request,
            b"{}",
            "https://example.com/versions.json",
            "application/json",
            1,
        )
    with pytest.raises(ValueError, match="content type differs"):
        refresh._snapshot(
            request,
            b"{}",
            request.uri,
            "text/plain",
            1,
        )


def test_refresh_reads_retained_bytes_without_network(tmp_path: Path) -> None:
    retained = tmp_path / "versions.json"
    retained.write_bytes(b'{"version":"retained"}')

    result = refresh._read_or_fetch(
        retained,
        "https://www.ecb.europa.eu/versions.json",
        fetch_missing=False,
    )

    assert result == (
        b'{"version":"retained"}',
        "https://www.ecb.europa.eu/versions.json",
        None,
    )


def test_refresh_requires_explicit_permission_to_fetch_missing_bytes(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing.json"

    with pytest.raises(FileNotFoundError, match="missing.json"):
        refresh._read_or_fetch(
            missing,
            "https://www.ecb.europa.eu/missing.json",
            fetch_missing=False,
        )


def test_refresh_retries_transient_status_with_identifying_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uri = "https://www.ecb.europa.eu/example.json"
    responses = iter(
        (
            _Response(uri, status_code=503),
            _Response(uri, content=b'{"ok":true}'),
        )
    )
    calls: list[dict[str, Any]] = []
    sleeps: list[int] = []

    def get(value: str, **kwargs: Any) -> _Response:
        calls.append({"uri": value, **kwargs})
        return next(responses)

    monkeypatch.setattr(refresh.requests, "get", get)
    monkeypatch.setattr(refresh.time, "sleep", sleeps.append)

    assert refresh._fetch(uri) == (
        b'{"ok":true}',
        uri,
        "application/json; charset=utf-8",
    )
    assert len(calls) == 2
    assert all(
        call["headers"]["User-Agent"] == refresh.USER_AGENT for call in calls
    )
    assert all(call["timeout"] == (15, 180) for call in calls)
    assert sleeps == [3]
