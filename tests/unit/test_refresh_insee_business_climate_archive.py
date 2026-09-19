"""Tests for the bounded INSEE business climate refresh boundary."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import requests

from histdatacom.market_context import (
    build_insee_business_climate_catalog_request,
    build_insee_business_climate_current_series_request,
    build_insee_business_climate_release_requests,
    build_insee_business_climate_search_requests,
    load_packaged_official_source_registry,
)
from scripts import refresh_insee_business_climate_archive as refresh


class _Response:
    def __init__(
        self,
        url: str,
        *,
        status_code: int = 200,
        content: bytes = b'{"ok":true}',
        content_type: str = "application/json; charset=utf-8",
        retry_after: str | None = None,
    ) -> None:
        self.url = url
        self.status_code = status_code
        self.content = content
        self.headers = {"Content-Type": content_type}
        if retry_after is not None:
            self.headers["Retry-After"] = retry_after

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


def test_refresh_maps_requests_to_stable_operator_corpus_paths(
    tmp_path: Path,
) -> None:
    registry = load_packaged_official_source_registry()
    legacy_search, current_search = (
        build_insee_business_climate_search_requests(registry)
    )
    release = build_insee_business_climate_release_requests(
        registry, ({"id": 9_038_023},)
    )[0]
    catalog = build_insee_business_climate_catalog_request(registry)
    series = build_insee_business_climate_current_series_request(registry)

    assert refresh._corpus_path(tmp_path, legacy_search, role="search") == (
        tmp_path / "search-legacy.json"
    )
    assert refresh._corpus_path(tmp_path, current_search, role="search") == (
        tmp_path / "search-current.json"
    )
    assert refresh._corpus_path(tmp_path, release, role="release") == (
        tmp_path / "releases/9038023.html"
    )
    assert refresh._corpus_path(tmp_path, catalog, role="catalog") == (
        tmp_path / "current/catalog.json"
    )
    assert refresh._corpus_path(tmp_path, series, role="series") == (
        tmp_path / "current/001565530.xml"
    )
    with pytest.raises(
        ValueError, match="unsupported INSEE business climate corpus role"
    ):
        refresh._corpus_path(tmp_path, legacy_search, role="unknown")


def test_refresh_reads_retained_bytes_without_network(tmp_path: Path) -> None:
    request = build_insee_business_climate_search_requests(
        load_packaged_official_source_registry()
    )[0]
    (tmp_path / "search-legacy.json").write_bytes(
        b'{"documents":[],"numFounds":0}'
    )

    snapshot = refresh._snapshot(
        tmp_path,
        request,
        role="search",
        fetch_missing=False,
    )

    assert snapshot.content == b'{"documents":[],"numFounds":0}'
    assert snapshot.request == request
    assert snapshot.content_type == "application/json"


def test_refresh_rejects_missing_or_mistyped_retained_artifacts(
    tmp_path: Path,
) -> None:
    request = build_insee_business_climate_search_requests(
        load_packaged_official_source_registry()
    )[0]

    with pytest.raises(
        FileNotFoundError,
        match="retained INSEE business climate artifact is missing",
    ):
        refresh._snapshot(
            tmp_path,
            request,
            role="search",
            fetch_missing=False,
        )

    (tmp_path / "search-legacy.json").write_bytes(b"<html></html>")
    with pytest.raises(
        ValueError, match="retained INSEE business climate JSON differs"
    ):
        refresh._snapshot(
            tmp_path,
            request,
            role="search",
            fetch_missing=False,
        )


def test_refresh_retries_transient_status_with_identifying_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = build_insee_business_climate_search_requests(
        load_packaged_official_source_registry()
    )[0]
    responses = iter(
        (
            _Response(request.uri, status_code=503, retry_after="2"),
            _Response(request.uri),
        )
    )
    calls: list[dict[str, Any]] = []
    sleeps: list[float] = []

    def send(method: str, uri: str, **kwargs: Any) -> _Response:
        calls.append({"method": method, "uri": uri, **kwargs})
        return next(responses)

    monkeypatch.setattr(refresh.requests, "request", send)
    monkeypatch.setattr(refresh.time, "sleep", sleeps.append)

    assert refresh._fetch(request) == b'{"ok":true}'
    assert len(calls) == 2
    assert all(
        call["headers"]["User-Agent"] == refresh.USER_AGENT for call in calls
    )
    assert all(call["timeout"] == (10.0, 60.0) for call in calls)
    assert all(call["method"] == "POST" for call in calls)
    assert sleeps == [2.0]


def test_refresh_rejects_redirect_and_media_type_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = build_insee_business_climate_search_requests(
        load_packaged_official_source_registry()
    )[0]
    responses = iter(
        (
            _Response("https://example.com/search"),
            _Response(request.uri, content_type="text/plain"),
        )
    )
    monkeypatch.setattr(
        refresh.requests,
        "request",
        lambda *_args, **_kwargs: next(responses),
    )

    with pytest.raises(ValueError, match="resolved outside its host"):
        refresh._fetch(request)
    with pytest.raises(ValueError, match="content type differs"):
        refresh._fetch(request)
