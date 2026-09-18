"""Tests for the Eurostat labour-cost refresh boundary."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import requests

from histdatacom.market_context import (
    EUROSTAT_LABOUR_COST_SOURCE_KEY,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    build_eurostat_labour_cost_dataset_request,
    load_packaged_official_source_registry,
)
from scripts import refresh_eurostat_labour_cost_archive as refresh


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class _Response:
    status_code = 200
    content = b"{}"

    def __init__(self, url: str) -> None:
        self.url = url
        self.headers = {"Content-Type": "application/json; charset=utf-8"}

    def raise_for_status(self) -> None:
        return None


def _product_request() -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_LABOUR_COST_SOURCE_KEY
    )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri="https://ec.europa.eu/eurostat/product?code=3-16092026-bp",
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def _document_request(
    source_format: OfficialSourceFormat,
) -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_LABOUR_COST_SOURCE_KEY
    )
    suffix = (
        "PDF.pdf" if source_format is OfficialSourceFormat.PDF else "HTML.html"
    )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=(
            "https://ec.europa.eu/eurostat/documents/2995521/5198130/"
            f"3-09012002-AP-EN.{suffix}/source"
        ),
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def test_refresh_prefers_equivalent_canonical_landing_uri() -> None:
    content = b"""<html><head>
    <link rel="canonical" href="https://ec.europa.eu/eurostat/web/products-euro-indicators/w/3-16092026-bp">
    <meta property="og:url" content="https://ec.europa.eu/eurostat/en/web/products-euro-indicators/w/3-16092026-bp">
    </head></html>"""

    assert refresh._resolved_uri(_product_request(), content) == (
        "https://ec.europa.eu/eurostat/web/products-euro-indicators/w/3-16092026-bp"
    )


def test_refresh_rejects_conflicting_landing_uri_metadata() -> None:
    content = b"""<html><head>
    <link rel="canonical" href="https://ec.europa.eu/eurostat/web/products-euro-indicators/w/3-16092026-bp">
    <meta property="og:url" content="https://ec.europa.eu/eurostat/en/web/products-euro-indicators/w/3-13082026-ap">
    </head></html>"""

    with pytest.raises(ValueError, match="URI metadata differs"):
        refresh._resolved_uri(_product_request(), content)


@pytest.mark.parametrize(
    ("source_format", "suffix"),
    (
        (OfficialSourceFormat.HTML, "html"),
        (OfficialSourceFormat.PDF, "pdf"),
    ),
)
def test_refresh_retains_release_documents_separately(
    tmp_path: Path,
    source_format: OfficialSourceFormat,
    suffix: str,
) -> None:
    request = _document_request(source_format)

    assert refresh._local_path(tmp_path, request) == (
        tmp_path / "documents" / f"3-09012002-ap.{suffix}"
    )
    assert refresh._resolved_uri(request, b"legacy bytes without metadata") == (
        request.uri
    )


@pytest.mark.parametrize("value", (12.999, float("nan"), float("inf"), 3_601.0))
def test_refresh_rejects_invalid_request_interval(value: float) -> None:
    with pytest.raises(ValueError, match="between 13 and 3600 seconds"):
        refresh._PoliteFetcher(value)


def test_refresh_spaces_retry_after_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = build_eurostat_labour_cost_dataset_request(
        load_packaged_official_source_registry()
    )
    clock = _Clock()
    starts: list[float] = []

    def get(uri: str, **kwargs: Any) -> _Response:
        starts.append(clock.now)
        assert kwargs["headers"]["User-Agent"] == refresh.USER_AGENT
        assert kwargs["timeout"] == (15, 180)
        if len(starts) == 1:
            raise requests.ConnectionError("test connection failure")
        return _Response(uri)

    monkeypatch.setattr(refresh.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(refresh.time, "sleep", clock.sleep)
    monkeypatch.setattr(refresh.requests, "get", get)

    result = refresh._PoliteFetcher(13.0).fetch(request)

    assert result == (b"{}", request.uri, "application/json")
    assert starts == [0.0, 13.0]
    assert clock.sleeps == [5.0, 8.0]
