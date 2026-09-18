"""Tests for the Eurostat international-trade-goods refresh boundary."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import requests

from histdatacom.market_context import (
    EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRequestV1,
    build_eurostat_international_trade_goods_dataset_request,
    load_packaged_official_source_registry,
)
from scripts import (
    refresh_eurostat_international_trade_goods_archive as refresh,
)


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
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY
    )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri="https://ec.europa.eu/eurostat/product?code=6-15092026-ap",
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def _document_request(
    uri: str = (
        "https://ec.europa.eu/eurostat/documents/2995521/5147318/"
        "6-15062012-BP-EN.PDF.pdf/source"
    ),
) -> OfficialSourceRequestV1:
    source = load_packaged_official_source_registry().source(
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY
    )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=OfficialSourceFormat.PDF,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def test_refresh_prefers_equivalent_canonical_landing_uri() -> None:
    content = b"""<html><head>
    <link rel="canonical" href="https://ec.europa.eu/eurostat/web/products-euro-indicators/w/6-15092026-ap">
    <meta property="og:url" content="https://ec.europa.eu/eurostat/en/web/products-euro-indicators/w/6-15092026-ap">
    </head></html>"""

    assert refresh._resolved_uri(_product_request(), content) == (
        "https://ec.europa.eu/eurostat/web/products-euro-indicators/w/6-15092026-ap"
    )


def test_refresh_rejects_conflicting_landing_uri_metadata() -> None:
    content = b"""<html><head>
    <link rel="canonical" href="https://ec.europa.eu/eurostat/web/products-euro-indicators/w/6-15092026-ap">
    <meta property="og:url" content="https://ec.europa.eu/eurostat/en/web/products-euro-indicators/w/6-13082026-ap">
    </head></html>"""

    with pytest.raises(ValueError, match="URI metadata differs"):
        refresh._resolved_uri(_product_request(), content)


def test_refresh_retains_current_dataset_and_release_document_separately(
    tmp_path: Path,
) -> None:
    dataset = build_eurostat_international_trade_goods_dataset_request(
        load_packaged_official_source_registry()
    )
    document = _document_request()

    assert refresh._local_path(tmp_path, dataset) == (
        tmp_path / "datasets" / "ext_st_easitc.json"
    )
    assert refresh._local_path(tmp_path, document) == (
        tmp_path / "documents" / "6-15062012-bp.pdf"
    )
    assert refresh._resolved_uri(
        document, b"legacy bytes without metadata"
    ) == (document.uri)


@pytest.mark.parametrize(
    ("uri", "filename"),
    (
        (
            (
                "https://ec.europa.eu/eurostat/documents/2995521/11563419/"
                "6-16122021-+AP-EN.pdf/id"
            ),
            "6-16122021-ap.pdf",
        ),
        (
            (
                "https://ec.europa.eu/eurostat/documents/2995521/17872469/"
                "6-15112023-PB-EN.pdf/id?download=true"
            ),
            "6-15112023-bp.pdf",
        ),
    ),
)
def test_refresh_accepts_only_bounded_document_filename_aliases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    uri: str,
    filename: str,
) -> None:
    request = _document_request(uri)
    monkeypatch.setattr(
        refresh.requests, "get", lambda requested, **_: _Response(requested)
    )

    content, resolved_uri, _ = refresh._PoliteFetcher(13.0).fetch(request)

    assert content == b"{}"
    assert resolved_uri == uri
    assert refresh._local_path(tmp_path, request) == (
        tmp_path / "documents" / filename
    )


@pytest.mark.parametrize("value", (12.999, float("nan"), float("inf"), 3_601.0))
def test_refresh_rejects_invalid_request_interval(value: float) -> None:
    with pytest.raises(ValueError, match="between 13 and 3600 seconds"):
        refresh._PoliteFetcher(value)


def test_refresh_spaces_retry_after_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = build_eurostat_international_trade_goods_dataset_request(
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
