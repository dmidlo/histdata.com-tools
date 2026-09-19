"""Tests for the bounded ECB SPF refresh boundary."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import requests

from histdatacom.market_context import (
    build_ecb_spf_round_requests,
    build_ecb_spf_support_requests,
    load_packaged_official_source_registry,
)
from scripts import refresh_ecb_spf_archive as refresh
from tests.unit.test_ecb_spf_archive import _index_snapshot


class _Response:
    def __init__(
        self,
        url: str,
        *,
        status_code: int = 200,
        content: bytes = b"<html></html>",
        content_type: str = "text/html; charset=UTF-8",
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
    support = build_ecb_spf_support_requests(registry)
    round_request = build_ecb_spf_round_requests(registry, _index_snapshot())[0]

    assert refresh._corpus_path(tmp_path, support[0], role="all-data") == (
        tmp_path / "indexes/all-data.html"
    )
    assert refresh._corpus_path(tmp_path, support[1], role="all-reports") == (
        tmp_path / "indexes/all-reports.html"
    )
    assert refresh._corpus_path(tmp_path, support[2], role="microdata") == (
        tmp_path / "support/SPF_individual_forecasts.zip"
    )
    assert refresh._corpus_path(tmp_path, support[3], role="description") == (
        tmp_path / "support/SPF_dataset_description.pdf"
    )
    assert refresh._corpus_path(tmp_path, round_request, role="round") == (
        tmp_path / "rounds/table_3_1999q1.en.html"
    )
    with pytest.raises(ValueError, match="unsupported ECB SPF corpus role"):
        refresh._corpus_path(tmp_path, support[0], role="unknown")


def test_refresh_reads_retained_bytes_without_network(tmp_path: Path) -> None:
    request = build_ecb_spf_support_requests(
        load_packaged_official_source_registry()
    )[0]
    path = tmp_path / "indexes/all-data.html"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"<html>retained</html>")

    snapshot = refresh._snapshot(
        tmp_path,
        request,
        role="all-data",
        fetch_missing=False,
    )

    assert snapshot.content == b"<html>retained</html>"
    assert snapshot.request == request
    assert snapshot.content_type == "text/html"


def test_refresh_rejects_missing_or_mistyped_artifacts(tmp_path: Path) -> None:
    request = build_ecb_spf_support_requests(
        load_packaged_official_source_registry()
    )[0]

    with pytest.raises(
        FileNotFoundError, match="retained ECB SPF artifact is missing"
    ):
        refresh._snapshot(
            tmp_path,
            request,
            role="all-data",
            fetch_missing=False,
        )

    path = tmp_path / "indexes/all-data.html"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not HTML")
    with pytest.raises(ValueError, match="retained ECB SPF HTML differs"):
        refresh._snapshot(
            tmp_path,
            request,
            role="all-data",
            fetch_missing=False,
        )


def test_refresh_retries_transient_status_with_identifying_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = build_ecb_spf_support_requests(
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

    assert refresh._fetch(request) == b"<html></html>"
    assert len(calls) == 2
    assert all(
        call["headers"]["User-Agent"] == refresh.USER_AGENT for call in calls
    )
    assert all(call["timeout"] == (10.0, 60.0) for call in calls)
    assert all(call["method"] == "GET" for call in calls)
    assert sleeps == [2.0]


def test_refresh_rejects_redirect_and_media_type_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = build_ecb_spf_support_requests(
        load_packaged_official_source_registry()
    )[0]
    responses = iter(
        (
            _Response("https://example.com/all_data.en.html"),
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
