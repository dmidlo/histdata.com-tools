from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
import requests

from histdatacom.market_context.economic_calendar import (
    EconomicEventFamily,
    EconomicTimePrecision,
)
from histdatacom.market_context.official_sources import (
    MAX_OFFICIAL_MANIFEST_BYTES,
    MAX_OFFICIAL_REQUEST_BODY_BYTES,
    OFFICIAL_FETCH_BUNDLE_SCHEMA_VERSION,
    OFFICIAL_FETCH_POLICY_SCHEMA_VERSION,
    OFFICIAL_RAW_SNAPSHOT_SCHEMA_VERSION,
    OFFICIAL_REQUEST_MANIFEST_SCHEMA_VERSION,
    OFFICIAL_SOURCE_ENTRY_SCHEMA_VERSION,
    OFFICIAL_SOURCE_REGISTRY_SCHEMA_VERSION,
    OFFICIAL_SOURCE_REQUEST_SCHEMA_VERSION,
    SCOPED_ECONOMIES,
    OfficialAuthenticationKind,
    OfficialFetchBundleV1,
    OfficialFetchError,
    OfficialFetchPolicyV1,
    OfficialRawSnapshotV1,
    OfficialRequestManifestV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
    OfficialSourceRole,
    OfficialSourceVerificationStatus,
    build_official_request_manifest,
    condition_official_request,
    environment_official_credentials,
    fetch_official_request_manifest,
    load_packaged_official_source_registry,
    normalize_official_source_timestamp,
    official_source_matrix,
    packaged_official_source_registry_path,
    parse_official_snapshot,
    plan_official_source_requests,
    replay_official_fetch_bundle,
    split_official_date_windows,
    write_official_fetch_bundle,
)


class _Response:
    def __init__(
        self,
        content: bytes = b'{"ok":true}',
        *,
        status: int = 200,
        url: str = "https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json",
        headers: dict[str, str] | None = None,
        chunks: list[bytes | str] | None = None,
    ) -> None:
        self.content = content
        self.status_code = status
        self.url = url
        self.headers = headers or {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": str(len(content)),
            "ETag": '"v1"',
            "Last-Modified": "Sun, 13 Sep 2026 12:00:00 GMT",
            "Date": "Sun, 13 Sep 2026 12:01:00 GMT",
            "Set-Cookie": "must-not-be-retained=1",
        }
        self._chunks = chunks
        self.closed = False

    def iter_content(self, *, chunk_size: int):
        assert 1024 <= chunk_size <= 1024 * 1024
        if self._chunks is not None:
            yield from self._chunks
            return
        midpoint = max(1, len(self.content) // 2)
        yield self.content[:midpoint]
        yield self.content[midpoint:]

    def close(self) -> None:
        self.closed = True


class _Transport:
    def __init__(self, outcomes: list[_Response | BaseException]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        method: str,
        url: str,
        *,
        params,
        headers,
        data,
        timeout,
        stream,
        allow_redirects,
    ) -> _Response:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "params": dict(params),
                "headers": dict(headers),
                "data": data,
                "timeout": timeout,
                "stream": stream,
                "allow_redirects": allow_redirects,
            }
        )
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _Clock:
    def __init__(self, values: list[int]) -> None:
        self.values = iter(values)

    def __call__(self) -> int:
        return next(self.values)


@pytest.fixture(scope="module")
def registry() -> OfficialSourceRegistryV1:
    return load_packaged_official_source_registry()


@pytest.fixture()
def policy() -> OfficialFetchPolicyV1:
    return OfficialFetchPolicyV1(
        max_attempts=2,
        backoff_seconds=(0.01,),
        max_response_bytes=1024,
        max_total_bytes=4096,
        max_requests=16,
        max_pages=8,
        max_events=20,
        max_runtime_seconds=5,
        chunk_size_bytes=1024,
    )


def _request(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
    source_key: str = "ca.boc.valet",
) -> OfficialSourceRequestV1:
    source = registry.source(source_key)
    return plan_official_source_requests(source, policy)[0]


def _manifest(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
    source_key: str = "ca.boc.valet",
) -> OfficialRequestManifestV1:
    return build_official_request_manifest(
        registry,
        (_request(registry, policy, source_key),),
        policy=policy,
    )


def _fetch(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
    response: _Response | None = None,
) -> tuple[OfficialFetchBundleV1, _Transport]:
    transport = _Transport([response or _Response()])
    bundle = fetch_official_request_manifest(
        registry,
        _manifest(registry, policy),
        transport=transport,
        clock_ns=_Clock([100, 101]),
        monotonic=lambda: 0.0,
        sleeper=lambda _: None,
    )
    return bundle, transport


def test_packaged_registry_is_a_complete_reviewed_21_economy_matrix(
    registry: OfficialSourceRegistryV1,
) -> None:
    assert packaged_official_source_registry_path().is_file()
    assert registry.schema_version == OFFICIAL_SOURCE_REGISTRY_SCHEMA_VERSION
    assert registry.scoped_economies == SCOPED_ECONOMIES
    assert len(registry.sources) == 68
    matrix = official_source_matrix(registry)
    assert set(matrix) == set(SCOPED_ECONOMIES)
    assert sum(len(row) for row in matrix.values()) == 21 * len(
        EconomicEventFamily
    )
    assert (
        matrix["US"][EconomicEventFamily.INFLATION_PRICES]
        == "us.bls.public-data"
    )
    assert (
        matrix["US"][EconomicEventFamily.GDP_NATIONAL_ACCOUNTS] == "us.bea.api"
    )
    assert (
        matrix["DE"][EconomicEventFamily.MONETARY_POLICY]
        == "de.ecb.monetary-policy"
    )
    assert (
        matrix["DE"][EconomicEventFamily.MONEY_CREDIT] == "de.bundesbank.sdmx"
    )
    assert all(
        source.legal_producer
        and source.verification_evidence_uris
        and source.historical_depth
        and source.revision_behavior
        and source.replay_strategy
        for source in registry.sources
    )
    assert (
        registry.source("tr.cbrt.evds").formats[0] is OfficialSourceFormat.JSON
    )


def test_registry_round_trip_and_identity_cover_source_endpoint_and_parser(
    registry: OfficialSourceRegistryV1,
) -> None:
    restored = OfficialSourceRegistryV1.from_dict(registry.to_dict())
    assert restored == registry
    assert restored.registry_id == registry.registry_id
    source = registry.source("ca.boc.valet")
    assert registry.source(source.source_id) == source
    with pytest.raises(ValueError, match="exactly one registered"):
        registry.source("ca.missing")

    changed_source = replace(source, parser_version="2")
    changed = replace(
        registry,
        sources=tuple(
            changed_source if item.source_key == source.source_key else item
            for item in registry.sources
        ),
    )
    assert changed_source.source_id != source.source_id
    assert changed.registry_id != registry.registry_id

    data = registry.to_dict()
    data["registry_id"] = "official-source-registry:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="registry identity differs"):
        OfficialSourceRegistryV1.from_dict(data)


def test_registry_rejects_primary_ambiguity_invalid_fallback_and_mirrors(
    registry: OfficialSourceRegistryV1,
) -> None:
    source = registry.source("ca.boc.valet")
    with pytest.raises(ValueError, match="primary source matrix cell"):
        replace(
            registry,
            sources=tuple(
                (
                    replace(item, roles=(OfficialSourceRole.OFFICIAL_ARCHIVE,))
                    if item.source_key == source.source_key
                    else item
                )
                for item in registry.sources
            ),
        )
    with pytest.raises(ValueError, match="fallback source is invalid"):
        replace(
            registry,
            sources=tuple(
                (
                    replace(item, fallback_source_keys=("ca.absent",))
                    if item.source_key == source.source_key
                    else item
                )
                for item in registry.sources
            ),
        )
    with pytest.raises(ValueError, match="primary source must"):
        replace(source, legal_producer=False)
    cross_check = replace(
        source,
        source_key="ca.boc.cross-check",
        legal_producer=False,
        roles=(OfficialSourceRole.INDEPENDENT_OFFICIAL_CROSS_CHECK,),
    )
    assert cross_check.legal_producer is False


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"allowed_hosts": ("example.com",)}, "endpoint host"),
        ({"source_timezone": "Mars/Olympus"}, "timezone"),
        ({"expected_media_types": ()}, "must not be empty"),
        ({"reviewed_on": "yesterday"}, "ISO date"),
        (
            {
                "authentication": OfficialAuthenticationKind.NONE,
                "credential_env_name": "SECRET",
                "credential_header_name": "X-Key",
            },
            "cannot declare credentials",
        ),
        (
            {
                "authentication": OfficialAuthenticationKind.REQUIRED_API_KEY,
                "credential_env_name": "SECRET",
                "credential_header_name": "X-Key",
                "credential_parameter_name": "key",
            },
            "exactly one header, parameter, or path placeholder",
        ),
        (
            {"indicator_ids": ("wrong-indicator",) * 2},
            "canonical identifier",
        ),
    ],
)
def test_source_entry_validation(
    registry: OfficialSourceRegistryV1,
    changes: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(registry.source("ca.boc.valet"), **changes)


def test_source_entry_allowlist_covers_all_fetch_routes(
    registry: OfficialSourceRegistryV1,
) -> None:
    source = registry.source("au.abs.data-api")
    with pytest.raises(ValueError, match="release-calendar and archive hosts"):
        replace(source, allowed_hosts=("data.api.abs.gov.au",))


def test_source_and_policy_schema_and_enum_validation(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    source = registry.source("ca.boc.valet")
    assert source.schema_version == OFFICIAL_SOURCE_ENTRY_SCHEMA_VERSION
    assert (
        source.verification_status
        is OfficialSourceVerificationStatus.REVIEWED_ENTRYPOINT
    )
    assert OfficialSourceFormat.from_value("JSON") is OfficialSourceFormat.JSON
    assert (
        OfficialRequestMethod.from_value("post") is OfficialRequestMethod.POST
    )
    with pytest.raises(ValueError, match="unsupported source format"):
        OfficialSourceFormat.from_value("parquet")
    with pytest.raises(ValueError, match="unsupported official request method"):
        OfficialRequestMethod.from_value("DELETE")
    assert OfficialFetchPolicyV1.from_dict(policy.to_dict()) == policy
    assert policy.schema_version == OFFICIAL_FETCH_POLICY_SCHEMA_VERSION
    with pytest.raises(ValueError, match="backoff_seconds"):
        replace(policy, max_attempts=3, backoff_seconds=(0.1,))
    with pytest.raises(ValueError, match="max_requests"):
        replace(policy, max_requests=0)
    with pytest.raises(ValueError, match="max_redirects"):
        replace(policy, max_redirects=-1)
    with pytest.raises(ValueError, match="control character"):
        replace(policy, user_agent="client\r\nX-Injected: yes")


def test_window_and_pagination_planning_is_deterministic_and_bounded(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    assert split_official_date_windows(
        "2026-01-01", "2026-01-05", max_days=2
    ) == (
        ("2026-01-01", "2026-01-02"),
        ("2026-01-03", "2026-01-04"),
        ("2026-01-05", "2026-01-05"),
    )
    source = registry.source("ca.boc.valet")
    requests_ = plan_official_source_requests(
        source,
        policy,
        query_parameters={"series": "FXUSDCAD"},
        start_date="2026-01-01",
        end_date="2026-01-03",
        max_window_days=2,
        start_parameter="start_date",
        end_parameter="end_date",
        page_count=2,
        page_parameter="page",
        page_start=1,
        page_size=50,
        page_size_parameter="limit",
    )
    assert len(requests_) == 4
    assert [item.page_number for item in requests_] == [1, 2, 1, 2]
    assert requests_[0].window_start == "2026-01-01"
    assert requests_[-1].window_end == "2026-01-03"
    assert requests_[0].query_parameters == {
        "end_date": "2026-01-02",
        "limit": "50",
        "page": "1",
        "series": "FXUSDCAD",
        "start_date": "2026-01-01",
    }
    assert requests_ == plan_official_source_requests(
        source,
        policy,
        query_parameters={"series": "FXUSDCAD"},
        start_date="2026-01-01",
        end_date="2026-01-03",
        max_window_days=2,
        start_parameter="start_date",
        end_parameter="end_date",
        page_count=2,
        page_parameter="page",
        page_start=1,
        page_size=50,
        page_size_parameter="limit",
    )
    manifest = build_official_request_manifest(
        registry, requests_, policy=policy
    )
    with pytest.raises(TypeError):
        cast(dict[str, str], manifest.requests[0].query_parameters)[
            "mutable"
        ] = "no"
    restored = OfficialRequestManifestV1.from_dict(manifest.to_dict())
    assert restored == manifest
    assert manifest.schema_version == OFFICIAL_REQUEST_MANIFEST_SCHEMA_VERSION
    assert len({item.request_id for item in manifest.requests}) == 4
    missing_request_id = manifest.to_dict()
    cast(list[dict[str, Any]], missing_request_id["requests"])[0].pop(
        "request_id"
    )
    with pytest.raises(ValueError, match="request identity differs"):
        OfficialRequestManifestV1.from_dict(missing_request_id)

    templated = plan_official_source_requests(
        source,
        policy,
        uri_template=(
            "https://www.bankofcanada.ca/valet/observations/FXUSDCAD/"
            "json?start={start}&end={end}&page={page}&limit={page_size}"
        ),
        start_date="2026-01-01",
        end_date="2026-01-01",
        max_window_days=1,
        start_parameter=None,
        end_parameter=None,
        page_parameter="page",
        page_size=50,
        page_size_parameter="limit",
    )[0]
    assert templated.uri.endswith(
        "json?start=2026-01-01&end=2026-01-01&page=0&limit=50"
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"page_count": 2}, "page_parameter"),
        ({"page_size": 10}, "page_size_parameter"),
        ({"start_date": "2026-01-01"}, "both start_date and end_date"),
        ({"page_count": 9, "page_parameter": "page"}, "page_count"),
        (
            {"uri_template": "https://example.com/data"},
            "host is not registered",
        ),
        (
            {"uri_template": "https://www.bankofcanada.ca/{series}"},
            "unsupported placeholder",
        ),
        ({"max_window_days": 1}, "requires a date window"),
        (
            {
                "start_date": "2026-01-01",
                "end_date": "2026-01-02",
                "max_window_days": 0,
            },
            "max_days is outside bounds",
        ),
    ],
)
def test_request_planning_rejects_unbounded_shapes(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        plan_official_source_requests(
            registry.source("ca.boc.valet"), policy, **kwargs
        )
    with pytest.raises(ValueError, match="precedes"):
        split_official_date_windows("2026-02-01", "2026-01-01", max_days=1)


def test_requests_are_credential_free_and_bound_to_source_parser_and_format(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    request = _request(registry, policy)
    assert request.schema_version == OFFICIAL_SOURCE_REQUEST_SCHEMA_VERSION
    assert OfficialSourceRequestV1.from_dict(request.to_dict()) == request
    with pytest.raises(ValueError, match="Sensitive|sensitive"):
        replace(request, headers={"Authorization": "secret"})
    with pytest.raises(ValueError, match="reserved transport headers"):
        replace(request, headers={"Accept-Encoding": "gzip"})
    with pytest.raises(ValueError, match="control character"):
        replace(request, headers={"X-Public": "value\r\nX-Injected: yes"})
    with pytest.raises(ValueError, match="fragment"):
        replace(request, uri=f"{request.uri}#ignored")
    with pytest.raises(ValueError, match="GET requests"):
        replace(
            request,
            body_text="{}",
            body_content_type="application/json",
        )
    with pytest.raises(TypeError, match="body_text must be a string"):
        replace(
            request,
            method=OfficialRequestMethod.POST,
            body_text=cast(Any, b"{}"),
            body_content_type="application/json",
        )
    with pytest.raises(ValueError, match="body and content type"):
        replace(request, method=OfficialRequestMethod.POST, body_text="{}")
    with pytest.raises(ValueError, match="body_text exceeds byte bound"):
        replace(
            request,
            method=OfficialRequestMethod.POST,
            body_text="x" * (MAX_OFFICIAL_REQUEST_BODY_BYTES + 1),
            body_content_type="application/json",
        )
    with pytest.raises(ValueError, match="control character"):
        replace(request, if_none_match='"v1"\r\nX-Injected: yes')
    with pytest.raises(ValueError, match="host is not registered"):
        build_official_request_manifest(
            registry,
            (replace(request, uri="https://example.com/data"),),
            policy=policy,
        )
    with pytest.raises(ValueError, match="format is not registered"):
        build_official_request_manifest(
            registry,
            (replace(request, source_format=OfficialSourceFormat.PDF),),
            policy=policy,
        )
    with pytest.raises(ValueError, match="parser identity"):
        build_official_request_manifest(
            registry,
            (replace(request, parser_version="2"),),
            policy=policy,
        )
    credential_source = registry.source("cz.cnb.arad")
    credential_request = plan_official_source_requests(
        credential_source, policy
    )[0]
    with pytest.raises(ValueError, match="credential parameter"):
        build_official_request_manifest(
            registry,
            (
                replace(
                    credential_request,
                    query_parameters={"api_key": "must-not-be-serialized"},
                ),
            ),
            policy=policy,
        )
    header_source = registry.source("nz.stats-api")
    header_request = plan_official_source_requests(header_source, policy)[0]
    with pytest.raises(ValueError, match="credential header"):
        build_official_request_manifest(
            registry,
            (
                replace(
                    header_request,
                    headers={
                        "Ocp-Apim-Subscription-Key": "must-not-be-serialized"
                    },
                ),
            ),
            policy=policy,
        )
    with pytest.raises(ValueError, match="credential parameter"):
        build_official_request_manifest(
            registry,
            (
                replace(
                    credential_request,
                    uri=f"{credential_request.uri}?api_key=x",
                ),
            ),
            policy=policy,
        )


def test_request_manifest_has_a_serialized_byte_bound(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    request = _request(registry, policy)
    body = "x" * MAX_OFFICIAL_REQUEST_BODY_BYTES
    requests_ = tuple(
        replace(
            request,
            method=OfficialRequestMethod.POST,
            query_parameters={"request": str(index)},
            body_text=body,
            body_content_type="application/json",
        )
        for index in range(MAX_OFFICIAL_MANIFEST_BYTES // len(body) + 1)
    )
    with pytest.raises(
        ValueError, match="manifest exceeds serialized byte bound"
    ):
        build_official_request_manifest(registry, requests_, policy=policy)


def test_timestamp_normalization_retains_lexical_zone_precision_and_fold() -> (
    None
):
    value = normalize_official_source_timestamp(
        "2026-09-13T08:30:00-04:00",
        "America/New_York",
        EconomicTimePrecision.EXACT_MINUTE,
    )
    assert value.source_lexical == "2026-09-13T08:30:00-04:00"
    assert value.source_timezone == "America/New_York"
    assert value.to_dict()["precision"] == "exact-minute"
    first = normalize_official_source_timestamp(
        "2022-11-06T01:30:00",
        "America/New_York",
        EconomicTimePrecision.EXACT_MINUTE,
        fold=0,
    )
    second = normalize_official_source_timestamp(
        "2022-11-06T01:30:00",
        "America/New_York",
        EconomicTimePrecision.EXACT_MINUTE,
        fold=1,
    )
    assert second.utc_ns - first.utc_ns == 3_600_000_000_000
    with pytest.raises(ValueError, match="differs from normalized"):
        replace(first, utc_ns=first.utc_ns + 1)


def test_successful_fetch_retains_safe_headers_exact_bytes_and_transport_policy(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    response = _Response()
    bundle, transport = _fetch(registry, policy, response)
    snapshot = bundle.snapshots[0]
    assert bundle.schema_version == OFFICIAL_FETCH_BUNDLE_SCHEMA_VERSION
    assert snapshot.schema_version == OFFICIAL_RAW_SNAPSHOT_SCHEMA_VERSION
    assert snapshot.content == b'{"ok":true}'
    assert snapshot.content_sha256
    assert snapshot.etag == '"v1"'
    assert "set-cookie" not in snapshot.response_headers
    assert response.closed is True
    assert transport.calls == [
        {
            "method": "GET",
            "url": "https://www.bankofcanada.ca/valet/",
            "params": {},
            "headers": {
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "User-Agent": policy.user_agent,
            },
            "data": None,
            "timeout": (5.0, 5.0),
            "stream": True,
            "allow_redirects": False,
        }
    ]


def test_fetch_retries_transient_transport_and_status_errors(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    failed = _Response(status=503)
    success = _Response()
    transport = _Transport([requests.Timeout("network timeout"), success])
    sleeps: list[float] = []
    bundle = fetch_official_request_manifest(
        registry,
        _manifest(registry, policy),
        transport=transport,
        clock_ns=_Clock([100, 101]),
        monotonic=lambda: 0.0,
        sleeper=sleeps.append,
    )
    assert bundle.snapshots[0].attempt_count == 2
    assert sleeps == [0.01]

    transport = _Transport([failed, _Response()])
    bundle = fetch_official_request_manifest(
        registry,
        _manifest(registry, policy),
        transport=transport,
        clock_ns=_Clock([100, 101]),
        monotonic=lambda: 0.0,
        sleeper=lambda _: None,
    )
    assert bundle.snapshots[0].attempt_count == 2
    assert failed.closed


def test_fetch_does_not_retry_permanent_failures(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    failed = _Response(status=404)
    transport = _Transport([failed, _Response()])
    with pytest.raises(OfficialFetchError, match="HTTP status 404"):
        fetch_official_request_manifest(
            registry,
            _manifest(registry, policy),
            transport=transport,
            clock_ns=_Clock([100]),
            monotonic=lambda: 0.0,
            sleeper=lambda _: None,
        )
    assert len(transport.calls) == 1
    assert failed.closed


def test_fetch_follows_only_bounded_credential_free_registered_redirects(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    redirect = _Response(
        status=302,
        url="https://www.bankofcanada.ca/valet/",
        headers={"Location": "/valet/observations/FXUSDCAD/json"},
    )
    transport = _Transport([redirect, _Response()])
    bundle = fetch_official_request_manifest(
        registry,
        _manifest(registry, policy),
        transport=transport,
        clock_ns=_Clock([100, 101]),
        monotonic=lambda: 0.0,
        sleeper=lambda _: None,
    )
    assert bundle.snapshots[0].status_code == 200
    assert redirect.closed
    assert transport.calls[1]["url"].endswith(
        "/valet/observations/FXUSDCAD/json"
    )
    assert all(call["allow_redirects"] is False for call in transport.calls)

    unregistered = _Response(
        status=302,
        url="https://www.bankofcanada.ca/valet/",
        headers={"Location": "https://example.com/stolen"},
    )
    transport = _Transport([unregistered])
    with pytest.raises(OfficialFetchError, match="unregistered host"):
        fetch_official_request_manifest(
            registry,
            _manifest(registry, policy),
            transport=transport,
            clock_ns=_Clock([100]),
            monotonic=lambda: 0.0,
            sleeper=lambda _: None,
        )
    assert len(transport.calls) == 1
    assert unregistered.closed

    bounded = _Response(
        status=302,
        url="https://www.bankofcanada.ca/valet/",
        headers={"Location": "/valet/next"},
    )
    with pytest.raises(OfficialFetchError, match="redirect bound"):
        fetch_official_request_manifest(
            registry,
            _manifest(registry, replace(policy, max_redirects=0)),
            transport=_Transport([bounded]),
            clock_ns=_Clock([100]),
            monotonic=lambda: 0.0,
            sleeper=lambda _: None,
        )
    assert bounded.closed


def test_sdmx_csv_response_uses_the_media_type_specific_signature(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    content = b"TIME_PERIOD,OBS_VALUE\n2026-01,1.25\n"
    response = _Response(
        content=content,
        url="https://data.api.abs.gov.au/rest/data/ABS,TEST,1.0/all",
        headers={
            "Content-Type": "text/csv",
            "Content-Length": str(len(content)),
        },
    )
    bundle = fetch_official_request_manifest(
        registry,
        _manifest(registry, policy, "au.abs.data-api"),
        transport=_Transport([response]),
        clock_ns=_Clock([100, 101]),
        monotonic=lambda: 0.0,
        sleeper=lambda _: None,
    )
    assert bundle.snapshots[0].content == content


def test_fetch_refuses_redirects_that_could_leak_runtime_credentials(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    source = registry.source("mx.inegi.indicators")
    request = plan_official_source_requests(
        source,
        policy,
        uri_template=(
            "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/"
            "jsonxml/INDICATOR/1002000002/en/00000/false/BISE/2.0/"
            "{credential}?type=json"
        ),
    )[0]
    response = _Response(
        status=302,
        url=request.uri.replace("{credential}", "top-secret"),
        headers={"Location": "/credential-capture"},
    )
    with pytest.raises(
        OfficialFetchError, match="authenticated.*redirect"
    ) as error:
        fetch_official_request_manifest(
            registry,
            build_official_request_manifest(
                registry, (request,), policy=policy
            ),
            transport=_Transport([response]),
            credential_provider=lambda _: "top-secret",
            clock_ns=_Clock([100]),
            monotonic=lambda: 0.0,
            sleeper=lambda _: None,
        )
    assert "top-secret" not in str(error.value)
    assert response.closed


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (
            _Response(
                headers={
                    "Content-Type": "application/json",
                    "Content-Length": "2000",
                }
            ),
            "declared byte bound",
        ),
        (
            _Response(
                headers={
                    "Content-Type": "application/json",
                    "Content-Length": "99",
                }
            ),
            "incomplete",
        ),
        (
            _Response(
                headers={"Content-Type": "text/html", "Content-Length": "11"}
            ),
            "MIME type",
        ),
        (_Response(content=b"not-json"), "signature"),
        (
            _Response(url="https://example.com/data"),
            "unregistered host",
        ),
        (
            _Response(headers={"Content-Length": "11"}),
            "omits Content-Type",
        ),
        (
            _Response(
                headers={
                    "Content-Type": "application/json",
                    "Content-Length": "oops",
                }
            ),
            "Content-Length is invalid",
        ),
        (
            _Response(
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip",
                    "Content-Length": "11",
                }
            ),
            "encoded representation",
        ),
        (_Response(status=404), "HTTP status 404"),
        (_Response(status=206), "HTTP status 206"),
        (
            _Response(
                content=b"", headers={"Content-Type": "application/json"}
            ),
            "empty",
        ),
        (
            _Response(
                chunks=["not-bytes"],
                headers={"Content-Type": "application/json"},
            ),
            "non-byte",
        ),
    ],
)
def test_fetch_fails_closed_on_corrupt_or_unregistered_responses(
    registry: OfficialSourceRegistryV1,
    response: _Response,
    message: str,
) -> None:
    one_attempt = OfficialFetchPolicyV1(
        max_attempts=1,
        backoff_seconds=(),
        max_response_bytes=1024,
        max_total_bytes=4096,
        max_requests=2,
        max_pages=2,
        max_events=20,
        max_runtime_seconds=5,
        chunk_size_bytes=1024,
    )
    with pytest.raises(OfficialFetchError, match=message):
        fetch_official_request_manifest(
            registry,
            _manifest(registry, one_attempt),
            transport=_Transport([response]),
            clock_ns=_Clock([100]),
            monotonic=lambda: 0.0,
            sleeper=lambda _: None,
        )
    assert response.closed


def test_conditional_request_reuses_exact_predecessor_bytes(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    first, _ = _fetch(registry, policy)
    previous = first.snapshots[0]
    conditioned = condition_official_request(previous.request, previous)
    assert conditioned.if_none_match == '"v1"'
    assert conditioned.if_modified_since
    manifest = build_official_request_manifest(
        registry, (conditioned,), policy=policy
    )
    response = _Response(
        content=b"",
        status=304,
        headers={"ETag": '"v1"', "Date": "Sun, 13 Sep 2026 12:02:00 GMT"},
    )
    bundle = fetch_official_request_manifest(
        registry,
        manifest,
        transport=_Transport([response]),
        previous_snapshots={conditioned.request_id: previous},
        clock_ns=_Clock([102, 103]),
        monotonic=lambda: 0.0,
        sleeper=lambda _: None,
    )
    current = bundle.snapshots[0]
    assert current.status_code == 304
    assert current.content == previous.content
    assert current.reused_from_snapshot_id == previous.snapshot_id
    assert current.snapshot_id != previous.snapshot_id

    with pytest.raises(ValueError, match="no conditional validators"):
        condition_official_request(
            replace(previous.request),
            replace(previous, etag=None, last_modified=None),
        )
    with pytest.raises(ValueError, match="request target"):
        condition_official_request(
            replace(previous.request, headers={"X-Public": "changed"}), previous
        )
    with pytest.raises(OfficialFetchError, match="no prior retained snapshot"):
        fetch_official_request_manifest(
            registry,
            manifest,
            transport=_Transport([response, response]),
            clock_ns=_Clock([104]),
            monotonic=lambda: 0.0,
            sleeper=lambda _: None,
        )


def test_runtime_credentials_never_enter_manifests_or_retained_resolved_urls(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    source = registry.source("mx.inegi.indicators")
    request = plan_official_source_requests(
        source,
        policy,
        uri_template=(
            "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/"
            "jsonxml/INDICATOR/1002000002/en/00000/false/BISE/2.0/"
            "{credential}?type=json"
        ),
    )[0]
    manifest = build_official_request_manifest(
        registry, (request,), policy=policy
    )
    serialized = json.dumps(manifest.to_dict())
    assert "top-secret" not in serialized
    response = _Response(
        url=(
            "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/"
            "jsonxml/INDICATOR/1002000002/en/00000/false/BISE/2.0/"
            "top-secret?type=json"
        )
    )
    transport = _Transport([response])
    bundle = fetch_official_request_manifest(
        registry,
        manifest,
        transport=transport,
        credential_provider=lambda _: "top-secret",
        clock_ns=_Clock([100, 101]),
        monotonic=lambda: 0.0,
        sleeper=lambda _: None,
    )
    assert transport.calls[0]["url"].endswith("top-secret?type=json")
    assert "top-secret" not in bundle.snapshots[0].resolved_uri
    assert "{credential}" in bundle.snapshots[0].resolved_uri
    assert "type=json" in bundle.snapshots[0].resolved_uri
    assert "top-secret" not in json.dumps(bundle.to_dict())

    header_source = registry.source("mx.banxico.sie")
    header_request = plan_official_source_requests(header_source, policy)[0]
    header_transport = _Transport(
        [
            _Response(
                url="https://www.banxico.org.mx/SieAPIRest/service/v1/series/"
            )
        ]
    )
    fetch_official_request_manifest(
        registry,
        build_official_request_manifest(
            registry, (header_request,), policy=policy
        ),
        transport=header_transport,
        credential_provider=lambda _: "top-secret",
        clock_ns=_Clock([100, 101]),
        monotonic=lambda: 0.0,
        sleeper=lambda _: None,
    )
    assert header_transport.calls[0]["headers"]["Bmx-Token"] == "top-secret"


def test_environment_credentials_obey_required_and_optional_contracts(
    registry: OfficialSourceRegistryV1,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    required = registry.source("mx.inegi.indicators")
    monkeypatch.delenv("HISTDATACOM_INEGI_TOKEN", raising=False)
    with pytest.raises(OfficialFetchError, match="environment variable"):
        environment_official_credentials(required)
    monkeypatch.setenv("HISTDATACOM_INEGI_TOKEN", "credential")
    assert environment_official_credentials(required) == "credential"
    assert (
        environment_official_credentials(registry.source("ca.boc.valet"))
        is None
    )
    optional = replace(
        required,
        authentication=OfficialAuthenticationKind.OPTIONAL_API_KEY,
        credential_env_name="HISTDATACOM_OPTIONAL_KEY",
    )
    monkeypatch.delenv("HISTDATACOM_OPTIONAL_KEY", raising=False)
    assert environment_official_credentials(optional) is None


def test_fetch_enforces_shared_total_bytes_runtime_and_registry_identity(
    registry: OfficialSourceRegistryV1,
) -> None:
    low_bytes = OfficialFetchPolicyV1(
        max_attempts=1,
        backoff_seconds=(),
        max_response_bytes=100,
        max_total_bytes=5,
        max_requests=2,
        max_pages=2,
        max_events=10,
        max_runtime_seconds=5,
        chunk_size_bytes=1024,
    )
    with pytest.raises(OfficialFetchError, match="total byte"):
        _fetch(registry, low_bytes)

    low_runtime = replace(
        low_bytes, max_total_bytes=100, max_runtime_seconds=0.5
    )
    transport = _Transport([_Response()])
    with pytest.raises(OfficialFetchError, match="runtime"):
        fetch_official_request_manifest(
            registry,
            _manifest(registry, low_runtime),
            transport=transport,
            clock_ns=_Clock([100, 101]),
            monotonic=_Clock([0, 1]),
            sleeper=lambda _: None,
        )
    manifest = _manifest(registry, low_runtime)
    with pytest.raises(ValueError, match="registry identity differs"):
        fetch_official_request_manifest(
            registry,
            replace(
                manifest,
                registry_id="official-source-registry:sha256:" + "0" * 64,
            ),
        )


def test_fetch_checks_shared_runtime_while_streaming_response_chunks(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    response = _Response(chunks=[b'{"ok":', b"true}"])
    with pytest.raises(OfficialFetchError, match="runtime"):
        fetch_official_request_manifest(
            registry,
            _manifest(registry, replace(policy, max_runtime_seconds=0.5)),
            transport=_Transport([response]),
            clock_ns=_Clock([100]),
            monotonic=_Clock([0, 0, 0, 0, 0, 0, 1]),
            sleeper=lambda _: None,
        )
    assert response.closed


def test_content_addressed_bundle_write_and_exact_replay(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
    tmp_path: Path,
) -> None:
    bundle, _ = _fetch(registry, policy)
    artifacts = write_official_fetch_bundle(bundle, tmp_path)
    bundle_path = Path(artifacts["bundle"].path)
    replayed = replay_official_fetch_bundle(bundle_path, registry=registry)
    assert replayed == bundle
    assert replayed.bundle_id == bundle.bundle_id
    assert artifacts["bundle"].metadata["snapshot_count"] == 1
    assert not list(tmp_path.rglob("*.tmp-*"))
    assert (
        write_official_fetch_bundle(bundle, tmp_path)["bundle"]
        == artifacts["bundle"]
    )

    wrong_name = tmp_path / f"official-fetch-bundle-{'0' * 64}.json"
    wrong_name.write_bytes(bundle_path.read_bytes())
    with pytest.raises(ValueError, match="hash differs"):
        replay_official_fetch_bundle(wrong_name)

    source_path = Path(
        next(
            value.path
            for key, value in artifacts.items()
            if key.startswith("source:")
        )
    )
    source_path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="source hash differs"):
        replay_official_fetch_bundle(bundle_path)


def test_snapshot_restore_and_bundle_reject_identity_corruption(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    bundle, _ = _fetch(registry, policy)
    snapshot = bundle.snapshots[0]
    with pytest.raises(TypeError):
        cast(dict[str, str], snapshot.response_headers)["mutable"] = "no"
    evidence = snapshot.to_dict()
    assert OfficialRawSnapshotV1.restore(evidence, snapshot.content) == snapshot
    evidence["content_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="source hash differs"):
        OfficialRawSnapshotV1.restore(evidence, snapshot.content)
    with pytest.raises(ValueError, match="request order"):
        replace(bundle, snapshots=())
    small_response_policy = replace(policy, max_response_bytes=10)
    with pytest.raises(ValueError, match="response exceeds source-byte policy"):
        replace(
            bundle,
            request_manifest=replace(
                bundle.request_manifest,
                policy=small_response_policy,
            ),
        )


class _Parser:
    parser_id = "official.json.v1"
    parser_version = "1"
    supported_formats = (OfficialSourceFormat.JSON,)

    def __init__(self, values: list[Any]) -> None:
        self.values = values

    def parse(self, snapshot: OfficialRawSnapshotV1, *, max_events: int):
        assert snapshot.content
        assert max_events > 0
        return self.values


def test_typed_parser_seam_binds_format_version_and_event_bounds(
    registry: OfficialSourceRegistryV1,
    policy: OfficialFetchPolicyV1,
) -> None:
    bundle, _ = _fetch(registry, policy)
    snapshot = bundle.snapshots[0]
    assert parse_official_snapshot(
        snapshot, _Parser([{"value": 1}]), max_events=2
    ) == ({"value": 1},)
    with pytest.raises(ValueError, match="parser identity"):
        parse_official_snapshot(
            snapshot,
            replace_parser(_Parser([]), parser_version="2"),
            max_events=2,
        )
    with pytest.raises(ValueError, match="does not support"):
        parse_official_snapshot(
            snapshot,
            replace_parser(
                _Parser([]), supported_formats=(OfficialSourceFormat.CSV,)
            ),
            max_events=2,
        )
    with pytest.raises(ValueError, match="event bound"):
        parse_official_snapshot(
            snapshot,
            _Parser([{"value": 1}, {"value": 2}, {"value": 3}]),
            max_events=2,
        )
    with pytest.raises(TypeError, match="must be an object"):
        parse_official_snapshot(snapshot, _Parser(["bad"]), max_events=2)


def replace_parser(parser: _Parser, **changes: Any) -> _Parser:
    for key, value in changes.items():
        setattr(parser, key, value)
    return parser
