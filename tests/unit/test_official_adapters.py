from __future__ import annotations

import base64
import gzip
import io
from dataclasses import replace

import pytest
from openpyxl import Workbook

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.official_adapters import (
    OFFICIAL_ADAPTER_RECORD_SCHEMA_VERSION,
    OfficialAdapterPack,
    OfficialAdapterRecordKind,
    OfficialAdapterRecordV1,
    OfficialArchiveParserV1,
    OfficialCensusFt900ParserV1,
    OfficialCensusM3ParserV1,
    OfficialCensusMartsParserV1,
    OfficialCensusMtisParserV1,
    OfficialCensusNrcParserV1,
    OfficialCensusNrsParserV1,
    OfficialDataCatalogParserV1,
    OfficialDolEtaInitialClaimsParserV1,
    OfficialEurostatGdpParserV1,
    OfficialEurostatHicpParserV1,
    OfficialEurostatInternationalTradeGoodsParserV1,
    OfficialEurostatRetailTradeParserV1,
    OfficialFederalReserveFomcParserV1,
    OfficialFederalReserveH6ParserV1,
    OfficialParserError,
    OfficialParserFailureCode,
    OfficialPdfParserV1,
    OfficialPhiladelphiaFedMbosParserV1,
    OfficialPhiladelphiaFedSpfParserV1,
    OfficialReleaseFeedParserV1,
    OfficialSpreadsheetLayoutV1,
    OfficialSpreadsheetParserV1,
    audit_official_adapter_coverage,
    built_in_official_source_parsers,
    parse_with_built_in_official_adapter,
    qualify_official_adapter_fixture,
    required_official_adapter_packs,
    resolve_official_source_parser,
    verify_official_adapter_fixture,
)
from histdatacom.market_context.official_sources import (
    OfficialFetchPolicyV1,
    OfficialRawSnapshotV1,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    load_packaged_official_source_registry,
    parse_official_snapshot,
    plan_official_source_requests,
)


@pytest.fixture(scope="module")
def registry() -> OfficialSourceRegistryV1:
    return load_packaged_official_source_registry()


def _snapshot(
    registry: OfficialSourceRegistryV1,
    source_key: str,
    content: bytes,
    *,
    source_format: OfficialSourceFormat | None = None,
    parser_id: str | None = None,
    parser_version: str = "1",
    content_type: str = "application/octet-stream",
) -> OfficialRawSnapshotV1:
    source = registry.source(source_key)
    request = plan_official_source_requests(
        source,
        OfficialFetchPolicyV1(max_requests=4, max_pages=4),
    )[0]
    request = replace(
        request,
        source_format=source_format or request.source_format,
        parser_id=parser_id or request.parser_id,
        parser_version=parser_version,
    )
    return OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=1_789_344_000_000_000_000,
        completed_at_ns=1_789_344_000_000_000_100,
        status_code=200,
        resolved_uri=request.uri,
        response_headers={"content-type": content_type},
        content=content,
        content_type=content_type,
    )


def _xlsx_bytes(
    rows: list[list[str | float]], *, sheet_name: str = "Series"
) -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = sheet_name
    for row in rows:
        sheet.append(row)
    buffer = io.BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def _text_pdf_bytes(lines: list[str]) -> bytes:
    escaped = [
        line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        for line in lines
    ]
    operations = ["BT /F1 12 Tf 72 720 Td"]
    for index, line in enumerate(escaped):
        if index:
            operations.append("0 -20 Td")
        operations.append(f"({line}) Tj")
    operations.append("ET")
    stream = "\n".join(operations).encode("ascii")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
        ),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length "
        + str(len(stream)).encode("ascii")
        + b" >>\nstream\n"
        + stream
        + b"\nendstream",
    ]
    document = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, value in enumerate(objects, start=1):
        offsets.append(len(document))
        document.extend(f"{index} 0 obj\n".encode("ascii"))
        document.extend(value)
        document.extend(b"\nendobj\n")
    xref = len(document)
    document.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    document.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        document.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    document.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
            f"startxref\n{xref}\n%%EOF\n"
        ).encode("ascii")
    )
    return bytes(document)


def test_built_in_parsers_cover_every_registered_source_and_format(
    registry: OfficialSourceRegistryV1,
) -> None:
    parsers = built_in_official_source_parsers()
    assert len(parsers) == 31
    assert isinstance(
        parsers["official.dol-eta-initial-claims.v1"],
        OfficialDolEtaInitialClaimsParserV1,
    )
    assert isinstance(
        parsers["official.federal-reserve-fomc.v1"],
        OfficialFederalReserveFomcParserV1,
    )
    assert isinstance(
        parsers["official.federal-reserve-h6.v1"],
        OfficialFederalReserveH6ParserV1,
    )
    assert isinstance(
        parsers["official.eurostat-gdp.v1"],
        OfficialEurostatGdpParserV1,
    )
    assert isinstance(
        parsers["official.eurostat-hicp.v1"],
        OfficialEurostatHicpParserV1,
    )
    assert isinstance(
        parsers["official.eurostat-retail-trade.v1"],
        OfficialEurostatRetailTradeParserV1,
    )
    assert isinstance(
        parsers["official.eurostat-international-trade-goods.v1"],
        OfficialEurostatInternationalTradeGoodsParserV1,
    )
    assert isinstance(
        parsers["official.census-ft900.v1"],
        OfficialCensusFt900ParserV1,
    )
    assert isinstance(
        parsers["official.census-m3.v1"],
        OfficialCensusM3ParserV1,
    )
    assert isinstance(
        parsers["official.census-marts.v1"],
        OfficialCensusMartsParserV1,
    )
    assert isinstance(
        parsers["official.census-mtis.v1"],
        OfficialCensusMtisParserV1,
    )
    assert isinstance(
        parsers["official.census-nrc.v1"],
        OfficialCensusNrcParserV1,
    )
    assert isinstance(
        parsers["official.census-nrs.v1"],
        OfficialCensusNrsParserV1,
    )
    assert isinstance(
        parsers["official.philadelphia-mbos.v1"],
        OfficialPhiladelphiaFedMbosParserV1,
    )
    assert isinstance(
        parsers["official.philadelphia-spf.v1"],
        OfficialPhiladelphiaFedSpfParserV1,
    )
    for source in registry.sources:
        parser = resolve_official_source_parser(source, parsers=parsers)
        assert parser.parser_id == source.parser_id
        assert set(source.formats) <= set(parser.supported_formats)
    assert required_official_adapter_packs(registry) == (
        OfficialAdapterPack.GENERIC_STRUCTURED,
        OfficialAdapterPack.HTML_RELEASE,
        OfficialAdapterPack.JSON_STAT,
        OfficialAdapterPack.PDF,
        OfficialAdapterPack.RELEASE_FEED,
        OfficialAdapterPack.SDMX,
        OfficialAdapterPack.SPREADSHEET,
        OfficialAdapterPack.STATIC_ARCHIVE,
    )
    audit = audit_official_adapter_coverage(registry)
    assert not audit.complete
    assert not audit.missing_parser_bindings
    assert set(audit.missing_qualifications) == set(audit.required_packs)
    assert set(audit.available_packs) == set(OfficialAdapterPack)


@pytest.mark.parametrize(
    ("source_format", "content_type", "content"),
    (
        (
            OfficialSourceFormat.JSON_STAT,
            "application/json",
            (
                b'{"class":"dataset","id":["time"],"size":[1],'
                b'"dimension":{"time":{"category":{"index":['
                b'"2026M08"]}}},"value":[3.3]}'
            ),
        ),
        (
            OfficialSourceFormat.HTML,
            "text/html",
            b"<html><body><h1>Euro area annual inflation 3.3%</h1></body></html>",
        ),
        (
            OfficialSourceFormat.PDF,
            "application/pdf",
            _text_pdf_bytes(["Euro area annual inflation 3.3%"]),
        ),
    ),
)
def test_eurostat_hicp_adapter_dispatches_each_registered_format(
    registry: OfficialSourceRegistryV1,
    source_format: OfficialSourceFormat,
    content_type: str,
    content: bytes,
) -> None:
    source = registry.source("ea.eurostat.hicp")
    snapshot = _snapshot(
        registry,
        source.source_key,
        content,
        source_format=source_format,
        content_type=content_type,
    )

    records = parse_with_built_in_official_adapter(
        snapshot, source, max_events=20
    )

    assert records
    assert {item["parser_id"] for item in records} == {
        "official.eurostat-hicp.v1"
    }


@pytest.mark.parametrize(
    ("source_format", "content_type", "content"),
    (
        (
            OfficialSourceFormat.ATOM,
            "application/atom+xml",
            (
                b'<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
                b"<title>GDP up by 0.3% in the euro area</title>"
                b"<id>2-07092026-ap</id></entry></feed>"
            ),
        ),
        (
            OfficialSourceFormat.JSON_STAT,
            "application/json",
            (
                b'{"class":"dataset","id":["time"],"size":[1],'
                b'"dimension":{"time":{"category":{"index":['
                b'"2026-Q2"]}}},"value":[0.3]}'
            ),
        ),
        (
            OfficialSourceFormat.HTML,
            "text/html",
            b"<html><body><h1>GDP up by 0.3% in the euro area</h1></body></html>",
        ),
        (
            OfficialSourceFormat.PDF,
            "application/pdf",
            _text_pdf_bytes(["GDP up by 0.3% in the euro area"]),
        ),
    ),
)
def test_eurostat_gdp_adapter_dispatches_each_registered_format(
    registry: OfficialSourceRegistryV1,
    source_format: OfficialSourceFormat,
    content_type: str,
    content: bytes,
) -> None:
    source = registry.source("ea.eurostat.gdp")
    snapshot = _snapshot(
        registry,
        source.source_key,
        content,
        source_format=source_format,
        content_type=content_type,
    )

    records = parse_with_built_in_official_adapter(
        snapshot, source, max_events=20
    )

    assert records
    assert {item["parser_id"] for item in records} == {
        "official.eurostat-gdp.v1"
    }


@pytest.mark.parametrize(
    ("source_format", "content_type", "content"),
    (
        (
            OfficialSourceFormat.ATOM,
            "application/atom+xml",
            (
                b'<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
                b"<title>Euro area unemployment at 6.4%</title>"
                b"<id>3-01092026-bp</id></entry></feed>"
            ),
        ),
        (
            OfficialSourceFormat.JSON_STAT,
            "application/json",
            (
                b'{"class":"dataset","id":["time"],"size":[1],'
                b'"dimension":{"time":{"category":{"index":['
                b'"2026-07"]}}},"value":[6.4]}'
            ),
        ),
        (
            OfficialSourceFormat.HTML,
            "text/html",
            b"<html><body><h1>Euro area unemployment at 6.4%</h1></body></html>",
        ),
        (
            OfficialSourceFormat.PDF,
            "application/pdf",
            _text_pdf_bytes(["Euro area unemployment at 6.4%"]),
        ),
    ),
)
def test_eurostat_unemployment_adapter_dispatches_each_registered_format(
    registry: OfficialSourceRegistryV1,
    source_format: OfficialSourceFormat,
    content_type: str,
    content: bytes,
) -> None:
    source = registry.source("ea.eurostat.unemployment")
    snapshot = _snapshot(
        registry,
        source.source_key,
        content,
        source_format=source_format,
        content_type=content_type,
    )

    records = parse_with_built_in_official_adapter(
        snapshot, source, max_events=20
    )

    assert records
    assert {item["parser_id"] for item in records} == {
        "official.eurostat-unemployment.v1"
    }


@pytest.mark.parametrize(
    ("source_format", "content_type", "content"),
    (
        (
            OfficialSourceFormat.ATOM,
            "application/atom+xml",
            (
                b'<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
                b"<title>Industrial production down by 0.8%</title>"
                b"<id>4-21012002-ap</id></entry></feed>"
            ),
        ),
        (
            OfficialSourceFormat.JSON_STAT,
            "application/json",
            (
                b'{"class":"dataset","id":["time"],"size":[1],'
                b'"dimension":{"time":{"category":{"index":['
                b'"2001-11"]}}},"value":[-0.8]}'
            ),
        ),
        (
            OfficialSourceFormat.HTML,
            "text/html",
            b"<html><body><h1>Industrial production down by 0.8%</h1></body></html>",
        ),
        (
            OfficialSourceFormat.PDF,
            "application/pdf",
            _text_pdf_bytes(["Industrial production down by 0.8%"]),
        ),
    ),
)
def test_eurostat_industrial_production_adapter_dispatches_each_format(
    registry: OfficialSourceRegistryV1,
    source_format: OfficialSourceFormat,
    content_type: str,
    content: bytes,
) -> None:
    source = registry.source("ea.eurostat.industrial-production")
    snapshot = _snapshot(
        registry,
        source.source_key,
        content,
        source_format=source_format,
        content_type=content_type,
    )

    records = parse_with_built_in_official_adapter(
        snapshot, source, max_events=20
    )

    assert records
    assert {item["parser_id"] for item in records} == {
        "official.eurostat-industrial-production.v1"
    }


@pytest.mark.parametrize(
    ("source_format", "content_type", "content"),
    (
        (
            OfficialSourceFormat.ATOM,
            "application/atom+xml",
            (
                b'<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
                b"<title>Production in construction stable</title>"
                b"<id>4-18092026-ap</id></entry></feed>"
            ),
        ),
        (
            OfficialSourceFormat.JSON_STAT,
            "application/json",
            (
                b'{"class":"dataset","id":["time"],"size":[1],'
                b'"dimension":{"time":{"category":{"index":['
                b'"2026-07"]}}},"value":[0.0]}'
            ),
        ),
        (
            OfficialSourceFormat.HTML,
            "text/html",
            b"<html><body><h1>Production in construction stable</h1></body></html>",
        ),
        (
            OfficialSourceFormat.PDF,
            "application/pdf",
            _text_pdf_bytes(["Production in construction stable"]),
        ),
    ),
)
def test_eurostat_construction_output_adapter_dispatches_each_format(
    registry: OfficialSourceRegistryV1,
    source_format: OfficialSourceFormat,
    content_type: str,
    content: bytes,
) -> None:
    source = registry.source("ea.eurostat.construction-output")
    snapshot = _snapshot(
        registry,
        source.source_key,
        content,
        source_format=source_format,
        content_type=content_type,
    )

    records = parse_with_built_in_official_adapter(
        snapshot, source, max_events=20
    )

    assert records
    assert {item["parser_id"] for item in records} == {
        "official.eurostat-construction-output.v1"
    }


@pytest.mark.parametrize(
    ("source_format", "content_type", "content"),
    (
        (
            OfficialSourceFormat.ATOM,
            "application/atom+xml",
            (
                b'<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
                b"<title>Volume of retail trade down by 0.6%</title>"
                b"<id>4-04092026-ap</id></entry></feed>"
            ),
        ),
        (
            OfficialSourceFormat.JSON_STAT,
            "application/json",
            (
                b'{"class":"dataset","id":["time"],"size":[1],'
                b'"dimension":{"time":{"category":{"index":['
                b'"2026-07"]}}},"value":[0.0]}'
            ),
        ),
        (
            OfficialSourceFormat.HTML,
            "text/html",
            b"<html><body><h1>Volume of retail trade down by 0.6%</h1></body></html>",
        ),
        (
            OfficialSourceFormat.PDF,
            "application/pdf",
            _text_pdf_bytes(["Volume of retail trade down by 0.6%"]),
        ),
    ),
)
def test_eurostat_retail_trade_adapter_dispatches_each_format(
    registry: OfficialSourceRegistryV1,
    source_format: OfficialSourceFormat,
    content_type: str,
    content: bytes,
) -> None:
    source = registry.source("ea.eurostat.retail-trade")
    snapshot = _snapshot(
        registry,
        source.source_key,
        content,
        source_format=source_format,
        content_type=content_type,
    )

    records = parse_with_built_in_official_adapter(
        snapshot, source, max_events=20
    )

    assert records
    assert {item["parser_id"] for item in records} == {
        "official.eurostat-retail-trade.v1"
    }


def test_generic_json_and_csv_emit_deterministic_raw_provenance(
    registry: OfficialSourceRegistryV1,
) -> None:
    content = (
        b'{"status":"REQUEST_SUCCEEDED","Results":{"series":['
        b'{"seriesID":"CUSR0000SA0","data":[{"year":"2026",'
        b'"period":"M08","value":"326.149"}]}]}}'
    )
    snapshot = _snapshot(
        registry,
        "us.bls.public-data",
        content,
        content_type="application/json",
    )
    source = registry.source("us.bls.public-data")
    first = parse_with_built_in_official_adapter(
        snapshot, source, max_events=10
    )
    second = parse_with_built_in_official_adapter(
        snapshot, source, max_events=10
    )
    assert first == second
    assert len(first) == 2
    assert first[0]["source_locator"] == "$/Results/series/0"
    assert first[1]["source_locator"] == "$/Results/series/0/data/0"
    assert first[0]["snapshot_id"] == snapshot.snapshot_id
    assert first[0]["content_sha256"] == snapshot.content_sha256
    assert first[0]["schema_version"] == OFFICIAL_ADAPTER_RECORD_SCHEMA_VERSION
    restored = OfficialAdapterRecordV1.restore(first[0], snapshot)
    assert restored.to_dict() == first[0]
    with pytest.raises(ValueError, match="differs"):
        OfficialAdapterRecordV1.restore(
            {**first[0], "content_sha256": "0" * 64}, snapshot
        )

    csv_snapshot = _snapshot(
        registry,
        "dk.dst.statbank",
        b"TIME,VALUE\n2026M07,101.4\n",
        source_format=OfficialSourceFormat.CSV,
        content_type="text/csv",
    )
    records = parse_with_built_in_official_adapter(
        csv_snapshot,
        registry.source("dk.dst.statbank"),
        max_events=10,
    )
    assert records[0]["kind"] == OfficialAdapterRecordKind.DELIMITED_ROW.value
    assert records[0]["fields"]["values"] == {
        "TIME": "2026M07",
        "VALUE": "101.4",
    }


def test_delimited_parser_fails_explicitly_on_header_or_row_drift(
    registry: OfficialSourceRegistryV1,
) -> None:
    parser = built_in_official_source_parsers()["official.csv.v1"]
    duplicate = _snapshot(
        registry,
        "cz.cnb.arad",
        b"date,date\n2026-09-01,4.25\n",
        content_type="text/csv",
    )
    with pytest.raises(OfficialParserError) as raised:
        parse_official_snapshot(duplicate, parser, max_events=10)
    assert raised.value.code is OfficialParserFailureCode.SCHEMA_DRIFT

    ragged = _snapshot(
        registry,
        "cz.cnb.arad",
        b"date,value\n2026-09-01\n",
        content_type="text/csv",
    )
    with pytest.raises(OfficialParserError, match="row width"):
        parse_official_snapshot(ragged, parser, max_events=10)


def test_json_stat_dense_and_sparse_datasets_are_decoded(
    registry: OfficialSourceRegistryV1,
) -> None:
    dense = b"""{
      "version":"2.0","class":"dataset","label":"CPI",
      "id":["contents","time"],"size":[1,2],
      "dimension":{
        "contents":{"category":{"index":{"CPI":0}}},
        "time":{"category":{"index":["2026M07","2026M08"]}}
      },
      "value":[101.2,101.4],"status":[null,"p"]
    }"""
    snapshot = _snapshot(
        registry,
        "no.ssb.pxweb",
        dense,
        content_type="application/json",
    )
    records = parse_with_built_in_official_adapter(
        snapshot, registry.source("no.ssb.pxweb"), max_events=10
    )
    assert [item["fields"]["value"] for item in records] == [101.2, 101.4]
    assert records[1]["fields"]["coordinates"] == {
        "contents": "CPI",
        "time": "2026M08",
    }
    assert records[1]["fields"]["status"] == "p"

    sparse = dense.replace(b'"value":[101.2,101.4]', b'"value":{"1":101.4}')
    sparse_snapshot = replace(snapshot, content=sparse)
    sparse_records = parse_with_built_in_official_adapter(
        sparse_snapshot, registry.source("no.ssb.pxweb"), max_events=10
    )
    assert [item["fields"]["value"] for item in sparse_records] == [
        None,
        101.4,
    ]


def test_json_stat_shape_drift_is_an_explicit_parser_error(
    registry: OfficialSourceRegistryV1,
) -> None:
    content = (
        b'{"class":"dataset","id":["time"],"size":[2],'
        b'"dimension":{"time":{"category":{"index":["a","b"]}}},'
        b'"value":[1]}'
    )
    snapshot = _snapshot(registry, "no.ssb.pxweb", content)
    with pytest.raises(OfficialParserError) as raised:
        parse_with_built_in_official_adapter(
            snapshot, registry.source("no.ssb.pxweb"), max_events=10
        )
    assert raised.value.code is OfficialParserFailureCode.SCHEMA_DRIFT
    assert "dense value count" in str(raised.value)


def test_sdmx_csv_xml_dataflow_and_codelist_shapes(
    registry: OfficialSourceRegistryV1,
) -> None:
    csv_snapshot = _snapshot(
        registry,
        "ea.ecb.data-portal",
        (
            b"KEY,FREQ,CURRENCY,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n"
            b"EXR.D.USD.EUR.SP00.A,D,USD,2026-09-11,1.1732,A\n"
        ),
        content_type="text/csv",
    )
    records = parse_with_built_in_official_adapter(
        csv_snapshot, registry.source("ea.ecb.data-portal"), max_events=10
    )
    assert (
        records[0]["kind"] == OfficialAdapterRecordKind.SDMX_OBSERVATION.value
    )
    assert records[0]["fields"]["dimensions"]["OBS_VALUE"] == "1.1732"

    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <message:GenericData
      xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
      xmlns:generic="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic">
      <message:DataSet>
        <generic:Series>
          <generic:SeriesKey>
            <generic:Value id="FREQ" value="M"/>
            <generic:Value id="REF_AREA" value="EA"/>
          </generic:SeriesKey>
          <generic:Obs>
            <generic:ObsDimension value="2026-08"/>
            <generic:ObsValue value="2.0"/>
          </generic:Obs>
        </generic:Series>
      </message:DataSet>
    </message:GenericData>"""
    xml_records = parse_with_built_in_official_adapter(
        replace(csv_snapshot, content=xml, content_type="application/xml"),
        registry.source("ea.ecb.data-portal"),
        max_events=10,
    )
    assert xml_records[0]["fields"]["dimensions"] == {
        "FREQ": "M",
        "REF_AREA": "EA",
        "ObsDimension": "2026-08",
        "ObsValue": "2.0",
    }

    dataflow = b"""<mes:Structure
      xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
      xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
      xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
      <mes:Structures><str:Dataflows>
        <str:Dataflow id="EXR" agencyID="ECB" version="1.0">
          <com:Name xml:lang="en">Exchange rates</com:Name>
        </str:Dataflow>
      </str:Dataflows></mes:Structures>
    </mes:Structure>"""
    flow_records = parse_with_built_in_official_adapter(
        replace(csv_snapshot, content=dataflow, content_type="application/xml"),
        registry.source("ea.ecb.data-portal"),
        max_events=10,
    )
    assert (
        flow_records[0]["kind"] == OfficialAdapterRecordKind.SDMX_DATAFLOW.value
    )
    assert flow_records[0]["fields"]["id"] == "EXR"

    codelist = b"""<mes:Structure
      xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
      xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
      xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
      <mes:Structures><str:Codelists><str:Codelist id="CL_FREQ">
        <str:Code id="M"><com:Name>Monthly</com:Name></str:Code>
      </str:Codelist></str:Codelists></mes:Structures>
    </mes:Structure>"""
    code_records = parse_with_built_in_official_adapter(
        replace(csv_snapshot, content=codelist, content_type="application/xml"),
        registry.source("ea.ecb.data-portal"),
        max_events=10,
    )
    assert code_records[0]["kind"] == OfficialAdapterRecordKind.SDMX_CODE.value
    assert code_records[0]["fields"]["codelist_id"] == "CL_FREQ"
    assert code_records[0]["fields"]["id"] == "M"


def test_sdmx_json_and_sdmx_30_json_stat_are_supported(
    registry: OfficialSourceRegistryV1,
) -> None:
    sdmx_json = b"""{
      "dataSets":[{"series":{"0":{"observations":{"0":[2.0,"A"]}}}}],
      "structure":{"dimensions":{
        "series":[{"id":"REF_AREA","values":[{"id":"EA"}]}],
        "observation":[{"id":"TIME_PERIOD","values":[{"id":"2026-08"}]}]
      }}
    }"""
    snapshot = _snapshot(
        registry,
        "ea.ecb.data-portal",
        sdmx_json,
        content_type="application/vnd.sdmx.data+json",
    )
    records = parse_with_built_in_official_adapter(
        snapshot, registry.source("ea.ecb.data-portal"), max_events=10
    )
    assert records[0]["fields"] == {
        "attributes": ["A"],
        "dimensions": {"REF_AREA": "EA", "TIME_PERIOD": "2026-08"},
        "value": 2.0,
    }

    json_stat = b"""{
      "version":"2.0","class":"dataset","id":["time"],"size":[1],
      "dimension":{"time":{"category":{"index":["2026M08"]}}},
      "value":[2.1]
    }"""
    eurostat = _snapshot(
        registry,
        "ea.eurostat.dissemination",
        json_stat,
        source_format=OfficialSourceFormat.JSON_STAT,
        content_type="application/json",
    )
    eurostat_records = parse_with_built_in_official_adapter(
        eurostat,
        registry.source("ea.eurostat.dissemination"),
        max_events=10,
    )
    assert eurostat_records[0]["fields"]["value"] == 2.1


def test_sdmx_schema_drift_never_falls_back_to_generic_json(
    registry: OfficialSourceRegistryV1,
) -> None:
    snapshot = _snapshot(
        registry,
        "ea.ecb.data-portal",
        b'{"unexpected":[{"value":2.0}]}',
        content_type="application/vnd.sdmx.data+json",
    )
    with pytest.raises(OfficialParserError) as raised:
        parse_with_built_in_official_adapter(
            snapshot, registry.source("ea.ecb.data-portal"), max_events=10
        )
    assert raised.value.code is OfficialParserFailureCode.SCHEMA_DRIFT
    assert "unrecognized SDMX-JSON" in str(raised.value)


def test_xlsx_rows_bind_sheet_layout_and_header_version(
    registry: OfficialSourceRegistryV1,
) -> None:
    content = _xlsx_bytes(
        [["RBNZ statistics"], ["Period", "OCR"], ["2026-08", 5.25]]
    )
    snapshot = _snapshot(
        registry,
        "nz.rbnz.statistics",
        content,
        content_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    parser = OfficialSpreadsheetParserV1(
        (OfficialSpreadsheetLayoutV1(sheet_name="Series", header_row=2),)
    )
    records = parse_official_snapshot(snapshot, parser, max_events=10)
    assert records[0]["fields"]["values"] == {
        "Period": "2026-08",
        "OCR": 5.25,
    }
    assert records[0]["fields"]["header_row"] == 2
    header_version = records[0]["fields"]["header_version"]
    assert len(header_version) == 64
    assert records[0]["parser_configuration"]["layouts"][0][
        "layout_id"
    ].startswith("official-spreadsheet-layout:sha256:")

    drift_parser = OfficialSpreadsheetParserV1(
        (
            OfficialSpreadsheetLayoutV1(
                sheet_name="Series",
                header_row=2,
                expected_header_version="0" * 64,
            ),
        )
    )
    with pytest.raises(OfficialParserError, match="header version"):
        parse_official_snapshot(snapshot, drift_parser, max_events=10)


def test_default_xlsx_parser_auto_detects_first_tabular_header(
    registry: OfficialSourceRegistryV1,
) -> None:
    snapshot = _snapshot(
        registry,
        "nz.rbnz.statistics",
        _xlsx_bytes([["title"], ["Period", "Value"], ["2026Q2", 101.0]]),
    )
    records = parse_with_built_in_official_adapter(
        snapshot, registry.source("nz.rbnz.statistics"), max_events=10
    )
    assert records[0]["source_locator"] == "Series!R3"
    assert records[0]["parser_configuration"]["header_selection"] == (
        "first-tabular-row-v1"
    )


def test_legacy_xls_workbook_is_extracted_without_conversion(
    registry: OfficialSourceRegistryV1,
) -> None:
    fixture = (
        "H4sIAAAAAAAC/7twXvDBwo1SDxnQgB0DM8O//5wMbEhijEDMCeMIMADl//8H"
        "MWE0BxD/HwVDCnByACOSjZVhN+8ZdlAcguL7IQMTwwaWg0CSgeEREMcwFD"
        "D45eelKtAROIHdkMgIcoMtkGRkmAMU4WOQBLtKCEwmg0lhMLkerHIPmHQAi/"
        "SCSVug2geMUQzn7f20LKCpOIJJCSzHxwAydztYzy2wiCGDKMMJUCqun8AIU"
        "cvK4FiUmZgzOCXkWHgYljAA4809NS+1KDHnAYMIMAKXMHz9r8DA8AWWUw8"
        "ojIrTV5yRASj+A1WcHYv4ZCYWBoYGhv8J4ATeCkyQx5ghmTA4tSgztfgPgy"
        "IDSIAZkjEDgIL5KUCOv3MQMNKNDIzMdA0suEClMjgXC6DkYl5w6uYBkikM/"
        "GC2IDiNCwDL6T8rP17yTQqwjweLNIBLbkj5rgpyEsN/hkaQDqBmPrAME5zU"
        "ApPaYLIJbKo0mC0FJkWAKRNIqwWIQhluzWA1LWBZNaA9xmBw2V4dia0BZLe"
        "+Dtwh1/rYXhPIXuf+sEpk3XX7hQxKwPomBagfBJsZdBh1GGfNBIGd9jCaEVo"
        "W3AWTkhjlAgeTANTt/6GVGD/DXwYuMFMQTEJ4oNBhhKpnxKKeEaweZGIdExf"
        "YPYIM5sBgs2MSYtgGDh4HpJqSi2EUjIJRMApGwSgYBaNgFIyCwQgYoe16UB"
        "cD1PJnhfYe2KHjOn+B+N/oMMmwBUEM+UBYAuyYujLkAekihkqS0o8YAysjz"
        "CxGIvXAxgtBIBxoexFDNkMS2B3ZJKdfYO+PEdk/RGsUoF4WItX+f6S4k8b2"
        "AwCFFsd2ABYAAA=="
    )
    content = gzip.decompress(base64.b64decode(fixture))
    snapshot = _snapshot(
        registry,
        "nz.rbnz.statistics",
        content,
        source_format=OfficialSourceFormat.XLS,
        content_type="application/vnd.ms-excel",
    )
    records = parse_official_snapshot(
        snapshot, OfficialSpreadsheetParserV1(), max_events=10
    )
    assert records[0]["fields"]["sheet_name"] == "Series"
    assert records[0]["fields"]["values"] == {
        "Period": "2026-08",
        "OCR": 5.25,
    }


def test_html_release_index_emits_document_table_link_and_time(
    registry: OfficialSourceRegistryV1,
) -> None:
    content = b"""<!doctype html><html><head>
      <title>Federal Reserve Board - Calendar</title>
      <meta name="description" content="FOMC calendars and information"/>
      </head><body><h1>2026 FOMC Meetings</h1>
      <time datetime="2026-09-16T18:00:00Z">September 16</time>
      <table><tr><th>Date</th><th>Release</th></tr>
      <tr><td>September 16</td><td>FOMC statement</td></tr></table>
      <a href="/newsevents/pressreleases/monetary20260916a.htm">Statement</a>
      </body></html>"""
    snapshot = _snapshot(
        registry,
        "us.frb.fomc",
        content,
        content_type="text/html",
    )
    records = parse_with_built_in_official_adapter(
        snapshot, registry.source("us.frb.fomc"), max_events=10
    )
    assert [item["kind"] for item in records] == [
        OfficialAdapterRecordKind.HTML_DOCUMENT.value,
        OfficialAdapterRecordKind.HTML_TABLE_ROW.value,
        OfficialAdapterRecordKind.HTML_LINK.value,
    ]
    assert records[0]["fields"]["times"] == [
        {"datetime": "2026-09-16T18:00:00Z", "text": "September 16"}
    ]
    assert records[1]["fields"]["values"]["Release"] == "FOMC statement"
    assert records[2]["fields"]["uri"].startswith("https://")


@pytest.mark.parametrize(
    ("source_format", "parser_id", "content", "expected"),
    (
        (
            OfficialSourceFormat.RSS,
            "official.release_feed.v1",
            b"""<rss version="2.0"><channel><title>Eurostat</title><item>
            <guid>estat-2026-09-01</guid><title>GDP release</title>
            <link>https://ec.europa.eu/eurostat/news/gdp</link>
            <pubDate>Tue, 01 Sep 2026 09:00:00 GMT</pubDate>
            </item></channel></rss>""",
            "GDP release",
        ),
        (
            OfficialSourceFormat.ATOM,
            "official.release_feed.v1",
            b"""<feed xmlns="http://www.w3.org/2005/Atom"><title>Releases</title>
            <entry><id>release-1</id><title>CPI release</title>
            <updated>2026-09-01T09:00:00Z</updated>
            <link href="https://example.gov/cpi"/></entry></feed>""",
            "CPI release",
        ),
        (
            OfficialSourceFormat.ICS,
            "official.release_feed.v1",
            (
                b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\n"
                b"UID:release-1\r\nDTSTART;TZID=Europe/Brussels:20260916T110000\r\n"
                b"SUMMARY:Eurostat CPI release\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
            ),
            "Eurostat CPI release",
        ),
    ),
)
def test_release_feed_protocols_preserve_fields_and_parameters(
    registry: OfficialSourceRegistryV1,
    source_format: OfficialSourceFormat,
    parser_id: str,
    content: bytes,
    expected: str,
) -> None:
    snapshot = _snapshot(
        registry,
        "ea.eurostat.dissemination",
        content,
        source_format=source_format,
        parser_id=parser_id,
        content_type=(
            "text/calendar"
            if source_format is OfficialSourceFormat.ICS
            else "application/xml"
        ),
    )
    records = parse_official_snapshot(
        snapshot, OfficialReleaseFeedParserV1(), max_events=10
    )
    assert (
        records[0]["kind"] == OfficialAdapterRecordKind.RELEASE_FEED_ITEM.value
    )
    if source_format is OfficialSourceFormat.ICS:
        assert records[0]["fields"]["summary"] == expected
        assert records[0]["fields"]["dtstart"] == {
            "value": "20260916T110000",
            "parameters": {"tzid": "Europe/Brussels"},
        }
    else:
        assert records[0]["fields"]["title"] == expected


def test_feed_schema_drift_is_explicit(
    registry: OfficialSourceRegistryV1,
) -> None:
    snapshot = _snapshot(
        registry,
        "ea.eurostat.dissemination",
        b"<rss><channel><item><guid>missing-title</guid></item></channel></rss>",
        source_format=OfficialSourceFormat.RSS,
        parser_id="official.release_feed.v1",
        content_type="application/xml",
    )
    with pytest.raises(OfficialParserError) as raised:
        parse_official_snapshot(
            snapshot, OfficialReleaseFeedParserV1(), max_events=10
        )
    assert raised.value.code is OfficialParserFailureCode.SCHEMA_DRIFT

    entity_snapshot = replace(
        snapshot,
        content=(
            b'<!DOCTYPE rss [<!ENTITY x "boom">]>'
            b"<rss><channel><item><title>&x;</title></item></channel></rss>"
        ),
    )
    with pytest.raises(OfficialParserError) as entity_error:
        parse_official_snapshot(
            entity_snapshot, OfficialReleaseFeedParserV1(), max_events=10
        )
    assert entity_error.value.code is (
        OfficialParserFailureCode.MALFORMED_DOCUMENT
    )


def test_pdf_parser_extracts_text_and_table_like_rows(
    registry: OfficialSourceRegistryV1,
) -> None:
    snapshot = _snapshot(
        registry,
        "us.frb.fomc",
        _text_pdf_bytes(["Release Date  Value", "2026-09-16  4.25"]),
        source_format=OfficialSourceFormat.PDF,
        content_type="application/pdf",
    )
    records = parse_with_built_in_official_adapter(
        snapshot, registry.source("us.frb.fomc"), max_events=10
    )
    assert records[0]["kind"] == OfficialAdapterRecordKind.PDF_PAGE.value
    assert "2026-09-16" in records[0]["fields"]["text"]
    assert ["Release Date", "Value"] in records[0]["fields"]["table_rows"]


def test_image_only_pdf_records_reacquisition_requirement(
    registry: OfficialSourceRegistryV1,
) -> None:
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    buffer = io.BytesIO()
    writer.write(buffer)
    snapshot = _snapshot(
        registry,
        "us.frb.fomc",
        buffer.getvalue(),
        source_format=OfficialSourceFormat.PDF,
        parser_id="official.pdf.v1",
        content_type="application/pdf",
    )
    records = parse_official_snapshot(
        snapshot, OfficialPdfParserV1(), max_events=10
    )
    assert records[0]["kind"] == (
        OfficialAdapterRecordKind.REACQUISITION_REQUIREMENT.value
    )
    assert records[0]["fields"]["reason"] == "image-only-or-empty-pdf"


def test_static_archive_html_json_and_zip_enumeration(
    registry: OfficialSourceRegistryV1,
) -> None:
    parser = OfficialArchiveParserV1()
    html_snapshot = _snapshot(
        registry,
        "au.rba.statistics",
        b'<html><body><a href="historical/f01hist.xlsx">F1 history</a></body></html>',
        source_format=OfficialSourceFormat.HTML,
        parser_id=parser.parser_id,
        content_type="text/html",
    )
    html_records = parse_official_snapshot(html_snapshot, parser, max_events=10)
    assert html_records[0]["fields"]["uri"].endswith("historical/f01hist.xlsx")

    json_snapshot = replace(
        html_snapshot,
        request=replace(
            html_snapshot.request, source_format=OfficialSourceFormat.JSON
        ),
        content=b'{"downloads":[{"url":"archive/cpi-2026.csv"}]}',
        content_type="application/json",
    )
    json_records = parse_official_snapshot(json_snapshot, parser, max_events=10)
    assert json_records[0]["source_locator"] == "$/downloads/0/url"

    archive_buffer = io.BytesIO()
    import zipfile

    with zipfile.ZipFile(archive_buffer, "w") as archive:
        archive.writestr("cpi/2026-08.csv", "period,value\n2026-08,101.4\n")
    zip_snapshot = replace(
        html_snapshot,
        request=replace(
            html_snapshot.request, source_format=OfficialSourceFormat.ARCHIVE
        ),
        content=archive_buffer.getvalue(),
        content_type="application/zip",
    )
    zip_records = parse_official_snapshot(zip_snapshot, parser, max_events=10)
    assert zip_records[0]["fields"]["name"] == "cpi/2026-08.csv"
    assert zip_records[0]["fields"]["crc32"]


def test_data_catalog_json_and_xml_discovery(
    registry: OfficialSourceRegistryV1,
) -> None:
    parser = OfficialDataCatalogParserV1()
    json_snapshot = _snapshot(
        registry,
        "ea.eurostat.dissemination",
        (
            b'{"result":{"results":[{"id":"prc_hicp_midx",'
            b'"title":"HICP monthly data","resources":['
            b'{"url":"https://ec.europa.eu/eurostat/api/data"}]}]}}'
        ),
        source_format=OfficialSourceFormat.DATA_CATALOG,
        parser_id=parser.parser_id,
        content_type="application/json",
    )
    records = parse_official_snapshot(json_snapshot, parser, max_events=10)
    assert records[0]["fields"]["id"] == "prc_hicp_midx"

    xml_snapshot = replace(
        json_snapshot,
        content=(
            b'<dcat:Catalog xmlns:dcat="http://www.w3.org/ns/dcat#">'
            b'<dcat:Dataset id="cpi"><dcat:title>CPI</dcat:title>'
            b"<dcat:distribution>https://example.gov/cpi.csv</dcat:distribution>"
            b"</dcat:Dataset></dcat:Catalog>"
        ),
        content_type="application/xml",
    )
    xml_records = parse_official_snapshot(xml_snapshot, parser, max_events=10)
    assert xml_records[0]["fields"] == {
        "distribution": "https://example.gov/cpi.csv",
        "id": "cpi",
        "title": "CPI",
    }


def test_parser_record_bound_is_enforced_before_silent_truncation(
    registry: OfficialSourceRegistryV1,
) -> None:
    snapshot = _snapshot(
        registry,
        "us.bls.public-data",
        b'{"rows":[{"id":1},{"id":2}]}',
        content_type="application/json",
    )
    with pytest.raises(OfficialParserError) as raised:
        parse_with_built_in_official_adapter(
            snapshot, registry.source("us.bls.public-data"), max_events=1
        )
    assert raised.value.code is OfficialParserFailureCode.RESOURCE_LIMIT


def test_record_rejects_parser_mismatch_and_non_json_fields(
    registry: OfficialSourceRegistryV1,
) -> None:
    snapshot = _snapshot(
        registry,
        "us.bls.public-data",
        b'{"ok":true}',
        content_type="application/json",
    )
    with pytest.raises(ValueError, match="parser differs"):
        OfficialAdapterRecordV1(
            kind=OfficialAdapterRecordKind.JSON_RECORD,
            snapshot=snapshot,
            parser_id="official.csv.v1",
            parser_version="1",
            ordinal=0,
            source_locator="$",
            fields={"ok": True},
        )
    with pytest.raises(ValueError, match="canonical JSON"):
        OfficialAdapterRecordV1(
            kind=OfficialAdapterRecordKind.JSON_RECORD,
            snapshot=snapshot,
            parser_id=snapshot.request.parser_id,
            parser_version="1",
            ordinal=0,
            source_locator="$",
            fields={"bad": float("nan")},
        )


def test_first_party_fixture_qualifications_complete_required_pack_audit(
    registry: OfficialSourceRegistryV1,
) -> None:
    fixture_cases = (
        (
            OfficialAdapterPack.GENERIC_STRUCTURED,
            registry.source("us.bls.public-data"),
            _snapshot(
                registry,
                "us.bls.public-data",
                b'{"Results":{"series":[{"seriesID":"CUSR0000SA0"}]}}',
                content_type="application/json",
            ),
            "https://api.bls.gov/publicAPI/v2/timeseries/data/CUSR0000SA0",
            None,
        ),
        (
            OfficialAdapterPack.HTML_RELEASE,
            registry.source("us.frb.fomc"),
            _snapshot(
                registry,
                "us.frb.fomc",
                b"<html><title>FOMC calendar</title><body>2026 meetings</body></html>",
                content_type="text/html",
            ),
            "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            None,
        ),
        (
            OfficialAdapterPack.JSON_STAT,
            registry.source("no.ssb.pxweb"),
            _snapshot(
                registry,
                "no.ssb.pxweb",
                (
                    b'{"class":"dataset","id":["time"],"size":[1],'
                    b'"dimension":{"time":{"category":{"index":['
                    b'"2026M08"]}}},"value":[101.4]}'
                ),
                content_type="application/json",
            ),
            "https://data.ssb.no/api/pxwebapi/v2/",
            None,
        ),
        (
            OfficialAdapterPack.PDF,
            registry.source("us.frb.fomc"),
            _snapshot(
                registry,
                "us.frb.fomc",
                _text_pdf_bytes(["September 2026 FOMC statement"]),
                source_format=OfficialSourceFormat.PDF,
                content_type="application/pdf",
            ),
            "https://www.federalreserve.gov/monetarypolicy/files/monetary20260916a1.pdf",
            None,
        ),
        (
            OfficialAdapterPack.RELEASE_FEED,
            registry.source("ea.eurostat.gdp"),
            _snapshot(
                registry,
                "ea.eurostat.gdp",
                (
                    b'<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
                    b"<title>GDP up by 0.3% in the euro area</title>"
                    b"<id>2-07092026-ap</id></entry></feed>"
                ),
                source_format=OfficialSourceFormat.ATOM,
                content_type="application/atom+xml",
            ),
            "https://ec.europa.eu/eurostat/search?text=GDP",
            None,
        ),
        (
            OfficialAdapterPack.SDMX,
            registry.source("ea.ecb.data-portal"),
            _snapshot(
                registry,
                "ea.ecb.data-portal",
                b"FREQ,TIME_PERIOD,OBS_VALUE\nM,2026-08,2.0\n",
                content_type="text/csv",
            ),
            "https://data-api.ecb.europa.eu/service/data/ICP/M.U2.N.000000.4.ANR",
            None,
        ),
        (
            OfficialAdapterPack.SPREADSHEET,
            registry.source("nz.rbnz.statistics"),
            _snapshot(
                registry,
                "nz.rbnz.statistics",
                _xlsx_bytes([["Period", "OCR"], ["2026-08", 5.25]]),
                content_type=(
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
            ),
            "https://www.rbnz.govt.nz/statistics/series/official-cash-rate",
            None,
        ),
        (
            OfficialAdapterPack.STATIC_ARCHIVE,
            registry.source("au.rba.statistics"),
            _snapshot(
                registry,
                "au.rba.statistics",
                b'<html><body><a href="f01hist.xlsx">F1 history</a></body></html>',
                source_format=OfficialSourceFormat.HTML,
                parser_id="official.archive.v1",
                content_type="text/html",
            ),
            "https://www.rba.gov.au/statistics/tables/",
            OfficialArchiveParserV1(),
        ),
    )
    qualifications = []
    replay_inputs = []
    for pack, source, snapshot, uri, parser in fixture_cases:
        qualification = qualify_official_adapter_fixture(
            snapshot,
            source,
            pack,
            fixture_uri=uri,
            parser=parser,
            max_events=20,
        )
        qualifications.append(qualification)
        replay_inputs.append((source, snapshot, parser, qualification))
        assert qualification.expected_record_count >= 1
        assert len(qualification.content_sha256) == 64
        assert len(qualification.normalized_sha256) == 64

    audit = audit_official_adapter_coverage(registry, qualifications)
    assert audit.complete
    assert not audit.missing_qualifications
    assert audit.to_dict()["complete"] is True
    assert type(audit).from_dict(audit.to_dict()) == audit

    for source, snapshot, parser, qualification in replay_inputs:
        records = verify_official_adapter_fixture(
            snapshot, source, qualification, parser=parser
        )
        assert len(records) == qualification.expected_record_count

    source, snapshot, parser, qualification = replay_inputs[0]
    with pytest.raises(ValueError, match="raw fixture drifted"):
        verify_official_adapter_fixture(
            replace(snapshot, content=snapshot.content + b" "),
            source,
            qualification,
            parser=parser,
        )


def test_qualification_output_is_canonical_json(
    registry: OfficialSourceRegistryV1,
) -> None:
    source = registry.source("us.bls.public-data")
    snapshot = _snapshot(
        registry,
        source.source_key,
        b'{"series":[{"id":"CUSR0000SA0"}]}',
        content_type="application/json",
    )
    qualification = qualify_official_adapter_fixture(
        snapshot,
        source,
        OfficialAdapterPack.GENERIC_STRUCTURED,
        fixture_uri="https://api.bls.gov/publicAPI/v2/timeseries/data/",
    )
    encoded = canonical_contract_json(qualification.to_dict())
    assert (
        type(qualification).from_dict(qualification.to_dict()) == qualification
    )
    assert qualification.qualification_id.startswith(
        "official-adapter-qualification:sha256:"
    )
    assert '"expected_record_count":1' in encoded
