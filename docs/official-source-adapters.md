# Reusable official-source protocol adapters

`histdatacom.market_context.official_adapters` supplies the protocol layer
between immutable `OfficialRawSnapshotV1` bytes and economy-specific calendar
adapters. It never performs network access. Acquisition, redirect, retry,
credential, MIME, byte, request, page, runtime, source-clock, and conditional
request policy remains in the
[official-source fetch layer](official-source-registry-and-fetch.md).

The built-ins implement every parser identity in the 73-source registry and
also expose forward-compatible feed, archive, PDF, and data-catalog parsers.
`resolve_official_source_parser()` requires the registry's exact parser ID and
version and verifies every declared format. It does not substitute another
parser or endpoint after a failure.

## Adapter packs

| Pack | Inputs | Normalized output and checks |
| --- | --- | --- |
| SDMX | SDMX 2.1/3.0 CSV, JSON, and XML | Observations with decoded series/observation dimensions; dataflows; codelist codes; required observation columns and structural-key bounds. |
| JSON-stat | Dense or sparse JSON-stat datasets | One record per declared coordinate, including null values and status; exact dimension/category/shape validation. |
| Generic structured | JSON, CSV, and TSV | Deterministic JSON Pointers for record arrays; strict UTF-8 delimited headers and row widths; header fingerprints. |
| Spreadsheet | XLS and XLSX | Sheet/row locators, replayable configured or automatic header selection, header fingerprints, bounded sheets/columns/cells, and formula-preserving XLSX reads. |
| HTML/text release | Official indexes and press releases | Plain text is retained as one bounded UTF-8 document; HTML retains document title, metadata, headings, time elements and text, strict table rows, and resolved links. Registered release sources can delegate their text, CSV, XLS/XLSX, or PDF variants without changing parser identity. |
| Release feed | RSS, Atom, and ICS | Feed item/event properties, Atom link attributes, iCalendar parameters and unfolded lines; required title or UID/start fields. |
| PDF | Official text-bearing PDFs | Page text and conservative whitespace-delimited table rows. Encrypted or image-only documents emit an explicit reacquisition/OCR requirement rather than empty success. |
| Static archive | HTML/JSON directories and ZIP files | Resolved download links or ZIP member name, sizes, CRC, and directory state without extraction. |
| Data catalog | JSON or XML/DCAT-style feeds | Dataset-shaped records with exact source locators; an unrecognized catalog shape fails as drift. |

`defusedxml`, `openpyxl`, `xlrd`, and `pypdf` are runtime dependencies because
hostile XML refusal, XLSX, legacy XLS, and PDF are production input concerns
rather than optional display features. Archive enumeration does not extract
member bytes and therefore does not expose path traversal or decompression as
a side effect.

## Record provenance

Every emitted `OfficialAdapterRecordV1` contains:

- source key and immutable source ID;
- request and snapshot IDs;
- exact raw content SHA-256;
- selected source format;
- parser ID/version;
- zero-based normalized ordinal;
- a JSON Pointer, XML element path, workbook sheet/row, PDF page, calendar
  line, or archive member locator;
- replay-relevant parser configuration; and
- the normalized fields.

The record ID hashes all of those values using canonical JSON. A downstream
economy adapter can therefore reproduce a value from the retained snapshot,
and a parser/configuration change creates new lineage. Spreadsheet layouts
serialize the selected sheet/header row and an optional expected header hash.
The automatic layout mode is itself named and retained, so it is not implicit.

```python
from histdatacom.market_context import (
    parse_with_built_in_official_adapter,
)

records = parse_with_built_in_official_adapter(
    snapshot,
    registry.source(snapshot.request.source_key),
    max_events=policy.max_events,
)
```

Parsing remains bounded twice: each pack stops before exceeding
`max_events`, and `parse_official_snapshot()` independently enforces the
protocol seam's mapping, count, and canonical record-size limits.

## Drift and refusal

`OfficialParserError` has a stable `OfficialParserFailureCode`, parser
identity, source key, and source locator. The built-ins distinguish malformed
documents, schema drift, resource limits, unsupported representations, and
extraction failures. Examples of drift include duplicate headers, ragged CSV
or HTML tables, JSON-stat size mismatches, invalid SDMX dimension keys, missing
feed identity fields, absent workbook sheets, and changed expected header
hashes.

There is no generic-parser fallback after a protocol parser rejects input.
For example, an arbitrary JSON object received by an SDMX request is reported
as SDMX schema drift; it is not accepted merely because it is valid JSON.

PDF is the deliberate exception to error-only refusal. When exact bytes are a
valid but encrypted or image-only official PDF, the parser emits
`reacquisition-requirement`. This preserves why normalized values could not be
reproduced and the precise authorized artifact or OCR evidence needed next.

## Qualification and registry audit

`qualify_official_adapter_fixture()` parses a pinned first-party snapshot and
records its source/format/parser binding, raw hash, canonical normalized hash,
and expected record count. `verify_official_adapter_fixture()` checks both raw
and normalized hashes. A changed official fixture, parser result, or count is
an explicit failure.

`required_official_adapter_packs()` derives the current requirement from the
registry format matrix and declared archive routes. The current registry needs
SDMX, JSON-stat, generic structured, spreadsheet, HTML release, release-feed,
PDF, and static-archive support. The Eurostat GDP, monthly-unemployment, and
monthly-industrial-production sources make Atom a reviewed registry input. The
qualified release-feed pack
covers RSS and ICS; the data-catalog parser remains installed but does not
become a qualification requirement until a reviewed registry entry declares
that format.

`audit_official_adapter_coverage()` reports:

- every missing source/parser/format/version binding;
- all nine available built-in packs; and
- any registry-required pack without a pinned qualification.

The unit qualification corpus uses reduced responses with the real structural
shapes and official fixture URIs for BLS JSON, ECB SDMX, Eurostat GDP,
monthly-unemployment, and monthly-industrial-production Atom/JSON-stat inputs,
Statistics Norway JSON-stat, RBNZ
spreadsheets, Federal Reserve HTML/PDF, and RBA archive indexes. Separate
fixtures cover SDMX
dataflow/codelist discovery, SDMX-JSON, RSS, ICS, ZIP enumeration, catalog
discovery, malformed input, drift, record bounds, and PDF reacquisition.

## Capability and vintage boundary

Parsing a value does not upgrade source capability. The source registry still
declares current values, historical observations, publication/schedule/
revision metadata, series metadata, forecast/survey support, archive
enumeration, and historical vintages independently. A latest-state API remains
useful for series discovery, but it cannot be projected backward.

Historical-vintage claims require both the registry capability and empirical
evidence or a contemporaneous artifact accepted by the
[archive-first vintage layer](archive-vintage-reconstruction.md). The protocol
record proves what a retained response contained; it does not prove that the
response existed at an earlier decision time.
