"""Literal publication-path contracts, including repeated JSON sanitization."""

from copy import deepcopy
import json

import pytest

from histdatacom.publication_safety import (
    publish_safe_json_mapping,
    publish_safe_json_value,
    publish_safe_path,
)
from histdatacom.runtime_contracts import JSONValue


@pytest.mark.parametrize(
    ("source", "expected"),
    (
        ("/tmp/fixture/report.json", "report.json"),
        ("/private/fixture/report.json", "report.json"),
        ("/home/fixture/report.json", "report.json"),
        ("/Users/fixture/report.json", "report.json"),
        ("/var/folders/fixture/report.json", "report.json"),
        (r"C:\Users\fixture\Temp\report.json", "report.json"),
        ("file:///Users/fixture/report.json", "report.json"),
        (
            "/Users/fixture/work/.histdatacom/check/tmp/report.json",
            ".histdatacom/check/tmp/report.json",
        ),
        (
            "/Users/fixture/work/.histdatacom/check/private/report.json",
            ".histdatacom/check/private/report.json",
        ),
        (
            "/Users/fixture/work/.histdatacom/check/home/report.json",
            ".histdatacom/check/home/report.json",
        ),
        (
            "/Users/fixture/work/.histdatacom/check/Users/report.json",
            ".histdatacom/check/Users/report.json",
        ),
        (
            "/Users/fixture/work/.histdatacom/check/var/folders/report.json",
            ".histdatacom/check/var/folders/report.json",
        ),
        (
            r"C:\Users\fixture\work\.histdatacom\check\tmp\report.json",
            ".histdatacom/check/tmp/report.json",
        ),
        (
            r"C:\Users\fixture\work\.histdatacom\check\var\folders\report.json",
            ".histdatacom/check/var/folders/report.json",
        ),
    ),
    ids=(
        "absolute-tmp",
        "absolute-private",
        "absolute-home",
        "absolute-users",
        "absolute-var-folders",
        "absolute-windows",
        "absolute-file-uri",
        "anchored-tmp",
        "anchored-private",
        "anchored-home",
        "anchored-users",
        "anchored-var-folders",
        "anchored-windows-tmp",
        "anchored-windows-var-folders",
    ),
)
def test_report_name_has_literal_output_and_survives_repeated_json(
    source: str, expected: str
) -> None:
    """A path-valued report name must not become an embedded prose path."""
    assert publish_safe_path(source) == expected
    assert publish_safe_json_value(source, key="report_name") == expected
    payload: dict[str, JSONValue] = {"input_reports": [{"report_name": source}]}
    original = deepcopy(payload)
    expected_payload: dict[str, JSONValue] = {
        "input_reports": [{"report_name": expected}]
    }

    first = publish_safe_json_mapping(payload)

    assert first == expected_payload
    assert publish_safe_json_mapping(first) == expected_payload
    assert publish_safe_json_mapping(expected_payload) == expected_payload
    assert payload == original
    assert expected_payload == {"input_reports": [{"report_name": expected}]}


def test_nested_report_names_redact_hosts_without_mutating_inputs() -> None:
    """Public relative suffixes survive; actual host prefixes remain private."""
    payload: dict[str, JSONValue] = {
        "result": {
            "input_reports": [
                {
                    "report_name": (
                        "/Users/fixture/work/.histdatacom/run/tmp/report.json"
                    ),
                    "message": "loaded /private/fixture/diagnostic.json",
                },
                {
                    "report_name": (
                        r"C:\Users\fixture\work\.histdatacom\run\home\report.json"
                    ),
                    "message": r"loaded C:\Users\fixture\private\note.json",
                },
            ],
            "path": "/home/fixture/private/metadata.json",
        }
    }
    expected: dict[str, JSONValue] = {
        "result": {
            "input_reports": [
                {
                    "report_name": ".histdatacom/run/tmp/report.json",
                    "message": "loaded diagnostic.json",
                },
                {
                    "report_name": ".histdatacom/run/home/report.json",
                    "message": "loaded note.json",
                },
            ],
            "path": "metadata.json",
        }
    }
    original = deepcopy(payload)

    first = publish_safe_json_mapping(payload)

    assert first == expected
    assert publish_safe_json_mapping(first) == expected
    assert payload == original
    serialized = json.dumps(first, sort_keys=True)
    for forbidden in (
        "/Users/fixture",
        "/private/fixture",
        "/home/fixture",
        r"C:\Users\fixture",
        "C:/Users/fixture",
    ):
        assert json.dumps(forbidden)[1:-1] not in serialized


@pytest.mark.parametrize(
    ("source", "expected"),
    (
        (
            "/Users/fixture/.histdatacom/reports/tmp/report.json",
            "reports/tmp/report.json",
        ),
        (
            "/Users/fixture/reports/data/private/report.json",
            "data/private/report.json",
        ),
        (
            "https://example.invalid/Users/fixture/tmp/report.json",
            "https://example.invalid/Users/fixture/tmp/report.json",
        ),
    ),
    ids=("reports-before-histdatacom", "data-before-reports", "remote-url"),
)
def test_report_name_retains_existing_anchor_priority_and_url_policy(
    source: str, expected: str
) -> None:
    """Recognizing this field must not change the existing path policy."""
    expected_payload = {"report_name": expected}

    assert publish_safe_path(source) == expected
    first = publish_safe_json_mapping({"report_name": source})

    assert first == expected_payload
    assert publish_safe_json_mapping(first) == expected_payload
