"""Pytest unit tests for histdatacom.utils.py."""

import pytest

from histdatacom.utils import (
    SUPPORTED_API_RETURN_TYPES,
    check_installed_module,
    normalize_api_return_type,
)


def test_utils() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


@pytest.mark.parametrize(
    ("return_type", "expected"),
    (
        ("polars", "polars"),
        ("POLARS", "polars"),
        ("pandas", "pandas"),
        ("arrow", "arrow"),
        ("pyarrow", "arrow"),
        (None, None),
        ("", None),
    ),
)
def test_normalize_api_return_type(
    return_type: str | None, expected: str | None
) -> None:
    """Normalize public API return type names and aliases."""
    assert normalize_api_return_type(return_type) == expected


@pytest.mark.parametrize("return_type", ("numpy", "datatable"))
def test_normalize_api_return_type_rejects_unsupported_values(
    return_type: str,
) -> None:
    """Unsupported API return values should fail before module import checks."""
    with pytest.raises(ValueError) as err:
        normalize_api_return_type(return_type)

    assert f"unsupported api_return_type '{return_type}'" in str(err.value)
    assert "arrow, pandas, polars" in str(err.value)


def test_api_return_type_contract_is_explicit() -> None:
    """Keep the public return-type contract visible."""
    assert SUPPORTED_API_RETURN_TYPES == {
        "arrow",
        "pandas",
        "polars",
    }


def test_check_installed_module_accepts_polars_return_type() -> None:
    """Polars is now the default dataframe dependency."""
    assert check_installed_module("polars")
