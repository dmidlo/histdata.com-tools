"""Pytest unit tests for histdatacom.histdatacom__init__.py."""

import sys

import histdatacom
from histdatacom import Options


def test_histdatacom__init__() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_exposed_classes() -> None:
    """Expose only what is necessary for end-user."""
    assert set(histdatacom.__all__) == {  # act
        "Options",
        "Pairs",
        "Timeframe",
        "Format",
    }


def test_masquerade_class() -> None:
    """Make histdatacom callable by extending itself with a call method."""
    assert sys.modules["histdatacom"].__class__.__name__ == "APICaller"  # act


def test_histdatacom_is_callable() -> None:
    """Ensure histdatacom's masquerade class adds __call__."""
    assert callable(histdatacom)  # act


def test_histdatacom_call() -> None:
    # pylint: disable=not-callable
    """Call histdatacom with minimal arguments."""
    options = Options()
    options.version = True

    assert histdatacom(options)  # type: ignore  # act
