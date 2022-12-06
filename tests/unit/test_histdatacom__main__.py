"""Pytest unit tests for histdatacom.histdatacom__main__.py."""
import pytest

import histdatacom.__main__ as histdatacom_main


def test_histdatacom__main__() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_import___main__() -> None:
    """Test histdatacom.histdata_com import."""
    assert (  # noqa:BLK100  # act
        histdatacom_main.histdata_com.__name__ == "histdatacom.histdata_com"
    )


def test___main__main() -> None:
    """Confirm Exit is SystemExit."""
    with pytest.raises(SystemExit) as exc_info:  # arrange
        histdatacom_main.main()  # act

    assert exc_info.type is SystemExit  # assert
