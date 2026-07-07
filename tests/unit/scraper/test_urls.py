"""Pytest unit tests for histdatacom.scraper.urls.py."""

from histdatacom.scraper.urls import Urls


def test_urls() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_generate_form_urls_rejects_removed_m1_timeframe() -> None:
    """M1 is no longer a supported raw HistData timeframe."""
    try:
        list(
            Urls().generate_form_urls(
                "202201",
                "202203",
                {"ascii"},
                {"eurusd"},
                {"M1"},
            )
        )
    except ValueError as exc:
        assert "'M1' is not a valid Timeframe" in str(exc)
    else:
        raise AssertionError("M1 URL generation should fail")


def test_generate_form_urls_preserves_tick_month_units() -> None:
    """Tick data ranges should generate one URL per month."""
    urls = list(
        Urls().generate_form_urls(
            "202201",
            "202203",
            {"ascii"},
            {"eurusd"},
            {"T"},
        )
    )

    assert urls == [
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/1",
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/2",
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/3",
    ]


def test_generate_form_urls_is_deterministic_for_set_inputs() -> None:
    """URL generation should no longer inherit arbitrary set iteration order."""
    urls = list(
        Urls().generate_form_urls(
            "202201",
            "202201",
            {"ascii"},
            {"gbpusd", "eurusd"},
            {"T"},
        )
    )

    assert urls == [
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/1",
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/gbpusd/2022/1",
    ]
