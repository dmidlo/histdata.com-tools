"""Pytest unit tests for histdatacom.scraper.urls.py."""

from histdatacom.scraper.urls import Urls


def test_urls() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_generate_form_urls_preserves_historical_m1_year_units() -> None:
    """Historical M1 ranges should generate yearly archive URLs."""
    urls = list(
        Urls().generate_form_urls(
            "202201",
            "202203",
            {"ascii"},
            {"eurusd"},
            {"M1"},
        )
    )

    assert urls == [
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/1-minute-bar-quotes/eurusd/2022"
    ]


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
            {"M1"},
        )
    )

    assert urls == [
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/1-minute-bar-quotes/eurusd/2022",
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/1-minute-bar-quotes/gbpusd/2022",
    ]
