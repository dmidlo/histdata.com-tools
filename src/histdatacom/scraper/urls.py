"""Generate histdata.com archive urls.

Yields:
    url (str): histdata.com archive url
"""

from typing import Any, Generator, Optional, Set

from histdatacom.activity_stages import (
    DEFAULT_HISTDATA_BASE_URL,
    plan_dataset_urls,
)


class Urls:  # noqa:H601
    """Generate histdata.com archive urls.

    Yields:
        url (str): histdata.com archive url
    """

    def __init__(self) -> None:
        """Set base_url for histdata.com archives."""
        self.base_url: str = DEFAULT_HISTDATA_BASE_URL

    def generate_form_urls(  # noqa:CCR001
        self,
        start_yearmonth: str,
        end_yearmonth: str,
        formats: set,
        pairs: Optional[Set[Any]],
        timeframes: set,
    ) -> Generator[str, None, None]:
        """Generate histdata.com urls.

        Generates permutations of user settings to yield valid urls
        for scraping.

        Args:
            start_yearmonth (str): numeric string - YYYYMM
            end_yearmonth (str): numeric string - YYYYMM
            formats (set): fx_enums.Format
            pairs (Optional[Set[Any]]): fx_enums.Pairs
            timeframes (set): fx_enums.Timeframe

        Yields:
            Generator[str, None, None]: _description_
        """
        yield from plan_dataset_urls(
            start_yearmonth=start_yearmonth,
            end_yearmonth=end_yearmonth,
            formats=formats,
            pairs=pairs,
            timeframes=timeframes,
            base_url=self.base_url,
        )
