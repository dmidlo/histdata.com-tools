"""Generate histdata.com archive urls.

Yields:
    url (str): histdata.com archive url
"""
from typing import Any, Generator, Optional, Set

from histdatacom.fx_enums import Timeframe, get_valid_format_timeframes
from histdatacom.utils import (
    get_current_datemonth_gmt_minus5,
    get_year_from_datemonth,
    get_month_from_datemonth,
)


class Urls:  # noqa:H601
    """Generate histdata.com archive urls.

    Yields:
        url (str): histdata.com archive url
    """

    def __init__(self) -> None:
        """Set base_url for histdata.com archives."""
        self.base_url: str = "http://www.histdata.com/download-free-forex-data/"

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
        current_yearmonth = get_current_datemonth_gmt_minus5()
        current_year = int(get_year_from_datemonth(current_yearmonth))

        if start_yearmonth is None and end_yearmonth is None:  # type: ignore
            # pylint: disable-next=line-too-long
            start_yearmonth, end_yearmonth = "200001", current_yearmonth  # type: ignore # noqa:LN002

        for sub_url, timeframe in self._valid_format_timeframe_pair_urls(
            formats, timeframes, pairs
        ):
            form_url = f"{self.base_url}?/{sub_url}"

            if end_yearmonth is None:
                for date_url in self._yield_single_year_or_month(  # type: ignore
                    timeframe, start_yearmonth
                ):
                    yield f"{form_url}{date_url}"
            else:
                start_year = int(get_year_from_datemonth(start_yearmonth))
                start_month = int(  # noqa:BLK100
                    get_month_from_datemonth(start_yearmonth)
                )
                end_year = int(get_year_from_datemonth(end_yearmonth))
                end_month = int(get_month_from_datemonth(end_yearmonth))

                # pylint: disable=not-an-iterable
                for year in range(start_year, end_year + 1):
                    yield from self._yield_range_of_yearmonths(
                        year,
                        timeframe,
                        form_url,
                        start_year,
                        start_month,
                        end_year,
                        end_month,
                        current_year,
                    )

    def _valid_format_timeframe_pair_urls(  # noqa:CCR001
        self, formats: set, timeframes: set, pairs: Optional[Set[Any]]
    ) -> Generator[tuple[str, Any], None, None]:
        """Yield permutations of format, timeframe, and pair as url fragments.

        Args:
            formats (set): fx_enums.Format
            pairs (Optional[Set[Any]]): fx_enums.Pairs
            timeframes (set): fx_enums.Timeframe

        Yields:
            Generator[tuple[str, Any], None, None]: {format}/{timeframe}/{pair}/
        """
        for csv_format in formats:
            for timeframe in timeframes:
                if timeframe in get_valid_format_timeframes(csv_format):
                    for pair in pairs:  # type: ignore
                        yield (
                            f"{csv_format}/"  # noqa:BLK001
                            f"{Timeframe[timeframe].value}/"
                            f"{pair}/"
                        ), timeframe

    def _yield_range_of_yearmonths(  # noqa:CFQ002
        self,
        year: int,
        timeframe: str,
        form_url: str,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        current_year: int,
    ) -> Generator[str, None, None]:
        """Generate final urls.

        Generate final urls, handling year/month edge cases.

        Args:
            year (int): current year in range.
            timeframe (str): fx_enums.Timeframe
            form_url (str): url fragment {base_url}/{format}/{timeframe}/{pair}
            start_year (int): YYYY
            start_month (int): MM
            end_year (int): YYYY
            end_month (int): MM
            current_year (int): YYYY

        Yields:
            Generator[str, None, None]:
                {base_url}/{format}/{timeframe}/{pair}/{year}/{month}
        """
        match year:
            case _ if year == current_year:
                for date_url in self._yield_current_year(
                    year, start_year, start_month, end_year, end_month
                ):
                    yield f"{form_url}{date_url}"

            case _ if start_year == year == end_year:
                for (
                    date_url
                ) in self._yield_same_year(  # pylint: disable=not-an-iterable
                    timeframe, year, start_month, end_month
                ):
                    yield f"{form_url}{date_url}"

            case _ if year == start_year != end_year:
                for (
                    date_url
                ) in self._yield_start_year(  # pylint: disable=not-an-iterable
                    timeframe, year, start_month
                ):
                    yield f"{form_url}{date_url}"

            case _ if year == end_year != start_year:
                for (  # noqa:BLK001
                    date_url
                ) in self._yield_end_year(  # pylint: disable=not-an-iterable
                    timeframe, year, end_month
                ):
                    yield f"{form_url}{date_url}"
            case _:
                for (  # noqa:BLK001
                    date_url
                ) in self._yield_year(  # pylint: disable=not-an-iterable
                    timeframe, year
                ):
                    yield f"{form_url}{date_url}"

    def _yield_current_year(
        self,
        year: int,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
    ) -> Generator[str, None, None]:
        """Handle edge case for current/incomplete year.

        Args:
            year (int): YYYY
            start_year (int): YYYY
            start_month (int): YYYY
            end_year (int): YYYY
            end_month (int): YYYY

        Yields:
            Generator[str, None, None]: url fragment {year}/{month}
        """
        if start_year == end_year:
            for month in range(start_month, end_month + 1):
                yield f"{year}/{month}"
        else:
            for month in range(1, end_month + 1):
                yield f"{year}/{month}"

    def _yield_same_year(
        self, timeframe: str, year: int, start_month: int, end_month: int
    ) -> Generator[str, None, None]:
        """Handle edge case for same/incomplete year and M1/Tick differences.

        Args:
            timeframe (str): fx_enums.Timeframe
            year (int): YYYY
            start_month (int): MM
            end_month (int): MM

        Yields:
            Generator[str, None, None]: url fragment
                - M1: {year}
                - Tick:{year}/{month}
        """
        match timeframe:
            case "M1":
                yield f"{year}"
            case _:
                for month in range(start_month, end_month + 1):
                    yield f"{year}/{month}"

    def _yield_start_year(
        self, timeframe: str, year: int, start_month: int
    ) -> Generator[str, None, None]:
        """Handle edge case for start/incomplete year and M1/Tick differences.

        Args:
            timeframe (str): fx_enums.Timeframe
            year (int): YYYY
            start_month (int): MM

        Yields:
            Generator[str, None, None]: url fragment
                - M1: {year}
                - Tick:{year}/{month}
        """
        match timeframe:
            case "M1":
                yield f"{year}"
            case _:
                for month in range(start_month, 12 + 1):
                    yield f"{year}/{month}"

    def _yield_end_year(
        self, timeframe: str, year: int, end_month: int
    ) -> Generator[str, None, None]:
        """Handle edge case for end/incomplete year and M1/Tick differences.

        Args:
            timeframe (str): fx_enums.Timeframe
            year (int): YYYY
            end_month (int): MM

        Yields:
            Generator[str, None, None]: url fragment
                - M1: {year}
                - Tick:{year}/{month}
        """
        match timeframe:
            case "M1":
                yield f"{year}"
            case _:
                for month in range(1, end_month + 1):
                    yield f"{year}/{month}"

    def _yield_year(  # noqa:BLK100
        self, timeframe: str, year: int
    ) -> Generator[str, None, None]:
        """Handle edge case M1/Tick differences.

        On year's end, histdata.com concatenates M1 monthly data into
        a single file. This means the current year has n monthly files
        where n is the number of months upto the current, but all previous
        years are single file which is indexed by year (in the url). Tick
        data, on the other hand, is month-by-month regardless of the year.

        Args:
            timeframe (str): fx_enums.Timeframe
            year (int): YYYY

        Yields:
            Generator[str, None, None]: url fragment
                - M1: {year}
                - Tick:{year}/{month}
        """
        match timeframe:
            case "M1":
                yield f"{year}"
            case _:
                for month in range(1, 12 + 1):
                    yield f"{year}/{month}"

    def _yield_single_year_or_month(  # noqa:CCR001
        self, timeframe: str, start_yearmonth: int
    ) -> Generator[str, None, None]:
        """Handle edge case where user setting is for a single year or month.

           handles a range-less edge case.

        Args:
            timeframe (str): _description_
            start_yearmonth (int): _description_

        Yields:
            Generator[str, None, None]: url fragment
                - M1: {year}
                - Tick:{year}/{month}
        """
        current_yearmonth = get_current_datemonth_gmt_minus5()
        current_year = int(get_year_from_datemonth(current_yearmonth))
        current_month = int(get_month_from_datemonth(current_yearmonth))

        start_year = int(get_year_from_datemonth(start_yearmonth))
        start_month = int(get_month_from_datemonth(start_yearmonth))

        if start_month == 0:  # sourcery skip:merge-else-if-into-elif
            if start_year == current_year:
                for month in range(1, current_month + 1):
                    yield f"{start_year}/{month}"
            else:
                yield from self._yield_year(  # pylint: disable=not-an-iterable
                    timeframe, start_year
                )
        else:
            if start_year == current_year:
                yield f"{start_year}/{start_month}"
            else:
                match timeframe:
                    case "M1":
                        yield f"{start_year}"
                    case _:
                        yield f"{start_year}/{start_month}"
