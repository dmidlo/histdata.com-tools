from typing import Generator
from typing import Any
from typing import Optional
from typing import Set

from histdatacom.utils import Utils
from histdatacom.fx_enums import Timeframe
from histdatacom.fx_enums import get_valid_format_timeframes


class Urls:
    @staticmethod
    def valid_format_timeframe_pair_urls(
        formats: set, timeframes: set, pairs: Optional[Set[Any]]
    ) -> Generator[tuple[str, Any], None, None]:
        for csv_format in formats:
            for timeframe in timeframes:
                if timeframe in get_valid_format_timeframes(csv_format):
                    for pair in pairs:  # type: ignore
                        yield f"{csv_format}/{Timeframe[timeframe].value}/{pair}/", timeframe

    @staticmethod
    def correct_for_zero_month(month: int) -> int:
        if month == 0:
            month = 1
        return month

    @staticmethod
    def generate_form_urls(
        start_yearmonth: str,
        end_yearmonth: str,
        formats: set,
        pairs: Optional[Set[Any]],
        timeframes: set,
        base_url: str,
    ) -> Generator[str, None, None]:
        current_yearmonth = Utils.get_current_datemonth_gmt_minus5()
        current_year = int(Utils.get_year_from_datemonth(current_yearmonth))

        if start_yearmonth is None and end_yearmonth is None:  # type: ignore
            start_yearmonth, end_yearmonth = "200001", current_yearmonth  # type: ignore

        for sub_url, timeframe in Urls.valid_format_timeframe_pair_urls(
            formats, timeframes, pairs
        ):
            form_url = f"{base_url}?/{sub_url}"

            if end_yearmonth is None:
                for date_url in Urls.yield_single_year_or_month(  # type: ignore
                    timeframe, start_yearmonth
                ):
                    yield f"{form_url}{date_url}"
            else:
                start_year = int(Utils.get_year_from_datemonth(start_yearmonth))
                start_month = int(Utils.get_month_from_datemonth(start_yearmonth))
                end_year = int(Utils.get_year_from_datemonth(end_yearmonth))
                end_month = int(Utils.get_month_from_datemonth(end_yearmonth))

                for year in range(start_year, end_year + 1):
                    yield from Urls.yield_range_of_yearmonths(  # pylint: disable=not-an-iterable
                        year,
                        timeframe,
                        form_url,
                        start_year,
                        start_month,
                        end_year,
                        end_month,
                        current_year,
                    )

    @staticmethod
    def yield_range_of_yearmonths(
        year: int,
        timeframe: str,
        form_url: str,
        start_year: int,
        start_month: int,
        end_year: int,
        end_month: int,
        current_year: int,
    ) -> Generator[str, None, None]:

        match year:
            case _ if year == current_year:
                for date_url in Urls.yield_current_year(
                    year, start_year, start_month, end_year, end_month
                ):
                    yield f"{form_url}{date_url}"

            case _ if start_year == year == end_year:
                for date_url in Urls.yield_same_year(  # pylint: disable=not-an-iterable
                    timeframe, year, start_month, end_month
                ):
                    yield f"{form_url}{date_url}"

            case _ if year == start_year != end_year:
                for (
                    date_url
                ) in Urls.yield_start_year(  # pylint: disable=not-an-iterable
                    timeframe, year, start_month
                ):
                    yield f"{form_url}{date_url}"

            case _ if year == end_year != start_year:
                for date_url in Urls.yield_end_year(  # pylint: disable=not-an-iterable
                    timeframe, year, end_month
                ):
                    yield f"{form_url}{date_url}"
            case _:
                for date_url in Urls.yield_year(  # pylint: disable=not-an-iterable
                    timeframe, year
                ):
                    yield f"{form_url}{date_url}"

    @staticmethod
    def yield_current_year(
        year: int, start_year: int, start_month: int, end_year: int, end_month: int
    ) -> Generator[str, None, None]:
        if start_year == end_year:
            for month in range(start_month, end_month + 1):
                yield f"{year}/{month}"
        else:
            for month in range(1, end_month + 1):
                yield f"{year}/{month}"

    @staticmethod
    def yield_same_year(
        timeframe: str, year: int, start_month: int, end_month: int
    ) -> Generator[str, None, None]:
        match timeframe:
            case "M1":
                yield f"{year}"
            case _:
                for month in range(start_month, end_month + 1):
                    yield f"{year}/{month}"

    @staticmethod
    def yield_start_year(
        timeframe: str, year: int, start_month: int
    ) -> Generator[str, None, None]:
        match timeframe:
            case "M1":
                yield f"{year}"
            case _:
                for month in range(start_month, 12 + 1):
                    yield f"{year}/{month}"

    @staticmethod
    def yield_end_year(
        timeframe: str, year: int, end_month: int
    ) -> Generator[str, None, None]:
        match timeframe:
            case "M1":
                yield f"{year}"
            case _:
                for month in range(1, end_month + 1):
                    yield f"{year}/{month}"

    @staticmethod
    def yield_year(timeframe: str, year: int) -> Generator[str, None, None]:
        match timeframe:
            case "M1":
                yield f"{year}"
            case _:
                for month in range(1, 12 + 1):
                    yield f"{year}/{month}"

    @staticmethod
    def yield_single_year_or_month(
        timeframe: str, start_yearmonth: int
    ) -> Generator[str, None, None]:

        current_yearmonth = Utils.get_current_datemonth_gmt_minus5()
        current_year = int(Utils.get_year_from_datemonth(current_yearmonth))
        current_month = int(Utils.get_month_from_datemonth(current_yearmonth))

        start_year = int(Utils.get_year_from_datemonth(start_yearmonth))
        start_month = int(Utils.get_month_from_datemonth(start_yearmonth))

        if start_month == 0:  # return the year's data
            if start_year == current_year:
                for month in range(1, current_month + 1):
                    yield f"{start_year}/{month}"
            else:
                for date_url in Urls.yield_year(  # pylint: disable=not-an-iterable
                    timeframe, start_year
                ):
                    yield date_url
        else:
            if start_year == current_year:
                yield f"{start_year}/{start_month}"
            else:
                match timeframe:
                    case "M1":
                        yield f"{start_year}"
                    case _:
                        yield f"{start_year}/{start_month}"
