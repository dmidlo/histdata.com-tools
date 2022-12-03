"""Command-Line interface for histdatacom.

CLI interface for histdatacom

Raises:
    ValueError: ERROR on -f {self.arg_namespace.formats}           ERROR
                * format must be ASCII when importing to influxdb. eg. -f ascii
    ValueError: ERROR on -e start           ERROR
                                    * keyword 'start' cannot be used as -e start
    ValueError: ERROR on -s start           ERROR
                                    * keyword 'start' must also specify
                                      an end year-month
    ValueError: ERROR on -e {end_yearmonth}  ERROR
                    * Malformed command:
                        - cannot include `-e end_year-month` without
                          specifying a start year-month.
                            eg. -s year-month -e year-month
    ValueError: ERROR on:
            -e {get_year_from_datemonth(start_yearmonth)}  {end_yearmonth}  ERROR
                    * Malformed command:
                        - cannot include `-e end_year-month` when
                          specifying a single year
                          with -s {get_year_from_datemonth(start_yearmonth)}
    ValueError: ERROR on -e {start_yearmonth}   ERROR
                    start month cannot be zero.
                        * valid inputs:
                            a) just the year
                                eg. -s 2022
                            b) months 1-12:
                                eg. -s 2022-04
    ValueError: ERROR on -s {start_yearmonth}  ERROR
                    * Malformed command:
                        - start month is greater than 12.
                          valid input is 01-12.
    ValueError: ERROR on -e {end_yearmonth}           ERROR
                            * You left out the end month.
                                - valid input is -e year-month(1-12)
                                    eg. -e 2022-03
    ValueError: ERROR on -e {end_yearmonth}           ERROR
                            * End month cannot be zero.
                                - valid input is -e year-month(1-12)
                                    eg. -e 2022-03
    ValueError: ERROR on -e {end_yearmonth}  ERROR
                            * Malformed command:
                                - end month is greater than 12.
                                valid input is 01-12.
    ValueError: ERROR on -s {start_yearmonth} -e {end_yearmonth}  ERROR
                    * start year-month and end year-month cannot be the same.
    ValueError: ERROR on -s {start_yearmonth}      ERROR
                            * bad year-month
                                - no data available for dates
                                prior to 2000y
    ValueError: ERROR on -s {start_yearmonth}      ERROR
                            * year-month cannot be in the future.
    ValueError: ERROR on -e {end_yearmonth}     ERROR
                            * bad year-month
                                - no data available for dates
                                prior to 2000y
    ValueError: ERROR on -e {end_yearmonth}     ERROR
                            * year-month cannot be in the future.
    ValueError: ERROR on -s {start_yearmonth} -e {end_yearmonth}    ERROR
                        * logic error: end year-month is before start year-month.
    ValueError: ERROR on {yearmonth}    ERROR
                            * invalid yearmonth format

Returns:
    _type_: self.arg_namespace
"""

import argparse
import re
import sys
from typing import Any, Optional, Tuple

from rich import print  # pylint: disable=redefined-builtin

from histdatacom import Options
from histdatacom.concurrency import get_pool_cpu_count
from histdatacom.fx_enums import Format, Pairs, Timeframe
from histdatacom.utils import (
    get_current_datemonth_gmt_minus5,
    get_year_from_datemonth,
    get_month_from_datemonth,
    replace_date_punct,
)

default_options = Options()


class ArgParser(argparse.ArgumentParser):  # noqa:H601
    """Encapsulation class for argparse related operations."""

    def __init__(self, options: Options = default_options):
        """Set up argparse.

        bring in defaults DTO, setup cli params, receive
        and overwrite defaults with user cli args.

        Args:
            options (Options): _description_. Defaults to default_options.
        """
        # init _HistDataCom.ArgParser to extend argparse.ArgumentParser
        argparse.ArgumentParser.__init__(self, prog="histdatacom")
        # bring in the defaults arg DTO from outer class, use the
        # __dict__ representation of it to set argparse argument defaults.
        self.arg_namespace = options
        self._default_args = self.arg_namespace.__dict__
        self.set_defaults(**self._default_args)

    @classmethod
    def arg_list_to_set(cls, args: dict) -> dict:
        """Convert any lists in from argparse to sets.

         This is to standardize data types. If the user specifies a parameter,
         argparse returns a list, our defaults are sets, so .

        Args:
            args (dict): argparse dict

        Returns:
            dict: args dict
        """
        for arg in args:
            if isinstance(args[arg], list):
                args[arg] = set(args[arg])
        return args

    def _adjust_for_repo_data_request(self) -> None:
        """Null user inputs when -A or -U are set and ready for repo run."""
        if (
            self.arg_namespace.available_remote_data
            or self.arg_namespace.update_remote_data
        ):
            self.arg_namespace.start_yearmonth = None
            self.arg_namespace.end_yearmonth = None
            self.arg_namespace.validate_urls = False
            self.arg_namespace.download_data_archives = False
            self.arg_namespace.extract_csvs = False
            self.arg_namespace.import_to_influxdb = False
            self.arg_namespace.formats = {"ascii"}
            self.arg_namespace.timeframes = {"tick-data-quotes"}

    def _clean_from_api_args(self) -> list:  # noqa:CCR001
        """Build the args list from api Options.

        Returns:
            list: args
        """
        args = [
            "--data-directory",
            self.arg_namespace.data_directory,
            *["-p", *self.arg_namespace.pairs],
            *["-f", *self.arg_namespace.formats],
            *["-t", *self.arg_namespace.timeframes],
            *["-c", self.arg_namespace.cpu_utilization],
            *["-b", self.arg_namespace.batch_size],
        ]

        if self.arg_namespace.start_yearmonth:
            args.extend(["-s", self.arg_namespace.start_yearmonth])
        if self.arg_namespace.end_yearmonth:
            args.extend(["-e", self.arg_namespace.end_yearmonth])
        if self.arg_namespace.available_remote_data:
            args.append("-A")
        if self.arg_namespace.update_remote_data:
            args.append("-U")
        if self.arg_namespace.validate_urls:
            args.append("-V")
        if self.arg_namespace.download_data_archives:
            args.append("-D")
        if self.arg_namespace.extract_csvs:
            args.append("-X")
        if self.arg_namespace.import_to_influxdb:
            args.append("-I")
        if self.arg_namespace.delete_after_influx:
            args.append("-d")
        return args

    def _false_from_api_if_behavior_flag(self) -> None:
        """Null the API flag if a behavior flag has been set."""
        if (
            self.arg_namespace.validate_urls
            or self.arg_namespace.download_data_archives
            or self.arg_namespace.extract_csvs
            or self.arg_namespace.import_to_influxdb
        ):
            self.arg_namespace.from_api = False

    def _check_for_ascii_if_influx(self) -> None:
        """Verify ascii csv_format type for influxdb import.

        # noqa: DAR402

        Raises:
            ValueError: if influx, must use ascii
            SystemExit: exit on error
        """
        try:
            if self.arg_namespace.import_to_influxdb:
                err_text_influx_must_be_ascii = f"""
            ERROR on -f {self.arg_namespace.formats}           ERROR
                * format must be ASCII when importing to influxdb. eg. -f ascii

            """
                for csv_format in self.arg_namespace.formats:
                    if str.lower(csv_format) != "ascii":
                        raise ValueError(err_text_influx_must_be_ascii)
        except ValueError as err:
            print(err)  # noqa:T201
            raise SystemExit from err

    def _check_for_ascii_if_api(self) -> None:  # noqa:CCR001
        """Verify ascii csv_format type for api use.

        # noqa: DAR402

        Raises:
            ValueError: if influx, must use ascii
            SystemExit: exit on error
        """
        try:
            if self.arg_namespace.from_api and not (
                self.arg_namespace.validate_urls
                or self.arg_namespace.download_data_archives
                or self.arg_namespace.extract_csvs
                or self.arg_namespace.import_to_influxdb
            ):
                err_text_api_must_be_ascii = f"""
                ERROR on -f {self.arg_namespace.formats}           ERROR
                    * format must be ASCII when calling from API
                        eg.
                            import histdatacom
                            from histdatacom.options import Options

                            options = Options()
                            options.formats = {{"ascii"}}
            """
                for csv_format in self.arg_namespace.formats:
                    if str.lower(csv_format) != "ascii":
                        raise ValueError(err_text_api_must_be_ascii)
        except ValueError as err:
            print(err)  # noqa:T201
            raise SystemExit from err

    def _check_datetime_input(self) -> None:
        """Check for invalid datetime input for -s and -e flags."""
        if (  # noqa:BLK001
            self.arg_namespace.start_yearmonth  # noqa:BLK001
            or self.arg_namespace.end_yearmonth
        ):
            (
                self.arg_namespace.start_yearmonth,
                self.arg_namespace.end_yearmonth,
            ) = self._check_for_start_in_yearmonth()

            (
                self.arg_namespace.start_yearmonth,
                self.arg_namespace.end_yearmonth,
            ) = self._check_for_now_in_yearmonth()

            (
                self.arg_namespace.start_yearmonth,
                self.arg_namespace.end_yearmonth,
            ) = self._check_cli_start_yearmonth()

            self._check_cli_end_yearmonth()

            self._check_for_same_start_yearmonth()

        (
            self.arg_namespace.start_yearmonth,
            self.arg_namespace.end_yearmonth,
        ) = self._replace_falsey_yearmonth_with_none()

        self._check_start_yearmonth_in_range()
        self._check_end_yearmonth_in_range()
        self._check_start_lessthan_end()
        self._validate_prerequisites()

    def _validate_prerequisites(self) -> None:
        """Set prereqs for behavior flags -V -D -X -I."""
        if self.arg_namespace.validate_urls:
            return

        if self.arg_namespace.download_data_archives:
            self.arg_namespace.validate_urls = True

        if self.arg_namespace.extract_csvs:
            self.arg_namespace.validate_urls = True
            self.arg_namespace.download_data_archives = True

        if self.arg_namespace.import_to_influxdb:
            self.arg_namespace.validate_urls = True
            self.arg_namespace.download_data_archives = True

        if (
            not self.arg_namespace.download_data_archives
            and not self.arg_namespace.extract_csvs
            and not self.arg_namespace.import_to_influxdb
        ):
            self.arg_namespace.validate_urls = True
            self.arg_namespace.download_data_archives = True
            self.arg_namespace.extract_csvs = True

    def _check_for_now_in_yearmonth(  # noqa:CCR001
        self,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Check for now in -s or -e and adjusts it to current year-month.

        Returns:
            Tuple[Optional[str], Optional[str]]: start_yearmonth, end_yearmonth
        """
        if start_yearmonth := self.arg_namespace.start_yearmonth:
            if start_yearmonth == "now":
                return get_current_datemonth_gmt_minus5(), None
            if end_yearmonth := self.arg_namespace.end_yearmonth:  # noqa:SIM102
                if end_yearmonth == "now":
                    return (
                        start_yearmonth,
                        get_current_datemonth_gmt_minus5(),
                    )

        return start_yearmonth, end_yearmonth

    def _check_for_start_in_yearmonth(  # noqa:CCR001
        self,
    ) -> Tuple[Optional[str], Optional[str]] | Any:
        """Check for 'start' keyword in -s and sets -s yearmonth to 200001.

        # noqa: DAR402

        Raises:
            ValueError: end_yearmonth cannot be "start"
            ValueError: keyword 'start' must also specify an end year-month.
            SystemExit: exit on error

        Returns:
            Tuple[Optional[str], Optional[str]] | Any: start_yearmonth,
                                                       end_yearmonth
        """
        try:
            if (start_yearmonth := self.arg_namespace.start_yearmonth) and (
                start_yearmonth == "start"
            ):
                if end_yearmonth := self.arg_namespace.end_yearmonth:
                    if end_yearmonth == "start":
                        err_text_end_yearmonth_cannot_be_start = """
                            ERROR on -e start           ERROR
                                * keyword 'start' cannot be used as -e start
                        """
                        raise ValueError(err_text_end_yearmonth_cannot_be_start)
                    return "200001", end_yearmonth

                err_text_start_must_have_end = """
                        ERROR on -s start           ERROR
                            * keyword 'start' must also specify
                                an end year-month
                """
                raise ValueError(err_text_start_must_have_end)  # noqa:TC301
            return (  # noqa:TC300
                self.arg_namespace.start_yearmonth,
                self.arg_namespace.end_yearmonth,
            )
        except ValueError as err:
            raise SystemExit from err

    def _check_cli_start_yearmonth(  # noqa:CCR001
        self,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Validate for -s start_yearmonth.

        # noqa: DAR402

        Raises:
            ValueError: No start_yearmonth
            ValueError: No end_yearmonth
            ValueError: Start month cannot be zero.
            ValueError: Start month cannot be greater than 12.
            SystemExit: exit on error.

        Returns:
            Tuple[Optional[str], Optional[str]]: _description_
        """
        start_yearmonth = self.arg_namespace.start_yearmonth
        start_year = get_year_from_datemonth(start_yearmonth)
        start_month = get_month_from_datemonth(str(start_yearmonth))

        err_text_start_month = f"""
                ERROR on -e {start_yearmonth}   ERROR
                    start month cannot be zero.
                        * valid inputs:
                            a) just the year
                                eg. -s 2022
                            b) months 1-12:
                                eg. -s 2022-04
        """

        end_yearmonth = self.arg_namespace.end_yearmonth

        err_text_no_end_yearmonth = f"""
        ERROR on -e {get_year_from_datemonth(start_yearmonth)} {end_yearmonth}
            * Malformed command:
                - cannot include `-e end_year-month` when
                    specifying a single year
                    with -s {get_year_from_datemonth(start_yearmonth)}
        """

        err_text_no_start_yearmonth = f"""
                ERROR on -e {end_yearmonth}  ERROR
                    * Malformed command:
                        - cannot include `-e end_year-month` without
                          specifying a start year-month.
                            eg. -s year-month -e year-month
        """
        err_text_start_month_greater_than_12 = f"""
                ERROR on -s {start_yearmonth}  ERROR
                    * Malformed command:
                        - start month is greater than 12.
                          valid input is 01-12.
        """

        try:
            if not start_month:
                if end_yearmonth:
                    if not start_year:
                        raise ValueError(err_text_no_start_yearmonth)
                    raise ValueError(err_text_no_end_yearmonth)
                return f"{start_year}00", None
            if start_month == "00":
                raise ValueError(err_text_start_month)  # noqa:TC301
            if int(start_month) > 12:
                raise ValueError(  # noqa:TC301,BLK100
                    err_text_start_month_greater_than_12
                )
            return start_yearmonth, end_yearmonth  # noqa:TC300
        except ValueError as err:
            raise SystemExit from err

    def _check_cli_end_yearmonth(self) -> None:  # noqa:CCR001
        """Validate for -e end_yearmonth.

        # noqa: DAR402

        Raises:
            ValueError: no end month specified.
            ValueError: end month cannot be zero
            ValueError: end month cannot be greater than zero
            SystemExit: exit on error
        """
        try:
            if end_yearmonth := self.arg_namespace.end_yearmonth:
                end_year = get_year_from_datemonth(end_yearmonth)
                end_month = get_month_from_datemonth(end_yearmonth)

                err_text_no_endmonth = f"""
                        ERROR on -e {end_yearmonth}           ERROR
                            * You left out the end month.
                                - valid input is -e year-month(1-12)
                                    eg. -e 2022-03

                """

                err_text_endmonth_cannot_be_zero = f"""
                        ERROR on -e {end_yearmonth}           ERROR
                            * End month cannot be zero.
                                - valid input is -e year-month(1-12)
                                    eg. -e 2022-03

                """
                err_text_end_month_greater_than_12 = f"""
                        ERROR on -e {end_yearmonth}  ERROR
                            * Malformed command:
                                - end month is greater than 12.
                                valid input is 01-12.
                """
                if end_year and not end_month:
                    raise ValueError(err_text_no_endmonth)
                if end_month == "00":
                    raise ValueError(err_text_endmonth_cannot_be_zero)
                if int(end_month) > 12:
                    raise ValueError(err_text_end_month_greater_than_12)
        except ValueError as err:
            raise SystemExit from err

    def _check_for_same_start_yearmonth(self) -> None:
        """Validate -s start_yearmonth and -e end_yearmonth are not the same.

        # noqa: DAR402

        Raises:
            ValueError: -s and -e cannot be the same.
            SystemExit: Exit on error
        """
        try:
            start_yearmonth = self.arg_namespace.start_yearmonth
            start_year = get_year_from_datemonth(start_yearmonth)
            start_month = get_month_from_datemonth(str(start_yearmonth))

            end_yearmonth = self.arg_namespace.end_yearmonth
            end_year = get_year_from_datemonth(end_yearmonth)
            end_month = get_month_from_datemonth(str(end_yearmonth))

            err_text_start_and_end_cannot_be_the_same = f"""
                ERROR on -s {start_yearmonth} -e {end_yearmonth}  ERROR
                    * start year-month and end year-month cannot be the same.
            """

            if f"{start_year}_{start_month}" == f"{end_year}_{end_month}":
                raise ValueError(  # noqa:TC301
                    err_text_start_and_end_cannot_be_the_same
                )
        except ValueError as err:
            raise SystemExit from err

    def _replace_falsey_yearmonth_with_none(
        self,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Set value to None if no user input on -s or -e.

        Returns:
            Tuple[Optional[str], Optional[str]]: start_yearmonth, end_yearmonth
        """
        start_yearmonth = self.arg_namespace.start_yearmonth
        end_yearmonth = self.arg_namespace.end_yearmonth

        if not start_yearmonth or start_yearmonth == "":
            start_yearmonth = None
        if not end_yearmonth or end_yearmonth == "":
            end_yearmonth = None

        return start_yearmonth, end_yearmonth

    def _check_start_yearmonth_in_range(self) -> None:
        """Validate -s not before than 200001 or later than current yearmonth.

        Raises:
            SystemExit: Exit on error
        """
        try:
            if start_yearmonth := self.arg_namespace.start_yearmonth:
                err_text_date_prior_to_dataset = f"""
                        ERROR on -s {start_yearmonth}      ERROR
                            * bad year-month
                                - no data available for dates
                                prior to 2000y
                """
                err_text_date_is_in_future = f"""
                        ERROR on -s {start_yearmonth}      ERROR
                            * year-month cannot be in the future.
                """
                if int(start_yearmonth) < 200000:
                    raise ValueError(err_text_date_prior_to_dataset)
                if int(start_yearmonth) > int(  # noqa:BLK100
                    get_current_datemonth_gmt_minus5()
                ):
                    raise ValueError(err_text_date_is_in_future)
        except ValueError as err:
            raise SystemExit from err

    def _check_end_yearmonth_in_range(self) -> None:
        """Check -e is not earlier than 200001 or later than current year-month.

        Raises:
            SystemExit: Exit on error
        """
        try:
            if end_yearmonth := self.arg_namespace.end_yearmonth:
                err_text_date_prior_to_dataset = f"""
                        ERROR on -e {end_yearmonth}     ERROR
                            * bad year-month
                                - no data available for dates
                                prior to 2000y
                """
                err_text_date_is_in_future = f"""
                        ERROR on -e {end_yearmonth}     ERROR
                            * year-month cannot be in the future.
                """
                if int(end_yearmonth) < 200000:
                    raise ValueError(err_text_date_prior_to_dataset)
                if int(end_yearmonth) > int(  # noqa:BLK100
                    get_current_datemonth_gmt_minus5()
                ):
                    raise ValueError(err_text_date_is_in_future)
        except ValueError as err:
            raise SystemExit from err

    def _check_start_lessthan_end(self) -> None:
        """Validate -e is not a year-month earlier than -s.

        Raises:
            SystemExit: Exit on error
        """
        try:
            if (start_yearmonth := self.arg_namespace.start_yearmonth) and (
                end_yearmonth := self.arg_namespace.end_yearmonth
            ):

                err_text_start_date_after_end_date = f"""
                    ERROR on -s {start_yearmonth} -e {end_yearmonth}    ERROR
                        * logic error: end year-month is before start year-month.
                    """
                if int(start_yearmonth) > int(end_yearmonth):
                    raise ValueError(err_text_start_date_after_end_date)
        except ValueError as err:
            raise SystemExit from err

    def _validate_yearmonth_format(self, yearmonth: str) -> str | Any:
        # pylint: disable=anomalous-backslash-in-string
        """Validate initial user input.

        checks year-month format for:
            -  0000
            -  0000-00
            - "0000 00"
            -  0000_00
            -  0000.00
            -  0000:00
            -  000000
            -  start
            -  now

        # noqa: DAR402

        Args:
            yearmonth (str): YYYYMM

        Raises:
            ValueError: invalid yearmonth format
            SystemExit: Exit on error

        Returns:
            str | Any: str("YYYYMM") or "now" or "start"
        """
        try:
            if (
                re.match("^\d{4}[-_.: ]\d{2}$", yearmonth)  # noqa:W605
                or re.match("^\d{6}$", yearmonth)  # noqa:W605
                or re.match("^\d{4}$", yearmonth)  # noqa:W605
                or str.lower(yearmonth) == "now"
                or str.lower(yearmonth) == "start"
                or not yearmonth
            ):
                return replace_date_punct(yearmonth)
            err_text_bad_yearmonth_format = f"""
                        ERROR on {yearmonth}    ERROR
                            * invalid yearmonth format
            """

            raise ValueError(err_text_bad_yearmonth_format)  # noqa:TC301
        except ValueError as err:
            raise SystemExit from err

    def _set_args(self) -> None:  # noqa:CFQ001
        # pylint: disable=unnecessary-lambda
        """Config CLI arguments and default values."""
        self.add_argument(
            "-V",
            "--validate_urls",
            action="store_true",
            help="Check generated list of URLs as valid download locations",
        )
        self.add_argument(
            "-A",
            "--available_remote_data",
            action="store_true",
            help="list data retrievable from histdata.com",
        )
        self.add_argument(
            "-U",
            "--update_remote_data",
            action="store_true",
            help="update list of data retrievable from histdata.com",
        )
        self.add_argument(
            "--by",
            type=str,
            help=(
                "With -A, -U, to sort --by"
                " [pair_asc, pair_dsc, start_asc, start_dsc]"
            ),
        )
        self.add_argument(
            "-D",
            "--download_data_archives",
            action="store_true",
            help=(
                "download specified pairs/formats/timeframe and"  # noqa:BLK100
                " create data files"
            ),
        )
        self.add_argument(
            "-X",
            "--extract_csvs",
            action="store_true",
            help=(
                "histdata.com delivers zip files."  # noqa:BLK100
                " Use the -X flag to extract them."
            ),
        )
        self.add_argument(
            "-I",
            "--import_to_influxdb",
            action="store_true",
            help=(
                "import data to influxdb instance."  # noqa:BLK100
                " Use influxdb.yaml to configure."
            ),
        )
        self.add_argument(
            "-c",
            "--cpu_utilization",
            type=str,
            help=(
                '"low", "medium", "high". High uses all'
                " available CPUs OR integer percent 1-200"
            ),  # noqa: E501
        )
        self.add_argument(
            "-p",
            "--pairs",
            nargs="+",
            type=str,
            choices=Pairs.list_keys(),
            help="space separated currency pairs. e.g. -p eurusd usdjpy ...",
            metavar="PAIR",
        )
        self.add_argument(
            "-f",
            "--formats",
            nargs="+",
            type=str,
            choices=Format.list_values(),
            help=(
                "space separated formats. -f "  # noqa:BLK100
                "metatrader ascii ninjatrader metastock"
            ),
            metavar="FORMAT",
        )
        self.add_argument(
            "-t",
            "--timeframes",
            nargs="+",
            type=(
                lambda v: Timeframe(v).name  # type: ignore
            ),  # convert long Timeframe .value to short .key
            choices=Timeframe.list_keys(),
            help=(
                "space separated Timeframes. -t "  # noqa:BLK100
                "tick-data-quotes 1-minute-bar-quotes"
            ),
            metavar="TIMEFRAME",
        )
        self.add_argument(
            "-s",
            "--start_yearmonth",
            type=(lambda v: self._validate_yearmonth_format(v)),
            help=(
                "set a start year and month for data."  # noqa:BLK100
                " e.g. -s 2000-04 or -s 2015-00"
            ),
        )
        self.add_argument(
            "-e",
            "--end_yearmonth",
            type=(lambda v: self._validate_yearmonth_format(v)),
            help=(
                "set a start year and month for data."  # noqa:BLK100
                " e.g. -e 2020-00 or -e 2022-04"
            ),
        )
        self.add_argument(
            "-b",
            "--batch_size",
            type=int,
            help="(integer) influxdb write_api batch size. defaults to 5000",
        )
        self.add_argument(
            "-d",
            "--delete_after_influx",
            action="store_true",
            help="delete data files after upload to influxdb",
        )
        self.add_argument(
            "--data-directory",
            type=str,
            help='Directory Used to save data. default is "./data/"',
        )

    def _sanitize_input(self) -> None:
        """Clean user-input before run."""
        # prevent running from cli with no arguments
        if len(sys.argv) == 1 and not self.arg_namespace.from_api:
            self.print_help(sys.stderr)
            sys.exit(1)

        self._adjust_for_repo_data_request()

        if "histdatacom" not in sys.argv[0] and self.arg_namespace.from_api:
            args = self._clean_from_api_args()
            self._false_from_api_if_behavior_flag()
            self.parse_args(args, namespace=self.arg_namespace)
        else:
            # Get the args from sys.argv
            self.parse_args(namespace=self.arg_namespace)

        self._check_datetime_input()
        self._check_for_ascii_if_influx()
        self._check_for_ascii_if_api()
        get_pool_cpu_count(self.arg_namespace.cpu_utilization)

    def __call__(self) -> Options:
        """Collect and process settings from CLI or API.

        Returns:
            Options: arg_namespace for config.ARGS
        """
        self._set_args()
        self._sanitize_input()
        return self.arg_namespace
