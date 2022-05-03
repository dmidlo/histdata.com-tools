"""_summary_
CLI interface for histdatacom

Raises:
    ValueError: ERROR on -f {args_namespace.formats}           ERROR
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
    ValueError: ERROR on -e {get_year_from_datemonth(start_yearmonth)}  {end_yearmonth}  ERROR
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
import sys
import re
from histdatacom.fx_enums import Pairs
from histdatacom.fx_enums import Format
from histdatacom.fx_enums import Timeframe
from histdatacom.utils import get_current_datemonth_gmt_minus5
from histdatacom.utils import get_month_from_datemonth
from histdatacom.utils import get_year_from_datemonth
from histdatacom.utils import replace_date_punct
from histdatacom.concurrency import get_pool_cpu_count
from histdatacom.options import Options


class ArgParser(argparse.ArgumentParser):
    """ Encapsulation class for argparse related operations """

    def __init__(self, options=Options(), **kwargs):
        """ set up argparse, bring in defaults DTO, setup cli params, receive
            and overwrite defaults with user cli args."""
        
        # init _HistDataCom.ArgParser to extend argparse.ArgumentParser
        argparse.ArgumentParser.__init__(self, prog='histdatacom')
        # bring in the defaults arg DTO from outer class, use the
        # __dict__ representation of it to set argparse argument defaults.
        self.arg_namespace = options
        self._default_args = self.arg_namespace.__dict__
        self.set_defaults(**self._default_args)

        # Nothing special here, adding cli params
        # metavar="..." is used to limit the display of choices="large iterables".
        self.add_argument(
            "-V", "--validate_urls",
            action='store_true',
            help='Check generated list of URLs as valid download locations')
        self.add_argument(
            "-D", "--download_data_archives",
            action='store_true',
            help='download specified pairs/formats/timeframe and create data files')
        self.add_argument(
            "-X", "--extract_csvs",
            action='store_true',
            help='histdata.com delivers zip files.  use the -X flag to extract them to .csv.')
        self.add_argument(
            "-I", "--import_to_influxdb",
            action='store_true',
            help='import csv data to influxdb instance. Use influxdb.yaml to configure.')
        self.add_argument(
            '-c', '--cpu_utilization',
            type=str,
            help='"low", "medium", "high". High uses all available CPUs. OR integer percent 1-200'  )
        self.add_argument(
            '-p', '--pairs',
            nargs='+',
            type=str,
            choices=Pairs.list_keys(),
            help='space separated currency pairs. e.g. -p eurusd usdjpy ...',
            metavar='PAIR')
        self.add_argument(
            '-f', '--formats',
            nargs='+',
            type=str,
            choices=Format.list_values(),
            help='space separated formats. e.g. -f metatrader ascii ninjatrader metastock',
            metavar='FORMAT')
        self.add_argument(
            '-t', '--timeframes',
            nargs='+',
            type=(lambda v: Timeframe(v).name),  # convert long Timeframe .value to short .key
            choices=Timeframe.list_keys(),
            help='space separated Timeframes. e.g. -t tick-data-quotes 1-minute-bar-quotes ...',
            metavar='TIMEFRAME')
        self.add_argument(
            "-s", "--start_yearmonth",
            type=(lambda v: self.validate_yearmonth_format(v)),
            help='set a start year and month for data. e.g. -s 2000-04 or -s 2015-00')
        self.add_argument(
            "-e", "--end_yearmonth",
            type=(lambda v: self.validate_yearmonth_format(v)),
            help='set a start year and month for data. e.g. -s 2020-00 or -s 2022-04')
        self.add_argument(
            '-d', '--data-directory',
            type=str,
            help='Directory Used to save data. default is "data" in the current directory')
        
        if "histdatacom" not in sys.argv[0] and self.arg_namespace.from_api:
            args = self.clean_from_api_args(self.arg_namespace)
            self.false_from_api_if_behavior_flag(self.arg_namespace)
            self.parse_args(args, namespace=self.arg_namespace)
        else:
            # Get the args from sys.argv
            self.parse_args(namespace=self.arg_namespace)

        # prevent running from cli with no arguments
        if len(sys.argv) == 1 and not self.arg_namespace.from_api:
            self.print_help(sys.stderr)
            sys.exit(1)

        self.check_datetime_input(self.arg_namespace)
        self.check_for_ascii_if_influx(self.arg_namespace)
        self.check_for_ascii_if_api(self.arg_namespace)
        get_pool_cpu_count(self.arg_namespace.cpu_utilization)



    def __call__(self):
        """ simply return the completed args object """
        return self.arg_namespace

    @classmethod
    def clean_from_api_args(cls, args_namespace):
        args = []
        
        args.extend(["-d", args_namespace.data_directory])
        args.extend(["-p", *args_namespace.pairs])
        args.extend(["-f", *args_namespace.formats])
        args.extend(["-t", *args_namespace.timeframes])
        args.extend(["-c", args_namespace.cpu_utilization])

        if args_namespace.start_yearmonth:
            args.extend(["-s", args_namespace.start_yearmonth])
        if args_namespace.end_yearmonth:
            args.extend(["-e", args_namespace.end_yearmonth])
        if args_namespace.validate_urls:
            args.append("-V")
        if args_namespace.download_data_archives:
            args.append("-D")
        if args_namespace.extract_csvs:
            args.append("-X")
        if args_namespace.import_to_influxdb:
            args.append("-I")
        return args

    @classmethod
    def _arg_list_to_set(cls, args):
        # This is to standardize data types. If the user specifies a parameter,
        # argparse returns a list, our defaults are sets, so .
        for arg in args:
            if isinstance(args[arg], list):
                args[arg] = set(args[arg])
        return args
    @classmethod
    def false_from_api_if_behavior_flag(cls, args_namespace):
        if args_namespace.validate_urls \
        or args_namespace.download_data_archives \
        or args_namespace.extract_csvs \
        or args_namespace.import_to_influxdb:
            args_namespace.from_api = False
            
    @classmethod
    def check_for_ascii_if_influx(cls, args_namespace):
        """Verify ascii csv_format type for influxdb import"""
        try:
            err_text_influx_must_be_ascii = \
            f"""
                ERROR on -f {args_namespace.formats}           ERROR
                    * format must be ASCII when importing to influxdb. eg. -f ascii

            """
            if args_namespace.import_to_influxdb:
                for csv_format in args_namespace.formats:
                    if str.lower(csv_format) != "ascii":
                        raise ValueError(err_text_influx_must_be_ascii)
        except ValueError as err:
            print(err)
            sys.exit(err)

    @classmethod
    def check_for_ascii_if_api(cls, args_namespace):
        try:
            err_text_api_must_be_ascii = \
            f"""
                ERROR on -f {args_namespace.formats}           ERROR
                    * format must be ASCII when calling from API 
                        eg. 
                            import histdatacom
                            from histdatacom.options import Options

                            options = Options()
                            options.formats = {{"ascii"}}
            """
            if args_namespace.from_api \
            and not (
                args_namespace.validate_urls 
                or args_namespace.download_data_archives \
                or args_namespace.extract_csvs \
                or args_namespace.import_to_influxdb
            ):
                for csv_format in args_namespace.formats:
                    if str.lower(csv_format) != "ascii":
                        raise ValueError(err_text_api_must_be_ascii)
        except ValueError as err:
            print(err)
            sys.exit()
    
    
    @classmethod
    def check_datetime_input(cls, args_namespace):
        """Checks for invalid datetime input for -s and -e flags"""
        if args_namespace.start_yearmonth \
        or args_namespace.end_yearmonth:
            args_namespace.start_yearmonth, args_namespace.end_yearmonth = \
                cls.check_for_start_in_yearmonth(args_namespace)

            args_namespace.start_yearmonth, args_namespace.end_yearmonth = \
                cls.check_for_now_in_yearmonth(args_namespace)

            args_namespace.start_yearmonth, args_namespace.end_yearmonth = \
                cls.check_cli_start_yearmonth(args_namespace)

            cls.check_cli_end_yearmonth(args_namespace)

            cls.check_for_same_start_yearmonth(args_namespace)

        args_namespace.start_yearmonth, args_namespace.end_yearmonth = \
            cls.replace_falsey_yearmonth_with_none(args_namespace)

        cls.check_start_yearmonth_in_range(args_namespace)
        cls.check_end_yearmonth_in_range(args_namespace)
        cls.check_start_lessthan_end(args_namespace)
        cls.validate_prerequisites(args_namespace)

    @classmethod
    def validate_prerequisites(cls, args_namespace):
        """Sets prereqs for behavior flags -V -D -X -I"""
        if args_namespace.validate_urls:
            return

        if args_namespace.download_data_archives:
            args_namespace.validate_urls = True

        if args_namespace.extract_csvs:
            args_namespace.validate_urls = True
            args_namespace.download_data_archives = True

        if args_namespace.import_to_influxdb:
            args_namespace.validate_urls = True
            args_namespace.download_data_archives = True
            #args_namespace.extract_csvs = True

        if not args_namespace.download_data_archives \
        and not args_namespace.extract_csvs \
        and not args_namespace.import_to_influxdb:
            args_namespace.validate_urls = True
            args_namespace.download_data_archives = True
            args_namespace.extract_csvs = True

    @classmethod
    def check_for_now_in_yearmonth(cls, args_namespace):
        """checks for now in -s or -e and adjusts it to current year-month"""
        if (start_yearmonth := args_namespace.start_yearmonth):
            if start_yearmonth == "now":
                return get_current_datemonth_gmt_minus5(), None
            if end_yearmonth := args_namespace.end_yearmonth:
                if end_yearmonth == "now":
                    return start_yearmonth, get_current_datemonth_gmt_minus5()

        return start_yearmonth, end_yearmonth

    @classmethod
    def check_for_start_in_yearmonth(cls, args_namespace):
        """Checks for 'start' keyword in -s and sets -s yearmonth to 200001"""
        try:
            if (start_yearmonth := args_namespace.start_yearmonth) and (start_yearmonth == "start"):
                if (end_yearmonth := args_namespace.end_yearmonth):
                    if end_yearmonth == "start":
                        err_text_end_yearmonth_cannot_be_start = \
                        """
                            ERROR on -e start           ERROR
                                * keyword 'start' cannot be used as -e start
                        """
                        raise ValueError(err_text_end_yearmonth_cannot_be_start)
                    return "200001", end_yearmonth

                err_text_start_must_have_end = \
                """
                        ERROR on -s start           ERROR
                            * keyword 'start' must also specify
                                an end year-month
                """
                raise ValueError(err_text_start_must_have_end)
            return args_namespace.start_yearmonth, args_namespace.end_yearmonth
        except ValueError as err:
            cls.exit_on_datetime_error(err)

    @classmethod
    def check_cli_start_yearmonth(cls, args_namespace):
        """Validations for -s start_yearmonth"""
        start_yearmonth = args_namespace.start_yearmonth
        start_year = get_year_from_datemonth(start_yearmonth)
        start_month = get_month_from_datemonth(start_yearmonth)

        err_text_start_month = \
        f"""
                ERROR on -e {start_yearmonth}   ERROR
                    start month cannot be zero.
                        * valid inputs:
                            a) just the year
                                eg. -s 2022
                            b) months 1-12:
                                eg. -s 2022-04
        """

        end_yearmonth = args_namespace.end_yearmonth

        err_text_no_end_yearmonth = \
        f"""
                ERROR on -e {get_year_from_datemonth(start_yearmonth)}  {end_yearmonth}  ERROR
                    * Malformed command:
                        - cannot include `-e end_year-month` when
                          specifying a single year
                          with -s {get_year_from_datemonth(start_yearmonth)}
        """

        err_text_no_start_yearmonth = \
        f"""
                ERROR on -e {end_yearmonth}  ERROR
                    * Malformed command:
                        - cannot include `-e end_year-month` without
                          specifying a start year-month.
                            eg. -s year-month -e year-month
        """
        err_text_start_month_greater_than_12 = \
        f"""
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
                raise ValueError(err_text_start_month)
            if int(start_month) > 12:
                raise ValueError(err_text_start_month_greater_than_12)
            return start_yearmonth, end_yearmonth
        except ValueError as err:
            cls.exit_on_datetime_error(err)

    @classmethod
    def check_cli_end_yearmonth(cls, args_namespace):
        """Validations for -e end_yearmonth"""
        try:
            if end_yearmonth := args_namespace.end_yearmonth:
                end_year = get_year_from_datemonth(end_yearmonth)
                end_month = get_month_from_datemonth(end_yearmonth)

                err_text_no_endmonth = \
                f"""
                        ERROR on -e {end_yearmonth}           ERROR
                            * You left out the end month.
                                - valid input is -e year-month(1-12)
                                    eg. -e 2022-03

                """

                err_text_endmonth_cannot_be_zero = \
                f"""
                        ERROR on -e {end_yearmonth}           ERROR
                            * End month cannot be zero.
                                - valid input is -e year-month(1-12)
                                    eg. -e 2022-03

                """
                err_text_end_month_greater_than_12 = \
                f"""
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
            cls.exit_on_datetime_error(err)

    @classmethod
    def check_for_same_start_yearmonth(cls, args_namespace):
        """Validates that -s start_yearmonth and -e end_yearmonth are not the same"""
        try:
            start_yearmonth = args_namespace.start_yearmonth
            start_year = get_year_from_datemonth(start_yearmonth)
            start_month = get_month_from_datemonth(start_yearmonth)

            end_yearmonth = args_namespace.end_yearmonth
            end_year = get_year_from_datemonth(end_yearmonth)
            end_month = get_month_from_datemonth(end_yearmonth)

            err_text_start_and_end_cannot_be_the_same = \
            f"""
                ERROR on -s {start_yearmonth} -e {end_yearmonth}  ERROR
                    * start year-month and end year-month cannot be the same.
            """

            if f"{start_year}_{start_month}" == f"{end_year}_{end_month}":
                raise ValueError(err_text_start_and_end_cannot_be_the_same)
        except ValueError as err:
            cls.exit_on_datetime_error(err)

    @classmethod
    def replace_falsey_yearmonth_with_none(cls, args_namespace):
        """if no user input on -s or -e set value to None"""
        start_yearmonth = args_namespace.start_yearmonth
        end_yearmonth = args_namespace.end_yearmonth

        if not start_yearmonth or start_yearmonth == "":
            start_yearmonth = None
        if not end_yearmonth or end_yearmonth == "":
            end_yearmonth = None

        return start_yearmonth, end_yearmonth

    @classmethod
    def check_start_yearmonth_in_range(cls, args_namespace):
        """Validate that -s is not earlier than 2000-01 or later than the current year-month"""
        try:
            if start_yearmonth := args_namespace.start_yearmonth:
                err_text_date_prior_to_dataset = \
                f"""
                        ERROR on -s {start_yearmonth}      ERROR
                            * bad year-month
                                - no data available for dates
                                prior to 2000y
                """
                err_text_date_is_in_future = \
                f"""
                        ERROR on -s {start_yearmonth}      ERROR
                            * year-month cannot be in the future.
                """
                if int(start_yearmonth) < 200000:
                    raise ValueError(err_text_date_prior_to_dataset)
                if int(start_yearmonth) > int(get_current_datemonth_gmt_minus5()):
                    raise ValueError(err_text_date_is_in_future)
        except ValueError as err:
            cls.exit_on_datetime_error(err)

    @classmethod
    def check_end_yearmonth_in_range(cls, args_namespace):
        """checks if -e is not earlier than 200001 or later than current year-month"""
        try:
            if end_yearmonth := args_namespace.end_yearmonth:
                err_text_date_prior_to_dataset = \
                f"""
                        ERROR on -e {end_yearmonth}     ERROR
                            * bad year-month
                                - no data available for dates
                                prior to 2000y
                """
                err_text_date_is_in_future = \
                f"""
                        ERROR on -e {end_yearmonth}     ERROR
                            * year-month cannot be in the future.
                """
                if int(end_yearmonth) < 200000:
                    raise ValueError(err_text_date_prior_to_dataset)
                if int(end_yearmonth) > int(get_current_datemonth_gmt_minus5()):
                    raise ValueError(err_text_date_is_in_future)
        except ValueError as err:
            cls.exit_on_datetime_error(err)

    @classmethod
    def check_start_lessthan_end(cls, args_namespace):
        """validats that -e is not a year-month earlier than -s"""
        try:
            if (start_yearmonth := args_namespace.start_yearmonth) \
            and (end_yearmonth := args_namespace.end_yearmonth):

                err_text_start_date_after_end_date = \
                    f"""
                        ERROR on -s {start_yearmonth} -e {end_yearmonth}    ERROR
                            * logic error: end year-month is before start year-month.
                    """
                if int(start_yearmonth) > int(end_yearmonth):
                    raise ValueError(err_text_start_date_after_end_date)
        except ValueError as err:
            cls.exit_on_datetime_error(err)

    @classmethod
    def validate_yearmonth_format(cls, yearmonth):
        """Initial user input validation.
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

            returns str("000000") or "now" or "start"
        """
        try:
            err_text_bad_yearmonth_format = \
            f"""
                        ERROR on {yearmonth}    ERROR
                            * invalid yearmonth format
            """

            if re.match("^\d{4}[-_.: ]\d{2}$", yearmonth) \
            or re.match("^\d{6}$", yearmonth) \
            or re.match("^\d{4}$", yearmonth) \
            or str.lower(yearmonth) == "now" \
            or str.lower(yearmonth) == "start" \
            or yearmonth == "":
                return replace_date_punct(yearmonth)
            raise ValueError(err_text_bad_yearmonth_format)
        except ValueError as err:
            cls.exit_on_datetime_error(err)

    @classmethod
    def exit_on_datetime_error(cls, err):
        """standared exit on error for user input"""
        print(err)
        sys.exit(err)
