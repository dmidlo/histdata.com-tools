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
from histdatacom.cli_config import (
    CliConfigError,
    config_path_from_cli_args,
    load_cli_config_args,
)
from histdatacom.concurrency import get_pool_cpu_count
from histdatacom.data_quality import QUALITY_CHECK_GROUPS, QUALITY_EXIT_TRIGGERS
from histdatacom.data_quality.preflight import (
    DEFAULT_QUALITY_PREFLIGHT_SAMPLE_SIZE,
)
from histdatacom.data_quality.profiles import (
    QualityProfileError,
    load_quality_profile_file,
    quality_profile_from_mapping,
)
from histdatacom.fx_enums import (
    Format,
    Pairs,
    Timeframe,
    expand_pair_selection,
    normalize_pair_group,
    pair_group_names,
    get_valid_format_timeframes,
)
from histdatacom.verbosity import normalize_verbosity
from histdatacom.utils import (
    get_current_datemonth_gmt_minus5,
    get_year_from_datemonth,
    get_month_from_datemonth,
    replace_date_punct,
)


def _non_negative_int(value: str) -> int:
    """Return a non-negative integer for argparse thresholds."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not an integer"
        ) from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_int(value: str) -> int:
    """Return a positive integer for argparse counts."""
    parsed = _non_negative_int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


class ArgParser(argparse.ArgumentParser):  # noqa:H601
    """Encapsulation class for argparse related operations."""

    def __init__(self, options: Options | None = None):
        """Set up argparse.

        bring in defaults DTO, setup cli params, receive
        and overwrite defaults with user cli args.

        Args:
            options (Options): runtime option namespace. A fresh namespace is
                allocated when omitted.
        """
        # init _HistDataCom.ArgParser to extend argparse.ArgumentParser
        argparse.ArgumentParser.__init__(
            self,
            prog="histdatacom",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=(
                "Commands:\n"
                "  analytics   Run offline data analytics operations\n"
                "  cleanup     Remove transient source artifacts\n"
                "  jobs        Inspect and control orchestrated work\n"
                "  runtime     Inspect and manage the orchestration runtime\n\n"
                "Run `histdatacom analytics --help` for analytics commands.\n"
                "Run `histdatacom cleanup --help` for cleanup commands.\n"
                "Run `histdatacom jobs --help` for job telemetry commands."
            ),
        )
        # bring in the defaults arg DTO from outer class, use the
        # __dict__ representation of it to set argparse argument defaults.

        self.arg_namespace = options if options is not None else Options()
        self._default_args = self.arg_namespace.to_dict()
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
            self.arg_namespace.build_cache = False
            self.arg_namespace.import_to_influxdb = False
            self.arg_namespace.formats = {"ascii"}
            self.arg_namespace.timeframes = {"T"}

    def _expand_pair_groups(self) -> None:
        """Expand named instrument groups into normal pair selections."""
        pair_groups: set[str] = set(
            getattr(self.arg_namespace, "pair_groups", ()) or ()
        )
        if not pair_groups:
            return
        try:
            self.arg_namespace.pairs = set(
                expand_pair_selection(self.arg_namespace.pairs, pair_groups)
            )
        except ValueError as exc:
            print(str(exc))  # noqa:T201
            raise SystemExit(1) from exc

    def _check_quality_mode(self) -> None:
        """Validate offline data quality mode inputs."""
        quality_requested = bool(
            self.arg_namespace.data_quality
            or self.arg_namespace.repo_quality_refresh
            or self.arg_namespace.quality_preflight
        )
        if (
            self.arg_namespace.repo_quality_columns
            and not self.arg_namespace.available_remote_data
            and not self.arg_namespace.update_remote_data
        ):
            print("--repo-quality-columns requires -A or -U")  # noqa:T201
            raise SystemExit(1)

        if not quality_requested:
            if self.arg_namespace.quality_paths:
                print(  # noqa:T201
                    "--quality-target requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            quality_groups = set(self.arg_namespace.quality_check_groups or ())
            if quality_groups and quality_groups != {"all"}:
                print(  # noqa:T201
                    "--quality-checks requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_report_path:
                print(  # noqa:T201
                    "--quality-report requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_fail_on != "error":
                print(  # noqa:T201
                    "--quality-fail-on requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_max_errors != 0:
                print(  # noqa:T201
                    "--quality-max-errors requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_max_warnings != 0:
                print(  # noqa:T201
                    "--quality-max-warnings requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_profile_path:
                print(  # noqa:T201
                    "--quality-profile requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_profile:
                print(  # noqa:T201
                    "quality_profile requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_preflight_evidence_path:
                print(  # noqa:T201
                    "--quality-preflight-evidence requires --quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_preflight_report_path:
                print(  # noqa:T201
                    "--quality-preflight-report requires --quality-preflight"
                )
                raise SystemExit(1)
            if (
                self.arg_namespace.quality_preflight_sample_size
                != DEFAULT_QUALITY_PREFLIGHT_SAMPLE_SIZE
            ):
                print(  # noqa:T201
                    "--quality-preflight-sample-size requires "
                    "--quality-preflight"
                )
                raise SystemExit(1)
            return

        if self.arg_namespace.quality_preflight and (
            self.arg_namespace.data_quality
            or self.arg_namespace.repo_quality_refresh
        ):
            print(  # noqa:T201
                "--quality-preflight cannot be combined with --quality or "
                "--repo-quality"
            )
            raise SystemExit(1)

        legacy_flags = {
            "-A/--available_remote_data": (
                self.arg_namespace.available_remote_data
            ),
            "-U/--update_remote_data": self.arg_namespace.update_remote_data,
            "-V/--validate_urls": self.arg_namespace.validate_urls,
            "-D/--download_data_archives": (
                self.arg_namespace.download_data_archives
            ),
            "-X/--extract_csvs": self.arg_namespace.extract_csvs,
            "-C/--build-cache": self.arg_namespace.build_cache,
            "-I/--import_to_influxdb": self.arg_namespace.import_to_influxdb,
        }
        conflicts = [flag for flag, enabled in legacy_flags.items() if enabled]
        if conflicts:
            print(  # noqa:T201
                "quality mode is an offline operation and cannot be combined "
                f"with {', '.join(conflicts)}"
            )
            raise SystemExit(1)

        if self.arg_namespace.quality_preflight:
            if len(tuple(self.arg_namespace.quality_paths or ())) > 1:
                print(  # noqa:T201
                    "--quality-preflight accepts one --quality-target "
                    "directory"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_report_path:
                print(  # noqa:T201
                    "--quality-report requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_fail_on != "error":
                print(  # noqa:T201
                    "--quality-fail-on requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_max_errors != 0:
                print(  # noqa:T201
                    "--quality-max-errors requires --quality or --repo-quality"
                )
                raise SystemExit(1)
            if self.arg_namespace.quality_max_warnings != 0:
                print(  # noqa:T201
                    "--quality-max-warnings requires --quality or "
                    "--repo-quality"
                )
                raise SystemExit(1)

        if (
            self.arg_namespace.quality_preflight_evidence_path
            and not self.arg_namespace.data_quality
        ):
            print(  # noqa:T201
                "--quality-preflight-evidence requires --quality"
            )
            raise SystemExit(1)

        if not self.arg_namespace.quality_paths:
            self.arg_namespace.quality_paths = (
                self.arg_namespace.data_directory,
            )
        self._load_quality_profile()

    def _load_quality_profile(self) -> None:
        """Validate and embed an operator quality profile, when configured."""
        try:
            if self.arg_namespace.quality_profile_path:
                profile = load_quality_profile_file(
                    self.arg_namespace.quality_profile_path
                )
            elif self.arg_namespace.quality_profile:
                profile = quality_profile_from_mapping(
                    self.arg_namespace.quality_profile,
                    source="api-options",
                )
            else:
                return
        except QualityProfileError as exc:
            print(f"quality profile error: {exc}")  # noqa:T201
            raise SystemExit(1) from exc
        self.arg_namespace.quality_profile = profile.to_request_payload()

    def _clean_from_api_args(self) -> list:  # noqa:CCR001
        """Build the args list from api Options.

        Returns:
            list: args
        """
        if (
            self.arg_namespace.data_quality
            or self.arg_namespace.repo_quality_refresh
            or self.arg_namespace.quality_preflight
        ):
            args = [
                *["--data-directory", self.arg_namespace.data_directory],
            ]
            if self.arg_namespace.verbosity:
                args.append("-" + "v" * self.arg_namespace.verbosity)
            if self.arg_namespace.data_quality:
                args.append("--quality")
            if self.arg_namespace.repo_quality_refresh:
                args.append("--repo-quality")
            if self.arg_namespace.quality_preflight:
                args.append("--quality-preflight")
            if self.arg_namespace.quality_paths:
                args.extend(
                    ["--quality-target", *self.arg_namespace.quality_paths]
                )
            if self.arg_namespace.quality_check_groups:
                args.extend(
                    [
                        "--quality-checks",
                        *sorted(self.arg_namespace.quality_check_groups),
                    ]
                )
            if self.arg_namespace.quality_report_path:
                args.extend(
                    ["--quality-report", self.arg_namespace.quality_report_path]
                )
            if self.arg_namespace.quality_profile_path:
                args.extend(
                    [
                        "--quality-profile",
                        self.arg_namespace.quality_profile_path,
                    ]
                )
            if self.arg_namespace.quality_preflight_evidence_path:
                args.extend(
                    [
                        "--quality-preflight-evidence",
                        self.arg_namespace.quality_preflight_evidence_path,
                    ]
                )
            if self.arg_namespace.quality_preflight:
                if self.arg_namespace.quality_preflight_report_path:
                    args.extend(
                        [
                            "--quality-preflight-report",
                            self.arg_namespace.quality_preflight_report_path,
                        ]
                    )
                args.extend(
                    [
                        "--quality-preflight-sample-size",
                        str(self.arg_namespace.quality_preflight_sample_size),
                    ]
                )
                pair_groups = tuple(
                    getattr(self.arg_namespace, "pair_groups", ()) or ()
                )
                pairs = tuple(self.arg_namespace.pairs or ())
                explicit_pairs = set(pairs) != Pairs.list_keys()
                if pair_groups:
                    args.extend(["--pair-groups", *pair_groups])
                    if explicit_pairs:
                        args.extend(["-p", *pairs])
                elif explicit_pairs:
                    args.extend(["-p", *pairs])
                if self.arg_namespace.formats:
                    args.extend(["-f", *self.arg_namespace.formats])
                if self.arg_namespace.timeframes:
                    args.extend(
                        [
                            "-t",
                            *Timeframe.convert_to_values(
                                set(self.arg_namespace.timeframes)
                            ),
                        ]
                    )
            args.extend(
                ["--quality-fail-on", self.arg_namespace.quality_fail_on]
            )
            args.extend(
                [
                    "--quality-max-errors",
                    str(self.arg_namespace.quality_max_errors),
                ]
            )
            args.extend(
                [
                    "--quality-max-warnings",
                    str(self.arg_namespace.quality_max_warnings),
                ]
            )
            return args

        self.arg_namespace.timeframes = Timeframe.convert_to_values(
            self.arg_namespace.timeframes
        )

        pair_groups = tuple(
            getattr(self.arg_namespace, "pair_groups", ()) or ()
        )
        pairs = tuple(self.arg_namespace.pairs or ())
        explicit_pairs = set(pairs) != Pairs.list_keys()
        args = [
            *["--data-directory", self.arg_namespace.data_directory],
            *["-f", *self.arg_namespace.formats],
            *["-t", *self.arg_namespace.timeframes],
            *["-c", self.arg_namespace.cpu_utilization],
            *["-b", self.arg_namespace.batch_size],
        ]
        if pair_groups:
            args.extend(["--pair-groups", *pair_groups])
            if explicit_pairs:
                args.extend(["-p", *pairs])
        else:
            args.extend(["-p", *pairs])
        if self.arg_namespace.verbosity:
            args.append("-" + "v" * self.arg_namespace.verbosity)

        if self.arg_namespace.start_yearmonth:
            args.extend(["-s", self.arg_namespace.start_yearmonth])
        if self.arg_namespace.end_yearmonth:
            args.extend(["-e", self.arg_namespace.end_yearmonth])
        if self.arg_namespace.available_remote_data:
            args.append("-A")
        if self.arg_namespace.update_remote_data:
            args.append("-U")
        if self.arg_namespace.repo_quality_columns:
            args.append("--repo-quality-columns")
        if self.arg_namespace.validate_urls:
            args.append("-V")
        if self.arg_namespace.download_data_archives:
            args.append("-D")
        if self.arg_namespace.extract_csvs:
            args.append("-X")
        if self.arg_namespace.build_cache:
            args.append("--build-cache")
        if self.arg_namespace.import_to_influxdb:
            args.append("-I")
        if self.arg_namespace.delete_after_influx:
            args.append("-d")
        if self.arg_namespace.orchestration_start:
            args.append("--orchestration-start")
        else:
            args.append("--no-orchestration-start")
        if self.arg_namespace.orchestration_keep_runtime:
            args.append("--keep-runtime")
        if not self.arg_namespace.orchestration_wait_result:
            args.append("--submit-only")
        return args

    def _config_args_from_cli(self, cli_args: list[str]) -> list[str]:
        """Return config-file arguments to prepend to explicit CLI args."""
        try:
            config_path = config_path_from_cli_args(cli_args)
            return [
                str(arg)
                for arg in load_cli_config_args(config_path, cli_args=cli_args)
            ]
        except CliConfigError as exc:
            print(f"config error: {exc}")  # noqa:T201
            raise SystemExit(1) from exc

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
            raise SystemExit(1) from err

    def _check_for_ascii_if_api(self) -> None:  # noqa:CCR001
        """Verify ascii csv_format type for api use.

        # noqa: DAR402

        Raises:
            ValueError: if influx, must use ascii
            SystemExit: exit on error
        """
        try:
            if self.arg_namespace.from_api and not (
                self.arg_namespace.data_quality
                or self.arg_namespace.repo_quality_refresh
                or self.arg_namespace.quality_preflight
                or self.arg_namespace.validate_urls
                or self.arg_namespace.download_data_archives
                or self.arg_namespace.extract_csvs
                or self.arg_namespace.build_cache
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
            raise SystemExit(1) from err

    def _check_for_supported_format_timeframe_combination(self) -> None:
        """Reject format/timeframe selections that generate no work."""
        if (
            self.arg_namespace.data_quality
            or self.arg_namespace.repo_quality_refresh
            or self.arg_namespace.quality_preflight
        ):
            return

        formats = self.arg_namespace.formats or set()
        timeframes = self.arg_namespace.timeframes or set()
        has_supported_combination = any(
            timeframe in get_valid_format_timeframes(csv_format)
            for csv_format in formats
            for timeframe in timeframes
        )

        if has_supported_combination:
            return

        requested = ", ".join(
            f"{csv_format}/{timeframe}"
            for csv_format in sorted(formats)
            for timeframe in sorted(timeframes)
        )
        supported = ", ".join(
            f"{csv_format}/{timeframe}"
            for csv_format in sorted(Format.list_values())
            for timeframe in get_valid_format_timeframes(csv_format)
        )
        print(  # noqa:T201
            "ERROR: no supported format/timeframe combinations requested.\n"
            f"Requested: {requested}\n"
            f"Supported: {supported}"
        )
        raise SystemExit(1)

    def _check_for_supported_cache_dimensions(self) -> None:
        """Constrain cache-only mode to dimensions that can produce .data."""
        if not self.arg_namespace.build_cache:
            return

        formats = {str(item).lower() for item in self.arg_namespace.formats}
        timeframes = {str(item) for item in self.arg_namespace.timeframes}
        cache_timeframes = {"M1", "T"}
        selected_cache_timeframes = timeframes & cache_timeframes
        if "ascii" in formats and selected_cache_timeframes:
            self.arg_namespace.formats = {"ascii"}
            self.arg_namespace.timeframes = selected_cache_timeframes
            return

        requested = ", ".join(
            f"{csv_format}/{timeframe}"
            for csv_format in sorted(formats)
            for timeframe in sorted(timeframes)
        )
        print(  # noqa:T201
            "ERROR: --build-cache can only build canonical Polars caches "
            "for ascii/M1 or ascii/T datasets.\n"
            f"Requested: {requested}"
        )
        raise SystemExit(1)

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
        if (
            self.arg_namespace.data_quality
            or self.arg_namespace.repo_quality_refresh
            or self.arg_namespace.quality_preflight
        ):
            return

        if (
            self.arg_namespace.available_remote_data
            or self.arg_namespace.update_remote_data
        ):
            return

        if self.arg_namespace.validate_urls:
            return

        if self.arg_namespace.download_data_archives:
            self.arg_namespace.validate_urls = True

        if self.arg_namespace.extract_csvs:
            self.arg_namespace.validate_urls = True
            self.arg_namespace.download_data_archives = True

        if self.arg_namespace.build_cache:
            self.arg_namespace.validate_urls = True
            self.arg_namespace.download_data_archives = True

        if self.arg_namespace.import_to_influxdb:
            self.arg_namespace.validate_urls = True
            self.arg_namespace.download_data_archives = True
            self.arg_namespace.extract_csvs = True

        if (
            not self.arg_namespace.download_data_archives
            and not self.arg_namespace.extract_csvs
            and not self.arg_namespace.build_cache
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
            raise SystemExit(1) from err

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
            raise SystemExit(1) from err

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
            raise SystemExit(1) from err

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
            raise SystemExit(1) from err

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
            raise SystemExit(1) from err

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
            raise SystemExit(1) from err

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
            raise SystemExit(1) from err

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
                re.match(r"^\d{4}[-_.: ]\d{2}$", yearmonth)
                or re.match(r"^\d{6}$", yearmonth)
                or re.match(r"^\d{4}$", yearmonth)
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
            raise SystemExit(1) from err

    def _set_args(self) -> None:  # noqa:CFQ001
        # pylint: disable=unnecessary-lambda
        """Config CLI arguments and default values."""
        mode_args = self.add_argument_group("Mode")
        config_args = self.add_argument_group("Config")
        influx_args = self.add_argument_group("Influxdb")
        system_args = self.add_argument_group("System")
        orchestration_args = self.add_argument_group("Orchestration")
        quality_args = self.add_argument_group("Data quality")
        info_args = self.add_argument_group("Info")

        info_args.add_argument(
            "-A",
            "--available_remote_data",
            action="store_true",
            help="list data retrievable from histdata.com",
        )
        info_args.add_argument(
            "-U",
            "--update_remote_data",
            action="store_true",
            help="update list of data retrievable from histdata.com",
        )
        info_args.add_argument(
            "--by",
            type=str,
            help=(
                "With -A, -U, to sort --by"
                " [pair_asc, pair_dsc, start_asc, start_dsc]"
            ),
        )
        info_args.add_argument(
            "--version",
            action="store_true",
            help="return current version of histdatacom.",
        )
        mode_args.add_argument(
            "-V",
            "--validate_urls",
            action="store_true",
            help="Check generated list of URLs as valid download locations",
        )
        mode_args.add_argument(
            "-D",
            "--download_data_archives",
            action="store_true",
            help=(
                "download specified pairs/formats/timeframe and"  # noqa:BLK100
                " create data files"
            ),
        )
        mode_args.add_argument(
            "-X",
            "--extract_csvs",
            action="store_true",
            help=(
                "histdata.com delivers zip files."  # noqa:BLK100
                " Use the -X flag to extract them."
            ),
        )
        mode_args.add_argument(
            "-C",
            "--build-cache",
            "--cache-only",
            "--build_cache",
            dest="build_cache",
            action="store_true",
            help=(
                "build canonical Polars .data caches and remove transient "
                "ZIP/CSV sources after each cache is ready"
            ),
        )
        config_args.add_argument(
            "--config",
            dest="config_path",
            type=str,
            metavar="PATH",
            help=(
                "read recurrent-run defaults from a YAML file; explicit CLI "
                "flags override configured values"
            ),
        )
        config_args.add_argument(
            "-p",
            "--pairs",
            nargs="+",
            type=str,
            choices=Pairs.list_keys(),
            help="space separated currency pairs. e.g. -p eurusd usdjpy ...",
            metavar="PAIR",
        )
        config_args.add_argument(
            "--pair-groups",
            "--instrument-groups",
            "--symbol-groups",
            dest="pair_groups",
            nargs="+",
            type=normalize_pair_group,
            choices=pair_group_names(),
            help=(
                "named instrument groups to union with --pairs. "
                "Common groups: majors, minors, crosses, exotics, "
                "major-triangles, metals, commodities, indices"
            ),
            metavar="GROUP",
        )
        config_args.add_argument(
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
        config_args.add_argument(
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
        config_args.add_argument(
            "-s",
            "--start_yearmonth",
            type=(lambda v: self._validate_yearmonth_format(v)),
            help=(
                "set a start year and month for data."  # noqa:BLK100
                " e.g. -s 2000-04 or -s 2015-00"
            ),
        )
        config_args.add_argument(
            "-e",
            "--end_yearmonth",
            type=(lambda v: self._validate_yearmonth_format(v)),
            help=(
                "set an end year and month for data."  # noqa:BLK100
                " e.g. -e 2020-00 or -e 2022-04"
            ),
        )
        influx_args.add_argument(
            "-I",
            "--import_to_influxdb",
            action="store_true",
            help=(
                "import data to influxdb instance."  # noqa:BLK100
                " Use influxdb.yaml to configure."
            ),
        )
        influx_args.add_argument(
            "-d",
            "--delete_after_influx",
            action="store_true",
            help="delete data files after upload to influxdb",
        )
        influx_args.add_argument(
            "-b",
            "--batch_size",
            type=int,
            help="(integer) influxdb write_api batch size. defaults to 5000",
        )
        system_args.add_argument(
            "-c",
            "--cpu_utilization",
            type=str,
            help=(
                '"low", "medium", "high". High uses all'
                " available CPUs OR integer percent 1-200"
            ),  # noqa: E501
        )
        system_args.add_argument(
            "--data-directory",
            type=str,
            help='Directory Used to save data. default is "./data/"',
        )
        system_args.add_argument(
            "-v",
            "--verbose",
            dest="verbosity",
            action="count",
            default=0,
            help=(
                "increase logging verbosity; repeat as -vv for debug and "
                "-vvv for trace"
            ),
        )
        orchestration_args.add_argument(
            "--orchestration-start",
            dest="orchestration_start",
            action="store_true",
            help=(
                "start the local orchestration runtime only when no healthy "
                "runtime is running"
            ),
        )
        orchestration_args.add_argument(
            "--no-orchestration-start",
            dest="orchestration_start",
            action="store_false",
            help=(
                "submit only when a healthy orchestration runtime is already "
                "running"
            ),
        )
        orchestration_args.add_argument(
            "--submit-only",
            dest="orchestration_wait_result",
            action="store_false",
            help="submit the orchestration job without waiting for its result",
        )
        orchestration_args.add_argument(
            "--keep-runtime",
            dest="orchestration_keep_runtime",
            action="store_true",
            help=(
                "leave a runtime started by this command running after the "
                "job completes"
            ),
        )
        orchestration_args.add_argument(
            "--no-keep-runtime",
            dest="orchestration_keep_runtime",
            action="store_false",
            help=(
                "stop a runtime started by this command after waited jobs "
                "complete"
            ),
        )
        quality_args.add_argument(
            "--quality",
            dest="data_quality",
            action="store_true",
            help=(
                "run offline data-quality assessment against local datasets "
                "without contacting HistData.com"
            ),
        )
        quality_args.add_argument(
            "--repo-quality",
            dest="repo_quality_refresh",
            action="store_true",
            help=(
                "run offline data-quality assessment and write bounded "
                "quality summary metadata back to the local .repo file"
            ),
        )
        quality_args.add_argument(
            "--quality-preflight",
            dest="quality_preflight",
            action="store_true",
            help=(
                "benchmark a deterministic sample of existing .data caches "
                "before running a cache-scale quality battery"
            ),
        )
        info_args.add_argument(
            "--repo-quality-columns",
            dest="repo_quality_columns",
            action="store_true",
            help=(
                "include stored data-quality status columns in -A/-U "
                "repository table output"
            ),
        )
        quality_args.add_argument(
            "--quality-target",
            "--quality-path",
            dest="quality_paths",
            nargs="+",
            type=str,
            metavar="PATH",
            help=(
                "local file or directory to assess; supports directories, "
                "HistData ZIP archives, CSV files, XLSX payloads, and .data "
                "cache files"
            ),
        )
        quality_args.add_argument(
            "--quality-checks",
            dest="quality_check_groups",
            nargs="+",
            type=str,
            choices=QUALITY_CHECK_GROUPS,
            metavar="GROUP",
            help=(
                "quality check groups to run; defaults to all. Supported: "
                + ", ".join(QUALITY_CHECK_GROUPS)
            ),
        )
        quality_args.add_argument(
            "--quality-report",
            dest="quality_report_path",
            type=str,
            metavar="PATH",
            help="write the full machine-readable JSON quality report to PATH",
        )
        quality_args.add_argument(
            "--quality-preflight-report",
            dest="quality_preflight_report_path",
            type=str,
            metavar="PATH",
            help=(
                "write the publish-safe JSON quality preflight report to PATH"
            ),
        )
        quality_args.add_argument(
            "--quality-preflight-evidence",
            dest="quality_preflight_evidence_path",
            type=str,
            metavar="PATH",
            help=(
                "use a saved quality preflight JSON report as evidence before "
                "a large cache-backed --quality run"
            ),
        )
        quality_args.add_argument(
            "--quality-preflight-sample-size",
            dest="quality_preflight_sample_size",
            type=_positive_int,
            metavar="COUNT",
            help=(
                "number of cache-size quantile targets to benchmark; "
                f"defaults to {DEFAULT_QUALITY_PREFLIGHT_SAMPLE_SIZE}"
            ),
        )
        quality_args.add_argument(
            "--quality-profile",
            dest="quality_profile_path",
            type=str,
            metavar="PATH",
            help=(
                "read a JSON quality profile with rule thresholds, "
                "severities, and modeling assumptions"
            ),
        )
        quality_args.add_argument(
            "--quality-fail-on",
            dest="quality_fail_on",
            type=str,
            choices=QUALITY_EXIT_TRIGGERS,
            metavar="SEVERITY",
            help=(
                "exit non-zero when configured thresholds are exceeded for "
                "error, warning, or never. Defaults to error"
            ),
        )
        quality_args.add_argument(
            "--quality-max-errors",
            dest="quality_max_errors",
            type=_non_negative_int,
            metavar="COUNT",
            help=(
                "maximum error findings allowed before quality mode exits "
                "non-zero; defaults to 0"
            ),
        )
        quality_args.add_argument(
            "--quality-max-warnings",
            dest="quality_max_warnings",
            type=_non_negative_int,
            metavar="COUNT",
            help=(
                "maximum warning findings allowed before quality mode exits "
                "non-zero when --quality-fail-on warning is selected; "
                "defaults to 0"
            ),
        )

    def _sanitize_input(self) -> None:  # noqa:DAR401
        """Clean user-input before run.

        Raises:
            SystemExit: Exit on no input args.
        """
        # prevent running from cli with no arguments
        if len(sys.argv) == 1 and not self.arg_namespace.from_api:
            self.print_help(sys.stdout)
            raise SystemExit(0)

        if self.arg_namespace.from_api:
            args = self._clean_from_api_args()
            self.parse_args(args, namespace=self.arg_namespace)
        else:
            # Get the args from sys.argv
            cli_args = sys.argv[1:]
            config_args = self._config_args_from_cli(cli_args)
            self.parse_args(
                [*config_args, *cli_args],
                namespace=self.arg_namespace,
            )

        self._expand_pair_groups()
        self._adjust_for_repo_data_request()
        self._check_quality_mode()
        self._check_datetime_input()
        self._check_for_ascii_if_influx()
        self._check_for_ascii_if_api()
        self._check_for_supported_format_timeframe_combination()
        self._check_for_supported_cache_dimensions()
        self.arg_namespace.verbosity = normalize_verbosity(
            self.arg_namespace.verbosity
        )
        get_pool_cpu_count(self.arg_namespace.cpu_utilization)

    def __call__(self) -> Options:
        """Collect and process settings from CLI or API.

        Returns:
            Options: parsed runtime namespace
        """
        self._set_args()
        self._sanitize_input()
        return self.arg_namespace
