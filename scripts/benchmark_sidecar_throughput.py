#!/usr/bin/env python
"""Run the issue-180 foreground versus live Temporal throughput benchmark."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Sequence

from histdatacom.sidecar.throughput import (
    LIVE_SIDECAR_THROUGHPUT_ENV,
    DEFAULT_THROUGHPUT_PERIOD,
    default_throughput_benchmark_matrix,
    run_live_sidecar_throughput_benchmark,
    write_throughput_report,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the throughput benchmark parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Compare queue-free foreground runtime and live Temporal sidecar "
            "runtime on the issue-180 non-Influx benchmark matrix."
        )
    )
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--runtime-home", type=Path, required=True)
    parser.add_argument("--data-directory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--temporal-executable",
        type=Path,
        default=None,
        help="Temporal CLI executable; defaults to packaged lookup/env.",
    )
    parser.add_argument(
        "--period",
        default=DEFAULT_THROUGHPUT_PERIOD,
        help="HistData period used for the one-period request matrix.",
    )
    parser.add_argument(
        "--max-work-items-per-batch",
        type=int,
        default=1,
        help="Benchmark batch size; default forces visible child handoff.",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=45.0,
    )
    parser.add_argument(
        "--completion-timeout",
        type=float,
        default=420.0,
    )
    parser.add_argument(
        "--stop-timeout",
        type=float,
        default=30.0,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(f"Run without requiring {LIVE_SIDECAR_THROUGHPUT_ENV}=1."),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the live throughput benchmark."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if (
        not args.force
        and os.environ.get(LIVE_SIDECAR_THROUGHPUT_ENV, "").strip() != "1"
    ):
        parser.error(f"{LIVE_SIDECAR_THROUGHPUT_ENV}=1 is required.")

    scenarios = default_throughput_benchmark_matrix(
        data_directory=args.data_directory,
        period=args.period,
        max_work_items_per_batch=args.max_work_items_per_batch,
    )
    report = run_live_sidecar_throughput_benchmark(
        workspace=args.workspace,
        runtime_home=args.runtime_home,
        data_directory=args.data_directory,
        temporal_executable=args.temporal_executable,
        scenarios=scenarios,
        startup_timeout=args.startup_timeout,
        completion_timeout=args.completion_timeout,
        stop_timeout=args.stop_timeout,
    )
    output = write_throughput_report(report, args.output)
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
