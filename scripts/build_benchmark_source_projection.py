#!/usr/bin/env python3
"""Build deterministic, lineage-bound benchmark source projections."""

from __future__ import annotations

import argparse
from pathlib import Path

from histdatacom.synthetic.benchmark_source_projection import (
    BenchmarkSourceProjectionManifestV1,
    build_benchmark_source_projection_manifest,
    write_benchmark_source_projection_manifest,
)


def _partition(value: str) -> tuple[tuple[str, str], str]:
    parts = value.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            "partition must use SYMBOL:YYYYMM:SHA256"
        )
    symbol, period, digest = parts
    return (symbol, period), digest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent-root", required=True, type=Path)
    parser.add_argument("--projection-root", required=True, type=Path)
    parser.add_argument("--manifest-directory", required=True, type=Path)
    parser.add_argument("--frozen-at-utc", required=True)
    parser.add_argument(
        "--partition",
        action="append",
        required=True,
        type=_partition,
        metavar="SYMBOL:YYYYMM:SHA256",
        help="repeat for each explicitly pinned parent partition",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    pinned = dict(args.partition)
    if len(pinned) != len(args.partition):
        raise ValueError("benchmark source projection partition is duplicated")
    projections = []
    for axis, digest in pinned.items():
        partial = build_benchmark_source_projection_manifest(
            args.parent_root,
            args.projection_root,
            expected_parent_sha256={axis: digest},
            frozen_at_utc=args.frozen_at_utc,
        )
        projection = partial.projections[0]
        projections.append(projection)
        print(
            f"verified {projection.symbol}:{projection.period} "
            f"{projection.projected_sha256}",
            flush=True,
        )
    manifest = BenchmarkSourceProjectionManifestV1(
        projections=tuple(projections), frozen_at_utc=args.frozen_at_utc
    )
    artifact = write_benchmark_source_projection_manifest(
        manifest, args.manifest_directory
    )
    print(artifact.path)
    print(manifest.manifest_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
