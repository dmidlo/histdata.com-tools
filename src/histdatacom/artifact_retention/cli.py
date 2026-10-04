"""Explicit managed-store commands; legacy cleanup never selects these APIs.

Canonical stdout and optional report files contain no trailing newline. A
returned receipt records observations, not atomic success or recovered space.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager, redirect_stdout
import os
from pathlib import Path
import re
import stat
import sys
import time
from typing import Iterator, Sequence

from histdatacom.managed_artifact_boundary import (
    assert_unmanaged_mutation_paths,
)

from .canonical import MAX_INTEGER, MAX_WIRE_BYTES, canonical_json
from .contracts import CollectionPlanV1, CollectionReceiptV1, StoreSnapshotV1


class _Interrupted(RuntimeError):
    """Carry actual bounded interruption evidence, never a normal receipt."""


def _path(value: str) -> Path:
    if (
        not value
        or len(value) > 4096
        or "\x00" in value
        or ".." in value.split("/")
    ):
        raise ValueError(
            "an explicit bounded path without traversal is required"
        )
    result = Path(os.path.abspath(value))
    if len(os.fsencode(result)) > 4096 or result == Path("/"):
        raise ValueError("a bounded non-root path is required")
    return result


def _identity(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


@contextmanager
def _parent(path: Path) -> Iterator[int]:
    """Walk real directory components without following ancestor symlinks."""
    if os.name != "posix" or any(
        not hasattr(os, name)
        for name in ("O_DIRECTORY", "O_NOFOLLOW", "O_CLOEXEC", "O_NONBLOCK")
    ):
        raise ValueError(
            "artifact CLI file access requires POSIX no-follow APIs"
        )
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = os.open("/", flags)
    try:
        for part in path.parent.parts[1:]:
            child = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor
    finally:
        os.close(descriptor)


def _read_plan(path: Path) -> CollectionPlanV1:
    with _parent(path) as directory:
        before = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode) or not (
            0 < before.st_size <= MAX_WIRE_BYTES and before.st_nlink == 1
        ):
            raise ValueError("plan must be a bounded single-link regular file")
        descriptor = os.open(
            path.name,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
            dir_fd=directory,
        )
        try:
            if _identity(os.fstat(descriptor)) != _identity(before):
                raise ValueError("plan changed before reading")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                raw = stream.read(MAX_WIRE_BYTES + 1)
            if (
                len(raw) != before.st_size
                or _identity(os.fstat(descriptor)) != _identity(before)
                or _identity(os.stat(path, follow_symlinks=False))
                != _identity(before)
            ):
                raise ValueError("plan changed while reading")
        finally:
            os.close(descriptor)
    return CollectionPlanV1.from_json(raw.decode("ascii"))


def _output_preflight(path: Path) -> None:
    assert_unmanaged_mutation_paths((path,))
    with _parent(path) as directory:
        try:
            os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            return
        raise ValueError("output must be a new exclusive unmanaged file")


def _write_output(path: Path, payload: str) -> None:
    _output_preflight(path)
    with _parent(path) as directory:
        descriptor = os.open(
            path.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=directory,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(payload.encode("ascii"))
                stream.flush()
            os.fsync(descriptor)
            observed = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink != 1
                or observed.st_size != len(payload)
                or _identity(os.stat(path, follow_symlinks=False))
                != _identity(observed)
            ):
                raise ValueError("output changed during report export")
            assert_unmanaged_mutation_paths((path,))
            os.fsync(directory)
        finally:
            os.close(descriptor)


def _inspect(root: Path) -> StoreSnapshotV1:
    from .storage import inspect_retention_store

    return inspect_retention_store(root)


def _plan(root: Path, cutoff: int | None) -> CollectionPlanV1:
    from .storage import plan_artifact_collection

    return plan_artifact_collection(root, cutoff_ns=cutoff)


def _apply(root: Path, plan: CollectionPlanV1) -> CollectionReceiptV1:
    from .apply import CollectionInterruptedError, apply_artifact_collection

    try:
        result: CollectionReceiptV1 = apply_artifact_collection(root, plan)
        return result
    except CollectionInterruptedError as exc:
        raise _Interrupted(canonical_json(exc.to_dict())) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="histdatacom cleanup", allow_abbrev=False
    )
    parser.add_argument(
        "command",
        choices=("artifacts-inspect", "artifacts-plan", "artifacts-apply"),
    )
    parser.add_argument("--store", required=True)
    parser.add_argument("--plan")
    parser.add_argument("--cutoff-ns")
    parser.add_argument("--output")
    parser.add_argument(
        "--json", action="store_true", help="canonical JSON (also the default)"
    )
    return parser


def main(argv: Sequence[str]) -> int:
    """Run only an explicit artifact operation after argument/file admission."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command != "artifacts-plan" and args.cutoff_ns is not None:
        parser.error("--cutoff-ns is only valid with artifacts-plan")
    if args.command == "artifacts-apply":
        if args.plan is None:
            parser.error("artifacts-apply requires --plan")
    elif args.plan is not None:
        parser.error("--plan is only valid with artifacts-apply")
    try:
        root = _path(args.store)
        output = _path(args.output) if args.output is not None else None
        cutoff = None
        if args.cutoff_ns is not None:
            if not re.fullmatch(r"[0-9]{1,19}", args.cutoff_ns):
                raise ValueError(
                    "cutoff must be a positive integer nanosecond time"
                )
            cutoff = int(args.cutoff_ns)
            if not 0 < cutoff <= min(MAX_INTEGER, time.time_ns()):
                raise ValueError(
                    "cutoff must be positive and not in the future"
                )
        plan = _read_plan(_path(args.plan)) if args.plan is not None else None
        if output is not None:
            _output_preflight(output)
        # Native diagnostic prints cannot corrupt shell-redirected JSON.
        with redirect_stdout(sys.stderr):
            if args.command == "artifacts-inspect":
                snapshot = _inspect(root)
                if type(snapshot) is not StoreSnapshotV1:
                    raise ValueError("inspect returned an invalid result type")
                payload = snapshot.to_json()
                StoreSnapshotV1.from_json(payload)
                status = int(bool(snapshot.blockers))
            elif args.command == "artifacts-plan":
                result = _plan(root, cutoff)
                if type(result) is not CollectionPlanV1:
                    raise ValueError("planning returned an invalid result type")
                payload = result.to_json()
                CollectionPlanV1.from_json(payload)
                status = int(bool(result.blockers))
            else:
                assert plan is not None
                receipt = _apply(root, plan)
                if type(receipt) is not CollectionReceiptV1:
                    raise ValueError("apply returned an invalid result type")
                payload = receipt.to_json()
                CollectionReceiptV1.from_json(payload)
                status = int(receipt.status != "complete")
    except _Interrupted as exc:
        payload, status = str(exc), 1
    except (ValueError, OSError) as exc:
        print(
            f"artifact operation refused: {exc}", file=sys.stderr
        )  # noqa:T201
        return 1
    sys.stdout.write(payload)
    sys.stdout.flush()
    if output is not None:
        try:
            _write_output(output, payload)
        except (ValueError, OSError) as exc:
            print(  # noqa:T201
                "operation returned the following receipt, but report export "
                f"failed: {exc}",
                file=sys.stderr,
            )
            return 1
    return status
