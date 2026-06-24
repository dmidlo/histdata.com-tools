"""Console entry points for local orchestration operations."""

from __future__ import annotations

from histdatacom.sidecar.cli import (
    build_jobs_parser,
    build_parser,
    jobs_main,
    main,
)

__all__ = ["build_jobs_parser", "build_parser", "jobs_main", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
