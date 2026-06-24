"""Console entry point for local orchestration lifecycle operations."""

from __future__ import annotations

from histdatacom.sidecar.cli import build_parser, main

__all__ = ["build_parser", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
