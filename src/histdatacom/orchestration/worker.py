"""Console entry point for orchestration worker subprocesses."""

from __future__ import annotations

from histdatacom.sidecar.worker import (
    build_temporal_worker,
    default_activities,
    default_workflows,
    main,
    run_temporal_worker,
)

__all__ = [
    "build_temporal_worker",
    "default_activities",
    "default_workflows",
    "main",
    "run_temporal_worker",
]


if __name__ == "__main__":
    raise SystemExit(main())
