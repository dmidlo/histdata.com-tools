"""Fresh-interpreter executor; private launcher, not an external verdict API."""

from __future__ import annotations

import argparse
import signal
from pathlib import Path
from types import FrameType

from .contracts import BrokerConformancePlanV1
from .execution import execute_conformance_case
from .storage import read_text, write_artifact


def main() -> int:
    def terminate(signum: int, frame: FrameType | None) -> None:
        # Unwind the native supervisor's finally/reap before parent escalation.
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser()
    parser.add_argument("plan", type=Path)
    parser.add_argument("case_id")
    parser.add_argument("directory", type=Path)
    parser.add_argument("--trusted-equivalence", action="store_true")
    args = parser.parse_args()
    plan = BrokerConformancePlanV1.from_json(read_text(args.plan))
    evidence = execute_conformance_case(
        plan,
        args.case_id,
        args.directory,
        trusted_equivalence=args.trusted_equivalence,
    )
    write_artifact(args.directory / "evidence.json", evidence)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
