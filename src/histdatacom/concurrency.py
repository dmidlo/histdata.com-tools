"""CPU worker sizing policy shared by orchestration runtime components."""

from __future__ import annotations

from math import ceil
from multiprocessing import cpu_count


def get_pool_cpu_count(count: str | int | None = None) -> int:  # noqa:CCR001
    """Return the worker count derived from the shared CPU policy.

    The public `-c/--cpu_utilization` sizing contract remains in use by
    orchestration worker and performance policies.
    """
    try:
        real_vcpu_count = cpu_count()

        if count is None:
            resolved_count = real_vcpu_count
        else:
            err_text_cpu_level_err = f"""
                    ERROR on -c {count}  ERROR
                        * Malformed command:
                            - -c cpu must be str:
                                low, medium, or high. or integer percent 1-200
            """
            normalized = str(count)
            match normalized:
                case "low":
                    resolved_count = ceil(real_vcpu_count / 2.5)
                case "medium":
                    resolved_count = ceil(real_vcpu_count / 1.5)
                case "high":
                    resolved_count = real_vcpu_count
                case _:
                    if normalized.isnumeric() and 1 <= int(normalized) <= 200:
                        resolved_count = ceil(
                            real_vcpu_count * (int(normalized) / 100)
                        )
                    else:
                        raise ValueError(err_text_cpu_level_err)

        return (
            resolved_count - 1
            if resolved_count > 2
            else ceil(resolved_count / 2)
        )
    except ValueError as err:
        raise SystemExit(1) from err
