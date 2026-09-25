"""Separate generated native-health provider wheel, never source-path loaded."""

from pathlib import Path

try:
    from .broker_permission_wheel import build_permission_wheel
except ImportError:
    from broker_permission_wheel import build_permission_wheel


def build_host_health_wheel(output: Path) -> Path:
    return build_permission_wheel(
        output,
        implementation_path=Path(__file__).with_name(
            "broker_host_health_external.py"
        ),
        extra_capabilities=(
            "gaps.v1",
            "heartbeat.v1",
            "timestamps.broker-event.v1",
        ),
    )
