"""Explicit generated host clocks; no historical ingress reconstruction."""


class SyntheticHostHealthClock:
    """Advance only when actual host observation boundaries sample the clock.

    This deterministic fixture is not a wall-clock throughput or fsync benchmark.
    Its input is a generated session's declared start, not retained final records.
    """

    def __init__(self, session, step_ns=1_000_000):
        self.wall = session.started_at_utc_ns
        self.monotonic = session.started_at_monotonic_ns
        self.step_ns = step_ns

    def sample(self):
        self.wall += self.step_ns
        self.monotonic += self.step_ns
        return self.wall, self.monotonic
