# Generated SDK 1.1 acceptance fixture

This independent distribution implements the public SDK 1.1 method, metadata,
capability and permissions; it is not a renamed SDK 1.0 starter. The new method
increments private state and `open_session` refuses unless it was dispatched.
Three separately registered factories deliberately omit it, return a string, or
return protocol information inconsistent with metadata. No real broker, network,
credentials, source timestamps, batching or empirical qualification is claimed.

Before freezing/building, run `python generate_metadata.py`, then
`python generate_metadata.py --check` using the new development host. Build this
directory with the normal wheel builder. The canonical metadata binds the exact
implementation bytes. Formatting the implementation requires regeneration.

Copy `../broker_sdk_scientific_qualification.py` and the conformance JSONs to a
neutral directory. In a fresh environment with genuine installed host/plugin
wheels, run the copied driver with `python -I -B`:

```
python -I -B driver.py capture --output NEW_DIRECTORY --sdk-version 1.1.0 \
  --driver org.example.sdk11.json --host-wheel-sha256 HOST_SHA256 \
  --plugin-wheel-sha256 PLUGIN_SHA256
```

The same command supports the actual reference/starter 1.0 wheels and their
drivers using `--sdk-version 1.0.0`. The caller installs/uninstalls the exact
temporary plugin distribution and retains process/installer receipts. After
uninstall, run a **new process**, with the same output directory and host hash:

```
python -I -B driver.py offline --output EXISTING_DIRECTORY \
  --host-wheel-sha256 HOST_SHA256
```

Run each SDK 1.1 negative in a fresh process with `method-refusal`, its own
conformance JSON, and a new output directory. These invoke the actual installed
factory under current provider/permission scope and assert the precise refusal.
Do not substitute fixture unit tests for this installed matrix.

The driver prospectively fixes host-health limits, requires independently
replayed QUALIFIED health and anchored native provenance, independently checks
quote count/spread/lexical precision/receive intervals, then checks a nonzero
scientific proposal transformation. Missing source time/batch facts remain
unsupported. It repeats fitting on identical retained inputs, checks fresh
rights denials, corrupts only a copy, and repeats retained replay/material use
without importing the removed plugin. Generated declarations are not provider
permission. Unknown upstream loss remains unknown, even when the host boundary
is qualified. No failed run is retried with weaker thresholds.

Before the first installed run, the resource capacity is fixed at 256 health
observations and 256 buckets: global and per-symbol one-second buckets must not
exhaust a 32-bucket diagnostic allocation during permission-check overhead.
This capacity sizing does not change the one-second clock-discontinuity or
60-second persistence SLO, establish empirical timing, or qualify an earlier
resource-exhausted source-only attempt.
