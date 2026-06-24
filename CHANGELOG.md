## Unreleased

## 0.79.0 (2026-06-24)

### Changed

- **temporal-sidecar**: make the Temporal sidecar the default CLI/API runtime.
  The foreground runtime remains available as a compatibility rollback through
  `--foreground` or `Options.use_sidecar = False`, and default sidecar runs
  start the bundled local sidecar when needed.

## 0.78.4 (2022-12-13)

### Fix

- **histdatacom**: address keyboard inturrupt for all but import to influx stages

## 0.78.3 (2022-12-05)

### Fix

- **package**: removed bs4 proxy dependency

## 0.78.2 (2022-12-05)

### Fix

- **cli.py**: sort arguments into logical groups

## 0.78.1 (2022-12-05)

### Fix

- **package**: add [pandas],[arrow], and [jupyter] pip install flags

## 0.78.0 (2022-12-05)

### Feat

- **package**: add --version arg to report version
