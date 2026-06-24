# Temporal CLI Notice

HistDataCom platform wheels may bundle the Temporal CLI executable as the
local orchestration runtime used by `histdatacom runtime`.

- Upstream project: https://github.com/temporalio/cli
- License: MIT
- License file: `third-party/temporal-cli/LICENSE`
- Release artifacts: https://github.com/temporalio/cli/releases
- Bundled-wheel provenance file: `temporal-cli-provenance.json`

Metadata-only source distributions and universal fallback wheels do not bundle
the Temporal CLI executable. Platform wheel builds add executable-specific
provenance during release packaging.
