# Reconstruction evidence diagnostics

Reconstruction diagnostics are publication-safe projections of retained
HistData evidence. They make the experiment and qualification chain reviewable
without embedding tick rows, residual rows, secrets, or workstation paths.
They do not rerun reconstruction, refit an engine, select a winner, or replace
any machine gate.

The domain contracts are provider-neutral enough to remain stable as the
research architecture evolves. The executable v1 publication path is
deliberately narrower: it accepts HistData.com ASCII tick evidence at timeframe
`T`. OANDA, broker feeds, broker-conditioned output, and alternate historical
providers are later milestones and are refused as current diagnostic inputs.

## Evidence graph and chart families

A `DiagnosticPublicationSpecV1` starts with a strong reference to one
`PoweredQualificationDossierV1`. The builder verifies that dossier's retained
evaluation, experiment, row-free metric trace, benchmark corpus, feed-epoch
definition, and observation-calibration campaign. Optional strong references
may add current HistData product, derived-bar, strategy, plan-execution, or
certification summaries. Local paths exist only in the regeneration spec; the
published source table uses safe relative locators, byte sizes, and SHA-256
identities.

Every `DiagnosticChartBundleV1` contains exactly these families. A family may
use multiple stable `view_id` documents when combining the retained evidence
would create an unreadable or scientifically misleading mixed axis; the bundle
therefore reports both `family_count` and bounded `chart_count`:

1. feed-epoch observation boundaries;
2. point-in-time quality-constraint coverage;
3. observation-operator reconstruction and empirical bounds;
4. point-process residuals;
5. mark calibration and explicit refusal evidence;
6. proper-score qualification power;
7. frozen engine-portfolio weights;
8. carving decision flow;
9. synchronized cross-series reconciliation;
10. protected-split leakage audit;
11. product origin and lineage; and
12. derived-bar and strategy sensitivity.

Missing downstream evidence is not converted to numeric zero. Each view has an
explicit `available`, `limited`, `underpowered`, `refused`, `missing_context`,
`unavailable`, or `empty` status plus reason codes. Point counts are bounded;
when sampling is necessary, selection is deterministic by SHA-256 rank and the
original count remains in the contract.

The current retained #490 evidence can therefore publish the upstream
feed/operator/residual/power/portfolio/split families while honestly marking
product-, carving-, and bar/strategy-dependent families unavailable. A fresh
full campaign in #491 is required before #485 itself can claim complete real
campaign coverage.

## Build and inspect

The base package builds JSON chart data without a plotting dependency. Install
the optional renderer for SVG and PNG output:

```sh
python -m pip install 'histdatacom[viz]'
```

Create a local spec with `qualification_dossier_local_ref`, optional
`additional_artifact_local_refs`, a bounded `max_points_per_chart`, and a
renderer configuration. An empty `formats` array requests JSON only; `svg` and
`png` request deterministic static artifacts.

```sh
histdatacom reconstruction --json diagnostic-build \
  --spec work/diagnostic-spec.json \
  --output-directory work/diagnostics

histdatacom reconstruction --json diagnostic-list \
  --manifest work/diagnostics/reconstruction-diagnostic-publication-<sha256>.json
```

The same operation is available through the typed facade:

```python
from histdatacom.reconstruction import ReconstructionClient

client = ReconstructionClient()
publication = client.publish_diagnostics(
    "work/diagnostic-spec.json",
    output_directory="work/diagnostics",
)
listing = client.diagnostics(
    "work/diagnostics/reconstruction-diagnostic-publication-<sha256>.json"
)
```

`diagnostic-list` verifies the content-addressed publication manifest, chart
bundle, every static artifact receipt, and the chart-to-family binding before
returning a bounded listing.

## Deterministic rendering boundary

Static rendering uses Matplotlib's noninteractive Agg backend, fixed dimensions,
the bundled DejaVu Sans font, a fixed color sequence, stable SVG hash salt, and
fixed metadata. Figure bytes are SHA-256 receipted and write-once. Repeating the
same spec under the same renderer contract and Matplotlib version produces the
same publication identity and bytes. Renderer name, package version, contract
version, and configuration identity are retained so a renderer upgrade is
visible rather than silently rewriting figures.

Matplotlib documents SVG hash-salt control in its
[runtime configuration reference](https://matplotlib.org/stable/users/explain/customizing.html)
and SVG/PNG plus metadata behavior in
[`savefig`](https://matplotlib.org/stable/api/_as_gen/matplotlib.pyplot.savefig.html).
Python 3.10 uses the supported
[Matplotlib 3.10.9 release](https://pypi.org/project/matplotlib/3.10.9/);
newer Python runtimes use the v3.11 renderer line.

Rendered figures are explanatory views. The chart bundle is the authoritative
publication data, and the retained experiment, qualification dossier, and
specialized manifests remain authoritative scientific evidence.
