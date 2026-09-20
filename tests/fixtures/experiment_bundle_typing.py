"""Installed public experiment consumer; never imports implementation helpers."""

from pathlib import Path

from histdatacom.experiments import (
    ExperimentBundleV1,
    ExperimentRegistryV1,
    assert_promotion_admissible,
    read_experiment_artifact,
    render_report_markdown,
    reverse_dependencies,
    scientific_differences,
    write_experiment_artifact,
)


def public_registry(
    path: Path, output_directory: Path
) -> tuple[ExperimentRegistryV1, Path, str]:
    registry = read_experiment_artifact(path, ExperimentRegistryV1)
    rendered = (
        render_report_markdown(registry, registry.reports[0])
        if registry.reports
        else ""
    )
    for promotion in registry.promotions:
        assert_promotion_admissible(registry, promotion)
    saved = write_experiment_artifact(registry, output_directory)
    return registry, saved, rendered


def public_lineage(
    registry: ExperimentRegistryV1,
    left: ExperimentBundleV1,
    right: ExperimentBundleV1,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    return (
        scientific_differences(left, right),
        reverse_dependencies(registry, left.experiment_id),
    )
