"""Complete diagnostic plans built only from newly generated test bytes."""

from histdatacom.data_quality.training_weight_candidates import (
    TrainingWeightFileV1,
)
from histdatacom.data_quality.training_weight_diagnostic_protocol import (
    TrainingWeightDiagnosticEvidenceKind,
    TrainingWeightDiagnosticInputV1,
    TrainingWeightDiagnosticPlanV1,
)
from histdatacom.data_quality.training_weight_diagnostic_protocol import (
    TrainingWeightDiagnosticInputRole as Role,
)
from tests.fixtures.training_weight_sources import (
    fixture_weight_model,
    fixture_weight_source,
)


def fixture_diagnostic_plan(directory, *, admitted=False):
    root = directory / "training-weight-diagnostic-fixture"
    inputs = []
    for period, dates in (
        ("201001", ("2010-01-04", "2010-01-15")),
        ("201101", ("2011-01-04",)),
        ("201102", ("2011-02-02",)),
    ):
        _, version = fixture_weight_source(
            root / period,
            period=period,
            dates=dates,
            step_seconds=5 if admitted else 60,
        )
        for p in version.partitions:
            inputs.append(
                TrainingWeightDiagnosticInputV1(
                    Role.HISTORICAL_SOURCE,
                    f"{p.symbol.lower()}/{period[:4]}/{int(period[4:])}/.data",
                    TrainingWeightFileV1(
                        p.artifact.path,
                        p.artifact.size_bytes,
                        p.artifact.sha256,
                    ),
                    p.symbol,
                    period,
                    p.row_count,
                )
            )
    model = fixture_weight_model(root / "model")
    inputs.append(
        TrainingWeightDiagnosticInputV1(
            Role.MODEL_INDEX, "model/index.json", model.index_file
        )
    )
    for file in model.source_files:
        symbol = file.path.rsplit("/", 1)[-1].split(".")[0]
        inputs.append(
            TrainingWeightDiagnosticInputV1(
                Role.MODEL_TRAINING_SOURCE,
                f"model/{symbol}.data",
                file,
                symbol,
                "202001",
            )
        )
    return TrainingWeightDiagnosticPlanV1(
        TrainingWeightDiagnosticEvidenceKind.FIXTURE,
        "synthetic-001",
        tuple(sorted(inputs, key=lambda i: (i.role.value, i.relative_path))),
    )
