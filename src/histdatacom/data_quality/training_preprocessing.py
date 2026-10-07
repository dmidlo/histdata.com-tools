"""Public source-replayed research preprocessing and dense-view interface.

Pure transform kernels are intentionally not exported as source authority.
Every fit, projection and artifact reader retains native replay obligations.
"""

from .training_preprocessing_artifacts import (
    read_training_preprocessing_fit,
    write_training_preprocessing_fit,
)
from .training_preprocessing_contracts import (
    PreprocessingFitMode,
    PreprocessingMissingness,
    PreprocessingViewKind,
    TrainingPreprocessingFitStepV1,
    TrainingPreprocessingFitV1,
    TrainingPreprocessingPlanV1,
    TrainingPreprocessingStepV1,
)
from .training_preprocessing_views import (
    TrainingEvidenceViewV1,
    TrainingPreprocessingViewV1,
    apply_training_preprocessing,
    evidence_records,
    fit_training_preprocessing,
    materialize_training_evidence_view,
    materialize_training_preprocessing_view,
    preprocessing_records,
    replay_training_preprocessing_fit,
)

__all__ = [
    "PreprocessingFitMode",
    "PreprocessingMissingness",
    "PreprocessingViewKind",
    "TrainingEvidenceViewV1",
    "TrainingPreprocessingFitStepV1",
    "TrainingPreprocessingFitV1",
    "TrainingPreprocessingPlanV1",
    "TrainingPreprocessingStepV1",
    "TrainingPreprocessingViewV1",
    "apply_training_preprocessing",
    "evidence_records",
    "fit_training_preprocessing",
    "materialize_training_evidence_view",
    "materialize_training_preprocessing_view",
    "preprocessing_records",
    "read_training_preprocessing_fit",
    "replay_training_preprocessing_fit",
    "write_training_preprocessing_fit",
]
