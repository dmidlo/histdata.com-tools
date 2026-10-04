"""Public, explicit research interface for uncertainty-preserving views.

Native source replay is performed by the operations, not by constructing a
contract. These functions do not require a callable package or CLI namespace.
"""

from .training_scenario_artifacts import (
    read_training_scenario_artifact,
    write_training_scenario_artifact,
)
from .training_scenario_contracts import (
    ScenarioAxis,
    ScenarioAxisState,
    ScenarioMemberStatus,
    ScenarioViewKind,
    TrainingScenarioCampaignBindingV1,
    TrainingScenarioPlanV1,
    TrainingScenarioPolicyV1,
    TrainingScenarioRationalV1,
    TrainingScenarioRequestV1,
    TrainingScenarioResearchBindingV1,
    TrainingScenarioViewV1,
)
from .training_scenario_views import (
    materialize_training_scenario_view,
    replay_training_scenario_view,
)

__all__ = [
    "ScenarioAxis",
    "ScenarioAxisState",
    "ScenarioMemberStatus",
    "ScenarioViewKind",
    "TrainingScenarioCampaignBindingV1",
    "TrainingScenarioPlanV1",
    "TrainingScenarioPolicyV1",
    "TrainingScenarioRationalV1",
    "TrainingScenarioRequestV1",
    "TrainingScenarioResearchBindingV1",
    "TrainingScenarioViewV1",
    "materialize_training_scenario_view",
    "read_training_scenario_artifact",
    "replay_training_scenario_view",
    "write_training_scenario_artifact",
]
