"""Conditional cross-feed diagnostics; no live acquisition or retention claim.

Public producers use actual sealed native captures. Pure kernels are available
in their implementation modules for synthetic tests, never as admission grants.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "RationalV1": "_wire",
    "NativeCaptureRefV1": "native",
    "admit_capture": "native",
    "ClockCandidatePairV1": "clock_contracts",
    "ClockFitPolicyV1": "clock_contracts",
    "ClockFitRequestV1": "clock_contracts",
    "ClockModelV1": "clock_contracts",
    "MatchingPolicyV1": "matching_contracts",
    "MatchingReportV1": "matching_contracts",
    "MatchingTruthV1": "matching_contracts",
    "MatchingEvaluationV1": "matching_contracts",
    "MatchingSensitivityV1": "matching_contracts",
    "fit_clock_model": "api",
    "replay_clock_model": "api",
    "match_captures": "api",
    "evaluate_matching": "evaluation",
    "evaluate_matching_sensitivity": "evaluation",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(name)
    return getattr(import_module(f"{__name__}.{_EXPORTS[name]}"), name)
