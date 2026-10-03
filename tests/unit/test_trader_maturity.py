"""Generated structural claims; never evidence of actual trader execution."""

from dataclasses import FrozenInstanceError, fields, replace
import json

import pytest

from histdatacom.synthetic import trader_maturity as module
from histdatacom.synthetic.capability_certification import (
    CapabilityCertificationSpecV1,
)
from histdatacom.synthetic.capability_matrix import (
    CAPABILITY_CATALOG_ID,
    CAPABILITY_REQUIREMENT_IDS,
    CapabilityClaimKindV1,
    CapabilityEvidenceScopeV1 as Scope,
    CapabilityMatrixPolicyV1,
    CapabilityMatrixRequirementV1,
    CapabilityMatrixRowV1,
    CapabilityMatrixV1,
    CapabilityStateV1 as State,
    structural_matrix_blockers,
)
from histdatacom.synthetic.trader_maturity import (
    MAX_TEXT_LENGTH,
    MAX_WIRE_BYTES,
    TRADER_MATURITY_CATALOG,
    TRADER_MATURITY_CATALOG_ID,
    TRADER_MATURITY_PASS_SCOPES,
    TRADER_MATURITY_STAGE_IDS,
    TraderCatalogIdentityV1 as Catalog,
    TraderMaturityEvidenceV1 as Evidence,
    TraderMaturityMatrixV1 as Matrix,
    TraderMaturityRowV1 as Row,
    TraderReferenceAvailabilityV1 as Availability,
    render_trader_maturity_markdown,
    validate_parent,
)

NOW = "2026-10-03T12:00:00Z"
RELEASE = "generated-test-only-release"
SOURCE_SHA = "a" * 64
CANONICAL_SHA = "b" * 64
COMMIT = "c" * 40
EXECUTION = "generated-execution:sha256:" + "d" * 64
VERIFICATION = "generated-verification:sha256:" + "e" * 64
CAMPAIGN = "generated-campaign:sha256:" + "f" * 64
PRODUCT = "generated-product:sha256:" + "1" * 64
DATASET = "generated-dataset:sha256:" + "2" * 64


def reset(record, **changes):
    changes[record._ID_FIELD] = ""
    return replace(record, **changes)


def parent(dataset=None):
    policy = CapabilityMatrixPolicyV1(
        RELEASE,
        dataset,
        CapabilityClaimKindV1.NARROWER_PREDECLARED,
        "Generated structural test",
        "One software row",
        ("No evidence",),
        (
            CapabilityMatrixRequirementV1(
                "model_bank_registration", Scope.SOFTWARE
            ),
        ),
        (),
    )
    rows = tuple(
        CapabilityMatrixRowV1(
            name,
            policy.policy_id,
            None,
            None,
            None,
            State.NOT_IMPLEMENTED,
            Scope.NONE,
            "Generated structural claim",
            ("Not a scientific result",),
            (),
            None,
            RELEASE,
            dataset,
        )
        for name in CAPABILITY_REQUIREMENT_IDS
    )
    return (
        CapabilityMatrixV1(policy.policy_id, RELEASE, dataset, NOW, rows),
        policy,
    )


def catalog(retained=False):
    return Catalog(
        SOURCE_SHA,
        CANONICAL_SHA,
        (
            Availability.RETAINED_REFERENCE
            if retained
            else Availability.DECLARED_UNAVAILABLE
        ),
        "generated-source:sha256:" + SOURCE_SHA if retained else None,
    )


def evidence(executed=False, passed=False):
    return Evidence(
        COMMIT if executed else None,
        EXECUTION if executed else None,
        VERIFICATION if passed else None,
        None,
        None,
        (
            Availability.RETAINED_REFERENCE
            if executed
            else Availability.DECLARED_UNAVAILABLE
        ),
        ("https://example.invalid/generated-reference",),
    )


def row(stage="archive_integrity", state=State.NOT_IMPLEMENTED):
    executed = state in (
        State.EXECUTED_FAILED,
        State.EXECUTED_INSUFFICIENT_EVIDENCE,
        State.EXECUTED_PASSED,
    )
    bound = evidence(executed, state is State.EXECUTED_PASSED)
    if state is State.IMPLEMENTED_UNEXECUTED:
        bound = reset(bound, implementation_commit=COMMIT)
    if state is State.EXECUTED_PASSED and stage in (
        "representative_campaign",
        "complete_population_campaign",
        "ml_incremental_value",
    ):
        bound = reset(bound, campaign_id=CAMPAIGN)
    if (
        state is State.EXECUTED_PASSED
        and stage == "wide_corpus_population_columns"
    ):
        bound = reset(bound, product_id=PRODUCT)
    return Row(
        stage,
        state,
        (
            dict(TRADER_MATURITY_PASS_SCOPES)[stage][0]
            if state is State.EXECUTED_PASSED
            else Scope.SOFTWARE if executed else Scope.NONE
        ),
        "Generated structural claim",
        ("No actual trader execution",),
        (680,) if state is State.DEFERRED_BLOCKED else (),
        NOW if state is State.EXECUTED_PASSED else None,
        bound,
    )


def matrix(*replacements, retained=False, dataset=None):
    native, _ = parent(dataset)
    changed = {item.stage_id: item for item in replacements}
    return Matrix(
        native.matrix_id,
        native.release_id,
        native.dataset_id,
        NOW,
        catalog(retained),
        tuple(
            changed.get(name) or row(name) for name in TRADER_MATURITY_STAGE_IDS
        ),
    )


def test_sixteen_closed_stages_are_separate_from_released_ninety_three():
    assert (
        len(TRADER_MATURITY_CATALOG)
        == len(set(TRADER_MATURITY_STAGE_IDS))
        == 16
    )
    assert TRADER_MATURITY_STAGE_IDS == (
        "archive_integrity",
        "catalog_identity",
        "dsl_compiler",
        "point_in_time_snapshot",
        "execution_account_ledger",
        "production_catalog_compilation",
        "portfolio_exposure_equivalence",
        "strategy_response_features",
        "synthetic_flow_aggregation",
        "population_fingerprint",
        "atomic_persistence_cli_replay",
        "branch_integration",
        "representative_campaign",
        "complete_population_campaign",
        "ml_incremental_value",
        "wide_corpus_population_columns",
    )
    assert len(CAPABILITY_REQUIREMENT_IDS) == 93
    assert (
        CAPABILITY_CATALOG_ID
        == "capability-catalog:sha256:bdb148a1f952609698df6a73d2cdb72a1ba69050b3ea24b7a9863f8b948c0c16"
    )
    assert TRADER_MATURITY_CATALOG_ID != CAPABILITY_CATALOG_ID


@pytest.mark.parametrize("record", [catalog(), evidence(), row(), matrix()])
def test_all_records_strict_roundtrip_detachment_and_immutability(record):
    wire = record.to_json()
    assert type(record).from_json(wire) == record
    assert type(record).from_dict(record.to_dict()) == record
    detached = record.to_dict()
    detached[record._ID_FIELD] = "bad"
    assert record.to_json() == wire
    with pytest.raises(FrozenInstanceError):
        setattr(record, record._ID_FIELD, "bad")
    with pytest.raises(ValueError, match="identity differs"):
        type(record).from_dict(detached)


@pytest.mark.parametrize(
    "state",
    [value for value in State if value is not State.WAIVED_WITH_LIMITATION],
)
def test_six_admitted_recorded_states_roundtrip_without_authority(state):
    actual = matrix(row(state=state))
    assert Matrix.from_json(actual.to_json()) == actual
    assert actual.claim_kind == "non_authoritative_supplement"
    assert actual.rows[0].state is state
    assert all(item.state is State.NOT_IMPLEMENTED for item in actual.rows[1:])


def test_waivers_are_rejected_without_a_new_frozen_policy():
    with pytest.raises(ValueError, match="no waiver policy"):
        row(state=State.WAIVED_WITH_LIMITATION)


@pytest.mark.parametrize("stage", TRADER_MATURITY_STAGE_IDS)
def test_each_stage_is_required_no_missing_duplicate_or_inherited_claim(stage):
    actual = matrix()
    with pytest.raises(ValueError, match="sixteen"):
        reset(
            actual,
            rows=tuple(item for item in actual.rows if item.stage_id != stage),
        )
    duplicate = next(item for item in actual.rows if item.stage_id != stage)
    with pytest.raises(ValueError, match="duplicates or omits"):
        reset(
            actual,
            rows=tuple(
                duplicate if item.stage_id == stage else item
                for item in actual.rows
            ),
        )


def test_order_canonicalization_changes_no_scientific_claim():
    actual = matrix()
    assert (
        reset(actual, rows=tuple(reversed(actual.rows))).to_json()
        == actual.to_json()
    )
    unordered = actual.to_dict()
    unordered["rows"].reverse()
    with pytest.raises(ValueError, match="not canonical"):
        Matrix.from_json(
            json.dumps(unordered, sort_keys=True, separators=(",", ":"))
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("parent_matrix_id", "generated-parent:sha256:" + "9" * 64),
        ("release_id", "another-release"),
        ("dataset_id", DATASET),
    ],
)
def test_parent_binding_rejects_each_identity_mismatch(field, value):
    actual = reset(matrix(), **{field: value})
    with pytest.raises(ValueError, match="parent/release/dataset"):
        validate_parent(actual, parent()[0])


def test_parent_readmission_and_presentation_do_not_mutate_v1_or_certify():
    native, policy = parent()
    wire = native.to_json()
    blockers = structural_matrix_blockers(native, policy)
    actual = matrix(row(state=State.EXECUTED_PASSED))
    admitted, detached = validate_parent(actual, native)
    assert admitted == actual and admitted is not actual
    assert detached == native and detached is not native
    assert native.to_json() == wire
    assert structural_matrix_blockers(native, policy) == blockers
    assert (
        next(
            item
            for item in native.rows
            if item.requirement_id == "synthetic_orderflow"
        ).state
        is State.NOT_IMPLEMENTED
    )
    with pytest.raises(ValueError, match="schema or field"):
        CapabilityMatrixV1.from_json(actual.to_json())
    with pytest.raises(TypeError):
        structural_matrix_blockers(actual, policy)
    with pytest.raises(TypeError, match="exact policy and matrix"):
        CapabilityCertificationSpecV1(
            policy=policy, matrix=actual, declarations=()
        )
    with pytest.raises(TypeError):
        validate_parent(actual, actual)


def test_parent_is_readmitted_without_calling_hostile_serializer():
    native, _ = parent()
    object.__setattr__(
        native, "matrix_id", "generated-parent:sha256:" + "9" * 64
    )
    with pytest.raises(ValueError, match="canonical content"):
        validate_parent(matrix(), native)


def test_parent_subclass_is_refused_before_serializer():
    class Hostile(CapabilityMatrixV1):
        def to_json(self):
            raise AssertionError("untrusted serializer")

    native, _ = parent()
    hostile = object.__new__(Hostile)
    for field in fields(native):
        object.__setattr__(hostile, field.name, getattr(native, field.name))
    with pytest.raises(TypeError, match="exact capability"):
        validate_parent(matrix(), hostile)


def test_declared_catalog_hashes_do_not_satisfy_retained_claim():
    actual = matrix(row("catalog_identity", State.DEFERRED_BLOCKED))
    assert actual.catalog.source_artifact_id is None
    assert (
        actual.catalog.source_json_sha256
        != actual.catalog.canonical_catalog_sha256
    )
    with pytest.raises(ValueError, match="retained catalog"):
        matrix(row("catalog_identity", State.EXECUTED_PASSED))
    retained = matrix(
        row("catalog_identity", State.EXECUTED_PASSED), retained=True
    )
    assert retained.rows[1].state is State.EXECUTED_PASSED
    assert all(
        item.state is State.NOT_IMPLEMENTED for item in retained.rows[2:]
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"source_json_sha256": "short"},
        {"canonical_catalog_sha256": True},
        {"availability": "declared_unavailable"},
        {"source_artifact_id": "source:sha256:" + SOURCE_SHA},
        {"availability": Availability.RETAINED_REFERENCE},
        {
            "availability": Availability.RETAINED_REFERENCE,
            "source_artifact_id": "source:sha256:" + CANONICAL_SHA,
        },
    ],
)
def test_catalog_exact_identity_and_availability_admission(changes):
    with pytest.raises((ValueError, TypeError)):
        reset(catalog(), **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"implementation_commit": "short"},
        {"execution_artifact_id": True},
        {"independent_verification_artifact_id": VERIFICATION},
        {"availability": Availability.RETAINED_REFERENCE},
        {"references": ["not-a-tuple"]},
        {"references": ("same", "same")},
        {"references": ("x" * (MAX_TEXT_LENGTH + 1),)},
        {"references": tuple(str(number) for number in range(17))},
    ],
)
def test_evidence_exact_bounded_identity_admission(changes):
    with pytest.raises((ValueError, TypeError)):
        reset(evidence(), **changes)


def test_execution_cannot_independently_verify_itself():
    with pytest.raises(ValueError, match="distinct execution"):
        reset(
            evidence(True, True), independent_verification_artifact_id=EXECUTION
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"stage_id": "unknown"},
        {"state": "not_implemented"},
        {"evidence_scope": "none"},
        {"scope": ""},
        {"scope": " leading"},
        {"limitations": ["mutable"]},
        {"blocking_issues": (True,)},
        {"blocking_issues": (0,)},
        {"blocking_issues": (680, 669)},
        {"blocking_issues": (669, 669)},
        {"last_verified_at_utc": NOW},
        {"last_verified_at_utc": "2026-10-03"},
        {"evidence": object()},
    ],
)
def test_row_rejects_malformed_claims(changes):
    with pytest.raises((ValueError, TypeError)):
        reset(row(), **changes)


def test_blocked_historical_execution_still_requires_implementation_and_scope():
    actual = row(state=State.DEFERRED_BLOCKED)
    for bound, scope in (
        (reset(evidence(), execution_artifact_id=EXECUTION), Scope.NONE),
        (evidence(True), Scope.NONE),
        (reset(evidence(), execution_artifact_id=EXECUTION), Scope.SOFTWARE),
    ):
        with pytest.raises(ValueError, match="implementation and scope"):
            reset(actual, evidence=bound, evidence_scope=scope)
    admitted = reset(
        actual, evidence=evidence(True), evidence_scope=Scope.SOFTWARE
    )
    assert admitted.state is State.DEFERRED_BLOCKED
    assert admitted.blocking_issues == (680,)


@pytest.mark.parametrize(
    "stage,scope",
    [
        (stage, scope)
        for stage, allowed in TRADER_MATURITY_PASS_SCOPES
        for scope in (Scope.SOFTWARE, Scope.BOUNDED, Scope.COMPLETE)
        if scope not in allowed
    ],
)
def test_each_recorded_pass_scope_is_closed_and_not_an_implicit_superscope(
    stage, scope
):
    with pytest.raises(ValueError, match="inadmissible evidence scope"):
        reset(row(stage, State.EXECUTED_PASSED), evidence_scope=scope)


def test_pass_scope_meanings_are_bound_into_supplement_catalog_identity():
    import hashlib

    payload = {
        "schema_version": "histdatacom.trader-maturity-catalog.v1",
        "stages": TRADER_MATURITY_CATALOG,
        "authority": "none",
        "stage_inference": "none",
        "waivers": "not_admitted",
        "pass_scopes": TRADER_MATURITY_PASS_SCOPES,
        "retained_catalog_required_for_pass": module._CATALOG_REQUIRED_FOR_PASS,
        "campaign_id_required_for_pass": module._CAMPAIGN_REQUIRED_FOR_PASS,
        "product_and_dataset_required_for_pass": module._PRODUCT_DATASET_REQUIRED_FOR_PASS,
        "pass_evidence": "own-implementation-retained-execution-distinct-verification-time-no-blockers-v1",
    }
    expected = hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("ascii")
    ).hexdigest()
    assert (
        TRADER_MATURITY_CATALOG_ID
        == "trader-maturity-catalog:sha256:" + expected
    )


@pytest.mark.parametrize(
    "state",
    [
        State.EXECUTED_FAILED,
        State.EXECUTED_INSUFFICIENT_EVIDENCE,
        State.EXECUTED_PASSED,
    ],
)
def test_execution_states_require_own_retained_evidence_not_a_locator(state):
    with pytest.raises(ValueError):
        reset(row(), state=state)
    actual = row(state=state)
    with pytest.raises(ValueError, match="retained scoped"):
        reset(
            actual,
            evidence=reset(
                actual.evidence, availability=Availability.DECLARED_UNAVAILABLE
            ),
        )
    with pytest.raises(ValueError, match="retained scoped"):
        reset(actual, evidence_scope=Scope.NONE)


def test_pass_cannot_hide_blocker_or_lose_verification_time():
    actual = row(state=State.EXECUTED_PASSED)
    with pytest.raises(ValueError, match="own verification"):
        reset(actual, blocking_issues=(680,))
    with pytest.raises(ValueError, match="verification time"):
        reset(actual, last_verified_at_utc=None)
    with pytest.raises(ValueError, match="verification reference"):
        reset(
            actual,
            evidence=reset(
                actual.evidence, independent_verification_artifact_id=None
            ),
        )


@pytest.mark.parametrize(
    "stage",
    [
        "representative_campaign",
        "complete_population_campaign",
        "ml_incremental_value",
    ],
)
def test_campaign_pass_requires_exact_campaign_binding(stage):
    actual = row(stage, State.EXECUTED_PASSED)
    with pytest.raises(ValueError, match="campaign identity"):
        reset(actual, evidence=reset(actual.evidence, campaign_id=None))


def test_wide_corpus_pass_requires_product_and_matching_dataset():
    actual = row("wide_corpus_population_columns", State.EXECUTED_PASSED)
    with pytest.raises(ValueError, match="product identity"):
        reset(actual, evidence=reset(actual.evidence, product_id=None))
    with pytest.raises(ValueError, match="dataset identity"):
        matrix(actual, retained=True)
    supplied = matrix(actual, retained=True, dataset=DATASET)
    assert validate_parent(supplied, parent(DATASET)[0])[0] == supplied


@pytest.mark.parametrize(
    "changes",
    [
        {"catalog_id": "catalog:sha256:" + "3" * 64},
        {"claim_kind": "certified"},
        {"rows": []},
        {"catalog": object()},
        {"as_of_utc": "2026-02-30T12:00:00Z"},
    ],
)
def test_matrix_refuses_malformed_or_authoritative_claims(changes):
    with pytest.raises((ValueError, TypeError)):
        reset(matrix(), **changes)


def test_snapshot_time_cannot_precede_parent_or_verification():
    with pytest.raises(ValueError, match="predates"):
        validate_parent(
            reset(matrix(), as_of_utc="2026-10-02T12:00:00Z"), parent()[0]
        )
    with pytest.raises(ValueError, match="after supplement"):
        reset(
            matrix(row(state=State.EXECUTED_PASSED)),
            as_of_utc="2026-10-02T12:00:00Z",
        )


@pytest.mark.parametrize("record", [catalog(), evidence(), row(), matrix()])
def test_unknown_fields_and_wrong_schemas_refuse(record):
    values = record.to_dict()
    values["unexpected"] = "no"
    with pytest.raises(ValueError, match="schema or fields"):
        type(record).from_dict(values)
    values = record.to_dict()
    values["schema_version"] = "histdatacom.wrong.v1"
    with pytest.raises(ValueError, match="schema or fields"):
        type(record).from_dict(values)


@pytest.mark.parametrize(
    "transform",
    [
        lambda text: text + "\n",
        lambda text: " " + text,
        lambda text: text.replace(
            '"claim_kind":',
            '"claim_kind":"non_authoritative_supplement","claim_kind":',
            1,
        ),
        lambda text: text.replace(
            '"release_id":"generated-test-only-release"', '"release_id":NaN'
        ),
    ],
)
def test_json_requires_canonical_order_no_duplicates_or_nonfinite(transform):
    with pytest.raises(ValueError):
        Matrix.from_json(transform(matrix().to_json()))


@pytest.mark.parametrize(
    "text", ["x" * (MAX_WIRE_BYTES + 1), "\U0001f600", "[" * 10000]
)
def test_raw_json_bounds_before_parser(monkeypatch, text):
    if text.startswith("["):
        with pytest.raises(ValueError):
            Matrix.from_json(text)
    else:
        monkeypatch.setattr(
            module.json,
            "loads",
            lambda *a, **k: (_ for _ in ()).throw(
                AssertionError("parser reached")
            ),
        )
        with pytest.raises(ValueError, match="bounded ASCII"):
            Matrix.from_json(text)


@pytest.mark.parametrize("character", ["\x7f", "\U0001f600"])
def test_escaped_text_budget_precedes_json_allocation(monkeypatch, character):
    monkeypatch.setattr(module, "MAX_WIRE_BYTES", 100)
    monkeypatch.setattr(
        module.json,
        "dumps",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("encoder reached")
        ),
    )
    with pytest.raises(ValueError, match="escaped wire"):
        module._canonical({"text": character * 30})


def test_nested_frozen_bypass_and_subclasses_refuse_before_serializer():
    class Hostile(Evidence):
        def to_dict(self):
            raise AssertionError("hostile serializer")

    original = evidence()
    hostile = Hostile(
        **{
            field.name: getattr(original, field.name)
            for field in fields(original)
        }
    )
    with pytest.raises(TypeError, match="exact record"):
        reset(row(), evidence=hostile)
    actual = matrix()
    object.__setattr__(actual.rows[0].evidence, "references", ["mutable"])
    with pytest.raises(ValueError, match="exact tuple"):
        actual.to_json()


def test_render_preserves_missing_evidence_and_escapes_untrusted_text():
    actual = matrix(
        reset(
            row(state=State.DEFERRED_BLOCKED),
            scope="scope | <script>",
            limitations=("reason & uncertainty",),
        )
    )
    text = render_trader_maturity_markdown(actual, parent()[0])
    assert "Non-authoritative recorded claims" in text
    assert "unchanged 93-row" in text and "16 independent stages" in text
    assert "No earlier state implies a later pass" in text
    assert "No waivers are admitted" in text
    assert f"Snapshot as of: `{NOW}`" in text
    assert (
        "declared_unavailable" in text and "source reference: `absent`" in text
    )
    assert "&#124; &lt;script&gt;" in text and "&amp; uncertainty" in text
    assert "<script>" not in text
    assert "| executed_passed |" not in text
    assert sum(line.startswith("| ") for line in text.splitlines()) == 17
