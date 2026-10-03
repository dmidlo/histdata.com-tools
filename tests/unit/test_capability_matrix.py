"""Generated governance contracts, not independent scientific execution proof."""

import json
from dataclasses import FrozenInstanceError, fields, replace

import pytest

from histdatacom.synthetic import capability_matrix as matrix_module
from histdatacom.synthetic.capability_matrix import (
    CAPABILITY_CATALOG,
    CAPABILITY_CATALOG_ID,
    CAPABILITY_DEPENDENCIES,
    CAPABILITY_PARENT_IDS,
    CAPABILITY_REQUIREMENT_IDS,
    V2_5_CRITICAL_REQUIREMENT_IDS,
    CapabilityClaimKindV1 as Claim,
    CapabilityEvidenceScopeV1 as Scope,
    CapabilityMatrixPolicyV1 as Policy,
    CapabilityMatrixRequirementV1 as Requirement,
    CapabilityMatrixRowV1 as Row,
    CapabilityMatrixV1 as Matrix,
    CapabilityMatrixWaiverV1 as Waiver,
    CapabilityStateV1 as State,
    render_capability_matrix_markdown,
    required_verification_rows,
    structural_matrix_blockers,
)

RELEASE = "generated-software-test:3.0.0@" + "a" * 40
NOW = "2026-10-03T12:00:00Z"
DATASET = "generated-dataset:sha256:" + "b" * 64
EXECUTION = "generated-execution:sha256:" + "c" * 64
VERIFICATION = "generated-verification:sha256:" + "d" * 64


def policy(
    *, ids=("model_bank_registration",), scope=Scope.SOFTWARE, **changes
):
    """Freeze an explicitly narrow synthetic requirement, never a full claim."""
    values = {
        "release_id": RELEASE,
        "dataset_id": None,
        "claim_kind": Claim.NARROWER_PREDECLARED,
        "claim_label": "Generated governance contract only",
        "scope": "One declared software requirement in generated fixtures",
        "limitations": ("No empirical evidence or certification authority",),
        "requirements": tuple(Requirement(item, scope) for item in ids),
        "permitted_waivers": (),
    }
    values.update(changes)
    return Policy(**values)


def row(
    frozen,
    name="model_bank_registration",
    state=State.NOT_IMPLEMENTED,
    **changes,
):
    """Produce an explicitly synthetic row, with evidence only when declared."""
    executed = state in (
        State.EXECUTED_PASSED,
        State.EXECUTED_FAILED,
        State.EXECUTED_INSUFFICIENT_EVIDENCE,
    )
    values = {
        "requirement_id": name,
        "policy_id": frozen.policy_id,
        "implementation_commit": (
            "a" * 40
            if executed or state is State.IMPLEMENTED_UNEXECUTED
            else None
        ),
        "execution_artifact_id": EXECUTION if executed else None,
        "independent_verification_artifact_id": (
            VERIFICATION if state is State.EXECUTED_PASSED else None
        ),
        "state": state,
        "evidence_scope": Scope.SOFTWARE if executed else Scope.NONE,
        "scope": "Generated fixture; structural claims are not authority",
        "limitations": ("Synthetic contract fixture only",),
        "blocking_issues": (525,) if state is State.DEFERRED_BLOCKED else (),
        "last_verified_at_utc": NOW if state is State.EXECUTED_PASSED else None,
        "release_id": frozen.release_id,
        "dataset_id": frozen.dataset_id,
        "waiver_policy_clause": (
            "section 4" if state is State.WAIVED_WITH_LIMITATION else None
        ),
    }
    values.update(changes)
    return Row(**values)


def matrix(frozen=None, replacements=()):
    """Keep the complete catalog even for a narrower frozen claim."""
    frozen = frozen or policy()
    changed = {item.requirement_id: item for item in replacements}
    return Matrix(
        frozen.policy_id,
        frozen.release_id,
        frozen.dataset_id,
        NOW,
        tuple(
            changed.get(name) or row(frozen, name)
            for name in CAPABILITY_REQUIREMENT_IDS
        ),
    )


def reset(value, **changes):
    """A changed synthetic record must derive a fresh identity."""
    changes[value._ID_FIELD] = ""
    return replace(value, **changes)


def test_closed_issue_inventory_includes_every_explicit_family_and_subrow():
    assert len(CAPABILITY_CATALOG) == len(set(CAPABILITY_REQUIREMENT_IDS)) == 93
    assert len(CAPABILITY_PARENT_IDS) == 36
    groups = {
        parent: sum(item.parent_id == parent for item in CAPABILITY_CATALOG)
        for parent in CAPABILITY_PARENT_IDS
    }
    assert {key: value for key, value in groups.items() if value} == {
        "alignment": 2,
        "broker_ecosystem": 15,
        "overlapping_feed_identification": 4,
        "schema_compatibility": 8,
        "computational_reproducibility": 9,
        "live_broker_transfer": 2,
        "multi_timeframe_features": 9,
        "training_substrate": 8,
    }
    assert set(V2_5_CRITICAL_REQUIREMENT_IDS) == set(
        CAPABILITY_PARENT_IDS[:22]
    ) | {"alignment.exact", "alignment.bounded_prior"}
    assert "package_release_promotion" in V2_5_CRITICAL_REQUIREMENT_IDS
    assert dict(CAPABILITY_DEPENDENCIES)["full_deep_verification"] == (
        "campaign_product_index",
    )
    assert {item.value for item in State} == {
        "not_implemented",
        "implemented_unexecuted",
        "executed_failed",
        "executed_insufficient_evidence",
        "executed_passed",
        "deferred_blocked",
        "waived_with_limitation",
    }


@pytest.mark.parametrize("state", tuple(State))
def test_all_seven_states_roundtrip_without_inventing_proof(state):
    frozen = policy()
    item = row(frozen, state=state)
    actual = Row.from_json(item.to_json())
    assert actual == item
    result = matrix(frozen, (actual,))
    assert Matrix.from_dict(result.to_dict()) == result
    assert Matrix.from_json(result.to_json()) == result
    if state is State.EXECUTED_PASSED:
        assert required_verification_rows(result, frozen) == (item,)
        # Empty structural blockers means the independent verifier still MUST
        # verify this row; it is deliberately not a certification result.
        assert structural_matrix_blockers(result, frozen) == ()
    else:
        assert required_verification_rows(result, frozen) == ()
        assert structural_matrix_blockers(result, frozen)


@pytest.mark.parametrize("missing", CAPABILITY_REQUIREMENT_IDS)
def test_every_catalog_row_is_mandatory_even_for_narrow_claim(missing):
    original = matrix()
    with pytest.raises(ValueError, match="complete catalog"):
        reset(
            original,
            rows=tuple(
                item for item in original.rows if item.requirement_id != missing
            ),
        )


def test_duplicate_unknown_and_mixed_membership_refuse():
    original = matrix()
    with pytest.raises(ValueError, match="duplicates or omits"):
        reset(original, rows=original.rows[:-1] + (original.rows[0],))
    with pytest.raises(ValueError, match="unknown"):
        row(policy(), "future_unregistered")
    with pytest.raises(ValueError, match="policy/release/dataset"):
        reset(
            original,
            rows=(
                reset(original.rows[0], release_id="other-release"),
                *original.rows[1:],
            ),
        )


@pytest.mark.parametrize("field", ["row_id", "matrix_id", "policy_id"])
def test_derived_ids_refuse_stale_or_forged_identity(field):
    value = {
        "row_id": row(policy()),
        "matrix_id": matrix(),
        "policy_id": policy(),
    }[field]
    wire = value.to_dict()
    wire[field] = "forged:sha256:" + "0" * 64
    with pytest.raises(ValueError, match="differs"):
        type(value).from_dict(wire)


def test_record_order_is_not_identity_and_nested_returns_are_detached():
    original = matrix()
    assert reset(original, rows=tuple(reversed(original.rows))) == original
    payload = original.to_dict()
    payload["rows"][0]["limitations"].append("mutation")
    assert "mutation" not in original.to_json()
    with pytest.raises(FrozenInstanceError):
        original.as_of_utc = "2025-01-01T00:00:00Z"


def test_catalog_semantics_are_bound_to_policy_and_matrix_identity():
    frozen = policy()
    original = matrix(frozen)
    assert frozen.catalog_id == original.catalog_id == CAPABILITY_CATALOG_ID
    for value in (frozen, original):
        wire = value.to_dict()
        assert wire["catalog_id"] == CAPABILITY_CATALOG_ID
        wire["catalog_id"] = "capability-catalog:sha256:" + "0" * 64
        with pytest.raises(ValueError, match="catalog identity"):
            type(value).from_dict(wire)


def test_canonical_reader_rejects_non_normalized_row_order():
    wire = matrix().to_dict()
    wire["rows"].reverse()
    # Direct dictionaries/constructors can normalize record order, but the
    # canonical wire reader must roundtrip the exact canonical published bytes.
    assert Matrix.from_dict(wire).to_dict() != wire
    with pytest.raises(ValueError, match="normalized canonical order"):
        Matrix.from_json(
            json.dumps(wire, sort_keys=True, separators=(",", ":"))
        )


@pytest.mark.parametrize(
    "raw",
    [
        '{"x":1,"x":2}',
        '{"x":NaN}',
        '{"x":1.5}',
        '{"x":-1}',
        '{"x":2147483648}',
        "[]",
    ],
)
def test_invalid_json_primitives_and_duplicate_keys_refuse(raw):
    with pytest.raises(ValueError):
        Matrix.from_json(raw)


@pytest.mark.parametrize(
    "alter",
    [
        lambda text: text + "\n",
        lambda text: " " + text,
        lambda text: text.replace(":", ": ", 1),
    ],
)
def test_noncanonical_whitespace_is_not_silently_normalized(alter):
    with pytest.raises(ValueError, match="canonical"):
        Matrix.from_json(alter(matrix().to_json()))


def test_unknown_missing_and_foreign_schema_fields_refuse():
    original = row(policy())
    for change in ("extra", "missing", "schema"):
        wire = original.to_dict()
        if change == "extra":
            wire["verified"] = True
        elif change == "missing":
            del wire["scope"]
        else:
            wire["schema_version"] = "histdatacom.capability-matrix-row.v2"
        with pytest.raises(ValueError, match="field inventory"):
            Row.from_dict(wire)


@pytest.mark.parametrize(
    "field,value",
    [
        ("implementation_commit", "abcdef1"),
        ("implementation_commit", True),
        ("execution_artifact_id", "https://github.com/example/issues/1"),
        ("independent_verification_artifact_id", EXECUTION),
        ("independent_verification_artifact_id", None),
        ("last_verified_at_utc", None),
        ("blocking_issues", (525,)),
        ("blocking_issues", (True,)),
        ("blocking_issues", [525]),
        ("evidence_scope", Scope.NONE),
        ("state", "executed_passed"),
        ("dataset_id", "not-content-addressed"),
    ],
)
def test_a_pass_claim_requires_exact_fields_but_is_still_not_proof(
    field, value
):
    with pytest.raises((ValueError, TypeError)):
        row(policy(), **{"state": State.EXECUTED_PASSED, field: value})


@pytest.mark.parametrize(
    "state", [State.NOT_IMPLEMENTED, State.IMPLEMENTED_UNEXECUTED]
)
def test_unexecuted_state_cannot_claim_an_execution(state):
    with pytest.raises(ValueError, match="unexecuted"):
        row(policy(), state=state, execution_artifact_id=EXECUTION)


def test_deferred_partial_evidence_is_retained_but_still_blocks():
    frozen = policy()
    partial = row(
        frozen,
        state=State.DEFERRED_BLOCKED,
        implementation_commit="a" * 40,
        execution_artifact_id=EXECUTION,
        evidence_scope=Scope.SOFTWARE,
    )
    result = matrix(frozen, (partial,))
    assert (
        structural_matrix_blockers(result, frozen)[0].reason
        == "critical_deferred_blocked"
    )
    assert required_verification_rows(result, frozen) == ()


@pytest.mark.parametrize(
    "time",
    [
        "2026-1-03T12:00:00Z",
        "2026-10-03T12:00:00+00:00",
        "2026-10-03T12:00:00.000Z",
        "2026-02-30T12:00:00Z",
    ],
)
def test_timestamp_has_exact_utc_second_semantics(time):
    with pytest.raises(ValueError):
        row(policy(), last_verified_at_utc=time)


def test_future_verification_and_scope_rebinding_refuse():
    frozen = policy()
    future = row(
        frozen,
        state=State.EXECUTED_PASSED,
        last_verified_at_utc="2026-10-04T12:00:00Z",
    )
    with pytest.raises(ValueError, match="after"):
        matrix(frozen, (future,))
    original = matrix(frozen)
    with pytest.raises(ValueError, match="frozen policy"):
        structural_matrix_blockers(
            original, reset(frozen, scope="different frozen scope")
        )


def test_full_campaign_cannot_omit_publication_or_alignment_or_dataset():
    with pytest.raises(ValueError, match="full v2.5"):
        policy(claim_kind=Claim.FULL_V2_5_CAMPAIGN)
    ids = tuple(
        name
        for name in V2_5_CRITICAL_REQUIREMENT_IDS
        if name != "package_release_promotion"
    )
    with pytest.raises(ValueError, match="full v2.5"):
        policy(ids=ids, claim_kind=Claim.FULL_V2_5_CAMPAIGN)
    frozen = policy(
        ids=V2_5_CRITICAL_REQUIREMENT_IDS,
        claim_kind=Claim.FULL_V2_5_CAMPAIGN,
        scope=Scope.COMPLETE,
    )
    result = matrix(frozen)
    assert (
        structural_matrix_blockers(result, frozen)[0].reason
        == "full_claim_requires_materialized_dataset"
    )
    actual = reset(frozen, dataset_id=DATASET)
    assert not any(
        item.requirement_id == "__matrix__"
        for item in structural_matrix_blockers(matrix(actual), actual)
    )


def test_narrower_claim_requires_visible_limitation_and_no_implicit_waiver():
    with pytest.raises(ValueError, match="explicit limitations"):
        policy(limitations=())
    frozen = policy()
    waived = row(
        frozen,
        state=State.WAIVED_WITH_LIMITATION,
        evidence_scope=Scope.SOFTWARE,
    )
    assert (
        structural_matrix_blockers(matrix(frozen, (waived,)), frozen)[0].reason
        == "waiver_not_exactly_permitted"
    )


@pytest.mark.parametrize("scope", [Scope.SOFTWARE, Scope.BOUNDED])
def test_full_claim_cannot_weaken_critical_scopes_to_software(scope):
    with pytest.raises(ValueError, match="COMPLETE"):
        policy(
            ids=V2_5_CRITICAL_REQUIREMENT_IDS,
            claim_kind=Claim.FULL_V2_5_CAMPAIGN,
            scope=scope,
        )


def test_waiver_exact_clause_scope_and_limitation_are_frozen():
    limitation = (
        "This generated software claim excludes empirical certification"
    )
    waiver = Waiver("model_bank_registration", "policy section 4", limitation)
    frozen = policy(permitted_waivers=(waiver,))
    item = row(
        frozen,
        state=State.WAIVED_WITH_LIMITATION,
        evidence_scope=Scope.SOFTWARE,
        waiver_policy_clause=waiver.policy_clause,
        limitations=(limitation,),
    )
    assert structural_matrix_blockers(matrix(frozen, (item,)), frozen) == ()
    assert required_verification_rows(matrix(frozen, (item,)), frozen) == ()
    for changed in (
        reset(item, waiver_policy_clause="section 5"),
        reset(item, limitations=("different",)),
    ):
        assert (
            structural_matrix_blockers(matrix(frozen, (changed,)), frozen)[
                0
            ].reason
            == "waiver_not_exactly_permitted"
        )
    with pytest.raises(ValueError, match="outside"):
        policy(
            permitted_waivers=(
                Waiver("source_experiment_identity", "clause", "limited"),
            )
        )


@pytest.mark.parametrize("scope", [Scope.BOUNDED, Scope.COMPLETE])
def test_software_or_larger_named_scope_is_not_a_universal_superscope(scope):
    frozen = policy(scope=scope)
    declared = row(frozen, state=State.EXECUTED_PASSED)
    assert (
        structural_matrix_blockers(matrix(frozen, (declared,)), frozen)[
            0
        ].reason
        == "required_evidence_scope_differs"
    )


def test_parent_and_dependency_closure_prevents_omitted_nonpassing_children():
    with pytest.raises(ValueError, match="children or dependencies"):
        policy(ids=("alignment",))
    with pytest.raises(ValueError, match="children or dependencies"):
        policy(ids=("product_engine_selection",))
    frozen = policy()
    with pytest.raises(ValueError, match="nonpassing prerequisites"):
        matrix(
            frozen, (row(frozen, "broker_ecosystem", State.EXECUTED_PASSED),)
        )
    with pytest.raises(ValueError, match="nonpassing prerequisites"):
        matrix(
            frozen,
            (row(frozen, "product_engine_selection", State.EXECUTED_PASSED),),
        )


def test_whole_training_claim_requires_all_multi_timeframe_evidence():
    training = tuple(
        name
        for name in CAPABILITY_REQUIREMENT_IDS
        if name == "training_substrate"
        or name.startswith("training_substrate.")
    )
    features = tuple(
        name
        for name in CAPABILITY_REQUIREMENT_IDS
        if name == "multi_timeframe_features"
        or name.startswith("multi_timeframe_features.")
    )
    assert len(training) == 9 and len(features) == 10
    assert dict(CAPABILITY_DEPENDENCIES)["training_substrate"] == (
        "multi_timeframe_features",
    )
    with pytest.raises(ValueError, match="children or dependencies"):
        policy(ids=training, scope=Scope.COMPLETE)
    frozen = policy(ids=training + features, scope=Scope.COMPLETE)
    passed_training = tuple(
        row(frozen, name, State.EXECUTED_PASSED, evidence_scope=Scope.COMPLETE)
        for name in training
    )
    with pytest.raises(ValueError, match="nonpassing prerequisites"):
        matrix(frozen, passed_training)
    passed_features = tuple(
        row(frozen, name, State.EXECUTED_PASSED, evidence_scope=Scope.COMPLETE)
        for name in features
    )
    declared = matrix(frozen, passed_training + passed_features)
    assert structural_matrix_blockers(declared, frozen) == ()
    # Structural closure still supplies no actual independent native proof.
    assert len(required_verification_rows(declared, frozen)) == 19


def test_narrow_training_child_does_not_claim_whole_substrate():
    name = "training_substrate.lineage"
    frozen = policy(ids=(name,), scope=Scope.BOUNDED)
    declared = matrix(
        frozen,
        (
            row(
                frozen,
                name,
                State.EXECUTED_PASSED,
                evidence_scope=Scope.BOUNDED,
            ),
        ),
    )
    assert structural_matrix_blockers(declared, frozen) == ()
    assert tuple(
        x.requirement_id for x in required_verification_rows(declared, frozen)
    ) == (name,)
    assert (
        next(
            x for x in declared.rows if x.requirement_id == "training_substrate"
        ).state
        is State.NOT_IMPLEMENTED
    )


def test_raw_input_and_escaped_text_bounds_precede_parse_and_encode(
    monkeypatch,
):
    def poisoned(*args, **kwargs):
        raise AssertionError("oversized input must be rejected before codec")

    monkeypatch.setattr(matrix_module.json, "loads", poisoned)
    with pytest.raises(ValueError, match="bounded canonical"):
        Matrix.from_json(" " * (matrix_module.MAX_MATRIX_BYTES + 1))
    with pytest.raises(ValueError, match="bounded canonical"):
        Matrix.from_json("é")
    monkeypatch.setattr(matrix_module.json, "dumps", poisoned)
    with pytest.raises(ValueError, match="escaped"):
        matrix_module._canonical({"payload": "\U00010000" * 200000})


def test_exact_nested_admission_precedes_hostile_serializer():
    class HostileRow(Row):
        def to_dict(self):
            raise AssertionError("untrusted subclass serializer")

    frozen = policy()
    original = row(frozen)
    hostile = HostileRow(
        **{
            field.name: getattr(original, field.name)
            for field in fields(original)
        }
    )
    original_matrix = matrix(frozen)
    with pytest.raises(TypeError, match="exact declared"):
        reset(original_matrix, rows=(hostile, *original_matrix.rows[1:]))
    object.__setattr__(original_matrix, "rows", list(original_matrix.rows))
    with pytest.raises(ValueError, match="complete catalog"):
        original_matrix.to_json()


def test_bypass_mutated_frozen_nested_text_rejected_before_encoder(monkeypatch):
    original = matrix()
    object.__setattr__(
        original.rows[0], "scope", "x" * (matrix_module.MAX_TEXT_LENGTH + 1)
    )

    def poisoned(*args, **kwargs):
        raise AssertionError("mutated text must not reach encoder")

    monkeypatch.setattr(matrix_module.json, "dumps", poisoned)
    with pytest.raises(ValueError, match="bounded"):
        original.to_json()


def test_markdown_contains_every_evidence_group_and_escapes_untrusted_text():
    frozen = policy()
    item = row(
        frozen,
        scope="<script>|`not passed`",
        limitations=("No real dataset exists",),
    )
    original = matrix(frozen, (item,))
    rendered = render_capability_matrix_markdown(original)
    assert rendered == render_capability_matrix_markdown(
        Matrix.from_json(original.to_json())
    )
    assert "dataset: absent" in rendered
    assert "<script>" not in rendered
    assert "&lt;script&gt;&#124;&#96;not passed&#96;" in rendered
    assert "independent scientific verification" in rendered
    assert "unsupported verification blocks" in rendered
    for item in original.rows:
        assert "| " + item.requirement_id + " |" in rendered
    assert (
        len([line for line in rendered.splitlines() if line.startswith("| ")])
        == 94
    )
    for field in (
        "Implementation commit",
        "Execution artifact",
        "Independent verification artifact",
        "Blocking issues",
        "Last verified UTC",
        "Policy / release / dataset",
    ):
        assert field in rendered


def test_no_boolean_verified_or_proof_field_can_be_injected():
    wire = matrix().to_dict()
    wire["verified"] = True
    with pytest.raises(ValueError, match="inventory"):
        Matrix.from_json(
            json.dumps(wire, sort_keys=True, separators=(",", ":"))
        )
