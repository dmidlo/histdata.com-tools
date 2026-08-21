"""Installed command-line surface for typed reconstruction operations."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from histdatacom.cli_config import (
    CliConfigError,
    add_config_argument,
    configured_reconstruction_argv,
)
from histdatacom.orchestration.supervisor import OrchestrationSupervisor
from histdatacom.reconstruction import (
    MAX_PREVIEW_LIMIT,
    ReconstructionClient,
    ReconstructionExitCode,
    ReconstructionPublicError,
    read_execution_request,
    read_operation_receipt,
    read_plan_spec,
    read_reconstruction_plan_set_execution_request,
    read_reconstruction_plan_set_receipt_index,
    reconstruction_exit_code,
    write_execution_request,
    write_operation_receipt,
    write_reconstruction_plan_set_execution_request,
)
from histdatacom.reconstruction_schema import ReconstructionCompatibilityStatus
from histdatacom.synthetic.certification import CertificationState
from histdatacom.synthetic.information import InformationMode


def build_parser() -> argparse.ArgumentParser:
    """Build the reconstruction command family parser."""
    parser = argparse.ArgumentParser(
        prog="histdatacom reconstruction",
        description=(
            "Plan, preflight, execute, control, and inspect first-party "
            "ASCII tick reconstruction."
        ),
        epilog=(
            "Every execution request must explicitly select an information "
            "mode and acknowledge that output is not recovered historical "
            "truth. M1/bar inputs and partial symbol groups are unsupported."
        ),
    )
    add_config_argument(parser)
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit machine-readable JSON instead of a compact summary",
    )
    parser.add_argument(
        "--start-runtime",
        action="store_true",
        help="start the local Temporal runtime before submit, cancel, or resume",
    )
    subparsers = parser.add_subparsers(
        dest="reconstruction_command", required=True
    )

    schemas = subparsers.add_parser(
        "schemas",
        help="discover installed reconstruction and evidence contracts",
    )
    schemas.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS
    )

    science = subparsers.add_parser(
        "science",
        help="inspect the current or one retained reconstruction science ledger",
    )
    science.add_argument("--ledger", metavar="PATH")
    science.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS
    )

    engines = subparsers.add_parser(
        "engines",
        help="discover concrete proposal engines and their executable scope",
    )
    engines.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS
    )

    portfolio = subparsers.add_parser(
        "portfolio",
        help="inspect qualified selections and refusals bound to one plan",
    )
    portfolio.add_argument("--plan", required=True, metavar="PATH")

    engine_evaluate = subparsers.add_parser(
        "engine-evaluate",
        help="run every HistData benchmark-eligible proposal engine",
    )
    engine_evaluate.add_argument(
        "--benchmark-manifest", required=True, metavar="PATH"
    )
    engine_evaluate.add_argument("--source-root", required=True, metavar="PATH")
    engine_evaluate.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )
    engine_evaluate.add_argument(
        "--engine",
        action="append",
        default=None,
        metavar="ENGINE_ID",
        help="evaluate one engine (repeat for an explicit portfolio)",
    )

    qualify = subparsers.add_parser(
        "qualify",
        help="power-qualify one exact HistData experiment and engine evaluation",
    )
    qualify.add_argument("--evaluation", required=True, metavar="PATH")
    qualify.add_argument("--experiment", required=True, metavar="PATH")
    qualify.add_argument("--output-directory", required=True, metavar="PATH")

    hawkes_select = subparsers.add_parser(
        "hawkes-select",
        help="freeze a validation-only diagonal-versus-full Hawkes choice",
    )
    hawkes_select.add_argument("--policy", required=True, metavar="PATH")
    hawkes_select.add_argument("--comparison", required=True, metavar="PATH")
    hawkes_select.add_argument("--qualification", required=True, metavar="PATH")
    hawkes_select.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )

    observation_uncertainty = subparsers.add_parser(
        "observation-uncertainty-policy",
        help="freeze the v2.5 three-scenario observation uncertainty policy",
    )
    observation_uncertainty.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )

    transition_policy = subparsers.add_parser(
        "feed-epoch-transition-policy",
        help="freeze the v2.5 three-scenario feed-epoch transition policy",
    )
    transition_policy.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )

    diagnostic_build = subparsers.add_parser(
        "diagnostic-build",
        help="build verified chart data and optional deterministic static figures",
    )
    diagnostic_build.add_argument("--spec", required=True, metavar="PATH")
    diagnostic_build.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )

    diagnostic_list = subparsers.add_parser(
        "diagnostic-list",
        help="verify and list one reconstruction diagnostic publication",
    )
    diagnostic_list.add_argument("--manifest", required=True, metavar="PATH")

    compatibility = subparsers.add_parser(
        "compatibility",
        help="audit a proposed plan against installed executable contracts",
    )
    compatibility.add_argument("--plan", required=True, metavar="PATH")
    compatibility.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS
    )

    experiment_list = subparsers.add_parser(
        "experiment-list",
        help="discover publication-safe frozen HistData experiments",
    )
    experiment_list.add_argument("--root", required=True, metavar="PATH")

    experiment_inspect = subparsers.add_parser(
        "experiment-inspect",
        help="inspect one frozen experiment without exposing local paths",
    )
    experiment_inspect.add_argument("--manifest", required=True, metavar="PATH")

    experiment_verify = subparsers.add_parser(
        "experiment-verify",
        help="verify one experiment's catalogs, partitions, artifacts, and code",
    )
    experiment_verify.add_argument("--manifest", required=True, metavar="PATH")

    plan = subparsers.add_parser(
        "plan", help="construct and validate a plan from a JSON plan spec"
    )
    plan.add_argument("--spec", required=True, metavar="PATH")

    plan_set = subparsers.add_parser(
        "plan-set",
        help="construct a full range as bounded contiguous plan shards",
    )
    plan_set.add_argument("--spec", required=True, metavar="PATH")
    plan_set.add_argument(
        "--periods-per-shard", type=int, default=12, metavar="COUNT"
    )

    preflight_set = subparsers.add_parser(
        "preflight-set", help="verify every shard in a full-range plan set"
    )
    preflight_set.add_argument("--plan-set", required=True, metavar="PATH")

    support_map = subparsers.add_parser(
        "support-map",
        help="materialize exact executable/refused coverage for a plan set",
    )
    support_map.add_argument("--plan-set", required=True, metavar="PATH")
    support_map.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )

    support_verify = subparsers.add_parser(
        "support-verify",
        help="independently replay and candidate-bind final adaptive support",
    )
    support_verify.add_argument("--plan-set", required=True, metavar="PATH")
    support_verify.add_argument("--support-map", required=True, metavar="PATH")
    support_verify.add_argument(
        "--release-candidate", required=True, metavar="PATH"
    )
    support_verify.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )

    support_inspect = subparsers.add_parser(
        "support-inspect",
        help="inspect a bounded slice of monolithic or indexed support",
    )
    support_inspect.add_argument("--support-map", required=True, metavar="PATH")
    support_inspect.add_argument("--start-ns", type=int)
    support_inspect.add_argument("--end-ns", type=int)
    support_inspect.add_argument("--limit", type=int, default=100)

    product_index = subparsers.add_parser(
        "product-index",
        help="reconcile every support outcome with committed member products",
    )
    product_index.add_argument("--plan-set", required=True, metavar="PATH")
    product_index.add_argument("--support-map", required=True, metavar="PATH")
    product_index.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )
    product_index.add_argument(
        "--manifest-only",
        action="store_true",
        help="skip full Parquet replay while building an in-progress index",
    )

    product_inspect = subparsers.add_parser(
        "product-inspect",
        help="inspect bounded products/outcomes from a campaign",
    )
    product_inspect.add_argument(
        "--product-index", required=True, metavar="PATH"
    )
    product_inspect.add_argument("--start-ns", type=int)
    product_inspect.add_argument("--end-ns", type=int)
    product_inspect.add_argument("--limit", type=int, default=100)

    dataset_publish = subparsers.add_parser(
        "dataset-publish",
        help="publish a complete campaign as a provider-neutral dataset version",
    )
    dataset_publish.add_argument(
        "--product-index", required=True, metavar="PATH"
    )
    dataset_publish.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )
    dataset_publish.add_argument(
        "--dataset-id",
        default="histdata-triangle-modern-reference-synthetic",
    )

    request = subparsers.add_parser(
        "request", help="bind explicit operator intent to an immutable plan"
    )
    request.add_argument("--plan", required=True, metavar="PATH")
    request.add_argument(
        "--information-mode",
        required=True,
        choices=tuple(mode.value for mode in InformationMode),
    )
    request.add_argument(
        "--acknowledge-scientific-nonclaim",
        action="store_true",
        help="acknowledge that output is plausible, not recovered truth",
    )
    request.add_argument(
        "--allow-refusals",
        action="store_true",
        help="execute supported windows while retaining explicit refusals",
    )
    request.add_argument("--output", required=True, metavar="PATH")

    request_set = subparsers.add_parser(
        "request-set",
        help="bind operator intent to every plan shard and its support map",
    )
    request_set.add_argument("--plan-set", required=True, metavar="PATH")
    request_set.add_argument("--support-map", required=True, metavar="PATH")
    request_set.add_argument(
        "--information-mode",
        required=True,
        choices=tuple(mode.value for mode in InformationMode),
    )
    request_set.add_argument(
        "--acknowledge-scientific-nonclaim", action="store_true"
    )
    request_set.add_argument(
        "--disallow-refusals",
        action="store_true",
        help="refuse the campaign if any shard contains an explicit refusal",
    )
    request_set.add_argument(
        "--output-directory", required=True, metavar="PATH"
    )

    preflight = subparsers.add_parser(
        "preflight", help="validate readiness and print dry-run resources"
    )
    preflight.add_argument("--request", required=True, metavar="PATH")

    run = subparsers.add_parser(
        "run",
        help="execute and wait, submit only, or run a bounded local smoke",
    )
    run.add_argument("--request", required=True, metavar="PATH")
    mode = run.add_mutually_exclusive_group()
    mode.add_argument(
        "--submit-only",
        action="store_true",
        help="return after Temporal submit",
    )
    mode.add_argument(
        "--local",
        action="store_true",
        help="run the registered pipeline in-process for smoke/recovery",
    )
    run.add_argument(
        "--window-id",
        default="",
        help="limit explicit local execution to one plan window",
    )
    run.add_argument("--receipt", required=True, metavar="PATH")

    run_set = subparsers.add_parser(
        "run-set",
        help="execute or submit every shard in a durable campaign request",
    )
    run_set.add_argument("--request-set", required=True, metavar="PATH")
    run_set_mode = run_set.add_mutually_exclusive_group()
    run_set_mode.add_argument("--submit-only", action="store_true")
    run_set_mode.add_argument("--local", action="store_true")
    run_set.add_argument("--output-directory", required=True, metavar="PATH")

    status = subparsers.add_parser(
        "status", help="inspect every job in an operation receipt"
    )
    status.add_argument("--receipt", required=True, metavar="PATH")
    status.add_argument(
        "--offline", action="store_true", help="read durable status only"
    )
    status.add_argument("--output", default="", metavar="PATH")

    status_set = subparsers.add_parser(
        "status-set", help="inspect every shard from a campaign receipt index"
    )
    status_set.add_argument("--receipt-index", required=True, metavar="PATH")
    status_set.add_argument("--offline", action="store_true")
    status_set.add_argument("--output-directory", required=True, metavar="PATH")

    cancel = subparsers.add_parser(
        "cancel", help="request cancellation for every submitted job"
    )
    cancel.add_argument("--receipt", required=True, metavar="PATH")
    cancel.add_argument("--reason", default="")
    cancel.add_argument("--output", required=True, metavar="PATH")

    cancel_set = subparsers.add_parser(
        "cancel-set", help="cancel every shard from a campaign receipt index"
    )
    cancel_set.add_argument("--receipt-index", required=True, metavar="PATH")
    cancel_set.add_argument("--reason", default="")
    cancel_set.add_argument("--output-directory", required=True, metavar="PATH")

    resume = subparsers.add_parser(
        "resume", help="resume durable checkpoints using fresh workflow IDs"
    )
    resume.add_argument("--receipt", required=True, metavar="PATH")
    resume_mode = resume.add_mutually_exclusive_group()
    resume_mode.add_argument("--submit-only", action="store_true")
    resume_mode.add_argument("--local", action="store_true")
    resume.add_argument("--output", required=True, metavar="PATH")

    resume_set = subparsers.add_parser(
        "resume-set",
        help="resume every shard from durable campaign checkpoints",
    )
    resume_set.add_argument("--receipt-index", required=True, metavar="PATH")
    resume_set_mode = resume_set.add_mutually_exclusive_group()
    resume_set_mode.add_argument("--submit-only", action="store_true")
    resume_set_mode.add_argument("--local", action="store_true")
    resume_set.add_argument("--output-directory", required=True, metavar="PATH")

    outputs = subparsers.add_parser(
        "outputs", help="list verified committed products for a request"
    )
    outputs.add_argument("--request", required=True, metavar="PATH")

    preview = subparsers.add_parser(
        "preview", help="show bounded event origin, lineage, and decisions"
    )
    preview.add_argument("--manifest", required=True, metavar="PATH")
    preview.add_argument(
        "--limit", type=int, default=20, choices=range(1, MAX_PREVIEW_LIMIT + 1)
    )

    replay = subparsers.add_parser(
        "replay", help="integrity-replay a committed reconstruction product"
    )
    replay.add_argument("--manifest", required=True, metavar="PATH")

    certify = subparsers.add_parser(
        "certify",
        help="evaluate a hash-verified modern-reference evidence campaign",
    )
    certify.add_argument("--spec", required=True, metavar="PATH")
    certify.add_argument("--output-directory", required=True, metavar="PATH")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the installed reconstruction command family."""
    parser = build_parser()
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(configured_reconstruction_argv(raw_argv))
        client = _client(args)
        result, code = _run_command(client, args)
        _write_result(result, as_json=bool(args.json))
        return int(code)
    except CliConfigError as err:
        return _write_error(
            err,
            reason_code="reconstruction_config_error",
            exit_code=ReconstructionExitCode.INVALID_PLAN,
            as_json="--json" in raw_argv,
        )
    except ReconstructionPublicError as err:
        return _write_error(
            err,
            reason_code=err.reason_code,
            exit_code=err.exit_code,
            as_json="--json" in raw_argv,
        )
    except (OSError, TypeError, ValueError) as err:
        return _write_error(
            err,
            reason_code="invalid_reconstruction_plan",
            exit_code=ReconstructionExitCode.INVALID_PLAN,
            as_json="--json" in raw_argv,
        )
    except Exception as err:  # noqa: BLE001  # pragma: no cover - CLI boundary
        return _write_error(
            err,
            reason_code="reconstruction_runtime_failure",
            exit_code=ReconstructionExitCode.RUNTIME_FAILURE,
            as_json="--json" in raw_argv,
        )


def _client(args: argparse.Namespace) -> ReconstructionClient:
    supervisor = None
    if args.start_runtime:
        supervisor = OrchestrationSupervisor()
        supervisor.start()
    return ReconstructionClient(supervisor=supervisor)


def _run_command(
    client: ReconstructionClient, args: argparse.Namespace
) -> tuple[Mapping[str, Any], ReconstructionExitCode]:
    command = args.reconstruction_command
    if command == "schemas":
        return client.schemas().to_dict(), ReconstructionExitCode.SUCCESS
    if command == "science":
        return (
            client.scientific_ledger(args.ledger).to_dict(),
            ReconstructionExitCode.SUCCESS,
        )
    if command == "engines":
        return (
            client.proposal_engines().to_dict(),
            ReconstructionExitCode.SUCCESS,
        )
    if command == "portfolio":
        return (
            client.proposal_portfolio(args.plan).to_dict(),
            ReconstructionExitCode.SUCCESS,
        )
    if command == "engine-evaluate":
        evaluation = client.evaluate_proposal_portfolio(
            args.benchmark_manifest,
            args.source_root,
            output_directory=args.output_directory,
            engine_ids=args.engine,
        )
        return evaluation.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "qualify":
        qualification = client.qualify_proposal_portfolio(
            args.evaluation,
            args.experiment,
            output_directory=args.output_directory,
        )
        return qualification.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "hawkes-select":
        selection = client.select_hawkes_product(
            args.policy,
            args.comparison,
            args.qualification,
            output_directory=args.output_directory,
        )
        return selection.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "observation-uncertainty-policy":
        observation_policy = client.create_observation_uncertainty_policy(
            output_directory=args.output_directory
        )
        return observation_policy.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "feed-epoch-transition-policy":
        transition_policy = client.create_feed_epoch_transition_policy(
            output_directory=args.output_directory
        )
        return transition_policy.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "diagnostic-build":
        publication = client.publish_diagnostics(
            args.spec, output_directory=args.output_directory
        )
        return publication.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "diagnostic-list":
        return client.diagnostics(args.manifest), ReconstructionExitCode.SUCCESS
    if command == "compatibility":
        report = client.compatibility(args.plan)
        if report.executable:
            code = ReconstructionExitCode.SUCCESS
        elif report.status is ReconstructionCompatibilityStatus.RESEARCH_ONLY:
            code = ReconstructionExitCode.REFUSED
        else:
            code = ReconstructionExitCode.INVALID_PLAN
        return report.to_dict(), code
    if command == "experiment-list":
        return (
            {"experiments": list(client.experiments(args.root))},
            ReconstructionExitCode.SUCCESS,
        )
    if command == "experiment-inspect":
        return (
            client.inspect_experiment(args.manifest).publication_summary(),
            ReconstructionExitCode.SUCCESS,
        )
    if command == "experiment-verify":
        verification = client.verify_experiment(args.manifest)
        return (
            verification.to_dict(),
            (
                ReconstructionExitCode.SUCCESS
                if verification.verified
                else ReconstructionExitCode.VALIDATION_FAILURE
            ),
        )
    if command == "plan":
        ref = client.construct_plan(read_plan_spec(args.spec))
        return ref.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "plan-set":
        ref = client.construct_plan_set(
            read_plan_spec(args.spec),
            periods_per_shard=args.periods_per_shard,
        )
        return ref.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "preflight-set":
        set_preflight = client.preflight_plan_set(args.plan_set)
        return (
            set_preflight.to_dict(),
            (
                ReconstructionExitCode.SUCCESS
                if set_preflight.executable
                else ReconstructionExitCode.REFUSED
            ),
        )
    if command == "support-map":
        ref = client.construct_plan_support_map(
            args.plan_set,
            output_directory=args.output_directory,
        )
        return ref.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "support-verify":
        ref = client.construct_final_adaptive_support_map(
            args.plan_set,
            args.support_map,
            args.release_candidate,
            output_directory=args.output_directory,
        )
        return ref.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "support-inspect":
        return (
            client.inspect_plan_support_map(
                args.support_map,
                start_ns=args.start_ns,
                end_ns=args.end_ns,
                limit=args.limit,
            ),
            ReconstructionExitCode.SUCCESS,
        )
    if command == "product-index":
        ref = client.construct_campaign_product_index(
            args.plan_set,
            args.support_map,
            output_directory=args.output_directory,
            verify_products=not args.manifest_only,
        )
        return ref.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "product-inspect":
        return (
            client.inspect_campaign_products(
                args.product_index,
                start_ns=args.start_ns,
                end_ns=args.end_ns,
                limit=args.limit,
            ),
            ReconstructionExitCode.SUCCESS,
        )
    if command == "dataset-publish":
        ref = client.publish_campaign_dataset(
            args.product_index,
            output_directory=args.output_directory,
            dataset_id=args.dataset_id,
        )
        return ref.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "request":
        request = client.create_request(
            args.plan,
            information_mode=args.information_mode,
            acknowledge_scientific_nonclaim=(
                args.acknowledge_scientific_nonclaim
            ),
            allow_refusals=args.allow_refusals,
        )
        path = write_execution_request(request, args.output)
        return {
            "request": request.to_dict(),
            "request_path": str(path),
        }, ReconstructionExitCode.SUCCESS
    if command == "request-set":
        request_set = client.create_plan_set_execution_request(
            args.plan_set,
            args.support_map,
            information_mode=args.information_mode,
            acknowledge_scientific_nonclaim=(
                args.acknowledge_scientific_nonclaim
            ),
            allow_refusals=not args.disallow_refusals,
        )
        ref = write_reconstruction_plan_set_execution_request(
            request_set, args.output_directory
        )
        return ref.to_dict(), ReconstructionExitCode.SUCCESS
    if command == "preflight":
        preflight = client.preflight(read_execution_request(args.request))
        return preflight.to_dict(), reconstruction_exit_code(preflight)
    if command == "run":
        request = read_execution_request(args.request)
        receipt = (
            client.execute_local(request, window_id=args.window_id)
            if args.local
            else client.submit(request, wait=not args.submit_only)
        )
        path = write_operation_receipt(receipt, args.receipt)
        return _receipt_result(receipt, path), reconstruction_exit_code(receipt)
    if command == "run-set":
        request_set = read_reconstruction_plan_set_execution_request(
            args.request_set
        )
        ref = client.run_plan_set_execution_request(
            request_set,
            output_directory=args.output_directory,
            wait=not args.submit_only,
            local=args.local,
        )
        index = read_reconstruction_plan_set_receipt_index(ref.path)
        return ref.to_dict(), reconstruction_exit_code(index)
    if command == "status":
        receipt = client.inspect(
            read_operation_receipt(args.receipt), offline=args.offline
        )
        status_path = (
            write_operation_receipt(receipt, args.output)
            if args.output
            else None
        )
        return _receipt_result(receipt, status_path), reconstruction_exit_code(
            receipt
        )
    if command == "status-set":
        ref = client.operate_plan_set_receipt_index(
            args.receipt_index,
            operation="status",
            output_directory=args.output_directory,
            offline=args.offline,
        )
        index = read_reconstruction_plan_set_receipt_index(ref.path)
        return ref.to_dict(), reconstruction_exit_code(index)
    if command == "cancel":
        receipt = client.cancel(
            read_operation_receipt(args.receipt), reason=args.reason
        )
        path = write_operation_receipt(receipt, args.output)
        return _receipt_result(receipt, path), reconstruction_exit_code(receipt)
    if command == "cancel-set":
        ref = client.operate_plan_set_receipt_index(
            args.receipt_index,
            operation="cancel",
            output_directory=args.output_directory,
            reason=args.reason,
        )
        index = read_reconstruction_plan_set_receipt_index(ref.path)
        return ref.to_dict(), reconstruction_exit_code(index)
    if command == "resume":
        receipt = client.resume(
            read_operation_receipt(args.receipt),
            wait=not args.submit_only,
            local=args.local,
        )
        path = write_operation_receipt(receipt, args.output)
        return _receipt_result(receipt, path), reconstruction_exit_code(receipt)
    if command == "resume-set":
        ref = client.operate_plan_set_receipt_index(
            args.receipt_index,
            operation="resume",
            output_directory=args.output_directory,
            wait=not args.submit_only,
            local=args.local,
        )
        index = read_reconstruction_plan_set_receipt_index(ref.path)
        return ref.to_dict(), reconstruction_exit_code(index)
    if command == "outputs":
        return (
            client.outputs(read_execution_request(args.request)),
            ReconstructionExitCode.SUCCESS,
        )
    if command == "preview":
        return (
            client.preview(args.manifest, limit=args.limit),
            ReconstructionExitCode.SUCCESS,
        )
    if command == "replay":
        return client.replay(args.manifest), ReconstructionExitCode.SUCCESS
    if command == "certify":
        certification_dossier, result = client.certify(
            args.spec, output_directory=args.output_directory
        )
        payload = result.to_dict()
        payload["summary"] = certification_dossier.summary
        return payload, _certification_exit_code(certification_dossier.state)
    raise ValueError(f"unsupported reconstruction command: {command}")


def _receipt_result(receipt: Any, path: Path | None) -> dict[str, Any]:
    payload = dict(receipt.to_dict())
    if path is not None:
        payload["receipt_path"] = str(path)
    return payload


def _certification_exit_code(
    state: CertificationState,
) -> ReconstructionExitCode:
    if state in {
        CertificationState.CERTIFIED,
        CertificationState.READY_FOR_PROMOTION,
    }:
        return ReconstructionExitCode.SUCCESS
    if state is CertificationState.INCOMPLETE:
        return ReconstructionExitCode.REFUSED
    return ReconstructionExitCode.VALIDATION_FAILURE


def _write_result(result: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, indent=2, sort_keys=True))
        return
    schema_version = result.get("schema_version")
    if schema_version == "histdatacom.reconstruction-schema-registry.v1":
        print(
            "Reconstruction schemas "
            f"({result.get('registry_id', '')}): "
            f"{result.get('contract_count', 0)} contracts"
        )
        scope = result.get("current_scope", {})
        if isinstance(scope, Mapping):
            print(
                "executable scope: "
                f"{scope.get('provider')}/"
                f"{scope.get('source_format')}/"
                f"{scope.get('timeframe')}"
            )
            print("broker/OANDA: later milestone")
        counts = result.get("status_counts", {})
        if isinstance(counts, Mapping):
            summary = ", ".join(
                f"{key}={counts[key]}" for key in sorted(counts)
            )
            print(f"contract status: {summary}")
        return
    if schema_version == "histdatacom.reconstruction-scientific-ledger.v1":
        estimand = result.get("estimand", {})
        estimand_id = (
            estimand.get("estimand_id", "")
            if isinstance(estimand, Mapping)
            else ""
        )
        print(
            "Reconstruction science "
            f"({result.get('ledger_id', '')}): {result.get('scope', '')}"
        )
        print(f"estimand: {estimand_id}")
        print(
            "assumptions/context states: "
            f"{len(result.get('assumptions', ()))} / "
            f"{len(result.get('context_missingness', ()))}"
        )
        print("broker/OANDA: later milestone")
        return
    if schema_version == "histdatacom.reconstruction-compatibility-report.v1":
        print(
            "Reconstruction compatibility "
            f"{result.get('status')} ({result.get('report_id', '')})"
        )
        print(f"executable: {str(bool(result.get('executable'))).lower()}")
        for finding in result.get("findings", ()):  # type: ignore[union-attr]
            if isinstance(finding, Mapping):
                print(
                    f"{finding.get('status')} {finding.get('code')}: "
                    f"{finding.get('message')}"
                )
        return
    if schema_version == "histdatacom.reconstruction-diagnostic-publication.v1":
        print(
            "Reconstruction diagnostics "
            f"({result.get('publication_id', '')}): "
            f"{result.get('family_count', 0)} families, "
            f"{result.get('chart_count', 0)} views"
        )
        counts = result.get("status_counts", {})
        if isinstance(counts, Mapping):
            summary = ", ".join(
                f"{key}={counts[key]}" for key in sorted(counts)
            )
            print(f"diagnostic status: {summary}")
        return
    status = result.get("status") or result.get("kind") or "complete"
    identity = (
        result.get("receipt_id")
        or result.get("request_id")
        or result.get("plan_id")
        or result.get("sha256")
        or ""
    )
    suffix = f" ({identity})" if identity else ""
    print(f"Reconstruction {status}{suffix}")
    for key in ("path", "request_path", "receipt_path", "manifest_path"):
        if result.get(key):
            print(f"{key}: {result[key]}")


def _write_error(
    error: Exception,
    *,
    reason_code: str,
    exit_code: ReconstructionExitCode,
    as_json: bool,
) -> int:
    payload = {
        "status": "error",
        "reason_code": reason_code,
        "message": str(error),
        "exit_code": int(exit_code),
    }
    if as_json:
        print(json.dumps(payload, sort_keys=True), file=sys.stderr)
    else:
        print(f"reconstruction error [{reason_code}]: {error}", file=sys.stderr)
    return int(exit_code)


__all__ = ["build_parser", "main"]
