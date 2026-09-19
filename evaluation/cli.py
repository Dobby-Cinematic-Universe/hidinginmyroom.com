"""Command-line contracts for proposal and frozen transcript evaluation data."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .interval_proposal import (
    DEFAULT_POLICY,
    emit_full_rendition_request,
    emit_local_window_proposal_request,
    prepare_interval_proposal,
    validate_interval_proposal,
    validate_interval_proposal_request,
)
from .selection_review import (
    compile_interval_freeze,
    emit_selection_review_template,
    validate_compiled_interval_freeze,
    validate_interval_selection_review,
)
from .scoring import (
    score_transcript_system,
    seal_transcript_system_output,
    validate_transcript_score_report,
    validate_transcript_system_output,
)
from .candidate_comparison import (
    compare_transcript_systems,
    seal_gpu_evaluation_measurement,
    validate_gpu_evaluation_measurement,
    validate_transcript_system_comparison,
)
from .validation import (
    ContractError,
    acquisition_selection,
    audit_tracked_evaluation_data,
    canonical_manifest_sha256,
    load_json,
    validate_adjudication,
    validate_annotation,
    validate_candidate_cohort,
    validate_interval_freeze,
)


def _proposal_policy(args: argparse.Namespace) -> dict[str, int]:
    return {
        key: getattr(args, key)
        for key in DEFAULT_POLICY
    }


def _emit_proposal_request(args: argparse.Namespace) -> dict[str, object]:
    return emit_full_rendition_request(
        load_json(args.cohort),
        args.catalog,
        args.preprocess_result,
        args.created_at,
        _proposal_policy(args),
    )


def _emit_local_window_proposal_request(
    args: argparse.Namespace,
) -> dict[str, object]:
    return emit_local_window_proposal_request(
        load_json(args.cohort),
        args.catalog,
        args.local_window_result,
        args.preprocess_result,
        args.created_at,
        _proposal_policy(args),
    )


def _validate_proposal_request(args: argparse.Namespace) -> dict[str, object]:
    request = validate_interval_proposal_request(
        load_json(args.request), load_json(args.cohort)
    )
    return {
        "valid": True,
        "manifest_kind": "interval_proposal_request",
        "request_id": request["request_id"],
        "proposal_created": False,
        "freeze_created": False,
    }


def _propose_intervals(args: argparse.Namespace) -> dict[str, object]:
    return prepare_interval_proposal(
        load_json(args.request), load_json(args.cohort), args.catalog
    )


def _validate_proposal(args: argparse.Namespace) -> dict[str, object]:
    proposal = validate_interval_proposal(
        load_json(args.proposal),
        load_json(args.request),
        load_json(args.cohort),
        args.catalog,
    )
    return {
        "valid": True,
        "manifest_kind": "interval_proposal",
        "proposal_id": proposal["proposal_id"],
        "proposal_state": "proposal_unreviewed",
        "reference_quality_claimed": False,
        "freeze_created": False,
    }


def _validate_candidate(args: argparse.Namespace) -> dict[str, object]:
    manifest = load_json(args.cohort)
    validate_candidate_cohort(manifest)
    return {"valid": True, "manifest_kind": "candidate_cohort", "cohort_id": manifest["cohort_id"]}


def _selection_inputs(args: argparse.Namespace) -> tuple[list[object], list[object]]:
    if len(args.request) != 2 or len(args.proposal) != 2:
        raise ContractError("$.proposal_inputs: requires exactly two --request/--proposal pairs")
    return (
        [load_json(path) for path in args.request],
        [load_json(path) for path in args.proposal],
    )


def _emit_selection_review_template(args: argparse.Namespace) -> dict[str, object]:
    requests, proposals = _selection_inputs(args)
    return emit_selection_review_template(
        load_json(args.cohort),
        requests,
        proposals,
        args.catalog,
        args.created_at,
        args.protocol_revision,
    )


def _validate_selection_review(args: argparse.Namespace) -> dict[str, object]:
    requests, proposals = _selection_inputs(args)
    review = validate_interval_selection_review(
        load_json(args.review),
        load_json(args.cohort),
        requests,
        proposals,
        args.catalog,
        require_completed=True,
    )
    return {
        "valid": True,
        "manifest_kind": "interval_selection_review",
        "review_id": review["review_id"],
        "review_state": review["review_state"],
        "freeze_ready": True,
    }


def _compile_selection_freeze(args: argparse.Namespace) -> dict[str, object]:
    requests, proposals = _selection_inputs(args)
    return compile_interval_freeze(
        load_json(args.review),
        load_json(args.cohort),
        requests,
        proposals,
        args.catalog,
        args.frozen_at,
    )


def _emit_acquisition_selection(args: argparse.Namespace) -> dict[str, object]:
    return acquisition_selection(load_json(args.cohort))


def _validate_freeze(args: argparse.Namespace) -> dict[str, object]:
    cohort = load_json(args.cohort)
    freeze = load_json(args.freeze)
    if freeze.get("schema_version") == 2:
        if not args.review or not args.catalog or not args.request or not args.proposal:
            raise ContractError(
                "$.selection_provenance: v2 CLI validation requires --review, --catalog, "
                "and exactly two --request/--proposal pairs"
            )
        requests, proposals = _selection_inputs(args)
        validate_compiled_interval_freeze(
            freeze,
            load_json(args.review),
            cohort,
            requests,
            proposals,
            args.catalog,
        )
    else:
        validate_interval_freeze(freeze, cohort)
    return {"valid": True, "manifest_kind": "interval_freeze", "freeze_id": freeze["freeze_id"]}


def _validate_annotation(args: argparse.Namespace) -> dict[str, object]:
    cohort = load_json(args.cohort)
    freeze = load_json(args.freeze)
    annotation = load_json(args.annotation)
    validate_annotation(annotation, freeze, cohort)
    return {
        "valid": True,
        "manifest_kind": "reference_annotation",
        "annotation_id": annotation["annotation_id"],
        "pass_name": annotation["pass_name"],
    }


def _validate_adjudication(args: argparse.Namespace) -> dict[str, object]:
    cohort = load_json(args.cohort)
    freeze = load_json(args.freeze)
    pass_a = load_json(args.pass_a)
    pass_b = load_json(args.pass_b)
    adjudication = load_json(args.adjudication)
    validate_adjudication(adjudication, freeze, cohort, pass_a, pass_b)
    return {
        "valid": True,
        "manifest_kind": "reference_adjudication",
        "adjudication_id": adjudication["adjudication_id"],
        "reference_ready": True,
        "accuracy_metrics_computed": False,
    }


def _validate_transcript_system_output(args: argparse.Namespace) -> dict[str, object]:
    validated = validate_transcript_system_output(
        load_json(args.system_output),
        load_json(args.freeze),
        load_json(args.cohort),
    )
    return {
        "valid": True,
        "manifest_kind": "transcript_system_output",
        "system_output_id": validated.manifest["system_output_id"],
        "complete_frozen_interval_coverage": True,
        "accuracy_metrics_computed": False,
    }


def _seal_transcript_system_output(args: argparse.Namespace) -> dict[str, object]:
    output = seal_transcript_system_output(load_json(args.system_output))
    validate_transcript_system_output(
        output,
        load_json(args.freeze),
        load_json(args.cohort),
    )
    return output


def _score_transcript_system(args: argparse.Namespace) -> dict[str, object]:
    return score_transcript_system(
        candidate_cohort=load_json(args.cohort),
        interval_freeze=load_json(args.freeze),
        pass_a=load_json(args.pass_a),
        pass_b=load_json(args.pass_b),
        adjudication=load_json(args.adjudication),
        system_output=load_json(args.system_output),
        created_at=args.created_at,
    )


def _validate_transcript_score_report(args: argparse.Namespace) -> dict[str, object]:
    report = validate_transcript_score_report(
        load_json(args.report),
        candidate_cohort=load_json(args.cohort),
        interval_freeze=load_json(args.freeze),
        pass_a=load_json(args.pass_a),
        pass_b=load_json(args.pass_b),
        adjudication=load_json(args.adjudication),
        system_output=load_json(args.system_output),
    )
    return {
        "valid": True,
        "manifest_kind": "transcript_score_report",
        "report_id": report["report_id"],
        "accuracy_metrics_computed": True,
        "calibrated_probability_claimed": False,
    }


def _seal_gpu_evaluation_measurement(args: argparse.Namespace) -> dict[str, object]:
    measurement = seal_gpu_evaluation_measurement(load_json(args.measurement))
    validate_gpu_evaluation_measurement(
        measurement,
        system_output=load_json(args.system_output),
        interval_freeze=load_json(args.freeze),
        candidate_cohort=load_json(args.cohort),
    )
    return measurement


def _validate_gpu_evaluation_measurement(args: argparse.Namespace) -> dict[str, object]:
    measurement = validate_gpu_evaluation_measurement(
        load_json(args.measurement),
        system_output=load_json(args.system_output),
        interval_freeze=load_json(args.freeze),
        candidate_cohort=load_json(args.cohort),
    )
    return {
        "valid": True,
        "manifest_kind": "gpu_asr_evaluation_measurement",
        "measurement_id": measurement["measurement_id"],
        "inference_performed": False,
    }


def _comparison_inputs(args: argparse.Namespace) -> dict[str, object]:
    return {
        "candidate_cohort": load_json(args.cohort),
        "interval_freeze": load_json(args.freeze),
        "pass_a": load_json(args.pass_a),
        "pass_b": load_json(args.pass_b),
        "adjudication": load_json(args.adjudication),
        "baseline_system_output": load_json(args.baseline_system_output),
        "challenger_system_output": load_json(args.challenger_system_output),
        "baseline_gpu_measurement": load_json(args.baseline_gpu_measurement),
        "challenger_gpu_measurement": load_json(args.challenger_gpu_measurement),
    }


def _compare_transcript_systems(args: argparse.Namespace) -> dict[str, object]:
    return compare_transcript_systems(
        **_comparison_inputs(args), created_at=args.created_at
    )


def _validate_transcript_system_comparison(args: argparse.Namespace) -> dict[str, object]:
    comparison = validate_transcript_system_comparison(
        load_json(args.comparison), **_comparison_inputs(args)
    )
    return {
        "valid": True,
        "manifest_kind": "transcript_system_comparison",
        "comparison_id": comparison["comparison_id"],
        "automatic_promotion_authorized": False,
    }


def _digest(args: argparse.Namespace) -> dict[str, object]:
    manifest = load_json(args.manifest)
    return {
        "manifest": str(args.manifest),
        "canonical_manifest_sha256": canonical_manifest_sha256(manifest),
    }


def _audit_tracked(args: argparse.Namespace) -> dict[str, object]:
    checked = audit_tracked_evaluation_data(args.repository_root)
    return {"valid": True, "released_reference_manifests_checked": checked}


def _serve_selection_review(args: argparse.Namespace) -> dict[str, object]:
    # Imported lazily so ordinary contract validation remains dependency-light and
    # never opens workspace, catalogue, or media handles.
    from .review_workspace import serve_selection_review

    result = serve_selection_review(
        cohort_path=args.cohort,
        catalog_path=args.catalog,
        request_paths=args.request,
        proposal_paths=args.proposal,
        template_path=args.template,
        media_root=args.media_root,
        workspace_path=args.workspace,
        port=args.port,
        guard_writable_media=args.guard_writable_media,
        open_browser=not args.no_open_browser,
        prepare_only=args.prepare_only,
    )
    if result is None:
        return {
            "stopped": True,
            "completed_review_created_automatically": False,
        }
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    candidate = subparsers.add_parser("validate-candidate", help="validate an unfrozen candidate cohort")
    candidate.add_argument("cohort", type=Path)
    candidate.set_defaults(handler=_validate_candidate)

    selection = subparsers.add_parser(
        "emit-acquisition-selection",
        help="emit ordered public YouTube IDs and stable IDs for queue planning",
    )
    selection.add_argument("cohort", type=Path)
    selection.set_defaults(handler=_emit_acquisition_selection)

    proposal_request = subparsers.add_parser(
        "emit-proposal-request",
        help="emit a pinned full-rendition proposal request to stdout only",
    )
    proposal_request.add_argument("--cohort", type=Path, required=True)
    proposal_request.add_argument("--catalog", type=Path, required=True)
    proposal_request.add_argument("--created-at", required=True)
    proposal_request.add_argument(
        "--preprocess-result",
        type=Path,
        action="append",
        required=True,
        help="sealed media_preprocess result; repeat once per processed recording",
    )
    for option, default in DEFAULT_POLICY.items():
        proposal_request.add_argument(
            "--" + option.replace("_", "-"),
            type=int,
            default=default,
        )
    proposal_request.set_defaults(handler=_emit_proposal_request)

    local_proposal_request = subparsers.add_parser(
        "emit-local-window-proposal-request",
        help="emit a grouped v2 request for admitted local windows to stdout only",
    )
    local_proposal_request.add_argument("--cohort", type=Path, required=True)
    local_proposal_request.add_argument("--catalog", type=Path, required=True)
    local_proposal_request.add_argument("--created-at", required=True)
    local_proposal_request.add_argument(
        "--local-window-result",
        type=Path,
        action="append",
        required=True,
        help="sealed admitted local-window result; repeat in matching pair order",
    )
    local_proposal_request.add_argument(
        "--preprocess-result",
        type=Path,
        action="append",
        required=True,
        help="routing-only media_preprocess result; repeat in matching pair order",
    )
    for option, default in DEFAULT_POLICY.items():
        local_proposal_request.add_argument(
            "--" + option.replace("_", "-"),
            type=int,
            default=default,
        )
    local_proposal_request.set_defaults(handler=_emit_local_window_proposal_request)

    proposal_request_validation = subparsers.add_parser(
        "validate-proposal-request",
        help="validate a proposal request without creating a proposal or freeze",
    )
    proposal_request_validation.add_argument("--cohort", type=Path, required=True)
    proposal_request_validation.add_argument("request", type=Path)
    proposal_request_validation.set_defaults(handler=_validate_proposal_request)

    proposal = subparsers.add_parser(
        "propose-intervals",
        help="emit deterministic proposal_unreviewed intervals to stdout only",
    )
    proposal.add_argument("--cohort", type=Path, required=True)
    proposal.add_argument("--catalog", type=Path, required=True)
    proposal.add_argument("request", type=Path)
    proposal.set_defaults(handler=_propose_intervals)

    proposal_validation = subparsers.add_parser(
        "validate-proposal",
        help="regenerate and validate one unreviewed interval proposal",
    )
    proposal_validation.add_argument("--cohort", type=Path, required=True)
    proposal_validation.add_argument("--catalog", type=Path, required=True)
    proposal_validation.add_argument("--request", type=Path, required=True)
    proposal_validation.add_argument("proposal", type=Path)
    proposal_validation.set_defaults(handler=_validate_proposal)

    def add_selection_inputs(command: argparse.ArgumentParser) -> None:
        command.add_argument("--cohort", type=Path, required=True)
        command.add_argument("--catalog", type=Path, required=True)
        command.add_argument("--request", type=Path, action="append", required=True)
        command.add_argument("--proposal", type=Path, action="append", required=True)

    review_template = subparsers.add_parser(
        "emit-selection-review-template",
        help="emit an incomplete private ASR-blind review template to stdout only",
    )
    add_selection_inputs(review_template)
    review_template.add_argument("--created-at", required=True)
    review_template.add_argument(
        "--protocol-revision", default="transcript_eval_protocol_v1"
    )
    review_template.set_defaults(handler=_emit_selection_review_template)

    review_validation = subparsers.add_parser(
        "validate-selection-review",
        help="validate a completed private selection review without writing files",
    )
    add_selection_inputs(review_validation)
    review_validation.add_argument("review", type=Path)
    review_validation.set_defaults(handler=_validate_selection_review)

    review_workspace = subparsers.add_parser(
        "serve-selection-review",
        help="run a private loopback-only direct-parent-media selection workspace",
    )
    add_selection_inputs(review_workspace)
    review_workspace.add_argument("--template", type=Path, required=True)
    review_workspace.add_argument("--media-root", type=Path, required=True)
    review_workspace.add_argument("--workspace", type=Path, required=True)
    review_workspace.add_argument("--port", type=int, default=0)
    review_workspace.add_argument(
        "--guard-writable-media",
        action="store_true",
        help=(
            "explicitly guard and rehash owner-writable exact parents; does not "
            "claim immutability or OS confidentiality"
        ),
    )
    review_workspace.add_argument(
        "--no-open-browser",
        action="store_true",
        help="print the one-time loopback URL without opening a browser",
    )
    review_workspace.add_argument(
        "--prepare-only",
        action="store_true",
        help="verify and initialize private state, then exit without listening",
    )
    review_workspace.set_defaults(handler=_serve_selection_review)

    freeze_compilation = subparsers.add_parser(
        "compile-freeze",
        help="compile a completed private review into a v2 freeze on stdout only",
    )
    add_selection_inputs(freeze_compilation)
    freeze_compilation.add_argument("--frozen-at", required=True)
    freeze_compilation.add_argument("review", type=Path)
    freeze_compilation.set_defaults(handler=_compile_selection_freeze)

    freeze = subparsers.add_parser("validate-freeze", help="validate a media-bound interval freeze")
    freeze.add_argument("--cohort", type=Path, required=True)
    freeze.add_argument("--catalog", type=Path)
    freeze.add_argument("--review", type=Path)
    freeze.add_argument("--request", type=Path, action="append")
    freeze.add_argument("--proposal", type=Path, action="append")
    freeze.add_argument("freeze", type=Path)
    freeze.set_defaults(handler=_validate_freeze)

    annotation = subparsers.add_parser("validate-annotation", help="validate one independent reference pass")
    annotation.add_argument("--cohort", type=Path, required=True)
    annotation.add_argument("--freeze", type=Path, required=True)
    annotation.add_argument("annotation", type=Path)
    annotation.set_defaults(handler=_validate_annotation)

    adjudication = subparsers.add_parser("validate-adjudication", help="validate two passes and adjudication")
    adjudication.add_argument("--cohort", type=Path, required=True)
    adjudication.add_argument("--freeze", type=Path, required=True)
    adjudication.add_argument("--pass-a", type=Path, required=True)
    adjudication.add_argument("--pass-b", type=Path, required=True)
    adjudication.add_argument("adjudication", type=Path)
    adjudication.set_defaults(handler=_validate_adjudication)

    system_output = subparsers.add_parser(
        "validate-transcript-system-output",
        help="validate complete private system hypotheses against an interval freeze",
    )
    system_output.add_argument("--cohort", type=Path, required=True)
    system_output.add_argument("--freeze", type=Path, required=True)
    system_output.add_argument("system_output", type=Path)
    system_output.set_defaults(handler=_validate_transcript_system_output)

    system_output_seal = subparsers.add_parser(
        "seal-transcript-system-output",
        help="seal and emit a complete private system-output manifest to stdout",
    )
    system_output_seal.add_argument("--cohort", type=Path, required=True)
    system_output_seal.add_argument("--freeze", type=Path, required=True)
    system_output_seal.add_argument("system_output", type=Path)
    system_output_seal.set_defaults(handler=_seal_transcript_system_output)

    def add_scoring_inputs(command: argparse.ArgumentParser) -> None:
        command.add_argument("--cohort", type=Path, required=True)
        command.add_argument("--freeze", type=Path, required=True)
        command.add_argument("--pass-a", type=Path, required=True)
        command.add_argument("--pass-b", type=Path, required=True)
        command.add_argument("--adjudication", type=Path, required=True)
        command.add_argument("--system-output", type=Path, required=True)

    score = subparsers.add_parser(
        "score-transcript-system",
        help="emit a private, text-free report over an adjudicated frozen reference",
    )
    add_scoring_inputs(score)
    score.add_argument("--created-at", required=True)
    score.set_defaults(handler=_score_transcript_system)

    score_validation = subparsers.add_parser(
        "validate-transcript-score-report",
        help="recompute and validate a sealed transcript score report",
    )
    add_scoring_inputs(score_validation)
    score_validation.add_argument("report", type=Path)
    score_validation.set_defaults(handler=_validate_transcript_score_report)

    def add_gpu_measurement_inputs(command: argparse.ArgumentParser) -> None:
        command.add_argument("--cohort", type=Path, required=True)
        command.add_argument("--freeze", type=Path, required=True)
        command.add_argument("--system-output", type=Path, required=True)
        command.add_argument("measurement", type=Path)

    measurement_seal = subparsers.add_parser(
        "seal-gpu-evaluation-measurement",
        help="seal one text-free GPU efficiency measurement to stdout",
    )
    add_gpu_measurement_inputs(measurement_seal)
    measurement_seal.set_defaults(handler=_seal_gpu_evaluation_measurement)

    measurement_validation = subparsers.add_parser(
        "validate-gpu-evaluation-measurement",
        help="validate a sealed GPU measurement against one system output",
    )
    add_gpu_measurement_inputs(measurement_validation)
    measurement_validation.set_defaults(handler=_validate_gpu_evaluation_measurement)

    def add_comparison_inputs(command: argparse.ArgumentParser) -> None:
        command.add_argument("--cohort", type=Path, required=True)
        command.add_argument("--freeze", type=Path, required=True)
        command.add_argument("--pass-a", type=Path, required=True)
        command.add_argument("--pass-b", type=Path, required=True)
        command.add_argument("--adjudication", type=Path, required=True)
        command.add_argument("--baseline-system-output", type=Path, required=True)
        command.add_argument("--challenger-system-output", type=Path, required=True)
        command.add_argument("--baseline-gpu-measurement", type=Path, required=True)
        command.add_argument("--challenger-gpu-measurement", type=Path, required=True)

    comparison = subparsers.add_parser(
        "compare-transcript-systems",
        help="emit a paired text-free accuracy/GPU-efficiency report",
    )
    add_comparison_inputs(comparison)
    comparison.add_argument("--created-at", required=True)
    comparison.set_defaults(handler=_compare_transcript_systems)

    comparison_validation = subparsers.add_parser(
        "validate-transcript-system-comparison",
        help="recompute and validate a paired candidate comparison",
    )
    add_comparison_inputs(comparison_validation)
    comparison_validation.add_argument("comparison", type=Path)
    comparison_validation.set_defaults(handler=_validate_transcript_system_comparison)

    digest = subparsers.add_parser("digest", help="print the canonical digest to seal a manifest")
    digest.add_argument("manifest", type=Path)
    digest.set_defaults(handler=_digest)

    audit = subparsers.add_parser(
        "audit-tracked", help="reject tracked private evaluation artifacts"
    )
    audit.add_argument("repository_root", type=Path, nargs="?", default=Path.cwd())
    audit.set_defaults(handler=_audit_tracked)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        output = args.handler(args)
    except ContractError as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps(output, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
