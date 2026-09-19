#!/usr/bin/env python3
"""Validate tracked JSON Schemas, examples, and optional generated instances."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATHS = (
    Path("acquisition/schemas/work-order.schema.json"),
    Path("acquisition/schemas/result.schema.json"),
    Path("acquisition/schemas/queue-selection.schema.json"),
    Path("acquisition/schemas/queue-plan.schema.json"),
    Path("acquisition/schemas/queue-bundle-manifest.schema.json"),
    Path("acquisition/schemas/campaign-epoch-manifest.schema.json"),
    Path("acquisition/schemas/background-producer-schedule.schema.json"),
    Path("acquisition/schemas/campaign-background-schedule-set-manifest.schema.json"),
    Path("acquisition/schemas/composite-campaign-schedule-set-manifest.schema.json"),
    Path("acquisition/schemas/long-recording-bundle-manifest.schema.json"),
    Path("acquisition/schemas/reddit-rss-snapshot.schema.json"),
    Path("acquisition/schemas/reddit-rss-discovery.schema.json"),
    Path("acquisition/schemas/reddit-video-selection.schema.json"),
    Path("acquisition/schemas/reddit-video-plan.schema.json"),
    Path("acquisition/schemas/reddit-video-bundle-manifest.schema.json"),
    Path("acquisition/schemas/archive-metadata-request.schema.json"),
    Path("acquisition/schemas/archive-metadata-snapshot.schema.json"),
    Path("acquisition/schemas/archive-metadata-delta.schema.json"),
    Path("acquisition/schemas/cold-storage-transfer-receipt.schema.json"),
    Path("pipeline/schemas/work-order.schema.json"),
    Path("pipeline/schemas/result.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-work-order.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-result.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-quarantine-receipt.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-batch-disposition.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-result-seal-plan.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-result-seal-receipt.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-batch-manifest.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-batch-run.schema.json"),
    Path("pipeline/schemas/contextual-asr-batch-manifest.schema.json"),
    Path("pipeline/schemas/contextual-asr-batch-run.schema.json"),
    Path("pipeline/schemas/contextual-asr-result-seal-plan.schema.json"),
    Path("pipeline/schemas/contextual-asr-result-seal-receipt.schema.json"),
    Path("pipeline/schemas/contextual-asr-diff.schema.json"),
    Path("pipeline/schemas/transform-calibration-receipt.schema.json"),
    Path("pipeline/schemas/preprocess-asr-queue-manifest.schema.json"),
    Path("pipeline/schemas/preprocess-asr-queue-run.schema.json"),
    Path("pipeline/schemas/sparse-frame-work-order.schema.json"),
    Path("pipeline/schemas/sparse-frame-result.schema.json"),
    Path("pipeline/schemas/ocr-tesseract-work-order.schema.json"),
    Path("pipeline/schemas/ocr-tesseract-result.schema.json"),
    Path("pipeline/schemas/whispercpp-vad-work-order.schema.json"),
    Path("pipeline/schemas/whispercpp-vad-result.schema.json"),
    Path("pipeline/schemas/audio-fingerprint-work-order.schema.json"),
    Path("pipeline/schemas/audio-fingerprint-result.schema.json"),
    Path("pipeline/schemas/audio-fingerprint-compare-work-order.schema.json"),
    Path("pipeline/schemas/audio-fingerprint-compare-result.schema.json"),
    Path("pipeline/schemas/audio-fingerprint-compare-work-order-v2.schema.json"),
    Path("pipeline/schemas/audio-fingerprint-compare-result-v2.schema.json"),
    Path("pipeline/schemas/visual-fingerprint-work-order.schema.json"),
    Path("pipeline/schemas/visual-fingerprint-result.schema.json"),
    Path("pipeline/schemas/visual-fingerprint-compare-work-order.schema.json"),
    Path("pipeline/schemas/visual-fingerprint-compare-result.schema.json"),
    Path("pipeline/schemas/face-candidate-work-order.schema.json"),
    Path("pipeline/schemas/face-candidate-result.schema.json"),
    Path("pipeline/schemas/shot-local-face-detections.schema.json"),
    Path("pipeline/schemas/yunet-face-detector-work-order.schema.json"),
    Path("pipeline/schemas/yunet-face-detector-result.schema.json"),
    Path("pipeline/schemas/shot-local-face-tracker-work-order.schema.json"),
    Path("pipeline/schemas/shot-local-face-tracker-result.schema.json"),
    Path("pipeline/schemas/speaker-reviewed-hints.schema.json"),
    Path("pipeline/schemas/speaker-activity-routing-work-order.schema.json"),
    Path("pipeline/schemas/speaker-activity-routing-result.schema.json"),
    Path("pipeline/schemas/local-window-work-order.schema.json"),
    Path("pipeline/schemas/local-window-bundle-manifest.schema.json"),
    Path("pipeline/schemas/local-window-result.schema.json"),
    Path("pipeline/schemas/preprocess-batch-selection.schema.json"),
    Path("pipeline/schemas/preprocess-batch-manifest.schema.json"),
    Path("pipeline/schemas/preprocess-batch-receipt.schema.json"),
    Path("pipeline/schemas/preprocess-batch-run-summary.schema.json"),
    Path("pipeline/schemas/longform-asr-span-work-order.schema.json"),
    Path("pipeline/schemas/longform-asr-span-transcript.schema.json"),
    Path("pipeline/schemas/longform-asr-span-result.schema.json"),
    Path("pipeline/schemas/longform-span-transcript-bindings.schema.json"),
    Path("pipeline/schemas/longform-recording-transcript.schema.json"),
    Path("pipeline/schemas/longform-asr-campaign-config.schema.json"),
    Path("pipeline/schemas/neutral-glossary.schema.json"),
    Path("corpus/schemas/public-release.schema.json"),
    Path("corpus/schemas/public-release-v2.schema.json"),
    Path("corpus/schemas/public-catalog-shard-v2.schema.json"),
    Path("corpus/schemas/public-recording-shard-v2.schema.json"),
    Path("corpus/schemas/public-entity-event-graph-manifest.schema.json"),
    Path("corpus/schemas/public-entity-event-graph.schema.json"),
    Path("corpus/schemas/publication-manifest.schema.json"),
    Path("corpus/schemas/reviewer-admin-manifest.schema.json"),
    Path("corpus/schemas/model-registry-manifest.schema.json"),
    Path("corpus/schemas/entity-event-map-manifest.schema.json"),
    Path("corpus/schemas/private-solo-voice-attestation-manifest.schema.json"),
    Path("corpus/schemas/local-window-catalog-admission.schema.json"),
    Path("corpus/schemas/rendition-local-asr-admission.schema.json"),
    Path("corpus/schemas/media-local-asr-admission.schema.json"),
    Path("corpus/schemas/private-glossary-registration.schema.json"),
    Path("corpus/schemas/contextual-media-local-asr-admission.schema.json"),
    Path("corpus/schemas/archive-hint-provider-reconciliation-plan.schema.json"),
    Path("corpus/schemas/torrent-bracket-reconciliation-plan.schema.json"),
    Path("corpus/schemas/torrent-suffix-audio-reconciliation-plan.schema.json"),
    Path("corpus/schemas/torrent-selective-acquisition-plan.schema.json"),
    Path("corpus/schemas/torrent-youtube-availability-probe.schema.json"),
    Path("corpus/schemas/torrent-acquisition-audit-receipt.schema.json"),
    Path("corpus/schemas/private-acquisition-seal.schema.json"),
    Path("corpus/schemas/longform-asr-recording-input-manifest.schema.json"),
    Path("corpus/schemas/longform-asr-planning-policy.schema.json"),
    Path("corpus/schemas/longform-asr-plan.schema.json"),
    Path("evaluation/schemas/acquisition-selection.schema.json"),
    Path("evaluation/schemas/candidate-cohort.schema.json"),
    Path("evaluation/schemas/interval-proposal-request.schema.json"),
    Path("evaluation/schemas/interval-proposal-request-v2.schema.json"),
    Path("evaluation/schemas/interval-proposal.schema.json"),
    Path("evaluation/schemas/interval-proposal-v2.schema.json"),
    Path("evaluation/schemas/interval-freeze.schema.json"),
    Path("evaluation/schemas/interval-freeze-v2.schema.json"),
    Path("evaluation/schemas/interval-selection-review.schema.json"),
    Path("evaluation/schemas/reference-annotation.schema.json"),
    Path("evaluation/schemas/reference-adjudication.schema.json"),
)
EXAMPLES = (
    (
        Path("acquisition/schemas/work-order.schema.json"),
        Path("acquisition/examples/work-order.local.example.json"),
    ),
    (
        Path("acquisition/schemas/queue-selection.schema.json"),
        Path("acquisition/examples/queue-selection.example.json"),
    ),
    (
        Path("acquisition/schemas/queue-bundle-manifest.schema.json"),
        Path("acquisition/examples/queue-bundle-manifest.example.json"),
    ),
    (
        Path("acquisition/schemas/reddit-rss-snapshot.schema.json"),
        Path("acquisition/examples/reddit-rss-snapshot.example.json"),
    ),
    (
        Path("acquisition/schemas/reddit-rss-discovery.schema.json"),
        Path("acquisition/examples/reddit-rss-discovery.example.json"),
    ),
    (
        Path("acquisition/schemas/reddit-video-selection.schema.json"),
        Path("acquisition/examples/reddit-video-selection.example.json"),
    ),
    (
        Path("acquisition/schemas/archive-metadata-request.schema.json"),
        Path("acquisition/examples/archive-metadata-request.example.json"),
    ),
    (
        Path("pipeline/schemas/work-order.schema.json"),
        Path("pipeline/examples/work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/asr-whispercpp-work-order.schema.json"),
        Path("pipeline/examples/asr-whispercpp-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/sparse-frame-work-order.schema.json"),
        Path("pipeline/examples/sparse-frame-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/whispercpp-vad-work-order.schema.json"),
        Path("pipeline/examples/whispercpp-vad-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/neutral-glossary.schema.json"),
        Path("pipeline/examples/neutral-glossary.example.json"),
    ),
    (
        Path("corpus/schemas/longform-asr-planning-policy.schema.json"),
        Path("corpus/examples/longform-asr-policy.initial-candidate.json"),
    ),
    (
        Path("pipeline/schemas/audio-fingerprint-work-order.schema.json"),
        Path("pipeline/examples/audio-fingerprint-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/audio-fingerprint-compare-work-order.schema.json"),
        Path("pipeline/examples/audio-fingerprint-compare-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/audio-fingerprint-compare-work-order-v2.schema.json"),
        Path("pipeline/examples/audio-fingerprint-compare-work-order-v2.example.json"),
    ),
    (
        Path("pipeline/schemas/visual-fingerprint-work-order.schema.json"),
        Path("pipeline/examples/visual-fingerprint-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/visual-fingerprint-compare-work-order.schema.json"),
        Path("pipeline/examples/visual-fingerprint-compare-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/face-candidate-work-order.schema.json"),
        Path("pipeline/examples/face-candidate-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/shot-local-face-detections.schema.json"),
        Path("pipeline/examples/shot-local-face-detections.example.json"),
    ),
    (
        Path("pipeline/schemas/yunet-face-detector-work-order.schema.json"),
        Path("pipeline/examples/yunet-face-detector-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/yunet-face-detector-result.schema.json"),
        Path("pipeline/examples/yunet-face-detector-result.example.json"),
    ),
    (
        Path("pipeline/schemas/shot-local-face-tracker-work-order.schema.json"),
        Path("pipeline/examples/shot-local-face-tracker-work-order.example.json"),
    ),
    (
        Path("pipeline/schemas/shot-local-face-tracker-result.schema.json"),
        Path("pipeline/examples/shot-local-face-tracker-result.example.json"),
    ),
    (
        Path("pipeline/schemas/speaker-reviewed-hints.schema.json"),
        Path("pipeline/examples/speaker-reviewed-hints.example.json"),
    ),
    (
        Path("pipeline/schemas/speaker-reviewed-hints.schema.json"),
        Path("pipeline/examples/speaker-reviewed-hints.daniel-solo.example.json"),
    ),
    (
        Path("pipeline/schemas/speaker-activity-routing-work-order.schema.json"),
        Path("pipeline/examples/speaker-activity-routing-work-order.example.json"),
    ),
    (
        Path("corpus/schemas/model-registry-manifest.schema.json"),
        Path("corpus/examples/model-registry-manifest.example.json"),
    ),
    (
        Path("corpus/schemas/reviewer-admin-manifest.schema.json"),
        Path("corpus/examples/reviewer-admin-manifest.example.json"),
    ),
    (
        Path("corpus/schemas/entity-event-map-manifest.schema.json"),
        Path("corpus/examples/entity-event-map-manifest.fictional.example.json"),
    ),
    (
        Path("corpus/schemas/private-solo-voice-attestation-manifest.schema.json"),
        Path("corpus/examples/private-solo-voice-attestation-manifest.example.json"),
    ),
    (
        Path("corpus/schemas/local-window-catalog-admission.schema.json"),
        Path("corpus/examples/local-window-catalog-admission.example.json"),
    ),
    (
        Path("corpus/schemas/rendition-local-asr-admission.schema.json"),
        Path("corpus/examples/rendition-local-asr-admission.example.json"),
    ),
    (
        Path("corpus/schemas/media-local-asr-admission.schema.json"),
        Path("corpus/examples/media-local-asr-admission.example.json"),
    ),
    (
        Path("corpus/schemas/public-release-v2.schema.json"),
        Path("src/data/corpus/manifest.json"),
    ),
    (
        Path("corpus/schemas/public-entity-event-graph-manifest.schema.json"),
        Path("src/data/corpus/graph/manifest.json"),
    ),
    (
        Path("corpus/schemas/public-entity-event-graph.schema.json"),
        Path(
            "src/data/corpus/graph/releases/"
            "graph_release_3a9457e203b4cddc2422587b/graph/"
            "graph-f26f4499a71fab68.json"
        ),
    ),
    (
        Path("evaluation/schemas/candidate-cohort.schema.json"),
        Path("evaluation/cohorts/himr-asr-candidate-cohort-v1.json"),
    ),
)
STRICT_RESULT_SCHEMAS = (
    Path("acquisition/schemas/result.schema.json"),
    Path("acquisition/schemas/queue-plan.schema.json"),
    Path("acquisition/schemas/queue-bundle-manifest.schema.json"),
    Path("acquisition/schemas/reddit-rss-snapshot.schema.json"),
    Path("acquisition/schemas/reddit-rss-discovery.schema.json"),
    Path("acquisition/schemas/reddit-video-plan.schema.json"),
    Path("acquisition/schemas/reddit-video-bundle-manifest.schema.json"),
    Path("acquisition/schemas/archive-metadata-request.schema.json"),
    Path("acquisition/schemas/archive-metadata-snapshot.schema.json"),
    Path("acquisition/schemas/archive-metadata-delta.schema.json"),
    Path("acquisition/schemas/cold-storage-transfer-receipt.schema.json"),
    Path("acquisition/schemas/long-recording-bundle-manifest.schema.json"),
    Path("pipeline/schemas/result.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-quarantine-receipt.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-batch-disposition.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-result-seal-plan.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-result-seal-receipt.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-batch-manifest.schema.json"),
    Path("pipeline/schemas/asr-whispercpp-batch-run.schema.json"),
    Path("pipeline/schemas/contextual-asr-batch-manifest.schema.json"),
    Path("pipeline/schemas/contextual-asr-batch-run.schema.json"),
    Path("pipeline/schemas/contextual-asr-result-seal-plan.schema.json"),
    Path("pipeline/schemas/contextual-asr-result-seal-receipt.schema.json"),
    Path("pipeline/schemas/contextual-asr-diff.schema.json"),
    Path("pipeline/schemas/transform-calibration-receipt.schema.json"),
    Path("pipeline/schemas/preprocess-asr-queue-manifest.schema.json"),
    Path("pipeline/schemas/preprocess-asr-queue-run.schema.json"),
    Path("pipeline/schemas/sparse-frame-result.schema.json"),
    Path("pipeline/schemas/ocr-tesseract-result.schema.json"),
    Path("pipeline/schemas/whispercpp-vad-result.schema.json"),
    Path("pipeline/schemas/audio-fingerprint-result.schema.json"),
    Path("pipeline/schemas/audio-fingerprint-compare-result.schema.json"),
    Path("pipeline/schemas/audio-fingerprint-compare-result-v2.schema.json"),
    Path("pipeline/schemas/visual-fingerprint-result.schema.json"),
    Path("pipeline/schemas/visual-fingerprint-compare-result.schema.json"),
    Path("pipeline/schemas/face-candidate-result.schema.json"),
    Path("pipeline/schemas/yunet-face-detector-result.schema.json"),
    Path("pipeline/schemas/shot-local-face-tracker-result.schema.json"),
    Path("pipeline/schemas/speaker-activity-routing-result.schema.json"),
    Path("pipeline/schemas/local-window-bundle-manifest.schema.json"),
    Path("pipeline/schemas/local-window-result.schema.json"),
    Path("pipeline/schemas/preprocess-batch-selection.schema.json"),
    Path("pipeline/schemas/preprocess-batch-manifest.schema.json"),
    Path("pipeline/schemas/preprocess-batch-receipt.schema.json"),
    Path("pipeline/schemas/preprocess-batch-run-summary.schema.json"),
    Path("pipeline/schemas/longform-asr-span-work-order.schema.json"),
    Path("pipeline/schemas/longform-asr-span-transcript.schema.json"),
    Path("pipeline/schemas/longform-asr-span-result.schema.json"),
    Path("pipeline/schemas/longform-span-transcript-bindings.schema.json"),
    Path("pipeline/schemas/longform-recording-transcript.schema.json"),
    Path("evaluation/schemas/interval-proposal-request.schema.json"),
    Path("evaluation/schemas/interval-proposal-request-v2.schema.json"),
    Path("evaluation/schemas/interval-proposal.schema.json"),
    Path("evaluation/schemas/interval-proposal-v2.schema.json"),
    Path("evaluation/schemas/interval-freeze-v2.schema.json"),
    Path("evaluation/schemas/interval-selection-review.schema.json"),
    Path("corpus/schemas/public-release.schema.json"),
    Path("corpus/schemas/public-release-v2.schema.json"),
    Path("corpus/schemas/public-catalog-shard-v2.schema.json"),
    Path("corpus/schemas/public-recording-shard-v2.schema.json"),
    Path("corpus/schemas/public-entity-event-graph-manifest.schema.json"),
    Path("corpus/schemas/public-entity-event-graph.schema.json"),
    Path("corpus/schemas/reviewer-admin-manifest.schema.json"),
    Path("corpus/schemas/private-solo-voice-attestation-manifest.schema.json"),
    Path("corpus/schemas/local-window-catalog-admission.schema.json"),
    Path("corpus/schemas/rendition-local-asr-admission.schema.json"),
    Path("corpus/schemas/media-local-asr-admission.schema.json"),
    Path("corpus/schemas/private-glossary-registration.schema.json"),
    Path("corpus/schemas/contextual-media-local-asr-admission.schema.json"),
    Path("corpus/schemas/archive-hint-provider-reconciliation-plan.schema.json"),
    Path("corpus/schemas/torrent-bracket-reconciliation-plan.schema.json"),
    Path("corpus/schemas/torrent-suffix-audio-reconciliation-plan.schema.json"),
    Path("corpus/schemas/torrent-selective-acquisition-plan.schema.json"),
    Path("corpus/schemas/torrent-youtube-availability-probe.schema.json"),
    Path("corpus/schemas/torrent-acquisition-audit-receipt.schema.json"),
    Path("corpus/schemas/private-acquisition-seal.schema.json"),
    Path("corpus/schemas/longform-asr-recording-input-manifest.schema.json"),
    Path("corpus/schemas/longform-asr-planning-policy.schema.json"),
    Path("corpus/schemas/longform-asr-plan.schema.json"),
)


class ContractFailure(RuntimeError):
    pass


def load_json(path: Path) -> Any:
    resolved = path if path.is_absolute() else REPOSITORY_ROOT / path
    try:
        return json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ContractFailure(f"Cannot load JSON {resolved}: {error}") from error


def json_path(error: Any) -> str:
    return "$" + "".join(
        f"[{part}]" if isinstance(part, int) else f".{part}"
        for part in error.absolute_path
    )


def dependencies():
    try:
        from jsonschema import FormatChecker
        from jsonschema.validators import Draft202012Validator
    except ImportError as error:
        raise ContractFailure(
            "The development-only JSON contract validator is missing. Run: "
            "python3 -m pip install -r scripts/requirements-json-contracts.txt"
        ) from error
    return Draft202012Validator, FormatChecker


def check_schema(schema_path: Path, schema: dict[str, Any]) -> None:
    Draft202012Validator, _ = dependencies()
    try:
        Draft202012Validator.check_schema(schema)
    except Exception as error:
        raise ContractFailure(f"Invalid Draft 2020-12 schema {schema_path}: {error}") from error


def check_exact_defs(schema_path: Path, schema: dict[str, Any]) -> None:
    definitions = schema.get("$defs")
    if not isinstance(definitions, dict):
        raise ContractFailure(f"{schema_path} must define $defs")
    failures: list[str] = []

    def walk(value: Any, pointer: str) -> None:
        if isinstance(value, dict):
            if (
                value.get("type") == "object"
                and value.get("additionalProperties") is not False
            ):
                failures.append(pointer)
            for key, child in value.items():
                walk(child, f"{pointer}/{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{pointer}/{index}")

    walk(schema, "#")
    if failures:
        raise ContractFailure(
            f"{schema_path} has object schemas without additionalProperties:false: "
            f"{', '.join(failures)}"
        )


def leaf_errors(error: Any):
    if not error.context:
        yield error
        return
    for child in error.context:
        yield from leaf_errors(child)


def validate_instance(schema_path: Path, instance_path: Path) -> None:
    Draft202012Validator, FormatChecker = dependencies()
    schema = load_json(schema_path)
    instance = load_json(instance_path)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(
        (
            leaf
            for error in validator.iter_errors(instance)
            for leaf in leaf_errors(error)
        ),
        key=lambda error: (
            tuple(str(part) for part in error.absolute_path),
            error.message,
        ),
    )
    if errors:
        lines = [
            f"{instance_path} does not satisfy {schema_path}:",
            *[f"  {json_path(error)}: {error.message}" for error in errors[:20]],
        ]
        if len(errors) > 20:
            lines.append(f"  ... {len(errors) - 20} additional errors")
        raise ContractFailure("\n".join(lines))


def validate_tracked_contracts() -> int:
    ids: dict[str, Path] = {}
    for schema_path in SCHEMA_PATHS:
        schema = load_json(schema_path)
        if not isinstance(schema, dict):
            raise ContractFailure(f"{schema_path} must contain a JSON object")
        check_schema(schema_path, schema)
        schema_id = schema.get("$id")
        if not isinstance(schema_id, str) or not schema_id:
            raise ContractFailure(f"{schema_path} requires a non-empty $id")
        if schema_id in ids:
            raise ContractFailure(
                f"Duplicate schema $id {schema_id}: {ids[schema_id]} and {schema_path}"
            )
        ids[schema_id] = schema_path
    for schema_path in STRICT_RESULT_SCHEMAS:
        check_exact_defs(schema_path, load_json(schema_path))
    for schema_path, example_path in EXAMPLES:
        validate_instance(schema_path, example_path)
    return len(SCHEMA_PATHS) + len(EXAMPLES)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--validate",
        action="append",
        nargs=2,
        metavar=("SCHEMA", "INSTANCE"),
        default=[],
        help="also validate a generated instance; repeatable",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        tracked = validate_tracked_contracts()
        for schema, instance in args.validate:
            validate_instance(Path(schema), Path(instance))
    except ContractFailure as error:
        print(str(error), file=sys.stderr)
        return 1
    print(
        f"JSON contracts passed ({tracked} tracked schema/example checks; "
        f"{len(args.validate)} generated instances)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
