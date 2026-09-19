"""Command-line interface for the corpus catalog."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .db import (
    connect,
    connect_audit_readonly,
    connect_readonly,
    migrate,
    verify_migrations,
)
from .exporter import export_release
from .asr_result_importer import (
    import_asr_whispercpp_result,
    validate_asr_whispercpp_result_file,
)
from .sharded_release import export_release_v2
from .graph_release import export_graph_release, validate_graph_release
from .importers import (
    approve_public_source_metadata,
    import_archive_url_hints,
    import_current_channel,
    import_internet_archive,
    import_legacy_manifest,
    import_snapshot_bundle,
    import_torrent_manifest,
    import_youtube_discovery_candidates,
    import_ytdlp_infos,
    load_json,
    snapshot_timestamp,
)
from .model_registry_admin import (
    import_model_registry_manifest,
    validate_model_registry_manifest,
)
from .result_importers import import_acquisition_result, import_preprocess_result
from .fingerprint_result_importer import (
    import_audio_fingerprint_compare_result,
    import_audio_fingerprint_result,
    validate_audio_fingerprint_compare_result_file,
    validate_audio_fingerprint_result_file,
)
from .visual_fingerprint_result_importer import (
    import_visual_fingerprint_compare_result,
    import_visual_fingerprint_result,
    validate_visual_fingerprint_compare_result_file,
    validate_visual_fingerprint_result_file,
)
from .sparse_frame_result_importer import (
    import_sparse_frame_result,
    validate_sparse_frame_result_file,
)
from .ocr_tesseract_result_importer import (
    import_ocr_tesseract_result,
    search_private_ocr,
    validate_ocr_tesseract_result_file,
)
from .entity_event_map_admin import (
    import_entity_event_map_manifest,
    validate_entity_event_map_manifest,
)
from .publication_admin import (
    apply_publication_manifest,
    validate_publication_manifest,
)
from .machine_transcript_publication import (
    apply_machine_transcript_publication_plan,
    build_machine_transcript_publication_plan,
)
from .reviewer_admin import (
    apply_reviewer_admin_manifest,
    validate_reviewer_admin_manifest,
)
from .solo_voice_attestation_admin import (
    apply_solo_voice_attestation_manifest,
    validate_solo_voice_attestation_manifest,
)
from .reddit_discovery_importer import (
    import_reddit_discovery_manifest,
    validate_reddit_discovery_manifest,
)
from .reddit_citation_media_handoff import (
    import_reddit_citation_media_handoff,
    materialize_reddit_citation_media_handoff,
    validate_reddit_citation_media_handoff,
)
from .archive_metadata_snapshot_importer import (
    import_archive_metadata_snapshot,
    validate_archive_metadata_snapshot,
)
from .archive_bracket_reconciler import (
    build_archive_bracket_reconciliation_plan,
    import_archive_bracket_reconciliation,
    summarize_archive_bracket_reconciliation_plan,
)
from .archive_hint_reconciler import (
    build_archive_hint_reconciliation_plan,
    import_archive_hint_reconciliation,
    summarize_archive_hint_reconciliation_plan,
)
from .torrent_bracket_reconciler import (
    build_torrent_bracket_reconciliation_plan,
    import_torrent_bracket_reconciliation,
    summarize_torrent_bracket_reconciliation_plan,
)
from .torrent_suffix_audio_planner import (
    build_torrent_suffix_audio_plan,
    summarize_torrent_suffix_audio_plan,
)
from .torrent_selective_planner import (
    build_torrent_selective_acquisition_plan,
    publish_private_torrent_selective_acquisition_plan,
    summarize_torrent_selective_acquisition_plan,
)
from .torrent_availability_probe import (
    DEFAULT_INTER_TARGET_DELAY_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    load_availability_probe_request,
    produce_availability_probe,
)
from .local_window_result_importer import (
    import_local_window_result,
    validate_local_window_result_file,
)
from .rendition_local_asr_bridge import (
    build_rendition_local_asr_admission_plan,
    import_rendition_local_asr_result,
    search_rendition_local_transcripts,
)
from .media_local_asr_bridge import (
    build_media_local_asr_admission_plan,
    import_media_local_asr_result,
    search_media_local_transcripts,
)
from .faster_whisper_gpu_v3_importer import (
    build_faster_whisper_gpu_v3_admission_plan,
    import_faster_whisper_gpu_v3_result,
)
from .contextual_media_local_asr import (
    build_contextual_media_local_asr_admission_plan,
    build_private_glossary_registration_plan,
    import_contextual_media_local_asr_result,
    import_private_glossary_registration,
)
from .media_local_transcript_projection import (
    apply_media_local_transcript_projection_plan,
    build_media_local_transcript_projection_plan,
)
from .validation import database_status, validate_database, validate_release_file
from .coverage_snapshot import compact_snapshot_summary, snapshot_catalog


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _connection(database: str):
    connection = connect(database)
    applied = migrate(connection)
    return connection, applied


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="himr-corpus")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ("init", "migrate", "status", "validate"):
        command = subparsers.add_parser(name)
        command.add_argument("--db", required=True)
    subparsers.choices["validate"].add_argument("--release")

    coverage = subparsers.add_parser(
        "coverage-snapshot",
        help="report deterministic aggregate coverage from a sealed private catalog",
    )
    coverage.add_argument("--db", required=True)
    coverage.add_argument("--expected-sha256", required=True)
    coverage.add_argument(
        "--compact",
        action="store_true",
        help="print the stable operator summary instead of the full aggregate snapshot",
    )

    validate_release = subparsers.add_parser(
        "validate-release",
        help="validate a static release without opening a private catalog",
    )
    validate_release.add_argument("--release", required=True)

    validate_graph = subparsers.add_parser(
        "validate-graph-release",
        help="validate an isolated static entity/event graph without a private catalog",
    )
    validate_graph.add_argument("--manifest", required=True)

    bundle = subparsers.add_parser("import-bundle")
    bundle.add_argument("--db", required=True)
    bundle.add_argument("--archive-dir", required=True)
    bundle.add_argument("--channel-dir", required=True)
    bundle.add_argument("--approve-public-metadata", action="store_true")

    archive = subparsers.add_parser("import-internet-archive")
    archive.add_argument("--db", required=True)
    archive.add_argument("--metadata", action="append", required=True)
    archive.add_argument("--snapshot-date", required=True)
    archive.add_argument("--observed-at")

    legacy = subparsers.add_parser("import-legacy-manifest")
    legacy.add_argument("--db", required=True)
    legacy.add_argument("--manifest", required=True)
    legacy.add_argument("--snapshot-date", required=True)
    legacy.add_argument("--observed-at")

    channel = subparsers.add_parser("import-current-channel")
    channel.add_argument("--db", required=True)
    channel.add_argument("--inventory", required=True)
    channel.add_argument("--snapshot-date", required=True)
    channel.add_argument("--observed-at")

    candidates = subparsers.add_parser("import-youtube-candidates")
    candidates.add_argument("--db", required=True)
    candidates.add_argument("--candidates", required=True)
    candidates.add_argument("--observed-at", required=True)
    candidates.add_argument("--query-label", required=True)

    ytdlp_info = subparsers.add_parser("import-ytdlp-info")
    ytdlp_info.add_argument("--db", required=True)
    ytdlp_info.add_argument(
        "--info",
        action="append",
        required=True,
        help="repeatable .info.json file or directory of *.info.json files",
    )
    ytdlp_info.add_argument("--observed-at", required=True)

    torrent = subparsers.add_parser("import-torrent-manifest")
    torrent.add_argument("--db", required=True)
    torrent.add_argument("--torrent", required=True)
    torrent.add_argument("--discovery-metadata")
    torrent.add_argument("--observed-at", required=True)

    archive_hints = subparsers.add_parser("import-archive-url-hints")
    archive_hints.add_argument("--db", required=True)
    archive_hints.add_argument("--hints", required=True)
    archive_hints.add_argument("--reddit-post-id", required=True)
    archive_hints.add_argument("--observed-at", required=True)

    reddit_discovery = subparsers.add_parser("import-reddit-rss-discovery")
    reddit_discovery.add_argument("--db", required=True)
    reddit_discovery.add_argument("--manifest", required=True)

    validate_reddit_discovery = subparsers.add_parser(
        "validate-reddit-rss-discovery"
    )
    validate_reddit_discovery.add_argument("--manifest", required=True)

    reddit_media_materialize = subparsers.add_parser(
        "materialize-reddit-citation-media-handoff"
    )
    reddit_media_materialize.add_argument("--snapshot-dir", required=True)
    reddit_media_materialize.add_argument("--out-dir", required=True)
    reddit_media_materialize.add_argument(
        "--guard-writable-inputs",
        action="store_true",
        help="explicitly accept writable frozen-snapshot files with pinned start/end verification",
    )

    reddit_media_validate = subparsers.add_parser(
        "validate-reddit-citation-media-handoff"
    )
    reddit_media_validate.add_argument("--snapshot-dir", required=True)
    reddit_media_validate.add_argument("--manifest", required=True)
    reddit_media_validate.add_argument("--guard-writable-inputs", action="store_true")

    reddit_media_import = subparsers.add_parser(
        "import-reddit-citation-media-handoff"
    )
    reddit_media_import.add_argument("--db", required=True)
    reddit_media_import.add_argument("--snapshot-dir", required=True)
    reddit_media_import.add_argument("--manifest", required=True)
    reddit_media_import.add_argument("--guard-writable-inputs", action="store_true")

    archive_snapshot = subparsers.add_parser("import-archive-metadata-snapshot")
    archive_snapshot.add_argument("--db", required=True)
    archive_snapshot.add_argument("--snapshot", required=True)

    validate_archive_snapshot = subparsers.add_parser(
        "validate-archive-metadata-snapshot"
    )
    validate_archive_snapshot.add_argument("--snapshot", required=True)

    archive_bracket_plan = subparsers.add_parser(
        "plan-archive-bracket-reconciliation"
    )
    archive_bracket_plan.add_argument("--db", required=True)
    archive_bracket_plan.add_argument("--snapshot", required=True)

    archive_bracket_import = subparsers.add_parser(
        "import-archive-bracket-reconciliation"
    )
    archive_bracket_import.add_argument("--db", required=True)
    archive_bracket_import.add_argument("--snapshot", required=True)

    archive_hint_plan = subparsers.add_parser(
        "plan-archive-hint-reconciliation"
    )
    archive_hint_plan.add_argument("--db", required=True)
    archive_hint_plan.add_argument("--original-hints", required=True)
    archive_hint_plan.add_argument("--normalized-hints", required=True)
    archive_hint_plan.add_argument("--discovery-metadata", required=True)
    archive_hint_plan.add_argument("--archive-snapshot", required=True)
    archive_hint_plan.add_argument(
        "--full",
        action="store_true",
        help="print the complete private 221-candidate plan instead of its summary",
    )

    archive_hint_import = subparsers.add_parser(
        "import-archive-hint-reconciliation"
    )
    archive_hint_import.add_argument("--db", required=True)
    archive_hint_import.add_argument("--original-hints", required=True)
    archive_hint_import.add_argument("--normalized-hints", required=True)
    archive_hint_import.add_argument("--discovery-metadata", required=True)
    archive_hint_import.add_argument("--archive-snapshot", required=True)
    archive_hint_import.add_argument(
        "--expected-plan-sha256",
        required=True,
        help="exact SHA-256 printed by a separately reviewed dry-run plan",
    )

    torrent_bracket_plan = subparsers.add_parser(
        "plan-torrent-bracket-reconciliation"
    )
    torrent_bracket_plan.add_argument("--db", required=True)
    torrent_bracket_plan.add_argument("--torrent", required=True)
    torrent_bracket_plan.add_argument("--discovery-metadata", required=True)
    torrent_bracket_plan.add_argument(
        "--full",
        action="store_true",
        help="print the complete private candidate plan instead of its summary",
    )

    torrent_bracket_import = subparsers.add_parser(
        "import-torrent-bracket-reconciliation"
    )
    torrent_bracket_import.add_argument("--db", required=True)
    torrent_bracket_import.add_argument("--torrent", required=True)
    torrent_bracket_import.add_argument("--discovery-metadata", required=True)

    torrent_suffix_audio_plan = subparsers.add_parser(
        "plan-torrent-suffix-audio-reconciliation"
    )
    torrent_suffix_audio_plan.add_argument("--db", required=True)
    torrent_suffix_audio_plan.add_argument("--torrent", required=True)
    torrent_suffix_audio_plan.add_argument("--discovery-metadata", required=True)
    torrent_suffix_audio_plan.add_argument(
        "--full",
        action="store_true",
        help="print the complete private plan, including raw manifest paths",
    )

    torrent_selective_plan = subparsers.add_parser(
        "plan-torrent-selective-acquisition",
        help=(
            "choose smallest torrent renditions after exact archive/catalogue "
            "reconciliation; never starts a torrent client"
        ),
    )
    torrent_selective_plan.add_argument("--db", required=True)
    torrent_selective_plan.add_argument("--torrent", required=True)
    torrent_selective_plan.add_argument("--discovery-metadata", required=True)
    torrent_selective_plan.add_argument(
        "--archive-snapshot",
        action="append",
        required=True,
        help="repeat for each sealed Archive.org metadata snapshot",
    )
    torrent_selective_plan.add_argument(
        "--availability-probe",
        help="optional strict credential-free yt-dlp no-download probe result",
    )
    torrent_selective_plan.add_argument(
        "--full",
        action="store_true",
        help=(
            "print private paths, probe targets, and selected file indices; "
            "the default summary is path-free"
        ),
    )
    torrent_selective_plan.add_argument(
        "--output",
        help=(
            "atomically write the full private plan to this absolute new path; "
            "requires --full and keeps private paths out of stdout"
        ),
    )

    torrent_availability_probe = subparsers.add_parser(
        "produce-torrent-youtube-availability-probe",
        help=(
            "atomically produce the planner's ordered credential-free yt-dlp "
            "no-download result"
        ),
    )
    torrent_availability_probe.add_argument(
        "--input",
        help="full selective plan JSON or standalone availability_probe_request JSON",
    )
    torrent_availability_probe.add_argument(
        "--db", help="sealed audit catalogue used to derive the request in memory"
    )
    torrent_availability_probe.add_argument("--torrent")
    torrent_availability_probe.add_argument("--discovery-metadata")
    torrent_availability_probe.add_argument(
        "--archive-snapshot",
        action="append",
        help="repeat for each snapshot when deriving the request without --input",
    )
    torrent_availability_probe.add_argument("--yt-dlp-executable", required=True)
    torrent_availability_probe.add_argument("--yt-dlp-sha256", required=True)
    torrent_availability_probe.add_argument("--yt-dlp-version", required=True)
    torrent_availability_probe.add_argument(
        "--checkpoint", required=True, help="absolute owner-only resumable checkpoint path"
    )
    torrent_availability_probe.add_argument(
        "--output", required=True, help="absolute atomic final probe-result path"
    )
    torrent_availability_probe.add_argument(
        "--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS
    )
    torrent_availability_probe.add_argument(
        "--inter-target-delay-seconds",
        type=float,
        default=DEFAULT_INTER_TARGET_DELAY_SECONDS,
    )

    acquisition_result = subparsers.add_parser("import-acquisition-result")
    acquisition_result.add_argument("--db", required=True)
    acquisition_result.add_argument("--result", required=True)
    acquisition_result.add_argument(
        "--private-artifact-root",
        help="root for portable paths in a policy-bearing private seal receipt",
    )
    acquisition_result.add_argument(
        "--private-seal-receipt",
        help="validated owner-only seal receipt required by policy-bearing results",
    )

    preprocess_result = subparsers.add_parser("import-preprocess-result")
    preprocess_result.add_argument("--db", required=True)
    preprocess_result.add_argument("--result", required=True)

    local_window_result = subparsers.add_parser("import-local-window-result")
    local_window_result.add_argument("--db", required=True)
    local_window_result.add_argument("--result", required=True)
    local_window_result.add_argument(
        "--observed-at",
        required=True,
        help="catalog admission observation time; not an extraction timestamp",
    )

    validate_local_window_result = subparsers.add_parser(
        "validate-local-window-result"
    )
    validate_local_window_result.add_argument("--db", required=True)
    validate_local_window_result.add_argument("--result", required=True)
    validate_local_window_result.add_argument(
        "--observed-at",
        required=True,
        help="proposed catalog admission observation time; no rows are written",
    )

    asr_result = subparsers.add_parser("import-asr-whispercpp-result")
    asr_result.add_argument("--db", required=True)
    asr_result.add_argument("--result", required=True)

    validate_asr_result = subparsers.add_parser("validate-asr-whispercpp-result")
    validate_asr_result.add_argument("--result", required=True)

    local_asr_plan = subparsers.add_parser(
        "plan-rendition-local-asr-admission",
        help="validate one local-window ASR result and print a text-free private plan",
    )
    local_asr_plan.add_argument("--db", required=True)
    local_asr_plan.add_argument("--result", required=True)

    local_asr_import = subparsers.add_parser(
        "import-rendition-local-asr-result",
        help="admit one separately reviewed plan to the private rendition-local lane",
    )
    local_asr_import.add_argument("--db", required=True)
    local_asr_import.add_argument("--result", required=True)
    local_asr_import.add_argument("--expected-plan-sha256", required=True)

    local_asr_search = subparsers.add_parser(
        "search-rendition-local-transcripts",
        help="search private rendition-local transcript text and retain local coordinates",
    )
    local_asr_search.add_argument("--db", required=True)
    local_asr_search.add_argument("--query", required=True)
    local_asr_search.add_argument("--limit", type=int, default=25)
    local_asr_search.add_argument("--recording-id")

    media_asr_plan = subparsers.add_parser(
        "plan-media-local-asr-admission",
        help="validate one sealed catalog-free ASR result and print a text-free plan",
    )
    media_asr_plan.add_argument("--db", required=True)
    media_asr_plan.add_argument("--result", required=True)
    media_asr_plan.add_argument("--queue-manifest", required=True)

    media_asr_import = subparsers.add_parser(
        "import-media-local-asr-result",
        help="admit one reviewed plan to the private media-local transcript lane",
    )
    media_asr_import.add_argument("--db", required=True)
    media_asr_import.add_argument("--result", required=True)
    media_asr_import.add_argument("--queue-manifest", required=True)
    media_asr_import.add_argument("--expected-plan-sha256", required=True)

    gpu_v3_plan = subparsers.add_parser(
        "plan-faster-whisper-gpu-v3-admission",
        help="validate one sealed GPU v3 result and print a text-free private plan",
    )
    gpu_v3_plan.add_argument("--db", required=True)
    gpu_v3_plan.add_argument("--result", required=True)
    gpu_v3_plan.add_argument("--work-order", required=True)
    gpu_v3_plan.add_argument(
        "--batch-completion",
        help="sealed resident-batch completion receipt, when the result was batched",
    )

    gpu_v3_import = subparsers.add_parser(
        "import-faster-whisper-gpu-v3-result",
        help="admit one reviewed GPU v3 plan to the private media-local lane",
    )
    gpu_v3_import.add_argument("--db", required=True)
    gpu_v3_import.add_argument("--result", required=True)
    gpu_v3_import.add_argument("--work-order", required=True)
    gpu_v3_import.add_argument("--expected-plan-sha256", required=True)
    gpu_v3_import.add_argument(
        "--batch-completion",
        help="sealed resident-batch completion receipt, when the result was batched",
    )

    media_asr_search = subparsers.add_parser(
        "search-media-local-transcripts",
        help="search private media-local text without recording/source coordinates",
    )
    media_asr_search.add_argument("--db", required=True)
    media_asr_search.add_argument("--query", required=True)
    media_asr_search.add_argument("--limit", type=int, default=25)
    media_asr_search.add_argument("--media-id")

    media_projection_plan = subparsers.add_parser(
        "plan-media-local-transcript-projection",
        help="plan an exact full-file identity projection into recording coordinates",
    )
    media_projection_plan.add_argument("--db", required=True)
    media_projection_plan.add_argument("--manifest", required=True)

    media_projection_apply = subparsers.add_parser(
        "apply-media-local-transcript-projection",
        help="apply one separately reviewed digest-bound identity projection plan",
    )
    media_projection_apply.add_argument("--db", required=True)
    media_projection_apply.add_argument("--manifest", required=True)
    media_projection_apply.add_argument("--expected-plan-sha256", required=True)

    glossary_plan = subparsers.add_parser(
        "plan-private-glossary-registration",
        help="print a term-free plan for the closed private neutral glossary",
    )
    glossary_plan.add_argument("--glossary", required=True)
    glossary_plan.add_argument("--observed-at", required=True)

    glossary_import = subparsers.add_parser(
        "import-private-glossary-registration",
        help="register the exact neutral glossary bytes without storing its terms",
    )
    glossary_import.add_argument("--db", required=True)
    glossary_import.add_argument("--glossary", required=True)
    glossary_import.add_argument("--observed-at", required=True)
    glossary_import.add_argument("--expected-plan-sha256", required=True)

    contextual_media_plan = subparsers.add_parser(
        "plan-contextual-media-local-asr-admission",
        help="validate a paired contextual result and text-private diff",
    )
    contextual_media_plan.add_argument("--db", required=True)
    contextual_media_plan.add_argument("--result", required=True)
    contextual_media_plan.add_argument("--batch-manifest", required=True)
    contextual_media_plan.add_argument("--diff", required=True)

    contextual_media_import = subparsers.add_parser(
        "import-contextual-media-local-asr-result",
        help="digest-authorize a contextual result as a competing private revision",
    )
    contextual_media_import.add_argument("--db", required=True)
    contextual_media_import.add_argument("--result", required=True)
    contextual_media_import.add_argument("--batch-manifest", required=True)
    contextual_media_import.add_argument("--diff", required=True)
    contextual_media_import.add_argument("--expected-plan-sha256", required=True)

    fingerprint_result = subparsers.add_parser("import-audio-fingerprint-result")
    fingerprint_result.add_argument("--db", required=True)
    fingerprint_result.add_argument("--result", required=True)

    validate_fingerprint_result = subparsers.add_parser(
        "validate-audio-fingerprint-result"
    )
    validate_fingerprint_result.add_argument("--result", required=True)

    fingerprint_compare = subparsers.add_parser(
        "import-audio-fingerprint-compare-result"
    )
    fingerprint_compare.add_argument("--db", required=True)
    fingerprint_compare.add_argument("--result", required=True)

    validate_fingerprint_compare = subparsers.add_parser(
        "validate-audio-fingerprint-compare-result"
    )
    validate_fingerprint_compare.add_argument("--result", required=True)

    visual_fingerprint_result = subparsers.add_parser(
        "import-visual-fingerprint-result"
    )
    visual_fingerprint_result.add_argument("--db", required=True)
    visual_fingerprint_result.add_argument("--result", required=True)

    validate_visual_fingerprint_result = subparsers.add_parser(
        "validate-visual-fingerprint-result"
    )
    validate_visual_fingerprint_result.add_argument("--result", required=True)

    visual_fingerprint_compare = subparsers.add_parser(
        "import-visual-fingerprint-compare-result"
    )
    visual_fingerprint_compare.add_argument("--db", required=True)
    visual_fingerprint_compare.add_argument("--result", required=True)

    validate_visual_fingerprint_compare = subparsers.add_parser(
        "validate-visual-fingerprint-compare-result"
    )
    validate_visual_fingerprint_compare.add_argument("--result", required=True)

    sparse_frame_result = subparsers.add_parser("import-sparse-frame-result")
    sparse_frame_result.add_argument("--db", required=True)
    sparse_frame_result.add_argument("--result", required=True)

    validate_sparse_frame_result = subparsers.add_parser(
        "validate-sparse-frame-result"
    )
    validate_sparse_frame_result.add_argument("--result", required=True)

    validate_ocr_result = subparsers.add_parser(
        "validate-ocr-tesseract-result",
        help="replay one completed private OCR envelope and all current local files",
    )
    validate_ocr_result.add_argument("--result", required=True)

    import_ocr_result = subparsers.add_parser(
        "import-ocr-tesseract-result",
        help="admit one digest-reviewed result to the private redaction-pending OCR lane",
    )
    import_ocr_result.add_argument("--db", required=True)
    import_ocr_result.add_argument("--result", required=True)
    import_ocr_result.add_argument("--expected-result-sha256", required=True)

    search_ocr = subparsers.add_parser(
        "search-private-ocr",
        help="search raw private machine OCR with exact rendition-media coordinates",
    )
    search_ocr.add_argument("--db", required=True)
    search_ocr.add_argument("--query", required=True)
    search_ocr.add_argument("--limit", type=int, default=25)
    search_ocr.add_argument("--source-id")
    search_ocr.add_argument("--recording-id")
    search_ocr.add_argument("--rendition-id")

    validate_models = subparsers.add_parser("validate-model-registry-manifest")
    validate_models.add_argument("--manifest", required=True)

    import_models = subparsers.add_parser("import-model-registry-manifest")
    import_models.add_argument("--db", required=True)
    import_models.add_argument("--manifest", required=True)

    validate_publication = subparsers.add_parser("validate-publication-manifest")
    validate_publication.add_argument("--db", required=True)
    validate_publication.add_argument("--manifest", required=True)

    import_publication = subparsers.add_parser("import-publication-manifest")
    import_publication.add_argument("--db", required=True)
    import_publication.add_argument("--manifest", required=True)
    import_publication.add_argument(
        "--dry-run",
        action="store_true",
        help="run all admission checks without inserting any decisions",
    )

    plan_machine_transcripts = subparsers.add_parser(
        "plan-machine-transcript-publication",
        help="print the text-free closed-policy scope and authorization digest",
    )
    plan_machine_transcripts.add_argument("--db", required=True)

    apply_machine_transcripts = subparsers.add_parser(
        "apply-machine-transcript-publication",
        help="publish the digest-authorized initial machine-transcript scope",
    )
    apply_machine_transcripts.add_argument("--db", required=True)
    apply_machine_transcripts.add_argument("--expected-plan-sha256", required=True)

    validate_reviewers = subparsers.add_parser(
        "validate-reviewer-admin-manifest",
        help="validate reviewer operations through a non-mutating audit connection",
    )
    validate_reviewers.add_argument("--db", required=True)
    validate_reviewers.add_argument("--manifest", required=True)

    import_reviewers = subparsers.add_parser(
        "import-reviewer-admin-manifest",
        help="dry-run reviewer operations unless --apply is explicitly supplied",
    )
    import_reviewers.add_argument("--db", required=True)
    import_reviewers.add_argument("--manifest", required=True)
    import_reviewers.add_argument(
        "--apply",
        action="store_true",
        help="atomically register reviewers and append active-state events",
    )

    validate_map = subparsers.add_parser("validate-entity-event-map-manifest")
    validate_map.add_argument("--db", required=True)
    validate_map.add_argument("--manifest", required=True)

    import_map = subparsers.add_parser("import-entity-event-map-manifest")
    import_map.add_argument("--db", required=True)
    import_map.add_argument("--manifest", required=True)

    validate_solo_voice = subparsers.add_parser(
        "validate-solo-voice-attestation-manifest",
        help="validate one private human named-voice manifest without writing",
    )
    validate_solo_voice.add_argument("--db", required=True)
    validate_solo_voice.add_argument("--manifest", required=True)

    import_solo_voice = subparsers.add_parser(
        "import-solo-voice-attestation-manifest",
        help="dry-run a private named-voice manifest unless --apply is supplied",
    )
    import_solo_voice.add_argument("--db", required=True)
    import_solo_voice.add_argument("--manifest", required=True)
    import_solo_voice.add_argument(
        "--apply",
        action="store_true",
        help="atomically append the independently reviewed private decisions",
    )
    import_solo_voice.add_argument(
        "--expected-input-sha256",
        help="exact input_sha256 returned by validation; required with --apply",
    )

    approve = subparsers.add_parser("approve-public-metadata")
    approve.add_argument("--db", required=True)

    export = subparsers.add_parser("export")
    export.add_argument("--db", required=True)
    export.add_argument("--out", required=True)

    export_sharded = subparsers.add_parser(
        "export-sharded",
        help="atomically export a v2 manifest and integrity-checked static shards",
    )
    export_sharded.add_argument("--db", required=True)
    export_sharded.add_argument("--out-dir", required=True)
    export_sharded.add_argument("--catalog-shard-size", type=int, default=250)

    export_graph = subparsers.add_parser(
        "export-graph",
        help="atomically export the independently reviewed public entity/event graph",
    )
    export_graph.add_argument("--db", required=True)
    export_graph.add_argument("--out-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "coverage-snapshot":
        snapshot = snapshot_catalog(
            args.db,
            expected_sha256=args.expected_sha256,
        )
        _print(compact_snapshot_summary(snapshot) if args.compact else snapshot)
        return
    if args.command in {"status", "validate"}:
        connection = connect_audit_readonly(args.db)
        try:
            if args.command == "status":
                _print(database_status(connection))
            else:
                result = {"database": validate_database(connection)}
                if args.release:
                    result["release"] = validate_release_file(args.release)
                _print(result)
        finally:
            connection.close()
        return
    if args.command == "validate-release":
        _print(validate_release_file(args.release))
        return
    if args.command == "validate-graph-release":
        _print(validate_graph_release(args.manifest))
        return
    if args.command == "validate-ocr-tesseract-result":
        _print(validate_ocr_tesseract_result_file(Path(args.result)))
        return
    if args.command == "search-private-ocr":
        connection = connect_audit_readonly(args.db)
        try:
            verify_migrations(connection)
            _print(
                search_private_ocr(
                    connection,
                    args.query,
                    limit=args.limit,
                    source_id=args.source_id,
                    recording_id=args.recording_id,
                    rendition_id=args.rendition_id,
                )
            )
        finally:
            connection.close()
        return
    if args.command == "import-ocr-tesseract-result":
        # OCR text is private authority, never schema authority. Refuse to install
        # a pending migration as a side effect of an import command.
        audit_connection = connect_audit_readonly(args.db)
        try:
            verify_migrations(audit_connection)
        finally:
            audit_connection.close()
        connection = connect(args.db)
        try:
            verify_migrations(connection)
            _print(
                import_ocr_tesseract_result(
                    connection,
                    Path(args.result),
                    expected_result_sha256=args.expected_result_sha256,
                )
            )
        finally:
            connection.close()
        return
    if args.command == "validate-reviewer-admin-manifest" or (
        args.command == "import-reviewer-admin-manifest" and not args.apply
    ):
        connection = connect_audit_readonly(args.db)
        try:
            verify_migrations(connection)
            _print(validate_reviewer_admin_manifest(connection, Path(args.manifest)))
        finally:
            connection.close()
        return
    if args.command == "validate-solo-voice-attestation-manifest" or (
        args.command == "import-solo-voice-attestation-manifest" and not args.apply
    ):
        connection = connect_audit_readonly(args.db)
        try:
            verify_migrations(connection)
            _print(
                validate_solo_voice_attestation_manifest(
                    connection, Path(args.manifest)
                )
            )
        finally:
            connection.close()
        return
    if (
        args.command == "import-solo-voice-attestation-manifest"
        and args.apply
    ):
        if args.expected_input_sha256 is None:
            parser.error("--expected-input-sha256 is required with --apply")
        # A named voice decision grants private identity authority, not schema
        # authority. Require an explicit, separately reviewed migration first.
        audit_connection = connect_audit_readonly(args.db)
        try:
            verify_migrations(audit_connection)
        finally:
            audit_connection.close()
        connection = connect(args.db)
        try:
            verify_migrations(connection)
            _print(
                apply_solo_voice_attestation_manifest(
                    connection,
                    Path(args.manifest),
                    dry_run=False,
                    expected_input_sha256=args.expected_input_sha256,
                )
            )
        finally:
            connection.close()
        return
    if args.command == "validate-publication-manifest" or (
        args.command == "import-publication-manifest" and args.dry_run
    ):
        connection = connect_audit_readonly(args.db)
        try:
            verify_migrations(connection)
            _print(validate_publication_manifest(connection, Path(args.manifest)))
        finally:
            connection.close()
        return
    if args.command == "plan-machine-transcript-publication":
        connection = connect_audit_readonly(args.db)
        try:
            verify_migrations(connection)
            _print(build_machine_transcript_publication_plan(connection))
        finally:
            connection.close()
        return
    if args.command == "apply-machine-transcript-publication":
        # This command grants publication authority, not schema authority. Refuse
        # pending migrations before opening a writer; an administrator must run the
        # explicit migrate command and review the resulting schema first.
        audit_connection = connect_audit_readonly(args.db)
        try:
            verify_migrations(audit_connection)
        finally:
            audit_connection.close()
        connection = connect(args.db)
        try:
            verify_migrations(connection)
            _print(
                apply_machine_transcript_publication_plan(
                    connection,
                    expected_plan_sha256=args.expected_plan_sha256,
                )
            )
        finally:
            connection.close()
        return
    if args.command == "validate-model-registry-manifest":
        manifest = validate_model_registry_manifest(Path(args.manifest))
        _print(
            {
                "manifest_id": manifest["manifest_id"],
                "input_sha256": manifest["input_sha256"],
                "model_count": len(manifest["models"]),
                "model_ids": [model["model_id"] for model in manifest["models"]],
            }
        )
        return
    if args.command == "validate-asr-whispercpp-result":
        _print(validate_asr_whispercpp_result_file(Path(args.result)))
        return
    if args.command == "plan-private-glossary-registration":
        _print(
            build_private_glossary_registration_plan(
                Path(args.glossary), observed_at=args.observed_at
            )
        )
        return
    if args.command == "validate-audio-fingerprint-result":
        result = validate_audio_fingerprint_result_file(Path(args.result))
        _print(
            {
                "valid": True,
                "recipe_id": result["recipe_id"],
                "processing_run_id": result["processing_run"]["processing_run_id"],
                "fingerprint_count": len(result["fingerprints"]),
                "catalog_context": result["catalog_context"],
            }
        )
        return
    if args.command == "validate-audio-fingerprint-compare-result":
        result = validate_audio_fingerprint_compare_result_file(Path(args.result))
        _print(
            {
                "valid": True,
                "recipe_id": result["recipe_id"],
                "processing_run_id": result["processing_run"]["processing_run_id"],
                "match_candidate_id": result["comparison"]["match_candidate_id"],
                "catalog_context": result["catalog_context"],
            }
        )
        return
    if args.command == "validate-visual-fingerprint-result":
        result = validate_visual_fingerprint_result_file(Path(args.result))
        _print(
            {
                "valid": True,
                "recipe_id": result["recipe_id"],
                "processing_run_id": result["processing_run"]["processing_run_id"],
                "fingerprint_count": len(result["frames"]),
                "catalog_context": result["catalog_context"],
                "calibration_state": "not_calibrated",
                "requires_human_review": True,
            }
        )
        return
    if args.command == "validate-visual-fingerprint-compare-result":
        result = validate_visual_fingerprint_compare_result_file(Path(args.result))
        _print(
            {
                "valid": True,
                "recipe_id": result["recipe_id"],
                "processing_run_id": result["processing_run"]["processing_run_id"],
                "comparison_id": result["comparison"]["match_candidate_id"],
                "candidate_emitted": result["comparison"]["candidate_emitted"],
                "decision_state": result["comparison"]["decision_state"],
                "catalog_context": result["catalog_context"],
                "calibration_state": "not_calibrated",
                "calibrated_probability": None,
                "requires_human_review": True,
            }
        )
        return
    if args.command == "validate-sparse-frame-result":
        _print(validate_sparse_frame_result_file(Path(args.result)))
        return
    if args.command == "validate-reddit-rss-discovery":
        manifest = validate_reddit_discovery_manifest(Path(args.manifest))
        _print(
            {
                "valid": True,
                "discovery_id": manifest["discovery_id"],
                "post_count": len(manifest["posts"]),
                "publication_authority": False,
            }
        )
        return
    if args.command == "materialize-reddit-citation-media-handoff":
        _print(
            materialize_reddit_citation_media_handoff(
                Path(args.snapshot_dir),
                Path(args.out_dir),
                guard_writable_inputs=args.guard_writable_inputs,
            )
        )
        return
    if args.command == "validate-reddit-citation-media-handoff":
        _print(
            validate_reddit_citation_media_handoff(
                Path(args.snapshot_dir),
                Path(args.manifest),
                guard_writable_inputs=args.guard_writable_inputs,
            )
        )
        return
    if args.command == "validate-archive-metadata-snapshot":
        snapshot = validate_archive_metadata_snapshot(Path(args.snapshot))
        _print(
            {
                "valid": True,
                "snapshot_id": snapshot["snapshot_id"],
                "snapshot_sha256": snapshot["_sha256"],
                "request_id": snapshot["request"]["request_id"],
                "observed_at": snapshot["observed_at"],
                "items": len(snapshot["items"]),
                "media_downloads": 0,
                "publication_authority": False,
                "identity_assertions": False,
            }
        )
        return
    if args.command == "plan-archive-bracket-reconciliation":
        connection = connect_readonly(args.db)
        try:
            _print(
                summarize_archive_bracket_reconciliation_plan(
                    build_archive_bracket_reconciliation_plan(
                        connection, Path(args.snapshot)
                    )
                )
            )
        finally:
            connection.close()
        return
    if args.command == "plan-archive-hint-reconciliation":
        connection = connect_readonly(args.db)
        try:
            plan = build_archive_hint_reconciliation_plan(
                connection,
                Path(args.original_hints),
                Path(args.normalized_hints),
                Path(args.discovery_metadata),
                Path(args.archive_snapshot),
            )
            _print(
                plan
                if args.full
                else summarize_archive_hint_reconciliation_plan(plan)
            )
        finally:
            connection.close()
        return
    if args.command == "plan-torrent-bracket-reconciliation":
        connection = connect_readonly(args.db)
        try:
            plan = build_torrent_bracket_reconciliation_plan(
                connection,
                Path(args.torrent),
                Path(args.discovery_metadata),
            )
            _print(
                plan
                if args.full
                else summarize_torrent_bracket_reconciliation_plan(plan)
            )
        finally:
            connection.close()
        return
    if args.command == "plan-torrent-suffix-audio-reconciliation":
        connection = connect_readonly(args.db)
        try:
            plan = build_torrent_suffix_audio_plan(
                connection,
                Path(args.torrent),
                Path(args.discovery_metadata),
            )
            _print(plan if args.full else summarize_torrent_suffix_audio_plan(plan))
        finally:
            connection.close()
        return
    if args.command == "plan-torrent-selective-acquisition":
        connection = connect_audit_readonly(args.db)
        try:
            plan = build_torrent_selective_acquisition_plan(
                connection,
                Path(args.torrent),
                Path(args.discovery_metadata),
                [Path(value) for value in args.archive_snapshot],
                availability_probe_path=(
                    Path(args.availability_probe) if args.availability_probe else None
                ),
            )
            if args.output and not args.full:
                parser.error("--output requires --full")
            if args.output:
                _print(
                    publish_private_torrent_selective_acquisition_plan(
                        plan, Path(args.output)
                    )
                )
            else:
                _print(
                    plan
                    if args.full
                    else summarize_torrent_selective_acquisition_plan(plan)
                )
        finally:
            connection.close()
        return
    if args.command == "produce-torrent-youtube-availability-probe":
        planner_inputs = (
            args.db,
            args.torrent,
            args.discovery_metadata,
            args.archive_snapshot,
        )
        if args.input:
            if any(value for value in planner_inputs):
                parser.error(
                    "--input cannot be combined with --db, --torrent, "
                    "--discovery-metadata, or --archive-snapshot"
                )
            request = load_availability_probe_request(Path(args.input))
        else:
            if not all(
                (args.db, args.torrent, args.discovery_metadata, args.archive_snapshot)
            ):
                parser.error(
                    "provide --input or all of --db, --torrent, "
                    "--discovery-metadata, and --archive-snapshot"
                )
            connection = connect_audit_readonly(args.db)
            try:
                plan = build_torrent_selective_acquisition_plan(
                    connection,
                    Path(args.torrent),
                    Path(args.discovery_metadata),
                    [Path(value) for value in args.archive_snapshot],
                )
                request = plan["availability_probe_request"]
            finally:
                connection.close()
        _print(
            produce_availability_probe(
                request,
                yt_dlp_executable=Path(args.yt_dlp_executable),
                expected_executable_sha256=args.yt_dlp_sha256,
                expected_version=args.yt_dlp_version,
                checkpoint_path=Path(args.checkpoint),
                output_path=Path(args.output),
                timeout_seconds=args.timeout_seconds,
                inter_target_delay_seconds=args.inter_target_delay_seconds,
            )
        )
        return
    if args.command == "validate-local-window-result":
        connection = connect_readonly(args.db)
        try:
            _print(
                validate_local_window_result_file(
                    connection,
                    Path(args.result),
                    observed_at=args.observed_at,
                )
            )
        finally:
            connection.close()
        return
    if args.command == "plan-rendition-local-asr-admission":
        connection = connect_readonly(args.db)
        try:
            _print(
                build_rendition_local_asr_admission_plan(
                    connection, Path(args.result)
                )
            )
        finally:
            connection.close()
        return
    if args.command == "search-rendition-local-transcripts":
        connection = connect_readonly(args.db)
        try:
            _print(
                search_rendition_local_transcripts(
                    connection,
                    args.query,
                    limit=args.limit,
                    recording_id=args.recording_id,
                )
            )
        finally:
            connection.close()
        return
    if args.command == "plan-media-local-asr-admission":
        connection = connect_readonly(args.db)
        try:
            _print(
                build_media_local_asr_admission_plan(
                    connection, Path(args.result), Path(args.queue_manifest)
                )
            )
        finally:
            connection.close()
        return
    if args.command == "plan-faster-whisper-gpu-v3-admission":
        connection = connect_readonly(args.db)
        try:
            _print(
                build_faster_whisper_gpu_v3_admission_plan(
                    connection,
                    Path(args.result),
                    Path(args.work_order),
                    batch_completion_path=(
                        Path(args.batch_completion) if args.batch_completion else None
                    ),
                )
            )
        finally:
            connection.close()
        return
    if args.command == "plan-contextual-media-local-asr-admission":
        connection = connect_readonly(args.db)
        try:
            _print(
                build_contextual_media_local_asr_admission_plan(
                    connection,
                    Path(args.result),
                    Path(args.batch_manifest),
                    Path(args.diff),
                )
            )
        finally:
            connection.close()
        return
    if args.command == "search-media-local-transcripts":
        connection = connect_readonly(args.db)
        try:
            _print(
                search_media_local_transcripts(
                    connection,
                    args.query,
                    limit=args.limit,
                    media_id=args.media_id,
                )
            )
        finally:
            connection.close()
        return
    if args.command == "plan-media-local-transcript-projection":
        connection = connect_readonly(args.db)
        try:
            _print(
                build_media_local_transcript_projection_plan(
                    connection, Path(args.manifest)
                )
            )
        finally:
            connection.close()
        return
    connection, applied = _connection(args.db)
    try:
        if args.command in {"init", "migrate"}:
            _print({"database": args.db, "applied_migrations": applied})
        elif args.command == "import-bundle":
            _print(
                import_snapshot_bundle(
                    connection,
                    Path(args.archive_dir),
                    Path(args.channel_dir),
                    approve_public_metadata=args.approve_public_metadata,
                )
            )
        elif args.command == "import-internet-archive":
            _print(
                import_internet_archive(
                    connection,
                    [Path(value) for value in args.metadata],
                    snapshot_date=args.snapshot_date,
                    observed_at=snapshot_timestamp(args.snapshot_date, args.observed_at),
                )
            )
        elif args.command == "import-legacy-manifest":
            _print(
                import_legacy_manifest(
                    connection,
                    Path(args.manifest),
                    snapshot_date=args.snapshot_date,
                    observed_at=snapshot_timestamp(args.snapshot_date, args.observed_at),
                )
            )
        elif args.command == "import-current-channel":
            _print(
                import_current_channel(
                    connection,
                    Path(args.inventory),
                    snapshot_date=args.snapshot_date,
                    observed_at=snapshot_timestamp(args.snapshot_date, args.observed_at),
                )
            )
        elif args.command == "import-youtube-candidates":
            _print(
                import_youtube_discovery_candidates(
                    connection,
                    Path(args.candidates),
                    observed_at=args.observed_at,
                    query_label=args.query_label,
                )
            )
        elif args.command == "import-ytdlp-info":
            _print(
                import_ytdlp_infos(
                    connection,
                    [Path(value) for value in args.info],
                    observed_at=args.observed_at,
                )
            )
        elif args.command == "import-torrent-manifest":
            _print(
                import_torrent_manifest(
                    connection,
                    Path(args.torrent),
                    observed_at=args.observed_at,
                    discovery_metadata_path=(
                        Path(args.discovery_metadata) if args.discovery_metadata else None
                    ),
                )
            )
        elif args.command == "import-archive-url-hints":
            _print(
                import_archive_url_hints(
                    connection,
                    Path(args.hints),
                    observed_at=args.observed_at,
                    reddit_post_id=args.reddit_post_id,
                )
            )
        elif args.command == "import-reddit-rss-discovery":
            _print(
                import_reddit_discovery_manifest(
                    connection, Path(args.manifest)
                )
            )
        elif args.command == "import-reddit-citation-media-handoff":
            _print(
                import_reddit_citation_media_handoff(
                    connection,
                    Path(args.snapshot_dir),
                    Path(args.manifest),
                    guard_writable_inputs=args.guard_writable_inputs,
                )
            )
        elif args.command == "import-archive-metadata-snapshot":
            _print(
                import_archive_metadata_snapshot(
                    connection, Path(args.snapshot)
                )
            )
        elif args.command == "import-archive-bracket-reconciliation":
            _print(
                import_archive_bracket_reconciliation(
                    connection, Path(args.snapshot)
                )
            )
        elif args.command == "import-archive-hint-reconciliation":
            _print(
                import_archive_hint_reconciliation(
                    connection,
                    Path(args.original_hints),
                    Path(args.normalized_hints),
                    Path(args.discovery_metadata),
                    Path(args.archive_snapshot),
                    expected_plan_sha256=args.expected_plan_sha256,
                )
            )
        elif args.command == "import-torrent-bracket-reconciliation":
            _print(
                import_torrent_bracket_reconciliation(
                    connection,
                    Path(args.torrent),
                    Path(args.discovery_metadata),
                )
            )
        elif args.command == "import-acquisition-result":
            _print(
                import_acquisition_result(
                    connection,
                    Path(args.result),
                    private_artifact_root=(
                        Path(args.private_artifact_root)
                        if args.private_artifact_root
                        else None
                    ),
                    private_seal_receipt_path=(
                        Path(args.private_seal_receipt)
                        if args.private_seal_receipt
                        else None
                    ),
                )
            )
        elif args.command == "import-preprocess-result":
            _print(import_preprocess_result(connection, Path(args.result)))
        elif args.command == "import-local-window-result":
            _print(
                import_local_window_result(
                    connection,
                    Path(args.result),
                    observed_at=args.observed_at,
                )
            )
        elif args.command == "import-asr-whispercpp-result":
            _print(import_asr_whispercpp_result(connection, Path(args.result)))
        elif args.command == "import-rendition-local-asr-result":
            _print(
                import_rendition_local_asr_result(
                    connection,
                    Path(args.result),
                    expected_plan_sha256=args.expected_plan_sha256,
                )
            )
        elif args.command == "import-media-local-asr-result":
            _print(
                import_media_local_asr_result(
                    connection,
                    Path(args.result),
                    Path(args.queue_manifest),
                    expected_plan_sha256=args.expected_plan_sha256,
                )
            )
        elif args.command == "import-faster-whisper-gpu-v3-result":
            _print(
                import_faster_whisper_gpu_v3_result(
                    connection,
                    Path(args.result),
                    Path(args.work_order),
                    expected_plan_sha256=args.expected_plan_sha256,
                    batch_completion_path=(
                        Path(args.batch_completion) if args.batch_completion else None
                    ),
                )
            )
        elif args.command == "import-private-glossary-registration":
            _print(
                import_private_glossary_registration(
                    connection,
                    Path(args.glossary),
                    observed_at=args.observed_at,
                    expected_plan_sha256=args.expected_plan_sha256,
                )
            )
        elif args.command == "import-contextual-media-local-asr-result":
            _print(
                import_contextual_media_local_asr_result(
                    connection,
                    Path(args.result),
                    Path(args.batch_manifest),
                    Path(args.diff),
                    expected_plan_sha256=args.expected_plan_sha256,
                )
            )
        elif args.command == "apply-media-local-transcript-projection":
            _print(
                apply_media_local_transcript_projection_plan(
                    connection,
                    Path(args.manifest),
                    expected_plan_sha256=args.expected_plan_sha256,
                )
            )
        elif args.command == "import-audio-fingerprint-result":
            _print(import_audio_fingerprint_result(connection, Path(args.result)))
        elif args.command == "import-audio-fingerprint-compare-result":
            _print(import_audio_fingerprint_compare_result(connection, Path(args.result)))
        elif args.command == "import-visual-fingerprint-result":
            _print(import_visual_fingerprint_result(connection, Path(args.result)))
        elif args.command == "import-visual-fingerprint-compare-result":
            _print(
                import_visual_fingerprint_compare_result(
                    connection, Path(args.result)
                )
            )
        elif args.command == "import-sparse-frame-result":
            _print(import_sparse_frame_result(connection, Path(args.result)))
        elif args.command == "import-model-registry-manifest":
            _print(import_model_registry_manifest(connection, Path(args.manifest)))
        elif args.command == "import-publication-manifest":
            _print(
                apply_publication_manifest(
                    connection, Path(args.manifest), dry_run=False
                )
            )
        elif args.command == "import-reviewer-admin-manifest":
            _print(
                apply_reviewer_admin_manifest(
                    connection, Path(args.manifest), dry_run=False
                )
            )
        elif args.command == "validate-entity-event-map-manifest":
            _print(validate_entity_event_map_manifest(connection, Path(args.manifest)))
        elif args.command == "import-entity-event-map-manifest":
            _print(import_entity_event_map_manifest(connection, Path(args.manifest)))
        elif args.command == "approve-public-metadata":
            _print(approve_public_source_metadata(connection))
        elif args.command == "export":
            _print(export_release(connection, args.out))
        elif args.command == "export-sharded":
            _print(
                export_release_v2(
                    connection,
                    args.out_dir,
                    catalog_shard_size=args.catalog_shard_size,
                )
            )
        elif args.command == "export-graph":
            _print(export_graph_release(connection, args.out_dir))
        else:  # pragma: no cover - argparse prevents this.
            raise RuntimeError(f"Unsupported command: {args.command}")
    finally:
        connection.close()
