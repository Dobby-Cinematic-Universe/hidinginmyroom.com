"""Dependency-light contracts for transcript evaluation data."""

from .interval_proposal import (
    emit_full_rendition_request,
    emit_local_window_proposal_request,
    prepare_interval_proposal,
    validate_interval_proposal,
    validate_interval_proposal_request,
)

from .validation import (
    ContractError,
    acquisition_selection,
    canonical_manifest_sha256,
    validate_adjudication,
    validate_annotation,
    validate_candidate_cohort,
    validate_interval_freeze,
)
from .selection_review import (
    compile_interval_freeze,
    emit_selection_review_template,
    validate_compiled_interval_freeze,
    validate_interval_selection_review,
)
from .scoring import (
    normalize_characters,
    normalize_words,
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

__all__ = [
    "ContractError",
    "acquisition_selection",
    "canonical_manifest_sha256",
    "compile_interval_freeze",
    "compare_transcript_systems",
    "emit_full_rendition_request",
    "emit_local_window_proposal_request",
    "emit_selection_review_template",
    "prepare_interval_proposal",
    "normalize_characters",
    "normalize_words",
    "score_transcript_system",
    "seal_transcript_system_output",
    "seal_gpu_evaluation_measurement",
    "validate_adjudication",
    "validate_annotation",
    "validate_candidate_cohort",
    "validate_interval_freeze",
    "validate_compiled_interval_freeze",
    "validate_interval_proposal",
    "validate_interval_proposal_request",
    "validate_interval_selection_review",
    "validate_gpu_evaluation_measurement",
    "validate_transcript_system_comparison",
    "validate_transcript_score_report",
    "validate_transcript_system_output",
]
