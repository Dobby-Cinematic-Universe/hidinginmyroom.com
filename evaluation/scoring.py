"""Deterministic, manifest-bound transcript scoring over adjudicated references.

This module is stdlib-only and performs no inference, media access, catalogue access,
or publication.  A text-private system-output manifest must cover the exact frozen
interval set.  Scoring first revalidates the cohort, freeze, both independent
annotations, and adjudication, then emits a text-free aggregate report.
"""

from __future__ import annotations

import hashlib
import math
import platform
import random
import unicodedata
from dataclasses import dataclass, field
from datetime import timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from .validation import (
    ContractError,
    LANGUAGE_RE,
    _array,
    _canonical_bytes,
    _choice,
    _constant,
    _fail,
    _id,
    _integer,
    _object,
    _sha256,
    _stable_id,
    _string,
    _timestamp,
    _verify_manifest_digest,
    canonical_manifest_sha256,
    validate_adjudication,
    validate_interval_freeze,
)


SYSTEM_OUTPUT_SCHEMA_VERSION = 1
SCORE_REPORT_SCHEMA_VERSION = 1
IMPLEMENTATION_NAME = "himr-transcript-evaluation-scorer"
IMPLEMENTATION_VERSION = "0.1.0"
NORMALIZATION_PROFILE = "unicode_nfkc_casefold_lexical_v1"
BOOTSTRAP_METHOD = "recording_family_percentile_v1"
BOOTSTRAP_UNIT = "recording_family"
CALIBRATION_MINIMUM_OBSERVATIONS = 200
CALIBRATION_MINIMUM_POSITIVES = 50
CALIBRATION_MINIMUM_NEGATIVES = 50
MAX_BOOTSTRAP_REPLICATES = 10_000
MIN_BOOTSTRAP_REPLICATES = 200
MAX_TERMS = 1_000
MAX_TERM_CHARACTERS = 256
MAX_SYSTEM_LABEL_CHARACTERS = 256
MAX_SEGMENTS_PER_INTERVAL = 10_000
MAX_WORD_OBJECTS_PER_INTERVAL = 20_000
MAX_NORMALIZED_WORD_TOKENS_PER_INTERVAL = 4_096
MAX_TEXT_CHARACTERS_PER_INTERVAL = 200_000
MAX_RESOURCE_INTEGER = 2**63 - 1
MAX_SCORE_MAGNITUDE = 1_000_000_000.0
CPU_BRONZE_RTF_LIMIT = 0.35
REMOTE_GPU_RTF_LIMIT = 0.08
RESOURCE_RSS_LIMIT_BYTES = int(5.5 * 1024**3)
RESOURCE_PROFILES = {"cpu_bronze", "local_gpu", "remote_gpu", "other_measured"}
REPORT_PUBLICATION = {"status": "withheld", "storage_policy": "private_only"}


def _implementation_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _finite_number(value: object, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(path, "must be a finite number")
    result = float(value)
    if not math.isfinite(result) or abs(result) > MAX_SCORE_MAGNITUDE:
        _fail(path, f"must be finite with magnitude <= {MAX_SCORE_MAGNITUDE:g}")
    return result


def _nullable_score(value: object, path: str) -> float | None:
    return None if value is None else _finite_number(value, path)


def _bounded_integer(
    value: object, path: str, *, minimum: int, maximum: int
) -> int:
    parsed = _integer(value, path, minimum=minimum)
    if parsed > maximum:
        _fail(path, f"must be <= {maximum}")
    return parsed


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 12)


def _normalized_apostrophe(character: str) -> str:
    return "'" if character in {"'", "\u2019", "\u02bc"} else character


def normalize_words(text: str) -> list[str]:
    """Return deterministic Unicode lexical tokens for WER and glossary matching."""

    folded = unicodedata.normalize("NFKC", text).casefold()
    tokens: list[str] = []
    current: list[str] = []
    for index, original in enumerate(folded):
        character = _normalized_apostrophe(original)
        category = unicodedata.category(character)
        lexical = category[0] in {"L", "M", "N"}
        apostrophe_inside = (
            character == "'"
            and bool(current)
            and index + 1 < len(folded)
            and unicodedata.category(folded[index + 1])[0] in {"L", "M", "N"}
        )
        if lexical or apostrophe_inside:
            current.append(character)
        elif current:
            tokens.append("".join(current))
            current = []
    if current:
        tokens.append("".join(current))
    return tokens


def normalize_characters(text: str) -> str:
    """Return the whitespace/punctuation-free character stream used for CER."""

    return "".join(
        character
        for token in normalize_words(text)
        for character in token
        if unicodedata.category(character)[0] in {"L", "M", "N"}
    )


def _levenshtein_distance(reference: Sequence[str] | str, hypothesis: Sequence[str] | str) -> int:
    """Compute Levenshtein distance with two bounded rows."""

    if len(reference) > len(hypothesis):
        reference, hypothesis = hypothesis, reference
    previous = list(range(len(reference) + 1))
    for h_index, h_item in enumerate(hypothesis, 1):
        current = [h_index]
        for r_index, r_item in enumerate(reference, 1):
            current.append(
                min(
                    current[-1] + 1,
                    previous[r_index] + 1,
                    previous[r_index - 1] + (r_item != h_item),
                )
            )
        previous = current
    return previous[-1]


@dataclass(frozen=True)
class Alignment:
    substitutions: int
    deletions: int
    insertions: int
    operations: tuple[tuple[str, int | None, int | None], ...]

    @property
    def errors(self) -> int:
        return self.substitutions + self.deletions + self.insertions


def _word_alignment(reference: Sequence[str], hypothesis: Sequence[str]) -> Alignment:
    """Produce a deterministic minimum-edit alignment with compact backpointers."""

    rows = len(reference)
    columns = len(hypothesis)
    if max(rows, columns) > MAX_NORMALIZED_WORD_TOKENS_PER_INTERVAL:
        raise ContractError(
            "normalized word sequence exceeds the scorer's bounded alignment ceiling"
        )
    # 0=diagonal, 1=delete reference, 2=insert hypothesis.  Equal-cost ties prefer
    # diagonal, then deletion, then insertion so replay is stable.
    back = [bytearray(columns + 1) for _ in range(rows + 1)]
    for column in range(1, columns + 1):
        back[0][column] = 2
    previous = list(range(columns + 1))
    for row in range(1, rows + 1):
        current = [row] + [0] * columns
        back[row][0] = 1
        for column in range(1, columns + 1):
            diagonal = previous[column - 1] + (
                reference[row - 1] != hypothesis[column - 1]
            )
            deletion = previous[column] + 1
            insertion = current[column - 1] + 1
            best, operation = min(
                ((diagonal, 0), (deletion, 1), (insertion, 2)),
                key=lambda item: (item[0], item[1]),
            )
            current[column] = best
            back[row][column] = operation
        previous = current

    operations: list[tuple[str, int | None, int | None]] = []
    substitutions = deletions = insertions = 0
    row, column = rows, columns
    while row or column:
        operation = back[row][column]
        if row and column and operation == 0:
            label = "match" if reference[row - 1] == hypothesis[column - 1] else "substitute"
            substitutions += label == "substitute"
            operations.append((label, row - 1, column - 1))
            row -= 1
            column -= 1
        elif row and (not column or operation == 1):
            deletions += 1
            operations.append(("delete", row - 1, None))
            row -= 1
        else:
            insertions += 1
            operations.append(("insert", None, column - 1))
            column -= 1
    operations.reverse()
    return Alignment(substitutions, deletions, insertions, tuple(operations))


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = quantile * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def _count_term(tokens: Sequence[str], term: Sequence[str]) -> int:
    return len(_term_occurrence_spans(tokens, term))


def _term_occurrence_spans(
    tokens: Sequence[str], term: Sequence[str]
) -> list[tuple[int, int]]:
    """Return greedy, nonoverlapping half-open occurrences of one term."""

    if not term:
        return []
    spans: list[tuple[int, int]] = []
    index = 0
    while index + len(term) <= len(tokens):
        if list(tokens[index : index + len(term)]) == list(term):
            spans.append((index, index + len(term)))
            index += len(term)
        else:
            index += 1
    return spans


def _max_identical_run(tokens: Sequence[str]) -> int:
    maximum = current = 0
    previous: str | None = None
    for token in tokens:
        current = current + 1 if token == previous else 1
        maximum = max(maximum, current)
        previous = token
    return maximum


def _max_repeated_cycle_span(tokens: Sequence[str]) -> int:
    """Find bounded literal cycles repeated >=4 times and spanning >=12 tokens."""

    maximum = 0
    for start in range(len(tokens)):
        for width in range(1, min(8, (len(tokens) - start) // 4) + 1):
            pattern = tokens[start : start + width]
            repeats = 1
            cursor = start + width
            while cursor + width <= len(tokens) and list(tokens[cursor : cursor + width]) == list(pattern):
                repeats += 1
                cursor += width
            span = repeats * width
            if repeats >= 4 and span >= 12:
                maximum = max(maximum, span)
    return maximum


@dataclass(frozen=True)
class HypothesisToken:
    start_ms: int | None
    end_ms: int | None
    raw_score: float | None


@dataclass(frozen=True)
class HypothesisWord:
    start_ms: int | None
    end_ms: int | None


@dataclass
class IntervalStats:
    recording_id: str
    family_id: str
    split: str
    duration_ms: int
    flags: dict[str, Any]
    excluded_reason: str | None = None
    reference_words: int = 0
    hypothesis_words: int = 0
    substitutions: int = 0
    deletions: int = 0
    insertions: int = 0
    reference_characters: int = 0
    character_errors: int = 0
    reference_term_occurrences: int = 0
    matched_term_occurrences: int = 0
    missed_term_occurrences: int = 0
    false_term_insertions: int = 0
    term_evaluable_duration_ms: int = 0
    nonspeech_duration_ms: int = 0
    nonspeech_nonempty_segments: int = 0
    hypothesis_word_object_count: int = 0
    timed_word_object_count: int = 0
    monotonic_timed_word_object_count: int = 0
    repetition_candidate: bool = False
    max_identical_token_run: int = 0
    max_repeated_cycle_span: int = 0
    utterance_timing_eligible: int = 0
    utterance_timing_covered: int = 0
    boundary_errors_ms: list[int] = field(default_factory=list)
    calibration_word_eligible: int = 0
    calibration_word_scored: int = 0
    calibration_word_positive: int = 0
    calibration_word_negative: int = 0
    calibration_speech_eligible: int = 0
    calibration_speech_scored: int = 0
    calibration_speech_positive: int = 0
    calibration_speech_negative: int = 0


@dataclass(frozen=True)
class ResourceMeasurement:
    recording_id: str
    family_id: str
    split: str
    audio_duration_ms: int
    wall_time_ms: int
    cpu_time_ms: int
    peak_rss_bytes: int


@dataclass(frozen=True)
class ValidatedSystemOutput:
    manifest: dict[str, Any]
    term_tokens: tuple[tuple[str, ...], ...]
    term_set_sha256: str


def _freeze_rows(freeze: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    ordered: list[dict[str, Any]] = []
    lookup: dict[str, dict[str, Any]] = {}
    for recording in freeze["recordings"]:
        for interval in recording["intervals"]:
            row = {
                "recording_id": recording["recording_id"],
                "split": recording["split"],
                "interval_id": interval["interval_id"],
                "start_ms": interval["start_ms"],
                "end_ms": interval["end_ms"],
                "flags": interval["flags"],
            }
            ordered.append(row)
            lookup[row["interval_id"]] = row
    return ordered, lookup


def _system_output_identity(value: dict[str, Any]) -> str:
    unsigned = dict(value)
    unsigned.pop("manifest_sha256", None)
    unsigned.pop("system_output_id", None)
    digest = hashlib.sha256(_canonical_bytes(unsigned)).hexdigest()
    return f"system_output_{digest[:32]}"


def validate_transcript_system_output(
    value: object,
    interval_freeze: object,
    candidate_cohort: object,
) -> ValidatedSystemOutput:
    """Validate one complete text-private system hypothesis against a freeze."""

    freeze = validate_interval_freeze(interval_freeze, candidate_cohort)
    output = _object(
        value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "system_output_id",
            "freeze_id",
            "freeze_manifest_sha256",
            "created_at",
            "system",
            "scoring_profile",
            "privacy",
            "recordings",
        },
    )
    _constant(output["schema_version"], SYSTEM_OUTPUT_SCHEMA_VERSION, "$.schema_version")
    _constant(output["manifest_kind"], "transcript_system_output", "$.manifest_kind")
    output_id = _id(output["system_output_id"], "$.system_output_id")
    if output["freeze_id"] != freeze["freeze_id"]:
        _fail("$.freeze_id", "does not match interval freeze")
    if output["freeze_manifest_sha256"] != freeze["manifest_sha256"]:
        _fail("$.freeze_manifest_sha256", "does not bind exact interval freeze")
    created = _timestamp(output["created_at"], "$.created_at")
    if created < _timestamp(freeze["frozen_at"], "$.freeze.frozen_at"):
        _fail("$.created_at", "cannot precede interval freeze")

    system = _object(
        output["system"],
        "$.system",
        {
            "system_id",
            "system_label",
            "model_id",
            "run_manifest_sha256",
            "revision_kind",
            "resource_profile",
        },
    )
    _id(system["system_id"], "$.system.system_id")
    system_label = _string(system["system_label"], "$.system.system_label")
    if not system_label.strip() or len(system_label) > MAX_SYSTEM_LABEL_CHARACTERS:
        _fail(
            "$.system.system_label",
            f"must contain visible text no longer than {MAX_SYSTEM_LABEL_CHARACTERS} characters",
        )
    _id(system["model_id"], "$.system.model_id")
    _sha256(system["run_manifest_sha256"], "$.system.run_manifest_sha256")
    _choice(system["revision_kind"], {"raw_asr", "contextual_asr"}, "$.system.revision_kind")
    _choice(
        system["resource_profile"],
        RESOURCE_PROFILES,
        "$.system.resource_profile",
    )

    profile = _object(
        output["scoring_profile"],
        "$.scoring_profile",
        {"normalization_profile", "term_set", "bootstrap"},
    )
    _constant(profile["normalization_profile"], NORMALIZATION_PROFILE, "$.scoring_profile.normalization_profile")
    term_set = _object(
        profile["term_set"],
        "$.scoring_profile.term_set",
        {"term_set_revision_id", "language", "terms"},
    )
    _id(term_set["term_set_revision_id"], "$.scoring_profile.term_set.term_set_revision_id")
    _string(term_set["language"], "$.scoring_profile.term_set.language", pattern=LANGUAGE_RE)
    terms = _array(term_set["terms"], "$.scoring_profile.term_set.terms")
    if not terms or len(terms) > MAX_TERMS:
        _fail("$.scoring_profile.term_set.terms", f"must contain 1 through {MAX_TERMS} terms")
    normalized_terms: list[tuple[str, ...]] = []
    for index, raw in enumerate(terms):
        text = _string(raw, f"$.scoring_profile.term_set.terms[{index}]")
        if len(text) > MAX_TERM_CHARACTERS:
            _fail(
                f"$.scoring_profile.term_set.terms[{index}]",
                f"must be no longer than {MAX_TERM_CHARACTERS} characters",
            )
        tokens = tuple(normalize_words(text))
        if not tokens:
            _fail(f"$.scoring_profile.term_set.terms[{index}]", "normalizes to no lexical tokens")
        normalized_terms.append(tokens)
    if len(set(normalized_terms)) != len(normalized_terms):
        _fail("$.scoring_profile.term_set.terms", "contains duplicate normalized terms")
    canonical_terms = sorted([list(item) for item in normalized_terms])
    term_set_sha256 = hashlib.sha256(_canonical_bytes(canonical_terms)).hexdigest()

    bootstrap = _object(
        profile["bootstrap"],
        "$.scoring_profile.bootstrap",
        {"unit", "method", "replicates", "seed", "confidence_level"},
    )
    _constant(bootstrap["unit"], BOOTSTRAP_UNIT, "$.scoring_profile.bootstrap.unit")
    _constant(bootstrap["method"], BOOTSTRAP_METHOD, "$.scoring_profile.bootstrap.method")
    _bounded_integer(
        bootstrap["replicates"],
        "$.scoring_profile.bootstrap.replicates",
        minimum=MIN_BOOTSTRAP_REPLICATES,
        maximum=MAX_BOOTSTRAP_REPLICATES,
    )
    _bounded_integer(
        bootstrap["seed"],
        "$.scoring_profile.bootstrap.seed",
        minimum=0,
        maximum=2**63 - 1,
    )
    confidence = _finite_number(bootstrap["confidence_level"], "$.scoring_profile.bootstrap.confidence_level")
    if confidence != 0.95:
        _fail("$.scoring_profile.bootstrap.confidence_level", "must equal 0.95")

    privacy = _object(
        output["privacy"],
        "$.privacy",
        {"contains_transcript_text", "storage_policy", "publication_authority"},
    )
    _constant(privacy["contains_transcript_text"], True, "$.privacy.contains_transcript_text")
    _constant(privacy["storage_policy"], "private_only", "$.privacy.storage_policy")
    _constant(privacy["publication_authority"], "none", "$.privacy.publication_authority")

    expected_recordings = freeze["recordings"]
    recordings = _array(output["recordings"], "$.recordings")
    if len(recordings) != len(expected_recordings):
        _fail("$.recordings", "must cover every frozen recording exactly once")
    family_splits: dict[str, str] = {}
    seen_segment_ids: set[str] = set()
    seen_word_ids: set[str] = set()
    for recording_index, raw_recording in enumerate(recordings):
        path = f"$.recordings[{recording_index}]"
        recording = _object(
            raw_recording,
            path,
            {
                "recording_id",
                "recording_family_id",
                "split",
                "evaluated_audio_duration_ms",
                "resources",
                "intervals",
            },
        )
        frozen_recording = expected_recordings[recording_index]
        if recording["recording_id"] != frozen_recording["recording_id"]:
            _fail(f"{path}.recording_id", "must preserve exact freeze recording order")
        if recording["split"] != frozen_recording["split"]:
            _fail(f"{path}.split", "does not match frozen recording split")
        family_id = _id(recording["recording_family_id"], f"{path}.recording_family_id")
        prior_split = family_splits.setdefault(family_id, recording["split"])
        if prior_split != recording["split"]:
            _fail(f"{path}.recording_family_id", "one recording family cannot cross calibration/scoring splits")
        expected_duration = sum(
            interval["end_ms"] - interval["start_ms"]
            for interval in frozen_recording["intervals"]
        )
        _constant(recording["evaluated_audio_duration_ms"], expected_duration, f"{path}.evaluated_audio_duration_ms")
        resources = _object(
            recording["resources"],
            f"{path}.resources",
            {"wall_time_ms", "cpu_time_ms", "peak_rss_bytes"},
        )
        for field, minimum in (
            ("wall_time_ms", 1),
            ("cpu_time_ms", 0),
            ("peak_rss_bytes", 1),
        ):
            _bounded_integer(
                resources[field],
                f"{path}.resources.{field}",
                minimum=minimum,
                maximum=MAX_RESOURCE_INTEGER,
            )

        intervals = _array(recording["intervals"], f"{path}.intervals")
        frozen_intervals = frozen_recording["intervals"]
        if len(intervals) != len(frozen_intervals):
            _fail(f"{path}.intervals", "must cover every frozen interval exactly once")
        for interval_index, raw_interval in enumerate(intervals):
            interval_path = f"{path}.intervals[{interval_index}]"
            interval = _object(
                raw_interval,
                interval_path,
                {
                    "interval_id",
                    "start_ms",
                    "end_ms",
                    "state",
                    "speech_presence_raw_score",
                    "segments",
                },
            )
            frozen_interval = frozen_intervals[interval_index]
            for field in ("interval_id", "start_ms", "end_ms"):
                if interval[field] != frozen_interval[field]:
                    _fail(f"{interval_path}.{field}", "does not match exact frozen interval order/lineage")
            _constant(interval["state"], "completed", f"{interval_path}.state")
            _nullable_score(interval["speech_presence_raw_score"], f"{interval_path}.speech_presence_raw_score")
            segments = _array(interval["segments"], f"{interval_path}.segments")
            if len(segments) > MAX_SEGMENTS_PER_INTERVAL:
                _fail(f"{interval_path}.segments", f"exceeds {MAX_SEGMENTS_PER_INTERVAL} segments")
            previous_segment_key: tuple[int, int, str] | None = None
            word_objects = 0
            lexical_tokens = 0
            text_characters = 0
            word_text_characters = 0
            for segment_index, raw_segment in enumerate(segments):
                segment_path = f"{interval_path}.segments[{segment_index}]"
                segment = _object(
                    raw_segment,
                    segment_path,
                    {"segment_id", "start_ms", "end_ms", "text", "raw_score", "words"},
                )
                segment_id = _id(segment["segment_id"], f"{segment_path}.segment_id")
                if segment_id in seen_segment_ids:
                    _fail(f"{segment_path}.segment_id", "must be globally unique")
                seen_segment_ids.add(segment_id)
                start = _integer(segment["start_ms"], f"{segment_path}.start_ms")
                end = _integer(segment["end_ms"], f"{segment_path}.end_ms", minimum=1)
                if end <= start or start < interval["start_ms"] or end > interval["end_ms"]:
                    _fail(segment_path, "must be a non-empty interval within its frozen interval")
                segment_key = (start, end, segment_id)
                if previous_segment_key is not None and segment_key < previous_segment_key:
                    _fail(f"{interval_path}.segments", "must be sorted by start_ms, end_ms, segment_id")
                previous_segment_key = segment_key
                text = _string(segment["text"], f"{segment_path}.text", nonempty=False)
                if text and not text.strip():
                    _fail(f"{segment_path}.text", "must not be whitespace-only")
                text_characters += len(text)
                _nullable_score(segment["raw_score"], f"{segment_path}.raw_score")
                words = _array(segment["words"], f"{segment_path}.words")
                word_objects += len(words)
                normalized_word_tokens: list[str] = []
                for word_index, raw_word in enumerate(words):
                    word_path = f"{segment_path}.words[{word_index}]"
                    word = _object(
                        raw_word,
                        word_path,
                        {"word_id", "text", "start_ms", "end_ms", "raw_score"},
                    )
                    word_id = _id(word["word_id"], f"{word_path}.word_id")
                    if word_id in seen_word_ids:
                        _fail(f"{word_path}.word_id", "must be globally unique")
                    seen_word_ids.add(word_id)
                    word_text = _string(word["text"], f"{word_path}.text")
                    word_text_characters += len(word_text)
                    tokens = normalize_words(word_text)
                    if not tokens:
                        _fail(f"{word_path}.text", "must normalize to at least one lexical token")
                    normalized_word_tokens.extend(tokens)
                    start_value = word["start_ms"]
                    end_value = word["end_ms"]
                    if (start_value is None) != (end_value is None):
                        _fail(word_path, "start_ms and end_ms must both be null or both be integers")
                    if start_value is not None:
                        word_start = _integer(start_value, f"{word_path}.start_ms")
                        word_end = _integer(end_value, f"{word_path}.end_ms", minimum=1)
                        if word_end <= word_start or word_start < start or word_end > end:
                            _fail(word_path, "timed word must be a non-empty interval within its segment")
                    _nullable_score(word["raw_score"], f"{word_path}.raw_score")
                if normalized_word_tokens != normalize_words(text):
                    _fail(f"{segment_path}.words", "normalized word sequence must exactly reproduce segment text")
                lexical_tokens += len(normalized_word_tokens)
            if word_objects > MAX_WORD_OBJECTS_PER_INTERVAL:
                _fail(interval_path, f"exceeds {MAX_WORD_OBJECTS_PER_INTERVAL} word objects")
            if lexical_tokens > MAX_NORMALIZED_WORD_TOKENS_PER_INTERVAL:
                _fail(interval_path, f"exceeds {MAX_NORMALIZED_WORD_TOKENS_PER_INTERVAL} normalized word tokens")
            if text_characters > MAX_TEXT_CHARACTERS_PER_INTERVAL:
                _fail(interval_path, f"exceeds {MAX_TEXT_CHARACTERS_PER_INTERVAL} text characters")
            if word_text_characters > MAX_TEXT_CHARACTERS_PER_INTERVAL:
                _fail(
                    interval_path,
                    f"exceeds {MAX_TEXT_CHARACTERS_PER_INTERVAL} word-text characters",
                )

    _verify_manifest_digest(output)
    expected_output_id = _system_output_identity(output)
    if output_id != expected_output_id:
        _fail("$.system_output_id", f"must equal deterministic ID {expected_output_id!r}")
    return ValidatedSystemOutput(
        manifest=output,
        term_tokens=tuple(normalized_terms),
        term_set_sha256=term_set_sha256,
    )


def seal_transcript_system_output(value: dict[str, Any]) -> dict[str, Any]:
    """Fill a system-output deterministic ID and canonical digest for private tooling."""

    value["system_output_id"] = _system_output_identity(value)
    value["manifest_sha256"] = canonical_manifest_sha256(value)
    return value


def _hypothesis(
    interval: dict[str, Any],
) -> tuple[list[str], list[HypothesisToken], list[HypothesisWord]]:
    tokens: list[str] = []
    token_rows: list[HypothesisToken] = []
    word_rows: list[HypothesisWord] = []
    for segment in interval["segments"]:
        for word in segment["words"]:
            normalized = tuple(normalize_words(word["text"]))
            start = word["start_ms"]
            end = word["end_ms"]
            raw_score = None if word["raw_score"] is None else float(word["raw_score"])
            word_rows.append(HypothesisWord(start, end))
            for token in normalized:
                tokens.append(token)
                token_rows.append(HypothesisToken(start, end, raw_score))
    return tokens, token_rows, word_rows


def _reference_tokens(interval: dict[str, Any]) -> tuple[list[str], str, list[tuple[int, int, int, int]]]:
    words: list[str] = []
    character_parts: list[str] = []
    utterance_spans: list[tuple[int, int, int, int]] = []
    utterances = sorted(
        interval["utterances"],
        key=lambda item: (item["start_ms"], item["end_ms"], item["speaker_label"], item["utterance_id"]),
    )
    for utterance in utterances:
        utterance_words = normalize_words(utterance["text"])
        start_index = len(words)
        words.extend(utterance_words)
        character_parts.append(normalize_characters(utterance["text"]))
        if utterance_words:
            utterance_spans.append(
                (start_index, len(words) - 1, utterance["start_ms"], utterance["end_ms"])
            )
    return words, "".join(character_parts), utterance_spans


def _interval_stats(
    frozen: dict[str, Any],
    reference: dict[str, Any],
    hypothesis: dict[str, Any],
    family_id: str,
    term_tokens: Sequence[Sequence[str]],
) -> IntervalStats:
    stats = IntervalStats(
        recording_id=frozen["recording_id"],
        family_id=family_id,
        split=frozen["split"],
        duration_ms=frozen["end_ms"] - frozen["start_ms"],
        flags=frozen["flags"],
    )
    hypothesis_tokens, token_rows, word_rows = _hypothesis(hypothesis)
    stats.hypothesis_word_object_count = len(word_rows)
    stats.timed_word_object_count = sum(row.start_ms is not None for row in word_rows)
    previous_end: int | None = None
    for row in word_rows:
        if row.start_ms is None:
            continue
        if previous_end is None or row.start_ms >= previous_end:
            stats.monotonic_timed_word_object_count += 1
        previous_end = row.end_ms
    stats.max_identical_token_run = _max_identical_run(hypothesis_tokens)
    stats.max_repeated_cycle_span = _max_repeated_cycle_span(hypothesis_tokens)
    stats.repetition_candidate = stats.max_repeated_cycle_span >= 12

    state = reference["annotation_state"]
    resolution = reference["resolution"]
    if state == "non_speech":
        if resolution != "non_speech":
            _fail("$.adjudication.intervals", "non_speech annotation must use non_speech resolution for scoring")
        stats.nonspeech_duration_ms = stats.duration_ms
        stats.term_evaluable_duration_ms = stats.duration_ms
        # The registered gate is expressed in nonempty ASR segments, not lexical
        # tokens.  Punctuation/music-symbol-only output must therefore count too.
        stats.nonspeech_nonempty_segments = sum(
            bool(segment["text"]) for segment in hypothesis["segments"]
        )
        for term in term_tokens:
            hypothesis_count = _count_term(hypothesis_tokens, term)
            stats.false_term_insertions += hypothesis_count
    elif resolution == "non_speech":
        _fail("$.adjudication.intervals", "non_speech resolution requires non_speech annotation state")

    speech_score = hypothesis["speech_presence_raw_score"]
    if state in {"transcribed", "non_speech"}:
        stats.calibration_speech_eligible = 1
        if speech_score is not None:
            stats.calibration_speech_scored = 1
            if state == "transcribed":
                stats.calibration_speech_positive = 1
            else:
                stats.calibration_speech_negative = 1

    if state == "unintelligible":
        if resolution not in {"uncertainty_retained", "resolved_after_review"}:
            _fail("$.adjudication.intervals", "unintelligible interval has incompatible resolution")
        stats.excluded_reason = "adjudicated_unintelligible"
        return stats
    if state == "non_speech":
        stats.excluded_reason = "adjudicated_non_speech"
        return stats
    if resolution == "uncertainty_retained" or any(
        row["text_state"] != "verbatim" for row in reference["utterances"]
    ):
        stats.excluded_reason = "adjudicated_text_uncertainty"
        return stats
    if resolution not in {"agreement", "resolved_after_review"}:
        _fail("$.adjudication.intervals", "transcribed interval has incompatible resolution")
    stats.term_evaluable_duration_ms = stats.duration_ms

    reference_words, reference_characters, utterance_spans = _reference_tokens(reference)
    alignment = _word_alignment(reference_words, hypothesis_tokens)
    stats.reference_words = len(reference_words)
    stats.hypothesis_words = len(hypothesis_tokens)
    stats.substitutions = alignment.substitutions
    stats.deletions = alignment.deletions
    stats.insertions = alignment.insertions
    hypothesis_characters = "".join(hypothesis_tokens)
    stats.reference_characters = len(reference_characters)
    stats.character_errors = _levenshtein_distance(reference_characters, hypothesis_characters)

    matched_by_reference: dict[int, int] = {}
    for operation, reference_index, hypothesis_index in alignment.operations:
        if operation == "match":
            assert reference_index is not None and hypothesis_index is not None
            matched_by_reference[reference_index] = hypothesis_index
        if hypothesis_index is not None and token_rows[hypothesis_index].raw_score is not None:
            stats.calibration_word_scored += 1
            if operation == "match":
                stats.calibration_word_positive += 1
            elif operation in {"substitute", "insert"}:
                stats.calibration_word_negative += 1

    # A reference term is recalled only when every one of its aligned tokens is an
    # exact match to one contiguous hypothesis occurrence.  A bag-of-counts match
    # would incorrectly credit a term hallucinated elsewhere in a long interval.
    for term in term_tokens:
        reference_occurrences = _term_occurrence_spans(reference_words, term)
        hypothesis_occurrences = set(_term_occurrence_spans(hypothesis_tokens, term))
        matched_occurrences: set[tuple[int, int]] = set()
        for reference_start, reference_end in reference_occurrences:
            aligned = [
                matched_by_reference.get(index)
                for index in range(reference_start, reference_end)
            ]
            if any(index is None for index in aligned):
                continue
            hypothesis_start = aligned[0]
            assert hypothesis_start is not None
            expected = list(range(hypothesis_start, hypothesis_start + len(term)))
            if aligned == expected and (hypothesis_start, hypothesis_start + len(term)) in hypothesis_occurrences:
                matched_occurrences.add((hypothesis_start, hypothesis_start + len(term)))
        matched = len(matched_occurrences)
        stats.reference_term_occurrences += len(reference_occurrences)
        stats.matched_term_occurrences += matched
        stats.missed_term_occurrences += len(reference_occurrences) - matched
        stats.false_term_insertions += len(hypothesis_occurrences) - matched
    # A hypothesis token without a score is still eligible for score-coverage accounting.
    stats.calibration_word_eligible = sum(
        1
        for operation, _, hypothesis_index in alignment.operations
        if hypothesis_index is not None and operation in {"match", "substitute", "insert"}
    )

    stats.utterance_timing_eligible = len(utterance_spans)
    for first_reference, last_reference, utterance_start, utterance_end in utterance_spans:
        first_hypothesis = matched_by_reference.get(first_reference)
        last_hypothesis = matched_by_reference.get(last_reference)
        if first_hypothesis is None or last_hypothesis is None:
            continue
        first_token = token_rows[first_hypothesis]
        last_token = token_rows[last_hypothesis]
        if first_token.start_ms is None or last_token.end_ms is None:
            continue
        stats.utterance_timing_covered += 1
        stats.boundary_errors_ms.extend(
            [abs(first_token.start_ms - utterance_start), abs(last_token.end_ms - utterance_end)]
        )
    return stats


def _scope_aggregate(
    interval_stats: Sequence[IntervalStats],
    resources: Sequence[ResourceMeasurement],
) -> dict[str, Any]:
    reference_words = sum(item.reference_words for item in interval_stats)
    substitutions = sum(item.substitutions for item in interval_stats)
    deletions = sum(item.deletions for item in interval_stats)
    insertions = sum(item.insertions for item in interval_stats)
    reference_characters = sum(item.reference_characters for item in interval_stats)
    character_errors = sum(item.character_errors for item in interval_stats)
    term_reference = sum(item.reference_term_occurrences for item in interval_stats)
    term_matched = sum(item.matched_term_occurrences for item in interval_stats)
    false_terms = sum(item.false_term_insertions for item in interval_stats)
    term_evaluable_ms = sum(item.term_evaluable_duration_ms for item in interval_stats)
    duration_ms = sum(item.duration_ms for item in interval_stats)
    nonspeech_ms = sum(item.nonspeech_duration_ms for item in interval_stats)
    hallucinated_segments = sum(item.nonspeech_nonempty_segments for item in interval_stats)
    word_objects = sum(item.hypothesis_word_object_count for item in interval_stats)
    timed_words = sum(item.timed_word_object_count for item in interval_stats)
    monotonic_words = sum(item.monotonic_timed_word_object_count for item in interval_stats)
    timing_eligible = sum(item.utterance_timing_eligible for item in interval_stats)
    timing_covered = sum(item.utterance_timing_covered for item in interval_stats)
    boundary_errors = [error for item in interval_stats for error in item.boundary_errors_ms]
    audio_ms = sum(item.audio_duration_ms for item in resources)
    wall_ms = sum(item.wall_time_ms for item in resources)
    cpu_ms = sum(item.cpu_time_ms for item in resources)
    peak_rss = max((item.peak_rss_bytes for item in resources), default=None)
    return {
        "interval_count": len(interval_stats),
        "recording_count": len({item.recording_id for item in interval_stats}),
        "recording_family_count": len({item.family_id for item in interval_stats}),
        "duration_ms": duration_ms,
        "text_scorable_interval_count": sum(item.excluded_reason is None for item in interval_stats),
        "excluded_interval_counts": {
            reason: sum(item.excluded_reason == reason for item in interval_stats)
            for reason in (
                "adjudicated_non_speech",
                "adjudicated_unintelligible",
                "adjudicated_text_uncertainty",
            )
        },
        "word": {
            "reference_count": reference_words,
            "hypothesis_count": sum(item.hypothesis_words for item in interval_stats),
            "substitutions": substitutions,
            "deletions": deletions,
            "insertions": insertions,
            "error_count": substitutions + deletions + insertions,
            "error_rate": _round(_ratio(substitutions + deletions + insertions, reference_words)),
        },
        "character": {
            "reference_count": reference_characters,
            "error_count": character_errors,
            "error_rate": _round(_ratio(character_errors, reference_characters)),
        },
        "himr_terms": {
            "evaluated_duration_ms": term_evaluable_ms,
            "reference_occurrences": term_reference,
            "matched_occurrences": term_matched,
            "missed_occurrences": sum(item.missed_term_occurrences for item in interval_stats),
            "false_insertions": false_terms,
            "recall": _round(_ratio(term_matched, term_reference)),
            "false_insertions_per_media_hour": _round(
                _ratio(false_terms * 3_600_000, term_evaluable_ms)
            ),
        },
        "nonspeech_hallucination": {
            "annotated_nonspeech_duration_ms": nonspeech_ms,
            "nonempty_segment_count": hallucinated_segments,
            "segments_per_nonspeech_hour": _round(
                _ratio(hallucinated_segments * 3_600_000, nonspeech_ms)
            ),
        },
        "repetition": {
            "candidate_interval_count": sum(item.repetition_candidate for item in interval_stats),
            "maximum_identical_token_run": max(
                (item.max_identical_token_run for item in interval_stats), default=0
            ),
            "maximum_repeated_cycle_span_tokens": max(
                (item.max_repeated_cycle_span for item in interval_stats), default=0
            ),
            "automatic_hallucination_claimed": False,
        },
        "timing": {
            "hypothesis_word_object_count": word_objects,
            "timed_word_object_count": timed_words,
            "word_timing_coverage": _round(_ratio(timed_words, word_objects)),
            "monotonic_timed_word_object_count": monotonic_words,
            "monotonic_timed_word_fraction": _round(_ratio(monotonic_words, timed_words)),
            "reference_utterance_count": timing_eligible,
            "covered_reference_utterance_count": timing_covered,
            "utterance_span_coverage": _round(_ratio(timing_covered, timing_eligible)),
            "boundary_error_sample_count": len(boundary_errors),
            "median_absolute_boundary_error_ms": _round(
                _percentile([float(item) for item in boundary_errors], 0.5)
            ),
            "p95_absolute_boundary_error_ms": _round(
                _percentile([float(item) for item in boundary_errors], 0.95)
            ),
            "word_boundary_accuracy_claimed": False,
        },
        "resources": {
            "evaluated_audio_duration_ms": audio_ms,
            "wall_time_ms": wall_ms,
            "cpu_time_ms": cpu_ms,
            "wall_real_time_factor": _round(_ratio(wall_ms, audio_ms)),
            "cpu_real_time_factor": _round(_ratio(cpu_ms, audio_ms)),
            "peak_rss_bytes": peak_rss,
        },
    }


def _metric_from_aggregate(aggregate: dict[str, Any], path: tuple[str, ...]) -> float | None:
    value: Any = aggregate
    for key in path:
        value = value[key]
    return None if value is None else float(value)


def _bootstrap_ci(
    *,
    label: str,
    families: dict[str, tuple[list[IntervalStats], list[ResourceMeasurement]]],
    metric: Callable[[dict[str, Any]], float | None],
    replicates: int,
    seed: int,
) -> dict[str, Any]:
    family_ids = sorted(families)
    if len(family_ids) < 2:
        return {"state": "not_available", "reason": "fewer_than_two_recording_families"}
    observed_intervals = [item for family in family_ids for item in families[family][0]]
    observed_resources = [item for family in family_ids for item in families[family][1]]
    if metric(_scope_aggregate(observed_intervals, observed_resources)) is None:
        return {"state": "not_available", "reason": "metric_denominator_is_zero"}
    label_seed = int.from_bytes(hashlib.sha256(label.encode("utf-8")).digest()[:8], "big")
    generator = random.Random((seed + label_seed) % (2**63))
    samples: list[float] = []
    for _ in range(replicates):
        intervals: list[IntervalStats] = []
        resources: list[ResourceMeasurement] = []
        for _ in family_ids:
            selected = family_ids[generator.randrange(len(family_ids))]
            intervals.extend(families[selected][0])
            resources.extend(families[selected][1])
        value = metric(_scope_aggregate(intervals, resources))
        if value is not None:
            samples.append(value)
    if len(samples) != replicates:
        return {"state": "not_available", "reason": "bootstrap_resample_lost_metric_denominator"}
    return {
        "state": "available",
        "method": BOOTSTRAP_METHOD,
        "unit": BOOTSTRAP_UNIT,
        "confidence_level": 0.95,
        "replicates": replicates,
        "lower": _round(_percentile(samples, 0.025)),
        "upper": _round(_percentile(samples, 0.975)),
    }


BOOTSTRAP_METRICS: dict[str, tuple[str, ...]] = {
    "word_error_rate": ("word", "error_rate"),
    "character_error_rate": ("character", "error_rate"),
    "himr_term_recall": ("himr_terms", "recall"),
    "himr_false_insertions_per_media_hour": ("himr_terms", "false_insertions_per_media_hour"),
    "nonspeech_segments_per_hour": ("nonspeech_hallucination", "segments_per_nonspeech_hour"),
    "word_timing_coverage": ("timing", "word_timing_coverage"),
    "monotonic_timed_word_fraction": ("timing", "monotonic_timed_word_fraction"),
    "utterance_span_coverage": ("timing", "utterance_span_coverage"),
    "median_absolute_boundary_error_ms": ("timing", "median_absolute_boundary_error_ms"),
    "p95_absolute_boundary_error_ms": ("timing", "p95_absolute_boundary_error_ms"),
    "wall_real_time_factor": ("resources", "wall_real_time_factor"),
    "cpu_real_time_factor": ("resources", "cpu_real_time_factor"),
    "peak_rss_bytes": ("resources", "peak_rss_bytes"),
}


def _scope_report(
    interval_stats: Sequence[IntervalStats],
    resources: Sequence[ResourceMeasurement],
    *,
    replicates: int,
    seed: int,
    label: str,
) -> dict[str, Any]:
    aggregate = _scope_aggregate(interval_stats, resources)
    families: dict[str, tuple[list[IntervalStats], list[ResourceMeasurement]]] = {}
    for family_id in sorted({item.family_id for item in interval_stats}):
        families[family_id] = (
            [item for item in interval_stats if item.family_id == family_id],
            [item for item in resources if item.family_id == family_id],
        )
    aggregate["bootstrap_confidence_intervals"] = {
        metric_name: _bootstrap_ci(
            label=f"{label}:{metric_name}",
            families=families,
            metric=lambda row, path=path: _metric_from_aggregate(row, path),
            replicates=replicates,
            seed=seed,
        )
        for metric_name, path in BOOTSTRAP_METRICS.items()
    }
    return aggregate


def _readiness_row(
    task: str,
    *,
    eligible: int,
    scored: int,
    positives: int,
    negatives: int,
) -> dict[str, Any]:
    if scored == 0:
        state = "no_raw_scores"
    elif scored < CALIBRATION_MINIMUM_OBSERVATIONS:
        state = "insufficient_observations"
    elif positives < CALIBRATION_MINIMUM_POSITIVES:
        state = "insufficient_positives"
    elif negatives < CALIBRATION_MINIMUM_NEGATIVES:
        state = "insufficient_negatives"
    else:
        state = "ready_to_fit_held_out_calibrator"
    return {
        "task": task,
        "split": "calibration",
        "eligible_observation_count": eligible,
        "raw_score_observation_count": scored,
        "raw_score_coverage": _round(_ratio(scored, eligible)),
        "positive_count": positives,
        "negative_count": negatives,
        "minimums": {
            "observations": CALIBRATION_MINIMUM_OBSERVATIONS,
            "positives": CALIBRATION_MINIMUM_POSITIVES,
            "negatives": CALIBRATION_MINIMUM_NEGATIVES,
        },
        "state": state,
        "calibration_fitted": False,
        "calibrated_probability_claimed": False,
    }


def _calibration_readiness(interval_stats: Sequence[IntervalStats]) -> list[dict[str, Any]]:
    calibration = [item for item in interval_stats if item.split == "calibration"]
    return [
        _readiness_row(
            "word_correctness",
            eligible=sum(item.calibration_word_eligible for item in calibration),
            scored=sum(item.calibration_word_scored for item in calibration),
            positives=sum(item.calibration_word_positive for item in calibration),
            negatives=sum(item.calibration_word_negative for item in calibration),
        ),
        _readiness_row(
            "speech_presence",
            eligible=sum(item.calibration_speech_eligible for item in calibration),
            scored=sum(item.calibration_speech_scored for item in calibration),
            positives=sum(item.calibration_speech_positive for item in calibration),
            negatives=sum(item.calibration_speech_negative for item in calibration),
        ),
    ]


def _gate(
    gate_id: str,
    metric: str,
    value: float | None,
    comparison: str,
    threshold: float,
    *,
    scope: str = "scoring",
    eligible: bool = True,
    reason: str | None = None,
) -> dict[str, Any]:
    if not eligible or value is None:
        status = "not_evaluable"
    elif comparison == "at_most":
        status = "pass" if value <= threshold else "fail"
    else:
        status = "pass" if value >= threshold else "fail"
    row = {
        "gate_id": gate_id,
        "scope": scope,
        "metric": metric,
        "value": _round(value),
        "comparison": comparison,
        "threshold": threshold,
        "status": status,
    }
    if status == "not_evaluable":
        row["reason"] = reason or "metric_denominator_is_zero"
    return row


def _quality_gates(
    scoring: dict[str, Any],
    overall: dict[str, Any],
    scoring_stats: Sequence[IntervalStats],
    revision_kind: str,
    resource_profile: str,
) -> list[dict[str, Any]]:
    text_stats = [item for item in scoring_stats if item.excluded_reason is None]
    all_english = bool(text_stats) and all(
        item.flags["language_tags"] == ["en"] and not item.flags["code_switch"]
        for item in text_stats
    )
    wer_gate_reason = (
        None
        if all_english
        else "scoring_text_is_not_exclusively_non_codeswitched_english"
    )
    term_recall = scoring["himr_terms"]["recall"]
    term_threshold = 0.92 if revision_kind == "contextual_asr" else 0.90
    contextual_absolute_branch = (
        revision_kind != "contextual_asr"
        or term_recall is None
        or term_recall >= term_threshold
    )
    term_gate_reason = (
        "contextual_absolute_branch_not_met_and_bound_baseline_is_required_for_relative_branch"
        if not contextual_absolute_branch
        else None
    )
    repetition_candidates = scoring["repetition"]["candidate_interval_count"]
    if resource_profile == "cpu_bronze":
        resource_rtf_limit = CPU_BRONZE_RTF_LIMIT
        resource_gate_eligible = True
        resource_reason = None
        rss_gate_eligible = True
        rss_reason = None
    elif resource_profile in {"local_gpu", "remote_gpu"}:
        resource_rtf_limit = REMOTE_GPU_RTF_LIMIT
        resource_gate_eligible = True
        resource_reason = None
        rss_gate_eligible = False
        rss_reason = "gpu_gate_requires_peak_vram_not_process_rss"
    else:
        resource_rtf_limit = CPU_BRONZE_RTF_LIMIT
        resource_gate_eligible = False
        resource_reason = "no_registered_gate_for_other_measured_profile"
        rss_gate_eligible = False
        rss_reason = "no_registered_gate_for_other_measured_profile"

    return [
        _gate(
            "asr_all_english_wer",
            "word_error_rate",
            scoring["word"]["error_rate"],
            "at_most",
            0.22,
            eligible=all_english,
            reason=wer_gate_reason,
        ),
        _gate(
            "himr_term_recall",
            "himr_term_recall",
            term_recall,
            "at_least",
            term_threshold,
            eligible=contextual_absolute_branch,
            reason=term_gate_reason,
        ),
        _gate(
            "himr_false_term_insertions",
            "false_insertions_per_media_hour",
            scoring["himr_terms"]["false_insertions_per_media_hour"],
            "at_most",
            1.0,
            eligible=revision_kind == "contextual_asr",
            reason=(
                None
                if revision_kind == "contextual_asr"
                else "registered_false_glossary_insertion_gate_applies_to_contextual_asr"
            ),
        ),
        _gate(
            "nonspeech_hallucinated_segments",
            "segments_per_nonspeech_hour",
            scoring["nonspeech_hallucination"]["segments_per_nonspeech_hour"],
            "at_most",
            1.0,
        ),
        _gate(
            "repetition_candidate_intervals",
            "repetition_candidate_interval_count",
            float(repetition_candidates),
            "at_most",
            0.0,
            eligible=repetition_candidates == 0,
            reason="deterministic_repetition_candidates_require_human_review",
        ),
        _gate("word_timing_coverage", "word_timing_coverage", scoring["timing"]["word_timing_coverage"], "at_least", 0.98),
        _gate(
            "monotonic_word_timing",
            "monotonic_timed_word_fraction",
            scoring["timing"]["monotonic_timed_word_fraction"],
            "at_least",
            0.98,
        ),
        _gate(
            "utterance_boundary_median",
            "median_absolute_boundary_error_ms",
            scoring["timing"]["median_absolute_boundary_error_ms"],
            "at_most",
            200.0,
        ),
        _gate(
            "utterance_boundary_p95",
            "p95_absolute_boundary_error_ms",
            scoring["timing"]["p95_absolute_boundary_error_ms"],
            "at_most",
            800.0,
        ),
        _gate(
            "resource_wall_rtf",
            "wall_real_time_factor",
            overall["resources"]["wall_real_time_factor"],
            "at_most",
            resource_rtf_limit,
            scope="overall",
            eligible=resource_gate_eligible,
            reason=resource_reason,
        ),
        _gate(
            "cpu_bronze_peak_rss",
            "peak_rss_bytes",
            None
            if overall["resources"]["peak_rss_bytes"] is None
            else float(overall["resources"]["peak_rss_bytes"]),
            "at_most",
            float(RESOURCE_RSS_LIMIT_BYTES),
            scope="overall",
            eligible=rss_gate_eligible,
            reason=rss_reason,
        ),
    ]


def score_transcript_system(
    *,
    candidate_cohort: object,
    interval_freeze: object,
    pass_a: object,
    pass_b: object,
    adjudication: object,
    system_output: object,
    created_at: str,
) -> dict[str, Any]:
    """Validate all inputs and emit one deterministic, text-free score report."""

    freeze = validate_interval_freeze(interval_freeze, candidate_cohort)
    reference = validate_adjudication(
        adjudication, freeze, candidate_cohort, pass_a, pass_b
    )
    passes_by_name = {pass_a["pass_name"]: pass_a, pass_b["pass_name"]: pass_b}
    validated_system = validate_transcript_system_output(
        system_output, freeze, candidate_cohort
    )
    created = _timestamp(created_at, "$.report.created_at")
    latest_input = max(
        _timestamp(reference["created_at"], "$.adjudication.created_at"),
        _timestamp(validated_system.manifest["created_at"], "$.system_output.created_at"),
    )
    if created < latest_input:
        _fail("$.report.created_at", "cannot precede adjudication or system output")

    ordered_freeze, freeze_lookup = _freeze_rows(freeze)
    reference_lookup = {item["interval_id"]: item for item in reference["intervals"]}
    system_lookup: dict[str, tuple[dict[str, Any], str]] = {}
    resources: list[ResourceMeasurement] = []
    for recording in validated_system.manifest["recordings"]:
        family_id = recording["recording_family_id"]
        resource = recording["resources"]
        resources.append(
            ResourceMeasurement(
                recording_id=recording["recording_id"],
                family_id=family_id,
                split=recording["split"],
                audio_duration_ms=recording["evaluated_audio_duration_ms"],
                wall_time_ms=resource["wall_time_ms"],
                cpu_time_ms=resource["cpu_time_ms"],
                peak_rss_bytes=resource["peak_rss_bytes"],
            )
        )
        for interval in recording["intervals"]:
            system_lookup[interval["interval_id"]] = (interval, family_id)

    interval_stats: list[IntervalStats] = []
    for frozen in ordered_freeze:
        interval_id = frozen["interval_id"]
        hypothesis, family_id = system_lookup[interval_id]
        interval_stats.append(
            _interval_stats(
                freeze_lookup[interval_id],
                reference_lookup[interval_id],
                hypothesis,
                family_id,
                validated_system.term_tokens,
            )
        )

    bootstrap = validated_system.manifest["scoring_profile"]["bootstrap"]
    scopes: list[dict[str, Any]] = []
    for split in ("calibration", "scoring"):
        split_stats = [item for item in interval_stats if item.split == split]
        split_resources = [item for item in resources if item.split == split]
        scopes.append(
            {
                "scope": split,
                "metrics": _scope_report(
                    split_stats,
                    split_resources,
                    replicates=bootstrap["replicates"],
                    seed=bootstrap["seed"],
                    label=split,
                ),
            }
        )
    overall = _scope_report(
        interval_stats,
        resources,
        replicates=bootstrap["replicates"],
        seed=bootstrap["seed"],
        label="overall",
    )
    scoring_metrics = next(row["metrics"] for row in scopes if row["scope"] == "scoring")
    scoring_stats = [item for item in interval_stats if item.split == "scoring"]
    implementation_sha = _implementation_sha256()
    python_version = platform.python_version()
    unicode_version = unicodedata.unidata_version
    system_manifest = validated_system.manifest
    report_id = _stable_id(
        "transcript_score_report",
        freeze["manifest_sha256"],
        reference["manifest_sha256"],
        system_manifest["manifest_sha256"],
        implementation_sha,
        python_version,
        unicode_version,
        created_at,
    )
    report = {
        "schema_version": SCORE_REPORT_SCHEMA_VERSION,
        "manifest_kind": "transcript_score_report",
        "manifest_sha256": "0" * 64,
        "report_id": report_id,
        "created_at": created.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "implementation": {
            "name": IMPLEMENTATION_NAME,
            "version": IMPLEMENTATION_VERSION,
            "source_sha256": implementation_sha,
            "python_version": python_version,
            "unicode_version": unicode_version,
        },
        "inputs": {
            "cohort_id": freeze["cohort_id"],
            "cohort_manifest_sha256": freeze["cohort_manifest_sha256"],
            "freeze_id": freeze["freeze_id"],
            "freeze_manifest_sha256": freeze["manifest_sha256"],
            "pass_a_annotation_id": passes_by_name["pass_a"]["annotation_id"],
            "pass_a_manifest_sha256": passes_by_name["pass_a"]["manifest_sha256"],
            "pass_b_annotation_id": passes_by_name["pass_b"]["annotation_id"],
            "pass_b_manifest_sha256": passes_by_name["pass_b"]["manifest_sha256"],
            "adjudication_id": reference["adjudication_id"],
            "adjudication_manifest_sha256": reference["manifest_sha256"],
            "system_output_id": system_manifest["system_output_id"],
            "system_output_manifest_sha256": system_manifest["manifest_sha256"],
            "system_id": system_manifest["system"]["system_id"],
            "model_id": system_manifest["system"]["model_id"],
            "run_manifest_sha256": system_manifest["system"]["run_manifest_sha256"],
            "resource_profile": system_manifest["system"]["resource_profile"],
        },
        "policy": {
            "normalization_profile": NORMALIZATION_PROFILE,
            "text_scoring": "adjudicated_verbatim_agreement_or_resolved_only",
            "character_scoring": "normalized_lexical_characters_without_whitespace_or_punctuation",
            "term_matching": "alignment_bound_greedy_nonoverlapping_normalized_occurrences_per_interval",
            "timing_error": "exact_matched_first_and_last_token_against_reference_utterance_span",
            "repetition_detection": "literal_cycle_1_to_8_tokens_four_repeats_minimum_12_tokens",
            "bootstrap": dict(bootstrap),
            "calibration_sample_gate": {
                "minimum_observations": CALIBRATION_MINIMUM_OBSERVATIONS,
                "minimum_positives": CALIBRATION_MINIMUM_POSITIVES,
                "minimum_negatives": CALIBRATION_MINIMUM_NEGATIVES,
                "calibration_fitted": False,
            },
        },
        "term_set": {
            "term_set_revision_id": system_manifest["scoring_profile"]["term_set"]["term_set_revision_id"],
            "language": system_manifest["scoring_profile"]["term_set"]["language"],
            "term_count": len(validated_system.term_tokens),
            "normalized_term_set_sha256": validated_system.term_set_sha256,
            "term_text_included": False,
        },
        "metrics": {"overall": overall, "splits": scopes},
        "calibration_readiness": _calibration_readiness(interval_stats),
        "quality_gates": _quality_gates(
            scoring_metrics,
            overall,
            scoring_stats,
            system_manifest["system"]["revision_kind"],
            system_manifest["system"]["resource_profile"],
        ),
        "warnings": [
            "Machine raw scores are not probabilities and no calibration is fitted by this report.",
            "Utterance-span timing error is not word-boundary ground truth.",
            "Repetition rows are deterministic review candidates, not automatic hallucination claims.",
            "Resource gates are selected by the system output's explicit resource profile.",
            "Frozen-cohort resource comparisons do not establish the separate six-hour capacity benchmark.",
            "Aggregate WER and CER do not establish language-specific or condition-stratum gates.",
            "A contextual system requires a separately bound baseline report for relative-improvement claims.",
        ],
        "publication": dict(REPORT_PUBLICATION),
        "contains_transcript_text": False,
        "accuracy_metrics_computed": True,
    }
    report["manifest_sha256"] = canonical_manifest_sha256(report)
    return report


def validate_transcript_score_report(
    report_value: object,
    *,
    candidate_cohort: object,
    interval_freeze: object,
    pass_a: object,
    pass_b: object,
    adjudication: object,
    system_output: object,
) -> dict[str, Any]:
    """Recompute and byte-compare a sealed score report."""

    if not isinstance(report_value, dict):
        _fail("$", "score report must be an object")
    _verify_manifest_digest(report_value)
    created_at = report_value.get("created_at")
    _timestamp(created_at, "$.created_at")
    expected = score_transcript_system(
        candidate_cohort=candidate_cohort,
        interval_freeze=interval_freeze,
        pass_a=pass_a,
        pass_b=pass_b,
        adjudication=adjudication,
        system_output=system_output,
        created_at=created_at,
    )
    if _canonical_bytes(report_value) != _canonical_bytes(expected):
        _fail("$", "score report differs from deterministic recomputation")
    return report_value
