"""Strict stdlib-only validators for the transcript evaluation workflow.

JSON Schema files document the wire contracts. These runtime validators enforce
the cross-document, timing, lineage, accounting, and independence invariants that
JSON Schema cannot express.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
NAMESPACE = uuid.UUID("8b92c560-551c-5bc7-a8b2-cec3aa8fb730")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
OPAQUE_ID_RE = re.compile(r"[A-Za-z][A-Za-z0-9._:-]{2,127}")
UUID_ID_RE = re.compile(r"(?:src|rec|rnd)_[0-9a-f]{32}")
YOUTUBE_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")
LANGUAGE_RE = re.compile(r"(?:[a-z]{2,3}|und)(?:-[A-Za-z0-9]{2,8})*")
TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
SELECTION_REVIEW_ID_RE = re.compile(r"selection_review_[0-9a-f]{32}")

CANDIDATE_V1_NATIVE_IDS = (
    "h3ySLeBAoXs",
    "0frp1tHu7ek",
    "s2OW-jRyFrw",
    "92LgEG6NhUw",
    "SlTpGmCrTxE",
    "8AbFGYob9SU",
    "UUqmpEOc5oc",
    "Z32Y-D5kJTg",
    "94ff_90_Dzs",
    "F_G3PXJL2AM",
    "tiMqrpC6ZZY",
    "fku-kaaUStw",
)
MEMBERS_ONLY_EXCLUSION = "rKdcv4QvGig"
SPLITS = ("calibration", "scoring")
MINIMUM_SELECTION_DURATION_MS = 3_600_000
NOISE_LEVELS = {"clean", "light", "moderate", "heavy", "unknown"}
ANNOTATION_STATES = {"transcribed", "non_speech", "unintelligible"}
TEXT_STATES = {
    "verbatim",
    "contains_unintelligible_marker",
    "unintelligible",
    "non_speech",
}


class ContractError(ValueError):
    """A manifest violated the exact runtime contract."""


def _fail(path: str, message: str) -> None:
    raise ContractError(f"{path}: {message}")


def _object(value: object, path: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(path, "must be an object")
    actual = set(value)
    if actual != keys:
        missing = sorted(keys - actual)
        extra = sorted(actual - keys)
        details: list[str] = []
        if missing:
            details.append(f"missing {missing}")
        if extra:
            details.append(f"unexpected {extra}")
        _fail(path, "; ".join(details))
    return value


def _array(value: object, path: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(path, "must be an array")
    return value


def _string(
    value: object,
    path: str,
    *,
    pattern: re.Pattern[str] | None = None,
    nonempty: bool = True,
) -> str:
    if not isinstance(value, str):
        _fail(path, "must be a string")
    if nonempty and not value:
        _fail(path, "must not be empty")
    if pattern is not None and pattern.fullmatch(value) is None:
        _fail(path, f"has invalid format: {value!r}")
    return value


def _integer(value: object, path: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(path, "must be an integer")
    if value < minimum:
        _fail(path, f"must be >= {minimum}")
    return value


def _boolean(value: object, path: str) -> bool:
    if not isinstance(value, bool):
        _fail(path, "must be a boolean")
    return value


def _constant(value: object, expected: object, path: str) -> None:
    if value != expected or type(value) is not type(expected):
        _fail(path, f"must equal {expected!r}")


def _choice(value: object, choices: set[str], path: str) -> str:
    text = _string(value, path)
    if text not in choices:
        _fail(path, f"must be one of {sorted(choices)}")
    return text


def _timestamp(value: object, path: str) -> datetime:
    text = _string(value, path, pattern=TIMESTAMP_RE)
    try:
        parsed = datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        _fail(path, f"must be a real UTC timestamp ({error})")
    return parsed


def _sha256(value: object, path: str) -> str:
    return _string(value, path, pattern=SHA256_RE)


def _id(value: object, path: str) -> str:
    return _string(value, path, pattern=OPAQUE_ID_RE)


def _stable_id(prefix: str, *parts: object) -> str:
    key = "\x1f".join(str(part) for part in parts)
    return f"{prefix}_{uuid.uuid5(NAMESPACE, key).hex}"


def _expected_source_id(native_id: str) -> str:
    return _stable_id("src", "youtube", "youtube_video", native_id)


def _expected_recording_id(native_id: str) -> str:
    return _stable_id("rec", f"youtube:video:{native_id}")


def _expected_rendition_id(recording_id: str, media_id: str, kind: str) -> str:
    return _stable_id("rnd", recording_id, media_id, kind)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_manifest_sha256(manifest: dict[str, Any]) -> str:
    """Hash canonical JSON after removing the root ``manifest_sha256`` field."""

    unsigned = dict(manifest)
    unsigned.pop("manifest_sha256", None)
    return hashlib.sha256(_canonical_bytes(unsigned)).hexdigest()


def _verify_manifest_digest(manifest: dict[str, Any], path: str = "$") -> None:
    declared = _sha256(manifest.get("manifest_sha256"), f"{path}.manifest_sha256")
    expected = canonical_manifest_sha256(manifest)
    if declared != expected:
        _fail(
            f"{path}.manifest_sha256",
            f"canonical digest mismatch; expected {expected}",
        )


def load_json(path: str | Path) -> dict[str, Any]:
    resolved = Path(path)

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, item in pairs:
            if key in output:
                raise ContractError(f"{resolved}: duplicate JSON object key {key!r}")
            output[key] = item
        return output

    def reject_constant(value: str) -> None:
        raise ContractError(f"{resolved}: non-finite JSON number {value!r} is forbidden")

    try:
        value = json.loads(
            resolved.read_text(encoding="utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(f"Cannot load JSON {resolved}: {error}") from error
    if not isinstance(value, dict):
        raise ContractError(f"{resolved}: top level must be an object")
    return value


def _validate_schema_header(
    manifest: dict[str, Any], *, kind: str, path: str = "$"
) -> None:
    _constant(manifest["schema_version"], SCHEMA_VERSION, f"{path}.schema_version")
    _constant(manifest["manifest_kind"], kind, f"{path}.manifest_kind")


def validate_candidate_cohort(value: object) -> dict[str, Any]:
    """Validate an unfrozen 12-recording candidate cohort."""

    cohort = _object(
        value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "cohort_id",
            "state",
            "created_at",
            "purpose",
            "expected_recording_count",
            "selection_policy",
            "candidates",
            "exclusions",
        },
    )
    _validate_schema_header(cohort, kind="candidate_cohort")
    cohort_id = _id(cohort["cohort_id"], "$.cohort_id")
    _constant(cohort["state"], "candidate_unfrozen", "$.state")
    _timestamp(cohort["created_at"], "$.created_at")
    _constant(cohort["purpose"], "asr_transcript_evaluation", "$.purpose")
    _constant(cohort["expected_recording_count"], 12, "$.expected_recording_count")

    policy = _object(
        cohort["selection_policy"],
        "$.selection_policy",
        {
            "candidate_basis",
            "asr_outputs_may_be_inspected_for_selection",
            "reference_freeze_required_before_system_output_inspection",
            "split_unit",
        },
    )
    _constant(
        policy["candidate_basis"],
        "public_source_locators_only",
        "$.selection_policy.candidate_basis",
    )
    _constant(
        policy["asr_outputs_may_be_inspected_for_selection"],
        False,
        "$.selection_policy.asr_outputs_may_be_inspected_for_selection",
    )
    _constant(
        policy["reference_freeze_required_before_system_output_inspection"],
        True,
        "$.selection_policy.reference_freeze_required_before_system_output_inspection",
    )
    _constant(policy["split_unit"], "recording", "$.selection_policy.split_unit")

    candidates = _array(cohort["candidates"], "$.candidates")
    if len(candidates) != 12:
        _fail("$.candidates", "must contain exactly 12 candidates")
    candidate_ids: set[str] = set()
    native_ids: set[str] = set()
    source_ids: set[str] = set()
    recording_ids: set[str] = set()
    for index, raw in enumerate(candidates):
        path = f"$.candidates[{index}]"
        row = _object(
            raw,
            path,
            {
                "candidate_id",
                "recording_id",
                "source_id",
                "platform",
                "source_kind",
                "native_id",
                "public_locator",
                "eligibility_state",
            },
        )
        native_id = _string(row["native_id"], f"{path}.native_id", pattern=YOUTUBE_ID_RE)
        candidate_id = _string(
            row["candidate_id"], f"{path}.candidate_id", pattern=OPAQUE_ID_RE
        )
        expected_candidate_id = f"candidate_youtube_{native_id}"
        if candidate_id != expected_candidate_id:
            _fail(f"{path}.candidate_id", f"must equal {expected_candidate_id!r}")
        _constant(row["platform"], "youtube", f"{path}.platform")
        _constant(row["source_kind"], "youtube_video", f"{path}.source_kind")
        source_id = _string(row["source_id"], f"{path}.source_id", pattern=UUID_ID_RE)
        recording_id = _string(
            row["recording_id"], f"{path}.recording_id", pattern=UUID_ID_RE
        )
        if source_id != _expected_source_id(native_id):
            _fail(f"{path}.source_id", "does not match the deterministic YouTube source ID")
        if recording_id != _expected_recording_id(native_id):
            _fail(
                f"{path}.recording_id",
                "does not match the deterministic YouTube recording ID",
            )
        expected_url = f"https://www.youtube.com/watch?v={native_id}"
        if row["public_locator"] != expected_url:
            _fail(f"{path}.public_locator", f"must equal {expected_url!r}")
        _choice(
            row["eligibility_state"],
            {"unresolved", "eligible", "ineligible"},
            f"{path}.eligibility_state",
        )
        for seen, item, label in (
            (candidate_ids, candidate_id, "candidate_id"),
            (native_ids, native_id, "native_id"),
            (source_ids, source_id, "source_id"),
            (recording_ids, recording_id, "recording_id"),
        ):
            if item in seen:
                _fail(path, f"duplicate {label} {item!r}")
            seen.add(item)

    exclusions = _array(cohort["exclusions"], "$.exclusions")
    excluded_ids: set[str] = set()
    for index, raw in enumerate(exclusions):
        path = f"$.exclusions[{index}]"
        row = _object(raw, path, {"native_id", "reason_code", "note"})
        native_id = _string(row["native_id"], f"{path}.native_id", pattern=YOUTUBE_ID_RE)
        if native_id in excluded_ids:
            _fail(f"{path}.native_id", "must be unique")
        excluded_ids.add(native_id)
        _choice(
            row["reason_code"],
            {"access_controlled", "private", "removed", "out_of_scope"},
            f"{path}.reason_code",
        )
        _string(row["note"], f"{path}.note")
        if native_id in native_ids:
            _fail(f"{path}.native_id", "cannot also appear in candidates")

    if cohort_id == "himr_asr_candidate_cohort_v1":
        if native_ids != set(CANDIDATE_V1_NATIVE_IDS):
            _fail("$.candidates", "does not match the named HIMR candidate cohort v1")
        if MEMBERS_ONLY_EXCLUSION not in excluded_ids:
            _fail(
                "$.exclusions",
                f"must explicitly exclude members-only {MEMBERS_ONLY_EXCLUSION}",
            )
        for index, raw in enumerate(exclusions):
            if raw["native_id"] == MEMBERS_ONLY_EXCLUSION and raw["reason_code"] != "access_controlled":
                _fail(
                    f"$.exclusions[{index}].reason_code",
                    "members-only source must be access_controlled",
                )

    _verify_manifest_digest(cohort)
    return cohort


def acquisition_selection(value: object) -> dict[str, Any]:
    """Project one validated, public, unfrozen cohort into queue selection JSON.

    Candidate order is preserved so the queue planner can apply its own stable
    priority rules without duplicating cohort validation or identity derivation.
    """

    cohort = validate_candidate_cohort(value)
    candidates = cohort["candidates"]
    for index, candidate in enumerate(candidates):
        if candidate["eligibility_state"] == "ineligible":
            _fail(
                f"$.candidates[{index}].eligibility_state",
                "ineligible candidates cannot enter an acquisition queue",
            )
        if candidate["platform"] != "youtube" or candidate["source_kind"] != "youtube_video":
            _fail(f"$.candidates[{index}]", "only public YouTube video candidates are supported")
        expected_url = f"https://www.youtube.com/watch?v={candidate['native_id']}"
        if candidate["public_locator"] != expected_url:
            _fail(f"$.candidates[{index}].public_locator", "must be a public YouTube watch URL")
    return {
        "schema_version": 1,
        "purpose": "transcript_evaluation_candidate_acquisition",
        "youtube_video_ids": [row["native_id"] for row in candidates],
        "source_ids": [row["source_id"] for row in candidates],
        "recording_ids": [row["recording_id"] for row in candidates],
    }


def _validate_flags(value: object, path: str) -> tuple[tuple[str, ...], bool, bool, bool, str]:
    flags = _object(
        value,
        path,
        {
            "language_tags",
            "code_switch",
            "speaker_overlap",
            "playback_speech",
            "noise",
        },
    )
    languages = _array(flags["language_tags"], f"{path}.language_tags")
    if not languages:
        _fail(f"{path}.language_tags", "must contain at least one language tag")
    parsed: list[str] = []
    for index, language in enumerate(languages):
        parsed.append(
            _string(
                language,
                f"{path}.language_tags[{index}]",
                pattern=LANGUAGE_RE,
            )
        )
    if len(set(parsed)) != len(parsed) or parsed != sorted(parsed):
        _fail(f"{path}.language_tags", "must be unique and sorted")
    code_switch = _boolean(flags["code_switch"], f"{path}.code_switch")
    if code_switch and len(parsed) < 2:
        _fail(f"{path}.language_tags", "code_switch=true requires at least two tags")
    overlap = _boolean(flags["speaker_overlap"], f"{path}.speaker_overlap")
    playback = _boolean(flags["playback_speech"], f"{path}.playback_speech")
    noise = _choice(flags["noise"], NOISE_LEVELS, f"{path}.noise")
    return tuple(parsed), code_switch, overlap, playback, noise


def _validate_interval_lineage(row: dict[str, Any], path: str) -> None:
    _id(row["interval_id"], f"{path}.interval_id")
    _string(row["recording_id"], f"{path}.recording_id", pattern=UUID_ID_RE)
    _string(row["source_id"], f"{path}.source_id", pattern=UUID_ID_RE)
    digest = _sha256(row["media_sha256"], f"{path}.media_sha256")
    expected_media = f"media_sha256_{digest}"
    if row["media_id"] != expected_media:
        _fail(f"{path}.media_id", f"must equal {expected_media!r}")
    _string(row["rendition_id"], f"{path}.rendition_id", pattern=UUID_ID_RE)
    _choice(row["split"], set(SPLITS), f"{path}.split")
    start = _integer(row["start_ms"], f"{path}.start_ms")
    end = _integer(row["end_ms"], f"{path}.end_ms")
    if end <= start:
        _fail(path, "interval must be a non-empty half-open [start_ms, end_ms) range")


def _freeze_interval_lookup(freeze: dict[str, Any]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for recording in freeze["recordings"]:
        for interval in recording["intervals"]:
            output[interval["interval_id"]] = {
                "interval_id": interval["interval_id"],
                "recording_id": recording["recording_id"],
                "source_id": recording["source_id"],
                "media_id": recording["media_id"],
                "media_sha256": recording["media_sha256"],
                "rendition_id": recording["rendition_id"],
                "split": recording["split"],
                "start_ms": interval["start_ms"],
                "end_ms": interval["end_ms"],
            }
    return output


def _validate_freeze_selection_provenance(
    value: object, path: str = "$.selection_provenance"
) -> dict[str, Any]:
    provenance = _object(
        value,
        path,
        {"review_id", "review_manifest_sha256", "proposal_inputs"},
    )
    _string(
        provenance["review_id"],
        f"{path}.review_id",
        pattern=SELECTION_REVIEW_ID_RE,
    )
    _sha256(provenance["review_manifest_sha256"], f"{path}.review_manifest_sha256")
    inputs = _array(provenance["proposal_inputs"], f"{path}.proposal_inputs")
    if len(inputs) != 2:
        _fail(f"{path}.proposal_inputs", "must contain exactly two proposal inputs")
    seen_request_ids: set[str] = set()
    seen_proposal_ids: set[str] = set()
    seen_request_hashes: set[str] = set()
    seen_proposal_hashes: set[str] = set()
    for index, raw in enumerate(inputs):
        item_path = f"{path}.proposal_inputs[{index}]"
        row = _object(
            raw,
            item_path,
            {
                "ordinal",
                "request_schema_version",
                "request_id",
                "request_manifest_sha256",
                "proposal_schema_version",
                "proposal_id",
                "proposal_manifest_sha256",
            },
        )
        _constant(row["ordinal"], index + 1, f"{item_path}.ordinal")
        request_version = _integer(
            row["request_schema_version"], f"{item_path}.request_schema_version", minimum=1
        )
        proposal_version = _integer(
            row["proposal_schema_version"], f"{item_path}.proposal_schema_version", minimum=1
        )
        if request_version not in {1, 2} or proposal_version not in {1, 2}:
            _fail(item_path, "request/proposal schema versions must be 1 or 2")
        if request_version != proposal_version:
            _fail(item_path, "request/proposal schema versions must match")
        request_id = _id(row["request_id"], f"{item_path}.request_id")
        proposal_id = _id(row["proposal_id"], f"{item_path}.proposal_id")
        request_hash = _sha256(
            row["request_manifest_sha256"], f"{item_path}.request_manifest_sha256"
        )
        proposal_hash = _sha256(
            row["proposal_manifest_sha256"], f"{item_path}.proposal_manifest_sha256"
        )
        for seen, item, field in (
            (seen_request_ids, request_id, "request_id"),
            (seen_proposal_ids, proposal_id, "proposal_id"),
            (seen_request_hashes, request_hash, "request_manifest_sha256"),
            (seen_proposal_hashes, proposal_hash, "proposal_manifest_sha256"),
        ):
            if item in seen:
                _fail(f"{item_path}.{field}", "must be unique")
            seen.add(item)
    return provenance


def validate_interval_freeze(
    value: object, candidate_cohort: object
) -> dict[str, Any]:
    """Validate a sealed interval selection against its exact candidate cohort."""

    cohort = validate_candidate_cohort(candidate_cohort)
    if not isinstance(value, dict):
        _fail("$", "must be an object")
    schema_version = value.get("schema_version")
    if schema_version not in {1, 2} or isinstance(schema_version, bool):
        _fail("$.schema_version", "must equal 1 or 2")
    root_keys = {
        "schema_version",
        "manifest_kind",
        "manifest_sha256",
        "freeze_id",
        "cohort_id",
        "cohort_manifest_sha256",
        "created_at",
        "frozen_at",
        "selection_attestation",
        "split_policy",
        "reference_state",
        "recordings",
        "accounting",
    }
    if schema_version == 2:
        root_keys.add("selection_provenance")
    freeze = _object(
        value,
        "$",
        root_keys,
    )
    _constant(freeze["schema_version"], schema_version, "$.schema_version")
    _constant(freeze["manifest_kind"], "interval_freeze", "$.manifest_kind")
    freeze_id = _id(freeze["freeze_id"], "$.freeze_id")
    selection_provenance: dict[str, Any] | None = None
    proposal_ids: set[str] = set()
    if schema_version == 2:
        selection_provenance = _validate_freeze_selection_provenance(
            freeze["selection_provenance"]
        )
        proposal_ids = {
            row["proposal_id"] for row in selection_provenance["proposal_inputs"]
        }
    if freeze["cohort_id"] != cohort["cohort_id"]:
        _fail("$.cohort_id", "does not match the candidate cohort")
    if freeze["cohort_manifest_sha256"] != cohort["manifest_sha256"]:
        _fail("$.cohort_manifest_sha256", "does not bind the exact candidate cohort")
    created_at = _timestamp(freeze["created_at"], "$.created_at")
    frozen_at = _timestamp(freeze["frozen_at"], "$.frozen_at")
    if created_at > frozen_at:
        _fail("$.frozen_at", "must not precede created_at")
    if schema_version == 2 and created_at != frozen_at:
        _fail("$.created_at", "must equal frozen_at for a version-2 compile event")

    attestation = _object(
        freeze["selection_attestation"],
        "$.selection_attestation",
        {
            "selected_without_asr_output_inspection",
            "attestor_id",
            "attested_at",
            "selection_basis",
            "protocol_revision",
        },
    )
    _constant(
        attestation["selected_without_asr_output_inspection"],
        True,
        "$.selection_attestation.selected_without_asr_output_inspection",
    )
    _id(attestation["attestor_id"], "$.selection_attestation.attestor_id")
    attested_at = _timestamp(attestation["attested_at"], "$.selection_attestation.attested_at")
    if attested_at > frozen_at:
        _fail("$.selection_attestation.attested_at", "must not follow frozen_at")
    _constant(
        attestation["selection_basis"],
        "source_metadata_and_direct_media_only",
        "$.selection_attestation.selection_basis",
    )
    _id(attestation["protocol_revision"], "$.selection_attestation.protocol_revision")
    if schema_version == 2:
        assert selection_provenance is not None
        expected_freeze_id = _stable_id(
            "freeze",
            freeze["cohort_manifest_sha256"],
            selection_provenance["review_manifest_sha256"],
            attestation["protocol_revision"],
            freeze["frozen_at"],
        )
        if freeze_id != expected_freeze_id:
            _fail("$.freeze_id", f"must equal deterministic ID {expected_freeze_id!r}")

    policy = _object(
        freeze["split_policy"],
        "$.split_policy",
        {
            "unit",
            "no_segment_leakage",
            "scoring_and_calibration_disjoint",
            "stratification_dimensions",
        },
    )
    _constant(policy["unit"], "recording", "$.split_policy.unit")
    _constant(policy["no_segment_leakage"], True, "$.split_policy.no_segment_leakage")
    _constant(
        policy["scoring_and_calibration_disjoint"],
        True,
        "$.split_policy.scoring_and_calibration_disjoint",
    )
    dimensions = _array(
        policy["stratification_dimensions"],
        "$.split_policy.stratification_dimensions",
    )
    expected_dimensions = [
        "language",
        "code_switch",
        "speaker_overlap",
        "playback_speech",
        "noise",
    ]
    if dimensions != expected_dimensions:
        _fail(
            "$.split_policy.stratification_dimensions",
            f"must equal {expected_dimensions!r}",
        )
    _constant(
        freeze["reference_state"],
        "interval_selection_only_no_reference_text",
        "$.reference_state",
    )

    candidates_by_id = {row["candidate_id"]: row for row in cohort["candidates"]}
    recordings = _array(freeze["recordings"], "$.recordings")
    if len(recordings) != 12:
        _fail("$.recordings", "must contain exactly 12 recordings")
    seen_candidates: set[str] = set()
    seen_recordings: set[str] = set()
    seen_sources: set[str] = set()
    seen_media: set[str] = set()
    seen_digests: set[str] = set()
    seen_intervals: set[str] = set()
    seen_proposal_intervals: set[str] = set()
    seen_selection_decisions: set[str] = set()
    split_recordings: defaultdict[str, set[str]] = defaultdict(set)
    split_intervals: defaultdict[str, int] = defaultdict(int)
    split_duration: defaultdict[str, int] = defaultdict(int)
    stratum_signature: dict[str, tuple[tuple[str, ...], bool, bool, bool, str]] = {}
    stratum_recordings: defaultdict[str, set[str]] = defaultdict(set)
    stratum_intervals: defaultdict[str, int] = defaultdict(int)
    stratum_duration: defaultdict[str, int] = defaultdict(int)
    total_duration = 0

    for recording_index, raw in enumerate(recordings):
        path = f"$.recordings[{recording_index}]"
        row = _object(
            raw,
            path,
            {
                "candidate_id",
                "recording_id",
                "source_id",
                "source_native_id",
                "source_locator",
                "media_id",
                "media_sha256",
                "media_byte_count",
                "media_duration_ms",
                "rendition_id",
                "rendition_kind",
                "timeline_coordinate_system",
                "split",
                "intervals",
            },
        )
        candidate_id = _id(row["candidate_id"], f"{path}.candidate_id")
        candidate = candidates_by_id.get(candidate_id)
        if candidate is None:
            _fail(f"{path}.candidate_id", "does not exist in candidate cohort")
        if candidate_id in seen_candidates:
            _fail(f"{path}.candidate_id", "must appear exactly once")
        seen_candidates.add(candidate_id)
        recording_id = _string(row["recording_id"], f"{path}.recording_id", pattern=UUID_ID_RE)
        source_id = _string(row["source_id"], f"{path}.source_id", pattern=UUID_ID_RE)
        native_id = _string(row["source_native_id"], f"{path}.source_native_id", pattern=YOUTUBE_ID_RE)
        for field in ("recording_id", "source_id"):
            if row[field] != candidate[field]:
                _fail(f"{path}.{field}", "does not match its candidate cohort row")
        if native_id != candidate["native_id"]:
            _fail(f"{path}.source_native_id", "does not match its candidate cohort row")
        if row["source_locator"] != candidate["public_locator"]:
            _fail(f"{path}.source_locator", "does not match its candidate cohort row")
        if recording_id in seen_recordings:
            _fail(f"{path}.recording_id", "duplicate recording would leak across splits")
        if source_id in seen_sources:
            _fail(f"{path}.source_id", "must be unique across the freeze")
        seen_recordings.add(recording_id)
        seen_sources.add(source_id)
        digest = _sha256(row["media_sha256"], f"{path}.media_sha256")
        media_id = f"media_sha256_{digest}"
        if row["media_id"] != media_id:
            _fail(f"{path}.media_id", f"must equal {media_id!r}")
        if media_id in seen_media or digest in seen_digests:
            _fail(
                f"{path}.media_sha256",
                "exact media bytes cannot occur in two evaluation recordings",
            )
        seen_media.add(media_id)
        seen_digests.add(digest)
        _integer(row["media_byte_count"], f"{path}.media_byte_count", minimum=1)
        media_duration = _integer(
            row["media_duration_ms"], f"{path}.media_duration_ms", minimum=1
        )
        kind = _id(row["rendition_kind"], f"{path}.rendition_kind")
        rendition_id = _string(row["rendition_id"], f"{path}.rendition_id", pattern=UUID_ID_RE)
        expected_rendition = _expected_rendition_id(recording_id, media_id, kind)
        if rendition_id != expected_rendition:
            _fail(
                f"{path}.rendition_id",
                "does not match recording, exact media bytes, and rendition kind",
            )
        _constant(
            row["timeline_coordinate_system"],
            "rendition_media_ms",
            f"{path}.timeline_coordinate_system",
        )
        split = _choice(row["split"], set(SPLITS), f"{path}.split")
        split_recordings[split].add(recording_id)
        intervals = _array(row["intervals"], f"{path}.intervals")
        if not intervals:
            _fail(f"{path}.intervals", "must contain at least one interval")
        previous_end = -1
        for interval_index, interval_raw in enumerate(intervals):
            interval_path = f"{path}.intervals[{interval_index}]"
            interval_keys = {"interval_id", "start_ms", "end_ms", "stratum_id", "flags"}
            if schema_version == 2:
                interval_keys.update(
                    {"proposal_id", "proposal_interval_id", "selection_decision_id"}
                )
            interval = _object(
                interval_raw,
                interval_path,
                interval_keys,
            )
            interval_id = _id(interval["interval_id"], f"{interval_path}.interval_id")
            if interval_id in seen_intervals:
                _fail(f"{interval_path}.interval_id", "must be globally unique")
            seen_intervals.add(interval_id)
            start = _integer(interval["start_ms"], f"{interval_path}.start_ms")
            end = _integer(interval["end_ms"], f"{interval_path}.end_ms")
            if end <= start:
                _fail(interval_path, "must be a non-empty half-open interval")
            if start < previous_end:
                _fail(interval_path, "must be sorted and nonoverlapping within recording")
            if end > media_duration:
                _fail(f"{interval_path}.end_ms", "exceeds media_duration_ms")
            previous_end = end
            if schema_version == 2:
                assert selection_provenance is not None
                proposal_id = _id(
                    interval["proposal_id"], f"{interval_path}.proposal_id"
                )
                if proposal_id not in proposal_ids:
                    _fail(
                        f"{interval_path}.proposal_id",
                        "does not exist in selection provenance",
                    )
                proposal_interval_id = _id(
                    interval["proposal_interval_id"],
                    f"{interval_path}.proposal_interval_id",
                )
                selection_decision_id = _id(
                    interval["selection_decision_id"],
                    f"{interval_path}.selection_decision_id",
                )
                if proposal_interval_id in seen_proposal_intervals:
                    _fail(
                        f"{interval_path}.proposal_interval_id",
                        "must be globally unique",
                    )
                if selection_decision_id in seen_selection_decisions:
                    _fail(
                        f"{interval_path}.selection_decision_id",
                        "must be globally unique",
                    )
                seen_proposal_intervals.add(proposal_interval_id)
                seen_selection_decisions.add(selection_decision_id)
                expected_decision_id = _stable_id(
                    "selection_decision",
                    selection_provenance["review_id"],
                    proposal_id,
                    proposal_interval_id,
                )
                if selection_decision_id != expected_decision_id:
                    _fail(
                        f"{interval_path}.selection_decision_id",
                        f"must equal deterministic ID {expected_decision_id!r}",
                    )
                expected_interval_id = _stable_id(
                    "interval",
                    selection_provenance["review_manifest_sha256"],
                    proposal_id,
                    proposal_interval_id,
                    start,
                    end,
                )
                if interval_id != expected_interval_id:
                    _fail(
                        f"{interval_path}.interval_id",
                        f"must equal deterministic ID {expected_interval_id!r}",
                    )
            duration = end - start
            stratum = _id(interval["stratum_id"], f"{interval_path}.stratum_id")
            signature = _validate_flags(interval["flags"], f"{interval_path}.flags")
            if schema_version == 2:
                expected_stratum_id = _stable_id(
                    "stratum",
                    hashlib.sha256(_canonical_bytes(interval["flags"])).hexdigest(),
                )
                if stratum != expected_stratum_id:
                    _fail(
                        f"{interval_path}.stratum_id",
                        f"must equal deterministic ID {expected_stratum_id!r}",
                    )
            previous_signature = stratum_signature.setdefault(stratum, signature)
            if previous_signature != signature:
                _fail(
                    f"{interval_path}.stratum_id",
                    "one stratum_id cannot represent different flag combinations",
                )
            split_intervals[split] += 1
            split_duration[split] += duration
            stratum_recordings[stratum].add(recording_id)
            stratum_intervals[stratum] += 1
            stratum_duration[stratum] += duration
            total_duration += duration

    if seen_candidates != set(candidates_by_id):
        missing = sorted(set(candidates_by_id) - seen_candidates)
        _fail("$.recordings", f"must freeze every candidate exactly once; missing {missing}")
    for split in SPLITS:
        if not split_recordings[split]:
            _fail("$.recordings", f"split {split!r} must contain at least one recording")
    if split_recordings["calibration"] & split_recordings["scoring"]:
        _fail("$.recordings", "recording leakage between calibration and scoring")
    if schema_version == 2 and total_duration < MINIMUM_SELECTION_DURATION_MS:
        shortfall = MINIMUM_SELECTION_DURATION_MS - total_duration
        _fail(
            "$.recordings",
            f"accepted duration {total_duration} ms is below minimum "
            f"{MINIMUM_SELECTION_DURATION_MS} ms; shortfall {shortfall} ms",
        )

    accounting = _object(
        freeze["accounting"],
        "$.accounting",
        {"recording_count", "interval_count", "total_duration_ms", "splits", "strata"},
    )
    _constant(accounting["recording_count"], len(recordings), "$.accounting.recording_count")
    _constant(accounting["interval_count"], len(seen_intervals), "$.accounting.interval_count")
    _constant(accounting["total_duration_ms"], total_duration, "$.accounting.total_duration_ms")
    split_rows = _array(accounting["splits"], "$.accounting.splits")
    if len(split_rows) != 2:
        _fail("$.accounting.splits", "must contain calibration and scoring rows")
    if [row.get("split") if isinstance(row, dict) else None for row in split_rows] != list(SPLITS):
        _fail("$.accounting.splits", f"must be ordered as {list(SPLITS)!r}")
    for index, raw in enumerate(split_rows):
        path = f"$.accounting.splits[{index}]"
        row = _object(raw, path, {"split", "recording_count", "interval_count", "duration_ms"})
        split = _choice(row["split"], set(SPLITS), f"{path}.split")
        _constant(row["recording_count"], len(split_recordings[split]), f"{path}.recording_count")
        _constant(row["interval_count"], split_intervals[split], f"{path}.interval_count")
        _constant(row["duration_ms"], split_duration[split], f"{path}.duration_ms")

    stratum_rows = _array(accounting["strata"], "$.accounting.strata")
    if len(stratum_rows) != len(stratum_signature):
        _fail("$.accounting.strata", "must have exactly one row per observed stratum")
    names = [row.get("stratum_id") if isinstance(row, dict) else None for row in stratum_rows]
    if names != sorted(stratum_signature):
        _fail("$.accounting.strata", "must be sorted by stratum_id")
    for index, raw in enumerate(stratum_rows):
        path = f"$.accounting.strata[{index}]"
        row = _object(
            raw,
            path,
            {"stratum_id", "flags", "recording_count", "interval_count", "duration_ms"},
        )
        stratum = _id(row["stratum_id"], f"{path}.stratum_id")
        signature = _validate_flags(row["flags"], f"{path}.flags")
        if stratum_signature.get(stratum) != signature:
            _fail(f"{path}.flags", "does not match interval flags for this stratum")
        _constant(row["recording_count"], len(stratum_recordings[stratum]), f"{path}.recording_count")
        _constant(row["interval_count"], stratum_intervals[stratum], f"{path}.interval_count")
        _constant(row["duration_ms"], stratum_duration[stratum], f"{path}.duration_ms")

    _verify_manifest_digest(freeze)
    return freeze


def _validate_publication(value: object, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(path, "must be an object")
    status = value.get("status")
    if status == "withheld":
        publication = _object(value, path, {"status", "storage_policy"})
        _constant(publication["storage_policy"], "private_only", f"{path}.storage_policy")
    elif status == "released":
        publication = _object(
            value,
            path,
            {"status", "decision_id", "decided_by", "decided_at", "basis"},
        )
        _id(publication["decision_id"], f"{path}.decision_id")
        _id(publication["decided_by"], f"{path}.decided_by")
        _timestamp(publication["decided_at"], f"{path}.decided_at")
        _string(publication["basis"], f"{path}.basis")
    else:
        _fail(f"{path}.status", "must be withheld or deliberately released")
    return publication


def _validate_tool(value: object, path: str) -> None:
    tool = _object(value, path, {"name", "version"})
    _string(tool["name"], f"{path}.name")
    _string(tool["version"], f"{path}.version")


def _validate_utterances(
    value: object,
    *,
    path: str,
    interval_start: int,
    interval_end: int,
    annotation_state: str,
) -> set[str]:
    utterances = _array(value, path)
    if annotation_state == "transcribed" and not utterances:
        _fail(path, "transcribed interval must contain at least one utterance")
    if annotation_state in {"non_speech", "unintelligible"} and utterances:
        _fail(path, f"{annotation_state} interval must not contain utterances")
    ids: set[str] = set()
    speaker_ends: defaultdict[str, int] = defaultdict(lambda: -1)
    for index, raw in enumerate(utterances):
        item_path = f"{path}[{index}]"
        row = _object(
            raw,
            item_path,
            {
                "utterance_id",
                "start_ms",
                "end_ms",
                "speaker_label",
                "text",
                "text_state",
                "flags",
            },
        )
        utterance_id = _id(row["utterance_id"], f"{item_path}.utterance_id")
        if utterance_id in ids:
            _fail(f"{item_path}.utterance_id", "must be unique within interval")
        ids.add(utterance_id)
        start = _integer(row["start_ms"], f"{item_path}.start_ms")
        end = _integer(row["end_ms"], f"{item_path}.end_ms")
        if end <= start:
            _fail(item_path, "must be a non-empty half-open interval")
        if start < interval_start or end > interval_end:
            _fail(item_path, "must lie within its frozen interval")
        speaker = _id(row["speaker_label"], f"{item_path}.speaker_label")
        if start < speaker_ends[speaker]:
            _fail(item_path, "utterances for one speaker must be sorted and nonoverlapping")
        speaker_ends[speaker] = end
        text = _string(row["text"], f"{item_path}.text", nonempty=False)
        state = _choice(row["text_state"], TEXT_STATES, f"{item_path}.text_state")
        if state in {"unintelligible", "non_speech"} and text != "":
            _fail(f"{item_path}.text", f"must be empty when text_state is {state}")
        if state in {"verbatim", "contains_unintelligible_marker"} and text == "":
            _fail(f"{item_path}.text", f"must not be empty when text_state is {state}")
        _validate_flags(row["flags"], f"{item_path}.flags")
    return ids


def _validate_bound_interval(
    raw: object,
    *,
    path: str,
    frozen: dict[str, Any],
) -> tuple[dict[str, Any], set[str]]:
    row = _object(
        raw,
        path,
        {
            "interval_id",
            "recording_id",
            "source_id",
            "media_id",
            "media_sha256",
            "rendition_id",
            "split",
            "start_ms",
            "end_ms",
            "annotation_state",
            "utterances",
        },
    )
    _validate_interval_lineage(row, path)
    for field in (
        "interval_id",
        "recording_id",
        "source_id",
        "media_id",
        "media_sha256",
        "rendition_id",
        "split",
        "start_ms",
        "end_ms",
    ):
        if row[field] != frozen[field]:
            _fail(f"{path}.{field}", "does not match the exact frozen interval lineage")
    state = _choice(row["annotation_state"], ANNOTATION_STATES, f"{path}.annotation_state")
    utterance_ids = _validate_utterances(
        row["utterances"],
        path=f"{path}.utterances",
        interval_start=row["start_ms"],
        interval_end=row["end_ms"],
        annotation_state=state,
    )
    return row, utterance_ids


def validate_annotation(
    value: object,
    interval_freeze: object,
    candidate_cohort: object,
) -> dict[str, Any]:
    """Validate one ASR-blind, independent human reference pass."""

    freeze = validate_interval_freeze(interval_freeze, candidate_cohort)
    annotation = _object(
        value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "annotation_id",
            "freeze_id",
            "freeze_manifest_sha256",
            "pass_name",
            "annotator_id",
            "created_at",
            "independence_attestation",
            "annotation_tool",
            "publication",
            "intervals",
        },
    )
    _validate_schema_header(annotation, kind="reference_annotation")
    _id(annotation["annotation_id"], "$.annotation_id")
    if annotation["freeze_id"] != freeze["freeze_id"]:
        _fail("$.freeze_id", "does not match interval freeze")
    if annotation["freeze_manifest_sha256"] != freeze["manifest_sha256"]:
        _fail("$.freeze_manifest_sha256", "does not bind exact interval freeze")
    _choice(annotation["pass_name"], {"pass_a", "pass_b"}, "$.pass_name")
    annotator_id = _id(annotation["annotator_id"], "$.annotator_id")
    created_at = _timestamp(annotation["created_at"], "$.created_at")
    freeze_at = _timestamp(freeze["frozen_at"], "$.freeze.frozen_at")
    if created_at < freeze_at:
        _fail("$.created_at", "cannot precede interval freeze")
    attestation = _object(
        annotation["independence_attestation"],
        "$.independence_attestation",
        {
            "attestor_id",
            "attested_at",
            "direct_media_reviewed",
            "asr_outputs_inspected",
            "other_reference_pass_inspected",
            "adjudication_inspected",
        },
    )
    if attestation["attestor_id"] != annotator_id:
        _fail("$.independence_attestation.attestor_id", "must equal annotator_id")
    attested_at = _timestamp(attestation["attested_at"], "$.independence_attestation.attested_at")
    if attested_at < freeze_at or attested_at > created_at:
        _fail(
            "$.independence_attestation.attested_at",
            "must fall between frozen_at and annotation created_at",
        )
    _constant(
        attestation["direct_media_reviewed"],
        True,
        "$.independence_attestation.direct_media_reviewed",
    )
    for field in (
        "asr_outputs_inspected",
        "other_reference_pass_inspected",
        "adjudication_inspected",
    ):
        _constant(attestation[field], False, f"$.independence_attestation.{field}")
    _validate_tool(annotation["annotation_tool"], "$.annotation_tool")
    publication = _validate_publication(annotation["publication"], "$.publication")
    if publication["status"] == "released":
        decided_at = _timestamp(publication["decided_at"], "$.publication.decided_at")
        if decided_at < created_at:
            _fail("$.publication.decided_at", "cannot precede annotation created_at")

    frozen_intervals = _freeze_interval_lookup(freeze)
    intervals = _array(annotation["intervals"], "$.intervals")
    if len(intervals) != len(frozen_intervals):
        _fail("$.intervals", "must annotate every frozen interval exactly once")
    seen: set[str] = set()
    for index, raw in enumerate(intervals):
        path = f"$.intervals[{index}]"
        if not isinstance(raw, dict):
            _fail(path, "must be an object")
        interval_id = raw.get("interval_id")
        if not isinstance(interval_id, str) or interval_id not in frozen_intervals:
            _fail(f"{path}.interval_id", "does not exist in interval freeze")
        if interval_id in seen:
            _fail(f"{path}.interval_id", "must be unique")
        seen.add(interval_id)
        _validate_bound_interval(raw, path=path, frozen=frozen_intervals[interval_id])
    if seen != set(frozen_intervals):
        _fail("$.intervals", "must cover the exact frozen interval set")
    _verify_manifest_digest(annotation)
    return annotation


def validate_adjudication(
    value: object,
    interval_freeze: object,
    candidate_cohort: object,
    pass_a_value: object,
    pass_b_value: object,
) -> dict[str, Any]:
    """Validate an adjudicated reference against two independent passes."""

    freeze = validate_interval_freeze(interval_freeze, candidate_cohort)
    pass_a = validate_annotation(pass_a_value, freeze, candidate_cohort)
    pass_b = validate_annotation(pass_b_value, freeze, candidate_cohort)
    passes = {pass_a["pass_name"]: pass_a, pass_b["pass_name"]: pass_b}
    if set(passes) != {"pass_a", "pass_b"}:
        _fail("$.inputs", "requires exactly one pass_a and one pass_b annotation")
    if pass_a["annotator_id"] == pass_b["annotator_id"]:
        _fail("$.inputs", "pass_a and pass_b must have different annotators")
    if pass_a["annotation_id"] == pass_b["annotation_id"]:
        _fail("$.inputs", "pass_a and pass_b must have different annotation IDs")

    adjudication = _object(
        value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "adjudication_id",
            "freeze_id",
            "freeze_manifest_sha256",
            "reference_state",
            "created_at",
            "inputs",
            "adjudicator",
            "publication",
            "intervals",
        },
    )
    _validate_schema_header(adjudication, kind="reference_adjudication")
    _id(adjudication["adjudication_id"], "$.adjudication_id")
    if adjudication["freeze_id"] != freeze["freeze_id"]:
        _fail("$.freeze_id", "does not match interval freeze")
    if adjudication["freeze_manifest_sha256"] != freeze["manifest_sha256"]:
        _fail("$.freeze_manifest_sha256", "does not bind exact interval freeze")
    _constant(
        adjudication["reference_state"],
        "adjudicated_human_reference",
        "$.reference_state",
    )
    created_at = _timestamp(adjudication["created_at"], "$.created_at")

    inputs = _object(adjudication["inputs"], "$.inputs", {"pass_a", "pass_b"})
    for name in ("pass_a", "pass_b"):
        path = f"$.inputs.{name}"
        row = _object(inputs[name], path, {"annotation_id", "manifest_sha256", "annotator_id"})
        expected = passes[name]
        for field in ("annotation_id", "manifest_sha256", "annotator_id"):
            if row[field] != expected[field]:
                _fail(f"{path}.{field}", f"does not match exact {name} annotation")

    adjudicator = _object(
        adjudication["adjudicator"],
        "$.adjudicator",
        {
            "adjudicator_id",
            "adjudicated_at",
            "direct_media_reviewed",
            "compared_both_passes",
            "asr_outputs_inspected",
            "adjudication_tool",
        },
    )
    adjudicator_id = _id(adjudicator["adjudicator_id"], "$.adjudicator.adjudicator_id")
    if adjudicator_id in {pass_a["annotator_id"], pass_b["annotator_id"]}:
        _fail("$.adjudicator.adjudicator_id", "must be independent of both annotators")
    adjudicated_at = _timestamp(adjudicator["adjudicated_at"], "$.adjudicator.adjudicated_at")
    pass_times = [
        _timestamp(pass_a["created_at"], "$.pass_a.created_at"),
        _timestamp(pass_b["created_at"], "$.pass_b.created_at"),
    ]
    if adjudicated_at < max(pass_times) or created_at < adjudicated_at:
        _fail(
            "$.adjudicator.adjudicated_at",
            "must follow both passes and not follow adjudication created_at",
        )
    _constant(
        adjudicator["direct_media_reviewed"],
        True,
        "$.adjudicator.direct_media_reviewed",
    )
    _constant(
        adjudicator["compared_both_passes"],
        True,
        "$.adjudicator.compared_both_passes",
    )
    _constant(
        adjudicator["asr_outputs_inspected"],
        False,
        "$.adjudicator.asr_outputs_inspected",
    )
    _validate_tool(adjudicator["adjudication_tool"], "$.adjudicator.adjudication_tool")
    publication = _validate_publication(adjudication["publication"], "$.publication")
    if publication["status"] == "released":
        decided_at = _timestamp(publication["decided_at"], "$.publication.decided_at")
        if decided_at < created_at:
            _fail("$.publication.decided_at", "cannot precede adjudication created_at")

    frozen_intervals = _freeze_interval_lookup(freeze)
    intervals = _array(adjudication["intervals"], "$.intervals")
    if len(intervals) != len(frozen_intervals):
        _fail("$.intervals", "must adjudicate every frozen interval exactly once")
    pass_utterances: dict[str, dict[str, set[str]]] = {
        "pass_a": {},
        "pass_b": {},
    }
    for pass_name, annotation in (("pass_a", pass_a), ("pass_b", pass_b)):
        for interval in annotation["intervals"]:
            pass_utterances[pass_name][interval["interval_id"]] = {
                row["utterance_id"] for row in interval["utterances"]
            }
    seen: set[str] = set()
    for index, raw in enumerate(intervals):
        path = f"$.intervals[{index}]"
        row = _object(
            raw,
            path,
            {
                "interval_id",
                "recording_id",
                "source_id",
                "media_id",
                "media_sha256",
                "rendition_id",
                "split",
                "start_ms",
                "end_ms",
                "annotation_state",
                "utterances",
                "resolution",
                "source_utterance_ids",
                "decision_note",
            },
        )
        interval_id = row["interval_id"]
        if not isinstance(interval_id, str) or interval_id not in frozen_intervals:
            _fail(f"{path}.interval_id", "does not exist in interval freeze")
        if interval_id in seen:
            _fail(f"{path}.interval_id", "must be unique")
        seen.add(interval_id)
        bound_fields = {
            "interval_id",
            "recording_id",
            "source_id",
            "media_id",
            "media_sha256",
            "rendition_id",
            "split",
            "start_ms",
            "end_ms",
            "annotation_state",
            "utterances",
        }
        _validate_bound_interval(
            {key: row[key] for key in bound_fields},
            path=path,
            frozen=frozen_intervals[interval_id],
        )
        _choice(
            row["resolution"],
            {"agreement", "resolved_after_review", "uncertainty_retained", "non_speech"},
            f"{path}.resolution",
        )
        sources = _array(row["source_utterance_ids"], f"{path}.source_utterance_ids")
        source_ids: list[str] = []
        for source_index, source_id in enumerate(sources):
            source_ids.append(_id(source_id, f"{path}.source_utterance_ids[{source_index}]"))
        if len(source_ids) != len(set(source_ids)) or source_ids != sorted(source_ids):
            _fail(f"{path}.source_utterance_ids", "must be unique and sorted")
        all_pass_sources = (
            pass_utterances["pass_a"].get(interval_id, set())
            | pass_utterances["pass_b"].get(interval_id, set())
        )
        if not set(source_ids).issubset(all_pass_sources):
            _fail(
                f"{path}.source_utterance_ids",
                "may reference only utterances from the two bound passes",
            )
        if row["resolution"] in {"agreement", "resolved_after_review"} and not source_ids:
            _fail(
                f"{path}.source_utterance_ids",
                "must cite pass utterances for this resolution",
            )
        if row["resolution"] in {"agreement", "resolved_after_review"}:
            for pass_name in ("pass_a", "pass_b"):
                if not set(source_ids) & pass_utterances[pass_name].get(interval_id, set()):
                    _fail(
                        f"{path}.source_utterance_ids",
                        f"must cite at least one {pass_name} utterance for this resolution",
                    )
        _string(row["decision_note"], f"{path}.decision_note", nonempty=False)
    if seen != set(frozen_intervals):
        _fail("$.intervals", "must cover the exact frozen interval set")
    _verify_manifest_digest(adjudication)
    return adjudication


def audit_tracked_evaluation_data(repository_root: str | Path) -> list[str]:
    """Reject tracked private evaluation artifacts; allow deliberately released references."""

    root = Path(repository_root).resolve()
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise ContractError(f"Cannot enumerate tracked files: {error}") from error
    checked: list[str] = []
    for raw_name in result.stdout.split(b"\0"):
        if not raw_name or not raw_name.endswith(b".json"):
            continue
        try:
            relative = raw_name.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ContractError(f"Tracked filename is not UTF-8: {raw_name!r}") from error
        path = root / relative
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(value, dict):
            continue
        private_kind = value.get("manifest_kind")
        if private_kind in {
            "interval_selection_review",
            "interval_selection_draft",
            "interval_selection_workspace",
            "interval_selection_finalization_receipt",
            "transcript_system_output",
            "transcript_score_report",
            "gpu_asr_evaluation_measurement",
            "transcript_system_comparison",
        }:
            if private_kind == "interval_selection_review":
                label = "selection review"
            elif private_kind == "transcript_system_output":
                label = "system transcript output"
            elif private_kind == "transcript_score_report":
                label = "transcript score report"
            elif private_kind == "gpu_asr_evaluation_measurement":
                label = "GPU evaluation measurement"
            elif private_kind == "transcript_system_comparison":
                label = "transcript system comparison"
            else:
                label = "selection workspace data"
            raise ContractError(
                f"Tracked private {label} {relative} is forbidden; "
                "store it under ignored research/evaluation/"
            )
        if value.get("manifest_kind") not in {
            "reference_annotation",
            "reference_adjudication",
        }:
            continue
        checked.append(relative)
        publication = value.get("publication")
        if not isinstance(publication, dict) or publication.get("status") != "released":
            raise ContractError(
                f"Tracked reference text {relative} is not deliberately released; "
                "store private references under ignored research/evaluation/"
            )
        _validate_publication(publication, f"{relative}.publication")
    return checked


def manifest_type(value: dict[str, Any]) -> str:
    kind = value.get("manifest_kind")
    if not isinstance(kind, str):
        raise ContractError("$.manifest_kind: missing or invalid")
    return kind
