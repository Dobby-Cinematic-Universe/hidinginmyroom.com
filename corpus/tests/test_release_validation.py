from __future__ import annotations

import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path

from jsonschema import FormatChecker
from jsonschema.validators import Draft202012Validator


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "corpus" / "src"))

from himr_corpus.exporter import validate_release_shape  # noqa: E402
from himr_corpus.importers import canonical_json  # noqa: E402


def valid_release() -> dict:
    payload = {
        "schema_version": 1,
        "generated_at": "2026-08-26T20:00:00Z",
        "counts": {
            "recordings": 1,
            "sources": 1,
            "transcript_revisions": 1,
            "segments": 1,
        },
        "recordings": [
            {
                "recording_id": f"rec_{'1' * 32}",
                "slug": "test-recording",
                "title": "Test recording",
                "date_label": "2026-08-26",
                "date_year": 2026,
                "date_basis": "source_metadata",
                "duration_ms": 2_000,
                "recording_type": "video",
                "review_state": "reviewed",
                "sources": [
                    {
                        "source_id": f"src_{'2' * 32}",
                        "platform": "youtube",
                        "url": "https://www.youtube.com/watch?v=abcdefghijk",
                        "native_id": "abcdefghijk",
                        "access_state": "public",
                    }
                ],
                "transcript_revisions": [
                    {
                        "revision_id": f"rev_{'3' * 32}",
                        "revision_kind": "human_verbatim",
                        "language": "en",
                        "review_state": "media_checked",
                        "machine_generated": False,
                        "unreviewed": False,
                        "verified_quotation": False,
                        "disclaimer_code": "reviewed_transcript_not_fact_checked_v1",
                        "lifecycle_state": "active",
                        "lifecycle_history": [],
                        "segments": [
                            {
                                "segment_id": f"seg_{'4' * 32}",
                                "start_ms": 0,
                                "end_ms": 1_000,
                                "text": "Checked words",
                                "speaker_label": "Daniel",
                                "confidence_band": "human",
                                "calibrated_probability": 1.0,
                            }
                        ],
                    }
                ],
            }
        ],
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return {
        "schema_version": payload["schema_version"],
        "release_id": f"release_{digest[:24]}",
        "generated_at": payload["generated_at"],
        "counts": payload["counts"],
        "recordings": payload["recordings"],
    }


def refresh_identity(release: dict) -> None:
    payload = {
        "schema_version": release["schema_version"],
        "generated_at": release["generated_at"],
        "counts": release["counts"],
        "recordings": release["recordings"],
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    release["release_id"] = f"release_{digest[:24]}"


class PublicReleaseValidationTests(unittest.TestCase):
    def test_valid_release_passes(self):
        validate_release_shape(valid_release())

    def test_payload_tampering_breaks_release_identity(self):
        release = valid_release()
        release["recordings"][0]["title"] = "Silently changed"
        with self.assertRaisesRegex(ValueError, "Release identity mismatch"):
            validate_release_shape(release)

    def test_nested_publication_invariants_fail_closed(self):
        mutations = {
            "non-public source": lambda release: release["recordings"][0]["sources"][0].update(
                access_state="members_only"
            ),
            "credentialed URL": lambda release: release["recordings"][0]["sources"][0].update(
                url="https://user:secret@example.com/video"
            ),
            "inconsistent machine transcript": lambda release: release["recordings"][0][
                "transcript_revisions"
            ][0].update(review_state="machine"),
            "unsafe recording state": lambda release: release["recordings"][0].update(
                review_state="rejected"
            ),
            "boolean count": lambda release: release["counts"].update(recordings=True),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                release = copy.deepcopy(valid_release())
                mutate(release)
                refresh_identity(release)
                with self.assertRaises(ValueError):
                    validate_release_shape(release)

    def test_duplicate_global_segment_id_is_rejected(self):
        release = valid_release()
        revision = release["recordings"][0]["transcript_revisions"][0]
        duplicate = copy.deepcopy(revision["segments"][0])
        duplicate["start_ms"] = 1_000
        duplicate["end_ms"] = 2_000
        revision["segments"].append(duplicate)
        release["counts"]["segments"] = 2
        refresh_identity(release)
        with self.assertRaisesRegex(ValueError, "Duplicate transcript segment_id"):
            validate_release_shape(release)

    def test_machine_dispute_and_retraction_match_the_public_json_schema(self):
        schema = json.loads(
            (ROOT / "corpus/schemas/public-release.schema.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema, format_checker=FormatChecker())

        disputed = valid_release()
        revision = disputed["recordings"][0]["transcript_revisions"][0]
        revision.update(
            revision_kind="raw_asr",
            review_state="machine",
            machine_generated=True,
            unreviewed=True,
            lifecycle_state="disputed",
            lifecycle_history=[
                {
                    "state": "disputed",
                    "reason_code": "transcription_error",
                    "decided_at": "2026-08-26T20:01:00Z",
                    "explanation": "A human review disputed the machine wording.",
                }
            ],
            disclaimer_code="disputed_transcript_not_verified_quotation_v1",
        )
        refresh_identity(disputed)
        validate_release_shape(disputed)
        validator.validate(disputed)

        retracted = copy.deepcopy(disputed)
        revision = retracted["recordings"][0]["transcript_revisions"][0]
        revision.update(
            lifecycle_state="retracted",
            lifecycle_history=[
                {
                    "state": "retracted",
                    "reason_code": "transcription_error",
                    "decided_at": "2026-08-26T20:02:00Z",
                    "explanation": "A human review withdrew the machine wording.",
                }
            ],
            disclaimer_code="retracted_transcript_text_withdrawn_v1",
            segments=[],
        )
        retracted["counts"]["segments"] = 0
        refresh_identity(retracted)
        validate_release_shape(retracted)
        validator.validate(retracted)

    def test_illegal_lifecycle_histories_are_rejected(self):
        cases = {
            "initial reinstatement": [
                {
                    "state": "reinstated",
                    "reason_code": "other",
                    "decided_at": "2026-08-26T20:01:00Z",
                    "explanation": "There was no preceding lifecycle decision.",
                }
            ],
            "retracted to disputed": [
                {
                    "state": "retracted",
                    "reason_code": "other",
                    "decided_at": "2026-08-26T20:01:00Z",
                    "explanation": "Withdrawn.",
                },
                {
                    "state": "disputed",
                    "reason_code": "other",
                    "decided_at": "2026-08-26T20:02:00Z",
                    "explanation": "This must use reinstatement instead.",
                },
            ],
            "duplicate dispute": [
                {
                    "state": "disputed",
                    "reason_code": "other",
                    "decided_at": "2026-08-26T20:01:00Z",
                    "explanation": "Disputed.",
                },
                {
                    "state": "disputed",
                    "reason_code": "other",
                    "decided_at": "2026-08-26T20:02:00Z",
                    "explanation": "Still disputed.",
                },
            ],
        }
        for label, history in cases.items():
            with self.subTest(label=label):
                release = valid_release()
                revision = release["recordings"][0]["transcript_revisions"][0]
                revision["lifecycle_history"] = history
                revision["lifecycle_state"] = history[-1]["state"]
                revision["disclaimer_code"] = (
                    "disputed_transcript_not_verified_quotation_v1"
                    if history[-1]["state"] == "disputed"
                    else "reviewed_transcript_not_fact_checked_v1"
                )
                refresh_identity(release)
                with self.assertRaisesRegex(ValueError, "illegal transition"):
                    validate_release_shape(release)

    def test_invalid_calendar_date_in_lifecycle_is_rejected(self):
        release = valid_release()
        revision = release["recordings"][0]["transcript_revisions"][0]
        revision.update(
            lifecycle_state="disputed",
            lifecycle_history=[
                {
                    "state": "disputed",
                    "reason_code": "other",
                    "decided_at": "2026-02-30T00:00:00Z",
                    "explanation": "Invalid calendar date.",
                }
            ],
            disclaimer_code="disputed_transcript_not_verified_quotation_v1",
        )
        refresh_identity(release)
        with self.assertRaisesRegex(ValueError, "valid UTC timestamp"):
            validate_release_shape(release)


if __name__ == "__main__":
    unittest.main()
