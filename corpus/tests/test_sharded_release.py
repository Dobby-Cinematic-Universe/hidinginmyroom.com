from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "corpus" / "src"))

from himr_corpus.importers import canonical_json  # noqa: E402
from himr_corpus.sharded_release import (  # noqa: E402
    export_release_v2_from_release,
    validate_sharded_release,
)


def _refresh_identity(release: dict) -> None:
    payload = {
        "schema_version": release["schema_version"],
        "generated_at": release["generated_at"],
        "counts": release["counts"],
        "recordings": release["recordings"],
    }
    release["release_id"] = "release_" + hashlib.sha256(
        canonical_json(payload).encode("utf-8")
    ).hexdigest()[:24]


def _recording(index: int) -> dict:
    digit = f"{index:x}"[-1]
    return {
        "recording_id": f"rec_{digit * 32}",
        "slug": f"recording-{index}",
        "title": f"Recording {index:03d}",
        "date_label": f"2026-08-{index + 1:02d}",
        "date_year": 2026,
        "date_basis": "source_metadata",
        "duration_ms": 2_000,
        "recording_type": "video",
        "review_state": "reviewed",
        "sources": [
            {
                "source_id": f"src_{digit * 32}",
                "platform": "youtube",
                "url": f"https://www.youtube.com/watch?v=test{index:07d}",
                "native_id": f"test{index:07d}",
                "access_state": "public",
            }
        ],
        "transcript_revisions": [
            {
                "revision_id": f"rev_{digit * 32}",
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
                        "segment_id": f"seg_{digit * 32}",
                        "start_ms": 0,
                        "end_ms": 1_000,
                        "text": f"Checked words {index}",
                        "speaker_label": "Daniel",
                        "confidence_band": "human",
                        "calibrated_probability": 1.0,
                    }
                ],
            }
        ],
    }


def _release(count: int = 1) -> dict:
    recordings = [_recording(index + 1) for index in range(count)]
    release = {
        "schema_version": 1,
        "release_id": "",
        "generated_at": "2026-08-26T20:00:00Z",
        "counts": {
            "recordings": len(recordings),
            "sources": len(recordings),
            "transcript_revisions": len(recordings),
            "segments": len(recordings),
        },
        "recordings": recordings,
    }
    _refresh_identity(release)
    return release


class ShardedReleaseTests(unittest.TestCase):
    def test_empty_manifest_validates_without_untrackable_empty_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = export_release_v2_from_release(_release(0), root)
            release_root = Path(result["release_directory"])
            release_root.rmdir()
            (root / "releases").rmdir()
            validated = validate_sharded_release(root / "manifest.json")
            self.assertEqual(validated["recordings"], 0)
            self.assertEqual(validated["catalog_shards"], 0)

    def test_export_is_deterministic_bounded_and_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = export_release_v2_from_release(_release(3), root, catalog_shard_size=2)
            first_manifest = (root / "manifest.json").read_bytes()
            second = export_release_v2_from_release(_release(3), root, catalog_shard_size=2)
            self.assertEqual(first["release_id"], second["release_id"])
            self.assertEqual(first_manifest, (root / "manifest.json").read_bytes())
            result = validate_sharded_release(root / "manifest.json")
            self.assertEqual(result["catalog_shards"], 2)
            self.assertEqual(result["recordings"], 3)
            manifest = json.loads(first_manifest)
            self.assertEqual([item["recording_count"] for item in manifest["catalog_shards"]], [2, 1])

    def test_detail_tampering_breaks_hash_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export_release_v2_from_release(_release(), root)
            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            release_root = root / "releases" / manifest["release_id"]
            catalog_ref = manifest["catalog_shards"][0]
            catalog = json.loads((release_root / catalog_ref["path"]).read_text(encoding="utf-8"))
            detail = release_root / catalog["recordings"][0]["detail"]["path"]
            detail.write_bytes(detail.read_bytes() + b" ")
            with self.assertRaisesRegex(ValueError, "byte count or SHA-256"):
                validate_sharded_release(root / "manifest.json")

    def test_manifest_tampering_breaks_release_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export_release_v2_from_release(_release(), root)
            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            manifest["stats"]["duration_ms"] += 1
            (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                validate_sharded_release(root / "manifest.json")

    def test_unreferenced_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export_release_v2_from_release(_release(), root)
            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            release_root = root / "releases" / manifest["release_id"]
            (release_root / "recordings" / "unreferenced.json").write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unreferenced files"):
                validate_sharded_release(root / "manifest.json")

    def test_invalid_new_projection_does_not_replace_active_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            export_release_v2_from_release(_release(), root)
            before = (root / "manifest.json").read_bytes()
            invalid = copy.deepcopy(_release())
            invalid["recordings"][0]["transcript_revisions"][0]["review_state"] = "machine"
            _refresh_identity(invalid)
            with self.assertRaisesRegex(ValueError, "machine_generated differs"):
                export_release_v2_from_release(invalid, root)
            self.assertEqual(before, (root / "manifest.json").read_bytes())
            validate_sharded_release(root / "manifest.json")

    def test_post_install_validation_failure_keeps_previous_active_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = export_release_v2_from_release(_release(), root)
            before = (root / "manifest.json").read_bytes()
            changed = _release()
            changed["recordings"][0]["title"] = "Changed but still publication-shaped"
            _refresh_identity(changed)
            with mock.patch(
                "himr_corpus.sharded_release.validate_sharded_release",
                side_effect=ValueError("injected post-install validation failure"),
            ):
                with self.assertRaisesRegex(ValueError, "injected post-install"):
                    export_release_v2_from_release(changed, root)
            self.assertEqual(before, (root / "manifest.json").read_bytes())
            self.assertEqual(
                first["release_id"],
                json.loads(before)["release_id"],
            )
            self.assertEqual(list(root.glob(".manifest-*.json")), [])
            validate_sharded_release(root / "manifest.json")

    def test_all_non_retracted_revisions_are_counted_as_searchable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            release = _release()
            recording = release["recordings"][0]
            older_english = copy.deepcopy(recording["transcript_revisions"][0])
            older_english["revision_id"] = f"rev_{'a' * 32}"
            older_english["review_state"] = "human_corrected"
            older_english["segments"][0]["segment_id"] = f"seg_{'a' * 32}"
            spanish = copy.deepcopy(recording["transcript_revisions"][0])
            spanish["revision_id"] = f"rev_{'b' * 32}"
            spanish["language"] = "es"
            spanish["segments"][0]["segment_id"] = f"seg_{'b' * 32}"
            recording["transcript_revisions"] = [older_english, recording["transcript_revisions"][0], spanish]
            release["counts"]["transcript_revisions"] = 3
            release["counts"]["segments"] = 3
            _refresh_identity(release)
            export_release_v2_from_release(release, root)
            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["stats"]["searchable_transcript_segments"], 3)
            self.assertEqual(manifest["facets"]["languages"], ["en", "es"])
            validate_sharded_release(root / "manifest.json")

    def test_retracted_revision_is_a_text_free_nonsearchable_tombstone(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            release = _release()
            revision = release["recordings"][0]["transcript_revisions"][0]
            revision.update(
                revision_kind="raw_asr",
                review_state="machine",
                machine_generated=True,
                unreviewed=True,
                lifecycle_state="retracted",
                lifecycle_history=[
                    {
                        "state": "retracted",
                        "reason_code": "transcription_error",
                        "decided_at": "2026-08-26T20:01:00Z",
                        "explanation": "A human review withdrew the machine wording.",
                    }
                ],
                disclaimer_code="retracted_transcript_text_withdrawn_v1",
                segments=[],
            )
            release["counts"]["segments"] = 0
            _refresh_identity(release)
            result = export_release_v2_from_release(release, root)
            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["stats"]["searchable_transcript_segments"], 0)
            self.assertEqual(manifest["facets"]["languages"], [])
            release_root = Path(result["release_directory"])
            catalog = json.loads(
                (release_root / manifest["catalog_shards"][0]["path"]).read_text(
                    encoding="utf-8"
                )
            )
            detail = json.loads(
                (release_root / catalog["recordings"][0]["detail"]["path"]).read_text(
                    encoding="utf-8"
                )
            )
            exported = detail["recording"]["transcript_revisions"][0]
            self.assertEqual(exported["lifecycle_state"], "retracted")
            self.assertEqual(exported["segments"], [])
            validate_sharded_release(root / "manifest.json")


if __name__ == "__main__":
    unittest.main()
