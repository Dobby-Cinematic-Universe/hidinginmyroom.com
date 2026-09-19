from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pipeline import cloud_transcription_archive as archive


class CloudArchiveTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.counter = 0

    def write(self, name, value):
        path = self.root / name
        body = archive.canonical(value)
        if path.exists():
            path.chmod(0o600)
        path.write_bytes(body)
        path.chmod(0o400)
        return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest()}

    def row(self, *, native=None, sha=None, audio=True, duration=1000):
        self.counter += 1
        number = self.counter
        sha = sha or f"{number:064x}"
        media = self.root / ("media-" + sha)
        if not media.exists():
            media.write_bytes(b"This is never read as media.")
            media.chmod(0o400)
        native = native or f"699994/20260912-Title-{number:011d}.mp4"
        recording = {"path": str(media), "sha256": sha, "byte_count": media.stat().st_size,
                     "media_id": "media_sha256_" + sha, "duration_hint_ms": duration}
        receipt = {"status": "completed", "errors": [], "job_id": f"job-{number}",
            "admission": {key: recording[key] for key in ("path", "sha256", "byte_count", "media_id")},
            "source": {"native_id": native, "title": "Title", "platform": "internet_archive",
                       "canonical_url": "https://archive.org/download/" + native}}
        ref = self.write(f"result-{number}.json", receipt)
        return {"recording": recording, "acquisition_result": ref,
            "cached_audio_state": "audio_present" if audio else "no_audio_stream",
            "duration_hint_disagreement": False,
            "aliases": [{"result": ref, "job_id": receipt["job_id"], "source_native_id": native,
                         "title": "Title", "date": {"value": "2026-09-12", "basis": "filename_date"}}]}

    def inventory(self, rows, name="source.json"):
        return self.write(name, {"kind": archive.SOURCE_KIND, "schema_version": 1,
            "records": rows, "counts": {"unique_media": len(rows),
                                      "admitted_sources": sum(len(row["aliases"]) for row in rows)}})

    def build(self, rows):
        ref = self.inventory(rows)
        return archive.build_inventory(ref["path"], ref["sha256"])

    def test_complete_inventory_reads_only_metadata(self):
        row = self.row()
        ref = self.inventory([row])
        real_open = os.open
        opened = []

        def checked(path, *args, **kwargs):
            opened.append(str(path))
            if str(path) == Path(row["recording"]["path"]).name:
                raise AssertionError("raw media must not be opened")
            return real_open(path, *args, **kwargs)

        with mock.patch.object(archive.os, "open", side_effect=checked):
            value = archive.build_inventory(ref["path"], ref["sha256"])
        self.assertIn("result-1.json", opened)
        output = value["recordings"][0]
        self.assertEqual(output["recording_id"], row["recording"]["media_id"])
        self.assertEqual(output["state"], "ready")
        self.assertEqual(output["source_ids"]["youtube"], ["00000000001"])
        self.assertEqual(output["source_ids"]["archive_native"], [row["aliases"][0]["source_native_id"]])
        self.assertEqual(output["duration_ms"], 1000)
        self.assertEqual(value["counts"]["metadata_receipts_verified"], 1)
        self.assertFalse(value["semantics"]["media_hashes_reverified"])
        self.assertFalse(value["semantics"]["paid_api_authority"])

    def test_no_audio_not_omitted(self):
        value = self.build([self.row(), self.row(audio=False)])
        self.assertEqual(value["counts"]["recordings"], 2)
        self.assertEqual(value["counts"]["no_audio"], 1)
        self.assertIn("no_audio_stream", value["recordings"][1]["reasons"])

    def test_missing_media_not_omitted(self):
        row = self.row()
        Path(row["recording"]["path"]).unlink()
        output = self.build([row])["recordings"][0]
        self.assertEqual(output["state"], "missing_media")
        self.assertIsNone(output["source_witness"])

    def test_size_changed_requires_review(self):
        row = self.row()
        media = Path(row["recording"]["path"])
        media.chmod(0o600)
        media.write_bytes(b"changed")
        value = self.build([row])
        self.assertEqual(value["recordings"][0]["state"], "review")
        self.assertIn("media_size_mismatch", value["recordings"][0]["reasons"])

    def test_changed_acquisition_receipt_rejected(self):
        row = self.row()
        self.write("result-1.json", {"status": "completed"})
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "SHA-256 mismatch"):
            self.build([row])

    def test_changed_source_inventory_rejected(self):
        ref = self.inventory([self.row()])
        self.write("source.json", {})
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "SHA-256 mismatch"):
            archive.build_inventory(ref["path"], ref["sha256"])

    def test_source_alias_identity_cannot_be_forged(self):
        row = self.row()
        row["aliases"][0]["source_native_id"] = "699994/other.mp4"
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "source identity differs"):
            self.build([row])

    def test_receipt_must_bind_exact_media(self):
        row = self.row()
        row["recording"]["byte_count"] += 1
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "media binding differs"):
            self.build([row])

    def test_source_counts_cannot_silently_drop_rows(self):
        ref = self.inventory([self.row()])
        value = archive.read_bound(ref)
        value["counts"]["unique_media"] = 2
        changed = self.write("source.json", value)
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "coverage counts differ"):
            archive.build_inventory(changed["path"], changed["sha256"])

    def test_duplicate_physical_rows_in_claimed_unique_source_rejected(self):
        row = self.row()
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "invalid or repeated"):
            self.build([row, row])

    def test_merge_snapshots_dedupes_physical_recordings_and_keeps_aliases(self):
        row = self.row()
        other = self.row(sha=row["recording"]["sha256"], native="69999/another-title.mp4")
        one = self.inventory([row], "first.json")
        two = self.inventory([row, self.row()], "second.json")
        three = self.inventory([other], "third.json")
        value = archive.build_inventory(one["path"], one["sha256"], additional_inventories=[two, three])
        self.assertEqual(value["counts"]["recordings"], 2)
        self.assertEqual(value["counts"]["source_inventory_rows"], 4)
        self.assertEqual(value["counts"]["aliases"], 3)
        self.assertEqual(len(value["recordings"][0]["aliases"]), 2)
        self.assertEqual(value["recordings"][0]["state"], "ready")

    def test_same_native_id_different_physical_media_require_review(self):
        first = self.row(native="699994/some-file.mp4")
        second = self.row(native="699994/some-file.mp4")
        value = self.build([first, second])
        self.assertEqual(value["counts"]["recordings"], 2)
        self.assertEqual(value["counts"]["review"], 2)
        self.assertEqual(value["counts"]["identity_conflicts"], 1)

    def test_same_youtube_id_different_physical_encodings_not_collapsed(self):
        first = self.row(native="699994/20260912-title-abcdefghijk.webm")
        second = self.row(native="69999/20260912-title-abcdefghijk.mp4")
        value = self.build([first, second])
        self.assertEqual(value["counts"]["recordings"], 2)
        self.assertEqual(value["counts"]["review"], 2)
        self.assertEqual(value["identity_conflicts"][0]["platform"], "youtube")

    def test_multiple_youtube_ids_same_media_requires_review(self):
        first = self.row(native="699994/title-abcdefghijk.webm")
        second = self.row(sha=first["recording"]["sha256"], native="69999/title-lmnopqrstuv.mp4")
        first["aliases"].extend(second["aliases"])
        value = self.build([first])
        self.assertEqual(value["counts"]["recordings"], 1)
        self.assertEqual(value["counts"]["review"], 1)
        self.assertIn("multiple_youtube_ids_for_one_physical_recording", value["recordings"][0]["reasons"])

    def test_conflicting_metadata_for_same_physical_identity_rejected(self):
        row = self.row()
        changed = copy.deepcopy(row)
        changed["recording"]["duration_hint_ms"] += 10
        first = self.inventory([row], "first.json")
        second = self.inventory([changed], "second.json")
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "conflicting media metadata"):
            archive.build_inventory(first["path"], first["sha256"], additional_inventories=[second])

    def test_duration_unknown_or_disagreement_requires_review(self):
        first = self.row(duration=None)
        second = self.row()
        second["duration_hint_disagreement"] = True
        value = self.build([first, second])
        self.assertEqual(value["counts"]["review"], 2)
        self.assertEqual(value["counts"]["known_duration_ms"], 1000)

    def test_filename_hint_is_not_arbitrary_title_matching(self):
        self.assertEqual(archive.youtube_id("699994/20260912-A normal title.mp4", "internet_archive"), (None, None))
        self.assertEqual(archive.youtube_id("abcdefghijk", "youtube"), ("abcdefghijk", "source_native_id"))
        self.assertEqual(archive.youtube_id("699994/clip [abc_def-ghi].mp4", "internet_archive"),
                         ("abc_def-ghi", "archive_filename_suffix"))

    def test_media_symlink_and_ancestor_symlink_rejected(self):
        row = self.row()
        path = Path(row["recording"]["path"])
        target = self.root / "target"
        path.rename(target)
        path.symlink_to(target)
        with self.assertRaises(archive.ArchiveInventoryError):
            self.build([row])
        nested = self.root / "nested"
        nested.mkdir(mode=0o700)
        link = self.root / "linked"
        link.symlink_to(nested, target_is_directory=True)
        with self.assertRaises(OSError):
            archive.media_witness(link / "anything", 1)

    def test_metadata_symlink_rejected(self):
        row = self.row()
        source = self.inventory([row])
        link = self.root / "source-link.json"
        link.symlink_to(source["path"])
        with self.assertRaises(OSError):
            archive.build_inventory(str(link), source["sha256"])

    def test_duplicate_json_keys_rejected(self):
        path = self.root / "duplicate.json"
        body = b'{"kind":"one","kind":"two"}'
        path.write_bytes(body)
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "duplicate JSON"):
            archive.read_bound({"path": str(path), "sha256": hashlib.sha256(body).hexdigest()})

    def test_private_fresh_output_only(self):
        value = self.build([self.row()])
        output = self.root / "output.json"
        ref = archive.write_inventory(output, value)
        self.assertEqual(archive.read_bound(ref), value)
        self.assertEqual(output.stat().st_mode & 0o777, 0o400)
        self.assertEqual(output.stat().st_nlink, 1)
        with self.assertRaises(FileExistsError):
            archive.write_inventory(output, value)
        self.assertEqual(archive.read_bound(ref), value)
        self.root.chmod(0o755)
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "private"):
            archive.write_inventory(self.root / "public.json", value)

    def test_live_controller_history_is_not_opened(self):
        source = self.inventory([self.row()])
        value = archive.read_bound(source)
        value["source_bindings"] = [{"path": str(self.root / "absent-live-controller.json"), "sha256": "f" * 64}]
        source = self.write("source.json", value)
        self.assertEqual(archive.build_inventory(source["path"], source["sha256"])["counts"]["ready"], 1)

    def test_malformed_nested_records_fail_closed(self):
        row = self.row()
        source = self.inventory([row])
        value = archive.read_bound(source)
        value["records"] = [None]
        source = self.write("source.json", value)
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "invalid source inventory recording"):
            archive.build_inventory(source["path"], source["sha256"])

    def test_reference_path_with_conflicting_hashes_rejected(self):
        first = self.row()
        second = self.row()
        second["acquisition_result"] = {"path": first["acquisition_result"]["path"], "sha256": "f" * 64}
        second["aliases"][0]["result"] = second["acquisition_result"]
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "conflicting hashes"):
            self.build([first, second])

    def test_incomplete_receipt_is_not_cloud_ready(self):
        row = self.row()
        value = archive.read_bound(row["acquisition_result"])
        value["status"] = "failed"
        reference = self.write("result-1.json", value)
        row["acquisition_result"] = reference
        row["aliases"][0]["result"] = reference
        with self.assertRaisesRegex(archive.ArchiveInventoryError, "not successfully completed"):
            self.build([row])


if __name__ == "__main__":
    unittest.main()
