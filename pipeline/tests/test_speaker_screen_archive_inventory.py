"""Synthetic, metadata-only archive inventory admission and dedup tests."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pipeline import speaker_screen_archive_inventory as inventory


class ArchiveInventoryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="speaker-archive-inventory-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.sequence = 0

    def save(self, value, path=None):
        self.sequence += 1
        path = path or self.root / f"metadata-{self.sequence}.json"
        body = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
        path.write_bytes(body)
        path.chmod(0o600)
        return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest()}

    def result(self, *, sha="1" * 64, title="Ordinary stream", native="archive/20260912-stream.mp4", audio=True):
        self.sequence += 1
        path = self.root / f"result-{self.sequence}.json"
        media_id = "media_sha256_" + sha
        media = {"media_id": media_id, "sha256": sha, "byte_count": 123456, "duration_ms": 30000}
        admission = {"media_id": media_id, "sha256": sha, "byte_count": 123456,
                     "path": str(self.root / (sha + ".media-not-opened")),
                     "normalized_probe": {"format": {"duration_ms": 30000, "format_name": "mov,mp4"},
                         "streams": [{"index": 0, "codec_type": "audio" if audio else "video", "codec_name": "aac" if audio else "h264", "duration_ms": 29000}]}}
        value = {"schema_version": 1, "status": "completed", "dry_run": False, "errors": [],
                 "job_id": f"acq-inventory-{self.sequence}", "work_order_sha256": "9" * 64,
                 "result_path": str(path), "duration_ms": 999, "admission": admission,
                 "catalog_records": {"media_objects": [media]},
                 "source": {"native_id": native, "title": title, "canonical_url": "https://example.invalid/" + native,
                            "published_at": "2035-01-01T00:00:00Z"}}
        return value, self.save(value, path)

    def checkpoint(self, results):
        orders = [{"job_id": result["job_id"], "result_path": binding["path"],
                   "work_order_identity_sha256": result["work_order_sha256"],
                   "state": {"result": result, "result_sha256": binding["sha256"],
                             "media_sha256": result["admission"]["sha256"], "byte_count": result["admission"]["byte_count"]}}
                  for result, binding in results]
        return {"kind": "himr_autonomous_controller_checkpoint", "schema_version": 1, "backend": {"queue_replay": {
            "kind": "himr_queue_operational_replay_restart_checkpoint", "schema_version": 2,
            "schedules": [{"work_order_count": len(orders), "orders": orders}],
            "totals": {"completed_count": len(orders), "pending_count": 0, "work_order_count": len(orders), "schedule_count": 1}}}}

    def admission(self, results):
        records = [{"result_path": binding["path"], "job_id": result["job_id"],
                    "work_order_semantic_sha256": result["work_order_sha256"], "native_id": result["source"]["native_id"],
                    "expected_byte_count": result["admission"]["byte_count"], "canonical_url": result["source"]["canonical_url"]}
                   for result, binding in results]
        return {"kind": "himr_exact_archive_incremental_admission", "schema_version": 1,
                "records": records, "totals": {"recordings": len(records)}}

    def test_dedupe_preserves_every_alias_and_selects_highest_advisory_title_priority(self):
        first = self.result()
        second = self.result(title="Interview with a guest", native="archive/[2026-09-11] Interview.mp4")
        result = inventory.inventory([self.save(self.checkpoint([first, second]))], [])
        self.assertEqual(result["counts"]["admitted_sources"], 2)
        self.assertEqual(result["counts"]["unique_media"], 1)
        self.assertEqual(result["counts"]["duplicate_source_admissions"], 1)
        row = result["records"][0]
        self.assertEqual(row["acquisition_result"], second[1])
        self.assertEqual(len(row["aliases"]), 2)
        self.assertGreater(row["priority_score"], 0)
        self.assertEqual(row["aliases"][0]["date"], {"value": "2026-09-11", "basis": "filename_date"})
        self.assertEqual(row["recording"]["duration_hint_ms"], 30000)  # Not acquisition wall time999.
        self.assertEqual(row["first_audio_duration_hint_ms"], 29000)
        self.assertFalse(Path(row["recording"]["path"]).exists())

    def test_checkpoint_plus_incremental_admission_bind_exact_results_without_media_reads(self):
        old = self.result()
        new = self.result(sha="2" * 64, native="new/20260912-new.mp4")
        checkpoint = self.save(self.checkpoint([old]))
        admission = self.save(self.admission([new]))
        original = copy.deepcopy([checkpoint, admission])
        result = inventory.inventory([checkpoint], [admission])
        self.assertEqual(result["counts"]["unique_audio_present"], 2)
        self.assertEqual([checkpoint, admission], original)
        self.assertEqual(result["source_bindings"], sorted([checkpoint, admission, old[1], new[1]], key=lambda row: row["path"]))
        self.assertTrue(result["semantics"]["result_metadata_currently_verified"])
        self.assertFalse(result["semantics"]["media_bytes_opened"])
        self.assertFalse(result["semantics"]["acquisition_audio_metadata_is_fresh_decode_proof"])

    def test_no_audio_aliases_are_preserved_and_counted_separately_from_unique_media(self):
        one = self.result(audio=False)
        two = self.result(audio=False, native="other/00000000-Intro.mp4")
        result = inventory.inventory([self.save(self.checkpoint([one, two]))], [])
        self.assertEqual(result["counts"]["unique_no_audio"], 1)
        self.assertEqual(result["counts"]["source_aliases_no_audio"], 2)
        self.assertEqual(result["counts"]["unique_audio_present"], 0)
        self.assertEqual(result["records"][0]["cached_audio_state"], "no_audio_stream")

    def test_cache_only_mode_preserves_exact_result_binding_but_does_not_claim_current_verification(self):
        value, binding = self.result()
        checkpoint = self.save(self.checkpoint([(value, binding)]))
        Path(binding["path"]).write_text('{"changed":true}\n')
        result = inventory.inventory([checkpoint], [], verify_results=False)
        self.assertEqual(result["records"][0]["acquisition_result"], binding)
        self.assertFalse(result["semantics"]["result_metadata_currently_verified"])
        with self.assertRaises(inventory.ScreenError):
            inventory.inventory([checkpoint], [])

    def test_cache_serialization_digest_is_verified_even_without_result_reread(self):
        result = self.result()
        checkpoint = self.checkpoint([result])
        checkpoint["backend"]["queue_replay"]["schedules"][0]["orders"][0]["state"]["result"]["source"]["title"] = "altered"
        with self.assertRaises(inventory.ScreenError):
            inventory.inventory([self.save(checkpoint)], [], verify_results=False)

    def test_incremental_result_identity_requires_job_workorder_source_url_and_bytes(self):
        result = self.result()
        for field, value in (("job_id", "wrong"), ("work_order_semantic_sha256", "0" * 64),
                             ("native_id", "wrong"), ("canonical_url", "wrong"), ("expected_byte_count", True)):
            admission = self.admission([result])
            admission["records"][0][field] = value
            with self.subTest(field=field), self.assertRaises(inventory.ScreenError):
                inventory.inventory([], [self.save(admission)])

    def test_identical_content_cannot_claim_conflicting_path_bytes_or_audio_presence(self):
        for field in ("path", "bytes", "audio"):
            first = self.result()
            value, binding = self.result(native="other/same-content.mp4")
            if field == "path":
                value["admission"]["path"] = str(self.root / "different-media-path")
            elif field == "bytes":
                value["admission"]["byte_count"] += 1
                value["catalog_records"]["media_objects"][0]["byte_count"] += 1
            else:
                value["admission"]["normalized_probe"]["streams"] = []
            changed = (value, self.save(value, Path(binding["path"])))
            with self.subTest(field=field), self.assertRaises(inventory.ScreenError):
                inventory.inventory([self.save(self.checkpoint([first, changed]))], [])

    def test_unknown_and_invalid_filename_dates_never_use_published_dates(self):
        for name in ("ordinary.mp4", "00000000-Intro.mp4", "20260230-invalid.mp4", "[2026-02-30] invalid.mp4"):
            result = self.result(native="archive/" + name)
            row = inventory.inventory([self.save(self.checkpoint([result]))], [])["records"][0]
            self.assertEqual(row["aliases"][0]["date"], {"value": None, "basis": "unknown"})

    def test_duplicate_sources_and_duplicate_result_admissions_fail_closed(self):
        first = self.result()
        second = self.result(sha="2" * 64)
        for results in ([first, first], [first, second]):
            with self.assertRaises(inventory.ScreenError):
                inventory.inventory([self.save(self.checkpoint(results))], [])

    def test_pending_checkpoint_and_count_mismatches_are_not_complete_inventories(self):
        result = self.result()
        for field, value in (("pending_count", 1), ("completed_count", 0), ("schedule_count", True), ("work_order_count", True)):
            checkpoint = self.checkpoint([result])
            checkpoint["backend"]["queue_replay"]["totals"][field] = value
            with self.assertRaises(inventory.ScreenError):
                inventory.inventory([self.save(checkpoint)], [])

    def test_malformed_result_media_identity_probe_and_duration_are_rejected(self):
        edits = [lambda value: value.update(status="pending"), lambda value: value.update(dry_run=0),
                 lambda value: value["admission"].update(media_id="incorrect"),
                 lambda value: value["catalog_records"]["media_objects"][0].update(byte_count=10),
                 lambda value: value["admission"]["normalized_probe"]["format"].update(duration_ms=True),
                 lambda value: value["admission"]["normalized_probe"]["streams"].append(copy.deepcopy(value["admission"]["normalized_probe"]["streams"][0]))]
        for edit in edits:
            value, binding = self.result()
            edit(value)
            changed = (value, self.save(value, Path(binding["path"])))
            with self.assertRaises(inventory.ScreenError):
                inventory.inventory([self.save(self.checkpoint([changed]))], [])

    def test_input_types_scope_and_byte_bounds_are_enforced(self):
        result = self.result()
        checkpoint = self.save(self.checkpoint([result]))
        for checkpoints, admissions in (([], []), ({}, []), ([checkpoint] * 17, [])):
            with self.assertRaises(inventory.ScreenError):
                inventory.inventory(checkpoints, admissions)
        with self.assertRaises(inventory.ScreenError):
            inventory.inventory([checkpoint], [], verify_results=1)
        with mock.patch.object(inventory, "MAX_CHECKPOINT_BYTES", 10), self.assertRaises(inventory.ScreenError):
            inventory.inventory([checkpoint], [])
        with mock.patch.object(inventory, "MAX_TOTAL_BYTES", 10), self.assertRaises(inventory.ScreenError):
            inventory.inventory([checkpoint], [])
        with self.assertRaises(inventory.ScreenError):
            inventory.inventory([self.save(self.checkpoint([]))], [])

    def test_hash_binding_symlink_and_peer_writable_metadata_fail_closed(self):
        result = self.result()
        binding = self.save(self.checkpoint([result]))
        with self.assertRaises(inventory.ScreenError):
            inventory.inventory([{**binding, "sha256": "0" * 64}], [])
        alias = self.root / "aliased-checkpoint"
        alias.symlink_to(binding["path"])
        with self.assertRaises((inventory.ScreenError, OSError)):
            inventory.inventory([{**binding, "path": str(alias)}], [])
        Path(binding["path"]).chmod(0o666)
        with self.assertRaises(inventory.ScreenError):
            inventory.inventory([binding], [])


if __name__ == "__main__":
    unittest.main()
