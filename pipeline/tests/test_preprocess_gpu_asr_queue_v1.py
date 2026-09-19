from __future__ import annotations

import errno
import hashlib
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pipeline import preprocess_gpu_asr_queue_v1 as HANDOFF
from pipeline.gpu import production_asr_v5 as ASR_V5


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
TEST_WORK_ROOT = REPOSITORY_ROOT / "pipeline/.test-work"
TEST_UUID = "2f0df088-4057-48f3-9be6-3f20c26fc6df"


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class PreprocessGpuAsrQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="preprocess-gpu-queue-test-", dir=TEST_WORK_ROOT
        )
        self.base = Path(self.temporary.name)
        self.root = self.base / "portable-hot-root"
        self.root.mkdir(mode=0o700)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir(mode=0o700)
        self.bundle.chmod(0o500)
        self.state = self.root / "state"
        self.state.mkdir(mode=0o700)
        self.queue_root = self.root / "gpu-queue"
        self.queue_root.mkdir(mode=0o700)
        self.data = self.root / "data"
        self.data.mkdir(mode=0o700)

        self.profile = HANDOFF.PROFILE_V2.default_profile()
        self.profile_path = self.root / "production-profile-v2.json"
        self.profile_path.write_bytes(HANDOFF.PROFILE_V2.canonical_bytes(self.profile))
        self.profile_path.chmod(0o400)

        self.registration = HANDOFF.PORTABLE_ROOT.make_registration(
            root_id="gpu-hot-handoff-test",
            tier="hot_main_drive",
            path=self.root,
            filesystem_uuid=TEST_UUID,
            owner_uid=os.geteuid(),
            historical_observation={
                "st_dev": 2**62,
                "st_ino": 2**61,
                "st_mtime_ns": 17,
                "st_ctime_ns": 19,
            },
        )
        self.registration_path = self.base / "root-registration.json"
        self.registration_path.write_bytes(
            HANDOFF.PORTABLE_ROOT.canonical_bytes(self.registration)
        )
        self.registration_path.chmod(0o400)
        self.registration_digest = digest(self.registration_path.read_bytes())

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def receipt_ref(self, ordinal: int) -> dict[str, object]:
        path = self.data / f"receipt-{ordinal:03d}.json"
        body = (json.dumps({"receipt": ordinal}) + "\n").encode()
        path.write_bytes(body)
        path.chmod(0o400)
        return {
            "path": str(path),
            "uri": path.as_uri(),
            "physical_sha256": digest(body),
            "receipt_id": f"ppreceipt_{ordinal:032x}",
            "receipt_sha256": digest(f"receipt-core-{ordinal}".encode()),
            "ordinal": ordinal,
        }

    def result_ref(self, ordinal: int) -> dict[str, object]:
        path = self.data / f"result-{ordinal:03d}.json"
        body = (json.dumps({"result": ordinal}) + "\n").encode()
        path.write_bytes(body)
        path.chmod(0o400)
        return {
            "path": str(path),
            "uri": path.as_uri(),
            "sha256": digest(body),
            "byte_count": len(body),
            "job_id": f"preprocess_job_{ordinal}",
            "processing_run_id": f"processing_run_{ordinal}",
            "recipe_sha256": digest(f"recipe-{ordinal}".encode()),
        }

    def source_ref(self, ordinal: int) -> dict[str, object]:
        source_digest = digest(f"source-{ordinal}".encode())
        return {
            "media_id": f"media_sha256_{source_digest}",
            "sha256": source_digest,
            "byte_count": 100 + ordinal,
            "path": f"/source-not-opened/source-{ordinal}.mp4",
            "storage_uri": f"file:///source-not-opened/source-{ordinal}.mp4",
            "duration_ms": 2_000 + ordinal,
            "first_cataloged_at": "2026-08-29T00:00:00Z",
        }

    def item(
        self,
        ordinal: int,
        *,
        byte_count: int | None = None,
        duration_ms: int = 1_000,
        forced_sha256: str | None = None,
    ) -> dict[str, object]:
        path = self.data / f"audio-{ordinal:03d}.flac"
        body = f"audio-{ordinal}\n".encode()
        path.write_bytes(body)
        path.chmod(0o400)
        audio_digest = forced_sha256 or digest(body)
        observed_bytes = len(body) if byte_count is None else byte_count
        probe = {
            "schema_version": 1,
            "media": {
                "media_id": f"media_sha256_{audio_digest}",
                "sha256": audio_digest,
                "byte_count": observed_bytes,
            },
            "primary_streams": {"audio_index": 0, "video_index": None},
            "format": {
                "format_name": "flac",
                "start_ms": 0,
                "duration_ms": duration_ms,
            },
            "streams": [
                {
                    "index": 0,
                    "codec_type": "audio",
                    "codec_name": "flac",
                    "start_ms": 0,
                    "duration_ms": duration_ms,
                    "audio": {
                        "channel_layout": "mono",
                        "channels": 1,
                        "sample_format": "s16",
                        "sample_rate_hz": 16_000,
                    },
                }
            ],
        }
        receipt = self.receipt_ref(ordinal)
        result = self.result_ref(ordinal)
        source = self.source_ref(ordinal)
        return {
            "result": result,
            "source_media": source,
            "asr_eligibility": {
                "eligible": True,
                "reason": None,
                "source_audio_stream_present": True,
                "normalized_audio_operation_enabled": True,
            },
            "routing_hint": "gpu",
            "raw_result": {},
            "audio": {
                "artifact_id": f"artifact_{ordinal:032x}",
                "artifact_kind": "audio_16khz_mono_flac",
                "processing_run_id": f"processing_run_{ordinal}",
                "media_id": f"media_sha256_{audio_digest}",
                "path": str(path),
                "uri": path.as_uri(),
                "sha256": audio_digest,
                "byte_count": observed_bytes,
                "duration_ms": duration_ms,
                "normalized_probe": probe,
                "visibility": "private",
            },
            "evidence": {
                "mode": "sealed_preprocess_receipt",
                "receipt": receipt,
                "preprocess_result": result,
                "source_media": source,
            },
        }

    def skip(self, ordinal: int) -> dict[str, object]:
        return {
            "ordinal": ordinal,
            "entry_id": f"entry-{ordinal}",
            "reason": "source_has_no_audio",
            "source_audio_stream_present": False,
            "normalized_audio_operation_enabled": True,
            "receipt": self.receipt_ref(ordinal),
            "preprocess_result": self.result_ref(ordinal),
            "source_media": self.source_ref(ordinal),
        }

    def origin(
        self,
        receipt_count: int,
        *,
        skips: list[dict[str, object]] | None = None,
        handling_control: dict[str, object] | None = None,
    ) -> dict[str, object]:
        value: dict[str, object] = {
            "mode": "sealed_preprocess_receipts",
            "preprocess_bundle": {
                "path": str(self.bundle),
                "manifest_path": str(self.bundle / "manifest.json"),
                "manifest_physical_sha256": digest(b"bundle-physical"),
                "bundle_id": "ppbundle_test",
                "identity_sha256": digest(b"bundle-identity"),
                "manifest_sha256": digest(b"bundle-manifest"),
            },
            "state_root": str(self.state),
            "receipt_count": receipt_count,
            "receipt_state_sha256": digest(b"receipt-state"),
            "receipt_refs_sha256": digest(b"receipt-refs"),
        }
        if skips:
            value["ineligible_receipts"] = skips
        if handling_control is not None:
            value["handling_control"] = handling_control
        return value

    def profile_reference(self) -> dict[str, object]:
        body = self.profile_path.read_bytes()
        return {
            "path": str(self.profile_path),
            "relative_path": self.profile_path.relative_to(self.root).as_posix(),
            "physical_sha256": digest(body),
            "byte_count": len(body),
            "profile_id": self.profile["profile_id"],
            "identity_sha256": self.profile["identity_sha256"],
        }

    def registration_reference(self) -> dict[str, object]:
        return HANDOFF._registration_reference(
            self.registration,
            self.registration_path,
            self.registration_digest,
        )

    def pure_manifest(
        self,
        origin: dict[str, object],
        items: list[dict[str, object]],
        *,
        handling: dict[int, dict[str, object]] | None = None,
    ) -> dict[str, object]:
        audio_sealed_modes = {
            int(item["evidence"]["receipt"]["ordinal"]): (
                f"{stat.S_IMODE(Path(item['audio']['path']).stat().st_mode):04o}"
            )
            for item in items
        }
        return HANDOFF._manifest_from_replay(
            origin_value=origin,
            items_value=items,
            handling_by_ordinal=handling or {},
            profile_value=self.profile,
            profile_reference=self.profile_reference(),
            registration_reference=self.registration_reference(),
            queue_root=self.queue_root,
            audio_sealed_modes=audio_sealed_modes,
        )

    def build_kwargs(self) -> dict[str, object]:
        return {
            "preprocess_bundle": self.bundle,
            "preprocess_state_root": self.state,
            "queue_root": self.queue_root,
            "production_profile_path": self.profile_path,
            "root_registration_path": self.registration_path,
            "root_registration_sha256": self.registration_digest,
        }

    def test_deterministic_ready_chunking_and_explicit_no_audio_skip(self) -> None:
        maximum_bytes = self.profile["item_limits"]["maximum_audio_bytes"]
        maximum_ms = int(
            self.profile["item_limits"]["maximum_audio_seconds"] * 1_000
        )
        items = [
            self.item(1),
            self.item(2, byte_count=maximum_bytes + 1),
            self.item(3, duration_ms=maximum_ms + 1),
        ]
        skipped = self.skip(4)
        origin = self.origin(4, skips=[skipped])
        first = self.pure_manifest(origin, items)
        second = self.pure_manifest(origin, items)
        self.assertEqual(first, second)
        self.assertEqual(
            HANDOFF.canonical_bytes(first), HANDOFF.canonical_bytes(second)
        )
        self.assertEqual(
            [row["resource_disposition"]["state"] for row in first["members"]],
            ["ready", "requires_chunking", "requires_chunking"],
        )
        self.assertEqual(
            [row["audio"]["sealed_mode"] for row in first["members"]],
            ["0400", "0400", "0400"],
        )
        self.assertEqual(
            first["members"][1]["resource_disposition"]["reasons"],
            ["maximum_audio_bytes_exceeded"],
        )
        self.assertEqual(
            first["members"][2]["resource_disposition"]["reasons"],
            ["maximum_audio_duration_exceeded"],
        )
        self.assertEqual(
            [row["ordinal"] for row in first["members"]], [1, 2, 3]
        )
        self.assertEqual(first["explicit_skips"][0]["preprocess_ordinal"], 4)
        self.assertEqual(
            first["explicit_skips"][0]["disposition"]["reason"],
            "source_has_no_audio",
        )
        self.assertEqual(first["totals"]["receipt_count"], 4)
        self.assertEqual(first["totals"]["requires_chunking_count"], 2)
        self.assertIsNone(
            first["members"][1]["resource_disposition"]["chunk_plan"]
        )
        self.assertEqual(
            first["members"][0]["lineage"]["preprocess_bundle"],
            origin["preprocess_bundle"],
        )
        self.assertEqual(first["safety"], HANDOFF.SAFETY)
        for key in (
            "execution_authority",
            "gpu_execution_authority",
            "result_import_authority",
            "publication_authority",
        ):
            self.assertEqual(first["safety"][key], "none")

    def test_duplicate_receipts_and_audio_fail_closed(self) -> None:
        first = self.item(1)
        second = self.item(2)
        second["evidence"]["receipt"]["receipt_id"] = first["evidence"][
            "receipt"
        ]["receipt_id"]
        with self.assertRaisesRegex(HANDOFF.QueueError, "duplicate receipt ID"):
            self.pure_manifest(self.origin(2), [first, second])

        second["evidence"]["receipt"]["receipt_id"] = (
            "ppreceipt_" + f"{2:032x}"
        )
        second["audio"]["sha256"] = first["audio"]["sha256"]
        with self.assertRaisesRegex(HANDOFF.QueueError, "duplicate audio content"):
            self.pure_manifest(self.origin(2), [first, second])

    def test_maximum_128_receipts_is_enforced_before_member_work(self) -> None:
        with self.assertRaisesRegex(HANDOFF.QueueError, r"\[1, 128\]"):
            self.pure_manifest(self.origin(129), [])

        with mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            return_value=(self.origin(129), []),
        ), mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ), mock.patch.object(HANDOFF, "_retained_audio_inputs") as retention:
            with self.assertRaisesRegex(HANDOFF.QueueError, r"\[1, 128\]"):
                HANDOFF.build_queue(**self.build_kwargs())
        retention.assert_not_called()

    def test_no_audio_only_receipt_set_seals_as_explicit_skip_queue(self) -> None:
        skipped = self.skip(1)
        manifest = self.pure_manifest(self.origin(1, skips=[skipped]), [])
        self.assertEqual(manifest["members"], [])
        self.assertEqual(manifest["totals"]["member_count"], 0)
        self.assertEqual(manifest["totals"]["explicit_skip_count"], 1)
        self.assertEqual(
            manifest["explicit_skips"][0]["disposition"],
            {
                "state": "skipped",
                "reason": "source_has_no_audio",
                "source_audio_stream_present": False,
                "normalized_audio_operation_enabled": True,
            },
        )

    def test_every_private_handling_descriptor_is_carried(self) -> None:
        item = self.item(1)
        skipped = self.skip(2)
        control = {"entries": [{"ordinal": 1}, {"ordinal": 2}]}
        handling = {
            1: {"descriptor": "eligible-boundary"},
            2: {"descriptor": "skipped-boundary"},
        }
        manifest = self.pure_manifest(
            self.origin(2, skips=[skipped], handling_control=control),
            [item],
            handling=handling,
        )
        self.assertEqual(manifest["members"][0]["private_handling"], handling[1])
        self.assertEqual(
            manifest["explicit_skips"][0]["private_handling"], handling[2]
        )
        self.assertEqual(
            manifest["totals"]["private_handling_descriptor_count"], 2
        )
        with self.assertRaisesRegex(HANDOFF.QueueError, "descriptor set"):
            self.pure_manifest(
                self.origin(2, skips=[skipped], handling_control=control),
                [item],
                handling={1: handling[1]},
            )

    def test_build_imports_v03_replay_and_binds_profile_and_portable_root(self) -> None:
        item = self.item(1)
        origin = self.origin(1)
        with mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            return_value=(origin, [item]),
        ) as replay, mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ), mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "load_registration",
            wraps=HANDOFF.PORTABLE_ROOT.load_registration,
        ) as registration_loader:
            manifest = HANDOFF.build_queue(**self.build_kwargs())
        replay.assert_called_once_with(self.bundle, self.state)
        self.assertEqual(
            registration_loader.call_args.kwargs["expected_document_uid"],
            os.geteuid(),
        )
        self.assertEqual(
            registration_loader.call_args.kwargs["expected_document_mode"], 0o400
        )
        self.assertEqual(
            manifest["portable_root_registration"]["registration_id"],
            self.registration["registration_id"],
        )
        self.assertEqual(
            manifest["production_profile"]["document"], self.profile
        )
        self.assertEqual(
            manifest["production_profile"]["reference"]["physical_sha256"],
            digest(self.profile_path.read_bytes()),
        )
        forbidden = {
            "st_dev",
            "st_ino",
            "inode",
            "device",
            "mount_id",
            "st_mtime_ns",
            "st_ctime_ns",
        }

        def keys(value: object) -> set[str]:
            result: set[str] = set()
            if isinstance(value, dict):
                for key, child in value.items():
                    result.add(str(key))
                    result.update(keys(child))
            elif isinstance(value, list):
                for child in value:
                    result.update(keys(child))
            return result

        self.assertTrue(keys(manifest).isdisjoint(forbidden))

    def test_audio_mode_is_exactly_observed_and_group_read_is_rejected(self) -> None:
        item = self.item(1)
        audio_path = Path(item["audio"]["path"])
        audio_path.chmod(0o444)
        origin = self.origin(1)
        with mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            return_value=(origin, [item]),
        ), mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ):
            manifest = HANDOFF.build_queue(**self.build_kwargs())
            self.assertEqual(manifest["members"][0]["audio"]["sealed_mode"], "0444")
            audio_path.chmod(0o440)
            with self.assertRaisesRegex(HANDOFF.QueueError, "audio retention"):
                HANDOFF.build_queue(**self.build_kwargs())

    def test_hardlinked_audio_is_rejected_before_queue_identity(self) -> None:
        item = self.item(1)
        audio_path = Path(item["audio"]["path"])
        alias = self.data / "hardlink-alias.flac"
        os.link(audio_path, alias)
        origin = self.origin(1)
        with mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            return_value=(origin, [item]),
        ), mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ):
            with self.assertRaisesRegex(HANDOFF.QueueError, "audio retention"):
                HANDOFF.build_queue(**self.build_kwargs())

    def test_ready_member_projects_exactly_into_gpu_v5_contract(self) -> None:
        manifest = self.pure_manifest(self.origin(1), [self.item(1)])
        member = manifest["members"][0]
        input_value = ASR_V5.input_from_preprocess_descriptor(member)
        lineage = ASR_V5.source_lineage_from_preprocess_descriptor(
            member, manifest
        )
        hot_root = ASR_V5.hot_root_from_gpu_queue_manifest(manifest)
        profile_reference, profile = ASR_V5.profile_from_gpu_queue_manifest(
            manifest
        )
        self.assertEqual(input_value["sealed_mode"], "0400")
        self.assertEqual(
            lineage["gpu_handoff"]["member_id"], member["member_id"]
        )
        self.assertEqual(
            hot_root["registration_id"],
            manifest["portable_root_registration"]["registration_id"],
        )
        self.assertEqual(profile, self.profile)
        self.assertEqual(profile_reference["profile_id"], self.profile["profile_id"])

    def test_profile_must_be_canonical_with_exact_control_mode(self) -> None:
        item = self.item(1)
        origin = self.origin(1)
        with mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            return_value=(origin, [item]),
        ), mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ):
            self.profile_path.chmod(0o600)
            with self.assertRaisesRegex(HANDOFF.QueueError, "profile retention"):
                HANDOFF.build_queue(**self.build_kwargs())
            if os.geteuid() != 0:
                self.profile_path.chmod(0o444)
                with self.assertRaisesRegex(HANDOFF.QueueError, "profile retention"):
                    HANDOFF.build_queue(**self.build_kwargs())
            self.profile_path.chmod(0o600)
            self.profile_path.write_text(
                json.dumps(self.profile, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            self.profile_path.chmod(0o400)
            with self.assertRaisesRegex(HANDOFF.QueueError, "not canonical"):
                HANDOFF.build_queue(**self.build_kwargs())

    def test_materialization_is_sealed_idempotent_and_validatable(self) -> None:
        item = self.item(1)
        origin = self.origin(1)
        with mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            return_value=(origin, [item]),
        ), mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ):
            first, path = HANDOFF.materialize_queue(**self.build_kwargs())
            second, second_path = HANDOFF.materialize_queue(**self.build_kwargs())
            validated = HANDOFF.validate_queue(
                manifest_path=path,
                root_registration_path=self.registration_path,
                root_registration_sha256=self.registration_digest,
            )
        self.assertEqual(first, second)
        self.assertEqual(first, validated)
        self.assertEqual(path, second_path)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o400)
        self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o500)
        self.assertEqual(path.read_bytes(), HANDOFF.canonical_bytes(first))
        self.assertEqual(
            {child.name for child in path.parent.iterdir()}, {"manifest.json"}
        )

    def test_admitted_queue_loader_never_replays_receipts_or_audio(self) -> None:
        item = self.item(1)
        origin = self.origin(1)
        with mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            return_value=(origin, [item]),
        ), mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ):
            expected, path = HANDOFF.materialize_queue(**self.build_kwargs())
        manifest_digest = digest(path.read_bytes())

        with mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ), mock.patch.object(
            HANDOFF,
            "build_queue",
            side_effect=AssertionError("admitted restore rebuilt queue"),
        ), mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            side_effect=AssertionError("admitted restore replayed receipts"),
        ):
            restored = HANDOFF.load_admitted_queue(
                manifest_path=path,
                expected_manifest_sha256=manifest_digest,
                root_registration_path=self.registration_path,
                root_registration_sha256=self.registration_digest,
            )
        self.assertEqual(expected, restored)

        with mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ), self.assertRaisesRegex(HANDOFF.QueueError, "admitted SHA-256"):
            HANDOFF.load_admitted_queue(
                manifest_path=path,
                expected_manifest_sha256="0" * 64,
                root_registration_path=self.registration_path,
                root_registration_sha256=self.registration_digest,
            )

    def test_writer_lock_is_nonblocking(self) -> None:
        item = self.item(1)
        origin = self.origin(1)
        busy = BlockingIOError(errno.EAGAIN, "busy")
        with mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            return_value=(origin, [item]),
        ), mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ), mock.patch.object(HANDOFF.fcntl, "flock", side_effect=busy):
            with self.assertRaisesRegex(HANDOFF.QueueError, "writer lock is busy"):
                HANDOFF.materialize_queue(**self.build_kwargs())
        queues = self.queue_root / "queues"
        self.assertTrue(queues.is_dir())
        self.assertFalse(
            any(path.name.startswith("gpuasrqueue_") for path in queues.iterdir())
        )

    def test_sealed_queue_path_replacement_during_verification_is_rejected(
        self,
    ) -> None:
        item = self.item(1)
        origin = self.origin(1)
        with mock.patch.object(
            HANDOFF.RECEIPT_REPLAY,
            "_collect_sealed_items",
            return_value=(origin, [item]),
        ), mock.patch.object(
            HANDOFF.PORTABLE_ROOT,
            "btrfs_filesystem_uuid",
            return_value=TEST_UUID,
        ):
            _manifest, manifest_path = HANDOFF.materialize_queue(
                **self.build_kwargs()
            )
            original_reader = HANDOFF._read_file_at
            replaced = False

            def replacing_reader(
                parent_fd: int, name: str, owner_uid: int
            ) -> bytes:
                nonlocal replaced
                body = original_reader(parent_fd, name, owner_uid)
                if not replaced:
                    replaced = True
                    queue_dir = manifest_path.parent
                    stale = queue_dir.with_name(queue_dir.name + "-stale")
                    queue_dir.rename(stale)
                    queue_dir.mkdir(mode=0o700)
                    replacement = queue_dir / "manifest.json"
                    replacement.write_bytes(body)
                    replacement.chmod(0o400)
                    queue_dir.chmod(0o500)
                return body

            with mock.patch.object(
                HANDOFF, "_read_file_at", side_effect=replacing_reader
            ):
                with self.assertRaisesRegex(HANDOFF.QueueError, "path changed"):
                    HANDOFF.materialize_queue(**self.build_kwargs())
        self.assertTrue(replaced)


if __name__ == "__main__":
    unittest.main()
