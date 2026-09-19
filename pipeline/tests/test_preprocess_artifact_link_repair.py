from __future__ import annotations

import hashlib
import fcntl
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pipeline.preprocess_artifact_link_repair as repair
from pipeline.preprocess_artifact_link_repair import (
    LinkRepairError,
    apply_plan,
    build_plan,
    validate_plan,
)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


class PreprocessArtifactLinkRepairTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.case = Path(self.temporary.name).resolve()
        self.root = self.case / "preprocess-output"
        self.control = self.case / "repair-control"
        self.root.mkdir(mode=0o700)
        self.control.mkdir(mode=0o700)
        self.state = self.case / "state"
        self.state.mkdir(mode=0o700)
        self.controller_lock = self.state / "controller.lock"
        self.controller_lock.touch(mode=0o600)
        self.controller_lock.chmod(0o600)
        source_sha256 = "a" * 64
        recipe_sha256 = "b" * 64
        self.recipe_dir = (
            self.root
            / "media"
            / "sha256"
            / source_sha256[:2]
            / source_sha256
            / "recipes"
            / recipe_sha256
        )
        self.recipe_dir.mkdir(parents=True, mode=0o700)
        for parent in [self.root, *self.recipe_dir.parents]:
            if parent == self.case or self.case not in parent.parents:
                continue
            parent.chmod(0o700)
        self.lock = self.recipe_dir / ".preprocess.lock"
        self.lock.touch(mode=0o600)
        self.lock.chmod(0o600)
        executions = self.recipe_dir / "executions"
        first_run = executions / ("run_preprocess_" + "1" * 32)
        second_run = executions / ("run_preprocess_" + "2" * 32)
        first_artifact = first_run / "artifacts" / "audio-16khz-mono.flac"
        self.artifact = second_run / "artifacts" / "audio-16khz-mono.flac"
        first_artifact.parent.mkdir(parents=True, mode=0o700)
        self.artifact.parent.mkdir(parents=True, mode=0o700)
        for parent in (
            executions,
            first_run,
            first_artifact.parent,
            second_run,
            self.artifact.parent,
        ):
            parent.chmod(0o700)
        self.audio_body = (b"fLaC\x00HIMR-link-repair-test\n" * 4096) + b"end"
        first_artifact.write_bytes(self.audio_body)
        first_artifact.chmod(0o444)
        os.link(first_artifact, self.artifact)
        self.sibling = first_artifact
        self.result = second_run / "result.json"
        result_value = {
            "schema_version": 1,
            "status": "completed",
            "result_path": str(self.result),
            "layout": {
                "output_root": str(self.root),
                "recipe_dir": str(self.recipe_dir),
                "run_dir": str(second_run),
            },
            "processing_run": {
                "implementation_version": "0.3.3",
                "processing_run_id": second_run.name,
                "status": "completed",
            },
            "reuse": {"mode": "verified_prior_result"},
            "artifacts": [
                {
                    "artifact_id": "artifact_"
                    + hashlib.sha256(
                        json.dumps(
                            {
                                "kind": "audio_16khz_mono_flac",
                                "processing_run_id": second_run.name,
                                "sha256": hashlib.sha256(self.audio_body).hexdigest(),
                            },
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode("utf-8")
                    ).hexdigest()[:32],
                    "artifact_kind": "audio_16khz_mono_flac",
                    "byte_count": len(self.audio_body),
                    "media_kind": "audio",
                    "mime_type": "audio/flac",
                    "path": str(self.artifact),
                    "processing_run_id": second_run.name,
                    "schema_version": 1,
                    "sha256": hashlib.sha256(self.audio_body).hexdigest(),
                    "storage_uri": self.artifact.as_uri(),
                    "visibility": "private",
                }
            ],
        }
        self.result.write_text(
            json.dumps(result_value, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.result.chmod(0o444)
        self.plan = self.control / "repair-plan.json"
        self.receipt = self.control / "repair-receipt.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_plan_apply_detaches_exact_bytes_and_preserves_result(self) -> None:
        shared_inode = self.artifact.stat().st_ino
        result_before = self.result.read_bytes()
        result_stat_before = self.result.stat()

        planned = build_plan(
            self.root, self.result, self.plan, self.controller_lock
        )
        self.assertEqual(planned["state"], "prepared_not_applied")
        self.assertEqual(self.artifact.stat().st_ino, shared_inode)
        self.assertEqual(self.artifact.stat().st_nlink, 2)
        self.assertEqual(validate_plan(self.plan)["state"], "valid_not_applied")

        applied = apply_plan(self.plan, self.receipt, self.controller_lock)
        self.assertEqual(applied["state"], "applied")
        self.assertNotEqual(self.artifact.stat().st_ino, shared_inode)
        self.assertEqual(self.artifact.stat().st_nlink, 1)
        self.assertEqual(self.sibling.stat().st_nlink, 1)
        self.assertEqual(self.artifact.read_bytes(), self.audio_body)
        self.assertEqual(digest(self.artifact), hashlib.sha256(self.audio_body).hexdigest())
        self.assertEqual(stat.S_IMODE(self.artifact.stat().st_mode), 0o444)
        self.assertEqual(self.result.read_bytes(), result_before)
        result_stat_after = self.result.stat()
        self.assertEqual(
            (result_stat_after.st_dev, result_stat_after.st_ino, result_stat_after.st_mode),
            (result_stat_before.st_dev, result_stat_before.st_ino, result_stat_before.st_mode),
        )
        self.assertEqual(stat.S_IMODE(self.plan.stat().st_mode), 0o400)
        self.assertEqual(self.plan.stat().st_nlink, 1)
        self.assertEqual(stat.S_IMODE(self.receipt.stat().st_mode), 0o400)
        self.assertEqual(self.receipt.stat().st_nlink, 1)
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertFalse(receipt["existing_documents"]["receipts_modified"])
        self.assertFalse(receipt["existing_documents"]["result_envelope_modified"])
        self.assertEqual(receipt["artifact"]["before"]["link_count"], 2)
        self.assertEqual(receipt["artifact"]["outcome"], "replaced")
        self.assertEqual(receipt["artifact"]["replacement_after"]["link_count"], 1)
        self.assertEqual(receipt["artifact"]["detached_source_after"]["link_count"], 1)

    def test_plan_rejects_an_already_single_link_artifact(self) -> None:
        self.sibling.unlink()
        with self.assertRaisesRegex(LinkRepairError, "at least 2 links"):
            build_plan(self.root, self.result, self.plan, self.controller_lock)
        self.assertFalse(self.plan.exists())

    def test_apply_reconciles_exact_single_link_inode_swap_without_overclaim(self) -> None:
        build_plan(self.root, self.result, self.plan, self.controller_lock)
        result_before = self.result.read_bytes()
        replacement = self.artifact.with_name("replacement.flac")
        replacement.write_bytes(self.audio_body)
        replacement.chmod(0o444)
        os.replace(replacement, self.artifact)
        replacement_inode = self.artifact.stat().st_ino

        applied = apply_plan(self.plan, self.receipt, self.controller_lock)
        self.assertEqual(applied["state"], "applied")
        self.assertEqual(self.artifact.stat().st_ino, replacement_inode)
        self.assertEqual(self.artifact.read_bytes(), self.audio_body)
        self.assertEqual(self.result.read_bytes(), result_before)
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual(
            receipt["artifact"]["outcome"], "reconciled_exact_single_link"
        )
        self.assertIsNone(receipt["artifact"]["detached_source_after"])

    def test_retry_reconciles_post_replace_directory_fsync_failure(self) -> None:
        build_plan(self.root, self.result, self.plan, self.controller_lock)
        real_fsync = os.fsync
        calls = 0

        def fail_third_fsync(descriptor: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise OSError("injected post-replace directory fsync failure")
            real_fsync(descriptor)

        with mock.patch.object(repair.os, "fsync", side_effect=fail_third_fsync):
            with self.assertRaisesRegex(LinkRepairError, "injected post-replace"):
                apply_plan(self.plan, self.receipt, self.controller_lock)
        self.assertEqual(self.artifact.stat().st_nlink, 1)
        self.assertFalse(self.receipt.exists())

        replay = apply_plan(self.plan, self.receipt, self.controller_lock)
        self.assertEqual(replay["state"], "applied")
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual(
            receipt["artifact"]["outcome"], "recovered_exchange"
        )
        self.assertIsNotNone(receipt["artifact"]["detached_source_after"])

    def test_retry_reconciles_failure_before_receipt_publication(self) -> None:
        build_plan(self.root, self.result, self.plan, self.controller_lock)
        with mock.patch.object(
            repair,
            "_publish_control_json",
            side_effect=LinkRepairError("injected receipt publication failure"),
        ):
            with self.assertRaisesRegex(LinkRepairError, "receipt publication"):
                apply_plan(self.plan, self.receipt, self.controller_lock)
        self.assertEqual(self.artifact.stat().st_nlink, 1)
        self.assertFalse(self.receipt.exists())

        apply_plan(self.plan, self.receipt, self.controller_lock)
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual(
            receipt["artifact"]["outcome"], "reconciled_exact_single_link"
        )

    def test_retry_replays_receipt_published_before_reported_failure(self) -> None:
        build_plan(self.root, self.result, self.plan, self.controller_lock)
        real_publish = repair._publish_control_json

        def publish_then_fail(*args, **kwargs):
            real_publish(*args, **kwargs)
            raise LinkRepairError("injected post-publication failure")

        with mock.patch.object(
            repair, "_publish_control_json", side_effect=publish_then_fail
        ):
            with self.assertRaisesRegex(LinkRepairError, "post-publication"):
                apply_plan(self.plan, self.receipt, self.controller_lock)
        self.assertTrue(self.receipt.is_file())

        replay = apply_plan(self.plan, self.receipt, self.controller_lock)
        self.assertEqual(replay["state"], "applied_or_exact_replay")
        self.assertEqual(self.artifact.stat().st_nlink, 1)

    def test_validate_rejects_noncanonical_plan_bytes(self) -> None:
        build_plan(self.root, self.result, self.plan, self.controller_lock)
        value = json.loads(self.plan.read_text(encoding="utf-8"))
        self.plan.chmod(0o600)
        self.plan.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        self.plan.chmod(0o400)
        with self.assertRaisesRegex(LinkRepairError, "not exact canonical"):
            validate_plan(self.plan)

    def test_plan_postpublication_failure_leaves_single_link_replayable_plan(self) -> None:
        real_fsync = os.fsync
        calls = 0

        def fail_parent_fsync(descriptor: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise OSError("injected plan parent fsync failure")
            real_fsync(descriptor)

        with mock.patch.object(repair.os, "fsync", side_effect=fail_parent_fsync):
            with self.assertRaisesRegex(LinkRepairError, "plan parent fsync"):
                build_plan(
                    self.root, self.result, self.plan, self.controller_lock
                )
        self.assertTrue(self.plan.is_file())
        self.assertEqual(self.plan.stat().st_nlink, 1)
        self.assertEqual(stat.S_IMODE(self.plan.stat().st_mode), 0o400)
        self.assertEqual(validate_plan(self.plan)["state"], "valid_not_applied")

    def test_apply_rejects_link_count_change_after_plan(self) -> None:
        build_plan(self.root, self.result, self.plan, self.controller_lock)
        third = self.artifact.with_name("third-link.flac")
        os.link(self.artifact, third)
        result_before = self.result.read_bytes()

        with self.assertRaisesRegex(LinkRepairError, "differs from both"):
            apply_plan(self.plan, self.receipt, self.controller_lock)
        self.assertEqual(self.artifact.stat().st_nlink, 3)
        self.assertEqual(self.result.read_bytes(), result_before)
        self.assertFalse(self.receipt.exists())

    def test_atomic_exchange_restores_a_target_swapped_after_precheck(self) -> None:
        build_plan(self.root, self.result, self.plan, self.controller_lock)
        result_before = self.result.read_bytes()
        attacker_body = b"different entry installed at the exchange boundary"
        attacker = self.artifact.with_name("attacker.flac")
        attacker.write_bytes(attacker_body)
        attacker.chmod(0o444)
        real_exchange = repair._rename_exchange
        raced = False

        def race_then_exchange(directory_fd: int, left: str, right: str) -> None:
            nonlocal raced
            if not raced:
                raced = True
                os.replace(attacker, self.artifact)
            real_exchange(directory_fd, left, right)

        with mock.patch.object(
            repair, "_rename_exchange", side_effect=race_then_exchange
        ):
            with self.assertRaisesRegex(LinkRepairError, "exchange boundary"):
                apply_plan(self.plan, self.receipt, self.controller_lock)
        self.assertEqual(self.artifact.read_bytes(), attacker_body)
        self.assertEqual(self.result.read_bytes(), result_before)
        self.assertFalse(self.receipt.exists())
        self.assertEqual(
            list(self.artifact.parent.glob(".*.repair_plan_*.swap")), []
        )

    def test_apply_rejects_an_occupied_controller_lock(self) -> None:
        build_plan(self.root, self.result, self.plan, self.controller_lock)
        result_before = self.result.read_bytes()
        with self.controller_lock.open("r+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(LinkRepairError, "controller may be active"):
                apply_plan(self.plan, self.receipt, self.controller_lock)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        self.assertEqual(self.artifact.stat().st_nlink, 2)
        self.assertEqual(self.result.read_bytes(), result_before)
        self.assertFalse(self.receipt.exists())


if __name__ == "__main__":
    unittest.main()
