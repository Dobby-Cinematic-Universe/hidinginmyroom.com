from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import stat
import sys
import threading
import unittest
from pathlib import Path
from unittest import mock

from jsonschema.validators import Draft202012Validator


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / f"contextual-result-seal-{os.getpid()}"
sys.path.insert(0, str(PIPELINE_ROOT))

import contextual_asr_result_store_seal as seal  # noqa: E402


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def write_json(path: Path, value: object, mode: int, *, pretty: bool = False) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    body = (
        (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
        if pretty
        else canonical_bytes(value) + b"\n"
    )
    path.write_bytes(body)
    path.chmod(mode)
    return body


def remove_tree() -> None:
    if not TEST_ROOT.exists():
        return
    for current, directories, files in os.walk(TEST_ROOT, topdown=False):
        current_path = Path(current)
        for name in files:
            path = current_path / name
            if not path.is_symlink():
                path.chmod(0o600)
        for name in directories:
            path = current_path / name
            if not path.is_symlink():
                path.chmod(0o700)
        if not current_path.is_symlink():
            current_path.chmod(0o700)
    shutil.rmtree(TEST_ROOT)


def write_reidentified_plan(document: dict[str, object]) -> Path:
    semantic = {
        key: value
        for key, value in document.items()
        if key not in {"identity_sha256", "plan_id", "receipt_path"}
    }
    identity = seal._document_identity(semantic, set())
    document["identity_sha256"] = identity
    document["plan_id"] = f"ctxasrsealplan_{identity[:32]}"
    source = document["source"]
    assert isinstance(source, dict)
    root = Path(str(document["store_root"]))
    control = seal._control_root(root, str(source["batch_id"]))
    for directory in (root, control, control / "plans", control / "receipts"):
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory.chmod(0o700)
    document["receipt_path"] = str(
        control / "receipts" / f"ctxasrsealreceipt_{identity[:32]}.json"
    )
    path = control / "plans" / f"ctxasrsealplan_{identity[:32]}.json"
    write_json(path, document, 0o400)
    return path


class FakeContextualBatch:
    def validate_batch(self, manifest_path: Path):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        orders = [
            json.loads(
                (manifest_path.parent / entry["path"]).read_text(encoding="utf-8")
            )
            for entry in manifest["work_orders"]
        ]
        return manifest, orders

    @staticmethod
    def _validate_adapter_result(
        result,
        order,
        entry,
        glossary,
        engine,
        model,
        *,
        dry_run,
    ):
        if dry_run or result["job_id"] != order["job_id"]:
            raise RuntimeError("fake contextual mismatch")
        expected_order = digest(canonical_bytes(order))
        if result["work_order_sha256"] != expected_order:
            raise RuntimeError("fake contextual work-order mismatch")
        return {
            "baseline_result_canonical_sha256": entry["baseline"]["canonical_sha256"],
            "glossary_revision_id": glossary["glossary_revision_id"],
            "processing_run_id": result["processing_run"]["processing_run_id"],
            "recipe_id": result["recipe_id"],
            "result_key": result["result_key"],
            "result_path": result["result_path"],
            "status": "completed",
            "work_order_sha256": expected_order,
        }


class SyntheticContextualStore:
    def __init__(self, count: int = 2) -> None:
        self.store = TEST_ROOT / "private-contextual-asr-results"
        self.store.mkdir(parents=True, mode=0o700)
        self.store.chmod(0o700)
        self.batch_root = TEST_ROOT / "private-contextual-asr-work-orders"
        self.batch_root.mkdir(parents=True, mode=0o700)
        identity = "a" * 64
        self.batch_id = f"ctxasrbatch_{identity[:32]}"
        self.batch_dir = self.batch_root / "batches" / self.batch_id
        self.orders_dir = self.batch_dir / "work-orders"
        self.orders_dir.mkdir(parents=True, mode=0o700)
        self.results: list[Path] = []
        self.orders: list[dict[str, object]] = []
        entries = []
        for ordinal in range(1, count + 1):
            input_sha = f"{ordinal:064x}"
            job_id = f"asr-contextual-{ordinal:032x}"
            order = {
                "catalog_context": None,
                "input": {
                    "artifact_id": f"artifact_{ordinal:032x}",
                    "expected_sha256": input_sha,
                    "media_id": f"media_sha256_{input_sha}",
                    "parent_processing_run_id": f"run_preprocess_{ordinal:032x}",
                    "path": str(TEST_ROOT / f"input-{ordinal}.flac"),
                },
                "job_id": job_id,
                "output": {"root": str(self.store)},
            }
            body = write_json(
                self.orders_dir / f"{ordinal:06d}.json", order, 0o400, pretty=True
            )
            canonical_sha = digest(canonical_bytes(order))
            result_key = digest(f"contextual-result-{ordinal}".encode())
            result_path = self._make_result(
                ordinal, order, canonical_sha, input_sha, result_key
            )
            self.results.append(result_path)
            self.orders.append(order)
            entries.append(
                {
                    "baseline": {"canonical_sha256": f"{ordinal + 100:064x}"},
                    "byte_count": len(body),
                    "canonical_sha256": canonical_sha,
                    "input": {"sha256": input_sha},
                    "job_id": job_id,
                    "ordinal": ordinal,
                    "path": f"work-orders/{ordinal:06d}.json",
                    "sha256": digest(body),
                }
            )
        self.manifest = {
            "batch_id": self.batch_id,
            "glossary": {"glossary_revision_id": "glossary_neutral_test_v1"},
            "identity_sha256": identity,
            "engine": {},
            "model": {},
            "output": {
                "asr_output_root": str(self.store),
                "batch_root": str(self.batch_root),
            },
            "work_order_count": count,
            "work_orders": entries,
        }
        self.manifest_path = self.batch_dir / "manifest.json"
        write_json(self.manifest_path, self.manifest, 0o400, pretty=True)
        self.orders_dir.chmod(0o500)
        self.batch_dir.chmod(0o500)

    def _make_result(
        self,
        ordinal: int,
        order: dict[str, object],
        order_sha: str,
        input_sha: str,
        result_key: str,
    ) -> Path:
        directory = (
            self.store
            / "asr"
            / "whispercpp"
            / "sha256"
            / input_sha[:2]
            / input_sha
            / "results"
            / result_key
        )
        directory.mkdir(parents=True, mode=0o700)
        current = self.store
        for component in directory.relative_to(self.store).parts:
            current = current / component
            current.chmod(0o700)
        normalized = {"segments": [{"text": f"PRIVATE SENTINEL {ordinal}"}]}
        raw_whisper = {"transcription": [{"text": f"RAW PRIVATE {ordinal}"}]}
        normalized_path = directory / "transcript.normalized.json"
        raw_path = directory / "whisper.raw.json"
        normalized_body = write_json(
            normalized_path, normalized, 0o600 if ordinal == 1 else 0o644
        )
        raw_body = write_json(raw_path, raw_whisper, 0o644)
        recipe_id = f"recipe_asr_whispercpp_{ordinal:032x}"
        result_path = directory / "result.json"
        result = {
            "artifacts": [
                {
                    "artifact_kind": "whispercpp_output_json_full",
                    "byte_count": len(raw_body),
                    "sha256": digest(raw_body),
                    "storage_uri": raw_path.as_uri(),
                    "visibility": "private",
                },
                {
                    "artifact_kind": "transcript_normalized_json",
                    "byte_count": len(normalized_body),
                    "sha256": digest(normalized_body),
                    "storage_uri": normalized_path.as_uri(),
                    "visibility": "private",
                },
            ],
            "input": {"sha256": input_sha},
            "job_id": order["job_id"],
            "processing_run": {
                "processing_run_id": f"run_asr_whispercpp_{ordinal:032x}"
            },
            "recipe_id": recipe_id,
            "result_key": result_key,
            "result_path": str(result_path),
            "work_order_sha256": order_sha,
        }
        write_json(result_path, result, 0o644)
        directory.chmod(0o700)
        return result_path

    @staticmethod
    def validator(path: Path) -> dict[str, object]:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {
            "artifact_count": 2,
            "glossary_revision_id": "glossary_neutral_test_v1",
            "job_id": raw["job_id"],
            "model_id": "model_test",
            "processing_run_id": raw["processing_run"]["processing_run_id"],
            "recipe_id": raw["recipe_id"],
            "recording_id": None,
            "rendition_id": None,
            "result_envelope_sha256": digest(canonical_bytes(raw)),
            "result_key": raw["result_key"],
            "segment_count": 1,
            "token_count": 3,
        }

    def plan(self, *, dry_run: bool = False):
        return seal.build_plan(
            manifest_path=self.manifest_path,
            result_paths=self.results,
            dry_run=dry_run,
            validator=self.validator,
            created_at="2026-08-27T12:00:00Z",
        )


class ContextualResultSealTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan_schema = json.loads(
            (PIPELINE_ROOT / "schemas/contextual-asr-result-seal-plan.schema.json").read_text(
                encoding="utf-8"
            )
        )
        cls.receipt_schema = json.loads(
            (PIPELINE_ROOT / "schemas/contextual-asr-result-seal-receipt.schema.json").read_text(
                encoding="utf-8"
            )
        )

    def setUp(self) -> None:
        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        TEST_ROOT.chmod(0o700)
        self.fake = FakeContextualBatch()
        self.module_patch = mock.patch.object(
            seal, "_contextual_module", return_value=self.fake
        )
        self.module_patch.start()

    def tearDown(self) -> None:
        self.module_patch.stop()
        remove_tree()

    def test_dry_run_is_exact_and_writes_nothing(self) -> None:
        fixture = SyntheticContextualStore()
        before = {
            path: (
                path.read_bytes(),
                path.stat().st_mtime_ns,
                stat.S_IMODE(path.stat().st_mode),
            )
            for result in fixture.results
            for path in (
                result,
                result.parent / "transcript.normalized.json",
                result.parent / "whisper.raw.json",
            )
        }
        with mock.patch.object(seal, "_atomic_control_json") as writer:
            summary = fixture.plan(dry_run=True)
        self.assertEqual(summary["state"], "dry_run_valid_no_writes")
        self.assertEqual(summary["result_count"], 2)
        writer.assert_not_called()
        self.assertFalse((fixture.store / "contextual-sealing-control").exists())
        self.assertEqual(
            before,
            {
                path: (
                    path.read_bytes(),
                    path.stat().st_mtime_ns,
                    stat.S_IMODE(path.stat().st_mode),
                )
                for path in before
            },
        )

    def test_plan_is_schema_valid_text_free_and_manifest_closed(self) -> None:
        fixture = SyntheticContextualStore()
        plan_path = fixture.plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(self.plan_schema)
        Draft202012Validator(self.plan_schema).validate(plan)
        self.assertEqual(stat.S_IMODE(plan_path.stat().st_mode), 0o400)
        self.assertEqual(plan["result_count"], plan["source"]["work_order_count"])
        self.assertEqual(
            [item["ordinal"] for item in plan["results"]], [1, 2]
        )
        body = plan_path.read_text(encoding="utf-8")
        self.assertNotIn("PRIVATE SENTINEL", body)
        self.assertNotIn("RAW PRIVATE", body)
        self.assertEqual(
            seal.validate_plan(plan_path, validator=fixture.validator)["state"],
            "valid_prepared_plan",
        )

    def test_validate_plan_rejects_each_direction_of_pre_mode_drift(self) -> None:
        cases = (
            ("transcript.normalized.json", 0o600, 0o644),
            ("result.json", 0o644, 0o600),
        )
        for name, expected_mode, drift_mode in cases:
            with self.subTest(name=name, drift=f"{expected_mode:04o}->{drift_mode:04o}"):
                remove_tree()
                TEST_ROOT.mkdir(parents=True, mode=0o700)
                TEST_ROOT.chmod(0o700)
                fixture = SyntheticContextualStore()
                plan_path = fixture.plan()
                plan = json.loads(plan_path.read_text(encoding="utf-8"))
                target = next(
                    Path(item["path"])
                    for item in plan["results"][0]["files"]
                    if item["name"] == name
                )
                self.assertEqual(stat.S_IMODE(target.stat().st_mode), expected_mode)
                target.chmod(drift_mode)
                with self.assertRaisesRegex(
                    seal.ContextualSealError, "mode must be one of"
                ):
                    seal.validate_plan(plan_path, validator=fixture.validator)

    def test_forged_reidentified_result_fields_do_not_override_retained_identity(self) -> None:
        fixture = SyntheticContextualStore()
        original = fixture.plan()
        forged = json.loads(original.read_text(encoding="utf-8"))
        result = forged["results"][0]
        forged_job = "asr-contextual-ffffffffffffffffffffffffffffffff"
        forged_key = "f" * 64
        forged_work_order = "e" * 64
        result["job_id"] = forged_job
        result["catalog_free_validation"]["job_id"] = forged_job
        result["result_key"] = forged_key
        result["adapter_validation"]["result_key"] = forged_key
        result["catalog_free_validation"]["result_key"] = forged_key
        result["work_order_sha256"] = forged_work_order
        result["adapter_validation"]["work_order_sha256"] = forged_work_order
        forged_path = write_reidentified_plan(forged)
        with self.assertRaisesRegex(
            seal.ContextualSealError, "stable contextual result identity differs"
        ):
            seal.validate_plan(forged_path, validator=fixture.validator)

    def test_self_consistent_plan_rejects_noncanonical_or_invalid_created_at(self) -> None:
        fixture = SyntheticContextualStore()
        original = fixture.plan()
        base = json.loads(original.read_text(encoding="utf-8"))
        for value in (
            20260827,
            "2026-08-27T12:00:00+00:00",
            "2026-02-30T12:00:00Z",
        ):
            with self.subTest(created_at=value):
                forged = json.loads(json.dumps(base))
                forged["created_at"] = value
                forged_path = write_reidentified_plan(forged)
                with self.assertRaisesRegex(
                    seal.ContextualSealError,
                    "plan created_at.*canonical|plan created_at.*real UTC",
                ):
                    seal.validate_plan(forged_path, validator=fixture.validator)

    def test_forged_alternate_control_root_cannot_validate_or_apply(self) -> None:
        fixture = SyntheticContextualStore()
        original = fixture.plan()
        forged = json.loads(original.read_text(encoding="utf-8"))
        alternate = TEST_ROOT / "alternate-private-root"
        alternate.mkdir(mode=0o700)
        alternate.chmod(0o700)
        forged["store_root"] = str(alternate)
        forged_path = write_reidentified_plan(forged)
        expected_modes = {
            path: stat.S_IMODE(path.stat().st_mode)
            for result in fixture.results
            for path in (
                result.parent,
                result,
                result.parent / "transcript.normalized.json",
                result.parent / "whisper.raw.json",
            )
        }
        for operation in (
            lambda: seal.validate_plan(forged_path, validator=fixture.validator),
            lambda: seal.apply_plan(forged_path, validator=fixture.validator),
        ):
            with self.assertRaisesRegex(
                seal.ContextualSealError,
                "store root differs from the manifest-bound ASR output root",
            ):
                operation()
        self.assertEqual(
            expected_modes,
            {path: stat.S_IMODE(path.stat().st_mode) for path in expected_modes},
        )

    def test_apply_is_mode_only_receipted_and_idempotent(self) -> None:
        fixture = SyntheticContextualStore()
        plan_path = fixture.plan()
        files = [
            path
            for result in fixture.results
            for path in (
                result,
                result.parent / "transcript.normalized.json",
                result.parent / "whisper.raw.json",
            )
        ]
        before_files = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in files}
        before_directories = {
            result.parent: result.parent.stat().st_mtime_ns for result in fixture.results
        }
        receipt_path = seal.apply_plan(
            plan_path,
            validator=fixture.validator,
            applied_at="2026-08-27T12:01:00Z",
        )
        receipt_body = receipt_path.read_bytes()
        receipt = json.loads(receipt_body)
        Draft202012Validator.check_schema(self.receipt_schema)
        Draft202012Validator(self.receipt_schema).validate(receipt)
        for path, expected in before_files.items():
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), expected)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o400)
            self.assertEqual(path.stat().st_nlink, 1)
        for path, expected_mtime in before_directories.items():
            self.assertEqual(path.stat().st_mtime_ns, expected_mtime)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o500)
            self.assertEqual(set(os.listdir(path)), set(seal.RESULT_FILENAMES))
        self.assertEqual(
            seal.validate_receipt(receipt_path, validator=fixture.validator)["state"],
            "valid_applied_receipt",
        )
        replay = seal.apply_plan(plan_path, validator=fixture.validator)
        self.assertEqual(replay, receipt_path)
        self.assertEqual(replay.read_bytes(), receipt_body)

    def test_self_consistent_receipt_rejects_wrong_type_or_invalid_applied_at(self) -> None:
        for value in (20260827, "2026-02-30T12:01:00Z"):
            with self.subTest(applied_at=value):
                remove_tree()
                TEST_ROOT.mkdir(parents=True, mode=0o700)
                TEST_ROOT.chmod(0o700)
                fixture = SyntheticContextualStore()
                plan_path = fixture.plan()
                receipt_path = seal.apply_plan(
                    plan_path,
                    validator=fixture.validator,
                    applied_at="2026-08-27T12:01:00Z",
                )
                forged = json.loads(receipt_path.read_text(encoding="utf-8"))
                forged["applied_at"] = value
                semantic = {
                    key: item
                    for key, item in forged.items()
                    if key not in {"identity_sha256", "receipt_id"}
                }
                forged["identity_sha256"] = seal._document_identity(semantic, set())
                receipt_path.chmod(0o600)
                write_json(receipt_path, forged, 0o400)
                with self.assertRaisesRegex(
                    seal.ContextualSealError,
                    "receipt applied_at.*canonical|receipt applied_at.*real UTC",
                ):
                    seal.validate_receipt(receipt_path, validator=fixture.validator)

    def test_incomplete_duplicate_and_unselected_sets_are_rejected(self) -> None:
        fixture = SyntheticContextualStore()
        with self.assertRaisesRegex(seal.ContextualSealError, "requires 2 paths"):
            seal.build_plan(
                manifest_path=fixture.manifest_path,
                result_paths=fixture.results[:1],
                dry_run=True,
                validator=fixture.validator,
            )
        with self.assertRaisesRegex(seal.ContextualSealError, "duplicate paths"):
            seal.build_plan(
                manifest_path=fixture.manifest_path,
                result_paths=[fixture.results[0], fixture.results[0]],
                dry_run=True,
                validator=fixture.validator,
            )
        outside = fixture.results[0].parent / "not-result.json"
        outside.write_bytes(fixture.results[0].read_bytes())
        outside.chmod(0o644)
        with self.assertRaisesRegex(seal.ContextualSealError, "entries differ|end in result"):
            seal.build_plan(
                manifest_path=fixture.manifest_path,
                result_paths=[outside, fixture.results[1]],
                dry_run=True,
                validator=fixture.validator,
            )

    def test_extra_entry_hardlink_and_symlink_fail_closed(self) -> None:
        fixture = SyntheticContextualStore()
        extra = fixture.results[0].parent / "unexpected.txt"
        extra.write_text("unexpected", encoding="utf-8")
        with self.assertRaisesRegex(seal.ContextualSealError, "entries differ"):
            fixture.plan(dry_run=True)
        extra.unlink()
        raw = fixture.results[0].parent / "whisper.raw.json"
        normalized = fixture.results[0].parent / "transcript.normalized.json"
        raw.unlink()
        os.link(normalized, raw)
        with self.assertRaisesRegex(seal.ContextualSealError, "single-link|hard link"):
            fixture.plan(dry_run=True)
        raw.unlink()
        raw.symlink_to(fixture.results[1].parent / "whisper.raw.json")
        with self.assertRaisesRegex(seal.ContextualSealError, "symlink|resolved"):
            fixture.plan(dry_run=True)

    def test_path_swap_during_catalog_validation_is_rejected(self) -> None:
        fixture = SyntheticContextualStore()
        swapped = False

        def validator(path: Path):
            nonlocal swapped
            value = fixture.validator(path)
            if not swapped:
                swapped = True
                held = path.with_name("result.held.json")
                path.rename(held)
                shutil.copy2(held, path)
            return value

        with self.assertRaisesRegex(seal.ContextualSealError, "replaced|entries differ"):
            seal.build_plan(
                manifest_path=fixture.manifest_path,
                result_paths=fixture.results,
                dry_run=True,
                validator=validator,
            )

    def test_apply_failure_restores_all_recorded_modes(self) -> None:
        fixture = SyntheticContextualStore()
        plan_path = fixture.plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        expected_modes = {
            Path(item["path"]): item["mode_before"]
            for result in plan["results"]
            for item in result["files"]
        }
        expected_modes.update(
            {Path(result["directory"]["path"]): result["directory"]["mode_before"] for result in plan["results"]}
        )
        real_fchmod = seal.os.fchmod
        calls = 0

        def fail_once(fd: int, mode: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 3:
                raise PermissionError("injected chmod failure")
            real_fchmod(fd, mode)

        with mock.patch.object(seal.os, "fchmod", side_effect=fail_once):
            with self.assertRaisesRegex(seal.ContextualSealError, "modes were restored"):
                seal.apply_plan(plan_path, validator=fixture.validator)
        for path, expected_mode in expected_modes.items():
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), expected_mode)
        self.assertFalse(Path(plan["receipt_path"]).exists())

    def test_mixed_interrupted_state_is_rolled_back_without_receipt(self) -> None:
        fixture = SyntheticContextualStore()
        plan_path = fixture.plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        first = Path(plan["results"][0]["files"][0]["path"])
        first.chmod(0o400)
        with self.assertRaisesRegex(seal.ContextualSealError, "partial.*recorded modes restored"):
            seal.apply_plan(plan_path, validator=fixture.validator)
        for result in plan["results"]:
            self.assertEqual(
                stat.S_IMODE(Path(result["directory"]["path"]).stat().st_mode),
                result["directory"]["mode_before"],
            )
            for item in result["files"]:
                self.assertEqual(stat.S_IMODE(Path(item["path"]).stat().st_mode), item["mode_before"])
        self.assertFalse(Path(plan["receipt_path"]).exists())

    def test_after_directories_with_opposite_pre_modes_are_never_treated_as_sealed(self) -> None:
        fixture = SyntheticContextualStore()
        plan_path = fixture.plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        for result in plan["results"]:
            Path(result["directory"]["path"]).chmod(0o500)
            for item in result["files"]:
                opposite = 0o644 if item["mode_before"] == 0o600 else 0o600
                Path(item["path"]).chmod(opposite)
        with self.assertRaisesRegex(
            seal.ContextualSealError,
            "partial or invalid.*recorded modes restored",
        ):
            seal.apply_plan(plan_path, validator=fixture.validator)
        for result in plan["results"]:
            self.assertEqual(
                stat.S_IMODE(Path(result["directory"]["path"]).stat().st_mode),
                result["directory"]["mode_before"],
            )
            for item in result["files"]:
                self.assertEqual(
                    stat.S_IMODE(Path(item["path"]).stat().st_mode),
                    item["mode_before"],
                )
        self.assertFalse(Path(plan["receipt_path"]).exists())

    def test_control_document_race_never_overwrites_existing_target(self) -> None:
        fixture = SyntheticContextualStore()
        real_link = seal.os.link
        racing_body = b"racing writer\n"

        def racing_link(source, target, **kwargs):
            Path(target).write_bytes(racing_body)
            return real_link(source, target, **kwargs)

        with mock.patch.object(seal.os, "link", side_effect=racing_link):
            with self.assertRaisesRegex(seal.ContextualSealError, "atomically commit"):
                fixture.plan()
        paths = list(
            (fixture.store / "contextual-sealing-control" / fixture.batch_id / "plans").glob(
                "ctxasrsealplan_*.json"
            )
        )
        self.assertEqual(len(paths), 1)
        self.assertEqual(paths[0].read_bytes(), racing_body)

    def test_receipt_commit_race_restores_result_modes(self) -> None:
        fixture = SyntheticContextualStore()
        plan_path = fixture.plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        expected_modes = {
            Path(item["path"]): item["mode_before"]
            for result in plan["results"]
            for item in result["files"]
        }
        expected_modes.update(
            {
                Path(result["directory"]["path"]): result["directory"]["mode_before"]
                for result in plan["results"]
            }
        )
        real_link = seal.os.link
        racing_body = b"competing invalid receipt\n"

        def racing_receipt_link(source, target, **kwargs):
            if Path(target).parent.name == "receipts":
                Path(target).write_bytes(racing_body)
            return real_link(source, target, **kwargs)

        with mock.patch.object(seal.os, "link", side_effect=racing_receipt_link):
            with self.assertRaisesRegex(seal.ContextualSealError, "modes were restored"):
                seal.apply_plan(plan_path, validator=fixture.validator)
        for path, expected_mode in expected_modes.items():
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), expected_mode)
        self.assertEqual(Path(plan["receipt_path"]).read_bytes(), racing_body)

    def test_valid_receipt_winning_eexist_is_kept_with_sealed_targets(self) -> None:
        fixture = SyntheticContextualStore()
        plan_path = fixture.plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        real_link = seal.os.link
        injected = False

        def valid_winner_then_eexist(source, target, **kwargs):
            nonlocal injected
            if Path(target).parent.name == "receipts" and not injected:
                injected = True
                real_link(source, target, **kwargs)
            return real_link(source, target, **kwargs)

        with mock.patch.object(seal.os, "link", side_effect=valid_winner_then_eexist):
            with mock.patch.object(
                seal, "_rollback_modes", wraps=seal._rollback_modes
            ) as rollback:
                receipt_path = seal.apply_plan(plan_path, validator=fixture.validator)
        rollback.assert_not_called()
        self.assertEqual(receipt_path, Path(plan["receipt_path"]))
        self.assertEqual(
            seal.validate_receipt(receipt_path, validator=fixture.validator)["state"],
            "valid_applied_receipt",
        )
        for result in plan["results"]:
            self.assertEqual(
                stat.S_IMODE(Path(result["directory"]["path"]).stat().st_mode), 0o500
            )
            for item in result["files"]:
                self.assertEqual(stat.S_IMODE(Path(item["path"]).stat().st_mode), 0o400)

    def test_concurrent_cooperating_apply_is_serial_and_idempotent(self) -> None:
        fixture = SyntheticContextualStore()
        plan_path = fixture.plan()
        entered_commit = threading.Event()
        release_commit = threading.Event()
        real_atomic = seal._atomic_control_json
        commit_calls = 0
        commit_guard = threading.Lock()

        def held_atomic(path: Path, document: dict[str, object]) -> None:
            nonlocal commit_calls
            if path.parent.name == "receipts":
                with commit_guard:
                    commit_calls += 1
                    first = commit_calls == 1
                if first:
                    entered_commit.set()
                    if not release_commit.wait(timeout=5):
                        raise RuntimeError("test timed out waiting to release receipt commit")
            real_atomic(path, document)

        outcomes: list[Path] = []
        failures: list[BaseException] = []

        def worker() -> None:
            try:
                outcomes.append(
                    seal.apply_plan(plan_path, validator=fixture.validator)
                )
            except BaseException as error:  # pragma: no cover - asserted below
                failures.append(error)

        with mock.patch.object(seal, "_atomic_control_json", side_effect=held_atomic):
            first = threading.Thread(target=worker)
            second = threading.Thread(target=worker)
            first.start()
            self.assertTrue(entered_commit.wait(timeout=5))
            second.start()
            release_commit.set()
            first.join(timeout=10)
            second.join(timeout=10)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(len(outcomes), 2)
        self.assertEqual(outcomes[0], outcomes[1])
        self.assertEqual(commit_calls, 1)
        self.assertEqual(
            seal.validate_receipt(outcomes[0], validator=fixture.validator)["state"],
            "valid_applied_receipt",
        )

    def test_cli_has_no_legacy_database_network_review_or_publication_lane(self) -> None:
        choices = seal.build_parser()._subparsers._group_actions[0].choices  # type: ignore[attr-defined]
        self.assertEqual(
            set(choices), {"plan", "validate-plan", "apply", "validate-receipt"}
        )
        source = Path(seal.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }
        self.assertTrue(
            {"requests", "socket", "sqlite3", "subprocess", "urllib3"}.isdisjoint(imports)
        )
        self.assertNotIn("asr_whispercpp_result_store_seal", source)
        self.assertNotIn("import_asr_whispercpp_result", source)
        self.assertNotIn("sqlite3.connect", source)


if __name__ == "__main__":
    unittest.main()
