from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import stat
import sys
import unittest
from pathlib import Path
from unittest import mock

from jsonschema.validators import Draft202012Validator


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / f"asr-result-store-seal-{os.getpid()}"
sys.path.insert(0, str(PIPELINE_ROOT))

import asr_whispercpp_result_store_seal as seal  # noqa: E402


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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


def write_json(path: Path, value: object, mode: int) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    body = canonical_bytes(value) + b"\n"
    path.write_bytes(body)
    path.chmod(mode)
    return body


def result_modes(results: list[Path]) -> dict[Path, int]:
    paths = {
        item
        for result in results
        for item in (
            result.parent,
            result,
            result.parent / "transcript.normalized.json",
            result.parent / "whisper.raw.json",
        )
    }
    return {path: stat.S_IMODE(path.stat().st_mode) for path in paths}


def repin_plan(fixture: "SyntheticStore", plan: dict[str, object]) -> Path:
    semantic = {
        key: value
        for key, value in plan.items()
        if key not in {"identity_sha256", "plan_id", "receipt_path"}
    }
    identity = seal._document_identity(semantic, set())
    plan["identity_sha256"] = identity
    plan["plan_id"] = f"asrsealplan_{identity[:32]}"
    plan["receipt_path"] = str(
        fixture.control / "receipts" / f"asrsealreceipt_{identity[:32]}.json"
    )
    path = fixture.control / "plans" / f"{plan['plan_id']}.json"
    write_json(path, plan, 0o400)
    return path


class SyntheticStore:
    def __init__(
        self,
        *,
        queue_selected_count: int = 1,
        queue_excluded_hints: list[str | None] | None = None,
    ) -> None:
        self.store = TEST_ROOT / "private-asr-results"
        self.store.mkdir(parents=True, mode=0o700)
        self.control = self.store / "sealing-control"
        self.batch_manifest = TEST_ROOT / "batch" / "manifest.json"
        self.queue_manifest = TEST_ROOT / "queue" / "manifest.json"
        self.results: list[Path] = []
        self._make_source(
            kind="batch", manifest_path=self.batch_manifest, identity="1" * 64,
            selected=[("batch-job", "a" * 64)], excluded=[],
        )
        self._batch_manifest_body = self.batch_manifest.read_bytes()
        self.batch_results = list(self.results)
        queue_selected = [
            (f"queue-job-{index}", f"{index + 3:x}" * 64)
            for index in range(1, queue_selected_count + 1)
        ]
        queue_start = len(self.results)
        self._make_source(
            kind="queue", manifest_path=self.queue_manifest, identity="2" * 64,
            selected=queue_selected,
            excluded=[
                (
                    f"review-job-{index}",
                    digest(f"review-input-{index}".encode()),
                    hint,
                )
                for index, hint in enumerate(
                    queue_excluded_hints
                    if queue_excluded_hints is not None
                    else ["review_near_silent_candidate"],
                    1,
                )
            ],
        )
        self._queue_manifest_body = self.queue_manifest.read_bytes()
        self.queue_results = self.results[queue_start:]

    def _make_source(
        self,
        *,
        kind: str,
        manifest_path: Path,
        identity: str,
        selected: list[tuple[str, str]],
        excluded: list[tuple[str, str, str | None]],
    ) -> None:
        records = []
        all_items = [(job, input_sha, "process") for job, input_sha in selected]
        all_items.extend(excluded)
        source_id = (
            f"asrbatch_{identity[:32]}" if kind == "batch"
            else f"asrppqueue_{identity[:32]}"
        )
        for ordinal, (job_id, input_sha, routing_hint) in enumerate(all_items, 1):
            work_order = {
                "engine": {"expected_sha256": "d" * 64, "version_label": "whisper.cpp v1.8.7"},
                "input": {"expected_sha256": input_sha},
                "job_id": job_id,
                "output": {"root": str(self.store)},
            }
            body = write_json(
                manifest_path.parent / "work-orders" / f"{ordinal:06d}.json",
                work_order,
                0o400,
            )
            canonical_sha = digest(canonical_bytes(work_order))
            record = {
                "byte_count": len(body),
                "canonical_sha256": canonical_sha,
                "job_id": job_id,
                "ordinal": ordinal,
                "path": f"work-orders/{ordinal:06d}.json",
                "sha256": digest(body),
            }
            if kind == "queue":
                record["routing_hint"] = routing_hint
            records.append(record)
            if routing_hint == "process":
                self.results.append(
                    self._make_result(job_id, canonical_sha, input_sha, ordinal)
                )
        manifest = {
            ("batch_id" if kind == "batch" else "queue_id"): source_id,
            "engine": {
                "expected_sha256": "d" * 64,
                "version_label": "whisper.cpp v1.8.7",
            },
            "identity_sha256": identity,
            "software": {
                "asr_adapter": {
                    "byte_count": 123,
                    "implementation_version": "0.3.0",
                    "name": "himr-asr-whispercpp",
                    "sha256": "e" * 64,
                }
            },
            "work_order_count": len(records),
            "work_orders": records,
        }
        if kind == "queue":
            manifest.update(
                {
                    "implementation_version": "test-queue-v1",
                    "materializer": "himr-test-preprocess-asr-queue",
                    "safety": {
                        "catalog_writes": False,
                        "network_access": "none",
                    },
                    "schema_version": 1,
                }
            )
        else:
            manifest.update(
                {
                    "catalog": {"binding_sha256": "9" * 64},
                    "implementation_version": "test-batch-v1",
                    "materializer": "himr-test-asr-batch",
                    "safety": {
                        "catalog_writes": False,
                        "network_access": "none",
                    },
                    "schema_version": 1,
                }
            )
        write_json(manifest_path, manifest, 0o400)

    def _make_result(
        self, job_id: str, work_order_sha: str, input_sha: str, ordinal: int
    ) -> Path:
        result_key = digest(f"{job_id}:{ordinal}".encode())
        directory = self.store / "objects" / job_id / "results" / result_key
        result_path = directory / "result.json"
        result = {
            "engine": {"sha256": "d" * 64, "version": "whisper.cpp v1.8.7"},
            "input": {"sha256": input_sha},
            "job_id": job_id,
            "processing_run": {
                "implementation_version": "0.3.0",
                "processing_run_id": f"run_asr_whispercpp_{digest(job_id.encode())[:32]}",
            },
            "recipe_id": f"recipe_asr_whispercpp_{digest((job_id + '-recipe').encode())[:32]}",
            "result_key": result_key,
            "result_path": str(result_path),
            "work_order_sha256": work_order_sha,
        }
        write_json(result_path, result, 0o644)
        write_json(directory / "transcript.normalized.json", {"segments": []}, 0o644)
        write_json(directory / "whisper.raw.json", {"transcription": []}, 0o644)
        directory.chmod(0o700)
        return result_path

    @staticmethod
    def validator(path: Path) -> dict[str, object]:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {
            "artifact_count": 2,
            "job_id": raw["job_id"],
            "model_id": "model_test",
            "processing_run_id": raw["processing_run"]["processing_run_id"],
            "recipe_id": raw["recipe_id"],
            "recording_id": None,
            "rendition_id": None,
            "result_envelope_sha256": digest(canonical_bytes(raw)),
            "result_key": raw["result_key"],
            "segment_count": 0,
            "token_count": 0,
        }

    def queue_validator(
        self, path: Path
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        body = path.read_bytes()
        if path != self.queue_manifest or body != self._queue_manifest_body:
            raise ValueError("synthetic sealed queue identity changed")
        manifest = json.loads(body)
        orders = [
            json.loads((path.parent / item["path"]).read_text(encoding="utf-8"))
            for item in manifest["work_orders"]
        ]
        return manifest, orders

    def batch_validator(
        self, path: Path
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        body = path.read_bytes()
        if path != self.batch_manifest or body != self._batch_manifest_body:
            raise ValueError("synthetic sealed batch identity changed")
        manifest = json.loads(body)
        orders = [
            json.loads((path.parent / item["path"]).read_text(encoding="utf-8"))
            for item in manifest["work_orders"]
        ]
        return manifest, orders

    def plan(self, validator=None) -> Path:
        return seal.build_plan(
            batch_manifest=self.batch_manifest,
            queue_manifest=self.queue_manifest,
            result_paths=self.results,
            output_directory=self.control / "plans",
            store_root=self.store,
            validator=validator or self.validator,
        )

    def queue_only_plan(
        self, *, result_paths: list[Path] | None = None, validator=None
    ) -> Path:
        return seal.build_plan(
            queue_manifest=self.queue_manifest,
            result_paths=self.queue_results if result_paths is None else result_paths,
            output_directory=self.control / "plans",
            store_root=self.store,
            validator=validator or self.validator,
            queue_validator=self.queue_validator,
            source_mode=seal.QUEUE_ONLY_CLI_MODE,
        )

    def batch_only_plan(
        self, *, result_paths: list[Path] | None = None, validator=None
    ) -> Path:
        return seal.build_plan(
            batch_manifest=self.batch_manifest,
            result_paths=self.batch_results if result_paths is None else result_paths,
            output_directory=self.control / "plans",
            store_root=self.store,
            validator=validator or self.validator,
            batch_validator=self.batch_validator,
            source_mode=seal.BATCH_ONLY_CLI_MODE,
        )


class ResultStoreSealTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan_schema = json.loads(
            (PIPELINE_ROOT / "schemas/asr-whispercpp-result-seal-plan.schema.json").read_text(
                encoding="utf-8"
            )
        )
        cls.receipt_schema = json.loads(
            (PIPELINE_ROOT / "schemas/asr-whispercpp-result-seal-receipt.schema.json").read_text(
                encoding="utf-8"
            )
        )

    def setUp(self) -> None:
        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)

    def tearDown(self) -> None:
        remove_tree()

    def test_plan_is_read_only_exact_and_schema_valid(self) -> None:
        fixture = SyntheticStore()
        watched = {
            path: (path.read_bytes(), path.stat().st_mtime_ns, stat.S_IMODE(path.stat().st_mode))
            for result in fixture.results
            for path in [result, result.parent / "transcript.normalized.json", result.parent / "whisper.raw.json"]
        }
        plan_path = fixture.plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(self.plan_schema)
        Draft202012Validator(self.plan_schema).validate(plan)
        self.assertEqual(stat.S_IMODE(plan_path.stat().st_mode), 0o400)
        self.assertEqual(plan["result_count"], 2)
        self.assertEqual(len(plan["excluded_source_entries"]), 1)
        self.assertEqual(
            stat.S_IMODE((fixture.control / "receipts").stat().st_mode), 0o700
        )
        self.assertEqual(
            stat.S_IMODE(
                (fixture.control / seal.APPLY_LOCK_FILENAME).stat().st_mode
            ),
            0o600,
        )
        self.assertEqual(plan["excluded_source_entries"][0]["routing_hint"], "review_near_silent_candidate")
        self.assertEqual(
            seal.validate_plan(plan_path, validator=fixture.validator)["state"],
            "valid_prepared_plan",
        )
        self.assertEqual(
            watched,
            {
                path: (path.read_bytes(), path.stat().st_mtime_ns, stat.S_IMODE(path.stat().st_mode))
                for path in watched
            },
        )

    def test_queue_only_v2_plan_has_exact_process_coverage_and_provenance(self) -> None:
        fixture = SyntheticStore(queue_selected_count=3)
        watched = {
            path: (
                path.read_bytes(),
                path.stat().st_mtime_ns,
                stat.S_IMODE(path.stat().st_mode),
            )
            for result in fixture.queue_results
            for path in (
                result,
                result.parent / "transcript.normalized.json",
                result.parent / "whisper.raw.json",
            )
        }
        plan_path = fixture.queue_only_plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(self.plan_schema)
        Draft202012Validator(self.plan_schema).validate(plan)
        self.assertEqual(plan["schema_version"], 2)
        self.assertEqual(plan["result_count"], 3)
        self.assertEqual(len(plan["sources"]), 1)
        self.assertEqual(plan["sources"][0]["kind"], "short_preprocess_queue")
        self.assertEqual(
            plan["source_authority"],
            seal._queue_only_source_authority(
                plan["sources"][0],
                plan["source_authority"]["queue_contract"],
            ),
        )
        self.assertEqual(
            plan["source_authority"]["selection_policy"],
            "all_and_only_routing_hint_process_entries",
        )
        self.assertEqual(
            plan["source_authority"]["queue_contract"]["queue_validator"],
            seal._file_identity(seal.PREPROCESS_QUEUE_SOURCE.resolve()),
        )
        self.assertEqual(
            plan["source_authority"]["queue_contract"]["queue_manifest_schema"],
            seal._file_identity(seal.PREPROCESS_QUEUE_SCHEMA_SOURCE.resolve()),
        )
        self.assertEqual(
            {item["source_ordinal"] for item in plan["results"]}, {1, 2, 3}
        )
        self.assertEqual(len(plan["excluded_source_entries"]), 1)
        self.assertEqual(
            seal.validate_plan(
                plan_path,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            )["state"],
            "valid_prepared_plan",
        )
        self.assertEqual(
            watched,
            {
                path: (
                    path.read_bytes(),
                    path.stat().st_mtime_ns,
                    stat.S_IMODE(path.stat().st_mode),
                )
                for path in watched
            },
        )

    def test_queue_only_v2_apply_carries_queue_provenance_into_receipt(self) -> None:
        fixture = SyntheticStore(queue_selected_count=3)
        plan_path = fixture.queue_only_plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        receipt_path = seal.apply_plan(
            plan_path,
            validator=fixture.validator,
            queue_validator=fixture.queue_validator,
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(self.receipt_schema)
        Draft202012Validator(self.receipt_schema).validate(receipt)
        self.assertEqual(receipt["schema_version"], 2)
        self.assertEqual(receipt["source_authority"], plan["source_authority"])
        self.assertEqual(receipt["result_count"], 3)
        self.assertEqual(
            seal.validate_receipt(
                receipt_path,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            )["state"],
            "valid_applied_receipt",
        )
        self.assertEqual(
            seal.apply_plan(
                plan_path,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            ),
            receipt_path,
        )

    def test_authoritative_queue_wrapper_calls_materializer_validator(self) -> None:
        import preprocess_asr_queue

        manifest_path = TEST_ROOT / "sealed-queue" / "manifest.json"
        expected = ({"queue_id": "test"}, [{"job_id": "job"}])
        with mock.patch.object(
            preprocess_asr_queue, "validate_queue", return_value=expected
        ) as validate:
            self.assertEqual(seal.authoritative_queue_validate(manifest_path), expected)
        validate.assert_called_once_with(manifest_path)

    def test_authoritative_batch_wrapper_calls_materializer_validator(self) -> None:
        import asr_whispercpp_batch

        manifest_path = TEST_ROOT / "sealed-batch" / "manifest.json"
        expected = ({"batch_id": "test"}, [{"job_id": "job"}])
        with mock.patch.object(
            asr_whispercpp_batch, "validate_batch", return_value=expected
        ) as validate:
            self.assertEqual(seal.authoritative_batch_validate(manifest_path), expected)
        validate.assert_called_once_with(manifest_path)

    def test_batch_only_v3_plan_and_receipt_are_exact_and_schema_valid(self) -> None:
        fixture = SyntheticStore()
        before = result_modes(fixture.batch_results)
        plan_path = fixture.batch_only_plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        Draft202012Validator(self.plan_schema).validate(plan)
        self.assertEqual(plan["schema_version"], seal.BATCH_ONLY_PLAN_SCHEMA_VERSION)
        self.assertEqual(plan["result_count"], 1)
        self.assertEqual([item["kind"] for item in plan["sources"]], ["long_window_batch"])
        self.assertEqual(plan["source_authority"]["mode"], "batch_only")
        self.assertEqual(
            plan["source_authority"]["selection_policy"], "all_batch_work_orders"
        )
        self.assertEqual(before, result_modes(fixture.batch_results))
        self.assertEqual(
            seal.validate_plan(
                plan_path,
                validator=fixture.validator,
                batch_validator=fixture.batch_validator,
            )["state"],
            "valid_prepared_plan",
        )

        receipt_path = seal.apply_plan(
            plan_path,
            validator=fixture.validator,
            batch_validator=fixture.batch_validator,
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        Draft202012Validator(self.receipt_schema).validate(receipt)
        self.assertEqual(receipt["schema_version"], seal.BATCH_ONLY_RECEIPT_SCHEMA_VERSION)
        self.assertEqual(receipt["source_authority"], plan["source_authority"])
        self.assertEqual(
            seal.validate_receipt(
                receipt_path,
                validator=fixture.validator,
                batch_validator=fixture.batch_validator,
            )["state"],
            "valid_applied_receipt",
        )

    def test_batch_only_v3_rejects_queue_missing_batch_and_wrong_source(self) -> None:
        fixture = SyntheticStore()
        common = {
            "result_paths": fixture.batch_results,
            "output_directory": fixture.control / "plans",
            "store_root": fixture.store,
            "validator": fixture.validator,
            "batch_validator": fixture.batch_validator,
            "source_mode": seal.BATCH_ONLY_CLI_MODE,
        }
        with self.assertRaisesRegex(seal.SealError, "requires --batch-manifest"):
            seal.build_plan(**common)
        with self.assertRaisesRegex(seal.SealError, "forbids --queue-manifest"):
            seal.build_plan(
                batch_manifest=fixture.batch_manifest,
                queue_manifest=fixture.queue_manifest,
                **common,
            )
        with self.assertRaisesRegex(seal.SealError, "requires exactly these source kinds"):
            seal.build_plan(batch_manifest=fixture.queue_manifest, **common)

    def test_batch_only_v3_rejects_forged_authority_and_batch(self) -> None:
        fixture = SyntheticStore()
        original_plan = fixture.batch_only_plan()
        plan = json.loads(original_plan.read_text(encoding="utf-8"))
        plan["source_authority"]["batch_manifest_sha256"] = "f" * 64
        crafted_path = repin_plan(fixture, plan)
        with self.assertRaisesRegex(seal.SealError, "differs from its batch"):
            seal.validate_plan(
                crafted_path,
                validator=fixture.validator,
                batch_validator=fixture.batch_validator,
            )

        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        fixture = SyntheticStore()
        manifest = json.loads(fixture.batch_manifest.read_text(encoding="utf-8"))
        manifest["safety"]["network_access"] = "self_consistent_forgery"
        fixture.batch_manifest.chmod(0o600)
        write_json(fixture.batch_manifest, manifest, 0o400)
        with self.assertRaisesRegex(
            seal.SealError, "authoritative raw ASR batch validation failed"
        ):
            fixture.batch_only_plan()

    def test_queue_only_v2_rejects_missing_extra_and_batch_sources(self) -> None:
        fixture = SyntheticStore(queue_selected_count=3)
        with self.assertRaisesRegex(seal.SealError, "allowlist has 2 results.*select 3"):
            fixture.queue_only_plan(result_paths=fixture.queue_results[:-1])
        with self.assertRaisesRegex(seal.SealError, "not selected by the sealed sources"):
            fixture.queue_only_plan(
                result_paths=[fixture.batch_results[0], *fixture.queue_results[:-1]]
            )
        with self.assertRaisesRegex(seal.SealError, "forbids --batch-manifest"):
            seal.build_plan(
                batch_manifest=fixture.batch_manifest,
                queue_manifest=fixture.queue_manifest,
                result_paths=fixture.queue_results,
                output_directory=fixture.control / "plans",
                store_root=fixture.store,
                validator=fixture.validator,
                source_mode=seal.QUEUE_ONLY_CLI_MODE,
            )
        with self.assertRaisesRegex(seal.SealError, "requires exactly these source kinds"):
            seal.build_plan(
                queue_manifest=fixture.batch_manifest,
                result_paths=fixture.batch_results,
                output_directory=fixture.control / "plans",
                store_root=fixture.store,
                validator=fixture.validator,
                source_mode=seal.QUEUE_ONLY_CLI_MODE,
            )

    def test_queue_only_v2_rejects_duplicate_allowlist_and_source_mode_drift(self) -> None:
        fixture = SyntheticStore()
        with self.assertRaisesRegex(seal.SealError, "contain no duplicates"):
            fixture.queue_only_plan(
                result_paths=[fixture.queue_results[0], fixture.queue_results[0]]
            )
        plan_path = fixture.queue_only_plan()
        fixture.queue_manifest.chmod(0o600)
        with self.assertRaisesRegex(seal.SealError, "mode must be 0400"):
            seal.validate_plan(
                plan_path,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            )

    def test_queue_only_v2_rejects_self_consistent_forged_source_authority(self) -> None:
        fixture = SyntheticStore()
        original_plan = fixture.queue_only_plan()
        plan = json.loads(original_plan.read_text(encoding="utf-8"))
        plan["source_authority"]["queue_manifest_sha256"] = "f" * 64
        semantic = {
            key: value
            for key, value in plan.items()
            if key not in {"identity_sha256", "plan_id", "receipt_path"}
        }
        identity = seal._document_identity(semantic, set())
        plan["identity_sha256"] = identity
        plan["plan_id"] = f"asrsealplan_{identity[:32]}"
        plan["receipt_path"] = str(
            fixture.control / "receipts" / f"asrsealreceipt_{identity[:32]}.json"
        )
        crafted_path = fixture.control / "plans" / f"{plan['plan_id']}.json"
        write_json(crafted_path, plan, 0o400)
        with self.assertRaisesRegex(seal.SealError, "differs from its queue"):
            seal.validate_plan(
                crafted_path,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            )

    def test_queue_only_v2_accepts_null_and_empty_excluded_routing_hints(self) -> None:
        fixture = SyntheticStore(queue_excluded_hints=[None, ""])
        plan_path = fixture.queue_only_plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        self.assertEqual(
            [item["routing_hint"] for item in plan["excluded_source_entries"]],
            [None, ""],
        )
        Draft202012Validator(self.plan_schema).validate(plan)
        self.assertEqual(
            seal.validate_plan(
                plan_path,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            )["state"],
            "valid_prepared_plan",
        )

    def test_queue_only_v2_rejects_forged_queue_before_plan_publication(self) -> None:
        fixture = SyntheticStore()
        forged = json.loads(fixture.queue_manifest.read_text(encoding="utf-8"))
        forged["identity_sha256"] = "3" * 64
        forged["queue_id"] = f"asrppqueue_{'3' * 32}"
        fixture.queue_manifest.chmod(0o600)
        write_json(fixture.queue_manifest, forged, 0o400)
        with self.assertRaisesRegex(
            seal.SealError,
            "authoritative preprocess ASR queue validation failed.*identity changed",
        ):
            fixture.queue_only_plan()
        self.assertFalse(
            list((fixture.control / "plans").glob("asrsealplan_*.json"))
        )

    def test_legacy_v1_implementation_identity_is_portable_but_v2_rejects_it(self) -> None:
        fixture = SyntheticStore()
        plan = json.loads(fixture.plan().read_text(encoding="utf-8"))
        byte_count, sha256 = next(iter(seal.LEGACY_V1_IMPLEMENTATION_IDENTITIES))
        plan["implementation"]["byte_count"] = byte_count
        plan["implementation"]["sha256"] = sha256
        legacy_v1 = repin_plan(fixture, plan)
        self.assertEqual(
            seal.validate_plan(legacy_v1, validator=fixture.validator)["state"],
            "valid_prepared_plan",
        )

        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        fixture = SyntheticStore()
        plan = json.loads(fixture.queue_only_plan().read_text(encoding="utf-8"))
        plan["implementation"]["byte_count"] = byte_count
        plan["implementation"]["sha256"] = sha256
        legacy_v2 = repin_plan(fixture, plan)
        with self.assertRaisesRegex(seal.SealError, "implementation identity differs"):
            seal.validate_plan(
                legacy_v2,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            )

    def test_schema_versions_fail_closed_on_source_authority_shape(self) -> None:
        fixture = SyntheticStore()
        v1 = json.loads(fixture.plan().read_text(encoding="utf-8"))
        v1["source_authority"] = {
            "mode": "queue_only",
        }
        self.assertTrue(list(Draft202012Validator(self.plan_schema).iter_errors(v1)))

        # Use another fixture root because the v1 plan has already claimed its
        # semantic output path in the first store.
        remove_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        fixture = SyntheticStore()
        v2 = json.loads(fixture.queue_only_plan().read_text(encoding="utf-8"))
        del v2["source_authority"]
        self.assertTrue(list(Draft202012Validator(self.plan_schema).iter_errors(v2)))

    def test_apply_changes_only_modes_and_receipt_validates(self) -> None:
        fixture = SyntheticStore()
        plan_path = fixture.plan()
        watched_files = [
            path
            for result in fixture.results
            for path in [result, result.parent / "transcript.normalized.json", result.parent / "whisper.raw.json"]
        ]
        before_files = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in watched_files}
        before_directories = {result.parent: result.parent.stat().st_mtime_ns for result in fixture.results}
        receipt_path = seal.apply_plan(plan_path, validator=fixture.validator)
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(self.receipt_schema)
        Draft202012Validator(self.receipt_schema).validate(receipt)
        self.assertEqual(stat.S_IMODE(receipt_path.stat().st_mode), 0o400)
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
        self.assertEqual(
            seal.apply_plan(plan_path, validator=fixture.validator), receipt_path
        )

    def test_preexisting_invalid_receipt_never_changes_result_modes(self) -> None:
        fixture = SyntheticStore()
        plan_path = fixture.queue_only_plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        receipt_path = Path(plan["receipt_path"])
        receipt_path.write_bytes(b'{"not":"a receipt"}\n')
        receipt_path.chmod(0o400)
        before = result_modes(fixture.queue_results)
        with self.assertRaisesRegex(seal.SealError, "receipt target already exists"):
            seal.apply_plan(
                plan_path,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            )
        self.assertEqual(result_modes(fixture.queue_results), before)
        self.assertEqual(receipt_path.read_bytes(), b'{"not":"a receipt"}\n')

    def test_failed_chmod_rolls_back_every_exact_pre_mode(self) -> None:
        fixture = SyntheticStore()
        plan_path = fixture.queue_only_plan()
        before = result_modes(fixture.queue_results)
        real_fchmod = os.fchmod
        calls = 0

        def fail_second_transition(descriptor: int, mode: int) -> None:
            nonlocal calls
            if mode == seal.RESULT_FILE_MODE_AFTER:
                calls += 1
                if calls == 2:
                    raise OSError("injected chmod failure")
            real_fchmod(descriptor, mode)

        with mock.patch.object(seal.os, "fchmod", side_effect=fail_second_transition):
            with self.assertRaisesRegex(seal.SealError, "recorded modes were restored"):
                seal.apply_plan(
                    plan_path,
                    validator=fixture.validator,
                    queue_validator=fixture.queue_validator,
                )
        self.assertEqual(result_modes(fixture.queue_results), before)

    def test_receipt_uses_retained_plan_body_without_pathname_reopen(self) -> None:
        fixture = SyntheticStore()
        plan_path = fixture.queue_only_plan()
        original_read_bytes = Path.read_bytes

        def reject_plan_reopen(path: Path) -> bytes:
            if path == plan_path:
                raise AssertionError("plan pathname was reopened")
            return original_read_bytes(path)

        with mock.patch.object(Path, "read_bytes", reject_plan_reopen):
            receipt_path = seal.apply_plan(
                plan_path,
                validator=fixture.validator,
                queue_validator=fixture.queue_validator,
            )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["plan"]["sha256"], digest(original_read_bytes(plan_path)))

    def test_post_link_receipt_substitution_is_preserved_and_modes_roll_back(self) -> None:
        fixture = SyntheticStore()
        plan_path = fixture.queue_only_plan()
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        receipt_path = Path(plan["receipt_path"])
        before = result_modes(fixture.queue_results)
        competitor = b'{"competitor":true}\n'
        real_link = os.link

        def substitute_after_link(source, target, **kwargs):
            value = real_link(source, target, **kwargs)
            os.unlink(target, dir_fd=kwargs["dst_dir_fd"])
            descriptor = os.open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o400,
                dir_fd=kwargs["dst_dir_fd"],
            )
            try:
                os.write(descriptor, competitor)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return value

        with mock.patch.object(seal.os, "link", side_effect=substitute_after_link):
            with self.assertRaisesRegex(seal.SealError, "recorded modes were restored"):
                seal.apply_plan(
                    plan_path,
                    validator=fixture.validator,
                    queue_validator=fixture.queue_validator,
                )
        self.assertEqual(result_modes(fixture.queue_results), before)
        self.assertEqual(receipt_path.read_bytes(), competitor)

    def test_receipt_runtime_rejects_noncanonical_state_time_and_scalar_types(self) -> None:
        fixture = SyntheticStore()
        plan_path = fixture.queue_only_plan()
        receipt_path = seal.apply_plan(
            plan_path,
            validator=fixture.validator,
            queue_validator=fixture.queue_validator,
            applied_at="2026-08-27T12:34:56Z",
        )
        original = json.loads(receipt_path.read_text(encoding="utf-8"))
        cases = (
            ("state", "wrong", "state"),
            ("applied_at", "2026-08-27T12:34:56.000Z", "canonical RFC3339"),
            ("applied_at", 123, "canonical RFC3339"),
            ("result_count", True, "integer"),
        )
        for field, value, message in cases:
            with self.subTest(field=field, value=value):
                receipt = json.loads(json.dumps(original))
                receipt[field] = value
                with self.assertRaisesRegex(seal.SealError, message):
                    seal._validate_receipt_shape(receipt)
                self.assertTrue(
                    list(Draft202012Validator(self.receipt_schema).iter_errors(receipt))
                )

        receipt = json.loads(json.dumps(original))
        receipt["results"][0]["directory"]["nlink"] = True
        with self.assertRaisesRegex(seal.SealError, "integer"):
            seal._validate_receipt_shape(receipt)
        self.assertTrue(
            list(Draft202012Validator(self.receipt_schema).iter_errors(receipt))
        )

    def test_extra_entry_is_rejected(self) -> None:
        fixture = SyntheticStore()
        extra = fixture.results[0].parent / "unexpected.txt"
        extra.write_text("unexpected", encoding="utf-8")
        with self.assertRaisesRegex(seal.SealError, "entries differ"):
            fixture.plan()

    def test_control_document_commit_never_overwrites_a_racing_path(self) -> None:
        fixture = SyntheticStore()
        real_link = os.link
        racing_body = b"racing-writer\n"

        def racing_link(source, target, **kwargs):
            descriptor = os.open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o400,
                dir_fd=kwargs["dst_dir_fd"],
            )
            try:
                os.write(descriptor, racing_body)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return real_link(source, target, **kwargs)

        with mock.patch.object(seal.os, "link", side_effect=racing_link):
            with self.assertRaisesRegex(seal.SealError, "cannot commit"):
                fixture.plan()
        committed = list((fixture.control / "plans").glob("asrsealplan_*.json"))
        self.assertEqual(len(committed), 1)
        self.assertEqual(committed[0].read_bytes(), racing_body)

    def test_hardlink_is_rejected(self) -> None:
        fixture = SyntheticStore()
        raw = fixture.results[0].parent / "whisper.raw.json"
        normalized = fixture.results[0].parent / "transcript.normalized.json"
        raw.unlink()
        os.link(normalized, raw)
        with self.assertRaisesRegex(seal.SealError, "nlink 1"):
            fixture.plan()

    def test_symlink_is_rejected(self) -> None:
        fixture = SyntheticStore()
        raw = fixture.results[0].parent / "whisper.raw.json"
        raw.unlink()
        raw.symlink_to(fixture.results[1].parent / "whisper.raw.json")
        with self.assertRaisesRegex(seal.SealError, "symlink|resolved"):
            fixture.plan()

    def test_self_consistent_plan_cannot_escape_the_planned_store_root(self) -> None:
        fixture = SyntheticStore()
        original_plan = fixture.plan()
        plan = json.loads(original_plan.read_text(encoding="utf-8"))
        result = plan["results"][0]
        outside_directory = TEST_ROOT / "outside-store" / result["result_key"]
        result["result_path"] = str(outside_directory / "result.json")
        result["directory"]["path"] = str(outside_directory)
        for item in result["files"]:
            item["path"] = str(outside_directory / item["name"])
        semantic = {
            key: value
            for key, value in plan.items()
            if key not in {"identity_sha256", "plan_id", "receipt_path"}
        }
        identity = seal._document_identity(semantic, set())
        plan["identity_sha256"] = identity
        plan["plan_id"] = f"asrsealplan_{identity[:32]}"
        plan["receipt_path"] = str(
            fixture.control / "receipts" / f"asrsealreceipt_{identity[:32]}.json"
        )
        crafted_path = fixture.control / "plans" / f"{plan['plan_id']}.json"
        write_json(crafted_path, plan, 0o400)
        with self.assertRaisesRegex(seal.SealError, "outside the allowlisted store root"):
            seal.validate_plan(crafted_path, validator=fixture.validator)

    def test_result_path_swap_during_catalog_validation_is_rejected(self) -> None:
        fixture = SyntheticStore()
        swapped = False

        def validator(path: Path) -> dict[str, object]:
            nonlocal swapped
            value = fixture.validator(path)
            if not swapped:
                swapped = True
                hold = path.with_name("result.held.json")
                path.rename(hold)
                shutil.copy2(hold, path)
            return value

        with self.assertRaisesRegex(seal.SealError, "replaced|entries differ"):
            fixture.plan(validator=validator)

    def test_in_place_tamper_and_mtime_restoration_is_rejected(self) -> None:
        fixture = SyntheticStore()
        tampered = False

        def validator(path: Path) -> dict[str, object]:
            nonlocal tampered
            value = fixture.validator(path)
            if not tampered:
                tampered = True
                artifact = path.parent / "transcript.normalized.json"
                original = artifact.read_bytes()
                mtime = artifact.stat().st_mtime_ns
                artifact.write_bytes(b'{"tampered":true}\n')
                artifact.write_bytes(original)
                os.utime(artifact, ns=(artifact.stat().st_atime_ns, mtime))
            return value

        with self.assertRaisesRegex(seal.SealError, "metadata changed"):
            fixture.plan(validator=validator)

    def test_post_chmod_tamper_is_rejected_before_receipt(self) -> None:
        fixture = SyntheticStore()
        plan_path = fixture.plan()
        calls = 0

        def validator(path: Path) -> dict[str, object]:
            nonlocal calls
            calls += 1
            value = fixture.validator(path)
            if calls == 3:
                artifact = path.parent / "transcript.normalized.json"
                original = artifact.read_bytes()
                mtime = artifact.stat().st_mtime_ns
                artifact.chmod(0o600)
                artifact.write_bytes(b'{"tampered":true}\n')
                artifact.write_bytes(original)
                os.utime(artifact, ns=(artifact.stat().st_atime_ns, mtime))
                artifact.chmod(0o400)
            return value

        with self.assertRaisesRegex(seal.SealError, "ctime changed"):
            seal.apply_plan(plan_path, validator=validator)
        self.assertFalse(Path(json.loads(plan_path.read_text())["receipt_path"]).exists())

    def test_cli_has_no_execution_import_publication_or_network_lane(self) -> None:
        parser = seal.build_parser()
        choices = parser._subparsers._group_actions[0].choices  # type: ignore[attr-defined]
        self.assertEqual(
            set(choices), {"plan", "validate-plan", "apply", "validate-receipt"}
        )
        tree = ast.parse(Path(seal.__file__).read_text(encoding="utf-8"))
        imports = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }
        self.assertTrue({"subprocess", "socket", "sqlite3", "requests"}.isdisjoint(imports))
        source = Path(seal.__file__).read_text(encoding="utf-8")
        self.assertNotIn("import_asr_whispercpp_result(", source)
        self.assertNotIn("sqlite3.connect", source)
        common = [
            "--queue-manifest", str(TEST_ROOT / "queue.json"),
            "--result", str(TEST_ROOT / "result.json"),
            "--output-directory", str(TEST_ROOT / "plans"),
        ]
        legacy = parser.parse_args(
            ["plan", "--batch-manifest", str(TEST_ROOT / "batch.json"), *common]
        )
        self.assertEqual(legacy.source_mode, seal.TWO_SOURCE_CLI_MODE)
        queue_only = parser.parse_args(
            ["plan", "--source-mode", seal.QUEUE_ONLY_CLI_MODE, *common]
        )
        self.assertEqual(queue_only.source_mode, seal.QUEUE_ONLY_CLI_MODE)
        self.assertIsNone(queue_only.batch_manifest)
        batch_only = parser.parse_args(
            [
                "plan",
                "--source-mode",
                seal.BATCH_ONLY_CLI_MODE,
                "--batch-manifest",
                str(TEST_ROOT / "batch.json"),
                "--result",
                str(TEST_ROOT / "result.json"),
                "--output-directory",
                str(TEST_ROOT / "plans"),
            ]
        )
        self.assertEqual(batch_only.source_mode, seal.BATCH_ONLY_CLI_MODE)
        self.assertIsNone(batch_only.queue_manifest)

    def test_private_current_plan_validates_when_available(self) -> None:
        plan_paths = sorted((seal.DEFAULT_CONTROL_ROOT / "plans").glob("asrsealplan_*.json"))
        if not plan_paths:
            self.skipTest("private current-result seal plan is unavailable")
        self.assertEqual(len(plan_paths), 1)
        plan = json.loads(plan_paths[0].read_text(encoding="utf-8"))
        receipt_path = Path(plan["receipt_path"])
        result = (
            seal.validate_receipt(receipt_path)
            if receipt_path.exists()
            else seal.validate_plan(plan_paths[0])
        )
        self.assertEqual(result["result_count"], 25)


if __name__ == "__main__":
    unittest.main()
