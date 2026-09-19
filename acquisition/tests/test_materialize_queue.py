from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from collections import Counter
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACQUISITION_ROOT = REPOSITORY_ROOT / "acquisition"
PROGRAM = ACQUISITION_ROOT / "materialize_queue.py"
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
BUNDLE_SCHEMA = ACQUISITION_ROOT / "schemas" / "queue-bundle-manifest.schema.json"
WORK_ORDER_SCHEMA = ACQUISITION_ROOT / "schemas" / "work-order.schema.json"
TEST_PARENT = ACQUISITION_ROOT / ".test-work"


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:32]}"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def counts(values: list[str]) -> list[dict[str, object]]:
    return [
        {"key": key, "count": count}
        for key, count in sorted(Counter(values).items())
    ]


def seal_plan(plan: dict) -> dict:
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    core_sha256 = hashlib.sha256(canonical_bytes(core)).hexdigest()
    plan["plan_id"] = stable_id("acqplan", core_sha256)
    return plan


def select_and_summarize(plan: dict) -> dict:
    selected_count = 0
    selected_bytes = 0
    limits = plan["limits"]
    for candidate in plan["candidates"]:
        if candidate["queue_state"] != "ready":
            candidate["queue_ordinal"] = None
            candidate["defer_reason"] = candidate["queue_state"]
        elif limits["selection_only"] and candidate["priority_tier"] != "explicit_selection":
            candidate["queue_ordinal"] = None
            candidate["defer_reason"] = "outside_explicit_selection"
        elif selected_count >= limits["max_items"]:
            candidate["queue_ordinal"] = None
            candidate["defer_reason"] = "plan_item_limit"
        elif selected_bytes + candidate["estimated_bytes"] > limits["plan_budget_bytes"]:
            candidate["queue_ordinal"] = None
            candidate["defer_reason"] = "plan_byte_budget"
        else:
            selected_count += 1
            selected_bytes += candidate["estimated_bytes"]
            candidate["queue_ordinal"] = selected_count
            candidate["defer_reason"] = None
    summary = plan["summary"]
    summary["supported_unacquired_sources"] = max(
        summary["supported_unacquired_sources"], len(plan["candidates"])
    )
    summary["recording_candidates"] = len(plan["candidates"])
    summary["selected_count"] = selected_count
    summary["selected_estimated_bytes"] = selected_bytes
    summary["deferred_count"] = len(plan["candidates"]) - selected_count
    summary["by_queue_state"] = counts(
        [candidate["queue_state"] for candidate in plan["candidates"]]
    )
    summary["by_platform"] = counts(
        [candidate["platform"] for candidate in plan["candidates"]]
    )
    summary["by_priority_tier"] = counts(
        [candidate["priority_tier"] for candidate in plan["candidates"]]
    )
    summary["by_defer_reason"] = counts(
        [
            candidate["defer_reason"]
            for candidate in plan["candidates"]
            if candidate["defer_reason"] is not None
        ]
    )
    return seal_plan(plan)


def fixture_plan() -> dict:
    youtube_estimate = 60_000 * 500_000 // 1_000 + 64 * 1024**2
    candidates = [
        {
            "recording_id": "rec_youtube",
            "source_id": "src_youtube",
            "platform": "youtube",
            "source_kind": "youtube_video",
            "native_id": "abcDEF12345",
            "title": "Public YouTube fixture",
            "canonical_url": "https://www.youtube.com/watch?v=abcDEF12345",
            "adapter": "yt_dlp",
            "recording_type": "video",
            "duration_ms": 60_000,
            "estimated_bytes": youtube_estimate,
            "estimate_basis": "duration_conservative_rate",
            "expected_byte_count": None,
            "expected_sha256": None,
            "source_class": None,
            "mapping_roles": ["current_platform_listing"],
            "priority": 30,
            "priority_tier": "current_public_upload",
            "reason_codes": ["bounded_single_job", "current_platform_listing"],
            "wiki_reference_count": 0,
            "wiki_reference_paths": [],
            "queue_state": "ready",
            "queue_ordinal": 1,
            "defer_reason": None,
        },
        {
            "recording_id": "rec_archive",
            "source_id": "src_archive",
            "platform": "internet_archive",
            "source_kind": "archive_media_file",
            "native_id": "item-one/movie.mp4",
            "title": "Public Archive.org fixture",
            "canonical_url": "https://archive.org/download/item-one/movie.mp4",
            "adapter": "direct_http",
            "recording_type": "video",
            "duration_ms": 90_000,
            "estimated_bytes": 1_000_000,
            "estimate_basis": "provider_declared_byte_count",
            "expected_byte_count": 1_000_000,
            "expected_sha256": "b" * 64,
            "source_class": "original",
            "mapping_roles": ["primary_archive_file"],
            "priority": 40,
            "priority_tier": "short_archive",
            "reason_codes": [
                "bounded_single_job",
                "provider_original",
                "short_archive_recording",
            ],
            "wiki_reference_count": 0,
            "wiki_reference_paths": [],
            "queue_state": "ready",
            "queue_ordinal": 2,
            "defer_reason": None,
        },
        {
            "recording_id": "rec_chunk",
            "source_id": "src_chunk",
            "platform": "internet_archive",
            "source_kind": "archive_media_file",
            "native_id": "item-long/long.mp4",
            "title": "Long recording fixture",
            "canonical_url": "https://archive.org/download/item-long/long.mp4",
            "adapter": "direct_http",
            "recording_type": "video",
            "duration_ms": 8_000_000,
            "estimated_bytes": 2_000_000,
            "estimate_basis": "provider_declared_byte_count",
            "expected_byte_count": 2_000_000,
            "expected_sha256": None,
            "source_class": "original",
            "mapping_roles": ["primary_archive_file"],
            "priority": 60,
            "priority_tier": "long_recording",
            "reason_codes": [
                "exceeds_single_job_policy",
                "long_or_unknown_archive_recording",
                "provider_original",
            ],
            "wiki_reference_count": 0,
            "wiki_reference_paths": [],
            "queue_state": "requires_chunking",
            "queue_ordinal": None,
            "defer_reason": "requires_chunking",
        },
    ]
    selected_bytes = youtube_estimate + 1_000_000
    plan = {
        "plan_id": "",
        "schema_version": 1,
        "planned_at": "2026-08-26T20:00:00Z",
        "catalog_basis_sha256": "a" * 64,
        "catalog_migrations": [
            {"version": 1, "name": "0001_fixture.sql", "sha256": "c" * 64}
        ],
        "selection_basis": {
            "purpose": "materializer test fixture",
            "manifest_sha256": None,
            "youtube_video_ids": [],
            "source_ids": [],
            "recording_ids": [],
            "requested_identifiers_already_acquired": [],
            "requested_identifiers_not_eligible": [],
        },
        "wiki_scan": {
            "root_count": 0,
            "youtube_ids_cited": 0,
            "archive_objects_cited": 0,
        },
        "limits": {
            "max_items": 10,
            "plan_budget_bytes": 20 * 1024**3,
            "max_job_bytes": 10 * 1024**3,
            "long_recording_ms": 2 * 60 * 60 * 1000,
            "youtube_bytes_per_second": 500_000,
            "youtube_fixed_overhead_bytes": 64 * 1024**2,
            "selection_only": False,
        },
        "safety": {
            "access_policy": "public_only",
            "publication_authority": "none",
            "credentials_allowed": False,
            "network_access_performed": False,
            "catalog_mutated": False,
        },
        "summary": {
            "supported_unacquired_sources": 3,
            "recording_candidates": 3,
            "selected_count": 2,
            "selected_estimated_bytes": selected_bytes,
            "deferred_count": 1,
            "already_acquired_recordings": 0,
            "withheld_access_sources": 0,
            "by_queue_state": counts(["ready", "ready", "requires_chunking"]),
            "by_platform": counts(["youtube", "internet_archive", "internet_archive"]),
            "by_priority_tier": counts(
                ["current_public_upload", "short_archive", "long_recording"]
            ),
            "by_defer_reason": counts(["requires_chunking"]),
        },
        "candidates": candidates,
    }
    return seal_plan(plan)


class QueueMaterializerTests(unittest.TestCase):
    def setUp(self) -> None:
        TEST_PARENT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="queue-materializer-", dir=TEST_PARENT
        )
        self.root = Path(self.temporary.name)
        self.plan_path = self.root / "plan.json"
        self.bundle_root = self.root / "private-bundles"
        self.media_root = self.root / "private-media"
        self.fake_ytdlp = self.root / "fake-yt-dlp"
        self.invocation_marker = self.root / "network-tool-was-invoked"
        self.fake_ytdlp.write_text(
            "#!/bin/sh\n"
            f"touch {str(self.invocation_marker)!r}\n"
            "exit 99\n",
            encoding="utf-8",
        )
        self.fake_ytdlp.chmod(0o755)

    def tearDown(self) -> None:
        for child in sorted(self.root.rglob("*"), reverse=True):
            try:
                child.chmod(0o700 if child.is_dir() else 0o600)
            except OSError:
                pass
        try:
            self.root.chmod(0o700)
        except OSError:
            pass
        self.temporary.cleanup()

    def write_plan(self, plan: dict) -> None:
        self.plan_path.write_text(
            json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def arguments(self, *, bundle_root: Path | None = None) -> list[str]:
        return [
            "--plan",
            str(self.plan_path.resolve()),
            "--bundle-root",
            str((bundle_root or self.bundle_root).resolve()),
            "--media-output-root",
            str(self.media_root.resolve()),
            "--yt-dlp-executable",
            str(self.fake_ytdlp.resolve()),
            "--yt-dlp-sha256",
            file_sha256(self.fake_ytdlp),
            "--global-cache-cap-bytes",
            str(50 * 1024**3),
            "--free-space-floor-bytes",
            str(80 * 1024**3),
        ]

    def execute(
        self, arguments: list[str] | None = None, *, input_text: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(PROGRAM), *(arguments or self.arguments())],
            input=input_text,
            stdin=None if input_text is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def assert_contract(self, schema: Path, instance: Path) -> None:
        completed = subprocess.run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(schema),
                str(instance),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_materializes_only_selected_ready_candidates_without_network(self) -> None:
        plan = fixture_plan()
        self.write_plan(plan)
        first = self.execute()
        self.assertEqual(first.returncode, 0, first.stderr)
        manifest = json.loads(first.stdout)
        self.assertEqual(manifest["work_order_count"], 2)
        self.assertEqual(
            [row["recording_id"] for row in manifest["work_orders"]],
            ["rec_youtube", "rec_archive"],
        )
        self.assertNotIn("rec_chunk", first.stdout)
        self.assertFalse(self.invocation_marker.exists())
        self.assertEqual(manifest["safety"]["publication_authority"], "none")
        bundle = self.bundle_root / manifest["bundle_relative_path"]
        self.assert_contract(BUNDLE_SCHEMA, bundle / "manifest.json")
        for row in manifest["work_orders"]:
            order_path = bundle / row["path"]
            self.assertEqual(file_sha256(order_path), row["sha256"])
            self.assertEqual(order_path.stat().st_size, row["byte_count"])
            self.assertFalse(order_path.stat().st_mode & 0o222)
            self.assert_contract(WORK_ORDER_SCHEMA, order_path)
        youtube_order = json.loads((bundle / "work-orders/000001.json").read_text())
        self.assertEqual(
            youtube_order["adapter_config"]["expected_executable_sha256"],
            file_sha256(self.fake_ytdlp),
        )
        self.assertEqual(youtube_order["source"]["access_state"], "public")
        self.assertEqual(youtube_order["output"]["root"], str(self.media_root.resolve()))

        # Stdin consumes the same canonical plan and produces identical bytes in a
        # different private bundle root.
        stdin_root = self.root / "stdin-bundles"
        stdin_args = self.arguments(bundle_root=stdin_root)
        stdin_args[stdin_args.index("--plan") + 1] = "-"
        second = self.execute(
            stdin_args,
            input_text=json.dumps(plan, ensure_ascii=False, sort_keys=True),
        )
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(second.stdout, first.stdout)
        self.assertFalse(self.invocation_marker.exists())

    def test_ready_but_unselected_and_requires_chunking_never_materialize(self) -> None:
        plan = fixture_plan()
        plan["limits"]["max_items"] = 1
        select_and_summarize(plan)
        self.write_plan(plan)
        completed = self.execute()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        manifest = json.loads(completed.stdout)
        self.assertEqual(manifest["work_order_count"], 1)
        self.assertEqual(manifest["work_orders"][0]["recording_id"], "rec_youtube")
        self.assertNotIn("rec_archive", completed.stdout)
        self.assertNotIn("rec_chunk", completed.stdout)

    def test_replay_is_exact_and_tampered_immutable_bundle_fails_closed(self) -> None:
        self.write_plan(fixture_plan())
        first = self.execute()
        second = self.execute()
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(first.stdout, second.stdout)
        manifest = json.loads(first.stdout)
        order = self.bundle_root / manifest["bundle_relative_path"] / "work-orders/000001.json"
        order.chmod(0o644)
        order.write_text("{}\n", encoding="utf-8")
        refused = self.execute()
        self.assertEqual(refused.returncode, 2)
        self.assertIn("immutable", json.loads(refused.stderr)["error"]["message"])

    def test_plan_id_and_semantics_are_revalidated(self) -> None:
        cases: list[tuple[str, dict]] = []
        plan = fixture_plan()
        plan["candidates"][0]["title"] = "tampered without changing plan id"
        cases.append(("plan_id", plan))

        plan = fixture_plan()
        plan["candidates"][0]["canonical_url"] += "&token=secret"
        seal_plan(plan)
        cases.append(("canonical", plan))

        plan = fixture_plan()
        plan["safety"]["access_policy"] = "members_only"
        seal_plan(plan)
        cases.append(("public", plan))

        plan = fixture_plan()
        plan["candidates"][2]["queue_ordinal"] = 3
        plan["candidates"][2]["defer_reason"] = None
        seal_plan(plan)
        cases.append(("queue selection", plan))

        plan = fixture_plan()
        duplicate = copy.deepcopy(plan["candidates"][0])
        plan["candidates"].insert(1, duplicate)
        select_and_summarize(plan)
        cases.append(("duplicate", plan))

        plan = fixture_plan()
        plan["candidates"][0]["cookie_file"] = "/tmp/cookies"
        seal_plan(plan)
        cases.append(("unknown", plan))

        for expected, candidate_plan in cases:
            with self.subTest(expected=expected):
                self.write_plan(candidate_plan)
                completed = self.execute()
                self.assertEqual(completed.returncode, 2, completed.stdout)
                message = json.loads(completed.stderr)["error"]["message"].lower()
                self.assertIn(expected.split()[0], message)

    def test_duplicate_json_keys_are_rejected(self) -> None:
        raw = json.dumps(fixture_plan(), sort_keys=True)
        raw = raw.replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1', 1)
        self.plan_path.write_text(raw, encoding="utf-8")
        completed = self.execute()
        self.assertEqual(completed.returncode, 2)
        self.assertIn("duplicate key", json.loads(completed.stderr)["error"]["message"])

    def test_wrong_executable_pin_and_writer_contention_fail_without_output(self) -> None:
        self.write_plan(fixture_plan())
        arguments = self.arguments()
        arguments[arguments.index("--yt-dlp-sha256") + 1] = "0" * 64
        wrong_pin = self.execute(arguments)
        self.assertEqual(wrong_pin.returncode, 2)
        self.assertIn("does not match", json.loads(wrong_pin.stderr)["error"]["message"])
        self.assertFalse((self.bundle_root / "bundles").exists())

        self.bundle_root.mkdir(parents=True)
        lock_path = self.bundle_root / ".queue-materializer.lock"
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            contended = self.execute()
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        self.assertEqual(contended.returncode, 2)
        self.assertIn("writer lock", json.loads(contended.stderr)["error"]["message"])
        self.assertFalse((self.bundle_root / "bundles").exists())

    def test_symlinked_bundle_admission_directory_is_rejected(self) -> None:
        self.write_plan(fixture_plan())
        self.bundle_root.mkdir(parents=True)
        escape = self.root / "must-not-receive-bundle"
        escape.mkdir()
        (self.bundle_root / "bundles").symlink_to(escape, target_is_directory=True)
        completed = self.execute()
        self.assertEqual(completed.returncode, 2)
        self.assertIn(
            "not a real directory",
            json.loads(completed.stderr)["error"]["message"],
        )
        self.assertEqual(list(escape.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
