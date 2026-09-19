from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from jsonschema.validators import Draft202012Validator


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.importers import canonical_json  # noqa: E402
from himr_corpus.torrent_availability_probe import (  # noqa: E402
    CHECKPOINT_KIND,
    NETWORK_POLICY,
    TorrentAvailabilityProbeError,
    _classify_failure,
    load_availability_probe_request,
    produce_availability_probe,
    validate_availability_probe_request,
)
from himr_corpus.torrent_selective_planner import (  # noqa: E402
    PLAN_KIND,
    PLANNER_VERSION,
    PROBE_FLAGS,
    PROBE_REQUEST_KIND,
)


AVAILABLE_ID = "Available01"
PRIVATE_ID = "PrivateVid1"
REMOVED_ID = "RemovedVid1"
SIGNIN_ID = "SigninVid01"
RATE_ID = "RateLimit01"
NETWORK_ID = "NetworkVid1"
ERROR_ID = "ErrorVideo1"
MUTATING_ID = "Mutating001"
VERSION = "2026.08.27"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def request_for(*video_ids: str) -> dict:
    core = {
        "schema_version": 1,
        "request_kind": PROBE_REQUEST_KIND,
        "evidence_binding_sha256": "a" * 64,
        "producer_contract": {
            "tool": "yt-dlp",
            "invocation_flags": list(PROBE_FLAGS),
            "one_target_per_process": True,
        },
        "network_policy": dict(NETWORK_POLICY),
        "targets": [
            {
                "youtube_video_id": video_id,
                "canonical_url": f"https://www.youtube.com/watch?v={video_id}",
            }
            for video_id in video_ids
        ],
    }
    return {
        **core,
        "request_sha256": hashlib.sha256(
            canonical_json(core).encode("utf-8")
        ).hexdigest(),
    }


class TorrentAvailabilityProbeTests(unittest.TestCase):
    def setUp(self) -> None:
        work = CORPUS_ROOT / "work"
        work.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="torrent-availability-probe-test-", dir=work
        )
        self.root = Path(self.temporary.name).resolve()
        self.log = self.root / "invocations.jsonl"
        self.executable = self.root / "yt-dlp"
        source = f'''#!/usr/bin/python3
import json
import os
import pathlib
import sys
import time

log = pathlib.Path({str(self.log)!r})
args = sys.argv[1:]
with log.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({{"args": args, "environment": sorted(os.environ)}}) + "\\n")
if args == ["--ignore-config", "--version"]:
    print({VERSION!r})
    raise SystemExit(0)
video_id = args[-1].split("v=", 1)[1]
if video_id == {AVAILABLE_ID!r}:
    print(json.dumps({{"id": video_id, "extractor": "youtube", "duration": 1.5}}))
    raise SystemExit(0)
if video_id == {MUTATING_ID!r}:
    with pathlib.Path(sys.argv[0]).open("a", encoding="utf-8") as handle:
        handle.write("# changed during target\\n")
    print(json.dumps({{"id": video_id, "extractor": "youtube"}}))
    raise SystemExit(0)
messages = {{
    {PRIVATE_ID!r}: "ERROR: Private video. Sign in if granted access RAW-SECRET",
    {REMOVED_ID!r}: "ERROR: This video has been removed by the uploader RAW-SECRET",
    {SIGNIN_ID!r}: "ERROR: Sign in to confirm your age RAW-SECRET",
    {RATE_ID!r}: "ERROR: HTTP Error 429: Too Many Requests RAW-SECRET",
    {NETWORK_ID!r}: "ERROR: Unable to download webpage: connection timed out RAW-SECRET",
    {ERROR_ID!r}: "ERROR: extractor changed unexpectedly RAW-SECRET",
}}
print(messages[video_id], file=sys.stderr)
raise SystemExit(1)
'''
        self.executable.write_text(source, encoding="utf-8")
        self.executable.chmod(0o700)
        self.executable_sha256 = digest(self.executable)
        self.checkpoint = self.root / "checkpoint.json"
        self.output = self.root / "probe.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _produce(self, request: dict, **overrides):
        options = {
            "yt_dlp_executable": self.executable,
            "expected_executable_sha256": self.executable_sha256,
            "expected_version": VERSION,
            "checkpoint_path": self.checkpoint,
            "output_path": self.output,
            "timeout_seconds": 5,
            "inter_target_delay_seconds": 0,
        }
        options.update(overrides)
        return produce_availability_probe(request, **options)

    def _invocations(self) -> list[dict]:
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text(encoding="utf-8").splitlines()]

    def test_complete_probe_is_ordered_fixed_flagged_private_and_schema_valid(self):
        request = request_for(
            AVAILABLE_ID,
            PRIVATE_ID,
            REMOVED_ID,
            SIGNIN_ID,
            RATE_ID,
            NETWORK_ID,
            ERROR_ID,
        )
        with patch.dict(
            os.environ,
            {
                "HTTP_PROXY": "http://user:password@example.invalid",
                "COOKIE": "RAW-SECRET",
                "AUTHORIZATION": "RAW-SECRET",
                "YTDLP_CONFIG": "RAW-SECRET",
                "PYTHONPATH": "RAW-SECRET",
            },
        ):
            summary = self._produce(request)

        self.assertTrue(summary["complete"])
        self.assertFalse(summary["reused_existing_output"])
        self.assertEqual(
            summary["outcome_counts"],
            {"available": 1, "unavailable": 2, "indeterminate": 4},
        )
        self.assertFalse(summary["raw_diagnostics_retained"])
        self.assertTrue(summary["executable_file_hash_pinned"])
        self.assertFalse(summary["runtime_module_tree_pinned"])
        self.assertFalse(summary["helper_runtime_executables_pinned"])
        self.assertNotIn(str(self.output), json.dumps(summary))
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o400)
        self.assertEqual(stat.S_IMODE(self.checkpoint.stat().st_mode), 0o600)

        result = json.loads(self.output.read_text(encoding="utf-8"))
        checkpoint = json.loads(self.checkpoint.read_text(encoding="utf-8"))
        self.assertNotIn("RAW-SECRET", self.output.read_text(encoding="utf-8"))
        self.assertNotIn("RAW-SECRET", self.checkpoint.read_text(encoding="utf-8"))
        self.assertEqual(checkpoint["checkpoint_kind"], CHECKPOINT_KIND)
        self.assertEqual(checkpoint["next_target_index"], len(request["targets"]))
        self.assertEqual(result["outcomes"][0]["evidence_code"], "metadata_resolved")
        self.assertEqual(result["outcomes"][1]["evidence_code"], "private")
        self.assertEqual(result["outcomes"][2]["evidence_code"], "removed")
        self.assertEqual(result["outcomes"][3]["evidence_code"], "sign_in_required")
        self.assertEqual(result["outcomes"][4]["evidence_code"], "rate_limited")
        self.assertEqual(result["outcomes"][5]["evidence_code"], "network_error")
        self.assertEqual(result["outcomes"][6]["evidence_code"], "extractor_error")

        result_schema = json.loads(
            (CORPUS_ROOT / "schemas" / "torrent-youtube-availability-probe.schema.json")
            .read_text(encoding="utf-8")
        )
        checkpoint_schema = json.loads(
            (
                CORPUS_ROOT
                / "schemas"
                / "torrent-youtube-availability-probe-checkpoint.schema.json"
            ).read_text(encoding="utf-8")
        )
        Draft202012Validator.check_schema(result_schema)
        Draft202012Validator(result_schema).validate(result)
        Draft202012Validator.check_schema(checkpoint_schema)
        Draft202012Validator(checkpoint_schema).validate(checkpoint)

        invocations = self._invocations()
        target_invocations = [row for row in invocations if "--version" not in row["args"]]
        self.assertEqual(
            [row["args"] for row in target_invocations],
            [
                [*PROBE_FLAGS, target["canonical_url"]]
                for target in request["targets"]
            ],
        )
        for invocation in invocations:
            environment = invocation["environment"]
            self.assertNotIn("HTTP_PROXY", environment)
            self.assertNotIn("COOKIE", environment)
            self.assertNotIn("AUTHORIZATION", environment)
            self.assertNotIn("YTDLP_CONFIG", environment)
            self.assertNotIn("PYTHONPATH", environment)

    def test_interrupted_run_resumes_from_digest_bound_outcome_prefix(self):
        request = request_for(AVAILABLE_ID, PRIVATE_ID)
        from himr_corpus import torrent_availability_probe as module

        original = module._run_target
        calls = 0

        def interrupt_after_first(executable, target, *, timeout_seconds):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise KeyboardInterrupt
            return original(
                executable, target, timeout_seconds=timeout_seconds
            )

        with patch.object(module, "_run_target", side_effect=interrupt_after_first):
            with self.assertRaises(KeyboardInterrupt):
                self._produce(request)
        partial = json.loads(self.checkpoint.read_text(encoding="utf-8"))
        self.assertEqual(partial["next_target_index"], 1)
        self.assertFalse(self.output.exists())

        summary = self._produce(request)
        self.assertTrue(summary["complete"])
        target_invocations = [
            row for row in self._invocations() if "--version" not in row["args"]
        ]
        self.assertEqual(
            [row["args"][-1] for row in target_invocations],
            [target["canonical_url"] for target in request["targets"]],
        )

    def test_tampered_checkpoint_and_wrong_executable_pin_fail_closed(self):
        request = request_for(AVAILABLE_ID, PRIVATE_ID)
        from himr_corpus import torrent_availability_probe as module

        original = module._run_target
        calls = 0

        def interrupt_after_first(executable, target, *, timeout_seconds):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise KeyboardInterrupt
            return original(executable, target, timeout_seconds=timeout_seconds)

        with patch.object(module, "_run_target", side_effect=interrupt_after_first):
            with self.assertRaises(KeyboardInterrupt):
                self._produce(request)
        checkpoint = json.loads(self.checkpoint.read_text(encoding="utf-8"))
        checkpoint["outcomes"][0]["youtube_video_id"] = PRIVATE_ID
        self.checkpoint.chmod(0o600)
        self.checkpoint.write_text(
            json.dumps(checkpoint, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(TorrentAvailabilityProbeError, "SHA-256"):
            self._produce(request)
        with self.assertRaisesRegex(TorrentAvailabilityProbeError, "SHA-256 pin"):
            self._produce(
                request,
                expected_executable_sha256="0" * 64,
                checkpoint_path=self.root / "other-checkpoint.json",
                output_path=self.root / "other-output.json",
            )

    def test_existing_valid_output_is_idempotently_reused(self):
        request = request_for(AVAILABLE_ID)
        first = self._produce(request)
        target_count = len(
            [row for row in self._invocations() if "--version" not in row["args"]]
        )
        second = self._produce(request)
        self.assertEqual(first["result_sha256"], second["result_sha256"])
        self.assertTrue(second["reused_existing_output"])
        self.assertEqual(
            len([row for row in self._invocations() if "--version" not in row["args"]]),
            target_count,
        )

        self.output.chmod(0o600)
        with self.assertRaisesRegex(TorrentAvailabilityProbeError, "owner-controlled"):
            self._produce(request)

    def test_request_and_full_plan_inputs_are_digest_bound(self):
        request = request_for(AVAILABLE_ID)
        request_path = self.root / "request.json"
        request_path.write_text(json.dumps(request), encoding="utf-8")
        self.assertEqual(load_availability_probe_request(request_path), request)

        core = {
            "schema_version": 1,
            "plan_kind": PLAN_KIND,
            "planner_version": PLANNER_VERSION,
            "inputs": {},
            "catalog_binding": {},
            "archive_binding": {},
            "evidence_binding_sha256": request["evidence_binding_sha256"],
            "coverage": {},
            "availability_probe_request": request,
            "availability_probe": None,
            "probe_candidates": [],
            "malformed_manual_review": [],
            "selected_files": [],
            "selected_torrent_file_indices": [],
            "statistics": {},
            "policy": {},
        }
        plan_sha256 = hashlib.sha256(canonical_json(core).encode("utf-8")).hexdigest()
        plan = {
            **core,
            "plan_id": f"tslp_{plan_sha256[:32]}",
            "plan_sha256": plan_sha256,
        }
        plan_path = self.root / "plan.json"
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        self.assertEqual(load_availability_probe_request(plan_path), request)
        plan["policy"]["tampered"] = True
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        with self.assertRaisesRegex(TorrentAvailabilityProbeError, "SHA-256"):
            load_availability_probe_request(plan_path)

    def test_executable_mutation_aborts_before_outcome_checkpoint(self):
        request = request_for(MUTATING_ID)
        with self.assertRaisesRegex(TorrentAvailabilityProbeError, "executable changed"):
            self._produce(request)
        checkpoint = json.loads(self.checkpoint.read_text(encoding="utf-8"))
        self.assertEqual(checkpoint["next_target_index"], 0)
        self.assertFalse(self.output.exists())

    def test_request_rejects_credentials_duplicates_and_digest_changes(self):
        request = request_for(AVAILABLE_ID)
        request["network_policy"]["cookies_sent"] = True
        with self.assertRaisesRegex(TorrentAvailabilityProbeError, "credentials"):
            validate_availability_probe_request(request)
        duplicate = request_for(AVAILABLE_ID, AVAILABLE_ID)
        with self.assertRaisesRegex(TorrentAvailabilityProbeError, "duplicate"):
            validate_availability_probe_request(duplicate)
        changed = request_for(AVAILABLE_ID)
        changed["targets"][0]["canonical_url"] += "&list=unsafe"
        with self.assertRaisesRegex(TorrentAvailabilityProbeError, "noncanonical"):
            validate_availability_probe_request(changed)

    def test_failure_classifier_is_closed_and_conservative(self):
        fixtures = {
            b"Private video. Sign in": ("unavailable", "private"),
            b"This video has been removed": ("unavailable", "removed"),
            b"Sign in to confirm your age": ("indeterminate", "sign_in_required"),
            b"Sign in to confirm you're not a bot": (
                "indeterminate",
                "sign_in_required",
            ),
            b"This members-only video requires channel membership": (
                "indeterminate",
                "sign_in_required",
            ),
            b"Video unavailable": ("unavailable", "video_unavailable"),
            b"HTTP Error 429: Too Many Requests": ("indeterminate", "rate_limited"),
            b"Unable to download webpage: timed out": ("indeterminate", "network_error"),
            b"unknown extractor wording": ("indeterminate", "extractor_error"),
        }
        for diagnostic, expected in fixtures.items():
            with self.subTest(diagnostic=diagnostic):
                self.assertEqual(_classify_failure(diagnostic, b""), expected)

    def test_cli_exposes_atomic_resumable_producer(self):
        choices = build_parser()._subparsers._group_actions[0].choices
        parser = choices["produce-torrent-youtube-availability-probe"]
        destinations = {action.dest for action in parser._actions}
        self.assertIn("output", destinations)
        self.assertIn("checkpoint", destinations)
        self.assertIn("input", destinations)
        self.assertIn("yt_dlp_sha256", destinations)
        self.assertIn("yt_dlp_version", destinations)


if __name__ == "__main__":
    unittest.main()
