"""Archive-preparation contracts with tiny fixtures; no real media/model work."""
from concurrent.futures import Future
from contextlib import ExitStack
import copy
import errno
import hashlib
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock

from pipeline import speaker_screen_archive_prepare as prepare
from pipeline import speaker_screen as screen


class ImmediateExecutor:
    instances = []
    def __init__(self, *, max_workers, mp_context):
        self.max_workers = max_workers
        self.start_method = mp_context.get_start_method()
        self.submissions, self.shutdown_calls = [], []
        self.instances.append(self)
    def submit(self, function, *args):
        self.submissions.append((function, args))
        future = Future()
        try:
            future.set_result(function(*args))
        except BaseException as error:
            future.set_exception(error)
        return future
    def shutdown(self, **kwargs):
        self.shutdown_calls.append(kwargs)


class ArchivePrepareTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="archive-prepare-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "preparation"
        self.runtime = {"python": prepare.guided.old_batch._python_binding()}
        self.ffmpeg = self.file("ffmpeg", b"fake tool never executed", executable=True)
        self.ffprobe = self.file("ffprobe", b"fake probe never executed", executable=True)
        self.models = {"kind": "himr_speaker_screen_models", "schema_version": 1,
            "silero_vad": self.file("vad.onnx", b"no real model"),
            "ecapa_embedding": self.file("model.ckpt", b"no real model either")}
        self.request = {"kind": "himr_archive_speaker_screen_prepare_request", "schema_version": 1,
            "state_root": str(self.state), "checkpoints": [self.document("controller.json", {"fixture": "checkpoint"})],
            "admissions": [self.document("admissions.json", {"fixture": "admissions"})],
            "ffmpeg": self.ffmpeg, "ffprobe": self.ffprobe, "models": self.models,
            "execution": {"device": "cuda", "gpu_uuid": "GPU-00000000-0000-0000-0000-000000000001",
                          "threads": 1, "batch_size": 4, "decode_prefetch": 2, "max_run_seconds": 3600},
            "sampling": "fast-triage", "recordings_per_batch": 64, "probe_workers": 2,
            "probe_timeout_seconds": 30, "max_prepare_seconds": 60,
            "free_space_floor_bytes": 1024**3}
        self.records = []
        for index in range(2):
            ref = self.file(f"source-{index}.media", b"synthetic source " + bytes([index]))
            recording = {"media_id": f"source:{index}", **ref,
                         "byte_count": Path(ref["path"]).stat().st_size, "duration_hint_ms": 10_999}
            acquisition = self.document(f"acquisition-{index}.json", {"fixture": index})
            self.records.append({"recording": recording, "aliases": [f"alias-{index}"],
                                 "acquisition_result": acquisition})
        self.records[0]["aliases"].append("audio-duplicate-alias")
        self.inventory = {"kind": "himr_private_speaker_screen_archive_inventory", "schema_version": 1, "records": self.records,
                          "counts": {"admitted_sources": 3, "unique_media": 2}}
        self.probes = [self.probe(self.records[0]), self.probe(self.records[1], status="needs_review")]
        self.sealed_batches, self.created_campaigns = [], []
        ImmediateExecutor.instances = []

    def file(self, name, body, *, executable=False):
        path = self.root / name
        path.write_bytes(body)
        path.chmod(0o700 if executable else 0o600)
        return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest()}

    def document(self, name, value):
        return self.file(name, screen.canonical(value))

    def probe(self, record, *, status="admitted"):
        with screen.opened(record["recording"]["path"]) as source:
            witness = screen.witness(source)
        return {"kind": "himr_speaker_screen_source_probe", "schema_version": 1,
            "status": status, "recording": copy.deepcopy(record["recording"]), "source_witness": witness,
            "tools": {"ffmpeg": self.ffmpeg, "ffprobe": self.ffprobe},
            "duration_ms": 10_001 if status == "admitted" else None,
            "timeline": {"measured_eof_ms": 10_001, "screenable_start_ms": 100,
                         "leading_interval_not_admitted_ms": 100} if status == "admitted" else None,
            "checks": {name: {"start_ms": 100, "end_ms": 10001, "pcm_bytes": 9901 * 32,
                              "exact_pcm_length": True} for name in ("first_probe", "last_probe")}, "tail_attempts": [],
            "error": None if status == "admitted" else {"code": "no_audio_stream", "message": "no audio stream"},
            "semantics": {"source_sha256_reverified": False}, "elapsed_seconds": 0.1}

    def receipt(self, record=None, probe=None, implementation=None):
        record = record or self.records[0]
        return {"kind": "himr_archive_screen_source_admission", "schema_version": 1,
                "recording": copy.deepcopy(record["recording"]),
                "implementation": implementation or prepare._implementation(),
                "probe": copy.deepcopy(probe or self.probe(record))}

    def stub_seal(self, path, expected, output):
        request = screen.read_json(Path(path), expected)
        self.sealed_batches.append(request)
        folder = Path(output).parent
        prepare._mkdir(folder)
        result = {"kind": "himr_archive_guided_speaker_screen_manifest", "schema_version": 1,
                  "state_root": str(folder), "fixture": len(self.sealed_batches)}
        screen.write_immutable(Path(output), result)
        return result

    def stub_campaign(self, path, expected, output):
        request = screen.read_json(Path(path), expected)
        self.created_campaigns.append(request)
        prepare._mkdir(Path(output).parent)
        result = {"kind": "himr_guided_speaker_screen_campaign_manifest", "schema_version": 1,
                  "campaign_id": "synthetic-campaign", "fixture": True}
        screen.write_immutable(Path(output), result)
        return result

    def mocks(self, *, probes=None):
        stack = ExitStack()
        selected = self.probes if probes is None else probes
        def probe_source(recording, *_args, **_kwargs):
            index = next(index for index, record in enumerate(self.records) if record["recording"] == recording)
            result = selected[index]
            if isinstance(result, BaseException):
                raise result
            return copy.deepcopy(result)
        self.runtime_mock = stack.enter_context(mock.patch.object(prepare.guided, "_runtime_binding", return_value=self.runtime))
        self.inventory_mock = stack.enter_context(mock.patch.object(prepare.inventory_api, "inventory", return_value=self.inventory))
        self.probe_mock = stack.enter_context(mock.patch.object(prepare.source_api, "probe_source", side_effect=probe_source))
        stack.enter_context(mock.patch.object(prepare, "ProcessPoolExecutor", ImmediateExecutor))
        stack.enter_context(mock.patch.object(prepare.os, "statvfs", return_value=SimpleNamespace(f_bavail=1024**3, f_frsize=4096)))
        self.seal_mock = stack.enter_context(mock.patch.object(prepare.guided, "seal_manifest", side_effect=self.stub_seal))
        self.campaign_mock = stack.enter_context(mock.patch.object(prepare.campaign, "create_campaign", side_effect=self.stub_campaign))
        return stack

    def request_reference(self):
        return self.document("request.json", self.request)

    def test_request_normalizes_only_cuda_fast_triage(self):
        value = prepare.validate_request(self.request)
        self.assertEqual(value["execution"]["device"], "cuda")
        self.assertEqual(value["execution"]["host_memory_max_bytes"], 4 * 1024**3)
        self.assertEqual(value["sampling"], "fast-triage")
        self.assertFalse(self.state.exists())

    def test_request_rejects_unknown_fields_cpu_other_sampling_and_wrong_versions(self):
        changes = [lambda r: r.update(extra=True), lambda r: r.update(schema_version=True),
                   lambda r: r.update(sampling="throughput"),
                   lambda r: r["execution"].update(device="cpu", gpu_uuid=None),
                   lambda r: r.update(checkpoints=[]), lambda r: r.update(admissions=[])]
        for change in changes:
            request = copy.deepcopy(self.request)
            change(request)
            with self.subTest(request=request), self.assertRaises(screen.ScreenError):
                prepare.validate_request(request)

    def test_request_numeric_limits_and_boolean_rejection(self):
        for key, values in {"recordings_per_batch": (31, 129, True), "probe_workers": (0, 3, True),
                "probe_timeout_seconds": (9, 121), "max_prepare_seconds": (59, 28801),
                "free_space_floor_bytes": (1024**3 - 1, 64 * 1024**3 + 1)}.items():
            for value in values:
                request = copy.deepcopy(self.request)
                request[key] = value
                with self.subTest(key=key, value=value), self.assertRaises(screen.ScreenError):
                    prepare.validate_request(request)

    def test_request_rejects_overlapping_metadata_models_and_tools(self):
        references = [self.request["checkpoints"][0], self.request["admissions"][0], self.ffmpeg,
                      self.ffprobe, self.models["silero_vad"], self.models["ecapa_embedding"]]
        for reference in references:
            request = copy.deepcopy(self.request)
            request["state_root"] = reference["path"]
            with self.subTest(reference=reference), self.assertRaises(screen.ScreenError):
                prepare.validate_request(request)

    def test_make_order_uses_admitted_duration_not_hint_and_preserves_source(self):
        original = copy.deepcopy(self.records[0])
        order = prepare.make_order(self.records[0], self.probes[0], self.request, self.state)
        self.assertEqual(order["recording"]["duration_ms"], 10_001)
        self.assertNotIn("duration_hint_ms", order["recording"])
        self.assertEqual(order["policy"]["stride_ms"], 300_000)
        self.assertEqual(order["policy"]["max_windows"], 64)
        self.assertEqual(order["resources"]["max_windows_per_run"], 64)
        self.assertFalse(order["resources"]["early_stop_on_positive"])
        self.assertEqual(order["source_verification"], "metadata_witness")
        self.assertEqual(self.records[0], original)
        self.assertFalse(Path(order["output_root"]).exists())

    def test_make_order_rejects_unadmitted_or_mismatched_source_and_bad_duration(self):
        for change in (lambda p: p.update(status="needs_review"),
                       lambda p: p["recording"].update(sha256="0" * 64),
                       lambda p: p.update(duration_ms=True), lambda p: p.update(duration_ms=86_400_001)):
            value = copy.deepcopy(self.probes[0])
            change(value)
            with self.assertRaises(screen.ScreenError):
                prepare.make_order(self.records[0], value, self.request, self.state)

    def test_admission_must_prove_explicit_100ms_start_and_preserve_full_eof(self):
        changes = [lambda p: p["timeline"].update(screenable_start_ms=0),
                   lambda p: p["timeline"].update(leading_interval_not_admitted_ms=0),
                   lambda p: p["timeline"].update(measured_eof_ms=9_999),
                   lambda p: p["checks"]["first_probe"].update(start_ms=0),
                   lambda p: p["checks"]["last_probe"].update(end_ms=10_000),
                   lambda p: p["checks"]["last_probe"].update(pcm_bytes=1),
                   lambda p: p["checks"]["first_probe"].update(exact_pcm_length=False),
                   lambda p: p.update(duration_ms=100)]
        for change in changes:
            value = copy.deepcopy(self.probes[0])
            change(value)
            with self.subTest(change=change), self.assertRaises(screen.ScreenError):
                prepare.make_order(self.records[0], value, self.request, self.state)

    def test_large_immutable_idempotent_private_and_never_overwrites(self):
        path = self.root / "large.json"
        value = {"records": ["large-test"] * 30}
        prepare._write_large_immutable(path, value)
        before = path.read_bytes()
        self.assertEqual(path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(path.stat().st_nlink, 1)
        prepare._write_large_immutable(path, copy.deepcopy(value))
        self.assertEqual(path.read_bytes(), before)
        with self.assertRaises(screen.ScreenError):
            prepare._write_large_immutable(path, {"different": True})
        self.assertEqual(path.read_bytes(), before)
        self.assertFalse(list(self.root.glob(".archive-screen-*")))

    def test_large_immutable_size_rejected_before_write(self):
        with mock.patch.object(prepare, "MAX_INVENTORY_BYTES", 8):
            with self.assertRaises(screen.ScreenError):
                prepare._write_large_immutable(self.root / "large.json", {"too_large": True})
        self.assertFalse((self.root / "large.json").exists())

    def test_large_inventory_publication_never_uses_hardlinks(self):
        path = self.root / "large.json"
        with mock.patch.object(prepare.os, "link", side_effect=AssertionError("hardlink publication forbidden")):
            prepare._write_large_immutable(path, {"inventory": True})
        self.assertEqual(path.stat().st_nlink, 1)
        self.assertFalse(list(self.root.glob(".archive-screen-*")))

    def test_interruption_after_atomic_inventory_rename_leaves_reusable_single_link(self):
        path = self.root / "large.json"
        value = {"inventory": "atomic publication"}
        original = os.fsync
        def interrupt_directory_sync(descriptor):
            import stat
            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                raise OSError(errno.EIO, "simulated directory sync failure after rename")
            return original(descriptor)
        with mock.patch.object(prepare.os, "fsync", side_effect=interrupt_directory_sync):
            with self.assertRaises(OSError):
                prepare._write_large_immutable(path, value)
        self.assertEqual(path.stat().st_nlink, 1)
        self.assertEqual(screen.read_json(path), value)
        prepare._write_large_immutable(path, value)
        self.assertFalse(list(self.root.glob(".archive-screen-*")))

    def test_status_snapshot_replaces_only_status_and_cleans_temp_files(self):
        protected = self.file("protected", b"unchanged")
        prepare._snapshot(self.root, {"state": "inventory"})
        prepare._snapshot(self.root, {"state": "checking_audio_endpoints", "checked_files": 2})
        value = screen.read_json(self.root / "preparation-status.json")
        self.assertEqual(value["state"], "checking_audio_endpoints")
        self.assertEqual(value["checked_files"], 2)
        self.assertEqual(value["kind"], "himr_archive_screen_preparation_status")
        self.assertEqual((self.root / "preparation-status.json").stat().st_mode & 0o777, 0o600)
        self.assertEqual(Path(protected["path"]).read_bytes(), b"unchanged")
        self.assertFalse(list(self.root.glob(".progress-*")))

    def test_snapshot_size_rejected_without_overwriting_previous(self):
        prepare._snapshot(self.root, {"state": "before"})
        before = (self.root / "preparation-status.json").read_bytes()
        with mock.patch.object(screen, "MAX_JSON", 10), self.assertRaises(screen.ScreenError):
            prepare._snapshot(self.root, {"state": "after"})
        self.assertEqual((self.root / "preparation-status.json").read_bytes(), before)

    def test_cached_probe_round_trip_missing_cache_and_implementation_drift(self):
        path = self.root / "probe.json"
        implementation = prepare._implementation()
        self.assertIsNone(prepare._cached_probe(path, self.records[0], implementation))
        screen.write_immutable(path, self.receipt(implementation=implementation))
        self.assertEqual(prepare._cached_probe(path, self.records[0], implementation), self.probes[0])
        with self.assertRaises(screen.ScreenError):
            prepare._cached_probe(path, self.records[0], {"different": "implementation"})

    def test_cached_probe_rejects_source_witness_drift(self):
        path = self.root / "probe.json"
        implementation = prepare._implementation()
        screen.write_immutable(path, self.receipt(implementation=implementation))
        source = Path(self.records[0]["recording"]["path"])
        os.utime(source, ns=(source.stat().st_atime_ns, source.stat().st_mtime_ns + 1))
        with self.assertRaisesRegex(screen.ScreenError, "source changed"):
            prepare._cached_probe(path, self.records[0], implementation)

    def test_malformed_cached_probe_receipts_fail_closed(self):
        changes = [lambda r: r.update(kind="wrong"), lambda r: r.update(schema_version=True),
                   lambda r: r.update(extra="unexpected"), lambda r: r["probe"].update(status="invented"),
                   lambda r: r["probe"].update(duration_ms=True), lambda r: r["probe"].update(duration_ms=0),
                   lambda r: r["probe"].update(duration_ms=86_400_001),
                   lambda r: r["probe"].update(error={"code": "failure", "message": "failure"}),
                   lambda r: r["probe"].update(status="needs_review", duration_ms=10_001),
                   lambda r: r["probe"].update(status="needs_review", duration_ms=None, error=None)]
        implementation = prepare._implementation()
        for index, change in enumerate(changes):
            value = self.receipt(implementation=implementation)
            change(value)
            ref = self.document(f"bad-cache-{index}.json", value)
            with self.subTest(index=index), self.assertRaises(screen.ScreenError):
                prepare._cached_probe(Path(ref["path"]), self.records[0], implementation)

    def test_prepare_two_sources_retains_noaudio_disposition_and_aliases(self):
        request = self.request_reference()
        with self.mocks():
            value = prepare.prepare(request["path"], request["sha256"])
        self.assertEqual(value["state"], "ready")
        self.assertEqual(value["sampling"], "fast-triage")
        self.assertEqual(value["admitted_unique_files"], 1)
        self.assertEqual(value["admitted_source_aliases"], 2)
        self.assertEqual(value["no_audio_unique_files"], 1)
        self.assertEqual(value["needs_review_unique_files"], 0)
        self.assertEqual(value["batches"], 1)
        self.assertEqual(self.probe_mock.call_count, 2)
        executor = ImmediateExecutor.instances[0]
        self.assertEqual(executor.max_workers, 2)
        self.assertEqual(executor.start_method, "spawn")
        self.assertEqual(executor.shutdown_calls, [{"wait": True, "cancel_futures": True}])
        self.inventory_mock.assert_called_once_with(self.request["checkpoints"], self.request["admissions"], verify_results=True)
        exclusions = screen.read_json(self.state / "excluded-inputs.json")
        self.assertTrue(exclusions["excluded_is_not_single_speaker"])
        self.assertEqual(exclusions["records"][0]["disposition"], "no_audio")
        self.assertEqual(exclusions["records"][0]["aliases"], ["alias-1"])
        self.assertEqual(len(list((self.state / "source-probes").glob("*.json"))), 2)
        self.assertEqual(len(list((self.state / "orders").glob("*.json"))), 1)
        self.assertEqual(self.sealed_batches[0]["kind"], "himr_archive_guided_speaker_screen_request")
        self.assertEqual(self.sealed_batches[0]["max_target_windows"], 0)
        self.assertEqual(value["leading_interval_not_screened_per_admitted_file_ms"], 100)
        self.assertEqual(value["total_leading_interval_not_screened_ms"], 100)
        self.assertEqual(len(self.sealed_batches[0]["work_orders"]), 1)
        self.assertEqual(self.created_campaigns[0]["max_passes_per_batch"], 8)
        self.assertFalse((self.state / "unused-order-outputs").exists())

    def test_timeline_failure_stays_needs_review_not_noaudio_or_solo(self):
        self.probes[1]["error"] = {"code": "nonzero_audio_start_requires_timeline_review", "message": "review required"}
        request = self.request_reference()
        with self.mocks():
            value = prepare.prepare(request["path"], request["sha256"])
        self.assertEqual(value["needs_review_unique_files"], 1)
        self.assertEqual(value["no_audio_unique_files"], 0)
        row = screen.read_json(self.state / "excluded-inputs.json")["records"][0]
        self.assertEqual(row["disposition"], "needs_review")
        self.assertEqual(row["reason"]["code"], "nonzero_audio_start_requires_timeline_review")

    def test_completed_preparation_resume_only_validates_bound_campaign(self):
        request = self.request_reference()
        with self.mocks():
            first = prepare.prepare(request["path"], request["sha256"])
        result_path = self.state / "preparation-result.json"
        before = result_path.read_bytes()
        before_mtime = result_path.stat().st_mtime_ns
        def load(path, digest):
            return screen.read_json(Path(path), digest)
        with self.mocks(), mock.patch.object(prepare.campaign, "_load_manifest", side_effect=load) as loader:
            second = prepare.prepare(request["path"], request["sha256"])
        self.assertEqual(second, first)
        loader.assert_called_once_with(first["campaign"]["path"], first["campaign"]["sha256"])
        self.assertEqual(self.probe_mock.call_count, 0)
        self.assertEqual(self.inventory_mock.call_count, 0)
        self.assertEqual(self.seal_mock.call_count, 0)
        self.assertEqual(self.campaign_mock.call_count, 0)
        self.assertEqual(result_path.read_bytes(), before)
        self.assertEqual(result_path.stat().st_mtime_ns, before_mtime)
        self.assertTrue(first["source_trust"]["immutable_cas_required_after_initial_launch"])
        self.assertFalse(first["source_trust"]["full_source_hash"])
        guard = screen.read_json(Path(first["source_guard"]["path"]), first["source_guard"]["sha256"])
        self.assertEqual(len(guard["receipts"]), 2)
        self.assertEqual([row["status"] for row in guard["receipts"]], ["admitted", "needs_review"])

    def test_completed_resume_rejects_changed_campaign_without_launch(self):
        request = self.request_reference()
        with self.mocks():
            first = prepare.prepare(request["path"], request["sha256"])
        Path(first["campaign"]["path"]).write_bytes(b'{"campaign_id":"tampered"}\n')
        with self.mocks(), mock.patch.object(prepare.campaign, "_load_manifest",
                side_effect=lambda path, digest: screen.read_json(Path(path), digest)):
            with self.assertRaises(screen.ScreenError):
                prepare.prepare(request["path"], request["sha256"])
        self.assertEqual(self.probe_mock.call_count, 0)
        self.assertEqual(self.campaign_mock.call_count, 0)

    def test_completed_resume_rejects_malformed_result_or_external_campaign_path(self):
        request = self.request_reference()
        with self.mocks():
            prepare.prepare(request["path"], request["sha256"])
        path = self.state / "preparation-result.json"
        original = screen.read_json(path)
        for change in (lambda result: result.update(schema_version=True),
                       lambda result: result.update(state="failed"),
                       lambda result: result["campaign"].update(path=str(self.root / "unrelated.json"))):
            altered = copy.deepcopy(original)
            change(altered)
            path.write_bytes(screen.canonical(altered))
            with self.mocks(), self.assertRaisesRegex(screen.ScreenError, "saved archive preparation result"):
                prepare.prepare(request["path"], request["sha256"])
            self.assertEqual(self.probe_mock.call_count, 0)

    def test_interrupted_preparation_reuses_completed_probe_receipts(self):
        request = self.request_reference()
        with self.mocks(), mock.patch.object(prepare.guided, "seal_manifest", side_effect=RuntimeError("interrupted test")):
            with self.assertRaisesRegex(RuntimeError, "interrupted test"):
                prepare.prepare(request["path"], request["sha256"])
        receipts = {path: (path.read_bytes(), path.stat().st_mtime_ns)
                    for path in (self.state / "source-probes").glob("*.json")}
        self.assertEqual(len(receipts), 2)
        self.assertFalse((self.state / "preparation-result.json").exists())
        with self.mocks():
            resumed = prepare.prepare(request["path"], request["sha256"])
        self.assertEqual(resumed["state"], "ready")
        self.assertEqual(self.probe_mock.call_count, 0)
        for path, (body, mtime) in receipts.items():
            self.assertEqual(path.read_bytes(), body)
            self.assertEqual(path.stat().st_mtime_ns, mtime)

    def test_source_witness_drift_before_batch_seal_blocks_sealing(self):
        request = self.request_reference()
        original = prepare._binding
        def mutate_before_sealing(path):
            value = original(path)
            if Path(path).name == "request.json" and Path(path).parent.name.startswith("batch-"):
                source = Path(self.records[0]["recording"]["path"])
                os.utime(source, ns=(source.stat().st_atime_ns, source.stat().st_mtime_ns + 1))
            return value
        with self.mocks(), mock.patch.object(prepare, "_binding", side_effect=mutate_before_sealing):
            with self.assertRaisesRegex(screen.ScreenError, "source changed"):
                prepare.prepare(request["path"], request["sha256"])
        self.assertEqual(self.seal_mock.call_count, 0)
        self.assertEqual(self.campaign_mock.call_count, 0)

    def test_source_drift_after_batches_sealed_blocks_ready_publication(self):
        request = self.request_reference()
        def mutate_after_campaign(*args):
            result = self.stub_campaign(*args)
            source = Path(self.records[0]["recording"]["path"])
            os.utime(source, ns=(source.stat().st_atime_ns, source.stat().st_mtime_ns + 1))
            return result
        with self.mocks(), mock.patch.object(prepare.campaign, "create_campaign", side_effect=mutate_after_campaign):
            with self.assertRaisesRegex(screen.ScreenError, "source changed"):
                prepare.prepare(request["path"], request["sha256"])
        self.assertFalse((self.state / "preparation-result.json").exists())
        self.assertEqual(screen.read_json(self.state / "preparation-status.json")["state"], "failed")

    def test_completed_resume_rejects_same_size_source_replacement(self):
        request = self.request_reference()
        with self.mocks():
            prepare.prepare(request["path"], request["sha256"])
        source = Path(self.records[0]["recording"]["path"])
        replacement = self.root / "replacement.media"
        replacement.write_bytes(source.read_bytes())
        os.replace(replacement, source)
        with self.mocks(), mock.patch.object(prepare.campaign, "_load_manifest", return_value={}):
            with self.assertRaisesRegex(screen.ScreenError, "source changed"):
                prepare.prepare(request["path"], request["sha256"])
        self.assertEqual(self.probe_mock.call_count, 0)
        self.assertEqual(screen.read_json(self.state / "preparation-status.json")["state"], "failed")

    def test_completed_resume_rechecks_excluded_source_witness_too(self):
        request = self.request_reference()
        with self.mocks():
            prepare.prepare(request["path"], request["sha256"])
        source = Path(self.records[1]["recording"]["path"])
        os.utime(source, ns=(source.stat().st_atime_ns, source.stat().st_mtime_ns + 1))
        with self.mocks(), mock.patch.object(prepare.campaign, "_load_manifest", return_value={}):
            with self.assertRaisesRegex(screen.ScreenError, "source changed"):
                prepare.prepare(request["path"], request["sha256"])

    def test_completed_resume_rejects_changed_inventory_receipt_or_guard_bytes(self):
        request = self.request_reference()
        with self.mocks():
            prepare.prepare(request["path"], request["sha256"])
        files = [self.state / "inventory.json", self.state / "source-probes" / "source:0.json", self.state / "source-guard.json"]
        for path in files:
            before = path.read_bytes()
            path.chmod(0o600)
            path.write_bytes(before + b" ")
            try:
                with self.mocks(), mock.patch.object(prepare.campaign, "_load_manifest", return_value={}):
                    with self.subTest(path=path), self.assertRaises(screen.ScreenError):
                        prepare.prepare(request["path"], request["sha256"])
            finally:
                path.write_bytes(before)

    def test_source_guard_reads_large_inventory_with_its_separate_bound(self):
        request = self.request_reference()
        with self.mocks():
            ready = prepare.prepare(request["path"], request["sha256"])
        read = prepare.inventory_api._Reader.read
        calls = []
        def check(reader, reference, **kwargs):
            calls.append(kwargs.get("maximum"))
            return read(reader, reference, **kwargs)
        with mock.patch.object(prepare.inventory_api._Reader, "read", check):
            result = prepare._verify_source_guard(self.state, ready["source_guard"], prepare._implementation())
        self.assertEqual(calls, [prepare.MAX_INVENTORY_BYTES])
        self.assertEqual(result, {"unique_files": 2, "admitted_unique_files": 1, "excluded_unique_files": 1})

    def test_storage_error_stops_before_campaign_and_records_failed_snapshot(self):
        request = self.request_reference()
        with self.mocks(probes=[OSError(errno.EIO, "Input/output error"), self.probes[1]]):
            with self.assertRaises(OSError) as caught:
                prepare.prepare(request["path"], request["sha256"])
        self.assertEqual(caught.exception.errno, errno.EIO)
        self.assertEqual(self.seal_mock.call_count, 0)
        self.assertEqual(self.campaign_mock.call_count, 0)
        self.assertEqual(screen.read_json(self.state / "preparation-status.json")["state"], "failed")
        self.assertTrue(ImmediateExecutor.instances[0].shutdown_calls)

    def test_all_unadmitted_sources_fail_without_campaign_or_hidden_exclusions(self):
        request = self.request_reference()
        values = [self.probe(record, status="needs_review") for record in self.records]
        with self.mocks(probes=values), self.assertRaisesRegex(screen.ScreenError, "no exact audio timelines"):
            prepare.prepare(request["path"], request["sha256"])
        exclusions = screen.read_json(self.state / "excluded-inputs.json")
        self.assertEqual(len(exclusions["records"]), 2)
        self.assertEqual(self.campaign_mock.call_count, 0)

    def test_unmarked_existing_workspace_is_not_reused(self):
        self.state.mkdir(mode=0o700)
        (self.state / "existing.txt").write_text("preserve this")
        request = self.request_reference()
        with self.mocks(), self.assertRaisesRegex(screen.ScreenError, "unmarked nonempty"):
            prepare.prepare(request["path"], request["sha256"])
        self.assertEqual((self.state / "existing.txt").read_text(), "preserve this")
        self.assertEqual(self.probe_mock.call_count, 0)

    def test_free_space_floor_blocks_new_probe_submission(self):
        request = self.request_reference()
        with self.mocks(), mock.patch.object(prepare.os, "statvfs", return_value=SimpleNamespace(f_bavail=1, f_frsize=4096)):
            with self.assertRaisesRegex(screen.ScreenError, "free-space floor"):
                prepare.prepare(request["path"], request["sha256"])
        self.assertEqual(self.probe_mock.call_count, 0)
        self.assertEqual(self.campaign_mock.call_count, 0)


if __name__ == "__main__":
    unittest.main()
