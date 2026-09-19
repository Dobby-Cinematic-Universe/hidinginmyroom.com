"""Synthetic, offline regression tests for additive GPU runtime succession."""

from __future__ import annotations

import copy
import hashlib
import os
import stat
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from autonomous_controller import gpu_runtime_successor as successor
from autonomous_controller.config import ControllerConfig, canonical_bytes
from pipeline.tests import test_gpu_trusted_launcher_v2 as fixtures


def seal(value: dict, id_key: str, prefix: str) -> dict:
    core = {key: item for key, item in value.items() if key not in {"identity_sha256", id_key}}
    digest = hashlib.sha256(canonical_bytes(core)).hexdigest()
    return {**core, "identity_sha256": digest, id_key: prefix + digest[:32]}


class RuntimeSuccessorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="himr-successor-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.old, self.old_bundle = self.bundle("old", successor.OLD_BWRAP)
        self.new, self.new_bundle = self.bundle("new", successor.NEW_BWRAP)
        profile = self.old_bundle["profile"]
        gpu = {**self.old,
               "production_profile": profile["production_profile"]["path"],
               "production_profile_sha256": profile["production_profile"]["sha256"],
               "root_registration": profile["root_registration"]["path"],
               "root_registration_sha256": profile["root_registration"]["sha256"],
               "result_root": str(self.root / "results"),
               "unmodified_option": {"private": True}}
        document = {"config_id": "himrautocfg_" + "a" * 32,
                    "gpu_readiness": gpu, "state_root": str(self.root / "state")}
        path = self.root / "config.json"
        digest = self.write(path, document)
        self.config = ControllerConfig(document, path, digest)
        self.old_pin = mock.patch.object(successor, "OLD_RUNTIME_SHA256", self.old["runtime_admission_sha256"])
        self.old_pin.start()
        self.addCleanup(self.old_pin.stop)
        self.replay = mock.patch.object(successor, "_replay_current")
        self.replay_mock = self.replay.start()
        self.addCleanup(self.replay.stop)

    @staticmethod
    def write(path, value, mode=0o400):
        body = canonical_bytes(value) if isinstance(value, dict) else value
        if path.exists():
            path.chmod(0o600)
        path.write_bytes(body)
        path.chmod(mode)
        return hashlib.sha256(body).hexdigest()

    def bundle(self, name, tool):
        directory = self.root / name
        directory.mkdir(mode=0o700)
        controls = {"runtime_admission": str(directory / "runtime.json"),
                    "launcher_profile": str(directory / "profile.json"),
                    "local_readiness": str(directory / "readiness.json"),
                    "local_launcher": str(directory / "launcher")}
        launcher_sha = self.write(Path(controls["local_launcher"]), b"synthetic-launcher\n", 0o500)
        profile = fixtures.launcher_profile_core()
        profile["launcher"] = {"path": controls["local_launcher"], "sha256": launcher_sha}
        profile["runtime_admission_install_path"] = controls["runtime_admission"]
        profile["system_tools"]["bubblewrap"] = successor._without(tool, "name")
        if name == "new":
            profile["host_abi"]["platform"]["release"] = "7.1.13-reviewed"
            profile["host_abi"]["platform"]["version"] = "#2 SMP PREEMPT_DYNAMIC"
            profile["host_abi"]["platform"]["nvidia_kernel_module_report_sha256"] = "d" * 64
            profile["host_abi"] = seal(profile["host_abi"], "manifest_id", "gpuhostabi_")
        profile = seal(profile, "profile_id", "gpulaunchprofile_")
        controls["launcher_profile_sha256"] = self.write(Path(controls["launcher_profile"]), profile)
        runtime = fixtures.runtime_receipt(profile, admitted=False)
        trusted = runtime["trusted_install"]
        trusted["owner_uid"] = os.geteuid()
        trusted["launcher"].update(uid=os.geteuid(), mode="0500", byte_count=len(b"synthetic-launcher\n"))
        trusted["launcher_profile"].update(
            path=controls["launcher_profile"], sha256=controls["launcher_profile_sha256"],
            uid=os.geteuid(), mode="0400", byte_count=len(canonical_bytes(profile)))
        runtime = seal(runtime, "receipt_id", "gpurtv2_")
        controls["runtime_admission_sha256"] = self.write(Path(controls["runtime_admission"]), runtime)
        readiness = fixtures.LAUNCHER.make_local_readiness(
            launcher_profile=profile,
            launcher_profile_file={"path": controls["launcher_profile"], "sha256": controls["launcher_profile_sha256"]},
            runtime=runtime,
            runtime_file={"path": controls["runtime_admission"], "sha256": controls["runtime_admission_sha256"]},
            production_profile={"identity_sha256": profile["production_profile"]["identity_sha256"]},
            production_profile_file=successor._without(profile["production_profile"], "identity_sha256"),
            registration=profile["root_registration"],
            root_file={key: profile["root_registration"][key] for key in ("path", "sha256")},
            host_abi_identity_sha256=profile["host_abi"]["identity_sha256"],
            gpu={"uuid": fixtures.GPU_UUID}, host_envelope=fixtures.host_envelope())
        controls["local_readiness_sha256"] = self.write(Path(controls["local_readiness"]), readiness)
        return controls, {"profile": profile, "runtime": runtime, "readiness": readiness}

    def stage(self):
        return successor.stage_successor(self.config, self.new, assert_stopped=lambda: None)

    def test_real_control_schemas_accept_reviewed_tool_kernel_transition(self):
        value = successor.build_successor(self.config, self.new)
        self.assertEqual(value["old_controls"], self.old)
        self.assertEqual(value["new_controls"], self.new)
        self.assertNotEqual(value["old_runtime"], value["new_runtime"])
        self.assertEqual(value["policy"], successor.POLICY)
        self.replay_mock.assert_called_once()

    def test_stage_is_private_durable_exclusive_and_calls_guard_twice(self):
        guard = mock.Mock()
        value = successor.stage_successor(self.config, self.new, assert_stopped=guard)
        path = self.root / successor.SIDECAR_NAME
        self.assertEqual(guard.call_count, 2)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o400)
        self.assertEqual(path.stat().st_nlink, 1)
        self.assertEqual(successor.load_successor(self.config), value)
        with self.assertRaises(successor.RuntimeSuccessorError):
            successor.stage_successor(self.config, self.new, assert_stopped=guard)
        self.assertEqual(path.read_bytes(), canonical_bytes(value))
        self.assertEqual(list(self.root.glob(".*.tmp-*")), [])

    def test_second_stopped_guard_cancels_before_publication(self):
        guard = mock.Mock(side_effect=[None, RuntimeError("operator started")])
        with self.assertRaisesRegex(RuntimeError, "operator started"):
            successor.stage_successor(self.config, self.new, assert_stopped=guard)
        self.assertFalse((self.root / successor.SIDECAR_NAME).exists())

    def test_selector_preserves_historical_specs_and_other_gpu_fields(self):
        untouched = copy.deepcopy(self.config.document)
        self.assertEqual(successor.effective_gpu(self.config), self.config.section("gpu_readiness"))
        authorization = self.stage()
        fresh = successor.effective_gpu(self.config)
        self.assertEqual(fresh["runtime_admission"], self.new["runtime_admission"])
        self.assertEqual(fresh["unmodified_option"], {"private": True})
        self.assertEqual(successor.effective_gpu(self.config, authorization["new_runtime"]), fresh)
        self.assertEqual(successor.effective_gpu(self.config, authorization["old_runtime"]),
                         self.config.section("gpu_readiness"))
        self.assertEqual(self.config.document, untouched)
        fresh["unmodified_option"]["private"] = False
        self.assertEqual(self.config.document, untouched)

    def test_selector_rejects_unknown_or_mixed_runtime(self):
        authorization = self.stage()
        for field in authorization["old_runtime"]:
            bad = dict(authorization["old_runtime"])
            bad[field] = authorization["new_runtime"][field] if field != "status" else "admitted"
            if bad == authorization["old_runtime"]:
                bad[field] = "unexpected"
            with self.subTest(field=field), self.assertRaises(successor.RuntimeSuccessorError):
                successor.effective_gpu(self.config, bad)

    def test_without_sidecar_checks_existing_batch_reference(self):
        exact = successor._reference(self.old, self.old_bundle["runtime"])
        self.assertEqual(successor.effective_gpu(self.config, exact), self.config.section("gpu_readiness"))
        with self.assertRaises(successor.RuntimeSuccessorError):
            successor.effective_gpu(self.config, {**exact, "receipt_sha256": "f" * 64})

    def test_wrong_config_or_authorization_identity_fails_closed(self):
        value = self.stage()
        for mutation in (lambda x: x["config"].update(sha256="f" * 64),
                         lambda x: x.update(identity_sha256="f" * 64),
                         lambda x: x["policy"].update(launcher_checks_bypassed=True),
                         lambda x: x.update(extra="unreviewed")):
            bad = copy.deepcopy(value)
            mutation(bad)
            self.write(self.root / successor.SIDECAR_NAME, bad)
            with self.assertRaises(successor.RuntimeSuccessorError):
                successor.load_successor(self.config)

    def test_controls_must_be_separate_exact_paths_and_keys(self):
        cases = [{**self.new, "extra": True},
                 {**self.new, "local_launcher": self.old["local_launcher"]},
                 {**self.new, "runtime_admission": "https://example.invalid/runtime"},
                 {**self.new, "runtime_admission_sha256": "bad"}]
        for bad in cases:
            with self.subTest(bad=bad), self.assertRaises(successor.RuntimeSuccessorError):
                successor.build_successor(self.config, bad)

    def test_transition_rejects_unreviewed_semantic_drift(self):
        mutations = [
            lambda x: x["profile"]["execution_image"].update(sha256="f" * 64),
            lambda x: x["profile"]["launcher"].update(sha256="f" * 64),
            lambda x: x["profile"]["system_tools"]["bubblewrap"].update(sha256="f" * 64),
            lambda x: x["profile"]["host_abi"]["consumer_scan"].update(identity_sha256="f" * 64),
            lambda x: x["profile"]["host_abi"]["libraries"][0].update(sha256="f" * 64),
            lambda x: x["profile"]["host_abi"]["platform"].update(nvidia_driver_version="999.1"),
            lambda x: x["runtime"]["runtime"].update(package="different"),
            lambda x: x["runtime"]["production_profile"].update(identity_sha256="f" * 64),
            lambda x: x["runtime"]["root"].update(root_id="different"),
            lambda x: x["runtime"]["trusted_install"].update(owner_uid=123456),
        ]
        for ordinal, mutation in enumerate(mutations):
            changed = copy.deepcopy(self.new_bundle)
            mutation(changed)
            with self.subTest(ordinal=ordinal), self.assertRaises(successor.RuntimeSuccessorError):
                successor._transition(self.old_bundle, changed)

    def test_readiness_must_bind_exact_new_doctor_controls(self):
        readiness = copy.deepcopy(self.new_bundle["readiness"])
        readiness["host_abi_identity_sha256"] = self.old_bundle["profile"]["host_abi"]["identity_sha256"]
        readiness = seal(readiness, "readiness_id", "gpulocalready_")
        self.new["local_readiness_sha256"] = self.write(Path(self.new["local_readiness"]), readiness)
        with self.assertRaises(successor.RuntimeSuccessorError):
            successor.build_successor(self.config, self.new)

    def test_control_read_rejects_symlink_hardlink_fifo_mutable_and_oversize(self):
        source = self.root / "source"
        self.write(source, b"content")
        symlink = self.root / "symlink"
        symlink.symlink_to(source)
        with self.assertRaises(successor.RuntimeSuccessorError):
            successor._read_file(symlink)
        link = self.root / "hardlink"
        os.link(source, link)
        with self.assertRaises(successor.RuntimeSuccessorError):
            successor._read_file(link)
        fifo = self.root / "fifo"
        os.mkfifo(fifo, 0o400)
        with self.assertRaises(successor.RuntimeSuccessorError):
            successor._read_file(fifo)
        mutable = self.root / "mutable"
        self.write(mutable, b"abc", 0o600)
        with self.assertRaises(successor.RuntimeSuccessorError):
            successor._read_file(mutable)
        with self.assertRaises(successor.RuntimeSuccessorError):
            successor._read_file(Path(self.old["runtime_admission"]), maximum=1)

    def test_json_rejects_duplicates_nonfinite_and_noncanonical(self):
        path = self.root / "invalid.json"
        for body in (b'{"a":1,"a":2}\n', b'{"a":NaN}\n', b'{ "a": 1 }\n'):
            self.write(path, body)
            with self.assertRaises(successor.RuntimeSuccessorError):
                successor._read_json(path)

    def test_historical_wrapper_is_narrow_and_does_not_patch_global_admission(self):
        authorization = self.stage()
        original = mock.Mock(return_value=("original", "receipt"))
        original._gpu_successor_config = None
        asr = types.SimpleNamespace(runtime_admission_reference=original)
        private = types.SimpleNamespace()
        current_tool = mock.Mock(return_value=successor.NEW_BWRAP)
        private._tool_reference = current_tool

        def historical_load(path, sha, *, require_admitted, deep_image):
            self.assertFalse(require_admitted)
            self.assertFalse(deep_image)
            old_ref = {key: successor.OLD_BWRAP[key] for key in ("name", "path", "sha256")}
            self.assertEqual(private._tool_reference(old_ref, "tool"), successor.OLD_BWRAP)
            return self.old_bundle["runtime"]

        private.load_receipt = mock.Mock(side_effect=historical_load)
        self.assertTrue(successor.install_historical_replay(asr, self.config))
        wrapper = asr.runtime_admission_reference
        self.assertTrue(successor.install_historical_replay(asr, self.config))
        self.assertIs(asr.runtime_admission_reference, wrapper)
        with mock.patch.object(successor, "_isolated_admission", return_value=private):
            reference, receipt = wrapper(self.old["runtime_admission"], self.old["runtime_admission_sha256"], require_admitted=False)
        self.assertEqual(reference, authorization["old_runtime"])
        self.assertEqual(receipt, self.old_bundle["runtime"])
        current_tool.assert_called_once()
        self.assertEqual(wrapper(self.old["runtime_admission"], self.old["runtime_admission_sha256"], require_admitted=True), ("original", "receipt"))
        self.assertEqual(wrapper(self.new["runtime_admission"], self.new["runtime_admission_sha256"], require_admitted=False), ("original", "receipt"))
        self.assertEqual(original.call_count, 2)

    def test_private_admission_functions_have_isolated_globals(self):
        shared = successor._helper("admit_runtime_v2.py")
        old_tool = shared._tool_reference
        private = successor._isolated_admission()
        private._tool_reference = mock.Mock()
        self.assertIs(shared._tool_reference, old_tool)
        self.assertIs(private.validate_receipt.__globals__, private.__dict__)
        self.assertIs(private.load_receipt.__globals__, private.__dict__)
        self.assertIs(shared.validate_receipt.__globals__, shared.__dict__)

    def test_current_replay_is_shallow_but_rejects_platform_or_receipt_change(self):
        admission = types.SimpleNamespace(load_receipt=mock.Mock(return_value=self.new_bundle["runtime"]))
        launcher = types.SimpleNamespace(observe_host_abi_platform=mock.Mock(
            return_value=self.new_bundle["profile"]["host_abi"]["platform"]))
        helper = lambda filename: admission if filename == "admit_runtime_v2.py" else launcher
        self.replay.stop()
        with mock.patch.object(successor, "_helper", side_effect=helper):
            successor._replay_current(self.new, self.new_bundle)
            admission.load_receipt.assert_called_once_with(self.new["runtime_admission"],
                self.new["runtime_admission_sha256"], require_admitted=False, deep_image=False)
            launcher.observe_host_abi_platform.return_value = self.old_bundle["profile"]["host_abi"]["platform"]
            with self.assertRaises(successor.RuntimeSuccessorError):
                successor._replay_current(self.new, self.new_bundle)
            admission.load_receipt.return_value = self.old_bundle["runtime"]
            with self.assertRaises(successor.RuntimeSuccessorError):
                successor._replay_current(self.new, self.new_bundle)

    def test_changed_control_or_in_memory_config_blocks_successor(self):
        self.stage()
        self.write(Path(self.new["local_launcher"]), b"changed-launcher", 0o500)
        with self.assertRaises(successor.RuntimeSuccessorError):
            successor.effective_gpu(self.config)
        self.config.document["gpu_readiness"]["unmodified_option"]["private"] = False
        with self.assertRaisesRegex(successor.RuntimeSuccessorError, "configuration snapshot"):
            successor.build_successor(self.config, self.new)


if __name__ == "__main__":
    unittest.main()
