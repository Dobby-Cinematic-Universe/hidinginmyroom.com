"""Execution releases authenticate code and exact plans without API calls."""
from __future__ import annotations

from copy import deepcopy
from contextvars import Context
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription as cloud
from pipeline import cloud_transcription_release as release
from pipeline import cloud_transcription_summary as summaries
from pipeline import transcript_summary as io


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.counter = 0
        self.addCleanup(patch.stopall)
        patch("socket.socket", side_effect=AssertionError("network forbidden")).start()
        files = {}
        for name in ("cloud_transcription.py", "cloud_transcription_summary.py", "transcript_summary.py"):
            path = self.root / "old-code" / name
            path.parent.mkdir(mode=0o700, exist_ok=True)
            path.write_bytes(("# Original " + name + "\n").encode())
            path.chmod(0o400)
            files[name] = io.binding(path)
        old_union = {name: ref["sha256"] for name, ref in files.items()}
        self.old_cloud = {name: digest for name, digest in old_union.items()
                          if name != "cloud_transcription_summary.py"}
        self.old_summary = dict(old_union)
        self.plan = {"kind": cloud.KIND, "schema_version": 1, "state_root": str(self.root / "cloud"),
                     "policy": cloud.POLICY, "rates_microusd_hour": cloud.RATES,
                     "implementation": self.old_cloud}
        self.plan_ref = self.save(self.root / "cloud" / "plan.json", self.plan)
        self.manifest = {"kind": summaries.KIND, "schema_version": 1,
                         "state_root": str(self.root / "summaries"), "policy": summaries.POLICY,
                         "cloud_plan": self.plan_ref, "max_total_budget_microusd": 150_000_000,
                         "implementation": self.old_summary}
        self.summary_ref = self.save(self.root / "summaries" / "manifest.json", self.manifest)
        self.snapshot = {"kind": release.SNAPSHOT_KIND, "schema_version": 1,
                         "cloud_plan": self.plan_ref, "summary_manifest": self.summary_ref,
                         "implementation": old_union, "files": files,
                         "new_paid_requests": 0, "source_mutation": False}
        self.snapshot_ref = self.save(self.root / "proof" / "code.json", self.snapshot)
        self.policy_ref = self.save(self.root / "policy" / "titles.json", {"policy": "frozen titles"})
        self.current = {"cloud": {**self.old_cloud, "cloud_transcription.py": "a" * 64,
                                   "cloud_transcription_release.py": "b" * 64,
                                   "cloud_transcription_title_policy.py": "c" * 64},
                        "parallel_sha256": "d" * 64, "release_sha256": "b" * 64,
                        "package_init_sha256": None}
        self.current["summary"] = {**self.current["cloud"], "cloud_transcription_summary.py": "e" * 64}
        self.current_patch = patch.object(release, "_current", side_effect=lambda: deepcopy(self.current))
        self.current_mock = self.current_patch.start()
        self.policy_patch = patch.object(release, "_policy", side_effect=io.read)
        self.policy_patch.start()

    @staticmethod
    def save(path, value):
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        return io.put(path, value)

    @staticmethod
    def rewrite(path, value):
        path.chmod(0o600)
        path.write_bytes(io.canonical(value))
        path.chmod(0o400)
        return io.binding(path)

    def prepare(self, **options):
        self.counter += 1
        kwargs = {"old_code_ref": self.snapshot_ref, "title_policy_ref": self.policy_ref,
                  "output": self.root / ("release-" + str(self.counter)) / "release.json"}
        kwargs.update(options)
        return release.prepare_release(self.plan_ref, self.summary_ref, **kwargs)

    def prepared_ref(self):
        return self.prepare()["execution_release"]

    def resealed(self, ref, change):
        value = io.read(ref)
        change(value)
        self.counter += 1
        return self.save(self.root / ("explicit-other-" + str(self.counter)) / "release.json", value)

    def test_requires_explicit_activation_and_exact_original_refs(self):
        reference = self.prepared_ref()
        self.assertFalse(release.permits_cloud(self.plan_ref, self.old_cloud, self.current["cloud"]))
        self.assertIsNone(release.active_title_policy(self.plan_ref))
        with release.activate(reference):
            self.assertTrue(release.permits_cloud(self.plan_ref, self.old_cloud, self.current["cloud"]))
            self.assertTrue(release.permits_summary(self.summary_ref, self.old_summary, self.current["summary"]))
            self.assertEqual(release.active_title_policy(self.plan_ref), self.policy_ref)
            for different in ({**self.plan_ref, "sha256": "0" * 64},
                              {**self.plan_ref, "path": str(self.root / "other-plan.json")}):
                self.assertFalse(release.permits_cloud(different, self.old_cloud, self.current["cloud"]))
                self.assertIsNone(release.active_title_policy(different))
            self.assertFalse(release.permits_summary(self.plan_ref, self.old_summary, self.current["summary"]))
        self.assertFalse(release.permits_cloud(self.plan_ref, self.old_cloud, self.current["cloud"]))

    def test_activation_resets_on_exception_and_does_not_allow_nesting(self):
        reference = self.prepared_ref()
        with self.assertRaisesRegex(ValueError, "synthetic"):
            with release.activate(reference):
                with self.assertRaises(release.ReleaseError):
                    with release.activate(reference):
                        self.fail("nested activation succeeded")
                raise ValueError("synthetic")
        self.assertIsNone(release.active_title_policy(self.plan_ref))

    def test_ref_return_values_cannot_mutate_authority(self):
        reference = self.prepared_ref()
        with release.activate(reference) as returned:
            returned["sha256"] = "0" * 64
            policy = release.active_title_policy(self.plan_ref)
            policy["sha256"] = "0" * 64
            self.assertTrue(release.permits_cloud(self.plan_ref, self.old_cloud, self.current["cloud"]))
            self.assertEqual(release.active_title_policy(self.plan_ref), self.policy_ref)

    def test_explicit_activation_does_not_leak_into_an_independent_context(self):
        reference = self.prepared_ref()
        with release.activate(reference):
            self.assertFalse(Context().run(release.permits_cloud,
                                           self.plan_ref, self.old_cloud, self.current["cloud"]))
            self.assertIsNone(Context().run(release.active_title_policy, self.plan_ref))

    def test_real_frozen_title_policy_is_checked_not_just_its_reference_shape(self):
        from pipeline import cloud_transcription_title_policy as titles
        self.policy_patch.stop()
        self.policy_ref = self.save(self.root / "real-policy" / "titles.json", titles.prepare_policy())
        reference = self.prepared_ref()
        with release.activate(reference):
            self.assertEqual(release.active_title_policy(self.plan_ref), self.policy_ref)
        changed = titles.prepare_policy()
        changed["schema_version"] = 99
        invalid = self.save(self.root / "invalid-policy" / "titles.json", changed)
        with self.assertRaises(RuntimeError):
            self.prepare(title_policy_ref=invalid)

    def test_current_or_original_map_mismatch_is_not_permitted(self):
        reference = self.prepared_ref()
        with release.activate(reference):
            self.assertFalse(release.permits_cloud(self.plan_ref, {}, self.current["cloud"]))
            self.assertFalse(release.permits_cloud(self.plan_ref, self.old_cloud, self.old_cloud))
            self.assertFalse(release.permits_summary(self.summary_ref, self.old_summary, {}))

    def test_release_checksum_change_fails_closed(self):
        reference = self.prepared_ref()
        self.rewrite(Path(reference["path"]), {**io.read(reference), "summary_budget_microusd": 1})
        with self.assertRaises(RuntimeError):
            with release.activate(reference):
                self.fail("changed release activated")
        self.assertIsNone(release.active_title_policy(self.plan_ref))

    def test_release_change_while_active_fails_closed(self):
        reference = self.prepared_ref()
        with release.activate(reference):
            self.rewrite(Path(reference["path"]), {**io.read(reference), "summary_budget_microusd": 1})
            with self.assertRaises(RuntimeError):
                release.permits_cloud(self.plan_ref, self.old_cloud, self.current["cloud"])

    def test_changed_current_code_or_parallel_launcher_or_release_rejected(self):
        reference = self.prepared_ref()
        for key in ("parallel_sha256", "release_sha256", "package_init_sha256"):
            with self.subTest(key=key):
                before = self.current[key]
                self.current[key] = "f" * 64
                with self.assertRaises(release.ReleaseError):
                    release.load(reference)
                self.current[key] = before
        self.current["cloud"]["cloud_transcription.py"] = "f" * 64
        with self.assertRaises(release.ReleaseError):
            release.load(reference)

    def test_package_loader_is_bound_when_present_and_none_for_namespace_package(self):
        self.current_patch.stop()
        folder = self.root / "private-package" / "pipeline"
        folder.mkdir(mode=0o700, parents=True)
        filename = folder / "cloud_transcription_release.py"
        filename.write_bytes(b"# release source fixture\n")
        (folder / "cloud_transcription_parallel.py").write_bytes(b"# parallel source fixture\n")
        with patch.object(release, "__file__", str(filename)), \
                patch.object(cloud, "implementation", return_value=self.current["cloud"]), \
                patch.object(summaries, "implementation", return_value=self.current["summary"]):
            self.assertIsNone(release._current()["package_init_sha256"])
            loader = folder / "__init__.py"
            loader.write_bytes(b"# fixed package loader\n")
            before = release._current()["package_init_sha256"]
            self.assertEqual(before, hashlib.sha256(loader.read_bytes()).hexdigest())
            loader.write_bytes(b"# changed package loader\n")
            self.assertNotEqual(before, release._current()["package_init_sha256"])

    def test_changed_original_plan_or_policy_bytes_rejected(self):
        reference = self.prepared_ref()
        for original in (self.plan_ref, self.summary_ref, self.policy_ref):
            with self.subTest(path=original["path"]):
                path = Path(original["path"])
                before = io.read(original)
                self.rewrite(path, {**before, "tampered": True})
                with self.assertRaises(RuntimeError):
                    release.load(reference)
                self.rewrite(path, before)

    def test_changed_old_source_snapshot_rejected_even_if_same_length(self):
        reference = self.prepared_ref()
        path = Path(self.snapshot["files"]["cloud_transcription.py"]["path"])
        raw = path.read_bytes()
        path.chmod(0o600)
        path.write_bytes(b"!" + raw[1:])
        path.chmod(0o400)
        with self.assertRaises(RuntimeError):
            release.load(reference)

    def test_snapshot_requires_exact_union_and_original_refs(self):
        for key in ("cloud_plan", "summary_manifest"):
            changed = deepcopy(self.snapshot)
            changed[key]["sha256"] = "0" * 64
            self.counter += 1
            ref = self.save(self.root / ("bad-proof-" + str(self.counter)) / "code.json", changed)
            with self.assertRaises(release.ReleaseError):
                self.prepare(old_code_ref=ref)
        for operation in ("missing", "extra", "wrong_digest"):
            changed = deepcopy(self.snapshot)
            if operation == "missing":
                changed["files"].pop("transcript_summary.py")
            elif operation == "extra":
                changed["files"]["unrelated.py"] = changed["files"]["transcript_summary.py"]
            else:
                changed["files"]["transcript_summary.py"]["sha256"] = "0" * 64
            self.counter += 1
            ref = self.save(self.root / ("bad-proof-" + str(self.counter)) / "code.json", changed)
            with self.assertRaises(release.ReleaseError):
                self.prepare(old_code_ref=ref)

    def test_unrelated_code_change_or_new_module_is_not_approved(self):
        self.current["cloud"]["transcript_summary.py"] = "f" * 64
        self.current["summary"]["transcript_summary.py"] = "f" * 64
        with self.assertRaisesRegex(release.ReleaseError, "unrelated"):
            self.prepare()
        self.current["cloud"]["transcript_summary.py"] = self.old_cloud["transcript_summary.py"]
        self.current["summary"]["transcript_summary.py"] = self.old_cloud["transcript_summary.py"]
        self.current["cloud"]["unrelated.py"] = "f" * 64
        with self.assertRaisesRegex(release.ReleaseError, "source set"):
            self.prepare()

    def test_summary_must_point_to_the_exact_cloud_plan(self):
        changed = deepcopy(self.manifest)
        changed["cloud_plan"]["sha256"] = "0" * 64
        self.summary_ref = self.rewrite(Path(self.summary_ref["path"]), changed)
        with self.assertRaises(release.ReleaseError):
            self.prepare()

    def test_release_cannot_change_budgets_original_maps_or_scope(self):
        original = self.prepared_ref()
        for change in (lambda value: value.update(summary_budget_microusd=300_000_000),
                       lambda value: value["old_cloud_implementation"].update({"cloud_transcription.py": "f" * 64}),
                       lambda value: value["semantics"].update(no_automatic_retranscription=False)):
            with self.subTest(change=change):
                reference = self.resealed(original, change)
                with self.assertRaises(release.ReleaseError):
                    release.load(reference)

    def test_output_fresh_private_and_original_documents_unmodified(self):
        before = {ref["path"]: Path(ref["path"]).read_bytes()
                  for ref in (self.plan_ref, self.summary_ref, self.snapshot_ref, self.policy_ref)}
        result = self.prepare()
        self.assertEqual(result["new_paid_requests"], 0)
        self.assertFalse(result["original_artifacts_modified"])
        self.assertEqual(Path(result["execution_release"]["path"]).stat().st_mode & 0o777, 0o400)
        for path, raw in before.items():
            self.assertEqual(Path(path).read_bytes(), raw)
        with self.assertRaises(RuntimeError):
            self.prepare(output=result["execution_release"]["path"])
        for path in (self.root / "cloud" / "release.json", self.root / "summaries" / "release.json"):
            with self.assertRaises(RuntimeError):
                self.prepare(output=path)

    def test_snapshot_symlink_is_not_accepted(self):
        original = self.prepared_ref()
        path = Path(self.snapshot["files"]["cloud_transcription.py"]["path"])
        moved = path.with_suffix(".retained")
        path.rename(moved)
        path.symlink_to(moved)
        with self.assertRaises((OSError, RuntimeError)):
            release.load(original)

    def test_pre_edit_snapshot_copies_exact_inert_bytes_and_publishes_union(self):
        self.current["cloud"] = deepcopy(self.old_cloud)
        self.current["summary"] = deepcopy(self.old_summary)
        output = self.root / "new-snapshot"
        with patch.object(release, "__file__", str(self.root / "old-code" / "cloud_transcription_release.py")):
            reference = release.snapshot_code(self.plan_ref, self.summary_ref, output)
        self.assertEqual(reference["path"], str(output / "old-code.json"))
        snapshot = io.read(reference)
        self.assertEqual(snapshot["implementation"], self.old_summary)
        self.assertEqual(set(snapshot["files"]), set(self.old_summary))
        for name, proof in snapshot["files"].items():
            self.assertEqual(proof["path"], str(output / "old-code" / name))
            self.assertEqual(proof["sha256"], self.old_summary[name])
            self.assertEqual(io.read_bytes(proof), io.read_bytes(self.snapshot["files"][name]))
        self.assertEqual(snapshot["new_paid_requests"], 0)
        with self.assertRaises(release.ReleaseError):
            release.snapshot_code(self.plan_ref, self.summary_ref, output)

    def test_pre_edit_snapshot_rejects_changed_code_before_or_during_copy(self):
        with self.assertRaises(release.ReleaseError):
            release.snapshot_code(self.plan_ref, self.summary_ref, self.root / "already-changed")
        self.current["cloud"] = deepcopy(self.old_cloud)
        self.current["summary"] = deepcopy(self.old_summary)
        changed = deepcopy(self.current)
        changed["parallel_sha256"] = "f" * 64
        self.current_mock.side_effect = [deepcopy(self.current), changed]
        output = self.root / "changed-mid-copy"
        with patch.object(release, "__file__", str(self.root / "old-code" / "cloud_transcription_release.py")):
            with self.assertRaises(release.ReleaseError):
                release.snapshot_code(self.plan_ref, self.summary_ref, output)
        self.assertFalse((output / "old-code.json").exists())


if __name__ == "__main__":
    unittest.main()
