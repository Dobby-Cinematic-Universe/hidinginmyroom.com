from __future__ import annotations

import copy
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[2]
TEST_WORK_ROOT = ROOT / "pipeline/.test-work"


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PROFILE = load_module(
    "himr_gpu_production_profile_v2_test_module",
    ROOT / "pipeline/gpu/production_profile_v2.py",
)


class GPUProductionProfileV2Tests(unittest.TestCase):
    def test_default_profile_is_canonical_and_binds_every_limit(self) -> None:
        value = PROFILE.default_profile()
        self.assertEqual(PROFILE.validate_profile(value), value)
        self.assertEqual(value["decoding"]["beam_size"], 5)
        self.assertEqual(value["item_limits"]["maximum_audio_seconds"], 420.0)
        self.assertEqual(value["batch_limits"]["maximum_items"], 32)
        self.assertEqual(value["batch_limits"]["preferred_total_audio_ms"], 7_200_000)
        self.assertEqual(value["telemetry"]["maximum_temperature_c"], 80)
        self.assertFalse(value["decoding"]["neural_batching"])
        self.assertEqual(value["decoding"]["num_workers"], 2)
        self.assertEqual(value["batch_limits"]["inference_concurrency"], 2)
        self.assertEqual(value["scheduler"]["prefetch_workers"], 0)
        self.assertEqual(value["scheduler"]["prefetch_depth"], 0)

    def test_every_resource_limit_changes_profile_identity(self) -> None:
        original = PROFILE.default_profile()
        fields = (
            ("item_limits", "maximum_audio_bytes"),
            ("item_limits", "maximum_audio_seconds"),
            ("item_limits", "maximum_wall_seconds"),
            ("item_limits", "maximum_result_bytes"),
            ("item_limits", "maximum_segments"),
            ("item_limits", "maximum_words"),
            ("batch_limits", "maximum_items"),
            ("batch_limits", "maximum_total_audio_ms"),
            ("batch_limits", "maximum_wall_seconds"),
            ("telemetry", "maximum_process_vram_bytes"),
            ("telemetry", "minimum_free_vram_bytes"),
            ("telemetry", "maximum_temperature_c"),
        )
        for section, field in fields:
            with self.subTest(section=section, field=field):
                changed = copy.deepcopy(original)
                changed.pop("identity_sha256")
                changed.pop("profile_id")
                changed[section][field] -= 1
                rebuilt = PROFILE.make_profile(changed)
                self.assertNotEqual(
                    rebuilt["identity_sha256"], original["identity_sha256"]
                )

    def test_decoding_change_cannot_claim_the_control_profile(self) -> None:
        value = PROFILE.default_profile()
        value.pop("identity_sha256")
        value.pop("profile_id")
        value["decoding"]["beam_size"] = 1
        with self.assertRaisesRegex(PROFILE.ProfileError, "exact v2 control"):
            PROFILE.make_profile(value)

    def test_unimplemented_prefetch_cannot_claim_the_v2_profile(self) -> None:
        value = PROFILE.default_profile()
        value.pop("identity_sha256")
        value.pop("profile_id")
        value["scheduler"]["prefetch_workers"] = 1
        value["scheduler"]["prefetch_depth"] = 1
        with self.assertRaisesRegex(PROFILE.ProfileError, "unimplemented prefetch"):
            PROFILE.make_profile(value)

    def test_unknown_field_and_forged_identity_fail(self) -> None:
        value = PROFILE.default_profile()
        value["extra"] = True
        with self.assertRaises(PROFILE.ProfileError):
            PROFILE.validate_profile(value)
        value = PROFILE.default_profile()
        value["identity_sha256"] = "0" * 64
        with self.assertRaisesRegex(PROFILE.ProfileError, "identity"):
            PROFILE.validate_profile(value)

    def test_materializer_is_private_canonical_and_no_replace(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        with tempfile.TemporaryDirectory(dir=TEST_WORK_ROOT) as temporary:
            parent = Path(temporary)
            parent.chmod(0o700)
            output = parent / "profile.json"
            PROFILE._write_exclusive(output, PROFILE.default_profile())
            self.assertEqual(output.stat().st_mode & 0o777, 0o400)
            self.assertEqual(
                output.read_bytes(), PROFILE.canonical_bytes(PROFILE.default_profile())
            )
            with self.assertRaises(PROFILE.ProfileError):
                PROFILE._write_exclusive(output, PROFILE.default_profile())


if __name__ == "__main__":
    unittest.main()
