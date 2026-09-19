"""Metadata-only example/schema checks; no media, model, or network access."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import unittest

from jsonschema import Draft202012Validator

from pipeline import speaker_screen_core as core


PIPELINE = Path(__file__).resolve().parents[1]


class SpeakerScreenSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads((PIPELINE / "schemas/speaker-screen-work-order.schema.json").read_text())
        cls.example = json.loads((PIPELINE / "examples/speaker-screen-work-order.example.json").read_text())
        cls.validator = Draft202012Validator(cls.schema)

    def changed(self, fields, value):
        result = copy.deepcopy(self.example)
        target = result
        for field in fields[:-1]:
            target = target[field]
        target[fields[-1]] = value
        return result

    def test_schema_is_valid_and_example_uses_core_defaults(self):
        Draft202012Validator.check_schema(self.schema)
        self.validator.validate(self.example)
        self.assertEqual(self.example["policy"], {})
        self.assertEqual(core.validate_policy({**core.DEFAULT_POLICY, **self.example["policy"]}), core.DEFAULT_POLICY)
        declared = self.schema["properties"]["policy"]["properties"]
        self.assertEqual({key: item["default"] for key, item in declared.items()}, core.DEFAULT_POLICY)

    def test_example_is_inert_with_explicit_placeholder_paths_and_hashes(self):
        self.assertTrue(self.example["output_root"].startswith("/ABSOLUTE/"))
        bindings = [self.example["recording"], self.example["ffmpeg"], self.example["models"]["silero_vad"], self.example["models"]["ecapa_embedding"]]
        for binding in bindings:
            self.assertTrue(binding["path"].startswith("/ABSOLUTE/"))
            self.assertEqual(binding["sha256"], "0" * 64)

    def test_required_fields_and_unrecognized_authority_are_rejected(self):
        for field in self.schema["required"]:
            with self.subTest(field=field):
                value = copy.deepcopy(self.example)
                value.pop(field)
                self.assertFalse(self.validator.is_valid(value))
        for fields in (("controller",), ("recording", "url"), ("models", "download"),
                       ("policy", "identify_person"), ("resources", "gpu"), ("ffmpeg", "extra_args")):
            with self.subTest(fields=fields):
                self.assertFalse(self.validator.is_valid(self.changed(fields, True)))

    def test_absolute_paths_reject_relative_root_dot_segments_and_controls(self):
        fields_list = (("recording", "path"), ("ffmpeg", "path"), ("models", "silero_vad", "path"),
                       ("models", "ecapa_embedding", "path"), ("output_root",))
        invalid = ("relative/file", "/", "/tmp//file", "/tmp/./file", "/tmp/../file", "/./file",
                   "/../file", "/tmp/.", "/tmp/..", "/tmp/file/", "/tmp/file\nname", "/tmp/file\n", "/tmp/file\x00name", "/tmp/file\x7fname", "https://example.invalid/file")
        for fields in fields_list:
            for path in invalid:
                with self.subTest(fields=fields, path=repr(path)):
                    self.assertFalse(self.validator.is_valid(self.changed(fields, path)))
        self.validator.validate(self.changed(("recording", "path"), "/tmp/space in name/media.audio"))

    def test_digest_and_identifier_shapes_are_strict(self):
        for fields in (("recording", "sha256"), ("ffmpeg", "sha256"), ("models", "silero_vad", "sha256"), ("models", "ecapa_embedding", "sha256")):
            for value in ("a" * 63, "A" * 64, "g" * 64, "a" * 64 + "\n", None, True):
                with self.subTest(fields=fields, value=value):
                    self.assertFalse(self.validator.is_valid(self.changed(fields, value)))
        for value in ("", "../identity", "a" * 257, "name with spaces", "name\n", False):
            self.assertFalse(self.validator.is_valid(self.changed(("recording", "media_id"), value)))

    def test_recording_resource_and_policy_bounds_reject_booleans(self):
        bounds = {
            ("recording", "byte_count"): (1, 68719476736),
            ("recording", "duration_ms"): (1, 86400000),
            ("resources", "threads"): (1, 2),
            ("resources", "window_timeout_seconds"): (10, 600),
            ("resources", "max_run_seconds"): (10, 3600),
            ("resources", "max_windows_per_run"): (1, 512),
            ("policy", "probe_ms"): (1, 10000),
            ("policy", "stride_ms"): (1, 604800000),
            ("policy", "max_windows"): (1, 512),
            ("policy", "min_speech_ms"): (2000, 5000),
            ("policy", "min_support"): (2, 8),
        }
        for fields, (minimum, maximum) in bounds.items():
            for value in (minimum, maximum):
                self.validator.validate(self.changed(fields, value))
            for value in (minimum - 1, maximum + 1, True, False, None, "1", 1.5):
                with self.subTest(fields=fields, value=value):
                    self.assertFalse(self.validator.is_valid(self.changed(fields, value)))

    def test_partial_policy_is_allowed_but_runtime_owns_cross_field_semantics(self):
        value = self.changed(("policy",), {"min_support": 3, "stride_ms": 120000})
        self.validator.validate(value)
        merged = core.validate_policy({**core.DEFAULT_POLICY, **value["policy"]})
        self.assertEqual(merged["min_support"], 3)
        self.assertEqual(merged["probe_ms"], 10000)
        for override in ({"stride_ms": 1}, {"distinct_cosine_max": 0.9}, {"match_cosine_min": 0.6}):
            value = self.changed(("policy",), override)
            self.validator.validate(value)
            with self.assertRaises(core.ScreenError):
                core.validate_policy({**core.DEFAULT_POLICY, **override})

    def test_thresholds_versions_and_verification_mode_are_closed(self):
        for value in (False, True):
            self.validator.validate(self.changed(("resources", "early_stop_on_positive"), value))
        for value in (0, 1, "true", None):
            self.assertFalse(self.validator.is_valid(self.changed(("resources", "early_stop_on_positive"), value)))
        for key in ("match_cosine_min", "distinct_cosine_max"):
            for value in (True, None, "0.85", -1.1, 1.1):
                self.assertFalse(self.validator.is_valid(self.changed(("policy", key), value)))
        for fields in (("schema_version",), ("models", "schema_version")):
            for value in (True, False, 0, 2, "1"):
                self.assertFalse(self.validator.is_valid(self.changed(fields, value)))
        for value in ("metadata_witness", "sha256"):
            self.validator.validate(self.changed(("source_verification",), value))
        for value in ("none", "trust", "", None):
            self.assertFalse(self.validator.is_valid(self.changed(("source_verification",), value)))


if __name__ == "__main__":
    unittest.main()
