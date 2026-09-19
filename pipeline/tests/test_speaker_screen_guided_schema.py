"""Structural example validation; semantic admission is tested separately."""
import copy
import json
from pathlib import Path
import unittest

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
NAMES = ("speaker-screen-guidance", "speaker-screen-timed-cues",
         "speaker-screen-reviewed-event", "speaker-screen-guided-request")


class GuidedSchemaTests(unittest.TestCase):
    def test_all_examples_match_strict_schemas(self):
        for name in NAMES:
            with self.subTest(name=name):
                schema = json.loads((ROOT / "schemas" / (name + ".schema.json")).read_text())
                example = json.loads((ROOT / "examples" / (name + ".example.json")).read_text())
                Draft202012Validator.check_schema(schema)
                validator = Draft202012Validator(schema, format_checker=FormatChecker())
                validator.validate(example)
                mutated = copy.deepcopy(example)
                mutated["speaker_label"] = "multiple_speakers"
                self.assertTrue(list(validator.iter_errors(mutated)))

    def test_guided_bounds_do_not_allow_unlimited_sampling(self):
        name = "speaker-screen-guided-request"
        schema = json.loads((ROOT / "schemas" / (name + ".schema.json")).read_text())
        example = json.loads((ROOT / "examples" / (name + ".example.json")).read_text())
        validator = Draft202012Validator(schema)
        for maximum in (-1, 65, True):
            with self.subTest(maximum=maximum):
                mutated = {**example, "max_target_windows": maximum}
                self.assertTrue(list(validator.iter_errors(mutated)))


if __name__ == "__main__":
    unittest.main()
