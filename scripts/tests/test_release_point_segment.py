import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('repair_point',Path(__file__).parents[1]/'repair_release_point_segment.py')
repair=importlib.util.module_from_spec(spec);spec.loader.exec_module(repair)

class PointRepairTests(unittest.TestCase):
    def test_retains_original_and_text(self):
        original={'segments':[{'start_ms':10,'end_ms':10,'text':'Keep all words.'},{'start_ms':20,'end_ms':30,'text':'Next.'}]}
        result=repair.normalize_point(original,0)
        self.assertEqual(original['segments'][0]['end_ms'],10)
        self.assertEqual(result['segments'][0],{'start_ms':10,'end_ms':11,'text':'Keep all words.'})
        self.assertEqual(result['segments'][1],original['segments'][1])

    def test_rejects_nonpoint_or_overlap(self):
        for segments in ([{'start_ms':10,'end_ms':9}], [{'start_ms':10,'end_ms':10},{'start_ms':10,'end_ms':20}]):
            with self.assertRaises(ValueError):repair.normalize_point({'segments':segments},0)
