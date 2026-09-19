import unittest
from pipeline.cloud_audio_offset_repair import silence_samples


class OffsetTests(unittest.TestCase):
    def test_fractional_offset(self):
        self.assertEqual(silence_samples('2.007007'),32112)
        self.assertEqual(silence_samples('0.00004'),1)

    def test_bounds(self):
        for value in ['0','-1','11','NaN','Infinity']:
            with self.assertRaises(ValueError):silence_samples(value)
