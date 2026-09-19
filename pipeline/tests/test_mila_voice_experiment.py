import unittest
from pipeline.mila_voice_experiment import longest_speech


class SpeechCropTests(unittest.TestCase):
    def test_longest_contiguous_run_without_stitching(self):
        p=[.9]*70+[.1]*10+[.9]*75
        self.assertEqual(longest_speech(p,155*512),(80*512,155*512))

    def test_short_runs_not_combined(self):
        self.assertIsNone(longest_speech(([.9]*30+[.1])*4,124*512))

    def test_no_silence_or_nonfinite_crop(self):
        self.assertIsNone(longest_speech([0.0]*100,51200))
        with self.assertRaises(ValueError):longest_speech([float('nan')],512)

    def test_padding_never_extends_past_audio(self):
        self.assertEqual(longest_speech([1.0]*100,50000),(0,50000))


if __name__=='__main__':unittest.main()
