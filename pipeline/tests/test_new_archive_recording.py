import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline import new_archive_recording as job


class MediaBindingTests(unittest.TestCase):
    def test_media_uses_media_bound_not_json_bound(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'video.mp4'
            path.write_bytes(b'media larger than test JSON bound')
            with patch.object(job.r, 'MAX_JSON', 4):
                result=job.media_binding(path,path.stat().st_size)
            self.assertEqual(result['sha256'],hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(result['byte_count'],path.stat().st_size)

    def test_rejects_wrong_size_and_excessive_bound(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'video.mp4'
            path.write_bytes(b'media')
            for size in (0,4,6,job.MAX_MEDIA_BYTES+1):
                with self.subTest(size=size), self.assertRaises(ValueError):
                    job.media_binding(path,size)
