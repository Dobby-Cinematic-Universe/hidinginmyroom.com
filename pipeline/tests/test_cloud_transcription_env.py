import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline import cloud_transcription_env as env


class EnvTests(unittest.TestCase):
    def test_parser_only_requested_keys_and_no_shell_evaluation(self):
        self.assertEqual(env.parse_keys(b'GEMINI_API_KEY="unterminated\nexport ASSEMBLYAI_API_KEY="literal$HOME`cmd`"\nREVAI_ACCESS_TOKEN=x # comment\n'),
                         {'ASSEMBLYAI_API_KEY': 'literal$HOME`cmd`', 'REVAI_ACCESS_TOKEN': 'x'})

    def test_duplicate_rejected_without_value_leak(self):
        with self.assertRaises(env.EnvError) as caught:
            env.parse_keys(b'ASSEMBLYAI_API_KEY=secret\nASSEMBLYAI_API_KEY=other\n')
        self.assertNotIn('secret', str(caught.exception))

    def test_private_file_and_environment_precedence(self):
        with tempfile.TemporaryDirectory() as root, patch.dict(os.environ, {}, clear=True):
            path = Path(root) / '.env'
            path.write_text('ASSEMBLYAI_API_KEY=fromfile\nREVAI_API_KEY=alias\n')
            path.chmod(0o600)
            self.assertEqual(env.api_key('assemblyai', env_file=path), 'fromfile')
            self.assertEqual(env.api_key('revai', env_file=path), 'alias')
            with patch.dict(os.environ, {'ASSEMBLYAI_API_KEY': 'explicit'}):
                self.assertEqual(env.api_key('assemblyai', env_file='/nonexistent'), 'explicit')
            path.chmod(0o644)
            with self.assertRaises(env.EnvError):
                env.api_key('assemblyai', env_file=path)

    def test_alias_conflict_and_header_injection_rejected(self):
        for values in ({'REVAI_API_KEY': 'a', 'REVAI_ACCESS_TOKEN': 'b'},
                       {'REVAI_API_KEY': 'secret\nheader'}, {'REVAI_API_KEY': ''}):
            with patch.dict(os.environ, values, clear=True), self.assertRaises(env.EnvError):
                env.api_key('revai')

    def test_equal_aliases_and_missing(self):
        with patch.dict(os.environ, {'REVAI_API_KEY': 'same', 'REVAI_ACCESS_TOKEN': 'same'}, clear=True):
            self.assertEqual(env.api_key('revai'), 'same')
        with patch.dict(os.environ, {}, clear=True), patch.object(env, 'DEFAULT_PATH', Path('/does/not/exist')):
            self.assertIsNone(env.api_key('assemblyai'))

    def test_symlink_and_explicit_missing_rejected(self):
        with tempfile.TemporaryDirectory() as root, patch.dict(os.environ, {}, clear=True):
            path = Path(root) / 'actual'
            path.write_text('ASSEMBLYAI_API_KEY=value')
            path.chmod(0o600)
            link = Path(root) / 'link'
            link.symlink_to(path)
            for candidate in (link, Path(root) / 'missing'):
                with self.assertRaises(env.EnvError):
                    env.api_key('assemblyai', env_file=candidate)


if __name__ == '__main__':
    unittest.main()
