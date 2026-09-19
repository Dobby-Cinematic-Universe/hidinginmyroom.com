"""Fresh campaigns bind the reviewed replacement; historical bytes stay stable."""

import copy
from pathlib import Path
import unittest

from autonomous_controller.config import (
    ConfigError, REPLACEMENT_SAFETY, SAFETY, build_config, normalize_config,
)
from autonomous_controller.tests.test_controller import config_core


class ReplacementDiskConfigTests(unittest.TestCase):
    def test_historical_policy_is_unchanged(self):
        document = build_config(config_core(Path('/tmp/himr-config-test')))
        self.assertEqual(normalize_config(document), document)
        self.assertEqual(document['safety'], SAFETY)

    def test_fresh_config_seals_exact_replacement_uuid(self):
        core = config_core(Path('/tmp/himr-config-test'))
        old = build_config(core)
        core['safety'] = dict(REPLACEMENT_SAFETY)
        current = build_config(core)
        self.assertEqual(normalize_config(current), current)
        self.assertEqual(current['safety'], REPLACEMENT_SAFETY)
        self.assertNotEqual(current['config_id'], old['config_id'])

    def test_no_other_policy_relaxation(self):
        for key, value in [('cold_mount_uuid', 'unreviewed'),
                           ('publication_authority', 'automatic'),
                           ('cold_mount_filesystem', 'ext4'),
                           ('credentials_allowed', 0),
                           ('credentials_allowed', 0.0),
                           ('deletion_authority', 'automatic')]:
            core = config_core(Path('/tmp/himr-config-test'))
            core['safety'] = copy.deepcopy(REPLACEMENT_SAFETY)
            core['safety'][key] = value
            with self.subTest(key=key), self.assertRaises(ConfigError):
                build_config(core)
