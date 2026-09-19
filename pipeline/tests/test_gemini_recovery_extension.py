from copy import deepcopy
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
from pipeline import gemini_recovery_extension as recovery
from pipeline import transcript_summary_campaign as campaign
from pipeline import transcript_summary_core as core


class CachedAccountingTests(unittest.TestCase):
    def setUp(self):
        self.job = dict(provider='gemini', model=core.PROFILES['gemini_flash_batch']['model'],
            budget=dict(maximum_cost_microusd=1000000, input_token_allowance=10000, output_token_allowance=10000))
        self.response = dict(modelVersion=self.job['model'], usageMetadata=dict(
            promptTokenCount=1000, candidatesTokenCount=100, totalTokenCount=1100, cachedContentTokenCount=500))
        self.calculate = recovery.cached_usage(campaign.usage_cost)

    def test_cached_input_settles_without_discount_or_mutation(self):
        before = deepcopy(self.response)
        plain = deepcopy(before)
        plain['usageMetadata']['cachedContentTokenCount'] = 0
        self.assertEqual(self.calculate(self.job, before), campaign.usage_cost(self.job, plain))
        self.assertTrue(self.calculate(self.job, before)[1])
        self.assertEqual(before, self.response)

    def test_invalid_cached_counts_and_missing_usage_stay_held(self):
        for cached in (-1, 1001, True, '500', None):
            response = deepcopy(self.response)
            response['usageMetadata']['cachedContentTokenCount'] = cached
            self.assertEqual(self.calculate(self.job, response), (1000000, False))
        self.assertEqual(self.calculate(self.job, None), (1000000, False))

    def test_tools_unknown_model_and_inconsistent_usage_still_held(self):
        for key, value in [('toolUsePromptTokenCount', 1), ('totalTokenCount', 1)]:
            response = deepcopy(self.response)
            response['usageMetadata'][key] = value
            self.assertEqual(self.calculate(self.job, response), (1000000, False))
        response = {**self.response, 'modelVersion': 'unknown'}
        self.assertEqual(self.calculate(self.job, response), (1000000, False))

    def test_allowance_overrun_still_errors(self):
        self.job['budget']['input_token_allowance'] = 1
        with self.assertRaises(campaign.runner.Error):
            self.calculate(self.job, self.response)


class MemoTests(unittest.TestCase):
    def test_mutations_miss_and_return_values_are_isolated(self):
        calls = []
        def validate(value):
            calls.append(deepcopy(value))
            return deepcopy(value)
        cached = recovery.memoized_validator(validate, core.canonical, core.json.loads)
        value = {'items': [1]}
        cached(value)['items'].append(2)
        self.assertEqual(cached(value), value)
        self.assertEqual(len(calls), 1)
        value['items'].append(3)
        self.assertEqual(cached(value), value)
        self.assertEqual(len(calls), 2)


class TerminalReuseTests(unittest.TestCase):
    def setUp(self):
        from pipeline.tests.test_cloud_transcription_summary import WorkerTests
        from pipeline import cloud_transcription_summary as worker
        from pipeline import cloud_transcription_summary_cache as cache
        from pipeline import cloud_transcription_summary_admission as admission
        self.worker = SimpleNamespace(r=worker.r, job_cache=cache, accounting=campaign, admission=admission)
        self.case = WorkerTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.prepare()
        self.case.cycle()
        self.case.client.complete = True
        self.case.cycle()
        self.case.cycle()
        manifest = worker.load_manifest(self.case.ref)
        self.entry = worker.r.read(worker.r.binding(next((self.case.worker_root / 'entries').glob('*.json'))))
        self.expected = worker._request(manifest, self.entry['source'])

    def test_retained_accounting_and_waves_match_full_snapshot_without_replay(self):
        r = self.worker.r
        plan, sources = r.load_plan(self.entry['plan']['path'], self.entry['plan']['sha256'])
        original = r.load_state(plan, sources)
        with patch.object(self.worker.r, 'load_state', side_effect=AssertionError('completed graph replay')):
            compact, artifact = recovery.retained_terminal(self.worker, self.entry, self.expected)
        self.assertEqual(compact['accounted'], campaign.accounted_state(plan, original))
        self.assertEqual([w['wave_id'] for w in compact['waves']], [w['wave_id'] for w in original['waves']])
        self.assertTrue(compact['status']['transcript_phase_complete'])
        self.assertTrue(artifact['phase_complete'])
        self.assertEqual(len(self.case.client.created), 2)

    def test_interrupted_collection_falls_back(self):
        folder = Path(self.entry['plan']['path']).parent
        next((folder / 'waves').glob('*/collection.json')).unlink()
        self.assertIsNone(recovery.retained_terminal(self.worker, self.entry, self.expected))

    def test_changed_capture_rejects(self):
        folder = Path(self.entry['plan']['path']).parent
        capture = next((folder / 'waves').glob('*/capture.json'))
        capture.chmod(0o600)
        capture.write_text('{}')
        with self.assertRaises(Exception):
            recovery.retained_terminal(self.worker, self.entry, self.expected)

    def test_changed_export_rejects(self):
        folder = Path(self.entry['plan']['path']).parent
        export = next((folder / 'exports').glob('*.json'))
        export.chmod(0o600)
        export.write_text('{}')
        with self.assertRaises(self.worker.r.Error):
            recovery.retained_terminal(self.worker, self.entry, self.expected)

    def test_changed_request_bytes_reject(self):
        folder = Path(self.entry['plan']['path']).parent
        request = next((folder / 'waves').glob('*/requests.bin'))
        request.chmod(0o600)
        request.write_bytes(b'corrupt')
        with self.assertRaises(Exception):
            recovery.retained_terminal(self.worker, self.entry, self.expected)

    def test_changed_intent_rejects(self):
        folder = Path(self.entry['plan']['path']).parent
        intent = next((folder / 'waves').glob('*/submit-intent.json'))
        intent.chmod(0o600)
        intent.write_text('{}')
        with self.assertRaises(self.worker.r.Error):
            recovery.retained_terminal(self.worker, self.entry, self.expected)
