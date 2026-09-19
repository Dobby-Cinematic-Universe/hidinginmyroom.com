from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from pipeline import gemini_queue_tokens as q
from pipeline import transcript_summary as io
from pipeline.tests.test_cloud_transcription_client import Response


def job(text='Hello', allowance=25000):
    return dict(provider='gemini', model='gemini-3.8-flash',
        request=dict(body=dict(store=False, contents=[dict(role='user', parts=[dict(text=text)])],
            systemInstruction=dict(parts=[dict(text='Keep the evidence.')]),
            generationConfig=dict(responseMimeType='application/json', responseSchema=dict(type='OBJECT')))),
        budget=dict(input_token_allowance=allowance, maximum_cost_microusd=20000))


class CounterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'counts'
        self.api = Mock()
        self.api.count.return_value = dict(totalTokens=5000)
        self.counter = q.Counter(self.root, self.api)

    def test_exact_request_count_with_headroom_does_not_edit_budget(self):
        value = job()
        before = deepcopy(value)
        self.counter.ensure([value])
        estimate = self.counter.estimate([value])
        self.assertEqual(estimate['input_tokens'], 5628)
        self.assertEqual(estimate['financial_input_token_allowance'], 25000)
        self.assertEqual(estimate['counted_jobs'], 1)
        self.assertEqual(value, before)

    def test_persistent_cache_survives_restart_without_another_request(self):
        self.counter.ensure([job()])
        second_api = Mock()
        second = q.Counter(self.root, second_api)
        second.ensure([job()])
        self.assertEqual(second.estimate([job()])['input_tokens'], 5628)
        second_api.count.assert_not_called()

    def test_changed_text_model_system_or_schema_gets_a_new_count(self):
        values = [job(), job('changed')]
        model = job(); model['model'] = 'gemini-3.8-pro'; values.append(model)
        system = job(); system['request']['body']['systemInstruction']['parts'][0]['text'] = 'Different'; values.append(system)
        schema = job(); schema['request']['body']['generationConfig']['responseSchema']['type'] = 'ARRAY'; values.append(schema)
        self.counter.ensure(values)
        self.assertEqual(self.api.count.call_count, 5)

    def test_count_failure_keeps_full_allowance_and_cools_down(self):
        self.api.count.side_effect = q.client.BatchClientError('HTTP failure', status_code=429)
        self.counter.ensure([job()])
        self.counter.ensure([job()])
        self.assertEqual(self.api.count.call_count, 1)
        self.assertEqual(self.counter.estimate([job()])['input_tokens'], 25000)
        self.assertEqual(self.counter.estimate([job()])['fallback_jobs'], 1)

    def test_bad_counts_never_become_zero(self):
        for value in (None, 0, -1, True, 2_000_001, '5000'):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as temp:
                api = Mock(); api.count.return_value = dict(totalTokens=value)
                counter = q.Counter(Path(temp) / 'cache', api)
                counter.ensure([job()])
                self.assertEqual(counter.estimate([job()])['input_tokens'], 25000)

    def test_orphan_and_pending_jobs_use_the_same_count_and_all_jobs_are_summed(self):
        values = [job('one'), job('two')]
        self.counter.ensure(values)
        self.assertEqual(self.counter.estimate(values)['input_tokens'], 11256)
        self.assertEqual(self.counter.estimate(values)['financial_input_token_allowance'], 50000)

    def test_missing_or_corrupt_optional_cache_falls_back(self):
        self.counter.ensure([job()])
        path = next(self.root.glob('*.json'))
        path.chmod(0o600); path.write_bytes(b'bad json')
        second = q.Counter(self.root)
        self.assertEqual(second.estimate([job()])['input_tokens'], 25000)

    def test_duplicate_request_counted_once(self):
        self.counter.ensure([job(), job(), job()])
        self.assertEqual(self.api.count.call_count, 1)

    def test_counted_policy_reuses_cache_without_per_request_padding(self):
        value = job()
        before = deepcopy(value)
        self.counter.ensure([value])
        api = Mock()
        counter = q.Counter(self.root, api, policy=q.COUNTED_POLICY)
        counter.ensure([value])
        estimate = counter.estimate([value])
        api.count.assert_not_called()
        self.assertEqual(estimate['input_tokens'], 5000)
        self.assertEqual(estimate['counted_input_tokens'], 5000)
        self.assertEqual(estimate['per_request_headroom_tokens'], 0)
        self.assertEqual(estimate['fallback_input_token_allowance'], 0)
        self.assertEqual(estimate['financial_input_token_allowance'], 25000)
        self.assertEqual(estimate['policy'], q.COUNTED_POLICY)
        self.assertFalse(estimate['exact'])
        self.assertEqual(value, before)

    def test_counted_policy_sums_counts_and_full_missing_allowance(self):
        self.counter.ensure([job()])
        counter = q.Counter(self.root, policy=q.COUNTED_POLICY)
        estimate = counter.estimate([job(), job('not counted', allowance=12345)])
        self.assertEqual(estimate['input_tokens'], 17345)
        self.assertEqual(estimate['counted_input_tokens'], 5000)
        self.assertEqual(estimate['fallback_input_token_allowance'], 12345)
        self.assertEqual(estimate['fallback_jobs'], 1)
        self.assertEqual(estimate['financial_input_token_allowance'], 37345)

    def test_unknown_policy_rejected_without_creating_cache(self):
        path = Path(self.temp.name) / 'invalid-policy'
        with self.assertRaises(ValueError):
            q.Counter(path, policy='disable-reservations')
        self.assertFalse(path.exists())


class ClientTests(unittest.TestCase):
    def test_count_only_endpoint_and_complete_prompt_projection(self):
        calls = []
        def transport(request, timeout):
            calls.append(request)
            self.assertTrue(request.full_url.endswith(':countTokens'))
            value = json.loads(request.data)['generateContentRequest']
            self.assertEqual(value['model'], 'models/gemini-3.8-flash')
            self.assertEqual(value['systemInstruction'], job()['request']['body']['systemInstruction'])
            self.assertEqual(value['generationConfig'], job()['request']['body']['generationConfig'])
            self.assertNotIn('store', value)
            return Response(dict(totalTokens=5000))
        client = q.CountClient('test-key', transport=transport)
        self.assertEqual(client.count(job())['totalTokens'], 5000)
        for method, path in [('POST', '/v1beta/models/gemini-3.8-flash:batchGenerateContent'),
                ('POST', '/v1beta/models/gemini-3.8-flash:generateContent'),
                ('GET', '/v1beta/batches/test'), ('POST', '//evil.example/countTokens')]:
            with self.assertRaises(q.client.BatchClientError):
                client._request(method, path)
        self.assertEqual(len(calls), 1)


if __name__ == '__main__':
    unittest.main()
