from copy import deepcopy
import unittest

from pipeline import cloud_diarization_tail_recovery as recovery
from pipeline import cloud_transcription_client as client
from pipeline.tests.test_cloud_transcription_client import assembly_result


class TailRecoveryTests(unittest.TestCase):
    def fixture(self):
        raw=assembly_result()
        words=[dict(text='meow',start=8000+i*50,end=8050+i*50,speaker='B',confidence=.99) for i in range(64)]
        tail=dict(start=words[0]['start'],end=words[-1]['end'],speaker='B',text=' '.join(w['text'] for w in words),words=words)
        raw['utterances'].append(tail);raw['words']+=deepcopy(words);raw['text']+=' '+tail['text']
        return raw

    def test_isolates_only_tail_without_fabricating_times_or_modifying_raw(self):
        raw=self.fixture();before=deepcopy(raw)
        adjusted,normalized,quarantine=recovery.isolate_tail(raw,10000)
        self.assertEqual(raw,before)
        self.assertEqual(adjusted['utterances'],before['utterances'][:-1])
        self.assertEqual(normalized['text'],'Hello.')
        self.assertEqual(normalized['segments'][0]['start_ms'],100)
        self.assertEqual(normalized['segments'][0]['end_ms'],700)
        self.assertEqual(quarantine['original_utterance'],before['utterances'][-1])
        self.assertEqual(quarantine['repetition_count'],64)
        self.assertFalse(quarantine['audio_content_confirmed'])

    def test_refuses_mixed_speech_valid_times_large_overruns_and_mismatched_tail(self):
        for change in ('mixed_speech','within_audio','large_overrun','text_suffix','word_suffix','too_long'):
            raw=self.fixture();tail=raw['utterances'][-1]
            if change=='mixed_speech':tail['words'][0]['text']='actual speech'
            if change=='within_audio':tail['end']=9999
            if change=='large_overrun':tail['end']=20000
            if change=='text_suffix':raw['text']+=' elsewhere'
            if change=='word_suffix':raw['words'][-1]['text']='different'
            if change=='too_long':tail['start']=-1000
            with self.subTest(change=change),self.assertRaises(ValueError):
                recovery.isolate_tail(raw,10000)

    def test_retained_prefix_must_pass_unchanged_strict_validation(self):
        raw=self.fixture();raw['utterances'][0]['words'][0]['start']=-1
        with self.assertRaises(client.CloudClientError):recovery.isolate_tail(raw,10000)


if __name__=='__main__':unittest.main()
