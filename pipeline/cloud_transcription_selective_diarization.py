"""Future-request routing: conversation titles, faces and exact transcript leads.

Acoustic diversity or uncertainty alone does not request diarization. This is
routing metadata, not a verified human count, identity or retranscription grant.
The historical risk policy remains unchanged so paid proofs still replay.
"""
from copy import deepcopy
import hashlib
from pathlib import Path

from pipeline import transcript_summary as io
from pipeline import cloud_transcription_title_policy as titles
from pipeline import cloud_transcription_screen as screen

KIND = 'himr_selective_cloud_diarization'
RULES = {'version': 1, 'conversation_titles': True, 'multiple_face_cues': True,
         'strong_or_moderate_exact_transcript_leads': True,
         'acoustic_diversity_alone': False, 'inconclusive_screen_alone': False,
         'new_followup_probes': False, 'existing_paid_decisions_frozen': True,
         'automatic_retranscription': False, 'speaker_identity_inferred': False,
         'verified_human_count_inferred': False}


def implementation():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def prepare(plan_ref, legacy_policy_ref, output):
    # Import only in versioned runtimes containing the original risk policy.
    from pipeline import cloud_transcription_diarization_policy as legacy
    old = legacy.load(legacy_policy_ref)
    plan = io.read(plan_ref)
    if old['plan'] != plan_ref or old['configuration'] != plan['screen_config']:
        raise ValueError('selective policy requires the exact prior campaign')
    value = {'kind': KIND + '_policy', 'schema_version': 1, 'plan': plan_ref,
             'configuration': plan['screen_config'], 'legacy_policy': legacy_policy_ref,
             'rules': RULES, 'implementation': implementation()}
    output = io.safe.path_value(output)
    io.protect(output.parent, {'plan': plan_ref, 'old': legacy_policy_ref,
                               'cloud_root': plan['state_root']})
    io.mkdir(output.parent)
    return io.put(output, value)


def load(ref):
    from pipeline import cloud_transcription_diarization_policy as legacy
    value = io.read(ref)
    io.safe.exact(value, {'kind', 'schema_version', 'plan', 'configuration',
                         'legacy_policy', 'rules', 'implementation'}, 'selective policy')
    if (value['kind'] != KIND + '_policy' or type(value['schema_version']) is not int
            or value['schema_version'] != 1 or value['rules'] != RULES
            or value['implementation'] != implementation()):
        raise ValueError('selective policy or implementation changed')
    old = legacy.load(value['legacy_policy'])
    if old['plan'] != value['plan'] or old['configuration'] != value['configuration']:
        raise ValueError('selective policy legacy campaign mismatch')
    return value


def route(recording, base, text_recordings):
    reasons = []
    if titles.match_titles(recording)['matched']:
        reasons.append('explicit_conversation_title')
    count = base['evidence_summary'].get('multiple_face_samples', 0)
    if type(count) is not int or count < 0:
        raise ValueError('invalid multiple-face count')
    if count:
        reasons.append('visual_people_cue')
    if recording['recording_id'] in text_recordings:
        reasons.append('strong_or_moderate_exact_transcript_lead')
    return {'diarization': bool(reasons), 'reasons': reasons,
            'acoustic_screen_state': base['state'],
            'whole_recording_solo_proven': False, 'speaker_identity_inferred': False}


def effective(recording, base_ref, policy_ref):
    from pipeline import cloud_transcription_diarization_policy as legacy
    policy = load(policy_ref)
    base = io.read(base_ref)
    screen.validate_decision(base, recording, policy['configuration'])
    routing = route(recording, base, legacy.load(policy['legacy_policy'])['text_recordings'])
    output = deepcopy(base)
    output['diarization'] = routing['diarization']
    output['method']['selective_diarization'] = {
        'policy': policy_ref, 'base_decision': base_ref, 'routing': routing,
        'paid_requests_reinterpreted': False}
    return output


def validate_effective(value, recording, config_ref):
    proof = value.get('method', {}).get('selective_diarization')
    if not isinstance(proof, dict):
        raise ValueError('missing selective diarization proof')
    policy = load(proof['policy'])
    if policy['configuration'] != config_ref:
        raise ValueError('selective screen configuration mismatch')
    if value != effective(recording, proof['base_decision'], proof['policy']):
        raise ValueError('selective diarization proof does not replay')
    return value
