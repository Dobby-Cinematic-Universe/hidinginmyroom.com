"""Unlabeled summary-source projection for exactly one anonymous speaker label.

Original transcripts, provider replies and completion receipts remain intact.
The derived source retains text, timestamps and original segment references;
only speaker metadata and its derived evidence/source IDs change. A single API
label is not a verified identity or proof that the recording has one person.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from pathlib import Path
import threading

from pipeline import transcript_summary as r

sources = r.sources_module
_ORIGINAL = sources.normalize_source
_ACTIVE = ContextVar('himr_single_label_projection', default=None)
_LOCK = threading.Lock()


def project(value):
    value = sources.validate_source(value)
    labels = {row['speaker'] for row in value['segments'] if row['speaker'] is not None}
    if value['format'] not in {'cloud', 'third_party'} or len(labels) != 1:
        return value
    output = deepcopy(value)
    for row in output['segments']:
        row['speaker'] = None
        row['source_ref']['speaker_scope'] = None
        row['evidence_id'] = sources._evidence_id(output['transcript'], row)
    body = {key: item for key, item in output.items() if key != 'source_id'}
    output['source_id'] = 'summarysrc_' + sources._hash(body)[:32]
    return sources.validate_source(output)


def _normalize(spec):
    original = _ORIGINAL(spec)
    state = _ACTIVE.get()
    if state is None:
        return original
    if (original['format'] not in {'cloud', 'third_party'} or
            len({row['speaker'] for row in original['segments'] if row['speaker'] is not None}) != 1):
        return original
    projected = project(original)
    if projected == original:
        return original
    from pipeline import cloud_transcription_summary as worker
    root = Path(state['manifest']['state_root'])
    plan_path = root / 'records' / worker._record_key(original['recording_id']) / 'plan.json'
    if r.safe.exists(plan_path):
        plan = r.read(r.binding(plan_path))
        # Preserve any pre-existing sealed plan rather than changing paid job
        # IDs or resending its inputs. New projected plans replay consistently.
        if original['source_id'] in plan.get('source_ids', []):
            state['legacy_plans_preserved'].add(original['recording_id'])
            return original
        if projected['source_id'] not in plan.get('source_ids', []):
            raise r.Error('singleton projection differs from the existing summary source')
    state['projected_recordings'].add(projected['recording_id'])
    return projected


def active():
    return _ACTIVE.get() is not None


def statistics():
    state = _ACTIVE.get()
    return None if state is None else {
        'single_label_recordings_projected': len(state['projected_recordings']),
        'legacy_summary_plans_preserved': len(state['legacy_plans_preserved']),
        'raw_transcripts_modified': False, 'speaker_identity_inferred': False}


@contextmanager
def scope(worker_ref):
    if active() or not _LOCK.acquire(blocking=False):
        raise r.Error('another singleton projection scope is active')
    token = None
    previous = sources.normalize_source
    try:
        if previous is not _ORIGINAL:
            raise r.Error('unexpected source normalization override')
        manifest = r.read(worker_ref)
        root = r.safe.path_value(manifest['state_root'])
        if (manifest.get('kind') != 'himr_cloud_transcript_summary_worker'
                or worker_ref['path'] != str(root / 'manifest.json')
                or r.read(r.binding(root / 'workspace.json')) != manifest):
            raise r.Error('singleton projection requires a bound summary worker')
        token = _ACTIVE.set({'manifest': manifest, 'projected_recordings': set(),
                             'legacy_plans_preserved': set()})
        sources.normalize_source = _normalize
        yield
    finally:
        sources.normalize_source = previous
        if token is not None:
            _ACTIVE.reset(token)
        _LOCK.release()
