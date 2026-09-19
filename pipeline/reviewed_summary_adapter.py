"""Explicit reviewed-evidence extension for the retained Gemini worker.

Provider transcripts and paid plans are immutable. A derived evidence document
renders saved speaker assignments as text and links each turn to its unchanged
original. Uncertainty is never resolved by guessing. All paid work stays in the
existing worker ledger; one reviewed revision can follow an older source.
"""
from copy import deepcopy
from functools import partial
from pathlib import Path

from pipeline import reviewed_transcript_feed as feed

KIND = 'himr_reviewed_summary_evidence'
SHORT_REASON = 'source_transcript_under_50_words'


def proof(reference, sources):
    """Replay just this copy and its explicit decisions, not archive media."""
    projected = sources.read_json(reference)
    audit = projected['projection']
    if audit['implementation'] != feed.bound(feed.__file__):
        raise sources.SourceError('review projection implementation changed')
    original = sources.read_json(audit['original_transcript'])
    if (original.get('kind') != 'himr_cloud_recording_transcript'
            or original.get('status') != 'completed'
            or original.get('schema_version') != 1):
        raise sources.SourceError('review requires a completed cloud transcript')
    reviews = [sources.read_json(ref) for ref in audit['review_decisions']]
    if any(row['transcript'] != audit['original_transcript']
           or row['decision']['job_id'] != original['job_id'] for row in reviews):
        raise sources.SourceError('review decision source differs')
    confirmed = sources.read_json(audit['confirmations'])
    matches = [row for row in confirmed['records'] if row['job_id'] == original['job_id']]
    if len(matches) > 1:
        raise sources.SourceError('duplicate review confirmation')
    expected, model, evidence = feed.project(original, audit['original_transcript'],
        reviews, matches[0] if matches else None)
    expected['projection'].update({key: audit[key]
        for key in ('review_decisions', 'confirmations', 'implementation')})
    if projected != expected or not audit['review_complete']:
        raise sources.SourceError('review projection differs or remains incomplete')
    return projected, model, evidence


def revision_id(recording, original_ref):
    # One revision per new transcript, not per auto-save/review edit.
    return recording + ':reviewed:' + original_ref['sha256'][:32]


def render(reference, recording, sources):
    projected, model, _ = proof(reference, sources)
    physical = projected['recording_id']
    if recording not in {physical, revision_id(physical, projected['projection']['original_transcript'])}:
        raise sources.SourceError('reviewed recording identity differs')
    rows = []
    for segment in model['segments']:
        index = int(segment['evidence_id'].removeprefix('segment_'))
        original = projected['segments'][index]
        rows.append(dict(text=segment['speaker'] + ': ' + segment['text'], speaker=None,
            start_ms=original['start_ms'], end_ms=original['end_ms'], original_segment_index=index))
    return dict(kind=KIND, schema_version=1, recording_id=recording,
        original_recording_id=physical, reviewed_copy=reference, status='completed',
        rendering='saved_speaker_prefix_plus_original_text_not_verbatim_transcript',
        speaker_identity_inferred=False, word_count=projected['projection']['word_count_for_summary'],
        summary_eligible=projected['projection']['summary_eligible'], segments=rows)


def validate_document(doc, spec, sources):
    if spec['format'] != 'cloud' or spec['completion'] != doc['reviewed_copy']:
        raise sources.SourceError('reviewed evidence completion differs')
    expected = render(doc['reviewed_copy'], spec['recording_id'], sources)
    if doc != expected:
        raise sources.SourceError('reviewed evidence differs from its explicit reviews')
    return doc


def units(doc, sources):
    return [sources._unit(row, sources._ref('segments', index,
        native_segment_id='reviewed-original-segment-' + str(row['original_segment_index'])),
        'approximate_provider_recording_ms') for index, row in enumerate(doc['segments'])]


def install_sources(sources):
    """Extend only the new native kind; standard source validation is unchanged."""
    original_cloud, original_validate = sources._cloud, sources.validate_source

    def cloud(doc, spec):
        if doc.get('kind') != KIND:
            return original_cloud(doc, spec)
        validate_document(doc, spec, sources)
        if not doc['summary_eligible']:
            raise sources.SourceError('reviewed source has fewer than 50 participant words')
        return units(doc, sources), spec['transcript']['sha256'], None, False

    def validate(value):
        if value.get('provenance', {}).get('source_kind') != KIND:
            return original_validate(value)
        # Reuse all existing structural, timestamp, citation, and provenance
        # checks on a temporary shape-only value. This is never persisted or
        # presented as provider output. The actual new native kind is replayed
        # against the manual decisions and rendered document below.
        shape = deepcopy(value)
        shape['provenance']['source_kind'] = 'himr_cloud_recording_transcript'
        shape['source_id'] = 'summarysrc_' + sources._hash({k: v for k, v in shape.items() if k != 'source_id'})[:32]
        original_validate(shape)
        body = {k: v for k, v in value.items() if k != 'source_id'}
        if value['source_id'] != 'summarysrc_' + sources._hash(body)[:32]:
            raise sources.SourceError('reviewed normalized source identity differs')
        doc = sources.read_json(value['transcript'])
        spec = dict(format=value['format'], recording_id=value['recording_id'],
            completion=value['provenance']['completion_evidence'])
        validate_document(doc, spec, sources)
        expected = units(doc, sources)
        for ordinal, row in enumerate(expected):
            row['ordinal'] = ordinal
            row['evidence_id'] = sources._evidence_id(value['transcript'], row)
        if (not doc['summary_eligible'] or value['segments'] != expected
                or value['provenance']['source_identity_sha256'] != value['transcript']['sha256']):
            raise sources.SourceError('reviewed source evidence differs')
        return deepcopy(value)

    sources._cloud, sources.validate_source = cloud, validate

    def restore():
        sources._cloud, sources.validate_source = original_cloud, original_validate
    return restore


def initialize_collector(extension_ref, feed_ref, *args):
    """Picklable spawn initializer; no keys cross a process boundary."""
    from pipeline import cloud_transcription_summary_parallel as parallel
    from pipeline import cloud_transcription_summary as worker
    for ref, actual in ((extension_ref, __file__), (feed_ref, feed.__file__)):
        if ref != feed.bound(actual):
            raise RuntimeError('reviewed collector implementation changed')
    parallel._initialize(*args)
    restore = install_sources(worker.r.sources_module)
    parallel._CHILD['stack'].callback(restore)


def select_sources(worker, manifest, selected, index_path):
    """Preserve sealed source revisions, even if the user later edits a review."""
    r = worker.r
    index_path = Path(index_path)
    index = r.read(r.binding(index_path))
    if index.get('kind') != 'himr_reviewed_transcript_feed' or len(index['records']) > worker.MAX_RECORDS:
        raise r.Error('unsupported reviewed feed')
    selected = deepcopy(selected)
    sealed = {}
    for path in sorted((Path(manifest['state_root']) / 'entries').glob('*.json')):
        entry = r.read(r.binding(path))
        source = entry['source']
        if path.name != worker._record_key(source['recording_id']) + '.json':
            raise r.Error('review source entry identity differs')
        sealed[source['recording_id']] = source
    for item in index['records']:
        if not item['review_complete']:
            continue
        physical = item['recording_id']
        recording = physical
        old = sealed.get(physical)
        if old is not None:
            old_doc = r.read(old['transcript'])
            if old_doc.get('kind') == KIND:
                selected[physical] = old
                continue  # No paid re-run for every review auto-save.
            if old['format'] != 'third_party':
                continue  # Existing cloud-summary revisions are not replaced.
            recording = revision_id(physical, item['original_transcript'])
        if recording in sealed:
            selected[recording] = sealed[recording]
            continue
        doc = render(item['transcript'], recording, r.sources_module)
        artifact = feed.immutable(index_path.parent / 'gemini-evidence', 'evidence', doc)
        selected[recording] = dict(recording_id=recording, title=item['title'],
            date_metadata=selected.get(physical, {}).get('date_metadata'), format='cloud',
            transcript=artifact, completion=item['transcript'])
    # Keep in-flight reviewed inputs exact if a newer feed no longer admits them.
    for recording, source in sealed.items():
        if source.get('format') == 'cloud' and r.read(source['transcript']).get('kind') == KIND:
            selected[recording] = source
    if len(selected) > worker.MAX_RECORDS:
        raise r.Error('reviewed source selection exceeds bound')
    return selected


def install(worker, index_path):
    install_sources(worker.r.sources_module)
    original_available, original_gate = worker._available, worker._identity_holds

    def available(manifest):
        return select_sources(worker, manifest, original_available(manifest), index_path)

    def gate(selected):
        ordinary, held = {}, {}
        for recording, source in selected.items():
            doc = worker.r.read(source['transcript'])
            if doc.get('kind') != KIND:
                ordinary[recording] = source
                continue
            validate_document(doc, {**source, 'completion': source['completion']}, worker.r.sources_module)
            if not doc['summary_eligible']:
                held[recording] = dict(recording_id=recording, reason=SHORT_REASON,
                    transcript=source['transcript'], word_count=doc['word_count'])
        return {**original_gate(ordinary), **held}

    worker._available, worker._identity_holds = available, gate
    worker.parallel_collection._initialize = partial(initialize_collector,
        feed.bound(__file__), feed.bound(feed.__file__))
