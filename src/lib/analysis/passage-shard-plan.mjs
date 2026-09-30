import { createHash } from 'node:crypto';
import { validatePassageData } from './passage-data.mjs';
import { passageShardEnvelope, PASSAGE_SHARD_MAX_BYTES, PASSAGE_SHARD_MAX_RECORDINGS } from './passage-shards.mjs';

const describe = (text, recording_ids) => {
  const id = createHash('sha256').update(text).digest('hex');
  return { id, url: `/corpus/analysis/passage-shards/${id}.json`, bytes: Buffer.byteLength(text), count: recording_ids.length, recording_ids };
};

// Keep only one small group while planning; cached plans contain identifiers,
// never the complete passage corpus. Reading again also detects changed inputs.
export async function planPassageShards(release, readRecording) {
  const descriptors = [];
  let group = [], ids = [], bytes = Buffer.byteLength(JSON.stringify(passageShardEnvelope(release, [])));
  const flush = () => {
    if (ids.length) descriptors.push(describe(JSON.stringify(passageShardEnvelope(release, group)), ids));
    group = []; ids = []; bytes = Buffer.byteLength(JSON.stringify(passageShardEnvelope(release, [])));
  };
  for (const video of release.videos) {
    const data = validatePassageData(await readRecording(video.recording_id), release, video);
    const size = Buffer.byteLength(JSON.stringify(data));
    if (group.length && (group.length === PASSAGE_SHARD_MAX_RECORDINGS || bytes + size + 1 > PASSAGE_SHARD_MAX_BYTES)) flush();
    const addition = size + (group.length ? 1 : 0);
    if (bytes + addition > PASSAGE_SHARD_MAX_BYTES) throw new Error(`Passage recording exceeds shard size bound: ${video.recording_id}`);
    group.push(data); ids.push(video.recording_id); bytes += addition;
  }
  flush();
  return descriptors;
}

export async function renderPassageShard(descriptor, release, readRecording) {
  const videos = new Map(release.videos.map(video => [video.recording_id, video]));
  const recordings = [];
  for (const id of descriptor.recording_ids) {
    const video = videos.get(id);
    if (!video) throw new Error('Passage shard contains an unknown recording.');
    recordings.push(validatePassageData(await readRecording(id), release, video));
  }
  const text = JSON.stringify(passageShardEnvelope(release, recordings));
  const actual = describe(text, descriptor.recording_ids);
  if (actual.id !== descriptor.id || actual.bytes !== descriptor.bytes || actual.bytes > PASSAGE_SHARD_MAX_BYTES) throw new Error('Passage shard inputs changed after planning.');
  return text;
}
