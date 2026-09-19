import { createHash } from 'node:crypto';
import { lstat, readFile } from 'node:fs/promises';
import path from 'node:path';
import { localPreviewRoot } from '../local-preview';
import { loadCorpusCatalog, loadCorpusRecording } from './release';
import { loadSummaryRelease } from '../summaries/release';
import { createDerivedGraph } from './derived-graph.mjs';
import { attachEventGroups } from './event-groups.mjs';

import type {
  CorpusGraphAnchor,
  CorpusGraphAppearance,
  CorpusGraphCounts,
  CorpusGraphDate,
  CorpusGraphEntity,
  CorpusGraphEvent,
  CorpusGraphEvidence,
  CorpusGraphParticipant,
  CorpusGraphRelation,
  CorpusGraphRelease,
  CorpusShardReference,
} from './types';

const graphDataRoot = process.env.HIMR_CORPUS_GRAPH_DATA_ROOT
  ? path.resolve(process.env.HIMR_CORPUS_GRAPH_DATA_ROOT)
  : path.resolve(process.cwd(), 'src', 'data', 'corpus', 'graph');
const graphManifestFile = path.join(graphDataRoot, 'manifest.json');

const identifierPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
const slugPattern = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const hashPattern = /^[a-f0-9]{64}$/;
const releaseIdPattern = /^graph_release_[a-f0-9]{24}$/;
const graphPathPattern = /^graph\/graph-[a-f0-9]{16}\.json$/;
const utcPattern =
  /^(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?Z$/;
const datePattern = /^\d{4}(?:-\d{2}(?:-\d{2})?)?$/;

const entityTypes = new Set([
  'person',
  'community_figure',
  'animal',
  'place',
  'organization',
  'platform',
  'term',
  'object',
  'unknown',
]);
const datePrecisions = new Set(['day', 'month', 'year', 'range', 'circa', 'unknown']);
const dateCertainties = new Set(['certain', 'probable', 'uncertain', 'disputed']);
const supportKinds = new Set(['direct', 'contextual', 'corroborating', 'contradicting']);

interface GraphManifest {
  schema_version: 1;
  kind: 'entity_event_graph_manifest';
  release_id: string;
  generated_at: string;
  counts: CorpusGraphCounts;
  graph_shard: CorpusShardReference;
}

let cachedGraph: Promise<CorpusGraphRelease> | undefined;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function exact(value: unknown, keys: string[], field: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`${field} must be an object.`);
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  if (
    actual.length !== expected.length ||
    actual.some((key, index) => key !== expected[index])
  ) {
    throw new Error(`${field} has unexpected or missing fields.`);
  }
  return value;
}

function text(value: unknown, field: string, maximum: number): string {
  if (
    typeof value !== 'string' ||
    value.trim() === '' ||
    value.length > maximum ||
    value.includes('\0')
  ) {
    throw new Error(`${field} must be a bounded non-empty string.`);
  }
  return value;
}

function identifier(value: unknown, field: string): string {
  const result = text(value, field, 256);
  if (!identifierPattern.test(result)) throw new Error(`${field} is not a safe ID.`);
  return result;
}

function slug(value: unknown, field: string): string {
  const result = text(value, field, 160);
  if (!slugPattern.test(result)) throw new Error(`${field} is not a safe slug.`);
  return result;
}

function integer(value: unknown, field: string, minimum = 0): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < minimum) {
    throw new Error(`${field} must be an integer >= ${minimum}.`);
  }
  return value;
}

function oneOf(value: unknown, allowed: Set<string>, field: string): string {
  const result = text(value, field, 512);
  if (!allowed.has(result)) throw new Error(`${field} is not allowed.`);
  return result;
}

function timestamp(value: unknown, field: string): string {
  const result = text(value, field, 64);
  const parsed = new Date(result);
  if (!utcPattern.test(result) || Number.isNaN(parsed.getTime())) {
    throw new Error(`${field} must be an explicit valid UTC timestamp.`);
  }
  return result;
}

function nullableDate(value: unknown, field: string): string | null {
  if (value === null) return null;
  const result = text(value, field, 10);
  if (!datePattern.test(result)) throw new Error(`${field} is not a bounded date value.`);
  const [yearText, monthText, dayText] = result.split('-');
  const year = Number(yearText);
  const month = monthText === undefined ? 1 : Number(monthText);
  const day = dayText === undefined ? 1 : Number(dayText);
  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (
    parsed.getUTCFullYear() !== year ||
    parsed.getUTCMonth() !== month - 1 ||
    parsed.getUTCDate() !== day
  ) {
    throw new Error(`${field} is not a valid calendar value.`);
  }
  return result;
}

function canonicalJson(value: unknown): string {
  if (
    value === null ||
    typeof value === 'boolean' ||
    typeof value === 'number' ||
    typeof value === 'string'
  ) {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(',')}}`;
  }
  throw new Error('Graph release contains a value that cannot be canonicalized.');
}

function sha256(value: string | Uint8Array): string {
  return createHash('sha256').update(value).digest('hex');
}

function parseCounts(value: unknown, field: string): CorpusGraphCounts {
  const counts = exact(
    value,
    [
      'entities',
      'events',
      'appearances',
      'event_participants',
      'event_dates',
      'event_relations',
      'event_evidence',
    ],
    field,
  );
  return {
    entities: integer(counts.entities, `${field}.entities`),
    events: integer(counts.events, `${field}.events`),
    appearances: integer(counts.appearances, `${field}.appearances`),
    event_participants: integer(counts.event_participants, `${field}.event_participants`),
    event_dates: integer(counts.event_dates, `${field}.event_dates`),
    event_relations: integer(counts.event_relations, `${field}.event_relations`),
    event_evidence: integer(counts.event_evidence, `${field}.event_evidence`),
  };
}

function parseReference(value: unknown): CorpusShardReference {
  const reference = exact(value, ['path', 'sha256', 'bytes'], 'graph_shard');
  const relativePath = text(reference.path, 'graph_shard.path', 100);
  if (
    !graphPathPattern.test(relativePath) ||
    path.posix.isAbsolute(relativePath) ||
    relativePath.split('/').some((part) => part === '' || part === '.' || part === '..')
  ) {
    throw new Error('graph_shard.path is unsafe.');
  }
  const digest = text(reference.sha256, 'graph_shard.sha256', 64);
  if (!hashPattern.test(digest)) throw new Error('graph_shard.sha256 is invalid.');
  return {
    path: relativePath,
    sha256: digest,
    bytes: integer(reference.bytes, 'graph_shard.bytes', 1),
  };
}

function parseManifest(value: unknown): GraphManifest {
  const manifest = exact(
    value,
    ['schema_version', 'kind', 'release_id', 'generated_at', 'counts', 'graph_shard'],
    'graph manifest',
  );
  if (manifest.schema_version !== 1 || manifest.kind !== 'entity_event_graph_manifest') {
    throw new Error('Unsupported graph manifest schema or kind.');
  }
  const releaseId = text(manifest.release_id, 'release_id', 38);
  if (!releaseIdPattern.test(releaseId)) throw new Error('Graph release_id is invalid.');
  const parsed: GraphManifest = {
    schema_version: 1,
    kind: 'entity_event_graph_manifest',
    release_id: releaseId,
    generated_at: timestamp(manifest.generated_at, 'generated_at'),
    counts: parseCounts(manifest.counts, 'counts'),
    graph_shard: parseReference(manifest.graph_shard),
  };
  const identity = Object.fromEntries(
    Object.entries(parsed).filter(([key]) => key !== 'release_id'),
  );
  const expected = `graph_release_${sha256(canonicalJson(identity)).slice(0, 24)}`;
  if (releaseId !== expected) throw new Error('Graph manifest release identity mismatch.');
  return parsed;
}

function parseAnchor(value: unknown, field: string): CorpusGraphAnchor {
  const anchor = exact(value, ['recording', 'source', 'rendition'], field);
  const recording = exact(anchor.recording, ['recording_id', 'slug', 'title'], `${field}.recording`);
  const source = exact(
    anchor.source,
    ['source_id', 'platform', 'url', 'native_id'],
    `${field}.source`,
  );
  const rendition = exact(
    anchor.rendition,
    ['rendition_id', 'time_basis'],
    `${field}.rendition`,
  );
  const url = text(source.url, `${field}.source.url`, 4096);
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new Error(`${field}.source.url is invalid.`);
  }
  if (
    !['http:', 'https:'].includes(parsed.protocol) ||
    !parsed.hostname ||
    parsed.username !== '' ||
    parsed.password !== ''
  ) {
    throw new Error(`${field}.source.url must be uncredentialed HTTP(S).`);
  }
  return {
    recording: {
      recording_id: identifier(recording.recording_id, `${field}.recording.recording_id`),
      slug: slug(recording.slug, `${field}.recording.slug`),
      title: text(recording.title, `${field}.recording.title`, 1000),
    },
    source: {
      source_id: identifier(source.source_id, `${field}.source.source_id`),
      platform: text(source.platform, `${field}.source.platform`, 200),
      url,
      native_id: text(source.native_id, `${field}.source.native_id`, 1000),
    },
    rendition: {
      rendition_id: identifier(rendition.rendition_id, `${field}.rendition.rendition_id`),
      time_basis: (() => {
        if (rendition.time_basis !== 'rendition_media_ms') {
          throw new Error(`${field}.rendition.time_basis is not allowed.`);
        }
        return 'rendition_media_ms' as const;
      })(),
    },
  };
}

function assertOrderedUnique<T, K extends keyof T>(
  values: T[],
  key: K,
  field: string,
): void {
  const ids = values.map((value) => String(value[key]));
  const sortedIds = [...ids].sort();
  if (new Set(ids).size !== ids.length || ids.some((id, index) => id !== sortedIds[index])) {
    throw new Error(`${field} IDs must be unique and deterministically ordered.`);
  }
}

function parseGraph(value: unknown, manifest: GraphManifest): CorpusGraphRelease {
  const graph = exact(
    value,
    [
      'schema_version',
      'kind',
      'generated_at',
      'counts',
      'entities',
      'events',
      'appearances',
      'event_participants',
      'event_dates',
      'event_relations',
      'event_evidence',
    ],
    'entity/event graph',
  );
  if (graph.schema_version !== 1 || graph.kind !== 'entity_event_graph') {
    throw new Error('Unsupported entity/event graph schema or kind.');
  }
  const generatedAt = timestamp(graph.generated_at, 'graph.generated_at');
  const counts = parseCounts(graph.counts, 'graph.counts');
  if (generatedAt !== manifest.generated_at || canonicalJson(counts) !== canonicalJson(manifest.counts)) {
    throw new Error('Graph manifest metadata differs from its shard.');
  }
  for (const name of [
    'entities',
    'events',
    'appearances',
    'event_participants',
    'event_dates',
    'event_relations',
    'event_evidence',
  ]) {
    if (!Array.isArray(graph[name]) || graph[name].length > 10_000) {
      throw new Error(`graph.${name} must be a bounded array.`);
    }
  }

  const entities = (graph.entities as unknown[]).map((item, index): CorpusGraphEntity => {
    const field = `entities[${index}]`;
    const entity = exact(item, ['entity_id', 'slug', 'label', 'entity_type'], field);
    return {
      entity_id: identifier(entity.entity_id, `${field}.entity_id`),
      slug: slug(entity.slug, `${field}.slug`),
      label: text(entity.label, `${field}.label`, 512),
      entity_type: oneOf(entity.entity_type, entityTypes, `${field}.entity_type`),
    };
  });
  const events = (graph.events as unknown[]).map((item, index): CorpusGraphEvent => {
    const field = `events[${index}]`;
    const event = exact(item, ['event_id', 'slug', 'label'], field);
    return {
      event_id: identifier(event.event_id, `${field}.event_id`),
      slug: slug(event.slug, `${field}.slug`),
      label: text(event.label, `${field}.label`, 512),
    };
  });
  const appearances = (graph.appearances as unknown[]).map(
    (item, index): CorpusGraphAppearance => {
      const field = `appearances[${index}]`;
      const appearance = exact(
        item,
        ['appearance_id', 'entity_id', 'label', 'start_ms', 'end_ms', 'anchor'],
        field,
      );
      const startMs = integer(appearance.start_ms, `${field}.start_ms`);
      const endMs = integer(appearance.end_ms, `${field}.end_ms`, 1);
      if (endMs <= startMs) throw new Error(`${field} has an invalid interval.`);
      return {
        appearance_id: identifier(appearance.appearance_id, `${field}.appearance_id`),
        entity_id: identifier(appearance.entity_id, `${field}.entity_id`),
        label: text(appearance.label, `${field}.label`, 512),
        start_ms: startMs,
        end_ms: endMs,
        anchor: parseAnchor(appearance.anchor, `${field}.anchor`),
      };
    },
  );
  const eventParticipants = (graph.event_participants as unknown[]).map(
    (item, index): CorpusGraphParticipant => {
      const field = `event_participants[${index}]`;
      const participant = exact(
        item,
        ['event_participant_id', 'event_id', 'entity_id', 'label'],
        field,
      );
      return {
        event_participant_id: identifier(
          participant.event_participant_id,
          `${field}.event_participant_id`,
        ),
        event_id: identifier(participant.event_id, `${field}.event_id`),
        entity_id: identifier(participant.entity_id, `${field}.entity_id`),
        label: text(participant.label, `${field}.label`, 512),
      };
    },
  );
  const eventDates = (graph.event_dates as unknown[]).map((item, index): CorpusGraphDate => {
    const field = `event_dates[${index}]`;
    const date = exact(
      item,
      ['event_date_id', 'event_id', 'label', 'value_start', 'value_end', 'precision', 'certainty'],
      field,
    );
    const start = nullableDate(date.value_start, `${field}.value_start`);
    const end = nullableDate(date.value_end, `${field}.value_end`);
    const precision = oneOf(date.precision, datePrecisions, `${field}.precision`);
    if ((precision === 'unknown') !== (start === null && end === null)) {
      throw new Error(`${field} has inconsistent unknown precision.`);
    }
    if (end !== null && (start === null || start.length !== end.length || end < start)) {
      throw new Error(`${field} has an invalid range.`);
    }
    return {
      event_date_id: identifier(date.event_date_id, `${field}.event_date_id`),
      event_id: identifier(date.event_id, `${field}.event_id`),
      label: text(date.label, `${field}.label`, 512),
      value_start: start,
      value_end: end,
      precision,
      certainty: oneOf(date.certainty, dateCertainties, `${field}.certainty`),
    };
  });
  const eventRelations = (graph.event_relations as unknown[]).map(
    (item, index): CorpusGraphRelation => {
      const field = `event_relations[${index}]`;
      const relation = exact(
        item,
        ['event_relation_id', 'from_event_id', 'to_event_id', 'label'],
        field,
      );
      return {
        event_relation_id: identifier(relation.event_relation_id, `${field}.event_relation_id`),
        from_event_id: identifier(relation.from_event_id, `${field}.from_event_id`),
        to_event_id: identifier(relation.to_event_id, `${field}.to_event_id`),
        label: text(relation.label, `${field}.label`, 512),
      };
    },
  );
  const eventEvidence = (graph.event_evidence as unknown[]).map(
    (item, index): CorpusGraphEvidence => {
      const field = `event_evidence[${index}]`;
      const evidence = exact(
        item,
        ['event_evidence_id', 'event_id', 'label', 'support_kind', 'start_ms', 'end_ms', 'anchor'],
        field,
      );
      const startMs = evidence.start_ms === null ? null : integer(evidence.start_ms, `${field}.start_ms`);
      const endMs = evidence.end_ms === null ? null : integer(evidence.end_ms, `${field}.end_ms`, 1);
      if ((startMs === null) !== (endMs === null) || (startMs !== null && endMs! <= startMs)) {
        throw new Error(`${field} has an invalid optional interval.`);
      }
      return {
        event_evidence_id: identifier(evidence.event_evidence_id, `${field}.event_evidence_id`),
        event_id: identifier(evidence.event_id, `${field}.event_id`),
        label: text(evidence.label, `${field}.label`, 512),
        support_kind: oneOf(evidence.support_kind, supportKinds, `${field}.support_kind`),
        start_ms: startMs,
        end_ms: endMs,
        anchor: parseAnchor(evidence.anchor, `${field}.anchor`),
      };
    },
  );

  assertOrderedUnique(entities, 'entity_id', 'entities');
  assertOrderedUnique(events, 'event_id', 'events');
  assertOrderedUnique(appearances, 'appearance_id', 'appearances');
  assertOrderedUnique(eventParticipants, 'event_participant_id', 'event_participants');
  assertOrderedUnique(eventDates, 'event_date_id', 'event_dates');
  assertOrderedUnique(eventRelations, 'event_relation_id', 'event_relations');
  assertOrderedUnique(eventEvidence, 'event_evidence_id', 'event_evidence');
  if (new Set(entities.map((item) => item.slug)).size !== entities.length) {
    throw new Error('Entity slugs must be unique.');
  }
  if (new Set(events.map((item) => item.slug)).size !== events.length) {
    throw new Error('Event slugs must be unique.');
  }
  const entityIds = new Set(entities.map((item) => item.entity_id));
  const eventIds = new Set(events.map((item) => item.event_id));
  const participantEvents = new Set<string>();
  const evidenceEvents = new Set<string>();
  const referencedEntities = new Set<string>();
  const recordings = new Map<string, string>();
  const sources = new Map<string, string>();
  for (const item of [...appearances, ...eventEvidence]) {
    const recording = canonicalJson(item.anchor.recording);
    const source = canonicalJson(item.anchor.source);
    const priorRecording = recordings.get(item.anchor.recording.recording_id);
    const priorSource = sources.get(item.anchor.source.source_id);
    if ((priorRecording && priorRecording !== recording) || (priorSource && priorSource !== source)) {
      throw new Error('Graph anchors contain conflicting public snapshots.');
    }
    recordings.set(item.anchor.recording.recording_id, recording);
    sources.set(item.anchor.source.source_id, source);
  }
  for (const appearance of appearances) {
    if (!entityIds.has(appearance.entity_id)) throw new Error('Appearance references a missing entity.');
    referencedEntities.add(appearance.entity_id);
  }
  for (const participant of eventParticipants) {
    if (!eventIds.has(participant.event_id) || !entityIds.has(participant.entity_id)) {
      throw new Error('Participant references a missing graph root.');
    }
    participantEvents.add(participant.event_id);
    referencedEntities.add(participant.entity_id);
  }
  for (const date of eventDates) {
    if (!eventIds.has(date.event_id)) throw new Error('Date references a missing event.');
  }
  for (const relation of eventRelations) {
    if (
      relation.from_event_id === relation.to_event_id ||
      !eventIds.has(relation.from_event_id) ||
      !eventIds.has(relation.to_event_id)
    ) {
      throw new Error('Relation has invalid graph endpoints.');
    }
  }
  const relationGraph = new Map([...eventIds].map((id) => [id, new Set<string>()]));
  for (const relation of eventRelations) {
    relationGraph.get(relation.from_event_id)!.add(relation.to_event_id);
  }
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (eventId: string): void => {
    if (visiting.has(eventId)) throw new Error('Public event relations contain a directed cycle.');
    if (visited.has(eventId)) return;
    visiting.add(eventId);
    for (const targetId of relationGraph.get(eventId)!) visit(targetId);
    visiting.delete(eventId);
    visited.add(eventId);
  };
  for (const eventId of [...eventIds].sort()) visit(eventId);
  for (const evidence of eventEvidence) {
    if (!eventIds.has(evidence.event_id)) throw new Error('Evidence references a missing event.');
    evidenceEvents.add(evidence.event_id);
  }
  if (
    [...eventIds].some((id) => !participantEvents.has(id) || !evidenceEvents.has(id)) ||
    [...entityIds].some((id) => !referencedEntities.has(id))
  ) {
    throw new Error('Graph roots must have independently published edge support.');
  }
  const observedCounts: CorpusGraphCounts = {
    entities: entities.length,
    events: events.length,
    appearances: appearances.length,
    event_participants: eventParticipants.length,
    event_dates: eventDates.length,
    event_relations: eventRelations.length,
    event_evidence: eventEvidence.length,
  };
  if (canonicalJson(observedCounts) !== canonicalJson(counts)) {
    throw new Error('Graph counts do not match its collections.');
  }
  if (Object.values(observedCounts).reduce((total, count) => total + count, 0) > 50_000) {
    throw new Error('Graph exceeds the total item bound.');
  }
  return {
    schemaVersion: 1,
    releaseId: manifest.release_id,
    generatedAt,
    counts,
    entities,
    events,
    appearances,
    eventParticipants,
    eventDates,
    eventRelations,
    eventEvidence,
  };
}

async function readGraph(): Promise<CorpusGraphRelease> {
  let derivedConfig: {corpus_release:string;summary_release:string}|undefined;
  if(!localPreviewRoot()){
    try{derivedConfig=JSON.parse(await readFile(path.join(graphDataRoot,'derived-config.json'),'utf8'));}
    catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}
  }
  if (localPreviewRoot() || derivedConfig) {
    const catalog=await loadCorpusCatalog();
    const summaries=await loadSummaryRelease();
    if(derivedConfig&&(derivedConfig.corpus_release!==catalog.releaseId||derivedConfig.summary_release!==summaries.release_id))throw new Error('Derived graph release binding differs');
    const builder=createDerivedGraph(catalog,summaries.summaries);
    for (const row of catalog.recordings) {
      if (row.transcript_revision_count > 0) builder.addRecording(await loadCorpusRecording(row));
    }
    const graph=builder.finish();
    const file=localPreviewRoot()?path.join(localPreviewRoot()!, 'event-groups.json'):path.join(graphDataRoot,'event-groups.json');
    try {
      const stat=await lstat(file);
      if(!stat.isFile()||stat.isSymbolicLink()||stat.size>16*1024*1024)throw new Error('Unsafe event grouping artifact');
      return attachEventGroups(graph,JSON.parse(await readFile(file,'utf8'))) as CorpusGraphRelease;
    } catch(error) {
      if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;
    }
    return graph as CorpusGraphRelease;
  }
  const manifestStatus = await lstat(graphManifestFile);
  if (manifestStatus.isSymbolicLink() || !manifestStatus.isFile()) {
    throw new Error('Graph manifest must be a regular non-symlink file.');
  }
  const manifest = parseManifest(JSON.parse(await readFile(graphManifestFile, 'utf8')) as unknown);
  const releaseRoot = path.resolve(graphDataRoot, 'releases', manifest.release_id);
  const releaseStatus = await lstat(releaseRoot);
  if (releaseStatus.isSymbolicLink() || !releaseStatus.isDirectory()) {
    throw new Error('Graph release directory is missing or unsafe.');
  }
  const shardPath = path.resolve(releaseRoot, ...manifest.graph_shard.path.split('/'));
  if (!shardPath.startsWith(`${releaseRoot}${path.sep}`)) {
    throw new Error('Graph shard escapes its release directory.');
  }
  const before = await lstat(shardPath);
  if (before.isSymbolicLink() || !before.isFile()) {
    throw new Error('Graph shard must be a regular non-symlink file.');
  }
  const bytes = await readFile(shardPath);
  const after = await lstat(shardPath);
  if (
    before.dev !== after.dev ||
    before.ino !== after.ino ||
    before.size !== after.size ||
    before.mtimeMs !== after.mtimeMs ||
    bytes.byteLength !== manifest.graph_shard.bytes ||
    sha256(bytes) !== manifest.graph_shard.sha256
  ) {
    throw new Error('Graph shard failed its stable byte-count/SHA-256 check.');
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(bytes.toString('utf8')) as unknown;
  } catch (error) {
    throw new Error('Graph shard is invalid UTF-8 JSON.', { cause: error });
  }
  return parseGraph(parsed, manifest);
}

/** Load the isolated, integrity-checked public entity/event graph. */
export function loadCorpusGraph(): Promise<CorpusGraphRelease> {
  cachedGraph ??= readGraph();
  return cachedGraph;
}
