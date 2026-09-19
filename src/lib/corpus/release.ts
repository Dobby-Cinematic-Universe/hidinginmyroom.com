import { createHash } from "node:crypto";
import { lstat, readFile } from "node:fs/promises";
import path from "node:path";
import { localPreviewRoot } from '../local-preview';

import type {
  CorpusFacets,
  CorpusCatalog,
  CorpusRecording,
  CorpusRecordingSummary,
  CorpusRelease,
  CorpusSegment,
  CorpusShardReference,
  CorpusSource,
  CorpusStats,
  TranscriptLifecycleEntry,
  TranscriptRevision,
} from "./types";

// Astro bundles this module before executing getStaticPaths. Resolving relative to
// import.meta.url would therefore point into the generated server chunk rather than
// the source tree. Cloudflare and local builds both run with the repository root as
// the working directory.
const previewRoot = localPreviewRoot();
const releaseDataRoot = previewRoot ? path.join(previewRoot, 'corpus') : process.env.HIMR_CORPUS_DATA_ROOT
  ? path.resolve(process.env.HIMR_CORPUS_DATA_ROOT)
  : path.resolve(process.cwd(), "src", "data", "corpus");
const releaseFile = path.join(releaseDataRoot, "release.json");
const shardedManifestFile = path.join(releaseDataRoot, "manifest.json");

const emptyRelease: CorpusRelease = {
  schema_version: "1",
  release_id: "empty",
  generated_at: "",
  counts: {},
  recordings: [],
};

const recordingIdPattern = /^rec_[a-f0-9]{32}$/;
const sourceIdPattern = /^src_[a-f0-9]{32}$/;
const revisionIdPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
const segmentIdPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
const releaseIdPattern = /^release_[a-f0-9]{24}$/;
const utcTimestampPattern =
  /^(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?Z$/;
const recordingTypes = new Set([
  "video",
  "livestream",
  "short",
  "guest_appearance",
  "compilation",
  "unknown",
]);
const recordingReviewStates = new Set([
  "metadata_only",
  "unreviewed",
  "reviewed",
  "disputed",
]);
const transcriptReviewStates = new Set([
  "machine",
  "human_corrected",
  "media_checked",
  "disputed",
]);
const transcriptRevisionKinds = new Set([
  "raw_asr",
  "contextual_asr",
  "human_verbatim",
  "readability_edit",
]);
const transcriptLifecycleStates = new Set([
  "active",
  "retracted",
  "disputed",
  "reinstated",
]);
const transcriptLifecycleHistoryStates = new Set([
  "retracted",
  "disputed",
  "reinstated",
]);
const transcriptLifecycleReasonCodes = new Set([
  "transcription_error",
  "speaker_misattribution",
  "source_mismatch",
  "privacy",
  "rights",
  "sensitivity",
  "editorial_decision",
  "other",
]);
const transcriptDisclaimerCodes = new Set([
  "machine_generated_unreviewed_not_verified_quotation_v1",
  "disputed_transcript_not_verified_quotation_v1",
  "reviewed_transcript_not_fact_checked_v1",
  "retracted_transcript_text_withdrawn_v1",
]);
const confidenceBands = new Set(["low", "medium", "high", "human"]);

let cachedRelease: Promise<CorpusRelease> | undefined;
let cachedCatalog: Promise<CorpusCatalog> | undefined;

interface CatalogShardDescriptor extends CorpusShardReference {
  recording_count: number;
  source_count: number;
  transcript_revision_count: number;
  segment_count: number;
  first_recording_id: string;
  last_recording_id: string;
}

interface ShardedManifest {
  schema_version: 2;
  release_id: string;
  generated_at: string;
  counts: Record<string, number>;
  catalog_shard_size: number;
  stats: {
    duration_ms: number;
    searchable_transcript_segments: number;
    source_listings: number;
    transcript_revisions: number;
  };
  facets: {
    platforms: string[];
    years: string[];
    languages: string[];
    speakers: string[];
    review_states: string[];
    confidence_bands: string[];
    recording_types: string[];
  };
  catalog_shards: CatalogShardDescriptor[];
}

let activeManifest: ShardedManifest | undefined;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assertExactKeys(
  value: Record<string, unknown>,
  expected: string[],
  field: string,
): void {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (
    actual.length !== wanted.length ||
    actual.some((key, index) => key !== wanted[index])
  ) {
    throw new Error(
      `Corpus release object ${field} has unexpected or missing fields.`,
    );
  }
}

function identifier(value: unknown, field: string, pattern: RegExp): string {
  const result = requiredString(value, field);
  if (!pattern.test(result))
    throw new Error(`Corpus release field ${field} has an invalid ID.`);
  return result;
}

function enumString(
  value: unknown,
  field: string,
  allowed: Set<string>,
): string {
  const result = requiredString(value, field);
  if (!allowed.has(result))
    throw new Error(`Corpus release field ${field} is not allowed.`);
  return result;
}

function requiredString(
  value: unknown,
  field: string,
  { allowEmpty = false }: { allowEmpty?: boolean } = {},
): string {
  if (typeof value !== "string" || (!allowEmpty && value.trim() === "")) {
    throw new Error(
      `Corpus release field ${field} must be a${allowEmpty ? "" : " non-empty"} string.`,
    );
  }
  return value;
}

function explicitUtcTimestamp(
  value: unknown,
  field: string,
): { text: string; milliseconds: number } {
  const text = requiredString(value, field);
  const parsed = new Date(text);
  if (
    !utcTimestampPattern.test(text) ||
    Number.isNaN(parsed.getTime()) ||
    parsed.toISOString().slice(0, 19) !== text.slice(0, 19)
  ) {
    throw new Error(
      `Corpus release field ${field} must be a valid explicit UTC timestamp.`,
    );
  }
  return { text, milliseconds: parsed.getTime() };
}

function requiredNumber(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Corpus release field ${field} must be a finite number.`);
  }
  return value;
}

function requiredInteger(value: unknown, field: string): number {
  const result = requiredNumber(value, field);
  if (!Number.isInteger(result))
    throw new Error(`Corpus release field ${field} must be an integer.`);
  return result;
}

function requiredBoolean(value: unknown, field: string): boolean {
  if (typeof value !== "boolean") {
    throw new Error(`Corpus release field ${field} must be a boolean.`);
  }
  return value;
}

function nullableNumber(value: unknown, field: string): number | null {
  if (value === null || typeof value === "undefined") return null;
  return requiredNumber(value, field);
}

function nullableString(value: unknown, field: string): string | null {
  if (value === null || typeof value === "undefined") return null;
  return requiredString(value, field, { allowEmpty: true });
}

function schemaVersion(value: unknown): string | number {
  if (value === 1) return value;
  throw new Error(
    "Corpus release field schema_version must be the supported version 1.",
  );
}

function parseSource(
  value: unknown,
  recordingIndex: number,
  sourceIndex: number,
): CorpusSource {
  if (!isRecord(value))
    throw new Error("Each corpus source must be an object.");
  const prefix = `recordings[${recordingIndex}].sources[${sourceIndex}]`;
  assertExactKeys(
    value,
    ["source_id", "platform", "url", "native_id", "access_state"],
    prefix,
  );
  const url = requiredString(value.url, `${prefix}.url`);
  let parsedURL: URL;
  try {
    parsedURL = new URL(url);
  } catch {
    throw new Error(`Corpus source URL is invalid at ${prefix}.url.`);
  }
  if (
    !["http:", "https:"].includes(parsedURL.protocol) ||
    !parsedURL.hostname ||
    parsedURL.username !== "" ||
    parsedURL.password !== ""
  ) {
    throw new Error(
      `Corpus source URL must be uncredentialed HTTP or HTTPS at ${prefix}.url.`,
    );
  }

  return {
    source_id: identifier(
      value.source_id,
      `${prefix}.source_id`,
      sourceIdPattern,
    ),
    platform: requiredString(value.platform, `${prefix}.platform`),
    url,
    native_id: requiredString(value.native_id, `${prefix}.native_id`),
    access_state: enumString(
      value.access_state,
      `${prefix}.access_state`,
      new Set(["public"]),
    ),
  };
}

function parseSegment(
  value: unknown,
  recordingIndex: number,
  revisionIndex: number,
  segmentIndex: number,
): CorpusSegment {
  if (!isRecord(value))
    throw new Error("Each corpus transcript segment must be an object.");
  const prefix =
    `recordings[${recordingIndex}].transcript_revisions[${revisionIndex}]` +
    `.segments[${segmentIndex}]`;
  assertExactKeys(
    value,
    [
      "segment_id",
      "start_ms",
      "end_ms",
      "text",
      "speaker_label",
      "confidence_band",
      "calibrated_probability",
    ],
    prefix,
  );
  const startMs = requiredInteger(value.start_ms, `${prefix}.start_ms`);
  const endMs = requiredInteger(value.end_ms, `${prefix}.end_ms`);
  if (startMs < 0 || endMs <= startMs) {
    throw new Error(`Corpus segment has an invalid time range at ${prefix}.`);
  }
  const probability = nullableNumber(
    value.calibrated_probability,
    `${prefix}.calibrated_probability`,
  );
  if (probability !== null && (probability < 0 || probability > 1)) {
    throw new Error(
      `Calibrated probability must be between 0 and 1 at ${prefix}.`,
    );
  }
  const confidenceBand = nullableString(
    value.confidence_band,
    `${prefix}.confidence_band`,
  );
  if (confidenceBand !== null && !confidenceBands.has(confidenceBand)) {
    throw new Error(
      `Corpus release field ${prefix}.confidence_band is not allowed.`,
    );
  }

  return {
    segment_id: identifier(
      value.segment_id,
      `${prefix}.segment_id`,
      segmentIdPattern,
    ),
    start_ms: startMs,
    end_ms: endMs,
    text: requiredString(value.text, `${prefix}.text`, { allowEmpty: true }),
    speaker_label: nullableString(
      value.speaker_label,
      `${prefix}.speaker_label`,
    ),
    confidence_band: confidenceBand,
    calibrated_probability: probability,
  };
}

function parseRevision(
  value: unknown,
  recordingIndex: number,
  revisionIndex: number,
): TranscriptRevision {
  if (!isRecord(value))
    throw new Error("Each corpus transcript revision must be an object.");
  const prefix = `recordings[${recordingIndex}].transcript_revisions[${revisionIndex}]`;
  assertExactKeys(
    value,
    [
      "revision_id",
      "revision_kind",
      "language",
      "review_state",
      "machine_generated",
      "unreviewed",
      "verified_quotation",
      "disclaimer_code",
      "lifecycle_state",
      "lifecycle_history",
      "segments",
    ],
    prefix,
  );
  if (!Array.isArray(value.segments)) {
    throw new Error(
      `Corpus release field ${prefix}.segments must be an array.`,
    );
  }
  if (!Array.isArray(value.lifecycle_history)) {
    throw new Error(
      `Corpus release field ${prefix}.lifecycle_history must be an array.`,
    );
  }

  const revisionKind = enumString(
    value.revision_kind,
    `${prefix}.revision_kind`,
    transcriptRevisionKinds,
  );
  const reviewState = enumString(
    value.review_state,
    `${prefix}.review_state`,
    transcriptReviewStates,
  );
  const machineGenerated = requiredBoolean(
    value.machine_generated,
    `${prefix}.machine_generated`,
  );
  const unreviewed = requiredBoolean(value.unreviewed, `${prefix}.unreviewed`);
  const verifiedQuotation = requiredBoolean(
    value.verified_quotation,
    `${prefix}.verified_quotation`,
  );
  if (verifiedQuotation) {
    throw new Error(
      `Corpus transcript ${prefix} must not claim to be a verified quotation.`,
    );
  }
  const expectedMachineGenerated =
    reviewState === "machine" ||
    ["raw_asr", "contextual_asr"].includes(revisionKind);
  if (machineGenerated !== expectedMachineGenerated) {
    throw new Error(
      `Corpus transcript ${prefix} has inconsistent machine provenance.`,
    );
  }
  if (unreviewed !== (reviewState === "machine")) {
    throw new Error(
      `Corpus transcript ${prefix} has inconsistent unreviewed status.`,
    );
  }

  let priorDecision: number | undefined;
  let priorLifecycleState = "active";
  const lifecycleTransitions: Record<string, Set<string>> = {
    active: new Set(["disputed", "retracted"]),
    disputed: new Set(["retracted", "reinstated"]),
    retracted: new Set(["reinstated"]),
    reinstated: new Set(["disputed", "retracted"]),
  };
  const lifecycleHistory = value.lifecycle_history.map(
    (entry, lifecycleIndex): TranscriptLifecycleEntry => {
      const lifecyclePrefix = `${prefix}.lifecycle_history[${lifecycleIndex}]`;
      if (!isRecord(entry))
        throw new Error(
          `Corpus transcript lifecycle ${lifecyclePrefix} must be an object.`,
        );
      assertExactKeys(
        entry,
        ["state", "reason_code", "decided_at", "explanation"],
        lifecyclePrefix,
      );
      const decidedAt = explicitUtcTimestamp(
        entry.decided_at,
        `${lifecyclePrefix}.decided_at`,
      );
      if (
        priorDecision !== undefined &&
        decidedAt.milliseconds <= priorDecision
      ) {
        throw new Error(
          `Corpus transcript lifecycle ${lifecyclePrefix} is not valid chronological UTC.`,
        );
      }
      priorDecision = decidedAt.milliseconds;
      const lifecycleState = enumString(
        entry.state,
        `${lifecyclePrefix}.state`,
        transcriptLifecycleHistoryStates,
      );
      if (!lifecycleTransitions[priorLifecycleState]?.has(lifecycleState)) {
        throw new Error(
          `Corpus transcript lifecycle ${lifecyclePrefix} has an illegal transition.`,
        );
      }
      priorLifecycleState = lifecycleState;
      const explanation = requiredString(
        entry.explanation,
        `${lifecyclePrefix}.explanation`,
      );
      if (explanation.length > 2048) {
        throw new Error(
          `Corpus transcript lifecycle ${lifecyclePrefix} explanation is too long.`,
        );
      }
      return {
        state: lifecycleState,
        reason_code: enumString(
          entry.reason_code,
          `${lifecyclePrefix}.reason_code`,
          transcriptLifecycleReasonCodes,
        ),
        decided_at: decidedAt.text,
        explanation,
      };
    },
  );
  const lifecycleState = enumString(
    value.lifecycle_state,
    `${prefix}.lifecycle_state`,
    transcriptLifecycleStates,
  );
  const expectedLifecycle = priorLifecycleState;
  if (lifecycleState !== expectedLifecycle) {
    throw new Error(
      `Corpus transcript ${prefix} lifecycle state differs from its history.`,
    );
  }
  const disclaimerCode = enumString(
    value.disclaimer_code,
    `${prefix}.disclaimer_code`,
    transcriptDisclaimerCodes,
  );
  const expectedDisclaimer =
    lifecycleState === "retracted"
      ? "retracted_transcript_text_withdrawn_v1"
      : lifecycleState === "disputed" || reviewState === "disputed"
        ? "disputed_transcript_not_verified_quotation_v1"
        : reviewState === "machine"
          ? "machine_generated_unreviewed_not_verified_quotation_v1"
          : "reviewed_transcript_not_fact_checked_v1";
  if (disclaimerCode !== expectedDisclaimer) {
    throw new Error(
      `Corpus transcript ${prefix} disclaimer differs from its state.`,
    );
  }
  if (lifecycleState === "retracted" && value.segments.length > 0) {
    throw new Error(
      `Retracted corpus transcript ${prefix} must be a text-free tombstone.`,
    );
  }

  return {
    revision_id: identifier(
      value.revision_id,
      `${prefix}.revision_id`,
      revisionIdPattern,
    ),
    revision_kind: revisionKind,
    language: requiredString(value.language, `${prefix}.language`),
    review_state: reviewState,
    machine_generated: machineGenerated,
    unreviewed,
    verified_quotation: false,
    disclaimer_code: disclaimerCode,
    lifecycle_state: lifecycleState,
    lifecycle_history: lifecycleHistory,
    segments: value.segments.map((segment, segmentIndex) =>
      parseSegment(segment, recordingIndex, revisionIndex, segmentIndex),
    ),
  };
}

function parseRecording(
  value: unknown,
  recordingIndex: number,
): CorpusRecording {
  if (!isRecord(value))
    throw new Error("Each corpus recording must be an object.");
  const prefix = `recordings[${recordingIndex}]`;
  assertExactKeys(
    value,
    [
      "recording_id",
      "slug",
      "title",
      "date_label",
      "date_year",
      "date_basis",
      "duration_ms",
      "recording_type",
      "review_state",
      "sources",
      "transcript_revisions",
    ],
    prefix,
  );
  if (!Array.isArray(value.sources)) {
    throw new Error(`Corpus release field ${prefix}.sources must be an array.`);
  }
  if (value.sources.length === 0) {
    throw new Error(
      `Published corpus recording ${prefix} must have a public source.`,
    );
  }
  if (!Array.isArray(value.transcript_revisions)) {
    throw new Error(
      `Corpus release field ${prefix}.transcript_revisions must be an array.`,
    );
  }
  const slug = requiredString(value.slug, `${prefix}.slug`);
  if (!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(slug)) {
    throw new Error(
      `Corpus recording slug is not URL-safe at ${prefix}.slug: ${slug}`,
    );
  }
  const durationMs =
    value.duration_ms === null
      ? null
      : requiredInteger(value.duration_ms, `${prefix}.duration_ms`);
  if (durationMs !== null && durationMs < 0) {
    throw new Error(
      `Corpus duration cannot be negative at ${prefix}.duration_ms.`,
    );
  }

  return {
    recording_id: identifier(
      value.recording_id,
      `${prefix}.recording_id`,
      recordingIdPattern,
    ),
    slug,
    title: requiredString(value.title, `${prefix}.title`),
    date_label: nullableString(value.date_label, `${prefix}.date_label`),
    date_year:
      value.date_year === null
        ? null
        : requiredInteger(value.date_year, `${prefix}.date_year`),
    date_basis: requiredString(value.date_basis, `${prefix}.date_basis`),
    duration_ms: durationMs,
    recording_type: enumString(
      value.recording_type,
      `${prefix}.recording_type`,
      recordingTypes,
    ),
    review_state: enumString(
      value.review_state,
      `${prefix}.review_state`,
      recordingReviewStates,
    ),
    sources: value.sources.map((source, sourceIndex) =>
      parseSource(source, recordingIndex, sourceIndex),
    ),
    transcript_revisions: value.transcript_revisions.map(
      (revision, revisionIndex) =>
        parseRevision(revision, recordingIndex, revisionIndex),
    ),
  };
}

function parseRelease(value: unknown): CorpusRelease {
  if (!isRecord(value))
    throw new Error("Corpus release must be a JSON object.");
  assertExactKeys(
    value,
    ["schema_version", "release_id", "generated_at", "counts", "recordings"],
    "release",
  );
  if (!Array.isArray(value.recordings)) {
    throw new Error("Corpus release field recordings must be an array.");
  }
  if (!isRecord(value.counts))
    throw new Error("Corpus release field counts must be an object.");
  assertExactKeys(
    value.counts,
    ["recordings", "sources", "transcript_revisions", "segments"],
    "counts",
  );

  const counts: Record<string, number> = {};
  for (const [key, count] of Object.entries(value.counts)) {
    const parsed = requiredInteger(count, `counts.${key}`);
    if (parsed < 0)
      throw new Error(`Corpus release field counts.${key} cannot be negative.`);
    counts[key] = parsed;
  }

  const recordings = value.recordings.map(parseRecording);
  const seenIds = new Set<string>();
  const seenSlugs = new Set<string>();
  const seenRevisionIds = new Set<string>();
  const seenSegmentIds = new Set<string>();
  let sourceCount = 0;
  let revisionCount = 0;
  let segmentCount = 0;
  for (const recording of recordings) {
    if (seenIds.has(recording.recording_id)) {
      throw new Error(
        `Duplicate corpus recording ID: ${recording.recording_id}`,
      );
    }
    if (seenSlugs.has(recording.slug))
      throw new Error(`Duplicate corpus recording slug: ${recording.slug}`);
    seenIds.add(recording.recording_id);
    seenSlugs.add(recording.slug);
    const sourceIds = new Set<string>();
    for (const source of recording.sources) {
      if (sourceIds.has(source.source_id)) {
        throw new Error(
          `Duplicate corpus source ${source.source_id} on ${recording.recording_id}.`,
        );
      }
      sourceIds.add(source.source_id);
      sourceCount += 1;
    }
    for (const revision of recording.transcript_revisions) {
      if (seenRevisionIds.has(revision.revision_id)) {
        throw new Error(
          `Duplicate corpus transcript revision ID: ${revision.revision_id}`,
        );
      }
      seenRevisionIds.add(revision.revision_id);
      revisionCount += 1;
      let priorStartMs = -1;
      for (const segment of revision.segments) {
        if (seenSegmentIds.has(segment.segment_id)) {
          throw new Error(
            `Duplicate corpus transcript segment ID: ${segment.segment_id}`,
          );
        }
        if (segment.start_ms < priorStartMs) {
          throw new Error(
            `Corpus transcript revision ${revision.revision_id} is not time-ordered.`,
          );
        }
        priorStartMs = segment.start_ms;
        seenSegmentIds.add(segment.segment_id);
        segmentCount += 1;
      }
    }
  }

  if (
    counts.recordings !== recordings.length ||
    counts.sources !== sourceCount ||
    counts.transcript_revisions !== revisionCount ||
    counts.segments !== segmentCount
  ) {
    throw new Error(
      "Corpus release counts do not match the nested release records.",
    );
  }

  const releaseId = identifier(
    value.release_id,
    "release_id",
    releaseIdPattern,
  );
  const generatedAt = explicitUtcTimestamp(
    value.generated_at,
    "generated_at",
  ).text;

  return {
    schema_version: schemaVersion(value.schema_version),
    release_id: releaseId,
    generated_at: generatedAt,
    counts,
    recordings,
  };
}

async function readCorpusRelease(): Promise<CorpusRelease> {
  try {
    const source = await readFile(releaseFile, "utf8");
    return parseRelease(JSON.parse(source) as unknown);
  } catch (error) {
    if (isRecord(error) && error.code === "ENOENT") return emptyRelease;
    if (error instanceof SyntaxError) {
      throw new Error(`Corpus release JSON is invalid: ${error.message}`, {
        cause: error,
      });
    }
    throw error;
  }
}

export function loadCorpusRelease(): Promise<CorpusRelease> {
  cachedRelease ??= readCorpusRelease();
  return cachedRelease;
}

function canonicalJson(value: unknown): string {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  throw new Error(
    "Corpus manifest contains a value that cannot be canonicalized.",
  );
}

function sha256Hex(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

function stringArray(value: unknown, field: string): string[] {
  if (
    !Array.isArray(value) ||
    value.some((item) => typeof item !== "string" || item.length === 0)
  ) {
    throw new Error(
      `Corpus release field ${field} must be an array of non-empty strings.`,
    );
  }
  if (new Set(value).size !== value.length) {
    throw new Error(
      `Corpus release field ${field} must not contain duplicates.`,
    );
  }
  return [...value];
}

function parseShardReference(
  value: unknown,
  field: string,
  prefix: "catalog/" | "recordings/",
): CorpusShardReference {
  if (!isRecord(value))
    throw new Error(`Corpus release field ${field} must be an object.`);
  assertExactKeys(value, ["path", "sha256", "bytes"], field);
  const relativePath = requiredString(value.path, `${field}.path`);
  const pathPattern =
    prefix === "catalog/"
      ? /^catalog\/catalog-[0-9]{5}-[a-f0-9]{16}\.json$/
      : /^recordings\/rec_[a-f0-9]{32}-[a-f0-9]{16}\.json$/;
  if (!pathPattern.test(relativePath) || path.posix.isAbsolute(relativePath)) {
    throw new Error(`Corpus release field ${field}.path is unsafe.`);
  }
  const digest = requiredString(value.sha256, `${field}.sha256`);
  if (!/^[a-f0-9]{64}$/.test(digest)) {
    throw new Error(`Corpus release field ${field}.sha256 is invalid.`);
  }
  const bytes = requiredInteger(value.bytes, `${field}.bytes`);
  if (bytes <= 0)
    throw new Error(`Corpus release field ${field}.bytes must be positive.`);
  return { path: relativePath, sha256: digest, bytes };
}

function parseSummary(value: unknown, index: number): CorpusRecordingSummary {
  if (!isRecord(value))
    throw new Error("Each corpus recording summary must be an object.");
  const prefix = `catalog.recordings[${index}]`;
  assertExactKeys(
    value,
    [
      "recording_id",
      "slug",
      "title",
      "date_label",
      "date_year",
      "date_basis",
      "duration_ms",
      "recording_type",
      "review_state",
      "source_count",
      "transcript_revision_count",
      "segment_count",
      "searchable_segment_count",
      "platforms",
      "languages",
      "detail",
    ],
    prefix,
  );
  const slug = requiredString(value.slug, `${prefix}.slug`);
  if (!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(slug)) {
    throw new Error(
      `Corpus recording summary slug is invalid at ${prefix}.slug.`,
    );
  }
  const duration =
    value.duration_ms === null
      ? null
      : requiredInteger(value.duration_ms, `${prefix}.duration_ms`);
  if (duration !== null && duration < 0)
    throw new Error(`Negative duration at ${prefix}.duration_ms.`);
  const sourceCount = requiredInteger(
    value.source_count,
    `${prefix}.source_count`,
  );
  const revisionCount = requiredInteger(
    value.transcript_revision_count,
    `${prefix}.transcript_revision_count`,
  );
  const segmentCount = requiredInteger(
    value.segment_count,
    `${prefix}.segment_count`,
  );
  const searchableSegmentCount = requiredInteger(
    value.searchable_segment_count,
    `${prefix}.searchable_segment_count`,
  );
  if (
    [sourceCount, revisionCount, segmentCount, searchableSegmentCount].some(
      (count) => count < 0,
    )
  ) {
    throw new Error(`Corpus summary counts cannot be negative at ${prefix}.`);
  }
  if (sourceCount === 0)
    throw new Error(`Published corpus summary ${prefix} has no public source.`);
  return {
    recording_id: identifier(
      value.recording_id,
      `${prefix}.recording_id`,
      recordingIdPattern,
    ),
    slug,
    title: requiredString(value.title, `${prefix}.title`),
    date_label: nullableString(value.date_label, `${prefix}.date_label`),
    date_year:
      value.date_year === null
        ? null
        : requiredInteger(value.date_year, `${prefix}.date_year`),
    date_basis: requiredString(value.date_basis, `${prefix}.date_basis`),
    duration_ms: duration,
    recording_type: enumString(
      value.recording_type,
      `${prefix}.recording_type`,
      recordingTypes,
    ),
    review_state: enumString(
      value.review_state,
      `${prefix}.review_state`,
      recordingReviewStates,
    ),
    source_count: sourceCount,
    transcript_revision_count: revisionCount,
    segment_count: segmentCount,
    searchable_segment_count: searchableSegmentCount,
    platforms: stringArray(value.platforms, `${prefix}.platforms`),
    languages: stringArray(value.languages, `${prefix}.languages`),
    detail: parseShardReference(
      value.detail,
      `${prefix}.detail`,
      "recordings/",
    ),
  };
}

function parseShardedManifest(value: unknown): ShardedManifest {
  if (!isRecord(value))
    throw new Error("Corpus sharded manifest must be an object.");
  assertExactKeys(
    value,
    [
      "schema_version",
      "release_id",
      "generated_at",
      "counts",
      "catalog_shard_size",
      "stats",
      "facets",
      "catalog_shards",
    ],
    "manifest",
  );
  if (value.schema_version !== 2)
    throw new Error("Unsupported sharded corpus schema version.");
  if (!isRecord(value.counts))
    throw new Error("Corpus sharded manifest counts must be an object.");
  assertExactKeys(
    value.counts,
    ["recordings", "sources", "transcript_revisions", "segments"],
    "manifest.counts",
  );
  const counts: Record<string, number> = {};
  for (const [key, count] of Object.entries(value.counts)) {
    counts[key] = requiredInteger(count, `manifest.counts.${key}`);
    if (counts[key] < 0)
      throw new Error(`Manifest count ${key} cannot be negative.`);
  }
  const shardSize = requiredInteger(
    value.catalog_shard_size,
    "manifest.catalog_shard_size",
  );
  if (shardSize < 1 || shardSize > 1_000)
    throw new Error("Manifest catalog shard bound is invalid.");

  if (!isRecord(value.stats))
    throw new Error("Corpus sharded manifest stats must be an object.");
  assertExactKeys(
    value.stats,
    [
      "duration_ms",
      "searchable_transcript_segments",
      "source_listings",
      "transcript_revisions",
    ],
    "manifest.stats",
  );
  const stats = {
    duration_ms: requiredInteger(
      value.stats.duration_ms,
      "manifest.stats.duration_ms",
    ),
    searchable_transcript_segments: requiredInteger(
      value.stats.searchable_transcript_segments,
      "manifest.stats.searchable_transcript_segments",
    ),
    source_listings: requiredInteger(
      value.stats.source_listings,
      "manifest.stats.source_listings",
    ),
    transcript_revisions: requiredInteger(
      value.stats.transcript_revisions,
      "manifest.stats.transcript_revisions",
    ),
  };
  if (Object.values(stats).some((count) => count < 0))
    throw new Error("Manifest stats cannot be negative.");

  if (!isRecord(value.facets))
    throw new Error("Corpus sharded manifest facets must be an object.");
  assertExactKeys(
    value.facets,
    [
      "platforms",
      "years",
      "languages",
      "speakers",
      "review_states",
      "confidence_bands",
      "recording_types",
    ],
    "manifest.facets",
  );
  const facets = {
    platforms: stringArray(value.facets.platforms, "manifest.facets.platforms"),
    years: stringArray(value.facets.years, "manifest.facets.years"),
    languages: stringArray(value.facets.languages, "manifest.facets.languages"),
    speakers: stringArray(value.facets.speakers, "manifest.facets.speakers"),
    review_states: stringArray(
      value.facets.review_states,
      "manifest.facets.review_states",
    ),
    confidence_bands: stringArray(
      value.facets.confidence_bands,
      "manifest.facets.confidence_bands",
    ),
    recording_types: stringArray(
      value.facets.recording_types,
      "manifest.facets.recording_types",
    ),
  };

  if (!Array.isArray(value.catalog_shards))
    throw new Error("Manifest catalog_shards must be an array.");
  const catalogShards = value.catalog_shards.map(
    (item, index): CatalogShardDescriptor => {
      if (!isRecord(item))
        throw new Error(`Catalog descriptor ${index} must be an object.`);
      assertExactKeys(
        item,
        [
          "path",
          "sha256",
          "bytes",
          "recording_count",
          "source_count",
          "transcript_revision_count",
          "segment_count",
          "first_recording_id",
          "last_recording_id",
        ],
        `manifest.catalog_shards[${index}]`,
      );
      const reference = parseShardReference(
        { path: item.path, sha256: item.sha256, bytes: item.bytes },
        `manifest.catalog_shards[${index}]`,
        "catalog/",
      );
      const descriptor = {
        ...reference,
        recording_count: requiredInteger(
          item.recording_count,
          `manifest.catalog_shards[${index}].recording_count`,
        ),
        source_count: requiredInteger(
          item.source_count,
          `manifest.catalog_shards[${index}].source_count`,
        ),
        transcript_revision_count: requiredInteger(
          item.transcript_revision_count,
          `manifest.catalog_shards[${index}].transcript_revision_count`,
        ),
        segment_count: requiredInteger(
          item.segment_count,
          `manifest.catalog_shards[${index}].segment_count`,
        ),
        first_recording_id: identifier(
          item.first_recording_id,
          `manifest.catalog_shards[${index}].first_recording_id`,
          recordingIdPattern,
        ),
        last_recording_id: identifier(
          item.last_recording_id,
          `manifest.catalog_shards[${index}].last_recording_id`,
          recordingIdPattern,
        ),
      };
      if (
        descriptor.recording_count < 1 ||
        descriptor.recording_count > shardSize ||
        descriptor.source_count < 0 ||
        descriptor.transcript_revision_count < 0 ||
        descriptor.segment_count < 0
      ) {
        throw new Error(`Catalog descriptor ${index} has invalid counts.`);
      }
      return descriptor;
    },
  );

  const releaseId = identifier(
    value.release_id,
    "manifest.release_id",
    releaseIdPattern,
  );
  const generatedAt = requiredString(
    value.generated_at,
    "manifest.generated_at",
  );
  const parsedGeneratedAt = new Date(generatedAt);
  if (
    !utcTimestampPattern.test(generatedAt) ||
    Number.isNaN(parsedGeneratedAt.getTime()) ||
    parsedGeneratedAt.toISOString().slice(0, 19) !== generatedAt.slice(0, 19)
  ) {
    throw new Error("Corpus sharded manifest generated_at must be valid UTC.");
  }
  const manifest: ShardedManifest = {
    schema_version: 2,
    release_id: releaseId,
    generated_at: generatedAt,
    counts,
    catalog_shard_size: shardSize,
    stats,
    facets,
    catalog_shards: catalogShards,
  };
  const identityPayload = Object.fromEntries(
    Object.entries(manifest).filter(([key]) => key !== "release_id"),
  );
  const expectedId = `release_${sha256Hex(canonicalJson(identityPayload)).slice(0, 24)}`;
  if (releaseId !== expectedId) {
    throw new Error(
      `Corpus sharded release identity mismatch: expected ${expectedId}.`,
    );
  }
  return manifest;
}

async function readVerifiedShard(
  manifest: ShardedManifest,
  reference: CorpusShardReference,
): Promise<unknown> {
  const root = path.resolve(releaseDataRoot, "releases", manifest.release_id);
  const candidate = path.resolve(root, ...reference.path.split("/"));
  if (candidate === root || !candidate.startsWith(`${root}${path.sep}`)) {
    throw new Error(
      `Corpus shard path escapes release directory: ${reference.path}`,
    );
  }
  const before = await lstat(candidate);
  if (!before.isFile() || before.isSymbolicLink()) {
    throw new Error(
      `Corpus shard is not a regular non-symlink file: ${reference.path}`,
    );
  }
  const payload = await readFile(candidate);
  const after = await lstat(candidate);
  if (
    before.dev !== after.dev ||
    before.ino !== after.ino ||
    before.size !== after.size ||
    before.mtimeMs !== after.mtimeMs
  ) {
    throw new Error(
      `Corpus shard changed while it was being read: ${reference.path}`,
    );
  }
  if (
    payload.byteLength !== reference.bytes ||
    sha256Hex(payload) !== reference.sha256
  ) {
    throw new Error(
      `Corpus shard failed its byte count or SHA-256 check: ${reference.path}`,
    );
  }
  try {
    return JSON.parse(payload.toString("utf8")) as unknown;
  } catch (error) {
    throw new Error(`Corpus shard is not valid UTF-8 JSON: ${reference.path}`, {
      cause: error,
    });
  }
}

function summaryFromLegacy(recording: CorpusRecording): CorpusRecordingSummary {
  const searchable = searchableTranscripts(recording);
  return {
    recording_id: recording.recording_id,
    slug: recording.slug,
    title: recording.title,
    date_label: recording.date_label,
    date_year: recording.date_year,
    date_basis: recording.date_basis,
    duration_ms: recording.duration_ms,
    recording_type: recording.recording_type,
    review_state: recording.review_state,
    source_count: recording.sources.length,
    transcript_revision_count: recording.transcript_revisions.length,
    segment_count: recording.transcript_revisions.reduce(
      (count, revision) => count + revision.segments.length,
      0,
    ),
    searchable_segment_count: searchable.reduce(
      (count, revision) =>
        count +
        revision.segments.filter((segment) => segment.text.trim()).length,
      0,
    ),
    platforms: [
      ...new Set(recording.sources.map((source) => source.platform)),
    ].sort(),
    languages: [
      ...new Set(searchable.map((revision) => revision.language)),
    ].sort(),
  };
}

async function readCorpusCatalog(): Promise<CorpusCatalog> {
  let manifestSource: string;
  try {
    manifestSource = await readFile(shardedManifestFile, "utf8");
  } catch (error) {
    if (!isRecord(error) || error.code !== "ENOENT") throw error;
    const release = await loadCorpusRelease();
    return {
      schemaVersion: 1,
      releaseId: release.release_id,
      generatedAt: release.generated_at,
      counts: release.counts,
      stats: corpusStats(release),
      facets: corpusFacets(release),
      recordings: sortedRecordings(release).map(summaryFromLegacy),
    };
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(manifestSource) as unknown;
  } catch (error) {
    throw new Error("Corpus sharded manifest is invalid JSON.", {
      cause: error,
    });
  }
  const manifest = parseShardedManifest(parsed);
  activeManifest = manifest;
  const recordings: CorpusRecordingSummary[] = [];
  const seenIds = new Set<string>();
  const seenSlugs = new Set<string>();
  let sources = 0;
  let revisions = 0;
  let segments = 0;
  for (const [ordinal, descriptor] of manifest.catalog_shards.entries()) {
    const value = await readVerifiedShard(manifest, descriptor);
    if (!isRecord(value))
      throw new Error(`Corpus catalog shard ${ordinal} must be an object.`);
    assertExactKeys(
      value,
      ["schema_version", "kind", "ordinal", "recordings"],
      `catalog shard ${ordinal}`,
    );
    if (
      value.schema_version !== 2 ||
      value.kind !== "catalog" ||
      value.ordinal !== ordinal ||
      !Array.isArray(value.recordings) ||
      value.recordings.length === 0 ||
      value.recordings.length > manifest.catalog_shard_size
    ) {
      throw new Error(
        `Corpus catalog shard ${ordinal} has an invalid envelope or bound.`,
      );
    }
    const batch = value.recordings.map((item, index) =>
      parseSummary(item, recordings.length + index),
    );
    const shardSources = batch.reduce(
      (count, item) => count + item.source_count,
      0,
    );
    const shardRevisions = batch.reduce(
      (count, item) => count + item.transcript_revision_count,
      0,
    );
    const shardSegments = batch.reduce(
      (count, item) => count + item.segment_count,
      0,
    );
    if (
      descriptor.recording_count !== batch.length ||
      descriptor.source_count !== shardSources ||
      descriptor.transcript_revision_count !== shardRevisions ||
      descriptor.segment_count !== shardSegments ||
      descriptor.first_recording_id !== batch[0].recording_id ||
      descriptor.last_recording_id !== batch.at(-1)?.recording_id
    ) {
      throw new Error(
        `Corpus catalog descriptor ${ordinal} does not match its shard.`,
      );
    }
    for (const summary of batch) {
      if (seenIds.has(summary.recording_id) || seenSlugs.has(summary.slug)) {
        throw new Error(
          "Corpus catalog contains a duplicate recording ID or slug.",
        );
      }
      seenIds.add(summary.recording_id);
      seenSlugs.add(summary.slug);
      recordings.push(summary);
    }
    sources += shardSources;
    revisions += shardRevisions;
    segments += shardSegments;
  }
  if (
    manifest.counts.recordings !== recordings.length ||
    manifest.counts.sources !== sources ||
    manifest.counts.transcript_revisions !== revisions ||
    manifest.counts.segments !== segments
  ) {
    throw new Error(
      "Corpus sharded manifest counts do not match its catalog shards.",
    );
  }
  return {
    schemaVersion: 2,
    releaseId: manifest.release_id,
    generatedAt: manifest.generated_at,
    counts: manifest.counts,
    stats: {
      recordings: manifest.counts.recordings,
      transcriptSegments: manifest.stats.searchable_transcript_segments,
      transcriptRevisions: manifest.stats.transcript_revisions,
      durationMs: manifest.stats.duration_ms,
      sourceListings: manifest.stats.source_listings,
    },
    facets: {
      platforms: manifest.facets.platforms,
      years: manifest.facets.years,
      languages: manifest.facets.languages,
      speakers: manifest.facets.speakers,
      reviewStates: manifest.facets.review_states,
      confidenceBands: manifest.facets.confidence_bands,
      recordingTypes: manifest.facets.recording_types,
    },
    recordings,
  };
}

/** Load only the manifest and bounded catalog summaries, never full transcripts. */
export function loadCorpusCatalog(): Promise<CorpusCatalog> {
  cachedCatalog ??= readCorpusCatalog();
  return cachedCatalog;
}

function arraysEqual(left: string[], right: string[]): boolean {
  return (
    left.length === right.length &&
    left.every((value, index) => value === right[index])
  );
}

/** Resolve and integrity-check exactly one full recording shard. */
export async function loadCorpusRecording(
  summary: CorpusRecordingSummary,
): Promise<CorpusRecording> {
  if (!summary.detail) {
    const release = await loadCorpusRelease();
    const recording = release.recordings.find(
      (item) => item.recording_id === summary.recording_id,
    );
    if (!recording)
      throw new Error(
        `Legacy corpus recording is missing: ${summary.recording_id}`,
      );
    return recording;
  }
  const catalog = await loadCorpusCatalog();
  if (catalog.schemaVersion !== 2 || !activeManifest) {
    throw new Error(
      "A sharded recording reference was used without an active v2 manifest.",
    );
  }
  const value = await readVerifiedShard(activeManifest, summary.detail);
  if (!isRecord(value))
    throw new Error("Corpus recording shard must be an object.");
  assertExactKeys(
    value,
    ["schema_version", "kind", "recording"],
    `recording shard ${summary.recording_id}`,
  );
  if (value.schema_version !== 2 || value.kind !== "recording") {
    throw new Error(
      `Corpus recording shard has an invalid envelope: ${summary.recording_id}`,
    );
  }
  const recording = parseRecording(value.recording, 0);
  const derived = summaryFromLegacy(recording);
  if (
    recording.recording_id !== summary.recording_id ||
    recording.slug !== summary.slug ||
    recording.title !== summary.title ||
    recording.date_label !== summary.date_label ||
    recording.date_year !== summary.date_year ||
    recording.date_basis !== summary.date_basis ||
    recording.duration_ms !== summary.duration_ms ||
    recording.recording_type !== summary.recording_type ||
    recording.review_state !== summary.review_state ||
    derived.source_count !== summary.source_count ||
    derived.transcript_revision_count !== summary.transcript_revision_count ||
    derived.segment_count !== summary.segment_count ||
    derived.searchable_segment_count !== summary.searchable_segment_count ||
    !arraysEqual(derived.platforms, [...summary.platforms].sort()) ||
    !arraysEqual(derived.languages, [...summary.languages].sort())
  ) {
    throw new Error(
      `Corpus recording summary differs from detail shard: ${summary.recording_id}`,
    );
  }
  return recording;
}

function normalizedLanguageTag(language: string): string {
  return language.trim().toLowerCase().replaceAll("_", "-") || "und";
}

/** Return every published, non-retracted revision without implying one is authoritative. */
export function searchableTranscripts(
  recording: CorpusRecording,
): TranscriptRevision[] {
  return recording.transcript_revisions
    .filter((revision) => revision.lifecycle_state !== "retracted")
    .sort(
      (a, b) =>
        normalizedLanguageTag(a.language).localeCompare(
          normalizedLanguageTag(b.language),
        ) || a.revision_id.localeCompare(b.revision_id),
    );
}

export function transcriptDisclaimer(revision: TranscriptRevision): string {
  switch (revision.disclaimer_code) {
    case "machine_generated_unreviewed_not_verified_quotation_v1":
      return "Machine-generated and unreviewed; may be wrong; not a verified quotation.";
    case "disputed_transcript_not_verified_quotation_v1":
      return "This transcript is disputed; it may be wrong and is not a verified quotation.";
    case "reviewed_transcript_not_fact_checked_v1":
      return "This transcript was reviewed for wording; it is not fact-checked and is not proof that a claim is true.";
    case "retracted_transcript_text_withdrawn_v1":
      return "Transcript text withdrawn by a human decision; see the public retraction explanation.";
    default:
      throw new Error(
        `Unsupported transcript disclaimer: ${revision.disclaimer_code}`,
      );
  }
}

export function sortedRecordings(release: CorpusRelease): CorpusRecording[] {
  return [...release.recordings].sort(
    (a, b) =>
      (b.date_year ?? Number.NEGATIVE_INFINITY) -
        (a.date_year ?? Number.NEGATIVE_INFINITY) ||
      a.title.localeCompare(b.title),
  );
}

export function corpusStats(release: CorpusRelease): CorpusStats {
  return release.recordings.reduce<CorpusStats>(
    (stats, recording) => {
      const revisions = searchableTranscripts(recording);
      stats.recordings += 1;
      stats.durationMs += recording.duration_ms ?? 0;
      stats.sourceListings += recording.sources.length;
      stats.transcriptRevisions += recording.transcript_revisions.length;
      stats.transcriptSegments += revisions.reduce(
        (count, revision) =>
          count +
          revision.segments.filter((segment) => segment.text.trim() !== "")
            .length,
        0,
      );
      return stats;
    },
    {
      recordings: 0,
      transcriptSegments: 0,
      transcriptRevisions: 0,
      durationMs: 0,
      sourceListings: 0,
    },
  );
}

function sorted(values: Set<string>): string[] {
  return [...values]
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
}

export function corpusFacets(release: CorpusRelease): CorpusFacets {
  const platforms = new Set<string>();
  const years = new Set<string>();
  const languages = new Set<string>();
  const speakers = new Set<string>();
  const reviewStates = new Set<string>();
  const confidenceBands = new Set<string>();
  const recordingTypes = new Set<string>();

  for (const recording of release.recordings) {
    if (recording.date_year !== null) years.add(String(recording.date_year));
    recordingTypes.add(recording.recording_type);
    reviewStates.add(recording.review_state);
    for (const source of recording.sources) platforms.add(source.platform);
    for (const revision of searchableTranscripts(recording)) {
      languages.add(revision.language);
      reviewStates.add(revision.review_state);
      for (const segment of revision.segments) {
        speakers.add(segment.speaker_label || "Unknown speaker");
        confidenceBands.add(segment.confidence_band || "Uncalibrated");
      }
    }
  }

  return {
    platforms: sorted(platforms),
    years: sorted(years).reverse(),
    languages: sorted(languages),
    speakers: sorted(speakers),
    reviewStates: sorted(reviewStates),
    confidenceBands: sorted(confidenceBands),
    recordingTypes: sorted(recordingTypes),
  };
}

export function formatTimestamp(milliseconds: number): string {
  const totalSeconds = Math.max(0, Math.floor(milliseconds / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return hours > 0
    ? `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`
    : `${minutes}:${String(seconds).padStart(2, "0")}`;
}

export function formatDuration(milliseconds: number | null): string {
  if (milliseconds === null) return "Duration unknown";
  if (milliseconds > 0 && milliseconds < 60_000) return `${Math.max(1, Math.round(milliseconds / 1000))} sec`;
  const totalMinutes = Math.round(milliseconds / 60_000);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  if (hours === 0) return `${minutes} min`;
  return minutes === 0 ? `${hours} hr` : `${hours} hr ${minutes} min`;
}

export function revisionFragment(revision: TranscriptRevision): string {
  return `transcript-${revision.revision_id}`;
}

/** Stable across resegmentation order changes and unambiguous across revisions. */
export function segmentFragment(
  revision: TranscriptRevision,
  segment: CorpusSegment,
): string {
  return `segment-${revision.revision_id}-${segment.segment_id}`;
}

export function segmentPath(
  recording: CorpusRecording,
  revision: TranscriptRevision,
  segment: CorpusSegment,
): string {
  const seconds = Math.floor(segment.start_ms / 1000);
  return `/corpus/videos/${recording.slug}/?t=${seconds}#${segmentFragment(revision, segment)}`;
}
