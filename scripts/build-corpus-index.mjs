import { createHash } from "node:crypto";
import { lstat, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import * as pagefind from "pagefind";
import { knownSpeakerLabel, showSpeakerLabels } from '../src/lib/corpus/presentation.mjs';

const projectRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const corpusDataRoot = process.env.HIMR_CORPUS_DATA_ROOT
  ? path.resolve(process.env.HIMR_CORPUS_DATA_ROOT)
  : path.join(projectRoot, "src", "data", "corpus");
const releasePath = path.join(corpusDataRoot, "release.json");
const manifestPath = path.join(corpusDataRoot, "manifest.json");
const corpusOutput = path.join(projectRoot, "dist", "corpus");
const previewOutput = process.env.HIMR_CORPUS_PREVIEW_OUTPUT ? path.resolve(process.env.HIMR_CORPUS_PREVIEW_OUTPUT) : undefined;
if (previewOutput) {
  const relative = path.relative(path.join(projectRoot, 'research/corpus/site-previews'), previewOutput);
  if (!/^release-[a-z0-9-]+[\\/]search$/.test(relative)) throw new Error('Invalid private preview search output.');
  const rootStat = await lstat(path.dirname(previewOutput));
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink() || (rootStat.mode & 0o077)) throw new Error('Preview search parent must be private.');
  try { await lstat(previewOutput); throw new Error('Preview search output already exists; use a fresh snapshot.'); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
}
const searchOutput = previewOutput || corpusOutput;
const indexOutput = path.join(searchOutput, "pagefind");
const buildManifestPath = path.join(searchOutput, "search-manifest.json");

const emptyRelease = {
  schema_version: "1",
  release_id: "empty",
  generated_at: "",
  counts: {},
  recordings: [],
};

const transcriptDisclaimers = {
  machine_generated_unreviewed_not_verified_quotation_v1:
    "Machine-generated and unreviewed; may be wrong; not a verified quotation.",
  disputed_transcript_not_verified_quotation_v1:
    "This transcript is disputed; it may be wrong and is not a verified quotation.",
  reviewed_transcript_not_fact_checked_v1:
    "This transcript was reviewed for wording; it is not fact-checked and is not proof that a claim is true.",
};

function isObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function canonicalJson(value) {
  if (
    value === null ||
    ["boolean", "number", "string"].includes(typeof value)
  ) {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (isObject(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  throw new Error(
    "Corpus manifest contains an unsupported canonical JSON value.",
  );
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function checkedReference(reference, prefix) {
  if (!isObject(reference) || typeof reference.path !== "string") {
    throw new Error("Corpus shard reference is invalid.");
  }
  const pattern =
    prefix === "catalog/"
      ? /^catalog\/catalog-[0-9]{5}-[a-f0-9]{16}\.json$/
      : /^recordings\/rec_[a-f0-9]{32}-[a-f0-9]{16}\.json$/;
  if (
    !pattern.test(reference.path) ||
    !/^[a-f0-9]{64}$/.test(reference.sha256) ||
    !Number.isInteger(reference.bytes) ||
    reference.bytes <= 0
  ) {
    throw new Error(
      `Corpus shard reference is unsafe or malformed: ${reference.path}`,
    );
  }
  return reference;
}

async function readVerifiedShard(root, reference, prefix) {
  checkedReference(reference, prefix);
  const candidate = path.resolve(root, ...reference.path.split("/"));
  if (!candidate.startsWith(`${root}${path.sep}`)) {
    throw new Error(
      `Corpus shard escapes its release directory: ${reference.path}`,
    );
  }
  const before = await lstat(candidate);
  if (!before.isFile() || before.isSymbolicLink()) {
    throw new Error(`Corpus shard is not a regular file: ${reference.path}`);
  }
  const payload = await readFile(candidate);
  const after = await lstat(candidate);
  if (
    before.dev !== after.dev ||
    before.ino !== after.ino ||
    before.size !== after.size ||
    before.mtimeMs !== after.mtimeMs
  ) {
    throw new Error(`Corpus shard changed during indexing: ${reference.path}`);
  }
  if (
    payload.length !== reference.bytes ||
    sha256(payload) !== reference.sha256
  ) {
    throw new Error(
      `Corpus shard failed integrity validation: ${reference.path}`,
    );
  }
  return JSON.parse(payload.toString("utf8"));
}

async function loadRelease() {
  try {
    const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
    if (
      !isObject(manifest) ||
      manifest.schema_version !== 2 ||
      !/^release_[a-f0-9]{24}$/.test(manifest.release_id) ||
      !Array.isArray(manifest.catalog_shards)
    ) {
      throw new Error("Corpus v2 manifest has an invalid envelope.");
    }
    const identityPayload = Object.fromEntries(
      Object.entries(manifest).filter(([key]) => key !== "release_id"),
    );
    const expectedId = `release_${sha256(canonicalJson(identityPayload)).slice(0, 24)}`;
    if (manifest.release_id !== expectedId) {
      throw new Error(
        `Corpus v2 manifest identity mismatch: expected ${expectedId}.`,
      );
    }
    const root = path.resolve(corpusDataRoot, "releases", manifest.release_id);
    return {
      schema_version: manifest.schema_version,
      release_id: manifest.release_id,
      generated_at: manifest.generated_at,
      async *recordings() {
        let yielded = 0;
        for (const [ordinal, descriptor] of manifest.catalog_shards.entries()) {
          const catalog = await readVerifiedShard(root, descriptor, "catalog/");
          if (
            !isObject(catalog) ||
            catalog.schema_version !== 2 ||
            catalog.kind !== "catalog" ||
            catalog.ordinal !== ordinal ||
            !Array.isArray(catalog.recordings) ||
            catalog.recordings.length !== descriptor.recording_count ||
            catalog.recordings.length > manifest.catalog_shard_size
          ) {
            throw new Error(
              `Corpus catalog shard ${ordinal} has an invalid envelope.`,
            );
          }
          for (const summary of catalog.recordings) {
            if (!isObject(summary) || !isObject(summary.detail)) {
              throw new Error(
                `Corpus catalog shard ${ordinal} has an invalid summary.`,
              );
            }
            const detail = await readVerifiedShard(
              root,
              summary.detail,
              "recordings/",
            );
            if (
              !isObject(detail) ||
              detail.schema_version !== 2 ||
              detail.kind !== "recording" ||
              !isObject(detail.recording) ||
              detail.recording.recording_id !== summary.recording_id
            ) {
              throw new Error(
                `Corpus recording shard differs from summary ${summary.recording_id}.`,
              );
            }
            yielded += 1;
            yield detail.recording;
          }
        }
        if (yielded !== manifest.counts?.recordings) {
          throw new Error(
            "Corpus v2 manifest recording count differs from its shards.",
          );
        }
      },
    };
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }

  try {
    const release = JSON.parse(await readFile(releasePath, "utf8"));
    if (!isObject(release) || !Array.isArray(release.recordings)) {
      throw new Error(
        "Corpus release must be an object with a recordings array.",
      );
    }
    return {
      schema_version: release.schema_version,
      release_id: release.release_id,
      generated_at: release.generated_at,
      async *recordings() {
        yield* release.recordings;
      },
    };
  } catch (error) {
    if (error?.code === "ENOENT") {
      return {
        ...emptyRelease,
        async *recordings() {},
      };
    }
    if (error instanceof SyntaxError) {
      throw new Error(`Corpus release JSON is invalid: ${error.message}`, {
        cause: error,
      });
    }
    throw error;
  }
}

function normalizedLanguageTag(language) {
  return (
    String(language || "")
      .trim()
      .toLowerCase()
      .replaceAll("_", "-") || "und"
  );
}

function searchableTranscripts(recording) {
  if (!Array.isArray(recording.transcript_revisions)) return [];
  return recording.transcript_revisions
    .filter(
      (revision) =>
        isObject(revision) && revision.lifecycle_state !== "retracted",
    )
    .sort(
      (left, right) =>
        normalizedLanguageTag(left.language).localeCompare(
          normalizedLanguageTag(right.language),
        ) ||
        String(left.revision_id || "").localeCompare(
          String(right.revision_id || ""),
        ),
    );
}

function transcriptDisclaimer(revision) {
  const code = safeText(revision.disclaimer_code);
  const disclaimer = transcriptDisclaimers[code];
  if (!disclaimer) {
    throw new Error(
      `Corpus transcript ${safeText(revision.revision_id, "(unknown)")} lacks a supported disclaimer.`,
    );
  }
  if (revision.verified_quotation !== false) {
    throw new Error(
      `Corpus transcript ${safeText(revision.revision_id, "(unknown)")} claims verified quotation status.`,
    );
  }
  return disclaimer;
}

function formatTimestamp(milliseconds) {
  const totalSeconds = Math.max(0, Math.floor(milliseconds / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return hours > 0
    ? `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`
    : `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function segmentFragment(revisionId, segmentId) {
  return `segment-${revisionId}-${segmentId}`;
}

function normalizeLanguage(language) {
  const normalized = String(language || "")
    .trim()
    .toLowerCase();
  const match = normalized.match(/^[a-z]{2}/);
  return match?.[0] ?? "en";
}

function uniqueStrings(values, fallback) {
  const unique = [
    ...new Set(
      values.map((value) => String(value || "").trim()).filter(Boolean),
    ),
  ];
  return unique.length > 0 ? unique : [fallback];
}

function assertResponse(response, action) {
  const errors = response?.errors ?? [];
  if (errors.length > 0) {
    throw new Error(`Pagefind failed while ${action}: ${errors.join("; ")}`);
  }
  return response;
}

function safeText(value, fallback = "") {
  return typeof value === "string" ? value : fallback;
}

function stringValue(value, fallback = "") {
  if (typeof value === "string") return value;
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return fallback;
}

function finiteNumber(value, fallback = 0) {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

const release = await loadRelease();
await mkdir(searchOutput, { recursive: true, mode: 0o700 });

const resolvedIndexOutput = path.resolve(indexOutput);
const resolvedDist = path.resolve(projectRoot, "dist");
if (!previewOutput && !resolvedIndexOutput.startsWith(`${resolvedDist}${path.sep}`)) {
  throw new Error(
    `Refusing to replace corpus index outside dist: ${resolvedIndexOutput}`,
  );
}
if (!previewOutput) await rm(resolvedIndexOutput, { recursive: true, force: true });

let index;
let recordCount = 0;
let batchRecordCount = 0;
let batchOrdinal = 0;
let recordingCount = 0;
let skippedEmptySegments = 0;
const seenRecordUrls = new Set();
const indexDescriptors = [];
const configuredBatchSize = Number(
  process.env.HIMR_CORPUS_INDEX_BATCH_SIZE || 25_000,
);
if (
  !Number.isInteger(configuredBatchSize) ||
  configuredBatchSize < 1_000 ||
  configuredBatchSize > 100_000
) {
  throw new Error(
    "HIMR_CORPUS_INDEX_BATCH_SIZE must be an integer between 1,000 and 100,000.",
  );
}

async function ensureIndex() {
  if (index) return index;
  const created = assertResponse(
    await pagefind.createIndex(),
    "creating a corpus index shard",
  );
  if (!created.index)
    throw new Error("Pagefind did not return a corpus index instance.");
  index = created.index;
  return index;
}

async function flushIndex({ allowEmpty = false } = {}) {
  if (batchRecordCount === 0 && !allowEmpty) return;
  const activeIndex = await ensureIndex();
  const directoryName = String(batchOrdinal).padStart(5, "0");
  const outputPath = path.join(resolvedIndexOutput, directoryName);
  assertResponse(
    await activeIndex.writeFiles({ outputPath }),
    `writing corpus index shard ${directoryName}`,
  );
  await Promise.resolve(activeIndex.deleteIndex());
  await pagefind.close();
  index = undefined;
  indexDescriptors.push({
    path: `/corpus/pagefind/${directoryName}/`,
    record_count: batchRecordCount,
  });
  batchRecordCount = 0;
  batchOrdinal += 1;
}

try {
  for await (const recording of release.recordings()) {
    if (!isObject(recording))
      throw new Error("Corpus recording entries must be objects.");
    const slug = safeText(recording.slug);
    const recordingId = safeText(recording.recording_id);
    const title = safeText(recording.title, "Untitled recording");
    if (!slug || !/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(slug)) {
      throw new Error(
        `Corpus recording has an invalid URL slug: ${slug || "(empty)"}`,
      );
    }
    if (!recordingId)
      throw new Error(`Corpus recording ${slug} has no recording_id.`);

    const revisions = searchableTranscripts(recording).filter((revision) =>
      Array.isArray(revision.segments),
    );
    if (revisions.length === 0) continue;
    recordingCount += 1;

    const platforms = uniqueStrings(
      Array.isArray(recording.sources)
        ? recording.sources.map((source) =>
            isObject(source) ? source.platform : "",
          )
        : [],
      "Unknown source",
    );
    for (const revision of revisions) {
      const displaySpeakers = showSpeakerLabels(revision.segments);
      const language = normalizeLanguage(revision.language);
      transcriptDisclaimer(revision);
      const disclaimerCode = safeText(revision.disclaimer_code);
      const reviewStates = uniqueStrings(
        [recording.review_state, revision.review_state],
        "Unreviewed",
      );

      // Local preview searches whole recordings without millions of fragments.
      // Keep original segment IDs as links; actual transcript pages are unchanged.
      const searchableSegments = previewOutput && revision.segments.length
        ? [{...revision.segments[0], text: revision.segments.map((s) => s.text).join('\n'),
            end_ms: revision.segments.reduce((end,s) => Math.max(end,s.end_ms),0), speaker_label: null}]
        : revision.segments;
      for (const [segmentIndex, segment] of searchableSegments.entries()) {
        if (!isObject(segment)) {
          throw new Error(
            `Corpus segment ${recordingId}:${segmentIndex} is invalid.`,
          );
        }
        const segmentId = safeText(segment.segment_id);
        if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/.test(segmentId)) {
          throw new Error(
            `Corpus segment ${recordingId}:${segmentIndex} has an invalid segment_id.`,
          );
        }
        const content = safeText(segment.text).trim();
        if (!content) {
          skippedEmptySegments += 1;
          continue;
        }

        const startMs = finiteNumber(segment.start_ms);
        const endMs = finiteNumber(segment.end_ms, startMs);
        const timestamp = formatTimestamp(startMs);
        const revisionId = safeText(revision.revision_id);
        const fragment = segmentFragment(revisionId, segmentId);
        const url = `/corpus/videos/${slug}/?t=${Math.floor(startMs / 1000)}#${fragment}`;
        if (seenRecordUrls.has(url))
          throw new Error(`Duplicate corpus search URL: ${url}`);
        seenRecordUrls.add(url);

        const speaker =
          safeText(segment.speaker_label).trim() || "Unknown speaker";
        const confidence =
          safeText(segment.confidence_band).trim() || "Uncalibrated";
        const probability =
          typeof segment.calibrated_probability === "number" &&
          Number.isFinite(segment.calibrated_probability)
            ? segment.calibrated_probability
            : null;

        const activeIndex = await ensureIndex();
        assertResponse(
          await activeIndex.addCustomRecord({
            url,
            content,
            language,
            meta: {
              title,
              recording_id: recordingId,
              segment_id: segmentId,
              revision_id: revisionId,
              date_label: safeText(recording.date_label, "Date unresolved"),
              timestamp,
              start_ms: String(startMs),
              end_ms: String(endMs),
              speaker_label: speaker,
              display_speaker_label: displaySpeakers ? knownSpeakerLabel(segment.speaker_label) || '' : '',
              confidence_band: confidence,
              calibrated_probability:
                probability === null ? "Uncalibrated" : String(probability),
              transcript_disclaimer_code: disclaimerCode,
            },
            filters: {
              platform: platforms,
              year: [
                typeof recording.date_year === "number" &&
                Number.isFinite(recording.date_year)
                  ? String(recording.date_year)
                  : "Unknown year",
              ],
              language: [safeText(revision.language, language)],
              speaker: previewOutput
                ? [...new Set(revision.segments.map((s) => knownSpeakerLabel(s.speaker_label)).filter(Boolean))]
                : [speaker],
              review_state: reviewStates,
              confidence: [confidence],
              recording_type: [
                safeText(recording.recording_type, "Unknown type"),
              ],
            },
            sort: {
              date_year:
                typeof recording.date_year === "number" &&
                Number.isFinite(recording.date_year)
                  ? String(recording.date_year)
                  : "-1",
              start_ms: String(startMs),
              confidence: probability === null ? "-1" : String(probability),
            },
          }),
          `indexing ${recordingId}:${segmentId}`,
        );
        recordCount += 1;
        batchRecordCount += 1;
        if (recordCount % 1_000 === 0) {
          console.log(`Indexed ${recordCount.toLocaleString()} segments…`);
        }
        if (batchRecordCount === configuredBatchSize) await flushIndex();
      }
    }
  }

  await flushIndex({ allowEmpty: indexDescriptors.length === 0 });
} finally {
  if (index) await Promise.resolve(index.deleteIndex());
  await pagefind.close();
}

const buildManifest = {
  schema_version: 2,
  release_id: safeText(release.release_id, "empty"),
  release_schema_version: stringValue(release.schema_version, "unknown"),
  release_generated_at: safeText(release.generated_at),
  index_batch_size: configuredBatchSize,
  indexes: indexDescriptors,
  recording_count: recordingCount,
  record_count: recordCount,
  skipped_empty_segments: skippedEmptySegments,
};
await writeFile(
  buildManifestPath,
  `${JSON.stringify(buildManifest, null, 2)}\n`,
  "utf8",
);

console.log(
  `Built isolated corpus search index (${recordCount.toLocaleString()} records from ` +
    `${recordingCount.toLocaleString()} recordings; ${skippedEmptySegments.toLocaleString()} empty ` +
    `segments skipped).`,
);
