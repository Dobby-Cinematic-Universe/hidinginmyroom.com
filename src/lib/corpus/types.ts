export interface CorpusSource {
  source_id: string;
  platform: string;
  url: string;
  native_id: string;
  access_state: string;
}

export interface CorpusSegment {
  segment_id: string;
  start_ms: number;
  end_ms: number;
  text: string;
  speaker_label: string | null;
  confidence_band: string | null;
  calibrated_probability: number | null;
}

export interface TranscriptRevision {
  revision_id: string;
  revision_kind: string;
  language: string;
  review_state: string;
  machine_generated: boolean;
  unreviewed: boolean;
  verified_quotation: false;
  disclaimer_code: string;
  lifecycle_state: string;
  lifecycle_history: TranscriptLifecycleEntry[];
  segments: CorpusSegment[];
}

export interface TranscriptLifecycleEntry {
  state: string;
  reason_code: string;
  decided_at: string;
  explanation: string;
}

export interface CorpusRecording {
  recording_id: string;
  slug: string;
  title: string;
  date_label: string | null;
  date_year: number | null;
  date_basis: string;
  duration_ms: number | null;
  recording_type: string;
  review_state: string;
  sources: CorpusSource[];
  transcript_revisions: TranscriptRevision[];
}

export interface CorpusRelease {
  schema_version: string | number;
  release_id: string;
  generated_at: string;
  counts: Record<string, number>;
  recordings: CorpusRecording[];
}

export interface CorpusShardReference {
  path: string;
  sha256: string;
  bytes: number;
}

/** Transcript-free row used by landing and browse pages. */
export interface CorpusRecordingSummary {
  recording_id: string;
  slug: string;
  title: string;
  date_label: string | null;
  date_year: number | null;
  date_basis: string;
  duration_ms: number | null;
  recording_type: string;
  review_state: string;
  source_count: number;
  transcript_revision_count: number;
  segment_count: number;
  searchable_segment_count: number;
  platforms: string[];
  languages: string[];
  detail?: CorpusShardReference;
}

export interface CorpusCatalog {
  schemaVersion: 1 | 2;
  releaseId: string;
  generatedAt: string;
  counts: Record<string, number>;
  stats: CorpusStats;
  facets: CorpusFacets;
  recordings: CorpusRecordingSummary[];
}

export interface CorpusFacets {
  platforms: string[];
  years: string[];
  languages: string[];
  speakers: string[];
  reviewStates: string[];
  confidenceBands: string[];
  recordingTypes: string[];
}

export interface CorpusStats {
  recordings: number;
  transcriptSegments: number;
  transcriptRevisions: number;
  durationMs: number;
  sourceListings: number;
}

export interface CorpusGraphCounts {
  entities: number;
  events: number;
  appearances: number;
  event_participants: number;
  event_dates: number;
  event_relations: number;
  event_evidence: number;
}

export interface CorpusGraphRecordingAnchor {
  recording_id: string;
  slug: string;
  title: string;
}

export interface CorpusGraphSourceAnchor {
  source_id: string;
  platform: string;
  url: string;
  native_id: string;
}

export interface CorpusGraphRenditionAnchor {
  rendition_id: string;
  time_basis: 'rendition_media_ms';
}

export interface CorpusGraphAnchor {
  recording: CorpusGraphRecordingAnchor;
  source: CorpusGraphSourceAnchor;
  rendition: CorpusGraphRenditionAnchor;
}

export interface CorpusGraphEntity {
  entity_id: string;
  slug: string;
  label: string;
  entity_type: string;
  mentions?: DerivedRecordingLink[];
  speakerRecordings?: DerivedRecordingLink[];
}

export interface DerivedRecordingLink {
  recording_id: string;
  slug: string;
  title: string;
  recording_date: string | null;
  date_basis: string;
  basis?: string[];
}

export interface CorpusGraphEvent {
  event_id: string;
  slug: string;
  label: string;
  description?: string;
  classification?: string;
  sourceRecordings?: DerivedRecordingLink[];
  sourceSummaryIds?: string[];
  mentionedEntities?: string[];
  dateMentions?: string[];
}

export interface CorpusGraphAppearance {
  appearance_id: string;
  entity_id: string;
  label: string;
  start_ms: number;
  end_ms: number;
  anchor: CorpusGraphAnchor;
}

export interface CorpusGraphParticipant {
  event_participant_id: string;
  event_id: string;
  entity_id: string;
  label: string;
}

export interface CorpusGraphDate {
  event_date_id: string;
  event_id: string;
  label: string;
  value_start: string | null;
  value_end: string | null;
  precision: string;
  certainty: string;
}

export interface CorpusGraphRelation {
  event_relation_id: string;
  from_event_id: string;
  to_event_id: string;
  label: string;
}

export interface CorpusGraphEvidence {
  event_evidence_id: string;
  event_id: string;
  label: string;
  support_kind: string;
  start_ms: number | null;
  end_ms: number | null;
  anchor: CorpusGraphAnchor;
}

export interface CorpusGraphRelease {
  derived?: boolean;
  eventGroups?: RelatedAccountGroup[];
  groupingStale?: boolean;
  schemaVersion: 1;
  releaseId: string;
  generatedAt: string;
  counts: CorpusGraphCounts;
  entities: CorpusGraphEntity[];
  events: CorpusGraphEvent[];
  appearances: CorpusGraphAppearance[];
  eventParticipants: CorpusGraphParticipant[];
  eventDates: CorpusGraphDate[];
  eventRelations: CorpusGraphRelation[];
  eventEvidence: CorpusGraphEvidence[];
}

export interface RelatedAccountGroup {
  id: string;
  slug: string;
  label: string;
  representativeId: string;
  members: string[];
  sourceRecordings: DerivedRecordingLink[];
  mentionedEntities: string[];
  sourceSummaryIds: string[];
}
