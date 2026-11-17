import type { AnalysisRelease } from './explorer.mjs';
export const PASSAGE_TABLE_FILE: 'passage-table.json';
export const TOPIC_TYPES: string[];
export interface PassageTopic { name: string; type: string; passages: number[] }
export interface PassageTable { schema_version: 1; corpus_release_id: string; questionnaire_version: string; model: string; source_generated_at: string; loadings_sha256: string; window_words: number; steps_per_sd: number; passages: number; factor_ids: string[]; subjects: string[]; addressees: string[]; recordings: [string, number, number, 0 | 1][]; dims: string; subject: string; addressee: string; start_s: string; topics: PassageTopic[] }
export interface PassageRecordingInfo { id: string; title: string; date: string | null; estimated?: string | null; kind: string; href: string }
export interface PassageFrameRecording extends PassageRecordingInfo { estimated: string | null; dateText: string; start: number; count: number; counted: boolean }
export interface PassageFrame { n: number; nd: number; dims: Int8Array; subject: Uint8Array; addressee: Uint8Array; startS: Uint32Array; rec: Uint32Array; kind: Uint8Array; year: Float32Array; counted: Uint8Array; recordings: PassageFrameRecording[]; factorIds: string[]; subjects: string[]; addressees: string[]; topics: PassageTopic[]; windowWords: number; loadingsSha256: string; dim(i: number, k: number): number }
export function validatePassageTable(value: unknown, release: AnalysisRelease): PassageTable;
export function decodePassageTable(value: PassageTable): { n: number; nd: number; dims: Int8Array; subject: Uint8Array; addressee: Uint8Array; startS: Uint32Array };
export function buildPassageFrame(value: PassageTable, recordings: PassageRecordingInfo[]): PassageFrame;
export function passageYears(frame: PassageFrame): number[];
export function subjectShares(frame: PassageFrame, years: number[], minimum?: number): { id: string; overall: number; share: number[][]; passages: number[][] }[];
export function subjectProfile(frame: PassageFrame, kind: number, minimum?: number): { id: string; passages: number; difference: number[] }[];
export function heldConstantTrend(frame: PassageFrame, kind: number, k: number, years: number[], minimum?: number): { year: number; passages: number; observed: number; held: number; covered: number }[];
export function recordingArc(frame: PassageFrame, r: number, k: number): { values: number[]; smooth: number[]; mean: number; sd: number; ranked: { value: number; passage: number }[] };
export function topicMask(frame: PassageFrame, topic: PassageTopic): Uint8Array;
export function topicContrast(frame: PassageFrame, mask: Uint8Array, kind?: number): { id: string; recordings: number; within: number; low: number; high: number; across: number }[];
export function findPassages(frame: PassageFrame, filters?: { kind?: number; subject?: number; addressee?: number; from?: number; to?: number; mask?: Uint8Array | null; levels?: Record<number, number>; sort?: number; descending?: boolean; versions?: boolean }): number[];
