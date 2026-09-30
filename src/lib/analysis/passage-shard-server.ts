import { loadAnalysisRelease } from './release';
import { readAnalysisAsset } from './assets';
import { planPassageShards, renderPassageShard } from './passage-shard-plan.mjs';
import type { PassageShardDescriptor } from './passage-shards.mjs';

let plan: Promise<PassageShardDescriptor[]> | undefined;
const readRecording = (id:string) => readAnalysisAsset(`passages/${id}.json`);
export async function loadPassageShardPlan(): Promise<PassageShardDescriptor[]> {
  if (import.meta.env.DEV) return [];
  return plan ??= (async () => {
    const release = await loadAnalysisRelease();
    return release ? planPassageShards(release, readRecording) : [];
  })();
}
export async function loadPassageShardText(id:string): Promise<string | null> {
  const descriptor = (await loadPassageShardPlan()).find(shard => shard.id === id);
  const release = await loadAnalysisRelease();
  return descriptor && release ? renderPassageShard(descriptor, release, readRecording) : null;
}
