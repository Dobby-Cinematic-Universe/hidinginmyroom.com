import type { APIRoute, GetStaticPaths } from 'astro';
import { loadAnalysisRelease } from '../../../../lib/analysis/release';
import { readAnalysisAsset } from '../../../../lib/analysis/assets';
import { validatePassageData } from '../../../../lib/analysis/passage-data.mjs';

export const getStaticPaths: GetStaticPaths = async () => {
  if (!import.meta.env.DEV) return [];
  const release = await loadAnalysisRelease();
  return release ? release.videos.map(video => ({params:{id:video.recording_id}})) : [];
};

export const GET: APIRoute = async ({params}) => {
  if (!/^rec_[a-f0-9]{32}$/.test(params.id ?? '')) return new Response('Not found',{status:404});
  const release = await loadAnalysisRelease();
  const video = release?.videos.find(item => item.recording_id === params.id);
  if (!release || !video) return new Response('Not found',{status:404});
  try {
    const value = await readAnalysisAsset(`passages/${video.recording_id}.json`);
    if (!value) return new Response('Passage data unavailable',{status:404});
    const data = validatePassageData(value,release,video);
    return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json; charset=utf-8','Cache-Control':import.meta.env.DEV?'no-store':'public, max-age=300'}});
  } catch (error) {
    if (import.meta.env.DEV) console.warn('Passage analysis unavailable:',error);
    return new Response('Passage data unavailable or incompatible with this release',{status:503});
  }
};
