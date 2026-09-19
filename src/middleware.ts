import { defineMiddleware } from 'astro:middleware';
import { readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { localPreviewRoot } from './lib/local-preview';

export const onRequest = defineMiddleware(async (context, next) => {
  const root = localPreviewRoot();
  const url = context.url.pathname;
  if (!root || !(url === '/corpus/search-manifest.json' || url.startsWith('/corpus/pagefind/'))) return next();
  const relative = url.slice('/corpus/'.length);
  if (!/^[A-Za-z0-9_./-]+$/.test(relative) || relative.split('/').some((x) => x === '..' || x === '.')) return new Response('Invalid path', {status:400});
  try {
    const base = await realpath(path.join(root,'search'));
    const target = await realpath(path.join(base,relative));
    if (!target.startsWith(base+path.sep)) return new Response('Invalid path',{status:400});
    const bytes = await readFile(target);
    const type = target.endsWith('.js') ? 'text/javascript' : target.endsWith('.json') ? 'application/json' : target.endsWith('.wasm') ? 'application/wasm' : 'application/octet-stream';
    return new Response(new Uint8Array(bytes),{headers:{'Content-Type':type,'Cache-Control':'private, no-store','X-Robots-Tag':'noindex, nofollow'}});
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return new Response('Local search index is still being prepared.',{status:503});
    throw error;
  }
});
