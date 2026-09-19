import path from 'node:path';

// These are operator/archive workspaces, not inputs to the public Astro site.
// Match the directory itself so Chokidar never descends into its job artifacts.
const workspaceDirectories = new Set([
  'research', 'vidsprivate', 'vidspublic', 'discord',
  "Hidinginmyroom - Daniel's Patreon",
  'acquisition', 'autonomous_controller', 'corpus', 'pipeline',
  'evaluation', 'operator_console', 'downloads', 'captures',
  'analysis', 'media-analysis',
]);

export function createDevWatchIgnore(root) {
  const projectRoot = path.resolve(root);
  return (watchedPath) => {
    const relative = path.relative(projectRoot, path.resolve(projectRoot, watchedPath));
    if (!relative || relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) return false;
    return workspaceDirectories.has(relative.split(path.sep)[0]);
  };
}
