// Providers may reindex after an instance-wide configuration write. Incremental
// content releases must not write these settings, even when they are unchanged.
function containsSettings(actual, desired) {
  if (desired === null || typeof desired !== 'object') return actual === desired;
  if (!actual || typeof actual !== 'object') return false;
  return Object.entries(desired).every(([key, value]) => containsSettings(actual[key], value));
}

export async function configureIndex({api, instance, config, allowIndexRebuild = false}) {
  const route = `ai-search/instances/${instance}`;
  const current = await api(route);
  if (containsSettings(current, config)) return {instance: current, changed: false};
  if (!allowIndexRebuild) {
    throw Error('Index configuration differs. Incremental releases must retain existing settings. Inspect the difference and use --allow-index-rebuild only with explicit approval for a possible full reindex.');
  }
  return {instance: await api(route, {method: 'PUT', body: config}), changed: true};
}
