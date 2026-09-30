export function analysisReleaseIdentity(stat) {
  return [stat.dev, stat.ino, stat.size, stat.mtimeNs, stat.ctimeNs].map(String).join(':');
}

export function analysisReleaseCacheKey(pointerIdentity, fileIdentity, corpusReleaseId) {
  return JSON.stringify([pointerIdentity, fileIdentity, corpusReleaseId]);
}

/** Cache only the current successful value; coalesce concurrent identical reads. */
export function createAnalysisReleaseCache() {
  let current;
  let latestKey;
  const inFlight = new Map();
  return function getOrLoad(key, load) {
    if (current?.key === key) return Promise.resolve(current.value);
    const pending = inFlight.get(key);
    if (pending) return pending;
    current = undefined;
    latestKey = key;
    const promise = Promise.resolve().then(load).then((value) => {
      if (latestKey === key) current = { key, value };
      return value;
    }).finally(() => {
      inFlight.delete(key);
    });
    inFlight.set(key, promise);
    return promise;
  };
}
