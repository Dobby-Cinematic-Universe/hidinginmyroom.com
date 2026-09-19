import { lstat, readFile, readdir, realpath } from "node:fs/promises";
import { execFile } from "node:child_process";
import path from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

const projectRoot = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "..",
);
const execFileAsync = promisify(execFile);
const failures = [];

const requiredPaths = [
  ".node-version",
  "astro.config.mjs",
  "package.json",
  "public",
  "src",
  "acquisition",
  "corpus",
  "pipeline",
  "src/data/corpus",
  "src/pages/corpus",
  "docs/CORPUS_ARCHITECTURE.md",
  "docs/CORPUS_RIGHTS_AND_TAKEDOWN.md",
];

const prohibitedPublicExtensions = new Set([
  ".7z",
  ".arrow",
  ".ass",
  ".avi",
  ".bin",
  ".bz2",
  ".db",
  ".facevec",
  ".flac",
  ".gz",
  ".iso",
  ".joblib",
  ".m4a",
  ".m4v",
  ".mkv",
  ".mov",
  ".mp3",
  ".mp4",
  ".npy",
  ".npz",
  ".onnx",
  ".p12",
  ".pem",
  ".pfx",
  ".pkl",
  ".parquet",
  ".pt",
  ".pth",
  ".rar",
  ".safetensors",
  ".srt",
  ".sqlite",
  ".sqlite3",
  ".tar",
  ".tgz",
  ".torrent",
  ".vtt",
  ".wav",
  ".webm",
  ".voicevec",
  ".gguf",
  ".xz",
  ".zip",
  ".zst",
]);

// These are the only binary formats that belong in the public source tree.
// Everything else is treated as text and scanned, or rejected if it is not
// valid UTF-8.  This keeps a new directory from silently becoming a scan
// bypass.
const allowedSiteBinaryExtensions = new Set([
  ".avif",
  ".gif",
  ".ico",
  ".jpeg",
  ".jpg",
  ".otf",
  ".png",
  ".ttf",
  ".webp",
  ".woff",
  ".woff2",
]);

const prohibitedRepositoryPrefixes = [
  "analysis/",
  "captures/",
  "corpus/artifacts/",
  "corpus/private/",
  "corpus/work/",
  "downloads/",
  "media-analysis/",
  "research/",
  "scripts/research/",
  "vidsprivate/",
  "vidspublic/",
];

const prohibitedSensitivePathPattern =
    /(?:^|\/)(?:\.env(?:\.[^/]*)?|\.netrc|\.npmrc|\.pypirc|authorized_keys|client[_-]?secret[^/]*|cookies?(?:\.(?:json|txt))?|credentials?(?:\.(?:json|txt|ya?ml))?|headers?(?:\.(?:json|txt|ya?ml))|id_(?:dsa|ecdsa|ed25519|rsa)(?:\.pub)?|secrets?(?:\.(?:json|txt|ya?ml))?|sessions?(?:\.(?:json|txt|ya?ml))?|tokens?(?:\.(?:json|txt|ya?ml))?)$/i;
const prohibitedBiometricDataPathPattern =
    /(?:^|\/)(?:(?:face|voice|audio|visual|speaker|identity)[_-]?)?(?:embeddings?|voiceprints?|faceprints?|biometric[_-]?(?:vectors?|features?|templates?))(?:[-_.][^/]*)?\.(?:csv|jsonl?|tsv|txt)$/i;
const prohibitedBiometricBinaryExtensions = new Set([
  ".f16le",
  ".f32le",
  ".f64le",
]);
const prohibitedBiometricArtifactPathPatterns = [
  prohibitedBiometricDataPathPattern,
  /(?:^|\/)(?:(?:aligned|enrollment|reference|query)[_-]?)?(?:face[_-]?)?crops?(?:[-_.][^/]*)?\.(?:avif|gif|jpe?g|png|webp)$/i,
  /(?:^|\/)(?:crops?|aligned[_-]?crops?|enrollment[_-]?crops?)\/[^/]+\.(?:avif|gif|jpe?g|png|webp)$/i,
  /(?:^|\/)(?:(?:reference|query|pairwise|face|voice|speaker|identity|biometric)[_-]?)?(?:scores?|score[_-]?matri(?:x|ces))(?:[-_.][^/]*)?\.(?:csv|jsonl?|tsv|txt)$/i,
];

function isProhibitedBiometricArtifactPath(relativePath) {
  return (
      prohibitedBiometricBinaryExtensions.has(
          path.extname(relativePath).toLowerCase(),
      ) ||
      prohibitedBiometricArtifactPathPatterns.some((pattern) =>
          pattern.test(relativePath),
      )
  );
}

const biometricPathClassifierCases = [
  ["public/" + "images/aligned-face-crop.png", true],
  ["public/generated/crops/0000.png", true],
  ["docs/data/reference-score-matrix.json", true],
  ["docs/data/query-scores.json", true],
  ["artifacts/vector.f32le", true],
  ["public/" + "images/wiki/the-face-community-art-2026.webp", false],
  ["public/" + "images/wiki/pia-profile-crop.webp", false],
  ["pipeline/examples/face-candidate-work-order.example.json", false],
  ["evaluation/schemas/transcript-score-report.schema.json", false],
];
for (const [probePath, expected] of biometricPathClassifierCases) {
  if (isProhibitedBiometricArtifactPath(probePath) !== expected) {
    failures.push(
        `Internal biometric path-classifier regression: ${probePath}`,
    );
  }
}

function hasExpectedBinarySignature(extension, contents) {
  const ascii = (start, end) => contents.subarray(start, end).toString("ascii");
  const startsWith = (...bytes) =>
      contents.length >= bytes.length &&
      bytes.every((value, index) => contents[index] === value);

  switch (extension) {
    case ".avif":
      return (
          contents.length >= 16 &&
          ascii(4, 8) === "ftyp" &&
          contents
              .subarray(8, Math.min(contents.length, 64))
              .includes(Buffer.from("avif"))
      );
    case ".gif":
      return ascii(0, 6) === "GIF87a" || ascii(0, 6) === "GIF89a";
    case ".ico":
      return startsWith(0x00, 0x00, 0x01, 0x00);
    case ".jpeg":
    case ".jpg":
      return startsWith(0xff, 0xd8, 0xff);
    case ".png":
      return startsWith(0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a);
    case ".webp":
      return (
          contents.length >= 12 &&
          ascii(0, 4) === "RIFF" &&
          ascii(8, 12) === "WEBP"
      );
    case ".woff":
      return ascii(0, 4) === "wOFF";
    case ".woff2":
      return ascii(0, 4) === "wOF2";
    case ".otf":
      return ascii(0, 4) === "OTTO";
    case ".ttf":
      return startsWith(0x00, 0x01, 0x00, 0x00) || ascii(0, 4) === "true";
    default:
      return false;
  }
}

async function pathExists(relativePath) {
  try {
    await readFile(path.join(projectRoot, relativePath));
    return true;
  } catch (error) {
    if (error?.code !== "EISDIR") return false;
    return true;
  }
}

async function walk(relativeDirectory) {
  const entries = await readdir(path.join(projectRoot, relativeDirectory), {
    withFileTypes: true,
  });
  const files = [];

  for (const entry of entries) {
    const relativePath = path.posix.join(relativeDirectory, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(relativePath)));
    else if (entry.isFile()) files.push(relativePath);
  }

  return files;
}

for (const requiredPath of requiredPaths) {
  if (!(await pathExists(requiredPath))) {
    failures.push(`Required public-build input is missing: ${requiredPath}`);
  }
}

const packageJson = JSON.parse(
    await readFile(path.join(projectRoot, "package.json"), "utf8"),
);

for (const scriptName of ["build", "check"]) {
  const command = packageJson.scripts?.[scriptName];
  if (typeof command !== "string" || command.trim() === "") {
    failures.push(`package.json is missing the ${scriptName} script.`);
    continue;
  }

  if (/research(?::|\/)|snapshot-current|snapshot-reddit/i.test(command)) {
    failures.push(
        `The ${scriptName} script depends on private research tooling: ${command}`,
    );
  }
}

const hasShardedRelease = await pathExists("src/data/corpus/manifest.json");
const hasLegacyRelease = await pathExists("src/data/corpus/release.json");
if (hasShardedRelease && hasLegacyRelease) {
  failures.push(
      "Both v2 manifest.json and legacy release.json are present; keep exactly one active corpus release.",
  );
}
if (!hasShardedRelease && !hasLegacyRelease) {
  failures.push(
      "No active static corpus release exists (expected manifest.json or release.json).",
  );
}
const activeReleasePath = hasShardedRelease
    ? "src/data/corpus/manifest.json"
    : "src/data/corpus/release.json";

try {
  await execFileAsync(
      "python3",
      ["-m", "himr_corpus", "validate-release", "--release", activeReleasePath],
      {
        cwd: projectRoot,
        encoding: "utf8",
        env: {
          ...process.env,
          PYTHONPATH: path.join(projectRoot, "corpus", "src"),
        },
        maxBuffer: 16 * 1024 * 1024,
      },
  );
} catch (error) {
  const detail = String(error.stderr || error.stdout || error.message).trim();
  failures.push(`Static corpus release validation failed: ${detail}`);
}

const graphManifestPath = "src/data/corpus/graph/manifest.json";
if (!(await pathExists(graphManifestPath))) {
  failures.push("The isolated public entity/event graph manifest is missing.");
} else {
  try {
    await execFileAsync(
        "python3",
        [
          "-m",
          "himr_corpus",
          "validate-graph-release",
          "--manifest",
          graphManifestPath,
        ],
        {
          cwd: projectRoot,
          encoding: "utf8",
          env: {
            ...process.env,
            PYTHONPATH: path.join(projectRoot, "corpus", "src"),
          },
          maxBuffer: 16 * 1024 * 1024,
        },
    );
  } catch (error) {
    const detail = String(error.stderr || error.stdout || error.message).trim();
    failures.push(`Static entity/event graph validation failed: ${detail}`);
  }
}

const publicFiles = await walk("public");
for (const relativePath of publicFiles) {
  const extension = path.extname(relativePath).toLowerCase();
  if (prohibitedPublicExtensions.has(extension)) {
    failures.push(
        `Non-site media or archive found in public/: ${relativePath}`,
    );
  }

  if (/^\.env(?:\.|$)/i.test(path.basename(relativePath))) {
    failures.push(`Environment file found in public/: ${relativePath}`);
  }
}

let candidateFiles = [];
try {
  // Include the index and every non-ignored untracked file. This is deliberate:
  // the check must protect a first commit just as strongly as an existing one.
  const { stdout } = await execFileAsync(
      "git",
      ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
      {
        cwd: projectRoot,
        encoding: "utf8",
        maxBuffer: 64 * 1024 * 1024,
      },
  );
  candidateFiles = [...new Set(stdout.split("\0").filter(Boolean))]
      .map((value) => value.replaceAll("\\", "/"))
      .sort((left, right) => left.localeCompare(right, "en"));
} catch (error) {
  failures.push(
      `Could not enumerate tracked and non-ignored files for the public-boundary check: ${error.message}`,
  );
}

const pathsNotSafeToScan = new Set();
for (const relativePath of candidateFiles) {
  const fullPath = path.join(projectRoot, relativePath);
  const extension = path.extname(relativePath).toLowerCase();

  if (
      prohibitedRepositoryPrefixes.some((prefix) =>
          relativePath.startsWith(prefix),
      )
  ) {
    failures.push(
        `Private-workspace path is publication-visible: ${relativePath}`,
    );
  }
  if (prohibitedSensitivePathPattern.test(relativePath)) {
    failures.push(
        `Credential/session filename is publication-visible: ${relativePath}`,
    );
  }
  if (isProhibitedBiometricArtifactPath(relativePath)) {
    pathsNotSafeToScan.add(relativePath);
    failures.push(
        `Raw biometric data filename is publication-visible: ${relativePath}`,
    );
  }
  if (
      prohibitedPublicExtensions.has(extension) ||
      /\.(?:db|sqlite3?)(?:-(?:journal|shm|wal))?$/i.test(relativePath)
  ) {
    pathsNotSafeToScan.add(relativePath);
    failures.push(
        `Raw media, model, database, subtitle, key, or archive is publication-visible: ${relativePath}`,
    );
  }

  try {
    const status = await lstat(fullPath);
    if (status.isSymbolicLink()) {
      pathsNotSafeToScan.add(relativePath);
      const target = await realpath(fullPath).catch(() => "<unresolved>");
      failures.push(
          `Symbolic links are not allowed in the public repository: ${relativePath} -> ${target}`,
      );
    } else if (!status.isFile()) {
      pathsNotSafeToScan.add(relativePath);
      failures.push(
          `Unexpected publication-visible file type: ${relativePath}`,
      );
    } else if (status.size > 100 * 1024 * 1024) {
      pathsNotSafeToScan.add(relativePath);
      failures.push(
          `File exceeds the public repository size gate: ${relativePath}`,
      );
    }
  } catch (error) {
    if (error?.code !== "ENOENT") {
      failures.push(
          `Could not inspect publication-visible path ${relativePath}: ${error.message}`,
      );
    }
  }
}

const sourceFiles = candidateFiles.filter(
    (relativePath) => !pathsNotSafeToScan.has(relativePath),
);
const assetReferences = new Set();
const assetPattern =
    /\/images\/[A-Za-z0-9_./%+-]+\.(?:avif|gif|jpe?g|png|svg|webp)/gi;
const absoluteLocalPathPattern =
    /(?:file:\/\/\/(?:home|Users|root)\/|\/(?:home|Users)\/[A-Za-z0-9._-]+\/|\/root\/|[A-Za-z]:\\Users\\|\/mnt\/[a-z]\/Users\/)/i;
const signedDiscordUrlPattern =
    /https?:\/\/(?:cdn\.|media\.)discordapp\.(?:com|net)\/[^\s"')>]+[?&](?:ex|is|hm)=/i;
const accessBearingUrlPattern =
    /https?:\/\/[^\s"')>]+[?&](?:access_token|auth|authorization|key-pair-id|signature|token|x-amz-(?:credential|signature))=[^\s"')>&]+/gi;
const prohibitedBiometricScoreDisclosurePatterns = [
  /\b(?:raw\s+SFace\s+(?:cosines?|cosine(?:-style)?\s+(?:values?|range)|similarit(?:y|ies)(?:\s+(?:values?|scores?))?|scores?|values?)|raw\s+(?:cosines?|cosine(?:-style)?\s+(?:values?|range)))\b[\s\S]{0,240}?-?(?:0\.\d{2,}|1\.0{2,})/i,
  /\b(?:SFace|biometric)\b[^\n]{0,120}\b(?:cosines?|similarity|scores?)\b[\s\S]{0,160}?-?(?:0\.\d{2,}|1\.0{2,})/i,
];
const concreteBiometricJsonScorePattern =
    /["'](?:raw_)?score["']\s*:\s*-?(?:0\.\d+|1\.0+)/i;
const secretValuePatterns = [
  /DISCORD_(?:BOT|USER)_TOKEN\s*=\s*["']?[^\s"']{16,}/i,
  /(?:authorization\s*[:=]\s*["']?bearer|bearer\s+)[A-Za-z0-9._~-]{20,}/i,
  /(?<![A-Za-z0-9])AKIA[A-Z0-9]{16}(?![A-Za-z0-9])/,
  /(?<![A-Za-z0-9])AIza[A-Za-z0-9_-]{30,}(?![A-Za-z0-9_-])/,
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/,
  /(?<![A-Za-z0-9])gh[opsu]_[A-Za-z0-9_]{20,}/,
  /(?<![A-Za-z0-9])sk-[A-Za-z0-9_-]{20,}/,
  /(?<![A-Za-z0-9])xox[baprs]-[A-Za-z0-9-]{20,}/,
];

for (const relativePath of sourceFiles) {
  let source;
  try {
    const contents = await readFile(path.join(projectRoot, relativePath));
    const extension = path.extname(relativePath).toLowerCase();
    if (allowedSiteBinaryExtensions.has(extension)) {
      if (!hasExpectedBinarySignature(extension, contents)) {
        failures.push(
            `File does not match its allowlisted site-asset format: ${relativePath}`,
        );
        continue;
      }
      // Scan ASCII metadata and embedded strings even in approved site assets.
      source = contents.toString("latin1");
    } else if (contents.includes(0)) {
      failures.push(
          `Unapproved binary format is publication-visible: ${relativePath}`,
      );
      continue;
    } else {
      source = new TextDecoder("utf-8", { fatal: true }).decode(contents);
    }
  } catch (error) {
    if (error?.code === "ENOENT") continue;
    failures.push(
        `Could not scan publication-visible text ${relativePath}: ${error.message}`,
    );
    continue;
  }

  if (absoluteLocalPathPattern.test(source)) {
    failures.push(
        `Local filesystem path found in public source: ${relativePath}`,
    );
  }

  if (signedDiscordUrlPattern.test(source)) {
    failures.push(
        `Access-bearing Discord URL found in public source: ${relativePath}`,
    );
  }

  for (const match of source.matchAll(accessBearingUrlPattern)) {
    let hostname = "";
    try {
      hostname = new URL(match[0]).hostname.toLowerCase();
    } catch {
      // A malformed access-bearing URL is still unsafe to publish.
    }
    if (
        hostname === "example.com" ||
        hostname === "example.net" ||
        hostname === "example.org" ||
        hostname === "example.test" ||
        hostname.endsWith(".test")
    ) {
      continue;
    }
    failures.push(`Access-bearing URL found in public source: ${relativePath}`);
    break;
  }

  if (secretValuePatterns.some((pattern) => pattern.test(source))) {
    failures.push(
        `Possible credential value found in public source: ${relativePath}`,
    );
  }

  if (
      prohibitedBiometricScoreDisclosurePatterns.some((pattern) =>
          pattern.test(source),
      )
  ) {
    failures.push(
        `Raw biometric score found in public source: ${relativePath}`,
    );
  }

  if (
      source.includes("cosine_similarity_raw_v1") &&
      concreteBiometricJsonScorePattern.test(source)
  ) {
    failures.push(
        `Concrete raw biometric score object found in public source: ${relativePath}`,
    );
  }

  for (const match of source.matchAll(assetPattern)) {
    assetReferences.add(decodeURI(match[0]).replace(/^\//, ""));
  }
}

for (const assetReference of assetReferences) {
  if (!(await pathExists(path.posix.join("public", assetReference)))) {
    failures.push(`Referenced public asset is missing: /${assetReference}`);
  }
}

if (failures.length > 0) {
  console.error("Public-release checks failed:");
  for (const failure of failures) console.error(`- ${failure}`);
  process.exitCode = 1;
} else {
  console.log(
      `Public-release checks passed (${publicFiles.length} public files; ${assetReferences.size} local image references).`,
  );
}
