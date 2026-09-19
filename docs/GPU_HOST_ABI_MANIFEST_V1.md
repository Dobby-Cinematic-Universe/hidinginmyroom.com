# GPU host ABI manifest v1

The GPU execution image is deterministic, but it cannot contain the NVIDIA
kernel driver. The production sandbox therefore needs a small set of host
shared libraries. `pipeline/gpu/host_abi_manifest_v1.py` makes that residual ABI
an exact, content-addressed input instead of exposing mutable `/usr/lib*`
directories.

The manifest is embedded in the root-owned trusted-launcher profile. Its profile
hash is already part of the runtime-candidate identity, so any kernel, driver,
library, symlink, consumer, or dependency change invalidates all earlier gate
evidence.

## What is bound

The canonical manifest records:

- the exact Linux release/version and x86-64 machine;
- the NVIDIA user-driver version, kernel-module version, and complete
  `/proc/driver/nvidia/version` report hash;
- the execution-image identity, source-tree identity, and hash/ELF metadata for
  every projected ELF regular file;
- the exact initial execution roots: bundled Python, every admitted projected
  Python extension module, and the two explicit bundled cuBLAS `dlopen` roots;
- literal in-image provider filenames reachable in that root's loader context.
  `DT_RPATH` and `DT_RUNPATH` are distinct: RPATH is inherited down a dependency
  chain, while RUNPATH applies only to direct dependencies. Private `av.libs`,
  `ctranslate2.libs`, and `numpy.libs` directories are never global authority;
- explicit runtime-loaded roots for `libcuda.so.1`, `libnvidia-ml.so.1`, the
  generic and CUDA-13 `libnvidia-nvvm` variants, the NVIDIA PTX/JIT components,
  and the ELF interpreter;
- every unresolved host SONAME and its recursive `DT_NEEDED` graph;
- each library's loader-facing sandbox path, resolved host source path, exact
  symlink chain, SHA-256, size, owner, mode, and parsed ELF metadata.

Every projected ELF is inventoried even when it is not reachable from an admitted
execution root. Unreachable objects do not silently seed host dependencies.
Only a literal projected basename can satisfy `DT_NEEDED`; `DT_SONAME` metadata
cannot stand in for a missing file or symlink. The parser reads ELF structures
directly. It never executes an inspected object, uses `ldd`, or treats
`/etc/ld.so.cache` as authority.

Loader search components are closed to exact `$ORIGIN` forms. Absolute paths,
empty/current-directory components, `$LIB`, `$PLATFORM`, `${ORIGIN}`, and other
substitutions are rejected rather than silently misclassified. For driver
610.57.04, `libnvidia-nvvm70.so.4` is a required explicit root because libcuda's
CUDA-13 JIT path selects it; the generic `libnvidia-nvvm.so.4` remains required.
The CC 8.6 RTX 3050 lane deliberately prohibits TileIR and NVIDIA PKCS#11 DSOs:
the reviewed short and 59.45-second FP16/beam-5 paths do not activate those
features, and unexpected activation must fail closed instead of widening the
host ABI.

## Generate the manifest

Start only after the final execution image and receipt exist on the main drive.
The receipt must already have been created by the authoritative image builder.
Use the production profile's exact driver version:

```sh
private_dir=/absolute/main-drive/mode-0700-control
image_receipt=/absolute/main-drive/execution-image-v2-receipt.json
image_receipt_sha256=REVIEWED_CANONICAL_SHA256

python3 -IB pipeline/gpu/host_abi_manifest_v1.py create \
  --execution-image-receipt "$image_receipt" \
  --expected-execution-image-receipt-sha256 "$image_receipt_sha256" \
  --nvidia-driver-version 610.57.04 \
  --output "$private_dir/host-abi-v1.json"
```

Creation performs the authoritative receipt replay and complete SquashFS hash,
then sequentially rehashes every projected regular source to prove the ELF
classification. Large projected DSOs are read through a bounded, read-only
mapping rather than copied wholesale into Python memory. This is a one-time
admission operation, not per-launch work. It does not access the cold archive or
media.

Review at least:

```sh
python3 -IB pipeline/gpu/host_abi_manifest_v1.py validate \
  --manifest "$private_dir/host-abi-v1.json" \
  --expected-manifest-sha256 REVIEWED_HOST_ABI_FILE_SHA256 \
  --replay \
  --observed-driver-version 610.57.04
```

Production accepts only root-owned, non-writable libraries below the exact
root-owned `/usr/lib64` ancestry. The generated manifest is copied into the
launcher install specification as the `host_abi` object; the standalone file is
review material, not an extra unbound launch dependency.

## Launch behavior

The installed single-file launcher embeds the validator logic. It does not
import this generator at runtime. Before either Bubblewrap phase it:

1. retains `/`, `/usr`, and `/usr/lib64` through a no-symlink `openat`
   component walk;
2. replays the booted kernel and NVIDIA module identity;
3. replays every alias, regular-file byte count/hash/owner/mode, and ELF graph;
4. retains the verified regular-file descriptors;
5. binds each retained descriptor exactly once at its manifest `sandbox_path`.

Only those individual files are visible. The sandbox has no broad `/usr/lib` or
`/usr/lib64` bind and no loader-cache bind. `/lib64` is a fixed sandbox symlink to
`/usr/lib64`, so the explicitly inventoried loader satisfies `PT_INTERP`.
After all file and authorized writable-submount binds are installed, Bubblewrap
remounts the synthetic `/` read-only. This is required: Bubblewrap 0.11 creates
`--dir` parents owned by the sandbox user, so individual read-only file binds
alone would still permit an uninventoried DSO to be created beside them.

Alias paths and resolved `source_path` values are integrity provenance. They are
not additional sandbox targets unless they independently appear as a manifest
library row. Thus `binding_count` equals `library_count`.

## Admission and change control

The manifest closes content identity, not compatibility. A real mounted
synthetic model-load/import canary and all accuracy, thermal, scheduler,
recovery, and isolation gates still must pass for this exact image/profile/host
ABI identity. The runtime is not production-admitted until the final eight-hour
gate and every other required gate pass.

An explicit driver update, kernel update, library update, execution-image change,
or consumer-set change requires a successor manifest/profile and fresh evidence.
Do not edit or retract an admitted manifest in place.

## Current final-a review artifact

The authoritative final-a generation on 2026-08-29 used receipt SHA-256
`f1ac8aeda1472faf4af5a1d984b040195d017fe1c7d6ddbbb5d25d042eeb4be0`
and execution-image SHA-256
`4cf78aff6577aaf8b73cbada5553ad60fff8b1049c160b2b1510da11536fbe1b`.
It produced 111 audited ELF consumers, 74 exact image load roots, 17 host
libraries, and 47 recursive host edges. The canonical manifest identity is
`98f1e80789e0547fb654b16c191c2a8bd0b234137ad6154f353555b24654dcb8`;
the standalone file SHA-256 is
`8423652aad422807b83facda5496a97f48166b5e7830e4fdc648238bde68e2d9`.
This review artifact does not itself admit production; the exact launcher
profile and all runtime gates remain required.
