# Trusted GPU launcher v2

Status: implemented and CPU-contract tested; not yet production-admitted. All
nine admission gates must pass for the exact final launcher, image, runtime
candidate, production profile, and evidence set before production use.

`pipeline/gpu/trusted_launcher_v2.py` is the host-side trust boundary for the GPU
successor. The installed file starts directly with the exact regular
`/usr/bin/python3.14 -IB`; it rejects production if isolated mode, disabled user
site loading, or disabled bytecode writes are absent. Its imports are standard
library only. Before inference it:

1. hashes the installed launcher and the exact seven-tool host closure, including
   `nvidia-smi` and `host_python`, and compares every byte/mode/path to the sealed
   launcher profile;
2. retains and exactly replays the launcher profile, runtime receipt, production
   profile, root registration, and reviewed batch manifest;
3. requires every production runtime gate to be `passed` and bound to the same
   `runtime_candidate_identity_sha256` as the receipt;
4. retains the registered hot-root directory descriptor, checks every path
   component, and replays its Btrfs filesystem UUID;
5. streams one full SHA-256 pass over the retained SquashFS descriptor without
   buffering the image in memory;
6. resolves the admitted GPU UUID with the pinned `nvidia-smi`, independently
   resolves its `/dev/nvidiaN` minor, requires the exact profile driver version,
   and checks the compute-capability floor;
7. replays the embedded host-ABI manifest against the booted kernel/NVIDIA
   module and every exact root-owned shared-library file, alias chain, digest,
   size, mode, owner, and ELF dependency edge;
8. mounts that image descriptor with foreground `squashfuse`, retains every
   admitted mapping as an open descriptor, and hands each source to Bubblewrap's
   numeric `--ro-bind-fd`/`--bind-fd` interface;
9. runs the GPU-less lineage preflight described below, validates its bounded
   canonical attestation, then constructs the main launch attestation;
10. launches a second, networkless Bubblewrap namespace with only exact retained
   source/control files, three declared writable directories, admitted image
   mappings, descriptor-bound host-ABI files, and the selected GPU devices; and
11. rechecks the host-ABI platform, resource envelope, and all retained
    identities, cleanly unmounts, and deletes only its fixed private transient
    tree.

Production also requires a finite kernel-enforced host envelope: effective
`memory.max` at most 12 GiB, `memory.swap.max=0`, effective `pids.max` at most 64,
`RLIMIT_NOFILE` at most 1024, `RLIMIT_CORE=0`, and `RLIMIT_FSIZE` between the
profile's per-result maximum and 32 MiB. Both soft and hard rlimits are checked.
Synthetic candidate mode records violations as `report_only_failed`. Both root
`production` and `local-private-production` enforce the envelope before the image
is mounted, and re-observe the exact envelope after the worker exits.

## Two filesystem phases

The short lineage phase has no GPU, network, result/event/lock roots, HOME, or
inference authority. A production queue replay needs same-Btrfs-mount semantics
and may traverse references not selected into the batch, so this phase alone gets
one descriptor-stable, read-only bind of the registered hot root. It writes only
one no-replace mode-`0400` lineage attestation into a launcher-owned mode-`0700`
transient directory, with bounded stdout/stderr and a ten-minute deadline.

Both phases use `--unshare-all` plus an explicit `--unshare-user` before
`--disable-userns`. The explicit token is required by pinned Bubblewrap 0.11 even
though user isolation is already implied by `--unshare-all`; omitting it makes the
launch fail before the payload starts.

Both production-lineage modes receive that root bind for typed queue replay. In
the local mode it is still descriptor-stable and read-only, but it is not a root
trust anchor. The synthetic candidate lineage phase never receives that root bind. It receives
only the retained fixture manifest and synthetic audio files at their original
absolute work-order paths. Empty parent mountpoints may be created, but no parent
directory contents are exposed.

The main GPU phase never binds the hot-root directory. Each full v5 work order is
the authority for one input plus its exact lineage JSON files. The launcher opens
each file with no symlink following, checks owner/mode/size/digest, retains the
descriptor, and binds it at its original absolute work-order path. The archive
tier and any candidate corpus media remain prohibited. Writable result, event,
and lock roots cannot equal, contain, or be contained by a retained read or
control artifact.

For image efficiency, both phases bind the admitted application,
application-support, and runtime roots once. The main phase additionally binds
the model bundle. Media probing uses authenticated bundled PyAV; neither a shell
wrapper nor a mutable host probe executable is in the image mapping closure. All
logical child mappings remain exact receipt evidence. The verified mapping
descriptors, rather than mutable FUSE pathnames, are the actual bind sources.
Pinned Bubblewrap 0.11 consumes and closes each numeric source descriptor while
constructing the namespace. This is security-critical: passing
`/proc/self/fd/N` as an ordinary pathname leaves that FD inherited by the payload,
and a retained read-only directory FD could otherwise bypass a read-only bind via
`openat`. Controls that need two targets receive two duplicated descriptors so
each FD is consumed exactly once.

The host runtime boundary is file-exact. The launcher profile embeds a canonical
`himr_gpu_host_abi_manifest` generated from a complete ELF scan of the exact
execution-image receipt. The scan classifies every projected regular file,
records `DT_RPATH` separately from `DT_RUNPATH`, and recomputes a canonical set
of initial load roots: the one bundled Python executable, projected Python
extension modules outside the three private `.libs` directories, and the two
explicit CUBLAS `dlopen` roots. Dependency resolution retains a distinct bounded
state for each inherited-RPATH context. `DT_RPATH` is inherited by descendants;
`DT_RUNPATH` applies only to the object that declares it. Only reachable
`$ORIGIN` directories and the fixed bundled CUBLAS directory can provide an
in-image dependency. Every projected ELF remains hashed and recorded, but an
unreachable private implementation library is not invented as an independent
load root. The scan then adds the explicitly audited runtime-loaded NVIDIA
libraries. The production manifest is limited to the real root-owned
`/usr/lib64` directory, records every symlink chain and recursive dependency
edge, and is tied to the execution-image identity and exact kernel, NVIDIA
module report, and driver version.
Absolute loader paths, empty path components, `$LIB`, `${ORIGIN}`, and every
other unsupported loader substitution are rejected at manifest validation;
silently ignoring a path that glibc might honor would create an uninventoried
provider channel.

At launch, `/`, `/usr`, and `/usr/lib64` are retained component-by-component;
aliases and libraries are opened relative to those descriptors. Each source is
hashed and reparsed as ELF, then its retained descriptor is bound once at the
manifest `sandbox_path` in both phases. Alias and resolved paths are provenance
only. No host library directory, `/usr`, or `/etc/ld.so.cache` is mounted. After
all exact mounts are constructed, Bubblewrap applies one non-recursive
`--remount-ro /`; authorized writable submounts remain writable while synthetic
parent directories (including `/usr/lib64`) cannot accept unmanifested files.
The launch attestation records manifest identity, successful platform/library
replay, exact per-file bind evidence, and binding count.

The current CTranslate2 candidate maps its authenticated CUBLAS directory but no
cuDNN directory or package; this reflects the candidate's measured dynamic
dependency closure and must be regenerated if that closure changes.

The pinned 610.57.04 driver closure includes both `libnvidia-nvvm.so.4` and
`libnvidia-nvvm70.so.4`; `libcuda` selects the latter for its embedded CUDA 13.3
tool path. TileIR and NVIDIA PKCS#11 provider loading are deliberately outside
this fixed RTX 3050 CC 8.6 ASR workload. Their versioned DSOs are not bound, so
attempting either unapproved feature fails closed inside the read-only sandbox
instead of discovering ambient host libraries. Enabling either feature requires
a new traced canary, host-ABI manifest, launcher profile, execution-image
admission, and runtime admission.

The numeric `nvidia-smi` index is observation only. `CUDA_VISIBLE_DEVICES` is set
to the admitted `GPU-...` UUID, so the selected device is stable across host index
reordering; the worker still sees visible device zero. The independently observed
device minor selects the single `/dev/nvidiaN` node. For schema compatibility the
profile field remains named `minimum_driver_version`, but launcher v2 treats its
value as an exact pin; both older and newer observed drivers are rejected. The
exact observed version is sealed into the launch attestation.

## Why ownership replay is outside Bubblewrap

An unprivileged user namespace maps host-root-owned files to overflow UID 65534.
It therefore cannot faithfully replay host UID-0 ancestry inside the sandbox.
The external launcher performs those checks before Bubblewrap and gives the worker
a sealed, read-only attestation containing the exact image, runtime, profile,
root, GPU UUID/index/minor, host-ABI identity and exact bindings, lineage
preflight, resource envelope, per-item read
bindings, parent network namespace, and sandbox-plan identity. The v2 worker
validates this compact attestation and must not reinterpret namespace UIDs as host
ownership.

## Image and control installation model

Production trust anchors are root-owned but world-readable, not root-private:

| Artifact | Final mode | Reason |
| --- | ---: | --- |
| external launcher | `0555` | executable but not mutable by the operator user |
| launcher profile | `0444` | readable by the user service, mutable only by root |
| execution SquashFS | `0444` | hashable/mountable by the user service, immutable to it |
| execution-image receipt | `0444` | exact root-controlled image binding |
| admitted runtime receipt | `0444` | reviewed production authority consumed by launcher |
| production batch manifest | `0444` | reviewed full-v5 work-order authority, root-owned and single-link |

The production profile and hot-root registration remain canonical single-link
controls in the registered hot tree and may be current-user-owned mode `0400`;
their exact hashes and semantic identities are bound by the root-owned launcher
profile/runtime authority. The production batch manifest is deliberately stricter:
it must itself be root-owned mode `0444`, even though its registered-hot-root
ancestors may be owned by the service user. Every referenced input/lineage file
is then opened and retained independently under the policy encoded by its full v5
work order.

The directories above the root-owned launcher, launcher profile, image, image
receipt, and admitted runtime receipt must also be root-owned and not group/other
writable. A mode-`0400` root-owned image or receipt is intentionally rejected as
an unusable deployment: a `systemd --user` service could not read it.

The image must be built at its final lexical pathname because that path is part of
the execution-image identity. One workable transition is to have root create a
temporary current-user-owned mode-`0700` final build directory, build there, then
review and atomically transition the directory, image, and receipt to root
ownership/modes before admission. Do not copy a completed image to a different
pathname and continue using its old receipt.

## Local-private production (unprivileged)

`local-private-production` is a distinct, weaker lane for starting reviewed
private work before root installation and long-form admission finish. It accepts
only production-lineage batches with execution class
`local_private_production_asr` and a `candidate` runtime bound to the same image,
profile, registered root, launcher, and exact host tools. The launcher is
current-user mode `0500`; its profile, runtime receipt, image, image receipt,
production profile, root registration, batch, and readiness receipt are
current-user mode `0400`.

This mode retains the production network namespace, GPU/device checks, full image
hash, per-input hashing, exact lineage replay, host-ABI closure, resource limits,
and absence of import/publication/catalogue/archive/deletion authority. It does
not claim protection from another process running as the same UID. Every launch
attestation records `current_user_same_uid` and
`same_uid_mutation_resistance=false`; it is never promoted to root `production`
implicitly.

Build the fresh image with `--image-mode 0400`, then generate controls without
pasting the roughly megabyte-sized host-ABI object into a hand-written profile:

```sh
pipeline/bin/prepare-local-private-production-v1 prepare \
  --execution-image-receipt /absolute/hot/image-receipt.json \
  --expected-execution-image-receipt-sha256 SHA256 \
  --production-profile /absolute/hot/production-profile.json \
  --expected-production-profile-sha256 SHA256 \
  --root-registration /absolute/hot/root-registration.json \
  --expected-root-registration-sha256 SHA256 \
  --host-abi-manifest /absolute/hot/host-abi.json \
  --expected-host-abi-manifest-sha256 SHA256 \
  --launcher-output /absolute/hot/controls/trusted-launcher-v2 \
  --launcher-profile-output /absolute/hot/controls/launcher-profile.json \
  --runtime-admission-output /absolute/hot/controls/runtime-candidate.json
```

The output parents must already be current-user mode `0700`; preparation never
replaces different bytes. Next run the copied launcher itself under the same
systemd-user memory/swap/PID/rlimit envelope used for a batch:

```sh
/absolute/hot/controls/trusted-launcher-v2 doctor-local-private \
  --runtime-admission /absolute/hot/controls/runtime-candidate.json \
  --expected-runtime-admission-sha256 SHA256 \
  --production-profile /absolute/hot/production-profile.json \
  --expected-production-profile-sha256 SHA256 \
  --root-registration /absolute/hot/root-registration.json \
  --expected-root-registration-sha256 SHA256 \
  --launcher-profile /absolute/hot/controls/launcher-profile.json \
  --expected-launcher-profile-sha256 SHA256 \
  --readiness-output /absolute/hot/controls/readiness.json
```

The doctor reads no media and performs no inference. It replays and hashes the
current image, host ABI, GPU UUID, candidate runtime, and enforced resource
envelope, then writes one no-replace mode-`0400` readiness receipt. A local real
batch fails before media hashing unless `run` receives that receipt and digest via
`--local-readiness` and `--expected-local-readiness-sha256`. A same-image/current-
launcher synthetic canary may be run separately with
`candidate-synthetic-canary`; it does not grant root production admission.

## Stage the launcher (no automatic sudo)

Create a canonical mode-`0400` install specification. It binds final paths and
identities; placeholders below must be replaced with reviewed values:

```json
{
  "execution_image": {
    "byte_count": 123,
    "identity_sha256": "EXECUTION_IMAGE_IDENTITY_SHA256",
    "path": "/var/lib/himr-gpu/execution-v2.squashfs",
    "receipt_path": "/var/lib/himr-gpu/execution-v2-receipt.json",
    "receipt_sha256": "EXECUTION_IMAGE_RECEIPT_SHA256",
    "sha256": "EXECUTION_IMAGE_SHA256"
  },
  "host_abi": "REPLACE_WITH_FULL_CANONICAL_HOST_ABI_MANIFEST_OBJECT",
  "kind": "himr_gpu_trusted_launcher_install_spec",
  "launcher_install_path": "/usr/local/libexec/himr-gpu/trusted-launcher-v2",
  "launcher_profile_install_path": "/etc/himr-gpu/launcher-profile-v2.json",
  "policy": {
    "archive_access": false,
    "biometric_authority": "none",
    "candidate_authority": "sealed_synthetic_canary_only",
    "local_private_production_authority": "production_lineage_candidate_runtime_same_uid",
    "catalogue_mutation_authority": "none",
    "deletion_authority": "launcher_owned_transient_mount_only",
    "identity_authority": "none",
    "network_access": false,
    "publication_authority": "none",
    "visibility": "private",
    "wheelhouse_execution_dependency": false,
    "wiki_authority": "none"
  },
  "production_profile": {
    "identity_sha256": "PRODUCTION_PROFILE_IDENTITY_SHA256",
    "path": "/absolute/hot/path/production-profile-v2.json",
    "sha256": "PRODUCTION_PROFILE_FILE_SHA256"
  },
  "root_registration": {
    "identity_sha256": "ROOT_REGISTRATION_IDENTITY_SHA256",
    "path": "/absolute/hot/path/root-registration-v1.json",
    "registration_id": "gpurootreg_00000000000000000000000000000000",
    "root_id": "himr-hot-main-v1",
    "sha256": "ROOT_REGISTRATION_FILE_SHA256"
  },
  "runtime_admission_install_path": "/etc/himr-gpu/runtime-admission-v2.json",
  "sandbox": {
    "control_root": "/run/himr-gpu/control",
    "gpu_control_devices": [
      "/dev/nvidiactl",
      "/dev/nvidia-uvm",
      "/dev/nvidia-uvm-tools"
    ],
    "image_mapping_prefix": "/opt/himr-gpu",
    "input_root": "/run/himr-gpu/input",
    "output_root": "/run/himr-gpu/output",
    "state_root": "/run/himr-gpu/state",
    "system_library_directories": [],
    "system_readonly_files": []
  },
  "schema_version": 2,
  "system_tool_paths": {
    "bubblewrap": "/usr/bin/bwrap",
    "fusermount": "/usr/bin/fusermount3",
    "host_python": "/usr/bin/python3.14",
    "nvidia_smi": "/usr/bin/nvidia-smi",
    "squashfuse": "/usr/bin/squashfuse_ll",
    "systemctl": "/usr/bin/systemctl",
    "systemd_run": "/usr/bin/systemd-run"
  }
}
```

The `host_abi` placeholder must be replaced by the JSON object itself, not a
pathname or digest-only reference. Generate it only after the final
execution-image receipt exists; creation authoritatively replays that receipt
and its full image hash:

```sh
pipeline/gpu/host_abi_manifest_v1.py create \
  --execution-image-receipt /var/lib/himr-gpu/execution-v2-receipt.json \
  --expected-execution-image-receipt-sha256 REVIEWED_SHA256 \
  --nvidia-driver-version 610.57.04 \
  --output /absolute/private/host-abi-v1.json

pipeline/gpu/host_abi_manifest_v1.py validate \
  --manifest /absolute/private/host-abi-v1.json \
  --expected-manifest-sha256 REVIEWED_SHA256 \
  --replay \
  --observed-driver-version 610.57.04
```

Insert the canonical contents of `host-abi-v1.json`, then compute the install
spec's physical SHA-256. The launcher rejects a consumer scan whose
execution-image identity differs from the install spec.

Then stage without privilege escalation:

```sh
install_spec=/absolute/private/launcher-install-spec-v2.json
install_spec_sha256=REVIEWED_CANONICAL_SHA256
staging=/absolute/private/empty-mode-0700-launcher-stage

pipeline/bin/launch-gpu-batch-v2 stage-install \
  --spec "$install_spec" \
  --expected-spec-sha256 "$install_spec_sha256" \
  --staging-dir "$staging"
```

The command refuses a nonempty or non-`0700` staging directory. It writes a
mode-`0500` launcher, mode-`0400` canonical profile, and mode-`0400` installation
manifest. The manifest contains only explicit `/usr/bin/install` argv arrays; it
does not invoke sudo. Review every digest and destination, then run the chosen
arrays with appropriate root authority. Install the admitted runtime receipt only
after the installed launcher/profile/image and all nine evidence gates replay.

Any launcher/profile/image change creates a new identity and invalidates earlier
launcher-isolation and performance evidence.

## Run interface

Production execution uses the root-owned launcher directly, normally from the
bounded systemd-user supervisor:

```sh
/usr/local/libexec/himr-gpu/trusted-launcher-v2 run \
  --mode production \
  --batch-manifest /absolute/hot/sealed/batch.json \
  --expected-batch-sha256 SHA256 \
  --runtime-admission /etc/himr-gpu/runtime-admission-v2.json \
  --expected-runtime-admission-sha256 SHA256 \
  --production-profile /absolute/hot/production-profile-v2.json \
  --expected-production-profile-sha256 SHA256 \
  --root-registration /absolute/hot/root-registration-v1.json \
  --expected-root-registration-sha256 SHA256 \
  --launcher-profile /etc/himr-gpu/launcher-profile-v2.json \
  --expected-launcher-profile-sha256 SHA256 \
  --writable-result-root /absolute/hot/mode-0700-results \
  --writable-event-root /absolute/hot/mode-0700-events \
  --writable-lock-root /run/user/UID/himr-gpu-mode-0700-locks
```

The production batch file must be canonical, single-link, root-owned mode `0444`,
and contain each complete v5 work order—not a caller-authored summary or path-only
projection. The three writable roots must be current-user-owned mode `0700`,
distinct, non-nested, and disjoint from all exact read/control artifacts. Result
and event roots must be within the registered hot root. The lock root may instead
be below `/run/user/UID`. The launcher never accesses the cold archive root.

The production service must establish the resource envelope before invoking the
launcher. It is not enough to put limits in a unit file if the effective cgroup or
process rlimits differ; the launcher reads the kernel state. The expected service
settings are 12 GiB memory, zero swap, 64 tasks, no core files, at most 1024 open
files, and a 32 MiB file-size limit.

Candidate mode is named `candidate-synthetic-canary` deliberately. It accepts only
`execution_class: synthetic_canary`; every item must bind
`source_lineage.kind: synthetic_canary`, `contains_corpus_media: false`, and
`corpus_authority: none`. Production requires
`execution_class: production_private_asr`, v03 preprocess lineage for every item,
an admitted runtime reference, and a successful typed queue-v1 replay in the
lineage phase. Mixed batches fail before inference.

## Cancellation and recovery

The supervisor should use `KillMode=control-group`, a hard `RuntimeMaxSec`, and a
graceful interrupt before SIGKILL. The launcher serializes instances with an
exclusive lock below `/run/user/UID/himr-gpu-launcher-v2` and uses only the fixed
`transient/{image,preflight-output,launch-attestation.json}` closure.

Foreground `squashfuse` is started in its own session with Linux
`PR_SET_PDEATHSIG=SIGKILL`; the pre-exec hook also checks the parent PID after
arming the signal. Thus abrupt launcher death kills the helper instead of leaving
an orphan process. A dead FUSE helper can still leave a kernel mount record, so
the next locked launch explicitly recognizes the allowlisted stale tree, performs
a normal pinned-`fusermount3` unmount, validates every leftover inode, and removes
it before creating a new tree. No lazy unmount or recursive deletion is used.
Unknown entries, unsafe ownership/modes, or a mount that survives clean unmount
fail closed.

SIGINT/SIGTERM terminates the active Bubblewrap child, then performs the same
cleanup. The operator must still reconcile the persistent result/event journal
before admitting another batch; launcher cleanup has no authority over those
roots.

## Residual production prerequisites

This design does not make an unadmitted image production-ready by itself. Before
deployment, review and pass the exact final admission evidence, install the
launcher/profile/image/runtime controls at their sealed paths and modes, exercise
the real two-phase Bubblewrap path under the final service envelope, and verify
clean shutdown plus forced-death recovery on the target kernel/FUSE versions.
Root can always replace host tools, the interpreter, devices, or kernel state;
that privileged-host trust is explicit and outside this unprivileged launcher.
The earlier broad-host-library gap is closed by the manifest-bound,
descriptor-stable per-file ABI closure. A root-level replacement or live kernel
reconfiguration remains outside the threat boundary; launch-start and launch-end
platform replay narrows, but cannot eliminate, that privileged race.
Production also still performs a full SquashFS SHA-256 pass on every launch. That
is the conservative integrity boundary for v2 but can dominate startup on a large
image; an fs-verity successor should bind the kernel measurement into admission
before replacing this scan. Root ownership or Btrfs checksums alone are not an
equivalent content identity.
