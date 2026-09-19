# Selective torrent acquisition

The planning and selector portions of this lane are read-only. They do not start a
torrent client, join a swarm, read payload pieces, mutate the catalogue, or authorize
publication. Their purpose is to minimize and bind a later, separately reviewed
acquisition. The current private acquisition and its read-only runtime auditor are
documented separately below.

## Decision sequence

For the four reviewed video directories, the planner:

1. replays the exact local `.torrent` bytes, retained discovery record, and prior
   torrent-manifest catalogue import;
2. accepts only the existing terminal `[YouTubeID].video` and
   `[YouTubeID] 480p.video` byte grammars;
3. excludes IDs with an exact locator already present in an approved catalogue
   namespace or a sealed Archive.org metadata snapshot;
4. chooses one smallest file for each remaining ID, using the lower torrent file
   index only as a tie-break;
5. emits a credential-free, no-download YouTube availability-probe request;
6. selects a file index only when a matching, complete probe says that ID is
   unavailable; and
7. leaves available, indeterminate, malformed, missing, reordered, or tampered
   outcomes unselected.

An exact matching locator is enough to avoid redundant torrent transfer, but it
is not treated as proof that two files have identical content. The plan creates no
recording relationship or merge.

The default CLI output is path-free. `--full` exposes private torrent paths, probe
targets, manual-review paths, and eventual file indices, so full plans belong only
under the ignored `research/` workspace. Combine `--full` with `--output` to publish
the complete plan atomically without printing it. The output must be an absolute path
under an existing real directory, must not already exist, and is created mode `0400`.

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  plan-torrent-selective-acquisition \
  --db research/corpus/private-catalog-backups/CATALOG.sqlite3 \
  --torrent research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent \
  --discovery-metadata research/corpus/discovery/reddit/1pqdsxm/discovery.json \
  --archive-snapshot research/corpus/archive-org-metadata/snapshots/SNAPSHOT/snapshot.json
```

The catalogue must be a closed, checkpointed file with no WAL, SHM, or journal
sidecar. The command opens it with SQLite's immutable audit reader. Repeat
`--archive-snapshot` when separately sealed snapshots add non-overlapping source
history.

## Availability result contract

The full pre-probe plan contains `availability_probe_request`. A producer must
process every target once, in the exact listed order, with the request's fixed
yt-dlp flags:

```text
--ignore-config --skip-download --no-playlist --no-warnings --dump-single-json
```

The producer records its version and executable SHA-256 and must send no cookies
or authorization. It writes one strict result conforming to
`corpus/schemas/torrent-youtube-availability-probe.schema.json`. Raw error messages
are deliberately not decision inputs. Outcomes are reduced to:

- `available / metadata_resolved`;
- `unavailable / private|removed|video_unavailable`; or
- `indeterminate / sign_in_required|network_error|rate_limited|extractor_error`.

`sign_in_required` deliberately remains indeterminate. Age gates, members-only
pages, and bot-confirmation challenges do not prove that a video is absent, and
therefore never authorize torrent selection. Explicit private, removed, or
video-unavailable responses are the only unavailable classifications.

## Resumable producer

The corpus CLI runs the exact request without shell redirection. It requires an
expected yt-dlp version and lowercase executable-file SHA-256, writes an owner-only
checkpoint after each normalized outcome, and atomically publishes a read-only
final result only after every target has an outcome:

```sh
probe_root="$PWD/research/corpus/torrent-availability"
ytdlp="$PWD/research/corpus/.venv/bin/yt-dlp"
mkdir -p "$probe_root"

PYTHONPATH=corpus/src python -m himr_corpus \
  produce-torrent-youtube-availability-probe \
  --input "$PWD/research/corpus/torrent-selective/full-plan.json" \
  --yt-dlp-executable "$ytdlp" \
  --yt-dlp-sha256 "$(sha256sum "$ytdlp" | cut -d' ' -f1)" \
  --yt-dlp-version "$($ytdlp --ignore-config --version)" \
  --checkpoint "$probe_root/checkpoint.json" \
  --output "$probe_root/probe.json"
```

The producer also accepts the planner's original `--db`, `--torrent`,
`--discovery-metadata`, and repeatable `--archive-snapshot` inputs instead of
`--input`; it then derives the exact request in memory. Its stdout is a path-free
summary in either mode. Restart the same command after an interruption. The
checkpoint is bound to the request digest, exact outcome prefix, executable hash,
reported version, flags, and credential/media policy; its canonical content digest
catches accidental or casual state edits. A separate nonblocking lock rejects
concurrent writers. Existing valid final output is reused rather than overwritten.

Each target runs in its own process with an empty temporary home and exactly the
five flags printed above. Ambient cookie, authorization, proxy, netrc, Python-path,
and yt-dlp variables are not inherited. Diagnostic stdout/stderr use unlinked,
size-bounded temporary files and are never copied into the checkpoint or result.
Timeouts, output overflow, rate limiting, sign-in gates, malformed metadata, and
unrecognized extractor errors all remain indeterminate.

The executable SHA-256 pins only the supplied executable file. If that file is a
Python launcher, it does **not** by itself hash-pin the imported `yt_dlp` package
tree. Prefer a standalone yt-dlp binary or a separately frozen and reviewed Python
environment; the result contract does not claim a module-tree pin. JavaScript or
other helper runtimes that yt-dlp discovers on `PATH` are likewise not hash-pinned
by this result.

Attach that separate result with `--availability-probe`. The planner verifies its
request digest, complete ordered target set, producer pin, and credential/media
policy. Only `unavailable` rows appear in `selected_torrent_file_indices`. Regenerate
the finalized private plan with `--full --output /absolute/ignored/path/plan.json`;
do not copy the full JSON from terminal history.

The tracked selective-acquisition schemas are:

- `corpus/schemas/torrent-selective-acquisition-plan.schema.json`
- `corpus/schemas/torrent-youtube-availability-probe.schema.json`
- `corpus/schemas/torrent-acquisition-audit-receipt.schema.json`

The private resumable state additionally conforms to
`corpus/schemas/torrent-youtube-availability-probe-checkpoint.schema.json`.

## Current pinned replay

The 2026-08-27 pre-probe replay of torrent SHA-256
`a0119dd75b81d17cc51b88bd6150f3726c114500e2a3e3ce072559c11aa779e9`
found:

- 1,448 scoped video file records;
- 1,445 usable file records representing 1,440 distinct YouTube IDs;
- 958 IDs already covered by the union of exact sealed Archive.org and approved
  catalogue locator evidence;
- 482 remaining IDs across 484 torrent files;
- 44,990,133,829 bytes (44.99 GB / 41.90 GiB) after choosing only the smallest
  rendition for each remaining ID; and
- three malformed-name files totaling 426,046,557 bytes, preserved for manual
  review and never automatically selected.

Thus the conservative pre-probe review ceiling is 45,416,180,386 bytes
(45.42 GB / 42.30 GiB), not a download instruction. The completed credential-free
probe, result SHA-256
`075bb129377c030393650ad3d7c1a55e56ea72e660a4be07a326dc55764fcbb9`, classified
all 482 targets as unavailable: 474 `private` and eight `video_unavailable`. A strict
replay produced finalized plan `tslp_9b55fffb408dab665cb517b94881acdb`, canonical
SHA-256 `9b55fffb408dab665cb517b94881acdb6f9946180684dfd9593ab3d53123ad3f`,
selecting 482 files and 44,990,133,829 logical bytes. The probe therefore did not
reduce the candidate logical selection. The three malformed-name files remain manual
review leads and are not selected. These figures are reproducible only for the pinned
torrent, catalogue evidence digest, Archive.org snapshot digest, and probe result
bound into that plan.

## Audited client path: aria2 1.37.0

There is intentionally no swarm-starting command in this repository. The reviewed
client path is Fedora's signed
[`aria2-1.37.0-9.fc44.x86_64`](https://packages.fedoraproject.org/pkgs/aria2/aria2/fedora-44.html)
package, built from the
[upstream aria2 1.37.0 release](https://github.com/aria2/aria2/releases/tag/release-1.37.0).
The exact package inspected on 2026-08-27 was:

| Artifact                    | Pin                                                                                         |
| --------------------------- | ------------------------------------------------------------------------------------------- |
| Fedora 44 RPM               | SHA-256 `72381bff2df034ea74453de5c5700a3080d9f7a25fadec16a97efe2da69344ac`; 1,446,010 bytes |
| RPM signer                  | Fedora 44 key fingerprint `36F612DCF27F7D1A48A835E4DBFCF71C6D9F90A6`                        |
| Extracted `/usr/bin/aria2c` | SHA-256 `02f07c5fc1764a71d118e79fbc95e0621aeab43e6fd4f1646b12c85494f55ffa`; 2,791,104 bytes |
| Installed-size field        | 4,673,321 bytes, excluding already-installed shared libraries                               |

The RPM came from Fedora's `fedora` release repository, not a third-party static
binary. A future operator should download that exact NEVRA into a new private audit
directory, compare the package SHA-256, and require both `rpm --checksig -v` digest
checks and the signer fingerprint before either extracting or installing it. The
package can be inspected without installation using `rpm2cpio` and `cpio`; a normal
local-file `dnf5 install` can be performed only after that verification. Do not
substitute a same-version binary with a different executable hash without recording
and reviewing a new client pin.

The reproducible Fedora acquisition and no-install extraction sequence is:

```sh
client_audit_root=$(mktemp -d /tmp/himr-aria2-rpm.XXXXXX)
client_extract_root=$(mktemp -d /tmp/himr-aria2-root.XXXXXX)
client_rpm="$client_audit_root/aria2-1.37.0-9.fc44.x86_64.rpm"

dnf5 download --from-repo=fedora --arch=x86_64 \
  --destdir="$client_audit_root" aria2-1.37.0-9.fc44.x86_64
printf '%s  %s\n' \
  72381bff2df034ea74453de5c5700a3080d9f7a25fadec16a97efe2da69344ac \
  "$client_rpm" | sha256sum --check --strict
rpm --checksig -v "$client_rpm"
gpg --show-keys --with-colons \
  /etc/pki/rpm-gpg/RPM-GPG-KEY-fedora-44-x86_64 \
  | rg '^fpr:'

(cd "$client_extract_root" && rpm2cpio "$client_rpm" | cpio -idm --quiet)
printf '%s  %s\n' \
  02f07c5fc1764a71d118e79fbc95e0621aeab43e6fd4f1646b12c85494f55ffa \
  "$client_extract_root/usr/bin/aria2c" | sha256sum --check --strict
"$client_extract_root/usr/bin/aria2c" --version
```

The fingerprint output must contain exactly the reviewed full fingerprint above.
Extraction is enough for the offline checks on this Fedora 44 host because all
reported shared-library dependencies are already present. If the operator later
chooses a system installation, install the already-verified local RPM with
`sudo dnf5 install "$client_rpm"`; do not ask the package manager to resolve an unpinned
`aria2` name at that stage.

### Index-base boundary

The corpus plan's `torrent_file_index` values are zero-based. aria2's documented
[`--select-file`](https://aria2.github.io/manual/en/html/aria2c.html#cmdoption-select-file)
values are **one-based**: the only permitted conversion is
`aria2_index = torrent_file_index + 1`. aria2 treats every file as selected when
`--select-file` is omitted, so an empty, missing, malformed, or unreviewed selector
must abort rather than fall back to a client invocation.

An offline `aria2c --no-conf=true --show-files=true` replay of the pinned manifest
reported info hash `4387cfaa6778205949bc39c91dc139e1eb7edebd`, exactly 4,719 file rows, and client
indices 1 through 4,719. Those results agree with the corpus parser's zero-based
indices 0 through 4,718. A one-file, network-isolated dry run accepted client index
1; client index 0 failed option validation. This is the required interpretation,
not an inference from display order.

Before any later network-capable run, the operator must regenerate the full plan
from the sealed inputs and complete availability probe, validate its canonical plan
digest, require a nonempty sorted-unique selection, bind the exact torrent SHA-256
and info hash above, transform every selected index by exactly `+1`, and compare the
result against `--show-files`. Passing a selector only at process creation means
aria2 never starts with all files wanted and then races an RPC update.

The offline selector helper enforces those conditions, re-parses the exact torrent,
checks every selected row's byte count and raw-path digest, computes the union of
required piece spans, and emits a path-free receipt. It refuses a pre-probe plan, an
empty selector, a digest mismatch, a malformed-review file, or selection without
explicit unavailability evidence:

```sh
PYTHONPATH=corpus/src python -m himr_corpus.torrent_aria2_selector \
  --plan research/corpus/torrent-selective/final-full-plan.json \
  --torrent research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent \
  --output "$PWD/research/corpus/torrent-selective/aria2-selector-receipt.json"
```

The receipt contains both index arrays and the exact `aria2_select_file_value`, but
does not contain a swarm-starting command. It also records zero client invocations,
zero network actions, and zero payload reads. Review and retain the receipt beside
the finalized private plan. The current receipt is
`a2sr_6e74c7bc3971c8d5818136d082c90bd8`, with canonical SHA-256
`6e74c7bc3971c8d5818136d082c90bd8eefcf9921d14bf98a1c7bb3d7ec040e6`.

### No-network validation

This host supports an unprivileged user and network namespace. The following shape
is the approved metadata/configuration check; it has no route or non-loopback
interface and does not start a swarm:

```sh
unshare --user --map-root-user --net -- \
  env -i \
  /path/to/pinned/aria2c \
  --no-conf=true --show-files=true \
  /path/to/pinned/hiding-in-my-room.torrent
```

`env -i` clears the ambient process environment without repurposing `HOME` or an
XDG directory. `--no-conf=true` independently prevents aria2 from loading a user
configuration file.

For the real manifest, `strace -f -e trace=network` around this command observed
only local `AF_NETLINK` interface enumeration and no `AF_INET`/`AF_INET6` socket or
connection attempt. To validate a final nonempty selector's syntax without creating
payload, use the same namespace and an empty temporary output directory with
`--dry-run=true`, `--file-allocation=none`, and the transformed
`--select-file=...` value. aria2 exits nonzero with its documented “Cancel
BitTorrent download in dry-run context” result; the audit must also assert that the
temporary output tree stayed empty. The expected nonzero exit is evidence of the
BitTorrent dry-run cancellation, not a successful transfer.

### Disk and piece-boundary implication

`--file-allocation=none` is required for any later reviewed acquisition so the
client does not preallocate the 722 GiB torrent. It does not eliminate piece-boundary
transfer. The pinned manifest uses 4 MiB pieces, and aria2 documents that a selected
file may require adjacent-file bytes from a shared piece. For `S` selected files, a
simple conservative boundary ceiling is less than `2 × 4 MiB × S`; the exact union
of selected files' piece spans should be calculated in the eventual client receipt.

At the finalized 482-file smallest-rendition selection, the loose boundary ceiling
is 4,043,309,056 bytes (4.04 GB / 3.77 GiB). The exact selector receipt improves on
that bound: its union of required pieces is 45,885,685,760 bytes (45.89 GB / 42.73
GiB), consisting of 44,990,133,829 selected payload bytes plus at most 895,551,931
boundary bytes (0.90 GB / 0.83 GiB). Filesystem metadata, control state, retries, and
an operator safety margin remain additional. The completed availability probe did
not lower the file count or logical byte requirement. Boundary files must be
retained for audit; do not enable aria2's destructive
`--bt-remove-unselected-file` option.

Record the client package and executable hashes, selector conversion receipt,
output-filesystem free space, actual network byte count, created adjacent files, and
final payload hashes before admitting any acquired media into the corpus. The private
2026-08-27 run uses the pinned extracted client and exact receipt selector. Its early
15:52 local control state recorded 54,525,952 verified required-piece bytes, zero
completed or in-flight nonrequired pieces, and an upload counter of zero. That proves only that at
least one peer supplied some required pieces during this observation; it does not
establish that every selected file is available or complete. The unfinished payload,
client state, and logs remain ignored private artifacts and have no catalogue or
publication authority. Nine unselected files totaling 7,488,530 bytes are wholly
contained in the selected files' required pieces, including four Windows shortcut or
batch files. They are boundary artifacts, not authorized content selections, and must
never be opened or executed. DHT and peer exchange also expose the downloader's
network address to public peers; the run caps upload at 128 KiB/s and disables
post-completion seeding, but BitTorrent may still upload pieces while downloading.

### Read-only acquisition audit

The offline runtime auditor binds the exact finalized plan, selector receipt, and
torrent before inspecting acquisition state. It parses aria2's pinned version-1
control-file structure itself; it never imports or invokes aria2, opens a socket,
executes a payload, or reads payload bytes. The saved session must contain the exact
sealed selector, torrent path, output directory, and reviewed safety options.

The payload inventory is fail-closed. A symlink, hard-linked file, special node,
cross-device subtree, unknown path, manifest-ineligible file, selector mismatch, or
malformed control bitfield prevents receipt creation. To reduce live-state races, the
auditor requires two identical session/control reads and two identical filesystem
metadata inventories. A changing acquisition may therefore require a retry; the
helper does not pause the client to obtain a snapshot.

```sh
acquisition_root="$PWD/research/corpus/torrent-acquisition/2026-08-27"

PYTHONPATH=corpus/src python -m himr_corpus.torrent_acquisition_audit \
  --plan "$PWD/research/corpus/torrent-selective/final-full-plan.json" \
  --selector-receipt \
    "$PWD/research/corpus/torrent-selective/aria2-selector-receipt.json" \
  --torrent \
    "$PWD/research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent" \
  --session "$acquisition_root/state/aria2.session" \
  --control "$acquisition_root/payload/hiding in my room.aria2" \
  --payload-root "$acquisition_root/payload" \
  --observed-at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --output "$acquisition_root/state/acquisition-audit.json"
```

The output must be a new absolute path outside the payload directory and is written
owner-read-only. The receipt contains no filesystem paths. It records hashes binding
all private inputs, selected and unavoidable boundary-file counts, logical and
allocated bytes, authorized versus unauthorized completed and in-flight piece counts
and bytes, and path hashes for unselected shortcut/executable/script-like artifacts.
An artifact is reported as fully materialized only when its expected logical size is
present and every torrent piece covering it is complete in the control bitfield.

Unauthorized pieces do not disappear from the evidence: they produce an
`unauthorized_piece_state_observed` receipt with `audit_passed: false`. Unknown
filesystem objects instead abort because the auditor cannot safely characterize the
tree. The receipt is acquisition evidence only; it grants no catalogue admission or
publication authority and does not replace final per-file hashing.

The first retained runtime checkpoint was written at **2026-08-27T20:15:49Z** as
private receipt `taudit_95f16b229242fc051955164ef63ab8c7`, canonical SHA-256
`95f16b229242fc051955164ef63ab8c707612d647c0beee4175df17df02850f7`.
The 6,144-byte mode-`0400` file has physical SHA-256
`ea2ce5e6b202f305579014e7fa967af16e38a8cfec1c4589d1fb7addfc7dfd01`.
It recorded 17 authorized complete pieces (71,303,168 bytes), one authorized
in-flight piece with 2,310,144 present bytes, zero unauthorized pieces, zero unknown
or ineligible files, and five risky unselected boundary placeholders, none fully
materialized. This is a point-in-time control-state observation, not a download-speed,
availability, completion, or content-integrity result.
