# Completed selected torrent-file handoff

`torrent-completed-handoff` creates the acquisition-side work order for exactly one
media file that aria2 has finished inside a reviewed selective torrent run. It does
not copy the file, admit it to the catalogue, update a database, stop or inspect the
torrent process, contact the network, or authorize publication.

The handoff fails closed unless all of these statements can be replayed from local
evidence:

- the finalized selective-acquisition plan has a valid canonical digest;
- the stored selector receipt is value-identical to a fresh replay from the exact
  plan and `.torrent` bytes;
- the saved aria2 session contains that exact selector, output directory, torrent
  path, and required safety options;
- the requested zero-based file index is in the selector and its explicit path,
  raw-path digest, and declared size match both the plan and torrent manifest;
- the target has an allowed audio/video suffix and is not an unselected file that
  merely shares a boundary piece;
- every torrent piece covering the target is complete in aria2's version-1 control
  state, with no completed or in-flight piece outside the selector's authorized
  piece ranges;
- the full metadata-only acquisition audit passes immediately before and after the
  payload read, and the exact control bytes remain unchanged;
- the target is a same-filesystem, single-link regular file reached without
  following a symlink, has exactly the declared size, and remains metadata-stable;
- descriptor-pinned `ffprobe` finds at least one audio or video stream.

The metadata auditor inventories all manifest paths but opens no payload content.
After the requested index is proven selected, the handoff opens only that media file.
It hashes the pinned descriptor and gives the same read-only descriptor to a pinned
`ffprobe` process through `/proc/self/fd`. It never opens, hashes, probes, or executes
an adjacent boundary artifact. In particular, `download.bat` is never a valid target
for the current selector and must never be opened or executed.

## Output contract

The output conforms to
[`torrent-completed-handoff.schema.json`](schemas/torrent-completed-handoff.schema.json).
Its `work_order_sha256` is the SHA-256 of canonical JSON before the two identity
fields are added. Important fields include:

- hashes and identities for the plan, selector receipt, torrent, saved session,
  stable control state, and pinned `ffprobe` executable;
- the zero-based torrent index, one-based aria2 index, display path, base64 raw path
  components, declared size, and exact covering-piece range;
- the selected file's SHA-256 and normalized ffprobe metadata;
- the existing catalogue source tuple
  `bittorrent / torrent_file_candidate / <info-hash>/<manifest-path>`;
- explicit statements that no catalogue import, cache copy, media admission, network
  action, or publication decision occurred.

An optional `--output` writes canonical JSON plus one trailing newline to a new
absolute path outside the payload tree. The file is created atomically, is mode
`0400`, and is never overwritten. Without `--output`, the work order is printed as
canonical JSON. Refusals use exit status `2` and a small machine-readable JSON error
on stderr.

The later catalogue-side importer must independently resolve the pre-existing source
tuple, copy the media into the canonical content-addressed cache, verify the copied
hash and probe data, and admit a rendition. This work order alone is not an admission
receipt, and the live catalogue must not be pointed at the torrent working tree.

## Current acquisition bundle

The first currently complete selected candidate is zero-based torrent index `4369`
(aria2 index `4370`):

```text
YouTube Videos/Chef Daniel/If Nintendo made Palworld [AoDs3RC2g7I].webm
declared bytes: 3537104
```

After an operator confirms that the acquisition state is quiescent enough for two
identical reads, the command shape is:

```sh
acquisition_root="$PWD/research/corpus/torrent-acquisition/2026-08-27"

acquisition/bin/torrent-completed-handoff \
  --plan "$PWD/research/corpus/torrent-selective/final-full-plan.json" \
  --selector-receipt \
    "$PWD/research/corpus/torrent-selective/aria2-selector-receipt.json" \
  --torrent \
    "$PWD/research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent" \
  --session "$acquisition_root/state/aria2.session" \
  --control "$acquisition_root/payload/hiding in my room.aria2" \
  --payload-root "$acquisition_root/payload" \
  --observed-at "YYYY-MM-DDTHH:MM:SSZ" \
  --file-index 4369 \
  --manifest-path \
    "YouTube Videos/Chef Daniel/If Nintendo made Palworld [AoDs3RC2g7I].webm" \
  --declared-byte-count 3537104 \
  --output "/absolute/private/path/new-handoff.json"
```

`--ffprobe-executable` can select an explicit executable, and
`--ffprobe-sha256` can require its pre-reviewed lowercase SHA-256. Whether or not an
expected hash is supplied, the implementation pins and hashes the executable before
use, executes through that descriptor, verifies it again afterward, and records its
identity in the work order.

Do not run this command merely because a file has reached its declared filesystem
size. Completion is established from all covering control bits, exact selector
membership, and a stable before/after audit together.

## Tests

The focused tests construct their own small torrent, selector, session, control file,
boundary script, and generated video under `acquisition/.test-work`. They never read
or modify the live acquisition bundle:

```sh
python3 -m unittest acquisition.tests.test_torrent_completed_handoff -v
```

Coverage includes a successful schema-valid handoff, canonical immutable output,
boundary-target rejection before hashing/probing, incomplete-piece rejection, exact
path/size and session-selector mismatches, control mutation across the read boundary,
hardlink/symlink refusal, and machine-readable CLI errors.
