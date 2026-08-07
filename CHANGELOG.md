# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Experimental.** The storage format is not stable and there is no
> compatibility promise before `v1.0.0`.

## [Unreleased]

## [0.1.0] - 2026-08-07

First release: an object-store-native store for temporal data — video, audio,
logs, sensor readings — where the manifest is the source of truth and the
payload is immutable objects. It takes the append-only ordered log from Kafka
and the independently-decodable segment from HLS, and makes a **Group** both at
once: the unit of independent decoding *and* the unit of storage.

### Added

- **ledger:** `Create` and `Open` are the entry points, after `os.Create` and
  `os.Open`. `Create` establishes a new track by writing the root manifest that
  fixes its `TrackSchema` (timescale, time source, MIME, encoding) and returns an
  opaque `*Track`; `Open` references an existing one. `Writer` and `Reader` are
  built from that handle. Where `os.Create` truncates an existing file, `Create`
  refuses one (`ErrTrackExists`) — a track is an immutable, append-only log — and
  unlike `os.Open` the handle is both read- and write-capable, so a writer that
  crashed mid-append resumes through `Open(...).Writer()`. Settings that belong
  to a deployment rather than a track — a logger, a clock, the seal threshold —
  are passed in `Config`, whose zero value is usable.

- **ledger:** `TrackInfo` — what `Reader.Root` and `Writer.Root` return — embeds
  the `TrackSchema` the track was created with and adds the track path and the
  current epoch. Its fields promote, so `info.Timescale` reads directly, and the
  schema is a value: `Create(ctx, objects, other, src.Root().TrackSchema, cfg)`
  makes a second track with the first one's schema instead of restating it.

- **ledger:** A group is *anchored* on two timelines rather than described as a
  closed interval, because groups are serial within an epoch — the start of one
  is the end of the last. `GroupInfo.MediaTime` is required; `Duration` and
  `Wallclock` are optional. Media time is exact and skew-free but relative to one
  track's origin; wallclock is absolute and comparable across publishers, which
  is what makes cross-track correlation possible at all. `Duration` is stored
  rather than derived from the next group's anchor, because that derivation
  silently spans a dropped group and is undefined for the newest one — and it is
  exactly what HLS `EXTINF` and DASH `@d` consume. Conversions between the two
  timelines are range-checked, so a coarse timescale over a long recording
  reports failure rather than returning a wrapped value.

- **ledger:** `Writer` appends sealed groups, with size-triggered rotation of
  the open region into sealed manifests. Writing a delta manifest *is* the
  commit: a delta is an atomic whole-object write and immutable, so an object
  that exists is valid by construction. Sealed manifests are keyed by the delta
  range they cover, which makes a retried seal idempotent rather than colliding
  with a narrower one. `head` is a discovery cache, not a transaction boundary —
  it may lag or vanish, and recovery reads it for a hint before probing forward
  to the true tip. Consequently nothing at the manifest layer can be orphaned;
  only a group object can, which gives garbage collection exactly one job.

- **ledger:** `Writer.Append(ctx, duration, payload)` is the common case —
  groups committed back to back — deriving sequence, media time, and wallclock
  from the duration alone. `Writer.AppendGroup` remains for a producer's own
  numbering, a dropped group, or a media anchor that is not simply the previous
  group's end, and takes a fully populated `GroupInfo`.

- **ledger:** `AppendGroup` refuses a group that contradicts its predecessor —
  one starting before the previous ended (`ErrGroupOutOfOrder`). Gaps remain
  legal, since a dropped group is real information rather than corruption.

- **ledger:** Group identity is a single `GroupID`, packing the producer epoch
  and the producer's own sequence into one number, so numeric order is commit
  order across the whole track. Producers reset numbering on restart, and
  because group objects are immutable a reused sequence would collide rather
  than overwrite; each producer lifetime is its own append-only log under
  `<track>/e%06d/`, giving it a fresh keyspace. The producer's own numbering is
  preserved rather than renumbered, so clients can align replay against a relay
  serving the same track live. A writer opens at the track's latest epoch and
  stamps it onto every append; a producer restart begins the next one through
  `Writer.NewEpoch`. Epoch is never a number a caller passes.

- **ledger:** `Reader` answers windows, not just instants: `RangeMedia` and
  `RangeWallclock` iterate the groups overlapping a range, fetching only the
  sealed runs that can contribute. `SeekMedia` and `SeekWallclock` resolve to
  the group anchored at or before a target, so a seek keeps working when a
  producer supplies no duration and reports honestly when the target falls in a
  gap. Reads need object-store access and nothing else — no ledger process has
  to be running anywhere. Manifests are verified against the key they were
  fetched from, so a misfiled or swapped object is caught rather than trusted.

- **ledger:** The same `Reader` also streams a track's groups in commit order,
  spanning every epoch as one ascending run of `GroupID`s, in the shape of
  `bufio.Scanner` and `database/sql.Rows`: `SeekStart`, `SeekTip`, `SeekAfter`,
  `SeekMedia`, and `SeekWallclock` position its cursor, and `Next` returns each
  group and `io.EOF` at the current tip — stepping into a new epoch on its own
  when the current one is drained. Tailing is a poll loop the caller owns
  (object stores do not push), and `Position` returns the `GroupID` to resume
  from — `ParseGroupID` round-trips its text form, so a follower survives a
  restart and stays valid across a seal that reclaims the deltas it was reading.
  A Reader is single-consumer; concurrent consumers each open their own, which
  costs one root fetch.

- **ledger/store:** The storage contract — conditional create (`ErrExist`) as
  the primitive everything rests on, compare-and-swap for the single mutable
  `head` object, and an optional `Lister` used only by garbage collection, since
  the read path never lists. It sits under `ledger` but stays a leaf package, so
  a backend never imports the ledger to be usable by it: the same shape as
  `database/sql/driver`.

- **ledger/store:** `memstore` and `fsstore` backends, plus `storetest` — a
  conformance suite both run, so the contract is enforced rather than
  per-backend folklore. `fsstore` refuses any key that is not local to its root,
  which matters because keys reach it from manifest data rather than from
  caller-authored input.

- **stream:** HLS and DASH renderers over a ledger track — derived views, not a
  storage format. A Group is one segment; `Duration` is HLS `EXTINF` and DASH
  `@d`; a new producer epoch is an HLS `EXT-X-DISCONTINUITY` and a DASH timeline
  reset; a wallclock anchor is `EXT-X-PROGRAM-DATE-TIME` and the MPD
  `availabilityStartTime`. A `Handler` is an `http.Handler` over one `*Track` that
  serves an EVENT playlist (`.m3u8`) and a dynamic MPD (`.mpd`), both reflecting
  the whole track and growing as groups land. Segments are addressed by their
  `GroupID`, so a single segment handler serves both formats.

- **stream:** Delivery is pluggable at the HTTP layer. The default
  `ProxyResolver` streams segment bytes through `Reader.ReadGroup` — for local
  development. A production deployment supplies a `RedirectResolver` that mints a
  signed URL from `GroupInfo.ObjectKey`, so clients fetch objects directly and
  the store stays free of any presigning method.

- **ledger:** `Reader.Lookup` resolves a committed `GroupInfo` by its `GroupID`,
  reading its `ObjectKey` from the manifest rather than deriving it. It is the
  point-lookup a serving layer needs — to proxy a segment and to sign its URL —
  and it does not advance the streaming cursor.

- **cmd/qumo-stream:** serves a track over HTTP as HLS and DASH from a local
  filesystem store.

- **stream:** `Options.Window` caps a manifest at the most recent segments. A
  live track runs indefinitely, so an unwindowed manifest grows without bound and
  a player opens it at its oldest segment — a recording rather than a live
  stream. A windowed HLS playlist is a sliding live one rather than `EVENT`
  (which forbids removing segments), `EXT-X-MEDIA-SEQUENCE` counts what rolled
  off, and the MPD gains a `timeShiftBufferDepth`. Rolling out of a manifest is
  not deletion: the ledger keeps every group, so a client holding an older URL
  can still fetch it.

- **stream:** `Options.EpochWindow` caps a manifest by producer lifetimes —
  `0` lists every one, `1` only the current session. A producer that restarts
  opens a new epoch, and the segments before it are a session that has ended;
  left listed, they are where a player starts. Above `1` keeps recent lifetimes
  listed, so a viewer already playing the previous one reaches the restart across
  a discontinuity instead of finding its segments gone.

- **fmp4:** reads what a fragmented-MP4 writer must otherwise assume:
  `Timescale` from an init segment's `mdhd` (`TimescaleForTrack` when the init
  describes several tracks), and `FragmentDuration` from a fragment's
  `tfhd`/`trun`. The renderers require a `Duration` the ledger gave writers no
  way to compute, so ingesters assumed one from configuration — and when the
  assumption and the encoder's real GOP disagree, every `EXTINF` is wrong and
  players drift with nothing contradicting them. Nothing is guessed: an absent
  duration is `ErrNotFound`, and a `sample_count` is checked against what the box
  can hold before it is used.

- **cmd/qumo-ledger:** `inspect` and `follow`, built on the public API alone.

- **docs:** `docs/ARCHITECTURE.md` records the design decisions and why each was
  taken; the package documentation and worked examples cover the same ground for
  someone reading the API.

- **CI:** build, coverage, race, and Windows jobs; `gofmt`, `go mod tidy` and
  magefiles verification; golangci-lint via reviewdog; release-on-tag; CHANGELOG
  enforcement; Dependabot for both modules and for Actions.

### Fixed

- **stream:** a fragmented-MP4 track with no `Options.InitSegment` now fails at
  `NewHandler` with `ErrInitRequired` instead of rendering manifests that omit
  `EXT-X-MAP` (and DASH `@initialization`) and serving them as a valid `200` —
  turning a misconfiguration into a silent playback failure.

- **stream:** a windowed manifest states its timeline correctly. The DASH
  `availabilityStartTime` is the presentation start rather than the first listed
  group's anchor, so it no longer moves as the window rolls and shifts every
  client's timeline with it; and an HLS window that opens on an epoch change
  emits the `EXT-X-DISCONTINUITY` belonging to its first segment, with
  `EXT-X-DISCONTINUITY-SEQUENCE` counting the resets that have rolled off.

### Notes

- Deferred deliberately, with rationale in `docs/ARCHITECTURE.md`: garbage
  collection, retention, the MoQT adapter, and an S3 backend.
- Manifests are JSON. They are small, read far less often than payloads, and
  inspecting a broken track in a text editor is worth more than the bytes saved.
  A `version` field is what allows this to be revisited.
