# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Experimental.** The storage format is not stable and there is no
> compatibility promise before `v1.0.0`.

## [Unreleased]

### Added

- **store:** `store.Open` opens a backend from a URI, so a deployment chooses
  its storage in configuration. A backend registers its scheme with
  `store.Register` when its package is imported; `store.Schemes` lists them, and
  an unregistered scheme is `ErrUnknownScheme`. An empty URI opens the memory
  store.
  - `mem:` — `mem`.
  - `file:///var/lib/ledger`, `file:ledger` — `fs` over that directory.
  - `postgres://…`, `postgresql://…` — `db`.
  - `s3://bucket/prefix?region=…&endpoint=…` — `bucket`.

- **ledger/store/db:** A backend over one table of a PostgreSQL or CockroachDB
  database, created when absent (`ledger_objects`, or the URI's `table`
  parameter). An object is one row with an integer version; `Create` is an
  insert that does nothing on conflict and `Swap` an update conditioned on the
  version, so the database enforces both across processes. Objects are read and
  written whole.

- **ledger/store/bucket:** A backend over a bucket of Amazon S3 or an S3-compatible
  service, under a key prefix. `Create` and `Swap` are conditional PUTs
  (`If-None-Match: *`, `If-Match`) and the version is the ETag. Requests are
  signed with Signature Version 4 from static credentials (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`); with an `endpoint` the bucket
  is addressed path-style.

- **ingest:** A new package that accepts records over HTTP, appends them to
  ledger tracks and reads them back, the inbound counterpart of `stream`. A
  `Handler` is an `http.Handler` over a store, mounted with `http.StripPrefix`.
  - A track is named by its broadcast path and track name, a `Track`, and
    addressed by its ledger key: `/tracks/room/123/chat` is the track `chat` of
    the broadcast `/room/123`.
  - `POST /tracks/{track}` appends the body, one JSON value in UTF-8, as one
    group and answers `201` once it is committed, creating the track when it
    does not exist. Requests are routed by the URL, so a track is resolved and
    authorized before the body is read.
  - Each group stores a `Record`, `{"sender": …, "payload": …}`: the payload
    as sent, and the sender `Options.Authorize` named for the request, so a
    record's sender comes from its credential rather than from its content. A
    record with no sender was written by a party trusted with the whole track.
  - `PUT /tracks/{track}` creates the track ahead of its first record (`201`,
    or `204` when it exists).
  - `GET /tracks/{track}` answers a page of records, oldest first: the newest,
    or those before `?before=<group>`, at most `?limit=` (default 50, at most
    200), with the cursor for the next older page.
  - A record with an `Idempotency-Key` header is stored once per sender and
    track; a retry with the same key gets the first reply. A track remembers
    its 1024 most recent keys.
  - `Options.SenderLimit` and `Options.TrackLimit` bound records per sender and
    per track as token buckets; a record past either is answered `429` with a
    `Retry-After`.
  - Any number of senders record into one track. Records of one track are
    serialized within a handler, and two processes recording into the same
    track are not coordinated.
  - `Options.Authorize(r, track, access)` is asked on every request, for a
    `Write` or a `Read`, and returns a write's sender; it refuses with `403`, or
    `401` for `ErrUnauthenticated`, with `Options.Challenge` as its
    `WWW-Authenticate` header. `Options.OnOpen(ctx, track)` observes a track
    the handler starts writing to, and
    `Options.OnRecord(ctx, track, group, record)` what was committed, which is
    where a caller forwards a record to live subscribers.
  - Tracks are created with `TimeSourceIngest`, timescale 1000 and encoding
    `json`. A record has no media time, so each group is anchored by the wall
    clock at commit; read a window back with `Reader.RangeWallclock`. The
    `stream` renderers need a duration per group and do not serve these tracks.

- **ledger:** `Reader.Before(ctx, id, n)` returns up to n groups committed
  before id, oldest first, or the newest n for the zero id: the way to page
  backwards through a track. It reads backwards from each epoch's newest delta
  and skips sealed runs that start at or after id.

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

- **ledger:** `GroupID.Compare` reports the order of two group identities as
  -1, 0, or +1, following the convention of `time.Time.Compare` and
  `netip.Addr.Compare`. It is the shape `slices.SortFunc` and
  `slices.BinarySearchFunc` want, so a consumer holding rows from more than one
  source can order them by identity.

### Changed

- **ledger/store:** The backend packages are named for the storage they keep
  objects in, beneath the `store` package whose interface they implement:
  `memstore` is now `mem` and `fsstore` is now `fs`, joined by `db` and
  `bucket`. Imports change from `ledger/store/memstore` and
  `ledger/store/fsstore` to `ledger/store/mem` and `ledger/store/fs`.

- **stream:** `Handler` answers internal failures with a generic 500 instead of
  echoing the error to the client, and records the detail — the error, the
  method, the request path — through the new `Options.Logger` when one is
  supplied (nil logs nothing; a canceled request logs nothing either way, being
  player churn rather than a failure). Store errors carry object keys and store
  paths, which have no business in a response body. A segment whose `Lookup`
  fails for any reason other than an absent group — or an epoch log that was
  never written, which the ledger treats as a creation that did not finish — is
  now a 500 rather than a 404: a store outage must not read as an absent
  segment, which a player would prune.

- **ledger:** `GroupInfo.ObjectCount` is documented as advisory, matching
  `Size`. It is the producer's figure, stored as-is and never verified against
  the payload — useful exactly where the bytes are not at hand, not a promise
  the ledger checks.

- **ledger:** `Reader.ReadGroup` takes the object key rather than the whole
  `GroupInfo`. It only ever read `ObjectKey`, and the rule that a key must come
  from a manifest row — never derived, because producer sequences are gappy —
  is enforced by the store's resolve boundary rather than by the parameter
  type. Callers pass `group.ObjectKey`.

- The `cmd/qumo-ledger` and `cmd/qumo-stream` binaries were demoted to runnable
  examples under `examples/` (`inspect-follow`, `stream-server`): reference code,
  not supported entrypoints. The repository no longer builds or ships binaries
  (`mage build` now compiles every package), and a client CLI for accessing a
  ledger will live in a separate repository.

### Removed

- **ledger:** `GroupID.Before`, superseded by `GroupID.Compare`. It had no
  callers, and unlike `time.Time` a `GroupID` is an ordered numeric type, so
  `a < b` was always available to a caller who wanted a predicate — the method
  was a second spelling of `<` rather than access to something otherwise
  unreachable. Keeping it would have invited a matching `After` and a
  permanently asymmetric API.

### Fixed

- **ledger:** A failed write no longer stops a writer's later appends.
  - A write whose outcome the store left unknown — a group object, a commit,
    or a seal that failed — makes the next append reload the writer's state
    from the store first, so a commit the store took though its answer was
    lost is counted rather than claimed again.
  - `Writer.Append` steps past a sequence an earlier failed append left an
    uncommitted group object at, up to 16 of them, instead of failing with
    `ErrGroupExists` on every later append and after every restart. The
    object is reclaimed with the other unreferenced ones. `AppendGroup`, whose
    caller chose the sequence, still reports the collision.

## [0.1.0] - unreleased
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
