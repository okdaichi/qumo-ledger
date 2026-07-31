# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Experimental.** The storage format is not stable and there is no
> compatibility promise before `v1.0.0`.

## [Unreleased]

_Nothing yet._

## [0.1.0] - unreleased

First release: an object-store-native store for temporal data — video, audio,
logs, sensor readings — where the manifest is the source of truth and the
payload is immutable objects. It takes the append-only ordered log from Kafka
and the independently-decodable segment from HLS, and makes a **Group** both at
once: the unit of independent decoding *and* the unit of storage.

### Added

- **ledger:** `Bucket` is the entry point. `&ledger.Bucket{Store: objects}`
  binds an object store once, and every `Writer` and `Reader` is opened from it,
  so settings that belong to a deployment rather than a track — a logger, a
  clock, the seal threshold — are configured in one place. Configuration is
  exported struct fields with documented zero-value defaults, following
  `http.Server` and `tls.Config`. `Store` is the one field with no default.

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

- **ledger:** `AppendGroup` refuses a group that contradicts its predecessor —
  one starting before the previous ended, or carrying an epoch behind the
  track's (`ErrGroupOutOfOrder`). Gaps remain legal, since a dropped group is
  real information rather than corruption.

- **ledger:** Group identity is `(Epoch, Sequence)`. Producers reset numbering
  on restart, and because group objects are immutable a reused sequence would
  collide rather than overwrite. The producer's own numbering is preserved
  rather than renumbered, so clients can align replay against a relay serving
  the same track live.

- **ledger:** `Reader` answers windows, not just instants: `RangeMedia`,
  `RangeWallclock` and `GroupsFrom` iterate the groups overlapping a range,
  fetching only the sealed runs that can contribute. `SeekMedia` and
  `SeekWallclock` resolve to the group anchored at or before a target, so a seek
  keeps working when a producer supplies no duration and reports honestly when
  the target falls in a gap. Reads need object-store access and nothing else —
  no ledger process has to be running anywhere. Manifests are verified against
  the key they were fetched from, so a misfiled or swapped object is caught
  rather than trusted.

- **ledger:** `Follow` tails a track by polling, since object stores do not
  push, and yields each group with an opaque `Cursor` that resumes immediately
  after it. The cursor marshals as text, so a follower survives a restart, and
  it stays valid across a seal that reclaims the deltas it named.

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

- **cmd/qumo-ledger:** `inspect` and `follow`, built on the public API alone.

- **docs:** `docs/ARCHITECTURE.md` records the design decisions and why each was
  taken; the package documentation and worked examples cover the same ground for
  someone reading the API.

- **CI:** build, coverage, race, and Windows jobs; `gofmt`, `go mod tidy` and
  magefiles verification; golangci-lint via reviewdog; release-on-tag; CHANGELOG
  enforcement; Dependabot for both modules and for Actions.

### Notes

- Deferred deliberately, with rationale in `docs/ARCHITECTURE.md`: garbage
  collection, retention, the MoQT adapter, HLS/DASH renderers, and an S3
  backend.
- Manifests are JSON. They are small, read far less often than payloads, and
  inspecting a broken track in a text editor is worth more than the bytes saved.
  `ManifestVersion` is what allows this to be revisited.
