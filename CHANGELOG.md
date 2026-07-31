# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Experimental.** The storage format is not stable and there is no
> compatibility promise before `v1.0.0`.

## [Unreleased]

### Added

- **ledger:** Initial core. `TrackPath`, `TrackConfig`, `GroupRef{Epoch, Sequence}`, and `GroupMeta`. A group is *anchored* on two timelines rather than described as a closed interval, because groups are serial within an epoch — the start of one is the end of the last. `T0` (media anchor) is required; `Duration` and `W0` (wallclock anchor) are optional. Media time is exact and skew-free but relative to one track's origin; wallclock is absolute and comparable across publishers, which is what makes cross-track correlation possible at all. `Duration` is stored rather than derived from the next group's anchor because that derivation spans a dropped group silently and is undefined for the newest group — and it is what HLS `EXTINF` and DASH `@d` consume. Seeks resolve to the last group anchored at or before the target, so they keep working when a producer supplies no duration.
- **ledger:** `AppendGroup` refuses a group that contradicts its predecessor — one starting before the previous ended, or carrying an epoch behind the track's (`ErrGroupOutOfOrder`). Gaps remain legal, since a dropped group is real information.
- **ledger:** `Writer` with size-triggered sealing and crash recovery. Writing a delta manifest *is* the commit — a delta is an atomic whole-object write and immutable, so an object that exists is valid by construction. `head` is a discovery cache, not a transaction boundary: it may lag or vanish, and recovery reads it for a hint before probing forward to the true tip. Consequently nothing at the manifest layer can be orphaned; only a group object can, which gives garbage collection exactly one job.
- **ledger:** `Reader` with `Groups`, `SeekMedia`, `SeekWallclock`, and a polling `Follow`. Reads require object-store access and nothing else — no ledger process has to be running anywhere.
- **ledger:** Group identity is `(Epoch, Sequence)`. Producers reset numbering on restart, and because group objects are immutable a reused sequence would collide rather than overwrite. The producer's own numbering is preserved rather than renumbered so clients can align replay against a relay serving the same track live.
- **objectstore:** `Store` interface built around conditional create (`ErrExist`) with compare-and-swap needed only for the single mutable `head` object, plus optional `Lister` (garbage collection only — the read path never lists) and `Presigner` (so an external authorization service can delegate rather than proxy).
- **objectstore:** `memstore` and `fsstore` backends, and `storetest` — a shared conformance suite both backends run, so guarantees are enforced rather than per-backend folklore.
- **cmd/qumo-ledger:** `inspect` and `follow`, using only the public API.
- **docs:** `docs/ARCHITECTURE.md` recording the design decisions and their rationale.
- **CI:** Go build/coverage, race, Windows, and tidy/gofmt/magefiles jobs; golangci-lint via reviewdog; release-on-tag; CHANGELOG enforcement; Dependabot for both modules and for Actions.

### Notes

- Deferred deliberately, with rationale in `docs/ARCHITECTURE.md`: garbage collection, retention, the MoQT adapter, HLS/DASH renderers, and an S3 backend.
- Manifests are JSON. They are small, read far less often than payloads, and inspecting a broken track in a text editor is worth more than the bytes saved. `ManifestVersion` is what allows this to be revisited.
