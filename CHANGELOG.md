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
- **ledger/store:** `Store` interface built around conditional create (`ErrExist`) with compare-and-swap needed only for the single mutable `head` object, plus optional `Lister` (garbage collection only — the read path never lists) and `Presigner` (so an external authorization service can delegate rather than proxy). It sits under `ledger` but stays a leaf package, so a backend never has to import the ledger to be usable by it — the same shape as `database/sql/driver`.
- **ledger/store:** `memstore` and `fsstore` backends, and `storetest` — a shared conformance suite both backends run, so guarantees are enforced rather than per-backend folklore.
- **cmd/qumo-ledger:** `inspect` and `follow`, using only the public API.
- **docs:** `docs/ARCHITECTURE.md` recording the design decisions and their rationale.
- **CI:** Go build/coverage, race, Windows, and tidy/gofmt/magefiles jobs; golangci-lint via reviewdog; release-on-tag; CHANGELOG enforcement; Dependabot for both modules and for Actions.

### Fixed

- **ledger:** A sealed manifest's key now names the delta range it covers (`sealed-<first>-<last>.manifest`) rather than its position. A seal whose root update failed left the manifest object behind; retrying after more groups had arrived recomputed a wider manifest but reused the same positional key, so `Create` returned `ErrExist`, the retry was silently discarded, and the root was then published with a summary describing groups the stored object did not hold — after which the superseding deltas were reclaimed, making those groups unreachable. Naming the range makes `ErrExist` mean exactly "this identical seal already landed".
- **fsstore:** Reject keys that are not local to the root. `resolve` previously rejected `../` but treated a backslash as an ordinary character, so `..\outside` escaped the root on Windows — reachable from `GroupMeta.Object`, which is manifest data rather than caller-authored input. Keys are now checked with `filepath.IsLocal`, which also rejects Windows reserved device names such as `NUL` (where `Get` had reported a phantom empty object), and backslashes and non-canonical keys are refused on every platform so one key names exactly one object.
- **ledger:** A seek now walks newest-first and fetches at most one sealed manifest, instead of one per sealed run for the length of the recording. Walking backwards also resolves what an epoch reset makes ambiguous: when a media timestamp exists in several epochs, the most recent wins.
- **ledger:** Media-to-wallclock conversion no longer overflows. `units * 1e9 / timescale` wraps at only ~9.2e9 units on a `Timescale: 1` track, well inside a long sensor recording; the conversion now divides first and range-checks, reporting failure rather than returning a wrapped value. A group whose media range would overflow is refused at append, since `MediaEnd` cannot report failure and a wrapped end reads as preceding its own start.
- **ledger:** Root and sealed manifests are verified against the key they were fetched from — track for both, delta range for sealed (`ErrManifestMismatch`). Manifests are self-describing precisely so a misfiled or swapped object is caught rather than trusted.
- **CI:** `lint.yml`'s job renamed `build` → `lint`, so two workflows no longer report the same check context and leave a required-status rule ambiguous. The required-stub gained a matching `lint` job, filtered separately because `lint.yml`'s paths differ from `go.yml`'s. The gofmt check now covers the whole repository instead of a hardcoded package list.

### Changed

- **ledger:** Added the range and cursor API, and pruned the surface it replaced. A temporal store could previously answer "which group covers this instant" but not "which groups cover this window" — a range query meant iterating the whole recording and filtering client-side. `Reader.RangeMedia`, `Reader.RangeWallclock` and `Reader.GroupsFrom` answer it directly, skipping the sealed runs that cannot contribute.
- **ledger:** `Reader.Follow` yields `Update` (a group plus the cursor that resumes after it) instead of `DeltaManifest`, and takes an opaque `Cursor` instead of a raw delta number. The commit-numbering scheme is no longer the public contract, so how commits are chunked can change without breaking callers. `Reader.Tip` gives a follower "everything from now", replacing a `Head()` call plus an off-by-one the caller had to get right. `Cursor` marshals as text, so a follower can persist its position and resume through a restart.
- **ledger:** `Reader.Delta` and `Reader.Sealed` are unexported. They were storage plumbing that no consumer needed.
- **ledger:** A follower resuming from a cursor whose deltas have since been sealed no longer waits forever for a reclaimed object. Sealing deletes the deltas it folds up, so any persisted cursor became a permanent hang once a seal passed it — found by running the CLI against a real track. Those groups are served from the sealed run instead. The cursor issued during that replay points after the whole run, because a sealed manifest does not record which delta each group came from, so a consumer stopping mid-replay sees the run again — within the at-least-once contract `Follow` already carries.
- **ledger:** Removed `TrackPath.Prefix`, which had no caller and implied a track-discovery API that does not exist. Unexported `TrackPath.Validate`, `TimeSource.Valid`, `GroupRef.Before`, and `GroupMeta`'s `HasDuration`/`HasWallclock`/`MediaEnd` — all one-line derivations of exported fields, and `MediaEnd` in particular invited treating a group with no duration as ending at its own start.

### Notes

- Deferred deliberately, with rationale in `docs/ARCHITECTURE.md`: garbage collection, retention, the MoQT adapter, HLS/DASH renderers, and an S3 backend.
- Manifests are JSON. They are small, read far less often than payloads, and inspecting a broken track in a text editor is worth more than the bytes saved. `ManifestVersion` is what allows this to be revisited.
