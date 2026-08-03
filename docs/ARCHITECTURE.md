# Architecture

qumo-ledger stores and replays temporal data — video, audio, logs, sensor
readings — as immutable objects described by manifest objects.

The lineage is Kafka and HLS. From Kafka: an append-only ordered log, and a
manifest that gives records their order. From HLS: a segment that is
independently decodable and separately addressable. The synthesis is that a
**Group** is both — the unit of independent decoding *and* the unit of storage —
while the **manifest** is the log that gives Groups their meaning.

This document records the decisions and, more importantly, why they were made.

---

## Object layout

```
<track-path>/
  root.manifest                 track root: schema + latest epoch (near-immutable)
  e000001/                      one producer lifetime
    log.manifest                epoch's log root: sealed index, open region
    delta/
      head                      {delta, latest}  ← the only mutable object, per epoch
      open/00000042.manifest    immutable, one per commit
      sealed-00000001.manifest  immutable, rotated by size
    groups/
      g00000042                 immutable payload (sequence in the key)
  e000002/                      the next producer lifetime, its own log
```

---

## 1. Library first; the core depends on no transport

The core `ledger` package imports neither a transport nor a cloud SDK. Media
over QUIC ingest, HLS and DASH rendering, and S3 access are all adapters around
it.

**Why.** The store must serve video, audio, logs, and sensor data. Any of those
can be forced into a MoQT-shaped API, but only by lying about three of them.

**Consequence.** The core cannot compute media timestamps, because extracting
them means parsing a wire format. Callers supply a fully populated `GroupInfo`;
see decision 8.

## 2. The ledger is not on the live path

A relay serves live traffic. The ledger serves what has already happened. A
client that wants both stitches the seam itself.

**Why.** A Group can only be stored once sealed. For a two-second GOP, the
earliest a reader could see frame 0 through the ledger is `2s + PUT + commit` —
hundreds of milliseconds behind a relay fanning the same frame out of memory in
under a millisecond. The durable path and the live path are physically different
paths; pretending otherwise would make both worse.

**Consequence.** No frame-level fanout, no in-memory hot tier, no partial
groups. The ingest unit is a sealed group. This is what HLS does, and the
live↔VOD join is the player's problem there too.

## 3. The manifest is the source of truth; there is no embedded database

**Why.** An embedded database would be the one stateful, non-object-store
component in the system — the thing needing backup, recovery, and a home. With
manifests as objects, object storage is the only stateful system, and a track is
complete and self-describing wherever it is copied to.

**Consequence.** HLS, DASH, and the MoQT catalog are all *derived views* over a
protocol-agnostic manifest, never the storage format itself. The manifest
carries what the serving formats cannot agree on: encoding, MIME type, object
references, and both timelines.

## 4. Everything is immutable except one tiny head pointer

**Why.** Immutable objects give writer fencing for free — a superseded writer
cannot clobber a key that already exists, it simply fails. They are cacheable
forever. And they need only conditional *create*, which every backend
implements uniformly, rather than conditional overwrite, which they do not.

The PUT rate is identical either way: one object write per group. Immutability
costs object count and a reclaim pass, not writes.

**Consequence.** `head` is the sole exception, and the sole reason
`store.Store` has a `Swap` method.

## 5. The delta write is the commit; head is a discovery cache

A delta manifest is a whole-object atomic write and is immutable, so **an object
that exists is valid by construction**. A reader that probes past `head` is not
reading dirty state.

**Consequence.**

- `head` may lag arbitrarily, or be lost entirely, without affecting
  correctness. It exists so a joining reader can skip to the tip instead of
  probing from zero.
- Crash recovery is: read `head`, probe forward until a delta is absent, resume.
  No repair pass, no write-ahead log.
- Nothing at the manifest layer can be orphaned. Only a Group object can — by
  being written just before a crash that stopped its delta from landing.
  Garbage collection has exactly that one job.

## 6. Sealing is triggered by manifest size

Not by elapsed time, and not by group count.

**Why.** Group duration is a property of the track. A 100 Hz sensor feed and a
ten-second-GOP video track have nothing in common except how fast they fill a
manifest. Size is the only trigger that self-tunes across both.

**Consequence.** `DefaultSealThreshold` is chosen for *read* amplification, not
storage efficiency: each open delta is a separate object, so a reader wanting
recent history pays one request per delta, while the same groups inside a sealed
manifest cost one request total.

## 7. Two addressing regimes

**Delta manifests are ledger-numbered and contiguous.** A reader tails them
arithmetically — hold N, request N+1, treat absence as "not yet". No listing.
This matters: `LIST` is roughly an order of magnitude more expensive per request
than `GET` on S3 and has the weakest consistency guarantees of any operation
object stores offer. The read path never lists, so `Lister` is optional and used
only by garbage collection.

**Group sequences are producer-assigned and gappy.** Groups are legitimately
dropped under congestion, so a gap is real information rather than corruption.
Readers must take a Group's key from `GroupInfo.ObjectKey` and never derive it.

## 8. Group identity is a single `GroupID`

**Why.** Sequence alone is not an identity: a producer that restarts resets its
numbering, and because group objects are immutable a reused sequence would
*collide* rather than overwrite — leaving the track wedged. Epoch gives each
producer lifetime its own keyspace, so it must be part of identity.

But epoch is carried *inside one number*, not handled as a separate dimension. A
`GroupID` packs the epoch into the high bits and the sequence into the low, so
numeric order is commit order across the whole track: a reader streams one
ascending run of IDs and epoch boundaries are invisible to it. Callers handle a
single value; `GroupID.String` is the portable form for logs and persisted
positions, and `ParseGroupID` recovers it.

The producer's own sequence numbers are preserved rather than renumbered, and
that is the whole point: a relay serving the same track live uses those numbers,
and a client can only align live playback against replay if both agree on what
group 42 is. Renumbering would break decision 2's contract with clients.

**How an epoch advances.** Each producer lifetime is its own append-only log
under `<track>/e%06d/`, so a reused sequence under a new epoch lands in a fresh
keyspace rather than colliding. A writer opens at the track's latest epoch, and
a producer restart is an explicit verb — `Writer.NewEpoch` — which creates the
next log and switches the writer to it. Epoch is never a number a caller passes;
it is the writer's lifetime, stamped onto every appended group. The track root
is near-immutable as a result: it changes only when an epoch is created, never
on a seal.

## 9. A group is anchored on two timelines, not described as an interval

Groups are serial within an epoch — the start of one is the end of the last — so
a group carries **anchors** rather than a closed range:

| field | required | purpose |
|---|---|---|
| `MediaTime` | yes | media-time anchor; orders the group and resolves media seeks |
| `Duration` | no | media extent; becomes `EXTINF` and DASH `@d` |
| `Wallclock` | no | wallclock anchor; the cross-track correlation key |

**Why two clocks.** Media time is exact, arrives with the data, and is immune to
clock skew — but it is relative to one track's origin and cannot be compared
across publishers. Wallclock is absolute and comparable across every track and
source type, which is what makes *"show me the video and the sensor readings at
14:32"* answerable at all — but it depends on a clock and drifts. `Wallclock` is
optional because not every producer has a clock worth trusting; a group without
one still replays within its own track, it just cannot be correlated.

**Why `Duration` is stored rather than derived.** Deriving it from the next
group's anchor fails in the two places that matter. Across a **dropped group** it
would silently span the gap, so a player waits on media that was never stored.
And the **newest group has no successor**, so a live HLS playlist could not carry
its newest segment until the following one landed — a full group of added
latency, against the requirement that new groups become visible quickly. The only
other source is the payload, which would force a manifest-only renderer to fetch
objects.

**There is no stored end for either timeline.** Seeks resolve to *the last group
anchored at or before the target*, which is what a player wants anyway — land on
or before, then decode forward — and which keeps working when a producer supplies
no duration. `Duration` is consulted only to reject a target past a known end.

**Why contradictions are rejected at append.** Values that can disagree with each
other are the real cost of redundancy. Since groups are serial, `AppendGroup`
refuses a group starting before its predecessor ended. Gaps stay legal — they
are real data.

**Note on provenance.** moq-lite draft-05 added a `Timestamp Delta` to `FRAME`,
zigzag-encoded in the track's negotiated `Timescale`, with the first frame of a
group delta-encoded from zero — so a group's `MediaTime` is readable from the first
varint of its first frame, and `Duration` accumulates for free while writing.
Earlier drafts carry no timestamps at all, so those tracks declare
`TimeSource: ingest` and the ledger stamps `Wallclock` from its own clock.

Per-frame timestamps stay inside the payload. Indexing them in the manifest would
multiply metadata by the frame rate — a hundredfold for 100 Hz sensor data — and
can exceed the payload it describes. `ObjectCount` is carried instead, so a
reader can confirm it consumed a whole group and range-check an object index
without fetching.

## 10. Reads require object-store access and nothing else

A reader with object-store credentials can seek and replay with no ledger
process running anywhere. The format is the product.

**Consequence for authorization.** It is deliberately not the ledger's job. An
external service authorizes access and mints scoped credentials or signed URLs;
clients keep reading objects directly, and the core never sees a principal.
That service holds a backend of its own, so it declares whatever presigning
interface it needs where it consumes one — this package does not guess the shape
in advance.

**Consequence for notification.** Object stores do not push, so following a
track means polling — probe forward by deterministic key. A deployment wanting
lower latency layers a real notification channel on top, but nothing in the
format depends on one existing.

## 11. `AppendGroup` takes bytes the caller already owns

`AppendGroup(ctx, meta, payload []byte)` rather than a writer handle that
buffers internally.

**Why.** Groups are sealed before storage, so the bytes already exist somewhere
— a relay holds them in its own cache. A ledger-owned buffer would duplicate
them: a thousand tracks holding one 2 MB video group each is 2 GB of avoidable
resident memory. A known length also means a single PUT and a freely retryable
write.

**Consequence.** The common case — groups committed back to back — is handled in
core by `Append`, which derives sequence, media time, and wallclock from a
duration alone. `AppendGroup` remains for a producer's own numbering, a gap, or a
non-contiguous timeline, and it is what an adapter calls once it has parsed a
transport: a MoQT adapter accumulates frames, derives `MediaTime`/`Duration` from
their timestamp deltas, and passes the bytes through unchanged.

## 12. The entry points are Create and Open, after os.Create and os.Open

A track is reached through two free functions, mirroring `os.Create` and
`os.Open`: `Create` establishes a new track by writing the root manifest that
fixes its schema, and `Open` references an existing one. Both return an opaque
`*Track`, from which a `Writer` or `Reader` is derived. There is no container
type above a track.

**Why os-style, not a single handle constructor.** Creation and use are different
acts with different inputs. `Create` takes a `TrackSchema` — the schema, written
once into the root and then immutable — while `Open` takes none, because a reader
or a resuming writer needs only the schema the track already has. Folding them
into one constructor would mean either inventing a schema for reads or carrying
an "is this a create?" flag; the os split lets each path take exactly the
arguments it needs.

**Why the schema is its own type.** `TrackSchema` describes content and is fixed
at creation; `Config` carries deployment settings — a logger, a clock, the seal
threshold — that belong to a process. Two names rather than two "Config"s in one
call. `TrackInfo` embeds the schema rather than restating it, so the read side
has one definition of what a track *is*, and a schema can be handed straight back
to `Create` to make a second track like the first. What `TrackInfo` adds beyond
the schema is deliberately *not* creation input: `Track` comes from the path
argument and `Epoch` is writer-assigned (decision 8), so neither belongs on a
struct a caller fills in.

**Why a reference, not a container.** A track is a persistent, named thing whose
schema is fixed at creation — a database table, not a file you own. You do not
`OPEN` a table to query it; you hold a reference and act on it. Binding the path
into the handle, rather than passing it to every call, also removes a class of
mistake: you cannot wire a reader and a writer to different tracks by typo.

**Why Open is read- and write-capable.** Object stores have no open, but a writer
that crashed mid-append still needs to reach the store to recover — probing
forward to the true tip, which is bounded by the seal threshold and cheap. That
recovery folds into `Writer` on an `Open`ed track, so one entry point serves both
a fresh reader and a resuming writer. `Create` diverges from `os.Create` in one
way: where os.Create truncates an existing file, `Create` refuses one
(`ErrTrackExists`), because a track is an immutable, append-only log.

**Why the handle is opaque.** Its fields are unexported and set through `Create`
or `Open` and `Config`, so the internal layout can change without breaking
callers. Deployment-level settings — a logger, a clock, the seal threshold —
belong to a process, not a track, so they ride on the handle in one place instead
of on every call.

**Consequence.** The ontology is one layer: a `Store` holds tracks. There is no
separate "bucket" type to explain alongside it.

## 13. Manifests are an internal wire format, not a public type

The manifest structs (`trackRoot`, `epochLogRoot`, `deltaManifest`,
`sealedManifest`, `head`) are unexported. A Go consumer reads track metadata
through `TrackInfo`, the projection returned by `Reader.Root` and `Writer.Root`;
a reader in any language reads the JSON schema documented in the object layout
above. There is no `Reader.Head` on the public API — the head pointer is a
discovery cache (decision 5), and `Reader.SeekTip` already serves a follower
resuming near the end.

**Why.** The on-disk format is the product, so it must be stable and is versioned
through a `version` field; the Go API is free to evolve separately. Exporting the
structs would lock the two together, and would expose storage structure — sealed
runs, the open region, delta numbering — that a consumer has no reason to see.
This is decision 12's opacity one layer down: the handle hides its fields, the
package hides its format.

## 14. HLS and DASH are derived views, served from the manifest

The `stream` package renders a track as HLS and DASH without the core knowing
either format — decision 3 made them derived views, and this is that view. A
Group is one segment; `Duration` is HLS `EXTINF` and DASH `@d`; a new producer
epoch is an HLS `#EXT-X-DISCONTINUITY` and a DASH timeline reset; a wallclock
anchor is `#EXT-X-PROGRAM-DATE-TIME` and the MPD `availabilityStartTime`. Both
address segments by their `GroupID`, so one segment handler serves them both.

**Why a GroupID point-lookup joined the core.** Serving a segment by its id needs
its `ObjectKey` — to proxy the bytes and to mint a signed URL — and the ledger
forbids deriving a group key (decision 7). So `Reader.Lookup` reads the key from
the manifest. It is the one read operation that is neither a window nor a stream,
and it belongs on the core because every renderer and every external serving tool
needs the same honest resolution.

**Why delivery stays out of the core.** Proxying is the dev default; redirecting
to signed object URLs is the production path (decision 10). Both are a
`stream`-layer concern — a `SegmentResolver` — so the store stays free of a
presigning method it could not implement uniformly, and the core never sees a
delivery strategy.

**Consequence.** The renderers reflect the whole track and regenerate per
request, which is O(track size). Caching the output and bounding it with a
sliding window are deliberate follow-ons; the format itself never depends on
them.

---

## Deferred

Named so they are choices rather than oversights.

| | |
|---|---|
| **Garbage collection** | Orphaned group objects are identifiable via `Lister`; no policy engine yet. |
| **Retention** | No time- or size-based expiry. |
| **Frame index footers** | Sealed-only writes make a per-group frame index *possible*, unlike live systems. Skipped so payloads stay byte-identical to the MoQT group stream, which keeps egress a zero-transform read. |
| **MSF renderer** | The manifest carries what a Media Source Framework presentation needs, as it does for HLS and DASH (decision 14). |
| **MoQT adapter** | Belongs in a `moqtstore` package; the only place that parses frames. |
| **S3 backend** | The interface is deliberately shaped around what S3 offers: conditional create and conditional overwrite. |
| **Manifest encoding** | JSON, because manifests are small, read far less often than payloads, and being able to inspect a broken track in a text editor is worth more than the bytes. A `version` field is what lets this be revisited. |
