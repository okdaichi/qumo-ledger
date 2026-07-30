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
  root.manifest                 track metadata; changes only on seal or epoch change
  delta/
    head                        {delta, latest}  ← the only mutable object
    open/00000042.manifest      immutable, one per commit
    open/00000043.manifest
    sealed-000001.manifest      immutable, rotated by size
  groups/
    e000001-g00000042           immutable payload
```

---

## 1. Library first; the core depends on no transport

The core `ledger` package imports neither a transport nor a cloud SDK. Media
over QUIC ingest, HLS and DASH rendering, and S3 access are all adapters around
it.

**Why.** The store must serve video, audio, logs, and sensor data. Any of those
can be forced into a MoQT-shaped API, but only by lying about three of them.

**Consequence.** The core cannot compute media timestamps, because extracting
them means parsing a wire format. Callers supply a fully populated `GroupMeta`;
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
`objectstore.Store` has a `Swap` method.

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
Readers must take a Group's key from `GroupMeta.Object` and never derive it.

## 8. Group identity is `(Epoch, Sequence)`

**Why.** Sequence alone is not an identity: a producer that restarts resets its
numbering, and because group objects are immutable a reused sequence would
*collide* rather than overwrite — leaving the track wedged. `Epoch` gives each
producer lifetime its own keyspace.

The producer's own sequence numbers are preserved rather than renumbered, and
that is the whole point: a relay serving the same track live uses those numbers,
and a client can only align live playback against replay if both agree on what
group 42 is. Renumbering would break decision 2's contract with clients.

## 9. Two independent time indexes per group

Media time `[T0, T1)` in the track's timescale, **and** wallclock `[W0, W1)` in
Unix nanoseconds.

**Why neither alone is enough.** Media time is exact, arrives with the data, and
is immune to clock skew — but it is relative to one track's origin and cannot be
compared across publishers. Wallclock is absolute and comparable across every
track and source type, which is what makes *"show me the video and the sensor
readings at 14:32"* answerable at all — but it depends on a clock and drifts.

Storing both costs about sixteen bytes per group row.

**Note on provenance.** moq-lite draft-05 added a `Timestamp Delta` to `FRAME`,
zigzag-encoded in the track's negotiated `Timescale`, with the first frame of a
group delta-encoded from zero — so a group's `T0` is readable from the first
varint of its first frame, and `T1` accumulates for free while writing. Earlier
drafts carry no timestamps at all, so those producers get ingest-clock values
instead. `TimeSource` records which, because a reader needs to know how much to
trust them.

Per-frame timestamps stay inside the payload. Indexing them in the manifest
would multiply metadata by the frame rate — a hundredfold for 100 Hz sensor
data — and can exceed the payload it describes.

## 10. Reads require object-store access and nothing else

A reader with bucket credentials can seek and replay with no ledger process
running anywhere. The format is the product.

**Consequence for authorization.** It is deliberately not the ledger's job. An
external service authorizes access and mints scoped credentials or signed URLs;
clients keep reading objects directly, and the core never sees a principal.
`objectstore.Presigner` exists so that service can delegate rather than proxy.

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

**Consequence.** Adapters layer the ergonomic API. A MoQT adapter accumulates
frames, derives `T0`/`T1` from their timestamp deltas, and calls through.

---

## Deferred

Named so they are choices rather than oversights.

| | |
|---|---|
| **Garbage collection** | Orphaned group objects are identifiable via `Lister`; no policy engine yet. |
| **Retention** | No time- or size-based expiry. |
| **Frame index footers** | Sealed-only writes make a per-group frame index *possible*, unlike live systems. Skipped so payloads stay byte-identical to the MoQT group stream, which keeps egress a zero-transform read. |
| **HLS / DASH / MSF renderers** | The manifest carries what they need — `epoch` maps to `EXT-X-DISCONTINUITY`, the wallclock index to `EXT-X-PROGRAM-DATE-TIME`. |
| **MoQT adapter** | Belongs in a `moqtstore` package; the only place that parses frames. |
| **S3 backend** | The interface is deliberately shaped around what S3 offers: conditional create, conditional overwrite, presigning. |
| **Manifest encoding** | JSON, because manifests are small, read far less often than payloads, and being able to inspect a broken track in a text editor is worth more than the bytes. `ManifestVersion` is what lets this be revisited. |
