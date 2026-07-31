// Package ledger stores and replays temporal data — video, audio, logs, and
// sensor readings — as immutable objects described by manifest objects.
//
// [Bucket] is the entry point: it binds an object store once, and every
// [Writer] and [Reader] is opened from it.
//
// The design borrows the append-only ordered log from Kafka and the
// independently-decodable segment from HLS, then keeps the two ideas separate:
// a Group is both the unit of independent decoding and the unit of storage,
// while the manifest is the ordered log that gives Groups their meaning.
//
// # What this package is not
//
// The ledger is not on the live path. A Group can only be stored once it is
// sealed, so the earliest a reader could see its first frame through the
// ledger is one group duration plus a round trip — hundreds of milliseconds
// behind a relay serving the same frames from memory. Live delivery belongs to
// a relay; the ledger serves what has already happened. A client that wants
// both is responsible for stitching the seam, exactly as an HLS player is.
//
// # Manifest is the source of truth
//
// There is no embedded database. Everything durable is an object:
//
//	<track>/root.manifest              track metadata: timescale, encoding, epoch
//	<track>/delta/head                 pointer to the newest committed delta
//	<track>/delta/open/00000042.manifest
//	<track>/delta/sealed-00000001.manifest
//	<track>/groups/e000001-g00000042   payload
//
// Every object above is immutable except head. Writes are therefore conditional
// creates, which makes a duplicate append fail cleanly instead of corrupting,
// and fences a writer that has been superseded after a failover.
//
// # Commit
//
// Writing the delta manifest is the commit. A delta is a whole-object atomic
// write and is immutable, so an object that exists is valid by construction and
// a reader that discovers one is never reading dirty state. head is a discovery
// cache, not a transaction boundary: it may lag arbitrarily without affecting
// correctness. A writer that crashes recovers by reading head and probing
// forward until a delta is absent — that gap is the true tip.
//
// One consequence is worth stating plainly: nothing at the manifest layer can
// be orphaned. Only a Group object can be, by being written just before a crash
// that prevented its delta from landing. Garbage collection has exactly that
// one job.
//
// # Two addressing regimes
//
// Delta manifests are numbered by the ledger and are contiguous, so a reader
// tails them arithmetically: hold N, request N+1, treat absence as "not yet".
// No listing is required, which matters because listing is the most expensive
// and least consistent operation object stores offer.
//
// Group sequences are assigned by the producer and are *not* contiguous. Groups
// are legitimately dropped under congestion, so gaps are real data rather than
// corruption, and a producer restart resets the sequence — which is what Epoch
// disambiguates. Preserving the producer's numbering is deliberate: a relay
// serving the same track live uses those sequence numbers, and a client can
// only align live playback with replay if both agree on what group 42 is.
//
// So: probe forward through manifests, but never derive a Group key. Read it
// from the manifest.
//
// # Time
//
// A Group is anchored on two timelines rather than described as a closed
// interval, because groups are serial within an epoch: the start of one is the
// end of the last.
//
// [GroupInfo.MediaTime] is the media-time anchor and is always present. It is exact,
// arrives with the data, and is immune to clock skew, but is relative to one
// track's origin and cannot be compared across publishers. [GroupInfo.Wallclock] is
// the wallclock anchor, absolute and comparable across every track and source
// type — what makes "show me the video and the sensor readings at 14:32"
// answerable at all — but it depends on a clock and drifts.
//
// Wallclock is optional, since not every producer has a clock worth trusting, and a
// Group without one still replays within its own track.
//
// [GroupInfo.Duration] is also optional, and is stored rather than derived from
// the next Group's anchor. That derivation fails in the two places that matter:
// across a dropped Group it would silently span the gap, and the newest Group
// has no successor, so a live HLS playlist could not include its newest segment
// until the following one landed. Derived views consume it directly — it is
// EXTINF in HLS and @d in a DASH SegmentTimeline.
//
// This package does not compute media time. Extracting it means parsing a
// transport's wire format, and the core deliberately depends on no transport.
// Callers supply a fully populated [GroupInfo]; adapters such as a Media over
// QUIC ingest are where frame parsing belongs.
//
// # Reading without the ledger
//
// A reader holding object-store credentials can seek and replay a track with no
// ledger process running anywhere. Authorization is somebody else's problem by
// design: an external service is expected to mint scoped credentials or signed
// URLs, leaving clients reading objects directly.
package ledger
