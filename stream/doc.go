// Package stream renders a [ledger] track over HTTP as HLS and DASH.
//
// Both are derived views over the same protocol-agnostic manifest, never the
// storage format itself: a Group is one segment, [ledger.GroupInfo.Duration]
// becomes HLS EXTINF and DASH @d, a new producer epoch becomes an HLS
// #EXT-X-DISCONTINUITY and a DASH timeline reset, and a wallclock anchor becomes
// #EXT-X-PROGRAM-DATE-TIME and the MPD availabilityStartTime. A [Handler] is an
// [http.Handler] over one [*ledger.Track] that routes playlist and manifest
// requests and serves the segments both point at.
//
// The playlist is the whole track and grows as groups land — HLS as an EVENT
// playlist, DASH as a dynamic MPD — because the ledger is append-only and never
// deletes. There is no end-list signal; the ledger has no notion of a finished
// track.
//
// # Segment delivery
//
// Delivery is pluggable at this layer. The default [ProxyResolver] streams
// segment bytes through the ledger via [ledger.Reader.ReadGroup] — fine for
// local development. A production deployment supplies a [RedirectResolver] that
// mints a signed URL from [ledger.GroupInfo.ObjectKey], so clients fetch objects
// directly and the store stays free of any presigning method it could not
// implement uniformly. Both formats address segments by their [ledger.GroupID],
// so one segment handler serves them both.
//
// # What the renderer assumes
//
// Each group must carry a positive Duration: a segment without one has no HLS
// EXTINF or DASH @d, so the renderers refuse it. The container is whatever the
// track's schema declares; an fMP4 init segment, when the container needs one,
// is optional caller-supplied bytes or URL ([Options.InitSegment]).
//
// The renderers reflect the whole track and regenerate per request, which is
// O(track size). Caching the output and bounding it with a sliding window are
// deliberate follow-ons.
package stream
